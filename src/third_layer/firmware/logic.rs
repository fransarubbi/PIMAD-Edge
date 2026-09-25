//! # Módulo Lógico de Firmware
//!
//! Este módulo actúa como el orquestador asíncrono. Gestiona la concurrencia,
//! la comunicación I/O con la red y ejecuta las acciones dictadas por el dominio.

use crate::config::firmware::OTA_TIMEOUT;
use crate::context::domain::AppContext;
use crate::second_layer::message::domain::{FirmwareEdgeResult, UpdateHubFirmware};
use crate::second_layer::message::{
    domain::{
        FirmwareHubAck, FirmwareHubResult, Metadata, UpdateEdgeFirmware, UpdateFirmwareRequestHub,
    },
    logic::MessageHandle,
};
use crate::third_layer::firmware::domain::Event;
use chrono::Utc;
use self_update::backends::github::Update;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument};

#[derive(PartialEq, Eq)]
enum State {
    Sleeping,
    Working,
}

/// Enum que encapsula los comandos que recibe la tarea `hub_ota`.
pub enum CommandToHubOta {
    /// Inicia el proceso de actualización solicitada por el servidor para una red dada.
    Request(UpdateHubFirmware),
    /// Evento asíncrono con el resultado de actualización de un Hub.
    Response(FirmwareHubAck),
}

struct HubFirmwareStatus {
    pub id: String,
    pub is_updated: bool,
    pub success: bool,
}

/// Tarea asíncrona dedicada a actualizar el firmware del propio dispositivo Edge (Auto-actualización).
///
/// # Funcionamiento
/// 1. Entra en estado `Sleeping` y espera el comando `UpdateEdgeFirmware`.
/// 2. Cuando llega el comando, verifica vía `self_update` si existe una *release* más reciente
///    en el repositorio de GitHub (de acuerdo al token y nombre de usuario).
/// 3. De ser así, descarga y reemplaza el binario en ejecución (`pimad_edge`).
/// 4. Notifica el resultado al servidor central (vía `MessageHandle`).
/// 5. Si fue exitoso, envía una señal de apagado general para reiniciar y ejecutar la nueva versión.
#[instrument(name = "edge_ota", skip_all)]
pub async fn edge_ota(
    mut rx: mpsc::Receiver<UpdateEdgeFirmware>,
    handle: MessageHandle,
    app_context: AppContext,
) {
    while let Some(update) = rx.recv().await {
        if update.metadata.destination_id != app_context.system.id_edge {
            info!("no se iniciará el proceso de actualización. ID equivocado");
            continue;
        }
        let update_result = tokio::task::spawn_blocking(
            || -> Result<_, Box<dyn std::error::Error + Send + Sync>> {
                Ok(Update::configure()
                    .repo_owner("fransarubbi")
                    .repo_name("PIMAD-Edge")
                    .bin_name("pimad_edge")
                    .show_download_progress(false)
                    .no_confirm(true)
                    .current_version(env!("CARGO_PKG_VERSION"))
                    .build()?
                    .update()?)
            },
        )
        .await;

        match update_result {
            Ok(Ok(status)) => {
                let metadata = Metadata {
                    sender_user_id: app_context.system.id_edge.clone(),
                    destination_id: "server0".to_string(),
                    timestamp: Utc::now().timestamp(),
                };
                let update = FirmwareEdgeResult {
                    metadata,
                    error: false,
                };

                if status.is_updated() {
                    info!("actualizado con éxito a la versión: {}", status.version());
                    handle.serialize_edge_firmware_result(update).await;
                    // Dormir 5 segundos para dar tiempo a que el mensaje gRPC salga
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    info!("iniciando apagado para aplicar actualización OTA...");
                    std::process::exit(0);
                } else {
                    info!("el sistema ya está en la última versión");
                    handle.serialize_edge_firmware_result(update).await;
                }
            }
            Ok(Err(e)) => {
                error!("error en la actualización OTA: {}", e);
                let metadata = Metadata {
                    sender_user_id: app_context.system.id_edge.clone(),
                    destination_id: "server0".to_string(),
                    timestamp: Utc::now().timestamp(),
                };
                let update = FirmwareEdgeResult {
                    metadata,
                    error: true,
                };
                handle.serialize_edge_firmware_result(update).await;
            }
            Err(e) => {
                error!("error al ejecutar la tarea bloqueante (JoinError): {}", e);
                error!("error en la actualización OTA: {}", e);
                let metadata = Metadata {
                    sender_user_id: app_context.system.id_edge.clone(),
                    destination_id: "server0".to_string(),
                    timestamp: Utc::now().timestamp(),
                };
                let update = FirmwareEdgeResult {
                    metadata,
                    error: true,
                };
                handle.serialize_edge_firmware_result(update).await;
            }
        }
    }
}

/// Tarea asíncrona dedicada a orquestar la actualización de firmware de una flota de Hubs.
///
/// Implementa una Máquina de Estados que:
/// 1. Espera la solicitud de actualizar una red (`Request`).
/// 2. Inicia un temporizador de seguridad (`firmware_watchdog_timer`).
/// 3. Obtiene la versión requerida y los Hubs pertenecientes a esa red.
/// 4. Despacha comandos de actualización (`UpdateFirmwareRequestHub`) mediante MQTT hacia los Hubs.
/// 5. Recolecta iterativamente los acuses de recibo (`FirmwareHubAck`) hasta que todos responden
///    o expira el temporizador (Timeout).
/// 6. Calcula el porcentaje de éxito y se lo envía de vuelta al servidor central.
#[instrument(name = "hub_ota", skip_all)]
pub async fn hub_ota(
    tx_to_timer: mpsc::Sender<Event>,
    mut rx_msg: mpsc::Receiver<CommandToHubOta>,
    mut rx_timer: mpsc::Receiver<Event>,
    app_context: AppContext,
    handle: MessageHandle,
    cancel: CancellationToken,
) {
    let mut state = State::Sleeping;
    let mut process_vector: Vec<HubFirmwareStatus> = Vec::new();
    let mut index: usize = 0;
    let mut version = String::new();
    let mut network = String::new();

    if tx_to_timer.send(Event::StopTimer).await.is_err() {
        error!("no se pudo enviar StopTimer");
    }

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("shutdown recibido update_firmware_task");
                break;
            }

            Some(cmd) = rx_msg.recv() => {
                match cmd {
                    CommandToHubOta::Request(update) => {
                        state = State::Working;
                        process_vector.clear();
                        index = 0;

                        if let Ok(v) = get_firmware_version().await {
                            version = v;
                            network = update.network.clone();
                            let manager = app_context.net_man.read().await;

                            let vec_of_ids = manager.get_all_hub_ids_by_network(&network);

                            if vec_of_ids.is_empty() {
                                state = State::Sleeping;
                                error!("No hay Hubs en la red {network}. Nada para actualizar!");
                                let error = format!("No hay Hubs en la red {network}. Nada para actualizar!");
                                let msg = generate_outcome_error(
                                    app_context.system.id_edge.clone(),
                                    network.clone(),
                                    error
                                );
                                handle.serialize_hub_firmware_result(msg).await;
                            } else {
                                let total = vec_of_ids.len();
                                info!("iniciando nueva sesión de actualización de firmware. Red {}. Cantidad de Hubs {}", network, total);
                                for id in vec_of_ids {
                                    let hub = HubFirmwareStatus {
                                        id,
                                        is_updated: false,
                                        success: false
                                    };
                                    process_vector.push(hub);
                                }

                                let msg = generate_message_to_hub(
                                    &process_vector,
                                    index,
                                    app_context.system.id_edge.clone(),
                                    network.clone(),
                                    version.clone()
                                );
                                handle.serialize_update_hub_firmware(msg).await;

                                if tx_to_timer.send(Event::InitTimer(OTA_TIMEOUT)).await.is_err(){
                                    error!("no se pudo enviar InitTimer");
                                }
                            }
                        } else {
                            error!("no se pudo obtener la versión actual del firmware");
                            state = State::Sleeping;
                            let error = format!("No se pudo obtener la versión actual del firmware");
                            let msg = generate_outcome_error(
                                app_context.system.id_edge.clone(),
                                network.clone(),
                                error
                            );
                            handle.serialize_hub_firmware_result(msg).await;
                        }
                    },

                    CommandToHubOta::Response(firmware) => {
                        if state == State::Working {
                            if index < process_vector.len() {
                                let id = process_vector[index].id.clone();
                                if firmware.metadata.sender_user_id == id {
                                    if tx_to_timer.send(Event::StopTimer).await.is_err() {
                                        error!("no se pudo enviar StopTimer");
                                    }
                                    process_vector[index].is_updated = firmware.is_updated;
                                    process_vector[index].success = firmware.success;
                                    index = index + 1;
                                    if index < process_vector.len() {
                                        let msg = generate_message_to_hub(
                                            &process_vector,
                                            index,
                                            app_context.system.id_edge.clone(),
                                            network.clone(),
                                            version.clone()
                                        );
                                        handle.serialize_update_hub_firmware(msg).await;
                                        if tx_to_timer.send(Event::InitTimer(OTA_TIMEOUT)).await.is_err() {
                                            error!("no se pudo enviar InitTimer");
                                        }
                                    } else {
                                        state = State::Sleeping;
                                        let msg = generate_outcome(
                                            &process_vector,
                                            app_context.system.id_edge.clone(),
                                            network.clone()
                                        );
                                        handle.serialize_hub_firmware_result(msg).await;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            Some(event) = rx_timer.recv() => {
                match event {
                    Event::Timeout => {
                        if tx_to_timer.send(Event::StopTimer).await.is_err() {
                            error!("no se pudo enviar StopTimer");
                        }
                        process_vector[index].is_updated = false;
                        process_vector[index].success = false;
                        index =  index + 1;
                        if index < process_vector.len() {
                            let msg = generate_message_to_hub(
                                &process_vector,
                                index,
                                app_context.system.id_edge.clone(),
                                network.clone(),
                                version.clone()
                            );
                            handle.serialize_update_hub_firmware(msg).await;
                            if tx_to_timer.send(Event::InitTimer(OTA_TIMEOUT)).await.is_err() {
                                error!("no se pudo enviar InitTimer");
                            }
                        } else {
                            state = State::Sleeping;
                            let msg = generate_outcome(
                                &process_vector,
                                app_context.system.id_edge.clone(),
                                network.clone()
                            );
                            handle.serialize_hub_firmware_result(msg).await;
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

/// Obtiene la versión actual del sistema desde el repositorio.
async fn get_firmware_version() -> Result<String, reqwest::Error> {
    let url =
        "https://raw.githubusercontent.com/fransarubbi/IoT_Environmental_Hub/master/version.txt";

    let response = reqwest::get(url).await?;
    let version_text = response.text().await?;
    let cleaned = version_text.trim();
    let final_version = cleaned.strip_prefix('v').unwrap_or(cleaned);
    Ok(final_version.to_string())
}

fn generate_message_to_hub(
    process_vector: &Vec<HubFirmwareStatus>,
    index: usize,
    id_edge: String,
    network: String,
    version: String,
) -> UpdateFirmwareRequestHub {
    let hub_id = process_vector[index].id.clone();
    let timestamp = Utc::now().timestamp();

    info!(
        "generando mensaje para el hub: {}, en la red: {}, version: {}",
        hub_id, network, version
    );
    let metadata = Metadata {
        sender_user_id: id_edge,
        destination_id: hub_id,
        timestamp: timestamp,
    };
    let msg = UpdateFirmwareRequestHub {
        metadata,
        network: network,
        version: version,
    };
    msg
}

fn generate_outcome_error(id_edge: String, network: String, error: String) -> FirmwareHubResult {
    let timestamp = Utc::now().timestamp();
    info!(
        "generando mensaje de error para el servidor: {}, en la red: {}, error: {}",
        id_edge, network, error
    );
    let metadata = Metadata {
        sender_user_id: id_edge,
        destination_id: "server0".to_string(),
        timestamp: timestamp,
    };

    let msg = FirmwareHubResult {
        metadata,
        network,
        percentage_ok: 0.0,
        error,
    };
    msg
}

fn generate_outcome(
    process_vector: &Vec<HubFirmwareStatus>,
    id_edge: String,
    network: String,
) -> FirmwareHubResult {
    let total = process_vector.len();
    let mut counter = 0;
    for status in process_vector {
        if status.is_updated || status.success {
            counter = counter + 1;
        }
    }
    let percentage_ok = (counter as f32 / total as f32) * 100.0;
    info!(
        "generando mensaje de outcome para el servidor: {}, en la red: {}, porcentaje ok: {}",
        id_edge, network, percentage_ok
    );
    let timestamp = Utc::now().timestamp();

    let metadata = Metadata {
        sender_user_id: id_edge,
        destination_id: "server0".to_string(),
        timestamp: timestamp,
    };

    let msg = FirmwareHubResult {
        metadata,
        network,
        percentage_ok,
        error: " ".to_string(),
    };
    msg
}
