//! Módulo de lógica y administración dinámica de Redes y Hubs.
//!
//! Este módulo contiene el núcleo de las reglas de negocio para mantener sincronizada
//! la topología de la red entre tres fuentes de verdad:
//! 1. **El Servidor (Nube):** Que dicta el estado deseado (Crear/Borrar redes, Configurar Hubs).
//! 2. **Los Hubs (Físicos):** Que reportan su existencia y confirman configuraciones aplicadas.
//! 3. **La Base de Datos Local (SQLite):** Que provee el estado base al iniciar y persiste los cambios.
//!
//! # Arquitectura de Tareas
//!
//! - **[`network_admin`]:** El "Cerebro". Toma decisiones basadas en eventos de red, actualiza
//!   la caché en memoria (`NetworkManager`) y enruta las acciones resultantes.
//! - **[`network_dba`]:** El "Traductor de Persistencia". Escucha los cambios de estado dictaminados
//!   por el administrador y los convierte en comandos transaccionales para la base de datos.

use crate::context::domain::AppContext;
use crate::database::domain::DataServiceCommand;
use crate::message::logic::{
    ActiveHub, DeleteHub, HubMessage, Metadata, Network as NetworkMsg, ServerMessage,
};
use crate::network::domain::NetworkAction::{Delete, Ignore, Insert, Update};
use crate::network::domain::{
    Batch, HubChanged, HubRow, Network, NetworkAction, NetworkChanged, NetworkRow,
    NetworkServiceResponse,
};
use chrono::Utc;
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument};

/// Tarea principal de procesamiento y reglas de negocio de la red.
///
/// Evalúa constantemente tres flujos de entrada:
/// 1. **Comandos del Servidor (`rx_from_server`):** Creación/Borrado de redes, y nuevas configuraciones.
/// 2. **Respuestas de Hubs (`rx_from_hub`):** Autodescubrimiento de Hubs y ACKs de configuración.
/// 3. **Carga Inicial (`rx_batch`):** Población de la memoria en el arranque desde la base de datos.
///
/// # Control de Concurrencia
///
/// La memoria compartida (`NetworkManager`) está protegida por un `RwLock`.
/// Se prioriza obtener un lock de lectura (`read().await`) para evaluar si un cambio es necesario
/// (Fase de Decisión). Solo si se requiere una mutación (Insert, Update, Delete), se solicita
/// un lock de escritura (`write().await`), minimizando los cuellos de botella.
///
/// # Caché de Configuraciones Pendientes
///
/// Utiliza `hub_hash_aux` para almacenar temporalmente las configuraciones dictadas por el servidor
/// hasta que el Hub físico confirme su aplicación mediante un `FromHubSettingsAck`.
#[instrument(name = "network_admin", skip_all)]
pub async fn network_admin(
    tx_to_insert_network: mpsc::Sender<NetworkChanged>,
    tx_to_core: mpsc::Sender<NetworkServiceResponse>,
    tx_to_insert_hub: mpsc::Sender<HubChanged>,
    mut rx_from_server: mpsc::Receiver<ServerMessage>,
    mut rx_from_hub: mpsc::Receiver<HubMessage>,
    mut rx_batch: mpsc::Receiver<Batch>,
    app_context: AppContext,
    shutdown: CancellationToken,
) {
    let mut hub_hash_aux: HashMap<String, HashSet<HubRow>> = HashMap::new();

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("shutdown recibido network_admin");
                break;
            }

            Some(msg_from_server) = rx_from_server.recv() => {
                match msg_from_server {
                    ServerMessage::Network(network) => {
                        let action = {
                            let manager = app_context.net_man.read().await;
                            match (manager.networks.get(&network.id_network), network.delete_network) {
                                (Some(_), true) => Delete,
                                (Some(existing), false) if existing.active != network.active => {
                                    Update
                                }
                                (None, false) => Insert,
                                _ => Ignore,
                            }
                        };
                        if action == Ignore {
                            continue;
                        }
                        let net_chan = handle_action(app_context.clone(), &network, action).await;
                        handle_event(&tx_to_insert_network, &tx_to_core, net_chan, app_context.clone()).await;
                    },
                    ServerMessage::FromServerSettings(ref settings) => {
                        let id = settings.network.clone();
                        hub_hash_aux.entry(settings.metadata.sender_user_id.clone())
                            .or_default()
                            .insert(settings.clone().cast_settings_to_hub_row(id));
                        if tx_to_core.send(NetworkServiceResponse::HubMessage(HubMessage::FromServerSettings(settings.clone()))).await.is_err() {
                            error!("no se pudo enviar el mensaje de nueva configuración al Hub");
                        }
                    },
                    ServerMessage::DeleteHub(ref hub) => {
                        let id = hub.network.clone();
                        let mut manager = app_context.net_man.write().await;
                        manager.remove_hub(&id, &hub.metadata.destination_id);
                        if tx_to_insert_hub.send(HubChanged::Delete(id.clone())).await.is_err() {
                            error!("no se pudo notificar a network_dba_task que debe eliminar un Hub");
                        }
                        if tx_to_core.send(NetworkServiceResponse::HubMessage(HubMessage::DeleteHub(hub.clone()))).await.is_err() {
                            error!("no se pudo enviar el mensaje de nueva configuración al Hub");
                        }
                    }
                    _ => {},
                }
            }

            Some(msg_from_hub) = rx_from_hub.recv() => {
                match msg_from_hub {
                    HubMessage::LinkageRequest(request) => {
                        let id = request.network.clone();
                        let mut manager = app_context.net_man.write().await;
                        if !manager.search_hub(&id, &request.metadata.sender_user_id) {
                            debug!("Hub con id {} ha solicitado unirse a la red {}", request.metadata.sender_user_id, id);
                            let hub_row = request.cast_to_hub_row();
                            manager.add_hub(id, hub_row.clone().cast_to_hub());
                            drop(manager);
                            if tx_to_insert_hub.send(HubChanged::Insert(hub_row)).await.is_err() {
                                error!("no se pudo notificar a network_dba_task que un nuevo Hub debe insertarse");
                            }
                        } else {
                            debug!("Hub con id {} ha solicitado unirse a la red {} pero ya está registrado", request.metadata.sender_user_id, id);
                            if tx_to_core.send(NetworkServiceResponse::GenerateLinkageAck(request.metadata.sender_user_id)).await.is_err() {
                                error!("no se pudo enviar NetworksReady desde network_admin")
                            }
                        }
                    },
                    _ => {},
                }
            }

            Some(batch) = rx_batch.recv() => {
                match batch {
                    Batch::Network(networks) => {
                        let mut manager = app_context.net_man.write().await;
                        for networks_row in networks {
                            let network = networks_row.cast_to_network();
                            manager.add_network(network);
                        }
                        info!("estado cargado, {} redes en sistema", manager.networks.len());
                        if tx_to_core.send(NetworkServiceResponse::NetworksReady).await.is_err() {
                            error!("no se pudo enviar NetworksReady desde network_admin")
                        }
                    }
                    Batch::Hub(hubs) => {
                        let mut manager = app_context.net_man.write().await;
                        for hubs_row in hubs {
                            let net_id = hubs_row.network_id.clone();
                            let hub = hubs_row.cast_to_hub();
                            manager.add_hub(net_id, hub);
                        }
                        info!("estado cargado, {} hubs en sistema", manager.get_total_hubs());
                    }
                }
            }
        }
    }
}

/// Ejecuta la mutación de estado en la memoria caché (`NetworkManager`).
///
/// Obtiene un bloqueo de escritura (`write().await`) exclusivo sobre el manejador, aplica
/// la operación requerida (Delete, Update, Insert) y retorna el evento de dominio
/// correspondiente (`NetworkChanged`) para ser procesado por los demás sistemas.
async fn handle_action(
    app_context: AppContext,
    network: &NetworkMsg,
    action: NetworkAction,
) -> NetworkChanged {
    let mut manager = app_context.net_man.write().await;
    let net_chan: NetworkChanged = match action {
        Delete => {
            manager.remove_network(&network.id_network);
            NetworkChanged::Delete {
                id: network.id_network.clone(),
            }
        }
        Update => {
            manager.change_active(network.active, &network.id_network);
            let net = NetworkRow::new(network.id_network.clone(), network.active);
            NetworkChanged::Update(net)
        }
        Insert => {
            manager.add_network(Network::new(network.id_network.clone(), network.active));
            let net = NetworkRow::new(network.id_network.clone(), network.active);
            NetworkChanged::Insert(net)
        }
        _ => unreachable!(),
    };
    net_chan
}

/// Función auxiliar para distribuir las consecuencias de un cambio de red.
///
/// Disemina el evento hacia dos destinos críticos:
/// 1. `tx_to_core`: Genera comandos MQTT (`DeleteHub` o `ActiveHub`) que viajan hacia
///    los dispositivos físicos informándoles del cambio en su red.
/// 2. `tx_to_insert_network`: Envía el evento a `network_dba` para asegurar la
///    persistencia del cambio en SQLite.
async fn handle_event(
    tx_to_insert_network: &mpsc::Sender<NetworkChanged>,
    tx_to_core: &mpsc::Sender<NetworkServiceResponse>,
    net_chan: NetworkChanged,
    app_context: AppContext,
) {
    match net_chan.clone() {
        NetworkChanged::Delete { id } => {
            let metadata = create_metadata(app_context.clone());
            let delete_hub = DeleteHub {
                metadata,
                network: id,
            };
            if tx_to_core
                .send(NetworkServiceResponse::HubMessage(HubMessage::DeleteHub(
                    delete_hub,
                )))
                .await
                .is_err()
            {
                error!("Error: No se pudo enviar mensaje de eliminación de red a los hubs");
            }
        }
        NetworkChanged::Update(network) => {
            let metadata = create_metadata(app_context.clone());
            let active_hub = ActiveHub {
                metadata,
                network: network.id_network,
                active: network.active,
            };
            if tx_to_core
                .send(NetworkServiceResponse::HubMessage(HubMessage::ActiveHub(
                    active_hub,
                )))
                .await
                .is_err()
            {
                error!(
                    "Error: No se pudo enviar mensaje de activación/desactivación de red a los hubs"
                );
            }
        }
        _ => {}
    }

    if tx_to_insert_network.send(net_chan).await.is_err() {
        error!("Error: No se pudo enviar NetworkChanged");
    }
}

/// Crea una cabecera de metadatos estandarizada para mensajes generados internamente.
///
/// Utiliza el ID del Edge Gateway como origen (`sender_user_id`) y marca el destino
/// como "all" para broadcasts locales.
fn create_metadata(app_context: AppContext) -> Metadata {
    let timestamp = Utc::now().timestamp();

    let metadata = Metadata {
        sender_user_id: app_context.system.id_edge.clone(),
        destination_id: "all".to_string(),
        timestamp: timestamp,
    };
    metadata
}

/// Tarea de traducción y persistencia para configuraciones de red y hubs.
///
/// Consume los eventos de dominio (`NetworkChanged`, `HubChanged`) emitidos por `network_admin`
/// y los traduce a comandos CRUD (`DataServiceCommand`) destinados a la capa de base de datos.
///
/// # Acciones
///
/// - **Redes:** Propaga la creación, actualización de estado (`active`) o eliminación en cascada.
/// - **Hubs:** Propaga registros (Autodescubrimiento), actualizaciones o bajas (Server request).
#[instrument(name = "network_dba", skip_all)]
pub async fn network_dba(
    tx: mpsc::Sender<DataServiceCommand>,
    mut rx_from_network: mpsc::Receiver<NetworkChanged>,
    mut rx_from_network_hub: mpsc::Receiver<HubChanged>,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("Info: shutdown recibido network_dba");
                break;
            }

            Some(msg_from_network) = rx_from_network.recv() => {
                match msg_from_network {
                    NetworkChanged::Delete { id} => {
                        if tx.send(DataServiceCommand::DeleteNetwork(id.clone())).await.is_err() {
                            error!("Error: no se pudo enviar comando DeleteNetwork");
                        }
                        if tx.send(DataServiceCommand::DeleteAllHubByNetwork(id)).await.is_err() {
                            error!("Error: no se pudo enviar comando DeleteNetwork");
                        }
                    },
                    NetworkChanged::Update(network) => {
                        if tx.send(DataServiceCommand::UpdateNetwork(network)).await.is_err() {
                            error!("Error: no se pudo enviar comando UpdateNetwork");
                        }
                    },
                    NetworkChanged::Insert(network) => {
                        if tx.send(DataServiceCommand::NewNetwork(network)).await.is_err() {
                            error!("Error: no se pudo enviar comando NewNetwork");
                        }
                    },
                }
            }

            Some(msg_hub_changed) = rx_from_network_hub.recv() => {
                match msg_hub_changed {
                    HubChanged::Insert(hub) => {
                        if tx.send(DataServiceCommand::NewHub(hub)).await.is_err() {
                            error!("Error: no se pudo enviar comando NewHub");
                        }
                    },
                    HubChanged::Delete(id) => {
                        if tx.send(DataServiceCommand::DeleteHub(id)).await.is_err() {
                            error!("Error: no se pudo enviar comando DeleteHub");
                        }
                    }
                }
            }
        }
    }
}
