//! Módulo de Lógica de Mensajería y Enrutamiento (Router/Switchboard).
//!
//! Este módulo es el núcleo de comunicaciones del Edge Gateway. Actúa como un "Switchboard"
//! o enrutador central que conecta los nodos físicos (Hubs vía MQTT) con la nube (Servidor vía gRPC),
//! pasando por la máquina de estados local (FSM) y la persistencia de datos (DB).
//!
//! # Responsabilidades Principales
//!
//! - **Uplink Local (`msg_from_hub`):** Recibe telemetría MQTT (MessagePack), la deserializa y
//!   decide si enviarla a la nube en tiempo real o a la base de datos si no hay conexión.
//! - **Downlink Local (`msg_to_hub`):** Recibe comandos internos o remotos, los serializa a
//!   MessagePack y los publica en el broker MQTT local hacia los Hubs.
//! - **Uplink Remoto (`msg_to_server`):** Convierte los mensajes del dominio a estructuras Protobuf
//!   y los transmite al servidor central a través de gRPC.
//! - **Downlink Remoto (`msg_from_server`):** Recibe instrucciones gRPC de la nube, las traduce
//!   al modelo de dominio y las distribuye a la FSM o a los Hubs.

use crate::context::domain::AppContext;
use crate::database::domain::TableDataVector;
use crate::grpc;
use crate::grpc::from_edge::Payload;
use crate::grpc::{
    EdgeState as StateEdge, FirmwareOutcome, FirmwareOutcomeError, FromEdge, HelloWorld as Hello,
    HubState as Hub, NetworkAck as AckNetwork, SettingOk as SettOk, Settings as Sett,
    SystemMetrics, ToEdge, UpdateEdgeFirmware as UpdateEdge, to_edge,
};
use crate::message::domain::{
    AlertAir, AlertTh, DeleteHub, EdgeState, EmptyQueue, EmptyQueueSafeMode, FirmwareOk,
    HandshakeFromHub, Heartbeat as HeartbeatMsg, HelloWorld, HubMessage, HubState, LinkageRequest,
    LocalStatus, Measurement, MessageServiceCommand, MessageServiceResponse, Metadata, Monitor,
    Network, NetworkAck, SerializedMessage, ServerMessage, ServerStatus, SettingOk, Settings,
    UpdateEdgeFirmware, UpdateFirmware,
};
use crate::network::domain::NetworkManager;
use crate::system::domain::{InternalEvent, System};
use chrono::Utc;
use rmp_serde::{from_slice, to_vec};
use serde::Serialize;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

/// Gestor de mensajes salientes hacia el Hub (Downlink Local).
///
/// Centraliza todo el tráfico que debe ser publicado en el broker MQTT local para
/// que los dispositivos finales (Hubs) lo consuman.
///
/// # Fuentes de Mensajes
///
/// 1.  **FSM (`rx`):** Mensajes de control generados internamente (Handshakes, Heartbeats, Ping).
/// 2.  **Servidor (`rx_server_msg`):** Comandos remotos (Settings, acks) provenientes de la nube.
/// 3.  **Red (`rx_internal`):** Notificaciones de conexión/desconexión del broker MQTT local.
///
/// # Lógica de Procesamiento
///
/// - Monitorea el estado de la conexión local. Si está desconectado, los mensajes se descartan
///   para evitar desbordar colas.
/// - Resuelve dinámicamente el `topic`, `QoS` y flag de `Retain` a través del `NetworkManager`.
/// - Llama a la función genérica `send` para serializar en **MessagePack** y despachar a la capa MQTT.
#[instrument(name = "msg_to_hub", skip_all)]
pub async fn msg_to_hub(
    tx_to_mqtt_local: mpsc::Sender<MessageServiceResponse>,
    mut rx_internal: mpsc::Receiver<InternalEvent>,
    mut rx_server_msg: mpsc::Receiver<ServerMessage>,
    mut rx: mpsc::Receiver<MessageServiceCommand>,
    app_context: AppContext,
    shutdown: CancellationToken,
) {
    let mut state = LocalStatus::Connected;
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("shutdown recibido msg_to_hub");
                break;
            }

            Some(internal) = rx_internal.recv() => {
                match internal {
                    InternalEvent::LocalConnected => state = LocalStatus::Connected,
                    InternalEvent::LocalDisconnected => state = LocalStatus::Disconnected,
                    _ => {},
                }
            }

            Some(msg) = rx_server_msg.recv() => {
                match msg {
                    ServerMessage::FromServerSettingsAck(ack) => {
                        if matches!(state, LocalStatus::Disconnected) {
                            warn!("mensaje descartado, broker desconectado");
                            continue;
                        }
                        let topic_opt = {
                            let manager = app_context.net_man.read().await;
                            manager.get_topic_to_send_msg_to_hub(&HubMessage::FromServerSettingsAck(ack.clone()))
                        };

                        if let Some(t) = topic_opt {
                            match send(&tx_to_mqtt_local, t.topic, t.qos, HubMessage::FromServerSettingsAck(ack), false).await {
                                Ok(_) => {},
                                Err(_) => error!("no se pudo serializar el mensaje FromServerSettingsAck"),
                            }
                        }
                    }
                    _ => {}
                }
            }

            Some(msg) = rx.recv() => {
                match msg {
                    MessageServiceCommand::ToHub(to_hub) => {
                        if matches!(state, LocalStatus::Disconnected) {
                            warn!("mensaje descartado, broker desconectado");
                            continue;
                        }

                        let topic_out = {
                            let manager = app_context.net_man.read().await;
                            resolve_static_topic(&manager, &to_hub).await
                        };

                        if let Some((topic, qos, retain)) = topic_out {
                            match send(&tx_to_mqtt_local, topic.clone(), qos, to_hub, retain).await {
                                Ok(_) => {},
                                Err(_) => error!("no se pudo serializar el mensaje de la FSM al broker"),
                            }
                        } else {
                            let topic_opt = {
                                let manager = app_context.net_man.read().await;
                                manager.get_topic_to_send_msg_to_hub(&to_hub)
                            };

                            if let Some(t) = topic_opt {
                                match send(&tx_to_mqtt_local, t.topic, t.qos, to_hub, false).await {
                                    Ok(_) => {},
                                    Err(_) => error!("No se pudo serializar el mensaje del servidor al broker"),
                                }
                            }
                        }
                    },
                    _ => {}
                }
            }
        }
    }
}

/// Extrae el tópico MQTT, el nivel QoS y la bandera Retain específicos para mensajes de estado y control (FSM).
///
/// Mensajes críticos como los cambios de estado operativos (`StateBalanceMode`, `StateNormal`)
/// se publican con el flag `Retain = true` para que los dispositivos nuevos los reciban al conectar.
async fn resolve_static_topic(
    manager: &NetworkManager,
    msg: &HubMessage,
) -> Option<(String, u8, bool)> {
    match msg {
        HubMessage::HandshakeToHub(_) => Some((
            manager.topic_handshake.topic.clone(),
            manager.topic_handshake.qos,
            false,
        )),
        HubMessage::StateToHub(_) => Some((
            format!("{}/edge_state", manager.topic_state.topic),
            manager.topic_state.qos,
            false,
        )),
        HubMessage::PhaseNotification(_) => Some((
            format!("{}/phase", manager.topic_state.topic),
            manager.topic_state.qos,
            false,
        )),
        HubMessage::Heartbeat(_) => Some((
            manager.topic_heartbeat.topic.clone(),
            manager.topic_heartbeat.qos,
            false,
        )),
        HubMessage::LinkageAck(_) => Some((
            manager.topic_linkage_ack.topic.clone(),
            manager.topic_linkage_ack.qos,
            false,
        )),
        HubMessage::ActiveHub(active_hub) => {
            if let Some(n) = manager.networks.get(&active_hub.network) {
                Some((
                    n.topic_active_hub.topic.clone(),
                    n.topic_active_hub.qos,
                    true,
                ))
            } else {
                None
            }
        }
        _ => None,
    }
}

// -------------------------------------------------------------------------------------------------

/// Procesador de mensajes entrantes del Hub (Uplink Local).
///
/// Actúa como la primera línea de procesamiento para los datos que llegan desde la red de sensores
/// a través de MQTT. Realiza la deserialización desde MessagePack y enruta según el estado de la conexión externa.
///
/// # Reglas de Enrutamiento (Store and Forward Core)
///
/// 1.  Si el servidor externo está **Conectado**, reenvía la telemetría (Reports, Monitors, Alerts)
///     directamente al módulo `msg_to_server`.
/// 2.  Si el servidor externo está **Desconectado**, enruta la telemetría hacia `tx` (Canal general)
///     donde será interceptada por la base de datos (`dba_insert_task`) para almacenamiento temporal.
#[instrument(name = "msg_from_hub", skip_all)]
pub async fn msg_from_hub(
    tx: mpsc::Sender<MessageServiceResponse>,
    tx_to_msg_to_server: mpsc::Sender<ServerMessage>,
    tx_to_msg_to_hub: mpsc::Sender<InternalEvent>,
    mut rx: mpsc::Receiver<MessageServiceCommand>,
    app_context: AppContext,
    shutdown: CancellationToken,
) {
    let mut state: ServerStatus = ServerStatus::Connected;
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("shutdown recibido msg_from_hub");
                break;
            }

            Some(msg) = rx.recv() => {
                match msg {
                    MessageServiceCommand::Internal(internal) => {
                        match internal {
                            InternalEvent::LocalDisconnected => {
                                if tx_to_msg_to_hub.send(InternalEvent::LocalDisconnected).await.is_err() {
                                    error!("no se pudo enviar el mensaje LocalDisconnected a msg_to_hub");
                                }
                            },
                            InternalEvent::LocalConnected => {
                                if tx_to_msg_to_hub.send(InternalEvent::LocalConnected).await.is_err() {
                                    error!("no se pudo enviar el mensaje LocalConnected a msg_to_hub");
                                }
                            },
                            InternalEvent::IncomingMessage(message) => {
                                let topic = message.topic;
                                let payload = message.payload;

                                // Enrutar explícitamente según el sufijo del tópico
                                let decoded: Option<HubMessage> = if topic.ends_with("data") {
                                    from_slice::<Measurement>(&payload).ok().map(HubMessage::Report)
                                } else if topic.ends_with("hub_state") {
                                    from_slice::<HubState>(&payload).ok().map(HubMessage::HubState)
                                } else if topic.ends_with("monitor") {
                                    from_slice::<Monitor>(&payload).ok().map(HubMessage::Monitor)
                                } else if topic.ends_with("alert_air") {
                                    from_slice::<AlertAir>(&payload).ok().map(HubMessage::AlertAir)
                                } else if topic.ends_with("alert_temp") {
                                    from_slice::<AlertTh>(&payload).ok().map(HubMessage::AlertTem)
                                } else if topic.ends_with("balance_mode_handshake") {
                                    from_slice::<HandshakeFromHub>(&payload).ok().map(HubMessage::HandshakeFromHub)
                                } else if topic.ends_with("hub_firmware_ok") {
                                    from_slice::<FirmwareOk>(&payload).ok().map(HubMessage::FirmwareOk)
                                } else if topic.ends_with("hub_setting_ok") {
                                    from_slice::<SettingOk>(&payload).ok().map(HubMessage::FromHubSettingsAck)
                                } else if topic.ends_with("setting") {
                                    from_slice::<Settings>(&payload).ok().map(HubMessage::FromHubSettings)
                                } else if topic.ends_with("linkage_request") {
                                    from_slice::<LinkageRequest>(&payload).ok().map(HubMessage::LinkageRequest)
                                } else if topic.ends_with("empty_queue_safe") {
                                    from_slice::<EmptyQueueSafeMode>(&payload).ok().map(HubMessage::EmptyQueueSafe)
                                }
                                else if topic.ends_with("empty_queue") {
                                    from_slice::<EmptyQueue>(&payload).ok().map(HubMessage::EmptyQueue)
                                } else {
                                    None
                                };

                                // Procesar el mensaje decodificado
                                if let Some(decoded_msg) = decoded {
                                    if matches!(state, ServerStatus::Connected) {
                                        match decoded_msg {
                                            HubMessage::HubState(hub_state) => {
                                                let network = hub_state.network.clone();
                                                let manager = app_context.net_man.read().await;
                                                if manager.is_active(&network) {
                                                    let id = hub_state.metadata.sender_user_id.clone();
                                                    debug!("mensaje HubState proveniente del Hub {id}");
                                                    if tx_to_msg_to_server.send(ServerMessage::HubState(hub_state)).await.is_err() {
                                                        error!("no se pudo enviar el mensaje HubState");
                                                    }
                                                }
                                            }
                                            HubMessage::Report(report) => {
                                                let network = report.network.clone();
                                                let manager = app_context.net_man.read().await;
                                                if manager.is_active(&network) {
                                                    let id = report.metadata.sender_user_id.clone();
                                                    debug!("mensaje Report proveniente del Hub {id}");
                                                    if tx_to_msg_to_server.send(ServerMessage::Report(report)).await.is_err() {
                                                        error!("no se pudo enviar el mensaje Report");
                                                    }
                                                }
                                            },
                                            HubMessage::Monitor(monitor) => {
                                                let network = monitor.network.clone();
                                                let manager = app_context.net_man.read().await;
                                                if manager.is_active(&network) {
                                                    let id = monitor.metadata.sender_user_id.clone();
                                                    debug!("mensaje Monitor proveniente del Hub {id}");
                                                    if tx_to_msg_to_server.send(ServerMessage::Monitor(monitor)).await.is_err() {
                                                        error!("no se pudo enviar el mensaje Monitor");
                                                    }
                                                }
                                            },
                                            HubMessage::AlertAir(alert) => {
                                                let network = alert.network.clone();
                                                let manager = app_context.net_man.read().await;
                                                if manager.is_active(&network) {
                                                    let id = alert.metadata.sender_user_id.clone();
                                                    debug!("mensaje AlertAir proveniente del Hub {id}");
                                                    if tx_to_msg_to_server.send(ServerMessage::AlertAir(alert)).await.is_err() {
                                                        error!("no se pudo enviar el mensaje AlertAir");
                                                    }
                                                }
                                            },
                                            HubMessage::AlertTem(alert) => {
                                                let network = alert.network.clone();
                                                let manager = app_context.net_man.read().await;
                                                if manager.is_active(&network) {
                                                    let id = alert.metadata.sender_user_id.clone();
                                                    debug!("mensaje AlertTemp proveniente del Hub {id}");
                                                    if tx_to_msg_to_server.send(ServerMessage::AlertTem(alert)).await.is_err() {
                                                        error!("no se pudo enviar el mensaje AlertTem");
                                                    }
                                                }
                                            },
                                            HubMessage::HandshakeFromHub(ref handshake) => {
                                                let id = handshake.metadata.sender_user_id.clone();
                                                debug!("mensaje HandshakeFromHub proveniente del Hub {id}");
                                                if tx.send(MessageServiceResponse::FromHub(decoded_msg)).await.is_err() {
                                                    error!("no se pudo enviar el mensaje HandshakeFromHub");
                                                }
                                            },
                                            HubMessage::FirmwareOk(ref firmware) => {
                                                let id = firmware.metadata.sender_user_id.clone();
                                                debug!("mensaje FirmwareOk proveniente del Hub {id}");
                                                if tx.send(MessageServiceResponse::FromHub(decoded_msg)).await.is_err() {
                                                    error!("no se pudo enviar el mensaje FirmwareOk");
                                                }
                                            },
                                            HubMessage::FromHubSettings(settings) => {
                                                let network = settings.network.clone();
                                                let manager = app_context.net_man.read().await;
                                                if manager.is_active(&network) {
                                                    let id = settings.metadata.sender_user_id.clone();
                                                    debug!("mensaje FromHubSettings proveniente del Hub {id}");
                                                    if tx_to_msg_to_server.send(ServerMessage::FromHubSettings(settings)).await.is_err() {
                                                        error!("no se pudo enviar el mensaje FromHub");
                                                    }
                                                }
                                            },
                                            HubMessage::FromHubSettingsAck(ack) => {
                                                let id = ack.metadata.sender_user_id.clone();
                                                debug!("mensaje FromHubSettingsAck proveniente del Hub {id}");
                                                if tx_to_msg_to_server.send(ServerMessage::FromHubSettingsAck(ack)).await.is_err() {
                                                    error!("no se pudo enviar el mensaje FromHub");
                                                }
                                            },
                                            HubMessage::EmptyQueue(ref empty) => {
                                                let id = empty.metadata.sender_user_id.clone();
                                                debug!("mensaje EmptyQueue proveniente del Hub {id}");
                                                if tx.send(MessageServiceResponse::FromHub(decoded_msg)).await.is_err() {
                                                    error!("no se pudo enviar el mensaje EmptyQueue");
                                                }
                                            },
                                            HubMessage::EmptyQueueSafe(ref empty) => {
                                                let id = empty.metadata.sender_user_id.clone();
                                                debug!("mensaje EmptyQueueSafe proveniente del Hub {id}");
                                                if tx.send(MessageServiceResponse::FromHub(decoded_msg)).await.is_err() {
                                                    error!("no se pudo enviar el mensaje EmptyQueueSafe");
                                                }
                                            },
                                            HubMessage::LinkageRequest(ref linkage) => {
                                                let id = linkage.metadata.sender_user_id.clone();
                                                debug!("mensaje LinkageRequest proveniente del Hub {id}");
                                                if tx.send(MessageServiceResponse::FromHub(decoded_msg)).await.is_err() {
                                                    error!("no se pudo enviar el mensaje LinkageRequest");
                                                }
                                            },
                                            _ => {}
                                        }
                                    } else {
                                        debug!("mensaje proveniente del Hub almacenado temporalmente (Servidor desconectado)");
                                        if tx.send(MessageServiceResponse::FromHub(decoded_msg)).await.is_err() {
                                            error!("no se pudo enviar el mensaje FromHub");
                                        }
                                    }
                                } else {
                                    error!("no se pudo deserializar el mensaje del tópico: {}", topic);
                                }
                            }
                            InternalEvent::ServerConnected => {
                                state = ServerStatus::Connected;
                            }
                            InternalEvent::ServerDisconnected => {
                                state = ServerStatus::Disconnected;
                            }
                            _ => {},
                        }
                    },
                    _ => {}
                }
            }
        }
    }
}

// -------------------------------------------------------------------------------------------------

/// Gestor de mensajes salientes hacia el Servidor (Uplink Remoto vía gRPC).
///
/// Recolecta mensajes del dominio, los transforma al modelo Protobuf (gRPC) y los encola
/// para su envío hacia la nube.
///
/// # Flujos de Datos Procesados
///
/// - **Tiempo Real (`rx_from_hub`):** Datos recién llegados que pasaron el filtro de conexión.
/// - **Datos Históricos (`Batch`):** Lotes de información extraídos de SQLite tras recuperar conexión.
/// - **Comandos Internos:** Mensajes como `HelloWorld` para establecer sesión en la nube.
#[instrument(name = "msg_to_server", skip_all)]
pub async fn msg_to_server(
    tx_to_server: mpsc::Sender<MessageServiceResponse>,
    mut rx_from_hub: mpsc::Receiver<ServerMessage>,
    mut rx: mpsc::Receiver<MessageServiceCommand>,
    app_context: AppContext,
    shutdown: CancellationToken,
) {
    let mut state = ServerStatus::Connected;
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("shutdown recibido msg_to_server");
                break;
            }

            Some(msg) = rx.recv() => {
                match msg {
                    MessageServiceCommand::Internal(internal) => {
                        match internal {
                            InternalEvent::ServerConnected => {
                                state = ServerStatus::Connected;
                            }
                            InternalEvent::ServerDisconnected => {
                                state = ServerStatus::Disconnected;
                            }
                            _ => {}
                        }
                    },
                    MessageServiceCommand::Batch(batch) => {
                        if matches!(state, ServerStatus::Disconnected) {
                            warn!("mensaje del hub descartado, servidor desconectado");
                            continue;
                        }
                        match process_batch(batch, &tx_to_server, &app_context.system).await {
                            Ok(_) => debug!("Extracción batch exitosa. Enviado al cliente gRPC."),
                            Err(_) => error!("fallo en envío de batch al cliente gRPC"),
                        }
                    },
                    MessageServiceCommand::ToServer(server_message) => {
                        if matches!(state, ServerStatus::Disconnected) {
                            warn!("Warning: mensaje del hub descartado, servidor desconectado");
                            continue;
                        }
                        if let Some(proto_msg) = convert_to_proto_upload(server_message, app_context.system.id_edge.clone()) {
                            if tx_to_server.send(MessageServiceResponse::EdgeUpload(proto_msg)).await.is_err() {
                                error!("no se puede enviar mensaje EdgeUpload al cliente gRPC");
                            }
                        }
                    },
                    MessageServiceCommand::GenerateHelloWorld => {
                        let metadata = Metadata {
                            sender_user_id: app_context.system.id_edge.clone(),
                            destination_id: "server0".to_string(),
                            timestamp: Utc::now().timestamp(),
                        };
                        let hello = HelloWorld {
                            metadata,
                            hello: true
                        };
                        if let Some(proto_msg) = convert_to_proto_upload(ServerMessage::HelloWorld(hello), app_context.system.id_edge.clone()) {
                            if tx_to_server.send(MessageServiceResponse::EdgeUpload(proto_msg)).await.is_err() {
                                error!("no se puede enviar mensaje EdgeUpload al cliente gRPC");
                            }
                        }
                    },
                    MessageServiceCommand::GenerateEdgeState(state) => {
                        let metadata = Metadata {
                            sender_user_id: app_context.system.id_edge.clone(),
                            destination_id: "server0".to_string(),
                            timestamp: Utc::now().timestamp(),
                        };
                        let msg = EdgeState {
                            metadata,
                            state,
                        };
                        if let Some(proto_msg) = convert_to_proto_upload(ServerMessage::EdgePeriodic(msg), app_context.system.id_edge.clone()) {
                            if tx_to_server.send(MessageServiceResponse::EdgeUpload(proto_msg)).await.is_err() {
                                error!("no se puede enviar mensaje EdgeUpload al cliente gRPC");
                            }
                        }
                    },
                    MessageServiceCommand::GenerateNetworkAck(code) => {
                        let metadata = Metadata {
                            sender_user_id: app_context.system.id_edge.clone(),
                            destination_id: "server0".to_string(),
                            timestamp: Utc::now().timestamp(),
                        };
                        let msg = NetworkAck {
                            metadata,
                            id_network: code.0,
                            code_of_ack: code.1,
                        };
                        if let Some(proto_msg) = convert_to_proto_upload(ServerMessage::NetworkAck(msg), app_context.system.id_edge.clone()) {
                            if tx_to_server.send(MessageServiceResponse::EdgeUpload(proto_msg)).await.is_err() {
                                error!("no se puede enviar mensaje EdgeUpload al cliente gRPC");
                            }
                        }
                    },
                    _ => {}
                }
            }

            Some(server_message) = rx_from_hub.recv() => {
                if matches!(state, ServerStatus::Disconnected) {
                    warn!("mensaje del hub descartado, servidor desconectado");
                    continue;
                }
                if let Some(proto_msg) = convert_to_proto_upload(server_message, app_context.system.id_edge.clone()) {
                    if tx_to_server.send(MessageServiceResponse::EdgeUpload(proto_msg)).await.is_err() {
                        error!("no se puede enviar mensaje EdgeUpload al cliente gRPC");
                    }
                }
            }
        }
    }
}

/// Mapea las estructuras internas de dominio (`ServerMessage`) a los tipos generados
/// por Protobuf (`FromEdge` / `Payload`).
///
/// Este es el punto de traducción formal antes de que la capa de red gRPC envíe los bytes.
fn convert_to_proto_upload(msg: ServerMessage, edge_id: String) -> Option<FromEdge> {
    let payload = match msg {
        ServerMessage::HelloWorld(hello) => {
            debug!("serializando mensaje HelloWorld para el servidor");
            Some(Payload::HelloWorld(Hello {
                metadata: Some(grpc::Metadata {
                    sender_user_id: hello.metadata.sender_user_id,
                    destination_id: hello.metadata.destination_id,
                    timestamp: hello.metadata.timestamp,
                }),
                hello: hello.hello,
            }))
        }
        ServerMessage::HubState(hub_state) => {
            debug!("serializando mensaje HubState para el servidor");
            Some(Payload::HubState(Hub {
                metadata: Some(grpc::Metadata {
                    sender_user_id: hub_state.metadata.sender_user_id,
                    destination_id: hub_state.metadata.destination_id,
                    timestamp: hub_state.metadata.timestamp,
                }),
                network: hub_state.network,
                state: hub_state.state,
            }))
        }
        ServerMessage::EdgePeriodic(edge_state) => {
            debug!("serializando mensaje EdgeState para el servidor");
            Some(Payload::EdgeState(StateEdge {
                metadata: Some(grpc::Metadata {
                    sender_user_id: edge_state.metadata.sender_user_id,
                    destination_id: edge_state.metadata.destination_id,
                    timestamp: edge_state.metadata.timestamp,
                }),
                state: edge_state.state,
            }))
        }
        ServerMessage::NetworkAck(network_ack) => {
            debug!("serializando mensaje NetworkAck para el servidor");
            Some(Payload::NetworkAck(AckNetwork {
                metadata: Some(grpc::Metadata {
                    sender_user_id: network_ack.metadata.sender_user_id,
                    destination_id: network_ack.metadata.destination_id,
                    timestamp: network_ack.metadata.timestamp,
                }),
                id_network: network_ack.id_network,
                code_of_ack: network_ack.code_of_ack,
            }))
        }
        ServerMessage::FromHubSettings(hub_settings) => {
            debug!("serializando mensaje FromHubSettings para el servidor");
            Some(Payload::Settings(Sett {
                metadata: Some(grpc::Metadata {
                    sender_user_id: hub_settings.metadata.sender_user_id,
                    destination_id: hub_settings.metadata.destination_id,
                    timestamp: hub_settings.metadata.timestamp,
                }),
                message_id: hub_settings.message_id,
                network: hub_settings.network,
                wifi_ssid: hub_settings.wifi_ssid,
                wifi_password: hub_settings.wifi_password,
                mqtt_uri: hub_settings.mqtt_uri,
                device_name: hub_settings.device_name,
                sample: hub_settings.sample as u32,
                energy_mode: hub_settings.energy_mode,
            }))
        }
        ServerMessage::FromHubSettingsAck(hub_settings_ack) => {
            debug!("serializando mensaje FromHubSettingsAck para el servidor");
            Some(Payload::SettingOk(SettOk {
                metadata: Some(grpc::Metadata {
                    sender_user_id: hub_settings_ack.metadata.sender_user_id,
                    destination_id: hub_settings_ack.metadata.destination_id,
                    timestamp: hub_settings_ack.metadata.timestamp,
                }),
                message_id: hub_settings_ack.message_id,
                network: hub_settings_ack.network,
                handshake: hub_settings_ack.handshake,
            }))
        }
        ServerMessage::FirmwareOutcome(firmware_outcome) => {
            debug!("serializando mensaje FirmwareOutcome para el servidor");
            Some(Payload::FirmwareOutcome(FirmwareOutcome {
                metadata: Some(grpc::Metadata {
                    sender_user_id: firmware_outcome.metadata.sender_user_id,
                    destination_id: firmware_outcome.metadata.destination_id,
                    timestamp: firmware_outcome.metadata.timestamp,
                }),
                network: firmware_outcome.network,
                percentage_ok: firmware_outcome.percentage_ok,
            }))
        }
        ServerMessage::FirmwareOutcomeError(firmware_outcome) => {
            debug!("serializando mensaje FirmwareOutcomeError para el servidor");
            Some(Payload::OutcomeError(FirmwareOutcomeError {
                metadata: Some(grpc::Metadata {
                    sender_user_id: firmware_outcome.metadata.sender_user_id,
                    destination_id: firmware_outcome.metadata.destination_id,
                    timestamp: firmware_outcome.metadata.timestamp,
                }),
                network: firmware_outcome.network,
                error: firmware_outcome.error,
            }))
        }
        ServerMessage::Report(report) => {
            debug!("serializando mensaje Report para el servidor");
            Some(Payload::Measurement(grpc::Measurement {
                metadata: Some(grpc::Metadata {
                    sender_user_id: report.metadata.sender_user_id,
                    destination_id: report.metadata.destination_id,
                    timestamp: report.metadata.timestamp,
                }),
                network: report.network, // Asegúrate de mapear este campo requerido por el proto
                pulse_counter: report.pulse_counter,
                temperature: report.temperature,
                humidity: report.humidity,
                air_quality: report.air_quality,
                sample: report.sample as u32,
            }))
        }
        ServerMessage::Monitor(monitor) => {
            debug!("serializando mensaje Monitor para el servidor");
            Some(Payload::Monitor(grpc::Monitor {
                metadata: Some(grpc::Metadata {
                    sender_user_id: monitor.metadata.sender_user_id,
                    destination_id: monitor.metadata.destination_id,
                    timestamp: monitor.metadata.timestamp,
                }),
                network: monitor.network,
                heap_free: monitor.heap_free,
                heap_min_free: monitor.heap_min_free,
                heap_largest_block: monitor.heap_largest_block,
                uptime_sec: monitor.uptime_sec as u64,
            }))
        }
        ServerMessage::AlertAir(alert_air) => {
            debug!("serializando mensaje AlertAir para el servidor");
            Some(Payload::AlertAir(grpc::AlertAir {
                metadata: Some(grpc::Metadata {
                    sender_user_id: alert_air.metadata.sender_user_id,
                    destination_id: alert_air.metadata.destination_id,
                    timestamp: alert_air.metadata.timestamp,
                }),
                network: alert_air.network,
                initial_air_quality: alert_air.initial_air_quality,
                actual_air_quality: alert_air.actual_air_quality,
            }))
        }
        ServerMessage::AlertTem(alert_tem) => {
            debug!("serializando mensaje AlertTem para el servidor");
            Some(Payload::AlertTh(grpc::AlertTh {
                metadata: Some(grpc::Metadata {
                    sender_user_id: alert_tem.metadata.sender_user_id,
                    destination_id: alert_tem.metadata.destination_id,
                    timestamp: alert_tem.metadata.timestamp,
                }),
                network: alert_tem.network,
                initial_temp: alert_tem.initial_temp,
                actual_temp: alert_tem.actual_temp,
            }))
        }
        ServerMessage::Metrics(metrics) => {
            debug!("serializando mensaje Metrics para el servidor");
            Some(Payload::Metrics(SystemMetrics {
                metadata: Some(grpc::Metadata {
                    sender_user_id: metrics.metadata.sender_user_id,
                    destination_id: metrics.metadata.destination_id,
                    timestamp: metrics.metadata.timestamp,
                }),
                uptime_seconds: metrics.uptime_seconds,
                cpu_usage_percent: metrics.cpu_usage_percent,
                cpu_temp_celsius: metrics.cpu_temp_celsius,
                ram_total_mb: metrics.ram_total_mb,
                ram_used_mb: metrics.ram_used_mb,
                ram_used_by_service_mb: metrics.ram_used_by_service_mb,
                sd_total_gb: metrics.sd_total_gb,
                sd_used_gb: metrics.sd_used_gb,
                sd_usage_percent: metrics.sd_usage_percent,
                network_rx_bytes: metrics.network_rx_bytes,
                network_tx_bytes: metrics.network_tx_bytes,
                wifi_rssi: metrics.wifi_rssi.unwrap_or(0),
                wifi_signal_dbm: metrics.wifi_signal_dbm.unwrap_or(0),
            }))
        }
        ServerMessage::ReportBatch(reports) => {
            debug!("serializando mensaje ReportBatch para el servidor");
            let proto_measurements = reports
                .into_iter()
                .map(|r| grpc::Measurement {
                    metadata: Some(grpc::Metadata {
                        sender_user_id: r.metadata.sender_user_id,
                        destination_id: r.metadata.destination_id,
                        timestamp: r.metadata.timestamp,
                    }),
                    network: r.network,
                    pulse_counter: r.pulse_counter,
                    temperature: r.temperature,
                    humidity: r.humidity,
                    air_quality: r.air_quality,
                    sample: r.sample as u32,
                })
                .collect();

            Some(Payload::MeasurementBatch(grpc::MeasurementBatch {
                measurements: proto_measurements,
            }))
        }
        ServerMessage::MonitorBatch(monitors) => {
            debug!("serializando mensaje MonitorBatch para el servidor");
            let proto_monitors = monitors
                .into_iter()
                .map(|m| grpc::Monitor {
                    metadata: Some(grpc::Metadata {
                        sender_user_id: m.metadata.sender_user_id,
                        destination_id: m.metadata.destination_id,
                        timestamp: m.metadata.timestamp,
                    }),
                    network: m.network,
                    heap_free: m.heap_free,
                    heap_min_free: m.heap_min_free,
                    heap_largest_block: m.heap_largest_block,
                    uptime_sec: m.uptime_sec as u64,
                })
                .collect();

            Some(Payload::MonitorBatch(grpc::MonitorBatch {
                monitors: proto_monitors,
            }))
        }
        ServerMessage::AlertAirBatch(alerts) => {
            debug!("serializando mensaje AlertAirBatch para el servidor");
            let proto_alerts = alerts
                .into_iter()
                .map(|a| grpc::AlertAir {
                    metadata: Some(grpc::Metadata {
                        sender_user_id: a.metadata.sender_user_id,
                        destination_id: a.metadata.destination_id,
                        timestamp: a.metadata.timestamp,
                    }),
                    network: a.network,
                    initial_air_quality: a.initial_air_quality,
                    actual_air_quality: a.actual_air_quality,
                })
                .collect();

            Some(Payload::AlertAirBatch(grpc::AlertAirBatch {
                alerts: proto_alerts,
            }))
        }
        ServerMessage::AlertTemBatch(alerts) => {
            debug!("serializando mensaje AlertTemBatch para el servidor");
            let proto_alerts = alerts
                .into_iter()
                .map(|a| grpc::AlertTh {
                    metadata: Some(grpc::Metadata {
                        sender_user_id: a.metadata.sender_user_id,
                        destination_id: a.metadata.destination_id,
                        timestamp: a.metadata.timestamp,
                    }),
                    network: a.network,
                    initial_temp: a.initial_temp,
                    actual_temp: a.actual_temp,
                })
                .collect();

            Some(Payload::AlertThBatch(grpc::AlertThBatch {
                alerts: proto_alerts,
            }))
        }
        ServerMessage::UpdateEdgeFirmware(update) => {
            debug!("serializando mensaje UpdateEdgeFirmware para el servidor");
            Some(Payload::UpdateEdgeFirmware(UpdateEdge {
                metadata: Some(grpc::Metadata {
                    sender_user_id: update.metadata.sender_user_id,
                    destination_id: update.metadata.destination_id,
                    timestamp: update.metadata.timestamp,
                }),
                version: update.version,
            }))
        }
        _ => None,
    };

    generate_edge_upload(payload, edge_id.to_string())
}

/// Envuelve el `Payload` gRPC validado en la estructura final de transmisión `FromEdge`.
fn generate_edge_upload(payload: Option<Payload>, edge_id: String) -> Option<FromEdge> {
    if let Some(p) = payload {
        Some(FromEdge {
            edge_id,
            payload: Some(p),
        })
    } else {
        None
    }
}

/// Itera y procesa por lotes los datos históricos recuperados de la base de datos local.
///
/// Se ejecuta cuando el patrón "Store and Forward" detecta que la red regresó y
/// la base de datos inyecta un `TableDataVector` (Batch). Convierte cada fila a gRPC
/// y las encola para envío.
async fn process_batch(
    batch: TableDataVector,
    tx: &mpsc::Sender<MessageServiceResponse>,
    system: &System,
) -> Result<(), ()> {
    if !batch.measurement.is_empty() {
        if let Some(proto_msg) = convert_to_proto_upload(
            ServerMessage::ReportBatch(batch.measurement),
            system.id_edge.clone(),
        ) {
            if tx
                .send(MessageServiceResponse::EdgeUpload(proto_msg))
                .await
                .is_err()
            {
                error!("no se puede enviar batch de measurements al cliente gRPC");
            }
        }
    }

    if !batch.monitor.is_empty() {
        if let Some(proto_msg) = convert_to_proto_upload(
            ServerMessage::MonitorBatch(batch.monitor),
            system.id_edge.clone(),
        ) {
            if tx
                .send(MessageServiceResponse::EdgeUpload(proto_msg))
                .await
                .is_err()
            {
                error!("no se puede enviar batch de monitors al cliente gRPC");
            }
        }
    }

    if !batch.alert_air.is_empty() {
        if let Some(proto_msg) = convert_to_proto_upload(
            ServerMessage::AlertAirBatch(batch.alert_air),
            system.id_edge.clone(),
        ) {
            if tx
                .send(MessageServiceResponse::EdgeUpload(proto_msg))
                .await
                .is_err()
            {
                error!("no se puede enviar batch de alert_air al cliente gRPC");
            }
        }
    }

    if !batch.alert_th.is_empty() {
        if let Some(proto_msg) = convert_to_proto_upload(
            ServerMessage::AlertTemBatch(batch.alert_th),
            system.id_edge.clone(),
        ) {
            if tx
                .send(MessageServiceResponse::EdgeUpload(proto_msg))
                .await
                .is_err()
            {
                error!("no se puede enviar batch de alert_th al cliente gRPC");
            }
        }
    }

    Ok(())
}

// -------------------------------------------------------------------------------------------------

/// Procesador de mensajes entrantes del Servidor (Downlink Remoto).
///
/// Recibe eventos crudos de la nube (vía el motor gRPC) y los clasifica.
///
/// # Funcionalidad
///
/// Extrae la carga útil (`Payload`) de los mensajes Protobuf y delega la traducción
/// al dominio a la función `handle_grpc_message`.
#[instrument(name = "msg_from_server", skip_all)]
pub async fn msg_from_server(
    tx: mpsc::Sender<MessageServiceResponse>,
    tx_to_msg_to_hub: mpsc::Sender<ServerMessage>,
    mut rx: mpsc::Receiver<MessageServiceCommand>,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("shutdown recibido msg_from_server");
                break;
            }

            Some(msg) = rx.recv() => {
                match msg {
                    MessageServiceCommand::Internal(internal) => {
                        match internal {
                            InternalEvent::IncomingGrpc(edge_download) => {
                                handle_grpc_message(edge_download,
                                            &tx,
                                            &tx_to_msg_to_hub).await;
                            },
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

/// Convierte los mensajes gRPC (`ToEdge`) entrantes en tipos nativos de dominio (`ServerMessage`)
/// y los enruta al destino adecuado (Hub local o procesos internos del Edge).
async fn handle_grpc_message(
    proto_msg: ToEdge,
    tx: &mpsc::Sender<MessageServiceResponse>,
    tx_to_msg_to_hub: &mpsc::Sender<ServerMessage>,
) {
    if let Some(payload) = proto_msg.payload {
        match payload {
            to_edge::Payload::UpdateFirmware(update_firmware) => {
                debug!("mensaje UpdateFirmware entrante desde el servidor");
                let msg = UpdateFirmware {
                    metadata: extract_metadata(update_firmware.metadata),
                    network: update_firmware.network,
                };
                if tx
                    .send(MessageServiceResponse::FromServer(
                        ServerMessage::UpdateFirmware(msg),
                    ))
                    .await
                    .is_err()
                {
                    error!("no se pudo enviar mensaje UpdateFirmware a firmware");
                }
            }
            to_edge::Payload::Settings(settings) => {
                debug!("mensaje Settings entrante desde el servidor");
                let msg = Settings {
                    metadata: extract_metadata(settings.metadata),
                    message_id: settings.message_id,
                    network: settings.network,
                    wifi_ssid: settings.wifi_ssid,
                    wifi_password: settings.wifi_password,
                    mqtt_uri: settings.mqtt_uri,
                    device_name: settings.device_name,
                    sample: settings.sample as u16,
                    energy_mode: settings.energy_mode,
                };
                if tx
                    .send(MessageServiceResponse::FromServer(
                        ServerMessage::FromServerSettings(msg),
                    ))
                    .await
                    .is_err()
                {
                    error!("no se pudo enviar mensaje a network");
                }
            }
            to_edge::Payload::DeleteHub(delete) => {
                debug!("mensaje DeleteHub entrante desde el servidor");
                let msg = DeleteHub {
                    metadata: extract_metadata(delete.metadata),
                    network: delete.network,
                };
                if tx
                    .send(MessageServiceResponse::FromServer(
                        ServerMessage::DeleteHub(msg),
                    ))
                    .await
                    .is_err()
                {
                    error!("no se pudo enviar mensaje a network");
                }
            }
            to_edge::Payload::SettingOk(setting_ok) => {
                debug!("mensaje SettingOk entrante desde el servidor");
                let msg = SettingOk {
                    metadata: extract_metadata(setting_ok.metadata),
                    message_id: setting_ok.message_id,
                    network: setting_ok.network,
                    handshake: setting_ok.handshake,
                };
                if tx_to_msg_to_hub
                    .send(ServerMessage::FromServerSettingsAck(msg))
                    .await
                    .is_err()
                {
                    error!("no se pudo enviar mensaje al hub");
                }
            }
            to_edge::Payload::Network(network) => {
                debug!("mensaje Network entrante desde el servidor");
                let msg = Network {
                    metadata: extract_metadata(network.metadata),
                    id_network: network.id_network,
                    name_network: network.name_network,
                    active: network.active,
                    delete_network: network.delete_network,
                };
                if tx
                    .send(MessageServiceResponse::FromServer(ServerMessage::Network(
                        msg,
                    )))
                    .await
                    .is_err()
                {
                    error!("no se pudo enviar mensaje a network");
                }
            }
            to_edge::Payload::Heartbeat(heartbeat) => {
                debug!("mensaje Heartbeat entrante desde el servidor");
                let msg = HeartbeatMsg {
                    metadata: extract_metadata(heartbeat.metadata),
                    beat: true,
                };
                if tx
                    .send(MessageServiceResponse::FromServer(
                        ServerMessage::Heartbeat(msg),
                    ))
                    .await
                    .is_err()
                {
                    error!("no se pudo enviar mensaje a heartbeat");
                }
            }
            to_edge::Payload::HelloWorld(hello_ack) => {
                debug!("mensaje HelloWorldAck entrante desde el servidor");
                let msg = HelloWorld {
                    metadata: extract_metadata(hello_ack.metadata),
                    hello: hello_ack.hello,
                };
                if tx
                    .send(MessageServiceResponse::FromServer(
                        ServerMessage::HelloWorld(msg),
                    ))
                    .await
                    .is_err()
                {
                    error!("no se pudo enviar mensaje HelloWorld a la fsm general");
                }
            }
            to_edge::Payload::UpdateEdgeFirmware(update) => {
                debug!("mensaje UpdateEdgeFirmware entrante desde el servidor");
                let msg = UpdateEdgeFirmware {
                    metadata: extract_metadata(update.metadata),
                    version: update.version,
                };
                if tx
                    .send(MessageServiceResponse::FromServer(
                        ServerMessage::UpdateEdgeFirmware(msg),
                    ))
                    .await
                    .is_err()
                {
                    error!("no se pudo enviar mensaje HelloWorld a la fsm general");
                }
            }
        }
    }
}

/// Helper para convertir la metadata de red generada por Protobuf en la estructura
/// plana de `Metadata` usada en el dominio del negocio.
fn extract_metadata(proto_meta: Option<grpc::Metadata>) -> Metadata {
    let meta = proto_meta.unwrap_or_default();
    Metadata {
        sender_user_id: meta.sender_user_id,
        destination_id: meta.destination_id,
        timestamp: meta.timestamp,
    }
}

// -------------------------------------------------------------------------------------------------

/// Utilidad genérica de serialización y encolamiento para el Downlink Local.
///
/// Serializa cualquier estructura de dominio (T) a un arreglo de bytes utilizando **MessagePack**
/// y la empaqueta en un objeto [`SerializedMessage`] listo para ser procesado y publicado
/// por el cliente MQTT.
pub async fn send<T>(
    tx: &mpsc::Sender<MessageServiceResponse>,
    topic: String,
    qos: u8,
    msg: T,
    retain: bool,
) -> Result<(), ()>
where
    T: Serialize,
{
    match to_vec(&msg) {
        Ok(payload) => {
            let serialized = SerializedMessage::new(topic, payload, qos, retain);
            if tx
                .send(MessageServiceResponse::Serialized(serialized))
                .await
                .is_err()
            {
                return Err(());
            }
        }
        Err(e) => {
            error!("no se pudo serializar mensaje: {:?}", e);
        }
    }
    Ok(())
}
