use crate::context::domain::AppContext;
use crate::first_layer::grpc_service::domain::GrpcHandle;
use crate::first_layer::mqtt::domain::{MqttHandle, PayloadTopic};
use crate::grpc;
use crate::grpc::from_edge::Payload;
use crate::grpc::{
    AlertAir as AlertAirGrpc, AlertTh as AlertThGrpc, EdgeMonitor as EdgeMonitorGrpc,
    EdgeState as EdgeStateGrpc, FirmwareHubResult as FirmwareHubResultGrpc, FromEdge,
    HelloWorld as HelloWorldGrpc, HubMonitor as HubMonitorGrpc, HubState as HubStateGrpc,
    Measurement as MeasurementGrpc, NetworkAck as NetworkAckGrpc, Settings as SettingsGrpc,
    SettingsAck as SettingAckGrpc, ToEdge, UpdateEdgeFirmware as UpdateEdgeFirmwareGrpc, to_edge,
};
use crate::second_layer::database::domain::TableDataVector;
use crate::second_layer::message::domain::*;
use crate::system::domain::InternalEvent;
use rmp_serde::{from_slice, to_vec};
use serde::Serialize;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

#[derive(Serialize)]
pub enum Alert {
    AlertAir(AlertAir),
    AlertTemperature(AlertTh),
}

#[derive(Serialize)]
enum InternalMessageCommand {
    // Mensajes para los Hub
    SerializeUpdateHubFirmware { data: UpdateFirmwareRequestHub },
    SerializeNewConfigHub { data: Settings },
    SerializeAckConfigHub { data: SettingsAck },
    SerializeHeartbeatHub { data: Heartbeat },
    SerializeHandshakeHub { data: HandshakeToHub },
    SerializePhaseHub { data: PhaseNotification },
    SerializeStateHub { data: StateToHub },
    SerializeLinkageHub { data: LinkageAck },

    // Mensajes para el Server
    SerializeHubFirmwareResult { data: FirmwareHubResult },
    SerializeHelloServer { data: HelloServer },
    SerializeHubSettingsServer { data: Settings },
    SerializeHubSettingsAckServer { data: SettingsAck },
    SerializeTelemetry { data: Measurement },
    SerializeHubMonitor { data: Monitor },
    SerializeAlert { data: Alert },
    SerializeEdgeMonitor { data: SystemMetrics },
    SerializeBatch { data: TableDataVector },
    SerializeEdgeState { data: EdgeState },
    SerializeNetworkAck { data: NetworkAck },
    SerializeHubState { data: HubState },
    SerializeEdgeFirmwareResult { data: UpdateEdgeFirmware },
}

#[derive(Clone)]
pub struct MessageHandle {
    tx: mpsc::Sender<InternalMessageCommand>,
}

impl MessageHandle {
    pub async fn serialize_update_hub_firmware(&self, data: UpdateFirmwareRequestHub) {
        let cmd = InternalMessageCommand::SerializeUpdateHubFirmware { data };
        let _ = self.tx.send(cmd).await;
    }
    pub async fn serialize_new_config_hub(&self, data: Settings) {
        let cmd = InternalMessageCommand::SerializeNewConfigHub { data };
        let _ = self.tx.send(cmd).await;
    }
    pub async fn serialize_ack_config_hub(&self, data: SettingsAck) {
        let cmd = InternalMessageCommand::SerializeAckConfigHub { data };
        let _ = self.tx.send(cmd).await;
    }
    pub async fn serialize_heartbeat_hub(&self, data: Heartbeat) {
        let cmd = InternalMessageCommand::SerializeHeartbeatHub { data };
        let _ = self.tx.send(cmd).await;
    }
    pub async fn serialize_handshake_hub(&self, data: HandshakeToHub) {
        let cmd = InternalMessageCommand::SerializeHandshakeHub { data };
        let _ = self.tx.send(cmd).await;
    }
    pub async fn serialize_phase_hub(&self, data: PhaseNotification) {
        let cmd = InternalMessageCommand::SerializePhaseHub { data };
        let _ = self.tx.send(cmd).await;
    }
    pub async fn serialize_state_hub(&self, data: StateToHub) {
        let cmd = InternalMessageCommand::SerializeStateHub { data };
        let _ = self.tx.send(cmd).await;
    }
    pub async fn serialize_linkage_hub(&self, data: LinkageAck) {
        let cmd = InternalMessageCommand::SerializeLinkageHub { data };
        let _ = self.tx.send(cmd).await;
    }
    pub async fn serialize_hub_firmware_result(&self, data: FirmwareHubResult) {
        let cmd = InternalMessageCommand::SerializeHubFirmwareResult { data };
        let _ = self.tx.send(cmd).await;
    }
    pub async fn serialize_hello_server(&self, data: HelloServer) {
        let cmd = InternalMessageCommand::SerializeHelloServer { data };
        let _ = self.tx.send(cmd).await;
    }
    pub async fn serialize_hub_settings_server(&self, data: Settings) {
        let cmd = InternalMessageCommand::SerializeHubSettingsServer { data };
        let _ = self.tx.send(cmd).await;
    }
    pub async fn serialize_hub_settings_ack_server(&self, data: SettingsAck) {
        let cmd = InternalMessageCommand::SerializeHubSettingsAckServer { data };
        let _ = self.tx.send(cmd).await;
    }
    pub async fn serialize_telemetry(&self, data: Measurement) {
        let cmd = InternalMessageCommand::SerializeTelemetry { data };
        let _ = self.tx.send(cmd).await;
    }
    pub async fn serialize_hub_monitor(&self, data: Monitor) {
        let cmd = InternalMessageCommand::SerializeHubMonitor { data };
        let _ = self.tx.send(cmd).await;
    }
    pub async fn serialize_alert(&self, data: Alert) {
        let cmd = InternalMessageCommand::SerializeAlert { data };
        let _ = self.tx.send(cmd).await;
    }
    pub async fn serialize_edge_monitor(&self, data: SystemMetrics) {
        let cmd = InternalMessageCommand::SerializeEdgeMonitor { data };
        let _ = self.tx.send(cmd).await;
    }
    pub async fn serialize_batch(&self, data: TableDataVector) {
        let cmd = InternalMessageCommand::SerializeBatch { data };
        let _ = self.tx.send(cmd).await;
    }
    pub async fn serialize_edge_state(&self, data: EdgeState) {
        let cmd = InternalMessageCommand::SerializeEdgeState { data };
        let _ = self.tx.send(cmd).await;
    }
    pub async fn serialize_network_ack(&self, data: NetworkAck) {
        let cmd = InternalMessageCommand::SerializeNetworkAck { data };
        let _ = self.tx.send(cmd).await;
    }
    pub async fn serialize_hub_state(&self, data: HubState) {
        let cmd = InternalMessageCommand::SerializeHubState { data };
        let _ = self.tx.send(cmd).await;
    }
    pub async fn serialize_edge_firmware_result(&self, data: UpdateEdgeFirmware) {
        let cmd = InternalMessageCommand::SerializeEdgeFirmwareResult { data };
        let _ = self.tx.send(cmd).await;
    }
}

pub enum MessageServiceResponse {
    FromHub(HubMessage),
    FromServer(ServerMessage),
}

pub struct MessageService {
    sender: mpsc::Sender<MessageServiceResponse>,
    receiver: mpsc::Receiver<InternalEvent>,
    rx: mpsc::Receiver<InternalMessageCommand>,
    grpc_handle: GrpcHandle,
    mqtt_handle: MqttHandle,
    context: AppContext,
    msg_handle: MessageHandle,
}

impl MessageService {
    pub fn new(
        sender: mpsc::Sender<MessageServiceResponse>,
        receiver: mpsc::Receiver<InternalEvent>,
        grpc_handle: GrpcHandle,
        mqtt_handle: MqttHandle,
        context: AppContext,
    ) -> (Self, MessageHandle) {
        let (tx, rx) = mpsc::channel(50);
        let msg_handle = MessageHandle { tx };
        let service = Self {
            sender,
            receiver,
            rx,
            grpc_handle,
            mqtt_handle,
            context,
            msg_handle: msg_handle.clone(),
        };
        (service, msg_handle)
    }

    pub async fn run(mut self, shutdown: CancellationToken) {
        let mut server_status = InternalEvent::ServerConnected;
        let mut local_status = InternalEvent::LocalConnected;
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("shutdown recibido MessageService");
                    break;
                }

                Some(msg) = self.rx.recv() => {
                    match msg {
                        InternalMessageCommand::SerializeUpdateHubFirmware { ref data } => {
                            let id_net = data.network.clone();
                            let topic = {
                                let manager = self.context.net_man.read().await;
                                if let Some(n) = manager.networks.get(&id_net) {
                                    Some(n.topic_new_firmware.clone())
                                } else {
                                    None
                                }
                            };
                            if let Some(t) = topic {
                                match send(&self.mqtt_handle, t.topic, t.qos, msg, false).await {
                                    Ok(_) => {},
                                    Err(_) => error!("no se pudo serializar el mensaje UpdateHubFirmware"),
                                }
                            }
                        }
                        InternalMessageCommand::SerializeNewConfigHub { ref data } => {
                            let id_net = data.network.clone();
                            let topic = {
                                let manager = self.context.net_man.read().await;
                                if let Some(n) = manager.networks.get(&id_net) {
                                    Some(n.topic_new_setting.clone())
                                } else {
                                    None
                                }
                            };
                            if let Some(t) = topic {
                                match send(&self.mqtt_handle, t.topic, t.qos, msg, false).await {
                                    Ok(_) => {},
                                    Err(_) => error!("no se pudo serializar el mensaje NewConfigHub"),
                                }
                            }
                        }
                        InternalMessageCommand::SerializeAckConfigHub { ref data } => {
                            let id_net = data.network.clone();
                            let topic = {
                                let manager = self.context.net_man.read().await;
                                if let Some(n) = manager.networks.get(&id_net) {
                                    Some(n.topic_setting_ok.clone())
                                } else {
                                    None
                                }
                            };
                            if let Some(t) = topic {
                                match send(&self.mqtt_handle, t.topic, t.qos, msg, false).await {
                                    Ok(_) => {},
                                    Err(_) => error!("no se pudo serializar el mensaje AckConfigHub"),
                                }
                            }
                        }
                        InternalMessageCommand::SerializeHeartbeatHub { data } => {
                            let topic = {
                                let manager = self.context.net_man.read().await;
                                Some((
                                    manager.topic_heartbeat.topic.clone(),
                                    manager.topic_heartbeat.qos,
                                    false,
                                ))
                            };
                            if let Some((t, qos, retain)) = topic {
                                match send(&self.mqtt_handle, t.clone(), qos, data, retain).await {
                                    Ok(_) => {}
                                    Err(_) => error!("no se pudo serializar el mensaje HeartbeatHub"),
                                }
                            }
                        }
                        InternalMessageCommand::SerializeHandshakeHub { data } => {
                            let topic = {
                                let manager = self.context.net_man.read().await;
                                Some((
                                    manager.topic_handshake.topic.clone(),
                                    manager.topic_handshake.qos,
                                    false,
                                ))
                            };
                            if let Some((t, qos, retain)) = topic {
                                match send(&self.mqtt_handle, t.clone(), qos, data, retain).await {
                                    Ok(_) => {}
                                    Err(_) => error!("no se pudo serializar el mensaje HandshakeHub"),
                                }
                            }
                        }
                        InternalMessageCommand::SerializePhaseHub { data } => {
                            let topic = {
                                let manager = self.context.net_man.read().await;
                                Some((
                                    format!("{}/phase", manager.topic_state.topic),
                                    manager.topic_state.qos,
                                    false,
                                ))
                            };
                            if let Some((t, qos, retain)) = topic {
                                match send(&self.mqtt_handle, t.clone(), qos, data, retain).await {
                                    Ok(_) => {}
                                    Err(_) => error!("no se pudo serializar el mensaje PhaseHub"),
                                }
                            }
                        }
                        InternalMessageCommand::SerializeStateHub { data } => {
                            let topic = {
                                let manager = self.context.net_man.read().await;
                                Some((
                                    format!("{}/edge_state", manager.topic_state.topic),
                                    manager.topic_state.qos,
                                    false,
                                ))
                            };
                            if let Some((t, qos, retain)) = topic {
                                match send(&self.mqtt_handle, t.clone(), qos, data, retain).await {
                                    Ok(_) => {}
                                    Err(_) => error!("no se pudo serializar el mensaje StateHub"),
                                }
                            }
                        }
                        InternalMessageCommand::SerializeLinkageHub { data } => {
                            let topic = {
                                let manager = self.context.net_man.read().await;
                                Some((
                                    manager.topic_linkage_ack.topic.clone(),
                                    manager.topic_linkage_ack.qos,
                                    false,
                                ))
                            };
                            if let Some((t, qos, retain)) = topic {
                                match send(&self.mqtt_handle, t.clone(), qos, data, retain).await {
                                    Ok(_) => {}
                                    Err(_) => error!("no se pudo serializar el mensaje LinkageHub"),
                                }
                            }
                        }
                        _ => {
                            let edge_id = self.context.system.id_edge.clone();
                            convert_to_proto_upload(&self.grpc_handle, msg, edge_id).await;
                        }
                    }
                }

                // Mensajes provenientes de MqttService y GrpcService para ser parseados
                Some(to_parse) = self.receiver.recv() => {
                    match to_parse {
                        InternalEvent::IncomingMessage(data) => {
                            handle_mqtt_message(
                                &self.msg_handle,
                                data,
                                &self.sender,
                                &self.context,
                                &server_status
                            ).await;
                        }
                        InternalEvent::IncomingGrpc(data) => {
                            handle_grpc_message(
                                &self.msg_handle,
                                &local_status,
                                data,
                                &self.sender).await;
                        }
                        InternalEvent::ServerDisconnected | InternalEvent::ServerConnected => {
                            server_status = to_parse;
                        }
                        InternalEvent::LocalDisconnected | InternalEvent::LocalConnected => {
                            local_status = to_parse;
                        }
                    }
                }
            }
        }
    }
}

/// Utilidad genérica de serialización.
///
/// Serializa cualquier estructura de dominio (T) a un arreglo de bytes utilizando **MessagePack**
/// y la empaqueta en un objeto [`SerializedMessage`] listo para ser procesado y publicado
/// por el cliente MQTT.
pub async fn send<T>(
    handle: &MqttHandle,
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
            handle.send_serialized(serialized).await;
        }
        Err(e) => {
            error!("no se pudo serializar mensaje: {:?}", e);
        }
    }
    Ok(())
}

/// Mapea las estructuras internas de dominio (`ServerMessage`) a los tipos generados
/// por Protobuf (`FromEdge` / `Payload`).
///
/// Este es el punto de traducción formal antes de que la capa de red gRPC envíe los bytes.
async fn convert_to_proto_upload(
    handle: &GrpcHandle,
    msg: InternalMessageCommand,
    edge_id: String,
) {
    /*
    * let metadata = Metadata {
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
    */
    match msg {
        InternalMessageCommand::SerializeHubFirmwareResult { data } => {
            let payload = {
                debug!("serializando mensaje HubFirmwareResult para el servidor");
                Some(Payload::FirmwareHubResult(FirmwareHubResultGrpc {
                    metadata: Some(grpc::Metadata {
                        sender_user_id: data.metadata.sender_user_id,
                        destination_id: data.metadata.destination_id,
                        timestamp: data.metadata.timestamp,
                    }),
                    network: data.network,
                    percentage_ok: data.percentage_ok,
                    error: data.error,
                }))
            };
            generate_edge_upload(handle, payload, edge_id).await;
        }
        InternalMessageCommand::SerializeEdgeFirmwareResult { data } => {
            let payload = {
                debug!("serializando mensaje UpdateEdgeFirmware para el servidor");
                Some(Payload::UpdateEdgeFirmware(UpdateEdgeFirmwareGrpc {
                    metadata: Some(grpc::Metadata {
                        sender_user_id: data.metadata.sender_user_id,
                        destination_id: data.metadata.destination_id,
                        timestamp: data.metadata.timestamp,
                    }),
                    version: data.version,
                }))
            };
            generate_edge_upload(handle, payload, edge_id).await;
        }
        InternalMessageCommand::SerializeHelloServer { data } => {
            let payload = {
                debug!("serializando mensaje HelloWorld para el servidor");
                Some(Payload::HelloWorld(HelloWorldGrpc {
                    metadata: Some(grpc::Metadata {
                        sender_user_id: data.metadata.sender_user_id,
                        destination_id: data.metadata.destination_id,
                        timestamp: data.metadata.timestamp,
                    }),
                    hello: data.hello,
                }))
            };
            generate_edge_upload(handle, payload, edge_id).await;
        }
        InternalMessageCommand::SerializeHubSettingsServer { data } => {
            let payload = {
                debug!("serializando mensaje HubSettingsServer para el servidor");
                Some(Payload::Settings(SettingsGrpc {
                    metadata: Some(grpc::Metadata {
                        sender_user_id: data.metadata.sender_user_id,
                        destination_id: data.metadata.destination_id,
                        timestamp: data.metadata.timestamp,
                    }),
                    message_id: data.message_id,
                    network: data.network,
                    wifi_ssid: data.wifi_ssid,
                    wifi_password: data.wifi_password,
                    mqtt_uri: data.mqtt_uri,
                    device_name: data.device_name,
                    sample: data.sample as u32,
                    energy_mode: data.energy_mode,
                }))
            };
            generate_edge_upload(handle, payload, edge_id).await;
        }
        InternalMessageCommand::SerializeHubSettingsAckServer { data } => {
            let payload = {
                debug!("serializando mensaje HubSettingsAckServer para el servidor");
                Some(Payload::SettingsAck(SettingAckGrpc {
                    metadata: Some(grpc::Metadata {
                        sender_user_id: data.metadata.sender_user_id,
                        destination_id: data.metadata.destination_id,
                        timestamp: data.metadata.timestamp,
                    }),
                    message_id: data.message_id,
                    network: data.network,
                    handshake: data.handshake,
                }))
            };
            generate_edge_upload(handle, payload, edge_id).await;
        }
        InternalMessageCommand::SerializeTelemetry { data } => {
            let payload = {
                debug!("serializando mensaje Report para el servidor");
                Some(Payload::Measurement(MeasurementGrpc {
                    metadata: Some(grpc::Metadata {
                        sender_user_id: data.metadata.sender_user_id,
                        destination_id: data.metadata.destination_id,
                        timestamp: data.metadata.timestamp,
                    }),
                    network: data.network,
                    pulse_counter: data.pulse_counter,
                    temperature: data.temperature,
                    humidity: data.humidity,
                    air_quality: data.air_quality,
                    sample: data.sample as u32,
                }))
            };
            generate_edge_upload(handle, payload, edge_id).await;
        }
        InternalMessageCommand::SerializeHubMonitor { data } => {
            let payload = {
                debug!("serializando mensaje Monitor para el servidor");
                Some(Payload::HubMonitor(HubMonitorGrpc {
                    metadata: Some(grpc::Metadata {
                        sender_user_id: data.metadata.sender_user_id,
                        destination_id: data.metadata.destination_id,
                        timestamp: data.metadata.timestamp,
                    }),
                    network: data.network,
                    heap_free: data.heap_free,
                    heap_min_free: data.heap_min_free,
                    heap_largest_block: data.heap_largest_block,
                    uptime_sec: data.uptime_sec as u64,
                }))
            };
            generate_edge_upload(handle, payload, edge_id).await;
        }
        InternalMessageCommand::SerializeAlert { data } => {
            let payload = match data {
                Alert::AlertAir(alert_air) => {
                    debug!("serializando mensaje AlertAir para el servidor");
                    Some(Payload::AlertAir(AlertAirGrpc {
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
                Alert::AlertTemperature(alert_tem) => {
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
            };
            generate_edge_upload(handle, payload, edge_id).await;
        }
        InternalMessageCommand::SerializeEdgeMonitor { data } => {
            let payload = {
                debug!("serializando mensaje Metrics para el servidor");
                Some(Payload::EdgeMonitor(EdgeMonitorGrpc {
                    metadata: Some(grpc::Metadata {
                        sender_user_id: data.metadata.sender_user_id,
                        destination_id: data.metadata.destination_id,
                        timestamp: data.metadata.timestamp,
                    }),
                    uptime_seconds: data.uptime_seconds,
                    cpu_usage_percent: data.cpu_usage_percent,
                    cpu_temp_celsius: data.cpu_temp_celsius,
                    ram_total_mb: data.ram_total_mb,
                    ram_used_mb: data.ram_used_mb,
                    ram_used_by_service_mb: data.ram_used_by_service_mb,
                    sd_total_gb: data.sd_total_gb,
                    sd_used_gb: data.sd_used_gb,
                    sd_usage_percent: data.sd_usage_percent,
                    network_rx_bytes: data.network_rx_bytes,
                    network_tx_bytes: data.network_tx_bytes,
                    wifi_rssi: data.wifi_rssi.unwrap_or(0),
                    wifi_signal_dbm: data.wifi_signal_dbm.unwrap_or(0),
                }))
            };
            generate_edge_upload(handle, payload, edge_id).await;
        }
        InternalMessageCommand::SerializeBatch { data } => {
            if !data.measurement.is_empty() {
                generate_batch_measurement(handle, data.measurement, edge_id.clone()).await;
            }
            if !data.monitor.is_empty() {
                generate_batch_monitor(handle, data.monitor, edge_id.clone()).await;
            }
            if !data.alert_air.is_empty() {
                generate_batch_alert_air(handle, data.alert_air, edge_id.clone()).await;
            }
            if !data.alert_th.is_empty() {
                generate_batch_alert_tem(handle, data.alert_th, edge_id.clone()).await;
            }
        }
        InternalMessageCommand::SerializeEdgeState { data } => {
            let payload = {
                debug!("serializando mensaje EdgeState para el servidor");
                Some(Payload::EdgeState(EdgeStateGrpc {
                    metadata: Some(grpc::Metadata {
                        sender_user_id: data.metadata.sender_user_id,
                        destination_id: data.metadata.destination_id,
                        timestamp: data.metadata.timestamp,
                    }),
                    state: data.state,
                }))
            };
            generate_edge_upload(handle, payload, edge_id).await;
        }
        InternalMessageCommand::SerializeNetworkAck { data } => {
            let payload = {
                debug!("serializando mensaje NetworkAck para el servidor");
                Some(Payload::NetworkAck(NetworkAckGrpc {
                    metadata: Some(grpc::Metadata {
                        sender_user_id: data.metadata.sender_user_id,
                        destination_id: data.metadata.destination_id,
                        timestamp: data.metadata.timestamp,
                    }),
                    id_network: data.id_network,
                    code_of_ack: data.code_of_ack,
                }))
            };
            generate_edge_upload(handle, payload, edge_id).await;
        }
        InternalMessageCommand::SerializeHubState { data } => {
            let payload = {
                debug!("serializando mensaje HubState para el servidor");
                Some(Payload::HubState(HubStateGrpc {
                    metadata: Some(grpc::Metadata {
                        sender_user_id: data.metadata.sender_user_id,
                        destination_id: data.metadata.destination_id,
                        timestamp: data.metadata.timestamp,
                    }),
                    network: data.network,
                    state: data.state,
                }))
            };
            generate_edge_upload(handle, payload, edge_id).await;
        }
        _ => {}
    }
}

async fn generate_batch_measurement(
    handle: &GrpcHandle,
    measurements: Vec<Measurement>,
    edge_id: String,
) {
    let payload = {
        debug!("serializando mensaje ReportBatch para el servidor");
        let proto_measurements = measurements
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
    };

    generate_edge_upload(handle, payload, edge_id.to_string()).await;
}

async fn generate_batch_monitor(handle: &GrpcHandle, monitors: Vec<Monitor>, edge_id: String) {
    let payload = {
        debug!("serializando mensaje MonitorBatch para el servidor");
        let proto_monitors = monitors
            .into_iter()
            .map(|m| HubMonitorGrpc {
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
    };

    generate_edge_upload(handle, payload, edge_id.to_string()).await;
}

async fn generate_batch_alert_air(handle: &GrpcHandle, alerts: Vec<AlertAir>, edge_id: String) {
    let payload = {
        debug!("serializando mensaje AlertAirBatch para el servidor");
        let proto_alerts = alerts
            .into_iter()
            .map(|a| AlertAirGrpc {
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
    };

    generate_edge_upload(handle, payload, edge_id.to_string()).await;
}

async fn generate_batch_alert_tem(handle: &GrpcHandle, alerts: Vec<AlertTh>, edge_id: String) {
    let payload = {
        debug!("serializando mensaje AlertTemBatch para el servidor");
        let proto_alerts = alerts
            .into_iter()
            .map(|a| AlertThGrpc {
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
    };

    generate_edge_upload(handle, payload, edge_id.to_string()).await;
}

/// Envuelve el `Payload` gRPC validado en la estructura final de transmisión `FromEdge`.
async fn generate_edge_upload(handle: &GrpcHandle, payload: Option<Payload>, edge_id: String) {
    if let Some(p) = payload {
        let data = Some(FromEdge {
            edge_id,
            payload: Some(p),
        });
        match data {
            Some(d) => handle.send_serialized(d).await,
            None => {}
        }
    }
}

pub async fn handle_mqtt_message(
    msg_handle: &MessageHandle,
    message: PayloadTopic,
    tx: &mpsc::Sender<MessageServiceResponse>,
    app_context: &AppContext,
    server: &InternalEvent,
) {
    let topic = message.topic;
    let payload = message.payload;

    // Enrutar explícitamente según el sufijo del tópico
    let decoded: Option<HubMessage> = if topic.ends_with("data") {
        from_slice::<Measurement>(&payload)
            .ok()
            .map(HubMessage::Report)
    } else if topic.ends_with("hub_state") {
        from_slice::<HubState>(&payload)
            .ok()
            .map(HubMessage::HubState)
    } else if topic.ends_with("monitor") {
        from_slice::<Monitor>(&payload)
            .ok()
            .map(HubMessage::Monitor)
    } else if topic.ends_with("alert_air") {
        from_slice::<AlertAir>(&payload)
            .ok()
            .map(HubMessage::AlertAir)
    } else if topic.ends_with("alert_temp") {
        from_slice::<AlertTh>(&payload)
            .ok()
            .map(HubMessage::AlertTem)
    } else if topic.ends_with("balance_mode_handshake") {
        from_slice::<HandshakeFromHub>(&payload)
            .ok()
            .map(HubMessage::HandshakeFromHub)
    } else if topic.ends_with("hub_firmware_ok") {
        from_slice::<FirmwareHubAck>(&payload)
            .ok()
            .map(HubMessage::FirmwareOk)
    } else if topic.ends_with("hub_setting_ok") {
        from_slice::<SettingsAck>(&payload)
            .ok()
            .map(HubMessage::FromHubSettingsAck)
    } else if topic.ends_with("setting") {
        from_slice::<Settings>(&payload)
            .ok()
            .map(HubMessage::FromHubSettings)
    } else if topic.ends_with("linkage_request") {
        from_slice::<LinkageRequest>(&payload)
            .ok()
            .map(HubMessage::LinkageRequest)
    } else if topic.ends_with("empty_queue_safe") {
        from_slice::<EmptyQueueSafeMode>(&payload)
            .ok()
            .map(HubMessage::EmptyQueueSafe)
    } else if topic.ends_with("empty_queue") {
        from_slice::<EmptyQueue>(&payload)
            .ok()
            .map(HubMessage::EmptyQueue)
    } else {
        None
    };

    // Procesar el mensaje decodificado
    if let Some(decoded_msg) = decoded {
        match decoded_msg {
            HubMessage::HubState(ref hub_state) => {
                let network = hub_state.network.clone();
                let manager = app_context.net_man.read().await;
                if manager.is_active(&network) {
                    let id = hub_state.metadata.sender_user_id.clone();
                    debug!("mensaje HubState proveniente del Hub {id}");
                    if server.server_connected() {
                        msg_handle.serialize_hub_state(hub_state.clone()).await;
                    }
                }
            }
            HubMessage::Report(ref report) => {
                let network = report.network.clone();
                let manager = app_context.net_man.read().await;
                if manager.is_active(&network) {
                    let id = report.metadata.sender_user_id.clone();
                    debug!("mensaje Report proveniente del Hub {id}");
                    if server.server_connected() {
                        msg_handle.serialize_telemetry(report.clone()).await;
                    } else {
                        if tx
                            .send(MessageServiceResponse::FromHub(decoded_msg))
                            .await
                            .is_err()
                        {
                            error!("no se pudo enviar el mensaje Report");
                        }
                    }
                }
            }
            HubMessage::Monitor(ref monitor) => {
                let network = monitor.network.clone();
                let manager = app_context.net_man.read().await;
                if manager.is_active(&network) {
                    let id = monitor.metadata.sender_user_id.clone();
                    debug!("mensaje Monitor proveniente del Hub {id}");
                    if server.server_connected() {
                        msg_handle.serialize_hub_monitor(monitor.clone()).await;
                    } else {
                        if tx
                            .send(MessageServiceResponse::FromHub(decoded_msg))
                            .await
                            .is_err()
                        {
                            error!("no se pudo enviar el mensaje Monitor");
                        }
                    }
                }
            }
            HubMessage::AlertAir(ref alert) => {
                let network = alert.network.clone();
                let manager = app_context.net_man.read().await;
                if manager.is_active(&network) {
                    let id = alert.metadata.sender_user_id.clone();
                    debug!("mensaje AlertAir proveniente del Hub {id}");
                    if server.server_connected() {
                        let data = Alert::AlertAir(alert.clone());
                        msg_handle.serialize_alert(data).await;
                    } else {
                        if tx
                            .send(MessageServiceResponse::FromHub(decoded_msg))
                            .await
                            .is_err()
                        {
                            error!("no se pudo enviar el mensaje AlertAir");
                        }
                    }
                }
            }
            HubMessage::AlertTem(ref alert) => {
                let network = alert.network.clone();
                let manager = app_context.net_man.read().await;
                if manager.is_active(&network) {
                    let id = alert.metadata.sender_user_id.clone();
                    debug!("mensaje AlertTemp proveniente del Hub {id}");
                    if server.server_connected() {
                        let data = Alert::AlertTemperature(alert.clone());
                        msg_handle.serialize_alert(data).await;
                    } else {
                        if tx
                            .send(MessageServiceResponse::FromHub(decoded_msg))
                            .await
                            .is_err()
                        {
                            error!("no se pudo enviar el mensaje AlertTem");
                        }
                    }
                }
            }
            HubMessage::HandshakeFromHub(ref handshake) => {
                let id = handshake.metadata.sender_user_id.clone();
                debug!("mensaje HandshakeFromHub proveniente del Hub {id}");
                if tx
                    .send(MessageServiceResponse::FromHub(decoded_msg))
                    .await
                    .is_err()
                {
                    error!("no se pudo enviar el mensaje HandshakeFromHub");
                }
            }
            HubMessage::FirmwareOk(ref firmware) => {
                let id = firmware.metadata.sender_user_id.clone();
                debug!("mensaje FirmwareOk proveniente del Hub {id}");
                if tx
                    .send(MessageServiceResponse::FromHub(decoded_msg))
                    .await
                    .is_err()
                {
                    error!("no se pudo enviar el mensaje FirmwareOk");
                }
            }
            HubMessage::FromHubSettings(ref settings) => {
                let network = settings.network.clone();
                let manager = app_context.net_man.read().await;
                if manager.is_active(&network) {
                    let id = settings.metadata.sender_user_id.clone();
                    debug!("mensaje FromHubSettings proveniente del Hub {id}");
                    if server.server_connected() {
                        msg_handle
                            .serialize_hub_settings_server(settings.clone())
                            .await;
                    }
                }
            }
            HubMessage::FromHubSettingsAck(ref ack) => {
                let id = ack.metadata.sender_user_id.clone();
                debug!("mensaje FromHubSettingsAck proveniente del Hub {id}");
                if server.server_connected() {
                    msg_handle.serialize_ack_config_hub(ack.clone()).await;
                }
            }
            HubMessage::EmptyQueue(ref empty) => {
                let id = empty.metadata.sender_user_id.clone();
                debug!("mensaje EmptyQueue proveniente del Hub {id}");
                if tx
                    .send(MessageServiceResponse::FromHub(decoded_msg))
                    .await
                    .is_err()
                {
                    error!("no se pudo enviar el mensaje EmptyQueue");
                }
            }
            HubMessage::EmptyQueueSafe(ref empty) => {
                let id = empty.metadata.sender_user_id.clone();
                debug!("mensaje EmptyQueueSafe proveniente del Hub {id}");
                if tx
                    .send(MessageServiceResponse::FromHub(decoded_msg))
                    .await
                    .is_err()
                {
                    error!("no se pudo enviar el mensaje EmptyQueueSafe");
                }
            }
            HubMessage::LinkageRequest(ref linkage) => {
                let id = linkage.metadata.sender_user_id.clone();
                debug!("mensaje LinkageRequest proveniente del Hub {id}");
                if tx
                    .send(MessageServiceResponse::FromHub(decoded_msg))
                    .await
                    .is_err()
                {
                    error!("no se pudo enviar el mensaje LinkageRequest");
                }
            }
        }
    } else {
        error!("no se pudo deserializar el mensaje del tópico: {}", topic);
    }
}

/// Convierte los mensajes gRPC (`ToEdge`) entrantes en tipos nativos de dominio (`ServerMessage`)
/// y los enruta hacia el (`Core`) para que sea direccionado donde corresponda.
async fn handle_grpc_message(
    msg_handle: &MessageHandle,
    local: &InternalEvent,
    proto_msg: ToEdge,
    tx: &mpsc::Sender<MessageServiceResponse>,
) {
    if let Some(payload) = proto_msg.payload {
        match payload {
            to_edge::Payload::UpdateFirmware(update_firmware) => {
                debug!("mensaje UpdateFirmware entrante desde el servidor");
                let msg = UpdateHubFirmware {
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
                msg_handle.serialize_new_config_hub(msg.clone()).await;
                if tx
                    .send(MessageServiceResponse::FromServer(
                        ServerMessage::FromServerSettings(msg),
                    ))
                    .await
                    .is_err()
                {
                    error!("no se pudo enviar mensaje FromServerSettings a firmware");
                }
            }
            to_edge::Payload::SettingsAck(setting_ok) => {
                debug!("mensaje SettingOk entrante desde el servidor");
                let msg = SettingsAck {
                    metadata: extract_metadata(setting_ok.metadata),
                    message_id: setting_ok.message_id,
                    network: setting_ok.network,
                    handshake: setting_ok.handshake,
                };
                if local.local_connected() {
                    msg_handle.serialize_hub_settings_ack_server(msg).await;
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
                let msg = Heartbeat {
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
