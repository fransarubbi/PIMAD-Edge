//! Dominio de Mensajería y Modelos de Datos.
//!
//! Este módulo define las estructuras de datos fundamentales que se intercambian
//! entre los distintos componentes del sistema (Hub, Edge, Servidor).
//! Actúa como el lenguaje común para la serialización (MessagePack) y
//! la persistencia en base de datos.
//!
//! # Organización
//!
//! - **Modelos Base:** Estructuras atómicas como `Metadata`, `DestinationType`.
//! - **Payloads de Negocio:** Estructuras como `Measurement`, `Monitor`, `Alert`.
//! - **Wrappers de Transporte:** Enums y Structs contenedores (`MessageFromHub`, `MessageToHub`)
//!   que agrupan los payloads para su enrutamiento.
//! - **Utilidades:** Funciones de casting para transformar modelos de memoria en filas de base de datos (`..._row`).

use crate::context::domain::AppContext;
use crate::database::domain::TableDataVector;
use crate::grpc::FromEdge;
use crate::message::logic::{msg_from_hub, msg_from_server, msg_to_hub, msg_to_server};
use crate::network::domain::HubRow;
use crate::system::domain::InternalEvent;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

pub enum MessageServiceResponse {
    EdgeUpload(FromEdge),
    Serialized(SerializedMessage),
    FromHub(HubMessage),
    FromServer(ServerMessage),
}

pub enum MessageServiceCommand {
    GenerateHelloWorld,
    Internal(InternalEvent),
    Batch(TableDataVector),
    ToHub(HubMessage),
    ToServer(ServerMessage),
    GenerateLinkageAck(String),
    GenerateEdgeState(String),
    GenerateNetworkAck((String, u32)),
}

pub struct MessageService {
    sender: mpsc::Sender<MessageServiceResponse>,
    receiver: mpsc::Receiver<MessageServiceCommand>,
    context: AppContext,
}

impl MessageService {
    pub fn new(
        sender: mpsc::Sender<MessageServiceResponse>,
        receiver: mpsc::Receiver<MessageServiceCommand>,
        context: AppContext,
    ) -> Self {
        Self {
            sender,
            receiver,
            context,
        }
    }

    pub async fn run(mut self, shutdown: CancellationToken) {
        let (tx, mut rx) = mpsc::channel::<MessageServiceResponse>(100);
        let (tx_to_msg_to_hub, rx_internal) = mpsc::channel::<InternalEvent>(100);
        let (tx_command_to_hub, rx_command_to_hub) = mpsc::channel::<MessageServiceCommand>(100);
        let (tx_command_from_hub, rx_command_from_hub) =
            mpsc::channel::<MessageServiceCommand>(100);
        let (tx_command_to_server, rx_command_to_server) =
            mpsc::channel::<MessageServiceCommand>(100);
        let (tx_server_to_msg_to_hub, rx_server_msg) = mpsc::channel::<ServerMessage>(100);
        let (tx_from_hub_to_server, rx_from_hub) = mpsc::channel::<ServerMessage>(100);
        let (tx_command_from_server, rx_command_from_server) =
            mpsc::channel::<MessageServiceCommand>(100);

        let tx_to_mqtt_local = tx.clone();
        tokio::spawn(msg_to_hub(
            tx_to_mqtt_local,
            rx_internal,
            rx_server_msg,
            rx_command_to_hub,
            self.context.clone(),
            shutdown.clone(),
        ));

        let tx_response_from_hub = tx.clone();
        tokio::spawn(msg_from_hub(
            tx_response_from_hub,
            tx_from_hub_to_server,
            tx_to_msg_to_hub,
            rx_command_from_hub,
            self.context.clone(),
            shutdown.clone(),
        ));

        let tx_to_server = tx.clone();
        tokio::spawn(msg_to_server(
            tx_to_server,
            rx_from_hub,
            rx_command_to_server,
            self.context.clone(),
            shutdown.clone(),
        ));

        let tx_from_server = tx.clone();
        tokio::spawn(msg_from_server(
            tx_from_server,
            tx_server_to_msg_to_hub,
            rx_command_from_server,
            shutdown.clone(),
        ));

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("shutdown recibido MessageService");
                    break;
                }

                Some(cmd) = self.receiver.recv() => {
                    match cmd {
                        MessageServiceCommand::Internal(internal) => {
                            match internal {
                                InternalEvent::LocalDisconnected => {
                                    if tx_command_from_hub.send(MessageServiceCommand::Internal(internal)).await.is_err() {
                                        error!("no se pudo enviar el mensaje LocalDisconnected a msg_from_hub");
                                    }
                                },
                                InternalEvent::LocalConnected => {
                                    if tx_command_from_hub.send(MessageServiceCommand::Internal(internal)).await.is_err() {
                                        error!("no se pudo enviar el mensaje LocalConnected a msg_from_hub");
                                    }
                                },
                                InternalEvent::ServerConnected => {
                                    if tx_command_from_hub.send(MessageServiceCommand::Internal(internal.clone())).await.is_err() {
                                        error!("no se pudo enviar el mensaje ServerConnected a msg_from_hub");
                                    }
                                    if tx_command_to_server.send(MessageServiceCommand::Internal(internal)).await.is_err() {
                                        error!("no se pudo enviar el mensaje ServerConnected a msg_to_server");
                                    }
                                },
                                InternalEvent::ServerDisconnected => {
                                    if tx_command_from_hub.send(MessageServiceCommand::Internal(internal.clone())).await.is_err() {
                                        error!("no se pudo enviar el mensaje ServerDisconnected a msg_from_hub");
                                    }
                                    if tx_command_to_server.send(MessageServiceCommand::Internal(internal)).await.is_err() {
                                        error!("no se pudo enviar el mensaje ServerDisconnected a msg_to_server");
                                    }
                                },
                                InternalEvent::IncomingMessage(_) => {
                                    if tx_command_from_hub.send(MessageServiceCommand::Internal(internal)).await.is_err() {
                                        error!("no se pudo enviar el mensaje IncomingMessage a msg_from_hub");
                                    }
                                },
                                InternalEvent::IncomingGrpc(_) => {
                                    if tx_command_from_server.send(MessageServiceCommand::Internal(internal)).await.is_err() {
                                        error!("no se pudo enviar el mensaje IncomingGrpc a msg_from_server");
                                    }
                                }
                            }
                        },
                        MessageServiceCommand::ToHub(to_hub) => {
                            match to_hub {
                                HubMessage::HandshakeToHub(_) => {
                                    if tx_command_to_hub.send(MessageServiceCommand::ToHub(to_hub)).await.is_err() {
                                        error!("no se pudo enviar mensaje a msg_to_hub");
                                    }
                                },
                                HubMessage::StateToHub(_) => {
                                    if tx_command_to_hub.send(MessageServiceCommand::ToHub(to_hub)).await.is_err() {
                                        error!("no se pudo enviar mensaje a msg_to_hub");
                                    }
                                },
                                HubMessage::PhaseNotification(_) => {
                                    if tx_command_to_hub.send(MessageServiceCommand::ToHub(to_hub)).await.is_err() {
                                        error!("no se pudo enviar mensaje a msg_to_hub");
                                    }
                                },
                                HubMessage::Heartbeat(_) => {
                                    if tx_command_to_hub.send(MessageServiceCommand::ToHub(to_hub)).await.is_err() {
                                        error!("no se pudo enviar mensaje a msg_to_hub");
                                    }
                                },
                                HubMessage::UpdateFirmwareRequest(_) => {
                                    if tx_command_to_hub.send(MessageServiceCommand::ToHub(to_hub)).await.is_err() {
                                        error!("no se pudo enviar mensaje a msg_to_hub");
                                    }
                                },
                                HubMessage::FromServerSettings(_) => {
                                    if tx_command_to_hub.send(MessageServiceCommand::ToHub(to_hub)).await.is_err() {
                                        error!("no se pudo enviar mensaje a msg_to_hub");
                                    }
                                },
                                HubMessage::FromServerSettingsAck(_) => {
                                    if tx_command_to_hub.send(MessageServiceCommand::ToHub(to_hub)).await.is_err() {
                                        error!("no se pudo enviar mensaje a msg_to_hub");
                                    }
                                },
                                HubMessage::DeleteHub(_) => {
                                    if tx_command_to_hub.send(MessageServiceCommand::ToHub(to_hub)).await.is_err() {
                                        error!("no se pudo enviar mensaje a msg_to_hub");
                                    }
                                },
                                HubMessage::ActiveHub(_) => {
                                    if tx_command_to_hub.send(MessageServiceCommand::ToHub(to_hub)).await.is_err() {
                                        error!("no se pudo enviar mensaje a msg_to_hub");
                                    }
                                },
                                HubMessage::LinkageAck(_) => {
                                    if tx_command_to_hub.send(MessageServiceCommand::ToHub(to_hub)).await.is_err() {
                                        error!("no se pudo enviar mensaje a msg_to_hub");
                                    }
                                },
                                _ => {}
                            }
                        },
                        MessageServiceCommand::Batch(batch) => {
                            if tx_command_to_server.send(MessageServiceCommand::Batch(batch)).await.is_err() {
                                error!("no se pudo enviar batch a msg_to_server");
                            }
                        },
                        MessageServiceCommand::ToServer(to_server) => {
                            match to_server {
                                ServerMessage::FromHubSettings(hub_settings) => {
                                    if tx_command_to_server.send(MessageServiceCommand::ToServer(ServerMessage::FromHubSettings(hub_settings))).await.is_err() {
                                        error!("no se pudo enviar mensaje a msg_to_server");
                                    }
                                },
                                ServerMessage::FromHubSettingsAck(hub_settings_ack) => {
                                    if tx_command_to_server.send(MessageServiceCommand::ToServer(ServerMessage::FromHubSettingsAck(hub_settings_ack))).await.is_err() {
                                        error!("no se pudo enviar mensaje a msg_to_server");
                                    }
                                },
                                ServerMessage::FirmwareOutcome(firmware_outcome) => {
                                    if tx_command_to_server.send(MessageServiceCommand::ToServer(ServerMessage::FirmwareOutcome(firmware_outcome))).await.is_err() {
                                        error!("no se pudo enviar mensaje a msg_to_server");
                                    }
                                },
                                ServerMessage::FirmwareOutcomeError(firmware_outcome_error) => {
                                    if tx_command_to_server.send(MessageServiceCommand::ToServer(ServerMessage::FirmwareOutcomeError(firmware_outcome_error))).await.is_err() {
                                        error!("no se pudo enviar mensaje FirmwareOutcomeError a msg_to_server");
                                    }
                                },
                                ServerMessage::Report(report) => {
                                    if tx_command_to_server.send(MessageServiceCommand::ToServer(ServerMessage::Report(report))).await.is_err() {
                                        error!("no se pudo enviar mensaje a msg_to_server");
                                    }
                                },
                                ServerMessage::Monitor(monitor) => {
                                    if tx_command_to_server.send(MessageServiceCommand::ToServer(ServerMessage::Monitor(monitor))).await.is_err() {
                                        error!("no se pudo enviar mensaje a msg_to_server");
                                    }
                                },
                                ServerMessage::AlertAir(alert_air) => {
                                    if tx_command_to_server.send(MessageServiceCommand::ToServer(ServerMessage::AlertAir(alert_air))).await.is_err() {
                                        error!("no se pudo enviar mensaje a msg_to_server");
                                    }
                                },
                                ServerMessage::AlertTem(alert_tem) => {
                                    if tx_command_to_server.send(MessageServiceCommand::ToServer(ServerMessage::AlertTem(alert_tem))).await.is_err() {
                                        error!("no se pudo enviar mensaje a msg_to_server");
                                    }
                                },
                                ServerMessage::Metrics(metrics) => {
                                    if tx_command_to_server.send(MessageServiceCommand::ToServer(ServerMessage::Metrics(metrics))).await.is_err() {
                                        error!("no se pudo enviar mensaje a msg_to_server");
                                    }
                                },
                                _ => {}
                            }
                        },
                        MessageServiceCommand::GenerateHelloWorld => {
                            if tx_command_to_server.send(cmd).await.is_err() {
                                error!("no se pudo enviar GenerateHelloWorld a msg_to_server");
                            }
                        },
                        MessageServiceCommand::GenerateLinkageAck(hub) => {
                            let metadata = Metadata {
                                sender_user_id: self.context.system.id_edge.clone(),
                                destination_id: hub,
                                timestamp: Utc::now().timestamp(),
                            };
                            let msg = LinkageAck {
                                metadata,
                                linkage_ack: true,
                            };
                            if tx_command_to_hub.send(MessageServiceCommand::ToHub(HubMessage::LinkageAck(msg))).await.is_err() {
                                error!("no se pudo enviar mensaje a msg_to_hub");
                            }
                        },
                        MessageServiceCommand::GenerateEdgeState(state) => {
                            if tx_command_to_server.send(MessageServiceCommand::GenerateEdgeState(state)).await.is_err() {
                                error!("no se pudo enviar GenerateEdgeState a msg_to_server");
                            }
                        },
                        MessageServiceCommand::GenerateNetworkAck(code) => {
                            if tx_command_to_server.send(MessageServiceCommand::GenerateNetworkAck(code)).await.is_err() {
                                error!("no se pudo enviar GenerateNetworkAck a msg_to_server");
                            }
                        }
                    }
                }

                Some(cmd) = rx.recv() => {
                    if self.sender.send(cmd).await.is_err() {
                        error!("no se pudo enviar mensaje MessageServiceResponse desde MessageService");
                    }
                }
            }
        }
    }
}

/// Metadatos estándar para todos los mensajes del sistema.
///
/// Proporciona contexto de trazabilidad, origen y destino para cada paquete de datos.
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, FromRow, Hash)]
pub struct Metadata {
    #[serde(rename = "s")]
    pub sender_user_id: String,
    #[serde(rename = "d")]
    pub destination_id: String,
    #[serde(rename = "t")]
    pub timestamp: i64,
}

/// Mediciones de sensores ambientales y operativos.
///
/// Representa el paquete de datos principal generado por los nodos.
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, FromRow)]
pub struct Measurement {
    #[sqlx(flatten)]
    #[serde(rename = "m")]
    pub metadata: Metadata,
    #[sqlx(rename = "network_id")]
    #[serde(rename = "n")]
    pub network: String,
    #[serde(rename = "pc")]
    pub pulse_counter: f32,
    #[serde(rename = "t")]
    pub temperature: f32,
    #[serde(rename = "h")]
    pub humidity: f32,
    #[serde(rename = "aq")]
    pub air_quality: f32,
    #[serde(rename = "s")]
    pub sample: u16,
}

/// Alerta de calidad de aire.
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, FromRow)]
pub struct AlertAir {
    #[sqlx(flatten)]
    #[serde(rename = "m")]
    pub metadata: Metadata,
    #[sqlx(rename = "network_id")]
    #[serde(rename = "n")]
    pub network: String,
    #[serde(rename = "ia")]
    pub initial_air_quality: f32,
    #[serde(rename = "aa")]
    pub actual_air_quality: f32,
}

/// Alerta de Temperatura y Humedad.
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, FromRow)]
pub struct AlertTh {
    #[sqlx(flatten)]
    #[serde(rename = "m")]
    pub metadata: Metadata,
    #[sqlx(rename = "network_id")]
    #[serde(rename = "n")]
    pub network: String,
    #[serde(rename = "i")]
    pub initial_temp: f32,
    #[serde(rename = "a")]
    pub actual_temp: f32,
}

/// Datos de telemetría y salud del Hub.
///
/// Incluye información sobre memoria, stack y conectividad para diagnóstico.
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, FromRow)]
pub struct Monitor {
    #[sqlx(flatten)]
    #[serde(rename = "m")]
    pub metadata: Metadata,
    #[sqlx(rename = "network_id")]
    #[serde(rename = "n")]
    pub network: String,
    #[serde(rename = "hf")]
    pub heap_free: u32,
    #[serde(rename = "hm")]
    pub heap_min_free: u32,
    #[serde(rename = "hb")]
    pub heap_largest_block: u32,
    #[serde(rename = "ut")]
    pub uptime_sec: i64,
}

/// Definición de una Red lógica.
///
/// Utilizada para agrupar dispositivos bajo un mismo identificador de red.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Network {
    pub metadata: Metadata,
    pub id_network: String,
    pub name_network: String,
    pub active: bool,
    pub delete_network: bool,
}

/// Definición del mensaje NetworkAck
///
/// Utilizado para responder al servidor cuando recibe un mensaje de tipo
/// Network y asi confirmar que fue efectiva la comunicación.
/// El campo `code_of_ack` contiene un código que representa el ack para un tipo de mensaje específico.
///
/// Códigos:
/// 100: Mensaje de ack para la creación de una nueva red (Éxito).
/// 101: Mensaje de ack para la creación de una nueva red (Fracaso).
/// 200: Mensaje de ack para la eliminación de una red existente (Éxito).
/// 201: Mensaje de ack para la eliminación de una red existente (Fracaso).
/// 300: Mensaje de ack para la activación de una red existente (Éxito).
/// 301: Mensaje de ack para la activación de una red existente (Fracaso).
/// 400: Mensaje de ack para la desactivación de una red existente (Éxito).
/// 401: Mensaje de ack para la desactivación de una red existente (Fracaso).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NetworkAck {
    pub metadata: Metadata,
    pub id_network: String,
    pub code_of_ack: u32,
}

/// Configuración remota para un dispositivo (Hub/Nodo).
///
/// Contiene credenciales WiFi/MQTT y parámetros operativos.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Settings {
    #[serde(rename = "m")]
    pub metadata: Metadata,
    #[serde(rename = "mi")]
    pub message_id: u32,
    #[serde(rename = "n")]
    pub network: String,
    #[serde(rename = "ws")]
    pub wifi_ssid: String,
    #[serde(rename = "wp")]
    pub wifi_password: String,
    #[serde(rename = "mu")]
    pub mqtt_uri: String,
    #[serde(rename = "dn")]
    pub device_name: String,
    #[serde(rename = "s")]
    pub sample: u16,
    #[serde(rename = "e")]
    pub energy_mode: u32,
}

impl Settings {
    /// Convierte la configuración recibida en un registro de Hub (`HubRow`) para persistencia.
    ///
    /// # Parámetros
    /// - `network`: ID de la red a la que se asocia este dispositivo.
    pub fn cast_settings_to_hub_row(self, network: String) -> HubRow {
        let mut hr = HubRow::default();
        hr.network_id = network;
        hr.device_name = self.device_name;
        hr
    }
}

/// Mensaje de Handshake enviado HACIA el Hub (Downlink).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HandshakeToHub {
    #[serde(rename = "m")]
    pub metadata: Metadata,
    #[serde(rename = "f")]
    pub flag: String,
    #[serde(rename = "b")]
    pub balance_epoch: u32,
}

/// Mensaje de Handshake proveniente del Hub (Uplink).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HandshakeFromHub {
    #[serde(rename = "m")]
    pub metadata: Metadata,
    #[serde(rename = "f")]
    pub flag: String,
    #[serde(rename = "b")]
    pub balance_epoch: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StateToHub {
    #[serde(rename = "m")]
    pub metadata: Metadata,
    #[serde(rename = "s")]
    pub state: String,

    #[serde(rename = "b")]
    pub balance_epoch: u32,
    #[serde(rename = "d")]
    pub duration: u32,

    #[serde(rename = "f")]
    pub frequency: u32,
    #[serde(rename = "j")]
    pub jitter: u32,
}

/// Notificación de cambio de Fase dentro del modo Balance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PhaseNotification {
    #[serde(rename = "m")]
    pub metadata: Metadata,
    #[serde(rename = "s")]
    pub state: String,
    #[serde(rename = "e")]
    pub epoch: u32,
    #[serde(rename = "p")]
    pub phase: String,
    #[serde(rename = "f")]
    pub frequency: u32,
    #[serde(rename = "j")]
    pub jitter: u32,
}

/// Mensaje de latido (Heartbeat) para indicar a los Hubs que el Edge está vivo.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Heartbeat {
    #[serde(rename = "m")]
    pub metadata: Metadata,
    #[serde(rename = "b")]
    pub beat: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HelloWorld {
    pub metadata: Metadata,
    pub hello: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EdgeState {
    pub metadata: Metadata,
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HubState {
    #[serde(rename = "m")]
    pub metadata: Metadata,
    #[serde(rename = "n")]
    pub network: String,
    #[serde(rename = "s")]
    pub state: String,
}

/// Comando para eliminar un Hub del registro.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeleteHub {
    pub metadata: Metadata, // El destination_id es el hub destino
    pub network: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActiveHub {
    pub metadata: Metadata, // El destination_id es el hub destino
    pub network: String,
    pub active: bool,
}

/// Confirmación de recepción de configuración (Handshake bidireccional).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SettingOk {
    #[serde(rename = "m")]
    pub metadata: Metadata,
    #[serde(rename = "i")]
    pub message_id: u32,
    #[serde(rename = "n")]
    pub network: String,
    #[serde(rename = "h")]
    pub handshake: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UpdateFirmware {
    pub metadata: Metadata,
    pub network: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UpdateFirmwareRequestHub {
    #[serde(rename = "m")]
    pub metadata: Metadata,
    #[serde(rename = "n")]
    pub network: String,
    #[serde(rename = "v")]
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct FirmwareOk {
    #[serde(rename = "m")]
    pub metadata: Metadata,
    #[serde(rename = "u")]
    pub is_updated: bool,
    #[serde(rename = "s")]
    pub success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FirmwareOutcome {
    pub metadata: Metadata,
    pub network: String,
    pub percentage_ok: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FirmwareOutcomeError {
    pub metadata: Metadata,
    pub network: String,
    pub error: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct SystemMetrics {
    pub metadata: Metadata,
    pub uptime_seconds: u64,
    pub cpu_usage_percent: f32,
    pub cpu_temp_celsius: f32,
    pub ram_total_mb: u64,
    pub ram_used_mb: u64,
    pub ram_used_by_service_mb: u64,
    pub sd_total_gb: u64,
    pub sd_used_gb: u64,
    pub sd_usage_percent: f32,
    pub network_rx_bytes: u64,
    pub network_tx_bytes: u64,
    pub wifi_rssi: Option<i32>,
    pub wifi_signal_dbm: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct EmptyQueue {
    #[serde(rename = "m")]
    pub metadata: Metadata,
    #[serde(rename = "s")]
    pub state: String,
    #[serde(rename = "p")]
    pub phase: String,
    #[serde(rename = "q")]
    pub queue_empty: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct EmptyQueueSafeMode {
    #[serde(rename = "m")]
    pub metadata: Metadata,
    #[serde(rename = "s")]
    pub state: String,
    #[serde(rename = "q")]
    pub queue_empty: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct LinkageRequest {
    #[serde(rename = "m")]
    pub metadata: Metadata,
    #[serde(rename = "d")]
    pub device_name: String,
    #[serde(rename = "n")]
    pub network: String,
    #[serde(rename = "l")]
    pub linkage_request: bool,
}

impl LinkageRequest {
    pub fn cast_to_hub_row(self) -> HubRow {
        let mut hr = HubRow::default();
        hr.id = self.metadata.sender_user_id;
        hr.device_name = self.device_name;
        hr.network_id = self.network;
        hr
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct LinkageAck {
    #[serde(rename = "m")]
    pub metadata: Metadata,
    #[serde(rename = "l")]
    pub linkage_ack: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum HubMessage {
    // Mensajes provenientes del Hub
    Report(Measurement),
    Monitor(Monitor),
    AlertAir(AlertAir),
    AlertTem(AlertTh),
    HandshakeFromHub(HandshakeFromHub),
    FirmwareOk(FirmwareOk),
    FromHubSettings(Settings),
    FromHubSettingsAck(SettingOk),
    EmptyQueue(EmptyQueue),
    EmptyQueueSafe(EmptyQueueSafeMode),
    LinkageRequest(LinkageRequest),
    HubState(HubState),

    // Mensajes para el Hub
    UpdateFirmwareRequest(UpdateFirmwareRequestHub),
    FromServerSettings(Settings),
    FromServerSettingsAck(SettingOk),
    DeleteHub(DeleteHub),
    ActiveHub(ActiveHub),
    Heartbeat(Heartbeat),
    HandshakeToHub(HandshakeToHub),
    PhaseNotification(PhaseNotification),
    StateToHub(StateToHub),
    LinkageAck(LinkageAck),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum ServerMessage {
    // Mensajes provenientes del Server
    UpdateFirmware(UpdateFirmware),
    DeleteHub(DeleteHub),
    FromServerSettings(Settings),
    FromServerSettingsAck(SettingOk),
    Network(Network),
    Heartbeat(Heartbeat),

    // Mensajes para el Server
    FirmwareOutcome(FirmwareOutcome),
    FirmwareOutcomeError(FirmwareOutcomeError),
    HelloWorld(HelloWorld), // y proveniente del server tambien
    FromHubSettings(Settings),
    FromHubSettingsAck(SettingOk),
    Report(Measurement),
    Monitor(Monitor),
    AlertAir(AlertAir),
    AlertTem(AlertTh),
    Metrics(SystemMetrics),
    ReportBatch(Vec<Measurement>),
    MonitorBatch(Vec<Monitor>),
    AlertAirBatch(Vec<AlertAir>),
    AlertTemBatch(Vec<AlertTh>),
    EdgePeriodic(EdgeState),
    NetworkAck(NetworkAck),
    HubState(HubState),
}

/// Estado de conexión con el servidor remoto.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerStatus {
    Connected,
    Disconnected,
}

/// Estado de conexión con el broker MQTT local.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalStatus {
    Connected,
    Disconnected,
}

/// Representación final de un mensaje listo para ser enviado por MQTT.
///
/// Contiene el payload binario (serializado) y los parámetros de transporte.
#[derive(Debug, Serialize, Deserialize)]
pub struct SerializedMessage {
    topic: String,
    payload: Vec<u8>,
    qos: u8,
    retain: bool,
}

impl SerializedMessage {
    pub fn new(topic: String, payload: Vec<u8>, qos: u8, retain: bool) -> Self {
        Self {
            topic,
            payload,
            qos,
            retain,
        }
    }
    pub fn get_topic(&self) -> &str {
        &self.topic
    }
    pub fn get_payload(&self) -> &[u8] {
        &self.payload
    }
    pub fn get_qos(&self) -> u8 {
        self.qos
    }
    pub fn get_retain(&self) -> bool {
        self.retain
    }
}
