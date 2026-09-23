use crate::second_layer::database::domain::HubRow;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

/// Metadatos estándar para todos los mensajes del sistema
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, FromRow, Hash)]
pub struct Metadata {
    #[serde(rename = "s")]
    pub sender_user_id: String,
    #[serde(rename = "d")]
    pub destination_id: String,
    #[serde(rename = "t")]
    pub timestamp: i64,
}

// ======= Grupo de mensajes de telemetría =======

/// Mediciones de sensores ambientales
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, FromRow)]
pub struct Measurement {
    #[sqlx(flatten)]
    #[serde(rename = "m")]
    pub metadata: Metadata,
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
    #[serde(rename = "n")]
    pub network: String,
    #[serde(rename = "ia")]
    pub initial_air_quality: f32,
    #[serde(rename = "aa")]
    pub actual_air_quality: f32,
}

/// Alerta de temperatura y humedad.
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, FromRow)]
pub struct AlertTh {
    #[sqlx(flatten)]
    #[serde(rename = "m")]
    pub metadata: Metadata,
    #[serde(rename = "n")]
    pub network: String,
    #[serde(rename = "i")]
    pub initial_temp: f32,
    #[serde(rename = "a")]
    pub actual_temp: f32,
}

/// Datos de telemetría y salud del Hub.
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, FromRow)]
pub struct Monitor {
    #[sqlx(flatten)]
    #[serde(rename = "m")]
    pub metadata: Metadata,
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

// ======= Grupo de mensajes de redes =======

/// Estructura de mensaje del servidor con indicaciones sobre una red (ABM).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Network {
    pub metadata: Metadata,
    pub id_network: String,
    pub name_network: String,
    pub active: bool,
    pub delete_network: bool,
}

/// Mensaje utilizado para responder al servidor cuando recibe un mensaje de tipo
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

// ======= Grupo de mensajes de configuracion =======

/// Estructura de un mensaje de configuración de un dispositivo Hub.
/// Es usado para ambos sentidos, Hub -> Edge, Hub <- Edge, Edge <- Servidor, Edge -> Servidor.
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

/// Confirmación de recepción de configuración (Handshake bidireccional).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SettingsAck {
    #[serde(rename = "m")]
    pub metadata: Metadata,
    #[serde(rename = "i")]
    pub message_id: u32,
    #[serde(rename = "n")]
    pub network: String,
    #[serde(rename = "h")]
    pub handshake: bool,
}

// ======= Grupo de mensajes de fsm =======

/// Mensaje de Handshake enviado HACIA el Hub.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HandshakeToHub {
    #[serde(rename = "m")]
    pub metadata: Metadata,
    #[serde(rename = "f")]
    pub flag: String,
    #[serde(rename = "b")]
    pub balance_epoch: u32,
}

/// Mensaje de Handshake proveniente del Hub.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HandshakeFromHub {
    #[serde(rename = "m")]
    pub metadata: Metadata,
    #[serde(rename = "f")]
    pub flag: String,
    #[serde(rename = "b")]
    pub balance_epoch: u32,
}

/// Notificación de cambio de Fase dentro del modo Balance.
/// Edge -> Hub
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

/// Mensaje proveniente de los Hub indicando cola vacia.
/// Hub -> Edge.
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

/// Mensaje proveniente de los Hub indicando cola vacia en safe mode.
/// Hub -> Edge.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct EmptyQueueSafeMode {
    #[serde(rename = "m")]
    pub metadata: Metadata,
    #[serde(rename = "s")]
    pub state: String,
    #[serde(rename = "q")]
    pub queue_empty: bool,
}

// ======= Grupo de mensajes de periodicos de control =======

/// Mensaje periódico para los Hub indicando el estado del Edge en tiempo real.
/// Edge -> Hub
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

/// Mensaje de latido para indicar a los Hubs que el Edge está vivo.
/// Tambien es recibido desde el servidor para indicar que este esta vivo.
/// Server -> Edge, Edge -> Hub.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Heartbeat {
    #[serde(rename = "m")]
    pub metadata: Metadata,
    #[serde(rename = "b")]
    pub beat: bool,
}

/// Mensaje de estado periódico del Edge enviado al servidor.
/// Edge -> Server.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EdgeState {
    pub metadata: Metadata,
    pub state: String,
}

/// Mensaje de estado periódico del Hub enviado al servidor.
/// Hub -> Edge -> Server.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HubState {
    #[serde(rename = "m")]
    pub metadata: Metadata,
    #[serde(rename = "n")]
    pub network: String,
    #[serde(rename = "s")]
    pub state: String,
}

// ======= Grupo de mensajes de firmware =======

/// Mensaje indicando la solicitud de actualizar el firmware de los Hub.
/// Server -> Edge.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UpdateHubFirmware {
    pub metadata: Metadata,
    pub network: String,
}

/// Mensaje indicando la solicitud de actualizar el firmware al Edge.
/// Server -> Edge.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UpdateEdgeFirmware {
    pub metadata: Metadata,
    pub version: String,
}

/// Mensaje indicando a un Hub la solicitud de actualizar su firmware.
/// Edge -> Hub.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UpdateFirmwareRequestHub {
    #[serde(rename = "m")]
    pub metadata: Metadata,
    #[serde(rename = "n")]
    pub network: String,
    #[serde(rename = "v")]
    pub version: String,
}

/// Mensaje indicando el resultado del proceso de actualizar el firmware.
/// Hub -> Edge.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct FirmwareHubAck {
    #[serde(rename = "m")]
    pub metadata: Metadata,
    #[serde(rename = "u")]
    pub is_updated: bool,
    #[serde(rename = "s")]
    pub success: bool,
}

/// Mensaje indicando el resultado de todo el proceso de actualizacion de firmware de una red.
/// Edge -> Server.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FirmwareHubResult {
    pub metadata: Metadata,
    pub network: String,
    pub percentage_ok: f32,
    pub error: String,
}

/// Mensaje indicando el resultado de todo el proceso de actualizacion de firmware de una red.
/// Edge -> Server.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FirmwareEdgeResult {
    pub metadata: Metadata,
    pub error: bool,
}

// ======= Grupo de mensajes de vinculacion =======

/// Mensaje de solicitud de linkage proveniente de un Hub. Hub -> Edge.
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

/// Mensaje de respuesta de linkage. Edge -> Hub.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct LinkageAck {
    #[serde(rename = "m")]
    pub metadata: Metadata,
    #[serde(rename = "l")]
    pub linkage_ack: bool,
}

// ======= Grupo de mensajes informativos =======

/// Mensaje de saludo para el servidor indicando que el Edge acaba de iniciarse.
/// Edge -> Server
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HelloServer {
    pub metadata: Metadata,
    pub hello: bool,
}

/// Mensaje de monitoreo de uso de recursos del Edge.
/// Edge -> Server.
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

/// Conjunto de mensajes que provienen de un Hub.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum HubMessage {
    Report(Measurement),
    Monitor(Monitor),
    AlertAir(AlertAir),
    AlertTem(AlertTh),
    HandshakeFromHub(HandshakeFromHub),
    FirmwareOk(FirmwareHubAck),
    FromHubSettings(Settings),
    FromHubSettingsAck(SettingsAck),
    EmptyQueue(EmptyQueue),
    EmptyQueueSafe(EmptyQueueSafeMode),
    LinkageRequest(LinkageRequest),
    HubState(HubState),
}

/// Conjunto de mensajes que provienen del Server.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum ServerMessage {
    UpdateFirmware(UpdateHubFirmware),
    UpdateEdgeFirmware(UpdateEdgeFirmware),
    FromServerSettings(Settings),
    FromServerSettingsAck(SettingsAck),
    Network(Network),
    Heartbeat(Heartbeat),
}

/// Representación final de un mensaje listo para ser enviado por MQTT.
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
