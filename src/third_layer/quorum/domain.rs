use serde::Deserialize;


/// Configuraciones del PCBP (Post-Failure Control and Balancing Protocol).
///
/// Modela los umbrales de tiempo, frecuencias y reintentos permitidos durante el algoritmo 
/// de balanceo y de funcionamiento normal/seguro.
#[derive(Default, Debug, Deserialize)]
pub struct ProtocolSettings {
    /// Número máximo de reintentos (ej: reintentos de handshake).
    max_attempts: u64,
    /// Frecuencia base usada en la fase de alerta/monitoreo/datos.
    frequency_phase: u32,
    /// Frecuencia para reportar latidos durante el modo seguro.
    frequency_safe_mode: u32,
    /// Tiempo límite esperando un Handshake de los Hubs.
    timeout_handshake: u64,
    /// Tiempo límite (timeout) general de una fase (ej. PhaseAlert).
    timeout_phase: u64,
    /// Tiempo límite para mantener el estado de Safe Mode.
    timeout_safe_mode: u64,
    /// Retardo entre latidos durante el modo de balanceo.
    time_between_heartbeats_balance_mode: u64,
    /// Retardo entre latidos durante el modo normal.
    time_between_heartbeats_normal: u64,
    /// Retardo entre latidos durante el modo seguro.
    time_between_heartbeats_safe_mode: u64,
}


impl ProtocolSettings {
    pub fn get_max_attempts(&self) -> u64 {
        self.max_attempts
    }
    pub fn get_frequency_phase(&self) -> u32 { self.frequency_phase }
    pub fn get_frequency_safe_mode(&self) -> u32 { self.frequency_safe_mode }
    pub fn get_timeout_handshake(&self) -> u64 { self.timeout_handshake }
    pub fn get_timeout_phase(&self) -> u64 { self.timeout_phase }
    pub fn get_timeout_safe_mode(&self) -> u64 { self.timeout_safe_mode }
    pub fn get_time_between_heartbeats_balance_mode(&self) -> u64 {
        self.time_between_heartbeats_balance_mode
    }
    pub fn get_time_between_heartbeats_normal(&self) -> u64 {
        self.time_between_heartbeats_normal
    }
    pub fn get_time_between_heartbeats_safe_mode(&self) -> u64 {
        self.time_between_heartbeats_safe_mode
    }
}
