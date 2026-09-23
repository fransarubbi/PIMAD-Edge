use serde::Deserialize;


/// Post-Failure Control and Balancing Protocol
#[derive(Default, Debug, Deserialize)]
pub struct ProtocolSettings {
    max_attempts: u64,
    frequency_phase: u32,
    frequency_safe_mode: u32,
    timeout_handshake: u64,
    timeout_phase: u64,
    timeout_safe_mode: u64,
    time_between_heartbeats_balance_mode: u64,
    time_between_heartbeats_normal: u64,
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
