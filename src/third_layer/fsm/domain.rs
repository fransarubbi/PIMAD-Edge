//! Dominio de la Máquina de Estados Finita General (FSM).
//!
//! Este módulo implementa el núcleo lógico del comportamiento del dispositivo.
//! Se basa en una **Máquina de Estados Jerárquica (HFSM)** puramente funcional.
//!
//! # Conceptos Clave
//!
//! 1.  **Estado Inmutable (`FsmState`):** Representa una "foto instantánea" del sistema. No se modifica, se reemplaza.
//! 2.  **Jerarquía:** El estado no es plano. Existe un `StateGlobal` que determina el modo de operación,
//!     y dentro de este, pueden existir sub-estados (ej. `BalanceMode` -> `Quorum` -> `CheckQuorumIn`).
//! 3.  **Transiciones Puras:** La función `step` recibe `(Estado Actual, Evento)` y retorna `(Nuevo Estado, Acciones)`.
//!     No hay efectos secundarios (I/O) dentro de la lógica de transición.
//! 4.  **Acciones (`Action`):** Instrucciones generadas por la FSM para que el "Runtime" (el bucle principal)
//!     ejecute tareas reales (enviar mensajes MQTT, iniciar timers, escribir en DB).
//!

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument};

/// Estados Globales de nivel superior.
///
/// Determinan el comportamiento macro del sistema.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum StateGlobal {
    Start,
    /// **Modo de Balanceo:** Fase crítica de negociación distribuida. El dispositivo
    /// intenta sincronizarse con sus hubs, verificar quórum y establecer su rol.
    BalanceMode,
    /// **Operación Normal:** El dispositivo ha completado el balanceo exitosamente y opera en régimen estable.
    Normal,
    /// **Modo Seguro:** Estado de fallo o emergencia. El dispositivo entra aquí tras errores críticos
    /// (DB corrupta, fallo de consenso repetido) para evitar operaciones inseguras.
    SafeMode,
    /// **Estad Desconectado:** Estado de transición luego de una pérdida de conexión MQTT.
    /// Cuando se recupere la conexión, se iniciará el PCBPF.
    Disconnected,
}

/// Sub-estados del modo de balanceo (`StateGlobal::BalanceMode`).
///
/// Esta es la parte más compleja de la FSM, gestionando el consenso y las fases de operación temporal.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SubStateBalanceMode {
    InitBalanceMode,
    InHandshake,
    Quorum,
    Phase,
    OutHandshake,
}

/// Sub-estados específicos de la verificación de Quorum.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SubStateQuorum {
    CheckQuorumIn,
    CheckQuorumOut,
    RepeatHandshakeIn,
    RepeatHandshakeOut,
}

/// Fases operativas dentro del modo de balanceo.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SubStatePhase {
    Alert,
    Data,
    Monitor,
}

/// Representación compuesta del estado completo de la FSM.
///
/// Utiliza `Option` para modelar la jerarquía. Si un estado padre no está activo,
/// sus hijos deben ser `None`.
///
/// # Ejemplo
/// Si estamos verificando quórum:
/// - `global`: `StateGlobal::BalanceMode`
/// - `balance`: `Some(SubStateBalanceMode::Quorum)`
/// - `quorum`: `Some(SubStateQuorum::CheckQuorumIn)`
/// - `phase`: `None`
#[derive(Debug, Clone)]
pub struct FsmState {
    global: StateGlobal,
    balance: Option<SubStateBalanceMode>,
    quorum: Option<SubStateQuorum>,
    phase: Option<SubStatePhase>,
}

/// Resultado de intentar aplicar un evento al estado actual.
#[derive(Debug)]
pub enum Transition {
    Valid(TransitionValid),
    Invalid(TransitionInvalid),
}

/// Datos resultantes de una transición exitosa.
#[derive(Debug)]
pub struct TransitionValid {
    change_state: FsmState,
    actions: Vec<Action>,
}

impl TransitionValid {
    pub fn get_change_state(&self) -> FsmState {
        self.change_state.clone()
    }
    pub fn get_actions(&self) -> Vec<Action> {
        self.actions.clone()
    }
}

/// Datos resultantes de una transición fallida o no permitida.
#[derive(Debug)]
pub struct TransitionInvalid {
    invalid: String,
}

impl TransitionInvalid {
    pub fn get_invalid(&self) -> &str {
        &self.invalid
    }
}

/// Acciones o Efectos Secundarios (Side Effects).
///
/// Estas variantes son instrucciones para el exterior. La FSM **decide** qué hacer,
/// pero no **ejecuta** la acción (principio de separación de responsabilidades).
#[derive(Debug, PartialEq, Clone)]
pub enum Action {
    SendHeartbeatMessagePhase,
    SendHeartbeatMessageSafeMode,
    SendHeartbeatMessageNormal,
    StopSendHeartbeatMessagePhase,
    StopSendHeartbeatMessageSafeMode,
    StopSendHeartbeatMessageNormal,
    OnEntryBalance(SubStateBalanceMode),
    OnEntryQuorum(SubStateQuorum),
    OnEntryPhase(SubStatePhase),
    OnEntryNormal,
    OnEntrySafeMode,
    OnEntryDisconnected,
    StopTimer,
    CalculateQuorum,
}

/// Eventos que alimentan la FSM.
///
/// Estos son los "Triggers" que provocan los cambios de estado.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    Start,
    Timeout,
    BalanceEpochOk,
    BalanceEpochNotOk,
    ApproveQuorum,
    QuorumPhase,
    QuorumSafeMode,
    NotApproveQuorum,
    NotApproveNotAttempts,
    InitTimer(Duration),
    StopTimer,
    NewMessageHandshake,
    LocalDisconnected,
    LocalConnected,
}

/// Indica el estado persistente y compartido (memoria de trabajo) de la sesión de balanceo.
#[derive(Debug, Clone, PartialEq)]
pub enum StateOfSession {
    None,
    InHandshake,
    Quorum,
    RepeatHandshake,
    OutHandshake,
    PhaseAlert,
    PhaseData,
    PhaseMonitor,
    SafeMode,
}

/// Mantiene la memoria y contadores durante el proceso iterativo de balanceo (handshakes y quórum).
///
/// Dado que la FSM de Rust es funcional y pura (sin estado mutable interno más allá de su `FsmState`),
/// `UpdateSession` se utiliza como un objeto de memoria anexo (`Context`) que se pasa
/// por las acciones para ir acumulando confirmaciones y reintentos.
pub struct UpdateSession {
    empty_hash: HashMap<String, bool>,
    handshake_hash: HashMap<String, u32>,
    state: StateOfSession,
    total_attempts: f64,
    epoch: u32,
}

impl UpdateSession {
    pub fn new() -> Self {
        Self {
            empty_hash: HashMap::new(),
            handshake_hash: HashMap::new(),
            state: StateOfSession::None,
            total_attempts: 0.0,
            epoch: 0,
        }
    }

    pub fn set_epoch(&mut self, epoch: u32) {
        self.epoch = epoch
    }

    pub fn get_epoch(&self) -> u32 {
        self.epoch
    }

    pub fn reset_total_attempts(&mut self) {
        self.total_attempts = 0.0;
    }

    pub fn reset_empty_hash(&mut self) {
        self.empty_hash.clear();
    }

    pub fn reset_handshake_hash(&mut self) {
        self.handshake_hash.clear();
    }

    pub fn insert_empty(&mut self, key: String, value: bool) {
        if !self.empty_hash.contains_key(&key) {
            self.empty_hash.insert(key, value);
        }
    }

    pub fn insert_handshake(&mut self, key: String, value: u32) {
        if !self.handshake_hash.contains_key(&key) {
            self.handshake_hash.insert(key, value);
        }
    }

    pub fn set_state(&mut self, state: StateOfSession) {
        self.state = state;
    }

    pub fn increment_attempts(&mut self) {
        self.total_attempts += 1.0;
    }

    pub fn get_state(&self) -> &StateOfSession {
        &self.state
    }

    pub fn get_total_handshake(&self) -> u64 {
        self.handshake_hash.len() as u64
    }

    pub fn get_total_attempts(&self) -> f64 {
        self.total_attempts
    }

    pub fn get_total_empty(&self) -> u64 {
        self.empty_hash.len() as u64
    }
}

impl FsmState {
    /// Crea una nueva instancia de la FSM en el estado inicial.
    pub fn new() -> Self {
        Self {
            global: StateGlobal::Start,
            balance: None,
            quorum: None,
            phase: None,
        }
    }

    /// Lógica interna de despacho según el estado global.
    fn step_inner(&self, event: Event) -> Transition {
        match self.global {
            StateGlobal::Start => self.step_start(event),
            StateGlobal::BalanceMode => self.step_balance_mode(event),
            StateGlobal::SafeMode => self.step_safe_mode(event),
            StateGlobal::Normal => self.step_normal(event),
            StateGlobal::Disconnected => self.step_disconnected(event),
        }
    }

    fn step_start(&self, event: Event) -> Transition {
        match (&self.global, event) {
            (StateGlobal::Start, Event::Start) => {
                let mut next_fsm = self.clone();
                next_fsm.global = StateGlobal::BalanceMode;
                next_fsm.balance = Some(SubStateBalanceMode::InitBalanceMode);

                let valid = TransitionValid {
                    change_state: next_fsm,
                    actions: vec![],
                };
                Transition::Valid(valid)
            }
            _ => {
                let invalid = TransitionInvalid {
                    invalid: "Invalid".to_string(),
                };
                Transition::Invalid(invalid)
            }
        }
    }

    /// Maneja transiciones cuando el estado global es `BalanceMode`.
    fn step_balance_mode(&self, event: Event) -> Transition {
        match (&self.balance, &event) {
            (_, Event::LocalDisconnected) => {
                let mut next_fsm = self.clone();
                next_fsm.balance = None;
                next_fsm.phase = None;
                next_fsm.quorum = None;
                next_fsm.global = StateGlobal::Disconnected;
                let valid = TransitionValid {
                    change_state: next_fsm,
                    actions: vec![Action::StopSendHeartbeatMessagePhase, Action::StopTimer],
                };
                Transition::Valid(valid)
            }
            (Some(SubStateBalanceMode::InitBalanceMode), Event::BalanceEpochOk) => {
                let next_fsm = self.clone();
                state_init_balance_mode_event_balance_epoch(next_fsm)
            }
            (Some(SubStateBalanceMode::InitBalanceMode), Event::BalanceEpochNotOk) => {
                let next_fsm = self.clone();
                state_init_balance_mode_event_balance_epoch_not_ok(next_fsm)
            }
            (Some(SubStateBalanceMode::InHandshake), Event::Timeout) => {
                let next_fsm = self.clone();
                state_in_handshake_event_timeout(next_fsm)
            }
            (Some(SubStateBalanceMode::InHandshake), Event::NewMessageHandshake) => {
                let next_fsm = self.clone();
                new_message_handshake(next_fsm)
            }
            (Some(SubStateBalanceMode::InHandshake), Event::ApproveQuorum) => {
                let next_fsm = self.clone();
                state_in_handshake_event_approve(next_fsm)
            }
            (Some(SubStateBalanceMode::InHandshake), Event::NotApproveQuorum) => {
                Transition::Valid(TransitionValid {
                    change_state: self.clone(),
                    actions: vec![],
                })
            }
            (Some(SubStateBalanceMode::Quorum), _) => match (&self.quorum, event) {
                (Some(SubStateQuorum::CheckQuorumIn), Event::NotApproveQuorum) => {
                    let next_fsm = self.clone();
                    state_check_quorum_in_event_not_approve(next_fsm)
                }
                (Some(SubStateQuorum::CheckQuorumIn), Event::ApproveQuorum) => {
                    let next_fsm = self.clone();
                    state_check_quorum_in_event_approve_quorum(next_fsm)
                }
                (Some(SubStateQuorum::CheckQuorumIn), Event::NotApproveNotAttempts) => {
                    let next_fsm = self.clone();
                    state_check_quorum_event_not_and_not(next_fsm)
                }
                (Some(SubStateQuorum::CheckQuorumOut), Event::NotApproveQuorum) => {
                    let next_fsm = self.clone();
                    state_check_quorum_out_event_not_approve(next_fsm)
                }
                (Some(SubStateQuorum::CheckQuorumOut), Event::ApproveQuorum) => {
                    let next_fsm = self.clone();
                    state_check_quorum_out_event_approve_quorum(next_fsm)
                }
                (Some(SubStateQuorum::CheckQuorumOut), Event::NotApproveNotAttempts) => {
                    let next_fsm = self.clone();
                    state_check_quorum_event_not_and_not(next_fsm)
                }
                (Some(SubStateQuorum::RepeatHandshakeIn), Event::Timeout) => {
                    let next_fsm = self.clone();
                    state_repeat_handshake_in(next_fsm)
                }
                (Some(SubStateQuorum::RepeatHandshakeIn), Event::NewMessageHandshake) => {
                    let next_fsm = self.clone();
                    new_message_handshake(next_fsm)
                }
                (Some(SubStateQuorum::RepeatHandshakeIn), Event::ApproveQuorum) => {
                    let next_fsm = self.clone();
                    state_repeat_in_handshake_event_approve(next_fsm)
                }
                (Some(SubStateQuorum::RepeatHandshakeIn), Event::NotApproveQuorum) => {
                    Transition::Valid(TransitionValid {
                        change_state: self.clone(),
                        actions: vec![],
                    })
                }
                (Some(SubStateQuorum::RepeatHandshakeOut), Event::Timeout) => {
                    let next_fsm = self.clone();
                    state_repeat_handshake_out(next_fsm)
                }
                (Some(SubStateQuorum::RepeatHandshakeOut), Event::NewMessageHandshake) => {
                    let next_fsm = self.clone();
                    new_message_handshake(next_fsm)
                }
                (Some(SubStateQuorum::RepeatHandshakeOut), Event::ApproveQuorum) => {
                    let next_fsm = self.clone();
                    state_repeat_out_handshake_event_approve(next_fsm)
                }
                (Some(SubStateQuorum::RepeatHandshakeOut), Event::NotApproveQuorum) => {
                    Transition::Valid(TransitionValid {
                        change_state: self.clone(),
                        actions: vec![],
                    })
                }
                _ => invalid(),
            },
            (Some(SubStateBalanceMode::Phase), _) => match (&self.phase, &event) {
                (Some(SubStatePhase::Alert), Event::Timeout | Event::QuorumPhase) => {
                    let next_fsm = self.clone();
                    state_alert_event_timeout_or_quorum(next_fsm)
                }
                (Some(SubStatePhase::Data), Event::Timeout | Event::QuorumPhase) => {
                    let next_fsm = self.clone();
                    state_data_event_timeout_or_quorum(next_fsm)
                }
                (Some(SubStatePhase::Monitor), Event::Timeout | Event::QuorumPhase) => {
                    let next_fsm = self.clone();
                    state_monitor_event_timeout_or_quorum(next_fsm)
                }
                _ => invalid(),
            },
            (Some(SubStateBalanceMode::OutHandshake), Event::Timeout) => {
                let next_fsm = self.clone();
                state_out_handshake_event_timeout(next_fsm)
            }
            (Some(SubStateBalanceMode::OutHandshake), Event::ApproveQuorum) => {
                let next_fsm = self.clone();
                state_out_handshake_event_approve(next_fsm)
            }
            (Some(SubStateBalanceMode::OutHandshake), Event::NotApproveQuorum) => {
                Transition::Valid(TransitionValid {
                    change_state: self.clone(),
                    actions: vec![],
                })
            }
            (Some(SubStateBalanceMode::OutHandshake), Event::NewMessageHandshake) => {
                let next_fsm = self.clone();
                new_message_handshake(next_fsm)
            }
            _ => invalid(),
        }
    }

    /// Maneja transiciones cuando el estado global es `Normal`.
    fn step_normal(&self, event: Event) -> Transition {
        match event {
            Event::LocalDisconnected => {
                let mut next_fsm = self.clone();
                next_fsm.global = StateGlobal::Disconnected;
                let valid = TransitionValid {
                    change_state: next_fsm,
                    actions: vec![Action::StopSendHeartbeatMessageNormal],
                };
                return Transition::Valid(valid);
            }
            _ => {
                let invalid = TransitionInvalid {
                    invalid: "Transición inválida".to_string(),
                };
                Transition::Invalid(invalid)
            }
        }
    }

    /// Maneja transiciones cuando el estado global es `Disconnect`.
    fn step_disconnected(&self, event: Event) -> Transition {
        match event {
            Event::LocalConnected => {
                let mut next_fsm = self.clone();
                next_fsm.global = StateGlobal::BalanceMode;
                next_fsm.balance = Some(SubStateBalanceMode::InitBalanceMode);
                let valid = TransitionValid {
                    change_state: next_fsm,
                    actions: vec![],
                };
                return Transition::Valid(valid);
            }
            _ => {
                let invalid = TransitionInvalid {
                    invalid: "Transición inválida".to_string(),
                };
                Transition::Invalid(invalid)
            }
        }
    }

    /// Maneja transiciones cuando el estado global es `SafeMode`.
    fn step_safe_mode(&self, event: Event) -> Transition {
        match event {
            Event::LocalDisconnected => {
                let mut next_fsm = self.clone();
                next_fsm.global = StateGlobal::Disconnected;
                let valid = TransitionValid {
                    change_state: next_fsm,
                    actions: vec![Action::StopSendHeartbeatMessageSafeMode],
                };
                return Transition::Valid(valid);
            }
            Event::Timeout | Event::QuorumSafeMode => {
                let mut next_fsm = self.clone();
                next_fsm.global = StateGlobal::Normal;
                let valid = TransitionValid {
                    change_state: next_fsm,
                    actions: vec![Action::StopSendHeartbeatMessageSafeMode],
                };
                return Transition::Valid(valid);
            }
            _ => {
                let invalid = TransitionInvalid {
                    invalid: "Transición inválida".to_string(),
                };
                Transition::Invalid(invalid)
            }
        }
    }

    /// Función principal de transición (API Pública).
    ///
    /// 1. Calcula el siguiente estado llamando a `step_inner`.
    /// 2. Si la transición es válida, calcula las acciones de entrada (`compute_on_entry`)
    ///    comparando el estado actual (`self`) con el nuevo (`change_state`).
    /// 3. Retorna la transición completa con todas las acciones acumuladas.
    pub fn step(&self, event: Event) -> Transition {
        let transition = self.step_inner(event);

        match transition {
            Transition::Valid(mut t) => {
                let entry_actions = compute_on_entry(self, &t.change_state);
                t.actions.extend(entry_actions);
                Transition::Valid(t)
            }
            invalid => invalid,
        }
    }
}

// --- Funciones auxiliares de transición ---
// Cada una define un cambio atómico de estado.

/// Retorna una transición inválida genérica para Init.
fn invalid() -> Transition {
    let invalid = TransitionInvalid {
        invalid: "Transición inválida".to_string(),
    };
    Transition::Invalid(invalid)
}

/// Transición: InitBalanceMode -> InHandshake.
fn state_init_balance_mode_event_balance_epoch(mut next_fsm: FsmState) -> Transition {
    next_fsm.balance = Some(SubStateBalanceMode::InHandshake);

    let valid = TransitionValid {
        change_state: next_fsm,
        actions: vec![],
    };
    Transition::Valid(valid)
}

/// Transición: InitBalanceMode -> SafeMode.
fn state_init_balance_mode_event_balance_epoch_not_ok(mut next_fsm: FsmState) -> Transition {
    next_fsm.balance = None;
    next_fsm.global = StateGlobal::SafeMode;

    let valid = TransitionValid {
        change_state: next_fsm,
        actions: vec![],
    };
    Transition::Valid(valid)
}

/// Transición: InHandshake -> Quorum (CheckQuorumIn).
fn state_in_handshake_event_timeout(mut next_fsm: FsmState) -> Transition {
    next_fsm.balance = Some(SubStateBalanceMode::Quorum);
    next_fsm.quorum = Some(SubStateQuorum::CheckQuorumIn);

    let valid = TransitionValid {
        change_state: next_fsm,
        actions: vec![],
    };
    Transition::Valid(valid)
}

fn new_message_handshake(next_fsm: FsmState) -> Transition {
    let valid = TransitionValid {
        change_state: next_fsm,
        actions: vec![Action::CalculateQuorum],
    };
    Transition::Valid(valid)
}

fn state_repeat_in_handshake_event_approve(mut next_fsm: FsmState) -> Transition {
    next_fsm.balance = Some(SubStateBalanceMode::Phase);
    next_fsm.phase = Some(SubStatePhase::Alert);
    next_fsm.quorum = None;

    let valid = TransitionValid {
        change_state: next_fsm,
        actions: vec![Action::StopTimer],
    };
    Transition::Valid(valid)
}

fn state_repeat_out_handshake_event_approve(mut next_fsm: FsmState) -> Transition {
    next_fsm.balance = None;
    next_fsm.quorum = None;
    next_fsm.global = StateGlobal::Normal;

    let valid = TransitionValid {
        change_state: next_fsm,
        actions: vec![Action::StopTimer],
    };
    Transition::Valid(valid)
}

fn state_in_handshake_event_approve(mut next_fsm: FsmState) -> Transition {
    next_fsm.balance = Some(SubStateBalanceMode::Phase);
    next_fsm.phase = Some(SubStatePhase::Alert);
    next_fsm.quorum = None;

    let valid = TransitionValid {
        change_state: next_fsm,
        actions: vec![Action::StopTimer],
    };
    Transition::Valid(valid)
}

/// Transición: CheckQuorumIn -> RepeatHandshakeIn (No Quorum).
fn state_check_quorum_in_event_not_approve(mut next_fsm: FsmState) -> Transition {
    next_fsm.quorum = Some(SubStateQuorum::RepeatHandshakeIn);

    let valid = TransitionValid {
        change_state: next_fsm,
        actions: vec![],
    };
    Transition::Valid(valid)
}

/// Transición: CheckQuorumOut -> RepeatHandshakeOut (No Quorum).
fn state_check_quorum_out_event_not_approve(mut next_fsm: FsmState) -> Transition {
    next_fsm.quorum = Some(SubStateQuorum::RepeatHandshakeOut);

    let valid = TransitionValid {
        change_state: next_fsm,
        actions: vec![],
    };
    Transition::Valid(valid)
}

/// Transición: CheckQuorumIn -> Alert Phase (Approve).
fn state_check_quorum_in_event_approve_quorum(mut next_fsm: FsmState) -> Transition {
    next_fsm.balance = Some(SubStateBalanceMode::Phase);
    next_fsm.phase = Some(SubStatePhase::Alert);
    next_fsm.quorum = None;

    let valid = TransitionValid {
        change_state: next_fsm,
        actions: vec![Action::StopTimer],
    };
    Transition::Valid(valid)
}

/// Transición: CheckQuorumOut -> Normal (Approve).
fn state_check_quorum_out_event_approve_quorum(mut next_fsm: FsmState) -> Transition {
    next_fsm.balance = None;
    next_fsm.quorum = None;
    next_fsm.global = StateGlobal::Normal;

    let valid = TransitionValid {
        change_state: next_fsm,
        actions: vec![Action::StopTimer],
    };
    Transition::Valid(valid)
}

/// Transición: CheckQuorum -> SafeMode (No Quorum & No Attempts left).
fn state_check_quorum_event_not_and_not(mut next_fsm: FsmState) -> Transition {
    next_fsm.global = StateGlobal::SafeMode;
    next_fsm.balance = None;
    next_fsm.quorum = None;

    let valid = TransitionValid {
        change_state: next_fsm,
        actions: vec![],
    };
    Transition::Valid(valid)
}

/// Transición: RepeatHandshakeIn -> CheckQuorumIn.
fn state_repeat_handshake_in(mut next_fsm: FsmState) -> Transition {
    next_fsm.quorum = Some(SubStateQuorum::CheckQuorumIn);

    let valid = TransitionValid {
        change_state: next_fsm,
        actions: vec![],
    };
    Transition::Valid(valid)
}

/// Transición: RepeatHandshakeOut -> CheckQuorumOut.
fn state_repeat_handshake_out(mut next_fsm: FsmState) -> Transition {
    next_fsm.quorum = Some(SubStateQuorum::CheckQuorumOut);

    let valid = TransitionValid {
        change_state: next_fsm,
        actions: vec![],
    };
    Transition::Valid(valid)
}

/// Transición: Alert -> Data.
fn state_alert_event_timeout_or_quorum(mut next_fsm: FsmState) -> Transition {
    next_fsm.phase = Some(SubStatePhase::Data);

    let valid = TransitionValid {
        change_state: next_fsm,
        actions: vec![Action::StopTimer],
    };
    Transition::Valid(valid)
}

/// Transición: Data -> Monitor.
fn state_data_event_timeout_or_quorum(mut next_fsm: FsmState) -> Transition {
    next_fsm.phase = Some(SubStatePhase::Monitor);

    let valid = TransitionValid {
        change_state: next_fsm,
        actions: vec![Action::StopTimer],
    };
    Transition::Valid(valid)
}

/// Transición: Monitor -> OutHandshake.
fn state_monitor_event_timeout_or_quorum(mut next_fsm: FsmState) -> Transition {
    next_fsm.balance = Some(SubStateBalanceMode::OutHandshake);
    next_fsm.phase = None;

    let valid = TransitionValid {
        change_state: next_fsm,
        actions: vec![Action::StopSendHeartbeatMessagePhase, Action::StopTimer],
    };
    Transition::Valid(valid)
}

/// Transición: OutHandshake -> CheckQuorumOut.
fn state_out_handshake_event_timeout(mut next_fsm: FsmState) -> Transition {
    next_fsm.balance = Some(SubStateBalanceMode::Quorum);
    next_fsm.quorum = Some(SubStateQuorum::CheckQuorumOut);

    let valid = TransitionValid {
        change_state: next_fsm,
        actions: vec![],
    };
    Transition::Valid(valid)
}

fn state_out_handshake_event_approve(mut next_fsm: FsmState) -> Transition {
    next_fsm.balance = None;
    next_fsm.quorum = None;
    next_fsm.global = StateGlobal::Normal;

    let valid = TransitionValid {
        change_state: next_fsm,
        actions: vec![Action::StopTimer],
    };
    Transition::Valid(valid)
}

/// Calcula las acciones de entrada (`OnEntry...`) detectando cambios de estado.
///
/// Compara el estado anterior (`old`) con el nuevo (`new`) para identificar
/// qué sub-estados han cambiado e inyectar las acciones correspondientes.
fn compute_on_entry(old: &FsmState, new: &FsmState) -> Vec<Action> {
    let mut actions = Vec::new();

    if old.balance != new.balance {
        if let Some(s) = &new.balance {
            actions.push(Action::OnEntryBalance(s.clone()));
        }
    }

    if old.quorum != new.quorum {
        if let Some(s) = &new.quorum {
            actions.push(Action::OnEntryQuorum(s.clone()));
        }
    }

    if old.global != new.global && new.global == StateGlobal::Normal {
        actions.push(Action::OnEntryNormal);
    }

    if old.global != new.global && new.global == StateGlobal::SafeMode {
        actions.push(Action::OnEntrySafeMode);
    }

    if old.global != new.global && new.global == StateGlobal::Disconnected {
        actions.push(Action::OnEntryDisconnected);
    }

    if old.phase != new.phase {
        if let Some(s) = &new.phase {
            actions.push(Action::OnEntryPhase(s.clone()));
        }
    }

    actions
}

/// Tarea asíncrona dedicada al temporizador de seguridad (Watchdog).
///
/// Implementa un patrón "Dead Man's Switch". Espera un comando `InitTimer`.
/// Si el tiempo expira antes de recibir `StopTimer`, envía un evento `Timeout` a la FSM.
#[instrument(name = "fsm_watchdog_timer", skip_all)]
pub async fn fsm_watchdog_timer(
    tx_to_fsm: mpsc::Sender<Event>,
    mut cmd_rx: mpsc::Receiver<Event>,
    cancel: CancellationToken,
) {
    loop {
        let duration = match cmd_rx.recv().await {
            Some(Event::InitTimer(d)) => d,
            Some(Event::StopTimer) => continue, // Si ya estaba parado, ignorar
            None => break,                      // Canal cerrado, terminar tarea
            _ => continue,
        };

        // Estado ACTIVO: Corriendo temporizador
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("shutdown recibido fsm_watchdog_timer");
                break;
            }
            _ = sleep(duration) => {
                // El tiempo se agotó
                let _ = tx_to_fsm.send(Event::Timeout).await;
            }
            Some(Event::StopTimer) = cmd_rx.recv() => {
                debug!("Watchdog timer de fsm general, cancelado");
            }
        }
    }
}
