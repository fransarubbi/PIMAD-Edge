//! Dominio de la FSM del Heartbeat.
//!
//! Este módulo define la lógica pura y las estructuras de datos necesarias para gestionar
//! el monitoreo de la conexión con el servidor (Heartbeat).
//!
//! # Arquitectura
//!
//! Se implementa una **Máquina de Estados Finita (FSM)** que evalúa eventos (`Heartbeat`, `Timeout`)
//! y determina transiciones de estado. La FSM es **pura**, lo que significa que no ejecuta
//! efectos secundarios directamente (como esperar tiempo o enviar mensajes de red), sino que
//! devuelve una lista de `Action` que el *runtime* debe ejecutar.
//!
//! # Ciclo de Vida
//!
//! 1. **StartingWait:** Estado inicial o de reinicio. Espera una señal.
//! 2. **ItsAlive:** El servidor respondió correctamente.
//! 3. **NotHeartbeatYet:** Se perdió un latido (advertencia).
//! 4. **DeadServer:** Se agotó el tiempo de espera máximo (desconexión confirmada).

use crate::heartbeat::logic::{heartbeat, run_fsm_heartbeat};
use crate::message::domain::Heartbeat;
use crate::system::domain::InternalEvent;
use tokio::sync::mpsc;
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument};

#[derive(Clone)]
pub struct HeartbeatHandle {
    tx: mpsc::Sender<Heartbeat>,
}

impl HeartbeatHandle {
    pub async fn send_heartbeat(&self, data: Heartbeat) {
        let _ = self.tx.send(data).await;
    }
}

pub struct HeartbeatService {
    sender: mpsc::Sender<InternalEvent>,
    rx: mpsc::Receiver<Heartbeat>,
}

impl HeartbeatService {
    pub fn new(sender: mpsc::Sender<InternalEvent>, rx: mpsc::Receiver<Heartbeat>) -> Self {
        Self { sender, rx }
    }

    pub async fn run(mut self, shutdown: CancellationToken) {
        let (tx_to_core, mut rx) = mpsc::channel::<InternalEvent>(10);
        let (tx_to_fsm, rx_from_heartbeat) = mpsc::channel::<Event>(10);
        let (tx_to_timer, rx_watchdog_heartbeat) = mpsc::channel::<Event>(10);
        let (tx_msg, rx_from_server) = mpsc::channel::<Heartbeat>(10);
        let (tx_actions, rx_fsm) = mpsc::channel::<Vec<Action>>(50);

        let heartbeat_tx_to_fsm = tx_to_fsm.clone();
        tokio::spawn(heartbeat(
            tx_to_core,
            heartbeat_tx_to_fsm,
            tx_to_timer,
            rx_from_server,
            rx_fsm,
            shutdown.clone(),
        ));

        tokio::spawn(run_fsm_heartbeat(
            tx_actions,
            rx_from_heartbeat,
            shutdown.clone(),
        ));

        let watchdog_tx_to_fsm = tx_to_fsm.clone();
        tokio::spawn(watchdog_timer_for_heartbeat(
            watchdog_tx_to_fsm,
            rx_watchdog_heartbeat,
            shutdown.clone(),
        ));

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("shutdown recibido HeartbeatService");
                    break;
                }

                Some(msg) = self.rx.recv() => {
                    if tx_msg.send(msg).await.is_err() {
                        error!("no se pudo enviar mensaje de Heartbeat proveniente del servidor");
                    }
                }
                Some(msg) = rx.recv() => {
                    if self.sender.send(msg).await.is_err() {
                        error!("no se pudo enviar el InternalEvent proveniente de heartbeat");
                    }
                }
            }
        }
    }
}

/// Estructura principal que mantiene el estado de la FSM del Heartbeat.
#[derive(Debug, Clone)]
pub struct FsmHeartbeat {
    /// Estado interno actual de la lógica de heartbeat.
    state: State,
    /// Estado de conexión anterior (utilizado para detectar cambios y notificar).
    old_status: Status,
    /// Estado de conexión actual calculado.
    status: Status,
}

/// Estados internos de la máquina de estados.
/// Determinan la salud de la recepción de heartbeats.
#[derive(Debug, Clone, PartialEq)]
pub enum State {
    /// Esperando el primer heartbeat o reiniciando el ciclo.
    StartingWait,
    /// El servidor está activo y enviando heartbeats correctamente.
    ItsAlive,
    /// El servidor ha sido declarado muerto/desconectado tras múltiples fallos.
    DeadServer,
    /// Estado de transición/advertencia: El temporizador venció, pero aún no se declara muerte total
    NotHeartbeatYet,
}

/// Estado de la conexión
#[derive(Debug, Clone, PartialEq)]
pub enum Status {
    Disconnected,
    Connected,
}

/// Eventos que alimentan a la FSM y provocan transiciones.
pub enum Event {
    /// Se recibió un mensaje de Heartbeat desde el servidor.
    Heartbeat,
    /// El temporizador de vigilancia (Watchdog) expiró.
    Timeout,
    /// Comando interno para iniciar el temporizador.
    InitTimer(Duration),
    /// Comando interno para detener el temporizador.
    StopTimer,
}

/// Acciones o Efectos Secundarios (Side Effects).
/// Instrucciones que la FSM genera para que el Runtime las ejecute.
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    /// Solicita enviar una notificación de cambio de estado (ej. de Conectado a Desconectado)
    /// solo si los estados difieren.
    SendStatusConditional(Status, Status),
    /// Indica que se ha entrado a un nuevo estado (útil para iniciar timers asociados a ese estado).
    OnEntry(State),
    /// No se requiere ninguna acción.
    Nothing,
    /// Solicita detener el temporizador.
    StopTimer,
}

/// Resultado de una transición válida.
/// Contiene el nuevo estado de la FSM y las acciones a ejecutar.
pub struct TransitionValid {
    change_state: FsmHeartbeat,
    action: Vec<Action>,
}

impl TransitionValid {
    pub fn get_change_state(&self) -> FsmHeartbeat {
        self.change_state.clone()
    }
    pub fn get_actions(&self) -> Vec<Action> {
        self.action.clone()
    }
}

/// Resultado de una transición inválida (error de lógica o evento inesperado).
pub struct TransitionInvalid {
    invalid: String,
}

impl TransitionInvalid {
    pub fn get_invalid(&self) -> &str {
        &self.invalid
    }
}

pub enum Transition {
    Valid(TransitionValid),
    Invalid(TransitionInvalid),
}

impl FsmHeartbeat {
    /// Crea una nueva instancia de la FSM en estado inicial desconectado.
    pub fn new() -> Self {
        Self {
            state: State::StartingWait,
            old_status: Status::Disconnected,
            status: Status::Disconnected,
        }
    }

    /// Lógica interna de despacho de eventos.
    /// Mapea el par `(Estado Actual, Evento)` a una función de transición específica.
    pub fn step_inner(&self, event: Event) -> Transition {
        match (&self.state, event) {
            (State::StartingWait, Event::Heartbeat) => {
                let next_fsm = self.clone();
                state_starting_wait_event_heartbeat(next_fsm)
            }
            (State::StartingWait, Event::Timeout) => {
                let next_fsm = self.clone();
                state_starting_wait_event_timeout(next_fsm)
            }
            (State::ItsAlive, Event::Heartbeat) => {
                let next_fsm = self.clone();
                state_its_alive_event_heartbeat(next_fsm)
            }
            (State::ItsAlive, Event::Timeout) => {
                let next_fsm = self.clone();
                state_its_alive_event_timeout(next_fsm)
            }
            (State::NotHeartbeatYet, Event::Heartbeat) => {
                let next_fsm = self.clone();
                state_not_heartbeat_yet_event_heartbeat(next_fsm)
            }
            (State::NotHeartbeatYet, Event::Timeout) => {
                let next_fsm = self.clone();
                state_not_heartbeat_yet_event_timeout(next_fsm)
            }
            (State::DeadServer, Event::Heartbeat) => {
                let next_fsm = self.clone();
                state_dead_server_event_heartbeat(next_fsm)
            }
            _ => invalid(),
        }
    }

    /// Función principal de transición (API Pública).
    ///
    /// Ejecuta la transición interna y calcula automáticamente las acciones `OnEntry`
    /// si el estado ha cambiado.
    pub fn step(&self, event: Event) -> Transition {
        let transition = self.step_inner(event);

        match transition {
            Transition::Valid(mut valid) => {
                let entry_action = compute_on_entry(self, &valid.change_state);
                if entry_action != Action::Nothing {
                    valid.action.push(entry_action);
                }
                Transition::Valid(valid)
            }
            invalid => invalid,
        }
    }
}

// --- Funciones de Transición Específicas ---

/// Transición: StartingWait + Heartbeat -> ItsAlive.
/// Se establece conexión exitosa.
fn state_starting_wait_event_heartbeat(mut next_fsm: FsmHeartbeat) -> Transition {
    let old_status = next_fsm.old_status.clone();
    next_fsm.state = State::ItsAlive;
    next_fsm.old_status = Status::Connected;
    next_fsm.status = Status::Connected;
    let valid = TransitionValid {
        change_state: next_fsm.clone(),
        action: vec![
            Action::SendStatusConditional(old_status, next_fsm.status), // (Disconnected, Connected)
            Action::StopTimer,
        ],
    };
    Transition::Valid(valid)
}

/// Transición: StartingWait + Timeout -> NotHeartbeatYet.
/// Primer fallo al esperar.
fn state_starting_wait_event_timeout(mut next_fsm: FsmHeartbeat) -> Transition {
    next_fsm.state = State::NotHeartbeatYet;
    let valid = TransitionValid {
        change_state: next_fsm,
        action: vec![],
    };
    Transition::Valid(valid)
}

/// Transición: ItsAlive + Heartbeat -> StartingWait.
/// Reinicia el ciclo de espera tras recibir un latido válido.
fn state_its_alive_event_heartbeat(mut next_fsm: FsmHeartbeat) -> Transition {
    next_fsm.state = State::StartingWait;
    let valid = TransitionValid {
        change_state: next_fsm.clone(),
        action: vec![Action::StopTimer],
    };
    Transition::Valid(valid)
}

/// Transición: ItsAlive + Timeout -> NotHeartbeatYet.
/// El servidor estaba vivo, pero se agotó el tiempo esperando el siguiente latido.
fn state_its_alive_event_timeout(mut next_fsm: FsmHeartbeat) -> Transition {
    next_fsm.state = State::NotHeartbeatYet;
    let valid = TransitionValid {
        change_state: next_fsm.clone(),
        action: vec![],
    };
    Transition::Valid(valid)
}

/// Transición: NotHeartbeatYet + Heartbeat -> StartingWait.
/// Recuperación exitosa antes de declarar muerte total.
fn state_not_heartbeat_yet_event_heartbeat(mut next_fsm: FsmHeartbeat) -> Transition {
    next_fsm.state = State::StartingWait;
    let valid = TransitionValid {
        change_state: next_fsm.clone(),
        action: vec![Action::StopTimer],
    };
    Transition::Valid(valid)
}

/// Transición: NotHeartbeatYet + Timeout -> DeadServer.
/// Fallo definitivo. Se marca el servidor como desconectado.
fn state_not_heartbeat_yet_event_timeout(mut next_fsm: FsmHeartbeat) -> Transition {
    let old_status = next_fsm.old_status.clone();
    next_fsm.state = State::DeadServer;
    next_fsm.old_status = Status::Disconnected;
    next_fsm.status = Status::Disconnected;
    let valid = TransitionValid {
        change_state: next_fsm.clone(),
        action: vec![Action::SendStatusConditional(old_status, next_fsm.status)], // (Connected, Disconnected)
    };
    Transition::Valid(valid)
}

/// Transición: DeadServer + Heartbeat -> StartingWait.
/// El servidor revivió tras haber estado muerto.
fn state_dead_server_event_heartbeat(mut next_fsm: FsmHeartbeat) -> Transition {
    next_fsm.state = State::StartingWait;
    let valid = TransitionValid {
        change_state: next_fsm.clone(),
        action: vec![],
    };
    Transition::Valid(valid)
}

/// Helper para generar una transición inválida genérica.
fn invalid() -> Transition {
    let invalid = TransitionInvalid {
        invalid: "Invalid state".to_string(),
    };
    Transition::Invalid(invalid)
}

/// Calcula la acción `OnEntry` comparando el estado anterior y el nuevo.
/// Si el estado cambia, genera la acción para inicializar los recursos del nuevo estado.
fn compute_on_entry(old: &FsmHeartbeat, new: &FsmHeartbeat) -> Action {
    if old.state != new.state {
        let state = new.state.clone();
        return Action::OnEntry(state);
    }
    Action::Nothing
}

/// Tarea asíncrona del Temporizador de Vigilancia (Watchdog).
///
/// Gestiona la espera. Si no recibe un comando `StopTimer` o una reinicialización antes
/// de que expire `duration`, envía un evento `Event::Timeout` a la FSM para indicar fallo.
///
/// # Argumentos
/// * `tx_to_fsm`: Canal para notificar el Timeout a la FSM.
/// * `cmd_rx`: Canal para recibir órdenes (`InitTimer`, `StopTimer`).
#[instrument(name = "watchdog_timer_for_heartbeat", skip_all)]
pub async fn watchdog_timer_for_heartbeat(
    tx_to_fsm: mpsc::Sender<Event>,
    mut cmd_rx: mpsc::Receiver<Event>,
    shutdown: CancellationToken,
) {
    loop {
        let duration = match cmd_rx.recv().await {
            Some(Event::InitTimer(d)) => d,
            Some(Event::StopTimer) => continue, // Si ya estaba parado, ignorar
            None => break,                      // Canal cerrado, terminar tarea
            _ => continue,
        };

        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("shutdown recibido watchdog_timer_for_heartbeat");
                break;
            }
            _ = sleep(duration) => {
                // El tiempo se agotó
                let _ = tx_to_fsm.send(Event::Timeout).await;
            }
            Some(Event::StopTimer) = cmd_rx.recv() => {
                debug!("Watchdog timer de fsm heartbeat, cancelado");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contains_action(actions: &[Action], target: &Action) -> bool {
        actions.iter().any(|a| a == target)
    }

    // ============================================================================
    // TESTS DE CREACIÓN Y ESTADO INICIAL
    // ============================================================================

    #[test]
    fn test_new_fsm_initial_state() {
        let fsm = FsmHeartbeat::new();
        assert_eq!(fsm.state, State::StartingWait);
        assert_eq!(fsm.status, Status::Disconnected);
        assert_eq!(fsm.old_status, Status::Disconnected);
    }

    // ============================================================================
    // TESTS DE ESTADO StartingWait
    // ============================================================================

    #[test]
    fn test_starting_wait_on_heartbeat() {
        let fsm = FsmHeartbeat::new();
        let transition = fsm.step(Event::Heartbeat);

        match transition {
            Transition::Valid(valid) => {
                let new_fsm = valid.get_change_state();
                let actions = valid.get_actions();

                assert_eq!(new_fsm.state, State::ItsAlive);
                assert_eq!(new_fsm.status, Status::Connected);
                assert_eq!(new_fsm.old_status, Status::Connected);

                // Debe enviar status condicional
                assert!(contains_action(
                    &actions,
                    &Action::SendStatusConditional(Status::Disconnected, Status::Connected)
                ));

                // Debe detener el timer
                assert!(contains_action(&actions, &Action::StopTimer));

                // Debe tener OnEntry
                assert!(contains_action(&actions, &Action::OnEntry(State::ItsAlive)));

                assert_eq!(actions.len(), 3);
            }
            _ => panic!("Se esperaba una transicion valida"),
        }
    }

    #[test]
    fn test_starting_wait_on_timeout() {
        let fsm = FsmHeartbeat::new();
        let transition = fsm.step(Event::Timeout);

        match transition {
            Transition::Valid(valid) => {
                let new_fsm = valid.get_change_state();
                let actions = valid.get_actions();

                assert_eq!(new_fsm.state, State::NotHeartbeatYet);
                assert_eq!(new_fsm.status, Status::Disconnected);

                // Solo debe tener OnEntry
                assert!(contains_action(
                    &actions,
                    &Action::OnEntry(State::NotHeartbeatYet)
                ));
                assert_eq!(actions.len(), 1);
            }
            _ => panic!("Se esperaba una transicion valida"),
        }
    }

    #[test]
    fn test_starting_wait_on_init_timer() {
        let fsm = FsmHeartbeat::new();
        let transition = fsm.step(Event::InitTimer(Duration::from_secs(1)));

        match transition {
            Transition::Invalid(invalid) => {
                assert_eq!(invalid.get_invalid(), "Invalid state");
            }
            _ => panic!("Expected invalid transition"),
        }
    }

    #[test]
    fn test_starting_wait_on_stop_timer() {
        let fsm = FsmHeartbeat::new();
        let transition = fsm.step(Event::StopTimer);

        match transition {
            Transition::Invalid(invalid) => {
                assert_eq!(invalid.get_invalid(), "Invalid state");
            }
            _ => panic!("Se esperaba una transicion invalida"),
        }
    }

    // ============================================================================
    // TESTS DE ESTADO ItsAlive
    // ============================================================================

    #[test]
    fn test_its_alive_on_heartbeat() {
        let mut fsm = FsmHeartbeat::new();
        fsm.state = State::ItsAlive;
        fsm.status = Status::Connected;
        fsm.old_status = Status::Connected;

        let transition = fsm.step(Event::Heartbeat);

        match transition {
            Transition::Valid(valid) => {
                let new_fsm = valid.get_change_state();
                let actions = valid.get_actions();

                assert_eq!(new_fsm.state, State::StartingWait);
                assert_eq!(new_fsm.status, Status::Connected);

                // Debe detener timer
                assert!(contains_action(&actions, &Action::StopTimer));

                // Debe tener OnEntry
                assert!(contains_action(
                    &actions,
                    &Action::OnEntry(State::StartingWait)
                ));

                assert_eq!(actions.len(), 2);
            }
            _ => panic!("Se esperaba una transicion valida"),
        }
    }

    #[test]
    fn test_its_alive_on_timeout() {
        let mut fsm = FsmHeartbeat::new();
        fsm.state = State::ItsAlive;
        fsm.status = Status::Connected;

        let transition = fsm.step(Event::Timeout);

        match transition {
            Transition::Valid(valid) => {
                let new_fsm = valid.get_change_state();
                let actions = valid.get_actions();

                assert_eq!(new_fsm.state, State::NotHeartbeatYet);
                assert_eq!(new_fsm.status, Status::Connected);

                // Solo OnEntry
                assert!(contains_action(
                    &actions,
                    &Action::OnEntry(State::NotHeartbeatYet)
                ));
                assert_eq!(actions.len(), 1);
            }
            _ => panic!("Se esperaba una transicion valida"),
        }
    }

    #[test]
    fn test_its_alive_on_init_timer() {
        let mut fsm = FsmHeartbeat::new();
        fsm.state = State::ItsAlive;

        let transition = fsm.step(Event::InitTimer(Duration::from_secs(1)));

        match transition {
            Transition::Invalid(_) => {}
            _ => panic!("Se esperaba una transicion invalida"),
        }
    }

    #[test]
    fn test_its_alive_on_stop_timer() {
        let mut fsm = FsmHeartbeat::new();
        fsm.state = State::ItsAlive;

        let transition = fsm.step(Event::StopTimer);

        match transition {
            Transition::Invalid(_) => {}
            _ => panic!("Se esperaba una transicion invalida"),
        }
    }

    // ============================================================================
    // TESTS DE ESTADO NotHeartbeatYet
    // ============================================================================

    #[test]
    fn test_not_heartbeat_yet_on_heartbeat() {
        let mut fsm = FsmHeartbeat::new();
        fsm.state = State::NotHeartbeatYet;
        fsm.status = Status::Disconnected;

        let transition = fsm.step(Event::Heartbeat);

        match transition {
            Transition::Valid(valid) => {
                let new_fsm = valid.get_change_state();
                let actions = valid.get_actions();

                assert_eq!(new_fsm.state, State::StartingWait);

                // Debe detener timer
                assert!(contains_action(&actions, &Action::StopTimer));

                // Debe tener OnEntry
                assert!(contains_action(
                    &actions,
                    &Action::OnEntry(State::StartingWait)
                ));

                assert_eq!(actions.len(), 2);
            }
            _ => panic!("Se esperaba una transicion valida"),
        }
    }

    #[test]
    fn test_not_heartbeat_yet_on_timeout_from_connected() {
        let mut fsm = FsmHeartbeat::new();
        fsm.state = State::NotHeartbeatYet;
        fsm.old_status = Status::Connected;
        fsm.status = Status::Connected;

        let transition = fsm.step(Event::Timeout);

        match transition {
            Transition::Valid(valid) => {
                let new_fsm = valid.get_change_state();
                let actions = valid.get_actions();

                assert_eq!(new_fsm.state, State::DeadServer);
                assert_eq!(new_fsm.status, Status::Disconnected);
                assert_eq!(new_fsm.old_status, Status::Disconnected);

                // Debe enviar cambio de estado
                assert!(contains_action(
                    &actions,
                    &Action::SendStatusConditional(Status::Connected, Status::Disconnected)
                ));

                // Debe tener OnEntry
                assert!(contains_action(
                    &actions,
                    &Action::OnEntry(State::DeadServer)
                ));

                assert_eq!(actions.len(), 2);
            }
            _ => panic!("Se esperaba una transicion valida"),
        }
    }

    #[test]
    fn test_not_heartbeat_yet_on_timeout_already_disconnected() {
        let mut fsm = FsmHeartbeat::new();
        fsm.state = State::NotHeartbeatYet;
        fsm.old_status = Status::Disconnected;
        fsm.status = Status::Disconnected;

        let transition = fsm.step(Event::Timeout);

        match transition {
            Transition::Valid(valid) => {
                let actions = valid.get_actions();

                // Debe enviar status condicional aunque sea igual
                assert!(contains_action(
                    &actions,
                    &Action::SendStatusConditional(Status::Disconnected, Status::Disconnected)
                ));

                assert_eq!(actions.len(), 2);
            }
            _ => panic!("Se esperaba una transicion valida"),
        }
    }

    #[test]
    fn test_not_heartbeat_yet_on_init_timer() {
        let mut fsm = FsmHeartbeat::new();
        fsm.state = State::NotHeartbeatYet;

        let transition = fsm.step(Event::InitTimer(Duration::from_secs(1)));

        match transition {
            Transition::Invalid(_) => {}
            _ => panic!("Se esperaba una transicion invalida"),
        }
    }

    #[test]
    fn test_not_heartbeat_yet_on_stop_timer() {
        let mut fsm = FsmHeartbeat::new();
        fsm.state = State::NotHeartbeatYet;

        let transition = fsm.step(Event::StopTimer);

        match transition {
            Transition::Invalid(_) => {}
            _ => panic!("Se esperaba una transicion invalida"),
        }
    }

    // ============================================================================
    // TESTS DE ESTADO DeadServer
    // ============================================================================

    #[test]
    fn test_dead_server_on_heartbeat() {
        let mut fsm = FsmHeartbeat::new();
        fsm.state = State::DeadServer;
        fsm.status = Status::Disconnected;

        let transition = fsm.step(Event::Heartbeat);

        match transition {
            Transition::Valid(valid) => {
                let new_fsm = valid.get_change_state();
                let actions = valid.get_actions();

                assert_eq!(new_fsm.state, State::StartingWait);

                // Solo OnEntry
                assert!(contains_action(
                    &actions,
                    &Action::OnEntry(State::StartingWait)
                ));
                assert_eq!(actions.len(), 1);
            }
            _ => panic!("Se esperaba una transicion valida"),
        }
    }

    #[test]
    fn test_dead_server_on_timeout() {
        let mut fsm = FsmHeartbeat::new();
        fsm.state = State::DeadServer;

        let transition = fsm.step(Event::Timeout);

        match transition {
            Transition::Invalid(_) => {}
            _ => panic!("Se esperaba una transicion invalida"),
        }
    }

    #[test]
    fn test_dead_server_on_init_timer() {
        let mut fsm = FsmHeartbeat::new();
        fsm.state = State::DeadServer;

        let transition = fsm.step(Event::InitTimer(Duration::from_secs(1)));

        match transition {
            Transition::Invalid(_) => {}
            _ => panic!("Se esperaba una transicion invalida"),
        }
    }

    #[test]
    fn test_dead_server_on_stop_timer() {
        let mut fsm = FsmHeartbeat::new();
        fsm.state = State::DeadServer;

        let transition = fsm.step(Event::StopTimer);

        match transition {
            Transition::Invalid(_) => {}
            _ => panic!("Se esperaba una transicion invalida"),
        }
    }

    // ============================================================================
    // Test de secuencia completa
    // ============================================================================

    #[test]
    fn test_full_connection_sequence() {
        let mut fsm = FsmHeartbeat::new();

        // StartingWait -> Heartbeat -> ItsAlive
        let transition = fsm.step(Event::Heartbeat);
        match transition {
            Transition::Valid(valid) => {
                fsm = valid.get_change_state();
                assert_eq!(fsm.state, State::ItsAlive);
                assert_eq!(fsm.status, Status::Connected);
            }
            _ => panic!("Se esperaba una transicion valida"),
        }

        // ItsAlive -> Heartbeat -> StartingWait (mantiene Connected)
        let transition = fsm.step(Event::Heartbeat);
        match transition {
            Transition::Valid(valid) => {
                fsm = valid.get_change_state();
                assert_eq!(fsm.state, State::StartingWait);
                assert_eq!(fsm.status, Status::Connected);
            }
            _ => panic!("Se esperaba una transicion valida"),
        }
    }

    #[test]
    fn test_timeout_sequence_to_dead() {
        let mut fsm = FsmHeartbeat::new();

        // StartingWait -> Timeout -> NotHeartbeatYet
        let transition = fsm.step(Event::Timeout);
        match transition {
            Transition::Valid(valid) => {
                fsm = valid.get_change_state();
                assert_eq!(fsm.state, State::NotHeartbeatYet);
            }
            _ => panic!("Se esperaba una transicion valida"),
        }

        // NotHeartbeatYet -> Timeout -> DeadServer
        let transition = fsm.step(Event::Timeout);
        match transition {
            Transition::Valid(valid) => {
                fsm = valid.get_change_state();
                assert_eq!(fsm.state, State::DeadServer);
                assert_eq!(fsm.status, Status::Disconnected);
            }
            _ => panic!("Se esperaba una transicion valida"),
        }
    }

    #[test]
    fn test_recovery_from_dead_server() {
        let mut fsm = FsmHeartbeat::new();
        fsm.state = State::DeadServer;
        fsm.status = Status::Disconnected;

        // DeadServer -> Heartbeat -> StartingWait
        let transition = fsm.step(Event::Heartbeat);
        match transition {
            Transition::Valid(valid) => {
                fsm = valid.get_change_state();
                assert_eq!(fsm.state, State::StartingWait);
            }
            _ => panic!("Se esperaba una transicion valida"),
        }

        // StartingWait -> Heartbeat -> ItsAlive + Connected
        let transition = fsm.step(Event::Heartbeat);
        match transition {
            Transition::Valid(valid) => {
                let actions = valid.get_actions();
                fsm = valid.get_change_state();

                assert_eq!(fsm.state, State::ItsAlive);
                assert_eq!(fsm.status, Status::Connected);

                // Debe notificar cambio de estado
                assert!(contains_action(
                    &actions,
                    &Action::SendStatusConditional(Status::Disconnected, Status::Connected)
                ));
            }
            _ => panic!("Se esperaba una transicion valida"),
        }
    }

    #[test]
    fn test_late_heartbeat_recovery() {
        let mut fsm = FsmHeartbeat::new();

        // StartingWait -> Timeout -> NotHeartbeatYet
        let transition = fsm.step(Event::Timeout);
        match transition {
            Transition::Valid(valid) => {
                fsm = valid.get_change_state();
            }
            _ => panic!("Se esperaba una transicion valida"),
        }

        // NotHeartbeatYet -> Heartbeat -> StartingWait (recuperación)
        let transition = fsm.step(Event::Heartbeat);
        match transition {
            Transition::Valid(valid) => {
                let actions = valid.get_actions();
                fsm = valid.get_change_state();

                assert_eq!(fsm.state, State::StartingWait);
                assert!(contains_action(&actions, &Action::StopTimer));
            }
            _ => panic!("Se esperaba una transicion valida"),
        }
    }

    #[test]
    fn test_connection_to_disconnection_sequence() {
        let mut fsm = FsmHeartbeat::new();

        // Conectar
        fsm = fsm.step(Event::Heartbeat).unwrap_valid().get_change_state();
        assert_eq!(fsm.status, Status::Connected);

        // ItsAlive -> Timeout -> NotHeartbeatYet (sigue Connected)
        fsm = fsm.step(Event::Timeout).unwrap_valid().get_change_state();
        assert_eq!(fsm.state, State::NotHeartbeatYet);
        assert_eq!(fsm.status, Status::Connected);

        // NotHeartbeatYet -> Timeout -> DeadServer (cambia a Disconnected)
        let transition = fsm.step(Event::Timeout);
        match transition {
            Transition::Valid(valid) => {
                let actions = valid.get_actions();
                fsm = valid.get_change_state();

                assert_eq!(fsm.state, State::DeadServer);
                assert_eq!(fsm.status, Status::Disconnected);

                // Debe notificar cambio
                assert!(contains_action(
                    &actions,
                    &Action::SendStatusConditional(Status::Connected, Status::Disconnected)
                ));
            }
            _ => panic!("Se esperaba una transicion valida"),
        }
    }

    #[test]
    fn test_multiple_heartbeats_while_alive() {
        let mut fsm = FsmHeartbeat::new();

        // Primer heartbeat: StartingWait -> ItsAlive
        fsm = fsm.step(Event::Heartbeat).unwrap_valid().get_change_state();
        assert_eq!(fsm.state, State::ItsAlive);

        // Heartbeats subsecuentes: ItsAlive -> StartingWait -> ItsAlive
        for _ in 0..5 {
            fsm = fsm.step(Event::Heartbeat).unwrap_valid().get_change_state();
            assert_eq!(fsm.state, State::StartingWait);

            fsm = fsm.step(Event::Heartbeat).unwrap_valid().get_change_state();
            assert_eq!(fsm.state, State::ItsAlive);
            assert_eq!(fsm.status, Status::Connected);
        }
    }

    // ===== Tests de compute_on_entry =====

    #[test]
    fn test_on_entry_same_state() {
        let fsm1 = FsmHeartbeat::new();
        let fsm2 = FsmHeartbeat::new();

        let action = compute_on_entry(&fsm1, &fsm2);
        assert_eq!(action, Action::Nothing);
    }

    #[test]
    fn test_on_entry_different_states() {
        let fsm1 = FsmHeartbeat::new();
        let mut fsm2 = FsmHeartbeat::new();
        fsm2.state = State::ItsAlive;

        let action = compute_on_entry(&fsm1, &fsm2);
        assert_eq!(action, Action::OnEntry(State::ItsAlive));
    }

    #[test]
    fn test_on_entry_all_states() {
        let fsm_base = FsmHeartbeat::new();

        let states = vec![
            State::StartingWait,
            State::ItsAlive,
            State::NotHeartbeatYet,
            State::DeadServer,
        ];

        for state in states {
            let mut fsm_new = FsmHeartbeat::new();
            fsm_new.state = state.clone();

            if fsm_base.state != state {
                let action = compute_on_entry(&fsm_base, &fsm_new);
                assert_eq!(action, Action::OnEntry(state));
            }
        }
    }

    // ===== Tests async del watchdog timer =====

    #[tokio::test]
    async fn test_watchdog_timer_timeout() {
        let (tx_to_fsm, mut rx_from_timer) = mpsc::channel(10);
        let (tx_cmd, rx_cmd) = mpsc::channel(10);

        let shutdown_token = CancellationToken::new();
        tokio::spawn(async move {
            watchdog_timer_for_heartbeat(tx_to_fsm, rx_cmd, shutdown_token).await;
        });

        // Iniciar timer
        tx_cmd
            .send(Event::InitTimer(Duration::from_millis(50)))
            .await
            .unwrap();

        // Esperar timeout
        match tokio::time::timeout(Duration::from_millis(200), rx_from_timer.recv()).await {
            Ok(Some(Event::Timeout)) => {}
            _ => panic!("Se esperaba timeout"),
        }
    }

    #[tokio::test]
    async fn test_watchdog_timer_cancel() {
        let (tx_to_fsm, mut rx_from_timer) = mpsc::channel(10);
        let (tx_cmd, rx_cmd) = mpsc::channel(10);

        let shutdown_token = CancellationToken::new();
        tokio::spawn(async move {
            watchdog_timer_for_heartbeat(tx_to_fsm, rx_cmd, shutdown_token).await;
        });

        // Iniciar timer
        tx_cmd
            .send(Event::InitTimer(Duration::from_millis(200)))
            .await
            .unwrap();

        // Cancelar
        sleep(Duration::from_millis(10)).await;
        tx_cmd.send(Event::StopTimer).await.unwrap();

        // No debe recibir timeout
        match tokio::time::timeout(Duration::from_millis(300), rx_from_timer.recv()).await {
            Err(_) => {}
            Ok(_) => panic!("No se deberia poder recibir eventos despues de cancelar"),
        }
    }

    #[tokio::test]
    async fn test_watchdog_timer_multiple_cycles() {
        let (tx_to_fsm, mut rx_from_timer) = mpsc::channel(10);
        let (tx_cmd, rx_cmd) = mpsc::channel(10);

        let shutdown_token = CancellationToken::new();
        tokio::spawn(async move {
            watchdog_timer_for_heartbeat(tx_to_fsm, rx_cmd, shutdown_token).await;
        });

        for _ in 0..3 {
            tx_cmd
                .send(Event::InitTimer(Duration::from_millis(50)))
                .await
                .unwrap();
            match tokio::time::timeout(Duration::from_millis(100), rx_from_timer.recv()).await {
                Ok(Some(Event::Timeout)) => {}
                _ => panic!("Se esperaba timeout"),
            }
        }
    }

    #[tokio::test]
    async fn test_watchdog_timer_stop_before_init() {
        let (tx_to_fsm, _rx_from_timer) = mpsc::channel(10);
        let (tx_cmd, rx_cmd) = mpsc::channel(10);

        let shutdown_token = CancellationToken::new();
        tokio::spawn(async move {
            watchdog_timer_for_heartbeat(tx_to_fsm, rx_cmd, shutdown_token).await;
        });

        // Enviar StopTimer sin InitTimer primero (debe ignorarse)
        tx_cmd.send(Event::StopTimer).await.unwrap();

        // Luego iniciar normalmente
        tx_cmd
            .send(Event::InitTimer(Duration::from_millis(50)))
            .await
            .unwrap();

        // Debe funcionar normalmente
        sleep(Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn test_watchdog_timer_channel_closed() {
        let (tx_to_fsm, _rx_from_timer) = mpsc::channel(10);
        let (tx_cmd, rx_cmd) = mpsc::channel(10);

        let shutdown_token = CancellationToken::new();
        let handle = tokio::spawn(async move {
            watchdog_timer_for_heartbeat(tx_to_fsm, rx_cmd, shutdown_token).await;
        });

        // Cerrar canal
        drop(tx_cmd);

        // El watchdog debe terminar
        tokio::time::timeout(Duration::from_millis(100), handle)
            .await
            .expect("El watchdog debe terminar cuando el canal se cierra")
            .expect("La tarea termina");
    }

    // ===== Helper trait para unwrap más limpio =====

    trait TransitionExt {
        fn unwrap_valid(self) -> TransitionValid;
    }

    impl TransitionExt for Transition {
        fn unwrap_valid(self) -> TransitionValid {
            match self {
                Transition::Valid(v) => v,
                Transition::Invalid(i) => {
                    panic!("Se esperaba una transicion valida: {}", i.get_invalid())
                }
            }
        }
    }
}
