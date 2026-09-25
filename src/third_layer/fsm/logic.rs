use crate::config::fsm::PERCENTAGE;
use crate::context::domain::AppContext;
use crate::second_layer::database::domain::DataHandle;
use crate::second_layer::message::domain::{
    EdgeState, HandshakeToHub, Heartbeat, HubMessage, Metadata, PhaseNotification, StateToHub,
};
use crate::second_layer::message::{
    domain::{EmptyQueue, EmptyQueueSafeMode, HandshakeFromHub},
    logic::MessageHandle,
};
use crate::system::domain::InternalEvent;
use crate::third_layer::fsm::domain::{
    Action, Event, FsmState, StateOfSession, SubStateBalanceMode, SubStatePhase, SubStateQuorum,
    Transition, UpdateSession, fsm_watchdog_timer,
};
use crate::third_layer::quorum::domain::ProtocolSettings;
use chrono::Utc;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Duration, interval, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument};

enum State {
    Start,
    BalanceMode {
        epoch: u32,
        duration: u32,
        frequency: u32,
        jitter: u32,
    },
    Normal,
    SafeMode {
        frequency: u32,
        jitter: u32,
    },
    Disconnected,
}

/// Manejador (`Handle`) ligero para comunicarse con el actor de la `FSM`.
///
/// Permite encolar notificaciones (como handshakes de Hubs, avisos de colas vacías o
/// cambios de conectividad) hacia la Máquina de Estados Finita.
#[derive(Clone)]
pub struct FsmHandle {
    tx: mpsc::Sender<InternalFsmCommand>,
}

impl FsmHandle {
    pub async fn handshake(&self, data: HandshakeFromHub) {
        let cmd = InternalFsmCommand::Handshake { data };
        let _ = self.tx.send(cmd).await;
    }
    pub async fn queue(&self, data: EmptyQueue) {
        let cmd = InternalFsmCommand::Queue { data };
        let _ = self.tx.send(cmd).await;
    }
    pub async fn queue_safe(&self, data: EmptyQueueSafeMode) {
        let cmd = InternalFsmCommand::QueueSafe { data };
        let _ = self.tx.send(cmd).await;
    }
    pub async fn connection_event(&self, data: InternalEvent) {
        let cmd = InternalFsmCommand::ConnectionEvent { data };
        let _ = self.tx.send(cmd).await;
    }
    pub async fn create_runtime(&self) -> bool {
        let (response_tx, response_rx) = oneshot::channel();
        let cmd = InternalFsmCommand::CreateRuntime {
            respond_to: response_tx,
        };
        if self.tx.send(cmd).await.is_err() {
            return false;
        }
        match response_rx.await {
            Ok(_) => true,
            Err(_) => false,
        }
    }
}

enum InternalFsmCommand {
    Handshake { data: HandshakeFromHub },
    Queue { data: EmptyQueue },
    QueueSafe { data: EmptyQueueSafeMode },
    ConnectionEvent { data: InternalEvent },
    CreateRuntime { respond_to: oneshot::Sender<bool> },
}

enum EventFsm {
    Handshake(HandshakeFromHub),
    Queue(EmptyQueue),
    QueueSafe(EmptyQueueSafeMode),
    Connection(InternalEvent),
}

/// Orquestador y enrutador principal de la Máquina de Estados.
///
/// Gestiona la recepción de eventos concurrentes, el mantenimiento del `UpdateSession`
/// y despacha los eventos a la función de transición pura `next_state`.
pub struct FsmService {
    rx: mpsc::Receiver<InternalFsmCommand>,
    context: AppContext,
    message_handle: MessageHandle,
    db_handle: DataHandle,
}

struct FsmRuntime {
    handles: Vec<JoinHandle<()>>,
    cancel_token: CancellationToken,
    tx_command: mpsc::Sender<EventFsm>,
}

impl FsmService {
    pub fn new(
        context: AppContext,
        message_handle: MessageHandle,
        db_handle: DataHandle,
    ) -> (Self, FsmHandle) {
        let (tx, rx) = mpsc::channel(10);
        let service = Self {
            rx,
            context,
            message_handle,
            db_handle,
        };
        let handle = FsmHandle { tx };
        (service, handle)
    }

    fn spawn_runtime(&self) -> FsmRuntime {
        let token = CancellationToken::new();
        let mut handles = Vec::new();

        let (tx_command, rx_command) = mpsc::channel::<EventFsm>(50);
        let (tx_to_edge_state, rx_from_fsm_to_edge) = mpsc::channel::<State>(50);
        let (tx_to_fsm, rx_event) = mpsc::channel::<Event>(50);
        let (general_tx_to_timer, rx_from_general) = mpsc::channel::<Event>(50);
        let (general_tx_to_heartbeat, rx_heartbeat_from_general) = mpsc::channel::<Action>(50);
        let (tx_actions, rx_from_fsm) = mpsc::channel::<Vec<Action>>(50);
        let (tx_to_heartbeat, rx_from_heartbeat_watchdog) = mpsc::channel::<Event>(50);
        let (heartbeat_tx_to_timer, rx_from_heartbeat) = mpsc::channel::<Event>(50);

        let child_token = token.child_token();
        let general_tx_to_fsm = tx_to_fsm.clone();
        handles.push(tokio::spawn(handle_events_and_actions(
            general_tx_to_fsm,
            general_tx_to_timer,
            general_tx_to_heartbeat,
            tx_to_edge_state,
            rx_command,
            rx_from_fsm,
            self.context.clone(),
            self.db_handle.clone(),
            self.message_handle.clone(),
            child_token,
        )));

        let child_token = token.child_token();
        handles.push(tokio::spawn(edge_state(
            self.message_handle.clone(),
            rx_from_fsm_to_edge,
            self.context.clone(),
            child_token,
        )));

        let child_token = token.child_token();
        handles.push(tokio::spawn(run_fsm(tx_actions, rx_event, child_token)));

        let child_token = token.child_token();
        let timer_tx_to_fsm = tx_to_fsm.clone();
        handles.push(tokio::spawn(fsm_watchdog_timer(
            timer_tx_to_fsm,
            rx_from_general,
            child_token,
        )));

        let child_token = token.child_token();
        handles.push(tokio::spawn(heartbeat_generator_timer(
            tx_to_heartbeat,
            rx_from_heartbeat,
            child_token,
        )));

        let child_token = token.child_token();
        handles.push(tokio::spawn(heartbeat_generator(
            self.message_handle.clone(),
            heartbeat_tx_to_timer,
            rx_heartbeat_from_general,
            rx_from_heartbeat_watchdog,
            self.context.clone(),
            child_token,
        )));

        FsmRuntime {
            handles,
            cancel_token: token,
            tx_command,
        }
    }

    pub async fn run(mut self, shutdown: CancellationToken) {
        let mut runtime: Option<FsmRuntime> = None;

        loop {
            match runtime {
                Some(ref mut rt) => {
                    tokio::select! {
                        _ = shutdown.cancelled() => {
                            info!("shutdown recibido FsmService");
                            if let Some(rt) = runtime.take() {
                                rt.cancel_token.cancel();
                                for h in rt.handles {
                                    let _ = tokio::time::timeout(Duration::from_secs(2), h).await;
                                }
                            }
                            break;
                        }

                        Some(cmd) = self.rx.recv() => {
                            match cmd {
                                InternalFsmCommand::Handshake { data } => {
                                    if rt.tx_command.send(EventFsm::Handshake(data)).await.is_err() {
                                        error!("no se pudo enviar Handshake a fsm");
                                    }
                                }
                                InternalFsmCommand::Queue { data } => {
                                    if rt.tx_command.send(EventFsm::Queue(data)).await.is_err() {
                                        error!("no se pudo enviar Queue a fsm");
                                    }
                                }
                                InternalFsmCommand::QueueSafe { data } => {
                                    if rt.tx_command.send(EventFsm::QueueSafe(data)).await.is_err() {
                                        error!("no se pudo enviar QueueSafe a fsm");
                                    }
                                }
                                InternalFsmCommand::ConnectionEvent { data } => {
                                    if rt.tx_command.send(EventFsm::Connection(data)).await.is_err() {
                                        error!("no se pudo enviar Connection a fsm");
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
                None => {
                    tokio::select! {
                        _ = shutdown.cancelled() => {
                            info!("shutdown recibido FsmService");
                            break;
                        }

                        Some(cmd) = self.rx.recv() => {
                            match cmd {
                                InternalFsmCommand::CreateRuntime { respond_to } => {
                                    if runtime.is_none() {
                                        runtime = Some(self.spawn_runtime());
                                        let _ = respond_to.send(true);
                                    } else {
                                        let _ = respond_to.send(false);
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Tarea principal asíncrona que gestiona la orquestación de mensajes y eventos de la FSM.
///
/// Actúa como un **Event Loop** que escucha múltiples canales utilizando `tokio::select!`.
/// Prioriza la recepción de mensajes para mantener la reactividad del sistema.
///
/// # Canales Monitorizados
/// * `rx_from_hub`: Mensajes provenientes de los dispositivos Hubs.
/// * `rx_from_server`: Mensajes provenientes del servidor central.
/// * `rx_from_fsm`: Vectores de acciones generados por la lógica pura de la FSM (`run_fsm`) que deben ejecutarse.
///
/// # Lógica Principal
/// 1.  **Handshakes:** Gestiona la confirmación de épocas de balanceo.
/// 2.  **Quorum:** Evalúa mensajes `EmptyQueue` para determinar si el sistema puede transicionar de estado.
/// 3.  **Ping/Pong:** Responde automáticamente a solicitudes de diagnóstico de red.
/// 4.  **Ejecución de Acciones:** Delega las acciones recibidas a `handle_action`.
#[instrument(name = "handle_events_and_actions", skip_all)]
async fn handle_events_and_actions(
    tx_to_fsm: mpsc::Sender<Event>,
    tx_to_timer: mpsc::Sender<Event>,
    tx_to_heartbeat: mpsc::Sender<Action>,
    tx_to_edge_state: mpsc::Sender<State>,
    mut rx_command: mpsc::Receiver<EventFsm>,
    mut rx_from_fsm: mpsc::Receiver<Vec<Action>>,
    app_context: AppContext,
    db_handle: DataHandle,
    msg_handle: MessageHandle,
    cancel: CancellationToken,
) {
    info!("iniciando tarea handle_events_and_actions");
    let mut session: UpdateSession = UpdateSession::new();

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("shutdown recibido handle_events_and_actions");
                break;
            }

            Some(event) = rx_command.recv() => {
                handle_events(
                    event,
                    &tx_to_fsm,
                    &tx_to_timer,
                    &app_context,
                    &mut session,
                ).await;
            }

            Some(vec_action) = rx_from_fsm.recv() => {
                for action in vec_action {
                    handle_action(
                        action,
                        &app_context,
                        &tx_to_fsm,
                        &tx_to_timer,
                        &tx_to_heartbeat,
                        &tx_to_edge_state,
                        &mut session,
                        &db_handle,
                        &msg_handle,
                    ).await;
                }
            }
        }
    }
}

async fn handle_events(
    event: EventFsm,
    tx_to_fsm: &mpsc::Sender<Event>,
    tx_to_timer: &mpsc::Sender<Event>,
    app_context: &AppContext,
    session: &mut UpdateSession,
) {
    match event {
        EventFsm::Handshake(handshake) => match session.get_state() {
            StateOfSession::InHandshake
            | StateOfSession::OutHandshake
            | StateOfSession::RepeatHandshake => {
                if handshake.balance_epoch == session.get_epoch() {
                    session.insert_handshake(
                        handshake.metadata.sender_user_id,
                        handshake.balance_epoch,
                    );
                    if tx_to_fsm.send(Event::NewMessageHandshake).await.is_err() {
                        error!("no se pudo enviar evento NewMessageHandshake");
                    }
                }
            }
            _ => {}
        },
        EventFsm::Queue(queue) => match session.get_state() {
            StateOfSession::PhaseAlert
            | StateOfSession::PhaseData
            | StateOfSession::PhaseMonitor => {
                quorum_phase(
                    session,
                    &tx_to_fsm,
                    &app_context,
                    HubMessage::EmptyQueue(queue),
                    &tx_to_timer,
                )
                .await;
            }
            _ => {}
        },
        EventFsm::QueueSafe(queue) => match session.get_state() {
            StateOfSession::SafeMode => {
                quorum_safe_mode(
                    session,
                    &tx_to_fsm,
                    &app_context,
                    HubMessage::EmptyQueueSafe(queue),
                    &tx_to_timer,
                )
                .await;
            }
            _ => {}
        },
        EventFsm::Connection(con) => match con {
            InternalEvent::LocalConnected => {
                if tx_to_fsm.send(Event::LocalConnected).await.is_err() {
                    error!("no se pudo enviar comando LocalConnected");
                }
            }
            InternalEvent::LocalDisconnected => {
                if tx_to_fsm.send(Event::LocalDisconnected).await.is_err() {
                    error!("no se pudo enviar comando LocalDisconnected");
                }
            }
            _ => {}
        },
    }
}

/// Ejecutor de efectos secundarios (Side-Effects Handler).
///
/// Esta función recibe una `Action` abstracta (definida en el dominio de la FSM) y realiza
/// la operación concreta requerida. Esto desacopla la lógica de decisión de la implementación de E/S.
///
/// # Acciones Manejadas
/// * **Inicialización:** Configura timers y saluda al servidor.
/// * **Modos de Balanceo:** Gestiona la entrada/salida de handshakes y actualiza el epoch en DB.
/// * **Quorums:** Ejecuta los algoritmos de verificación de votos.
/// * **Fases:** Configura notificaciones de fase (Alerta, Datos, Monitor).
/// * **Heartbeats:** Controla el inicio y parada del generador de latidos.
async fn handle_action(
    action: Action,
    app_context: &AppContext,
    tx_to_fsm: &mpsc::Sender<Event>,
    tx_to_timer: &mpsc::Sender<Event>,
    tx_to_heartbeat: &mpsc::Sender<Action>,
    tx_to_edge_state: &mpsc::Sender<State>,
    session: &mut UpdateSession,
    db_handle: &DataHandle,
    msg_handle: &MessageHandle,
) {
    match action {
        Action::OnEntryBalance(sub_bm) => match sub_bm {
            SubStateBalanceMode::InitBalanceMode => {
                debug!("entrando a init_balance_mode");
                let res: bool;
                match db_handle.get_epoch().await {
                    Some(e) => {
                        session.set_epoch(e + 1);
                        res = db_handle.save_epoch(e + 1).await;
                    }
                    None => res = false,
                }
                if res {
                    if tx_to_fsm.send(Event::BalanceEpochOk).await.is_err() {
                        error!("no se pudo enviar Event::BalanceEpochOk");
                    }
                    let jitter = fastrand::u32(0..=5);
                    if tx_to_edge_state
                        .send(State::BalanceMode {
                            epoch: session.get_epoch(),
                            duration: 300,
                            frequency: app_context.quorum.get_frequency_phase(),
                            jitter,
                        })
                        .await
                        .is_err()
                    {
                        error!("no se pudo enviar State::BalanceMode a edge_state");
                    }
                } else {
                    if tx_to_fsm.send(Event::BalanceEpochNotOk).await.is_err() {
                        error!("no se pudo enviar Event::BalanceEpochNotOk");
                    }
                }
            }
            SubStateBalanceMode::InHandshake => {
                session.set_state(StateOfSession::InHandshake);
                let epoch = session.get_epoch();
                on_entry_in_handshake(msg_handle, tx_to_timer, &epoch, app_context.clone(), sub_bm)
                    .await;
            }
            SubStateBalanceMode::OutHandshake => {
                session.set_state(StateOfSession::OutHandshake);
                let epoch = session.get_epoch();
                on_entry_out_handshake(
                    msg_handle,
                    tx_to_timer,
                    &epoch,
                    app_context.clone(),
                    sub_bm,
                )
                .await;
            }
            _ => {}
        },
        Action::OnEntryQuorum(sub_q) => match sub_q {
            SubStateQuorum::CheckQuorumIn | SubStateQuorum::CheckQuorumOut => {
                debug!("entrando a estado CheckQuorum (In/Out)");
                session.set_state(StateOfSession::Quorum);
                quorum_algorithm(session, tx_to_fsm, app_context).await;
            }
            SubStateQuorum::RepeatHandshakeIn | SubStateQuorum::RepeatHandshakeOut => {
                session.set_state(StateOfSession::RepeatHandshake);
                session.increment_attempts();
                session.reset_handshake_hash();
                let epoch = session.get_epoch();
                on_entry_repeat_handshake(msg_handle, tx_to_timer, &epoch, app_context, sub_q)
                    .await;
            }
        },
        Action::OnEntryPhase(sub_p) => {
            session.reset_total_attempts();
            session.reset_handshake_hash();
            session.reset_empty_hash();
            match sub_p {
                SubStatePhase::Alert => {
                    session.set_state(StateOfSession::PhaseAlert);
                    let epoch = session.get_epoch();
                    on_entry_alert(
                        msg_handle,
                        tx_to_timer,
                        tx_to_heartbeat,
                        &epoch,
                        app_context,
                        &app_context.quorum,
                    )
                    .await;
                }
                SubStatePhase::Data => {
                    session.set_state(StateOfSession::PhaseData);
                    let epoch = session.get_epoch();
                    on_entry_data(
                        msg_handle,
                        tx_to_timer,
                        &epoch,
                        app_context,
                        &app_context.quorum,
                    )
                    .await;
                }
                SubStatePhase::Monitor => {
                    session.set_state(StateOfSession::PhaseMonitor);
                    let epoch = session.get_epoch();
                    on_entry_monitor(
                        msg_handle,
                        tx_to_timer,
                        &epoch,
                        app_context,
                        &app_context.quorum,
                    )
                    .await;
                }
            }
        }
        Action::OnEntryNormal => {
            on_entry_normal(tx_to_heartbeat).await;
            if tx_to_edge_state.send(State::Normal).await.is_err() {
                error!("no se pudo enviar State::Normal a edge_state");
            }
        }
        Action::OnEntrySafeMode => {
            session.reset_empty_hash();
            session.set_state(StateOfSession::SafeMode);
            on_entry_safe_mode(tx_to_timer, tx_to_heartbeat, app_context).await;
            let frequency = app_context.quorum.get_frequency_safe_mode();
            let jitter = fastrand::u32(0..=5);
            if tx_to_edge_state
                .send(State::SafeMode { frequency, jitter })
                .await
                .is_err()
            {
                error!("no se pudo enviar State::SafeMode a edge_state");
            }
        }
        Action::CalculateQuorum => {
            quorum_algorithm(session, tx_to_fsm, app_context).await;
        }
        Action::OnEntryDisconnected => {
            if tx_to_edge_state.send(State::Disconnected).await.is_err() {
                error!("no se pudo enviar State::Disconnected a edge_state");
            }
        }
        Action::StopTimer => {
            if tx_to_timer.send(Event::StopTimer).await.is_err() {
                error!("no se pudo enviar evento de finalización de watchdog de fsm general");
            }
        }
        Action::StopSendHeartbeatMessagePhase => {
            if tx_to_heartbeat
                .send(Action::StopSendHeartbeatMessagePhase)
                .await
                .is_err()
            {
                error!("no se pudo enviar acción StopSendHeartbeatMessagePhase");
            }
        }
        Action::StopSendHeartbeatMessageSafeMode => {
            if tx_to_heartbeat
                .send(Action::StopSendHeartbeatMessageSafeMode)
                .await
                .is_err()
            {
                error!("no se pudo enviar acción StopSendHeartbeatMessageSafeMode");
            }
        }
        Action::StopSendHeartbeatMessageNormal => {
            if tx_to_heartbeat
                .send(Action::StopSendHeartbeatMessageNormal)
                .await
                .is_err()
            {
                error!("no se pudo enviar acción StopSendHeartbeatMessageNormal");
            }
        }
        _ => {}
    }
}

/// Tarea asíncrona que ejecuta la lógica pura de la Máquina de Estados.
///
/// Mantiene el estado persistente (`FsmState`) y avanza pasos tras recibir eventos.
///
/// * `tx_actions`: Canal para emitir los efectos secundarios que deben ejecutarse.
/// * `rx_event`: Canal de entrada de eventos (triggers).
#[instrument(name = "run_fsm", skip_all)]
pub async fn run_fsm(
    tx_actions: mpsc::Sender<Vec<Action>>,
    mut rx_event: mpsc::Receiver<Event>,
    cancel: CancellationToken,
) {
    info!("iniciando tarea fsm");
    let mut state = FsmState::new();

    handle_transition(state.step(Event::Start), &mut state, &tx_actions).await;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("shutdown recibido run_fsm");
                break;
            }
            Some(event) = rx_event.recv() => {
                handle_transition(state.step(event), &mut state, &tx_actions).await;
            }
        }
    }
}

/// Watchdog Timer (Perro guardián) para el envío de Heartbeats.
///
/// Si este timer expira sin ser reseteado o detenido, envía un evento `Timeout`
/// que fuerza el envío de un nuevo latido.
#[instrument(name = "heartbeat_to_send_watchdog_timer", skip_all)]
pub async fn heartbeat_generator_timer(
    tx_to_heartbeat: mpsc::Sender<Event>,
    mut cmd_rx: mpsc::Receiver<Event>,
    cancel: CancellationToken,
) {
    info!("iniciando heartbeat_generator_timer");
    loop {
        let duration = match cmd_rx.recv().await {
            Some(Event::InitTimer(d)) => d,
            Some(Event::StopTimer) => continue,
            None => break,
            _ => continue,
        };

        tokio::select! {
            _ = cancel.cancelled() => {
                info!("shutdown recibido heartbeat_generator_timer");
                break;
            }
            _ = sleep(duration) => {
                if tx_to_heartbeat.send(Event::Timeout).await.is_err() {
                    error!("no se pudo enviar evento Timeout");
                }
            }
            Some(Event::StopTimer) = cmd_rx.recv() => { }
        }
    }
}

/// Generador de mensajes Heartbeat.
///
/// Gestiona la cadencia y el tipo de mensaje de latido (Heartbeat) enviado a los hubs
/// dependiendo del estado actual (Phase, Normal, SafeMode).
#[instrument(name = "heartbeat_generator", skip_all)]
pub async fn heartbeat_generator(
    handle: MessageHandle,
    tx_to_timer: mpsc::Sender<Event>,
    mut rx_from_fsm: mpsc::Receiver<Action>,
    mut cmd_rx: mpsc::Receiver<Event>,
    app_context: AppContext,
    cancel: CancellationToken,
) {
    info!("iniciando heartbeat_generator");
    enum HeartbeatState {
        BalanceMode,
        Normal,
        SafeMode,
        None,
    }

    let mut beat = HeartbeatState::None;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("shutdown recibido heartbeat_generator");
                break;
            }
            Some(action) = rx_from_fsm.recv() => {
                match action {
                    Action::SendHeartbeatMessagePhase => {
                        debug!("comenzando envío de Heartbeat para las fases");
                        beat = HeartbeatState::BalanceMode;
                        let duration = app_context.quorum.get_time_between_heartbeats_balance_mode();
                        if tx_to_timer.send(Event::InitTimer(Duration::from_secs(duration))).await.is_err() {
                            error!("no se pudo enviar evento InitTimer");
                        }
                    },
                    Action::SendHeartbeatMessageNormal => {
                        debug!("comenzando envío de Heartbeat para estado normal");
                        beat = HeartbeatState::Normal;
                        let duration = app_context.quorum.get_time_between_heartbeats_normal();
                        if tx_to_timer.send(Event::InitTimer(Duration::from_secs(duration))).await.is_err() {
                            error!("no se pudo enviar evento InitTimer");
                        }
                    },
                    Action::SendHeartbeatMessageSafeMode => {
                        debug!("comenzando envío de Heartbeat para estado safe_mode");
                        beat = HeartbeatState::SafeMode;
                        let duration = app_context.quorum.get_time_between_heartbeats_safe_mode();
                        if tx_to_timer.send(Event::InitTimer(Duration::from_secs(duration))).await.is_err() {
                            error!("no se pudo enviar evento InitTimer");
                        }
                    },
                    Action::StopSendHeartbeatMessagePhase => {
                        debug!("finalizando envío de Heartbeat para las fases");
                        if tx_to_timer.send(Event::StopTimer).await.is_err() {
                            error!("no se pudo enviar evento StopTimer");
                        }
                    }
                    Action::StopSendHeartbeatMessageSafeMode => {
                        debug!("finalizando envío de Heartbeat para estado safe_mode");
                        if tx_to_timer.send(Event::StopTimer).await.is_err() {
                            error!("no se pudo enviar evento StopTimer");
                        }
                    }
                    Action::StopSendHeartbeatMessageNormal => {
                        debug!("finalizando envío de Heartbeat para estado normal");
                        if tx_to_timer.send(Event::StopTimer).await.is_err() {
                            error!("no se pudo enviar evento StopTimer");
                        }
                    }
                    _ => {}
                }
            }

            Some(Event::Timeout) = cmd_rx.recv() => {
                send_heartbeat(&handle, &app_context).await;
                let duration = match beat {
                    HeartbeatState::BalanceMode => Duration::from_secs(app_context.quorum.get_time_between_heartbeats_balance_mode()),
                    HeartbeatState::Normal => Duration::from_secs(app_context.quorum.get_time_between_heartbeats_normal()),
                    HeartbeatState::SafeMode => Duration::from_secs(app_context.quorum.get_time_between_heartbeats_safe_mode()),
                    HeartbeatState::None => continue,
                };
                if tx_to_timer.send(Event::InitTimer(duration)).await.is_err() {
                    error!("no se pudo enviar evento de InitTimer");
                }
            }
        }
    }
}

/// Tarea asíncrona que gestiona el envío periódico del mensaje de estado al servidor y a los hub.
///
/// # Canal Monitorizado
/// * `rx_command`: Mensajes de tipo StateGlobal proveniente de `handle_action`.
///
#[instrument(name = "edge_state", skip_all)]
async fn edge_state(
    msg_handle: MessageHandle,
    mut rx_command: mpsc::Receiver<State>,
    app_context: AppContext,
    cancel: CancellationToken,
) {
    let mut state: State = State::Start;
    let mut ticker = interval(Duration::from_secs(10));

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("shutdown recibido edge_state");
                break;
            }

            _ = ticker.tick() => {
                match state {
                    State::BalanceMode { epoch, duration, frequency, jitter } => {
                        let metadata = build_metadata(&app_context, "all");
                        let data = EdgeState {
                            metadata: metadata.clone(),
                            state: "Balance".to_string(),
                        };
                        msg_handle.serialize_edge_state(data).await;
                        let state = StateToHub {
                            metadata,
                            state: "balance".to_string(),
                            balance_epoch: epoch,
                            duration,
                            frequency,
                            jitter,
                        };
                        msg_handle.serialize_state_hub(state).await;
                    },
                    // En estado Normal no se requieren los datos balance_epoch, duration, frequency ni jitter. Por ende, se ponen
                    // valores cero para completar la estructura del mensaje, pero no seran analizados por los Hubs.
                    State::Normal => {
                        let metadata = build_metadata(&app_context, "all");
                        let data = EdgeState {
                            metadata: metadata.clone(),
                            state: "Normal".to_string(),
                        };
                        msg_handle.serialize_edge_state(data).await;
                        let state = StateToHub {
                            metadata,
                            state: "normal".to_string(),
                            balance_epoch: 0,
                            duration: 0,
                            frequency: 0,
                            jitter: 0,
                        };
                        msg_handle.serialize_state_hub(state).await;
                    }
                    // En estado Normal no se requieren los datos balance_epoch ni duration. Por ende, se ponen
                    // valores cero para completar la estructura del mensaje, pero no seran analizados por los Hubs.
                    State::SafeMode { frequency, jitter } => {
                        let metadata = build_metadata(&app_context, "all");
                        let data = EdgeState {
                            metadata: metadata.clone(),
                            state: "SafeMode".to_string(),
                        };
                        msg_handle.serialize_edge_state(data).await;
                        let state = StateToHub {
                            metadata: metadata.clone(),
                            state: "safe".to_string(),
                            balance_epoch: 0,
                            duration: 0,
                            frequency,
                            jitter,
                        };
                        msg_handle.serialize_state_hub(state).await;
                    }
                    State::Disconnected => debug!("en estado Disconnected no hay nada para mandar!"),
                    _ => {}
                }
            }

            Some(msg) = rx_command.recv() => {
                state = msg;
            }
        }
    }
}

async fn init_timer(tx_to_timer: &mpsc::Sender<Event>, duration: Duration) {
    if tx_to_timer.send(Event::InitTimer(duration)).await.is_err() {
        error!("no se pudo enviar evento de inicialización del watchdog de fsm general");
    }
}

async fn on_entry_in_handshake(
    handle: &MessageHandle,
    tx_to_timer: &mpsc::Sender<Event>,
    current_epoch: &u32,
    app_context: AppContext,
    state: SubStateBalanceMode,
) {
    debug!("entrando a in_handshake");
    let metadata = build_metadata(&app_context, "all");
    send_handshake(handle, metadata, *current_epoch, state).await;
    init_timer(
        tx_to_timer,
        Duration::from_secs(app_context.quorum.get_timeout_handshake()),
    )
    .await;
}

async fn on_entry_out_handshake(
    handle: &MessageHandle,
    tx_to_timer: &mpsc::Sender<Event>,
    current_epoch: &u32,
    app_context: AppContext,
    state: SubStateBalanceMode,
) {
    debug!("entrando a out_handshake");
    let metadata = build_metadata(&app_context, "all");
    send_handshake(handle, metadata.clone(), *current_epoch, state).await;
    init_timer(
        tx_to_timer,
        Duration::from_secs(app_context.quorum.get_timeout_handshake()),
    )
    .await;
}

/// Algoritmo de Quorum para Handshakes.
///
/// Determina si suficientes Hubs han respondido para proceder al siguiente estado.
///
/// # Lógica
/// 1. Verifica si se han excedido los intentos máximos.
/// 2. Calcula el umbral de aceptación basado en `PERCENTAGE` y penalización por intentos.
/// 3. Envía `ApproveQuorum` o `NotApproveQuorum` a la FSM.
async fn quorum_algorithm(
    session: &mut UpdateSession,
    tx_to_fsm: &mpsc::Sender<Event>,
    app_context: &AppContext,
) {
    let max_attempts = app_context.quorum.get_max_attempts();
    let attempts = max_attempts as f64 - session.get_total_attempts();
    debug!("iniciando algoritmo de quorum. Intentos restantes: {attempts}");

    if session.get_total_attempts() > max_attempts as f64 {
        if tx_to_fsm.send(Event::NotApproveNotAttempts).await.is_err() {
            error!("no se pudo enviar evento NotApproveNotAttempts a la fsm general");
        }
    } else {
        let manager = app_context.net_man.read().await;
        let total_hubs = manager.get_total_hubs();
        drop(manager);
        let penalty = (session.get_total_attempts() - 1.0).max(0.0) * 5.0;
        let threshold = PERCENTAGE - penalty;

        let votes = (session.get_total_handshake() as f64 / total_hubs as f64) * 100.0;
        if (votes >= threshold) && threshold > 10.0 {
            debug!(
                "quorum aprobado con el {votes}% de {threshold}% esperado. Hubs totales: {total_hubs}"
            );
            if tx_to_fsm.send(Event::ApproveQuorum).await.is_err() {
                error!("no se pudo enviar evento ApproveQuorum a la fsm general");
            }
        } else {
            debug!(
                "quorum desaprobado con el {votes}% de {threshold}% esperado. Hubs totales: {total_hubs}"
            );
            if tx_to_fsm.send(Event::NotApproveQuorum).await.is_err() {
                error!("no se pudo enviar evento NotApproveQuorum a la fsm general");
            }
        }
    }
}

/// Algoritmo de Quorum para vaciado de colas (EmptyQueue).
///
/// Verifica si un porcentaje (>= 80%) de los hubs han reportado que sus colas están vacías.
/// Si se cumple, dispara el evento `QuorumPhase` y detiene el watchdog.
async fn quorum_phase(
    session: &mut UpdateSession,
    tx_to_fsm: &mpsc::Sender<Event>,
    app_context: &AppContext,
    msg: HubMessage,
    tx_to_timer: &mpsc::Sender<Event>,
) {
    debug!("iniciando quorum de fase");
    match msg {
        HubMessage::EmptyQueue(empty) => {
            if empty.queue_empty {
                session.insert_empty(empty.metadata.sender_user_id, empty.queue_empty);
                let manager = app_context.net_man.read().await;
                let total_hubs = manager.get_total_hubs();
                let percentage = (session.get_total_empty() as f64 / total_hubs as f64) * 100.0;
                drop(manager);
                if percentage >= 80.0 {
                    debug!("aprobado el QuorumPhase con el {percentage}%");
                    if tx_to_fsm.send(Event::QuorumPhase).await.is_err() {
                        error!("no se pudo enviar evento QuorumPhase a la fsm general");
                    }
                    if tx_to_timer.send(Event::StopTimer).await.is_err() {
                        error!(
                            "no se pudo enviar evento de finalización de watchdog de QuorumPhase"
                        );
                    }
                } else {
                    debug!("quorum de fase desaprobado con el {percentage}%");
                }
            }
        }
        _ => {}
    }
}

/// Algoritmo de Quorum para vaciado de colas (EmptyQueue).
///
/// Verifica si un porcentaje (>= 70%) de los hubs han reportado que sus colas están vacías.
/// Si se cumple, dispara el evento `QuorumSafeMode` y detiene el watchdog.
async fn quorum_safe_mode(
    session: &mut UpdateSession,
    tx_to_fsm: &mpsc::Sender<Event>,
    app_context: &AppContext,
    msg: HubMessage,
    tx_to_timer: &mpsc::Sender<Event>,
) {
    debug!("iniciando quorum de safe_mode");
    match msg {
        HubMessage::EmptyQueueSafe(empty) => {
            if empty.queue_empty {
                session.insert_empty(empty.metadata.sender_user_id, empty.queue_empty);
                let manager = app_context.net_man.read().await;
                let total_hubs = manager.get_total_hubs();
                let percentage = (session.get_total_empty() as f64 / total_hubs as f64) * 100.0;
                drop(manager);
                if percentage >= 70.0 {
                    debug!("quorum de SafeMode aprobado con el {percentage}%");
                    if tx_to_fsm.send(Event::QuorumSafeMode).await.is_err() {
                        error!("no se pudo enviar evento QuorumSafeMode a la fsm general");
                    }
                    if tx_to_timer.send(Event::StopTimer).await.is_err() {
                        error!(
                            "no se pudo enviar evento de finalización de watchdog de QuorumSafeMode"
                        );
                    }
                } else {
                    debug!("quorum de SafeMode desaprobado con el {percentage}%");
                }
            }
        }
        _ => {}
    }
}

async fn on_entry_repeat_handshake(
    handle: &MessageHandle,
    tx_to_timer: &mpsc::Sender<Event>,
    current_epoch: &u32,
    app_context: &AppContext,
    state: SubStateQuorum,
) {
    debug!("entrando a repeat_handshake");
    match state {
        SubStateQuorum::RepeatHandshakeIn => {
            let metadata = build_metadata(app_context, "all");
            let handshake = HandshakeToHub {
                metadata,
                flag: "in".to_string(),
                balance_epoch: *current_epoch,
            };
            handle.serialize_handshake_hub(handshake).await;
        }
        SubStateQuorum::RepeatHandshakeOut => {
            let metadata = build_metadata(app_context, "all");
            let handshake = HandshakeToHub {
                metadata,
                flag: "out".to_string(),
                balance_epoch: *current_epoch,
            };
            handle.serialize_handshake_hub(handshake).await;
        }
        _ => {}
    }

    init_timer(
        tx_to_timer,
        Duration::from_secs(app_context.quorum.get_timeout_handshake()),
    )
    .await;
}

async fn on_entry_alert(
    handle: &MessageHandle,
    tx_to_timer: &mpsc::Sender<Event>,
    tx_to_heartbeat: &mpsc::Sender<Action>,
    current_epoch: &u32,
    app_context: &AppContext,
    protocol_settings: &ProtocolSettings,
) {
    debug!("entrando a fase alert");
    init_timer(
        tx_to_timer,
        Duration::from_secs(app_context.quorum.get_timeout_phase()),
    )
    .await;
    let metadata = build_metadata(app_context, "all");
    send_phase_notification(
        handle,
        metadata,
        *current_epoch,
        "alert".to_string(),
        protocol_settings,
    )
    .await;

    if tx_to_heartbeat
        .send(Action::SendHeartbeatMessagePhase)
        .await
        .is_err()
    {
        error!("no se pudo enviar acción SendHeartbeatMessagePhase");
    }
}

async fn on_entry_data(
    handle: &MessageHandle,
    tx_to_timer: &mpsc::Sender<Event>,
    current_epoch: &u32,
    app_context: &AppContext,
    protocol_settings: &ProtocolSettings,
) {
    debug!("entrando a fase data");
    init_timer(
        tx_to_timer,
        Duration::from_secs(app_context.quorum.get_timeout_phase()),
    )
    .await;
    let metadata = build_metadata(app_context, "all");
    send_phase_notification(
        handle,
        metadata,
        *current_epoch,
        "data".to_string(),
        protocol_settings,
    )
    .await;
}

async fn on_entry_monitor(
    handle: &MessageHandle,
    tx_to_timer: &mpsc::Sender<Event>,
    current_epoch: &u32,
    app_context: &AppContext,
    protocol_settings: &ProtocolSettings,
) {
    debug!("entrando a fase monitor");
    init_timer(
        tx_to_timer,
        Duration::from_secs(app_context.quorum.get_timeout_phase()),
    )
    .await;
    let metadata = build_metadata(app_context, "all");
    send_phase_notification(
        handle,
        metadata,
        *current_epoch,
        "monitor".to_string(),
        protocol_settings,
    )
    .await;
}

async fn on_entry_normal(tx_to_heartbeat: &mpsc::Sender<Action>) {
    debug!("entrando a estado normal");
    if tx_to_heartbeat
        .send(Action::SendHeartbeatMessageNormal)
        .await
        .is_err()
    {
        error!("no se pudo enviar acción SendHeartbeatMessageNormal");
    }
}

async fn on_entry_safe_mode(
    tx_to_timer: &mpsc::Sender<Event>,
    tx_to_heartbeat: &mpsc::Sender<Action>,
    app_context: &AppContext,
) {
    debug!("entrando a estado safe_mode");
    init_timer(
        tx_to_timer,
        Duration::from_secs(app_context.quorum.get_timeout_safe_mode()),
    )
    .await;

    if tx_to_heartbeat
        .send(Action::SendHeartbeatMessageSafeMode)
        .await
        .is_err()
    {
        error!("no se pudo enviar acción SendHeartbeatMessageSafeMode");
    }
}

// ===== Funciones auxiliares =====

fn build_metadata(app_context: &AppContext, destination: &str) -> Metadata {
    Metadata {
        sender_user_id: app_context.system.id_edge.clone(),
        destination_id: destination.to_string(),
        timestamp: Utc::now().timestamp(),
    }
}

async fn send_handshake(
    handle: &MessageHandle,
    metadata: Metadata,
    epoch: u32,
    state: SubStateBalanceMode,
) {
    match state {
        SubStateBalanceMode::InHandshake => {
            let msg = HandshakeToHub {
                metadata,
                flag: "in".to_string(),
                balance_epoch: epoch,
            };
            handle.serialize_handshake_hub(msg).await;
        }
        SubStateBalanceMode::OutHandshake => {
            let msg = HandshakeToHub {
                metadata,
                flag: "out".to_string(),
                balance_epoch: epoch,
            };
            handle.serialize_handshake_hub(msg).await;
        }
        _ => {}
    }
}

async fn send_phase_notification(
    handle: &MessageHandle,
    metadata: Metadata,
    epoch: u32,
    phase: String,
    protocol_settings: &ProtocolSettings,
) {
    let jitter = fastrand::u32(0..=10);
    let phase = PhaseNotification {
        metadata,
        state: "balance_mode".to_string(),
        epoch,
        phase,
        frequency: protocol_settings.get_frequency_phase(),
        jitter,
    };
    handle.serialize_phase_hub(phase).await;
}

async fn send_heartbeat(handle: &MessageHandle, app_context: &AppContext) {
    let metadata = build_metadata(app_context, "all");
    let heartbeat = Heartbeat {
        metadata,
        beat: true,
    };
    handle.serialize_heartbeat_hub(heartbeat).await;
}

/// Procesa una transición de la FSM.
///
/// Si la transición es válida, actualiza el estado y transmite las acciones resultantes.
/// Si es inválida, loguea un error.
async fn handle_transition(
    transition: Transition,
    state: &mut FsmState,
    tx: &mpsc::Sender<Vec<Action>>,
) {
    match transition {
        Transition::Valid(t) => {
            *state = t.get_change_state();
            let _ = tx.send(t.get_actions()).await;
        }
        Transition::Invalid(t) => {
            error!("FSM transición inválida: {}", t.get_invalid());
        }
    }
}
