//! # Módulo de Coordinación de la Máquina de Estados Finitos (FSM)
//!
//! Este módulo actúa como el **controlador principal** y la capa de "pegamento" entre la lógica pura
//! de la FSM, la comunicación de red (Hubs y Servidor) y los temporizadores del sistema.
//!
//! ## Arquitectura
//! El módulo sigue un patrón de diseño basado en actores utilizando canales de `tokio` (MPSC).
//! Se divide en las siguientes responsabilidades:
//!
//! 1.  **Orquestador de Canales (`fsm_general_channels`):** Es el bucle de eventos principal. Multiplexa
//!     mensajes entrantes de la red y comandos de la FSM lógica.
//! 2.  **Ejecutor de Efectos (`handle_action`):** Traduce las `Actions` abstractas de la FSM en operaciones
//!     concretas (enviar paquetes gRPC, iniciar timers, actualizar base de datos).
//! 3.  **Lógica de FSM (`run_fsm`):** Mantiene el estado actual y decide la siguiente transición basada en eventos.
//! 4.  **Subsistema de Heartbeats:** Genera latidos periódicos hacia los Hubs y monitorea timeouts.
//!

use crate::config::fsm::PERCENTAGE;
use crate::context::domain::AppContext;
use crate::fsm::domain::{
    Action, Event, FsmServiceCommand, FsmServiceResponse, FsmState, StateGlobal, StateOfSession,
    SubStateBalanceMode, SubStatePhase, SubStateQuorum, Transition, UpdateSession,
};

use crate::message::domain::{
    HandshakeToHub, Heartbeat, HubMessage, Metadata, PhaseNotification, StateToHub,
};
use crate::quorum::domain::ProtocolSettings;
use chrono::Utc;
use tokio::sync::mpsc;
use tokio::time::{Duration, interval, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument};

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
pub async fn handle_events_and_actions(
    tx_to_core: mpsc::Sender<FsmServiceResponse>,
    tx_to_fsm: mpsc::Sender<Event>,
    tx_to_timer: mpsc::Sender<Event>,
    tx_to_heartbeat: mpsc::Sender<Action>,
    tx_to_edge_state: mpsc::Sender<StateGlobal>,
    mut rx_command: mpsc::Receiver<FsmServiceCommand>,
    mut rx_from_fsm: mpsc::Receiver<Vec<Action>>,
    app_context: AppContext,
    cancel: CancellationToken,
) {
    info!("iniciando tarea handle_events_and_actions");
    let mut session: UpdateSession = UpdateSession::new();
    let mut current_epoch: u32 = 0;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("shutdown recibido handle_events_and_actions");
                break;
            }

            Some(msg) = rx_command.recv() => {
                handle_events(
                    msg,
                    &tx_to_core,
                    &tx_to_fsm,
                    &tx_to_timer,
                    &app_context,
                    &mut current_epoch,
                ).await;
            }

            Some(vec_action) = rx_from_fsm.recv() => {
                for action in vec_action {
                    handle_action(
                        action,
                        &app_context,
                        &tx_to_core,
                        &tx_to_fsm,
                        &tx_to_timer,
                        &tx_to_heartbeat,
                        &tx_to_edge_state,
                        &mut session,
                        &current_epoch,
                    ).await;
                }
            }
        }
    }
}

async fn handle_events(
    msg: FsmServiceCommand,
    tx_to_core: &mpsc::Sender<FsmServiceResponse>,
    tx_to_fsm: &mpsc::Sender<Event>,
    tx_to_timer: &mpsc::Sender<Event>,
    app_context: &AppContext,
    current_epoch: &mut u32,
) {
    let mut session: UpdateSession = UpdateSession::new();

    match msg {
        FsmServiceCommand::FromHub(hub_msg) => match hub_msg {
            HubMessage::HandshakeFromHub(handshake) => match session.get_state() {
                StateOfSession::InHandshake
                | StateOfSession::OutHandshake
                | StateOfSession::RepeatHandshake => {
                    if handshake.balance_epoch == *current_epoch {
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
            HubMessage::EmptyQueue(msg) => match session.get_state() {
                StateOfSession::PhaseAlert
                | StateOfSession::PhaseData
                | StateOfSession::PhaseMonitor => {
                    quorum_phase(
                        &mut session,
                        &tx_to_fsm,
                        &app_context,
                        HubMessage::EmptyQueue(msg),
                        &tx_to_timer,
                    )
                    .await;
                }
                _ => {}
            },
            HubMessage::EmptyQueueSafe(msg) => match session.get_state() {
                StateOfSession::SafeMode => {
                    quorum_safe_mode(
                        &mut session,
                        &tx_to_fsm,
                        &app_context,
                        HubMessage::EmptyQueueSafe(msg),
                        &tx_to_timer,
                    )
                    .await;
                }
                _ => {}
            },
            _ => {}
        },
        FsmServiceCommand::ErrorEpoch => {
            if tx_to_fsm.send(Event::BalanceEpochNotOk).await.is_err() {
                error!("no se pudo enviar comando BalanceEpochNotOk");
            }
        }
        FsmServiceCommand::Epoch(epoch) => {
            *current_epoch = epoch + 1;
            if tx_to_fsm.send(Event::BalanceEpochOk).await.is_err() {
                error!("no se pudo enviar comando BalanceEpochOk");
            }
            if tx_to_core
                .send(FsmServiceResponse::NewEpoch(*current_epoch))
                .await
                .is_err()
            {
                error!("no se pudo enviar comando NewEpoch");
            }
        }
        FsmServiceCommand::LocalConnected => {
            if tx_to_fsm.send(Event::LocalConnected).await.is_err() {
                error!("no se pudo enviar comando LocalConnected");
            }
        }
        FsmServiceCommand::LocalDisconnected => {
            if tx_to_fsm.send(Event::LocalDisconnected).await.is_err() {
                error!("no se pudo enviar comando LocalDisconnected");
            }
        }
        _ => {}
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
    tx_to_core: &mpsc::Sender<FsmServiceResponse>,
    tx_to_fsm: &mpsc::Sender<Event>,
    tx_to_timer: &mpsc::Sender<Event>,
    tx_to_heartbeat: &mpsc::Sender<Action>,
    tx_to_edge_state: &mpsc::Sender<StateGlobal>,
    session: &mut UpdateSession,
    current_epoch: &u32,
) {
    match action {
        Action::OnEntryBalance(sub_bm) => match sub_bm {
            SubStateBalanceMode::InitBalanceMode => {
                debug!("entrando a init_balance_mode");
                if tx_to_edge_state
                    .send(StateGlobal::BalanceMode)
                    .await
                    .is_err()
                {
                    error!("no se pudo enviar StateGlobal::BalanceMode a edge_state");
                }
                let mut flag = true;
                loop {
                    if tx_to_core.send(FsmServiceResponse::GetEpoch).await.is_err() {
                        error!("no se pudo enviar comando GetEpoch");
                        flag = false;
                    }
                    if flag {
                        break;
                    } else {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
            SubStateBalanceMode::InHandshake => {
                session.set_state(StateOfSession::InHandshake);
                on_entry_in_handshake(
                    tx_to_core,
                    tx_to_timer,
                    current_epoch,
                    app_context.clone(),
                    sub_bm,
                )
                .await;
            }
            SubStateBalanceMode::OutHandshake => {
                session.set_state(StateOfSession::OutHandshake);
                on_entry_out_handshake(
                    tx_to_core,
                    tx_to_timer,
                    current_epoch,
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
                on_entry_repeat_handshake(
                    tx_to_core,
                    tx_to_timer,
                    current_epoch,
                    app_context,
                    sub_q,
                )
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
                    on_entry_alert(
                        tx_to_core,
                        tx_to_timer,
                        tx_to_heartbeat,
                        current_epoch,
                        app_context,
                        &app_context.quorum,
                    )
                    .await;
                }
                SubStatePhase::Data => {
                    session.set_state(StateOfSession::PhaseData);
                    on_entry_data(
                        tx_to_core,
                        tx_to_timer,
                        current_epoch,
                        app_context,
                        &app_context.quorum,
                    )
                    .await;
                }
                SubStatePhase::Monitor => {
                    session.set_state(StateOfSession::PhaseMonitor);
                    on_entry_monitor(
                        tx_to_core,
                        tx_to_timer,
                        current_epoch,
                        app_context,
                        &app_context.quorum,
                    )
                    .await;
                }
            }
        }
        Action::OnEntryNormal => {
            on_entry_normal(tx_to_heartbeat).await;
            if tx_to_edge_state.send(StateGlobal::Normal).await.is_err() {
                error!("no se pudo enviar StateGlobal::Normal a edge_state");
            }
        }
        Action::OnEntrySafeMode => {
            session.reset_empty_hash();
            session.set_state(StateOfSession::SafeMode);
            on_entry_safe_mode(tx_to_timer, tx_to_heartbeat, app_context).await;
            if tx_to_edge_state.send(StateGlobal::SafeMode).await.is_err() {
                error!("no se pudo enviar StateGlobal::SafeMode a edge_state");
            }
        }
        Action::CalculateQuorum => {
            quorum_algorithm(session, tx_to_fsm, app_context).await;
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
    tx_to_core: mpsc::Sender<FsmServiceResponse>,
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
                send_heartbeat(&tx_to_core, &app_context).await;
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
pub async fn edge_state(
    tx: mpsc::Sender<FsmServiceResponse>,
    mut rx_command: mpsc::Receiver<StateGlobal>,
    app_context: AppContext,
    cancel: CancellationToken,
) {
    let mut state: StateGlobal = StateGlobal::Start;
    let mut ticker = interval(Duration::from_secs(10));

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("shutdown recibido edge_state");
                break;
            }

            _ = ticker.tick() => {
                match state {
                    StateGlobal::BalanceMode => {
                        if tx.send(FsmServiceResponse::EdgeState("Balance".to_string())).await.is_err() {
                            error!("no se pudo enviar mensaje EdgeState periódico");
                        }
                        let metadata = build_metadata(&app_context, "all");
                        let state = StateToHub {
                            metadata,
                            state: "balance".to_string(),
                            balance_epoch: 0,
                            duration: 0,
                            frequency: 0,
                            jitter: 0,
                        };
                        if tx.send(FsmServiceResponse::ToHub(HubMessage::StateToHub(state))).await.is_err() {
                            error!("no se pudo enviar mensaje EdgeState periódico");
                        }
                    },
                    StateGlobal::Normal => {
                        if tx.send(FsmServiceResponse::EdgeState("Normal".to_string())).await.is_err() {
                            error!("no se pudo enviar mensaje EdgeState periódico");
                        }
                        let metadata = build_metadata(&app_context, "all");
                        let state = StateToHub {
                            metadata,
                            state: "normal".to_string(),
                            balance_epoch: 0,
                            duration: 0,
                            frequency: 0,
                            jitter: 0,
                        };
                        if tx.send(FsmServiceResponse::ToHub(HubMessage::StateToHub(state))).await.is_err() {
                            error!("no se pudo enviar mensaje EdgeState periódico");
                        }
                    }
                    StateGlobal::SafeMode => {
                        if tx.send(FsmServiceResponse::EdgeState("SafeMode".to_string())).await.is_err() {
                            error!("no se pudo enviar mensaje EdgeState periódico");
                        }
                        let metadata = build_metadata(&app_context, "all");
                        let jitter = fastrand::u32(0..=5);
                        let state = StateToHub {
                            metadata: metadata.clone(),
                            state: "safe".to_string(),
                            balance_epoch: 0,
                            duration: 0,
                            frequency: app_context.quorum.get_frequency_safe_mode(),
                            jitter,
                        };
                        if tx.send(FsmServiceResponse::ToHub(HubMessage::StateToHub(state))).await.is_err() {
                            error!("no se pudo enviar mensaje EdgeState periódico");
                        }
                    }
                    _ => {}
                }
            }

            Some(msg) = rx_command.recv() => {
                match msg {
                    StateGlobal::BalanceMode => {
                        state = StateGlobal::BalanceMode;
                    },
                    StateGlobal::Normal => {
                        state = StateGlobal::Normal;
                    },
                    StateGlobal::SafeMode => {
                        state = StateGlobal::SafeMode;
                    }
                    _ => {}
                }
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
    tx_to_core: &mpsc::Sender<FsmServiceResponse>,
    tx_to_timer: &mpsc::Sender<Event>,
    current_epoch: &u32,
    app_context: AppContext,
    state: SubStateBalanceMode,
) {
    debug!("entrando a in_handshake");
    let metadata = build_metadata(&app_context, "all");
    send_handshake(tx_to_core, metadata, *current_epoch, state).await;
    init_timer(
        tx_to_timer,
        Duration::from_secs(app_context.quorum.get_timeout_handshake()),
    )
    .await;
}

async fn on_entry_out_handshake(
    tx_to_core: &mpsc::Sender<FsmServiceResponse>,
    tx_to_timer: &mpsc::Sender<Event>,
    current_epoch: &u32,
    app_context: AppContext,
    state: SubStateBalanceMode,
) {
    debug!("entrando a out_handshake");
    let metadata = build_metadata(&app_context, "all");
    send_handshake(tx_to_core, metadata.clone(), *current_epoch, state).await;
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
    tx_to_core: &mpsc::Sender<FsmServiceResponse>,
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
            if tx_to_core
                .send(FsmServiceResponse::ToHub(HubMessage::HandshakeToHub(
                    handshake,
                )))
                .await
                .is_err()
            {
                error!("no se pudo enviar mensaje HandshakeToHub");
            }
        }
        SubStateQuorum::RepeatHandshakeOut => {
            let metadata = build_metadata(app_context, "all");
            let handshake = HandshakeToHub {
                metadata,
                flag: "out".to_string(),
                balance_epoch: *current_epoch,
            };
            if tx_to_core
                .send(FsmServiceResponse::ToHub(HubMessage::HandshakeToHub(
                    handshake,
                )))
                .await
                .is_err()
            {
                error!("no se pudo enviar mensaje HandshakeToHub");
            }
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
    tx_to_core: &mpsc::Sender<FsmServiceResponse>,
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
        tx_to_core,
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
    tx_to_core: &mpsc::Sender<FsmServiceResponse>,
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
        tx_to_core,
        metadata,
        *current_epoch,
        "data".to_string(),
        protocol_settings,
    )
    .await;
}

async fn on_entry_monitor(
    tx_to_core: &mpsc::Sender<FsmServiceResponse>,
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
        tx_to_core,
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
    tx: &mpsc::Sender<FsmServiceResponse>,
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
            if tx
                .send(FsmServiceResponse::ToHub(HubMessage::HandshakeToHub(msg)))
                .await
                .is_err()
            {
                error!("no se pudo enviar mensaje HandshakeToHub");
            }
        }
        SubStateBalanceMode::OutHandshake => {
            let msg = HandshakeToHub {
                metadata,
                flag: "out".to_string(),
                balance_epoch: epoch,
            };
            if tx
                .send(FsmServiceResponse::ToHub(HubMessage::HandshakeToHub(msg)))
                .await
                .is_err()
            {
                error!("no se pudo enviar mensaje HandshakeToHub");
            }
        }
        _ => {}
    }
}

async fn send_phase_notification(
    tx_to_hub: &mpsc::Sender<FsmServiceResponse>,
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

    if tx_to_hub
        .send(FsmServiceResponse::ToHub(HubMessage::PhaseNotification(
            phase,
        )))
        .await
        .is_err()
    {
        error!("no se pudo enviar PhaseNotification");
    }
}

async fn send_heartbeat(tx_to_core: &mpsc::Sender<FsmServiceResponse>, app_context: &AppContext) {
    let metadata = build_metadata(app_context, "all");
    let heartbeat = Heartbeat {
        metadata,
        beat: true,
    };

    if tx_to_core
        .send(FsmServiceResponse::ToHub(HubMessage::Heartbeat(heartbeat)))
        .await
        .is_err()
    {
        error!("no se pudo enviar Heartbeat");
    }
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
