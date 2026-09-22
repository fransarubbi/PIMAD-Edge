use crate::context::domain::AppContext;
use crate::second_layer::message::domain::{FirmwareHubAck, UpdateEdgeFirmware, UpdateHubFirmware};
use crate::second_layer::message::logic::MessageHandle;
use crate::third_layer::firmware::logic::CommandToHubOta;
use crate::third_layer::firmware::logic::{edge_ota, hub_ota};
use tokio::{
    sync::mpsc,
    time::{Duration, sleep},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument};

enum InternalFirmwareCommand {
    UpdateHub { data: UpdateHubFirmware },
    AckFromHub { data: FirmwareHubAck },
    UpdateEdge { data: UpdateEdgeFirmware },
}

#[derive(Clone)]
pub struct FirmwareHandle {
    tx: mpsc::Sender<InternalFirmwareCommand>,
}

impl FirmwareHandle {
    pub async fn update_hubs(&self, data: UpdateHubFirmware) {
        let cmd = InternalFirmwareCommand::UpdateHub { data };
        let _ = self.tx.send(cmd).await;
    }
    pub async fn ack_from_hub(&self, data: FirmwareHubAck) {
        let cmd = InternalFirmwareCommand::AckFromHub { data };
        let _ = self.tx.send(cmd).await;
    }
    pub async fn update_edge(&self, data: UpdateEdgeFirmware) {
        let cmd = InternalFirmwareCommand::UpdateEdge { data };
        let _ = self.tx.send(cmd).await;
    }
}

pub struct FirmwareService {
    rx: mpsc::Receiver<InternalFirmwareCommand>,
    context: AppContext,
    message_handle: MessageHandle,
}

impl FirmwareService {
    pub fn new(context: AppContext, message_handle: MessageHandle) -> (Self, FirmwareHandle) {
        let (tx, rx) = mpsc::channel(10);
        let service = Self {
            rx,
            context,
            message_handle,
        };
        let handle = FirmwareHandle { tx };
        (service, handle)
    }

    pub async fn run(mut self, shutdown: CancellationToken) {
        let token = CancellationToken::new();

        let (tx_to_timer, rx_from_update_task) = mpsc::channel::<Event>(10);
        let (tx_to_update, rx_from_timer) = mpsc::channel::<Event>(10);
        let (tx_msg, rx_msg) = mpsc::channel::<CommandToHubOta>(10);
        let (tx_msg_edge, rx_cmd_edge) = mpsc::channel::<UpdateEdgeFirmware>(10);

        let child_token = token.child_token();
        tokio::spawn(hub_ota(
            tx_to_timer,
            rx_msg,
            rx_from_timer,
            self.context.clone(),
            self.message_handle.clone(),
            child_token,
        ));

        tokio::spawn(edge_ota(
            rx_cmd_edge,
            self.message_handle.clone(),
            self.context.clone(),
        ));

        let child_token = token.child_token();
        tokio::spawn(firmware_watchdog_timer(
            tx_to_update,
            rx_from_update_task,
            child_token,
        ));

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("shutdown recibido FirmwareService");
                    break;
                }

                Some(cmd) = self.rx.recv() => {
                    match cmd {
                        InternalFirmwareCommand::UpdateHub { data } => {
                            if tx_msg.send(CommandToHubOta::Request(data)).await.is_err() {
                                error!("no se pudo enviar comando Update a update_firmware_task");
                            }
                        }
                        InternalFirmwareCommand::AckFromHub { data } => {
                            if tx_msg.send(CommandToHubOta::Response(data)).await.is_err() {
                                error!("no se pudo enviar mensaje HubResponse a update_firmware_task");
                            }
                        }
                        InternalFirmwareCommand::UpdateEdge { data } => {
                            if tx_msg_edge.send(data).await.is_err() {
                                error!("no se pudo enviar comando Update a update_firmware_task");
                            }
                        }
                    }
                }
            }
        }
    }
}

pub enum Event {
    /// Comando interno para iniciar el timer.
    InitTimer(Duration),
    /// Comando interno para detener el timer.
    StopTimer,
    Timeout,
}

/// Tarea asíncrona dedicada al temporizador de seguridad (Watchdog).
///
/// Implementa un patrón "Dead Man's Switch". Espera un comando `InitTimer`.
/// Si el tiempo expira antes de recibir `StopTimer`, envía un evento `Timeout` a la FSM.
#[instrument(name = "firmware_watchdog_timer", skip(cmd_rx))]
async fn firmware_watchdog_timer(
    tx_to_fsm: mpsc::Sender<Event>,
    mut cmd_rx: mpsc::Receiver<Event>,
    cancel: CancellationToken,
) {
    loop {
        // Estado IDLE: Esperar comando de inicio
        let duration = match cmd_rx.recv().await {
            Some(Event::InitTimer(d)) => d,
            Some(Event::StopTimer) => continue, // Si ya estaba parado, ignorar
            None => break,                      // Canal cerrado, terminar tarea
            _ => continue,
        };

        // Estado ACTIVO: Corriendo temporizador
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("shutdown recibido firmware_watchdog_timer");
                break;
            }
            _ = sleep(duration) => {
                // El tiempo se agotó
                if tx_to_fsm.send(Event::Timeout).await.is_err() {
                    error!("no se pudo enviar evento Timeout");
                }
            }
            Some(Event::StopTimer) = cmd_rx.recv() => {
                debug!("watchdog timer de fsm firmware, cancelado");
            }
        }
    }
}
