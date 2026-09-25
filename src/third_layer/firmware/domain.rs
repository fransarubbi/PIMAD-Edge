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

/// Comandos internos que recibe el orquestador `FirmwareService`.
enum InternalFirmwareCommand {
    /// Solicitud desde el servidor para actualizar los Hubs de una red específica.
    UpdateHub { data: UpdateHubFirmware },
    /// Acuse de recibo o respuesta de un Hub tras haber intentado actualizar su firmware.
    AckFromHub { data: FirmwareHubAck },
    /// Solicitud desde el servidor para actualizar el firmware del propio Edge.
    UpdateEdge { data: UpdateEdgeFirmware },
}

/// Manejador (`Handle`) ligero para comunicarse con el `FirmwareService`.
///
/// Permite enviar comandos asíncronos para iniciar procesos de actualización OTA
/// (Over-The-Air) tanto del propio Edge como de los Hubs.
#[derive(Clone)]
pub struct FirmwareHandle {
    tx: mpsc::Sender<InternalFirmwareCommand>,
}

impl FirmwareHandle {
    /// Inicia el proceso de actualización para una red completa de Hubs.
    pub async fn update_hubs(&self, data: UpdateHubFirmware) {
        let cmd = InternalFirmwareCommand::UpdateHub { data };
        let _ = self.tx.send(cmd).await;
    }
    /// Enruta la confirmación/resultado de actualización proveniente de un Hub.
    pub async fn ack_from_hub(&self, data: FirmwareHubAck) {
        let cmd = InternalFirmwareCommand::AckFromHub { data };
        let _ = self.tx.send(cmd).await;
    }
    /// Inicia el proceso de actualización del firmware del propio dispositivo Edge.
    pub async fn update_edge(&self, data: UpdateEdgeFirmware) {
        let cmd = InternalFirmwareCommand::UpdateEdge { data };
        let _ = self.tx.send(cmd).await;
    }
}

/// Orquestador central para la gestión de actualizaciones de firmware OTA.
///
/// Lanza y supervisa tareas concurrentes (tasks) separadas para gestionar
/// tanto las actualizaciones de los Hubs, como las del propio Edge, además
/// de manejar temporizadores (watchdogs) de seguridad.
pub struct FirmwareService {
    /// Canal receptor de los comandos provenientes de otras partes del sistema.
    rx: mpsc::Receiver<InternalFirmwareCommand>,
    /// Contexto global de la aplicación.
    context: AppContext,
    /// Handle para enviar respuestas o mensajes al servidor o a los Hubs vía MQTT/gRPC.
    message_handle: MessageHandle,
}

impl FirmwareService {
    /// Crea y enlaza el servicio con su manejador.
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

    /// Lanza el bucle de eventos asíncrono y los sub-procesos concurrentes.
    ///
    /// Se encarga de instanciar las tareas hijas:
    /// - `hub_ota`: Gestiona el rollout de un firmware a varios Hubs.
    /// - `edge_ota`: Gestiona la auto-actualización.
    /// - `firmware_watchdog_timer`: Temporizador de seguridad para abortar procesos bloqueados.
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

/// Eventos para controlar el temporizador asíncrono (Watchdog) del proceso OTA.
pub enum Event {
    /// Comando interno para iniciar el timer.
    /// Comando interno para iniciar el temporizador con la duración dada.
    InitTimer(Duration),
    /// Comando interno para detener el timer.
    /// Comando interno para detener o cancelar el temporizador activo.
    StopTimer,
    /// Señal emitida por el temporizador indicando que el tiempo ha expirado.
    Timeout,
}

/// Tarea asíncrona dedicada al temporizador de seguridad (Watchdog).
///
/// Implementa un patrón "Dead Man's Switch". Espera un comando `InitTimer`.
/// Si el tiempo expira antes de recibir `StopTimer`, envía un evento `Timeout` a la FSM.
/// Si el tiempo expira antes de recibir `StopTimer`, envía un evento `Timeout` a la FSM
/// o tarea controladora para abortar la operación que tardó demasiado.
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
