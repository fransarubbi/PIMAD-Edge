//! # Módulo Core: Orquestador Central del Sistema
//!
//! Este módulo implementa el componente `Core`, que actúa como el **Bus de Mensajes Central** y
//! el coordinador principal de la aplicación (patrón *Mediator*).
//!
//! ## Responsabilidad
//! Su única responsabilidad es recibir mensajes de los distintos micro-servicios internos
//! (Data, Firmware, FSM, Red, etc.) y enrutarlos hacia su destino correcto.
//!
//! ## Arquitectura
//! El sistema sigue una arquitectura de estrella donde todos los servicios se comunican únicamente
//! con el `Core`, y el `Core` redistribuye los mensajes. Esto desacopla los servicios entre sí.
//!
//! - **Entradas:** `mpsc::Receiver` (El Core escucha estos canales).
//! - **Salidas:** `mpsc::Sender` (El Core envía comandos a través de estos canales).
//! - **Concurrencia:** Utiliza `tokio::select!` para multiplexar todos los canales de entrada en un único hilo de ejecución (Event Loop).

use crate::database::domain::DataHandle;
use crate::firmware::domain::FirmwareHandle;
use crate::fsm::logic::FsmHandle;
use crate::heartbeat::domain::HeartbeatHandle;
use crate::message::{
    domain::{HubMessage, ServerMessage},
    logic::{MessageHandle, MessageServiceResponse},
};
use crate::mqtt::domain::MqttServiceCommand;
use crate::network::domain::NetworkServiceResponse;
use crate::system::domain::InternalEvent;
use crate::{database::domain::TableDataVector, network::domain::NetworkHandle};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

/// Estructura principal que mantiene los canales de comunicación.
///
/// No contiene lógica de negocio compleja, solo la topología de la red de canales
/// y las reglas de enrutamiento en su método `run`.
pub struct Core {
    core_from_data_service: mpsc::Receiver<TableDataVector>,
    core_to_firmware_service: FirmwareHandle,
    core_to_fsm_service: FsmHandle,
    core_to_message_service: MessageHandle,
    core_to_data_service: DataHandle,
    core_from_heartbeat_service: mpsc::Receiver<InternalEvent>,
    core_to_heartbeat_service: HeartbeatHandle,
    core_from_message_service: mpsc::Receiver<MessageServiceResponse>,
    core_from_network_service: mpsc::Receiver<NetworkServiceResponse>,
    core_to_network_service: NetworkHandle,
}

/// Builder para construir la estructura `Core`.
///
/// Dado que `Core` tiene múltiples dependencias estrictas (canales), este builder
/// asegura que todos los canales sean inyectados antes de crear la instancia,
/// evitando estados inválidos.
#[derive(Default)]
pub struct CoreBuilder {
    core_from_data_service: Option<mpsc::Receiver<TableDataVector>>,
    core_from_heartbeat_service: Option<mpsc::Receiver<InternalEvent>>,
    core_from_message_service: Option<mpsc::Receiver<MessageServiceResponse>>,
    core_from_network_service: Option<mpsc::Receiver<NetworkServiceResponse>>,
}

impl CoreBuilder {
    // --- Métodos Setter ---
    // Cada método consume self y devuelve self para permitir encadenamiento.

    pub fn core_from_data_service(mut self, ch: mpsc::Receiver<TableDataVector>) -> Self {
        self.core_from_data_service = Some(ch);
        self
    }

    pub fn core_from_heartbeat_service(mut self, ch: mpsc::Receiver<InternalEvent>) -> Self {
        self.core_from_heartbeat_service = Some(ch);
        self
    }

    pub fn core_from_message_service(mut self, ch: mpsc::Receiver<MessageServiceResponse>) -> Self {
        self.core_from_message_service = Some(ch);
        self
    }

    pub fn core_from_network_service(mut self, ch: mpsc::Receiver<NetworkServiceResponse>) -> Self {
        self.core_from_network_service = Some(ch);
        self
    }

    /// Construye la instancia de `Core`.
    ///
    /// # Errores
    /// Retorna un `Err(String)` si falta configurar alguno de los canales.
    pub fn build(
        self,
        firmware: FirmwareHandle,
        fsm: FsmHandle,
        message: MessageHandle,
        heartbeat: HeartbeatHandle,
        network: NetworkHandle,
        data: DataHandle,
    ) -> Result<Core, String> {
        Ok(Core {
            core_to_firmware_service: firmware,
            core_to_fsm_service: fsm,
            core_to_message_service: message,
            core_to_data_service: data,
            core_to_heartbeat_service: heartbeat,
            core_to_network_service: network,
            core_from_data_service: self
                .core_from_data_service
                .ok_or("Falta: core_from_data_service")?,
            core_from_heartbeat_service: self
                .core_from_heartbeat_service
                .ok_or("Falta: core_from_heartbeat_service")?,
            core_from_message_service: self
                .core_from_message_service
                .ok_or("Falta: core_from_message_service")?,
            core_from_network_service: self
                .core_from_network_service
                .ok_or("Falta: core_from_network_service")?,
        })
    }
}

impl Core {
    /// Crea un nuevo `CoreBuilder` inicializado con valores por defecto (None).
    pub fn builder() -> CoreBuilder {
        CoreBuilder::default()
    }

    pub async fn run(mut self, shutdown: CancellationToken) {
        let mut networks = NetworkServiceResponse::Empty;
        self.core_to_network_service.load_in_memory().await;
        //tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        if self
            .core_to_message_service
            .send(MessageServiceCommand::GenerateHelloWorld)
            .await
            .is_err()
        {
            error!("no se pudo enviar comando GenerateHelloWorld desde Core");
        }

        let mut server_status = InternalEvent::ServerDisconnected;

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("Info: shutdown recibido Core");
                    break;
                }

                Some(response) = self.core_from_data_service.recv() => {
                    self.core_to_message_service.serialize_batch(response);
                }
                Some(response) = self.core_from_heartbeat_service.recv() => {
                    server_status = response.clone();
                    self.core_to_data_service.send_status_connection_server(response).await;
                }
                Some(response) = self.core_from_message_service.recv() => {
                    match response {
                        MessageServiceResponse::FromHub(from_hub) => {
                            match from_hub {
                                HubMessage::Report(_) => {
                                    self.core_to_data_service.save_data_from_hub(from_hub).await;
                                },
                                HubMessage::Monitor(_) => {
                                    self.core_to_data_service.save_data_from_hub(from_hub).await;
                                },
                                HubMessage::AlertAir(_) => {
                                    self.core_to_data_service.save_data_from_hub(from_hub).await;
                                },
                                HubMessage::AlertTem(_) => {
                                    self.core_to_data_service.save_data_from_hub(from_hub).await;
                                },
                                HubMessage::HandshakeFromHub(hand) => {
                                    self.core_to_fsm_service.handshake(hand).await;
                                },
                                HubMessage::FirmwareOk(firmware) => {
                                    self.core_to_firmware_service.ack_from_hub(firmware).await;
                                },
                                HubMessage::EmptyQueue(empty) => {
                                    self.core_to_fsm_service.queue(empty).await;
                                },
                                HubMessage::EmptyQueueSafe(empty) => {
                                    self.core_to_fsm_service.queue_safe(empty).await;
                                },
                                HubMessage::LinkageRequest(_) => {
                                    self.core_to_network_service.message_from_hub(from_hub).await;
                                },
                                _ => {}
                            }
                        },
                        MessageServiceResponse::FromServer(from_server) => {
                            match from_server {
                                ServerMessage::UpdateFirmware(update) => {
                                    self.core_to_firmware_service.update_hubs(update).await;
                                },
                                ServerMessage::Network(_) => {
                                    self.core_to_network_service.message_from_server(from_server).await;
                                },
                                ServerMessage::Heartbeat(beat) => {
                                    self.core_to_heartbeat_service.send_heartbeat(beat).await;
                                },
                                ServerMessage::UpdateEdgeFirmware(update) => {
                                    self.core_to_firmware_service.update_edge(update).await;
                                }
                                _ => {}
                            }
                        },
                    }
                }
                Some(response) = self.core_from_network_service.recv() => {
                    if after_was_empty_now_is_empty(&networks, &response) {
                        networks = response;
                    }

                }
            }
        }
    }
}
