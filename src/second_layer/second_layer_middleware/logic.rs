//! # Lógica del Middleware de la Segunda Capa
//!
//! Contiene la implementación del actor que rutea eventos entre los diferentes servicios
//! del sistema. Este middleware toma decisiones basadas en el tipo de mensaje que llega,
//! dirigiéndolo al servicio apropiado (FSM, Firmware, Network, etc.).

use crate::second_layer::{
    database::domain::{DataHandle, TableDataVector},
    message::{
        domain::{HubMessage, ServerMessage},
        logic::{MessageHandle, MessageServiceResponse},
    },
};
use crate::system::domain::InternalEvent;
use crate::third_layer::{
    firmware::domain::FirmwareHandle, fsm::logic::FsmHandle, heartbeat::domain::HeartbeatHandle,
    network::logic::NetworkHandle,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::info;

/// Servicio orquestador central de la segunda capa.
///
/// Evalúa el origen y tipo de cada evento o mensaje, y lo redirige
/// utilizando el manejador (Handle) del servicio destino.
pub struct SecondLayerMiddleware {
    /// Canal para recibir mensajes procesados desde `MessageService`.
    middleware_from_message_service: mpsc::Receiver<MessageServiceResponse>,
    /// Canal para recibir lotes de datos desde el servicio de base de datos.
    middleware_from_data_service: mpsc::Receiver<TableDataVector>,
    /// Manejador para enviar comandos hacia `MessageService`.
    middleware_to_message_service: MessageHandle,
    /// Manejador para enviar eventos hacia la máquina de estados (FSM).
    middleware_to_fsm_service: FsmHandle,
    /// Manejador para enviar eventos al gestor de red.
    middleware_to_network_service: NetworkHandle,
    /// Manejador para interactuar con el servicio de actualizaciones de firmware.
    middleware_to_firmware_service: FirmwareHandle,
    /// Manejador para enviar eventos a la FSM de telemetría (Heartbeat).
    middleware_to_heartbeat_service: HeartbeatHandle,
    /// Manejador para enviar comandos a la base de datos.
    middleware_to_data_service: DataHandle,
}

/// Constructor con el patrón Builder para inicializar de forma segura
/// el `SecondLayerMiddleware`.
#[derive(Default)]
pub struct SecondLayerMiddlewareBuilder {
    middleware_from_message_service: Option<mpsc::Receiver<MessageServiceResponse>>,
    middleware_from_data_service: Option<mpsc::Receiver<TableDataVector>>,
}

impl SecondLayerMiddlewareBuilder {
    /// Inyecta el canal de recepción desde `MessageService`.
    pub fn middleware_from_message_service(
        mut self,
        ch: mpsc::Receiver<MessageServiceResponse>,
    ) -> Self {
        self.middleware_from_message_service = Some(ch);
        self
    }

    /// Inyecta el canal de recepción desde la base de datos.
    pub fn middleware_from_data_service(mut self, ch: mpsc::Receiver<TableDataVector>) -> Self {
        self.middleware_from_data_service = Some(ch);
        self
    }

    /// Construye y valida la instancia de `SecondLayerMiddleware`.
    ///
    /// Asegura que todos los canales receptores fueron provistos.
    /// Consume el builder y los `Handle`s de la tercera capa y servicios vecinos.
    pub fn build(
        self,
        message: MessageHandle,
        fsm: FsmHandle,
        network: NetworkHandle,
        firmware: FirmwareHandle,
        heartbeat: HeartbeatHandle,
        data: DataHandle,
    ) -> Result<SecondLayerMiddleware, String> {
        Ok(SecondLayerMiddleware {
            middleware_to_message_service: message,
            middleware_to_fsm_service: fsm,
            middleware_to_network_service: network,
            middleware_to_firmware_service: firmware,
            middleware_to_heartbeat_service: heartbeat,
            middleware_to_data_service: data,
            middleware_from_message_service: self
                .middleware_from_message_service
                .ok_or("falta middleware_from_message_service")?,
            middleware_from_data_service: self
                .middleware_from_data_service
                .ok_or("falta middleware_from_data_service")?,
        })
    }
}

impl SecondLayerMiddleware {
    /// Crea un nuevo constructor (`Builder`) para ensamblar el servicio.
    pub fn builder() -> SecondLayerMiddlewareBuilder {
        SecondLayerMiddlewareBuilder::default()
    }

    /// Lanza el bucle de eventos infinito del middleware.
    ///
    /// Utiliza `tokio::select!` para atender simultáneamente:
    /// - Señales de apagado del sistema.
    /// - Mensajes entrantes del servidor o de la red local (vía `MessageService`).
    /// - Lotes de datos para ser exportados al exterior (vía base de datos).
    pub async fn run(mut self, shutdown: CancellationToken) {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("Info: shutdown recibido Core");
                    break;
                }

                Some(response) = self.middleware_from_message_service.recv() => {
                    match response {
                        MessageServiceResponse::FromHub(from_hub) => {
                            match from_hub {
                                HubMessage::Report(_) => {
                                    self.middleware_to_data_service.save_data_from_hub(from_hub).await;
                                },
                                HubMessage::Monitor(_) => {
                                    self.middleware_to_data_service.save_data_from_hub(from_hub).await;
                                },
                                HubMessage::AlertAir(_) => {
                                    self.middleware_to_data_service.save_data_from_hub(from_hub).await;
                                },
                                HubMessage::AlertTem(_) => {
                                    self.middleware_to_data_service.save_data_from_hub(from_hub).await;
                                },
                                HubMessage::HandshakeFromHub(hand) => {
                                    self.middleware_to_fsm_service.handshake(hand).await;
                                },
                                HubMessage::FirmwareOk(firmware) => {
                                    self.middleware_to_firmware_service.ack_from_hub(firmware).await;
                                },
                                HubMessage::EmptyQueue(empty) => {
                                    self.middleware_to_fsm_service.queue(empty).await;
                                },
                                HubMessage::EmptyQueueSafe(empty) => {
                                    self.middleware_to_fsm_service.queue_safe(empty).await;
                                },
                                HubMessage::LinkageRequest(linkage) => {
                                    self.middleware_to_network_service.linkage_request(linkage).await;
                                },
                                _ => {}
                            }
                        },
                        MessageServiceResponse::FromServer(from_server) => {
                            match from_server {
                                ServerMessage::UpdateFirmware(update) => {
                                    self.middleware_to_firmware_service.update_hubs(update).await;
                                },
                                ServerMessage::Network(network) => {
                                    self.middleware_to_network_service.network(network).await;
                                },
                                ServerMessage::FromServerSettings(settings) => {
                                    self.middleware_to_network_service.settings_from_server(settings).await;
                                }
                                ServerMessage::Heartbeat(beat) => {
                                    self.middleware_to_heartbeat_service.send_heartbeat(beat).await;
                                },
                                ServerMessage::UpdateEdgeFirmware(update) => {
                                    self.middleware_to_firmware_service.update_edge(update).await;
                                }
                                _ => {}
                            }
                        },
                        MessageServiceResponse::LocalConnected => {
                            self.middleware_to_fsm_service.connection_event(InternalEvent::LocalConnected).await;
                        }
                        MessageServiceResponse::LocalDisconnected => {
                            self.middleware_to_fsm_service.connection_event(InternalEvent::LocalDisconnected).await;
                        }
                    }
                }
                Some(response) = self.middleware_from_data_service.recv() => {
                    self.middleware_to_message_service.serialize_batch(response).await;
                }
            }
        }
    }
}
