use crate::second_layer::database::domain::{DataHandle, TableDataVector};
use crate::second_layer::message::domain::{HubMessage, ServerMessage};
use crate::second_layer::message::logic::{MessageHandle, MessageServiceResponse};
use crate::system::domain::InternalEvent;
use crate::third_layer::firmware::domain::FirmwareHandle;
use crate::third_layer::fsm::logic::FsmHandle;
use crate::third_layer::heartbeat::domain::HeartbeatHandle;
use crate::third_layer::network::logic::NetworkHandle;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::info;

pub struct SecondLayerMiddleware {
    middleware_from_message_service: mpsc::Receiver<MessageServiceResponse>,
    middleware_from_data_service: mpsc::Receiver<TableDataVector>,
    middleware_to_message_service: MessageHandle,
    middleware_to_fsm_service: FsmHandle,
    middleware_to_network_service: NetworkHandle,
    middleware_to_firmware_service: FirmwareHandle,
    middleware_to_heartbeat_service: HeartbeatHandle,
    middleware_to_data_service: DataHandle,
}

#[derive(Default)]
pub struct SecondLayerMiddlewareBuilder {
    middleware_from_message_service: Option<mpsc::Receiver<MessageServiceResponse>>,
    middleware_from_data_service: Option<mpsc::Receiver<TableDataVector>>,
}

impl SecondLayerMiddlewareBuilder {
    pub fn middleware_from_message_service(
        mut self,
        ch: mpsc::Receiver<MessageServiceResponse>,
    ) -> Self {
        self.middleware_from_message_service = Some(ch);
        self
    }

    pub fn middleware_from_data_service(mut self, ch: mpsc::Receiver<TableDataVector>) -> Self {
        self.middleware_from_data_service = Some(ch);
        self
    }

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
    pub fn builder() -> SecondLayerMiddlewareBuilder {
        SecondLayerMiddlewareBuilder::default()
    }

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
