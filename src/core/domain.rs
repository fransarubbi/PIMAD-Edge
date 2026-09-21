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

use crate::database::domain::{DataServiceCommand, DataServiceResponse};
use crate::firmware::domain::{FirmwareServiceCommand, FirmwareServiceResponse};
use crate::fsm::domain::{FsmServiceCommand, FsmServiceResponse};
use crate::grpc::FromEdge;
use crate::message::logic::{
    HubMessage, MessageServiceCommand, MessageServiceResponse, ServerMessage,
};
use crate::mqtt::domain::MqttServiceCommand;
use crate::network::domain::{Batch, NetworkServiceCommand, NetworkServiceResponse};
use crate::system::domain::InternalEvent;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

/// Estructura principal que mantiene los canales de comunicación.
///
/// No contiene lógica de negocio compleja, solo la topología de la red de canales
/// y las reglas de enrutamiento en su método `run`.
pub struct Core {
    core_from_data_service: mpsc::Receiver<DataServiceResponse>,
    core_to_data_service: mpsc::Sender<DataServiceCommand>,
    core_from_firmware_service: mpsc::Receiver<FirmwareServiceResponse>,
    core_to_firmware_service: mpsc::Sender<FirmwareServiceCommand>,
    core_from_fsm_service: mpsc::Receiver<FsmServiceResponse>,
    core_to_fsm_service: mpsc::Sender<FsmServiceCommand>,
    core_from_grpc_service: mpsc::Receiver<InternalEvent>,
    core_to_grpc_service: mpsc::Sender<FromEdge>,
    core_from_heartbeat_service: mpsc::Receiver<InternalEvent>,
    core_to_heartbeat_service: mpsc::Sender<ServerMessage>,
    core_from_message_service: mpsc::Receiver<MessageServiceResponse>,
    core_to_message_service: mpsc::Sender<MessageServiceCommand>,
    core_from_metrics_service: mpsc::Receiver<ServerMessage>,
    core_from_mqtt_service: mpsc::Receiver<InternalEvent>,
    core_to_mqtt_service: mpsc::Sender<MqttServiceCommand>,
    core_from_network_service: mpsc::Receiver<NetworkServiceResponse>,
    core_to_network_service: mpsc::Sender<NetworkServiceCommand>,
}

/// Builder para construir la estructura `Core`.
///
/// Dado que `Core` tiene múltiples dependencias estrictas (canales), este builder
/// asegura que todos los canales sean inyectados antes de crear la instancia,
/// evitando estados inválidos.
#[derive(Default)]
pub struct CoreBuilder {
    core_from_data_service: Option<mpsc::Receiver<DataServiceResponse>>,
    core_to_data_service: Option<mpsc::Sender<DataServiceCommand>>,
    core_from_firmware_service: Option<mpsc::Receiver<FirmwareServiceResponse>>,
    core_to_firmware_service: Option<mpsc::Sender<FirmwareServiceCommand>>,
    core_from_fsm_service: Option<mpsc::Receiver<FsmServiceResponse>>,
    core_to_fsm_service: Option<mpsc::Sender<FsmServiceCommand>>,
    core_from_grpc_service: Option<mpsc::Receiver<InternalEvent>>,
    core_to_grpc_service: Option<mpsc::Sender<FromEdge>>,
    core_from_heartbeat_service: Option<mpsc::Receiver<InternalEvent>>,
    core_to_heartbeat_service: Option<mpsc::Sender<ServerMessage>>,
    core_from_message_service: Option<mpsc::Receiver<MessageServiceResponse>>,
    core_to_message_service: Option<mpsc::Sender<MessageServiceCommand>>,
    core_from_metrics_service: Option<mpsc::Receiver<ServerMessage>>,
    core_from_mqtt_service: Option<mpsc::Receiver<InternalEvent>>,
    core_to_mqtt_service: Option<mpsc::Sender<MqttServiceCommand>>,
    core_from_network_service: Option<mpsc::Receiver<NetworkServiceResponse>>,
    core_to_network_service: Option<mpsc::Sender<NetworkServiceCommand>>,
}

impl CoreBuilder {
    // --- Métodos Setter ---
    // Cada método consume self y devuelve self para permitir encadenamiento.

    pub fn core_from_data_service(mut self, ch: mpsc::Receiver<DataServiceResponse>) -> Self {
        self.core_from_data_service = Some(ch);
        self
    }
    pub fn core_to_data_service(mut self, ch: mpsc::Sender<DataServiceCommand>) -> Self {
        self.core_to_data_service = Some(ch);
        self
    }

    pub fn core_from_firmware_service(
        mut self,
        ch: mpsc::Receiver<FirmwareServiceResponse>,
    ) -> Self {
        self.core_from_firmware_service = Some(ch);
        self
    }
    pub fn core_to_firmware_service(mut self, ch: mpsc::Sender<FirmwareServiceCommand>) -> Self {
        self.core_to_firmware_service = Some(ch);
        self
    }

    pub fn core_from_fsm_service(mut self, ch: mpsc::Receiver<FsmServiceResponse>) -> Self {
        self.core_from_fsm_service = Some(ch);
        self
    }
    pub fn core_to_fsm_service(mut self, ch: mpsc::Sender<FsmServiceCommand>) -> Self {
        self.core_to_fsm_service = Some(ch);
        self
    }

    pub fn core_from_grpc_service(mut self, ch: mpsc::Receiver<InternalEvent>) -> Self {
        self.core_from_grpc_service = Some(ch);
        self
    }
    pub fn core_to_grpc_service(mut self, ch: mpsc::Sender<FromEdge>) -> Self {
        self.core_to_grpc_service = Some(ch);
        self
    }

    pub fn core_from_heartbeat_service(mut self, ch: mpsc::Receiver<InternalEvent>) -> Self {
        self.core_from_heartbeat_service = Some(ch);
        self
    }
    pub fn core_to_heartbeat_service(mut self, ch: mpsc::Sender<ServerMessage>) -> Self {
        self.core_to_heartbeat_service = Some(ch);
        self
    }

    pub fn core_from_message_service(mut self, ch: mpsc::Receiver<MessageServiceResponse>) -> Self {
        self.core_from_message_service = Some(ch);
        self
    }
    pub fn core_to_message_service(mut self, ch: mpsc::Sender<MessageServiceCommand>) -> Self {
        self.core_to_message_service = Some(ch);
        self
    }

    pub fn core_from_metrics_service(mut self, ch: mpsc::Receiver<ServerMessage>) -> Self {
        self.core_from_metrics_service = Some(ch);
        self
    }

    pub fn core_from_mqtt_service(mut self, ch: mpsc::Receiver<InternalEvent>) -> Self {
        self.core_from_mqtt_service = Some(ch);
        self
    }
    pub fn core_to_mqtt_service(mut self, ch: mpsc::Sender<MqttServiceCommand>) -> Self {
        self.core_to_mqtt_service = Some(ch);
        self
    }

    pub fn core_from_network_service(mut self, ch: mpsc::Receiver<NetworkServiceResponse>) -> Self {
        self.core_from_network_service = Some(ch);
        self
    }
    pub fn core_to_network_service(mut self, ch: mpsc::Sender<NetworkServiceCommand>) -> Self {
        self.core_to_network_service = Some(ch);
        self
    }

    /// Construye la instancia de `Core`.
    ///
    /// # Errores
    /// Retorna un `Err(String)` si falta configurar alguno de los canales.
    pub fn build(self) -> Result<Core, String> {
        Ok(Core {
            core_from_data_service: self
                .core_from_data_service
                .ok_or("Falta: core_from_data_service")?,
            core_to_data_service: self
                .core_to_data_service
                .ok_or("Falta: core_to_data_service")?,
            core_from_firmware_service: self
                .core_from_firmware_service
                .ok_or("Falta: core_from_firmware_service")?,
            core_to_firmware_service: self
                .core_to_firmware_service
                .ok_or("Falta: core_to_firmware_service")?,
            core_from_fsm_service: self
                .core_from_fsm_service
                .ok_or("Falta: core_from_fsm_service")?,
            core_to_fsm_service: self
                .core_to_fsm_service
                .ok_or("Falta: core_to_fsm_service")?,
            core_from_grpc_service: self
                .core_from_grpc_service
                .ok_or("Falta: core_from_grpc_service")?,
            core_to_grpc_service: self
                .core_to_grpc_service
                .ok_or("Falta: core_to_grpc_service")?,
            core_from_heartbeat_service: self
                .core_from_heartbeat_service
                .ok_or("Falta: core_from_heartbeat_service")?,
            core_to_heartbeat_service: self
                .core_to_heartbeat_service
                .ok_or("Falta: core_to_heartbeat_service")?,
            core_from_message_service: self
                .core_from_message_service
                .ok_or("Falta: core_from_message_service")?,
            core_to_message_service: self
                .core_to_message_service
                .ok_or("Falta: core_to_message_service")?,
            core_from_metrics_service: self
                .core_from_metrics_service
                .ok_or("Falta: core_from_metrics_service")?,
            core_from_mqtt_service: self
                .core_from_mqtt_service
                .ok_or("Falta: core_from_mqtt_service")?,
            core_to_mqtt_service: self
                .core_to_mqtt_service
                .ok_or("Falta: core_to_mqtt_service")?,
            core_from_network_service: self
                .core_from_network_service
                .ok_or("Falta: core_from_network_service")?,
            core_to_network_service: self
                .core_to_network_service
                .ok_or("Falta: core_to_network_service")?,
        })
    }
}

impl Core {
    /// Crea un nuevo `CoreBuilder` inicializado con valores por defecto (None).
    pub fn builder() -> CoreBuilder {
        CoreBuilder::default()
    }

    /// Inicia el bucle principal de enrutamiento de mensajes.
    ///
    /// Este método consume la instancia de `Core` y ejecuta un bucle infinito
    /// utilizando `tokio::select!` para procesar mensajes de forma concurrente
    /// desde todos los servicios conectados.
    ///
    /// # Lógica de Enrutamiento
    ///
    /// | Origen | Mensaje | Destino | Propósito |
    /// |--------|---------|---------|-----------|
    /// | **DataService** | `Batch` | MessageService | Enviar datos históricos al servidor |
    /// | **DataService** | `Epoch` | FsmService | Sincronizar época de balanceo |
    /// | **FirmwareService** | `Ack/HubCommand` | MessageService | Comunicar resultado al servidor o enviar orden a los Hubs |
    /// | **FsmService** | `ToServer/ToHub` | MessageService | Notificaciones de estado de la FSM |
    /// | **FsmService** | `NewEpoch` | DataService | Persistir nueva época en la base de datos |
    /// | **GrpcService** | `Internal` | MessageService | Mensaje entrante gRPC |
    /// | **HeartbeatService** | `Internal` | MessageService/DataService | Latidos emitidos por el servidor |
    /// | **MessageService** | `EdgeUpload` | GrpcService | Enviar mensajes al servidor |
    /// | **MessageService** | `HubMessage` | DataService/FirmwareService/FsmService | Procesar mensajes provenientes de los Hubs |
    /// | **MessageService** | `ServerMessage` | FirmwareService/NetworkService/HeartbeatService/FsmService | Procesar mensajes provenientes del Server |
    /// | **MetricsService** | `ServerMessage` | MessageService | Enviar telemetría al servidor |
    /// | **NetworkService** | `DataCommand` | DataService | CRUD de redes/hubs en base de datos |
    pub async fn run(mut self, shutdown: CancellationToken) {
        if self
            .core_to_data_service
            .send(DataServiceCommand::GetTotalOfNetworks)
            .await
            .is_err()
        {
            error!("no se pudo enviar comando GetTotalOfNetworks desde Core");
        }

        if self
            .core_to_message_service
            .send(MessageServiceCommand::GenerateHelloWorld)
            .await
            .is_err()
        {
            error!("no se pudo enviar comando GenerateHelloWorld desde Core");
        }

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("Info: shutdown recibido Core");
                    break;
                }
                Some(response) = self.core_from_data_service.recv() => {
                    match response {
                        DataServiceResponse::Batch(batch) => {
                            if self.core_to_message_service.send(MessageServiceCommand::Batch(batch)).await.is_err() {
                                error!("no se pudo enviar Batch desde Core");
                            }
                        },
                        DataServiceResponse::Epoch(epoch) => {
                            if self.core_to_fsm_service.send(FsmServiceCommand::Epoch(epoch)).await.is_err() {
                                error!("no se pudo enviar Epoch desde Core");
                            }
                        },
                        DataServiceResponse::ErrorEpoch => {
                            if self.core_to_fsm_service.send(FsmServiceCommand::ErrorEpoch).await.is_err() {
                                error!("no se pudo enviar ErrorEpoch desde Core");
                            }
                        },
                        DataServiceResponse::NoNetworks => {
                            if self.core_to_fsm_service.send(FsmServiceCommand::DeleteRuntime).await.is_err() {
                                error!("no se pudo enviar DeleteRuntime desde Core");
                            }
                            if self.core_to_firmware_service.send(FirmwareServiceCommand::DeleteRuntime).await.is_err() {
                                error!("no se pudo enviar DeleteRuntime desde Core");
                            }
                        },
                        DataServiceResponse::ThereAreNetworks => {
                            if self.core_to_fsm_service.send(FsmServiceCommand::CreateRuntime).await.is_err() {
                                error!("no se pudo enviar CreateRuntime desde Core");
                            }
                            if self.core_to_firmware_service.send(FirmwareServiceCommand::CreateRuntime).await.is_err() {
                                error!("no se pudo enviar CreateRuntime desde Core");
                            }
                        },
                        DataServiceResponse::BatchNetwork(networks) => {
                            if self.core_to_network_service.send(NetworkServiceCommand::Batch(Batch::Network(networks))).await.is_err() {
                                error!("no se pudo enviar BatchNetwork desde Core");
                            }
                        },
                        DataServiceResponse::BatchHub(hubs) => {
                            if self.core_to_network_service.send(NetworkServiceCommand::Batch(Batch::Hub(hubs))).await.is_err() {
                                error!("no se pudo enviar BatchHub desde Core");
                            }
                        },
                        DataServiceResponse::NetworksUpdated(code) => {
                            if self.core_to_mqtt_service.send(MqttServiceCommand::NetworksUpdated).await.is_err() {
                                error!("no se pudo enviar NetworksUpdated desde Core");
                            }
                            if self.core_to_message_service.send(MessageServiceCommand::GenerateNetworkAck(code)).await.is_err() {
                                error!("no se pudo enviar GenerateNetworkAck desde Core");
                            }
                        },
                        DataServiceResponse::HubInserted(hub) => {
                            if self.core_to_message_service.send(MessageServiceCommand::GenerateLinkageAck(hub)).await.is_err() {
                                error!("no se pudo enviar GenerateLinkageAck desde Core");
                            }
                        }
                    }
                }
                Some(response) = self.core_from_firmware_service.recv() => {
                    match response {
                        FirmwareServiceResponse::ServerAck(server_msg) => {
                            if self.core_to_message_service.send(MessageServiceCommand::ToServer(server_msg)).await.is_err() {
                                error!("no se pudo enviar ServerAck desde Core");
                            }
                        },
                        FirmwareServiceResponse::HubCommand(hub_msg) => {
                            if self.core_to_message_service.send(MessageServiceCommand::ToHub(hub_msg)).await.is_err() {
                                error!("no se pudo enviar HubCommand desde Core");
                            }
                        },
                        FirmwareServiceResponse::EdgeUpdated(updated) => {
                            if self.core_to_message_service.send(MessageServiceCommand::ToServer(updated)).await.is_err() {
                                error!("no se pudo enviar UpdateEdgeFirmware desde Core");
                            }
                        }
                    }
                }
                Some(response) = self.core_from_fsm_service.recv() => {
                    match response {
                        FsmServiceResponse::ToHub(to_hub) => {
                            if self.core_to_message_service.send(MessageServiceCommand::ToHub(to_hub)).await.is_err() {
                                error!("no se pudo enviar ToHub desde Core");
                            }
                        },
                        FsmServiceResponse::NewEpoch(new_epoch) => {
                            if self.core_to_data_service.send(DataServiceCommand::NewEpoch(new_epoch)).await.is_err() {
                                error!("no se pudo enviar NewEpoch desde Core");
                            }
                        },
                        FsmServiceResponse::GetEpoch => {
                            if self.core_to_data_service.send(DataServiceCommand::GetEpoch).await.is_err() {
                                error!("no se pudo enviar GetEpoch desde Core");
                            }
                        },
                        FsmServiceResponse::EdgeState(state) => {
                            if self.core_to_message_service.send(MessageServiceCommand::GenerateEdgeState(state)).await.is_err() {
                                error!("no se pudo enviar GenerateEdgeState desde Core");
                            }
                        }
                    }
                }
                Some(response) = self.core_from_grpc_service.recv() => {
                    if self.core_to_message_service.send(MessageServiceCommand::Internal(response)).await.is_err() {
                        error!("no se pudo enviar Internal desde Core");
                    }
                }
                Some(response) = self.core_from_heartbeat_service.recv() => {
                    if self.core_to_message_service.send(MessageServiceCommand::Internal(response.clone())).await.is_err() {
                        error!("no se pudo enviar Internal desde Core");
                    }
                    if self.core_to_data_service.send(DataServiceCommand::Internal(response)).await.is_err() {
                        error!("no se pudo enviar Internal desde Core");
                    }
                }
                Some(response) = self.core_from_message_service.recv() => {
                    match response {
                        MessageServiceResponse::EdgeUpload(upload) => {
                            if self.core_to_grpc_service.send(upload).await.is_err() {
                                error!("no se pudo enviar EdgeUpload desde Core");
                            }
                        },
                        MessageServiceResponse::FromHub(from_hub) => {
                            match from_hub {
                                HubMessage::Report(_) => {
                                    if self.core_to_data_service.send(DataServiceCommand::Hub(from_hub)).await.is_err() {
                                        error!("no se pudo enviar Report desde Core");
                                    }
                                },
                                HubMessage::Monitor(_) => {
                                    if self.core_to_data_service.send(DataServiceCommand::Hub(from_hub)).await.is_err() {
                                        error!("no se pudo enviar Monitor desde Core");
                                    }
                                },
                                HubMessage::AlertAir(_) => {
                                    if self.core_to_data_service.send(DataServiceCommand::Hub(from_hub)).await.is_err() {
                                        error!("no se pudo enviar AlertAir desde Core");
                                    }
                                },
                                HubMessage::AlertTem(_) => {
                                    if self.core_to_data_service.send(DataServiceCommand::Hub(from_hub)).await.is_err() {
                                        error!("no se pudo enviar AlertTem desde Core");
                                    }
                                },
                                HubMessage::FirmwareOk(firmware) => {
                                    if self.core_to_firmware_service.send(FirmwareServiceCommand::HubResponse(firmware)).await.is_err() {
                                        error!("no se pudo enviar FirmwareOk desde Core");
                                    }
                                },
                                HubMessage::HandshakeFromHub(_) => {
                                    if self.core_to_fsm_service.send(FsmServiceCommand::FromHub(from_hub)).await.is_err() {
                                        error!("no se pudo enviar HandshakeFromHub desde Core");
                                    }
                                },
                                HubMessage::EmptyQueue(_) => {
                                    if self.core_to_fsm_service.send(FsmServiceCommand::FromHub(from_hub)).await.is_err() {
                                        error!("no se pudo enviar EmptyQueue desde Core");
                                    }
                                },
                                HubMessage::EmptyQueueSafe(_) => {
                                    if self.core_to_fsm_service.send(FsmServiceCommand::FromHub(from_hub)).await.is_err() {
                                        error!("no se pudo enviar EmptyQueueSafe desde Core");
                                    }
                                },
                                HubMessage::LinkageRequest(_) => {
                                    if self.core_to_network_service.send(NetworkServiceCommand::HubMessage(from_hub)).await.is_err() {
                                        error!("no se pudo enviar LinkageRequest desde Core");
                                    }
                                },
                                _ => {}
                            }
                        },
                        MessageServiceResponse::FromServer(from_server) => {
                            match from_server {
                                ServerMessage::UpdateFirmware(update) => {
                                    if self.core_to_firmware_service.send(FirmwareServiceCommand::UpdateHub(update)).await.is_err() {
                                        error!("no se pudo enviar UpdateFirmware desde Core");
                                    }
                                },
                                ServerMessage::DeleteHub(_) => {
                                    if self.core_to_network_service.send(NetworkServiceCommand::ServerMessage(from_server)).await.is_err() {
                                        error!("no se pudo enviar DeleteHub desde Core");
                                    }
                                },
                                ServerMessage::FromServerSettings(_) => {
                                    if self.core_to_network_service.send(NetworkServiceCommand::ServerMessage(from_server)).await.is_err() {
                                        error!("no se pudo enviar FromServerSettings desde Core");
                                    }
                                },
                                ServerMessage::FromServerSettingsAck(ack) => {
                                    if self.core_to_message_service.send(MessageServiceCommand::ToHub(HubMessage::FromHubSettingsAck(ack))).await.is_err() {
                                        error!("no se pudo enviar FromServerSettingsAck desde Core");
                                    }
                                },
                                ServerMessage::Network(_) => {
                                    if self.core_to_network_service.send(NetworkServiceCommand::ServerMessage(from_server)).await.is_err() {
                                        error!("no se pudo enviar Network desde Core");
                                    }
                                },
                                ServerMessage::Heartbeat(_) => {
                                    if self.core_to_heartbeat_service.send(from_server).await.is_err() {
                                        error!("no se pudo enviar Heartbeat desde Core");
                                    }
                                },
                                ServerMessage::UpdateEdgeFirmware(update) => {
                                    if self.core_to_firmware_service.send(FirmwareServiceCommand::UpdateEdge(update)).await.is_err() {
                                        error!("no se pudo enviar UpdateEdge desde Core");
                                    }
                                }
                                _ => {}
                            }
                        },
                        MessageServiceResponse::Serialized(serialized) => {
                            if self.core_to_mqtt_service.send(MqttServiceCommand::Serialized(serialized)).await.is_err() {
                                error!("no se pudo enviar Serialized desde Core");
                            }
                        }
                    }
                }
                Some(response) = self.core_from_metrics_service.recv() => {
                    if self.core_to_message_service.send(MessageServiceCommand::ToServer(response)).await.is_err() {
                        error!("no se pudo enviar ToServer desde Core");
                    }
                }
                Some(response) = self.core_from_mqtt_service.recv() => {
                    match &response {
                        InternalEvent::LocalDisconnected => {
                            if self.core_to_fsm_service.send(FsmServiceCommand::LocalDisconnected).await.is_err() {
                                error!("no se pudo enviar LocalDisconnect desde Core");
                            }
                        }
                        InternalEvent::LocalConnected => {
                            if self.core_to_fsm_service.send(FsmServiceCommand::LocalConnected).await.is_err() {
                                error!("no se pudo enviar LocalConnect desde Core");
                            }
                        }
                        _ => {}
                    }
                    if self.core_to_message_service.send(MessageServiceCommand::Internal(response)).await.is_err() {
                        error!("no se pudo enviar Internal desde Core");
                    }
                }
                Some(response) = self.core_from_network_service.recv() => {
                    match response {
                        NetworkServiceResponse::HubMessage(hub_msg) => {
                            if self.core_to_message_service.send(MessageServiceCommand::ToHub(hub_msg)).await.is_err() {
                                error!("no se pudo enviar ToHub desde Core");
                            }
                        },
                        NetworkServiceResponse::DataCommand(data_command) => {
                            match data_command {
                                DataServiceCommand::DeleteNetwork(id) => {
                                    if self.core_to_data_service.send(DataServiceCommand::DeleteNetwork(id)).await.is_err() {
                                        error!("no se pudo enviar DeleteNetwork desde Core");
                                    }
                                },
                                DataServiceCommand::DeleteAllHubByNetwork(id) => {
                                    if self.core_to_data_service.send(DataServiceCommand::DeleteAllHubByNetwork(id)).await.is_err() {
                                        error!("no se pudo enviar DeleteAllHubByNetwork desde Core");
                                    }
                                },
                                DataServiceCommand::UpdateNetwork(network) => {
                                    if self.core_to_data_service.send(DataServiceCommand::UpdateNetwork(network)).await.is_err() {
                                        error!("no se pudo enviar UpdateNetwork desde Core");
                                    }
                                },
                                DataServiceCommand::NewNetwork(network) => {
                                    if self.core_to_data_service.send(DataServiceCommand::NewNetwork(network)).await.is_err() {
                                        error!("no se pudo enviar NewNetwork desde Core");
                                    }
                                },
                                DataServiceCommand::NewHub(id) => {
                                    if self.core_to_data_service.send(DataServiceCommand::NewHub(id)).await.is_err() {
                                        error!("no se pudo enviar NewHub desde Core");
                                    }
                                },
                                DataServiceCommand::DeleteHub(id) => {
                                    if self.core_to_data_service.send(DataServiceCommand::DeleteHub(id)).await.is_err() {
                                        error!("no se pudo enviar DeleteHub desde Core");
                                    }
                                },
                                _ => {}
                            }
                        }
                        NetworkServiceResponse::NetworksReady => {
                            if self.core_to_mqtt_service.send(MqttServiceCommand::NetworksReady).await.is_err() {
                                error!("no se pudo enviar NetworksReady desde Core");
                            }
                        },
                        NetworkServiceResponse::GenerateLinkageAck(id) => {
                            if self.core_to_message_service.send(MessageServiceCommand::GenerateLinkageAck(id)).await.is_err() {
                                error!("no se pudo enviar NetworksReady desde Core");
                            }
                        }
                    }
                }
            }
        }
    }
}
