//! # Servicio Supervisor de MQTT
//!
//! Este módulo actúa como la capa de abstracción y orquestación (Supervisor) para el cliente MQTT.
//! Se encarga de aislar la lógica pura de protocolo (implementada en `crate::mqtt::logic::mqtt`)
//! del resto del sistema, enrutando comandos desde el `Core` y gestionando el ciclo de vida de la tarea.
//!
//! ## Arquitectura
//! Implementa el patrón Actor/Enrutador. El servicio recibe un flujo unificado de comandos
//! (`MqttServiceCommand`) desde el `Core`, y los demultiplexa internamente en dos canales separados:
//! 1. Mensajes de red (actualizaciones de topología).
//! 2. Cargas útiles (payloads) que deben ser publicadas en el broker.

use crate::context::domain::AppContext;
use crate::first_layer::mqtt::logic::mqtt;
use crate::second_layer::message::domain::SerializedMessage;
use crate::system::domain::InternalEvent;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

/// Manejador (`Handle`) ligero para comunicarse con el `MqttService`.
///
/// Permite inyectar comandos e interactuar asíncronamente con el bucle
/// del cliente MQTT en ejecución, sin necesidad de acceder a los canales 
/// de forma directa.
#[derive(Clone)]
pub struct MqttHandle {
    /// Canal de envío hacia el orquestador MQTT.
    tx: mpsc::Sender<InternalMqttCommand>,
}

impl MqttHandle {
    /// Envía un mensaje serializado al servicio MQTT para que sea publicado.
    ///
    /// # Argumentos
    /// * `data` - El mensaje listo con su tópico y payload (`SerializedMessage`).
    pub async fn send_serialized(&self, data: SerializedMessage) {
        let _ = self.tx.send(InternalMqttCommand::Serialized(data)).await;
    }
    
    /// Notifica al servicio MQTT que la red inicial está lista y cargada en memoria.
    pub async fn networks_ready(&self) {
        let _ = self.tx.send(InternalMqttCommand::NetworksReady).await;
    }
    
    /// Notifica al servicio MQTT que hubo un cambio en la configuración de la red 
    /// (e.g. nuevos dispositivos) y debe actualizar sus suscripciones.
    pub async fn update_networks(&self) {
        let _ = self.tx.send(InternalMqttCommand::NetworksUpdated).await;
    }
}

/// Enumeración interna que representa los comandos que recibe el `MqttService`
/// desde el manejador `MqttHandle`.
enum InternalMqttCommand {
    /// Un mensaje de datos ya procesado y serializado, listo para ser publicado.
    Serialized(SerializedMessage),
    /// Señal que indica que el gestor de red ha terminado de cargar la topología inicial en memoria.
    NetworksReady,
    /// Señal que indica que la topología de la red ha cambiado (nuevos Hubs, bajas, etc.)
    /// y las suscripciones deben ser reevaluadas.
    NetworksUpdated,
}

/// Igual que InternalMqttCommand pero para uso del modulo. Se busca simplicidad en el diseño de logic.rs.
/// El exterior debe usar MqttHandle.
pub enum MqttServiceCommand {
    /// Indica que las redes se han cargado por primera vez en memoria.
    NetworksReady,
    /// Indica que hubo una actualización en las redes y se deben recalcular las suscripciones.
    NetworksUpdated,
}

/// Actor supervisor del cliente MQTT.
///
/// Mantiene los canales de comunicación hacia y desde el `Core` y posee el contexto
/// global de la aplicación. Su función principal es iniciar la tarea en segundo plano
/// del cliente MQTT y traducir/enrutar los comandos bidireccionales.
pub struct MqttService {
    /// Canal para enviar eventos generados por el cliente MQTT (conexiones, mensajes entrantes) al Core.
    sender: mpsc::Sender<InternalEvent>,
    /// Canal por donde se reciben comandos (`MqttServiceCommand`) provenientes del Core.
    rx: mpsc::Receiver<InternalMqttCommand>,
    /// Contexto global compartido de la aplicación.
    context: AppContext,
}

impl MqttService {
    /// Crea una nueva instancia del supervisor MQTT.
    ///
    /// # Argumentos
    /// * `sender` - Extremo de transmisión hacia el bus central del Core.
    /// * `receiver` - Extremo de recepción para escuchar comandos del Core.
    /// * `context` - Estado global (configuraciones, base de datos, gestor de red).
    pub fn new(sender: mpsc::Sender<InternalEvent>, context: AppContext) -> (Self, MqttHandle) {
        let (tx, rx) = mpsc::channel(10);
        let service = Self {
            sender,
            rx,
            context,
        };
        let handle = MqttHandle { tx };
        (service, handle)
    }

    /// Inicia el bucle principal de eventos del supervisor.
    ///
    /// Este método consume la instancia, crea los canales internos necesarios,
    /// levanta la tarea asíncrona real del cliente MQTT (`logic::mqtt`), y se queda
    /// en un bucle infinito enrutando mensajes hasta que se reciba la señal de cancelación.
    ///
    /// # Argumentos
    /// * `shutdown` - Token utilizado para detener de forma segura el bucle y la tarea hija.
    pub async fn run(mut self, shutdown: CancellationToken) {
        let (tx, mut rx_response) = mpsc::channel::<InternalEvent>(50);
        let (tx_command, rx_msg) = mpsc::channel::<SerializedMessage>(50);
        let (tx_command_net, rx_net) = mpsc::channel::<MqttServiceCommand>(50);

        tokio::spawn(mqtt(
            tx,
            rx_msg,
            rx_net,
            self.context.clone(),
            shutdown.clone(),
        ));

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("shutdown recibido MqttService");
                    break;
                }
                Some(cmd) = self.rx.recv() => {
                    match cmd {
                        InternalMqttCommand::Serialized(msg) => {
                            if tx_command.send(msg).await.is_err() {
                                error!("no se pudo enviar SerializedMessage desde MqttService");
                            }
                        },
                        InternalMqttCommand::NetworksReady => {
                            if tx_command_net.send(MqttServiceCommand::NetworksReady).await.is_err() {
                                error!("no se pudo enviar NetworksReady desde MqttService");
                            }
                        },
                        InternalMqttCommand::NetworksUpdated => {
                            if tx_command_net.send(MqttServiceCommand::NetworksUpdated).await.is_err() {
                                error!("no se pudo enviar NetworksUpdated desde MqttService");
                            }
                        }
                    }
                }
                Some(msg) = rx_response.recv() => {
                    if self.sender.send(msg).await.is_err() {
                        error!("no se pudo enviar InternalEvent desde MqttService");
                    }
                }
            }
        }
    }
}

/// Estructura de Transporte de Datos MQTT (DTO).
///
/// Agrupa una carga útil binaria bruta junto con su tópico de destino o procedencia.
/// Se utiliza para desacoplar el contenido del mensaje de los tipos específicos de la librería MQTT subyacente.
#[derive(Debug, Clone, PartialEq)]
pub struct PayloadTopic {
    /// El contenido crudo del mensaje en bytes.
    pub payload: Vec<u8>,
    /// La ruta completa del tópico MQTT (ej. `devices/sensor_1/data`).
    pub topic: String,
}

impl PayloadTopic {
    /// Construye una nueva instancia de `PayloadTopic`.
    pub fn new(payload: Vec<u8>, topic: String) -> Self {
        Self { payload, topic }
    }
}
