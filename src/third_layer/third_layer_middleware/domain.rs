//! # Dominio del Middleware de la Tercera Capa
//!
//! Define las abstracciones de comunicación exclusivas para interconectar los
//! distintos componentes de la lógica de negocio (Capa 3).

use tokio::sync::mpsc;
use tracing::info;

use crate::{system::domain::InternalEvent, third_layer::network::logic::NetworkServiceResponse};

/// Contenedor de canales de transmisión y recepción que interconectan
/// el Middleware de la capa 3 con sus servicios dependientes (Heartbeat, Network).
pub struct ChannelsThirdLayerMiddleware {
    /// Extremo receptor donde el middleware escucha las respuestas o eventos del `NetworkService`.
    pub middleware_from_network_service: mpsc::Receiver<NetworkServiceResponse>,
    /// Canal de envío utilizado por el `NetworkService` para notificar al middleware.
    pub network_service_to_middleware: mpsc::Sender<NetworkServiceResponse>,

    /// Extremo receptor donde el middleware escucha las señales del `HeartbeatService`.
    pub middleware_from_heartbeat_service: mpsc::Receiver<InternalEvent>,
    /// Canal de envío utilizado por el `HeartbeatService` para notificar al middleware.
    pub heartbeat_service_to_middleware: mpsc::Sender<InternalEvent>,
}

impl ChannelsThirdLayerMiddleware {
    /// Inicializa y crea los canales asíncronos MPSC para la comunicación de la capa 3.
    ///
    /// # Argumentos
    /// * `buffer_size` - Capacidad máxima (en cantidad de mensajes) de los canales.
    pub fn new(buffer_size: usize) -> Self {
        info!("creando canales del middleware de capa 3");

        let (network_service_to_middleware, middleware_from_network_service) =
            mpsc::channel(buffer_size);
        let (heartbeat_service_to_middleware, middleware_from_heartbeat_service) =
            mpsc::channel(buffer_size);

        Self {
            heartbeat_service_to_middleware,
            middleware_from_heartbeat_service,
            network_service_to_middleware,
            middleware_from_network_service,
        }
    }
}
