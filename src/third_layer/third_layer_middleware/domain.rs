use tokio::sync::mpsc;
use tracing::info;

use crate::{system::domain::InternalEvent, third_layer::network::logic::NetworkServiceResponse};

pub struct ChannelsThirdLayerMiddleware {
    pub middleware_from_network_service: mpsc::Receiver<NetworkServiceResponse>,
    pub network_service_to_middleware: mpsc::Sender<NetworkServiceResponse>,

    pub middleware_from_heartbeat_service: mpsc::Receiver<InternalEvent>,
    pub heartbeat_service_to_middleware: mpsc::Sender<InternalEvent>,
}

impl ChannelsThirdLayerMiddleware {
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
