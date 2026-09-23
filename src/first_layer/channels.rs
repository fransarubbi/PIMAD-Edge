use crate::system::domain::InternalEvent;
use tokio::sync::mpsc;
use tracing::info;

pub struct ChannelsFirstLayer {
    pub to_message_service: mpsc::Sender<InternalEvent>,
    pub message_service_from_services: mpsc::Receiver<InternalEvent>,
}

impl ChannelsFirstLayer {
    pub fn new(buffer_size: usize) -> Self {
        info!("creando canales del middleware de capa 2");

        let (to_message_service, message_service_from_services) = mpsc::channel(buffer_size);

        Self {
            to_message_service,
            message_service_from_services,
        }
    }
}
