use crate::second_layer::{
    database::domain::TableDataVector, message::logic::MessageServiceResponse,
};
use tokio::sync::mpsc;
use tracing::info;

pub struct ChannelsSecondLayerMiddleware {
    pub message_service_to_middleware: mpsc::Sender<MessageServiceResponse>,
    pub middleware_from_message_service: mpsc::Receiver<MessageServiceResponse>,

    pub data_service_to_middleware: mpsc::Sender<TableDataVector>,
    pub middleware_from_data_service: mpsc::Receiver<TableDataVector>,
}

impl ChannelsSecondLayerMiddleware {
    pub fn new(buffer_size: usize) -> Self {
        info!("creando canales del middleware de capa 2");

        let (message_service_to_middleware, middleware_from_message_service) =
            mpsc::channel(buffer_size);
        let (data_service_to_middleware, middleware_from_data_service) = mpsc::channel(buffer_size);

        Self {
            message_service_to_middleware,
            middleware_from_message_service,
            data_service_to_middleware,
            middleware_from_data_service,
        }
    }
}
