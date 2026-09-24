//! # Dominio del Middleware de la Segunda Capa
//!
//! Define las abstracciones de comunicación (canales MPSC) exclusivas
//! para la coordinación interna de la segunda capa.

use crate::second_layer::{
    database::domain::TableDataVector, message::logic::MessageServiceResponse,
};
use tokio::sync::mpsc;
use tracing::info;

/// Contenedor de canales de transmisión y recepción que interconectan
/// el Middleware con el `MessageService` y el servicio de base de datos.
pub struct ChannelsSecondLayerMiddleware {
    /// Canal de envío utilizado por el `MessageService` para pasar respuestas al middleware.
    pub message_service_to_middleware: mpsc::Sender<MessageServiceResponse>,
    /// Extremo receptor donde el middleware escucha las respuestas del `MessageService`.
    pub middleware_from_message_service: mpsc::Receiver<MessageServiceResponse>,

    /// Canal de envío utilizado por el servicio de la base de datos para notificar al middleware.
    pub data_service_to_middleware: mpsc::Sender<TableDataVector>,
    /// Extremo receptor donde el middleware escucha eventos originados en la base de datos.
    pub middleware_from_data_service: mpsc::Receiver<TableDataVector>,
}

impl ChannelsSecondLayerMiddleware {
    /// Inicializa y crea los canales asíncronos para la comunicación interna
    /// de la segunda capa.
    ///
    /// # Argumentos
    /// * `buffer_size` - Capacidad máxima (en cantidad de mensajes) de los canales `mpsc`.
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
