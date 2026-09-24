//! # Canales de la Primera Capa
//!
//! Este módulo define las estructuras y funciones utilizadas para establecer la
//! comunicación asíncrona mediante canales de Tokio (`mpsc`) entre la primera
//! capa (First Layer) y la segunda capa (Second Layer) del sistema.

use crate::system::domain::InternalEvent;
use tokio::sync::mpsc;
use tracing::info;

/// Contiene los canales de transmisión y recepción para comunicar la 
/// Primera Capa con el servicio de mensajería (Segunda Capa).
pub struct ChannelsFirstLayer {
    /// Canal de envío hacia el servicio de mensajería (hacia arriba).
    pub to_message_service: mpsc::Sender<InternalEvent>,
    /// Canal de recepción desde el servicio de mensajería.
    pub message_service_from_services: mpsc::Receiver<InternalEvent>,
}

impl ChannelsFirstLayer {
    /// Crea una nueva instancia de los canales de comunicación de la primera capa.
    ///
    /// # Argumentos
    ///
    /// * `buffer_size` - Capacidad máxima del buffer para los canales asíncronos `mpsc`.
    /// 
    /// # Retorno
    ///
    /// Una nueva estructura `ChannelsFirstLayer` con los canales listos para ser utilizados.
    pub fn new(buffer_size: usize) -> Self {
        info!("creando canales del middleware de capa 2");

        let (to_message_service, message_service_from_services) = mpsc::channel(buffer_size);

        Self {
            to_message_service,
            message_service_from_services,
        }
    }
}
