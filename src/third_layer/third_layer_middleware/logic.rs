//! # Lógica del Middleware de la Tercera Capa
//!
//! Orquesta y rutea los eventos generados por los servicios de alto nivel 
//! (como `NetworkService` y `HeartbeatService`) hacia las otras piezas del
//! sistema de capa 3, tales como la máquina de estados (`FsmService`) 
//! y el recolector de métricas (`MetricsService`).

use crate::system::domain::InternalEvent;
use crate::third_layer::fsm::logic::FsmHandle;
use crate::third_layer::metrics::domain::MetricsHandle;
use crate::third_layer::network::logic::NetworkServiceResponse;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

/// Orquestador y enrutador asíncrono para la Tercera Capa.
///
/// Escucha los canales MPSC en un bucle select, redirigiendo 
/// los eventos del gestor de topología de redes y del monitor de latidos
/// a la máquina de estados principal y al motor de métricas del Edge.
pub struct ThirdLayerMiddleware {
    /// Canal para recibir eventos procedentes de `NetworkService`.
    middleware_from_network_service: mpsc::Receiver<NetworkServiceResponse>,
    /// Canal para recibir eventos procedentes de `HeartbeatService`.
    middleware_from_heartbeat_service: mpsc::Receiver<InternalEvent>,
    /// Handle para comandar acciones sobre el `FsmService`.
    middleware_to_fsm_service: FsmHandle,
    /// Handle para inyectar eventos al servicio recolector de métricas.
    middleware_to_metrics_service: MetricsHandle,
}

/// Patrón Builder para crear instancias seguras de `ThirdLayerMiddleware`.
#[derive(Default)]
pub struct ThirdLayerMiddlewareBuilder {
    middleware_from_network_service: Option<mpsc::Receiver<NetworkServiceResponse>>,
    middleware_from_heartbeat_service: Option<mpsc::Receiver<InternalEvent>>,
}

impl ThirdLayerMiddlewareBuilder {
    /// Inyecta el canal receptor de eventos de red.
    pub fn middleware_from_network_service(
        mut self,
        ch: mpsc::Receiver<NetworkServiceResponse>,
    ) -> Self {
        self.middleware_from_network_service = Some(ch);
        self
    }

    /// Inyecta el canal receptor de eventos del monitor de latidos.
    pub fn middleware_from_heartbeat_service(mut self, ch: mpsc::Receiver<InternalEvent>) -> Self {
        self.middleware_from_heartbeat_service = Some(ch);
        self
    }

    /// Ensambla y retorna una instancia válida de `ThirdLayerMiddleware`.
    /// 
    /// Retorna un `Err` si faltan inyectar canales obligatorios.
    pub fn build(
        self,
        metrics: MetricsHandle,
        fsm: FsmHandle,
    ) -> Result<ThirdLayerMiddleware, String> {
        Ok(ThirdLayerMiddleware {
            middleware_to_fsm_service: fsm,
            middleware_to_metrics_service: metrics,
            middleware_from_network_service: self
                .middleware_from_network_service
                .ok_or("falta middleware_from_network_service")?,
            middleware_from_heartbeat_service: self
                .middleware_from_heartbeat_service
                .ok_or("falta middleware_from_heartbeat_service")?,
        })
    }
}

impl ThirdLayerMiddleware {
    /// Inicia un nuevo constructor para `ThirdLayerMiddleware`.
    pub fn builder() -> ThirdLayerMiddlewareBuilder {
        ThirdLayerMiddlewareBuilder::default()
    }

    /// Entra en el bucle principal de multiplexación (select!).
    ///
    /// Se encarga de:
    /// - Atender peticiones de encendido (Run) de parte de la red para iniciar el runtime de la FSM.
    /// - Escuchar los eventos de estado del servidor (ServerConnected / ServerDisconnected) e informar a métricas.
    /// - Atender a la señal de apagado general (shutdown).
    pub async fn run(mut self, shutdown: CancellationToken) {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("Info: shutdown recibido Core");
                    break;
                }

                Some(response) = self.middleware_from_network_service.recv() => {
                    match response {
                        NetworkServiceResponse::Run => {
                            let result = self.middleware_to_fsm_service.create_runtime().await;
                            if !result {
                                error!("no se pudo crear runtime de la fsm");
                            }
                        }
                    }
                }
                Some(response) = self.middleware_from_heartbeat_service.recv() => {
                    match response {
                        InternalEvent::ServerConnected | InternalEvent::ServerDisconnected => {
                            self.middleware_to_metrics_service.connection(response).await;
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}
