use crate::system::domain::InternalEvent;
use crate::third_layer::fsm::logic::FsmHandle;
use crate::third_layer::metrics::domain::MetricsHandle;
use crate::third_layer::network::logic::NetworkServiceResponse;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

pub struct ThirdLayerMiddleware {
    middleware_from_network_service: mpsc::Receiver<NetworkServiceResponse>,
    middleware_from_heartbeat_service: mpsc::Receiver<InternalEvent>,
    middleware_to_fsm_service: FsmHandle,
    middleware_to_metrics_service: MetricsHandle,
}

#[derive(Default)]
pub struct ThirdLayerMiddlewareBuilder {
    middleware_from_network_service: Option<mpsc::Receiver<NetworkServiceResponse>>,
    middleware_from_heartbeat_service: Option<mpsc::Receiver<InternalEvent>>,
}

impl ThirdLayerMiddlewareBuilder {
    pub fn middleware_from_network_service(
        mut self,
        ch: mpsc::Receiver<NetworkServiceResponse>,
    ) -> Self {
        self.middleware_from_network_service = Some(ch);
        self
    }

    pub fn middleware_from_heartbeat_service(mut self, ch: mpsc::Receiver<InternalEvent>) -> Self {
        self.middleware_from_heartbeat_service = Some(ch);
        self
    }

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
    pub fn builder() -> ThirdLayerMiddlewareBuilder {
        ThirdLayerMiddlewareBuilder::default()
    }

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
