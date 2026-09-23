mod config;
mod context;
mod first_layer;
mod second_layer;
mod system;
mod third_layer;

pub mod grpc {
    tonic::include_proto!("grpc");
}

use crate::second_layer::{
    database::domain::DataService,
    database::repository::Repository,
    message::logic::MessageService,
    second_layer_middleware::{
        domain::ChannelsSecondLayerMiddleware, logic::SecondLayerMiddleware,
    },
};
use crate::system::domain::init_tracing;
use crate::system::fsm::init_fsm;
use crate::third_layer::{
    firmware::domain::FirmwareService, fsm::logic::FsmService, heartbeat::domain::HeartbeatService,
    metrics::domain::MetricsService, network::logic::NetworkService,
    third_layer_middleware::domain::ChannelsThirdLayerMiddleware,
};
use crate::{
    first_layer::{
        channels::ChannelsFirstLayer, grpc_service::domain::GrpcService, mqtt::domain::MqttService,
    },
    third_layer::third_layer_middleware::logic::ThirdLayerMiddleware,
};
use futures::stream::{FuturesUnordered, StreamExt};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let tracing_handle = init_tracing();

    info!("Iniciando sistema PIMAD Edge...");

    let app_context = init_fsm().await?;

    let new_filter = EnvFilter::new(app_context.system.rust_log.clone());
    if let Err(e) = tracing_handle.reload(new_filter) {
        error!("error al recargar el nivel de log: {:?}", e);
    }

    let repo = Repository::create_repository(&app_context.system.db_path).await;

    let channels_first_layer = ChannelsFirstLayer::new(app_context.system.buffer_size);
    let channels_second_middleware =
        ChannelsSecondLayerMiddleware::new(app_context.system.buffer_size);
    let channels_third_middleware =
        ChannelsThirdLayerMiddleware::new(app_context.system.buffer_size);
    let shutdown_token = CancellationToken::new();

    let (mqtt_service, mqtt_handle) = MqttService::new(
        channels_first_layer.to_message_service.clone(),
        app_context.clone(),
    );
    let (grpc_service, grpc_handle) =
        GrpcService::new(channels_first_layer.to_message_service, app_context.clone());

    let (msg_service, msg_handle) = MessageService::new(
        channels_second_middleware.message_service_to_middleware,
        channels_first_layer.message_service_from_services,
        grpc_handle.clone(),
        mqtt_handle.clone(),
        app_context.clone(),
    );

    let (data_service, data_handle) =
        DataService::new(channels_second_middleware.data_service_to_middleware, repo);
    let (fsm_service, fsm_handle) =
        FsmService::new(app_context.clone(), msg_handle.clone(), data_handle.clone());
    let (network_service, network_handle) = NetworkService::new(
        channels_third_middleware.network_service_to_middleware,
        app_context.clone(),
        data_handle.clone(),
        msg_handle.clone(),
        mqtt_handle.clone(),
    );
    let (firmware_service, firmware_handle) =
        FirmwareService::new(app_context.clone(), msg_handle.clone());
    let (heartbeat_service, heart_handle) = HeartbeatService::new(
        channels_third_middleware.heartbeat_service_to_middleware,
        data_handle.clone(),
    );
    let (metrics_service, metrics_handle) =
        MetricsService::new(msg_handle.clone(), app_context.clone());

    let snd_layer_middleware = SecondLayerMiddleware::builder()
        .middleware_from_data_service(channels_second_middleware.middleware_from_data_service)
        .middleware_from_message_service(channels_second_middleware.middleware_from_message_service)
        .build(
            msg_handle.clone(),
            fsm_handle.clone(),
            network_handle.clone(),
            firmware_handle.clone(),
            heart_handle.clone(),
            data_handle.clone(),
        )?;

    let thr_layer_middleware = ThirdLayerMiddleware::builder()
        .middleware_from_heartbeat_service(
            channels_third_middleware.middleware_from_heartbeat_service,
        )
        .middleware_from_network_service(channels_third_middleware.middleware_from_network_service)
        .build(metrics_handle.clone(), fsm_handle.clone())?;

    // ===================== SERVICIOS =====================

    let data_service_tokio_handle = tokio::spawn(data_service.run(shutdown_token.clone()));
    let firmware_service_tokio_handle = tokio::spawn(firmware_service.run(shutdown_token.clone()));
    let fsm_service_tokio_handle = tokio::spawn(fsm_service.run(shutdown_token.clone()));
    let metrics_service_tokio_handle = tokio::spawn(metrics_service.run(shutdown_token.clone()));
    let heartbeat_service_tokio_handle =
        tokio::spawn(heartbeat_service.run(shutdown_token.clone()));
    let message_serive_tokio_handle = tokio::spawn(msg_service.run(shutdown_token.clone()));
    let grpc_service_tokio_handle = tokio::spawn(grpc_service.run(shutdown_token.clone()));
    let mqtt_service_tokio_handle = tokio::spawn(mqtt_service.run(shutdown_token.clone()));
    let thr_mid_layer_tokio_handle = tokio::spawn(thr_layer_middleware.run(shutdown_token.clone()));
    let snd_mid_layer_tokio_handle = tokio::spawn(snd_layer_middleware.run(shutdown_token.clone()));
    let network_service_tokio_handle = tokio::spawn(network_service.run(shutdown_token.clone()));

    // ===================== SUPERVISION =====================

    let mut tasks: FuturesUnordered<_> = [
        data_service_tokio_handle,
        firmware_service_tokio_handle,
        fsm_service_tokio_handle,
        metrics_service_tokio_handle,
        heartbeat_service_tokio_handle,
        message_serive_tokio_handle,
        grpc_service_tokio_handle,
        mqtt_service_tokio_handle,
        thr_mid_layer_tokio_handle,
        snd_mid_layer_tokio_handle,
        network_service_tokio_handle,
    ]
    .into_iter()
    .collect();

    tokio::select! {
        Some(res) = tasks.next() => {
            error!("Error: una tarea terminó inesperadamente: {:?}", res);
            std::process::exit(1);
        }

        _ = tokio::signal::ctrl_c() => {
            info!("Info: señal shutdown recibida");
            shutdown_token.cancel();
        }
    }

    // ===================== ESPERAR CIERRE LIMPIO =====================

    while let Some(res) = tasks.next().await {
        if let Err(e) = res {
            error!("Error: task terminó con error durante el shutdown: {:?}", e);
        }
    }

    info!("Info: shutdown completo");

    Ok(())
}
