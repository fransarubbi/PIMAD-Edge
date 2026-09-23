//! Módulo de comunicación gRPC con el servidor central.
//!
//! Este módulo establece y mantiene una conexión **gRPC bidireccional segura (mTLS)**
//! entre el gateway/dispositivo Edge y el servidor. Implementa un cliente
//! resiliente capaz de recuperarse de caídas de red mediante reconexión automática.
//!
//!
//! # Arquitectura
//!
//! El diseño se basa en un modelo de actores asíncronos de `tokio` para aislar la red
//! del resto del sistema operativo (Core). Se divide en dos componentes principales:
//!
//! 1. **[`GrpcService`]**: El orquestador público. Recibe datos del sistema mediante un canal,
//!    levanta la tarea de red en segundo plano y enruta los mensajes hacia/desde ella.
//! 2. **Bucle de conexión (`grpc`)**: Una máquina de estados pura que maneja el ciclo
//!    de vida de la capa de transporte (HTTP/2), cifrado (TLS) y streaming (Tonic).
//!
//! # Seguridad
//!
//! Requiere Autenticación Mutua TLS (mTLS). El cliente presenta sus propios certificados
//! (`CRT_EDGE_GRPC`, `KEY_EDGE_GRPC`) y valida al servidor mediante una CA compartida (`CA_EDGE_GRPC`).

use crate::config::grpc_service::*;
use crate::context::domain::AppContext;
use crate::grpc::edge_service_client::EdgeServiceClient;
use crate::grpc::{FromEdge, ToEdge};
use crate::system::domain::{ErrorType, InternalEvent};
use std::fs;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::Request;
use tonic::codec::CompressionEncoding;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};
use tracing::{error, info, instrument, warn};

#[derive(Clone)]
pub struct GrpcHandle {
    tx: mpsc::Sender<FromEdge>,
}

impl GrpcHandle {
    pub async fn send_serialized(&self, data: FromEdge) {
        let _ = self.tx.send(data).await;
    }
}

/// Servicio administrador de la conexión gRPC.
///
/// Actúa como una fachada que envuelve la complejidad del cliente bidireccional de `tonic`.
/// Al invocar `run`, crea los canales puente necesarios y lanza el hilo de red concurrente.
pub struct GrpcService {
    /// Canal de salida hacia el Core del sistema (eventos de red y mensajes entrantes).
    sender: mpsc::Sender<InternalEvent>,
    /// Canal de entrada
    rx: mpsc::Receiver<FromEdge>,
    /// Contexto global de la aplicación (configuración, secretos, base de datos).
    context: AppContext,
}

impl GrpcService {
    /// Crea una nueva instancia de `GrpcService`.
    ///
    /// No inicia la conexión. Se debe llamar a `run()` explícitamente.
    pub fn new(sender: mpsc::Sender<InternalEvent>, context: AppContext) -> (Self, GrpcHandle) {
        let (tx, rx) = mpsc::channel(10);
        let service = Self {
            sender,
            rx,
            context,
        };
        let handle = GrpcHandle { tx };
        (service, handle)
    }

    /// Inicia el servicio y bloquea la tarea actual de forma asíncrona.
    ///
    /// 1. Crea canales proxy para comunicar esta estructura con el demonio gRPC.
    /// 2. Hace un `tokio::spawn` del bucle `grpc`.
    /// 3. Entra en un select que escucha apagados del sistema y redirige el tráfico
    ///    bidireccional entre la app y el demonio gRPC.
    pub async fn run(mut self, shutdown: CancellationToken) {
        let (tx_to_core, mut rx_from_grpc) = mpsc::channel::<InternalEvent>(50);
        let (tx, rx) = mpsc::channel::<FromEdge>(50);

        tokio::spawn(grpc(tx_to_core, rx, self.context.clone(), shutdown.clone()));

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("shutdown recibido GrpcService");
                    break;
                }

                Some(edge_upload) = self.rx.recv() => {
                    if tx.send(edge_upload).await.is_err() {
                        error!("no se pudo enviar EdgeUpload a remote_grpc");
                    }
                }

                Some(response) = rx_from_grpc.recv() => {
                    if self.sender.send(response).await.is_err() {
                        error!("no se pudo enviar InternalEvent desde remote_grpc");
                    }
                }
            }
        }
    }
}

/// Estados posibles de la máquina de estados del cliente gRPC.
#[derive(Debug)]
enum StateClient {
    /// Estado inicial o de reconexión. Intenta crear el canal TLS y negociar el stream.
    Init,
    /// Estado operativo. Mantiene activos los streams de entrada y salida bidireccionales.
    Work {
        /// Canal interno donde se inyectan datos para enviar al servidor.
        tx_session: mpsc::Sender<FromEdge>,
        /// Stream receptivo directo desde el servidor `tonic`.
        inbound_stream: tonic::Streaming<ToEdge>,
    },
    /// Ocurrió un fallo (desconexión, error TLS, etc). Dispara un cool-down antes de volver a `Init`.
    Error,
}

/// Crea y configura un canal HTTP/2 (Transporte subyacente de gRPC) con cifrado mTLS.
///
/// # Errores
/// Retorna `ErrorType::Generic` si no puede leer los certificados físicos.
/// Retorna `ErrorType::Endpoint` si la URL es inválida o la conexión inicial falla.
async fn create_tls_channel(system: &crate::system::domain::System) -> Result<Channel, ErrorType> {
    let ca_pem = fs::read(CA_EDGE_GRPC).map_err(|e| {
        error!("fallo leyendo CA gRPC: {}", e);
        ErrorType::Generic
    })?;
    let cert_pem = fs::read(CRT_EDGE_GRPC).map_err(|e| {
        error!("fallo leyendo Cert gRPC: {}", e);
        ErrorType::Generic
    })?;
    let key_pem = fs::read(KEY_EDGE_GRPC).map_err(|e| {
        error!("fallo leyendo Key gRPC: {}", e);
        ErrorType::Generic
    })?;

    let ca = Certificate::from_pem(ca_pem);
    let identity = Identity::from_pem(cert_pem, key_pem);

    let tls = ClientTlsConfig::new()
        .ca_certificate(ca)
        .identity(identity)
        .domain_name(system.cn.clone());

    let url = format!("https://{}:{}", system.host_server, system.host_port);

    let endpoint = Channel::from_shared(url)
        .map_err(|_| ErrorType::Endpoint)?
        .tls_config(tls)
        .map_err(|_| ErrorType::Endpoint)?
        .connect_timeout(Duration::from_secs(TLS_CERT_TIMEOUT_SECS))
        .keep_alive_timeout(Duration::from_secs(KEEP_ALIVE_TIMEOUT_SECS))
        .http2_keep_alive_interval(Duration::from_secs(HTTP2_KEEP_ALIVE_INTERVAL_SECS))
        .keep_alive_while_idle(true);

    endpoint.connect().await.map_err(|e| {
        error!("no se pudo conectar gRPC: {}", e);
        ErrorType::Endpoint
    })
}

/// Motor asíncrono que gobierna la conexión gRPC mediante una máquina de estados finitos.
///
/// Se encarga de:
/// - Crear el stream con compresión GZIP en ambos sentidos.
/// - Leer del canal local para enviar al stream (Upstream).
/// - Leer del stream para enviar al canal local (Downstream).
/// - Ejecutar el retroceso (delay) de 5 segundos al producirse fallos.
#[instrument(name = "grpc", skip_all)]
async fn grpc(
    tx: mpsc::Sender<InternalEvent>,
    mut rx_outbound: mpsc::Receiver<FromEdge>,
    app_context: AppContext,
    shutdown: CancellationToken,
) {
    let mut state = StateClient::Init;

    loop {
        match &mut state {
            StateClient::Init => {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        info!("shutdown recibido en grpc (Init)");
                        break;
                    }

                    result = create_tls_channel(&app_context.system) => {
                        match result {
                            Ok(channel) => {
                                // Crear cliente con compresión
                                let mut grpc_client = EdgeServiceClient::new(channel)
                                    .send_compressed(CompressionEncoding::Gzip)
                                    .accept_compressed(CompressionEncoding::Gzip);

                                // Configurar stream bidireccional
                                let (tx_session, rx_session) = mpsc::channel::<FromEdge>(100);
                                let request = Request::new(ReceiverStream::new(rx_session));

                                match grpc_client.connect_stream(request).await {
                                    Ok(response) => {
                                        info!("gRPC Conectado. Stream Bidireccional iniciado");

                                        let inbound_stream = response.into_inner();

                                        if tx.send(InternalEvent::ServerConnected).await.is_err() {
                                            error!("no se pudo enviar el evento ServerConnected");
                                        }
                                        state = StateClient::Work { tx_session, inbound_stream };
                                    }
                                    Err(e) => {
                                        error!("falló la conexión gRPC. {e}");
                                        state = StateClient::Error;
                                    }
                                }
                            }
                            Err(e) => {
                                error!("{e}");
                                state = StateClient::Error;
                            }
                        }
                    }
                }
            }
            StateClient::Work {
                tx_session,
                inbound_stream,
            } => {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        info!("shutdown recibido gRPC (Work)");
                        break;
                    }

                    // [UPSTREAM]: Datos del Core que deben ir al Servidor
                    msg_opt = rx_outbound.recv() => {
                        match msg_opt {
                            Some(msg) => {
                                if let Err(e) = tx_session.send(msg.clone()).await {
                                    warn!("stream de envío cerrado {e}");
                                    state = StateClient::Error;
                                }
                            }
                            None => {
                                info!("canal de salida cerrado, terminando tarea remota");
                                return;
                            }
                        }
                    }

                    // [DOWNSTREAM]: Datos o comandos desde el Servidor hacia el Core
                    server_msg = inbound_stream.next() => {
                        match server_msg {
                            Some(Ok(download_msg)) => {
                                if tx.send(InternalEvent::IncomingGrpc(download_msg)).await.is_err() {
                                    error!("no se pudo enviar el mensaje recibido del servidor");
                                }
                            }
                            Some(Err(e)) => {
                                error!("{e}");
                                state = StateClient::Error;
                            }
                            None => {
                                warn!("stream cerrado por el servidor");
                                state = StateClient::Error;
                            }
                        }
                    }
                }
            }
            StateClient::Error => {
                if tx.send(InternalEvent::ServerDisconnected).await.is_err() {
                    error!("no se pudo enviar el evento ServerDisconnected");
                }
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        info!("shutdown recibido gRPC (Error)");
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {
                        state = StateClient::Init;
                    }
                }
            }
        }
    }
}
