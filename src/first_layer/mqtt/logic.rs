//! # Servicio Cliente MQTT
//!
//! Este módulo implementa un cliente asíncrono robusto para la comunicación MQTT,
//! diseñado para operar como un demonio en segundo plano dentro del sistema.
//!
//! ## Arquitectura
//! El servicio está construido sobre una Máquina de Estados Finitos (FSM) que gestiona
//! el ciclo de vida de la conexión:
//! 1.  `Waiting`: Espera a que el gestor de red (`NetworkManager`) cargue la configuración inicial.
//! 2.  `Init`: Crea el cliente TLS y establece las suscripciones iniciales basadas en la topología de red.
//! 3.  `Work`: Bucle principal multiplexado que maneja el envío/recepción de mensajes y eventos de reconexión.
//! 4.  `Error`: Estado de recuperación que aplica una pausa antes de intentar reinicializar la conexión.
//!
//! ## Características
//! - **Soporte mTLS:** Conexión segura mediante autenticación mutua TLS utilizando certificados preconfigurados.
//! - **Suscripciones Dinámicas:** Capacidad para suscribirse y desuscribirse de tópicos en tiempo de ejecución
//!   cuando la topología de la red cambia (mediante el comando `NetworksUpdated`).
//! - **Multiplexación No Bloqueante:** Utiliza `tokio::select!` para atender simultáneamente eventos de red,
//!   órdenes de apagado (`CancellationToken`) y el ciclo de eventos nativo de `rumqttc`.

use crate::config::mqtt_service::{
    BUFFER_SIZE, CA_EDGE_MQTT, CLEAN_SESSION, CRT_EDGE_MQTT, KEEP_ALIVE_TIMEOUT_SECS,
    KEY_EDGE_MQTT, MTLS_PORT,
};
use crate::context::domain::AppContext;
use crate::first_layer::mqtt::domain::{MqttServiceCommand, PayloadTopic};
use crate::second_layer::message::domain::SerializedMessage;
use crate::system::domain::{ErrorType, InternalEvent, System};
use crate::third_layer::network::domain::NetworkManager;
use rumqttc::{
    AsyncClient, Event, EventLoop, Incoming, MqttOptions, QoS, TlsConfiguration, Transport,
};
use std::collections::HashMap;
use std::fs;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument};

/// Representa los diferentes estados en el ciclo de vida del cliente MQTT.
enum StateClient {
    /// Estado inactivo, esperando la señal `NetworksReady` para comenzar.
    Waiting,
    /// Fase de configuración de conexión TLS y subscripciones iniciales.
    Init,
    /// Estado operativo normal. Contiene las instancias activas del cliente y el estado de los tópicos.
    Work {
        client: AsyncClient,
        event_loop: EventLoop,
        current_subs: HashMap<String, SubEntry>,
    },
    /// Estado de fallo. El sistema esperará antes de intentar transicionar de nuevo a `Init`.
    Error,
}

/// Mantiene el estado interno de seguimiento para una suscripción a un tópico MQTT específico.
///
/// Se utiliza junto con un algoritmo de "diffing" para determinar qué tópicos deben
/// añadirse o eliminarse dinámicamente en el broker.
#[derive(Clone)]
struct SubEntry {
    /// Indica si la suscripción es requerida por la configuración de red actual.
    active: bool,
    /// Indica si el cliente ya ha enviado exitosamente el comando SUBSCRIBE al broker.
    subscribed: bool,
    /// El Nivel de Calidad de Servicio configurado para este tópico.
    qos: QoS,
}

/// Inicializa la configuración base del cliente MQTT y establece el contexto TLS mTLS.
///
/// # Argumentos
/// * `system` - Estructura que contiene el ID del dispositivo y la dirección del host local.
///
/// # Retorno
/// Retorna una tupla `(AsyncClient, EventLoop)` lista para iniciar la conexión, o un `ErrorType`
/// si falla la lectura de los certificados.
fn create_local_mqtt(system: &System) -> Result<(AsyncClient, EventLoop), ErrorType> {
    let mut opts = MqttOptions::new(system.id_edge.clone(), system.host_local.clone(), MTLS_PORT);

    opts.set_keep_alive(Duration::from_secs(KEEP_ALIVE_TIMEOUT_SECS));
    opts.set_clean_session(CLEAN_SESSION);

    let ca = fs::read(CA_EDGE_MQTT)?;
    let cert = fs::read(CRT_EDGE_MQTT)?;
    let key = fs::read(KEY_EDGE_MQTT)?;

    opts.set_transport(Transport::Tls(TlsConfiguration::Simple {
        ca,
        alpn: None,
        client_auth: Some((cert, key)),
    }));

    Ok(AsyncClient::new(opts, BUFFER_SIZE))
}

/// Tarea principal asíncrona que ejecuta la Máquina de Estados del Cliente MQTT.
///
/// Esta función debe ser generada (spawned) por el hilo principal o el Core del sistema.
/// No retornará hasta que se reciba una señal a través del `CancellationToken`.
///
/// # Argumentos
/// * `tx` - Canal para enviar eventos internos del sistema (ej. conexión, mensajes recibidos) de vuelta al Core.
/// * `rx_msg` - Canal para recibir payloads serializados desde el Core para publicarlos en el broker.
/// * `rx_net` - Canal para recibir comandos de control (ej. red lista, actualizaciones de topología).
/// * `app_context` - Contexto global compartido de la aplicación.
/// * `shutdown` - Token de cancelación para terminar la tarea de forma limpia.
#[instrument(name = "mqtt", skip_all)]
pub async fn mqtt(
    tx: mpsc::Sender<InternalEvent>,
    mut rx_msg: mpsc::Receiver<SerializedMessage>,
    mut rx_net: mpsc::Receiver<MqttServiceCommand>,
    app_context: AppContext,
    shutdown: CancellationToken,
) {
    info!("iniciando tarea mqtt");
    let mut state = StateClient::Waiting;
    let mut flag: bool;

    loop {
        match &mut state {
            StateClient::Waiting => {
                while let Some(cmd) = rx_net.recv().await {
                    match cmd {
                        // NetworkManager cargo todas las redes en memoria y por ende
                        // es correcto hacer lectura de ellas para suscribirse a los tópicos
                        MqttServiceCommand::NetworksReady => {
                            state = StateClient::Init;
                            break;
                        }
                        _ => {}
                    }
                }
            }
            StateClient::Init => {
                flag = false;
                match create_local_mqtt(&app_context.system) {
                    Ok((client, event_loop)) => {
                        let current_subs = {
                            let manager = app_context.net_man.read().await;
                            collect_subscriptions(&manager)
                        };

                        for (topic, entry) in &current_subs {
                            if let Err(e) = client.subscribe(topic.clone(), entry.qos).await {
                                error!("no se pudo suscribir a {topic}: {e}");
                                flag = true;
                                continue;
                            }
                        }

                        if flag {
                            state = StateClient::Error;
                        } else {
                            state = StateClient::Work {
                                client,
                                event_loop,
                                current_subs,
                            };
                        }
                    }
                    Err(e) => {
                        error!("{e}");
                        state = StateClient::Error;
                    }
                }
            }
            StateClient::Work {
                client,
                event_loop,
                current_subs,
            } => {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        info!("shutdown recibido mqtt");
                        break;
                    }

                    Some(cmd) = rx_net.recv() => {
                        match cmd {
                            MqttServiceCommand::NetworksUpdated => {
                                let manager = app_context.net_man.read().await;
                                update_subscriptions(&manager, client, current_subs).await;
                            }
                            _ => {}
                        }
                    }

                    notification = event_loop.poll() => {
                        match notification {
                            Ok(event) => match event {
                                Event::Incoming(Incoming::Publish(packet)) => {
                                    let msg = PayloadTopic::new(
                                        packet.payload.to_vec(),
                                        packet.topic
                                    );
                                    if tx.send(InternalEvent::IncomingMessage(msg)).await.is_err() {
                                        error!("no se pudo enviar IncomingMessage desde mqtt");
                                    }
                                },
                                Event::Incoming(Incoming::ConnAck(_)) => {
                                    if tx.send(InternalEvent::LocalConnected).await.is_err() {
                                        error!("no se pudo enviar LocalConnected desde mqtt");
                                    }
                                },
                                _ => {}, // KeepAlive, Pings, etc.
                            },
                            Err(e) => {
                                error!("error en conexión mqtt: {}", e);
                                if tx.send(InternalEvent::LocalDisconnected).await.is_err() {
                                    error!("no se pudo enviar LocalDisconnected desde mqtt");
                                }
                                state = StateClient::Error;  // Vamos a error para reconectar
                            }
                        }
                    },

                    msg = rx_msg.recv() => {   // Enviar mensajes
                        match msg {
                            Some(msg) => {
                                let topic = msg.get_topic().to_string();
                                let res = client.publish(
                                    msg.get_topic().to_string(),
                                    cast_qos(&msg.get_qos()),
                                    msg.get_retain(),
                                    msg.get_payload()
                                ).await;

                                if let Err(e) = res {
                                    error!("publicando mensaje: {e}");
                                }
                                else {
                                    debug!("mensaje mqtt enviado con éxito. Tópico {topic}");
                                }
                            },
                            None => {   // El canal se cerró
                                error!("canal rx_msg cerrado");
                                state = StateClient::Error;
                            }
                        }
                    },
                }
            }
            StateClient::Error => {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        info!("shutdown recibido mqtt");
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

/// Extrae todos los tópicos requeridos desde la configuración de redes actual.
///
/// Utilizada exclusivamente durante la fase `Init` para poblar el mapa inicial de suscripciones.
fn collect_subscriptions(manager: &NetworkManager) -> HashMap<String, SubEntry> {
    debug!("suscribiendo a tópicos mqtt");
    let mut subs = HashMap::new();

    subs.insert(
        manager.topic_linkage_request.topic.clone(),
        SubEntry {
            active: true,
            subscribed: true,
            qos: cast_qos(&manager.topic_linkage_request.qos),
        },
    );

    for net in manager.networks.values() {
        subs.insert(
            net.topic_hub_state.topic.clone(),
            SubEntry {
                active: true,
                subscribed: true,
                qos: cast_qos(&net.topic_hub_state.qos),
            },
        );
        subs.insert(
            net.topic_data.topic.clone(),
            SubEntry {
                active: true,
                subscribed: true,
                qos: cast_qos(&net.topic_data.qos),
            },
        );
        subs.insert(
            net.topic_monitor.topic.clone(),
            SubEntry {
                active: true,
                subscribed: true,
                qos: cast_qos(&net.topic_monitor.qos),
            },
        );
        subs.insert(
            net.topic_alert_air.topic.clone(),
            SubEntry {
                active: true,
                subscribed: true,
                qos: cast_qos(&net.topic_alert_air.qos),
            },
        );
        subs.insert(
            net.topic_alert_temp.topic.clone(),
            SubEntry {
                active: true,
                subscribed: true,
                qos: cast_qos(&net.topic_alert_temp.qos),
            },
        );
        subs.insert(
            net.topic_hub_firmware_ok.topic.clone(),
            SubEntry {
                active: true,
                subscribed: true,
                qos: cast_qos(&net.topic_hub_firmware_ok.qos),
            },
        );
        subs.insert(
            net.topic_balance_mode_handshake.topic.clone(),
            SubEntry {
                active: true,
                subscribed: true,
                qos: cast_qos(&net.topic_balance_mode_handshake.qos),
            },
        );
        subs.insert(
            net.topic_hub_setting_ok.topic.clone(),
            SubEntry {
                active: true,
                subscribed: true,
                qos: cast_qos(&net.topic_hub_setting_ok.qos),
            },
        );
        subs.insert(
            net.topic_setting.topic.clone(),
            SubEntry {
                active: true,
                subscribed: true,
                qos: cast_qos(&net.topic_setting.qos),
            },
        );
        subs.insert(
            net.topic_queue_empty.topic.clone(),
            SubEntry {
                active: true,
                subscribed: true,
                qos: cast_qos(&net.topic_queue_empty.qos),
            },
        );
        subs.insert(
            net.topic_queue_empty_safe.topic.clone(),
            SubEntry {
                active: true,
                subscribed: true,
                qos: cast_qos(&net.topic_queue_empty_safe.qos),
            },
        );
    }

    subs
}

/// Algoritmo de sincronización dinámica de tópicos.
///
/// Compara los tópicos activos en el `NetworkManager` con el estado actual del mapa `subs`.
/// Realiza las llamadas asíncronas `subscribe` y `unsubscribe` al broker MQTT según sea necesario,
/// actualizando el estado interno para reflejar los cambios exitosos.
async fn update_subscriptions(
    manager: &NetworkManager,
    client: &AsyncClient,
    subs: &mut HashMap<String, SubEntry>,
) {
    debug!("actualizando subscripciones a tópicos mqtt");

    // Marcar todos como inactivos temporalmente
    for (_, entry) in subs.iter_mut() {
        entry.active = false;
    }

    // Reactivar o registrar los tópicos definidos en la topología de red actual
    for net in manager.networks.values() {
        all_topics(&net.topic_hub_state.topic, net.topic_hub_state.qos, subs);
        all_topics(&net.topic_data.topic, net.topic_data.qos, subs);
        all_topics(&net.topic_monitor.topic, net.topic_monitor.qos, subs);
        all_topics(&net.topic_alert_air.topic, net.topic_alert_air.qos, subs);
        all_topics(&net.topic_alert_temp.topic, net.topic_alert_temp.qos, subs);
        all_topics(
            &net.topic_hub_firmware_ok.topic,
            net.topic_hub_firmware_ok.qos,
            subs,
        );
        all_topics(
            &net.topic_balance_mode_handshake.topic,
            net.topic_balance_mode_handshake.qos,
            subs,
        );
        all_topics(
            &net.topic_hub_setting_ok.topic,
            net.topic_hub_setting_ok.qos,
            subs,
        );
        all_topics(&net.topic_setting.topic, net.topic_setting.qos, subs);
        all_topics(
            &net.topic_queue_empty.topic,
            net.topic_queue_empty.qos,
            subs,
        );
        all_topics(
            &net.topic_queue_empty_safe.topic,
            net.topic_queue_empty_safe.qos,
            subs,
        );
    }

    let mut to_sub = Vec::new();
    let mut to_unsub = Vec::new();

    // Determinar diferencias (Diff)
    for (topic, entry) in subs.iter() {
        if entry.active && !entry.subscribed {
            // Requerido pero no está suscrito
            to_sub.push((topic.clone(), entry.qos));
        } else if !entry.active && entry.subscribed {
            // Ya no es requerido pero sigue suscrito
            to_unsub.push(topic.clone());
        }
    }

    // Aplicar cambios al broker
    for (topic, qos) in to_sub {
        if client.subscribe(topic.clone(), qos).await.is_ok() {
            if let Some(entry) = subs.get_mut(&topic) {
                entry.subscribed = true;
            }
        }
    }

    for topic in to_unsub {
        if client.unsubscribe(topic.clone()).await.is_ok() {
            // Eliminación segura del registro de seguimiento local
            subs.remove(&topic);
        }
    }
}

/// Función auxiliar para el proceso de sincronización de suscripciones (`update_subscriptions`).
///
/// Marca un tópico existente como `active` en el mapa de seguimiento.
/// Si el tópico es nuevo, lo inserta con estado `active = true` y `subscribed = false`
/// para que la siguiente fase del algoritmo inicie la suscripción en red.
fn all_topics(topic: &String, qos: u8, subs: &mut HashMap<String, SubEntry>) {
    if let Some(entry) = subs.get_mut(topic) {
        entry.active = true;
    } else {
        // Si no existe en subs, hay que agregarlo
        let new_entry = SubEntry {
            active: true,
            subscribed: false,
            qos: cast_qos(&qos),
        };
        subs.insert(topic.clone(), new_entry);
    }
}

/// Convierte un entero primitivo al enumerado nativo `QoS` de la librería `rumqttc`.
///
/// Por defecto, o ante valores desconocidos, aplica la calidad de servicio más baja (`AtMostOnce`).
fn cast_qos(qos: &u8) -> QoS {
    match qos {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtMostOnce,
    }
}
