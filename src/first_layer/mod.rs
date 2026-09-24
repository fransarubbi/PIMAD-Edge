//! # Primera Capa (First Layer)
//!
//! Este módulo agrupa los componentes de infraestructura y comunicaciones externas del sistema Edge.
//! Se encarga de manejar la conectividad bidireccional tanto con el nivel local (sensores/actuadores
//! vía MQTT) como con el nivel central o nube (servidor vía gRPC).
//!
//! - [`channels`]: Provee la configuración de canales MPSC para la comunicación interna entre capas.
//! - [`grpc_service`]: Implementa el cliente gRPC para la comunicación con el servidor central.
//! - [`mqtt`]: Administra el cliente y la conexión con el broker MQTT local.

pub mod channels;
pub mod grpc_service;
pub mod mqtt;
