//! # Segunda Capa
//!
//! Este módulo contiene la lógica de mediación y almacenamiento persistente del Edge.
//! Actúa como puente entre los servicios de mensajeria y base de datos con y la
//! tercera capa (lógica de negocio y procesamiento de dispositivos).
//!
//! - [`database`]: Gestiona la persistencia de datos y estados mediante SQLite, proporcionando repositorios para interactuar con la DB.
//! - [`message`]: Servicio centralizado de enrutamiento y transformación de mensajes. Evalúa de dónde viene un mensaje, lo deserializa/serializa y lo envía a donde deba ir.
//! - [`second_layer_middleware`]: Middleware de la segunda capa que gestiona el flujo bidireccional entre la capa 2 y las capas superiores/inferiores.

pub mod database;
pub mod message;
pub mod second_layer_middleware;
