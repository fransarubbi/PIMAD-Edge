//! # Middleware de la Tercera Capa
//!
//! Coordina la comunicación interna de los distintos servicios de la lógica de negocio (FSM, Heartbeat, Network).
//!
//! - [`domain`]: Estructura que agrupa los canales MPSC del middleware.
//! - [`logic`]: Bucle asíncrono que recibe eventos de los servicios de la capa 3 y los enruta entre ellos.

pub mod domain;
pub mod logic;
