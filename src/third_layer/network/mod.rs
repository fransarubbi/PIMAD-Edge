//! # Servicio de Red (Network)
//!
//! Administra la topología de la red de Hubs, manejando el alta y la baja
//! dinámica de redes y dispositivos individuales, coordinando estos cambios 
//! en base de datos, colas de la capa inferior y notificaciones al servidor central.

pub mod domain;
pub mod logic;