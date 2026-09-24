//! # Servicio de Heartbeat (Latidos)
//!
//! Este módulo implementa la lógica de monitoreo vital. Evalúa de forma periódica
//! la conectividad con el servidor central mediante el envío de mensajes asíncronos y 
//! espera respuestas dentro de un margen de tiempo.
//! 
//! Informa de inmediato a otras partes del sistema si se detecta una pérdida
//! o recuperación de conexión (Network Events).

pub mod logic;
pub mod domain;