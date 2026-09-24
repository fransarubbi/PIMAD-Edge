//! # Servicio de Métricas (Metrics)
//!
//! Este módulo es responsable de recolectar información sobre el estado de salud
//! del Edge (uso de CPU, RAM, temperaturas del hardware, etc.) y de gestionar su envío
//! al servidor central periódicamente.

pub mod domain;
pub mod logic;