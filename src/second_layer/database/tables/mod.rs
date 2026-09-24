//! # Definición de Tablas
//!
//! Este módulo agrupa las definiciones individuales de cada tabla persistida
//! en la base de datos SQLite. Cada submódulo incluye habitualmente:
//! - El query de inicialización / creación de la tabla (schema).
//! - Queries de inserción de datos.
//! - Queries de consulta o eliminación.

pub mod alert_air;
pub mod alert_temp;
pub mod measurement;
pub mod monitor;
pub mod network;
pub mod balance_epoch;
pub mod hub;