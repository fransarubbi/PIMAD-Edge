//! # Servicio de Base de Datos
//!
//! Este módulo encapsula el acceso asíncrono y las operaciones CRUD a la base de datos
//! SQLite utilizando `sqlx`. Actúa como punto de almacenamiento local intermedio para mensajes
//! generados cuando el Edge está desconectado del servidor, y persistencia de estados de redes.
//!
//! - [`logic`]: Contiene el servicio en segundo plano (`DataService`) que gestiona lotes de datos y atiende peticiones.
//! - [`domain`]: Estructuras de datos base que representan filas (`Row`) en las tablas de SQLite y comandos del actor.
//! - [`repository`]: Abstracciones (repositorios) con las queries SQL preparadas para acceder a las tablas.
//! - [`tables`]: Definición de la creación inicial de las tablas (esquema) en SQLite.

pub mod logic;
pub(crate) mod domain;
pub mod repository;
mod tables;