//! # Middleware de la Segunda Capa
//!
//! Este módulo actúa como coordinador de los flujos de información dentro de la
//! Segunda Capa. Orquesta los canales de comunicación entre el servicio de mensajes (`MessageService`)
//! y la base de datos, antes de interactuar con la tercera capa.
//!
//! - [`domain`]: Define las estructuras de canales utilizadas en esta capa.
//! - [`logic`]: Contiene la máquina de estados y las tareas concurrentes que rutean eventos.

pub mod domain;
pub mod logic;
