//! # Máquina de Estados Finita (FSM)
//!
//! Este módulo implementa la FSM global que dicta el modo de operación del Edge.
//! Gobierna el flujo de mensajes hacia el servidor central, decidiendo cuándo
//! reportar datos de forma inmediata (Modo Normal) o almacenarlos (Modo Seguro),
//! además de manejar la reconexión e inicio del sistema.
//!
//! - [`domain`]: Define las estructuras de datos, eventos y los manejadores de comunicación de la FSM.
//! - [`logic`]: Contiene el bucle principal de transición de estados y la ejecución asíncrona (el runtime).

pub mod logic;
pub mod domain;