//! # Servicio de Mensajería (Message Service)
//!
//! Este módulo contiene la lógica de transformación, enrutamiento y serialización de mensajes
//! que transitan entre el Edge y los dispositivos de la red local o la nube.
//!
//! - [`domain`]: Define las estructuras de datos, enumeraciones (como `HubMessage` y `ServerMessage`) que representan el esquema de mensajes JSON (MessagePack o Protobuf).
//! - [`logic`]: Implementa el actor responsable de procesar mensajes crudos, decodificarlos y enrutarlos al lugar correcto, o tomar datos internos y codificarlos para la salida.

pub mod domain;
pub mod logic;
