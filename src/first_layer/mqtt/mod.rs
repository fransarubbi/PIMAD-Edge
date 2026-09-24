//! # Módulo MQTT
//!
//! Este módulo contiene la implementación del cliente MQTT y su orquestación, 
//! permitiendo la comunicación a nivel local con los Hubs.
//!
//! - [`logic`]: Contiene la máquina de estados y la interacción directa con la librería MQTT (`rumqttc`).
//! - [`domain`]: Proporciona la interfaz unificada (`MqttService` y `MqttHandle`) para que el resto de la aplicación interactúe con el cliente MQTT.

pub mod logic;
pub mod domain;
