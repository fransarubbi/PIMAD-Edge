//! # Servicio de Firmware (OTA)
//!
//! Este módulo encapsula todo el proceso de Actualización por Aire (Over-The-Air, OTA).
//! Administra la descarga e instalación de nuevos binarios, tanto para el propio
//! sistema Edge (auto-actualización) como para los dispositivos (Hubs) de las subredes.
//!
//! - [`domain`]: Contiene la definición del servicio `FirmwareService` y su `FirmwareHandle` asociado.
//! - [`logic`]: Contiene los algoritmos asíncronos para contactar con repositorios externos (como GitHub releases) y para iterar sobre los Hubs a actualizar.

pub mod logic;
pub mod domain;