//! # Tercera Capa (Third Layer)
//!
//! Este módulo contiene la lógica de negocio, las máquinas de estado y el monitoreo
//! de más alto nivel del Edge. Toma decisiones con base en los mensajes recibidos y orquesta
//! el comportamiento global del sistema y de la red de Hubs.
//!
//! - [`firmware`]: Gestiona el proceso asíncrono de actualización OTA tanto del propio Edge como de los Hubs (descarga, verificación, despliegue).
//! - [`fsm`]: Implementa la Máquina de Estados Finita (Finite State Machine) principal que controla el modo de operación del Edge (Inicio, Normal, Balance, Seguro).
//! - [`heartbeat`]: Responsable de mantener los latidos (*heartbeats*) periódicos con la nube y monitorizar el estado vital de los Hubs.
//! - [`metrics`]: Recolecta métricas de salud y consumo de recursos del sistema operativo del Edge.
//! - [`network`]: Gestor de la topología local, administración dinámica de altas y bajas de Hubs y Redes en MQTT y base de datos.
//! - [`quorum`]: Define umbrales y heurísticas (quorum) para transiciones de fase críticas.
//! - [`third_layer_middleware`]: Middleware de integración para la tercera capa, canalizando la comunicación interna.

pub mod firmware;
pub mod fsm;
pub mod heartbeat;
pub mod metrics;
pub mod network;
pub mod quorum;
pub mod third_layer_middleware;
