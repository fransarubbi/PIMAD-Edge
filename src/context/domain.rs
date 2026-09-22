//! Definición del Contexto de Aplicación (Shared State).
//!
//! Este módulo implementa el patrón de **Estado Compartido** para aplicaciones asíncronas.
//! El `AppContext` actúa como un contenedor de "Inyección de Dependencias" manual,
//! agrupando los recursos que deben ser accesibles por múltiples tareas concurrentes
//! (Base de datos, Configuración, Caché en memoria).

use crate::quorum::domain::ProtocolSettings;
use crate::system::domain::System;
use crate::third_layer::network::domain::NetworkManager;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Contenedor del estado global y recursos compartidos del sistema.
///
/// Esta estructura está diseñada para ser **barata de clonar** (Shallow Clone).
/// Al derivar `Clone`, lo que se copian son los punteros inteligentes (`Arc`) y los
/// manejadores internos, no los datos en sí. Esto permite pasar una instancia de
/// `AppContext` a cada `tokio::spawn` sin impacto en el rendimiento.
///
/// # Componentes
///
/// - **Estado Mutable (`net_man`):** Caché de redes dinámica.
/// - **Configuración (`system`):** Datos estáticos del dispositivo.

#[derive(Clone, Debug)]
pub struct AppContext {
    /// Gestor de Redes (Caché en memoria).
    ///
    /// Se envuelve en `Arc<RwLock<...>>` porque:
    /// 1. **`Arc`**: Permite que múltiples hilos posean el gestor.
    /// 2. **`RwLock`**: Optimiza el acceso. Permite **múltiples lecturas simultáneas**
    ///    (varias tareas consultando tópicos a la vez) pero bloquea todo para una
    ///    **única escritura** (cuando se actualiza la configuración).
    pub net_man: Arc<RwLock<NetworkManager>>,

    /// Configuración del Sistema e Identidad del Edge.
    ///
    /// Se envuelve solo en `Arc<...>` porque es **inmutable** después del inicio.
    /// No se requiere bloqueo (`Lock`) para leer datos que nunca cambian, lo que
    /// mejora el rendimiento.
    pub system: Arc<System>,

    /// Configuración de Quorum.
    pub quorum: Arc<ProtocolSettings>,
}

impl AppContext {
    /// Construye un nuevo contexto de aplicación.
    ///
    /// # Parámetros
    ///
    /// - `repo`: Instancia inicializada del repositorio.
    /// - `net_man`: Gestor de redes ya envuelto en las primitivas de concurrencia.
    /// - `system`: Configuración del sistema ya envuelta en `Arc`.
    /// - `quorum`: Configuración del comportamiento del protocolo de balanceo.
    pub fn new(
        net_man: Arc<RwLock<NetworkManager>>,
        system: Arc<System>,
        quorum: Arc<ProtocolSettings>,
    ) -> Self {
        Self {
            net_man,
            system,
            quorum,
        }
    }
}
