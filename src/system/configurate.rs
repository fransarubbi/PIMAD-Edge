//! Configuración del sistema usando archivos toml.
//!

use crate::config::files::{PROTOCOL_TOML_PATH, SYSTEM_TOML_PATH};
use crate::context::domain::AppContext;
use crate::system::domain::{ErrorType, System};
use crate::third_layer::network::domain::NetworkManager;
use crate::third_layer::quorum::domain::ProtocolSettings;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Inicializa los componentes fundamentales del sistema y crea el `AppContext`
///
/// Lee los datos del archivo `system.toml` (el cual persiste y es de solo lectura) y
/// crea la estructura `System` con los campos fundamentales de funcionamiento del sistema.
/// Luego sigue con la lectura del archivo `protocol.toml`, el cual contiene la información necesaria
/// para configurar el comportamiento del protocolo de control y balanceo post fallo.
///
/// # Retorno
///
/// - `AppContext` si finaliza con éxito.
/// - `ErrorType` si falla o no puede ejecutarse.
///
/// # Requisitos del file system
///
/// etc/edge/files/system.toml
/// etc/edge/files/protocol.toml
///
pub fn initializing_system() -> Result<AppContext, ErrorType> {
    let system = match load_system_toml(Path::new(SYSTEM_TOML_PATH)) {
        Ok(system) => system,
        Err(e) => return Err(e),
    };
    let system = Arc::new(system);

    let protocol = match load_protocol_toml(Path::new(PROTOCOL_TOML_PATH)) {
        Ok(system) => system,
        Err(e) => return Err(e),
    };
    let protocol = Arc::new(protocol);
    let net_man = Arc::new(RwLock::new(NetworkManager::new_empty(&system)));
    Ok(AppContext::new(net_man, system, protocol))
}

/// Carga los datos del archivo `system.toml`
///
/// Lee los datos del archivo `system.toml`, el cual tiene los campos que necesita
/// la estructura `System` para funcionar.
///
/// # Retorno
///
/// - `System` si finaliza con éxito.
/// - `ErrorType` si falla o no puede ejecutarse.
///
/// # Requisitos del file system
///
/// El archivo toml en: `etc/edge/files/system.toml`
///
fn load_system_toml(path: &Path) -> Result<System, ErrorType> {
    let content = fs::read_to_string(path).map_err(|e| {
        ErrorType::SystemFile(format!(
            "Error al leer la configuración en '{}': {}",
            path.display(),
            e
        ))
    })?;

    toml::from_str(&content).map_err(|e| {
        ErrorType::SystemFile(format!(
            "Error de sintaxis en el TOML de configuración '{}': {}",
            path.display(),
            e
        ))
    })
}

/// Carga los datos del archivo `protocol.toml`
///
/// Lee los datos del archivo `protocol.toml`, el cual tiene los campos que necesita
/// la estructura `ProtocolSettings` para funcionar.
///
/// # Retorno
///
/// - `ProtocolSettings` si finaliza con éxito.
/// - `ErrorType` si falla o no puede ejecutarse.
///
/// # Requisitos del file system
///
/// El archivo toml en: `etc/edge/files/protocol.toml`
///
fn load_protocol_toml(path: &Path) -> Result<ProtocolSettings, ErrorType> {
    let content = fs::read_to_string(path).map_err(|e| {
        ErrorType::ProtocolFile(format!(
            "Error al leer la configuración en '{}': {}",
            path.display(),
            e
        ))
    })?;

    toml::from_str(&content).map_err(|e| {
        ErrorType::ProtocolFile(format!(
            "Error de sintaxis en el TOML de configuración '{}': {}",
            path.display(),
            e
        ))
    })
}
