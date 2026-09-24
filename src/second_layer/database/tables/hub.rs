//! Módulo de persistencia para la configuración de Hubs.
//!
//! Este módulo gestiona el ciclo de vida de los datos de los dispositivos "Hub" en la base de datos SQLite.
//! Almacena información crítica de conectividad (WiFi, MQTT), identidad (IDs) y configuración operativa
//! (modos de energía, sample rate).
//!
//! # Tabla `hub`
//! Es una tabla plana que almacena tanto los metadatos aplanados (`MetadataRow`) como
//! los campos específicos de configuración del Hub (`HubRow`).

use crate::second_layer::database::domain::HubRow;
use sqlx::{Executor, SqlitePool};

/// Inicializa la tabla `hub` en la base de datos.
///
/// Crea el esquema necesario para almacenar la configuración de los Hubs si no existe.
///
/// # Esquema
/// - `id`: Identificador interno (Primary Key).
/// - Campos de Metadatos: `sender_user_id`, `destination_type`, `destination_id`, `timestamp`, `topic_where_arrive`.
/// - Campos de Configuración: `network_id`, `wifi_ssid`, `wifi_password`, `mqtt_uri`, `device_name`, `sample`, `energy_mode`.
pub async fn create_table_hub(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    pool.execute(
        r#"
        CREATE TABLE IF NOT EXISTS hub (
            id                   TEXT NOT NULL PRIMARY KEY,
            network_id           TEXT NOT NULL,
            device_name          TEXT NOT NULL
        );
        "#,
    )
    .await?;

    Ok(())
}

/// Inserta un nuevo registro de Hub en la base de datos.
///
/// Desglosa la estructura jerárquica `HubRow` (que contiene `MetadataRow`)
/// en una inserción plana SQL.
///
/// # Errores
/// Retorna `sqlx::Error` si falla la conexión o la restricción de datos.
pub async fn insert_hub_table(pool: &SqlitePool, data: HubRow) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
            INSERT INTO hub (
                id,
                network_id,
                device_name
            )
            VALUES (?, ?, ?)
            "#,
    )
    .bind(data.id)
    .bind(data.network_id)
    .bind(data.device_name)
    .execute(pool)
    .await?;

    Ok(())
}

/// Elimina todos los Hubs asociados a una red específica.
///
/// Útil cuando se elimina una configuración de red completa y se deben limpiar
/// los dispositivos asociados a ella.
///
/// # Parámetros
/// - `id`: El `network_id` a buscar y eliminar.
pub async fn delete_hub_according_to_network(
    pool: &SqlitePool,
    id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        DELETE FROM hub
        WHERE network_id = ?
        "#,
    )
    .bind(id)
    .execute(pool)
    .await?;

    Ok(())
}

<<<<<<< HEAD:src/second_layer/database/tables/hub.rs
=======

/// Elimina un Hub específico basado en su identificador de usuario/dispositivo.
///
/// # Parámetros
/// - `id`: El `sender_user_id` único del dispositivo.
pub async fn delete_hub_according_to_id(pool: &SqlitePool, id: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        DELETE FROM hub
        WHERE id = ?
        "#
    )
        .bind(id)
        .execute(pool)
        .await?;

    Ok(())
}


>>>>>>> master:src/database/tables/hub.rs
/// Recupera todos los Hubs registrados en el sistema.
///
/// Mapea automáticamente las filas SQL a la estructura `HubRow`.
pub async fn get_all_hubs(pool: &SqlitePool) -> Result<Vec<HubRow>, sqlx::Error> {
    let result = sqlx::query_as::<_, HubRow>(
        r#"
        SELECT * FROM hub
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(result)
}

pub async fn count_hubs(pool: &SqlitePool) -> Result<i64, sqlx::Error> {
    let query = format!("SELECT COUNT(*) FROM {}", "hub");

    let count: i64 = sqlx::query_scalar(&query).fetch_one(pool).await?;

    Ok(count)
}
