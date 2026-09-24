use crate::second_layer::database::domain::NetworkRow;
use sqlx::{Executor, SqlitePool};

/// Crea la tabla `network` en la base de datos si aún no existe.
///
/// # Descripción
///
/// Esta función inicializa el esquema de persistencia para los datos de tipo
/// [`Network`]. Se utiliza durante la fase de arranque del sistema, típicamente
/// desde la inicialización del [`Repository`].
///
/// La tabla almacena la configuración de las redes IoT conocidas por el Edge.
///
/// # Esquema de la tabla
///
/// La tabla `network` contiene las siguientes columnas:
///
/// - `id_network`: Identificador único (Primary Key, ejemplo: "sala7").
/// - `active`: Flag booleano para habilitar/deshabilitar la red lógicamente.
///
/// # Errores
///
/// Retorna un [`sqlx::Error`] si:
/// - La conexión al pool falla.
/// - La sentencia SQL es inválida o no puede ejecutarse.
///
/// # Notas de diseño
///
/// - La función es *idempotente* (`IF NOT EXISTS`).
/// - No realiza migraciones: si el esquema cambia en el código, se debe
///   actualizar la base de datos manualmente o borrar el archivo `.db`.
pub async fn create_table_network(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    pool.execute(
        r#"
        CREATE TABLE IF NOT EXISTS network (
            id_network               TEXT PRIMARY KEY,
            active                   BOOLEAN NOT NULL DEFAULT TRUE
        );
        "#,
    )
    .await?;
    Ok(())
}

/// Inserta una nueva configuración de red en la tabla `network`.
///
/// # Descripción
///
/// Recibe un objeto de dominio [`Network`], extrae sus campos y los persiste
/// mediante una operación atómica.
///
/// # Parámetros
///
/// - `pool`: Pool de conexiones SQLite compartido.
/// - `data`: Estructura [`Network`] con la información a persistir.
///
/// # Errores
///
/// Retorna un [`sqlx::Error`] si:
/// - Ocurre una violación de la *Primary Key* (el `id_network` ya existe).
/// - Falla la conexión o la ejecución de la query.
///
/// # Notas
///
/// - Los campos booleanos se convierten automáticamente a enteros (0/1) por SQLite.
/// - Los campos de tipo struct interno (`Topic`) se aplanan en columnas individuales.
pub async fn insert_network_database(
    pool: &SqlitePool,
    data: NetworkRow,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO network (
            id_network,
            active
        )
        VALUES (?, ?)
        "#,
    )
    .bind(data.id_network)
    .bind(data.active)
    .execute(pool)
    .await?;

    Ok(())
}

/// Elimina una red de la base de datos.
///
/// # Descripción
///
/// Borra la fila correspondiente al `id` proporcionado.
///
/// # Parámetros
///
/// * `id` - El identificador único de la red (ej. "sala7").
///
/// # Errores
///
/// Retorna un [`sqlx::Error`] si falla la ejecución SQL.
///
/// # Notas
///
/// Si el ID no existe, la operación es exitosa pero no borra nada
/// (rows affected = 0). No retorna error en ese caso.
pub async fn delete_network_database(pool: &SqlitePool, id: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        DELETE FROM network
        WHERE id_network = ?
        "#,
    )
    .bind(id)
    .execute(pool)
    .await?;

    Ok(())
}

/// Recupera todas las redes almacenadas en la base de datos.
///
/// # Descripción
///
/// Ejecuta un `SELECT *` sobre la tabla `network` y mapea cada fila
/// a una estructura [`NetworkRow`].
///
/// # Uso
///
/// Esta función es fundamental durante el inicio del sistema para poblar
/// el `NetworkManager` en memoria con la configuración persistida.
///
/// # Errores
///
/// Retorna [`sqlx::Error`] si:
/// - El esquema de la base de datos no coincide con [`NetworkRow`].
/// - Falla la conexión.
pub async fn get_all_network_data(pool: &SqlitePool) -> Result<Vec<NetworkRow>, sqlx::Error> {
    let result = sqlx::query_as::<_, NetworkRow>(
        r#"
        SELECT * FROM network
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(result)
}

pub async fn upsert_network(pool: &SqlitePool, data: NetworkRow) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO network (
            id_network,
            active
        ) VALUES (?, ?)
        ON CONFLICT(id_network) DO UPDATE SET
            active       = excluded.active
        "#,
    )
    .bind(data.id_network)
    .bind(data.active)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn count_networks(pool: &SqlitePool) -> Result<i64, sqlx::Error> {
    let query = format!("SELECT COUNT(*) FROM {}", "network");

    let count: i64 = sqlx::query_scalar(&query).fetch_one(pool).await?;

    Ok(count)
}
