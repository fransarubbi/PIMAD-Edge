//! Módulo de persistencia para el estado de "Balance Epoch".
//!
//! Este módulo gestiona la persistencia del contador de época (epoch) utilizado
//! por la Máquina de Estados (FSM) durante el modo de balanceo.
//! Permite guardar el estado actual para recuperarlo tras un reinicio del sistema,
//! garantizando la continuidad de la lógica de sincronización.


use sqlx::{Executor, Row, SqlitePool};


/// Crea la tabla `balance_epoch` en la base de datos si no existe.
///
/// # Esquema
///
/// - `id`: Entero, llave primaria autoincremental.
/// - `epoch`: Entero no nulo, almacena el valor de la época.
///
/// # Errores
///
/// Retorna `sqlx::Error` si hay problemas de conexión o ejecución del SQL.
pub async fn create_table_balance_epoch(pool: &SqlitePool) -> Result<(), sqlx::Error>  {
    pool.execute(
        r#"
        CREATE TABLE IF NOT EXISTS balance_epoch (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            epoch    INTEGER NOT NULL
        );
        "#
    )
        .await?;
    Ok(())
}


/// Inserta un nuevo valor de época en el registro histórico.
///
/// En lugar de actualizar una única fila, este diseño inserta un nuevo registro
/// cada vez, manteniendo un historial implícito ordenado por `id`.
///
/// # Parámetros
///
/// - `pool`: Pool de conexiones a SQLite.
/// - `balance`: El valor `u32` del epoch a persistir.
pub async fn insert_balance_epoch(pool: &SqlitePool,
                                  balance: u32
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO balance_epoch (
            epoch
        )
        VALUES (?)
        "#
    )
        .bind(balance)
        .execute(pool)
        .await?;

    Ok(())
}


/// Obtiene el valor de época más reciente almacenado.
///
/// Realiza una consulta ordenando por `id` descendente y limitando a 1 para
/// obtener el último estado guardado.
///
/// # Retorno
///
/// - `Ok(u32)`: El último valor de epoch guardado.
/// - `Ok(0)`: Si la tabla está vacía (comportamiento por defecto inicial).
/// - `Err(sqlx::Error)`: Si falla la consulta a la base de datos.
pub async fn get_balance_epoch(pool: &SqlitePool) -> Result<u32, sqlx::Error> {
    let row_opt = sqlx::query(
        r#"
        SELECT epoch
        FROM balance_epoch
        ORDER BY id DESC
        LIMIT 1
        "#
    )
        .fetch_optional(pool)
        .await?;

    match row_opt {
        Some(row) => {
            let epoch: u32 = row.try_get("epoch")?;
            Ok(epoch)
        }
        None => Ok(0),
    }
}