use crate::database::repository::pop_batch_generic;
use crate::message::domain::Monitor;
use sqlx::{Executor, QueryBuilder, Sqlite, SqlitePool};

/// Crea la tabla `monitor` en la base de datos si aún no existe.
///
/// # Descripción
///
/// Esta función inicializa el esquema de persistencia para los datos de tipo
/// [`Monitor`].
/// Se utiliza durante la fase de arranque del sistema, típicamente desde
/// la inicialización del [`Repository`].
///
/// La tabla almacena mediciones provenientes de nodos IoT y está diseñada
/// para escritura frecuente y lectura por lotes (*batch consumption*).
///
///
/// # Errores
///
/// Retorna un [`sqlx::Error`] si:
/// - la conexión al pool falla,
/// - la sentencia SQL no puede ejecutarse.
///
/// # Notas de diseño
///
/// - La función es *idempotente* gracias a `CREATE TABLE IF NOT EXISTS`.
/// - No realiza migraciones ni validaciones de versión del esquema.
/// - Se asume que el nombre de la tabla es estable y conocido por el sistema.
///

pub async fn create_table_monitor(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    pool.execute(
        r#"
        CREATE TABLE IF NOT EXISTS monitor (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_user_id       TEXT NOT NULL,
            destination_id       TEXT NOT NULL,
            timestamp            INTEGER NOT NULL,
            network_id           TEXT NOT NULL,
            heap_free            INTEGER NOT NULL,
            heap_min_free        INTEGER NOT NULL,
            heap_largest_block   INTEGER NOT NULL,
            uptime_sec           INTEGER NOT NULL
        );
        "#,
    )
    .await?;

    Ok(())
}

/// Inserta un lote (*batch*) de mediciones en la tabla `monitor`.
///
/// # Descripción
///
/// Recibe un vector de [`Monitor`] y persiste todos los elementos
/// dentro de una única transacción SQL.
///
/// Esta función está optimizada para inserciones por lotes y es utilizada
/// por tareas asincrónicas de larga duración que acumulan datos antes
/// de escribirlos en disco.
///
/// # Comportamiento transaccional
///
/// - Todas las inserciones se realizan dentro de una única transacción.
/// - Si una inserción falla, **ningún dato del batch es persistido**.
/// - La transacción se confirma solo si todas las operaciones tienen éxito.
///
/// # Parámetros
///
/// - `pool`: pool de conexiones SQLite compartido por el sistema.
/// - `data_vec`: vector de mediciones a persistir.
///
/// # Errores
///
/// Retorna un [`sqlx::Error`] si:
/// - falla el inicio de la transacción,
/// - alguna sentencia `INSERT` falla,
/// - el `commit` no puede completarse.
///
/// # Notas de diseño
///
/// - Esta función **consume** el vector recibido.
/// - No realiza validación semántica de los datos.
/// - Está pensada para ser llamada con batches relativamente pequeños
///   (controlados por `BATCH_SIZE` en capas superiores).
///

pub async fn insert_monitor(pool: &SqlitePool, data_vec: &Vec<Monitor>) -> Result<(), sqlx::Error> {
    if data_vec.is_empty() {
        return Ok(());
    }

    let mut query_builder: QueryBuilder<Sqlite> = QueryBuilder::new(
        "INSERT INTO monitor (
            sender_user_id, destination_id, timestamp, network_id,
            heap_free, heap_min_free, heap_largest_block, uptime_sec
        ) ",
    );

    query_builder.push_values(data_vec, |mut b, data| {
        b.push_bind(data.metadata.sender_user_id.clone())
            .push_bind(data.metadata.destination_id.clone())
            .push_bind(data.metadata.timestamp)
            .push_bind(data.network.clone())
            .push_bind(data.heap_free)
            .push_bind(data.heap_min_free)
            .push_bind(data.heap_largest_block)
            .push_bind(data.uptime_sec);
    });

    let query = query_builder.build();
    query.execute(pool).await?;

    Ok(())
}

/// Extrae y elimina un lote de mediciones de la tabla `monitor`.
///
/// # Descripción
///
/// Esta función obtiene un conjunto de filas antiguas de la tabla
/// `monitor`, las elimina de la base de datos y las retorna como
/// un vector de [`Monitor`].
///
/// Se utiliza cuando el sistema necesita vaciar la base de datos local de forma controlada.
///
/// # Detalles de implementación
///
/// - La selección y eliminación se realizan en una única sentencia SQL
///   (`DELETE ... RETURNING *`).
/// - El tamaño del batch está definido internamente por la capa de repositorio
///   (por ejemplo, mediante `LIMIT`).
///
/// # Errores
///
/// Retorna un [`sqlx::Error`] si:
/// - falla la consulta SQL,
/// - ocurre un error de conexión con la base de datos.
///
/// # Notas de diseño
///
/// - El orden de extracción es ascendente por `id`.
/// - Si la tabla está vacía, retorna un vector vacío.
/// - La lógica específica del SQL se delega a [`pop_batch_generic`].
///

pub async fn pop_batch_monitor(pool: &SqlitePool) -> Result<Vec<Monitor>, sqlx::Error> {
    pop_batch_generic(pool, "monitor").await
}
