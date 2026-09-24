use crate::second_layer::database::repository::pop_batch_generic;
use crate::second_layer::message::domain::AlertAir;
use sqlx::{Executor, QueryBuilder, Sqlite, SqlitePool};

/// Crea la tabla `alert_air` en la base de datos si aún no existe.
///
/// # Descripción
///
/// Esta función inicializa el esquema de persistencia para los datos de tipo
/// [`AlertAir`].
/// Se utiliza durante la fase de arranque del sistema, típicamente desde
/// la inicialización del [`Repository`].
///
/// La tabla almacena mediciones provenientes de nodos IoT y está diseñada
/// para escritura frecuente y lectura por lotes (*batch consumption*).
///
/// # Esquema de la tabla
///
/// La tabla `alert_air` contiene las siguientes columnas:
///
/// - `id`: clave primaria autoincremental.
/// - `sender_user_id`: identificador del nodo emisor.
/// - `destination_type`: tipo de destino lógico del mensaje.
/// - `destination_id`: identificador del destino.
/// - `timestamp`: instante de generación de la medición (formato texto).
/// - `co2_initial_ppm`: ppm de CO₂ estable previo a la alarma.
/// - `co2_actual_ppm`: ppm de CO₂ en alarma.
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

pub async fn create_table_alert_air(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    pool.execute(
        r#"
        CREATE TABLE IF NOT EXISTS alert_air (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_user_id       TEXT NOT NULL,
            destination_id       TEXT NOT NULL,
            timestamp            INTEGER NOT NULL,
            network_id           TEXT NOT NULL,
            initial_air_quality      REAL NOT NULL,
            actual_air_quality       REAL NOT NULL
        );
        "#,
    )
    .await?;
    Ok(())
}

/// Inserta un lote (*batch*) de mediciones en la tabla `alert_air`.
///
/// # Descripción
///
/// Recibe un vector de [`AlertAir`] y persiste todos los elementos
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

pub async fn insert_alert_air(
    executor: impl sqlx::Executor<'_, Database = sqlx::Sqlite>,
    data_vec: &Vec<AlertAir>,
) -> Result<(), sqlx::Error> {
    if data_vec.is_empty() {
        return Ok(());
    }

    let mut query_builder: QueryBuilder<Sqlite> = QueryBuilder::new(
        "INSERT INTO alert_air (
            sender_user_id, destination_id, timestamp,
            network_id, initial_air_quality, actual_air_quality
      )",
    );

    query_builder.push_values(data_vec, |mut b, data| {
        b.push_bind(data.metadata.sender_user_id.clone())
            .push_bind(data.metadata.destination_id.clone())
            .push_bind(data.metadata.timestamp)
            .push_bind(data.network.clone())
            .push_bind(data.initial_air_quality)
            .push_bind(data.actual_air_quality);
    });

    let query = query_builder.build();
    query.execute(executor).await?;

    Ok(())
}

/// Extrae y elimina un lote de mediciones de la tabla `alert_air`.
///
/// # Descripción
///
/// Esta función obtiene un conjunto de filas antiguas de la tabla
/// `alert_air`, las elimina de la base de datos y las retorna como
/// un vector de [`AlertAir`].
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

pub async fn pop_batch_alert_air(
    executor: impl sqlx::Executor<'_, Database = sqlx::Sqlite>,
) -> Result<Vec<AlertAir>, sqlx::Error> {
    pop_batch_generic(executor, "alert_air").await
}
