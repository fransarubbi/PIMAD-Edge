//! Repositorio central de acceso a la base de datos.
//!
//! Este módulo implementa el patrón *Repository* para encapsular
//! toda la interacción con SQLite.
//!
//! Actúa como una fachada sobre las distintas tablas del sistema,
//! delegando las operaciones concretas a los módulos especializados
//! de cada entidad (`measurement`, `monitor`, `alert_*`).
//!
//! ## Responsabilidades
//!
//! - Inicializar y configurar la base de datos
//! - Gestionar el pool de conexiones SQLite
//! - Insertar datos de forma polimórfica
//! - Extraer batches de datos pendientes
//! - Proveer consultas de estado (`has_data`)
//! - Administrar la configuración de redes (`Network`)
//!
//! ## Diseño
//!
//! - Seguro para concurrencia (usa `SqlitePool`)
//! - Pensado para ejecución como servicio `systemd`
//! - Soporta reintentos infinitos ante fallos de inicialización
//!
//! ## Límites del módulo
//!
//! - No contiene lógica de negocio
//! - No decide cuándo enviar datos al servidor
//! - No maneja reintentos de red
//!
//! ## Uso
//!
//! Este módulo es utilizado por:
//! - `dba_task`
//! - `dba_insert_task`
//! - `dba_get_task`


use std::time::Duration;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{FromRow, SqlitePool};
use tracing::{debug, error};
use crate::config::sqlite::{LIMIT, WAIT_FOR};
use crate::database::domain::{TableDataVector};
use crate::database::tables::alert_air::{create_table_alert_air, insert_alert_air, pop_batch_alert_air};
use crate::database::tables::alert_temp::{create_table_alert_temp, insert_alert_temp, pop_batch_alert_temp};
use crate::database::tables::balance_epoch::{create_table_balance_epoch, get_balance_epoch, insert_balance_epoch};
use crate::database::tables::hub::{count_hubs, create_table_hub, delete_hub_according_to_id, delete_hub_according_to_network, get_all_hubs, insert_hub_table};
use crate::database::tables::measurement::{create_table_measurement, insert_measurement, pop_batch_measurement};
use crate::database::tables::monitor::{create_table_monitor, insert_monitor, pop_batch_monitor};
use crate::database::tables::network::{count_networks, create_table_network, delete_network_database, get_all_network_data, insert_network_database, upsert_network};
use crate::network::domain::{HubRow, NetworkRow};


/// Repositorio central de acceso a la base de datos.
///
/// # Responsabilidad
///
/// Este struct encapsula el acceso a SQLite y actúa como una **fachada**
/// sobre las distintas tablas del sistema.
///
/// El `Repository`:
/// - inicializa la base de datos,
/// - configura parámetros de rendimiento,
/// - delega operaciones CRUD a los módulos de cada tabla,
/// - provee una API unificada para el resto del sistema.
///
/// # Diseño
///
/// - Contiene un [`SqlitePool`] compartido.
/// - Es seguro para uso concurrente.
/// - No expone detalles SQL a capas superiores.
///
#[derive(Clone, Debug)]
pub struct Repository {
    /// Repositorio de acceso a datos.
    ///
    /// No requiere `Arc` ni `Mutex` externos porque el pool de conexiones de `sqlx`
    /// ya maneja internamente la concurrencia y el conteo de referencias.
    pool: SqlitePool,
}

impl Repository {

    /// Crea una nueva instancia del repositorio.
    ///
    /// # Flujo de inicialización
    ///
    /// 1. Crea el pool de conexiones SQLite.
    /// 2. Configura los parámetros de la base de datos (PRAGMAs).
    /// 3. Inicializa el esquema completo (todas las tablas).
    ///
    /// # Errores
    ///
    /// Retorna [`sqlx::Error`] si falla cualquiera de los pasos anteriores.
    pub async fn new(path: &str) -> Result<Self, sqlx::Error> {
        let pool = create_pool(path).await?;
        configure_db(&pool).await?;
        init_schema(&pool).await?;
        Ok(Self { pool })
    }

    /// Crea el repositorio con reintentos infinitos.
    ///
    /// # Motivación
    ///
    /// Esta función está pensada para servicios `systemd`:
    /// si la base de datos no está disponible al arranque,
    /// el sistema **no falla**, sino que reintenta indefinidamente.
    ///
    /// # Comportamiento
    ///
    /// - Reintenta cada `WAIT_FOR` segundos.
    /// - Loguea errores en cada intento fallido.
    /// - Retorna solo cuando la inicialización es exitosa.
    pub async fn create_repository(path: &str) -> Self {
        loop {
            match Self::new(path).await {
                Ok(repo) => return repo,
                Err(e) => {
                    error!("no se pudo inicializar repo: {:?}", e);
                    tokio::time::sleep(Duration::from_secs(WAIT_FOR)).await;
                }
            }
        }
    }

    /// Inserta datos en la base de datos de forma polimórfica.
    ///
    /// # Descripción
    ///
    /// Recibe un vector de [`TableDataVector`], donde cada variante
    /// representa un lote de datos de una tabla específica.
    ///
    /// La función delega la inserción a la función correspondiente
    /// de cada tabla.
    ///
    /// # Notas de diseño
    ///
    /// - El orden de inserción respeta el orden del vector recibido.
    /// - Cada tabla maneja su propia transacción o inserción atómica.
    /// - Si ocurre un error, la operación se aborta y retorna el error.
    pub async fn insert(&self, tdv: &TableDataVector) -> Result<(), sqlx::Error> {
        debug!("insertando batch en base de datos");
        let mut tx = self.pool.begin().await?;
        
        if !tdv.measurement.is_empty() {
            insert_measurement(&mut *tx, &tdv.measurement).await?;
        }
        if !tdv.monitor.is_empty() {
            insert_monitor(&mut *tx, &tdv.monitor).await?;
        }
        if !tdv.alert_th.is_empty() {
            insert_alert_temp(&mut *tx, &tdv.alert_th).await?;
        }
        if !tdv.alert_air.is_empty() {
            insert_alert_air(&mut *tx, &tdv.alert_air).await?;
        }
        
        tx.commit().await?;
        Ok(())
    }

    /// Extrae y elimina en batch de la base de datos.
    pub async fn pop_batch(&self) -> Result<TableDataVector, sqlx::Error> {
        let mut tx = self.pool.begin().await?;

        let vec_measurement = pop_batch_measurement(&mut *tx).await?;
        let vec_monitor = pop_batch_monitor(&mut *tx).await?;
        let vec_alert_th = pop_batch_alert_temp(&mut *tx).await?;
        let vec_alert_air = pop_batch_alert_air(&mut *tx).await?;

        tx.commit().await?;

        Ok(TableDataVector::new_pop(vec_measurement, vec_alert_air, vec_alert_th, vec_monitor))
    }

    
    // --- Métodos de Gestión de Redes (Network) ---

    /// Inserta una nueva configuración de red en la base de datos.
    ///
    /// Utilizado al recibir nuevas configuraciones desde el Servidor.
    pub async fn insert_network(&self, data: NetworkRow) -> Result<(), sqlx::Error> {
        insert_network_database(&self.pool, data).await?;
        Ok(())
    }

    /// Elimina una red de la base de datos por su ID.
    ///
    /// # Argumentos
    ///
    /// * `id` - El identificador único de la red (ej. "sala7").
    pub async fn delete_network(&self, id: &str) -> Result<(), sqlx::Error> {
        delete_network_database(&self.pool, id).await?;
        Ok(())
    }

    pub async fn update_network(&self, net: NetworkRow) -> Result<(), sqlx::Error> {
        upsert_network(&self.pool, net).await?;
        Ok(())
    }

    /// Obtiene todas las redes configuradas en el sistema.
    ///
    /// # Retorno
    ///
    /// Devuelve un vector de objetos [`Network`] mapeados desde la base de datos.
    /// Se utiliza para poblar la memoria (`NetworkManager`) al iniciar el sistema.
    pub async fn get_all_network(&self) -> Result<Vec<NetworkRow>, sqlx::Error> {
        let rows = get_all_network_data(&self.pool).await?;
        Ok(rows)
    }

    /// Obtiene el número total de redes del sistema.
    pub async fn get_number_of_networks(&self) -> Result<i64, sqlx::Error> {
        let rows = count_networks(&self.pool).await?;
        Ok(rows)
    }

    /// Obtiene el número total de hubs del sistema.
    pub async fn get_number_of_hubs(&self) -> Result<i64, sqlx::Error> {
        let rows = count_hubs(&self.pool).await?;
        Ok(rows)
    }

    
    // --- Métodos de Gestión de Balance Epoch ---
    
    /// Inserta un nuevo valor de época en la base de datos.
    ///
    /// Utilizado al entrar en un nuevo Balance Mode.
    pub async fn update_epoch(&self, epoch: u32) -> Result<(), sqlx::Error> {
        insert_balance_epoch(&self.pool, epoch).await?;
        Ok(())
    }
    
    /// Obtiene el último valor de época del sistema.
    ///
    /// # Retorno
    ///
    /// Devuelve un valor u32.
    pub async fn get_epoch(&self) -> Result<u32, sqlx::Error> {
        let row = get_balance_epoch(&self.pool).await?;
        Ok(row)
    }


    // --- Métodos de Gestión de Hubs ---

    /// Inserta un nuevo Hub en la base de datos con su configuración incluida.
    pub async fn insert_hub(&self, data: HubRow) -> Result<(), sqlx::Error> {
        insert_hub_table(&self.pool, data).await?;
        Ok(())
    }

    /// Elimina todos los Hubs de la base de datos según id de la red.
    /// Usada cuando se elimina la red y se deben eliminar los nodos asociados.
    pub async fn delete_hub_network(&self, id: &str) -> Result<(), sqlx::Error> {
        delete_hub_according_to_network(&self.pool, id).await?;
        Ok(())
    }

    /// Elimina un Hub de la base de datos según id del Hub.
    /// Usada cuando se desea eliminar un nodo de una red, por ejemplo por estar dañado.
    pub async fn delete_hub(&self, id: &str) -> Result<(), sqlx::Error> {
        delete_hub_according_to_id(&self.pool, id).await?;
        Ok(())
    }

    /// Carga en memoria todos los Hubs registrados en la base de datos.
    pub async fn get_all_hubs(&self) -> Result<Vec<HubRow>, sqlx::Error> {
        let rows = get_all_hubs(&self.pool).await?;
        Ok(rows)
    }
}


/// Crea el pool de conexiones SQLite.
///
/// # Notas
///
/// - `max_connections` limita la concurrencia.
/// - SQLite maneja internamente el locking.
async fn create_pool(db_path: &str) -> Result<SqlitePool, sqlx::Error> {
    let database_url = format!("sqlite://{}", db_path);

    let pool = SqlitePoolOptions::new()
        .max_connections(3)
        .connect(&database_url)
        .await?;

    Ok(pool)
}


/// Configura parámetros de rendimiento y concurrencia de SQLite.
///
/// # PRAGMAs utilizados
///
/// - `journal_mode = WAL`: mejora concurrencia entre lecturas y escrituras.
/// - `synchronous = NORMAL`: balance entre seguridad y rendimiento.
/// - `busy_timeout`: espera antes de fallar por bloqueo (5000 ms).
async fn configure_db(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query("PRAGMA journal_mode = WAL;").execute(pool).await?;
    sqlx::query("PRAGMA synchronous = NORMAL;").execute(pool).await?;
    sqlx::query("PRAGMA busy_timeout = 5000;").execute(pool).await?;
    Ok(())
}


/// Inicializa el esquema completo de la base de datos.
///
/// # Descripción
///
/// Crea todas las tablas necesarias para el funcionamiento del sistema.
/// Es segura de ejecutar múltiples veces (`CREATE TABLE IF NOT EXISTS`).
async fn init_schema(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    create_table_measurement(pool).await?;
    create_table_monitor(pool).await?;
    create_table_alert_temp(pool).await?;
    create_table_alert_air(pool).await?;
    create_table_network(pool).await?;
    create_table_balance_epoch(pool).await?;
    create_table_hub(pool).await?;
    Ok(())
}


/// Función genérica para extraer y eliminar batches de una tabla.
///
/// # Descripción
///
/// Ejecuta un `DELETE ... RETURNING *` sobre la tabla indicada,
/// devolviendo los registros eliminados como un vector del tipo `T`.
///
pub async fn pop_batch_generic<T>(
    executor: impl sqlx::Executor<'_, Database = sqlx::Sqlite>,
    table: &str,
) -> Result<Vec<T>, sqlx::Error>
where
    T: for<'r> FromRow<'r, sqlx::sqlite::SqliteRow>
    + Send
    + Unpin
    + 'static,
{
    let sql = format!(
        r#"
        DELETE FROM "{table}"
        WHERE id IN (
            SELECT id FROM "{table}"
            ORDER BY id ASC
            LIMIT ?
        )
        RETURNING *
        "#
    );

    let result = sqlx::query_as::<_, T>(&sql)
        .bind(LIMIT)
        .fetch_all(executor)
        .await?;

    Ok(result)
}