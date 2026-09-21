use crate::config::sqlite::{BATCH_SIZE, FLUSH_INTERVAL};
use crate::database::{
    logic::{all_networks, delete_network, save_hub, sort_by_vectors, update_network},
    repository::Repository,
};
use crate::message::domain::{AlertAir, AlertTh, HubMessage, Measurement, Monitor};
use crate::system::domain::InternalEvent;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use tokio::{
    sync::{mpsc, oneshot},
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

pub struct NetworkResult {
    pub result: bool,
    pub zero_networks: bool,
}

pub struct AllNetworksResult {
    pub networks: Option<Vec<NetworkRow>>,
    pub hubs: Option<Vec<HubRow>>,
}

#[derive(Debug, FromRow, Deserialize, PartialEq, Clone)]
pub struct NetworkRow {
    pub id_network: String,
    pub active: bool,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, FromRow, Hash)]
pub struct HubRow {
    pub id: String,
    pub device_name: String,
    pub network_id: String,
}

enum InternalDataCommand {
    SaveDataFromHub {
        data: HubMessage,
    },
    StatusConnectionServer {
        data: InternalEvent,
        respond_to: oneshot::Sender<bool>,
    },
    SaveEpoch {
        epoch: u32,
        respond_to: oneshot::Sender<bool>,
    },
    GetEpoch {
        respond_to: oneshot::Sender<Option<u32>>,
    },
    SaveNewHub {
        data: HubRow,
        respond_to: oneshot::Sender<bool>,
    },
    DeleteHub {
        id: String,
        respond_to: oneshot::Sender<bool>,
    },
    SaveNetwork {
        data: NetworkRow,
        respond_to: oneshot::Sender<bool>,
    },
    DeleteNetwork {
        id: String,
        respond_to: oneshot::Sender<NetworkResult>,
    },
    UpdateNetwork {
        data: NetworkRow,
        respond_to: oneshot::Sender<bool>,
    },
    DeleteAllHubsByNetwork {
        id: String,
        respond_to: oneshot::Sender<bool>,
    },
    GetTotalNetworks {
        respond_to: oneshot::Sender<AllNetworksResult>,
    },
}

#[derive(Clone)]
pub struct DataHandle {
    tx: mpsc::Sender<InternalDataCommand>,
}

impl DataHandle {
    pub async fn save_data_from_hub(&self, data: HubMessage) {
        let cmd = InternalDataCommand::SaveDataFromHub { data };
        let _ = self.tx.send(cmd).await;
    }
    pub async fn send_status_connection_server(&self, data: InternalEvent) -> bool {
        let (response_tx, response_rx) = oneshot::channel();
        let cmd = InternalDataCommand::StatusConnectionServer {
            data,
            respond_to: response_tx,
        };
        if self.tx.send(cmd).await.is_err() {
            return false;
        }
        match response_rx.await {
            Ok(_) => true,
            Err(_) => false,
        }
    }
    pub async fn save_epoch(&self, epoch: u32) -> bool {
        let (response_tx, response_rx) = oneshot::channel();
        let cmd = InternalDataCommand::SaveEpoch {
            epoch,
            respond_to: response_tx,
        };
        if self.tx.send(cmd).await.is_err() {
            return false;
        }
        match response_rx.await {
            Ok(_) => true,
            Err(_) => false,
        }
    }
    pub async fn get_epoch(&self) -> Option<u32> {
        let (response_tx, response_rx) = oneshot::channel();
        let cmd = InternalDataCommand::GetEpoch {
            respond_to: response_tx,
        };
        if self.tx.send(cmd).await.is_err() {
            return None;
        }
        match response_rx.await {
            Ok(epoch) => epoch,
            Err(_) => None,
        }
    }
    pub async fn save_new_hub(&self, data: HubRow) -> bool {
        let (response_tx, response_rx) = oneshot::channel();
        let cmd = InternalDataCommand::SaveNewHub {
            data,
            respond_to: response_tx,
        };
        if self.tx.send(cmd).await.is_err() {
            return false;
        }
        match response_rx.await {
            Ok(_) => true,
            Err(_) => false,
        }
    }
    pub async fn delete_hub(&self, id: String) -> bool {
        let (response_tx, response_rx) = oneshot::channel();
        let cmd = InternalDataCommand::DeleteHub {
            id,
            respond_to: response_tx,
        };
        if self.tx.send(cmd).await.is_err() {
            return false;
        }
        match response_rx.await {
            Ok(_) => true,
            Err(_) => false,
        }
    }
    pub async fn save_network(&self, data: NetworkRow) -> bool {
        let (response_tx, response_rx) = oneshot::channel();
        let cmd = InternalDataCommand::SaveNetwork {
            data,
            respond_to: response_tx,
        };
        if self.tx.send(cmd).await.is_err() {
            return false;
        }
        match response_rx.await {
            Ok(_) => true,
            Err(_) => false,
        }
    }
    pub async fn delete_network(&self, id: String) -> NetworkResult {
        let (response_tx, response_rx) = oneshot::channel();
        let cmd = InternalDataCommand::DeleteNetwork {
            id,
            respond_to: response_tx,
        };
        if self.tx.send(cmd).await.is_err() {
            let res = NetworkResult {
                result: false,
                zero_networks: false,
            };
            return res;
        }
        match response_rx.await {
            Ok(res) => res,
            Err(_) => {
                let res = NetworkResult {
                    result: false,
                    zero_networks: false,
                };
                return res;
            }
        }
    }
    pub async fn update_network(&self, data: NetworkRow) -> bool {
        let (response_tx, response_rx) = oneshot::channel();
        let cmd = InternalDataCommand::UpdateNetwork {
            data,
            respond_to: response_tx,
        };
        if self.tx.send(cmd).await.is_err() {
            return false;
        }
        match response_rx.await {
            Ok(_) => true,
            Err(_) => false,
        }
    }
    pub async fn delete_all_hubs_by_network(&self, id: String) -> bool {
        let (response_tx, response_rx) = oneshot::channel();
        let cmd = InternalDataCommand::DeleteAllHubsByNetwork {
            id,
            respond_to: response_tx,
        };
        if self.tx.send(cmd).await.is_err() {
            return false;
        }
        match response_rx.await {
            Ok(_) => true,
            Err(_) => false,
        }
    }
    pub async fn get_total_networks(&self) -> AllNetworksResult {
        let (response_tx, response_rx) = oneshot::channel();
        let cmd = InternalDataCommand::GetTotalNetworks {
            respond_to: response_tx,
        };
        if self.tx.send(cmd).await.is_err() {
            let res = AllNetworksResult {
                networks: None,
                hubs: None,
            };
            return res;
        }
        match response_rx.await {
            Ok(data) => data,
            Err(_) => {
                let res = AllNetworksResult {
                    networks: None,
                    hubs: None,
                };
                return res;
            }
        }
    }
}

pub struct DataService {
    tx: mpsc::Sender<TableDataVector>, // canal para enviar batches extraídos
    rx: mpsc::Receiver<InternalDataCommand>,
    repo: Repository,
}

impl DataService {
    pub fn new(
        tx: mpsc::Sender<TableDataVector>,
        rx: mpsc::Receiver<InternalDataCommand>,
        repo: Repository,
    ) -> Self {
        Self { tx, rx, repo }
    }

    pub async fn run(mut self, shutdown: CancellationToken) {
        // Temporizador de Batching de escritura (flush a la DB)
        let mut flush_timer = tokio::time::interval(FLUSH_INTERVAL);
        flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Temporizador de extracción periódica (1 minuto)
        let mut extract_timer = tokio::time::interval(Duration::from_secs(60));
        extract_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut tdv = TableDataVector::new();

        // Variable de estado para controlar la extracción
        let mut is_server_connected = false;
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("shutdown recibido DataService");
                    break;
                }

                // Tick del timer de guardado (FLUSH)
                _ = flush_timer.tick() => {
                    if !tdv.is_empty() {
                        self.flush_to_db(&mut tdv).await;
                    }
                }

                // Tick del timer de extracción (cada 1 min)
                _ = extract_timer.tick() => {
                    if is_server_connected {
                        self.extract_and_send_batches().await;
                    }
                }

                Some(cmd) = self.rx.recv() => {
                    match cmd {
                        InternalDataCommand::SaveDataFromHub { data } => {
                            self.buffer_message(data, &mut tdv).await;
                        }
                        InternalDataCommand::StatusConnectionServer { data, respond_to } => {
                            match data {
                                InternalEvent::ServerConnected => {
                                    info!("servidor conectado. Habilitando extracción.");
                                    is_server_connected = true;
                                }
                                InternalEvent::ServerDisconnected => {
                                    info!("Servidor desconectado. Pausando extracción.");
                                    is_server_connected = false;
                                }
                                _ => {}
                            }
                            let _ = respond_to.send(true);
                        }
                        InternalDataCommand::SaveEpoch { epoch, respond_to } => {
                            let res = match self.repo.update_epoch(epoch).await {
                                Ok(_) => true,
                                Err(_) => false,
                            };
                            let _ = respond_to.send(res);
                        }
                        InternalDataCommand::GetEpoch { respond_to } => {
                            let result = match self.repo.get_epoch().await {
                                Ok(epoch) => Some(epoch),
                                Err(_) => None,
                            };
                            let _ = respond_to.send(result);
                        }
                        InternalDataCommand::SaveNewHub { data, respond_to } => {
                            let result = save_hub(&self.repo, data).await;
                            let _ = respond_to.send(result);
                        }
                        InternalDataCommand::DeleteHub { id, respond_to } => {
                            let result = match self.repo.delete_hub(&id).await {
                                Ok(_) => true,
                                Err(_) => false,
                            };
                            let _ = respond_to.send(result);
                        }
                        InternalDataCommand::SaveNetwork { data, respond_to } => {
                            let result = match self.repo.insert_network(data.clone()).await {
                                Ok(_) => true,
                                Err(_) => false,
                            };
                            let _ = respond_to.send(result);
                        }
                        InternalDataCommand::DeleteNetwork { id, respond_to } => {
                            let res = delete_network(&self.repo, id).await;
                            let _ = respond_to.send(res);
                        }
                        InternalDataCommand::UpdateNetwork { data, respond_to } => {
                            let result = update_network(&self.repo, data).await;
                            let _ = respond_to.send(result);
                        }
                        InternalDataCommand::DeleteAllHubsByNetwork { id, respond_to } => {
                            let result = match self.repo.delete_hub_network(&id).await {
                                Ok(_) => true,
                                Err(_) => false,
                            };
                            let _ = respond_to.send(result);
                        }
                        InternalDataCommand::GetTotalNetworks { respond_to } => {
                            let result = all_networks(&self.repo).await;
                            let _ = respond_to.send(result);
                        }
                    }
                }
            }
        }
    }

    async fn buffer_message(&self, msg: HubMessage, tdv: &mut TableDataVector) {
        sort_by_vectors(msg, tdv);
        if tdv.is_some_vector_full() {
            self.flush_to_db(tdv).await;
        }
    }

    async fn flush_to_db(&self, tdv: &mut TableDataVector) {
        let _ = self.repo.insert(tdv).await;
        tdv.clear();
    }

    async fn insert_hub_directly(&self, hub: HubRow) {
        let _ = self.repo.insert_hub(hub).await;
    }

    /// Hace pop_batch() hasta vaciar la DB y los envía por el canal tx
    async fn extract_and_send_batches(&self) {
        loop {
            match self.repo.pop_batch().await {
                Ok(batch) => {
                    if batch.is_empty() {
                        // DB vacía o ya no hay batches pendientes
                        break;
                    }

                    if self.tx.send(batch).await.is_err() {
                        error!("canal cerrado, no se pudo enviar pop batch al Core");
                        break;
                    }

                    // descanso de 200ms para no asfixiar el canal ni a gRPC
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Err(e) => {
                    error!("no se pudo hacer pop batch desde la base de datos. {}", e);
                    break;
                }
            }
        }
    }
}

/// Estructura que agrupa un lote de datos de un tipo específico junto con su metadato de origen.
/// Se utiliza para mover batches desde la DB hacia el sistema de mensajería.
#[derive(Clone, Debug, Default)]
pub struct TableDataVector {
    pub measurement: Vec<Measurement>,
    pub alert_air: Vec<AlertAir>,
    pub alert_th: Vec<AlertTh>,
    pub monitor: Vec<Monitor>,
}

impl TableDataVector {
    /// Crea un nuevo contenedor con la capacidad pre-reservada.
    ///
    /// Inicializa los vectores internos utilizando `Vec::with_capacity(BATCH_SIZE)`.
    /// Esto evita realocaciones dinámicas de memoria mientras se llena el buffer,
    /// mejorando el rendimiento de inserción.
    pub fn new() -> Self {
        Self {
            measurement: Vec::with_capacity(BATCH_SIZE),
            alert_air: Vec::with_capacity(BATCH_SIZE),
            alert_th: Vec::with_capacity(BATCH_SIZE),
            monitor: Vec::with_capacity(BATCH_SIZE),
        }
    }

    /// Constructor con parámetros para hacer pop batch.
    pub fn new_pop(
        measurement: Vec<Measurement>,
        alert_air: Vec<AlertAir>,
        alert_th: Vec<AlertTh>,
        monitor: Vec<Monitor>,
    ) -> Self {
        Self {
            measurement,
            alert_air,
            alert_th,
            monitor,
        }
    }

    /// Retorna true si todos los vectores están vacíos.
    pub fn is_empty(&self) -> bool {
        self.measurement.is_empty()
            && self.alert_air.is_empty()
            && self.alert_th.is_empty()
            && self.monitor.is_empty()
    }

    /// Verifica si alguno de los buffers internos ha alcanzado su capacidad máxima.
    ///
    /// Este método se utiliza como disparador (Trigger) para realizar el volcado (flush)
    /// a la base de datos.
    ///
    /// # Retorno
    /// * `true`: Al menos uno de los vectores tiene longitud igual a `BATCH_SIZE`.
    /// * `false`: Todos los vectores tienen espacio disponible.
    pub fn is_some_vector_full(&self) -> bool {
        self.is_measurement_full()
            || self.is_alert_air_full()
            || self.is_alert_th_full()
            || self.is_monitor_full()
    }

    fn is_measurement_full(&self) -> bool {
        self.measurement.len() == BATCH_SIZE
    }
    fn is_monitor_full(&self) -> bool {
        self.monitor.len() == BATCH_SIZE
    }
    fn is_alert_air_full(&self) -> bool {
        self.alert_air.len() == BATCH_SIZE
    }
    fn is_alert_th_full(&self) -> bool {
        self.alert_th.len() == BATCH_SIZE
    }

    /// Reinicia los buffers sin liberar la memoria asignada.
    ///
    /// Establece la longitud de todos los vectores a 0, pero mantiene la capacidad
    /// reservada en el Heap. Esto permite reutilizar la estructura en el siguiente
    /// ciclo de acumulación sin costo de asignación de memoria.
    pub fn clear(&mut self) {
        self.measurement.clear();
        self.alert_air.clear();
        self.alert_th.clear();
        self.monitor.clear();
    }
}
