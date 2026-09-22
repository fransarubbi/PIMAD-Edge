use crate::context::domain::AppContext;
use crate::second_layer::database::domain::{DataHandle, HubRow, NetworkRow};
use crate::second_layer::message::{
    domain::{LinkageAck, LinkageRequest, Metadata, Network, NetworkAck, Settings},
    logic::MessageHandle,
};
use crate::third_layer::network::{
    domain::NetworkAction::{Delete, Ignore, Insert, Update},
    domain::{Network as NetworkDomain, NetworkAction, NetworkChanged},
};
use chrono::Utc;
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

#[derive(Clone)]
pub struct NetworkHandle {
    tx: mpsc::Sender<InternalNetworkCommand>,
}

impl NetworkHandle {
    pub async fn linkage_request(&self, data: LinkageRequest) {
        let cmd = InternalNetworkCommand::LinkageReq { data };
        let _ = self.tx.send(cmd).await;
    }
    pub async fn settings_from_server(&self, data: Settings) {
        let cmd = InternalNetworkCommand::SettingsServer { data };
        let _ = self.tx.send(cmd).await;
    }
    pub async fn network(&self, data: Network) {
        let cmd = InternalNetworkCommand::Net { data };
        let _ = self.tx.send(cmd).await;
    }
    pub async fn load_in_memory(&self) {
        let cmd = InternalNetworkCommand::LoadInMemory;
        let _ = self.tx.send(cmd).await;
    }
}

enum InternalNetworkCommand {
    LinkageReq { data: LinkageRequest },
    SettingsServer { data: Settings },
    Net { data: Network },
    LoadInMemory,
}

#[derive(PartialEq, Eq)]
pub enum NetworkServiceResponse {
    Run,
}

pub struct NetworkService {
    sender: mpsc::Sender<NetworkServiceResponse>,
    rx: mpsc::Receiver<InternalNetworkCommand>,
    context: AppContext,
    db_handle: DataHandle,
    msg_handle: MessageHandle,
}

impl NetworkService {
    /// Crea una nueva instancia del servicio de red.
    pub fn new(
        sender: mpsc::Sender<NetworkServiceResponse>,
        context: AppContext,
        db_handle: DataHandle,
        msg_handle: MessageHandle,
    ) -> (Self, NetworkHandle) {
        let (tx, rx) = mpsc::channel(50);
        let service = Self {
            sender,
            rx,
            context,
            db_handle,
            msg_handle,
        };
        let handle = NetworkHandle { tx };
        (service, handle)
    }

    pub async fn run(mut self, shutdown: CancellationToken) {
        let (tx, mut rx_response) = mpsc::channel::<NetworkServiceResponse>(10);
        let mut hub_hash_aux: HashMap<String, HashSet<HubRow>> = HashMap::new();

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("Info: shutdown recibido NetworkService");
                    break;
                }

                Some(command) = self.rx.recv() => {
                    match command {
                        InternalNetworkCommand::LinkageReq { data } => {
                            linkage(
                                &tx,
                                &self.context,
                                &self.db_handle,
                                &self.msg_handle,
                                data,
                            ).await;
                        }
                        InternalNetworkCommand::Net { data } => {
                            network(
                                &self.context,
                                &self.db_handle,
                                data,
                                &self.msg_handle
                            ).await;
                        }
                        InternalNetworkCommand::SettingsServer { data } => {
                            settings_from_server(
                                data,
                                &self.msg_handle,
                                &mut hub_hash_aux,
                            ).await;
                        }
                        InternalNetworkCommand::LoadInMemory => {
                            load_memory(
                                &tx,
                                &self.context,
                                &self.db_handle,
                            ).await;
                        }
                    }
                }
                Some(response) = rx_response.recv() => {
                    if self.sender.send(response).await.is_err() {
                        error!("no se pudo enviar NetworkServiceResponse");
                    }
                }
            }
        }
    }
}

async fn network(
    app_context: &AppContext,
    db_handle: &DataHandle,
    network: Network,
    msg_handle: &MessageHandle,
) {
    let action = {
        let manager = app_context.net_man.read().await;
        match (
            manager.networks.get(&network.id_network),
            network.delete_network,
        ) {
            (Some(_), true) => Delete,
            (Some(existing), false) if existing.active != network.active => Update {
                before: existing.active,
                after: network.active,
            },
            (None, false) => Insert,
            _ => Ignore,
        }
    };
    if action == Ignore {
        return;
    }
    let net_chan = handle_action(app_context.clone(), &network, action).await;
    handle_event(
        net_chan,
        db_handle,
        msg_handle,
        app_context,
        network.id_network.clone(),
    )
    .await;
}

async fn handle_action(
    app_context: AppContext,
    network: &Network,
    action: NetworkAction,
) -> NetworkChanged {
    let mut manager = app_context.net_man.write().await;
    let net_chan: NetworkChanged = match action {
        Delete => {
            manager.remove_network(&network.id_network);
            NetworkChanged::Delete {
                id: network.id_network.clone(),
            }
        }
        Update { before, after } => {
            manager.change_active(network.active, &network.id_network);
            let net = NetworkRow {
                id_network: network.id_network.clone(),
                active: network.active,
            };
            NetworkChanged::Update {
                data: net,
                before,
                after,
            }
        }
        Insert => {
            manager.add_network(NetworkDomain::new(
                network.id_network.clone(),
                network.active,
            ));
            let net = NetworkRow {
                id_network: network.id_network.clone(),
                active: network.active,
            };
            NetworkChanged::Insert(net)
        }
        _ => unreachable!(),
    };
    net_chan
}

async fn handle_event(
    net_chan: NetworkChanged,
    db_handle: &DataHandle,
    msg_handle: &MessageHandle,
    app_context: &AppContext,
    id: String,
) {
    match net_chan {
        NetworkChanged::Delete { id } => {
            let res = db_handle.delete_network(id.clone()).await;
            db_handle.delete_all_hubs_by_network(id.clone()).await;
            if !res.result {
                error!("error eliminando la red de la base de datos");
                let metadata = create_metadata(app_context);
                let msg = NetworkAck {
                    metadata,
                    id_network: id,
                    code_of_ack: 201,
                };
                msg_handle.serialize_network_ack(msg).await;
            } else {
                let metadata = create_metadata(app_context);
                let msg = NetworkAck {
                    metadata,
                    id_network: id,
                    code_of_ack: 200,
                };
                msg_handle.serialize_network_ack(msg).await;
            }
        }
        NetworkChanged::Update {
            data,
            before,
            after,
        } => {
            let result = db_handle.save_network(data).await;
            if result {
                if before == false && after == true {
                    let metadata = create_metadata(app_context);
                    let msg = NetworkAck {
                        metadata,
                        id_network: id,
                        code_of_ack: 300,
                    };
                    msg_handle.serialize_network_ack(msg).await;
                } else if before == true && after == false {
                    let metadata = create_metadata(app_context);
                    let msg = NetworkAck {
                        metadata,
                        id_network: id,
                        code_of_ack: 400,
                    };
                    msg_handle.serialize_network_ack(msg).await;
                }
            } else {
                if before == false && after == true {
                    let metadata = create_metadata(app_context);
                    let msg = NetworkAck {
                        metadata,
                        id_network: id,
                        code_of_ack: 301,
                    };
                    msg_handle.serialize_network_ack(msg).await;
                } else if before == true && after == false {
                    let metadata = create_metadata(app_context);
                    let msg = NetworkAck {
                        metadata,
                        id_network: id,
                        code_of_ack: 401,
                    };
                    msg_handle.serialize_network_ack(msg).await;
                }
            }
        }
        NetworkChanged::Insert(network) => {
            let r = db_handle.get_amount_of_networks().await;
            if r == 0 {
                let result = db_handle.save_network(network).await;
                if result {
                    let metadata = create_metadata(app_context);
                    let msg = NetworkAck {
                        metadata,
                        id_network: id,
                        code_of_ack: 100,
                    };
                    msg_handle.serialize_network_ack(msg).await;
                } else {
                    let metadata = create_metadata(app_context);
                    let msg = NetworkAck {
                        metadata,
                        id_network: id,
                        code_of_ack: 101,
                    };
                    msg_handle.serialize_network_ack(msg).await;
                }
            } else {
                let result = db_handle.save_network(network).await;
                if result {
                    let metadata = create_metadata(app_context);
                    let msg = NetworkAck {
                        metadata,
                        id_network: id,
                        code_of_ack: 100,
                    };
                    msg_handle.serialize_network_ack(msg).await;
                } else {
                    let metadata = create_metadata(app_context);
                    let msg = NetworkAck {
                        metadata,
                        id_network: id,
                        code_of_ack: 101,
                    };
                    msg_handle.serialize_network_ack(msg).await;
                }
            }
        }
    }
}

async fn settings_from_server(
    settings: Settings,
    msg_handle: &MessageHandle,
    hub_hash_aux: &mut HashMap<String, HashSet<HubRow>>,
) {
    let id = settings.network.clone();
    hub_hash_aux
        .entry(settings.metadata.sender_user_id.clone())
        .or_default()
        .insert(settings.clone().cast_settings_to_hub_row(id));
    msg_handle.serialize_new_config_hub(settings.clone()).await;
}

async fn linkage(
    tx: &mpsc::Sender<NetworkServiceResponse>,
    app_context: &AppContext,
    db_handle: &DataHandle,
    msg_handle: &MessageHandle,
    request: LinkageRequest,
) {
    let id = request.network.clone();
    let mut manager = app_context.net_man.write().await;
    if !manager.search_hub(&id, &request.metadata.sender_user_id) {
        debug!(
            "Hub con id {} ha solicitado unirse a la red {}",
            request.metadata.sender_user_id, id
        );
        let total = manager.get_total_hubs();
        let hub_row = request.clone().cast_to_hub_row();
        manager.add_hub(id, hub_row.clone().cast_to_hub());
        drop(manager);
        let result = db_handle.save_new_hub(hub_row).await;
        if result {
            if total == 0 {
                if tx.send(NetworkServiceResponse::Run).await.is_err() {
                    error!("no se pudo enviar NetworkServiceResponse::Run desde linkage");
                }
            }
            send_message_linkage_ack(msg_handle, app_context, request).await;
        }
    } else {
        debug!(
            "Hub con id {} ha solicitado unirse a la red {} pero ya está registrado",
            request.metadata.sender_user_id, id
        );
        send_message_linkage_ack(msg_handle, app_context, request).await;
    }
}

async fn send_message_linkage_ack(
    msg_handle: &MessageHandle,
    app_context: &AppContext,
    request: LinkageRequest,
) {
    let timestamp = Utc::now().timestamp();
    let metadata = Metadata {
        sender_user_id: app_context.system.id_edge.clone(),
        destination_id: request.metadata.sender_user_id,
        timestamp: timestamp,
    };
    let msg = LinkageAck {
        metadata,
        linkage_ack: true,
    };
    msg_handle.serialize_linkage_hub(msg).await;
}

async fn load_memory(
    tx: &mpsc::Sender<NetworkServiceResponse>,
    app_context: &AppContext,
    db_handle: &DataHandle,
) {
    let result = db_handle.get_total_networks().await;
    match result.networks {
        Some(net) => {
            let mut manager = app_context.net_man.write().await;
            for networks_row in net {
                let network = NetworkDomain::new(networks_row.id_network, networks_row.active);
                manager.add_network(network);
            }
            let total_networks = manager.networks.len();
            match result.hubs {
                Some(hub) => {
                    let mut manager = app_context.net_man.write().await;
                    for hubs_row in hub {
                        let net_id = hubs_row.network_id.clone();
                        let hub = hubs_row.cast_to_hub();
                        manager.add_hub(net_id, hub);
                    }
                    let total_hubs = manager.get_total_hubs();
                    if total_hubs > 0 && total_networks > 0 {
                        info!(
                            "estado cargado: {} redes en sistema y {} Hubs registrados",
                            total_networks, total_hubs
                        );
                        if tx.send(NetworkServiceResponse::Run).await.is_err() {
                            error!("no se pudo enviar Run")
                        }
                    }
                }
                None => {
                    info!(
                        "estado cargado: {} redes en sistema. Ningún Hub aun",
                        total_networks
                    );
                    return;
                }
            }
        }
        None => info!("no hay redes en sistema, y por lo tanto, tampoco Hubs"),
    }
}

/// Crea una cabecera de metadatos estandarizada para mensajes generados internamente.
///
/// Utiliza el ID del Edge Gateway como origen (`sender_user_id`) y marca el destino
/// como "all" para broadcasts locales.
fn create_metadata(app_context: &AppContext) -> Metadata {
    let timestamp = Utc::now().timestamp();

    let metadata = Metadata {
        sender_user_id: app_context.system.id_edge.clone(),
        destination_id: "all".to_string(),
        timestamp: timestamp,
    };
    metadata
}
