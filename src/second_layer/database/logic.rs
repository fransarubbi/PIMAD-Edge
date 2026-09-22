use super::domain::{AllNetworksResult, HubRow, NetworkResult, NetworkRow, TableDataVector};
use crate::second_layer::database::repository::Repository;
use crate::second_layer::message::domain::HubMessage;
use tracing::{error, info};

pub async fn update_network(repo: &Repository, network: NetworkRow) -> bool {
    let mut result = false;
    match repo.update_network(network.clone()).await {
        Ok(_) => {
            if network.active == true {
                let id = network.id_network;
                info!("red con {id} activada. Modificación exitosa en base de datos");
                result = true;
                return result;
            } else {
                let id = network.id_network;
                info!("red con {id} desactivada. Modificación exitosa en base de datos");
                result = true;
                return result;
            }
        }
        Err(e) => {
            if network.active == true {
                let id = network.id_network;
                error!(
                    "Falló la actualización en la base de datos de la red con {id}. Estado que se quería cargar: activada. {e}"
                );
                return result;
            } else {
                let id = network.id_network;
                error!(
                    "Falló la actualización en la base de datos de la red con {id}. Estado que se quería cargar: desactivada. {e}"
                );
                return result;
            }
        }
    }
}

pub async fn save_hub(repo: &Repository, hub: HubRow) -> bool {
    let mut result = false;
    match repo.get_number_of_networks().await {
        Ok(networks) => {
            info!("hay {networks} redes en la base de datos");
            if networks > 0 {
                match repo.insert_hub(hub.clone()).await {
                    Ok(_) => {
                        let id = hub.id.clone();
                        info!("nuevo Hub con id {id} insertado en la base de datos");
                        result = true;
                    }
                    Err(e) => {
                        let id = hub.id;
                        error!("no se pudo insertar un nuevo Hub con id {id} en base de datos. {e}")
                    }
                }
            } else {
                info!("no se puede insertar el hub, debido a que no hay redes en la base de datos");
            }
        }
        Err(e) => error!("no se pudo obtener el total de redes presentes en el sistema. {e}"),
    }
    return result;
}

pub async fn delete_network(repo: &Repository, id: String) -> NetworkResult {
    let mut res = NetworkResult {
        result: false,
        zero_networks: false,
    };
    match repo.delete_network(&id).await {
        Ok(_) => res.result = true,
        Err(e) => {
            error!("no se pudo eliminar red con id: {id}. {e}");
            res.result = false;
        }
    }
    match repo.get_number_of_networks().await {
        Ok(networks) => {
            if networks == 0 {
                res.zero_networks = true;
            }
        }
        Err(e) => error!("no se pudo obtener el total de redes presentes en el sistema. {e}"),
    }
    return res;
}

pub async fn all_networks(repo: &Repository) -> AllNetworksResult {
    let mut result = AllNetworksResult {
        networks: None,
        hubs: None,
    };
    match repo.get_number_of_networks().await {
        Ok(networks) => {
            if networks > 0 {
                match repo.get_all_network().await {
                    Ok(networks) => result.networks = Some(networks),
                    Err(_) => {}
                }
                match repo.get_number_of_hubs().await {
                    Ok(hubs) => {
                        if hubs > 0 {
                            match repo.get_all_hubs().await {
                                Ok(hubs) => result.hubs = Some(hubs),
                                Err(_) => {}
                            }
                        } else {
                            info!("no hay hubs registrados en la base de datos");
                        }
                    }
                    Err(e) => {
                        error!("no se pudo obtener el total de hubs presentes en el sistema. {e}")
                    }
                }
            } else {
                info!("no hay redes registradas en la base de datos");
            }
        }
        Err(e) => error!("no se pudo obtener el total de redes presentes en el sistema. {e}"),
    }
    return result;
}

/// Clasifica un mensaje de telemetría entrante y lo apila en el vector correspondiente.
///
/// Dependiendo de la variante del enum `HubMessage`, el dato se enruta a la tabla
/// lógica pertinente dentro del buffer `TableDataVector`.
pub fn sort_by_vectors(msg: HubMessage, tdv: &mut TableDataVector) {
    match msg {
        HubMessage::Report(report) => {
            tdv.measurement.push(report);
        }
        HubMessage::Monitor(monitor) => {
            tdv.monitor.push(monitor);
        }
        HubMessage::AlertAir(alert_air) => {
            tdv.alert_air.push(alert_air);
        }
        HubMessage::AlertTem(alert_tem) => {
            tdv.alert_th.push(alert_tem);
        }
        _ => {}
    }
}
