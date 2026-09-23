//! # Módulo de Recolección de Métricas (Monitor)
//!
//! Este módulo se encarga de interactuar con el hardware y el sistema operativo
//! para extraer métricas en tiempo real. Está optimizado para sistemas Linux
//! (específicamente Raspberry Pi) mediante la lectura directa de archivos del kernel
//!
//! ## Características
//! - Persistencia de conexiones con `sysinfo` para optimizar rendimiento.
//! - Lectura manual de temperatura térmica (Thermal Zone 0).
//! - Parsing manual de calidad de señal WiFi (RSSI/dBm).

use crate::context::domain::AppContext;
use crate::second_layer::message::{
    domain::{Metadata, SystemMetrics},
    logic::MessageHandle,
};
use crate::system::domain::InternalEvent;
use crate::third_layer::metrics::logic::{MetricsTimerEvent, metrics_timer, system_metrics};
use std::{fs, process::Command, time::Instant};
use sysinfo::{Disks, Networks, System};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

#[derive(Clone)]
pub struct MetricsHandle {
    tx: mpsc::Sender<InternalMetricsCommand>,
}

impl MetricsHandle {
    pub async fn connection(&self, data: InternalEvent) {
        let cmd = InternalMetricsCommand::Connection { data };
        let _ = self.tx.send(cmd).await;
    }
}

enum InternalMetricsCommand {
    Connection { data: InternalEvent },
}

pub struct MetricsService {
    rx: mpsc::Receiver<InternalMetricsCommand>,
    handler: MessageHandle,
    context: AppContext,
}

impl MetricsService {
    pub fn new(handler: MessageHandle, context: AppContext) -> (Self, MetricsHandle) {
        let (tx, rx) = mpsc::channel(10);
        let service = Self {
            rx,
            handler,
            context,
        };
        let handle = MetricsHandle { tx };
        (service, handle)
    }

    pub async fn run(mut self, shutdown: CancellationToken) {
        let (tx_to_timer, rx_from_metrics) = mpsc::channel::<MetricsTimerEvent>(50);
        let (tx_to_metrics, rx_from_timer) = mpsc::channel::<MetricsTimerEvent>(50);
        let (tx_conn, rx_conn) = mpsc::channel::<InternalEvent>(10);

        tokio::spawn(system_metrics(
            rx_conn,
            self.handler.clone(),
            tx_to_timer,
            rx_from_timer,
            self.context.clone(),
            shutdown.clone(),
        ));

        tokio::spawn(metrics_timer(
            tx_to_metrics,
            rx_from_metrics,
            shutdown.clone(),
        ));

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("shutdown recibido Core");
                    break;
                }
                Some(cmd) = self.rx.recv() => {
                    match cmd {
                        InternalMetricsCommand::Connection { data } => {
                            if tx_conn.send(data).await.is_err() {
                                error!("no se pudo enviar InternalEvent a system_metrics");
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Recolector de estado del sistema.
///
/// Mantiene las instancias de las estructuras de `sysinfo` para evitar
/// la realocación de memoria en cada ciclo de lectura. Es necesario instanciar
/// esta estructura una sola vez y mantenerla viva durante la ejecución del programa.
pub struct MetricsCollector {
    system: System,
    disks: Disks,
    networks: Networks,
    last_rx_bytes: u64,
    last_tx_bytes: u64,
    start_time: Instant,
}

/// Representación interna de la señal inalámbrica.
pub struct WifiSignal {
    pub rssi: i32,
    pub dbm: i32,
}

impl MetricsCollector {
    /// Crea una nueva instancia del recolector.
    ///
    /// Inicializa y escanea todos los componentes de hardware disponibles.
    /// Esta operación puede tardar unos milisegundos.
    pub fn new() -> Self {
        let mut system = System::new_all();
        system.refresh_all();

        let disks = Disks::new_with_refreshed_list();
        let networks = Networks::new_with_refreshed_list();

        // Inicializar contadores con valores actuales
        let mut last_rx_bytes = 0;
        let mut last_tx_bytes = 0;

        for (_, data) in networks.iter() {
            last_rx_bytes += data.received();
            last_tx_bytes += data.transmitted();
        }

        Self {
            system,
            disks,
            networks,
            last_rx_bytes,
            last_tx_bytes,
            start_time: Instant::now(),
        }
    }

    pub fn prep_cpu_refresh(&mut self) {
        self.system.refresh_cpu_usage();
    }

    /// Actualiza y retorna las métricas actuales del sistema.
    ///
    /// Este método refresca la información de hardware de forma incremental
    /// (solo actualiza valores, no re-escanea listas de dispositivos) y calcula
    /// promedios cuando es necesario.
    ///
    /// # Retorno
    /// Retorna un `SystemMetrics` (DTO) listo para ser enviado o almacenado.
    pub fn collect(&mut self, metadata: Metadata) -> SystemMetrics {
        self.system.refresh_cpu_usage();
        self.system.refresh_memory();
        self.disks.refresh(false);
        self.networks.refresh(false);

        // Cálculo de CPU (promedio de todos los núcleos)
        let cpu_usage_percent = {
            let cpus = self.system.cpus();
            if cpus.is_empty() {
                0.0
            } else {
                cpus.iter().map(|c| c.cpu_usage()).sum::<f32>() / cpus.len() as f32
            }
        };

        // Temperatura (lectura directa de archivo para mayor precisión en RPi)
        let cpu_temp_celsius = read_cpu_temperature().unwrap_or(0.0);

        let ram_total_mb = self.system.total_memory() / (1024 * 1024);
        let ram_used_mb = self.system.used_memory() / (1024 * 1024);

        // Almacenamiento (buscando partición raíz /)
        let mut sd_total_gb = 0;
        let mut sd_used_gb = 0;
        let mut sd_usage_percent = 0.0;

        for disk in self.disks.iter() {
            if disk.mount_point() == std::path::Path::new("/") {
                let total = disk.total_space();
                let used = total - disk.available_space();

                sd_total_gb = total / 1024 / 1024 / 1024;
                sd_used_gb = used / 1024 / 1024 / 1024;
                sd_usage_percent = (used as f32 / total as f32) * 100.0;
                break;
            }
        }

        // Red (acumulado actual)
        let mut current_rx_bytes = 0;
        let mut current_tx_bytes = 0;

        for (_, data) in self.networks.iter() {
            current_rx_bytes += data.received();
            current_tx_bytes += data.transmitted();
        }

        // Calcular delta desde última medición
        let network_rx_bytes = current_rx_bytes.saturating_sub(self.last_rx_bytes);
        let network_tx_bytes = current_tx_bytes.saturating_sub(self.last_tx_bytes);

        // Actualizar valores para próxima medición
        self.last_rx_bytes = current_rx_bytes;
        self.last_tx_bytes = current_tx_bytes;

        // WiFi (específico para interfaz wlan0)
        let wifi = read_wifi_signal("wlan0");
        let wifi_rssi = wifi.as_ref().map(|w| w.rssi);
        let wifi_signal_dbm = wifi.as_ref().map(|w| w.dbm);

        let ram_used_by_service_mb = match read_service_memory_mb() {
            Some(ram) => ram,
            _ => 0,
        };

        let uptime_seconds = self.start_time.elapsed().as_secs();

        SystemMetrics {
            metadata,
            uptime_seconds,
            cpu_usage_percent,
            cpu_temp_celsius,
            ram_total_mb,
            ram_used_mb,
            ram_used_by_service_mb,
            sd_total_gb,
            sd_used_gb,
            sd_usage_percent,
            network_rx_bytes,
            network_tx_bytes,
            wifi_rssi,
            wifi_signal_dbm,
        }
    }
}

/// Lee la temperatura de la CPU directamente desde el sistema de archivos virtual.
///
/// Intenta leer `/sys/class/thermal/thermal_zone0/temp`.
///
/// # Retorno
/// * `Some(f32)`: Temperatura en grados Celsius.
/// * `None`: Si el archivo no existe o no se puede parsear.
fn read_cpu_temperature() -> Option<f32> {
    let raw = fs::read_to_string("/sys/class/thermal/thermal_zone0/temp").ok()?;
    let millideg: f32 = raw.trim().parse().ok()?;
    Some(millideg / 1000.0)
}

fn read_wifi_signal(interface: &str) -> Option<WifiSignal> {
    // Intentar primero con /proc (más confiable)
    if let Some(signal) = read_wifi_from_proc(interface) {
        return Some(signal);
    }

    // Fallback a iw si /proc falla
    read_wifi_from_iw(interface)
}

fn read_wifi_from_proc(interface: &str) -> Option<WifiSignal> {
    let content = fs::read_to_string("/proc/net/wireless").ok()?;

    for line in content.lines().skip(2) {
        let trimmed = line.trim_start();
        if trimmed.starts_with(interface) {
            let parts: Vec<&str> = trimmed.split_whitespace().collect();

            if parts.len() < 4 {
                return None;
            }

            let rssi = parts[2].trim_end_matches('.').parse().ok()?;
            let dbm = parts[3].trim_end_matches('.').parse().ok()?;

            return Some(WifiSignal { rssi, dbm });
        }
    }
    None
}

fn read_wifi_from_iw(interface: &str) -> Option<WifiSignal> {
    let output = Command::new("iw")
        .args(&["dev", interface, "link"])
        .output()
        .ok()?;

    let content = String::from_utf8_lossy(&output.stdout);

    for line in content.lines() {
        if line.contains("signal:") {
            let dbm = line.split_whitespace().nth(1)?.parse().ok()?;
            let rssi = calculate_rssi_from_dbm(dbm);
            return Some(WifiSignal { rssi, dbm });
        }
    }
    None
}

/// Convierte dBm a escala RSSI 0-100
/// -90 dBm o menos = 0% (sin señal)
/// -30 dBm o más = 100% (excelente)
fn calculate_rssi_from_dbm(dbm: i32) -> i32 {
    if dbm <= -90 {
        0
    } else if dbm >= -30 {
        100
    } else {
        // Escala lineal de -90 a -30 → 0 a 100
        ((dbm + 90) * 100 / 60).max(0).min(100)
    }
}

/// Lee la memoria RAM real (Resident Set Size) consumida por este proceso.
///
/// Intenta leer `/proc/self/status` y buscar la línea VmRSS.
///
/// # Retorno
/// * `Some(u64)`: Memoria en Megabytes (MB).
/// * `None`: Si falla la lectura o el parseo.
fn read_service_memory_mb() -> Option<u64> {
    let content = std::fs::read_to_string("/proc/self/status").ok()?;

    for line in content.lines() {
        if line.starts_with("VmRSS:") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            // parts[0] = "VmRSS:", parts[1] = valor, parts[2] = "kB"
            if parts.len() >= 2 {
                let kb: u64 = parts[1].parse().ok()?;
                return Some((kb * 1024) / 1000000); // Convertir de KiB a MB
            }
        }
    }
    None
}
