<<<<<<< HEAD
# PIMAD-Edge
=======
# PIMAD Edge
>>>>>>> master

An advanced Edge gateway for IoT networks written in Rust. It serves as the critical bridge connecting local physical devices (Hubs) to the cloud (Central Server), enabling robust telemetry collection, distributed control, and localized decision-making.

PIMAD-Edge is designed with high availability, fault tolerance, and dynamic topology management in mind, specifically tailored for the **Post-Failure Control and Balancing Protocol (PCBP)**.

## Key Features

- **Store-and-Forward Strategy:** Guarantees data delivery. Telemetry and alerts are stored locally (SQLite) when cloud connectivity is lost and are securely forwarded in batches once the connection is restored.
- **Dynamic Topology Management:** Supports real-time insertion, updates, and deletion of subnets and Hubs. The Edge gateway acts as the local source of truth, synchronizing the physical layer with the cloud.
- **PCBP Integration (Quorum & Balancing):** Implements the Post-Failure Control and Balancing Protocol. It handles leader election, quorum verification, and adaptive transmission frequencies (Normal Mode vs. Safe Mode) when the network topology changes or failures occur.
- **Over-The-Air (OTA) Updates:** Orchestrates safe firmware updates not just for the connected Hubs, but also features a self-updating mechanism to pull the latest Edge releases from GitHub.
- **Protocol Translation:** Seamlessly routes and translates between local MQTT (using MessagePack) and cloud gRPC (using Protocol Buffers).

## Overall Architecture

The project has been recently refactored into a **Clean Three-Layer Architecture**, built heavily around the **Actor Model** using asynchronous Tokio tasks and MPSC channels. This decouples responsibilities and ensures non-blocking I/O.

### 1. First Layer (Ports & Adapters)
Located in `src/first_layer/`, this layer handles all external I/O boundaries.
* **`grpc`**: Manages the persistent, bidirectional connection with the Central Server using Tonic. It translates protobuf definitions into internal domain events.
* **`mqtt`**: Manages the local messaging broker (e.g., Mosquitto) using `rumqttc`. It handles topic subscriptions, QoS, and binary payloads.
* **`channels`**: Defines the cross-boundary channels used to communicate with the upper layers.

### 2. Second Layer (Routing & Persistence)
Located in `src/second_layer/`, this layer acts as the data broker and router.
* **`database`**: Implements the Repository pattern over SQLite using `sqlx`. It handles batch insertions, data querying for the store-and-forward mechanism, and schema migrations.
* **`message`**: The core translator. It deserializes incoming MessagePack payloads from Hubs and serializes outbound commands.
* **`second_layer_middleware`**: A central router/mediator that inspects incoming messages and routes them either to the database (for storage) or up to the Third Layer (for business logic).

### 3. Third Layer (Business Logic & Services)
Located in `src/third_layer/`, this layer houses all the autonomous domain services.
* **`fsm` (Finite State Machine):** The global brain of the Edge. Dictates the operational mode (Normal, BalanceMode, SafeMode) based on quorum and connectivity.
* **`network`**: Maintains the in-memory cache of the network topology (Subnets and Hubs). Calculates routing paths and MQTT topics dynamically.
* **`firmware`**: Orchestrates OTA updates.
* **`quorum`**: Implements the PCBP consensus logic.
* **`heartbeat`**: Continuously monitors the health of the connection to the Central Server and triggers failover mechanisms.
* **`metrics`**: Gathers Edge hardware telemetry (CPU, RAM, Temperatures) using `sysinfo` and reports it to the cloud.
* **`third_layer_middleware`**: The orchestrator for this layer, dispatching commands between the FSM, Network, Firmware, and other services.

## Core Technologies

- **Language:** Rust (async/await)
- **Runtime:** Tokio
- **Cloud RPC:** Tonic (gRPC) & Prost (Protocol Buffers)
- **Local Messaging:** Rumqttc (MQTT)
- **Persistence:** SQLx (SQLite)
- **Serialization:** Serde, Rmp-serde (MessagePack)
- **Observability:** Tracing, Tracing-subscriber

<<<<<<< HEAD
## Concurrency & Actor Model

PIMAD-Edge avoids shared mutable state by relying on the Actor Model. Each service (e.g., `NetworkService`, `FsmService`, `DatabaseService`) runs inside its own isolated `tokio::spawn` task.
Services interact strictly by passing messages through `tokio::sync::mpsc` channels, encapsulated behind lightweight `*Handle` structs. Graceful shutdowns are coordinated globally using `tokio_util::sync::CancellationToken`.
=======
### Network (src/network/domain.rs, src/network/logic.rs)

- Maintains topology: `NetworkService` (actor), `NetworkManager` (cache), `NetworkRow` and `HubRow` models for persistence.
- Synchronizes three sources of truth: server (cloud), physical hubs, and local database.

`network_admin` makes business decisions, updates the cache, and generates actions.
`network_dba` translates actions into transactional commands for the database.

Caching Strategy:

- `NetworkManager` maintains HashMap in memory with active networks and hubs.
- Pre-calculates MQTT topics.

### Database / Repository (src/database/repository.rs, src/database/logic.rs, src/database/tables/*)

- `Repository` implements the Repository/Facade pattern on top of SQLite (SQLitePool).
- Handles database initialization, pool management, and performance configuration.
- Exposes a unified API for inserts, batches, status queries (`has_data`), network/hub management, and epoch/balance.
- Independent table modules: measurement, monitor, alert_temp, alert_air, hub, network, balance_epoch, etc.
- `logic.rs` implements DB orchestration and the Store-and-Forward strategy (dba_insert_task groups data into batches and stores it; dba_get_task retrieves batches for sending).

### MQTT

- Expected local broker (Mosquitto is assumed to be a service).
- Hub messages: Binary MessagePack.
- Topics calculated by NetworkManager based on topology.

### gRPC (src/grpc_service)

- Communication channel with the cloud server.
- Used to synchronize status, send real-time telemetry, and receive commands/updates.

### Firmware / OTA

- Firmware module manages OTA updates with a timeout (OTA_TIMEOUT is configurable, default is 5 minutes).
- Services coordinated with the Core and messages sent to the Hub.

### FSM (State Machine)

- Implements local control/decision logic for specific devices or flows.
- Parameters such as PERCENTAGE for configurable decisions.

### Heartbeat / Metrics / Quorum

- Services that maintain health, metrics, and coordination when applicable (quorum).

## Applied Design Patterns

- Mediator: `Core` as a central bus that decouples services.
- Repository / Façade: encapsulates SQLite access (`Repository`).
- Actor Model / Task-based: each service runs in its own asynchronous task and they communicate via `mpsc` channels.
- Store and Forward: to ensure entry
>>>>>>> master
