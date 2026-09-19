# PIMAD Edge

An Edge gateway for IoT networks written in Rust. Its objective is to connect physical hubs with the cloud, providing:

- Routing and message transformation between MQTT (local) and gRPC (cloud).
- Local persistence with a "Store and Forward" strategy to guarantee delivery in the event of connectivity losses.
- Dynamic management of network topologies (logical networks and hubs).
- Auxiliary services: firmware (OTA), local FSM, metrics, heartbeat, and quorum.

## Summary

This project is an Edge gateway designed for industrial/IoT environments that:
- Consumes telemetry from hubs via MQTT (MessagePack).
- Decides routing: real-time transmission to the cloud (gRPC) or local storage in SQLite when there is no connection.
- Keeps the network topology synchronized between the server, local database, and physical hubs.
- It is designed to run as a Linux service (systemd) and cooperate with a local MQTT broker (for example, Mosquitto).

## Core Technologies

- Language: Rust (async)
- Asynchronous Runtime: Tokio
- Local Persistence: SQLite via sqlx
- Local Messaging: MQTT (external broker, Mosquitto is assumed as the service)
- Binary Serialization: MessagePack (for telemetry between hubs and gateway)
- Cloud Communication: gRPC
- Observability: tracing, tracing_subscriber (EnvFilter)

## Overall Architecture

The architecture is modular and based on asynchronous actors/tasks:
- Multiple asynchronous tasks (Tokio tasks) form the services:
- Data/DB tasks (dba_task, dba_insert_task, dba_get_task)
- Network tasks (network_admin, network_dba)
- Message routing tasks (msg_from_hub, msg_to_hub)
- Auxiliary Services: GrpcService, FsmService, FirmwareService HeartbeatService, MetricsService
- Core acts as a central mediator: it decouples services and channels messages.
- Repository encapsulates access to SQLite as a facade; sqlx::SqlitePool enables concurrent use.
- NetworkManager is the in-memory cache (HashMap) that avoids database queries for each message.
- Store-and-Forward strategy: data is stored locally when it cannot be sent and is retried later.

## Components and Responsibilities

### Core (src/core/domain.rs)

- Implements the mediator pattern.
- Receives messages from internal services and routes them.
- Decouples services (Data, Firmware, FSM, Network, Message, etc.).

### Message (src/message/logic.rs, src/message/domain.rs)

- Router/Switchboard between Hubs (MQTT) and the cloud (gRPC).
- Uplink (msg_from_hub): Receives MessagePack telemetry from MQTT, deserializes it, and decides the destination (cloud or database).
- Downlink (msg_to_hub): Serializes commands to MessagePack and publishes them to MQTT topics for Hubs.

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
