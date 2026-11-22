# Sentinel Pulse Broker

A distributed message broker built with Apache Pekko and gRPC, implementing a pub/sub pattern with support for multiple channels, message TTL, and two operational modes: **strict-order** and **high-throughput**.

## Overview

Sentinel Pulse Broker is a high-performance message broker that enables asynchronous communication between services using a publish/subscribe pattern. It leverages Apache Pekko's actor model for concurrent message handling and gRPC for efficient, type-safe communication.

## Features

- **Multi-Channel Pub/Sub**: Publish and subscribe to multiple distinct channels
- **TTL Support**: Messages can be configured with Time-To-Live expiration
- **Two Operational Modes**:
  - **Strict-Order**: Per-channel ordering with least-loaded actor assignment
  - **High-Throughput**: Round-robin distribution across an actor pool (no ordering guarantee)
- **Load Balancing**: Distributes channels across multiple actor instances
- **Message Persistence**: Stores messages in memory with configurable expiration
- **Historical Data**: New subscribers can receive previously stored messages
- **gRPC API**: Type-safe protocol buffers-based API
- **Docker Support**: Ready-to-deploy Docker container

## Architecture

### Strict-Order Mode

Each channel is assigned to a single `ChannelActor` based on least-loaded heuristics. Messages within a channel are processed in order.

```mermaid
graph TD
    BS[BrokerServer<br/>gRPC] --> PS[ProducerService]
    BS --> CS[ConsumerService]
    PS --> BM[BrokerManager]
    CS --> BM
    BM -->|"Least Loaded"| CA1[ChannelActor 1]
    BM -->|"Least Loaded"| CA2[ChannelActor 2]
    BM -->|"Least Loaded"| CA3[ChannelActor N]
    CA1 --> |"Sub"| C1[Consumer]
    CA2 --> |"Sub"| C2[Consumer]
    CA3 --> |"Sub"| C3[Consumer]
```

### High-Throughput Mode

Messages for all channels are round-robined across a pool of `ChannelActor`s. Consumer subscriptions are broadcast to the entire pool.

```mermaid
graph TD
    BS[BrokerServer<br/>gRPC] --> PS[ProducerService]
    BS --> CS[ConsumerService]
    PS --> BM[BrokerManager]
    CS --> BM
    BM --> R[Router<br/>Round Robin]
    R --> CA1[ChannelActor 1]
    R --> CA2[ChannelActor 2]
    R --> CA3[ChannelActor N]
    CA1 --> |"Sub"| C1[Consumer]
    CA2 --> |"Sub"| C2[Consumer]
    CA3 --> |"Sub"| C3[Consumer]
```

### Core Components

- **BrokerServer**: gRPC server handling incoming requests
- **ProducerService**: Handles message publishing
- **ConsumerService**: Handles message consumption/subscription
- **BrokerManager**: Manages actor pool and load distribution (mode-dependent)
- **ChannelActors**: Individual actors handling message storage and delivery

## Technology Stack

- **Scala** 3.3.4
- **Apache Pekko** 1.4.0 (Actor, Stream, gRPC)
- **gRPC** with Protocol Buffers
- **Logback** for logging
- **sbt** as build tool
- **Docker** for deployment

## Configuration

Configure the broker via `application.conf`:

```hocon
broker {
  ip = "0.0.0.0"
  port = 8080
  actors = 4
  mode = "strict-order"  # or "high-throughput"

  producer {
    timeout = 2s
    parallelism = 8
  }
}
```

| Parameter                     | Description                          | Default        |
|-------------------------------|--------------------------------------|----------------|
| `broker.ip`                   | IP address to bind                   | `0.0.0.0`      |
| `broker.port`                 | Port number                          | `8080`         |
| `broker.actors`               | Number of actors in the pool         | `4`            |
| `broker.mode`                 | `strict-order` or `high-throughput`  | `strict-order` |
| `broker.producer.timeout`     | Ask timeout for producer messages    | `2s`           |
| `broker.producer.parallelism` | Parallelism for producer stream asks | `cores * 2`    |

Environment variables override:
- `BROKER_IP`
- `BROKER_PORT`
- `BROKER_ACTORS`
- `BROKER_STORAGE_MODE`

## Operational Modes

### Strict-Order Mode

- **Best for**: State machines, event sourcing, replication, ordered processing.
- **Behavior**: One actor per channel. Messages within a channel are delivered in publication order.
- **Routing**: `BrokerManager` assigns channels to the least-loaded actor and tracks assignments in `channelIndex`.

### High-Throughput Mode

- **Best for**: Metrics, logs, telemetry, fire-and-forget events where order does not matter.
- **Behavior**: Messages are round-robined across the entire actor pool. No ordering guarantee.
- **Routing**: A Pekko `PoolRouter` with `withBroadcastPredicate` handles both round-robin publishing and broadcast subscription.

## API

### Producer Service

```protobuf
service ProducerService {
  rpc push(stream PublishRequest) returns (PublishSummary);
}
message PublishRequest {
  oneof payload {
    PublishMetadata metadata = 1;
    bytes data = 2;
  }
}
message PublishMetadata {
  string channel = 1;
  int64 ttl = 2;
}

message PublishSummary {
  bool success = 1;
  int32 count = 2;
  string error_message = 3;
}
```

### Consumer Service

```protobuf
service ConsumerService {
  rpc pull(PullRequest) returns (stream PullResponse);
}
message PullRequest {
  string channel = 1;
  bool all_messages = 2;
}

message PullResponse {
  string channel = 1;
  bytes payload = 2;
}
```

## Building

```bash
sbt compile
```

## Testing

```bash
sbt test
```

## Docker

Build the Docker image:

```bash
sbt Docker/publishLocal
```

Run the container:

```bash
docker run -d -p 8080:8080 --name broker sentinel-pulse-broker:latest
```

## Usage Example

See [Python Examples](examples/python/README.md) for client implementation in Python.

## License

MIT License
