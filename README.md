# GopherLoad

GopherLoad is a high-performance, professional L7 Load Balancer and Reverse Proxy written in Go. It features dynamic routing strategies, gRPC-based real-time load reporting, and automated Kubernetes scaling integration.

## Features

- **L7 Reverse Proxy**: Efficiently routes HTTP traffic to backend clusters.
- **Pluggable Routing Strategies**:
  - `current_load`: Routes to the least busy cluster (least connections).
  - `proximity`: Region-aware routing favoring the lowest latency.
  - `modulo`: Hash-based sticky routing for client consistency.
- **gRPC Load Reporting**: Real-time metric ingestion from backend clusters via gRPC (using a lightweight JSON codec).
- **Auto-Scaling**: Built-in Kubernetes `ScaleController` that manages infrastructure based on total system load.
- **Graceful Shutdown**: Orchestrated shutdown of HTTP and gRPC servers.

## Project Structure

GopherLoad follows the [Standard Go Project Layout](https://github.com/golang-standards/project-layout):

- `cmd/gopherload`: The main application entry point.
- `internal/balancer`: Core load balancing and proxying logic.
- `internal/strategy`: Implementations of different routing algorithms.
- `internal/scaler`: Kubernetes integration for auto-scaling.
- `internal/rpc`: gRPC service for cluster load reporting.
- `api/proto`: API definitions and protobuf contracts.

## Installation & Usage

### Prerequisites
- Go 1.22+
- Access to a Kubernetes cluster (optional, for scaling features)

### Build
```bash
go build -o gopherload ./cmd/gopherload
```

### Run
Start the load balancer with default settings:
```bash
./gopherload
```

### Configuration
GopherLoad can be configured via command-line flags:

| Flag | Description | Default |
|------|-------------|---------|
| `--http-addr` | HTTP proxy listen address | `:8080` |
| `--grpc-addr` | gRPC listen address | `:9090` |
| `--strategy` | Routing strategy (modulo/proximity/current_load) | `current_load` |
| `--scale-up` | Load threshold to trigger scale-up | `800` |
| `--scale-down`| Load threshold to trigger scale-down | `200` |
| `--backend`   | Backend cluster spec (multiple allowed) | `cluster-a,b,c` |

**Example Backend Spec:**
```bash
./gopherload --backend "id=prod-1,url=http://10.0.0.1,region=us-east,max=1000"
```

## API

Backends report their load via the `ClusterStatus` gRPC service. The API definition is located in `api/proto/cluster_status.proto`.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
