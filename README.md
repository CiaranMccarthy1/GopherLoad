# GopherLoad

GopherLoad is an L7 HTTP reverse proxy and load balancer written in Go (1.22+) that features dynamic routing strategies and Kubernetes integration. It ingests real-time backend metrics via a gRPC Protobuf interface and automatically scales a target Kubernetes Deployment based on aggregated connection loads.

## Architecture

```text
       HTTP (:8080)                                         gRPC (:9090)
[Client] ───> [GopherLoad HTTP Proxy]                         [GopherLoad gRPC Server]
                    │                                                  ▲
                    ├──> [Cluster A] ─────────(LoadReport)─────────────┤
                    ├──> [Cluster B] ─────────(LoadReport)─────────────┤
                    └──> [Cluster C] ─────────(LoadReport)─────────────┘
                                                                       │
                                                                       ▼
                                                                [K8s Scaler]
                                                                       │ (client-go)
                                                                       ▼
                                                          [Kubernetes Deployment]
```

*Note: GopherLoad supports Graceful Shutdown handling `os.Interrupt` and `syscall.SIGTERM` using `signal.NotifyContext`, safely draining the HTTP reverse proxy (`httpServer.Shutdown`) and gRPC interface (`grpcServer.GracefulStop`). Diagnostic endpoints include Prometheus metrics at `/metrics` and a readiness check at `/__health`.*

## Routing Strategies

The balancer uses structural typing to enforce a single canonical `Strategy` interface. Backends are selected based on the configured algorithm.

| Strategy | Algorithm | Best For | Tie-break |
| :--- | :--- | :--- | :--- |
| `current_load` | Least active connections | Workloads with variable request durations | Lowest Cluster ID (lexicographical) |
| `proximity` | Region-latency aware | Multi-region topologies | Lowest load, then lowest ID |
| `modulo` | FNV-32a hash of Client ID | Sticky sessions and cache coherency | Hash remainder |

## Configuration Flags

| Flag | Default | Description |
| :--- | :--- | :--- |
| `--http-addr` | `:8080` | HTTP reverse proxy and diagnostics listen address |
| `--grpc-addr` | `:9090` | gRPC load reporting listen address |
| `--strategy` | `current_load` | Routing strategy (`modulo`, `proximity`, `current_load`) |
| `--kubeconfig` | `""` | Path to kubeconfig (falls back to InClusterConfig if empty) |
| `--namespace` | `default` | Kubernetes namespace for the target deployment |
| `--deployment` | `gopherload` | Kubernetes deployment name to scale |
| `--scale-up` | `800` | Aggregate reported load threshold to increment replicas |
| `--scale-down` | `200` | Aggregate reported load threshold to decrement replicas |
| `--scale-cooldown` | `2m0s` | Minimum duration between scaling operations |
| `--backend` | (3 mock clusters) | Cluster spec format: `id=<id>,url=<url>,region=<region>,max=<max>` |

## gRPC API

Backend clusters actively push connection metrics to GopherLoad using the `ClusterStatus.ReportLoad` RPC. The `active_connections` field updates the internal gauge used by the `current_load` routing strategy, while the accumulated `total_load` across all clusters evaluates scaling thresholds in the Kubernetes `Controller`.

```protobuf
syntax = "proto3";

package gopherload.v1;

option go_package = "github.com/ciara/gopherload/api/proto;gopherloadv1";

// ClusterStatus reports load metrics from clusters to the balancer.
service ClusterStatus {
  rpc ReportLoad(LoadReport) returns (LoadAck);
}

message LoadReport {
  string cluster_id = 1;
  int64 active_connections = 2;
  string region = 3;
  int64 max_connections = 4;
  int64 observed_at_unix = 5;
}

message LoadAck {
  bool accepted = 1;
  string message = 2;
  int64 total_load = 3;
}
```

## Local Testing Tools

To simplify development and testing, GopherLoad includes two helper utilities:

### 1. Fake Backend (`cmd/fakebackend`)
A lightweight HTTP server that simulates a real cluster node.
*   **Latency:** Randomly adds 0–50ms delay to non-health requests.
*   **Errors:** Simulates a 5% failure rate (HTTP 500) for testing resilience.
*   **Health:** Responds to `GET /health` with `200 OK`.
*   **Load Reporting:** Optional gRPC load reports to the balancer when `-grpc-addr` is set.

```bash
# Run 3 instances on different ports
go run ./cmd/fakebackend -port 8081 -id cluster-a
go run ./cmd/fakebackend -port 8082 -id cluster-b
go run ./cmd/fakebackend -port 8083 -id cluster-c

# Run with load reporting to the balancer
go run ./cmd/fakebackend -port 8081 -id cluster-a -grpc-addr localhost:9090 -region us-east
```

### 2. Load Tester (`cmd/loadtest`)
Sends requests at a controlled rate and provides a live summary of results.
*   **Live Stats:** Shows latency, status codes, and backend distribution.
*   **Configurable:** Adjust rate (req/min), target URL, and summary interval.
*   **Optional Stop:** Stop after N consecutive failures with `-fail-after`.

```bash
# Send 100 requests per minute to the balancer
go run ./cmd/loadtest -rate 100 -url http://localhost:8080

# Stop after 20 consecutive failures
go run ./cmd/loadtest -rate 500 -fail-after 20
```

## Development

The project requires Go 1.23 or higher and uses `client-go` and `google.golang.org/grpc`.

### Run Tests
```bash
go test ./... -v -count=1
```

### Build
```bash
go build -o gopherload ./cmd/gopherload
go build -o fakebackend ./cmd/fakebackend
go build -o loadtest ./cmd/loadtest
```

### Run Locally (Full Cluster Simulation)

1. **Start Backends:** (In separate terminals)
   ```bash
   go run ./cmd/fakebackend -port 8081 -id cluster-a
   go run ./cmd/fakebackend -port 8082 -id cluster-b
   ```

2. **Start Balancer:**
   ```bash
   go run ./cmd/gopherload --strategy=current_load
   ```

3. **Start Traffic:**
   ```bash
   go run ./cmd/loadtest -rate 500
   ```

The Balancer will log its routing decisions (e.g., `[LB] Routing GET /test -> cluster-a`), and the Load Tester will provide a summary of the distribution every 10 seconds.
