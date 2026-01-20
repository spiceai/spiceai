# Cluster Observability Implementation Plan

## Overview

This document outlines the implementation plan for adding unified observability features to Spice's cluster mode. The features enable operators to have a single view of task history and metrics across all nodes in a Spice cluster.

## Features

### Feature 1: Federated Task History

Query `runtime.task_history` across all schedulers with a `scheduler_id` dimension that identifies which scheduler executed each task.

**Key Behaviors:**
- `scheduler_id` column only added in cluster mode (when `effective_role` is set)
- Filter pushdown by `scheduler_id` supported for efficiency
- No timeout - expects all peers to be responsive
- No pagination - all results expected to fit in memory
- Partial failures result in failure response (includes which peers failed)

### Feature 2: Cluster-Scoped Prometheus Metrics

The `/metrics` Prometheus endpoint gains support for a `?scope=cluster` query parameter that triggers fan-out collection of metrics from all schedulers and executors.

**Key Behaviors:**
- Add `spice_node_id` (full advertise address) and `spice_node_role` labels to all metrics
- All collection happens in parallel (local + schedulers + executors)
- No timeout for fan-out queries
- No caching
- Partial failures result in failure response (includes which peers failed)
- Cluster observability is on by default in cluster mode

### Feature 3: Executor Control Stream

Executors establish bidirectional gRPC streams to ALL schedulers, enabling schedulers to request metrics from executors on-demand.

**Key Behaviors:**
- Each executor initiates control streams to ALL schedulers
- Schedulers accept all connections (no replacement on reconnect)
- Wait indefinitely for executor metrics response (no timeout)
- Executors identify themselves using their advertise address
- Control stream events (connect/disconnect/request) logged at DEBUG level

---

## Protobuf Schema Changes

**File:** `crates/runtime-proto/proto/spice.proto`

### New RPCs in ClusterService

```protobuf
service ClusterService {
    // Existing RPCs
    rpc GetAppDefinition(GetAppDefinitionRequest) returns (GetAppDefinitionResponse);
    rpc ExpandSecret(ExpandSecretRequest) returns (ExpandSecretResponse);
    rpc GetSchedulers(GetSchedulersRequest) returns (GetSchedulersResponse);

    // New: Query task history from this scheduler
    rpc GetTaskHistory(GetTaskHistoryRequest) returns (GetTaskHistoryResponse);

    // New: Get OTLP metrics from this scheduler
    rpc GetMetrics(GetMetricsRequest) returns (GetMetricsResponse);

    // New: Bidirectional control stream (executor-initiated)
    rpc ControlStream(stream ExecutorControlMessage) returns (stream SchedulerControlMessage);
}
```

### New Messages

```protobuf
// Task History Federation
message GetTaskHistoryRequest {
    string sql = 1;  // SQL query to execute against local task_history
}

message GetTaskHistoryResponse {
    bytes arrow_ipc = 1;  // Arrow IPC-encoded RecordBatch
}

// Metrics Collection
message GetMetricsRequest {}

message GetMetricsResponse {
    bytes otlp_metrics = 1;  // OTLP ExportMetricsServiceRequest protobuf
}

// Executor Control Stream
message ExecutorControlMessage {
    string executor_id = 1;  // Executor's advertise address
    oneof message {
        ExecutorHeartbeat heartbeat = 2;
        MetricsResponse metrics = 3;
    }
}

message SchedulerControlMessage {
    oneof message {
        MetricsRequest request_metrics = 1;
    }
}

message ExecutorHeartbeat {
    int64 timestamp_ms = 1;
}

message MetricsRequest {
    string request_id = 1;
}

message MetricsResponse {
    string request_id = 1;
    bytes otlp_metrics = 2;  // OTLP ExportMetricsServiceRequest protobuf
}
```

---

## Implementation Phases

### Phase 1: Protobuf Schema Updates

**Files to modify:**
- `crates/runtime-proto/proto/spice.proto` - Add new RPCs and messages

**Steps:**
1. Add `GetTaskHistory` RPC and messages
2. Add `GetMetrics` RPC and messages
3. Add `ControlStream` RPC and control messages
4. Regenerate Rust bindings via `cargo build`

---

### Phase 2: Task History with `scheduler_id`

**Files to modify:**
- `crates/runtime/src/task_history/mod.rs` - Add conditional `scheduler_id` column
- `crates/runtime/src/task_history/otel_exporter.rs` - Populate `scheduler_id`
- `crates/runtime/src/init/task_history.rs` - Pass cluster mode flag

**Steps:**

#### 2.1 Update TaskSpan Schema
- Add `scheduler_id: Option<Arc<str>>` field to `TaskSpan` struct
- Modify `table_schema()` to accept `is_cluster_mode: bool` parameter
- Conditionally include `scheduler_id` column when in cluster mode
- Update `to_record_batch()` to handle the new column

#### 2.2 Update TaskHistoryExporter
- Accept `scheduler_id: Option<Arc<str>>` in constructor
- Store scheduler_id (advertise address) for populating on each span
- Populate `scheduler_id` field on each `TaskSpan` during export

#### 2.3 Update Task History Initialization
- Pass cluster mode flag from `Runtime` to `TaskSpan::instantiate_table()`
- Pass scheduler's advertise address to `TaskHistoryExporter`

---

### Phase 3: GetTaskHistory RPC Implementation

**Files to modify:**
- `crates/runtime/src/cluster/service.rs` - Add RPC handler

**New files:**
- `crates/runtime/src/task_history/federated.rs` - Federated TableProvider

**Steps:**

#### 3.1 Implement GetTaskHistory RPC Handler
```rust
async fn get_task_history(
    &self,
    request: Request<GetTaskHistoryRequest>,
) -> Result<Response<GetTaskHistoryResponse>, Status>
```
- Execute SQL query against local `runtime.task_history`
- Encode results as Arrow IPC bytes
- Return in response

#### 3.2 Create Federated TableProvider
```rust
pub struct FederatedTaskHistoryTable {
    local_table: Arc<dyn TableProvider>,
    scheduler_peers: Arc<RwLock<SchedulerPeers>>,
    client_tls_config: Option<ClientTlsConfig>,
}
```
- Implement `TableProvider` trait
- On `scan()`: fan out to all peers in parallel, union results
- If any peer fails, return error with peer identifiers

#### 3.3 Register Federated Table
- In cluster scheduler mode, wrap local table with `FederatedTaskHistoryTable`
- Register as `runtime.task_history`

---

### Phase 4: Executor Control Stream

**Files to modify:**
- `crates/runtime/src/cluster/service.rs` - Add stream handler
- `crates/runtime/src/cluster/mod.rs` - Add executor-side client

**New files:**
- `crates/runtime/src/cluster/executor_registry.rs` - Track executor connections

**Steps:**

#### 4.1 Create Executor Registry
```rust
pub struct ExecutorRegistry {
    connections: Arc<RwLock<HashMap<String, ExecutorConnection>>>,
}

struct ExecutorConnection {
    executor_id: String,
    request_tx: mpsc::Sender<SchedulerControlMessage>,
    pending_requests: Arc<RwLock<HashMap<String, oneshot::Sender<MetricsResponse>>>>,
}

impl ExecutorRegistry {
    pub fn register(&self, executor_id: String, tx: mpsc::Sender<SchedulerControlMessage>);
    pub fn unregister(&self, executor_id: &str);
    pub async fn request_metrics_from_all(&self) -> Result<Vec<(String, Vec<u8>)>, Error>;
}
```

#### 4.2 Implement Scheduler-Side Stream Handler
- Add `executor_registry: Arc<ExecutorRegistry>` to `ClusterServiceImpl`
- Implement `control_stream()` RPC
- Register executor on stream start
- Route metrics requests/responses
- Unregister on disconnect
- Log events at DEBUG level

#### 4.3 Implement Executor-Side Stream Client
- In `initialize_cluster_executor()`, spawn control stream to each scheduler
- Listen for `MetricsRequest` messages
- On request: collect local OTLP metrics, send `MetricsResponse`
- Handle reconnection with existing backoff logic

---

### Phase 5: Cluster Metrics Endpoint

**Files to modify:**
- `crates/runtime/src/metrics_server/mod.rs` - Handle `?scope=cluster`

**New files:**
- `crates/runtime/src/metrics_server/cluster.rs` - Cluster metrics collection

**Steps:**

#### 5.1 Create Cluster Metrics Collector
```rust
pub struct ClusterMetricsCollector {
    scheduler_peers: Arc<RwLock<SchedulerPeers>>,
    executor_registry: Arc<ExecutorRegistry>,
    client_tls_config: Option<ClientTlsConfig>,
    node_id: String,
}

impl ClusterMetricsCollector {
    pub async fn collect(&self) -> Result<ExportMetricsServiceRequest, Error> {
        // In parallel:
        // 1. Collect local metrics
        // 2. Fan out GetMetrics to all peer schedulers
        // 3. Request metrics from all executors via control stream
        
        // Wait for all results
        // If any fails, return error with failed peer identifiers
        
        // Merge all OTLP metrics
        // Add spice_node_id and spice_node_role labels
    }
}
```

#### 5.2 Implement GetMetrics RPC Handler
```rust
async fn get_metrics(
    &self,
    request: Request<GetMetricsRequest>,
) -> Result<Response<GetMetricsResponse>, Status>
```
- Collect local OTLP metrics from `SdkMeterProvider`
- Serialize to OTLP protobuf bytes
- Return in response

#### 5.3 Update Metrics Server
- Parse `?scope=cluster` query parameter
- If cluster scope: call `ClusterMetricsCollector::collect()`
- Convert merged OTLP to Prometheus format with node labels
- If local scope (default): existing behavior

#### 5.4 OTLP to Prometheus Conversion
```rust
fn otlp_to_prometheus(
    metrics: ExportMetricsServiceRequest,
    node_labels: &[(String, String)],
) -> Vec<MetricFamily>
```
- Convert OTLP metrics to Prometheus `MetricFamily` format
- Add `spice_node_id` and `spice_node_role` labels to all metrics

---

### Phase 6: Wire Everything Together

**Files to modify:**
- `crates/runtime/src/cluster/service.rs` - Update constructor
- `bin/spiced/src/lib.rs` - Create and wire components

**Steps:**

#### 6.1 Update ClusterServiceImpl
- Accept `ExecutorRegistry` and `DataFusion` in constructor
- Store references for RPC handlers

#### 6.2 Update Runtime Initialization
- Create `ExecutorRegistry` when in scheduler mode
- Create `ClusterMetricsCollector` when in scheduler mode
- Pass to `ClusterServiceImpl`
- Pass to metrics server

---

## File Change Summary

| File | Type | Description |
|------|------|-------------|
| `crates/runtime-proto/proto/spice.proto` | Modify | Add 3 RPCs and messages |
| `crates/runtime/src/task_history/mod.rs` | Modify | Add conditional `scheduler_id` column |
| `crates/runtime/src/task_history/otel_exporter.rs` | Modify | Populate `scheduler_id` |
| `crates/runtime/src/task_history/federated.rs` | **New** | Federated TableProvider |
| `crates/runtime/src/init/task_history.rs` | Modify | Cluster mode handling |
| `crates/runtime/src/cluster/service.rs` | Modify | Add RPC handlers, executor registry |
| `crates/runtime/src/cluster/executor_registry.rs` | **New** | Track executor connections |
| `crates/runtime/src/cluster/mod.rs` | Modify | Add executor control stream client |
| `crates/runtime/src/metrics_server/mod.rs` | Modify | Handle `?scope=cluster` |
| `crates/runtime/src/metrics_server/cluster.rs` | **New** | Cluster metrics collection/merging |
| `bin/spiced/src/lib.rs` | Modify | Wire up new components |

---

## Dependencies

**Crates to verify/add:**
- `opentelemetry-proto` - For OTLP protobuf types
- `arrow-ipc` - For Arrow IPC encoding (verify feature enabled)

---

## Testing Strategy

### Unit Tests
- Task history schema with/without `scheduler_id`
- OTLP metrics merging logic
- Arrow IPC encoding/decoding for task history
- Executor registry connection management

### Integration Tests
- Multi-scheduler task history federation
- Cluster metrics endpoint with multiple schedulers
- Cluster metrics endpoint with executors
- Executor control stream lifecycle (connect/disconnect/reconnect)
- Partial failure handling

---

## Error Handling

All fan-out operations follow this pattern:
1. Execute requests in parallel
2. Collect all results (success or failure)
3. If ANY request fails, return error containing:
   - List of failed peer identifiers (advertise addresses)
   - Error details for each failure
4. Only return success if ALL requests succeed

Example error message:
```
Failed to collect cluster metrics: peers failed: [192.168.1.10:50052: connection refused, 192.168.1.11:50052: timeout]
```

---

## Configuration

Cluster observability features are **enabled by default** when running in cluster mode (when `effective_role()` returns `Some(...)`).

No additional configuration flags are required.
