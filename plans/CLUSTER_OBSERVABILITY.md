# Cluster Observability Implementation Plan

## Status

| Phase | Description | Status |
|-------|-------------|--------|
| Phase 1 | Protobuf Schema Updates | ✅ Complete |
| Phase 2 | Task History with `scheduler_id` | ✅ Complete |
| Phase 3 | GetTaskHistory RPC Implementation | ✅ Complete |
| Phase 4 | Executor Control Stream | ✅ Complete |
| Phase 5 | Cluster Metrics Endpoint | ✅ Complete |
| Phase 6 | Wire Everything Together | ✅ Complete |

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
- Add `node_id` (full advertise address) and `node_role` labels to all metrics
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
        // Add node_id and node_role labels
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
- Add `node_id` and `node_role` labels to all metrics

---

### Phase 6: Wire Everything Together

**Note:** This phase also includes items deferred from Phase 5:
- Wire `MetricsReader` to executor control stream client for actual metrics collection
- Wire `MetricsReader` to `ClusterServiceImpl` for `GetMetrics` RPC
- Create and pass `ClusterMetricsCollector` to metrics server in cluster mode

**Files to modify:**
- `crates/runtime/src/cluster/service.rs` - Update constructor
- `crates/runtime/src/cluster/servers.rs` - Pass MetricsReader
- `crates/runtime/src/cluster/control_stream_client.rs` - Accept MetricsReader
- `crates/runtime/src/lib.rs` - Create and wire components
- `bin/spiced/src/lib.rs` - Create MetricsReader for cluster mode

**Steps:**

#### 6.1 Create MetricsReader for Cluster Mode
- In `bin/spiced/src/lib.rs`, create a `MetricsReader` when in cluster mode
- Add it to the `SdkMeterProvider` alongside existing readers
- Store reference for passing to cluster components

#### 6.2 Update ClusterServiceImpl
- Accept `MetricsReader` in constructor (for `GetMetrics` RPC)
- Accept `ExecutorRegistry` and `DataFusion` in constructor
- Store references for RPC handlers

#### 6.3 Wire Executor Control Stream Client
- Update `ControlStreamManager::new()` to accept `Option<MetricsReader>`
- Pass `MetricsReader` to `spawn_control_stream()`
- Use `MetricsReader::collect_otlp()` in `handle_scheduler_message()` instead of returning empty Vec

#### 6.4 Create and Wire ClusterMetricsCollector
- Create `ClusterMetricsCollector` when in scheduler mode
- Pass to metrics server `start()` function
- Collector needs: `SchedulerPeers`, `ExecutorRegistry`, `ClientTlsConfig`, node_id, local metrics fn

#### 6.5 Update Runtime Initialization
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
| `crates/runtime/src/cluster/control_stream_client.rs` | **New** | Executor-side control stream |
| `crates/runtime/src/cluster/servers.rs` | Modify | Wire up cluster service components |
| `crates/runtime/src/metrics_server/mod.rs` | Modify | Handle `?scope=cluster`, OTLP→Prometheus |
| `crates/runtime/src/metrics_server/cluster.rs` | **New** | Cluster metrics collection/merging |
| `crates/runtime/src/metrics_reader.rs` | **New** | On-demand OTLP metrics collection |
| `crates/runtime/src/lib.rs` | Modify | Wire up metrics server with cluster collector |
| `bin/spiced/src/lib.rs` | Modify | Wire up new components, create MetricsReader |

---

## Dependencies

**Crates to verify/add:**
- `opentelemetry-proto` - For OTLP protobuf types
- `arrow-ipc` - For Arrow IPC encoding (verify feature enabled)

---

## Testing Strategy

### Unit Tests

The following unit tests are implemented in the respective modules:

#### `control_stream_client.rs`
- `test_normalize_scheduler_endpoint` - Verify URL normalization with/without scheme
- `test_control_stream_manager_new` - Verify manager initialization
- `test_control_stream_manager_update_schedulers_empty` - Handle empty scheduler list

#### `metrics_server/cluster.rs`
- `test_normalize_endpoint` - Verify endpoint URL normalization
- `test_add_node_labels_to_empty_request` - Handle empty OTLP request
- `test_add_node_labels_with_gauge_metrics` - Verify labels added to gauge metrics
- `test_add_node_labels_with_counter_metrics` - Verify labels added to counter metrics
- `test_add_node_labels_idempotent` - Labels not duplicated on repeated calls

#### `executor_registry.rs`
- `test_register_unregister` - Basic registration lifecycle
- `test_reconnect_replaces_connection` - Re-registration replaces old connection
- `test_request_metrics_empty_registry` - Handle empty registry
- `test_multiple_executors` - Manage multiple executor connections
- `test_unregister_nonexistent` - Handle unregistering unknown executor

#### `metrics_server/mod.rs`
- `test_parse_query_string` - Parse `?scope=cluster&foo=bar`
- `test_parse_query_string_empty_value` - Handle `key=` with no value
- `test_parse_query_string_multiple_equals` - Handle `key=value=with=equals`
- `test_otlp_to_prometheus_gauge` - Convert OTLP gauge to Prometheus format
- `test_otlp_to_prometheus_counter` - Convert OTLP counter to Prometheus format
- `test_otlp_to_prometheus_histogram` - Convert OTLP histogram to Prometheus format
- `test_otlp_to_prometheus_empty` - Handle empty OTLP request
- `test_sanitize_metric_name` - Prometheus metric name sanitization
- `test_sanitize_label_name` - Prometheus label name sanitization
- `test_escape_label_value` - Escape special characters in label values

#### `task_history/federated.rs`
- `test_normalize_scheduler_endpoint` - URL normalization for peer queries
- `test_build_peer_sql` - SQL generation for peer queries
- `test_build_peer_sql_with_limit` - SQL with LIMIT clause

#### `metrics_reader.rs`
- `test_metrics_reader_default` - Default reader doesn't panic
- `test_otel_value_to_proto_*` - Convert OpenTelemetry values to protobuf

### Manual E2E Tests

These tests require a running cluster and should be verified manually:

#### 1. Multi-Scheduler Task History Federation

**Setup:**
```bash
# Terminal 1: Start scheduler 1
spiced --role scheduler --node-bind-address 0.0.0.0:50051 --metrics 127.0.0.1:9091

# Terminal 2: Start scheduler 2 with peer discovery
spiced --role scheduler --node-bind-address 0.0.0.0:50052 --metrics 127.0.0.1:9092 \
  --scheduler-address 127.0.0.1:50051
```

**Verify:**
1. Execute queries on each scheduler to generate task history
2. Query `SELECT * FROM runtime.task_history` on scheduler 1
3. Verify results include `scheduler_id` column
4. Verify results contain tasks from both schedulers

#### 2. Executor Control Stream Lifecycle

**Setup:**
```bash
# Terminal 1: Start scheduler
spiced --role scheduler --node-bind-address 0.0.0.0:50051

# Terminal 2: Start executor
spiced --role executor --scheduler-address 127.0.0.1:50051
```

**Verify:**
1. Check scheduler logs for "Control stream established" at DEBUG level
2. Kill executor (Ctrl+C)
3. Check scheduler logs for disconnect
4. Restart executor
5. Verify reconnection logged

#### 3. Cluster Metrics Endpoint

**Setup:**
```bash
# Start scheduler with metrics enabled
spiced --role scheduler --node-bind-address 0.0.0.0:50051 --metrics 127.0.0.1:9090
```

**Verify:**
```bash
# Local metrics (default)
curl http://127.0.0.1:9090/metrics

# Cluster-scoped metrics
curl "http://127.0.0.1:9090/metrics?scope=cluster"
```

Expected: Cluster metrics include `node_id` and `node_role` labels on all metrics.

#### 4. Cluster Metrics with Executors

**Setup:**
```bash
# Terminal 1: Scheduler
spiced --role scheduler --node-bind-address 0.0.0.0:50051 --metrics 127.0.0.1:9090

# Terminal 2: Executor
spiced --role executor --scheduler-address 127.0.0.1:50051 --metrics 127.0.0.1:9091
```

**Verify:**
```bash
curl "http://127.0.0.1:9090/metrics?scope=cluster"
```

Expected: Metrics from both scheduler (`node_role="scheduler"`) and executor (`node_role="executor"`).

#### 5. Partial Failure Handling

**Setup:**
```bash
# Start 3 schedulers
spiced --role scheduler --node-bind-address 0.0.0.0:50051 --metrics 127.0.0.1:9091
spiced --role scheduler --node-bind-address 0.0.0.0:50052 --scheduler-address 127.0.0.1:50051
spiced --role scheduler --node-bind-address 0.0.0.0:50053 --scheduler-address 127.0.0.1:50051
```

**Verify:**
1. Kill one scheduler
2. Query `runtime.task_history` on a running scheduler
3. Verify error message includes the failed peer's address

### Test Commands

```bash
# Run all cluster observability unit tests
cargo test -p runtime control_stream
cargo test -p runtime executor_registry
cargo test -p runtime metrics_server
cargo test -p runtime metrics_reader
cargo test -p runtime federated

# Run with verbose output
cargo test -p runtime -- --nocapture

# Run specific test
cargo test -p runtime test_add_node_labels_with_gauge_metrics
```

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
