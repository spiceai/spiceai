/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Integration tests for the `RunQuery`, `GetPodLogs`, and `GetStatus`
//! dispatch paths.
//!
//! These exercise the cloud-connect client driver end-to-end against an
//! in-process tonic mock server. `GetPodLogs` and `GetStatus` are covered
//! here because they reuse the same mock-server / driver harness:
//! `GetPodLogs` asserts the log text rides verbatim in `payload_json` (and an
//! unavailable-logs runtime reports failure, not empty success); `GetStatus`
//! asserts the payload is a JSON status document carrying `phase`/`reason`.
//!
//! For `RunQuery` they cover:
//!
//! - A RunQuery → CommandResult round-trip whose `payload_json` carries
//!   only the `{row_count, truncated}` metadata, with the tabular rows
//!   delivered out-of-band as an Arrow IPC stream in `result_arrow_ipc`.
//! - The accompanying `EventLog{kind: "audit"}` carrying the SQL hash,
//!   row count, truncation flag, and command id.
//! - Truncation handling when the mock runtime reports more rows than
//!   the cloud-side cap.
//!
//! The runtime is mocked at the `RuntimeHandle::execute_sql` layer so the
//! tests run without DataFusion. The spiced binary's separate cap
//! arithmetic (DEFAULT / HARD) is covered by unit tests inside
//! `bin/spiced/src/cloud_connect.rs`.

#![expect(
    clippy::unwrap_used,
    clippy::doc_markdown,
    clippy::struct_field_names,
    clippy::items_after_statements,
    reason = "integration-test harness — readability over lint strictness"
)]

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use runtime_cloud_connect::config::CloudConnectConfig;
use runtime_cloud_connect::handlers::{QueryResult, RuntimeHandle};
use runtime_cloud_connect::identity::IdentityStore;
use runtime_cloud_connect::proto;
use runtime_cloud_connect::proto::cloud_connect_server::{CloudConnect, CloudConnectServer};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

/// Mock RuntimeHandle that fabricates RunQuery payloads using the same
/// shape the real spiced impl produces. Each test sets up the handle
/// with a fixed payload to return.
struct ScriptedRuntime {
    result: QueryResult,
    captured_sql: Mutex<Option<String>>,
    captured_max_rows: Mutex<Option<u32>>,
}

#[async_trait]
impl RuntimeHandle for ScriptedRuntime {
    async fn execute_sql(&self, sql: &str, max_rows: u32) -> Result<QueryResult, String> {
        *self.captured_sql.lock().await = Some(sql.to_string());
        *self.captured_max_rows.lock().await = Some(max_rows);
        Ok(self.result.clone())
    }
}

#[derive(Default)]
struct CapturedState {
    last_hello: Option<proto::Hello>,
    last_result: Option<proto::CommandResult>,
    last_audit: Option<proto::EventLog>,
}

#[derive(Clone, Default)]
struct MockServer {
    state: Arc<Mutex<CapturedState>>,
    script: Arc<Vec<proto::ControlMessage>>,
}

impl MockServer {
    fn new(script: Vec<proto::ControlMessage>) -> Self {
        Self {
            state: Arc::new(Mutex::new(CapturedState::default())),
            script: Arc::new(script),
        }
    }
}

#[async_trait]
impl CloudConnect for MockServer {
    type StreamStream = ReceiverStream<Result<proto::ControlMessage, Status>>;

    async fn stream(
        &self,
        request: Request<Streaming<proto::ClientMessage>>,
    ) -> Result<Response<Self::StreamStream>, Status> {
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel::<Result<proto::ControlMessage, Status>>(16);

        let state = Arc::clone(&self.state);
        let script = Arc::clone(&self.script);

        tokio::spawn(async move {
            let mut sent_script = false;
            while let Ok(Some(msg)) = inbound.message().await {
                match msg.body {
                    Some(proto::client_message::Body::Hello(hello)) => {
                        state.lock().await.last_hello = Some(hello);
                        if !sent_script {
                            for ctrl in script.iter() {
                                if tx.send(Ok(ctrl.clone())).await.is_err() {
                                    return;
                                }
                            }
                            sent_script = true;
                        }
                    }
                    Some(proto::client_message::Body::Result(result)) => {
                        state.lock().await.last_result = Some(result);
                    }
                    Some(proto::client_message::Body::Event(event)) => {
                        if event.kind == "audit" {
                            state.lock().await.last_audit = Some(event);
                        }
                    }
                    Some(_) | None => {
                        // Heartbeats, telemetry, adopt_ack — irrelevant
                        // to these tests.
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

async fn spawn_server(mock: MockServer) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let svc = CloudConnectServer::new(mock);
    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(svc)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });
    // No readiness sleep: the listener is already bound before this task is
    // spawned, so a client can connect immediately — the TCP connection
    // queues in the backlog until tonic starts accepting.
    addr
}

/// Encode record batches into an Arrow IPC stream (the wire shape the
/// real spiced impl produces for RunQuery).
fn encode_ipc(schema: &Schema, batches: &[RecordBatch]) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, schema).expect("ipc writer");
        for b in batches {
            w.write(b).expect("ipc write");
        }
        w.finish().expect("ipc finish");
    }
    buf
}

/// Decode an Arrow IPC stream into (column names, batches) for assertions.
fn decode_ipc(bytes: &[u8]) -> (Vec<String>, Vec<RecordBatch>) {
    let reader = StreamReader::try_new(std::io::Cursor::new(bytes.to_vec()), None).expect("reader");
    let columns = reader
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let batches = reader.collect::<Result<Vec<_>, _>>().expect("batches");
    (columns, batches)
}

/// Build a `QueryResult` carrying an `(id: Int64, name: Utf8)` batch as
/// native Arrow IPC — the same shape the real spiced impl emits.
fn id_name_result(ids: &[i64], names: &[&str], truncated: bool) -> QueryResult {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
        ],
    )
    .expect("batch");
    QueryResult {
        arrow_ipc: encode_ipc(&schema, &[batch]),
        row_count: ids.len() as u64,
        truncated,
    }
}

/// Build a single-column `(id: Int64)` `QueryResult`.
fn id_result(ids: &[i64], truncated: bool) -> QueryResult {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int64Array::from(ids.to_vec()))],
    )
    .expect("batch");
    QueryResult {
        arrow_ipc: encode_ipc(&schema, &[batch]),
        row_count: ids.len() as u64,
        truncated,
    }
}

fn config_with(
    gateway_endpoint: String,
    identity_path: std::path::PathBuf,
    config_dir: std::path::PathBuf,
) -> CloudConnectConfig {
    CloudConnectConfig {
        // The enroll endpoint is never contacted in these tests (identity
        // is pre-seeded), but must be a valid URL.
        enroll_endpoint: "http://127.0.0.1:9".to_string(),
        gateway_endpoint: Some(gateway_endpoint),
        ca_cert_pem: None,
        insecure: true,
        identity_path,
        config_dir,
        adoption_code: None,
        pending_adopt_code_path: None,
        runtime_version: "v0.0.0-test".to_string(),
        heartbeat_interval: Duration::from_secs(30),
        telemetry_interval: Duration::from_mins(1),
        renewal_lead: Duration::from_hours(12),
    }
}

fn preseed_identity(path: &std::path::Path) {
    let identity = runtime_cloud_connect::identity::Identity {
        identifier: "inst_unit_test".to_string(),
        identity_cert_pem: "UNIT-TEST-CERT".to_string(),
        private_key_pem: "UNIT-TEST-KEY".to_string(),
        public_key_pem: "UNIT-TEST-PUB".to_string(),
        ca_bundle_pem: String::new(),
        gateway_addr: String::new(),
        not_after_unix: 0,
    };
    IdentityStore::store(path, &identity).unwrap();
}

#[tokio::test]
async fn run_query_returns_documented_envelope_and_audit() {
    let dir = tempfile::tempdir().unwrap();
    let identity_path = dir.path().join("identity.json");
    preseed_identity(&identity_path);

    // 2 rows of (id, name) — same shape the cloud portal Query tab
    // renders, carried as native Arrow IPC.
    let runtime: Arc<dyn RuntimeHandle> = Arc::new(ScriptedRuntime {
        result: id_name_result(&[1, 2], &["alpha", "beta"], false),
        captured_sql: Mutex::new(None),
        captured_max_rows: Mutex::new(None),
    });

    let run_query_cmd = proto::ControlMessage {
        body: Some(proto::control_message::Body::RunQuery(proto::RunQuery {
            command_id: "cmd-q-1".to_string(),
            sql: "SELECT id, name FROM t".to_string(),
            max_rows: 100,
        })),
    };
    let mock = MockServer::new(vec![run_query_cmd]);
    let mock_state = Arc::clone(&mock.state);
    let addr = spawn_server(mock).await;

    let cfg = config_with(
        format!("http://{addr}"),
        identity_path.clone(),
        dir.path().to_path_buf(),
    );
    let handle = runtime_cloud_connect::CloudConnect::start(cfg, runtime)
        .await
        .expect("start")
        .expect("started");

    // Wait for the CommandResult to land.
    let mut saw_result = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if mock_state.lock().await.last_result.is_some() {
            saw_result = true;
            break;
        }
    }
    assert!(saw_result, "CommandResult should arrive within 5s");

    // Verify CommandResult envelope.
    let state = mock_state.lock().await;
    let result = state.last_result.clone().expect("result");
    assert_eq!(result.command_id, "cmd-q-1");
    assert!(
        result.success,
        "result.success=true, error={}",
        result.error
    );
    // Metadata rides in payload_json; tabular data is native Arrow IPC.
    let meta: Value = serde_json::from_str(&result.payload_json).expect("parse payload");
    assert_eq!(meta["row_count"], 2);
    assert_eq!(meta["truncated"], false);
    // Decode the Arrow IPC and verify columns + values round-tripped.
    let (columns, batches) = decode_ipc(&result.result_arrow_ipc);
    assert_eq!(columns, vec!["id".to_string(), "name".to_string()]);
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 2);
    let batch = &batches[0];
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id col");
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name col");
    assert_eq!(ids.value(0), 1);
    assert_eq!(names.value(0), "alpha");
    assert_eq!(ids.value(1), 2);
    assert_eq!(names.value(1), "beta");

    // Verify audit EventLog.
    let audit = state.last_audit.clone().expect("audit event");
    assert_eq!(audit.kind, "audit");
    assert_eq!(audit.identifier, "inst_unit_test");
    let audit_payload: Value = serde_json::from_str(&audit.event_json).expect("parse audit event");
    assert_eq!(audit_payload["action"], "run_query");
    assert_eq!(audit_payload["command_id"], "cmd-q-1");
    assert_eq!(audit_payload["row_count"], 2);
    assert_eq!(audit_payload["truncated"], false);
    assert_eq!(audit_payload["success"], true);
    // SQL hash must be present but must NOT contain the SQL text.
    let sql_hash = audit_payload["sql_hash"].as_str().expect("sql_hash string");
    assert_eq!(sql_hash.len(), 64, "sha256 hex digest is 64 chars");
    assert!(!sql_hash.contains("SELECT"));
    // duration_ms must be present and a number.
    assert!(audit_payload["duration_ms"].is_u64());
    drop(state);

    handle.shutdown().await;
}

#[tokio::test]
async fn run_query_propagates_truncation_flag() {
    let dir = tempfile::tempdir().unwrap();
    let identity_path = dir.path().join("identity.json");
    preseed_identity(&identity_path);

    let runtime: Arc<dyn RuntimeHandle> = Arc::new(ScriptedRuntime {
        // server-side runtime says we truncated.
        result: id_result(&[1, 2], true),
        captured_sql: Mutex::new(None),
        captured_max_rows: Mutex::new(None),
    });

    let cmd = proto::ControlMessage {
        body: Some(proto::control_message::Body::RunQuery(proto::RunQuery {
            command_id: "cmd-trunc".to_string(),
            sql: "SELECT id FROM big".to_string(),
            max_rows: 2,
        })),
    };
    let mock = MockServer::new(vec![cmd]);
    let mock_state = Arc::clone(&mock.state);
    let addr = spawn_server(mock).await;

    let cfg = config_with(
        format!("http://{addr}"),
        identity_path.clone(),
        dir.path().to_path_buf(),
    );
    let handle = runtime_cloud_connect::CloudConnect::start(cfg, runtime)
        .await
        .expect("start")
        .expect("started");

    let mut saw_result = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if mock_state.lock().await.last_result.is_some() {
            saw_result = true;
            break;
        }
    }
    assert!(saw_result);

    let state = mock_state.lock().await;
    let result = state.last_result.clone().unwrap();
    assert!(result.success);
    let meta: Value = serde_json::from_str(&result.payload_json).unwrap();
    assert_eq!(meta["truncated"], true);
    assert_eq!(meta["row_count"], 2);
    // Data is still present as Arrow IPC even when truncated.
    let (_, batches) = decode_ipc(&result.result_arrow_ipc);
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);

    let audit_payload: Value =
        serde_json::from_str(&state.last_audit.clone().unwrap().event_json).unwrap();
    assert_eq!(audit_payload["truncated"], true);
    assert_eq!(audit_payload["row_count"], 2);
    drop(state);

    handle.shutdown().await;
}

/// When the runtime errors out, the CommandResult must report `success:
/// false` *and* the audit event must record `success: false` without
/// echoing the SQL text or the unredacted error.
#[tokio::test]
async fn run_query_failure_is_safe_and_audited() {
    let dir = tempfile::tempdir().unwrap();
    let identity_path = dir.path().join("identity.json");
    preseed_identity(&identity_path);

    struct ErrRuntime;
    #[async_trait]
    impl RuntimeHandle for ErrRuntime {
        async fn execute_sql(&self, sql: &str, _max_rows: u32) -> Result<QueryResult, String> {
            // Pretend the planner echoed our SQL back in the error —
            // the client must NOT pass that through to the cloud.
            Err(format!("plan error near `{sql}`: table not found"))
        }
    }
    let runtime: Arc<dyn RuntimeHandle> = Arc::new(ErrRuntime);

    let cmd = proto::ControlMessage {
        body: Some(proto::control_message::Body::RunQuery(proto::RunQuery {
            command_id: "cmd-err".to_string(),
            sql: "SELECT * FROM does_not_exist".to_string(),
            max_rows: 100,
        })),
    };
    let mock = MockServer::new(vec![cmd]);
    let mock_state = Arc::clone(&mock.state);
    let addr = spawn_server(mock).await;

    let cfg = config_with(
        format!("http://{addr}"),
        identity_path.clone(),
        dir.path().to_path_buf(),
    );
    let handle = runtime_cloud_connect::CloudConnect::start(cfg, runtime)
        .await
        .expect("start")
        .expect("started");

    let mut saw_result = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if mock_state.lock().await.last_result.is_some() {
            saw_result = true;
            break;
        }
    }
    assert!(saw_result);

    let state = mock_state.lock().await;
    let result = state.last_result.clone().unwrap();
    assert!(!result.success);
    // Sanitized error stays short and excludes the raw SQL token.
    assert!(!result.error.contains("SELECT * FROM does_not_exist"));
    assert!(result.error.len() <= 257);

    let audit_payload: Value =
        serde_json::from_str(&state.last_audit.clone().unwrap().event_json).unwrap();
    assert_eq!(audit_payload["action"], "run_query");
    assert_eq!(audit_payload["success"], false);
    assert_eq!(audit_payload["row_count"], 0);
    // Hash is still emitted even on error.
    assert!(audit_payload["sql_hash"].as_str().unwrap().len() == 64);
    drop(state);

    handle.shutdown().await;
}

/// A `GetPodLogs` command round-trips to a `CommandResult` whose
/// `payload_json` carries the log text **verbatim** — a raw string, NOT a
/// JSON-encoded/quoted value (the gateway relays `payload_json` through as
/// text). The runtime's `tail_lines` argument is forwarded unchanged.
#[tokio::test]
async fn get_pod_logs_returns_verbatim_text_payload() {
    let dir = tempfile::tempdir().unwrap();
    let identity_path = dir.path().join("identity.json");
    preseed_identity(&identity_path);

    struct LogRuntime {
        logs: String,
        captured_tail: Mutex<Option<i64>>,
    }
    #[async_trait]
    impl RuntimeHandle for LogRuntime {
        async fn get_pod_logs(&self, tail_lines: i64) -> Result<String, String> {
            *self.captured_tail.lock().await = Some(tail_lines);
            Ok(self.logs.clone())
        }
    }

    // Deliberately multi-line with characters JSON would escape (quotes,
    // backslash) so a regression that JSON-encodes the payload is obvious.
    let log_text = "2026-07-23T00:00:00Z  INFO spiced: started\n2026-07-23T00:00:01Z  WARN path=\"c:\\x\": retry\n";
    let runtime = Arc::new(LogRuntime {
        logs: log_text.to_string(),
        captured_tail: Mutex::new(None),
    });
    let captured = Arc::clone(&runtime);
    let runtime: Arc<dyn RuntimeHandle> = runtime;

    let cmd = proto::ControlMessage {
        body: Some(proto::control_message::Body::GetPodLogs(
            proto::GetPodLogs {
                command_id: "cmd-logs-1".to_string(),
                namespace: String::new(),
                name: String::new(),
                kind: String::new(),
                pod_name: String::new(),
                tail_lines: 50,
            },
        )),
    };
    let mock = MockServer::new(vec![cmd]);
    let mock_state = Arc::clone(&mock.state);
    let addr = spawn_server(mock).await;

    let cfg = config_with(
        format!("http://{addr}"),
        identity_path.clone(),
        dir.path().to_path_buf(),
    );
    let handle = runtime_cloud_connect::CloudConnect::start(cfg, runtime)
        .await
        .expect("start")
        .expect("started");

    let mut saw_result = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if mock_state.lock().await.last_result.is_some() {
            saw_result = true;
            break;
        }
    }
    assert!(saw_result, "CommandResult should arrive within 5s");

    let state = mock_state.lock().await;
    let result = state.last_result.clone().expect("result");
    assert_eq!(result.command_id, "cmd-logs-1");
    assert!(result.success, "error={}", result.error);
    // The payload is the log text byte-for-byte — not JSON-quoted/escaped.
    assert_eq!(result.payload_json, log_text);
    assert!(result.result_arrow_ipc.is_empty());
    drop(state);

    // tail_lines was forwarded to the runtime unchanged.
    assert_eq!(*captured.captured_tail.lock().await, Some(50));

    handle.shutdown().await;
}

/// When the runtime has no log capture available (e.g. capture layer not
/// installed), `GetPodLogs` returns `success: false` with an explanatory
/// error rather than an empty success — the default `RuntimeHandle` impl.
#[tokio::test]
async fn get_pod_logs_unavailable_is_reported_as_failure() {
    let dir = tempfile::tempdir().unwrap();
    let identity_path = dir.path().join("identity.json");
    preseed_identity(&identity_path);

    // Bare handle: inherits the default get_pod_logs (returns Err).
    struct BareRuntime;
    #[async_trait]
    impl RuntimeHandle for BareRuntime {}
    let runtime: Arc<dyn RuntimeHandle> = Arc::new(BareRuntime);

    let cmd = proto::ControlMessage {
        body: Some(proto::control_message::Body::GetPodLogs(
            proto::GetPodLogs {
                command_id: "cmd-logs-2".to_string(),
                namespace: String::new(),
                name: String::new(),
                kind: String::new(),
                pod_name: String::new(),
                tail_lines: 0,
            },
        )),
    };
    let mock = MockServer::new(vec![cmd]);
    let mock_state = Arc::clone(&mock.state);
    let addr = spawn_server(mock).await;

    let cfg = config_with(
        format!("http://{addr}"),
        identity_path.clone(),
        dir.path().to_path_buf(),
    );
    let handle = runtime_cloud_connect::CloudConnect::start(cfg, runtime)
        .await
        .expect("start")
        .expect("started");

    let mut saw_result = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if mock_state.lock().await.last_result.is_some() {
            saw_result = true;
            break;
        }
    }
    assert!(saw_result);

    let state = mock_state.lock().await;
    let result = state.last_result.clone().unwrap();
    assert!(!result.success);
    assert!(!result.error.is_empty());
    assert!(result.payload_json.is_empty());
    drop(state);

    handle.shutdown().await;
}

/// A `GetStatus` command round-trips to a `CommandResult` whose
/// `payload_json` is a JSON status **document** (unlike GetPodLogs, which is
/// raw text). The document must carry the top-level `phase`/`reason` the
/// control plane parses.
#[tokio::test]
async fn get_status_returns_json_status_document() {
    let dir = tempfile::tempdir().unwrap();
    let identity_path = dir.path().join("identity.json");
    preseed_identity(&identity_path);

    struct StatusRuntime;
    #[async_trait]
    impl RuntimeHandle for StatusRuntime {
        async fn get_status(&self) -> Result<Value, String> {
            Ok(serde_json::json!({
                "phase": "Ready",
                "reason": "2/2 components ready",
                "ready": true,
                "restart_pending": false,
            }))
        }
    }
    let runtime: Arc<dyn RuntimeHandle> = Arc::new(StatusRuntime);

    let cmd = proto::ControlMessage {
        body: Some(proto::control_message::Body::GetStatus(proto::GetStatus {
            command_id: "cmd-status-1".to_string(),
            // Standalone: targeting fields are empty and ignored by the runtime.
            namespace: String::new(),
            kind: String::new(),
            name: String::new(),
        })),
    };
    let mock = MockServer::new(vec![cmd]);
    let mock_state = Arc::clone(&mock.state);
    let addr = spawn_server(mock).await;

    let cfg = config_with(
        format!("http://{addr}"),
        identity_path.clone(),
        dir.path().to_path_buf(),
    );
    let handle = runtime_cloud_connect::CloudConnect::start(cfg, runtime)
        .await
        .expect("start")
        .expect("started");

    let mut saw_result = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if mock_state.lock().await.last_result.is_some() {
            saw_result = true;
            break;
        }
    }
    assert!(saw_result);

    let state = mock_state.lock().await;
    let result = state.last_result.clone().unwrap();
    assert_eq!(result.command_id, "cmd-status-1");
    assert!(result.success, "error={}", result.error);
    // payload_json is a JSON object (not raw text) with the parseable phase.
    let doc: Value = serde_json::from_str(&result.payload_json).expect("status doc is JSON");
    assert_eq!(doc["phase"], "Ready");
    assert_eq!(doc["reason"], "2/2 components ready");
    assert_eq!(doc["ready"], true);
    drop(state);

    handle.shutdown().await;
}
