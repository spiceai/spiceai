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

//! Integration tests for the `RunQuery` dispatch path.
//!
//! These exercise the cloud-connect client driver end-to-end against an
//! in-process tonic mock server. They cover:
//!
//! - A RunQuery → CommandResult round-trip whose `payload_json`
//!   deserializes into the documented `{columns, rows, row_count,
//!   truncated}` envelope.
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
    endpoint: String,
    identity_path: std::path::PathBuf,
    config_dir: std::path::PathBuf,
) -> CloudConnectConfig {
    CloudConnectConfig {
        endpoint,
        ca_cert_pem: None,
        insecure: true,
        identity_path,
        config_dir,
        adoption_code: None,
        pending_adopt_code_path: None,
        runtime_version: "v0.0.0-test".to_string(),
    }
}

fn preseed_identity(path: &std::path::Path) {
    let identity = runtime_cloud_connect::identity::Identity {
        identifier: "inst_unit_test".to_string(),
        identity_cert_pem: "UNIT-TEST-CERT".to_string(),
        private_key_pem: "UNIT-TEST-KEY".to_string(),
        public_key_pem: "UNIT-TEST-PUB".to_string(),
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
