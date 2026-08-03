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

//! Integration tests for the Spice Cloud Connect client driver.
//!
//! These spin up an in-process axum server that mocks the cloud enroll
//! endpoint (`/v1/cloud-connect/enroll`) plus an in-process tonic server
//! that speaks the `spice.cloud.v1.CloudConnect` protocol (the gateway),
//! then exercise:
//!
//! - Out-of-band enrollment: adoption code + CSR + host facts → HTTP
//!   enroll → identity persisted (with the issued gateway address) → the
//!   gRPC stream opens against the gateway with the assigned identifier
//!   and no credential of its own; a subsequent `Adopt` marker is
//!   acknowledged with `AdoptAck` + an `OK` `CommandResult`.
//! - Permanent enroll rejection (consumed/expired code): the driver
//!   discards the staged code and exits without creating an identity.
//! - `ApplySpicepod` round-trip: server sends ApplySpicepod → client
//!   writes the YAML to disk and replies with success.

#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::doc_markdown,
    clippy::struct_field_names,
    reason = "integration-test harness — readability over lint strictness"
)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use runtime_cloud_connect::config::CloudConnectConfig;
use runtime_cloud_connect::handlers::{
    ApplyOutcome, Capability, CommandError, RuntimeHandle, SpicepodDeployment,
};
use runtime_cloud_connect::identity::IdentityStore;
use runtime_cloud_connect::proto;
use runtime_cloud_connect::proto::cloud_connect_server::{CloudConnect, CloudConnectServer};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

const ADOPTION_CODE: &str = "SPICE-ADOPT-AAAAA-BBBBB";
const ASSIGNED_ID: &str = "inst_unit_test";

#[derive(Default)]
struct CapturedState {
    last_hello: Option<proto::Hello>,
    last_adopt_ack: Option<proto::AdoptAck>,
    last_result: Option<proto::CommandResult>,
}

#[derive(Clone, Default)]
struct MockServer {
    state: Arc<Mutex<CapturedState>>,
    /// Behavior: list of `ControlMessage`s to send to the client once a
    /// `Hello` arrives.
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
                        state.lock().await.last_hello = Some(hello.clone());
                        // After Hello, deliver the script to the client.
                        if !sent_script {
                            for ctrl in script.iter() {
                                if tx.send(Ok(ctrl.clone())).await.is_err() {
                                    return;
                                }
                            }
                            sent_script = true;
                        }
                    }
                    Some(proto::client_message::Body::AdoptAck(ack)) => {
                        state.lock().await.last_adopt_ack = Some(ack);
                    }
                    Some(proto::client_message::Body::Result(result)) => {
                        state.lock().await.last_result = Some(result);
                    }
                    Some(_) => {
                        // Heartbeats, telemetry, events — ignore.
                    }
                    None => break,
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

struct CapturedRuntime {
    applied: Arc<Mutex<Option<(PathBuf, String)>>>,
}

#[async_trait]
impl RuntimeHandle for CapturedRuntime {
    fn supports(&self, capability: Capability) -> bool {
        capability == Capability::ApplySpicepod
    }

    async fn apply_spicepod(
        &self,
        deployment: SpicepodDeployment<'_>,
    ) -> Result<ApplyOutcome, CommandError> {
        let path = deployment
            .config_dir
            .join(runtime_cloud_connect::config::CLOUD_MANAGED_SPICEPOD_FILE);
        std::fs::create_dir_all(deployment.config_dir)
            .map_err(|e| CommandError::failed(e.to_string()))?;
        std::fs::write(&path, deployment.spicepod_yaml)
            .map_err(|e| CommandError::failed(e.to_string()))?;
        *self.applied.lock().await = Some((path.clone(), deployment.spicepod_yaml.to_string()));
        // `settled`, not `exit_to_apply`: this handle has no process to restart,
        // and asking the client to exit would take the test process with it.
        Ok(ApplyOutcome::settled(
            serde_json::json!({ "path": path.display().to_string() }),
        ))
    }
}

async fn spawn_server(mock: MockServer) -> SocketAddr {
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

// --------------------------------------------------------------------------
// Mock cloud enroll endpoint (plain HTTP; the enroll contract is HTTPS in
// production, which is reqwest's standard path and not under test here).
// --------------------------------------------------------------------------

#[derive(Clone)]
struct EnrollMockState {
    /// Captured request bodies, in arrival order.
    requests: Arc<Mutex<Vec<Value>>>,
    /// `gateway_addr` returned on success.
    gateway_addr: String,
    /// When set, every request is rejected with this (status, error).
    reject: Option<(u16, &'static str)>,
}

fn not_after_in(hours: i64) -> String {
    (chrono::Utc::now() + chrono::Duration::hours(hours)).to_rfc3339()
}

async fn enroll_handler(
    State(state): State<EnrollMockState>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    state.requests.lock().await.push(body);
    if let Some((status, error)) = state.reject {
        return (
            StatusCode::from_u16(status).expect("valid status"),
            Json(serde_json::json!({ "error": error })),
        );
    }
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "instance_id": ASSIGNED_ID,
            "identity_cert_pem":
                "-----BEGIN CERTIFICATE-----\nUNIT-TEST\n-----END CERTIFICATE-----\n",
            "ca_bundle_pem":
                "-----BEGIN CERTIFICATE-----\nUNIT-TEST-CA\n-----END CERTIFICATE-----\n",
            "gateway_addr": state.gateway_addr,
            "not_after": not_after_in(24),
        })),
    )
}

/// Serve the mock enroll endpoint on an ephemeral port; returns its address
/// and the captured request log.
async fn spawn_enroll_server(
    gateway_addr: String,
    reject: Option<(u16, &'static str)>,
) -> (SocketAddr, Arc<Mutex<Vec<Value>>>) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let state = EnrollMockState {
        requests: Arc::clone(&requests),
        gateway_addr,
        reject,
    };
    let app = Router::new()
        .route("/v1/cloud-connect/enroll", post(enroll_handler))
        .with_state(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind http");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (addr, requests)
}

fn enroll_config(
    enroll_addr: SocketAddr,
    dir: &std::path::Path,
    pending_code_path: Option<PathBuf>,
) -> CloudConnectConfig {
    CloudConnectConfig {
        enroll_endpoint: format!("http://{enroll_addr}"),
        // No override: the stream must connect to the gateway_addr issued
        // by the enroll response (http:// scheme because insecure=true).
        gateway_endpoint: None,
        ca_cert_pem: None,
        insecure: true,
        identity_path: dir.join("identity.json"),
        config_dir: dir.to_path_buf(),
        adoption_code: Some(ADOPTION_CODE.to_string()),
        pending_adopt_code_path: pending_code_path,
        adopt_app_name: None,
        adopt_create_app: false,
        instance_region: None,
        runtime_version: "v0.0.0-test".to_string(),
        heartbeat_interval: Duration::from_secs(30),
        telemetry_interval: Duration::from_mins(1),
        renewal_lead: Duration::from_mins(1),
    }
}

#[tokio::test]
async fn out_of_band_enroll_persists_identity_and_connects() {
    let dir = tempfile::tempdir().unwrap();
    let identity_path = dir.path().join("identity.json");
    let pending_path = dir.path().join("pending-adopt-code");
    std::fs::write(&pending_path, ADOPTION_CODE).expect("stage pending code");

    // Gateway mock: send an Adopt trust/marker after Hello (the portal
    // admin clicked Adopt). Post-DR-025 it carries no certificate.
    let adopt_cmd = proto::ControlMessage {
        command_id: "cmd-adopt-1".to_string(),
        target: None,
        body: Some(proto::control_message::Body::Adopt(proto::Adopt {
            assigned_identifier: Some(ASSIGNED_ID.to_string()),
        })),
    };
    let mock = MockServer::new(vec![adopt_cmd]);
    let mock_state = Arc::clone(&mock.state);
    let gateway_addr = spawn_server(mock).await;

    let (enroll_addr, enroll_requests) = spawn_enroll_server(gateway_addr.to_string(), None).await;

    let config = enroll_config(enroll_addr, dir.path(), Some(pending_path.clone()));

    let runtime: Arc<dyn RuntimeHandle> =
        Arc::new(runtime_cloud_connect::handlers::NoopRuntimeHandle);
    let handle = runtime_cloud_connect::CloudConnect::start(config, runtime)
        .await
        .expect("start")
        .expect("started");

    // Wait for enrollment to persist the identity.
    let mut adopted = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if identity_path.exists() {
            adopted = true;
            break;
        }
    }
    assert!(adopted, "identity file should be created within 5s");

    let identity = IdentityStore::load_optional(&identity_path)
        .expect("load identity")
        .expect("identity present");
    assert_eq!(identity.identifier, ASSIGNED_ID);
    assert!(identity.identity_cert_pem.contains("UNIT-TEST"));
    assert!(identity.public_key_pem.contains("PUBLIC KEY"));
    assert!(identity.private_key_pem.contains("PRIVATE KEY"));
    assert_eq!(identity.gateway_addr, gateway_addr.to_string());
    assert!(
        identity.ca_bundle_pem.contains("UNIT-TEST-CA"),
        "enroll ca_bundle_pem should be persisted into identity.json"
    );
    assert!(
        identity.not_after_unix.is_some_and(|secs| secs > 0),
        "not_after must be parsed"
    );

    // The enroll request carried the contract shape: adoption_code +
    // csr_pem + host facts nested under `instance`.
    let requests = enroll_requests.lock().await.clone();
    assert_eq!(requests.len(), 1, "exactly one enroll request");
    let body = &requests[0];
    assert_eq!(body["adoption_code"], ADOPTION_CODE);
    assert!(
        body["csr_pem"]
            .as_str()
            .unwrap()
            .contains("CERTIFICATE REQUEST"),
        "enroll must carry a PKCS#10 CSR"
    );
    let instance = &body["instance"];
    assert_eq!(instance["fingerprint"].as_str().unwrap().len(), 64);
    assert!(!instance["hostname"].as_str().unwrap().is_empty());
    assert!(!instance["os"].as_str().unwrap().is_empty());
    assert!(!instance["arch"].as_str().unwrap().is_empty());
    assert_eq!(instance["runtime_version"], "v0.0.0-test");

    // The staged single-use code is discarded after the cloud consumed it.
    let mut code_discarded = false;
    for _ in 0..50 {
        if !pending_path.exists() {
            code_discarded = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(code_discarded, "pending code must be removed after enroll");

    // Server should have received the Hello, AdoptAck, and a successful
    // CommandResult. The CommandResult lands last, so poll for it (rather
    // than sleeping a fixed duration) to avoid flakiness on slow CI.
    let mut result_seen = false;
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if mock_state.lock().await.last_result.is_some() {
            result_seen = true;
            break;
        }
    }
    assert!(result_seen, "server should see CommandResult within ~3s");

    let s = mock_state.lock().await;
    let hello = s.last_hello.clone().expect("server saw Hello");
    assert_eq!(hello.instance_kind, proto::InstanceKind::Standalone as i32);
    // Enroll-first contract: by the time the stream opens, the identity is
    // held — the Hello names the instance and carries no credential at all
    // (the client certificate is the authN; certless contact is gone).
    assert_eq!(hello.identifier, ASSIGNED_ID);
    assert_eq!(
        hello.protocol_version,
        runtime_cloud_connect::PROTOCOL_VERSION,
        "Hello must announce the protocol revision it implements"
    );
    assert!(
        hello.capabilities.is_empty(),
        "the no-op handle supports nothing, so the Hello must advertise nothing"
    );

    let ack = s.last_adopt_ack.clone().expect("server saw AdoptAck");
    assert_eq!(ack.identifier, ASSIGNED_ID);
    assert!(ack.identity_pubkey_pem.contains("PUBLIC KEY"));

    let result = s.last_result.clone().expect("server saw CommandResult");
    assert_eq!(result.command_id, "cmd-adopt-1");
    assert_eq!(
        result.code,
        proto::ResultCode::Ok as i32,
        "{}",
        result.message
    );
    drop(s);

    handle.shutdown().await;
}

#[tokio::test]
async fn enroll_rejection_discards_code_and_exits_without_identity() {
    let dir = tempfile::tempdir().unwrap();
    let identity_path = dir.path().join("identity.json");
    let pending_path = dir.path().join("pending-adopt-code");
    std::fs::write(&pending_path, ADOPTION_CODE).expect("stage pending code");

    // The cloud authoritatively rejects the code (single-use, already
    // consumed). No gateway is involved.
    let (enroll_addr, enroll_requests) =
        spawn_enroll_server(String::new(), Some((401, "Adoption code already used"))).await;

    let config = enroll_config(enroll_addr, dir.path(), Some(pending_path.clone()));
    let runtime: Arc<dyn RuntimeHandle> =
        Arc::new(runtime_cloud_connect::handlers::NoopRuntimeHandle);
    let handle = runtime_cloud_connect::CloudConnect::start(config, runtime)
        .await
        .expect("start")
        .expect("started");

    // The permanent rejection removes the staged (dead) code so a restart
    // does not replay it.
    let mut code_discarded = false;
    for _ in 0..50 {
        if !pending_path.exists() {
            code_discarded = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        code_discarded,
        "a permanently-rejected code must be discarded"
    );

    // No identity was created, and no retry was attempted (401 is
    // authoritative — retrying a consumed code cannot succeed).
    assert!(!identity_path.exists(), "no identity on rejection");
    assert_eq!(
        enroll_requests.lock().await.len(),
        1,
        "permanent rejection must not be retried"
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn apply_spicepod_writes_file_and_acks() {
    let dir = tempfile::tempdir().unwrap();
    let identity_path = dir.path().join("identity.json");
    let config_dir = dir.path().to_path_buf();

    let yaml = "name: cloud-managed\n";
    let apply_cmd = proto::ControlMessage {
        command_id: "cmd-apply-1".to_string(),
        target: None,
        body: Some(proto::control_message::Body::ApplySpicepod(
            proto::ApplySpicepod {
                spicepod_yaml: yaml.to_string(),
                sealed_secret_payload: None,
                deployment_version: None,
            },
        )),
    };
    let mock = MockServer::new(vec![apply_cmd]);
    let mock_state = Arc::clone(&mock.state);
    let addr = spawn_server(mock).await;

    // Pre-seed identity so the client connects in identity mode, pointing
    // its issued gateway_addr at the mock. The transport is insecure (h2c)
    // here, so the cert/key PEMs are never used for a real handshake —
    // this test isolates the ApplySpicepod dispatch.
    let identity = runtime_cloud_connect::identity::Identity {
        identifier: "inst_pre_adopted".to_string(),
        identity_cert_pem: "PRE-ADOPTED-CERT".to_string(),
        private_key_pem: "PRE-ADOPTED-KEY".to_string(),
        public_key_pem: "PRE-ADOPTED-PUB".to_string(),
        ca_bundle_pem: String::new(),
        gateway_addr: addr.to_string(),
        not_after_unix: None,
        enc_private_key_pem: String::new(),
        enc_public_key_pem: String::new(),
        enc_previous_private_key_pem: String::new(),
        cache_key_b64: String::new(),
    };
    IdentityStore::store(&identity_path, &identity).unwrap();

    let captured = Arc::new(Mutex::new(None));
    let runtime: Arc<dyn RuntimeHandle> = Arc::new(CapturedRuntime {
        applied: Arc::clone(&captured),
    });

    let config = CloudConnectConfig {
        // Never contacted: the identity is pre-seeded and unbounded.
        enroll_endpoint: "http://127.0.0.1:9".to_string(),
        gateway_endpoint: None,
        ca_cert_pem: None,
        insecure: true,
        identity_path: identity_path.clone(),
        config_dir: config_dir.clone(),
        adoption_code: None,
        pending_adopt_code_path: None,
        adopt_app_name: None,
        adopt_create_app: false,
        instance_region: None,
        runtime_version: "v0.0.0-test".to_string(),
        heartbeat_interval: Duration::from_secs(30),
        telemetry_interval: Duration::from_mins(1),
        renewal_lead: Duration::from_hours(12),
    };

    let handle = runtime_cloud_connect::CloudConnect::start(config, runtime)
        .await
        .expect("start")
        .expect("started");

    let mut applied_seen = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if captured.lock().await.is_some() {
            applied_seen = true;
            break;
        }
    }
    assert!(
        applied_seen,
        "runtime should have received ApplySpicepod within 5s"
    );

    let (written_path, written_yaml) = captured.lock().await.clone().unwrap();
    assert_eq!(written_yaml, yaml);
    assert!(written_path.exists(), "file should be on disk");

    // Server should see the CommandResult for the apply. Poll for it with a
    // bounded timeout instead of a fixed sleep so the assertion does not race
    // on loaded CI, mirroring the `applied_seen` wait above.
    let mut result_seen = false;
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if mock_state.lock().await.last_result.is_some() {
            result_seen = true;
            break;
        }
    }
    assert!(result_seen, "server should see CommandResult within ~3s");

    let s = mock_state.lock().await;
    let hello = s.last_hello.clone().expect("server saw Hello");
    assert_eq!(hello.identifier, "inst_pre_adopted");
    assert_eq!(
        hello.capabilities,
        vec!["apply_spicepod".to_string()],
        "Hello must announce exactly what the runtime handle supports"
    );

    let result = s.last_result.clone().expect("server saw CommandResult");
    assert_eq!(result.command_id, "cmd-apply-1");
    assert_eq!(
        result.code,
        proto::ResultCode::Ok as i32,
        "{}",
        result.message
    );
    drop(s);

    handle.shutdown().await;
}

/// Field number of `ControlMessage.command_id`. Taken from the generated
/// descriptor rather than written out, so a renumbering of the contract cannot
/// leave this test encoding a different field than it means to.
fn command_id_field_number() -> u32 {
    let probe = proto::ControlMessage {
        command_id: "x".to_string(),
        target: None,
        body: None,
    };
    let mut encoded = Vec::new();
    prost::Message::encode(&probe, &mut encoded).expect("encode probe");
    let (key, _) = prost::encoding::decode_key(&mut encoded.as_slice()).expect("decode key");
    key
}

/// Hand-encode a `ControlMessage` carrying a command this build has no oneof
/// arm for: the `command_id` field, then an unknown length-delimited field 99
/// standing in for the command a newer control plane would send.
///
/// This is the wire shape of "newer control plane, older client" — the point
/// of the test is that prost drops the unrecognized arm to `body: None` while
/// the envelope keeps the `command_id`, which is what makes the command
/// answerable at all.
fn encode_unknown_command(command_id: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    prost::encoding::encode_key(
        command_id_field_number(),
        prost::encoding::WireType::LengthDelimited,
        &mut buf,
    );
    prost::encoding::encode_varint(command_id.len() as u64, &mut buf);
    buf.extend_from_slice(command_id.as_bytes());
    // Field 99, wire type 2 — the unknown command.
    prost::encoding::encode_key(99, prost::encoding::WireType::LengthDelimited, &mut buf);
    prost::encoding::encode_varint(0, &mut buf);
    buf
}

#[tokio::test]
async fn unknown_command_is_nacked_rather_than_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let identity_path = dir.path().join("identity.json");

    let encoded = encode_unknown_command("cmd-from-the-future");
    let unknown_cmd =
        <proto::ControlMessage as prost::Message>::decode(encoded.as_slice()).expect("decode");
    assert_eq!(unknown_cmd.command_id, "cmd-from-the-future");
    assert!(
        unknown_cmd.body.is_none(),
        "an unrecognized command must decode to an absent body"
    );

    let mock = MockServer::new(vec![unknown_cmd]);
    let mock_state = Arc::clone(&mock.state);
    let addr = spawn_server(mock).await;

    let identity = runtime_cloud_connect::identity::Identity {
        identifier: "inst_pre_adopted".to_string(),
        identity_cert_pem: "PRE-ADOPTED-CERT".to_string(),
        private_key_pem: "PRE-ADOPTED-KEY".to_string(),
        public_key_pem: "PRE-ADOPTED-PUB".to_string(),
        ca_bundle_pem: String::new(),
        gateway_addr: addr.to_string(),
        not_after_unix: None,
        enc_private_key_pem: String::new(),
        enc_public_key_pem: String::new(),
        enc_previous_private_key_pem: String::new(),
        cache_key_b64: String::new(),
    };
    IdentityStore::store(&identity_path, &identity).unwrap();

    let config = CloudConnectConfig {
        enroll_endpoint: "http://127.0.0.1:9".to_string(),
        gateway_endpoint: None,
        ca_cert_pem: None,
        insecure: true,
        identity_path: identity_path.clone(),
        config_dir: dir.path().to_path_buf(),
        adoption_code: None,
        pending_adopt_code_path: None,
        instance_region: None,
        runtime_version: "v0.0.0-test".to_string(),
        heartbeat_interval: Duration::from_secs(30),
        telemetry_interval: Duration::from_mins(1),
        renewal_lead: Duration::from_hours(12),
        adopt_app_name: None,
        adopt_create_app: false,
    };

    let runtime: Arc<dyn RuntimeHandle> =
        Arc::new(runtime_cloud_connect::handlers::NoopRuntimeHandle);
    let handle = runtime_cloud_connect::CloudConnect::start(config, runtime)
        .await
        .expect("start")
        .expect("started");

    let mut result_seen = false;
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if mock_state.lock().await.last_result.is_some() {
            result_seen = true;
            break;
        }
    }
    assert!(
        result_seen,
        "an unrecognized command must be answered, not silently dropped"
    );

    let result = mock_state
        .lock()
        .await
        .last_result
        .clone()
        .expect("server saw CommandResult");
    assert_eq!(result.command_id, "cmd-from-the-future");
    assert_eq!(
        result.code,
        proto::ResultCode::Unsupported as i32,
        "the NACK must say UNSUPPORTED, not a generic failure: {}",
        result.message
    );
    assert!(!result.message.is_empty(), "the NACK must explain itself");

    handle.shutdown().await;
}
