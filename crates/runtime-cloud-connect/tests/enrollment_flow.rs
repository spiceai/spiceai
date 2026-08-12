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
//! - Pre-runtime enrollment (`enroll_now`, the `spiced --token` core):
//!   enrollment key + CSR + host facts → HTTP enroll → identity persisted
//!   (with the issued gateway address) → the gRPC stream opens against the
//!   gateway with the assigned identifier and no credential of its own; a
//!   subsequent `Adopt` marker is acknowledged with `AdoptAck` + an `OK`
//!   `CommandResult`.
//! - Terminal enroll rejection (an unknown/consumed key): `enroll_now`
//!   stops after one request and creates no identity.
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
use rcgen::{
    BasicConstraints, CertificateParams, CertificateSigningRequestParams, DnType, IsCa, Issuer,
    KeyPair, KeyUsagePurpose,
};
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

const ENROLLMENT_KEY: &str = "spice-enroll-flow00000000000000000000000000aa";
const ASSIGNED_ID: &str = "inst_unit_test";

fn reconnect_identity(identifier: &str, gateway_addr: String) -> runtime_cloud_connect::Identity {
    let key_pair = KeyPair::generate().expect("generate reconnect identity key");
    let certificate = CertificateParams::new(Vec::<String>::new())
        .expect("build reconnect certificate parameters")
        .self_signed(&key_pair)
        .expect("sign reconnect certificate");
    runtime_cloud_connect::Identity {
        identifier: identifier.to_string(),
        identity_cert_pem: certificate.pem(),
        private_key_pem: key_pair.serialize_pem(),
        public_key_pem: key_pair.public_key_pem(),
        ca_bundle_pem: String::new(),
        gateway_addr,
        not_after_unix: None,
        app_id: None,
        enc_private_key_pem: String::new(),
        enc_public_key_pem: String::new(),
        enc_previous_private_key_pem: String::new(),
        cache_key_b64: String::new(),
    }
}

#[tokio::test]
async fn start_rejects_a_persisted_public_key_that_does_not_match_the_mtls_identity() {
    let dir = tempfile::tempdir().expect("create tempdir");
    let identity_path = dir.path().join("identity.json");
    let mut identity = reconnect_identity("inst_corrupt", "127.0.0.1:9".to_string());
    identity.public_key_pem = KeyPair::generate()
        .expect("generate mismatched persisted public key")
        .public_key_pem();
    IdentityStore::store(&identity_path, &identity).expect("store corrupt identity");

    let config = enroll_config(
        "127.0.0.1:9".parse().expect("parse unused endpoint"),
        dir.path(),
    );
    let runtime: Arc<dyn RuntimeHandle> =
        Arc::new(runtime_cloud_connect::handlers::NoopRuntimeHandle);
    let error = match runtime_cloud_connect::CloudConnect::start(config, runtime).await {
        Ok(_) => panic!("startup must fail closed before advertising a mismatched public key"),
        Err(error) => error,
    };
    assert!(
        matches!(
            error,
            runtime_cloud_connect::Error::IdentityUnusable {
                reason: "the client identity public and private keys do not match",
                ..
            }
        ),
        "{error}"
    );
}

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

/// What the last apply wrote, as the handle saw it.
#[derive(Clone)]
struct AppliedSpicepod {
    path: PathBuf,
    spicepod_yaml: String,
    app_id: Option<String>,
}

struct CapturedRuntime {
    applied: Arc<Mutex<Option<AppliedSpicepod>>>,
}

struct AttachmentRuntime {
    applied: Arc<Mutex<Vec<Option<String>>>>,
}

#[async_trait]
impl RuntimeHandle for AttachmentRuntime {
    fn supports(&self, capability: Capability) -> bool {
        capability == Capability::AttachApp
    }

    async fn attach_app(&self, app_id: Option<&str>) -> Result<serde_json::Value, CommandError> {
        self.applied.lock().await.push(app_id.map(str::to_string));
        Ok(serde_json::json!({ "app_id": app_id }))
    }
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
        *self.applied.lock().await = Some(AppliedSpicepod {
            path: path.clone(),
            spicepod_yaml: deployment.spicepod_yaml.to_string(),
            app_id: deployment.app_id.map(str::to_string),
        });
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
    /// When set, every request is rejected with this (status, code, error).
    reject: Option<(u16, &'static str, &'static str)>,
    ca: Arc<EnrollCa>,
}

struct EnrollCa {
    certificate_pem: String,
    issuer: Issuer<'static, KeyPair>,
}

impl EnrollCa {
    fn new() -> Self {
        let key = KeyPair::generate().expect("generate enrollment test CA key");
        let mut parameters = CertificateParams::default();
        parameters
            .distinguished_name
            .push(DnType::CommonName, "Cloud Connect Flow Test CA");
        parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        parameters.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let certificate = parameters
            .self_signed(&key)
            .expect("self-sign enrollment test CA");
        Self {
            certificate_pem: certificate.pem(),
            issuer: Issuer::new(parameters, key),
        }
    }

    fn sign_csr(&self, csr_pem: &str) -> String {
        CertificateSigningRequestParams::from_pem(csr_pem)
            .expect("parse enrollment CSR")
            .signed_by(&self.issuer)
            .expect("sign enrollment CSR")
            .pem()
    }
}

fn not_after_in(hours: i64) -> String {
    (chrono::Utc::now() + chrono::Duration::hours(hours)).to_rfc3339()
}

async fn enroll_handler(
    State(state): State<EnrollMockState>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    state.requests.lock().await.push(body.clone());
    if let Some((status, code, error)) = state.reject {
        return (
            StatusCode::from_u16(status).expect("valid status"),
            Json(serde_json::json!({ "code": code, "error": error, "retryable": false })),
        );
    }
    let identity_certificate = state
        .ca
        .sign_csr(body["csr_pem"].as_str().expect("CSR is a string"));
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "instance_id": ASSIGNED_ID,
            "identity_cert_pem": identity_certificate,
            "ca_bundle_pem": state.ca.certificate_pem.clone(),
            "gateway_addr": state.gateway_addr,
            "not_after": not_after_in(24),
            "organization": {"id": 7, "name": "unit-org"},
            "portal": {"new_project_url": "https://cloud.test/unit-org/new?instance=inst_unit_test"},
            "attachment": null,
        })),
    )
}

/// Serve the mock enroll endpoint on an ephemeral port; returns its address
/// and the captured request log.
async fn spawn_enroll_server(
    gateway_addr: String,
    reject: Option<(u16, &'static str, &'static str)>,
) -> (SocketAddr, Arc<Mutex<Vec<Value>>>) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let state = EnrollMockState {
        requests: Arc::clone(&requests),
        gateway_addr,
        reject,
        ca: Arc::new(EnrollCa::new()),
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

fn enroll_config(enroll_addr: SocketAddr, dir: &std::path::Path) -> CloudConnectConfig {
    CloudConnectConfig {
        enroll_endpoint: format!("http://{enroll_addr}"),
        // No override: the stream must connect to the gateway_addr issued
        // by the enroll response (http:// scheme because insecure=true).
        gateway_endpoint: None,
        ca_cert_pem: None,
        insecure: true,
        identity_path: dir.join("identity.json"),
        config_dir: dir.to_path_buf(),
        instance_region: None,
        runtime_version: "v0.0.0-test".to_string(),
        heartbeat_interval: Duration::from_secs(30),
        telemetry_interval: Duration::from_mins(1),
        metrics_interval: Duration::from_secs(30),
        renewal_lead: Duration::from_mins(1),
        query_deadline: Duration::from_mins(1),
    }
}

fn token_authority() -> runtime_cloud_connect::EnrollmentAuthority {
    runtime_cloud_connect::EnrollmentAuthority::Token {
        key: runtime_cloud_connect::EnrollmentKey::parse(ENROLLMENT_KEY)
            .expect("test key is canonical"),
        expected_org: None,
    }
}

fn quick_retry() -> runtime_cloud_connect::RetryPolicy {
    runtime_cloud_connect::RetryPolicy {
        deadline: Duration::from_secs(10),
    }
}

#[tokio::test]
async fn pre_runtime_enroll_persists_identity_and_connects() {
    let dir = tempfile::tempdir().unwrap();
    let identity_path = dir.path().join("identity.json");

    // Gateway mock: send an Adopt trust/marker after Hello (the portal
    // admin confirmed the instance). Post-DR-025 it carries no certificate.
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

    let config = enroll_config(enroll_addr, dir.path());

    // Enrollment happens BEFORE the client exists — the `spiced --token`
    // sequence — and its return means the identity is durable.
    let outcome = runtime_cloud_connect::enroll_now(&config, &token_authority(), quick_retry())
        .await
        .expect("enrollment succeeds");
    let runtime_cloud_connect::EnrollNowOutcome::Enrolled { metadata, .. } = outcome else {
        panic!("a fresh directory must enroll");
    };
    assert_eq!(metadata.organization.name, "unit-org");
    assert!(identity_path.exists(), "identity durable before the client");

    let runtime: Arc<dyn RuntimeHandle> =
        Arc::new(runtime_cloud_connect::handlers::NoopRuntimeHandle);
    let handle = runtime_cloud_connect::CloudConnect::start(config, runtime)
        .await
        .expect("start")
        .expect("started");

    let identity = IdentityStore::load_optional(&identity_path)
        .expect("load identity")
        .expect("identity present");
    assert_eq!(identity.identifier, ASSIGNED_ID);
    assert!(identity.identity_cert_pem.contains("BEGIN CERTIFICATE"));
    assert!(identity.public_key_pem.contains("PUBLIC KEY"));
    assert!(identity.private_key_pem.contains("PRIVATE KEY"));
    assert_eq!(identity.gateway_addr, gateway_addr.to_string());
    assert_eq!(
        identity.ca_bundle_pem.matches("BEGIN CERTIFICATE").count(),
        1,
        "enroll ca_bundle_pem should be persisted into identity.json"
    );
    assert!(
        identity.not_after_unix.is_some_and(|secs| secs > 0),
        "not_after must be parsed"
    );

    // The enroll request carried the canonical contract shape: kind + token
    // + csr_pem + host facts nested under `instance`.
    let requests = enroll_requests.lock().await.clone();
    assert_eq!(requests.len(), 1, "exactly one enroll request");
    let body = &requests[0];
    assert_eq!(body["kind"], "standalone");
    assert_eq!(body["token"], ENROLLMENT_KEY);
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
async fn a_terminal_rejection_creates_no_identity_and_stops_immediately() {
    let dir = tempfile::tempdir().unwrap();
    let identity_path = dir.path().join("identity.json");

    // The cloud terminally rejects the key (unknown/consumed). No gateway
    // is involved.
    let (enroll_addr, enroll_requests) = spawn_enroll_server(
        String::new(),
        Some((401, "invalid_token", "unknown enrollment key")),
    )
    .await;

    let config = enroll_config(enroll_addr, dir.path());
    let err = runtime_cloud_connect::enroll_now(&config, &token_authority(), quick_retry())
        .await
        .expect_err("a terminal rejection fails the bootstrap");
    assert!(
        matches!(err, runtime_cloud_connect::EnrollNowError::Rejected { .. }),
        "{err}"
    );

    // No identity was created, and no retry was attempted (401 is
    // authoritative — retrying a dead key cannot succeed). A client started
    // afterwards finds no identity and stays disabled.
    assert!(!identity_path.exists(), "no identity on rejection");
    assert_eq!(
        enroll_requests.lock().await.len(),
        1,
        "a terminal rejection must not be retried"
    );

    let runtime: Arc<dyn RuntimeHandle> =
        Arc::new(runtime_cloud_connect::handlers::NoopRuntimeHandle);
    let started = runtime_cloud_connect::CloudConnect::start(config, runtime)
        .await
        .expect("start succeeds");
    assert!(
        started.is_none(),
        "with no identity the client must stay disabled"
    );
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
                app_id: "4002".to_string(),
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
    let identity = reconnect_identity("inst_pre_enrolled", addr.to_string());
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
        instance_region: None,
        runtime_version: "v0.0.0-test".to_string(),
        heartbeat_interval: Duration::from_secs(30),
        telemetry_interval: Duration::from_mins(1),
        metrics_interval: Duration::from_secs(30),
        renewal_lead: Duration::from_hours(12),
        query_deadline: Duration::from_mins(1),
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

    let written = captured.lock().await.clone().unwrap();
    assert_eq!(written.spicepod_yaml, yaml);
    assert!(written.path.exists(), "file should be on disk");
    // The runtime has no other way to learn its app, and withholds metrics
    // entirely until this arrives.
    assert_eq!(written.app_id.as_deref(), Some("4002"));

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
    assert_eq!(hello.identifier, "inst_pre_enrolled");
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

#[tokio::test]
async fn attach_app_applies_complete_state_and_rejects_empty_ids() {
    let dir = tempfile::tempdir().expect("create tempdir");
    let identity_path = dir.path().join("identity.json");
    let commands = [Some("4002"), None, Some("3387"), Some("")]
        .into_iter()
        .enumerate()
        .map(|(index, app_id)| proto::ControlMessage {
            command_id: format!("cmd-attach-{index}"),
            target: None,
            body: Some(proto::control_message::Body::AttachApp(proto::AttachApp {
                app_id: app_id.map(str::to_string),
            })),
        })
        .collect();
    let mock = MockServer::new(commands);
    let mock_state = Arc::clone(&mock.state);
    let addr = spawn_server(mock).await;
    IdentityStore::store(
        &identity_path,
        &reconnect_identity("inst_attachment", addr.to_string()),
    )
    .expect("store identity");

    let applied = Arc::new(Mutex::new(Vec::new()));
    let runtime: Arc<dyn RuntimeHandle> = Arc::new(AttachmentRuntime {
        applied: Arc::clone(&applied),
    });
    let config = CloudConnectConfig {
        enroll_endpoint: "http://127.0.0.1:9".to_string(),
        gateway_endpoint: None,
        ca_cert_pem: None,
        insecure: true,
        identity_path,
        config_dir: dir.path().to_path_buf(),
        instance_region: None,
        runtime_version: "v0.0.0-test".to_string(),
        heartbeat_interval: Duration::from_secs(30),
        telemetry_interval: Duration::from_mins(1),
        metrics_interval: Duration::from_secs(30),
        renewal_lead: Duration::from_hours(12),
        query_deadline: Duration::from_mins(1),
    };
    let handle = runtime_cloud_connect::CloudConnect::start(config, runtime)
        .await
        .expect("start")
        .expect("started");

    for _ in 0..50 {
        if mock_state
            .lock()
            .await
            .last_result
            .as_ref()
            .is_some_and(|result| result.command_id == "cmd-attach-3")
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        applied.lock().await.as_slice(),
        [Some("4002".to_string()), None, Some("3387".to_string())]
    );
    let state = mock_state.lock().await;
    assert_eq!(
        state
            .last_hello
            .as_ref()
            .expect("server saw Hello")
            .capabilities,
        ["attach_app".to_string()]
    );
    let result = state.last_result.as_ref().expect("server saw a result");
    assert_eq!(result.command_id, "cmd-attach-3");
    assert_eq!(result.code, proto::ResultCode::InvalidArgument as i32);
    drop(state);

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

    let identity = reconnect_identity("inst_pre_enrolled", addr.to_string());
    IdentityStore::store(&identity_path, &identity).unwrap();

    let config = CloudConnectConfig {
        enroll_endpoint: "http://127.0.0.1:9".to_string(),
        gateway_endpoint: None,
        ca_cert_pem: None,
        insecure: true,
        identity_path: identity_path.clone(),
        config_dir: dir.path().to_path_buf(),
        instance_region: None,
        runtime_version: "v0.0.0-test".to_string(),
        heartbeat_interval: Duration::from_secs(30),
        telemetry_interval: Duration::from_mins(1),
        metrics_interval: Duration::from_secs(30),
        renewal_lead: Duration::from_hours(12),
        query_deadline: Duration::from_mins(1),
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
