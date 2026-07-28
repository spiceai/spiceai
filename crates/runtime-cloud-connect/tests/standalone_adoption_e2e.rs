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

//! Full standalone-adoption end-to-end suite for Spice Cloud Connect
//! (enroll-first model, DR-025).
//!
//! Unlike `adoption_flow.rs` (which runs the gateway over an insecure h2c
//! channel and returns canned enroll responses), this suite
//! stands up the full split control plane:
//!
//! - a **cloud mock** (axum, HTTP): `/v1/cloud-connect/enroll` atomically
//!   consumes single-use adoption codes and **signs the client's CSR** with
//!   a throwaway CA; `/v1/cloud-connect/renew` verifies the current-key
//!   proof-of-possession signature and re-issues over the new CSR
//!   (rotating the pinned key);
//! - a **gateway** (real TLS tonic server) that **requires mTLS** — the
//!   post-DR-025 gateway holds no CA and rejects certless connections —
//!   and multiplexes control commands.
//!
//! The suite drives the real [`runtime_cloud_connect::CloudConnect`]
//! client through the whole lifecycle:
//!
//! 1. `enrollment` — out-of-band HTTP enroll (code + CSR + host facts) →
//!    identity (leaf + key + CA bundle + gateway addr) persisted →
//!    mTLS stream to the gateway with the assigned identifier.
//! 2. `single-use codes` — a consumed code is rejected and never retried.
//! 3. `identity_reuse_across_restart` — a fresh client with no adoption
//!    code loads the persisted identity and reconnects over mTLS.
//! 4. `heartbeat_and_telemetry_cadence` — periodic frames on their
//!    configured cadences.
//! 5. `apply_spicepod` — the YAML is written and hot-applied.
//! 6. `reconnect_over_mtls` — after the server drops the stream, the
//!    client reconnects, presenting its client certificate again.
//! 7. `renewal` — a short-lived leaf triggers the renewal loop: a fresh
//!    keypair + CSR + PoP signature against `/renew`, and the rotated
//!    identity is persisted.
//! 8. `forget` — the server sends `Forget`, the client clears
//!    `identity.json` and the cloud-connect task exits while the
//!    (simulated) runtime stays up.
//!
//! Determinism: no fixed sleeps for correctness — every wait polls a
//! captured condition with a bounded timeout. Heartbeat / telemetry
//! cadences are sub-second via the config so the suite runs in seconds.

#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::doc_markdown,
    clippy::struct_field_names,
    clippy::items_after_statements,
    clippy::too_many_lines,
    reason = "integration-test harness — readability over lint strictness"
)]

use std::collections::{HashSet, VecDeque};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use base64::Engine as _;
use rcgen::{
    BasicConstraints, CertificateParams, CertificateSigningRequestParams, DnType,
    ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose, PublicKeyData as _, SanType,
};
use runtime_cloud_connect::config::CloudConnectConfig;
use runtime_cloud_connect::handlers::RuntimeHandle;
use runtime_cloud_connect::identity::IdentityStore;
use runtime_cloud_connect::proto;
use runtime_cloud_connect::proto::cloud_connect_server::{CloudConnect, CloudConnectServer};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::{Certificate, Identity as TonicIdentity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming};

const ADOPTION_CODE: &str = "SPICE-ADOPT-E2E11-E2E22";
const ASSIGNED_ID: &str = "inst_e2e_standalone";

// --------------------------------------------------------------------------
// Throwaway PKI: a CA that signs the gateway's server cert AND the client
// CSRs presented to the cloud mock (standing in for the cloud KMS CA).
// --------------------------------------------------------------------------

/// Ensure a process-wide rustls crypto provider is installed. tonic's server
/// builds its `ServerConfig` off the process default, which panics if none is
/// set. Idempotent.
fn ensure_crypto_provider() {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
}

/// A minimal issuing CA plus a server leaf it signed. `issuer` is retained
/// to sign client CSRs on demand.
struct TestCa {
    ca_cert_pem: String,
    server_cert_pem: String,
    server_key_pem: String,
    issuer: Issuer<'static, KeyPair>,
}

impl TestCa {
    fn new() -> Self {
        // Self-signed issuing CA.
        let ca_key = KeyPair::generate().expect("ca keypair");
        let mut ca_params = CertificateParams::default();
        ca_params
            .distinguished_name
            .push(DnType::CommonName, "Spice Test Issuing CA");
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let ca_cert = ca_params.self_signed(&ca_key).expect("self-signed CA");
        let ca_cert_pem = ca_cert.pem();

        // Server leaf (SANs: 127.0.0.1 + localhost) signed by the CA.
        let server_key = KeyPair::generate().expect("server keypair");
        let mut srv_params = CertificateParams::default();
        srv_params
            .distinguished_name
            .push(DnType::CommonName, "spice-test-gateway");
        srv_params.subject_alt_names = vec![
            SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            SanType::DnsName("localhost".try_into().expect("dns san")),
        ];
        srv_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];

        // Build the issuer once (owns the CA params + key) and use it to sign
        // the server leaf now and client CSRs later.
        let issuer = Issuer::new(ca_params, ca_key);
        let server_cert = srv_params
            .signed_by(&server_key, &issuer)
            .expect("sign server leaf");

        Self {
            ca_cert_pem,
            server_cert_pem: server_cert.pem(),
            server_key_pem: server_key.serialize_pem(),
            issuer,
        }
    }

    /// Sign a client-submitted PKCS#10 CSR, returning the leaf PEM plus the
    /// requester's P-256 public key point (for later PoP verification).
    /// `from_pem` verifies the CSR's self-signature, so this only succeeds
    /// if the client genuinely holds the private key it enrolled with.
    fn sign_csr(&self, csr_pem: &str) -> Result<(String, Vec<u8>), rcgen::Error> {
        let csr = CertificateSigningRequestParams::from_pem(csr_pem)?;
        let point = p256_point(csr.public_key.der_bytes());
        let leaf = csr.signed_by(&self.issuer)?;
        Ok((leaf.pem(), point))
    }
}

/// Extract the uncompressed P-256 point (0x04 || X || Y, 65 bytes) from a
/// public-key DER blob — works whether the input is a full SPKI or the raw
/// BIT STRING payload, since the point is always the suffix.
fn p256_point(der: &[u8]) -> Vec<u8> {
    assert!(der.len() >= 65, "public key DER too short for P-256");
    der[der.len() - 65..].to_vec()
}

// --------------------------------------------------------------------------
// Cloud mock: HTTP enroll + renew (state plane), backed by the TestCa.
// --------------------------------------------------------------------------

#[derive(Clone)]
struct CloudMock {
    ca: Arc<TestCa>,
    /// `gateway_addr` (host:port) handed out in enroll responses.
    gateway_addr: String,
    /// Validity (seconds) of issued leaves, as reported in `not_after`.
    leaf_validity_secs: i64,
    /// Unconsumed adoption codes; enroll consumes atomically.
    codes: Arc<Mutex<HashSet<String>>>,
    /// The public key pinned at the last enroll/renew — the only key whose
    /// PoP signature authorizes a rotation (mirrors the cloud's pinning).
    pinned_point: Arc<Mutex<Option<Vec<u8>>>>,
    enroll_requests: Arc<Mutex<Vec<Value>>>,
    renew_requests: Arc<Mutex<Vec<Value>>>,
}

impl CloudMock {
    fn new(ca: Arc<TestCa>, gateway_addr: String, leaf_validity_secs: i64) -> Self {
        let mut codes = HashSet::new();
        codes.insert(ADOPTION_CODE.to_string());
        Self {
            ca,
            gateway_addr,
            leaf_validity_secs,
            codes: Arc::new(Mutex::new(codes)),
            pinned_point: Arc::new(Mutex::new(None)),
            enroll_requests: Arc::new(Mutex::new(Vec::new())),
            renew_requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn not_after(&self) -> String {
        (chrono::Utc::now() + chrono::Duration::seconds(self.leaf_validity_secs)).to_rfc3339()
    }
}

fn error_json(status: StatusCode, message: &str) -> (StatusCode, Json<Value>) {
    (status, Json(serde_json::json!({ "error": message })))
}

async fn mock_enroll(
    State(mock): State<CloudMock>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    mock.enroll_requests.lock().await.push(body.clone());

    let Some(code) = body["adoption_code"].as_str() else {
        return error_json(StatusCode::BAD_REQUEST, "Validation error");
    };
    // Atomic consume: a code redeems exactly once.
    if !mock.codes.lock().await.remove(code) {
        return error_json(StatusCode::UNAUTHORIZED, "Adoption code already used");
    }
    let Some(csr_pem) = body["csr_pem"].as_str() else {
        return error_json(StatusCode::BAD_REQUEST, "Validation error");
    };
    // Host facts are NOT NULL registry columns.
    for field in ["fingerprint", "hostname", "os", "arch", "runtime_version"] {
        if body["instance"][field].as_str().is_none_or(str::is_empty) {
            return error_json(StatusCode::BAD_REQUEST, "Validation error");
        }
    }
    let Ok((leaf_pem, point)) = mock.ca.sign_csr(csr_pem) else {
        return error_json(StatusCode::BAD_REQUEST, "Malformed CSR");
    };
    *mock.pinned_point.lock().await = Some(point);
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "instance_id": ASSIGNED_ID,
            "identity_cert_pem": leaf_pem,
            "ca_bundle_pem": mock.ca.ca_cert_pem,
            "gateway_addr": mock.gateway_addr,
            "not_after": mock.not_after(),
        })),
    )
}

async fn mock_renew(
    State(mock): State<CloudMock>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    mock.renew_requests.lock().await.push(body.clone());

    let (Some(cert_pem), Some(csr_pem), Some(pop_sig)) = (
        body["cert_pem"].as_str(),
        body["csr_pem"].as_str(),
        body["pop_sig"].as_str(),
    ) else {
        return error_json(StatusCode::BAD_REQUEST, "Validation error");
    };
    if cert_pem.is_empty() || csr_pem.is_empty() || pop_sig.is_empty() {
        return error_json(StatusCode::BAD_REQUEST, "Validation error");
    }

    // Current-key proof-of-possession against the PINNED key (a cert is not
    // a secret; only the currently-pinned key may rotate the identity).
    let pinned = mock.pinned_point.lock().await.clone();
    let Some(pinned_point) = pinned else {
        return error_json(
            StatusCode::UNAUTHORIZED,
            "Current-key proof-of-possession failed",
        );
    };
    let Ok(signature) = base64::engine::general_purpose::STANDARD.decode(pop_sig) else {
        return error_json(
            StatusCode::UNAUTHORIZED,
            "Current-key proof-of-possession failed",
        );
    };
    let Ok(csr_der) = pem::parse(csr_pem) else {
        return error_json(StatusCode::BAD_REQUEST, "Malformed CSR");
    };
    let verifier = aws_lc_rs::signature::UnparsedPublicKey::new(
        &aws_lc_rs::signature::ECDSA_P256_SHA256_ASN1,
        pinned_point,
    );
    if verifier.verify(csr_der.contents(), &signature).is_err() {
        return error_json(
            StatusCode::UNAUTHORIZED,
            "Current-key proof-of-possession failed",
        );
    }

    // Re-issue over the CSR's NEW key and pin it (the rotation).
    let Ok((leaf_pem, point)) = mock.ca.sign_csr(csr_pem) else {
        return error_json(StatusCode::BAD_REQUEST, "Malformed CSR");
    };
    *mock.pinned_point.lock().await = Some(point);
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "identity_cert_pem": leaf_pem,
            "not_after": (chrono::Utc::now() + chrono::Duration::hours(24)).to_rfc3339(),
        })),
    )
}

/// Serve the cloud mock on an ephemeral port; returns its address.
async fn spawn_cloud_mock(mock: CloudMock) -> SocketAddr {
    let app = Router::new()
        .route("/v1/cloud-connect/enroll", post(mock_enroll))
        .route("/v1/cloud-connect/renew", post(mock_renew))
        .with_state(mock);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind http");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    addr
}

// --------------------------------------------------------------------------
// Gateway: real tonic CloudConnect/Stream over TLS with REQUIRED mTLS
// (post-DR-025 the gateway rejects certless connections).
// --------------------------------------------------------------------------

#[derive(Default)]
struct Captured {
    /// Number of streams opened by the client (reconnect counter).
    stream_count: u32,
    /// Hellos and whether the client presented a cert (mTLS) on them.
    hellos: Vec<(proto::Hello, bool)>,
    adopt_acks: Vec<proto::AdoptAck>,
    results: Vec<proto::CommandResult>,
    heartbeats: Vec<proto::Heartbeat>,
    telemetry: Vec<proto::Telemetry>,
    audits: Vec<proto::EventLog>,
}

#[derive(Clone)]
struct GatewayServer {
    captured: Arc<Mutex<Captured>>,
    /// Commands the server should push to the client on the current stream.
    /// Drained by a per-stream forwarder, so tests can enqueue at any time.
    outbound: Arc<Mutex<VecDeque<proto::ControlMessage>>>,
    /// When set, the server closes the FIRST stream right after its Hello —
    /// used to force a reconnect.
    drop_first_stream: Arc<AtomicBool>,
}

impl GatewayServer {
    fn new() -> Self {
        Self {
            captured: Arc::new(Mutex::new(Captured::default())),
            outbound: Arc::new(Mutex::new(VecDeque::new())),
            drop_first_stream: Arc::new(AtomicBool::new(false)),
        }
    }
}

fn ctrl(body: proto::control_message::Body) -> proto::ControlMessage {
    proto::ControlMessage { body: Some(body) }
}

#[async_trait]
impl CloudConnect for GatewayServer {
    type StreamStream = ReceiverStream<Result<proto::ControlMessage, Status>>;

    async fn stream(
        &self,
        request: Request<Streaming<proto::ClientMessage>>,
    ) -> Result<Response<Self::StreamStream>, Status> {
        // Presence of client certs proves the transport is mutually
        // authenticated on this stream (the TLS config also *requires* it).
        let has_client_cert = request.peer_certs().is_some_and(|certs| !certs.is_empty());

        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel::<Result<proto::ControlMessage, Status>>(32);

        let captured = Arc::clone(&self.captured);
        let outbound = Arc::clone(&self.outbound);
        let drop_first_stream = Arc::clone(&self.drop_first_stream);

        let stream_index = {
            let mut c = captured.lock().await;
            c.stream_count += 1;
            c.stream_count
        };

        tokio::spawn(async move {
            let mut forwarder: Option<tokio::task::JoinHandle<()>> = None;
            while let Ok(Some(msg)) = inbound.message().await {
                match msg.body {
                    Some(proto::client_message::Body::Hello(hello)) => {
                        captured
                            .lock()
                            .await
                            .hellos
                            .push((hello.clone(), has_client_cert));

                        // Force a reconnect by closing the first stream right
                        // after its Hello (dropping the outbound sender).
                        if stream_index == 1 && drop_first_stream.load(Ordering::SeqCst) {
                            if let Some(f) = forwarder.take() {
                                f.abort();
                            }
                            return;
                        }

                        // Start forwarding queued commands on this stream.
                        if forwarder.is_none() {
                            let tx = tx.clone();
                            let outbound = Arc::clone(&outbound);
                            forwarder = Some(tokio::spawn(async move {
                                loop {
                                    if tx.is_closed() {
                                        break;
                                    }
                                    let next = outbound.lock().await.pop_front();
                                    match next {
                                        Some(cmd) => {
                                            if tx.send(Ok(cmd)).await.is_err() {
                                                break;
                                            }
                                        }
                                        None => tokio::time::sleep(Duration::from_millis(20)).await,
                                    }
                                }
                            }));
                        }
                    }
                    Some(proto::client_message::Body::AdoptAck(ack)) => {
                        captured.lock().await.adopt_acks.push(ack);
                    }
                    Some(proto::client_message::Body::Result(result)) => {
                        captured.lock().await.results.push(result);
                    }
                    Some(proto::client_message::Body::Heartbeat(hb)) => {
                        captured.lock().await.heartbeats.push(hb);
                    }
                    Some(proto::client_message::Body::Telemetry(t)) => {
                        captured.lock().await.telemetry.push(t);
                    }
                    Some(proto::client_message::Body::Event(event)) => {
                        if event.kind == "audit" {
                            captured.lock().await.audits.push(event);
                        }
                    }
                    // A standalone runtime announces no per-connection
                    // encryption key, so this never arrives. The arm is spelled
                    // out rather than wildcarded so a new client message still
                    // has to be accounted for here.
                    Some(proto::client_message::Body::SecretsKey(_)) => {}
                    None => break,
                }
            }
            if let Some(f) = forwarder {
                f.abort();
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// Bind an ephemeral TLS port and serve the gateway on it. Client
/// certificates are REQUIRED — a certless connection fails the handshake,
/// as on the real post-DR-025 gateway.
async fn spawn_gateway(server: GatewayServer, ca: &TestCa) -> SocketAddr {
    ensure_crypto_provider();
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");

    let tls = ServerTlsConfig::new()
        .identity(TonicIdentity::from_pem(
            ca.server_cert_pem.clone(),
            ca.server_key_pem.clone(),
        ))
        .client_ca_root(Certificate::from_pem(ca.ca_cert_pem.clone()))
        .client_auth_optional(false);

    let svc = CloudConnectServer::new(server);
    tokio::spawn(async move {
        let _ = Server::builder()
            .tls_config(tls)
            .expect("server tls config")
            .add_service(svc)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    addr
}

// --------------------------------------------------------------------------
// A realistic runtime handle (mirrors the spiced adapter's observable
// behavior: spicepod-to-disk apply).
// --------------------------------------------------------------------------

#[derive(Default)]
struct E2eRuntimeState {
    applied_spicepod: Option<(std::path::PathBuf, String)>,
}

struct E2eRuntime {
    state: Arc<Mutex<E2eRuntimeState>>,
}

impl E2eRuntime {
    fn new() -> (Arc<Self>, Arc<Mutex<E2eRuntimeState>>) {
        let state = Arc::new(Mutex::new(E2eRuntimeState::default()));
        (
            Arc::new(Self {
                state: Arc::clone(&state),
            }),
            state,
        )
    }
}

#[async_trait]
impl RuntimeHandle for E2eRuntime {
    async fn active_datasets(&self) -> u32 {
        2
    }
    async fn active_models(&self) -> u32 {
        1
    }

    async fn apply_spicepod(
        &self,
        config_dir: &Path,
        spicepod_yaml: &str,
    ) -> Result<Value, String> {
        // Persist to the canonical path and report a hot apply, mirroring the
        // spiced adapter's observable result envelope.
        let path = config_dir.join(runtime_cloud_connect::config::CLOUD_MANAGED_SPICEPOD_FILE);
        tokio::fs::create_dir_all(config_dir)
            .await
            .map_err(|e| e.to_string())?;
        tokio::fs::write(&path, spicepod_yaml)
            .await
            .map_err(|e| e.to_string())?;
        self.state.lock().await.applied_spicepod = Some((path.clone(), spicepod_yaml.to_string()));
        Ok(serde_json::json!({
            "path": path.display().to_string(),
            "applied": true,
            "reload": "hot",
        }))
    }
}

// --------------------------------------------------------------------------
// Harness: cloud mock + gateway + client config + polling helpers.
// --------------------------------------------------------------------------

/// A fully-wired split control plane: the cloud mock (HTTP) and the
/// gateway (mTLS gRPC), sharing one throwaway CA.
struct Harness {
    ca: Arc<TestCa>,
    cloud: CloudMock,
    cloud_addr: SocketAddr,
    gateway: GatewayServer,
}

impl Harness {
    /// Stand up the gateway + cloud mock. `leaf_validity_secs` controls the
    /// `not_after` the cloud reports on enroll (short values trigger the
    /// renewal loop quickly).
    async fn new(leaf_validity_secs: i64) -> Self {
        let ca = Arc::new(TestCa::new());
        let gateway = GatewayServer::new();
        let gateway_addr = spawn_gateway(gateway.clone(), &ca).await;
        let cloud = CloudMock::new(
            Arc::clone(&ca),
            gateway_addr.to_string(),
            leaf_validity_secs,
        );
        let cloud_addr = spawn_cloud_mock(cloud.clone()).await;
        Self {
            ca,
            cloud,
            cloud_addr,
            gateway,
        }
    }

    fn config(
        &self,
        identity_path: std::path::PathBuf,
        config_dir: std::path::PathBuf,
        adoption_code: Option<String>,
        renewal_lead: Duration,
    ) -> CloudConnectConfig {
        CloudConnectConfig {
            enroll_endpoint: format!("http://{}", self.cloud_addr),
            // No override: the stream must connect to the gateway_addr the
            // enroll response issued (persisted in the identity).
            gateway_endpoint: None,
            // Pin the test CA so gateway verification is hermetic (no
            // dependence on the host's native trust store). The identity's
            // ca_bundle_pem (returned by enroll) pins the same root.
            ca_cert_pem: Some(self.ca.ca_cert_pem.clone()),
            insecure: false,
            identity_path,
            config_dir,
            adoption_code,
            pending_adopt_code_path: None,
            runtime_version: "v0.0.0-e2e".to_string(),
            // Sub-second cadences keep the suite fast while still exercising
            // the periodic frame paths.
            heartbeat_interval: Duration::from_millis(150),
            telemetry_interval: Duration::from_millis(250),
            renewal_lead,
        }
    }
}

/// Poll a **synchronous** `cond` every 25ms until it is true or `budget`
/// elapses. Used for filesystem checks (e.g. `identity.json` existence).
async fn wait_until<F>(budget: Duration, mut cond: F) -> bool
where
    F: FnMut() -> bool,
{
    let deadline = std::time::Instant::now() + budget;
    loop {
        if cond() {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Poll an **async** `cond` (which typically locks the captured server state)
/// every 25ms until it is true or `budget` elapses.
async fn wait_until_async<F, Fut>(budget: Duration, mut cond: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = std::time::Instant::now() + budget;
    loop {
        if cond().await {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Read the captured server state under its mutex for a single assertion.
/// The bound identifier refers to the locked `&Captured`; `$body` is a plain
/// expression over it (no closure — keeps clippy's `allow_attributes` /
/// `redundant_closure_call` happy).
macro_rules! with_captured {
    ($captured:expr, $c:ident => $body:expr) => {{
        let $c = $captured.lock().await;
        $body
    }};
}

/// Drive enrollment to completion (identity persisted + mTLS Hello observed
/// by the gateway) and return the loaded identity.
async fn enroll(
    harness: &Harness,
    config: &CloudConnectConfig,
    runtime: Arc<dyn RuntimeHandle>,
) -> (
    runtime_cloud_connect::CloudConnect,
    runtime_cloud_connect::identity::Identity,
) {
    let handle = runtime_cloud_connect::CloudConnect::start(config.clone(), runtime)
        .await
        .expect("start")
        .expect("started");

    let identity_path = config.identity_path.clone();
    let enrolled = wait_until(Duration::from_secs(10), || identity_path.exists()).await;
    assert!(enrolled, "identity.json must be written within 10s");

    // Wait for the gateway to observe the mTLS Hello so the handshake is
    // fully settled before the test proceeds.
    let captured = Arc::clone(&harness.gateway.captured);
    let connected = wait_until_async(Duration::from_secs(10), || {
        let captured = Arc::clone(&captured);
        async move {
            captured
                .lock()
                .await
                .hellos
                .iter()
                .any(|(h, mtls)| h.identifier == ASSIGNED_ID && *mtls)
        }
    })
    .await;
    assert!(connected, "gateway must observe the mTLS Hello within 10s");

    let identity = IdentityStore::load_optional(&identity_path)
        .expect("load identity")
        .expect("identity present");
    (handle, identity)
}

// --------------------------------------------------------------------------
// Tests.
// --------------------------------------------------------------------------

#[tokio::test]
async fn enrollment_issues_identity_and_streams_over_mtls() {
    let harness = Harness::new(24 * 60 * 60).await;
    let dir = tempfile::tempdir().unwrap();
    let config = harness.config(
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Some(ADOPTION_CODE.to_string()),
        Duration::from_hours(12),
    );

    let (runtime, _rt_state) = E2eRuntime::new();
    let (handle, identity) = enroll(&harness, &config, runtime).await;

    // The enroll request carried the out-of-band contract: adoption code +
    // CSR + host facts under `instance` — no bearer token field.
    let requests = harness.cloud.enroll_requests.lock().await.clone();
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
    assert_eq!(body["instance"]["fingerprint"].as_str().unwrap().len(), 64);
    assert_eq!(body["instance"]["runtime_version"], "v0.0.0-e2e");

    // The persisted identity binds the cloud-signed leaf to the client key
    // and captured the issued CA bundle + gateway address.
    assert_eq!(identity.identifier, ASSIGNED_ID);
    assert!(identity.identity_cert_pem.contains("BEGIN CERTIFICATE"));
    assert!(identity.private_key_pem.contains("PRIVATE KEY"));
    assert!(identity.ca_bundle_pem.contains("BEGIN CERTIFICATE"));
    assert_eq!(identity.gateway_addr, harness.cloud.gateway_addr);
    assert!(identity.not_after_unix > 0, "leaf expiry must be recorded");
    // That the signed leaf genuinely chains to the CA is proved
    // operationally: the gateway REQUIRES client certs chaining to it, so
    // the observed mTLS Hello (in `enroll`) implies a valid chain.

    // The stream Hello names the instance with an empty credential and no
    // CSR — enrollment moved out-of-band.
    let captured = Arc::clone(&harness.gateway.captured);
    let ok = with_captured!(captured, c => {
        c.hellos.iter().any(|(h, mtls)| {
            h.identifier == ASSIGNED_ID
                && *mtls
                && h.credential.is_empty()
                && h.csr_pem.is_empty()
                && h.kind == proto::InstanceKind::Standalone as i32
        })
    });
    assert!(ok, "mTLS Hello must carry identifier + empty credential");

    handle.shutdown().await;
}

#[tokio::test]
async fn adoption_code_is_single_use() {
    let harness = Harness::new(24 * 60 * 60).await;

    // First machine redeems the code.
    let dir1 = tempfile::tempdir().unwrap();
    let config1 = harness.config(
        dir1.path().join("identity.json"),
        dir1.path().to_path_buf(),
        Some(ADOPTION_CODE.to_string()),
        Duration::from_hours(12),
    );
    let (runtime1, _s1) = E2eRuntime::new();
    let (handle1, _identity) = enroll(&harness, &config1, runtime1).await;
    handle1.shutdown().await;

    // A replay of the consumed code is rejected (401) and never retried;
    // no identity is created.
    let dir2 = tempfile::tempdir().unwrap();
    let identity_path2 = dir2.path().join("identity.json");
    let config2 = harness.config(
        identity_path2.clone(),
        dir2.path().to_path_buf(),
        Some(ADOPTION_CODE.to_string()),
        Duration::from_hours(12),
    );
    let (runtime2, _s2) = E2eRuntime::new();
    let handle2 = runtime_cloud_connect::CloudConnect::start(config2, runtime2)
        .await
        .expect("start")
        .expect("started");

    // The replayed enroll arrives at the cloud mock...
    let cloud = harness.cloud.clone();
    let replay_seen = wait_until_async(Duration::from_secs(5), || {
        let cloud = cloud.clone();
        async move { cloud.enroll_requests.lock().await.len() >= 2 }
    })
    .await;
    assert!(replay_seen, "the replayed enroll must reach the cloud");

    // ...is rejected, and the rejection is terminal: give the driver a
    // moment and confirm no identity appeared and no retry was sent.
    let retried = wait_until_async(Duration::from_secs(2), || {
        let cloud = cloud.clone();
        async move { cloud.enroll_requests.lock().await.len() > 2 }
    })
    .await;
    assert!(!retried, "a consumed code must not be retried");
    assert!(
        !identity_path2.exists(),
        "no identity may be issued for a consumed code"
    );

    handle2.shutdown().await;
}

#[tokio::test]
async fn identity_is_reused_across_restart_over_mtls() {
    let harness = Harness::new(24 * 60 * 60).await;
    let dir = tempfile::tempdir().unwrap();
    let identity_path = dir.path().join("identity.json");

    // First boot: enroll with the adoption code.
    let enroll_cfg = harness.config(
        identity_path.clone(),
        dir.path().to_path_buf(),
        Some(ADOPTION_CODE.to_string()),
        Duration::from_hours(12),
    );
    let (runtime, _s) = E2eRuntime::new();
    let (handle, _identity) = enroll(&harness, &enroll_cfg, runtime).await;
    handle.shutdown().await; // simulate process stop; identity.json persists.

    let captured = Arc::clone(&harness.gateway.captured);
    let hellos_before = with_captured!(captured, c => c.hellos.len());

    // Second boot: NO adoption code — the client must load the persisted
    // identity and reconnect over mTLS, presenting its client certificate,
    // without touching the enroll endpoint again.
    let enrolls_before = harness.cloud.enroll_requests.lock().await.len();
    let reuse_cfg = harness.config(
        identity_path.clone(),
        dir.path().to_path_buf(),
        None,
        Duration::from_hours(12),
    );
    let (runtime2, _s2) = E2eRuntime::new();
    let handle2 = runtime_cloud_connect::CloudConnect::start(reuse_cfg, runtime2)
        .await
        .expect("start")
        .expect("started (identity mode)");

    let captured = Arc::clone(&harness.gateway.captured);
    let reconnected = wait_until_async(Duration::from_secs(10), || {
        let captured = Arc::clone(&captured);
        async move {
            let c = captured.lock().await;
            c.hellos.len() > hellos_before
                && c.hellos.iter().skip(hellos_before).any(|(h, mtls)| {
                    h.identifier == ASSIGNED_ID && *mtls && h.credential.is_empty()
                })
        }
    })
    .await;
    assert!(
        reconnected,
        "restarted client must reconnect over mTLS with its identifier"
    );
    assert_eq!(
        harness.cloud.enroll_requests.lock().await.len(),
        enrolls_before,
        "identity reuse must not re-enroll"
    );

    handle2.shutdown().await;
}

#[tokio::test]
async fn heartbeat_and_telemetry_cadence() {
    let harness = Harness::new(24 * 60 * 60).await;
    let dir = tempfile::tempdir().unwrap();
    let config = harness.config(
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Some(ADOPTION_CODE.to_string()),
        Duration::from_hours(12),
    );
    let (runtime, _s) = E2eRuntime::new();
    let (handle, _identity) = enroll(&harness, &config, runtime).await;

    // With a 150ms heartbeat and 250ms telemetry cadence, several of each must
    // arrive within a couple of seconds.
    let captured = Arc::clone(&harness.gateway.captured);
    let enough = wait_until_async(Duration::from_secs(5), || {
        let captured = Arc::clone(&captured);
        async move {
            let c = captured.lock().await;
            c.heartbeats.len() >= 3 && c.telemetry.len() >= 2
        }
    })
    .await;
    assert!(enough, "expected >=3 heartbeats and >=2 telemetry frames");

    // The frames carry the enrolled identifier and the runtime counters.
    let (hb_ok, tel_ok) = with_captured!(captured, c => {
        let hb_ok = c
            .heartbeats
            .iter()
            .any(|h| h.identifier == ASSIGNED_ID && h.active_datasets == 2 && h.status == "online");
        let tel_ok = c.telemetry.iter().any(|t| {
            t.identifier == ASSIGNED_ID
                && t.metrics.contains_key("datasets_active")
                && t.window_end_unix >= t.window_start_unix
        });
        (hb_ok, tel_ok)
    });
    assert!(hb_ok, "a heartbeat must carry the identifier + counters");
    assert!(
        tel_ok,
        "a telemetry frame must carry billing-shaped metrics"
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn apply_spicepod_hot_applies_and_persists() {
    let harness = Harness::new(24 * 60 * 60).await;
    let dir = tempfile::tempdir().unwrap();
    let config = harness.config(
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Some(ADOPTION_CODE.to_string()),
        Duration::from_hours(12),
    );
    let (runtime, rt_state) = E2eRuntime::new();
    let (handle, _identity) = enroll(&harness, &config, runtime).await;

    let yaml = "version: v2\nkind: Spicepod\nname: e2e-cloud-managed\n";
    harness.gateway.outbound.lock().await.push_back(ctrl(
        proto::control_message::Body::ApplySpicepod(proto::ApplySpicepod {
            command_id: "cmd-apply".to_string(),
            spicepod_yaml: yaml.to_string(),
        }),
    ));

    let captured = Arc::clone(&harness.gateway.captured);
    let applied = wait_until_async(Duration::from_secs(5), || {
        let captured = Arc::clone(&captured);
        async move {
            captured
                .lock()
                .await
                .results
                .iter()
                .any(|r| r.command_id == "cmd-apply")
        }
    })
    .await;
    assert!(applied, "apply result must arrive within 5s");

    let result = with_captured!(captured, c => c
        .results
        .iter()
        .find(|r| r.command_id == "cmd-apply")
        .cloned())
    .expect("apply result");
    assert!(result.success, "apply must succeed: {}", result.error);
    let meta: Value = serde_json::from_str(&result.payload_json).unwrap();
    assert_eq!(meta["applied"], true);
    assert_eq!(meta["reload"], "hot");

    // The runtime persisted the YAML to the canonical cloud-managed path.
    let (path, written) = rt_state
        .lock()
        .await
        .applied_spicepod
        .clone()
        .expect("spicepod applied");
    assert_eq!(written, yaml);
    assert!(path.exists(), "spicepod file must be on disk");

    handle.shutdown().await;
}

#[tokio::test]
async fn reconnects_over_mtls_after_disconnect() {
    let harness = Harness::new(24 * 60 * 60).await;
    // Force the gateway to drop the first stream right after its Hello so
    // the client must reconnect.
    harness
        .gateway
        .drop_first_stream
        .store(true, Ordering::SeqCst);

    let dir = tempfile::tempdir().unwrap();
    let config = harness.config(
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Some(ADOPTION_CODE.to_string()),
        Duration::from_hours(12),
    );
    let (runtime, _s) = E2eRuntime::new();

    let handle = runtime_cloud_connect::CloudConnect::start(config.clone(), runtime)
        .await
        .expect("start")
        .expect("started");

    // The identity is persisted by the out-of-band enroll regardless of the
    // (soon-dropped) first stream.
    let identity_path = config.identity_path.clone();
    assert!(
        wait_until(Duration::from_secs(10), || identity_path.exists()).await,
        "identity must persist before the reconnect"
    );

    // After the drop + backoff, the client reconnects with its identifier
    // over a second mutually-authenticated stream — without re-enrolling.
    let captured = Arc::clone(&harness.gateway.captured);
    let reconnected = wait_until_async(Duration::from_secs(15), || {
        let captured = Arc::clone(&captured);
        async move {
            let c = captured.lock().await;
            c.stream_count >= 2
                && c.hellos
                    .iter()
                    .filter(|(h, mtls)| {
                        h.identifier == ASSIGNED_ID && *mtls && h.credential.is_empty()
                    })
                    .count()
                    >= 2
        }
    })
    .await;
    assert!(
        reconnected,
        "client must reopen a second, mutually-authenticated stream"
    );
    assert_eq!(
        harness.cloud.enroll_requests.lock().await.len(),
        1,
        "reconnect must not re-enroll"
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn renewal_rotates_keypair_and_persists() {
    // Issue a leaf that "expires" in 5s with a 2s renewal lead: renewal
    // becomes due ~3s after enrollment, exercising the ~12h loop at test
    // speed. (The rcgen-signed cert itself is valid longer — the client
    // schedules from the reported not_after, which is what's under test.)
    let harness = Harness::new(5).await;
    let dir = tempfile::tempdir().unwrap();
    let identity_path = dir.path().join("identity.json");
    let config = harness.config(
        identity_path.clone(),
        dir.path().to_path_buf(),
        Some(ADOPTION_CODE.to_string()),
        Duration::from_secs(2),
    );
    let (runtime, _s) = E2eRuntime::new();

    // Start the client directly (not via the `enroll` helper): the
    // pre-rotation identity must be snapshotted as soon as it lands on
    // disk, before the renewal timer can fire and overwrite it.
    let handle = runtime_cloud_connect::CloudConnect::start(config, runtime)
        .await
        .expect("start")
        .expect("started");
    assert!(
        wait_until(Duration::from_secs(10), || identity_path.exists()).await,
        "identity.json must be written within 10s"
    );
    let enrolled_identity = IdentityStore::load_optional(&identity_path)
        .expect("load identity")
        .expect("identity present");
    assert_eq!(enrolled_identity.identifier, ASSIGNED_ID);

    // The renewal request must arrive and be verified (the mock rejects any
    // PoP signature that does not verify against the pinned key).
    let cloud = harness.cloud.clone();
    let renewed = wait_until_async(Duration::from_secs(15), || {
        let cloud = cloud.clone();
        async move { !cloud.renew_requests.lock().await.is_empty() }
    })
    .await;
    assert!(renewed, "a renewal must be attempted within 15s");

    // The renew request carried the standard contract shape.
    let renew_body = harness.cloud.renew_requests.lock().await[0].clone();
    assert_eq!(
        renew_body["cert_pem"].as_str().unwrap(),
        enrolled_identity.identity_cert_pem,
        "renew presents the current leaf"
    );
    assert!(
        renew_body["csr_pem"]
            .as_str()
            .unwrap()
            .contains("CERTIFICATE REQUEST"),
        "renew carries a fresh CSR"
    );
    assert!(
        !renew_body["pop_sig"].as_str().unwrap().is_empty(),
        "renew carries the current-key proof-of-possession"
    );

    // The rotated identity is persisted: new keypair, new leaf, later
    // expiry; identifier / CA bundle / gateway address unchanged.
    let rotated = wait_until(Duration::from_secs(10), || {
        IdentityStore::load_optional(&identity_path)
            .ok()
            .flatten()
            .is_some_and(|id| id.public_key_pem != enrolled_identity.public_key_pem)
    })
    .await;
    assert!(rotated, "the rotated identity must be persisted within 10s");

    let renewed_identity = IdentityStore::load_optional(&identity_path)
        .expect("load identity")
        .expect("identity present");
    assert_eq!(renewed_identity.identifier, enrolled_identity.identifier);
    assert_ne!(
        renewed_identity.private_key_pem, enrolled_identity.private_key_pem,
        "every renewal rotates the keypair"
    );
    assert_ne!(
        renewed_identity.identity_cert_pem, enrolled_identity.identity_cert_pem,
        "a new leaf is issued"
    );
    assert!(
        renewed_identity.not_after_unix > enrolled_identity.not_after_unix,
        "the renewed leaf expires later"
    );
    assert_eq!(
        renewed_identity.gateway_addr, enrolled_identity.gateway_addr,
        "the gateway address is preserved across renewal"
    );
    assert_eq!(
        renewed_identity.ca_bundle_pem, enrolled_identity.ca_bundle_pem,
        "the CA bundle is preserved across renewal"
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn forget_clears_identity_and_exits() {
    let harness = Harness::new(24 * 60 * 60).await;
    let dir = tempfile::tempdir().unwrap();
    let identity_path = dir.path().join("identity.json");
    let config = harness.config(
        identity_path.clone(),
        dir.path().to_path_buf(),
        Some(ADOPTION_CODE.to_string()),
        Duration::from_hours(12),
    );
    let (runtime, _s) = E2eRuntime::new();
    let (handle, _identity) = enroll(&harness, &config, runtime).await;
    assert!(identity_path.exists(), "identity present after enrollment");

    // Server issues Forget.
    harness
        .gateway
        .outbound
        .lock()
        .await
        .push_back(ctrl(proto::control_message::Body::Forget(proto::Forget {
            command_id: "cmd-forget".to_string(),
        })));

    // The client clears identity.json and the cloud-connect task exits; spiced
    // itself (here, the runtime handle) is untouched.
    let cleared = wait_until(Duration::from_secs(5), || !identity_path.exists()).await;
    assert!(cleared, "Forget must remove identity.json");

    let captured = Arc::clone(&harness.gateway.captured);
    let acked = wait_until_async(Duration::from_secs(5), || {
        let captured = Arc::clone(&captured);
        async move {
            captured
                .lock()
                .await
                .results
                .iter()
                .any(|r| r.command_id == "cmd-forget" && r.success)
        }
    })
    .await;
    assert!(acked, "server must see a successful Forget result");

    // shutdown() returns promptly because the task already exited on Forget.
    handle.shutdown().await;
}
