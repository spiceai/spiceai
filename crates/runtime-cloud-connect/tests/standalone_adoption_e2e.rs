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
//! 5. `apply_spicepod` — the YAML is persisted, the result is flushed, and the
//!    runtime is asked to exit so its supervisor restarts it onto the new
//!    configuration; the version it comes back on rides the next `Hello`.
//! 6. `reconnect_over_mtls` — after the server drops the stream, the
//!    client reconnects, presenting its client certificate again.
//! 7. `renewal` — a short-lived leaf triggers the renewal loop: a fresh
//!    keypair + CSR + PoP signature against `/renew`, and the rotated
//!    identity is persisted.
//! 8. `remove` — the server sends `Remove`, the client clears
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
    reason = "integration-test harness — readability over lint strictness"
)]

use std::collections::{HashSet, VecDeque};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
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
use runtime_cloud_connect::handlers::{
    ApplyOutcome, Capability, CommandError, DeployState, RuntimeHandle, SpicepodDeployment,
};
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
    /// The region on the instance's registry row, standing in for the stored
    /// column: an enroll declaring a region writes it, one that declares none
    /// leaves it untouched.
    stored_region: Arc<Mutex<Option<String>>>,
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
            stored_region: Arc::new(Mutex::new(None)),
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
    let mut response = serde_json::json!({
        "instance_id": ASSIGNED_ID,
        "identity_cert_pem": leaf_pem,
        "ca_bundle_pem": mock.ca.ca_cert_pem,
        "gateway_addr": mock.gateway_addr,
        "not_after": mock.not_after(),
    });
    // Attach-at-connect: the real cloud validates and attaches; the mock
    // echoes the requested app back, matching the response contract.
    if let Some(app_name) = body["app_name"].as_str() {
        response["app_name"] = serde_json::Value::String(app_name.to_string());
    }
    // The real cloud reports the region now stored on the row: the declared
    // one when the request carried it, otherwise whatever the row already
    // held (a re-enrol with no `region` leaves it alone). The mock stands in
    // for that stored value.
    let stored_region = match body["region"].as_str() {
        Some(region) => {
            *mock.stored_region.lock().await = Some(region.to_string());
            Some(region.to_string())
        }
        None => mock.stored_region.lock().await.clone(),
    };
    if let Some(region) = stored_region {
        response["region"] = serde_json::Value::String(region);
    }
    (StatusCode::OK, Json(response))
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
    /// Per-connection encryption keys the client announced. The gateway seals
    /// delivered secrets to these, so a session that announced none receives
    /// none.
    secrets_keys: Vec<proto::SecretsKey>,
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

/// A command the client must answer with a `CommandResult` correlated by
/// `command_id`. The id lives on the envelope, not on the command.
fn ctrl_id(command_id: &str, body: proto::control_message::Body) -> proto::ControlMessage {
    proto::ControlMessage {
        command_id: command_id.to_string(),
        target: None,
        body: Some(body),
    }
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
                        if event.event_type == "audit" {
                            captured.lock().await.audits.push(event);
                        }
                    }
                    // Neither of these is emitted yet: a standalone runtime
                    // announces no per-connection encryption key, and nothing
                    // pushes OTLP metrics. The arms are spelled out rather than
                    // wildcarded so a new client message still has to be
                    // accounted for here.
                    Some(proto::client_message::Body::ExportMetrics(_)) => {}
                    // Announced once per stream, immediately after the Hello:
                    // the gateway needs it to seal the outer layer of any
                    // secrets it dispatches on this session.
                    Some(proto::client_message::Body::SecretsKey(key)) => {
                        captured.lock().await.secrets_keys.push(key);
                    }
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
    /// Names of the secrets delivered with the last applied spicepod, never
    /// values. `None` when the deployment carried no payload at all.
    delivered_secret_names: Option<Vec<String>>,
    /// The deployment this stand-in reports as applied. `0` is "none since
    /// enrolment"; seeded to model an instance that came back up on a
    /// deployment, and advanced by an apply the way `spiced` advances the record
    /// it writes before restarting.
    applied_deployment_version: u64,
    /// The deployment it refused, and why — set through `refuse_deployment` and
    /// cleared by the next accepted one, as the real adapter does.
    refused: Option<(u64, String)>,
    /// Set when the client asked the runtime to exit and apply. The real
    /// adapter ends the process here; a test one records that it was asked, so
    /// the test can assert the result was flushed first.
    exit_requested: bool,
    /// Makes the next apply fail validation, so the refusal path can be driven
    /// without building a spicepod this harness would have to parse.
    reject_next_apply: Option<String>,
}

struct E2eRuntime {
    state: Arc<Mutex<E2eRuntimeState>>,
}

impl E2eRuntime {
    fn new() -> (Arc<Self>, Arc<Mutex<E2eRuntimeState>>) {
        Self::with_applied_version(0)
    }

    /// An instance already serving `version`, as one that restarted onto a
    /// deployment reports itself. `0` is an instance that has applied nothing.
    fn with_applied_version(version: u64) -> (Arc<Self>, Arc<Mutex<E2eRuntimeState>>) {
        let state = Arc::new(Mutex::new(E2eRuntimeState {
            applied_deployment_version: version,
            ..E2eRuntimeState::default()
        }));
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
    fn supports(&self, capability: Capability) -> bool {
        matches!(
            capability,
            Capability::ApplySpicepod | Capability::DeployVersions
        )
    }

    async fn active_datasets(&self) -> u32 {
        2
    }
    async fn active_models(&self) -> u32 {
        1
    }

    async fn deploy_state(&self) -> Option<DeployState> {
        let state = self.state.lock().await;
        let deploy = DeployState::applied(state.applied_deployment_version);
        Some(match state.refused.as_ref() {
            Some((version, message)) => deploy.with_failure(*version, message.clone()),
            None => deploy,
        })
    }

    async fn refuse_deployment(&self, deployment_version: Option<u64>, message: &str) {
        if let Some(version) = deployment_version {
            self.state.lock().await.refused = Some((version, message.to_string()));
        }
    }

    async fn apply_spicepod(
        &self,
        deployment: SpicepodDeployment<'_>,
    ) -> Result<ApplyOutcome, CommandError> {
        if let Some(reason) = self.state.lock().await.reject_next_apply.take() {
            // Validation refuses it: nothing is persisted and nothing restarts,
            // so the client records the refusal and the next heartbeat reports it.
            return Err(CommandError::invalid_argument(reason));
        }
        // Record the delivered names (never values) so a test can assert the
        // payload reached the runtime adapter.
        self.state.lock().await.delivered_secret_names = deployment
            .delivered_secrets
            .as_ref()
            .map(|secrets| secrets.keys().cloned().collect());
        // Persist to the canonical path and ask for the restart that makes it
        // live, mirroring the spiced adapter's observable behavior.
        let path = deployment
            .config_dir
            .join(runtime_cloud_connect::config::CLOUD_MANAGED_SPICEPOD_FILE);
        tokio::fs::create_dir_all(deployment.config_dir)
            .await
            .map_err(|e| CommandError::failed(e.to_string()))?;
        tokio::fs::write(&path, deployment.spicepod_yaml)
            .await
            .map_err(|e| CommandError::failed(e.to_string()))?;
        {
            let mut state = self.state.lock().await;
            state.applied_spicepod = Some((path.clone(), deployment.spicepod_yaml.to_string()));
            state.applied_deployment_version = deployment.deployment_version.unwrap_or(0);
            // An accepted deployment supersedes an earlier refusal, which is how
            // a stale failure stops being reported.
            state.refused = None;
        }
        Ok(ApplyOutcome::exit_to_apply(serde_json::json!({
            "path": path.display().to_string(),
            "applied": true,
            "live": false,
            "restart": "required",
            "deployment_version": deployment.deployment_version,
        })))
    }

    async fn exit_to_apply(&self) {
        self.state.lock().await.exit_requested = true;
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
            adopt_app_name: None,
            adopt_create_app: false,
            instance_region: None,
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
    assert!(
        body["enc_pubkey_pem"]
            .as_str()
            .unwrap()
            .contains("BEGIN PUBLIC KEY"),
        "enroll must carry the X25519 encryption public key (SPKI PEM)"
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
    assert!(
        identity.not_after_unix.is_some_and(|secs| secs > 0),
        "leaf expiry must be recorded"
    );
    assert!(
        identity.enc_private_key_pem.contains("PRIVATE KEY")
            && identity.enc_public_key_pem.contains("PUBLIC KEY"),
        "the X25519 encryption keypair must be persisted with the identity"
    );
    // That the signed leaf genuinely chains to the CA is proved
    // operationally: the gateway REQUIRES client certs chaining to it, so
    // the observed mTLS Hello (in `enroll`) implies a valid chain.

    // The stream Hello names the instance and carries no credential of its
    // own — enrollment moved out-of-band, and mTLS is the authN. Asserted field
    // by field rather than as one predicate, so a failure says which part broke.
    let captured = Arc::clone(&harness.gateway.captured);
    let (hello, over_mtls) = with_captured!(captured, c => c
        .hellos
        .iter()
        .find(|(h, _)| h.identifier == ASSIGNED_ID)
        .cloned())
    .expect("the gateway must observe a Hello naming the enrolled instance");
    assert!(
        over_mtls,
        "the Hello must arrive on a mutually-authenticated stream"
    );
    assert_eq!(hello.instance_kind, proto::InstanceKind::Standalone as i32);
    assert_eq!(
        hello.protocol_version,
        runtime_cloud_connect::PROTOCOL_VERSION
    );
    assert_eq!(
        hello.capabilities,
        vec![
            "apply_spicepod".to_string(),
            runtime_cloud_connect::handlers::CAPABILITY_DEPLOY_VERSIONS.to_string(),
        ],
        "this handle applies spicepods and reports deploy versions, and announces exactly those"
    );

    handle.shutdown().await;
}

/// The `spice connect` enroll-and-exit contract: a one-shot `enroll_now`
/// issues and persists the identity with no client running (no gateway
/// connection), discards the staged pending-code file, and a later
/// `CloudConnect::start` with **no adoption code** connects using the
/// persisted identity — enroll and run as two separate steps.
#[tokio::test]
async fn one_shot_enroll_then_separate_run_connects_with_stored_identity() {
    let harness = Harness::new(24 * 60 * 60).await;
    let dir = tempfile::tempdir().unwrap();

    // Stage the code the way `spice connect` does.
    let pending_path = dir.path().join("pending-adopt-code");
    std::fs::write(&pending_path, ADOPTION_CODE).unwrap();

    let mut config = harness.config(
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Some(ADOPTION_CODE.to_string()),
        Duration::from_hours(12),
    );
    config.pending_adopt_code_path = Some(pending_path.clone());

    // Phase 1: one-shot enroll — no client task, no stream.
    let outcome = runtime_cloud_connect::enroll::enroll_now(&config)
        .await
        .expect("one-shot enroll succeeds");
    assert_eq!(outcome.identity.identifier, ASSIGNED_ID);
    assert_eq!(
        outcome.registration.app_name, None,
        "no attachment was requested"
    );
    assert!(
        config.identity_path.exists(),
        "identity must be persisted by the one-shot enroll"
    );
    assert!(
        !pending_path.exists(),
        "the staged code must be discarded once consumed"
    );
    let captured_after_enroll = Arc::clone(&harness.gateway.captured);
    let hellos = with_captured!(captured_after_enroll, c => c.hellos.len());
    assert_eq!(hellos, 0, "one-shot enroll must not connect to the gateway");

    // Phase 2: a separate start with NO adoption code connects with the
    // stored identity.
    let run_config = harness.config(
        config.identity_path.clone(),
        dir.path().to_path_buf(),
        None,
        Duration::from_hours(12),
    );
    let (runtime, _rt_state) = E2eRuntime::new();
    let handle = runtime_cloud_connect::CloudConnect::start(run_config, runtime)
        .await
        .expect("start")
        .expect("enabled with stored identity");
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
    assert!(
        connected,
        "the runtime must connect with the persisted identity"
    );
    assert_eq!(
        harness.cloud.enroll_requests.lock().await.len(),
        1,
        "the run phase must reuse the identity, not enroll again"
    );
    handle.shutdown().await;
}

/// An authoritative cloud rejection of a one-shot enroll burns the staged
/// code file (a dead code must not be re-presented by a later `spiced`
/// start) and persists no identity.
#[tokio::test]
async fn one_shot_enroll_discards_staged_code_on_rejection() {
    let harness = Harness::new(24 * 60 * 60).await;
    let dir = tempfile::tempdir().unwrap();
    let pending_path = dir.path().join("pending-adopt-code");
    std::fs::write(&pending_path, "SPICE-ADOPT-DEADD-BEEFF").unwrap();

    let mut config = harness.config(
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        // Not registered with the cloud mock — rejected as unknown/consumed.
        Some("SPICE-ADOPT-DEADD-BEEFF".to_string()),
        Duration::from_hours(12),
    );
    config.pending_adopt_code_path = Some(pending_path.clone());

    let err = runtime_cloud_connect::enroll::enroll_now(&config)
        .await
        .expect_err("an unknown code must be rejected");
    assert!(
        err.is_authoritative_rejection(),
        "a 4xx cloud rejection is authoritative: {err}"
    );
    assert!(
        !pending_path.exists(),
        "a dead code must not stay staged for retry"
    );
    assert!(
        !config.identity_path.exists(),
        "no identity may be persisted on a rejected enroll"
    );
}

/// Attach-at-connect: `adopt_app_name`/`adopt_create_app` ride the enroll
/// request (`app_name`/`create_app` on the wire, omitted when unset) and
/// the response's attached app comes back in the outcome.
#[tokio::test]
async fn one_shot_enroll_carries_app_attachment() {
    let harness = Harness::new(24 * 60 * 60).await;
    let dir = tempfile::tempdir().unwrap();

    let mut config = harness.config(
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Some(ADOPTION_CODE.to_string()),
        Duration::from_hours(12),
    );
    config.adopt_app_name = Some("e2e-app".to_string());
    config.adopt_create_app = true;

    let outcome = runtime_cloud_connect::enroll::enroll_now(&config)
        .await
        .expect("enroll with attachment succeeds");
    assert_eq!(outcome.registration.app_name.as_deref(), Some("e2e-app"));

    let requests = harness.cloud.enroll_requests.lock().await.clone();
    assert_eq!(requests.len(), 1, "exactly one enroll request");
    assert_eq!(requests[0]["app_name"], "e2e-app");
    assert_eq!(requests[0]["create_app"], true);
}

/// The declared instance region rides the enroll request as a **sibling of
/// the probed host facts** and comes back on the registry row. Any
/// syntactically valid label enrolls — including one no region catalog knows —
/// because a standalone host may not be in a cloud region at all.
#[tokio::test]
async fn one_shot_enroll_records_the_declared_region() {
    let harness = Harness::new(24 * 60 * 60).await;
    let dir = tempfile::tempdir().unwrap();

    let mut config = harness.config(
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Some(ADOPTION_CODE.to_string()),
        Duration::from_hours(12),
    );
    config.instance_region = Some("on-prem-syd".to_string());

    let outcome = runtime_cloud_connect::enroll::enroll_now(&config)
        .await
        .expect("enroll with a non-catalog region succeeds");
    assert_eq!(outcome.registration.region.as_deref(), Some("on-prem-syd"));

    let requests = harness.cloud.enroll_requests.lock().await.clone();
    assert_eq!(requests.len(), 1, "exactly one enroll request");
    assert_eq!(requests[0]["region"], "on-prem-syd");
    assert!(
        requests[0]["instance"].get("region").is_none(),
        "the declared region must not be nested inside the probed host facts"
    );
}

/// Omitting `--region` on a re-enrol must leave the stored region alone.
/// Re-enrolment is how a standalone instance recovers past its renewal grace
/// window, so a request that unconditionally wrote the region would erase one
/// set in the portal on every recovery.
#[tokio::test]
async fn re_enroll_without_a_region_leaves_the_stored_region_untouched() {
    // A second code stands in for the re-issued one a past-grace recovery uses.
    const SECOND_CODE: &str = "SPICE-ADOPT-22222-22222-22222-22222";

    let harness = Harness::new(24 * 60 * 60).await;
    let dir = tempfile::tempdir().unwrap();

    // First enroll declares the region.
    let mut config = harness.config(
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Some(ADOPTION_CODE.to_string()),
        Duration::from_hours(12),
    );
    config.instance_region = Some("us-west-2".to_string());
    let first = runtime_cloud_connect::enroll::enroll_now(&config)
        .await
        .expect("first enroll succeeds");
    assert_eq!(first.registration.region.as_deref(), Some("us-west-2"));

    harness
        .cloud
        .codes
        .lock()
        .await
        .insert(SECOND_CODE.to_string());

    let mut re_enroll = harness.config(
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Some(SECOND_CODE.to_string()),
        Duration::from_hours(12),
    );
    re_enroll.instance_region = None;
    let second = runtime_cloud_connect::enroll::enroll_now(&re_enroll)
        .await
        .expect("re-enroll without a region succeeds");

    let requests = harness.cloud.enroll_requests.lock().await.clone();
    assert_eq!(requests.len(), 2, "two enroll requests");
    assert!(
        requests[1].get("region").is_none(),
        "an omitted region must not appear on the wire at all — `null` would clear it"
    );
    assert_eq!(
        second.registration.region.as_deref(),
        Some("us-west-2"),
        "the region set by the first enroll must survive the re-enrol"
    );
}

/// `create_app` is meaningless without an app to name, so it must never
/// reach the wire alone — an invalid enroll request. Reachable by setting
/// `SPICE_CONNECT_ADOPT_CREATE` with no `SPICE_CONNECT_ADOPT_APP_NAME`
/// (the `--create` flag pair is guarded by clap, the env pair is not).
#[tokio::test]
async fn one_shot_enroll_omits_create_app_without_app_name() {
    let harness = Harness::new(24 * 60 * 60).await;
    let dir = tempfile::tempdir().unwrap();

    let mut config = harness.config(
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Some(ADOPTION_CODE.to_string()),
        Duration::from_hours(12),
    );
    config.adopt_app_name = None;
    config.adopt_create_app = true;

    let outcome = runtime_cloud_connect::enroll::enroll_now(&config)
        .await
        .expect("enroll succeeds unattached");
    assert_eq!(outcome.registration.app_name, None, "nothing was attached");

    let requests = harness.cloud.enroll_requests.lock().await.clone();
    assert_eq!(requests.len(), 1, "exactly one enroll request");
    assert!(
        requests[0].get("app_name").is_none(),
        "no app name was configured"
    );
    assert!(
        requests[0].get("create_app").is_none(),
        "create_app must not ride without app_name"
    );
}

/// A persistence failure lands *after* the cloud consumed the code to issue
/// the identity, so the staged copy is spent: it must be discarded, not left
/// for `status` to report as redeemable and a later `spiced` start to
/// re-present for a 401.
#[tokio::test]
async fn one_shot_enroll_discards_staged_code_when_identity_cannot_persist() {
    let harness = Harness::new(24 * 60 * 60).await;
    let dir = tempfile::tempdir().unwrap();
    let pending_path = dir.path().join("pending-adopt-code");
    std::fs::write(&pending_path, ADOPTION_CODE).unwrap();

    // The identity's parent is a regular file, so the directory for it
    // cannot be created and the issued identity cannot be written.
    let blocker = dir.path().join("blocker");
    std::fs::write(&blocker, b"not a directory").unwrap();

    let mut config = harness.config(
        blocker.join("identity.json"),
        dir.path().to_path_buf(),
        Some(ADOPTION_CODE.to_string()),
        Duration::from_hours(12),
    );
    config.pending_adopt_code_path = Some(pending_path.clone());

    let err = runtime_cloud_connect::enroll::enroll_now(&config)
        .await
        .expect_err("an unwritable identity path must fail the enroll");
    assert!(
        matches!(
            err,
            runtime_cloud_connect::enroll::EnrollNowError::Persist { .. }
        ),
        "expected a persistence failure, got: {err}"
    );
    assert!(
        !err.is_authoritative_rejection(),
        "a local persistence failure is not a cloud rejection"
    );
    assert!(
        !pending_path.exists(),
        "the code was consumed to issue the identity, so it must not stay staged"
    );
    assert_eq!(
        harness.cloud.enroll_requests.lock().await.len(),
        1,
        "the code was presented exactly once"
    );
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
                && c.hellos
                    .iter()
                    .skip(hellos_before)
                    .any(|(h, mtls)| h.identifier == ASSIGNED_ID && *mtls)
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
        let hb_ok = c.heartbeats.iter().any(|h| {
            h.identifier == ASSIGNED_ID
                && h.active_datasets == 2
                && h.active_models == 1
                // This handle cannot report status, so it must leave the phase
                // unspecified rather than inventing an "online".
                && h.phase == proto::RuntimePhase::Unspecified as i32
        });
        let tel_ok = c.telemetry.iter().any(|t| {
            t.identifier == ASSIGNED_ID
                // The dataset/model counters ride on the Heartbeat and only
                // there; the telemetry map is for everything else.
                && !t.metrics.contains_key("datasets_active")
                && !t.metrics.contains_key("models_active")
                && t.window_end.map(|ts| ts.seconds) >= t.window_start.map(|ts| ts.seconds)
        });
        (hb_ok, tel_ok)
    });
    assert!(hb_ok, "a heartbeat must carry the identifier + counters");
    assert!(
        tel_ok,
        "a telemetry frame must carry a well-ordered window and no heartbeat counters"
    );

    handle.shutdown().await;
}

/// The delivery path end to end: the client announces a per-connection key, the
/// (mock) gateway double-seals a payload to it exactly as the real one does, and
/// the opened secrets reach the runtime adapter alongside the spicepod.
#[tokio::test]
async fn apply_spicepod_delivers_double_sealed_secrets() {
    use cloud_connect_crypto::{RecipientKey, SealLayer, SecretAddress};
    use prost::Message as _;

    // The envelope's `command_id` is part of the outer AAD, so the seal below and
    // the dispatch must name the same one.
    const COMMAND_ID: &str = "cmd-apply-secrets";

    let harness = Harness::new(24 * 60 * 60).await;
    let dir = tempfile::tempdir().unwrap();
    let identity_path = dir.path().join("identity.json");
    let config = harness.config(
        identity_path.clone(),
        dir.path().to_path_buf(),
        Some(ADOPTION_CODE.to_string()),
        Duration::from_hours(12),
    );
    let (runtime, rt_state) = E2eRuntime::new();
    let (handle, identity) = enroll(&harness, &config, runtime).await;

    // The session key the client announced — the gateway's outer recipient.
    let captured = Arc::clone(&harness.gateway.captured);
    let announced = wait_until_async(Duration::from_secs(5), || {
        let captured = Arc::clone(&captured);
        async move { !captured.lock().await.secrets_keys.is_empty() }
    })
    .await;
    assert!(
        announced,
        "the client must announce a per-connection secrets key, or it can receive no secrets"
    );
    let session = with_captured!(captured, c => c.secrets_keys[0].clone());
    assert_eq!(session.kem_id, cloud_connect_crypto::KEM_ID);
    assert_eq!(session.aead_id, cloud_connect_crypto::AEAD_ID);

    // Inner: the control plane seals to the instance's *enrolled* key.
    let enrolled_pub =
        cloud_connect_crypto::EncryptionKeypair::from_pkcs8_pem(&identity.enc_private_key_pem)
            .expect("enrolled key parses");
    let plaintext = proto::SecretPayload {
        string_data: [("openai_key".to_string(), b"sk-e2e".to_vec())]
            .into_iter()
            .collect(),
    }
    .encode_to_vec();
    let inner_aad = SecretAddress::standalone(ASSIGNED_ID, enrolled_pub.key_id())
        .expect("inner address")
        .inner_aad();
    let inner_sealed = RecipientKey::from_public_key(enrolled_pub.public_key())
        .expect("inner recipient")
        .seal(SealLayer::Inner, &plaintext, &inner_aad)
        .expect("inner seal");
    let inner = proto::SealedSecretPayload {
        key_id: enrolled_pub.key_id().to_string(),
        enc: inner_sealed.enc,
        ciphertext: inner_sealed.ciphertext,
    };

    // Outer: the gateway seals that opaque envelope to the announced key.
    let outer_aad = SecretAddress::standalone(ASSIGNED_ID, &session.key_id)
        .expect("outer address")
        .outer_aad(COMMAND_ID)
        .expect("outer aad");
    let outer_sealed = RecipientKey::from_announcement(
        &session.key_id,
        session.kem_id,
        session.kdf_id,
        session.aead_id,
        &session.public_key,
    )
    .expect("outer recipient")
    .seal(SealLayer::Outer, &inner.encode_to_vec(), &outer_aad)
    .expect("outer seal");

    let yaml = "version: v2\nkind: Spicepod\nname: e2e-secrets\n";
    harness.gateway.outbound.lock().await.push_back(ctrl_id(
        COMMAND_ID,
        proto::control_message::Body::ApplySpicepod(proto::ApplySpicepod {
            spicepod_yaml: yaml.to_string(),
            deployment_version: None,
            sealed_secret_payload: Some(proto::SealedSecretPayload {
                key_id: session.key_id.clone(),
                enc: outer_sealed.enc,
                ciphertext: outer_sealed.ciphertext,
            }),
        }),
    ));

    let captured = Arc::clone(&harness.gateway.captured);
    let succeeded = wait_until_async(Duration::from_secs(5), || {
        let captured = Arc::clone(&captured);
        async move {
            captured
                .lock()
                .await
                .results
                .iter()
                .any(|r| r.command_id == COMMAND_ID && r.code == proto::ResultCode::Ok as i32)
        }
    })
    .await;
    assert!(succeeded, "the delivery must be applied, not refused");

    // The opened names reached the adapter; the value never leaves the process.
    let names = rt_state.lock().await.delivered_secret_names.clone();
    assert_eq!(
        names,
        Some(vec!["openai_key".to_string()]),
        "the opened secrets must reach the runtime adapter with the spicepod"
    );

    // No result may carry a secret value — in any payload arm, or in the
    // human-readable message.
    let rendered = with_captured!(captured, c => c
        .results
        .iter()
        .map(|r| {
            let payload = match &r.payload {
                Some(proto::command_result::Payload::Json(json)) => json.clone(),
                Some(proto::command_result::Payload::Text(text)) => text.clone(),
                Some(proto::command_result::Payload::Binary(bytes)) => {
                    String::from_utf8_lossy(bytes).into_owned()
                }
                None => String::new(),
            };
            format!("{} {payload}", r.message)
        })
        .collect::<Vec<_>>());
    assert!(
        rendered.iter().all(|r| !r.contains("sk-e2e")),
        "no command result may echo a delivered secret value: {rendered:?}"
    );

    handle.shutdown().await;
}

/// A payload the session key cannot open must fail the whole command — the
/// spicepod is not applied, rather than applied without the secrets it needs.
#[tokio::test]
async fn apply_spicepod_refuses_an_unopenable_payload() {
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

    // Garbage addressed to a key this session never announced.
    harness.gateway.outbound.lock().await.push_back(ctrl_id(
        "cmd-bad-secrets",
        proto::control_message::Body::ApplySpicepod(proto::ApplySpicepod {
            spicepod_yaml: "version: v2\nkind: Spicepod\nname: nope\n".to_string(),
            deployment_version: None,
            sealed_secret_payload: Some(proto::SealedSecretPayload {
                key_id: "0000000000000000".to_string(),
                enc: vec![0_u8; 32],
                ciphertext: vec![0_u8; 64],
            }),
        }),
    ));

    let captured = Arc::clone(&harness.gateway.captured);
    let failed = wait_until_async(Duration::from_secs(5), || {
        let captured = Arc::clone(&captured);
        async move {
            captured.lock().await.results.iter().any(|r| {
                r.command_id == "cmd-bad-secrets"
                    && r.code == proto::ResultCode::InvalidArgument as i32
            })
        }
    })
    .await;
    assert!(failed, "an unopenable payload must fail the command");

    assert!(
        rt_state.lock().await.applied_spicepod.is_none(),
        "the spicepod must NOT be applied when its secrets could not be opened"
    );

    handle.shutdown().await;
}

/// A deployment persists the spicepod and applies it by restarting — and the
/// result reaches the gateway *before* the runtime is asked to exit. If the
/// client exited first, every deployment would lose its validation outcome, and
/// an operator watching a deploy would see nothing at all.
#[tokio::test]
async fn apply_spicepod_persists_then_exits_to_restart() {
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
    harness.gateway.outbound.lock().await.push_back(ctrl_id(
        "cmd-apply",
        proto::control_message::Body::ApplySpicepod(proto::ApplySpicepod {
            spicepod_yaml: yaml.to_string(),
            sealed_secret_payload: None,
            deployment_version: Some(41),
        }),
    ));

    // Wait on the exit request, not on the result: the exit is the *last* step
    // of the apply, so once it has happened the result must already be out.
    let exited = wait_until_async(Duration::from_secs(5), || {
        let state = Arc::clone(&rt_state);
        async move { state.lock().await.exit_requested }
    })
    .await;
    assert!(
        exited,
        "a persisted deployment must ask the runtime to exit so the supervisor restarts it"
    );

    let captured = Arc::clone(&harness.gateway.captured);
    let result = with_captured!(captured, c => c
        .results
        .iter()
        .find(|r| r.command_id == "cmd-apply")
        .cloned())
    .expect("the apply result must be flushed to the gateway before the runtime exits");
    assert_eq!(
        result.code,
        proto::ResultCode::Ok as i32,
        "apply must succeed: {}",
        result.message
    );
    let Some(proto::command_result::Payload::Json(json)) = result.payload else {
        panic!(
            "ApplySpicepod must answer with a JSON payload, got {:?}",
            result.payload
        );
    };
    let meta: Value = serde_json::from_str(&json).expect("parse ApplySpicepod JSON payload");
    assert_eq!(meta["applied"], true);
    assert_eq!(
        meta["live"], false,
        "the deployment is persisted, not yet serving — the restart is what makes it live"
    );
    assert_eq!(meta["restart"], "required");
    assert_eq!(meta["deployment_version"], 41);

    // The runtime persisted the YAML to the canonical cloud-managed path, which
    // is what the restart comes back up on.
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

/// The reconciliation contract: an instance that came up on a deployment says
/// so in its `Hello`. The control plane cannot learn it from the command result
/// — the apply exits the process before the result is guaranteed to land.
#[tokio::test]
async fn hello_reports_the_applied_deployment_version() {
    let harness = Harness::new(24 * 60 * 60).await;
    let dir = tempfile::tempdir().unwrap();
    let config = harness.config(
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Some(ADOPTION_CODE.to_string()),
        Duration::from_hours(12),
    );
    let (runtime, _rt_state) = E2eRuntime::with_applied_version(41);
    let (handle, _identity) = enroll(&harness, &config, runtime).await;

    let captured = Arc::clone(&harness.gateway.captured);
    let hello = with_captured!(captured, c => c.hellos[0].0.clone());
    let state = hello
        .deploy_state
        .expect("a reporting instance always attaches a DeployState to its Hello");
    assert_eq!(
        state.applied_deployment_version,
        Some(41),
        "an instance serving a deployment must name it, or a deploy can never resolve"
    );
    assert_eq!(state.failed_deployment_version, None);
    assert!(state.failure_message.is_empty());

    handle.shutdown().await;
}

/// A freshly enrolled instance that has applied nothing announces
/// `deploy.versions` and reports a **zero**, not an absent version. The
/// capability is what tells the control plane which reconciliation path to take,
/// and it must be readable before any frame arrives — the gateway registers the
/// session first, and "reports versions, has applied nothing" would otherwise be
/// indistinguishable from "does not report versions" for the width of that window.
#[tokio::test]
async fn hello_announces_the_capability_and_reports_zero_before_any_deployment() {
    let harness = Harness::new(24 * 60 * 60).await;
    let dir = tempfile::tempdir().unwrap();
    let config = harness.config(
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Some(ADOPTION_CODE.to_string()),
        Duration::from_hours(12),
    );
    let (runtime, _rt_state) = E2eRuntime::new();
    let (handle, _identity) = enroll(&harness, &config, runtime).await;

    let captured = Arc::clone(&harness.gateway.captured);
    let hello = with_captured!(captured, c => c.hellos[0].0.clone());
    assert!(
        hello
            .capabilities
            .iter()
            .any(|c| c == runtime_cloud_connect::handlers::CAPABILITY_DEPLOY_VERSIONS),
        "an instance that reports deploy versions must announce it: {:?}",
        hello.capabilities
    );
    let state = hello.deploy_state.expect("the Hello always carries one");
    assert_eq!(
        state.applied_deployment_version,
        Some(0),
        "nothing applied is a zero, not an absence — the two settle a deployment differently"
    );

    handle.shutdown().await;
}

/// A spicepod rejected at validation restarts nothing, so no new `Hello`
/// follows and the open session's `Heartbeat` is the only route the failure has.
/// It names the version it refused and leaves the applied version reporting what
/// is still running.
#[tokio::test]
async fn a_rejected_deployment_is_reported_on_a_heartbeat() {
    let harness = Harness::new(24 * 60 * 60).await;
    let dir = tempfile::tempdir().unwrap();
    let config = harness.config(
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Some(ADOPTION_CODE.to_string()),
        Duration::from_hours(12),
    );
    let (runtime, rt_state) = E2eRuntime::with_applied_version(7);
    rt_state.lock().await.reject_next_apply = Some("invalid spicepod: bad yaml".to_string());
    let (handle, _identity) = enroll(&harness, &config, runtime).await;

    harness.gateway.outbound.lock().await.push_back(ctrl_id(
        "cmd-reject",
        proto::control_message::Body::ApplySpicepod(proto::ApplySpicepod {
            spicepod_yaml: "name: [unclosed".to_string(),
            sealed_secret_payload: None,
            deployment_version: Some(8),
        }),
    ));

    let captured = Arc::clone(&harness.gateway.captured);
    let reported = wait_until_async(Duration::from_secs(5), || {
        let captured = Arc::clone(&captured);
        async move {
            captured
                .lock()
                .await
                .heartbeats
                .iter()
                .any(|hb| hb.deploy_state.is_some())
        }
    })
    .await;
    assert!(
        reported,
        "a rejected deployment must be reported on a heartbeat — nothing restarts, so there is no other frame"
    );

    let state = with_captured!(captured, c => c
        .heartbeats
        .iter()
        .filter_map(|hb| hb.deploy_state.clone())
        .next_back())
    .expect("a heartbeat carried a deploy state");
    assert_eq!(state.failed_deployment_version, Some(8));
    assert_eq!(state.failure_message, "invalid spicepod: bad yaml");
    assert_eq!(
        state.applied_deployment_version,
        Some(7),
        "the applied version must still report what is running, not the refused one"
    );

    // The refusal was also answered as a command failure — the heartbeat report
    // is what settles the deployment, not a replacement for the result.
    let failed = with_captured!(captured, c => c
        .results
        .iter()
        .any(|r| r.command_id == "cmd-reject"
            && r.code == proto::ResultCode::InvalidArgument as i32));
    assert!(
        failed,
        "the rejected apply must also answer INVALID_ARGUMENT"
    );

    handle.shutdown().await;
}

/// Heartbeats do not repeat a state the control plane already holds: each one
/// *replaces* its record, so an unchanged report says nothing, and a heartbeat
/// carrying none leaves the previous report standing.
#[tokio::test]
async fn heartbeats_carry_a_deploy_state_only_when_it_changes() {
    let harness = Harness::new(24 * 60 * 60).await;
    let dir = tempfile::tempdir().unwrap();
    let config = harness.config(
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Some(ADOPTION_CODE.to_string()),
        Duration::from_hours(12),
    );
    let (runtime, _rt_state) = E2eRuntime::with_applied_version(3);
    let (handle, _identity) = enroll(&harness, &config, runtime).await;

    // The 150ms cadence gives several heartbeats inside this budget.
    let captured = Arc::clone(&harness.gateway.captured);
    let enough = wait_until_async(Duration::from_secs(5), || {
        let captured = Arc::clone(&captured);
        async move { captured.lock().await.heartbeats.len() >= 3 }
    })
    .await;
    assert!(enough, "expected >=3 heartbeats");

    let repeated = with_captured!(captured, c => c
        .heartbeats
        .iter()
        .filter(|hb| hb.deploy_state.is_some())
        .count());
    assert_eq!(
        repeated, 0,
        "the Hello already reported this state; the heartbeats behind it must add nothing"
    );

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
                    .filter(|(h, mtls)| h.identifier == ASSIGNED_ID && *mtls)
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
    assert!(
        !enrolled_identity.cache_key_b64.is_empty(),
        "enrollment must mint a local secrets-cache key"
    );

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
    // The encryption key rotates in the SAME request as the identity key, so
    // the cloud re-pins both in one atomic update. The endpoint requires the
    // field, so omitting it would fail every renewal.
    let renewed_enc_pubkey = renew_body["enc_pubkey_pem"]
        .as_str()
        .expect("renew must carry the rotated encryption public key");
    assert!(renewed_enc_pubkey.contains("PUBLIC KEY"));
    assert_ne!(
        renewed_enc_pubkey, enrolled_identity.enc_public_key_pem,
        "the renewal must mint a FRESH encryption keypair, not re-send the enrolled one"
    );
    assert!(
        !renew_body["pop_sig"].as_str().unwrap().is_empty(),
        "renew carries the current-key proof-of-possession"
    );
    // The cloud schema requires enc_pubkey_pem; without it renew returns 400.
    let enc_pubkey = renew_body["enc_pubkey_pem"]
        .as_str()
        .expect("renew must carry enc_pubkey_pem — the cloud Zod schema requires it");
    assert!(
        enc_pubkey.contains("PUBLIC KEY"),
        "renew carries an X25519 SPKI public key, got: {enc_pubkey}"
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
        renewed_identity.enc_private_key_pem, enrolled_identity.enc_private_key_pem,
        "the encryption key rotates with the identity key"
    );
    assert_eq!(
        renewed_identity.enc_previous_private_key_pem, enrolled_identity.enc_private_key_pem,
        "the outgoing encryption key must be retained for one rotation, so a payload sealed just \
         before the rotation still opens"
    );
    assert_eq!(
        renewed_identity.cache_key_b64, enrolled_identity.cache_key_b64,
        "the local cache key must NOT rotate, or the cache is stranded every ~12h"
    );
    // Both keys are reachable for an open; the current one is the rotated key.
    let keyring = renewed_identity
        .encryption_keyring()
        .expect("the rotated identity yields a keyring");
    assert!(
        keyring
            .select(
                cloud_connect_crypto::EncryptionKeypair::from_pkcs8_pem(
                    &enrolled_identity.enc_private_key_pem
                )
                .expect("pre-rotation key parses")
                .key_id()
            )
            .is_some(),
        "the pre-rotation key must still be selectable by its key id"
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

    // The encryption keypair rotates alongside the identity keypair on renewal:
    // verify it changed and that private/public keys correspond.
    assert_ne!(
        renewed_identity.enc_public_key_pem, enrolled_identity.enc_public_key_pem,
        "the encryption public key must rotate on renewal"
    );
    // The public key sent in the renew request is the same one persisted:
    // sending a stale public key while persisting a new private key would
    // break future secret delivery.
    assert_eq!(
        enc_pubkey, renewed_identity.enc_public_key_pem,
        "the persisted encryption public key must match what was sent to the cloud"
    );
    // Round-trip: the persisted private key must derive the same public key
    // that was sent to the cloud in the renew request.
    let loaded_keypair = cloud_connect_crypto::EncryptionKeypair::from_pkcs8_pem(
        &renewed_identity.enc_private_key_pem,
    )
    .expect("persisted encryption private key must load");
    assert_eq!(
        loaded_keypair.public_key_spki_pem(),
        renewed_identity.enc_public_key_pem,
        "persisted private key must derive the persisted public key"
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn remove_clears_identity_and_exits() {
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

    // Server issues Remove.
    harness.gateway.outbound.lock().await.push_back(ctrl_id(
        "cmd-remove",
        proto::control_message::Body::Remove(proto::Remove {}),
    ));

    // The client clears identity.json and the cloud-connect task exits; spiced
    // itself (here, the runtime handle) is untouched.
    let cleared = wait_until(Duration::from_secs(5), || !identity_path.exists()).await;
    assert!(cleared, "Remove must clear identity.json");

    let captured = Arc::clone(&harness.gateway.captured);
    let acked = wait_until_async(Duration::from_secs(5), || {
        let captured = Arc::clone(&captured);
        async move {
            captured
                .lock()
                .await
                .results
                .iter()
                .any(|r| r.command_id == "cmd-remove" && r.code == proto::ResultCode::Ok as i32)
        }
    })
    .await;
    assert!(acked, "server must see a successful Remove result");

    // shutdown() returns promptly because the task already exited on Remove.
    handle.shutdown().await;
}
