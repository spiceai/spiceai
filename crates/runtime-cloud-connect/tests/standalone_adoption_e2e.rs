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

//! Full standalone-adoption end-to-end suite for Spice Cloud Connect.
//!
//! Unlike `adoption_flow.rs` / `run_query.rs` (which run over an insecure
//! h2c channel and script a mock), this suite stands up a **real TLS** tonic
//! control server backed by a throwaway CA that actually **signs the client's
//! CSR**, and drives the real [`runtime_cloud_connect::CloudConnect`] client
//! through the whole lifecycle:
//!
//! 1. `enrollment` — adoption-code `Hello` carrying a CSR + public key, the
//!    server signs the CSR and returns the leaf + CA bundle in `Adopt`, the
//!    client persists the identity (leaf + key + ca bundle) to `identity.json`.
//! 2. `identity_reuse_across_restart` — a fresh client with no adoption code
//!    loads the persisted identity and reconnects over **mTLS** (leaf as the
//!    client certificate, empty credential).
//! 3. `heartbeat_and_telemetry_cadence` — the driver emits `Heartbeat` and
//!    `Telemetry` frames on their configured cadences.
//! 4. `run_query` — read-only dispatch with row/byte caps and an audit
//!    `EventLog` carrying the SHA-256 of the SQL (never the SQL itself).
//! 5. `apply_spicepod` — the YAML is written and hot-applied.
//! 6. `reconnect_over_mtls` — after the server drops the stream, the client
//!    reconnects, presenting its client certificate, and the reconnect `Hello`
//!    carries the identifier with an empty credential.
//! 7. `forget` — the server sends `Forget`, the client clears `identity.json`
//!    and the cloud-connect task exits while the (simulated) runtime stays up.
//!
//! Determinism: no fixed sleeps for correctness — every wait polls a captured
//! condition with a bounded timeout. Heartbeat / telemetry cadences are set to
//! sub-second values via the config so the suite runs in a couple of seconds.

#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::doc_markdown,
    clippy::struct_field_names,
    clippy::items_after_statements,
    clippy::too_many_lines,
    reason = "integration-test harness — readability over lint strictness"
)]

use std::collections::VecDeque;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use rcgen::{
    BasicConstraints, CertificateParams, CertificateSigningRequestParams, DnType,
    ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose, SanType,
};
use runtime_cloud_connect::config::CloudConnectConfig;
use runtime_cloud_connect::handlers::{QueryResult, RuntimeHandle};
use runtime_cloud_connect::identity::IdentityStore;
use runtime_cloud_connect::proto;
use runtime_cloud_connect::proto::cloud_connect_server::{CloudConnect, CloudConnectServer};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::{Certificate, Identity as TonicIdentity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming};

const ADOPTION_CODE: &str = "SPICE-ADOPT-E2E1-E2E2";
const ASSIGNED_ID: &str = "inst_e2e_standalone";

// --------------------------------------------------------------------------
// Throwaway PKI: a CA that signs the server cert AND the client CSRs.
// --------------------------------------------------------------------------

/// Ensure a process-wide rustls crypto provider is installed. tonic's server
/// builds its `ServerConfig` off the process default, which panics if none is
/// set; the client falls back to `ring` on its own. Idempotent.
fn ensure_crypto_provider() {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
}

/// A minimal issuing CA plus a server leaf it signed. Stands in for the dp's
/// FileCa/DevCa issuer. `issuer` is retained to sign client CSRs on demand.
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
            .push(DnType::CommonName, "spice-test-control-plane");
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

    /// Sign a client-submitted PKCS#10 CSR, returning the leaf PEM. `from_pem`
    /// verifies the CSR's self-signature, so this only succeeds if the client
    /// genuinely holds the private key it enrolled with.
    fn sign_csr(&self, csr_pem: &str) -> Result<String, rcgen::Error> {
        let csr = CertificateSigningRequestParams::from_pem(csr_pem)?;
        let leaf = csr.signed_by(&self.issuer)?;
        Ok(leaf.pem())
    }
}

// --------------------------------------------------------------------------
// Control server: real tonic CloudConnect/Stream over TLS with optional mTLS.
// --------------------------------------------------------------------------

#[derive(Default)]
struct Captured {
    /// Number of streams opened by the client (reconnect counter).
    stream_count: u32,
    /// The enrollment Hello (the one carrying a CSR).
    enroll_hello: Option<proto::Hello>,
    /// Reconnect Hellos and whether the client presented a cert (mTLS) on them.
    reconnect_hellos: Vec<(proto::Hello, bool)>,
    adopt_acks: Vec<proto::AdoptAck>,
    results: Vec<proto::CommandResult>,
    heartbeats: Vec<proto::Heartbeat>,
    telemetry: Vec<proto::Telemetry>,
    audits: Vec<proto::EventLog>,
}

#[derive(Clone)]
struct ControlServer {
    ca: Arc<TestCa>,
    captured: Arc<Mutex<Captured>>,
    /// Commands the server should push to the client on the current stream.
    /// Drained by a per-stream forwarder, so tests can enqueue at any time.
    outbound: Arc<Mutex<VecDeque<proto::ControlMessage>>>,
    /// When set, the server closes the stream right after handling the
    /// enrollment Hello + Adopt — used to force an mTLS reconnect.
    drop_after_enroll: Arc<AtomicBool>,
}

impl ControlServer {
    fn new(ca: Arc<TestCa>) -> Self {
        Self {
            ca,
            captured: Arc::new(Mutex::new(Captured::default())),
            outbound: Arc::new(Mutex::new(VecDeque::new())),
            drop_after_enroll: Arc::new(AtomicBool::new(false)),
        }
    }
}

fn ctrl(body: proto::control_message::Body) -> proto::ControlMessage {
    proto::ControlMessage { body: Some(body) }
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

#[async_trait]
impl CloudConnect for ControlServer {
    type StreamStream = ReceiverStream<Result<proto::ControlMessage, Status>>;

    async fn stream(
        &self,
        request: Request<Streaming<proto::ClientMessage>>,
    ) -> Result<Response<Self::StreamStream>, Status> {
        // Presence of client certs proves the transport is mutually
        // authenticated on this stream (the reconnect path).
        let has_client_cert = request.peer_certs().is_some_and(|certs| !certs.is_empty());

        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel::<Result<proto::ControlMessage, Status>>(32);

        let ca = Arc::clone(&self.ca);
        let captured = Arc::clone(&self.captured);
        let outbound = Arc::clone(&self.outbound);
        let drop_after_enroll = Arc::clone(&self.drop_after_enroll);

        captured.lock().await.stream_count += 1;

        tokio::spawn(async move {
            let mut forwarder: Option<tokio::task::JoinHandle<()>> = None;
            while let Ok(Some(msg)) = inbound.message().await {
                match msg.body {
                    Some(proto::client_message::Body::Hello(hello)) => {
                        let is_enrollment = !hello.csr_pem.is_empty();
                        if is_enrollment {
                            captured.lock().await.enroll_hello = Some(hello.clone());
                            // Validate the adoption code, then sign the CSR and
                            // hand back the leaf + issuing-CA bundle.
                            if hello.credential != ADOPTION_CODE {
                                let _ = tx
                                    .send(Err(Status::unauthenticated("bad adoption code")))
                                    .await;
                                return;
                            }
                            match ca.sign_csr(&hello.csr_pem) {
                                Ok(leaf) => {
                                    let adopt = proto::Adopt {
                                        command_id: "cmd-adopt".to_string(),
                                        assigned_identifier: ASSIGNED_ID.to_string(),
                                        identity_cert_pem: leaf,
                                        not_after_unix: now_unix() + 86_400 * 365,
                                        ca_bundle_pem: ca.ca_cert_pem.clone(),
                                    };
                                    if tx
                                        .send(Ok(ctrl(proto::control_message::Body::Adopt(adopt))))
                                        .await
                                        .is_err()
                                    {
                                        return;
                                    }
                                }
                                Err(err) => {
                                    let _ = tx
                                        .send(Err(Status::invalid_argument(format!(
                                            "CSR rejected: {err}"
                                        ))))
                                        .await;
                                    return;
                                }
                            }
                            // Force a reconnect (to exercise mTLS) by closing
                            // the stream once the Adopt has had time to land
                            // and the client has persisted its identity. The
                            // client persists synchronously on receiving Adopt
                            // (well within this window), so the reconnect finds
                            // an identity and connects over mTLS.
                            if drop_after_enroll.load(Ordering::SeqCst) {
                                tokio::time::sleep(Duration::from_millis(500)).await;
                                return;
                            }
                        } else {
                            captured
                                .lock()
                                .await
                                .reconnect_hellos
                                .push((hello.clone(), has_client_cert));
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

/// Bind an ephemeral TLS port and serve the control server on it. Returns the
/// bound address; the server runs until the returned task is dropped (i.e. the
/// test ends).
async fn spawn_tls_server(server: ControlServer) -> SocketAddr {
    ensure_crypto_provider();
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");

    let tls = ServerTlsConfig::new()
        .identity(TonicIdentity::from_pem(
            server.ca.server_cert_pem.clone(),
            server.ca.server_key_pem.clone(),
        ))
        // Verify client certs against the CA when presented, but do not
        // require them: the enrollment stream connects server-auth only.
        .client_ca_root(Certificate::from_pem(server.ca.ca_cert_pem.clone()))
        .client_auth_optional(true);

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
// behavior: Arrow-IPC results with row/byte caps, spicepod-to-disk apply).
// --------------------------------------------------------------------------

/// Test constant caps, mirroring the spiced adapter's shape.
const HARD_ROW_CAP: usize = 1_000;
const BYTE_BUDGET: usize = 64 * 1024;

#[derive(Default)]
struct E2eRuntimeState {
    last_sql: Option<String>,
    last_max_rows: Option<u32>,
    applied_spicepod: Option<(std::path::PathBuf, String)>,
}

struct E2eRuntime {
    state: Arc<Mutex<E2eRuntimeState>>,
    /// Number of rows the fabricated table "contains" — the source of
    /// truncation when it exceeds the effective cap.
    source_rows: usize,
}

impl E2eRuntime {
    fn new(source_rows: usize) -> (Arc<Self>, Arc<Mutex<E2eRuntimeState>>) {
        let state = Arc::new(Mutex::new(E2eRuntimeState::default()));
        (
            Arc::new(Self {
                state: Arc::clone(&state),
                source_rows,
            }),
            state,
        )
    }
}

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

#[async_trait]
impl RuntimeHandle for E2eRuntime {
    async fn active_datasets(&self) -> u32 {
        2
    }
    async fn active_models(&self) -> u32 {
        1
    }

    async fn execute_sql(&self, sql: &str, max_rows: u32) -> Result<QueryResult, String> {
        {
            let mut st = self.state.lock().await;
            st.last_sql = Some(sql.to_string());
            st.last_max_rows = Some(max_rows);
        }

        // Read-only surface: cloud-originated RunQuery must never mutate the
        // runtime. The real enforcement lives in the spiced adapter
        // (`query_builder(...).read_only(true)`); here we simulate its
        // rejection so the boundary behavior (sanitized error + audit) is
        // exercised end-to-end.
        let verb = sql.split_whitespace().next().unwrap_or("");
        if verb.eq_ignore_ascii_case("insert")
            || verb.eq_ignore_ascii_case("update")
            || verb.eq_ignore_ascii_case("delete")
            || verb.eq_ignore_ascii_case("create")
            || verb.eq_ignore_ascii_case("drop")
        {
            return Err(format!("read-only: refusing to execute `{sql}`"));
        }

        // Row cap: min(requested, hard). max_rows == 0 means "server default";
        // treat it as the hard cap here.
        let effective = if max_rows == 0 {
            HARD_ROW_CAP
        } else {
            (max_rows as usize).min(HARD_ROW_CAP)
        };
        let mut row_truncated = false;
        let emit = if self.source_rows > effective {
            row_truncated = true;
            effective
        } else {
            self.source_rows
        };

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ]);
        let ids: Vec<i64> = (0..emit as i64).collect();
        let labels: Vec<String> = (0..emit).map(|i| format!("row-{i}")).collect();
        let label_refs: Vec<&str> = labels.iter().map(String::as_str).collect();
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(label_refs)),
            ],
        )
        .expect("batch");

        // Byte budget: a coarse secondary guard. If the encoded stream would
        // exceed the budget, drop the batch (schema-only) and flag truncation.
        let full = encode_ipc(&schema, std::slice::from_ref(&batch));
        let (arrow_ipc, row_count, byte_truncated) = if full.len() > BYTE_BUDGET {
            (encode_ipc(&schema, &[]), 0, true)
        } else {
            (full, emit as u64, false)
        };

        Ok(QueryResult {
            arrow_ipc,
            row_count,
            truncated: row_truncated || byte_truncated,
        })
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
// Client config + polling helpers.
// --------------------------------------------------------------------------

fn tls_config(
    addr: SocketAddr,
    ca: &TestCa,
    identity_path: std::path::PathBuf,
    config_dir: std::path::PathBuf,
    adoption_code: Option<String>,
) -> CloudConnectConfig {
    CloudConnectConfig {
        endpoint: format!("https://127.0.0.1:{}", addr.port()),
        // Pin the test CA so server verification is hermetic (no dependence on
        // the host's native trust store). Also mirrors the dev/self-hosted
        // control-plane path.
        ca_cert_pem: Some(ca.ca_cert_pem.clone()),
        insecure: false,
        identity_path,
        config_dir,
        adoption_code,
        pending_adopt_code_path: None,
        runtime_version: "v0.0.0-e2e".to_string(),
        // Sub-second cadences keep the suite fast while still exercising the
        // periodic frame paths.
        heartbeat_interval: Duration::from_millis(150),
        telemetry_interval: Duration::from_millis(250),
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

/// Drive enrollment to completion and return the loaded identity.
async fn enroll(
    server: &ControlServer,
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
    let adopted = wait_until(Duration::from_secs(10), || identity_path.exists()).await;
    assert!(adopted, "identity.json must be written within 10s");

    // Wait for the AdoptAck to be observed server-side so the handshake is
    // fully settled before the test proceeds.
    let captured = Arc::clone(&server.captured);
    let acked = wait_until_async(Duration::from_secs(5), || {
        let captured = Arc::clone(&captured);
        async move { !captured.lock().await.adopt_acks.is_empty() }
    })
    .await;
    assert!(acked, "server must observe AdoptAck within 5s");

    let identity = IdentityStore::load_optional(&identity_path)
        .expect("load identity")
        .expect("identity present");
    (handle, identity)
}

// --------------------------------------------------------------------------
// Tests.
// --------------------------------------------------------------------------

#[tokio::test]
async fn enrollment_signs_csr_and_persists_identity() {
    let ca = Arc::new(TestCa::new());
    let server = ControlServer::new(Arc::clone(&ca));
    let addr = spawn_tls_server(server.clone()).await;

    let dir = tempfile::tempdir().unwrap();
    let config = tls_config(
        addr,
        &ca,
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Some(ADOPTION_CODE.to_string()),
    );

    let (runtime, _rt_state) = E2eRuntime::new(0);
    let (handle, identity) = enroll(&server, &config, runtime).await;

    // The enrollment Hello carried the CSR + public key BEFORE the cert was
    // issued (the ordering fix).
    let captured = Arc::clone(&server.captured);
    let hello =
        with_captured!(captured, c => c.enroll_hello.clone()).expect("server saw enrollment Hello");
    assert_eq!(hello.kind, proto::InstanceKind::Standalone as i32);
    assert!(
        hello.identifier.is_empty(),
        "pending Hello has no identifier"
    );
    assert_eq!(hello.credential, ADOPTION_CODE);
    assert!(
        hello.csr_pem.contains("CERTIFICATE REQUEST"),
        "Hello must carry a PKCS#10 CSR"
    );
    assert!(hello.agent_pubkey_pem.contains("PUBLIC KEY"));

    // The persisted identity binds the server-signed leaf to the client key,
    // and the issuing-CA bundle was captured for mTLS reconnects.
    assert_eq!(identity.identifier, ASSIGNED_ID);
    assert!(identity.identity_cert_pem.contains("BEGIN CERTIFICATE"));
    assert!(identity.private_key_pem.contains("PRIVATE KEY"));
    assert!(identity.ca_bundle_pem.contains("BEGIN CERTIFICATE"));
    assert!(
        identity.not_after_unix > now_unix(),
        "leaf must be unexpired"
    );
    // That the signed leaf genuinely chains to the pinned CA bundle is proved
    // operationally by the mTLS reconnect tests below: if it did not chain, the
    // server's `client_ca_root` verification would reject the handshake.

    // AdoptAck echoed the enrolled public key so the server can pin it.
    let ack = with_captured!(captured, c => c.adopt_acks.first().cloned()).expect("adopt ack");
    assert_eq!(ack.identifier, ASSIGNED_ID);
    assert_eq!(ack.identity_pubkey_pem, identity.public_key_pem);

    handle.shutdown().await;
}

#[tokio::test]
async fn identity_is_reused_across_restart_over_mtls() {
    let ca = Arc::new(TestCa::new());
    let server = ControlServer::new(Arc::clone(&ca));
    let addr = spawn_tls_server(server.clone()).await;

    let dir = tempfile::tempdir().unwrap();
    let identity_path = dir.path().join("identity.json");

    // First boot: enroll with the adoption code.
    let enroll_cfg = tls_config(
        addr,
        &ca,
        identity_path.clone(),
        dir.path().to_path_buf(),
        Some(ADOPTION_CODE.to_string()),
    );
    let (runtime, _s) = E2eRuntime::new(0);
    let (handle, _identity) = enroll(&server, &enroll_cfg, runtime).await;
    handle.shutdown().await; // simulate process stop; identity.json persists.

    // Second boot: NO adoption code — the client must load the persisted
    // identity and reconnect over mTLS, presenting its client certificate.
    let reuse_cfg = tls_config(
        addr,
        &ca,
        identity_path.clone(),
        dir.path().to_path_buf(),
        None,
    );
    let (runtime2, _s2) = E2eRuntime::new(0);
    let handle2 = runtime_cloud_connect::CloudConnect::start(reuse_cfg, runtime2)
        .await
        .expect("start")
        .expect("started (identity mode)");

    let captured = Arc::clone(&server.captured);
    let reconnected = wait_until_async(Duration::from_secs(10), || {
        let captured = Arc::clone(&captured);
        async move {
            captured
                .lock()
                .await
                .reconnect_hellos
                .iter()
                .any(|(h, mtls)| h.identifier == ASSIGNED_ID && *mtls)
        }
    })
    .await;
    assert!(
        reconnected,
        "restarted client must reconnect over mTLS with its identifier"
    );

    // And the reconnect Hello carries an empty credential (client cert authN).
    let ok = with_captured!(captured, c => {
        c.reconnect_hellos
            .iter()
            .any(|(h, mtls)| h.identifier == ASSIGNED_ID && *mtls && h.credential.is_empty())
    });
    assert!(ok, "reconnect Hello must have an empty credential");

    handle2.shutdown().await;
}

#[tokio::test]
async fn heartbeat_and_telemetry_cadence() {
    let ca = Arc::new(TestCa::new());
    let server = ControlServer::new(Arc::clone(&ca));
    let addr = spawn_tls_server(server.clone()).await;

    let dir = tempfile::tempdir().unwrap();
    let config = tls_config(
        addr,
        &ca,
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Some(ADOPTION_CODE.to_string()),
    );
    let (runtime, _s) = E2eRuntime::new(0);
    let (handle, _identity) = enroll(&server, &config, runtime).await;

    // With a 150ms heartbeat and 250ms telemetry cadence, several of each must
    // arrive within a couple of seconds.
    let captured = Arc::clone(&server.captured);
    let enough = wait_until_async(Duration::from_secs(5), || {
        let captured = Arc::clone(&captured);
        async move {
            let c = captured.lock().await;
            c.heartbeats.len() >= 3 && c.telemetry.len() >= 2
        }
    })
    .await;
    assert!(enough, "expected >=3 heartbeats and >=2 telemetry frames");

    // The frames carry the adopted identifier and the runtime counters.
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
async fn run_query_read_only_caps_and_audit() {
    let ca = Arc::new(TestCa::new());
    let server = ControlServer::new(Arc::clone(&ca));
    let addr = spawn_tls_server(server.clone()).await;

    let dir = tempfile::tempdir().unwrap();
    let config = tls_config(
        addr,
        &ca,
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Some(ADOPTION_CODE.to_string()),
    );
    // The fabricated table has more rows than the requested cap, so the
    // result must come back truncated.
    let (runtime, rt_state) = E2eRuntime::new(50);
    let (handle, _identity) = enroll(&server, &config, runtime).await;

    // (a) A read-only SELECT with a row cap below the source size.
    server
        .outbound
        .lock()
        .await
        .push_back(ctrl(proto::control_message::Body::RunQuery(
            proto::RunQuery {
                command_id: "cmd-select".to_string(),
                sql: "SELECT id, label FROM t".to_string(),
                max_rows: 10,
            },
        )));

    let captured = Arc::clone(&server.captured);
    let got = wait_until_async(Duration::from_secs(5), || {
        let captured = Arc::clone(&captured);
        async move {
            let c = captured.lock().await;
            c.results.iter().any(|r| r.command_id == "cmd-select")
                && c.audits.iter().any(|e| e.event_json.contains("cmd-select"))
        }
    })
    .await;
    assert!(got, "RunQuery result + audit must arrive within 5s");

    // The runtime received the raw max_rows the control plane requested.
    assert_eq!(rt_state.lock().await.last_max_rows, Some(10));

    let result = with_captured!(captured, c => c
        .results
        .iter()
        .find(|r| r.command_id == "cmd-select")
        .cloned())
    .expect("select result");
    assert!(result.success, "select must succeed: {}", result.error);

    // Metadata rides in payload_json; tabular data is native Arrow IPC.
    let meta: Value = serde_json::from_str(&result.payload_json).expect("meta json");
    assert_eq!(meta["row_count"], 10, "row cap applied");
    assert_eq!(meta["truncated"], true, "source exceeded the cap");
    let reader =
        StreamReader::try_new(std::io::Cursor::new(result.result_arrow_ipc.clone()), None).unwrap();
    let rows: usize = reader.map(|b| b.unwrap().num_rows()).sum();
    assert_eq!(rows, 10, "Arrow IPC carries exactly the capped rows");

    // The audit EventLog carries the SHA-256 of the SQL — never the SQL text.
    let audit = with_captured!(captured, c => c
        .audits
        .iter()
        .find(|e| e.event_json.contains("cmd-select"))
        .cloned())
    .expect("select audit");
    assert_eq!(audit.identifier, ASSIGNED_ID);
    let ap: Value = serde_json::from_str(&audit.event_json).unwrap();
    assert_eq!(ap["action"], "run_query");
    assert_eq!(ap["success"], true);
    assert_eq!(ap["row_count"], 10);
    assert_eq!(ap["truncated"], true);
    let sql_hash = ap["sql_hash"].as_str().unwrap();
    assert_eq!(sql_hash.len(), 64, "sha256 hex digest");
    assert!(
        !audit.event_json.contains("SELECT"),
        "audit must not leak SQL"
    );

    // (b) A mutating statement must be rejected (read-only surface) and the
    // failure audited without leaking the statement.
    server
        .outbound
        .lock()
        .await
        .push_back(ctrl(proto::control_message::Body::RunQuery(
            proto::RunQuery {
                command_id: "cmd-write".to_string(),
                sql: "DELETE FROM secrets WHERE id = 1".to_string(),
                max_rows: 0,
            },
        )));

    let got_err = wait_until_async(Duration::from_secs(5), || {
        let captured = Arc::clone(&captured);
        async move {
            captured
                .lock()
                .await
                .results
                .iter()
                .any(|r| r.command_id == "cmd-write")
        }
    })
    .await;
    assert!(got_err, "write result must arrive within 5s");

    let werr = with_captured!(captured, c => c
        .results
        .iter()
        .find(|r| r.command_id == "cmd-write")
        .cloned())
    .expect("write result");
    assert!(!werr.success, "mutating statement must be rejected");
    assert!(
        !werr.error.contains("secrets"),
        "sanitized error must not echo the SQL: {}",
        werr.error
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn apply_spicepod_hot_applies_and_persists() {
    let ca = Arc::new(TestCa::new());
    let server = ControlServer::new(Arc::clone(&ca));
    let addr = spawn_tls_server(server.clone()).await;

    let dir = tempfile::tempdir().unwrap();
    let config = tls_config(
        addr,
        &ca,
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Some(ADOPTION_CODE.to_string()),
    );
    let (runtime, rt_state) = E2eRuntime::new(0);
    let (handle, _identity) = enroll(&server, &config, runtime).await;

    let yaml = "version: v2\nkind: Spicepod\nname: e2e-cloud-managed\n";
    server
        .outbound
        .lock()
        .await
        .push_back(ctrl(proto::control_message::Body::ApplySpicepod(
            proto::ApplySpicepod {
                command_id: "cmd-apply".to_string(),
                spicepod_yaml: yaml.to_string(),
            },
        )));

    let captured = Arc::clone(&server.captured);
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
    let ca = Arc::new(TestCa::new());
    let server = ControlServer::new(Arc::clone(&ca));
    // Force the server to drop the enrollment stream right after Adopt so the
    // client must reconnect — this time over mTLS.
    server.drop_after_enroll.store(true, Ordering::SeqCst);
    let addr = spawn_tls_server(server.clone()).await;

    let dir = tempfile::tempdir().unwrap();
    let config = tls_config(
        addr,
        &ca,
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Some(ADOPTION_CODE.to_string()),
    );
    let (runtime, _s) = E2eRuntime::new(0);

    let handle = runtime_cloud_connect::CloudConnect::start(config.clone(), runtime)
        .await
        .expect("start")
        .expect("started");

    // The identity is persisted from the (soon-dropped) enrollment stream.
    let identity_path = config.identity_path.clone();
    assert!(
        wait_until(Duration::from_secs(10), || identity_path.exists()).await,
        "identity must persist before the reconnect"
    );

    // After the drop + backoff, the client reconnects with its identifier over
    // a mutually-authenticated stream.
    let captured = Arc::clone(&server.captured);
    let reconnected = wait_until_async(Duration::from_secs(15), || {
        let captured = Arc::clone(&captured);
        async move {
            let c = captured.lock().await;
            c.stream_count >= 2
                && c.reconnect_hellos.iter().any(|(h, mtls)| {
                    h.identifier == ASSIGNED_ID && *mtls && h.credential.is_empty()
                })
        }
    })
    .await;
    assert!(
        reconnected,
        "client must reopen a second, mutually-authenticated stream"
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn forget_clears_identity_and_exits() {
    let ca = Arc::new(TestCa::new());
    let server = ControlServer::new(Arc::clone(&ca));
    let addr = spawn_tls_server(server.clone()).await;

    let dir = tempfile::tempdir().unwrap();
    let identity_path = dir.path().join("identity.json");
    let config = tls_config(
        addr,
        &ca,
        identity_path.clone(),
        dir.path().to_path_buf(),
        Some(ADOPTION_CODE.to_string()),
    );
    let (runtime, _s) = E2eRuntime::new(0);
    let (handle, _identity) = enroll(&server, &config, runtime).await;
    assert!(identity_path.exists(), "identity present after enrollment");

    // Server issues Forget.
    server
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

    let captured = Arc::clone(&server.captured);
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
