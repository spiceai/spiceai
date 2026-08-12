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

//! Full standalone-enrollment end-to-end suite for Spice Cloud Connect
//! (enroll-first model, DR-025).
//!
//! Unlike `enrollment_flow.rs` (which runs the gateway over an insecure h2c
//! channel and returns canned enroll responses), this suite
//! stands up the full split control plane against the frozen canonical
//! Cloud Connect contract:
//!
//! - a **cloud mock** (axum, HTTP) implementing the canonical
//!   `/v1/cloud-connect/enroll` semantics: exactly one enrollment
//!   authority per request (a single-use `spice-enroll-` token, or a
//!   bearer session with `X-Org-Name`), a retry-safe **operation store**
//!   keyed by `Idempotency-Key` whose exact replay returns the same
//!   instance (evaluated before ordinary token rejection), token
//!   expiry/consumption/`expected_org` checks with the canonical
//!   `{code, error, retryable}` bodies, and CSR signing with a throwaway
//!   CA. `/v1/cloud-connect/renew` verifies the current-key
//!   proof-of-possession signature and re-issues over the new CSR
//!   (rotating the pinned key);
//! - a **gateway** (real TLS tonic server) that **requires mTLS** — the
//!   post-DR-025 gateway holds no CA and rejects certless connections —
//!   and multiplexes control commands.
//!
//! The suite drives [`runtime_cloud_connect::enroll::enroll_now`] (the
//! typed enrollment entry `spiced --token` uses) and the real
//! [`runtime_cloud_connect::CloudConnect`] client through the whole
//! lifecycle: enrollment, response-loss replay, new-token recovery of a
//! pending operation, existing-identity precedence, authority
//! exclusivity, redaction, identity reuse across restarts, heartbeats,
//! spicepod deployment, reconnect, renewal, and removal.
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

use std::collections::{HashMap, VecDeque};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use axum::http::HeaderMap;
use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use base64::Engine as _;
use rcgen::{
    BasicConstraints, CertificateParams, CertificateSigningRequestParams, DnType,
    ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose, PublicKeyData as _, SanType,
};
use runtime_cloud_connect::config::CloudConnectConfig;
use runtime_cloud_connect::enroll::{
    EnrollNowOutcome, EnrollmentAuthority, RetryPolicy, enroll_now,
};
use runtime_cloud_connect::enrollment_key::EnrollmentKey;
use runtime_cloud_connect::handlers::{
    ApplyOutcome, Capability, CommandError, MAX_QUERY_RESULT_BYTES, MAX_QUERY_ROWS, QueryOutcome,
    RuntimeHandle, SpicepodDeployment,
};
use runtime_cloud_connect::identity::IdentityStore;
use runtime_cloud_connect::proto;
use runtime_cloud_connect::proto::cloud_connect_server::{CloudConnect, CloudConnectServer};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Notify, mpsc};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::{Certificate, Identity as TonicIdentity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming};

/// The canonical single-use enrollment key the mock pre-registers.
const ENROLLMENT_KEY: &str = "spice-enroll-E2E0aaaabbbbccccddddeeeeffff0001";
/// A second registered key, for tests that need a fresh unconsumed one.
const SECOND_ENROLLMENT_KEY: &str = "spice-enroll-E2E0aaaabbbbccccddddeeeeffff0002";
/// The organization both registered keys are scoped to.
const ORG_NAME: &str = "acme";
/// The id the mock assigns to the first instance row it creates.
const ASSIGNED_ID: &str = "inst_e2e_1";
/// The bearer token the mock accepts for authenticated (logged-in) enrollment.
const SESSION_BEARER: &str = "session-bearer-e2e";

fn parse_key(raw: &str) -> EnrollmentKey {
    EnrollmentKey::parse(raw).expect("test key is canonical")
}

fn token_authority(raw: &str) -> EnrollmentAuthority {
    EnrollmentAuthority::Token {
        key: parse_key(raw),
        expected_org: None,
    }
}

/// The retry policy most tests use: generous enough for one transient
/// retry, far below the test timeout.
fn test_retry() -> RetryPolicy {
    RetryPolicy {
        deadline: Duration::from_secs(15),
    }
}

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
// Cloud mock: the canonical enroll + renew contract (state plane), backed by
// the TestCa. This is the frozen CLOUD-1/CLOUD-2 fixture the client is
// implemented against: an operation store keyed by `Idempotency-Key` whose
// exact replay returns the same instance, single-use org-scoped tokens with
// expiry/consumption/`expected_org` semantics, exactly one enrollment
// authority per request, and `{code, error, retryable}` error bodies.
// --------------------------------------------------------------------------

/// The registry-side state of one minted enrollment token.
#[derive(Clone)]
struct TokenState {
    org: String,
    expired: bool,
    /// The operation that consumed this token, when one has.
    consumed_by: Option<String>,
}

/// One stored enrollment operation: the canonical request hash
/// (fingerprint + identity public key, mirroring CLOUD-1) and the exact
/// response it produced, replayed verbatim for an exact retry.
#[derive(Clone)]
struct OperationRecord {
    request_hash: String,
    response: Value,
}

#[derive(Clone)]
struct CloudMock {
    ca: Arc<TestCa>,
    /// `gateway_addr` (host:port) handed out in enroll responses.
    gateway_addr: String,
    /// Validity (seconds) of issued leaves, as reported in `not_after`.
    leaf_validity_secs: i64,
    /// Minted tokens by plaintext value (the real cloud stores hashes; the
    /// mock's lookup semantics are identical).
    tokens: Arc<Mutex<HashMap<String, TokenState>>>,
    /// Enrollment operations by `Idempotency-Key`.
    operations: Arc<Mutex<HashMap<String, OperationRecord>>>,
    /// Number of instance registry rows created — the sibling detector.
    instances_created: Arc<Mutex<u32>>,
    /// While > 0, an enroll request is fully processed (operation stored,
    /// instance created, token consumed) but the response is replaced with a
    /// 503 — simulating a response lost on the wire. Decremented per use.
    drop_responses: Arc<Mutex<u32>>,
    /// While > 0, an enroll request is refused 503 BEFORE any processing —
    /// a plain transient outage. Decremented per use.
    unavailable_responses: Arc<Mutex<u32>>,
    /// Test-only gate that pauses one request after capture but before cloud
    /// processing, making overlapping local enrollment deterministic.
    pause_next_enroll: Arc<AtomicBool>,
    enroll_paused: Arc<Notify>,
    resume_enroll: Arc<Notify>,
    /// When set, enrollment returns a valid X.509 leaf for a different key
    /// than the submitted CSR, modeling an unusable committed response.
    issue_mismatched_enroll_certificate: Arc<AtomicBool>,
    /// The public key pinned at the last enroll/renew — the only key whose
    /// PoP signature authorizes a rotation (mirrors the cloud's pinning).
    pinned_point: Arc<Mutex<Option<Vec<u8>>>>,
    /// The region on the instance's registry row, standing in for the stored
    /// column: an enroll declaring a region writes it, one that declares none
    /// leaves it untouched.
    stored_region: Arc<Mutex<Option<String>>>,
    /// Captured request bodies and the headers that rode them.
    enroll_requests: Arc<Mutex<Vec<(Value, CapturedHeaders)>>>,
    renew_requests: Arc<Mutex<Vec<Value>>>,
}

/// The header facts the contract cares about, captured per enroll request.
#[derive(Clone, Debug)]
struct CapturedHeaders {
    idempotency_key: Option<String>,
    authorization: Option<String>,
    org_name: Option<String>,
}

impl CloudMock {
    fn new(ca: Arc<TestCa>, gateway_addr: String, leaf_validity_secs: i64) -> Self {
        let mut tokens = HashMap::new();
        for key in [ENROLLMENT_KEY, SECOND_ENROLLMENT_KEY] {
            tokens.insert(
                key.to_string(),
                TokenState {
                    org: ORG_NAME.to_string(),
                    expired: false,
                    consumed_by: None,
                },
            );
        }
        Self {
            ca,
            gateway_addr,
            leaf_validity_secs,
            tokens: Arc::new(Mutex::new(tokens)),
            operations: Arc::new(Mutex::new(HashMap::new())),
            instances_created: Arc::new(Mutex::new(0)),
            drop_responses: Arc::new(Mutex::new(0)),
            unavailable_responses: Arc::new(Mutex::new(0)),
            pause_next_enroll: Arc::new(AtomicBool::new(false)),
            enroll_paused: Arc::new(Notify::new()),
            resume_enroll: Arc::new(Notify::new()),
            issue_mismatched_enroll_certificate: Arc::new(AtomicBool::new(false)),
            pinned_point: Arc::new(Mutex::new(None)),
            stored_region: Arc::new(Mutex::new(None)),
            enroll_requests: Arc::new(Mutex::new(Vec::new())),
            renew_requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn not_after(&self) -> String {
        (chrono::Utc::now() + chrono::Duration::seconds(self.leaf_validity_secs)).to_rfc3339()
    }

    /// Mark a registered token expired (its plaintext still known, so a
    /// request presenting it is told `expired_token` rather than unknown).
    async fn expire_token(&self, token: &str) {
        if let Some(state) = self.tokens.lock().await.get_mut(token) {
            state.expired = true;
        }
    }
}

/// The canonical error body: `{code, error, retryable}`.
fn error_json(status: StatusCode, code: &str, message: &str) -> (StatusCode, Json<Value>) {
    let retryable = status.is_server_error()
        || status == StatusCode::TOO_MANY_REQUESTS
        || status == StatusCode::REQUEST_TIMEOUT;
    (
        status,
        Json(serde_json::json!({ "code": code, "error": message, "retryable": retryable })),
    )
}

async fn mock_enroll(
    State(mock): State<CloudMock>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let captured = CapturedHeaders {
        idempotency_key: headers
            .get("idempotency-key")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string),
        authorization: headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string),
        org_name: headers
            .get("x-org-name")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string),
    };
    mock.enroll_requests
        .lock()
        .await
        .push((body.clone(), captured.clone()));

    if mock.pause_next_enroll.swap(false, Ordering::SeqCst) {
        mock.enroll_paused.notify_one();
        mock.resume_enroll.notified().await;
    }

    // A plain transient outage: refused before any processing.
    {
        let mut unavailable = mock.unavailable_responses.lock().await;
        if *unavailable > 0 {
            *unavailable -= 1;
            return error_json(
                StatusCode::SERVICE_UNAVAILABLE,
                "internal",
                "temporarily unavailable",
            );
        }
    }

    if body["kind"].as_str() != Some("standalone") {
        return error_json(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "kind must be standalone or cluster",
        );
    }
    // The deleted enrollment-time project authority is rejected before any
    // token could be consumed.
    if body.get("app_name").is_some() || body.get("create_app").is_some() {
        return error_json(
            StatusCode::BAD_REQUEST,
            "unsupported_enrollment_field",
            "app_name/create_app are not accepted by this endpoint",
        );
    }
    // Exactly one enrollment authority.
    let has_token = body.get("token").is_some();
    let has_session = captured.authorization.is_some();
    if has_token == has_session {
        return error_json(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "exactly one of a login authorization or a token is required",
        );
    }
    let Some(operation_id) = captured.idempotency_key.clone().filter(|k| !k.is_empty()) else {
        return error_json(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "Idempotency-Key is required",
        );
    };
    let Some(csr_pem) = body["csr_pem"].as_str() else {
        return error_json(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "csr_pem is required",
        );
    };
    // Host facts are NOT NULL registry columns.
    for field in ["fingerprint", "hostname", "os", "arch", "runtime_version"] {
        if body["instance"][field].as_str().is_none_or(str::is_empty) {
            return error_json(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "Validation error",
            );
        }
    }
    if let Some(region) = body["region"].as_str()
        && !runtime_cloud_connect::is_valid_instance_region(region)
    {
        return error_json(
            StatusCode::BAD_REQUEST,
            "invalid_region",
            "region must be 2-64 lowercase letters, digits, or hyphens",
        );
    }

    // CLOUD-1: SHA-256 of the canonical fingerprint/public-key tuple. The
    // CSR carries the identity public key, so it stands in for it here.
    let request_hash = format!(
        "{}\u{1}{}",
        body["instance"]["fingerprint"].as_str().unwrap_or(""),
        csr_pem
    );

    // An exact operation/request replay is evaluated BEFORE ordinary
    // used/expiry rejection: it returns the recorded instance identity and
    // cannot create or retarget an instance. A new token presented with the
    // same operation recovers it the same way (consumed against the existing
    // instance rather than creating a sibling).
    {
        let operations = mock.operations.lock().await;
        if let Some(record) = operations.get(&operation_id) {
            if record.request_hash == request_hash {
                let response = record.response.clone();
                drop(operations);
                if let Some(token) = body["token"].as_str()
                    && let Some(state) = mock.tokens.lock().await.get_mut(token)
                    && state.consumed_by.is_none()
                {
                    state.consumed_by = Some(operation_id.clone());
                }
                return (StatusCode::OK, Json(response));
            }
            return error_json(
                StatusCode::CONFLICT,
                "idempotency_mismatch",
                "this operation exists with a different request",
            );
        }
    }

    // Resolve the authority to an organization.
    let org = if let Some(token) = body["token"].as_str() {
        let tokens = mock.tokens.lock().await;
        let Some(state) = tokens.get(token) else {
            // Deliberately model a hostile proxy/server that echoes the
            // rejected bearer. The client must redact it before constructing
            // any user-facing error even though the real cloud never echoes
            // credentials.
            let message = format!("unknown enrollment key {token}");
            return error_json(StatusCode::UNAUTHORIZED, "invalid_token", &message);
        };
        // expected_org is asserted BEFORE the key could be consumed.
        if let Some(expected) = body["expected_org"].as_str()
            && expected != state.org
        {
            return error_json(
                StatusCode::CONFLICT,
                "org_mismatch",
                "the enrollment key does not belong to the asserted organization; the key was not consumed",
            );
        }
        if state.expired {
            return error_json(
                StatusCode::GONE,
                "expired_token",
                "the enrollment key expired",
            );
        }
        if state.consumed_by.is_some() {
            return error_json(
                StatusCode::CONFLICT,
                "consumed_token",
                "the enrollment key was already used",
            );
        }
        state.org.clone()
    } else {
        // Authenticated session: the bearer must be known and the org header
        // names the selected organization. No minted key exists on this path.
        if captured.authorization.as_deref() != Some(&format!("Bearer {SESSION_BEARER}")) {
            return error_json(
                StatusCode::UNAUTHORIZED,
                "unauthenticated",
                "unknown session",
            );
        }
        let Some(org) = captured.org_name.clone().filter(|o| !o.is_empty()) else {
            return error_json(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "X-Org-Name is required for authenticated enrollment",
            );
        };
        org
    };

    let Ok((mut leaf_pem, point)) = mock.ca.sign_csr(csr_pem) else {
        return error_json(StatusCode::BAD_REQUEST, "invalid_request", "Malformed CSR");
    };
    if mock
        .issue_mismatched_enroll_certificate
        .load(Ordering::SeqCst)
    {
        let unrelated_key = KeyPair::generate().expect("generate mismatched response key");
        leaf_pem = CertificateParams::new(Vec::<String>::new())
            .expect("build mismatched response certificate")
            .self_signed(&unrelated_key)
            .expect("sign mismatched response certificate")
            .pem();
    }
    *mock.pinned_point.lock().await = Some(point);

    // Create the instance registry row.
    let instance_id = {
        let mut created = mock.instances_created.lock().await;
        *created += 1;
        format!("inst_e2e_{created}")
    };

    // The real cloud reports the region now stored on the row: the declared
    // one when the request carried it, otherwise whatever the row already
    // held (a re-enroll with no `region` leaves it alone). The mock stands in
    // for that stored value.
    let stored_region = match body["region"].as_str() {
        Some(region) => {
            *mock.stored_region.lock().await = Some(region.to_string());
            Some(region.to_string())
        }
        None => mock.stored_region.lock().await.clone(),
    };

    let mut response = serde_json::json!({
        "instance_id": instance_id,
        "identity_cert_pem": leaf_pem,
        "ca_bundle_pem": mock.ca.ca_cert_pem,
        "gateway_addr": mock.gateway_addr,
        "not_after": mock.not_after(),
        "organization": {"id": 42, "name": org},
        "portal": {"new_project_url": format!("https://cloud.test/{org}/new?instance={instance_id}")},
        "attachment": null,
    });
    if let Some(region) = stored_region {
        response["region"] = serde_json::Value::String(region);
    }

    // Store the operation, consume the token: the mutation is durable
    // whether or not the response below reaches the client.
    mock.operations.lock().await.insert(
        operation_id.clone(),
        OperationRecord {
            request_hash,
            response: response.clone(),
        },
    );
    if let Some(token) = body["token"].as_str()
        && let Some(state) = mock.tokens.lock().await.get_mut(token)
    {
        state.consumed_by = Some(operation_id);
    }

    // Response loss: everything above happened, but the client never hears.
    {
        let mut drops = mock.drop_responses.lock().await;
        if *drops > 0 {
            *drops -= 1;
            return error_json(
                StatusCode::SERVICE_UNAVAILABLE,
                "internal",
                "response lost after the enrollment was processed",
            );
        }
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
        return error_json(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "Validation error",
        );
    };
    if cert_pem.is_empty() || csr_pem.is_empty() || pop_sig.is_empty() {
        return error_json(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "Validation error",
        );
    }

    // Current-key proof-of-possession against the PINNED key (a cert is not
    // a secret; only the currently-pinned key may rotate the identity).
    let pinned = mock.pinned_point.lock().await.clone();
    let Some(pinned_point) = pinned else {
        return error_json(
            StatusCode::UNAUTHORIZED,
            "invalid_pop",
            "Current-key proof-of-possession failed",
        );
    };
    let Ok(signature) = base64::engine::general_purpose::STANDARD.decode(pop_sig) else {
        return error_json(
            StatusCode::UNAUTHORIZED,
            "invalid_pop",
            "Current-key proof-of-possession failed",
        );
    };
    let Ok(csr_der) = pem::parse(csr_pem) else {
        return error_json(StatusCode::BAD_REQUEST, "invalid_request", "Malformed CSR");
    };
    let verifier = aws_lc_rs::signature::UnparsedPublicKey::new(
        &aws_lc_rs::signature::ECDSA_P256_SHA256_ASN1,
        pinned_point,
    );
    if verifier.verify(csr_der.contents(), &signature).is_err() {
        return error_json(
            StatusCode::UNAUTHORIZED,
            "invalid_pop",
            "Current-key proof-of-possession failed",
        );
    }

    // Re-issue over the CSR's NEW key and pin it (the rotation).
    let Ok((leaf_pem, point)) = mock.ca.sign_csr(csr_pem) else {
        return error_json(StatusCode::BAD_REQUEST, "invalid_request", "Malformed CSR");
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
    /// The path, spicepod, and app id of the last apply.
    applied_spicepod: Option<(std::path::PathBuf, String, Option<String>)>,
    /// Names of the secrets delivered with the last applied spicepod, never
    /// values. `None` when the deployment carried no payload at all.
    delivered_secret_names: Option<Vec<String>>,
    /// Set when the client asked the runtime to exit and apply. The real
    /// adapter ends the process here; a test one records that it was asked, so
    /// the test can assert the result was flushed first.
    exit_requested: bool,
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
    fn supports(&self, capability: Capability) -> bool {
        capability == Capability::ApplySpicepod
    }

    async fn active_datasets(&self) -> u32 {
        2
    }
    async fn active_models(&self) -> u32 {
        1
    }

    async fn apply_spicepod(
        &self,
        deployment: SpicepodDeployment<'_>,
    ) -> Result<ApplyOutcome, CommandError> {
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
        self.state.lock().await.applied_spicepod = Some((
            path.clone(),
            deployment.spicepod_yaml.to_string(),
            deployment.app_id.map(str::to_string),
        ));
        Ok(ApplyOutcome::exit_to_apply(serde_json::json!({
            "path": path.display().to_string(),
            "applied": true,
            "live": false,
            "restart": "required",
        })))
    }

    async fn exit_to_apply(&self) {
        self.state.lock().await.exit_requested = true;
    }
}

// --------------------------------------------------------------------------
// Runtime handle for the ExecuteQuery path. Records what the client handed it and
// returns a scripted outcome, so the tests below observe the client's own
// behavior (clamping, the single slot, the byte cap) rather than a query
// engine's.
// --------------------------------------------------------------------------

#[derive(Default)]
struct QueryRuntimeState {
    /// The `max_rows` value of every call, in order — the clamp is the
    /// client's job, so this is what proves it happened before the handle ran.
    max_rows_seen: Vec<u32>,
    /// The SQL of every call, so a test can confirm the statement reached the
    /// handle intact.
    sql_seen: Vec<String>,
}

struct QueryRuntime {
    state: Arc<Mutex<QueryRuntimeState>>,
    can_query: bool,
    /// What `execute_query` answers with.
    reply: Mutex<Option<Result<QueryOutcome, CommandError>>>,
    /// Released to let an in-flight `execute_query` return. `None` returns at once.
    release: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

impl QueryRuntime {
    /// A handle that can query and answers every call with `bytes`.
    fn returning(bytes: Vec<u8>, row_count: u64) -> (Arc<Self>, Arc<Mutex<QueryRuntimeState>>) {
        Self::build(
            true,
            Some(Ok(QueryOutcome {
                arrow_ipc: bytes,
                row_count,
            })),
            None,
        )
    }

    /// A handle that can query but blocks inside `execute_query` until the returned
    /// sender fires — how a test holds the single query slot open.
    fn blocking() -> (
        Arc<Self>,
        Arc<Mutex<QueryRuntimeState>>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let (runtime, state) = Self::build(
            true,
            Some(Ok(QueryOutcome {
                arrow_ipc: b"held".to_vec(),
                row_count: 1,
            })),
            Some(rx),
        );
        (runtime, state, tx)
    }

    /// A handle that cannot query at all.
    fn incapable() -> (Arc<Self>, Arc<Mutex<QueryRuntimeState>>) {
        Self::build(false, None, None)
    }

    fn build(
        can_query: bool,
        reply: Option<Result<QueryOutcome, CommandError>>,
        release: Option<tokio::sync::oneshot::Receiver<()>>,
    ) -> (Arc<Self>, Arc<Mutex<QueryRuntimeState>>) {
        let state = Arc::new(Mutex::new(QueryRuntimeState::default()));
        (
            Arc::new(Self {
                state: Arc::clone(&state),
                can_query,
                reply: Mutex::new(reply),
                release: Mutex::new(release),
            }),
            state,
        )
    }
}

#[async_trait]
impl RuntimeHandle for QueryRuntime {
    fn supports(&self, capability: Capability) -> bool {
        match capability {
            Capability::ExecuteQuery => self.can_query,
            // GetRuntimeInfo needs no capability, so this keeps the handle to
            // exactly the one command under test.
            _ => false,
        }
    }

    async fn execute_query(&self, sql: &str, max_rows: u32) -> Result<QueryOutcome, CommandError> {
        {
            let mut state = self.state.lock().await;
            state.max_rows_seen.push(max_rows);
            state.sql_seen.push(sql.to_string());
        }
        if let Some(release) = self.release.lock().await.take() {
            let _ = release.await;
        }
        self.reply
            .lock()
            .await
            .take()
            .unwrap_or_else(|| Err(CommandError::failed("no scripted reply left")))
    }
}

/// Enroll a client whose handle is a [`QueryRuntime`], and return the running
/// client plus the temp dir keeping its identity alive.
async fn enroll_query_runtime(
    harness: &Harness,
    runtime: Arc<dyn RuntimeHandle>,
) -> (runtime_cloud_connect::CloudConnect, tempfile::TempDir) {
    enroll_query_runtime_with_deadline(harness, runtime, Duration::from_mins(1)).await
}

/// As [`enroll_query_runtime`], with the `ExecuteQuery` deadline set explicitly so
/// a test can exercise it without waiting out the production value.
async fn enroll_query_runtime_with_deadline(
    harness: &Harness,
    runtime: Arc<dyn RuntimeHandle>,
    query_deadline: Duration,
) -> (runtime_cloud_connect::CloudConnect, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = harness.config(
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Duration::from_hours(12),
    );
    config.query_deadline = query_deadline;
    let (handle, _identity) = enroll(harness, &config, runtime).await;
    (handle, dir)
}

/// Poll for the `CommandResult` correlated to `command_id`.
async fn await_result(
    captured: &Arc<Mutex<Captured>>,
    command_id: &str,
) -> Option<proto::CommandResult> {
    let found = wait_until_async(Duration::from_secs(5), || {
        let captured = Arc::clone(captured);
        let command_id = command_id.to_string();
        async move {
            captured
                .lock()
                .await
                .results
                .iter()
                .any(|r| r.command_id == command_id)
        }
    })
    .await;
    if !found {
        return None;
    }
    let c = captured.lock().await;
    c.results
        .iter()
        .find(|r| r.command_id == command_id)
        .cloned()
}

/// The capability list on the most recent `Hello`.
async fn advertised_capabilities(captured: &Arc<Mutex<Captured>>) -> Vec<String> {
    let c = captured.lock().await;
    c.hellos
        .last()
        .map(|(hello, _)| hello.capabilities.clone())
        .expect("a Hello must have been captured")
}

fn execute_query(sql: &str, max_rows: u32) -> proto::control_message::Body {
    proto::control_message::Body::ExecuteQuery(proto::ExecuteQuery {
        sql: sql.to_string(),
        max_rows,
    })
}

/// A successful query comes back on the `binary` arm, byte-for-byte what the
/// runtime encoded — the client is a courier for the Arrow IPC stream, not a
/// re-encoder of it.
#[tokio::test]
async fn execute_query_returns_the_runtime_bytes_on_the_binary_arm() {
    let harness = Harness::new(24 * 60 * 60).await;
    let payload = b"ARROW-IPC-STREAM-BYTES".to_vec();
    let (runtime, state) = QueryRuntime::returning(payload.clone(), 3);
    let (handle, _dir) = enroll_query_runtime(&harness, runtime).await;

    harness
        .gateway
        .outbound
        .lock()
        .await
        .push_back(ctrl_id("cmd-query", execute_query("SELECT 1", 10)));

    let result = await_result(&harness.gateway.captured, "cmd-query")
        .await
        .expect("the client must answer an ExecuteQuery");
    assert_eq!(
        result.code,
        proto::ResultCode::Ok as i32,
        "query must succeed: {}",
        result.message
    );
    assert_eq!(
        result.payload,
        Some(proto::command_result::Payload::Binary(payload)),
        "the Arrow IPC stream must ride on the binary arm, unmodified"
    );
    assert_eq!(state.lock().await.sql_seen, vec!["SELECT 1".to_string()]);

    handle.shutdown().await;
}

/// The row cap is the client's to enforce: zero means the default, and a
/// request above the cap is clamped before the runtime ever sees it. A runtime
/// handed 500 cannot return 100_000 rows even if the caller asked for them.
#[tokio::test]
async fn execute_query_clamps_the_row_limit_before_the_runtime_runs() {
    for (requested, expected) in [(0_u32, MAX_QUERY_ROWS), (10, 10), (100_000, MAX_QUERY_ROWS)] {
        let harness = Harness::new(24 * 60 * 60).await;
        let (runtime, state) = QueryRuntime::returning(b"rows".to_vec(), 1);
        let (handle, _dir) = enroll_query_runtime(&harness, runtime).await;

        harness
            .gateway
            .outbound
            .lock()
            .await
            .push_back(ctrl_id("cmd-clamp", execute_query("SELECT 1", requested)));

        await_result(&harness.gateway.captured, "cmd-clamp")
            .await
            .expect("the client must answer an ExecuteQuery");
        assert_eq!(
            state.lock().await.max_rows_seen,
            vec![expected],
            "a request for {requested} rows must reach the runtime as {expected}"
        );

        handle.shutdown().await;
    }
}

/// A second query while one is in flight is refused before it executes — the
/// runtime handle must be called exactly once, not queued behind the first.
#[tokio::test]
async fn execute_query_answers_busy_without_executing_a_second_query() {
    let harness = Harness::new(24 * 60 * 60).await;
    let (runtime, state, release) = QueryRuntime::blocking();
    let (handle, _dir) = enroll_query_runtime(&harness, runtime).await;

    harness
        .gateway
        .outbound
        .lock()
        .await
        .push_back(ctrl_id("cmd-first", execute_query("SELECT 1", 10)));

    // Wait until the first query is genuinely inside the handle, so the second
    // one races a held slot rather than an empty one.
    let running = wait_until_async(Duration::from_secs(5), || {
        let state = Arc::clone(&state);
        async move { !state.lock().await.max_rows_seen.is_empty() }
    })
    .await;
    assert!(running, "the first query must reach the runtime handle");

    harness
        .gateway
        .outbound
        .lock()
        .await
        .push_back(ctrl_id("cmd-second", execute_query("SELECT 2", 10)));

    let second = await_result(&harness.gateway.captured, "cmd-second")
        .await
        .expect("a concurrent query must be answered, not dropped");
    assert_eq!(
        second.code,
        proto::ResultCode::Busy as i32,
        "a concurrent query must be answered busy: {}",
        second.message
    );
    assert!(
        second.payload.is_none(),
        "a busy answer carries no payload, got {:?}",
        second.payload
    );
    assert_eq!(
        state.lock().await.max_rows_seen.len(),
        1,
        "the second query must be refused BEFORE execution, not queued"
    );

    // Releasing the first query frees the slot again.
    let _ = release.send(());
    let first = await_result(&harness.gateway.captured, "cmd-first")
        .await
        .expect("the first query must still answer");
    assert_eq!(first.code, proto::ResultCode::Ok as i32);

    handle.shutdown().await;
}

/// Query work runs off the control-message pump.
///
/// The command dispatched mid-query is what proves it: awaiting the query in
/// the pump stops every later command from being read at all. Heartbeats ride
/// their own task and would survive a blocked pump, so they are asserted for
/// the acceptance criterion rather than as evidence of where the query runs.
#[tokio::test]
async fn heartbeats_and_commands_stay_live_while_a_query_runs() {
    let harness = Harness::new(24 * 60 * 60).await;
    let (runtime, state, release) = QueryRuntime::blocking();
    let (handle, _dir) = enroll_query_runtime(&harness, runtime).await;

    harness
        .gateway
        .outbound
        .lock()
        .await
        .push_back(ctrl_id("cmd-slow", execute_query("SELECT 1", 10)));

    let running = wait_until_async(Duration::from_secs(5), || {
        let state = Arc::clone(&state);
        async move { !state.lock().await.max_rows_seen.is_empty() }
    })
    .await;
    assert!(running, "the query must reach the runtime handle");

    let captured = Arc::clone(&harness.gateway.captured);
    let before = captured.lock().await.heartbeats.len();

    // An unrelated command dispatched while the query is stuck must still be
    // answered by the pump.
    harness.gateway.outbound.lock().await.push_back(ctrl_id(
        "cmd-info",
        proto::control_message::Body::GetRuntimeInfo(proto::GetRuntimeInfo {}),
    ));
    let info = await_result(&captured, "cmd-info")
        .await
        .expect("the pump must answer other commands while a query runs");
    assert_eq!(info.code, proto::ResultCode::Ok as i32);

    let beating = wait_until_async(Duration::from_secs(5), || {
        let captured = Arc::clone(&captured);
        async move { captured.lock().await.heartbeats.len() > before + 1 }
    })
    .await;
    assert!(
        beating,
        "heartbeats must keep flowing while a query is in flight"
    );

    let _ = release.send(());
    handle.shutdown().await;
}

/// A result over the byte cap is refused at the contract boundary with no
/// payload at all — a partial result would look to the caller like the whole
/// answer.
#[tokio::test]
async fn execute_query_refuses_an_oversized_result_without_partial_data() {
    let harness = Harness::new(24 * 60 * 60).await;
    let oversized = vec![0_u8; MAX_QUERY_RESULT_BYTES + 1];
    let (runtime, _state) = QueryRuntime::returning(oversized, 1);
    let (handle, _dir) = enroll_query_runtime(&harness, runtime).await;

    harness
        .gateway
        .outbound
        .lock()
        .await
        .push_back(ctrl_id("cmd-big", execute_query("SELECT 1", 10)));

    let result = await_result(&harness.gateway.captured, "cmd-big")
        .await
        .expect("an oversized result must still be answered");
    assert_eq!(
        result.code,
        proto::ResultCode::ResultTooLarge as i32,
        "an oversized result must be typed, not a generic failure: {}",
        result.message
    );
    assert!(
        result.payload.is_none(),
        "an oversized result must carry no partial payload, got {:?}",
        result.payload
    );

    handle.shutdown().await;
}

/// The row cap is re-checked against what the handle actually returned. A
/// handle that ignores the limit it was given must not have its result
/// forwarded — the whole point of holding the limits at the instance is that
/// nothing downstream re-checks them.
#[tokio::test]
async fn execute_query_refuses_a_result_with_more_rows_than_the_limit() {
    let harness = Harness::new(24 * 60 * 60).await;
    // Small payload, but it claims far more rows than the 10 that were asked
    // for — an honest byte count with a dishonest row count.
    let (runtime, _state) = QueryRuntime::returning(b"rows".to_vec(), 5_000);
    let (handle, _dir) = enroll_query_runtime(&harness, runtime).await;

    harness
        .gateway
        .outbound
        .lock()
        .await
        .push_back(ctrl_id("cmd-toomany", execute_query("SELECT 1", 10)));

    let result = await_result(&harness.gateway.captured, "cmd-toomany")
        .await
        .expect("an over-limit result must still be answered");
    assert_eq!(
        result.code,
        proto::ResultCode::Internal as i32,
        "a handle breaking the row cap is an instance fault: {}",
        result.message
    );
    assert!(
        result.payload.is_none(),
        "an over-limit result must not be forwarded, got {:?}",
        result.payload
    );

    handle.shutdown().await;
}

/// A runtime that cannot query neither advertises `execute_query` nor pretends to
/// answer one — and the session survives the refusal.
#[tokio::test]
async fn execute_query_is_unsupported_when_the_runtime_cannot_query() {
    let harness = Harness::new(24 * 60 * 60).await;
    let (runtime, state) = QueryRuntime::incapable();
    let (handle, _dir) = enroll_query_runtime(&harness, runtime).await;

    let captured = Arc::clone(&harness.gateway.captured);
    let advertised = advertised_capabilities(&captured).await;
    assert!(
        !advertised.contains(&"execute_query".to_string()),
        "a runtime that cannot query must not advertise execute_query: {advertised:?}"
    );

    harness
        .gateway
        .outbound
        .lock()
        .await
        .push_back(ctrl_id("cmd-nope", execute_query("SELECT 1", 10)));

    let result = await_result(&captured, "cmd-nope")
        .await
        .expect("an unsupported query must still be answered");
    assert_eq!(
        result.code,
        proto::ResultCode::Unsupported as i32,
        "an unsupported query must be typed unsupported: {}",
        result.message
    );
    assert!(
        state.lock().await.max_rows_seen.is_empty(),
        "an unsupported query must never reach the runtime handle"
    );

    // The refusal must not tear the session down: existing commands keep working.
    harness.gateway.outbound.lock().await.push_back(ctrl_id(
        "cmd-after",
        proto::control_message::Body::GetRuntimeInfo(proto::GetRuntimeInfo {}),
    ));
    let after = await_result(&captured, "cmd-after")
        .await
        .expect("the session must survive an unsupported query");
    assert_eq!(after.code, proto::ResultCode::Ok as i32);

    handle.shutdown().await;
}

/// A capable runtime advertises `execute_query` and announces the protocol revision
/// that carries it. The gateway gates dispatch on the capability, so the two
/// must appear together.
#[tokio::test]
async fn a_querying_runtime_advertises_execute_query() {
    let harness = Harness::new(24 * 60 * 60).await;
    let (runtime, _state) = QueryRuntime::returning(b"rows".to_vec(), 1);
    let (handle, _dir) = enroll_query_runtime(&harness, runtime).await;

    let captured = Arc::clone(&harness.gateway.captured);
    let advertised = advertised_capabilities(&captured).await;
    assert!(
        advertised.contains(&"execute_query".to_string()),
        "a querying runtime must advertise execute_query: {advertised:?}"
    );
    let protocol_version = captured
        .lock()
        .await
        .hellos
        .last()
        .map(|(hello, _)| hello.protocol_version)
        .expect("a Hello must have been captured");
    assert_eq!(
        protocol_version,
        runtime_cloud_connect::PROTOCOL_VERSION,
        "the announced revision must be the one that carries execute_query"
    );

    handle.shutdown().await;
}

/// An empty statement is the caller's mistake and is refused before the slot is
/// taken, so a stream of blank queries cannot lock the instance out of real
/// ones.
#[tokio::test]
async fn execute_query_rejects_an_empty_statement() {
    let harness = Harness::new(24 * 60 * 60).await;
    let (runtime, state) = QueryRuntime::returning(b"rows".to_vec(), 1);
    let (handle, _dir) = enroll_query_runtime(&harness, runtime).await;

    harness
        .gateway
        .outbound
        .lock()
        .await
        .push_back(ctrl_id("cmd-empty", execute_query("   \n\t ", 10)));

    let result = await_result(&harness.gateway.captured, "cmd-empty")
        .await
        .expect("an empty query must be answered");
    assert_eq!(
        result.code,
        proto::ResultCode::InvalidArgument as i32,
        "an empty query is the caller's mistake: {}",
        result.message
    );
    assert!(
        state.lock().await.max_rows_seen.is_empty(),
        "an empty query must never reach the runtime handle"
    );

    // The slot was not consumed: a real query still runs.
    harness
        .gateway
        .outbound
        .lock()
        .await
        .push_back(ctrl_id("cmd-real", execute_query("SELECT 1", 10)));
    let real = await_result(&harness.gateway.captured, "cmd-real")
        .await
        .expect("a real query must still run after an empty one");
    assert_eq!(real.code, proto::ResultCode::Ok as i32);

    handle.shutdown().await;
}

/// A query that never returns is abandoned at the deadline and, crucially,
/// gives the slot back. Without the deadline the single in-flight slot would be
/// held for the life of the process and every later query would answer busy —
/// there is no cancellation command to rescue it.
#[tokio::test]
async fn a_query_that_never_returns_is_abandoned_and_frees_the_slot() {
    let harness = Harness::new(24 * 60 * 60).await;
    // The sender is dropped at the end of this scope, but `blocking()` holds
    // the receiver, so the query never completes on its own.
    let (runtime, state, release) = QueryRuntime::blocking();
    let (handle, _dir) =
        enroll_query_runtime_with_deadline(&harness, runtime, Duration::from_millis(300)).await;

    harness
        .gateway
        .outbound
        .lock()
        .await
        .push_back(ctrl_id("cmd-hang", execute_query("SELECT 1", 10)));

    let hung = await_result(&harness.gateway.captured, "cmd-hang")
        .await
        .expect("a hung query must still be answered");
    assert_eq!(
        hung.code,
        proto::ResultCode::Failed as i32,
        "a query past the deadline is retryable, not the caller's mistake: {}",
        hung.message
    );
    assert!(hung.payload.is_none(), "an abandoned query carries no data");

    // The slot must be back: a second query runs rather than answering busy.
    harness
        .gateway
        .outbound
        .lock()
        .await
        .push_back(ctrl_id("cmd-after-hang", execute_query("SELECT 2", 10)));
    let after = await_result(&harness.gateway.captured, "cmd-after-hang")
        .await
        .expect("a query after the deadline must be answered");
    assert_ne!(
        after.code,
        proto::ResultCode::Busy as i32,
        "the deadline must free the query slot, but the next query was refused as busy"
    );
    assert_eq!(
        state.lock().await.max_rows_seen.len(),
        2,
        "the second query must actually reach the runtime handle"
    );

    drop(release);
    handle.shutdown().await;
}

/// The failure classes a query can produce reach the control plane as their own
/// codes, so the portal can tell a bad statement from a busy instance from a
/// broken one without reading the English.
#[tokio::test]
async fn execute_query_maps_runtime_failures_onto_their_own_codes() {
    for (error, expected) in [
        (
            CommandError::invalid_argument("Query failed: no such column"),
            proto::ResultCode::InvalidArgument,
        ),
        (
            CommandError::result_too_large("too big"),
            proto::ResultCode::ResultTooLarge,
        ),
        (
            CommandError::internal("encoder fault"),
            proto::ResultCode::Internal,
        ),
        (
            CommandError::failed("the source is unreachable"),
            proto::ResultCode::Failed,
        ),
    ] {
        let harness = Harness::new(24 * 60 * 60).await;
        let (runtime, _state) = QueryRuntime::build(true, Some(Err(error)), None);
        let (handle, _dir) = enroll_query_runtime(&harness, runtime).await;

        harness
            .gateway
            .outbound
            .lock()
            .await
            .push_back(ctrl_id("cmd-err", execute_query("SELECT 1", 10)));

        let result = await_result(&harness.gateway.captured, "cmd-err")
            .await
            .expect("a failing query must be answered");
        assert_eq!(
            result.code, expected as i32,
            "unexpected code for {}: {}",
            result.message, result.code
        );
        assert!(result.payload.is_none(), "a failure carries no payload");

        handle.shutdown().await;
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
            instance_region: None,
            runtime_version: "v0.0.0-e2e".to_string(),
            // Sub-second cadences keep the suite fast while still exercising
            // the periodic frame paths.
            heartbeat_interval: Duration::from_millis(150),
            telemetry_interval: Duration::from_millis(250),
            metrics_interval: Duration::from_millis(200),
            renewal_lead,
            // Long enough that only the test that targets the deadline ever
            // reaches it; that test sets its own.
            query_deadline: Duration::from_mins(1),
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

/// Enroll with a one-time key (the pre-runtime step `spiced --token`
/// performs), then start the client and wait for the mTLS Hello — the
/// production sequence, condensed for test setup. Returns the running
/// client and the enrolled identity.
async fn enroll(
    harness: &Harness,
    config: &CloudConnectConfig,
    runtime: Arc<dyn RuntimeHandle>,
) -> (
    runtime_cloud_connect::CloudConnect,
    runtime_cloud_connect::identity::Identity,
) {
    enroll_with_key(harness, config, runtime, ENROLLMENT_KEY).await
}

/// As [`enroll`], with the enrollment key chosen by the test.
async fn enroll_with_key(
    harness: &Harness,
    config: &CloudConnectConfig,
    runtime: Arc<dyn RuntimeHandle>,
    key: &str,
) -> (
    runtime_cloud_connect::CloudConnect,
    runtime_cloud_connect::identity::Identity,
) {
    let outcome = enroll_now(config, &token_authority(key), test_retry())
        .await
        .expect("enrollment succeeds");
    let identity = match outcome {
        runtime_cloud_connect::EnrollNowOutcome::Enrolled { identity, .. }
        | runtime_cloud_connect::EnrollNowOutcome::AlreadyEnrolled { identity } => identity,
    };
    assert!(
        config.identity_path.exists(),
        "the enrolled identity must be durable before the client starts"
    );

    let handle = runtime_cloud_connect::CloudConnect::start(config.clone(), runtime)
        .await
        .expect("start")
        .expect("started");

    // Wait for the gateway to observe the mTLS Hello so the handshake is
    // fully settled before the test proceeds.
    let expected = identity.identifier.clone();
    let captured = Arc::clone(&harness.gateway.captured);
    let connected = wait_until_async(Duration::from_secs(10), || {
        let captured = Arc::clone(&captured);
        let expected = expected.clone();
        async move {
            captured
                .lock()
                .await
                .hellos
                .iter()
                .any(|(h, mtls)| h.identifier == expected && *mtls)
        }
    })
    .await;
    assert!(connected, "gateway must observe the mTLS Hello within 10s");

    (handle, identity)
}

// --------------------------------------------------------------------------
// Tests.
// --------------------------------------------------------------------------

#[tokio::test]
async fn enrollment_issues_identity_and_streams_over_mtls() {
    let harness = Harness::new(24 * 60 * 60).await;
    let dir = tempfile::tempdir().unwrap();
    let mut config = harness.config(
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Duration::from_hours(12),
    );
    config.instance_region = Some("us-west-2".to_string());

    let (runtime, _rt_state) = E2eRuntime::new();
    let (handle, identity) = enroll(&harness, &config, runtime).await;

    // The enroll request carried the canonical contract: kind + token +
    // CSR + encryption key + host facts under `instance`, under an
    // Idempotency-Key, with no login authorization and none of the deleted
    // enrollment-time project fields.
    let requests = harness.cloud.enroll_requests.lock().await.clone();
    assert_eq!(requests.len(), 1, "exactly one enroll request");
    let (body, headers) = &requests[0];
    assert_eq!(body["kind"], "standalone");
    assert_eq!(body["token"], ENROLLMENT_KEY);
    assert!(
        headers
            .idempotency_key
            .as_deref()
            .is_some_and(|k| !k.is_empty()),
        "every enrollment attempt carries its operation as Idempotency-Key"
    );
    assert!(
        headers.authorization.is_none(),
        "a token enrollment must not also carry login authorization"
    );
    assert!(
        body.get("app_name").is_none() && body.get("create_app").is_none(),
        "the deleted enrollment-time project fields must not exist on the wire"
    );
    assert!(
        body.get("expected_org").is_none(),
        "no expected_org was asserted, so none may be sent"
    );
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
    // own — enrollment moved out-of-band, and mTLS is the authN.
    let captured = Arc::clone(&harness.gateway.captured);
    let ok = with_captured!(captured, c => {
        c.hellos.iter().any(|(h, mtls)| {
            h.identifier == ASSIGNED_ID
                && *mtls
                && h.instance_kind == proto::InstanceKind::Standalone as i32
                && h.protocol_version == runtime_cloud_connect::PROTOCOL_VERSION
                && h.capabilities == vec!["apply_spicepod".to_string()]
        })
    });
    assert!(
        ok,
        "mTLS Hello must name the instance and announce its protocol version + capabilities"
    );

    handle.shutdown().await;
}

/// The pre-runtime bootstrap contract: `enroll_now` issues and persists the
/// identity with no client running (no gateway connection), promotes the
/// draft away, and returns the canonical response metadata; a later
/// `CloudConnect::start` with no key connects using the persisted identity
/// alone — enroll and run as two separate steps.
#[tokio::test]
async fn enrollment_precedes_the_client_and_reconnects_from_identity() {
    let harness = Harness::new(24 * 60 * 60).await;
    let dir = tempfile::tempdir().unwrap();
    let config = harness.config(
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Duration::from_hours(12),
    );

    // Phase 1: pre-runtime enrollment — no client task, no stream.
    let outcome = enroll_now(&config, &token_authority(ENROLLMENT_KEY), test_retry())
        .await
        .expect("enrollment succeeds");
    let runtime_cloud_connect::EnrollNowOutcome::Enrolled { identity, metadata } = outcome else {
        panic!("a fresh directory must enroll, not reuse");
    };
    assert_eq!(identity.identifier, ASSIGNED_ID);
    assert_eq!(metadata.organization.name, ORG_NAME);
    assert_eq!(
        metadata.new_project_url.as_deref(),
        Some("https://cloud.test/acme/new?instance=inst_e2e_1"),
        "the canonical response's portal link must reach the caller"
    );
    assert!(
        config.identity_path.exists(),
        "identity must be persisted by the pre-runtime enrollment"
    );
    assert!(
        !runtime_cloud_connect::EnrollmentDraft::path_in(dir.path()).exists(),
        "a successful enrollment promotes the draft away"
    );
    let captured_after_enroll = Arc::clone(&harness.gateway.captured);
    let hellos = with_captured!(captured_after_enroll, c => c.hellos.len());
    assert_eq!(hellos, 0, "enrollment must not connect to the gateway");

    // Phase 2: a separate start with NO key connects with the stored
    // identity — the identity alone is the activation signal.
    let run_config = harness.config(
        config.identity_path.clone(),
        dir.path().to_path_buf(),
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

/// A terminal cloud rejection stops immediately: one request, no identity,
/// and the retry-safe draft kept for a later attempt with a fresh key.
#[tokio::test]
async fn a_terminal_rejection_persists_no_identity_and_is_not_retried() {
    let harness = Harness::new(24 * 60 * 60).await;
    let dir = tempfile::tempdir().unwrap();
    let config = harness.config(
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Duration::from_hours(12),
    );

    // Canonically shaped but never minted: the mock answers 401 invalid_token.
    let unknown = "spice-enroll-neverminted0000000000000000000aa";
    let err = enroll_now(&config, &token_authority(unknown), test_retry())
        .await
        .expect_err("an unknown key is terminally rejected");
    assert!(
        matches!(err, runtime_cloud_connect::EnrollNowError::Rejected { .. }),
        "{err}"
    );
    assert!(err.is_terminal_rejection());
    assert_eq!(
        harness.cloud.enroll_requests.lock().await.len(),
        1,
        "a terminal rejection must not be retried"
    );
    assert!(!config.identity_path.exists(), "no identity may be issued");
    assert!(
        runtime_cloud_connect::EnrollmentDraft::path_in(dir.path()).exists(),
        "the draft survives so a fresh key can resume the same operation"
    );
    // The rejection is actionable and never echoes the key.
    let message = err.to_string();
    assert!(
        !message.contains(unknown),
        "the error must not echo the key: {message}"
    );
}

/// A 200 response is not enrollment success unless the issued leaf is usable
/// with the locally-generated private key. Preserve the operation draft so
/// support can diagnose/recover the committed response; never promote the bad
/// credential into `identity.json`.
#[tokio::test]
async fn a_mismatched_issued_certificate_is_rejected_before_persistence() {
    let harness = Harness::new(24 * 60 * 60).await;
    harness
        .cloud
        .issue_mismatched_enroll_certificate
        .store(true, Ordering::SeqCst);
    let dir = tempfile::tempdir().unwrap();
    let config = harness.config(
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Duration::from_hours(12),
    );

    let err = enroll_now(&config, &token_authority(ENROLLMENT_KEY), test_retry())
        .await
        .expect_err("a leaf for another private key must be rejected");

    assert!(
        matches!(err, runtime_cloud_connect::EnrollNowError::Rejected { .. }),
        "unexpected error: {err}"
    );
    let message = err.to_string();
    assert!(
        message.contains("certificate and private key do not match"),
        "unexpected validation error: {message}"
    );
    assert!(message.contains("contact Spice Cloud support"), "{message}");
    assert!(
        !config.identity_path.exists(),
        "an unusable issued credential must not become durable"
    );
    assert!(
        runtime_cloud_connect::EnrollmentDraft::path_in(dir.path()).exists(),
        "the committed operation draft must survive for recovery"
    );
    assert_eq!(
        *harness.cloud.instances_created.lock().await,
        1,
        "the response was committed server-side before validation"
    );
}

/// Response loss is the reason enrollment is operation-aware: the first
/// request lands (instance created, key consumed) but its response never
/// arrives. The retry re-presents the same operation and material, and the
/// cloud replays the recorded identity instead of creating a sibling.
#[tokio::test]
async fn response_loss_replay_does_not_create_a_second_instance() {
    let harness = Harness::new(24 * 60 * 60).await;
    let dir = tempfile::tempdir().unwrap();
    let config = harness.config(
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Duration::from_hours(12),
    );

    *harness.cloud.drop_responses.lock().await = 1;

    let outcome = enroll_now(&config, &token_authority(ENROLLMENT_KEY), test_retry())
        .await
        .expect("the retried enrollment succeeds");
    let runtime_cloud_connect::EnrollNowOutcome::Enrolled { identity, .. } = outcome else {
        panic!("a fresh directory must enroll");
    };

    let requests = harness.cloud.enroll_requests.lock().await.clone();
    assert!(
        requests.len() >= 2,
        "the lost response must have forced a retry"
    );
    let first_key = requests[0].1.idempotency_key.clone().expect("first op id");
    for (_, headers) in &requests {
        assert_eq!(
            headers.idempotency_key.as_deref(),
            Some(first_key.as_str()),
            "every retry must present the SAME operation"
        );
    }
    assert_eq!(
        *harness.cloud.instances_created.lock().await,
        1,
        "a replayed operation must not create a sibling instance"
    );
    assert_eq!(identity.identifier, ASSIGNED_ID);
    assert!(config.identity_path.exists());
}

/// A key that expires while its operation is stuck mid-retry is recoverable:
/// a NEW key presented with the SAME persisted draft/operation is consumed
/// against the existing instance rather than enrolling a sibling.
#[tokio::test]
async fn a_new_enrollment_key_recovers_the_pending_operation() {
    let harness = Harness::new(24 * 60 * 60).await;
    let dir = tempfile::tempdir().unwrap();
    let mut config = harness.config(
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Duration::from_hours(12),
    );
    config.instance_region = Some("us-west-2".to_string());

    // Phase 1: every processed attempt loses its response until the tight
    // retry budget expires — the operation and instance exist server-side,
    // the client has nothing.
    *harness.cloud.drop_responses.lock().await = 99;
    let err = enroll_now(
        &config,
        &token_authority(ENROLLMENT_KEY),
        RetryPolicy {
            // The first request is always made. The retry loop's non-zero
            // pacing floor then makes this budget deterministically expire
            // before a second attempt, leaving the committed operation for
            // the fresh key below to recover.
            deadline: Duration::from_millis(1),
        },
    )
    .await
    .expect_err("the budget expires with every response lost");
    assert!(
        matches!(
            err,
            runtime_cloud_connect::EnrollNowError::DeadlineExceeded { .. }
        ),
        "{err}"
    );
    assert!(!config.identity_path.exists());
    assert!(
        runtime_cloud_connect::EnrollmentDraft::path_in(dir.path()).exists(),
        "the draft must survive the failed run"
    );

    // The first key then expires before anyone retries.
    *harness.cloud.drop_responses.lock().await = 0;
    harness.cloud.expire_token(ENROLLMENT_KEY).await;

    // Model a replacement container and image upgrade. The pending draft owns
    // the canonical non-authority request, so a changed runtime version or
    // region cannot turn the same operation into an idempotency mismatch.
    config.runtime_version = "v9.9.9-replacement".to_string();
    config.instance_region = Some("eu-west-1".to_string());

    // Phase 2: a fresh key with the same directory (same draft, same
    // operation) recovers the SAME instance.
    let outcome = enroll_now(
        &config,
        &token_authority(SECOND_ENROLLMENT_KEY),
        test_retry(),
    )
    .await
    .expect("a new key recovers the pending operation");
    let runtime_cloud_connect::EnrollNowOutcome::Enrolled { identity, .. } = outcome else {
        panic!("phase 2 must enroll");
    };
    assert_eq!(
        identity.identifier, ASSIGNED_ID,
        "the recovered enrollment must return the operation's instance"
    );
    assert_eq!(
        *harness.cloud.instances_created.lock().await,
        1,
        "recovery must not create a sibling instance"
    );
    assert!(config.identity_path.exists());
    let requests = harness.cloud.enroll_requests.lock().await.clone();
    let first = &requests.first().expect("phase 1 request").0;
    let recovered = &requests.last().expect("phase 2 request").0;
    for field in ["csr_pem", "enc_pubkey_pem", "instance", "region"] {
        assert_eq!(
            first[field], recovered[field],
            "{field} must be replayed from the pending draft"
        );
    }
    assert_eq!(recovered["instance"]["runtime_version"], "v0.0.0-e2e");
    assert_eq!(recovered["region"], "us-west-2");
    assert!(
        !runtime_cloud_connect::EnrollmentDraft::path_in(dir.path()).exists(),
        "success promotes the draft away"
    );
}

/// An enrollment key redeems exactly once: a second instance directory
/// presenting the consumed key (a different operation) is refused with
/// `consumed_token`, terminally.
#[tokio::test]
async fn an_enrollment_key_is_single_use_across_instances() {
    let harness = Harness::new(24 * 60 * 60).await;

    // First directory redeems the key.
    let dir1 = tempfile::tempdir().unwrap();
    let config1 = harness.config(
        dir1.path().join("identity.json"),
        dir1.path().to_path_buf(),
        Duration::from_hours(12),
    );
    enroll_now(&config1, &token_authority(ENROLLMENT_KEY), test_retry())
        .await
        .expect("first redemption succeeds");

    // A second directory is a different operation, so the replay path does
    // not apply and the consumed key is refused terminally.
    let dir2 = tempfile::tempdir().unwrap();
    let config2 = harness.config(
        dir2.path().join("identity.json"),
        dir2.path().to_path_buf(),
        Duration::from_hours(12),
    );
    let requests_before = harness.cloud.enroll_requests.lock().await.len();
    let err = enroll_now(&config2, &token_authority(ENROLLMENT_KEY), test_retry())
        .await
        .expect_err("a consumed key must be refused");
    assert!(err.is_terminal_rejection(), "{err}");
    assert_eq!(
        harness.cloud.enroll_requests.lock().await.len(),
        requests_before + 1,
        "a consumed key must not be retried"
    );
    assert!(
        !config2.identity_path.exists(),
        "no identity may be issued for a consumed key"
    );
    assert_eq!(
        *harness.cloud.instances_created.lock().await,
        1,
        "the consumed key must not have created a second instance"
    );
}

/// `expected_org` is an assertion checked before the key is consumed: a
/// mismatch is terminal, and the untouched key still redeems afterwards
/// against the correct organization.
#[tokio::test]
async fn an_expected_org_mismatch_is_terminal_and_leaves_the_key_unconsumed() {
    let harness = Harness::new(24 * 60 * 60).await;
    let dir = tempfile::tempdir().unwrap();
    let config = harness.config(
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Duration::from_hours(12),
    );

    let mismatched = EnrollmentAuthority::Token {
        key: parse_key(ENROLLMENT_KEY),
        expected_org: Some("someone-else".to_string()),
    };
    let err = enroll_now(&config, &mismatched, test_retry())
        .await
        .expect_err("an org mismatch is refused");
    assert!(err.is_terminal_rejection(), "{err}");
    assert!(!config.identity_path.exists());

    // The key was NOT consumed: asserting the right organization succeeds.
    let matched = EnrollmentAuthority::Token {
        key: parse_key(ENROLLMENT_KEY),
        expected_org: Some(ORG_NAME.to_string()),
    };
    let outcome = enroll_now(&config, &matched, test_retry())
        .await
        .expect("the unconsumed key redeems with the correct org asserted");
    let runtime_cloud_connect::EnrollNowOutcome::Enrolled { metadata, .. } = outcome else {
        panic!("must enroll");
    };
    assert_eq!(metadata.organization.name, ORG_NAME);
    // The asserted org rode the request both times.
    let requests = harness.cloud.enroll_requests.lock().await.clone();
    assert_eq!(requests[0].0["expected_org"], "someone-else");
    assert_eq!(requests[1].0["expected_org"], ORG_NAME);
}

/// Logged-in enrollment: the session rides the headers (bearer +
/// `X-Org-Name`), the body carries no minted key, and the two authorities
/// are mutually exclusive — unrepresentable client-side, rejected
/// server-side.
#[tokio::test]
async fn authenticated_enrollment_carries_the_session_and_no_key() {
    let harness = Harness::new(24 * 60 * 60).await;
    let dir = tempfile::tempdir().unwrap();
    let config = harness.config(
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Duration::from_hours(12),
    );

    let session = EnrollmentAuthority::AuthenticatedSession {
        access_token: runtime_cloud_connect::SessionToken::new(SESSION_BEARER.to_string()),
        org: ORG_NAME.to_string(),
    };
    let outcome = enroll_now(&config, &session, test_retry())
        .await
        .expect("authenticated enrollment succeeds");
    let runtime_cloud_connect::EnrollNowOutcome::Enrolled { metadata, .. } = outcome else {
        panic!("must enroll");
    };
    assert_eq!(metadata.organization.name, ORG_NAME);

    let requests = harness.cloud.enroll_requests.lock().await.clone();
    assert_eq!(requests.len(), 1);
    let (body, headers) = &requests[0];
    assert_eq!(
        headers.authorization.as_deref(),
        Some(format!("Bearer {SESSION_BEARER}").as_str()),
        "the session must ride the Authorization header"
    );
    assert_eq!(headers.org_name.as_deref(), Some(ORG_NAME));
    assert!(
        body.get("token").is_none() && body.get("expected_org").is_none(),
        "no minted key may exist on the authenticated path"
    );

    // The server side of the exclusivity contract: a hand-crafted request
    // carrying BOTH a login authorization and a token is rejected before
    // anything is consumed. (The typed client cannot even represent it.)
    let both = reqwest::Client::new()
        .post(format!("http://{}/v1/cloud-connect/enroll", harness.cloud_addr))
        .header("Idempotency-Key", "op-carrying-both-authorities")
        .bearer_auth(SESSION_BEARER)
        .header("X-Org-Name", ORG_NAME)
        .json(&serde_json::json!({
            "kind": "standalone",
            "token": SECOND_ENROLLMENT_KEY,
            "csr_pem": "irrelevant",
            "enc_pubkey_pem": "irrelevant",
            "instance": {"fingerprint": "f", "hostname": "h", "os": "o", "arch": "a", "runtime_version": "v"},
        }))
        .send()
        .await
        .expect("the mock answers");
    assert_eq!(
        both.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "a request carrying both authorities must be rejected"
    );
    assert_eq!(
        *harness.cloud.instances_created.lock().await,
        1,
        "the rejected dual-authority request must not have enrolled"
    );
}

/// An existing valid identity always wins: the supplied key is not
/// redeemed, nothing about it is persisted, and no request is made — the
/// key stays usable elsewhere.
#[tokio::test]
async fn an_existing_identity_wins_without_redeeming_the_key() {
    let harness = Harness::new(24 * 60 * 60).await;
    let dir = tempfile::tempdir().unwrap();
    let config = harness.config(
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Duration::from_hours(12),
    );
    enroll_now(&config, &token_authority(ENROLLMENT_KEY), test_retry())
        .await
        .expect("first enrollment succeeds");
    let requests_after_first = harness.cloud.enroll_requests.lock().await.len();

    // Simulate cleanup failing after promotion. Reusing the identity must
    // scrub this provisional private material without contacting the cloud.
    runtime_cloud_connect::EnrollmentDraft::load_or_create(
        dir.path(),
        &runtime_cloud_connect::enroll::InstanceFacts::gather(&config.runtime_version),
        config.instance_region.as_deref(),
    )
    .expect("create a stale enrollment draft");
    assert!(
        runtime_cloud_connect::EnrollmentDraft::path_in(dir.path()).exists(),
        "test setup must leave a stale draft"
    );

    // A second bootstrap with a fresh, valid key: the identity wins.
    let outcome = enroll_now(
        &config,
        &token_authority(SECOND_ENROLLMENT_KEY),
        test_retry(),
    )
    .await
    .expect("the existing identity short-circuits");
    assert!(
        matches!(
            outcome,
            runtime_cloud_connect::EnrollNowOutcome::AlreadyEnrolled { ref identity }
                if identity.identifier == ASSIGNED_ID
        ),
        "the stored identity must be returned unredeemed"
    );
    assert_eq!(
        harness.cloud.enroll_requests.lock().await.len(),
        requests_after_first,
        "the supplied key must not be presented at all"
    );
    assert!(
        !runtime_cloud_connect::EnrollmentDraft::path_in(dir.path()).exists(),
        "identity reuse must scrub stale provisional key material"
    );

    // Not redeemed means still usable: the same key enrolls another
    // directory afterwards.
    let dir2 = tempfile::tempdir().unwrap();
    let config2 = harness.config(
        dir2.path().join("identity.json"),
        dir2.path().to_path_buf(),
        Duration::from_hours(12),
    );
    enroll_now(
        &config2,
        &token_authority(SECOND_ENROLLMENT_KEY),
        test_retry(),
    )
    .await
    .expect("the unredeemed key still enrolls a fresh directory");
}

/// Two processes sharing one config directory must serialize the complete
/// enrollment transaction. The contender cannot submit a second authority
/// while the owner is between its identity check and durable promotion.
#[tokio::test]
async fn concurrent_enrollment_is_serialized_through_identity_promotion() {
    let harness = Harness::new(24 * 60 * 60).await;
    let dir = tempfile::tempdir().expect("create shared enrollment directory");
    let config = harness.config(
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Duration::from_hours(12),
    );

    harness
        .cloud
        .pause_next_enroll
        .store(true, Ordering::SeqCst);
    let first_config = config.clone();
    let first = tokio::spawn(async move {
        enroll_now(
            &first_config,
            &token_authority(ENROLLMENT_KEY),
            test_retry(),
        )
        .await
    });
    tokio::time::timeout(
        Duration::from_secs(5),
        harness.cloud.enroll_paused.notified(),
    )
    .await
    .expect("the first request reaches the deterministic cloud gate");

    let second_config = config.clone();
    let mut second = tokio::spawn(async move {
        enroll_now(
            &second_config,
            &token_authority(SECOND_ENROLLMENT_KEY),
            test_retry(),
        )
        .await
    });

    assert!(
        tokio::time::timeout(Duration::from_millis(250), &mut second)
            .await
            .is_err(),
        "the contender must wait for the transaction owner"
    );
    assert_eq!(
        harness.cloud.enroll_requests.lock().await.len(),
        1,
        "the contender must not submit another authority before promotion"
    );

    harness.cloud.resume_enroll.notify_one();
    let first_outcome = tokio::time::timeout(Duration::from_secs(5), first)
        .await
        .expect("the transaction owner finishes")
        .expect("the transaction owner task joins")
        .expect("the transaction owner enrolls");
    let second_outcome = tokio::time::timeout(Duration::from_secs(5), second)
        .await
        .expect("the contender finishes after promotion")
        .expect("the contender task joins")
        .expect("the contender reuses the promoted identity");

    assert!(matches!(first_outcome, EnrollNowOutcome::Enrolled { .. }));
    assert!(matches!(
        second_outcome,
        EnrollNowOutcome::AlreadyEnrolled { .. }
    ));
    assert_eq!(harness.cloud.enroll_requests.lock().await.len(), 1);
    assert_eq!(*harness.cloud.instances_created.lock().await, 1);
    assert!(
        harness
            .cloud
            .tokens
            .lock()
            .await
            .get(SECOND_ENROLLMENT_KEY)
            .is_some_and(|token| token.consumed_by.is_none()),
        "the waiting contender's key must remain unredeemed"
    );
}

/// A readable but unusable identity fails closed. The caller receives the
/// exact removal recovery step, while the supplied key remains unredeemed.
#[tokio::test]
async fn an_unusable_existing_identity_refuses_to_redeem_the_key() {
    let harness = Harness::new(24 * 60 * 60).await;
    let dir = tempfile::tempdir().unwrap();
    let config = harness.config(
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Duration::from_hours(12),
    );
    enroll_now(&config, &token_authority(ENROLLMENT_KEY), test_retry())
        .await
        .expect("first enrollment succeeds");

    let mut identity = IdentityStore::load_optional(&config.identity_path)
        .expect("load identity")
        .expect("identity exists");
    let gateway_addr = identity.gateway_addr.clone();
    identity.gateway_addr.clear();
    IdentityStore::store(&config.identity_path, &identity).expect("store unusable identity");
    let requests_before_recovery = harness.cloud.enroll_requests.lock().await.len();

    let err = enroll_now(
        &config,
        &token_authority(SECOND_ENROLLMENT_KEY),
        test_retry(),
    )
    .await
    .expect_err("an unusable identity must fail closed");
    assert!(
        matches!(
            err,
            runtime_cloud_connect::EnrollNowError::IdentityUnusable { .. }
        ),
        "unexpected error: {err}"
    );
    let message = err.to_string();
    assert!(message.contains("remove this identity file"), "{message}");
    assert!(
        !message.contains(SECOND_ENROLLMENT_KEY),
        "recovery error leaked the supplied key: {message}"
    );
    assert_eq!(
        harness.cloud.enroll_requests.lock().await.len(),
        requests_before_recovery,
        "an unusable identity must not cause implicit re-enrollment"
    );

    identity.gateway_addr = gateway_addr;
    identity.private_key_pem = KeyPair::generate()
        .expect("generate a mismatched identity key")
        .serialize_pem();
    IdentityStore::store(&config.identity_path, &identity)
        .expect("store identity with mismatched credentials");

    let err = enroll_now(
        &config,
        &token_authority(SECOND_ENROLLMENT_KEY),
        test_retry(),
    )
    .await
    .expect_err("mismatched identity credentials must fail closed");
    assert!(
        err.to_string()
            .contains("certificate and private key do not match"),
        "unexpected mismatch error: {err}"
    );
    assert_eq!(
        harness.cloud.enroll_requests.lock().await.len(),
        requests_before_recovery,
        "mismatched identity credentials must not cause implicit re-enrollment"
    );
}

/// The enrollment key exists only in the one request that consumes it:
/// nothing under the config directory may contain it, and neither may the
/// typed authority's Debug or a rejection's message.
#[tokio::test]
async fn the_enrollment_key_never_reaches_disk_or_debug() {
    let harness = Harness::new(24 * 60 * 60).await;
    let dir = tempfile::tempdir().unwrap();
    let config = harness.config(
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Duration::from_hours(12),
    );

    let authority = token_authority(ENROLLMENT_KEY);
    assert!(
        !format!("{authority:?}").contains(ENROLLMENT_KEY),
        "the authority's Debug must redact the key"
    );

    enroll_now(&config, &authority, test_retry())
        .await
        .expect("enrollment succeeds");

    // Every file the enrollment left behind — identity.json and anything
    // else — must be free of the key.
    for entry in std::fs::read_dir(dir.path()).expect("read config dir") {
        let path = entry.expect("dir entry").path();
        if path.is_file() {
            let contents = std::fs::read_to_string(&path).unwrap_or_default();
            assert!(
                !contents.contains("spice-enroll-"),
                "{} must not contain an enrollment key",
                path.display()
            );
        }
    }

    // A denial's message must not echo the key either.
    let dir2 = tempfile::tempdir().unwrap();
    let config2 = harness.config(
        dir2.path().join("identity.json"),
        dir2.path().to_path_buf(),
        Duration::from_hours(12),
    );
    let err = enroll_now(&config2, &token_authority(ENROLLMENT_KEY), test_retry())
        .await
        .expect_err("the consumed key is refused");
    assert!(
        !err.to_string().contains(ENROLLMENT_KEY),
        "the rejection must not echo the key: {err}"
    );
}

/// A plain transient outage (503 before any processing) is retried within
/// the budget and succeeds without operator involvement.
#[tokio::test]
async fn a_transient_outage_is_retried_to_success() {
    let harness = Harness::new(24 * 60 * 60).await;
    let dir = tempfile::tempdir().unwrap();
    let config = harness.config(
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Duration::from_hours(12),
    );

    *harness.cloud.unavailable_responses.lock().await = 1;
    let outcome = enroll_now(&config, &token_authority(ENROLLMENT_KEY), test_retry())
        .await
        .expect("the retried enrollment succeeds");
    assert!(matches!(
        outcome,
        runtime_cloud_connect::EnrollNowOutcome::Enrolled { .. }
    ));
    assert!(
        harness.cloud.enroll_requests.lock().await.len() >= 2,
        "the outage must have been retried"
    );
    assert_eq!(*harness.cloud.instances_created.lock().await, 1);
}

/// The declared instance region rides the enroll request as a **sibling of
/// the probed host facts** and comes back on the registry row. Any
/// syntactically valid label enrolls — including one no region catalog
/// knows — because a standalone host may not be in a cloud region at all.
#[tokio::test]
async fn enrollment_records_the_declared_region() {
    let harness = Harness::new(24 * 60 * 60).await;
    let dir = tempfile::tempdir().unwrap();
    let mut config = harness.config(
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Duration::from_hours(12),
    );
    config.instance_region = Some("on-prem-syd".to_string());

    let outcome = enroll_now(&config, &token_authority(ENROLLMENT_KEY), test_retry())
        .await
        .expect("enrollment succeeds");
    let runtime_cloud_connect::EnrollNowOutcome::Enrolled { metadata, .. } = outcome else {
        panic!("must enroll");
    };
    assert_eq!(metadata.region.as_deref(), Some("on-prem-syd"));

    let requests = harness.cloud.enroll_requests.lock().await.clone();
    let body = &requests[0].0;
    assert_eq!(body["region"], "on-prem-syd");
    assert!(
        body["instance"].get("region").is_none(),
        "the declared region is a sibling of the probed facts, never one of them"
    );
}

/// Omitting the region must leave the stored region alone. Region-less
/// enrollment is the common case, and a request that unconditionally wrote
/// the column would erase a region set in the portal.
#[tokio::test]
async fn enrolling_without_a_region_leaves_the_stored_region_untouched() {
    let harness = Harness::new(24 * 60 * 60).await;

    // First enrollment declares a region, which the registry stores.
    let dir1 = tempfile::tempdir().unwrap();
    let mut config1 = harness.config(
        dir1.path().join("identity.json"),
        dir1.path().to_path_buf(),
        Duration::from_hours(12),
    );
    config1.instance_region = Some("us-west-2".to_string());
    enroll_now(&config1, &token_authority(ENROLLMENT_KEY), test_retry())
        .await
        .expect("first enrollment succeeds");

    // A second enrollment declares none: the wire must omit the field (not
    // null it), and the registry's stored value must survive.
    let dir2 = tempfile::tempdir().unwrap();
    let config2 = harness.config(
        dir2.path().join("identity.json"),
        dir2.path().to_path_buf(),
        Duration::from_hours(12),
    );
    let outcome = enroll_now(
        &config2,
        &token_authority(SECOND_ENROLLMENT_KEY),
        test_retry(),
    )
    .await
    .expect("second enrollment succeeds");
    let runtime_cloud_connect::EnrollNowOutcome::Enrolled { metadata, .. } = outcome else {
        panic!("must enroll");
    };
    let requests = harness.cloud.enroll_requests.lock().await.clone();
    assert!(
        requests[1].0.get("region").is_none(),
        "an undeclared region must be omitted from the wire entirely"
    );
    assert_eq!(
        metadata.region.as_deref(),
        Some("us-west-2"),
        "the stored region must be reported back untouched"
    );
}

/// A persistence failure lands *after* the cloud recorded the operation and
/// issued the identity — and because the operation is durable server-side,
/// fixing the directory and retrying recovers the SAME instance instead of
/// creating a duplicate (exactly what the error message promises).
#[cfg(unix)]
#[tokio::test]
async fn a_persistence_failure_is_terminal_and_recoverable_by_replay() {
    use std::os::unix::fs::PermissionsExt as _;

    let harness = Harness::new(24 * 60 * 60).await;
    let dir = tempfile::tempdir().unwrap();
    let mut config = harness.config(
        dir.path().join("identity.json"),
        dir.path().to_path_buf(),
        Duration::from_hours(12),
    );

    // The draft is written first (needs the dir writable); the identity
    // write then fails against a read-only directory placed at the identity
    // path's parent.
    let sealed = dir.path().join("sealed");
    std::fs::create_dir_all(&sealed).unwrap();
    std::fs::set_permissions(&sealed, std::fs::Permissions::from_mode(0o555)).unwrap();
    config.identity_path = sealed.join("identity.json");

    let err = enroll_now(&config, &token_authority(ENROLLMENT_KEY), test_retry())
        .await
        .expect_err("the identity cannot be persisted");
    assert!(
        matches!(err, runtime_cloud_connect::EnrollNowError::Persist { .. }),
        "{err}"
    );
    assert!(
        err.to_string().contains("pending operation"),
        "the failure must say a retry resumes the operation: {err}"
    );
    assert_eq!(*harness.cloud.instances_created.lock().await, 1);

    // Fix the directory; the first key is spent, so retry with a fresh one:
    // the same draft/operation replays the same instance.
    std::fs::set_permissions(&sealed, std::fs::Permissions::from_mode(0o755)).unwrap();
    let outcome = enroll_now(
        &config,
        &token_authority(SECOND_ENROLLMENT_KEY),
        test_retry(),
    )
    .await
    .expect("the retried enrollment recovers");
    let runtime_cloud_connect::EnrollNowOutcome::Enrolled { identity, .. } = outcome else {
        panic!("must enroll");
    };
    assert_eq!(identity.identifier, ASSIGNED_ID);
    assert_eq!(
        *harness.cloud.instances_created.lock().await,
        1,
        "recovery must not create a sibling instance"
    );
}

#[tokio::test]
async fn identity_is_reused_across_restart_over_mtls() {
    let harness = Harness::new(24 * 60 * 60).await;
    let dir = tempfile::tempdir().unwrap();
    let identity_path = dir.path().join("identity.json");

    // First boot: enroll with a one-time enrollment key.
    let enroll_cfg = harness.config(
        identity_path.clone(),
        dir.path().to_path_buf(),
        Duration::from_hours(12),
    );
    let (runtime, _s) = E2eRuntime::new();
    let (handle, _identity) = enroll(&harness, &enroll_cfg, runtime).await;
    handle.shutdown().await; // simulate process stop; identity.json persists.

    let captured = Arc::clone(&harness.gateway.captured);
    let hellos_before = with_captured!(captured, c => c.hellos.len());

    // Second boot: NO enrollment key — the client must load the persisted
    // identity and reconnect over mTLS, presenting its client certificate,
    // without touching the enroll endpoint again.
    let enrolls_before = harness.cloud.enroll_requests.lock().await.len();
    let reuse_cfg = harness.config(
        identity_path.clone(),
        dir.path().to_path_buf(),
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
            sealed_secret_payload: Some(proto::SealedSecretPayload {
                key_id: session.key_id.clone(),
                enc: outer_sealed.enc,
                ciphertext: outer_sealed.ciphertext,
            }),
            app_id: "4002".to_string(),
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
        Duration::from_hours(12),
    );
    let (runtime, rt_state) = E2eRuntime::new();
    let (handle, _identity) = enroll(&harness, &config, runtime).await;

    // Garbage addressed to a key this session never announced.
    harness.gateway.outbound.lock().await.push_back(ctrl_id(
        "cmd-bad-secrets",
        proto::control_message::Body::ApplySpicepod(proto::ApplySpicepod {
            spicepod_yaml: "version: v2\nkind: Spicepod\nname: nope\n".to_string(),
            sealed_secret_payload: Some(proto::SealedSecretPayload {
                key_id: "0000000000000000".to_string(),
                enc: vec![0_u8; 32],
                ciphertext: vec![0_u8; 64],
            }),
            app_id: String::new(),
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
            app_id: "4002".to_string(),
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

    // The runtime persisted the YAML to the canonical cloud-managed path, which
    // is what the restart comes back up on.
    let (path, written, app_id) = rt_state
        .lock()
        .await
        .applied_spicepod
        .clone()
        .expect("spicepod applied");
    assert_eq!(written, yaml);
    assert!(path.exists(), "spicepod file must be on disk");
    // The app id rides the deploy: it is the only way the runtime learns which
    // app to attribute its metrics to, and it exports none until it has one.
    assert_eq!(app_id.as_deref(), Some("4002"));

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
        Duration::from_hours(12),
    );
    enroll_now(&config, &token_authority(ENROLLMENT_KEY), test_retry())
        .await
        .expect("enrollment succeeds");
    let (runtime, _s) = E2eRuntime::new();

    let handle = runtime_cloud_connect::CloudConnect::start(config.clone(), runtime)
        .await
        .expect("start")
        .expect("started");

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
        Duration::from_secs(2),
    );
    let (runtime, _s) = E2eRuntime::new();

    // Enroll first, snapshotting the pre-rotation identity before the
    // client's renewal timer can fire and overwrite it.
    enroll_now(&config, &token_authority(ENROLLMENT_KEY), test_retry())
        .await
        .expect("enrollment succeeds");
    let enrolled_identity = IdentityStore::load_optional(&identity_path)
        .expect("load identity")
        .expect("identity present");
    assert_eq!(enrolled_identity.identifier, ASSIGNED_ID);
    let handle = runtime_cloud_connect::CloudConnect::start(config, runtime)
        .await
        .expect("start")
        .expect("started");

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
