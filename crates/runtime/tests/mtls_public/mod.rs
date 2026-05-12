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

//! End-to-end tests for the public-facing mTLS surface (HTTP + Flight).
//!
//! Exercises:
//!
//! - `client_auth: required` accepts certs signed by the configured CA
//!   on **both** the HTTP and Flight listeners.
//! - `client_auth: required` admits no-cert HTTPS connections at the
//!   transport (so Kubernetes liveness/readiness probes work) but the
//!   HTTP route layer still 401s non-probe routes that arrived
//!   without a verified client cert.
//! - The metrics port (`/health`, `/metrics`) is always reachable over
//!   TLS without a client cert when `client_auth: required` is set.
//! - The Flight listener stays strict: no-cert and foreign-CA cert
//!   handshakes are rejected at the TLS layer.
//! - mTLS-as-identity mode ([`IdentitySource::Channel`]): a verified
//!   peer cert is sufficient — no API key is required even when the
//!   underlying servers have no `runtime.auth` configured.
//!
//! mTLS-as-channel mode (cert + API key) is covered indirectly by the
//! existing API-key auth tests plus the unit tests on the per-protocol
//! mTLS layers in `crates/runtime/src/{http,flight}/mtls.rs`.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use crate::{init_tracing, utils::test_request_context};
use arrow_flight::{
    FlightDescriptor,
    flight_service_client::FlightServiceClient,
    sql::{CommandStatementQuery, ProstMessageExt},
};
use prost::Message;
use rand::RngExt;
use rcgen::{
    CertificateParams, DistinguishedName, DnType, IsCa, Issuer, KeyPair, KeyUsagePurpose, SanType,
};
use runtime::{Runtime, auth::EndpointAuth, config::Config, tls::TlsConfig};
use runtime_auth::IdentitySource;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

fn install_crypto_provider() {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
}

/// PKI bundle generated for one test: a CA, a server leaf signed by
/// the CA, and a client leaf signed by the same CA.
#[expect(
    clippy::struct_field_names,
    reason = "`_pem` postfix matches the rest of the test module's PEM-shaped vars"
)]
struct TestPki {
    ca_pem: String,
    server_cert_pem: String,
    server_key_pem: String,
    client_cert_pem: String,
    client_key_pem: String,
}

fn generate_pki(tag: &str) -> TestPki {
    // CA
    let mut ca_dn = DistinguishedName::new();
    ca_dn.push(DnType::CommonName, format!("Spice mTLS test CA {tag}"));
    let mut ca_params = CertificateParams::default();
    ca_params.distinguished_name = ca_dn;
    ca_params.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let ca_key = KeyPair::generate().expect("ca keypair");
    let ca = ca_params.self_signed(&ca_key).expect("self-signed CA");
    let ca_issuer = Issuer::new(ca_params, ca_key);

    // Server leaf
    let mut srv_dn = DistinguishedName::new();
    srv_dn.push(DnType::CommonName, format!("spiced-server-{tag}"));
    let mut srv_params = CertificateParams::default();
    srv_params.distinguished_name = srv_dn;
    srv_params
        .subject_alt_names
        .push(SanType::DnsName("localhost".try_into().expect("dns")));
    srv_params
        .subject_alt_names
        .push(SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST)));
    let srv_key = KeyPair::generate().expect("srv keypair");
    let srv = srv_params
        .signed_by(&srv_key, &ca_issuer)
        .expect("server signed by CA");

    // Client leaf
    let mut cli_dn = DistinguishedName::new();
    cli_dn.push(DnType::CommonName, format!("spiced-client-{tag}"));
    let mut cli_params = CertificateParams::default();
    cli_params.distinguished_name = cli_dn;
    let cli_key = KeyPair::generate().expect("cli keypair");
    let cli = cli_params
        .signed_by(&cli_key, &ca_issuer)
        .expect("client signed by CA");

    TestPki {
        ca_pem: ca.pem(),
        server_cert_pem: srv.pem(),
        server_key_pem: srv_key.serialize_pem(),
        client_cert_pem: cli.pem(),
        client_key_pem: cli_key.serialize_pem(),
    }
}

fn random_ports() -> (u16, u16, u16) {
    let mut rng = rand::rng();
    let http: u16 = rng.random_range(50000..60000);
    (http, http + 1, http + 2)
}

/// Spin up a `Runtime` listening on the given ports with TLS +
/// the given [`ClientAuthEnforcement`] configured against `pki`.
async fn start_runtime_with_mtls(
    http_port: u16,
    flight_port: u16,
    metrics_port: u16,
    pki: &TestPki,
    identity_source: IdentitySource,
    enforcement: runtime::tls::ClientAuthEnforcement,
) {
    let api_config = Config::new()
        .with_http_bind_address(SocketAddr::new(LOCALHOST, http_port))
        .with_flight_bind_address(SocketAddr::new(LOCALHOST, flight_port));

    let tls_config = TlsConfig::try_new_with_client_auth_mode(
        pki.server_cert_pem.as_bytes(),
        pki.server_key_pem.as_bytes(),
        Some(pki.ca_pem.as_bytes()),
        enforcement,
    )
    .expect("valid TlsConfig with client auth");

    let registry = prometheus::Registry::new();
    let app = app::AppBuilder::new("test_app").build();

    let rt = Arc::new(
        Runtime::builder()
            .with_metrics_server(SocketAddr::new(LOCALHOST, metrics_port), registry)
            .with_app(app)
            .build()
            .await,
    );

    let endpoint_auth = EndpointAuth::no_auth().with_identity_source(identity_source);
    tokio::spawn(async move {
        Box::pin(Arc::clone(&rt).start_servers(
            api_config,
            Some(Arc::new(tls_config)),
            endpoint_auth,
        ))
        .await
    });
}

/// Build a reqwest HTTPS client that trusts `ca_pem` and (optionally)
/// presents a client cert/key. Concatenates cert + key into the single
/// blob `reqwest::Identity::from_pem` expects.
fn build_http_client(ca_pem: &str, client_pem_pair: Option<(&str, &str)>) -> reqwest::Client {
    let ca = reqwest::tls::Certificate::from_pem(ca_pem.as_bytes()).expect("valid CA");
    let mut builder = reqwest::Client::builder()
        .use_rustls_tls()
        .tls_built_in_root_certs(false)
        .add_root_certificate(ca);
    if let Some((cert_pem, key_pem)) = client_pem_pair {
        let mut buf = Vec::with_capacity(cert_pem.len() + key_pem.len() + 1);
        buf.extend_from_slice(cert_pem.as_bytes());
        buf.push(b'\n');
        buf.extend_from_slice(key_pem.as_bytes());
        let id = reqwest::Identity::from_pem(&buf).expect("valid client identity");
        builder = builder.identity(id);
    }
    builder.build().expect("reqwest client")
}

async fn build_flight_client(
    ca_pem: &str,
    client_pem_pair: Option<(&str, &str)>,
    flight_port: u16,
) -> Result<FlightServiceClient<Channel>, tonic::transport::Error> {
    let mut tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca_pem.as_bytes()))
        .domain_name("localhost");
    if let Some((cert_pem, key_pem)) = client_pem_pair {
        tls = tls.identity(Identity::from_pem(cert_pem.as_bytes(), key_pem.as_bytes()));
    }
    let channel = Channel::from_shared(format!("https://localhost:{flight_port}"))
        .expect("valid uri")
        .tls_config(tls)?
        .connect()
        .await?;
    Ok(FlightServiceClient::new(channel))
}

async fn flight_show_tables(
    client: &mut FlightServiceClient<Channel>,
) -> Result<(), tonic::Status> {
    let cmd = CommandStatementQuery {
        query: "show tables".to_string(),
        transaction_id: None,
    };
    let req = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
    let _ = client.get_flight_info(req).await?;
    Ok(())
}

/// Wait for `f` to return `Ok(())`, retrying for up to `timeout`. Useful
/// while the spawned `Runtime` boots its listeners.
async fn wait_for_ok<F, Fut, E>(timeout: Duration, mut f: F) -> Result<(), E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), E>>,
    E: std::fmt::Debug,
{
    let deadline = std::time::Instant::now() + timeout;
    let mut last_err: Option<E> = None;
    while std::time::Instant::now() < deadline {
        match f().await {
            Ok(()) => return Ok(()),
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    Err(last_err.expect("at least one attempt was made"))
}

/// `client_auth: required` + `IdentitySource::Channel` (mTLS-as-identity):
///
/// - Plain HTTPS connection with no client cert → handshake fails.
/// - Client cert signed by the trusted CA → HTTP `/health` returns 200
///   AND a Flight `get_flight_info` succeeds, with no API key on the
///   wire (the per-protocol mTLS layers must promote the cert to the
///   request principal so the auth-layer short-circuit fires).
/// - Client cert signed by an unrelated CA → handshake fails.
#[tokio::test]
async fn test_public_mtls_required_channel_mode() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    install_crypto_provider();

    test_request_context()
        .scope(async {
            let span = tracing::info_span!("test_public_mtls_required_channel_mode");
            let _span_guard = span.enter();

            let pki = generate_pki("primary");
            let foreign = generate_pki("foreign");
            let (http_port, flight_port, metrics_port) = random_ports();
            tracing::debug!(
                "mTLS ports: http={http_port}, flight={flight_port}, metrics={metrics_port}"
            );

            start_runtime_with_mtls(
                http_port,
                flight_port,
                metrics_port,
                &pki,
                IdentitySource::Channel,
                runtime::tls::ClientAuthEnforcement::Required,
            )
            .await;

            // 1. Wait for the HTTP listener to be up by repeatedly
            //    trying with a *valid* client cert.
            let valid_http = build_http_client(
                &pki.ca_pem,
                Some((&pki.client_cert_pem, &pki.client_key_pem)),
            );
            let url = format!("https://localhost:{http_port}/health");
            wait_for_ok::<_, _, reqwest::Error>(Duration::from_secs(15), || async {
                valid_http.get(&url).send().await.map(|_| ())
            })
            .await
            .expect("HTTP listener never came up under valid client cert");
            let resp = valid_http
                .get(&url)
                .send()
                .await
                .expect("HTTP /health under valid cert");
            assert!(
                resp.status().is_success(),
                "valid client cert should reach HTTP /health, got {}",
                resp.status()
            );
            tracing::info!("HTTP /health OK with valid client cert");

            // 2. Same client, no client cert → /health bypasses the
            //    route gate so probes succeed; /v1/sql is on the
            //    authenticated router and 401s.
            let no_cert_http = build_http_client(&pki.ca_pem, None);
            let resp = no_cert_http
                .get(&url)
                .send()
                .await
                .expect("no-cert /health must complete the TLS handshake");
            assert!(
                resp.status().is_success(),
                "no-cert /health must succeed under client_auth: required, got {}",
                resp.status()
            );
            tracing::info!("no-cert HTTPS /health bypassed gate as expected");

            let ready_resp = no_cert_http
                .get(format!("https://localhost:{http_port}/v1/ready"))
                .send()
                .await
                .expect("no-cert /v1/ready must complete the TLS handshake");
            assert!(
                ready_resp.status().is_success() || ready_resp.status().as_u16() == 503,
                "no-cert /v1/ready must reach the route handler (200 or 503), got {}",
                ready_resp.status()
            );

            let sql_resp = no_cert_http
                .post(format!("https://localhost:{http_port}/v1/sql"))
                .body("SELECT 1")
                .send()
                .await
                .expect("no-cert /v1/sql must complete the TLS handshake");
            assert_eq!(
                sql_resp.status().as_u16(),
                401,
                "no-cert /v1/sql must be 401 under client_auth: required, got {}",
                sql_resp.status()
            );
            tracing::info!("no-cert HTTPS /v1/sql correctly 401d by route gate");

            // 3. Client cert signed by a *different* CA → handshake
            //    fails on every route (the lax verifier still rejects
            //    presented-but-untrusted certs at TLS).
            let foreign_http = build_http_client(
                &pki.ca_pem,
                Some((&foreign.client_cert_pem, &foreign.client_key_pem)),
            );
            let err = foreign_http
                .get(&url)
                .send()
                .await
                .expect_err("foreign-CA cert HTTPS request must fail");
            tracing::info!("foreign-CA HTTPS correctly rejected: {err}");

            // 4. Flight: valid client cert succeeds.
            let mut flight_client = build_flight_client(
                &pki.ca_pem,
                Some((&pki.client_cert_pem, &pki.client_key_pem)),
                flight_port,
            )
            .await
            .expect("flight connect with valid cert");
            flight_show_tables(&mut flight_client)
                .await
                .expect("flight show tables under valid cert");
            tracing::info!("Flight get_flight_info OK with valid client cert");

            // 5. Flight: no client cert. tonic's `Channel::connect()`
            //    may complete the TCP connect before the TLS
            //    handshake actually rejects (the server-side rustls
            //    `WebPkiClientVerifier` runs during handshake, but
            //    detection at the client may surface only on the
            //    first RPC). Either an error from `connect` OR an
            //    error from the first `get_flight_info` is acceptable.
            let no_cert_outcome: Result<(), tonic::Status> =
                match build_flight_client(&pki.ca_pem, None, flight_port).await {
                    Err(_) => Err(tonic::Status::unavailable("connect failed (no cert)")),
                    Ok(mut c) => flight_show_tables(&mut c).await,
                };
            let err = no_cert_outcome
                .expect_err("flight RPC without client cert must fail at connect or first call");
            tracing::info!("Flight no-cert correctly rejected: {err}");

            // 6. Flight: foreign-CA cert. Same caveat as #5.
            let foreign_outcome: Result<(), tonic::Status> = match build_flight_client(
                &pki.ca_pem,
                Some((&foreign.client_cert_pem, &foreign.client_key_pem)),
                flight_port,
            )
            .await
            {
                Err(_) => Err(tonic::Status::unavailable("connect failed (foreign cert)")),
                Ok(mut c) => flight_show_tables(&mut c).await,
            };
            let err = foreign_outcome
                .expect_err("flight RPC with foreign-CA cert must fail at connect or first call");
            tracing::info!("Flight foreign-CA correctly rejected: {err}");

            Ok::<_, anyhow::Error>(())
        })
        .await
}

/// `client_auth: required` + `IdentitySource::Anonymous` (the
/// out-of-the-box default with no `runtime.auth` and no per-protocol
/// promotion): a verified peer cert still passes the channel
/// requirement, the request just runs as the anonymous principal.
/// Verifies the validation surface is wired and a request can complete
/// when only `client_auth` is required, regardless of the `IdentitySource`.
#[tokio::test]
async fn test_public_mtls_required_anonymous_mode() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    install_crypto_provider();

    test_request_context()
        .scope(async {
            let span = tracing::info_span!("test_public_mtls_required_anonymous_mode");
            let _span_guard = span.enter();

            let pki = generate_pki("anon");
            let (http_port, flight_port, metrics_port) = random_ports();

            start_runtime_with_mtls(
                http_port,
                flight_port,
                metrics_port,
                &pki,
                IdentitySource::Anonymous,
                runtime::tls::ClientAuthEnforcement::Required,
            )
            .await;

            let http = build_http_client(
                &pki.ca_pem,
                Some((&pki.client_cert_pem, &pki.client_key_pem)),
            );
            let url = format!("https://localhost:{http_port}/health");
            wait_for_ok::<_, _, reqwest::Error>(Duration::from_secs(15), || async {
                http.get(&url).send().await.map(|_| ())
            })
            .await
            .expect("HTTP listener never came up");
            let resp = http.get(&url).send().await.expect("HTTP /health");
            assert!(
                resp.status().is_success(),
                "valid client cert in anonymous mode should reach /health, got {}",
                resp.status()
            );

            // No-cert /health is now allowed (probes); /v1/sql is
            // still 401d by the route gate even in anonymous mode.
            let no_cert = build_http_client(&pki.ca_pem, None);
            let resp = no_cert
                .get(&url)
                .send()
                .await
                .expect("no-cert /health must complete the handshake");
            assert!(
                resp.status().is_success(),
                "no-cert /health in anonymous mode must succeed, got {}",
                resp.status()
            );
            let sql_resp = no_cert
                .post(format!("https://localhost:{http_port}/v1/sql"))
                .body("SELECT 1")
                .send()
                .await
                .expect("no-cert /v1/sql must complete the handshake");
            assert_eq!(
                sql_resp.status().as_u16(),
                401,
                "no-cert /v1/sql in anonymous mode must be 401, got {}",
                sql_resp.status()
            );
            tracing::info!("anonymous-mode no-cert: /health 200, /v1/sql 401");

            // The metrics port reuses the lax HTTP `ServerConfig` and
            // has no application-layer client-auth gate, so /health
            // and /metrics are reachable over TLS with no client cert.
            let metrics_health = no_cert
                .get(format!("https://localhost:{metrics_port}/health"))
                .send()
                .await
                .expect("no-cert metrics /health");
            assert!(
                metrics_health.status().is_success(),
                "no-cert metrics /health must succeed, got {}",
                metrics_health.status()
            );
            let metrics_body = no_cert
                .get(format!("https://localhost:{metrics_port}/metrics"))
                .send()
                .await
                .expect("no-cert metrics /metrics");
            assert!(
                metrics_body.status().is_success(),
                "no-cert metrics /metrics must succeed, got {}",
                metrics_body.status()
            );
            tracing::info!("metrics endpoints reachable over TLS without client cert");

            Ok::<_, anyhow::Error>(())
        })
        .await
}

/// `client_auth_mode: request` (optional mTLS): both HTTP and Flight
/// listeners send `CertificateRequest` but accept no-cert
/// handshakes. Presented certs must be signed by the configured CA.
///
/// - No-cert HTTPS `/v1/sql` succeeds (no app-layer gate). Probes
///   continue to work.
/// - Valid client cert reaches Flight `get_flight_info` (no app-layer
///   gate either; the cert is promoted to the auth principal under
///   `IdentitySource::Channel`).
/// - A foreign-CA cert is rejected at the rustls handshake \u2014 the
///   `allow_unauthenticated` verifier still validates *presented*
///   certs against the trust roots.
#[tokio::test]
async fn test_public_mtls_request_mode() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    install_crypto_provider();

    test_request_context()
        .scope(async {
            let span = tracing::info_span!("test_public_mtls_request_mode");
            let _span_guard = span.enter();

            let pki = generate_pki("request");
            let foreign = generate_pki("foreign-req");
            let (http_port, flight_port, metrics_port) = random_ports();

            start_runtime_with_mtls(
                http_port,
                flight_port,
                metrics_port,
                &pki,
                IdentitySource::Channel,
                runtime::tls::ClientAuthEnforcement::Requested,
            )
            .await;

            // 1. Wait for the HTTP listener to come up under a valid
            //    client cert.
            let valid_http = build_http_client(
                &pki.ca_pem,
                Some((&pki.client_cert_pem, &pki.client_key_pem)),
            );
            let health_url = format!("https://localhost:{http_port}/health");
            wait_for_ok::<_, _, reqwest::Error>(Duration::from_secs(15), || async {
                valid_http.get(&health_url).send().await.map(|_| ())
            })
            .await
            .expect("HTTP listener never came up under request mode");

            // 2. No-cert HTTPS reaches /v1/sql without a 401 (no gate
            //    under Requested). We don't care about the *body*; we
            //    care that the response was not 401 from the route
            //    middleware. Returns 400 (bad SQL) or similar from
            //    the handler is fine.
            let no_cert_http = build_http_client(&pki.ca_pem, None);
            let sql_resp = no_cert_http
                .post(format!("https://localhost:{http_port}/v1/sql"))
                .body("SELECT 1")
                .send()
                .await
                .expect("no-cert /v1/sql must complete the handshake under request mode");
            assert_ne!(
                sql_resp.status().as_u16(),
                401,
                "request-mode no-cert /v1/sql must NOT be 401, got {}",
                sql_resp.status()
            );

            // 3. Foreign-CA cert is still rejected at the handshake \u2014
            //    presented certs are verified.
            let foreign_http = build_http_client(
                &pki.ca_pem,
                Some((&foreign.client_cert_pem, &foreign.client_key_pem)),
            );
            let err = foreign_http
                .get(&health_url)
                .send()
                .await
                .expect_err("foreign-CA cert must be rejected at the handshake");
            tracing::info!("request-mode foreign-CA rejected: {err}");

            // 4. Flight: no-cert handshake succeeds under Requested
            //    (lax verifier on Flight too).
            let mut no_cert_flight = build_flight_client(&pki.ca_pem, None, flight_port)
                .await
                .expect("flight connect with no cert under request mode");
            flight_show_tables(&mut no_cert_flight)
                .await
                .expect("flight show tables with no cert under request mode");

            // 5. Flight: valid client cert succeeds.
            let mut valid_flight = build_flight_client(
                &pki.ca_pem,
                Some((&pki.client_cert_pem, &pki.client_key_pem)),
                flight_port,
            )
            .await
            .expect("flight connect with valid cert under request mode");
            flight_show_tables(&mut valid_flight)
                .await
                .expect("flight show tables with valid cert under request mode");

            let _ = metrics_port; // metrics port behavior is covered by anonymous-mode test
            Ok::<_, anyhow::Error>(())
        })
        .await
}
