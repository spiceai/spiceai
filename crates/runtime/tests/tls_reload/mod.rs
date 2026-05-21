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

//! End-to-end tests for TLS certificate hot-reload.
//!
//! Each test:
//! 1. Generates a fresh CA + server cert/key into a tempdir.
//! 2. Starts a real `Runtime` with the public HTTP / Flight / Metrics
//!    endpoints bound to localhost over TLS, using the on-disk paths
//!    (`TlsConfig::try_new_from_paths`) so the watcher is wired in.
//! 3. Verifies the client sees the original leaf fingerprint.
//! 4. Atomically rotates the cert + key on disk to a new identity signed
//!    by the same CA.
//! 5. Triggers + waits for the reload, then verifies a fresh handshake
//!    sees the new leaf fingerprint.
//!
//! These tests exercise the *full* reload path including `notify`-based
//! filesystem watching, debouncing, atomic-rename safety, and the swap of
//! the rustls `ResolvesServerCert` resolver. They do not stub anything.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
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
use runtime::{
    Runtime,
    auth::EndpointAuth,
    config::Config,
    tls::{ReloadScope, TlsConfig, reload::reload_count_for_tests},
};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tonic::transport::Channel;

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// Output of [`generate_ca_and_leaf`].
struct GeneratedPki {
    ca_pem: String,
    leaf_cert_pem: String,
    leaf_key_pem: String,
    /// SHA-256 of the leaf cert DER, hex-encoded — what we assert on after
    /// rotation.
    leaf_fingerprint_hex: String,
}

fn install_crypto_provider() {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
}

/// Generate a self-signed CA and a leaf cert + key for `localhost` signed
/// by it. `cn` differentiates leaves between rotations so we can assert
/// the swap actually happened.
fn generate_ca_and_leaf(cn: &str) -> GeneratedPki {
    let mut ca_dn = DistinguishedName::new();
    ca_dn.push(DnType::CommonName, "Spice mTLS reload test CA");
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

    let mut leaf_dn = DistinguishedName::new();
    leaf_dn.push(DnType::CommonName, cn);
    let mut leaf_params = CertificateParams::default();
    leaf_params.distinguished_name = leaf_dn;
    leaf_params
        .subject_alt_names
        .push(SanType::DnsName("localhost".try_into().expect("dns")));
    leaf_params
        .subject_alt_names
        .push(SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST)));
    let leaf_key = KeyPair::generate().expect("leaf keypair");
    let leaf = leaf_params
        .signed_by(&leaf_key, &ca_issuer)
        .expect("leaf signed by CA");

    let leaf_cert_pem = leaf.pem();
    let leaf_der = leaf.der();
    let leaf_fingerprint_hex = hex(&Sha256::digest(leaf_der.as_ref()));

    GeneratedPki {
        ca_pem: ca.pem(),
        leaf_cert_pem,
        leaf_key_pem: leaf_key.serialize_pem(),
        leaf_fingerprint_hex,
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Atomically write `contents` to `dst` via temp file + rename. Mirrors how
/// SPIRE / cert-manager / kubelet rotate cert files on disk.
fn atomic_write(dst: &std::path::Path, contents: &str) {
    let tmp = dst.with_extension("tmp.write");
    std::fs::write(&tmp, contents).expect("write tmp");
    std::fs::rename(&tmp, dst).expect("atomic rename");
}

/// Read the leaf-cert SHA-256 the server presents on a fresh TLS handshake
/// to `host:port`. Uses a one-shot rustls connector so each call always
/// performs a new handshake (no connection pooling / session resumption).
async fn fetch_server_leaf_fingerprint(
    host: &str,
    port: u16,
    ca_pem: &str,
) -> anyhow::Result<String> {
    use rustls::client::danger::{ServerCertVerified, ServerCertVerifier};
    use rustls::crypto::aws_lc_rs::default_provider;
    use rustls::pki_types::{CertificateDer, ServerName};
    use std::sync::Mutex;

    /// Verifier that records the leaf cert it sees and accepts everything.
    /// We can't use the normal `WebPkiVerifier` because we want the raw DER,
    /// not just a Verified marker; and rcgen-issued chains chain back to a
    /// CA we control anyway, so trust verification is not what's under test.
    #[derive(Debug)]
    struct CapturingVerifier {
        captured: Arc<Mutex<Option<CertificateDer<'static>>>>,
    }
    impl ServerCertVerifier for CapturingVerifier {
        fn verify_server_cert(
            &self,
            end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp: &[u8],
            _now: rustls::pki_types::UnixTime,
        ) -> Result<ServerCertVerified, rustls::Error> {
            *self.captured.lock().expect("poisoned") = Some(end_entity.clone().into_owned());
            Ok(ServerCertVerified::assertion())
        }
        fn verify_tls12_signature(
            &self,
            _: &[u8],
            _: &CertificateDer<'_>,
            _: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }
        fn verify_tls13_signature(
            &self,
            _: &[u8],
            _: &CertificateDer<'_>,
            _: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }
        fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
            default_provider()
                .signature_verification_algorithms
                .supported_schemes()
        }
    }

    // ca_pem accepted purely so callers can document trust intent; the
    // capturing verifier accepts all chains.
    let _ = ca_pem;

    let captured = Arc::new(Mutex::new(None));
    let config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(CapturingVerifier {
            captured: Arc::clone(&captured),
        }))
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));

    let stream = tokio::net::TcpStream::connect((host, port)).await?;
    let server_name = ServerName::try_from(host.to_string())?;
    let _tls = connector.connect(server_name, stream).await?;

    let leaf = captured
        .lock()
        .expect("poisoned")
        .clone()
        .ok_or_else(|| anyhow::anyhow!("verifier was never invoked"))?;
    Ok(hex(&Sha256::digest(leaf.as_ref())))
}

async fn wait_for<F, Fut>(timeout: Duration, mut f: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if f().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

/// End-to-end: rotate the on-disk cert + key, verify a new handshake on
/// HTTP, Metrics, **and** Flight all see the new leaf without restarting
/// the runtime.
#[tokio::test]
async fn test_tls_hot_reload_all_endpoints() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    install_crypto_provider();

    test_request_context()
        .scope(async {
            let span = tracing::info_span!("test_tls_hot_reload_all_endpoints");
            let _span_guard = span.enter();

            // 1. Generate v1 PKI and write it to a tempdir.
            let pki_v1 = generate_ca_and_leaf("spiced-v1");
            let dir = TempDir::new()?;
            let cert_path = dir.path().join("server.crt");
            let key_path = dir.path().join("server.key");
            std::fs::write(&cert_path, &pki_v1.leaf_cert_pem)?;
            std::fs::write(&key_path, &pki_v1.leaf_key_pem)?;

            // 2. Build TlsConfig from those paths so the watcher is wired in.
            let control = runtime::tls::TlsControl::new()?;
            let tls_config = Arc::new(
                TlsConfig::try_new_from_paths(cert_path.clone(), key_path.clone(), &control)
                    .map_err(|e| anyhow::anyhow!("build TlsConfig: {e}"))?,
            );

            let mut rng = rand::rng();
            let http_port: u16 = rng.random_range(50000..60000);
            let flight_port: u16 = http_port + 1;
            let metrics_port: u16 = http_port + 2;
            tracing::info!(
                "TLS hot-reload ports: http={http_port} flight={flight_port} metrics={metrics_port}"
            );

            let api_config = Config::new()
                .with_http_bind_address(SocketAddr::new(LOCALHOST, http_port))
                .with_flight_bind_address(SocketAddr::new(LOCALHOST, flight_port));

            let registry = prometheus::Registry::new();
            let app = app::AppBuilder::new("test_app").build();
            let rt = Arc::new(
                Runtime::builder()
                    .with_metrics_server(SocketAddr::new(LOCALHOST, metrics_port), registry)
                    .with_app(app)
                    .build()
                    .await,
            );
            let rt_for_servers = Arc::clone(&rt);
            let tls_for_servers = Arc::clone(&tls_config);
            tokio::spawn(async move {
                Box::pin(rt_for_servers.start_servers(
                    api_config,
                    Some(tls_for_servers),
                    EndpointAuth::no_auth(),
                ))
                .await
            });

            // 3. Wait for the servers to come up by handshaking against HTTP.
            tracing::info!("waiting for servers...");
            assert!(
                wait_for(Duration::from_secs(10), || async {
                    fetch_server_leaf_fingerprint("127.0.0.1", http_port, &pki_v1.ca_pem)
                        .await
                        .is_ok()
                })
                .await,
                "HTTP server did not become reachable"
            );

            // 4. Verify each endpoint serves the v1 leaf.
            for (name, port) in [
                ("http", http_port),
                ("metrics", metrics_port),
                ("flight", flight_port),
            ] {
                let fp =
                    fetch_server_leaf_fingerprint("127.0.0.1", port, &pki_v1.ca_pem).await?;
                assert_eq!(
                    fp, pki_v1.leaf_fingerprint_hex,
                    "{name}: pre-rotation fingerprint mismatch"
                );
                tracing::info!("{name}: pre-rotation fingerprint = {fp}");
            }

            // 5. Smoke-test Flight gRPC (separate from raw handshake) to make
            //    sure the swapped tonic path actually serves traffic over TLS.
            run_flight_smoke_query(flight_port, &pki_v1.ca_pem).await?;

            // 6. Rotate to v2 atomically.
            let pki_v2 = generate_ca_and_leaf("spiced-v2");
            assert_ne!(
                pki_v1.leaf_fingerprint_hex, pki_v2.leaf_fingerprint_hex,
                "v1 and v2 leaves must differ"
            );
            let reload_count_before = reload_count_for_tests(ReloadScope::Public, "ok");
            atomic_write(&cert_path, &pki_v2.leaf_cert_pem);
            atomic_write(&key_path, &pki_v2.leaf_key_pem);
            tracing::info!("rotated cert + key on disk to v2");

            // 7. Wait for the public-scope `ok` reload bucket to tick.
            //    PollWatcher uses a 2s poll interval (see CertWatcher::spawn),
            //    so allow up to 15s for the rotation to be picked up.
            assert!(
                wait_for(Duration::from_secs(15), || async {
                    reload_count_for_tests(ReloadScope::Public, "ok") > reload_count_before
                })
                .await,
                "public `ok` reload bucket never incremented after rotation"
            );

            // 8. Verify each endpoint now serves the v2 leaf.
            //    Retry briefly because debounce + handshake racing with reload
            //    can cause one extra v1 handshake right at the boundary.
            for (name, port) in [
                ("http", http_port),
                ("metrics", metrics_port),
                ("flight", flight_port),
            ] {
                let target = pki_v2.leaf_fingerprint_hex.clone();
                let ca = pki_v2.ca_pem.clone();
                let deadline = Instant::now() + Duration::from_secs(15);
                let mut last = String::new();
                let mut saw_v2 = false;
                while Instant::now() < deadline {
                    if let Ok(fp) =
                        fetch_server_leaf_fingerprint("127.0.0.1", port, &ca).await
                    {
                        last = fp.clone();
                        if fp == target {
                            saw_v2 = true;
                            break;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                assert!(
                    saw_v2,
                    "{name}: post-rotation fingerprint never matched v2 (last seen: {last}, want: {})",
                    pki_v2.leaf_fingerprint_hex
                );
                tracing::info!("{name}: post-rotation fingerprint = {last}");
            }

            // 9. Smoke-test Flight again under the new cert.
            run_flight_smoke_query(flight_port, &pki_v2.ca_pem).await?;

            Ok(())
        })
        .await
}

/// Verify that a malformed PEM written to the cert path does NOT take down
/// the server: the old cert keeps serving and a `parse_error` metric ticks.
#[tokio::test]
async fn test_tls_hot_reload_rejects_malformed_pem() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    install_crypto_provider();

    test_request_context()
        .scope(async {
            let pki = generate_ca_and_leaf("spiced-stable");
            let dir = TempDir::new()?;
            let cert_path = dir.path().join("server.crt");
            let key_path = dir.path().join("server.key");
            std::fs::write(&cert_path, &pki.leaf_cert_pem)?;
            std::fs::write(&key_path, &pki.leaf_key_pem)?;

            let control = runtime::tls::TlsControl::new()?;
            let tls_config = Arc::new(
                TlsConfig::try_new_from_paths(cert_path.clone(), key_path.clone(), &control)
                    .map_err(|e| anyhow::anyhow!("build TlsConfig: {e}"))?,
            );

            let mut rng = rand::rng();
            let http_port: u16 = rng.random_range(50000..60000);
            let flight_port: u16 = http_port + 1;
            let metrics_port: u16 = http_port + 2;

            let api_config = Config::new()
                .with_http_bind_address(SocketAddr::new(LOCALHOST, http_port))
                .with_flight_bind_address(SocketAddr::new(LOCALHOST, flight_port));
            let registry = prometheus::Registry::new();
            let app = app::AppBuilder::new("test_app").build();
            let rt = Arc::new(
                Runtime::builder()
                    .with_metrics_server(SocketAddr::new(LOCALHOST, metrics_port), registry)
                    .with_app(app)
                    .build()
                    .await,
            );
            let rt_for_servers = Arc::clone(&rt);
            let tls_for_servers = Arc::clone(&tls_config);
            tokio::spawn(async move {
                Box::pin(rt_for_servers.start_servers(
                    api_config,
                    Some(tls_for_servers),
                    EndpointAuth::no_auth(),
                ))
                .await
            });

            assert!(
                wait_for(Duration::from_secs(10), || async {
                    fetch_server_leaf_fingerprint("127.0.0.1", http_port, &pki.ca_pem)
                        .await
                        .is_ok()
                })
                .await,
                "HTTP server did not become reachable"
            );

            let baseline =
                fetch_server_leaf_fingerprint("127.0.0.1", http_port, &pki.ca_pem).await?;
            assert_eq!(baseline, pki.leaf_fingerprint_hex);

            // Write garbage. Server should keep serving the old cert.
            let reload_before = reload_count_for_tests(ReloadScope::Public, "parse_error");
            atomic_write(&cert_path, "this is not a PEM\n");

            // Wait for the public-scope `parse_error` bucket to tick. We
            //  assert on the specific outcome bucket so this test does not
            //  pass spuriously when the watcher fires for a different reason.
            assert!(
                wait_for(Duration::from_secs(15), || async {
                    reload_count_for_tests(ReloadScope::Public, "parse_error") > reload_before
                })
                .await,
                "public `parse_error` bucket never incremented after writing malformed PEM"
            );

            // Old cert should still serve.
            let after = fetch_server_leaf_fingerprint("127.0.0.1", http_port, &pki.ca_pem).await?;
            assert_eq!(
                after, pki.leaf_fingerprint_hex,
                "server downgraded to a different cert after rejecting malformed PEM"
            );

            Ok(())
        })
        .await
}

async fn run_flight_smoke_query(flight_port: u16, ca_pem: &str) -> anyhow::Result<()> {
    use tonic::transport::{Certificate, ClientTlsConfig};

    let ca = Certificate::from_pem(ca_pem.as_bytes());
    let tls = ClientTlsConfig::new()
        .ca_certificate(ca)
        .domain_name("localhost");
    let channel = Channel::from_shared(format!("https://localhost:{flight_port}"))?
        .tls_config(tls)?
        .connect()
        .await?;

    let mut client = FlightServiceClient::new(channel);
    let sql = CommandStatementQuery {
        query: "show tables".to_string(),
        transaction_id: None,
    };
    let req = FlightDescriptor::new_cmd(sql.as_any().encode_to_vec());
    let _ = client.get_flight_info(req).await?;
    Ok(())
}
