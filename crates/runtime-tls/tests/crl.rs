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

//! Client certificate revocation (CRLs) on the public TLS listeners.
//!
//! Every test drives a real TLS 1.3 handshake over loopback TCP against the
//! `rustls::ServerConfig`s that [`TlsConfig`] hands the Flight listener
//! (strict verifier) and the HTTP and metrics listeners (lax verifier), and
//! asserts the server's verdict: the exact client certificate it admitted,
//! or the exact rustls error it rejected the client with.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use rcgen::{
    BasicConstraints, CertificateParams, CertificateRevocationListParams, DistinguishedName,
    DnType, IsCa, Issuer, KeyIdMethod, KeyPair, KeyUsagePurpose, RevocationReason,
    RevokedCertParams, SanType, SerialNumber,
};
use runtime_tls::{ClientAuthEnforcement, ClientTrustPaths, ClientTrustPem, TlsConfig, TlsControl};
use rustls::{
    CertificateError, ClientConfig, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, pem::PemObject},
};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, TlsConnector};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a rewritten file may take to reach the verifiers through the
/// watcher, which polls every 2s.
const WATCH_TIMEOUT: Duration = Duration::from_secs(20);

fn install_crypto_provider() {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
}

/// A certificate authority that issues client certificates and signs CRLs.
struct Ca {
    issuer: Issuer<'static, KeyPair>,
    cert_pem: String,
}

impl Ca {
    fn new(name: &str) -> Self {
        let mut params = CertificateParams::default();
        params.distinguished_name = DistinguishedName::new();
        params.distinguished_name.push(DnType::CommonName, name);
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        let key = KeyPair::generate().expect("generate CA key");
        let cert = params.self_signed(&key).expect("self-sign CA");
        Self {
            issuer: Issuer::new(params, key),
            cert_pem: cert.pem(),
        }
    }

    fn issue(&self, name: &str, serial: u64, server: bool) -> Leaf {
        let mut params = CertificateParams::default();
        params.distinguished_name = DistinguishedName::new();
        params.distinguished_name.push(DnType::CommonName, name);
        params.serial_number = Some(SerialNumber::from(serial));
        if server {
            params
                .subject_alt_names
                .push(SanType::DnsName("localhost".try_into().expect("DNS name")));
        }
        let key = KeyPair::generate().expect("generate leaf key");
        let cert = params
            .signed_by(&key, &self.issuer)
            .expect("CA signs the leaf");
        Leaf {
            cert_der: cert.der().clone(),
            cert_pem: cert.pem(),
            key_der: key.serialize_der(),
            key_pem: key.serialize_pem(),
        }
    }

    /// A CRL from this CA revoking `revoked` serial numbers, valid until
    /// `next_update` from now (negative: already expired).
    fn crl(
        &self,
        revoked: &[u64],
        next_update: time::Duration,
    ) -> rcgen::CertificateRevocationList {
        let now = time::OffsetDateTime::now_utc();
        let (this_update, next_update) = if next_update.is_negative() {
            (
                now + next_update - time::Duration::days(1),
                now + next_update,
            )
        } else {
            (now - time::Duration::minutes(5), now + next_update)
        };
        CertificateRevocationListParams {
            this_update,
            next_update,
            crl_number: SerialNumber::from(1u64),
            issuing_distribution_point: None,
            revoked_certs: revoked
                .iter()
                .map(|serial| RevokedCertParams {
                    serial_number: SerialNumber::from(*serial),
                    revocation_time: this_update,
                    reason_code: Some(RevocationReason::KeyCompromise),
                    invalidity_date: None,
                })
                .collect(),
            key_identifier_method: KeyIdMethod::Sha256,
        }
        .signed_by(&self.issuer)
        .expect("CA signs the CRL")
    }

    fn crl_pem(&self, revoked: &[u64]) -> String {
        self.crl(revoked, time::Duration::days(7))
            .pem()
            .expect("CRL PEM")
    }
}

struct Leaf {
    cert_der: CertificateDer<'static>,
    cert_pem: String,
    key_der: Vec<u8>,
    key_pem: String,
}

/// The server's verdict on one handshake: the client certificate it
/// admitted (`None` for an admitted client that presented none), or the
/// rustls error it rejected the client with.
type Verdict = Result<Option<CertificateDer<'static>>, rustls::Error>;

/// Handshake `client` (or a client presenting no certificate) against
/// `server_config` over loopback, and return the server's verdict.
async fn handshake(server_config: &Arc<ServerConfig>, ca: &Ca, client: Option<&Leaf>) -> Verdict {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("listener address");

    let acceptor = TlsAcceptor::from(Arc::clone(server_config));
    let server = async move {
        let (tcp, _) = listener.accept().await.expect("accept TCP");
        acceptor.accept(tcp).await
    };

    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from_pem_slice(ca.cert_pem.as_bytes()).expect("CA certificate PEM"))
        .expect("trust the CA");
    let builder = ClientConfig::builder().with_root_certificates(roots);
    let client_config = match client {
        Some(leaf) => builder
            .with_client_auth_cert(
                vec![leaf.cert_der.clone()],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(leaf.key_der.clone())),
            )
            .expect("client identity"),
        None => builder.with_no_client_auth(),
    };
    let connector = TlsConnector::from(Arc::new(client_config));
    let client = async move {
        let tcp = TcpStream::connect(addr).await.expect("connect TCP");
        let server_name = ServerName::try_from("localhost").expect("server name");
        // Hold the client side open until the server has decided.
        connector.connect(server_name, tcp).await
    };

    let (server_result, _client_result) =
        tokio::time::timeout(HANDSHAKE_TIMEOUT, async { tokio::join!(server, client) })
            .await
            .expect("handshake finishes within the timeout");

    match server_result {
        Ok(stream) => Ok(stream
            .get_ref()
            .1
            .peer_certificates()
            .and_then(|certs| certs.first())
            .map(|cert| cert.clone().into_owned())),
        Err(err) => Err(err
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<rustls::Error>())
            .cloned()
            .unwrap_or_else(|| panic!("server handshake failed without a rustls error: {err}"))),
    }
}

fn revoked() -> Verdict {
    Err(rustls::Error::InvalidCertificate(CertificateError::Revoked))
}

fn unknown_revocation_status() -> Verdict {
    Err(rustls::Error::InvalidCertificate(
        CertificateError::UnknownRevocationStatus,
    ))
}

#[expect(
    clippy::unnecessary_wraps,
    reason = "compared against the `Verdict` a handshake returns"
)]
fn admitted(leaf: &Leaf) -> Verdict {
    Ok(Some(leaf.cert_der.clone()))
}

struct Fixture {
    ca: Ca,
    server: Leaf,
    alice: Leaf,
    bob: Leaf,
}

const ALICE_SERIAL: u64 = 1001;
const BOB_SERIAL: u64 = 1002;

fn fixture() -> Fixture {
    install_crypto_provider();
    let ca = Ca::new("Spice CRL test CA");
    let server = ca.issue("spiced", 1, true);
    let alice = ca.issue("alice", ALICE_SERIAL, false);
    let bob = ca.issue("bob", BOB_SERIAL, false);
    Fixture {
        ca,
        server,
        alice,
        bob,
    }
}

fn inline_config(f: &Fixture, ca_pem: &str, crl: Option<&[u8]>) -> TlsConfig {
    TlsConfig::try_new_with_client_auth_mode(
        f.server.cert_pem.as_bytes(),
        f.server.key_pem.as_bytes(),
        Some(ClientTrustPem {
            ca: ca_pem.as_bytes(),
            crl,
        }),
        ClientAuthEnforcement::Required,
    )
    .expect("TLS config with client CRLs")
}

#[tokio::test]
async fn revoked_client_certificate_is_rejected_by_every_listener() {
    let f = fixture();
    let crl = f.ca.crl_pem(&[BOB_SERIAL]);
    let tls = inline_config(&f, &f.ca.cert_pem, Some(crl.as_bytes()));

    // Flight: strict verifier.
    let flight = &tls.flight_server_config;
    assert_eq!(
        handshake(flight, &f.ca, Some(&f.alice)).await,
        admitted(&f.alice)
    );
    assert_eq!(handshake(flight, &f.ca, Some(&f.bob)).await, revoked());
    assert_eq!(
        handshake(flight, &f.ca, None).await,
        Err(rustls::Error::NoCertificatesPresented)
    );

    // HTTP and metrics: lax verifier. A client without a certificate is
    // admitted (for probes), but a revoked certificate is never downgraded
    // to an anonymous client.
    let http = &tls.http_server_config;
    assert_eq!(
        handshake(http, &f.ca, Some(&f.alice)).await,
        admitted(&f.alice)
    );
    assert_eq!(handshake(http, &f.ca, Some(&f.bob)).await, revoked());
    assert_eq!(handshake(http, &f.ca, None).await, Ok(None));
}

#[tokio::test]
async fn without_crls_a_revoked_certificate_is_admitted() {
    // The control for every other test here: the same certificate the CRL
    // revokes is admitted when no CRL is configured, so a rejection
    // elsewhere is the CRL's doing.
    let f = fixture();
    let tls = inline_config(&f, &f.ca.cert_pem, None);
    assert_eq!(
        handshake(&tls.flight_server_config, &f.ca, Some(&f.bob)).await,
        admitted(&f.bob)
    );
}

#[tokio::test]
async fn der_encoded_crl_is_enforced() {
    let f = fixture();
    let crl = f.ca.crl(&[BOB_SERIAL], time::Duration::days(7));
    let tls = inline_config(&f, &f.ca.cert_pem, Some(crl.der().as_ref()));
    let flight = &tls.flight_server_config;
    assert_eq!(
        handshake(flight, &f.ca, Some(&f.alice)).await,
        admitted(&f.alice)
    );
    assert_eq!(handshake(flight, &f.ca, Some(&f.bob)).await, revoked());
}

#[tokio::test]
async fn certificate_from_a_ca_without_a_crl_is_rejected() {
    // Fail closed: with CRLs configured, a client whose issuer published no
    // CRL in the bundle has an unknown revocation status and is rejected,
    // even though its CA is trusted.
    let f = fixture();
    let other_ca = Ca::new("Spice CRL test CA without a CRL");
    let carol = other_ca.issue("carol", 2001, false);
    let bundle = format!("{}{}", f.ca.cert_pem, other_ca.cert_pem);
    let crl = f.ca.crl_pem(&[]);
    let tls = inline_config(&f, &bundle, Some(crl.as_bytes()));
    let flight = &tls.flight_server_config;
    assert_eq!(
        handshake(flight, &f.ca, Some(&f.alice)).await,
        admitted(&f.alice)
    );
    assert_eq!(
        handshake(flight, &f.ca, Some(&carol)).await,
        unknown_revocation_status()
    );
    assert_eq!(
        handshake(&tls.http_server_config, &f.ca, Some(&carol)).await,
        unknown_revocation_status()
    );
}

#[tokio::test]
async fn crls_from_several_cas_in_one_bundle_are_each_enforced() {
    let f = fixture();
    let other_ca = Ca::new("Spice CRL test CA two");
    let carol = other_ca.issue("carol", 2001, false);
    let dave = other_ca.issue("dave", 2002, false);
    let bundle = format!("{}{}", f.ca.cert_pem, other_ca.cert_pem);
    let crls = format!(
        "{}{}",
        f.ca.crl_pem(&[BOB_SERIAL]),
        other_ca.crl_pem(&[2002])
    );
    let tls = inline_config(&f, &bundle, Some(crls.as_bytes()));
    let flight = &tls.flight_server_config;
    assert_eq!(
        handshake(flight, &f.ca, Some(&f.alice)).await,
        admitted(&f.alice)
    );
    assert_eq!(handshake(flight, &f.ca, Some(&f.bob)).await, revoked());
    assert_eq!(
        handshake(flight, &f.ca, Some(&carol)).await,
        admitted(&carol)
    );
    assert_eq!(handshake(flight, &f.ca, Some(&dave)).await, revoked());
}

#[tokio::test]
async fn expired_crl_still_enforces_its_revocations() {
    let f = fixture();
    let crl =
        f.ca.crl(&[BOB_SERIAL], -time::Duration::hours(1))
            .pem()
            .expect("CRL PEM");
    let tls = inline_config(&f, &f.ca.cert_pem, Some(crl.as_bytes()));
    let flight = &tls.flight_server_config;
    assert_eq!(
        handshake(flight, &f.ca, Some(&f.alice)).await,
        admitted(&f.alice)
    );
    assert_eq!(handshake(flight, &f.ca, Some(&f.bob)).await, revoked());
}

#[tokio::test]
async fn crl_signed_by_another_key_rejects_the_ca_clients() {
    // A CRL naming the trusted CA as issuer but signed by a different key
    // (a forged or mis-rotated CRL) must not be trusted to clear anyone.
    let f = fixture();
    let impostor = Ca::new("Spice CRL test CA");
    let crl = impostor.crl_pem(&[]);
    let tls = inline_config(&f, &f.ca.cert_pem, Some(crl.as_bytes()));
    let verdict = handshake(&tls.flight_server_config, &f.ca, Some(&f.alice)).await;
    assert_eq!(
        verdict,
        Err(rustls::Error::InvalidCertRevocationList(
            rustls::CertRevocationListError::BadSignature
        ))
    );
}

#[test]
fn unusable_crl_material_fails_construction() {
    let f = fixture();
    let build = |crl: &[u8]| {
        TlsConfig::try_new_with_client_auth_mode(
            f.server.cert_pem.as_bytes(),
            f.server.key_pem.as_bytes(),
            Some(ClientTrustPem {
                ca: f.ca.cert_pem.as_bytes(),
                crl: Some(crl),
            }),
            ClientAuthEnforcement::Required,
        )
        .err()
        .map(|err| err.to_string())
    };
    let no_crls = "client certificate revocation list file contained no CRLs: expected PEM `-----BEGIN X509 CRL-----` blocks or one DER-encoded CRL";
    assert_eq!(build(b"").as_deref(), Some(no_crls));
    // The CA bundle passed where the CRLs belong.
    assert_eq!(build(f.ca.cert_pem.as_bytes()).as_deref(), Some(no_crls));
    assert_eq!(
        build(&[0x30, 0x03, 0x02, 0x01, 0x00]).as_deref(),
        Some("invalid client certificate revocation list: ParseError")
    );
}

fn write(path: &Path, contents: impl AsRef<[u8]>) {
    // Write then rename, the way cert-manager and SPIRE rotate files, so the
    // watcher never reads a half-written file.
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, contents).expect("write temp file");
    std::fs::rename(&tmp, path).expect("rename into place");
}

struct Files {
    _dir: tempfile::TempDir,
    cert: PathBuf,
    key: PathBuf,
    ca: PathBuf,
    crl: PathBuf,
}

fn files(f: &Fixture, crl: &str) -> Files {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let files = Files {
        cert: dir.path().join("server.crt"),
        key: dir.path().join("server.key"),
        ca: dir.path().join("client-ca.crt"),
        crl: dir.path().join("client.crl"),
        _dir: dir,
    };
    write(&files.cert, &f.server.cert_pem);
    write(&files.key, &f.server.key_pem);
    write(&files.ca, &f.ca.cert_pem);
    write(&files.crl, crl);
    files
}

/// Handshake until the server's verdict on `client` is `expected`, failing
/// with the last verdict seen if it does not arrive within `WATCH_TIMEOUT`.
async fn await_verdict(tls: &TlsConfig, ca: &Ca, client: &Leaf, expected: &Verdict) {
    let deadline = Instant::now() + WATCH_TIMEOUT;
    loop {
        let verdict = handshake(&tls.flight_server_config, ca, Some(client)).await;
        if &verdict == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "verdict did not become {expected:?} within {WATCH_TIMEOUT:?}; last verdict: {verdict:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn rotated_crl_file_revokes_without_a_restart() {
    let f = fixture();
    let files = files(&f, &f.ca.crl_pem(&[]));
    let control = TlsControl::new().expect("TLS control");
    let tls = TlsConfig::try_new_from_paths_with_client_auth_mode(
        files.cert.clone(),
        files.key.clone(),
        Some(ClientTrustPaths {
            ca: files.ca.clone(),
            crl: Some(files.crl.clone()),
        }),
        ClientAuthEnforcement::Required,
        &control,
    )
    .expect("file-backed TLS config with client CRLs");

    let flight = &tls.flight_server_config;
    assert_eq!(
        handshake(flight, &f.ca, Some(&f.bob)).await,
        admitted(&f.bob)
    );

    // The CA revokes bob and republishes its CRL; the watcher picks it up.
    write(&files.crl, f.ca.crl_pem(&[BOB_SERIAL]));
    await_verdict(&tls, &f.ca, &f.bob, &revoked()).await;
    assert_eq!(
        handshake(flight, &f.ca, Some(&f.alice)).await,
        admitted(&f.alice)
    );
    assert_eq!(
        handshake(&tls.http_server_config, &f.ca, Some(&f.bob)).await,
        revoked()
    );
}

#[tokio::test]
async fn unusable_crl_rotation_keeps_the_previous_crls() {
    let f = fixture();
    let files = files(&f, &f.ca.crl_pem(&[BOB_SERIAL]));
    let control = TlsControl::new().expect("TLS control");
    let tls = TlsConfig::try_new_from_paths_with_client_auth_mode(
        files.cert.clone(),
        files.key.clone(),
        Some(ClientTrustPaths {
            ca: files.ca.clone(),
            crl: Some(files.crl.clone()),
        }),
        ClientAuthEnforcement::Required,
        &control,
    )
    .expect("file-backed TLS config with client CRLs");
    let flight = &tls.flight_server_config;

    // Each of these would disable revocation checking if it were taken to
    // mean "no CRLs"; each must leave bob revoked and alice admitted.
    for unusable in [&b""[..], b"not a CRL", f.ca.cert_pem.as_bytes()] {
        write(&files.crl, unusable);
        tls.force_reload_for_tests();
        assert_eq!(handshake(flight, &f.ca, Some(&f.bob)).await, revoked());
        assert_eq!(
            handshake(flight, &f.ca, Some(&f.alice)).await,
            admitted(&f.alice)
        );
    }

    // A missing CRL file (the publisher deleted it mid-rotation) likewise.
    std::fs::remove_file(&files.crl).expect("remove CRL file");
    tls.force_reload_for_tests();
    assert_eq!(handshake(flight, &f.ca, Some(&f.bob)).await, revoked());
}

#[test]
fn missing_crl_file_fails_construction() {
    let f = fixture();
    let files = files(&f, &f.ca.crl_pem(&[]));
    std::fs::remove_file(&files.crl).expect("remove CRL file");
    let control = TlsControl::new().expect("TLS control");
    let err = TlsConfig::try_new_from_paths_with_client_auth_mode(
        files.cert.clone(),
        files.key.clone(),
        Some(ClientTrustPaths {
            ca: files.ca.clone(),
            crl: Some(files.crl.clone()),
        }),
        ClientAuthEnforcement::Required,
        &control,
    )
    .err()
    .map(|err| err.to_string())
    .expect("a missing CRL file must fail construction");
    assert!(
        err.starts_with(&format!(
            "failed to read TLS file at {}: ",
            files.crl.display()
        )),
        "unexpected error: {err}"
    );
}
