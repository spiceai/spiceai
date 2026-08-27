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

//! Process-level proof that `spiced --token` enrollment gates listeners and
//! readiness, and that an authority echoed by an untrusted HTTPS peer is still
//! absent from process output.

#![expect(
    clippy::expect_used,
    reason = "process-level integration harness — descriptive expects keep failures actionable"
)]

use std::io::{Read, Write as _};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

fn unused_local_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
    listener.local_addr().expect("read ephemeral address")
}

fn read_http_request(stream: &mut impl Read) -> serde_json::Value {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 4096];
    loop {
        let count = stream.read(&mut buffer).expect("read enrollment request");
        assert_ne!(count, 0, "client closed before sending the whole request");
        bytes.extend_from_slice(&buffer[..count]);

        let Some(header_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") else {
            continue;
        };
        let headers = String::from_utf8_lossy(&bytes[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .unwrap_or_default();
        if bytes.len() >= header_end + 4 + content_length {
            return serde_json::from_slice(&bytes[header_end + 4..header_end + 4 + content_length])
                .expect("parse enrollment request JSON");
        }
    }
}

struct HttpsFixture {
    listener: TcpListener,
    address: SocketAddr,
    tls: Arc<rustls::ServerConfig>,
    ca_path: PathBuf,
}

fn https_fixture(config_dir: &Path) -> HttpsFixture {
    use std::net::{IpAddr, Ipv4Addr};

    use rcgen::{
        BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
        KeyUsagePurpose, SanType,
    };

    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let ca_key = KeyPair::generate().expect("generate HTTPS test CA key");
    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
    let ca_certificate = ca_params
        .self_signed(&ca_key)
        .expect("self-sign HTTPS test CA");
    let issuer = Issuer::new(ca_params, ca_key);

    let server_key = KeyPair::generate().expect("generate HTTPS server key");
    let mut server_params = CertificateParams::default();
    server_params.subject_alt_names = vec![SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST))];
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let server_certificate = server_params
        .signed_by(&server_key, &issuer)
        .expect("sign HTTPS server certificate");
    let server_key = rustls::pki_types::PrivateKeyDer::Pkcs8(
        rustls::pki_types::PrivatePkcs8KeyDer::from(server_key.serialize_der()),
    );
    let tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![server_certificate.der().clone()], server_key)
        .expect("build HTTPS server configuration");

    let ca_path = config_dir.join("cloud-connect-test-ca.pem");
    std::fs::write(&ca_path, ca_certificate.pem()).expect("write HTTPS test CA");
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind enrollment HTTPS mock");
    let address = listener
        .local_addr()
        .expect("read enrollment HTTPS mock address");
    HttpsFixture {
        listener,
        address,
        tls: Arc::new(tls),
        ca_path,
    }
}

fn accept_https(
    listener: &TcpListener,
    tls: Arc<rustls::ServerConfig>,
) -> rustls::StreamOwned<rustls::ServerConnection, TcpStream> {
    let (stream, _) = listener.accept().expect("accept enrollment HTTPS request");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set enrollment request timeout");
    let connection = rustls::ServerConnection::new(tls).expect("start enrollment TLS server");
    rustls::StreamOwned::new(connection, stream)
}

fn sign_enrollment_csr(csr_pem: &str) -> (String, String) {
    use rcgen::{
        BasicConstraints, CertificateParams, CertificateSigningRequestParams, IsCa, Issuer,
        KeyPair, KeyUsagePurpose,
    };

    let ca_key = KeyPair::generate().expect("generate enrollment test CA key");
    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
    let ca_certificate = ca_params
        .self_signed(&ca_key)
        .expect("self-sign enrollment test CA");
    let issuer = Issuer::new(ca_params, ca_key);
    let mut identity_params =
        CertificateSigningRequestParams::from_pem(csr_pem).expect("parse enrollment CSR");
    identity_params.params.not_after = rcgen::date_time_ymd(2099, 1, 1);
    let identity_certificate = identity_params
        .signed_by(&issuer)
        .expect("sign enrollment CSR");
    (identity_certificate.pem(), ca_certificate.pem())
}

#[test]
fn enrollment_failure_happens_before_any_listener_or_readiness() {
    let config_dir = tempfile::tempdir().expect("create config directory");
    let runtime_addr = unused_local_addr();
    let fixture = https_fixture(config_dir.path());
    let enrollment_addr = fixture.address;
    let ca_path = fixture.ca_path;
    let enrollment_tls = Arc::clone(&fixture.tls);
    let enrollment_listener = fixture.listener;
    let key = format!("spice-enroll-{}", "C".repeat(32));

    let (request_seen_tx, request_seen_rx) = mpsc::sync_channel(1);
    let (respond_tx, respond_rx) = mpsc::sync_channel(1);
    let echoed_key = key.clone();
    let server = std::thread::spawn(move || {
        let mut stream = accept_https(&enrollment_listener, enrollment_tls);
        let _request = read_http_request(&mut stream);
        request_seen_tx.send(()).expect("signal request");
        respond_rx.recv().expect("wait for listener assertion");

        // Deliberately hostile response: the peer echoes the bearer through
        // JSON escapes, so raw byte replacement cannot find it. The runtime
        // must redact the decoded field before constructing or logging the
        // error.
        let escaped_key = echoed_key.replace('-', r"\u002d");
        let body = format!(
            r#"{{"code":"invalid_token","error":"rejected enrollment key {escaped_key}"}}"#
        );
        assert!(
            !body.contains(&echoed_key),
            "the mock response must not contain the literal key"
        );
        let response = format!(
            "HTTP/1.1 401 Unauthorized\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        stream
            .write_all(response.as_bytes())
            .expect("write enrollment rejection");
        stream.flush().expect("flush enrollment rejection");
    });

    let mut child = Command::new(env!("CARGO_BIN_EXE_spiced"))
        .args(["--token", &key, "--http", &runtime_addr.to_string()])
        .env("SPICE_CONFIG_DIR", config_dir.path())
        .env("SPICE_CLOUD_ENDPOINT", format!("https://{enrollment_addr}"))
        .env("SSL_CERT_FILE", ca_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start spiced");

    // A default-feature `spiced` binary is large enough that cold macOS
    // loading/signature checks can take tens of seconds on a busy builder.
    let request_deadline = Instant::now() + Duration::from_mins(1);
    loop {
        match request_seen_rx.recv_timeout(Duration::from_millis(20)) {
            Ok(()) => break,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!("enrollment mock exited before receiving a request")
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
        if let Some(status) = child.try_wait().expect("poll spiced startup") {
            let output = child
                .wait_with_output()
                .expect("collect early spiced output");
            panic!(
                "spiced exited as {status} before enrollment: stdout={} stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        assert!(
            TcpStream::connect_timeout(&runtime_addr, Duration::from_millis(20)).is_err(),
            "the HTTP listener became reachable before enrollment was attempted"
        );
        if Instant::now() >= request_deadline {
            child.kill().expect("kill spiced that never enrolled");
            let output = child
                .wait_with_output()
                .expect("collect stuck spiced output");
            panic!(
                "spiced did not attempt enrollment within the startup budget: stdout={} stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
    assert!(
        TcpStream::connect_timeout(&runtime_addr, Duration::from_millis(250)).is_err(),
        "the HTTP listener became reachable before enrollment completed"
    );
    assert!(
        !config_dir.path().join("identity.json").exists(),
        "a failed enrollment must not create an identity"
    );
    respond_tx.send(()).expect("release enrollment response");

    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        match child.try_wait().expect("poll spiced") {
            Some(status) => break status,
            None if Instant::now() >= deadline => {
                child.kill().expect("kill stuck spiced");
                panic!("spiced did not exit after terminal enrollment rejection");
            }
            None => std::thread::sleep(Duration::from_millis(10)),
        }
    };
    let output = child.wait_with_output().expect("collect spiced output");
    server.join().expect("enrollment mock");

    assert!(
        !status.success(),
        "terminal enrollment failure must exit nonzero"
    );
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined.contains("Failed to enroll this instance"),
        "{combined}"
    );
    assert!(
        !combined.contains(&key),
        "process output leaked the key: {combined}"
    );
    assert!(
        combined.contains("the server returned an error while processing the enrollment request"),
        "{combined}"
    );
}

#[test]
fn successful_enrollment_is_durable_before_the_runtime_listener_binds() {
    let config_dir = tempfile::tempdir().expect("create config directory");
    let runtime_addr = unused_local_addr();
    let metrics_addr = unused_local_addr();
    let flight_addr = unused_local_addr();
    let fixture = https_fixture(config_dir.path());
    let enrollment_addr = fixture.address;
    let ca_path = fixture.ca_path;
    let enrollment_tls = Arc::clone(&fixture.tls);
    let enrollment_listener = fixture.listener;
    let key = format!("spice-enroll-{}", "D".repeat(32));

    let server = std::thread::spawn(move || {
        let mut stream = accept_https(&enrollment_listener, enrollment_tls);
        let request = read_http_request(&mut stream);
        let csr_pem = request["csr_pem"]
            .as_str()
            .expect("enrollment request contains a CSR");
        let (identity_cert_pem, ca_bundle_pem) = sign_enrollment_csr(csr_pem);
        let body = serde_json::json!({
            "instance_id": "inst_process_bootstrap",
            "identity_cert_pem": identity_cert_pem,
            "ca_bundle_pem": ca_bundle_pem,
            "gateway_addr": "127.0.0.1:9",
            "not_after": "2099-01-01T00:00:00Z",
            "organization": {"id": 42, "name": "acme"},
        })
        .to_string();
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        stream
            .write_all(response.as_bytes())
            .expect("write enrollment success");
        stream.flush().expect("flush enrollment success");
    });

    let mut child = Command::new(env!("CARGO_BIN_EXE_spiced"))
        .args([
            "--token",
            &key,
            "--http",
            &runtime_addr.to_string(),
            "--metrics",
            &metrics_addr.to_string(),
            "--flight",
            &flight_addr.to_string(),
        ])
        .current_dir(config_dir.path())
        .env("SPICE_CONFIG_DIR", config_dir.path())
        .env("SPICE_CLOUD_ENDPOINT", format!("https://{enrollment_addr}"))
        .env("SSL_CERT_FILE", ca_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start spiced");

    let identity_path = config_dir.path().join("identity.json");
    let deadline = Instant::now() + Duration::from_mins(1);
    loop {
        let http_reachable =
            TcpStream::connect_timeout(&runtime_addr, Duration::from_millis(20)).is_ok();
        let metrics_reachable =
            TcpStream::connect_timeout(&metrics_addr, Duration::from_millis(20)).is_ok();
        let flight_reachable =
            TcpStream::connect_timeout(&flight_addr, Duration::from_millis(20)).is_ok();
        assert!(
            !(http_reachable || metrics_reachable || flight_reachable) || identity_path.exists(),
            "a runtime listener became reachable before identity.json was durable: http={http_reachable}, metrics={metrics_reachable}, flight={flight_reachable}"
        );
        if http_reachable {
            break;
        }
        if let Some(status) = child.try_wait().expect("poll spiced startup") {
            let output = child
                .wait_with_output()
                .expect("collect early spiced output");
            panic!(
                "spiced exited as {status} before binding after enrollment: stdout={} stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        if Instant::now() >= deadline {
            child.kill().expect("kill spiced that never bound");
            let output = child
                .wait_with_output()
                .expect("collect stuck spiced output");
            panic!(
                "spiced did not bind within the startup budget: stdout={} stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    let identity = std::fs::read_to_string(&identity_path).expect("read durable identity");
    assert!(identity.contains("inst_process_bootstrap"), "{identity}");
    assert!(
        !identity.contains(&key),
        "identity leaked the key: {identity}"
    );

    child.kill().expect("stop spiced after assertion");
    let output = child.wait_with_output().expect("collect spiced output");
    server.join().expect("enrollment mock");
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!combined.contains(&key), "process output leaked the key");
}
