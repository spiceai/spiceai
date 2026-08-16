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

#![expect(clippy::expect_used, reason = "process-level integration-test harness")]

use std::collections::BTreeMap;
use std::io::{Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use assert_cmd::{Command, cargo::cargo_bin_cmd};
use rcgen::{
    BasicConstraints, CertificateParams, CertificateSigningRequestParams, DnType,
    ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose,
};
use tempfile::TempDir;

const LOGIN_TOKEN: &str = "fixture-login-token";
const INSTANCE_ID: &str = "inst_fixture_1401";
const PROJECT_NAME: &str = "fault-replay";
const NOT_AFTER_UNIX: u64 = 1_893_456_000;
const NOT_AFTER_RFC3339: &str = "2030-01-01T00:00:00Z";

fn spice_cmd() -> Command {
    cargo_bin_cmd!("spice")
}

/// Install a runtime that exits immediately into an isolated home.
///
/// A completed `spice connect` ends at a running instance, so every fixture
/// here would otherwise install and run a real `spiced`. The stub keeps these
/// tests about the transaction: the CLI starts it, it exits 0, and the command
/// returns.
#[cfg(unix)]
fn stub_runtime(home: &TempDir) {
    use std::os::unix::fs::PermissionsExt as _;

    let bin = home.path().join(".spice").join("bin");
    std::fs::create_dir_all(&bin).expect("create the stub runtime directory");
    let stub = bin.join("spiced");
    std::fs::write(&stub, "#!/bin/sh\nexit 0\n").expect("write the stub runtime");
    std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755))
        .expect("make the stub runtime executable");
}

/// No stub is needed where the CLI does not manage a local runtime: `connect`
/// completes the transaction and names how to start the instance instead.
#[cfg(not(unix))]
fn stub_runtime(_home: &TempDir) {}

struct Request {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
    body: String,
}

#[derive(Default)]
struct FixtureState {
    requests: Vec<Request>,
    enrollment_bodies: Vec<String>,
    enrollment_operations: Vec<String>,
    project_bodies: Vec<String>,
    instance_mutations: usize,
    project_mutations: usize,
    enrollment_response: Option<String>,
}

struct TestCa {
    cert_pem: String,
    issuer: Issuer<'static, KeyPair>,
}

impl TestCa {
    fn new() -> Self {
        let key = KeyPair::generate().expect("generate fixture CA key");
        let mut params = CertificateParams::default();
        params
            .distinguished_name
            .push(DnType::CommonName, "Spice Connect Test CA");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let cert_pem = params
            .self_signed(&key)
            .expect("self-sign fixture CA")
            .pem();
        Self {
            cert_pem,
            issuer: Issuer::new(params, key),
        }
    }

    fn sign_csr(&self, csr_pem: &str) -> String {
        let mut request =
            CertificateSigningRequestParams::from_pem(csr_pem).expect("parse enrollment CSR");
        request.params.not_after = time::OffsetDateTime::from_unix_timestamp(
            i64::try_from(NOT_AFTER_UNIX).expect("fixture expiry fits i64"),
        )
        .expect("valid fixture expiry");
        request
            .signed_by(&self.issuer)
            .expect("sign enrollment CSR")
            .pem()
    }
}

struct Fixture {
    endpoint: String,
    state: Arc<Mutex<FixtureState>>,
    thread: std::thread::JoinHandle<()>,
}

impl Fixture {
    fn start() -> Self {
        Self::start_expecting(5)
    }

    /// The fixture returns once `expected` requests have arrived and the socket
    /// goes idle, so a flow that makes fewer requests than another does not have
    /// to wait out the deadline to prove it.
    fn start_expecting(expected: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind HTTP fixture");
        listener
            .set_nonblocking(true)
            .expect("make HTTP fixture nonblocking");
        let endpoint = format!("http://{}", listener.local_addr().expect("fixture address"));
        let state = Arc::new(Mutex::new(FixtureState::default()));
        let server_state = Arc::clone(&state);
        let thread = std::thread::spawn(move || {
            let ca = TestCa::new();
            let deadline = Instant::now() + Duration::from_secs(20);
            while Instant::now() < deadline {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        stream
                            .set_nonblocking(false)
                            .expect("make accepted fixture connection blocking");
                        serve_one(&mut stream, &server_state, &ca);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        if server_state
                            .lock()
                            .expect("lock fixture state")
                            .requests
                            .len()
                            >= expected
                        {
                            return;
                        }
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("accept HTTP fixture request: {error}"),
                }
            }
            panic!("HTTP fixture did not receive all expected requests within 20 seconds");
        });
        Self {
            endpoint,
            state,
            thread,
        }
    }

    fn finish(self) -> Arc<Mutex<FixtureState>> {
        self.thread.join().expect("HTTP fixture completed");
        self.state
    }
}

fn serve_one(stream: &mut TcpStream, state: &Arc<Mutex<FixtureState>>, ca: &TestCa) {
    let request = read_request(stream);
    let mut state = state.lock().expect("lock fixture state");
    let response = match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/api/spice-cli/auth") => Some((
            200,
            serde_json::json!({
                "username": "fixture-user",
                "email": "fixture@example.invalid",
                "org": {"name": "acme"},
                "app": null
            })
            .to_string(),
        )),
        ("GET", "/v1/orgs") => Some((
            200,
            serde_json::json!({
                "orgs": [{"id": 42, "name": "acme", "display_name": "Acme", "role": "owner"}]
            })
            .to_string(),
        )),
        ("POST", "/v1/cloud-connect/enroll") => {
            assert_eq!(
                request.headers.get("authorization").map(String::as_str),
                Some("Bearer fixture-login-token")
            );
            assert_eq!(
                request.headers.get("x-org-name").map(String::as_str),
                Some("acme")
            );
            let operation = request
                .headers
                .get("idempotency-key")
                .expect("enrollment operation header")
                .clone();
            uuid::Uuid::parse_str(&operation).expect("canonical enrollment operation UUID");
            let body: serde_json::Value =
                serde_json::from_str(&request.body).expect("authenticated enrollment body JSON");
            let object = body.as_object().expect("enrollment body is an object");
            let mut keys = object.keys().map(String::as_str).collect::<Vec<_>>();
            keys.sort_unstable();
            assert_eq!(
                keys,
                ["csr_pem", "enc_pubkey_pem", "instance", "kind", "region"]
            );
            assert_eq!(body["kind"], "standalone");
            assert_eq!(body["region"], "lab-seoul");
            for field in ["fingerprint", "hostname", "os", "arch", "runtime_version"] {
                assert!(
                    body["instance"][field]
                        .as_str()
                        .is_some_and(|value| !value.is_empty()),
                    "missing canonical instance fact {field}"
                );
            }
            state.enrollment_operations.push(operation);
            state.enrollment_bodies.push(request.body.clone());
            if state.instance_mutations == 0 {
                state.instance_mutations = 1;
                state.enrollment_response = Some(
                    serde_json::json!({
                        "kind": "standalone",
                        "instance_id": INSTANCE_ID,
                        "identity_cert_pem": ca.sign_csr(
                            body["csr_pem"].as_str().expect("enrollment CSR")
                        ),
                        "ca_bundle_pem": ca.cert_pem.clone(),
                        "gateway_addr": "127.0.0.1:443",
                        "not_after": NOT_AFTER_RFC3339,
                        "organization": {"id": 42, "name": "acme"},
                        "region": "lab-seoul",
                        "portal": {"new_project_url": "https://spice.ai/acme/new?instance=inst_fixture_1401"},
                        "attachment": null,
                        "recovered": true
                    })
                    .to_string(),
                );
                // Commit the mutation, then lose the response. The driver's
                // bounded retry must replay the exact operation and body.
                None
            } else {
                Some((
                    200,
                    state
                        .enrollment_response
                        .clone()
                        .expect("committed enrollment response"),
                ))
            }
        }
        ("POST", "/v1/cloud-connect/project") => {
            assert_eq!(
                request.headers.get("authorization").map(String::as_str),
                Some("Bearer fixture-login-token")
            );
            assert_eq!(
                request.headers.get("x-org-name").map(String::as_str),
                Some("acme")
            );
            state.project_bodies.push(request.body.clone());
            if state.project_mutations == 0 {
                state.project_mutations = 1;
                // The atomic endpoint committed; its first response is lost.
                None
            } else {
                Some((
                    200,
                    serde_json::json!({
                        "instance_id": INSTANCE_ID,
                        "organization": {"id": 42, "name": "acme"},
                        "project": {"id": 314, "name": PROJECT_NAME},
                        "monitor_url": "https://spice.ai/acme/fault-replay/monitor"
                    })
                    .to_string(),
                ))
            }
        }
        route => panic!("unexpected fixture route: {route:?}"),
    };
    state.requests.push(request);
    drop(state);
    if let Some((status, body)) = response {
        write_response(stream, status, &body);
    }
}

fn read_request(stream: &mut TcpStream) -> Request {
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set fixture read timeout");
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 4096];
    loop {
        let count = stream.read(&mut chunk).expect("read HTTP fixture request");
        assert_ne!(count, 0, "request closed before its body arrived");
        bytes.extend_from_slice(&chunk[..count]);
        let text = String::from_utf8_lossy(&bytes);
        let Some(header_end) = text.find("\r\n\r\n") else {
            continue;
        };
        let content_length = text[..header_end]
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().expect("content length"))
            })
            .unwrap_or_default();
        if bytes.len() >= header_end + 4 + content_length {
            break;
        }
    }

    let text = String::from_utf8(bytes).expect("HTTP fixture request is utf8");
    let (head, body) = text.split_once("\r\n\r\n").expect("HTTP separator");
    let mut lines = head.lines();
    let request_line = lines.next().expect("HTTP request line");
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().expect("HTTP method").to_string();
    let path = request_parts.next().expect("HTTP path").to_string();
    let headers = lines
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            Some((name.to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect();
    Request {
        method,
        path,
        headers,
        body: body.to_string(),
    }
}

fn write_response(stream: &mut TcpStream, status: u16, body: &str) {
    let reason = if status == 200 { "OK" } else { "Created" };
    write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
    .expect("write HTTP fixture response");
}

fn connect_command(instance: &TempDir, home: &TempDir, endpoint: &str, project: &str) -> Command {
    stub_runtime(home);
    let mut command = spice_cmd();
    command
        .current_dir(instance.path())
        .env("HOME", home.path())
        .env("SPICE_SPICEAI_TOKEN", LOGIN_TOKEN)
        .env("SPICE_SPICEAI_TOKEN_ACME", LOGIN_TOKEN)
        .env_remove("SPICE_CONFIG_DIR")
        .env_remove("SPICE_CLOUD_ENDPOINT")
        .arg("connect")
        .arg("--org")
        .arg("acme")
        .arg("--project")
        .arg(project)
        .arg("--region")
        .arg("lab-seoul")
        .arg("--endpoint")
        .arg(endpoint);
    command
}

fn unattached_identity() -> runtime_cloud_connect::Identity {
    let ca = TestCa::new();
    let key = KeyPair::generate().expect("generate existing identity key");
    let mut params = CertificateParams::default();
    params
        .distinguished_name
        .push(DnType::CommonName, "Existing Spice Connect Identity");
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    params.not_after = time::OffsetDateTime::from_unix_timestamp(
        i64::try_from(NOT_AFTER_UNIX).expect("fixture expiry fits i64"),
    )
    .expect("valid fixture expiry");
    let certificate = params
        .signed_by(&key, &ca.issuer)
        .expect("sign existing identity certificate")
        .pem();
    runtime_cloud_connect::Identity {
        identifier: INSTANCE_ID.to_string(),
        identity_cert_pem: certificate,
        private_key_pem: key.serialize_pem(),
        public_key_pem: key.public_key_pem(),
        ca_bundle_pem: ca.cert_pem,
        gateway_addr: "127.0.0.1:443".to_string(),
        not_after_unix: Some(NOT_AFTER_UNIX),
        enc_private_key_pem: String::new(),
        enc_public_key_pem: String::new(),
        enc_previous_private_key_pem: String::new(),
        cache_key_b64: String::new(),
        app_id: None,
        org_name: Some("acme".to_string()),
        app_name: None,
        monitor_url: None,
        new_project_url: None,
        control_plane_endpoint: None,
    }
}

#[test]
fn corrupted_existing_identity_fails_before_project_mutation() {
    let instance = TempDir::new().expect("create instance directory");
    let home = TempDir::new().expect("create isolated home");
    let config_dir = instance.path().join(".spice");
    let mut identity = unattached_identity();
    identity.private_key_pem = "not-a-private-key".to_string();
    runtime_cloud_connect::IdentityStore::store(&config_dir.join("identity.json"), &identity)
        .expect("store corrupted identity fixture");

    let output = connect_command(&instance, &home, "http://127.0.0.1:9", PROJECT_NAME)
        .output()
        .expect("run connect with corrupted identity");
    assert!(!output.status.success());
    let rendered = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(rendered.contains("identity") && rendered.contains("unusable"));
    assert!(!config_dir.join("connect-project-operation.json").exists());
    assert!(!config_dir.join("cloud-endpoint").exists());
}

#[test]
fn existing_unattached_identity_reaches_project_without_auth_context() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind recovery fixture");
    let endpoint = format!(
        "http://{}",
        listener.local_addr().expect("recovery fixture address")
    );
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept recovery request");
        let request = read_request(&mut stream);
        if request.path == "/api/spice-cli/auth" {
            write_response(&mut stream, 404, "{}");
            return request;
        }
        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/v1/cloud-connect/project");
        assert_eq!(
            request.headers.get("authorization").map(String::as_str),
            Some("Bearer fixture-login-token")
        );
        assert_eq!(
            request.headers.get("x-org-name").map(String::as_str),
            Some("acme")
        );
        let body: serde_json::Value =
            serde_json::from_str(&request.body).expect("project recovery body JSON");
        assert_eq!(body["instance_id"], INSTANCE_ID);
        assert_eq!(body["name"], PROJECT_NAME);
        write_response(
            &mut stream,
            201,
            &serde_json::json!({
                "instance_id": INSTANCE_ID,
                "organization": {"id": 42, "name": "acme"},
                "project": {"id": 314, "name": PROJECT_NAME},
                "monitor_url": "https://spice.ai/acme/fault-replay/monitor"
            })
            .to_string(),
        );
        request
    });

    let instance = TempDir::new().expect("create instance directory");
    let home = TempDir::new().expect("create isolated home");
    let config_dir = instance.path().join(".spice");
    runtime_cloud_connect::IdentityStore::store(
        &config_dir.join("identity.json"),
        &unattached_identity(),
    )
    .expect("store enrolled unattached identity");

    connect_command(&instance, &home, &endpoint, PROJECT_NAME)
        .assert()
        .success();

    let request = server.join().expect("recovery fixture completed");
    assert_eq!(
        request.path, "/v1/cloud-connect/project",
        "the project operation must not depend on legacy auth context"
    );
    let identity =
        runtime_cloud_connect::IdentityStore::load_optional(&config_dir.join("identity.json"))
            .expect("load recovered identity")
            .expect("recovered identity exists");
    assert_eq!(identity.app_id.as_deref(), Some("314"));
    assert_eq!(identity.app_name.as_deref(), Some(PROJECT_NAME));
    assert_eq!(
        std::fs::read_to_string(config_dir.join("cloud-endpoint"))
            .expect("custom endpoint is persisted")
            .trim(),
        endpoint
    );
}

#[test]
fn a_default_credential_for_another_org_never_reaches_a_mutation() {
    for existing_identity in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind credential fixture");
        let endpoint = format!(
            "http://{}",
            listener.local_addr().expect("credential fixture address")
        );
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept credential probe");
            let request = read_request(&mut stream);
            assert_eq!(request.method, "GET");
            assert_eq!(request.path, "/api/spice-cli/auth");
            write_response(
                &mut stream,
                200,
                &serde_json::json!({
                    "username": "fixture-user",
                    "email": "fixture@example.invalid",
                    "org": {"name": "globex"},
                    "app": null
                })
                .to_string(),
            );
            request
        });

        let instance = TempDir::new().expect("create instance directory");
        let home = TempDir::new().expect("create isolated home");
        let config_dir = instance.path().join(".spice");
        if existing_identity {
            runtime_cloud_connect::IdentityStore::store(
                &config_dir.join("identity.json"),
                &unattached_identity(),
            )
            .expect("store enrolled unattached identity");
        }

        stub_runtime(&home);
        let output = spice_cmd()
            .current_dir(instance.path())
            .env("HOME", home.path())
            .env("SPICE_SPICEAI_TOKEN", LOGIN_TOKEN)
            .env_remove("SPICE_SPICEAI_TOKEN_ACME")
            .env_remove("SPICE_CONFIG_DIR")
            .env_remove("SPICE_CLOUD_ENDPOINT")
            .arg("connect")
            .arg("--org")
            .arg("acme")
            .arg("--project")
            .arg(PROJECT_NAME)
            .arg("--endpoint")
            .arg(&endpoint)
            .output()
            .expect("run connect with mismatched default credential");
        assert_eq!(output.status.code(), Some(4));
        let rendered = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(rendered.contains("globex"), "unexpected error: {rendered}");
        assert!(rendered.contains("acme"), "unexpected error: {rendered}");
        assert!(
            !config_dir.join("connect-operation.json").exists(),
            "enrollment mutation journal must not be created"
        );
        assert!(
            !config_dir.join("connect-project-operation.json").exists(),
            "project mutation journal must not be created"
        );

        let request = server.join().expect("credential fixture completed");
        assert_eq!(request.path, "/api/spice-cli/auth");
    }
}

#[test]
fn project_name_conflict_keeps_the_existing_identity_unattached() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind conflict fixture");
    let endpoint = format!(
        "http://{}",
        listener.local_addr().expect("conflict fixture address")
    );
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept conflict request");
        let request = read_request(&mut stream);
        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/v1/cloud-connect/project");
        let body: serde_json::Value =
            serde_json::from_str(&request.body).expect("project conflict body JSON");
        assert_eq!(body["instance_id"], INSTANCE_ID);
        assert_eq!(body["name"], PROJECT_NAME);
        write_response(
            &mut stream,
            409,
            &serde_json::json!({
                "code": "project_name_conflict",
                "retryable": false
            })
            .to_string(),
        );
    });

    let instance = TempDir::new().expect("create instance directory");
    let home = TempDir::new().expect("create isolated home");
    let config_dir = instance.path().join(".spice");
    runtime_cloud_connect::IdentityStore::store(
        &config_dir.join("identity.json"),
        &unattached_identity(),
    )
    .expect("store enrolled unattached identity");

    let output = connect_command(&instance, &home, &endpoint, PROJECT_NAME)
        .output()
        .expect("run conflicting project assignment");
    assert!(!output.status.success());
    let rendered = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        rendered.contains("project_name_conflict"),
        "unexpected conflict output: {rendered}"
    );
    server.join().expect("conflict fixture completed");

    let identity =
        runtime_cloud_connect::IdentityStore::load_optional(&config_dir.join("identity.json"))
            .expect("load identity after conflict")
            .expect("identity remains durable");
    assert_eq!(identity.app_id, None);
    assert_eq!(identity.app_name, None);
}

#[test]
fn remote_attachment_conflict_never_reports_the_instance_as_unattached() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind attachment fixture");
    let endpoint = format!(
        "http://{}",
        listener.local_addr().expect("attachment fixture address")
    );
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept attachment request");
        let request = read_request(&mut stream);
        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/v1/cloud-connect/project");
        write_response(
            &mut stream,
            409,
            &serde_json::json!({
                "code": "instance_already_attached",
                "retryable": false
            })
            .to_string(),
        );
    });

    let instance = TempDir::new().expect("create instance directory");
    let home = TempDir::new().expect("create isolated home");
    let config_dir = instance.path().join(".spice");
    runtime_cloud_connect::IdentityStore::store(
        &config_dir.join("identity.json"),
        &unattached_identity(),
    )
    .expect("store locally unattached identity");

    let output = connect_command(&instance, &home, &endpoint, PROJECT_NAME)
        .output()
        .expect("run remote attachment conflict");
    assert!(!output.status.success());
    let rendered = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        rendered.contains("instance_already_attached"),
        "unexpected attachment conflict output: {rendered}"
    );
    assert!(
        !rendered.contains("not yet attached"),
        "remote attachment was reported as unattached: {rendered}"
    );
    server.join().expect("attachment fixture completed");
}

#[test]
fn ambiguous_committed_project_response_is_replayed_and_never_reports_unattached() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ambiguous fixture");
    let endpoint = format!(
        "http://{}",
        listener.local_addr().expect("ambiguous fixture address")
    );
    let server = std::thread::spawn(move || {
        let mut bodies = Vec::new();
        for attempt in 0..6 {
            let (mut stream, _) = listener.accept().expect("accept ambiguous request");
            let request = read_request(&mut stream);
            assert_eq!(request.path, "/v1/cloud-connect/project");
            bodies.push(request.body);
            // A 2xx proves only that a response arrived; an invalid body cannot
            // prove whether the atomic server mutation committed.
            if attempt < 5 {
                write_response(&mut stream, 200, "{}");
            } else {
                write_response(
                    &mut stream,
                    200,
                    &serde_json::json!({
                        "instance_id": INSTANCE_ID,
                        "organization": {"id": 42, "name": "acme"},
                        "project": {"id": 314, "name": PROJECT_NAME},
                        "monitor_url": "https://spice.ai/acme/fault-replay/monitor"
                    })
                    .to_string(),
                );
            }
        }
        bodies
    });

    let instance = TempDir::new().expect("create instance directory");
    let home = TempDir::new().expect("create isolated home");
    let config_dir = instance.path().join(".spice");
    runtime_cloud_connect::IdentityStore::store(
        &config_dir.join("identity.json"),
        &unattached_identity(),
    )
    .expect("store locally unattached identity");

    let output = connect_command(&instance, &home, &endpoint, PROJECT_NAME)
        .output()
        .expect("run ambiguous project assignment");
    assert!(!output.status.success());
    let rendered = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        rendered.contains("attachment result is unknown"),
        "ambiguous result was not explicit: {rendered}"
    );
    assert!(
        !rendered.contains("not yet attached"),
        "ambiguous commit was reported as unattached: {rendered}"
    );
    assert!(
        config_dir.join("connect-project-operation.json").exists(),
        "an ambiguous process exit must retain exact project replay state"
    );
    connect_command(&instance, &home, &endpoint, PROJECT_NAME)
        .assert()
        .success();
    let bodies = server.join().expect("ambiguous fixture completed");
    assert_eq!(bodies.len(), 6);
    assert!(
        bodies.windows(2).all(|pair| pair[0] == pair[1]),
        "every recovery attempt must replay the exact request"
    );
    assert!(
        !config_dir.join("connect-project-operation.json").exists(),
        "durable local attachment must retire project replay state"
    );
    let identity =
        runtime_cloud_connect::IdentityStore::load_optional(&config_dir.join("identity.json"))
            .expect("load recovered attachment")
            .expect("identity remains");
    assert_eq!(identity.app_name.as_deref(), Some(PROJECT_NAME));
}

#[test]
fn response_loss_replays_exact_mutations_without_duplicates() {
    let fixture = Fixture::start();
    let instance = TempDir::new().expect("create instance directory");
    let home = TempDir::new().expect("create isolated home");

    connect_command(&instance, &home, &fixture.endpoint, PROJECT_NAME)
        .assert()
        .success();

    let config_dir = instance.path().join(".spice");
    let first_identity =
        runtime_cloud_connect::IdentityStore::load_optional(&config_dir.join("identity.json"))
            .expect("load identity after response-loss recovery")
            .expect("enrollment and project attachment are durable");
    assert_eq!(first_identity.identifier, INSTANCE_ID);
    assert_eq!(first_identity.app_id.as_deref(), Some("314"));
    assert_eq!(first_identity.org_name.as_deref(), Some("acme"));
    assert!(
        !config_dir.join("connect-operation.json").exists(),
        "a durable identity retires the enrollment journal"
    );

    connect_command(&instance, &home, &fixture.endpoint, PROJECT_NAME)
        .assert()
        .success();

    let conflict = connect_command(&instance, &home, &fixture.endpoint, "different-project")
        .output()
        .expect("run attached identity with conflicting project assertion");
    assert!(!conflict.status.success());
    let conflict_output = format!(
        "{}{}",
        String::from_utf8_lossy(&conflict.stdout),
        String::from_utf8_lossy(&conflict.stderr)
    );
    assert!(
        conflict_output.contains("already attached to project fault-replay"),
        "unexpected conflict error: {conflict_output}"
    );

    let identity =
        runtime_cloud_connect::IdentityStore::load_optional(&config_dir.join("identity.json"))
            .expect("load attached identity")
            .expect("attached identity exists");
    assert_eq!(identity.identifier, INSTANCE_ID);
    assert_eq!(identity.app_id.as_deref(), Some("314"));
    assert_eq!(identity.app_name.as_deref(), Some(PROJECT_NAME));
    assert_eq!(
        identity.monitor_url.as_deref(),
        Some("https://spice.ai/acme/fault-replay/monitor")
    );

    let state = fixture.finish();
    let state = state.lock().expect("lock final fixture state");
    assert_eq!(state.instance_mutations, 1, "instance must be created once");
    assert_eq!(state.project_mutations, 1, "project must be created once");
    assert_eq!(state.enrollment_bodies.len(), 2, "enrollment was replayed");
    assert_eq!(state.enrollment_bodies[0], state.enrollment_bodies[1]);
    assert_eq!(
        state.enrollment_operations[0], state.enrollment_operations[1],
        "replay must carry the same idempotency key"
    );
    assert_eq!(
        state.project_bodies.len(),
        2,
        "project request was replayed"
    );
    assert_eq!(state.project_bodies[0], state.project_bodies[1]);
    let project: serde_json::Value =
        serde_json::from_str(&state.project_bodies[0]).expect("project body JSON");
    assert_eq!(project["instance_id"], INSTANCE_ID);
    assert_eq!(project["name"], PROJECT_NAME);
}

#[test]
fn token_mode_stays_unattached_and_existing_identity_prevents_reenrollment() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind token fixture");
    let endpoint = format!("http://{}", listener.local_addr().expect("fixture address"));
    let server = std::thread::spawn(move || {
        let ca = TestCa::new();
        let (mut stream, _) = listener.accept().expect("accept token enrollment");
        let request = read_request(&mut stream);
        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/v1/cloud-connect/enroll");
        assert!(!request.headers.contains_key("authorization"));
        assert!(!request.headers.contains_key("x-org-name"));
        uuid::Uuid::parse_str(
            request
                .headers
                .get("idempotency-key")
                .expect("token enrollment operation header"),
        )
        .expect("canonical enrollment operation UUID");
        let body: serde_json::Value =
            serde_json::from_str(&request.body).expect("token enrollment body JSON");
        let object = body
            .as_object()
            .expect("token enrollment body is an object");
        let mut keys = object.keys().map(String::as_str).collect::<Vec<_>>();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "csr_pem",
                "enc_pubkey_pem",
                "expected_org",
                "instance",
                "kind",
                "token"
            ]
        );
        assert_eq!(body["kind"], "standalone");
        assert_eq!(body["expected_org"], "acme");
        assert_eq!(
            body["token"],
            "spice-enroll-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        );
        let identity_cert_pem =
            ca.sign_csr(body["csr_pem"].as_str().expect("token enrollment CSR"));
        write_response(
            &mut stream,
            200,
            &serde_json::json!({
                "kind": "standalone",
                "instance_id": "inst_token_fixture",
                "identity_cert_pem": identity_cert_pem,
                "ca_bundle_pem": ca.cert_pem,
                "gateway_addr": "127.0.0.1:443",
                "not_after": NOT_AFTER_RFC3339,
                "organization": {"id": 42, "name": "acme"},
                "region": "us-east-1",
                "portal": {"new_project_url": "https://spice.ai/acme/new?instance=inst_token_fixture"},
                "attachment": null,
                "recovered": false
            })
            .to_string(),
        );
    });

    let instance = TempDir::new().expect("create instance directory");
    let home = TempDir::new().expect("create isolated home");
    stub_runtime(&home);
    let first = spice_cmd()
        .current_dir(instance.path())
        .env("HOME", home.path())
        .env_remove("SPICE_CONFIG_DIR")
        .env_remove("SPICE_SPICEAI_TOKEN")
        .arg("connect")
        .arg("--token")
        .arg("spice-enroll-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
        .arg("--org")
        .arg("acme")
        .arg("--endpoint")
        .arg(&endpoint)
        .output()
        .expect("run token enrollment");
    assert!(
        first.status.success(),
        "token enrollment failed: {}{}",
        String::from_utf8_lossy(&first.stdout),
        String::from_utf8_lossy(&first.stderr)
    );
    let first_output = String::from_utf8_lossy(&first.stdout);
    assert!(first_output.contains("not yet attached to a project"));
    assert!(first_output.contains("https://spice.ai/acme/new?instance=inst_token_fixture"));
    server.join().expect("token fixture completed");

    let config_dir = instance.path().join(".spice");
    let identity =
        runtime_cloud_connect::IdentityStore::load_optional(&config_dir.join("identity.json"))
            .expect("load token identity")
            .expect("token identity exists");
    assert_eq!(identity.app_id, None);
    assert_eq!(identity.app_name, None);

    // The fixture is gone. Success proves the existing identity won before
    // the fresh token could be redeemed or any HTTP mutation was attempted.
    spice_cmd()
        .current_dir(instance.path())
        .env("HOME", home.path())
        .env_remove("SPICE_CONFIG_DIR")
        .env_remove("SPICE_SPICEAI_TOKEN")
        .arg("connect")
        .arg("--token")
        .arg("spice-enroll-BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
        .arg("--org")
        .arg("acme")
        .arg("--endpoint")
        .arg(&endpoint)
        .assert()
        .success();
}

/// A pending operation's own state is published the way the runtime publishes
/// it, so these exercises resume exactly what a lost response leaves behind.
fn publish_pending_draft(
    config_dir: &std::path::Path,
    endpoint: &str,
    authority: runtime_cloud_connect::EnrollmentAuthorityBinding,
) -> runtime_cloud_connect::EnrollmentDraft {
    runtime_cloud_connect::EnrollmentDraft::load_or_create(
        config_dir,
        &runtime_cloud_connect::enroll::InstanceFacts::gather("v0.0.0-integration"),
        Some("lab-seoul"),
        &runtime_cloud_connect::EnrollmentRequestBinding {
            endpoint: endpoint.to_string(),
            authority,
        },
    )
    .expect("publish a pending enrollment draft")
}

/// A loopback address nothing listens on: a command that reaches the control
/// plane fails visibly differently from one that fails on its own inputs.
fn closed_endpoint() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve a loopback port");
    let address = listener.local_addr().expect("reserved port");
    drop(listener);
    format!("http://{address}")
}

fn connect_at(instance: &TempDir, home: &TempDir, endpoint: &str) -> Command {
    // A completed transaction ends at a running instance, so these exercises
    // need the same exit-immediately runtime `connect_command` installs — the
    // subject here is what the transaction resumed, not what it started.
    stub_runtime(home);
    let mut command = spice_cmd();
    command
        .current_dir(instance.path())
        .env("HOME", home.path())
        .env_remove("SPICE_CONFIG_DIR")
        .env_remove("SPICE_CLOUD_ENDPOINT")
        .env_remove("SPICE_SPICEAI_TOKEN")
        .arg("connect")
        .arg("--endpoint")
        .arg(endpoint);
    command
}

fn output_of(command: &mut Command) -> (bool, String) {
    let output = command.output().expect("run spice connect");
    (
        output.status.success(),
        format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ),
    )
}

#[test]
fn a_pending_token_draft_replays_its_operation_and_organization_assertion() {
    let instance = TempDir::new().expect("create instance directory");
    let home = TempDir::new().expect("create isolated home");
    let config_dir = instance.path().join(".spice");

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind resume fixture");
    let endpoint = format!(
        "http://{}",
        listener.local_addr().expect("resume fixture address")
    );
    let draft = publish_pending_draft(
        &config_dir,
        &endpoint,
        runtime_cloud_connect::EnrollmentAuthorityBinding::Token {
            expected_org: Some("acme".to_string()),
        },
    );

    let operation = draft.enrollment_operation_id;
    let csr = draft.csr_pem;
    let server = std::thread::spawn(move || {
        let ca = TestCa::new();
        let (mut stream, _) = listener.accept().expect("accept resumed enrollment");
        let request = read_request(&mut stream);
        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/v1/cloud-connect/enroll");
        assert!(!request.headers.contains_key("authorization"));
        assert_eq!(
            request.headers.get("idempotency-key"),
            Some(&operation),
            "a resumed operation must carry the durable operation ID"
        );
        let body: serde_json::Value =
            serde_json::from_str(&request.body).expect("resumed enrollment body JSON");
        assert_eq!(
            body["csr_pem"].as_str(),
            Some(csr.as_str()),
            "a resumed operation must replay the provisional key material"
        );
        assert_eq!(
            body["expected_org"], "acme",
            "the draft's organization assertion survives a run that never named one"
        );
        assert_eq!(
            body["region"], "lab-seoul",
            "the draft's declared location survives a run that never named one"
        );
        let identity_cert_pem = ca.sign_csr(csr.as_str());
        write_response(
            &mut stream,
            200,
            &serde_json::json!({
                "kind": "standalone",
                "instance_id": "inst_resumed_fixture",
                "identity_cert_pem": identity_cert_pem,
                "ca_bundle_pem": ca.cert_pem,
                "gateway_addr": "127.0.0.1:443",
                "not_after": NOT_AFTER_RFC3339,
                "organization": {"id": 42, "name": "acme"},
                "region": "lab-seoul",
                "portal": {"new_project_url": "https://spice.ai/acme/new?instance=inst_resumed_fixture"},
                "attachment": null,
                "recovered": true
            })
            .to_string(),
        );
    });

    // No --org and no --region: everything the replay needs comes from the
    // draft, which is the only place it was ever durable.
    let (success, output) = output_of(
        connect_at(&instance, &home, &endpoint)
            .arg("--token")
            .arg("spice-enroll-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
    );
    assert!(success, "resumed token enrollment failed: {output}");
    server.join().expect("resume fixture completed");

    let identity =
        runtime_cloud_connect::IdentityStore::load_optional(&config_dir.join("identity.json"))
            .expect("load resumed identity")
            .expect("the resumed enrollment is durable");
    assert_eq!(identity.identifier, "inst_resumed_fixture");
    assert_eq!(identity.org_name.as_deref(), Some("acme"));
    assert_eq!(
        identity.new_project_url.as_deref(),
        Some("https://spice.ai/acme/new?instance=inst_resumed_fixture"),
        "the Cloud's create-project page is persisted so any later start can offer it"
    );
    assert!(
        runtime_cloud_connect::EnrollmentDraft::load_optional(&config_dir)
            .expect("read the draft state")
            .is_none(),
        "a promoted identity retires the draft"
    );
}

#[test]
fn a_pending_token_draft_names_the_key_flag_without_a_terminal() {
    let instance = TempDir::new().expect("create instance directory");
    let home = TempDir::new().expect("create isolated home");
    let config_dir = instance.path().join(".spice");
    let endpoint = closed_endpoint();
    let draft = publish_pending_draft(
        &config_dir,
        &endpoint,
        runtime_cloud_connect::EnrollmentAuthorityBinding::Token {
            expected_org: Some("acme".to_string()),
        },
    );

    let (success, output) = output_of(&mut connect_at(&instance, &home, &endpoint));
    assert!(!success, "a key cannot be prompted for without a terminal");
    assert!(
        output.contains("pending enrollment that was authorized by an enrollment key")
            && output.contains("--token <enrollment-key>"),
        "the failure must name the pending operation and what finishes it: {output}"
    );

    let preserved = runtime_cloud_connect::EnrollmentDraft::load_optional(&config_dir)
        .expect("read the preserved draft")
        .expect("a refused run keeps the retry-safe state");
    assert_eq!(
        preserved.enrollment_operation_id, draft.enrollment_operation_id,
        "an actionable failure must not discard the pending operation"
    );
}

#[test]
fn a_pending_login_draft_refuses_an_enrollment_key() {
    let instance = TempDir::new().expect("create instance directory");
    let home = TempDir::new().expect("create isolated home");
    let config_dir = instance.path().join(".spice");
    let endpoint = closed_endpoint();
    let draft = publish_pending_draft(
        &config_dir,
        &endpoint,
        runtime_cloud_connect::EnrollmentAuthorityBinding::AuthenticatedSession {
            organization: "acme".to_string(),
        },
    );

    let (success, output) = output_of(
        connect_at(&instance, &home, &endpoint)
            .arg("--token")
            .arg("spice-enroll-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
    );
    assert!(
        !success,
        "an enrollment key must not replace the pending login authority"
    );
    assert!(
        output.contains("spice connect --project <name>")
            && output.contains("spice connect remove --yes"),
        "the failure must name what finishes the pending operation, and what abandons it: {output}"
    );

    let preserved = runtime_cloud_connect::EnrollmentDraft::load_optional(&config_dir)
        .expect("read the preserved draft")
        .expect("a refused run keeps the retry-safe state");
    assert_eq!(
        preserved.enrollment_operation_id, draft.enrollment_operation_id,
        "a rejected authority must not discard the pending operation"
    );
    assert_eq!(preserved.binding, draft.binding);
}

#[test]
fn a_pending_login_draft_resumes_its_organization_without_the_org_flag() {
    let fixture = Fixture::start_expecting(4);
    let instance = TempDir::new().expect("create instance directory");
    let home = TempDir::new().expect("create isolated home");
    let config_dir = instance.path().join(".spice");
    let draft = publish_pending_draft(
        &config_dir,
        &fixture.endpoint,
        runtime_cloud_connect::EnrollmentAuthorityBinding::AuthenticatedSession {
            organization: "acme".to_string(),
        },
    );

    // No --org: the pending operation is already bound to one, and a resumed
    // run must not need the flag — nor let anything else choose.
    let (success, output) = output_of(
        connect_at(&instance, &home, &fixture.endpoint)
            .env("SPICE_SPICEAI_TOKEN", LOGIN_TOKEN)
            .env("SPICE_SPICEAI_TOKEN_ACME", LOGIN_TOKEN)
            .arg("--project")
            .arg(PROJECT_NAME),
    );
    assert!(success, "resumed login enrollment failed: {output}");

    let state = fixture.finish();
    let state = state.lock().expect("lock final fixture state");
    assert_eq!(state.instance_mutations, 1, "instance must be created once");
    assert_eq!(state.project_mutations, 1, "project must be created once");
    assert!(
        state
            .enrollment_operations
            .iter()
            .all(|operation| *operation == draft.enrollment_operation_id),
        "every attempt must carry the pending operation ID"
    );
    assert!(
        state
            .requests
            .iter()
            .all(|request| request.path.starts_with("/v1/cloud-connect/")),
        "a bound organization needs no discovery: {:?}",
        state.requests.iter().map(|r| &r.path).collect::<Vec<_>>()
    );
    let body: serde_json::Value =
        serde_json::from_str(&state.enrollment_bodies[0]).expect("enrollment body JSON");
    assert_eq!(
        body["csr_pem"].as_str(),
        Some(draft.csr_pem.as_str()),
        "a resumed operation must replay the provisional key material"
    );

    let identity =
        runtime_cloud_connect::IdentityStore::load_optional(&config_dir.join("identity.json"))
            .expect("load resumed identity")
            .expect("the resumed enrollment is durable");
    assert_eq!(identity.identifier, INSTANCE_ID);
    assert_eq!(identity.org_name.as_deref(), Some("acme"));
    assert_eq!(identity.app_name.as_deref(), Some(PROJECT_NAME));
    assert!(
        runtime_cloud_connect::EnrollmentDraft::load_optional(&config_dir)
            .expect("read the draft state")
            .is_none(),
        "a promoted identity retires the draft"
    );
}

#[test]
fn a_directory_with_nothing_pending_still_requires_explicit_authority() {
    let instance = TempDir::new().expect("create instance directory");
    let home = TempDir::new().expect("create isolated home");
    let endpoint = closed_endpoint();

    let (success, output) = output_of(&mut connect_at(&instance, &home, &endpoint));
    assert!(
        !success,
        "a fresh directory cannot enroll without authority"
    );
    assert!(
        output.contains(
            "non-interactive Cloud Connect requires either a login with --org <org> --project <name>, or --token <enrollment-key>"
        ),
        "the no-draft failure must stay unchanged: {output}"
    );
    assert!(
        !instance
            .path()
            .join(".spice")
            .join("identity.json")
            .exists(),
        "a refused run enrolls nothing"
    );
}

/// A stored login credential used to short-circuit the authentication choice
/// before the draft was consulted, which sent `GET /api/spice-cli/auth` — a
/// portal route — to the pending operation's control plane. The draft decides
/// first now, so a key operation never identifies a stored credential at all.
#[test]
fn a_pending_token_draft_never_probes_a_stored_login_credential() {
    let instance = TempDir::new().expect("create instance directory");
    let home = TempDir::new().expect("create isolated home");
    let config_dir = instance.path().join(".spice");

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind resume fixture");
    let endpoint = format!(
        "http://{}",
        listener.local_addr().expect("resume fixture address")
    );
    let draft = publish_pending_draft(
        &config_dir,
        &endpoint,
        runtime_cloud_connect::EnrollmentAuthorityBinding::Token {
            expected_org: Some("acme".to_string()),
        },
    );

    let csr = draft.csr_pem;
    let server = std::thread::spawn(move || {
        let ca = TestCa::new();
        let (mut stream, _) = listener.accept().expect("accept resumed enrollment");
        let request = read_request(&mut stream);
        let identity_cert_pem = ca.sign_csr(csr.as_str());
        write_response(
            &mut stream,
            200,
            &serde_json::json!({
                "kind": "standalone",
                "instance_id": "inst_resumed_fixture",
                "identity_cert_pem": identity_cert_pem,
                "ca_bundle_pem": ca.cert_pem,
                "gateway_addr": "127.0.0.1:443",
                "not_after": NOT_AFTER_RFC3339,
                "organization": {"id": 42, "name": "acme"},
                "region": "lab-seoul",
                "portal": {"new_project_url": "https://spice.ai/acme/new?instance=inst_resumed_fixture"},
                "attachment": null,
                "recovered": true
            })
            .to_string(),
        );
        request
    });

    // A stored default credential exists, and both the portal and the default
    // API origin are dead: any identification probe fails visibly instead of
    // being answered by the control-plane fixture.
    let (success, output) = output_of(
        connect_at(&instance, &home, &endpoint)
            .env("SPICE_SPICEAI_TOKEN", LOGIN_TOKEN)
            .env("SPICE_BASE_URL", closed_endpoint())
            .env("SPICE_CLOUD_API_URL", closed_endpoint())
            .arg("--token")
            .arg("spice-enroll-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
    );
    assert!(success, "resumed token enrollment failed: {output}");

    let request = server.join().expect("resume fixture completed");
    assert_eq!(
        request.path, "/v1/cloud-connect/enroll",
        "the first and only request must be the enrollment replay"
    );
    assert!(
        !request.headers.contains_key("authorization"),
        "a key operation must not present a login credential"
    );
    let identity =
        runtime_cloud_connect::IdentityStore::load_optional(&config_dir.join("identity.json"))
            .expect("load resumed identity")
            .expect("the resumed enrollment is durable");
    assert_eq!(identity.org_name.as_deref(), Some("acme"));
}

/// Split preview origins: the portal and the control plane are different hosts.
/// A resumed login operation must reach only the control plane its draft names,
/// so the organization comes from the binding rather than from a portal
/// identification route or an organization listing.
#[test]
fn a_pending_login_draft_resumes_without_a_portal_or_discovery_request() {
    let fixture = Fixture::start_expecting(4);
    let instance = TempDir::new().expect("create instance directory");
    let home = TempDir::new().expect("create isolated home");
    let config_dir = instance.path().join(".spice");
    let draft = publish_pending_draft(
        &config_dir,
        &fixture.endpoint,
        runtime_cloud_connect::EnrollmentAuthorityBinding::AuthenticatedSession {
            organization: "acme".to_string(),
        },
    );

    // Only the default credential is stored — what an inline login leaves
    // behind — and the portal origin is dead, so identifying it there would
    // fail the run instead of quietly succeeding on one host.
    let (success, output) = output_of(
        connect_at(&instance, &home, &fixture.endpoint)
            .env("SPICE_SPICEAI_TOKEN", LOGIN_TOKEN)
            .env("SPICE_BASE_URL", closed_endpoint())
            .env("SPICE_CLOUD_API_URL", closed_endpoint())
            .arg("--project")
            .arg(PROJECT_NAME),
    );
    assert!(success, "resumed login enrollment failed: {output}");

    let state = fixture.finish();
    let state = state.lock().expect("lock final fixture state");
    let paths = state
        .requests
        .iter()
        .map(|request| request.path.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        paths,
        [
            "/v1/cloud-connect/enroll",
            "/v1/cloud-connect/enroll",
            "/v1/cloud-connect/project",
            "/v1/cloud-connect/project"
        ],
        "a resumed login operation reaches only the enrollment control plane"
    );
    assert!(
        state
            .enrollment_operations
            .iter()
            .all(|operation| *operation == draft.enrollment_operation_id),
        "every attempt must carry the pending operation ID"
    );

    let identity =
        runtime_cloud_connect::IdentityStore::load_optional(&config_dir.join("identity.json"))
            .expect("load resumed identity")
            .expect("the resumed enrollment is durable");
    assert_eq!(identity.org_name.as_deref(), Some("acme"));
    assert_eq!(identity.app_name.as_deref(), Some(PROJECT_NAME));
}

/// An enrolled instance ignores an enrollment key — the identity wins — so
/// accepting one beside `--project` would take the operator's secret, drop it,
/// and attach the project with a stored login credential instead. The rule has to
/// hold on this path too, not only on a fresh enrollment.
#[test]
fn an_enrolled_directory_refuses_a_key_with_a_project() {
    let instance = TempDir::new().expect("create instance directory");
    let home = TempDir::new().expect("create isolated home");
    let config_dir = instance.path().join(".spice");
    runtime_cloud_connect::IdentityStore::store(
        &config_dir.join("identity.json"),
        &unattached_identity(),
    )
    .expect("store an enrolled identity");

    let (success, output) = output_of(
        connect_at(&instance, &home, &closed_endpoint())
            .env("SPICE_SPICEAI_TOKEN", LOGIN_TOKEN)
            .arg("--token")
            .arg("spice-enroll-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
            .arg("--project")
            .arg(PROJECT_NAME),
    );
    assert!(!success, "a key with a project must be refused");
    assert!(
        output.contains("--project cannot be used with --token"),
        "the pair must be named as the problem: {output}"
    );
    assert!(
        !output.contains("spice-enroll-"),
        "the key must never be echoed: {output}"
    );
}
