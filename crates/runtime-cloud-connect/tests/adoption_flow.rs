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
//! These spin up an in-process tonic server that speaks the
//! `spice.cloud.v1.CloudConnect` protocol on a free TCP port, then
//! exercise:
//!
//! - First-contact adoption: Hello with code → server sends Adopt →
//!   client persists identity, replies with AdoptAck + CommandResult.
//! - `ApplySpicepod` round-trip: server sends ApplySpicepod → client
//!   writes the YAML to disk and replies with success.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use runtime_cloud_connect::config::CloudConnectConfig;
use runtime_cloud_connect::handlers::RuntimeHandle;
use runtime_cloud_connect::identity::IdentityStore;
use runtime_cloud_connect::proto;
use runtime_cloud_connect::proto::cloud_connect_server::{CloudConnect, CloudConnectServer};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

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

struct CapturedRuntime {
    applied: Arc<Mutex<Option<(PathBuf, String)>>>,
}

#[async_trait]
impl RuntimeHandle for CapturedRuntime {
    async fn apply_spicepod(
        &self,
        config_dir: &std::path::Path,
        spicepod_yaml: &str,
    ) -> Result<serde_json::Value, String> {
        let path = config_dir.join(runtime_cloud_connect::config::CLOUD_MANAGED_SPICEPOD_FILE);
        std::fs::create_dir_all(config_dir).map_err(|e| e.to_string())?;
        std::fs::write(&path, spicepod_yaml).map_err(|e| e.to_string())?;
        *self.applied.lock().await = Some((path.clone(), spicepod_yaml.to_string()));
        Ok(serde_json::json!({ "path": path.display().to_string() }))
    }
}

async fn spawn_server(mock: MockServer) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let svc = CloudConnectServer::new(mock);
    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(svc)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });
    // Give the server a moment to bind.
    tokio::time::sleep(Duration::from_millis(20)).await;
    addr
}

#[tokio::test]
async fn first_contact_adoption_persists_identity_and_acks() {
    let dir = tempfile::tempdir().unwrap();
    let identity_path = dir.path().join("identity.json");

    // Server script: send a single Adopt command after Hello.
    let adopt_cmd = proto::ControlMessage {
        body: Some(proto::control_message::Body::Adopt(proto::Adopt {
            command_id: "cmd-adopt-1".to_string(),
            assigned_identifier: "inst_unit_test".to_string(),
            identity_cert_pem:
                "-----BEGIN CERTIFICATE-----\nUNIT-TEST\n-----END CERTIFICATE-----\n".to_string(),
            not_after_unix: 0,
        })),
    };
    let mock = MockServer::new(vec![adopt_cmd]);
    let mock_state = Arc::clone(&mock.state);
    let addr = spawn_server(mock).await;

    let endpoint = format!("http://{addr}");

    let config = CloudConnectConfig {
        endpoint,
        ca_cert_pem: None,
        insecure: true,
        identity_path: identity_path.clone(),
        config_dir: dir.path().to_path_buf(),
        adoption_code: Some("SPICE-ADOPT-AAAA-BBBB".to_string()),
        pending_adopt_code_path: None,
        runtime_version: "v0.0.0-test".to_string(),
    };

    let runtime: Arc<dyn RuntimeHandle> =
        Arc::new(runtime_cloud_connect::handlers::NoopRuntimeHandle);
    let handle = runtime_cloud_connect::CloudConnect::start(config, runtime)
        .await
        .expect("start")
        .expect("started");

    // Wait for adoption to settle.
    let mut adopted = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if identity_path.exists() {
            adopted = true;
            break;
        }
    }
    assert!(adopted, "identity file should be created within 5s");

    let identity = IdentityStore::load_optional(&identity_path)
        .expect("load identity")
        .expect("identity present");
    assert_eq!(identity.identifier, "inst_unit_test");
    assert!(identity.identity_cert_pem.contains("UNIT-TEST"));
    assert!(identity.public_key_pem.contains("PUBLIC KEY"));

    // Server should have received the Hello, AdoptAck, and a successful CommandResult.
    // Give a bit more time for the result to land.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let s = mock_state.lock().await;
    let hello = s.last_hello.clone().expect("server saw Hello");
    assert_eq!(hello.kind, proto::InstanceKind::Standalone as i32);
    assert!(
        hello.identifier.is_empty(),
        "first hello has empty identifier"
    );
    assert_eq!(hello.credential, "SPICE-ADOPT-AAAA-BBBB");

    let ack = s.last_adopt_ack.clone().expect("server saw AdoptAck");
    assert_eq!(ack.identifier, "inst_unit_test");
    assert!(ack.identity_pubkey_pem.contains("PUBLIC KEY"));

    let result = s.last_result.clone().expect("server saw CommandResult");
    assert_eq!(result.command_id, "cmd-adopt-1");
    assert!(result.success);
    drop(s);

    handle.shutdown().await;
}

#[tokio::test]
async fn apply_spicepod_writes_file_and_acks() {
    let dir = tempfile::tempdir().unwrap();
    let identity_path = dir.path().join("identity.json");
    let config_dir = dir.path().to_path_buf();

    // Pre-seed identity so the client connects in identity mode.
    let identity = runtime_cloud_connect::identity::Identity {
        identifier: "inst_pre_adopted".to_string(),
        identity_cert_pem: "PRE-ADOPTED-CERT".to_string(),
        private_key_pem: "PRE-ADOPTED-KEY".to_string(),
        public_key_pem: "PRE-ADOPTED-PUB".to_string(),
        not_after_unix: 0,
    };
    IdentityStore::store(&identity_path, &identity).unwrap();

    let yaml = "name: cloud-managed\n";
    let apply_cmd = proto::ControlMessage {
        body: Some(proto::control_message::Body::ApplySpicepod(
            proto::ApplySpicepod {
                command_id: "cmd-apply-1".to_string(),
                spicepod_yaml: yaml.to_string(),
            },
        )),
    };
    let mock = MockServer::new(vec![apply_cmd]);
    let mock_state = Arc::clone(&mock.state);
    let addr = spawn_server(mock).await;

    let captured = Arc::new(Mutex::new(None));
    let runtime: Arc<dyn RuntimeHandle> = Arc::new(CapturedRuntime {
        applied: Arc::clone(&captured),
    });

    let endpoint = format!("http://{addr}");
    let config = CloudConnectConfig {
        endpoint,
        ca_cert_pem: None,
        insecure: true,
        identity_path: identity_path.clone(),
        config_dir: config_dir.clone(),
        adoption_code: None,
        pending_adopt_code_path: None,
        runtime_version: "v0.0.0-test".to_string(),
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

    let (written_path, written_yaml) = captured.lock().await.clone().unwrap();
    assert_eq!(written_yaml, yaml);
    assert!(written_path.exists(), "file should be on disk");

    // Server should see the CommandResult for the apply.
    tokio::time::sleep(Duration::from_millis(150)).await;
    let s = mock_state.lock().await;
    let hello = s.last_hello.clone().expect("server saw Hello");
    assert_eq!(hello.identifier, "inst_pre_adopted");
    assert_eq!(hello.credential, "PRE-ADOPTED-CERT");

    let result = s.last_result.clone().expect("server saw CommandResult");
    assert_eq!(result.command_id, "cmd-apply-1");
    assert!(result.success);
    drop(s);

    handle.shutdown().await;
}
