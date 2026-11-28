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

use std::{
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use base64::Engine as _;
use bollard::{Docker, auth::DockerCredentials, secret::HealthConfig};
use parking_lot::RwLock;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    process::{Child, ChildStdin, Command},
    sync::Semaphore,
};

use super::{ContainerRunnerBuilder, RunningContainer, pull_image, registry_credentials};

fn http_fixture(marker: &str) -> ContainerRunnerBuilder {
    ContainerRunnerBuilder::new("runtime-integration-test-isolation")
        .image("busybox:1.36".into())
        .publish_port(8080)
        .publish_port(8081)
        .command([
            "sh".into(),
            "-c".into(),
            format!("mkdir -p /www; printf '%s' '{marker}' > /www/index.html; httpd -f -p 8080 -h /www & exec httpd -f -p 8081 -h /www"),
        ])
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD-SHELL".into(),
                "wget -qO- http://127.0.0.1:8080 && wget -qO- http://127.0.0.1:8081".into(),
            ]),
            interval: Some(100_000_000),
            timeout: Some(1_000_000_000),
            retries: Some(10),
            ..Default::default()
        })
}

async fn assert_serves(ports: &[u16], marker: &str) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    for port in ports {
        let text = client
            .get(format!("http://127.0.0.1:{port}"))
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        assert_eq!(text, marker);
    }
    Ok(())
}

struct FixtureProcess {
    child: Child,
    input: ChildStdin,
    _output: tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
    id: String,
    name: String,
    ports: Vec<u16>,
}

impl FixtureProcess {
    async fn start(marker: &str) -> anyhow::Result<Self> {
        let mut child = Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "docker::tests::fixture_process",
                "--ignored",
                "--nocapture",
            ])
            .env("SPICE_DOCKER_ISOLATION_CHILD", marker)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let input = child
            .stdin
            .take()
            .ok_or_else(|| anyhow::anyhow!("child stdin missing"))?;
        let output = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("child stdout missing"))?;
        let mut lines = BufReader::new(output).lines();
        let ready = tokio::time::timeout(Duration::from_secs(90), async {
            while let Some(line) = lines.next_line().await? {
                if let Some((_, ready)) = line.split_once("ISOLATION_READY ") {
                    return Ok::<_, anyhow::Error>(ready.to_string());
                }
            }
            anyhow::bail!("Fixture process exited before reporting its endpoint");
        })
        .await??;
        let fields = ready.split_whitespace().collect::<Vec<_>>();
        anyhow::ensure!(fields.len() == 4, "Invalid fixture report: {ready}");
        Ok(Self {
            child,
            input,
            _output: lines,
            id: fields[0].into(),
            name: fields[1].into(),
            ports: vec![fields[2].parse()?, fields[3].parse()?],
        })
    }

    async fn finish(mut self) -> anyhow::Result<()> {
        self.input.write_all(b"finish\n").await?;
        let status = tokio::time::timeout(Duration::from_secs(45), self.child.wait()).await??;
        anyhow::ensure!(status.success(), "Fixture process failed: {status}");
        Ok(())
    }
}

// Invoked by the parent regression in two independent processes, not by the suite.
#[tokio::test]
#[ignore = "child process of overlapping_fixture_processes_are_isolated"]
async fn fixture_process() -> anyhow::Result<()> {
    let marker = std::env::var("SPICE_DOCKER_ISOLATION_CHILD")?;
    let container = http_fixture(&marker)
        .build()?
        .run(Some(Duration::from_secs(30)))
        .await?;
    println!(
        "ISOLATION_READY {} {} {} {}",
        container.id(),
        container.name(),
        container.host_port(8080)?,
        container.host_port(8081)?
    );
    let mut command = String::new();
    // EOF also drops the guard if the parent fails or is killed.
    BufReader::new(tokio::io::stdin())
        .read_line(&mut command)
        .await?;
    container.remove().await?;
    container.remove().await?;
    Ok(())
}

#[tokio::test]
async fn overlapping_fixture_processes_are_isolated() -> anyhow::Result<()> {
    let first = FixtureProcess::start("first").await?;
    let second = FixtureProcess::start("second").await?;
    assert_ne!(first.id, second.id);
    assert_ne!(first.name, second.name);
    for port in &first.ports {
        assert!(!second.ports.contains(port));
    }
    assert_serves(&first.ports, "first").await?;
    assert_serves(&second.ports, "second").await?;
    first.finish().await?;
    assert_serves(&second.ports, "second").await?;
    second.finish().await?;
    Ok(())
}

#[tokio::test]
async fn failed_readiness_cleans_only_its_instance() -> anyhow::Result<()> {
    let peer = http_fixture("peer")
        .build()?
        .run(Some(Duration::from_secs(30)))
        .await?;
    let failed = http_fixture("failed").healthcheck(HealthConfig {
        test: Some(vec!["CMD".into(), "false".into()]),
        interval: Some(100_000_000),
        timeout: Some(1_000_000_000),
        retries: Some(1),
        ..Default::default()
    });
    let failed_name = failed.name.clone();
    assert!(
        failed
            .build()?
            .run(Some(Duration::from_secs(2)))
            .await
            .is_err()
    );
    let error = peer
        .docker
        .inspect_container(&failed_name, None)
        .await
        .expect_err("failed container must be removed");
    assert!(super::has_status(&error, 404));
    assert_serves(&[peer.host_port(8080)?, peer.host_port(8081)?], "peer").await?;
    peer.remove().await?;
    Ok(())
}

#[tokio::test]
async fn dropping_one_instance_preserves_its_peer() -> anyhow::Result<()> {
    let first: RunningContainer = http_fixture("first").build()?.run(None).await?;
    let second = http_fixture("second").build()?.run(None).await?;
    first.stop().await?;
    first.start().await?;
    first.wait_healthy(Some(Duration::from_secs(30))).await?;
    assert_serves(&[first.host_port(8080)?, first.host_port(8081)?], "first").await?;
    let id = first.id().to_string();
    drop(first);
    let error = second
        .docker
        .inspect_container(&id, None)
        .await
        .expect_err("dropped container must be removed");
    assert!(super::has_status(&error, 404));
    assert_serves(
        &[second.host_port(8080)?, second.host_port(8081)?],
        "second",
    )
    .await?;
    second.remove().await?;
    Ok(())
}

/// A container on a daemon that refuses every request, so each removal of it fails.
fn unremovable() -> anyhow::Result<RunningContainer> {
    Ok(RunningContainer {
        name: "unremovable".into(),
        id: "unremovable".into(),
        // Nothing listens on port 1, so the connection is refused at once.
        docker: Docker::connect_with_http("http://127.0.0.1:1", 5, bollard::API_DEFAULT_VERSION)?,
        ports: RwLock::default(),
        removed: AtomicBool::new(false),
        _permit: Arc::new(Semaphore::new(1)).try_acquire_owned()?,
    })
}

#[tokio::test]
async fn a_failed_removal_keeps_the_scenario_result() -> anyhow::Result<()> {
    let container = unremovable()?;
    assert!(container.remove().await.is_err(), "the removal must fail");
    assert_eq!(container.remove_after(Ok::<_, anyhow::Error>(7)).await?, 7);
    let error = container
        .remove_after(Err::<(), _>(anyhow::anyhow!("scenario failed")))
        .await
        .expect_err("the scenario failed");
    assert_eq!(error.to_string(), "scenario failed");
    Ok(())
}

#[tokio::test]
async fn a_failed_cleanup_leaves_the_container_for_drop_to_remove() -> anyhow::Result<()> {
    let container = unremovable()?;
    container.cleanup().await;
    assert!(
        !container.removed.load(Ordering::Relaxed),
        "a failed cleanup must not mark the container removed, or Drop skips its retry"
    );
    Ok(())
}

/// `docker login` to Docker Hub as `user` with password `s3cr3t`, as the Docker
/// CLI stores it.
const HUB_LOGIN: &str = r#"{"auths":{"https://index.docker.io/v1/":{"auth":"dXNlcjpzM2NyM3Q="}}}"#;

fn hub_login() -> DockerCredentials {
    DockerCredentials {
        username: Some("user".into()),
        password: Some("s3cr3t".into()),
        serveraddress: Some("https://index.docker.io/v1/".into()),
        ..Default::default()
    }
}

#[test]
fn reads_the_login_for_the_registry_an_image_is_pulled_from() {
    for image in [
        "postgres:18",
        "docker.io/library/postgres:18",
        "clickhouse/clickhouse-server:24.8",
    ] {
        assert_eq!(
            registry_credentials(HUB_LOGIN, image),
            Some(hub_login()),
            "{image}"
        );
    }

    // A login to another registry is not sent to Docker Hub, and the Hub login
    // is not sent to another registry.
    let ghcr = r#"{"auths":{"ghcr.io":{"username":"bot","password":"token"}}}"#;
    assert_eq!(registry_credentials(ghcr, "postgres:18"), None);
    assert_eq!(
        registry_credentials(HUB_LOGIN, "ghcr.io/spiceai/image:1"),
        None
    );
    assert_eq!(
        registry_credentials(HUB_LOGIN, "localhost:5000/image"),
        None
    );
    assert_eq!(
        registry_credentials(ghcr, "ghcr.io/spiceai/image:1"),
        Some(DockerCredentials {
            username: Some("bot".into()),
            password: Some("token".into()),
            serveraddress: Some("ghcr.io".into()),
            ..Default::default()
        })
    );

    // A login kept in a credential helper leaves an empty entry: pull
    // anonymously rather than send an empty login.
    let helper = r#"{"auths":{"https://index.docker.io/v1/":{}},"credsStore":"desktop"}"#;
    assert_eq!(registry_credentials(helper, "postgres:18"), None);
    assert_eq!(registry_credentials("not json", "postgres:18"), None);
}

/// A Docker API that answers every request with one pull progress message and
/// keeps each request's `X-Registry-Auth` header.
async fn recording_daemon() -> anyhow::Result<(Docker, Arc<parking_lot::Mutex<Vec<String>>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let auth = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let seen = Arc::clone(&auth);
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let seen = Arc::clone(&seen);
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buf = [0; 4096];
                while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                    match socket.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => request.extend_from_slice(&buf[..n]),
                    }
                }
                let request = String::from_utf8_lossy(&request).into_owned();
                if let Some(value) = request.lines().find_map(|line| {
                    line.split_once(':')
                        .filter(|(name, _)| name.eq_ignore_ascii_case("x-registry-auth"))
                        .map(|(_, value)| value.trim().to_string())
                }) {
                    seen.lock().push(value);
                }
                let response = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"status\":\"Pulling from library/postgres\"}\n";
                let _ = socket.write_all(response.as_bytes()).await;
            });
        }
    });
    let docker =
        Docker::connect_with_http(&format!("http://{addr}"), 5, bollard::API_DEFAULT_VERSION)?;
    Ok((docker, auth))
}

#[tokio::test]
async fn a_pull_sends_the_stored_login() -> anyhow::Result<()> {
    let (docker, auth) = recording_daemon().await?;
    pull_image(&docker, "postgres:18", Some(hub_login())).await?;
    pull_image(&docker, "postgres:18", None).await?;
    let sent = auth.lock().clone();
    assert_eq!(sent.len(), 2, "{sent:?}");
    assert!(
        !sent[0].is_empty(),
        "the stored login was not sent: {sent:?}"
    );
    let login: serde_json::Value =
        serde_json::from_slice(&base64::engine::general_purpose::STANDARD.decode(&sent[0])?)?;
    assert_eq!(login["username"], "user");
    assert_eq!(login["password"], "s3cr3t");
    assert_eq!(login["serveraddress"], "https://index.docker.io/v1/");
    // Without a login the header is empty: Docker pulls anonymously.
    assert!(sent[1].is_empty(), "{sent:?}");
    Ok(())
}
