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

use std::{process::Stdio, time::Duration};

use bollard::secret::HealthConfig;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, Command},
};

use super::{ContainerRunnerBuilder, RunningContainer};

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
