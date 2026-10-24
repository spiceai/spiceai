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

//! Source containers. Adapted from `crates/runtime/tests/docker`, with one
//! difference that scripts depend on: every port is published on a host port
//! chosen up front, so a container that is stopped and started again comes
//! back at the same address and a running `spiced` can reconnect to it.

use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result, bail};
use bollard::{
    Docker,
    container::{
        Config, CreateContainerOptions, InspectContainerOptions, RemoveContainerOptions,
        StartContainerOptions, StopContainerOptions,
    },
    image::CreateImageOptions,
    secret::{ContainerStateStatusEnum, HealthConfig, HealthStatusEnum, HostConfig, PortBinding},
};
use futures::StreamExt;

pub struct ContainerSpec {
    pub name: String,
    pub image: String,
    pub env: Vec<(String, String)>,
    pub command: Option<Vec<String>>,
    /// Container ports to publish; each gets a free host port.
    pub ports: Vec<u16>,
    pub healthcheck: Option<HealthConfig>,
    /// A Docker network to join (created if missing), so containers of one
    /// source can reach each other by name.
    pub network: Option<String>,
}

pub struct Container {
    docker: Docker,
    id: String,
    pub name: String,
    /// Container port to host port.
    ports: HashMap<u16, u16>,
    /// The network it joined, if any.
    network: Option<String>,
}

impl Container {
    pub async fn start(spec: ContainerSpec) -> Result<Self> {
        Self::start_on(spec, HashMap::new()).await
    }

    /// Starts a container publishing each port in `host_ports` on the host
    /// port given there, and every other port of `spec` on a free one.
    pub async fn start_on(spec: ContainerSpec, host_ports: HashMap<u16, u16>) -> Result<Self> {
        anyhow::ensure!(
            !crate::cleanup::shutting_down(),
            "not starting container {}: the run is shutting down",
            spec.name
        );
        let docker = patient(
            Docker::connect_with_local_defaults()
                .context("Failed to connect to Docker; is it running?")?,
        );
        docker
            .ping()
            .await
            .context("Docker did not answer; start Docker (or OrbStack) and retry")?;
        pull_if_missing(&docker, &spec.image).await?;
        if let Some(network) = &spec.network
            && docker
                .inspect_network::<String>(network, None)
                .await
                .is_err()
        {
            let pid = std::process::id().to_string();
            docker
                .create_network(bollard::network::CreateNetworkOptions {
                    name: network.as_str(),
                    labels: HashMap::from([(RUNNER_LABEL, pid.as_str())]),
                    ..Default::default()
                })
                .await
                .with_context(|| format!("Failed to create network {network}"))?;
        }

        let mut ports = HashMap::new();
        for port in &spec.ports {
            let host = match host_ports.get(port) {
                Some(host) => *host,
                None => free_port()?,
            };
            ports.insert(*port, host);
        }
        let bindings = ports
            .iter()
            .map(|(container, host)| {
                (
                    format!("{container}/tcp"),
                    Some(vec![PortBinding {
                        host_ip: Some("127.0.0.1".into()),
                        host_port: Some(host.to_string()),
                    }]),
                )
            })
            .collect::<HashMap<_, _>>();
        #[expect(
            clippy::zero_sized_map_values,
            reason = "bollard's exposed-ports shape"
        )]
        let exposed = bindings
            .keys()
            .map(|k| (k.clone(), HashMap::new()))
            .collect();
        let config = Config::<String> {
            image: Some(spec.image.clone()),
            labels: Some(
                runner_label()
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v))
                    .collect(),
            ),
            env: Some(spec.env.iter().map(|(k, v)| format!("{k}={v}")).collect()),
            cmd: spec.command.clone(),
            exposed_ports: Some(exposed),
            healthcheck: spec.healthcheck.clone(),
            host_config: Some(HostConfig {
                port_bindings: Some(bindings),
                init: Some(true),
                network_mode: spec.network.clone(),
                // On the host's network the container's loopback is the
                // host's; this name then means the host on every platform,
                // as it does under Docker Desktop.
                extra_hosts: (spec.network.as_deref() == Some("host"))
                    .then(|| vec!["host.docker.internal:127.0.0.1".to_string()]),
                ..Default::default()
            }),
            ..Default::default()
        };
        // A container left behind by an interrupted run would hold the name.
        let _ = docker
            .remove_container(
                &spec.name,
                Some(RemoveContainerOptions {
                    force: true,
                    v: true,
                    ..Default::default()
                }),
            )
            .await;
        let created = docker
            .create_container(
                Some(CreateContainerOptions {
                    name: spec.name.as_str(),
                    platform: None,
                }),
                config,
            )
            .await
            .with_context(|| format!("Failed to create container {}", spec.name))?;
        crate::cleanup::track_container(&created.id, &spec.name, spec.network.as_deref());
        let container = Self {
            docker,
            id: created.id,
            name: spec.name,
            ports,
            network: spec.network,
        };
        // On failure the container is dropped, which removes it.
        container.resume().await?;
        Ok(container)
    }

    pub fn host_port(&self, container_port: u16) -> Result<u16> {
        self.ports
            .get(&container_port)
            .copied()
            .with_context(|| format!("container {} does not publish {container_port}", self.name))
    }

    /// Starts a stopped container and waits for it to be healthy.
    pub async fn resume(&self) -> Result<()> {
        self.docker
            .start_container(&self.id, None::<StartContainerOptions<String>>)
            .await
            .with_context(|| format!("Failed to start container {}", self.name))?;
        self.wait_healthy(Duration::from_mins(2)).await
    }

    pub async fn stop(&self) -> Result<()> {
        self.docker
            .stop_container(&self.id, Some(StopContainerOptions { t: 10 }))
            .await
            .with_context(|| format!("Failed to stop container {}", self.name))
    }

    /// Freezes every process in the container; open connections stay open.
    pub async fn pause(&self) -> Result<()> {
        self.docker
            .pause_container(&self.id)
            .await
            .with_context(|| format!("Failed to pause container {}", self.name))
    }

    pub async fn unpause(&self) -> Result<()> {
        self.docker
            .unpause_container(&self.id)
            .await
            .with_context(|| format!("Failed to unpause container {}", self.name))
    }

    /// Runs a command in the container and returns its standard output; fails
    /// with its standard error if it exits non-zero.
    pub async fn exec(&self, cmd: &[&str]) -> Result<String> {
        self.exec_with_input(cmd, &mut std::iter::empty()).await
    }

    /// Runs a command in the container with `input` written to its standard
    /// input, chunk by chunk as the iterator yields them; returns its standard
    /// output and fails if it exits non-zero.
    pub async fn exec_with_input(
        &self,
        cmd: &[&str],
        input: &mut (dyn Iterator<Item = Result<Vec<u8>>> + Send),
    ) -> Result<String> {
        use bollard::{
            container::LogOutput,
            exec::{CreateExecOptions, StartExecResults},
        };
        use tokio::io::AsyncWriteExt;
        let exec = self
            .docker
            .create_exec(
                &self.id,
                CreateExecOptions {
                    cmd: Some(cmd.to_vec()),
                    attach_stdin: Some(true),
                    attach_stdout: Some(true),
                    attach_stderr: Some(true),
                    ..Default::default()
                },
            )
            .await?;
        let StartExecResults::Attached {
            output: mut stream,
            input: mut stdin,
        } = self.docker.start_exec(&exec.id, None).await?
        else {
            bail!("`{}` in {} did not attach", cmd.join(" "), self.name);
        };
        // Output is read alongside the writes: a command that fills its output
        // pipe would otherwise stop reading its input, and both sides wait.
        // Bytes are decoded once, at the end: a chunk can end inside a
        // multi-byte character.
        let reader = tokio::spawn(async move {
            let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
            while let Some(chunk) = stream.next().await {
                match chunk? {
                    LogOutput::StdErr { message } => stderr.extend_from_slice(&message),
                    LogOutput::StdOut { message }
                    | LogOutput::Console { message }
                    | LogOutput::StdIn { message } => {
                        stdout.extend_from_slice(&message);
                    }
                }
            }
            anyhow::Ok((
                String::from_utf8(stdout).context("command output is not UTF-8")?,
                String::from_utf8_lossy(&stderr).into_owned(),
            ))
        });
        let mut written = Ok(());
        for chunk in input {
            // A command that exits early closes its input; its own error is the
            // one worth reporting, so keep going to collect it.
            if let Err(e) = stdin.write_all(&chunk?).await {
                written = Err(e);
                break;
            }
        }
        stdin.shutdown().await.ok();
        drop(stdin);
        let (stdout, stderr) = reader.await.context("exec output reader panicked")??;
        let code = self.docker.inspect_exec(&exec.id).await?.exit_code;
        if code != Some(0) {
            bail!(
                "`{}` in {} exited with {code:?}: {}",
                cmd.join(" "),
                self.name,
                stderr.trim()
            );
        }
        written.with_context(|| format!("Failed to write the input of `{}`", cmd.join(" ")))?;
        Ok(stdout)
    }

    pub async fn remove(&self) -> Result<()> {
        self.docker
            .remove_container(
                &self.id,
                Some(RemoveContainerOptions {
                    force: true,
                    v: true,
                    ..Default::default()
                }),
            )
            .await
            .with_context(|| format!("Failed to remove container {}", self.name))?;
        crate::cleanup::untrack_container(&self.id);
        Ok(())
    }

    /// Removes the network the container joined, once no container is on it.
    pub async fn remove_network(&self) -> Result<()> {
        // Docker's own networks are shared by everything on the host.
        if let Some(network) = self
            .network
            .as_ref()
            .filter(|n| !matches!(n.as_str(), "host" | "bridge" | "none"))
        {
            self.docker
                .remove_network(network)
                .await
                .with_context(|| format!("Failed to remove network {network}"))?;
        }
        Ok(())
    }

    async fn wait_healthy(&self, timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut last;
        loop {
            let state = self
                .docker
                .inspect_container(&self.id, None::<InspectContainerOptions>)
                .await?
                .state;
            let health = state
                .as_ref()
                .and_then(|s| s.health.as_ref())
                .and_then(|h| h.status);
            match health {
                Some(HealthStatusEnum::HEALTHY) | None
                    if state.as_ref().and_then(|s| s.running) == Some(true) =>
                {
                    return Ok(());
                }
                other => last = format!("{other:?}"),
            }
            if let Some(
                status @ (ContainerStateStatusEnum::EXITED | ContainerStateStatusEnum::DEAD),
            ) = state.as_ref().and_then(|s| s.status)
            {
                let code = state
                    .as_ref()
                    .and_then(|s| s.exit_code)
                    .map_or_else(|| "unknown".to_string(), |c| c.to_string());
                bail!(
                    "container {} {status} (exit code {code}) before it was healthy; see `docker logs {}`",
                    self.name,
                    self.name
                );
            }
            if tokio::time::Instant::now() > deadline {
                bail!(
                    "container {} not healthy after {timeout:?} (last health: {last})",
                    self.name
                );
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

impl Drop for Container {
    /// A container dropped without being removed (a failed start, a panic, a
    /// cancelled run) is removed here, unless `--keep`.
    fn drop(&mut self) {
        crate::cleanup::remove_if_tracked(&self.id);
    }
}

/// How long a request to Docker may take. bollard's default, 120 s, is
/// shorter than the stalls a daemon on a busy CI host goes through, and a
/// request it gives up on, such as a create, is still carried out by the
/// daemon, so failing at 120 s only fails the script.
const DOCKER_REQUEST_TIMEOUT: Duration = Duration::from_mins(10);

/// `docker` waiting out a stalled daemon instead of giving up at bollard's
/// default request timeout.
fn patient(docker: Docker) -> Docker {
    docker.with_timeout(DOCKER_REQUEST_TIMEOUT)
}

async fn pull_if_missing(docker: &Docker, image: &str) -> Result<()> {
    if docker.inspect_image(image).await.is_ok() {
        return Ok(());
    }
    let mut pull = docker.create_image(
        Some(CreateImageOptions {
            from_image: image,
            ..Default::default()
        }),
        None,
        None,
    );
    while let Some(event) = pull.next().await {
        event.with_context(|| format!("Failed to pull {image}"))?;
    }
    Ok(())
}

/// A host port that is free now. Another process could take it before Docker
/// binds it; the window is small and a collision fails loudly at create time.
/// A free local port that no other instance of this run has been given. The
/// port is free when it is chosen and released for its user to bind, so two
/// instances starting at once could otherwise be handed the same one.
/// A local port no one is listening on and this run has not given out.
///
/// Ports come from below the operating systems' ephemeral ranges (macOS
/// 49152+, Linux 32768+): the OS hands those to outgoing connections too, so
/// one could take a port between this check and the container or `spiced`
/// binding it. Each runner starts at its own offset so runners on one host
/// rarely probe the same ports.
/// The label naming the runner process that created a container or network,
/// which the reaper reads to tell a live run's resources from a dead one's.
pub const RUNNER_LABEL: &str = "spicescript.runner";

fn runner_label() -> HashMap<&'static str, String> {
    HashMap::from([(RUNNER_LABEL, std::process::id().to_string())])
}

pub fn free_port() -> Result<u16> {
    const LOW: u16 = 20000;
    const HIGH: u16 = 32768;
    static GIVEN: std::sync::LazyLock<parking_lot::Mutex<std::collections::HashSet<u16>>> =
        std::sync::LazyLock::new(Default::default);
    static NEXT: std::sync::LazyLock<std::sync::atomic::AtomicU32> =
        std::sync::LazyLock::new(|| {
            std::sync::atomic::AtomicU32::new(
                std::process::id().wrapping_mul(7919) % u32::from(HIGH - LOW),
            )
        });
    for _ in LOW..HIGH {
        let offset =
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % u32::from(HIGH - LOW);
        let port = LOW + u16::try_from(offset).unwrap_or(0);
        if GIVEN.lock().contains(&port) || std::net::TcpListener::bind(("127.0.0.1", port)).is_err()
        {
            continue;
        }
        if GIVEN.lock().insert(port) {
            return Ok(port);
        }
    }
    anyhow::bail!("no free local port in {LOW}..{HIGH} that this run has not already given out")
}

pub fn healthcheck(probe: &str) -> HealthConfig {
    HealthConfig {
        test: Some(vec!["CMD-SHELL".to_string(), probe.to_string()]),
        interval: Some(1_000_000_000),
        timeout: Some(5_000_000_000),
        retries: Some(120),
        start_period: Some(10_000_000_000),
        start_interval: Some(250_000_000),
    }
}

#[cfg(test)]
mod timeout_tests {
    use std::time::Duration;

    use bollard::{
        API_DEFAULT_VERSION, Docker,
        container::{Config, CreateContainerOptions},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    /// A Docker API that answers each create 2 s late, as a stalled daemon
    /// does, through a client that would give up after 1 s.
    async fn slow_daemon() -> Docker {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local address");
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut request = Vec::new();
                    let mut buf = [0; 4096];
                    while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                        match socket.read(&mut buf).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => request.extend_from_slice(&buf[..n]),
                        }
                    }
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    let body = r#"{"Id":"c1","Warnings":[]}"#;
                    let response = format!(
                        "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });
        Docker::connect_with_http(&format!("http://{addr}"), 1, API_DEFAULT_VERSION)
            .expect("client")
    }

    #[tokio::test]
    async fn a_patient_client_waits_out_a_create_slower_than_its_default_timeout() {
        let docker = super::patient(slow_daemon().await);
        let created = docker
            .create_container(
                Some(CreateContainerOptions {
                    name: "src",
                    platform: None,
                }),
                Config::<String>::default(),
            )
            .await
            .expect("the create is answered within the patient timeout");
        assert_eq!(created.id, "c1");
    }
}

#[cfg(test)]
mod port_tests {
    #[test]
    fn free_port_hands_out_distinct_ports_below_the_ephemeral_ranges() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..3000 {
            let port = super::free_port().expect("a free port");
            assert!(
                (20000..32768).contains(&port),
                "port {port} is outside 20000..32768"
            );
            assert!(seen.insert(port), "port {port} was given out twice");
        }
    }
}
