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
    auth::DockerCredentials,
    container::{
        Config, CreateContainerOptions, InspectContainerOptions, NetworkingConfig,
        RemoveContainerOptions, StartContainerOptions, StopContainerOptions,
    },
    image::CreateImageOptions,
    secret::{
        ContainerStateStatusEnum, EndpointSettings, HealthConfig, HealthStatusEnum, HostConfig,
        PortBinding,
    },
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
        let (created_id, name) = create_or_adopt(
            CREATE_ATTEMPTS,
            CREATE_RETRY_PAUSE,
            &spec.name,
            |name| {
                let docker = &docker;
                let mut config = config.clone();
                // Under another name, keep the original as an alias on the
                // container's network, so its peers still find it by it.
                if name != spec.name
                    && let Some(network) = spec.network.as_deref().filter(|n| *n != "host")
                {
                    config.networking_config = Some(NetworkingConfig {
                        endpoints_config: HashMap::from([(
                            network.to_string(),
                            EndpointSettings {
                                aliases: Some(vec![spec.name.clone()]),
                                ..Default::default()
                            },
                        )]),
                    });
                }
                async move {
                    docker
                        .create_container(
                            Some(CreateContainerOptions {
                                name: name.as_str(),
                                platform: None,
                            }),
                            config,
                        )
                        .await
                        .map(|created| created.id)
                }
            },
            |holder| {
                let docker = &docker;
                async move {
                    docker
                        .inspect_container(&holder, None)
                        .await
                        .map(|found| found.id.unwrap_or(holder))
                }
            },
        )
        .await
        .with_context(|| format!("Failed to create container {}", spec.name))?;
        crate::cleanup::track_container(&created_id, &name, spec.network.as_deref());
        let container = Self {
            docker,
            id: created_id,
            name,
            ports,
            network: spec.network,
        };
        // On failure the container is dropped, which removes it, so what it
        // printed goes into the error.
        if let Err(e) = container.resume().await {
            let tail = container.logs(Some(START_FAILURE_LOG_LINES)).await;
            return Err(match tail {
                Ok(tail) if !tail.is_empty() => anyhow::anyhow!(
                    "{e:#}\nits last {START_FAILURE_LOG_LINES} lines of output:\n{}",
                    String::from_utf8_lossy(&tail).trim_end()
                ),
                _ => e,
            });
        }
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
        // A database container on a CI runner busy with other jobs can take
        // over two minutes to become healthy.
        self.wait_healthy(Duration::from_mins(5)).await
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

    /// Writes the container's output so far, standard output and error
    /// interleaved and timestamped, to `path`: what a failed script's source
    /// said, kept after the container is removed.
    pub async fn save_logs(&self, path: &std::path::Path) -> Result<()> {
        let out = self.logs(None).await?;
        tokio::fs::write(path, out)
            .await
            .with_context(|| format!("Failed to write {}", path.display()))
    }

    /// The container's output, timestamped: all of it, or its last `tail`
    /// lines.
    async fn logs(&self, tail: Option<usize>) -> Result<Vec<u8>> {
        use bollard::container::LogsOptions;
        let mut stream = self.docker.logs(
            &self.id,
            Some(LogsOptions::<String> {
                stdout: true,
                stderr: true,
                timestamps: true,
                tail: tail.map_or_else(|| "all".to_string(), |n| n.to_string()),
                ..Default::default()
            }),
        );
        let mut out = Vec::new();
        while let Some(chunk) = stream.next().await {
            out.extend_from_slice(
                &chunk
                    .with_context(|| format!("Failed to read the logs of container {}", self.name))?
                    .into_bytes(),
            );
        }
        Ok(out)
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

/// How much of a container's output an error for its failed start carries.
const START_FAILURE_LOG_LINES: usize = 20;

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

/// How many times an image is pulled before the pull counts as failed. A
/// pull streams the image from its registry through the daemon, and that
/// stream can break off part way: the response ends mid-message (`bytes
/// remaining on stream`), or the daemon reports a registry error mid-stream.
/// Pulling again keeps the layers already fetched. Only the pull is retried;
/// the image is not what a script tests.
const PULL_ATTEMPTS: u32 = 3;

/// How long to wait before pulling again after a pull broke off.
const PULL_RETRY_PAUSE: Duration = Duration::from_secs(5);

async fn pull_if_missing(docker: &Docker, image: &str) -> Result<()> {
    if docker.inspect_image(image).await.is_ok() {
        return Ok(());
    }
    let credentials = match docker_config_path() {
        Some(path) => tokio::fs::read_to_string(path)
            .await
            .ok()
            .and_then(|config| registry_credentials(&config, image)),
        None => None,
    };
    let login = match credentials
        .as_ref()
        .and_then(|c| c.serveraddress.as_deref())
    {
        Some(address) => format!("with the docker login for {address}"),
        None => "anonymously".to_string(),
    };
    crate::runner::emit(&format!("   pulling {image} {login}"));
    pull_with_retries(PULL_ATTEMPTS, PULL_RETRY_PAUSE, image, || {
        pull_image(docker, image, credentials.clone())
    })
    .await
    .map_err(|err| {
        anyhow::anyhow!(
            "Failed to pull {image} ({login}): {}",
            pull_error_text(&err)
        )
    })
}

async fn pull_image(
    docker: &Docker,
    image: &str,
    credentials: Option<DockerCredentials>,
) -> Result<(), bollard::errors::Error> {
    let mut pull = docker.create_image(
        Some(CreateImageOptions {
            from_image: image,
            ..Default::default()
        }),
        None,
        credentials,
    );
    while let Some(event) = pull.next().await {
        event?;
    }
    Ok(())
}

/// The Docker CLI's config file, where `docker login` stores credentials:
/// `$DOCKER_CONFIG/config.json`, else `~/.docker/config.json`.
fn docker_config_path() -> Option<std::path::PathBuf> {
    if let Some(dir) = std::env::var_os("DOCKER_CONFIG") {
        return Some(std::path::PathBuf::from(dir).join("config.json"));
    }
    std::env::var_os("HOME").map(|home| {
        std::path::PathBuf::from(home)
            .join(".docker")
            .join("config.json")
    })
}

/// The credentials `docker login` stored in `config` (the Docker CLI's
/// config file) for the registry `image` is pulled from. The Docker CLI
/// sends these with each pull and the daemon never reads the CLI's config,
/// so a pull made without them is anonymous, and Docker Hub rate limits
/// anonymous pulls by address (`429 Too Many Requests`). A login kept in a
/// credential helper (`credsStore`, `credHelpers`) has no usable entry here,
/// and the pull stays anonymous.
fn registry_credentials(config: &str, image: &str) -> Option<DockerCredentials> {
    use base64::Engine as _;

    let config: serde_json::Value = serde_json::from_str(config).ok()?;
    let registry = registry_host(image_registry(image));
    let (address, entry) = config
        .get("auths")?
        .as_object()?
        .iter()
        .find(|(address, _)| registry_host(address) == registry)?;
    let field = |name: &str| entry.get(name).and_then(|v| v.as_str()).map(str::to_string);
    let (username, password) = match field("auth") {
        Some(auth) => {
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(auth.trim())
                .ok()?;
            let (username, password) = std::str::from_utf8(&decoded).ok()?.split_once(':')?;
            (Some(username.to_string()), Some(password.to_string()))
        }
        None => (field("username"), field("password")),
    };
    let identitytoken = field("identitytoken");
    if username.is_none() && identitytoken.is_none() {
        return None;
    }
    Some(DockerCredentials {
        username,
        password,
        identitytoken,
        serveraddress: Some(address.clone()),
        ..Default::default()
    })
}

/// The registry an image reference names: its first path component when that
/// is a host (it has a `.` or a port, or is `localhost`), else Docker Hub.
fn image_registry(image: &str) -> &str {
    match image.split_once('/') {
        Some((first, _)) if first.contains(['.', ':']) || first == "localhost" => first,
        _ => "docker.io",
    }
}

/// A registry address reduced to its host, with Docker Hub's several names
/// (`https://index.docker.io/v1/`, the key `docker login` writes, among them)
/// reduced to one.
fn registry_host(address: &str) -> &str {
    let host = address
        .strip_prefix("https://")
        .or_else(|| address.strip_prefix("http://"))
        .unwrap_or(address);
    let host = host.split('/').next().unwrap_or(host);
    match host {
        "index.docker.io" | "registry-1.docker.io" => "docker.io",
        host => host,
    }
}

/// Runs `pull` until it succeeds, fails other than by its stream breaking
/// off, or has been tried `attempts` times, pausing `pause` before each
/// retry. Each retry is reported in the script's output, naming `image`.
async fn pull_with_retries<P, PF>(
    attempts: u32,
    pause: Duration,
    image: &str,
    mut pull: P,
) -> Result<(), bollard::errors::Error>
where
    P: FnMut() -> PF,
    PF: std::future::Future<Output = Result<(), bollard::errors::Error>>,
{
    let mut tried = 1;
    loop {
        match pull().await {
            Ok(()) => return Ok(()),
            Err(err) if tried < attempts && pull_broke_off(&err) => {
                crate::runner::emit(&format!(
                    "   pulling {image} broke off ({}); pulling again ({tried} of {attempts} attempts made)",
                    pull_error_text(&err)
                ));
                tried += 1;
                tokio::time::sleep(pause).await;
            }
            Err(err) => return Err(err),
        }
    }
}

/// Whether a pull failed because its stream broke off, which pulling again
/// can get past, rather than because the image cannot be pulled: Docker
/// answers a missing image or a refused login before the stream starts, and
/// reports a missing manifest or platform in the stream.
fn pull_broke_off(err: &bollard::errors::Error) -> bool {
    use bollard::errors::Error;
    const PERMANENT: [&str; 5] = [
        "manifest unknown",
        "no matching manifest",
        "not found",
        "access denied",
        "unauthorized",
    ];
    match err {
        Error::IOError { .. } => true,
        Error::DockerStreamError { error } => {
            let error = error.to_ascii_lowercase();
            !PERMANENT.iter().any(|permanent| error.contains(permanent))
        }
        _ => false,
    }
}

/// The error a pull failed with, including the daemon's own message for an
/// error reported mid-stream, which bollard's `Display` leaves out.
fn pull_error_text(err: &bollard::errors::Error) -> String {
    match err {
        bollard::errors::Error::DockerStreamError { error } => {
            format!("Docker stream error: {error}")
        }
        other => other.to_string(),
    }
}

/// How many times creating a container is tried when Docker does not answer
/// in time. A daemon busy with many containers at once (several runs on one
/// host) can take longer than the client's request timeout, and creating the
/// container again usually succeeds.
const CREATE_ATTEMPTS: u32 = 5;

/// How long to wait before trying again after Docker did not answer in time,
/// and between lookups of a container a timed-out attempt created.
const CREATE_RETRY_PAUSE: Duration = Duration::from_secs(2);

/// How many times to look up a container a timed-out attempt created before
/// creating under another name (about 20 s at [`CREATE_RETRY_PAUSE`]).
const ADOPT_LOOKUPS: u32 = 10;

/// Runs `create` (with the name to create under) until it succeeds, fails
/// other than by Docker not answering in time, or has been tried `attempts`
/// times, pausing `pause` before each retry; returns the container's id and
/// name. Once an attempt has timed out, a name conflict (409) means that
/// attempt's container was created after all, with this name and
/// configuration: `adopt` looks up the container the conflict names, again
/// after `pause` while Docker does not know it yet, and its id is used. If it
/// never turns up, Docker is holding the name for a container it cannot
/// find, and the next attempt creates under a fresh name (`what-r2`, …).
/// Each retry is reported in the script's output, naming `what`.
async fn create_or_adopt<C, CF, A, AF>(
    attempts: u32,
    pause: Duration,
    what: &str,
    mut create: C,
    mut adopt: A,
) -> Result<(String, String), bollard::errors::Error>
where
    C: FnMut(String) -> CF,
    CF: std::future::Future<Output = Result<String, bollard::errors::Error>>,
    A: FnMut(String) -> AF,
    AF: std::future::Future<Output = Result<String, bollard::errors::Error>>,
{
    let mut tried = 1;
    let mut timed_out = false;
    let mut name = what.to_string();
    loop {
        let conflict = match create(name.clone()).await {
            Ok(id) => return Ok((id, name)),
            Err(bollard::errors::Error::RequestTimeoutError) if tried < attempts => {
                timed_out = true;
                crate::runner::emit(&format!(
                    "   Docker did not answer in time creating {name}; trying again ({tried} of {attempts} attempts made)"
                ));
                tried += 1;
                tokio::time::sleep(pause).await;
                continue;
            }
            Err(bollard::errors::Error::DockerResponseServerError {
                status_code: 409,
                message,
            }) if timed_out => message,
            Err(other) => return Err(other),
        };
        // The conflict names the container holding the name; look it up by
        // that id, retrying while Docker is still registering it.
        let holder = conflict_holder(&conflict).unwrap_or_else(|| name.clone());
        let mut looked = 1;
        let missing = loop {
            match adopt(holder.clone()).await {
                Ok(id) => {
                    crate::runner::emit(&format!(
                        "   a timed-out attempt had created {name}; using that container"
                    ));
                    return Ok((id, name));
                }
                Err(bollard::errors::Error::DockerResponseServerError {
                    status_code: 404,
                    message,
                }) => {
                    if looked >= ADOPT_LOOKUPS {
                        break message;
                    }
                    looked += 1;
                    tokio::time::sleep(pause).await;
                }
                Err(other) => return Err(other),
            }
        };
        if tried >= attempts {
            return Err(bollard::errors::Error::DockerResponseServerError {
                status_code: 404,
                message: missing,
            });
        }
        tried += 1;
        let fresh = format!("{what}-r{tried}");
        crate::runner::emit(&format!(
            "   Docker holds the name {name} for a container it cannot find; creating {fresh} instead ({} of {attempts} attempts made)",
            tried - 1
        ));
        name = fresh;
        tokio::time::sleep(pause).await;
    }
}

/// The id of the container a 409 name conflict says holds the name:
/// `… is already in use by container "<id>". …`.
fn conflict_holder(message: &str) -> Option<String> {
    let (_, rest) = message.split_once("in use by container \"")?;
    let id = rest.split('"').next()?;
    (!id.is_empty() && id.bytes().all(|b| b.is_ascii_hexdigit())).then(|| id.to_string())
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

#[cfg(test)]
mod create_retry_tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use bollard::errors::Error;

    use super::{conflict_holder, create_or_adopt};

    const HOLDER: &str = "7cd4ae6e9028774521e285a3aca8c021b11b04882a842cdd56dfffe3a3814318";

    fn conflict() -> Error {
        Error::DockerResponseServerError {
            status_code: 409,
            // As Docker words it.
            message: format!(
                "Conflict. The container name \"/c\" is already in use by container \"{HOLDER}\". You have to remove (or rename) that container to be able to reuse that name."
            ),
        }
    }

    fn not_found() -> Error {
        Error::DockerResponseServerError {
            status_code: 404,
            message: "No such container".into(),
        }
    }

    fn pair(id: &str, name: &str) -> (String, String) {
        (id.to_string(), name.to_string())
    }

    #[test]
    fn reads_the_holder_from_a_name_conflict() {
        let Err(Error::DockerResponseServerError { message, .. }) = Err::<(), _>(conflict()) else {
            unreachable!("built above");
        };
        assert_eq!(conflict_holder(&message).as_deref(), Some(HOLDER));
        assert_eq!(conflict_holder("Conflict."), None);
    }

    #[tokio::test]
    async fn retries_a_timeout_and_adopts_or_renames_the_container_it_created() {
        let no_pause = Duration::ZERO;

        // Two timeouts, then success: three attempts, under the one name.
        let calls = AtomicU32::new(0);
        let got = create_or_adopt(
            3,
            no_pause,
            "c",
            |_| async {
                match calls.fetch_add(1, Ordering::SeqCst) {
                    0 | 1 => Err(Error::RequestTimeoutError),
                    _ => Ok("new".to_string()),
                }
            },
            |_| async { Err(not_found()) },
        )
        .await;
        assert_eq!(got.ok(), Some(pair("new", "c")));
        assert_eq!(calls.load(Ordering::SeqCst), 3);

        // A timeout, then a conflict: the container the conflict names is
        // looked up by its id (twice, as Docker registers it) and adopted.
        let calls = AtomicU32::new(0);
        let looked_up = Mutex::new(Vec::new());
        let got = create_or_adopt(
            3,
            no_pause,
            "c",
            |_| async {
                match calls.fetch_add(1, Ordering::SeqCst) {
                    0 => Err(Error::RequestTimeoutError),
                    _ => Err(conflict()),
                }
            },
            |holder| {
                let mut seen = looked_up.lock().expect("lock");
                seen.push(holder.clone());
                let found = seen.len() > 1;
                async move { if found { Ok(holder) } else { Err(not_found()) } }
            },
        )
        .await;
        assert_eq!(got.ok(), Some(pair(HOLDER, "c")));
        assert_eq!(
            *looked_up.lock().expect("lock"),
            vec![HOLDER.to_string(); 2]
        );

        // The holder never turns up: the next attempt creates under a fresh
        // name, which succeeds.
        let names = Mutex::new(Vec::new());
        let got = create_or_adopt(
            3,
            no_pause,
            "c",
            |name| {
                let mut tried = names.lock().expect("lock");
                tried.push(name);
                let result = match tried.len() {
                    1 => Err(Error::RequestTimeoutError),
                    2 => Err(conflict()),
                    _ => Ok("new".to_string()),
                };
                async move { result }
            },
            |_| async { Err(not_found()) },
        )
        .await;
        assert_eq!(got.ok(), Some(pair("new", "c-r3")));
        assert_eq!(*names.lock().expect("lock"), ["c", "c", "c-r3"]);

        // Timeouts every time: gives up after the allowed attempts.
        let calls = AtomicU32::new(0);
        let got = create_or_adopt(
            3,
            no_pause,
            "c",
            |_| async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(Error::RequestTimeoutError)
            },
            |_| async { Err(not_found()) },
        )
        .await;
        assert!(matches!(got, Err(Error::RequestTimeoutError)), "{got:?}");
        assert_eq!(calls.load(Ordering::SeqCst), 3);

        // A conflict with no timeout before it is returned at once.
        let calls = AtomicU32::new(0);
        let got = create_or_adopt(
            3,
            no_pause,
            "c",
            |_| async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(conflict())
            },
            |_| async { Ok("adopted".to_string()) },
        )
        .await;
        assert!(
            matches!(
                got,
                Err(Error::DockerResponseServerError {
                    status_code: 409,
                    ..
                })
            ),
            "{got:?}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}

#[cfg(test)]
mod pull_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use base64::Engine as _;
    use bollard::{API_DEFAULT_VERSION, Docker, auth::DockerCredentials, errors::Error};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use super::{
        pull_broke_off, pull_error_text, pull_image, pull_with_retries, registry_credentials,
    };

    /// `docker login` to Docker Hub as `user` with password `s3cr3t`, as the
    /// Docker CLI stores it.
    const HUB_LOGIN: &str =
        r#"{"auths":{"https://index.docker.io/v1/":{"auth":"dXNlcjpzM2NyM3Q="}}}"#;

    /// A Docker API that answers every request with `body` after the
    /// headers of a `200 OK`, then closes the connection, and keeps each
    /// request's `X-Registry-Auth` header.
    async fn daemon(body: &'static str) -> (Docker, Arc<parking_lot::Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local address");
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
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{body}"
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });
        let docker = Docker::connect_with_http(&format!("http://{addr}"), 5, API_DEFAULT_VERSION)
            .expect("client");
        (docker, auth)
    }

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
            "mongo:8.0",
            "docker.io/library/mongo:8.0",
            "clickhouse/clickhouse-server:24.8",
        ] {
            assert_eq!(
                registry_credentials(HUB_LOGIN, image),
                Some(hub_login()),
                "{image}"
            );
        }

        // A login to another registry is not sent to Docker Hub, and the Hub
        // login is not sent to another registry.
        let ghcr = r#"{"auths":{"ghcr.io":{"username":"bot","password":"token"}}}"#;
        assert_eq!(registry_credentials(ghcr, "mongo:8.0"), None);
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
        assert_eq!(registry_credentials(helper, "mongo:8.0"), None);
        assert_eq!(registry_credentials("not json", "mongo:8.0"), None);
    }

    #[tokio::test]
    async fn a_pull_sends_the_stored_login() {
        let (docker, auth) = daemon("{\"status\":\"Pulling from library/mongo\"}\n").await;
        pull_image(&docker, "mongo:8.0", Some(hub_login()))
            .await
            .expect("the pull succeeds");
        pull_image(&docker, "mongo:8.0", None)
            .await
            .expect("the pull succeeds");
        let sent = auth.lock().clone();
        assert_eq!(sent.len(), 2, "{sent:?}");
        let login: serde_json::Value = serde_json::from_slice(
            &base64::engine::general_purpose::STANDARD
                .decode(&sent[0])
                .expect("base64"),
        )
        .expect("JSON");
        assert_eq!(login["username"], "user");
        assert_eq!(login["password"], "s3cr3t");
        assert_eq!(login["serveraddress"], "https://index.docker.io/v1/");
        // Without a login the header is empty: Docker pulls anonymously.
        assert!(sent[1].is_empty(), "{sent:?}");
    }

    #[tokio::test]
    async fn a_stream_cut_off_mid_message_or_failing_mid_stream_is_retried() {
        // The response ends inside a progress message, as a pull does when
        // its connection drops.
        let (docker, _) = daemon("{\"status\":\"Downloading\",\"progressDetail\":{\"curr").await;
        let err = pull_image(&docker, "mongo:8.0", None)
            .await
            .expect_err("a cut-off stream fails the pull");
        assert_eq!(pull_error_text(&err), "bytes remaining on stream");
        assert!(pull_broke_off(&err), "{err:?}");

        // The daemon reports a registry error in the stream; its message is
        // kept, though bollard's `Display` leaves it out.
        let (docker, _) = daemon(
            "{\"errorDetail\":{\"message\":\"read tcp: connection reset by peer\"},\"error\":\"read tcp: connection reset by peer\"}\n",
        )
        .await;
        let err = pull_image(&docker, "mongo:8.0", None)
            .await
            .expect_err("an error in the stream fails the pull");
        assert_eq!(
            pull_error_text(&err),
            "Docker stream error: read tcp: connection reset by peer"
        );
        assert!(pull_broke_off(&err), "{err:?}");
    }

    #[test]
    fn an_image_that_cannot_be_pulled_is_not_retried() {
        for err in [
            Error::DockerResponseServerError {
                status_code: 404,
                message: "pull access denied for nosuch/image, repository does not exist".into(),
            },
            Error::DockerResponseServerError {
                status_code: 500,
                message: "toomanyrequests: You have reached your unauthenticated pull rate limit"
                    .into(),
            },
            Error::DockerStreamError {
                error: "no matching manifest for linux/arm64/v8 in the manifest list entries"
                    .into(),
            },
            Error::DockerStreamError {
                error: "manifest unknown".into(),
            },
        ] {
            assert!(!pull_broke_off(&err), "{err:?}");
        }
    }

    #[tokio::test]
    async fn retries_a_pull_that_broke_off_up_to_the_allowed_attempts() {
        let broke_off = || Error::DockerStreamError {
            error: "unexpected EOF".into(),
        };

        // Two breaks, then success: three attempts.
        let calls = AtomicU32::new(0);
        let got = pull_with_retries(3, Duration::ZERO, "mongo:8.0", || async {
            match calls.fetch_add(1, Ordering::SeqCst) {
                0 | 1 => Err(broke_off()),
                _ => Ok(()),
            }
        })
        .await;
        assert!(got.is_ok(), "{got:?}");
        assert_eq!(calls.load(Ordering::SeqCst), 3);

        // Breaks every time: gives up after the allowed attempts with the
        // last error.
        let calls = AtomicU32::new(0);
        let got = pull_with_retries(3, Duration::ZERO, "mongo:8.0", || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Err(broke_off())
        })
        .await;
        assert!(
            matches!(got, Err(Error::DockerStreamError { .. })),
            "{got:?}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3);

        // A missing image fails at once.
        let calls = AtomicU32::new(0);
        let got = pull_with_retries(3, Duration::ZERO, "nosuch/image", || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Err(Error::DockerResponseServerError {
                status_code: 404,
                message: "pull access denied".into(),
            })
        })
        .await;
        assert!(
            matches!(
                got,
                Err(Error::DockerResponseServerError {
                    status_code: 404,
                    ..
                })
            ),
            "{got:?}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
