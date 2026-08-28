/*
Copyright 2024-2025 The Spice.ai OSS Authors

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
#![allow(dead_code, clippy::allow_attributes)]

use std::{
    collections::HashMap,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use bollard::{
    Docker,
    container::{
        Config, CreateContainerOptions, ListContainersOptions, LogOutput, RemoveContainerOptions,
        StartContainerOptions,
    },
    exec::{CreateExecOptions, StartExecResults},
    image::CreateImageOptions,
    secret::{
        ContainerState, ContainerStateStatusEnum, Health, HealthConfig, HealthStatusEnum,
        HostConfig, PortBinding,
    },
};

use futures::StreamExt;
use tokio::sync::Semaphore;

// Limit the number of concurrent container operations to avoid overwhelming the Docker daemon and containers stopping due to OOM
static CONTAINER_SEMAPHORE: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(3)));

/// How long a dropped container waits on the Docker daemon before giving up.
/// Cleanup is best-effort, and a test process must never hang in it.
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a container name is waited on before it is reused, once its
/// removal has been requested.
const NAME_RELEASE_TIMEOUT: Duration = Duration::from_secs(60);

/// How often the name is re-checked while waiting for it to be released.
const NAME_RELEASE_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Whether a removal failed only because the daemon is already removing that
/// container, which reaches the same end state this wanted.
///
/// Matched on the status code rather than the message, so a reworded daemon
/// error cannot silently turn this back into a hard failure.
fn is_removal_already_in_progress(error: &anyhow::Error) -> bool {
    matches!(
        error.downcast_ref::<bollard::errors::Error>(),
        Some(bollard::errors::Error::DockerResponseServerError {
            status_code: 409,
            ..
        })
    )
}

pub struct RunningContainer<'a> {
    name: &'a str,
    docker: Docker,
    /// Whether the container has already been removed, so dropping one a test
    /// cleaned up explicitly does not attempt it a second time.
    removed: AtomicBool,
    // Store the permit to release it when the container is dropped
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl RunningContainer<'_> {
    pub async fn remove(&self) -> Result<(), anyhow::Error> {
        let result = remove(&self.docker, self.name).await;
        if result.is_ok() {
            self.removed.store(true, Ordering::Relaxed);
        }
        result
    }

    pub async fn stop(&self) -> Result<(), anyhow::Error> {
        stop(&self.docker, self.name).await
    }

    pub async fn start(&self) -> Result<(), anyhow::Error> {
        start(&self.docker, self.name).await
    }

    pub async fn exec_cmd(&self, cmd: &str) -> Result<String, anyhow::Error> {
        let cmd_vec: Vec<String> = cmd
            .split_whitespace()
            .map(std::string::ToString::to_string)
            .collect();
        let exec = self
            .docker
            .create_exec(
                self.name,
                CreateExecOptions {
                    attach_stdout: Some(true),
                    attach_stderr: Some(true),
                    cmd: Some(cmd_vec.clone()),
                    ..Default::default()
                },
            )
            .await?;

        let exec_result = self.docker.start_exec(&exec.id, None).await?;
        let mut output_str = String::new();

        if let StartExecResults::Attached { mut output, .. } = exec_result {
            while let Some(Ok(log)) = output.next().await {
                match log {
                    LogOutput::StdOut { message } => {
                        output_str.push_str(&String::from_utf8_lossy(&message));
                    }
                    LogOutput::StdErr { message } => {
                        return Err(anyhow::anyhow!(
                            String::from_utf8_lossy(&message).to_string()
                        ));
                    }
                    _ => {}
                }
            }
        }
        Ok(output_str)
    }
}

/// Removes the container when the test that started it is finished with it,
/// including when that test panicked partway through.
///
/// Explicit cleanup cannot cover this on its own: a `.remove()` call at the end
/// of a test is skipped by the `?` or the failed assertion that ends the test
/// early, which is exactly when a suite is being run repeatedly.
///
/// `Drop` cannot await, and the removal must work whether or not the test's
/// runtime is still running -- a `#[tokio::test]` drops its runtime while
/// unwinding, and spawning onto a runtime that is shutting down silently drops
/// the task. So the removal gets a runtime of its own, on a thread joined
/// before the drop returns. Best-effort by construction: a failure here must
/// not mask the test result that is already on its way out.
///
/// Two cases this cannot reach, both because `Drop` never runs:
///
/// - A container parked in a `static` for the life of the process, as
///   `tpcds_postgres` does to share one database across its queries. Such a
///   suite has to remove its container itself.
/// - A process killed outright rather than unwound (`SIGKILL`, a hard CI
///   timeout).
impl Drop for RunningContainer<'_> {
    fn drop(&mut self) {
        if *self.removed.get_mut() {
            return;
        }

        let docker = self.docker.clone();
        let name = self.name.to_string();
        let removal = std::thread::Builder::new()
            .name(format!("cleanup-{name}"))
            .spawn(move || {
                let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    return Err(anyhow::anyhow!("could not build a runtime for cleanup"));
                };
                runtime.block_on(async {
                    // Bounded: a daemon that accepts the connection and then
                    // stops answering would otherwise hang the test process
                    // here, turning a readable failure into a CI timeout.
                    tokio::time::timeout(CLEANUP_TIMEOUT, remove(&docker, &name))
                        .await
                        .unwrap_or_else(|_| {
                            Err(anyhow::anyhow!(
                                "timed out after {CLEANUP_TIMEOUT:?} waiting for Docker"
                            ))
                        })
                })
            });

        // Report rather than panic: a panicking `Drop` during an unwind aborts
        // the process, which would replace a readable test failure with none.
        // `Builder::spawn` is used over `thread::spawn` for the same reason --
        // it reports a thread that cannot be created instead of panicking, and
        // exhaustion is the very condition this cleanup exists to relieve.
        match removal.map(std::thread::JoinHandle::join) {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(e))) => eprintln!("failed to remove test container {}: {e}", self.name),
            Ok(Err(_)) => eprintln!(
                "the cleanup thread for test container {} panicked",
                self.name
            ),
            Err(e) => eprintln!(
                "could not start a cleanup thread for test container {}: {e}",
                self.name
            ),
        }
    }
}

/// Removes a container that a test has finished with, and every test *must*
/// reach this -- directly or by dropping its [`RunningContainer`].
///
/// A leaked container is not merely untidy: each one holds a running database
/// and an anonymous volume carrying its data directory, so a few runs of the
/// suite are enough to exhaust memory (a concurrent build gets OOM-killed) and
/// tens of gigabytes of disk that nothing reclaims.
pub async fn remove(docker: &Docker, name: &str) -> Result<(), anyhow::Error> {
    Ok(docker
        .remove_container(
            name,
            Some(RemoveContainerOptions {
                force: true,
                // The data directory is an anonymous volume, which outlives the
                // container unless it is removed with it.
                v: true,
                ..Default::default()
            }),
        )
        .await?)
}

pub async fn stop(docker: &Docker, name: &str) -> Result<(), anyhow::Error> {
    Ok(docker.stop_container(name, None).await?)
}

pub async fn start(docker: &Docker, name: &str) -> Result<(), anyhow::Error> {
    Ok(docker
        .start_container(name, None::<StartContainerOptions<String>>)
        .await?)
}

pub struct ContainerRunnerBuilder<'a> {
    name: &'a str,
    image: Option<String>,
    port_bindings: Vec<(u16, u16)>,
    env_vars: Vec<(String, String)>,
    healthcheck: Option<HealthConfig>,
    command: Option<Vec<String>>,
}

impl<'a> ContainerRunnerBuilder<'a> {
    pub fn new(name: &'a str) -> Self {
        ContainerRunnerBuilder {
            name,
            image: None,
            port_bindings: Vec::new(),
            env_vars: Vec::new(),
            healthcheck: None,
            command: None,
        }
    }

    pub fn image(mut self, image: String) -> Self {
        self.image = Some(image);
        self
    }

    pub fn add_port_binding(mut self, host_port: u16, container_port: u16) -> Self {
        self.port_bindings.push((host_port, container_port));
        self
    }

    pub fn add_env_var(mut self, key: &str, value: &str) -> Self {
        self.env_vars.push((key.to_string(), value.to_string()));
        self
    }

    pub fn healthcheck(mut self, healthcheck: HealthConfig) -> Self {
        self.healthcheck = Some(healthcheck);
        self
    }

    pub fn command<I, S>(mut self, cmd: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.command = Some(cmd.into_iter().map(Into::into).collect());
        self
    }

    pub fn build(self) -> Result<ContainerRunner<'a>, anyhow::Error> {
        let image = self
            .image
            .ok_or_else(|| anyhow::anyhow!("Image must be set"))?;
        Ok(ContainerRunner::<'a> {
            name: self.name,
            docker: Docker::connect_with_local_defaults()?,
            image,
            port_bindings: self.port_bindings,
            env_vars: self.env_vars,
            healthcheck: self.healthcheck,
            command: self.command,
        })
    }
}

pub struct ContainerRunner<'a> {
    name: &'a str,
    docker: Docker,
    image: String,
    port_bindings: Vec<(u16, u16)>,
    env_vars: Vec<(String, String)>,
    healthcheck: Option<HealthConfig>,
    command: Option<Vec<String>>,
}

impl<'a> ContainerRunner<'a> {
    pub async fn run(
        self,
        start_timeout: Option<Duration>,
    ) -> Result<RunningContainer<'a>, anyhow::Error> {
        self.wait_for_name_release().await?;

        let permit = tokio::time::timeout(
            std::time::Duration::from_mins(5), // Timeout after 5min
            CONTAINER_SEMAPHORE.clone().acquire_owned(),
        )
        .await
        .map_err(|_| anyhow::anyhow!("Timed out waiting for available container slot"))?
        .map_err(|_| anyhow::anyhow!("Failed to acquire container permit"))?;

        self.pull_image().await?;

        let options = CreateContainerOptions {
            name: self.name,
            platform: None,
        };

        let mut port_bindings_map = HashMap::new();
        for (container_port, host_port) in self.port_bindings {
            port_bindings_map.insert(
                format!("{container_port}/tcp"),
                Some(vec![PortBinding {
                    host_ip: Some("127.0.0.1".to_string()),
                    host_port: Some(format!("{host_port}")),
                }]),
            );
        }
        tracing::debug!("Port bindings: {:?}", port_bindings_map);

        let port_bindings_keys: Vec<String> = port_bindings_map.keys().cloned().collect();

        let (exposed_ports, port_bindings) = if port_bindings_map.is_empty() {
            (None, None)
        } else {
            #[expect(clippy::zero_sized_map_values)]
            let exposed_ports = port_bindings_keys
                .iter()
                .map(|k| (k.as_str(), HashMap::new()))
                .collect::<HashMap<_, _>>();
            (Some(exposed_ports), Some(port_bindings_map))
        };

        let host_config = Some(HostConfig {
            port_bindings,
            // Reap zombies inside the container, so it stays killable and its
            // name and host port are actually released.
            //
            // Several of these images fork children that their PID 1 never
            // reaps (mongod, the DynamoDB local JVM, the Kafka broker). Once a
            // zombie accumulates, Docker cannot kill the container -- removal
            // fails with `could not kill container: ... is zombie and can not
            // be killed`, whose own remedy is this flag. The container then
            // leaks under a name derived from a fixed port, so every later test
            // reusing that name fails on a 409 (`name already in use`, or
            // `removal ... already in progress`) and every later test needing
            // that host port fails with it -- one unkillable container spraying
            // failures across unrelated connectors.
            init: Some(true),
            ..Default::default()
        });

        let env_vars: Vec<String> = self
            .env_vars
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        let env_vars_str = env_vars.iter().map(String::as_str).collect::<Vec<&str>>();

        let config = Config::<&str> {
            image: Some(&self.image),
            env: Some(env_vars_str),
            host_config,
            healthcheck: self.healthcheck,
            exposed_ports,
            cmd: self
                .command
                .as_ref()
                .map(|v| v.iter().map(String::as_str).collect()),
            ..Default::default()
        };

        let _ = self.docker.create_container(Some(options), config).await?;

        // The container exists from here on, so hold it in the guard before
        // anything else can fail. Starting it, inspecting it, or waiting for it
        // to report healthy can all return early, and each of those paths would
        // otherwise leave behind a container no test ever saw.
        let container = RunningContainer::<'a> {
            name: self.name,
            docker: self.docker.clone(),
            removed: AtomicBool::new(false),
            _permit: permit,
        };

        self.docker
            .start_container(self.name, None::<StartContainerOptions<String>>)
            .await?;

        let start_timeout = start_timeout.unwrap_or_else(|| Duration::from_mins(1));
        let start_time = std::time::Instant::now();
        loop {
            let inspect_container = self.docker.inspect_container(self.name, None).await?;
            tracing::trace!("Container status: {:?}", inspect_container.state);

            if let Some(ContainerState {
                status: Some(ContainerStateStatusEnum::RUNNING),
                health:
                    Some(Health {
                        status: Some(HealthStatusEnum::HEALTHY),
                        ..
                    }),
                ..
            }) = inspect_container.state
            {
                tracing::debug!("Container running & healthy");
                break;
            }

            if start_time.elapsed() > start_timeout {
                return Err(anyhow::anyhow!(
                    "Container failed to start (timeout waiting for healthy state)"
                ));
            }

            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        Ok(container)
    }

    async fn pull_image(&self) -> Result<(), anyhow::Error> {
        // Check if image is already pulled
        let images = self.docker.list_images::<&str>(None).await?;
        for image in images {
            if image.repo_tags.iter().any(|t| t == &self.image) {
                tracing::debug!("Docker image {} already pulled", self.image);
                return Ok(());
            }
        }

        let options = Some(CreateImageOptions::<&str> {
            from_image: &self.image,
            ..Default::default()
        });

        let mut pulling_stream = self.docker.create_image(options, None, None);
        while let Some(event) = pulling_stream.next().await {
            tracing::debug!("Pulling image: {:?}", event?);
        }

        Ok(())
    }

    /// Frees `self.name` so a fresh container can take it, waiting out a
    /// removal that the daemon has accepted but not yet finished.
    ///
    /// `remove_container` returns once the removal is *accepted*, not once it is
    /// done, and the name stays taken until it is. Creating inside that window
    /// fails with a 409 (`Conflict. The container name ... is already in use`),
    /// so the name is polled until it is genuinely gone rather than assumed free
    /// the moment the call returns.
    ///
    /// A concurrent remover answers a second removal with a 409 (`removal of
    /// container ... is already in progress`) -- the previous test's `Drop`
    /// cleanup is the usual one, since it joins its thread as soon as the daemon
    /// accepts. That reports the end state this method wants, so it is waited
    /// out rather than raised as a failure.
    async fn wait_for_name_release(&self) -> Result<(), anyhow::Error> {
        if !self.container_exist().await? {
            return Ok(());
        }

        if let Err(e) = remove(&self.docker, self.name).await {
            if !is_removal_already_in_progress(&e) {
                return Err(e);
            }
            tracing::debug!(
                "Docker container {} is already being removed; waiting for its name",
                self.name
            );
        }

        let start_time = std::time::Instant::now();
        while self.container_exist().await? {
            // An unkillable container never releases its name, so this reports
            // that rather than waiting on it for the job's whole budget.
            if start_time.elapsed() > NAME_RELEASE_TIMEOUT {
                return Err(anyhow::anyhow!(
                    "test container {} was still present {NAME_RELEASE_TIMEOUT:?} after its removal was requested, so a new one cannot take its name",
                    self.name
                ));
            }
            tokio::time::sleep(NAME_RELEASE_POLL_INTERVAL).await;
        }

        Ok(())
    }

    async fn container_exist(&self) -> Result<bool, anyhow::Error> {
        let containers = self
            .docker
            .list_containers::<&str>(Some(ListContainersOptions {
                all: true,
                ..Default::default()
            }))
            .await?;
        for container in containers {
            let Some(names) = container.names else {
                continue;
            };
            if names.iter().any(|n| {
                tracing::debug!("Docker container: {n}");
                n == self.name || n == &format!("/{}", self.name)
            }) {
                tracing::debug!("Docker container {} already running", self.name);
                return Ok(true);
            }
        }

        Ok(false)
    }
}

/// Check if Docker is available on this system.
///
/// Returns `true` if Docker daemon is accessible, `false` otherwise.
/// This is useful for tests that require Docker to skip gracefully
/// when Docker is not available (e.g., on certain CI runners).
pub async fn is_docker_available() -> bool {
    let Ok(docker) = Docker::connect_with_local_defaults() else {
        return false;
    };

    // Try to ping the Docker daemon to verify it's actually running
    docker.ping().await.is_ok()
}

pub async fn wait_for_tcp_port(
    host: &str,
    port: u16,
    timeout: Duration,
) -> Result<(), anyhow::Error> {
    let start_time = std::time::Instant::now();
    let mut last_error = None;

    while start_time.elapsed() <= timeout {
        match tokio::net::TcpStream::connect((host, port)).await {
            Ok(_) => return Ok(()),
            Err(error) => last_error = Some(error.to_string()),
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Err(anyhow::anyhow!(
        "Timed out waiting for TCP port {host}:{port} within {}s. Last error: {}",
        timeout.as_secs(),
        last_error.unwrap_or_else(|| "none".to_string())
    ))
}

#[cfg(test)]
mod tests {
    use super::is_removal_already_in_progress;

    fn docker_error(status_code: u16, message: &str) -> anyhow::Error {
        anyhow::Error::new(bollard::errors::Error::DockerResponseServerError {
            status_code,
            message: message.to_string(),
        })
    }

    #[test]
    fn a_concurrent_removal_is_waited_out() {
        assert!(is_removal_already_in_progress(&docker_error(
            409,
            "removal of container runtime-integration-test-mysql-13306 is already in progress"
        )));
    }

    #[test]
    fn an_unkillable_container_stays_a_failure() {
        // The zombie-reap case: nothing will release this name, so waiting on it
        // would spend the whole poll budget and then fail anyway. It has to
        // surface as the error it is.
        assert!(!is_removal_already_in_progress(&docker_error(
            500,
            "cannot remove container: could not kill container: PID 4291 is zombie and can not be killed"
        )));
    }

    #[test]
    fn an_unrelated_error_stays_a_failure() {
        assert!(!is_removal_already_in_progress(&anyhow::anyhow!(
            "the daemon is not reachable"
        )));
    }
}
