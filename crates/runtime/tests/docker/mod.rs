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
        Config, CreateContainerOptions, LogOutput, RemoveContainerOptions, StartContainerOptions,
    },
    exec::{CreateExecOptions, StartExecResults},
    image::CreateImageOptions,
    secret::{ContainerStateStatusEnum, HealthConfig, HealthStatusEnum, HostConfig, PortBinding},
};
use futures::StreamExt;
use parking_lot::RwLock;
use tokio::sync::Semaphore;

// Hold the permit for the container's lifetime to bound the services' memory use.
static CONTAINER_SEMAPHORE: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(3)));
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(30);

#[cfg(test)]
mod tests;

pub struct RunningContainer {
    name: String,
    // Only the ID returned by create_container is used for lifecycle operations.
    id: String,
    docker: Docker,
    ports: RwLock<HashMap<u16, u16>>,
    removed: AtomicBool,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl RunningContainer {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    /// The host port Docker allocated atomically for this container's TCP port.
    pub fn host_port(&self, container_port: u16) -> Result<u16, anyhow::Error> {
        self.ports
            .read()
            .get(&container_port)
            .copied()
            .filter(|port| *port != 0)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Test container {} ({}) has no published TCP port {container_port}",
                    self.name,
                    self.id
                )
            })
    }

    pub async fn remove(&self) -> Result<(), anyhow::Error> {
        if self.removed.load(Ordering::Relaxed) {
            return Ok(());
        }
        remove(&self.docker, &self.id).await?;
        self.removed.store(true, Ordering::Relaxed);
        Ok(())
    }

    pub async fn stop(&self) -> Result<(), anyhow::Error> {
        Ok(self.docker.stop_container(&self.id, None).await?)
    }

    pub async fn start(&self) -> Result<(), anyhow::Error> {
        self.docker
            .start_container(&self.id, None::<StartContainerOptions<String>>)
            .await?;
        let inspected = self.docker.inspect_container(&self.id, None).await?;
        let bindings = inspected
            .network_settings
            .and_then(|settings| settings.ports)
            .unwrap_or_default();
        let mut ports = self.ports.write();
        for (port, host_port) in ports.iter_mut() {
            *host_port = bindings
                .get(&format!("{port}/tcp"))
                .and_then(Option::as_ref)
                .and_then(|bindings| bindings.first())
                .and_then(|binding| binding.host_port.as_ref())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Docker did not publish TCP port {port} for {} ({})",
                        self.name,
                        self.id
                    )
                })?
                .parse::<u16>()?;
            anyhow::ensure!(
                *host_port != 0,
                "Docker returned an unallocated port for {} ({})",
                self.name,
                self.id
            );
        }
        Ok(())
    }

    pub async fn exec_cmd(&self, cmd: &str) -> Result<String, anyhow::Error> {
        self.exec(cmd.split_whitespace()).await
    }

    /// Execute an argument vector, preserving shell scripts and quoted arguments.
    pub async fn exec<I, S>(&self, cmd: I) -> Result<String, anyhow::Error>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let exec = self
            .docker
            .create_exec(
                &self.id,
                CreateExecOptions {
                    attach_stdout: Some(true),
                    attach_stderr: Some(true),
                    cmd: Some(cmd.into_iter().map(Into::into).collect::<Vec<String>>()),
                    ..Default::default()
                },
            )
            .await?;
        let mut text = String::new();
        if let StartExecResults::Attached { mut output, .. } =
            self.docker.start_exec(&exec.id, None).await?
        {
            while let Some(log) = output.next().await {
                match log? {
                    LogOutput::StdOut { message } | LogOutput::StdErr { message } => {
                        text.push_str(&String::from_utf8_lossy(&message));
                    }
                    _ => {}
                }
            }
        }
        let result = self.docker.inspect_exec(&exec.id).await?;
        anyhow::ensure!(
            result.exit_code == Some(0),
            "Command in test container {} ({}) failed with exit {:?}: {text}",
            self.name,
            self.id,
            result.exit_code
        );
        Ok(text)
    }

    pub async fn wait_healthy(&self, timeout: Option<Duration>) -> Result<(), anyhow::Error> {
        let timeout = timeout.unwrap_or_else(|| Duration::from_mins(1));
        let mut last_state = None;
        let result = tokio::time::timeout(timeout, async {
            loop {
                last_state = self.docker.inspect_container(&self.id, None).await?.state;
                if let Some(state) = &last_state {
                    anyhow::ensure!(
                        !matches!(
                            state.status,
                            Some(ContainerStateStatusEnum::EXITED | ContainerStateStatusEnum::DEAD)
                        ),
                        "Test container {} ({}) exited: {state:?}",
                        self.name,
                        self.id
                    );
                    if state.status == Some(ContainerStateStatusEnum::RUNNING)
                        && state
                            .health
                            .as_ref()
                            .is_none_or(|health| health.status == Some(HealthStatusEnum::HEALTHY))
                    {
                        return Ok(());
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
        result.unwrap_or_else(|_| {
            Err(anyhow::anyhow!(
                "Test container {} ({}) was not healthy within {timeout:?}: {last_state:?}",
                self.name,
                self.id
            ))
        })
    }
}

/// Cleanup has its own runtime so it also runs while a test runtime is unwinding.
/// A hard-killed process or a guard stored in a static cannot run `Drop`.
impl Drop for RunningContainer {
    fn drop(&mut self) {
        if *self.removed.get_mut() {
            return;
        }
        let docker = self.docker.clone();
        let id = self.id.clone();
        let removal = std::thread::Builder::new()
            .name(format!("cleanup-{}", self.name))
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                runtime.block_on(remove(&docker, &id))
            });
        match removal.map(std::thread::JoinHandle::join) {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(e))) => eprintln!(
                "failed to remove test container {} ({}): {e}",
                self.name, self.id
            ),
            Ok(Err(_)) => eprintln!(
                "cleanup thread for test container {} ({}) panicked",
                self.name, self.id
            ),
            Err(e) => eprintln!(
                "could not start cleanup for test container {} ({}): {e}",
                self.name, self.id
            ),
        }
    }
}

fn has_status(error: &bollard::errors::Error, status: u16) -> bool {
    matches!(error, bollard::errors::Error::DockerResponseServerError { status_code, .. } if *status_code == status)
}

// Wait for this ID to disappear, including when another removal of this same ID
// is already in progress. Never find or remove a container by its logical name.
async fn remove(docker: &Docker, id: &str) -> Result<(), anyhow::Error> {
    tokio::time::timeout(CLEANUP_TIMEOUT, async {
        if let Err(error) = docker
            .remove_container(
                id,
                Some(RemoveContainerOptions {
                    force: true,
                    v: true, // Anonymous data volumes belong to the container as well.
                    ..Default::default()
                }),
            )
            .await
        {
            if has_status(&error, 404) {
                return Ok(());
            }
            if !has_status(&error, 409) {
                return Err(error.into());
            }
        }
        loop {
            match docker.inspect_container(id, None).await {
                Err(error) if has_status(&error, 404) => return Ok(()),
                Err(error) => return Err(error.into()),
                Ok(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    })
    .await
    .map_err(|_| {
        anyhow::anyhow!("Timed out after {CLEANUP_TIMEOUT:?} removing test container {id}")
    })?
}

pub struct ContainerRunnerBuilder {
    name: String,
    image: Option<String>,
    ports: Vec<u16>,
    env_vars: Vec<(String, String)>,
    healthcheck: Option<HealthConfig>,
    command: Option<Vec<String>>,
    entrypoint: Option<Vec<String>>,
}

impl ContainerRunnerBuilder {
    pub fn new(name: &str) -> Self {
        Self {
            name: format!("{name}-{}", uuid::Uuid::new_v4()),
            image: None,
            ports: Vec::new(),
            env_vars: Vec::new(),
            healthcheck: None,
            command: None,
            entrypoint: None,
        }
    }

    pub fn image(mut self, image: String) -> Self {
        self.image = Some(image);
        self
    }

    /// Publish on loopback with a daemon-assigned host port; read it from the guard.
    pub fn publish_port(mut self, container_port: u16) -> Self {
        self.ports.push(container_port);
        self
    }

    pub fn add_env_var(mut self, key: &str, value: &str) -> Self {
        self.env_vars.push((key.into(), value.into()));
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

    pub fn entrypoint<I, S>(mut self, entrypoint: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.entrypoint = Some(entrypoint.into_iter().map(Into::into).collect());
        self
    }

    pub fn build(self) -> Result<ContainerRunner, anyhow::Error> {
        anyhow::ensure!(self.image.is_some(), "Image must be set");
        Ok(ContainerRunner {
            builder: self,
            docker: Docker::connect_with_local_defaults()?,
        })
    }
}

pub struct ContainerRunner {
    builder: ContainerRunnerBuilder,
    docker: Docker,
}

impl ContainerRunner {
    pub async fn run(self, timeout: Option<Duration>) -> Result<RunningContainer, anyhow::Error> {
        let container = self.start().await?;
        container.wait_healthy(timeout).await?;
        Ok(container)
    }

    /// Start and discover published ports before waiting for health. Services
    /// advertising an external endpoint can configure it while Docker owns the port.
    pub async fn start(self) -> Result<RunningContainer, anyhow::Error> {
        let permit = tokio::time::timeout(
            Duration::from_mins(5),
            Arc::clone(&CONTAINER_SEMAPHORE).acquire_owned(),
        )
        .await
        .map_err(|_| anyhow::anyhow!("Timed out waiting for available container slot"))??;
        self.pull_image().await?;
        let builder = self.builder;
        let ports = builder
            .ports
            .iter()
            .map(|port| {
                (
                    format!("{port}/tcp"),
                    Some(vec![PortBinding {
                        host_ip: Some("127.0.0.1".into()),
                        host_port: Some("0".into()),
                    }]),
                )
            })
            .collect::<HashMap<_, _>>();
        #[expect(clippy::zero_sized_map_values)]
        let exposed_ports = ports
            .keys()
            .map(|key| (key.clone(), HashMap::new()))
            .collect();
        let config = Config::<String> {
            image: builder.image,
            env: Some(
                builder
                    .env_vars
                    .iter()
                    .map(|(key, value)| format!("{key}={value}"))
                    .collect(),
            ),
            host_config: Some(HostConfig {
                port_bindings: Some(ports),
                init: Some(true), // Reap children so force-removal can kill the service.
                ..Default::default()
            }),
            exposed_ports: Some(exposed_ports),
            healthcheck: builder.healthcheck,
            cmd: builder.command,
            entrypoint: builder.entrypoint,
            ..Default::default()
        };
        let created = self
            .docker
            .create_container(
                Some(CreateContainerOptions {
                    name: builder.name.as_str(),
                    platform: None,
                }),
                config,
            )
            .await?;
        // Establish ownership before start, inspection or readiness can fail.
        let container = RunningContainer {
            name: builder.name,
            id: created.id,
            docker: self.docker,
            ports: RwLock::new(builder.ports.into_iter().map(|port| (port, 0)).collect()),
            removed: AtomicBool::new(false),
            _permit: permit,
        };
        container.start().await?;
        tracing::debug!(name = container.name, id = container.id, ports = ?container.ports, "Started test container");
        Ok(container)
    }

    async fn pull_image(&self) -> Result<(), anyhow::Error> {
        let image = self
            .builder
            .image
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Image must be set"))?;
        if self
            .docker
            .list_images::<&str>(None)
            .await?
            .iter()
            .any(|entry| entry.repo_tags.contains(image))
        {
            return Ok(());
        }
        let options = Some(CreateImageOptions {
            from_image: image.as_str(),
            ..Default::default()
        });
        let mut pulling = self.docker.create_image(options, None, None);
        while let Some(event) = pulling.next().await {
            tracing::debug!("Pulling image: {:?}", event?);
        }
        Ok(())
    }
}

pub async fn is_docker_available() -> bool {
    let Ok(docker) = Docker::connect_with_local_defaults() else {
        return false;
    };
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
        "Timed out waiting for TCP port {host}:{port} within {timeout:?}. Last error: {last_error:?}"
    ))
}
