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

use std::collections::HashMap;

use bollard::{
    container::{
        Config, CreateContainerOptions, ListContainersOptions, RemoveContainerOptions,
        StartContainerOptions,
    },
    image::CreateImageOptions,
    secret::{
        ContainerState, ContainerStateStatusEnum, Health, HealthConfig, HealthStatusEnum,
        HostConfig, PortBinding,
    },
    Docker,
};
use futures::StreamExt;
pub struct RunningContainer<'a> {
    name: &'a str,
    docker: Docker,
}

impl RunningContainer<'_> {
    pub async fn remove(&self) -> Result<(), anyhow::Error> {
        remove(&self.docker, self.name).await
    }

    pub async fn stop(&self) -> Result<(), anyhow::Error> {
        stop(&self.docker, self.name).await
    }

    pub async fn start(&self) -> Result<(), anyhow::Error> {
        start(&self.docker, self.name).await
    }
}

pub async fn remove(docker: &Docker, name: &str) -> Result<(), anyhow::Error> {
    Ok(docker
        .remove_container(
            name,
            Some(RemoveContainerOptions {
                force: true,
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
}

impl<'a> ContainerRunnerBuilder<'a> {
    pub fn new(name: &'a str) -> Self {
        ContainerRunnerBuilder {
            name,
            image: None,
            port_bindings: Vec::new(),
            env_vars: Vec::new(),
            healthcheck: None,
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
}

impl<'a> ContainerRunner<'a> {
    pub async fn run(self) -> Result<RunningContainer<'a>, anyhow::Error> {
        if self.container_exist().await? {
            remove(&self.docker, self.name).await?;
        }

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
                    host_port: Some(format!("{host_port}/tcp")),
                }]),
            );
        }
        tracing::debug!("Port bindings: {:?}", port_bindings_map);

        let port_bindings = if port_bindings_map.is_empty() {
            None
        } else {
            Some(port_bindings_map)
        };

        let host_config = Some(HostConfig {
            port_bindings,
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
            ..Default::default()
        };

        let _ = self.docker.create_container(Some(options), config).await?;

        self.docker
            .start_container(self.name, None::<StartContainerOptions<String>>)
            .await?;

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

            if start_time.elapsed().as_secs() > 60 {
                return Err(anyhow::anyhow!("Container failed to start"));
            }

            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        Ok(RunningContainer::<'a> {
            name: self.name,
            docker: self.docker,
        })
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
