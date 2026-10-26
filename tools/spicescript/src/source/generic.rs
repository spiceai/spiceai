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

//! A source that is any Docker image (`kind: docker`), for systems with no
//! source of their own here, such as MongoDB, `DynamoDB` under `LocalStack`, or
//! Kafka. `source-exec` runs a shell command inside the container and
//! `source-query` returns the lines a command prints, one row each. The
//! script isolates instances itself, by naming its databases, tables or
//! topics after `${sources.<name>.namespace}`.

use std::collections::HashMap;

use anyhow::{Result, bail};
use async_trait::async_trait;
use serde_json::json;

use super::{Source, TextResult};
use crate::{
    docker::{Container, ContainerSpec, free_port, healthcheck},
    generate::Dialect,
    script::SourceSpec,
};

pub struct Generic {
    container: Container,
    host_ports: HashMap<u16, u16>,
}

impl Generic {
    pub async fn start(name: &str, run_id: &str, spec: &SourceSpec) -> Result<Self> {
        let Some(image) = &spec.image else {
            bail!("a `docker` source needs an `image`");
        };
        // On Linux a host-network container's ports are the host's own.
        let shares_host = spec.host_network && cfg!(target_os = "linux");
        let mut host_ports = HashMap::new();
        for port in &spec.ports {
            host_ports.insert(*port, if shares_host { *port } else { free_port()? });
        }
        let ports = |text: &str| {
            let mut text = text.to_string();
            for (container, host) in &host_ports {
                text = text.replace(&format!("${{port.{container}}}"), &host.to_string());
            }
            text
        };
        let env = spec
            .env
            .iter()
            .map(|(k, v)| (k.clone(), ports(v)))
            .collect();
        let command = spec
            .command
            .as_ref()
            .map(|c| c.iter().map(|arg| ports(arg)).collect());
        let container = Container::start_on(
            ContainerSpec {
                name: format!("spicescript-{run_id}-{name}"),
                image: image.clone(),
                env,
                command,
                ports: if shares_host {
                    Vec::new()
                } else {
                    spec.ports.clone()
                },
                healthcheck: spec.ready.as_deref().map(healthcheck),
                network: shares_host.then(|| "host".to_string()),
            },
            if shares_host {
                HashMap::new()
            } else {
                host_ports.clone()
            },
        )
        .await?;
        Ok(Self {
            container,
            host_ports,
        })
    }

    async fn shell(&self, command: &str) -> Result<String> {
        self.container.exec(&["sh", "-c", command]).await
    }
}

#[async_trait]
impl Source for Generic {
    fn container(&self) -> Option<&Container> {
        Some(&self.container)
    }

    async fn create_namespace(&self, _namespace: &str) -> Result<super::Namespace> {
        let mut extra = serde_json::Map::new();
        extra.insert("host".into(), json!("127.0.0.1"));
        extra.insert("host_from_container".into(), json!("host.docker.internal"));
        let ports: serde_json::Map<String, serde_json::Value> = self
            .host_ports
            .iter()
            .map(|(container, host)| (container.to_string(), json!(host)))
            .collect();
        extra.insert("port".into(), serde_json::Value::Object(ports));
        Ok(super::Namespace {
            params: json!({}),
            extra,
        })
    }

    async fn exec(&self, _namespace: &str, command: &str) -> Result<()> {
        self.shell(command).await.map(|_| ())
    }

    async fn query(&self, _namespace: &str, command: &str) -> Result<TextResult> {
        let out = self.shell(command).await?;
        Ok(output_lines(&out))
    }

    fn dialect(&self) -> Dialect {
        Dialect::Postgres
    }

    async fn load(
        &self,
        _namespace: &str,
        _table: &str,
        _columns: &[String],
        _rows: &mut (dyn Iterator<Item = Vec<Option<String>>> + Send),
    ) -> Result<u64> {
        bail!("a docker source is loaded with source-exec commands")
    }
}

/// A command's output as a one-column answer, `line`: one row per line, kept
/// as printed, so blank lines and trailing spaces are part of the answer. A
/// command whose output needs cleaning does it itself.
fn output_lines(out: &str) -> TextResult {
    TextResult {
        columns: vec!["line".into()],
        rows: out.lines().map(|l| vec![Some(l.to_string())]).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::output_lines;

    #[test]
    fn keeps_blank_lines_and_trailing_spaces() {
        let rows: Vec<Option<String>> = output_lines("alpha  \n\n \nbeta\n")
            .rows
            .into_iter()
            .flatten()
            .collect();
        let s = |v: &str| Some(v.to_string());
        assert_eq!(rows, vec![s("alpha  "), s(""), s(" "), s("beta")]);
        assert!(output_lines("").rows.is_empty());
    }
}
