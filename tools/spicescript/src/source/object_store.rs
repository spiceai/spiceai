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

//! An S3-compatible object store with conditional writes, served by rustfs
//! (`kind: rustfs`). Each instance has its own bucket; a generated table is the
//! prefix `<table>/` of part files. Like a files source it answers no queries,
//! so a script mirrors its rows into a database. Buckets and objects are
//! managed with the `aws` CLI on the host, so the image needs no client.
//!
//! `${sources.<name>.params}` holds the `s3_*` parameters an `s3://` dataset
//! needs; `${sources.<name>.bucket}`, `.endpoint`, `.key` and `.secret` serve
//! other uses, such as a Cayenne datalake tier or a state location.
//! `${sources.<name>.shared_bucket}` is one bucket every instance of the run
//! sees, for state that instances share, such as one `runtime.state.location`
//! for two replicas.

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde_json::json;

use super::{Source, TextResult};
use crate::{
    docker::{Container, ContainerSpec, healthcheck},
    generate::{Dialect, TableSpec, WriteMode},
};

const USER: &str = "spicescript";
const PASSWORD: &str = "spicescript-secret";

fn spec(name: String) -> ContainerSpec {
    ContainerSpec {
        name,
        image: "rustfs/rustfs:1.0.1".into(),
        env: vec![
            ("RUSTFS_ACCESS_KEY".into(), USER.into()),
            ("RUSTFS_SECRET_KEY".into(), PASSWORD.into()),
        ],
        command: Some(vec!["/data".into()]),
        ports: vec![9000],
        healthcheck: Some(healthcheck("curl -fs http://127.0.0.1:9000/health")),
        network: None,
    }
}

pub struct ObjectStore {
    container: Container,
    /// Where part files are written before they are uploaded.
    staging: PathBuf,
}

impl ObjectStore {
    pub async fn start(name: &str, run_id: &str, staging: PathBuf) -> Result<Self> {
        let container = Container::start(spec(format!("spicescript-{run_id}-{name}"))).await?;
        std::fs::create_dir_all(&staging)?;
        Ok(Self { container, staging })
    }

    fn endpoint(&self) -> Result<String> {
        Ok(format!(
            "http://127.0.0.1:{}",
            self.container.host_port(9000)?
        ))
    }

    /// Runs the `aws` CLI against the store; returns what it printed.
    async fn aws(&self, args: &[&str]) -> Result<String> {
        let out = tokio::process::Command::new("aws")
            .args(args)
            .args(["--endpoint-url", &self.endpoint()?])
            .env("AWS_ACCESS_KEY_ID", USER)
            .env("AWS_SECRET_ACCESS_KEY", PASSWORD)
            .env("AWS_DEFAULT_REGION", "us-east-1")
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .output()
            .await
            .context("Failed to run the `aws` CLI, which object store sources need")?;
        if !out.status.success() {
            bail!(
                "aws {} failed: {}",
                args.join(" "),
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    }

    /// The objects under `prefix` in `bucket`, by key; none when there are none.
    async fn keys(&self, bucket: &str, prefix: &str) -> Result<Vec<String>> {
        let out = self
            .aws(&[
                "s3api",
                "list-objects-v2",
                "--bucket",
                bucket,
                "--prefix",
                prefix,
                "--query",
                "Contents[].Key",
                "--output",
                "text",
            ])
            .await?;
        Ok(out
            .split_whitespace()
            .filter(|k| *k != "None")
            .map(str::to_string)
            .collect())
    }
}

/// The bucket shared by every instance; no namespace bucket has this name,
/// since those begin with the instance's index.
const SHARED_BUCKET: &str = "shared";

/// Bucket names allow lowercase letters, digits, dots and hyphens.
fn bucket(namespace: &str) -> String {
    namespace.replace('_', "-").to_lowercase()
}

#[async_trait]
impl Source for ObjectStore {
    fn container(&self) -> Option<&Container> {
        Some(&self.container)
    }

    async fn create_namespace(&self, namespace: &str) -> Result<super::Namespace> {
        let bucket = bucket(namespace);
        // The server can answer its health check a moment before it takes
        // requests; retry the first one briefly.
        let mut last = None;
        for _ in 0..20 {
            match self
                .aws(&["s3api", "create-bucket", "--bucket", &bucket])
                .await
            {
                Ok(_) => {
                    last = None;
                    break;
                }
                Err(e) => last = Some(e),
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
        if let Some(e) = last {
            return Err(e).with_context(|| format!("Failed to create bucket '{bucket}'"));
        }
        // Every instance creates the shared bucket; the first one wins.
        if self
            .aws(&["s3api", "create-bucket", "--bucket", SHARED_BUCKET])
            .await
            .is_err()
        {
            self.aws(&["s3api", "head-bucket", "--bucket", SHARED_BUCKET])
                .await
                .with_context(|| format!("Failed to create bucket '{SHARED_BUCKET}'"))?;
        }
        let endpoint = self.endpoint()?;
        let mut extra = serde_json::Map::new();
        extra.insert("bucket".into(), json!(bucket));
        extra.insert("shared_bucket".into(), json!(SHARED_BUCKET));
        extra.insert("endpoint".into(), json!(endpoint));
        extra.insert("key".into(), json!(USER));
        extra.insert("secret".into(), json!(PASSWORD));
        Ok(super::Namespace {
            params: json!({
                "s3_endpoint": endpoint,
                "s3_region": "us-east-1",
                "s3_url_style": "path",
                "s3_auth": "key",
                "s3_key": USER,
                "s3_secret": PASSWORD,
                "allow_http": "true",
            }),
            extra,
        })
    }

    async fn exec(&self, _namespace: &str, sql: &str) -> Result<()> {
        bail!("an object store runs no SQL (`{sql}`); generate objects instead")
    }

    async fn query(&self, _namespace: &str, sql: &str) -> Result<TextResult> {
        bail!("an object store answers no queries (`{sql}`); mirror its rows into a database")
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
        bail!("an object store is written with `generate`")
    }

    async fn list(&self, namespace: &str, prefix: &str) -> Result<Vec<String>> {
        self.keys(&bucket(namespace), prefix).await
    }

    /// Writes the part files locally, then uploads them under `<table>/`,
    /// after removing what is there unless appending.
    async fn generate(&self, namespace: &str, spec: &TableSpec) -> Result<u64> {
        let bucket = bucket(namespace);
        let prefix = format!("{}/", spec.table);
        let existing = self.keys(&bucket, &prefix).await?;
        if spec.mode == WriteMode::Replace && !existing.is_empty() {
            self.aws(&[
                "s3",
                "rm",
                "--recursive",
                &format!("s3://{bucket}/{prefix}"),
            ])
            .await
            .with_context(|| format!("Failed to remove {prefix}"))?;
        }
        let first = if spec.mode == WriteMode::Append {
            existing.len()
        } else {
            0
        };
        let dir = self.staging.join(namespace).join(&spec.table);
        if dir.exists() {
            std::fs::remove_dir_all(&dir)?;
        }
        std::fs::create_dir_all(&dir)?;
        let (written, dir) = {
            let spec = spec.clone();
            tokio::task::spawn_blocking(move || {
                super::files::write_parts(&dir, first, &spec).map(|n| (n, dir))
            })
            .await
            .context("file writer panicked")??
        };
        let dir_text = dir.display().to_string();
        self.aws(&[
            "s3",
            "cp",
            "--recursive",
            "--quiet",
            &dir_text,
            &format!("s3://{bucket}/{prefix}"),
        ])
        .await
        .with_context(|| format!("Failed to upload the parts of {}", spec.table))?;
        Ok(written)
    }
}
