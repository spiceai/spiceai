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

//! Iceberg tables behind a REST catalog, stored in an S3-compatible object store (rustfs).
//!
//! Statements and oracle queries run through the `duckdb` CLI (which must be
//! on `PATH`), attached to the catalog with the instance's namespace as its
//! default schema, so a script writes Iceberg with plain SQL — `CREATE TABLE`,
//! `INSERT`, `DELETE`, `UPDATE` — and the answers Spice gives are checked
//! against `DuckDB` reading the same tables, which shares no code with Spice's
//! Iceberg reader.
//!
//! `${sources.<name>.params}` holds the `iceberg_s3_*` dataset parameters and
//! `${sources.<name>.catalog}` the catalog URL, so a dataset reads
//! `iceberg:${sources.<name>.catalog}/v1/namespaces/${sources.<name>.namespace}/tables/<table>`.

use std::fmt::Write as _;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde_json::json;

use super::{Namespace, Source, TextResult, duckdb_cli};
use crate::{
    docker::{Container, ContainerSpec, healthcheck},
    generate::Dialect,
};

const STORE_IMAGE: &str = "rustfs/rustfs:1.0.1";
const CATALOG_IMAGE: &str = "apache/iceberg-rest-fixture:latest";
const USER: &str = "spicescript";
const PASSWORD: &str = "spicescript-secret";
const WAREHOUSE: &str = "warehouse";

pub struct Iceberg {
    store: Container,
    catalog: Container,
    /// Where generated part files are written before `DuckDB` loads them.
    staging: std::path::PathBuf,
}

/// Creates the warehouse bucket with the `aws` CLI on the host, retrying while
/// the store comes up.
async fn create_bucket(endpoint: &str) -> Result<()> {
    let mut last = String::new();
    for _ in 0..40 {
        let out = tokio::process::Command::new("aws")
            .args([
                "s3api",
                "create-bucket",
                "--bucket",
                WAREHOUSE,
                "--endpoint-url",
                endpoint,
            ])
            .env("AWS_ACCESS_KEY_ID", USER)
            .env("AWS_SECRET_ACCESS_KEY", PASSWORD)
            .env("AWS_DEFAULT_REGION", "us-east-1")
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .output()
            .await
            .context("Failed to run the `aws` CLI, which the iceberg source needs")?;
        if out.status.success() {
            return Ok(());
        }
        last = String::from_utf8_lossy(&out.stderr).trim().to_string();
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    anyhow::bail!("Failed to create the warehouse bucket: {last}")
}

impl Iceberg {
    pub async fn start(name: &str, run_id: &str, staging: std::path::PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&staging)?;
        let network = format!("spicescript-{run_id}-{name}");
        // The catalog reaches the store by this name in a URL, where `_` is
        // not a valid host character.
        let store_name = format!("spicescript-{run_id}-{name}-store").replace('_', "-");
        let store = Container::start(ContainerSpec {
            name: store_name.clone(),
            image: STORE_IMAGE.into(),
            env: vec![
                ("RUSTFS_ACCESS_KEY".into(), USER.into()),
                ("RUSTFS_SECRET_KEY".into(), PASSWORD.into()),
            ],
            command: Some(vec!["/data".into()]),
            ports: vec![9000],
            healthcheck: Some(healthcheck("curl -fs http://127.0.0.1:9000/health")),
            network: Some(network.clone()),
        })
        .await?;
        create_bucket(&format!("http://127.0.0.1:{}", store.host_port(9000)?)).await?;
        let catalog = Container::start(ContainerSpec {
            name: format!("spicescript-{run_id}-{name}-catalog"),
            image: CATALOG_IMAGE.into(),
            env: vec![
                ("AWS_ACCESS_KEY_ID".into(), USER.into()),
                ("AWS_SECRET_ACCESS_KEY".into(), PASSWORD.into()),
                ("AWS_REGION".into(), "us-east-1".into()),
                ("CATALOG_WAREHOUSE".into(), format!("s3://{WAREHOUSE}/")),
                (
                    "CATALOG_IO__IMPL".into(),
                    "org.apache.iceberg.aws.s3.S3FileIO".into(),
                ),
                (
                    "CATALOG_S3_ENDPOINT".into(),
                    format!("http://{store_name}:9000"),
                ),
                ("CATALOG_S3_PATH__STYLE__ACCESS".into(), "true".into()),
            ],
            command: None,
            ports: vec![8181],
            healthcheck: None,
            network: Some(network),
        })
        .await?;
        let this = Self {
            store,
            catalog,
            staging,
        };
        this.wait_for_catalog().await?;
        Ok(this)
    }

    fn catalog_url(&self) -> Result<String> {
        Ok(format!(
            "http://127.0.0.1:{}",
            self.catalog.host_port(8181)?
        ))
    }

    fn s3_endpoint(&self) -> Result<String> {
        Ok(format!("127.0.0.1:{}", self.store.host_port(9000)?))
    }

    async fn wait_for_catalog(&self) -> Result<()> {
        let url = format!("{}/v1/config", self.catalog_url()?);
        let client = reqwest::Client::new();
        for _ in 0..240 {
            if client
                .get(&url)
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
            {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
        bail!("the Iceberg REST catalog did not answer {url}")
    }

    /// Runs `sql` in `duckdb`, attached to the catalog as `lake` with
    /// `namespace` (when given) as the default schema; returns its output.
    async fn duckdb(&self, namespace: Option<&str>, sql: &str, output: &str) -> Result<String> {
        // The prelude's own results (`CREATE SECRET` prints one) are discarded.
        let mut prelude = format!(
            "INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;\n\
             CREATE SECRET (TYPE S3, KEY_ID '{USER}', SECRET '{PASSWORD}', ENDPOINT '{}', \
             URL_STYLE 'path', USE_SSL false, REGION 'us-east-1');\n\
             ATTACH '{WAREHOUSE}' AS lake (TYPE iceberg, ENDPOINT '{}', AUTHORIZATION_TYPE 'none');",
            self.s3_endpoint()?,
            self.catalog_url()?
        );
        if let Some(namespace) = namespace {
            let _ = write!(prelude, "\nUSE lake.{namespace};");
        }
        duckdb_cli::run("duckdb", &prelude, output, sql).await
    }
}

#[async_trait]
impl Source for Iceberg {
    fn container(&self) -> Option<&Container> {
        Some(&self.catalog)
    }

    async fn remove(&self) -> Result<()> {
        let catalog = self.catalog.remove().await;
        let store = self.store.remove().await;
        catalog?;
        store?;
        // Both containers are gone, so the network they shared can go too.
        self.store.remove_network().await
    }

    async fn create_namespace(&self, namespace: &str) -> Result<Namespace> {
        self.duckdb(
            None,
            &format!("CREATE SCHEMA IF NOT EXISTS lake.{namespace}"),
            "",
        )
        .await
        .with_context(|| format!("Failed to create Iceberg namespace '{namespace}'"))?;
        let mut extra = serde_json::Map::new();
        extra.insert("catalog".into(), json!(self.catalog_url()?));
        Ok(Namespace {
            params: json!({
                "iceberg_s3_endpoint": format!("http://{}", self.s3_endpoint()?),
                "iceberg_s3_access_key_id": USER,
                "iceberg_s3_secret_access_key": PASSWORD,
                "iceberg_s3_region": "us-east-1",
            }),
            extra,
        })
    }

    async fn exec(&self, namespace: &str, sql: &str) -> Result<()> {
        self.duckdb(Some(namespace), sql, "").await.map(|_| ())
    }

    async fn query(&self, namespace: &str, sql: &str) -> Result<TextResult> {
        let text = self
            .duckdb(Some(namespace), &duckdb_cli::copy_to_stdout(sql), "")
            .await?;
        duckdb_cli::parse(&text)
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
        bail!("an Iceberg table is written with `generate` or `source-exec`")
    }

    async fn generate(&self, namespace: &str, spec: &crate::generate::TableSpec) -> Result<u64> {
        duckdb_cli::generate(self, &self.staging, namespace, spec).await
    }
}
