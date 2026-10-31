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

//! `DuckDB` files written and queried through the CLI matching Spice's
//! embedded release. Each namespace has its own canonical database path.
//!
//! `DuckDB` permits one writer or multiple read-only processes. Writes must
//! happen before `start` or while `spiced` is stopped; oracle queries attach
//! read-only so they can coexist with Spice's read-only connection.
//!
//! `${sources.<name>.params}` holds `duckdb_open` for `duckdb:<table>`.
//! `.path` holds the file path, and `.adbc_params` holds `adbc_driver`,
//! `adbc_uri` and `adbc_driver_path` for `adbc:<table>`.
//!
//! `.library_dir` is passed to `spiced` through `LD_LIBRARY_PATH` on Linux and
//! `DYLD_FALLBACK_LIBRARY_PATH` on macOS, preserving existing search paths.
//! The runner launches `spiced` directly, so macOS does not strip `DYLD_*`.
//! This lets `duckdb:` load the matching installed `libduckdb` by name.
//! Direct `adbc:` opens read-write; its CLI oracle must run before `start` or
//! after `stop`, since it cannot share the writer's file lock.

use std::path::PathBuf;

use anyhow::{Result, bail};
use async_trait::async_trait;
use serde_json::json;

use super::{Namespace, Source, TextResult, duckdb_cli};
use crate::{docker::Container, generate::Dialect};

pub struct DuckDb {
    dir: PathBuf,
    cli: String,
    library: String,
}

impl DuckDb {
    pub async fn start(dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&dir)?;
        let near = std::env::current_dir()?;
        Ok(Self {
            dir: std::fs::canonicalize(dir)?,
            cli: duckdb_cli::embedded(&near, "SPICESCRIPT_DUCKDB_CLI").await?,
            library: duckdb_cli::embedded_library(&near).await?,
        })
    }

    fn path(&self, namespace: &str) -> PathBuf {
        self.dir.join(format!("{namespace}.duckdb"))
    }

    async fn run(&self, namespace: &str, sql: &str, read_only: bool) -> Result<String> {
        let path = self
            .path(namespace)
            .display()
            .to_string()
            .replace('\'', "''");
        let mode = if read_only { " (READ_ONLY)" } else { "" };
        let prelude = format!("ATTACH '{path}' AS db{mode}; USE db;");
        duckdb_cli::run(&self.cli, &prelude, "", sql).await
    }
}

#[async_trait]
impl Source for DuckDb {
    fn container(&self) -> Option<&Container> {
        None
    }

    async fn create_namespace(&self, namespace: &str) -> Result<Namespace> {
        self.run(namespace, "SELECT 1", false).await?;
        let path = std::fs::canonicalize(self.path(namespace))?;
        let mut result = Namespace::params(json!({ "duckdb_open": path }));
        result.extra.insert("path".into(), json!(path));
        let library_dir = std::path::Path::new(&self.library)
            .parent()
            .ok_or_else(|| {
                anyhow::anyhow!("DuckDB library has no parent directory: {}", self.library)
            })?;
        result
            .extra
            .insert("library_dir".into(), json!(library_dir));
        result.extra.insert(
            "adbc_params".into(),
            json!({
                "adbc_driver": "duckdb",
                "adbc_uri": path,
                "adbc_driver_path": self.library,
            }),
        );
        Ok(result)
    }

    async fn exec(&self, namespace: &str, sql: &str) -> Result<()> {
        self.run(namespace, sql, false).await.map(|_| ())
    }

    async fn query(&self, namespace: &str, sql: &str) -> Result<TextResult> {
        let text = self
            .run(namespace, &duckdb_cli::copy_to_stdout(sql), true)
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
        bail!("a DuckDB table is written with `generate` or `source-exec`")
    }

    async fn generate(&self, namespace: &str, spec: &crate::generate::TableSpec) -> Result<u64> {
        duckdb_cli::generate(self, &self.dir.join("staging"), namespace, spec).await
    }
}
