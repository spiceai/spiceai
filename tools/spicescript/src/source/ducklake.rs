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

//! `DuckLake` lakes whose catalog is kept in Postgres, so the `duckdb` CLI can
//! write to a lake while Spice reads it (a `DuckDB` file catalog admits one
//! process at a time).
//!
//! Each instance gets its own lake: a Postgres database for the catalog and a
//! directory for the data files, with tables in its `main` schema. Statements
//! and oracle queries run through the `duckdb` CLI of one release, whose
//! library `spiced` loads to read the lake, since a release cannot attach a
//! lake a newer one wrote: the source's `version`, else `--duckdb-version`,
//! else the newest release `test/duckdb_versions.json` lists (fetched once;
//! `SPICESCRIPT_DUCKLAKE_DUCKDB` names another CLI). The catalog runs the
//! default PostgreSQL image.
//!
//! `${sources.<name>.params}` holds `ducklake_connection_string`, so a dataset
//! reads `ducklake:main.<table>`. `.library_dir` is the library's directory,
//! which the runner puts on `spiced`'s library search path, as for `duckdb`
//! sources. Stopping the source stops the catalog.

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde_json::json;

use super::{Namespace, Source, TextResult, duckdb_cli, postgres::Postgres};
use crate::{docker::Container, generate::Dialect};

pub struct DuckLake {
    catalog: Postgres,
    /// Holds each lake's data files, and generated part files before they are
    /// loaded.
    dir: PathBuf,
    duckdb: String,
    library: String,
}

impl DuckLake {
    pub async fn start(
        name: &str,
        run_id: &str,
        version: Option<&str>,
        dir: PathBuf,
    ) -> Result<Self> {
        std::fs::create_dir_all(&dir)?;
        let catalog = Postgres::start(name, run_id, None).await?;
        let version = duckdb_cli::version(version)?;
        Ok(Self {
            catalog,
            dir: std::fs::canonicalize(&dir)?,
            duckdb: duckdb_cli::cli(&version, "SPICESCRIPT_DUCKLAKE_DUCKDB").await?,
            library: duckdb_cli::library(&version).await?,
        })
    }

    fn connection_string(&self, namespace: &str) -> Result<String> {
        let port = self
            .catalog
            .container()
            .context("the DuckLake catalog has no container")?
            .host_port(5432)?;
        Ok(format!(
            "postgres:dbname={namespace} host=127.0.0.1 port={port} user=postgres password={}",
            super::postgres::PASSWORD
        ))
    }

    fn data_dir(&self, namespace: &str) -> PathBuf {
        self.dir.join(namespace).join("data")
    }

    /// Runs `sql` in `duckdb` with the namespace's lake attached as `lake` and
    /// in use; `create` also names the data directory, which creating a lake
    /// needs.
    async fn duckdb(
        &self,
        namespace: &str,
        sql: &str,
        output: &str,
        create: bool,
    ) -> Result<String> {
        let data_path = if create {
            format!(" (DATA_PATH '{}/')", self.data_dir(namespace).display())
        } else {
            String::new()
        };
        let prelude = format!(
            "INSTALL ducklake; LOAD ducklake; INSTALL postgres; LOAD postgres;\n\
             ATTACH 'ducklake:{}' AS lake{data_path};\nUSE lake;",
            self.connection_string(namespace)?
        );
        duckdb_cli::run(&self.duckdb, &prelude, output, sql).await
    }
}

#[async_trait]
impl Source for DuckLake {
    fn container(&self) -> Option<&Container> {
        self.catalog.container()
    }

    async fn create_namespace(&self, namespace: &str) -> Result<Namespace> {
        self.catalog.create_namespace(namespace).await?;
        std::fs::create_dir_all(self.data_dir(namespace))?;
        self.duckdb(namespace, "SELECT 1", "", true)
            .await
            .with_context(|| format!("Failed to create the DuckLake lake for '{namespace}'"))?;
        let mut result = Namespace::params(
            json!({ "ducklake_connection_string": self.connection_string(namespace)? }),
        );
        let library_dir = std::path::Path::new(&self.library)
            .parent()
            .with_context(|| format!("DuckDB library has no parent directory: {}", self.library))?;
        result
            .extra
            .insert("library_dir".into(), json!(library_dir));
        Ok(result)
    }

    async fn exec(&self, namespace: &str, sql: &str) -> Result<()> {
        self.duckdb(namespace, sql, "", false).await.map(|_| ())
    }

    async fn query(&self, namespace: &str, sql: &str) -> Result<TextResult> {
        let text = self
            .duckdb(namespace, &duckdb_cli::copy_to_stdout(sql), "", false)
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
        bail!("a DuckLake table is written with `generate` or `source-exec`")
    }

    async fn generate(&self, namespace: &str, spec: &crate::generate::TableSpec) -> Result<u64> {
        duckdb_cli::generate(self, &self.dir.join("staging"), namespace, spec).await
    }
}
