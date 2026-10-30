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

//! `DuckDB` database files, read by Spice's `duckdb:` connector.
//!
//! Each instance gets its own file. Statements and oracle queries run through
//! the `duckdb` CLI of the release Spice embeds (fetched once;
//! `SPICESCRIPT_DUCKDB` names another), so Spice reads a file in the storage
//! format it was written in.
//!
//! Spice opens the file read-only, and `DuckDB` lets several processes read a
//! file but only one write it, and none while another reads. So oracle queries
//! run read-only alongside `spiced`, and writes (`source-exec`, `generate`)
//! happen while `spiced` is stopped: before `start`, or between `stop` and the
//! next `start`.
//!
//! `${sources.<name>.params}` holds `duckdb_open`, so a dataset reads
//! `duckdb:<table>`.

use std::path::PathBuf;

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde_json::json;

use super::{Namespace, Source, TextResult, duckdb_cli};
use crate::{docker::Container, generate::Dialect};

pub struct DuckDb {
    /// Holds each namespace's database file, and generated part files before
    /// they are loaded.
    dir: PathBuf,
    duckdb: String,
}

impl DuckDb {
    pub async fn start(dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&dir)?;
        let duckdb =
            duckdb_cli::embedded(&std::env::current_dir()?, "SPICESCRIPT_DUCKDB").await?;
        Ok(Self {
            dir: std::fs::canonicalize(&dir)?,
            duckdb,
        })
    }

    fn file(&self, namespace: &str) -> PathBuf {
        self.dir.join(format!("{namespace}.duckdb"))
    }

    /// Runs `sql` in `duckdb` against the namespace's file, read-only unless
    /// `write`.
    async fn duckdb(&self, namespace: &str, sql: &str, write: bool) -> Result<String> {
        let mode = if write { "" } else { " (READ_ONLY)" };
        let prelude = format!(
            "ATTACH '{}' AS db{mode};\nUSE db;",
            self.file(namespace).display()
        );
        duckdb_cli::run(&self.duckdb, &prelude, "", sql).await
    }
}

#[async_trait]
impl Source for DuckDb {
    fn container(&self) -> Option<&Container> {
        None
    }

    async fn create_namespace(&self, namespace: &str) -> Result<Namespace> {
        self.duckdb(namespace, "SELECT 1", true)
            .await
            .with_context(|| format!("Failed to create the DuckDB file for '{namespace}'"))?;
        let path = self.file(namespace);
        Ok(Namespace {
            params: json!({ "duckdb_open": path.display().to_string() }),
            extra: serde_json::Map::from_iter([(
                "path".to_string(),
                json!(path.display().to_string()),
            )]),
        })
    }

    async fn exec(&self, namespace: &str, sql: &str) -> Result<()> {
        self.duckdb(namespace, sql, true).await.map(|_| ()).context(
            "a DuckDB source is written while spiced is stopped (before `start`, or after `stop`): \
             DuckDB lets no process write a file another one has open",
        )
    }

    async fn query(&self, namespace: &str, sql: &str) -> Result<TextResult> {
        let text = self
            .duckdb(namespace, &duckdb_cli::copy_to_stdout(sql), false)
            .await?;
        duckdb_cli::parse(&text)
    }

    fn dialect(&self) -> Dialect {
        Dialect::Postgres
    }

    /// Stages the rows as a CSV file, every value quoted and NULL an unquoted
    /// `\N`, so an empty string stays apart from NULL; then copies it in.
    async fn load(
        &self,
        namespace: &str,
        table: &str,
        columns: &[String],
        rows: &mut (dyn Iterator<Item = Vec<Option<String>>> + Send),
    ) -> Result<u64> {
        let dir = self.dir.join("staging").join(namespace);
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(format!("{table}.csv"));
        let mut csv = String::new();
        let mut count = 0u64;
        for row in rows {
            let cells: Vec<String> = row
                .into_iter()
                .map(|cell| match cell {
                    Some(v) => format!("\"{}\"", v.replace('"', "\"\"")),
                    None => "\\N".to_string(),
                })
                .collect();
            csv.push_str(&cells.join(","));
            csv.push('\n');
            count += 1;
        }
        tokio::fs::write(&path, csv).await?;
        self.exec(
            namespace,
            &format!(
                "COPY {table} ({}) FROM '{}' (FORMAT csv, HEADER false, NULLSTR '\\N', ALLOW_QUOTED_NULLS false)",
                columns.join(", "),
                path.display()
            ),
        )
        .await?;
        Ok(count)
    }

    async fn generate(&self, namespace: &str, spec: &crate::generate::TableSpec) -> Result<u64> {
        duckdb_cli::generate(self, &self.dir.join("staging"), namespace, spec).await
    }
}
