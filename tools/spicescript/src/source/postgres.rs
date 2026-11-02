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

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde_json::json;

use super::{Source, TextResult};
use crate::docker::{Container, ContainerSpec, healthcheck};

pub(super) const PASSWORD: &str = "spicescript";

pub struct Postgres {
    container: Container,
}

impl Postgres {
    /// Starts Postgres with logical replication enabled, so the same source
    /// serves full, append and CDC refresh.
    pub async fn start(name: &str, run_id: &str, version: Option<&str>) -> Result<Self> {
        let container = Container::start(ContainerSpec {
            name: format!("spicescript-{run_id}-{name}"),
            image: super::image("postgres", version)?,
            env: vec![("POSTGRES_PASSWORD".into(), PASSWORD.into())],
            command: Some(
                [
                    "postgres",
                    "-c",
                    "wal_level=logical",
                    "-c",
                    "max_replication_slots=64",
                    "-c",
                    "max_wal_senders=64",
                    "-c",
                    "max_connections=500",
                ]
                .map(String::from)
                .to_vec(),
            ),
            ports: vec![5432],
            healthcheck: Some(healthcheck("pg_isready -U postgres -h 127.0.0.1")),
            network: None,
        })
        .await?;
        Ok(Self { container })
    }

    /// Runs `psql` against `database` inside the container, with `args`
    /// after the connection options.
    async fn psql(&self, database: &str, args: &[&str]) -> Result<String> {
        self.container.exec(&psql_command(database, args)).await
    }
}

/// The `psql` command line for `database`, stopping at the first error.
fn psql_command<'a>(database: &'a str, args: &[&'a str]) -> Vec<&'a str> {
    let mut cmd = vec![
        "psql",
        "-X",
        "-q",
        "-v",
        "ON_ERROR_STOP=1",
        "-U",
        "postgres",
        "-d",
        database,
    ];
    cmd.extend_from_slice(args);
    cmd
}

#[async_trait]
impl Source for Postgres {
    fn container(&self) -> Option<&Container> {
        Some(&self.container)
    }

    async fn create_namespace(&self, namespace: &str) -> Result<super::Namespace> {
        self.psql(
            "postgres",
            &["-c", &format!("CREATE DATABASE \"{namespace}\"")],
        )
        .await
        .with_context(|| format!("Failed to create database '{namespace}'"))?;
        Ok(super::Namespace::params(json!({
            "pg_host": "127.0.0.1",
            "pg_port": self.container.host_port(5432)?.to_string(),
            "pg_user": "postgres",
            "pg_pass": PASSWORD,
            "pg_db": namespace,
            "pg_sslmode": "disable",
        })))
    }

    /// `psql -c` sends the statements as one query string, so they run as one
    /// implicit transaction unless they say otherwise, as an application's
    /// multi-statement query would.
    async fn exec(&self, namespace: &str, sql: &str) -> Result<()> {
        self.psql(namespace, &["-c", sql])
            .await
            .with_context(|| format!("Postgres rejected: {sql}"))
            .map(|_| ())
    }

    /// The answer as `COPY … TO STDOUT` CSV, which quotes an empty string and
    /// leaves NULL unquoted, so the two stay apart.
    async fn query(&self, namespace: &str, sql: &str) -> Result<TextResult> {
        let select = sql.trim().trim_end_matches(';');
        let text = self
            .psql(
                namespace,
                &[
                    "-c",
                    &format!("COPY ({select}) TO STDOUT WITH (FORMAT csv, HEADER)"),
                ],
            )
            .await
            .with_context(|| {
                format!("Postgres rejected oracle query (it must be one query): {sql}")
            })?;
        let mut records = super::parse_csv(&text)?.into_iter();
        let columns = records
            .next()
            .context("COPY printed no header")?
            .into_iter()
            .map(Option::unwrap_or_default)
            .collect();
        let rows = records.map(|row| row.into_iter().collect()).collect();
        Ok(TextResult { columns, rows })
    }

    async fn apply(
        &self,
        namespace: &str,
        table: &str,
        key: &str,
        changes: &[super::write::RowChange],
    ) -> Result<()> {
        let sql = super::write::sql_changes(table, key, changes, self.dialect())?;
        if sql.is_empty() {
            return Ok(());
        }
        let mut input = std::iter::once(Ok(sql.into_bytes()));
        self.container
            .exec_with_input(&psql_command(namespace, &[]), &mut input)
            .await?;
        Ok(())
    }

    fn dialect(&self) -> crate::generate::Dialect {
        crate::generate::Dialect::Postgres
    }

    /// `COPY … FROM STDIN` in text format, streamed to `psql` in 1 MiB chunks.
    async fn load(
        &self,
        namespace: &str,
        table: &str,
        columns: &[String],
        rows: &mut (dyn Iterator<Item = Vec<Option<String>>> + Send),
    ) -> Result<u64> {
        let count = std::sync::atomic::AtomicU64::new(0);
        let mut chunks = std::iter::from_fn(|| {
            let mut buffer = String::with_capacity(1 << 20);
            for row in &mut *rows {
                copy_text_row(&mut buffer, &row);
                count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if buffer.len() >= 1 << 20 {
                    break;
                }
            }
            (!buffer.is_empty()).then_some(Ok(buffer.into_bytes()))
        });
        let copy = format!("COPY {table} ({}) FROM STDIN", columns.join(", "));
        self.container
            .exec_with_input(&psql_command(namespace, &["-c", &copy]), &mut chunks)
            .await
            .with_context(|| format!("Failed to load {table}"))?;
        Ok(count.into_inner())
    }
}

/// Appends one row in `COPY` text format.
fn copy_text_row(buffer: &mut String, row: &[Option<String>]) {
    for (i, cell) in row.iter().enumerate() {
        if i > 0 {
            buffer.push('\t');
        }
        match cell {
            None => buffer.push_str("\\N"),
            Some(v) => {
                for c in v.chars() {
                    match c {
                        '\\' => buffer.push_str("\\\\"),
                        '\t' => buffer.push_str("\\t"),
                        '\n' => buffer.push_str("\\n"),
                        '\r' => buffer.push_str("\\r"),
                        c => buffer.push(c),
                    }
                }
            }
        }
    }
    buffer.push('\n');
}
