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

//! `ClickHouse`, driven through `clickhouse-client` inside its container. Each
//! instance gets its own database. Answers are read as `TabSeparatedWithNames`,
//! which escapes tabs, newlines and backslashes and writes NULL as `\N`.

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde_json::json;

use super::{Source, TextResult};
use crate::{
    docker::{Container, ContainerSpec, healthcheck},
    generate::Dialect,
};

const IMAGE: &str = "clickhouse/clickhouse-server";
const DEFAULT_VERSION: &str = "24.8";
const USER: &str = "spicescript";
const PASSWORD: &str = "spicescript-secret";

pub struct ClickHouse {
    container: Container,
}

impl ClickHouse {
    pub async fn start(name: &str, run_id: &str, version: Option<&str>) -> Result<Self> {
        let container = Container::start(ContainerSpec {
            name: format!("spicescript-{run_id}-{name}"),
            image: format!("{IMAGE}:{}", version.unwrap_or(DEFAULT_VERSION)),
            env: vec![
                ("CLICKHOUSE_USER".into(), USER.into()),
                ("CLICKHOUSE_PASSWORD".into(), PASSWORD.into()),
                ("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT".into(), "1".into()),
            ],
            command: None,
            ports: vec![9000, 8123],
            healthcheck: Some(healthcheck(&format!(
                "clickhouse-client --user {USER} --password {PASSWORD} --query 'SELECT 1'"
            ))),
            network: None,
        })
        .await?;
        Ok(Self { container })
    }

    fn client(database: &str, query: &str) -> Vec<String> {
        [
            "clickhouse-client",
            "--user",
            USER,
            "--password",
            PASSWORD,
            "--database",
            database,
            "--multiquery",
            "--query",
            query,
        ]
        .map(String::from)
        .to_vec()
    }
}

#[async_trait]
impl Source for ClickHouse {
    fn container(&self) -> Option<&Container> {
        Some(&self.container)
    }

    async fn create_namespace(&self, namespace: &str) -> Result<super::Namespace> {
        let create = format!("CREATE DATABASE {namespace}");
        let cmd = Self::client("default", &create);
        let cmd: Vec<&str> = cmd.iter().map(String::as_str).collect();
        self.container
            .exec(&cmd)
            .await
            .with_context(|| format!("Failed to create database '{namespace}'"))?;
        Ok(super::Namespace::params(json!({
            "clickhouse_host": "127.0.0.1",
            "clickhouse_tcp_port": self.container.host_port(9000)?.to_string(),
            "clickhouse_user": USER,
            "clickhouse_pass": PASSWORD,
            "clickhouse_db": namespace,
            "clickhouse_secure": "false",
        })))
    }

    async fn exec(&self, namespace: &str, sql: &str) -> Result<()> {
        let cmd = Self::client(namespace, sql);
        let cmd: Vec<&str> = cmd.iter().map(String::as_str).collect();
        self.container
            .exec(&cmd)
            .await
            .with_context(|| format!("ClickHouse rejected: {sql}"))
            .map(|_| ())
    }

    async fn query(&self, namespace: &str, sql: &str) -> Result<TextResult> {
        let select = format!(
            "{} FORMAT TabSeparatedWithNames",
            sql.trim().trim_end_matches(';')
        );
        let cmd = Self::client(namespace, &select);
        let cmd: Vec<&str> = cmd.iter().map(String::as_str).collect();
        let text = self
            .container
            .exec(&cmd)
            .await
            .with_context(|| format!("ClickHouse rejected oracle query: {sql}"))?;
        Ok(parse_tsv(&text))
    }

    fn dialect(&self) -> Dialect {
        Dialect::ClickHouse
    }

    /// `INSERT … FORMAT TabSeparated`, streamed to one client in 1 MiB chunks.
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
                for (i, cell) in row.iter().enumerate() {
                    if i > 0 {
                        buffer.push('\t');
                    }
                    match cell {
                        None => buffer.push_str("\\N"),
                        Some(v) => escape_into(&mut buffer, v),
                    }
                }
                buffer.push('\n');
                count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if buffer.len() >= 1 << 20 {
                    break;
                }
            }
            (!buffer.is_empty()).then_some(Ok(buffer.into_bytes()))
        });
        let insert = format!(
            "INSERT INTO {table} ({}) FORMAT TabSeparated",
            columns.join(", ")
        );
        let cmd = Self::client(namespace, &insert);
        let cmd: Vec<&str> = cmd.iter().map(String::as_str).collect();
        self.container
            .exec_with_input(&cmd, &mut chunks)
            .await
            .with_context(|| format!("Failed to load {table}"))?;
        Ok(count.into_inner())
    }
}

fn escape_into(buffer: &mut String, value: &str) {
    for c in value.chars() {
        match c {
            '\\' => buffer.push_str("\\\\"),
            '\t' => buffer.push_str("\\t"),
            '\n' => buffer.push_str("\\n"),
            '\r' => buffer.push_str("\\r"),
            c => buffer.push(c),
        }
    }
}

/// Reads `TabSeparatedWithNames`: `\N` alone is NULL; `\\`, `\t`, `\n`, `\r`
/// and `\0` are escapes.
fn parse_tsv(text: &str) -> TextResult {
    let mut lines = text.lines();
    let columns = lines
        .next()
        .map(|h| h.split('\t').map(unescape).collect())
        .unwrap_or_default();
    let rows = lines
        .map(|line| {
            line.split('\t')
                .map(|cell| (cell != "\\N").then(|| unescape(cell)))
                .collect()
        })
        .collect();
    TextResult { columns, rows }
}

fn unescape(cell: &str) -> String {
    let mut out = String::with_capacity(cell.len());
    let mut chars = cell.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('t') => out.push('\t'),
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('0') => out.push('\0'),
            Some(other) => out.push(other),
            None => out.push('\\'),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::parse_tsv;

    #[test]
    fn tsv_keeps_null_apart_from_text_and_unescapes() {
        let parsed = parse_tsv("id\tv\n1\t\\N\n2\t\\\\N\n3\ta\\tb\\nc\n4\t\n");
        let s = |v: &str| Some(v.to_string());
        assert_eq!(parsed.columns, vec!["id", "v"]);
        assert_eq!(
            parsed.rows,
            vec![
                vec![s("1"), None],
                vec![s("2"), s("\\N")],
                vec![s("3"), s("a\tb\nc")],
                vec![s("4"), s("")],
            ]
        );
    }
}
