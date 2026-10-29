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

//! Data sources a script starts, writes to, and asks for the oracle answer.
//!
//! One container serves every instance of a run; each instance gets its own
//! *namespace* in it (a database, for Postgres), so instances never see each
//! other's writes and can run side by side.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde_json::Value;

use crate::docker::Container;

pub mod clickhouse;
pub mod document;
pub mod duckdb_cli;
pub mod ducklake;
pub mod files;
pub mod generic;
pub mod iceberg;
pub mod mock_http;
pub mod mysql;
pub mod object_store;
pub mod postgres;
pub mod write;

pub struct Namespace {
    pub params: Value,
    pub extra: serde_json::Map<String, Value>,
}

impl Namespace {
    pub fn params(params: Value) -> Self {
        Self {
            params,
            extra: serde_json::Map::new(),
        }
    }
}

/// An answer in the source's own text rendering, before it is typed.
#[derive(Debug, Clone)]
pub struct TextResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
}

#[async_trait]
pub trait Source: Send + Sync {
    /// The source's container; `None` for a source with no server (files).
    fn container(&self) -> Option<&Container>;

    /// Every container the source runs, for saving their logs. A source
    /// running more than its one `container` lists the rest too.
    fn containers(&self) -> Vec<&Container> {
        self.container().into_iter().collect()
    }

    /// Removes everything the source started. A source running more than its
    /// one `container` removes the rest too.
    async fn remove(&self) -> Result<()> {
        match self.container() {
            Some(container) => container.remove().await,
            None => Ok(()),
        }
    }

    /// Creates an isolated namespace and returns the connector `params` a
    /// spicepod needs to read it, exposed as `${sources.<name>.params}`, and
    /// other facts about it (a directory, a bucket) exposed as
    /// `${sources.<name>.<field>}`.
    async fn create_namespace(&self, namespace: &str) -> Result<Namespace>;

    async fn exec(&self, namespace: &str, sql: &str) -> Result<()>;

    async fn query(&self, namespace: &str, sql: &str) -> Result<TextResult>;

    /// The dialect generated tables are created in.
    fn dialect(&self) -> crate::generate::Dialect;

    /// Loads rows into an existing table in bulk. Cells are text, `None` is NULL.
    async fn load(
        &self,
        namespace: &str,
        table: &str,
        columns: &[String],
        rows: &mut (dyn Iterator<Item = Vec<Option<String>>> + Send),
    ) -> Result<u64>;

    /// Applies full row images, preserving the order returned by the mirror.
    async fn apply(
        &self,
        _namespace: &str,
        _table: &str,
        _key: &str,
        _changes: &[write::RowChange],
    ) -> Result<()> {
        anyhow::bail!("this source does not support row-image writes")
    }

    /// The objects (or files) under `prefix` in this namespace.
    async fn list(&self, _namespace: &str, prefix: &str) -> Result<Vec<String>> {
        anyhow::bail!("this source has no objects to list under '{prefix}'")
    }

    /// Writes a generated table: by default creates it (unless appending) and
    /// loads the rows.
    async fn generate(&self, namespace: &str, spec: &crate::generate::TableSpec) -> Result<u64> {
        use crate::generate::WriteMode;
        if spec.mode == WriteMode::Replace {
            self.exec(namespace, &format!("DROP TABLE IF EXISTS {}", spec.table))
                .await?;
            self.exec(
                namespace,
                &crate::generate::create_table(spec, self.dialect()),
            )
            .await?;
        }
        let columns: Vec<String> = spec.columns.iter().map(|c| c.name.clone()).collect();
        let rows = crate::generate::rows(spec)?;
        // MySQL's BOOLEAN is TINYINT(1): it takes 1 and 0, not true and false.
        let bools: Vec<usize> = spec
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| matches!(c.spec.kind, crate::generate::ColumnKind::Bool(_)))
            .map(|(i, _)| i)
            .collect();
        let mysql = self.dialect() == crate::generate::Dialect::Mysql;
        let mut rows = rows.map(move |mut row| {
            if mysql {
                for &i in &bools {
                    if let Some(v) = row[i].as_mut() {
                        *v = if v == "true" { "1".into() } else { "0".into() };
                    }
                }
            }
            row
        });
        self.load(namespace, &spec.table, &columns, &mut rows).await
    }
}

/// Starts a source of `kind`; `files`, `rustfs`, `iceberg` and `ducklake`
/// sources keep their directories under `work`.
pub async fn start(
    name: &str,
    run_id: &str,
    kind: &str,
    version: Option<&str>,
    work: &Path,
    spec: &crate::script::SourceSpec,
) -> Result<Box<dyn Source>> {
    let dir = work.join(name);
    let source: Box<dyn Source> = match kind {
        "kafka" | "mongodb" | "dynamodb" => {
            Box::new(document::Document::start(name, run_id, kind, version).await?)
        }
        "docker" => Box::new(generic::Generic::start(name, run_id, spec).await?),
        "postgres" => Box::new(postgres::Postgres::start(name, run_id, version).await?),
        "mysql" => Box::new(mysql::Mysql::start(name, run_id, version).await?),
        "files" => Box::new(files::Files::new(dir)?),
        "rustfs" => Box::new(object_store::ObjectStore::start(name, run_id, dir).await?),
        "iceberg" => Box::new(iceberg::Iceberg::start(name, run_id, dir).await?),
        "clickhouse" => Box::new(clickhouse::ClickHouse::start(name, run_id, version).await?),
        "mock-http" => Box::new(mock_http::MockHttp::start().await?),
        "ducklake" => Box::new(ducklake::DuckLake::start(name, run_id, version, dir).await?),
        other => anyhow::bail!(
            "unknown source kind '{other}'; expected kafka, mongodb, dynamodb, postgres, mysql, clickhouse, files, rustfs, iceberg, ducklake, mock-http or docker"
        ),
    };
    Ok(source)
}

/// The image for a source: `version` if given, otherwise the newest version
/// `test/source_versions.json` lists.
pub fn image(source: &str, version: Option<&str>) -> Result<String> {
    let path = repo_file("test/source_versions.json")?;
    let manifest: Value = serde_json::from_str(&std::fs::read_to_string(&path)?)
        .with_context(|| format!("Failed to parse {}", path.display()))?;
    let entry = manifest
        .get(source)
        .with_context(|| format!("{} has no '{source}' entry", path.display()))?;
    let image = entry
        .get("image")
        .and_then(Value::as_str)
        .context("source entry has no image")?;
    let version = match version {
        Some(v) => v.to_string(),
        None => entry
            .get("versions")
            .and_then(Value::as_array)
            .and_then(|v| v.last())
            .and_then(|v| v.get("version"))
            .and_then(Value::as_str)
            .context("source entry has no versions")?
            .to_string(),
    };
    Ok(format!("{image}:{version}"))
}

/// Finds `relative` in the current directory or one of its ancestors, so the
/// runner works from anywhere inside the repository.
fn repo_file(relative: &str) -> Result<PathBuf> {
    let cwd = std::env::current_dir()?;
    cwd.ancestors()
        .map(|dir| dir.join(relative))
        .find(|p| Path::exists(p))
        .with_context(|| format!("{relative} not found above {}", cwd.display()))
}

/// Reads CSV as `COPY … (FORMAT csv)` writes it: an unquoted empty field is
/// NULL and a quoted one an empty string; quoted fields may hold separators,
/// newlines and doubled quotes.
pub fn parse_csv(text: &str) -> Result<Vec<Vec<Option<String>>>> {
    let mut records = Vec::new();
    let mut record = Vec::new();
    let mut field = String::new();
    let mut quoted = false;
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' if field.is_empty() && !quoted => {
                quoted = true;
                loop {
                    match chars.next() {
                        Some('"') if chars.peek() == Some(&'"') => {
                            chars.next();
                            field.push('"');
                        }
                        Some('"') => break,
                        Some(c) => field.push(c),
                        None => anyhow::bail!("unterminated quoted CSV field"),
                    }
                }
            }
            ',' => {
                record.push(take_field(&mut field, &mut quoted));
            }
            '\n' => {
                record.push(take_field(&mut field, &mut quoted));
                records.push(std::mem::take(&mut record));
            }
            '\r' if chars.peek() == Some(&'\n') => {}
            c => field.push(c),
        }
    }
    if !field.is_empty() || quoted || !record.is_empty() {
        record.push(take_field(&mut field, &mut quoted));
        records.push(record);
    }
    Ok(records)
}

fn take_field(field: &mut String, quoted: &mut bool) -> Option<String> {
    let value = std::mem::take(field);
    let was_quoted = std::mem::replace(quoted, false);
    (was_quoted || !value.is_empty()).then_some(value)
}

#[cfg(test)]
mod tests {
    use super::parse_csv;

    #[test]
    fn csv_keeps_null_apart_from_empty_and_quoted_text_intact() {
        let text = "id,note\n1,\n2,\"\"\n3,\"a,b\"\n4,\"line\nbreak \"\"quoted\"\"\"\n5,plain\n";
        let parsed = parse_csv(text).expect("valid CSV");
        let s = |v: &str| Some(v.to_string());
        assert_eq!(
            parsed,
            vec![
                vec![s("id"), s("note")],
                vec![s("1"), None],
                vec![s("2"), s("")],
                vec![s("3"), s("a,b")],
                vec![s("4"), s("line\nbreak \"quoted\"")],
                vec![s("5"), s("plain")],
            ]
        );
    }
}
