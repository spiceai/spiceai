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

//! Runs statements through the `duckdb` CLI, for sources whose writer and
//! oracle is `DuckDB` (Iceberg, `DuckLake`).

use std::process::Stdio;

use anyhow::{Context, Result, bail};
use tokio::io::AsyncWriteExt;

use super::{Source, TextResult};

/// Separates cells and marks NULL in the CLI's output; neither occurs in text.
const SEPARATOR: char = '\u{1f}';
const NULL: &str = "\u{1e}";

/// The `duckdb` CLI of the release Spice embeds, for writers whose format
/// Spice must read (a newer release writes a `DuckLake` format Spice cannot
/// attach). The release is the `duckdb` crate's version in the workspace
/// `Cargo.lock` above `near`; its CLI is downloaded once into
/// `~/.cache/spicescript/duckdb-<version>/`. `override_var`, when set, names a
/// binary to use instead.
pub async fn embedded(near: &std::path::Path, override_var: &str) -> Result<String> {
    if let Ok(binary) = std::env::var(override_var) {
        return Ok(binary);
    }
    let lock = near
        .ancestors()
        .map(|dir| dir.join("Cargo.lock"))
        .find(|path| path.is_file())
        .with_context(|| {
            format!(
                "no Cargo.lock above {} to read Spice's DuckDB release from; set {override_var}",
                near.display()
            )
        })?;
    let text = tokio::fs::read_to_string(&lock).await?;
    let version = text
        .split("[[package]]")
        .find(|package| package.contains("name = \"duckdb\"\n"))
        .and_then(|package| package.lines().find_map(|l| l.strip_prefix("version = \"")))
        .map(|v| v.trim_end_matches('"').to_string())
        .with_context(|| {
            format!(
                "no duckdb package in {}; set {override_var}",
                lock.display()
            )
        })?;
    let home = std::env::var("HOME").context("HOME is not set")?;
    let dir = std::path::PathBuf::from(home).join(format!(".cache/spicescript/duckdb-{version}"));
    let binary = dir.join("duckdb");
    if !binary.is_file() {
        let asset = match (std::env::consts::OS, std::env::consts::ARCH) {
            ("macos", _) => "duckdb_cli-osx-universal.zip",
            ("linux", "aarch64") => "duckdb_cli-linux-arm64.zip",
            ("linux", _) => "duckdb_cli-linux-amd64.zip",
            (os, arch) => bail!("no DuckDB CLI download for {os}/{arch}; set {override_var}"),
        };
        tokio::fs::create_dir_all(&dir).await?;
        let zip = dir.join(asset);
        let url = format!("https://github.com/duckdb/duckdb/releases/download/v{version}/{asset}");
        let status = tokio::process::Command::new("curl")
            .args(["-fsSL", "-o"])
            .arg(&zip)
            .arg(&url)
            .status()
            .await
            .context("Failed to run curl")?;
        if !status.success() {
            bail!("Failed to download the DuckDB {version} CLI from {url}; set {override_var}");
        }
        let status = tokio::process::Command::new("unzip")
            .args(["-o", "-q"])
            .arg(&zip)
            .arg("-d")
            .arg(&dir)
            .status()
            .await
            .context("Failed to run unzip")?;
        if !status.success() || !binary.is_file() {
            bail!(
                "Failed to unpack the DuckDB {version} CLI from {}",
                zip.display()
            );
        }
    }
    Ok(binary.display().to_string())
}

/// Output settings that make [`parse`] able to read a query's answer.
pub fn list_output() -> String {
    format!(".mode list\n.headers on\n.separator \"{SEPARATOR}\"\n.nullvalue \"{NULL}\"\n")
}

/// Runs `prelude` with its output discarded, then `output` and `sql`, in the
/// `duckdb` at `binary`; returns what `sql` printed.
pub async fn run(binary: &str, prelude: &str, output: &str, sql: &str) -> Result<String> {
    let mut script = format!(".output /dev/null\n{prelude}\n.output\n{output}{sql}");
    if !sql.trim_end().ends_with(';') {
        script.push(';');
    }
    script.push('\n');
    let mut child = tokio::process::Command::new(binary)
        .args(["-bail", "-batch", "-noheader"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("Failed to run `{binary}`; is DuckDB installed?"))?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(script.as_bytes()).await?;
    }
    let out = child.wait_with_output().await?;
    if !out.status.success() {
        bail!(
            "duckdb rejected `{}`: {}",
            sql.trim(),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Reads an answer printed under [`list_output`]: a header, then one line per
/// row.
pub fn parse(text: &str) -> TextResult {
    let mut lines = text.lines().filter(|l| !l.is_empty());
    let columns: Vec<String> = lines
        .next()
        .map(|h| h.split(SEPARATOR).map(str::to_string).collect())
        .unwrap_or_default();
    let rows = lines
        .map(|line| {
            line.split(SEPARATOR)
                .map(|cell| (cell != NULL).then(|| cell.to_string()))
                .collect()
        })
        .collect();
    TextResult { columns, rows }
}

/// Writes a generated table's rows as Parquet under `staging`, then has the
/// source's `DuckDB` create the table from them (`replace`) or append them
/// (`append`) in one statement.
pub async fn generate(
    source: &(dyn Source + '_),
    staging: &std::path::Path,
    namespace: &str,
    spec: &crate::generate::TableSpec,
) -> Result<u64> {
    use crate::generate::{FileFormat, WriteMode};
    let dir = staging.join(namespace).join(&spec.table);
    if dir.exists() {
        std::fs::remove_dir_all(&dir)?;
    }
    std::fs::create_dir_all(&dir)?;
    let mut parquet = spec.clone();
    parquet.format = FileFormat::Parquet;
    let written = {
        let dir = dir.clone();
        tokio::task::spawn_blocking(move || super::files::write_parts(&dir, 0, &parquet))
            .await
            .context("file writer panicked")??
    };
    let files = format!("{}/*.parquet", dir.display());
    let sql = match spec.mode {
        WriteMode::Replace => format!(
            "DROP TABLE IF EXISTS {table}; CREATE TABLE {table} AS SELECT * FROM read_parquet('{files}')",
            table = spec.table
        ),
        WriteMode::Append => format!(
            "INSERT INTO {} SELECT * FROM read_parquet('{files}')",
            spec.table
        ),
    };
    source.exec(namespace, &sql).await?;
    Ok(written)
}
