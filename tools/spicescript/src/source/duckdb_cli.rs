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
//! oracle is `DuckDB` (Iceberg, `DuckLake`, `DuckDB` files).

use std::process::Stdio;

use anyhow::{Context, Result, bail};
use tokio::io::AsyncWriteExt;

use super::{Source, TextResult};

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
    let version = embedded_version(near, override_var).await?;
    let asset = match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", _) => "duckdb_cli-osx-universal.zip",
        ("linux", "aarch64") => "duckdb_cli-linux-arm64.zip",
        ("linux", "x86_64") => "duckdb_cli-linux-amd64.zip",
        (os, arch) => bail!("no DuckDB CLI download for {os}/{arch}; set {override_var}"),
    };
    download(&version, asset, "duckdb", override_var).await
}

/// The matching shared library, which also exports the `DuckDB` ADBC driver.
/// `SPICESCRIPT_DUCKDB_LIBRARY` names a library to use instead.
pub async fn embedded_library(near: &std::path::Path) -> Result<String> {
    let override_var = "SPICESCRIPT_DUCKDB_LIBRARY";
    if let Ok(library) = std::env::var(override_var) {
        return Ok(std::fs::canonicalize(library)?.display().to_string());
    }
    let version = embedded_version(near, override_var).await?;
    let (asset, library) = match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", _) => ("libduckdb-osx-universal.zip", "libduckdb.dylib"),
        ("linux", "aarch64") => ("libduckdb-linux-arm64.zip", "libduckdb.so"),
        ("linux", "x86_64") => ("libduckdb-linux-amd64.zip", "libduckdb.so"),
        (os, arch) => bail!("no DuckDB library download for {os}/{arch}; set {override_var}"),
    };
    download(&version, asset, library, override_var).await
}

async fn embedded_version(near: &std::path::Path, override_var: &str) -> Result<String> {
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
    Ok(version)
}

async fn download(version: &str, asset: &str, file: &str, override_var: &str) -> Result<String> {
    // Matrix combinations can resolve the same release concurrently.
    static DOWNLOAD: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _download = DOWNLOAD.lock().await;
    let home = std::env::var("HOME").context("HOME is not set")?;
    let dir = std::path::PathBuf::from(home).join(format!(".cache/spicescript/duckdb-{version}"));
    let binary = dir.join(file);
    if !binary.is_file() {
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
            bail!("Failed to download the DuckDB {version} {file} from {url}; set {override_var}");
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
                "Failed to unpack the DuckDB {version} {file} from {}",
                zip.display()
            );
        }
    }
    Ok(binary.display().to_string())
}

/// `sql` as a COPY to standard output in the convention PostgreSQL's COPY uses
/// and [`parse`] reads: every value quoted, NULL an unquoted empty field. So
/// an empty string, a value with a newline and the text `NULL` all survive.
pub fn copy_to_stdout(sql: &str) -> String {
    let select = sql.trim().trim_end_matches(';').trim_end();
    format!("COPY ({select}) TO '/dev/stdout' (FORMAT csv, HEADER, NULLSTR '', FORCE_QUOTE *)")
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

/// Reads an answer printed by [`copy_to_stdout`]: a header, then the rows.
pub fn parse(text: &str) -> Result<TextResult> {
    let mut records = super::parse_csv(text)?.into_iter();
    let columns = records
        .next()
        .unwrap_or_default()
        .into_iter()
        .map(Option::unwrap_or_default)
        .collect();
    Ok(TextResult {
        columns,
        rows: records.collect(),
    })
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

#[cfg(test)]
mod tests {
    use super::{copy_to_stdout, parse};

    #[test]
    fn copy_output_keeps_empty_strings_newlines_and_the_word_null_apart_from_null() {
        // DuckDB 1.5.5's CLI output for copy_to_stdout over
        // VALUES (1, ''), (2, NULL), (3, 'a\nb'), (4, 'x,y'), (5, 'say "hi"'), (6, 'NULL').
        let answer = parse(
            "\"id\",\"s\"\n\"1\",\"\"\n\"2\",\n\"3\",\"a\nb\"\n\"4\",\"x,y\"\n\"5\",\"say \"\"hi\"\"\"\n\"6\",\"NULL\"\n",
        )
        .expect("parsed");
        assert_eq!(answer.columns, vec!["id", "s"]);
        let s: Vec<Option<&str>> = answer.rows.iter().map(|r| r[1].as_deref()).collect();
        assert_eq!(
            s,
            vec![
                Some(""),
                None,
                Some("a\nb"),
                Some("x,y"),
                Some("say \"hi\""),
                Some("NULL")
            ]
        );
        // A one-column table holding '' and NULL: a quoted empty line, then a
        // blank one.
        let one = parse("\"s\"\n\"\"\n\n").expect("parsed");
        assert_eq!(one.rows, vec![vec![Some(String::new())], vec![None]]);
    }

    #[test]
    fn copies_a_query_whatever_its_trailing_semicolon() {
        assert_eq!(
            copy_to_stdout("SELECT 1 ;\n"),
            "COPY (SELECT 1) TO '/dev/stdout' (FORMAT csv, HEADER, NULLSTR '', FORCE_QUOTE *)"
        );
    }
}
