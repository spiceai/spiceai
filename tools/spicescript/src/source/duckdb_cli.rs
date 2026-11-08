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

/// The run-wide `DuckDB` release for `duckdb` and `ducklake` sources that do
/// not name one (`--duckdb-version`).
static DEFAULT_VERSION: std::sync::OnceLock<String> = std::sync::OnceLock::new();

/// Sets the release `duckdb` and `ducklake` sources use when their spec has
/// no `version`, in place of the newest one `test/duckdb_versions.json` lists.
pub fn set_default_version(version: String) {
    let _ = DEFAULT_VERSION.set(version);
}

/// The `DuckDB` release a `duckdb` or `ducklake` source writes, checks
/// answers and lends `spiced` its library with: `requested` (the source's
/// `version`), else `--duckdb-version`, else the newest release
/// `test/duckdb_versions.json` lists. Spice loads the installed `DuckDB`, so
/// the CLI writing a file and the library reading it are the same release.
pub fn version(requested: Option<&str>) -> Result<String> {
    if let Some(version) = requested.or(DEFAULT_VERSION.get().map(String::as_str)) {
        return Ok(version.trim_start_matches('v').to_string());
    }
    let path = super::repo_file("test/duckdb_versions.json")?;
    let listed: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(&path)?)
        .with_context(|| format!("Failed to parse {}", path.display()))?;
    listed
        .get("versions")
        .and_then(serde_json::Value::as_array)
        .and_then(|versions| versions.last())
        .and_then(|newest| newest.get("version"))
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string)
        .with_context(|| format!("{} lists no versions", path.display()))
}

/// The `duckdb` CLI of `version`, downloaded once into
/// `~/.cache/spicescript/duckdb-<version>/`. `override_var`, when set, names a
/// binary to use instead.
pub async fn cli(version: &str, override_var: &str) -> Result<String> {
    if let Ok(binary) = std::env::var(override_var) {
        return Ok(binary);
    }
    let asset = release_asset(version, "cli", override_var)?;
    download(version, &asset, "duckdb", override_var).await
}

/// The shared library of `version`, which also exports the `DuckDB` ADBC
/// driver. `SPICESCRIPT_DUCKDB_LIBRARY` names a library to use instead.
pub async fn library(version: &str) -> Result<String> {
    let override_var = "SPICESCRIPT_DUCKDB_LIBRARY";
    if let Ok(library) = std::env::var(override_var) {
        return Ok(std::fs::canonicalize(library)?.display().to_string());
    }
    let asset = release_asset(version, "lib", override_var)?;
    let file = if cfg!(target_os = "macos") {
        "libduckdb.dylib"
    } else {
        "libduckdb.so"
    };
    download(version, &asset, file, override_var).await
}

/// The release asset holding the CLI (`cli`) or the shared library (`lib`)
/// for this platform. Releases from 2.0 name them
/// `duckdb-{cli,shared-libs}-<os>-<arch>.tar.gz`; earlier ones
/// `duckdb_cli-<os>-<arch>.zip` and `libduckdb-<os>-<arch>.zip`.
fn release_asset(version: &str, what: &str, override_var: &str) -> Result<String> {
    let platform = match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", _) => "osx-universal",
        ("linux", "aarch64") => "linux-arm64",
        ("linux", "x86_64") => "linux-amd64",
        (os, arch) => bail!("no DuckDB download for {os}/{arch}; set {override_var}"),
    };
    let major: u32 = version
        .split('.')
        .next()
        .and_then(|major| major.parse().ok())
        .with_context(|| format!("DuckDB version '{version}' does not start with a number"))?;
    Ok(match (what, major >= 2) {
        ("cli", true) => format!("duckdb-cli-{platform}.tar.gz"),
        ("cli", false) => format!("duckdb_cli-{platform}.zip"),
        (_, true) => format!("duckdb-shared-libs-{platform}.tar.gz"),
        (_, false) => format!("libduckdb-{platform}.zip"),
    })
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
        let archive = dir.join(asset);
        let url = format!("https://github.com/duckdb/duckdb/releases/download/v{version}/{asset}");
        let status = tokio::process::Command::new("curl")
            .args(["-fsSL", "-o"])
            .arg(&archive)
            .arg(&url)
            .status()
            .await
            .context("Failed to run curl")?;
        if !status.success() {
            bail!("Failed to download the DuckDB {version} {file} from {url}; set {override_var}");
        }
        let mut unpack = if asset.ends_with(".tar.gz") {
            let mut tar = tokio::process::Command::new("tar");
            tar.arg("-xzf").arg(&archive).arg("-C").arg(&dir);
            tar
        } else {
            let mut unzip = tokio::process::Command::new("unzip");
            unzip.args(["-o", "-q"]).arg(&archive).arg("-d").arg(&dir);
            unzip
        };
        let status = unpack.status().await.context("Failed to unpack")?;
        if !status.success() || !binary.is_file() {
            bail!(
                "Failed to unpack the DuckDB {version} {file} from {}",
                archive.display()
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
