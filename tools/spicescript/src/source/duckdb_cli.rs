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
    releases(VERSIONS_JSON)?
        .last()
        .and_then(|newest| newest.get("version"))
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string)
        .with_context(|| format!("{VERSIONS_FILE} lists no versions"))
}

/// The `duckdb` CLI of `version`, downloaded once into
/// `~/.cache/spicescript/duckdb-<version>/` and checked against the SHA-256
/// `test/duckdb_versions.json` records for it (see [`fetch_into`]).
/// `override_var`, when set, names a binary to use instead.
pub async fn cli(version: &str, override_var: &str) -> Result<String> {
    if let Ok(binary) = std::env::var(override_var) {
        return Ok(binary);
    }
    fetch_part(version, Part::Cli).await.with_context(|| {
        format!("Cannot fetch the DuckDB {version} CLI; set {override_var} to use one installed another way")
    })
}

/// The shared library of `version`, which also exports the `DuckDB` ADBC
/// driver, fetched and checked as [`cli`] is. `SPICESCRIPT_DUCKDB_LIBRARY`
/// names a library to use instead.
pub async fn library(version: &str) -> Result<String> {
    let override_var = "SPICESCRIPT_DUCKDB_LIBRARY";
    if let Ok(library) = std::env::var(override_var) {
        return Ok(std::fs::canonicalize(library)?.display().to_string());
    }
    fetch_part(version, Part::Library).await.with_context(|| {
        format!("Cannot fetch the DuckDB {version} library; set {override_var} to use one installed another way")
    })
}

async fn fetch_part(version: &str, part: Part) -> Result<String> {
    fetch(archive(VERSIONS_JSON, version, part, platform()?)?).await
}

const VERSIONS_FILE: &str = "test/duckdb_versions.json";

/// The releases `test/duckdb_versions.json` lists and the SHA-256 of each of
/// their archives, as of this build.
const VERSIONS_JSON: &str = include_str!("../../../../test/duckdb_versions.json");

/// A part of a release, each in an archive of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Part {
    Cli,
    Library,
}

impl Part {
    /// Its archives' checksums in `test/duckdb_versions.json`, by platform.
    fn key(self) -> &'static str {
        match self {
            Self::Cli => "cli_sha256",
            Self::Library => "sha256",
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Cli => "CLI",
            Self::Library => "shared library",
        }
    }

    /// The file taken from its archive.
    fn file(self) -> &'static str {
        match self {
            Self::Cli => "duckdb",
            Self::Library if cfg!(target_os = "macos") => "libduckdb.dylib",
            Self::Library => "libduckdb.so",
        }
    }
}

/// A release archive to fetch: which, what it must hash to, and the file in
/// it to use.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Archive {
    version: String,
    /// The release asset, such as `duckdb_cli-osx-universal.zip`.
    asset: String,
    /// The SHA-256 `test/duckdb_versions.json` records for it.
    sha256: String,
    /// The file to take from it.
    file: String,
}

/// `DuckDB`'s name for this platform's release archives.
fn platform() -> Result<&'static str> {
    Ok(match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", _) => "osx-universal",
        ("linux", "aarch64") => "linux-arm64",
        ("linux", "x86_64") => "linux-amd64",
        (os, arch) => bail!("DuckDB publishes no release for {os}/{arch}"),
    })
}

fn releases(versions: &str) -> Result<Vec<serde_json::Value>> {
    let listed: serde_json::Value = serde_json::from_str(versions)
        .with_context(|| format!("{VERSIONS_FILE} is not valid JSON"))?;
    listed
        .get("versions")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .with_context(|| format!("{VERSIONS_FILE} has no `versions` list"))
}

/// The archive holding `part` of `DuckDB` `version` for `platform`, with the
/// SHA-256 `versions` (the JSON of `test/duckdb_versions.json`) records for
/// it. A release, or a platform of one, with no recorded checksum is refused:
/// its download could not be checked. Releases from 2.0 name their archives
/// `duckdb-{cli,shared-libs}-<platform>.tar.gz`; earlier ones
/// `duckdb_cli-<platform>.zip` and `libduckdb-<platform>.zip`.
fn archive(versions: &str, version: &str, part: Part, platform: &str) -> Result<Archive> {
    let releases = releases(versions)?;
    let Some(release) = releases.iter().find(|release| {
        release.get("version").and_then(serde_json::Value::as_str) == Some(version)
    }) else {
        let listed: Vec<&str> = releases
            .iter()
            .filter_map(|release| release.get("version").and_then(serde_json::Value::as_str))
            .collect();
        bail!(
            "DuckDB {version} is not listed in {VERSIONS_FILE} (it lists {}), so its download could not be checked. Add it there with the SHA-256 of each of its archives",
            listed.join(", ")
        );
    };
    let major: u32 = version
        .split('.')
        .next()
        .and_then(|major| major.parse().ok())
        .with_context(|| format!("DuckDB version '{version}' does not start with a number"))?;
    let asset = match (part, major >= 2) {
        (Part::Cli, true) => format!("duckdb-cli-{platform}.tar.gz"),
        (Part::Cli, false) => format!("duckdb_cli-{platform}.zip"),
        (Part::Library, true) => format!("duckdb-shared-libs-{platform}.tar.gz"),
        (Part::Library, false) => format!("libduckdb-{platform}.zip"),
    };
    let sha256 = release
        .get(part.key())
        .and_then(|digests| digests.get(platform))
        .and_then(serde_json::Value::as_str)
        .filter(|digest| is_sha256(digest))
        .with_context(|| {
            format!(
                "{VERSIONS_FILE} records no SHA-256 (`{}.{platform}`) for the DuckDB {version} {} archive {asset}, so its download could not be checked. Add the checksum GitHub publishes for that asset",
                part.key(),
                part.name()
            )
        })?;
    Ok(Archive {
        version: version.to_string(),
        asset,
        sha256: sha256.to_string(),
        file: part.file().to_string(),
    })
}

fn is_sha256(text: &str) -> bool {
    text.len() == 64 && text.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

fn sha256_file(path: &std::path::Path) -> Result<String> {
    use sha2::Digest as _;
    use std::io::Read as _;

    let mut file =
        std::fs::File::open(path).with_context(|| format!("Failed to open {}", path.display()))?;
    let mut hasher = sha2::Sha256::new();
    let mut buffer = vec![0; 1 << 20];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// `archive`'s file from the cache, checked as [`fetch_into`] describes.
async fn fetch(archive: Archive) -> Result<String> {
    // The files this process has already checked and placed, which it then
    // reuses: matrix combinations start the same release's sources many times.
    static CHECKED: std::sync::LazyLock<
        parking_lot::Mutex<std::collections::HashSet<std::path::PathBuf>>,
    > = std::sync::LazyLock::new(Default::default);
    let home = std::env::var("HOME").context("HOME is not set")?;
    let root = std::path::PathBuf::from(home).join(".cache/spicescript");
    let url = format!(
        "https://github.com/duckdb/duckdb/releases/download/v{}/{}",
        archive.version, archive.asset
    );
    // In a task of its own, so that a caller that stops waiting (a source
    // start cancelled because another source failed) cannot leave a half-
    // unpacked release in the cache for the next run to find.
    let path = tokio::spawn(async move {
        // Matrix combinations can resolve the same release concurrently.
        static DOWNLOAD: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
        let _download = DOWNLOAD.lock().await;
        let target = root
            .join(format!("duckdb-{}", archive.version))
            .join(&archive.file);
        if CHECKED.lock().contains(&target) && target.is_file() {
            return Ok(target);
        }
        let target = fetch_into(&root, &url, &archive).await?;
        CHECKED.lock().insert(target.clone());
        anyhow::Ok(target)
    })
    .await
    .context("the DuckDB download task failed")??;
    Ok(path.display().to_string())
}

/// A directory removed, with what is in it, when dropped.
struct Scratch(std::path::PathBuf);

impl Scratch {
    /// A new, empty directory in `parent` that no other fetch uses.
    fn new(parent: &std::path::Path, label: &str) -> Result<Self> {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = parent.join(format!(".{label}-{}-{n}", std::process::id()));
        if dir.exists() {
            std::fs::remove_dir_all(&dir)?;
        }
        std::fs::create_dir(&dir).with_context(|| format!("Failed to create {}", dir.display()))?;
        Ok(Self(dir))
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// The regular file named `name` in `dir` or below it, never a link.
fn find_file(dir: &std::path::Path, name: &str) -> Option<std::path::PathBuf> {
    let mut entries: Vec<_> = std::fs::read_dir(dir).ok()?.flatten().collect();
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in &entries {
        let kind = entry.file_type().ok()?;
        if kind.is_file() && entry.file_name() == name {
            return Some(entry.path());
        }
    }
    entries
        .iter()
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
        .find_map(|entry| find_file(&entry.path(), name))
}

/// `archive`'s file, placed at `root/duckdb-<version>/<file>`.
///
/// Nothing in the cache is taken on trust, since another run on the host can
/// write there. The release archive is kept beside the file taken from it,
/// and each time a run first needs the file, the kept archive is hashed
/// against the SHA-256 `test/duckdb_versions.json` records and the file is
/// taken from it again, so what runs is what the checked archive holds. A
/// kept archive that does not match, or none, is replaced by a download from
/// `url`, which is checked before anything is unpacked from it: a download
/// that does not match is refused and removed.
async fn fetch_into(
    root: &std::path::Path,
    url: &str,
    archive: &Archive,
) -> Result<std::path::PathBuf> {
    let dir = root.join(format!("duckdb-{}", archive.version));
    let kept = dir.join(&archive.asset);
    let target = dir.join(&archive.file);
    // A link in its place would put what is fetched wherever it points.
    if dir.is_symlink() {
        tokio::fs::remove_file(&dir).await?;
    }
    tokio::fs::create_dir_all(&dir).await?;
    remove_stale_scratch(&dir).await;
    let kept_matches = kept.is_file()
        && !kept.is_symlink()
        && sha256_file(&kept).is_ok_and(|actual| actual == archive.sha256);
    if !kept_matches {
        let download = Scratch::new(&dir, "download")?;
        let downloaded = download.0.join(&archive.asset);
        let status = tokio::process::Command::new("curl")
            .args(crate::release::CURL_RELEASE_RETRY)
            // `--retry-max-time` bounds the retries, not one stalled transfer.
            .args(["--max-time", &DOWNLOAD_MAX.as_secs().to_string()])
            .args(["-fsSL", "-o"])
            .arg(&downloaded)
            .arg(url)
            .status()
            .await
            .context("Failed to run curl")?;
        if !status.success() {
            bail!(
                "Failed to download the DuckDB {} archive {url}",
                archive.version
            );
        }
        let actual = sha256_file(&downloaded)?;
        if actual != archive.sha256 {
            bail!(
                "The DuckDB {} archive {url} has SHA-256 {actual}, but {VERSIONS_FILE} records {}; nothing was unpacked from it",
                archive.version,
                archive.sha256
            );
        }
        tokio::fs::rename(&downloaded, &kept).await?;
    }
    let unpack = Scratch::new(&dir, "unpack")?;
    let mut command = if archive.asset.ends_with(".tar.gz") {
        let mut tar = tokio::process::Command::new("tar");
        tar.arg("-xzf").arg(&kept).arg("-C").arg(&unpack.0);
        tar
    } else {
        let mut unzip = tokio::process::Command::new("unzip");
        unzip.args(["-o", "-q"]).arg(&kept).arg("-d").arg(&unpack.0);
        unzip
    };
    let status = command.status().await.context("Failed to unpack")?;
    let file = find_file(&unpack.0, &archive.file).filter(|_| status.success());
    let Some(file) = file else {
        bail!(
            "Failed to unpack {} from the DuckDB {} archive {}",
            archive.file,
            archive.version,
            kept.display()
        );
    };
    // A rename, so a process already running the file it replaces keeps it.
    tokio::fs::rename(&file, &target).await?;
    Ok(target)
}

/// The longest one download of a release may take (curl's `--max-time`).
const DOWNLOAD_MAX: std::time::Duration = std::time::Duration::from_mins(30);

/// Removes the download and unpack directories ([`Scratch`]) in `dir` that a
/// process killed while fetching left behind. Only those older than four
/// times the longest a download may take: a live one is never that old, since
/// its curl would have given up and its process removed it.
async fn remove_stale_scratch(dir: &std::path::Path) {
    let Ok(mut entries) = tokio::fs::read_dir(dir).await else {
        return;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().into_owned();
        let stale = (name.starts_with(".download-") || name.starts_with(".unpack-"))
            && entry
                .metadata()
                .await
                .ok()
                .and_then(|m| m.modified().ok())
                .and_then(|modified| modified.elapsed().ok())
                .is_some_and(|age| age > DOWNLOAD_MAX * 4);
        if stale {
            let _ = tokio::fs::remove_dir_all(entry.path()).await;
        }
    }
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
    use std::path::{Path, PathBuf};

    use super::{
        Archive, DOWNLOAD_MAX, Part, VERSIONS_JSON, archive, copy_to_stdout, fetch_into, parse,
        releases, remove_stale_scratch, sha256_file,
    };

    const PLATFORMS: [&str; 3] = ["linux-amd64", "linux-arm64", "osx-universal"];

    #[test]
    fn every_listed_release_records_both_archives_for_every_platform() {
        for release in releases(VERSIONS_JSON).expect("versions") {
            let version = release["version"].as_str().expect("version");
            for platform in PLATFORMS {
                for part in [Part::Cli, Part::Library] {
                    archive(VERSIONS_JSON, version, part, platform)
                        .unwrap_or_else(|e| panic!("DuckDB {version} {platform} {part:?}: {e:#}"));
                }
            }
        }
        assert_eq!(
            archive(VERSIONS_JSON, "2.0.0", Part::Cli, "osx-universal").expect("listed"),
            Archive {
                version: "2.0.0".into(),
                asset: "duckdb-cli-osx-universal.tar.gz".into(),
                sha256: "8a8f67e7c7f7d1392fa63ba3971948391392fe2a25fca63e7008e33c50beb4c2".into(),
                file: "duckdb".into(),
            }
        );
        assert_eq!(
            archive(VERSIONS_JSON, "1.5.6", Part::Library, "linux-amd64").expect("listed"),
            Archive {
                version: "1.5.6".into(),
                asset: "libduckdb-linux-amd64.zip".into(),
                sha256: "b845005f5132a7d8180057c35e14a7626632258782f871a90861b19c1c03841b".into(),
                file: Part::Library.file().into(),
            }
        );
    }

    #[test]
    fn a_release_or_platform_with_no_recorded_checksum_is_refused() {
        let digest = "4711438f0fdb04f0441803409bec5430b763d4f2ac3482c1f97cfa6b5ecb4c15";
        let versions = format!(
            r#"{{"versions": [{{"version": "1.5.2", "sha256": {{"linux-amd64": "{digest}"}}, "cli_sha256": {{"linux-amd64": "FC91"}}}}]}}"#
        );
        let refusal = |version, part, platform| {
            archive(&versions, version, part, platform)
                .expect_err("refused")
                .to_string()
        };
        assert_eq!(
            refusal("1.4.4", Part::Library, "linux-amd64"),
            "DuckDB 1.4.4 is not listed in test/duckdb_versions.json (it lists 1.5.2), so its download could not be checked. Add it there with the SHA-256 of each of its archives"
        );
        assert_eq!(
            refusal("1.5.2", Part::Library, "linux-arm64"),
            "test/duckdb_versions.json records no SHA-256 (`sha256.linux-arm64`) for the DuckDB 1.5.2 shared library archive libduckdb-linux-arm64.zip, so its download could not be checked. Add the checksum GitHub publishes for that asset"
        );
        // A checksum that is not one is no checksum.
        assert_eq!(
            refusal("1.5.2", Part::Cli, "linux-amd64"),
            "test/duckdb_versions.json records no SHA-256 (`cli_sha256.linux-amd64`) for the DuckDB 1.5.2 CLI archive duckdb_cli-linux-amd64.zip, so its download could not be checked. Add the checksum GitHub publishes for that asset"
        );
        assert_eq!(
            archive(&versions, "1.5.2", Part::Library, "linux-amd64")
                .expect("recorded")
                .sha256,
            digest
        );
    }

    /// A directory of its own for one test, removed when it ends.
    struct TestDir(PathBuf);

    impl TestDir {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir()
                .join(format!("spicescript-duckdb-{name}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).expect("test directory");
            Self(dir)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// A `.tar.gz` named `asset` in `dir`, holding the file `duckdb` with
    /// `contents`; its `file://` URL and SHA-256.
    fn release(dir: &Path, asset: &str, contents: &str) -> (String, String) {
        let source = dir.join(format!("{asset}.contents"));
        std::fs::create_dir_all(&source).expect("source directory");
        std::fs::write(source.join("duckdb"), contents).expect("file");
        let path = dir.join(asset);
        let status = std::process::Command::new("tar")
            .env("COPYFILE_DISABLE", "1")
            .arg("-czf")
            .arg(&path)
            .arg("-C")
            .arg(&source)
            .arg("duckdb")
            .status()
            .expect("tar");
        assert!(status.success(), "tar failed");
        let sha256 = sha256_file(&path).expect("hash");
        (format!("file://{}", path.display()), sha256)
    }

    fn cli_archive(version: &str, sha256: &str) -> Archive {
        Archive {
            version: version.into(),
            asset: "duckdb-cli-test.tar.gz".into(),
            sha256: sha256.into(),
            file: "duckdb".into(),
        }
    }

    fn read(path: &Path) -> String {
        std::fs::read_to_string(path).expect("read")
    }

    #[tokio::test]
    async fn a_download_is_unpacked_only_when_it_matches_its_recorded_checksum() {
        let dir = TestDir::new("download");
        let cache = dir.0.join("cache");
        let releases = dir.0.join("releases");
        std::fs::create_dir_all(&releases).expect("releases");
        let (url, sha256) = release(&releases, "duckdb-cli-test.tar.gz", "the CLI");

        let fetched = fetch_into(&cache, &url, &cli_archive("9.9.9", &sha256))
            .await
            .expect("a matching download");
        assert_eq!(fetched, cache.join("duckdb-9.9.9/duckdb"));
        assert_eq!(read(&fetched), "the CLI");
        assert_eq!(
            sha256_file(&cache.join("duckdb-9.9.9/duckdb-cli-test.tar.gz")).expect("kept"),
            sha256,
            "the checked archive is kept"
        );

        let recorded = "0".repeat(64);
        let refused = fetch_into(&cache, &url, &cli_archive("9.9.8", &recorded))
            .await
            .expect_err("a download that does not match");
        assert_eq!(
            refused.to_string(),
            format!(
                "The DuckDB 9.9.8 archive {url} has SHA-256 {sha256}, but test/duckdb_versions.json records {recorded}; nothing was unpacked from it"
            )
        );
        let left: Vec<_> = std::fs::read_dir(cache.join("duckdb-9.9.8"))
            .expect("cache directory")
            .collect();
        assert!(left.is_empty(), "the refused download left {left:?}");
    }

    #[tokio::test]
    async fn a_cached_release_is_checked_again_before_it_is_reused() {
        let dir = TestDir::new("reuse");
        let cache = dir.0.join("cache");
        let releases = dir.0.join("releases");
        std::fs::create_dir_all(&releases).expect("releases");
        let (url, sha256) = release(&releases, "duckdb-cli-test.tar.gz", "the CLI");
        // Another archive: a fetch that downloaded it would be refused, so
        // one that succeeds from it used the kept archive.
        let (other_url, _) = release(&releases, "other.tar.gz", "not the CLI");
        let wanted = cli_archive("9.9.9", &sha256);
        let file = cache.join("duckdb-9.9.9/duckdb");
        let kept = cache.join("duckdb-9.9.9/duckdb-cli-test.tar.gz");
        fetch_into(&cache, &url, &wanted)
            .await
            .expect("first fetch");

        // A changed file is taken from the kept archive again.
        std::fs::write(&file, "changed").expect("change the file");
        fetch_into(&cache, &other_url, &wanted)
            .await
            .expect("taken from the kept archive");
        assert_eq!(read(&file), "the CLI");

        // A changed archive is replaced by a checked download.
        std::fs::write(&kept, "changed").expect("change the archive");
        std::fs::write(&file, "changed").expect("change the file");
        fetch_into(&cache, &url, &wanted)
            .await
            .expect("downloaded again");
        assert_eq!(read(&file), "the CLI");
        assert_eq!(sha256_file(&kept).expect("kept"), sha256);

        // And when that download does not match either, nothing is used.
        std::fs::write(&kept, "changed").expect("change the archive");
        let refused = fetch_into(&cache, &other_url, &wanted)
            .await
            .expect_err("neither archive matches");
        assert!(
            refused
                .to_string()
                .contains("but test/duckdb_versions.json records"),
            "{refused:#}"
        );
    }

    #[tokio::test]
    async fn a_killed_fetchs_scratch_directories_are_removed_once_stale() {
        let dir = TestDir::new("stale");
        let release = dir.0.join("duckdb-9.9.9");
        let made = |name: &str, age: std::time::Duration| {
            let path = release.join(name);
            std::fs::create_dir_all(&path).expect("directory");
            let modified = std::time::SystemTime::now() - age;
            std::fs::File::open(&path)
                .and_then(|handle| handle.set_modified(modified))
                .expect("set its time");
            path
        };
        let stale = DOWNLOAD_MAX * 5;
        let old_download = made(".download-1-0", stale);
        let old_unpack = made(".unpack-2-0", stale);
        // Still in use: not older than a download may take.
        let live = made(".download-3-0", DOWNLOAD_MAX);
        // Not a fetch's scratch directory.
        let other = made("other", stale);
        std::fs::write(release.join("duckdb"), "the CLI").expect("file");

        remove_stale_scratch(&release).await;

        assert!(!old_download.exists() && !old_unpack.exists());
        assert!(live.exists() && other.exists());
        assert_eq!(read(&release.join("duckdb")), "the CLI");
    }

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
