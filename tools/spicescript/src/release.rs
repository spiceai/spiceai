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

//! Resolves script base releases and caches their OSS binaries.
//!
//! A release's `spiced` runs only once its archive is verified: against the
//! digest `.github/release-digests.json` records for a release published
//! before SHA256SUMS, or else against the release's SHA256SUMS, with the
//! archive and SHA256SUMS each carrying a build attestation from
//! `build_release.yml` on a release branch. A cached archive is verified the
//! same way each time it is used, and `spiced` is unpacked from it afresh.

use std::{
    collections::BTreeMap,
    io::Read,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// The reviewed digests of the releases published before SHA256SUMS.
const RELEASE_DIGESTS: &str = include_str!("../../../.github/release-digests.json");

/// The signer a release file's attestation must name: `build_release.yml` on a
/// release branch, as the install scripts require (docs/DISTRIBUTIONS.md).
const RELEASE_SIGNER: &str = r"^https://github\.com/spiceai/spiceai/\.github/workflows/build_release\.yml@refs/heads/release/";

const SUMS: &str = "SHA256SUMS";

#[derive(Deserialize)]
struct ReleaseDigests {
    releases: BTreeMap<String, BTreeMap<String, String>>,
}

fn is_sha256(text: &str) -> bool {
    text.len() == 64 && text.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// The digest `.github/release-digests.json` records for `asset` of `v{version}`.
fn pinned_digest(version: &str, asset: &str) -> Result<Option<String>> {
    let digests: ReleaseDigests = serde_json::from_str(RELEASE_DIGESTS)
        .context("Failed to read .github/release-digests.json")?;
    let Some(digest) = digests
        .releases
        .get(&format!("v{version}"))
        .and_then(|assets| assets.get(asset))
    else {
        return Ok(None);
    };
    ensure!(
        is_sha256(digest),
        ".github/release-digests.json records '{digest}' for v{version} {asset}, which is not a SHA-256 digest"
    );
    Ok(Some(digest.clone()))
}

#[derive(Debug)]
struct Version {
    major: u64,
    minor: u64,
    patch: Option<u64>,
}

impl Version {
    fn parse(text: &str) -> Result<Self> {
        let parts: Vec<&str> = text.split('.').collect();
        let number = |part: &str| {
            ensure!(
                !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()),
                "invalid version component"
            );
            Ok::<u64, anyhow::Error>(part.parse()?)
        };
        let parsed = || -> Result<Self> {
            ensure!(parts.len() == 3, "expected three components");
            Ok(Self {
                major: number(parts[0])?,
                minor: number(parts[1])?,
                patch: if parts[2] == "x" {
                    None
                } else {
                    Some(number(parts[2])?)
                },
            })
        };
        parsed().with_context(|| format!("invalid `from:` release '{text}'; expected MAJOR.MINOR.PATCH or MAJOR.MINOR.x (digits only, no leading v or pre-release)"))
    }
}

/// Checks a `from:` value: `X.Y.Z` or `X.Y.x`.
pub fn validate(text: &str) -> Result<()> {
    Version::parse(text).map(|_| ())
}

/// Whether a valid `from:` value names one release (`X.Y.Z`, not `X.Y.x`).
pub fn is_exact(text: &str) -> Result<bool> {
    Ok(Version::parse(text)?.patch.is_some())
}

/// The release a run starts on: the script's `from:`, the exact version
/// it resolved to, and the downloaded binary.
#[derive(Clone)]
pub struct BaseRelease {
    pub spec: String,
    pub version: String,
    pub binary: PathBuf,
}

/// The published OSS releases `X.Y.x` resolves among, as read from GitHub
/// and cached in `~/.cache/spicescript/releases.json` for [`LIST_TTL`].
#[derive(Serialize, Deserialize)]
pub struct Releases {
    /// When the list was read from GitHub (RFC 3339).
    fetched_at: String,
    /// What each `X.Y.x` resolves to: the newest published X.Y release with
    /// a `spiced` build for this host.
    newest: BTreeMap<String, String>,
}

/// How long a cached release list is used before GitHub is asked again.
const LIST_TTL: Duration = Duration::from_hours(1);

const RELEASES_URL: &str = "https://api.github.com/repos/spiceai/spiceai/releases";

#[derive(Deserialize)]
struct GithubRelease {
    tag_name: String,
    draft: bool,
    prerelease: bool,
    assets: Vec<GithubAsset>,
}

impl GithubRelease {
    fn has(&self, name: &str) -> bool {
        self.assets.iter().any(|a| a.name == name)
    }
}

#[derive(Deserialize)]
struct GithubAsset {
    name: String,
}

impl Releases {
    /// The cached list while it is fresh, else the list from GitHub. When
    /// GitHub can't be reached, a stale cached list is used with a warning.
    pub async fn load() -> Result<Self> {
        let path = cache_dir()?.join("releases.json");
        let mut cached = tokio::fs::read_to_string(&path)
            .await
            .ok()
            .and_then(|text| serde_json::from_str::<Self>(&text).ok());
        if let Some(fresh) = cached.take_if(|c| c.age().is_some_and(|age| age < LIST_TTL)) {
            return Ok(fresh);
        }
        match Self::from_github().await {
            Ok(releases) => {
                let partial = path.with_extension(format!("json.partial-{}", std::process::id()));
                tokio::fs::create_dir_all(cache_dir()?).await?;
                tokio::fs::write(&partial, serde_json::to_string_pretty(&releases)? + "\n").await?;
                tokio::fs::rename(&partial, &path).await?;
                println!("Spice releases read from GitHub into {}", path.display());
                Ok(releases)
            }
            Err(e) => {
                let cached = cached.with_context(|| {
                    format!("Failed to list Spice releases on GitHub to resolve `from: X.Y.x`, and none are cached in {}. Pass --from-release X.Y.x=X.Y.Z or --from PATH. Cause: {e:#}", path.display())
                })?;
                println!(
                    "warning: failed to list Spice releases on GitHub, so `from: X.Y.x` resolves with the list read at {} from {}. Cause: {e:#}",
                    cached.fetched_at,
                    path.display()
                );
                Ok(cached)
            }
        }
    }

    fn age(&self) -> Option<Duration> {
        let fetched = chrono::DateTime::parse_from_rfc3339(&self.fetched_at).ok()?;
        (chrono::Utc::now() - fetched.with_timezone(&chrono::Utc))
            .to_std()
            .ok()
    }

    async fn from_github() -> Result<Self> {
        let token = ["GITHUB_TOKEN", "GH_TOKEN"]
            .into_iter()
            .find_map(|name| std::env::var(name).ok().filter(|t| !t.is_empty()));
        let client = reqwest::Client::new();
        let mut releases = Vec::new();
        // GitHub returns at most 100 releases a page, newest first.
        for page in 1..=20 {
            let mut request = client
                .get(RELEASES_URL)
                .query(&[("per_page", "100"), ("page", &page.to_string())])
                .header(reqwest::header::USER_AGENT, "spicescript")
                .header(reqwest::header::ACCEPT, "application/vnd.github+json");
            if let Some(token) = &token {
                request = request.bearer_auth(token);
            }
            let response = request.send().await?;
            let status = response.status();
            ensure!(
                status.is_success(),
                "GET {RELEASES_URL} returned {status}{}",
                if token.is_none() {
                    "; set GITHUB_TOKEN to raise GitHub's rate limit"
                } else {
                    ""
                }
            );
            let page: Vec<GithubRelease> = response.json().await?;
            let last = page.len() < 100;
            releases.extend(page);
            if last {
                break;
            }
        }
        let asset = host_asset()?;
        let published: Vec<&GithubRelease> = releases
            .iter()
            .filter(|r| !r.draft && !r.prerelease && r.has(&asset))
            .collect();
        // Only a release whose spiced can be verified is a base release.
        let mut verifiable = Vec::new();
        for release in published {
            let Some(version) = release.tag_name.strip_prefix('v') else {
                continue;
            };
            if release.has(SUMS) || pinned_digest(version, &asset)?.is_some() {
                verifiable.push(version);
            } else if Version::parse(version).is_ok_and(|v| v.patch.is_some()) {
                println!(
                    "warning: release {} has no SHA256SUMS and no digest in .github/release-digests.json, so `X.Y.x` never resolves to it",
                    release.tag_name
                );
            }
        }
        Ok(Self {
            fetched_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            newest: newest_per_minor(verifiable.into_iter()),
        })
    }

    /// An exact version as is, or `X.Y.x` as the newest X.Y release.
    pub fn resolve(&self, spec: &str) -> Result<String> {
        let version = Version::parse(spec)?;
        if version.patch.is_some() {
            return Ok(spec.to_string());
        }
        self.newest.get(spec).cloned().with_context(|| {
            format!(
                "no published {}.{} release on GitHub (as of {}) has a spiced build for this host; pass --from-release {spec}=X.Y.Z or --from PATH",
                version.major, version.minor, self.fetched_at
            )
        })
    }
}

/// The newest exact version of each minor line, keyed `X.Y.x`. Anything
/// that isn't `X.Y.Z` (a release candidate, say) is skipped.
fn newest_per_minor<'a>(versions: impl Iterator<Item = &'a str>) -> BTreeMap<String, String> {
    let mut newest: BTreeMap<String, (u64, String)> = BTreeMap::new();
    for text in versions {
        let Ok(Version {
            major,
            minor,
            patch: Some(patch),
        }) = Version::parse(text)
        else {
            continue;
        };
        let entry = newest
            .entry(format!("{major}.{minor}.x"))
            .or_insert((patch, text.to_string()));
        if patch > entry.0 {
            *entry = (patch, text.to_string());
        }
    }
    newest
        .into_iter()
        .map(|(spec, (_, text))| (spec, text))
        .collect()
}

fn cache_dir() -> Result<PathBuf> {
    let home = std::env::var("HOME").context("HOME is not set")?;
    Ok(PathBuf::from(home).join(".cache/spicescript"))
}

/// The release asset holding this host's `spiced`.
fn host_asset() -> Result<String> {
    let os = match std::env::consts::OS {
        "macos" => "darwin",
        os => os,
    };
    let arch = std::env::consts::ARCH;
    ensure!(
        matches!(
            (os, arch),
            ("linux", "x86_64" | "aarch64") | ("darwin", "aarch64")
        ),
        "no spiced release build for {os}/{arch}; pass --from PATH"
    );
    Ok(format!("spiced_{os}_{arch}.tar.gz"))
}

/// The `spiced` of an OSS release for this host. Its archive is downloaded once
/// into `~/.cache/spicescript/verified/v<version>/`, verified each time it is
/// used, and unpacked afresh into a directory of this process's own; the
/// binary is then checked by its `--version`. A cache from before verification
/// (`~/.cache/spicescript/spiced-<version>/`) is never used.
pub async fn fetch(version: &str) -> Result<PathBuf> {
    ensure!(
        Version::parse(version)?.patch.is_some(),
        "base release '{version}' must be an exact X.Y.Z release"
    );
    let asset = host_asset()?;
    let cache = cache_dir()?.join("verified").join(format!("v{version}"));
    let archive = cache.join(&asset);
    tokio::fs::create_dir_all(&cache).await?;
    let work = scratch_dir(&format!("spiced-{version}")).await?;
    let expected = expected_digest(version, &asset, &work).await?;
    // The copy that is checked is the one unpacked, so another job on the host
    // replacing the cached archive in between changes nothing.
    let local = work.join(&asset);
    let mut verified = false;
    if tokio::fs::try_exists(&archive).await? {
        tokio::fs::copy(&archive, &local).await?;
        match check_archive(&local, &expected).await {
            Ok(()) => verified = true,
            Err(e) => {
                println!(
                    "warning: discarding the cached {}: {e:#}",
                    archive.display()
                );
                if let Err(e) = tokio::fs::remove_file(&archive).await
                    && e.kind() != std::io::ErrorKind::NotFound
                {
                    return Err(e.into());
                }
            }
        }
    }
    if !verified {
        download(version, &asset, &local).await?;
        check_archive(&local, &expected).await?;
        let staged = cache.join(format!("{asset}.partial-{}", std::process::id()));
        tokio::fs::copy(&local, &staged).await?;
        tokio::fs::rename(&staged, &archive).await?;
    }
    let binary = unpack(&local, &work).await?;
    let output = tokio::process::Command::new(&binary)
        .arg("--version")
        .output()
        .await
        .with_context(|| {
            format!(
                "Failed to check base release v{version} at {}",
                binary.display()
            )
        })?;
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    ensure!(
        output.status.success() && text.contains(&format!("v{version}")),
        "base release {} --version must contain v{version}; got status {}: {}",
        binary.display(),
        output.status,
        text.trim()
    );
    Ok(binary)
}

/// curl flags for a GitHub release download. GitHub's release downloads answer
/// 504 in bursts, so a single attempt fails a whole script partition. With no
/// `--retry-delay`, curl waits 1s and doubles each time, so 8 retries span
/// about 4 minutes; `--retry-max-time 300` bounds that. `-f` still fails a 404
/// after the retries, so a missing asset is reported, not hidden.
pub const CURL_RELEASE_RETRY: [&str; 5] = [
    "--retry",
    "8",
    "--retry-all-errors",
    "--retry-max-time",
    "300",
];

/// What a release's archive must be: its SHA-256, and whether its build
/// attestation must also verify (a release with SHA256SUMS).
struct Expected {
    sha256: String,
    attested: bool,
}

/// The pinned digest of a release published before SHA256SUMS, or else the
/// digest its SHA256SUMS lists, once SHA256SUMS's own attestation verifies.
async fn expected_digest(version: &str, asset: &str, work: &Path) -> Result<Expected> {
    if let Some(sha256) = pinned_digest(version, asset)? {
        return Ok(Expected {
            sha256,
            attested: false,
        });
    }
    let sums = work.join(SUMS);
    let url = format!("https://github.com/spiceai/spiceai/releases/download/v{version}/{SUMS}");
    let status = tokio::process::Command::new("curl")
        .args(CURL_RELEASE_RETRY)
        .args(["-fsSL", "-o"])
        .arg(&sums)
        .arg(&url)
        .status()
        .await
        .context("Failed to run curl")?;
    ensure!(
        status.success(),
        "v{version} has no digest in .github/release-digests.json and its {SUMS} could not be downloaded from {url}, so its spiced cannot be verified"
    );
    verify_attestation(&sums).await?;
    let text = tokio::fs::read_to_string(&sums).await?;
    let sha256 =
        sums_digest(&text, asset).with_context(|| format!("{url} does not list {asset}"))?;
    Ok(Expected {
        sha256,
        attested: true,
    })
}

/// The digest a SHA256SUMS file lists for `asset` (`<digest>  <name>` or
/// `<digest> *<name>`).
fn sums_digest(text: &str, asset: &str) -> Option<String> {
    text.lines().find_map(|line| {
        let (digest, name) = line.split_once(char::is_whitespace)?;
        let name = name.trim_start();
        let name = name.strip_prefix('*').unwrap_or(name);
        (name == asset && is_sha256(digest)).then(|| digest.to_string())
    })
}

/// Fails unless the file carries a build attestation from `build_release.yml`
/// on a release branch, on a GitHub-hosted runner.
async fn verify_attestation(file: &Path) -> Result<()> {
    let output = tokio::process::Command::new("gh")
        .args(["attestation", "verify"])
        .arg(file)
        .args([
            "--repo",
            "spiceai/spiceai",
            "--cert-identity-regex",
            RELEASE_SIGNER,
            "--deny-self-hosted-runners",
        ])
        .output()
        .await
        .context("Failed to run gh, which checks a release file's build attestation; install the GitHub CLI")?;
    ensure!(
        output.status.success(),
        "{} has no build attestation from build_release.yml on a release branch: {}",
        file.display(),
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file =
        std::fs::File::open(path).with_context(|| format!("Failed to open {}", path.display()))?;
    let mut hasher = Sha256::new();
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

/// Fails unless the archive has the expected digest and, for a release with
/// SHA256SUMS, its own build attestation.
async fn check_archive(archive: &Path, expected: &Expected) -> Result<()> {
    let path = archive.to_path_buf();
    let actual = tokio::task::spawn_blocking(move || sha256_file(&path)).await??;
    ensure!(
        actual == expected.sha256,
        "{} has SHA-256 {actual}, not the release's {}",
        archive.display(),
        expected.sha256
    );
    if expected.attested {
        verify_attestation(archive).await?;
    }
    Ok(())
}

/// A directory of this process's own, under `RUNNER_TEMP` when it is set.
async fn scratch_dir(name: &str) -> Result<PathBuf> {
    let base = std::env::var_os("RUNNER_TEMP").map_or_else(std::env::temp_dir, PathBuf::from);
    let dir = base
        .join(format!("spicescript-{}", std::process::id()))
        .join(name);
    if tokio::fs::try_exists(&dir).await? {
        tokio::fs::remove_dir_all(&dir).await?;
    }
    tokio::fs::create_dir_all(&dir)
        .await
        .with_context(|| format!("Failed to create {}", dir.display()))?;
    Ok(dir)
}

async fn download(version: &str, asset: &str, archive: &Path) -> Result<()> {
    let url = format!("https://github.com/spiceai/spiceai/releases/download/v{version}/{asset}");
    let status = tokio::process::Command::new("curl")
        .args(CURL_RELEASE_RETRY)
        .args(["-fsSL", "-o"])
        .arg(archive)
        .arg(&url)
        .status()
        .await
        .context("Failed to run curl")?;
    ensure!(
        status.success(),
        "Failed to download spiced v{version} from {url}"
    );
    Ok(())
}

/// Unpacks `spiced` from the verified archive in `work` into `work`.
async fn unpack(archive: &Path, work: &Path) -> Result<PathBuf> {
    let status = tokio::process::Command::new("tar")
        .arg("-xzf")
        .arg(archive)
        .arg("-C")
        .arg(work)
        .status()
        .await
        .context("Failed to run tar")?;
    let binary = work.join("spiced");
    let metadata = tokio::fs::symlink_metadata(&binary).await;
    if !status.success() || !metadata.is_ok_and(|m| m.is_file()) {
        bail!("Failed to unpack spiced from {}", archive.display());
    }
    tokio::fs::remove_file(archive).await?;
    Ok(binary)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_pinned_digest_names_an_exact_release_and_a_spiced_archive() {
        let digests: ReleaseDigests = serde_json::from_str(RELEASE_DIGESTS).expect("valid JSON");
        assert!(!digests.releases.is_empty());
        for (tag, assets) in &digests.releases {
            let version = tag.strip_prefix('v').expect("a v-prefixed tag");
            assert!(is_exact(version).expect("a valid version"), "{tag}");
            for (asset, digest) in assets {
                assert!(
                    asset.starts_with("spiced_") && asset.ends_with(".tar.gz"),
                    "{tag} {asset}"
                );
                assert_eq!(
                    pinned_digest(version, asset).expect("valid"),
                    Some(digest.clone())
                );
            }
        }
        assert_eq!(
            pinned_digest("9.9.9", "spiced_linux_x86_64.tar.gz").expect("valid"),
            None
        );
    }

    #[test]
    fn sha256sums_lines_name_their_asset() {
        let a = "a".repeat(64);
        let b = "b".repeat(64);
        let text = format!(
            "{a}  spice_linux_x86_64.tar.gz\n{b} *spiced_linux_x86_64.tar.gz\nnot-a-digest  spiced_darwin_aarch64.tar.gz\n"
        );
        assert_eq!(sums_digest(&text, "spiced_linux_x86_64.tar.gz"), Some(b));
        assert_eq!(sums_digest(&text, "spice_linux_x86_64.tar.gz"), Some(a));
        assert_eq!(sums_digest(&text, "spiced_darwin_aarch64.tar.gz"), None);
        assert_eq!(sums_digest(&text, "spiced_linux_aarch64.tar.gz"), None);
    }

    #[tokio::test]
    async fn an_archive_runs_only_with_the_digest_it_must_have() {
        let dir =
            std::env::temp_dir().join(format!("spicescript-release-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let archive = dir.join("spiced_linux_x86_64.tar.gz");
        std::fs::write(&archive, b"spiced").expect("write");
        // printf spiced | sha256sum
        let actual = sha256_file(&archive).expect("hash");
        assert_eq!(
            actual,
            "677e903199916067c479594e77308560145a863b94342cc471f0043403c92d0e"
        );
        let good = Expected {
            sha256: actual.clone(),
            attested: false,
        };
        check_archive(&archive, &good)
            .await
            .expect("the expected digest");
        let bad = Expected {
            sha256: "0".repeat(64),
            attested: false,
        };
        let error = check_archive(&archive, &bad)
            .await
            .expect_err("another digest")
            .to_string();
        assert!(
            error.contains(&actual) && error.contains(&"0".repeat(64)),
            "{error}"
        );
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[test]
    fn minor_lines_resolve_to_the_highest_numeric_patch() {
        let releases = Releases {
            fetched_at: "2026-10-28T00:00:00Z".to_string(),
            newest: newest_per_minor(
                [
                    "2.3.9",
                    "2.4.0-rc.1",
                    "2.3.10",
                    "2.4.0",
                    "2.2.2",
                    "2.5.0-rc.1",
                ]
                .into_iter(),
            ),
        };
        assert_eq!(
            releases.newest,
            [("2.2.x", "2.2.2"), ("2.3.x", "2.3.10"), ("2.4.x", "2.4.0")]
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .into(),
        );
        assert_eq!(releases.resolve("2.3.x").expect("2.3 published"), "2.3.10");
        assert_eq!(releases.resolve("2.3.1").expect("exact"), "2.3.1");
        assert_eq!(
            releases
                .resolve("2.5.x")
                .expect_err("only a release candidate")
                .to_string(),
            "no published 2.5 release on GitHub (as of 2026-10-28T00:00:00Z) has a spiced build for this host; pass --from-release 2.5.x=X.Y.Z or --from PATH"
        );
    }
}
