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
//! The releases a script can start on are the ones `.github/release-digests.json`
//! records, a file only a reviewed trunk change edits: `X.Y.x` resolves among
//! them, and each one's `spiced` archive must match its recorded digest. A
//! release that also publishes SHA256SUMS is checked against it too, and
//! SHA256SUMS and the archive must each carry a build attestation from
//! `build_release.yml` on a release branch, for the commit its tag names. Off
//! trunk, outside a job with the full CI roles, an exact release that is not
//! recorded may run on SHA256SUMS and the attestations alone. A cached archive
//! is verified the same way each time it is used, and `spiced` is unpacked from
//! it afresh.

use std::{
    collections::BTreeMap,
    io::Read,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;
use sha2::{Digest, Sha256};

/// The digests of the recorded releases, reviewed on trunk.
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

/// The recorded releases `X.Y.x` resolves among: those
/// `.github/release-digests.json` records a `spiced` archive for this host.
pub struct Releases {
    /// What each `X.Y.x` resolves to: the newest recorded X.Y release.
    newest: BTreeMap<String, String>,
}

impl Releases {
    /// The recorded releases, read from `.github/release-digests.json`.
    pub fn load() -> Result<Self> {
        let digests: ReleaseDigests = serde_json::from_str(RELEASE_DIGESTS)
            .context("Failed to read .github/release-digests.json")?;
        let asset = host_asset()?;
        let versions = digests
            .releases
            .iter()
            .filter(|(_, assets)| assets.contains_key(&asset))
            .filter_map(|(tag, _)| tag.strip_prefix('v'));
        Ok(Self {
            newest: newest_per_minor(versions),
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
                "no {}.{} release with a spiced build for this host is recorded in .github/release-digests.json; record it with a trunk change, or pass --from-release {spec}=X.Y.Z or --from PATH",
                version.major, version.minor
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

/// What a release's archive must be: its SHA-256, and for a release with
/// SHA256SUMS, the commit its tag names, which the archive's build attestation
/// must record.
#[derive(Debug)]
struct Expected {
    sha256: String,
    attested_commit: Option<String>,
}

/// Whether this run trusts only the recorded digests: a GitHub Actions run on
/// trunk, or one with the full CI roles (`CI_ROLE_FULL`).
fn recorded_only() -> bool {
    let var = |name: &str| std::env::var(name).ok();
    recorded_only_for(
        var("GITHUB_ACTIONS").as_deref(),
        var("GITHUB_REF").as_deref(),
        var("CI_ROLE_FULL").as_deref(),
    )
}

fn recorded_only_for(
    actions: Option<&str>,
    git_ref: Option<&str>,
    full_role: Option<&str>,
) -> bool {
    actions == Some("true") && (git_ref == Some("refs/heads/trunk") || full_role == Some("true"))
}

/// The recorded digest of the archive, checked against the release's
/// SHA256SUMS and its attestation when it publishes one. A release that is not
/// recorded is refused on trunk and with the full CI roles; elsewhere its
/// attested SHA256SUMS decides.
async fn expected_digest(version: &str, asset: &str, work: &Path) -> Result<Expected> {
    let pinned = pinned_digest(version, asset)?;
    if pinned.is_none() && recorded_only() {
        bail!(
            "v{version} is not recorded in .github/release-digests.json, and a run on trunk or with the full CI roles runs only a recorded release; record it with a trunk change"
        );
    }
    let sums = work.join(SUMS);
    let from_sums = if download_sums(version, &sums).await? {
        let commit = tag_commit(version).await?;
        verify_attestation(&sums, &commit).await?;
        let text = tokio::fs::read_to_string(&sums).await?;
        let digest = sums_digest(&text, asset)
            .with_context(|| format!("the {SUMS} of v{version} does not list {asset}"))?;
        Some((digest, commit))
    } else {
        None
    };
    decide(version, asset, pinned, from_sums)
}

/// The recorded digest wins; SHA256SUMS, when the release has one, must agree
/// with it, and its attestation's commit is then required of the archive too.
fn decide(
    version: &str,
    asset: &str,
    pinned: Option<String>,
    from_sums: Option<(String, String)>,
) -> Result<Expected> {
    match (pinned, from_sums) {
        (Some(pinned), None) => Ok(Expected {
            sha256: pinned,
            attested_commit: None,
        }),
        (Some(pinned), Some((listed, commit))) => {
            ensure!(
                pinned == listed,
                "v{version} {asset} is recorded as {pinned} in .github/release-digests.json, but its {SUMS} lists {listed}"
            );
            Ok(Expected {
                sha256: pinned,
                attested_commit: Some(commit),
            })
        }
        (None, Some((listed, commit))) => Ok(Expected {
            sha256: listed,
            attested_commit: Some(commit),
        }),
        (None, None) => bail!(
            "v{version} is not recorded in .github/release-digests.json and publishes no {SUMS}, so its spiced cannot be verified"
        ),
    }
}

/// Downloads the release's SHA256SUMS: whether it has one (a 404 means none).
async fn download_sums(version: &str, sums: &Path) -> Result<bool> {
    let url = format!("https://github.com/spiceai/spiceai/releases/download/v{version}/{SUMS}");
    let output = tokio::process::Command::new("curl")
        .args(CURL_RELEASE_RETRY)
        .args(["-sSL", "-w", "%{http_code}", "-o"])
        .arg(sums)
        .arg(&url)
        .output()
        .await
        .context("Failed to run curl")?;
    match String::from_utf8_lossy(&output.stdout).trim() {
        "200" if output.status.success() => Ok(true),
        "404" => Ok(false),
        code => bail!(
            "Failed to download {url} (HTTP {code}): {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ),
    }
}

/// The commit the release tag `v{version}` names, read by the tag's full name.
async fn tag_commit(version: &str) -> Result<String> {
    let output = tokio::process::Command::new("gh")
        .args([
            "api",
            &format!("repos/spiceai/spiceai/commits/refs/tags/v{version}"),
            "--jq",
            ".sha",
        ])
        .output()
        .await
        .context(
            "Failed to run gh, which reads the commit a release tag names; install the GitHub CLI",
        )?;
    let commit = String::from_utf8_lossy(&output.stdout).trim().to_string();
    ensure!(
        output.status.success()
            && commit.len() == 40
            && commit.bytes().all(|b| b.is_ascii_hexdigit()),
        "Failed to read the commit v{version} names: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(commit)
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
/// on a release branch, on a GitHub-hosted runner, for `commit`.
async fn verify_attestation(file: &Path, commit: &str) -> Result<()> {
    let output = tokio::process::Command::new("gh")
        .args(["attestation", "verify"])
        .arg(file)
        .args([
            "--repo",
            "spiceai/spiceai",
            "--cert-identity-regex",
            RELEASE_SIGNER,
            "--deny-self-hosted-runners",
            "--source-digest",
            commit,
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
    if let Some(commit) = &expected.attested_commit {
        verify_attestation(archive, commit).await?;
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
            attested_commit: None,
        };
        check_archive(&archive, &good)
            .await
            .expect("the expected digest");
        let bad = Expected {
            sha256: "0".repeat(64),
            attested_commit: None,
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
            "no 2.5 release with a spiced build for this host is recorded in .github/release-digests.json; record it with a trunk change, or pass --from-release 2.5.x=X.Y.Z or --from PATH"
        );
    }

    #[test]
    fn x_y_x_resolves_only_among_recorded_releases() {
        let releases = Releases::load().expect("the recorded releases");
        assert_eq!(releases.resolve("2.4.x").expect("2.4 recorded"), "2.4.3");
        assert_eq!(releases.resolve("2.3.x").expect("2.3 recorded"), "2.3.4");
        releases
            .resolve("2.2.x")
            .expect_err("no 2.2 release is recorded");
    }

    #[test]
    fn only_a_ci_run_off_trunk_without_the_full_roles_may_run_an_unrecorded_release() {
        assert!(recorded_only_for(
            Some("true"),
            Some("refs/heads/trunk"),
            None
        ));
        assert!(recorded_only_for(
            Some("true"),
            Some("refs/pull/1/merge"),
            Some("true")
        ));
        assert!(!recorded_only_for(
            Some("true"),
            Some("refs/pull/1/merge"),
            None
        ));
        assert!(!recorded_only_for(
            Some("true"),
            Some("refs/heads/release/2.4"),
            Some("false")
        ));
        assert!(!recorded_only_for(None, Some("refs/heads/trunk"), None));
    }

    #[test]
    fn the_recorded_digest_wins_and_sha256sums_must_agree_with_it() {
        let a = "a".repeat(64);
        let b = "b".repeat(64);
        let commit = "c".repeat(40);
        let asset = "spiced_linux_x86_64.tar.gz";
        let pinned = decide("2.4.3", asset, Some(a.clone()), None).expect("recorded");
        assert_eq!(
            (pinned.sha256.as_str(), pinned.attested_commit),
            (a.as_str(), None)
        );
        let both = decide(
            "2.5.0",
            asset,
            Some(a.clone()),
            Some((a.clone(), commit.clone())),
        )
        .expect("agree");
        assert_eq!(
            (both.sha256.as_str(), both.attested_commit.as_deref()),
            (a.as_str(), Some(commit.as_str()))
        );
        let error = decide("2.5.0", asset, Some(a), Some((b.clone(), commit.clone())))
            .expect_err("disagree")
            .to_string();
        assert!(
            error.contains("is recorded as") && error.contains(&b),
            "{error}"
        );
        let sums =
            decide("2.5.1", asset, None, Some((b.clone(), commit.clone()))).expect("attested sums");
        assert_eq!(
            (sums.sha256.as_str(), sums.attested_commit.as_deref()),
            (b.as_str(), Some(commit.as_str()))
        );
        decide("2.5.2", asset, None, None).expect_err("neither");
    }
}
