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

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

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
        Ok(Self {
            fetched_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            newest: newest_per_minor(
                releases
                    .iter()
                    .filter(|r| {
                        !r.draft && !r.prerelease && r.assets.iter().any(|a| a.name == asset)
                    })
                    .filter_map(|r| r.tag_name.strip_prefix('v')),
            ),
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

/// The `spiced` of an OSS release for this host, downloaded once into
/// `~/.cache/spicescript/spiced-<version>/` and checked by its `--version`.
pub async fn fetch(version: &str) -> Result<PathBuf> {
    let asset = host_asset()?;
    let cache = cache_dir()?;
    let dir = cache.join(format!("spiced-{version}"));
    let binary = dir.join("spiced");
    if !tokio::fs::try_exists(&dir).await? {
        tokio::fs::create_dir_all(&cache).await?;
        let partial = cache.join(format!("spiced-{version}.partial-{}", std::process::id()));
        tokio::fs::create_dir(&partial)
            .await
            .with_context(|| format!("Failed to create {}", partial.display()))?;
        let result = download(version, &asset, &partial, &dir).await;
        let cleanup = tokio::fs::remove_dir_all(&partial).await;
        result?;
        if let Err(e) = cleanup
            && e.kind() != std::io::ErrorKind::NotFound
        {
            return Err(e.into());
        }
    }
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

async fn download(version: &str, asset: &str, partial: &Path, dir: &Path) -> Result<()> {
    let archive = partial.join("spiced.tar.gz");
    let url = format!("https://github.com/spiceai/spiceai/releases/download/v{version}/{asset}");
    let status = tokio::process::Command::new("curl")
        .args(["-fsSL", "-o"])
        .arg(&archive)
        .arg(&url)
        .status()
        .await
        .context("Failed to run curl")?;
    ensure!(
        status.success(),
        "Failed to download spiced v{version} from {url}"
    );
    let status = tokio::process::Command::new("tar")
        .arg("-xzf")
        .arg(&archive)
        .arg("-C")
        .arg(partial)
        .status()
        .await
        .context("Failed to run tar")?;
    ensure!(
        status.success() && partial.join("spiced").is_file(),
        "Failed to unpack spiced v{version} from {}",
        archive.display()
    );
    tokio::fs::remove_file(archive).await?;
    if let Err(e) = tokio::fs::rename(partial, dir).await {
        ensure!(
            tokio::fs::try_exists(dir).await?,
            "Failed to publish base release cache {}: {e}",
            dir.display()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
