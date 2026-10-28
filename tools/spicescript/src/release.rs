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

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use serde::Deserialize;

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

/// The release a run starts on: the script's `from:`, the exact version
/// it resolved to, and the downloaded binary.
#[derive(Clone)]
pub struct BaseRelease {
    pub spec: String,
    pub version: String,
    pub binary: PathBuf,
}

#[derive(Deserialize)]
struct ReleaseList {
    releases: Vec<String>,
}

/// `releases.json`: the releases `X.Y.x` resolves among.
pub struct Releases {
    path: PathBuf,
    versions: Vec<String>,
}

impl Releases {
    /// The nearest `releases.json` at or above `near`.
    pub fn find(near: &Path) -> Result<Self> {
        let dir = std::path::absolute(near)?;
        let path = dir
            .ancestors()
            .map(|d| d.join("releases.json"))
            .find(|p| p.is_file())
            .with_context(|| {
                format!(
                    "`from:` requires releases.json; looked in {} and every parent directory",
                    dir.display()
                )
            })?;
        let list: ReleaseList = serde_json::from_str(
            &std::fs::read_to_string(&path)
                .with_context(|| format!("Failed to read {}", path.display()))?,
        )
        .with_context(|| format!("Invalid {}", path.display()))?;
        for text in &list.releases {
            ensure!(
                Version::parse(text)?.patch.is_some(),
                "{} lists non-exact release '{text}'",
                path.display()
            );
        }
        Ok(Self {
            path,
            versions: list.releases,
        })
    }

    /// An exact version as is, or `X.Y.x` as the newest listed X.Y release.
    pub fn resolve(&self, text: &str) -> Result<String> {
        let version = Version::parse(text)?;
        if version.patch.is_some() {
            return Ok(text.to_string());
        }
        let mut newest = None;
        for text in &self.versions {
            let candidate = Version::parse(text)?;
            if candidate.major == version.major
                && candidate.minor == version.minor
                && let Some(patch) = candidate.patch
                && newest.as_ref().is_none_or(|(n, _)| patch > *n)
            {
                newest = Some((patch, text));
            }
        }
        newest.map(|(_, text)| text.clone()).with_context(|| {
            format!(
                "no {}.{} release is listed in {}; add it there",
                version.major,
                version.minor,
                self.path.display()
            )
        })
    }
}

/// The `spiced` of an OSS release for this host, downloaded once into
/// `~/.cache/spicescript/spiced-<version>/` and checked by its `--version`.
pub async fn fetch(version: &str) -> Result<PathBuf> {
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
    let home = std::env::var("HOME").context("HOME is not set")?;
    let cache = PathBuf::from(home).join(".cache/spicescript");
    let dir = cache.join(format!("spiced-{version}"));
    let binary = dir.join("spiced");
    if !tokio::fs::try_exists(&dir).await? {
        tokio::fs::create_dir_all(&cache).await?;
        let partial = cache.join(format!("spiced-{version}.partial-{}", std::process::id()));
        tokio::fs::create_dir(&partial)
            .await
            .with_context(|| format!("Failed to create {}", partial.display()))?;
        let result = download(version, os, arch, &partial, &dir).await;
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

async fn download(version: &str, os: &str, arch: &str, partial: &Path, dir: &Path) -> Result<()> {
    let archive = partial.join("spiced.tar.gz");
    let url = format!(
        "https://github.com/spiceai/spiceai/releases/download/v{version}/spiced_{os}_{arch}.tar.gz"
    );
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
    fn minor_lines_choose_the_highest_numeric_patch() {
        let releases = Releases {
            path: PathBuf::from("releases.json"),
            versions: ["2.3.10", "2.3.9", "2.4.0"].map(str::to_string).to_vec(),
        };
        assert_eq!(releases.resolve("2.3.x").expect("2.3 listed"), "2.3.10");
        assert_eq!(releases.resolve("2.4.x").expect("2.4 listed"), "2.4.0");
        assert_eq!(
            releases
                .resolve("2.5.x")
                .expect_err("not listed")
                .to_string(),
            "no 2.5 release is listed in releases.json; add it there"
        );
    }
}
