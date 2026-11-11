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

//! Nightly builds: the `spice` and `spiced` binaries the `build_nightly` workflow uploads as
//! artifacts of its daily run on `trunk`. GitHub publishes no release for them, keeps them only
//! for the workflow's retention period, and serves them only to signed-in users, so a build is
//! found by listing the workflow's runs and downloaded with a GitHub token.

use std::fmt::Write as _;
use std::io::Read;

use chrono::{DateTime, NaiveDate, Utc};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use super::{GITHUB_API_BASE, GitHubClient, GitHubError};

const WORKFLOW: &str = "build_nightly.yml";
const BRANCH: &str = "trunk";
/// Runs listed per search: two months of daily runs, more than GitHub keeps artifacts for.
const RUNS_LISTED: u32 = 60;
/// Most runs whose artifacts are read when no date is given: a platform's build seldom fails
/// several nights running.
const RUNS_SEARCHED: usize = 10;
/// Most dates an error lists.
const DATES_LISTED: usize = 10;

/// One run of the nightly workflow.
#[derive(Debug, Clone, Deserialize)]
pub struct WorkflowRun {
    pub id: u64,
    pub head_sha: String,
    pub created_at: DateTime<Utc>,
    pub html_url: String,
    pub conclusion: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WorkflowRuns {
    workflow_runs: Vec<WorkflowRun>,
}

/// One file a run uploaded: a zip archive of a `.tar.gz` that holds one binary.
#[derive(Debug, Clone, Deserialize)]
pub struct Artifact {
    pub name: String,
    pub size_in_bytes: u64,
    pub expired: bool,
    pub expires_at: Option<DateTime<Utc>>,
    /// `sha256:<hex>` of the zip archive.
    pub digest: Option<String>,
    pub archive_download_url: String,
}

#[derive(Debug, Deserialize)]
struct Artifacts {
    artifacts: Vec<Artifact>,
}

/// A nightly build's CLI and runtime for this machine's platform.
#[derive(Debug, Clone)]
pub struct NightlyBuild {
    pub run: WorkflowRun,
    pub cli: Artifact,
    pub runtime: Artifact,
}

impl NightlyBuild {
    /// The UTC date the build ran, which names it.
    #[must_use]
    pub fn date(&self) -> NaiveDate {
        self.run.created_at.date_naive()
    }

    /// The build's name for messages: `2026-11-10 (trunk 6d499cec5d)`.
    #[must_use]
    pub fn describe(&self) -> String {
        format!("{} (trunk {})", self.date(), short_sha(&self.run.head_sha))
    }

    /// What a binary of this build has in its version, as in
    /// `v2.5.0-unstable.nightly.20261110.g6d499ce`.
    #[must_use]
    pub fn version_marker(&self) -> String {
        format!(
            "nightly.{}.g{}",
            self.date().format("%Y%m%d"),
            self.run.head_sha.get(..7).unwrap_or(&self.run.head_sha)
        )
    }
}

fn short_sha(sha: &str) -> &str {
    sha.get(..10).unwrap_or(sha)
}

/// Why no nightly build can be installed. Each message names the fix.
#[derive(Debug)]
pub enum NightlyError {
    UnsupportedPlatform {
        os: String,
        arch: String,
    },
    InvalidDate {
        value: String,
    },
    NeedsToken,
    NoBuildOn {
        date: NaiveDate,
        available: Vec<NaiveDate>,
    },
    NoBinaries {
        date: NaiveDate,
        platform: &'static str,
        url: String,
        available: Vec<NaiveDate>,
    },
    Expired {
        date: NaiveDate,
        available: Vec<NaiveDate>,
    },
    NoneAvailable {
        platform: &'static str,
    },
    DigestMismatch {
        name: String,
        expected: String,
        actual: String,
    },
    Archive {
        name: String,
        message: String,
    },
    GitHub(GitHubError),
}

impl std::fmt::Display for NightlyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedPlatform { os, arch } => write!(
                f,
                "Spice nightly builds are published for Linux x86_64, Linux aarch64 and macOS aarch64, not for {os} {arch}. Install a release with `spice install` instead."
            ),
            Self::InvalidDate { value } => write!(
                f,
                "'{value}' is not a date. Name a nightly build by the UTC date it ran, as YYYY-MM-DD, for example `spice install nightly 2026-11-10`."
            ),
            Self::NeedsToken => write!(
                f,
                "GitHub serves nightly builds only to signed-in users. Sign in with `gh auth login`, or set GITHUB_TOKEN to a GitHub token, then rerun the command."
            ),
            Self::NoBuildOn { date, available } => {
                write!(f, "No Spice nightly build ran on {date}.")?;
                write_available(f, available)
            }
            Self::NoBinaries {
                date,
                platform,
                url,
                available,
            } => {
                write!(
                    f,
                    "The Spice nightly build of {date} has no {platform} binaries, because that build did not finish: {url}."
                )?;
                write_available(f, available)
            }
            Self::Expired { date, available } => {
                write!(
                    f,
                    "The Spice nightly build of {date} has expired: GitHub deletes a nightly build's binaries after the workflow's retention period."
                )?;
                write_available(f, available)
            }
            Self::NoneAvailable { platform } => write!(
                f,
                "No Spice nightly build with {platform} binaries is available. Install a release with `spice install` instead."
            ),
            Self::DigestMismatch {
                name,
                expected,
                actual,
            } => write!(
                f,
                "The download of '{name}' does not match the sha256 GitHub published for it (expected {expected}, got {actual}). Rerun the command."
            ),
            Self::Archive { name, message } => {
                write!(
                    f,
                    "The download of '{name}' is not a nightly build archive: {message}"
                )
            }
            Self::GitHub(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for NightlyError {}

impl From<GitHubError> for NightlyError {
    fn from(e: GitHubError) -> Self {
        Self::GitHub(e)
    }
}

fn write_available(f: &mut std::fmt::Formatter<'_>, available: &[NaiveDate]) -> std::fmt::Result {
    if available.is_empty() {
        return write!(f, " No complete nightly build is available.");
    }
    let mut dates = String::new();
    for (i, date) in available.iter().enumerate() {
        if i > 0 {
            dates.push_str(", ");
        }
        let _ = write!(dates, "{date}");
    }
    write!(
        f,
        " Complete nightly builds: {dates}. Install one with `spice install nightly <date>`."
    )
}

/// The suffix of this platform's artifact names, as in `spiced_linux_aarch64`.
///
/// # Errors
///
/// Returns [`NightlyError::UnsupportedPlatform`] for a platform the nightly workflow does not
/// build.
pub fn platform(os: &str, arch: &str) -> Result<&'static str, NightlyError> {
    match (os, arch) {
        ("linux", "x86_64") => Ok("linux_x86_64"),
        ("linux", "aarch64") => Ok("linux_aarch64"),
        ("macos", "aarch64") => Ok("darwin_aarch64"),
        _ => Err(NightlyError::UnsupportedPlatform {
            os: os.to_string(),
            arch: arch.to_string(),
        }),
    }
}

/// A nightly build's date, as `YYYY-MM-DD` or `YYYYMMDD`.
///
/// # Errors
///
/// Returns [`NightlyError::InvalidDate`] for anything else.
pub fn parse_date(value: &str) -> Result<NaiveDate, NightlyError> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .or_else(|_| NaiveDate::parse_from_str(value, "%Y%m%d"))
        .map_err(|_| NightlyError::InvalidDate {
            value: value.to_string(),
        })
}

/// Where the nightly workflow's runs and their artifacts are read from.
pub(crate) trait NightlySource {
    /// The workflow's completed runs on `trunk`, newest first.
    async fn runs(&self) -> Result<Vec<WorkflowRun>, GitHubError>;
    /// The artifacts of run `id`.
    async fn artifacts(&self, id: u64) -> Result<Vec<Artifact>, GitHubError>;
}

impl NightlySource for GitHubClient {
    async fn runs(&self) -> Result<Vec<WorkflowRun>, GitHubError> {
        let url = format!(
            "{GITHUB_API_BASE}/repos/{}/{}/actions/workflows/{WORKFLOW}/runs?branch={BRANCH}&status=completed&per_page={RUNS_LISTED}",
            self.owner, self.repo
        );
        Ok(self.get::<WorkflowRuns>(&url).await?.workflow_runs)
    }

    async fn artifacts(&self, id: u64) -> Result<Vec<Artifact>, GitHubError> {
        let url = format!(
            "{GITHUB_API_BASE}/repos/{}/{}/actions/runs/{id}/artifacts?per_page=100",
            self.owner, self.repo
        );
        Ok(self.get::<Artifacts>(&url).await?.artifacts)
    }
}

/// What one run holds for `platform`.
enum Found {
    Build(Box<NightlyBuild>),
    Expired,
    Missing,
}

async fn look(
    source: &impl NightlySource,
    run: &WorkflowRun,
    platform: &str,
) -> Result<Found, GitHubError> {
    let artifacts = source.artifacts(run.id).await?;
    let named = |prefix: &str| {
        let name = format!("{prefix}_{platform}");
        artifacts.iter().find(|a| a.name == name).cloned()
    };
    Ok(match (named("spice"), named("spiced")) {
        (Some(cli), Some(runtime)) if !cli.expired && !runtime.expired => {
            Found::Build(Box::new(NightlyBuild {
                run: run.clone(),
                cli,
                runtime,
            }))
        }
        (Some(_), Some(_)) => Found::Expired,
        _ => Found::Missing,
    })
}

/// The dates of the successful runs whose artifacts GitHub still keeps, newest first. How long
/// it keeps them is read off the newest successful run's artifacts.
async fn available(
    source: &impl NightlySource,
    runs: &[WorkflowRun],
    now: DateTime<Utc>,
) -> Result<Vec<NaiveDate>, GitHubError> {
    let succeeded: Vec<&WorkflowRun> = runs
        .iter()
        .filter(|run| run.conclusion.as_deref() == Some("success"))
        .collect();
    let Some(newest) = succeeded.first() else {
        return Ok(Vec::new());
    };
    let retention = source
        .artifacts(newest.id)
        .await?
        .iter()
        .find_map(|a| a.expires_at)
        .map(|expires| expires - newest.created_at);
    let mut dates: Vec<NaiveDate> = succeeded
        .iter()
        .filter(|run| retention.is_none_or(|kept| run.created_at + kept > now))
        .map(|run| run.created_at.date_naive())
        .collect();
    dates.dedup();
    dates.truncate(DATES_LISTED);
    Ok(dates)
}

/// The nightly build to install for `platform`: the one that ran on `date`, or the newest one
/// with this platform's binaries.
///
/// # Errors
///
/// Returns why there is none, listing the dates there are builds for, or a GitHub API error.
pub(crate) async fn find(
    source: &impl NightlySource,
    date: Option<NaiveDate>,
    platform: &'static str,
    now: DateTime<Utc>,
) -> Result<NightlyBuild, NightlyError> {
    let runs = source.runs().await?;
    let Some(date) = date else {
        for run in runs.iter().take(RUNS_SEARCHED) {
            if let Found::Build(build) = look(source, run, platform).await? {
                return Ok(*build);
            }
        }
        return Err(NightlyError::NoneAvailable { platform });
    };
    let on_date: Vec<&WorkflowRun> = runs
        .iter()
        .filter(|run| run.created_at.date_naive() == date)
        .collect();
    let mut expired = false;
    for run in &on_date {
        match look(source, run, platform).await? {
            Found::Build(build) => return Ok(*build),
            Found::Expired => expired = true,
            Found::Missing => {}
        }
    }
    let available = available(source, &runs, now).await?;
    Err(match on_date.first() {
        None => NightlyError::NoBuildOn { date, available },
        Some(_) if expired => NightlyError::Expired { date, available },
        Some(run) => NightlyError::NoBinaries {
            date,
            platform,
            url: run.html_url.clone(),
            available,
        },
    })
}

/// Downloads `artifact`, checks it against the size and sha256 GitHub published for it, and
/// returns the one binary it holds, `binary`.
///
/// # Errors
///
/// Returns an error when the download fails or does not match, or the archive does not hold
/// `binary`.
pub async fn download_binary(
    client: &GitHubClient,
    artifact: &Artifact,
    binary: &str,
) -> Result<Vec<u8>, NightlyError> {
    let total = artifact.size_in_bytes;
    #[expect(
        clippy::cast_precision_loss,
        reason = "a download's size, shown to one decimal"
    )]
    let megabytes = |bytes: u64| bytes as f64 / 1_048_576.0;
    let zip = client
        .download_artifact(artifact, |downloaded| {
            eprint!(
                "\rDownloading {}: {:.1} of {:.1} MB",
                artifact.name,
                megabytes(downloaded),
                megabytes(total)
            );
        })
        .await;
    eprintln!();
    let zip = zip?;
    if let Some(expected) = artifact
        .digest
        .as_deref()
        .and_then(|d| d.strip_prefix("sha256:"))
    {
        let actual = format!("{:x}", Sha256::digest(&zip));
        if !actual.eq_ignore_ascii_case(expected) {
            return Err(NightlyError::DigestMismatch {
                name: artifact.name.clone(),
                expected: expected.to_string(),
                actual,
            });
        }
    }
    let archive_error = |message: String| NightlyError::Archive {
        name: artifact.name.clone(),
        message,
    };
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(zip))
        .map_err(|e| archive_error(e.to_string()))?;
    let inner = format!("{}.tar.gz", artifact.name);
    let mut tar_gz = Vec::new();
    archive
        .by_name(&inner)
        .map_err(|e| archive_error(format!("no {inner}: {e}")))?
        .read_to_end(&mut tar_gz)
        .map_err(|e| archive_error(e.to_string()))?;
    let mut tar = tar::Archive::new(flate2::read::GzDecoder::new(tar_gz.as_slice()));
    for entry in tar.entries().map_err(|e| archive_error(e.to_string()))? {
        let mut entry = entry.map_err(|e| archive_error(e.to_string()))?;
        let is_binary = entry
            .path()
            .is_ok_and(|path| path.file_name().is_some_and(|name| name == binary));
        if is_binary {
            let mut bytes = Vec::new();
            entry
                .read_to_end(&mut bytes)
                .map_err(|e| archive_error(e.to_string()))?;
            return Ok(bytes);
        }
    }
    Err(archive_error(format!("{inner} holds no `{binary}`")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    struct Recorded {
        runs: Vec<WorkflowRun>,
        artifacts: std::collections::HashMap<u64, Vec<Artifact>>,
    }

    impl NightlySource for Recorded {
        fn runs(&self) -> impl Future<Output = Result<Vec<WorkflowRun>, GitHubError>> {
            std::future::ready(Ok(self.runs.clone()))
        }
        fn artifacts(&self, id: u64) -> impl Future<Output = Result<Vec<Artifact>, GitHubError>> {
            std::future::ready(Ok(self.artifacts.get(&id).cloned().unwrap_or_default()))
        }
    }

    fn at(day: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, day, 9, 8, 0)
            .single()
            .expect("a valid time")
    }

    fn run(id: u64, day: u32, conclusion: &str) -> WorkflowRun {
        WorkflowRun {
            id,
            head_sha: format!("{id:0>40}"),
            created_at: at(day),
            html_url: format!("https://github.com/spiceai/spiceai/actions/runs/{id}"),
            conclusion: Some(conclusion.to_string()),
        }
    }

    fn artifact(name: &str, day: u32, expired: bool) -> Artifact {
        Artifact {
            name: name.to_string(),
            size_in_bytes: 1,
            expired,
            expires_at: Some(at(day) + chrono::Duration::days(30)),
            digest: None,
            archive_download_url: String::new(),
        }
    }

    fn both(platform: &str, day: u32, expired: bool) -> Vec<Artifact> {
        vec![
            artifact(&format!("spice_{platform}"), day, expired),
            artifact(&format!("spiced_{platform}"), day, expired),
        ]
    }

    /// Nightly runs as GitHub lists them on 2026-11-11: 11-10 succeeded, 11-09 succeeded,
    /// 11-08 failed after the Linux builds, and 10-08's binaries have expired.
    fn november() -> Recorded {
        let mut failed = both("linux_aarch64", 8, false);
        failed.extend(both("linux_x86_64", 8, false));
        Recorded {
            runs: vec![
                run(10, 10, "success"),
                run(9, 9, "success"),
                run(8, 8, "failure"),
                run(1008, 8, "success"),
            ],
            artifacts: [
                (10, both("darwin_aarch64", 10, false)),
                (9, both("darwin_aarch64", 9, false)),
                (8, failed),
                (1008, both("darwin_aarch64", 8, true)),
            ]
            .into_iter()
            .collect(),
        }
    }

    fn date(day: u32) -> Option<NaiveDate> {
        NaiveDate::from_ymd_opt(2026, 11, day)
    }

    #[tokio::test]
    async fn without_a_date_the_newest_build_with_this_platforms_binaries_is_chosen() {
        let build = find(&november(), None, "darwin_aarch64", at(11))
            .await
            .expect("a build");
        assert_eq!(build.run.id, 10);
        assert_eq!(build.describe(), "2026-11-10 (trunk 0000000000)");
        assert_eq!(build.version_marker(), "nightly.20261110.g0000000");
    }

    #[tokio::test]
    async fn a_date_chooses_that_days_build() {
        let build = find(&november(), date(9), "darwin_aarch64", at(11))
            .await
            .expect("a build");
        assert_eq!(build.run.id, 9);
    }

    #[tokio::test]
    async fn a_date_with_no_build_lists_the_dates_there_are() {
        let error = find(&november(), date(12), "darwin_aarch64", at(11))
            .await
            .expect_err("no build ran on 11-12");
        assert_eq!(
            error.to_string(),
            "No Spice nightly build ran on 2026-11-12. Complete nightly builds: 2026-11-10, 2026-11-09, 2026-11-08. Install one with `spice install nightly <date>`."
        );
    }

    #[tokio::test]
    async fn a_build_that_failed_for_this_platform_says_so() {
        let mut source = november();
        source.runs.retain(|run| run.id != 1008);
        let error = find(&source, date(8), "darwin_aarch64", at(11))
            .await
            .expect_err("11-08's macOS build failed");
        assert_eq!(
            error.to_string(),
            "The Spice nightly build of 2026-11-08 has no darwin_aarch64 binaries, because that build did not finish: https://github.com/spiceai/spiceai/actions/runs/8. Complete nightly builds: 2026-11-10, 2026-11-09. Install one with `spice install nightly <date>`."
        );
        let build = find(&source, date(8), "linux_aarch64", at(11))
            .await
            .expect("11-08's Linux build finished");
        assert_eq!(build.run.id, 8);
    }

    #[tokio::test]
    async fn an_expired_build_says_so() {
        let mut source = november();
        source.runs.retain(|run| run.id != 8);
        let error = find(&source, date(8), "darwin_aarch64", at(11))
            .await
            .expect_err("1008's binaries expired");
        assert_eq!(
            error.to_string(),
            "The Spice nightly build of 2026-11-08 has expired: GitHub deletes a nightly build's binaries after the workflow's retention period. Complete nightly builds: 2026-11-10, 2026-11-09, 2026-11-08. Install one with `spice install nightly <date>`."
        );
    }

    #[test]
    fn only_the_platforms_the_workflow_builds_have_nightly_builds() {
        assert_eq!(platform("macos", "aarch64").ok(), Some("darwin_aarch64"));
        assert_eq!(platform("linux", "aarch64").ok(), Some("linux_aarch64"));
        assert_eq!(platform("linux", "x86_64").ok(), Some("linux_x86_64"));
        let error = platform("macos", "x86_64").expect_err("no Intel macOS nightly");
        assert_eq!(
            error.to_string(),
            "Spice nightly builds are published for Linux x86_64, Linux aarch64 and macOS aarch64, not for macos x86_64. Install a release with `spice install` instead."
        );
    }

    #[test]
    fn a_date_is_read_as_iso_or_as_the_version_writes_it() {
        assert_eq!(parse_date("2026-11-10").ok(), date(10));
        assert_eq!(parse_date("20261110").ok(), date(10));
        assert_eq!(
            parse_date("yesterday").expect_err("not a date").to_string(),
            "'yesterday' is not a date. Name a nightly build by the UTC date it ran, as YYYY-MM-DD, for example `spice install nightly 2026-11-10`."
        );
    }
}
