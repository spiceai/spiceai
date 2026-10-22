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

//! A repository at one commit, downloaded into the cache file by file: what a model,
//! embedding or reranker loads from.
//!
//! [`Snapshot::open`] resolves the revision to a commit and lists the repository's files there;
//! a component picks the files it needs from [`Snapshot::files`] and [`Snapshot::download`]s
//! them, getting back a directory in the cache's snapshot layout that an engine loads from
//! without touching the network.
//!
//! When the Hub cannot answer — it is unreachable, or fails with anything but a missing
//! repository or revision — the snapshot the cache last recorded for the revision is used, with
//! a warning. With `HF_HUB_OFFLINE` set the Hub is never asked.

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{StreamExt, TryStreamExt};
use tokio::runtime::Handle;

use crate::cache::{HubCache, relative_path};
use crate::client::{self, EntryKind, Hub, HubConfig, TreeEntry};
use crate::download::{self, Checksum, FetchError, Progress};
use crate::error::{Cause, DownloadContext, DownloadError};
use crate::location::RepoId;
use crate::settings::Settings;
use crate::xet::XetFiles;

/// Files of one repository downloaded at once, as `hf download` does.
const MAX_CONCURRENT_FILES: usize = 8;
/// How often a running download logs its progress.
const PROGRESS_INTERVAL: Duration = Duration::from_secs(30);
/// The revision read when none is named.
pub const DEFAULT_REVISION: &str = "main";

/// The component a download is for, which every log line and error names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Requester {
    /// What the component is, as a message names it: `model`, `embedding model`, `reranker`.
    pub kind: &'static str,
    /// The component's name in the Spicepod.
    pub name: String,
    /// The page an error links to.
    pub docs_url: &'static str,
    /// Whether Spice loads the component again after a download fails for a reason other
    /// than the Spicepod, as it does a model. An error's advice says that Spice retries, or
    /// that a restart does.
    pub retried: bool,
}

impl Requester {
    /// `Model 'qwen'`: the requester at the start of a sentence.
    #[must_use]
    pub fn capitalized(&self) -> String {
        let mut kind = self.kind.chars();
        let first = kind
            .next()
            .map(|c| c.to_uppercase().collect::<String>())
            .unwrap_or_default();
        format!("{first}{} '{}'", kind.as_str(), self.name)
    }
}

impl fmt::Display for Requester {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} '{}'", self.kind, self.name)
    }
}

/// A file of the repository at the snapshot's commit.
#[derive(Debug, Clone)]
pub struct RepoFile {
    /// The path from the repository root, `/`-separated.
    pub path: String,
    pub size: u64,
    /// The Hub's description, which a download needs; `None` for a file only the cache knows.
    entry: Option<TreeEntry>,
}

impl From<TreeEntry> for RepoFile {
    fn from(entry: TreeEntry) -> Self {
        Self {
            path: entry.path.clone(),
            size: entry.size,
            entry: Some(entry),
        }
    }
}

/// Where a snapshot's files came from, which decides what a file missing from it means.
#[derive(Debug, Clone)]
pub(crate) enum Source {
    /// The Hub's listing of the commit.
    Hub,
    /// The cache, read alone because `HF_HUB_OFFLINE` is set.
    OfflineCache,
    /// The cache, read alone because the Hub failed (`reason`).
    CacheInOutage { reason: String },
}

/// A repository at one commit.
pub struct Snapshot {
    requester: Requester,
    repo: RepoId,
    revision: String,
    commit: String,
    files: BTreeMap<String, RepoFile>,
    source: Source,
    hub: Arc<Hub>,
    cache: HubCache,
    settings: Settings,
}

impl fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Snapshot")
            .field("requester", &self.requester)
            .field("repo", &self.repo)
            .field("revision", &self.revision)
            .field("commit", &self.commit)
            .finish_non_exhaustive()
    }
}

impl Snapshot {
    /// Resolves `revision` (`main` when `None`) of `repo` to a commit and lists its files, in
    /// one request to the Hub.
    ///
    /// # Errors
    ///
    /// Returns an error if the repository or revision does not exist or cannot be read, and
    /// the cache holds no snapshot of the revision to fall back to.
    pub async fn open(
        settings: Settings,
        requester: Requester,
        repo: RepoId,
        revision: Option<&str>,
    ) -> Result<Self, DownloadError> {
        Self::open_listing(settings, requester, repo, revision, None).await
    }

    /// Resolves `revision` as [`Snapshot::open`] does, but keeps only `paths`: for a component
    /// that reads a few named files of a repository. A path already in the cache at the commit
    /// is used as it is, as `huggingface_hub` does, so a warm load costs the one request that
    /// resolves a branch or tag, and none for a commit. A path the repository does not have is
    /// left out, and downloading it names it.
    ///
    /// # Errors
    ///
    /// See [`Snapshot::open`].
    pub async fn open_paths(
        settings: Settings,
        requester: Requester,
        repo: RepoId,
        revision: Option<&str>,
        paths: &[&str],
    ) -> Result<Self, DownloadError> {
        Self::open_listing(settings, requester, repo, revision, Some(paths)).await
    }

    async fn open_listing(
        settings: Settings,
        requester: Requester,
        repo: RepoId,
        revision: Option<&str>,
        paths: Option<&[&str]>,
    ) -> Result<Self, DownloadError> {
        let revision = revision
            .filter(|revision| !revision.is_empty())
            .unwrap_or(DEFAULT_REVISION)
            .to_string();
        let context = DownloadContext::new(&requester, &repo, &revision, &settings);
        let hub = Hub::new(
            HubConfig {
                endpoint: settings.endpoint.clone(),
                token: settings.token.clone(),
                token_scope: settings.token_scope,
            },
            Handle::current(),
        )
        .map_err(|source| context.error(None, Cause::Hub(source)))?;
        let hub = Arc::new(hub);
        let cache = HubCache::new(settings.cache_dir.clone());

        let (commit, files, source) = if settings.offline {
            let (commit, files) = from_cache(&cache, &repo, &revision)
                .await
                .ok_or_else(|| context.error(None, Cause::OfflineNoSnapshot))?;
            (commit, files, Source::OfflineCache)
        } else {
            let listed = match paths {
                None => from_hub(&hub, &cache, &repo, &revision).await,
                Some(paths) => paths_from_hub(&hub, &cache, &repo, &revision, paths).await,
            };
            match listed {
                Ok((commit, files)) => (commit, files, Source::Hub),
                Err(error) if falls_back(&error) => {
                    let Some((commit, files)) = from_cache(&cache, &repo, &revision).await else {
                        return Err(context.error(None, Cause::Hub(error)));
                    };
                    let reason = crate::error::unreachable_reason(&error);
                    tracing::warn!(
                        "{} could not reach the Hugging Face Hub ({reason}), so it loads revision '{revision}' of '{repo}' from the cache at commit {}. Changes to '{revision}' are not seen until the Hub is reachable.",
                        requester.capitalized(),
                        short(&commit)
                    );
                    (commit, files, Source::CacheInOutage { reason })
                }
                Err(error) => return Err(context.error(None, Cause::Hub(error))),
            }
        };
        Ok(Self {
            requester,
            repo,
            revision,
            commit,
            files,
            source,
            hub,
            cache,
            settings,
        })
    }

    #[must_use]
    pub fn repo(&self) -> &RepoId {
        &self.repo
    }

    #[must_use]
    pub fn commit(&self) -> &str {
        &self.commit
    }

    /// The directory the snapshot's files are downloaded into.
    #[must_use]
    pub fn dir(&self) -> PathBuf {
        self.cache.snapshot_dir(&self.repo, &self.commit)
    }

    /// The repository's files at the commit. When the snapshot comes from the cache alone,
    /// only the files already cached.
    pub fn files(&self) -> impl Iterator<Item = &RepoFile> {
        self.files.values()
    }

    #[must_use]
    pub fn file(&self, path: &str) -> Option<&RepoFile> {
        self.files.get(path)
    }

    /// Downloads `paths`, as [`Snapshot::download`] does, without logging: for the few small
    /// files a component reads to decide what else to download.
    ///
    /// # Errors
    ///
    /// See [`Snapshot::download`].
    pub async fn fetch_quietly(&self, paths: &[&str]) -> Result<Vec<PathBuf>, DownloadError> {
        self.fetch(paths, false).await
    }

    /// Downloads every file in `paths` that is not in the cache yet, at most
    /// [`MAX_CONCURRENT_FILES`] at a time, and returns their paths in the snapshot directory,
    /// in the order given.
    ///
    /// Logs the files and bytes to download when it starts, the progress every
    /// [`PROGRESS_INTERVAL`], and the time taken when it ends — or that everything was cached.
    ///
    /// # Errors
    ///
    /// Returns an error if a path is not in the repository, or a download fails.
    pub async fn download(&self, paths: &[&str]) -> Result<Vec<PathBuf>, DownloadError> {
        self.fetch(paths, true).await
    }

    /// `at commit 0a1b2c3 (revision 'main')`, or `at commit 0a1b2c3` when the revision is the
    /// commit itself.
    fn at_commit(&self) -> String {
        if self.revision == self.commit {
            format!("at commit {}", short(&self.commit))
        } else {
            format!(
                "at commit {} (revision '{}')",
                short(&self.commit),
                self.revision
            )
        }
    }

    /// Where the snapshot's files came from.
    pub(crate) fn source(&self) -> &Source {
        &self.source
    }

    /// The component the snapshot is for.
    #[must_use]
    pub fn requester(&self) -> &Requester {
        &self.requester
    }

    /// An error of this snapshot's download, about `file` when one is named.
    pub(crate) fn error(&self, file: Option<&str>, cause: Cause) -> DownloadError {
        DownloadContext::new(&self.requester, &self.repo, &self.revision, &self.settings)
            .error(file, cause)
    }

    async fn fetch(&self, paths: &[&str], log: bool) -> Result<Vec<PathBuf>, DownloadError> {
        let context =
            DownloadContext::new(&self.requester, &self.repo, &self.revision, &self.settings);
        let mut missing = Vec::new();
        let mut located = Vec::with_capacity(paths.len());
        for &path in paths {
            // The Hub names the paths; one that would leave the snapshot is never written.
            let Some(relative) = relative_path(path) else {
                return Err(context.error(Some(path), Cause::InvalidPath));
            };
            let missing_file = || {
                context.error(
                    None,
                    Cause::Missing {
                        what: format!("'{path}'"),
                        source: self.source.clone(),
                    },
                )
            };
            let file = self.files.get(path).ok_or_else(missing_file)?;
            if let Some(cached) = self
                .cache
                .cached_file(&self.repo, &self.commit, path, Some(file.size))
                .await
            {
                located.push(cached);
            } else {
                // A file only the cache knows of is one whose blob went missing.
                let entry = file.entry.clone().ok_or_else(missing_file)?;
                located.push(self.dir().join(relative));
                missing.push(entry);
            }
        }

        if missing.is_empty() {
            if log {
                tracing::info!(
                    "{} uses Hugging Face repo '{}' {}, already in the cache.",
                    self.requester.capitalized(),
                    self.repo,
                    self.at_commit()
                );
            }
            return Ok(located);
        }

        let total: u64 = missing.iter().map(|entry| entry.size).sum();
        let started = Instant::now();
        let progress = Arc::new(Progress::default());
        let _reporter = log.then(|| {
            tracing::info!(
                "Downloading {} for {} from Hugging Face repo '{}' {}.",
                files_and_size(missing.len(), total),
                self.requester,
                self.repo,
                self.at_commit()
            );
            AbortOnDrop(report_progress(
                self.requester.clone(),
                Arc::clone(&progress),
                total,
            ))
        });

        let xet = match self.settings.xet.as_deref() {
            Some(cache_dir) => {
                XetFiles::of(&self.hub, cache_dir, &self.repo, &self.commit, &missing).await
            }
            None => None,
        };
        let result = futures::stream::iter(missing)
            .map(|entry| self.fetch_file(entry, Arc::clone(&progress), xet.as_ref()))
            .buffer_unordered(MAX_CONCURRENT_FILES)
            .try_collect::<Vec<_>>()
            .await;
        if let Err(failure) = result {
            let (path, error) = *failure;
            return Err(context.error(Some(&path), Cause::Fetch(error)));
        }
        if log {
            tracing::info!(
                "Downloaded {} from Hugging Face repo '{}' in {}.",
                self.requester,
                self.repo,
                human_duration(started.elapsed())
            );
        }
        Ok(located)
    }
}

impl Snapshot {
    /// Downloads `entry`'s blob and links it into the snapshot directory. A failure names the
    /// file it was for.
    async fn fetch_file(
        &self,
        entry: TreeEntry,
        progress: Arc<Progress>,
        xet: Option<&XetFiles>,
    ) -> Result<(), Box<(String, FetchError)>> {
        download::fetch_blob(
            &self.hub,
            &self.cache,
            &self.repo,
            &self.commit,
            &entry,
            &progress,
            xet,
        )
        .await
        .map_err(|error| Box::new((entry.path.clone(), error)))?;
        self.cache
            .link(
                &self.repo,
                &self.commit,
                &entry.path,
                Checksum::of(&entry).etag(),
            )
            .await
            .map(|_| ())
            .map_err(|source| {
                Box::new((
                    entry.path.clone(),
                    FetchError::Write {
                        path: self.dir().join(&entry.path),
                        source,
                    },
                ))
            })
    }
}

/// Resolves `revision` on the Hub and describes every file at the commit, with the one request
/// that resolves it when the endpoint describes the files, else with a listing.
async fn from_hub(
    hub: &Arc<Hub>,
    cache: &HubCache,
    repo: &RepoId,
    revision: &str,
) -> Result<(String, BTreeMap<String, RepoFile>), client::Error> {
    let (commit, described) = hub.describe(repo, revision).await?;
    let commit = commit.sha;
    record_ref(cache, repo, revision, &commit).await;
    let entries = match described {
        Some(entries) => entries,
        None => hub.list(repo, &commit, "", true).await?,
    };
    let files = entries
        .iter()
        .filter(|entry| entry.kind == EntryKind::File)
        .map(|entry| (entry.path.clone(), RepoFile::from(entry.clone())))
        .collect();
    Ok((commit, files))
}

/// Resolves `revision` on the Hub and describes `paths` at the commit: a path the cache holds
/// at the commit from the cache, any other from the Hub. A commit names itself, so when the
/// cache holds every path at it, no request is made. A branch or tag whose last commit the
/// cache holds every path at is confirmed with one `HEAD`, the Hub's quickest answer; any other
/// is resolved and described in one request.
async fn paths_from_hub(
    hub: &Arc<Hub>,
    cache: &HubCache,
    repo: &RepoId,
    revision: &str,
    paths: &[&str],
) -> Result<(String, BTreeMap<String, RepoFile>), client::Error> {
    let (commit, described) = if crate::client::is_commit_sha(revision) {
        (revision.to_string(), None)
    } else if let Some(commit) = unchanged_commit(hub, cache, repo, revision, paths).await? {
        (commit, None)
    } else {
        let (commit, described) = hub.describe(repo, revision).await?;
        (commit.sha, described)
    };
    record_ref(cache, repo, revision, &commit).await;
    let mut files = BTreeMap::new();
    for &path in paths {
        let file = if let Some(metadata) = cached_metadata(cache, repo, &commit, path).await {
            Some(RepoFile {
                path: path.to_string(),
                size: metadata.len(),
                entry: None,
            })
        } else {
            let entry = match &described {
                Some(entries) => entries.iter().find(|entry| entry.path == path).cloned(),
                None => hub.entry(repo, &commit, path).await?,
            };
            entry
                .filter(|entry| entry.kind == EntryKind::File)
                .map(RepoFile::from)
        };
        if let Some(file) = file {
            files.insert(path.to_string(), file);
        }
    }
    Ok((commit, files))
}

/// The commit `revision` last resolved to, when the cache holds every path at it and the Hub
/// still names it for the revision, which a `HEAD` of the first path says without its content.
async fn unchanged_commit(
    hub: &Hub,
    cache: &HubCache,
    repo: &RepoId,
    revision: &str,
    paths: &[&str],
) -> Result<Option<String>, client::Error> {
    let Some(first) = paths.first() else {
        return Ok(None);
    };
    let Ok(Some(commit)) = cache.read_ref(repo, revision).await else {
        return Ok(None);
    };
    for path in paths {
        if cached_metadata(cache, repo, &commit, path).await.is_none() {
            return Ok(None);
        }
    }
    match hub.commit_of_file(repo, revision, first).await {
        Ok(now) => Ok(now.filter(|now| *now == commit)),
        // The path is gone from the revision; the revision's description says so.
        Err(error) if error.is_entry_not_found() => Ok(None),
        Err(error) => Err(error),
    }
}

/// The metadata of `path` in the cache at `commit`, when the cache holds it.
async fn cached_metadata(
    cache: &HubCache,
    repo: &RepoId,
    commit: &str,
    path: &str,
) -> Option<std::fs::Metadata> {
    let relative = relative_path(path)?;
    tokio::fs::metadata(cache.snapshot_dir(repo, commit).join(relative))
        .await
        .ok()
        .filter(std::fs::Metadata::is_file)
}

/// Records in the cache that `revision` resolved to `commit`, so an offline load or one in an
/// outage finds it. A failure costs only that, so it is not an error.
async fn record_ref(cache: &HubCache, repo: &RepoId, revision: &str, commit: &str) {
    if let Err(error) = cache.write_ref(repo, revision, commit).await {
        tracing::debug!(
            "Failed to record that revision '{revision}' of Hugging Face {} '{repo}' is commit {commit} in the cache: {error}",
            repo.kind()
        );
    }
}

/// The commit the cache last recorded for `revision`, and the files of it the cache holds.
async fn from_cache(
    cache: &HubCache,
    repo: &RepoId,
    revision: &str,
) -> Option<(String, BTreeMap<String, RepoFile>)> {
    let commit = cache.read_ref(repo, revision).await.ok()??;
    let cached = cache.cached_files(repo, &commit).await.ok()?;
    if cached.is_empty() {
        return None;
    }
    let files = cached
        .into_iter()
        .map(|file| {
            (
                file.path.clone(),
                RepoFile {
                    path: file.path,
                    size: file.size,
                    entry: None,
                },
            )
        })
        .collect();
    Some((commit, files))
}

/// Whether a load that failed with `error` may use the cache instead: every failure but a
/// repository, revision or file the Hub says does not exist.
fn falls_back(error: &client::Error) -> bool {
    !matches!(
        error,
        client::Error::RepoNotFound { .. }
            | client::Error::RevisionNotFound { .. }
            | client::Error::EntryNotFound { .. }
            | client::Error::InvalidEndpoint { .. }
            | client::Error::InvalidToken { .. }
    )
}

/// Logs the progress of a download every [`PROGRESS_INTERVAL`] until aborted.
fn report_progress(
    requester: Requester,
    progress: Arc<Progress>,
    total: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(PROGRESS_INTERVAL);
        interval.tick().await;
        let mut previous = progress.received();
        loop {
            interval.tick().await;
            let received = progress.received().min(total);
            let rate = received.saturating_sub(previous) / PROGRESS_INTERVAL.as_secs();
            previous = received;
            tracing::info!(
                "Downloading {requester}: {}.",
                progress_line(received, total, rate)
            );
        }
    })
}

/// The first seven digits of a commit, as the Hub shows it.
fn short(commit: &str) -> &str {
    commit.get(..7).unwrap_or(commit)
}

fn files_and_size(count: usize, bytes: u64) -> String {
    let files = if count == 1 { "file" } else { "files" };
    format!("{count} {files} ({})", human_bytes(bytes))
}

/// Aborts a background task when dropped: the progress reporting of a load that is cancelled,
/// or a Xet read whose bytes are no longer wanted.
pub(crate) struct AbortOnDrop(pub(crate) tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

const UNITS: [&str; 5] = ["B", "kB", "MB", "GB", "TB"];

/// The decimal unit `bytes` is shown in, as the Hub shows file sizes, and its divisor.
fn unit_of(bytes: u64) -> (usize, f64) {
    let mut unit = 0;
    let mut divisor = 1.0;
    #[expect(clippy::cast_precision_loss)]
    let value = bytes as f64;
    while value / divisor >= 1000.0 && unit < UNITS.len() - 1 {
        divisor *= 1000.0;
        unit += 1;
    }
    (unit, divisor)
}

#[expect(clippy::cast_precision_loss)]
fn in_unit(bytes: u64, divisor: f64) -> f64 {
    bytes as f64 / divisor
}

/// `16.4 GB`, `523 B`.
#[must_use]
pub fn human_bytes(bytes: u64) -> String {
    let (unit, divisor) = unit_of(bytes);
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{:.1} {}", in_unit(bytes, divisor), UNITS[unit])
    }
}

/// `6.1 of 16.4 GB (37%) at 52 MB/s, about 3 min left`: how far a download has got.
#[must_use]
pub fn progress_line(received: u64, total: u64, rate: u64) -> String {
    let (unit, divisor) = unit_of(total);
    let amounts = if unit == 0 {
        format!("{received} of {total} B")
    } else {
        format!(
            "{:.1} of {:.1} {}",
            in_unit(received, divisor),
            in_unit(total, divisor),
            UNITS[unit]
        )
    };
    let percent = (u128::from(received) * 100)
        .checked_div(u128::from(total))
        .unwrap_or(100);
    let (rate_unit, rate_divisor) = unit_of(rate);
    let rate_value = in_unit(rate, rate_divisor);
    let speed = if rate_value < 10.0 && rate_unit > 0 {
        format!("{rate_value:.1} {}/s", UNITS[rate_unit])
    } else {
        format!("{rate_value:.0} {}/s", UNITS[rate_unit])
    };
    let left = total
        .saturating_sub(received)
        .checked_div(rate)
        .map_or_else(String::new, |seconds| {
            format!(", about {} left", rough_duration(seconds))
        });
    format!("{amounts} ({percent}%) at {speed}{left}")
}

/// `3 min`, `40 s`, `1 h 4 min`: a remaining time, to the nearest minute once it is that long.
fn rough_duration(seconds: u64) -> String {
    match seconds {
        0..60 => format!("{seconds} s"),
        60..3600 => format!("{} min", (seconds + 30) / 60),
        _ => {
            let minutes = (seconds + 30) / 60;
            format!("{} h {} min", minutes / 60, minutes % 60)
        }
    }
}

/// `5 min 12 s`, `3.2 s`, `1 h 4 min`: how long a download took.
#[must_use]
pub fn human_duration(duration: Duration) -> String {
    let seconds = duration.as_secs();
    match seconds {
        0..60 => format!("{:.1} s", duration.as_secs_f64()),
        60..3600 => format!("{} min {} s", seconds / 60, seconds % 60),
        _ => format!("{} h {} min", seconds / 3600, (seconds % 3600) / 60),
    }
}
