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

//! A client for the parts of the Hugging Face Hub a dataset read uses.
//!
//! - `GET /api/datasets/{repo}/revision/{revision}` resolves a branch or tag to a commit;
//! - `GET /api/datasets/{repo}/tree/{commit}/{path}` lists files, 1,000 per page;
//! - `POST /api/datasets/{repo}/paths-info/{commit}` describes one path;
//! - `GET /datasets/{repo}/resolve/{commit}/{path}` reads a file. For a large file it redirects
//!   to a signed CDN URL that serves byte ranges; the redirect target is cached, so a file costs
//!   one `resolve` request however many ranges a scan reads from it.
//!
//! Every read names a commit, never a branch, so everything one scan reads comes from the same
//! snapshot of the dataset, and anything keyed by a commit can be cached without invalidation.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use futures::stream::BoxStream;
use moka::future::Cache;
use object_store::GetRange;
use reqwest::header::{
    AUTHORIZATION, CONTENT_RANGE, HeaderMap, HeaderValue, LINK, LOCATION, RANGE, RETRY_AFTER,
};
use reqwest::{Method, StatusCode};
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use snafu::prelude::*;
use tokio::runtime::Handle;
use url::Url;

use crate::location::RepoId;

/// The public Hugging Face Hub.
pub const DEFAULT_ENDPOINT: &str = "https://huggingface.co";
const DOCS_URL: &str =
    "https://github.com/spiceai/spiceai/blob/trunk/docs/features/huggingface-connector.md";

/// Attempts for one request, the first included.
const MAX_ATTEMPTS: u32 = 5;
const INITIAL_BACKOFF: Duration = Duration::from_millis(250);
const MAX_BACKOFF: Duration = Duration::from_secs(8);
/// The longest a rate-limited request waits for the Hub's window to reset before failing.
const MAX_RATE_LIMIT_WAIT: Duration = Duration::from_secs(30);
/// Redirects followed for one file read (resolve → resolve-cache or CDN).
const MAX_REDIRECTS: usize = 5;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Longest wait for the next bytes of a response; a slow but progressing download never trips it.
const READ_TIMEOUT: Duration = Duration::from_secs(60);

/// How long a branch or tag resolves to the same commit without asking the Hub again. Bounds
/// how stale a scan can be after the dataset changes, and coalesces the lookups of a burst of
/// scans into one.
const REVISION_TTL: Duration = Duration::from_secs(10);
/// How long a resolved CDN URL is reused. The Hub signs them for an hour; an expired one is
/// detected by its error status and resolved again.
const FILE_URL_TTL: Duration = Duration::from_mins(30);
const MAX_CACHED_COMMITS: u64 = 10_000;
const MAX_CACHED_FILE_URLS: u64 = 100_000;
/// Entries across all cached listings. Listings are immutable at a commit, so this bounds
/// memory, not freshness.
const MAX_CACHED_LISTING_ENTRIES: u64 = 500_000;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display(
        "Hugging Face dataset '{repo}' was not found{private_hint}. Check the owner and dataset name in `from`. See: {DOCS_URL}"
    ))]
    RepoNotFound { repo: RepoId, private_hint: String },

    #[snafu(display(
        "Revision '{revision}' does not exist in Hugging Face dataset '{repo}'. Use an existing branch, tag or commit after '@' in `from`, or remove '@{revision}' to read 'main'."
    ))]
    RevisionNotFound { repo: RepoId, revision: String },

    #[snafu(display("'{path}' does not exist in Hugging Face dataset '{repo}'."))]
    EntryNotFound { repo: RepoId, path: String },

    #[snafu(display(
        "Hugging Face dataset '{repo}' is gated{token_hint}: accept its access conditions at {dataset_url} with the account `hf_token` belongs to. See: {DOCS_URL}"
    ))]
    Gated {
        repo: RepoId,
        dataset_url: String,
        token_hint: String,
    },

    #[snafu(display(
        "The Hugging Face Hub rejected `hf_token` for dataset '{repo}' (HTTP {status}). Check that the token is valid and grants read access to the dataset. See: {DOCS_URL}"
    ))]
    TokenRejected { repo: RepoId, status: u16 },

    #[snafu(display(
        "The Hugging Face Hub kept rate-limiting requests for dataset '{repo}'{reset_hint}. Set `hf_token` (authenticated requests have higher limits) or accelerate the dataset so it is read once per refresh. See: {DOCS_URL}"
    ))]
    RateLimited { repo: RepoId, reset_hint: String },

    #[snafu(display(
        "The Hugging Face Hub returned HTTP {status} for dataset '{repo}' after {attempts} attempt(s){message}"
    ))]
    Status {
        repo: RepoId,
        status: u16,
        attempts: u32,
        message: String,
    },

    #[snafu(display(
        "Failed to reach the Hugging Face Hub at {endpoint} for dataset '{repo}' after {attempts} attempt(s): {source}"
    ))]
    Request {
        repo: RepoId,
        endpoint: String,
        attempts: u32,
        source: reqwest::Error,
    },

    #[snafu(display(
        "The Hugging Face Hub returned an unexpected response for dataset '{repo}': {message}"
    ))]
    UnexpectedResponse { repo: RepoId, message: String },

    #[snafu(display(
        "A request for Hugging Face dataset '{repo}' stopped before it completed: {source}"
    ))]
    TaskStopped {
        repo: RepoId,
        source: tokio::task::JoinError,
    },

    #[snafu(display("The Hugging Face Hub endpoint '{endpoint}' is not an http(s) URL."))]
    InvalidEndpoint { endpoint: String },

    /// An error several concurrent callers received from one shared request.
    #[snafu(display("{message}"))]
    Shared { message: String },

    #[snafu(display("Failed to build the HTTP client for the Hugging Face Hub: {source}"))]
    HttpClient { source: reqwest::Error },

    #[snafu(display(
        "`hf_token` is not a valid Hugging Face access token: it contains characters an HTTP header cannot carry. See: {DOCS_URL}"
    ))]
    InvalidToken,
}

impl Error {
    /// Whether the error means the path does not exist, as opposed to the dataset or revision.
    #[must_use]
    pub fn is_entry_not_found(&self) -> bool {
        matches!(self, Error::EntryNotFound { .. })
    }

    #[must_use]
    pub fn is_unauthorized(&self) -> bool {
        matches!(self, Error::Gated { .. } | Error::TokenRejected { .. })
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A commit of a dataset repository.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit {
    pub sha: String,
    pub date: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EntryKind {
    File,
    Directory,
}

#[derive(Debug, Clone, Deserialize)]
struct LfsInfo {
    oid: String,
}

/// A file or folder in a dataset repository, as the tree and paths-info endpoints describe it.
#[derive(Debug, Clone, Deserialize)]
pub struct TreeEntry {
    #[serde(rename = "type")]
    pub kind: EntryKind,
    /// The path from the repository root.
    pub path: String,
    #[serde(default)]
    pub size: u64,
    /// The git object id.
    #[serde(default)]
    oid: String,
    #[serde(default)]
    lfs: Option<LfsInfo>,
}

impl TreeEntry {
    /// An identifier of the file's content: the SHA-256 of an LFS file, else its git object id.
    #[must_use]
    pub fn content_id(&self) -> &str {
        self.lfs
            .as_ref()
            .map_or(self.oid.as_str(), |lfs| lfs.oid.as_str())
    }
}

#[derive(Debug, Deserialize)]
struct RevisionInfo {
    sha: String,
    #[serde(rename = "lastModified")]
    last_modified: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ListingKey {
    repo: RepoId,
    commit: String,
    path: String,
    recursive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FileKey {
    repo: RepoId,
    commit: String,
    path: String,
}

/// The response to a file read: the bytes of `range`, as a stream.
pub struct FileRead {
    /// The byte range the stream yields, resolved against the file size.
    pub range: Range<u64>,
    /// The content id the Hub reported for the file, when it reported one.
    pub e_tag: Option<String>,
    /// The size of the whole file.
    pub size: u64,
    pub stream: BoxStream<'static, Result<Bytes>>,
}

/// The configuration that identifies a Hub client: two datasets with the same identity share
/// one client and its caches.
#[derive(Clone)]
pub struct HubConfig {
    pub endpoint: Url,
    pub token: Option<SecretString>,
}

impl fmt::Debug for HubConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HubConfig")
            .field("endpoint", &self.endpoint.as_str())
            .field("token", &self.token.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}

/// A Hugging Face Hub client for one endpoint and token.
pub struct Hub {
    config: HubConfig,
    /// `Authorization: Bearer {token}`, marked sensitive so it is never logged.
    authorization: Option<HeaderValue>,
    http: reqwest::Client,
    io_runtime: Handle,
    revisions: Cache<(RepoId, String), Commit>,
    commit_dates: Cache<(RepoId, String), DateTime<Utc>>,
    listings: Cache<ListingKey, Arc<[TreeEntry]>>,
    /// The CDN URL of a file and the content id the Hub reported for it.
    file_urls: Cache<FileKey, (Url, Option<String>)>,
    /// The description of a path at a commit, which cannot change.
    entries: Cache<FileKey, Option<TreeEntry>>,
}

impl fmt::Debug for Hub {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Hub")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl Hub {
    /// Builds a client that sends every request on `io_runtime`.
    ///
    /// # Errors
    ///
    /// Returns an error if the endpoint is not an http(s) URL, the token cannot be sent in an
    /// HTTP header, or the HTTP client cannot be built.
    pub fn new(config: HubConfig, io_runtime: Handle) -> Result<Self> {
        ensure!(
            matches!(config.endpoint.scheme(), "http" | "https") && config.endpoint.has_host(),
            InvalidEndpointSnafu {
                endpoint: config.endpoint.to_string(),
            }
        );
        let authorization = config
            .token
            .as_ref()
            .map(|token| {
                let mut value = HeaderValue::from_str(&format!("Bearer {}", token.expose_secret()))
                    .map_err(|_| Error::InvalidToken)?;
                value.set_sensitive(true);
                Ok::<_, Error>(value)
            })
            .transpose()?;
        // Redirects are followed by hand: the CDN target of a file is cached, and the token is
        // sent only to the Hub's own origin. Transparent decompression is off: a byte range
        // must address the file as stored, and its length must be the file's.
        let http = reqwest::Client::builder()
            .user_agent(util::spiceai_user_agent())
            .redirect(reqwest::redirect::Policy::none())
            .no_gzip()
            .no_brotli()
            .no_zstd()
            .no_deflate()
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(READ_TIMEOUT)
            .build()
            .context(HttpClientSnafu)?;
        let listing_weigher = |_: &ListingKey, entries: &Arc<[TreeEntry]>| {
            u32::try_from(entries.len()).unwrap_or(u32::MAX).max(1)
        };
        Ok(Self {
            config,
            authorization,
            http,
            io_runtime,
            revisions: Cache::builder()
                .max_capacity(MAX_CACHED_COMMITS)
                .time_to_live(REVISION_TTL)
                .build(),
            commit_dates: Cache::builder().max_capacity(MAX_CACHED_COMMITS).build(),
            listings: Cache::builder()
                .max_capacity(MAX_CACHED_LISTING_ENTRIES)
                .weigher(listing_weigher)
                .build(),
            file_urls: Cache::builder()
                .max_capacity(MAX_CACHED_FILE_URLS)
                .time_to_live(FILE_URL_TTL)
                .build(),
            entries: Cache::builder().max_capacity(MAX_CACHED_FILE_URLS).build(),
        })
    }

    #[must_use]
    pub fn config(&self) -> &HubConfig {
        &self.config
    }

    /// Forgets every branch and tag resolution, as if [`REVISION_TTL`] had passed.
    #[cfg(test)]
    pub(crate) fn forget_revisions(&self) {
        self.revisions.invalidate_all();
    }

    /// The dataset's page on the Hub.
    #[must_use]
    pub fn dataset_url(&self, repo: &RepoId) -> String {
        format!(
            "{}/datasets/{repo}",
            self.config.endpoint.as_str().trim_end_matches('/')
        )
    }

    /// Resolves `revision` (a branch, tag or commit) to a commit.
    ///
    /// A branch or tag is resolved again once [`REVISION_TTL`] has passed, so a dataset change
    /// is seen within that time; concurrent lookups of the same revision share one request.
    ///
    /// # Errors
    ///
    /// Returns an error if the dataset or revision does not exist, access is denied, or the Hub
    /// cannot be reached.
    pub async fn commit(&self, repo: &RepoId, revision: &str) -> Result<Commit> {
        // A commit names an immutable snapshot: only its date is looked up, once.
        if is_commit_sha(revision) {
            let date = self.commit_date(repo, revision).await?;
            return Ok(Commit {
                sha: revision.to_string(),
                date,
            });
        }
        let key = (repo.clone(), revision.to_string());
        let commit = self
            .revisions
            .try_get_with(key, self.fetch_commit(repo, revision))
            .await
            .map_err(|error| Arc::try_unwrap(error).unwrap_or_else(|shared| shared.duplicate()))?;
        self.commit_dates
            .insert((repo.clone(), commit.sha.clone()), commit.date)
            .await;
        Ok(commit)
    }

    /// The date of `commit`, a full commit SHA.
    ///
    /// # Errors
    ///
    /// Returns an error if the commit cannot be looked up.
    pub async fn commit_date(&self, repo: &RepoId, commit: &str) -> Result<DateTime<Utc>> {
        // Every object read of a scan asks for the date; concurrent first asks share one
        // request.
        self.commit_dates
            .try_get_with((repo.clone(), commit.to_string()), async {
                self.fetch_commit(repo, commit)
                    .await
                    .map(|resolved| resolved.date)
            })
            .await
            .map_err(|error| Arc::try_unwrap(error).unwrap_or_else(|shared| shared.duplicate()))
    }

    async fn fetch_commit(&self, repo: &RepoId, revision: &str) -> Result<Commit> {
        let mut url = self.api_url(repo, &["revision"], revision, "");
        url.query_pairs_mut()
            .append_pair("expand[]", "sha")
            .append_pair("expand[]", "lastModified");
        let body = self
            .request_bytes(repo, Some(revision), Method::GET, url, None)
            .await?;
        let info: RevisionInfo =
            serde_json::from_slice(&body).map_err(|e| Error::UnexpectedResponse {
                repo: repo.clone(),
                message: format!("the revision '{revision}' could not be read ({e})"),
            })?;
        ensure!(
            is_commit_sha(&info.sha),
            UnexpectedResponseSnafu {
                repo: repo.clone(),
                message: format!("revision '{revision}' resolved to an invalid commit id"),
            }
        );
        Ok(Commit {
            sha: info.sha,
            date: info.last_modified,
        })
    }

    /// Lists the entries under `path` at `commit`: every file and folder below it when
    /// `recursive`, else its direct children. A path that does not exist, or names a file, has
    /// no entries.
    ///
    /// # Errors
    ///
    /// Returns an error if the dataset or commit does not exist, access is denied, or the Hub
    /// cannot be reached.
    pub async fn list(
        &self,
        repo: &RepoId,
        commit: &str,
        path: &str,
        recursive: bool,
    ) -> Result<Arc<[TreeEntry]>> {
        let key = ListingKey {
            repo: repo.clone(),
            commit: commit.to_string(),
            path: path.to_string(),
            recursive,
        };
        self.listings
            .try_get_with(key, self.fetch_listing(repo, commit, path, recursive))
            .await
            .map_err(|error| Arc::try_unwrap(error).unwrap_or_else(|shared| shared.duplicate()))
    }

    async fn fetch_listing(
        &self,
        repo: &RepoId,
        commit: &str,
        path: &str,
        recursive: bool,
    ) -> Result<Arc<[TreeEntry]>> {
        let mut url = self.api_url(repo, &["tree"], commit, path);
        url.query_pairs_mut()
            .append_pair("recursive", if recursive { "true" } else { "false" })
            .append_pair("expand", "false");
        let mut entries = Vec::new();
        let mut next = Some(url);
        while let Some(page_url) = next.take() {
            let first_page = entries.is_empty();
            let (headers, body) = match self
                .request(repo, Some(commit), Method::GET, page_url, None)
                .await
            {
                Ok(response) => {
                    let headers = response.headers().clone();
                    let body = read_body(repo, &self.config.endpoint, response).await?;
                    (headers, body)
                }
                // The tree of a path that does not exist (or names a file) is empty. A later
                // page that is missing is an error: ending the listing there would drop files.
                Err(error) if first_page && error.is_entry_not_found() => break,
                Err(error) => return Err(error),
            };
            let page: Vec<TreeEntry> =
                serde_json::from_slice(&body).map_err(|e| Error::UnexpectedResponse {
                    repo: repo.clone(),
                    message: format!("a page of the file listing could not be read ({e})"),
                })?;
            entries.extend(page);
            next = next_page(&headers, &self.config.endpoint).map_err(|message| {
                Error::UnexpectedResponse {
                    repo: repo.clone(),
                    message,
                }
            })?;
        }
        Ok(entries.into())
    }

    /// Describes the file or folder at `path` at `commit`, `None` when it does not exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the dataset or commit does not exist, access is denied, or the Hub
    /// cannot be reached.
    pub async fn entry(
        &self,
        repo: &RepoId,
        commit: &str,
        path: &str,
    ) -> Result<Option<TreeEntry>> {
        let key = FileKey {
            repo: repo.clone(),
            commit: commit.to_string(),
            path: path.to_string(),
        };
        self.entries
            .try_get_with(key, self.fetch_entry(repo, commit, path))
            .await
            .map_err(|error| Arc::try_unwrap(error).unwrap_or_else(|shared| shared.duplicate()))
    }

    async fn fetch_entry(
        &self,
        repo: &RepoId,
        commit: &str,
        path: &str,
    ) -> Result<Option<TreeEntry>> {
        let url = self.api_url(repo, &["paths-info"], commit, "");
        let form = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("paths", path)
            .append_pair("expand", "false")
            .finish();
        let body = self
            .request_bytes(repo, Some(commit), Method::POST, url, Some(form))
            .await?;
        let entries: Vec<TreeEntry> =
            serde_json::from_slice(&body).map_err(|e| Error::UnexpectedResponse {
                repo: repo.clone(),
                message: format!("the description of '{path}' could not be read ({e})"),
            })?;
        Ok(entries.into_iter().find(|entry| entry.path == path))
    }

    /// Reads `range` of the file at `path` and `commit` (the whole file when `None`).
    ///
    /// A download that fails part way resumes from the first byte not yet delivered: the file
    /// at a commit cannot change, so the resumed bytes continue the same content.
    ///
    /// # Errors
    ///
    /// Returns an error if the file does not exist, access is denied, the Hub cannot be
    /// reached, or the Hub does not return exactly the requested range.
    pub async fn read(
        self: &Arc<Self>,
        repo: &RepoId,
        commit: &str,
        path: &str,
        range: Option<GetRange>,
    ) -> Result<FileRead> {
        let file = FileKey {
            repo: repo.clone(),
            commit: commit.to_string(),
            path: path.to_string(),
        };
        let (response, e_tag) = self.open(&file, range.as_ref()).await?;
        // A whole-file response without `Content-Length` (a chunked proxy) is sized from the
        // file's description instead.
        let known_size = if range.is_none() && response.content_length().is_none() {
            self.entry(repo, commit, path)
                .await?
                .map(|entry| entry.size)
        } else {
            None
        };
        let (returned, size) = checked_range(&file, range.as_ref(), &response, known_size)?;
        let download = Download {
            hub: Arc::clone(self),
            next: returned.start,
            end: returned.end,
            file,
            body: Some(response.bytes_stream().boxed()),
            resumes_left: MAX_ATTEMPTS - 1,
        };
        Ok(FileRead {
            range: returned,
            e_tag,
            size,
            stream: download.into_stream(),
        })
    }

    /// Sends the read of `range` of `file`, following the Hub's redirects. A CDN URL resolved
    /// earlier is tried first; if it fails (an expired signature), the file is resolved again.
    ///
    /// Returns the response and the content id the Hub reported for the file.
    async fn open(
        &self,
        file: &FileKey,
        range: Option<&GetRange>,
    ) -> Result<(reqwest::Response, Option<String>)> {
        let FileKey { repo, commit, path } = file;
        let range_header = range.map(range_header);

        if let Some((cached, e_tag)) = self.file_urls.get(file).await {
            match self
                .request(
                    repo,
                    Some(commit),
                    Method::GET,
                    cached,
                    range_header.clone(),
                )
                .await
            {
                Ok(response) if response.status().is_success() => return Ok((response, e_tag)),
                Ok(response) => {
                    tracing::debug!(
                        "Re-resolving '{path}' of Hugging Face dataset '{repo}': its cached download URL returned HTTP {}",
                        response.status()
                    );
                }
                Err(error) => {
                    tracing::debug!(
                        "Re-resolving '{path}' of Hugging Face dataset '{repo}' after its cached download URL failed: {error}"
                    );
                }
            }
            self.file_urls.invalidate(file).await;
        }

        let mut url = self.resolve_url(repo, commit, path);
        let mut e_tag = None;
        for _ in 0..=MAX_REDIRECTS {
            let response = self
                .request(
                    repo,
                    Some(commit),
                    Method::GET,
                    url.clone(),
                    range_header.clone(),
                )
                .await?;
            if let Some(linked) = header_str(response.headers(), "x-linked-etag")
                .or_else(|| header_str(response.headers(), "etag"))
            {
                e_tag.get_or_insert_with(|| linked.trim_matches('"').to_string());
            }
            if !response.status().is_redirection() {
                return Ok((response, e_tag));
            }
            let location = response
                .headers()
                .get(LOCATION)
                .and_then(|value| value.to_str().ok())
                .and_then(|location| url.join(location).ok())
                .context(UnexpectedResponseSnafu {
                    repo: repo.clone(),
                    message: format!("the download of '{path}' redirected without a location"),
                })?;
            ensure!(
                redirect_allowed(&self.config.endpoint, &location),
                UnexpectedResponseSnafu {
                    repo: repo.clone(),
                    message: format!(
                        "the download of '{path}' redirected to a non-https URL, which was not followed"
                    ),
                }
            );
            // Only a redirect off the Hub's origin is a CDN URL worth reusing: the Hub's own
            // `resolve-cache` redirects are cheap and not signed.
            if !self.is_endpoint_origin(&location) {
                self.file_urls
                    .insert(file.clone(), (location.clone(), e_tag.clone()))
                    .await;
            }
            url = location;
        }
        UnexpectedResponseSnafu {
            repo: repo.clone(),
            message: format!("the download of '{path}' redirected more than {MAX_REDIRECTS} times"),
        }
        .fail()
    }

    /// `{endpoint}/api/datasets/{repo}/{kind...}/{revision}/{path}`.
    fn api_url(&self, repo: &RepoId, kind: &[&str], revision: &str, path: &str) -> Url {
        let mut url = self.config.endpoint.clone();
        {
            let mut segments = url
                .path_segments_mut()
                // The endpoint was validated to be an http(s) URL, which can be a base.
                .unwrap_or_else(|()| unreachable!("an http(s) endpoint has path segments"));
            segments
                .pop_if_empty()
                .extend(["api", "datasets", repo.owner(), repo.name()])
                .extend(kind)
                .push(revision);
            if !path.is_empty() {
                segments.extend(path.split('/'));
            }
        }
        url
    }

    /// `{endpoint}/datasets/{repo}/resolve/{commit}/{path}`.
    fn resolve_url(&self, repo: &RepoId, commit: &str, path: &str) -> Url {
        let mut url = self.config.endpoint.clone();
        {
            let mut segments = url
                .path_segments_mut()
                .unwrap_or_else(|()| unreachable!("an http(s) endpoint has path segments"));
            segments
                .pop_if_empty()
                .extend(["datasets", repo.owner(), repo.name(), "resolve", commit])
                .extend(path.split('/'));
        }
        url
    }

    fn is_endpoint_origin(&self, url: &Url) -> bool {
        url.origin() == self.config.endpoint.origin()
    }

    async fn request_bytes(
        &self,
        repo: &RepoId,
        revision: Option<&str>,
        method: Method,
        url: Url,
        form: Option<String>,
    ) -> Result<Bytes> {
        let response = self
            .send_with_retry(repo, revision, method, url, None, form)
            .await?;
        read_body(repo, &self.config.endpoint, response).await
    }

    async fn request(
        &self,
        repo: &RepoId,
        revision: Option<&str>,
        method: Method,
        url: Url,
        range: Option<String>,
    ) -> Result<reqwest::Response> {
        self.send_with_retry(repo, revision, method, url, range, None)
            .await
    }

    /// Sends a request, retrying connection failures, server errors and rate limiting with
    /// backoff, and maps a final error status to an [`Error`]. A redirect is returned as is.
    async fn send_with_retry(
        &self,
        repo: &RepoId,
        revision: Option<&str>,
        method: Method,
        url: Url,
        range: Option<String>,
        form: Option<String>,
    ) -> Result<reqwest::Response> {
        let mut backoff = INITIAL_BACKOFF;
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut request = self.http.request(method.clone(), url.clone());
            if let Some(range) = &range {
                request = request.header(RANGE, range);
            }
            if let Some(form) = &form {
                request = request
                    .header(
                        reqwest::header::CONTENT_TYPE,
                        "application/x-www-form-urlencoded",
                    )
                    .body(form.clone());
            }
            // The token goes only to the Hub's own origin, never to a CDN.
            if let Some(authorization) = &self.authorization
                && self.is_endpoint_origin(&url)
            {
                request = request.header(AUTHORIZATION, authorization.clone());
            }
            let request = request.build().map_err(|source| Error::Request {
                repo: repo.clone(),
                endpoint: self.config.endpoint.to_string(),
                attempts: attempt,
                source: source.without_url(),
            })?;

            let client = self.http.clone();
            let outcome = self
                .io_runtime
                .spawn(async move { client.execute(request).await })
                .await
                .context(TaskStoppedSnafu { repo: repo.clone() })?;

            let response = match outcome {
                Ok(response) => response,
                Err(source) => {
                    if attempt < MAX_ATTEMPTS && is_transient(&source) {
                        tracing::debug!(
                            "Retrying a request for Hugging Face dataset '{repo}' after {source} (attempt {attempt} of {MAX_ATTEMPTS})"
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        continue;
                    }
                    return Err(Error::Request {
                        repo: repo.clone(),
                        endpoint: self.config.endpoint.to_string(),
                        attempts: attempt,
                        source: source.without_url(),
                    });
                }
            };

            let status = response.status();
            if status.is_success() || status.is_redirection() {
                return Ok(response);
            }
            if status == StatusCode::TOO_MANY_REQUESTS {
                let wait = rate_limit_reset(response.headers());
                if attempt < MAX_ATTEMPTS && wait.is_none_or(|wait| wait <= MAX_RATE_LIMIT_WAIT) {
                    let wait = wait.unwrap_or(backoff).max(backoff);
                    tracing::debug!(
                        "The Hugging Face Hub rate-limited a request for dataset '{repo}'; retrying in {wait:?} (attempt {attempt} of {MAX_ATTEMPTS})"
                    );
                    tokio::time::sleep(wait).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    continue;
                }
                return RateLimitedSnafu {
                    repo: repo.clone(),
                    reset_hint: wait.map_or_else(String::new, |wait| {
                        format!(" (the limit resets in {}s)", wait.as_secs())
                    }),
                }
                .fail();
            }
            if attempt < MAX_ATTEMPTS && is_transient_status(status) {
                tracing::debug!(
                    "Retrying a request for Hugging Face dataset '{repo}' after HTTP {status} (attempt {attempt} of {MAX_ATTEMPTS})"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
                continue;
            }
            return Err(self.status_error(repo, revision, &url, &response, attempt));
        }
    }

    fn status_error(
        &self,
        repo: &RepoId,
        revision: Option<&str>,
        url: &Url,
        response: &reqwest::Response,
        attempts: u32,
    ) -> Error {
        let status = response.status();
        let code = header_str(response.headers(), "x-error-code").map(str::to_string);
        let message = header_str(response.headers(), "x-error-message")
            .map(str::to_string)
            .filter(|message| !message.is_empty());
        let has_token = self.config.token.is_some();
        let from_hub = self.is_endpoint_origin(url);

        match (status, code.as_deref()) {
            (_, Some("GatedRepo")) => Error::Gated {
                repo: repo.clone(),
                dataset_url: self.dataset_url(repo),
                token_hint: if has_token {
                    String::new()
                } else {
                    " and no `hf_token` is set".to_string()
                },
            },
            (_, Some("RepoNotFound")) => Error::RepoNotFound {
                repo: repo.clone(),
                private_hint: private_hint(has_token),
            },
            (_, Some("RevisionNotFound")) => Error::RevisionNotFound {
                repo: repo.clone(),
                revision: revision.unwrap_or_default().to_string(),
            },
            (StatusCode::NOT_FOUND, Some("EntryNotFound")) => Error::EntryNotFound {
                repo: repo.clone(),
                path: url_file_path(url),
            },
            // Without a token the Hub answers 401 for a dataset that does not exist and for one
            // that is private, so the two cannot be told apart.
            (StatusCode::UNAUTHORIZED, _) if !has_token && from_hub => Error::RepoNotFound {
                repo: repo.clone(),
                private_hint: private_hint(false),
            },
            (StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN, _) if from_hub => {
                Error::TokenRejected {
                    repo: repo.clone(),
                    status: status.as_u16(),
                }
            }
            _ => Error::Status {
                repo: repo.clone(),
                status: status.as_u16(),
                attempts,
                message: message.map_or_else(String::new, |message| {
                    format!(": {}", single_line(&message))
                }),
            },
        }
    }
}

impl Error {
    /// A copy of an error that a cache shared between concurrent callers.
    fn duplicate(&self) -> Self {
        match self {
            Error::RepoNotFound { repo, private_hint } => Error::RepoNotFound {
                repo: repo.clone(),
                private_hint: private_hint.clone(),
            },
            Error::RevisionNotFound { repo, revision } => Error::RevisionNotFound {
                repo: repo.clone(),
                revision: revision.clone(),
            },
            Error::EntryNotFound { repo, path } => Error::EntryNotFound {
                repo: repo.clone(),
                path: path.clone(),
            },
            Error::Gated {
                repo,
                dataset_url,
                token_hint,
            } => Error::Gated {
                repo: repo.clone(),
                dataset_url: dataset_url.clone(),
                token_hint: token_hint.clone(),
            },
            Error::TokenRejected { repo, status } => Error::TokenRejected {
                repo: repo.clone(),
                status: *status,
            },
            Error::RateLimited { repo, reset_hint } => Error::RateLimited {
                repo: repo.clone(),
                reset_hint: reset_hint.clone(),
            },
            Error::Status {
                repo,
                status,
                attempts,
                message,
            } => Error::Status {
                repo: repo.clone(),
                status: *status,
                attempts: *attempts,
                message: message.clone(),
            },
            // `reqwest::Error` and `JoinError` are not `Clone`: keep their message.
            Error::Request { .. }
            | Error::TaskStopped { .. }
            | Error::HttpClient { .. }
            | Error::InvalidToken
            | Error::InvalidEndpoint { .. }
            | Error::Shared { .. } => Error::Shared {
                message: self.to_string(),
            },
            Error::UnexpectedResponse { repo, message } => Error::UnexpectedResponse {
                repo: repo.clone(),
                message: message.clone(),
            },
        }
    }
}

fn private_hint(has_token: bool) -> String {
    if has_token {
        ", or the account `hf_token` belongs to cannot read it".to_string()
    } else {
        " or it is private: a private dataset needs `hf_token`".to_string()
    }
}

/// The `Range` header for `range`.
fn range_header(range: &GetRange) -> String {
    match range {
        // The store never sends an empty range: HTTP cannot express one.
        GetRange::Bounded(range) => {
            format!("bytes={}-{}", range.start, range.end.saturating_sub(1))
        }
        GetRange::Offset(offset) => format!("bytes={offset}-"),
        GetRange::Suffix(length) => format!("bytes=-{length}"),
    }
}

/// The absolute byte range a successful response carries and the size of the file, checked
/// against what was requested. A server that ignored `Range` and returned the whole file, or
/// returned a different range, must not be read as if it had returned the requested bytes.
fn checked_range(
    file: &FileKey,
    requested: Option<&GetRange>,
    response: &reqwest::Response,
    known_size: Option<u64>,
) -> Result<(Range<u64>, u64)> {
    let FileKey { repo, path, .. } = file;
    let status = response.status();
    let Some(requested) = requested else {
        ensure!(
            status == StatusCode::OK,
            UnexpectedResponseSnafu {
                repo: repo.clone(),
                message: format!("a read of '{path}' returned HTTP {status}"),
            }
        );
        let size = response
            .content_length()
            .or(known_size)
            .context(UnexpectedResponseSnafu {
                repo: repo.clone(),
                message: format!("a read of '{path}' returned no Content-Length"),
            })?;
        return Ok((0..size, size));
    };

    ensure!(
        status == StatusCode::PARTIAL_CONTENT,
        UnexpectedResponseSnafu {
            repo: repo.clone(),
            message: format!(
                "a read of {} of '{path}' returned HTTP {status} instead of the requested range",
                range_header(requested)
            ),
        }
    );
    let (start, end_inclusive, size) = response
        .headers()
        .get(CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_content_range)
        .context(UnexpectedResponseSnafu {
            repo: repo.clone(),
            message: format!("a ranged read of '{path}' returned no valid Content-Range"),
        })?;
    let expected = match requested {
        GetRange::Bounded(range) => range.start..range.end.min(size),
        GetRange::Offset(offset) => *offset..size,
        GetRange::Suffix(length) => size.saturating_sub(*length)..size,
    };
    let returned = start..end_inclusive + 1;
    ensure!(
        returned == expected,
        UnexpectedResponseSnafu {
            repo: repo.clone(),
            message: format!(
                "a read of {} of '{path}' returned bytes {}..{} of {size}",
                range_header(requested),
                returned.start,
                returned.end
            ),
        }
    );
    Ok((returned, size))
}

/// A download of `next..end` of a file that resumes after a failed connection.
struct Download {
    hub: Arc<Hub>,
    file: FileKey,
    /// The next byte to deliver.
    next: u64,
    /// The end of the range, exclusive.
    end: u64,
    body: Option<BoxStream<'static, reqwest::Result<Bytes>>>,
    resumes_left: u32,
}

impl Download {
    fn into_stream(self) -> BoxStream<'static, Result<Bytes>> {
        futures::stream::try_unfold(self, |mut download| async move {
            loop {
                if download.next >= download.end {
                    return Ok(None);
                }
                if download.body.is_none() {
                    let range = GetRange::Bounded(download.next..download.end);
                    let (response, _) = download.hub.open(&download.file, Some(&range)).await?;
                    checked_range(&download.file, Some(&range), &response, None)?;
                    download.body = Some(response.bytes_stream().boxed());
                }
                let Some(body) = download.body.as_mut() else {
                    continue;
                };
                let failure = match body.next().await {
                    Some(Ok(chunk)) => {
                        let remaining = download.end - download.next;
                        let take = usize::try_from(remaining).map_or(chunk.len(), |remaining| {
                            remaining.min(chunk.len())
                        });
                        download.next += take as u64;
                        if take == 0 {
                            continue;
                        }
                        return Ok(Some((chunk.slice(..take), download)));
                    }
                    Some(Err(source)) => source.without_url().to_string(),
                    None => format!(
                        "the connection closed {} bytes before the end of the range",
                        download.end - download.next
                    ),
                };
                let FileKey { repo, path, .. } = &download.file;
                ensure!(
                    download.resumes_left > 0,
                    UnexpectedResponseSnafu {
                        repo: repo.clone(),
                        message: format!("the download of '{path}' failed: {failure}"),
                    }
                );
                tracing::debug!(
                    "Resuming the download of '{path}' of Hugging Face dataset '{repo}' at byte {} after: {failure}",
                    download.next
                );
                download.resumes_left -= 1;
                download.body = None;
            }
        })
        .boxed()
    }
}

async fn read_body(repo: &RepoId, endpoint: &Url, response: reqwest::Response) -> Result<Bytes> {
    response.bytes().await.map_err(|source| Error::Request {
        repo: repo.clone(),
        endpoint: endpoint.to_string(),
        attempts: 1,
        source: source.without_url(),
    })
}

/// The `rel="next"` page of a paginated listing, `None` on the last page.
///
/// A next page on another origin — a mirror or proxy passing the Hub's own `Link` through —
/// is requested from the configured endpoint, which serves the same paths. A `Link` whose next
/// page cannot be read is an error: treating it as the last page would drop files.
fn next_page(headers: &HeaderMap, endpoint: &Url) -> Result<Option<Url>, String> {
    let Some(link) = headers.get(LINK) else {
        return Ok(None);
    };
    let link = link
        .to_str()
        .map_err(|_| "the listing's Link header is not valid text".to_string())?;
    let Some(target) = link.split(',').find_map(|part| {
        let (target, params) = part.trim().split_once(';')?;
        params
            .split(';')
            .any(|param| matches!(param.trim(), "rel=\"next\"" | "rel=next"))
            .then_some(target.trim())
    }) else {
        return Ok(None);
    };
    let mut url = target
        .strip_prefix('<')
        .and_then(|target| target.strip_suffix('>'))
        .and_then(|target| endpoint.join(target).ok())
        .ok_or_else(|| format!("the listing's next page {target:?} is not a valid URL"))?;
    if url.origin() != endpoint.origin() {
        // A mirror served under a path (`https://host/proxy`) serves the Hub's paths below it.
        let path = format!("{}{}", endpoint.path().trim_end_matches('/'), url.path());
        url.set_path(&path);
        let rebased = url.set_scheme(endpoint.scheme()).is_ok()
            && url.set_host(endpoint.host_str()).is_ok()
            && url.set_port(endpoint.port()).is_ok();
        if !rebased {
            return Err(format!(
                "the listing's next page {target:?} could not be requested from {endpoint}"
            ));
        }
    }
    Ok(Some(url))
}

/// The time until the Hub's rate-limit window resets, from its `RateLimit` header
/// (`"api";r=0;t=55`) or `Retry-After`.
fn rate_limit_reset(headers: &HeaderMap) -> Option<Duration> {
    let from_ratelimit = header_str(headers, "ratelimit").and_then(|value| {
        value
            .split(';')
            .find_map(|part| part.trim().strip_prefix("t="))
            .and_then(|seconds| seconds.trim().parse::<u64>().ok())
    });
    let from_retry_after = headers
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|seconds| seconds.trim().parse::<u64>().ok());
    from_ratelimit.or(from_retry_after).map(Duration::from_secs)
}

/// Parses `bytes {start}-{end}/{size}`.
fn parse_content_range(value: &str) -> Option<(u64, u64, u64)> {
    let (range, size) = value.strip_prefix("bytes ")?.split_once('/')?;
    let (start, end) = range.split_once('-')?;
    let (start, end, size) = (
        start.trim().parse().ok()?,
        end.trim().parse().ok()?,
        size.trim().parse().ok()?,
    );
    (start <= end && end < size).then_some((start, end, size))
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

/// The file path of a `resolve` or `tree` URL, for an error message.
fn url_file_path(url: &Url) -> String {
    let segments: Vec<_> = url
        .path_segments()
        .map(Iterator::collect)
        .unwrap_or_default();
    let skip = segments
        .iter()
        .position(|segment| matches!(*segment, "resolve" | "tree" | "paths-info"))
        .map_or(segments.len(), |index| index + 2);
    segments
        .iter()
        .skip(skip)
        .map(|segment| {
            percent_encoding::percent_decode_str(segment)
                .decode_utf8_lossy()
                .into_owned()
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn single_line(message: &str) -> String {
    message.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn is_transient(error: &reqwest::Error) -> bool {
    error.is_connect() || error.is_timeout() || error.is_request() || error.is_body()
}

fn is_transient_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::REQUEST_TIMEOUT
            | StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
    )
}

/// Whether a file read may follow a redirect to `location`: bytes from an https Hub are never
/// fetched in cleartext.
fn redirect_allowed(endpoint: &Url, location: &Url) -> bool {
    location.scheme() == "https" || (endpoint.scheme() == "http" && location.scheme() == "http")
}

/// Whether `revision` is a full commit SHA, which names an immutable snapshot.
#[must_use]
pub fn is_commit_sha(revision: &str) -> bool {
    revision.len() == 40 && revision.bytes().all(|b| b.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(value: &str) -> Url {
        Url::parse(value).expect("a valid URL")
    }

    #[test]
    fn content_range_is_parsed_and_validated() {
        assert_eq!(
            parse_content_range("bytes 0-15/20470363"),
            Some((0, 15, 20_470_363))
        );
        assert_eq!(parse_content_range("bytes 7-7/8"), Some((7, 7, 8)));
        for invalid in [
            "bytes 9-3/20",
            "bytes 0-20/20",
            "bytes */20",
            "bytes 0-1",
            "items 0-1/2",
            "",
        ] {
            assert_eq!(parse_content_range(invalid), None, "{invalid}");
        }
    }

    #[test]
    fn the_next_page_is_requested_from_the_endpoint() {
        let endpoint = url("https://hf-mirror.example");
        let next = |link: &'static str| {
            let mut headers = HeaderMap::new();
            headers.insert(LINK, HeaderValue::from_static(link));
            next_page(&headers, &endpoint).map(|page| page.map(|u| u.to_string()))
        };
        assert_eq!(
            next("<https://hf-mirror.example/api/datasets/o/d/tree/main?cursor=abc>; rel=\"next\""),
            Ok(Some(
                "https://hf-mirror.example/api/datasets/o/d/tree/main?cursor=abc".to_string()
            ))
        );
        // A mirror passing the Hub's own link through: the same page, from the mirror.
        assert_eq!(
            next("<https://huggingface.co/api/datasets/o/d/tree/main?cursor=abc>; rel=\"next\""),
            Ok(Some(
                "https://hf-mirror.example/api/datasets/o/d/tree/main?cursor=abc".to_string()
            ))
        );
        assert_eq!(
            next("<https://hf-mirror.example/x>; rel=\"prev\""),
            Ok(None)
        );
        assert_eq!(next_page(&HeaderMap::new(), &endpoint), Ok(None));
        // A mirror under a path keeps it.
        let proxy = url("https://artifacts.example/api/huggingfaceml/hf-remote");
        let mut headers = HeaderMap::new();
        headers.insert(
            LINK,
            HeaderValue::from_static(
                "<https://huggingface.co/api/datasets/o/d/tree/main?cursor=abc>; rel=\"next\"",
            ),
        );
        assert_eq!(
            next_page(&headers, &proxy).map(|page| page.map(|u| u.to_string())),
            Ok(Some(
                "https://artifacts.example/api/huggingfaceml/hf-remote/api/datasets/o/d/tree/main?cursor=abc"
                    .to_string()
            ))
        );
        assert_eq!(
            next("https://hf-mirror.example/x; rel=\"next\""),
            Err(
                "the listing's next page \"https://hf-mirror.example/x\" is not a valid URL"
                    .to_string()
            )
        );
    }

    #[test]
    fn the_rate_limit_reset_comes_from_the_hub_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("ratelimit", HeaderValue::from_static("\"api\";r=0;t=55"));
        assert_eq!(rate_limit_reset(&headers), Some(Duration::from_secs(55)));
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("7"));
        assert_eq!(rate_limit_reset(&headers), Some(Duration::from_secs(7)));
        assert_eq!(rate_limit_reset(&HeaderMap::new()), None);
    }

    #[test]
    fn a_read_never_follows_an_https_hub_to_cleartext() {
        let hub = url("https://huggingface.co");
        assert!(redirect_allowed(
            &hub,
            &url("https://cas-bridge.xethub.hf.co/x")
        ));
        assert!(!redirect_allowed(
            &hub,
            &url("http://cas-bridge.xethub.hf.co/x")
        ));
        let loopback = url("http://127.0.0.1:8080");
        assert!(redirect_allowed(
            &loopback,
            &url("http://localhost:9000/blob")
        ));
        assert!(!redirect_allowed(&loopback, &url("ftp://localhost/blob")));
    }

    #[test]
    fn hub_urls_keep_every_component_in_its_own_segment() {
        let hub = Hub::new(
            HubConfig {
                endpoint: url("https://huggingface.co"),
                token: None,
            },
            tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("a runtime")
                .handle()
                .clone(),
        )
        .expect("a Hub client");
        let repo = RepoId::new("o", "d").expect("a valid repository");
        assert_eq!(
            hub.api_url(
                &repo,
                &["tree"],
                "refs/convert/parquet",
                "a b/c%d#e?.parquet"
            )
            .as_str(),
            "https://huggingface.co/api/datasets/o/d/tree/refs%2Fconvert%2Fparquet/a%20b/c%25d%23e%3F.parquet"
        );
        assert_eq!(
            hub.resolve_url(
                &repo,
                "e6281661ce1c48d982bc483cf8a173c1bbeb5d31",
                "plain_text/x.parquet"
            )
            .as_str(),
            "https://huggingface.co/datasets/o/d/resolve/e6281661ce1c48d982bc483cf8a173c1bbeb5d31/plain_text/x.parquet"
        );
    }
}
