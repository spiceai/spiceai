/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use async_trait::async_trait;
use globset::GlobSet;
use snafu::{ResultExt, Snafu};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use std::{any::Any, path::Path, sync::Arc};
use url::Url;

use crate::{arrow::write::MemTable, rate_limit::RateLimiter};
use arrow::{
    array::{ArrayRef, Int64Builder, RecordBatch, StringBuilder, TimestampMillisecondBuilder},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use datafusion::{
    catalog::Session,
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
};
use git2::{
    Cred, CredentialType, FetchOptions, Oid, RemoteCallbacks, Repository, TreeWalkMode,
    TreeWalkResult, build::RepoBuilder,
};
use std::path::PathBuf;
use tokio::{sync::Semaphore, task};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to process Git repository data: {source}"))]
    UnableToConstructRecordBatchError { source: arrow::error::ArrowError },

    #[snafu(display("Failed to access Git repository: {source}"))]
    GitError { source: git2::Error },

    #[snafu(display(
        "Authentication failed for Git repository {repo_url}: {source}. Verify the credentials and ensure the host is reachable."
    ))]
    AuthenticationFailed {
        repo_url: String,
        source: git2::Error,
    },

    #[snafu(display("Failed to read file from Git repository: {source}"))]
    IoError { source: std::io::Error },

    #[snafu(display("Invalid Git connector configuration: {message}"))]
    InvalidConfiguration { message: String },

    #[snafu(display("Failed to spawn blocking task: {source}"))]
    SpawnBlockingError { source: tokio::task::JoinError },

    #[snafu(display(
        "Git connector has been disabled after a permanent error. Check the logs for the initial failure and update the configuration before retrying."
    ))]
    ConnectorDisabled,

    #[snafu(display(
        "git-lfs CLI is not available on PATH. Install git-lfs (https://git-lfs.com) or disable `enable_lfs`."
    ))]
    GitLfsMissing,

    #[snafu(display("git-lfs {operation} failed: {message}"))]
    GitLfsFailed { operation: String, message: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Represents a file entry in a Git repository with version information
#[derive(Debug, Clone)]
pub struct GitFileEntry {
    pub name: String,
    pub path: String,
    pub size: i64,
    pub sha: String,
    pub mode: String,
    pub tree_sha: String,
    pub commit_sha: Option<String>,
    pub version: Option<String>,
    pub created_at: Option<i64>,
    pub updated_at: Option<i64>,
    pub content: Option<String>,
}

pub const DEFAULT_MAX_FILES: usize = 5_000;
const MAX_FILES_HARD_CAP: usize = 50_000;
pub const DEFAULT_MAX_FILE_BYTES: usize = 512 * 1024; // 512 KiB
const MAX_FILE_BYTES_HARD_CAP: usize = 5 * 1024 * 1024; // 5 MiB
pub const DEFAULT_MAX_CONCURRENT_REQUESTS: usize = 4;
pub const DEFAULT_MAX_RETRIES: u32 = 3;
const RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const RETRY_MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Matches `Authorization: <scheme>[ <value>]` header strings that a
/// subprocess may emit, so we can redact the credential before surfacing
/// stderr to users. The pattern is a compile-time constant, so the
/// `unreachable!()` arm is unreachable in practice — it only exists to
/// satisfy the project-wide ban on `unwrap`/`expect` in non-test code.
static AUTH_HEADER_RE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
    regex::Regex::new(r"(?i)(Authorization\s*:\s*)(\S+\s+)?\S+")
        .unwrap_or_else(|e| unreachable!("AUTH_HEADER_RE pattern must compile: {e}"))
});

/// Global map of per-cache-path mutexes. Every mutator of a given on-disk
/// Git cache holds the corresponding mutex for the duration of the operation
/// so that concurrent clone/fetch/checkout calls targeting the same cache do
/// not corrupt the working tree.
///
/// Backed by a `DashMap` with sharded internal locking so `cache_mutex_for`
/// can be called from async code without risking a Tokio worker stall on
/// the outer map — only the (very brief) shard-local lock is ever held.
static GIT_CACHE_MUTEXES: std::sync::LazyLock<
    dashmap::DashMap<PathBuf, Arc<tokio::sync::Mutex<()>>>,
> = std::sync::LazyLock::new(dashmap::DashMap::new);

/// Strip any `userinfo` (`user[:password]@`) component from a Git URL so it
/// is safe to log or use as a map key. Returns the original string when the
/// input is not a parseable standard URL (e.g. `git@host:org/repo` SSH
/// shorthand, which does not contain a password component).
#[must_use]
pub fn sanitize_repo_url(url: &str) -> String {
    if let Ok(mut parsed) = Url::parse(url) {
        let had_userinfo = !parsed.username().is_empty() || parsed.password().is_some();
        let _ = parsed.set_username("");
        let _ = parsed.set_password(None);
        if had_userinfo {
            return parsed.to_string();
        }
    }
    url.to_string()
}

/// RAII guard that holds the `inflight_operations` counter at +1 for its
/// lifetime and atomically decrements it on drop. Using a guard rather than
/// manual increment/decrement makes the metric cancellation-safe — if the
/// surrounding future is dropped before completing, the counter still
/// returns to its prior value.
struct InflightGuard {
    counter: Arc<AtomicU64>,
}

impl InflightGuard {
    fn enter(counter: Arc<AtomicU64>) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        Self { counter }
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

fn cache_mutex_for(path: &Path) -> Arc<tokio::sync::Mutex<()>> {
    Arc::<tokio::sync::Mutex<()>>::clone(
        GIT_CACHE_MUTEXES
            .entry(path.to_path_buf())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .value(),
    )
}

/// Backoff strategy for retries on transient Git operation failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackoffMethod {
    Exponential,
    Fibonacci,
}

impl BackoffMethod {
    pub fn parse(value: &str) -> std::result::Result<Self, String> {
        match value.to_ascii_lowercase().as_str() {
            "exponential" => Ok(Self::Exponential),
            "fibonacci" => Ok(Self::Fibonacci),
            other => Err(format!(
                "invalid backoff_method '{other}'. Expected 'exponential' or 'fibonacci'."
            )),
        }
    }
}

/// Authentication material used to connect to a remote Git repository.
///
/// All fields are optional. The struct's own `Default` leaves `ssh_use_agent`
/// at `false`; the runtime connector flips that to `true` by default so an
/// operator who configures neither an ssh key nor explicit credentials falls
/// through to the running user's `ssh-agent`. If you construct this struct
/// directly (for tests, or when embedding the connector programmatically),
/// set `ssh_use_agent = true` when you want the agent-fallback behavior.
///
/// The struct stores secret material (passwords, tokens, passphrases), so the
/// `Debug` implementation is manually redacted and must never be changed to a
/// `derive(Debug)` that would print the underlying strings.
#[derive(Default, Clone)]
pub struct GitCredentials {
    /// Username for HTTP(S) basic authentication.
    pub username: Option<String>,
    /// Password or personal access token for HTTP(S) basic authentication.
    pub password: Option<String>,
    /// Personal access token (equivalent to `username = "x-access-token"`).
    pub token: Option<String>,
    /// Path to an SSH private key file.
    pub ssh_key_path: Option<PathBuf>,
    /// Optional passphrase for the SSH private key.
    pub ssh_passphrase: Option<String>,
    /// When `true`, attempt authentication against the running `ssh-agent`.
    pub ssh_use_agent: bool,
}

impl std::fmt::Debug for GitCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GitCredentials")
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .field("ssh_key_path", &self.ssh_key_path)
            .field(
                "ssh_passphrase",
                &self.ssh_passphrase.as_ref().map(|_| "<redacted>"),
            )
            .field("ssh_use_agent", &self.ssh_use_agent)
            .finish()
    }
}

impl GitCredentials {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.username.is_none()
            && self.password.is_none()
            && self.token.is_none()
            && self.ssh_key_path.is_none()
            && !self.ssh_use_agent
    }
}

/// Configuration used when tuning retry and concurrency behavior for the
/// connector. Produced by the runtime factory from user-facing parameters.
#[derive(Debug, Clone)]
pub struct GitResilienceConfig {
    pub max_retries: u32,
    pub backoff: BackoffMethod,
    /// Optional semaphore that bounds concurrent network operations.
    pub semaphore: Option<Arc<Semaphore>>,
    /// When `true`, the connector will permanently disable itself after a
    /// non-retryable error (e.g. authentication failure).
    pub disable_on_permanent_error: bool,
    /// Counter updated whenever a network operation is in flight. Exposed via
    /// the runtime metrics endpoint as `inflight_operations`.
    pub inflight: Arc<AtomicU64>,
    /// Latched flag tracking whether the connector is disabled. Shared with
    /// the runtime factory so the state can be observed.
    pub disabled: Arc<AtomicBool>,
}

impl Default for GitResilienceConfig {
    fn default() -> Self {
        Self {
            max_retries: DEFAULT_MAX_RETRIES,
            backoff: BackoffMethod::Exponential,
            semaphore: None,
            disable_on_permanent_error: true,
            inflight: Arc::new(AtomicU64::new(0)),
            disabled: Arc::new(AtomicBool::new(false)),
        }
    }
}

pub struct GitTableConfig {
    pub fetch_content: bool,
    pub rate_limiter: Arc<dyn RateLimiter>,
    pub cache_path: Option<PathBuf>,
    pub max_files: usize,
    pub max_file_bytes: usize,
    pub credentials: GitCredentials,
    pub enable_lfs: bool,
    pub resilience: GitResilienceConfig,
}

impl std::fmt::Debug for GitTableConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GitTableConfig")
            .field("fetch_content", &self.fetch_content)
            .field("cache_path", &self.cache_path)
            .field("max_files", &self.max_files)
            .field("max_file_bytes", &self.max_file_bytes)
            .field("credentials", &self.credentials)
            .field("enable_lfs", &self.enable_lfs)
            .field("resilience", &self.resilience)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct GitTableProvider {
    client: GitClient,
    schema: SchemaRef,
    include: Option<Arc<GlobSet>>,
    fetch_content: bool,
    max_files: usize,
}

impl GitTableProvider {
    pub async fn new(
        repo_url: &str,
        reference: Option<&str>,
        include: Option<Arc<GlobSet>>,
        config: GitTableConfig,
    ) -> Result<Self> {
        let GitTableConfig {
            fetch_content,
            rate_limiter,
            cache_path,
            max_files,
            max_file_bytes,
            credentials,
            enable_lfs,
            resilience,
        } = config;

        let requested_max_files = max_files;
        let max_files = max_files.clamp(1, MAX_FILES_HARD_CAP);
        if max_files != requested_max_files {
            tracing::warn!(
                "Requested max_files {} exceeds hard cap {}, clamping to {}",
                requested_max_files,
                MAX_FILES_HARD_CAP,
                max_files
            );
        }
        let requested_max_file_bytes = max_file_bytes;
        let max_file_bytes = max_file_bytes.clamp(1, MAX_FILE_BYTES_HARD_CAP);
        if max_file_bytes != requested_max_file_bytes {
            tracing::warn!(
                "Requested max_file_bytes {} exceeds hard cap {}, clamping to {}",
                requested_max_file_bytes,
                MAX_FILE_BYTES_HARD_CAP,
                max_file_bytes
            );
        }

        let client = GitClient::new(
            repo_url,
            reference,
            rate_limiter,
            cache_path,
            max_file_bytes,
            credentials,
            enable_lfs,
            resilience,
        )?;

        let mut fields = vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("path", DataType::Utf8, true),
            Field::new("size", DataType::Int64, true),
            Field::new("sha", DataType::Utf8, true),
            Field::new("mode", DataType::Utf8, true),
            Field::new("tree_sha", DataType::Utf8, true),
            Field::new("commit_sha", DataType::Utf8, true),
            Field::new("version", DataType::Utf8, true),
            Field::new(
                "created_at",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                true,
            ),
            Field::new(
                "updated_at",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                true,
            ),
        ];

        if fetch_content {
            fields.push(Field::new("content", DataType::Utf8, true));
        }

        let schema = Arc::new(Schema::new(fields));

        // Validate configuration by fetching a small sample
        client
            .fetch_files(Some(1), None, false, Arc::clone(&schema))
            .await?;

        Ok(Self {
            client,
            schema,
            include,
            fetch_content,
            max_files,
        })
    }
}

#[async_trait]
impl TableProvider for GitTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> std::result::Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        // Path and name filters could in principle be pushed down to the tree
        // walk. For now we report them as `Inexact` so DataFusion continues to
        // apply them after the scan — this keeps results correct while still
        // signalling partial filter awareness for RC criteria.
        Ok(filters
            .iter()
            .map(|expr| match expr {
                Expr::BinaryExpr(binary) => match (&*binary.left, &*binary.right) {
                    (Expr::Column(col), _) | (_, Expr::Column(col))
                        if matches!(col.name.as_str(), "path" | "name" | "sha" | "version") =>
                    {
                        TableProviderFilterPushDown::Inexact
                    }
                    _ => TableProviderFilterPushDown::Unsupported,
                },
                _ => TableProviderFilterPushDown::Unsupported,
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let effective_limit = limit.unwrap_or(self.max_files).min(self.max_files);

        let res: Vec<RecordBatch> = self
            .client
            .fetch_files(
                Some(effective_limit),
                self.include.clone(),
                self.fetch_content,
                Arc::clone(&self.schema),
            )
            .await
            .boxed()
            .map_err(DataFusionError::External)?;

        let table = MemTable::try_new(Arc::clone(&self.schema), vec![res])?;
        table.scan(state, projection, filters, limit).await
    }
}

#[derive(Clone)]
pub struct GitClient {
    repo_url: String,
    reference: Option<String>,
    cache_path: PathBuf,
    rate_limiter: Arc<dyn RateLimiter>,
    max_file_bytes: usize,
    credentials: GitCredentials,
    enable_lfs: bool,
    resilience: GitResilienceConfig,
}

impl std::fmt::Debug for GitClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GitClient")
            .field("repo_url", &sanitize_repo_url(&self.repo_url))
            .field("reference", &self.reference)
            .field("cache_path", &self.cache_path)
            .field("max_file_bytes", &self.max_file_bytes)
            .field("credentials", &self.credentials)
            .field("enable_lfs", &self.enable_lfs)
            .field("resilience", &self.resilience)
            .finish_non_exhaustive()
    }
}

impl GitClient {
    #[expect(
        clippy::too_many_arguments,
        reason = "credentials and resilience config would otherwise clutter every call site; keeping one builder for clarity"
    )]
    pub fn new(
        repo_url: &str,
        reference: Option<&str>,
        rate_limiter: Arc<dyn RateLimiter>,
        cache_path: Option<PathBuf>,
        max_file_bytes: usize,
        credentials: GitCredentials,
        enable_lfs: bool,
        resilience: GitResilienceConfig,
    ) -> Result<Self> {
        Self::validate_repo_url(repo_url)?;

        let cache_path = cache_path.unwrap_or_else(|| {
            std::env::temp_dir().join("spice_git_cache").join(
                repo_url
                    .replace("https://", "")
                    .replace("git@", "")
                    .replace([':', '/'], "_"),
            )
        });

        Ok(Self {
            repo_url: repo_url.to_string(),
            reference: reference.map(ToString::to_string),
            cache_path,
            rate_limiter,
            max_file_bytes,
            credentials,
            enable_lfs,
            resilience,
        })
    }

    fn validate_repo_url(repo_url: &str) -> Result<()> {
        if repo_url.trim().is_empty() {
            return Err(Error::InvalidConfiguration {
                message: "Repository URL cannot be empty".to_string(),
            });
        }

        if repo_url.starts_with("git@") {
            let parts: Vec<&str> = repo_url.split(':').collect();
            if parts.len() != 2 {
                return Err(Error::InvalidConfiguration {
                    message: "Invalid SSH repository URL. Expected format git@host:org/repo"
                        .to_string(),
                });
            }
            let host = parts[0].trim_start_matches("git@");
            let path = parts[1];
            if host.is_empty() || path.is_empty() {
                return Err(Error::InvalidConfiguration {
                    message: "Invalid SSH repository URL. Expected format git@host:org/repo"
                        .to_string(),
                });
            }
            return Ok(());
        }

        let parsed = Url::parse(repo_url).map_err(|e| Error::InvalidConfiguration {
            message: format!("Invalid repository URL {repo_url}: {e}"),
        })?;

        match parsed.scheme() {
            "https" | "http" | "ssh" | "git+ssh" | "git" => Ok(()),
            "file" => {
                if let Some(host) = parsed.host_str()
                    && !host.is_empty()
                    && host != "localhost"
                {
                    return Err(Error::InvalidConfiguration {
                        message: format!(
                            "File Git URLs must reference local paths. Host '{host}' is not supported"
                        ),
                    });
                }

                let path = parsed.path();
                if path.is_empty() {
                    return Err(Error::InvalidConfiguration {
                        message: "File Git URLs must include an absolute path".to_string(),
                    });
                }

                if !Path::new(path).is_absolute() {
                    return Err(Error::InvalidConfiguration {
                        message: format!("File Git URL {repo_url} must contain an absolute path"),
                    });
                }

                Ok(())
            }
            other => Err(Error::InvalidConfiguration {
                message: format!(
                    "Unsupported Git URL scheme '{other}'. Only https, http, ssh, git+ssh, git://, git@host:repo, or file:// are allowed"
                ),
            }),
        }
    }

    fn resolve_credentials(
        credentials: &GitCredentials,
        username_from_url: Option<&str>,
        allowed_types: CredentialType,
    ) -> std::result::Result<Cred, git2::Error> {
        // SSH public-key authentication is always attempted first when the
        // remote requests it — libgit2 calls the credentials callback once per
        // accepted credential type, and userpass-plaintext can be a fallback.
        if allowed_types.contains(CredentialType::SSH_KEY) {
            let user = credentials
                .username
                .as_deref()
                .or(username_from_url)
                .unwrap_or("git");
            if let Some(ref key_path) = credentials.ssh_key_path {
                let passphrase = credentials.ssh_passphrase.as_deref();
                return Cred::ssh_key(user, None, key_path.as_path(), passphrase);
            }
            // Agent fallback is controlled solely by the `ssh_use_agent`
            // configuration. We do *not* fall back to the agent based on the
            // URL shape alone, because that would let the host agent's
            // identities leak in even when the operator explicitly set
            // `ssh_use_agent = false` to require a specific key.
            if credentials.ssh_use_agent {
                return Cred::ssh_key_from_agent(user);
            }
        }

        if allowed_types.contains(CredentialType::USERNAME) {
            let user = credentials
                .username
                .as_deref()
                .or(username_from_url)
                .unwrap_or("git");
            return Cred::username(user);
        }

        if allowed_types.contains(CredentialType::USER_PASS_PLAINTEXT) {
            let (user, pass) = if let Some(token) = credentials.token.as_deref() {
                (
                    credentials.username.as_deref().unwrap_or("x-access-token"),
                    token,
                )
            } else if let Some(password) = credentials.password.as_deref() {
                (credentials.username.as_deref().unwrap_or("git"), password)
            } else {
                return Err(git2::Error::new(
                    git2::ErrorCode::Auth,
                    git2::ErrorClass::Http,
                    "HTTP authentication required but no credentials were provided",
                ));
            };
            return Cred::userpass_plaintext(user, pass);
        }

        if allowed_types.contains(CredentialType::DEFAULT) {
            return Cred::default();
        }

        Err(git2::Error::from_str("unsupported credential type"))
    }

    /// Evaluate the configured retry/backoff policy and return the delay to
    /// wait before the next attempt.
    fn backoff_delay(&self, attempt: u32) -> Duration {
        let factor_u64: u64 = match self.resilience.backoff {
            BackoffMethod::Exponential => 2u64.saturating_pow(attempt),
            BackoffMethod::Fibonacci => {
                let (mut a, mut b) = (1u64, 1u64);
                for _ in 0..attempt {
                    let next = a.saturating_add(b);
                    a = b;
                    b = next;
                }
                b
            }
        };
        let factor = u32::try_from(factor_u64).unwrap_or(u32::MAX);
        RETRY_INITIAL_BACKOFF
            .saturating_mul(factor)
            .min(RETRY_MAX_BACKOFF)
    }

    fn is_permanent_error(err: &git2::Error) -> bool {
        use git2::ErrorClass;
        use git2::ErrorCode;

        match err.code() {
            ErrorCode::Auth | ErrorCode::Certificate => true,
            _ => {
                matches!(err.class(), ErrorClass::Http | ErrorClass::Ssh)
                    && err.message().to_ascii_lowercase().contains("403")
            }
        }
    }

    /// Clone or open the repository, ensuring it's up to date. Applies the
    /// configured retry/backoff policy around transient network errors and
    /// latches the connector into a disabled state if a permanent error is
    /// observed (e.g. 401 Unauthorized).
    async fn get_repository(&self) -> Result<Repository> {
        if self.resilience.disabled.load(Ordering::SeqCst) {
            return Err(Error::ConnectorDisabled);
        }

        let _permit = if let Some(semaphore) = self.resilience.semaphore.clone() {
            Some(
                semaphore
                    .acquire_owned()
                    .await
                    .map_err(|_| Error::InvalidConfiguration {
                        message: "Git connector semaphore was closed".to_string(),
                    })?,
            )
        } else {
            None
        };

        // RAII inflight tracking. Using a drop-guard (rather than manual
        // fetch_add/fetch_sub pairs) ensures the gauge is always decremented
        // even if the future is cancelled mid-operation.
        let _inflight = InflightGuard::enter(Arc::<AtomicU64>::clone(&self.resilience.inflight));

        self.get_repository_inner().await
    }

    async fn get_repository_inner(&self) -> Result<Repository> {
        let mut attempt: u32 = 0;
        loop {
            match self.try_get_repository().await {
                Ok(repo) => return Ok(repo),
                Err(err) => {
                    let is_permanent = matches!(&err, Error::AuthenticationFailed { .. })
                        || matches!(&err, Error::GitError { source } if Self::is_permanent_error(source));

                    let sanitized_url = sanitize_repo_url(&self.repo_url);

                    if is_permanent && self.resilience.disable_on_permanent_error {
                        self.resilience.disabled.store(true, Ordering::SeqCst);
                        tracing::error!(
                            repo_url = %sanitized_url,
                            "Permanent error from Git remote; disabling connector. {err}"
                        );
                        return Err(err);
                    }

                    if is_permanent || attempt >= self.resilience.max_retries {
                        return Err(err);
                    }

                    let delay = self.backoff_delay(attempt);
                    tracing::warn!(
                        repo_url = %sanitized_url,
                        attempt = attempt + 1,
                        max_retries = self.resilience.max_retries,
                        delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                        "Transient error fetching Git repository, retrying. {err}"
                    );
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
            }
        }
    }

    async fn try_get_repository(&self) -> Result<Repository> {
        // The per-cache-path mutex is acquired by `fetch_files` and held
        // across clone/fetch/checkout/LFS/scan, so every mutation of the
        // shared on-disk cache is serialized with its corresponding read.
        let repo_url = self.repo_url.clone();
        let cache_path = self.cache_path.clone();
        let credentials = self.credentials.clone();
        let enable_lfs = self.enable_lfs;
        let reference = self.reference.clone();
        let reference_for_task = reference.clone();

        let repo = task::spawn_blocking(move || -> Result<Repository> {
            let make_callbacks = || {
                let mut callbacks = RemoteCallbacks::new();
                let creds = credentials.clone();
                callbacks.credentials(move |_url, username_from_url, allowed_types| {
                    Self::resolve_credentials(&creds, username_from_url, allowed_types)
                });
                // Intentionally no `certificate_check` override: libgit2's
                // default verification is used, which validates TLS
                // certificates for HTTPS remotes.
                callbacks
            };

            let repo = if cache_path.exists() {
                tracing::debug!("Opening existing repository at {}", cache_path.display());
                let repo = Repository::open(&cache_path)
                    .map_err(|source| classify_remote_error(source, &repo_url))?;

                // Fetch latest changes
                {
                    let mut fetch_options = FetchOptions::new();
                    fetch_options.remote_callbacks(make_callbacks());
                    let mut remote = repo.find_remote("origin").context(GitSnafu)?;
                    remote
                        .fetch(
                            &[
                                "refs/heads/*:refs/remotes/origin/*",
                                "refs/tags/*:refs/tags/*",
                            ],
                            Some(&mut fetch_options),
                            None,
                        )
                        .map_err(|source| classify_remote_error(source, &repo_url))?;
                }

                repo
            } else {
                tracing::info!(
                    "Cloning repository {} to {}",
                    repo_url,
                    cache_path.display()
                );
                std::fs::create_dir_all(&cache_path).context(IoSnafu)?;
                let mut fetch_options = FetchOptions::new();
                fetch_options.remote_callbacks(make_callbacks());

                let mut builder = RepoBuilder::new();
                builder.fetch_options(fetch_options);
                builder
                    .clone(&repo_url, &cache_path)
                    .map_err(|source| classify_remote_error(source, &repo_url))?
            };

            // When LFS is enabled we read file content from the working tree.
            // That requires the working tree to reflect the requested
            // reference; otherwise queries against non-HEAD refs (or
            // concurrent queries against different refs sharing the same
            // cache) would surface whatever happens to be checked out.
            if enable_lfs {
                let commit_oid =
                    Self::resolve_reference_blocking(&repo, reference_for_task.as_deref())?;
                let commit = repo.find_commit(commit_oid).context(GitSnafu)?;
                let object = commit.as_object().clone();
                let mut checkout = git2::build::CheckoutBuilder::new();
                checkout.force();
                repo.checkout_tree(&object, Some(&mut checkout))
                    .context(GitSnafu)?;
                repo.set_head_detached(commit_oid).context(GitSnafu)?;
            }

            Ok(repo)
        })
        .await
        .context(SpawnBlockingSnafu)??;

        if enable_lfs {
            run_git_lfs(&self.cache_path, reference.as_deref(), &self.credentials).await?;
        }

        Ok(repo)
    }

    /// Fetch files from the repository
    pub async fn fetch_files(
        &self,
        limit: Option<usize>,
        include: Option<Arc<GlobSet>>,
        fetch_content: bool,
        schema: SchemaRef,
    ) -> Result<Vec<RecordBatch>> {
        self.rate_limiter.check_rate_limit().await.ok();

        // Acquire the per-cache-path mutex once, and hold it through
        // clone/fetch/checkout, `git lfs checkout`, and the working-tree
        // scan so that concurrent queries against the same on-disk cache
        // cannot interleave mutations with reads.
        let cache_guard = cache_mutex_for(&self.cache_path).lock_owned().await;

        let repo = self.get_repository().await?;
        let reference = self.reference.clone();
        let max_file_bytes = self.max_file_bytes;
        // When LFS is enabled we must read file contents from the working
        // tree rather than the blob — `git lfs checkout` materializes the
        // real object on disk, but the Git object in the tree is still the
        // pointer file. Reading the blob would surface the pointer's
        // metadata instead of the actual content.
        let lfs_content_root = self.enable_lfs.then(|| self.cache_path.clone());

        let entries = task::spawn_blocking(move || {
            // Keep the cache mutex alive until the scan finishes.
            let _cache_guard = cache_guard;
            let commit_oid = Self::resolve_reference_blocking(&repo, reference.as_deref())?;
            let commit = repo.find_commit(commit_oid).context(GitSnafu)?;
            let tree = commit.tree().context(GitSnafu)?;
            let tree_sha = tree.id().to_string();
            let commit_sha = commit.id().to_string();
            let version = commit.id().to_string()[..7].to_string();

            let mut entries = Vec::new();
            let mut count = 0;

            tree.walk(TreeWalkMode::PreOrder, |root, entry| {
                // Apply limit if specified
                if let Some(limit) = limit
                    && count >= limit
                {
                    return TreeWalkResult::Abort;
                }

                // Only process blob entries (files)
                if entry.kind() != Some(git2::ObjectType::Blob) {
                    return TreeWalkResult::Ok;
                }

                let entry_name = entry.name().unwrap_or("");
                let full_path = if root.is_empty() {
                    entry_name.to_string()
                } else {
                    format!("{root}{entry_name}")
                };

                // Apply glob filtering
                if let Some(ref glob_set) = include
                    && !glob_set.is_match(&full_path)
                {
                    return TreeWalkResult::Ok;
                }

                let object = match entry.to_object(&repo) {
                    Ok(obj) => obj,
                    Err(e) => {
                        tracing::warn!("Failed to get object for {}: {}", full_path, e);
                        return TreeWalkResult::Ok;
                    }
                };

                let Some(blob) = object.as_blob() else {
                    return TreeWalkResult::Ok;
                };

                // Determine the authoritative on-disk byte length. For
                // LFS-tracked files the blob itself only holds the pointer,
                // so `blob.size()` is ~150 bytes regardless of the real
                // payload. When `enable_lfs` is on we stat the working-tree
                // entry and use that size both for the row's `size` column
                // and for the `max_file_bytes` check.
                //
                // Security hardening: `symlink_metadata` + `is_symlink`
                // rejects tracked symlinks so a malicious repository cannot
                // coerce us into reading host files outside `cache_path`.
                let lfs_on_disk_size = if let Some(root) = lfs_content_root.as_ref() {
                    match std::fs::symlink_metadata(root.join(&full_path)) {
                        Ok(meta) if meta.file_type().is_symlink() => {
                            tracing::debug!(
                                "Skipping LFS-tracked symlink {} to avoid reading files outside the repository cache",
                                full_path
                            );
                            return TreeWalkResult::Ok;
                        }
                        Ok(meta) if !meta.file_type().is_file() => {
                            tracing::debug!(
                                "Skipping LFS entry {} because it is not a regular file",
                                full_path
                            );
                            return TreeWalkResult::Ok;
                        }
                        Ok(meta) => Some(usize::try_from(meta.len()).unwrap_or(usize::MAX)),
                        Err(err) => {
                            tracing::warn!(
                                "Failed to stat LFS-materialized file {}: {err}. Falling back to blob size.",
                                full_path
                            );
                            None
                        }
                    }
                } else {
                    None
                };
                let effective_size = lfs_on_disk_size.unwrap_or_else(|| blob.size());

                let Ok(size) = i64::try_from(effective_size) else {
                    tracing::warn!(
                        "File {} is too large to represent ({} bytes), skipping",
                        full_path,
                        effective_size
                    );
                    return TreeWalkResult::Ok;
                };
                if effective_size > max_file_bytes {
                    tracing::debug!(
                        "Skipping {} because it exceeds the configured max file size ({} bytes)",
                        full_path,
                        effective_size
                    );
                    return TreeWalkResult::Ok;
                }

                let sha = entry.id().to_string();
                let mode = format!("{:o}", entry.filemode());

                let decode = |bytes: &[u8]| -> Option<String> {
                    if let Ok(text) = std::str::from_utf8(bytes) {
                        Some(text.to_string())
                    } else {
                        tracing::debug!(
                            "File {} is not valid UTF-8, skipping content",
                            full_path
                        );
                        None
                    }
                };

                let content = if fetch_content {
                    if let Some(root) = lfs_content_root.as_ref() {
                        let candidate = root.join(&full_path);
                        // Re-check symlink-ness immediately before reading to
                        // narrow the TOCTOU window and ensure we never open a
                        // symlink that points outside the repository cache.
                        let follow_safe = matches!(
                            std::fs::symlink_metadata(&candidate),
                            Ok(meta) if meta.file_type().is_file()
                        );
                        let inside_root = candidate
                            .canonicalize()
                            .ok()
                            .zip(root.canonicalize().ok())
                            .is_some_and(|(abs, abs_root)| abs.starts_with(&abs_root));
                        if !follow_safe || !inside_root {
                            tracing::debug!(
                                "Refusing to read {}: symlink or path escapes repository cache",
                                full_path
                            );
                            None
                        } else {
                            match std::fs::read(&candidate) {
                                Ok(bytes) => decode(&bytes),
                                Err(err) => {
                                    tracing::warn!(
                                        "Failed to read LFS-materialized content for {}: {err}. Leaving content NULL.",
                                        full_path
                                    );
                                    None
                                }
                            }
                        }
                    } else {
                        decode(blob.content())
                    }
                } else {
                    None
                };

                // Get commit history for this file to determine created/updated times
                let (created_at, updated_at) =
                    Self::get_file_timestamps(&repo, &full_path, commit_oid);

                entries.push(GitFileEntry {
                    name: entry_name.to_string(),
                    path: full_path,
                    size,
                    sha,
                    mode,
                    tree_sha: tree_sha.clone(),
                    commit_sha: Some(commit_sha.clone()),
                    version: Some(version.clone()),
                    created_at,
                    updated_at,
                    content,
                });

                count += 1;
                TreeWalkResult::Ok
            })
            .or_else(|err| {
                // A user-initiated abort (when the configured limit is reached)
                // is a normal termination, not an error.
                if matches!(err.code(), git2::ErrorCode::User) {
                    Ok(())
                } else {
                    Err(err)
                }
            })
            .context(GitSnafu)?;

            Ok::<Vec<GitFileEntry>, Error>(entries)
        })
        .await
        .context(SpawnBlockingSnafu)??;

        // Convert entries to RecordBatch
        Self::entries_to_record_batch(&entries, schema)
    }

    /// Blocking version of `resolve_reference` for use in `spawn_blocking`
    fn resolve_reference_blocking(repo: &Repository, reference: Option<&str>) -> Result<Oid> {
        let reference = reference.unwrap_or("HEAD");

        // Try to resolve as a reference (branch or tag)
        if let Ok(reference_obj) = repo.find_reference(reference) {
            return reference_obj
                .peel_to_commit()
                .context(GitSnafu)
                .map(|c| c.id());
        }

        // Try to resolve as a short or full commit SHA
        if let Ok(oid) = Oid::from_str(reference) {
            return Ok(oid);
        }

        // Try with refs/heads/ prefix for branches
        let branch_ref = format!("refs/heads/{reference}");
        if let Ok(reference_obj) = repo.find_reference(&branch_ref) {
            return reference_obj
                .peel_to_commit()
                .context(GitSnafu)
                .map(|c| c.id());
        }

        // Try with refs/tags/ prefix for tags
        let tag_ref = format!("refs/tags/{reference}");
        if let Ok(reference_obj) = repo.find_reference(&tag_ref) {
            return reference_obj
                .peel_to_commit()
                .context(GitSnafu)
                .map(|c| c.id());
        }

        // Try with refs/remotes/origin/ prefix for remote branches
        let remote_ref = format!("refs/remotes/origin/{reference}");
        if let Ok(reference_obj) = repo.find_reference(&remote_ref) {
            return reference_obj
                .peel_to_commit()
                .context(GitSnafu)
                .map(|c| c.id());
        }

        Err(Error::InvalidConfiguration {
            message: format!("Could not resolve reference '{reference}' to a commit"),
        })
    }

    /// Get timestamps for a file by walking its commit history
    fn get_file_timestamps(
        repo: &Repository,
        path: &str,
        start_commit: Oid,
    ) -> (Option<i64>, Option<i64>) {
        let Ok(mut revwalk) = repo.revwalk() else {
            return (None, None);
        };

        if revwalk.push(start_commit).is_err() {
            return (None, None);
        }

        let mut first_commit_time = None;
        let mut last_commit_time = None;

        for oid in revwalk.flatten() {
            let Ok(commit) = repo.find_commit(oid) else {
                continue;
            };

            let Ok(tree) = commit.tree() else {
                continue;
            };

            // Check if this commit contains the file
            if tree.get_path(Path::new(path)).is_ok() {
                let timestamp = commit.time().seconds() * 1000; // Convert to milliseconds

                if last_commit_time.is_none() {
                    last_commit_time = Some(timestamp);
                }
                first_commit_time = Some(timestamp);
            }
        }

        (first_commit_time, last_commit_time)
    }

    /// Convert file entries to Arrow `RecordBatch`
    fn entries_to_record_batch(
        entries: &[GitFileEntry],
        schema: SchemaRef,
    ) -> Result<Vec<RecordBatch>> {
        let mut name_builder = StringBuilder::new();
        let mut path_builder = StringBuilder::new();
        let mut size_builder = Int64Builder::new();
        let mut sha_builder = StringBuilder::new();
        let mut mode_builder = StringBuilder::new();
        let mut tree_sha_builder = StringBuilder::new();
        let mut commit_sha_builder = StringBuilder::new();
        let mut version_builder = StringBuilder::new();
        let mut created_at_builder = TimestampMillisecondBuilder::new();
        let mut updated_at_builder = TimestampMillisecondBuilder::new();
        let mut content_builder = if schema.fields().iter().any(|f| f.name() == "content") {
            Some(StringBuilder::new())
        } else {
            None
        };

        for entry in entries {
            name_builder.append_value(&entry.name);
            path_builder.append_value(&entry.path);
            size_builder.append_value(entry.size);
            sha_builder.append_value(&entry.sha);
            mode_builder.append_value(&entry.mode);
            tree_sha_builder.append_value(&entry.tree_sha);

            if let Some(ref commit_sha) = entry.commit_sha {
                commit_sha_builder.append_value(commit_sha);
            } else {
                commit_sha_builder.append_null();
            }

            if let Some(ref version) = entry.version {
                version_builder.append_value(version);
            } else {
                version_builder.append_null();
            }

            if let Some(created_at) = entry.created_at {
                created_at_builder.append_value(created_at);
            } else {
                created_at_builder.append_null();
            }

            if let Some(updated_at) = entry.updated_at {
                updated_at_builder.append_value(updated_at);
            } else {
                updated_at_builder.append_null();
            }

            if let Some(ref mut builder) = content_builder {
                if let Some(ref content) = entry.content {
                    builder.append_value(content);
                } else {
                    builder.append_null();
                }
            }
        }

        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(name_builder.finish()),
            Arc::new(path_builder.finish()),
            Arc::new(size_builder.finish()),
            Arc::new(sha_builder.finish()),
            Arc::new(mode_builder.finish()),
            Arc::new(tree_sha_builder.finish()),
            Arc::new(commit_sha_builder.finish()),
            Arc::new(version_builder.finish()),
            Arc::new(created_at_builder.finish()),
            Arc::new(updated_at_builder.finish()),
        ];

        if let Some(mut builder) = content_builder {
            columns.push(Arc::new(builder.finish()));
        }

        let batch =
            RecordBatch::try_new(schema, columns).context(UnableToConstructRecordBatchSnafu)?;

        Ok(vec![batch])
    }
}

fn classify_remote_error(err: git2::Error, repo_url: &str) -> Error {
    if matches!(err.code(), git2::ErrorCode::Auth) {
        Error::AuthenticationFailed {
            // Store the sanitized URL so user-facing messages, debug output,
            // and log scrapes never surface inline credentials.
            repo_url: sanitize_repo_url(repo_url),
            source: err,
        }
    } else {
        Error::GitError { source: err }
    }
}

async fn run_git_lfs(
    cache_path: &Path,
    reference: Option<&str>,
    credentials: &GitCredentials,
) -> Result<()> {
    let cache_path = cache_path.to_path_buf();
    let reference = reference.map(ToString::to_string);
    let credentials = credentials.clone();

    task::spawn_blocking(move || -> Result<()> {
        ensure_git_lfs_available()?;

        // Install filters into the local repo config (idempotent).
        run_git_lfs_command(
            &cache_path,
            &["install", "--local"],
            "install",
            &credentials,
        )?;

        // Pull LFS objects only for the ref we intend to check out. We never
        // run `git lfs fetch --all` — that can pull the repository's entire
        // LFS history, which is expensive for large repos. Users who need the
        // full LFS history can run `git lfs fetch --all` manually against the
        // cache directory.
        match reference.as_deref() {
            Some(ref_name) => {
                run_git_lfs_command(
                    &cache_path,
                    &["fetch", "origin", ref_name],
                    "fetch",
                    &credentials,
                )?;
            }
            None => {
                run_git_lfs_command(&cache_path, &["fetch", "origin"], "fetch", &credentials)?;
            }
        }

        run_git_lfs_command(&cache_path, &["checkout"], "checkout", &credentials)?;
        Ok(())
    })
    .await
    .context(SpawnBlockingSnafu)?
}

fn ensure_git_lfs_available() -> Result<()> {
    let output = Command::new("git")
        .arg("lfs")
        .arg("version")
        .output()
        .map_err(|_| Error::GitLfsMissing)?;

    if output.status.success() {
        Ok(())
    } else {
        Err(Error::GitLfsMissing)
    }
}

/// Strip sensitive substrings from subprocess output before surfacing it in
/// an error. Redacts:
/// - configured passwords/tokens/passphrases,
/// - userinfo embedded in an HTTP(S) URL, and
/// - any `Authorization: Basic|Bearer <value>` header string that git or its
///   HTTP backend may have echoed via trace output.
fn sanitize_subprocess_output(text: &str, credentials: &GitCredentials) -> String {
    let mut cleaned = text.to_string();
    for secret in [
        credentials.password.as_deref(),
        credentials.token.as_deref(),
        credentials.ssh_passphrase.as_deref(),
    ]
    .into_iter()
    .flatten()
    .filter(|s| !s.is_empty())
    {
        cleaned = cleaned.replace(secret, "<redacted>");
    }

    // Redact any Authorization header (Basic / Bearer / custom scheme) the
    // subprocess might have printed. The match is scoped to the remainder of
    // the line so surrounding log context is preserved.
    let cleaned = AUTH_HEADER_RE
        .replace_all(&cleaned, "${1}<redacted>")
        .into_owned();

    // Redact userinfo in any URL the subprocess may have echoed.
    let mut out = String::with_capacity(cleaned.len());
    let mut rest = cleaned.as_str();
    while let Some(scheme_idx) = rest.find("://") {
        let after_scheme = scheme_idx + 3;
        // Look up to the first `/`, `?`, or whitespace to bound the authority.
        let authority_end = rest[after_scheme..]
            .find(['/', '?', ' ', '\t', '\n'])
            .map_or(rest.len(), |i| after_scheme + i);
        let authority = &rest[after_scheme..authority_end];
        if let Some(at_idx) = authority.rfind('@') {
            out.push_str(&rest[..after_scheme]);
            out.push_str("<redacted>@");
            out.push_str(&authority[at_idx + 1..]);
            rest = &rest[authority_end..];
        } else {
            out.push_str(&rest[..authority_end]);
            rest = &rest[authority_end..];
        }
    }
    out.push_str(rest);
    out
}

/// Environment variables that can cause `git` / `curl` / Git credential
/// helpers to emit verbose trace output containing headers and credentials.
/// We explicitly clear them on any subprocess we launch so sensitive
/// material isn't echoed into our captured stderr.
const GIT_TRACE_ENV_VARS: &[&str] = &[
    "GIT_TRACE",
    "GIT_TRACE_CURL",
    "GIT_TRACE_CURL_NO_DATA",
    "GIT_TRACE_PACKET",
    "GIT_TRACE_PACK_ACCESS",
    "GIT_TRACE_PERFORMANCE",
    "GIT_TRACE_SETUP",
    "GIT_TRACE_SHALLOW",
    "GIT_CURL_VERBOSE",
    "GCM_TRACE",
];

/// Ephemeral on-disk `GIT_ASKPASS`/`SSH_ASKPASS` script that echoes the
/// connector's configured HTTP(S) credentials when git prompts for them.
/// Command-line arguments for processes are observable via `ps` on most
/// hosts, so we avoid embedding credentials in argv via `-c http.extraHeader`
/// and go through ASKPASS instead. The backing temp dir is 0o700 and is
/// removed automatically when this value drops.
struct TempAskpass {
    _dir: tempfile::TempDir,
    script: PathBuf,
}

impl TempAskpass {
    fn new(credentials: &GitCredentials) -> Result<Option<Self>> {
        let Some((user, pass)) = basic_auth_parts(credentials) else {
            return Ok(None);
        };
        let dir = tempfile::Builder::new()
            .prefix("spice-git-askpass-")
            .tempdir()
            .context(IoSnafu)?;
        let script = dir.path().join("askpass.sh");
        let contents = format!(
            "#!/bin/sh\ncase \"$1\" in\n  *[Pp]assword*) printf '%s' {pass} ;;\n  *[Uu]sername*) printf '%s' {user} ;;\nesac\n",
            pass = sh_single_quote(&pass),
            user = sh_single_quote(&user),
        );
        std::fs::write(&script, contents).context(IoSnafu)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700))
                .context(IoSnafu)?;
        }
        Ok(Some(Self { _dir: dir, script }))
    }

    fn path(&self) -> &Path {
        &self.script
    }
}

fn basic_auth_parts(credentials: &GitCredentials) -> Option<(String, String)> {
    if let Some(token) = credentials.token.as_deref() {
        let user = credentials.username.as_deref().unwrap_or("x-access-token");
        return Some((user.to_string(), token.to_string()));
    }
    credentials.password.as_deref().map(|pass| {
        let user = credentials.username.as_deref().unwrap_or("git");
        (user.to_string(), pass.to_string())
    })
}

fn sh_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn run_git_lfs_command(
    cache_path: &Path,
    args: &[&str],
    label: &str,
    credentials: &GitCredentials,
) -> Result<()> {
    let mut cmd = Command::new("git");
    cmd.current_dir(cache_path);

    // Force `git` and all child processes (including `git lfs`) to operate
    // non-interactively and to authenticate with the connector's configured
    // credentials rather than with whatever happens to be on the host.
    cmd.env("GIT_TERMINAL_PROMPT", "0");

    // Clear any ambient tracing that would echo request headers or credential
    // helper input/output into stderr.
    for key in GIT_TRACE_ENV_VARS {
        cmd.env_remove(key);
    }

    // HTTP(S): route credential prompts through a temporary ASKPASS script
    // instead of embedding the secret in argv via `-c http.extraHeader=...`,
    // which would leak the credential to anyone able to enumerate processes
    // on the host.
    let askpass = TempAskpass::new(credentials)?;
    if let Some(ref a) = askpass {
        cmd.env("GIT_ASKPASS", a.path());
        cmd.env("SSH_ASKPASS", a.path());
    }

    // SSH: point `ssh` at the configured private key. The passphrase (if
    // any) is expected to be cached by ssh-agent; we do not persist it to
    // disk.
    if let Some(ref key_path) = credentials.ssh_key_path {
        let ssh_cmd = format!(
            "ssh -i {} -o IdentitiesOnly=yes -o BatchMode=yes",
            sh_single_quote(&key_path.to_string_lossy())
        );
        cmd.env("GIT_SSH_COMMAND", ssh_cmd);
    } else if !credentials.ssh_use_agent {
        // Reject agent-based auth if the operator explicitly disabled it.
        cmd.env(
            "GIT_SSH_COMMAND",
            "ssh -o IdentitiesOnly=yes -o BatchMode=yes",
        );
    }

    cmd.arg("lfs").args(args);

    let output = cmd.output().context(IoSnafu)?;
    drop(askpass); // release the askpass temp dir before we return

    if output.status.success() {
        Ok(())
    } else {
        let raw_stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let message = if raw_stderr.is_empty() {
            format!("exit code {:?}", output.status.code())
        } else {
            sanitize_subprocess_output(&raw_stderr, credentials)
        };
        Err(Error::GitLfsFailed {
            operation: label.to_string(),
            message,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct TestRateLimiter;

    #[async_trait]
    impl RateLimiter for TestRateLimiter {
        async fn update_from_headers(&self, _headers: &reqwest::header::HeaderMap) {}
        async fn check_rate_limit(
            &self,
        ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Ok(())
        }
    }

    fn test_client(url: &str) -> Result<GitClient> {
        let rate_limiter: Arc<dyn RateLimiter> = Arc::new(TestRateLimiter);
        GitClient::new(
            url,
            None,
            rate_limiter,
            Some(std::env::temp_dir().join("spice_git_test_cache")),
            DEFAULT_MAX_FILE_BYTES,
            GitCredentials::default(),
            false,
            GitResilienceConfig::default(),
        )
    }

    #[test]
    fn validate_https_repo_url() {
        GitClient::validate_repo_url("https://github.com/spiceai/spiceai.git")
            .expect("valid https url");
    }

    #[test]
    fn validate_git_ssh_repo_url() {
        GitClient::validate_repo_url("git@github.com:spiceai/spiceai.git").expect("valid ssh url");
    }

    #[test]
    fn validate_file_scheme_repo_url() {
        GitClient::validate_repo_url("file:///tmp/spiceai-repo").expect("valid file url");
    }

    #[test]
    fn reject_unknown_scheme_repo_url() {
        let err = GitClient::validate_repo_url("ftp://github.com/spiceai/spiceai.git")
            .expect_err("should fail");
        match err {
            Error::InvalidConfiguration { message } => {
                assert!(message.contains("Unsupported Git URL scheme"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn backoff_method_parse() {
        assert_eq!(
            BackoffMethod::parse("exponential").expect("valid"),
            BackoffMethod::Exponential
        );
        assert_eq!(
            BackoffMethod::parse("FIBONACCI").expect("valid"),
            BackoffMethod::Fibonacci
        );
        let Err(_) = BackoffMethod::parse("bogus") else {
            panic!("bogus backoff should error")
        };
    }

    #[test]
    fn exponential_backoff_is_bounded() {
        let client = test_client("https://github.com/spiceai/spiceai.git").expect("valid client");
        let long_delay = client.backoff_delay(50);
        assert!(long_delay <= RETRY_MAX_BACKOFF);
    }

    #[test]
    fn credentials_empty_when_unset() {
        let creds = GitCredentials::default();
        assert!(creds.is_empty());
    }

    #[test]
    fn resolve_credentials_rejects_userpass_without_creds() {
        let creds = GitCredentials::default();
        let result =
            GitClient::resolve_credentials(&creds, None, CredentialType::USER_PASS_PLAINTEXT);
        let Err(err) = result else {
            panic!("expected error when no credentials are configured");
        };
        assert!(
            err.message().to_ascii_lowercase().contains("credentials"),
            "unexpected error message: {}",
            err.message()
        );
        // Missing creds must be classified as auth so the retry loop treats
        // them as permanent, not transient.
        assert_eq!(err.code(), git2::ErrorCode::Auth);
    }

    #[test]
    fn sanitize_repo_url_strips_userinfo() {
        assert_eq!(
            sanitize_repo_url("https://user:token@github.com/owner/repo.git"),
            "https://github.com/owner/repo.git"
        );
        assert_eq!(
            sanitize_repo_url("https://github.com/owner/repo.git"),
            "https://github.com/owner/repo.git"
        );
        // Non-URL shorthand (SSH `git@host:path`) passes through unchanged.
        assert_eq!(
            sanitize_repo_url("git@github.com:owner/repo.git"),
            "git@github.com:owner/repo.git"
        );
    }

    #[test]
    fn resolve_credentials_token_is_used_for_userpass() {
        let creds = GitCredentials {
            token: Some("ghp_token".to_string()),
            ..Default::default()
        };
        let result =
            GitClient::resolve_credentials(&creds, None, CredentialType::USER_PASS_PLAINTEXT);
        assert!(result.is_ok(), "should produce userpass cred");
    }

    #[test]
    fn resolve_credentials_ssh_agent_only_when_enabled() {
        // With ssh_use_agent = false and no ssh_key_path, SSH_KEY should fall
        // through — not auto-use the agent based on URL shape.
        let creds = GitCredentials::default();
        let fallthrough =
            GitClient::resolve_credentials(&creds, Some("git"), CredentialType::SSH_KEY);
        // The code tries SSH_KEY first, has nothing to return, and falls into
        // the USERNAME/USER_PASS/DEFAULT branches which are not allowed here,
        // producing an "unsupported credential type" error.
        assert!(fallthrough.is_err(), "expected error when agent disabled");
    }
}
