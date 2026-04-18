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

use crate::component::ComponentType;
use crate::component::dataset::Dataset;
use crate::component::metrics::{MetricSpec, MetricType, MetricsProvider, ObserveMetricCallback};
use crate::dataconnector::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    DataConnectorResult, ParameterSpec, Parameters,
};
use async_trait::async_trait;
use data_components::git::{
    BackoffMethod, DEFAULT_MAX_CONCURRENT_REQUESTS, DEFAULT_MAX_FILE_BYTES, DEFAULT_MAX_FILES,
    DEFAULT_MAX_RETRIES, GitCredentials, GitResilienceConfig, GitTableConfig, GitTableProvider,
};
use data_components::rate_limit::RateLimiter;
use datafusion::datasource::TableProvider;
use globset::{Glob, GlobSet, GlobSetBuilder};
use opentelemetry::KeyValue;
use secrecy::ExposeSecret;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use std::{any::Any, future::Future, pin::Pin, sync::Arc};
use tokio::sync::Semaphore;

/// A concurrency semaphore paired with the numeric limit it was constructed
/// with, so that mismatches between datasets sharing the same repository URL
/// can be detected and surfaced as a warning.
type SemaphoreEntry = (Arc<Semaphore>, usize);

/// Per-repository concurrency semaphores. Keyed by the fully-qualified
/// repository URL so that multiple datasets sharing the same remote share a
/// single concurrency budget.
///
/// Entries are never evicted during the runtime's lifetime: each slot holds an
/// `Arc<Semaphore>` + `usize` (~40 bytes on 64-bit platforms), and typical
/// deployments configure a bounded set of repositories. Workloads that
/// dynamically materialize many distinct Git URLs should treat this as a
/// known upper bound on memory use.
static GIT_CONCURRENCY_LIMITS: LazyLock<Mutex<HashMap<String, SemaphoreEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Per-repository disabled-state flags. Shared across all `Git` connector
/// instances that target the same repository so that a permanent error
/// observed by one dataset latches the connector for every dataset pointing
/// at the same remote. Same memory footprint trade-off as
/// `GIT_CONCURRENCY_LIMITS` above.
static GIT_DISABLED_FLAGS: LazyLock<Mutex<HashMap<String, Arc<AtomicBool>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Resolve (or create) the shared semaphore for a repository URL.
///
/// When multiple datasets target the same URL with different
/// `max_concurrent_requests` values, the first configuration wins (to keep
/// the concurrency budget deterministic) and subsequent mismatches are
/// logged so the operator can reconcile the configuration.
fn shared_semaphore(key: &str, max_concurrent: usize) -> Arc<Semaphore> {
    let mut guard = GIT_CONCURRENCY_LIMITS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some((semaphore, existing_max)) = guard.get(key) {
        if *existing_max != max_concurrent {
            tracing::warn!(
                repo_url = %key,
                existing_max,
                requested_max = max_concurrent,
                "Multiple datasets target the same Git repository with different max_concurrent_requests values. Keeping the first-seen limit ({existing_max}). Reconcile the configuration for consistent behavior."
            );
        }
        Arc::<Semaphore>::clone(semaphore)
    } else {
        let semaphore = Arc::new(Semaphore::new(max_concurrent));
        guard.insert(
            key.to_string(),
            (Arc::<Semaphore>::clone(&semaphore), max_concurrent),
        );
        semaphore
    }
}

fn shared_disabled_flag(key: &str) -> Arc<AtomicBool> {
    let mut guard = GIT_DISABLED_FLAGS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Arc::<AtomicBool>::clone(
        guard
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(AtomicBool::new(false))),
    )
}

#[derive(Debug)]
pub struct Git {
    params: Parameters,
    /// Shared counters updated by every `GitClient` instance created by this
    /// connector. Used by the metrics provider to expose observability.
    metrics: Arc<GitMetrics>,
}

impl Git {
    #[must_use]
    pub fn new(params: Parameters) -> Self {
        Self {
            params,
            metrics: Arc::new(GitMetrics::default()),
        }
    }

    /// Parse the Git URL from the dataset path
    /// Supports formats like:
    /// - git:https://github.com/spiceai/spiceai.git
    /// - git:https://user:token@github.com/spiceai/spiceai.git@trunk
    /// - git:git@github.com:spiceai/spiceai.git@v1.0.0
    /// - git:file:///tmp/repo@main
    fn parse_git_url(path: &str) -> Result<(String, Option<String>), String> {
        let path = path.strip_prefix("git:").unwrap_or(path).trim();
        if path.is_empty() {
            return Err("Git path is empty".to_string());
        }

        // SSH shorthand: git@host:org/repo[@ref]
        if path.starts_with("git@") {
            if let Some(colon_pos) = path.find(':') {
                let suffix = &path[colon_pos + 1..];
                if let Some(at_rel) = suffix.rfind('@') {
                    let at_pos = colon_pos + 1 + at_rel;
                    let url = path[..at_pos].to_string();
                    let reference = path[at_pos + 1..].to_string();
                    return Ok((url, Some(reference)));
                }
            }
            return Ok((path.to_string(), None));
        }

        // Standard URL: `<scheme>://[userinfo@]authority[/path][@<ref>]`.
        //
        // The reference separator is the last `@` that appears *after* the
        // start of the path component (the first `/` following `://`). This
        // way a URL that contains userinfo (e.g. `https://user:token@host/`)
        // is never mistaken for a `@ref` suffix.
        if let Some(scheme_end) = path.find("://") {
            let after_scheme = scheme_end + 3;
            let path_start = path[after_scheme..]
                .find('/')
                .map_or(path.len(), |idx| after_scheme + idx);
            if let Some(at_rel) = path[path_start..].rfind('@') {
                let at_pos = path_start + at_rel;
                let url = path[..at_pos].to_string();
                let reference = path[at_pos + 1..].to_string();
                return Ok((url, Some(reference)));
            }
            return Ok((path.to_string(), None));
        }

        // No scheme (bare path, etc.): fall back to treating the last `@`
        // as the ref separator.
        if let Some(at_pos) = path.rfind('@') {
            let url = path[..at_pos].to_string();
            let reference = path[at_pos + 1..].to_string();
            Ok((url, Some(reference)))
        } else {
            Ok((path.to_string(), None))
        }
    }

    fn build_credentials(&self) -> GitCredentials {
        // Credentials and tuning values are sourced exclusively from the
        // validated, secret-resolved connector parameters. Using
        // `self.params` guarantees:
        //
        // * secret-store references such as `${ env:GIT_TOKEN }` are
        //   resolved into their real values, and
        // * prefixed/unprefixed keys are normalized by
        //   `ConnectorParamsBuilder`.
        let username = self
            .params
            .get("username")
            .expose()
            .ok()
            .map(ToString::to_string);

        let password = self
            .params
            .get("password")
            .ok()
            .map(|s| s.expose_secret().to_string());

        let token = self
            .params
            .get("token")
            .ok()
            .map(|s| s.expose_secret().to_string());

        let ssh_key_path = self.params.get("ssh_key").expose().ok().map(PathBuf::from);

        let ssh_passphrase = self
            .params
            .get("ssh_passphrase")
            .ok()
            .map(|s| s.expose_secret().to_string());

        let ssh_use_agent = self
            .params
            .get("ssh_use_agent")
            .expose()
            .ok()
            .and_then(|v| v.parse::<bool>().ok())
            .unwrap_or(true);

        GitCredentials {
            username,
            password,
            token,
            ssh_key_path,
            ssh_passphrase,
            ssh_use_agent,
        }
    }

    fn build_resilience(&self, repo_url: &str) -> GitResilienceConfig {
        let max_concurrent_requests = self
            .params
            .get("max_concurrent_requests")
            .expose()
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAX_CONCURRENT_REQUESTS)
            .max(1);

        let max_retries = self
            .params
            .get("git_max_retries")
            .expose()
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(DEFAULT_MAX_RETRIES);

        let backoff_value = self
            .params
            .get("backoff_method")
            .expose()
            .ok()
            .unwrap_or("exponential");
        let backoff = BackoffMethod::parse(backoff_value).unwrap_or_else(|message| {
            tracing::warn!("{message}; falling back to 'exponential'.");
            BackoffMethod::Exponential
        });

        let disable_on_permanent_error = self
            .params
            .get("disable_on_permanent_error")
            .expose()
            .ok()
            .and_then(|v| v.parse::<bool>().ok())
            .unwrap_or(true);

        // Use a sanitized URL as the map key so inline credentials (e.g.
        // `https://user:token@host/repo`) do not end up as a long-lived map
        // key in the global resilience tables.
        let key = data_components::git::sanitize_repo_url(repo_url);
        let semaphore = shared_semaphore(&key, max_concurrent_requests);
        let disabled = shared_disabled_flag(&key);

        GitResilienceConfig {
            max_retries,
            backoff,
            semaphore: Some(semaphore),
            disable_on_permanent_error,
            inflight: Arc::clone(&self.metrics.inflight_operations),
            disabled,
        }
    }

    /// Create a `GitTableProvider` from dataset configuration
    async fn create_table_provider(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        let path = dataset.path();
        let component = ConnectorComponent::from(dataset);

        let (repo_url, reference) =
            Self::parse_git_url(path).map_err(|e| DataConnectorError::UnableToGetReadProvider {
                dataconnector: "git".to_string(),
                connector_component: component.clone(),
                source: format!("Invalid Git URL: {e}").into(),
            })?;

        tracing::debug!(
            "Connecting to Git repository: {} (reference: {:?})",
            data_components::git::sanitize_repo_url(&repo_url),
            reference
        );

        // All runtime parameters are sourced from the validated, normalized
        // `self.params` (produced by `ConnectorParamsBuilder`) so that
        // secret-store references are resolved and prefix/validation rules
        // are honored. Avoid reading these from `dataset.params` directly
        // because that bypasses validation and would silently accept keys
        // that are rejected by the parameter spec.
        let include = self
            .params
            .get("include")
            .expose()
            .ok()
            .map(|patterns| {
                parse_globs(&component, patterns).map_err(|e| {
                    DataConnectorError::UnableToGetReadProvider {
                        dataconnector: "git".to_string(),
                        connector_component: component.clone(),
                        source: format!("Failed to parse include patterns: {e}").into(),
                    }
                })
            })
            .transpose()?;

        let fetch_content = self
            .params
            .get("fetch_content")
            .expose()
            .ok()
            .and_then(|v| v.parse::<bool>().ok())
            .unwrap_or(false);

        let cache_path = self
            .params
            .get("cache_path")
            .expose()
            .ok()
            .map(PathBuf::from);

        let max_files = self
            .params
            .get("max_files")
            .expose()
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAX_FILES);

        let max_file_bytes = self
            .params
            .get("max_file_bytes")
            .expose()
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAX_FILE_BYTES);

        let enable_lfs = self
            .params
            .get("enable_lfs")
            .expose()
            .ok()
            .and_then(|v| v.parse::<bool>().ok())
            .unwrap_or(false);

        // Create a no-op rate limiter (Git operations are local after initial clone)
        let rate_limiter: Arc<dyn RateLimiter> = Arc::new(NoOpRateLimiter);

        let credentials = self.build_credentials();
        let resilience = self.build_resilience(&repo_url);

        let config = GitTableConfig {
            fetch_content,
            rate_limiter,
            cache_path,
            max_files,
            max_file_bytes,
            credentials,
            enable_lfs,
            resilience,
        };

        let table_provider =
            GitTableProvider::new(&repo_url, reference.as_deref(), include, config)
                .await
                .map_err(|e| DataConnectorError::UnableToGetReadProvider {
                    dataconnector: "git".to_string(),
                    connector_component: component,
                    source: Box::new(e),
                })?;

        Ok(Arc::new(table_provider))
    }
}

#[async_trait]
impl DataConnector for Git {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        self.create_table_provider(dataset).await
    }

    fn metrics_provider(&self) -> Option<Arc<dyn MetricsProvider>> {
        Some(Arc::new(GitMetricsProvider {
            metrics: Arc::clone(&self.metrics),
        }))
    }
}

#[derive(Default, Debug, Copy, Clone)]
pub struct GitFactory {}

impl GitFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    // File selection / limits
    ParameterSpec::runtime("include")
        .description("Include only files matching the glob pattern. Multiple patterns can be separated by comma or semicolon.")
        .examples(&["*.rs", "**/*.yaml;src/**/*.json"]),
    ParameterSpec::runtime("fetch_content")
        .description("Whether to fetch file content. Set to 'true' to include file content in the 'content' column.")
        .default("false")
        .is_boolean(),
    ParameterSpec::runtime("cache_path")
        .description("Custom path for the local Git repository cache. If not specified, uses system temp directory."),
    ParameterSpec::runtime("max_files")
        .description("Maximum number of files to materialize from a Git repository. Default: 5000. Hard limit: 50000.")
        .default("5000"),
    ParameterSpec::runtime("max_file_bytes")
        .description("Maximum size (bytes) for an individual file when fetching content. Files larger than this value are skipped. Default: 524288. Maximum: 5242880 (5 MiB)."),

    // Authentication (HTTPS / HTTP)
    ParameterSpec::component("username")
        .description("Username for HTTP(S) basic authentication."),
    ParameterSpec::component("password")
        .description("Password or personal access token for HTTP(S) basic authentication.")
        .secret(),
    ParameterSpec::component("token")
        .description("Personal access token used for HTTP(S) authentication. Equivalent to providing a username of 'x-access-token' with the token as the password.")
        .secret(),

    // Authentication (SSH)
    ParameterSpec::component("ssh_key")
        .description("Absolute path to an SSH private key used to authenticate to the remote repository."),
    ParameterSpec::component("ssh_passphrase")
        .description("Passphrase for the SSH private key identified by 'ssh_key'.")
        .secret(),
    ParameterSpec::component("ssh_use_agent")
        .description("When 'true', attempt to authenticate via the running ssh-agent when no explicit ssh_key is provided. Defaults to 'true'.")
        .default("true")
        .is_boolean(),

    // Large File Storage
    ParameterSpec::runtime("enable_lfs")
        .description("Whether to fetch git-lfs objects after clone/fetch. Requires the 'git-lfs' CLI to be available on PATH.")
        .default("false")
        .is_boolean(),

    // Resilience / connection tuning
    ParameterSpec::runtime("max_concurrent_requests")
        .description("Maximum number of concurrent Git network operations (clone/fetch) across datasets that share the same repository URL.")
        .default("4"),
    ParameterSpec::runtime("git_max_retries")
        .description("Maximum number of retries when the connector encounters a transient error cloning or fetching from the remote.")
        .default("3"),
    ParameterSpec::runtime("backoff_method")
        .description("Backoff strategy for retries on transient errors.")
        .one_of(&["exponential", "fibonacci"])
        .default("exponential"),
    ParameterSpec::runtime("disable_on_permanent_error")
        .description("When true, a permanent error (authentication failure, access denied) will disable the connector to prevent a thundering herd of failed requests.")
        .default("true")
        .is_boolean(),

    // Backwards-compat aliases: earlier releases of the Git connector read
    // these runtime options from `dataset.params` using a `git_`-prefixed
    // key. Runtime parameters normally reject the prefix, but these
    // deprecated entries keep those existing spicepods working while they
    // migrate to the unprefixed forms above. Emit a deprecation warning so
    // the mis-prefixed keys surface in logs.
    ParameterSpec::component("include")
        .description("[deprecated] Use unprefixed 'include'.")
        .deprecated("Rename 'git_include' to 'include'."),
    ParameterSpec::component("fetch_content")
        .description("[deprecated] Use unprefixed 'fetch_content'.")
        .deprecated("Rename 'git_fetch_content' to 'fetch_content'."),
    ParameterSpec::component("cache_path")
        .description("[deprecated] Use unprefixed 'cache_path'.")
        .deprecated("Rename 'git_cache_path' to 'cache_path'."),
    ParameterSpec::component("max_files")
        .description("[deprecated] Use unprefixed 'max_files'.")
        .deprecated("Rename 'git_max_files' to 'max_files'."),
    ParameterSpec::component("max_file_bytes")
        .description("[deprecated] Use unprefixed 'max_file_bytes'.")
        .deprecated("Rename 'git_max_file_bytes' to 'max_file_bytes'."),
    ParameterSpec::component("enable_lfs")
        .description("[deprecated] Use unprefixed 'enable_lfs'.")
        .deprecated("Rename 'git_enable_lfs' to 'enable_lfs'."),
];

impl DataConnectorFactory for GitFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move { Ok(Arc::new(Git::new(params.parameters)) as Arc<dyn DataConnector>) })
    }

    fn prefix(&self) -> &'static str {
        "git"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

/// Metrics recorded by the Git data connector.
///
/// The counters are incremented by every `GitClient` created by this
/// connector so that dashboards reflect the combined behavior across all
/// datasets using the same connector instance.
#[derive(Debug, Default)]
struct GitMetrics {
    inflight_operations: Arc<AtomicU64>,
}

const GIT_METRICS: &[MetricSpec] = &[MetricSpec::new(
    "inflight_operations",
    MetricType::ObservableGaugeU64,
)
.description("Current number of Git network operations (clone/fetch) holding a concurrency permit")
.auto_register()];

#[derive(Debug, Clone)]
struct GitMetricsProvider {
    metrics: Arc<GitMetrics>,
}

impl MetricsProvider for GitMetricsProvider {
    fn component_type(&self) -> ComponentType {
        ComponentType::Dataset
    }

    fn component_name(&self) -> &'static str {
        "git"
    }

    fn available_metrics(&self) -> &'static [MetricSpec] {
        GIT_METRICS
    }

    fn callback_to_observe_metric(
        &self,
        metric: &MetricSpec,
        attributes: Vec<KeyValue>,
    ) -> Option<ObserveMetricCallback> {
        match metric.name {
            "inflight_operations" => {
                let metrics = Arc::clone(&self.metrics);
                Some(ObserveMetricCallback::U64(Box::new(move |observer| {
                    observer.observe(
                        metrics.inflight_operations.load(Ordering::Relaxed),
                        &attributes,
                    );
                })))
            }
            _ => None,
        }
    }
}

/// Parse glob patterns from a comma or semicolon separated string
pub fn parse_globs(
    component: &ConnectorComponent,
    input: &str,
) -> Result<Arc<GlobSet>, Box<dyn std::error::Error + Send + Sync>> {
    let patterns: Vec<&str> = input.split(&[',', ';'][..]).collect();
    let mut builder = GlobSetBuilder::new();

    for pattern in patterns {
        let trimmed_pattern = pattern.trim();
        if !trimmed_pattern.is_empty() {
            builder.add(Glob::new(trimmed_pattern).map_err(|e| {
                format!("Invalid glob pattern '{trimmed_pattern}' for {component}: {e}")
            })?);
        }
    }

    let glob_set = builder
        .build()
        .map_err(|e| format!("Failed to build glob set for {component}: {e}"))?;

    Ok(Arc::new(glob_set))
}

/// A no-op rate limiter for Git operations (local operations after clone)
#[derive(Debug)]
struct NoOpRateLimiter;

#[async_trait]
impl RateLimiter for NoOpRateLimiter {
    async fn update_from_headers(&self, _headers: &reqwest::header::HeaderMap) {
        // No rate limiting needed for local Git operations
    }

    async fn check_rate_limit(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

register_data_connector!("git", GitFactory);
