/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Git data connector for Spice.ai runtime.
//!
//! This crate provides the Git connector implementation, allowing
//! Spice.ai to connect to Git repositories as data sources.
//!
//! This connector is extracted from the runtime crate to enable faster
//! incremental builds - changes to this connector only require rebuilding
//! this crate, not the entire runtime.

use async_trait::async_trait;
use data_components::git::{
    BackoffMethod, DEFAULT_MAX_CONCURRENT_REQUESTS, DEFAULT_MAX_FILE_BYTES, DEFAULT_MAX_FILES,
    DEFAULT_MAX_RETRIES, GitCredentials, GitResilienceConfig, GitTableConfig, GitTableProvider,
};
use data_components::rate_limit::RateLimiter;
use datafusion::datasource::TableProvider;
use globset::{Glob, GlobSet, GlobSetBuilder};
use opentelemetry::KeyValue;
use runtime::component::ComponentType;
use runtime::component::dataset::Dataset;
use runtime::component::metrics::{MetricSpec, MetricType, MetricsProvider, ObserveMetricCallback};
use runtime::dataconnector::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    DataConnectorResult,
};
use runtime::parameters::{ParameterSpec, Parameters};
use secrecy::{ExposeSecret, SecretString};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::{any::Any, future::Future, pin::Pin};
use tokio::sync::Semaphore;

/// The name used to identify this connector in configuration.
pub const CONNECTOR_NAME: &str = "git";

/// Returns a new instance of the Git connector factory.
#[must_use]
pub fn factory() -> Arc<dyn DataConnectorFactory> {
    GitFactory::new_arc()
}

/// A concurrency semaphore paired with the numeric limit it was constructed
/// with, so that mismatches between datasets sharing the same repository URL
/// can be detected and surfaced as a warning.
type SemaphoreEntry = (Arc<Semaphore>, usize);

static GIT_CONCURRENCY_LIMITS: LazyLock<Mutex<HashMap<String, SemaphoreEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

static GIT_DISABLED_FLAGS: LazyLock<Mutex<HashMap<String, Arc<AtomicBool>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

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

    fn parse_git_url(path: &str) -> Result<(String, Option<String>), String> {
        let path = path.strip_prefix("git:").unwrap_or(path).trim();
        if path.is_empty() {
            return Err("Git path is empty".to_string());
        }

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

        if let Some(at_pos) = path.rfind('@') {
            let url = path[..at_pos].to_string();
            let reference = path[at_pos + 1..].to_string();
            Ok((url, Some(reference)))
        } else {
            Ok((path.to_string(), None))
        }
    }

    fn build_credentials(&self) -> GitCredentials {
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
            .map(|s: &SecretString| s.expose_secret().to_string());

        let token = self
            .params
            .get("token")
            .ok()
            .map(|s: &SecretString| s.expose_secret().to_string());

        let ssh_key_path = self.params.get("ssh_key").expose().ok().map(PathBuf::from);

        let ssh_passphrase = self
            .params
            .get("ssh_passphrase")
            .ok()
            .map(|s: &SecretString| s.expose_secret().to_string());

        let ssh_use_agent = self
            .params
            .get("ssh_use_agent")
            .expose()
            .ok()
            .and_then(|v: &str| v.parse::<bool>().ok())
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
            .and_then(|v: &str| v.parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAX_CONCURRENT_REQUESTS)
            .max(1);

        let max_retries = self
            .params
            .get("git_max_retries")
            .expose()
            .ok()
            .and_then(|v: &str| v.parse::<u32>().ok())
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
            .and_then(|v: &str| v.parse::<bool>().ok())
            .unwrap_or(true);

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
            .and_then(|v: &str| v.parse::<bool>().ok())
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
            .and_then(|v: &str| v.parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAX_FILES);

        let max_file_bytes = self
            .params
            .get("max_file_bytes")
            .expose()
            .ok()
            .and_then(|v: &str| v.parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAX_FILE_BYTES);

        let enable_lfs = self
            .params
            .get("enable_lfs")
            .expose()
            .ok()
            .and_then(|v: &str| v.parse::<bool>().ok())
            .unwrap_or(false);

        let rate_limiter = Arc::new(NoOpRateLimiter) as Arc<dyn RateLimiter>;

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
    ParameterSpec::component("username")
        .description("Username for HTTP(S) basic authentication."),
    ParameterSpec::component("password")
        .description("Password or personal access token for HTTP(S) basic authentication.")
        .secret(),
    ParameterSpec::component("token")
        .description("Personal access token used for HTTP(S) authentication. Equivalent to providing a username of 'x-access-token' with the token as the password.")
        .secret(),
    ParameterSpec::component("ssh_key")
        .description("Absolute path to an SSH private key used to authenticate to the remote repository."),
    ParameterSpec::component("ssh_passphrase")
        .description("Passphrase for the SSH private key identified by 'ssh_key'.")
        .secret(),
    ParameterSpec::component("ssh_use_agent")
        .description("When 'true', attempt to authenticate via the running ssh-agent when no explicit ssh_key is provided. Defaults to 'true'.")
        .default("true")
        .is_boolean(),
    ParameterSpec::runtime("enable_lfs")
        .description("Whether to fetch git-lfs objects after clone/fetch. Requires the 'git-lfs' CLI to be available on PATH.")
        .default("false")
        .is_boolean(),
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
    ) -> Pin<Box<dyn Future<Output = runtime::dataconnector::NewDataConnectorResult> + Send>> {
        Box::pin(async move { Ok(Arc::new(Git::new(params.parameters)) as Arc<dyn DataConnector>) })
    }

    fn prefix(&self) -> &'static str {
        "git"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

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

/// Parse glob patterns from a comma or semicolon separated string.
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

/// A no-op rate limiter for Git operations (local operations after clone).
#[derive(Debug)]
struct NoOpRateLimiter;

#[async_trait]
impl RateLimiter for NoOpRateLimiter {
    async fn update_from_headers(&self, _headers: &reqwest::header::HeaderMap) {}

    async fn check_rate_limit(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}
