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

//! HTTP rate control for data connectors.
//!
//! `runtime-rate-control` owns the rate-controller mechanism. This crate is the
//! connector-facing layer over it: the `rate_control_*` / `http_*` parameters a
//! dataset declares, the process-wide registry that gives every connector
//! pointed at the same origin one shared controller, and the metrics it reports.
//!
//! Every entry point takes a [`ConnectorComponent`] and a spicepod name rather
//! than a dataset handle, so a connector can reach rate control without naming
//! the runtime.

use std::collections::{HashMap, HashSet};
use std::hash::BuildHasher;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, RwLock as StdRwLock};
use std::time::Duration;

use data_components::rate_limit::{HttpRateLimiter, HttpRateLimiterMetrics};
use data_connector_types::{ConnectorComponent, DataConnectorError, DataConnectorResult};
use governor::Quota;
use object_store::ObjectStore;
use opentelemetry::KeyValue;
use runtime_api_types::v1::ComponentType;
use runtime_metrics::component::{MetricSpec, MetricType, MetricsProvider, ObserveMetricCallback};
use runtime_parameters::{ParameterSpec, Parameters};
pub use runtime_rate_control::AdaptiveRateControl;
use runtime_rate_control::{
    AdaptiveRateControlError, DEFAULT_ADAPTIVE_FAILURE_THRESHOLD, DEFAULT_ADAPTIVE_WINDOW,
    JitterConfig, RateController, RateControllerMetrics,
};
use tokio::sync::RwLock;
use url::Url;

const DEFAULT_RATE_CONTROL_JITTER_MIN: Duration = Duration::from_millis(5);
const DEFAULT_RATE_CONTROL_JITTER_MAX: Duration = Duration::from_millis(10);
const RUNTIME_MAX_CONCURRENT_REQUESTS: &str = "http_max_concurrent_requests";
const RUNTIME_REQUESTS_PER_SECOND_LIMIT: &str = "http_requests_per_second_limit";
const RUNTIME_REQUESTS_PER_MINUTE_LIMIT: &str = "http_requests_per_minute_limit";
const RUNTIME_RATE_CONTROL_JITTER_MIN: &str = "http_rate_control_jitter_min";
const RUNTIME_RATE_CONTROL_JITTER_MAX: &str = "http_rate_control_jitter_max";
const RUNTIME_RATE_CONTROL_MODE: &str = "http_rate_control_mode";
const RUNTIME_RATE_CONTROL_FAILURE_THRESHOLD: &str = "http_rate_control_failure_threshold";
const RUNTIME_RATE_CONTROL_WINDOW: &str = "http_rate_control_window";
const RUNTIME_RATE_CONTROL_ACQUIRE_TIMEOUT: &str = "http_rate_control_acquire_timeout";

/// Every `http_*` rate-control key this module reads from `runtime.params`.
/// Exposed as the authoritative list for this family; the startup unknown-param
/// check merges it into the full `runtime.params` vocabulary
/// (`known_runtime_params`) used to recognize keys and scope "did you mean"
/// suggestions across the whole section.
pub const HTTP_RATE_CONTROL_RUNTIME_PARAMS: &[&str] = &[
    RUNTIME_MAX_CONCURRENT_REQUESTS,
    RUNTIME_REQUESTS_PER_SECOND_LIMIT,
    RUNTIME_REQUESTS_PER_MINUTE_LIMIT,
    RUNTIME_RATE_CONTROL_JITTER_MIN,
    RUNTIME_RATE_CONTROL_JITTER_MAX,
    RUNTIME_RATE_CONTROL_MODE,
    RUNTIME_RATE_CONTROL_FAILURE_THRESHOLD,
    RUNTIME_RATE_CONTROL_WINDOW,
    RUNTIME_RATE_CONTROL_ACQUIRE_TIMEOUT,
];
const MIN_PERSISTED_INSTANCE_TTL: Duration = Duration::from_secs(5);

// Fallback for direct connector construction without a Runtime. Factory-created
// connectors use Runtime's per-instance registry so reloads/tests do not reuse
// stale origin state.
static GLOBAL_HTTP_RATE_CONTROL_REGISTRY: LazyLock<Arc<HttpRateControlRegistry>> =
    LazyLock::new(|| Arc::new(HttpRateControlRegistry::default()));

pub struct HttpRateControlRegistry {
    rate_limiters: RwLock<HashMap<String, Arc<HttpRateLimiter>>>,
    rate_controllers: RwLock<HashMap<String, SharedRateControllerEntry>>,
    metrics_by_origin: StdRwLock<HashMap<String, Arc<HttpRateControlMetrics>>>,
    metric_owners: StdRwLock<HashMap<String, String>>,
    persisted_governor_state: Option<HttpRateControlPersistedState>,
    persistence_task_started: AtomicBool,
}

impl std::fmt::Debug for HttpRateControlRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpRateControlRegistry")
            .field("persisted_governor_state", &self.persisted_governor_state)
            .field(
                "persistence_task_started",
                &self.persistence_task_started.load(Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
}

impl Default for HttpRateControlRegistry {
    fn default() -> Self {
        Self {
            rate_limiters: RwLock::new(HashMap::new()),
            rate_controllers: RwLock::new(HashMap::new()),
            metrics_by_origin: StdRwLock::new(HashMap::new()),
            metric_owners: StdRwLock::new(HashMap::new()),
            persisted_governor_state: None,
            persistence_task_started: AtomicBool::new(false),
        }
    }
}

#[derive(Clone)]
struct HttpRateControlPersistedState {
    store: Arc<dyn ObjectStore>,
    base_prefix: String,
    refresh_interval: Duration,
    instance_id: String,
    instance_ttl: Duration,
}

impl std::fmt::Debug for HttpRateControlPersistedState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpRateControlPersistedState")
            .field("base_prefix", &self.base_prefix)
            .field("refresh_interval", &self.refresh_interval)
            .field("instance_id", &self.instance_id)
            .field("instance_ttl", &self.instance_ttl)
            .finish_non_exhaustive()
    }
}

#[must_use]
pub fn global_registry() -> Arc<HttpRateControlRegistry> {
    Arc::clone(&GLOBAL_HTTP_RATE_CONTROL_REGISTRY)
}

#[derive(Clone, Debug, PartialEq)]
pub struct HttpRateControlConfig {
    pub max_concurrent_requests: Option<usize>,
    pub requests_per_second: Option<NonZeroU32>,
    pub requests_per_minute: Option<NonZeroU32>,
    pub jitter_min: Duration,
    pub jitter_max: Duration,
    /// How the configured limits are applied for this origin.
    pub mode: RateControlMode,
    /// Upper bound on how long a request waits to acquire rate-control capacity
    /// before failing instead of waiting indefinitely. `None` = wait
    /// indefinitely. `Some(ZERO)` is normalized to `None` at build time.
    pub acquire_timeout: Option<Duration>,
}

/// How an origin applies its configured rate limits.
///
/// An enum rather than a switch, so a future mode can join `static` and
/// `adaptive`. The adaptive tuning lives in the `Adaptive` variant, because the
/// failure threshold and the window have no meaning in any other mode.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RateControlMode {
    /// Apply the configured limits as they are.
    Static,
    /// Scale the configured limits down while the origin fails, and back up as
    /// the origin recovers.
    Adaptive(AdaptiveRateControl),
}

impl RateControlMode {
    /// The adaptive tuning, or `None` in any non-adaptive mode.
    #[must_use]
    pub fn adaptive(&self) -> Option<AdaptiveRateControl> {
        match self {
            Self::Static => None,
            Self::Adaptive(control) => Some(*control),
        }
    }
}

/// The value of the `rate_control_mode` parameter, before the mode's own tuning
/// parameters are read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RateControlModeName {
    Static,
    Adaptive,
}

impl RateControlModeName {
    /// Parse a `rate_control_mode` value.
    ///
    /// * absent / empty / `static` -> [`RateControlModeName::Static`]
    /// * `adaptive` -> [`RateControlModeName::Adaptive`]
    /// * anything else -> `None` (the caller reports the offending value)
    fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "static" => Some(Self::Static),
            "adaptive" => Some(Self::Adaptive),
            _ => None,
        }
    }
}

impl HttpRateControlConfig {
    /// A config with no rate control of any kind — no static limits, no jitter,
    /// no adaptive control. The state a limiter starts in before any parameter
    /// is applied.
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            max_concurrent_requests: None,
            requests_per_second: None,
            requests_per_minute: None,
            jitter_min: Duration::ZERO,
            jitter_max: Duration::ZERO,
            mode: RateControlMode::Static,
            acquire_timeout: None,
        }
    }

    /// Fill the acquire-timeout bound from the connector's request timeouts when
    /// the user set no explicit `rate_control_acquire_timeout`. A parsed explicit
    /// `0` (`Some(ZERO)`) is left untouched so it still disables the bound.
    pub fn apply_default_acquire_timeout(&mut self, client_timeout: Duration) {
        if self.acquire_timeout.is_none() {
            self.acquire_timeout = Some(default_acquire_timeout(client_timeout));
        }
    }

    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.max_concurrent_requests.is_some()
            || self.requests_per_second.is_some()
            || self.requests_per_minute.is_some()
            || !self.jitter_min.is_zero()
            || !self.jitter_max.is_zero()
    }

    /// Whether the origin defines any static rate limit for adaptive control to
    /// scale. Adaptive control is a modifier on a defined limit, so this being
    /// `false` while adaptive control is enabled is a configuration error (see
    /// [`ensure_adaptive_has_static_limit`]).
    #[must_use]
    pub fn has_static_limit(&self) -> bool {
        self.max_concurrent_requests.is_some()
            || self.requests_per_second.is_some()
            || self.requests_per_minute.is_some()
    }

    /// Whether this origin runs in adaptive mode.
    #[must_use]
    pub fn adaptive_enabled(&self) -> bool {
        self.mode.adaptive().is_some()
    }
}

#[derive(Clone, Debug)]
pub struct SharedRateController {
    pub config: HttpRateControlConfig,
    pub controller: Option<Arc<RateController>>,
}

#[derive(Clone, Debug)]
struct SharedRateControllerEntry {
    shared: SharedRateController,
    pending_registrations: usize,
    active_registrations: usize,
}

#[derive(Debug)]
pub struct SharedRateControllerReservation {
    registry: Arc<HttpRateControlRegistry>,
    key: String,
    shared: SharedRateController,
}

impl SharedRateControllerReservation {
    #[must_use]
    pub fn shared(&self) -> &SharedRateController {
        &self.shared
    }

    pub async fn commit(self) -> SharedRateController {
        self.registry
            .commit_rate_controller_reservation(&self.key)
            .await;
        self.shared
    }

    pub async fn rollback(self) {
        self.registry
            .rollback_rate_controller_reservation(&self.key)
            .await;
    }
}

#[derive(Clone, Debug)]
pub struct HttpRateControlMetricSource {
    registry: Arc<HttpRateControlRegistry>,
    base_url: Url,
    owner: String,
}

impl HttpRateControlMetricSource {
    #[must_use]
    pub fn new(registry: Arc<HttpRateControlRegistry>, base_url: Url, owner: String) -> Self {
        Self {
            registry,
            base_url,
            owner,
        }
    }

    #[must_use]
    pub fn claim_owner(&self) -> bool {
        self.registry
            .claim_metrics_owner(&self.base_url, self.owner.as_str())
    }

    fn is_owner(&self) -> bool {
        self.registry
            .is_metrics_owner(&self.base_url, self.owner.as_str())
    }
}

#[derive(Debug, Default)]
pub struct HttpRateControlMetrics {
    rate_limiter_metrics: StdRwLock<Option<Arc<HttpRateLimiterMetrics>>>,
    rate_controller: StdRwLock<Option<Arc<RateController>>>,
    rate_controller_metrics: StdRwLock<Option<Arc<RateControllerMetrics>>>,
    max_concurrent_requests: AtomicU64,
    requests_per_second_limit: AtomicU64,
    requests_per_minute_limit: AtomicU64,
    rate_control_jitter_min_ms: AtomicU64,
    rate_control_jitter_max_ms: AtomicU64,
}

impl HttpRateControlMetrics {
    pub fn set_rate_limiter(&self, rate_limiter: &Arc<HttpRateLimiter>) {
        if let Ok(mut metrics) = self.rate_limiter_metrics.write() {
            *metrics = Some(rate_limiter.metrics());
        }
    }

    pub fn set_rate_controller(&self, rate_controller: Option<&Arc<RateController>>) {
        if let Ok(mut controller) = self.rate_controller.write() {
            *controller = rate_controller.map(Arc::clone);
        }

        if let Ok(mut metrics) = self.rate_controller_metrics.write() {
            *metrics = rate_controller.map(|controller| controller.metrics());
        }
    }

    pub fn set_config(&self, config: &HttpRateControlConfig) {
        self.max_concurrent_requests.store(
            config
                .max_concurrent_requests
                .map(usize_to_u64)
                .unwrap_or_default(),
            Ordering::Relaxed,
        );
        self.requests_per_second_limit.store(
            config
                .requests_per_second
                .map(|limit| u64::from(limit.get()))
                .unwrap_or_default(),
            Ordering::Relaxed,
        );
        self.requests_per_minute_limit.store(
            config
                .requests_per_minute
                .map(|limit| u64::from(limit.get()))
                .unwrap_or_default(),
            Ordering::Relaxed,
        );
        self.rate_control_jitter_min_ms
            .store(duration_millis_u64(config.jitter_min), Ordering::Relaxed);
        self.rate_control_jitter_max_ms
            .store(duration_millis_u64(config.jitter_max), Ordering::Relaxed);
    }

    #[must_use]
    pub fn max_concurrent_requests(&self) -> u64 {
        self.max_concurrent_requests.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn requests_per_second_limit(&self) -> u64 {
        self.requests_per_second_limit.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn requests_per_minute_limit(&self) -> u64 {
        self.requests_per_minute_limit.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn available_permits(&self) -> u64 {
        self.rate_controller
            .read()
            .ok()
            .and_then(|controller| {
                controller
                    .as_ref()
                    .and_then(|controller| controller.available_permits())
                    .map(usize_to_u64)
            })
            .unwrap_or_default()
    }

    /// The fraction of the configured limits currently admitted, in `[0, 1]`:
    /// `1` means "admit everything", lower means the origin is throttled. `None`
    /// in static mode. Read live from the controller, so it reflects window
    /// decay between scrapes.
    #[must_use]
    pub fn adaptive_admission_ratio(&self) -> Option<f64> {
        self.rate_controller
            .read()
            .ok()
            .and_then(|controller| {
                controller
                    .as_ref()
                    .and_then(|controller| controller.admission_coefficient())
            })
            .map(|coefficient| coefficient.clamp(0.0, 1.0))
    }

    /// Requests adaptive rate control has throttled, or `None` in static mode —
    /// a throttle that cannot happen reports no series rather than a `0` that
    /// reads as a healthy origin.
    #[must_use]
    pub fn adaptive_throttled_total(&self) -> Option<u64> {
        let adaptive_enabled = self
            .rate_controller
            .read()
            .ok()
            .and_then(|controller| {
                controller
                    .as_ref()
                    .map(|controller| controller.admission_coefficient().is_some())
            })
            .unwrap_or(false);
        adaptive_enabled
            .then(|| self.rate_controller_metric(RateControllerMetrics::adaptive_throttled_total))
    }

    fn rate_controller_metric(
        &self,
        observe_metric: impl FnOnce(&RateControllerMetrics) -> u64,
    ) -> u64 {
        self.rate_controller_metrics
            .read()
            .ok()
            .and_then(|metrics| metrics.as_ref().map(|metrics| observe_metric(metrics)))
            .unwrap_or_default()
    }

    fn rate_limiter_metric(
        &self,
        observe_metric: impl FnOnce(&HttpRateLimiterMetrics) -> u64,
    ) -> u64 {
        self.rate_limiter_metrics
            .read()
            .ok()
            .and_then(|metrics| metrics.as_ref().map(|metrics| observe_metric(metrics)))
            .unwrap_or_default()
    }
}

/// Every rate-control metric this crate can report.
///
/// Public so a connector that reports other metric families alongside these can
/// present one combined list: `available_metrics` is what answers whether a
/// metric a user asked for exists.
pub const HTTP_RATE_CONTROL_METRIC_SPECS: &[MetricSpec] = &[
    MetricSpec::new("inflight_operations", MetricType::ObservableGaugeU64)
        .description("Current number of HTTP requests holding a rate-control permit")
        .auto_register(),
    MetricSpec::new(
        "rate_control_max_concurrent_requests",
        MetricType::ObservableGaugeU64,
    )
    .description("Configured maximum concurrent HTTP requests for this upstream origin; 0 means disabled")
    .auto_register(),
    MetricSpec::new(
        "rate_control_requests_per_second_limit",
        MetricType::ObservableGaugeU64,
    )
    .description("Configured HTTP request-per-second limit for this upstream origin; 0 means disabled")
    .auto_register(),
    MetricSpec::new(
        "rate_control_requests_per_minute_limit",
        MetricType::ObservableGaugeU64,
    )
    .description("Configured HTTP request-per-minute limit for this upstream origin; 0 means disabled")
    .auto_register(),
    MetricSpec::new("rate_control_jitter_min_ms", MetricType::ObservableGaugeU64)
        .description("Configured minimum rate-control jitter before HTTP requests")
        .unit("ms")
        .auto_register(),
    MetricSpec::new("rate_control_jitter_max_ms", MetricType::ObservableGaugeU64)
        .description("Configured maximum rate-control jitter before HTTP requests")
        .unit("ms")
        .auto_register(),
    MetricSpec::new(
        "rate_control_available_permits",
        MetricType::ObservableGaugeU64,
    )
    .description("Current available permits in the HTTP request concurrency semaphore; 0 when concurrency limiting is disabled")
    .auto_register(),
    MetricSpec::new(
        "rate_control_acquisitions_total",
        MetricType::ObservableCounterU64,
    )
    .description("Total HTTP request rate-control permits acquired")
    .auto_register(),
    MetricSpec::new(
        "rate_control_acquire_errors_total",
        MetricType::ObservableCounterU64,
    )
    .description("Total HTTP request rate-control permit acquisition errors")
    .auto_register(),
    MetricSpec::new(
        "rate_control_wait_duration_ms",
        MetricType::ObservableCounterU64,
    )
    .description("Cumulative time spent waiting for HTTP rate-control permits, quotas, and jitter")
    .unit("ms")
    .auto_register(),
    MetricSpec::new(
        "rate_limit_retry_after_updates_total",
        MetricType::ObservableCounterU64,
    )
    .description("Total HTTP upstream cooldown hints accepted from Retry-After or RateLimit reset headers")
    .auto_register(),
    MetricSpec::new(
        "rate_limit_retry_after_waits_total",
        MetricType::ObservableCounterU64,
    )
    .description("Total waits caused by HTTP Retry-After or RateLimit reset headers")
    .auto_register(),
    MetricSpec::new(
        "rate_limit_retry_after_wait_duration_ms",
        MetricType::ObservableCounterU64,
    )
    .description("Cumulative time spent waiting because of HTTP Retry-After or RateLimit reset headers")
    .unit("ms")
    .auto_register(),
    MetricSpec::new(
        "rate_limit_retry_after_remaining_ms",
        MetricType::ObservableGaugeU64,
    )
    .description("Current remaining HTTP Retry-After or RateLimit reset cooldown for this upstream origin")
    .unit("ms")
    .auto_register(),
    // TODO(#14136): honor server-advertised RateLimit/RateLimit-Policy headers
    // (the IETF advertised-quota headers) here, alongside the reset-hint metrics
    // above, once that separate work lands.
    MetricSpec::new(
        "rate_control_adaptive_admission_ratio",
        MetricType::ObservableGaugeF64,
    )
    .description("Fraction of the configured HTTP rate limits currently admitted for this upstream origin (1 = admit all); absent in static rate-control mode")
    .auto_register(),
    MetricSpec::new(
        "rate_control_adaptive_throttled_total",
        MetricType::ObservableCounterU64,
    )
    .description("Total HTTP requests adaptive rate control throttled for this upstream origin (charged an above-normal weight because the origin was failing); 0 while the origin has stayed healthy, absent in static rate-control mode")
    .auto_register(),
];

#[derive(Debug, Clone)]
pub struct HttpRateControlMetricsProvider {
    connector_name: &'static str,
    metrics: Arc<HttpRateControlMetrics>,
    metric_source: Option<HttpRateControlMetricSource>,
    origin: Option<String>,
}

impl HttpRateControlMetricsProvider {
    #[must_use]
    pub fn new(
        connector_name: &'static str,
        metrics: Arc<HttpRateControlMetrics>,
        metric_source: Option<HttpRateControlMetricSource>,
    ) -> Self {
        let origin = metric_source
            .as_ref()
            .map(|source| rate_control_key(&source.base_url));
        Self {
            connector_name,
            metrics,
            metric_source,
            origin,
        }
    }
}

impl MetricsProvider for HttpRateControlMetricsProvider {
    fn component_type(&self) -> ComponentType {
        ComponentType::Dataset
    }

    fn component_name(&self) -> &'static str {
        self.connector_name
    }

    fn available_metrics(&self) -> &'static [MetricSpec] {
        HTTP_RATE_CONTROL_METRIC_SPECS
    }

    fn callback_to_observe_metric(
        &self,
        metric: &MetricSpec,
        attributes: Vec<KeyValue>,
    ) -> Option<ObserveMetricCallback> {
        let metrics = Arc::clone(&self.metrics);
        let metric_source = self.metric_source.clone();

        // Use `origin` (upstream URL) instead of `name` (dataset name) as the
        // metric label. Multiple datasets sharing the same origin share one
        // rate controller, and only one dataset emits metrics via `claim_owner`.
        let mut attributes = attributes;
        if let Some(origin) = &self.origin {
            if let Some(pos) = attributes.iter().position(|kv| kv.key.as_str() == "name") {
                attributes[pos] = KeyValue::new("origin", origin.clone());
            } else {
                attributes.push(KeyValue::new("origin", origin.clone()));
            }
        }

        macro_rules! observe_metric {
            ($value:expr) => {{
                Some(ObserveMetricCallback::U64(Box::new(move |observer| {
                    if should_observe_metrics(metric_source.as_ref()) {
                        observer.observe($value, &attributes);
                    }
                })))
            }};
        }

        // For a metric that does not apply to this origin: observe nothing, so
        // the series is absent rather than a constant `0`.
        macro_rules! observe_optional_metric {
            ($value:expr) => {{
                Some(ObserveMetricCallback::U64(Box::new(move |observer| {
                    if should_observe_metrics(metric_source.as_ref())
                        && let Some(value) = $value
                    {
                        observer.observe(value, &attributes);
                    }
                })))
            }};
        }

        macro_rules! observe_optional_f64_metric {
            ($value:expr) => {{
                Some(ObserveMetricCallback::F64(Box::new(move |observer| {
                    if should_observe_metrics(metric_source.as_ref())
                        && let Some(value) = $value
                    {
                        observer.observe(value, &attributes);
                    }
                })))
            }};
        }

        match metric.name {
            "inflight_operations" => observe_metric!(
                metrics.rate_controller_metric(RateControllerMetrics::inflight_permits)
            ),
            "rate_control_max_concurrent_requests" => {
                observe_metric!(metrics.max_concurrent_requests())
            }
            "rate_control_requests_per_second_limit" => {
                observe_metric!(metrics.requests_per_second_limit())
            }
            "rate_control_requests_per_minute_limit" => {
                observe_metric!(metrics.requests_per_minute_limit())
            }
            "rate_control_jitter_min_ms" => {
                observe_metric!(metrics.rate_control_jitter_min_ms.load(Ordering::Relaxed))
            }
            "rate_control_jitter_max_ms" => {
                observe_metric!(metrics.rate_control_jitter_max_ms.load(Ordering::Relaxed))
            }
            "rate_control_available_permits" => observe_metric!(metrics.available_permits()),
            "rate_control_acquisitions_total" => observe_metric!(
                metrics.rate_controller_metric(RateControllerMetrics::permits_acquired_total)
            ),
            "rate_control_acquire_errors_total" => observe_metric!(
                metrics.rate_controller_metric(RateControllerMetrics::acquire_errors_total)
            ),
            "rate_control_wait_duration_ms" => observe_metric!(
                metrics.rate_controller_metric(RateControllerMetrics::wait_duration_ms_total)
            ),
            "rate_limit_retry_after_updates_total" => observe_metric!(
                metrics.rate_limiter_metric(HttpRateLimiterMetrics::retry_after_updates_total)
            ),
            "rate_limit_retry_after_waits_total" => observe_metric!(
                metrics.rate_limiter_metric(HttpRateLimiterMetrics::retry_after_waits_total)
            ),
            "rate_limit_retry_after_wait_duration_ms" => {
                observe_metric!(metrics.rate_limiter_metric(
                    HttpRateLimiterMetrics::retry_after_wait_duration_ms_total,
                ))
            }
            "rate_limit_retry_after_remaining_ms" => observe_metric!(
                metrics.rate_limiter_metric(HttpRateLimiterMetrics::retry_after_remaining_ms)
            ),
            "rate_control_adaptive_admission_ratio" => {
                observe_optional_f64_metric!(metrics.adaptive_admission_ratio())
            }
            "rate_control_adaptive_throttled_total" => {
                observe_optional_metric!(metrics.adaptive_throttled_total())
            }
            _ => None,
        }
    }
}

fn should_observe_metrics(metric_source: Option<&HttpRateControlMetricSource>) -> bool {
    metric_source.is_none_or(HttpRateControlMetricSource::is_owner)
}

#[must_use]
pub fn parameter_specs() -> [ParameterSpec; 9] {
    [
        ParameterSpec::runtime("max_concurrent_requests")
            .description("Maximum number of concurrent HTTP requests to the same upstream origin. Overrides runtime.params.http_max_concurrent_requests when set. If both are unset, connector-level concurrency limiting is disabled."),
        ParameterSpec::runtime("requests_per_second_limit")
            .description("Maximum number of HTTP requests per second to the same upstream origin. Overrides runtime.params.http_requests_per_second_limit when set. If both are unset, no per-second request rate limit is applied."),
        ParameterSpec::runtime("requests_per_minute_limit")
            .description("Maximum number of HTTP requests per minute to the same upstream origin. Overrides runtime.params.http_requests_per_minute_limit when set. If both are unset, no per-minute request rate limit is applied."),
        ParameterSpec::runtime("rate_control_jitter_min")
            .description("Minimum random delay added before HTTP requests when rate control is active. Overrides runtime.params.http_rate_control_jitter_min when set. Accepts durations such as '5ms' or '0ms'. Defaults to 5ms when a request-rate limit is configured, otherwise 0ms."),
        ParameterSpec::runtime("rate_control_jitter_max")
            .description("Maximum random delay added before HTTP requests when rate control is active. Overrides runtime.params.http_rate_control_jitter_max when set. Accepts durations such as '10ms' or '0ms'. Defaults to 10ms when a request-rate limit is configured, otherwise 0ms."),
        ParameterSpec::runtime("rate_control_acquire_timeout")
            .description("Maximum time a request waits to acquire HTTP rate-control capacity (a concurrency slot and the per-second/minute quota) before failing instead of waiting indefinitely. Accepts durations such as '30s' or '500ms'. Defaults to the connector's `client_timeout`. '0' disables the bound. Overrides runtime.params.http_rate_control_acquire_timeout when set."),
        ParameterSpec::runtime("rate_control_mode")
            .description("How the configured HTTP rate limits apply. 'static' (default) applies them as they are. 'adaptive' lowers the effective request rate while the upstream origin fails or times out, then raises it again as the origin recovers, always within the configured static limits. Overrides runtime.params.http_rate_control_mode when set."),
        ParameterSpec::runtime("rate_control_failure_threshold")
            .description("The upstream error rate above which adaptive rate control begins throttling, as a percentage like '25%' or a fraction like '0.25'. Below this error rate the configured limits are used unchanged; above it, admission is scaled down in proportion to the success rate. Overrides runtime.params.http_rate_control_failure_threshold when set. Applies only when rate_control_mode is 'adaptive'. Defaults to 10%."),
        ParameterSpec::runtime("rate_control_window")
            .description("The reaction and recovery window for adaptive rate control, as a duration such as '10s' — the half-life over which request outcomes decay. A shorter window reacts to and recovers from failures faster; a longer one is smoother and slower. Overrides runtime.params.http_rate_control_window when set. Applies only when rate_control_mode is 'adaptive'. Defaults to 10s."),
    ]
}

/// Resolve a component's rate-control configuration from its own parameters,
/// falling back to the matching `runtime.params` key.
///
/// For connectors that declare [`parameter_specs`] and report request outcomes,
/// so adaptive rate control can take effect.
///
/// # Errors
/// Returns an invalid-configuration error when a `max_concurrent_requests`,
/// `requests_per_second_limit` or `requests_per_minute_limit` value does not
/// parse as a non-zero integer, a `rate_control_jitter_min` /
/// `rate_control_jitter_max` value does not parse as a duration, a
/// `rate_control_mode` / `rate_control_failure_threshold` / `rate_control_window`
/// value is invalid, or adaptive mode is set without a static limit to scale.
pub fn resolve_config_for_component<S: BuildHasher>(
    params: &Parameters,
    runtime_params: Option<&HashMap<String, String, S>>,
    connector_component: &ConnectorComponent,
    dataconnector: &'static str,
) -> DataConnectorResult<HttpRateControlConfig> {
    let config = HttpRateControlConfig {
        mode: resolve_rate_control_mode(
            params,
            runtime_params,
            connector_component,
            dataconnector,
        )?,
        ..resolve_static_config_for_component(
            params,
            runtime_params,
            connector_component,
            dataconnector,
        )?
    };

    ensure_adaptive_has_static_limit(&config, connector_component, dataconnector)?;
    Ok(config)
}

/// Resolve only the static limits (concurrency, request rate, jitter) of a
/// component's rate-control configuration; the mode stays
/// [`RateControlMode::Static`].
///
/// For connectors that do not yet record per-request outcomes and so do not
/// declare the `rate_control_mode` family: their clients report no outcome, so
/// adaptive mode could not take effect, and the runtime-wide
/// `runtime.params.http_rate_control_mode` default does not apply to them.
///
/// # Errors
/// Returns an invalid-configuration error when a `max_concurrent_requests`,
/// `requests_per_second_limit` or `requests_per_minute_limit` value does not
/// parse as a non-zero integer, or a `rate_control_jitter_min` /
/// `rate_control_jitter_max` / `rate_control_acquire_timeout` value does not
/// parse as a duration.
pub fn resolve_static_config_for_component<S: BuildHasher>(
    params: &Parameters,
    runtime_params: Option<&HashMap<String, String, S>>,
    connector_component: &ConnectorComponent,
    dataconnector: &'static str,
) -> DataConnectorResult<HttpRateControlConfig> {
    let config = HttpRateControlConfig {
        max_concurrent_requests: parse_optional_nonzero_usize_param(
            params,
            runtime_params,
            connector_component,
            dataconnector,
            "max_concurrent_requests",
            RUNTIME_MAX_CONCURRENT_REQUESTS,
        )?,
        requests_per_second: parse_optional_nonzero_u32_param(
            params,
            runtime_params,
            connector_component,
            dataconnector,
            "requests_per_second_limit",
            RUNTIME_REQUESTS_PER_SECOND_LIMIT,
        )?,
        requests_per_minute: parse_optional_nonzero_u32_param(
            params,
            runtime_params,
            connector_component,
            dataconnector,
            "requests_per_minute_limit",
            RUNTIME_REQUESTS_PER_MINUTE_LIMIT,
        )?,
        jitter_min: Duration::ZERO,
        jitter_max: Duration::ZERO,
        mode: RateControlMode::Static,
        acquire_timeout: parse_optional_duration_param(
            params,
            runtime_params,
            connector_component,
            dataconnector,
            "rate_control_acquire_timeout",
            RUNTIME_RATE_CONTROL_ACQUIRE_TIMEOUT,
        )?,
    };

    with_jitter(
        params,
        runtime_params,
        connector_component,
        dataconnector,
        config,
    )
}

/// Reject an origin that is in adaptive mode but defines no static rate limit
/// for it to adjust.
///
/// Adaptive mode is a modifier on a defined limit, never a limiter of its own,
/// so there must be at least one of `requests_per_second_limit`,
/// `requests_per_minute_limit`, or `max_concurrent_requests` (in either the
/// dataset-level or `runtime.params.http_*` form) for it to scale.
///
/// # Errors
/// Returns an invalid-configuration error when the origin is in adaptive mode
/// and has no static rate limit configured.
pub fn ensure_adaptive_has_static_limit(
    config: &HttpRateControlConfig,
    connector_component: &ConnectorComponent,
    dataconnector: &'static str,
) -> DataConnectorResult<()> {
    if config.adaptive_enabled() && !config.has_static_limit() {
        return Err(DataConnectorError::InvalidConfigurationNoSource {
            dataconnector: dataconnector.to_string(),
            connector_component: connector_component.clone(),
            message:
                "`rate_control_mode: adaptive` adjusts a defined rate limit, but no rate limit is set for this origin. \
                Set at least one of `requests_per_second_limit`, `requests_per_minute_limit`, or `max_concurrent_requests` \
                (as a dataset parameter or the matching `runtime.params.http_*` value), or set `rate_control_mode: static`. \
                See: https://spiceai.org/docs/components/data-connectors/http"
                    .to_string(),
        });
    }

    Ok(())
}

/// Resolve the `rate_control_mode` parameter and, in adaptive mode, the two
/// adaptive tuning parameters. Each one falls back to its matching
/// `runtime.params.http_*` default.
///
/// The tuning parameters are `rate_control_failure_threshold` (the error rate
/// above which throttling begins, as a percentage or a fraction, default 10%)
/// and `rate_control_window` (the reaction/recovery decay half-life, default
/// 10s). Both are inert outside adaptive mode, so a runtime-wide default applies
/// only to the datasets that select adaptive mode.
///
/// # Errors
/// Returns an invalid-configuration error for a `rate_control_mode` value other
/// than `static`/`adaptive`, a failure threshold that is not an error rate
/// between 0 and 1, or a window that is not a positive, finite duration.
fn resolve_rate_control_mode<S: BuildHasher>(
    params: &Parameters,
    runtime_params: Option<&HashMap<String, String, S>>,
    connector_component: &ConnectorComponent,
    dataconnector: &'static str,
) -> DataConnectorResult<RateControlMode> {
    let mode =
        resolve_rate_control_mode_name(params, runtime_params, connector_component, dataconnector)?;
    if mode == RateControlModeName::Static {
        // The failure-threshold/window knobs are meaningless outside adaptive
        // mode, so they stay inert here rather than erroring — a runtime-wide
        // default must not fail every dataset that stays in static mode.
        return Ok(RateControlMode::Static);
    }

    let failure_threshold = parse_optional_failure_threshold_param(
        params,
        runtime_params,
        connector_component,
        dataconnector,
    )?
    .unwrap_or(DEFAULT_ADAPTIVE_FAILURE_THRESHOLD);
    let window = parse_optional_duration_param(
        params,
        runtime_params,
        connector_component,
        dataconnector,
        "rate_control_window",
        RUNTIME_RATE_CONTROL_WINDOW,
    )?
    .unwrap_or(DEFAULT_ADAPTIVE_WINDOW);

    AdaptiveRateControl::new(failure_threshold, window).map(RateControlMode::Adaptive).map_err(|error| {
        let (param_name, runtime_param_name, detail) = match error {
            AdaptiveRateControlError::FailureThresholdInvalid { failure_threshold } => (
                "rate_control_failure_threshold",
                RUNTIME_RATE_CONTROL_FAILURE_THRESHOLD,
                format!(
                    "the failure threshold must be an error rate above 0% and below 100%, but got {:.0}%. It is the error rate above which throttling begins. Use a value such as '25%' or '0.25'.",
                    failure_threshold * 100.0
                ),
            ),
            AdaptiveRateControlError::WindowInvalid { .. } => (
                "rate_control_window",
                RUNTIME_RATE_CONTROL_WINDOW,
                "the window must be a positive, finite duration such as '10s'. A shorter window reacts and recovers faster.".to_string(),
            ),
        };
        let display_name = runtime_or_dataset_param_name(
            params,
            runtime_params,
            param_name,
            runtime_param_name,
        );
        DataConnectorError::InvalidConfigurationNoSource {
            dataconnector: dataconnector.to_string(),
            connector_component: connector_component.clone(),
            message: format!(
                "The '{display_name}' parameter is invalid: {detail} See: https://spiceai.org/docs/components/data-connectors/http"
            ),
        }
    })
}

/// Resolve the `rate_control_mode` value, falling back to
/// `runtime.params.http_rate_control_mode`, defaulting to `static`.
fn resolve_rate_control_mode_name<S: BuildHasher>(
    params: &Parameters,
    runtime_params: Option<&HashMap<String, String, S>>,
    connector_component: &ConnectorComponent,
    dataconnector: &'static str,
) -> DataConnectorResult<RateControlModeName> {
    let (raw_value, display_name) =
        if let Some(raw_value) = params.get("rate_control_mode").expose().ok() {
            (
                raw_value,
                params.user_param("rate_control_mode").to_string(),
            )
        } else if let Some(raw_value) = runtime_params
            .and_then(|runtime_params| runtime_params.get(RUNTIME_RATE_CONTROL_MODE))
            .map(String::as_str)
        {
            (
                raw_value,
                format!("runtime.params.{RUNTIME_RATE_CONTROL_MODE}"),
            )
        } else {
            return Ok(RateControlModeName::Static);
        };

    RateControlModeName::parse(raw_value).ok_or_else(|| {
        DataConnectorError::InvalidConfigurationNoSource {
            dataconnector: dataconnector.to_string(),
            connector_component: connector_component.clone(),
            message: format!(
                "The '{display_name}' parameter is invalid: '{}' is not a valid value. Use 'static' or 'adaptive'. See: https://spiceai.org/docs/components/data-connectors/http",
                raw_value.trim()
            ),
        }
    })
}

/// Whether `runtime.params.http_rate_control_mode` selects adaptive mode.
///
/// For a connector that does not declare the `rate_control_mode` family and so
/// always uses static rate control: it can report that the runtime-wide default
/// does not apply to it. An unparseable value reads as `false`; the connectors
/// that do declare the parameter report it as a configuration error.
#[must_use]
pub fn runtime_default_is_adaptive<S: BuildHasher>(
    runtime_params: Option<&HashMap<String, String, S>>,
) -> bool {
    runtime_params
        .and_then(|runtime_params| runtime_params.get(RUNTIME_RATE_CONTROL_MODE))
        .and_then(|value| RateControlModeName::parse(value.as_str()))
        == Some(RateControlModeName::Adaptive)
}

/// Report, once per process for `connector`, that it keeps static rate control
/// although `runtime.params.http_rate_control_mode` selects adaptive mode.
///
/// For a connector that does not yet record per-request outcomes: the
/// runtime-wide adaptive default silently does not apply to it, so it says so.
pub fn log_static_rate_control_once<S: BuildHasher>(
    connector: &str,
    runtime_params: Option<&HashMap<String, String, S>>,
) {
    static LOGGED_CONNECTORS: LazyLock<StdRwLock<HashSet<String>>> =
        LazyLock::new(|| StdRwLock::new(HashSet::new()));

    if !runtime_default_is_adaptive(runtime_params) {
        return;
    }

    let Ok(mut logged) = LOGGED_CONNECTORS.write() else {
        return;
    };
    if !logged.insert(connector.to_string()) {
        return;
    }

    tracing::info!(
        "The '{connector}' connector does not support `rate_control_mode: adaptive` yet, so it uses static rate control. The `runtime.params.http_rate_control_mode: adaptive` default has no effect on its components. See: https://spiceai.org/docs/components/data-connectors/http"
    );
}

impl HttpRateControlRegistry {
    #[must_use]
    pub fn with_persisted_governor_state(
        store: Arc<dyn ObjectStore>,
        base_prefix: impl Into<String>,
        refresh_interval: Duration,
    ) -> Self {
        Self {
            persisted_governor_state: Some(HttpRateControlPersistedState {
                store,
                base_prefix: base_prefix.into(),
                refresh_interval,
                instance_id: uuid::Uuid::new_v4().to_string(),
                instance_ttl: persisted_instance_ttl(refresh_interval),
            }),
            ..Self::default()
        }
    }

    pub fn start_persistence_task(self: &Arc<Self>) {
        let Some(persisted_state) = &self.persisted_governor_state else {
            return;
        };

        if self.persistence_task_started.swap(true, Ordering::AcqRel) {
            return;
        }

        let weak_registry = Arc::downgrade(self);
        let refresh_interval = persisted_state.refresh_interval;
        // Tick faster than `refresh_interval` so a fresh lease is acquired
        // shortly after each window rolls. With this cadence the worst-case
        // dead-zone at a window boundary is `tick_interval` (≈1/4 of the
        // window) rather than a full `refresh_interval`. Each tick is cheap
        // when nothing has changed (no OCC write).
        let tick_interval = (refresh_interval / 4).max(std::time::Duration::from_millis(100));
        let persistence_task = tokio::spawn(async move {
            if let Some(registry) = weak_registry.upgrade() {
                registry.refresh_and_persist_governor_states().await;
            }

            let first_tick = tokio::time::Instant::now() + tick_interval;
            let mut tick = tokio::time::interval_at(first_tick, tick_interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tick.tick().await;
                let Some(registry) = weak_registry.upgrade() else {
                    break;
                };
                registry.refresh_and_persist_governor_states().await;
            }
        });
        drop(persistence_task);
    }

    async fn persisted_rate_controllers(&self) -> Vec<(String, Arc<RateController>)> {
        self.rate_controllers
            .read()
            .await
            .iter()
            .filter_map(|(origin, entry)| {
                entry
                    .shared
                    .controller
                    .as_ref()
                    .map(|controller| (origin.clone(), Arc::clone(controller)))
            })
            .collect()
    }

    async fn refresh_and_persist_governor_states(&self) {
        for (origin, controller) in self.persisted_rate_controllers().await {
            if let Err(error) = controller.refresh_and_persist_state_snapshot().await {
                tracing::warn!(
                    origin = origin.as_str(),
                    "Failed to persist rate-control state: {error}"
                );
            }
        }
    }

    pub async fn shared_rate_limiter(&self, base_url: &Url) -> Arc<HttpRateLimiter> {
        self.shared_rate_limiter_for_config(base_url, &HttpRateControlConfig::disabled())
            .await
    }

    /// The per-origin rate limiter, built on first use. This limiter handles only
    /// server-advertised cooldowns (`Retry-After` / `RateLimit` reset headers);
    /// the origin's configured static limits and any adaptive scaling are enforced
    /// by the separate [`RateController`]. One origin gets one limiter.
    ///
    /// `_config` is retained for call-site symmetry with the controller
    /// reservation path; the limiter itself no longer depends on it.
    pub async fn shared_rate_limiter_for_config(
        &self,
        base_url: &Url,
        _config: &HttpRateControlConfig,
    ) -> Arc<HttpRateLimiter> {
        let key = rate_control_key(base_url);
        let rate_limiters = self.rate_limiters.read().await;
        if let Some(rate_limiter) = rate_limiters.get(&key) {
            return Arc::clone(rate_limiter);
        }

        drop(rate_limiters);
        let mut rate_limiters = self.rate_limiters.write().await;
        Arc::clone(
            rate_limiters
                .entry(key)
                .or_insert_with(|| Arc::new(HttpRateLimiter::default())),
        )
    }

    #[must_use]
    pub fn shared_metrics(&self, base_url: &Url) -> Arc<HttpRateControlMetrics> {
        let key = rate_control_key(base_url);
        let metrics_by_origin = self.metrics_by_origin.read().ok();
        if let Some(metrics) = metrics_by_origin
            .as_ref()
            .and_then(|metrics_by_origin| metrics_by_origin.get(&key))
        {
            return Arc::clone(metrics);
        }
        drop(metrics_by_origin);

        let Ok(mut metrics_by_origin) = self.metrics_by_origin.write() else {
            return Arc::new(HttpRateControlMetrics::default());
        };

        Arc::clone(
            metrics_by_origin
                .entry(key)
                .or_insert_with(|| Arc::new(HttpRateControlMetrics::default())),
        )
    }

    pub fn claim_metrics_owner(&self, base_url: &Url, owner: &str) -> bool {
        let key = rate_control_key(base_url);
        let Ok(mut metric_owners) = self.metric_owners.write() else {
            return false;
        };

        if let Some(existing_owner) = metric_owners.get(&key) {
            if existing_owner != owner {
                tracing::warn!(
                    upstream_origin = %key,
                    metrics_owner = existing_owner.as_str(),
                    skipped_dataset = owner,
                    "HTTP rate-control metrics are shared per upstream origin. Metrics are emitted with origin={key} by dataset '{existing_owner}'. Skipping duplicate metric registration for dataset '{owner}'.",
                );
            }
            existing_owner == owner
        } else {
            metric_owners.insert(key, owner.to_string());
            true
        }
    }

    fn is_metrics_owner(&self, base_url: &Url, owner: &str) -> bool {
        let key = rate_control_key(base_url);
        self.metric_owners
            .read()
            .ok()
            .and_then(|metric_owners| metric_owners.get(&key).cloned())
            .is_some_and(|existing_owner| existing_owner == owner)
    }

    /// Reject a rate-control mode this registry cannot enforce.
    ///
    /// Cluster rate control charges the cluster-wide budget whole-number
    /// weights, which cannot express a fractional admission ratio, so adaptive
    /// mode would be a partial no-op. It is rejected until a weighted cluster
    /// acquire lands.
    ///
    /// # Errors
    /// Returns an invalid-configuration error when the component selects
    /// adaptive mode and this registry uses cluster rate control.
    fn ensure_mode_is_supported(
        &self,
        origin: &str,
        config: &HttpRateControlConfig,
        connector_component: &ConnectorComponent,
        dataconnector: &'static str,
    ) -> DataConnectorResult<()> {
        if self.persisted_governor_state.is_none() || !config.adaptive_enabled() {
            return Ok(());
        }

        Err(DataConnectorError::InvalidConfigurationNoSource {
            dataconnector: dataconnector.to_string(),
            connector_component: connector_component.clone(),
            message: format!(
                "`rate_control_mode: adaptive` is not yet supported with cluster rate control (`runtime.source_rate_control.state_location`) for origin '{origin}'. \
                Set `rate_control_mode: static` for this dataset, or remove `state_location`. \
                See: https://spiceai.org/docs/components/data-connectors/http"
            ),
        })
    }

    /// Reserve the controller shared by every component targeting this origin,
    /// creating it if this is the first. The reservation must be committed or
    /// rolled back so an abandoned registration does not pin the origin's config.
    ///
    /// # Errors
    /// Returns an invalid-configuration error when the origin already has a
    /// controller built from different rate-control settings — one origin gets
    /// one config, so the conflicting component names the values to reconcile.
    pub async fn reserve_shared_rate_controller_for_component(
        self: Arc<Self>,
        base_url: &Url,
        config: &HttpRateControlConfig,
        spicepod_name: &str,
        connector_component: &ConnectorComponent,
        dataconnector: &'static str,
    ) -> DataConnectorResult<SharedRateControllerReservation> {
        let key = rate_control_key(base_url);
        self.ensure_mode_is_supported(&key, config, connector_component, dataconnector)?;
        let mut rate_controllers = self.rate_controllers.write().await;

        if let Some(existing) = rate_controllers.get_mut(&key) {
            if existing.shared.config != *config {
                return conflicting_config_error(connector_component, dataconnector, &key);
            }
            existing.pending_registrations = existing.pending_registrations.saturating_add(1);
            let shared = existing.shared.clone();
            drop(rate_controllers);
            return Ok(SharedRateControllerReservation {
                registry: self,
                key,
                shared,
            });
        }

        let shared = build_shared_rate_controller(
            &key,
            spicepod_name,
            config,
            self.persisted_governor_state.as_ref(),
        );
        rate_controllers.insert(
            key.clone(),
            SharedRateControllerEntry {
                shared: shared.clone(),
                pending_registrations: 1,
                active_registrations: 0,
            },
        );

        drop(rate_controllers);
        Ok(SharedRateControllerReservation {
            registry: self,
            key,
            shared,
        })
    }

    async fn commit_rate_controller_reservation(&self, key: &str) {
        let mut rate_controllers = self.rate_controllers.write().await;
        if let Some(existing) = rate_controllers.get_mut(key) {
            existing.pending_registrations = existing.pending_registrations.saturating_sub(1);
            existing.active_registrations = existing.active_registrations.saturating_add(1);
        }
    }

    async fn rollback_rate_controller_reservation(&self, key: &str) {
        let mut rate_controllers = self.rate_controllers.write().await;
        if let Some(existing) = rate_controllers.get_mut(key) {
            existing.pending_registrations = existing.pending_registrations.saturating_sub(1);
            if existing.pending_registrations == 0 && existing.active_registrations == 0 {
                rate_controllers.remove(key);
            }
        }
    }

    /// The controller shared by every component targeting this origin, built on
    /// first use. Prefer [`Self::reserve_shared_rate_controller_for_component`]
    /// when the caller can still fail after this point.
    ///
    /// # Errors
    /// Returns an invalid-configuration error when the origin already has a
    /// controller built from different rate-control settings — one origin gets
    /// one config, so the conflicting component names the values to reconcile.
    pub async fn shared_rate_controller_for_component(
        &self,
        base_url: &Url,
        config: &HttpRateControlConfig,
        spicepod_name: &str,
        connector_component: &ConnectorComponent,
        dataconnector: &'static str,
    ) -> DataConnectorResult<SharedRateController> {
        let key = rate_control_key(base_url);
        self.ensure_mode_is_supported(&key, config, connector_component, dataconnector)?;
        let rate_controllers = self.rate_controllers.read().await;
        if let Some(existing) = rate_controllers.get(&key) {
            return resolve_existing_controller(
                &existing.shared,
                config,
                connector_component,
                dataconnector,
                &key,
            );
        }

        drop(rate_controllers);
        let mut rate_controllers = self.rate_controllers.write().await;
        if let Some(existing) = rate_controllers.get(&key) {
            return resolve_existing_controller(
                &existing.shared,
                config,
                connector_component,
                dataconnector,
                &key,
            );
        }

        let shared = build_shared_rate_controller(
            &key,
            spicepod_name,
            config,
            self.persisted_governor_state.as_ref(),
        );
        rate_controllers.insert(
            key.clone(),
            SharedRateControllerEntry {
                shared: shared.clone(),
                pending_registrations: 0,
                active_registrations: 1,
            },
        );

        drop(rate_controllers);
        Ok(shared)
    }
}

pub async fn shared_rate_limiter(base_url: &Url) -> Arc<HttpRateLimiter> {
    GLOBAL_HTTP_RATE_CONTROL_REGISTRY
        .shared_rate_limiter(base_url)
        .await
}

#[must_use]
pub fn shared_metrics(base_url: &Url) -> Arc<HttpRateControlMetrics> {
    GLOBAL_HTTP_RATE_CONTROL_REGISTRY.shared_metrics(base_url)
}

pub fn claim_metrics_owner(base_url: &Url, owner: &str) -> bool {
    GLOBAL_HTTP_RATE_CONTROL_REGISTRY.claim_metrics_owner(base_url, owner)
}

fn build_shared_rate_controller(
    origin_key: &str,
    spicepod_name: &str,
    config: &HttpRateControlConfig,
    persisted_state: Option<&HttpRateControlPersistedState>,
) -> SharedRateController {
    if !config.is_enabled() {
        return SharedRateController {
            config: config.clone(),
            controller: None,
        };
    }

    let mut builder = RateController::builder()
        .with_origin(origin_key)
        .with_jitter(JitterConfig::new(config.jitter_min, config.jitter_max));
    if let Some(persisted_state) = persisted_state {
        builder = builder.with_object_store_persistence_for_instance(
            Arc::clone(&persisted_state.store),
            persisted_state.base_prefix.clone(),
            rate_control_state_object_key(spicepod_name, origin_key),
            origin_key.to_string(),
            persisted_state.instance_id.clone(),
            persisted_state.refresh_interval,
        );
    }
    if let Some(max_concurrent_requests) = config.max_concurrent_requests {
        builder = builder.with_max_concurrent_requests(max_concurrent_requests);
    }
    if let Some(requests_per_second) = config.requests_per_second {
        builder = builder.add_quota_with_name(
            "requests_per_second",
            Quota::per_second(requests_per_second),
        );
    }
    if let Some(requests_per_minute) = config.requests_per_minute {
        builder = builder.add_quota_with_name(
            "requests_per_minute",
            Quota::per_minute(requests_per_minute),
        );
    }
    if let Some(control) = config.mode.adaptive() {
        builder = builder.with_adaptive(control, origin_key);
    }
    // A zero timeout means "no bound" (wait indefinitely), matching the param
    // docs. It is the only spelling for that: parsing rejects 'inf'.
    if let Some(acquire_timeout) = config.acquire_timeout
        && !acquire_timeout.is_zero()
    {
        builder = builder.with_acquire_timeout(acquire_timeout);
    }

    SharedRateController {
        config: config.clone(),
        controller: Some(builder.build()),
    }
}

fn resolve_existing_controller(
    existing: &SharedRateController,
    config: &HttpRateControlConfig,
    connector_component: &ConnectorComponent,
    dataconnector: &'static str,
    key: &str,
) -> DataConnectorResult<SharedRateController> {
    if existing.config == *config {
        return Ok(existing.clone());
    }

    conflicting_config_error(connector_component, dataconnector, key)
}

fn parse_optional_nonzero_u32_param<S: BuildHasher>(
    params: &Parameters,
    runtime_params: Option<&HashMap<String, String, S>>,
    connector_component: &ConnectorComponent,
    dataconnector: &'static str,
    parameter_name: &str,
    runtime_parameter_name: &'static str,
) -> DataConnectorResult<Option<NonZeroU32>> {
    let (raw_value, display_name) =
        if let Some(raw_value) = params.get(parameter_name).expose().ok() {
            (raw_value, params.user_param(parameter_name).to_string())
        } else if let Some(raw_value) = runtime_params
            .and_then(|runtime_params| runtime_params.get(runtime_parameter_name))
            .map(String::as_str)
        {
            (
                raw_value,
                format!("runtime.params.{runtime_parameter_name}"),
            )
        } else {
            return Ok(None);
        };

    let trimmed = raw_value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let value =
        trimmed
            .parse::<u32>()
            .map_err(|source| DataConnectorError::InvalidConfiguration {
                dataconnector: dataconnector.to_string(),
                message: format!("The '{display_name}' parameter must be a positive integer."),
                connector_component: connector_component.clone(),
                source: source.into(),
            })?;

    NonZeroU32::new(value).map(Some).ok_or_else(|| {
        DataConnectorError::InvalidConfigurationNoSource {
            dataconnector: dataconnector.to_string(),
            connector_component: connector_component.clone(),
            message: format!("The '{display_name}' parameter must be greater than 0."),
        }
    })
}

fn parse_optional_nonzero_usize_param<S: BuildHasher>(
    params: &Parameters,
    runtime_params: Option<&HashMap<String, String, S>>,
    connector_component: &ConnectorComponent,
    dataconnector: &'static str,
    parameter_name: &str,
    runtime_parameter_name: &'static str,
) -> DataConnectorResult<Option<usize>> {
    let (raw_value, display_name) =
        if let Some(raw_value) = params.get(parameter_name).expose().ok() {
            (raw_value, params.user_param(parameter_name).to_string())
        } else if let Some(raw_value) = runtime_params
            .and_then(|runtime_params| runtime_params.get(runtime_parameter_name))
            .map(String::as_str)
        {
            (
                raw_value,
                format!("runtime.params.{runtime_parameter_name}"),
            )
        } else {
            return Ok(None);
        };

    let trimmed = raw_value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let value =
        trimmed
            .parse::<usize>()
            .map_err(|source| DataConnectorError::InvalidConfiguration {
                dataconnector: dataconnector.to_string(),
                message: format!("The '{display_name}' parameter must be a positive integer."),
                connector_component: connector_component.clone(),
                source: source.into(),
            })?;

    if value == 0 {
        return Err(DataConnectorError::InvalidConfigurationNoSource {
            dataconnector: dataconnector.to_string(),
            connector_component: connector_component.clone(),
            message: format!("The '{display_name}' parameter must be greater than 0."),
        });
    }

    Ok(Some(value))
}

/// Parse the optional `rate_control_failure_threshold` (an error rate),
/// falling back to its `runtime.params.http_*` default, into a fraction.
///
/// Accepts either a percentage like `25%` or a bare fraction like `0.25`. Range
/// validation (`(0, 1)`) is left to [`AdaptiveRateControl::new`]; this only
/// rejects a value that is neither a percentage nor a number.
fn parse_optional_failure_threshold_param<S: BuildHasher>(
    params: &Parameters,
    runtime_params: Option<&HashMap<String, String, S>>,
    connector_component: &ConnectorComponent,
    dataconnector: &'static str,
) -> DataConnectorResult<Option<f64>> {
    let parameter_name = "rate_control_failure_threshold";
    let (raw_value, display_name) =
        if let Some(raw_value) = params.get(parameter_name).expose().ok() {
            (raw_value, params.user_param(parameter_name).to_string())
        } else if let Some(raw_value) = runtime_params
            .and_then(|runtime_params| runtime_params.get(RUNTIME_RATE_CONTROL_FAILURE_THRESHOLD))
            .map(String::as_str)
        {
            (
                raw_value,
                format!("runtime.params.{RUNTIME_RATE_CONTROL_FAILURE_THRESHOLD}"),
            )
        } else {
            return Ok(None);
        };

    let trimmed = raw_value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    // `25%` -> 0.25; a bare `0.25` -> 0.25. A bare `25` parses to 25.0 and is
    // rejected downstream as out of range, guiding the user to `25%`.
    let parsed = if let Some(percent) = trimmed.strip_suffix('%') {
        percent.trim().parse::<f64>().map(|value| value / 100.0)
    } else {
        trimmed.parse::<f64>()
    };

    parsed
        .map(Some)
        .map_err(|source| DataConnectorError::InvalidConfiguration {
            dataconnector: dataconnector.to_string(),
            message: format!(
                "The '{display_name}' parameter must be an error rate as a percentage like '25%' or a fraction like '0.25'."
            ),
            connector_component: connector_component.clone(),
            source: source.into(),
        })
}

/// Default bound for the permit-acquire wait when `rate_control_acquire_timeout`
/// is unset: the connector's own `client_timeout`. The acquire happens once per
/// request attempt, so a queued request should wait for a slot about as long as
/// one in-flight request can take (a concurrency slot frees, or a governor token
/// refills, on that timescale); waiting longer means the holder is stuck and
/// failing fast is correct. Scales automatically when the user raises
/// `client_timeout` for a slow origin. Bounds the otherwise-unbounded permit
/// wait (#14348). `client_timeout` already includes the connect phase, so
/// `connect_timeout` is deliberately not added.
#[must_use]
pub fn default_acquire_timeout(client_timeout: Duration) -> Duration {
    client_timeout
}

fn parse_optional_duration_param<S: BuildHasher>(
    params: &Parameters,
    runtime_params: Option<&HashMap<String, String, S>>,
    connector_component: &ConnectorComponent,
    dataconnector: &'static str,
    parameter_name: &str,
    runtime_parameter_name: &'static str,
) -> DataConnectorResult<Option<Duration>> {
    let (raw_value, display_name) =
        if let Some(raw_value) = params.get(parameter_name).expose().ok() {
            (raw_value, params.user_param(parameter_name).to_string())
        } else if let Some(raw_value) = runtime_params
            .and_then(|runtime_params| runtime_params.get(runtime_parameter_name))
            .map(String::as_str)
        {
            (
                raw_value,
                format!("runtime.params.{runtime_parameter_name}"),
            )
        } else {
            return Ok(None);
        };

    let trimmed = raw_value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let value = fundu::parse_duration(trimmed).map_err(|source| {
        DataConnectorError::InvalidConfiguration {
            dataconnector: dataconnector.to_string(),
            message: format!(
                "The '{display_name}' parameter must be a valid duration such as '10ms', '1s', or '0ms'."
            ),
            connector_component: connector_component.clone(),
            source: source.into(),
        }
    })?;

    // `fundu` parses 'inf'/'infinity' into a saturated duration. Refuse it, so
    // '0' stays the one spelling for "no bound".
    if value == Duration::MAX {
        return Err(DataConnectorError::InvalidConfigurationNoSource {
            dataconnector: dataconnector.to_string(),
            message: format!(
                "The '{display_name}' parameter must be a finite duration such as '10ms' or '1s'. Use '0' for no limit."
            ),
            connector_component: connector_component.clone(),
        });
    }

    Ok(Some(value))
}

fn with_jitter<S: BuildHasher>(
    params: &Parameters,
    runtime_params: Option<&HashMap<String, String, S>>,
    connector_component: &ConnectorComponent,
    dataconnector: &'static str,
    mut config: HttpRateControlConfig,
) -> DataConnectorResult<HttpRateControlConfig> {
    let jitter_min = parse_optional_duration_param(
        params,
        runtime_params,
        connector_component,
        dataconnector,
        "rate_control_jitter_min",
        RUNTIME_RATE_CONTROL_JITTER_MIN,
    )?;
    let jitter_max = parse_optional_duration_param(
        params,
        runtime_params,
        connector_component,
        dataconnector,
        "rate_control_jitter_max",
        RUNTIME_RATE_CONTROL_JITTER_MAX,
    )?;

    let has_request_quota =
        config.requests_per_second.is_some() || config.requests_per_minute.is_some();

    let (resolved_min, resolved_max) = match (jitter_min, jitter_max) {
        (Some(min), Some(max)) => (min, max),
        (Some(min), None) => (min, min),
        (None, Some(max)) => (Duration::ZERO, max),
        (None, None) if has_request_quota => (
            DEFAULT_RATE_CONTROL_JITTER_MIN,
            DEFAULT_RATE_CONTROL_JITTER_MAX,
        ),
        (None, None) => (Duration::ZERO, Duration::ZERO),
    };

    if resolved_min > resolved_max {
        return Err(DataConnectorError::InvalidConfigurationNoSource {
            dataconnector: dataconnector.to_string(),
            connector_component: connector_component.clone(),
            message: format!(
                "The '{}' parameter must be less than or equal to '{}'.",
                runtime_or_dataset_param_name(
                    params,
                    runtime_params,
                    "rate_control_jitter_min",
                    RUNTIME_RATE_CONTROL_JITTER_MIN
                ),
                runtime_or_dataset_param_name(
                    params,
                    runtime_params,
                    "rate_control_jitter_max",
                    RUNTIME_RATE_CONTROL_JITTER_MAX
                )
            ),
        });
    }

    config.jitter_min = resolved_min;
    config.jitter_max = resolved_max;
    Ok(config)
}

fn runtime_or_dataset_param_name<S: BuildHasher>(
    params: &Parameters,
    runtime_params: Option<&HashMap<String, String, S>>,
    parameter_name: &str,
    runtime_parameter_name: &'static str,
) -> String {
    if params.get(parameter_name).expose().ok().is_some() {
        params.user_param(parameter_name).to_string()
    } else if runtime_params
        .is_some_and(|runtime_params| runtime_params.contains_key(runtime_parameter_name))
    {
        format!("runtime.params.{runtime_parameter_name}")
    } else {
        params.user_param(parameter_name).to_string()
    }
}

#[must_use]
pub fn rate_control_key(base_url: &Url) -> String {
    let scheme = base_url.scheme();
    let host = base_url.host_str().unwrap_or_default().to_ascii_lowercase();
    match base_url.port_or_known_default() {
        Some(port) => format!("{scheme}://{host}:{port}"),
        None => format!("{scheme}://{host}"),
    }
}

fn rate_control_state_object_key(spicepod_name: &str, origin_key: &str) -> String {
    let origin_without_scheme = origin_key
        .split_once("://")
        .map_or(origin_key, |(_, origin)| origin);
    format!(
        "{}/{}-{:016x}",
        friendly_state_key_component(spicepod_name),
        friendly_state_key_component(origin_without_scheme),
        stable_state_key_hash(origin_key)
    )
}

fn friendly_state_key_component(value: &str) -> String {
    let mut component = String::with_capacity(value.len());
    let mut previous_was_separator = false;

    for character in value.chars() {
        if character.is_ascii_alphanumeric() || matches!(character, '-' | '.' | '_') {
            component.push(character);
            previous_was_separator = false;
        } else if !previous_was_separator {
            component.push('_');
            previous_was_separator = true;
        }
    }

    let component = component.trim_matches('_');
    if component.is_empty() {
        "default".to_string()
    } else {
        component.to_string()
    }
}

fn stable_state_key_hash(value: &str) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 14_695_981_039_346_656_037;
    const FNV_PRIME: u64 = 1_099_511_628_211;

    value.bytes().fold(FNV_OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(FNV_PRIME)
    })
}

fn conflicting_config_error<T>(
    connector_component: &ConnectorComponent,
    dataconnector: &'static str,
    key: &str,
) -> DataConnectorResult<T> {
    Err(DataConnectorError::InvalidConfigurationNoSource {
        dataconnector: dataconnector.to_string(),
        connector_component: connector_component.clone(),
        message: format!(
            "Multiple HTTP-based components target {key} with different rate-control settings. Use the same max_concurrent_requests, requests_per_second_limit, requests_per_minute_limit, rate_control_jitter_min, rate_control_jitter_max, rate_control_acquire_timeout, rate_control_mode, rate_control_failure_threshold and rate_control_window values for components sharing an origin."
        ),
    })
}

fn duration_millis_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn persisted_instance_ttl(refresh_interval: Duration) -> Duration {
    refresh_interval
        .saturating_mul(3)
        .max(MIN_PERSISTED_INSTANCE_TTL)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU32;
    use std::sync::Arc;

    // The `ensure_adaptive_has_static_limit` tests live in
    // `crates/runtime/tests/rate_control/mod.rs`: they need a `ConnectorComponent`,
    // which requires `runtime-component`, and this crate must not depend on it.

    #[test]
    fn rate_control_mode_accepts_the_documented_values() {
        assert_eq!(
            RateControlModeName::parse(""),
            Some(RateControlModeName::Static)
        );
        assert_eq!(
            RateControlModeName::parse("static"),
            Some(RateControlModeName::Static)
        );
        assert_eq!(
            RateControlModeName::parse("STATIC"),
            Some(RateControlModeName::Static)
        );
        assert_eq!(
            RateControlModeName::parse("adaptive"),
            Some(RateControlModeName::Adaptive)
        );
        assert_eq!(
            RateControlModeName::parse(" Adaptive "),
            Some(RateControlModeName::Adaptive)
        );
        // The old switch values, and a bare number, are not modes.
        assert_eq!(RateControlModeName::parse("enabled"), None);
        assert_eq!(RateControlModeName::parse("2.0"), None);
        assert_eq!(RateControlModeName::parse("sometimes"), None);
    }

    #[test]
    fn runtime_default_reports_only_the_adaptive_mode() {
        let runtime_params = |value: &str| {
            HashMap::<String, String>::from([(
                RUNTIME_RATE_CONTROL_MODE.to_string(),
                value.to_string(),
            )])
        };

        assert!(!runtime_default_is_adaptive(
            None::<&HashMap<String, String>>
        ));
        assert!(!runtime_default_is_adaptive(Some(&runtime_params(
            "static"
        ))));
        assert!(!runtime_default_is_adaptive(Some(&runtime_params(
            "not-a-mode"
        ))));
        assert!(runtime_default_is_adaptive(Some(&runtime_params(
            "adaptive"
        ))));
    }

    #[test]
    fn has_static_limit_reflects_any_configured_limit() {
        let mut config = HttpRateControlConfig::disabled();
        assert!(!config.has_static_limit());

        config.requests_per_second = NonZeroU32::new(5);
        assert!(config.has_static_limit());
    }

    #[test]
    fn rate_control_state_object_key_uses_spicepod_and_origin_without_scheme() {
        let object_key = rate_control_state_object_key(
            "rate control registry test",
            "https://api.example.com:443",
        );

        let hash = object_key
            .strip_prefix("rate_control_registry_test/api.example.com_443-")
            .expect("object key should start with spicepod name and origin without scheme");
        assert_eq!(hash.len(), 16);
        assert!(hash.chars().all(|character| character.is_ascii_hexdigit()));
        assert!(!object_key.contains("https"));
    }

    /// The adaptive metrics report no value (so no series) for an origin in
    /// static mode, and real values — `0` throttled up front — for one in
    /// adaptive mode.
    #[test]
    fn adaptive_metrics_are_absent_in_static_mode() {
        let quota = Quota::per_second(NonZeroU32::new(10).expect("non-zero test quota"));
        let metrics = HttpRateControlMetrics::default();
        assert_eq!(metrics.adaptive_admission_ratio(), None);
        assert_eq!(metrics.adaptive_throttled_total(), None);

        let static_controller = RateController::builder().add_quota(quota).build();
        metrics.set_rate_controller(Some(&static_controller));
        assert_eq!(metrics.adaptive_admission_ratio(), None);
        assert_eq!(metrics.adaptive_throttled_total(), None);

        let adaptive_controller = RateController::builder()
            .add_quota(quota)
            .with_adaptive(
                AdaptiveRateControl::new(0.1, runtime_rate_control::DEFAULT_ADAPTIVE_WINDOW)
                    .expect("valid adaptive control"),
                "https://adaptive-metrics.example.com",
            )
            .build();
        metrics.set_rate_controller(Some(&adaptive_controller));
        assert_eq!(metrics.adaptive_admission_ratio(), Some(1.0));
        assert_eq!(metrics.adaptive_throttled_total(), Some(0));
    }

    #[test]
    fn metric_source_observes_only_after_owner_claim() {
        let registry = Arc::new(HttpRateControlRegistry::default());
        let url = Url::parse("https://rate-control-metrics.example.com/data")
            .expect("test URL should parse");
        let owner = HttpRateControlMetricSource::new(
            Arc::clone(&registry),
            url.clone(),
            "owner".to_string(),
        );
        let other = HttpRateControlMetricSource::new(registry, url, "other".to_string());

        assert!(!owner.is_owner());
        assert!(owner.claim_owner());
        assert!(owner.is_owner());
        assert!(!other.claim_owner());
        assert!(!other.is_owner());
    }
}
