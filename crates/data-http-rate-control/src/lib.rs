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

use std::collections::HashMap;
use std::hash::BuildHasher;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, RwLock as StdRwLock};
use std::time::Duration;

pub use data_components::rate_limit::{
    AdaptiveMode, AdaptiveRateControl, AdaptiveRateControlParseError,
};
use data_components::rate_limit::{
    DEFAULT_ADAPTIVE_ELASTICITY, DEFAULT_ADAPTIVE_MAGNITUDE, HttpRateLimiter,
    HttpRateLimiterMetrics, parse_adaptive_mode,
};
use data_connector_types::{ConnectorComponent, DataConnectorError, DataConnectorResult};
use governor::Quota;
use object_store::ObjectStore;
use opentelemetry::KeyValue;
use runtime_api_types::v1::ComponentType;
use runtime_metrics::component::{MetricSpec, MetricType, MetricsProvider, ObserveMetricCallback};
use runtime_parameters::{ParameterSpec, Parameters};
use runtime_rate_control::{JitterConfig, RateController, RateControllerMetrics};
use tokio::sync::RwLock;
use url::Url;

const DEFAULT_RATE_CONTROL_JITTER_MIN: Duration = Duration::from_millis(5);
const DEFAULT_RATE_CONTROL_JITTER_MAX: Duration = Duration::from_millis(10);
const RUNTIME_MAX_CONCURRENT_REQUESTS: &str = "http_max_concurrent_requests";
const RUNTIME_REQUESTS_PER_SECOND_LIMIT: &str = "http_requests_per_second_limit";
const RUNTIME_REQUESTS_PER_MINUTE_LIMIT: &str = "http_requests_per_minute_limit";
const RUNTIME_RATE_CONTROL_JITTER_MIN: &str = "http_rate_control_jitter_min";
const RUNTIME_RATE_CONTROL_JITTER_MAX: &str = "http_rate_control_jitter_max";
const RUNTIME_ADAPTIVE_RATE_CONTROL: &str = "http_adaptive_rate_control";
const RUNTIME_ADAPTIVE_RATE_CONTROL_MAGNITUDE: &str = "http_adaptive_rate_control_magnitude";
const RUNTIME_ADAPTIVE_RATE_CONTROL_ELASTICITY: &str = "http_adaptive_rate_control_elasticity";

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
    RUNTIME_ADAPTIVE_RATE_CONTROL,
    RUNTIME_ADAPTIVE_RATE_CONTROL_MAGNITUDE,
    RUNTIME_ADAPTIVE_RATE_CONTROL_ELASTICITY,
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
    pub adaptive_rate_control: AdaptiveRateControl,
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
            adaptive_rate_control: AdaptiveRateControl::Disabled,
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

    /// The origin's configured static rate limit that adaptive control adjusts:
    /// the largest of the configured per-second / per-minute / concurrency
    /// limits, or `None` when the origin defines no static rate limit at all.
    ///
    /// Adaptive control is a modifier on a defined limit, so this being `None`
    /// while [`Self::adaptive_rate_control`] is enabled is a configuration error
    /// (see [`ensure_adaptive_has_static_limit`]). The reference is the ceiling
    /// the admission coefficient scales within; that single coefficient scales
    /// every configured limit uniformly, so any one of them serves as the
    /// reference.
    #[must_use]
    pub fn static_limit_reference(&self) -> Option<f64> {
        [
            self.requests_per_second.map(|limit| f64::from(limit.get())),
            self.requests_per_minute.map(|limit| f64::from(limit.get())),
            self.max_concurrent_requests.map(usize_to_f64),
        ]
        .into_iter()
        .flatten()
        .fold(None, |max: Option<f64>, value| {
            Some(max.map_or(value, |current| current.max(value)))
        })
    }

    /// Whether adaptive control is enabled for this origin.
    #[must_use]
    pub fn adaptive_enabled(&self) -> bool {
        self.adaptive_rate_control != AdaptiveRateControl::Disabled
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
        "adaptive_rate_control_effective_limit",
        MetricType::ObservableGaugeU64,
    )
    .description("Current adaptive client-side effective request-rate limit for this upstream origin; 0 when adaptive rate control is disabled")
    .auto_register(),
    MetricSpec::new(
        "adaptive_rate_control_admission_coefficient_permille",
        MetricType::ObservableGaugeU64,
    )
    .description("Current adaptive admission coefficient for this upstream origin, in parts-per-thousand (1000 = admit all); 0 when adaptive rate control is disabled")
    .auto_register(),
    MetricSpec::new(
        "adaptive_rate_control_throttled_total",
        MetricType::ObservableCounterU64,
    )
    .description("Total HTTP requests the adaptive controller throttled for this upstream origin")
    .auto_register(),
    MetricSpec::new(
        "adaptive_rate_control_throttle_wait_duration_ms",
        MetricType::ObservableCounterU64,
    )
    .description("Cumulative time HTTP requests spent waiting on the adaptive throttle gate for this upstream origin")
    .unit("ms")
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
            "adaptive_rate_control_effective_limit" => observe_metric!(
                metrics.rate_limiter_metric(HttpRateLimiterMetrics::adaptive_effective_limit)
            ),
            "adaptive_rate_control_admission_coefficient_permille" => {
                observe_metric!(metrics.rate_limiter_metric(
                    HttpRateLimiterMetrics::adaptive_admission_coefficient_permille
                ))
            }
            "adaptive_rate_control_throttled_total" => observe_metric!(
                metrics.rate_limiter_metric(HttpRateLimiterMetrics::adaptive_throttled_total)
            ),
            "adaptive_rate_control_throttle_wait_duration_ms" => {
                observe_metric!(metrics.rate_limiter_metric(
                    HttpRateLimiterMetrics::adaptive_throttle_wait_duration_ms_total
                ))
            }
            _ => None,
        }
    }
}

fn should_observe_metrics(metric_source: Option<&HttpRateControlMetricSource>) -> bool {
    metric_source.is_none_or(HttpRateControlMetricSource::is_owner)
}

#[must_use]
pub fn parameter_specs() -> [ParameterSpec; 8] {
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
        ParameterSpec::runtime("adaptive_rate_control")
            .description("Client-side adaptive throttling that lowers the effective HTTP request rate when the upstream origin fails or times out, then raises it again as the origin recovers, scaling within the configured static rate limits. Overrides runtime.params.http_adaptive_rate_control when set. Values: 'disabled' (default) or 'enabled'."),
        ParameterSpec::runtime("adaptive_rate_control_magnitude")
            .description("Throttling magnitude for adaptive rate control: the Google SRE client-side throttling coefficient K (a number greater than 1). Sets where the effective rate settles for a given failure rate; throttling begins only when the success rate falls below 1/K, so a larger value tolerates a higher failure rate before throttling. Overrides runtime.params.http_adaptive_rate_control_magnitude when set. Applies only when adaptive_rate_control is enabled. Defaults to 2.0."),
        ParameterSpec::runtime("adaptive_rate_control_elasticity")
            .description("Throttling elasticity for adaptive rate control: the half-life of the outcome-decay window as a duration such as '10s'. Sets how fast the controller reacts to and recovers from a change in the failure rate; a shorter value reacts and recovers faster, a longer value is smoother and slower. Overrides runtime.params.http_adaptive_rate_control_elasticity when set. Applies only when adaptive_rate_control is enabled. Defaults to 10s."),
    ]
}

/// Resolve a component's rate-control configuration from its own parameters,
/// falling back to the matching `runtime.params` key.
///
/// # Errors
/// Returns an invalid-configuration error when a `max_concurrent_requests`,
/// `requests_per_second_limit` or `requests_per_minute_limit` value does not
/// parse as a non-zero integer, or a `rate_control_jitter_min` /
/// `rate_control_jitter_max` value does not parse as a duration.
pub fn resolve_config_for_component<S: BuildHasher>(
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
        adaptive_rate_control: parse_adaptive_rate_control_param(
            params,
            runtime_params,
            connector_component,
            dataconnector,
        )?,
    };

    let config = with_jitter(
        params,
        runtime_params,
        connector_component,
        dataconnector,
        config,
    )?;

    ensure_adaptive_has_static_limit(&config, connector_component, dataconnector)?;
    Ok(config)
}

/// Reject an origin that enables adaptive rate control but defines no static
/// rate limit for it to adjust.
///
/// Adaptive control is a modifier on a defined limit, never a limiter of its
/// own, so there must be at least one of `requests_per_second_limit`,
/// `requests_per_minute_limit`, or `max_concurrent_requests` (in either the
/// dataset-level or `runtime.params.http_*` form) for it to scale.
///
/// # Errors
/// Returns an invalid-configuration error when adaptive control is enabled and
/// the origin has no static rate limit configured.
fn ensure_adaptive_has_static_limit(
    config: &HttpRateControlConfig,
    connector_component: &ConnectorComponent,
    dataconnector: &'static str,
) -> DataConnectorResult<()> {
    if config.adaptive_enabled() && config.static_limit_reference().is_none() {
        return Err(DataConnectorError::InvalidConfigurationNoSource {
            dataconnector: dataconnector.to_string(),
            connector_component: connector_component.clone(),
            message:
                "`adaptive_rate_control` adjusts a defined rate limit, but no rate limit is set for this origin. \
                Set at least one of `requests_per_second_limit`, `requests_per_minute_limit`, or `max_concurrent_requests` \
                (as a dataset parameter or the matching `runtime.params.http_*` value), or remove `adaptive_rate_control`. \
                See: https://spiceai.org/docs/components/data-connectors/http"
                    .to_string(),
        });
    }

    Ok(())
}

/// Resolve the three `adaptive_rate_control*` parameters (each falling back to
/// its matching `runtime.params.http_*` default) into an [`AdaptiveRateControl`].
///
/// The on/off switch is `adaptive_rate_control`; the two hyperparameters are
/// `adaptive_rate_control_magnitude` (SRE coefficient `k > 1`, default 2.0) and
/// `adaptive_rate_control_elasticity` (decay-window half-life, default 10s). The
/// two hyperparameters are inert when adaptive control is disabled, so a
/// runtime-wide magnitude/elasticity default applies only to the datasets that
/// enable adaptive control.
///
/// # Errors
/// Returns an invalid-configuration error for an `adaptive_rate_control` value
/// other than `disabled`/`enabled`, a magnitude that is not a number greater than
/// 1, or an elasticity that is not a positive, finite duration.
fn parse_adaptive_rate_control_param<S: BuildHasher>(
    params: &Parameters,
    runtime_params: Option<&HashMap<String, String, S>>,
    connector_component: &ConnectorComponent,
    dataconnector: &'static str,
) -> DataConnectorResult<AdaptiveRateControl> {
    let mode = resolve_adaptive_mode(params, runtime_params, connector_component, dataconnector)?;
    if mode == AdaptiveMode::Disabled {
        // The magnitude/elasticity knobs are meaningless without the feature on,
        // so they stay inert here rather than erroring — a runtime-wide default
        // must not fail every dataset that leaves adaptive control off.
        return Ok(AdaptiveRateControl::Disabled);
    }

    let magnitude = parse_optional_f64_param(
        params,
        runtime_params,
        connector_component,
        dataconnector,
        "adaptive_rate_control_magnitude",
        RUNTIME_ADAPTIVE_RATE_CONTROL_MAGNITUDE,
    )?
    .unwrap_or(DEFAULT_ADAPTIVE_MAGNITUDE);
    let elasticity = parse_optional_duration_param(
        params,
        runtime_params,
        connector_component,
        dataconnector,
        "adaptive_rate_control_elasticity",
        RUNTIME_ADAPTIVE_RATE_CONTROL_ELASTICITY,
    )?
    .unwrap_or(DEFAULT_ADAPTIVE_ELASTICITY);

    AdaptiveRateControl::enabled(magnitude, elasticity).map_err(|error| {
        let (param_name, runtime_param_name, detail) = match error {
            AdaptiveRateControlParseError::MagnitudeInvalid { magnitude } => (
                "adaptive_rate_control_magnitude",
                RUNTIME_ADAPTIVE_RATE_CONTROL_MAGNITUDE,
                format!(
                    "the throttling magnitude must be a number greater than 1, but got {magnitude}. A larger value tolerates a higher failure rate before throttling. Use a value such as '2.0'."
                ),
            ),
            AdaptiveRateControlParseError::ElasticityInvalid { .. } => (
                "adaptive_rate_control_elasticity",
                RUNTIME_ADAPTIVE_RATE_CONTROL_ELASTICITY,
                "the throttling elasticity must be a positive, finite duration such as '10s'. A shorter value reacts and recovers faster.".to_string(),
            ),
            // The on/off switch was already resolved by `resolve_adaptive_mode`.
            AdaptiveRateControlParseError::UnrecognizedMode { value } => (
                "adaptive_rate_control",
                RUNTIME_ADAPTIVE_RATE_CONTROL,
                format!("'{value}' is not a valid value. Use 'disabled' or 'enabled'."),
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

/// Resolve the `adaptive_rate_control` on/off switch, falling back to
/// `runtime.params.http_adaptive_rate_control`, defaulting to disabled.
fn resolve_adaptive_mode<S: BuildHasher>(
    params: &Parameters,
    runtime_params: Option<&HashMap<String, String, S>>,
    connector_component: &ConnectorComponent,
    dataconnector: &'static str,
) -> DataConnectorResult<AdaptiveMode> {
    let (raw_value, display_name) =
        if let Some(raw_value) = params.get("adaptive_rate_control").expose().ok() {
            (
                raw_value,
                params.user_param("adaptive_rate_control").to_string(),
            )
        } else if let Some(raw_value) = runtime_params
            .and_then(|runtime_params| runtime_params.get(RUNTIME_ADAPTIVE_RATE_CONTROL))
            .map(String::as_str)
        {
            (
                raw_value,
                format!("runtime.params.{RUNTIME_ADAPTIVE_RATE_CONTROL}"),
            )
        } else {
            return Ok(AdaptiveMode::Disabled);
        };

    parse_adaptive_mode(raw_value).map_err(|_| DataConnectorError::InvalidConfigurationNoSource {
        dataconnector: dataconnector.to_string(),
        connector_component: connector_component.clone(),
        message: format!(
            "The '{display_name}' parameter is invalid: '{}' is not a valid value. Use 'disabled' or 'enabled'. See: https://spiceai.org/docs/components/data-connectors/http",
            raw_value.trim()
        ),
    })
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

    /// The per-origin rate limiter, built on first use with `config`'s adaptive
    /// controller. The controller adjusts the origin's configured static limit,
    /// recovering back toward [`HttpRateControlConfig::static_limit_reference`].
    /// One origin gets one limiter; the first component to register it fixes its
    /// adaptive strategy. A component whose full rate-control config differs is
    /// rejected later by [`Self::reserve_shared_rate_controller_for_component`].
    pub async fn shared_rate_limiter_for_config(
        &self,
        base_url: &Url,
        config: &HttpRateControlConfig,
    ) -> Arc<HttpRateLimiter> {
        let key = rate_control_key(base_url);
        let rate_limiters = self.rate_limiters.read().await;
        if let Some(rate_limiter) = rate_limiters.get(&key) {
            return Arc::clone(rate_limiter);
        }

        drop(rate_limiters);
        let adaptive_control = config.adaptive_rate_control;
        // The reference ceiling is only consulted when a controller is built,
        // which config validation guarantees requires a static limit; the
        // fallback is the minimum ceiling and never reached for an enabled control.
        let ceiling = config.static_limit_reference().unwrap_or(1.0);
        let mut rate_limiters = self.rate_limiters.write().await;
        Arc::clone(
            rate_limiters.entry(key).or_insert_with(|| {
                Arc::new(HttpRateLimiter::with_adaptive(adaptive_control, ceiling))
            }),
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

/// Parse an optional floating-point parameter, falling back to its
/// `runtime.params.http_*` default. Domain bounds (e.g. magnitude `> 1`) are
/// enforced by the caller; this only rejects an unparseable value.
fn parse_optional_f64_param<S: BuildHasher>(
    params: &Parameters,
    runtime_params: Option<&HashMap<String, String, S>>,
    connector_component: &ConnectorComponent,
    dataconnector: &'static str,
    parameter_name: &str,
    runtime_parameter_name: &'static str,
) -> DataConnectorResult<Option<f64>> {
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

    trimmed
        .parse::<f64>()
        .map(Some)
        .map_err(|source| DataConnectorError::InvalidConfiguration {
            dataconnector: dataconnector.to_string(),
            message: format!("The '{display_name}' parameter must be a number, such as '2.0'."),
            connector_component: connector_component.clone(),
            source: source.into(),
        })
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

    fundu::parse_duration(trimmed).map(Some).map_err(|source| {
        DataConnectorError::InvalidConfiguration {
            dataconnector: dataconnector.to_string(),
            message: format!(
                "The '{display_name}' parameter must be a valid duration such as '10ms', '1s', or '0ms'."
            ),
            connector_component: connector_component.clone(),
            source: source.into(),
        }
    })
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
            "Multiple HTTP-based components target {key} with different rate-control settings. Use the same max_concurrent_requests, requests_per_second_limit, requests_per_minute_limit, rate_control_jitter_min, and rate_control_jitter_max values for components sharing an origin."
        ),
    })
}

fn duration_millis_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[expect(
    clippy::cast_precision_loss,
    reason = "adaptive ceiling only needs approximate magnitude, not exact representation"
)]
fn usize_to_f64(value: usize) -> f64 {
    value as f64
}

fn persisted_instance_ttl(refresh_interval: Duration) -> Duration {
    refresh_interval
        .saturating_mul(3)
        .max(MIN_PERSISTED_INSTANCE_TTL)
}

#[cfg(test)]
mod tests {
    use super::*;
    use runtime_component::dataset::DatasetSpec;
    use std::num::NonZeroU32;
    use std::sync::Arc;

    fn test_component() -> ConnectorComponent {
        ConnectorComponent::Dataset(Arc::new(DatasetSpec::new(
            "https://origin.example.com/data",
            "adaptive_rate_control_test".into(),
        )))
    }

    fn adaptive_config(
        adaptive: AdaptiveRateControl,
        requests_per_second: Option<u32>,
    ) -> HttpRateControlConfig {
        HttpRateControlConfig {
            max_concurrent_requests: None,
            requests_per_second: requests_per_second
                .map(|rps| NonZeroU32::new(rps).expect("test rps must be non-zero")),
            requests_per_minute: None,
            jitter_min: Duration::ZERO,
            jitter_max: Duration::ZERO,
            adaptive_rate_control: adaptive,
        }
    }

    #[test]
    fn adaptive_enabled_without_a_static_limit_is_a_config_error() {
        let component = test_component();
        let adaptive = AdaptiveRateControl::enabled(2.0, Duration::from_secs(10))
            .expect("test control should be valid");
        let error =
            ensure_adaptive_has_static_limit(&adaptive_config(adaptive, None), &component, "https")
                .expect_err("adaptive control with no static rate limit must be rejected");

        match error {
            DataConnectorError::InvalidConfigurationNoSource { message, .. } => {
                assert!(
                    message.contains("adaptive_rate_control"),
                    "message must name the parameter: {message}"
                );
                assert!(
                    message.contains("requests_per_second_limit")
                        && message.contains("max_concurrent_requests"),
                    "message must name the fix: {message}"
                );
                assert!(
                    message.contains("spiceai.org/docs"),
                    "message must include a docs link: {message}"
                );
            }
            other => panic!("expected an invalid-configuration error, got {other:?}"),
        }
    }

    #[test]
    fn adaptive_enabled_with_a_static_limit_is_accepted() {
        let component = test_component();
        let adaptive = AdaptiveRateControl::enabled(3.0, Duration::from_secs(10))
            .expect("test control should be valid");
        ensure_adaptive_has_static_limit(&adaptive_config(adaptive, Some(10)), &component, "https")
            .expect("adaptive control with a static limit is a valid configuration");
    }

    #[test]
    fn disabled_adaptive_needs_no_static_limit() {
        ensure_adaptive_has_static_limit(
            &adaptive_config(AdaptiveRateControl::Disabled, None),
            &test_component(),
            "https",
        )
        .expect("disabled adaptive control requires no static limit");
    }

    #[test]
    fn static_limit_reference_is_the_max_configured_limit() {
        let mut config = HttpRateControlConfig::disabled();
        assert_eq!(config.static_limit_reference(), None);

        config.requests_per_second = NonZeroU32::new(5);
        config.max_concurrent_requests = Some(20);
        assert_eq!(config.static_limit_reference(), Some(20.0));
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
