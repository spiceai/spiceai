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

use std::collections::HashMap;
use std::hash::BuildHasher;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, RwLock as StdRwLock};
use std::time::Duration;

use crate::component::ComponentType;
use crate::component::dataset::Dataset;
use crate::component::metrics::{MetricSpec, MetricType, MetricsProvider, ObserveMetricCallback};
use crate::dataconnector::{ConnectorComponent, DataConnectorError, DataConnectorResult};
use crate::parameters::{ParameterSpec, Parameters};
use data_components::rate_limit::{HttpRateLimiter, HttpRateLimiterMetrics};
use governor::Quota;
use opentelemetry::KeyValue;
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

static HTTP_RATE_LIMITERS: LazyLock<RwLock<HashMap<String, Arc<HttpRateLimiter>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

static HTTP_RATE_CONTROLLERS: LazyLock<RwLock<HashMap<String, SharedRateController>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

static HTTP_RATE_CONTROL_METRICS_BY_ORIGIN: LazyLock<
    StdRwLock<HashMap<String, Arc<HttpRateControlMetrics>>>,
> = LazyLock::new(|| StdRwLock::new(HashMap::new()));

static HTTP_RATE_CONTROL_METRIC_OWNERS: LazyLock<StdRwLock<HashMap<String, String>>> =
    LazyLock::new(|| StdRwLock::new(HashMap::new()));

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpRateControlConfig {
    pub max_concurrent_requests: Option<usize>,
    pub requests_per_second: Option<NonZeroU32>,
    pub requests_per_minute: Option<NonZeroU32>,
    pub jitter_min: Duration,
    pub jitter_max: Duration,
}

impl HttpRateControlConfig {
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.max_concurrent_requests.is_some()
            || self.requests_per_second.is_some()
            || self.requests_per_minute.is_some()
            || !self.jitter_min.is_zero()
            || !self.jitter_max.is_zero()
    }
}

#[derive(Clone, Debug)]
pub struct SharedRateController {
    pub config: HttpRateControlConfig,
    pub controller: Option<Arc<RateController>>,
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

const HTTP_RATE_CONTROL_METRIC_SPECS: &[MetricSpec] = &[
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
];

#[derive(Debug, Clone)]
pub struct HttpRateControlMetricsProvider {
    connector_name: &'static str,
    metrics: Arc<HttpRateControlMetrics>,
}

impl HttpRateControlMetricsProvider {
    #[must_use]
    pub fn new(connector_name: &'static str, metrics: Arc<HttpRateControlMetrics>) -> Self {
        Self {
            connector_name,
            metrics,
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
        match metric.name {
            "inflight_operations" => Some(ObserveMetricCallback::U64(Box::new(move |observer| {
                observer.observe(
                    metrics.rate_controller_metric(RateControllerMetrics::inflight_permits),
                    &attributes,
                );
            }))),
            "rate_control_max_concurrent_requests" => {
                Some(ObserveMetricCallback::U64(Box::new(move |observer| {
                    observer.observe(metrics.max_concurrent_requests(), &attributes);
                })))
            }
            "rate_control_requests_per_second_limit" => {
                Some(ObserveMetricCallback::U64(Box::new(move |observer| {
                    observer.observe(metrics.requests_per_second_limit(), &attributes);
                })))
            }
            "rate_control_requests_per_minute_limit" => {
                Some(ObserveMetricCallback::U64(Box::new(move |observer| {
                    observer.observe(metrics.requests_per_minute_limit(), &attributes);
                })))
            }
            "rate_control_jitter_min_ms" => {
                Some(ObserveMetricCallback::U64(Box::new(move |observer| {
                    observer.observe(
                        metrics.rate_control_jitter_min_ms.load(Ordering::Relaxed),
                        &attributes,
                    );
                })))
            }
            "rate_control_jitter_max_ms" => {
                Some(ObserveMetricCallback::U64(Box::new(move |observer| {
                    observer.observe(
                        metrics.rate_control_jitter_max_ms.load(Ordering::Relaxed),
                        &attributes,
                    );
                })))
            }
            "rate_control_available_permits" => {
                Some(ObserveMetricCallback::U64(Box::new(move |observer| {
                    observer.observe(metrics.available_permits(), &attributes);
                })))
            }
            "rate_control_acquisitions_total" => {
                Some(ObserveMetricCallback::U64(Box::new(move |observer| {
                    observer.observe(
                        metrics
                            .rate_controller_metric(RateControllerMetrics::permits_acquired_total),
                        &attributes,
                    );
                })))
            }
            "rate_control_acquire_errors_total" => {
                Some(ObserveMetricCallback::U64(Box::new(move |observer| {
                    observer.observe(
                        metrics.rate_controller_metric(RateControllerMetrics::acquire_errors_total),
                        &attributes,
                    );
                })))
            }
            "rate_control_wait_duration_ms" => {
                Some(ObserveMetricCallback::U64(Box::new(move |observer| {
                    observer.observe(
                        metrics
                            .rate_controller_metric(RateControllerMetrics::wait_duration_ms_total),
                        &attributes,
                    );
                })))
            }
            "rate_limit_retry_after_updates_total" => {
                Some(ObserveMetricCallback::U64(Box::new(move |observer| {
                    observer.observe(
                        metrics
                            .rate_limiter_metric(HttpRateLimiterMetrics::retry_after_updates_total),
                        &attributes,
                    );
                })))
            }
            "rate_limit_retry_after_waits_total" => {
                Some(ObserveMetricCallback::U64(Box::new(move |observer| {
                    observer.observe(
                        metrics
                            .rate_limiter_metric(HttpRateLimiterMetrics::retry_after_waits_total),
                        &attributes,
                    );
                })))
            }
            "rate_limit_retry_after_wait_duration_ms" => {
                Some(ObserveMetricCallback::U64(Box::new(move |observer| {
                    observer.observe(
                        metrics.rate_limiter_metric(
                            HttpRateLimiterMetrics::retry_after_wait_duration_ms_total,
                        ),
                        &attributes,
                    );
                })))
            }
            "rate_limit_retry_after_remaining_ms" => {
                Some(ObserveMetricCallback::U64(Box::new(move |observer| {
                    observer.observe(
                        metrics
                            .rate_limiter_metric(HttpRateLimiterMetrics::retry_after_remaining_ms),
                        &attributes,
                    );
                })))
            }
            _ => None,
        }
    }
}

#[must_use]
pub fn parameter_specs() -> [ParameterSpec; 5] {
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
    ]
}

pub fn resolve_config<S: BuildHasher>(
    params: &Parameters,
    runtime_params: Option<&HashMap<String, String, S>>,
    dataset: &Dataset,
    dataconnector: &'static str,
) -> DataConnectorResult<HttpRateControlConfig> {
    let config = HttpRateControlConfig {
        max_concurrent_requests: parse_optional_nonzero_usize_param(
            params,
            runtime_params,
            dataset,
            dataconnector,
            "max_concurrent_requests",
            RUNTIME_MAX_CONCURRENT_REQUESTS,
        )?,
        requests_per_second: parse_optional_nonzero_u32_param(
            params,
            runtime_params,
            dataset,
            dataconnector,
            "requests_per_second_limit",
            RUNTIME_REQUESTS_PER_SECOND_LIMIT,
        )?,
        requests_per_minute: parse_optional_nonzero_u32_param(
            params,
            runtime_params,
            dataset,
            dataconnector,
            "requests_per_minute_limit",
            RUNTIME_REQUESTS_PER_MINUTE_LIMIT,
        )?,
        jitter_min: Duration::ZERO,
        jitter_max: Duration::ZERO,
    };

    with_jitter(params, runtime_params, dataset, dataconnector, config)
}

pub async fn shared_rate_limiter(base_url: &Url) -> Arc<HttpRateLimiter> {
    let key = rate_control_key(base_url);
    let rate_limiters = HTTP_RATE_LIMITERS.read().await;
    if let Some(rate_limiter) = rate_limiters.get(&key) {
        return Arc::clone(rate_limiter);
    }

    drop(rate_limiters);
    let mut rate_limiters = HTTP_RATE_LIMITERS.write().await;
    Arc::clone(
        rate_limiters
            .entry(key)
            .or_insert_with(|| Arc::new(HttpRateLimiter::new())),
    )
}

#[must_use]
pub fn shared_metrics(base_url: &Url) -> Arc<HttpRateControlMetrics> {
    let key = rate_control_key(base_url);
    let metrics_by_origin = HTTP_RATE_CONTROL_METRICS_BY_ORIGIN.read().ok();
    if let Some(metrics) = metrics_by_origin
        .as_ref()
        .and_then(|metrics_by_origin| metrics_by_origin.get(&key))
    {
        return Arc::clone(metrics);
    }
    drop(metrics_by_origin);

    let Ok(mut metrics_by_origin) = HTTP_RATE_CONTROL_METRICS_BY_ORIGIN.write() else {
        return Arc::new(HttpRateControlMetrics::default());
    };

    Arc::clone(
        metrics_by_origin
            .entry(key)
            .or_insert_with(|| Arc::new(HttpRateControlMetrics::default())),
    )
}

pub fn claim_metrics_owner(base_url: &Url, owner: &str) -> bool {
    let key = rate_control_key(base_url);
    let Ok(mut metric_owners) = HTTP_RATE_CONTROL_METRIC_OWNERS.write() else {
        return false;
    };

    if let Some(existing_owner) = metric_owners.get(&key) {
        existing_owner == owner
    } else {
        metric_owners.insert(key, owner.to_string());
        true
    }
}

pub async fn shared_rate_controller(
    base_url: &Url,
    config: &HttpRateControlConfig,
    dataset: &Dataset,
    dataconnector: &'static str,
) -> DataConnectorResult<SharedRateController> {
    let key = rate_control_key(base_url);
    let rate_controllers = HTTP_RATE_CONTROLLERS.read().await;
    if let Some(existing) = rate_controllers.get(&key) {
        return resolve_existing_controller(existing, config, dataset, dataconnector, &key);
    }

    drop(rate_controllers);
    let mut rate_controllers = HTTP_RATE_CONTROLLERS.write().await;
    if let Some(existing) = rate_controllers.get(&key) {
        return resolve_existing_controller(existing, config, dataset, dataconnector, &key);
    }

    if !config.is_enabled() {
        let shared = SharedRateController {
            config: config.clone(),
            controller: None,
        };
        rate_controllers.insert(key, shared.clone());
        return Ok(shared);
    }

    let mut builder = RateController::builder()
        .with_jitter(JitterConfig::new(config.jitter_min, config.jitter_max));
    if let Some(max_concurrent_requests) = config.max_concurrent_requests {
        builder = builder.with_max_concurrent_requests(max_concurrent_requests);
    }
    if let Some(requests_per_second) = config.requests_per_second {
        builder = builder.add_quota(Quota::per_second(requests_per_second));
    }
    if let Some(requests_per_minute) = config.requests_per_minute {
        builder = builder.add_quota(Quota::per_minute(requests_per_minute));
    }

    let controller = builder.build();
    let shared = SharedRateController {
        config: config.clone(),
        controller: Some(controller),
    };
    rate_controllers.insert(key, shared.clone());

    Ok(shared)
}

fn resolve_existing_controller(
    existing: &SharedRateController,
    config: &HttpRateControlConfig,
    dataset: &Dataset,
    dataconnector: &'static str,
    key: &str,
) -> DataConnectorResult<SharedRateController> {
    if existing.config == *config || (existing.config.is_enabled() && !config.is_enabled()) {
        return Ok(existing.clone());
    }

    conflicting_config_error(dataset, dataconnector, key)
}

fn parse_optional_nonzero_u32_param<S: BuildHasher>(
    params: &Parameters,
    runtime_params: Option<&HashMap<String, String, S>>,
    dataset: &Dataset,
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
                connector_component: ConnectorComponent::from(dataset),
                source: source.into(),
            })?;

    NonZeroU32::new(value).map(Some).ok_or_else(|| {
        DataConnectorError::InvalidConfigurationNoSource {
            dataconnector: dataconnector.to_string(),
            connector_component: ConnectorComponent::from(dataset),
            message: format!("The '{display_name}' parameter must be greater than 0."),
        }
    })
}

fn parse_optional_nonzero_usize_param<S: BuildHasher>(
    params: &Parameters,
    runtime_params: Option<&HashMap<String, String, S>>,
    dataset: &Dataset,
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
                connector_component: ConnectorComponent::from(dataset),
                source: source.into(),
            })?;

    if value == 0 {
        return Err(DataConnectorError::InvalidConfigurationNoSource {
            dataconnector: dataconnector.to_string(),
            connector_component: ConnectorComponent::from(dataset),
            message: format!("The '{display_name}' parameter must be greater than 0."),
        });
    }

    Ok(Some(value))
}

fn parse_optional_duration_param<S: BuildHasher>(
    params: &Parameters,
    runtime_params: Option<&HashMap<String, String, S>>,
    dataset: &Dataset,
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
            connector_component: ConnectorComponent::from(dataset),
            source: source.into(),
        }
    })
}

fn with_jitter<S: BuildHasher>(
    params: &Parameters,
    runtime_params: Option<&HashMap<String, String, S>>,
    dataset: &Dataset,
    dataconnector: &'static str,
    mut config: HttpRateControlConfig,
) -> DataConnectorResult<HttpRateControlConfig> {
    let jitter_min = parse_optional_duration_param(
        params,
        runtime_params,
        dataset,
        dataconnector,
        "rate_control_jitter_min",
        RUNTIME_RATE_CONTROL_JITTER_MIN,
    )?;
    let jitter_max = parse_optional_duration_param(
        params,
        runtime_params,
        dataset,
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
            connector_component: ConnectorComponent::from(dataset),
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

fn conflicting_config_error<T>(
    dataset: &Dataset,
    dataconnector: &'static str,
    key: &str,
) -> DataConnectorResult<T> {
    Err(DataConnectorError::InvalidConfigurationNoSource {
        dataconnector: dataconnector.to_string(),
        connector_component: ConnectorComponent::from(dataset),
        message: format!(
            "Multiple HTTP-based datasets target {key} with different rate-control settings. Use the same max_concurrent_requests, requests_per_second_limit, requests_per_minute_limit, rate_control_jitter_min, and rate_control_jitter_max values for datasets sharing an origin."
        ),
    })
}

fn duration_millis_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}
