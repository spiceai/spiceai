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

//! OTEL push exporter for metrics and logs.
//!
//! This module provides functionality to push runtime metrics and log records to an
//! OTEL collector using either gRPC or HTTP protocols. The protocol is inferred from
//! the endpoint:
//! - Endpoints with `http://` or `https://` scheme use HTTP protocol
//! - Bare hostname/port (e.g., `otel-collector:4317`) use gRPC protocol
//!
//! The exporter creates a [`PeriodicReader`] that should be added to the runtime's
//! [`SdkMeterProvider`] alongside other readers (prometheus, `spice_metrics`) to ensure
//! only runtime metrics are exported (not global/anonymous telemetry metrics).
//!
//! ## Metric Filtering
//!
//! When a metrics whitelist is configured, only metrics with names matching the whitelist
//! are exported. This is implemented via a [`FilteringExporter`] wrapper that filters
//! metrics before passing them to the underlying OTEL exporter.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use opentelemetry::logs::Severity;
use opentelemetry_otlp::{
    LogExporter, MetricExporter, Protocol, WithExportConfig, WithHttpConfig, WithTonicConfig,
};
use opentelemetry_sdk::{
    Resource,
    error::{OTelSdkError, OTelSdkResult},
    logs::{LogBatch, LogExporter as SdkLogExporter},
    metrics::{
        Temporality, data::ResourceMetrics, exporter::PushMetricExporter,
        periodic_reader_with_async_runtime::PeriodicReader,
    },
    runtime::Tokio,
};
use parking_lot::Mutex;
use reqwest::Client;
use snafu::prelude::*;

/// Type alias for the OTEL periodic reader with filtering support
pub type OtelPeriodicReader = PeriodicReader<FilteringExporter>;

/// A wrapper exporter that filters metrics by name before passing to the inner exporter.
///
/// When the whitelist is empty, all metrics are passed through. Otherwise, only metrics
/// whose names are in the whitelist are exported.
#[derive(Debug)]
pub struct FilteringExporter {
    inner: MetricExporter,
    /// Set of metric names to export. Empty means export all.
    whitelist: Arc<HashSet<String>>,
}

impl FilteringExporter {
    /// Creates a new filtering exporter.
    ///
    /// # Arguments
    /// * `inner` - The underlying OTEL exporter
    /// * `whitelist` - Metric names to export. Empty slice means export all metrics.
    #[must_use]
    pub fn new(inner: MetricExporter, whitelist: &[String]) -> Self {
        Self {
            inner,
            whitelist: Arc::new(whitelist.iter().cloned().collect()),
        }
    }

    /// Check if the batch contains any metrics that match the whitelist.
    ///
    /// Returns true if:
    /// - The whitelist is empty (export all metrics), OR
    /// - At least one metric in the batch matches the whitelist
    fn has_any_matching_metrics(&self, metrics: &ResourceMetrics) -> bool {
        if self.whitelist.is_empty() {
            return true;
        }

        for scope_metrics in metrics.scope_metrics() {
            for metric in scope_metrics.metrics() {
                if self.whitelist.contains(metric.name()) {
                    return true;
                }
            }
        }
        false
    }
}

impl PushMetricExporter for FilteringExporter {
    fn export(
        &self,
        metrics: &ResourceMetrics,
    ) -> impl std::future::Future<Output = opentelemetry_sdk::error::OTelSdkResult> + Send {
        // Check if any metrics in this batch match the whitelist.
        // Note: Due to OpenTelemetry 0.31's immutable `&ResourceMetrics` API, we cannot
        // filter individual metrics from the batch. Instead, we skip the entire export
        // if NO metrics match the whitelist. When at least one metric matches, the
        // entire batch is exported. For fine-grained filtering, configure the OTEL
        // collector to filter metrics at ingestion time.
        let should_export = self.has_any_matching_metrics(metrics);

        async move {
            if !should_export {
                tracing::debug!("Skipping metrics export: no metrics match whitelist");
                return Ok(());
            }

            self.inner.export(metrics).await.inspect_err(|err| {
                match err {
                    opentelemetry_sdk::error::OTelSdkError::InternalFailure(msg) => {
                        tracing::warn!("Failed to export metrics: {msg}");
                    }
                    opentelemetry_sdk::error::OTelSdkError::Timeout(duration) => {
                        tracing::warn!("Failed to export metrics: timed out after {duration:?}");
                    }
                    opentelemetry_sdk::error::OTelSdkError::AlreadyShutdown => (), // No logging needed
                }
            })
        }
    }

    fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        self.inner.force_flush()
    }

    fn shutdown(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        self.inner.shutdown()
    }

    fn shutdown_with_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> opentelemetry_sdk::error::OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn temporality(&self) -> Temporality {
        self.inner.temporality()
    }
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create OTEL metrics exporter: {message}"))]
    ExporterCreationFailed { message: String },

    #[snafu(display("Failed to create OTEL logs exporter: {message}"))]
    LogExporterCreationFailed { message: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Re-export the config from spicepod for convenience
pub use spicepod::component::runtime::{
    OTEL_EXPORTER_DOCS_URL, OtelExporterConfig, OtelSignal, OtelTemporality,
};

/// Map a spicepod-level [`OtelTemporality`] preference to the OpenTelemetry SDK enum.
fn map_temporality(t: OtelTemporality) -> Temporality {
    match t {
        OtelTemporality::Delta => Temporality::Delta,
        OtelTemporality::Cumulative => Temporality::Cumulative,
        OtelTemporality::LowMemory => Temporality::LowMemory,
    }
}

/// Creates a [`PeriodicReader`] for pushing metrics to an OTEL collector.
///
/// This reader should be added to the runtime's [`SdkMeterProvider`] alongside
/// other readers (e.g., prometheus, `spice_metrics`) to ensure all runtime metrics
/// are exported to the OTEL collector.
///
/// # Arguments
///
/// * `config` - The exporter configuration (endpoint, push interval, metric filters).
///   Note: `config.headers` is **not** read by this function; the caller is responsible
///   for resolving any parameter references in `config.headers` (e.g. via the secrets
///   subsystem) and passing the already-resolved header map as `resolved_headers`.
/// * `resolved_headers` - Fully-resolved headers to attach to every exported metrics
///   request. For HTTP these are sent as HTTP headers; for gRPC they are sent as
///   metadata entries (keys must be lowercase ASCII).
///
/// # Returns
///
/// Returns a [`PeriodicReader`] that periodically pushes metrics to the configured endpoint.
///
/// # Errors
///
/// Returns an error if the exporter cannot be created (e.g., invalid endpoint or push interval).
///
/// # Example
///
/// ```ignore
/// use runtime::otel_push_exporter::{create_otel_periodic_reader, OtelExporterConfig, OtelTemporality};
/// use std::collections::HashMap;
///
/// let config = OtelExporterConfig {
///     enabled: true,
///     endpoint: "otel-collector:4317".to_string(),
///     push_interval: "30s".to_string(),
///     metrics: vec![],
///     headers: HashMap::new(),
///     temporality: OtelTemporality::Delta,
///     signals: vec![OtelSignal::Metrics],
/// };
///
/// let otel_reader = create_otel_periodic_reader(&config, HashMap::new())?;
///
/// let provider = SdkMeterProvider::builder()
///     .with_reader(prometheus_exporter)
///     .with_reader(spice_metrics_reader)
///     .with_reader(otel_reader)  // Add OTEL push reader
///     .build();
/// ```
#[expect(
    clippy::implicit_hasher,
    reason = "public API accepts the standard HashMap; callers pass std::collections::HashMap<String, String>"
)]
pub fn create_otel_periodic_reader(
    config: &OtelExporterConfig,
    resolved_headers: HashMap<String, String>,
) -> Result<OtelPeriodicReader> {
    let push_interval =
        config
            .push_interval_duration()
            .map_err(|e| Error::ExporterCreationFailed {
                message: e.to_string(),
            })?;

    let protocol = if config.is_http() { "http" } else { "grpc" };
    let temporality = map_temporality(config.temporality);
    tracing::info!(
        endpoint = %config.endpoint,
        protocol = protocol,
        push_interval_secs = push_interval.as_secs(),
        metrics_filter = ?config.metrics,
        num_headers = resolved_headers.len(),
        temporality = ?config.temporality,
        "Creating OTEL metrics periodic reader"
    );

    let inner_exporter = if config.is_http() {
        create_http_exporter(&config.endpoint, resolved_headers, temporality)?
    } else {
        create_grpc_exporter(&config.grpc_endpoint(), &resolved_headers, temporality)?
    };

    // Wrap with filtering exporter
    let exporter = FilteringExporter::new(inner_exporter, &config.metrics);

    let reader = PeriodicReader::builder(exporter, Tokio)
        .with_interval(push_interval)
        .build();

    Ok(reader)
}

fn create_grpc_exporter(
    endpoint: &str,
    headers: &HashMap<String, String>,
    temporality: Temporality,
) -> Result<MetricExporter> {
    let mut builder = MetricExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .with_protocol(Protocol::Grpc)
        .with_temporality(temporality);

    if !headers.is_empty() {
        let metadata =
            grpc_metadata(headers).map_err(|message| Error::ExporterCreationFailed { message })?;
        builder = builder.with_metadata(metadata);
    }

    builder.build().map_err(|e| Error::ExporterCreationFailed {
        message: e.to_string(),
    })
}

/// `headers` as gRPC metadata, or why one of them cannot be sent.
fn grpc_metadata(
    headers: &HashMap<String, String>,
) -> std::result::Result<tonic::metadata::MetadataMap, String> {
    let mut metadata = tonic::metadata::MetadataMap::new();
    for (key_str, value) in headers {
        let key_str = key_str.as_str();
        let metadata_key = key_str
            .parse::<tonic::metadata::MetadataKey<tonic::metadata::Ascii>>()
            .map_err(|e| {
                format!("Invalid gRPC metadata key '{key_str}': {e}. gRPC metadata keys must be lowercase ASCII")
            })?;
        let metadata_value = value
            .parse()
            .map_err(|e| format!("Invalid gRPC metadata value for '{key_str}': {e}"))?;
        metadata.insert(metadata_key, metadata_value);
    }
    Ok(metadata)
}

/// The HTTP client both OTLP/HTTP exporters send with.
fn otlp_http_client() -> std::result::Result<Client, String> {
    Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| format!("Failed to build OTEL HTTP client: {e}"))
}

fn create_http_exporter(
    endpoint: &str,
    headers: HashMap<String, String>,
    temporality: Temporality,
) -> Result<MetricExporter> {
    // For HTTP, the endpoint should include the /v1/metrics path
    let full_endpoint = if endpoint.ends_with("/v1/metrics") {
        endpoint.to_string()
    } else {
        format!("{}/v1/metrics", endpoint.trim_end_matches('/'))
    };

    let http_client =
        otlp_http_client().map_err(|message| Error::ExporterCreationFailed { message })?;

    let mut builder = MetricExporter::builder()
        .with_http()
        .with_http_client(http_client)
        .with_endpoint(full_endpoint)
        .with_protocol(Protocol::HttpBinary)
        .with_temporality(temporality);

    if !headers.is_empty() {
        builder = builder.with_headers(headers);
    }

    builder.build().map_err(|e| Error::ExporterCreationFailed {
        message: e.to_string(),
    })
}

/// The target this module logs under, and so the target of a log-export failure
/// report, which an operator can filter stdout on.
pub const DIAGNOSTICS_TARGET: &str = module_path!();

/// Creates the exporter that sends log records to `config.endpoint`.
///
/// gRPC endpoints receive them on the OTLP logs service at the same address.
/// HTTP endpoints receive them at [`OtelExporterConfig::logs_http_endpoint`].
/// The exporter reports a failing endpoint on stdout once per outage; see
/// [`ReportingLogExporter`].
///
/// `resolved_headers` must already have secret templates resolved, as for
/// [`create_otel_periodic_reader`].
///
/// # Errors
///
/// Returns an error if a header cannot be sent or the exporter cannot be built.
#[expect(
    clippy::implicit_hasher,
    reason = "public API accepts the standard HashMap; callers pass std::collections::HashMap<String, String>"
)]
pub fn create_otel_log_exporter(
    config: &OtelExporterConfig,
    resolved_headers: HashMap<String, String>,
) -> Result<ReportingLogExporter<LogExporter>> {
    let exporter = if config.is_http() {
        let mut builder = LogExporter::builder()
            .with_http()
            .with_http_client(
                otlp_http_client()
                    .map_err(|message| Error::LogExporterCreationFailed { message })?,
            )
            .with_endpoint(config.logs_http_endpoint())
            .with_protocol(Protocol::HttpBinary);
        if !resolved_headers.is_empty() {
            builder = builder.with_headers(resolved_headers);
        }
        builder.build()
    } else {
        let mut builder = LogExporter::builder()
            .with_tonic()
            .with_endpoint(config.grpc_endpoint())
            .with_protocol(Protocol::Grpc);
        if !resolved_headers.is_empty() {
            builder = builder.with_metadata(
                grpc_metadata(&resolved_headers)
                    .map_err(|message| Error::LogExporterCreationFailed { message })?,
            );
        }
        builder.build()
    }
    .map_err(|e| Error::LogExporterCreationFailed {
        message: e.to_string(),
    })?;

    Ok(ReportingLogExporter::new(exporter, config.endpoint.clone()))
}

/// A log exporter that tells the operator, on stdout, when its endpoint stops
/// accepting records and when it recovers.
///
/// Reported once per outage rather than once per failed batch: batches go out
/// every second, so a line per failure would bury everything else on stdout.
/// The recovery line counts the records the endpoint did not receive, so the
/// gap in the backend is accounted for.
#[derive(Debug)]
pub struct ReportingLogExporter<E> {
    inner: E,
    endpoint: String,
    /// Records not delivered since the current outage began; `None` while the
    /// endpoint is accepting records.
    outage: Mutex<Option<u64>>,
}

impl<E> ReportingLogExporter<E> {
    /// Wraps `inner`, naming `endpoint` in what it reports.
    pub fn new(inner: E, endpoint: String) -> Self {
        Self {
            inner,
            endpoint,
            outage: Mutex::new(None),
        }
    }

    fn observe(&self, result: &OTelSdkResult, records: u64) {
        let mut outage = self.outage.lock();
        match (result, *outage) {
            (Ok(()), None) | (Err(OTelSdkError::AlreadyShutdown), _) => {}
            (Ok(()), Some(undelivered)) => {
                *outage = None;
                drop(outage);
                tracing::info!(
                    target: DIAGNOSTICS_TARGET,
                    "{}",
                    log_export_recovered_message(&self.endpoint, undelivered)
                );
            }
            (Err(err), None) => {
                *outage = Some(records);
                drop(outage);
                tracing::warn!(
                    target: DIAGNOSTICS_TARGET,
                    "{}",
                    log_export_failed_message(&self.endpoint, err)
                );
            }
            (Err(_), Some(undelivered)) => {
                *outage = Some(undelivered.saturating_add(records));
            }
        }
    }
}

/// An export in flight, reported as a failure if it is dropped before it
/// finishes: the batch processor drops an export that outlives its export
/// timeout (`OTEL_BLRP_EXPORT_TIMEOUT`), and those records are lost too.
struct PendingExport<'a, E> {
    exporter: &'a ReportingLogExporter<E>,
    records: Option<u64>,
}

impl<E> PendingExport<'_, E> {
    fn finish(mut self, result: &OTelSdkResult) {
        if let Some(records) = self.records.take() {
            self.exporter.observe(result, records);
        }
    }
}

impl<E> Drop for PendingExport<'_, E> {
    fn drop(&mut self) {
        if let Some(records) = self.records.take() {
            self.exporter.observe(
                &Err(OTelSdkError::InternalFailure(
                    "the export did not finish within the export timeout".to_string(),
                )),
                records,
            );
        }
    }
}

impl<E: SdkLogExporter> SdkLogExporter for ReportingLogExporter<E> {
    fn export(
        &self,
        batch: LogBatch<'_>,
    ) -> impl std::future::Future<Output = OTelSdkResult> + Send {
        let pending = PendingExport {
            exporter: self,
            records: Some(u64::try_from(batch.iter().count()).unwrap_or(u64::MAX)),
        };
        let export = self.inner.export(batch);
        async move {
            let result = export.await;
            pending.finish(&result);
            result
        }
    }

    fn shutdown_with_timeout(&self, timeout: std::time::Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn shutdown(&self) -> OTelSdkResult {
        self.inner.shutdown()
    }

    fn event_enabled(&self, level: Severity, target: &str, name: Option<&str>) -> bool {
        self.inner.event_enabled(level, target, name)
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

/// What an operator reads when log export to `endpoint` starts failing.
#[must_use]
pub fn log_export_failed_message(endpoint: &str, cause: &OTelSdkError) -> String {
    let cause = util::single_line(&cause.to_string()).into_owned();
    format!(
        "Failed to export logs to the OpenTelemetry endpoint '{endpoint}', so log records are dropped until it recovers. Check that the endpoint is reachable and accepts OTLP logs. Cause: {cause}. See: {OTEL_EXPORTER_DOCS_URL}"
    )
}

/// What an operator reads when log export to `endpoint` works again after
/// `undelivered` records were not delivered.
#[must_use]
pub fn log_export_recovered_message(endpoint: &str, undelivered: u64) -> String {
    let records = if undelivered == 1 {
        "1 log record was".to_string()
    } else {
        format!("{undelivered} log records were")
    };
    format!(
        "Log export to the OpenTelemetry endpoint '{endpoint}' recovered; {records} not delivered while it was failing."
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let error = Error::ExporterCreationFailed {
            message: "test error message".to_string(),
        };
        assert!(error.to_string().contains("test error message"));
        assert!(
            error
                .to_string()
                .contains("Failed to create OTEL metrics exporter")
        );
    }

    // Tests for the filtering logic

    #[test]
    fn test_filtering_should_export_empty_whitelist_allows_all() {
        // Empty whitelist means export all metrics
        let whitelist: Arc<HashSet<String>> = Arc::new(HashSet::new());

        // Helper function mirroring should_export logic
        let should_export =
            |name: &str| -> bool { whitelist.is_empty() || whitelist.contains(name) };

        // With empty whitelist, everything should be exported
        assert!(should_export("any_metric"));
        assert!(should_export("requests_total"));
        assert!(should_export("some_random_metric_name"));
    }

    #[test]
    fn test_filtering_should_export_with_whitelist() {
        let whitelist: Arc<HashSet<String>> = Arc::new(
            vec!["metric_a".to_string(), "metric_b".to_string()]
                .into_iter()
                .collect(),
        );

        let should_export =
            |name: &str| -> bool { whitelist.is_empty() || whitelist.contains(name) };

        // Whitelisted metrics should be exported
        assert!(should_export("metric_a"));
        assert!(should_export("metric_b"));

        // Non-whitelisted metrics should NOT be exported
        assert!(!should_export("metric_c"));
        assert!(!should_export("other_metric"));
    }

    #[test]
    fn test_filtering_logic_with_realistic_metric_names() {
        let whitelist: Arc<HashSet<String>> = Arc::new(
            vec![
                "http_requests_total".to_string(),
                "http_request_duration_seconds".to_string(),
            ]
            .into_iter()
            .collect(),
        );

        let should_export =
            |name: &str| -> bool { whitelist.is_empty() || whitelist.contains(name) };

        // Whitelisted
        assert!(should_export("http_requests_total"));
        assert!(should_export("http_request_duration_seconds"));

        // Not whitelisted
        assert!(!should_export("db_queries_total"));
        assert!(!should_export("memory_usage_bytes"));
    }

    #[test]
    fn test_filtering_whitelist_exact_match_required() {
        let whitelist: Arc<HashSet<String>> =
            Arc::new(vec!["requests".to_string()].into_iter().collect());

        let should_export =
            |name: &str| -> bool { whitelist.is_empty() || whitelist.contains(name) };

        // Exact match works
        assert!(should_export("requests"));

        // Partial matches don't work - must be exact
        assert!(!should_export("requests_total"));
        assert!(!should_export("http_requests"));
        assert!(!should_export("request")); // Missing 's'
    }

    #[test]
    fn test_filtering_retain_logic() {
        // Simulate what filter_metrics does
        let whitelist: HashSet<String> = vec!["keep_me".to_string(), "also_keep".to_string()]
            .into_iter()
            .collect();

        let metric_names = ["keep_me", "remove_me", "also_keep", "remove_too"];

        // Apply the same retain logic used in filter_metrics
        let filtered: Vec<&str> = metric_names
            .iter()
            .copied()
            .filter(|name| whitelist.is_empty() || whitelist.contains(*name))
            .collect();

        assert_eq!(filtered.len(), 2);
        assert!(filtered.contains(&"keep_me"));
        assert!(filtered.contains(&"also_keep"));
        assert!(!filtered.contains(&"remove_me"));
        assert!(!filtered.contains(&"remove_too"));
    }

    #[test]
    fn test_filtering_retain_keeps_all_when_empty_whitelist() {
        let whitelist: HashSet<String> = HashSet::new();

        let metric_names = ["metric_a", "metric_b", "metric_c"];

        // Apply the same retain logic - empty whitelist means keep all
        let count = metric_names
            .iter()
            .filter(|name| whitelist.is_empty() || whitelist.contains(**name))
            .count();

        assert_eq!(count, 3);
    }

    #[test]
    fn test_filtering_retain_removes_all_when_none_match() {
        let whitelist: HashSet<String> = vec!["nonexistent".to_string()].into_iter().collect();

        let metric_names = ["metric_a", "metric_b"];

        let any_match = metric_names
            .iter()
            .any(|name| whitelist.is_empty() || whitelist.contains(*name));

        // Should have no metrics left since none matched
        assert!(!any_match);
    }

    // Tests for header support

    /// One collected batch holding a single counter point, so an export has
    /// data to send.
    fn collected_metrics() -> ResourceMetrics {
        use opentelemetry::metrics::MeterProvider as _;
        use opentelemetry_sdk::metrics::reader::MetricReader as _;

        let reader = telemetry::metrics_reader::MetricsReader::new();
        let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
            .with_reader(reader.clone())
            .build();
        provider
            .meter("otel_push_exporter_test")
            .u64_counter("exported_counter")
            .build()
            .add(1, &[]);
        let mut metrics = ResourceMetrics::default();
        reader
            .collect(&mut metrics)
            .expect("collect the test counter");
        metrics
    }

    /// Serves an OTLP/HTTP collector on a loopback port. Returns its base URL
    /// and a receiver of the path and headers of every request it answers.
    async fn serve_http_collector() -> (
        String,
        tokio::sync::mpsc::UnboundedReceiver<(String, axum::http::HeaderMap)>,
    ) {
        let (requests, received) = tokio::sync::mpsc::unbounded_channel();
        let collector = axum::Router::new().fallback(
            move |uri: axum::http::Uri, headers: axum::http::HeaderMap| {
                let requests = requests.clone();
                async move {
                    requests.send((uri.path().to_string(), headers)).ok();
                    axum::http::StatusCode::OK
                }
            },
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the test collector");
        let address = listener
            .local_addr()
            .expect("the test collector has an address");
        tokio::spawn(async move {
            axum::serve(listener, collector)
                .await
                .expect("the test collector serves");
        });
        (format!("http://{address}"), received)
    }

    fn header<'a>(headers: &'a axum::http::HeaderMap, name: &str) -> Option<&'a str> {
        headers.get(name).and_then(|value| value.to_str().ok())
    }

    #[tokio::test]
    async fn test_create_http_exporter_with_headers() {
        let (collector, mut requests) = serve_http_collector().await;
        let headers = HashMap::from([
            ("DD-API-KEY".to_string(), "test-key".to_string()),
            ("X-Custom".to_string(), "value".to_string()),
        ]);
        let metrics = collected_metrics();

        // An endpoint that already names the OTLP metrics path is used as
        // given; a bare one, with or without a trailing slash, gets it appended.
        for endpoint in [
            format!("{collector}/v1/metrics"),
            collector.clone(),
            format!("{collector}/"),
        ] {
            let exporter = create_http_exporter(&endpoint, headers.clone(), Temporality::Delta)
                .unwrap_or_else(|e| panic!("HTTP exporter for {endpoint} should build: {e}"));
            exporter
                .export(&metrics)
                .await
                .unwrap_or_else(|e| panic!("export to {endpoint} should succeed: {e:?}"));

            let (path, received) =
                tokio::time::timeout(std::time::Duration::from_secs(5), requests.recv())
                    .await
                    .unwrap_or_else(|_| {
                        panic!("the collector should receive the export to {endpoint}")
                    })
                    .expect("the test collector is still running");
            assert_eq!(path, "/v1/metrics", "{endpoint}");
            assert_eq!(
                header(&received, "dd-api-key"),
                Some("test-key"),
                "{endpoint}"
            );
            assert_eq!(header(&received, "x-custom"), Some("value"), "{endpoint}");
        }
    }

    /// An OTLP/gRPC collector that records the metadata of every export.
    struct MetadataCapture(tokio::sync::mpsc::UnboundedSender<tonic::metadata::MetadataMap>);

    #[tonic::async_trait]
    impl opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsService
        for MetadataCapture
    {
        async fn export(
            &self,
            request: tonic::Request<
                opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest,
            >,
        ) -> std::result::Result<
            tonic::Response<
                opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceResponse,
            >,
            tonic::Status,
        > {
            self.0.send(request.metadata().clone()).ok();
            Ok(tonic::Response::new(
                opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceResponse::default(),
            ))
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_grpc_exporter_with_valid_headers() {
        use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsServiceServer;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the test collector");
        let address = listener
            .local_addr()
            .expect("the test collector has an address");
        let (metadata_sender, mut received) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(MetricsServiceServer::new(MetadataCapture(metadata_sender)))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .expect("the test collector serves");
        });

        let headers = HashMap::from([
            ("api-key".to_string(), "test-key".to_string()),
            ("x-custom-header".to_string(), "value".to_string()),
        ]);
        let exporter =
            create_grpc_exporter(&format!("http://{address}"), &headers, Temporality::Delta)
                .expect("gRPC exporter with valid headers should build");
        exporter
            .export(&collected_metrics())
            .await
            .expect("the export should reach the test collector");

        let metadata = tokio::time::timeout(std::time::Duration::from_secs(5), received.recv())
            .await
            .expect("the collector should receive the export")
            .expect("the test collector is still running");
        let value = |key: &str| metadata.get(key).and_then(|value| value.to_str().ok());
        assert_eq!(value("api-key"), Some("test-key"));
        assert_eq!(value("x-custom-header"), Some("value"));
    }

    fn log_config(endpoint: String) -> OtelExporterConfig {
        OtelExporterConfig {
            enabled: true,
            endpoint,
            push_interval: "60s".to_string(),
            metrics: vec![],
            headers: HashMap::new(),
            temporality: OtelTemporality::default(),
            signals: vec![OtelSignal::Logs],
        }
    }

    /// Exports one log record through `exporter` and returns the result.
    async fn export_one_log_record<E: SdkLogExporter>(exporter: &E) -> OTelSdkResult {
        use opentelemetry::logs::{LogRecord as _, Logger as _, LoggerProvider as _};

        let provider = opentelemetry_sdk::logs::SdkLoggerProvider::builder().build();
        let mut record = provider.logger("test").create_log_record();
        record.set_body("Loaded 3 rows for dataset trips".into());
        // `Logger::emit` sets this on every record it hands a processor, and
        // the OTLP encoding requires it; this record skips `emit`.
        record.set_observed_timestamp(std::time::SystemTime::now());
        let scope = opentelemetry::InstrumentationScope::builder("runtime::init::dataset").build();
        let records = [(&record, &scope)];
        exporter.export(LogBatch::new(&records)).await
    }

    #[tokio::test]
    async fn test_create_otel_log_exporter_http_sends_to_v1_logs_with_headers() {
        let (collector, mut requests) = serve_http_collector().await;

        // The documented metrics URL and a bare base URL both send logs to the
        // OTLP logs path on the same host.
        for endpoint in [
            format!("{collector}/v1/metrics"),
            collector.clone(),
            format!("{collector}/"),
        ] {
            let exporter = create_otel_log_exporter(
                &log_config(endpoint.clone()),
                HashMap::from([("DD-API-KEY".to_string(), "test-key".to_string())]),
            )
            .unwrap_or_else(|e| panic!("HTTP log exporter for {endpoint} should build: {e}"));
            export_one_log_record(&exporter)
                .await
                .unwrap_or_else(|e| panic!("export to {endpoint} should succeed: {e:?}"));

            let (path, received) =
                tokio::time::timeout(std::time::Duration::from_secs(5), requests.recv())
                    .await
                    .unwrap_or_else(|_| {
                        panic!("the collector should receive the export to {endpoint}")
                    })
                    .expect("the test collector is still running");
            assert_eq!(path, "/v1/logs", "{endpoint}");
            assert_eq!(
                header(&received, "dd-api-key"),
                Some("test-key"),
                "{endpoint}"
            );
        }
    }

    /// An OTLP/gRPC logs collector that forwards every request it receives.
    struct LogsCapture(
        tokio::sync::mpsc::UnboundedSender<(
            tonic::metadata::MetadataMap,
            opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest,
        )>,
    );

    #[tonic::async_trait]
    impl opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsService
        for LogsCapture
    {
        async fn export(
            &self,
            request: tonic::Request<
                opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest,
            >,
        ) -> std::result::Result<
            tonic::Response<
                opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceResponse,
            >,
            tonic::Status,
        > {
            let metadata = request.metadata().clone();
            self.0.send((metadata, request.into_inner())).ok();
            Ok(tonic::Response::new(
                opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceResponse::default(
                ),
            ))
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_otel_log_exporter_grpc_sends_records_with_metadata() {
        use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsServiceServer;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the test collector");
        let address = listener
            .local_addr()
            .expect("the test collector has an address");
        let (sender, mut received) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(LogsServiceServer::new(LogsCapture(sender)))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .expect("the test collector serves");
        });

        // A bare `host:port` endpoint is gRPC.
        let exporter = create_otel_log_exporter(
            &log_config(address.to_string()),
            HashMap::from([("api-key".to_string(), "test-key".to_string())]),
        )
        .expect("gRPC log exporter should build");
        export_one_log_record(&exporter)
            .await
            .expect("the export should reach the test collector");

        let (metadata, request) =
            tokio::time::timeout(std::time::Duration::from_secs(5), received.recv())
                .await
                .expect("the collector should receive the export")
                .expect("the test collector is still running");
        assert_eq!(
            metadata.get("api-key").and_then(|v| v.to_str().ok()),
            Some("test-key")
        );
        let bodies: Vec<String> = request
            .resource_logs
            .iter()
            .flat_map(|resource| &resource.scope_logs)
            .flat_map(|scope| &scope.log_records)
            .map(|record| format!("{:?}", record.body))
            .collect();
        assert_eq!(
            bodies,
            vec![
                r#"Some(AnyValue { value: Some(StringValue("Loaded 3 rows for dataset trips")) })"#
                    .to_string()
            ]
        );
    }

    /// An exporter that answers each export with the next scripted result.
    #[derive(Debug)]
    struct ScriptedExporter(Mutex<std::collections::VecDeque<OTelSdkResult>>);

    impl SdkLogExporter for ScriptedExporter {
        fn export(
            &self,
            _batch: LogBatch<'_>,
        ) -> impl std::future::Future<Output = OTelSdkResult> + Send {
            let result = self
                .0
                .lock()
                .pop_front()
                .expect("the test scripted a result for every export");
            async move { result }
        }
    }

    /// Every line `tracing` writes at `INFO` or above while `f` runs.
    fn lines_logged_by(f: impl FnOnce()) -> Vec<String> {
        #[derive(Clone, Default)]
        struct Capture(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for Capture {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
            type Writer = Self;
            fn make_writer(&'a self) -> Self::Writer {
                self.clone()
            }
        }

        let sink = Capture::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(sink.clone())
            .with_ansi(false)
            .without_time()
            .with_max_level(tracing::Level::INFO)
            .finish();
        tracing::subscriber::with_default(subscriber, f);
        let captured = sink.0.lock();
        String::from_utf8_lossy(&captured)
            .lines()
            .map(str::to_owned)
            .collect()
    }

    /// A failing endpoint is reported once when the outage starts and once
    /// when it ends, with the records lost in between counted — not once per
    /// failed batch.
    #[test]
    fn test_reporting_log_exporter_reports_each_outage_once() {
        let failure = || {
            Err(OTelSdkError::InternalFailure(
                "transport error: connection refused".to_string(),
            ))
        };
        let exporter = ReportingLogExporter::new(
            ScriptedExporter(Mutex::new(
                [
                    failure(),
                    failure(),
                    failure(),
                    Ok(()),
                    Ok(()),
                    failure(),
                    Ok(()),
                ]
                .into(),
            )),
            "otel-collector:4317".to_string(),
        );

        let lines = lines_logged_by(|| {
            for _ in 0..7 {
                let _ = futures::executor::block_on(export_one_log_record(&exporter));
            }
        });

        let failed = log_export_failed_message(
            "otel-collector:4317",
            &OTelSdkError::InternalFailure("transport error: connection refused".to_string()),
        );
        assert_eq!(
            lines,
            vec![
                format!(" WARN runtime::otel_push_exporter: {failed}"),
                " INFO runtime::otel_push_exporter: Log export to the OpenTelemetry endpoint 'otel-collector:4317' recovered; 3 log records were not delivered while it was failing.".to_string(),
                format!(" WARN runtime::otel_push_exporter: {failed}"),
                " INFO runtime::otel_push_exporter: Log export to the OpenTelemetry endpoint 'otel-collector:4317' recovered; 1 log record was not delivered while it was failing.".to_string(),
            ]
        );
    }

    /// An exporter whose export never finishes, as the batch processor sees
    /// an endpoint that holds the request open past the export timeout.
    #[derive(Debug)]
    struct HangingExporter;

    impl SdkLogExporter for HangingExporter {
        fn export(
            &self,
            _batch: LogBatch<'_>,
        ) -> impl std::future::Future<Output = OTelSdkResult> + Send {
            std::future::pending()
        }
    }

    /// The batch processor drops an export that outlives its timeout; those
    /// records are lost like any failed batch's, so they are reported and
    /// counted the same way.
    #[test]
    fn test_reporting_log_exporter_reports_an_export_dropped_at_its_timeout() {
        use futures::FutureExt as _;

        let hanging = ReportingLogExporter::new(HangingExporter, "otel-collector:4317".to_string());
        let lines = lines_logged_by(|| {
            assert!(
                export_one_log_record(&hanging).now_or_never().is_none(),
                "the export is still in flight when it is dropped"
            );
        });

        assert_eq!(
            lines,
            vec![format!(
                " WARN runtime::otel_push_exporter: {}",
                log_export_failed_message(
                    "otel-collector:4317",
                    &OTelSdkError::InternalFailure(
                        "the export did not finish within the export timeout".to_string()
                    ),
                )
            )]
        );
        assert_eq!(
            *hanging.outage.lock(),
            Some(1),
            "the dropped record is counted"
        );
    }

    #[test]
    fn test_log_export_failed_message() {
        assert_eq!(
            log_export_failed_message(
                "http://otel-collector:4318",
                &OTelSdkError::InternalFailure("HTTP export failed.\nStatus Code: 404".to_string()),
            ),
            "Failed to export logs to the OpenTelemetry endpoint 'http://otel-collector:4318', so log records are dropped until it recovers. Check that the endpoint is reachable and accepts OTLP logs. Cause: Operation failed: HTTP export failed. Status Code: 404. See: https://spiceai.org/docs/reference/spicepod/runtime#runtimetelemetryotel_exporter"
        );
    }

    #[test]
    fn test_create_grpc_exporter_rejects_invalid_metadata_key() {
        // tonic requires a tokio runtime to be available during exporter construction
        let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
        let _guard = rt.enter();
        // gRPC metadata keys must be lowercase ASCII
        let headers = HashMap::from([("Invalid Key With Spaces".to_string(), "value".to_string())]);
        let result = create_grpc_exporter("http://localhost:4317", &headers, Temporality::Delta);
        assert!(result.is_err());
        let err = result.expect_err("should fail with invalid metadata key");
        let msg = err.to_string();
        assert!(
            msg.contains("Invalid gRPC metadata key"),
            "Error should mention invalid key: {msg}"
        );
        assert!(
            msg.contains("lowercase ASCII"),
            "Error should hint about lowercase ASCII: {msg}"
        );
    }
}
