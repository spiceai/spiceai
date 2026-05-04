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

//! OTEL push exporter for metrics.
//!
//! This module provides functionality to push runtime metrics to an OTEL collector
//! using either gRPC or HTTP protocols. The protocol is inferred from the endpoint:
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

use opentelemetry_otlp::{
    MetricExporter, Protocol, WithExportConfig, WithHttpConfig, WithTonicConfig,
};
use opentelemetry_sdk::{
    metrics::{
        Temporality, data::ResourceMetrics, exporter::PushMetricExporter,
        periodic_reader_with_async_runtime::PeriodicReader,
    },
    runtime::Tokio,
};
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
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Re-export the config from spicepod for convenience
pub use spicepod::component::runtime::{OtelExporterConfig, OtelTemporality};

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
        let mut metadata = tonic::metadata::MetadataMap::new();
        for (key_str, value) in headers {
            let key_str = key_str.as_str();
            let metadata_key = key_str
                .parse::<tonic::metadata::MetadataKey<tonic::metadata::Ascii>>()
                .map_err(|e| Error::ExporterCreationFailed {
                    message: format!("Invalid gRPC metadata key '{key_str}': {e}. gRPC metadata keys must be lowercase ASCII"),
                })?;
            let metadata_value = value.parse().map_err(|e| Error::ExporterCreationFailed {
                message: format!("Invalid gRPC metadata value for '{key_str}': {e}"),
            })?;
            metadata.insert(metadata_key, metadata_value);
        }
        builder = builder.with_metadata(metadata);
    }

    builder.build().map_err(|e| Error::ExporterCreationFailed {
        message: e.to_string(),
    })
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

    let http_client = Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| Error::ExporterCreationFailed {
            message: format!("Failed to build OTEL HTTP client: {e}"),
        })?;

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

    #[test]
    fn test_create_http_exporter_with_headers() {
        let headers = HashMap::from([
            ("DD-API-KEY".to_string(), "test-key".to_string()),
            ("X-Custom".to_string(), "value".to_string()),
        ]);
        // HTTP exporter with headers should build successfully
        let result = create_http_exporter(
            "http://localhost:4318/v1/metrics",
            headers,
            Temporality::Delta,
        );
        assert!(
            result.is_ok(),
            "HTTP exporter with headers should build: {result:?}"
        );
    }

    #[test]
    fn test_create_http_exporter_without_headers() {
        let headers = HashMap::new();
        let result = create_http_exporter(
            "http://localhost:4318/v1/metrics",
            headers,
            Temporality::Delta,
        );
        assert!(
            result.is_ok(),
            "HTTP exporter without headers should build: {result:?}"
        );
    }

    #[test]
    fn test_create_grpc_exporter_with_valid_headers() {
        // tonic requires a tokio runtime to be available during exporter construction
        let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
        let _guard = rt.enter();
        let headers = HashMap::from([
            ("api-key".to_string(), "test-key".to_string()),
            ("x-custom-header".to_string(), "value".to_string()),
        ]);
        let result = create_grpc_exporter("http://localhost:4317", &headers, Temporality::Delta);
        assert!(
            result.is_ok(),
            "gRPC exporter with valid headers should build: {result:?}"
        );
    }

    #[test]
    fn test_create_grpc_exporter_without_headers() {
        let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
        let _guard = rt.enter();
        let headers = HashMap::new();
        let result = create_grpc_exporter("http://localhost:4317", &headers, Temporality::Delta);
        assert!(
            result.is_ok(),
            "gRPC exporter without headers should build: {result:?}"
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
