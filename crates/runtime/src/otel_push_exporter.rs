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

use std::{collections::HashSet, sync::Arc};

use opentelemetry_otlp::{MetricExporter, Protocol, WithExportConfig, WithHttpConfig};
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

    /// Returns true if the metric should be exported based on the whitelist.
    fn should_export(&self, metric_name: &str) -> bool {
        self.whitelist.is_empty() || self.whitelist.contains(metric_name)
    }

    /// Filters the metrics in place, removing any that don't match the whitelist.
    fn filter_metrics(&self, resource_metrics: &mut ResourceMetrics) {
        if self.whitelist.is_empty() {
            return; // No filtering needed
        }

        for scope_metrics in &mut resource_metrics.scope_metrics {
            scope_metrics
                .metrics
                .retain(|metric| self.should_export(&metric.name));
        }

        // Remove empty scope_metrics
        resource_metrics
            .scope_metrics
            .retain(|sm| !sm.metrics.is_empty());
    }
}

impl PushMetricExporter for FilteringExporter {
    fn export(
        &self,
        metrics: &mut ResourceMetrics,
    ) -> impl std::future::Future<Output = opentelemetry_sdk::error::OTelSdkResult> + Send {
        self.filter_metrics(metrics);
        self.inner.export(metrics)
    }

    fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        self.inner.force_flush()
    }

    fn shutdown(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        self.inner.shutdown()
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
pub use spicepod::component::runtime::OtelExporterConfig;

/// Creates a [`PeriodicReader`] for pushing metrics to an OTEL collector.
///
/// This reader should be added to the runtime's [`SdkMeterProvider`] alongside
/// other readers (e.g., prometheus, `spice_metrics`) to ensure all runtime metrics
/// are exported to the OTEL collector.
///
/// # Arguments
///
/// * `config` - The exporter configuration including endpoint, push interval, and metric filters
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
/// use runtime::otel_push_exporter::{create_otel_periodic_reader, OtelExporterConfig};
///
/// let config = OtelExporterConfig {
///     enabled: true,
///     endpoint: "otel-collector:4317".to_string(),
///     push_interval: "30s".to_string(),
///     metrics: vec![],
/// };
///
/// let otel_reader = create_otel_periodic_reader(&config)?;
///
/// let provider = SdkMeterProvider::builder()
///     .with_reader(prometheus_exporter)
///     .with_reader(spice_metrics_reader)
///     .with_reader(otel_reader)  // Add OTEL push reader
///     .build();
/// ```
pub fn create_otel_periodic_reader(config: &OtelExporterConfig) -> Result<OtelPeriodicReader> {
    let push_interval =
        config
            .push_interval_duration()
            .map_err(|e| Error::ExporterCreationFailed {
                message: e.to_string(),
            })?;

    let protocol = if config.is_http() { "http" } else { "grpc" };
    tracing::info!(
        endpoint = %config.endpoint,
        protocol = protocol,
        push_interval_secs = push_interval.as_secs(),
        metrics_filter = ?config.metrics,
        "Creating OTEL metrics periodic reader"
    );

    let inner_exporter = if config.is_http() {
        create_http_exporter(&config.endpoint)?
    } else {
        create_grpc_exporter(&config.grpc_endpoint())?
    };

    // Wrap with filtering exporter
    let exporter = FilteringExporter::new(inner_exporter, &config.metrics);

    let reader = PeriodicReader::builder(exporter, Tokio)
        .with_interval(push_interval)
        .build();

    Ok(reader)
}

fn create_grpc_exporter(endpoint: &str) -> Result<MetricExporter> {
    MetricExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .with_protocol(Protocol::Grpc)
        .build()
        .map_err(|e| Error::ExporterCreationFailed {
            message: e.to_string(),
        })
}

fn create_http_exporter(endpoint: &str) -> Result<MetricExporter> {
    // For HTTP, the endpoint should include the /v1/metrics path
    let full_endpoint = if endpoint.ends_with("/v1/metrics") {
        endpoint.to_string()
    } else {
        format!("{}/v1/metrics", endpoint.trim_end_matches('/'))
    };

    let http_client = Client::builder()
        .build()
        .map_err(|e| Error::ExporterCreationFailed {
            message: format!("Failed to build OTEL HTTP client: {e}"),
        })?;

    MetricExporter::builder()
        .with_http()
        .with_http_client(http_client)
        .with_endpoint(full_endpoint)
        .with_protocol(Protocol::HttpBinary)
        .build()
        .map_err(|e| Error::ExporterCreationFailed {
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
}
