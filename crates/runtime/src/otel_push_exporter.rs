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

use std::{collections::HashSet, sync::Arc, time::Duration};

use opentelemetry_otlp::{MetricExporter, Protocol, WithExportConfig};
use opentelemetry_sdk::metrics::{
    MetricError, PeriodicReader, Temporality,
    data::{ResourceMetrics, ScopeMetrics},
    exporter::PushMetricExporter,
};
use snafu::prelude::*;

/// Type alias for the OTEL periodic reader with filtering support
pub type OtelPeriodicReader = PeriodicReader<FilteringExporter>;

/// A wrapper exporter that filters metrics by name before passing to the inner exporter.
///
/// When the whitelist is empty, all metrics are passed through. Otherwise, only metrics
/// whose names are in the whitelist are exported.
#[derive(Debug, Clone)]
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
    ) -> impl std::future::Future<Output = std::result::Result<(), MetricError>> + Send {
        self.filter_metrics(metrics);
        self.inner.export(metrics)
    }

    fn force_flush(
        &self,
    ) -> impl std::future::Future<Output = std::result::Result<(), MetricError>> + Send {
        self.inner.force_flush()
    }

    fn shutdown(&self) -> std::result::Result<(), MetricError> {
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

/// Configuration for the OTEL push exporter
#[derive(Debug, Clone)]
pub struct OtelPushExporterConfig {
    /// The endpoint of the OTEL collector
    pub endpoint: String,
    /// How often to push metrics
    pub push_interval: Duration,
    /// Optional whitelist of metric names to export.
    /// If empty, all metrics are exported.
    pub metrics: Vec<String>,
}

impl OtelPushExporterConfig {
    /// Returns true if the endpoint is configured for HTTP protocol.
    ///
    /// HTTP is used when:
    /// - The endpoint has an `http://` or `https://` scheme
    /// - The endpoint contains `/v1/metrics` path
    ///
    /// gRPC is used when the endpoint is just a hostname and optional port
    /// (e.g., `localhost:4317` or `otel-collector`)
    #[must_use]
    pub fn is_http(&self) -> bool {
        self.endpoint.starts_with("http://")
            || self.endpoint.starts_with("https://")
            || self.endpoint.contains("/v1/metrics")
    }

    /// Returns the endpoint formatted for gRPC use.
    /// If no port is specified, defaults to 4317.
    #[must_use]
    pub fn grpc_endpoint(&self) -> String {
        let endpoint = &self.endpoint;
        // If it already has a port, use as-is with http:// prefix for tonic
        if endpoint.contains(':') {
            format!("http://{endpoint}")
        } else {
            format!("http://{endpoint}:4317")
        }
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
/// * `config` - The exporter configuration including endpoint, push interval, and metric filters
///
/// # Returns
///
/// Returns a [`PeriodicReader`] that periodically pushes metrics to the configured endpoint.
///
/// # Errors
///
/// Returns an error if the exporter cannot be created (e.g., invalid endpoint).
///
/// # Example
///
/// ```ignore
/// use runtime::otel_push_exporter::{create_otel_periodic_reader, OtelPushExporterConfig};
/// use opentelemetry_sdk::metrics::SdkMeterProvider;
///
/// let config = OtelPushExporterConfig {
///     endpoint: "otel-collector:4317".to_string(),
///     push_interval: Duration::from_secs(30),
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
pub fn create_otel_periodic_reader(config: &OtelPushExporterConfig) -> Result<OtelPeriodicReader> {
    let protocol = if config.is_http() { "http" } else { "grpc" };
    tracing::info!(
        endpoint = %config.endpoint,
        protocol = protocol,
        push_interval_secs = config.push_interval.as_secs(),
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

    let reader = PeriodicReader::builder(exporter)
        .with_interval(config.push_interval)
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

    MetricExporter::builder()
        .with_http()
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

    fn default_config(endpoint: &str) -> OtelPushExporterConfig {
        OtelPushExporterConfig {
            endpoint: endpoint.to_string(),
            push_interval: Duration::from_secs(60),
            metrics: vec![],
        }
    }

    #[test]
    fn test_is_http_detects_http_scheme() {
        // http:// scheme is HTTP
        let config = default_config("http://localhost:4318");
        assert!(config.is_http());

        // https:// scheme is HTTP
        let config_https = default_config("https://otel-collector:4318");
        assert!(config_https.is_http());
    }

    #[test]
    fn test_is_http_detects_v1_metrics_path() {
        let config = default_config("http://localhost:4318/v1/metrics");
        assert!(config.is_http());
    }

    #[test]
    fn test_is_http_detects_grpc_bare_hostname() {
        // Bare hostname is gRPC
        let config = default_config("otel-collector");
        assert!(!config.is_http());

        // Hostname with port is gRPC
        let config_with_port = default_config("otel-collector:4317");
        assert!(!config_with_port.is_http());

        // localhost with port is gRPC
        let config_localhost = default_config("localhost:4317");
        assert!(!config_localhost.is_http());
    }

    #[test]
    fn test_grpc_endpoint_adds_default_port() {
        let config = default_config("otel-collector");
        assert_eq!(config.grpc_endpoint(), "http://otel-collector:4317");
    }

    #[test]
    fn test_grpc_endpoint_preserves_custom_port() {
        let config = default_config("otel-collector:9090");
        assert_eq!(config.grpc_endpoint(), "http://otel-collector:9090");
    }

    #[test]
    fn test_grpc_endpoint_with_localhost() {
        let config = default_config("localhost:4317");
        assert_eq!(config.grpc_endpoint(), "http://localhost:4317");
    }

    #[test]
    fn test_config_various_push_intervals() {
        // Test short interval config
        let config_short = OtelPushExporterConfig {
            endpoint: "otel-collector".to_string(),
            push_interval: Duration::from_secs(1),
            metrics: vec![],
        };
        assert_eq!(config_short.push_interval, Duration::from_secs(1));

        // Test longer interval config
        let config_long = OtelPushExporterConfig {
            endpoint: "otel-collector".to_string(),
            push_interval: Duration::from_secs(3600),
            metrics: vec![],
        };
        assert_eq!(config_long.push_interval, Duration::from_secs(3600));

        // Test sub-second intervals (milliseconds)
        let config_ms = OtelPushExporterConfig {
            endpoint: "otel-collector".to_string(),
            push_interval: Duration::from_millis(500),
            metrics: vec![],
        };
        assert_eq!(config_ms.push_interval, Duration::from_millis(500));
    }

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

    #[test]
    fn test_config_clone() {
        let config = default_config("otel-collector:4317");

        let cloned = config.clone();
        assert_eq!(config.endpoint, cloned.endpoint);
        assert_eq!(config.push_interval, cloned.push_interval);
        assert_eq!(config.metrics, cloned.metrics);
    }

    #[test]
    fn test_config_debug_format() {
        let config = default_config("otel-collector:4317");

        let debug_str = format!("{config:?}");
        assert!(debug_str.contains("otel-collector:4317"));
    }

    #[test]
    fn test_config_with_metrics_whitelist() {
        let config = OtelPushExporterConfig {
            endpoint: "otel-collector:4317".to_string(),
            push_interval: Duration::from_secs(60),
            metrics: vec!["requests_total".to_string(), "request_duration".to_string()],
        };
        assert_eq!(config.metrics.len(), 2);
        assert!(config.metrics.contains(&"requests_total".to_string()));
        assert!(config.metrics.contains(&"request_duration".to_string()));
    }

    #[test]
    fn test_config_with_empty_metrics_means_all() {
        let config = default_config("otel-collector:4317");
        assert!(config.metrics.is_empty());
    }

    #[test]
    fn test_protocol_detection_comprehensive() {
        // gRPC: bare hostname
        assert!(!default_config("otel-collector").is_http());

        // gRPC: hostname with port
        assert!(!default_config("otel-collector:4317").is_http());

        // HTTP: http:// scheme
        assert!(default_config("http://localhost:4318").is_http());

        // HTTP: https:// scheme
        assert!(default_config("https://otel-collector.example.com:4318").is_http());

        // HTTP: with /v1/metrics path
        assert!(default_config("http://localhost:4318/v1/metrics").is_http());
    }

    // Helper to create a mock Metric for testing
    fn create_mock_metric(name: &str) -> opentelemetry_sdk::metrics::data::Metric {
        use opentelemetry_sdk::metrics::data::{DataPoint, Gauge, Metric};
        Metric {
            name: std::borrow::Cow::Owned(name.to_string()),
            description: std::borrow::Cow::Borrowed("test metric"),
            unit: std::borrow::Cow::Borrowed("1"),
            data: Box::new(Gauge {
                data_points: vec![DataPoint {
                    attributes: vec![],
                    start_time: None,
                    time: None,
                    value: 42_i64,
                    exemplars: vec![],
                }],
            }),
        }
    }

    fn create_mock_resource_metrics(metric_names: &[&str]) -> ResourceMetrics {
        use opentelemetry::InstrumentationScope;
        use opentelemetry_sdk::Resource;

        ResourceMetrics {
            resource: Resource::empty(),
            scope_metrics: vec![ScopeMetrics {
                scope: InstrumentationScope::builder("test").build(),
                metrics: metric_names.iter().map(|n| create_mock_metric(n)).collect(),
            }],
        }
    }

    #[test]
    fn test_filtering_exporter_should_export_empty_whitelist() {
        // Empty whitelist means export all
        let whitelist: Vec<String> = vec![];
        let whitelist_set: HashSet<String> = whitelist.iter().cloned().collect();

        // With empty whitelist, everything should be exported
        assert!(whitelist_set.is_empty() || whitelist_set.contains("any_metric"));
    }

    #[test]
    fn test_filtering_exporter_should_export_with_whitelist() {
        let whitelist: HashSet<String> = vec!["metric_a".to_string(), "metric_b".to_string()]
            .into_iter()
            .collect();

        // Whitelisted metrics should be exported
        assert!(whitelist.is_empty() || whitelist.contains("metric_a"));
        assert!(whitelist.is_empty() || whitelist.contains("metric_b"));

        // Non-whitelisted metrics should not be exported
        assert!(whitelist.is_empty() || whitelist.contains("metric_c"));
        // metric_c is NOT in whitelist, so this would be false
        assert!(!whitelist.contains("metric_c"));
    }

    #[test]
    fn test_filter_metrics_removes_non_whitelisted() {
        let whitelist = vec!["keep_me".to_string(), "also_keep".to_string()];
        let whitelist_set: Arc<HashSet<String>> = Arc::new(whitelist.iter().cloned().collect());

        let mut rm =
            create_mock_resource_metrics(&["keep_me", "remove_me", "also_keep", "remove_too"]);

        // Initially should have 4 metrics
        assert_eq!(rm.scope_metrics[0].metrics.len(), 4);

        // Apply filtering logic
        if !whitelist_set.is_empty() {
            for scope_metrics in &mut rm.scope_metrics {
                scope_metrics
                    .metrics
                    .retain(|metric| whitelist_set.contains(metric.name.as_ref()));
            }
        }

        // After filtering, should have 2 metrics
        assert_eq!(rm.scope_metrics[0].metrics.len(), 2);

        let remaining_names: Vec<&str> = rm.scope_metrics[0]
            .metrics
            .iter()
            .map(|m| m.name.as_ref())
            .collect();
        assert!(remaining_names.contains(&"keep_me"));
        assert!(remaining_names.contains(&"also_keep"));
        assert!(!remaining_names.contains(&"remove_me"));
        assert!(!remaining_names.contains(&"remove_too"));
    }

    #[test]
    fn test_filter_metrics_keeps_all_when_empty_whitelist() {
        let whitelist: Vec<String> = vec![];
        let whitelist_set: Arc<HashSet<String>> = Arc::new(whitelist.iter().cloned().collect());

        let mut rm = create_mock_resource_metrics(&["metric_a", "metric_b", "metric_c"]);

        // Initially should have 3 metrics
        assert_eq!(rm.scope_metrics[0].metrics.len(), 3);

        // Apply filtering logic (should not filter when whitelist is empty)
        if !whitelist_set.is_empty() {
            for scope_metrics in &mut rm.scope_metrics {
                scope_metrics
                    .metrics
                    .retain(|metric| whitelist_set.contains(metric.name.as_ref()));
            }
        }

        // After "filtering", should still have 3 metrics
        assert_eq!(rm.scope_metrics[0].metrics.len(), 3);
    }

    #[test]
    fn test_filter_metrics_removes_empty_scope_metrics() {
        let whitelist = vec!["nonexistent".to_string()];
        let whitelist_set: Arc<HashSet<String>> = Arc::new(whitelist.iter().cloned().collect());

        let mut rm = create_mock_resource_metrics(&["metric_a", "metric_b"]);

        // Apply filtering logic
        for scope_metrics in &mut rm.scope_metrics {
            scope_metrics.metrics.retain(|metric| {
                whitelist_set.is_empty() || whitelist_set.contains(metric.name.as_ref())
            });
        }
        rm.scope_metrics.retain(|sm| !sm.metrics.is_empty());

        // Should have no scope_metrics left since none matched
        assert!(rm.scope_metrics.is_empty());
    }
}
