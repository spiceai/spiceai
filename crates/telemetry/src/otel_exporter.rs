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

//! OTEL metrics exporter module for pushing metrics to an OpenTelemetry collector.
//!
//! This module provides functionality to push metrics to an OTEL collector using either
//! gRPC or HTTP protocols. The protocol is inferred from the endpoint:
//! - Endpoints containing `/v1/metrics` use HTTP protocol
//! - All other endpoints use gRPC protocol (default)

use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry_otlp::{MetricExporter, Protocol, WithExportConfig};
use opentelemetry_sdk::{
    Resource,
    metrics::{PeriodicReader, SdkMeterProvider},
};
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create OTEL metrics exporter: {message}"))]
    ExporterCreationFailed { message: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Configuration for the OTEL metrics exporter
#[derive(Debug, Clone)]
pub struct OtelExporterConfig {
    /// The endpoint of the OTEL collector
    pub endpoint: String,
    /// How often to push metrics
    pub push_interval: Duration,
}

impl OtelExporterConfig {
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

/// Builder for creating an OTEL metrics exporter
#[derive(Debug, Default)]
pub struct OtelMetricsExporterBuilder {
    config: Option<OtelExporterConfig>,
    resource_attributes: Vec<KeyValue>,
}

impl OtelMetricsExporterBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_config(mut self, config: OtelExporterConfig) -> Self {
        self.config = Some(config);
        self
    }

    #[must_use]
    pub fn with_resource_attributes(mut self, attributes: Vec<KeyValue>) -> Self {
        self.resource_attributes = attributes;
        self
    }

    /// Build the OTEL metrics exporter and return a meter provider
    ///
    /// # Errors
    ///
    /// Returns an error if the exporter cannot be created
    pub fn build(self) -> Result<SdkMeterProvider> {
        let Some(config) = self.config else {
            return Err(Error::ExporterCreationFailed {
                message: "No configuration provided".to_string(),
            });
        };

        let exporter = if config.is_http() {
            create_http_exporter(&config.endpoint)?
        } else {
            create_grpc_exporter(&config.grpc_endpoint())?
        };

        let resource = Resource::builder_empty()
            .with_attributes(self.resource_attributes.into_iter().chain(vec![
                KeyValue::new("service.name", "spiced"),
                KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            ]))
            .build();

        let periodic_reader = PeriodicReader::builder(exporter)
            .with_interval(config.push_interval)
            .build();

        let provider = SdkMeterProvider::builder()
            .with_resource(resource)
            .with_reader(periodic_reader)
            .build();

        Ok(provider)
    }
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

/// Start the OTEL metrics exporter with the given configuration
///
/// This function sets up a meter provider that periodically pushes metrics
/// to the configured OTEL collector endpoint.
///
/// # Arguments
///
/// * `config` - The exporter configuration
/// * `resource_attributes` - Additional resource attributes to include with metrics
///
/// # Returns
///
/// Returns the `SdkMeterProvider` that can be used to create meters, or an error if setup fails.
///
/// # Errors
///
/// Returns an error if the exporter cannot be created or configured.
pub fn create_otel_meter_provider(
    config: OtelExporterConfig,
    resource_attributes: Vec<KeyValue>,
) -> Result<SdkMeterProvider> {
    let protocol = if config.is_http() { "http" } else { "grpc" };
    tracing::info!(
        endpoint = %config.endpoint,
        protocol = protocol,
        push_interval_secs = config.push_interval.as_secs(),
        "Initializing OTEL metrics exporter"
    );

    OtelMetricsExporterBuilder::new()
        .with_config(config)
        .with_resource_attributes(resource_attributes)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_http_detects_http_scheme() {
        // http:// scheme is HTTP
        let config = OtelExporterConfig {
            endpoint: "http://localhost:4318".to_string(),
            push_interval: Duration::from_secs(60),
        };
        assert!(config.is_http());

        // https:// scheme is HTTP
        let config_https = OtelExporterConfig {
            endpoint: "https://otel-collector:4318".to_string(),
            push_interval: Duration::from_secs(60),
        };
        assert!(config_https.is_http());
    }

    #[test]
    fn test_is_http_detects_v1_metrics_path() {
        let config = OtelExporterConfig {
            endpoint: "http://localhost:4318/v1/metrics".to_string(),
            push_interval: Duration::from_secs(60),
        };
        assert!(config.is_http());
    }

    #[test]
    fn test_is_http_detects_grpc_bare_hostname() {
        // Bare hostname is gRPC
        let config = OtelExporterConfig {
            endpoint: "otel-collector".to_string(),
            push_interval: Duration::from_secs(60),
        };
        assert!(!config.is_http());

        // Hostname with port is gRPC
        let config_with_port = OtelExporterConfig {
            endpoint: "otel-collector:4317".to_string(),
            push_interval: Duration::from_secs(60),
        };
        assert!(!config_with_port.is_http());

        // localhost with port is gRPC
        let config_localhost = OtelExporterConfig {
            endpoint: "localhost:4317".to_string(),
            push_interval: Duration::from_secs(60),
        };
        assert!(!config_localhost.is_http());
    }

    #[test]
    fn test_grpc_endpoint_adds_default_port() {
        let config = OtelExporterConfig {
            endpoint: "otel-collector".to_string(),
            push_interval: Duration::from_secs(60),
        };
        assert_eq!(config.grpc_endpoint(), "http://otel-collector:4317");
    }

    #[test]
    fn test_grpc_endpoint_preserves_custom_port() {
        let config = OtelExporterConfig {
            endpoint: "otel-collector:9090".to_string(),
            push_interval: Duration::from_secs(60),
        };
        assert_eq!(config.grpc_endpoint(), "http://otel-collector:9090");
    }

    #[test]
    fn test_grpc_endpoint_with_localhost() {
        let config = OtelExporterConfig {
            endpoint: "localhost:4317".to_string(),
            push_interval: Duration::from_secs(60),
        };
        assert_eq!(config.grpc_endpoint(), "http://localhost:4317");
    }

    #[test]
    fn test_builder_without_config_fails() {
        let result = OtelMetricsExporterBuilder::new().build();
        assert!(result.is_err());
        let Err(err) = result else {
            panic!("Expected error");
        };
        assert!(
            err.to_string().contains("No configuration provided"),
            "Error should indicate missing config: {err}"
        );
    }

    #[test]
    fn test_builder_stores_grpc_config() {
        let config = OtelExporterConfig {
            endpoint: "otel-collector:4317".to_string(),
            push_interval: Duration::from_secs(60),
        };

        let builder = OtelMetricsExporterBuilder::new().with_config(config.clone());

        // Verify config is stored
        assert!(builder.config.is_some());
        let stored_config = builder.config.as_ref().expect("config should be set");
        assert_eq!(stored_config.endpoint, "otel-collector:4317");
        assert!(!stored_config.is_http());
        assert_eq!(stored_config.push_interval, Duration::from_secs(60));
    }

    #[test]
    fn test_builder_stores_http_config() {
        let config = OtelExporterConfig {
            endpoint: "http://localhost:4318/v1/metrics".to_string(),
            push_interval: Duration::from_secs(30),
        };

        let builder = OtelMetricsExporterBuilder::new().with_config(config);

        let stored_config = builder.config.as_ref().expect("config should be set");
        assert_eq!(stored_config.endpoint, "http://localhost:4318/v1/metrics");
        assert!(stored_config.is_http());
        assert_eq!(stored_config.push_interval, Duration::from_secs(30));
    }

    #[test]
    fn test_builder_stores_resource_attributes() {
        let config = OtelExporterConfig {
            endpoint: "otel-collector".to_string(),
            push_interval: Duration::from_secs(60),
        };

        let attributes = vec![
            KeyValue::new("service.namespace", "spiceai"),
            KeyValue::new("deployment.environment", "test"),
            KeyValue::new("custom.attribute", "custom-value"),
        ];

        let builder = OtelMetricsExporterBuilder::new()
            .with_config(config)
            .with_resource_attributes(attributes);

        assert_eq!(builder.resource_attributes.len(), 3);
        // Verify specific attributes are stored
        let attr_keys: Vec<_> = builder
            .resource_attributes
            .iter()
            .map(|kv| kv.key.as_str())
            .collect();
        assert!(attr_keys.contains(&"service.namespace"));
        assert!(attr_keys.contains(&"deployment.environment"));
        assert!(attr_keys.contains(&"custom.attribute"));
    }

    #[test]
    fn test_config_various_push_intervals() {
        // Test short interval config
        let config_short = OtelExporterConfig {
            endpoint: "otel-collector".to_string(),
            push_interval: Duration::from_secs(1),
        };
        assert_eq!(config_short.push_interval, Duration::from_secs(1));

        // Test longer interval config
        let config_long = OtelExporterConfig {
            endpoint: "otel-collector".to_string(),
            push_interval: Duration::from_secs(3600),
        };
        assert_eq!(config_long.push_interval, Duration::from_secs(3600));

        // Test sub-second intervals (milliseconds)
        let config_ms = OtelExporterConfig {
            endpoint: "otel-collector".to_string(),
            push_interval: Duration::from_millis(500),
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
    fn test_error_from_build_without_config() {
        // Verify that building without config produces appropriate error message
        let result = OtelMetricsExporterBuilder::new().build();
        assert!(result.is_err());
        let Err(error) = result else {
            panic!("Expected error")
        };
        let error_str = error.to_string();
        assert!(
            error_str.contains("No configuration provided"),
            "Error should describe the issue: {error_str}"
        );
    }

    #[test]
    fn test_config_clone() {
        let config = OtelExporterConfig {
            endpoint: "otel-collector:4317".to_string(),
            push_interval: Duration::from_secs(60),
        };

        let cloned = config.clone();
        assert_eq!(config.endpoint, cloned.endpoint);
        assert_eq!(config.push_interval, cloned.push_interval);
    }

    #[test]
    fn test_config_debug_format() {
        let config = OtelExporterConfig {
            endpoint: "otel-collector:4317".to_string(),
            push_interval: Duration::from_secs(60),
        };

        let debug_str = format!("{config:?}");
        assert!(debug_str.contains("otel-collector:4317"));
    }

    #[test]
    fn test_protocol_detection_comprehensive() {
        // gRPC: bare hostname
        assert!(
            !OtelExporterConfig {
                endpoint: "otel-collector".to_string(),
                push_interval: Duration::from_secs(60),
            }
            .is_http()
        );

        // gRPC: hostname with port
        assert!(
            !OtelExporterConfig {
                endpoint: "otel-collector:4317".to_string(),
                push_interval: Duration::from_secs(60),
            }
            .is_http()
        );

        // HTTP: http:// scheme
        assert!(
            OtelExporterConfig {
                endpoint: "http://localhost:4318".to_string(),
                push_interval: Duration::from_secs(60),
            }
            .is_http()
        );

        // HTTP: https:// scheme
        assert!(
            OtelExporterConfig {
                endpoint: "https://otel-collector.example.com:4318".to_string(),
                push_interval: Duration::from_secs(60),
            }
            .is_http()
        );

        // HTTP: with /v1/metrics path
        assert!(
            OtelExporterConfig {
                endpoint: "http://localhost:4318/v1/metrics".to_string(),
                push_interval: Duration::from_secs(60),
            }
            .is_http()
        );
    }
}
