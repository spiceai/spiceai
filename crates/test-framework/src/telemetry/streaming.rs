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

//! Streaming OTLP metrics exporter for real-time query metrics.
//!
//! This module provides incremental metrics export during test execution,
//! allowing correlation with other system metrics in real-time.

use std::sync::Arc;
use std::time::Duration;

use opentelemetry::metrics::{Histogram, Meter, MeterProvider as _};
use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// A query metric event to be exported
#[derive(Debug, Clone)]
pub struct QueryMetricEvent {
    pub query_name: Arc<str>,
    pub duration_ms: f64,
    pub success: bool,
    pub worker_id: usize,
    /// Reason for failure, if any (e.g., "error", "timeout")
    pub failure_reason: Option<String>,
}

impl QueryMetricEvent {
    /// Create a new query metric event
    #[must_use]
    pub fn new(query_name: String, duration: Duration, success: bool, worker_id: usize) -> Self {
        Self {
            query_name: Arc::from(query_name),
            duration_ms: duration.as_secs_f64() * 1000.0,
            success,
            worker_id,
            failure_reason: None,
        }
    }

    /// Create a new query metric event with a failure reason
    #[must_use]
    pub fn with_failure(
        query_name: String,
        duration: Duration,
        worker_id: usize,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            query_name: Arc::from(query_name),
            duration_ms: duration.as_secs_f64() * 1000.0,
            success: false,
            worker_id,
            failure_reason: Some(reason.into()),
        }
    }
}

/// Streaming OTLP exporter that sends query metrics in real-time
pub struct StreamingOtlpExporter {
    tx: mpsc::Sender<QueryMetricEvent>,
    handle: JoinHandle<()>,
    shutdown_token: CancellationToken,
}

impl StreamingOtlpExporter {
    /// Spawn a new streaming OTLP exporter with the given endpoint.
    ///
    /// Metrics will be exported periodically (every 5 seconds).
    #[must_use]
    pub fn spawn(endpoint: String) -> Self {
        // Use a bounded channel to avoid unbounded memory growth
        let (tx, rx) = mpsc::channel(10_000);
        let shutdown_token = CancellationToken::new();

        let handle = tokio::spawn(Self::exporter_task(rx, endpoint, shutdown_token.clone()));

        Self {
            tx,
            handle,
            shutdown_token,
        }
    }

    /// Get a sender that can be cloned and passed to workers
    #[must_use]
    pub fn sender(&self) -> mpsc::Sender<QueryMetricEvent> {
        self.tx.clone()
    }

    async fn exporter_task(
        mut rx: mpsc::Receiver<QueryMetricEvent>,
        endpoint: String,
        shutdown_token: CancellationToken,
    ) {
        // Build the OTLP exporter
        let exporter = match MetricExporter::builder()
            .with_tonic()
            .with_timeout(Duration::from_secs(10))
            .with_endpoint(&endpoint)
            .build()
        {
            Ok(exp) => exp,
            Err(e) => {
                eprintln!("Failed to create streaming OTLP exporter: {e}");
                return;
            }
        };

        // Create a periodic reader that exports every 5 seconds
        let reader = PeriodicReader::builder(exporter)
            .with_interval(Duration::from_secs(5))
            .build();

        let resource = Resource::builder()
            .with_service_name("testoperator-streaming")
            .build();

        let provider = SdkMeterProvider::builder()
            .with_resource(resource)
            .with_reader(reader)
            .build();

        // Set as global provider for this task
        global::set_meter_provider(provider.clone());

        let meter: Meter = provider.meter("testoperator-streaming");
        let query_duration_histogram: Histogram<f64> = meter
            .f64_histogram("testoperator.streaming.query.duration_ms")
            .with_description("Query execution duration in milliseconds (streaming)")
            .with_unit("ms")
            .build();

        let query_count = meter
            .u64_counter("testoperator.streaming.query.count")
            .with_description("Total number of queries executed (streaming)")
            .build();

        let query_success_count = meter
            .u64_counter("testoperator.streaming.query.success_count")
            .with_description("Number of successful queries (streaming)")
            .build();

        let query_failure_count = meter
            .u64_counter("testoperator.streaming.query.failure_count")
            .with_description("Number of failed queries (streaming)")
            .build();

        println!("Streaming OTLP metrics exporter started (endpoint: {endpoint})");

        loop {
            tokio::select! {
                Some(event) = rx.recv() => {
                    let mut attributes = vec![
                        KeyValue::new("query_name", event.query_name.to_string()),
                        KeyValue::new("worker_id", event.worker_id.to_string()),
                        KeyValue::new("success", event.success.to_string()),
                    ];
                    if let Some(reason) = &event.failure_reason {
                        attributes.push(KeyValue::new("failure_reason", reason.clone()));
                    }

                    query_duration_histogram.record(event.duration_ms, &attributes);
                    query_count.add(1, &attributes);

                    if event.success {
                        query_success_count.add(1, &attributes);
                    } else {
                        query_failure_count.add(1, &attributes);
                    }
                }
                () = shutdown_token.cancelled() => {
                    println!("Streaming OTLP metrics exporter shutting down");
                    // Drain any remaining events
                    while let Ok(event) = rx.try_recv() {
                        let mut attributes = vec![
                            KeyValue::new("query_name", event.query_name.to_string()),
                            KeyValue::new("worker_id", event.worker_id.to_string()),
                            KeyValue::new("success", event.success.to_string()),
                        ];
                        if let Some(reason) = &event.failure_reason {
                            attributes.push(KeyValue::new("failure_reason", reason.clone()));
                        }
                        query_duration_histogram.record(event.duration_ms, &attributes);
                        query_count.add(1, &attributes);
                        if event.success {
                            query_success_count.add(1, &attributes);
                        } else {
                            query_failure_count.add(1, &attributes);
                        }
                    }

                    // Force a final flush
                    if let Err(e) = provider.force_flush() {
                        eprintln!("Failed to flush streaming metrics: {e}");
                    }
                    break;
                }
            }
        }
    }

    /// Shutdown the exporter and wait for the background task to complete
    pub async fn shutdown(self) {
        self.shutdown_token.cancel();
        let _ = self.handle.await;
    }
}
