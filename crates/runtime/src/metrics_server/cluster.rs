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

//! Cluster-wide metrics collection for the `/metrics?scope=cluster` endpoint.
//!
//! This module provides functionality to collect metrics from all nodes in a Spice cluster:
//! - Local metrics from this scheduler
//! - Metrics from peer schedulers via GetMetrics RPC
//! - Metrics from executors via control stream
//!
//! All metrics are merged and labeled with `spice_node_id` and `spice_node_role`.

use std::sync::Arc;

use ballista_core::utils::create_grpc_client_endpoint;
use opentelemetry_proto::tonic::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::{AnyValue, KeyValue, any_value::Value},
    metrics::v1::ResourceMetrics as OtlpResourceMetrics,
};
use prost::Message;
use runtime_proto::cluster_service_client::ClusterServiceClient;
use runtime_proto::GetMetricsRequest;
use snafu::prelude::*;
use tokio::sync::RwLock;
use tonic::transport::ClientTlsConfig;

use crate::cluster::ExecutorRegistry;
use crate::cluster::SchedulerPeers;

/// Labels added to all metrics for node identification.
const NODE_ID_LABEL: &str = "spice_node_id";
const NODE_ROLE_LABEL: &str = "spice_node_role";

/// Node roles in the cluster.
const ROLE_SCHEDULER: &str = "scheduler";
const ROLE_EXECUTOR: &str = "executor";

/// Error type for cluster metrics collection.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to collect metrics from peers: [{failed_peers}]"))]
    PeerCollectionFailed { failed_peers: String },

    #[snafu(display("Failed to collect metrics from executors: [{failed_executors}]"))]
    ExecutorCollectionFailed { failed_executors: String },

    #[snafu(display("Failed to decode OTLP metrics from {node_id}: {reason}"))]
    DecodeError { node_id: String, reason: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Collects metrics from all nodes in a Spice cluster.
///
/// This collector is used when the `/metrics?scope=cluster` endpoint is requested.
/// It fans out to all schedulers and executors, collects their OTLP metrics,
/// merges them, and adds node identification labels.
pub struct ClusterMetricsCollector {
    /// Peer schedulers to query
    scheduler_peers: Arc<RwLock<SchedulerPeers>>,
    /// Registry of connected executors
    executor_registry: Arc<ExecutorRegistry>,
    /// TLS configuration for gRPC clients
    client_tls_config: Option<ClientTlsConfig>,
    /// This node's identifier (advertise address)
    node_id: String,
    /// Function to collect local metrics (returns OTLP protobuf bytes)
    local_metrics_collector: Arc<dyn Fn() -> Vec<u8> + Send + Sync>,
}

impl ClusterMetricsCollector {
    /// Creates a new cluster metrics collector.
    #[must_use]
    pub fn new(
        scheduler_peers: Arc<RwLock<SchedulerPeers>>,
        executor_registry: Arc<ExecutorRegistry>,
        client_tls_config: Option<ClientTlsConfig>,
        node_id: String,
        local_metrics_collector: Arc<dyn Fn() -> Vec<u8> + Send + Sync>,
    ) -> Self {
        Self {
            scheduler_peers,
            executor_registry,
            client_tls_config,
            node_id,
            local_metrics_collector,
        }
    }

    /// Collects metrics from all cluster nodes.
    ///
    /// This method:
    /// 1. Collects local metrics
    /// 2. Fans out GetMetrics RPC to all peer schedulers
    /// 3. Requests metrics from all executors via control stream
    /// 4. Merges all metrics and adds node labels
    ///
    /// If any peer or executor fails, returns an error with the list of failed nodes.
    pub async fn collect(&self) -> Result<ExportMetricsServiceRequest> {
        // Collect metrics in parallel from all sources
        let (local_result, scheduler_results, executor_results) = tokio::join!(
            self.collect_local(),
            self.collect_from_schedulers(),
            self.collect_from_executors(),
        );

        // Check for failures
        let scheduler_results = scheduler_results?;
        let executor_results = executor_results?;

        // Merge all metrics
        let mut merged = ExportMetricsServiceRequest::default();

        // Add local metrics with labels
        let local_metrics = local_result;
        if !local_metrics.is_empty() {
            if let Ok(mut request) = ExportMetricsServiceRequest::decode(local_metrics.as_slice()) {
                add_node_labels(&mut request, &self.node_id, ROLE_SCHEDULER);
                merged
                    .resource_metrics
                    .append(&mut request.resource_metrics);
            }
        }

        // Add scheduler metrics with labels
        for (node_id, metrics_bytes) in scheduler_results {
            if metrics_bytes.is_empty() {
                continue;
            }
            match ExportMetricsServiceRequest::decode(metrics_bytes.as_slice()) {
                Ok(mut request) => {
                    add_node_labels(&mut request, &node_id, ROLE_SCHEDULER);
                    merged
                        .resource_metrics
                        .append(&mut request.resource_metrics);
                }
                Err(e) => {
                    tracing::warn!("Failed to decode metrics from scheduler {node_id}: {e}");
                }
            }
        }

        // Add executor metrics with labels
        for (node_id, metrics_bytes) in executor_results {
            if metrics_bytes.is_empty() {
                continue;
            }
            match ExportMetricsServiceRequest::decode(metrics_bytes.as_slice()) {
                Ok(mut request) => {
                    add_node_labels(&mut request, &node_id, ROLE_EXECUTOR);
                    merged
                        .resource_metrics
                        .append(&mut request.resource_metrics);
                }
                Err(e) => {
                    tracing::warn!("Failed to decode metrics from executor {node_id}: {e}");
                }
            }
        }

        Ok(merged)
    }

    /// Collects local metrics from this node.
    async fn collect_local(&self) -> Vec<u8> {
        (self.local_metrics_collector)()
    }

    /// Collects metrics from all peer schedulers via GetMetrics RPC.
    async fn collect_from_schedulers(&self) -> Result<Vec<(String, Vec<u8>)>> {
        let peers = self.scheduler_peers.read().await;

        // Filter out self from peers
        let peer_addresses: Vec<String> = peers
            .values()
            .filter(|record| record.advertise_address != self.node_id)
            .map(|record| record.advertise_address.clone())
            .collect();

        drop(peers);

        if peer_addresses.is_empty() {
            return Ok(Vec::new());
        }

        let tls_enabled = self.client_tls_config.is_some();
        let client_tls_config = self.client_tls_config.clone();

        // Spawn requests to all peers in parallel
        let mut handles = Vec::with_capacity(peer_addresses.len());
        for address in peer_addresses {
            let tls_config = client_tls_config.clone();
            handles.push(tokio::spawn(async move {
                let result = fetch_metrics_from_scheduler(&address, tls_enabled, tls_config).await;
                (address, result)
            }));
        }

        // Collect results
        let mut results = Vec::new();
        let mut failures = Vec::new();

        for handle in handles {
            match handle.await {
                Ok((address, Ok(metrics))) => {
                    results.push((address, metrics));
                }
                Ok((address, Err(e))) => {
                    failures.push(format!("{address}: {e}"));
                }
                Err(e) => {
                    failures.push(format!("task panic: {e}"));
                }
            }
        }

        if failures.is_empty() {
            Ok(results)
        } else {
            Err(Error::PeerCollectionFailed {
                failed_peers: failures.join(", "),
            })
        }
    }

    /// Collects metrics from all executors via control stream.
    async fn collect_from_executors(&self) -> Result<Vec<(String, Vec<u8>)>> {
        self.executor_registry
            .request_metrics_from_all()
            .await
            .map_err(|e| Error::ExecutorCollectionFailed {
                failed_executors: e.to_string(),
            })
    }
}

/// Fetches metrics from a single scheduler via GetMetrics RPC.
async fn fetch_metrics_from_scheduler(
    address: &str,
    tls_enabled: bool,
    tls_config: Option<ClientTlsConfig>,
) -> std::result::Result<Vec<u8>, String> {
    let endpoint_url = normalize_endpoint(address, tls_enabled);

    let endpoint =
        create_grpc_client_endpoint(endpoint_url.clone()).map_err(|e| e.to_string())?;

    let endpoint = if let Some(tls_config) = tls_config {
        endpoint.tls_config(tls_config).map_err(|e| e.to_string())?
    } else {
        endpoint
    };

    let channel = endpoint.connect().await.map_err(|e| e.to_string())?;

    let mut client = ClusterServiceClient::new(channel)
        .max_encoding_message_size(usize::MAX)
        .max_decoding_message_size(usize::MAX);

    let response = client
        .get_metrics(GetMetricsRequest {})
        .await
        .map_err(|e| e.to_string())?;

    Ok(response.into_inner().otlp_metrics)
}

/// Normalizes an endpoint address to include a scheme.
fn normalize_endpoint(address: &str, tls_enabled: bool) -> String {
    if address.starts_with("http://") || address.starts_with("https://") {
        return address.to_string();
    }

    let scheme = if tls_enabled { "https" } else { "http" };
    format!("{scheme}://{address}")
}

/// Adds node identification labels to all metrics in the request.
fn add_node_labels(request: &mut ExportMetricsServiceRequest, node_id: &str, role: &str) {
    for resource_metrics in &mut request.resource_metrics {
        add_labels_to_resource_metrics(resource_metrics, node_id, role);
    }
}

/// Adds node labels to a single `ResourceMetrics`.
fn add_labels_to_resource_metrics(
    resource_metrics: &mut OtlpResourceMetrics,
    node_id: &str,
    role: &str,
) {
    // Add labels to resource attributes
    if let Some(ref mut resource) = resource_metrics.resource {
        // Check if labels already exist
        let has_node_id = resource
            .attributes
            .iter()
            .any(|kv| kv.key == NODE_ID_LABEL);
        let has_role = resource
            .attributes
            .iter()
            .any(|kv| kv.key == NODE_ROLE_LABEL);

        if !has_node_id {
            resource.attributes.push(KeyValue {
                key: NODE_ID_LABEL.to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue(node_id.to_string())),
                }),
            });
        }

        if !has_role {
            resource.attributes.push(KeyValue {
                key: NODE_ROLE_LABEL.to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue(role.to_string())),
                }),
            });
        }
    }

    // Also add labels to each data point for better Prometheus compatibility
    for scope_metrics in &mut resource_metrics.scope_metrics {
        for metric in &mut scope_metrics.metrics {
            add_labels_to_metric_data_points(metric, node_id, role);
        }
    }
}

/// Adds node labels to data points within a metric.
fn add_labels_to_metric_data_points(
    metric: &mut opentelemetry_proto::tonic::metrics::v1::Metric,
    node_id: &str,
    role: &str,
) {
    use opentelemetry_proto::tonic::metrics::v1::metric::Data;

    let node_id_kv = KeyValue {
        key: NODE_ID_LABEL.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(node_id.to_string())),
        }),
    };
    let role_kv = KeyValue {
        key: NODE_ROLE_LABEL.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(role.to_string())),
        }),
    };

    match &mut metric.data {
        Some(Data::Gauge(gauge)) => {
            for dp in &mut gauge.data_points {
                dp.attributes.push(node_id_kv.clone());
                dp.attributes.push(role_kv.clone());
            }
        }
        Some(Data::Sum(sum)) => {
            for dp in &mut sum.data_points {
                dp.attributes.push(node_id_kv.clone());
                dp.attributes.push(role_kv.clone());
            }
        }
        Some(Data::Histogram(histogram)) => {
            for dp in &mut histogram.data_points {
                dp.attributes.push(node_id_kv.clone());
                dp.attributes.push(role_kv.clone());
            }
        }
        Some(Data::ExponentialHistogram(exp_histogram)) => {
            for dp in &mut exp_histogram.data_points {
                dp.attributes.push(node_id_kv.clone());
                dp.attributes.push(role_kv.clone());
            }
        }
        Some(Data::Summary(summary)) => {
            for dp in &mut summary.data_points {
                dp.attributes.push(node_id_kv.clone());
                dp.attributes.push(role_kv.clone());
            }
        }
        None => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{any_value::Value, AnyValue};
    use opentelemetry_proto::tonic::metrics::v1::{
        number_data_point::Value as NumberValue, Gauge, Metric, NumberDataPoint, ResourceMetrics,
        ScopeMetrics, Sum,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;

    #[test]
    fn test_normalize_endpoint() {
        assert_eq!(
            normalize_endpoint("localhost:50051", false),
            "http://localhost:50051"
        );
        assert_eq!(
            normalize_endpoint("localhost:50051", true),
            "https://localhost:50051"
        );
        assert_eq!(
            normalize_endpoint("http://localhost:50051", false),
            "http://localhost:50051"
        );
        assert_eq!(
            normalize_endpoint("https://localhost:50051", true),
            "https://localhost:50051"
        );
    }

    #[test]
    fn test_add_node_labels_to_empty_request() {
        let mut request = ExportMetricsServiceRequest::default();
        add_node_labels(&mut request, "node-1", ROLE_SCHEDULER);
        // Should not panic with empty request
        assert!(request.resource_metrics.is_empty());
    }

    #[test]
    fn test_add_node_labels_with_gauge_metrics() {
        let mut request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![],
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "test_gauge".to_string(),
                        description: "A test gauge".to_string(),
                        unit: String::new(),
                        metadata: Vec::new(),
                        data: Some(
                            opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(Gauge {
                                data_points: vec![NumberDataPoint {
                                    attributes: vec![],
                                    start_time_unix_nano: 0,
                                    time_unix_nano: 0,
                                    value: Some(NumberValue::AsDouble(42.5)),
                                    exemplars: Vec::new(),
                                    flags: 0,
                                }],
                            }),
                        ),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        add_node_labels(&mut request, "executor-1", ROLE_EXECUTOR);

        // Check resource attributes
        let resource = request.resource_metrics[0]
            .resource
            .as_ref()
            .expect("resource should exist");
        assert_eq!(resource.attributes.len(), 2);

        let node_id_attr = resource
            .attributes
            .iter()
            .find(|kv| kv.key == NODE_ID_LABEL)
            .expect("node_id label should exist");
        assert_eq!(
            node_id_attr.value,
            Some(AnyValue {
                value: Some(Value::StringValue("executor-1".to_string()))
            })
        );

        let role_attr = resource
            .attributes
            .iter()
            .find(|kv| kv.key == NODE_ROLE_LABEL)
            .expect("role label should exist");
        assert_eq!(
            role_attr.value,
            Some(AnyValue {
                value: Some(Value::StringValue(ROLE_EXECUTOR.to_string()))
            })
        );

        // Check data point attributes
        let gauge = match &request.resource_metrics[0].scope_metrics[0].metrics[0].data {
            Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(g)) => g,
            _ => panic!("expected gauge"),
        };
        assert_eq!(gauge.data_points[0].attributes.len(), 2);
    }

    #[test]
    fn test_add_node_labels_with_counter_metrics() {
        let mut request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![],
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "requests_total".to_string(),
                        description: "Total requests".to_string(),
                        unit: String::new(),
                        metadata: Vec::new(),
                        data: Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(
                            Sum {
                                data_points: vec![NumberDataPoint {
                                    attributes: vec![],
                                    start_time_unix_nano: 0,
                                    time_unix_nano: 0,
                                    value: Some(NumberValue::AsInt(100)),
                                    exemplars: Vec::new(),
                                    flags: 0,
                                }],
                                aggregation_temporality: 2,
                                is_monotonic: true,
                            },
                        )),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        add_node_labels(&mut request, "scheduler-1", ROLE_SCHEDULER);

        // Check data point attributes for Sum metric
        let sum = match &request.resource_metrics[0].scope_metrics[0].metrics[0].data {
            Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(s)) => s,
            _ => panic!("expected sum"),
        };
        assert_eq!(sum.data_points[0].attributes.len(), 2);

        let has_node_id = sum.data_points[0]
            .attributes
            .iter()
            .any(|kv| kv.key == NODE_ID_LABEL);
        let has_role = sum.data_points[0]
            .attributes
            .iter()
            .any(|kv| kv.key == NODE_ROLE_LABEL);

        assert!(has_node_id, "node_id label should be added to data points");
        assert!(has_role, "role label should be added to data points");
    }

    #[test]
    fn test_add_node_labels_idempotent() {
        let mut request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![
                        KeyValue {
                            key: NODE_ID_LABEL.to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("existing-node".to_string())),
                            }),
                        },
                        KeyValue {
                            key: NODE_ROLE_LABEL.to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue(ROLE_SCHEDULER.to_string())),
                            }),
                        },
                    ],
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_metrics: vec![],
                schema_url: String::new(),
            }],
        };

        // Call add_node_labels with different values
        add_node_labels(&mut request, "new-node", ROLE_EXECUTOR);

        // Labels should NOT be duplicated - original values preserved
        let resource = request.resource_metrics[0]
            .resource
            .as_ref()
            .expect("resource should exist");

        // Should still have exactly 2 attributes, not 4
        assert_eq!(
            resource.attributes.len(),
            2,
            "labels should not be duplicated"
        );

        // Original values should be preserved
        let node_id_attr = resource
            .attributes
            .iter()
            .find(|kv| kv.key == NODE_ID_LABEL)
            .expect("node_id should exist");
        assert_eq!(
            node_id_attr.value,
            Some(AnyValue {
                value: Some(Value::StringValue("existing-node".to_string()))
            }),
            "original node_id should be preserved"
        );
    }
}
