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

//! Kafka support retained in `runtime` for the debezium sidecar.
//!
//! The Kafka *data connector* itself now lives in the `connector-kafka` crate.
//! This module keeps only the offset-store / commit-hook glue and the metrics
//! provider that the debezium connector (`super::debezium`) depends on.

use data_components::{
    cdc::CommitError,
    kafka::{KafkaMetrics, KafkaOffset, KafkaOffsetCommitHook},
};
use snafu::prelude::*;
use std::sync::Arc;
use tonic::async_trait;

use crate::{
    component::{
        ComponentType,
        metrics::{MetricSpec, MetricType, MetricsProvider, ObserveMetricCallback},
    },
    dataaccelerator::spice_sys::{self, kafka::KafkaSys},
};

#[async_trait]
pub(crate) trait SidecarOffsetStore: Send + Sync {
    async fn upsert_offsets(&self, offsets: &[KafkaOffset]) -> spice_sys::Result<()>;
}

#[async_trait]
impl SidecarOffsetStore for KafkaSys {
    async fn upsert_offsets(&self, offsets: &[KafkaOffset]) -> spice_sys::Result<()> {
        KafkaSys::upsert_offsets(self, offsets).await
    }
}

pub(crate) struct SidecarOffsetCommitHook<T> {
    store: Arc<T>,
}

impl<T> SidecarOffsetCommitHook<T> {
    pub(crate) fn new(store: Arc<T>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl<T> KafkaOffsetCommitHook for SidecarOffsetCommitHook<T>
where
    T: SidecarOffsetStore,
{
    async fn commit_offsets(&self, offsets: &[KafkaOffset]) -> Result<(), CommitError> {
        self.store
            .upsert_offsets(offsets)
            .await
            .boxed()
            .map_err(|e| CommitError::UnableToCommitChange { source: e })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct KafkaMetricsProvider {
    metrics: Arc<KafkaMetrics>,
}

impl KafkaMetricsProvider {
    pub(crate) fn new(metrics: Arc<KafkaMetrics>) -> Self {
        Self { metrics }
    }
}

const METRICS: &[MetricSpec] = &[
    MetricSpec {
        name: "records_consumed_total",
        description: Some("Total number of records consumed"),
        unit: Some("records"),
        metric_type: MetricType::ObservableCounterU64,
        auto_register: false,
    },
    MetricSpec {
        name: "bytes_consumed_total",
        description: Some("Total bytes consumed"),
        unit: Some("bytes"),
        metric_type: MetricType::ObservableCounterU64,
        auto_register: false,
    },
    MetricSpec {
        name: "records_lag",
        description: Some("Total consumer lag across all partitions"),
        unit: Some("records"),
        metric_type: MetricType::ObservableGaugeU64,
        auto_register: false,
    },
];

impl MetricsProvider for KafkaMetricsProvider {
    fn component_type(&self) -> ComponentType {
        ComponentType::Dataset
    }

    fn component_name(&self) -> &'static str {
        "kafka"
    }

    fn available_metrics(&self) -> &'static [MetricSpec] {
        METRICS
    }

    fn callback_to_observe_metric(
        &self,
        metric: &MetricSpec,
        attributes: Vec<opentelemetry::KeyValue>,
    ) -> Option<ObserveMetricCallback> {
        match metric.name {
            "records_consumed_total" => {
                let metrics = Arc::clone(&self.metrics);
                Some(ObserveMetricCallback::U64(Box::new(move |observer| {
                    observer.observe(
                        metrics
                            .records_consumed
                            .load(std::sync::atomic::Ordering::Relaxed),
                        &attributes,
                    );
                })))
            }
            "bytes_consumed_total" => {
                let metrics = Arc::clone(&self.metrics);
                Some(ObserveMetricCallback::U64(Box::new(move |observer| {
                    observer.observe(
                        metrics
                            .bytes_consumed
                            .load(std::sync::atomic::Ordering::Relaxed),
                        &attributes,
                    );
                })))
            }
            "records_lag" => {
                let metrics = Arc::clone(&self.metrics);
                Some(ObserveMetricCallback::U64(Box::new(move |observer| {
                    observer.observe(
                        metrics
                            .records_lag
                            .load(std::sync::atomic::Ordering::Relaxed),
                        &attributes,
                    );
                })))
            }
            _ => None,
        }
    }
}
