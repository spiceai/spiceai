/*
Copyright 2024 The Spice.ai OSS Authors

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

use arrow::array::RecordBatch;
use async_trait::async_trait;
use opentelemetry::metrics::MetricsError;
use opentelemetry_sdk::metrics::{
    data::{ResourceMetrics, Temporality},
    exporter::PushMetricsExporter,
    reader::{AggregationSelector, DefaultAggregationSelector, TemporalitySelector},
    Aggregation, InstrumentKind,
};

#[async_trait]
pub trait ArrowExporter: Send + Sync + 'static {
    async fn export(&self, record: RecordBatch) -> Result<(), MetricsError>;

    async fn force_flush(&self) -> Result<(), MetricsError>;

    fn shutdown(&self) -> Result<(), MetricsError>;
}

pub struct OtelArrowExporter<E: ArrowExporter> {
    exporter: E,
    aggregation_selector: Box<dyn AggregationSelector>,
}

impl<E: ArrowExporter> OtelArrowExporter<E> {
    pub fn new(exporter: E) -> Self {
        OtelArrowExporter {
            exporter,
            aggregation_selector: Box::new(DefaultAggregationSelector::new()),
        }
    }
}

impl<E: ArrowExporter> TemporalitySelector for OtelArrowExporter<E> {
    fn temporality(&self, _kind: InstrumentKind) -> Temporality {
        Temporality::Cumulative
    }
}

impl<E: ArrowExporter> AggregationSelector for OtelArrowExporter<E> {
    fn aggregation(&self, kind: InstrumentKind) -> Aggregation {
        self.aggregation_selector.aggregation(kind)
    }
}

#[async_trait]
impl<E: ArrowExporter> PushMetricsExporter for OtelArrowExporter<E> {
    async fn export(&self, metrics: &mut ResourceMetrics) -> Result<(), MetricsError> {
        todo!()
    }

    async fn force_flush(&self) -> Result<(), MetricsError> {
        todo!()
    }

    fn shutdown(&self) -> Result<(), MetricsError> {
        todo!()
    }
}
