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

use arrow::array::RecordBatch;
use async_trait::async_trait;
use opentelemetry_sdk::{
    error::{OTelSdkError, OTelSdkResult},
    metrics::{MetricError, Temporality, data::ResourceMetrics, exporter::PushMetricExporter},
};

use crate::converter::OtelToArrowConverter;

#[async_trait]
pub trait ArrowExporter: Send + Sync + 'static {
    async fn export(&self, metrics: RecordBatch) -> OTelSdkResult;

    /// # Errors
    ///
    /// Returns an error if pending metrics could not be flushed.
    fn force_flush(&self) -> OTelSdkResult;

    /// Shutdown the exporter.
    ///
    /// # Errors
    ///
    /// This function will return an error if the shutdown couldn't complete successfully.
    fn shutdown(&self) -> OTelSdkResult;
}

pub struct OtelArrowExporter<E: ArrowExporter> {
    exporter: E,
}

impl<E: ArrowExporter + Clone> Clone for OtelArrowExporter<E> {
    fn clone(&self) -> Self {
        OtelArrowExporter {
            exporter: self.exporter.clone(),
        }
    }
}

impl<E: ArrowExporter> OtelArrowExporter<E> {
    pub fn new(exporter: E) -> Self {
        OtelArrowExporter { exporter }
    }
}

#[async_trait]
impl<E: ArrowExporter> PushMetricExporter for OtelArrowExporter<E> {
    #[allow(clippy::manual_async_fn)]
    fn export(
        &self,
        metrics: &mut ResourceMetrics,
    ) -> impl std::future::Future<Output = OTelSdkResult> + Send {
        async move {
            let mut converter = OtelToArrowConverter::new(metrics.scope_metrics.len());
            let batch = converter
                .convert(metrics)
                .map_err(|err| metric_error_to_sdk_error(&err))?;

            self.exporter.export(batch).await
        }
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.exporter.force_flush()
    }

    fn shutdown(&self) -> OTelSdkResult {
        self.exporter.shutdown()
    }

    fn temporality(&self) -> Temporality {
        Temporality::Cumulative
    }
}

fn metric_error_to_sdk_error(err: &MetricError) -> OTelSdkError {
    OTelSdkError::InternalFailure(err.to_string())
}
