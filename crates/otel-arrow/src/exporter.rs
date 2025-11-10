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
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::metrics::{
    Temporality, data::ResourceMetrics, exporter::PushMetricExporter,
};
use std::time::Duration;

use crate::converter::OtelToArrowConverter;

pub trait ArrowExporter: Send + Sync + 'static {
    fn export(
        &self,
        metrics: RecordBatch,
    ) -> impl std::future::Future<Output = OTelSdkResult> + Send;

    fn force_flush(&self) -> OTelSdkResult;

    /// Shutdown the exporter with a timeout.
    ///
    /// # Errors
    ///
    /// This function will return an error if the shutdown couldn't complete successfully.
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult;

    /// Shutdown the exporter with the default timeout of 5 seconds.
    ///
    /// # Errors
    ///
    /// This function will return an error if the shutdown couldn't complete successfully.
    fn shutdown(&self) -> OTelSdkResult {
        self.shutdown_with_timeout(Duration::from_secs(5))
    }
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

impl<E: ArrowExporter> PushMetricExporter for OtelArrowExporter<E> {
    fn export(
        &self,
        metrics: &ResourceMetrics,
    ) -> impl std::future::Future<Output = OTelSdkResult> + Send {
        async move {
            // Estimate capacity based on scope metrics count
            let capacity: usize = metrics.scope_metrics().count();
            let mut converter = OtelToArrowConverter::new(capacity);
            let batch = converter.convert(metrics)?;

            self.exporter.export(batch).await
        }
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.exporter.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.exporter.shutdown_with_timeout(timeout)
    }

    fn temporality(&self) -> Temporality {
        Temporality::Cumulative
    }
}
