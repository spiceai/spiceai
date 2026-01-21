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

use std::sync::{Arc, Weak};

use opentelemetry_sdk::{
    error::OTelSdkResult,
    metrics::{
        InstrumentKind, ManualReader, Pipeline, Temporality, data::ResourceMetrics,
        reader::MetricReader,
    },
};

#[derive(Debug, Clone)]
pub struct InitialReader {
    reader: Arc<ManualReader>,
}

impl Default for InitialReader {
    fn default() -> Self {
        Self::new()
    }
}

impl InitialReader {
    #[must_use]
    pub fn new() -> Self {
        Self {
            reader: Arc::new(ManualReader::builder().build()),
        }
    }
}

impl MetricReader for InitialReader {
    fn register_pipeline(&self, pipeline: Weak<Pipeline>) {
        self.reader.register_pipeline(pipeline);
    }

    fn collect(&self, rm: &mut ResourceMetrics) -> OTelSdkResult {
        self.reader.collect(rm)
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.reader.force_flush()
    }

    fn shutdown(&self) -> OTelSdkResult {
        self.reader.shutdown()
    }

    /// Delegates to the inner `ManualReader`'s timeout-aware shutdown.
    ///
    /// Simple delegation is appropriate here because `InitialReader` is a thin wrapper
    /// that adds no state requiring cleanup beyond what `ManualReader` handles. The
    /// inner reader manages all pipeline state and timeout logic.
    fn shutdown_with_timeout(&self, timeout: std::time::Duration) -> OTelSdkResult {
        self.reader.shutdown_with_timeout(timeout)
    }

    fn temporality(&self, kind: InstrumentKind) -> Temporality {
        self.reader.temporality(kind)
    }
}
