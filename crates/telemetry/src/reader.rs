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
use std::time::Duration;

use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::metrics::{
    InstrumentKind, ManualReader, Pipeline, Temporality, data::ResourceMetrics,
    reader::MetricReader,
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

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.reader.shutdown_with_timeout(timeout)
    }

    fn temporality(&self, kind: InstrumentKind) -> Temporality {
        self.reader.temporality(kind)
    }
}
