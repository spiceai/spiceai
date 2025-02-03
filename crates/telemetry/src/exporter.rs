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

use std::sync::Arc;

use arrow::array::RecordBatch;
use async_trait::async_trait;
use flight_client::{Credentials, FlightClient};
use opentelemetry_sdk::metrics::MetricError;

#[derive(Debug, Clone)]
pub struct AnonymousTelemetryExporter {
    flight_client: Option<FlightClient>,
}

impl AnonymousTelemetryExporter {
    #[allow(dead_code)]
    pub async fn new(url: Arc<str>) -> Self {
        let flight_client = match FlightClient::try_new(url, Credentials::anonymous(), None).await {
            Ok(client) => Some(client),
            Err(e) => {
                tracing::trace!("Unable to initialize anonymous telemetry: {e}");
                None
            }
        };
        Self { flight_client }
    }
}

#[async_trait]
impl otel_arrow::ArrowExporter for AnonymousTelemetryExporter {
    async fn export(&self, metrics: RecordBatch) -> Result<(), MetricError> {
        let Some(mut flight_client) = self.flight_client.clone() else {
            return Ok(());
        };

        if let Err(e) = flight_client.publish("oss_telemetry", vec![metrics]).await {
            tracing::trace!("Unable to publish anonymous telemetry: {e}");
        };

        Ok(())
    }

    async fn force_flush(&self) -> Result<(), MetricError> {
        Ok(())
    }

    fn shutdown(&self) -> Result<(), MetricError> {
        Ok(())
    }
}
