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

use std::sync::{Arc, LazyLock};

use arrow::array::RecordBatch;
use async_trait::async_trait;
use flight_client::{Credentials, FlightClient};
use opentelemetry_sdk::metrics::MetricError;
use secrecy::SecretString;

const ENDPOINT_CONST: &str = "https://telemetry.spiceai.io";

pub static ENDPOINT: LazyLock<Arc<str>> = LazyLock::new(|| {
    std::env::var("SPICEAI_TELEMETRY_ENDPOINT")
        .unwrap_or_else(|_| ENDPOINT_CONST.into())
        .into()
});

#[derive(Debug, Default)]
pub struct TelemetryExporterBuilder {
    api_key: Option<SecretString>,
}

impl TelemetryExporterBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_api_key(mut self, api_key: SecretString) -> Self {
        self.api_key = Some(api_key);
        self
    }

    #[must_use]
    pub async fn build(self, url: Arc<str>) -> TelemetryExporter {
        let credentials = if let Some(api_key) = self.api_key {
            Credentials::new("", api_key)
        } else {
            Credentials::anonymous()
        };

        let flight_client = match FlightClient::try_new(url, credentials, None).await {
            Ok(client) => Some(client),
            Err(e) => {
                tracing::trace!("Unable to initialize anonymous telemetry: {e}");
                None
            }
        };

        TelemetryExporter { flight_client }
    }
}

#[derive(Debug, Clone)]
pub struct TelemetryExporter {
    flight_client: Option<FlightClient>,
}

#[async_trait]
impl otel_arrow::ArrowExporter for TelemetryExporter {
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
