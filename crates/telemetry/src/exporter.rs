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
    service_name: Option<Arc<str>>,
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
    pub fn with_service_name(mut self, service_name: impl Into<Arc<str>>) -> Self {
        self.service_name = Some(service_name.into());
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
                tracing::trace!("Unable to initialize telemetry: {e}");
                None
            }
        };

        TelemetryExporter {
            flight_client,
            service_name: self.service_name.unwrap_or("oss_telemetry".into()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TelemetryExporter {
    flight_client: Option<FlightClient>,
    service_name: Arc<str>,
}

#[async_trait]
impl otel_arrow::ArrowExporter for TelemetryExporter {
    async fn export(&self, metrics: RecordBatch) -> Result<(), MetricError> {
        let Some(mut flight_client) = self.flight_client.clone() else {
            return Ok(());
        };

        if let Err(e) = flight_client
            .publish(&self.service_name, vec![metrics])
            .await
        {
            tracing::trace!("Unable to publish telemetry: {e}");
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
