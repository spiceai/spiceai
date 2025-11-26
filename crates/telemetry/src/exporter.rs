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
use opentelemetry_sdk::error::OTelSdkResult;
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "An endpoint is required to connect to telemetry. Supply an endpoint to the telemetry builder. Report a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    MissingEndpoint,
    #[snafu(display(
        "A service name is required to connect to telemetry. Supply a service name to the telemetry builder. Report a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    MissingServiceName,
}

#[derive(Debug, Default)]
pub struct TelemetryExporterBuilder {
    credentials: Option<Credentials>,
    service_name: Option<Arc<str>>,
    endpoint: Option<Arc<str>>,
}

impl TelemetryExporterBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_credentials(mut self, credentials: Credentials) -> Self {
        self.credentials = Some(credentials);
        self
    }

    #[must_use]
    pub fn with_service_name(mut self, service_name: Arc<str>) -> Self {
        self.service_name = Some(service_name);
        self
    }

    #[must_use]
    pub fn with_endpoint(mut self, endpoint: Arc<str>) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    /// Creates a new telemetry exporter.
    ///
    /// # Errors
    ///
    /// Returns an error if the endpoint is not set.
    pub async fn build(self) -> Result<TelemetryExporter, Error> {
        let credentials = self.credentials.unwrap_or(Credentials::anonymous());

        let endpoint = self.endpoint.ok_or(Error::MissingEndpoint)?;
        let flight_client = match FlightClient::try_new(endpoint, credentials, None).await {
            Ok(client) => Some(client),
            Err(e) => {
                tracing::trace!("Unable to initialize telemetry: {e}");
                None
            }
        };

        let service_name = self.service_name.ok_or(Error::MissingServiceName)?;

        Ok(TelemetryExporter {
            flight_client,
            service_name,
        })
    }
}

#[derive(Debug, Clone)]
pub struct TelemetryExporter {
    flight_client: Option<FlightClient>,
    service_name: Arc<str>,
}

#[async_trait]
impl otel_arrow::ArrowExporter for TelemetryExporter {
    async fn export(&self, metrics: RecordBatch) -> OTelSdkResult {
        let Some(mut flight_client) = self.flight_client.clone() else {
            return Ok(());
        };

        if let Err(e) = flight_client
            .publish(&self.service_name, vec![metrics])
            .await
        {
            tracing::trace!("Unable to publish telemetry: {e}");
        }

        Ok(())
    }

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    fn shutdown(&self) -> OTelSdkResult {
        Ok(())
    }
}
