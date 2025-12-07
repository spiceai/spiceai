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

use anyhow::Result;

use opentelemetry::metrics::Meter;

use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::metrics::reader::MetricReader;
use opentelemetry_sdk::{
    Resource,
    metrics::{SdkMeterProvider, data::ResourceMetrics},
};

use secrecy::SecretString;
use telemetry::exporter::TelemetryExporterBuilder;
pub use telemetry::meter::{METER_PROVIDER, METER_PROVIDER_ONCE};
use telemetry::reader::InitialReader;

const ENDPOINT_CONST: &str = "https://telemetry.spiceai.io";

pub static ENDPOINT: LazyLock<Arc<str>> = LazyLock::new(|| {
    std::env::var("SPICEAI_TELEMETRY_ENDPOINT")
        .unwrap_or_else(|_| ENDPOINT_CONST.into())
        .into()
});

pub static METER: LazyLock<Meter> = LazyLock::new(|| METER_PROVIDER.meter("benchmarks_telemetry"));

pub struct Telemetry {
    reader: InitialReader,
    resource: Resource,
    setup: bool,
    api_key: Option<SecretString>,
}

impl Telemetry {
    /// Create telemetry with empty resource.
    /// Use `set_resource()` later to set the actual resource before calling `emit()`.
    #[must_use]
    pub fn new(api_key_name: &str) -> Self {
        let resource = Resource::builder_empty().build();
        Self::new_with_resource(&resource, api_key_name)
    }

    /// Create telemetry with a resource provided upfront.
    ///
    /// Use this when the resource attributes are already available at creation time.
    /// For most cases, prefer `new()` + `set_resource()` to ensure telemetry is initialized
    /// before any metrics calls.
    #[must_use]
    pub fn new_with_resource(resource: &Resource, api_key_name: &str) -> Self {
        let reader = InitialReader::default();

        let provider = SdkMeterProvider::builder()
            .with_resource(resource.clone())
            .with_reader(reader.clone())
            .build();

        let setup = METER_PROVIDER_ONCE.set(Arc::new(provider)).is_ok();
        if !setup {
            println!("Telemetry disabled");
        }

        Self {
            reader,
            resource: resource.clone(),
            setup,
            api_key: std::env::var(api_key_name)
                .ok()
                .as_deref()
                .map(|key| SecretString::new(key.into())),
        }
    }

    /// Set the resource to be used when emitting metrics.
    ///
    /// Call this after collecting all the resource attributes (e.g., `spiced_version`, `commit_sha`)
    /// but before calling `emit()`.
    pub fn set_resource(&mut self, resource: Resource) {
        self.resource = resource;
    }

    pub async fn emit(&self) -> Result<()> {
        if !self.setup {
            return Ok(());
        }

        if let Some(api_key) = &self.api_key {
            println!("Emitting to exporter at {}", *ENDPOINT);
            let telemetry_exporter = otel_arrow::OtelArrowExporter::new(
                TelemetryExporterBuilder::new()
                    .with_credentials(flight_client::Credentials::Bearer {
                        token: api_key.clone().into(),
                        prefix: false,
                    })
                    .with_service_name("benchmarks_telemetry".into())
                    .with_endpoint(Arc::clone(&ENDPOINT))
                    .build()
                    .await?,
            );

            let mut rm = ResourceMetrics {
                resource: self.resource.clone(),
                scope_metrics: vec![],
            };

            self.reader.collect(&mut rm)?;

            // Replace the resource from the provider with our potentially deferred resource.
            // The provider was initialized with an empty resource, but we set the
            // actual resource later via set_resource() once all attributes are known.
            rm.resource = self.resource.clone();

            telemetry_exporter
                .export(&mut rm)
                .await
                .unwrap_or_else(|err| {
                    println!("Failed to export initial telemetry: {err:?}");
                });
        } else {
            println!("No API key provided, telemetry is disabled");
        }

        Ok(())
    }
}
