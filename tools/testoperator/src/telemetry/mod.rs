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

use opentelemetry::metrics::Meter;

use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::metrics::reader::MetricReader;
use opentelemetry_sdk::{
    Resource,
    metrics::{SdkMeterProvider, data::ResourceMetrics},
};

use telemetry::exporter::{ENDPOINT, TelemetryExporterBuilder};
use telemetry::meter::{METER_PROVIDER, METER_PROVIDER_ONCE};

pub(crate) static METER: LazyLock<Meter> =
    LazyLock::new(|| METER_PROVIDER.meter("benchmarks_telemetry"));

pub(crate) struct Telemetry {
    reader: InitialReader,
    resource: Resource,
}

impl Telemetry {
    #[must_use]
    pub(crate) fn new(resource: &Resource) -> Self {
        let reader = InitialReader::default();

        let provider = SdkMeterProvider::builder()
            .with_resource(resource.clone())
            .with_reader(reader.clone())
            .build();

        if METER_PROVIDER_ONCE.set(Arc::new(provider)).is_err() {
            println!("Testoperator metrics are disabled");
        }

        Self {
            reader,
            resource: resource.clone(),
        }
    }

    pub(crate) async fn read(&self, api_key: Option<&str>) -> Result<()> {
        if let Some(api_key) = api_key {
            let telemetry_exporter = otel_arrow::OtelArrowExporter::new(
                TelemetryExporterBuilder::new()
                    .with_api_key(api_key.into())
                    .with_service_name("benchmarks_telemetry")
                    .build(ENDPOINT.clone())
                    .await,
            );

            let mut rm = ResourceMetrics {
                resource: self.resource.clone(),
                scope_metrics: vec![],
            };

            self.reader.collect(&mut rm)?;

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

mod metrics;
pub(crate) use metrics::*;
use telemetry::reader::InitialReader;
use test_framework::anyhow::Result;
