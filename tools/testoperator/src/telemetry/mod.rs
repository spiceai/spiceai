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

use std::sync::{Arc, LazyLock, OnceLock};

use opentelemetry::metrics::{Meter, MeterProvider};

use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::{
    Resource,
    metrics::{SdkMeterProvider, data::ResourceMetrics},
};
use telemetry::noop::NoopMeterProvider;

const ENDPOINT_CONST: &str = "https://telemetry.spiceai.io";

static ENDPOINT: LazyLock<Arc<str>> = LazyLock::new(|| {
    std::env::var("SPICEAI_TELEMETRY_ENDPOINT")
        .unwrap_or_else(|_| ENDPOINT_CONST.into())
        .into()
});

pub(crate) static METER_PROVIDER_ONCE: OnceLock<Arc<dyn MeterProvider + Send + Sync>> =
    OnceLock::new();

static METER_PROVIDER: LazyLock<&'static Arc<dyn MeterProvider + Send + Sync>> =
    LazyLock::new(|| METER_PROVIDER_ONCE.get_or_init(|| Arc::new(NoopMeterProvider::new())));

pub(crate) static METER: LazyLock<Meter> =
    LazyLock::new(|| METER_PROVIDER.meter("benchmarks_telemetry"));

pub(crate) async fn setup(resource: Resource, api_key: Arc<str>) {
    // TODO: setup an exporter with API key
    let telemetry_exporter = otel_arrow::OtelArrowExporter::new(
        AuthenticatedTelemetryExporter::new(ENDPOINT.clone(), api_key).await,
    );

    let provider = SdkMeterProvider::builder()
        .with_resource(resource.clone())
        .build();

    if METER_PROVIDER_ONCE.set(Arc::new(provider)).is_err() {
        println!("Testoperator metrics are disabled");
    }

    let mut rm = ResourceMetrics {
        resource,
        scope_metrics: vec![],
    };

    telemetry_exporter
        .export(&mut rm)
        .await
        .unwrap_or_else(|err| {
            println!("Failed to export initial telemetry: {err:?}");
        });
}

mod exporter;
mod metrics;
pub(crate) use exporter::*;
pub(crate) use metrics::*;
