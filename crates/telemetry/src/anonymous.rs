/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::{
    sync::{Arc, LazyLock},
    time::Duration,
};

use crate::exporter::AnonymousTelemetryExporter;
use opentelemetry::global::GlobalMeterProvider;
use opentelemetry_sdk::{
    metrics::{PeriodicReader, SdkMeterProvider},
    runtime::Tokio,
    Resource,
};
use otel_arrow::OtelArrowExporter;

const ENDPOINT_CONST: &str = "https://data.spiceai.io";

/// How often to send telemetry data to the endpoint
const TELEMETRY_INTERVAL_SECONDS: u64 = 3600; // 1 hour
const TELEMETRY_TIMEOUT_SECONDS: u64 = 30;

static ENDPOINT: LazyLock<Arc<str>> = LazyLock::new(|| {
    std::env::var("SPICEAI_ENDPOINT")
        .unwrap_or_else(|_| ENDPOINT_CONST.into())
        .into()
});

pub async fn start() {
    let resource = Resource::default();

    let oss_telemetry_exporter =
        OtelArrowExporter::new(AnonymousTelemetryExporter::new(Arc::clone(&ENDPOINT)).await);

    let periodic_reader = PeriodicReader::builder(oss_telemetry_exporter, Tokio)
        .with_interval(Duration::from_secs(TELEMETRY_INTERVAL_SECONDS))
        .with_timeout(Duration::from_secs(TELEMETRY_TIMEOUT_SECONDS))
        .build();

    let provider = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_reader(periodic_reader)
        .build();

    let _ = crate::meter::METER_PROVIDER.get_or_init(|| GlobalMeterProvider::new(provider));

    crate::meter::METER.u64_counter("start").init().add(1, &[]);

    tracing::debug!("Started anonymous telemetry collection to {}", *ENDPOINT);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_start() {
        start().await;
    }
}
