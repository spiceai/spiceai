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

use std::{
    sync::{Arc, LazyLock},
    time::Duration,
};

use crate::{exporter::TelemetryExporterBuilder, hardware::HardwareInfo, reader::InitialReader};
use opentelemetry::KeyValue;
use opentelemetry_sdk::{
    Resource,
    metrics::{
        PeriodicReader, SdkMeterProvider, data::ResourceMetrics, exporter::PushMetricExporter,
        reader::MetricReader,
    },
};
use otel_arrow::OtelArrowExporter;
use sha2::{Digest, Sha256};

const ENDPOINT_CONST: &str = "https://telemetry.spiceai.io";

pub static ENDPOINT: LazyLock<Arc<str>> = LazyLock::new(|| {
    std::env::var("SPICEAI_TELEMETRY_ENDPOINT")
        .unwrap_or_else(|_| ENDPOINT_CONST.into())
        .into()
});

/// How often to send telemetry data to the endpoint
const TELEMETRY_INTERVAL_SECONDS: u64 = 3600; // 1 hour

/// Converts a usize to i64 for telemetry, logging a warning if overflow occurs.
fn usize_to_i64_telemetry(value: usize, metric_name: &str) -> i64 {
    i64::try_from(value).unwrap_or_else(|_| {
        tracing::warn!("{metric_name} value {value} exceeds i64::MAX, clamping to i64::MAX");
        i64::MAX
    })
}

/// Converts a u64 to i64 for telemetry, logging a warning if overflow occurs.
fn u64_to_i64_telemetry(value: u64, metric_name: &str) -> i64 {
    i64::try_from(value).unwrap_or_else(|_| {
        tracing::warn!("{metric_name} value {value} exceeds i64::MAX, clamping to i64::MAX");
        i64::MAX
    })
}

async fn resource(spicepod_name: &str, telemetry_properties: Vec<KeyValue>) -> Resource {
    let hostname = hostname::get()
        .unwrap_or_else(|_| "unknown".into())
        .into_encoded_bytes();

    // instance_id = SHA256(hostname + spicepod_name)
    let mut instance_id_hasher = Sha256::new();
    instance_id_hasher.update(hostname);
    instance_id_hasher.update(spicepod_name);
    let instance_id = format!("{:x}", instance_id_hasher.finalize());

    // spicepod_id = SHA256(spicepod_name)
    let mut spicepod_id_hasher = Sha256::new();
    spicepod_id_hasher.update(spicepod_name);
    let spicepod_id = format!("{:x}", spicepod_id_hasher.finalize());

    // Detect hardware info (vCPUs, GPUs, and memory) using async version
    // to avoid blocking the async runtime
    let hardware_info = HardwareInfo::detect_async().await.unwrap_or_else(|e| {
        tracing::warn!("Failed to detect hardware info: {e}. Using default values.");
        HardwareInfo::default()
    });

    Resource::builder_empty()
        .with_attributes(telemetry_properties.into_iter().chain(vec![
            KeyValue::new("service.name", "spiced"), // May be overridden by setting OTEL_SERVICE_NAME env variable
            KeyValue::new("name", "spiced"),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            KeyValue::new("service.instance.id", instance_id),
            KeyValue::new("spicepod.id", spicepod_id),
            KeyValue::new(
                "host.cpu.count",
                usize_to_i64_telemetry(hardware_info.vcpu_count, "host.cpu.count"),
            ),
            KeyValue::new(
                "host.gpu.count",
                usize_to_i64_telemetry(hardware_info.gpu_count, "host.gpu.count"),
            ),
            KeyValue::new(
                "host.memory.bytes",
                u64_to_i64_telemetry(hardware_info.total_memory_bytes, "host.memory.bytes"),
            ),
        ]))
        .build()
}

pub async fn start(spicepod_name: &str, telemetry_properties: Vec<KeyValue>) {
    let resource = resource(spicepod_name, telemetry_properties).await;

    let Ok(exporter) = TelemetryExporterBuilder::new()
        .with_endpoint(Arc::clone(&ENDPOINT))
        .with_service_name("oss_telemetry".into())
        .build()
        .await
    else {
        tracing::trace!("Failed to setup telemetry exporter - skipping telemetry");
        return;
    };

    let oss_telemetry_exporter = OtelArrowExporter::new(exporter);

    let periodic_reader = PeriodicReader::builder(oss_telemetry_exporter.clone())
        .with_interval(Duration::from_secs(TELEMETRY_INTERVAL_SECONDS))
        .build();

    let initial_reader = InitialReader::new();

    let provider = SdkMeterProvider::builder()
        .with_resource(resource.clone())
        .with_reader(periodic_reader)
        .with_reader(initial_reader.clone())
        .build();

    if crate::meter::METER_PROVIDER_ONCE
        .set(Arc::new(provider))
        .is_err()
    {
        tracing::trace!(
            "Failed to set global meter provider for the anonymous telemetry, already set by another codepath?"
        );
    }

    // Send an initial telemetry event to indicate the start of telemetry collection
    crate::QUERY_COUNT.add(0, &[]);

    let mut rm = ResourceMetrics::default();

    if let Err(err) = initial_reader.collect(&mut rm) {
        tracing::trace!("Failed to collect initial telemetry: {:?}", err);
    }

    oss_telemetry_exporter
        .export(&rm)
        .await
        .unwrap_or_else(|err| {
            tracing::trace!("Failed to export initial telemetry: {:?}", err);
        });

    tracing::trace!("Started anonymous telemetry collection to {}", *ENDPOINT);
}
