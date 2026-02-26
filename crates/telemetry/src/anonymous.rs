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

    // instance_id = SHA256(hostname + ":" + spicepod_name)
    // The ":" separator prevents collisions between different (hostname, spicepod_name)
    // pairs (e.g., "ab"+"c" vs "a"+"bc").
    let mut instance_id_hasher = Sha256::new();
    instance_id_hasher.update(hostname);
    instance_id_hasher.update(b":");
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

    let provider: Arc<dyn opentelemetry::metrics::MeterProvider + Send + Sync> = Arc::new(provider);

    if crate::meter::METER_PROVIDER_ONCE
        .set(Arc::clone(&provider))
        .is_err()
    {
        tracing::trace!(
            "Failed to set global meter provider for the anonymous telemetry, already set by another codepath?"
        );
    }

    // Initialize the global meter AFTER the provider is set.
    // Using OnceLock prevents the race where early access permanently locks
    // the meter to a noop provider (which happened with LazyLock).
    if crate::meter::METER
        .set(provider.meter("oss_telemetry"))
        .is_err()
    {
        tracing::trace!("Global meter already initialized by another codepath");
    }

    // Register the query counter so it appears in the initial export.
    // Recording 0 avoids phantom counts while still registering the instrument.
    crate::register_query_counter(&[]);

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_usize_to_i64_telemetry_normal_value() {
        assert_eq!(usize_to_i64_telemetry(0, "test"), 0);
        assert_eq!(usize_to_i64_telemetry(42, "test"), 42);
        assert_eq!(usize_to_i64_telemetry(1_000_000, "test"), 1_000_000);
    }

    #[test]
    fn test_usize_to_i64_telemetry_max_i64() {
        let max_i64_as_usize = usize::try_from(i64::MAX).expect("i64::MAX fits in usize on 64-bit");
        assert_eq!(usize_to_i64_telemetry(max_i64_as_usize, "test"), i64::MAX);
    }

    #[test]
    fn test_usize_to_i64_telemetry_overflow_clamps() {
        // Values above i64::MAX should clamp to i64::MAX
        let overflow_value = usize::try_from(i64::MAX)
            .expect("i64::MAX fits in usize on 64-bit")
            .saturating_add(1);
        assert_eq!(
            usize_to_i64_telemetry(overflow_value, "test"),
            i64::MAX,
            "Values exceeding i64::MAX should clamp to i64::MAX"
        );
    }

    #[test]
    fn test_u64_to_i64_telemetry_normal_value() {
        assert_eq!(u64_to_i64_telemetry(0, "test"), 0);
        assert_eq!(u64_to_i64_telemetry(42, "test"), 42);
        assert_eq!(u64_to_i64_telemetry(1_000_000, "test"), 1_000_000);
    }

    #[test]
    fn test_u64_to_i64_telemetry_max_i64() {
        #[expect(clippy::cast_sign_loss)]
        let max_i64_as_u64 = i64::MAX as u64;
        assert_eq!(u64_to_i64_telemetry(max_i64_as_u64, "test"), i64::MAX);
    }

    #[test]
    fn test_u64_to_i64_telemetry_overflow_clamps() {
        assert_eq!(u64_to_i64_telemetry(u64::MAX, "test"), i64::MAX);

        #[expect(clippy::cast_sign_loss)]
        let just_over = (i64::MAX as u64) + 1;
        assert_eq!(u64_to_i64_telemetry(just_over, "test"), i64::MAX);
    }

    #[tokio::test]
    async fn test_resource_hashes_hostname_and_spicepod_name() {
        let resource = resource("test-spicepod", vec![]).await;

        // Verify instance_id is a SHA256 hash (64 hex chars)
        let instance_id = resource
            .iter()
            .find(|(k, _)| k.as_str() == "service.instance.id")
            .map(|(_, v)| v.to_string())
            .expect("resource should have service.instance.id");
        assert_eq!(
            instance_id.len(),
            64,
            "instance_id should be a SHA256 hex string"
        );
        assert!(
            instance_id.chars().all(|c| c.is_ascii_hexdigit()),
            "instance_id should contain only hex characters"
        );

        // Verify spicepod_id is deterministic SHA256 of the name
        let spicepod_id = resource
            .iter()
            .find(|(k, _)| k.as_str() == "spicepod.id")
            .map(|(_, v)| v.to_string())
            .expect("resource should have spicepod.id");
        let mut expected_hasher = Sha256::new();
        expected_hasher.update("test-spicepod");
        let expected_hash = format!("{:x}", expected_hasher.finalize());
        assert_eq!(
            spicepod_id, expected_hash,
            "spicepod_id should be SHA256 of the spicepod name"
        );
    }

    #[tokio::test]
    async fn test_resource_does_not_contain_raw_spicepod_name() {
        let resource = resource("my-secret-project", vec![]).await;

        // Verify the raw spicepod name doesn't appear in any attribute value
        for (key, value) in &resource {
            let value_str = value.to_string();
            assert!(
                !value_str.contains("my-secret-project"),
                "Resource attribute '{key}' contains raw spicepod name: {value_str}"
            );
        }
    }

    #[tokio::test]
    async fn test_resource_contains_required_attributes() {
        let resource = resource("test", vec![]).await;

        let keys: Vec<&str> = resource.iter().map(|(k, _)| k.as_str()).collect();
        assert!(keys.contains(&"service.name"), "Missing service.name");
        assert!(keys.contains(&"service.version"), "Missing service.version");
        assert!(
            keys.contains(&"service.instance.id"),
            "Missing service.instance.id"
        );
        assert!(keys.contains(&"spicepod.id"), "Missing spicepod.id");
        assert!(keys.contains(&"host.cpu.count"), "Missing host.cpu.count");
        assert!(keys.contains(&"host.gpu.count"), "Missing host.gpu.count");
        assert!(
            keys.contains(&"host.memory.bytes"),
            "Missing host.memory.bytes"
        );
    }

    #[tokio::test]
    async fn test_resource_includes_custom_properties() {
        let properties = vec![
            KeyValue::new("custom.key", "custom_value"),
            KeyValue::new("env", "staging"),
        ];
        let resource = resource("test", properties).await;

        let custom = resource
            .iter()
            .find(|(k, _)| k.as_str() == "custom.key")
            .map(|(_, v)| v.to_string());
        assert_eq!(
            custom.as_deref(),
            Some("custom_value"),
            "Custom property should be present in resource"
        );
    }

    #[tokio::test]
    async fn test_resource_deterministic_for_same_inputs() {
        let r1 = resource("deterministic-test", vec![]).await;
        let r2 = resource("deterministic-test", vec![]).await;

        let spicepod_id_1 = r1
            .iter()
            .find(|(k, _)| k.as_str() == "spicepod.id")
            .map(|(_, v)| v.to_string())
            .expect("should have spicepod.id");
        let spicepod_id_2 = r2
            .iter()
            .find(|(k, _)| k.as_str() == "spicepod.id")
            .map(|(_, v)| v.to_string())
            .expect("should have spicepod.id");
        assert_eq!(
            spicepod_id_1, spicepod_id_2,
            "Same inputs should produce same spicepod_id"
        );

        let instance_id_1 = r1
            .iter()
            .find(|(k, _)| k.as_str() == "service.instance.id")
            .map(|(_, v)| v.to_string())
            .expect("should have service.instance.id");
        let instance_id_2 = r2
            .iter()
            .find(|(k, _)| k.as_str() == "service.instance.id")
            .map(|(_, v)| v.to_string())
            .expect("should have service.instance.id");
        assert_eq!(
            instance_id_1, instance_id_2,
            "Same inputs on same host should produce same instance_id"
        );
    }

    #[tokio::test]
    async fn test_resource_different_names_produce_different_ids() {
        let r1 = resource("project-a", vec![]).await;
        let r2 = resource("project-b", vec![]).await;

        let id1 = r1
            .iter()
            .find(|(k, _)| k.as_str() == "spicepod.id")
            .map(|(_, v)| v.to_string())
            .expect("should have spicepod.id");
        let id2 = r2
            .iter()
            .find(|(k, _)| k.as_str() == "spicepod.id")
            .map(|(_, v)| v.to_string())
            .expect("should have spicepod.id");
        assert_ne!(
            id1, id2,
            "Different spicepod names should produce different spicepod_ids"
        );
    }
}
