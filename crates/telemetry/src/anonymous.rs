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
    sync::{Arc, LazyLock, Weak},
    time::Duration,
};

use crate::exporter::AnonymousTelemetryExporter;
use opentelemetry::{global::GlobalMeterProvider, KeyValue};
use opentelemetry_sdk::{
    metrics::{
        data::{ResourceMetrics, Temporality},
        exporter::PushMetricsExporter,
        reader::{AggregationSelector, MetricReader, TemporalitySelector},
        Aggregation, InstrumentKind, ManualReader, PeriodicReader, Pipeline, SdkMeterProvider,
    },
    runtime::Tokio,
    Resource,
};
use otel_arrow::OtelArrowExporter;
use sha2::{Digest, Sha256};

const ENDPOINT_CONST: &str = "https://telemetry.spiceai.io";

/// How often to send telemetry data to the endpoint
const TELEMETRY_INTERVAL_SECONDS: u64 = 3600; // 1 hour
const TELEMETRY_TIMEOUT_SECONDS: u64 = 30;

static ENDPOINT: LazyLock<Arc<str>> = LazyLock::new(|| {
    std::env::var("SPICEAI_TELEMETRY_ENDPOINT")
        .unwrap_or_else(|_| ENDPOINT_CONST.into())
        .into()
});

static INTERNAL_BUILD: LazyLock<bool> = LazyLock::new(|| {
    std::env::var("SPICEAI_INTERNAL_BUILD")
        .map(|v| v == "true")
        .unwrap_or(false)
});

fn resource(spicepod_name: &str) -> Resource {
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

    let mut key_values = vec![
        KeyValue::new("service.name", "spiced"), // May be overridden by setting OTEL_SERVICE_NAME env variable
        KeyValue::new("name", "spiced"),
        KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        #[cfg(debug_assertions)]
        KeyValue::new("service.build", "dev"),
        #[cfg(not(debug_assertions))]
        KeyValue::new("service.build", "release"),
        KeyValue::new("service.instance.id", instance_id),
        KeyValue::new("spicepod.id", spicepod_id),
    ];

    if *INTERNAL_BUILD {
        key_values.push(KeyValue::new("service.internal", "true"));
    } else {
        key_values.push(KeyValue::new("service.internal", "false"));
    }

    Resource::new(key_values)
}

pub async fn start(spicepod_name: &str) {
    let resource = resource(spicepod_name);

    let oss_telemetry_exporter =
        OtelArrowExporter::new(AnonymousTelemetryExporter::new(Arc::clone(&ENDPOINT)).await);

    let periodic_reader = PeriodicReader::builder(oss_telemetry_exporter.clone(), Tokio)
        .with_interval(Duration::from_secs(TELEMETRY_INTERVAL_SECONDS))
        .with_timeout(Duration::from_secs(TELEMETRY_TIMEOUT_SECONDS))
        .build();

    let initial_reader = InitialReader::new();

    let provider = SdkMeterProvider::builder()
        .with_resource(resource.clone())
        .with_reader(periodic_reader)
        .with_reader(initial_reader.clone())
        .build();

    if crate::meter::METER_PROVIDER_ONCE
        .set(GlobalMeterProvider::new(provider))
        .is_err()
    {
        tracing::trace!("Failed to set global meter provider for the anonymous telemetry, already set by another codepath?");
    }

    // Send an initial telemetry event to indicate the start of telemetry collection
    crate::QUERY_COUNT.add(0, &[]);

    let mut rm = ResourceMetrics {
        resource,
        scope_metrics: vec![],
    };

    if let Err(err) = initial_reader.collect(&mut rm) {
        tracing::trace!("Failed to collect initial telemetry: {:?}", err);
    };

    oss_telemetry_exporter
        .export(&mut rm)
        .await
        .unwrap_or_else(|err| {
            tracing::trace!("Failed to export initial telemetry: {:?}", err);
        });

    tracing::trace!("Started anonymous telemetry collection to {}", *ENDPOINT);
}

#[derive(Debug, Clone)]
struct InitialReader {
    reader: Arc<ManualReader>,
}

impl InitialReader {
    pub fn new() -> Self {
        Self {
            reader: Arc::new(ManualReader::builder().build()),
        }
    }
}

impl MetricReader for InitialReader {
    fn register_pipeline(&self, pipeline: Weak<Pipeline>) {
        self.reader.register_pipeline(pipeline);
    }

    fn collect(&self, rm: &mut ResourceMetrics) -> opentelemetry::metrics::Result<()> {
        self.reader.collect(rm)
    }

    fn force_flush(&self) -> opentelemetry::metrics::Result<()> {
        self.reader.force_flush()
    }

    fn shutdown(&self) -> opentelemetry::metrics::Result<()> {
        self.reader.shutdown()
    }
}

impl TemporalitySelector for InitialReader {
    fn temporality(&self, kind: InstrumentKind) -> Temporality {
        self.reader.temporality(kind)
    }
}

impl AggregationSelector for InitialReader {
    fn aggregation(&self, kind: InstrumentKind) -> Aggregation {
        self.reader.aggregation(kind)
    }
}
