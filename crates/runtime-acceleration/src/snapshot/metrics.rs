/*
Copyright 2025 The Spice.ai OSS Authors
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

use std::sync::LazyLock;

use opentelemetry::{
    KeyValue, global,
    metrics::{Counter, Gauge, Histogram, Meter},
};

static METER: LazyLock<Meter> =
    LazyLock::new(|| global::meter("dataset_acceleration_snapshot_metrics"));

static SNAPSHOT_BOOTSTRAP_DURATION_MS: LazyLock<Counter<f64>> = LazyLock::new(|| {
    METER
        .f64_counter("dataset_acceleration_snapshot_bootstrap_duration_ms")
        .with_description(
            "Time in milliseconds taken to download the snapshot used to bootstrap acceleration.",
        )
        .build()
});

static SNAPSHOT_BOOTSTRAP_BYTES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("dataset_acceleration_snapshot_bootstrap_bytes")
        .with_description(
            "Number of bytes downloaded when bootstrapping the acceleration from a snapshot.",
        )
        .build()
});

static SNAPSHOT_BOOTSTRAP_CHECKSUM: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    METER
        .f64_gauge("dataset_acceleration_snapshot_bootstrap_checksum")
        .with_description("Checksum of the snapshot downloaded during bootstrap. Emitted with dataset and checksum attributes.")
        .build()
});

static SNAPSHOT_FAILURE_COUNT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("dataset_acceleration_snapshot_failure_count")
        .with_description("Number of failures encountered while writing snapshots.")
        .build()
});

static SNAPSHOT_WRITE_TIMESTAMP: LazyLock<Gauge<i64>> = LazyLock::new(|| {
    METER
        .i64_gauge("dataset_acceleration_snapshot_write_timestamp")
        .with_description("Unix timestamp (seconds) when the most recent snapshot write completed.")
        .build()
});

static SNAPSHOT_WRITE_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("dataset_acceleration_snapshot_write_duration_ms")
        .with_description(
            "Time in milliseconds taken to write the latest snapshot to object storage.",
        )
        .with_unit("ms")
        .build()
});

static SNAPSHOT_WRITE_BYTES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("dataset_acceleration_snapshot_write_bytes")
        .with_description("Number of bytes written for the most recent snapshot.")
        .build()
});

static SNAPSHOT_WRITE_CHECKSUM: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    METER
        .f64_gauge("dataset_acceleration_snapshot_write_checksum")
        .with_description("Checksum of the most recent snapshot write. Emitted with dataset and checksum attributes.")
        .build()
});

fn dataset_label(dataset: &str) -> KeyValue {
    KeyValue::new("dataset", dataset.to_string())
}

pub fn record_bootstrap_metrics(dataset: &str, duration_ms: f64, bytes: u64, checksum: &str) {
    let dataset_attr = dataset_label(dataset);
    let duration_labels = [dataset_attr.clone()];
    SNAPSHOT_BOOTSTRAP_DURATION_MS.add(duration_ms, &duration_labels);

    let bytes_labels = [dataset_attr.clone()];
    SNAPSHOT_BOOTSTRAP_BYTES.record(bytes, &bytes_labels);

    let checksum_labels = [
        dataset_attr,
        KeyValue::new("checksum", checksum.to_string()),
    ];
    SNAPSHOT_BOOTSTRAP_CHECKSUM.record(1.0, &checksum_labels);
}

pub fn record_snapshot_failure(dataset: &str) {
    let labels = [dataset_label(dataset)];
    SNAPSHOT_FAILURE_COUNT.add(1, &labels);
}

pub fn record_write_metrics(
    dataset: &str,
    timestamp_secs: i64,
    duration_ms: f64,
    bytes: u64,
    checksum: &str,
) {
    let dataset_attr = dataset_label(dataset);

    let timestamp_labels = [dataset_attr.clone()];
    SNAPSHOT_WRITE_TIMESTAMP.record(timestamp_secs, &timestamp_labels);

    let duration_labels = [dataset_attr.clone()];
    SNAPSHOT_WRITE_DURATION_MS.record(duration_ms, &duration_labels);

    let byte_labels = [dataset_attr.clone()];
    SNAPSHOT_WRITE_BYTES.record(bytes, &byte_labels);

    let checksum_labels = [
        dataset_attr,
        KeyValue::new("checksum", checksum.to_string()),
    ];
    SNAPSHOT_WRITE_CHECKSUM.record(1.0, &checksum_labels);
}
