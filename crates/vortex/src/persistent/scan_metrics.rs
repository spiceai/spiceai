/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Process-wide counters for work initiated by the Vortex scan integration.
//!
//! These count calls, including failed or subsequently cancelled operations. A root-reader
//! construction is not a count of its lazy children, a scan stream is not a completed
//! `prepare`, and a read request is not an OS file open or a blocking task.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Meter, ObservableCounter};

/// Operations visible at the integration boundary, independent of the storage backend.
#[derive(Clone, Copy)]
pub(super) enum ScanOperation {
    FileOpen,
    LayoutReaderRoot,
    NaturalSplits,
    SchemaAdaptation,
    ScanStream,
    ReadRequest,
}

impl ScanOperation {
    const ALL: [Self; 6] = [
        Self::FileOpen,
        Self::LayoutReaderRoot,
        Self::NaturalSplits,
        Self::SchemaAdaptation,
        Self::ScanStream,
        Self::ReadRequest,
    ];

    const fn label(self) -> &'static str {
        match self {
            Self::FileOpen => "file_open",
            Self::LayoutReaderRoot => "layout_reader_root",
            Self::NaturalSplits => "natural_splits",
            Self::SchemaAdaptation => "schema_adaptation",
            Self::ScanStream => "scan_stream",
            Self::ReadRequest => "read_request",
        }
    }

    pub(super) fn record(self) {
        OPERATIONS[self as usize].fetch_add(1, Ordering::Relaxed);
    }
}

static OPERATIONS: [AtomicU64; 6] = [const { AtomicU64::new(0) }; 6];
static INSTRUMENT: OnceLock<ObservableCounter<u64>> = OnceLock::new();

/// Export cumulative Vortex scan setup and read-request calls through the global meter.
///
/// Call after installing the metrics provider. The bounded operation label has no file,
/// expression, or query identity; scans update atomics without calling the exporter.
pub fn register_scan_metrics() {
    INSTRUMENT.get_or_init(|| register(&global::meter("vortex_scan")));
}

fn register(meter: &Meter) -> ObservableCounter<u64> {
    meter
        .u64_observable_counter("vortex_scan_operations")
        .with_description("Calls initiated by Vortex scans, including failed or cancelled calls. Root readers exclude children; read requests are not OS opens or blocking tasks.")
        .with_callback(|observer| {
            for operation in ScanOperation::ALL {
                observer.observe(
                    OPERATIONS[operation as usize].load(Ordering::Relaxed),
                    &[KeyValue::new("operation", operation.label())],
                );
            }
        })
        .build()
}

#[cfg(test)]
mod tests {
    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::metrics::SdkMeterProvider;
    use prometheus::proto::MetricType;

    use super::*;

    #[test]
    fn exports_every_operation_with_a_monotonic_counter() {
        let registry = prometheus::Registry::new();
        let exporter = opentelemetry_prometheus::exporter()
            .with_registry(registry.clone())
            .build()
            .expect("create exporter");
        let provider = SdkMeterProvider::builder().with_reader(exporter).build();
        let _instrument = register(&provider.meter("vortex_scan_test"));
        let collect = || {
            let families = registry.gather();
            let family = families
                .iter()
                .find(|family| family.name() == "vortex_scan_operations_total")
                .expect("scan operations are exported before any scan");
            assert_eq!(family.get_field_type(), MetricType::COUNTER);
            let mut values = std::collections::BTreeMap::new();
            for metric in family.get_metric() {
                let operation = metric
                    .get_label()
                    .iter()
                    .find(|label| label.name() == "operation")
                    .expect("operation label");
                values.insert(operation.value().to_owned(), metric.get_counter().value());
            }
            values
        };
        let before = collect();
        assert_eq!(before.len(), ScanOperation::ALL.len());
        for operation in ScanOperation::ALL {
            operation.record();
        }
        let after = collect();
        for operation in ScanOperation::ALL {
            assert!(after[operation.label()] >= before[operation.label()] + 1.0);
        }
    }
}
