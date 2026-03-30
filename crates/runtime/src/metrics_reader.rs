/*
Copyright 2026 The Spice.ai OSS Authors

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

//! On-demand metrics collection for cluster observability.
//!
//! This module provides a [`MetricsReader`] that can be used to collect metrics on demand
//! as OTLP protobuf bytes. This is used by:
//! - The `GetMetrics` RPC handler to return local metrics to peer schedulers
//! - Executors responding to metrics requests from schedulers via control stream
//! - The cluster metrics endpoint to collect local metrics before fan-out

use std::sync::{Arc, Weak};

use opentelemetry_proto::tonic::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::{AnyValue, any_value::Value},
};
use opentelemetry_sdk::metrics::{
    InstrumentKind, ManualReader, Pipeline, Temporality, data::ResourceMetrics,
    reader::MetricReader,
};
use prost::Message;

/// A metrics reader that supports on-demand collection of OTLP metrics.
///
/// This reader wraps a [`ManualReader`] and provides a method to collect
/// the current metrics as OTLP protobuf bytes.
///
/// # Usage
///
/// Add this reader to your `SdkMeterProvider`:
///
/// ```ignore
/// use runtime::metrics_reader::MetricsReader;
/// use opentelemetry_sdk::metrics::SdkMeterProvider;
///
/// let metrics_reader = MetricsReader::new();
/// let provider = SdkMeterProvider::builder()
///     .with_reader(metrics_reader.clone())
///     .build();
///
/// // Later, collect metrics on demand:
/// let otlp_bytes = metrics_reader.collect_otlp();
/// ```
#[derive(Debug, Clone)]
pub struct MetricsReader {
    reader: Arc<ManualReader>,
}

impl Default for MetricsReader {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsReader {
    /// Creates a new metrics reader.
    #[must_use]
    pub fn new() -> Self {
        Self {
            reader: Arc::new(ManualReader::builder().build()),
        }
    }

    /// Collects the current metrics as OTLP protobuf bytes.
    ///
    /// Returns an empty Vec if collection fails or there are no metrics.
    #[must_use]
    pub fn collect_otlp(&self) -> Vec<u8> {
        let mut rm = ResourceMetrics::default();

        if let Err(e) = self.reader.collect(&mut rm) {
            tracing::warn!("Failed to collect metrics: {e:?}");
            return Vec::new();
        }

        // Convert SDK ResourceMetrics to OTLP proto ResourceMetrics
        let otlp_request = sdk_metrics_to_otlp(&rm);

        // Encode as protobuf
        otlp_request.encode_to_vec()
    }
}

impl MetricReader for MetricsReader {
    fn register_pipeline(&self, pipeline: Weak<Pipeline>) {
        self.reader.register_pipeline(pipeline);
    }

    fn collect(&self, rm: &mut ResourceMetrics) -> opentelemetry_sdk::error::OTelSdkResult {
        self.reader.collect(rm)
    }

    fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        self.reader.force_flush()
    }

    fn shutdown(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        self.reader.shutdown()
    }

    fn shutdown_with_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> opentelemetry_sdk::error::OTelSdkResult {
        self.reader.shutdown_with_timeout(timeout)
    }

    fn temporality(&self, kind: InstrumentKind) -> Temporality {
        self.reader.temporality(kind)
    }
}

/// Converts OpenTelemetry SDK `ResourceMetrics` to OTLP protobuf `ExportMetricsServiceRequest`.
fn sdk_metrics_to_otlp(rm: &ResourceMetrics) -> ExportMetricsServiceRequest {
    use opentelemetry_proto::tonic::{
        common::v1::{AnyValue, InstrumentationScope, KeyValue},
        metrics::v1::{Metric, ResourceMetrics as OtlpRM, ScopeMetrics},
        resource::v1::Resource,
    };

    let mut request = ExportMetricsServiceRequest::default();

    // Convert resource
    let resource = {
        let r = rm.resource();
        Some(Resource {
            attributes: r
                .iter()
                .map(|(k, v)| KeyValue {
                    key: k.to_string(),
                    value: Some(AnyValue {
                        value: Some(otel_value_to_proto(v)),
                    }),
                })
                .collect(),
            dropped_attributes_count: 0,
            entity_refs: Vec::new(),
        })
    };

    // Convert scope metrics
    let mut scope_metrics_list = Vec::new();
    for sm in rm.scope_metrics() {
        let scope = InstrumentationScope {
            name: sm.scope().name().to_string(),
            version: sm
                .scope()
                .version()
                .map(ToString::to_string)
                .unwrap_or_default(),
            attributes: sm
                .scope()
                .attributes()
                .map(|kv| KeyValue {
                    key: kv.key.to_string(),
                    value: Some(AnyValue {
                        value: Some(otel_value_to_proto(&kv.value)),
                    }),
                })
                .collect(),
            dropped_attributes_count: 0,
        };

        // metrics() returns an iterator directly (no .iter() needed)
        let metrics: Vec<Metric> = sm.metrics().filter_map(convert_metric).collect();

        scope_metrics_list.push(ScopeMetrics {
            scope: Some(scope),
            metrics,
            schema_url: String::new(),
        });
    }

    request.resource_metrics.push(OtlpRM {
        resource,
        scope_metrics: scope_metrics_list,
        schema_url: String::new(),
    });

    request
}

/// Converts an OpenTelemetry Value to protobuf `AnyValue`.
fn otel_value_to_proto(value: &opentelemetry::Value) -> Value {
    match value {
        opentelemetry::Value::Bool(b) => Value::BoolValue(*b),
        opentelemetry::Value::I64(i) => Value::IntValue(*i),
        opentelemetry::Value::F64(f) => Value::DoubleValue(*f),
        opentelemetry::Value::String(s) => Value::StringValue(s.to_string()),
        opentelemetry::Value::Array(arr) => {
            use opentelemetry_proto::tonic::common::v1::ArrayValue;
            let values = match arr {
                opentelemetry::Array::Bool(arr) => arr
                    .iter()
                    .map(|b| AnyValue {
                        value: Some(Value::BoolValue(*b)),
                    })
                    .collect(),
                opentelemetry::Array::I64(arr) => arr
                    .iter()
                    .map(|i| AnyValue {
                        value: Some(Value::IntValue(*i)),
                    })
                    .collect(),
                opentelemetry::Array::F64(arr) => arr
                    .iter()
                    .map(|f| AnyValue {
                        value: Some(Value::DoubleValue(*f)),
                    })
                    .collect(),
                opentelemetry::Array::String(arr) => arr
                    .iter()
                    .map(|s| AnyValue {
                        value: Some(Value::StringValue(s.to_string())),
                    })
                    .collect(),
                // Handle unknown array types that may be added in future versions
                _ => Vec::new(),
            };
            Value::ArrayValue(ArrayValue { values })
        }
        // Handle unknown value types that may be added in future versions
        _ => Value::StringValue(format!("{value:?}")),
    }
}

/// Converts an SDK metric to OTLP protobuf metric.
fn convert_metric(
    metric: &opentelemetry_sdk::metrics::data::Metric,
) -> Option<opentelemetry_proto::tonic::metrics::v1::Metric> {
    use opentelemetry_proto::tonic::metrics::v1::Metric;
    use opentelemetry_sdk::metrics::data::AggregatedMetrics;

    let metric_data = match metric.data() {
        AggregatedMetrics::I64(data) => convert_metric_data_i64(data),
        AggregatedMetrics::U64(data) => convert_metric_data_u64(data),
        AggregatedMetrics::F64(data) => convert_metric_data_f64(data),
    };

    metric_data.map(|data| Metric {
        name: metric.name().to_string(),
        description: metric.description().to_string(),
        unit: metric.unit().to_string(),
        metadata: Vec::new(),
        data: Some(data),
    })
}

/// Converts i64 metric data to OTLP format.
fn convert_metric_data_i64(
    data: &opentelemetry_sdk::metrics::data::MetricData<i64>,
) -> Option<opentelemetry_proto::tonic::metrics::v1::metric::Data> {
    use opentelemetry_proto::tonic::metrics::v1 as otlp;
    use opentelemetry_sdk::metrics::data::MetricData;

    match data {
        MetricData::Gauge(gauge) => {
            let start_time = gauge.start_time().map_or(0, system_time_to_nanos);
            let time = system_time_to_nanos(gauge.time());
            Some(otlp::metric::Data::Gauge(otlp::Gauge {
                data_points: gauge
                    .data_points()
                    .map(|dp| otlp::NumberDataPoint {
                        attributes: convert_attributes_iter(dp.attributes()),
                        start_time_unix_nano: start_time,
                        time_unix_nano: time,
                        value: Some(otlp::number_data_point::Value::AsInt(dp.value())),
                        exemplars: Vec::new(),
                        flags: 0,
                    })
                    .collect(),
            }))
        }
        MetricData::Sum(sum) => {
            let start_time = system_time_to_nanos(sum.start_time());
            let time = system_time_to_nanos(sum.time());
            Some(otlp::metric::Data::Sum(otlp::Sum {
                data_points: sum
                    .data_points()
                    .map(|dp| otlp::NumberDataPoint {
                        attributes: convert_attributes_iter(dp.attributes()),
                        start_time_unix_nano: start_time,
                        time_unix_nano: time,
                        value: Some(otlp::number_data_point::Value::AsInt(dp.value())),
                        exemplars: Vec::new(),
                        flags: 0,
                    })
                    .collect(),
                aggregation_temporality: temporality_to_proto(sum.temporality()),
                is_monotonic: sum.is_monotonic(),
            }))
        }
        #[expect(clippy::cast_precision_loss)]
        MetricData::Histogram(histogram) => {
            let start_time = system_time_to_nanos(histogram.start_time());
            let time = system_time_to_nanos(histogram.time());
            Some(otlp::metric::Data::Histogram(otlp::Histogram {
                data_points: histogram
                    .data_points()
                    .map(|dp| otlp::HistogramDataPoint {
                        attributes: convert_attributes_iter(dp.attributes()),
                        start_time_unix_nano: start_time,
                        time_unix_nano: time,
                        count: dp.count(),
                        sum: Some(dp.sum() as f64),
                        bucket_counts: dp.bucket_counts().collect(),
                        explicit_bounds: dp.bounds().collect(),
                        exemplars: Vec::new(),
                        flags: 0,
                        min: dp.min().map(|m| m as f64),
                        max: dp.max().map(|m| m as f64),
                    })
                    .collect(),
                aggregation_temporality: temporality_to_proto(histogram.temporality()),
            }))
        }
        MetricData::ExponentialHistogram(_) => {
            tracing::trace!("ExponentialHistogram not yet supported, skipping");
            None
        }
    }
}

/// Converts u64 metric data to OTLP format.
#[expect(clippy::cast_possible_wrap)]
fn convert_metric_data_u64(
    data: &opentelemetry_sdk::metrics::data::MetricData<u64>,
) -> Option<opentelemetry_proto::tonic::metrics::v1::metric::Data> {
    use opentelemetry_proto::tonic::metrics::v1 as otlp;
    use opentelemetry_sdk::metrics::data::MetricData;

    match data {
        MetricData::Gauge(gauge) => {
            let start_time = gauge.start_time().map_or(0, system_time_to_nanos);
            let time = system_time_to_nanos(gauge.time());
            Some(otlp::metric::Data::Gauge(otlp::Gauge {
                data_points: gauge
                    .data_points()
                    .map(|dp| otlp::NumberDataPoint {
                        attributes: convert_attributes_iter(dp.attributes()),
                        start_time_unix_nano: start_time,
                        time_unix_nano: time,
                        // u64 -> i64 cast for OTLP compatibility
                        value: Some(otlp::number_data_point::Value::AsInt(dp.value() as i64)),
                        exemplars: Vec::new(),
                        flags: 0,
                    })
                    .collect(),
            }))
        }
        MetricData::Sum(sum) => {
            let start_time = system_time_to_nanos(sum.start_time());
            let time = system_time_to_nanos(sum.time());
            Some(otlp::metric::Data::Sum(otlp::Sum {
                data_points: sum
                    .data_points()
                    .map(|dp| otlp::NumberDataPoint {
                        attributes: convert_attributes_iter(dp.attributes()),
                        start_time_unix_nano: start_time,
                        time_unix_nano: time,
                        value: Some(otlp::number_data_point::Value::AsInt(dp.value() as i64)),
                        exemplars: Vec::new(),
                        flags: 0,
                    })
                    .collect(),
                aggregation_temporality: temporality_to_proto(sum.temporality()),
                is_monotonic: sum.is_monotonic(),
            }))
        }
        #[expect(clippy::cast_precision_loss)]
        MetricData::Histogram(histogram) => {
            let start_time = system_time_to_nanos(histogram.start_time());
            let time = system_time_to_nanos(histogram.time());
            Some(otlp::metric::Data::Histogram(otlp::Histogram {
                data_points: histogram
                    .data_points()
                    .map(|dp| otlp::HistogramDataPoint {
                        attributes: convert_attributes_iter(dp.attributes()),
                        start_time_unix_nano: start_time,
                        time_unix_nano: time,
                        count: dp.count(),
                        sum: Some(dp.sum() as f64),
                        bucket_counts: dp.bucket_counts().collect(),
                        explicit_bounds: dp.bounds().collect(),
                        exemplars: Vec::new(),
                        flags: 0,
                        min: dp.min().map(|m| m as f64),
                        max: dp.max().map(|m| m as f64),
                    })
                    .collect(),
                aggregation_temporality: temporality_to_proto(histogram.temporality()),
            }))
        }
        MetricData::ExponentialHistogram(_) => {
            tracing::trace!("ExponentialHistogram not yet supported, skipping");
            None
        }
    }
}

/// Converts f64 metric data to OTLP format.
fn convert_metric_data_f64(
    data: &opentelemetry_sdk::metrics::data::MetricData<f64>,
) -> Option<opentelemetry_proto::tonic::metrics::v1::metric::Data> {
    use opentelemetry_proto::tonic::metrics::v1 as otlp;
    use opentelemetry_sdk::metrics::data::MetricData;

    match data {
        MetricData::Gauge(gauge) => {
            let start_time = gauge.start_time().map_or(0, system_time_to_nanos);
            let time = system_time_to_nanos(gauge.time());
            Some(otlp::metric::Data::Gauge(otlp::Gauge {
                data_points: gauge
                    .data_points()
                    .map(|dp| otlp::NumberDataPoint {
                        attributes: convert_attributes_iter(dp.attributes()),
                        start_time_unix_nano: start_time,
                        time_unix_nano: time,
                        value: Some(otlp::number_data_point::Value::AsDouble(dp.value())),
                        exemplars: Vec::new(),
                        flags: 0,
                    })
                    .collect(),
            }))
        }
        MetricData::Sum(sum) => {
            let start_time = system_time_to_nanos(sum.start_time());
            let time = system_time_to_nanos(sum.time());
            Some(otlp::metric::Data::Sum(otlp::Sum {
                data_points: sum
                    .data_points()
                    .map(|dp| otlp::NumberDataPoint {
                        attributes: convert_attributes_iter(dp.attributes()),
                        start_time_unix_nano: start_time,
                        time_unix_nano: time,
                        value: Some(otlp::number_data_point::Value::AsDouble(dp.value())),
                        exemplars: Vec::new(),
                        flags: 0,
                    })
                    .collect(),
                aggregation_temporality: temporality_to_proto(sum.temporality()),
                is_monotonic: sum.is_monotonic(),
            }))
        }
        MetricData::Histogram(histogram) => {
            let start_time = system_time_to_nanos(histogram.start_time());
            let time = system_time_to_nanos(histogram.time());
            Some(otlp::metric::Data::Histogram(otlp::Histogram {
                data_points: histogram
                    .data_points()
                    .map(|dp| otlp::HistogramDataPoint {
                        attributes: convert_attributes_iter(dp.attributes()),
                        start_time_unix_nano: start_time,
                        time_unix_nano: time,
                        count: dp.count(),
                        sum: Some(dp.sum()),
                        bucket_counts: dp.bucket_counts().collect(),
                        explicit_bounds: dp.bounds().collect(),
                        exemplars: Vec::new(),
                        flags: 0,
                        min: dp.min(),
                        max: dp.max(),
                    })
                    .collect(),
                aggregation_temporality: temporality_to_proto(histogram.temporality()),
            }))
        }
        MetricData::ExponentialHistogram(_) => {
            tracing::trace!("ExponentialHistogram not yet supported, skipping");
            None
        }
    }
}

/// Converts SDK attributes from an iterator to OTLP `KeyValue` list.
fn convert_attributes_iter<'a>(
    attrs: impl Iterator<Item = &'a opentelemetry::KeyValue>,
) -> Vec<opentelemetry_proto::tonic::common::v1::KeyValue> {
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};

    attrs
        .map(|kv| KeyValue {
            key: kv.key.to_string(),
            value: Some(AnyValue {
                value: Some(otel_value_to_proto(&kv.value)),
            }),
        })
        .collect()
}

/// Converts SDK temporality to OTLP proto temporality.
fn temporality_to_proto(temporality: opentelemetry_sdk::metrics::Temporality) -> i32 {
    use opentelemetry_proto::tonic::metrics::v1::AggregationTemporality;
    use opentelemetry_sdk::metrics::Temporality;

    match temporality {
        Temporality::Delta => AggregationTemporality::Delta as i32,
        Temporality::Cumulative => AggregationTemporality::Cumulative as i32,
        _ => AggregationTemporality::Unspecified as i32,
    }
}

/// Converts a `SystemTime` to nanoseconds since Unix epoch.
fn system_time_to_nanos(time: std::time::SystemTime) -> u64 {
    time.duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_reader_default() {
        let reader = MetricsReader::default();
        // Should not panic
        let _ = reader.collect_otlp();
    }

    #[test]
    fn test_otel_value_to_proto_bool() {
        let value = opentelemetry::Value::Bool(true);
        let proto = otel_value_to_proto(&value);
        assert!(matches!(proto, Value::BoolValue(true)));
    }

    #[test]
    fn test_otel_value_to_proto_i64() {
        let value = opentelemetry::Value::I64(42);
        let proto = otel_value_to_proto(&value);
        assert!(matches!(proto, Value::IntValue(42)));
    }

    #[test]
    fn test_otel_value_to_proto_f64() {
        let value = opentelemetry::Value::F64(std::f64::consts::PI);
        let proto = otel_value_to_proto(&value);
        if let Value::DoubleValue(v) = proto {
            assert!((v - std::f64::consts::PI).abs() < f64::EPSILON);
        } else {
            panic!("Expected DoubleValue");
        }
    }

    #[test]
    fn test_otel_value_to_proto_string() {
        let value = opentelemetry::Value::String("test".into());
        let proto = otel_value_to_proto(&value);
        assert!(matches!(proto, Value::StringValue(s) if s == "test"));
    }
}
