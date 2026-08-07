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
    common::v1::{AnyValue, KeyValue, any_value::Value},
    metrics::v1::{Metric, metric::Data},
};
use opentelemetry_sdk::metrics::{
    InstrumentKind, ManualReader, Pipeline, Temporality, data::ResourceMetrics,
    reader::MetricReader,
};
use prost::Message;
use snafu::prelude::*;

/// Why an on-demand collection produced no payload.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to collect Runtime metrics: {source}"))]
    Collect {
        source: opentelemetry_sdk::error::OTelSdkError,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

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

    /// Creates a reader that reports cumulative totals rather than per-interval
    /// deltas.
    ///
    /// Pinned rather than inherited because a consumer that pushes on a timer
    /// depends on it: with cumulative totals a skipped or dropped push loses a
    /// data point but no data, since the next one carries the running total.
    #[must_use]
    pub fn new_cumulative() -> Self {
        Self {
            reader: Arc::new(
                ManualReader::builder()
                    .with_temporality(Temporality::Cumulative)
                    .build(),
            ),
        }
    }

    /// Collects the current metrics as an OTLP payload.
    ///
    /// `Ok(None)` means there is nothing to report. That is deliberately a
    /// different outcome from a failed collection: a caller exporting on a timer
    /// has no one watching its return value, so conflating the two would let a
    /// permanently broken collection look exactly like an idle runtime — which
    /// is the symptom the export exists to remove.
    ///
    /// Every attribute the SDK aggregated on is carried through. Dropping a
    /// label is an aggregation — the series that shared it have to be summed —
    /// so it belongs where aggregation happens: an SDK view (which filters on the
    /// key set *before* aggregating) or the metrics backend's own rollup rules.
    /// Removing keys from an already-aggregated batch collapses distinct series
    /// into label-identical duplicates whose values are never summed, which reads
    /// downstream as a duplicate sample and silently loses the rest.
    ///
    /// `app_id` is attached as `scp_app_id`, the label the metrics backend's app
    /// dashboards filter on. A parameter rather than reader state because a
    /// runtime does not know it at boot — the control plane sends it — and taking
    /// it here makes an export payload impossible to build without one.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Collect`] if the underlying reader cannot collect.
    pub fn collect_otlp_export(&self, app_id: &str) -> Result<Option<Vec<u8>>> {
        let mut rm = ResourceMetrics::default();
        self.reader.collect(&mut rm).context(CollectSnafu)?;

        let mut request = sdk_metrics_to_otlp(&rm);
        clear_units(&mut request);
        stamp_app_id(&mut request, app_id);

        // The conversion always emits one ResourceMetrics, so an idle runtime
        // encodes to a resource with no data points rather than to nothing.
        // Sending that would spend a round trip to say nothing.
        if !has_data_points(&request) {
            tracing::debug!("Metrics export: nothing to report (no data points collected)");
            return Ok(None);
        }

        let summary = summarize(&request);
        let payload = request.encode_to_vec();
        tracing::debug!(
            metrics = summary.metrics,
            data_points = summary.data_points,
            names = %summary.names.join(","),
            "Metrics export: contents"
        );
        Ok(Some(payload))
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

/// Clear the unit on every metric in `request`.
///
/// The runtime's Prometheus exporter is built `.without_units()`, so a metric
/// scraped from `/metrics` is named `query_duration_ms` — the unit already lives
/// in the name. A backend that ingests OTLP applies the `OpenTelemetry`
/// Prometheus naming convention instead, expanding the unit and appending it, so
/// the same instrument arrives as `query_duration_ms_milliseconds`. The name is
/// then both duplicated and different from what the scrape path produces, and one
/// dashboard query cannot serve both.
///
/// Clearing the unit applies the existing `.without_units()` decision to this
/// export path too, so both of the runtime's paths name a metric identically
/// whoever consumes them. No information is lost: the unit is in the name.
///
/// Unlike dropping an attribute, this cannot merge series — the unit is metadata
/// and is not part of a metric's identity, so no aggregation is implied.
fn clear_units(request: &mut ExportMetricsServiceRequest) {
    for resource_metrics in &mut request.resource_metrics {
        for scope_metrics in &mut resource_metrics.scope_metrics {
            for metric in &mut scope_metrics.metrics {
                metric.unit.clear();
            }
        }
    }
}

/// Resource attribute naming the app a runtime's telemetry belongs to. The
/// metrics backend's app dashboards filter on it, so a series without it is
/// ingested and then invisible to everything that would read it.
const APP_ID_ATTRIBUTE: &str = "scp_app_id";

/// Attach `app_id` as [`APP_ID_ATTRIBUTE`] on every resource in `request`,
/// replacing any value already there.
///
/// A resource attribute rather than a data-point one: it describes the emitting
/// instance, not an individual measurement, so one copy per batch labels every
/// series in it. That also matches how the collector attributes platform-managed
/// workloads, keeping both ingest paths on one shape.
///
/// Replacing rather than appending matters — protobuf repeated fields permit
/// duplicate keys, and two `scp_app_id` entries on one resource is not a
/// well-defined series.
fn stamp_app_id(request: &mut ExportMetricsServiceRequest, app_id: &str) {
    for resource_metrics in &mut request.resource_metrics {
        let resource = resource_metrics
            .resource
            .get_or_insert_with(Default::default);
        resource.attributes.retain(|kv| kv.key != APP_ID_ATTRIBUTE);
        resource.attributes.push(KeyValue {
            key: APP_ID_ATTRIBUTE.to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(app_id.to_string())),
            }),
            ..Default::default()
        });
    }
}

/// What a collected batch contains, for the export log line.
struct Summary {
    metrics: usize,
    data_points: usize,
    /// Metric names, deduplicated and ordered, for the debug-level breakdown.
    names: Vec<String>,
}

/// Count what `request` carries, so an export can be logged by content rather
/// than only by byte size — a payload that is the right size but the wrong
/// shape is otherwise indistinguishable from a correct one.
fn summarize(request: &ExportMetricsServiceRequest) -> Summary {
    let mut metrics = 0usize;
    let mut data_points = 0usize;
    let mut names = std::collections::BTreeSet::new();

    for resource_metrics in &request.resource_metrics {
        for scope_metrics in &resource_metrics.scope_metrics {
            for metric in &scope_metrics.metrics {
                metrics += 1;
                names.insert(metric.name.clone());
                data_points += data_point_count(metric);
            }
        }
    }

    Summary {
        metrics,
        data_points,
        names: names.into_iter().collect(),
    }
}

/// How many data points `metric` carries, across every aggregation OTLP
/// defines. A metric with no `data` arm carries none.
fn data_point_count(metric: &Metric) -> usize {
    match &metric.data {
        Some(Data::Gauge(gauge)) => gauge.data_points.len(),
        Some(Data::Sum(sum)) => sum.data_points.len(),
        Some(Data::Histogram(histogram)) => histogram.data_points.len(),
        Some(Data::ExponentialHistogram(histogram)) => histogram.data_points.len(),
        Some(Data::Summary(summary)) => summary.data_points.len(),
        None => 0,
    }
}

/// Whether `request` carries any data point at all.
///
/// Tested on the decoded shape rather than on the encoded length, because a
/// request with no metrics still encodes its resource attributes and so is not
/// empty on the wire.
///
/// A declared metric is not a reported one: an instrument registered but never
/// recorded arrives as a `Metric` whose aggregation holds no points, and
/// exporting that spends a round trip to say nothing. The check therefore
/// descends to the points rather than stopping at the metric list.
fn has_data_points(request: &ExportMetricsServiceRequest) -> bool {
    request.resource_metrics.iter().any(|resource_metrics| {
        resource_metrics.scope_metrics.iter().any(|scope_metrics| {
            scope_metrics
                .metrics
                .iter()
                .any(|metric| data_point_count(metric) > 0)
        })
    })
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
                    ..Default::default()
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
                    ..Default::default()
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
            ..Default::default()
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

    fn app_id_of(request: &ExportMetricsServiceRequest) -> Vec<String> {
        request.resource_metrics[0]
            .resource
            .as_ref()
            .expect("resource present")
            .attributes
            .iter()
            .filter(|kv| kv.key == APP_ID_ATTRIBUTE)
            .filter_map(|kv| match kv.value.as_ref()?.value.as_ref()? {
                Value::StringValue(s) => Some(s.clone()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn stamp_app_id_attaches_the_label_when_the_resource_has_none() {
        let mut request = request_with(&[], &[], false);
        stamp_app_id(&mut request, "4002");
        assert_eq!(app_id_of(&request), vec!["4002".to_string()]);
    }

    /// Protobuf repeated fields permit duplicate keys, so appending a second
    /// `scp_app_id` would leave the resource carrying two — not a well-defined
    /// series. Stamping twice must still leave exactly one, with the new value.
    #[test]
    fn stamp_app_id_replaces_rather_than_appends() {
        let mut request = request_with(&[], &[], false);
        stamp_app_id(&mut request, "4002");
        stamp_app_id(&mut request, "3387");
        assert_eq!(app_id_of(&request), vec!["3387".to_string()]);
    }

    /// A resource that arrives with no `Resource` at all still has to end up
    /// labelled — the backend drops what it cannot attribute.
    #[test]
    fn stamp_app_id_creates_a_missing_resource() {
        use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics as OtlpRM;
        let mut request = ExportMetricsServiceRequest {
            resource_metrics: vec![OtlpRM::default()],
        };
        stamp_app_id(&mut request, "4002");
        assert_eq!(app_id_of(&request), vec!["4002".to_string()]);
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

    fn attribute(key: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue("v".to_string())),
            }),
            ..Default::default()
        }
    }

    /// A request carrying a resource but no metrics, which is what an idle
    /// runtime converts to.
    fn request_with(
        resource_keys: &[&str],
        data_point_keys: &[&str],
        with_metric: bool,
    ) -> ExportMetricsServiceRequest {
        use opentelemetry_proto::tonic::metrics::v1::{
            Metric, NumberDataPoint, ResourceMetrics as OtlpRM, ScopeMetrics, Sum,
        };
        use opentelemetry_proto::tonic::resource::v1::Resource;

        let metrics = if with_metric {
            vec![Metric {
                name: "m".to_string(),
                data: Some(Data::Sum(Sum {
                    data_points: vec![NumberDataPoint {
                        attributes: data_point_keys.iter().copied().map(attribute).collect(),
                        ..Default::default()
                    }],
                    ..Default::default()
                })),
                ..Default::default()
            }]
        } else {
            Vec::new()
        };

        ExportMetricsServiceRequest {
            resource_metrics: vec![OtlpRM {
                resource: Some(Resource {
                    attributes: resource_keys.iter().copied().map(attribute).collect(),
                    ..Default::default()
                }),
                scope_metrics: vec![ScopeMetrics {
                    metrics,
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
        }
    }

    /// The exported payload carries no unit, matching the Prometheus exporter's
    /// `.without_units()`. A backend that expanded `ms` into the metric name
    /// would produce `query_duration_ms_milliseconds`, which is both a duplicated
    /// unit and a different name from the one the scrape path publishes.
    #[test]
    fn units_are_cleared_so_a_backend_cannot_append_them_to_the_name() {
        use opentelemetry_proto::tonic::metrics::v1::{
            Metric, ResourceMetrics as OtlpRM, ScopeMetrics, Sum,
        };

        let mut request = ExportMetricsServiceRequest {
            resource_metrics: vec![OtlpRM {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "query_duration_ms".to_string(),
                        unit: "ms".to_string(),
                        data: Some(Data::Sum(Sum::default())),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        clear_units(&mut request);

        let metric = &request.resource_metrics[0].scope_metrics[0].metrics[0];
        assert_eq!(metric.unit, "", "the unit must not reach the backend");
        assert_eq!(
            metric.name, "query_duration_ms",
            "the name already carries the unit and must be untouched"
        );
    }

    /// An idle runtime still converts to a request carrying its resource, so
    /// emptiness has to be judged on data points rather than on encoded length.
    #[test]
    fn a_resource_without_metrics_has_no_data_points() {
        let idle = request_with(&["service_name"], &[], false);
        assert!(
            !has_data_points(&idle),
            "a resource-only request reports nothing"
        );
        assert!(
            !idle.encode_to_vec().is_empty(),
            "yet it is not empty on the wire, which is why length is the wrong test"
        );

        let busy = request_with(&["service_name"], &["protocol"], true);
        assert!(has_data_points(&busy));
    }

    /// An instrument that was registered but never recorded arrives as a metric
    /// whose aggregation holds no points. Counting the metric rather than its
    /// points would export a payload the log line calls empty.
    #[test]
    fn a_metric_without_points_reports_nothing() {
        use opentelemetry_proto::tonic::metrics::v1::{
            ResourceMetrics as OtlpRM, ScopeMetrics, Sum,
        };

        let declared_only = ExportMetricsServiceRequest {
            resource_metrics: vec![OtlpRM {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "never_recorded".to_string(),
                        data: Some(Data::Sum(Sum::default())),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        assert!(
            !has_data_points(&declared_only),
            "a metric carrying no points has nothing to report"
        );
    }

    /// The export reader reports cumulative totals, which is what makes a
    /// dropped push lose a data point rather than data.
    #[test]
    fn the_export_reader_is_cumulative() {
        let reader = MetricsReader::new_cumulative();
        assert_eq!(
            reader.temporality(InstrumentKind::Counter),
            Temporality::Cumulative
        );
    }

    /// A collection that cannot run is an error, not silence.
    ///
    /// An unregistered reader is the reachable form of that failure, and it is
    /// exactly the case the older accessor cannot express: it returns an empty
    /// payload, indistinguishable from a runtime with nothing to say.
    #[test]
    fn a_failed_collection_is_not_reported_as_nothing_to_report() {
        let reader = MetricsReader::new_cumulative();

        reader
            .collect_otlp_export("4002")
            .expect_err("an unregistered reader cannot collect");

        assert!(
            reader.collect_otlp().is_empty(),
            "the older accessor reports the same failure as emptiness, which is what the \
             fallible one exists to separate"
        );
    }
}
