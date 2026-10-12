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

//! The OTLP metrics store: one table with a narrow, fixed schema that holds every metric
//! of an export (#15060).
//!
//! A `sink:` dataset opts in with the `otlp_metrics: all` param. Every data point becomes one
//! row that keeps the metric's semantics (type, temporality, monotonicity, unit) and carries
//! its attributes as a JSON object, so a new attribute key never changes the table schema.
//!
//! `series_hash` identifies a series (metric name, resource attributes, scope, and point
//! attributes), so `(time_unix_nano, series_hash)` is a complete key and a retried export
//! writes rows that a primary key deduplicates.

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, ListBuilder,
    StringBuilder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use base64::Engine;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value;
use serde_json::Value as JsonValue;

/// Dataset param that routes OTLP metrics into a dataset. The only accepted value is `all`.
pub const OTLP_METRICS_PARAM: &str = "otlp_metrics";
/// The value of [`OTLP_METRICS_PARAM`] that routes every metric of an export to the dataset.
pub const OTLP_METRICS_PARAM_ALL: &str = "all";

/// Whether a dataset's `otlp_metrics` param value routes every metric to it.
#[must_use]
pub fn is_otlp_metrics_store_param(value: &str) -> bool {
    value.trim().eq_ignore_ascii_case(OTLP_METRICS_PARAM_ALL)
}

fn list_of(item: DataType) -> DataType {
    DataType::List(Arc::new(Field::new_list_field(item, true)))
}

/// The fixed schema of the OTLP metrics store table.
pub static METRICS_STORE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("metric_type", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("unit", DataType::Utf8, true),
        Field::new("temporality", DataType::Utf8, true),
        Field::new("is_monotonic", DataType::Boolean, true),
        Field::new("time_unix_nano", DataType::UInt64, false),
        Field::new("start_time_unix_nano", DataType::UInt64, true),
        Field::new("series_hash", DataType::UInt64, false),
        Field::new("attributes", DataType::Utf8, true),
        Field::new("resource_attributes", DataType::Utf8, true),
        Field::new("scope_name", DataType::Utf8, true),
        Field::new("scope_version", DataType::Utf8, true),
        Field::new("value_double", DataType::Float64, true),
        Field::new("value_int", DataType::Int64, true),
        Field::new("count", DataType::UInt64, true),
        Field::new("sum", DataType::Float64, true),
        Field::new("min", DataType::Float64, true),
        Field::new("max", DataType::Float64, true),
        Field::new("explicit_bounds", list_of(DataType::Float64), true),
        Field::new("bucket_counts", list_of(DataType::UInt64), true),
        Field::new("scale", DataType::Int32, true),
        Field::new("zero_count", DataType::UInt64, true),
        Field::new("positive_offset", DataType::Int32, true),
        Field::new("positive_bucket_counts", list_of(DataType::UInt64), true),
        Field::new("negative_offset", DataType::Int32, true),
        Field::new("negative_bucket_counts", list_of(DataType::UInt64), true),
        Field::new("quantiles", list_of(DataType::Float64), true),
        Field::new("quantile_values", list_of(DataType::Float64), true),
    ]))
});

/// JSON form of an OTLP attribute value. Integers, doubles, booleans and strings keep their
/// JSON type; bytes are base64; arrays and key-value lists nest.
fn any_value_to_json(value: Option<&AnyValue>) -> JsonValue {
    let Some(value) = value.and_then(|v| v.value.as_ref()) else {
        return JsonValue::Null;
    };
    match value {
        any_value::Value::StringValue(s) => JsonValue::String(s.clone()),
        any_value::Value::BoolValue(b) => JsonValue::Bool(*b),
        any_value::Value::IntValue(i) => JsonValue::from(*i),
        // NaN and infinities have no JSON number form; keep them as text.
        any_value::Value::DoubleValue(d) => serde_json::Number::from_f64(*d)
            .map_or_else(|| JsonValue::String(d.to_string()), JsonValue::Number),
        any_value::Value::BytesValue(b) => {
            JsonValue::String(base64::engine::general_purpose::STANDARD.encode(b))
        }
        any_value::Value::ArrayValue(a) => JsonValue::Array(
            a.values
                .iter()
                .map(|v| any_value_to_json(Some(v)))
                .collect(),
        ),
        any_value::Value::KvlistValue(kv) => attributes_to_json(&kv.values),
    }
}

/// Attributes as a JSON object with keys in sorted order, so equal attribute sets always
/// serialize, and hash, the same way. A repeated key keeps its last value.
fn attributes_to_json(attributes: &[KeyValue]) -> JsonValue {
    let sorted: BTreeMap<&str, JsonValue> = attributes
        .iter()
        .map(|kv| (kv.key.as_str(), any_value_to_json(kv.value.as_ref())))
        .collect();
    let mut map = serde_json::Map::with_capacity(sorted.len());
    for (key, value) in sorted {
        map.insert(key.to_string(), value);
    }
    JsonValue::Object(map)
}

fn attributes_to_json_string(attributes: &[KeyValue]) -> String {
    attributes_to_json(attributes).to_string()
}

/// Identity of a series: metric name, resource attributes, scope name and version, and point
/// attributes. Each part is length-prefixed so no two different series encode the same bytes.
/// The algorithm (`XxHash3_64`, seed 0) is part of the stored data and must not change.
#[must_use]
pub fn series_hash(
    metric_name: &str,
    resource_attributes_json: &str,
    scope_name: &str,
    scope_version: &str,
    attributes_json: &str,
) -> u64 {
    let parts = [
        metric_name,
        resource_attributes_json,
        scope_name,
        scope_version,
        attributes_json,
    ];
    let mut bytes = Vec::with_capacity(parts.iter().map(|p| p.len() + 8).sum());
    for part in parts {
        bytes.extend_from_slice(&(part.len() as u64).to_le_bytes());
        bytes.extend_from_slice(part.as_bytes());
    }
    twox_hash::XxHash3_64::oneshot(&bytes)
}

fn temporality_name(temporality: i32) -> Option<&'static str> {
    match temporality {
        1 => Some("delta"),
        2 => Some("cumulative"),
        _ => None,
    }
}

/// Builders for every column of [`METRICS_STORE_SCHEMA`].
#[derive(Default)]
struct StoreBuilders {
    metric_name: StringBuilder,
    metric_type: StringBuilder,
    description: StringBuilder,
    unit: StringBuilder,
    temporality: StringBuilder,
    is_monotonic: BooleanBuilder,
    time_unix_nano: UInt64Builder,
    start_time_unix_nano: UInt64Builder,
    series_hash: UInt64Builder,
    attributes: StringBuilder,
    resource_attributes: StringBuilder,
    scope_name: StringBuilder,
    scope_version: StringBuilder,
    value_double: Float64Builder,
    value_int: Int64Builder,
    count: UInt64Builder,
    sum: Float64Builder,
    min: Float64Builder,
    max: Float64Builder,
    explicit_bounds: ListBuilder<Float64Builder>,
    bucket_counts: ListBuilder<UInt64Builder>,
    scale: Int32Builder,
    zero_count: UInt64Builder,
    positive_offset: Int32Builder,
    positive_bucket_counts: ListBuilder<UInt64Builder>,
    negative_offset: Int32Builder,
    negative_bucket_counts: ListBuilder<UInt64Builder>,
    quantiles: ListBuilder<Float64Builder>,
    quantile_values: ListBuilder<Float64Builder>,
}

/// Columns shared by every row of one metric of one scope.
struct MetricContext<'a> {
    name: &'a str,
    metric_type: &'static str,
    description: &'a str,
    unit: &'a str,
    temporality: Option<&'static str>,
    is_monotonic: Option<bool>,
    resource_attributes: &'a str,
    scope_name: &'a str,
    scope_version: &'a str,
}

fn non_empty(s: &str) -> Option<&str> {
    (!s.is_empty()).then_some(s)
}

/// Values specific to one data point type. Columns not set are null.
#[derive(Default)]
struct PointValues<'a> {
    value_double: Option<f64>,
    value_int: Option<i64>,
    count: Option<u64>,
    sum: Option<f64>,
    min: Option<f64>,
    max: Option<f64>,
    explicit_bounds: Option<&'a [f64]>,
    bucket_counts: Option<&'a [u64]>,
    scale: Option<i32>,
    zero_count: Option<u64>,
    positive: Option<(i32, &'a [u64])>,
    negative: Option<(i32, &'a [u64])>,
    quantiles: Option<(Vec<f64>, Vec<f64>)>,
}

fn append_f64_list(builder: &mut ListBuilder<Float64Builder>, values: Option<&[f64]>) {
    match values {
        Some(values) => {
            builder.values().append_slice(values);
            builder.append(true);
        }
        None => builder.append(false),
    }
}

fn append_u64_list(builder: &mut ListBuilder<UInt64Builder>, values: Option<&[u64]>) {
    match values {
        Some(values) => {
            builder.values().append_slice(values);
            builder.append(true);
        }
        None => builder.append(false),
    }
}

impl StoreBuilders {
    fn append(
        &mut self,
        ctx: &MetricContext<'_>,
        time_unix_nano: u64,
        start_time_unix_nano: u64,
        attributes: &[KeyValue],
        values: PointValues<'_>,
    ) {
        let attributes_json = attributes_to_json_string(attributes);
        self.metric_name.append_value(ctx.name);
        self.metric_type.append_value(ctx.metric_type);
        self.description.append_option(non_empty(ctx.description));
        self.unit.append_option(non_empty(ctx.unit));
        self.temporality.append_option(ctx.temporality);
        self.is_monotonic.append_option(ctx.is_monotonic);
        self.time_unix_nano.append_value(time_unix_nano);
        // OTLP uses 0 for "not set".
        self.start_time_unix_nano
            .append_option((start_time_unix_nano != 0).then_some(start_time_unix_nano));
        self.series_hash.append_value(series_hash(
            ctx.name,
            ctx.resource_attributes,
            ctx.scope_name,
            ctx.scope_version,
            &attributes_json,
        ));
        self.attributes.append_value(&attributes_json);
        self.resource_attributes.append_value(ctx.resource_attributes);
        self.scope_name.append_option(non_empty(ctx.scope_name));
        self.scope_version.append_option(non_empty(ctx.scope_version));
        self.value_double.append_option(values.value_double);
        self.value_int.append_option(values.value_int);
        self.count.append_option(values.count);
        self.sum.append_option(values.sum);
        self.min.append_option(values.min);
        self.max.append_option(values.max);
        append_f64_list(&mut self.explicit_bounds, values.explicit_bounds);
        append_u64_list(&mut self.bucket_counts, values.bucket_counts);
        self.scale.append_option(values.scale);
        self.zero_count.append_option(values.zero_count);
        self.positive_offset
            .append_option(values.positive.map(|(offset, _)| offset));
        append_u64_list(
            &mut self.positive_bucket_counts,
            values.positive.map(|(_, counts)| counts),
        );
        self.negative_offset
            .append_option(values.negative.map(|(offset, _)| offset));
        append_u64_list(
            &mut self.negative_bucket_counts,
            values.negative.map(|(_, counts)| counts),
        );
        let (quantiles, quantile_values) = match values.quantiles {
            Some((q, v)) => (Some(q), Some(v)),
            None => (None, None),
        };
        append_f64_list(&mut self.quantiles, quantiles.as_deref());
        append_f64_list(&mut self.quantile_values, quantile_values.as_deref());
    }

    fn finish(mut self) -> Result<RecordBatch, ArrowError> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.metric_name.finish()),
            Arc::new(self.metric_type.finish()),
            Arc::new(self.description.finish()),
            Arc::new(self.unit.finish()),
            Arc::new(self.temporality.finish()),
            Arc::new(self.is_monotonic.finish()),
            Arc::new(self.time_unix_nano.finish()),
            Arc::new(self.start_time_unix_nano.finish()),
            Arc::new(self.series_hash.finish()),
            Arc::new(self.attributes.finish()),
            Arc::new(self.resource_attributes.finish()),
            Arc::new(self.scope_name.finish()),
            Arc::new(self.scope_version.finish()),
            Arc::new(self.value_double.finish()),
            Arc::new(self.value_int.finish()),
            Arc::new(self.count.finish()),
            Arc::new(self.sum.finish()),
            Arc::new(self.min.finish()),
            Arc::new(self.max.finish()),
            Arc::new(self.explicit_bounds.finish()),
            Arc::new(self.bucket_counts.finish()),
            Arc::new(self.scale.finish()),
            Arc::new(self.zero_count.finish()),
            Arc::new(self.positive_offset.finish()),
            Arc::new(self.positive_bucket_counts.finish()),
            Arc::new(self.negative_offset.finish()),
            Arc::new(self.negative_bucket_counts.finish()),
            Arc::new(self.quantiles.finish()),
            Arc::new(self.quantile_values.finish()),
        ];
        RecordBatch::try_new(Arc::clone(&METRICS_STORE_SCHEMA), columns)
    }
}

/// Builds one metrics store batch holding every data point of every metric in `request`, of
/// every OTLP metric type. Returns `None` when the export carries no data points.
///
/// # Errors
///
/// Returns an error if the arrays cannot form a batch, which indicates a bug in this module.
pub fn export_to_store_batch(
    request: &ExportMetricsServiceRequest,
) -> Result<Option<RecordBatch>, ArrowError> {
    let mut builders = StoreBuilders::default();
    let mut rows = 0usize;
    for resource_metric in &request.resource_metrics {
        let resource_attributes = attributes_to_json_string(
            resource_metric
                .resource
                .as_ref()
                .map_or(&[][..], |r| r.attributes.as_slice()),
        );
        for scope_metric in &resource_metric.scope_metrics {
            let (scope_name, scope_version) = scope_metric
                .scope
                .as_ref()
                .map_or(("", ""), |s| (s.name.as_str(), s.version.as_str()));
            for metric in &scope_metric.metrics {
                let Some(data) = metric.data.as_ref() else {
                    continue;
                };
                let mut ctx = MetricContext {
                    name: &metric.name,
                    metric_type: "",
                    description: &metric.description,
                    unit: &metric.unit,
                    temporality: None,
                    is_monotonic: None,
                    resource_attributes: &resource_attributes,
                    scope_name,
                    scope_version,
                };
                match data {
                    Data::Gauge(gauge) => {
                        ctx.metric_type = "gauge";
                        for p in &gauge.data_points {
                            builders.append(
                                &ctx,
                                p.time_unix_nano,
                                p.start_time_unix_nano,
                                &p.attributes,
                                number_values(p.value.as_ref()),
                            );
                            rows += 1;
                        }
                    }
                    Data::Sum(sum) => {
                        ctx.metric_type = "sum";
                        ctx.temporality = temporality_name(sum.aggregation_temporality);
                        ctx.is_monotonic = Some(sum.is_monotonic);
                        for p in &sum.data_points {
                            builders.append(
                                &ctx,
                                p.time_unix_nano,
                                p.start_time_unix_nano,
                                &p.attributes,
                                number_values(p.value.as_ref()),
                            );
                            rows += 1;
                        }
                    }
                    Data::Histogram(histogram) => {
                        ctx.metric_type = "histogram";
                        ctx.temporality = temporality_name(histogram.aggregation_temporality);
                        for p in &histogram.data_points {
                            builders.append(
                                &ctx,
                                p.time_unix_nano,
                                p.start_time_unix_nano,
                                &p.attributes,
                                PointValues {
                                    count: Some(p.count),
                                    sum: p.sum,
                                    min: p.min,
                                    max: p.max,
                                    explicit_bounds: Some(&p.explicit_bounds),
                                    bucket_counts: Some(&p.bucket_counts),
                                    ..PointValues::default()
                                },
                            );
                            rows += 1;
                        }
                    }
                    Data::ExponentialHistogram(histogram) => {
                        ctx.metric_type = "exponential_histogram";
                        ctx.temporality = temporality_name(histogram.aggregation_temporality);
                        for p in &histogram.data_points {
                            builders.append(
                                &ctx,
                                p.time_unix_nano,
                                p.start_time_unix_nano,
                                &p.attributes,
                                PointValues {
                                    count: Some(p.count),
                                    sum: p.sum,
                                    min: p.min,
                                    max: p.max,
                                    scale: Some(p.scale),
                                    zero_count: Some(p.zero_count),
                                    positive: p
                                        .positive
                                        .as_ref()
                                        .map(|b| (b.offset, b.bucket_counts.as_slice())),
                                    negative: p
                                        .negative
                                        .as_ref()
                                        .map(|b| (b.offset, b.bucket_counts.as_slice())),
                                    ..PointValues::default()
                                },
                            );
                            rows += 1;
                        }
                    }
                    Data::Summary(summary) => {
                        ctx.metric_type = "summary";
                        for p in &summary.data_points {
                            let (quantiles, values) = p
                                .quantile_values
                                .iter()
                                .map(|q| (q.quantile, q.value))
                                .unzip();
                            builders.append(
                                &ctx,
                                p.time_unix_nano,
                                p.start_time_unix_nano,
                                &p.attributes,
                                PointValues {
                                    count: Some(p.count),
                                    sum: Some(p.sum),
                                    quantiles: Some((quantiles, values)),
                                    ..PointValues::default()
                                },
                            );
                            rows += 1;
                        }
                    }
                }
            }
        }
    }
    if rows == 0 {
        return Ok(None);
    }
    builders.finish().map(Some)
}

fn number_values(value: Option<&Value>) -> PointValues<'static> {
    match value {
        Some(Value::AsDouble(d)) => PointValues {
            value_double: Some(*d),
            ..PointValues::default()
        },
        Some(Value::AsInt(i)) => PointValues {
            value_int: Some(*i),
            ..PointValues::default()
        },
        None => PointValues::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::{Float64Type, Int64Type, UInt64Type};
    use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
    use opentelemetry_proto::tonic::metrics::v1::{
        ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram,
        HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, Summary,
        SummaryDataPoint, exponential_histogram_data_point::Buckets,
        summary_data_point::ValueAtQuantile,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;

    fn kv(key: &str, value: any_value::Value) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue { value: Some(value) }),
        }
    }

    fn s(key: &str, value: &str) -> KeyValue {
        kv(key, any_value::Value::StringValue(value.to_string()))
    }

    fn number_point(time: u64, value: Value, attributes: Vec<KeyValue>) -> NumberDataPoint {
        NumberDataPoint {
            attributes,
            start_time_unix_nano: 0,
            time_unix_nano: time,
            exemplars: vec![],
            flags: 0,
            value: Some(value),
        }
    }

    fn metric(name: &str, unit: &str, data: Data) -> Metric {
        Metric {
            name: name.to_string(),
            description: String::new(),
            unit: unit.to_string(),
            metadata: vec![],
            data: Some(data),
        }
    }

    fn request(metrics: Vec<Metric>) -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![s("service.name", "spiced")],
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: "runtime".to_string(),
                        version: "1.0".to_string(),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                    }),
                    metrics,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    fn all_types_request() -> ExportMetricsServiceRequest {
        request(vec![
            metric(
                "http.server.active_requests",
                "{request}",
                Data::Gauge(Gauge {
                    data_points: vec![number_point(10, Value::AsInt(3), vec![s("route", "/a")])],
                }),
            ),
            metric(
                "http.server.requests",
                "{request}",
                Data::Sum(Sum {
                    data_points: vec![number_point(
                        10,
                        Value::AsDouble(5.0),
                        vec![s("route", "/a")],
                    )],
                    aggregation_temporality: 2,
                    is_monotonic: true,
                }),
            ),
            metric(
                "http.server.request.duration",
                "s",
                Data::Histogram(Histogram {
                    data_points: vec![HistogramDataPoint {
                        attributes: vec![],
                        start_time_unix_nano: 1,
                        time_unix_nano: 10,
                        count: 3,
                        sum: Some(1.5),
                        bucket_counts: vec![1, 2, 0],
                        explicit_bounds: vec![0.1, 1.0],
                        exemplars: vec![],
                        flags: 0,
                        min: Some(0.05),
                        max: Some(0.9),
                    }],
                    aggregation_temporality: 1,
                }),
            ),
            metric(
                "db.latency",
                "ms",
                Data::ExponentialHistogram(ExponentialHistogram {
                    data_points: vec![ExponentialHistogramDataPoint {
                        attributes: vec![],
                        start_time_unix_nano: 0,
                        time_unix_nano: 10,
                        count: 4,
                        sum: Some(10.0),
                        scale: 2,
                        zero_count: 1,
                        positive: Some(Buckets {
                            offset: -1,
                            bucket_counts: vec![1, 2],
                        }),
                        negative: None,
                        flags: 0,
                        exemplars: vec![],
                        min: None,
                        max: None,
                        zero_threshold: 0.0,
                    }],
                    aggregation_temporality: 2,
                }),
            ),
            metric(
                "rpc.latency",
                "ms",
                Data::Summary(Summary {
                    data_points: vec![SummaryDataPoint {
                        attributes: vec![],
                        start_time_unix_nano: 0,
                        time_unix_nano: 10,
                        count: 2,
                        sum: 3.0,
                        quantile_values: vec![
                            ValueAtQuantile {
                                quantile: 0.5,
                                value: 1.0,
                            },
                            ValueAtQuantile {
                                quantile: 0.99,
                                value: 2.0,
                            },
                        ],
                        flags: 0,
                    }],
                }),
            ),
        ])
    }

    #[test]
    fn every_metric_type_lands_in_one_fixed_schema_batch() {
        let batch = export_to_store_batch(&all_types_request())
            .expect("batch")
            .expect("rows");
        assert_eq!(batch.schema(), *METRICS_STORE_SCHEMA);
        assert_eq!(batch.num_rows(), 5);
        let types: Vec<&str> = batch
            .column_by_name("metric_type")
            .expect("col")
            .as_string::<i32>()
            .iter()
            .map(Option::unwrap)
            .collect();
        assert_eq!(
            types,
            [
                "gauge",
                "sum",
                "histogram",
                "exponential_histogram",
                "summary"
            ]
        );
    }

    #[test]
    fn metric_semantics_are_kept() {
        let batch = export_to_store_batch(&all_types_request())
            .expect("batch")
            .expect("rows");
        let temporality = batch
            .column_by_name("temporality")
            .expect("col")
            .as_string::<i32>();
        assert!(temporality.is_null(0));
        assert_eq!(temporality.value(1), "cumulative");
        assert_eq!(temporality.value(2), "delta");
        let monotonic = batch
            .column_by_name("is_monotonic")
            .expect("col")
            .as_boolean();
        assert!(monotonic.is_null(0));
        assert!(monotonic.value(1));
        let unit = batch.column_by_name("unit").expect("col").as_string::<i32>();
        assert_eq!(unit.value(2), "s");
        let value_int = batch
            .column_by_name("value_int")
            .expect("col")
            .as_primitive::<Int64Type>();
        assert_eq!(value_int.value(0), 3);
        assert!(value_int.is_null(1));
        let value_double = batch
            .column_by_name("value_double")
            .expect("col")
            .as_primitive::<Float64Type>();
        assert!((value_double.value(1) - 5.0).abs() < f64::EPSILON);
        let start = batch
            .column_by_name("start_time_unix_nano")
            .expect("col")
            .as_primitive::<UInt64Type>();
        assert!(start.is_null(0), "0 means unset in OTLP");
        assert_eq!(start.value(2), 1);
        let positive = batch
            .column_by_name("positive_bucket_counts")
            .expect("col")
            .as_list::<i32>();
        assert_eq!(
            positive
                .value(3)
                .as_primitive::<UInt64Type>()
                .values()
                .to_vec(),
            vec![1, 2]
        );
        assert!(
            batch
                .column_by_name("negative_bucket_counts")
                .expect("col")
                .is_null(3)
        );
        let quantile_values = batch
            .column_by_name("quantile_values")
            .expect("col")
            .as_list::<i32>();
        assert_eq!(
            quantile_values
                .value(4)
                .as_primitive::<Float64Type>()
                .values()
                .to_vec(),
            vec![1.0, 2.0]
        );
        let resource = batch
            .column_by_name("resource_attributes")
            .expect("col")
            .as_string::<i32>();
        assert_eq!(resource.value(0), r#"{"service.name":"spiced"}"#);
    }

    #[test]
    fn new_attribute_keys_do_not_change_the_schema() {
        let a = request(vec![metric(
            "m",
            "",
            Data::Gauge(Gauge {
                data_points: vec![number_point(1, Value::AsInt(1), vec![s("a", "1")])],
            }),
        )]);
        let b = request(vec![metric(
            "m",
            "",
            Data::Gauge(Gauge {
                data_points: vec![number_point(
                    1,
                    Value::AsInt(1),
                    vec![s("a", "1"), s("brand_new_key", "x")],
                )],
            }),
        )]);
        let a = export_to_store_batch(&a).expect("a").expect("rows");
        let b = export_to_store_batch(&b).expect("b").expect("rows");
        assert_eq!(a.schema(), b.schema());
    }

    #[test]
    fn series_hash_is_stable_across_attribute_order_and_retries() {
        let one = request(vec![metric(
            "m",
            "",
            Data::Gauge(Gauge {
                data_points: vec![number_point(
                    1,
                    Value::AsInt(1),
                    vec![s("a", "1"), s("b", "2")],
                )],
            }),
        )]);
        let reordered = request(vec![metric(
            "m",
            "",
            Data::Gauge(Gauge {
                data_points: vec![number_point(
                    1,
                    Value::AsInt(1),
                    vec![s("b", "2"), s("a", "1")],
                )],
            }),
        )]);
        let hash = |r: &ExportMetricsServiceRequest| {
            export_to_store_batch(r)
                .expect("batch")
                .expect("rows")
                .column_by_name("series_hash")
                .expect("col")
                .as_primitive::<UInt64Type>()
                .value(0)
        };
        assert_eq!(hash(&one), hash(&one));
        assert_eq!(hash(&one), hash(&reordered));
    }

    #[test]
    fn series_hash_distinguishes_every_identity_part() {
        let base = series_hash("m", "{}", "s", "1", r#"{"a":"1"}"#);
        assert_ne!(base, series_hash("n", "{}", "s", "1", r#"{"a":"1"}"#));
        assert_ne!(base, series_hash("m", r#"{"x":1}"#, "s", "1", r#"{"a":"1"}"#));
        assert_ne!(base, series_hash("m", "{}", "t", "1", r#"{"a":"1"}"#));
        assert_ne!(base, series_hash("m", "{}", "s", "2", r#"{"a":"1"}"#));
        assert_ne!(base, series_hash("m", "{}", "s", "1", r#"{"a":"2"}"#));
        // Length prefixes keep part boundaries from shifting into each other.
        assert_ne!(
            series_hash("ab", "", "", "", ""),
            series_hash("a", "b", "", "", "")
        );
    }

    #[test]
    fn typed_attribute_values_keep_their_json_type() {
        let json = attributes_to_json_string(&[
            kv("i", any_value::Value::IntValue(7)),
            kv("d", any_value::Value::DoubleValue(1.5)),
            kv("b", any_value::Value::BoolValue(true)),
            kv("nan", any_value::Value::DoubleValue(f64::NAN)),
            kv("bytes", any_value::Value::BytesValue(vec![1, 2])),
        ]);
        assert_eq!(
            json,
            r#"{"b":true,"bytes":"AQI=","d":1.5,"i":7,"nan":"NaN"}"#
        );
    }

    #[test]
    fn export_with_no_data_points_builds_no_batch() {
        assert!(
            export_to_store_batch(&request(vec![]))
                .expect("ok")
                .is_none()
        );
    }

    #[test]
    fn store_param_accepts_only_all() {
        assert!(is_otlp_metrics_store_param("all"));
        assert!(is_otlp_metrics_store_param(" ALL "));
        assert!(!is_otlp_metrics_store_param("none"));
        assert!(!is_otlp_metrics_store_param(""));
    }
}
