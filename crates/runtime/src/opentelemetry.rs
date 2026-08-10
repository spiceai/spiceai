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

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Weak;

use arrow::array::ArrayBuilder;
use arrow::array::ArrayRef;
use arrow::array::BinaryBuilder;
use arrow::array::BooleanBuilder;
use arrow::array::Float64Builder;
use arrow::array::Int64Builder;
use arrow::array::ListBuilder;
use arrow::array::StringBuilder;
use arrow::array::UInt64Builder;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use datafusion::sql::TableReference;
use indexmap::IndexMap;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsPartialSuccess;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceResponse;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsService;
pub use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsServiceServer;
use opentelemetry_proto::tonic::common::v1::KeyValue;
use opentelemetry_proto::tonic::common::v1::any_value;
use opentelemetry_proto::tonic::metrics::v1::DataPointFlags;
use opentelemetry_proto::tonic::metrics::v1::HistogramDataPoint;
use opentelemetry_proto::tonic::metrics::v1::NumberDataPoint;
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value;
use snafu::prelude::*;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::async_trait;
use tonic::codec::CompressionEncoding;

use crate::Runtime;
use runtime_query_engine::query_engine::{QueryEngine, UpdateType};
use util::tracers::OnceTracer;
use util::warn_once;

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build record batch from OpenTelemetry metrics: {source}"))]
    FailedToBuildRecordBatch { source: arrow::error::ArrowError },

    #[snafu(display("Unsupported metric data type"))]
    UnsupportedMetricDataType {},

    #[snafu(display("Unsupported metric attribute type"))]
    UnsupportedMetricAttributeType {},

    #[snafu(display("Metric with no data points"))]
    MetricWithNoDataPoints {},

    #[snafu(display(
        "Existing table for metric {metric} has unsupported `value` column data type {data_type} for data point type {data_point_type}"
    ))]
    UnsupportedExistingMetricValueColumnType {
        metric: String,
        data_type: DataType,
        data_point_type: String,
    },

    #[snafu(display(
        "First data point for metric {metric} has no value and therefore is not valid for establishing schema"
    ))]
    FirstMetricDataPointHasNoValue { metric: String },
}

const VALUE_COLUMN_NAME: &str = "value";
const TIME_UNIX_NANO_COLUMN_NAME: &str = "time_unix_nano";
const START_TIME_UNIX_NANO_COLUMN_NAME: &str = "start_time_unix_nano";

// Histogram-specific value columns.
const COUNT_COLUMN_NAME: &str = "count";
const SUM_COLUMN_NAME: &str = "sum";
const MIN_COLUMN_NAME: &str = "min";
const MAX_COLUMN_NAME: &str = "max";
const BUCKET_COUNTS_COLUMN_NAME: &str = "bucket_counts";
const EXPLICIT_BOUNDS_COLUMN_NAME: &str = "explicit_bounds";

/// Column names that carry values for a number (Gauge/Sum) data point, as opposed to
/// data-point attributes. These must never be treated as attributes when (re)building such
/// a metric's schema. The set is per data-point shape: a histogram's value-column names are
/// ordinary dimensions on a Gauge or Sum, and excluding them there drops the dimension from
/// the seeded attribute schema (see [`initialize_attribute_schema`]).
const NUMBER_VALUE_COLUMN_NAMES: &[&str] = &[
    VALUE_COLUMN_NAME,
    TIME_UNIX_NANO_COLUMN_NAME,
    START_TIME_UNIX_NANO_COLUMN_NAME,
];

/// Column names that carry values for a histogram data point. See [`NUMBER_VALUE_COLUMN_NAMES`].
const HISTOGRAM_VALUE_COLUMN_NAMES: &[&str] = &[
    COUNT_COLUMN_NAME,
    SUM_COLUMN_NAME,
    MIN_COLUMN_NAME,
    MAX_COLUMN_NAME,
    BUCKET_COUNTS_COLUMN_NAME,
    EXPLICIT_BOUNDS_COLUMN_NAME,
    TIME_UNIX_NANO_COLUMN_NAME,
    START_TIME_UNIX_NANO_COLUMN_NAME,
];

pub struct Service {
    datafusion: Arc<dyn QueryEngine>,
    /// Weak handle to the runtime, used to evolve an accelerated metric table's schema
    /// in place when a new metric dimension (attribute column) arrives, per the dataset's
    /// `on_schema_change` policy. `None` (or a dead weak ref) disables write-time evolution
    /// and metrics with new dimensions are rejected as before.
    runtime: Option<Weak<Runtime>>,
    once_tracer: OnceTracer,
}

/// Names of columns present in `incoming` but absent from `existing` (by name). New metric
/// dimensions are strictly additive nullable columns, so a non-empty result is the signal to
/// attempt write-time schema evolution before writing.
fn detect_added_columns(existing: &Schema, incoming: &Schema) -> Vec<String> {
    incoming
        .fields()
        .iter()
        .filter(|field| existing.field_with_name(field.name()).is_err())
        .map(|field| field.name().clone())
        .collect()
}

#[async_trait]
impl MetricsService for Service {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> std::result::Result<Response<ExportMetricsServiceResponse>, Status> {
        let mut rejected_data_points = 0;
        let mut total_data_points = 0;
        let resource_metrics = request.into_inner().resource_metrics;
        tracing::debug!(
            "OpenTelemetry export: received {} resource metric group(s)",
            resource_metrics.len()
        );
        for resource_metric in resource_metrics {
            for scope_metric in resource_metric.scope_metrics {
                for metric in scope_metric.metrics {
                    let Some(data) = metric.data else {
                        tracing::debug!(
                            "OpenTelemetry export: metric {} has no data, skipping",
                            metric.name
                        );
                        continue;
                    };
                    let existing_schema = (self
                        .datafusion
                        .get_arrow_schema(TableReference::bare(metric.name.clone()))
                        .await)
                        .ok();
                    let (record_batch_result, data_points_count) = metric_data_to_record_batch(
                        metric.name.as_str(),
                        &data,
                        existing_schema.as_ref(),
                    );
                    total_data_points += data_points_count;
                    tracing::debug!(
                        "OpenTelemetry export: processing metric {} (type={}, {} data point(s))",
                        metric.name,
                        metric_data_type_name(&data),
                        data_points_count
                    );

                    match record_batch_result {
                        Ok(mut record_batch) => {
                            if !self
                                .datafusion
                                .is_writable(&TableReference::bare(metric.name.clone()))
                            {
                                warn_once!(
                                    self.once_tracer,
                                    "No writable dataset defined for metric {}, skipping",
                                    metric.name
                                );
                                tracing::debug!(
                                    "OpenTelemetry export: metric {} is not writable, rejecting {} data point(s)",
                                    metric.name,
                                    data_points_count
                                );
                                rejected_data_points += data_points_count;
                                continue;
                            }

                            // Pre-flight schema evolution: when this batch introduces new
                            // dimension columns beyond the stored schema, evolve the
                            // accelerator (per `on_schema_change`) BEFORE writing so the
                            // rebound provider accepts the wider batch. On `block`/`fail`,
                            // an unsupported engine, or an incompatible change this is a
                            // no-op and the write below rejects the batch as it does today.
                            if let Some(existing) = existing_schema.as_ref() {
                                let added =
                                    detect_added_columns(existing, record_batch.schema().as_ref());
                                if !added.is_empty()
                                    && let Some(runtime) =
                                        self.runtime.as_ref().and_then(Weak::upgrade)
                                {
                                    let table_ref = TableReference::bare(metric.name.clone());
                                    match runtime
                                        .evolve_accelerated_schema_for_write(
                                            &table_ref,
                                            &record_batch.schema(),
                                        )
                                        .await
                                    {
                                        Ok(Some(evolved)) => {
                                            tracing::debug!(
                                                "OpenTelemetry export: evolved schema for metric {} (added dimension(s): {})",
                                                metric.name,
                                                added.join(", ")
                                            );
                                            // Rebuild against the evolved schema so the batch
                                            // matches the rebound provider exactly by column
                                            // set AND order (verify_schema is exact-positional).
                                            match metric_data_to_record_batch(
                                                metric.name.as_str(),
                                                &data,
                                                Some(&evolved),
                                            )
                                            .0
                                            {
                                                Ok(rebuilt) => record_batch = rebuilt,
                                                Err(e) => {
                                                    tracing::warn!(
                                                        "Failed to rebuild OpenTelemetry batch for metric {} after schema evolution: {e}",
                                                        metric.name
                                                    );
                                                    rejected_data_points += data_points_count;
                                                    continue;
                                                }
                                            }
                                        }
                                        Ok(None) => {
                                            // Not evolved (block/fail/incompatible/unsupported);
                                            // fall through to the write, which rejects as today.
                                        }
                                        Err(e) => {
                                            tracing::warn!(
                                                "Failed to evolve schema for OpenTelemetry metric {}: {e}",
                                                metric.name
                                            );
                                            rejected_data_points += data_points_count;
                                            continue;
                                        }
                                    }
                                }
                            }

                            let schema = record_batch.schema();
                            let mut write_failed = false;
                            if let Err(e) = self
                                .datafusion
                                .write_data(
                                    &TableReference::bare(metric.name.as_str()),
                                    schema,
                                    vec![record_batch],
                                    UpdateType::Append,
                                )
                                .await
                            {
                                write_failed = true;
                                // Surface at warn: a failed write silently rejects data
                                // points, and the underlying accelerator/connector error is
                                // the only signal for why (e.g. a schema or type mismatch).
                                tracing::warn!(
                                    "Failed to write OpenTelemetry data for metric {}: {e}",
                                    metric.name
                                );
                            } else {
                                tracing::debug!(
                                    "OpenTelemetry export: wrote {} data point(s) for metric {}",
                                    data_points_count,
                                    metric.name
                                );
                            }

                            if write_failed {
                                rejected_data_points += data_points_count;
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                "Failed to build arrow data from OpenTelemetry metrics for metric {}: {e}",
                                metric.name
                            );
                            rejected_data_points += data_points_count;
                        }
                    }
                }
            }
        }

        if rejected_data_points >= total_data_points {
            return Err(Status::invalid_argument("All data points were rejected"));
        }

        let partial_success = if rejected_data_points == 0 {
            None
        } else {
            Some(ExportMetricsPartialSuccess {
                error_message: "Some data points were rejected".to_string(),
                rejected_data_points: rejected_data_points.try_into().unwrap_or(i64::MAX),
            })
        };
        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success,
        }))
    }
}

pub fn metric_data_to_record_batch(
    metric: &str,
    data: &Data,
    existing_schema: Option<&Schema>,
) -> (Result<RecordBatch>, u64) {
    match data {
        Data::Gauge(gauge) => (
            number_data_points_to_record_batch(metric, &gauge.data_points, existing_schema),
            gauge.data_points.len() as u64,
        ),
        Data::Sum(sum) => (
            number_data_points_to_record_batch(metric, &sum.data_points, existing_schema),
            sum.data_points.len() as u64,
        ),
        Data::Histogram(histogram) => (
            histogram_data_points_to_record_batch(metric, &histogram.data_points, existing_schema),
            histogram.data_points.len() as u64,
        ),
        // TODO: Support other metric data types (ExponentialHistogram, Summary)
        _ => (UnsupportedMetricDataTypeSnafu.fail(), 0),
    }
}

/// Human-readable name of a metric's data type, for diagnostics/logging.
fn metric_data_type_name(data: &Data) -> &'static str {
    match data {
        Data::Gauge(_) => "Gauge",
        Data::Sum(_) => "Sum",
        Data::Histogram(_) => "Histogram",
        Data::ExponentialHistogram(_) => "ExponentialHistogram",
        Data::Summary(_) => "Summary",
    }
}

macro_rules! append_value {
    ($values_builder:expr, $data_points_type:expr, $value:expr, $builder_type:ty, $data_type:expr, $metric:expr) => {
        match &mut $values_builder {
            Some(builder) => {
                if let Some(typed_builder) = builder.as_any_mut().downcast_mut::<$builder_type>() {
                    typed_builder.append_value(*$value);
                } else {
                    tracing::warn!("Metric {} has data points with different types, skipping data point that introduces new type", $metric);
                    continue;
                }
            }
            None => {
                let mut new_builder = <$builder_type>::new();
                new_builder.append_value(*$value);
                $values_builder = Some(Box::new(new_builder));
                $data_points_type = $data_type;
            }
        }
    };
}

fn number_data_points_to_record_batch(
    metric: &str,
    data_points: &Vec<NumberDataPoint>,
    existing_schema: Option<&Schema>,
) -> Result<RecordBatch> {
    let mut values_builder: Option<Box<dyn ArrayBuilder>> = None;
    let mut values_type = DataType::Null;
    let mut time_unix_nano_builder = UInt64Builder::new();
    let mut start_time_unix_nano_builder = UInt64Builder::new();
    let mut attributes = Vec::new();

    if let Some(s) = existing_schema
        && let Ok(value_field) = s.field_with_name(VALUE_COLUMN_NAME)
    {
        match value_field.data_type() {
            DataType::Float64 => {
                values_builder = Some(Box::new(Float64Builder::new()));
                values_type = DataType::Float64;
            }
            DataType::Int64 => {
                values_builder = Some(Box::new(Int64Builder::new()));
                values_type = DataType::Int64;
            }
            _ => {
                return UnsupportedExistingMetricValueColumnTypeSnafu {
                    metric,
                    data_type: value_field.data_type().clone(),
                    data_point_type: "NumberDataPoint",
                }
                .fail();
            }
        }
    }

    for data_point in data_points {
        if let Some(value) = &data_point.value {
            match value {
                Value::AsDouble(double_value) => {
                    append_value!(
                        values_builder,
                        values_type,
                        double_value,
                        Float64Builder,
                        DataType::Float64,
                        metric
                    );
                }
                Value::AsInt(int_value) => {
                    append_value!(
                        values_builder,
                        values_type,
                        int_value,
                        Int64Builder,
                        DataType::Int64,
                        metric
                    );
                }
            }
        } else if let Some(builder) = &mut values_builder {
            if (data_point.flags & DataPointFlags::NoRecordedValueMask as u32)
                != DataPointFlags::NoRecordedValueMask as u32
            {
                tracing::warn!(
                    "Metric {} has data point with no recorded value without flag set to indicate no recorded value, skipping",
                    metric
                );
                continue;
            }

            if let Some(float_64_builder) = builder.as_any_mut().downcast_mut::<Float64Builder>() {
                float_64_builder.append_null();
            } else if let Some(int_64_builder) = builder.as_any_mut().downcast_mut::<Int64Builder>()
            {
                int_64_builder.append_null();
            }
        } else {
            return FirstMetricDataPointHasNoValueSnafu { metric }.fail();
        }
        attributes.push(data_point.attributes.as_slice());
        time_unix_nano_builder.append_value(data_point.time_unix_nano);
        start_time_unix_nano_builder.append_value(data_point.start_time_unix_nano);
    }

    let mut columns: Vec<ArrayRef>;
    let mut fields: Vec<Arc<Field>>;
    if let Some(builder) = &mut values_builder {
        fields = vec![
            Arc::new(Field::new(VALUE_COLUMN_NAME, values_type, true)),
            Arc::new(Field::new(
                TIME_UNIX_NANO_COLUMN_NAME,
                DataType::UInt64,
                true,
            )),
            Arc::new(Field::new(
                START_TIME_UNIX_NANO_COLUMN_NAME,
                DataType::UInt64,
                true,
            )),
        ];
        columns = vec![
            Arc::new(builder.finish()),
            Arc::new(time_unix_nano_builder.finish()),
            Arc::new(start_time_unix_nano_builder.finish()),
        ];
    } else {
        return MetricWithNoDataPointsSnafu.fail();
    }

    let (attribute_fields_map, attribute_columns_map) = attributes_to_fields_and_columns(
        metric,
        attributes.as_slice(),
        existing_schema,
        NUMBER_VALUE_COLUMN_NAMES,
    );
    fields.extend(
        attribute_fields_map
            .into_iter()
            .map(|(_, v)| v)
            .collect::<Vec<Arc<Field>>>(),
    );
    columns.extend(
        attribute_columns_map
            .into_iter()
            .map(|(_, mut v)| v.finish()),
    );

    match RecordBatch::try_new(Arc::new(Schema::new(fields)), columns) {
        Ok(record_batch) => Ok(record_batch),
        Err(e) => Err(e).context(FailedToBuildRecordBatchSnafu),
    }
}

/// Builds a `RecordBatch` from OpenTelemetry histogram data points.
///
/// Unlike number data points (Gauge/Sum), a histogram data point has a fixed set of
/// value columns, so the schema does not depend on the observed value types:
/// - `count` (`UInt64`): number of values in the population.
/// - `sum` (`Float64`, nullable): sum of the values, absent when not recorded.
/// - `min` / `max` (`Float64`, nullable): extrema over the interval, absent when not recorded.
/// - `bucket_counts` (`List<UInt64>`): per-bucket counts.
/// - `explicit_bounds` (`List<Float64>`): the explicit bucket boundaries.
///
/// `explicit_bounds` has exactly one fewer element than `bucket_counts` (per the OTLP spec),
/// except when both are empty. Attributes and the time columns are handled identically to
/// number data points.
fn histogram_data_points_to_record_batch(
    metric: &str,
    data_points: &[HistogramDataPoint],
    existing_schema: Option<&Schema>,
) -> Result<RecordBatch> {
    if data_points.is_empty() {
        return MetricWithNoDataPointsSnafu.fail();
    }

    let mut count_builder = UInt64Builder::new();
    let mut sum_builder = Float64Builder::new();
    let mut min_builder = Float64Builder::new();
    let mut max_builder = Float64Builder::new();
    let mut bucket_counts_builder = ListBuilder::new(UInt64Builder::new());
    let mut explicit_bounds_builder = ListBuilder::new(Float64Builder::new());
    let mut time_unix_nano_builder = UInt64Builder::new();
    let mut start_time_unix_nano_builder = UInt64Builder::new();
    let mut attributes = Vec::with_capacity(data_points.len());

    for data_point in data_points {
        count_builder.append_value(data_point.count);
        sum_builder.append_option(data_point.sum);
        min_builder.append_option(data_point.min);
        max_builder.append_option(data_point.max);

        bucket_counts_builder
            .values()
            .append_slice(&data_point.bucket_counts);
        bucket_counts_builder.append(true);

        explicit_bounds_builder
            .values()
            .append_slice(&data_point.explicit_bounds);
        explicit_bounds_builder.append(true);

        attributes.push(data_point.attributes.as_slice());
        time_unix_nano_builder.append_value(data_point.time_unix_nano);
        start_time_unix_nano_builder.append_value(data_point.start_time_unix_nano);
    }

    // Finish the value arrays first, then derive their fields from the produced arrays so the
    // list-element field names/nullability always match what the builders emit (avoiding a
    // schema/data mismatch in `RecordBatch::try_new`).
    let value_columns: Vec<(&str, ArrayRef)> = vec![
        (COUNT_COLUMN_NAME, Arc::new(count_builder.finish())),
        (SUM_COLUMN_NAME, Arc::new(sum_builder.finish())),
        (MIN_COLUMN_NAME, Arc::new(min_builder.finish())),
        (MAX_COLUMN_NAME, Arc::new(max_builder.finish())),
        (
            BUCKET_COUNTS_COLUMN_NAME,
            Arc::new(bucket_counts_builder.finish()),
        ),
        (
            EXPLICIT_BOUNDS_COLUMN_NAME,
            Arc::new(explicit_bounds_builder.finish()),
        ),
        (
            TIME_UNIX_NANO_COLUMN_NAME,
            Arc::new(time_unix_nano_builder.finish()),
        ),
        (
            START_TIME_UNIX_NANO_COLUMN_NAME,
            Arc::new(start_time_unix_nano_builder.finish()),
        ),
    ];

    let mut fields: Vec<Arc<Field>> = value_columns
        .iter()
        .map(|(name, array)| Arc::new(Field::new(*name, array.data_type().clone(), true)))
        .collect();
    let mut columns: Vec<ArrayRef> = value_columns.into_iter().map(|(_, array)| array).collect();

    let (attribute_fields_map, attribute_columns_map) = attributes_to_fields_and_columns(
        metric,
        attributes.as_slice(),
        existing_schema,
        HISTOGRAM_VALUE_COLUMN_NAMES,
    );
    fields.extend(attribute_fields_map.into_iter().map(|(_, v)| v));
    columns.extend(
        attribute_columns_map
            .into_iter()
            .map(|(_, mut v)| v.finish()),
    );

    match RecordBatch::try_new(Arc::new(Schema::new(fields)), columns) {
        Ok(record_batch) => Ok(record_batch),
        Err(e) => Err(e).context(FailedToBuildRecordBatchSnafu),
    }
}

macro_rules! append_attribute {
    ($columns:expr, $fields:expr, $key:expr, $value:expr, $builder_type:ty, $data_type:expr, $metric:expr, $row_index:expr) => {{
        let key_str = $key.as_str();
        match $columns.get_mut(key_str) {
            None => {
                $fields.insert(
                    $key.clone(),
                    Arc::new(Field::new(key_str, $data_type, true)),
                );
                let mut builder = <$builder_type>::new();
                // This attribute was absent from every preceding data point, so backfill a null
                // for each so the value lands on the correct row ($row_index) and the column
                // length matches the other columns.
                for _ in 0..$row_index {
                    builder.append_null();
                }
                builder.append_value($value);
                $columns.insert($key.clone(), Box::new(builder));
            }
            Some(column) => {
                if let Some(builder) = column.as_any_mut().downcast_mut::<$builder_type>() {
                    builder.append_value($value);
                } else {
                    tracing::warn!(
                        "Metric {} has attribute {} with different types, appending null for attribute that introduces new type",
                        $metric,
                        key_str
                    );
                    append_null(&mut $fields, &mut $columns, key_str);
                }
            }
        };
    }};
}

#[expect(clippy::type_complexity)]
fn attributes_to_fields_and_columns(
    metric: &str,
    attributes: &[&[KeyValue]],
    existing_schema: Option<&Schema>,
    value_columns: &[&str],
) -> (
    IndexMap<String, Arc<Field>>,
    IndexMap<String, Box<dyn ArrayBuilder>>,
) {
    let mut fields: IndexMap<String, Arc<Field>> = IndexMap::new();
    let mut columns: IndexMap<String, Box<dyn ArrayBuilder>> = IndexMap::new();
    let mut warned_collisions: HashSet<&str> = HashSet::new();

    initialize_attribute_schema(&mut fields, &mut columns, existing_schema, value_columns);

    for (i, inner_attributes) in attributes.iter().enumerate() {
        for attribute in *inner_attributes {
            let key_str = attribute.key.as_str();
            // An attribute whose key is one of this metric's value columns cannot be
            // represented alongside it: emitting it as an attribute would put two columns of
            // that name, with different types, in the same batch.
            if value_columns.contains(&key_str) {
                if warned_collisions.insert(key_str) {
                    tracing::warn!(
                        "Metric {metric} has attribute {key_str} with the same name as one of its value columns, dropping the attribute"
                    );
                }
                continue;
            }
            if let Some(any_value) = &attribute.value {
                if let Some(value) = &any_value.value {
                    match value {
                        any_value::Value::StringValue(string_value) => {
                            append_attribute!(
                                columns,
                                fields,
                                attribute.key,
                                string_value,
                                StringBuilder,
                                DataType::Utf8,
                                metric,
                                i
                            );
                        }
                        any_value::Value::BoolValue(bool_value) => {
                            append_attribute!(
                                columns,
                                fields,
                                attribute.key,
                                *bool_value,
                                BooleanBuilder,
                                DataType::Boolean,
                                metric,
                                i
                            );
                        }
                        any_value::Value::IntValue(int_value) => {
                            append_attribute!(
                                columns,
                                fields,
                                attribute.key,
                                *int_value,
                                Int64Builder,
                                DataType::Int64,
                                metric,
                                i
                            );
                        }
                        any_value::Value::DoubleValue(double_value) => {
                            append_attribute!(
                                columns,
                                fields,
                                attribute.key,
                                *double_value,
                                Float64Builder,
                                DataType::Float64,
                                metric,
                                i
                            );
                        }
                        any_value::Value::BytesValue(bytes_value) => {
                            append_attribute!(
                                columns,
                                fields,
                                attribute.key,
                                bytes_value,
                                BinaryBuilder,
                                DataType::Binary,
                                metric,
                                i
                            );
                        }
                        // TODO: Support List and Map attribute types
                        _ => {
                            tracing::warn!(
                                "Metric {metric} has attribute {key_str} with unsupported type, appending null for attribute if possible"
                            );
                            append_null(&mut fields, &mut columns, key_str);
                        }
                    }
                } else {
                    tracing::warn!(
                        "Metric {metric} has attribute {key_str} with no value, appending null for attribute if possible"
                    );
                    append_null(&mut fields, &mut columns, key_str);
                }
            } else {
                tracing::warn!(
                    "Metric {metric} has attribute {key_str} with no value, appending null for attribute if possible"
                );
                append_null(&mut fields, &mut columns, key_str);
            }
        }

        // If an attribute previously existed but is missing from this metric, append a null value.
        let mut needs_null = Vec::new();
        for (column_name, column_values) in columns.as_slice() {
            if column_values.len() < i + 1 {
                needs_null.push(column_name.clone());
            }
        }
        for column_name in needs_null {
            append_null(&mut fields, &mut columns, column_name.as_str());
        }
    }

    (fields, columns)
}

fn initialize_attribute_schema(
    fields: &mut IndexMap<String, Arc<Field>>,
    columns: &mut IndexMap<String, Box<dyn ArrayBuilder>>,
    existing_schema: Option<&Schema>,
    value_columns: &[&str],
) {
    if let Some(s) = existing_schema {
        for field in s.fields() {
            // Skip only this metric's own value/time columns: they are not attributes and are
            // already handled by the value-column builders. Every other stored column is a
            // dimension and must be seeded here so it keeps its position and gets a null
            // backfill on a data point that omits it — including one named like another data
            // point shape's value column, such as `count` on a Gauge.
            if value_columns.contains(&field.name().as_str()) {
                continue;
            }

            fields.insert(field.name().clone(), Arc::clone(field));
            match field.data_type() {
                DataType::Utf8 => {
                    columns.insert(field.name().clone(), Box::new(StringBuilder::new()));
                }
                DataType::Boolean => {
                    columns.insert(field.name().clone(), Box::new(BooleanBuilder::new()));
                }
                DataType::Int64 => {
                    columns.insert(field.name().clone(), Box::new(Int64Builder::new()));
                }
                DataType::Float64 => {
                    columns.insert(field.name().clone(), Box::new(Float64Builder::new()));
                }
                DataType::Binary => {
                    columns.insert(field.name().clone(), Box::new(BinaryBuilder::new()));
                }
                _ => {}
            }
        }
    }
}

macro_rules! append_null {
    ($columns:expr, $key:expr, $builder_type:ty) => {
        if let Some(column) = $columns.get_mut($key) {
            if let Some(builder) = column.as_any_mut().downcast_mut::<$builder_type>() {
                builder.append_null();
            }
        }
    };
}

fn append_null(
    fields: &mut IndexMap<String, Arc<Field>>,
    columns: &mut IndexMap<String, Box<dyn ArrayBuilder>>,
    key: &str,
) {
    if let Some(field) = fields.get(key) {
        match field.data_type() {
            DataType::Utf8 => append_null!(columns, key, StringBuilder),
            DataType::Boolean => append_null!(columns, key, BooleanBuilder),
            DataType::Int64 => append_null!(columns, key, Int64Builder),
            DataType::Float64 => append_null!(columns, key, Float64Builder),
            DataType::Binary => append_null!(columns, key, BinaryBuilder),
            _ => {}
        }
    }
}

/// Creates the OpenTelemetry `MetricsService` server that can be added to a gRPC server.
///
/// This is used to add OpenTelemetry metrics ingestion to the Flight gRPC server.
#[must_use]
pub fn create_metrics_service(
    datafusion: Arc<dyn QueryEngine>,
    runtime: Option<Weak<Runtime>>,
) -> MetricsServiceServer<Service> {
    let service = Service {
        datafusion,
        runtime,
        once_tracer: OnceTracer::new(),
    };
    MetricsServiceServer::new(service).accept_compressed(CompressionEncoding::Gzip)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use arrow::array::AsArray;
    use arrow::array::Float64Array;
    use arrow::array::UInt64Array;
    use arrow::datatypes::Float64Type;
    use arrow::datatypes::UInt64Type;
    use opentelemetry_proto::tonic::common::v1::AnyValue;
    use opentelemetry_proto::tonic::metrics::v1::Histogram;

    fn string_attribute(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.to_string())),
            }),
            ..Default::default()
        }
    }

    fn number_data_point(value: f64, attributes: Vec<KeyValue>) -> NumberDataPoint {
        NumberDataPoint {
            attributes,
            start_time_unix_nano: 100,
            time_unix_nano: 200,
            exemplars: vec![],
            flags: 0,
            value: Some(Value::AsDouble(value)),
        }
    }

    fn histogram_data_point(
        count: u64,
        sum: Option<f64>,
        min: Option<f64>,
        max: Option<f64>,
        bucket_counts: Vec<u64>,
        explicit_bounds: Vec<f64>,
        attributes: Vec<KeyValue>,
    ) -> HistogramDataPoint {
        HistogramDataPoint {
            attributes,
            start_time_unix_nano: 100,
            time_unix_nano: 200,
            count,
            sum,
            bucket_counts,
            explicit_bounds,
            exemplars: vec![],
            flags: 0,
            min,
            max,
        }
    }

    fn column<'a>(batch: &'a RecordBatch, name: &str) -> &'a ArrayRef {
        let idx = batch
            .schema()
            .index_of(name)
            .expect("column should be present");
        batch.column(idx)
    }

    #[test]
    fn histogram_builds_expected_columns_and_values() {
        let data = Data::Histogram(Histogram {
            data_points: vec![
                histogram_data_point(
                    5,
                    Some(12.5),
                    Some(0.5),
                    Some(9.0),
                    vec![1, 2, 2],
                    vec![1.0, 5.0],
                    vec![string_attribute("host", "a")],
                ),
                histogram_data_point(
                    3,
                    Some(6.0),
                    Some(1.0),
                    Some(4.0),
                    vec![0, 1, 2],
                    vec![1.0, 5.0],
                    vec![string_attribute("host", "b")],
                ),
            ],
            aggregation_temporality: 0,
        });

        let (result, count) = metric_data_to_record_batch("latency", &data, None);
        assert_eq!(count, 2, "both data points should be counted");
        let batch = result.expect("record batch should build");

        assert_eq!(batch.num_rows(), 2);

        let counts = column(&batch, COUNT_COLUMN_NAME)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("count is UInt64");
        assert_eq!(counts.values().to_vec(), vec![5u64, 3]);

        let sums = column(&batch, SUM_COLUMN_NAME)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("sum is Float64");
        assert!((sums.value(0) - 12.5).abs() < f64::EPSILON);
        assert!((sums.value(1) - 6.0).abs() < f64::EPSILON);

        let mins = column(&batch, MIN_COLUMN_NAME)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("min is Float64");
        assert!((mins.value(0) - 0.5).abs() < f64::EPSILON);

        // bucket_counts is List<UInt64>
        let buckets = column(&batch, BUCKET_COUNTS_COLUMN_NAME).as_list::<i32>();
        let first_buckets = buckets.value(0);
        let first_buckets = first_buckets.as_primitive::<UInt64Type>();
        assert_eq!(first_buckets.values().to_vec(), vec![1u64, 2, 2]);

        // explicit_bounds is List<Float64>
        let bounds = column(&batch, EXPLICIT_BOUNDS_COLUMN_NAME).as_list::<i32>();
        let first_bounds = bounds.value(0);
        let first_bounds = first_bounds.as_primitive::<Float64Type>();
        let expected_bounds = [1.0_f64, 5.0];
        assert_eq!(first_bounds.len(), expected_bounds.len());
        for (got, want) in first_bounds.values().iter().zip(expected_bounds.iter()) {
            assert!((got - want).abs() < f64::EPSILON);
        }

        // attribute column is present
        let hosts = column(&batch, "host").as_string::<i32>();
        assert_eq!(hosts.value(0), "a");
        assert_eq!(hosts.value(1), "b");
    }

    #[test]
    fn histogram_handles_missing_optional_values_as_null() {
        let data = Data::Histogram(Histogram {
            data_points: vec![histogram_data_point(
                0,
                None,
                None,
                None,
                vec![],
                vec![],
                vec![],
            )],
            aggregation_temporality: 0,
        });

        let (result, _) = metric_data_to_record_batch("empty_metric", &data, None);
        let batch = result.expect("record batch should build");

        let sums = column(&batch, SUM_COLUMN_NAME)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("sum is Float64");
        assert!(sums.is_null(0), "missing sum should be null");

        let mins = column(&batch, MIN_COLUMN_NAME)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("min is Float64");
        assert!(mins.is_null(0), "missing min should be null");

        // Empty bucket_counts should produce an empty (non-null) list.
        let buckets = column(&batch, BUCKET_COUNTS_COLUMN_NAME).as_list::<i32>();
        assert!(!buckets.is_null(0));
        assert_eq!(buckets.value(0).len(), 0);
    }

    #[test]
    fn histogram_backfills_attribute_introduced_on_a_later_row() {
        // Regression: an attribute key first appearing on a data point at index >= 2 must be
        // backfilled with nulls for the preceding rows so all columns have equal length and the
        // value lands on the correct row.
        let data = Data::Histogram(Histogram {
            data_points: vec![
                histogram_data_point(
                    1,
                    Some(1.0),
                    None,
                    None,
                    vec![1],
                    vec![],
                    vec![string_attribute("protocol", "http")],
                ),
                histogram_data_point(
                    2,
                    Some(2.0),
                    None,
                    None,
                    vec![2],
                    vec![],
                    vec![string_attribute("protocol", "http")],
                ),
                // Third data point introduces a brand-new `tenant` attribute at row index 2.
                histogram_data_point(
                    3,
                    Some(3.0),
                    None,
                    None,
                    vec![3],
                    vec![],
                    vec![
                        string_attribute("protocol", "flightsql"),
                        string_attribute("tenant", "acme"),
                    ],
                ),
            ],
            aggregation_temporality: 0,
        });

        let (result, count) = metric_data_to_record_batch("query_duration_ms", &data, None);
        assert_eq!(count, 3);
        let batch = result.expect("record batch should build despite late attribute");

        assert_eq!(batch.num_rows(), 3);

        // Every column must have the same length (3).
        for column in batch.columns() {
            assert_eq!(column.len(), 3, "all columns must have equal length");
        }

        let tenant = column(&batch, "tenant").as_string::<i32>();
        assert!(tenant.is_null(0), "tenant absent on row 0 -> null");
        assert!(tenant.is_null(1), "tenant absent on row 1 -> null");
        assert_eq!(
            tenant.value(2),
            "acme",
            "tenant value must land on row 2, not row 0"
        );

        let protocol = column(&batch, "protocol").as_string::<i32>();
        assert_eq!(protocol.value(0), "http");
        assert_eq!(protocol.value(1), "http");
        assert_eq!(protocol.value(2), "flightsql");
    }

    #[test]
    fn histogram_with_no_data_points_is_error() {
        let data = Data::Histogram(Histogram {
            data_points: vec![],
            aggregation_temporality: 0,
        });

        let (result, count) = metric_data_to_record_batch("no_points", &data, None);
        assert_eq!(count, 0);
        assert!(
            matches!(result, Err(Error::MetricWithNoDataPoints {})),
            "empty histogram should return MetricWithNoDataPoints error"
        );
    }

    #[test]
    fn histogram_reuses_existing_attribute_schema() {
        // Build an initial batch to obtain a representative schema, then feed it back in as the
        // existing schema for a second batch with a data point missing the attribute.
        let first = Data::Histogram(Histogram {
            data_points: vec![histogram_data_point(
                1,
                Some(1.0),
                Some(1.0),
                Some(1.0),
                vec![1],
                vec![],
                vec![string_attribute("host", "a")],
            )],
            aggregation_temporality: 0,
        });
        let (first_result, _) = metric_data_to_record_batch("latency", &first, None);
        let first_batch = first_result.expect("first batch builds");
        let existing_schema = first_batch.schema();

        // Second batch: no attributes on the data point, but schema knows about `host`.
        let second = Data::Histogram(Histogram {
            data_points: vec![histogram_data_point(
                2,
                Some(2.0),
                Some(2.0),
                Some(2.0),
                vec![2],
                vec![],
                vec![],
            )],
            aggregation_temporality: 0,
        });
        let (second_result, _) =
            metric_data_to_record_batch("latency", &second, Some(existing_schema.as_ref()));
        let second_batch = second_result.expect("second batch builds");

        // `host` column should be carried over from the existing schema and be null here.
        let hosts = column(&second_batch, "host").as_string::<i32>();
        assert!(hosts.is_null(0), "missing attribute should be null");

        // Reserved histogram columns must not be duplicated as attributes.
        assert_eq!(
            second_batch
                .schema()
                .fields()
                .iter()
                .filter(|f| f.name() == COUNT_COLUMN_NAME)
                .count(),
            1,
            "count column should appear exactly once"
        );
    }

    #[test]
    fn detect_added_columns_reports_only_new_named_columns() {
        let existing = Schema::new(vec![
            Field::new(VALUE_COLUMN_NAME, DataType::Float64, true),
            Field::new(TIME_UNIX_NANO_COLUMN_NAME, DataType::UInt64, true),
            Field::new("region", DataType::Utf8, true),
        ]);
        // Same columns plus a new `tier` dimension.
        let incoming = Schema::new(vec![
            Field::new(VALUE_COLUMN_NAME, DataType::Float64, true),
            Field::new(TIME_UNIX_NANO_COLUMN_NAME, DataType::UInt64, true),
            Field::new("region", DataType::Utf8, true),
            Field::new("tier", DataType::Utf8, true),
        ]);
        assert_eq!(
            detect_added_columns(&existing, &incoming),
            vec!["tier".to_string()]
        );
        // No new columns -> empty (no evolution trigger).
        assert!(detect_added_columns(&existing, &existing).is_empty());
        // A subset (missing column) reports nothing added, even though it differs.
        assert!(detect_added_columns(&incoming, &existing).is_empty());
    }

    #[test]
    fn rebuild_against_evolved_schema_matches_exact_field_order() {
        // A metric first seen with only `region`, then a data point adds `tier`. The batch
        // built against the pre-evolution schema and the batch rebuilt against the evolved
        // schema must agree on column set AND order, since the write path's verify_schema is
        // exact-positional.
        let first = Data::Gauge(opentelemetry_proto::tonic::metrics::v1::Gauge {
            data_points: vec![number_data_point(
                1.0,
                vec![string_attribute("region", "us")],
            )],
        });
        let (first_result, _) = metric_data_to_record_batch("svc_requests", &first, None);
        let first_schema = first_result.expect("first batch builds").schema();

        // Second export introduces `tier`; build against the first schema to get the widened one.
        let second = Data::Gauge(opentelemetry_proto::tonic::metrics::v1::Gauge {
            data_points: vec![number_data_point(
                2.0,
                vec![
                    string_attribute("region", "eu"),
                    string_attribute("tier", "gold"),
                ],
            )],
        });
        let (widened_result, _) =
            metric_data_to_record_batch("svc_requests", &second, Some(first_schema.as_ref()));
        let widened_schema = widened_result.expect("widened batch builds").schema();
        assert!(detect_added_columns(&first_schema, &widened_schema) == vec!["tier".to_string()]);

        // Rebuilding the same data against the (evolved) widened schema yields the identical
        // field order — the invariant the OTel pre-flight relies on for the rebuilt batch.
        let (rebuilt_result, _) =
            metric_data_to_record_batch("svc_requests", &second, Some(widened_schema.as_ref()));
        let rebuilt_schema = rebuilt_result.expect("rebuilt batch builds").schema();
        let widened_names: Vec<&str> = widened_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        let rebuilt_names: Vec<&str> = rebuilt_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(widened_names, rebuilt_names);
    }

    fn field_names(schema: &Schema) -> Vec<&str> {
        schema.fields().iter().map(|f| f.name().as_str()).collect()
    }

    /// A histogram value-column name is an ordinary dimension on a Gauge or Sum, so it must be
    /// carried through the stored schema like any other: same position on every export, and
    /// null-backfilled when a data point omits it. Otherwise the batch stops matching the
    /// stored table and `write_data`'s exact-positional `verify_schema` rejects every export
    /// after the first.
    #[test]
    fn gauge_dimension_named_like_a_histogram_column_round_trips() {
        let first = Data::Gauge(opentelemetry_proto::tonic::metrics::v1::Gauge {
            data_points: vec![number_data_point(
                1.0,
                vec![
                    string_attribute("count", "5"),
                    string_attribute("host", "a"),
                ],
            )],
        });
        let (first_result, _) = metric_data_to_record_batch("svc", &first, None);
        let first_schema = first_result.expect("first batch builds").schema();
        assert_eq!(
            field_names(&first_schema),
            vec![
                VALUE_COLUMN_NAME,
                TIME_UNIX_NANO_COLUMN_NAME,
                START_TIME_UNIX_NANO_COLUMN_NAME,
                "count",
                "host"
            ]
        );

        // Second export, same dimensions: the schema must be identical, not just equivalent.
        let (second_result, _) = metric_data_to_record_batch("svc", &first, Some(&first_schema));
        let second_batch = second_result.expect("second batch builds");
        assert_eq!(
            field_names(&second_batch.schema()),
            field_names(&first_schema),
            "column order must match the stored schema"
        );
        assert_eq!(
            column(&second_batch, "count").as_string::<i32>().value(0),
            "5"
        );

        // Third export omits the `count` dimension: it must survive as a null column.
        let third = Data::Gauge(opentelemetry_proto::tonic::metrics::v1::Gauge {
            data_points: vec![number_data_point(2.0, vec![string_attribute("host", "b")])],
        });
        let (third_result, _) = metric_data_to_record_batch("svc", &third, Some(&first_schema));
        let third_batch = third_result.expect("third batch builds");
        assert_eq!(
            field_names(&third_batch.schema()),
            field_names(&first_schema),
            "a dimension missing from this export must stay in the schema"
        );
        assert!(
            column(&third_batch, "count").as_string::<i32>().is_null(0),
            "missing dimension should be null"
        );
    }

    /// An attribute that really does collide with one of the metric's own value columns cannot
    /// be represented next to it, so it is dropped rather than emitted as a second column of
    /// the same name.
    #[test]
    fn histogram_attribute_colliding_with_a_value_column_is_dropped() {
        let data = Data::Histogram(Histogram {
            data_points: vec![histogram_data_point(
                7,
                Some(1.0),
                None,
                None,
                vec![7],
                vec![],
                vec![
                    string_attribute(COUNT_COLUMN_NAME, "not a count"),
                    string_attribute("host", "a"),
                ],
            )],
            aggregation_temporality: 0,
        });

        let (result, _) = metric_data_to_record_batch("latency", &data, None);
        let batch = result.expect("record batch should build");

        assert_eq!(
            field_names(&batch.schema())
                .iter()
                .filter(|name| **name == COUNT_COLUMN_NAME)
                .count(),
            1,
            "count must appear exactly once"
        );
        let counts = column(&batch, COUNT_COLUMN_NAME)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("count is the UInt64 value column, not the attribute");
        assert_eq!(counts.values().to_vec(), vec![7u64]);
        assert_eq!(column(&batch, "host").as_string::<i32>().value(0), "a");
    }

    #[test]
    fn number_attribute_colliding_with_the_value_column_is_dropped() {
        let data = Data::Gauge(opentelemetry_proto::tonic::metrics::v1::Gauge {
            data_points: vec![number_data_point(
                1.5,
                vec![string_attribute(VALUE_COLUMN_NAME, "not a value")],
            )],
        });

        let (result, _) = metric_data_to_record_batch("svc", &data, None);
        let batch = result.expect("record batch should build");

        assert_eq!(
            field_names(&batch.schema()),
            vec![
                VALUE_COLUMN_NAME,
                TIME_UNIX_NANO_COLUMN_NAME,
                START_TIME_UNIX_NANO_COLUMN_NAME
            ]
        );
        let values = column(&batch, VALUE_COLUMN_NAME)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("value keeps the metric's own type");
        assert!((values.value(0) - 1.5).abs() < f64::EPSILON);
    }
}
