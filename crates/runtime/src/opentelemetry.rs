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

use std::borrow::Cow;
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
use datafusion::error::DataFusionError;
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
use runtime_query_engine::query_engine::Error as QueryEngineError;
use runtime_query_engine::query_engine::{QueryEngine, UpdateType};
use util::tracers::OnceTracer;
use util::warn_once;

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build record batch from OpenTelemetry metrics: {source}"))]
    FailedToBuildRecordBatch { source: arrow::error::ArrowError },

    #[snafu(display(
        "Failed to ingest OpenTelemetry metric {metric}: metric type {data_type} is not supported. \
        Supported metric types are Gauge, Sum and Histogram; its data points were rejected. \
        See: https://spiceai.org/docs/features/observability"
    ))]
    UnsupportedMetricDataType {
        metric: String,
        data_type: &'static str,
    },

    #[snafu(display(
        "Failed to ingest OpenTelemetry metric {metric}: its table has more than one column named \
        {columns}, so no export can be written to it. Drop and recreate the dataset for {metric} \
        to resume ingesting this metric. See: https://spiceai.org/docs/features/observability"
    ))]
    MetricTableHasDuplicateColumns { metric: String, columns: String },

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
            // Resource attributes (`service.name`, `service.instance.id`, …) identify the
            // process a data point came from, so they are merged into every data point's own
            // attributes and become dimension columns of the metric table.
            let resource_attrs = resource_metric
                .resource
                .as_ref()
                .map_or(&[][..], |resource| resource.attributes.as_slice());
            for scope_metric in resource_metric.scope_metrics {
                for metric in scope_metric.metrics {
                    let Some(data) = metric.data else {
                        tracing::debug!(
                            "OpenTelemetry export: metric {} has no data, skipping",
                            metric.name
                        );
                        continue;
                    };
                    let data_points_count = data_point_count(&data);
                    total_data_points += data_points_count;
                    tracing::debug!(
                        "OpenTelemetry export: processing metric {} (type={}, {} data point(s))",
                        metric.name,
                        metric_data_type_name(&data),
                        data_points_count
                    );
                    rejected_data_points += self
                        .ingest_metric(&metric.name, &data, resource_attrs, data_points_count)
                        .await;
                }
            }
        }

        // An export carrying no data points at all (e.g. only metrics with no data) rejected
        // nothing, so it succeeds; only an export that had data points and lost all of them is
        // a failed export.
        if total_data_points > 0 && rejected_data_points >= total_data_points {
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

/// Outcome of one attempt to build and write a metric's batch.
enum MetricWriteOutcome {
    /// The batch was written. `rejected` counts data points the batch could not hold, such
    /// as one whose value type differs from the metric's existing `value` column.
    Written { rejected: u64 },
    /// Nothing was written; every data point of the metric is rejected.
    Rejected,
    /// The write was refused because the batch no longer matches the table: another export
    /// added a column between this batch being built and written. Nothing was inserted, so
    /// rebuilding and retrying is safe. `built_against` is the schema the rejected batch was
    /// built from, which the retry loop compares to tell whether retrying can help.
    SchemaMismatch { built_against: Option<Schema> },
}

/// Cap on write attempts for one metric of one export. Retries already stop as soon as the
/// table schema stops changing, so this only bounds the rare case of a table that changes on
/// every attempt.
const MAX_METRIC_WRITE_ATTEMPTS: usize = 5;

/// Whether the write failed the schema check the runtime runs before inserting anything. No
/// rows were written, so the caller can rebuild the batch and retry without duplicating
/// rows.
fn is_schema_mismatch_error(error: &QueryEngineError) -> bool {
    let QueryEngineError::WriteData { source, .. } = error else {
        return false;
    };
    let DataFusionError::External(inner) = source else {
        return false;
    };
    inner
        .downcast_ref::<crate::datafusion::Error>()
        .is_some_and(|e| matches!(e, crate::datafusion::Error::SchemaMismatch { .. }))
}

impl Service {
    /// Ingests one metric of an export, returning how many of its data points were rejected.
    ///
    /// A write rejected by the schema check is retried, because another export adding a
    /// column between this batch being built and written is a race, not bad data. Any number
    /// of exports can add columns while this one is in flight, so one retry is not enough.
    /// Instead, retry for as long as the schema keeps changing: evolution only adds columns,
    /// so each retry gets closer, while an unchanged schema means a rebuild would produce
    /// the same rejected batch and the export gives up.
    async fn ingest_metric(
        &self,
        metric: &str,
        data: &Data,
        resource_attrs: &[KeyValue],
        data_points_count: u64,
    ) -> u64 {
        let mut previous_schema: Option<Option<Schema>> = None;
        for attempt in 1..=MAX_METRIC_WRITE_ATTEMPTS {
            match self
                .try_write_metric(metric, data, resource_attrs, data_points_count)
                .await
            {
                MetricWriteOutcome::Written { rejected } => return rejected,
                MetricWriteOutcome::Rejected => return data_points_count,
                MetricWriteOutcome::SchemaMismatch { built_against } => {
                    if previous_schema.as_ref() == Some(&built_against) {
                        // Same schema as the last attempt, so a rebuild changes nothing.
                        tracing::warn!(
                            "Failed to write OpenTelemetry data for metric {metric}: the batch no longer matches the table, so its data points were rejected"
                        );
                        return data_points_count;
                    }
                    if attempt == MAX_METRIC_WRITE_ATTEMPTS {
                        tracing::warn!(
                            "Failed to write OpenTelemetry data for metric {metric}: the table schema changed during every one of {MAX_METRIC_WRITE_ATTEMPTS} write attempts, so its data points were rejected"
                        );
                        return data_points_count;
                    }
                    previous_schema = Some(built_against);
                    tracing::debug!(
                        "OpenTelemetry export: the table schema for metric {metric} changed during the write, rebuilding the batch and retrying"
                    );
                }
            }
        }
        data_points_count
    }

    /// One attempt at ingesting a metric: build its batch against the stored schema, add any
    /// new columns the batch carries to the table, then write it.
    async fn try_write_metric(
        &self,
        metric: &str,
        data: &Data,
        resource_attrs: &[KeyValue],
        data_points_count: u64,
    ) -> MetricWriteOutcome {
        let table_ref = TableReference::bare(metric.to_string());
        let existing_schema = match self.datafusion.get_arrow_schema(table_ref.clone()).await {
            Ok(schema) => Some(schema),
            // The dataset is not registered yet: a `sink` dataset has no table until its
            // first write. Use the acceleration checkpoint instead, so the batch still gets
            // every stored column. Building it without them makes it narrower than the
            // table, and the write rejects it as having dropped columns.
            Err(_) => match self.runtime.as_ref().and_then(Weak::upgrade) {
                Some(runtime) => runtime
                    .accelerated_checkpoint_schema(&table_ref)
                    .await
                    .map(|schema| schema.as_ref().clone()),
                None => None,
            },
        };
        let (record_batch_result, _) =
            metric_data_to_record_batch(metric, data, resource_attrs, existing_schema.as_ref());
        let mut record_batch = match record_batch_result {
            Ok(record_batch) => record_batch,
            Err(e) => {
                tracing::error!(
                    "Failed to build arrow data from OpenTelemetry metrics for metric {metric}: {e}"
                );
                return MetricWriteOutcome::Rejected;
            }
        };

        if !self.datafusion.is_writable(&table_ref) {
            warn_once!(
                self.once_tracer,
                "No writable dataset defined for metric {}, skipping",
                metric
            );
            tracing::debug!(
                "OpenTelemetry export: metric {metric} is not writable, rejecting {data_points_count} data point(s)"
            );
            return MetricWriteOutcome::Rejected;
        }

        // If this batch carries columns the table does not have, add them before writing, so
        // the write accepts the wider batch. `on_schema_change` decides whether that is
        // allowed; when it is not, this does nothing and the write rejects the batch.
        if let Some(existing) = existing_schema.as_ref() {
            let added = detect_added_columns(existing, record_batch.schema().as_ref());
            if !added.is_empty()
                && let Some(runtime) = self.runtime.as_ref().and_then(Weak::upgrade)
            {
                match runtime
                    .evolve_accelerated_schema_for_write(&table_ref, &record_batch.schema())
                    .await
                {
                    Ok(Some(evolved)) => {
                        tracing::debug!(
                            "OpenTelemetry export: evolved schema for metric {metric} (added dimension(s): {})",
                            added.join(", ")
                        );
                        // Rebuild against the new schema. The write compares columns by
                        // position, so the batch must match it in order as well as by name.
                        match metric_data_to_record_batch(
                            metric,
                            data,
                            resource_attrs,
                            Some(&evolved),
                        )
                        .0
                        {
                            Ok(rebuilt) => record_batch = rebuilt,
                            Err(e) => {
                                tracing::warn!(
                                    "Failed to rebuild OpenTelemetry batch for metric {metric} after schema evolution: {e}"
                                );
                                return MetricWriteOutcome::Rejected;
                            }
                        }
                    }
                    Ok(None) => {
                        // The columns were not added, so let the write reject the batch.
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to evolve schema for OpenTelemetry metric {metric}: {e}"
                        );
                        return MetricWriteOutcome::Rejected;
                    }
                }
            }
        }

        // The build skips any data point the batch cannot hold, such as one whose value type
        // differs from the metric's `value` column. Those points are missing from the batch,
        // so count them as rejected instead of reporting them as written.
        let batch_rows = u64::try_from(record_batch.num_rows()).unwrap_or(u64::MAX);
        let skipped = data_points_count.saturating_sub(batch_rows);
        if skipped > 0 {
            tracing::debug!(
                "OpenTelemetry export: {skipped} data point(s) of metric {metric} could not be represented in its table and were rejected"
            );
        }

        let schema = record_batch.schema();
        if let Err(e) = self
            .datafusion
            .write_data(&table_ref, schema, vec![record_batch], UpdateType::Append)
            .await
        {
            if is_schema_mismatch_error(&e) {
                return MetricWriteOutcome::SchemaMismatch {
                    built_against: existing_schema,
                };
            }
            // Warn, not debug: this drops data points, and the underlying error is the only
            // explanation the operator gets.
            tracing::warn!("Failed to write OpenTelemetry data for metric {metric}: {e}");
            return MetricWriteOutcome::Rejected;
        }
        tracing::debug!(
            "OpenTelemetry export: wrote {batch_rows} data point(s) for metric {metric}"
        );
        MetricWriteOutcome::Written { rejected: skipped }
    }
}

/// Builds the batch for one metric, paired with the number of data points it carries.
///
/// The count is the *whole* metric's data-point count, including data points that cannot be
/// built into a batch (an unsupported metric type, a corrupt stored schema). The caller adds it
/// to the export's total and, on `Err`, to its rejected count — so dropped data points are
/// reported to the client through `ExportMetricsPartialSuccess.rejected_data_points` instead of
/// being invisible in a mixed export (#12188).
///
/// `resource_attrs` are the resource-level attributes of the export the metric arrived in; they
/// are merged into every data point's attributes (see `ResourceAttributeMerger`).
pub fn metric_data_to_record_batch(
    metric: &str,
    data: &Data,
    resource_attrs: &[KeyValue],
    existing_schema: Option<&Schema>,
) -> (Result<RecordBatch>, u64) {
    let data_points_count = data_point_count(data);

    // Arrow permits duplicate field names, so a metric table can carry two same-named columns.
    // No batch this module builds ever does (attributes are keyed by name and one colliding
    // with a value column is dropped), and `write_data`'s `verify_schema` is exact-positional,
    // so such a table rejects every export. Fail with an error naming the metric and the fix
    // rather than letting each export be dropped by a mismatch the operator cannot act on.
    if let Some(schema) = existing_schema {
        let duplicates = duplicate_column_names(schema);
        if !duplicates.is_empty() {
            return (
                MetricTableHasDuplicateColumnsSnafu {
                    metric,
                    columns: duplicates.join(", "),
                }
                .fail(),
                data_points_count,
            );
        }
    }

    let record_batch = match data {
        Data::Gauge(gauge) => number_data_points_to_record_batch(
            metric,
            &gauge.data_points,
            resource_attrs,
            existing_schema,
        ),
        Data::Sum(sum) => number_data_points_to_record_batch(
            metric,
            &sum.data_points,
            resource_attrs,
            existing_schema,
        ),
        Data::Histogram(histogram) => histogram_data_points_to_record_batch(
            metric,
            &histogram.data_points,
            resource_attrs,
            existing_schema,
        ),
        // TODO: Support other metric data types (ExponentialHistogram, Summary)
        Data::ExponentialHistogram(_) | Data::Summary(_) => UnsupportedMetricDataTypeSnafu {
            metric,
            data_type: metric_data_type_name(data),
        }
        .fail(),
    };

    (record_batch, data_points_count)
}

/// Number of data points a metric carries, for every metric type — including the types this
/// module cannot yet build a batch for, whose data points are rejected rather than written.
fn data_point_count(data: &Data) -> u64 {
    match data {
        Data::Gauge(gauge) => gauge.data_points.len() as u64,
        Data::Sum(sum) => sum.data_points.len() as u64,
        Data::Histogram(histogram) => histogram.data_points.len() as u64,
        Data::ExponentialHistogram(histogram) => histogram.data_points.len() as u64,
        Data::Summary(summary) => summary.data_points.len() as u64,
    }
}

/// Names appearing on more than one field of `schema`, in first-seen order (empty when the
/// schema is well-formed). Arrow allows duplicate field names, so this cannot be assumed away.
fn duplicate_column_names(schema: &Schema) -> Vec<&str> {
    let mut seen: HashSet<&str> = HashSet::with_capacity(schema.fields().len());
    let mut duplicates: Vec<&str> = Vec::new();
    for field in schema.fields() {
        let name = field.name().as_str();
        if !seen.insert(name) && !duplicates.contains(&name) {
            duplicates.push(name);
        }
    }
    duplicates
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

/// Appends `$converted` to `$builder` when both the downcast and the conversion succeeded.
macro_rules! append_converted {
    ($builder:expr, $builder_type:ty, $converted:expr) => {
        match (
            $builder.as_any_mut().downcast_mut::<$builder_type>(),
            $converted,
        ) {
            (Some(builder), Some(value)) => {
                builder.append_value(value);
                true
            }
            _ => false,
        }
    };
}

/// Appends a data point's value to the metric's `value` column, converting between integer
/// and double when the conversion is exact. Returns `false` when it is not, so the caller can
/// report the data point as rejected rather than store a rounded number.
///
/// A metric reported as an integer on one data point and a double on the next is common, and
/// the whole point is still kept whenever the two forms agree exactly.
fn append_number_value(builder: &mut dyn ArrayBuilder, value: &Value) -> bool {
    if builder.as_any().is::<Float64Builder>() {
        let converted = match value {
            Value::AsDouble(v) => Some(*v),
            Value::AsInt(v) => exact_f64_from_i64(*v),
        };
        return append_converted!(builder, Float64Builder, converted);
    }
    let converted = match value {
        Value::AsInt(v) => Some(*v),
        Value::AsDouble(v) => exact_i64_from_f64(*v),
    };
    append_converted!(builder, Int64Builder, converted)
}

/// The `value` column for a metric's first data point, taking its type from that value.
fn new_number_value_column(value: &Value) -> (Box<dyn ArrayBuilder>, DataType) {
    match value {
        Value::AsDouble(v) => {
            let mut builder = Float64Builder::new();
            builder.append_value(*v);
            (Box::new(builder), DataType::Float64)
        }
        Value::AsInt(v) => {
            let mut builder = Int64Builder::new();
            builder.append_value(*v);
            (Box::new(builder), DataType::Int64)
        }
    }
}

fn number_data_points_to_record_batch(
    metric: &str,
    data_points: &Vec<NumberDataPoint>,
    resource_attrs: &[KeyValue],
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

    let mut warned_value_type = false;
    for data_point in data_points {
        if let Some(value) = &data_point.value {
            if let Some(builder) = &mut values_builder {
                if !append_number_value(builder.as_mut(), value) {
                    if !warned_value_type {
                        warned_value_type = true;
                        tracing::warn!(
                            "Metric '{metric}' sent a value that does not fit the {} column already storing it, so that data point was rejected. Report '{metric}' with one value type from the exporter producing it. See: https://spiceai.org/docs/features/observability",
                            column_type_name(&values_type),
                        );
                    }
                    continue;
                }
            } else {
                let (builder, data_type) = new_number_value_column(value);
                values_builder = Some(builder);
                values_type = data_type;
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
        resource_attrs,
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
    resource_attrs: &[KeyValue],
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
        resource_attrs,
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

/// The value of an OTLP attribute as text, when it has one that is exact.
///
/// Every scalar has a faithful text form, so a string column accepts them all. Bytes do not:
/// they are not required to be valid text.
fn attribute_as_str(value: &any_value::Value) -> Option<Cow<'_, str>> {
    match value {
        any_value::Value::StringValue(v) => Some(Cow::Borrowed(v)),
        any_value::Value::BoolValue(v) => Some(Cow::Owned(v.to_string())),
        any_value::Value::IntValue(v) => Some(Cow::Owned(v.to_string())),
        // `{}` prints the shortest text that parses back to the same value, so this is
        // exact. Infinities and NaN have no numeric text form and are left out.
        any_value::Value::DoubleValue(v) if v.is_finite() => Some(Cow::Owned(v.to_string())),
        _ => None,
    }
}

/// The value of an OTLP attribute as a boolean, when it is exactly one. Only the two words a
/// boolean prints as are accepted from text.
fn attribute_as_bool(value: &any_value::Value) -> Option<bool> {
    match value {
        any_value::Value::BoolValue(v) => Some(*v),
        any_value::Value::StringValue(v) => v.parse().ok(),
        _ => None,
    }
}

/// `value` as an `i64` that loses nothing: a whole double inside the `i64` range, or text
/// that parses as one.
fn attribute_as_i64(value: &any_value::Value) -> Option<i64> {
    match value {
        any_value::Value::IntValue(v) => Some(*v),
        any_value::Value::DoubleValue(v) => exact_i64_from_f64(*v),
        any_value::Value::StringValue(v) => v.parse().ok(),
        _ => None,
    }
}

/// `value` as a `u64` that loses nothing. A negative integer has no `u64` form.
fn attribute_as_u64(value: &any_value::Value) -> Option<u64> {
    match value {
        any_value::Value::IntValue(v) => u64::try_from(*v).ok(),
        any_value::Value::DoubleValue(v) => exact_u64_from_f64(*v),
        any_value::Value::StringValue(v) => v.parse().ok(),
        _ => None,
    }
}

/// `value` as an `f64` that loses nothing. A large integer is refused when a double cannot
/// hold it exactly, since storing a rounded value would report a number the client never
/// sent.
fn attribute_as_f64(value: &any_value::Value) -> Option<f64> {
    match value {
        any_value::Value::DoubleValue(v) => Some(*v),
        any_value::Value::IntValue(v) => exact_f64_from_i64(*v),
        any_value::Value::StringValue(v) => {
            v.parse().ok().filter(|parsed: &f64| parsed.is_finite())
        }
        _ => None,
    }
}

/// `value` as bytes. Text is stored as its UTF-8 bytes, which loses nothing.
fn attribute_as_bytes(value: &any_value::Value) -> Option<&[u8]> {
    match value {
        any_value::Value::BytesValue(v) => Some(v),
        any_value::Value::StringValue(v) => Some(v.as_bytes()),
        _ => None,
    }
}

/// Whether `d` holds a whole number, so converting it to an integer loses nothing.
#[expect(
    clippy::float_cmp,
    reason = "an exact comparison is the point: a value that differs from its own truncation \
              by any amount at all has no integer form"
)]
fn is_whole(d: f64) -> bool {
    d.is_finite() && d == d.trunc()
}

/// `d` as an `i64`, if it is whole and inside the `i64` range. A whole double in range has an
/// exact `i64` value, so the conversion loses nothing.
#[expect(
    clippy::cast_possible_truncation,
    reason = "`is_whole` rules out a fraction and `i128` is far wider than the `f64` range, so \
              the cast is exact; `try_from` then rejects anything outside `i64`"
)]
fn exact_i64_from_f64(d: f64) -> Option<i64> {
    is_whole(d).then(|| i64::try_from(d as i128).ok()).flatten()
}

/// `d` as a `u64`, if it is whole, not negative, and inside the `u64` range.
#[expect(
    clippy::cast_possible_truncation,
    reason = "exact for the same reason as `exact_i64_from_f64`; `try_from` rejects a negative \
              value and anything outside `u64`"
)]
fn exact_u64_from_f64(d: f64) -> Option<u64> {
    is_whole(d).then(|| u64::try_from(d as i128).ok()).flatten()
}

/// `i` as an `f64`, if a double holds it exactly. A double carries 53 bits of integer
/// precision, so a larger integer rounds; comparing in `i128` catches exactly that and
/// refuses the value instead of storing a number the client never sent.
#[expect(
    clippy::cast_precision_loss,
    reason = "the possible precision loss is what this function detects and refuses"
)]
#[expect(
    clippy::cast_possible_truncation,
    reason = "`i128` covers the whole `f64` integer range, so this cast cannot truncate"
)]
fn exact_f64_from_i64(i: i64) -> Option<f64> {
    let converted = i as f64;
    (converted as i128 == i128::from(i)).then_some(converted)
}

/// Appends `value` to a column that already holds `target` values, converting it when the
/// conversion is exact. Returns `false` when the value cannot be stored without changing it,
/// which is when the caller stores NULL instead.
///
/// This is what lets one attribute arrive as an integer on one data point and a double, or a
/// string, on the next: the column keeps the type it was created with and every value that
/// fits it exactly is still stored.
fn append_coerced_attribute(
    builder: &mut dyn ArrayBuilder,
    target: &DataType,
    value: &any_value::Value,
) -> bool {
    match target {
        DataType::Utf8 => append_converted!(builder, StringBuilder, attribute_as_str(value)),
        DataType::Boolean => append_converted!(builder, BooleanBuilder, attribute_as_bool(value)),
        DataType::Int64 => append_converted!(builder, Int64Builder, attribute_as_i64(value)),
        DataType::UInt64 => append_converted!(builder, UInt64Builder, attribute_as_u64(value)),
        DataType::Float64 => append_converted!(builder, Float64Builder, attribute_as_f64(value)),
        DataType::Binary => append_converted!(builder, BinaryBuilder, attribute_as_bytes(value)),
        // Lists hold a histogram's bucket arrays, which no attribute value maps onto.
        _ => false,
    }
}

macro_rules! new_attribute_column {
    ($builder_type:ty, $data_type:expr, $value:expr, $nulls:expr) => {{
        let mut builder = <$builder_type>::new();
        // This attribute was absent from every preceding data point, so backfill a null for
        // each. That puts the value on the right row and keeps every column the same length.
        for _ in 0..$nulls {
            builder.append_null();
        }
        builder.append_value($value);
        Some((Box::new(builder) as Box<dyn ArrayBuilder>, $data_type))
    }};
}

/// The column for an attribute key seen for the first time, holding `nulls` leading nulls and
/// then `value`. Its type is the value's own; later values of other types are converted into
/// it by [`append_coerced_attribute`]. `None` for a value type with no column type.
fn new_attribute_column(
    value: &any_value::Value,
    nulls: usize,
) -> Option<(Box<dyn ArrayBuilder>, DataType)> {
    match value {
        any_value::Value::StringValue(v) => {
            new_attribute_column!(StringBuilder, DataType::Utf8, v, nulls)
        }
        any_value::Value::BoolValue(v) => {
            new_attribute_column!(BooleanBuilder, DataType::Boolean, *v, nulls)
        }
        any_value::Value::IntValue(v) => {
            new_attribute_column!(Int64Builder, DataType::Int64, *v, nulls)
        }
        any_value::Value::DoubleValue(v) => {
            new_attribute_column!(Float64Builder, DataType::Float64, *v, nulls)
        }
        any_value::Value::BytesValue(v) => {
            new_attribute_column!(BinaryBuilder, DataType::Binary, v, nulls)
        }
        // Arrays and key-value lists have no column type yet.
        _ => None,
    }
}

/// Stores one attribute value in its column, creating the column if this is the first data
/// point to carry the key. Returns `false` when the value cannot be stored, so the caller can
/// report it and store NULL.
fn append_attribute(
    fields: &mut IndexMap<String, Arc<Field>>,
    columns: &mut IndexMap<String, Box<dyn ArrayBuilder>>,
    key: &String,
    value: &any_value::Value,
    row_index: usize,
) -> bool {
    if let Some(field) = fields.get(key) {
        let target = field.data_type().clone();
        return columns
            .get_mut(key)
            .is_some_and(|builder| append_coerced_attribute(builder.as_mut(), &target, value));
    }

    let Some((builder, data_type)) = new_attribute_column(value, row_index) else {
        return false;
    };
    fields.insert(
        key.clone(),
        Arc::new(Field::new(key.as_str(), data_type, true)),
    );
    columns.insert(key.clone(), builder);
    true
}

/// The name of an attribute value's type, as an OTLP user would say it.
fn attribute_value_type_name(value: &any_value::Value) -> &'static str {
    match value {
        any_value::Value::StringValue(_) => "string",
        any_value::Value::BoolValue(_) => "boolean",
        any_value::Value::IntValue(_) => "integer",
        any_value::Value::DoubleValue(_) => "double",
        any_value::Value::BytesValue(_) => "bytes",
        any_value::Value::ArrayValue(_) => "array",
        any_value::Value::KvlistValue(_) => "key-value list",
        // An index into the request's shared string table, which this ingest does not read.
        any_value::Value::StringValueStrindex(_) => "interned string",
    }
}

/// The name of a metric column's type, as an OTLP user would say it.
fn column_type_name(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string",
        DataType::Boolean => "boolean",
        DataType::Int64 => "integer",
        DataType::UInt64 => "unsigned integer",
        DataType::Float64 => "double",
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "bytes",
        DataType::List(_) => "list",
        _ => "unsupported",
    }
}

/// Merges the export's resource attributes into each data point's own attributes. Nothing is
/// copied; both slices stay borrowed from the request.
///
/// When a key appears in both, the data point's value wins, because it describes that one
/// measurement rather than the whole process.
///
/// The data point's keys go into a set first, so a row costs the two attribute counts added
/// rather than multiplied. Attribute lists can be long and this runs inline in `export`. The
/// set is reused, so a batch allocates one no matter how many data points it has.
struct ResourceAttributeMerger<'a> {
    resource_attrs: &'a [KeyValue],
    data_point_keys: HashSet<&'a str>,
}

impl<'a> ResourceAttributeMerger<'a> {
    fn new(resource_attrs: &'a [KeyValue]) -> Self {
        Self {
            resource_attrs,
            data_point_keys: HashSet::new(),
        }
    }

    /// One row's attributes: the resource attributes the data point does not override, followed
    /// by the data point's own.
    fn row(&mut self, data_point_attrs: &'a [KeyValue]) -> impl Iterator<Item = &'a KeyValue> {
        self.data_point_keys.clear();
        // With no resource attributes nothing can be overridden, so the keys are not collected.
        if !self.resource_attrs.is_empty() {
            self.data_point_keys
                .extend(data_point_attrs.iter().map(|attr| attr.key.as_str()));
        }

        let data_point_keys = &self.data_point_keys;
        self.resource_attrs
            .iter()
            .filter(move |resource_attr| !data_point_keys.contains(resource_attr.key.as_str()))
            .chain(data_point_attrs.iter())
    }
}

#[expect(clippy::type_complexity)]
fn attributes_to_fields_and_columns(
    metric: &str,
    resource_attrs: &[KeyValue],
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
    let mut warned_duplicates: HashSet<&str> = HashSet::new();
    let mut warned_type_mismatch: HashSet<&str> = HashSet::new();
    let mut row_keys: HashSet<&str> = HashSet::new();

    initialize_attribute_schema(
        metric,
        &mut fields,
        &mut columns,
        existing_schema,
        value_columns,
    );

    let mut merger = ResourceAttributeMerger::new(resource_attrs);
    for (i, inner_attributes) in attributes.iter().copied().enumerate() {
        row_keys.clear();
        for attribute in merger.row(inner_attributes) {
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
            // OTLP requires attribute keys to be unique within a data point, but a client can
            // still send duplicates. Appending both values would give that column one row
            // more than the others and fail the whole batch, so keep only the first.
            if !row_keys.insert(key_str) {
                if warned_duplicates.insert(key_str) {
                    tracing::warn!(
                        "Metric '{metric}' has a data point that carries the attribute '{key_str}' more than once: the first value is kept and the later values are ignored. Remove the duplicated attribute from the exporter producing this metric. See: https://spiceai.org/docs/features/observability"
                    );
                }
                continue;
            }
            if let Some(any_value) = &attribute.value {
                if let Some(value) = &any_value.value {
                    if !append_attribute(&mut fields, &mut columns, &attribute.key, value, i) {
                        // The value does not fit this attribute's column and converting it
                        // would change it, so store NULL and say so once per attribute.
                        if warned_type_mismatch.insert(key_str) {
                            let stored = fields
                                .get(key_str)
                                .map(|field| column_type_name(field.data_type()));
                            let value_type = attribute_value_type_name(value);
                            if let Some(stored) = stored {
                                tracing::warn!(
                                    "Metric '{metric}' sent attribute '{key_str}' as a {value_type} that does not fit the {stored} column already storing it, so it is recorded as NULL. Send '{key_str}' with one type, or with values that convert exactly, from the exporter producing this metric. See: https://spiceai.org/docs/features/observability"
                                );
                            } else {
                                tracing::warn!(
                                    "Metric '{metric}' sent attribute '{key_str}' as a {value_type}, which cannot be stored as a column, so it is recorded as NULL. Send '{key_str}' as a string, boolean, integer, double or bytes value from the exporter producing this metric. See: https://spiceai.org/docs/features/observability"
                                );
                            }
                        }
                        append_null(&mut fields, &mut columns, key_str);
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

/// Builder plus the canonical Arrow type it produces for a stored dimension column, or `None`
/// when this module has no builder for the column's type.
///
/// The string/binary *families* collapse onto the canonical `Utf8`/`Binary` builders:
/// accelerators store them in view/large layouts (e.g. Cayenne stores `Utf8View`) but
/// `append_attribute!` always builds `Utf8`/`Binary` and `verify_schema` treats the families
/// as equivalent, so seeding the canonical type writes back cleanly. `UInt64` and the two list
/// types cover the columns the histogram path (#11992) writes — `count` and the
/// `bucket_counts`/`explicit_bounds` arrays — which are value columns on a histogram but
/// ordinary stored dimensions for a data point of another shape (#12117); the list element
/// field is taken from the stored schema so the built array's type matches the field exactly.
fn dimension_builder_for(field: &Field) -> Option<(Box<dyn ArrayBuilder>, DataType)> {
    match field.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            Some((Box::new(StringBuilder::new()), DataType::Utf8))
        }
        DataType::Boolean => Some((Box::new(BooleanBuilder::new()), DataType::Boolean)),
        DataType::Int64 => Some((Box::new(Int64Builder::new()), DataType::Int64)),
        DataType::UInt64 => Some((Box::new(UInt64Builder::new()), DataType::UInt64)),
        DataType::Float64 => Some((Box::new(Float64Builder::new()), DataType::Float64)),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            Some((Box::new(BinaryBuilder::new()), DataType::Binary))
        }
        DataType::List(item) => match item.data_type() {
            DataType::UInt64 => Some((
                Box::new(ListBuilder::new(UInt64Builder::new()).with_field(Arc::clone(item))),
                field.data_type().clone(),
            )),
            DataType::Float64 => Some((
                Box::new(ListBuilder::new(Float64Builder::new()).with_field(Arc::clone(item))),
                field.data_type().clone(),
            )),
            _ => None,
        },
        _ => None,
    }
}

fn initialize_attribute_schema(
    metric: &str,
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

            // `fields` and `columns` are zipped positionally into the batch, so a field seeded
            // without a builder makes the two lists disagree and `RecordBatch::try_new` rejects
            // the whole export. Seed the pair together, or drop the column (naming it) and let
            // the write path report the schema mismatch it can describe.
            let Some((builder, builder_type)) = dimension_builder_for(field) else {
                tracing::warn!(
                    "Metric {metric} has stored column {name} of unsupported type {data_type}, dropping it from this export",
                    name = field.name(),
                    data_type = field.data_type(),
                );
                continue;
            };

            // Force the seeded dimension column nullable: a data point that omits this
            // attribute is backfilled with NULL, so a non-nullable field (e.g. a source
            // column the accelerator stored as `NOT NULL`) would make `RecordBatch::try_new`
            // reject the whole batch and drop the export. Adjust the stored field's data type
            // to the builder's output (matching the view/large-family collapse) and force it
            // nullable, preserving its metadata rather than rebuilding it from scratch.
            let name = field.name().clone();
            let nullable_field = Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(builder_type)
                    .with_nullable(true),
            );
            fields.insert(name.clone(), nullable_field);
            columns.insert(name, builder);
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
        // Keep in step with `dimension_builder_for`: a column it can seed but this cannot null
        // stays short of the other columns, and `RecordBatch::try_new` rejects the export.
        match field.data_type() {
            DataType::Utf8 => append_null!(columns, key, StringBuilder),
            DataType::Boolean => append_null!(columns, key, BooleanBuilder),
            DataType::Int64 => append_null!(columns, key, Int64Builder),
            DataType::UInt64 => append_null!(columns, key, UInt64Builder),
            DataType::Float64 => append_null!(columns, key, Float64Builder),
            DataType::Binary => append_null!(columns, key, BinaryBuilder),
            DataType::List(item) => match item.data_type() {
                DataType::UInt64 => append_null!(columns, key, ListBuilder<UInt64Builder>),
                DataType::Float64 => append_null!(columns, key, ListBuilder<Float64Builder>),
                _ => {}
            },
            _ => {}
        }
    }
}

/// Builds the OpenTelemetry metrics ingest [`Service`] (the `MetricsService::export` handler).
///
/// Exposed so the ingest handler can be driven directly (e.g. in tests) without standing up a
/// gRPC transport; production wiring goes through [`create_metrics_service`].
#[must_use]
pub fn build_metrics_service(
    datafusion: Arc<dyn QueryEngine>,
    runtime: Option<Weak<Runtime>>,
) -> Service {
    Service {
        datafusion,
        runtime,
        once_tracer: OnceTracer::new(),
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
    MetricsServiceServer::new(build_metrics_service(datafusion, runtime))
        .accept_compressed(CompressionEncoding::Gzip)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use arrow::array::AsArray;
    use arrow::array::Float64Array;
    use arrow::array::Int64Array;
    use arrow::array::UInt64Array;
    use arrow::datatypes::Float64Type;
    use arrow::datatypes::UInt64Type;
    use datafusion::datasource::TableProvider;
    use datafusion::error::DataFusionError;
    use datafusion::execution::SendableRecordBatchStream;
    use datafusion::logical_expr::LogicalPlan;
    use datafusion::prelude::SessionContext;
    use opentelemetry_proto::tonic::common::v1::AnyValue;
    use opentelemetry_proto::tonic::metrics::v1::ExponentialHistogram;
    use opentelemetry_proto::tonic::metrics::v1::ExponentialHistogramDataPoint;
    use opentelemetry_proto::tonic::metrics::v1::Gauge;
    use opentelemetry_proto::tonic::metrics::v1::Histogram;
    use opentelemetry_proto::tonic::metrics::v1::Metric as OtlpMetric;
    use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
    use opentelemetry_proto::tonic::metrics::v1::ScopeMetrics;
    use opentelemetry_proto::tonic::metrics::v1::Summary;
    use opentelemetry_proto::tonic::metrics::v1::SummaryDataPoint;
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use parking_lot::Mutex;
    use runtime_query_engine::query_engine::Error as QueryEngineError;
    use runtime_query_engine::query_engine::QueryRequest;
    use runtime_query_engine::query_engine::Result as QueryEngineResult;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;

    fn string_attribute(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.to_string())),
            }),
            ..Default::default()
        }
    }

    /// An attribute carrying `value` verbatim, for the type-coercion tests.
    fn typed_attribute(key: &str, value: any_value::Value) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue { value: Some(value) }),
            ..Default::default()
        }
    }

    fn int_attribute(key: &str, value: i64) -> KeyValue {
        typed_attribute(key, any_value::Value::IntValue(value))
    }

    fn double_attribute(key: &str, value: f64) -> KeyValue {
        typed_attribute(key, any_value::Value::DoubleValue(value))
    }

    fn bool_attribute(key: &str, value: bool) -> KeyValue {
        typed_attribute(key, any_value::Value::BoolValue(value))
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

        let (result, count) = metric_data_to_record_batch("latency", &data, &[], None);
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

        let (result, _) = metric_data_to_record_batch("empty_metric", &data, &[], None);
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

        let (result, count) = metric_data_to_record_batch("query_duration_ms", &data, &[], None);
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

        let (result, count) = metric_data_to_record_batch("no_points", &data, &[], None);
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
        let (first_result, _) = metric_data_to_record_batch("latency", &first, &[], None);
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
            metric_data_to_record_batch("latency", &second, &[], Some(existing_schema.as_ref()));
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
        let (first_result, _) = metric_data_to_record_batch("svc_requests", &first, &[], None);
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
            metric_data_to_record_batch("svc_requests", &second, &[], Some(first_schema.as_ref()));
        let widened_schema = widened_result.expect("widened batch builds").schema();
        assert!(detect_added_columns(&first_schema, &widened_schema) == vec!["tier".to_string()]);

        // Rebuilding the same data against the (evolved) widened schema yields the identical
        // field order — the invariant the OTel pre-flight relies on for the rebuilt batch.
        let (rebuilt_result, _) = metric_data_to_record_batch(
            "svc_requests",
            &second,
            &[],
            Some(widened_schema.as_ref()),
        );
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

    #[test]
    fn seeded_non_nullable_dimension_is_null_filled_not_dropped() {
        // Regression: a metric whose stored schema declares a dimension column as
        // non-nullable (e.g. a source column the accelerator persisted as `NOT NULL`).
        // A data point that omits that dimension must produce the column present-but-NULL,
        // not fail the batch build (which would silently drop the whole export).
        let existing = Schema::new(vec![
            Field::new(VALUE_COLUMN_NAME, DataType::Float64, true),
            Field::new(TIME_UNIX_NANO_COLUMN_NAME, DataType::UInt64, true),
            Field::new(START_TIME_UNIX_NANO_COLUMN_NAME, DataType::UInt64, true),
            Field::new("region", DataType::Utf8, false),
        ]);

        let data = Data::Gauge(opentelemetry_proto::tonic::metrics::v1::Gauge {
            data_points: vec![number_data_point(1.0, vec![])],
        });

        let (result, _) = metric_data_to_record_batch("svc_requests", &data, &[], Some(&existing));
        let batch = result.expect("batch must build despite the non-nullable stored dimension");

        let region_field = batch
            .schema()
            .field_with_name("region")
            .expect("region column carried over from the existing schema")
            .clone();
        assert!(
            region_field.is_nullable(),
            "seeded dimension column must be emitted nullable so NULL backfill is valid"
        );

        let region = column(&batch, "region").as_string::<i32>();
        assert!(
            region.is_null(0),
            "omitted dimension must be NULL, not dropped"
        );
    }

    #[test]
    fn seeded_unsupported_type_dimension_is_skipped_without_desync() {
        // An existing-schema column with a type the attribute builders don't support must be
        // skipped entirely (field AND column), never inserted as a field with no column —
        // which would desync `fields`/`columns` and fail `RecordBatch::try_new`.
        let existing = Schema::new(vec![
            Field::new(VALUE_COLUMN_NAME, DataType::Float64, true),
            Field::new(TIME_UNIX_NANO_COLUMN_NAME, DataType::UInt64, true),
            Field::new(START_TIME_UNIX_NANO_COLUMN_NAME, DataType::UInt64, true),
            Field::new("region", DataType::Utf8, true),
            // Not one of the supported attribute builder types.
            Field::new("weird", DataType::Int32, true),
        ]);

        let data = Data::Gauge(opentelemetry_proto::tonic::metrics::v1::Gauge {
            data_points: vec![number_data_point(
                1.0,
                vec![string_attribute("region", "us")],
            )],
        });

        let (result, _) = metric_data_to_record_batch("svc_requests", &data, &[], Some(&existing));
        let batch = result.expect("batch must build even with an unsupported existing column");

        assert!(
            batch.schema().field_with_name("region").is_ok(),
            "supported dimension is still carried over"
        );
        assert!(
            batch.schema().field_with_name("weird").is_err(),
            "unsupported-type column must be skipped, not partially seeded"
        );
    }

    #[test]
    fn seeded_view_type_dimensions_are_materialized_not_dropped() {
        // Regression for the Cayenne field-count mismatch: accelerators store string/binary
        // dimensions in view/large layouts (Cayenne uses `Utf8View`). Matching only the exact
        // `Utf8`/`Binary` type skipped those columns, so a data point missing them produced a
        // narrower batch than the stored table (e.g. 17 expected vs 14 received) and the write
        // failed. Every stored dimension must be materialized, present-but-NULL when absent.
        let existing = Schema::new(vec![
            Field::new(VALUE_COLUMN_NAME, DataType::Float64, true),
            Field::new(TIME_UNIX_NANO_COLUMN_NAME, DataType::UInt64, true),
            Field::new(START_TIME_UNIX_NANO_COLUMN_NAME, DataType::UInt64, true),
            Field::new("region", DataType::Utf8View, true),
            Field::new("team", DataType::LargeUtf8, true),
            Field::new("payload", DataType::BinaryView, true),
        ]);

        // Data point carries none of the view-typed dimensions.
        let data = Data::Gauge(opentelemetry_proto::tonic::metrics::v1::Gauge {
            data_points: vec![number_data_point(1.0, vec![])],
        });

        let (result, _) =
            metric_data_to_record_batch("query_active_count", &data, &[], Some(&existing));
        let batch = result.expect("batch must build with the view-typed dimensions materialized");

        assert_eq!(
            batch.num_columns(),
            existing.fields().len(),
            "every stored column must be present so the write matches the table width"
        );

        // View/large string columns are materialized as canonical `Utf8` (matching how present
        // attributes are appended); `verify_schema` treats the utf8 family as equivalent.
        for name in ["region", "team"] {
            let field = batch
                .schema()
                .field_with_name(name)
                .unwrap_or_else(|_| panic!("{name} must be carried over"))
                .clone();
            assert_eq!(field.data_type(), &DataType::Utf8, "{name} seeded as Utf8");
            assert!(
                column(&batch, name).as_string::<i32>().is_null(0),
                "{name} omitted on the data point must be NULL"
            );
        }

        let payload_field = batch
            .schema()
            .field_with_name("payload")
            .expect("payload must be carried over")
            .clone();
        assert_eq!(payload_field.data_type(), &DataType::Binary);
    }

    /// A stored column keeps its type, and a later value of another type is converted into it
    /// whenever the conversion is exact. Without this every mismatch became NULL, losing a
    /// value the table could hold perfectly well.
    #[test]
    fn attribute_values_are_converted_into_the_stored_column_type() {
        // (stored column type, attribute value, expected text of the stored value)
        let cases: Vec<(DataType, any_value::Value, &str)> = vec![
            // An integer fits a double column exactly while it stays under 2^53.
            (
                DataType::Float64,
                any_value::Value::IntValue(1 << 52),
                "4503599627370496.0",
            ),
            // A whole double fits an integer column.
            (DataType::Int64, any_value::Value::DoubleValue(42.0), "42"),
            // A whole, non-negative double fits an unsigned column.
            (DataType::UInt64, any_value::Value::DoubleValue(7.0), "7"),
            // A non-negative integer fits an unsigned column.
            (DataType::UInt64, any_value::Value::IntValue(7), "7"),
            // Every scalar has a faithful text form.
            (DataType::Utf8, any_value::Value::IntValue(-5), "-5"),
            (DataType::Utf8, any_value::Value::DoubleValue(1.5), "1.5"),
            (DataType::Utf8, any_value::Value::BoolValue(true), "true"),
            // Text that parses exactly fits a numeric column.
            (
                DataType::Int64,
                any_value::Value::StringValue("123".to_string()),
                "123",
            ),
            (
                DataType::Float64,
                any_value::Value::StringValue("1.5".to_string()),
                "1.5",
            ),
            (
                DataType::UInt64,
                any_value::Value::StringValue("9".to_string()),
                "9",
            ),
            (
                DataType::Boolean,
                any_value::Value::StringValue("true".to_string()),
                "true",
            ),
        ];

        for (stored_type, value, expected) in cases {
            let existing = Schema::new(vec![
                Field::new(VALUE_COLUMN_NAME, DataType::Float64, true),
                Field::new(TIME_UNIX_NANO_COLUMN_NAME, DataType::UInt64, true),
                Field::new(START_TIME_UNIX_NANO_COLUMN_NAME, DataType::UInt64, true),
                Field::new("label", stored_type.clone(), true),
            ]);
            let data = Data::Gauge(Gauge {
                data_points: vec![number_data_point(
                    1.0,
                    vec![typed_attribute("label", value.clone())],
                )],
            });

            let (result, _) = metric_data_to_record_batch("svc", &data, &[], Some(&existing));
            let batch = result.expect("the batch must build");
            let column = column(&batch, "label");
            assert!(
                !column.is_null(0),
                "a {value:?} must be stored in a {stored_type} column, not dropped to NULL"
            );
            let stored = arrow::util::display::array_value_to_string(column, 0)
                .expect("the stored value must be printable");
            assert_eq!(
                stored, expected,
                "a {value:?} stored in a {stored_type} column"
            );
        }
    }

    /// Correctness comes first: a value the column cannot hold exactly is stored as NULL
    /// rather than rounded, truncated or reinterpreted into a number the client never sent.
    #[test]
    fn attribute_values_that_would_lose_information_are_stored_as_null() {
        let cases: Vec<(DataType, any_value::Value, &str)> = vec![
            // 2^53 + 1 is the first integer a double cannot represent.
            (
                DataType::Float64,
                any_value::Value::IntValue((1 << 53) + 1),
                "an integer a double would round",
            ),
            (
                DataType::Float64,
                any_value::Value::IntValue(i64::MAX),
                "the largest integer, which a double would round",
            ),
            // A fraction has no integer form.
            (
                DataType::Int64,
                any_value::Value::DoubleValue(1.5),
                "a fractional double",
            ),
            (
                DataType::UInt64,
                any_value::Value::DoubleValue(1.5),
                "a fractional double",
            ),
            // A negative number has no unsigned form.
            (
                DataType::UInt64,
                any_value::Value::IntValue(-1),
                "a negative integer",
            ),
            (
                DataType::UInt64,
                any_value::Value::DoubleValue(-2.0),
                "a negative double",
            ),
            // A double beyond the integer range.
            (
                DataType::Int64,
                any_value::Value::DoubleValue(1e30),
                "a double past the integer range",
            ),
            // Text that is not a number.
            (
                DataType::Int64,
                any_value::Value::StringValue("not a number".to_string()),
                "text that is not an integer",
            ),
            (
                DataType::Float64,
                any_value::Value::StringValue(String::new()),
                "empty text",
            ),
            // Only the two words a boolean prints as are accepted.
            (
                DataType::Boolean,
                any_value::Value::StringValue("yes".to_string()),
                "text that is not a boolean",
            ),
            (
                DataType::Boolean,
                any_value::Value::IntValue(1),
                "an integer, which is not a boolean",
            ),
            // Bytes are not required to be text.
            (
                DataType::Utf8,
                any_value::Value::BytesValue(vec![0xff, 0xfe]),
                "bytes, which have no text form",
            ),
            // Non-finite doubles have no numeric text form.
            (
                DataType::Utf8,
                any_value::Value::DoubleValue(f64::NAN),
                "NaN, which has no numeric text form",
            ),
        ];

        for (stored_type, value, description) in cases {
            let existing = Schema::new(vec![
                Field::new(VALUE_COLUMN_NAME, DataType::Float64, true),
                Field::new(TIME_UNIX_NANO_COLUMN_NAME, DataType::UInt64, true),
                Field::new(START_TIME_UNIX_NANO_COLUMN_NAME, DataType::UInt64, true),
                Field::new("label", stored_type.clone(), true),
            ]);
            let data = Data::Gauge(Gauge {
                data_points: vec![number_data_point(
                    1.0,
                    vec![typed_attribute("label", value)],
                )],
            });

            let (result, _) = metric_data_to_record_batch("svc", &data, &[], Some(&existing));
            let batch = result.expect("the batch must still build");
            assert!(
                column(&batch, "label").is_null(0),
                "{description} must be NULL in a {stored_type} column, never converted"
            );
        }
    }

    /// The column takes its type from the first data point that carries the attribute, and
    /// later data points of other types are converted into it. Every column must still end up
    /// the same length, or the batch fails to build.
    #[test]
    fn mixed_attribute_types_within_one_batch_keep_every_column_aligned() {
        let data = Data::Gauge(Gauge {
            data_points: vec![
                // Establishes `port` as an integer column and `zone` as a string column.
                number_data_point(
                    1.0,
                    vec![int_attribute("port", 8080), string_attribute("zone", "a")],
                ),
                // The same keys arrive as the other type; both convert exactly.
                number_data_point(
                    2.0,
                    vec![string_attribute("port", "9090"), int_attribute("zone", 42)],
                ),
                // A fractional double cannot be an integer port, so that one is NULL.
                number_data_point(
                    3.0,
                    vec![double_attribute("port", 1.5), bool_attribute("zone", true)],
                ),
            ],
        });

        let (result, count) = metric_data_to_record_batch("svc", &data, &[], None);
        assert_eq!(count, 3);
        let batch = result.expect("the batch must build");
        assert_eq!(batch.num_rows(), 3);
        for column in batch.columns() {
            assert_eq!(column.len(), 3, "every column must have the same length");
        }

        let ports = column(&batch, "port")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("port keeps the integer type of its first value");
        assert_eq!(ports.value(0), 8080);
        assert_eq!(ports.value(1), 9090, "text that parses exactly is stored");
        assert!(ports.is_null(2), "a fractional double cannot be an integer");

        let zones = column(&batch, "zone").as_string::<i32>();
        assert_eq!(zones.value(0), "a");
        assert_eq!(zones.value(1), "42", "an integer has a faithful text form");
        assert_eq!(zones.value(2), "true", "so does a boolean");
    }

    /// A metric reported as an integer on one data point and a double on the next keeps both,
    /// as long as each value fits the column exactly. Previously the whole data point was
    /// dropped.
    #[test]
    fn metric_values_are_converted_between_integer_and_double() {
        let data = Data::Gauge(Gauge {
            data_points: vec![
                // Establishes an integer `value` column.
                NumberDataPoint {
                    value: Some(Value::AsInt(5)),
                    ..number_data_point(0.0, vec![])
                },
                // A whole double fits it.
                NumberDataPoint {
                    value: Some(Value::AsDouble(6.0)),
                    ..number_data_point(0.0, vec![])
                },
            ],
        });

        let (result, count) = metric_data_to_record_batch("svc", &data, &[], None);
        assert_eq!(count, 2);
        let batch = result.expect("the batch must build");
        assert_eq!(batch.num_rows(), 2, "both data points must be kept");
        let values = column(&batch, VALUE_COLUMN_NAME)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value keeps the integer type of its first data point");
        assert_eq!(values.value(0), 5);
        assert_eq!(values.value(1), 6);
    }

    /// A metric value that cannot be stored exactly is still rejected, so the table never
    /// reports a number the client did not send.
    #[test]
    fn metric_values_that_would_lose_information_are_rejected() {
        let data = Data::Gauge(Gauge {
            data_points: vec![
                NumberDataPoint {
                    value: Some(Value::AsInt(5)),
                    ..number_data_point(0.0, vec![])
                },
                // 1.5 has no integer form, so this data point is dropped from the batch.
                NumberDataPoint {
                    value: Some(Value::AsDouble(1.5)),
                    ..number_data_point(0.0, vec![])
                },
            ],
        });

        let (result, count) = metric_data_to_record_batch("svc", &data, &[], None);
        assert_eq!(count, 2, "both data points are counted");
        let batch = result.expect("the batch must build");
        assert_eq!(
            batch.num_rows(),
            1,
            "the data point that cannot be stored exactly is left out, and the export reports \
             it as rejected"
        );
    }

    /// The stored `value` column type wins over the incoming one, so a double metric written
    /// to an integer column is converted rather than dropped.
    #[test]
    fn metric_values_are_converted_into_the_stored_value_column_type() {
        let existing = Schema::new(vec![
            Field::new(VALUE_COLUMN_NAME, DataType::Int64, true),
            Field::new(TIME_UNIX_NANO_COLUMN_NAME, DataType::UInt64, true),
            Field::new(START_TIME_UNIX_NANO_COLUMN_NAME, DataType::UInt64, true),
        ]);
        let data = Data::Gauge(Gauge {
            data_points: vec![number_data_point(12.0, vec![])],
        });

        let (result, _) = metric_data_to_record_batch("svc", &data, &[], Some(&existing));
        let batch = result.expect("the batch must build");
        let values = column(&batch, VALUE_COLUMN_NAME)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("the stored integer type is kept");
        assert_eq!(values.value(0), 12);
    }

    /// The exact-conversion helpers are the whole correctness argument for coercion, so the
    /// boundaries are pinned directly.
    #[test]
    fn exact_conversions_accept_only_values_that_round_trip() {
        // The largest integer a double holds exactly, and the first one it does not.
        assert_eq!(exact_f64_from_i64(1 << 53), Some(9_007_199_254_740_992.0));
        assert_eq!(exact_f64_from_i64((1 << 53) + 1), None);
        assert_eq!(exact_f64_from_i64(i64::MAX), None);
        // i64::MIN is a power of two, so a double holds it exactly.
        assert_eq!(
            exact_f64_from_i64(i64::MIN),
            Some(-9_223_372_036_854_775_808.0)
        );

        assert_eq!(exact_i64_from_f64(42.0), Some(42));
        assert_eq!(exact_i64_from_f64(-42.0), Some(-42));
        assert_eq!(exact_i64_from_f64(42.5), None);
        assert_eq!(exact_i64_from_f64(1e30), None);
        assert_eq!(exact_i64_from_f64(f64::NAN), None);
        assert_eq!(exact_i64_from_f64(f64::INFINITY), None);

        assert_eq!(exact_u64_from_f64(42.0), Some(42));
        assert_eq!(exact_u64_from_f64(-1.0), None);
        assert_eq!(exact_u64_from_f64(0.5), None);
        assert_eq!(exact_u64_from_f64(f64::NEG_INFINITY), None);
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
        let (first_result, _) = metric_data_to_record_batch("svc", &first, &[], None);
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
        let (second_result, _) =
            metric_data_to_record_batch("svc", &first, &[], Some(&first_schema));
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
        let (third_result, _) =
            metric_data_to_record_batch("svc", &third, &[], Some(&first_schema));
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

        let (result, _) = metric_data_to_record_batch("latency", &data, &[], None);
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

        let (result, _) = metric_data_to_record_batch("svc", &data, &[], None);
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

    /// A stored column is seeded so it keeps its position and gets a null backfill, which needs
    /// both a builder to seed and a null to append. The histogram path writes `count` as
    /// `UInt64` and the two bucket arrays as lists, and those are dimensions — not value
    /// columns — for a data point of another shape, so seeding must cover them (fixes #12117).
    #[test]
    fn histogram_columns_seed_as_dimensions_on_a_later_gauge_export() {
        let histogram = Data::Histogram(Histogram {
            data_points: vec![histogram_data_point(
                5,
                Some(12.5),
                Some(0.5),
                Some(9.0),
                vec![1, 2, 2],
                vec![1.0, 5.0],
                vec![string_attribute("host", "a")],
            )],
            aggregation_temporality: 0,
        });
        let stored = metric_data_to_record_batch("latency", &histogram, &[], None)
            .0
            .expect("histogram batch builds")
            .schema();

        let gauge = Data::Gauge(opentelemetry_proto::tonic::metrics::v1::Gauge {
            data_points: vec![number_data_point(1.0, vec![string_attribute("host", "b")])],
        });
        let (result, _) = metric_data_to_record_batch("latency", &gauge, &[], Some(&stored));
        let batch = result.expect("gauge batch builds against the stored histogram schema");

        assert_eq!(
            field_names(&batch.schema()),
            vec![
                VALUE_COLUMN_NAME,
                TIME_UNIX_NANO_COLUMN_NAME,
                START_TIME_UNIX_NANO_COLUMN_NAME,
                COUNT_COLUMN_NAME,
                SUM_COLUMN_NAME,
                MIN_COLUMN_NAME,
                MAX_COLUMN_NAME,
                BUCKET_COUNTS_COLUMN_NAME,
                EXPLICIT_BOUNDS_COLUMN_NAME,
                "host",
            ],
            "every stored dimension must keep its position"
        );
        assert_eq!(batch.num_rows(), 1);
        for name in [
            COUNT_COLUMN_NAME,
            SUM_COLUMN_NAME,
            BUCKET_COUNTS_COLUMN_NAME,
            EXPLICIT_BOUNDS_COLUMN_NAME,
        ] {
            assert!(
                column(&batch, name).is_null(0),
                "{name} is not carried by this data point shape, so it must be null"
            );
        }
        assert_eq!(column(&batch, "host").as_string::<i32>().value(0), "b");
    }

    /// A metric whose type has no batch builder still carries data points, and dropping them
    /// silently tells the client an export it lost data from succeeded. The count must be the
    /// real one so `export` can report the points as rejected (regression test for #12188).
    #[test]
    fn unsupported_metric_type_counts_its_dropped_data_points() {
        let summary = Data::Summary(Summary {
            data_points: vec![SummaryDataPoint::default(); 3],
        });
        let (result, count) = metric_data_to_record_batch("gc_pause_seconds", &summary, &[], None);
        assert_eq!(count, 3, "a Summary's data points must still be counted");
        let error = result.expect_err("Summary has no batch builder");
        assert!(matches!(error, Error::UnsupportedMetricDataType { .. }));
        let message = error.to_string();
        assert!(
            message.contains("gc_pause_seconds") && message.contains("Summary"),
            "error must name the metric and its type, got: {message}"
        );

        let exponential = Data::ExponentialHistogram(ExponentialHistogram {
            data_points: vec![ExponentialHistogramDataPoint::default(); 2],
            aggregation_temporality: 0,
        });
        let (result, count) = metric_data_to_record_batch("latency", &exponential, &[], None);
        assert_eq!(
            count, 2,
            "an ExponentialHistogram's data points must still be counted"
        );
        assert!(matches!(
            result,
            Err(Error::UnsupportedMetricDataType { .. })
        ));
    }

    #[test]
    fn duplicate_column_names_reports_each_repeated_name_once() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Int64, true),
            Field::new("a", DataType::Int64, true),
            Field::new("a", DataType::Float64, true),
        ]);
        assert_eq!(duplicate_column_names(&schema), vec!["a"]);

        let well_formed = Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Int64, true),
        ]);
        assert!(duplicate_column_names(&well_formed).is_empty());
    }

    /// A metric table can hold two columns of the same name, because Arrow permits duplicate
    /// field names. No batch this module builds can ever match such a table, so the export must
    /// fail with an error naming the metric, the duplicated column and the fix — not be dropped
    /// by a schema mismatch the operator cannot act on (regression test for #12095).
    #[test]
    fn metric_table_with_duplicate_columns_fails_with_an_actionable_error() {
        let existing = Schema::new(vec![
            Field::new(COUNT_COLUMN_NAME, DataType::UInt64, true),
            Field::new(SUM_COLUMN_NAME, DataType::Float64, true),
            Field::new(MIN_COLUMN_NAME, DataType::Float64, true),
            Field::new(MAX_COLUMN_NAME, DataType::Float64, true),
            Field::new(TIME_UNIX_NANO_COLUMN_NAME, DataType::UInt64, true),
            Field::new(START_TIME_UNIX_NANO_COLUMN_NAME, DataType::UInt64, true),
            // A second column named `count`, alongside the histogram value column.
            Field::new(COUNT_COLUMN_NAME, DataType::Utf8, true),
            Field::new("host", DataType::Utf8, true),
        ]);

        let data = Data::Histogram(Histogram {
            data_points: vec![
                histogram_data_point(
                    1,
                    Some(1.0),
                    None,
                    None,
                    vec![1],
                    vec![],
                    vec![string_attribute("host", "a")],
                ),
                histogram_data_point(
                    2,
                    Some(2.0),
                    None,
                    None,
                    vec![2],
                    vec![],
                    vec![string_attribute("host", "b")],
                ),
            ],
            aggregation_temporality: 0,
        });

        let (result, count) = metric_data_to_record_batch("latency", &data, &[], Some(&existing));
        assert_eq!(
            count, 2,
            "the data points this table cannot accept must still be counted as rejected"
        );
        let error = result.expect_err("a table with duplicate columns cannot accept an export");
        assert!(matches!(
            error,
            Error::MetricTableHasDuplicateColumns { .. }
        ));
        let message = error.to_string();
        assert!(
            message.contains("latency") && message.contains(COUNT_COLUMN_NAME),
            "error must name the metric and the duplicated column, got: {message}"
        );
        assert!(
            message.contains("Drop and recreate"),
            "error must tell the operator how to recover, got: {message}"
        );
    }

    /// The duplicate-column check must not reject a table that merely reuses a name across
    /// value and dimension *roles* — only a genuinely repeated column name is a problem.
    #[test]
    fn well_formed_stored_schema_is_unaffected_by_the_duplicate_check() {
        let existing = Schema::new(vec![
            Field::new(VALUE_COLUMN_NAME, DataType::Float64, true),
            Field::new(TIME_UNIX_NANO_COLUMN_NAME, DataType::UInt64, true),
            Field::new(START_TIME_UNIX_NANO_COLUMN_NAME, DataType::UInt64, true),
            Field::new(COUNT_COLUMN_NAME, DataType::Utf8, true),
        ]);

        let data = Data::Gauge(opentelemetry_proto::tonic::metrics::v1::Gauge {
            data_points: vec![number_data_point(
                1.0,
                vec![string_attribute(COUNT_COLUMN_NAME, "5")],
            )],
        });

        let (result, _) = metric_data_to_record_batch("svc", &data, &[], Some(&existing));
        let batch = result.expect("a well-formed stored schema must still build");
        assert_eq!(
            column(&batch, COUNT_COLUMN_NAME)
                .as_string::<i32>()
                .value(0),
            "5"
        );
    }

    /// A [`QueryEngine`] that accepts every write and reports no stored table, so
    /// [`Service::export`] can be driven end to end without a runtime. Only the methods the
    /// export path calls do anything; the rest are unreachable from it.
    struct WriteRecordingQueryEngine {
        session: Arc<SessionContext>,
        rows_written: AtomicU64,
        batches_written: Mutex<Vec<RecordBatch>>,
    }

    // `QueryEngine` requires `Debug`, and `SessionContext` does not implement it.
    impl std::fmt::Debug for WriteRecordingQueryEngine {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("WriteRecordingQueryEngine")
                .field("rows_written", &self.rows_written())
                .finish_non_exhaustive()
        }
    }

    impl WriteRecordingQueryEngine {
        fn new() -> Self {
            Self {
                session: Arc::new(SessionContext::new()),
                rows_written: AtomicU64::new(0),
                batches_written: Mutex::new(Vec::new()),
            }
        }

        fn rows_written(&self) -> u64 {
            self.rows_written.load(Ordering::SeqCst)
        }

        fn batches_written(&self) -> Vec<RecordBatch> {
            self.batches_written.lock().clone()
        }
    }

    #[async_trait]
    impl QueryEngine for WriteRecordingQueryEngine {
        fn session_context(&self) -> &Arc<SessionContext> {
            &self.session
        }

        async fn get_table(&self, _table_ref: &TableReference) -> Option<Arc<dyn TableProvider>> {
            None
        }

        fn get_table_sync(&self, _table_ref: &TableReference) -> Option<Arc<dyn TableProvider>> {
            None
        }

        fn table_exists(&self, _table_ref: &TableReference) -> bool {
            false
        }

        async fn get_arrow_schema(&self, table_ref: TableReference) -> QueryEngineResult<Schema> {
            // No stored schema: each batch is built from the exported data points alone.
            Err(QueryEngineError::GetSchema {
                table_ref: table_ref.to_string(),
                source: DataFusionError::Plan(format!("table {table_ref} is not registered")),
            })
        }

        fn get_user_table_names(&self) -> Vec<TableReference> {
            Vec::new()
        }

        fn get_public_table_names(&self) -> QueryEngineResult<Vec<String>> {
            Ok(Vec::new())
        }

        fn is_writable(&self, _table_ref: &TableReference) -> bool {
            true
        }

        fn is_path_catalog_writable(&self, _table_ref: &TableReference) -> bool {
            true
        }

        async fn execute_query(
            &self,
            _request: QueryRequest,
        ) -> QueryEngineResult<SendableRecordBatchStream> {
            unimplemented!("the OpenTelemetry export path does not run queries")
        }

        async fn execute_plan(
            &self,
            _plan: LogicalPlan,
        ) -> QueryEngineResult<SendableRecordBatchStream> {
            unimplemented!("the OpenTelemetry export path does not run plans")
        }

        async fn write_data(
            &self,
            _table_ref: &TableReference,
            _schema: Arc<Schema>,
            data: Vec<RecordBatch>,
            _update_type: UpdateType,
        ) -> QueryEngineResult<()> {
            let rows: u64 = data.iter().map(|batch| batch.num_rows() as u64).sum();
            self.rows_written.fetch_add(rows, Ordering::SeqCst);
            self.batches_written.lock().extend(data);
            Ok(())
        }
    }

    fn otlp_metric(name: &str, data: Option<Data>) -> OtlpMetric {
        OtlpMetric {
            name: name.to_string(),
            data,
            ..Default::default()
        }
    }

    fn otlp_request(metrics: Vec<OtlpMetric>) -> ExportMetricsServiceRequest {
        otlp_request_with_resource(metrics, vec![])
    }

    fn otlp_request_with_resource(
        metrics: Vec<OtlpMetric>,
        resource_attributes: Vec<KeyValue>,
    ) -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: resource_attributes,
                    ..Default::default()
                }),
                scope_metrics: vec![ScopeMetrics {
                    metrics,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn gauge(value: f64) -> Data {
        Data::Gauge(Gauge {
            data_points: vec![number_data_point(
                value,
                vec![string_attribute("region", "us")],
            )],
        })
    }

    fn summary(data_points: usize) -> Data {
        Data::Summary(Summary {
            data_points: vec![SummaryDataPoint::default(); data_points],
        })
    }

    /// An export mixing a supported metric with one whose type has no batch builder is a
    /// partially accepted export: the client must be told how many data points were dropped
    /// through `ExportMetricsPartialSuccess`, rather than being told the export succeeded
    /// (regression test for #12188).
    #[tokio::test]
    async fn mixed_export_reports_unsupported_metric_data_points_as_rejected() {
        let engine = Arc::new(WriteRecordingQueryEngine::new());
        let service = build_metrics_service(Arc::clone(&engine) as Arc<dyn QueryEngine>, None);

        let request = otlp_request(vec![
            otlp_metric("svc_requests", Some(gauge(1.0))),
            otlp_metric("gc_pause_seconds", Some(summary(3))),
        ]);

        let response = service
            .export(Request::new(request))
            .await
            .expect("an export with accepted data points must not fail")
            .into_inner();

        let partial = response
            .partial_success
            .expect("the dropped Summary data points must be reported as rejected");
        assert_eq!(
            partial.rejected_data_points, 3,
            "every data point of the unsupported metric must be counted"
        );
        assert_eq!(
            engine.rows_written(),
            1,
            "the supported metric's data point must still be written"
        );
    }

    /// An export whose data points were all rejected is a failed export, including when every
    /// metric in it has an unsupported type.
    #[tokio::test]
    async fn export_of_only_unsupported_metrics_fails() {
        let engine = Arc::new(WriteRecordingQueryEngine::new());
        let service = build_metrics_service(Arc::clone(&engine) as Arc<dyn QueryEngine>, None);

        let request = otlp_request(vec![otlp_metric("gc_pause_seconds", Some(summary(3)))]);

        let status = service
            .export(Request::new(request))
            .await
            .expect_err("an export that lost every data point must fail");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(engine.rows_written(), 0);
    }

    /// An export carrying no data points at all lost nothing, so it succeeded — it must not be
    /// reported as fully rejected.
    #[tokio::test]
    async fn export_without_data_points_succeeds() {
        let engine = Arc::new(WriteRecordingQueryEngine::new());
        let service = build_metrics_service(Arc::clone(&engine) as Arc<dyn QueryEngine>, None);

        let request = otlp_request(vec![
            otlp_metric("svc_requests", None),
            otlp_metric(
                "svc_latency",
                Some(Data::Gauge(Gauge {
                    data_points: vec![],
                })),
            ),
        ]);

        let response = service
            .export(Request::new(request))
            .await
            .expect("an export that rejected nothing must succeed")
            .into_inner();
        assert!(
            response.partial_success.is_none(),
            "nothing was rejected, so no partial success is reported"
        );
        assert_eq!(engine.rows_written(), 0);
    }
    /// Resource attributes identify the process that produced a measurement, so they must land
    /// as dimension columns on every data point of the export rather than being dropped.
    #[test]
    fn resource_attributes_become_columns_on_number_data_points() {
        let data = Data::Gauge(Gauge {
            data_points: vec![
                number_data_point(1.0, vec![string_attribute("region", "us")]),
                number_data_point(2.0, vec![]),
            ],
        });
        let resource_attrs = vec![
            string_attribute("service.name", "spiced"),
            string_attribute("service.instance.id", "instance-a"),
        ];

        let (result, _) = metric_data_to_record_batch("svc_requests", &data, &resource_attrs, None);
        let batch = result.expect("record batch should build");

        assert_eq!(
            column(&batch, "service.name").as_string::<i32>().value(0),
            "spiced"
        );
        assert_eq!(
            column(&batch, "service.instance.id")
                .as_string::<i32>()
                .value(1),
            "instance-a",
            "a data point with no attributes of its own still carries the resource attributes"
        );
        assert_eq!(
            column(&batch, "region").as_string::<i32>().value(0),
            "us",
            "the data point's own attributes are kept alongside the resource attributes"
        );
    }

    #[test]
    fn resource_attributes_become_columns_on_histogram_data_points() {
        let data = Data::Histogram(Histogram {
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
        let resource_attrs = vec![string_attribute("service.instance.id", "instance-a")];

        let (result, _) = metric_data_to_record_batch("latency", &data, &resource_attrs, None);
        let batch = result.expect("record batch should build");

        assert_eq!(
            column(&batch, "service.instance.id")
                .as_string::<i32>()
                .value(0),
            "instance-a"
        );
        assert_eq!(column(&batch, "host").as_string::<i32>().value(0), "a");
    }

    /// A data point's own attribute describes that measurement more specifically than the
    /// resource-level one of the same key, so it must not be overwritten by the merge.
    #[test]
    fn data_point_attribute_wins_over_resource_attribute_of_the_same_key() {
        let data = Data::Gauge(Gauge {
            data_points: vec![number_data_point(
                1.0,
                vec![string_attribute("region", "eu")],
            )],
        });
        let resource_attrs = vec![string_attribute("region", "us")];

        let (result, _) = metric_data_to_record_batch("svc_requests", &data, &resource_attrs, None);
        let batch = result.expect("record batch should build");

        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .filter(|field| field.name() == "region")
                .count(),
            1,
            "the colliding key must produce exactly one column"
        );
        assert_eq!(column(&batch, "region").as_string::<i32>().value(0), "eu");
    }

    /// A client can send the same attribute key twice on one data point. Appending both
    /// values would fail the batch and drop the whole export, so the first value wins.
    #[test]
    fn duplicate_attribute_keys_on_one_data_point_keep_the_first_value() {
        let data = Data::Gauge(Gauge {
            data_points: vec![
                number_data_point(
                    1.0,
                    vec![
                        string_attribute("region", "us"),
                        string_attribute("region", "eu"),
                        string_attribute("tenant", "acme"),
                    ],
                ),
                number_data_point(2.0, vec![string_attribute("region", "apac")]),
            ],
        });

        let (result, count) = metric_data_to_record_batch("svc_requests", &data, &[], None);
        assert_eq!(count, 2);
        let batch = result.expect("a duplicated attribute key must not fail the batch");

        assert_eq!(batch.num_rows(), 2);
        for column in batch.columns() {
            assert_eq!(column.len(), 2, "all columns must have equal length");
        }
        assert_eq!(
            field_names(&batch.schema())
                .iter()
                .filter(|name| **name == "region")
                .count(),
            1,
            "the duplicated key must produce exactly one column"
        );

        let region = column(&batch, "region").as_string::<i32>();
        assert_eq!(region.value(0), "us", "the first value wins");
        assert_eq!(region.value(1), "apac");
        let tenant = column(&batch, "tenant").as_string::<i32>();
        assert_eq!(tenant.value(0), "acme");
        assert!(tenant.is_null(1));
    }

    /// Duplicate keys can also arrive through the resource attributes, which are merged into
    /// every data point; they must collapse to one column the same way.
    #[test]
    fn duplicate_resource_attribute_keys_do_not_desync_the_batch() {
        let data = Data::Gauge(Gauge {
            data_points: vec![
                number_data_point(1.0, vec![string_attribute("host", "a")]),
                number_data_point(2.0, vec![]),
            ],
        });
        let resource_attrs = vec![
            string_attribute("service.name", "spiced"),
            string_attribute("service.name", "other"),
        ];

        let (result, _) = metric_data_to_record_batch("svc_requests", &data, &resource_attrs, None);
        let batch = result.expect("duplicated resource attribute keys must not fail the batch");

        assert_eq!(batch.num_rows(), 2);
        for column in batch.columns() {
            assert_eq!(column.len(), 2, "all columns must have equal length");
        }
        let service = column(&batch, "service.name").as_string::<i32>();
        assert_eq!(service.value(0), "spiced", "the first value wins");
        assert_eq!(service.value(1), "spiced");
    }

    /// A resource attribute named like one of the metric's value columns goes through the
    /// same collision handling as a data-point attribute: dropped, never a second column.
    #[test]
    fn resource_attribute_colliding_with_a_value_column_is_dropped() {
        let data = Data::Gauge(Gauge {
            data_points: vec![number_data_point(1.5, vec![string_attribute("host", "a")])],
        });
        let resource_attrs = vec![string_attribute(VALUE_COLUMN_NAME, "not a value")];

        let (result, _) = metric_data_to_record_batch("svc_requests", &data, &resource_attrs, None);
        let batch = result.expect("record batch should build");

        assert_eq!(
            field_names(&batch.schema())
                .iter()
                .filter(|name| **name == VALUE_COLUMN_NAME)
                .count(),
            1,
            "value must appear exactly once"
        );
        let values = column(&batch, VALUE_COLUMN_NAME)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("value keeps the metric's own type");
        assert!((values.value(0) - 1.5).abs() < f64::EPSILON);
        assert_eq!(column(&batch, "host").as_string::<i32>().value(0), "a");
    }

    /// Each data point is handled on its own: one point overriding a resource attribute must
    /// not change the value the other points in the batch get.
    #[test]
    fn resource_attribute_is_overridden_only_on_the_data_point_carrying_the_key() {
        let data = Data::Gauge(Gauge {
            data_points: vec![
                number_data_point(1.0, vec![string_attribute("region", "eu")]),
                number_data_point(2.0, vec![string_attribute("host", "a")]),
            ],
        });
        let resource_attrs = vec![string_attribute("region", "us")];

        let (result, _) = metric_data_to_record_batch("svc_requests", &data, &resource_attrs, None);
        let batch = result.expect("record batch should build");

        let regions = column(&batch, "region").as_string::<i32>();
        assert_eq!(regions.value(0), "eu");
        assert_eq!(regions.value(1), "us");
    }

    /// The export handler must pass each group's resource attributes through to the batch it
    /// writes, or a column like `service.instance.id` never reaches the table.
    #[tokio::test]
    async fn export_writes_resource_attributes_as_columns() {
        let engine = Arc::new(WriteRecordingQueryEngine::new());
        let service = build_metrics_service(Arc::clone(&engine) as Arc<dyn QueryEngine>, None);

        let request = otlp_request_with_resource(
            vec![otlp_metric("svc_requests", Some(gauge(1.0)))],
            vec![string_attribute("service.instance.id", "instance-a")],
        );

        service
            .export(Request::new(request))
            .await
            .expect("the export must succeed");

        let batches = engine.batches_written();
        let batch = batches.first().expect("one batch must have been written");
        assert_eq!(
            column(batch, "service.instance.id")
                .as_string::<i32>()
                .value(0),
            "instance-a"
        );
        assert_eq!(column(batch, "region").as_string::<i32>().value(0), "us");
    }

    /// A data point the batch cannot hold is skipped while building it. The client must be
    /// told it was rejected, not that the whole export was written.
    #[tokio::test]
    async fn data_points_skipped_during_the_batch_build_are_reported_as_rejected() {
        let engine = Arc::new(WriteRecordingQueryEngine::new());
        let service = build_metrics_service(Arc::clone(&engine) as Arc<dyn QueryEngine>, None);

        // The first data point makes the column a double. The second one's integer is past
        // the 53 bits a double holds exactly, so storing it would round it and it is skipped.
        // A smaller integer would convert exactly and be kept.
        let data = Data::Gauge(Gauge {
            data_points: vec![
                number_data_point(1.0, vec![string_attribute("region", "us")]),
                NumberDataPoint {
                    value: Some(Value::AsInt((1 << 53) + 1)),
                    ..number_data_point(0.0, vec![string_attribute("region", "eu")])
                },
            ],
        });
        let request = otlp_request(vec![otlp_metric("svc_requests", Some(data))]);

        let response = service
            .export(Request::new(request))
            .await
            .expect("an export with an accepted data point must not fail")
            .into_inner();

        let partial = response
            .partial_success
            .expect("the skipped data point must be reported as rejected");
        assert_eq!(partial.rejected_data_points, 1);
        assert_eq!(
            engine.rows_written(),
            1,
            "the representable data point must still be written"
        );
    }

    /// How a simulated write should fail (see [`SchemaEvolvingQueryEngine`]).
    enum WriteFailureMode {
        /// The schema check refused the batch, as it does when another export added a column
        /// while this one was in flight.
        SchemaMismatch,
        /// Any other write failure (no retry must happen for these).
        Generic,
    }

    /// The error `write_data` returns when the schema check refuses a batch, in the exact
    /// shape it reaches this module in.
    fn schema_mismatch_write_error(table_ref: &TableReference) -> QueryEngineError {
        let expected = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        let actual = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]);
        let source = arrow_tools::schema::verify_schema(expected.fields(), actual.fields())
            .expect_err("the two schemas differ");
        QueryEngineError::WriteData {
            table_ref: table_ref.to_string(),
            source: DataFusionError::External(Box::new(crate::datafusion::Error::SchemaMismatch {
                source,
            })),
        }
    }

    /// A [`QueryEngine`] that acts like a table being changed by other exports. Each schema
    /// read returns the next entry of `schemas`, repeating the last one, and the first
    /// `failing_writes` writes fail as `failure_mode` says.
    struct SchemaEvolvingQueryEngine {
        session: Arc<SessionContext>,
        schemas: Vec<Schema>,
        schema_calls: AtomicU64,
        write_calls: AtomicU64,
        failing_writes: u64,
        failure_mode: WriteFailureMode,
        batches_written: Mutex<Vec<RecordBatch>>,
    }

    impl std::fmt::Debug for SchemaEvolvingQueryEngine {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("SchemaEvolvingQueryEngine")
                .field("write_calls", &self.write_calls.load(Ordering::SeqCst))
                .finish_non_exhaustive()
        }
    }

    impl SchemaEvolvingQueryEngine {
        fn new(schemas: Vec<Schema>, failing_writes: u64, failure_mode: WriteFailureMode) -> Self {
            Self {
                session: Arc::new(SessionContext::new()),
                schemas,
                schema_calls: AtomicU64::new(0),
                write_calls: AtomicU64::new(0),
                failing_writes,
                failure_mode,
                batches_written: Mutex::new(Vec::new()),
            }
        }

        fn write_calls(&self) -> u64 {
            self.write_calls.load(Ordering::SeqCst)
        }

        fn batches_written(&self) -> Vec<RecordBatch> {
            self.batches_written.lock().clone()
        }
    }

    #[async_trait]
    impl QueryEngine for SchemaEvolvingQueryEngine {
        fn session_context(&self) -> &Arc<SessionContext> {
            &self.session
        }

        async fn get_table(&self, _table_ref: &TableReference) -> Option<Arc<dyn TableProvider>> {
            None
        }

        fn get_table_sync(&self, _table_ref: &TableReference) -> Option<Arc<dyn TableProvider>> {
            None
        }

        fn table_exists(&self, _table_ref: &TableReference) -> bool {
            true
        }

        async fn get_arrow_schema(&self, _table_ref: TableReference) -> QueryEngineResult<Schema> {
            let call = usize::try_from(self.schema_calls.fetch_add(1, Ordering::SeqCst))
                .expect("test schema-call counts fit in usize");
            let index = call.min(self.schemas.len().saturating_sub(1));
            Ok(self.schemas[index].clone())
        }

        fn get_user_table_names(&self) -> Vec<TableReference> {
            Vec::new()
        }

        fn get_public_table_names(&self) -> QueryEngineResult<Vec<String>> {
            Ok(Vec::new())
        }

        fn is_writable(&self, _table_ref: &TableReference) -> bool {
            true
        }

        fn is_path_catalog_writable(&self, _table_ref: &TableReference) -> bool {
            true
        }

        async fn execute_query(
            &self,
            _request: QueryRequest,
        ) -> QueryEngineResult<SendableRecordBatchStream> {
            unimplemented!("the OpenTelemetry export path does not run queries")
        }

        async fn execute_plan(
            &self,
            _plan: LogicalPlan,
        ) -> QueryEngineResult<SendableRecordBatchStream> {
            unimplemented!("the OpenTelemetry export path does not run plans")
        }

        async fn write_data(
            &self,
            table_ref: &TableReference,
            _schema: Arc<Schema>,
            data: Vec<RecordBatch>,
            _update_type: UpdateType,
        ) -> QueryEngineResult<()> {
            let call = self.write_calls.fetch_add(1, Ordering::SeqCst);
            if call < self.failing_writes {
                return Err(match self.failure_mode {
                    WriteFailureMode::SchemaMismatch => schema_mismatch_write_error(table_ref),
                    WriteFailureMode::Generic => QueryEngineError::WriteData {
                        table_ref: table_ref.to_string(),
                        source: DataFusionError::Plan("write failed".to_string()),
                    },
                });
            }
            self.batches_written.lock().extend(data);
            Ok(())
        }
    }

    fn number_schema_with_dimensions(dimensions: &[&str]) -> Schema {
        let mut fields = vec![
            Field::new(VALUE_COLUMN_NAME, DataType::Float64, true),
            Field::new(TIME_UNIX_NANO_COLUMN_NAME, DataType::UInt64, true),
            Field::new(START_TIME_UNIX_NANO_COLUMN_NAME, DataType::UInt64, true),
        ];
        fields.extend(
            dimensions
                .iter()
                .map(|name| Field::new(*name, DataType::Utf8, true)),
        );
        Schema::new(fields)
    }

    /// A batch refused by the schema check means another export added a column while this
    /// one was in flight. Nothing was inserted, so the export rebuilds against the current
    /// schema and retries, rather than dropping its data points.
    #[tokio::test]
    async fn write_rejected_by_a_concurrent_evolution_is_retried_and_lands() {
        let engine = Arc::new(SchemaEvolvingQueryEngine::new(
            vec![
                number_schema_with_dimensions(&["region"]),
                number_schema_with_dimensions(&["region", "tier"]),
            ],
            1,
            WriteFailureMode::SchemaMismatch,
        ));
        let service = build_metrics_service(Arc::clone(&engine) as Arc<dyn QueryEngine>, None);

        let request = otlp_request(vec![otlp_metric("svc_requests", Some(gauge(1.0)))]);
        let response = service
            .export(Request::new(request))
            .await
            .expect("the retried export must succeed")
            .into_inner();

        assert!(
            response.partial_success.is_none(),
            "nothing must be rejected once the retry lands"
        );
        assert_eq!(engine.write_calls(), 2, "one failed write plus one retry");
        let batches = engine.batches_written();
        let batch = batches.first().expect("the retry must write one batch");
        assert!(
            batch.schema().field_with_name("tier").is_ok(),
            "the retried batch must be rebuilt against the live (evolved) schema"
        );
        assert!(
            column(batch, "tier").as_string::<i32>().is_null(0),
            "the dimension this export does not carry must be NULL"
        );
        assert_eq!(column(batch, "region").as_string::<i32>().value(0), "us");
    }

    /// Several exports can each add a column while this one is in flight, so one retry is
    /// not enough. Each retry rebuilds against the current schema until the write lands.
    #[tokio::test]
    async fn writes_racing_repeated_evolutions_retry_until_they_land() {
        let engine = Arc::new(SchemaEvolvingQueryEngine::new(
            vec![
                number_schema_with_dimensions(&["region"]),
                number_schema_with_dimensions(&["region", "tier"]),
                number_schema_with_dimensions(&["region", "tier", "tenant"]),
            ],
            2,
            WriteFailureMode::SchemaMismatch,
        ));
        let service = build_metrics_service(Arc::clone(&engine) as Arc<dyn QueryEngine>, None);

        let request = otlp_request(vec![otlp_metric("svc_requests", Some(gauge(1.0)))]);
        let response = service
            .export(Request::new(request))
            .await
            .expect("the export must land once the schema settles")
            .into_inner();

        assert!(
            response.partial_success.is_none(),
            "nothing must be rejected once the retries land"
        );
        assert_eq!(
            engine.write_calls(),
            3,
            "two mismatched writes, then the landing one"
        );
        let batches = engine.batches_written();
        let batch = batches
            .first()
            .expect("the final retry must write one batch");
        for dimension in ["tier", "tenant"] {
            assert!(
                batch.schema().field_with_name(dimension).is_ok(),
                "{dimension} must be present after rebuilding against the final schema"
            );
        }
    }

    /// Once the schema stops changing, a rebuild would produce the same rejected batch, so
    /// the export gives up rather than spinning.
    #[tokio::test]
    async fn write_schema_mismatch_gives_up_when_the_schema_stops_changing() {
        let engine = Arc::new(SchemaEvolvingQueryEngine::new(
            vec![number_schema_with_dimensions(&["region"])],
            u64::MAX,
            WriteFailureMode::SchemaMismatch,
        ));
        let service = build_metrics_service(Arc::clone(&engine) as Arc<dyn QueryEngine>, None);

        let request = otlp_request(vec![otlp_metric("svc_requests", Some(gauge(1.0)))]);
        let status = service
            .export(Request::new(request))
            .await
            .expect_err("an export that lost every data point must fail");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            engine.write_calls(),
            2,
            "one retry proves the schema is unchanged, then the export gives up"
        );
    }

    /// A table whose schema changes on every attempt must still stop retrying, at the cap.
    #[tokio::test]
    async fn runaway_schema_churn_is_bounded_by_the_attempt_cap() {
        let dimensions = ["d1", "d2", "d3", "d4", "d5"];
        let mut schemas = vec![number_schema_with_dimensions(&["region"])];
        for grown in 1..=dimensions.len() {
            let mut all = vec!["region"];
            all.extend(&dimensions[..grown]);
            schemas.push(number_schema_with_dimensions(&all));
        }
        let engine = Arc::new(SchemaEvolvingQueryEngine::new(
            schemas,
            u64::MAX,
            WriteFailureMode::SchemaMismatch,
        ));
        let service = build_metrics_service(Arc::clone(&engine) as Arc<dyn QueryEngine>, None);

        let request = otlp_request(vec![otlp_metric("svc_requests", Some(gauge(1.0)))]);
        let status = service
            .export(Request::new(request))
            .await
            .expect_err("an export that lost every data point must fail");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            usize::try_from(engine.write_calls()).expect("write counts fit in usize"),
            MAX_METRIC_WRITE_ATTEMPTS,
            "a schema that changes on every attempt is bounded by the cap"
        );
    }

    /// Only the schema check is safe to retry, because it rejects before inserting anything.
    /// Any other failure may have written rows already, so retrying could duplicate them.
    #[tokio::test]
    async fn non_schema_write_failures_are_not_retried() {
        let engine = Arc::new(SchemaEvolvingQueryEngine::new(
            vec![number_schema_with_dimensions(&["region"])],
            u64::MAX,
            WriteFailureMode::Generic,
        ));
        let service = build_metrics_service(Arc::clone(&engine) as Arc<dyn QueryEngine>, None);

        let request = otlp_request(vec![otlp_metric("svc_requests", Some(gauge(1.0)))]);
        let status = service
            .export(Request::new(request))
            .await
            .expect_err("an export that lost every data point must fail");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            engine.write_calls(),
            1,
            "a non-schema write failure must not be retried"
        );
    }
}
