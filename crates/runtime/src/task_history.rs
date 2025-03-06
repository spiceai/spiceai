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

use crate::accelerated_table::refresh::Refresh;
use crate::datafusion::DataFusion;
use crate::dataupdate::{DataUpdate, UpdateType};
use crate::internal_table::create_internal_accelerated_table;
use crate::status;
use crate::{component::dataset::acceleration::Acceleration, datafusion::SPICE_RUNTIME_SCHEMA};
use crate::{component::dataset::TimeFormat, secrets::Secrets};
use arrow::array::{ArrayBuilder, MapBuilder, RecordBatch, StringArray, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow_schema::ArrowError;
use data_components::arrow::struct_builder::StructBuilder;
use datafusion::sql::TableReference;
use futures::TryStreamExt;
use snafu::prelude::*;
use snafu::{ResultExt, Snafu};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;

use crate::accelerated_table::{AcceleratedTable, Retention};

pub mod otel_exporter;

pub const DEFAULT_TASK_HISTORY_TABLE: &str = "task_history";
pub const DEFAULT_TASK_HISTORY_RETENTION_PERIOD_SECS: u64 = 8 * 60 * 60; // 8 hours
pub const DEFAULT_TASK_HISTORY_RETENTION_CHECK_INTERVAL_SECS: u64 = 15 * 60; // 15 minutes

/// [`TaskSpan`] records information about the execution of a given task. On [`finish`], it will write to the datafusion.
pub(crate) struct TaskSpan {
    pub(crate) trace_id: Arc<str>,

    /// A user-defined trace id that can be used to override the default trace id when exported.
    /// This is useful for when a trace id wants to be known before its eventually written to an exporter.
    /// If this isn't a valid 16-byte array (in 32 character hexadecimal representation), it will be ignored.
    pub(crate) trace_id_override: Option<Arc<str>>,

    /// An identifier for the top level [`TaskSpan`] that this [`TaskSpan`] occurs in.
    pub(crate) span_id: Arc<str>,

    /// An identifier to the [`TaskSpan`] that directly started this [`TaskSpan`].
    pub(crate) parent_span_id: Option<Arc<str>>,

    /// A span id that came from a client-initiated trace. When set, it indicates this span is the child of a trace for a broader, distributed system (i.e. greater than just `spiced` used for distributed tracing).
    ///
    /// Only used if `parent_span_id` is `None`.
    pub(crate) distributed_parent_id: Option<Arc<str>>,

    pub(crate) task: Arc<str>,
    pub(crate) input: Arc<str>,
    pub(crate) captured_output: Option<Arc<str>>,

    pub(crate) start_time: SystemTime,
    pub(crate) end_time: SystemTime,
    pub(crate) execution_duration_ms: f64,
    pub(crate) error_message: Option<Arc<str>>,
    pub(crate) labels: HashMap<Arc<str>, Arc<str>>,
    // For top-level HTTP tasks, have a label:
    // - "http_status" (200, 400)
}

impl TaskSpan {
    pub async fn instantiate_table(
        status: Arc<status::RuntimeStatus>,
        retention_period_secs: u64,
        retention_check_interval_secs: u64,
    ) -> Result<Arc<AcceleratedTable>, Error> {
        let time_column = Some("start_time".to_string());
        let time_format = Some(TimeFormat::UnixSeconds);

        tracing::debug!("Task history retention period: {retention_period_secs} seconds");
        tracing::debug!(
            "Task history retention check interval: {retention_check_interval_secs} seconds"
        );

        let retention = Retention::new(
            time_column.clone(),
            time_format,
            None,
            None,
            Some(Duration::from_secs(retention_period_secs)), // 1 day
            Some(Duration::from_secs(retention_check_interval_secs)),
            true,
        );
        let tbl_reference =
            TableReference::partial(SPICE_RUNTIME_SCHEMA, DEFAULT_TASK_HISTORY_TABLE);

        create_internal_accelerated_table(
            status,
            tbl_reference,
            Arc::new(TaskSpan::table_schema()),
            Some(vec!["span_id".to_string()]),
            Acceleration::default(),
            Refresh::default(),
            retention,
            Arc::new(RwLock::new(Secrets::default())),
        )
        .await
        .boxed()
        .context(UnableToRegisterTableSnafu)
    }

    fn table_schema() -> Schema {
        Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
            Field::new("parent_span_id", DataType::Utf8, true),
            Field::new("task", DataType::Utf8, false),
            Field::new("input", DataType::Utf8, false),
            Field::new("captured_output", DataType::Utf8, true),
            Field::new(
                "start_time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ), // Note: Used for time column of Retention
            Field::new(
                "end_time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("execution_duration_ms", DataType::Float64, false),
            Field::new("error_message", DataType::Utf8, true),
            Field::new(
                "labels",
                DataType::Map(
                    Arc::new(Field::new_struct(
                        "entries",
                        vec![
                            Arc::new(Field::new("keys", DataType::Utf8, false)),
                            Arc::new(Field::new("values", DataType::Utf8, false)),
                        ],
                        false,
                    )),
                    false,
                ),
                false,
            ),
        ])
    }

    pub async fn write(df: Arc<DataFusion>, spans: Vec<TaskSpan>) -> Result<(), Error> {
        let overrides: Vec<_> = spans
            .iter()
            .filter_map(|s| {
                s.trace_id_override
                    .as_ref()
                    .map(|new_trace| (Arc::clone(&s.trace_id), Arc::clone(new_trace)))
            })
            .collect();

        let data = Self::to_record_batch(spans)
            .boxed()
            .context(UnableToWriteToTableSnafu)?;

        let data_update = DataUpdate {
            schema: Arc::new(Self::table_schema()),
            data: vec![data],
            update_type: crate::dataupdate::UpdateType::Append,
        };

        df.write_data(
            &TableReference::partial(SPICE_RUNTIME_SCHEMA, DEFAULT_TASK_HISTORY_TABLE),
            data_update,
        )
        .await
        .boxed()
        .context(UnableToWriteToTableSnafu)?;

        // Override trace_ids if necessary. Must be after above write so that it also handles override this batch of spans.
        for (from, to) in overrides {
            Self::override_trace_id(Arc::clone(&df), from, to)
                .await
                .boxed()
                .context(UnableToUpdateTracesSnafu)?;
        }

        Ok(())
    }

    async fn override_trace_id(
        df: Arc<DataFusion>,
        from: Arc<str>,
        to: Arc<str>,
    ) -> Result<(), Error> {
        let overriden: Vec<_> = df
            .query_builder(
                format!(
                    "SELECT * FROM {} where trace_id = '{from}'",
                    TableReference::partial(SPICE_RUNTIME_SCHEMA, DEFAULT_TASK_HISTORY_TABLE)
                        .to_quoted_string()
                )
                .as_str(),
            )
            .build()
            .run()
            .await
            .boxed()
            .context(UnableToUpdateTracesSnafu)?
            .data
            .try_collect::<Vec<RecordBatch>>()
            .await
            .boxed()
            .context(UnableToUpdateTracesSnafu)?
            .into_iter()
            .filter_map(|rb| {
                // Replace the trace id column with the new `to` trace id.
                let (idx, _) = rb.schema().column_with_name("trace_id")?;
                let mut cols = rb.columns().to_vec();

                cols[idx] = Arc::new(StringArray::from(vec![to.to_string(); rb.num_rows()]));
                Some(RecordBatch::try_new(rb.schema(), cols))
            })
            .collect::<Result<_, ArrowError>>()
            .boxed()
            .context(UnableToUpdateTracesSnafu)?;

        df.write_data(
            &TableReference::partial(SPICE_RUNTIME_SCHEMA, DEFAULT_TASK_HISTORY_TABLE),
            DataUpdate {
                schema: Arc::new(Self::table_schema()),
                data: overriden,
                update_type: UpdateType::Changes,
            },
        )
        .await
        .boxed()
        .context(UnableToUpdateTracesSnafu)?;

        Ok(())
    }

    #[allow(clippy::cast_possible_truncation)]
    fn to_record_batch(spans: Vec<TaskSpan>) -> Result<RecordBatch, Error> {
        let schema = Self::table_schema();
        let mut struct_builder = StructBuilder::from_fields(schema.fields().clone(), spans.len());

        for span in spans {
            struct_builder.append(true);

            for (col_idx, field) in schema.fields().iter().enumerate() {
                let field_builder = struct_builder.field_builder_array(col_idx);
                match field.name().as_str() {
                    "trace_id" => {
                        let str_builder = downcast_builder::<StringBuilder>(field_builder)?;
                        str_builder.append_value(&span.trace_id);
                    }
                    "span_id" => {
                        let str_builder = downcast_builder::<StringBuilder>(field_builder)?;
                        str_builder.append_value(&span.span_id);
                    }
                    "parent_span_id" => {
                        let str_builder = downcast_builder::<StringBuilder>(field_builder)?;
                        match (&span.parent_span_id, &span.distributed_parent_id) {
                            (Some(parent_span_id), _) => str_builder.append_value(parent_span_id),
                            (None, Some(parent_id)) => str_builder.append_value(parent_id),
                            (None, None) => str_builder.append_null(),
                        }
                    }
                    "task" => {
                        let str_builder = downcast_builder::<StringBuilder>(field_builder)?;
                        str_builder.append_value(&span.task);
                    }
                    "input" => {
                        let str_builder = downcast_builder::<StringBuilder>(field_builder)?;
                        str_builder.append_value(&span.input);
                    }
                    "captured_output" => {
                        let str_builder = downcast_builder::<StringBuilder>(field_builder)?;
                        match &span.captured_output {
                            Some(captured_output) => str_builder.append_value(captured_output),
                            None => str_builder.append_null(),
                        }
                    }
                    "start_time" => {
                        let timestamp_builder = downcast_builder::<
                            arrow::array::TimestampNanosecondBuilder,
                        >(field_builder)?;
                        let start_time = span
                            .start_time
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .boxed()
                            .context(UnableToCreateRowSnafu)?;
                        timestamp_builder.append_value(start_time.as_nanos() as i64);
                    }
                    "end_time" => {
                        let timestamp_builder = downcast_builder::<
                            arrow::array::TimestampNanosecondBuilder,
                        >(field_builder)?;
                        let end_time = span
                            .end_time
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .boxed()
                            .context(UnableToCreateRowSnafu)?;
                        timestamp_builder.append_value(end_time.as_nanos() as i64);
                    }
                    "execution_duration_ms" => {
                        let float_builder =
                            downcast_builder::<arrow::array::Float64Builder>(field_builder)?;
                        float_builder.append_value(span.execution_duration_ms);
                    }
                    "error_message" => {
                        let str_builder = downcast_builder::<StringBuilder>(field_builder)?;
                        match &span.error_message {
                            Some(error_message) => str_builder.append_value(error_message),
                            None => str_builder.append_null(),
                        }
                    }
                    "labels" => {
                        let map_builder = downcast_builder::<
                            MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>,
                        >(field_builder)?;
                        let (keys_field, values_field) = map_builder.entries();
                        let keys_field = downcast_builder::<StringBuilder>(keys_field)?;
                        let values_field = downcast_builder::<StringBuilder>(values_field)?;
                        for (key, value) in &span.labels {
                            keys_field.append_value(key);
                            values_field.append_value(value);
                        }
                        map_builder
                            .append(true)
                            .boxed()
                            .context(UnableToCreateRowSnafu)?;
                    }
                    name => unreachable!("unexpected field name: {name}"),
                }
            }
        }

        Ok(struct_builder.finish().into())
    }
}

pub(crate) fn downcast_builder<T: ArrayBuilder>(
    builder: &mut dyn ArrayBuilder,
) -> Result<&mut T, Error> {
    let builder = builder
        .as_any_mut()
        .downcast_mut::<T>()
        .context(DowncastBuilderSnafu)?;
    Ok(builder)
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error registering `task_history` table: {source}"))]
    UnableToRegisterTable {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Error writing to `task_history` table: {source}"))]
    UnableToWriteToTable {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Unable to update `trace_id` column in the `task_history` table: {source}"))]
    UnableToUpdateTraces {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Error creating `task_history` row: {source}"))]
    UnableToCreateRow {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Error validating `task_history` row. Columns {columns} are required but missing"
    ))]
    MissingColumnsInRow { columns: String },

    #[snafu(display("Unable to get table provider for `task_history` table: {source}"))]
    UnableToGetTableProvider {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Unable to downcast ArrayBuilder"))]
    DowncastBuilder,

    #[snafu(display("Invalid `task_history` configuration: {source}"))]
    InvalidConfiguration {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}
