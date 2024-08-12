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

use crate::accelerated_table::refresh::Refresh;
use crate::datafusion::DataFusion;
use crate::dataupdate::DataUpdate;
use crate::internal_table::create_internal_accelerated_table;
use crate::{component::dataset::acceleration::Acceleration, datafusion::SPICE_RUNTIME_SCHEMA};
use crate::{component::dataset::TimeFormat, secrets::Secrets};
use arrow::array::{ArrayBuilder, RecordBatch, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use data_components::arrow::struct_builder::StructBuilder;
use datafusion::sql::TableReference;
use snafu::prelude::*;
use snafu::{ResultExt, Snafu};
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;

use crate::accelerated_table::{AcceleratedTable, Retention};

pub mod otel_exporter;

pub const DEFAULT_TASK_HISTORY_TABLE: &str = "task_history";

pub enum TaskType {
    SqlQuery,
    NsqlQuery,
    AiCompletion,
    TextEmbed,
    VectorSearch,
}

impl Display for TaskType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskType::SqlQuery => write!(f, "sql_query"),
            TaskType::NsqlQuery => write!(f, "nsql_query"),
            TaskType::AiCompletion => write!(f, "ai_completion"),
            TaskType::TextEmbed => write!(f, "text_embed"),
            TaskType::VectorSearch => write!(f, "vector_search"),
        }
    }
}

// fn convert_hashmap_to_maparray(labels: &HashMap<String, String>) -> Result<MapArray, ArrowError> {
//     let keys_field = Arc::new(Field::new("keys", DataType::Utf8, false));
//     let values_field = Arc::new(Field::new("values", DataType::Utf8, false));

//     let (keys, values): (Vec<&str>, Vec<&str>) =
//         labels.iter().map(|(k, v)| (k.as_str(), v.as_str())).unzip();

//     let keys_array = StringArray::from(keys);
//     let values_array = StringArray::from(values);

//     let entry_struct = StructArray::from(vec![
//         (
//             Arc::clone(&keys_field),
//             Arc::new(keys_array) as Arc<dyn arrow::array::Array>,
//         ),
//         (
//             Arc::clone(&values_field),
//             Arc::new(values_array) as Arc<dyn arrow::array::Array>,
//         ),
//     ]);

//     let entry_offsets = Buffer::from_vec(vec![0, labels.len() as u64]);
//     let map_data_type = DataType::Map(
//         Arc::new(Field::new_struct(
//             "entries",
//             vec![
//                 Arc::new(Field::new("keys", DataType::Utf8, false)),
//                 Arc::new(Field::new("values", DataType::Utf8, false)),
//             ],
//             false,
//         )),
//         false,
//     );

//     let map_data = ArrayData::builder(map_data_type)
//         .len(1)
//         .add_buffer(entry_offsets)
//         .add_child_data(entry_struct.to_data())
//         .build()?;

//     Ok(MapArray::from(map_data))
// }

/// [`TaskSpan`] records information about the execution of a given task. On [`finish`], it will write to the datafusion.
pub(crate) struct TaskSpan {
    pub(crate) trace_id: Arc<str>,

    /// An identifier for the top level [`TaskSpan`] that this [`TaskSpan`] occurs in.
    pub(crate) span_id: Arc<str>,

    /// An identifier to the [`TaskSpan`] that directly started this [`TaskSpan`].
    pub(crate) parent_span_id: Option<Arc<str>>,

    pub(crate) task: Arc<str>,
    pub(crate) input: Arc<str>,
    pub(crate) truncated_output: Option<Arc<str>>,

    pub(crate) start_time: SystemTime,
    pub(crate) end_time: SystemTime,
    pub(crate) execution_duration_ms: f64,
    pub(crate) error_message: Option<Arc<str>>,
    pub(crate) labels: HashMap<Arc<str>, Arc<str>>,
    // For top-level HTTP tasks, have a label:
    // - "http_status" (200, 400)
}

impl TaskSpan {
    pub async fn instantiate_table() -> Result<Arc<AcceleratedTable>, Error> {
        let time_column = Some("start_time".to_string());
        let time_format = Some(TimeFormat::UnixSeconds);

        let retention = Retention::new(
            time_column.clone(),
            time_format,
            Some(Duration::from_secs(24 * 60 * 60)), // 1 day
            Some(Duration::from_secs(300)),
            true,
        );
        let tbl_reference =
            TableReference::partial(SPICE_RUNTIME_SCHEMA, DEFAULT_TASK_HISTORY_TABLE);

        create_internal_accelerated_table(
            tbl_reference,
            Arc::new(TaskSpan::table_schema()),
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
            Field::new("truncated_output", DataType::Utf8, true),
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
        let data = Self::to_record_batch(spans)
            .boxed()
            .context(UnableToWriteToTableSnafu)?;

        let data_update = DataUpdate {
            schema: Arc::new(Self::table_schema()),
            data: vec![data],
            update_type: crate::dataupdate::UpdateType::Append,
        };

        df.write_data(
            TableReference::partial(SPICE_RUNTIME_SCHEMA, DEFAULT_TASK_HISTORY_TABLE),
            data_update,
        )
        .await
        .boxed()
        .context(UnableToWriteToTableSnafu)?;

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
                        match &span.parent_span_id {
                            Some(parent_span_id) => str_builder.append_value(parent_span_id),
                            None => str_builder.append_null(),
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
                    "truncated_output" => {
                        let str_builder = downcast_builder::<StringBuilder>(field_builder)?;
                        match &span.truncated_output {
                            Some(truncated_output) => str_builder.append_value(truncated_output),
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
                            arrow::array::MapBuilder<StringBuilder, StringBuilder>,
                        >(field_builder)?;
                        let (keys_field, values_field) = map_builder.entries();
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
}
