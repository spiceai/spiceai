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
use crate::dataupdate::DataUpdate;
use crate::internal_table::create_internal_accelerated_table;
use crate::{component::dataset::acceleration::Acceleration, datafusion::SPICE_RUNTIME_SCHEMA};
use crate::{component::dataset::TimeFormat, secrets::Secrets};
use arrow::array::{
    Array, ArrayData, BooleanArray, Float32Array, MapArray, RecordBatch, StringArray, StructArray,
    TimestampNanosecondArray, UInt64Array,
};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::error::ArrowError;
use datafusion::sql::TableReference;
use snafu::{ResultExt, Snafu};
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
use tokio::time::Instant;
use uuid::Uuid;

use crate::accelerated_table::{AcceleratedTable, Retention};

pub const DEFAULT_TASK_HISTORY_TABLE: &str = "task_history";

pub enum TaskType {
    Query,
    Chat,
    Embed,
    Search,
    FunctionCall,
}

impl Display for TaskType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskType::Query => write!(f, "query"),
            TaskType::Chat => write!(f, "chat"),
            TaskType::Embed => write!(f, "embed"),
            TaskType::Search => write!(f, "search"),
            TaskType::FunctionCall => write!(f, "function_call"),
        }
    }
}

fn convert_hashmap_to_maparray(labels: &HashMap<String, String>) -> Result<MapArray, ArrowError> {
    let keys_field = Arc::new(Field::new("keys", DataType::Utf8, false));
    let values_field = Arc::new(Field::new("values", DataType::Utf8, false));

    let (keys, values): (Vec<&str>, Vec<&str>) =
        labels.iter().map(|(k, v)| (k.as_str(), v.as_str())).unzip();

    let keys_array = StringArray::from(keys);
    let values_array = StringArray::from(values);

    let entry_struct = StructArray::from(vec![
        (
            keys_field.clone(),
            Arc::new(keys_array) as Arc<dyn arrow::array::Array>,
        ),
        (
            values_field.clone(),
            Arc::new(values_array) as Arc<dyn arrow::array::Array>,
        ),
    ]);

    let entry_offsets = Buffer::from_vec(vec![0, labels.len() as i32]);
    let map_data_type = DataType::Map(
        Arc::new(Field::new_struct(
            "entries",
            vec![
                Arc::new(Field::new("keys", DataType::Utf8, false)),
                Arc::new(Field::new("values", DataType::Utf8, false)),
            ],
            false,
        )),
        false,
    );

    let map_data = ArrayData::builder(map_data_type)
        .len(1)
        .add_buffer(entry_offsets)
        .add_child_data(entry_struct.to_data())
        .build()?;

    Ok(MapArray::from(map_data))
}

pub(crate) struct TaskTracker {
    pub(crate) df: Arc<crate::datafusion::DataFusion>,
    pub(crate) id: Uuid,
    pub(crate) context_id: Uuid,
    pub(crate) parent_id: Option<Uuid>,

    pub(crate) task_type: TaskType,
    pub(crate) input_text: Arc<str>,

    pub(crate) start_time: SystemTime,
    pub(crate) end_time: Option<SystemTime>,
    pub(crate) execution_time: Option<f32>,
    pub(crate) outputs_produced: u64,
    pub(crate) cache_hit: Option<bool>,
    pub(crate) error_message: Option<String>,
    pub(crate) labels: HashMap<String, String>,

    pub(crate) timer: Instant,
}

impl TaskTracker {
    pub fn new(
        df: Arc<crate::datafusion::DataFusion>,
        context_id: Uuid,
        task_type: TaskType,
        input_text: Arc<str>,
        id: Option<Uuid>,
    ) -> Self {
        Self {
            df,
            id: id.unwrap_or_else(Uuid::new_v4),
            context_id,
            parent_id: None,
            task_type,
            input_text,
            start_time: SystemTime::now(),
            end_time: None,
            execution_time: None,
            outputs_produced: 0,
            cache_hit: None,
            error_message: None,
            labels: HashMap::default(),
            timer: Instant::now(),
        }
    }

    pub fn outputs_produced(mut self, outputs_produced: u64) -> Self {
        self.outputs_produced = outputs_produced;
        self
    }

    pub fn with_error_message(mut self, error_message: String) -> Self {
        self.error_message = Some(error_message);
        self
    }

    pub fn labels<I: IntoIterator<Item = (String, String)>>(mut self, labels: I) -> Self {
        self.labels.extend(labels);
        self
    }

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
            Arc::new(TaskTracker::table_schema()),
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
            Field::new("id", DataType::Utf8, false),
            Field::new("context_id", DataType::Utf8, false),
            Field::new("parent_id", DataType::Utf8, true),
            Field::new("task_type", DataType::Utf8, false),
            Field::new("input_text", DataType::Utf8, false),
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
            Field::new("execution_time", DataType::Float32, false),
            Field::new("outputs_produced", DataType::UInt64, false),
            Field::new("cache_hit", DataType::Boolean, false),
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

    pub async fn finish(mut self) {
        if self.end_time.is_none() {
            self.end_time = Some(SystemTime::now());
        }

        let duration = self.timer.elapsed();

        if self.execution_time.is_none() {
            self.execution_time = Some(duration.as_secs_f32());
        }

        if let Err(err) = self.write().await {
            tracing::error!("Error writing task history: {err}");
        };
    }

    pub async fn write(&self) -> Result<(), Error> {
        if self.end_time.is_none() {
            return Err(Error::MissingColumnsInRow {
                columns: "end_time".to_string(),
            });
        }

        let data = self
            .to_record_batch()
            .boxed()
            .context(UnableToWriteToTableSnafu)?;

        let data_update = DataUpdate {
            schema: Arc::new(Self::table_schema()),
            data: vec![data],
            update_type: crate::dataupdate::UpdateType::Append,
        };

        self.df
            .write_data(
                TableReference::partial(SPICE_RUNTIME_SCHEMA, DEFAULT_TASK_HISTORY_TABLE),
                data_update,
            )
            .await
            .boxed()
            .context(UnableToWriteToTableSnafu)?;

        Ok(())
    }

    fn to_record_batch(&self) -> Result<RecordBatch, Error> {
        let end_time = self
            .end_time
            .and_then(|s| {
                s.duration_since(SystemTime::UNIX_EPOCH)
                    .map(|x| i64::try_from(x.as_nanos()))
                    .ok()
            })
            .transpose()
            .boxed()
            .context(UnableToCreateRowSnafu)?;

        let start_time = self
            .start_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|duration| i64::try_from(duration.as_nanos()).ok())
            .boxed()
            .context(UnableToCreateRowSnafu)?;

        let labels = convert_hashmap_to_maparray(&self.labels)
            .boxed()
            .context(UnableToCreateRowSnafu)?;

        RecordBatch::try_new(
            Arc::new(Self::table_schema()),
            vec![
                Arc::new(StringArray::from(vec![self.id.to_string()])),
                Arc::new(StringArray::from(vec![self.context_id.to_string()])),
                Arc::new(StringArray::from(vec![self
                    .parent_id
                    .map(|s| s.to_string())])),
                Arc::new(StringArray::from(vec![self.task_type.to_string()])),
                Arc::new(StringArray::from(vec![self.input_text.as_ref()])),
                Arc::new(TimestampNanosecondArray::from(vec![start_time])),
                Arc::new(TimestampNanosecondArray::from(vec![end_time])),
                Arc::new(Float32Array::from(vec![self.execution_time])),
                Arc::new(UInt64Array::from(vec![self.outputs_produced])),
                Arc::new(BooleanArray::from(vec![self.cache_hit.unwrap_or(false)])),
                Arc::new(StringArray::from(vec![self.error_message.clone()])),
                Arc::new(labels),
            ],
        )
        .boxed()
        .context(UnableToCreateRowSnafu)
    }
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
}
