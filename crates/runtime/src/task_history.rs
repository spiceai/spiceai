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
use crate::datafusion::query::QueryTracker;
use crate::dataupdate::DataUpdate;
use crate::internal_table::create_internal_accelerated_table;
use crate::{component::dataset::acceleration::Acceleration, datafusion::SPICE_RUNTIME_SCHEMA};
use crate::{component::dataset::TimeFormat, secrets::Secrets};
use arrow::array::{
    Array, ArrayData, BooleanArray, Float64Array, MapArray, RecordBatch, StringArray, StructArray,
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

impl From<&QueryTracker> for TaskSpan {
    fn from(qt: &QueryTracker) -> Self {
        let mut labels = HashMap::new();
        if let Some(schema) = &qt.schema {
            labels.insert("schema".to_string(), format!("{schema:?}"));
        }
        if let Some(error_code) = &qt.error_code {
            labels.insert("error_code".to_string(), format!("{error_code}"));
        }
        labels.insert("protocol".to_string(), format!("{:?}", qt.protocol));
        labels.insert("datasets".to_string(), format!("{:?}", qt.datasets));

        let task_type = if qt.nsql.is_some() {
            TaskType::NsqlQuery
        } else {
            TaskType::SqlQuery
        };

        let input_text = if let Some(nsql) = &qt.nsql {
            Arc::clone(nsql)
        } else {
            Arc::<str>::clone(&qt.sql)
        };

        TaskSpan {
            df: Arc::clone(&qt.df),
            id: qt.query_id,
            context_id: qt.query_id, // assuming context_id and id are the same; adjust as needed
            parent_id: None,
            task_type,
            input_text,
            start_time: qt.start_time,
            end_time: qt.end_time,
            execution_duration_ms: qt.execution_time.map(|t| f64::from(1000.0 * t)), // convert s to ms.
            outputs_produced: qt.rows_produced,
            cache_hit: qt.results_cache_hit,
            error_message: qt.error_message.clone(),
            labels,
            timer: qt.timer,
            truncated_output_text: None,
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
            Arc::clone(&keys_field),
            Arc::new(keys_array) as Arc<dyn arrow::array::Array>,
        ),
        (
            Arc::clone(&values_field),
            Arc::new(values_array) as Arc<dyn arrow::array::Array>,
        ),
    ]);

    let entry_offsets = Buffer::from_vec(vec![0, labels.len() as u64]);
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

/// [`TaskSpan`] records information about the execution of a given task. On [`finish`], it will write to the datafusion.
pub(crate) struct TaskSpan {
    pub(crate) df: Arc<crate::datafusion::DataFusion>,
    pub(crate) id: Uuid,

    /// An identifier for the top level [`TaskSpan`] that this [`TaskSpan`] occurs in.
    pub(crate) context_id: Uuid,

    /// An identifier to the [`TaskSpan`] that directly started this [`TaskSpan`].
    pub(crate) parent_id: Option<Uuid>,

    pub(crate) task_type: TaskType,
    pub(crate) input_text: Arc<str>,
    pub(crate) truncated_output_text: Option<Arc<str>>,

    pub(crate) start_time: SystemTime,
    pub(crate) end_time: Option<SystemTime>,
    pub(crate) execution_duration_ms: Option<f64>,
    pub(crate) outputs_produced: u64,
    pub(crate) cache_hit: Option<bool>,
    pub(crate) error_message: Option<String>,
    pub(crate) labels: HashMap<String, String>,

    pub(crate) timer: Instant,
}

impl TaskSpan {
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
            execution_duration_ms: None,
            outputs_produced: 0,
            cache_hit: None,
            error_message: None,
            labels: HashMap::default(),
            timer: Instant::now(),
            truncated_output_text: None,
        }
    }

    pub fn truncated_output_text(mut self, truncated_output_text: Arc<str>) -> Self {
        self.truncated_output_text = Some(truncated_output_text);
        self
    }

    pub fn outputs_produced(mut self, outputs_produced: u64) -> Self {
        self.outputs_produced = outputs_produced;
        self
    }

    pub fn with_error_message(mut self, error_message: String) -> Self {
        self.error_message = Some(error_message);
        self
    }

    pub fn label(mut self, key: String, value: String) -> Self {
        self.labels.insert(key, value);
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
            Field::new("id", DataType::Utf8, false),
            Field::new("context_id", DataType::Utf8, false),
            Field::new("parent_id", DataType::Utf8, true),
            Field::new("task_type", DataType::Utf8, false),
            Field::new("input_text", DataType::Utf8, false),
            Field::new("truncated_output_text", DataType::Utf8, true),
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

    pub fn finish(mut self) {
        if self.end_time.is_none() {
            self.end_time = Some(SystemTime::now());
        }

        let duration = self.timer.elapsed();

        if self.execution_duration_ms.is_none() {
            self.execution_duration_ms = Some(1000.0 * duration.as_secs_f64());
        }

        tokio::task::spawn(async move {
            if let Err(err) = self.write().await {
                tracing::error!("Error writing task history: {err}");
            }
        });
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
                Arc::new(StringArray::from(vec![self.input_text.to_string()])),
                Arc::new(StringArray::from(vec![self
                    .truncated_output_text
                    .clone()
                    .map(|s| s.to_string())])),
                Arc::new(TimestampNanosecondArray::from(vec![start_time])),
                Arc::new(TimestampNanosecondArray::from(vec![end_time])),
                Arc::new(Float64Array::from(vec![self.execution_duration_ms])),
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
