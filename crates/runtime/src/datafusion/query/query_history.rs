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

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use crate::{
    component::dataset::acceleration::Acceleration, datafusion::SPICE_RUNTIME_SCHEMA,
    task_history::TaskSpan,
};
use crate::{component::dataset::TimeFormat, secrets::Secrets};
use arrow::{
    array::{
        BooleanArray, Float32Array, Int8Array, RecordBatch, StringArray, TimestampNanosecondArray,
        UInt64Array,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
};
use datafusion::sql::TableReference;

use snafu::{ResultExt, Snafu};
use tokio::sync::RwLock;

use crate::{
    accelerated_table::{refresh::Refresh, AcceleratedTable, Retention},
    dataupdate::DataUpdate,
    internal_table::create_internal_accelerated_table,
};

use super::tracker::QueryTracker;

pub const DEFAULT_QUERY_HISTORY_TABLE: &str = "query_history";

pub async fn instantiate_query_history_table() -> Result<Arc<AcceleratedTable>, Error> {
    let time_column = Some("start_time".to_string());
    let time_format = Some(TimeFormat::UnixSeconds);

    let retention = Retention::new(
        time_column.clone(),
        time_format,
        Some(Duration::from_secs(24 * 60 * 60)), // 1 day
        Some(Duration::from_secs(300)),
        true,
    );
    let query_history_table_reference =
        TableReference::partial(SPICE_RUNTIME_SCHEMA, DEFAULT_QUERY_HISTORY_TABLE);
    create_internal_accelerated_table(
        query_history_table_reference,
        Arc::new(table_schema()),
        Acceleration::default(),
        Refresh::default(),
        retention,
        Arc::new(RwLock::new(Secrets::default())),
    )
    .await
    .boxed()
    .context(UnableToRegisterTableSnafu)
}

#[must_use]
fn table_schema() -> Schema {
    Schema::new(vec![
        Field::new("query_id", DataType::Utf8, false),
        Field::new("schema", DataType::Utf8, true),
        Field::new("sql", DataType::Utf8, false),
        Field::new("nsql", DataType::Utf8, true),
        Field::new(
            "start_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "end_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("execution_time", DataType::Float32, false),
        Field::new("execution_status", DataType::Int8, false),
        Field::new("rows_produced", DataType::UInt64, false),
        Field::new("results_cache_hit", DataType::Boolean, false),
        Field::new("error_message", DataType::Utf8, true),
    ])
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error registering table: {source}"))]
    UnableToRegisterTable {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Error writing to query_history table: {source}"))]
    UnableToWriteToTable {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Error writing to task_history table: {source}"))]
    UnableToWriteToTaskTable {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Error creating query_history row: {source}"))]
    UnableToCreateRow {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Error validating query_history row. Columns {columns} are required but missing"
    ))]
    MissingColumnsInRow { columns: String },

    #[snafu(display("Unable to get table provider for query_history table: {source}"))]
    UnableToGetTableProvider {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// Checks if a required field is missing in a [`QueryHistory`] struct. Adds field name to missing fields vector if field is None.
macro_rules! check_required_field {
    ($field:expr, $field_name:expr, $missing_fields:expr) => {
        if $field.is_none() {
            $missing_fields.push($field_name.into());
        }
    };
}

impl QueryTracker {
    pub async fn write_query_history(&self, truncated_output: Arc<str>) -> Result<(), Error> {
        self.validate()?;

        let data = self
            .to_record_batch()
            .boxed()
            .context(UnableToWriteToTableSnafu)?;

        let data_update = DataUpdate {
            schema: Arc::new(table_schema()),
            data: vec![data],
            update_type: crate::dataupdate::UpdateType::Append,
        };

        self.df
            .write_data(
                TableReference::partial(SPICE_RUNTIME_SCHEMA, DEFAULT_QUERY_HISTORY_TABLE),
                data_update,
            )
            .await
            .boxed()
            .context(UnableToWriteToTableSnafu)?;

        // Whilst both the query history and task history tables exist, don't need a `TaskTracker` for recording queries.
        Into::<TaskSpan>::into(self)
            .truncated_output_text(truncated_output)
            .write()
            .await
            .boxed()
            .context(UnableToWriteToTaskTableSnafu)
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

        let execution_status = match &self.error_code {
            Some(code) => code.into(),
            None => 0,
        };

        RecordBatch::try_new(
            Arc::new(table_schema()),
            vec![
                Arc::new(StringArray::from(vec![self.query_id.to_string()])),
                Arc::new(StringArray::from(vec![self
                    .schema
                    .as_ref()
                    .map(ToString::to_string)])),
                Arc::new(StringArray::from(vec![self.sql.as_ref()])),
                Arc::new(StringArray::from(vec![self.nsql.as_ref().map(Arc::as_ref)])),
                Arc::new(TimestampNanosecondArray::from(vec![start_time])),
                Arc::new(TimestampNanosecondArray::from(vec![end_time])),
                Arc::new(Float32Array::from(vec![self.execution_time])),
                Arc::new(Int8Array::from(vec![execution_status])),
                Arc::new(UInt64Array::from(vec![self.rows_produced])),
                Arc::new(BooleanArray::from(vec![self
                    .results_cache_hit
                    .unwrap_or(false)])),
                Arc::new(StringArray::from(vec![self.error_message.clone()])),
            ],
        )
        .boxed()
        .context(UnableToCreateRowSnafu)
    }

    fn validate(&self) -> Result<(), Error> {
        let mut missing_fields: Vec<&str> = Vec::new();

        check_required_field!(self.end_time, "end_time", missing_fields);

        if missing_fields.is_empty() {
            Ok(())
        } else {
            Err(Error::MissingColumnsInRow {
                columns: missing_fields.join(", "),
            })
        }
    }
}
