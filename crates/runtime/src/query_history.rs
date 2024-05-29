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

use crate::component::dataset::TimeFormat;
use crate::{component::dataset::acceleration::Acceleration, datafusion::SPICE_RUNTIME_SCHEMA};
use arrow::{
    array::{BooleanArray, RecordBatch, StringArray, TimestampNanosecondArray, UInt64Array},
    datatypes::{DataType, Field, Schema, TimeUnit},
};
use datafusion::sql::TableReference;
use snafu::{ResultExt, Snafu};

use crate::{
    accelerated_table::{refresh::Refresh, AcceleratedTable, Retention},
    datafusion::DataFusion,
    dataupdate::DataUpdate,
    internal_table::create_internal_accelerated_table,
};

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
    )
    .await
    .boxed()
    .context(UnableToRegisterTableSnafu)
}

#[must_use]
fn table_schema() -> Schema {
    Schema::new(vec![
        Field::new("query_id", DataType::Utf8, false),
        Field::new("schema", DataType::Utf8, false),
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
        Field::new("execution_time", DataType::UInt64, true), //todo: make this required
        Field::new("rows_produced", DataType::UInt64, false),
        Field::new("results_cache_hit", DataType::Boolean, false),
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

    #[snafu(display("Error creating query_history row: {source}"))]
    UnableToCreateRow {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Error validating query_history row. Columns {columns} are required but missing"
    ))]
    MissingColumnsInRow { columns: String },
}

/// Checks if a required field is missing in a [`QueryHistory`] struct. Adds field name to missing fields vector if field is None.
macro_rules! check_required_field {
    ($field:expr, $field_name:expr, $missing_fields:expr) => {
        if $field.is_none() {
            $missing_fields.push($field_name.into());
        }
    };
}

pub struct QueryHistory {
    query_id: Option<String>,
    schema: Option<Arc<Schema>>,
    sql: Option<String>,
    nsql: Option<String>,
    start_time: Option<SystemTime>,
    end_time: Option<SystemTime>,
    execution_time: Option<u64>,
    rows_produced: Option<u64>,
    results_cache_hit: Option<bool>,

    // The datafusion instance and the internal table to route data to.
    df: Arc<DataFusion>,
    to_table: Option<String>,
}

impl QueryHistory {
    pub fn new(df: Arc<DataFusion>) -> Self {
        Self {
            query_id: None,
            schema: None,
            sql: None,
            nsql: None,
            start_time: None,
            end_time: None,
            execution_time: None,
            rows_produced: None,
            results_cache_hit: None,
            df,
            to_table: None,
        }
    }

    #[must_use]
    pub fn query_id(mut self, query_id: String) -> Self {
        self.query_id = Some(query_id);
        self
    }

    #[must_use]
    pub fn schema(mut self, schema: Arc<Schema>) -> Self {
        self.schema = Some(schema);
        self
    }

    #[must_use]
    pub fn sql(mut self, sql: String) -> Self {
        self.sql = Some(sql);
        self
    }

    #[must_use]
    pub fn nsql(mut self, nsql: String) -> Self {
        self.nsql = Some(nsql);
        self
    }

    #[must_use]
    pub fn start_time(mut self, start_time: SystemTime) -> Self {
        self.start_time = Some(start_time);
        self
    }

    #[must_use]
    pub fn end_time(mut self, end_time: SystemTime) -> Self {
        self.end_time = Some(end_time);
        self
    }

    #[must_use]
    pub fn execution_time(mut self, execution_time: u64) -> Self {
        self.execution_time = Some(execution_time);
        self
    }

    #[must_use]
    pub fn rows_produced(mut self, rows_produced: u64) -> Self {
        self.rows_produced = Some(rows_produced);
        self
    }

    #[must_use]
    pub fn results_cache_hit(mut self, results_cache_hit: bool) -> Self {
        self.results_cache_hit = Some(results_cache_hit);
        self
    }

    #[must_use]
    pub fn to_table(mut self, to_table: String) -> Self {
        self.to_table = Some(to_table);
        self
    }

    pub fn into_record_batch(&self) -> Result<RecordBatch, Error> {
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
            .and_then(|s| {
                s.duration_since(SystemTime::UNIX_EPOCH)
                    .map(|x| Some(i64::try_from(x.as_nanos())))
                    .unwrap_or(None)
            })
            .transpose()
            .boxed()
            .context(UnableToCreateRowSnafu)?;

        RecordBatch::try_new(
            Arc::new(table_schema()),
            vec![
                Arc::new(StringArray::from(vec![self.query_id.clone()])),
                Arc::new(StringArray::from(vec![self
                    .schema
                    .as_ref()
                    .map(ToString::to_string)])),
                Arc::new(StringArray::from(vec![self.sql.clone()])),
                Arc::new(StringArray::from(vec![self.nsql.clone()])),
                Arc::new(TimestampNanosecondArray::from(vec![start_time])),
                Arc::new(TimestampNanosecondArray::from(vec![end_time])),
                Arc::new(UInt64Array::from(vec![self.execution_time])),
                Arc::new(UInt64Array::from(vec![self.rows_produced])),
                Arc::new(BooleanArray::from(vec![self.results_cache_hit])),
            ],
        )
        .boxed()
        .context(UnableToCreateRowSnafu)
    }

    pub fn validate(&self) -> Result<(), Error> {
        let mut missing_fields: Vec<&str> = Vec::new();

        check_required_field!(self.schema, "schema", missing_fields);
        check_required_field!(self.sql, "sql", missing_fields);
        check_required_field!(self.start_time, "start_time", missing_fields);
        // check_required_field!(self.execution_time, "execution_time", missing_fields);
        check_required_field!(self.rows_produced, "rows_produced", missing_fields);
        check_required_field!(self.results_cache_hit, "results_cache_hit", missing_fields);

        if missing_fields.is_empty() {
            Ok(())
        } else {
            Err(Error::MissingColumnsInRow {
                columns: missing_fields.join(", "),
            })
        }
    }

    pub async fn write(&mut self) -> Result<(), Error> {
        self.validate()?;

        if self.end_time.is_none() {
            self.end_time = Some(SystemTime::now());
        }
        if self.to_table.is_none() {
            self.to_table = Some(DEFAULT_QUERY_HISTORY_TABLE.to_string());
        }
        if self.query_id.is_none() {
            self.query_id = Some(uuid::Uuid::new_v4().to_string());
        }
        let table = self
            .to_table
            .clone()
            .unwrap_or(DEFAULT_QUERY_HISTORY_TABLE.to_string());

        let data = self
            .into_record_batch()
            .boxed()
            .context(UnableToWriteToTableSnafu)?;

        let data_update = DataUpdate {
            schema: Arc::new(table_schema()),
            data: vec![data],
            update_type: crate::dataupdate::UpdateType::Append,
        };

        self.df
            .write_data(TableReference::partial("runtime", table), data_update)
            .await
            .boxed()
            .context(UnableToWriteToTableSnafu)?;

        Ok(())
    }
}
