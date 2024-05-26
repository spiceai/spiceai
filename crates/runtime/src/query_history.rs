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

use crate::component::dataset::acceleration::{Acceleration, RefreshMode};
use crate::component::dataset::TimeFormat;
use arrow::{
    array::{BooleanArray, RecordBatch, StringArray, TimestampNanosecondArray, UInt64Array},
    datatypes::{DataType, Field, Schema, TimeUnit},
};
use datafusion::sql::TableReference;
use snafu::{ResultExt, Snafu};
use tokio::sync::RwLock;

use crate::{
    accelerated_table::{refresh::Refresh, AcceleratedTable, Retention},
    datafusion::DataFusion,
    dataupdate::DataUpdate,
    internal_table::{create_internal_accelerated_table, create_synced_internal_accelerated_table},
};

pub const DEFAULT_QUERY_HISTORY_TABLE: &str = "query_history";

pub async fn instantiate_query_history_table(
    secrets_provider: &Arc<RwLock<secrets::SecretsProvider>>,
    cloud_dataset_path: Option<String>,
) -> Result<Arc<AcceleratedTable>, Error> {
    let time_column = Some("start_time".to_string());
    let time_format = Some(TimeFormat::UnixSeconds);

    let retention = Retention::new(
        time_column.clone(),
        time_format,
        Some(Duration::from_secs(3 * 24 * 60 * 60)), // 3 days
        Some(Duration::from_secs(300)),
        true,
    );

    match cloud_dataset_path {
        Some(path) => {
            let secret = secrets_provider
                .read()
                .await
                .get_secret("spiceai")
                .await
                .context(UnableToRegisterTableSnafu)?;

            let refresh = Refresh {
                mode: RefreshMode::Append,
                time_column,
                time_format,
                check_interval: Some(Duration::from_secs(10)),
                period: Some(Duration::from_secs(1800)), // sync only last 30 minutes from cloud
                ..Default::default()
            };

            create_synced_internal_accelerated_table(
                DEFAULT_QUERY_HISTORY_TABLE.into(),
                path.as_str(),
                secret,
                Acceleration::default(),
                refresh,
                retention,
            )
            .await
            .boxed()
            .context(UnableToRegisterTableSnafu)
        }
        None => create_internal_accelerated_table(
            DEFAULT_QUERY_HISTORY_TABLE.into(),
            Arc::new(table_schema()),
            Acceleration::default(),
            Refresh::default(),
            retention,
        )
        .await
        .boxed()
        .context(UnableToRegisterTableSnafu),
    }
}

#[must_use]
pub fn table_schema() -> Schema {
    Schema::new(vec![
        Field::new("query_id", DataType::Utf8, false),
        Field::new("schema", DataType::Utf8, false),
        Field::new("sql", DataType::Utf8, false),
        Field::new("nsql_query", DataType::Utf8, true),
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

    #[snafu(display("Error validating query_history row. {source} is required"))]
    MissingColumnInRow {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

macro_rules! check_required_field {
    ($field:expr, $field_name:expr) => {
        if $field.is_none() {
            return Err(Error::MissingColumnInRow {
                source: $field_name.into(),
            });
        }
    };
}

#[derive(Default)]
pub struct QueryHistory {
    query_id: Option<String>,
    schema: Option<Arc<Schema>>,
    sql: Option<String>,
    nsql_query: Option<String>,
    start_time: Option<SystemTime>,
    end_time: Option<SystemTime>,
    execution_time: Option<u64>,
    rows_produced: Option<u64>,
    results_cache_hit: Option<bool>,

    // The datafusion instance and the internal table to route data to.
    df: Option<Arc<RwLock<DataFusion>>>,
    to_table: Option<String>,
}

impl QueryHistory {
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
    pub fn nsql_query(mut self, nsql_query: String) -> Self {
        self.nsql_query = Some(nsql_query);
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
    pub fn df(mut self, df: Arc<RwLock<DataFusion>>) -> Self {
        self.df = Some(df);
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
                Arc::new(StringArray::from(vec![self.nsql_query.clone()])),
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
        check_required_field!(self.schema, "schema");
        check_required_field!(self.sql, "sql");
        check_required_field!(self.start_time, "start_time");
        // check_required_field!(self.execution_time, "execution_time");
        check_required_field!(self.rows_produced, "rows_produced");
        check_required_field!(self.results_cache_hit, "results_cache_hit");
        check_required_field!(self.df, "df");

        Ok(())
    }
}

impl Drop for QueryHistory {
    fn drop(&mut self) {
        if let Err(err) = self.validate() {
            tracing::error!("Error validating QueryHistory: {err}");
            return;
        }
        if self.end_time.is_none() {
            self.end_time = Some(SystemTime::now());
        }
        if self.to_table.is_none() {
            self.to_table = Some(DEFAULT_QUERY_HISTORY_TABLE.to_string());
        }
        if self.query_id.is_none() {
            self.query_id = Some(uuid::Uuid::new_v4().to_string());
        }
        let data = match self.into_record_batch() {
            Ok(data) => data,
            Err(err) => {
                tracing::error!("Cannot create record batch for query history: {err}");
                return;
            }
        };
        let table = self
            .to_table
            .clone()
            .unwrap_or(DEFAULT_QUERY_HISTORY_TABLE.to_string());

        match self.df.clone() {
            Some(df) => {
                tokio::spawn(async move {
                    let data_update = DataUpdate {
                        schema: Arc::new(table_schema()),
                        data: vec![data],
                        update_type: crate::dataupdate::UpdateType::Append,
                    };
                    if let Err(e) = df
                        .write()
                        .await
                        .write_data(TableReference::partial("runtime", table), data_update)
                        .await
                        .boxed()
                        .context(UnableToWriteToTableSnafu)
                    {
                        tracing::error!("Error writing to query_history table: {}", e);
                    }
                });
            }
            None => {
                tracing::error!("DataFusion instance is missing");
            }
        }
    }
}
