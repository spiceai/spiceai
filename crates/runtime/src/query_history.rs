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

use std::{sync::Arc, time::{Duration, SystemTime}};

use arrow::{array::{BooleanArray, Int64Array, RecordBatch, StringArray, UInt64Array}, datatypes::{DataType, Field, Schema, TimeUnit}};
use datafusion::sql::TableReference;
use secrets::Secret;
use snafu::{ResultExt, Snafu};
use spicepod::component::dataset::{acceleration::{Acceleration, RefreshMode}, TimeFormat};
use tokio::sync::RwLock;

use crate::{accelerated_table::{refresh::Refresh, Retention}, datafusion::DataFusion, dataupdate::DataUpdate, internal_table::{create_internal_accelerated_table, create_synced_internal_accelerated_table}};

const DEFAULT_QUERY_HISTORY_TABLE: &str = "query_history";


#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error registering table: {source}"))]
    UnableToRegisterTable { source: Box<dyn std::error::Error + Send + Sync>, },

    #[snafu(display("Error writing to query_history table: {source}"))]
    UnableToWriteToTable { source: Box<dyn std::error::Error + Send + Sync>, },
}

pub struct QueryHistory {
    schema: Arc<Schema>,
    sql: String,
    start_time: SystemTime,
    end_time: SystemTime,
    execution_time: u64,
    
    nsql_query: Option<String>,
    query_id: String,
    rows_produced: u64,
    results_cache_hit: bool,

    // The datafusion instance and the internal table to route data to.
    df: Arc<RwLock<DataFusion>>,
    to_table: String,
}

impl Drop for QueryHistory {
    fn drop(&mut self) {
        let df = self.df.clone();

        tokio::spawn(async move {
            let mut df = df.write().await;
            
            let data_update = DataUpdate {
                schema: Arc::new(query_history_schema()),
                data: vec![self.into_record_batch()],
                update_type: crate::dataupdate::UpdateType::Append,
            };

            if let Err(e) = df.write_data(TableReference::partial("runtime", DEFAULT_QUERY_HISTORY_TABLE), data_update)
                .await
                .boxed()
                .context(UnableToWriteToTableSnafu) {
                tracing::error!("Error writing to query_history table: {}", e);
            }

            drop(df);
        });
    }
}

impl QueryHistory {
    pub fn builder() -> QueryHistoryBuilder {
        QueryHistoryBuilder::default()
    }

    pub fn into_record_batch(self) -> RecordBatch {
        
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(StringArray::from(vec![self.query_id])),
                Arc::new(StringArray::from(vec![self.schema.to_string()])),
                Arc::new(StringArray::from(vec![self.sql])),
                Arc::new(StringArray::from(vec![self.nsql_query])),
                Arc::new(Int64Array::from(vec![self.start_time.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as i64])),
                Arc::new(Int64Array::from(vec![self.end_time.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as i64])),
                Arc::new(UInt64Array::from(vec![self.execution_time])),
                Arc::new(UInt64Array::from(vec![self.rows_produced])),
                Arc::new(BooleanArray::from(vec![self.results_cache_hit])),
            ],
        )
        .unwrap()
    
    }
}

#[derive(Default)]
pub struct QueryHistoryBuilder {
    query_id: Option<String>,
    schema: Option<Arc<Schema>>,
    sql: Option<String>,
    nsql_query: Option<String>,
    start_time: Option<SystemTime>,
    end_time: Option<SystemTime>,
    execution_time: Option<u64>,
    rows_produced: Option<u64>,
    results_cache_hit: Option<bool>,

    df: Option<Arc<RwLock<DataFusion>>>,
    to_table: Option<String>,
}

pub async fn instantiate_query_history_table(df: Arc<RwLock<DataFusion>>,secret: Option<Secret>, cloud_dataset_path: Option<String>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {

    let time_column = Some("start_time".to_string());
    let time_format = Some(TimeFormat::UnixSeconds);

    let retention = Retention::new(
        time_column.clone(),
        time_format.clone(),
        Some(Duration::from_secs(3*24*60*60)), // 3 days
        Some(Duration::from_secs(300)),
        true,
    );

    let table = match cloud_dataset_path {
        Some(path) => {
            let refresh = Refresh {
                mode: RefreshMode::Append,
                time_column,
                time_format,
                check_interval: Some(Duration::from_secs(10)),
                period: Some(Duration::from_secs(1800)), // sync only last 30 minutes from cloud
                ..Default::default()
            };

            create_synced_internal_accelerated_table(
                DEFAULT_QUERY_HISTORY_TABLE,
                path.as_str(),
                secret,
                Acceleration::default(),
                refresh,
                retention,
            )
            .await
            .boxed()
            .context(UnableToRegisterTableSnafu)?
        }
        None => create_internal_accelerated_table(
            DEFAULT_QUERY_HISTORY_TABLE,
            Arc::new(query_history_schema()),
            Acceleration::default(),
            Refresh::default(),
            retention,
        )
        .await
        .boxed()
        .context(UnableToRegisterTableSnafu)?,
    };

    df
        .write()
        .await
        .register_runtime_table(DEFAULT_QUERY_HISTORY_TABLE, table)
        .boxed()
        .context(UnableToRegisterTableSnafu)?;

    Ok(())

}

pub fn query_history_schema() -> Schema {
    Schema::new(vec![
        Field::new("query_id", DataType::Utf8, false),
        Field::new("schema", DataType::Utf8, false),
        Field::new("sql", DataType::Utf8, false),
        Field::new("nsql_query", DataType::Utf8, true),
        Field::new("start_time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("end_time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("execution_time", DataType::UInt64, false),
        Field::new("rows_produced", DataType::UInt64, false),
        Field::new("results_cache_hit", DataType::Boolean, false),
    ])
}

impl QueryHistoryBuilder {
    pub fn query_id(mut self, query_id: String) -> Self {
        self.query_id = Some(query_id);
        self
    }

    pub fn schema(mut self, schema: Arc<Schema>) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn sql(mut self, sql: String) -> Self {
        self.sql = Some(sql);
        self
    }

    pub fn nsql_query(mut self, nsql_query: Option<String>) -> Self {
        self.nsql_query = nsql_query;
        self
    }

    pub fn start_time(mut self, start_time: SystemTime) -> Self {
        self.start_time = Some(start_time);
        self
    }

    pub fn end_time(mut self, end_time: SystemTime) -> Self {
        self.end_time = Some(end_time);
        self
    }

    pub fn execution_time(mut self, execution_time: u64) -> Self {
        self.execution_time = Some(execution_time);
        self
    }

    pub fn rows_produced(mut self, rows_produced: u64) -> Self {
        self.rows_produced = Some(rows_produced);
        self
    }

    pub fn results_cache_hit(mut self, results_cache_hit: bool) -> Self {
        self.results_cache_hit = Some(results_cache_hit);
        self
    }

    pub fn df(mut self, df: Arc<RwLock<DataFusion>>) -> Self {
        self.df = Some(df);
        self
    }

    pub fn to_table(mut self, to_table: String) -> Self {
        self.to_table = Some(to_table);
        self
    }

    pub fn build(self) -> Result<QueryHistory, &'static str> {
        Ok(QueryHistory {
            query_id: self.query_id.ok_or("query_id is required")?,
            schema: self.schema.ok_or("schema is required")?,
            sql: self.sql.ok_or("sql is required")?,
            nsql_query: self.nsql_query,
            start_time: self.start_time.ok_or("start_time is required")?,
            end_time: self.end_time.ok_or("end_time is required")?,
            execution_time: self.execution_time.ok_or("execution_time is required")?,
            rows_produced: self.rows_produced.ok_or("rows_produced is required")?,
            results_cache_hit: self.results_cache_hit.ok_or("results_cache_hit is required")?,
            df: self.df.ok_or("df is required")?,
            to_table: self.to_table.unwrap_or(DEFAULT_QUERY_HISTORY_TABLE.to_string()),
        })
    }
}