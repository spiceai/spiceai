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

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use clickhouse_rs::ClientHandle;
use datafusion::{datasource::TableProvider, sql::TableReference};
use datafusion_table_providers::sql::{
    db_connection_pool::DbConnectionPool,
    sql_provider_datafusion::{self, SqlTable},
};
use snafu::prelude::*;
use std::sync::Arc;

use crate::Read;

pub type ClickhouseConnectionPool =
    dyn DbConnectionPool<ClientHandle, &'static (dyn Sync)> + Send + Sync;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to construct SQL table: {source}"))]
    UnableToConstructSQLTable {
        source: sql_provider_datafusion::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct ClickhouseTableFactory {
    pool: Arc<ClickhouseConnectionPool>,
}

impl ClickhouseTableFactory {
    #[must_use]
    pub fn new(pool: Arc<ClickhouseConnectionPool>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl Read for ClickhouseTableFactory {
    async fn table_provider(
        &self,
        table_reference: TableReference,
        schema: Option<SchemaRef>,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let pool = Arc::clone(&self.pool);
        let table_provider = match schema {
            Some(schema) => Arc::new(SqlTable::new_with_schema(
                "clickhouse",
                &pool,
                schema,
                table_reference,
                None,
            )),
            None => Arc::new(
                SqlTable::new("clickhouse", &pool, table_reference, None)
                    .await
                    .context(UnableToConstructSQLTableSnafu)?,
            ),
        };

        let table_provider = Arc::new(
            table_provider
                .create_federated_table_provider()
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        );

        Ok(table_provider)
    }
}
