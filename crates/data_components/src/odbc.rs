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
use datafusion::{
    datasource::TableProvider,
    sql::{unparser::dialect::Dialect, TableReference},
};
use datafusion_table_providers::sql::{
    db_connection_pool as db_connection_pool_datafusion,
    sql_provider_datafusion::{
        expr::{self, Engine},
        SqlTable,
    },
};
use db_connection_pool::dbconnection::odbcconn::ODBCDbConnectionPool;
use snafu::prelude::*;
use std::sync::Arc;

use crate::Read;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DbConnectionError: {source}"))]
    DbConnectionError {
        source: db_connection_pool_datafusion::dbconnection::GenericError,
    },
    #[snafu(display("The table '{table_name}' doesn't exist in the Postgres server"))]
    TableDoesntExist { table_name: String },

    #[snafu(display("Unable to get a DB connection from the pool: {source}"))]
    UnableToGetConnectionFromPool {
        source: db_connection_pool_datafusion::Error,
    },

    #[snafu(display("Unable to get schema: {source}"))]
    UnableToGetSchema {
        source: db_connection_pool_datafusion::dbconnection::Error,
    },

    #[snafu(display("Unable to generate SQL: {source}"))]
    UnableToGenerateSQL { source: expr::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct ODBCTableFactory<'a> {
    pool: Arc<ODBCDbConnectionPool<'a>>,
    dialect: Option<Arc<dyn Dialect + Send + Sync>>,
}

impl<'a> ODBCTableFactory<'a>
where
    'a: 'static,
{
    #[must_use]
    pub fn new(
        pool: Arc<ODBCDbConnectionPool<'a>>,
        dialect: Option<Arc<dyn Dialect + Send + Sync>>,
    ) -> Self {
        Self { pool, dialect }
    }
}

#[async_trait]
impl<'a> Read for ODBCTableFactory<'a>
where
    'a: 'static,
{
    async fn table_provider(
        &self,
        table_reference: TableReference,
        _schema: Option<SchemaRef>,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let pool = Arc::clone(&self.pool);
        let dyn_pool: Arc<ODBCDbConnectionPool<'a>> = pool;

        let table = SqlTable::new("odbc", &dyn_pool, table_reference, Some(Engine::ODBC))
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let table = if let Some(dialect) = &self.dialect {
            table.with_dialect(Arc::clone(dialect))
        } else {
            table
        };

        let table_provider = Arc::new(table);

        let table_provider = Arc::new(
            table_provider
                .create_federated_table_provider()
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        );

        Ok(table_provider)
    }
}
