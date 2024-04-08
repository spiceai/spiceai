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

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    common::OwnedTableReference,
    datasource::{provider::TableProviderFactory, MemTable, TableProvider},
    execution::context::SessionState,
    logical_expr::CreateExternalTable,
};
use db_connection_pool::{duckdbpool::DuckDbConnectionPool, DbConnectionPool};
use duckdb::{DuckdbConnectionManager, ToSql};
use snafu::prelude::*;
use std::sync::Arc;

use crate::{Read, ReadWrite};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DbConnectionError: {source}"))]
    DbConnectionError {
        source: db_connection_pool::dbconnection::GenericError,
    },

    #[snafu(display("DbConnectionPoolError: {source}"))]
    DbConnectionPoolError { source: db_connection_pool::Error },

    // #[snafu(display("DuckDBError: {source}"))]
    // DuckDB { source: duckdb::Error },
    #[snafu(display("DuckDBDataFusionError: {source}"))]
    DuckDBDataFusion {
        source: sql_provider_datafusion::Error,
    },

    #[snafu(display("DataFusionError: {source}"))]
    DataFusion {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Lock is poisoned: {message}"))]
    LockPoisoned { message: String },

    #[snafu(display("Unable to downcast DbConnection to DuckDbConnection"))]
    UnableToDowncastDbConnection {},
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct DuckDBTableFactory {}

impl TableProviderFactory for DuckDBTableFactory {
    async fn create(
        &self,
        state: &SessionState,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        let name = cmd.name.to_string();
        let mode = cmd.options.remove("mode").unwrap_or_default();

        let pool =
            DuckDbConnectionPool::new(&name, mode, &cmd.options).context(DbConnectionPoolSnafu)?;
    }
}

pub struct DuckDB {
    pool: Arc<
        dyn DbConnectionPool<r2d2::PooledConnection<DuckdbConnectionManager>, &'static dyn ToSql>
            + Send
            + Sync,
    >,
}

impl DuckDB {
    pub fn new(schema: SchemaRef) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            memtable: Arc::new(MemTable::try_new(schema, vec![])?),
        })
    }
}

#[async_trait]
impl Read for DuckDB {
    async fn table_provider(
        &self,
        _table_reference: OwnedTableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Arc::clone(&self.memtable))
    }
}

#[async_trait]
impl ReadWrite for DuckDB {
    async fn table_provider(
        &self,
        _table_reference: OwnedTableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Arc::clone(&self.memtable))
    }
}
