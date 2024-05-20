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

#![allow(clippy::module_name_repetitions)]
use async_trait::async_trait;
use datafusion::{datasource::TableProvider, sql::TableReference};
use db_connection_pool::DbConnectionPool;
use mysql_async::prelude::ToValue;
use snafu::prelude::*;
use sql_provider_datafusion::SqlTable;
use std::sync::Arc;

use crate::Read;

// use self::write::PostgresTableWriter;

// pub mod write;

pub type MySQLConnectionPool =
    dyn DbConnectionPool<mysql_async::Conn, &'static (dyn ToValue + Sync)> + Send + Sync;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to construct SQL table: {source}"))]
    UnableToConstructSQLTable {
        source: sql_provider_datafusion::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct MySQLTableFactory {
    pool: Arc<MySQLConnectionPool>,
}

impl MySQLTableFactory {
    #[must_use]
    pub fn new(pool: Arc<MySQLConnectionPool>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl Read for MySQLTableFactory {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let pool = Arc::clone(&self.pool);
        let table_provider = Arc::new(
            SqlTable::new("mysql", &pool, table_reference, None)
                .await
                .context(UnableToConstructSQLTableSnafu)?,
        );

        let table_provider = Arc::new(
            table_provider
                .create_federated_table_provider()
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        );

        Ok(table_provider)
    }
}
