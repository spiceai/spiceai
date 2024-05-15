#![allow(clippy::module_name_repetitions)]
use async_trait::async_trait;
use datafusion::{datasource::TableProvider, sql::TableReference};
use db_connection_pool::DbConnectionPool;
use snafu::prelude::*;
use snowflake_api::SnowflakeApi;
use sql_provider_datafusion::SqlTable;
use std::sync::Arc;

use crate::Read;

pub type SnowflakeConnectionPool =
    dyn DbConnectionPool<Arc<SnowflakeApi>, &'static (dyn Sync)> + Send + Sync;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to construct SQL table: {source}"))]
    UnableToConstructSQLTable {
        source: sql_provider_datafusion::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct SnowflakeTableFactory {
    pool: Arc<SnowflakeConnectionPool>,
}

impl SnowflakeTableFactory {
    #[must_use]
    pub fn new(pool: Arc<SnowflakeConnectionPool>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl Read for SnowflakeTableFactory {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let pool = Arc::clone(&self.pool);
        let table_provider = SqlTable::new("snowflake", &pool, table_reference, None)
            .await
            .context(UnableToConstructSQLTableSnafu)?;

        Ok(Arc::new(table_provider))
    }
}
