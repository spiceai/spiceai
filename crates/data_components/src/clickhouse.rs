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

use async_trait::async_trait;
use clickhouse_rs::ClientHandle;
use datafusion::{datasource::TableProvider, sql::TableReference};
use datafusion_table_providers::sql::{
    db_connection_pool::DbConnectionPool,
    sql_provider_datafusion::{self, SqlTable},
};
use crate::function_support::FunctionSupport;
use snafu::prelude::*;
use std::sync::Arc;

use crate::Read;
use crate::federation::create_spice_federated_table_provider;

pub type ClickhouseConnectionPool =
    dyn DbConnectionPool<ClientHandle, &'static dyn Sync> + Send + Sync;

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
    function_support: Option<FunctionSupport>,
}

impl std::fmt::Debug for ClickhouseTableFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickhouseTableFactory")
            .finish_non_exhaustive()
    }
}

impl ClickhouseTableFactory {
    #[must_use]
    pub fn new(pool: Arc<ClickhouseConnectionPool>) -> Self {
        Self {
            pool,
            function_support: None,
        }
    }

    /// Install the federation function deny-list. Functions on the list (Spice-only
    /// UDFs such as `json_get_str`) are evaluated locally instead of being pushed
    /// into the SQL sent to `ClickHouse`. Without this, `ClickHouse`'s `SqlTable`
    /// inherits the upstream `can_execute_plan` default (always federate) and the
    /// remote engine rejects the unknown function. See issue #10703.
    #[must_use]
    pub fn with_function_support(mut self, function_support: FunctionSupport) -> Self {
        self.function_support = Some(function_support);
        self
    }
}

#[async_trait]
impl Read for ClickhouseTableFactory {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let pool = Arc::clone(&self.pool);
        let sql_table = Arc::new(
            SqlTable::new("clickhouse", &pool, table_reference.clone())
                .await
                .context(UnableToConstructSQLTableSnafu)?,
        );

        let schema = sql_table.schema();
        let table_provider = Arc::new(create_spice_federated_table_provider(
            sql_table,
            schema,
            table_reference,
            self.function_support.clone(),
        ));

        Ok(table_provider)
    }
}
