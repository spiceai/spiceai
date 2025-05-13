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

use crate::Read;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::sql::TableReference;
use datafusion::{datasource::TableProvider, execution::SendableRecordBatchStream};
use datafusion_table_providers::sql::{
    db_connection_pool::{
        DbConnectionPool, JoinPushDown,
        dbconnection::{self, AsyncDbConnection, DbConnection},
    },
    sql_provider_datafusion::SqlTable,
};
use reqwest::{Client, ClientBuilder};
use snafu::prelude::*;
use std::{any::Any, sync::Arc};

pub struct DatabricksSqlWarehouse {
    pool: Arc<dyn DbConnectionPool<Arc<SqlWarehouseApi>, &'static (dyn Sync)> + Send + Sync>,
}

#[derive(Debug, Snafu)]
pub enum Error {}

impl DatabricksSqlWarehouse {
    pub fn new(// endpoint: Endpoint,
        // storage_options: HashMap<String, SecretString>,
        // token_provider: Arc<dyn TokenProvider>,
    ) -> Self {
        let client = ClientBuilder::new()
            .user_agent(super::user_agent())
            .build()
            .unwrap();
        let api = Arc::new(SqlWarehouseApi { client });
        let pool = Arc::new(SqlWarehouseConnectionPool { api });
        Self { pool }
    }
}

struct SqlWarehouseConnectionPool {
    api: Arc<SqlWarehouseApi>,
}

#[async_trait]
impl DbConnectionPool<Arc<SqlWarehouseApi>, &'static (dyn Sync)> for SqlWarehouseConnectionPool {
    async fn connect(
        &self,
    ) -> Result<
        Box<dyn DbConnection<Arc<SqlWarehouseApi>, &'static (dyn Sync)>>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        todo!()
    }

    fn join_push_down(&self) -> JoinPushDown {
        // TODO: allow?
        JoinPushDown::Disallow
    }
}

struct SqlWarehouseApi {
    client: Client,
}

struct SqlWarehouseConnection {
    api: Arc<SqlWarehouseApi>,
}

impl<'a> DbConnection<Arc<SqlWarehouseApi>, &'a (dyn Sync)> for SqlWarehouseConnection {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_async(&self) -> Option<&dyn AsyncDbConnection<Arc<SqlWarehouseApi>, &'a (dyn Sync)>> {
        Some(self)
    }
}

#[async_trait]
impl<'a> AsyncDbConnection<Arc<SqlWarehouseApi>, &'a (dyn Sync)> for SqlWarehouseConnection {
    fn new(api: Arc<SqlWarehouseApi>) -> Self {
        Self { api }
    }

    async fn get_schema(
        &self,
        table_reference: &TableReference,
    ) -> Result<SchemaRef, dbconnection::Error> {
        todo!()
    }

    async fn query_arrow(
        &self,
        sql: &str,
        _: &[&'a (dyn Sync)],
        _projected_schema: Option<SchemaRef>,
    ) -> Result<SendableRecordBatchStream, Box<dyn std::error::Error + Send + Sync>> {
        todo!()
    }

    async fn execute(
        &self,
        _query: &str,
        _: &[&'a (dyn Sync)],
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        todo!()
    }
}

#[async_trait]
impl Read for DatabricksSqlWarehouse {
    async fn table_provider(
        &self,
        table_reference: TableReference,
        schema: Option<SchemaRef>,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let pool = Arc::clone(&self.pool);
        let table_provider = match schema {
            Some(schema) => Arc::new(SqlTable::new_with_schema(
                "databricks",
                &pool,
                schema,
                table_reference,
                None,
            )),
            None => Arc::new(SqlTable::new("databricks", &pool, table_reference, None).await?),
        };

        let table_provider = Arc::new(
            table_provider
                .create_federated_table_provider()
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        );

        Ok(table_provider)
    }
}
