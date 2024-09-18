use arrow::datatypes::{Schema, SchemaRef};
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
use async_trait::async_trait;
use snafu::Snafu;
use tiberius::Client;
use tokio::net::TcpStream;

use crate::arrow::write::MemTable;

use datafusion::{
    catalog::Session,
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan, sql::TableReference,
};
use std::{any::Any, sync::Arc};

use tokio_util::compat::Compat;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error executing query: {source}"))]
    QueryError { source: tiberius::error::Error },
}

pub type SqlServerTcpClient = Client<Compat<TcpStream>>;

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct SqlServerTableProvider {
    client: Arc<SqlServerTcpClient>,
    schema: SchemaRef,
}

impl SqlServerTableProvider {
    pub async fn new(client: Arc<SqlServerTcpClient>, table: &TableReference) -> Result<Self> {

        let schema = Self::get_schema(Arc::clone(&client), table).await?;

        Ok(Self { client, schema })
    }

    pub async fn get_schema(client: Arc<SqlServerTcpClient>, _table: &TableReference) -> Result<SchemaRef> {
        Ok(Arc::new(Schema::empty()))
    }
}

#[async_trait]
impl TableProvider for SqlServerTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> std::result::Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let table = MemTable::try_new(Arc::clone(&self.schema), vec![])?;
        table.scan(state, projection, filters, limit).await
    }
}
