/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! A simple [`QueryEngine`] implementation backed by a bare [`SessionContext`].
//!
//! Useful for unit tests and lightweight scenarios that don't need the full
//! runtime `DataFusion` wrapper.

use std::fmt::Debug;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;

use crate::query_engine::{Error, QueryEngine, QueryRequest, Result, UpdateType};

/// A [`QueryEngine`] backed directly by a [`SessionContext`].
#[derive(Debug)]
pub struct QuerySession {
    ctx: Arc<SessionContext>,
}

impl QuerySession {
    /// Create a new `QuerySession` wrapping the given context.
    #[must_use]
    pub fn new(ctx: Arc<SessionContext>) -> Self {
        Self { ctx }
    }

    /// Create a new `QuerySession` with a default `SessionContext`.
    #[must_use]
    pub fn default_session() -> Self {
        Self {
            ctx: Arc::new(SessionContext::new()),
        }
    }
}

#[async_trait]
impl QueryEngine for QuerySession {
    fn session_context(&self) -> &Arc<SessionContext> {
        &self.ctx
    }

    async fn get_table(&self, table_ref: &TableReference) -> Option<Arc<dyn TableProvider>> {
        self.ctx.table_provider(table_ref.clone()).await.ok()
    }

    fn get_table_sync(&self, _table_ref: &TableReference) -> Option<Arc<dyn TableProvider>> {
        None // sync lookup not supported for bare SessionContext
    }

    fn table_exists(&self, table_ref: &TableReference) -> bool {
        self.ctx.table_exist(table_ref.clone()).unwrap_or(false)
    }

    async fn get_arrow_schema(&self, table_ref: TableReference) -> Result<Schema> {
        let provider = self
            .get_table(&table_ref)
            .await
            .ok_or_else(|| Error::GetSchema {
                table_ref: table_ref.to_string(),
                source: DataFusionError::Plan(format!("Table '{table_ref}' not found")),
            })?;
        Ok(provider.schema().as_ref().clone())
    }

    fn get_user_table_names(&self) -> Vec<TableReference> {
        self.ctx
            .catalog_names()
            .into_iter()
            .flat_map(|catalog| {
                let Some(cat) = self.ctx.catalog(&catalog) else {
                    return vec![];
                };
                cat.schema_names()
                    .into_iter()
                    .flat_map(|schema| {
                        let Some(sch) = cat.schema(&schema) else {
                            return vec![];
                        };
                        sch.table_names()
                            .into_iter()
                            .map(|table| TableReference::Full {
                                catalog: catalog.as_str().into(),
                                schema: schema.as_str().into(),
                                table: table.into(),
                            })
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn get_public_table_names(&self) -> Result<Vec<String>> {
        Ok(self
            .get_user_table_names()
            .into_iter()
            .map(|t| t.to_string())
            .collect())
    }

    fn is_writable(&self, _table_ref: &TableReference) -> bool {
        false
    }

    fn is_path_catalog_writable(&self, _table_ref: &TableReference) -> bool {
        false
    }

    async fn execute_query(&self, request: QueryRequest) -> Result<SendableRecordBatchStream> {
        let df = self
            .ctx
            .sql(&request.sql)
            .await
            .map_err(|source| Error::QueryExecution { source })?;
        df.execute_stream()
            .await
            .map_err(|source| Error::QueryExecution { source })
    }

    async fn execute_plan(&self, plan: LogicalPlan) -> Result<SendableRecordBatchStream> {
        let df = self
            .ctx
            .execute_logical_plan(plan)
            .await
            .map_err(|source| Error::QueryExecution { source })?;
        df.execute_stream()
            .await
            .map_err(|source| Error::QueryExecution { source })
    }

    async fn write_data(
        &self,
        table_ref: &TableReference,
        _schema: Arc<Schema>,
        _data: Vec<RecordBatch>,
        _update_type: UpdateType,
    ) -> Result<()> {
        Err(Error::WriteData {
            table_ref: table_ref.to_string(),
            source: DataFusionError::Plan("QuerySession does not support writes".to_string()),
        })
    }
}
