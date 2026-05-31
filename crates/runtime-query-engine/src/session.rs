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

use std::{fmt::Debug, sync::Arc};

use arrow::record_batch::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use datafusion::{
    catalog::TableProvider,
    common::DataFusionError,
    execution::{SendableRecordBatchStream, context::SQLOptions},
    logical_expr::LogicalPlan,
    physical_plan::{ExecutionPlan, collect},
    prelude::SessionContext,
    sql::TableReference,
};
use snafu::ResultExt;

use crate::query_engine::{
    Error, GetSchemaSnafu, QueryEngine, QueryExecutionSnafu, QueryRequest, Result, UpdateType,
    WriteDataSnafu,
};

pub struct QuerySession {
    ctx: Arc<SessionContext>,
}

impl Debug for QuerySession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuerySession").finish_non_exhaustive()
    }
}

impl QuerySession {
    pub fn new(ctx: Arc<SessionContext>) -> Self {
        Self { ctx }
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
        None
    }

    fn table_exists(&self, table_ref: &TableReference) -> bool {
        self.ctx.table_exist(table_ref.clone()).unwrap_or(false)
    }

    async fn get_arrow_schema(&self, table_ref: TableReference) -> Result<Schema> {
        let tbl = self
            .ctx
            .table_provider(table_ref.clone())
            .await
            .context(GetSchemaSnafu {
                table_ref: table_ref.to_string(),
            })?;
        Ok(Arc::unwrap_or_clone(tbl.schema()))
    }

    fn get_user_table_names(&self) -> Vec<TableReference> {
        self.ctx
            .catalog_names()
            .iter()
            .flat_map(|catalog| {
                let Some(cat) = self.ctx.catalog(catalog) else {
                    return vec![];
                };
                cat.schema_names()
                    .iter()
                    .flat_map(|schema| {
                        cat.schema(schema)
                            .map(|s| {
                                s.table_names()
                                    .into_iter()
                                    .map(|t| {
                                        TableReference::full(
                                            Arc::from(catalog.clone()),
                                            Arc::from(schema.clone()),
                                            Arc::from(t),
                                        )
                                    })
                                    .collect::<Vec<_>>()
                            })
                            .unwrap_or_default()
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn get_public_table_names(&self) -> Result<Vec<String>> {
        let default_catalog =
            self.ctx
                .catalog_names()
                .into_iter()
                .next()
                .ok_or(Error::GetTableNames {
                    source: DataFusionError::Internal("No catalogs registered".to_string()),
                })?;
        let catalog = self
            .ctx
            .catalog(&default_catalog)
            .ok_or(Error::GetTableNames {
                source: DataFusionError::Internal(format!("Catalog '{default_catalog}' not found")),
            })?;
        let default_schema =
            catalog
                .schema_names()
                .into_iter()
                .next()
                .ok_or(Error::GetTableNames {
                    source: DataFusionError::Internal(format!(
                        "No schemas in catalog '{default_catalog}'"
                    )),
                })?;
        let schema = catalog
            .schema(&default_schema)
            .ok_or_else(|| Error::GetTableNames {
                source: DataFusionError::Internal(format!(
                    "Schema '{default_schema}' not found in catalog '{default_catalog}'"
                )),
            })?;
        Ok(schema.table_names())
    }

    fn is_writable(&self, _table_ref: &TableReference) -> bool {
        false
    }

    fn is_path_catalog_writable(&self, _table_ref: &TableReference) -> bool {
        false
    }

    async fn execute_query(&self, request: QueryRequest) -> Result<SendableRecordBatchStream> {
        let options = if request.read_only {
            SQLOptions::new()
                .with_allow_ddl(false)
                .with_allow_dml(false)
                .with_allow_statements(false)
        } else {
            SQLOptions::new()
        };

        let plan = self
            .ctx
            .state()
            .create_logical_plan(&request.sql)
            .await
            .context(QueryExecutionSnafu)?;

        options.verify_plan(&plan).context(QueryExecutionSnafu)?;

        let plan = if let Some(params) = request.parameters {
            plan.with_param_values(params)
                .context(QueryExecutionSnafu)?
        } else {
            plan
        };

        self.ctx
            .execute_logical_plan(plan)
            .await
            .context(QueryExecutionSnafu)?
            .execute_stream()
            .await
            .context(QueryExecutionSnafu)
    }

    async fn execute_plan(&self, plan: LogicalPlan) -> Result<SendableRecordBatchStream> {
        self.ctx
            .execute_logical_plan(plan)
            .await
            .context(QueryExecutionSnafu)?
            .execute_stream()
            .await
            .context(QueryExecutionSnafu)
    }

    async fn write_data(
        &self,
        table_ref: &TableReference,
        schema: Arc<Schema>,
        data: Vec<RecordBatch>,
        update_type: UpdateType,
    ) -> Result<()> {
        let provider =
            self.ctx
                .table_provider(table_ref.clone())
                .await
                .context(WriteDataSnafu {
                    table_ref: table_ref.to_string(),
                })?;

        let insert_op = match update_type {
            UpdateType::Append => datafusion::logical_expr::dml::InsertOp::Append,
            UpdateType::Overwrite => datafusion::logical_expr::dml::InsertOp::Overwrite,
            UpdateType::Changes => datafusion::logical_expr::dml::InsertOp::Replace,
        };

        let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![data]).context(
            WriteDataSnafu {
                table_ref: table_ref.to_string(),
            },
        )?;

        let scan = mem_table
            .scan(&self.ctx.state(), None, &[], None)
            .await
            .context(WriteDataSnafu {
                table_ref: table_ref.to_string(),
            })?;

        let insert_plan = provider
            .insert_into(&self.ctx.state(), scan, insert_op)
            .await
            .context(WriteDataSnafu {
                table_ref: table_ref.to_string(),
            })?;

        collect(insert_plan, self.ctx.task_ctx())
            .await
            .context(WriteDataSnafu {
                table_ref: table_ref.to_string(),
            })?;

        Ok(())
    }
}
