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

use std::{any::Any, fmt, sync::Arc};

use crate::mssql::{convert::rows_to_arrow, ConnectionPoolSnafu, QuerySnafu};
use arrow::datatypes::SchemaRef;
use datafusion::{
    error::{DataFusionError, Result as DataFusionResult},
    execution::TaskContext,
    logical_expr::Expr,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionMode,
        ExecutionPlan, Partitioning, PlanProperties, SendableRecordBatchStream,
    },
    sql::{
        sqlparser::ast::DataType,
        unparser::{
            dialect::{CustomDialect, CustomDialectBuilder},
            Unparser,
        },
        TableReference,
    },
};
use futures::StreamExt;
use snafu::ResultExt;

use super::connection_manager::SqlServerConnectionPool;

pub type Result<T, E = super::Error> = std::result::Result<T, E>;

use async_stream::try_stream;

#[derive(Clone)]
pub struct SqlServerExecPlan {
    projected_schema: SchemaRef,
    table_reference: TableReference,
    pool: Arc<SqlServerConnectionPool>,
    filters: Vec<Expr>,
    limit: Option<usize>,
    properties: PlanProperties,
}

pub fn project_schema_safe(
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> DataFusionResult<SchemaRef> {
    let schema = match projection {
        Some(columns) => {
            if columns.is_empty() {
                Arc::clone(schema)
            } else {
                Arc::new(schema.project(columns)?)
            }
        }
        None => Arc::clone(schema),
    };
    Ok(schema)
}

impl SqlServerExecPlan {
    pub fn new(
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        table_reference: &TableReference,
        pool: Arc<SqlServerConnectionPool>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Self> {
        let projected_schema = project_schema_safe(schema, projections)?;

        Ok(Self {
            projected_schema: Arc::clone(&projected_schema),
            table_reference: table_reference.clone(),
            pool,
            filters: filters.to_vec(),
            limit,
            properties: PlanProperties::new(
                EquivalenceProperties::new(projected_schema),
                Partitioning::UnknownPartitioning(1),
                ExecutionMode::Bounded,
            ),
        })
    }

    fn dialect() -> CustomDialect {
        CustomDialectBuilder::new()
            .with_float64_ast_dtype(DataType::Float(None))
            .build()
    }

    pub fn sql(&self) -> DataFusionResult<String> {
        let columns = self
            .projected_schema
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect::<Vec<_>>()
            .join(", ");

        let top_expr = match self.limit {
            Some(limit) => format!("TOP {limit} "),
            None => String::new(),
        };

        let dialect = SqlServerExecPlan::dialect();

        let where_expr = if self.filters.is_empty() {
            String::new()
        } else {
            let filter_expr = self
                .filters
                .iter()
                .map(|f| {
                    Unparser::new(&dialect)
                        .expr_to_sql(f)
                        .map(|e| e.to_string())
                })
                .collect::<DataFusionResult<Vec<String>>>()?
                .join(" AND ");
            format!("WHERE {filter_expr}")
        };

        Ok(format!(
            "SELECT {top_expr}{columns} FROM {table_reference} {where_expr}",
            table_reference = self.table_reference
        ))
    }
}

impl std::fmt::Debug for SqlServerExecPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "SqlServerExec sql={sql}")
    }
}

impl DisplayAs for SqlServerExecPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "SqlServerExec sql={sql}")
    }
}

impl ExecutionPlan for SqlServerExecPlan {
    fn name(&self) -> &'static str {
        "SqlServerExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let sql = self.sql().map_err(to_execution_error)?;
        tracing::debug!("SqlServerExecPlan sql: {sql}");

        let schema = self.schema();

        Ok(query_arrow(Arc::clone(&self.pool), sql, &schema))
    }
}

fn query_arrow(
    pool: Arc<SqlServerConnectionPool>,
    sql: String,
    projected_schema: &SchemaRef,
) -> SendableRecordBatchStream {
    tracing::debug!("Executing sql: {sql}");

    let schema = Arc::clone(projected_schema);

    let stream = try_stream! {
        let mut conn = pool.get().await.boxed().context(ConnectionPoolSnafu).map_err(to_datafusion_err)?;
        let query_res = conn
            .simple_query(sql)
            .await
            .boxed()
            .context(ConnectionPoolSnafu)
            .map_err(to_datafusion_err)?
            .into_row_stream();

        let mut chunked_stream = query_res.chunks(4_000).boxed();

        while let Some(chunk) = chunked_stream.next().await {
            let rows = chunk
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .context(QuerySnafu)
                .map_err(to_datafusion_err)?;

            yield rows_to_arrow(&rows, &schema)
                .map_err(to_datafusion_err)?;
        }
    };

    Box::pin(RecordBatchStreamAdapter::new(
        Arc::clone(projected_schema),
        Box::pin(stream),
    )) as SendableRecordBatchStream
}

#[allow(clippy::needless_pass_by_value)]
pub fn to_execution_error(
    e: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> DataFusionError {
    DataFusionError::Execution(format!("{}", e.into()).to_string())
}

fn to_datafusion_err(e: super::Error) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::External(Box::new(e))
}
