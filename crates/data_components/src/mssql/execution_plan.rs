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

use crate::mssql::{ConnectionPoolSnafu, QuerySnafu, convert::rows_to_arrow};
use arrow::datatypes::SchemaRef;
use datafusion::{
    common::utils::quote_identifier,
    error::{DataFusionError, Result as DataFusionResult},
    execution::TaskContext,
    logical_expr::Expr,
    physical_expr::{EquivalenceProperties, LexOrdering, PhysicalSortExpr, expressions::Column},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream, SortOrderPushdownResult,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
    sql::{TableReference, unparser::Unparser},
};
use futures::StreamExt;
use snafu::ResultExt;

use super::{connection_manager::SqlServerConnectionPool, dialect::MsSqlDialect};

pub type Result<T, E = super::Error> = std::result::Result<T, E>;

use async_stream::try_stream;

#[derive(Clone)]
pub struct SqlServerExecPlan {
    projected_schema: SchemaRef,
    table_reference: TableReference,
    pool: Arc<SqlServerConnectionPool>,
    filters: Vec<Expr>,
    limit: Option<usize>,
    sort_exprs: Vec<PhysicalSortExpr>,
    properties: Arc<PlanProperties>,
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
            sort_exprs: Vec::new(),
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(projected_schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        })
    }

    pub fn sql(&self) -> DataFusionResult<String> {
        let columns = self
            .projected_schema
            .fields()
            .iter()
            .map(|f| quote_identifier(f.name()))
            .collect::<Vec<_>>()
            .join(", ");

        let top_expr = match self.limit {
            Some(limit) => format!("TOP {limit} "),
            None => String::new(),
        };

        let dialect = MsSqlDialect::new();

        let where_expr = if self.filters.is_empty() {
            String::new()
        } else {
            let filter_expr = self
                .filters
                .iter()
                .map(|f| {
                    Unparser::new(&dialect)
                        .expr_to_sql(f)
                        .map(|e| format!("({e})"))
                })
                .collect::<DataFusionResult<Vec<String>>>()?
                .join(" AND ");
            format!("WHERE {filter_expr}")
        };

        let order_expr = if self.sort_exprs.is_empty() {
            String::new()
        } else {
            let sort_terms: DataFusionResult<Vec<String>> = self
                .sort_exprs
                .iter()
                .map(|sort| {
                    let col = sort.expr.as_any().downcast_ref::<Column>().ok_or_else(|| {
                        DataFusionError::Internal(
                            "Sort pushdown contains non-column expressions".to_string(),
                        )
                    })?;
                    let dir = if sort.options.descending {
                        "DESC"
                    } else {
                        "ASC"
                    };
                    Ok(format!("{} {dir}", quote_identifier(col.name())))
                })
                .collect();
            format!("ORDER BY {}", sort_terms?.join(", "))
        };

        let mut sql = format!(
            "SELECT {top_expr}{columns} FROM {table_reference}",
            table_reference = self.table_reference.to_quoted_string()
        );
        if !where_expr.is_empty() {
            sql.push(' ');
            sql.push_str(&where_expr);
        }
        if !order_expr.is_empty() {
            sql.push(' ');
            sql.push_str(&order_expr);
        }

        Ok(sql)
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

    fn properties(&self) -> &Arc<PlanProperties> {
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

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn fetch(&self) -> Option<usize> {
        self.limit
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        Some(Arc::new(SqlServerExecPlan {
            projected_schema: Arc::clone(&self.projected_schema),
            table_reference: self.table_reference.clone(),
            pool: Arc::clone(&self.pool),
            filters: self.filters.clone(),
            limit,
            sort_exprs: self.sort_exprs.clone(),
            properties: Arc::clone(&self.properties),
        }))
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
    ) -> DataFusionResult<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        // MSSQL treats NULLs as smallest: ASC => nulls first, DESC => nulls last.
        // We can return Exact when the requested null ordering either:
        //   (a) matches MSSQL's native behavior, or
        //   (b) is irrelevant because the field is not nullable.
        let mut nulls_match_native = true;
        for sort_expr in order {
            // Only support simple column references
            let Some(col) = sort_expr.expr.as_any().downcast_ref::<Column>() else {
                return Ok(SortOrderPushdownResult::Unsupported);
            };

            // If the field is not nullable, null ordering is irrelevant — always Exact.
            let is_nullable = self
                .projected_schema
                .field_with_name(col.name())
                .map(arrow::datatypes::Field::is_nullable)
                .unwrap_or(true);
            if !is_nullable {
                continue;
            }

            // For nullable fields, check if the requested ordering matches MSSQL's native behavior.
            let expected_nulls_first = !sort_expr.options.descending;
            if sort_expr.options.nulls_first != expected_nulls_first {
                nulls_match_native = false;
            }
        }

        let sort_exprs = order.to_vec();

        // Build equivalence properties reflecting MSSQL's actual null ordering behavior,
        // not the requested ordering. MSSQL always sorts NULLs as smallest (ASC => nulls
        // first, DESC => nulls last) regardless of what the user requested.
        let native_sort_exprs: Vec<PhysicalSortExpr> = sort_exprs
            .iter()
            .map(|expr| PhysicalSortExpr {
                expr: Arc::clone(&expr.expr),
                options: datafusion::arrow::compute::SortOptions {
                    descending: expr.options.descending,
                    nulls_first: !expr.options.descending,
                },
            })
            .collect();
        let mut eq_properties = EquivalenceProperties::new(Arc::clone(&self.projected_schema));
        if let Some(ordering) = LexOrdering::new(native_sort_exprs) {
            eq_properties.add_orderings([ordering]);
        }

        let new_plan = SqlServerExecPlan {
            projected_schema: Arc::clone(&self.projected_schema),
            table_reference: self.table_reference.clone(),
            pool: Arc::clone(&self.pool),
            filters: self.filters.clone(),
            limit: self.limit,
            sort_exprs,
            properties: Arc::new(PlanProperties::new(
                eq_properties,
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        };

        let inner = Arc::new(new_plan) as Arc<dyn ExecutionPlan>;
        if nulls_match_native {
            Ok(SortOrderPushdownResult::Exact { inner })
        } else {
            // MSSQL can't express NULLS FIRST/LAST, so the sort is pushed down
            // for performance but DataFusion will add a verification sort for
            // correct null ordering.
            Ok(SortOrderPushdownResult::Inexact { inner })
        }
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

pub fn to_execution_error(
    e: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> DataFusionError {
    DataFusionError::Execution(format!("{}", e.into()))
}

fn to_datafusion_err(e: super::Error) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::External(Box::new(e))
}
