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
use connection_manager::SqlServerConnectionPool;
use convert::{map_column_type_to_arrow_type, map_type_name_to_column_type};
use execution_plan::SqlServerExecPlan;
use futures::StreamExt;
use snafu::{OptionExt, ResultExt, Snafu};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::{
    catalog::Session,
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    logical_expr::{Operator, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
    prelude::Expr,
    sql::TableReference,
};

use std::{any::Any, sync::Arc};
pub mod connection_manager;
mod convert;
mod execution_plan;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error executing query: {source}"))]
    QueryError { source: tiberius::error::Error },

    #[snafu(display("Unable to connect: {source}"))]
    SqlServerAccessError { source: tiberius::error::Error },

    #[snafu(display("Unable to connect: {source}"))]
    ConnectionPoolError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to retrieve table schema"))]
    SchemaRetrieval,

    #[snafu(display("Unable to retrieve schema: table '{table}' does not exist"))]
    SchemaRetrievalTableNotFound { table: String },

    #[snafu(display("Unsupported data type: {data_type}"))]
    UnsupportedType { data_type: String },

    #[snafu(display("Failed to build record batch: {source}"))]
    FailedToBuildRecordBatch { source: arrow::error::ArrowError },

    #[snafu(display("No builder found for index {index}"))]
    NoBuilderForIndex { index: usize },

    #[snafu(display("Failed to downcast builder for {mssql_type}"))]
    FailedToDowncastBuilder { mssql_type: String },

    #[snafu(display("Unable to generate SQL: {source}"))]
    UnableToGenerateSQL { source: DataFusionError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct SqlServerTableProvider {
    conn: Arc<SqlServerConnectionPool>,
    schema: SchemaRef,
    table: TableReference,
}

impl SqlServerTableProvider {
    pub async fn new(conn: Arc<SqlServerConnectionPool>, table: &TableReference) -> Result<Self> {
        let schema = Self::get_schema(Arc::clone(&conn), table).await?;

        Ok(Self {
            conn,
            schema,
            table: table.clone(),
        })
    }

    pub async fn get_schema(
        conn: Arc<SqlServerConnectionPool>,
        table: &TableReference,
    ) -> Result<SchemaRef> {
        let table_name = table.table();
        let table_schema = table.schema().unwrap_or("dbo");

        let columns_meta_query: String = format!(
            "SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS \
            WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{table_schema}'"
        );
        tracing::debug!("Executing schema query for dataset {table_name}: {columns_meta_query}");

        let mut conn = conn.get().await.boxed().context(ConnectionPoolSnafu)?;

        let mut query_res = conn
            .simple_query(columns_meta_query)
            .await
            .context(QuerySnafu)?
            .into_row_stream();

        let mut fields = Vec::new();

        while let Some(row_result) = query_res.next().await {
            let row = row_result.context(QuerySnafu)?;

            let column_name: &str = row.get(0).context(SchemaRetrievalSnafu)?;
            let data_type: &str = row.get(1).context(SchemaRetrievalSnafu)?;
            let numeric_precision: Option<u8> = row.get(2);
            let numeric_scale: Option<i32> = row.get(3);

            let Some(column_type) = map_type_name_to_column_type(data_type) else {
                tracing::warn!("Column '{column_name}' of table '{table_name}' has unsupported data type '{data_type}' and will be ignored");
                continue;
            };

            let arrow_data_type = map_column_type_to_arrow_type(
                column_type,
                numeric_precision,
                numeric_scale.map(|v| i8::try_from(v).unwrap_or_default()),
            );

            fields.push(Field::new(column_name, arrow_data_type, true));
        }

        if fields.is_empty() {
            return Err(Error::SchemaRetrievalTableNotFound {
                table: table.to_string(),
            });
        }

        tracing::trace!("Retrieved dataset {table_name} schema: {fields:?}");

        Ok(Arc::new(Schema::new(fields)))
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
        let mut results = Vec::with_capacity(filters.len());

        for filter in filters {
            match filter {
                Expr::BinaryExpr(binary_expr) => match binary_expr.op {
                    Operator::Eq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq => {
                        if is_time_related_expr(&binary_expr.left)
                            || is_time_related_expr(&binary_expr.right)
                        {
                            results.push(TableProviderFilterPushDown::Unsupported);
                        } else {
                            results.push(TableProviderFilterPushDown::Exact);
                        }
                    }
                    _ => results.push(TableProviderFilterPushDown::Unsupported),
                },
                _ => results.push(TableProviderFilterPushDown::Unsupported),
            }
        }
        Ok(results)
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(SqlServerExecPlan::new(
            projection,
            &self.schema,
            &self.table,
            Arc::clone(&self.conn),
            filters,
            limit,
        )?))
    }
}

pub fn project_schema(
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> datafusion::error::Result<SchemaRef> {
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

fn is_time_related_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Cast(cast) => {
            matches!(
                cast.data_type,
                DataType::Time32(_) | DataType::Time64(_) | DataType::Timestamp(_, _)
            )
        }
        Expr::Literal(literal) => {
            matches!(
                literal.data_type(),
                DataType::Time32(_) | DataType::Time64(_) | DataType::Timestamp(_, _)
            )
        }
        Expr::ScalarVariable(dara_type, _) => {
            matches!(
                dara_type,
                DataType::Time32(_) | DataType::Time64(_) | DataType::Timestamp(_, _)
            )
        }
        _ => false,
    }
}
