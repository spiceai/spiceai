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

pub mod provider;

use std::sync::Arc;

use async_trait::async_trait;
use connection_manager::SqlServerConnectionPool;
use convert::{map_column_type_to_arrow_type, map_type_name_to_column_type};
use execution_plan::SqlServerExecPlan;
use futures::StreamExt;
use snafu::{OptionExt, ResultExt, Snafu};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::{
    catalog::Session,
    common::ScalarValue,
    common::TableReference,
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    logical_expr::{Operator, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use util::format_datafusion_error;

pub mod connection_manager;
mod convert;
pub mod dialect;
mod execution_plan;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to execute query on SQL Server: {source}"))]
    QueryError { source: tiberius::error::Error },

    #[snafu(display("Failed to connect to SQL Server: {source}"))]
    SqlServerAccessError { source: tiberius::error::Error },

    #[snafu(display("Failed to connect to SQL Server: {source}"))]
    ConnectionPoolError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to retrieve table schema from SQL Server"))]
    SchemaRetrieval,

    #[snafu(display(
        "Unable to retrieve schema: table '{table}' does not exist in the SQL Server database"
    ))]
    SchemaRetrievalTableNotFound { table: String },

    #[snafu(display("Unsupported SQL Server data type: {data_type}"))]
    UnsupportedType { data_type: String },

    #[snafu(display("Failed to process SQL Server query result: {source}"))]
    FailedToBuildRecordBatch { source: arrow::error::ArrowError },

    #[snafu(display("Failed to process SQL Server query result: no column at index {index}"))]
    NoBuilderForIndex { index: usize },

    #[snafu(display("Failed to process SQL Server query result: unsupported type '{mssql_type}'"))]
    FailedToDowncastBuilder { mssql_type: String },

    #[snafu(display(
        "Failed to convert SQL Server timestamp {v} to Arrow nanosecond timestamp: value is outside the supported range (~1677-2262)"
    ))]
    FailedToConvertTimestampToNanos { v: String },

    #[snafu(display(
        "Failed to generate SQL for SQL Server query: {}",
        format_datafusion_error(source)
    ))]
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

        let columns_meta_query = "SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS \
            WHERE TABLE_NAME = @P1 AND TABLE_SCHEMA = @P2";
        tracing::debug!("Executing schema query for dataset {table_name}: {columns_meta_query}");

        let mut conn = conn.get().await.boxed().context(ConnectionPoolSnafu)?;

        let mut query_res = conn
            .query(columns_meta_query, &[&table_name, &table_schema])
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
            let is_nullable: &str = row.get(4).unwrap_or("YES");

            let Some(column_type) = map_type_name_to_column_type(data_type) else {
                tracing::warn!(
                    "Column '{column_name}' of table '{table_name}' has unsupported data type '{data_type}' and will be ignored"
                );
                continue;
            };

            let arrow_data_type = map_column_type_to_arrow_type(
                column_type,
                numeric_precision,
                numeric_scale.map(|v| i8::try_from(v).unwrap_or_default()),
            );

            fields.push(Field::new(
                column_name,
                arrow_data_type,
                is_nullable == "YES",
            ));
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
            results.push(classify_mssql_filter(filter, &self.schema));
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

/// How far SQL Server can be trusted to evaluate `filter` as `DataFusion` does.
///
/// SQL Server compares text by the column's collation (the default ignores case and trailing
/// spaces), pads binary values with zero bytes, converts a text literal without an `N` prefix to
/// the database code page, and gives functions and `LIKE` other meanings (`LOG` is the natural
/// logarithm; a `LIKE` pattern has no backslash escape, reads `[...]` as a character class and
/// follows the collation's letters). So only comparisons between columns and literals it reads
/// the same way are pushed. On text and binary, an equality or `IN` returns every row
/// `DataFusion` keeps and maybe more, so it is pushed as inexact; other comparisons on them are
/// not pushed.
fn classify_mssql_filter(filter: &Expr, schema: &Schema) -> TableProviderFilterPushDown {
    use TableProviderFilterPushDown::{Exact, Inexact, Unsupported};
    match filter {
        Expr::BinaryExpr(binary_expr) => match binary_expr.op {
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => {
                let (left, right) = (binary_expr.left.as_ref(), binary_expr.right.as_ref());
                if !is_pushable_operand(left) || !is_pushable_operand(right) {
                    Unsupported
                } else if is_text(left, schema) || is_text(right, schema) {
                    if binary_expr.op == Operator::Eq {
                        Inexact
                    } else {
                        Unsupported
                    }
                } else {
                    Exact
                }
            }
            Operator::And | Operator::Or => {
                let left = classify_mssql_filter(&binary_expr.left, schema);
                let right = classify_mssql_filter(&binary_expr.right, schema);
                if left == Unsupported || right == Unsupported {
                    Unsupported
                } else {
                    Inexact
                }
            }
            _ => Unsupported,
        },
        // A negated inexact filter could drop rows `DataFusion` keeps.
        Expr::Not(inner) => {
            if classify_mssql_filter(inner, schema) == Exact {
                Exact
            } else {
                Unsupported
            }
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            if is_pushable_operand(inner) {
                Inexact
            } else {
                Unsupported
            }
        }
        Expr::Between(between) => {
            let operands = [&between.expr, &between.low, &between.high];
            if operands.iter().all(|e| is_pushable_operand(e))
                && !operands.iter().any(|e| is_text(e, schema))
            {
                Inexact
            } else {
                Unsupported
            }
        }
        Expr::InList(in_list) => {
            let negated_text = in_list.negated
                && (is_text(&in_list.expr, schema)
                    || in_list.list.iter().any(|e| is_text(e, schema)));
            if is_pushable_operand(&in_list.expr)
                && in_list.list.iter().all(is_pushable_operand)
                && !negated_text
            {
                Inexact
            } else {
                Unsupported
            }
        }
        _ => Unsupported,
    }
}

/// A column, or a literal SQL Server reads as the same value.
fn is_pushable_operand(expr: &Expr) -> bool {
    match expr {
        Expr::Column(_) => true,
        Expr::Literal(value, _) => match value {
            ScalarValue::Utf8(Some(text))
            | ScalarValue::LargeUtf8(Some(text))
            | ScalarValue::Utf8View(Some(text)) => {
                // In a T-SQL string, a backslash before a line break continues the line: both go.
                text.is_ascii() && !text.contains("\\\n") && !text.contains("\\\r")
            }
            ScalarValue::Int8(Some(_))
            | ScalarValue::Int16(Some(_))
            | ScalarValue::Int32(Some(_))
            | ScalarValue::Int64(Some(_))
            | ScalarValue::UInt8(Some(_))
            | ScalarValue::UInt16(Some(_))
            | ScalarValue::UInt32(Some(_))
            | ScalarValue::UInt64(Some(_))
            | ScalarValue::Float32(Some(_))
            | ScalarValue::Float64(Some(_))
            | ScalarValue::Decimal128(Some(_), _, _)
            | ScalarValue::Decimal256(Some(_), _, _)
            | ScalarValue::Date32(Some(_)) => true,
            _ => false,
        },
        _ => false,
    }
}

/// Whether SQL Server compares `expr` other than by code point: text, binary, or a column of
/// unknown type.
fn is_text(expr: &Expr, schema: &Schema) -> bool {
    match expr {
        Expr::Column(column) => schema
            .field_with_name(column.name())
            .map_or(true, |field| is_text_type(field.data_type())),
        Expr::Literal(value, _) => is_text_type(&value.data_type()),
        _ => false,
    }
}

/// Text, compared by the column's collation, and binary, compared after zero padding.
pub(crate) fn is_text_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use TableProviderFilterPushDown::{Exact, Inexact, Unsupported};
    use datafusion::prelude::{cast, col, lit};

    fn classify(filter: &Expr) -> TableProviderFilterPushDown {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("b", DataType::Binary, true),
        ]);
        classify_mssql_filter(filter, &schema)
    }

    #[test]
    fn text_is_compared_by_sql_server_only_where_it_keeps_every_matching_row() {
        assert_eq!(classify(&col("s").eq(lit("A"))), Inexact);
        assert_eq!(
            classify(&col("s").in_list(vec![lit("A"), lit("b")], false)),
            Inexact
        );

        assert_eq!(classify(&col("s").not_eq(lit("a"))), Unsupported);
        assert_eq!(classify(&col("s").gt(lit("Z"))), Unsupported);
        assert_eq!(
            classify(&Expr::Not(Box::new(col("s").eq(lit("a"))))),
            Unsupported
        );
        assert_eq!(
            classify(&col("s").in_list(vec![lit("a")], true)),
            Unsupported
        );
        assert_eq!(classify(&col("s").not_like(lit("a%"))), Unsupported);
        assert_eq!(classify(&col("s").like(lit("A%"))), Unsupported);
        assert_eq!(classify(&col("s").like(lit("50\\%"))), Unsupported);
        assert_eq!(classify(&col("s").ilike(lit("a%"))), Unsupported);
        assert_eq!(classify(&col("s").between(lit("a"), lit("c"))), Unsupported);
        assert_eq!(classify(&col("unknown").eq(lit(1))), Inexact);
        assert_eq!(classify(&col("s").is_null()), Inexact);
        assert_eq!(classify(&col("b").not_eq(lit(1))), Unsupported);
        assert_eq!(classify(&col("s").eq(lit("日本"))), Unsupported);
        assert_eq!(classify(&col("s").eq(lit("x\\\ny"))), Unsupported);
        assert_eq!(classify(&col("s").eq(lit("x\\\r\ny"))), Unsupported);
        assert_eq!(
            classify(&col("id").gt(lit(1)).or(col("s").like(lit("a%")))),
            Unsupported
        );
    }

    #[test]
    fn numbers_are_compared_by_sql_server() {
        assert_eq!(classify(&col("id").gt(lit(1))), Exact);
        assert_eq!(classify(&Expr::Not(Box::new(col("id").eq(lit(1))))), Exact);
        assert_eq!(
            classify(&col("id").gt(lit(1)).and(col("s").eq(lit("a")))),
            Inexact
        );
    }

    #[test]
    fn functions_and_casts_stay_in_datafusion() {
        let log = datafusion::functions::math::expr_fn::log10(col("id"));
        assert_eq!(classify(&log.clone().gt(lit(0.5))), Unsupported);
        assert_eq!(classify(&log.eq(lit(1.0))), Unsupported);
        assert_eq!(
            classify(&cast(col("id"), DataType::Int64).eq(lit(1_i64))),
            Unsupported
        );
        let substr = datafusion::functions::unicode::expr_fn::substr(col("s"), lit(1_i64));
        assert_eq!(classify(&substr.eq(lit("ab"))), Unsupported);
        assert_eq!(classify(&col("id").gt(lit(1)).is_null()), Unsupported);
    }
}
