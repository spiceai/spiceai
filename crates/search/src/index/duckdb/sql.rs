/*
Copyright 2026 The Spice.ai OSS Authors

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

use std::collections::HashSet;

use arrow_schema::SchemaRef;
use datafusion::{
    common::utils::quote_identifier,
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::TableProviderFilterPushDown,
    prelude::Expr,
    sql::unparser::{Unparser, dialect::DuckDBDialect},
};

use crate::SEARCH_SCORE_COLUMN_NAME;

use super::hnsw::DuckDBHnswOptions;

/// CTE name used for the inner nearest-neighbor subquery.
pub(super) const CTE_NAME: &str = "__spice_nn";
/// Alias for the pre-computed distance value inside the CTE.
pub(super) const CTE_DISTANCE_ALIAS: &str = "__spice_dist";

pub(super) const DEFAULT_DUCKDB_VECTOR_SEARCH_LIMIT: usize = 1000;
pub(super) const EMPTY_PROJECTION_ROW_COLUMN: &str = "__spice_empty_projection_row";

/// Build vector search SQL for DuckDB.
///
/// When **no filters** are present, uses a CTE that preserves the clean
/// `TopN → Projection → SeqScan` plan shape required by the HNSW optimizer:
/// ```sql
/// WITH __spice_nn AS (
///     SELECT *, distance_func(col, vec) AS __spice_dist
///     FROM table
///     ORDER BY __spice_dist ASC LIMIT k
/// )
/// SELECT col1, col2, CAST(score_from_dist AS DOUBLE) AS _score
/// FROM __spice_nn ORDER BY __spice_dist ASC
/// ```
///
/// When **filters** are present, falls back to a flat query so filters are applied before the distance calculation.
pub(super) fn duckdb_vector_sql(
    table_name: &str,
    embedding_column: &str,
    projected_columns: &[String],
    filters: &[Expr],
    limit: Option<usize>,
    hnsw: &DuckDBHnswOptions,
    vector_literal: &str,
) -> DataFusionResult<String> {
    let limit = limit.unwrap_or(DEFAULT_DUCKDB_VECTOR_SEARCH_LIMIT);

    if filters.is_empty() {
        // CTE path — activates HNSW index scan
        Ok(duckdb_vector_sql_cte(
            table_name,
            embedding_column,
            projected_columns,
            limit,
            hnsw,
            vector_literal,
        ))
    } else {
        // Flat query path — score calculation
        duckdb_vector_sql_flat(
            table_name,
            embedding_column,
            projected_columns,
            filters,
            limit,
            hnsw,
            vector_literal,
        )
    }
}

/// CTE path: no filters, clean plan shape for HNSW optimizer.
fn duckdb_vector_sql_cte(
    table_name: &str,
    embedding_column: &str,
    projected_columns: &[String],
    limit: usize,
    hnsw: &DuckDBHnswOptions,
    vector_literal: &str,
) -> String {
    let distance_expr = hnsw.metric.distance_expr(embedding_column, vector_literal);
    let score_expr = hnsw.metric.cte_score_expr(CTE_DISTANCE_ALIAS);
    let embedding_not_null = embedding_not_null_predicate(embedding_column);

    let cte = format!(
        "WITH {CTE_NAME} AS (\
         SELECT *, {distance_expr} AS {CTE_DISTANCE_ALIAS} \
         FROM {table} \
         WHERE {embedding_not_null} \
         ORDER BY {CTE_DISTANCE_ALIAS} ASC LIMIT {limit}\
         )",
        table = quote_identifier(table_name),
    );

    let select_exprs = build_select_exprs(projected_columns, &score_expr);

    format!(
        "{cte} SELECT {select_exprs} FROM {CTE_NAME} \
         ORDER BY {CTE_DISTANCE_ALIAS} ASC",
    )
}

/// Flat query path: filters applied via WHERE, no HNSW index (brute-force scan).
fn duckdb_vector_sql_flat(
    table_name: &str,
    embedding_column: &str,
    projected_columns: &[String],
    filters: &[Expr],
    limit: usize,
    hnsw: &DuckDBHnswOptions,
    vector_literal: &str,
) -> DataFusionResult<String> {
    let score_expr = hnsw.metric.score_expr(embedding_column, vector_literal);
    let distance_expr = hnsw.metric.distance_expr(embedding_column, vector_literal);

    let select_exprs = build_select_exprs(projected_columns, &score_expr);

    let filter_exprs: Vec<String> = filters
        .iter()
        .map(duckdb_filter_to_sql)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| DataFusionError::Plan(e.to_string()))?;
    let mut predicates = Vec::with_capacity(filter_exprs.len() + 1);
    predicates.push(embedding_not_null_predicate(embedding_column));
    predicates.extend(filter_exprs);
    let where_clause = format!(" WHERE {}", predicates.join(" AND "));

    Ok(format!(
        "SELECT {select_exprs} FROM {table}{where_clause} ORDER BY {distance_expr} ASC LIMIT {limit}",
        table = quote_identifier(table_name),
    ))
}

fn embedding_not_null_predicate(embedding_column: &str) -> String {
    format!("{} IS NOT NULL", quote_identifier(embedding_column))
}

/// Build the SELECT expression list, substituting `_score` with the given score expression.
fn build_select_exprs(projected_columns: &[String], score_expr: &str) -> String {
    if projected_columns.is_empty() {
        format!("1 AS {}", quote_identifier(EMPTY_PROJECTION_ROW_COLUMN))
    } else {
        projected_columns
            .iter()
            .map(|column| {
                if column == SEARCH_SCORE_COLUMN_NAME {
                    format!(
                        "CAST({score_expr} AS DOUBLE) AS {}",
                        quote_identifier(SEARCH_SCORE_COLUMN_NAME)
                    )
                } else {
                    quote_identifier(column).to_string()
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

pub(super) fn duckdb_filter_pushdown(
    schema: &SchemaRef,
    filter: &Expr,
) -> TableProviderFilterPushDown {
    let pushdownable_columns: HashSet<&str> = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .filter(|name| *name != SEARCH_SCORE_COLUMN_NAME)
        .collect();

    if !filter
        .column_refs()
        .iter()
        .all(|column| pushdownable_columns.contains(column.name()))
    {
        return TableProviderFilterPushDown::Unsupported;
    }

    match duckdb_filter_to_sql(filter) {
        Ok(_) => TableProviderFilterPushDown::Exact,
        Err(_) => TableProviderFilterPushDown::Unsupported,
    }
}

fn duckdb_filter_to_sql(filter: &Expr) -> DataFusionResult<String> {
    let dialect = DuckDBDialect::new();
    Unparser::new(&dialect)
        .expr_to_sql(filter)
        .map(|sql| sql.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::prelude::{col, lit};
    use std::sync::Arc;

    use crate::index::duckdb::hnsw::DuckDBHnswOptions;

    #[test]
    fn duckdb_vector_sql_orders_by_distance_and_projects_score() {
        let sql = duckdb_vector_sql(
            "docs",
            "body_embedding",
            &["id".to_string(), SEARCH_SCORE_COLUMN_NAME.to_string()],
            &[],
            Some(10),
            &DuckDBHnswOptions::default(),
            "[1.0, 0.0]::FLOAT[2]",
        )
        .expect("SQL should build");

        assert_eq!(
            sql,
            "WITH __spice_nn AS (\
             SELECT *, array_cosine_distance(body_embedding, [1.0, 0.0]::FLOAT[2]) AS __spice_dist \
             FROM docs \
             WHERE body_embedding IS NOT NULL \
             ORDER BY __spice_dist ASC LIMIT 10\
             ) SELECT id, CAST(1.0 - __spice_dist AS DOUBLE) AS _score \
             FROM __spice_nn \
             ORDER BY __spice_dist ASC"
        );
    }

    #[test]
    fn duckdb_vector_sql_handles_empty_projection() {
        let sql = duckdb_vector_sql(
            "docs",
            "body_embedding",
            &[],
            &[],
            Some(10),
            &DuckDBHnswOptions::default(),
            "[1.0, 0.0]::FLOAT[2]",
        )
        .expect("SQL should build");

        assert_eq!(
            sql,
            "WITH __spice_nn AS (\
             SELECT *, array_cosine_distance(body_embedding, [1.0, 0.0]::FLOAT[2]) AS __spice_dist \
             FROM docs \
             WHERE body_embedding IS NOT NULL \
             ORDER BY __spice_dist ASC LIMIT 10\
             ) SELECT 1 AS __spice_empty_projection_row \
             FROM __spice_nn \
             ORDER BY __spice_dist ASC"
        );
    }

    #[test]
    fn duckdb_filter_pushdown_rejects_score_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(SEARCH_SCORE_COLUMN_NAME, DataType::Float64, false),
        ]));
        let filter = col(SEARCH_SCORE_COLUMN_NAME).gt(lit(0.5));

        assert_eq!(
            duckdb_filter_pushdown(&schema, &filter),
            TableProviderFilterPushDown::Unsupported
        );
    }

    #[test]
    fn duckdb_filter_pushdown_allows_base_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(SEARCH_SCORE_COLUMN_NAME, DataType::Float64, false),
        ]));
        let filter = col("id").gt(lit(10_i64));

        assert_eq!(
            duckdb_filter_pushdown(&schema, &filter),
            TableProviderFilterPushDown::Exact
        );
    }

    #[test]
    fn duckdb_filter_pushdown_rejects_mixed_score_filter() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(SEARCH_SCORE_COLUMN_NAME, DataType::Float64, false),
        ]));
        let filter = col("id")
            .gt(lit(10_i64))
            .and(col(SEARCH_SCORE_COLUMN_NAME).gt(lit(0.5)));

        assert_eq!(
            duckdb_filter_pushdown(&schema, &filter),
            TableProviderFilterPushDown::Unsupported
        );
    }

    #[test]
    fn duckdb_vector_sql_applies_default_limit() {
        let sql = duckdb_vector_sql(
            "docs",
            "body_embedding",
            &["id".to_string()],
            &[],
            None,
            &DuckDBHnswOptions::default(),
            "[1.0, 0.0]::FLOAT[2]",
        )
        .expect("SQL should build");

        assert!(sql.contains("LIMIT 1000"));
    }

    #[test]
    fn duckdb_vector_sql_uses_flat_query_with_filters() {
        let filter = col("id").gt(lit(10_i64));
        let sql = duckdb_vector_sql(
            "docs",
            "body_embedding",
            &["id".to_string(), SEARCH_SCORE_COLUMN_NAME.to_string()],
            &[filter],
            Some(10),
            &DuckDBHnswOptions::default(),
            "[1.0, 0.0]::FLOAT[2]",
        )
        .expect("SQL should build");

        // Flat query: no CTE, direct SELECT with WHERE and inline score
        assert_eq!(
            sql,
            "SELECT id, CAST(1.0 - array_cosine_distance(body_embedding, [1.0, 0.0]::FLOAT[2]) AS DOUBLE) AS _score \
                    FROM docs WHERE body_embedding IS NOT NULL AND (\"id\" > 10) \
             ORDER BY array_cosine_distance(body_embedding, [1.0, 0.0]::FLOAT[2]) ASC LIMIT 10"
        );
    }

    #[test]
    fn duckdb_vector_sql_uses_cte_without_filters() {
        let sql = duckdb_vector_sql(
            "docs",
            "body_embedding",
            &["id".to_string(), SEARCH_SCORE_COLUMN_NAME.to_string()],
            &[],
            Some(10),
            &DuckDBHnswOptions::default(),
            "[1.0, 0.0]::FLOAT[2]",
        )
        .expect("SQL should build");

        assert_eq!(
            sql,
            "WITH __spice_nn AS (\
             SELECT *, array_cosine_distance(body_embedding, [1.0, 0.0]::FLOAT[2]) AS __spice_dist \
             FROM docs \
             WHERE body_embedding IS NOT NULL \
             ORDER BY __spice_dist ASC LIMIT 10\
             ) SELECT id, CAST(1.0 - __spice_dist AS DOUBLE) AS _score \
             FROM __spice_nn \
             ORDER BY __spice_dist ASC"
        );
    }
}
