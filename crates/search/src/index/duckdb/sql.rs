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
};
use datafusion_table_providers::sql::sql_provider_datafusion::expr::{self, Engine};

use crate::SEARCH_SCORE_COLUMN_NAME;

use super::hnsw::DuckDBHnswOptions;

/// CTE name used for the inner nearest-neighbor subquery.
pub(super) const CTE_NAME: &str = "__spice_nn";
/// Alias for the pre-computed distance value inside the CTE.
pub(super) const CTE_DISTANCE_ALIAS: &str = "__spice_dist";

pub(super) const DEFAULT_DUCKDB_VECTOR_SEARCH_LIMIT: usize = 1000;
pub(super) const EMPTY_PROJECTION_ROW_COLUMN: &str = "__spice_empty_projection_row";

/// The pushed-down filters together with the schema their columns come from.
///
/// The two travel together because the DuckDB rendering needs both: it declines the timestamp
/// normalization only for a column whose *resolved* type proves the normalization unnecessary, and
/// a render site holding the filters alone renders a timezone-aware timestamp through
/// `EPOCH_MS`, which truncates it to whole milliseconds and moves which rows a comparison inside
/// that millisecond selects.
#[derive(Clone, Copy)]
pub(super) struct ScopedFilters<'a> {
    pub(super) filters: &'a [Expr],
    pub(super) schema: &'a SchemaRef,
}

impl ScopedFilters<'_> {
    fn is_empty(&self) -> bool {
        self.filters.is_empty()
    }

    fn render(&self) -> DataFusionResult<Vec<String>> {
        self.filters
            .iter()
            .map(|filter| render_filter(filter, self.schema))
            .collect()
    }
}

/// Render one filter as DuckDB SQL, against the schema its columns come from.
///
/// Both the capability probe and the statement render through here, so they cannot disagree about
/// what is renderable or about how a given column's type is rendered.
fn render_filter(filter: &Expr, schema: &SchemaRef) -> DataFusionResult<String> {
    expr::to_sql_with_engine_and_schema(filter, Some(Engine::DuckDB), Some(schema.as_ref()))
        .map_err(|e| DataFusionError::Plan(e.to_string()))
}

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
    filters: ScopedFilters<'_>,
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
    filters: ScopedFilters<'_>,
    limit: usize,
    hnsw: &DuckDBHnswOptions,
    vector_literal: &str,
) -> DataFusionResult<String> {
    let score_expr = hnsw.metric.score_expr(embedding_column, vector_literal);
    let distance_expr = hnsw.metric.distance_expr(embedding_column, vector_literal);

    let select_exprs = build_select_exprs(projected_columns, &score_expr);

    let filter_exprs = filters.render()?;
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

    match render_filter(filter, schema) {
        Ok(_) => TableProviderFilterPushDown::Exact,
        Err(_) => TableProviderFilterPushDown::Unsupported,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use datafusion::common::Column;
    use datafusion::prelude::{col, lit};
    use datafusion::scalar::ScalarValue;
    use std::sync::Arc;

    use crate::index::duckdb::hnsw::DuckDBHnswOptions;

    /// A schema in the shape `query_result_schema` builds: the source columns plus `_score`.
    fn docs_schema(extra: Vec<Field>) -> SchemaRef {
        let mut fields = vec![Field::new("id", DataType::Int64, false)];
        fields.extend(extra);
        fields.push(Field::new(
            SEARCH_SCORE_COLUMN_NAME,
            DataType::Float64,
            false,
        ));
        Arc::new(Schema::new(fields))
    }

    /// A microsecond timestamp literal DuckDB renders with its sub-millisecond digits intact.
    fn micros_literal(micros: i64) -> Expr {
        lit(ScalarValue::TimestampMicrosecond(
            Some(micros),
            Some("UTC".into()),
        ))
    }

    /// A `docs` schema carrying one timezone-aware microsecond `ts` column — the resolved type
    /// that declines the UTC normalization, so a guard on the literal sees the literal alone.
    fn aware_ts_schema() -> SchemaRef {
        docs_schema(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        )])
    }

    /// A whole-second timestamp literal, whose count DuckDB has to scale to microseconds before it
    /// can hold it.
    fn seconds_literal(seconds: i64) -> Expr {
        lit(ScalarValue::TimestampSecond(
            Some(seconds),
            Some("UTC".into()),
        ))
    }

    /// The flat (filtered) statement for one filter, rendered against `schema`.
    fn flat_sql(schema: &SchemaRef, filter: &Expr) -> DataFusionResult<String> {
        duckdb_vector_sql(
            "docs",
            "body_embedding",
            &["id".to_string()],
            ScopedFilters {
                filters: std::slice::from_ref(filter),
                schema,
            },
            Some(10),
            &DuckDBHnswOptions::default(),
            "[1.0, 0.0]::FLOAT[2]",
        )
    }

    #[test]
    fn duckdb_vector_sql_orders_by_distance_and_projects_score() {
        let schema = docs_schema(vec![]);
        let sql = duckdb_vector_sql(
            "docs",
            "body_embedding",
            &["id".to_string(), SEARCH_SCORE_COLUMN_NAME.to_string()],
            ScopedFilters {
                filters: &[],
                schema: &schema,
            },
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
        let schema = docs_schema(vec![]);
        let sql = duckdb_vector_sql(
            "docs",
            "body_embedding",
            &[],
            ScopedFilters {
                filters: &[],
                schema: &schema,
            },
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
        let schema = docs_schema(vec![]);
        let filter = col(SEARCH_SCORE_COLUMN_NAME).gt(lit(0.5));

        assert_eq!(
            duckdb_filter_pushdown(&schema, &filter),
            TableProviderFilterPushDown::Unsupported
        );
    }

    #[test]
    fn duckdb_filter_pushdown_allows_base_column() {
        let schema = docs_schema(vec![]);
        let filter = col("id").gt(lit(10_i64));

        assert_eq!(
            duckdb_filter_pushdown(&schema, &filter),
            TableProviderFilterPushDown::Exact
        );
    }

    #[test]
    fn duckdb_filter_pushdown_rejects_mixed_score_filter() {
        let schema = docs_schema(vec![]);
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
        let schema = docs_schema(vec![]);
        let sql = duckdb_vector_sql(
            "docs",
            "body_embedding",
            &["id".to_string()],
            ScopedFilters {
                filters: &[],
                schema: &schema,
            },
            None,
            &DuckDBHnswOptions::default(),
            "[1.0, 0.0]::FLOAT[2]",
        )
        .expect("SQL should build");

        assert!(sql.contains("LIMIT 1000"));
    }

    #[test]
    fn duckdb_vector_sql_uses_flat_query_with_filters() {
        let schema = docs_schema(vec![]);
        let filters = vec![col("id").gt(lit(10_i64))];
        let sql = duckdb_vector_sql(
            "docs",
            "body_embedding",
            &["id".to_string(), SEARCH_SCORE_COLUMN_NAME.to_string()],
            ScopedFilters {
                filters: &filters,
                schema: &schema,
            },
            Some(10),
            &DuckDBHnswOptions::default(),
            "[1.0, 0.0]::FLOAT[2]",
        )
        .expect("SQL should build");

        // Flat query: no CTE, direct SELECT with WHERE and inline score
        assert_eq!(
            sql,
            "SELECT id, CAST(1.0 - array_cosine_distance(body_embedding, [1.0, 0.0]::FLOAT[2]) AS DOUBLE) AS _score \
               FROM docs WHERE body_embedding IS NOT NULL AND \"id\" > 10 \
             ORDER BY array_cosine_distance(body_embedding, [1.0, 0.0]::FLOAT[2]) ASC LIMIT 10"
        );
    }

    #[test]
    fn duckdb_vector_sql_uses_cte_without_filters() {
        let schema = docs_schema(vec![]);
        let sql = duckdb_vector_sql(
            "docs",
            "body_embedding",
            &["id".to_string(), SEARCH_SCORE_COLUMN_NAME.to_string()],
            ScopedFilters {
                filters: &[],
                schema: &schema,
            },
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

    /// regression test for #13144: a timezone-aware timestamp column is already the type and the
    /// reference frame the rendered literal is in, so it must be compared directly. Rendered
    /// through `EPOCH_MS` it is truncated to whole milliseconds, and a comparison inside a
    /// millisecond then selects a different set of rows than the caller asked for. `flat_sql`
    /// projects `id` alone, so this also covers a filter on a column the projection drops.
    #[test]
    fn a_timezone_aware_timestamp_filter_is_rendered_without_the_millisecond_truncation() {
        let schema = aware_ts_schema();
        let filter = col("ts").gt(micros_literal(1_767_225_600_000_999));

        let sql = flat_sql(&schema, &filter).expect("SQL should build");

        assert!(
            !sql.contains("EPOCH_MS"),
            "a timezone-aware column needs no normalization, so it must not be rendered through whole milliseconds: {sql}"
        );
        assert!(
            sql.contains(r#""ts" > make_timestamptz(1767225600000999)"#),
            "the column must be compared directly against the microsecond the literal names: {sql}"
        );
    }

    /// Regression test for #13432: past about 2255-06-05 an epoch-microsecond count exceeds the
    /// `2^53` up to which an `f64` holds consecutive integers exactly. `TO_TIMESTAMP` takes a
    /// `DOUBLE`, so rendering through it rounds the count and the literal names a neighbouring
    /// microsecond — a filter that then selects a row set the query never asked for, silently,
    /// since a wrong instant is still a valid one. `make_timestamptz` takes a `BIGINT`, so nothing
    /// is widened.
    ///
    /// `2^53 + 1` is the first count an `f64` cannot hold: it rounds to `2^53`, one microsecond
    /// below. That is the whole error, which is why this pins the literal rather than a tolerance.
    #[test]
    fn a_timestamp_past_the_f64_integer_bound_names_the_microsecond_it_holds() {
        let schema = aware_ts_schema();
        let past_2255 = 9_007_199_254_740_993_i64;
        let filter = col("ts").gt(micros_literal(past_2255));

        let sql = flat_sql(&schema, &filter).expect("SQL should build");

        assert!(
            sql.contains(&format!(r#""ts" > make_timestamptz({past_2255})"#)),
            "the literal must name the microsecond it holds, not the one an f64 rounds it to: {sql}"
        );
    }

    /// DuckDB reserves `i64::MAX` and `-i64::MAX` microseconds as its infinity sentinels and
    /// refuses both, and a second count large enough to overflow when scaled to microseconds names
    /// an instant it cannot hold either. None of the three has a literal to render.
    ///
    /// The probe promises `Exact` for every filter it accepts, so rendering one of these anyway
    /// would report the filter pushed down and then fail the statement built from it — which a
    /// `DELETE` or `UPDATE` reaches only after its SQL is generated. Declining leaves DataFusion
    /// applying the filter itself, which is correct and merely slower.
    #[test]
    fn a_timestamp_duckdb_cannot_hold_is_declined_by_both_the_probe_and_the_statement() {
        let schema = aware_ts_schema();
        let seconds_schema = docs_schema(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
            false,
        )]);

        let undeclinable = [
            (&schema, col("ts").gt(micros_literal(i64::MAX))),
            (&schema, col("ts").gt(micros_literal(-i64::MAX))),
            (&seconds_schema, col("ts").gt(seconds_literal(i64::MAX))),
        ];

        for (schema, filter) in &undeclinable {
            assert_eq!(
                duckdb_filter_pushdown(schema, filter),
                TableProviderFilterPushDown::Unsupported,
                "the probe must not promise a filter it cannot render: {filter}"
            );
            assert!(
                flat_sql(schema, filter).is_err(),
                "an instant DuckDB refuses must not be rendered: {filter}"
            );
        }
    }

    /// The positive control for the refusal above: `i64::MIN` is not one of the sentinels — the
    /// fork measures DuckDB round-tripping it as the finite instant it is — so it must still
    /// render. A guard that only pinned the refusals would be satisfied by a renderer that
    /// declined every timestamp, and the over-refusal that would cost is silent: the filter simply
    /// stops being pushed down.
    #[test]
    fn the_smallest_microsecond_count_is_finite_and_still_renders() {
        let schema = aware_ts_schema();
        let filter = col("ts").gt(micros_literal(i64::MIN));

        assert_eq!(
            duckdb_filter_pushdown(&schema, &filter),
            TableProviderFilterPushDown::Exact,
            "i64::MIN is a finite instant, not a sentinel"
        );
        let sql = flat_sql(&schema, &filter).expect("SQL should build");
        assert!(
            sql.contains(&format!(r#""ts" > make_timestamptz({})"#, i64::MIN)),
            "a finite instant must render as the count it is: {sql}"
        );
    }

    /// The normalization is what makes a *naive* timestamp compare against the rendered UTC
    /// literal at all, so passing a schema must not remove it there.
    #[test]
    fn a_naive_timestamp_filter_keeps_the_utc_normalization() {
        let schema = docs_schema(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        )]);
        let filter = col("ts").gt(micros_literal(1_767_225_600_000_999));

        let sql = flat_sql(&schema, &filter).expect("SQL should build");

        assert!(
            sql.contains(r#"TO_TIMESTAMP(EPOCH_MS("ts") / 1000)"#),
            "a naive column still has to be pinned to UTC to compare with the literal: {sql}"
        );
    }

    /// A column the schema does not carry keeps the normalization, which is what makes the types it
    /// exists for bind. Only a resolved timezone-aware type declines it.
    #[test]
    fn an_unresolved_filter_column_keeps_the_normalization() {
        let schema = docs_schema(vec![]);
        let filter = col("ts").gt(micros_literal(1_767_225_600_000_999));

        let sql = flat_sql(&schema, &filter).expect("SQL should build");

        assert!(
            sql.contains(r#"TO_TIMESTAMP(EPOCH_MS("ts") / 1000)"#),
            "an unresolved column must keep the normalization: {sql}"
        );
    }

    /// The capability probe promises `Exact` for every filter it accepts, so a filter the probe
    /// accepts has to be one the statement can render — and one it rejects for being unrenderable
    /// must be one the statement would have failed on. Both go through `render_filter`, and this
    /// pins the consequence.
    #[test]
    fn the_pushdown_probe_accepts_exactly_the_filters_the_statement_can_render() {
        let schema = docs_schema(vec![
            Field::new(
                "aware",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new(
                "naive",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ]);

        let renderable = [
            col("aware").gt(micros_literal(1_767_225_600_000_999)),
            col("naive").gt(micros_literal(1_767_225_600_000_999)),
            col("id").gt(lit(10_i64)),
        ];
        // Two relations in one expression is the shape the renderer refuses; both column *names*
        // are in the schema, so the probe gets past its own column check and has to ask the
        // renderer.
        let unrenderable = [
            Expr::from(Column::new(Some("a"), "id")).gt(Expr::from(Column::new(Some("b"), "id")))
        ];

        for filter in &renderable {
            assert_eq!(
                duckdb_filter_pushdown(&schema, filter),
                TableProviderFilterPushDown::Exact,
                "the probe must accept {filter}"
            );
            flat_sql(&schema, filter).expect("a filter the probe accepted must render");
        }

        for filter in &unrenderable {
            assert_eq!(
                duckdb_filter_pushdown(&schema, filter),
                TableProviderFilterPushDown::Unsupported,
                "the probe must reject {filter}"
            );
            assert!(
                flat_sql(&schema, filter).is_err(),
                "a filter the probe rejected must not render: {filter}"
            );
        }
    }
}
