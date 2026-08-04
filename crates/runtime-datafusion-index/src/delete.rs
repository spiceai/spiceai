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

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::ScalarValue;
use datafusion::datasource::DefaultTableSource;
use datafusion::error::Result;
use datafusion::logical_expr::{Expr, LogicalPlanBuilder, ident};
use datafusion::physical_plan::collect;

/// Resolves the rows of `table` that currently match `filters`, projected down to
/// `key_columns` — a generic way to read "the rows matching a predicate, but only these
/// columns" out of any [`TableProvider`]. `filters` may reference *any* column of `table`, not
/// just `key_columns`: the predicate is evaluated against `table`'s full schema, and only the
/// result is narrowed to `key_columns`.
///
/// This is a read against `table`'s *current* state — callers deleting rows must run it before
/// the matching rows are actually removed, otherwise there is nothing left to resolve. Used by
/// [`crate::Index::resolve_delete_keys`]'s default to bridge a predicate-only delete (retention,
/// an ad hoc SQL `DELETE`) down to the primary-key-based [`crate::Index::delete_by_keys`] every
/// backend actually implements; `key_columns` is typically [`crate::Index::required_columns`].
///
/// Builds and runs a real `LogicalPlan` (scan → filter → project) through
/// [`Session::create_physical_plan`] rather than calling [`TableProvider::scan`] directly:
/// `scan`'s `filters` argument is only a pushdown *hint* — most providers report
/// `Unsupported`/`Inexact` and rely on the query planner to add the actual `FilterExec` above the
/// scan, which only happens by going through plan creation.
///
/// # Errors
///
/// Returns an error if the logical plan can't be built (e.g. an invalid filter or projection), or
/// if physical plan creation or execution against `table` fails.
pub async fn resolve_keys_matching_predicate(
    table: &Arc<dyn TableProvider>,
    session: &dyn Session,
    filters: Vec<Expr>,
    key_columns: &[String],
) -> Result<RecordBatch> {
    let schema = table.schema();
    let key_fields: Vec<Field> = key_columns
        .iter()
        .filter_map(|name| schema.field_with_name(name).ok().cloned())
        .collect();
    let key_schema = Arc::new(Schema::new(key_fields));

    if key_schema.fields().len() != key_columns.len() {
        // A requested column isn't on `table` — nothing to resolve.
        return Ok(RecordBatch::new_empty(key_schema));
    }

    let table_source = Arc::new(DefaultTableSource::new(Arc::clone(table)));
    let mut builder = LogicalPlanBuilder::scan("t", table_source, None)?;
    for filter in filters {
        builder = builder.filter(filter)?;
    }
    let projection: Vec<Expr> = key_columns.iter().map(|name| ident(name.as_str())).collect();
    let plan = builder.project(projection)?.build()?;

    let physical_plan = session.create_physical_plan(&plan).await?;
    let batches = collect(physical_plan, session.task_ctx()).await?;

    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(key_schema));
    }

    concat_batches(&batches[0].schema(), &batches).map_err(datafusion::error::DataFusionError::from)
}

/// Builds a balanced OR-of-AND-equalities predicate matching every row of `keys` on the named
/// `key_columns` — `(c1 = keys[0].c1 AND c2 = keys[0].c2) OR (c1 = keys[1].c1 AND ...) OR ...`.
///
/// Returns `None` if `keys` has zero rows. Used to translate a batch of primary-key rows into a
/// predicate a store can filter/query by, e.g. when a wrapper index (chunked, compound) needs to
/// address a backing store's own data using a subset of that store's key columns.
///
/// # Errors
///
/// Returns an error if `keys` is missing one of `key_columns`, or if a key value can't be
/// converted to a scalar literal.
pub fn build_key_match_predicate(
    keys: &RecordBatch,
    key_columns: &[String],
) -> Result<Option<Expr>> {
    let arrays: Vec<_> = key_columns
        .iter()
        .map(|name| keys.column_by_name(name).cloned())
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Plan(format!(
                "key batch is missing one of the requested key columns: {key_columns:?}"
            ))
        })?;

    let mut row_conditions = Vec::with_capacity(keys.num_rows());
    for row in 0..keys.num_rows() {
        let mut eq_exprs = Vec::with_capacity(key_columns.len());
        for (name, array) in key_columns.iter().zip(&arrays) {
            let value = ScalarValue::try_from_array(array.as_ref(), row)?;
            eq_exprs.push(ident(name.as_str()).eq(Expr::Literal(value, None)));
        }
        if let Some(row_condition) = balanced_binary(eq_exprs, Expr::and) {
            row_conditions.push(row_condition);
        }
    }

    Ok(balanced_binary(row_conditions, Expr::or))
}

/// Balanced binary tree of expressions — avoids the O(n)-depth stack-overflow risk of a plain
/// `reduce`. Mirrors `data_components::pk_filter_expr::balanced_binary`.
fn balanced_binary(mut conditions: Vec<Expr>, op: fn(Expr, Expr) -> Expr) -> Option<Expr> {
    match conditions.len() {
        0 => None,
        1 => conditions.into_iter().next(),
        _ => {
            let mid = conditions.len() / 2;
            let right_exprs = conditions.split_off(mid);
            match (
                balanced_binary(conditions, op),
                balanced_binary(right_exprs, op),
            ) {
                (Some(l), Some(r)) => Some(op(l, r)),
                (Some(s), None) | (None, Some(s)) => Some(s),
                (None, None) => None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::DataType;
    use datafusion::datasource::MemTable;
    // The dot-splitting constructor, kept here rather than at module scope: the tests use
    // it deliberately for undotted filter columns, while the code under test must not.
    use datafusion::logical_expr::col;
    use datafusion::prelude::SessionContext;

    fn id_name_batch(ids: &[i64], names: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .expect("valid batch")
    }

    #[test]
    fn build_key_match_predicate_empty_batch_returns_none() {
        let keys = id_name_batch(&[], &[]);
        let predicate =
            build_key_match_predicate(&keys, &["id".to_string()]).expect("should not error");
        assert!(predicate.is_none());
    }

    #[test]
    fn build_key_match_predicate_missing_column_errors() {
        let keys = id_name_batch(&[1], &["a"]);
        let err = build_key_match_predicate(&keys, &["nonexistent".to_string()])
            .expect_err("missing column should error");
        assert!(err.to_string().contains("nonexistent"));
    }

    #[test]
    fn build_key_match_predicate_single_column_builds_or_of_eq() {
        let keys = id_name_batch(&[1, 2, 3], &["a", "b", "c"]);
        let predicate = build_key_match_predicate(&keys, &["id".to_string()])
            .expect("should not error")
            .expect("non-empty batch produces a predicate");

        // Every matching id must satisfy the predicate when evaluated against a superset table.
        let display = format!("{predicate}");
        for id in [1, 2, 3] {
            assert!(
                display.contains(&format!("id = Int64({id})")),
                "predicate missing id = {id}: {display}"
            );
        }
    }

    #[tokio::test]
    async fn resolve_keys_matching_predicate_projects_and_filters() {
        let batch = id_name_batch(&[1, 2, 3, 4], &["a", "b", "c", "d"]);
        let schema = batch.schema();
        let table: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("mem table"));

        let ctx = SessionContext::new();
        let filters = vec![col("id").gt(datafusion::logical_expr::lit(2_i64))];
        let key_columns = vec!["id".to_string()];

        let keys = resolve_keys_matching_predicate(&table, &ctx.state(), filters, &key_columns)
            .await
            .expect("resolve should succeed");

        assert_eq!(
            keys.num_columns(),
            1,
            "projected down to just the id column"
        );
        let id_col = keys
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column is Int64");
        let mut values: Vec<i64> = id_col.values().to_vec();
        values.sort_unstable();
        assert_eq!(values, vec![3, 4]);
    }

    /// Point of this test: the filter references `name`, a column that isn't in `key_columns` at
    /// all — `resolve_keys_matching_predicate` must still apply it correctly, since the filter is
    /// evaluated against `table`'s full schema, not restricted to `key_columns`.
    #[tokio::test]
    async fn resolve_keys_matching_predicate_filters_on_a_non_key_column() {
        let batch = id_name_batch(&[1, 2, 3, 4], &["a", "b", "a", "b"]);
        let schema = batch.schema();
        let table: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("mem table"));

        let ctx = SessionContext::new();
        let filters = vec![col("name").eq(datafusion::logical_expr::lit("b"))];
        let key_columns = vec!["id".to_string()];

        let keys = resolve_keys_matching_predicate(&table, &ctx.state(), filters, &key_columns)
            .await
            .expect("resolve should succeed");

        assert_eq!(keys.num_columns(), 1, "still projected down to just id");
        let id_col = keys
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column is Int64");
        let mut values: Vec<i64> = id_col.values().to_vec();
        values.sort_unstable();
        assert_eq!(values, vec![2, 4], "only rows where name = 'b'");
    }

    #[tokio::test]
    async fn resolve_keys_matching_predicate_no_matches_returns_empty_batch() {
        let batch = id_name_batch(&[1, 2], &["a", "b"]);
        let schema = batch.schema();
        let table: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("mem table"));

        let ctx = SessionContext::new();
        let filters = vec![col("id").gt(datafusion::logical_expr::lit(100_i64))];
        let key_columns = vec!["id".to_string()];

        let keys = resolve_keys_matching_predicate(&table, &ctx.state(), filters, &key_columns)
            .await
            .expect("resolve should succeed");

        assert_eq!(keys.num_rows(), 0);
    }

    #[tokio::test]
    async fn resolve_keys_matching_predicate_missing_key_column_returns_empty_batch() {
        let batch = id_name_batch(&[1, 2], &["a", "b"]);
        let schema = batch.schema();
        let table: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("mem table"));

        let ctx = SessionContext::new();
        let key_columns = vec!["not_a_column".to_string()];

        let keys = resolve_keys_matching_predicate(&table, &ctx.state(), vec![], &key_columns)
            .await
            .expect("resolve should succeed");

        assert_eq!(keys.num_rows(), 0);
    }

    /// A chunked index's primary keys include `_spice.chunk_id`, and a flattened `JSON`
    /// column is named like `message.body`. Both are single columns whose *name*
    /// contains a dot — not a `relation.column` reference.
    fn dotted_key_batch(chunk_ids: &[&str], ids: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_spice.chunk_id", DataType::Utf8, false),
            Field::new("id", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(chunk_ids.to_vec())),
                Arc::new(Int64Array::from(ids.to_vec())),
            ],
        )
        .expect("valid batch")
    }

    /// Regression test: `col(name)` routes through `Column::from_qualified_name`, which
    /// splits on `.`, so projecting a dotted key asked for column `chunk_id` of relation
    /// `_spice`. The scan is built as relation `t`, so planning failed with
    /// `No field named _spice.chunk_id` and a delete on a chunked index could resolve no
    /// rows at all.
    #[tokio::test]
    async fn resolve_keys_matching_predicate_projects_a_dotted_key_column() {
        let batch = dotted_key_batch(&["c1", "c2", "c3"], &[1, 2, 3]);
        let schema = batch.schema();
        let table: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("mem table"));

        let ctx = SessionContext::new();
        let filters = vec![col("id").gt(datafusion::logical_expr::lit(1_i64))];
        let key_columns = vec!["_spice.chunk_id".to_string()];

        let keys = resolve_keys_matching_predicate(&table, &ctx.state(), filters, &key_columns)
            .await
            .expect("a dotted key column must resolve, not be read as relation.column");

        assert_eq!(keys.num_columns(), 1);
        assert_eq!(
            keys.schema().field(0).name(),
            "_spice.chunk_id",
            "the projected field keeps its whole name"
        );
        let chunk_ids = keys
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("chunk id column is Utf8");
        let mut values: Vec<&str> = chunk_ids.iter().flatten().collect();
        values.sort_unstable();
        assert_eq!(values, vec!["c2", "c3"]);
    }

    /// The same split in `build_key_match_predicate`. Asserted by *planning* the predicate
    /// rather than by rendering it: `Expr`'s display is `_spice.chunk_id` either way, so
    /// only resolution against a real schema tells the two apart.
    #[tokio::test]
    async fn build_key_match_predicate_keeps_a_dotted_key_name_whole() {
        let keys = dotted_key_batch(&["c1", "c2"], &[1, 2]);
        let key_columns = vec!["_spice.chunk_id".to_string()];
        let predicate = build_key_match_predicate(&keys, &key_columns)
            .expect("should not error")
            .expect("non-empty batch produces a predicate");

        let mem = MemTable::try_new(keys.schema(), vec![vec![keys.clone()]]).expect("mem table");
        let table: Arc<dyn TableProvider> = Arc::new(mem);
        let table_source = Arc::new(DefaultTableSource::new(table));
        let plan = LogicalPlanBuilder::scan("t", table_source, None)
            .expect("scan builds")
            .filter(predicate)
            .expect("the predicate must resolve against the scanned relation")
            .build()
            .expect("plan builds");

        assert!(
            format!("{plan:?}").contains("_spice.chunk_id"),
            "the filter must reference the whole dotted name: {plan:?}"
        );
    }
}
