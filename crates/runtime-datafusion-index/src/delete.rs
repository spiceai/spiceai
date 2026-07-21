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
use datafusion::logical_expr::{Expr, LogicalPlanBuilder, col};
use datafusion::physical_plan::collect;

/// Resolves the primary-key rows of `accelerator` that currently match `filters`, projected down
/// to `key_fields`.
///
/// This is a read against `accelerator`'s *current* state, so callers must run it before the
/// matching rows are actually deleted from `accelerator` — otherwise there is nothing left to
/// resolve. Used by [`crate::Index::delete_by_predicate`] overrides to bridge a predicate-only
/// delete (retention, an ad hoc SQL `DELETE`) down to the primary-key-based
/// [`crate::Index::delete_by_keys`] every backend actually implements.
///
/// Builds and runs a real `LogicalPlan` (scan → filter → project) through
/// [`Session::create_physical_plan`] rather than calling [`TableProvider::scan`] directly:
/// `scan`'s `filters` argument is only a pushdown *hint* — most providers report
/// `Unsupported`/`Inexact` and rely on the query planner to add the actual `FilterExec` above the
/// scan, which only happens by going through plan creation.
pub async fn resolve_keys_matching_predicate(
    accelerator: &Arc<dyn TableProvider>,
    session: &dyn Session,
    filters: Vec<Expr>,
    key_fields: &[Field],
) -> Result<RecordBatch> {
    let schema = accelerator.schema();
    let key_schema = Arc::new(Schema::new(key_fields.to_vec()));

    if key_fields
        .iter()
        .any(|f| schema.index_of(f.name()).is_err())
    {
        // A key field isn't a column on `accelerator` — nothing to resolve.
        return Ok(RecordBatch::new_empty(key_schema));
    }

    let table_source = Arc::new(DefaultTableSource::new(Arc::clone(accelerator)));
    let mut builder = LogicalPlanBuilder::scan("accelerator", table_source, None)?;
    for filter in filters {
        builder = builder.filter(filter)?;
    }
    let projection: Vec<Expr> = key_fields.iter().map(|f| col(f.name())).collect();
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
            eq_exprs.push(col(name.as_str()).eq(Expr::Literal(value, None)));
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
        let key_fields = vec![Field::new("id", DataType::Int64, false)];

        let keys = resolve_keys_matching_predicate(&table, &ctx.state(), filters, &key_fields)
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

    #[tokio::test]
    async fn resolve_keys_matching_predicate_no_matches_returns_empty_batch() {
        let batch = id_name_batch(&[1, 2], &["a", "b"]);
        let schema = batch.schema();
        let table: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("mem table"));

        let ctx = SessionContext::new();
        let filters = vec![col("id").gt(datafusion::logical_expr::lit(100_i64))];
        let key_fields = vec![Field::new("id", DataType::Int64, false)];

        let keys = resolve_keys_matching_predicate(&table, &ctx.state(), filters, &key_fields)
            .await
            .expect("resolve should succeed");

        assert_eq!(keys.num_rows(), 0);
    }

    #[tokio::test]
    async fn resolve_keys_matching_predicate_missing_key_field_returns_empty_batch() {
        let batch = id_name_batch(&[1, 2], &["a", "b"]);
        let schema = batch.schema();
        let table: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("mem table"));

        let ctx = SessionContext::new();
        let key_fields = vec![Field::new("not_a_column", DataType::Int64, false)];

        let keys = resolve_keys_matching_predicate(&table, &ctx.state(), vec![], &key_fields)
            .await
            .expect("resolve should succeed");

        assert_eq!(keys.num_rows(), 0);
    }
}
