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
use datafusion::error::Result;
use datafusion::logical_expr::{Expr, col};
use datafusion::physical_plan::collect;

/// Resolves the primary-key rows of `accelerator` that currently match `filters`, projected down
/// to `key_fields`.
///
/// This is a read against `accelerator`'s *current* state, so callers must run it before the
/// matching rows are actually deleted from `accelerator` — otherwise there is nothing left to
/// resolve. Used by [`crate::Index::delete_by_predicate`] overrides to bridge a predicate-only
/// delete (retention, an ad hoc SQL `DELETE`) down to the primary-key-based
/// [`crate::Index::delete_by_keys`] every backend actually implements.
pub async fn resolve_keys_matching_predicate(
    accelerator: &Arc<dyn TableProvider>,
    session: &dyn Session,
    filters: Vec<Expr>,
    key_fields: &[Field],
) -> Result<RecordBatch> {
    let schema = accelerator.schema();
    let projection: Vec<usize> = key_fields
        .iter()
        .filter_map(|f| schema.index_of(f.name()).ok())
        .collect();

    let key_schema = Arc::new(Schema::new(key_fields.to_vec()));

    if projection.len() != key_fields.len() {
        // A key field isn't a column on `accelerator` — nothing to resolve.
        return Ok(RecordBatch::new_empty(key_schema));
    }

    let plan = accelerator
        .scan(session, Some(&projection), &filters, None)
        .await?;
    let batches = collect(plan, session.task_ctx()).await?;

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
pub fn build_key_match_predicate(keys: &RecordBatch, key_columns: &[String]) -> Result<Option<Expr>> {
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
