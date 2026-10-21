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

//! Bounded primary-key predicates and index deletion batches.

use arrow::array::RecordBatch;
use data_components::cdc::ChangeBatch;
use data_components::pk_filter_expr::{self, balanced_binary};
use datafusion::common::{Column, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, col, lit};

use super::cdc::select_rows;

#[must_use]
pub fn missing_primary_keys(dataset_name: &str) -> DataFusionError {
    DataFusionError::Execution(format!(
        "Cannot delete rows from dataset '{dataset_name}' without primary keys"
    ))
}

/// Build a delete predicate for the selected rows' primary keys.
///
/// # Errors
/// Returns an error if primary keys are missing or a key predicate cannot be built.
pub fn build_batch_delete_expr_from_change_batch(
    change_batch: &ChangeBatch,
    row_indices: &[usize],
    dataset_name: &str,
) -> Result<Option<Expr>> {
    let Some(&first) = row_indices.first() else {
        return Ok(None);
    };
    let first_row_pks = change_batch.primary_keys(first);
    if first_row_pks.is_empty() {
        return Err(missing_primary_keys(dataset_name));
    }

    let data_batch = change_batch.data_batch();
    if first_row_pks.len() == 1 {
        return pk_filter_expr::build_pk_in_list_from_batch(
            row_indices,
            &first_row_pks[0],
            &data_batch,
        )
        .map(Some)
        .map_err(|error| DataFusionError::External(Box::new(error)));
    }

    let row_conditions = row_indices
        .iter()
        .map(|&row| {
            let exprs = pk_filter_expr::get_delete_where_expr_from_batch(
                &data_batch,
                row,
                change_batch.primary_keys(row),
            )
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
            balanced_binary(exprs, Expr::and).ok_or_else(|| missing_primary_keys(dataset_name))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(balanced_binary(row_conditions, Expr::or))
}

/// Build a delete predicate for the primary keys in `keys`, a batch of the
/// table's primary-key columns in key order: an `IN` list for a single-column
/// key, a disjunction of one equality conjunction per key otherwise. These are
/// the shapes an accelerator can read keys from without evaluating them.
///
/// # Errors
/// Returns an error if a key value cannot be read.
pub fn build_primary_key_delete_expr(keys: &RecordBatch) -> Result<Option<Expr>> {
    if keys.num_rows() == 0 || keys.num_columns() == 0 {
        return Ok(None);
    }
    let schema = keys.schema();
    let value =
        |column: usize, row: usize| ScalarValue::try_from_array(keys.column(column), row).map(lit);
    let key_column = |column: usize| col(Column::from_name(schema.field(column).name()));
    if keys.num_columns() == 1 {
        let values = (0..keys.num_rows())
            .map(|row| value(0, row))
            .collect::<Result<Vec<_>>>()?;
        return Ok(Some(key_column(0).in_list(values, false)));
    }
    let conjunctions = (0..keys.num_rows())
        .map(|row| {
            let equalities = (0..keys.num_columns())
                .map(|column| Ok(key_column(column).eq(value(column, row)?)))
                .collect::<Result<Vec<_>>>()?;
            balanced_binary(equalities, Expr::and).ok_or_else(|| {
                DataFusionError::Internal("A primary key has no columns".to_string())
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(balanced_binary(conjunctions, Expr::or))
}

/// Project only the keys required by `Index::delete_by_keys`.
///
/// # Errors
/// Returns an error if a key column is missing or Arrow projection or row selection fails.
pub fn build_pk_only_batch_from_change_batch(
    change_batch: &ChangeBatch,
    row_indices: &[usize],
) -> Result<Option<RecordBatch>> {
    let Some(&first) = row_indices.first() else {
        return Ok(None);
    };
    let pk_names = change_batch.primary_keys(first);
    if pk_names.is_empty() {
        return Ok(None);
    }
    let data_batch = change_batch.data_batch();
    let schema = data_batch.schema();
    let projection = pk_names
        .iter()
        .map(|name| schema.index_of(name))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    select_rows(&data_batch.project(&projection)?, row_indices).map(Some)
}
