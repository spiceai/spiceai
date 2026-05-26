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

//! Primary-key filter expression construction helpers.
//!
//! This module provides functions to build `Expr` filter trees that encode a
//! set of primary key values to delete:
//!
//! - **Single PK**: `pk_col IN (v1, v2, ...)` via [`build_pk_in_list`].
//! - **Composite PK**: Balanced OR tree of AND-equality conjunctions via
//!   [`build_composite_pk_filter`].
//!
//! These are the canonical shapes recognised by the fast-path extractor in
//! Cayenne's `pk_filter_extract` module.

use std::sync::Arc;

use arrow::array::{Array, RecordBatch};
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Expr, col, lit};
use snafu::prelude::*;

/// Build `pk_col IN (v1, v2, ...)` from a list of `ScalarValue` literals.
///
/// Returns `None` if `values` is empty.
#[must_use]
pub fn build_pk_in_list(pk_name: &str, values: Vec<ScalarValue>) -> Option<Expr> {
    if values.is_empty() {
        return None;
    }
    let literals: Vec<Expr> = values.into_iter().map(|v| Expr::Literal(v, None)).collect();
    Some(col(pk_name).in_list(literals, false))
}

/// Build a balanced OR tree of AND-equality conjunctions for composite PKs.
///
/// Each entry in `rows` is a list of `(column_name, value)` pairs — one per
/// PK column, in declaration order. The result is:
///
/// ```text
/// (pk1 = a AND pk2 = b) OR (pk1 = c AND pk2 = d) OR ...
/// ```
///
/// Returns `None` if `rows` is empty or any row is empty.
#[must_use]
pub fn build_composite_pk_filter(rows: &[Vec<(&str, ScalarValue)>]) -> Option<Expr> {
    let mut row_conditions: Vec<Expr> = Vec::with_capacity(rows.len());
    for row in rows {
        let eq_exprs: Vec<Expr> = row
            .iter()
            .map(|(col_name, val)| col(*col_name).eq(Expr::Literal(val.clone(), None)))
            .collect();
        // Return None if any row produces no equalities (empty row).
        row_conditions.push(balanced_binary(eq_exprs, Expr::and)?);
    }
    balanced_binary(row_conditions, Expr::or)
}

/// Build a balanced binary tree of expressions to avoid deep nesting.
///
/// Instead of creating a right-associative chain like `OP(a, OP(b, OP(c, d)))`
/// which has O(n) depth and risks stack overflow when cloned, this creates a
/// balanced tree with O(log n) depth.
pub fn balanced_binary(mut conditions: Vec<Expr>, op: fn(Expr, Expr) -> Expr) -> Option<Expr> {
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

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Expected schema to have field '{field_name}', but it did not. Spice found the schema: {schema} Is the primary key configuration correct?"
    ))]
    FieldNotFound {
        field_name: String,
        schema: SchemaRef,
    },

    #[snafu(display("Primary key type not yet supported: {data_type}"))]
    PrimaryKeyTypeNotYetSupported { data_type: String },

    #[snafu(display("Primary key column '{field_name}' has NULL value at row {row}"))]
    PrimaryKeyNullValue { field_name: String, row: usize },

    #[snafu(display(
        "Expected the field in schema '{field_name}' to have type '{expected_type}', but it did not. Spice found the schema: {schema} Is the primary key configuration correct?"
    ))]
    ArrayDowncastFailed {
        field_name: String,
        expected_type: String,
        schema: SchemaRef,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Extract the scalar value of a primary key column at a given row.
///
/// Returns the `lit(value)` expression for the cell. Supports Int32, Int64,
/// and Utf8 column types.
pub fn pk_value_at_row(data: &RecordBatch, row: usize, key: &str) -> Result<Expr> {
    let data_schema = data.schema();
    let (primary_key_idx, field) =
        data_schema
            .column_with_name(key)
            .ok_or_else(|| Error::FieldNotFound {
                field_name: key.to_string(),
                schema: Arc::clone(&data_schema),
            })?;

    let key_col = data.column(primary_key_idx);
    match field.data_type() {
        DataType::Int32 => {
            let typed = key_col
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .context(ArrayDowncastFailedSnafu {
                    field_name: key.to_string(),
                    expected_type: "Int32",
                    schema: Arc::clone(&data_schema),
                })?;
            ensure!(
                !typed.is_null(row),
                PrimaryKeyNullValueSnafu {
                    field_name: key.to_string(),
                    row
                }
            );
            Ok(lit(typed.value(row)))
        }
        DataType::Int64 => {
            let typed = key_col
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .context(ArrayDowncastFailedSnafu {
                    field_name: key.to_string(),
                    expected_type: "Int64",
                    schema: Arc::clone(&data_schema),
                })?;
            ensure!(
                !typed.is_null(row),
                PrimaryKeyNullValueSnafu {
                    field_name: key.to_string(),
                    row
                }
            );
            Ok(lit(typed.value(row)))
        }
        DataType::Utf8 => {
            let typed = key_col
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .context(ArrayDowncastFailedSnafu {
                    field_name: key.to_string(),
                    expected_type: "String",
                    schema: Arc::clone(&data_schema),
                })?;
            ensure!(
                !typed.is_null(row),
                PrimaryKeyNullValueSnafu {
                    field_name: key.to_string(),
                    row
                }
            );
            Ok(lit(typed.value(row)))
        }
        other => Err(Error::PrimaryKeyTypeNotYetSupported {
            data_type: other.to_string(),
        }),
    }
}

/// Builds an IN list expression for single-column primary key deletes.
///
/// Instead of `id = 1 OR id = 2 OR id = 3 ...` (deeply nested tree),
/// creates `id IN (1, 2, 3, ...)` which is a flat structure with O(1) depth.
pub fn build_pk_in_list_from_batch(
    row_indices: &[usize],
    primary_key: &str,
    data: &RecordBatch,
) -> Result<Expr> {
    let values: Vec<Expr> = row_indices
        .iter()
        .map(|&row| pk_value_at_row(data, row, primary_key))
        .collect::<Result<Vec<_>>>()?;

    Ok(col(primary_key).in_list(values, false))
}

/// Builds equality expressions for composite primary key deletes at a given row.
///
/// For each primary key column, produces `col(pk) = literal_value`.
pub fn get_delete_where_expr_from_batch(
    data: &RecordBatch,
    row: usize,
    primary_keys: Vec<String>,
) -> Result<Vec<Expr>> {
    let mut delete_where_exprs: Vec<Expr> = Vec::with_capacity(primary_keys.len());

    for primary_key in primary_keys {
        let expr_val = pk_value_at_row(data, row, &primary_key)?;
        delete_where_exprs.push(col(primary_key).eq(expr_val));
    }

    Ok(delete_where_exprs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn make_int64_batch(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let arr: ArrayRef = Arc::new(Int64Array::from(values.to_vec()));
        RecordBatch::try_new(schema, vec![arr]).expect("batch")
    }

    fn make_utf8_batch(values: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let arr: ArrayRef = Arc::new(StringArray::from(values.to_vec()));
        RecordBatch::try_new(schema, vec![arr]).expect("batch")
    }

    fn make_composite_batch(pks: &[i64], sks: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int64, false),
            Field::new("sk", DataType::Utf8, false),
        ]));
        let pk_arr: ArrayRef = Arc::new(Int64Array::from(pks.to_vec()));
        let sk_arr: ArrayRef = Arc::new(StringArray::from(sks.to_vec()));
        RecordBatch::try_new(schema, vec![pk_arr, sk_arr]).expect("batch")
    }

    #[test]
    fn test_pk_value_at_row_int64() {
        let batch = make_int64_batch(&[10, 20, 30]);
        let expr = pk_value_at_row(&batch, 1, "id").expect("ok");
        assert_eq!(expr, lit(20_i64));
    }

    #[test]
    fn test_pk_value_at_row_utf8() {
        let batch = make_utf8_batch(&["alice", "bob"]);
        let expr = pk_value_at_row(&batch, 0, "name").expect("ok");
        assert_eq!(expr, lit("alice"));
    }

    #[test]
    fn test_pk_value_at_row_missing_column() {
        let batch = make_int64_batch(&[1]);
        let err = pk_value_at_row(&batch, 0, "missing").expect_err("should fail");
        assert!(matches!(err, Error::FieldNotFound { .. }));
    }

    #[test]
    fn test_build_pk_in_list_from_batch_int64() {
        let batch = make_int64_batch(&[10, 20, 30]);
        let expr = build_pk_in_list_from_batch(&[0, 1, 2], "id", &batch).expect("ok");
        let Expr::InList(in_list) = &expr else {
            panic!("expected InList, got {expr:?}");
        };
        assert_eq!(in_list.list.len(), 3);
        assert!(!in_list.negated);
    }

    #[test]
    fn test_build_pk_in_list_from_batch_utf8() {
        let batch = make_utf8_batch(&["alice", "bob", "carol"]);
        let expr = build_pk_in_list_from_batch(&[0, 2], "name", &batch).expect("ok");
        let Expr::InList(in_list) = &expr else {
            panic!("expected InList, got {expr:?}");
        };
        assert_eq!(in_list.list.len(), 2);
    }

    #[test]
    fn test_get_delete_where_expr_from_batch_composite() {
        let batch = make_composite_batch(&[1, 2], &["a", "b"]);
        let exprs =
            get_delete_where_expr_from_batch(&batch, 0, vec!["pk".to_string(), "sk".to_string()])
                .expect("ok");
        assert_eq!(exprs.len(), 2);
        // Each should be an equality expression
        for expr in &exprs {
            assert!(matches!(expr, Expr::BinaryExpr(_)));
        }
    }
}
