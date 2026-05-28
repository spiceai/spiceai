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

//! Fast-path extraction of primary key values from structured deletion filters.
//!
//! Deletion filters arriving at the Cayenne deletion sink may already encode the
//! exact set of primary key values to delete. When that is the case the engine
//! can skip scanning data files entirely and write deletion vectors directly.
//!
//! This module recognises two filter shapes:
//!
//! - **Single PK**: `pk_col IN (v1, v2, ...)` — a flat `Expr::InList`.
//! - **Composite PK**: A balanced OR tree of AND-equality conjunctions, e.g.
//!   `(pk1 = a AND pk2 = b) OR (pk1 = c AND pk2 = d)`.
//!

use arrow::array::ArrayRef;
use arrow_row::RowConverter;
use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::Expr;
use datafusion_expr::Operator;

const MAX_PK_FILTER_TREE_NODES: usize = 65_536;

/// PK values extracted directly from structured deletion filters
pub(crate) enum ExtractedPkDeletes {
    /// Single-column Int64 primary key values from an `IN` list.
    Int64(Vec<i64>),
    /// Primary key row keys (serialised via `RowConverter`).
    RowKeys(Vec<Box<[u8]>>),
}

// ---------------------------------------------------------------------------
// Single-PK Int64 extraction
// ---------------------------------------------------------------------------

/// Try to extract Int64 PK values from an `IN` list expression.
///
/// Matches: `pk_col IN (v1, v2, ...)` where every literal is an integer scalar that can be widened to `i64`.
pub(crate) fn try_extract_int64_in_list(expr: &Expr, pk_name: &str) -> Option<Vec<i64>> {
    let Expr::InList(in_list) = expr else {
        return None;
    };
    if in_list.negated {
        return None;
    }
    let Expr::Column(col) = in_list.expr.as_ref() else {
        return None;
    };
    if col.name != pk_name {
        return None;
    }

    let mut values = Vec::with_capacity(in_list.list.len());
    for item in &in_list.list {
        match item {
            Expr::Literal(ScalarValue::Int64(Some(v)), _) => values.push(*v),
            Expr::Literal(ScalarValue::Int32(Some(v)), _) => values.push(i64::from(*v)),
            Expr::Literal(ScalarValue::Int16(Some(v)), _) => values.push(i64::from(*v)),
            Expr::Literal(ScalarValue::Int8(Some(v)), _) => values.push(i64::from(*v)),
            _ => return None,
        }
    }
    Some(values)
}

// ---------------------------------------------------------------------------
// Single-PK non-Int64 (RowConverter) extraction
// ---------------------------------------------------------------------------

/// Try to extract row keys from an `IN` list expression for a single non-Int64 PK.
///
/// Each literal is converted to a single-element Arrow array, cast to the
/// schema's data type if needed, and serialised via the `RowConverter`.
pub(crate) fn try_extract_in_list_row_keys(
    expr: &Expr,
    pk_name: &str,
    target_type: &DataType,
    row_converter: &RowConverter,
) -> Option<Vec<Box<[u8]>>> {
    let Expr::InList(in_list) = expr else {
        return None;
    };
    if in_list.negated {
        return None;
    }
    let Expr::Column(col) = in_list.expr.as_ref() else {
        return None;
    };
    if col.name != pk_name {
        return None;
    }

    let mut scalars = Vec::with_capacity(in_list.list.len());
    for item in &in_list.list {
        let Expr::Literal(scalar, _) = item else {
            return None;
        };
        scalars.push(scalar.clone());
    }

    let array = ScalarValue::iter_to_array(scalars.into_iter()).ok()?;
    let array = if array.data_type() == target_type {
        array
    } else {
        arrow::compute::cast(&array, target_type).ok()?
    };
    if array.null_count() > 0 {
        return None;
    }

    let rows = row_converter.convert_columns(&[array]).ok()?;
    Some(rows.iter().map(|row| row.as_ref().into()).collect())
}

// ---------------------------------------------------------------------------
// Composite-PK extraction (balanced OR-of-AND equality tree)
// ---------------------------------------------------------------------------

/// Try to extract composite PK row keys from a balanced OR-of-AND equality tree.
///
/// Each leaf AND conjunction is `pk1 = v1 AND pk2 = v2 AND ...`. The scalar
/// values are collected in PK column order, converted to Arrow arrays, and
/// serialised via the `RowConverter`.
///
/// - `pk_columns`: PK column names from the table schema, in declaration order.
/// - `pk_target_types`: the corresponding Arrow data types from the table schema
///   (same length as `pk_columns`). Used to cast filter literals when needed.
pub(crate) fn try_extract_composite_pk_keys(
    expr: &Expr,
    pk_columns: &[String],
    pk_target_types: &[&DataType],
    row_converter: &RowConverter,
) -> Option<Vec<Box<[u8]>>> {
    if pk_columns.len() != pk_target_types.len() {
        return None;
    }
    let mut and_conjunctions: Vec<&Expr> = Vec::new();
    if !collect_or_leaves(expr, &mut and_conjunctions) {
        return None;
    }
    if and_conjunctions.is_empty() {
        return None;
    }

    let mut pk_column_values: Vec<Vec<ScalarValue>> = pk_columns
        .iter()
        .map(|_| Vec::with_capacity(and_conjunctions.len()))
        .collect();

    for conjunction in &and_conjunctions {
        let mut eq_pairs: Vec<(&Expr, &Expr)> = Vec::new();
        if !collect_and_eq_pairs(conjunction, &mut eq_pairs) {
            return None;
        }
        if eq_pairs.len() != pk_columns.len() {
            return None;
        }

        for (pk_idx, pk_col_name) in pk_columns.iter().enumerate() {
            let scalar = find_scalar_for_column(&eq_pairs, pk_col_name)?;
            pk_column_values[pk_idx].push(scalar.clone());
        }
    }

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(pk_columns.len());
    for (pk_idx, values) in pk_column_values.into_iter().enumerate() {
        let array = ScalarValue::iter_to_array(values.into_iter()).ok()?;
        let target_type = pk_target_types[pk_idx];
        let array = if array.data_type() == target_type {
            array
        } else {
            arrow::compute::cast(&array, target_type).ok()?
        };
        if array.null_count() > 0 {
            return None;
        }
        arrays.push(array);
    }

    let rows = row_converter.convert_columns(&arrays).ok()?;
    Some(rows.iter().map(|row| row.as_ref().into()).collect())
}

// ---------------------------------------------------------------------------
// Tree-walking helpers
// ---------------------------------------------------------------------------

/// Iteratively collect leaf expressions from a balanced OR tree.
/// Non-OR nodes are treated as leaves.
fn collect_or_leaves<'a>(expr: &'a Expr, leaves: &mut Vec<&'a Expr>) -> bool {
    let mut stack = vec![expr];
    let mut visited = 0usize;

    while let Some(current) = stack.pop() {
        visited += 1;
        if visited > MAX_PK_FILTER_TREE_NODES {
            return false;
        }

        if let Expr::BinaryExpr(bin) = current
            && bin.op == Operator::Or
        {
            stack.push(&bin.right);
            stack.push(&bin.left);
            continue;
        }
        leaves.push(current);
    }

    true
}

/// Iteratively collect `(left, right)` pairs from an AND tree of equalities.
fn collect_and_eq_pairs<'a>(expr: &'a Expr, pairs: &mut Vec<(&'a Expr, &'a Expr)>) -> bool {
    let mut stack = vec![expr];
    let mut visited = 0usize;

    while let Some(current) = stack.pop() {
        visited += 1;
        if visited > MAX_PK_FILTER_TREE_NODES {
            return false;
        }

        let Expr::BinaryExpr(bin) = current else {
            return false;
        };

        match bin.op {
            Operator::And => {
                stack.push(&bin.right);
                stack.push(&bin.left);
            }
            Operator::Eq => pairs.push((&bin.left, &bin.right)),
            _ => return false,
        }
    }

    true
}

/// Find the `ScalarValue` paired with a specific column name in equality pairs.
fn find_scalar_for_column<'a>(
    pairs: &[(&'a Expr, &'a Expr)],
    col_name: &str,
) -> Option<&'a ScalarValue> {
    for (left, right) in pairs {
        if let (Expr::Column(c), Expr::Literal(sv, _)) = (left, right)
            && c.name == col_name
        {
            return Some(sv);
        }
        if let (Expr::Literal(sv, _), Expr::Column(c)) = (left, right)
            && c.name == col_name
        {
            return Some(sv);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use arrow_row::SortField;
    use data_components::pk_filter_expr::{
        balanced_binary, build_pk_in_list_from_batch, get_delete_where_expr_from_batch,
    };
    use datafusion_expr::col;
    use std::sync::Arc;

    // -----------------------------------------------------------------------
    // Int64 InList tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_int64_in_list_basic() {
        let expr = col("id").in_list(
            vec![
                Expr::Literal(ScalarValue::Int64(Some(1)), None),
                Expr::Literal(ScalarValue::Int64(Some(2)), None),
                Expr::Literal(ScalarValue::Int64(Some(3)), None),
            ],
            false,
        );
        let result = try_extract_int64_in_list(&expr, "id");
        assert_eq!(result, Some(vec![1, 2, 3]));
    }

    #[test]
    fn test_int64_in_list_mixed_int_widths() {
        let expr = col("id").in_list(
            vec![
                Expr::Literal(ScalarValue::Int8(Some(10)), None),
                Expr::Literal(ScalarValue::Int16(Some(20)), None),
                Expr::Literal(ScalarValue::Int32(Some(30)), None),
                Expr::Literal(ScalarValue::Int64(Some(40)), None),
            ],
            false,
        );
        let result = try_extract_int64_in_list(&expr, "id");
        assert_eq!(result, Some(vec![10, 20, 30, 40]));
    }

    #[test]
    fn test_int64_in_list_wrong_column_returns_none() {
        let expr = col("other").in_list(
            vec![Expr::Literal(ScalarValue::Int64(Some(1)), None)],
            false,
        );
        assert!(try_extract_int64_in_list(&expr, "id").is_none());
    }

    #[test]
    fn test_int64_in_list_negated_returns_none() {
        let expr = col("id").in_list(vec![Expr::Literal(ScalarValue::Int64(Some(1)), None)], true);
        assert!(try_extract_int64_in_list(&expr, "id").is_none());
    }

    #[test]
    fn test_int64_in_list_non_integer_literal_returns_none() {
        let expr = col("id").in_list(
            vec![
                Expr::Literal(ScalarValue::Int64(Some(1)), None),
                Expr::Literal(ScalarValue::Utf8(Some("abc".to_string())), None),
            ],
            false,
        );
        assert!(try_extract_int64_in_list(&expr, "id").is_none());
    }

    #[test]
    fn test_int64_in_list_non_inlist_expr_returns_none() {
        let expr = col("id").eq(Expr::Literal(ScalarValue::Int64(Some(1)), None));
        assert!(try_extract_int64_in_list(&expr, "id").is_none());
    }

    // -----------------------------------------------------------------------
    // Single-PK RowConverter (non-Int64) InList tests
    // -----------------------------------------------------------------------

    fn make_utf8_row_converter() -> RowConverter {
        RowConverter::new(vec![SortField::new(arrow_schema::DataType::Utf8)]).expect("RowConverter")
    }

    #[test]
    fn test_in_list_row_keys_utf8() {
        let converter = make_utf8_row_converter();
        let expr = col("name").in_list(
            vec![
                Expr::Literal(ScalarValue::Utf8(Some("alice".to_string())), None),
                Expr::Literal(ScalarValue::Utf8(Some("bob".to_string())), None),
            ],
            false,
        );
        let result =
            try_extract_in_list_row_keys(&expr, "name", &arrow_schema::DataType::Utf8, &converter);
        assert!(result.is_some());
        let keys = result.expect("keys");
        assert_eq!(keys.len(), 2);
        // Keys must be non-empty and distinct.
        assert_ne!(keys[0], keys[1]);
    }

    #[test]
    fn test_in_list_row_keys_wrong_column_returns_none() {
        let converter = make_utf8_row_converter();
        let expr = col("other").in_list(
            vec![Expr::Literal(
                ScalarValue::Utf8(Some("x".to_string())),
                None,
            )],
            false,
        );
        assert!(
            try_extract_in_list_row_keys(&expr, "name", &arrow_schema::DataType::Utf8, &converter)
                .is_none()
        );
    }

    #[test]
    fn test_in_list_row_keys_negated_returns_none() {
        let converter = make_utf8_row_converter();
        let expr = col("name").in_list(
            vec![Expr::Literal(
                ScalarValue::Utf8(Some("x".to_string())),
                None,
            )],
            true,
        );
        assert!(
            try_extract_in_list_row_keys(&expr, "name", &arrow_schema::DataType::Utf8, &converter)
                .is_none()
        );
    }

    // -----------------------------------------------------------------------
    // Composite PK extraction tests
    // -----------------------------------------------------------------------

    fn make_composite_converter() -> RowConverter {
        RowConverter::new(vec![
            SortField::new(arrow_schema::DataType::Int64),
            SortField::new(arrow_schema::DataType::Utf8),
        ])
        .expect("RowConverter")
    }

    /// Build `(pk = v1 AND sk = v2)` conjunction.
    fn make_and_conjunction(pk_val: i64, sk_val: &str) -> Expr {
        col("pk")
            .eq(Expr::Literal(ScalarValue::Int64(Some(pk_val)), None))
            .and(col("sk").eq(Expr::Literal(
                ScalarValue::Utf8(Some(sk_val.to_string())),
                None,
            )))
    }

    #[test]
    fn test_composite_pk_single_row() {
        let converter = make_composite_converter();
        let pk_columns = vec!["pk".to_string(), "sk".to_string()];
        let target_types = vec![
            &arrow_schema::DataType::Int64 as &DataType,
            &arrow_schema::DataType::Utf8 as &DataType,
        ];

        let expr = make_and_conjunction(1, "hello");
        let result = try_extract_composite_pk_keys(&expr, &pk_columns, &target_types, &converter);
        assert!(result.is_some());
        assert_eq!(result.expect("keys").len(), 1);
    }

    #[test]
    fn test_composite_pk_multiple_rows() {
        let converter = make_composite_converter();
        let pk_columns = vec!["pk".to_string(), "sk".to_string()];
        let target_types = vec![
            &arrow_schema::DataType::Int64 as &DataType,
            &arrow_schema::DataType::Utf8 as &DataType,
        ];

        // (pk=1 AND sk='a') OR (pk=2 AND sk='b') OR (pk=3 AND sk='c')
        let expr = make_and_conjunction(1, "a")
            .or(make_and_conjunction(2, "b"))
            .or(make_and_conjunction(3, "c"));

        let result = try_extract_composite_pk_keys(&expr, &pk_columns, &target_types, &converter);
        assert!(result.is_some());
        let keys = result.expect("keys");
        assert_eq!(keys.len(), 3);
        // All keys must be distinct.
        assert_ne!(keys[0], keys[1]);
        assert_ne!(keys[1], keys[2]);
    }

    #[test]
    fn test_composite_pk_wrong_column_count_returns_none() {
        let converter = make_composite_converter();
        let pk_columns = vec!["pk".to_string(), "sk".to_string()];
        let target_types = vec![
            &arrow_schema::DataType::Int64 as &DataType,
            &arrow_schema::DataType::Utf8 as &DataType,
        ];

        // Only one equality instead of two — should fail.
        let expr = col("pk").eq(Expr::Literal(ScalarValue::Int64(Some(1)), None));
        assert!(
            try_extract_composite_pk_keys(&expr, &pk_columns, &target_types, &converter).is_none()
        );
    }

    #[test]
    fn test_composite_pk_missing_column_returns_none() {
        let converter = make_composite_converter();
        let pk_columns = vec!["pk".to_string(), "sk".to_string()];
        let target_types = vec![
            &arrow_schema::DataType::Int64 as &DataType,
            &arrow_schema::DataType::Utf8 as &DataType,
        ];

        // Two equalities but wrong column name.
        let expr = col("pk")
            .eq(Expr::Literal(ScalarValue::Int64(Some(1)), None))
            .and(col("wrong").eq(Expr::Literal(
                ScalarValue::Utf8(Some("x".to_string())),
                None,
            )));
        assert!(
            try_extract_composite_pk_keys(&expr, &pk_columns, &target_types, &converter).is_none()
        );
    }

    #[test]
    fn test_composite_pk_extra_predicate_returns_none() {
        let converter = make_composite_converter();
        let pk_columns = vec!["pk".to_string(), "sk".to_string()];
        let target_types = vec![
            &arrow_schema::DataType::Int64 as &DataType,
            &arrow_schema::DataType::Utf8 as &DataType,
        ];

        let expr = make_and_conjunction(1, "x")
            .and(col("other").gt(Expr::Literal(ScalarValue::Int64(Some(0)), None)));
        assert!(
            try_extract_composite_pk_keys(&expr, &pk_columns, &target_types, &converter).is_none()
        );
    }

    // -----------------------------------------------------------------------
    // Tree-walking helper tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_collect_or_leaves_flat() {
        let a = col("a").eq(Expr::Literal(ScalarValue::Int64(Some(1)), None));
        let b = col("b").eq(Expr::Literal(ScalarValue::Int64(Some(2)), None));
        let c = col("c").eq(Expr::Literal(ScalarValue::Int64(Some(3)), None));
        let expr = a.or(b).or(c);
        let mut leaves = Vec::new();
        assert!(collect_or_leaves(&expr, &mut leaves));
        assert_eq!(leaves.len(), 3);
    }

    #[test]
    fn test_collect_or_leaves_single_non_or() {
        let expr = col("a").eq(Expr::Literal(ScalarValue::Int64(Some(1)), None));
        let mut leaves = Vec::new();
        assert!(collect_or_leaves(&expr, &mut leaves));
        assert_eq!(leaves.len(), 1);
    }

    #[test]
    fn test_collect_and_eq_pairs() {
        let expr = col("pk")
            .eq(Expr::Literal(ScalarValue::Int64(Some(1)), None))
            .and(col("sk").eq(Expr::Literal(
                ScalarValue::Utf8(Some("x".to_string())),
                None,
            )));
        let mut pairs = Vec::new();
        assert!(collect_and_eq_pairs(&expr, &mut pairs));
        assert_eq!(pairs.len(), 2);
    }

    #[test]
    fn test_collect_and_eq_pairs_rejects_non_equality_predicate() {
        let expr = make_and_conjunction(1, "x")
            .and(col("other").gt(Expr::Literal(ScalarValue::Int64(Some(0)), None)));
        let mut pairs = Vec::new();
        assert!(!collect_and_eq_pairs(&expr, &mut pairs));
    }

    #[test]
    fn test_find_scalar_for_column_found() {
        let left = Expr::Column(datafusion_common::Column::new_unqualified("pk"));
        let right = Expr::Literal(ScalarValue::Int64(Some(42)), None);
        let pairs = vec![(&left, &right)];
        let result = find_scalar_for_column(&pairs, "pk");
        assert_eq!(result, Some(&ScalarValue::Int64(Some(42))));
    }

    #[test]
    fn test_find_scalar_for_column_reversed() {
        let left = Expr::Literal(ScalarValue::Int64(Some(42)), None);
        let right = Expr::Column(datafusion_common::Column::new_unqualified("pk"));
        let pairs = vec![(&left, &right)];
        let result = find_scalar_for_column(&pairs, "pk");
        assert_eq!(result, Some(&ScalarValue::Int64(Some(42))));
    }

    #[test]
    fn test_find_scalar_for_column_not_found() {
        let left = Expr::Column(datafusion_common::Column::new_unqualified("other"));
        let right = Expr::Literal(ScalarValue::Int64(Some(42)), None);
        let pairs = vec![(&left, &right)];
        assert!(find_scalar_for_column(&pairs, "pk").is_none());
    }

    // -----------------------------------------------------------------------
    // CDC round-trip tests: construct filter expressions the same way CDC does from RecordBatches,
    // then verify we can extract the same PK values back.
    // -----------------------------------------------------------------------

    fn make_int64_pk_batch(values: &[i64]) -> arrow::array::RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let arr: ArrayRef = Arc::new(Int64Array::from(values.to_vec()));
        arrow::array::RecordBatch::try_new(schema, vec![arr]).expect("batch")
    }

    fn make_utf8_pk_batch(values: &[&str]) -> arrow::array::RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let arr: ArrayRef = Arc::new(StringArray::from(values.to_vec()));
        arrow::array::RecordBatch::try_new(schema, vec![arr]).expect("batch")
    }

    fn make_composite_pk_batch(pks: &[i64], sks: &[&str]) -> arrow::array::RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int64, false),
            Field::new("sk", DataType::Utf8, false),
        ]));
        let pk_arr: ArrayRef = Arc::new(Int64Array::from(pks.to_vec()));
        let sk_arr: ArrayRef = Arc::new(StringArray::from(sks.to_vec()));
        arrow::array::RecordBatch::try_new(schema, vec![pk_arr, sk_arr]).expect("batch")
    }

    #[test]
    fn test_roundtrip_int64_in_list() {
        let batch = make_int64_pk_batch(&[10, 20, 30]);
        let expr = build_pk_in_list_from_batch(&[0, 1, 2], "id", &batch).expect("build");
        let extracted = try_extract_int64_in_list(&expr, "id").expect("should extract");
        assert_eq!(extracted, vec![10, 20, 30]);
    }

    #[test]
    fn test_roundtrip_utf8_in_list_row_keys() {
        let batch = make_utf8_pk_batch(&["alice", "bob"]);
        let expr = build_pk_in_list_from_batch(&[0, 1], "name", &batch).expect("build");

        let sort_fields = vec![SortField::new(DataType::Utf8)];
        let converter = RowConverter::new(sort_fields).expect("converter");

        let keys = try_extract_in_list_row_keys(&expr, "name", &DataType::Utf8, &converter)
            .expect("should extract");
        assert_eq!(keys.len(), 2);

        // Verify the extracted row keys match the original values.
        let original_values = vec![
            ScalarValue::Utf8(Some("alice".to_string())),
            ScalarValue::Utf8(Some("bob".to_string())),
        ];
        for (key, val) in keys.iter().zip(&original_values) {
            let arr: ArrayRef = val.to_array().expect("to_array");
            let rows = converter.convert_columns(&[arr]).expect("convert");
            assert_eq!(&**key, rows.row(0).as_ref());
        }
    }

    #[test]
    fn test_roundtrip_composite_pk() {
        let batch = make_composite_pk_batch(&[1, 2, 3], &["a", "b", "c"]);

        // Build the filter the same way CDC does: per-row AND conjunctions
        // combined into a balanced OR tree.
        let pk_names = vec!["pk".to_string(), "sk".to_string()];
        let row_conditions: Vec<Expr> = (0..3)
            .map(|row| {
                let exprs =
                    get_delete_where_expr_from_batch(&batch, row, pk_names.clone()).expect("build");
                balanced_binary(exprs, Expr::and).expect("and")
            })
            .collect();
        let expr = balanced_binary(row_conditions, Expr::or).expect("or");

        let pk_columns = vec!["pk".to_string(), "sk".to_string()];
        let target_types: Vec<&DataType> = vec![&DataType::Int64, &DataType::Utf8];
        let sort_fields = vec![
            SortField::new(DataType::Int64),
            SortField::new(DataType::Utf8),
        ];
        let converter = RowConverter::new(sort_fields).expect("converter");

        let keys = try_extract_composite_pk_keys(&expr, &pk_columns, &target_types, &converter)
            .expect("should extract");
        assert_eq!(keys.len(), 3);

        // Verify each extracted key matches the original row.
        let original_rows: Vec<Vec<ScalarValue>> = vec![
            vec![
                ScalarValue::Int64(Some(1)),
                ScalarValue::Utf8(Some("a".to_string())),
            ],
            vec![
                ScalarValue::Int64(Some(2)),
                ScalarValue::Utf8(Some("b".to_string())),
            ],
            vec![
                ScalarValue::Int64(Some(3)),
                ScalarValue::Utf8(Some("c".to_string())),
            ],
        ];
        for (key, row_vals) in keys.iter().zip(&original_rows) {
            let arrays: Vec<ArrayRef> = row_vals
                .iter()
                .map(|val| val.to_array().expect("to_array"))
                .collect();
            let row_batch = converter.convert_columns(&arrays).expect("convert");
            assert_eq!(&**key, row_batch.row(0).as_ref());
        }
    }
}
