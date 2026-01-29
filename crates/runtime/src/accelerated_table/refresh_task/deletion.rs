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
use crate::accelerated_table::refresh_task::changes::get_primary_key_value;
use arrow::array::RecordBatch;
use data_components::cdc::ChangeBatch;
use datafusion::logical_expr::{Expr, col};

pub fn build_batch_delete_expr<F, G>(
    row_indices: &[usize],
    get_primary_keys: F,
    get_row_data: G,
    dataset_name: &str,
) -> crate::accelerated_table::Result<Option<Expr>>
where
    F: Fn(usize) -> Vec<String>,
    G: Fn(usize) -> RecordBatch,
{
    if row_indices.is_empty() {
        return Ok(None);
    }

    // Get primary keys from first row to determine strategy
    let first_row_pks = get_primary_keys(row_indices[0]);
    if first_row_pks.is_empty() {
        return Err(crate::accelerated_table::Error::NoPrimaryKeysDefined {
            dataset_name: dataset_name.to_string(),
        });
    }

    // For single-column primary keys, use IN list which is much more efficient
    // as it creates a flat structure with O(1) depth instead of O(n) nested ORs
    if first_row_pks.len() == 1 {
        let pk = &first_row_pks[0];
        return Ok(Some(build_in_list_expr(row_indices, pk, &get_row_data)?));
    }

    // For composite keys, build row conditions and combine with balanced OR tree
    // to avoid stack overflow from deeply nested expressions
    let row_conditions: Vec<Expr> = row_indices
        .iter()
        .map(|&row| {
            let primary_keys = get_primary_keys(row);
            let row_data = get_row_data(row);
            let exprs = get_delete_where_expr(&row_data, primary_keys)?;
            // Use balanced AND for composite keys (typically small, but consistent)
            balanced_and(exprs).ok_or_else(|| {
                crate::accelerated_table::Error::NoPrimaryKeysDefined {
                    dataset_name: dataset_name.to_string(),
                }
            })
        })
        .collect::<crate::accelerated_table::Result<Vec<_>>>()?;

    // Use balanced OR tree instead of reduce(Expr::or) to avoid O(n) depth
    Ok(balanced_or(row_conditions))
}

/// Simplified version that works directly with `ChangeBatch`
pub fn build_batch_delete_expr_from_change_batch(
    change_batch: &ChangeBatch,
    row_indices: &[usize],
    dataset_name: &str,
) -> crate::accelerated_table::Result<Option<Expr>> {
    build_batch_delete_expr(
        row_indices,
        |row| change_batch.primary_keys(row),
        |row| change_batch.data(row),
        dataset_name,
    )
}

/// Builds a balanced binary tree of OR expressions to avoid deep nesting.
///
/// Instead of creating a right-associative chain like `OR(a, OR(b, OR(c, d)))` which
/// has O(n) depth and causes stack overflow when cloned, this creates a balanced tree
/// with O(log n) depth.
///
/// For 975 rows, depth goes from 975 to ~10.
fn balanced_or(conditions: Vec<Expr>) -> Option<Expr> {
    match conditions.len() {
        0 => None,
        1 => Some(
            conditions
                .into_iter()
                .next()
                .unwrap_or_else(|| unreachable!("len checked")),
        ),
        _ => {
            let mid = conditions.len() / 2;
            let (left_exprs, right_exprs) = conditions.split_at(mid);
            let left_exprs = left_exprs.to_vec();
            let right_exprs = right_exprs.to_vec();

            match (balanced_or(left_exprs), balanced_or(right_exprs)) {
                (Some(l), Some(r)) => Some(l.or(r)),
                (Some(l), None) => Some(l),
                (None, Some(r)) => Some(r),
                (None, None) => None,
            }
        }
    }
}

/// Builds a balanced binary tree of AND expressions to avoid deep nesting.
fn balanced_and(conditions: Vec<Expr>) -> Option<Expr> {
    match conditions.len() {
        0 => None,
        1 => Some(
            conditions
                .into_iter()
                .next()
                .unwrap_or_else(|| unreachable!("len checked")),
        ),
        _ => {
            let mid = conditions.len() / 2;
            let (left_exprs, right_exprs) = conditions.split_at(mid);
            let left_exprs = left_exprs.to_vec();
            let right_exprs = right_exprs.to_vec();

            match (balanced_and(left_exprs), balanced_and(right_exprs)) {
                (Some(l), Some(r)) => Some(l.and(r)),
                (Some(l), None) => Some(l),
                (None, Some(r)) => Some(r),
                (None, None) => None,
            }
        }
    }
}

/// Builds an IN list expression for single-column primary key deletes.
///
/// Instead of `id = 1 OR id = 2 OR id = 3 ...` (deeply nested tree),
/// creates `id IN (1, 2, 3, ...)` which is a flat structure with O(1) depth.
fn build_in_list_expr<G>(
    row_indices: &[usize],
    primary_key: &str,
    get_row_data: &G,
) -> crate::accelerated_table::Result<Expr>
where
    G: Fn(usize) -> RecordBatch,
{
    let values: Vec<Expr> = row_indices
        .iter()
        .map(|&row| {
            let row_data = get_row_data(row);
            let (_, expr_val) = get_primary_key_value(&row_data, primary_key)?;
            Ok(expr_val)
        })
        .collect::<crate::accelerated_table::Result<Vec<_>>>()?;

    Ok(col(primary_key).in_list(values, false))
}

fn get_delete_where_expr(
    data: &RecordBatch,
    primary_keys: Vec<String>,
) -> crate::accelerated_table::Result<Vec<Expr>> {
    let mut delete_where_exprs: Vec<Expr> = vec![];

    for primary_key in primary_keys {
        let (_, expr_val) = get_primary_key_value(data, &primary_key)?;
        delete_where_exprs.push(col(primary_key).eq(expr_val));
    }

    Ok(delete_where_exprs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::Operator;
    use std::sync::Arc;

    fn make_single_row_batch(pk: i64, sk: &str) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("PK", DataType::Int64, false),
            Field::new("SK", DataType::Utf8, false),
        ]));

        let pk_array: ArrayRef = Arc::new(Int64Array::from(vec![pk]));
        let sk_array: ArrayRef = Arc::new(StringArray::from(vec![sk]));

        RecordBatch::try_new(schema, vec![pk_array, sk_array]).expect("record batch")
    }

    fn make_single_key_batch(id: &str) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        let id_array: ArrayRef = Arc::new(StringArray::from(vec![id]));
        RecordBatch::try_new(schema, vec![id_array]).expect("record batch")
    }

    #[test]
    fn test_empty_row_indices_returns_none() {
        let result = build_batch_delete_expr(
            &[],
            |_| vec!["id".to_string()],
            |_| make_single_key_batch("test"),
            "test_dataset",
        )
        .expect("result");

        assert!(result.is_none());
    }

    #[test]
    fn test_single_row_single_key() {
        // Single row with single primary key: WHERE id IN ('id-5')
        let row_indices = vec![0];
        let result = build_batch_delete_expr(
            &row_indices,
            |_| vec!["id".to_string()],
            |_| make_single_key_batch("id-5"),
            "test_dataset",
        )
        .expect("result")
        .expect("result");

        // Should be: id IN ('id-5') (an IN list expression)
        if let Expr::InList(in_list) = &result {
            // Expr should be column "id"
            if let Expr::Column(col) = in_list.expr.as_ref() {
                assert_eq!(col.name, "id");
            } else {
                panic!("Expected Column in IN list, got: {:?}", in_list.expr);
            }

            // List should contain one value: "id-5"
            assert_eq!(in_list.list.len(), 1, "Expected 1 value in IN list");
            if let Expr::Literal(ScalarValue::Utf8(Some(val)), _) = &in_list.list[0] {
                assert_eq!(val, "id-5");
            } else {
                panic!(
                    "Expected Utf8 literal in IN list, got: {:?}",
                    in_list.list[0]
                );
            }

            assert!(!in_list.negated, "IN list should not be negated");
        } else {
            panic!("Expected InList, got: {result:?}");
        }
    }

    #[test]
    fn test_multiple_rows_single_key_produces_in_list() {
        // Should produce: id IN ('id-5', 'id-6', 'id-7')
        let row_indices = vec![0, 1, 2];
        let ids = ["id-5", "id-6", "id-7"];

        let result = build_batch_delete_expr(
            &row_indices,
            |_| vec!["id".to_string()],
            |row| make_single_key_batch(ids[row]),
            "test_dataset",
        )
        .expect("result")
        .expect("result");

        // Should be an IN list expression
        if let Expr::InList(in_list) = &result {
            // Expr should be column "id"
            if let Expr::Column(col) = in_list.expr.as_ref() {
                assert_eq!(col.name, "id");
            } else {
                panic!("Expected Column in IN list, got: {:?}", in_list.expr);
            }

            // List should contain 3 values
            assert_eq!(in_list.list.len(), 3, "Expected 3 values in IN list");

            // Extract values from the list
            let values: Vec<String> = in_list
                .list
                .iter()
                .map(|expr| {
                    if let Expr::Literal(ScalarValue::Utf8(Some(val)), _) = expr {
                        val.clone()
                    } else {
                        panic!("Expected Utf8 literal in IN list, got: {expr:?}");
                    }
                })
                .collect();

            assert!(values.contains(&"id-5".to_string()));
            assert!(values.contains(&"id-6".to_string()));
            assert!(values.contains(&"id-7".to_string()));

            assert!(!in_list.negated, "IN list should not be negated");
        } else {
            panic!("Expected InList, got: {result:?}");
        }
    }

    #[test]
    #[expect(clippy::similar_names)]
    fn test_single_row_composite_key_produces_and() {
        // Single row with composite key: WHERE pk=1 AND sk='300'
        let row_indices = vec![0];

        let result = build_batch_delete_expr(
            &row_indices,
            |_| vec!["PK".to_string(), "SK".to_string()],
            |_| make_single_row_batch(1, "300"),
            "test_dataset",
        )
        .expect("result")
        .expect("result");

        // Should be AND at top level
        if let Expr::BinaryExpr(binary) = &result {
            assert_eq!(binary.op, Operator::And, "Expected AND for composite key");

            // Collect the two equality conditions
            let conditions = collect_and_conditions(&result);
            assert_eq!(conditions.len(), 2, "Expected 2 AND conditions");

            // Verify we have conditions for both pk and sk
            let has_pk = conditions.iter().any(|e| is_column_eq(e, "pk"));
            let has_sk = conditions.iter().any(|e| is_column_eq(e, "sk"));
            assert!(has_pk, "Expected condition for pk");
            assert!(has_sk, "Expected condition for sk");
        } else {
            panic!("Expected BinaryExpr with AND, got: {result:?}");
        }
    }

    #[test]
    fn test_multiple_rows_composite_key_produces_or_of_ands() {
        // Should produce: (pk=1 AND sk='300') OR (pk=1 AND sk='400') OR (pk=2 AND sk='100')
        let row_indices = vec![0, 1, 2];
        let rows = [(1i64, "300"), (1i64, "400"), (2i64, "100")];

        let result = build_batch_delete_expr(
            &row_indices,
            |_| vec!["PK".to_string(), "SK".to_string()],
            |row| make_single_row_batch(rows[row].0, rows[row].1),
            "test_dataset",
        )
        .expect("result")
        .expect("result");

        // Collect top-level OR conditions
        let or_conditions = collect_or_conditions(&result);
        assert_eq!(or_conditions.len(), 3, "Expected 3 OR conditions");

        // Each OR condition should be an AND of two equality expressions
        for condition in &or_conditions {
            if let Expr::BinaryExpr(binary) = condition {
                assert_eq!(binary.op, Operator::And, "Each OR branch should be AND");
            } else {
                panic!("Expected AND expression in OR branch, got: {condition:?}");
            }
        }

        // Verify we have the expected (pk, sk) pairs
        let pairs: Vec<(i64, String)> = or_conditions
            .iter()
            .map(|expr| extract_pk_sk_values(expr))
            .collect();

        assert!(pairs.contains(&(1, "300".to_string())));
        assert!(pairs.contains(&(1, "400".to_string())));
        assert!(pairs.contains(&(2, "100".to_string())));
    }

    #[test]
    fn test_two_rows_single_key_structure() {
        // Test the exact structure: id IN ('a', 'b')
        let row_indices = vec![0, 1];
        let ids = ["a", "b"];

        let result = build_batch_delete_expr(
            &row_indices,
            |_| vec!["id".to_string()],
            |row| make_single_key_batch(ids[row]),
            "test_dataset",
        )
        .expect("result")
        .expect("result");

        // Should be an IN list expression
        if let Expr::InList(in_list) = &result {
            // Expr should be column "id"
            if let Expr::Column(col) = in_list.expr.as_ref() {
                assert_eq!(col.name, "id");
            } else {
                panic!("Expected Column in IN list, got: {:?}", in_list.expr);
            }

            // List should contain 2 values
            assert_eq!(in_list.list.len(), 2, "Expected 2 values in IN list");

            // Extract values from the list
            let values: Vec<String> = in_list
                .list
                .iter()
                .map(|expr| {
                    if let Expr::Literal(ScalarValue::Utf8(Some(val)), _) = expr {
                        val.clone()
                    } else {
                        panic!("Expected Utf8 literal in IN list, got: {expr:?}");
                    }
                })
                .collect();

            assert!(values.contains(&"a".to_string()));
            assert!(values.contains(&"b".to_string()));
        } else {
            panic!("Expected InList, got: {result:?}");
        }
    }

    #[test]
    fn test_two_rows_composite_key_structure() {
        // Test: (pk=1 AND sk='a') OR (pk=2 AND sk='b')
        let row_indices = vec![0, 1];
        let rows = [(1i64, "a"), (2i64, "b")];

        let result = build_batch_delete_expr(
            &row_indices,
            |_| vec!["PK".to_string(), "SK".to_string()],
            |row| make_single_row_batch(rows[row].0, rows[row].1),
            "test_dataset",
        )
        .expect("result")
        .expect("result");

        // Collect top-level OR conditions
        let or_conditions = collect_or_conditions(&result);
        assert_eq!(or_conditions.len(), 2, "Expected 2 OR conditions");

        // Each OR condition should be an AND
        for cond in &or_conditions {
            if let Expr::BinaryExpr(binary) = cond {
                assert_eq!(binary.op, Operator::And, "Each OR branch should be AND");
            } else {
                panic!("Expected AND in OR branch, got: {cond:?}");
            }

            // Each AND should have 2 equality conditions
            let and_conditions = collect_and_conditions(cond);
            assert_eq!(and_conditions.len(), 2, "Expected 2 AND conditions per row");
        }

        // Verify the (pk, sk) pairs
        let pairs: Vec<(i64, String)> = or_conditions
            .iter()
            .map(|expr| extract_pk_sk_values(expr))
            .collect();

        assert!(pairs.contains(&(1, "a".to_string())));
        assert!(pairs.contains(&(2, "b".to_string())));
    }

    /// Recursively collects all leaf conditions from an OR tree
    fn collect_or_conditions(expr: &Expr) -> Vec<&Expr> {
        match expr {
            Expr::BinaryExpr(binary) if binary.op == Operator::Or => {
                let mut conditions = collect_or_conditions(&binary.left);
                conditions.extend(collect_or_conditions(&binary.right));
                conditions
            }
            _ => vec![expr],
        }
    }

    /// Recursively collects all leaf conditions from an AND tree
    fn collect_and_conditions(expr: &Expr) -> Vec<&Expr> {
        match expr {
            Expr::BinaryExpr(binary) if binary.op == Operator::And => {
                let mut conditions = collect_and_conditions(&binary.left);
                conditions.extend(collect_and_conditions(&binary.right));
                conditions
            }
            _ => vec![expr],
        }
    }

    /// Checks if expression is `column_name = <something>`
    fn is_column_eq(expr: &Expr, column_name: &str) -> bool {
        if let Expr::BinaryExpr(binary) = expr
            && binary.op == Operator::Eq
            && let Expr::Column(col) = binary.left.as_ref()
        {
            return col.name == column_name;
        }
        false
    }

    /// Extracts (pk, sk) values from `pk = N AND sk = 'S'`
    fn extract_pk_sk_values(expr: &Expr) -> (i64, String) {
        let conditions = collect_and_conditions(expr);

        let mut pk_value: Option<i64> = None;
        let mut sk_value: Option<String> = None;

        for cond in conditions {
            if let Expr::BinaryExpr(binary) = cond
                && binary.op == Operator::Eq
                && let Expr::Column(col) = binary.left.as_ref()
            {
                match col.name.as_str() {
                    "pk" => {
                        if let Expr::Literal(ScalarValue::Int64(Some(v)), _) = binary.right.as_ref()
                        {
                            pk_value = Some(*v);
                        }
                    }
                    "sk" => {
                        if let Expr::Literal(ScalarValue::Utf8(Some(v)), _) = binary.right.as_ref()
                        {
                            sk_value = Some(v.clone());
                        }
                    }
                    _ => {}
                }
            }
        }

        (
            pk_value.expect("Expected pk value"),
            sk_value.expect("Expected sk value"),
        )
    }

    // ============================================================
    // Tests for balanced_or function
    // ============================================================

    #[test]
    fn test_balanced_or_empty() {
        let result = balanced_or(vec![]);
        assert!(result.is_none());
    }

    #[test]
    fn test_balanced_or_single() {
        let expr = col("a").eq(datafusion::logical_expr::lit(1));
        let result = balanced_or(vec![expr.clone()]);
        assert!(result.is_some());
        // Single element should be returned as-is
        assert_eq!(
            format!("{}", result.expect("expected Some")),
            format!("{}", expr)
        );
    }

    #[test]
    fn test_balanced_or_two_elements() {
        let expr1 = col("a").eq(datafusion::logical_expr::lit(1));
        let expr2 = col("b").eq(datafusion::logical_expr::lit(2));
        let result = balanced_or(vec![expr1, expr2]).expect("expected Some for two elements");

        // Should be a single OR
        if let Expr::BinaryExpr(binary) = &result {
            assert_eq!(binary.op, Operator::Or);
        } else {
            panic!("Expected BinaryExpr with OR, got: {result:?}");
        }
    }

    #[test]
    fn test_balanced_or_three_elements() {
        let exprs: Vec<Expr> = (0..3)
            .map(|i| col("x").eq(datafusion::logical_expr::lit(i)))
            .collect();

        let result = balanced_or(exprs).expect("expected Some for three elements");

        // Collect all OR conditions - should get 3 leaf nodes
        let conditions = collect_or_conditions(&result);
        assert_eq!(conditions.len(), 3, "Expected 3 OR conditions");
    }

    #[test]
    fn test_balanced_or_four_elements_is_balanced() {
        let exprs: Vec<Expr> = (0..4)
            .map(|i| col("x").eq(datafusion::logical_expr::lit(i)))
            .collect();

        let result = balanced_or(exprs).expect("expected Some for four elements");

        // For 4 elements, should be perfectly balanced: OR(OR(a,b), OR(c,d))
        // Depth should be 2 (log2(4) = 2)
        let depth = measure_expr_depth(&result);
        assert_eq!(depth, 2, "Expected depth 2 for 4 elements, got {depth}");

        // Verify all 4 conditions are present
        let conditions = collect_or_conditions(&result);
        assert_eq!(conditions.len(), 4, "Expected 4 OR conditions");
    }

    #[test]
    fn test_balanced_or_preserves_all_values() {
        let count = 10;
        let exprs: Vec<Expr> = (0..count)
            .map(|i| col("x").eq(datafusion::logical_expr::lit(i)))
            .collect();

        let result = balanced_or(exprs).expect("expected Some for multiple elements");

        // All original expressions should be in the tree
        let conditions = collect_or_conditions(&result);
        assert_eq!(
            conditions.len(),
            usize::try_from(count).expect("count fits in usize")
        );

        // Verify the values are 0..count
        let mut values: Vec<i32> = conditions
            .iter()
            .filter_map(|e| {
                if let Expr::BinaryExpr(binary) = e
                    && let Expr::Literal(ScalarValue::Int32(Some(v)), _) = binary.right.as_ref()
                {
                    Some(*v)
                } else {
                    None
                }
            })
            .collect();
        values.sort_unstable();
        assert_eq!(values, (0..count).collect::<Vec<_>>());
    }

    // ============================================================
    // Tests for balanced_and function
    // ============================================================

    #[test]
    fn test_balanced_and_empty() {
        let result = balanced_and(vec![]);
        assert!(result.is_none());
    }

    #[test]
    fn test_balanced_and_single() {
        let expr = col("a").eq(datafusion::logical_expr::lit(1));
        let result = balanced_and(vec![expr.clone()]);
        assert!(result.is_some());
        assert_eq!(
            format!("{}", result.expect("expected Some")),
            format!("{}", expr)
        );
    }

    #[test]
    fn test_balanced_and_two_elements() {
        let expr1 = col("a").eq(datafusion::logical_expr::lit(1));
        let expr2 = col("b").eq(datafusion::logical_expr::lit(2));
        let result = balanced_and(vec![expr1, expr2]).expect("expected Some for two elements");

        if let Expr::BinaryExpr(binary) = &result {
            assert_eq!(binary.op, Operator::And);
        } else {
            panic!("Expected BinaryExpr with AND, got: {result:?}");
        }
    }

    #[test]
    fn test_balanced_and_four_elements_is_balanced() {
        let exprs: Vec<Expr> = (0..4)
            .map(|i| col(format!("col_{i}")).eq(datafusion::logical_expr::lit(i)))
            .collect();

        let result = balanced_and(exprs).expect("expected Some for four elements");

        // For 4 elements, should be perfectly balanced: AND(AND(a,b), AND(c,d))
        let depth = measure_and_depth(&result);
        assert_eq!(depth, 2, "Expected depth 2 for 4 elements, got {depth}");

        // Verify all 4 conditions are present
        let conditions = collect_and_conditions(&result);
        assert_eq!(conditions.len(), 4, "Expected 4 AND conditions");
    }

    #[test]
    fn test_balanced_and_preserves_all_values() {
        let count = 10;
        let exprs: Vec<Expr> = (0..count)
            .map(|i| col(format!("col_{i}")).eq(datafusion::logical_expr::lit(i)))
            .collect();

        let result = balanced_and(exprs).expect("expected Some for multiple elements");

        let conditions = collect_and_conditions(&result);
        assert_eq!(
            conditions.len(),
            usize::try_from(count).expect("count fits in usize")
        );
    }

    // ============================================================
    // Tests for build_in_list_expr function
    // ============================================================

    #[test]
    fn test_build_in_list_expr_single_value() {
        let row_indices = vec![0];
        let result = build_in_list_expr(&row_indices, "id", &|_| make_single_key_batch("val1"))
            .expect("build_in_list_expr should succeed");

        if let Expr::InList(in_list) = &result {
            assert_eq!(in_list.list.len(), 1);
            assert!(!in_list.negated);
            if let Expr::Column(col) = in_list.expr.as_ref() {
                assert_eq!(col.name, "id");
            } else {
                panic!("Expected Column, got: {:?}", in_list.expr);
            }
        } else {
            panic!("Expected InList, got: {result:?}");
        }
    }

    #[test]
    fn test_build_in_list_expr_multiple_values() {
        let values = ["a", "b", "c", "d", "e"];
        let row_indices: Vec<usize> = (0..values.len()).collect();
        let result = build_in_list_expr(&row_indices, "id", &|row| {
            make_single_key_batch(values[row])
        })
        .expect("build_in_list_expr should succeed");

        if let Expr::InList(in_list) = &result {
            assert_eq!(in_list.list.len(), 5);
            assert!(!in_list.negated);

            // Extract and verify all values
            let extracted: Vec<String> = in_list
                .list
                .iter()
                .filter_map(|e| {
                    if let Expr::Literal(ScalarValue::Utf8(Some(v)), _) = e {
                        Some(v.clone())
                    } else {
                        None
                    }
                })
                .collect();

            for val in &values {
                assert!(
                    extracted.contains(&(*val).to_string()),
                    "Missing value: {val}"
                );
            }
        } else {
            panic!("Expected InList, got: {result:?}");
        }
    }

    #[test]
    fn test_build_in_list_expr_with_integer_values() {
        fn make_int_batch(id: i64) -> RecordBatch {
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
            let id_array: ArrayRef = Arc::new(Int64Array::from(vec![id]));
            RecordBatch::try_new(schema, vec![id_array]).expect("record batch")
        }

        let row_indices: Vec<usize> = (0..5).collect();
        let result = build_in_list_expr(&row_indices, "id", &|row| {
            make_int_batch(i64::try_from(row).expect("row fits in i64"))
        })
        .expect("build_in_list_expr should succeed");

        if let Expr::InList(in_list) = &result {
            assert_eq!(in_list.list.len(), 5);

            let extracted: Vec<i64> = in_list
                .list
                .iter()
                .filter_map(|e| {
                    if let Expr::Literal(ScalarValue::Int64(Some(v)), _) = e {
                        Some(*v)
                    } else {
                        None
                    }
                })
                .collect();

            for i in 0..5 {
                assert!(extracted.contains(&i), "Missing value: {i}");
            }
        } else {
            panic!("Expected InList, got: {result:?}");
        }
    }

    // ============================================================
    // Large batch tests (2000 values)
    // ============================================================

    #[test]
    fn test_large_batch_in_list_2000_values() {
        // Test that IN list handles 2000 values without issues
        let count = 2000;
        let row_indices: Vec<usize> = (0..count).collect();

        let result = build_batch_delete_expr(
            &row_indices,
            |_| vec!["id".to_string()],
            |row| make_single_key_batch(&format!("id-{row}")),
            "test_dataset",
        )
        .expect("should not error")
        .expect("should produce expression");

        // Should be an IN list with 2000 values
        if let Expr::InList(in_list) = &result {
            assert_eq!(
                in_list.list.len(),
                count,
                "Expected {count} values in IN list"
            );
            assert!(!in_list.negated);

            if let Expr::Column(col) = in_list.expr.as_ref() {
                assert_eq!(col.name, "id");
            } else {
                panic!("Expected Column in IN list");
            }

            // Spot check some values
            let has_first = in_list
                .list
                .iter()
                .any(|e| matches!(e, Expr::Literal(ScalarValue::Utf8(Some(v)), _) if v == "id-0"));
            let has_last = in_list.list.iter().any(
                |e| matches!(e, Expr::Literal(ScalarValue::Utf8(Some(v)), _) if v == "id-1999"),
            );
            let has_middle = in_list.list.iter().any(
                |e| matches!(e, Expr::Literal(ScalarValue::Utf8(Some(v)), _) if v == "id-1000"),
            );

            assert!(has_first, "Missing first value id-0");
            assert!(has_last, "Missing last value id-1999");
            assert!(has_middle, "Missing middle value id-1000");
        } else {
            panic!("Expected InList for single-key batch, got: {result:?}");
        }
    }

    #[test]
    fn test_large_batch_balanced_or_2000_values() {
        // Test that balanced OR tree handles 2000 composite key rows without stack overflow
        let count = 2000;
        let row_indices: Vec<usize> = (0..count).collect();

        let result = build_batch_delete_expr(
            &row_indices,
            |_| vec!["PK".to_string(), "SK".to_string()],
            |row| {
                make_single_row_batch(
                    i64::try_from(row).expect("row fits in i64"),
                    &format!("sk-{row}"),
                )
            },
            "test_dataset",
        )
        .expect("should not error")
        .expect("should produce expression");

        // Should be a balanced OR tree
        let or_conditions = collect_or_conditions(&result);
        assert_eq!(or_conditions.len(), count, "Expected {count} OR conditions");

        // Verify the depth is O(log n), not O(n)
        // For 2000 elements, log2(2000) ≈ 11, so depth should be around 11
        let depth = measure_expr_depth(&result);
        let max_expected_depth = 15; // Allow some margin
        assert!(
            depth <= max_expected_depth,
            "Tree depth {depth} exceeds expected max {max_expected_depth} for balanced tree"
        );

        // Verify some values are present
        let pairs: Vec<(i64, String)> = or_conditions
            .iter()
            .take(10) // Just check first 10 to keep test fast
            .map(|e| extract_pk_sk_values(e))
            .collect();

        // Check that we got valid pairs (pk values should be 0-9 for first 10)
        for (pk, sk) in &pairs {
            assert!(*pk >= 0 && *pk < i64::try_from(count).expect("count fits in i64"));
            assert!(sk.starts_with("sk-"));
        }
    }

    #[test]
    fn test_large_batch_balanced_or_can_be_cloned() {
        // This test verifies that the balanced tree can be cloned without stack overflow
        // (the original deeply-nested tree would overflow on clone)
        let count = 2000;
        let row_indices: Vec<usize> = (0..count).collect();

        let result = build_batch_delete_expr(
            &row_indices,
            |_| vec!["PK".to_string(), "SK".to_string()],
            |row| {
                make_single_row_batch(
                    i64::try_from(row).expect("row fits in i64"),
                    &format!("sk-{row}"),
                )
            },
            "test_dataset",
        )
        .expect("should not error")
        .expect("should produce expression");

        // This would cause stack overflow with deeply nested tree
        let cloned = result.clone();

        // Verify the clone has the same structure
        let original_conditions = collect_or_conditions(&result);
        let cloned_conditions = collect_or_conditions(&cloned);
        assert_eq!(original_conditions.len(), cloned_conditions.len());
    }

    #[test]
    fn test_large_batch_in_list_can_be_cloned() {
        // Verify IN list expressions can be cloned (they should always be flat)
        let count = 2000;
        let row_indices: Vec<usize> = (0..count).collect();

        let result = build_batch_delete_expr(
            &row_indices,
            |_| vec!["id".to_string()],
            |row| make_single_key_batch(&format!("id-{row}")),
            "test_dataset",
        )
        .expect("should not error")
        .expect("should produce expression");

        // This should work fine since IN list is flat
        let cloned = result.clone();

        if let (Expr::InList(orig), Expr::InList(clone)) = (&result, &cloned) {
            assert_eq!(orig.list.len(), clone.list.len());
        } else {
            panic!("Expected both to be InList");
        }
    }

    #[test]
    fn test_balanced_or_depth_is_logarithmic() {
        // Test various sizes to verify O(log n) depth
        let test_cases = [
            (1, 0),     // Single element has depth 0
            (2, 1),     // 2 elements: depth 1
            (4, 2),     // 4 elements: depth 2
            (8, 3),     // 8 elements: depth 3
            (16, 4),    // 16 elements: depth 4
            (100, 7),   // 100 elements: ceil(log2(100)) = 7
            (1000, 10), // 1000 elements: ceil(log2(1000)) = 10
        ];

        for (count, expected_depth) in test_cases {
            let exprs: Vec<Expr> = (0..count)
                .map(|i| col("x").eq(datafusion::logical_expr::lit(i)))
                .collect();

            let result = balanced_or(exprs).expect("expected Some for multiple elements");
            let actual_depth = measure_expr_depth(&result);

            assert!(
                actual_depth <= expected_depth + 1,
                "For {count} elements: expected depth ~{expected_depth}, got {actual_depth}"
            );
        }
    }

    #[test]
    fn test_balanced_and_depth_is_logarithmic() {
        let test_cases = [(1, 0), (2, 1), (4, 2), (8, 3), (16, 4)];

        for (count, expected_depth) in test_cases {
            let exprs: Vec<Expr> = (0..count)
                .map(|i| col(format!("col_{i}")).eq(datafusion::logical_expr::lit(i)))
                .collect();

            let result = balanced_and(exprs).expect("expected Some for multiple elements");
            let actual_depth = measure_and_depth(&result);

            assert!(
                actual_depth <= expected_depth + 1,
                "For {count} elements: expected depth ~{expected_depth}, got {actual_depth}"
            );
        }
    }

    /// Measures the depth of OR expressions in the tree
    fn measure_expr_depth(expr: &Expr) -> usize {
        match expr {
            Expr::BinaryExpr(binary) if binary.op == Operator::Or => {
                let left_depth = measure_expr_depth(&binary.left);
                let right_depth = measure_expr_depth(&binary.right);
                1 + std::cmp::max(left_depth, right_depth)
            }
            _ => 0,
        }
    }

    /// Measures the depth of AND expressions in the tree
    fn measure_and_depth(expr: &Expr) -> usize {
        match expr {
            Expr::BinaryExpr(binary) if binary.op == Operator::And => {
                let left_depth = measure_and_depth(&binary.left);
                let right_depth = measure_and_depth(&binary.right);
                1 + std::cmp::max(left_depth, right_depth)
            }
            _ => 0,
        }
    }
}
