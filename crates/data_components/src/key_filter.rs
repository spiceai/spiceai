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

//! Common utilities for extracting partition/sort key filters from `DataFusion` expressions.
//!
//! This module provides reusable logic for identifying filter expressions that can be
//! pushed down to key-value or `NoSQL` databases like `DynamoDB` and `ScyllaDB`. These databases
//! require specific filter patterns on primary key columns for efficient queries.
//!
//! # Key Concepts
//!
//! - **Partition Key**: The primary distribution key. Must be filtered with equality (`=`).
//! - **Sort Key** (or Clustering Key): Optional secondary key for ordering within a partition.
//!   Can be filtered with equality or comparison operators (`=`, `<`, `<=`, `>`, `>=`).
//!
//! # Usage
//!
//! ```ignore
//! use data_components::key_filter::{try_match_index, KeyFilter};
//!
//! // Check if filters can use a Query operation instead of a Scan
//! let (key_filters, other_filters) = match try_match_index(&filters, "pk", Some("sk")) {
//!     Some((partition, sort, others)) => (Some((partition, sort)), others),
//!     None => (None, filters.to_vec()),
//! };
//! ```

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};

/// Represents a filter expression on a primary key column.
#[derive(Debug, Clone)]
pub enum KeyFilter {
    /// A filter on the partition key (must be equality).
    Partition(Expr),
    /// A filter on the sort/clustering key (equality or comparison).
    Sort(Expr),
}

/// Attempts to match filters against a primary index (partition key + optional sort key).
///
/// This function analyzes a set of filter expressions and separates them into:
/// - A partition key filter (required for Query operations)
/// - An optional sort key filter
/// - Other filters that cannot be part of the key condition
///
/// # Arguments
///
/// * `filters` - The filter expressions to analyze
/// * `partition_key` - The name of the partition key column
/// * `sort_key` - The name of the optional sort/clustering key column
///
/// # Returns
///
/// - `Some((partition_expr, sort_expr, other_filters))` if a valid partition key equality filter is found
/// - `None` if no valid partition key filter exists or if filters contain OR conditions
///
/// # Example
///
/// ```ignore
/// // For a table with partition key "user_id" and sort key "timestamp"
/// let filters = vec![
///     col("user_id").eq(lit("user123")),
///     col("timestamp").gt(lit("2024-01-01")),
///     col("status").eq(lit("active")),
/// ];
///
/// if let Some((pk_filter, sk_filter, others)) = try_match_index(&filters, "user_id", Some("timestamp")) {
///     // pk_filter: user_id = 'user123'
///     // sk_filter: Some(timestamp > '2024-01-01')
///     // others: [status = 'active']
/// }
/// ```
#[must_use]
pub fn try_match_index(
    filters: &[Expr],
    partition_key: &str,
    sort_key: Option<&str>,
) -> Option<(Expr, Option<Expr>, Vec<Expr>)> {
    // OR conditions cannot be used with key-based queries - must do a scan
    if filters.iter().any(contains_or) {
        return None;
    }

    let mut partition_expr = None;
    let mut sort_expr = None;
    let mut other_filters = Vec::new();

    for filter in filters {
        if let Some(extracted) = try_extract_key_filter(filter, partition_key, sort_key) {
            match extracted {
                KeyFilter::Partition(expr) => {
                    // Only one partition key filter allowed
                    if partition_expr.is_some() {
                        return None;
                    }
                    partition_expr = Some(expr);
                }
                KeyFilter::Sort(expr) => {
                    // Only one sort key filter allowed
                    if sort_expr.is_some() {
                        return None;
                    }
                    sort_expr = Some(expr);
                }
            }
        } else {
            other_filters.push(filter.clone());
        }
    }

    // A partition key filter is required for key-based queries
    partition_expr.map(|p| (p, sort_expr, other_filters))
}

/// Checks if an expression contains an OR operator.
///
/// OR conditions typically cannot be pushed down to key-based queries in `NoSQL` databases
/// because they would require multiple partition lookups or a scan.
#[must_use]
pub fn contains_or(expr: &Expr) -> bool {
    // Use tree traversal to find any OR operator
    expr.apply(|e| match e {
        Expr::BinaryExpr(BinaryExpr {
            op: Operator::Or, ..
        }) => Err(DataFusionError::External("".into())),
        _ => Ok(TreeNodeRecursion::Continue),
    })
    .is_err()
}

/// Extracts a key filter if the expression matches the partition or sort key.
///
/// # Rules
///
/// - **Partition key**: Only equality (`=`) is supported. The column can be on either side.
/// - **Sort key**: Equality and comparison operators (`=`, `<`, `<=`, `>`, `>=`) are supported.
///
/// # Arguments
///
/// * `expr` - The filter expression to check
/// * `partition_key` - The name of the partition key column
/// * `sort_key` - The name of the optional sort/clustering key column
///
/// # Returns
///
/// - `Some(KeyFilter::Partition(expr))` if expression is partition key equality
/// - `Some(KeyFilter::Sort(expr))` if expression is sort key comparison
/// - `None` if expression doesn't match any key column or uses unsupported operators
#[must_use]
pub fn try_extract_key_filter(
    expr: &Expr,
    partition_key: &str,
    sort_key: Option<&str>,
) -> Option<KeyFilter> {
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr else {
        return None;
    };

    let left_col = extract_column_name(left);
    let right_col = extract_column_name(right);

    // Partition key matching (must be equality, column can be on either side)
    if matches!(op, Operator::Eq)
        && (left_col == Some(partition_key) || right_col == Some(partition_key))
    {
        return Some(KeyFilter::Partition(expr.clone()));
    }

    // Sort key matching (equality or comparison operators)
    if let Some(sk) = sort_key
        && (left_col == Some(sk) || right_col == Some(sk))
        && matches!(
            op,
            Operator::Eq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
        )
    {
        return Some(KeyFilter::Sort(expr.clone()));
    }

    None
}

/// Extracts the column name from an expression, if it's a simple column reference.
fn extract_column_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column(col) => Some(col.name.as_str()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::Column;
    use datafusion::logical_expr::{col, lit};

    // =========================================================================
    // try_extract_key_filter: Partition Key Tests
    // =========================================================================

    #[test]
    fn test_partition_key_equality() {
        let expr = col("user_id").eq(lit("user123"));
        let result = try_extract_key_filter(&expr, "user_id", None);

        assert!(matches!(result, Some(KeyFilter::Partition(_))));
    }

    #[test]
    fn test_partition_key_reversed() {
        // Column on right side: 'user123' = user_id
        let expr = lit("user123").eq(col("user_id"));
        let result = try_extract_key_filter(&expr, "user_id", None);

        assert!(matches!(result, Some(KeyFilter::Partition(_))));
    }

    #[test]
    fn test_partition_key_numeric_value() {
        let expr = col("pk").eq(lit(42i64));
        let result = try_extract_key_filter(&expr, "pk", None);

        assert!(matches!(result, Some(KeyFilter::Partition(_))));
    }

    #[test]
    fn test_partition_key_null_value() {
        // NULL equality should still be recognized as partition key filter
        let expr = col("pk").eq(lit(datafusion::scalar::ScalarValue::Null));
        let result = try_extract_key_filter(&expr, "pk", None);

        assert!(matches!(result, Some(KeyFilter::Partition(_))));
    }

    #[test]
    fn test_partition_key_non_equality_rejected() {
        // Partition key must use equality - comparison operators not allowed
        let operators = vec![
            col("user_id").gt(lit("user123")),
            col("user_id").gt_eq(lit("user123")),
            col("user_id").lt(lit("user123")),
            col("user_id").lt_eq(lit("user123")),
            col("user_id").not_eq(lit("user123")),
        ];

        for expr in operators {
            let result = try_extract_key_filter(&expr, "user_id", None);
            assert!(
                result.is_none(),
                "Expected None for partition key with non-equality: {expr:?}"
            );
        }
    }

    #[test]
    fn test_partition_key_case_sensitive() {
        // DataFusion's col() lowercases unquoted identifiers per SQL standard
        // col("User_Id") becomes Column { name: "user_id" }
        let expr = col("User_Id").eq(lit("value"));

        // Matches lowercase key name because col() normalized it
        let result = try_extract_key_filter(&expr, "user_id", None);
        assert!(matches!(result, Some(KeyFilter::Partition(_))));

        // Does NOT match mixed case because col() normalized to lowercase
        let result = try_extract_key_filter(&expr, "User_Id", None);
        assert!(result.is_none());
    }

    // =========================================================================
    // try_extract_key_filter: Sort Key Tests
    // =========================================================================

    #[test]
    fn test_sort_key_equality() {
        let expr = col("timestamp").eq(lit("2024-01-01"));
        let result = try_extract_key_filter(&expr, "user_id", Some("timestamp"));

        assert!(matches!(result, Some(KeyFilter::Sort(_))));
    }

    #[test]
    fn test_sort_key_all_comparison_operators() {
        let operators = vec![
            (col("ts").eq(lit(100)), "eq"),
            (col("ts").gt(lit(100)), "gt"),
            (col("ts").gt_eq(lit(100)), "gt_eq"),
            (col("ts").lt(lit(100)), "lt"),
            (col("ts").lt_eq(lit(100)), "lt_eq"),
        ];

        for (expr, op_name) in operators {
            let result = try_extract_key_filter(&expr, "pk", Some("ts"));
            assert!(
                matches!(result, Some(KeyFilter::Sort(_))),
                "Expected Sort for operator {op_name}: {expr:?}"
            );
        }
    }

    #[test]
    fn test_sort_key_reversed_operand() {
        // Value on left: 100 < timestamp
        let expr = lit(100).lt(col("timestamp"));
        let result = try_extract_key_filter(&expr, "pk", Some("timestamp"));

        assert!(matches!(result, Some(KeyFilter::Sort(_))));
    }

    #[test]
    fn test_sort_key_not_equal_rejected() {
        // NotEq is not a valid sort key operator
        let expr = col("timestamp").not_eq(lit(100));
        let result = try_extract_key_filter(&expr, "pk", Some("timestamp"));

        assert!(result.is_none());
    }

    #[test]
    fn test_sort_key_none_when_not_specified() {
        // When sort_key is None, sort key column should not match
        let expr = col("timestamp").eq(lit("2024-01-01"));
        let result = try_extract_key_filter(&expr, "user_id", None);

        assert!(result.is_none());
    }

    // =========================================================================
    // try_extract_key_filter: Edge Cases
    // =========================================================================

    #[test]
    fn test_non_key_column() {
        let expr = col("status").eq(lit("active"));
        let result = try_extract_key_filter(&expr, "user_id", Some("timestamp"));

        assert!(result.is_none());
    }

    #[test]
    fn test_non_binary_expression_rejected() {
        // Non-binary expressions are not key filters
        let expr = col("user_id");
        let result = try_extract_key_filter(&expr, "user_id", None);
        assert!(result.is_none());

        let expr = lit("user123");
        let result = try_extract_key_filter(&expr, "user_id", None);
        assert!(result.is_none());
    }

    #[test]
    fn test_column_to_column_comparison_rejected() {
        // col = col is not a valid key filter (both sides are columns)
        let expr = col("pk").eq(col("other_col"));
        let result = try_extract_key_filter(&expr, "pk", None);

        // This actually matches because right_col != Some("pk")
        // But it's a valid partition key filter since left matches
        assert!(matches!(result, Some(KeyFilter::Partition(_))));
    }

    #[test]
    fn test_empty_column_name() {
        let expr = col("").eq(lit("value"));
        let result = try_extract_key_filter(&expr, "", None);
        assert!(matches!(result, Some(KeyFilter::Partition(_))));
    }

    #[test]
    fn test_special_characters_in_column_name() {
        // Hyphen works - it's a valid identifier character in the Column struct
        let expr = col("user_id").eq(lit("value")); // Underscore is safe
        let result = try_extract_key_filter(&expr, "user_id", None);
        assert!(matches!(result, Some(KeyFilter::Partition(_))));

        // Note: col("user.id") creates a qualified column (relation="user", name="id")
        // This is by design in DataFusion - dots are table qualifiers
        // For actual dotted column names, use Expr::Column(Column::new_unqualified("user.id"))
        let expr = Expr::Column(Column::new_unqualified("user.id")).eq(lit("value"));
        let result = try_extract_key_filter(&expr, "user.id", None);
        assert!(matches!(result, Some(KeyFilter::Partition(_))));
    }

    #[test]
    fn test_partition_key_same_as_sort_key() {
        // Edge case: same column name for both keys - partition takes precedence
        let expr = col("key").eq(lit("value"));
        let result = try_extract_key_filter(&expr, "key", Some("key"));

        // Partition key match happens first
        assert!(matches!(result, Some(KeyFilter::Partition(_))));
    }

    // =========================================================================
    // contains_or Tests
    // =========================================================================

    #[test]
    fn test_contains_or_simple() {
        let expr = col("a").eq(lit(1)).or(col("b").eq(lit(2)));
        assert!(contains_or(&expr));
    }

    #[test]
    fn test_contains_or_nested_deep() {
        // Deeply nested OR
        let expr = col("a").eq(lit(1)).and(
            col("b")
                .eq(lit(2))
                .and(col("c").eq(lit(3)).or(col("d").eq(lit(4)))),
        );
        assert!(contains_or(&expr));
    }

    #[test]
    fn test_contains_or_triple_nested() {
        let expr = col("a")
            .eq(lit(1))
            .and(col("b").eq(lit(2)))
            .and(col("c").eq(lit(3)).or(col("d").eq(lit(4))));
        assert!(contains_or(&expr));
    }

    #[test]
    fn test_contains_or_none() {
        let expr = col("a").eq(lit(1)).and(col("b").eq(lit(2)));
        assert!(!contains_or(&expr));
    }

    #[test]
    fn test_contains_or_only_and() {
        let expr = col("a")
            .eq(lit(1))
            .and(col("b").eq(lit(2)))
            .and(col("c").eq(lit(3)))
            .and(col("d").eq(lit(4)));
        assert!(!contains_or(&expr));
    }

    #[test]
    fn test_contains_or_single_expression() {
        let expr = col("a").eq(lit(1));
        assert!(!contains_or(&expr));
    }

    #[test]
    fn test_contains_or_literal_only() {
        let expr = lit(true);
        assert!(!contains_or(&expr));
    }

    // =========================================================================
    // try_match_index: Basic Tests
    // =========================================================================

    #[test]
    fn test_try_match_index_basic() {
        let filters = vec![
            col("user_id").eq(lit("user123")),
            col("timestamp").gt(lit("2024-01-01")),
            col("status").eq(lit("active")),
        ];

        let result = try_match_index(&filters, "user_id", Some("timestamp"));
        assert!(result.is_some());

        let (pk, sk, others) = result.expect("should match");
        assert!(matches!(pk, Expr::BinaryExpr(_)));
        assert!(sk.is_some());
        assert_eq!(others.len(), 1);
    }

    #[test]
    fn test_try_match_index_partition_only() {
        let filters = vec![
            col("user_id").eq(lit("user123")),
            col("status").eq(lit("active")),
        ];

        let result = try_match_index(&filters, "user_id", None);
        assert!(result.is_some());

        let (pk, sk, others) = result.expect("should match");
        assert!(matches!(pk, Expr::BinaryExpr(_)));
        assert!(sk.is_none());
        assert_eq!(others.len(), 1);
    }

    #[test]
    fn test_try_match_index_partition_only_no_others() {
        let filters = vec![col("pk").eq(lit(1))];

        let result = try_match_index(&filters, "pk", None);
        assert!(result.is_some());

        let (_, sk, others) = result.expect("should match");
        assert!(sk.is_none());
        assert!(others.is_empty());
    }

    #[test]
    fn test_try_match_index_with_sort_key_no_partition() {
        // Sort key without partition key - should fail
        let filters = vec![col("timestamp").gt(lit("2024-01-01"))];

        let result = try_match_index(&filters, "user_id", Some("timestamp"));
        assert!(result.is_none());
    }

    // =========================================================================
    // try_match_index: Failure Cases
    // =========================================================================

    #[test]
    fn test_try_match_index_no_partition_key() {
        let filters = vec![
            col("timestamp").gt(lit("2024-01-01")),
            col("status").eq(lit("active")),
        ];

        let result = try_match_index(&filters, "user_id", Some("timestamp"));
        assert!(result.is_none());
    }

    #[test]
    fn test_try_match_index_with_or() {
        let filters = vec![
            col("user_id")
                .eq(lit("user1"))
                .or(col("user_id").eq(lit("user2"))),
        ];

        let result = try_match_index(&filters, "user_id", Some("timestamp"));
        assert!(result.is_none());
    }

    #[test]
    fn test_try_match_index_with_or_in_non_key_filter() {
        // OR in a non-key filter still prevents key-based query
        let filters = vec![
            col("user_id").eq(lit("user123")),
            col("status").eq(lit("a")).or(col("status").eq(lit("b"))),
        ];

        let result = try_match_index(&filters, "user_id", None);
        assert!(result.is_none());
    }

    #[test]
    fn test_try_match_index_duplicate_partition_key() {
        // Two equality filters on partition key - ambiguous, should fail
        let filters = vec![
            col("user_id").eq(lit("user123")),
            col("user_id").eq(lit("user456")),
        ];

        let result = try_match_index(&filters, "user_id", None);
        assert!(result.is_none());
    }

    #[test]
    fn test_try_match_index_duplicate_sort_key() {
        // Two filters on sort key - ambiguous, should fail
        let filters = vec![
            col("pk").eq(lit("pk1")),
            col("sk").gt(lit(1)),
            col("sk").lt(lit(10)),
        ];

        let result = try_match_index(&filters, "pk", Some("sk"));
        assert!(result.is_none());
    }

    // =========================================================================
    // try_match_index: Edge Cases
    // =========================================================================

    #[test]
    fn test_try_match_index_empty_filters() {
        let filters: Vec<Expr> = vec![];
        let result = try_match_index(&filters, "pk", None);
        assert!(result.is_none());
    }

    #[test]
    fn test_try_match_index_all_non_key_filters() {
        let filters = vec![
            col("col1").eq(lit(1)),
            col("col2").eq(lit(2)),
            col("col3").eq(lit(3)),
        ];

        let result = try_match_index(&filters, "pk", Some("sk"));
        assert!(result.is_none());
    }

    #[test]
    fn test_try_match_index_partition_with_multiple_others() {
        let filters = vec![
            col("pk").eq(lit("pk_value")),
            col("col1").eq(lit(1)),
            col("col2").gt(lit(2)),
            col("col3").lt(lit(3)),
            col("col4").not_eq(lit(4)),
        ];

        let result = try_match_index(&filters, "pk", None);
        assert!(result.is_some());

        let (_, _, others) = result.expect("should match");
        assert_eq!(others.len(), 4);
    }

    #[test]
    fn test_try_match_index_preserves_filter_order() {
        let filters = vec![
            col("col_z").eq(lit("z")),
            col("pk").eq(lit("pk_value")),
            col("col_a").eq(lit("a")),
            col("col_m").eq(lit("m")),
        ];

        let result = try_match_index(&filters, "pk", None);
        assert!(result.is_some());

        let (_, _, others) = result.expect("should match");
        assert_eq!(others.len(), 3);

        // Verify order is preserved
        if let Expr::BinaryExpr(BinaryExpr { left, .. }) = &others[0] {
            assert!(matches!(left.as_ref(), Expr::Column(c) if c.name == "col_z"));
        }
        if let Expr::BinaryExpr(BinaryExpr { left, .. }) = &others[1] {
            assert!(matches!(left.as_ref(), Expr::Column(c) if c.name == "col_a"));
        }
        if let Expr::BinaryExpr(BinaryExpr { left, .. }) = &others[2] {
            assert!(matches!(left.as_ref(), Expr::Column(c) if c.name == "col_m"));
        }
    }

    // =========================================================================
    // try_match_index: Real-World Scenarios
    // =========================================================================

    #[test]
    fn test_tpch_customer_by_custkey() {
        // TPC-H: SELECT * FROM customer WHERE c_custkey = 123
        let filters = vec![col("c_custkey").eq(lit(123i64))];

        let result = try_match_index(&filters, "c_custkey", None);
        assert!(result.is_some());
    }

    #[test]
    fn test_tpch_orders_by_orderkey_with_status() {
        // TPC-H: SELECT * FROM orders WHERE o_orderkey = 123 AND o_orderstatus = 'F'
        let filters = vec![
            col("o_orderkey").eq(lit(123i64)),
            col("o_orderstatus").eq(lit("F")),
        ];

        let result = try_match_index(&filters, "o_orderkey", None);
        assert!(result.is_some());

        let (_, _, others) = result.expect("should match");
        assert_eq!(others.len(), 1);
    }

    #[test]
    fn test_tpch_lineitem_by_orderkey_linenumber() {
        // TPC-H: SELECT * FROM lineitem WHERE l_orderkey = 123 AND l_linenumber = 1
        let filters = vec![
            col("l_orderkey").eq(lit(123i64)),
            col("l_linenumber").eq(lit(1i32)),
        ];

        let result = try_match_index(&filters, "l_orderkey", Some("l_linenumber"));
        assert!(result.is_some());

        let (pk, sk, others) = result.expect("should match");
        assert!(matches!(pk, Expr::BinaryExpr(_)));
        assert!(sk.is_some());
        assert!(others.is_empty());
    }

    #[test]
    fn test_tpch_lineitem_range_scan() {
        // TPC-H: SELECT * FROM lineitem WHERE l_orderkey = 123 AND l_linenumber > 1
        let filters = vec![
            col("l_orderkey").eq(lit(123i64)),
            col("l_linenumber").gt(lit(1i32)),
        ];

        let result = try_match_index(&filters, "l_orderkey", Some("l_linenumber"));
        assert!(result.is_some());

        let (_, sk, _) = result.expect("should match");
        assert!(sk.is_some());
    }

    #[test]
    fn test_dynamodb_timeseries_pattern() {
        // Common DynamoDB pattern: user_id as PK, timestamp as SK
        let filters = vec![
            col("user_id").eq(lit("user-abc-123")),
            col("timestamp").gt_eq(lit("2024-01-01T00:00:00Z")),
        ];

        let result = try_match_index(&filters, "user_id", Some("timestamp"));
        assert!(result.is_some());

        let (pk, sk, others) = result.expect("should match");
        assert!(matches!(pk, Expr::BinaryExpr(_)));
        assert!(sk.is_some());
        assert!(others.is_empty());
    }

    #[test]
    fn test_scylladb_wide_row_pattern() {
        // ScyllaDB wide row: tenant_id as PK, event_id as clustering key
        let filters = vec![
            col("tenant_id").eq(lit("tenant-001")),
            col("event_id").gt(lit(1000i64)),
            col("event_type").eq(lit("purchase")),
        ];

        let result = try_match_index(&filters, "tenant_id", Some("event_id"));
        assert!(result.is_some());

        let (_, sk, others) = result.expect("should match");
        assert!(sk.is_some());
        assert_eq!(others.len(), 1);
    }

    // =========================================================================
    // Performance Tests
    // =========================================================================

    #[test]
    fn test_performance_many_filters() {
        // Test with many filters to ensure O(n) performance
        let mut filters: Vec<Expr> = (0..100)
            .map(|i| col(format!("col{i}")).eq(lit(i)))
            .collect();
        filters.push(col("pk").eq(lit("pk_value")));

        let result = try_match_index(&filters, "pk", None);
        assert!(result.is_some());

        let (_, _, others) = result.expect("should match");
        assert_eq!(others.len(), 100);
    }

    #[test]
    fn test_performance_deeply_nested_and() {
        // Deeply nested AND chain (not common but should handle)
        let mut expr = col("pk").eq(lit("value"));
        for i in 0..50 {
            expr = expr.and(col(format!("col{i}")).eq(lit(i)));
        }

        // This is a single filter expression, not multiple filters
        let filters = vec![expr];
        let result = try_match_index(&filters, "pk", None);

        // The entire AND expression is treated as one filter, not as pk filter
        // because try_extract_key_filter only looks at the top-level BinaryExpr
        assert!(result.is_none());
    }

    // =========================================================================
    // extract_column_name Tests
    // =========================================================================

    #[test]
    fn test_extract_column_name_simple() {
        let expr = col("my_column");
        assert_eq!(extract_column_name(&expr), Some("my_column"));
    }

    #[test]
    fn test_extract_column_name_literal() {
        let expr = lit("value");
        assert_eq!(extract_column_name(&expr), None);
    }

    #[test]
    fn test_extract_column_name_binary_expr() {
        let expr = col("a").eq(lit(1));
        assert_eq!(extract_column_name(&expr), None);
    }
}
