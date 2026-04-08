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

//! Optimizer rule for that commutes a `Join` of two disjoint `Union`s when it can be guaranteed that the join operation can be applied pair-wise. For example, when the tables are partitioned by their respective `Join` predicates.
//!
//! # Transformation
//!
//! ```text
//! Join(Union[A1, A2, ..., An], Union[B1, B2, ..., Bn])
//! → Union[Join(A1, B1), Join(A2, B2), ..., Join(An, Bn)]
//! ```
//!
//! Currently, this is valid when:
//! 1. The join is an `INNER JOIN` with equi-join predicates
//! 2. Both sides are `Union`s (produced by `PartitionedTableScanRewrite`) with the same
//!    number of children (same executor count)
//! 3. Both tables have `bucket(N, col)` partition expressions with the **same N**, and the
//!    bucket columns match the join keys
//!
//! Because `bucket(N, col)` deterministically assigns rows to executors via
//! `hash(col) % N`, equal join keys always land on the same executor, so each
//! per-executor join produces correct, disjoint results.

use std::{fmt::Debug, sync::Arc};

use datafusion::{
    common::tree_node::Transformed,
    error::DataFusionError,
    logical_expr::{Expr, JoinType, LogicalPlan, SubqueryAlias, Union, logical_plan::Join},
    optimizer::{ApplyOrder, OptimizerRule},
    sql::TableReference,
};

use crate::analyzer_rule::TableDisjointPartitionProvider;

/// Parsed representation of a `bucket(N, column)` partition expression.
#[derive(Debug, Clone, PartialEq, Eq)]
struct BucketExpr {
    /// Number of buckets
    num_buckets: i64,
    /// Column name that is bucketed
    column: String,
}

/// An [`OptimizerRule`] that pushes joins down through unions when the underlying
/// tables are co-located via compatible `bucket(N, col)` partitioning.
///
/// # Prerequisites
///
/// This rule must run **after** [`PartitionedTableScanRewrite`] has expanded
/// `TableScan → Union[TableScan_per_executor]`, and assumes that
/// `get_partitions_from_manager` produces Union children in a deterministic
/// (sorted by executor ID) order so that child `i` on both sides corresponds
/// to the same executor.
pub struct ColocatedJoinRewrite {
    partition_provider: Arc<dyn TableDisjointPartitionProvider>,
}

impl ColocatedJoinRewrite {
    pub fn new(partition_provider: Arc<dyn TableDisjointPartitionProvider>) -> Self {
        Self { partition_provider }
    }
}

impl Debug for ColocatedJoinRewrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColocatedJoinRewrite")
            .finish_non_exhaustive()
    }
}

impl OptimizerRule for ColocatedJoinRewrite {
    fn name(&self) -> &'static str {
        "ColocatedJoinRewrite"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn datafusion::optimizer::OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        let LogicalPlan::Join(ref join) = plan else {
            return Ok(Transformed::no(plan));
        };

        // Only handle inner joins — LEFT/RIGHT/FULL joins break co-location guarantees
        // because unmatched rows from one side aren't confined to a single executor.
        if join.join_type != JoinType::Inner {
            return Ok(Transformed::no(plan));
        }

        // Must have at least one equi-join condition
        if join.on.is_empty() {
            return Ok(Transformed::no(plan));
        }

        // Extract Union children from both sides, peeling off SubqueryAlias if present.
        let Some((left_table, left_children)) = extract_union_children(&join.left) else {
            return Ok(Transformed::no(plan));
        };
        let Some((right_table, right_children)) = extract_union_children(&join.right) else {
            return Ok(Transformed::no(plan));
        };

        // Must have the same number of children (same executor count)
        if left_children.len() != right_children.len() {
            tracing::debug!(
                "ColocatedJoinRewrite: Union child count mismatch ({} vs {}) for {left_table} JOIN {right_table}",
                left_children.len(),
                right_children.len(),
            );
            return Ok(Transformed::no(plan));
        }

        // Need at least 2 children for the optimization to matter
        if left_children.len() < 2 {
            return Ok(Transformed::no(plan));
        }

        // Check co-location: both tables must use compatible bucket(N, col) schemes
        if !self.is_colocated(&left_table, &right_table, &join.on) {
            return Ok(Transformed::no(plan));
        }

        tracing::debug!(
            "ColocatedJoinRewrite: Pushing join through union for {left_table} JOIN {right_table} ({} executors)",
            left_children.len(),
        );

        // Build Union[Join(L_i, R_i)] for each executor
        let per_executor_joins: Vec<Arc<LogicalPlan>> = left_children
            .iter()
            .zip(right_children.iter())
            .map(|(left_child, right_child)| {
                let per_executor_join = LogicalPlan::Join(Join::try_new(
                    Arc::clone(left_child),
                    Arc::clone(right_child),
                    join.on.clone(),
                    join.filter.clone(),
                    join.join_type,
                    join.join_constraint,
                    join.null_equality,
                )?);
                Ok(Arc::new(per_executor_join))
            })
            .collect::<Result<Vec<_>, DataFusionError>>()?;

        let result = LogicalPlan::Union(Union::try_new(per_executor_joins)?);

        Ok(Transformed::yes(result))
    }
}

impl ColocatedJoinRewrite {
    /// Checks whether two tables are co-located — i.e. they share compatible
    /// `bucket(N, col)` partition expressions where `N` is the same and the
    /// bucket columns match the equi-join keys.
    fn is_colocated(
        &self,
        left_table: &TableReference,
        right_table: &TableReference,
        join_on: &[(Expr, Expr)],
    ) -> bool {
        let Some(left_exprs) = self
            .partition_provider
            .get_partition_expressions(left_table)
        else {
            return false;
        };
        let Some(right_exprs) = self
            .partition_provider
            .get_partition_expressions(right_table)
        else {
            return false;
        };

        // For now, only handle single-expression partitioning (one bucket expression per table)
        if left_exprs.len() != 1 || right_exprs.len() != 1 {
            tracing::debug!(
                "ColocatedJoinRewrite: Multi-expression partitioning not yet supported ({} left, {} right)",
                left_exprs.len(),
                right_exprs.len(),
            );
            return false;
        }

        let Some(left_bucket) = parse_bucket_expr(&left_exprs[0]) else {
            return false;
        };
        let Some(right_bucket) = parse_bucket_expr(&right_exprs[0]) else {
            return false;
        };

        // Same number of buckets
        if left_bucket.num_buckets != right_bucket.num_buckets {
            tracing::debug!(
                "ColocatedJoinRewrite: Bucket count mismatch ({} vs {}) for {left_table} vs {right_table}",
                left_bucket.num_buckets,
                right_bucket.num_buckets,
            );
            return false;
        }

        // Check that at least one equi-join pair matches the bucket columns.
        // The join `on` is Vec<(left_expr, right_expr)> where each is an `Expr`.
        // We need: left_expr references left_bucket.column AND right_expr references right_bucket.column.
        let matched = join_on.iter().any(|(left_expr, right_expr)| {
            expr_references_column(left_expr, &left_bucket.column)
                && expr_references_column(right_expr, &right_bucket.column)
        });

        if !matched {
            tracing::debug!(
                "ColocatedJoinRewrite: Join keys don't match bucket columns ({} / {}) for {left_table} vs {right_table}",
                left_bucket.column,
                right_bucket.column,
            );
        }

        matched
    }
}

/// Extracts Union children from a plan, peeling off `SubqueryAlias` if present.
///
/// The `PartitionedTableScanRewrite` produces plans of the form:
/// ```text
/// SubqueryAlias(table_name)
///   └── Union
///         ├── child_1 (TableScan for executor 1)
///         ├── child_2 (TableScan for executor 2)
///         └── ...
/// ```
///
/// Returns `(table_reference, union_children)` if the pattern matches.
fn extract_union_children(plan: &LogicalPlan) -> Option<(TableReference, &[Arc<LogicalPlan>])> {
    match plan {
        LogicalPlan::SubqueryAlias(SubqueryAlias { input, alias, .. }) => {
            if let LogicalPlan::Union(Union { inputs, .. }) = input.as_ref() {
                Some((alias.clone(), inputs.as_slice()))
            } else {
                None
            }
        }
        LogicalPlan::Union(Union { inputs, .. }) => {
            // Union without alias — try to extract a table name from the first child
            None.or_else(|| {
                let first = inputs.first()?;
                let table_ref = extract_table_ref(first)?;
                Some((table_ref, inputs.as_slice()))
            })
        }
        _ => None,
    }
}

/// Try to extract a [`TableReference`] from a plan node (e.g. from a `TableScan`).
fn extract_table_ref(plan: &LogicalPlan) -> Option<TableReference> {
    match plan {
        LogicalPlan::TableScan(scan) => Some(scan.table_name.clone()),
        // Recurse through single-input wrapper nodes (Filter, Projection, etc.)
        other if other.inputs().len() == 1 => extract_table_ref(other.inputs()[0]),
        _ => None,
    }
}

/// Parses a SQL partition expression string like `"bucket(7, customer_id)"` into
/// a [`BucketExpr`].
///
/// Uses simple string parsing rather than a full SQL parser to avoid dependencies.
/// Handles whitespace variations like `bucket( 7 , customer_id )`.
fn parse_bucket_expr(expr_str: &str) -> Option<BucketExpr> {
    let trimmed = expr_str.trim();

    // Case-insensitive prefix match for "bucket("
    let lower = trimmed.to_ascii_lowercase();
    if !lower.starts_with("bucket(") {
        return None;
    }
    // Strip the "bucket(" prefix (7 chars) and the trailing ")"
    let inner = &trimmed[7..];
    let inner = inner.strip_suffix(')')?;

    // Split by comma — expect exactly two parts: N and column_name
    let mut parts = inner.splitn(2, ',');
    let n_str = parts.next()?.trim();
    let col_str = parts.next()?.trim();

    let num_buckets: i64 = n_str.parse().ok()?;
    if num_buckets <= 0 {
        return None;
    }

    // Column name might be quoted — strip quotes if present
    let column = col_str
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('\'')
        .to_string();

    if column.is_empty() {
        return None;
    }

    Some(BucketExpr {
        num_buckets,
        column,
    })
}

/// Checks whether an [`Expr`] references a column with the given name.
///
/// For equi-join conditions, the expressions are typically `Expr::Column(col)`
/// where `col.name` is the column name. This handles both qualified and
/// unqualified references.
fn expr_references_column(expr: &Expr, column_name: &str) -> bool {
    match expr {
        Expr::Column(col) => col.name() == column_name,
        // TODO: Handle cast expressions like `CAST(col AS type)`
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_bucket_expr() {
        // Basic
        let result = parse_bucket_expr("bucket(7, customer_id)");
        assert_eq!(
            result,
            Some(BucketExpr {
                num_buckets: 7,
                column: "customer_id".to_string(),
            })
        );

        // With whitespace
        let result = parse_bucket_expr("bucket( 10 , order_id )");
        assert_eq!(
            result,
            Some(BucketExpr {
                num_buckets: 10,
                column: "order_id".to_string(),
            })
        );

        // Quoted column
        let result = parse_bucket_expr("bucket(3, \"my_column\")");
        assert_eq!(
            result,
            Some(BucketExpr {
                num_buckets: 3,
                column: "my_column".to_string(),
            })
        );

        // Invalid: negative N
        assert_eq!(parse_bucket_expr("bucket(-1, col)"), None);

        // Invalid: zero N
        assert_eq!(parse_bucket_expr("bucket(0, col)"), None);

        // Invalid: not a bucket expression
        assert_eq!(parse_bucket_expr("modulo(5, col)"), None);

        // Invalid: missing closing paren
        assert_eq!(parse_bucket_expr("bucket(5, col"), None);

        // Invalid: empty column
        assert_eq!(parse_bucket_expr("bucket(5, )"), None);

        // Invalid: single arg
        assert_eq!(parse_bucket_expr("bucket(5)"), None);
    }

    #[test]
    fn test_expr_references_column() {
        use datafusion::prelude::col;

        assert!(expr_references_column(&col("customer_id"), "customer_id"));
        assert!(!expr_references_column(&col("customer_id"), "order_id"));

        // Qualified column
        let qualified = Expr::Column(datafusion::common::Column::new(
            Some::<TableReference>("orders".into()),
            "customer_id",
        ));
        assert!(expr_references_column(&qualified, "customer_id"));
    }

    /// Mock partition provider for testing
    #[derive(Debug)]
    struct MockPartitionProvider {
        expressions: std::collections::HashMap<String, Vec<String>>,
    }

    impl TableDisjointPartitionProvider for MockPartitionProvider {
        fn get_partitions(
            &self,
            _table: &TableReference,
            _schema: &arrow::datatypes::SchemaRef,
        ) -> Vec<(
            Arc<dyn datafusion::datasource::TableProvider>,
            Vec<std::collections::HashMap<String, String>>,
        )> {
            vec![]
        }

        fn should_partition(&self, _tbl: &datafusion::logical_expr::TableScan) -> bool {
            false
        }

        fn get_partition_expressions(&self, table: &TableReference) -> Option<Vec<String>> {
            self.expressions.get(&table.to_string()).cloned()
        }
    }

    #[test]
    fn test_is_colocated() {
        use datafusion::prelude::col;

        let mut expressions = std::collections::HashMap::new();
        expressions.insert(
            "orders".to_string(),
            vec!["bucket(7, customer_id)".to_string()],
        );
        expressions.insert(
            "customers".to_string(),
            vec!["bucket(7, customer_id)".to_string()],
        );
        expressions.insert(
            "lineitem".to_string(),
            vec!["bucket(10, order_id)".to_string()],
        );

        let provider = Arc::new(MockPartitionProvider { expressions });
        let rule = ColocatedJoinRewrite::new(provider);

        // Same bucket scheme, matching join columns → co-located
        let on = vec![(col("customer_id"), col("customer_id"))];
        assert!(rule.is_colocated(
            &TableReference::bare("orders"),
            &TableReference::bare("customers"),
            &on,
        ));

        // Different N → not co-located
        let on = vec![(col("customer_id"), col("order_id"))];
        assert!(!rule.is_colocated(
            &TableReference::bare("orders"),
            &TableReference::bare("lineitem"),
            &on,
        ));

        // Same table, wrong join column → not co-located
        let on = vec![(col("order_date"), col("customer_id"))];
        assert!(!rule.is_colocated(
            &TableReference::bare("orders"),
            &TableReference::bare("customers"),
            &on,
        ));

        // Unknown table → not co-located
        let on = vec![(col("id"), col("id"))];
        assert!(!rule.is_colocated(
            &TableReference::bare("unknown"),
            &TableReference::bare("customers"),
            &on,
        ));
    }

    #[test]
    fn test_parse_bucket_expr_case_insensitive() {
        // The strip_prefix approach only handles lowercase "bucket(".
        // For case-insensitive matching, use the normalized approach.
        let result = parse_bucket_expr("BUCKET(5, col)");
        assert_eq!(
            result,
            Some(BucketExpr {
                num_buckets: 5,
                column: "col".to_string(),
            })
        );
    }
}
