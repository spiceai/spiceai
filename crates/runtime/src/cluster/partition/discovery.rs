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

//! Discovery layer: query the federated source for a table's current partition values.
//!
//! The only public entry point is [`query_source_partitions`], which is called by
//! the [`runtime_cluster::context::PartitionDiscoverer`] impl on `DataFusion`. The
//! diff-and-apply logic lives in `runtime_cluster::service::PartitionService`.

use std::collections::HashMap;

use crate::{
    accelerated_table::AcceleratedTable,
    cluster::partition::{Error, PartitionDiscoverySnafu, PartitionValue, Result},
    datafusion::DataFusion,
    search::util::find_concrete_table_provider,
};
use datafusion::common::ToDFSchema;
use datafusion::sql::sqlparser::{
    ast::{self, FunctionArgExpr, FunctionArguments},
    dialect::GenericDialect,
    parser::Parser,
};
use datafusion::{prelude::SessionContext, sql::TableReference};
use snafu::prelude::*;
use spicepod::partitioning::PartitionedBy;
use util::session_state::builder_from_existing;

/// Query the source table for the partition values present right now.
///
/// If every partition expression has a statically known value set (e.g.
/// `bucket(N, col)` produces `0..N-1`), the values are generated without
/// querying the source. Otherwise a `SELECT DISTINCT` is executed against the
/// federated source.
pub(crate) async fn query_source_partitions(
    table: &TableReference,
    partitioning: &[PartitionedBy],
    df: &DataFusion,
) -> Result<Vec<PartitionValue>> {
    let table_name = table.to_string();

    tracing::debug!(
        table = %table_name,
        partitioning = ?partitioning,
        "Starting partition value discovery"
    );

    if partitioning.is_empty() {
        return Ok(Vec::new());
    }

    // Fast path: if every partition expression has a statically known value set,
    // generate partition values without querying the source table.
    if let Some(values) = try_static_partition_values(partitioning) {
        tracing::debug!(
            table = %table_name,
            partition_count = values.len(),
            "Partition values resolved statically (skipping source query)"
        );
        return Ok(values);
    }

    // Slow path: query the federated source table.
    let partition_exprs: Vec<String> = partitioning
        .iter()
        .map(|p| {
            let PartitionedBy { name, expression } = p;
            format!("{expression} AS {name}")
        })
        .collect();

    if partition_exprs.is_empty() {
        return Ok(Vec::new());
    }

    let batches = execute_partition_discovery_query(df, table, partition_exprs).await?;

    let mut partition_values = Vec::new();
    for batch in batches {
        let num_rows = batch.num_rows();
        let num_cols = batch.num_columns();

        for row_idx in 0..num_rows {
            let mut value_parts = HashMap::new();
            for col_idx in 0..num_cols {
                let column = batch.column(col_idx);
                let value = if column.is_null(row_idx) {
                    None
                } else {
                    Some(
                        arrow::util::display::array_value_to_string(column, row_idx)
                            .boxed()
                            .context(PartitionDiscoverySnafu {
                                table: table_name.clone(),
                            })?,
                    )
                };
                if let Some(pname) = partitioning.get(col_idx).map(|p| p.expression.clone()) {
                    value_parts.insert(pname, value);
                }
            }
            partition_values.push(value_parts);
        }
    }

    tracing::debug!(
        table = %table_name,
        partition_count = partition_values.len(),
        "Discovered partition values"
    );

    Ok(partition_values)
}

/// Executes a SQL query against the underlying table source of an accelerated dataset to discover partition values.
///
/// This function creates a temporary, isolated `SessionContext` to execute the query. It is critical
/// to query the *federated* table (the source) rather than the accelerated table itself, as the
/// acceleration will be empty (for schedulers).
async fn execute_partition_discovery_query(
    df: &DataFusion,
    table: &TableReference,
    partition_exprs: Vec<String>,
) -> Result<Vec<arrow::record_batch::RecordBatch>> {
    let table_name = table.to_string();

    // Wait for the table provider to be registered. Cannot use
    // `wait_for_dataset_ready` here because on a cluster scheduler the
    // dataset stays `Refreshing` until executors ack their partition loads,
    // and PartitionsLoaded acks can't happen until discovery completes —
    // waiting for `Ready` would deadlock.
    df.runtime_status().wait_for_dataset_registered(table).await;

    // Must get table source of `AcceleratedTable` to get true value of partition.
    // The table may be registered directly as AcceleratedTable, or wrapped in a
    // FederatedTableProviderAdaptor when federation is enabled. Use the generic
    // unwrapping helper to handle all known wrapper types.
    let table_opt = df.get_table(table).await;
    let Some(acc) = table_opt.as_ref().and_then(|t| {
        find_concrete_table_provider::<AcceleratedTable>(t)
            .map(AcceleratedTable::get_federated_table)
    }) else {
        return Err(Error::NotAcceleratedTable {
            table: table.to_string(),
        });
    };

    let ctx = SessionContext::new_with_state(builder_from_existing(&df.ctx.state()).build());

    let provider = acc.table_provider().await;
    let schema = provider.schema();
    let df_schema = schema
        .to_dfschema()
        .boxed()
        .context(PartitionDiscoverySnafu {
            table: table_name.clone(),
        })?;

    let exprs = partition_exprs
        .iter()
        .map(|e| {
            ctx.parse_sql_expr(e, &df_schema)
                .boxed()
                .context(PartitionDiscoverySnafu {
                    table: table_name.clone(),
                })
        })
        .collect::<Result<Vec<_>>>()?;

    let df_result = ctx
        .read_table(provider)
        .boxed()
        .context(PartitionDiscoverySnafu {
            table: table_name.clone(),
        })?
        .select(exprs)
        .boxed()
        .context(PartitionDiscoverySnafu {
            table: table_name.clone(),
        })?
        .distinct()
        .boxed()
        .context(PartitionDiscoverySnafu {
            table: table_name.clone(),
        });

    let df_result = df_result?;
    if tracing::enabled!(tracing::Level::DEBUG) {
        tracing::debug!(
            table = %table_name,
            "Executing partition discovery query against federated source\n{}",
            df_result.logical_plan().display_indent(),
        );
    }
    let batches = df_result
        .collect()
        .await
        .boxed()
        .context(PartitionDiscoverySnafu {
            table: table_name.clone(),
        })?;

    let total_rows: usize = batches
        .iter()
        .map(arrow::record_batch::RecordBatch::num_rows)
        .sum();
    tracing::debug!(
        table = %table_name,
        total_rows,
        "Partition discovery query completed"
    );

    Ok(batches)
}

// ---------------------------------------------------------------------------
// Static partition value resolution (fast path)
// ---------------------------------------------------------------------------

/// Maximum number of buckets the `bucket()` UDF accepts.  Mirrors the constant
/// in `runtime-datafusion-udfs/src/bucket.rs`.
const MAX_NUM_BUCKETS: i64 = 1_000_000;

/// Attempt to resolve partition values without querying the source table.
///
/// Returns `Some(values)` when **all** of the following hold:
///   1. There is exactly one partition expression (multi-expression partitioning
///      falls back to the source query).
///   2. The expression is deterministic with a statically known value set — see
///      [`try_static_values_for_expr`] for the list of supported expressions.
///
/// Returns `None` otherwise, causing the caller to fall back to the slow path
/// (`SELECT DISTINCT … FROM source`).
fn try_static_partition_values(partitioning: &[PartitionedBy]) -> Option<Vec<PartitionValue>> {
    let [partition] = partitioning else {
        return None;
    };

    let values = try_static_values_for_expr(&partition.expression)?;

    Some(
        values
            .into_iter()
            .map(|v| HashMap::from([(partition.expression.clone(), v)]))
            .collect(),
    )
}

/// Resolve the complete set of values for a single partition expression without
/// querying the source table.
///
/// Currently supported expressions:
///   - `bucket(N, col)` — produces `[Some("0"), …, Some("N-1"), None]`. `N`
///     must be a positive integer literal (zero and negative values are
///     rejected). The `None` entry covers rows where the column value is NULL.
///
/// Returns `Some(values)` if the expression can be resolved statically, `None`
/// otherwise.  Add new match arms here to support additional expressions.
fn try_static_values_for_expr(expression: &str) -> Option<Vec<Option<String>>> {
    let expr = Parser::new(&GenericDialect)
        .try_with_sql(expression)
        .ok()?
        .parse_expr()
        .ok()?;

    match &expr {
        // bucket(N, column) → Some("0")..Some("N-1"), plus None for rows where column is NULL
        ast::Expr::Function(func) if is_function_named(func, "bucket") => {
            let n = extract_first_int_arg(func)?;
            if n <= 0 || n > MAX_NUM_BUCKETS {
                return None;
            }
            let mut values: Vec<Option<String>> = (0..n).map(|i| Some(i.to_string())).collect();
            values.push(None);
            Some(values)
        }

        // Future extensions:
        //   modulo(N, col)  → 0..N-1
        //   year(col)       → user-specified range or known bounds
        //   ...
        _ => None,
    }
}

/// Check whether a parsed function has the given name (case-insensitive).
fn is_function_named(func: &ast::Function, name: &str) -> bool {
    func.name.0.last().is_some_and(|part| match part {
        ast::ObjectNamePart::Identifier(ident) => ident.value.eq_ignore_ascii_case(name),
        ast::ObjectNamePart::Function(_) => false,
    })
}

/// Extract the first argument of a function call as an `i64`, if it is a literal integer.
fn extract_first_int_arg(func: &ast::Function) -> Option<i64> {
    let FunctionArguments::List(arg_list) = &func.args else {
        return None;
    };

    let first_arg = arg_list.args.first()?;
    match first_arg {
        ast::FunctionArg::Unnamed(FunctionArgExpr::Expr(ast::Expr::Value(
            ast::ValueWithSpan {
                value: ast::Value::Number(s, _),
                ..
            },
        ))) => s.parse::<i64>().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_try_static_values_for_expr_bucket() {
        // Valid bucket expressions — includes None partition for rows where column is NULL
        let vals = try_static_values_for_expr("bucket(50, organization_id)").expect("should parse");
        assert_eq!(vals.len(), 51); // Some("0")..Some("49") + None
        assert_eq!(vals[0], Some("0".to_string()));
        assert_eq!(vals[49], Some("49".to_string()));
        assert_eq!(vals[50], None);

        let vals = try_static_values_for_expr("bucket( 5 , c_name)").expect("should parse");
        assert_eq!(vals.len(), 6); // Some("0")..Some("4") + None

        let vals = try_static_values_for_expr("BUCKET(10, user_id)").expect("should parse");
        assert_eq!(vals.len(), 11); // Some("0")..Some("9") + None

        // bucket(1, col) → two partitions: [Some("0"), None]
        let vals = try_static_values_for_expr("bucket(1, col)").expect("should parse");
        assert_eq!(vals, vec![Some("0".to_string()), None]);

        // bucket(0, col) is meaningless → None
        assert!(try_static_values_for_expr("bucket(0, col)").is_none());

        // Negative bucket count → None (sqlparser parses `-5` as UnaryOp, not a Number literal)
        assert!(try_static_values_for_expr("bucket(-5, col)").is_none());

        // Exceeds MAX_NUM_BUCKETS → None (fall back to slow path where UDF rejects it)
        assert!(try_static_values_for_expr("bucket(2000000, col)").is_none());

        // Non-bucket expressions → None
        assert!(try_static_values_for_expr("year(created_at)").is_none());
        assert!(try_static_values_for_expr("some_column").is_none());
    }

    #[test]
    fn test_try_static_partition_values_single_bucket() {
        let partitioning = vec![PartitionedBy {
            name: "org_bucket".to_string(),
            expression: "bucket(3, org_id)".to_string(),
        }];

        let values = try_static_partition_values(&partitioning).expect("should resolve statically");
        assert_eq!(values.len(), 4); // Some("0"), Some("1"), Some("2"), None
        for i in 0..3 {
            let expected: HashMap<String, Option<String>> =
                [("bucket(3, org_id)".to_string(), Some(i.to_string()))]
                    .into_iter()
                    .collect();
            assert!(values.contains(&expected), "missing partition value {i}");
        }
        // None partition for rows where org_id is NULL
        let null_expected: HashMap<String, Option<String>> =
            [("bucket(3, org_id)".to_string(), None)]
                .into_iter()
                .collect();
        assert!(
            values.contains(&null_expected),
            "missing NULL partition value"
        );
    }

    #[test]
    fn test_try_static_partition_values_not_resolvable() {
        let partitioning = vec![PartitionedBy {
            name: "year".to_string(),
            expression: "year(created_at)".to_string(),
        }];

        assert!(try_static_partition_values(&partitioning).is_none());
    }

    #[test]
    fn test_try_static_partition_values_multiple_not_supported() {
        // Multiple partition expressions fall back to the slow path.
        let partitioning = vec![
            PartitionedBy {
                name: "a".to_string(),
                expression: "bucket(2, col_a)".to_string(),
            },
            PartitionedBy {
                name: "b".to_string(),
                expression: "bucket(3, col_b)".to_string(),
            },
        ];

        assert!(try_static_partition_values(&partitioning).is_none());
    }
}
