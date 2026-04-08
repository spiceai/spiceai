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

//! Regression test for query-time data duplication in distributed write-through acceleration.
//!
//! Simulates the scheduler's two `PartitionedTableScanRewrite` analyzer rules to verify
//! that a query against a write-through accelerated table doesn't produce duplicate scans.
//!
//! The scheduler has TWO `PartitionedTableScanRewrite` rules:
//!   1. For accelerated tables (using `accelerations_partition_manager`)
//!   2. For federated tables (using `federated_partition_manager`)
//!
//! For DDL write-through tables, only the `federated_partition_manager` has metadata.
//! The bug was that BOTH rules could match the same table by different criteria — Rule 1
//! by provider type (AcceleratedTable), Rule 2 by table name in partition metadata — and
//! each would produce its own set of executor scans, resulting in a Union of Unions (2x data).

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::tree_node::TreeNode;
use datafusion::datasource::{DefaultTableSource, MemTable, TableProvider};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use datafusion::sql::TableReference;
use runtime_datafusion::analyzer_rule::partitioned_table_scan_rewrite::{
    PartitionValue, PartitionedTableScanRewrite, TablePartitionProvider,
};

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn test_batch() -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
        ],
    )
    .expect("create test batch")
}

/// Mock partition provider that simulates `ExecutorRegistry`'s behavior:
/// matches tables by provider type (checks if the source IS an AcceleratedTable).
/// For this test, we match by a registered table name since we can't use AcceleratedTable directly.
#[derive(Debug)]
struct AccelerationsPartitionProvider {
    /// Table names this provider will match on.
    match_tables: Vec<String>,
    /// The executor providers to return.
    executors: Vec<Arc<dyn TableProvider>>,
}

impl TablePartitionProvider for AccelerationsPartitionProvider {
    fn should_partition(&self, _tbl: &datafusion::logical_expr::TableScan) -> bool {
        true
    }

    fn get_partitions(
        &self,
        _table: &TableReference,
        _schema: &SchemaRef,
    ) -> Vec<(Arc<dyn TableProvider>, Vec<PartitionValue>)> {
        // Simulates the accelerations_partition_manager with no metadata:
        // returns a single executor as fallback.
        if let Some(first) = self.executors.first() {
            vec![(Arc::clone(first), vec![])]
        } else {
            vec![]
        }
    }
}

/// Mock partition provider that simulates `FederatedPartitionProvider`'s behavior:
/// matches tables by name in the partition manager, regardless of provider type.
#[derive(Debug)]
struct FederatedPartitionProvider {
    /// Table names this provider will match on.
    match_tables: Vec<String>,
    /// The executor providers to return (one per executor).
    executors: Vec<Arc<dyn TableProvider>>,
}

impl TablePartitionProvider for FederatedPartitionProvider {
    fn should_partition(&self, _tbl: &datafusion::logical_expr::TableScan) -> bool {
        true
    }

    fn get_partitions(
        &self,
        _table: &TableReference,
        _schema: &SchemaRef,
    ) -> Vec<(Arc<dyn TableProvider>, Vec<PartitionValue>)> {
        // Returns all executors with empty partition values (same as the real
        // FederatedPartitionProvider which strips partition values).
        self.executors
            .iter()
            .map(|p| (Arc::clone(p), vec![]))
            .collect()
    }
}

/// Count the number of `TableScan` nodes in a logical plan.
fn count_table_scans(plan: &LogicalPlan) -> usize {
    let mut count = 0;
    plan.apply(|node| {
        if matches!(node, LogicalPlan::TableScan(_)) {
            count += 1;
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    })
    .expect("plan traversal");
    count
}

/// Regression test: two PartitionedTableScanRewrite rules must not produce double scans.
///
/// Simulates the scheduler's analyzer configuration with two partition rewrite rules.
/// For a table that matches BOTH rules (like a DDL write-through table that is both an
/// AcceleratedTable and has federated partition metadata), the rules must not compound
/// to produce N×N scans (where N = number of executors).
///
/// With 2 executors, the expected result is 2 table scans (one per executor).
/// The bug would produce 4 table scans (Rule 1 produces 1, Rule 2 expands each to 2).
#[tokio::test]
async fn test_partition_rewrite_rules_do_not_compound() {
    let schema = test_schema();
    let table_name = "wt_table";

    // Create the "local" table (registered on the scheduler) that will be scanned.
    let local_table: Arc<dyn TableProvider> = Arc::new(
        MemTable::try_new(Arc::clone(&schema), vec![vec![test_batch()]])
            .expect("create local MemTable"),
    );

    // Create mock "executor" table providers (simulating FlightSQL providers).
    let executor1: Arc<dyn TableProvider> = Arc::new(
        MemTable::try_new(Arc::clone(&schema), vec![vec![test_batch()]])
            .expect("create executor1 MemTable"),
    );
    let executor2: Arc<dyn TableProvider> = Arc::new(
        MemTable::try_new(Arc::clone(&schema), vec![vec![test_batch()]])
            .expect("create executor2 MemTable"),
    );

    // Build a SessionContext with both analyzer rules, mimicking the scheduler.
    let ctx = SessionContext::new();
    ctx.register_table(table_name, local_table)
        .expect("register table");

    // Rule 1: Accelerations partition provider (simulates ExecutorRegistry)
    // Matches the table and returns a single executor (no partition metadata).
    ctx.add_analyzer_rule(Arc::new(PartitionedTableScanRewrite::new(
        Arc::new(AccelerationsPartitionProvider {
            match_tables: vec![table_name.to_string()],
            executors: vec![Arc::clone(&executor1)],
        }),
        &ctx,
    )));

    // Rule 2: Federated partition provider (simulates FederatedPartitionProvider)
    // Also matches the table (by name) and returns both executors.
    ctx.add_analyzer_rule(Arc::new(PartitionedTableScanRewrite::new(
        Arc::new(FederatedPartitionProvider {
            match_tables: vec![table_name.to_string()],
            executors: vec![Arc::clone(&executor1), Arc::clone(&executor2)],
        }),
        &ctx,
    )));

    // Execute the query. The analyzer rules run during physical planning.
    // Each mock executor has 3 rows. With 2 executors, we expect 6 total.
    // If both rules compound, we'd get 4 scans × 3 rows = 12 rows.
    let results = ctx
        .sql(&format!("SELECT * FROM {table_name}"))
        .await
        .expect("plan query")
        .collect()
        .await
        .expect("collect query results");
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();

    assert_eq!(
        total_rows, 6,
        "Expected 6 rows (3 per executor × 2 executors). \
         Got {total_rows} — if > 6, the partition rewrite rules are compounding \
         and producing duplicate scans."
    );
}

/// Same test but with the accelerations provider returning BOTH executors
/// (simulating the case where accelerations_partition_manager has metadata,
/// e.g. for spicepod-registered datasets with partitioning).
///
/// This tests the worst case: both rules match AND both return all executors,
/// which would produce 2×2 = 4 scans without the fix.
#[tokio::test]
async fn test_partition_rewrite_rules_both_match_all_executors() {
    let schema = test_schema();
    let table_name = "wt_table2";

    let local_table: Arc<dyn TableProvider> = Arc::new(
        MemTable::try_new(Arc::clone(&schema), vec![vec![test_batch()]])
            .expect("create local MemTable"),
    );

    // Distinct data per executor so we can detect duplication by row count.
    let exec1_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ],
    )
    .expect("batch");
    let exec2_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(Int64Array::from(vec![30])),
        ],
    )
    .expect("batch");

    let executor1: Arc<dyn TableProvider> = Arc::new(
        MemTable::try_new(Arc::clone(&schema), vec![vec![exec1_batch]])
            .expect("create executor1 MemTable"),
    );
    let executor2: Arc<dyn TableProvider> = Arc::new(
        MemTable::try_new(Arc::clone(&schema), vec![vec![exec2_batch]])
            .expect("create executor2 MemTable"),
    );

    let ctx = SessionContext::new();
    ctx.register_table(table_name, local_table)
        .expect("register table");

    // Rule 1: Returns BOTH executors (has partition metadata).
    ctx.add_analyzer_rule(Arc::new(PartitionedTableScanRewrite::new(
        Arc::new(AccelerationsPartitionProvider {
            match_tables: vec![table_name.to_string()],
            executors: vec![Arc::clone(&executor1), Arc::clone(&executor2)],
        }),
        &ctx,
    )));

    // Rule 2: Also returns BOTH executors.
    ctx.add_analyzer_rule(Arc::new(PartitionedTableScanRewrite::new(
        Arc::new(FederatedPartitionProvider {
            match_tables: vec![table_name.to_string()],
            executors: vec![Arc::clone(&executor1), Arc::clone(&executor2)],
        }),
        &ctx,
    )));

    // Execute the query.
    // Correct: 2 rows (exec1) + 1 row (exec2) = 3 rows.
    // With compounding: 2×(2+1) = 6 rows (each executor scanned twice).
    let results = ctx
        .sql(&format!("SELECT * FROM {table_name}"))
        .await
        .expect("plan query")
        .collect()
        .await
        .expect("collect query results");
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();

    assert_eq!(
        total_rows, 3,
        "Expected 3 rows (2 from exec1 + 1 from exec2). \
         Got {total_rows} — if 6, both partition rewrite rules fired and each \
         executor was scanned twice."
    );
}
