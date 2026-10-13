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

//! `x NOT IN (subquery)` decorrelates to a null-aware anti join: a NULL among the
//! subquery's values leaves no row selected, and a NULL `x` is never selected. A
//! join the reorder rebuilds has to stay null-aware, or it answers as the plain
//! anti join `NOT EXISTS` plans, which keeps both kinds of row.
//!
//! Every query runs with and without `ReorderJoinRule`; the rows must match.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::util::display::array_value_to_string;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::TreeNode;
use datafusion::common::{ColumnStatistics, JoinType, Statistics};
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::{Expr, LogicalPlan, TableType};
use datafusion::optimizer::Optimizer;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;

use super::ReorderJoinRule;

/// A `MemTable` that reports its row count, which the reorder's cost model needs
/// before it will move a join.
#[derive(Debug)]
struct CountedTable {
    inner: MemTable,
    num_rows: usize,
}

#[async_trait]
impl TableProvider for CountedTable {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn statistics(&self) -> Option<Statistics> {
        let column_statistics = self
            .schema()
            .fields()
            .iter()
            .map(|_| ColumnStatistics::new_unknown())
            .collect();
        Some(Statistics {
            num_rows: Precision::Exact(self.num_rows),
            total_byte_size: Precision::Absent,
            column_statistics,
        })
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }
}

fn counted_table(column: &str, values: Vec<Option<i64>>) -> Arc<CountedTable> {
    let schema = Arc::new(Schema::new(vec![Field::new(column, DataType::Int64, true)]));
    let num_rows = values.len();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(values))],
    )
    .expect("batch matches schema");
    Arc::new(CountedTable {
        inner: MemTable::try_new(schema, vec![vec![batch]]).expect("memory table"),
        num_rows,
    })
}

/// `DataFusion`'s default optimizer, with the reorder inserted where the runtime
/// inserts it when `with_reorder` is set.
fn session(with_reorder: bool) -> SessionContext {
    let mut rules = Optimizer::new().rules;
    if with_reorder {
        let insert_at = ["push_down_filter", "eliminate_cross_join"]
            .iter()
            .filter_map(|name| rules.iter().position(|rule| rule.name() == *name))
            .max()
            .map_or(rules.len(), |position| position + 1);
        rules.insert(insert_at, Arc::new(ReorderJoinRule::default()));
    }
    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_optimizer_rules(rules)
            .build(),
    );

    // `a` is small, `b` large and `c` tiny, so applying the anti join before the
    // inner join is the cheaper order and the reorder has a reason to move it.
    let small: Vec<Option<i64>> = (1..=100).map(Some).chain([None]).collect();
    let large: Vec<Option<i64>> = (1..=10_000).map(|value| Some(value % 100 + 1)).collect();
    for (name, column, values) in [
        ("a", "x", small),
        ("b", "k", large),
        ("c_null", "y", vec![Some(5), None]),
        ("c", "y", vec![Some(5)]),
    ] {
        ctx.register_table(name, counted_table(column, values))
            .expect("register table");
    }
    ctx
}

async fn sorted_rows(ctx: &SessionContext, sql: &str) -> Vec<String> {
    let batches = ctx
        .sql(sql)
        .await
        .expect("query plans")
        .collect()
        .await
        .expect("query runs");
    let mut rows = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let rendered: Vec<String> = batch
                .columns()
                .iter()
                .map(|column| array_value_to_string(column, row).expect("renders"))
                .collect();
            rows.push(rendered.join(","));
        }
    }
    rows.sort_unstable();
    rows
}

/// The `null_aware` flag of every anti join in the optimized plan.
async fn anti_join_null_awareness(ctx: &SessionContext, sql: &str) -> Vec<bool> {
    let plan = ctx
        .sql(sql)
        .await
        .expect("query plans")
        .into_optimized_plan()
        .expect("query optimizes");
    let mut flags = Vec::new();
    plan.apply(|node| {
        if let LogicalPlan::Join(join) = node
            && matches!(join.join_type, JoinType::LeftAnti | JoinType::RightAnti)
        {
            flags.push(join.null_aware);
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    })
    .expect("plan walk");
    flags
}

#[tokio::test]
async fn a_reordered_not_in_keeps_its_null_aware_semantics() {
    let reordered = session(true);
    let reference = session(false);
    let queries = [
        // A NULL among the values: no row qualifies.
        "SELECT a.x, COUNT(*) FROM a JOIN b ON a.x = b.k WHERE a.x NOT IN (SELECT y FROM c_null) GROUP BY a.x",
        // No NULL among the values: 5 and the NULL `x` do not qualify.
        "SELECT a.x, COUNT(*) FROM a JOIN b ON a.x = b.k WHERE a.x NOT IN (SELECT y FROM c) GROUP BY a.x",
        "SELECT COUNT(*) FROM a JOIN b ON a.x = b.k WHERE a.x NOT IN (SELECT y FROM c_null)",
        "SELECT COUNT(*) FROM b JOIN a ON a.x = b.k WHERE a.x NOT IN (SELECT y FROM c)",
    ];
    let mut wrong = Vec::new();
    for sql in queries {
        let got = sorted_rows(&reordered, sql).await;
        let expected = sorted_rows(&reference, sql).await;
        let flags = anti_join_null_awareness(&reordered, sql).await;
        if got != expected || flags.iter().any(|null_aware| !null_aware) {
            wrong.push(format!(
                "{sql}: got {} rows {:?}, expected {} rows {:?}; anti joins null_aware={flags:?}",
                got.len(),
                got.iter().take(3).collect::<Vec<_>>(),
                expected.len(),
                expected.iter().take(3).collect::<Vec<_>>(),
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "the reorder turned a NOT IN into a plain anti join: {wrong:#?}"
    );
}
