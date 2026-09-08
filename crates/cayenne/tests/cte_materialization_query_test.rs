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

//! Query-path CTE materialization against a real [`CayenneTableProvider`].
//!
//! Unit tests in `cte_materialization.rs` replace Cayenne detection with an
//! always-true predicate over `MemTable`. This file exercises the production
//! predicate (`is::<CayenneTableProvider>`) and both `auto` (rule registered)
//! and `disabled` (default optimizer) plan shapes plus result equality.

#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::metadata::CreateTableOptions;
use cayenne::{
    CTE_SCAN_NODE_NAME, CayenneCteMaterialization, CayenneCteMaterializationPlanner,
    CayenneTableProvider, MATERIALIZED_CTE_NODE_NAME, MetadataCatalog,
};
use common::TestFixture;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::TableProvider;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::Optimizer;
use datafusion::physical_plan::displayable;
use datafusion::prelude::SessionContext;
use runtime_datafusion::extension::ExtensionPlanQueryPlanner;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

const SQL: &str = "WITH expensive AS (SELECT id, sum(value) AS s FROM t GROUP BY id) \
     SELECT a.id, CAST(a.s AS BIGINT) AS s FROM expensive a \
     JOIN expensive b ON a.id = b.id \
     ORDER BY a.id";

fn session(cte_auto: bool) -> SessionContext {
    // Match production: the extension planner is always registered; the
    // logical rewrite is the `auto` switch.
    let mut builder = SessionStateBuilder::new()
        .with_default_features()
        .with_query_planner(Arc::new(
            ExtensionPlanQueryPlanner::from_extension_planners(vec![Arc::new(
                CayenneCteMaterializationPlanner,
            )]),
        ));
    if cte_auto {
        let mut rules = Optimizer::new().rules;
        rules.insert(0, Arc::new(CayenneCteMaterialization::new()));
        builder = builder.with_optimizer_rules(rules);
    }
    SessionContext::new_with_state(builder.build())
}

fn plan_contains(plan: &LogicalPlan, name: &str) -> bool {
    let mut found = false;
    plan.apply_with_subqueries(|node| {
        if let LogicalPlan::Extension(extension) = node
            && extension.node.name() == name
        {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("walk logical plan");
    found
}

fn i64_col(batches: &[RecordBatch], col: usize) -> Vec<Option<i64>> {
    let mut out = Vec::new();
    for batch in batches {
        let array = batch
            .column(col)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 column");
        out.extend(array.iter());
    }
    out
}

async fn seeded_table(fixture: &TestFixture) -> TestResult<Arc<CayenneTableProvider>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: "t".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: None,
                base_path: fixture.data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            },
            ctx.runtime_env(),
        )
        .await?,
    );
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
        ],
    )?;
    common::insert_batch(table.as_ref(), batch).await?;
    Ok(table)
}

struct QueryOutcome {
    logical: LogicalPlan,
    physical: String,
    rows: Vec<RecordBatch>,
}

async fn run(cte_auto: bool, table: &Arc<CayenneTableProvider>) -> TestResult<QueryOutcome> {
    let ctx = session(cte_auto);
    ctx.register_table("t", Arc::clone(table) as Arc<dyn TableProvider>)?;
    let df = ctx.sql(SQL).await?;
    let logical = df.clone().into_optimized_plan()?;
    let physical = displayable(df.create_physical_plan().await?.as_ref())
        .indent(true)
        .to_string();
    let rows = ctx.sql(SQL).await?.collect().await?;
    Ok(QueryOutcome {
        logical,
        physical,
        rows,
    })
}

async fn cayenne_table_auto_materializes_and_disabled_stays_inlined_impl(
    fixture: TestFixture,
) -> TestResult<()> {
    let table = seeded_table(&fixture).await?;

    let auto = run(true, &table).await?;
    let off = run(false, &table).await?;

    assert!(
        plan_contains(&auto.logical, MATERIALIZED_CTE_NODE_NAME)
            && plan_contains(&auto.logical, CTE_SCAN_NODE_NAME),
        "auto must rewrite a Cayenne-backed multi-ref aggregate CTE:\n{}",
        auto.logical
    );
    assert!(
        auto.physical.contains("MaterializedCteExec"),
        "auto physical plan must collect the CTE once:\n{}",
        auto.physical
    );
    assert!(
        !plan_contains(&off.logical, MATERIALIZED_CTE_NODE_NAME),
        "disabled must keep DataFusion inlining:\n{}",
        off.logical
    );
    assert!(
        !off.physical.contains("MaterializedCteExec"),
        "disabled physical plan must not materialize the CTE:\n{}",
        off.physical
    );
    assert_eq!(
        i64_col(&auto.rows, 0),
        vec![Some(1), Some(2), Some(3)],
        "auto rows"
    );
    assert_eq!(
        i64_col(&auto.rows, 1),
        vec![Some(10), Some(20), Some(30)],
        "auto sums"
    );
    assert_eq!(
        i64_col(&off.rows, 0),
        i64_col(&auto.rows, 0),
        "disabled rows must match auto"
    );
    assert_eq!(
        i64_col(&off.rows, 1),
        i64_col(&auto.rows, 1),
        "disabled sums must match auto"
    );
    Ok(())
}

test_with_backends!(cayenne_table_auto_materializes_and_disabled_stays_inlined_impl);
