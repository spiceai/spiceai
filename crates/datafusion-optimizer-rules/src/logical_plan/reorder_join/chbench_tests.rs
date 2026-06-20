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

//! `ReorderJoinRule` logic snapshot tests based on the CH-benCHmark queries (SF100).

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, Statistics};
use datafusion::datasource::empty::EmptyTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TableType,
    Volatility,
};
use datafusion::optimizer::Optimizer;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;

use super::ReorderJoinRule;

/// A table whose only purpose is to expose injected statistics
/// (`num_rows` + per-column `distinct_count`) to the cost model. Never executed
/// — these tests only build/optimize the logical plan.
#[derive(Debug)]
struct StatTable {
    schema: SchemaRef,
    num_rows: usize,
    ndv: Vec<usize>,
}

#[async_trait]
impl TableProvider for StatTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    fn statistics(&self) -> Option<Statistics> {
        let column_statistics = self
            .ndv
            .iter()
            .map(|&n| ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Absent,
                min_value: Precision::Absent,
                sum_value: Precision::Absent,
                distinct_count: Precision::Inexact(n),
                byte_size: Precision::Absent,
            })
            .collect();
        Some(Statistics {
            num_rows: Precision::Inexact(self.num_rows),
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
        EmptyTable::new(Arc::clone(&self.schema))
            .scan(state, projection, filters, limit)
            .await
    }
}

/// Minimal `mod(a, b)` scalar UDF so the canonical chbench queries (which use
/// the `mod(...)` spelling, provided by Spice/Cayenne in production) can be
/// *planned* in a plain DataFusion context — vanilla DataFusion has no `mod`
/// function. It is never executed: these tests only build/optimize the logical
/// plan, and the reorder cost model matches the function by name (capping the
/// key NDV at the modulo literal, mirroring `% k`).
#[derive(Debug, PartialEq, Eq, Hash)]
struct ModUdf {
    signature: Signature,
}

impl ModUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ModUdf {
    fn name(&self) -> &'static str {
        "mod"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(DataType::Int64)
    }
    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> Result<ColumnarValue, DataFusionError> {
        Err(DataFusionError::NotImplemented(
            "mod UDF stub is logical-plan-only (reorder tests never execute it)".into(),
        ))
    }
}

fn ts() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, None)
}

/// (schema, per-column NDV, num_rows) for a chbench table at W=100 (SF100).
fn chbench_table(name: &str) -> (Schema, Vec<usize>, usize) {
    let i = DataType::Int64;
    let f = DataType::Float64;
    let s = DataType::Utf8;
    let spec: Vec<(&str, DataType, usize)> = match name {
        "customer" => vec![
            ("c_id", i.clone(), 3_000),
            ("c_w_id", i.clone(), 100),
            ("c_d_id", i.clone(), 10),
            ("c_state", s.clone(), 50),
            ("c_last", s.clone(), 1_000),
            ("c_city", s.clone(), 10_000),
            ("c_phone", s.clone(), 3_000_000),
        ],
        "oorder" => vec![
            ("o_id", i.clone(), 3_000),
            ("o_c_id", i.clone(), 3_000),
            ("o_w_id", i.clone(), 100),
            ("o_d_id", i.clone(), 10),
            ("o_entry_d", ts(), 1_000_000),
            ("o_ol_cnt", i.clone(), 15),
        ],
        "new_order" => vec![
            ("no_o_id", i.clone(), 900),
            ("no_w_id", i.clone(), 100),
            ("no_d_id", i.clone(), 10),
        ],
        "order_line" => vec![
            ("ol_o_id", i.clone(), 3_000),
            ("ol_w_id", i.clone(), 100),
            ("ol_d_id", i.clone(), 10),
            ("ol_i_id", i.clone(), 100_000),
            ("ol_supply_w_id", i.clone(), 100),
            ("ol_amount", f.clone(), 1_000_000),
            ("ol_delivery_d", ts(), 1_000_000),
        ],
        "stock" => vec![
            ("s_w_id", i.clone(), 100),
            ("s_i_id", i.clone(), 100_000),
            ("s_quantity", i.clone(), 100),
            ("s_order_cnt", i.clone(), 1_000),
        ],
        "supplier" => vec![
            ("su_suppkey", i.clone(), 10_000),
            ("su_nationkey", i.clone(), 62),
            ("su_name", s.clone(), 10_000),
            ("su_address", s.clone(), 10_000),
            ("su_phone", s.clone(), 10_000),
            ("su_comment", s.clone(), 10_000),
        ],
        "nation" => vec![
            ("n_nationkey", i.clone(), 62),
            ("n_name", s.clone(), 62),
            ("n_regionkey", i.clone(), 5),
        ],
        "region" => vec![("r_regionkey", i.clone(), 5), ("r_name", s.clone(), 5)],
        "item" => vec![
            ("i_id", i.clone(), 100_000),
            ("i_data", s.clone(), 100_000),
            ("i_name", s.clone(), 100_000),
        ],
        other => panic!("unknown chbench table {other}"),
    };
    let num_rows = match name {
        "customer" | "oorder" => 3_000_000,
        "new_order" => 900_000,
        "order_line" => 30_000_000,
        "stock" => 10_000_000,
        "supplier" => 10_000,
        "nation" => 62,
        "region" => 5,
        "item" => 100_000,
        _ => unreachable!(),
    };
    let fields: Vec<Field> = spec
        .iter()
        .map(|(c, t, _)| Field::new(*c, t.clone(), true))
        .collect();
    let ndv: Vec<usize> = spec.iter().map(|(_, _, n)| *n).collect();
    (Schema::new(fields), ndv, num_rows)
}

const TABLES: &[&str] = &[
    "customer",
    "oorder",
    "new_order",
    "order_line",
    "stock",
    "supplier",
    "nation",
    "region",
    "item",
];

/// A plain DataFusion default optimizer with *only* `ReorderJoinRule` inserted,
/// at the same pipeline position the Spice runtime uses (after the join graph is
/// formed — `eliminate_cross_join` — and before projection pushdown fragments it).
fn make_reordered_ctx() -> SessionContext {
    let mut rules = Optimizer::new().rules;
    let insert_at = rules
        .iter()
        .position(|rule| rule.name() == "eliminate_cross_join")
        .map(|position| position + 1)
        .or_else(|| {
            rules
                .iter()
                .position(|rule| rule.name() == "push_down_filter")
        })
        .unwrap_or(rules.len());
    rules.insert(insert_at, Arc::new(ReorderJoinRule::default()));

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_optimizer_rules(rules)
        .build();
    let ctx = SessionContext::new_with_state(state);
    // chbench uses `mod(...)`; vanilla DataFusion has no such function.
    ctx.register_udf(ScalarUDF::from(ModUdf::new()));
    for &table in TABLES {
        let (schema, ndv, num_rows) = chbench_table(table);
        let provider = StatTable {
            schema: Arc::new(schema),
            num_rows,
            ndv,
        };
        ctx.register_table(table, Arc::new(provider))
            .expect("register chbench stat table");
    }
    ctx
}

/// Optimize `sql` through the reorder-enabled optimizer, returning the plan
/// rendered as a string — or, if planning fails, the error text.
async fn reordered_plan(ctx: &SessionContext, sql: &str) -> String {
    match ctx.sql(sql).await {
        Ok(df) => match df.into_optimized_plan() {
            Ok(plan) => format!("{}", plan.display_indent()),
            Err(e) => format!("[optimize error] {e}"),
        },
        Err(e) => format!("[plan error] {e}"),
    }
}

macro_rules! chbench_reorder_snapshot {
    ($name:ident, $file:literal) => {
        #[tokio::test]
        async fn $name() {
            let ctx = make_reordered_ctx();
            let sql = include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../test-framework/src/queries/chbench/",
                $file
            ));
            let plan = reordered_plan(&ctx, sql).await;
            insta::assert_snapshot!(stringify!($name), plan);
        }
    };
}

/// Test: a pure cross join (no equi-predicates) is a disconnected join
/// graph. The reorder must not drop relations — every base table must survive.
#[tokio::test]
async fn reorder_cross_join_preserves_all_relations() {
    let ctx = make_reordered_ctx();
    let plan = reordered_plan(&ctx, "SELECT COUNT(*) FROM nation, region, supplier").await;
    for table in ["nation", "region", "supplier"] {
        assert!(
            plan.contains(table),
            "cross-join reorder dropped `{table}`; plan was:\n{plan}"
        );
    }
}
/// Test that reorder of a plan with a non-correlated scalar subquery
/// does not drop table components
#[tokio::test]
async fn reorder_q15_scalar_subquery_preserves_relations() {
    let ctx = make_reordered_ctx();
    let sql = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../test-framework/src/queries/chbench/q15.sql"
    ));
    let plan = reordered_plan(&ctx, sql).await;
    for table in ["supplier", "order_line", "stock"] {
        assert!(
            plan.contains(table),
            "q15 reorder dropped `{table}`; plan was:\n{plan}"
        );
    }
}

// CH-benCHmark queries with join chains.
chbench_reorder_snapshot!(reorder_q2, "q2.sql");
chbench_reorder_snapshot!(reorder_q3, "q3.sql");
chbench_reorder_snapshot!(reorder_q5, "q5.sql");
chbench_reorder_snapshot!(reorder_q7, "q7.sql");
chbench_reorder_snapshot!(reorder_q8, "q8.sql");
chbench_reorder_snapshot!(reorder_q9, "q9.sql");
chbench_reorder_snapshot!(reorder_q10, "q10.sql");
chbench_reorder_snapshot!(reorder_q11, "q11.sql");
chbench_reorder_snapshot!(reorder_q18, "q18.sql");
chbench_reorder_snapshot!(reorder_q21, "q21.sql");
