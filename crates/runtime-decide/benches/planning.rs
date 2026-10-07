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

//! What the decision planner rules cost a query that calls no decision function, and
//! what planning a query that calls them costs.
//!
//! Plans the `TPC-H` queries over empty `TPC-H` tables, through the analyzer and the
//! optimizer, in a session with `DataFusion`'s rules alone and in one with the decision
//! functions registered and the leaf-pushdown rules guarded. The difference is the
//! overhead every ordinary query pays for the rules. A second group plans one query
//! that calls `ai_decide` and `ai_classify` with JSON constants, which are checked and
//! normalized while the query is planned.

#![expect(clippy::expect_used, reason = "benchmark setup")]

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::optimizer::Optimizer;
use datafusion::prelude::{SessionConfig, SessionContext};
use evaluate_api::Evaluate;
use runtime_decide::{DecisionFunctions, guard_async_calls};
use runtime_status::RuntimeStatus;
use tokio::sync::RwLock;

const TPCH_QUERIES: &str = "../test-framework/src/queries/tpch";

fn table(columns: &[(&str, DataType)]) -> Arc<MemTable> {
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, data_type)| Field::new(*name, data_type.clone(), false))
            .collect::<Vec<_>>(),
    ));
    Arc::new(MemTable::try_new(schema, vec![vec![]]).expect("empty table"))
}

fn register_tpch(ctx: &SessionContext) {
    use DataType::{Date32, Int64, Utf8};
    let money = DataType::Decimal128(15, 2);
    let tables: [(&str, Vec<(&str, DataType)>); 8] = [
        (
            "nation",
            vec![
                ("n_nationkey", Int64),
                ("n_name", Utf8),
                ("n_regionkey", Int64),
                ("n_comment", Utf8),
            ],
        ),
        (
            "region",
            vec![
                ("r_regionkey", Int64),
                ("r_name", Utf8),
                ("r_comment", Utf8),
            ],
        ),
        (
            "part",
            vec![
                ("p_partkey", Int64),
                ("p_name", Utf8),
                ("p_mfgr", Utf8),
                ("p_brand", Utf8),
                ("p_type", Utf8),
                ("p_size", Int64),
                ("p_container", Utf8),
                ("p_retailprice", money.clone()),
                ("p_comment", Utf8),
            ],
        ),
        (
            "supplier",
            vec![
                ("s_suppkey", Int64),
                ("s_name", Utf8),
                ("s_address", Utf8),
                ("s_nationkey", Int64),
                ("s_phone", Utf8),
                ("s_acctbal", money.clone()),
                ("s_comment", Utf8),
            ],
        ),
        (
            "partsupp",
            vec![
                ("ps_partkey", Int64),
                ("ps_suppkey", Int64),
                ("ps_availqty", Int64),
                ("ps_supplycost", money.clone()),
                ("ps_comment", Utf8),
            ],
        ),
        (
            "customer",
            vec![
                ("c_custkey", Int64),
                ("c_name", Utf8),
                ("c_address", Utf8),
                ("c_nationkey", Int64),
                ("c_phone", Utf8),
                ("c_acctbal", money.clone()),
                ("c_mktsegment", Utf8),
                ("c_comment", Utf8),
            ],
        ),
        (
            "orders",
            vec![
                ("o_orderkey", Int64),
                ("o_custkey", Int64),
                ("o_orderstatus", Utf8),
                ("o_totalprice", money.clone()),
                ("o_orderdate", Date32),
                ("o_orderpriority", Utf8),
                ("o_clerk", Utf8),
                ("o_shippriority", Int64),
                ("o_comment", Utf8),
            ],
        ),
        (
            "lineitem",
            vec![
                ("l_orderkey", Int64),
                ("l_partkey", Int64),
                ("l_suppkey", Int64),
                ("l_linenumber", Int64),
                ("l_quantity", money.clone()),
                ("l_extendedprice", money.clone()),
                ("l_discount", money.clone()),
                ("l_tax", money),
                ("l_returnflag", Utf8),
                ("l_linestatus", Utf8),
                ("l_shipdate", Date32),
                ("l_commitdate", Date32),
                ("l_receiptdate", Date32),
                ("l_shipinstruct", Utf8),
                ("l_shipmode", Utf8),
                ("l_comment", Utf8),
            ],
        ),
    ];
    for (name, columns) in tables {
        ctx.register_table(name, table(&columns))
            .expect("register table");
    }
}

fn session(with_decisions: bool) -> SessionContext {
    let config = SessionConfig::new().set_str("datafusion.sql_parser.dialect", "PostgreSQL");
    let rules = if with_decisions {
        guard_async_calls(Optimizer::new().rules)
    } else {
        Optimizer::new().rules
    };
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_optimizer_rules(rules)
        .build();
    let ctx = SessionContext::new_with_state(state);
    if with_decisions {
        let store: HashMap<String, Arc<dyn Evaluate>> = HashMap::new();
        DecisionFunctions::new(Arc::new(RwLock::new(store)), RuntimeStatus::new()).register(&ctx);
    }
    register_tpch(&ctx);
    ctx
}

fn queries() -> Vec<String> {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(TPCH_QUERIES);
    // Q15 is a view query and has no file here, so 21 queries are planned.
    let queries: Vec<String> = (1..=22)
        .filter_map(|n| std::fs::read_to_string(dir.join(format!("q{n}.sql"))).ok())
        .collect();
    assert_eq!(
        queries.len(),
        21,
        "the TPC-H queries moved: {}",
        dir.display()
    );
    queries
}

async fn plan_all(ctx: &SessionContext, queries: &[String]) {
    for sql in queries {
        let state = ctx.state();
        let plan = state.create_logical_plan(sql).await.expect("logical plan");
        state.optimize(&plan).expect("optimized plan");
    }
}

fn planning(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    let queries = queries();
    let mut group = c.benchmark_group("tpch_planning");
    for (arm, with_decisions) in [("datafusion_rules", false), ("with_decision_rules", true)] {
        let ctx = session(with_decisions);
        group.bench_function(arm, |b| {
            b.iter(|| runtime.block_on(plan_all(&ctx, &queries)));
        });
    }
    group.finish();
}

/// A query whose constants are parsed and checked when it is planned: an `ai_decide`
/// with a choice, a noul and a score, and an `ai_classify` with labels as a JSON object.
const DECISION_QUERY: &str = r#"SELECT
    ai_decide(l_comment, '{
        "team": {"type": "choice", "instructions": "Which team should handle this?",
                 "criteria": {"billing": "Payments and payouts", "technical": "Bugs and outages",
                              "account": "Login and access", "shipping": "Delivery", "other": null}},
        "urgent": {"type": "noul", "instructions": "Does this convey urgency?"},
        "tone": {"type": "score", "instructions": "How upset is the customer?",
                 "criteria": ["calm", "annoyed", "furious"]}
    }') AS d,
    ai_classify(l_comment, '{"billing": "Payments", "technical": null, "other": null}') AS team
FROM lineitem"#;

fn decision_call_planning(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    let queries = [DECISION_QUERY.to_string()];
    let ctx = session(true);
    c.bench_function("decision_call_planning", |b| {
        b.iter(|| runtime.block_on(plan_all(&ctx, &queries)));
    });
}

criterion_group!(benches, planning, decision_call_planning);
criterion_main!(benches);
