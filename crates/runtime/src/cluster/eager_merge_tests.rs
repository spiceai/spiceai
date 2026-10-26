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

//! Eager aggregation on a cluster scheduler adds merges whose `Partial` and
//! `Final` can land in different stages. Executors decode `sum` by name to
//! checked `SUM`, so every stage they decode must keep the schema the
//! scheduler planned, or the shuffle between the halves of a merge carries a
//! state the other side cannot read.

use std::sync::Arc;

use arrow::array::{Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use ballista_core::serde::BallistaPhysicalExtensionCodec;
use ballista_scheduler::planner::{DefaultDistributedPlanner, DistributedPlanner};
use datafusion::execution::TaskContext;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;

use super::datafusion::datafusion_and_cluster_physical_optimizers;
use super::{apply_distributed_execution_config, executor_function_registry};
use crate::dataaccelerator::AcceleratorEngineRegistry;
use crate::datafusion::builder::DataFusionBuilder;
use crate::status;

const ROWS: i32 = 10_000;

/// A cluster scheduler's session over a fact (`fk` in `0..100`) and a dim
/// whose keys 1 and 2 share one name, so `fk IN (1, 2)` groups 200 rows.
fn scheduler_session(amounts: &[i64]) -> datafusion::error::Result<SessionContext> {
    let df = DataFusionBuilder::new(
        status::RuntimeStatus::new(),
        Arc::new(AcceleratorEngineRegistry::default()),
        tokio::runtime::Handle::current(),
    )
    .build();

    let fact = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("fk", DataType::Int32, false),
            Field::new("amount", DataType::Int64, false),
            Field::new("n", DataType::Int32, false),
            Field::new("x", DataType::Float64, false),
        ])),
        vec![
            Arc::new(Int32Array::from_iter_values((1..=ROWS).map(|i| i % 100))),
            Arc::new(Int64Array::from(amounts.to_vec())),
            Arc::new(Int32Array::from_iter_values(std::iter::repeat_n(1, 10_000))),
            Arc::new(Float64Array::from_iter_values(std::iter::repeat_n(
                1.5, 10_000,
            ))),
        ],
    )?;
    let dim = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("dk", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int32Array::from_iter_values(0..100)),
            Arc::new(StringArray::from_iter_values((0..100).map(|k| {
                if k == 1 || k == 2 {
                    "g".to_string()
                } else {
                    format!("n{k}")
                }
            }))),
        ],
    )?;
    df.ctx.register_batch("fact", fact)?;
    df.ctx.register_batch("dim", dim)?;

    // As the cluster session builder does: the local config made safe for
    // stages, and the cluster physical optimizer rules.
    let mut config =
        apply_distributed_execution_config(df.ctx.copied_config().with_target_partitions(4));
    // Ballista defaults to sort-merge joins; eager aggregation rewrites hash
    // joins, which a cluster gets with `runtime.query.prefer_hash_join` or
    // `SET datafusion.optimizer.prefer_hash_join = true`.
    config.options_mut().optimizer.prefer_hash_join = true;
    // The memory tables carry the NDV stats eager aggregation needs; drop
    // only the guard that refuses to distribute them.
    let rules = datafusion_and_cluster_physical_optimizers()
        .into_iter()
        .filter(|rule| rule.name() != "EnsureSerializableFileScanOptimizer")
        .collect();
    let state = SessionStateBuilder::new_from_existing(df.ctx.state())
        .with_config(config)
        .with_physical_optimizer_rules(rules)
        .build();
    Ok(SessionContext::new_with_state(state))
}

fn zero_amounts() -> Vec<i64> {
    vec![0; 10_000]
}

fn aggregate_above_a_join(plan: &Arc<dyn ExecutionPlan>) -> bool {
    fn has_join(plan: &Arc<dyn ExecutionPlan>) -> bool {
        plan.is::<HashJoinExec>() || plan.children().into_iter().any(has_join)
    }
    fn below_join(plan: &Arc<dyn ExecutionPlan>, seen_join: bool) -> bool {
        if seen_join && plan.is::<AggregateExec>() {
            return true;
        }
        let seen_join = seen_join || plan.is::<HashJoinExec>();
        plan.children()
            .into_iter()
            .any(|child| below_join(child, seen_join))
    }
    has_join(plan) && below_join(plan, false)
}

/// Each node's output fields as `(name, type)`. Nullability is left out:
/// `datafusion-proto` does not carry an aggregate's overridden return field,
/// so a decoded non-null `COUNT` merge reads back as nullable whichever `sum`
/// decodes it. The state type is what must match across the shuffle.
fn node_schemas(plan: &Arc<dyn ExecutionPlan>) -> Vec<(String, Vec<(String, DataType)>)> {
    let fields = plan
        .schema()
        .fields()
        .iter()
        .map(|field| (field.name().clone(), field.data_type().clone()))
        .collect();
    let mut out = vec![(plan.name().to_string(), fields)];
    for child in plan.children() {
        out.extend(node_schemas(child));
    }
    out
}

/// The modes of the aggregates in `plan` that group by `name`: the merge's
/// halves (the pushed-down aggregate groups by the join key instead).
fn merge_modes(plan: &Arc<dyn ExecutionPlan>) -> Vec<AggregateMode> {
    let mut out = Vec::new();
    if let Some(aggregate) = plan.downcast_ref::<AggregateExec>()
        && aggregate
            .group_expr()
            .expr()
            .iter()
            .any(|(_, name)| name == "name")
    {
        out.push(*aggregate.mode());
    }
    for child in plan.children() {
        out.extend(merge_modes(child));
    }
    out
}

/// Split `sql` into stages, decode each the way an executor does, and
/// require every node to keep the scheduler's schema. Returns the rendered
/// scheduler plan for messages.
async fn assert_stages_decode_to_the_planned_schemas(ctx: &SessionContext, sql: &str) -> String {
    let plan = ctx
        .sql(sql)
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let rendered = displayable(plan.as_ref()).indent(true).to_string();
    assert!(
        aggregate_above_a_join(&plan),
        "eager aggregation must fire (an aggregate below the join) for this test to mean anything:\n{rendered}"
    );

    let stages = DefaultDistributedPlanner::new()
        .plan_query_stages(
            &"eager-merge".to_string().into(),
            Arc::clone(&plan),
            ctx.state().config().options(),
        )
        .expect("plan stages");

    // The merge must straddle a shuffle: its `Partial` ends one stage and its
    // final half starts the next.
    let partial_merge_stage = stages.iter().position(|stage| {
        merge_modes(&(Arc::clone(stage) as Arc<dyn ExecutionPlan>))
            .contains(&AggregateMode::Partial)
    });
    let final_merge_stage = stages.iter().position(|stage| {
        merge_modes(&(Arc::clone(stage) as Arc<dyn ExecutionPlan>))
            .iter()
            .any(|mode| matches!(mode, AggregateMode::Final | AggregateMode::FinalPartitioned))
    });
    assert!(
        partial_merge_stage.is_some()
            && final_merge_stage.is_some()
            && partial_merge_stage != final_merge_stage,
        "the merge's Partial ({partial_merge_stage:?}) and Final ({final_merge_stage:?}) must be in different stages:\n{rendered}"
    );

    let registry = executor_function_registry();
    let session_state = ctx.state();
    let task_ctx = TaskContext::new(
        None,
        "eager-merge".to_string(),
        session_state.config().clone(),
        registry.scalar_functions.clone(),
        registry.higher_order_functions.clone(),
        registry.aggregate_functions.clone(),
        registry.window_functions,
        Arc::clone(session_state.runtime_env()),
    );
    let codec = BallistaPhysicalExtensionCodec::default();
    for stage in stages {
        let stage = stage as Arc<dyn ExecutionPlan>;
        let node = PhysicalPlanNode::try_from_physical_plan(Arc::clone(&stage), &codec)
            .expect("encode stage");
        let decoded = node
            .try_into_physical_plan(&task_ctx, &codec)
            .expect("executor decodes stage");
        assert_eq!(
            node_schemas(&decoded),
            node_schemas(&stage),
            "an executor must decode the stage with the schemas the scheduler planned:\n{}\nscheduler plan:\n{rendered}",
            displayable(stage.as_ref()).indent(true)
        );
    }
    rendered
}

async fn rows(ctx: &SessionContext, sql: &str) -> Result<String, String> {
    match ctx.sql(sql).await {
        Err(error) => Err(error.to_string()),
        Ok(df) => match df.collect().await {
            Err(error) => Err(error.to_string()),
            Ok(batches) => Ok(arrow::util::pretty::pretty_format_batches(&batches)
                .expect("format")
                .to_string()),
        },
    }
}

/// Eager `COUNT` merges its partial counts with a `sum`.
#[tokio::test]
async fn eager_count_merge_decodes_on_executors_with_the_planned_state() {
    let ctx = scheduler_session(&zero_amounts()).expect("scheduler session");
    let sql = "SELECT d.name, COUNT(f.n) AS c FROM fact f JOIN dim d ON f.fk = d.dk \
               WHERE f.fk IN (1, 2) GROUP BY d.name";
    let plan = assert_stages_decode_to_the_planned_schemas(&ctx, sql).await;
    let rows = rows(&ctx, sql)
        .await
        .unwrap_or_else(|error| panic!("eager COUNT must run, got {error}\n{plan}"));
    assert!(
        rows.contains("| g    | 200 |"),
        "COUNT over 200 rows must be 200\n{rows}\n{plan}"
    );
}

/// Eager `AVG(Float64)` merges both its sum and its count with a `sum`.
#[tokio::test]
async fn eager_float_avg_merge_decodes_on_executors_with_the_planned_state() {
    let ctx = scheduler_session(&zero_amounts()).expect("scheduler session");
    let sql = "SELECT d.name, AVG(f.x) AS a FROM fact f JOIN dim d ON f.fk = d.dk \
               WHERE f.fk IN (1, 2) GROUP BY d.name";
    let plan = assert_stages_decode_to_the_planned_schemas(&ctx, sql).await;
    let rows = rows(&ctx, sql)
        .await
        .unwrap_or_else(|error| panic!("eager AVG must run, got {error}\n{plan}"));
    assert!(
        rows.contains("| g    | 1.5 |"),
        "AVG of 1.5s must be 1.5\n{rows}\n{plan}"
    );
}

/// The rewrite must not cost checked `SUM` its overflow error.
#[tokio::test]
async fn checked_sum_over_a_join_still_errors_on_overflow() {
    let amounts: Vec<i64> = (1..=ROWS)
        .map(|i| match (i % 100, (i - 1) / 100) {
            (1 | 2, 0 | 1) => i64::MAX,
            _ => 0,
        })
        .collect();
    let ctx = scheduler_session(&amounts).expect("scheduler session");
    let sql = "SELECT d.name, SUM(f.amount) AS total, COUNT(f.n) AS c FROM fact f \
               JOIN dim d ON f.fk = d.dk WHERE f.fk IN (1, 2) GROUP BY d.name";
    let error = rows(&ctx, sql)
        .await
        .expect_err("SUM of four i64::MAX must overflow");
    assert!(
        error.to_ascii_lowercase().contains("overflow"),
        "checked SUM must error on overflow, got {error}"
    );
}
