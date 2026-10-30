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

use std::sync::Arc;

use arrow::array::{
    ArrayRef, Decimal128Array, Int32Array, Int64Array, RecordBatch, StringArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::display::array_value_to_string;
use datafusion::catalog::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::Operator;
use datafusion::optimizer::{AnalyzerRule, Optimizer};
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
use datafusion::prelude::SessionContext;

use crate::analyzer_rule::AnalyzerRulesBuilder;
use crate::checked_arithmetic::op::checked_op_udf;
use crate::checked_arithmetic::{
    CHECKED_ADD_UDF_NAME, CHECKED_MUL_UDF_NAME, CHECKED_SUB_UDF_NAME, CheckedIntegerOverflow,
    install_checked_eager_aggregation, install_checked_single_distinct,
    register_checked_arithmetic,
};
use crate::session_config::get_df_default_config;

const SLT: &str = include_str!("integer_overflow.slt");

fn overflow_table() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("big", DataType::Int64, true),
            Field::new("i", DataType::Int32, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef,
            Arc::new(Int64Array::from(vec![
                Some(i64::MAX),
                Some(i64::MAX - 1),
                Some(1),
                Some(2),
            ])),
            Arc::new(Int32Array::from(vec![
                Some(i32::MAX),
                Some(i32::MAX - 1),
                Some(1),
                Some(2),
            ])),
        ],
    )
    .expect("overflow table batch")
}

fn register_overflow_table(ctx: &SessionContext) {
    let batch = overflow_table();
    let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).expect("mem table");
    ctx.register_table("t", Arc::new(table))
        .expect("register overflow table");
}

fn checked_session() -> SessionContext {
    checked_session_with_eager(false)
}

fn checked_session_with_eager(eager_aggregation: bool) -> SessionContext {
    let mut analyzer_rules = AnalyzerRulesBuilder::new()
        .include_federation(false)
        .build();
    if !analyzer_rules
        .iter()
        .any(|rule| rule.name() == "checked_integer_overflow")
    {
        analyzer_rules.push(Arc::new(CheckedIntegerOverflow::new()));
    }
    let mut optimizer_rules = Optimizer::new().rules;
    install_checked_single_distinct(&mut optimizer_rules);
    let mut config = get_df_default_config();
    config.options_mut().optimizer.enable_eager_aggregation = eager_aggregation;
    let mut physical_rules = PhysicalOptimizer::new().rules;
    install_checked_eager_aggregation(&mut physical_rules);
    let mut state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_analyzer_rules(analyzer_rules)
        .with_optimizer_rules(optimizer_rules)
        .with_physical_optimizer_rules(physical_rules)
        .build();
    register_checked_arithmetic(&mut state).expect("register checked arithmetic");
    let ctx = SessionContext::new_with_state(state);
    register_overflow_table(&ctx);
    ctx
}

fn vanilla_session() -> SessionContext {
    let ctx = SessionContext::new_with_config(get_df_default_config());
    register_overflow_table(&ctx);
    ctx
}

async fn query_error(ctx: &SessionContext, sql: &str) -> String {
    match ctx.sql(sql).await {
        Err(error) => error.to_string(),
        Ok(df) => match df.collect().await {
            Err(error) => error.to_string(),
            Ok(batches) => panic!("expected overflow for `{sql}`, got {batches:?}"),
        },
    }
}

fn contains_overflow(message: &str) -> bool {
    message.to_ascii_lowercase().contains("overflow")
}

fn render_rows(batches: &[RecordBatch]) -> Vec<String> {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let mut cells = Vec::with_capacity(batch.num_columns());
            for column in batch.columns() {
                if column.is_null(row) {
                    cells.push("NULL".to_string());
                } else {
                    cells.push(array_value_to_string(column, row).expect("render cell"));
                }
            }
            rows.push(cells.join(" "));
        }
    }
    rows
}

#[tokio::test]
async fn vanilla_datafusion_wraps_int64_sum_and_add() {
    let ctx = vanilla_session();
    let sum = ctx
        .sql("SELECT SUM(big) FROM t")
        .await
        .expect("plan vanilla SUM")
        .collect()
        .await
        .expect("run vanilla SUM");
    // i64::MAX + (i64::MAX - 1) + 1 + 2 wraps to 0.
    assert_eq!(render_rows(&sum), vec!["0".to_string()]);

    let add = ctx
        .sql("SELECT big + 1 FROM t WHERE id = 1")
        .await
        .expect("plan vanilla add")
        .collect()
        .await
        .expect("run vanilla add");
    assert_eq!(
        render_rows(&add),
        vec![i64::MIN.to_string()],
        "vanilla BIGINT + 1 wraps to i64::MIN"
    );
}

#[tokio::test]
async fn checked_session_errors_on_int64_sum_and_add() {
    let ctx = checked_session();
    let sum = query_error(&ctx, "SELECT SUM(big) FROM t").await;
    assert!(contains_overflow(&sum), "SUM overflow, got {sum}");

    let add = query_error(&ctx, "SELECT big + 1 FROM t WHERE id = 1").await;
    assert!(contains_overflow(&add), "add overflow, got {add}");

    let literal = query_error(&ctx, "SELECT CAST(9223372036854775807 AS BIGINT) + 1").await;
    assert!(
        contains_overflow(&literal),
        "literal add overflow, got {literal}"
    );
}

#[tokio::test]
async fn checked_session_keeps_non_overflowing_sums() {
    let ctx = checked_session();
    let batches = ctx
        .sql("SELECT SUM(big) FROM t WHERE id IN (3, 4)")
        .await
        .expect("plan safe SUM")
        .collect()
        .await
        .expect("run safe SUM");
    assert_eq!(render_rows(&batches), vec!["3".to_string()]);
}

#[tokio::test]
async fn integer_overflow_slt() {
    let ctx = checked_session();
    run_slt(&ctx, SLT).await;
}

#[test]
fn analyzer_rule_name_is_stable() {
    assert_eq!(
        CheckedIntegerOverflow::new().name(),
        "checked_integer_overflow"
    );
}

#[test]
fn checked_ops_are_registered_for_plus_minus_multiply_only() {
    assert!(checked_op_udf(Operator::Plus).is_some());
    assert!(checked_op_udf(Operator::Minus).is_some());
    assert!(checked_op_udf(Operator::Multiply).is_some());
    assert!(checked_op_udf(Operator::Divide).is_none());
    assert_eq!(
        checked_op_udf(Operator::Plus).expect("plus").name(),
        CHECKED_ADD_UDF_NAME
    );
    assert_eq!(
        checked_op_udf(Operator::Minus).expect("minus").name(),
        CHECKED_SUB_UDF_NAME
    );
    assert_eq!(
        checked_op_udf(Operator::Multiply).expect("mul").name(),
        CHECKED_MUL_UDF_NAME
    );
}

#[tokio::test]
async fn checked_helpers_are_not_a_public_sql_operator_api() {
    let ctx = checked_session();
    let ok = ctx
        .sql("SELECT __spice_checked_add(CAST(1 AS BIGINT), CAST(2 AS BIGINT))")
        .await
        .expect("plan internal helper")
        .collect()
        .await
        .expect("run internal helper");
    assert_eq!(render_rows(&ok), vec!["3".to_string()]);

    let error = match ctx.sql("SELECT spice_checked_add(1, 2)").await {
        Err(error) => error.to_string(),
        Ok(df) => match df.collect().await {
            Err(error) => error.to_string(),
            Ok(batches) => panic!("public helper name must not plan, got {batches:?}"),
        },
    };
    assert!(
        error.to_ascii_lowercase().contains("spice_checked_add"),
        "unknown public name should be refused, got {error}"
    );
}

#[tokio::test]
async fn projection_name_for_integer_add_is_preserved() {
    let ctx = checked_session();
    let batches = ctx
        .sql("SELECT big + 1 FROM t WHERE id = 3")
        .await
        .expect("plan named add")
        .collect()
        .await
        .expect("run named add");
    let name = batches
        .first()
        .expect("one batch")
        .schema()
        .field(0)
        .name()
        .clone();
    assert!(
        name.contains("big") && (name.contains('+') || name.contains("plus")),
        "rewritten add must keep a recognizable column name, got {name}"
    );
}

#[tokio::test]
async fn checked_session_errors_on_int32_same_type_add() {
    let ctx = checked_session();
    let add = query_error(&ctx, "SELECT i + CAST(1 AS INT) FROM t WHERE id = 1").await;
    assert!(contains_overflow(&add), "Int32 add overflow, got {add}");
}

#[tokio::test]
async fn checked_session_errors_on_filter_overflow() {
    let ctx = checked_session();
    let error = query_error(&ctx, "SELECT id FROM t WHERE id = 1 AND big + 1 > 0").await;
    assert!(
        contains_overflow(&error),
        "filter BIGINT + 1 must overflow, got {error}"
    );
}

#[tokio::test]
async fn checked_session_sum_of_empty_and_all_null_is_null() {
    let ctx = checked_session();
    let empty = ctx
        .sql("SELECT SUM(big) FROM t WHERE id = 0")
        .await
        .expect("plan empty SUM")
        .collect()
        .await
        .expect("run empty SUM");
    assert_eq!(render_rows(&empty), vec!["NULL".to_string()]);
}

fn register_table(ctx: &SessionContext, name: &str, batch: RecordBatch) {
    let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).expect("mem table");
    ctx.register_table(name, Arc::new(table))
        .expect("register table");
}

fn register_int64_partitions(ctx: &SessionContext, name: &str, partitions: &[&[Option<i64>]]) {
    let schema = Arc::new(Schema::new(vec![Field::new("big", DataType::Int64, true)]));
    let batches = partitions
        .iter()
        .map(|values| {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(values.to_vec())) as ArrayRef],
            )
            .expect("partition batch");
            vec![batch]
        })
        .collect();
    let table = MemTable::try_new(schema, batches).expect("partitioned table");
    ctx.register_table(name, Arc::new(table))
        .expect("register partitioned table");
}

async fn plan_and_collect(ctx: &SessionContext, sql: &str) -> (String, Vec<RecordBatch>) {
    let df = ctx.sql(sql).await.expect("plan sql");
    let plan = df.create_physical_plan().await.expect("physical plan");
    let rendered = displayable(plan.as_ref()).indent(true).to_string();
    let batches = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .unwrap_or_else(|error| panic!("collect `{sql}`: {error}\nplan:\n{rendered}"));
    (rendered, batches)
}

async fn plan_and_error(ctx: &SessionContext, sql: &str) -> (String, String) {
    let df = ctx.sql(sql).await.expect("plan sql");
    let plan = df.create_physical_plan().await.expect("physical plan");
    let rendered = displayable(plan.as_ref()).indent(true).to_string();
    let error = match collect(plan, ctx.task_ctx()).await {
        Err(error) => error.to_string(),
        Ok(batches) => panic!("expected overflow for `{sql}`, got {batches:?}\nplan:\n{rendered}"),
    };
    (rendered, error)
}

fn assert_plan_ran(plan: &str, sql: &str, needles: &[&str]) {
    for needle in needles {
        assert!(
            plan.contains(needle),
            "plan for `{sql}` must contain `{needle}`, got:\n{plan}"
        );
    }
}

#[tokio::test]
async fn checked_group_by_int64_sum_errors_one_group_keeps_the_other() {
    let ctx = checked_session();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("g", DataType::Utf8, false),
            Field::new("big", DataType::Int64, true),
        ])),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["a", "a", "b", "b"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![
                Some(i64::MAX),
                Some(1),
                Some(1),
                Some(2),
            ])),
        ],
    )
    .expect("grouped batch");
    register_table(&ctx, "t_grp", batch);

    let overflow_sql = "SELECT g, SUM(big) FROM t_grp GROUP BY g";
    let (overflow_plan, overflow) = plan_and_error(&ctx, overflow_sql).await;
    assert_plan_ran(&overflow_plan, overflow_sql, &["AggregateExec", "gby="]);
    assert!(
        contains_overflow(&overflow),
        "GROUP BY SUM overflow, got {overflow}\nplan:\n{overflow_plan}"
    );

    let safe_sql = "SELECT g, SUM(big) FROM t_grp WHERE g = 'b' GROUP BY g";
    let (safe_plan, safe) = plan_and_collect(&ctx, safe_sql).await;
    assert_plan_ran(&safe_plan, safe_sql, &["AggregateExec", "gby="]);
    assert_eq!(render_rows(&safe), vec!["b 3".to_string()]);
}

#[tokio::test]
async fn checked_multiparts_int64_sum_cancels_and_overflows_on_merge() {
    let ctx = checked_session();
    register_int64_partitions(&ctx, "t_cancel", &[&[Some(i64::MAX)], &[Some(i64::MIN)]]);
    register_int64_partitions(&ctx, "t_merge_ovf", &[&[Some(i64::MAX)], &[Some(i64::MAX)]]);

    let cancel_sql = "SELECT SUM(big) FROM t_cancel";
    let (cancel_plan, cancel) = plan_and_collect(&ctx, cancel_sql).await;
    assert_plan_ran(
        &cancel_plan,
        cancel_sql,
        &["AggregateExec", "mode=Final", "mode=Partial"],
    );
    assert!(
        cancel_plan.contains("partitions=2") || cancel_plan.matches("MemoryExec").count() >= 1,
        "merge test must scan the two-partition table, got:\n{cancel_plan}"
    );
    assert_eq!(
        render_rows(&cancel),
        vec!["-1".to_string()],
        "MAX + MIN across partitions is -1\nplan:\n{cancel_plan}"
    );

    let overflow_sql = "SELECT SUM(big) FROM t_merge_ovf";
    let (overflow_plan, overflow) = plan_and_error(&ctx, overflow_sql).await;
    assert_plan_ran(
        &overflow_plan,
        overflow_sql,
        &["AggregateExec", "mode=Final", "mode=Partial"],
    );
    assert!(
        contains_overflow(&overflow),
        "two-partition MAX+MAX must overflow, got {overflow}\nplan:\n{overflow_plan}"
    );

    // A partition whose running total exceeds i64 must still merge. The final
    // total MAX+MAX+(-MAX) is representable.
    register_int64_partitions(
        &ctx,
        "t_partial_ovf_cancel",
        &[&[Some(i64::MAX), Some(i64::MAX)], &[Some(-i64::MAX)]],
    );
    let cancel_overflow_sql = "SELECT SUM(big) FROM t_partial_ovf_cancel";
    let (cancel_overflow_plan, cancel_overflow) = plan_and_collect(&ctx, cancel_overflow_sql).await;
    assert_plan_ran(
        &cancel_overflow_plan,
        cancel_overflow_sql,
        &["AggregateExec", "mode=Final", "mode=Partial"],
    );
    assert_eq!(
        render_rows(&cancel_overflow),
        vec![i64::MAX.to_string()],
        "MAX+MAX and -MAX across partitions must cancel to MAX\nplan:\n{cancel_overflow_plan}"
    );
}

#[tokio::test]
async fn checked_uint64_sum_and_window_overflow_at_boundary() {
    let ctx = checked_session();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("u", DataType::UInt64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![Some(u64::MAX), Some(1), Some(2)])),
        ],
    )
    .expect("uint64 batch");
    register_table(&ctx, "t_u", batch);

    let sum_sql = "SELECT SUM(u) FROM t_u";
    let (sum_plan, sum) = plan_and_error(&ctx, sum_sql).await;
    assert_plan_ran(&sum_plan, sum_sql, &["AggregateExec"]);
    assert!(
        contains_overflow(&sum),
        "SUM(UInt64) overflow, got {sum}\nplan:\n{sum_plan}"
    );

    let safe_sql = "SELECT SUM(u) FROM t_u WHERE id IN (2, 3)";
    let (safe_plan, safe) = plan_and_collect(&ctx, safe_sql).await;
    assert_plan_ran(&safe_plan, safe_sql, &["AggregateExec"]);
    assert_eq!(render_rows(&safe), vec!["3".to_string()]);

    let window_sql =
        "SELECT SUM(u) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t_u";
    let (window_plan, window) = plan_and_error(&ctx, window_sql).await;
    assert_plan_ran(&window_plan, window_sql, &["Window"]);
    assert!(
        contains_overflow(&window),
        "UInt64 window SUM overflow, got {window}\nplan:\n{window_plan}"
    );
}

#[tokio::test]
async fn checked_bounded_window_sum_retracts_and_null_only_frame_is_null() {
    let ctx = checked_session();
    let slide = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(5), Some(7), Some(11)])),
        ],
    )
    .expect("slide batch");
    register_table(&ctx, "t_slide", slide);

    let slide_sql =
        "SELECT SUM(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t_slide";
    let (slide_plan, slide_rows) = plan_and_collect(&ctx, slide_sql).await;
    assert_plan_ran(&slide_plan, slide_sql, &["Window", "ROWS"]);
    assert_eq!(
        render_rows(&slide_rows),
        vec!["5".to_string(), "12".to_string(), "18".to_string()],
        "ROWS 1 PRECEDING must retract the leaving value\nplan:\n{slide_plan}"
    );

    let nulls = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(Int64Array::from(vec![None, None])),
        ],
    )
    .expect("null window batch");
    register_table(&ctx, "t_win_null", nulls);

    let null_sql = "SELECT SUM(v) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t_win_null";
    let (null_plan, null_rows) = plan_and_collect(&ctx, null_sql).await;
    assert_plan_ran(&null_plan, null_sql, &["Window"]);
    assert_eq!(
        render_rows(&null_rows),
        vec!["NULL".to_string(), "NULL".to_string()],
        "NULL-only frames must stay NULL\nplan:\n{null_plan}"
    );

    let overflow_sql = "SELECT SUM(big) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t WHERE id IN (1, 3)";
    let (overflow_plan, overflow) = plan_and_error(&ctx, overflow_sql).await;
    assert_plan_ran(&overflow_plan, overflow_sql, &["Window"]);
    assert!(
        contains_overflow(&overflow),
        "bounded window MAX+1 must overflow, got {overflow}\nplan:\n{overflow_plan}"
    );
}

#[tokio::test]
async fn vanilla_datafusion_wraps_int64_sum_distinct() {
    let ctx = vanilla_session();
    let sum = ctx
        .sql("SELECT SUM(DISTINCT big) FROM t")
        .await
        .expect("plan vanilla SUM(DISTINCT)")
        .collect()
        .await
        .expect("run vanilla SUM(DISTINCT)");
    // All four values are distinct, so this wraps the same way as SUM(big).
    assert_eq!(render_rows(&sum), vec!["0".to_string()]);
}

#[tokio::test]
async fn checked_sum_distinct_int64_overflow_window_and_sanity() {
    let ctx = checked_session();

    let overflow_sql = "SELECT SUM(DISTINCT big) FROM t";
    let (overflow_plan, overflow) = plan_and_error(&ctx, overflow_sql).await;
    assert_plan_ran(&overflow_plan, overflow_sql, &["AggregateExec"]);
    assert!(
        contains_overflow(&overflow),
        "SUM(DISTINCT) overflow, got {overflow}\nplan:\n{overflow_plan}"
    );

    // A second non-distinct aggregate keeps `SUM(DISTINCT)` on the distinct
    // accumulator (`single_distinct_to_groupby` only fires when every aggregate
    // is DISTINCT on the same column).
    let forced_sql = "SELECT SUM(DISTINCT big), COUNT(*) FROM t";
    let (forced_plan, forced) = plan_and_error(&ctx, forced_sql).await;
    assert_plan_ran(&forced_plan, forced_sql, &["AggregateExec", "DISTINCT"]);
    assert!(
        contains_overflow(&forced),
        "SUM(DISTINCT)+COUNT overflow, got {forced}\nplan:\n{forced_plan}"
    );

    let safe_sql = "SELECT SUM(DISTINCT big) FROM t WHERE id IN (3, 4)";
    let (safe_plan, safe) = plan_and_collect(&ctx, safe_sql).await;
    assert_plan_ran(&safe_plan, safe_sql, &["AggregateExec"]);
    assert_eq!(render_rows(&safe), vec!["3".to_string()]);

    let dups = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(5), Some(5), Some(7)])),
        ],
    )
    .expect("distinct-dup batch");
    register_table(&ctx, "t_ddup", dups);
    let dups_sql = "SELECT SUM(DISTINCT v) FROM t_ddup";
    let (dups_plan, dups_rows) = plan_and_collect(&ctx, dups_sql).await;
    assert_plan_ran(&dups_plan, dups_sql, &["AggregateExec"]);
    assert_eq!(
        render_rows(&dups_rows),
        vec!["12".to_string()],
        "two 5s and a 7 must sum as 12\nplan:\n{dups_plan}"
    );

    register_int64_partitions(&ctx, "t_ddist", &[&[Some(i64::MAX)], &[Some(1)]]);
    let merge_sql = "SELECT SUM(DISTINCT big) FROM t_ddist";
    let (merge_plan, merge) = plan_and_error(&ctx, merge_sql).await;
    assert_plan_ran(
        &merge_plan,
        merge_sql,
        &["AggregateExec", "mode=Final", "mode=Partial"],
    );
    assert!(
        contains_overflow(&merge),
        "two-partition DISTINCT MAX+1 must overflow, got {merge}\nplan:\n{merge_plan}"
    );

    let window_sql = "SELECT SUM(DISTINCT big) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t WHERE id IN (1, 3)";
    let (window_plan, window) = plan_and_error(&ctx, window_sql).await;
    assert_plan_ran(&window_plan, window_sql, &["Window", "DISTINCT"]);
    assert!(
        contains_overflow(&window),
        "window SUM(DISTINCT) MAX+1 must overflow, got {window}\nplan:\n{window_plan}"
    );

    let win_ok = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(5), Some(5), Some(7)])),
        ],
    )
    .expect("window distinct-ok batch");
    register_table(&ctx, "t_dwin_ok", win_ok);
    let win_ok_sql = "SELECT SUM(DISTINCT v) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t_dwin_ok";
    let (win_ok_plan, win_ok_rows) = plan_and_collect(&ctx, win_ok_sql).await;
    assert_plan_ran(&win_ok_plan, win_ok_sql, &["Window"]);
    assert_eq!(
        render_rows(&win_ok_rows),
        vec!["5".to_string(), "5".to_string(), "12".to_string()],
        "window SUM(DISTINCT) must keep the first 5 and add 7 once\nplan:\n{win_ok_plan}"
    );
}

#[tokio::test]
async fn checked_bounded_window_sum_distinct_retracts_int64_and_uint64() {
    let ctx = checked_session();
    let expected = ["5", "5", "12", "7", "7", "NULL"].map(str::to_string);

    let i64_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])) as ArrayRef,
            Arc::new(Int64Array::from(vec![
                Some(5),
                Some(5),
                Some(7),
                Some(7),
                None,
                None,
            ])),
        ],
    )
    .expect("int64 distinct-window batch");
    register_table(&ctx, "t_dwin_i64", i64_batch);

    let i64_sql = "SELECT SUM(DISTINCT v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t_dwin_i64";
    let (i64_plan, i64_rows) = plan_and_collect(&ctx, i64_sql).await;
    assert_plan_ran(&i64_plan, i64_sql, &["Window", "DISTINCT", "ROWS"]);
    assert_eq!(
        render_rows(&i64_rows),
        expected,
        "Int64 bounded SUM(DISTINCT) must keep a duplicate, drop the last occurrence, and NULL a NULL-only frame\nplan:\n{i64_plan}"
    );

    let u64_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::UInt64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![
                Some(5),
                Some(5),
                Some(7),
                Some(7),
                None,
                None,
            ])),
        ],
    )
    .expect("uint64 distinct-window batch");
    register_table(&ctx, "t_dwin_u64", u64_batch);

    let u64_sql = "SELECT SUM(DISTINCT v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t_dwin_u64";
    let (u64_plan, u64_rows) = plan_and_collect(&ctx, u64_sql).await;
    assert_plan_ran(&u64_plan, u64_sql, &["Window", "DISTINCT", "ROWS"]);
    assert_eq!(
        render_rows(&u64_rows),
        expected,
        "UInt64 bounded SUM(DISTINCT) must keep a duplicate, drop the last occurrence, and NULL a NULL-only frame\nplan:\n{u64_plan}"
    );
}

#[tokio::test]
async fn vanilla_datafusion_wraps_mixed_sum_and_count_distinct_to_max() {
    let ctx = vanilla_session();
    let batch = cancel_across_groups_batch();
    register_table(&ctx, "t_cancel", batch);

    let mixed = ctx
        .sql("SELECT SUM(v), COUNT(DISTINCT k) FROM t_cancel")
        .await
        .expect("plan vanilla mixed")
        .collect()
        .await
        .expect("run vanilla mixed");
    assert_eq!(
        render_rows(&mixed),
        vec![format!("{} 2", i64::MAX)],
        "vanilla wrapping MAX+MAX+(-MAX) happens to be MAX"
    );
}

#[tokio::test]
async fn checked_sum_matches_across_count_distinct_rewrite_and_overflows_when_true_total_overflows()
{
    let ctx = checked_session();
    register_table(&ctx, "t_cancel", cancel_across_groups_batch());

    let sum_sql = "SELECT SUM(v) FROM t_cancel";
    let (sum_plan, sum_rows) = plan_and_collect(&ctx, sum_sql).await;
    assert_plan_ran(&sum_plan, sum_sql, &["AggregateExec"]);
    assert_eq!(
        render_rows(&sum_rows),
        vec![i64::MAX.to_string()],
        "MAX+MAX+(-MAX) is MAX\nplan:\n{sum_plan}"
    );

    let mixed_sql = "SELECT SUM(v), COUNT(DISTINCT k) FROM t_cancel";
    let (mixed_plan, mixed_rows) = plan_and_collect(&ctx, mixed_sql).await;
    assert_plan_ran(&mixed_plan, mixed_sql, &["AggregateExec"]);
    assert_eq!(
        render_rows(&mixed_rows),
        vec![format!("{} 2", i64::MAX)],
        "SUM(v), COUNT(DISTINCT k) must match SUM(v) and count two keys\nplan:\n{mixed_plan}"
    );
    assert!(
        mixed_plan.contains("DISTINCT"),
        "checked integer SUM must stay out of single_distinct_to_groupby so per-group evaluate cannot narrow early\nplan:\n{mixed_plan}"
    );

    register_table(&ctx, "t_ovf", true_total_overflow_batch());
    let ovf_sum_sql = "SELECT SUM(v) FROM t_ovf";
    let (ovf_sum_plan, ovf_sum) = plan_and_error(&ctx, ovf_sum_sql).await;
    assert_plan_ran(&ovf_sum_plan, ovf_sum_sql, &["AggregateExec"]);
    assert!(
        contains_overflow(&ovf_sum),
        "true total MAX+MAX+MAX must overflow, got {ovf_sum}\nplan:\n{ovf_sum_plan}"
    );

    let ovf_mixed_sql = "SELECT SUM(v), COUNT(DISTINCT k) FROM t_ovf";
    let (ovf_mixed_plan, ovf_mixed) = plan_and_error(&ctx, ovf_mixed_sql).await;
    assert_plan_ran(&ovf_mixed_plan, ovf_mixed_sql, &["AggregateExec"]);
    assert!(
        contains_overflow(&ovf_mixed),
        "mixed query whose true total overflows must overflow, got {ovf_mixed}\nplan:\n{ovf_mixed_plan}"
    );
}

/// What a `FlightSQL` executor runs when the scheduler pushes a complete
/// `SUM(v)` for one partition. `MAX+MAX` is not a representable `Int64`
/// total, so the checked kernel errors. The same rows as
/// `SUM(CAST(v AS DECIMAL(38, 0)))` stay wide — that is the SQL the
/// cluster pushdown must send so the scheduler can still cancel.
#[tokio::test]
async fn executor_plain_sum_errors_on_overflowing_partition_decimal_sum_stays_wide() {
    let ctx = checked_session();
    register_int64_partitions(&ctx, "t_exec", &[&[Some(i64::MAX), Some(i64::MAX)]]);

    let plain_sql = "SELECT SUM(big) FROM t_exec";
    let (plain_plan, plain) = plan_and_error(&ctx, plain_sql).await;
    assert_plan_ran(&plain_plan, plain_sql, &["AggregateExec"]);
    assert!(
        contains_overflow(&plain),
        "executor-style SUM(v) of MAX+MAX must overflow, got {plain}\nplan:\n{plain_plan}"
    );

    let wide_sql = "SELECT SUM(CAST(big AS DECIMAL(38, 0))) FROM t_exec";
    let (wide_plan, wide) = plan_and_collect(&ctx, wide_sql).await;
    assert_plan_ran(&wide_plan, wide_sql, &["AggregateExec"]);
    assert_eq!(
        render_rows(&wide),
        vec![(i128::from(i64::MAX) * 2).to_string()],
        "executor-style wide SUM must keep MAX+MAX\nplan:\n{wide_plan}"
    );
}

fn aggregates_below_a_join(plan: &Arc<dyn ExecutionPlan>, below_a_join: bool) -> bool {
    use datafusion::physical_plan::aggregates::AggregateExec;

    if below_a_join && plan.is::<AggregateExec>() {
        return true;
    }
    let below_a_join = below_a_join || plan.is::<HashJoinExec>();
    plan.children()
        .into_iter()
        .any(|child| aggregates_below_a_join(child, below_a_join))
}

/// Fact rows that make `EagerAggregation`'s cost gate fire (10k rows, 100
/// join keys, a filter that keeps two keys) and whose join-key subtotals
/// overflow but cancel: key 1 is `[MAX, MAX, 0…]`, key 2 is `[-MAX, 0…]`.
/// Both keys map to the same `GROUP BY` name, so the true `SUM` is `MAX`.
fn register_eager_join_tables(ctx: &SessionContext) {
    let fact_schema = Arc::new(Schema::new(vec![
        Field::new("fk", DataType::Int32, false),
        Field::new("amount", DataType::Int64, false),
        Field::new("n", DataType::Int32, false),
    ]));
    let fact = RecordBatch::try_new(
        Arc::clone(&fact_schema),
        vec![
            Arc::new(Int32Array::from_iter_values((1..=10_000).map(|i| i % 100))) as ArrayRef,
            Arc::new(Int64Array::from_iter_values((1..=10_000).map(|i| {
                let fk = i % 100;
                let nth = (i - 1) / 100;
                match (fk, nth) {
                    (1, 0 | 1) => i64::MAX,
                    (2, 0) => -i64::MAX,
                    _ => 0,
                }
            }))),
            Arc::new(Int32Array::from_iter_values(std::iter::repeat_n(1, 10_000))),
        ],
    )
    .expect("eager-join fact batch");
    let dim_schema = Arc::new(Schema::new(vec![
        Field::new("dk", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let dim = RecordBatch::try_new(
        Arc::clone(&dim_schema),
        vec![
            Arc::new(Int32Array::from_iter_values(0..100)) as ArrayRef,
            Arc::new(arrow::array::StringArray::from_iter_values((0..100).map(
                |k| {
                    if k == 1 || k == 2 {
                        "g".to_string()
                    } else {
                        format!("n{k}")
                    }
                },
            ))),
        ],
    )
    .expect("eager-join dim batch");
    for (name, schema, batch) in [("fact", fact_schema, fact), ("dim", dim_schema, dim)] {
        let table = MemTable::try_new(schema, vec![vec![batch]]).expect("mem table");
        ctx.register_table(name, Arc::new(table))
            .expect("register eager-join table");
    }
}

fn register_eager_overflow_tables(ctx: &SessionContext) {
    let fact_schema = Arc::new(Schema::new(vec![
        Field::new("fk", DataType::Int32, false),
        Field::new("amount", DataType::Int64, false),
    ]));
    let fact = RecordBatch::try_new(
        Arc::clone(&fact_schema),
        vec![
            Arc::new(Int32Array::from_iter_values((1..=10_000).map(|i| i % 100))) as ArrayRef,
            Arc::new(Int64Array::from_iter_values((1..=10_000).map(|i| {
                let fk = i % 100;
                let nth = (i - 1) / 100;
                match (fk, nth) {
                    (1 | 2, 0 | 1) => i64::MAX,
                    _ => 0,
                }
            }))),
        ],
    )
    .expect("eager-overflow fact batch");
    let dim_schema = Arc::new(Schema::new(vec![
        Field::new("dk", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let dim = RecordBatch::try_new(
        Arc::clone(&dim_schema),
        vec![
            Arc::new(Int32Array::from_iter_values(0..100)) as ArrayRef,
            Arc::new(arrow::array::StringArray::from_iter_values((0..100).map(
                |k| {
                    if k == 1 || k == 2 {
                        "g".to_string()
                    } else {
                        format!("n{k}")
                    }
                },
            ))),
        ],
    )
    .expect("eager-overflow dim batch");
    for (name, schema, batch) in [
        ("fact_ovf", fact_schema, fact),
        ("dim_ovf", dim_schema, dim),
    ] {
        let table = MemTable::try_new(schema, vec![vec![batch]]).expect("mem table");
        ctx.register_table(name, Arc::new(table))
            .expect("register eager-overflow table");
    }
}

const EAGER_JOIN_SUM_SQL: &str = "SELECT d.name, SUM(f.amount) AS total FROM fact f JOIN dim d \
     ON f.fk = d.dk WHERE f.fk IN (1, 2) GROUP BY d.name";
const EAGER_JOIN_INT32_SUM_SQL: &str = "SELECT d.name, SUM(f.n) AS total FROM fact f JOIN dim d \
     ON f.fk = d.dk WHERE f.fk IN (1, 2) GROUP BY d.name";
const EAGER_JOIN_OVERFLOW_SQL: &str = "SELECT d.name, SUM(f.amount) AS total FROM fact_ovf f \
     JOIN dim_ovf d ON f.fk = d.dk WHERE f.fk IN (1, 2) GROUP BY d.name";

async fn plan_eager_join_sql(
    ctx: &SessionContext,
    sql: &str,
) -> (bool, String, std::result::Result<Vec<String>, String>) {
    let df = ctx.sql(sql).await.expect("plan eager-join sql");
    let plan = df.create_physical_plan().await.expect("physical plan");
    let rendered = displayable(plan.as_ref()).indent(true).to_string();
    let pushed = aggregates_below_a_join(&plan, false);
    let rows = match collect(plan, ctx.task_ctx()).await {
        Ok(batches) => Ok(render_rows(&batches)),
        Err(error) => Err(error.to_string()),
    };
    (pushed, rendered, rows)
}

/// Codex round 4: `EagerAggregation` completes a checked integer `SUM` per
/// join-key group (`Partial` → `FinalPartitioned` below the join), so
/// `evaluate()` narrows to `i64` before groups can cancel.
///
/// Stats match the fork-patch guard (`10_000` fact rows, `100` keys, a
/// `FilterExec` sunk below the join). Join keys `1` (`[MAX, MAX]`) and `2`
/// (`[-MAX]`) both map to `name = 'g'`. The true total is `MAX`.
#[tokio::test]
async fn checked_sum_matches_across_eager_aggregation_and_overflows_when_true_total_overflows() {
    let expected_cancel = vec![format!("g {}", i64::MAX)];
    let expected_int32 = vec!["g 200".to_string()];

    let ctx_on = checked_session_with_eager(true);
    register_eager_join_tables(&ctx_on);
    let (int32_pushed, int32_plan, int32_rows) =
        plan_eager_join_sql(&ctx_on, EAGER_JOIN_INT32_SUM_SQL).await;
    assert!(
        int32_pushed,
        "Int32 SUM must still be pre-aggregated below the join when eager aggregation is on, \
         or the cost-gate control cannot tell the rule firing from the plan's own shape:\n{int32_plan}"
    );
    assert_eq!(
        int32_rows.expect("Int32 SUM must succeed"),
        expected_int32,
        "Int32 SUM over the join must stay 200\nplan:\n{int32_plan}"
    );

    let (on_pushed, on_plan, on_rows) = plan_eager_join_sql(&ctx_on, EAGER_JOIN_SUM_SQL).await;
    assert!(
        !on_pushed,
        "checked integer SUM must stay out of EagerAggregation so per-join-key evaluate cannot \
         narrow early\nplan:\n{on_plan}"
    );
    let on_rows = on_rows.unwrap_or_else(|error| {
        panic!("eager on: representable join SUM must not overflow, got {error}\nplan:\n{on_plan}")
    });
    assert_eq!(
        on_rows, expected_cancel,
        "eager on: join-key [MAX, MAX] + [-MAX] must cancel to MAX\nplan:\n{on_plan}"
    );

    let ctx_off = checked_session_with_eager(false);
    register_eager_join_tables(&ctx_off);
    let (off_pushed, off_plan, off_rows) = plan_eager_join_sql(&ctx_off, EAGER_JOIN_SUM_SQL).await;
    assert!(
        !off_pushed,
        "eager off: nothing may be pre-aggregated below the join:\n{off_plan}"
    );
    assert_eq!(
        off_rows.expect("eager off SUM must succeed"),
        expected_cancel,
        "eager off: join-key [MAX, MAX] + [-MAX] must cancel to MAX\nplan:\n{off_plan}"
    );
    assert_eq!(
        on_rows, expected_cancel,
        "eager on and off must return the same representable total"
    );

    register_eager_overflow_tables(&ctx_on);
    let (_ovf_on_pushed, ovf_on_plan, ovf_on_rows) =
        plan_eager_join_sql(&ctx_on, EAGER_JOIN_OVERFLOW_SQL).await;
    let ovf_on = ovf_on_rows.expect_err("true overflow with eager on must error");
    assert!(
        contains_overflow(&ovf_on),
        "eager on: MAX+MAX+MAX+MAX must overflow, got {ovf_on}\nplan:\n{ovf_on_plan}"
    );

    register_eager_overflow_tables(&ctx_off);
    let (_ovf_off_pushed, ovf_off_plan, ovf_off_rows) =
        plan_eager_join_sql(&ctx_off, EAGER_JOIN_OVERFLOW_SQL).await;
    let ovf_off = ovf_off_rows.expect_err("true overflow with eager off must error");
    assert!(
        contains_overflow(&ovf_off),
        "eager off: MAX+MAX+MAX+MAX must overflow, got {ovf_off}\nplan:\n{ovf_off_plan}"
    );

    if std::env::var("DUMP_EAGER_PLANS").is_ok() {
        eprintln!("int32 pushed={int32_pushed} rows={expected_int32:?}\n{int32_plan}");
        eprintln!("eager on pushed={on_pushed} rows={on_rows:?}\n{on_plan}");
        eprintln!("eager off pushed={off_pushed} rows={expected_cancel:?}\n{off_plan}");
        eprintln!("overflow on: {ovf_on}\n{ovf_on_plan}");
        eprintln!("overflow off: {ovf_off}\n{ovf_off_plan}");
    }
}

fn cancel_across_groups_cast_batch() -> RecordBatch {
    let max = i64::MAX.to_string();
    let neg = (-i64::MAX).to_string();
    let decimals = Decimal128Array::from(vec![
        Some(i128::from(i64::MAX)),
        Some(i128::from(i64::MAX)),
        Some(-i128::from(i64::MAX)),
    ])
    .with_precision_and_scale(38, 0)
    .expect("decimal128 cancel values");
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("s", DataType::Utf8, true),
            Field::new("d", DataType::Decimal128(38, 0), true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some(max.as_str()),
                Some(max.as_str()),
                Some(neg.as_str()),
            ])),
            Arc::new(decimals),
        ],
    )
    .expect("cast-cancel batch")
}

fn true_total_overflow_cast_batch() -> RecordBatch {
    let max = i64::MAX.to_string();
    let decimals = Decimal128Array::from(vec![
        Some(i128::from(i64::MAX)),
        Some(i128::from(i64::MAX)),
        Some(i128::from(i64::MAX)),
    ])
    .with_precision_and_scale(38, 0)
    .expect("decimal128 overflow values");
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("s", DataType::Utf8, true),
            Field::new("d", DataType::Decimal128(38, 0), true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some(max.as_str()),
                Some(max.as_str()),
                Some(max.as_str()),
            ])),
            Arc::new(decimals),
        ],
    )
    .expect("cast-overflow batch")
}

fn eager_join_cast_amount(i: i32) -> i64 {
    let fk = i % 100;
    let nth = (i - 1) / 100;
    match (fk, nth) {
        (1, 0 | 1) => i64::MAX,
        (2, 0) => -i64::MAX,
        _ => 0,
    }
}

fn register_eager_join_cast_tables(ctx: &SessionContext) {
    let amounts: Vec<i64> = (1..=10_000).map(eager_join_cast_amount).collect();
    let strings: Vec<String> = amounts.iter().map(ToString::to_string).collect();
    let decimals = Decimal128Array::from(
        amounts
            .iter()
            .map(|value| Some(i128::from(*value)))
            .collect::<Vec<_>>(),
    )
    .with_precision_and_scale(38, 0)
    .expect("eager-join decimal amounts");
    let fact_schema = Arc::new(Schema::new(vec![
        Field::new("fk", DataType::Int32, false),
        Field::new("amount_str", DataType::Utf8, false),
        Field::new("amount_dec", DataType::Decimal128(38, 0), false),
    ]));
    let fact = RecordBatch::try_new(
        Arc::clone(&fact_schema),
        vec![
            Arc::new(Int32Array::from_iter_values((1..=10_000).map(|i| i % 100))) as ArrayRef,
            Arc::new(StringArray::from_iter_values(strings)),
            Arc::new(decimals),
        ],
    )
    .expect("eager-join cast fact batch");
    let dim_schema = Arc::new(Schema::new(vec![
        Field::new("dk", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let dim = RecordBatch::try_new(
        Arc::clone(&dim_schema),
        vec![
            Arc::new(Int32Array::from_iter_values(0..100)) as ArrayRef,
            Arc::new(StringArray::from_iter_values((0..100).map(|k| {
                if k == 1 || k == 2 {
                    "g".to_string()
                } else {
                    format!("n{k}")
                }
            }))),
        ],
    )
    .expect("eager-join cast dim batch");
    for (name, schema, batch) in [
        ("fact_cast", fact_schema, fact),
        ("dim_cast", dim_schema, dim),
    ] {
        let table = MemTable::try_new(schema, vec![vec![batch]]).expect("mem table");
        ctx.register_table(name, Arc::new(table))
            .expect("register eager-join cast table");
    }
}

const EAGER_JOIN_CAST_SUM_SQL: &str = "SELECT d.name, SUM(CAST(f.amount_str AS BIGINT)) AS total \
     FROM fact_cast f JOIN dim_cast d ON f.fk = d.dk WHERE f.fk IN (1, 2) GROUP BY d.name";
const EAGER_JOIN_TRY_CAST_SUM_SQL: &str = "SELECT d.name, SUM(TRY_CAST(f.amount_dec AS BIGINT)) AS total \
     FROM fact_cast f JOIN dim_cast d ON f.fk = d.dk WHERE f.fk IN (1, 2) GROUP BY d.name";

/// Copilot on `804b67cd53`: the unwrap helpers used to strip every
/// `CAST` / `TRY_CAST`, so `SUM(CAST(utf8 AS BIGINT))` and
/// `SUM(TRY_CAST(decimal AS BIGINT))` looked like non-64-bit SUMs and
/// could complete early. Keys `[1, 1, 2]`, values `[MAX, MAX, -MAX]`.
#[tokio::test]
async fn checked_sum_cast_and_try_cast_to_bigint_cancel_across_single_distinct_and_eager() {
    let expected_sum = vec![i64::MAX.to_string()];
    let expected_mixed = vec![format!("{} 2", i64::MAX)];
    let expected_join = vec![format!("g {}", i64::MAX)];

    let ctx = checked_session();
    register_table(&ctx, "t_cast", cancel_across_groups_cast_batch());

    for (label, sum_sql, mixed_sql) in [
        (
            "CAST(utf8 AS BIGINT)",
            "SELECT SUM(CAST(s AS BIGINT)) FROM t_cast",
            "SELECT SUM(CAST(s AS BIGINT)), COUNT(DISTINCT k) FROM t_cast",
        ),
        (
            "TRY_CAST(decimal AS BIGINT)",
            "SELECT SUM(TRY_CAST(d AS BIGINT)) FROM t_cast",
            "SELECT SUM(TRY_CAST(d AS BIGINT)), COUNT(DISTINCT k) FROM t_cast",
        ),
    ] {
        let (sum_plan, sum_rows) = plan_and_collect(&ctx, sum_sql).await;
        assert_plan_ran(&sum_plan, sum_sql, &["AggregateExec"]);
        assert_eq!(
            render_rows(&sum_rows),
            expected_sum,
            "{label}: MAX+MAX+(-MAX) is MAX\nplan:\n{sum_plan}"
        );

        let (mixed_plan, mixed_rows) = plan_and_collect(&ctx, mixed_sql).await;
        assert_plan_ran(&mixed_plan, mixed_sql, &["AggregateExec"]);
        assert_eq!(
            render_rows(&mixed_rows),
            expected_mixed,
            "{label}: mixed SUM + COUNT(DISTINCT) must match SUM and count two keys\nplan:\n{mixed_plan}"
        );
        assert!(
            mixed_plan.contains("DISTINCT"),
            "{label}: checked CAST/TRY_CAST SUM must stay out of single_distinct_to_groupby\nplan:\n{mixed_plan}"
        );
    }

    register_table(&ctx, "t_cast_ovf", true_total_overflow_cast_batch());
    for (label, sql) in [
        (
            "CAST overflow",
            "SELECT SUM(CAST(s AS BIGINT)), COUNT(DISTINCT k) FROM t_cast_ovf",
        ),
        (
            "TRY_CAST overflow",
            "SELECT SUM(TRY_CAST(d AS BIGINT)), COUNT(DISTINCT k) FROM t_cast_ovf",
        ),
    ] {
        let (ovf_plan, ovf) = plan_and_error(&ctx, sql).await;
        assert_plan_ran(&ovf_plan, sql, &["AggregateExec"]);
        assert!(
            contains_overflow(&ovf),
            "{label}: MAX+MAX+MAX must overflow, got {ovf}\nplan:\n{ovf_plan}"
        );
    }

    let ctx_on = checked_session_with_eager(true);
    register_eager_join_cast_tables(&ctx_on);
    let ctx_off = checked_session_with_eager(false);
    register_eager_join_cast_tables(&ctx_off);

    for (label, sql) in [
        ("CAST eager", EAGER_JOIN_CAST_SUM_SQL),
        ("TRY_CAST eager", EAGER_JOIN_TRY_CAST_SUM_SQL),
    ] {
        let (on_pushed, on_plan, on_rows) = plan_eager_join_sql(&ctx_on, sql).await;
        assert!(
            !on_pushed,
            "{label} on: must stay out of EagerAggregation\nplan:\n{on_plan}"
        );
        let on_rows = on_rows.unwrap_or_else(|error| {
            panic!("{label} on: representable join SUM must not overflow, got {error}\nplan:\n{on_plan}")
        });
        assert_eq!(
            on_rows, expected_join,
            "{label} on: join-key [MAX, MAX] + [-MAX] must cancel to MAX\nplan:\n{on_plan}"
        );

        let (off_pushed, off_plan, off_rows) = plan_eager_join_sql(&ctx_off, sql).await;
        assert!(
            !off_pushed,
            "{label} off: nothing may be pre-aggregated below the join:\n{off_plan}"
        );
        assert_eq!(
            off_rows.expect("eager off CAST/TRY_CAST SUM must succeed"),
            expected_join,
            "{label} off: join-key [MAX, MAX] + [-MAX] must cancel to MAX\nplan:\n{off_plan}"
        );
    }
}

fn cancel_across_groups_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])) as ArrayRef,
            Arc::new(Int64Array::from(vec![
                Some(i64::MAX),
                Some(i64::MAX),
                Some(-i64::MAX),
            ])),
        ],
    )
    .expect("cancel-across-groups batch")
}

fn true_total_overflow_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])) as ArrayRef,
            Arc::new(Int64Array::from(vec![
                Some(i64::MAX),
                Some(i64::MAX),
                Some(i64::MAX),
            ])),
        ],
    )
    .expect("true-total-overflow batch")
}

async fn run_slt(ctx: &SessionContext, script: &str) {
    let mut lines = script.lines().peekable();
    while let Some(line) = lines.next() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if let Some(sql) = trimmed.strip_prefix("statement ok") {
            let sql = next_sql(sql, &mut lines);
            ctx.sql(&sql)
                .await
                .unwrap_or_else(|error| panic!("statement ok `{sql}` plans: {error}"))
                .collect()
                .await
                .unwrap_or_else(|error| panic!("statement ok `{sql}` runs: {error}"));
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix("statement error") {
            let expected = rest.trim();
            let sql = next_sql("", &mut lines);
            let error = query_error(ctx, &sql).await;
            assert!(
                error
                    .to_ascii_lowercase()
                    .contains(&expected.to_ascii_lowercase()),
                "statement error `{sql}` should contain `{expected}`, got {error}"
            );
            continue;
        }
        if let Some(_types) = trimmed.strip_prefix("query ") {
            let sql = next_sql("", &mut lines);
            let separator = lines.next().map(str::trim).unwrap_or_default();
            assert_eq!(separator, "----", "query `{sql}` must be followed by ----");
            let mut expected = Vec::new();
            for row in lines.by_ref() {
                if row.trim().is_empty() {
                    break;
                }
                expected.push(row.trim().to_string());
            }
            let batches = ctx
                .sql(&sql)
                .await
                .unwrap_or_else(|error| panic!("query `{sql}` plans: {error}"))
                .collect()
                .await
                .unwrap_or_else(|error| panic!("query `{sql}` runs: {error}"));
            assert_eq!(render_rows(&batches), expected, "query `{sql}`");
        }
    }
}

fn next_sql(first: &str, lines: &mut std::iter::Peekable<std::str::Lines<'_>>) -> String {
    let mut sql = first.trim().to_string();
    if !sql.is_empty() {
        return sql;
    }
    for next in lines.by_ref() {
        let trimmed = next.trim();
        if trimmed.is_empty() {
            break;
        }
        if !sql.is_empty() {
            sql.push(' ');
        }
        sql.push_str(trimmed);
        if trimmed.ends_with(';') {
            break;
        }
    }
    sql.trim_end_matches(';').to_string()
}

async fn analyzed_plan(ctx: &SessionContext, sql: &str) -> String {
    let state = ctx.state();
    let plan = state
        .create_logical_plan(sql)
        .await
        .unwrap_or_else(|error| panic!("`{sql}` plans: {error}"));
    let plan = state
        .optimize(&plan)
        .unwrap_or_else(|error| panic!("`{sql}` optimizes: {error}"));
    format!("{}", plan.display_indent())
}

fn names_a_checked_op(plan: &str) -> bool {
    [
        CHECKED_ADD_UDF_NAME,
        CHECKED_SUB_UDF_NAME,
        CHECKED_MUL_UDF_NAME,
    ]
    .iter()
    .any(|name| plan.contains(name))
}

/// A table provider receives an `UPDATE`'s assignments and a `DELETE`'s
/// filters, and one backed by another engine renders them as SQL there:
/// `__spice_checked_add("b", 1)` is a function PostgreSQL does not have, so a
/// write-through `UPDATE t SET b = b + 1` failed with "db error".
#[tokio::test]
async fn update_and_delete_keep_the_operators_for_the_table_provider() {
    let ctx = checked_session();
    for sql in [
        "UPDATE t SET big = big + 1, i = i * 2 WHERE id - 1 = 2",
        "DELETE FROM t WHERE big + 1 > 2 AND id * 2 < 8",
    ] {
        let plan = analyzed_plan(&ctx, sql).await;
        assert!(
            !names_a_checked_op(&plan),
            "`{sql}` must keep `+` / `-` / `*`:\n{plan}"
        );
    }

    // The rewrite still runs everywhere else in this session, the rows an
    // `INSERT` selects included: DataFusion computes those itself.
    for sql in [
        "SELECT big + 1 FROM t",
        "INSERT INTO t SELECT id + 10, big - 1, i * 2 FROM t",
    ] {
        let plan = analyzed_plan(&ctx, sql).await;
        assert!(
            names_a_checked_op(&plan),
            "`{sql}` must use the checked kernels:\n{plan}"
        );
    }
}
