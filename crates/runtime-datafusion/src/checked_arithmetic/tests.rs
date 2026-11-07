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
    ArrayRef, Decimal128Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
    UInt64Array,
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
    CHECKED_ADD_UDF_NAME, CHECKED_MUL_UDF_NAME, CHECKED_SUB_UDF_NAME,
    CHECKED_SUM_PLUS_COUNT_UDF_NAME, CHECKED_SUM_WIDEN_UDF_NAME, CheckedIntegerOverflow,
    checked_sum_udaf, install_checked_eager_aggregation, install_checked_single_distinct,
    plain_arithmetic, register_checked_arithmetic,
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

/// The checked kernels must report the nullability `DataFusion`'s operators
/// do: a Flight SQL client compares the schema a plan advertises with the one
/// it streams, and the two disagreed on `id + id` once the rewrite reported
/// every result nullable.
#[tokio::test]
async fn checked_ops_keep_the_operators_nullability() {
    let sql = "SELECT id + id AS a, id - 1 AS s, id * 2 AS m, big - 1 AS nullable_sub FROM t";
    let nullability = |schema: &Schema| {
        schema
            .fields()
            .iter()
            .map(|field| (field.name().clone(), field.is_nullable()))
            .collect::<Vec<_>>()
    };

    let vanilla = vanilla_session()
        .sql(sql)
        .await
        .expect("plan vanilla arithmetic")
        .collect()
        .await
        .expect("run vanilla arithmetic");
    let expected = nullability(
        vanilla
            .first()
            .expect("one vanilla batch")
            .schema()
            .as_ref(),
    );
    assert_eq!(
        expected,
        vec![
            ("a".to_string(), false),
            ("s".to_string(), false),
            ("m".to_string(), false),
            ("nullable_sub".to_string(), true),
        ],
        "DataFusion's own operators are the reference"
    );

    let ctx = checked_session();
    let df = ctx.sql(sql).await.expect("plan checked arithmetic");
    let plan = df
        .clone()
        .into_optimized_plan()
        .expect("optimize checked arithmetic");
    assert!(
        format!("{}", plan.display_indent()).contains(CHECKED_SUB_UDF_NAME),
        "the analyzer must have rewritten the operators:\n{}",
        plan.display_indent()
    );
    assert_eq!(
        nullability(plan.schema().as_arrow()),
        expected,
        "optimized plan schema"
    );
    let batches = df.collect().await.expect("run checked arithmetic");
    assert_eq!(
        nullability(batches.first().expect("one batch").schema().as_ref()),
        expected,
        "streamed schema"
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
    register_partitions(ctx, name, schema, batches);
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

/// Partial aggregation decides after the first batch and, with 1 group per
/// 1 row, skips the rest, so later rows reach the final aggregate through
/// `convert_to_state`. Two partitions force `Partial` and `FinalPartitioned`.
async fn skip_partial_aggregation_after_one_row(ctx: &SessionContext) {
    for setting in [
        "SET datafusion.execution.target_partitions = 2",
        "SET datafusion.execution.skip_partial_aggregation_probe_rows_threshold = 1",
        "SET datafusion.execution.skip_partial_aggregation_probe_ratio_threshold = 0.1",
    ] {
        ctx.sql(setting)
            .await
            .unwrap_or_else(|error| panic!("`{setting}`: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("apply `{setting}`: {error}"));
    }
}

fn skipped_aggregation_rows(plan: &Arc<dyn ExecutionPlan>) -> usize {
    let skipped = plan
        .metrics()
        .and_then(|metrics| metrics.sum_by_name("skipped_aggregation_rows"));
    let own = if let Some(value) = skipped {
        value.as_usize()
    } else {
        0
    };
    own + plan
        .children()
        .into_iter()
        .map(skipped_aggregation_rows)
        .sum::<usize>()
}

/// Runs `sql` and returns its rows and the rows partial aggregation skipped.
async fn collect_counting_skipped_rows(ctx: &SessionContext, sql: &str) -> (Vec<String>, usize) {
    let plan = ctx
        .sql(sql)
        .await
        .expect("plan sql")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let batches = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .unwrap_or_else(|error| panic!("collect `{sql}`: {error}"));
    (render_rows(&batches), skipped_aggregation_rows(&plan))
}

/// Two partitions of three 250-row batches: 1,500 rows in 300 groups. `v`
/// mixes signs at ~10^13, is NULL every seventh row and in all of group 0;
/// `u` is unsigned and NULL every eleventh row; `keep` is NULL every
/// thirteenth row and false every fifth row and in all of group 1.
fn group_by_partitions() -> (Arc<Schema>, Vec<Vec<RecordBatch>>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
        Field::new("u", DataType::UInt64, true),
        Field::new("keep", DataType::Boolean, true),
    ]));
    let partitions = (0..2_i64)
        .map(|partition| {
            (0..3_i64)
                .map(|batch| {
                    let first = (partition * 3 + batch) * 250;
                    let rows: Vec<i64> = (first..first + 250).collect();
                    let g = Int64Array::from_iter_values(rows.iter().map(|row| row % 300));
                    let v = Int64Array::from_iter(rows.iter().map(|row| {
                        (row % 7 != 0 && row % 300 != 0)
                            .then_some(((row * 7_919) % 20_011 - 10_005) * 1_000_000_007)
                    }));
                    let u = UInt64Array::from_iter(rows.iter().map(|row| {
                        let row = u64::try_from(*row).expect("row numbers are non-negative");
                        (!row.is_multiple_of(11)).then_some(row * 3_000_000_019)
                    }));
                    let keep = arrow::array::BooleanArray::from_iter(
                        rows.iter()
                            .map(|row| (row % 13 != 0).then_some(row % 5 != 0 && row % 300 != 1)),
                    );
                    RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![
                            Arc::new(g) as ArrayRef,
                            Arc::new(v),
                            Arc::new(u),
                            Arc::new(keep),
                        ],
                    )
                    .expect("group-by batch")
                })
                .collect()
        })
        .collect();
    (schema, partitions)
}

fn register_partitions(
    ctx: &SessionContext,
    name: &str,
    schema: Arc<Schema>,
    partitions: Vec<Vec<RecordBatch>>,
) {
    let table = MemTable::try_new(schema, partitions).expect("partitioned table");
    ctx.register_table(name, Arc::new(table))
        .expect("register partitioned table");
}

#[tokio::test]
async fn checked_group_by_sum_matches_builtin_sum_when_partial_aggregation_is_skipped() {
    let sql = "SELECT g, SUM(v), SUM(v) FILTER (WHERE keep), SUM(u) \
               FROM t_groups GROUP BY g ORDER BY g";
    let checked = checked_session();
    skip_partial_aggregation_after_one_row(&checked).await;
    let vanilla = vanilla_session();
    for ctx in [&checked, &vanilla] {
        let (schema, partitions) = group_by_partitions();
        register_partitions(ctx, "t_groups", schema, partitions);
    }

    let (rows, skipped) = collect_counting_skipped_rows(&checked, sql).await;
    let expected = render_rows(
        &vanilla
            .sql(sql)
            .await
            .expect("plan built-in SUM")
            .collect()
            .await
            .expect("run built-in SUM"),
    );
    assert_eq!(rows.len(), 300, "one row per group");
    assert_eq!(rows[0], expected[0]);
    assert!(
        rows[0].starts_with("0 NULL "),
        "group 0 has no v, got {}",
        rows[0]
    );
    assert!(
        rows[1].contains(" NULL "),
        "group 1 keeps no row in the FILTER sum, got {}",
        rows[1]
    );
    assert_eq!(rows, expected, "checked SUM must match the built-in SUM");
    assert!(
        skipped > 0 && skipped < 1_500,
        "partial aggregation must skip the rows after its first batches, skipped {skipped}"
    );
}

/// A partition per value list, each value list split into a first batch of
/// one row in group 1 and a second batch carrying the values in group 0.
fn skipped_rows_partitions(values: &[&[i64]]) -> (Arc<Schema>, Vec<Vec<RecordBatch>>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, false),
        Field::new("big", DataType::Int64, true),
    ]));
    let batch = |groups: Vec<i64>, values: Vec<i64>| {
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(groups)) as ArrayRef,
                Arc::new(Int64Array::from(values)),
            ],
        )
        .expect("skipped-rows batch")
    };
    let partitions = values
        .iter()
        .map(|values| {
            vec![
                batch(vec![1], vec![1]),
                batch(vec![0; values.len()], values.to_vec()),
            ]
        })
        .collect();
    (schema, partitions)
}

#[tokio::test]
async fn checked_group_by_sum_cancels_and_overflows_across_skipped_partial_aggregation() {
    let ctx = checked_session();
    skip_partial_aggregation_after_one_row(&ctx).await;

    let (schema, partitions) = skipped_rows_partitions(&[&[i64::MAX, i64::MAX], &[-i64::MAX]]);
    register_partitions(&ctx, "t_skip_cancel", schema, partitions);
    let cancel_sql = "SELECT g, SUM(big) FROM t_skip_cancel GROUP BY g ORDER BY g";
    let (rows, skipped) = collect_counting_skipped_rows(&ctx, cancel_sql).await;
    assert_eq!(
        rows,
        vec![format!("0 {}", i64::MAX), "1 2".to_string()],
        "MAX+MAX and -MAX in skipped rows must cancel to MAX"
    );
    assert_eq!(
        skipped, 3,
        "the three group-0 rows skip partial aggregation"
    );

    let (schema, partitions) = skipped_rows_partitions(&[&[i64::MAX], &[i64::MAX]]);
    register_partitions(&ctx, "t_skip_overflow", schema, partitions);
    let overflow_sql = "SELECT g, SUM(big) FROM t_skip_overflow GROUP BY g ORDER BY g";
    let (plan, error) = plan_and_error(&ctx, overflow_sql).await;
    assert!(
        contains_overflow(&error),
        "MAX+MAX in skipped rows must overflow, got {error}\nplan:\n{plan}"
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

/// The plan `sql` optimizes to on `ctx`, as `EXPLAIN` indents it.
async fn optimized_plan(ctx: &SessionContext, sql: &str) -> datafusion::logical_expr::LogicalPlan {
    let state = ctx.state();
    let plan = state
        .create_logical_plan(sql)
        .await
        .unwrap_or_else(|error| panic!("`{sql}` plans: {error}"));
    state
        .optimize(&plan)
        .unwrap_or_else(|error| panic!("`{sql}` optimizes: {error}"))
}

/// A write a provider renders for another engine carries the operators
/// `DataFusion` itself plans, not Spice's checked kernels: restoring them in
/// the checked session's plan gives exactly the plan of a session that never
/// rewrote them.
#[tokio::test]
async fn plain_arithmetic_restores_the_operators_of_an_update_and_a_delete() {
    use datafusion::common::tree_node::{Transformed, TreeNode};

    for sql in [
        "UPDATE t SET big = big + 1, i = i * 2 WHERE big - id > 0",
        "DELETE FROM t WHERE big * 2 = id + 1",
    ] {
        let checked = optimized_plan(&checked_session(), sql).await;
        let rendered = checked.display_indent().to_string();
        assert!(
            [
                CHECKED_ADD_UDF_NAME,
                CHECKED_SUB_UDF_NAME,
                CHECKED_MUL_UDF_NAME
            ]
            .iter()
            .any(|name| rendered.contains(name)),
            "the checked session rewrites `{sql}`:\n{rendered}"
        );

        let restored = checked
            .transform_up(|plan| {
                plan.map_expressions(|expr| plain_arithmetic(expr).map(Transformed::yes))
            })
            .expect("restore the operators")
            .data;
        let vanilla = optimized_plan(&vanilla_session(), sql).await;
        assert_eq!(
            restored.display_indent().to_string(),
            vanilla.display_indent().to_string(),
            "`{sql}`"
        );
    }
}

/// The number of aggregate expressions in each `Aggregate` of `plan`.
fn aggregate_widths(plan: &datafusion::logical_expr::LogicalPlan) -> Vec<usize> {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::logical_expr::LogicalPlan;

    let mut widths = Vec::new();
    plan.apply(|node| {
        if let LogicalPlan::Aggregate(aggregate) = node {
            widths.push(aggregate.aggr_expr.len());
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("walk plan");
    widths
}

/// `SUM(x + 1), SUM(x + 2), ...` over a column whose `x + k` cannot overflow
/// is one wide `SUM` and one `COUNT` of `x` however many literals there are,
/// as `DataFusion` plans it (`ClickBench` Q29 has a bare `SUM(x)` and 89 of
/// them), not one checked `SUM` per literal. The two are named apart from the
/// query's own `SUM(x)` and `COUNT(x)`. NULL rows count in neither, and a
/// group of only NULLs sums to NULL.
#[tokio::test]
async fn sums_of_a_column_plus_literals_share_one_sum_and_one_count() {
    let checked = checked_session();
    let vanilla = vanilla_session();
    let nullable = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, false),
            Field::new("i", DataType::Int32, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 1, 2])) as ArrayRef,
            Arc::new(Int32Array::from(vec![Some(i32::MAX), None, Some(3), None])),
        ],
    )
    .expect("nullable batch");
    for ctx in [&checked, &vanilla] {
        register_table(ctx, "n", nullable.clone());
    }

    // (query, aggregates it plans, rows, whether `DataFusion` alone plans it)
    for (sql, aggregates, expected, vanilla_plans) in [
        (
            "SELECT SUM(i + 1), SUM(i + 2), SUM(i + 3) FROM t",
            2,
            vec!["4294967300 4294967304 4294967308"],
            true,
        ),
        (
            "SELECT SUM(i), SUM(i + 1), SUM(i + 2), SUM(i + 3) FROM t",
            3,
            vec!["4294967296 4294967300 4294967304 4294967308"],
            true,
        ),
        (
            "SELECT COUNT(i), SUM(i + 1), SUM(i + 2), SUM(i + 3) FROM t",
            3,
            vec!["4 4294967300 4294967304 4294967308"],
            // Checked against these rows only: `DataFusion`'s own rewrite adds
            // a `COUNT(CAST(i AS BIGINT))`, which a column name shows as
            // `COUNT(i)`.
            false,
        ),
        (
            "SELECT g, SUM(i + 1), SUM(1 + i), SUM(i + 10) FROM n GROUP BY g ORDER BY g",
            2,
            vec!["1 2147483652 2147483652 2147483670", "2 NULL NULL NULL"],
            true,
        ),
    ] {
        let plan = optimized_plan(&checked, sql).await;
        let rendered = plan.display_indent().to_string();
        assert_eq!(
            aggregate_widths(&plan),
            vec![aggregates],
            "`{sql}` aggregates:\n{rendered}"
        );
        assert_plan_ran(&rendered, sql, &[CHECKED_SUM_PLUS_COUNT_UDF_NAME]);
        assert!(
            !rendered.contains(CHECKED_ADD_UDF_NAME),
            "`{sql}` adds nothing per row that can overflow:\n{rendered}"
        );
        let contexts = if vanilla_plans {
            vec![&checked, &vanilla]
        } else {
            vec![&checked]
        };
        for ctx in contexts {
            let batches = ctx
                .sql(sql)
                .await
                .unwrap_or_else(|error| panic!("`{sql}` plans: {error}"))
                .collect()
                .await
                .unwrap_or_else(|error| panic!("`{sql}` runs: {error}"));
            assert_eq!(render_rows(&batches), expected, "`{sql}`");
        }
    }
}

/// Two widenings of one column that a column name shows alike (`s` and
/// `CAST(s AS INT)`, both read as BIGINT) get wide aggregates of their own.
#[tokio::test]
async fn sums_over_two_casts_of_one_column_plus_literals_plan() {
    let ctx = checked_session();
    let smallints = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("s", DataType::Int16, true)])),
        vec![Arc::new(Int16Array::from(vec![Some(1), Some(2), None])) as ArrayRef],
    )
    .expect("smallint batch");
    register_table(&ctx, "s16", smallints);
    let sql =
        "SELECT SUM(s + 1), SUM(s + 2), SUM(CAST(s AS INT) + 3), SUM(CAST(s AS INT) + 4) FROM s16";
    let plan = optimized_plan(&ctx, sql).await.display_indent().to_string();
    assert_plan_ran(&plan, sql, &[CHECKED_SUM_PLUS_COUNT_UDF_NAME]);
    let batches = ctx
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("`{sql}` plans: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("`{sql}` runs: {error}"));
    assert_eq!(render_rows(&batches), vec!["5 7 9 11"], "`{sql}`");
}

/// `DISTINCT` and `FILTER` change which rows a `SUM` adds, so a `SUM(x + k)`
/// with either keeps a `SUM` of its own, while plain sums of the same column
/// beside it still share one wide `SUM` and `COUNT`. The rows match plain
/// `DataFusion`'s and the sums worked by hand.
#[tokio::test]
async fn sums_of_a_column_plus_literals_with_distinct_or_filter_keep_their_rows() {
    let checked = checked_session();
    let vanilla = vanilla_session();
    let repeated = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)])),
        vec![Arc::new(Int32Array::from(vec![
            Some(1),
            Some(1),
            Some(2),
            None,
            Some(5),
        ])) as ArrayRef],
    )
    .expect("repeated batch");
    for ctx in [&checked, &vanilla] {
        register_table(ctx, "r", repeated.clone());
    }

    // (query, whether it plans the shared `SUM` and `COUNT`, its row)
    for (sql, shared, expected) in [
        // DISTINCT i + 1 adds 2, 3 and 6; DISTINCT i + 2 adds 3, 4 and 7.
        (
            "SELECT SUM(DISTINCT i + 1), SUM(DISTINCT i + 2) FROM r",
            false,
            "11 14",
        ),
        // The filter keeps i = 2 and i = 5.
        (
            "SELECT SUM(i + 1) FILTER (WHERE i > 1), SUM(i + 2) FILTER (WHERE i > 1) FROM r",
            false,
            "9 11",
        ),
        // i + 2 over 1, 1, 2, 5 adds 17; i + 3 adds 21.
        (
            "SELECT SUM(i + 1) FILTER (WHERE i > 1), SUM(i + 2), SUM(i + 3) FROM r",
            true,
            "9 17 21",
        ),
    ] {
        let plan = optimized_plan(&checked, sql)
            .await
            .display_indent()
            .to_string();
        assert_eq!(
            plan.contains(CHECKED_SUM_PLUS_COUNT_UDF_NAME),
            shared,
            "`{sql}` plans the shared SUM and COUNT: {shared}\n{plan}"
        );
        for ctx in [&checked, &vanilla] {
            let batches = ctx
                .sql(sql)
                .await
                .unwrap_or_else(|error| panic!("`{sql}` plans: {error}"))
                .collect()
                .await
                .unwrap_or_else(|error| panic!("`{sql}` runs: {error}"));
            assert_eq!(render_rows(&batches), vec![expected], "`{sql}`");
        }
    }
}

/// The rewrite's `COUNT` has no `DISTINCT` or `FILTER`, so checked `SUM`
/// declines a `SUM(x + k)` with either, whichever caller offers it.
/// `DataFusion`'s own pass offers neither, so only a direct call reaches
/// this.
#[test]
fn checked_sum_declines_a_distinct_or_filtered_sum_plus_literal() {
    use datafusion::logical_expr::Expr;
    use datafusion::logical_expr::expr::AggregateFunction;
    use datafusion::prelude::{col, lit};

    let sum = checked_sum_udaf();
    let arg = col("i");
    let offer = |distinct: bool, filter: Option<Box<Expr>>| {
        let aggregate = AggregateFunction::new_udf(
            Arc::clone(&sum),
            vec![arg.clone() + lit(1_i64)],
            distinct,
            filter,
            vec![],
            None,
        );
        sum.simplify_expr_op_literal(&aggregate, &arg, Operator::Plus, &lit(1_i64), true)
            .expect("simplify_expr_op_literal")
    };

    let plain = offer(false, None)
        .expect("a plain SUM(i + 1) is rewritten")
        .to_string();
    assert!(
        plain.contains(CHECKED_SUM_PLUS_COUNT_UDF_NAME)
            && plain.contains(CHECKED_SUM_WIDEN_UDF_NAME),
        "a plain SUM(i + 1) becomes the shared SUM and COUNT: {plain}"
    );
    assert_eq!(offer(true, None), None, "SUM(DISTINCT i + 1)");
    assert_eq!(
        offer(false, Some(Box::new(col("i").gt(lit(1_i64))))),
        None,
        "SUM(i + 1) FILTER (WHERE i > 1)"
    );
}

/// The rewrite keeps checked `SUM`'s answer to a total that is not an `Int64`.
#[tokio::test]
async fn sums_of_a_column_plus_literals_error_when_the_total_overflows() {
    let sql = "SELECT SUM(i + 4611686018427387904), SUM(i + 4611686018427387905) FROM t";
    let ctx = checked_session();
    let plan = optimized_plan(&ctx, sql).await.display_indent().to_string();
    assert_plan_ran(&plan, sql, &[CHECKED_SUM_PLUS_COUNT_UDF_NAME]);
    let error = query_error(&ctx, sql).await;
    assert!(
        error.contains("Overflow in SUM of BIGINT: the total is not representable as an Int64"),
        "`{sql}`: {error}"
    );
}

/// A `SUM` of a decimal plus literals is left as written, so its result keeps
/// the type `SUM(d + 1)` alone has; `DataFusion`'s own rewrite widens it.
#[tokio::test]
async fn sums_of_a_decimal_column_plus_literals_keep_their_type() {
    let ctx = checked_session();
    let decimals = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "d",
            DataType::Decimal128(10, 2),
            true,
        )])),
        vec![Arc::new(
            Decimal128Array::from(vec![Some(125), Some(250), None])
                .with_precision_and_scale(10, 2)
                .expect("decimal(10, 2)"),
        ) as ArrayRef],
    )
    .expect("decimal batch");
    register_table(&ctx, "dec", decimals);

    let alone = ctx
        .sql("SELECT SUM(d + 1) FROM dec")
        .await
        .expect("plan one decimal SUM");
    let alone_type = alone.schema().field(0).data_type().clone();
    let sql = "SELECT SUM(d + 1), SUM(d + 2), SUM(d + 3) FROM dec";
    let plan = optimized_plan(&ctx, sql).await;
    assert_eq!(aggregate_widths(&plan), vec![3], "`{sql}`");
    let batches = ctx
        .sql(sql)
        .await
        .expect("plan decimal SUMs")
        .collect()
        .await
        .expect("run decimal SUMs");
    let schema = batches.first().expect("one batch").schema();
    for field in schema.fields() {
        assert_eq!(
            field.data_type(),
            &alone_type,
            "`{sql}` column {}",
            field.name()
        );
    }
    assert_eq!(render_rows(&batches), vec!["5.75 7.75 9.75"]);
}

/// An integer `+` or `-` stays plain only over columns, literals and casts of
/// them that cannot fail and whose values keep the result inside its type;
/// every other one, and every `*`, gets the checked kernel.
#[tokio::test]
async fn only_an_operator_that_can_overflow_gets_the_checked_kernel() {
    let ctx = checked_session();
    for (sql, checked_kernels) in [
        // INT widened to BIGINT, plus or minus a literal, cannot leave BIGINT.
        ("SELECT i + 1, i - 1, 1 - i FROM t", 0),
        // The inner sum ends exactly at i64::MAX and stays plain; an operand
        // that is itself an operator keeps the outer one checked.
        ("SELECT i + 9223372034707292160 + 1 FROM t", 1),
        // A BIGINT column has no room for a literal.
        ("SELECT big + 1 FROM t", 1),
        // Neither has INT plus INT.
        ("SELECT i + CAST(1 AS INT) FROM t", 1),
        // A literal past the room the column leaves.
        ("SELECT i + 9223372036854775807 FROM t", 1),
        // `DataFusion` folds a plain `A * 0` to 0, dropping errors in `A`.
        ("SELECT i * 2, (big + 1) * 0 FROM t", 3),
        // A plain `A + NULL` folds to NULL: an operand that can fail (an
        // overflow, a narrowing cast) keeps the operator checked.
        (
            "SELECT CAST(CAST(big + 1 AS INT) AS BIGINT) + CAST(NULL AS SMALLINT) FROM t",
            2,
        ),
        ("SELECT CAST(i AS SMALLINT) + 1 FROM t", 1),
        // A `TRY_CAST` cannot fail.
        ("SELECT TRY_CAST(i AS BIGINT) + 1 FROM t", 0),
    ] {
        let plan = optimized_plan(&ctx, sql).await.display_indent().to_string();
        let found: usize = [
            CHECKED_ADD_UDF_NAME,
            CHECKED_SUB_UDF_NAME,
            CHECKED_MUL_UDF_NAME,
        ]
        .iter()
        .map(|name| plan.matches(name).count())
        .sum();
        assert_eq!(found, checked_kernels, "`{sql}`:\n{plan}");
    }
}
