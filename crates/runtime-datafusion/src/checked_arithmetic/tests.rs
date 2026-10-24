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

use arrow::array::{ArrayRef, Int32Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::display::array_value_to_string;
use datafusion::catalog::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::Operator;
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::SessionContext;

use crate::analyzer_rule::AnalyzerRulesBuilder;
use crate::checked_arithmetic::op::checked_op_udf;
use crate::checked_arithmetic::{CheckedIntegerOverflow, register_checked_arithmetic};
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
    let mut analyzer_rules = AnalyzerRulesBuilder::new()
        .include_federation(false)
        .build();
    if !analyzer_rules
        .iter()
        .any(|rule| rule.name() == "checked_integer_overflow")
    {
        analyzer_rules.push(Arc::new(CheckedIntegerOverflow::new()));
    }
    let mut state = SessionStateBuilder::new()
        .with_config(get_df_default_config())
        .with_default_features()
        .with_analyzer_rules(analyzer_rules)
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
