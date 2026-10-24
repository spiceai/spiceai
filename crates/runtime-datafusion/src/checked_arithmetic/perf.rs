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

//! P99 timings for non-overflowing `SUM` on vanilla vs checked sessions.
//!
//! TPC-H q1 sums decimals, so the integer accumulator is not on that path.
//! This harness times a q1-shaped decimal `SUM` and a `BIGINT` `SUM` of the
//! same cardinality. Run in release:
//! `cargo test -p runtime-datafusion --release --lib checked_sum_p99 -- --ignored --nocapture`

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Decimal128Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;

use crate::analyzer_rule::AnalyzerRulesBuilder;
use crate::checked_arithmetic::{CheckedIntegerOverflow, register_checked_arithmetic};
use crate::session_config::get_df_default_config;

const ROWS: usize = 100_000;
const SAMPLES: usize = 1_000;
const GROUPS: usize = 10_000;

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
    SessionContext::new_with_state(state)
}

fn vanilla_session() -> SessionContext {
    SessionContext::new_with_config(get_df_default_config())
}

fn register_sum_tables(ctx: &SessionContext) {
    let big: ArrayRef = Arc::new(Int64Array::from_iter_values(
        (0..ROWS).map(|i| i64::try_from(i).expect("row fits i64")),
    ));
    let groups: ArrayRef = Arc::new(Int64Array::from_iter_values(
        (0..ROWS).map(|i| i64::try_from(i % GROUPS).expect("group fits i64")),
    ));
    let qty: ArrayRef = Arc::new(
        Decimal128Array::from_iter_values(
            (0..ROWS).map(|i| i128::try_from(i % 50 + 1).expect("quantity digit fits i128")),
        )
        .with_precision_and_scale(15, 2)
        .expect("q1-like quantity decimal"),
    );
    let price: ArrayRef = Arc::new(
        Decimal128Array::from_iter_values(
            (0..ROWS).map(|i| i128::try_from(i % 1000 + 1).expect("price digit fits i128") * 100),
        )
        .with_precision_and_scale(15, 2)
        .expect("q1-like price decimal"),
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("big", DataType::Int64, false),
            Field::new("g", DataType::Int64, false),
            Field::new("l_quantity", DataType::Decimal128(15, 2), false),
            Field::new("l_extendedprice", DataType::Decimal128(15, 2), false),
        ])),
        vec![big, groups, qty, price],
    )
    .expect("perf batch");
    let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).expect("perf table");
    ctx.register_table("lineitem", Arc::new(table))
        .expect("register lineitem");
}

fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

async fn collect_timed(ctx: &SessionContext, sql: &str, expect_rows: usize) -> u128 {
    let start = Instant::now();
    let batches = ctx
        .sql(sql)
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");
    let elapsed = start.elapsed().as_nanos();
    assert_eq!(
        row_count(&batches),
        expect_rows,
        "`{sql}` must return {expect_rows} rows"
    );
    elapsed
}

async fn time_sql(ctx: &SessionContext, sql: &str, expect_rows: usize) -> Vec<u128> {
    // Warm the plan cache and page cache.
    let _ = collect_timed(ctx, sql, expect_rows).await;
    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        samples.push(collect_timed(ctx, sql, expect_rows).await);
    }
    samples.sort_unstable();
    samples
}

/// Interleave A, B, B, A, … so a noisy first arm does not own the tail.
async fn time_sql_interleaved(
    first: &SessionContext,
    second: &SessionContext,
    sql: &str,
    expect_rows: usize,
) -> (Vec<u128>, Vec<u128>) {
    let _ = collect_timed(first, sql, expect_rows).await;
    let _ = collect_timed(second, sql, expect_rows).await;
    let mut first_samples = Vec::with_capacity(SAMPLES);
    let mut second_samples = Vec::with_capacity(SAMPLES);
    for i in 0..SAMPLES {
        if i.is_multiple_of(2) {
            first_samples.push(collect_timed(first, sql, expect_rows).await);
            second_samples.push(collect_timed(second, sql, expect_rows).await);
        } else {
            second_samples.push(collect_timed(second, sql, expect_rows).await);
            first_samples.push(collect_timed(first, sql, expect_rows).await);
        }
    }
    first_samples.sort_unstable();
    second_samples.sort_unstable();
    (first_samples, second_samples)
}

fn percentile(sorted: &[u128], numer: usize, denom: usize) -> u128 {
    let last = sorted.len().saturating_sub(1);
    let rank = last.saturating_mul(numer) / denom;
    sorted[rank]
}

fn report(label: &str, samples: &[u128]) {
    let p99 = percentile(samples, 99, 100);
    let p999 = percentile(samples, 999, 1000);
    println!(
        "{label}: n={} p99_ns={} p99.9_ns={} max_ns={}",
        samples.len(),
        p99,
        p999,
        samples.last().copied().expect("samples")
    );
}

#[tokio::test]
#[ignore = "timing harness; run with --ignored --nocapture in release"]
async fn checked_sum_p99_int64_and_q1_shaped_decimal() {
    let vanilla = vanilla_session();
    let checked = checked_session();
    register_sum_tables(&vanilla);
    register_sum_tables(&checked);

    let int64_sql = "SELECT SUM(big) FROM lineitem";
    let q1_sql = "SELECT SUM(l_quantity), SUM(l_extendedprice) FROM lineitem";
    let grouped_sql = "SELECT g, SUM(big) FROM lineitem GROUP BY g";

    let vanilla_i64 = time_sql(&vanilla, int64_sql, 1).await;
    let checked_i64 = time_sql(&checked, int64_sql, 1).await;
    let vanilla_q1 = time_sql(&vanilla, q1_sql, 1).await;
    let checked_q1 = time_sql(&checked, q1_sql, 1).await;
    let (vanilla_grp, checked_grp) =
        time_sql_interleaved(&vanilla, &checked, grouped_sql, GROUPS).await;

    println!("rows={ROWS} samples={SAMPLES} groups={GROUPS}");
    report("vanilla Int64 SUM", &vanilla_i64);
    report("checked Int64 SUM", &checked_i64);
    report("vanilla q1-shaped decimal SUM", &vanilla_q1);
    report("checked q1-shaped decimal SUM", &checked_q1);
    report("vanilla grouped Int64 SUM", &vanilla_grp);
    report("checked grouped Int64 SUM", &checked_grp);
}
