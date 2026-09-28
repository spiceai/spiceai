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

//! `sort_plan` must return exactly its input rows, ordered as one sort of all
//! of them would order them, whatever shape of order its partitions arrive in
//! and however many of them it sorts at once.
#![cfg(feature = "datafusion")]

use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::{ExecutionPlan, displayable, execute_stream};
use datafusion::prelude::SessionConfig;
use futures::TryStreamExt;
use util::stream_utils::{SORT_PARTITION_WORKING_BYTES, max_sort_partitions, sort_plan};

type Row = (Option<i64>, String);

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

/// Deterministic pseudo-random keys (xorshift) with NULLs and many duplicates,
/// so a failure reproduces.
fn scrambled(n: usize, seed: u64) -> Vec<Option<i64>> {
    let mut x = seed | 1;
    (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (!x.is_multiple_of(97)).then(|| i64::try_from(x % 10_000).expect("small key"))
        })
        .collect()
}

/// Every row carries a payload unique to it, so a dropped or duplicated row
/// changes the multiset even when its key repeats.
fn rows(keys: &[Option<i64>], tag: &str) -> Vec<Row> {
    keys.iter()
        .enumerate()
        .map(|(i, k)| (*k, format!("{tag}-{i}")))
        .collect()
}

fn batches(rows: &[Row], batch_rows: usize) -> Vec<RecordBatch> {
    rows.chunks(batch_rows)
        .map(|chunk| {
            RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(Int64Array::from(
                        chunk.iter().map(|r| r.0).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        chunk.iter().map(|r| r.1.clone()).collect::<Vec<_>>(),
                    )),
                ],
            )
            .expect("valid batch")
        })
        .collect()
}

fn source(parts: &[Vec<Row>], batch_rows: usize) -> Arc<dyn ExecutionPlan> {
    let partitions: Vec<Vec<RecordBatch>> = parts.iter().map(|p| batches(p, batch_rows)).collect();
    MemorySourceConfig::try_new_exec(&partitions, schema(), None).expect("memory source")
}

/// Partition shapes: already sorted, several ascending runs, scrambled with
/// NULLs, empty, and a sorted run with a scrambled stretch spliced in.
fn partitions() -> Vec<Vec<Row>> {
    let sorted: Vec<Option<i64>> = (0..6_000).map(Some).chain([None]).collect();
    let runs: Vec<Option<i64>> = (0..5)
        .flat_map(|r| (0..1_500).map(move |i| Some(i * 5 + r)))
        .collect();
    let mut spliced: Vec<Option<i64>> = (0..4_000).map(Some).collect();
    spliced.splice(2_048..2_048, scrambled(700, 7));
    vec![
        rows(&sorted, "sorted"),
        rows(&runs, "runs"),
        rows(&scrambled(9_000, 42), "scrambled"),
        Vec::new(),
        rows(&spliced, "spliced"),
    ]
}

async fn run(plan: Arc<dyn ExecutionPlan>, ctx: &Arc<TaskContext>) -> Vec<Row> {
    let out: Vec<RecordBatch> = execute_stream(plan, Arc::clone(ctx))
        .expect("plan executes")
        .try_collect()
        .await
        .expect("stream succeeds");
    out.iter()
        .flat_map(|b| {
            let k = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 key");
            let p = b
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8 payload");
            (0..b.num_rows())
                .map(|i| (k.is_valid(i).then(|| k.value(i)), p.value(i).to_string()))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Assert `output` is non-decreasing by `rank` and holds exactly the input rows.
fn assert_ordered<R: Ord>(input: &[Vec<Row>], output: &[Row], rank: impl Fn(&Row) -> R) {
    for w in output.windows(2) {
        assert!(
            rank(&w[0]) <= rank(&w[1]),
            "out of order: {:?} then {:?}",
            w[0],
            w[1]
        );
    }
    let mut expected: Vec<Row> = input.iter().flatten().cloned().collect();
    let mut got = output.to_vec();
    expected.sort();
    got.sort();
    assert_eq!(expected.len(), got.len(), "row count changed");
    assert_eq!(expected, got, "rows changed");
}

// Ascending, NULLs last: what a bare column name sorts by.
fn asc_nulls_last(row: &Row) -> (bool, i64) {
    (row.0.is_none(), row.0.unwrap_or(0))
}

async fn sort(parts: &[Vec<Row>], batch_rows: usize, columns: &[&str]) -> Vec<Row> {
    let columns: Vec<String> = columns.iter().map(ToString::to_string).collect();
    let ctx = Arc::new(TaskContext::default());
    let plan = sort_plan(source(parts, batch_rows), &columns, &ctx).expect("plan builds");
    run(plan, &ctx).await
}

fn plan_text(plan: &Arc<dyn ExecutionPlan>) -> String {
    displayable(plan.as_ref()).indent(false).to_string()
}

// The merge spawns one task per partition; a multi-threaded runtime runs them
// in parallel, which is how the compaction rewrite runs it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn orders_every_partition_shape() {
    let parts = partitions();
    for batch_rows in [1, 97, 1_000, 8_192] {
        let out = sort(&parts, batch_rows, &["k"]).await;
        assert_ordered(&parts, &out, asc_nulls_last);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn honors_direction_and_nulls_placement() {
    let parts = partitions();
    let out = sort(&parts, 500, &["k DESC NULLS LAST"]).await;
    assert_ordered(&parts, &out, |r| {
        (r.0.is_none(), std::cmp::Reverse(r.0.unwrap_or(0)))
    });
    let out = sort(&parts, 500, &["k ASC NULLS FIRST"]).await;
    assert_ordered(&parts, &out, |r| (r.0.is_some(), r.0.unwrap_or(0)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn orders_by_every_sort_column() {
    // Keys collide often, so the payload tiebreak decides much of the order.
    let parts = partitions();
    let out = sort(&parts, 700, &["k", "payload DESC"]).await;
    assert_ordered(&parts, &out, |r| {
        (asc_nulls_last(r), std::cmp::Reverse(r.1.clone()))
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_partition_sorts_without_a_merge() {
    let parts = vec![rows(&scrambled(5_000, 3), "only")];
    let ctx = Arc::new(TaskContext::default());
    let plan = sort_plan(source(&parts, 333), &["k".to_string()], &ctx).expect("plan builds");
    assert!(
        !plan_text(&plan).contains("SortPreservingMergeExec"),
        "one partition needs no merge:\n{}",
        plan_text(&plan)
    );
    assert_ordered(&parts, &run(plan, &ctx).await, asc_nulls_last);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_input_yields_no_rows() {
    assert!(sort(&[], 100, &["k"]).await.is_empty());
    assert!(
        sort(&[Vec::new(), Vec::new()], 100, &["k"])
            .await
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unresolvable_or_empty_sort_columns_keep_every_row() {
    // `sort_stream` leaves the rows unsorted for these; so does `sort_plan`,
    // returning its input with every partition intact.
    let parts = partitions();
    for columns in [&[][..], &["missing"][..], &["k SIDEWAYS"][..]] {
        let out = sort(&parts, 500, columns).await;
        assert_ordered(&parts, &out, |_| ());
    }
}

fn bounded_context(pool_bytes: usize, reservation: usize) -> Arc<TaskContext> {
    let mut config = SessionConfig::new();
    config.options_mut().execution.sort_spill_reservation_bytes = reservation;
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(GreedyMemoryPool::new(pool_bytes)))
        .build_arc()
        .expect("runtime");
    Arc::new(
        TaskContext::default()
            .with_session_config(config)
            .with_runtime(runtime),
    )
}

#[test]
fn sort_width_is_what_half_the_pool_gives_each_sort() {
    let reservation = 1024 * 1024;
    let per_sort = reservation + SORT_PARTITION_WORKING_BYTES;
    for (pool, width) in [
        (4 * per_sort + 1, 2),
        (2 * per_sort, 1),
        // A pool too small for even one sort still sorts, serially.
        (per_sort / 3, 1),
        (40 * per_sort, 20),
    ] {
        assert_eq!(
            max_sort_partitions(&bounded_context(pool, reservation)),
            width,
            "pool {pool}"
        );
    }
    assert_eq!(max_sort_partitions(&TaskContext::default()), usize::MAX);
}

/// Memory another consumer already holds is not free for these sorts: a pool
/// with room for every partition's sort when empty sorts them as one once
/// most of it is taken.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_already_held_counts_against_the_sort_width() {
    use datafusion::execution::memory_pool::MemoryConsumer;
    let reservation = 1024 * 1024;
    let per_sort = reservation + SORT_PARTITION_WORKING_BYTES;
    let parts = partitions();
    let ctx = bounded_context(2 * parts.len() * per_sort, reservation);
    assert_eq!(max_sort_partitions(&ctx), parts.len());
    let held = MemoryConsumer::new("concurrent rewrite").register(ctx.memory_pool());
    held.try_grow((2 * parts.len() - 2) * per_sort)
        .expect("room to hold");
    assert_eq!(max_sort_partitions(&ctx), 1);
    let plan = sort_plan(source(&parts, 250), &["k".to_string()], &ctx).expect("plan builds");
    assert!(
        !plan_text(&plan).contains("SortPreservingMergeExec"),
        "with most of the pool held elsewhere, the partitions sort as one:\n{}",
        plan_text(&plan)
    );
    drop(held);
    assert_eq!(
        max_sort_partitions(&ctx),
        parts.len(),
        "released memory is free again"
    );
}

/// A pool with room for a sort per partition sorts them separately and merges.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_roomy_pool_sorts_every_partition_separately() {
    let reservation = 1024 * 1024;
    let parts = partitions();
    let ctx = bounded_context(
        2 * parts.len() * (reservation + SORT_PARTITION_WORKING_BYTES),
        reservation,
    );
    let plan = sort_plan(source(&parts, 250), &["k".to_string()], &ctx).expect("plan builds");
    let text = plan_text(&plan);
    assert!(
        text.contains("SortPreservingMergeExec") && !text.contains("CoalescePartitionsExec"),
        "every partition fits, so each is sorted and they are merged:\n{text}"
    );
    assert_ordered(&parts, &run(plan, &ctx).await, asc_nulls_last);
}

/// A pool without room for a sort per partition sorts them as one, over the
/// coalesced input — never a subset of sorts, and never a repartition, which
/// can deadlock a spilling sort.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tight_pool_sorts_all_partitions_as_one() {
    let reservation = 1024 * 1024;
    let ctx = bounded_context(
        4 * (reservation + SORT_PARTITION_WORKING_BYTES) + 1,
        reservation,
    );
    assert_eq!(max_sort_partitions(&ctx), 2);
    let parts = partitions();
    let plan = sort_plan(source(&parts, 250), &["k".to_string()], &ctx).expect("plan builds");
    let text = plan_text(&plan);
    assert!(
        text.contains("CoalescePartitionsExec")
            && !text.contains("SortPreservingMergeExec")
            && !text.contains("RepartitionExec"),
        "five partitions do not fit two sorts, so they are sorted as one:\n{text}"
    );
    assert_ordered(&parts, &run(plan, &ctx).await, asc_nulls_last);
}

/// Under a pool several times smaller than its input, `sort_plan` must still
/// finish — spilling — and order every row. Regression guard: a plan that
/// round-robined the partitions down in front of a spilling sort deadlocked
/// here, and separate sorts sharing the small pool failed with "Not enough
/// memory to continue external sort".
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn finishes_under_memory_pressure() {
    let payload = "x".repeat(200);
    let parts: Vec<Vec<Row>> = (0..18_u64)
        .map(|p| {
            scrambled(40_000, p + 11)
                .into_iter()
                .enumerate()
                .map(|(i, k)| (k, format!("{payload}-{p}-{i}")))
                .collect()
        })
        .collect();
    // ~170 MB of rows against a 32 MB pool.
    let ctx = bounded_context(32 * 1024 * 1024, 1024 * 1024);
    let plan = sort_plan(source(&parts, 4_096), &["k".to_string()], &ctx).expect("plan builds");
    let out = tokio::time::timeout(std::time::Duration::from_mins(2), run(plan, &ctx))
        .await
        .expect("a spilling sort under memory pressure must finish, not hang");
    assert_ordered(&parts, &out, asc_nulls_last);
}

/// Partitions that already arrive in the requested order are merged, not
/// sorted again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn already_ordered_partitions_are_only_merged() {
    let parts: Vec<Vec<Row>> = (0..4_i64)
        .map(|p| {
            let keys: Vec<Option<i64>> = (0..2_000).map(|i| Some(i * 4 + p)).collect();
            rows(&keys, &format!("p{p}"))
        })
        .collect();
    let partitions: Vec<Vec<RecordBatch>> = parts.iter().map(|p| batches(p, 300)).collect();
    let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(
        col("k", &schema()).expect("k exists"),
    )])
    .expect("non-empty ordering");
    let input = MemorySourceConfig::try_new(&partitions, schema(), None)
        .and_then(|s| s.try_with_sort_information(vec![ordering]))
        .map(datafusion::datasource::source::DataSourceExec::from_data_source)
        .expect("ordered memory source");
    let ctx = Arc::new(TaskContext::default());
    // A bare column name sorts NULLs last; the source's default ordering is
    // NULLs first. The keys here have no NULLs, but the orderings differ, so
    // ask for the source's own ordering to exercise the no-sort path.
    let plan = sort_plan(input, &["k ASC NULLS FIRST".to_string()], &ctx).expect("plan builds");
    let text = plan_text(&plan);
    assert!(
        text.contains("SortPreservingMergeExec") && !text.contains("SortExec:"),
        "ordered partitions need a merge and no sort:\n{text}"
    );
    assert_ordered(&parts, &run(plan, &ctx).await, asc_nulls_last);
}
