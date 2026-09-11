// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! What zone pruning an `IN (<list>)` filter actually buys, by list shape.
//!
//! The statistics predicate a `list_contains` falsifier emits decides which
//! zones the scan can skip without reading them. Describing the list as
//! intervals — its outer range, plus the gaps between adjacent sorted values —
//! is far cheaper to derive than a term per element, but it proves a zone empty
//! only when the zone lies wholly outside the range or wholly inside one gap.
//! The shapes below are chosen to span what that costs:
//!
//! - `clustered` — the list occupies a narrow band of the key range, so the
//!   outer range alone excludes every zone outside the band.
//! - `two_bands` — the list holds keys only at the very bottom and the very top
//!   of the range. Its outer range spans the whole table and excludes nothing,
//!   so every zone in between is proved empty by the one wide interior gap or
//!   not at all. This is the shape the gaps exist for, and the reason to measure
//!   rather than assume. It is 2048 keys rather than 2, because `DataFusion`
//!   rewrites a short `IN` list into a disjunction of equalities, which never
//!   reaches this path.
//! - `scattered` — the list is spread across the whole key range, which is the
//!   shape a hash-join build side produces. Every gap is narrower than a zone,
//!   so nothing prunes however many are emitted.
//!
//! `id` is written in ascending order so zones carry tight, disjoint min/max
//! ranges and pruning has something to work with, and the projection is wide so
//! the cost of *not* pruning a zone is decoding it — which makes the timing a
//! reading of how many zones the scan touched, since the scan exposes no
//! zones-read counter. The row counts and the payload sum are asserted, so a
//! "faster" run that dropped rows fails instead of looking good.

use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::datasource::provider::DefaultTableFactory;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_common::GetExt;
use itertools::Itertools;
use object_store::memory::InMemory;
use tokio::runtime::Runtime;
use vortex::VortexSessionDefault;
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;
use vortex_datafusion::{VortexFormatFactory, VortexTableOptions};

const ROW_COUNT: usize = 1_048_576;

/// Vortex's write path resolves its executor from the ambient Tokio runtime
/// unless the session is explicitly configured with one; a current-thread
/// runtime satisfies that (a multi-thread `Runtime::new()` does not).
fn build_runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread tokio runtime")
}

fn build_context(rt: &Runtime) -> SessionContext {
    rt.block_on(async {
        // `with_tokio` captures `Handle::current()`, so it must run inside `block_on`.
        let session = VortexSession::default().with_tokio();
        let factory = Arc::new(VortexFormatFactory::new_with_options(
            session,
            VortexTableOptions::default(),
        ));
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_table_factory(
                factory.get_ext().to_uppercase(),
                Arc::new(DefaultTableFactory::new()),
            )
            .with_file_formats(vec![factory])
            .build();
        let ctx = SessionContext::new_with_state(state).enable_url_table();
        ctx.register_object_store(
            &url::Url::try_from("file://").expect("file:// should parse as a URL"),
            Arc::new(InMemory::new()),
        );

        ctx.sql(
            "CREATE EXTERNAL TABLE sorted (\
                id BIGINT NOT NULL, \
                p0 BIGINT NOT NULL, p1 BIGINT NOT NULL, p2 BIGINT NOT NULL, \
                p3 BIGINT NOT NULL, p4 BIGINT NOT NULL, p5 BIGINT NOT NULL) \
             STORED AS vortex LOCATION '/sorted/'",
        )
        .await
        .expect("create table");
        // Ascending `id`, so each zone covers a narrow, disjoint key range.
        // The payload columns are what a pruned zone avoids decoding.
        ctx.sql(&format!(
            "INSERT INTO sorted SELECT v AS id, \
                    v * 7 AS p0, v * 11 AS p1, v * 13 AS p2, \
                    v * 17 AS p3, v * 19 AS p4, v * 23 AS p5 \
             FROM generate_series(0, {last}) AS t(v)",
            last = ROW_COUNT - 1,
        ))
        .await
        .expect("insert plan")
        .collect()
        .await
        .expect("insert exec");

        ctx
    })
}

/// The three list shapes, as (name, values).
fn shapes() -> Vec<(&'static str, Vec<usize>)> {
    let last = ROW_COUNT - 1;
    vec![
        // 2048 consecutive keys in the middle of the range.
        ("clustered", (0..2048).map(|i| ROW_COUNT / 2 + i).collect()),
        // 1024 keys at the bottom of the range and 1024 at the top, leaving
        // the whole middle of the table outside the list but inside its bounds.
        (
            "two_bands",
            (0..1024)
                .chain((last - 1023)..=last)
                .collect(),
        ),
        // 2048 keys spread evenly over the whole range.
        ("scattered", (0..2048).map(|i| i * (ROW_COUNT / 2048)).collect()),
    ]
}

fn query(values: &[usize]) -> String {
    format!(
        "SELECT count(*), sum(p0 + p1 + p2 + p3 + p4 + p5) FROM sorted WHERE id IN ({})",
        values.iter().join(", ")
    )
}

/// (matched rows, payload sum) — both asserted, so pruning that skipped a zone
/// it should have read shows up as a wrong answer rather than a fast one.
fn run_query(rt: &Runtime, ctx: &SessionContext, sql: &str) -> (i64, i64) {
    rt.block_on(async {
        let batches = ctx
            .sql(sql)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("exec");
        let column = |idx: usize| -> i64 {
            batches
                .iter()
                .flat_map(|b| {
                    b.column(idx)
                        .as_any()
                        .downcast_ref::<datafusion::arrow::array::Int64Array>()
                        .expect("aggregate is Int64")
                        .iter()
                        .flatten()
                        .collect_vec()
                })
                .sum()
        };
        (column(0), column(1))
    })
}

/// Prints the scan's own metrics for one query, so how much the scan actually
/// read is on the record next to the timing rather than inferred from it.
fn report_scan_metrics(rt: &Runtime, ctx: &SessionContext, name: &str, sql: &str) {
    let plan = rt.block_on(async {
        let batches = ctx
            .sql(&format!("EXPLAIN ANALYZE {sql}"))
            .await
            .expect("plan explain analyze")
            .collect()
            .await
            .expect("exec explain analyze");
        datafusion::arrow::util::pretty::pretty_format_batches(&batches)
            .expect("format explain")
            .to_string()
    });
    for line in plan.lines().filter(|l| l.contains("DataSourceExec")) {
        // The predicate itself is one literal per list element; only the
        // metrics say how much the scan read.
        let metrics = line
            .split_once("metrics=[")
            .map_or("<no metrics>", |(_, tail)| tail);
        eprintln!("[{name}] metrics=[{}", metrics.trim());
    }
}

fn bench_pruning_power(c: &mut Criterion) {
    let rt = build_runtime();
    let ctx = build_context(&rt);

    let mut group = c.benchmark_group("in_list_pruning_power");
    group.sample_size(20);

    for (name, values) in shapes() {
        let sql = query(&values);
        let expected_sum: i64 = values
            .iter()
            .map(|&v| (v as i64) * (7 + 11 + 13 + 17 + 19 + 23))
            .sum();
        assert_eq!(
            run_query(&rt, &ctx, &sql),
            (values.len() as i64, expected_sum),
            "{name} must match one row per list value and read their payloads"
        );
        report_scan_metrics(&rt, &ctx, name, &sql);
        group.bench_function(name, |b| b.iter(|| run_query(&rt, &ctx, &sql)));
    }
    group.finish();
}

criterion_group!(benches, bench_pruning_power);
criterion_main!(benches);
