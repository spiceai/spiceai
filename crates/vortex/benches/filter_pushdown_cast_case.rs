// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! End-to-end benchmark for `CAST(CASE ... END)` filter pushdown.
//!
//! A non-elided type-changing cast over a `CASE` — `CAST(CASE ... AS DOUBLE)` —
//! only pushes into the Vortex scan once `is_convertible_expr` recognizes the
//! `CASE` child. When it pushes, the scan evaluates the predicate during the
//! scan and late-materializes the wide payload columns only for surviving rows,
//! instead of decoding every column for every row and re-filtering in a
//! `FilterExec` above the scan.
//!
//! `cast_case_double` is the query the change fixes. `plain_gt_reference` is the
//! same selectivity expressed as a plain comparison that always pushed down — it
//! is the floor `cast_case_double` should reach once the cast-over-`CASE` pushes.

use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::datasource::provider::DefaultTableFactory;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_common::GetExt;
use object_store::memory::InMemory;
use tokio::runtime::Runtime;
use vortex::VortexSessionDefault;
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;
use vortex_datafusion::{VortexFormatFactory, VortexTableOptions};

/// Rows in the benched table. Wide enough that per-row decode dominates and
/// small enough that the bench completes quickly.
const ROW_COUNT: usize = 262_144;

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

        let store = Arc::new(InMemory::new());
        ctx.register_object_store(
            &url::Url::try_from("file://").expect("file:// should parse as a URL"),
            store,
        );

        ctx.sql(
            "CREATE EXTERNAL TABLE wide (\
                id BIGINT NOT NULL, \
                sel BIGINT NOT NULL, \
                p0 BIGINT NOT NULL, p1 BIGINT NOT NULL, p2 BIGINT NOT NULL, \
                p3 BIGINT NOT NULL, p4 BIGINT NOT NULL, p5 BIGINT NOT NULL) \
            STORED AS vortex LOCATION '/wide/'",
        )
        .await
        .expect("create table");

        // `sel` cycles 0..99; both benched filters keep sel in 93..=99 (~7%).
        // The payload columns give the scan real per-row decode work.
        ctx.sql(&format!(
            "INSERT INTO wide \
             SELECT v AS id, v % 100 AS sel, \
                    v * 2 AS p0, v * 3 AS p1, v * 5 AS p2, \
                    v * 7 AS p3, v * 11 AS p4, v * 13 AS p5 \
             FROM generate_series(1, {ROW_COUNT}) AS t(v)",
        ))
        .await
        .expect("insert plan")
        .collect()
        .await
        .expect("insert exec");

        ctx
    })
}

fn run_query(rt: &Runtime, ctx: &SessionContext, sql: &str) -> usize {
    rt.block_on(async {
        let batches = ctx
            .sql(sql)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("exec");
        batches.iter().map(|b| b.num_rows()).sum()
    })
}

fn bench_cast_case_pushdown(c: &mut Criterion) {
    let rt = build_runtime();
    let ctx = build_context(&rt);

    let projection = "id, p0, p1, p2, p3, p4, p5";
    // `CASE WHEN sel > 90 THEN sel ELSE 0 END` cast to f64, keep > 92.0 -> sel in 93..=99.
    let cast_case = format!(
        "SELECT {projection} FROM wide \
         WHERE CAST(CASE WHEN sel > 90 THEN sel ELSE 0 END AS DOUBLE) > 92.0"
    );
    // Same surviving rows via a plain comparison that always pushed.
    let plain_gt = format!("SELECT {projection} FROM wide WHERE sel > 92");

    // Sanity-check both filters select the same (selective) row set before timing.
    let cast_rows = run_query(&rt, &ctx, &cast_case);
    let plain_rows = run_query(&rt, &ctx, &plain_gt);
    assert_eq!(
        cast_rows, plain_rows,
        "CAST(CASE) and plain comparison must select the same rows"
    );
    assert!(
        cast_rows > 0 && cast_rows < ROW_COUNT / 4,
        "filter should be selective, kept {cast_rows} of {ROW_COUNT}"
    );

    let mut group = c.benchmark_group("filter_pushdown_cast_case");
    group.bench_function("cast_case_double", |b| {
        b.iter(|| run_query(&rt, &ctx, &cast_case));
    });
    group.bench_function("plain_gt_reference", |b| {
        b.iter(|| run_query(&rt, &ctx, &plain_gt));
    });
    group.finish();
}

criterion_group!(benches, bench_cast_case_pushdown);
criterion_main!(benches);
