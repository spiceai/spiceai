// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! End-to-end benchmark for boolean `NOT` filter pushdown.
//!
//! A selective `WHERE NOT flag` over a wide table is the target: when the
//! predicate pushes into the Vortex scan, the scan evaluates the boolean
//! during the scan and only materializes the wide payload columns for the
//! surviving rows (late materialization) instead of decoding every column for
//! every row and re-filtering in a `FilterExec` above the scan.
//!
//! `not_flag_boolean` is the query the pushdown fixes. `eq_zero_int_reference`
//! is the same selectivity expressed as an integer equality that always pushed
//! down — it is the floor `not_flag_boolean` should reach once the boolean
//! `NOT` also pushes.

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
/// One row in every `SELECTIVITY_DIVISOR` has `flag = false`, so `NOT flag`
/// (and the integer reference) keep ~6% of rows — selective enough that late
/// materialization of the payload columns is the dominant cost.
const SELECTIVITY_DIVISOR: usize = 16;

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
        // Configure the Vortex session with the current Tokio runtime so the
        // write sink can resolve its executor (see `build_runtime`); `with_tokio`
        // captures `Handle::current()`, so it must run inside `block_on`.
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
                flag BOOLEAN NOT NULL, \
                flag_int BIGINT NOT NULL, \
                p0 BIGINT NOT NULL, p1 BIGINT NOT NULL, p2 BIGINT NOT NULL, \
                p3 BIGINT NOT NULL, p4 BIGINT NOT NULL, p5 BIGINT NOT NULL) \
            STORED AS vortex LOCATION '/wide/'",
        )
        .await
        .expect("create table");

        // `flag` is true for all but every SELECTIVITY_DIVISOR-th row; `flag_int`
        // is 0 exactly when `flag` is false, so the two filters select the same
        // rows. The payload columns give the scan real per-row decode work.
        ctx.sql(&format!(
            "INSERT INTO wide \
             SELECT v AS id, \
                    (v % {div} <> 0) AS flag, \
                    CASE WHEN v % {div} = 0 THEN 0 ELSE 1 END AS flag_int, \
                    v * 2 AS p0, v * 3 AS p1, v * 5 AS p2, \
                    v * 7 AS p3, v * 11 AS p4, v * 13 AS p5 \
             FROM generate_series(1, {rows}) AS t(v)",
            div = SELECTIVITY_DIVISOR,
            rows = ROW_COUNT,
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

fn bench_not_pushdown(c: &mut Criterion) {
    let rt = build_runtime();
    let ctx = build_context(&rt);

    // Materialize all payload columns so late materialization of the wide side
    // is the cost under test, not just the filter column.
    let projection = "id, p0, p1, p2, p3, p4, p5";
    let not_flag = format!("SELECT {projection} FROM wide WHERE NOT flag");
    let eq_zero = format!("SELECT {projection} FROM wide WHERE flag_int = 0");

    // Sanity-check both filters select the same (selective) row set before timing.
    let not_rows = run_query(&rt, &ctx, &not_flag);
    let eq_rows = run_query(&rt, &ctx, &eq_zero);
    assert_eq!(
        not_rows, eq_rows,
        "NOT flag and flag_int = 0 must select the same rows"
    );
    assert!(
        not_rows > 0 && not_rows < ROW_COUNT / 4,
        "filter should be selective, kept {not_rows} of {ROW_COUNT}"
    );

    let mut group = c.benchmark_group("filter_pushdown_not");
    group.bench_function("not_flag_boolean", |b| {
        b.iter(|| run_query(&rt, &ctx, &not_flag));
    });
    group.bench_function("eq_zero_int_reference", |b| {
        b.iter(|| run_query(&rt, &ctx, &eq_zero));
    });
    group.finish();
}

criterion_group!(benches, bench_not_pushdown);
criterion_main!(benches);
