// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

//! Large-IN-list `DELETE`: Cayenne vs DuckDB.
//!
//! This bench exists to expose the **MERGE INTO slow path** on Cayenne PK-based
//! tables. `crates/cayenne/src/ddl/physical_plans.rs:611-628` shows the routing:
//!
//! ```ignore
//! let delete_count = if let Some(count) =
//!     try_key_probe_delete(&target_provider, &target_key_columns, matched_keys).await?
//! {
//!     count
//! } else {
//!     // Legacy path: build filter expression and push through delete_from.
//!     let delete_filter = build_delete_filter(&normalized_batches, &target_key_columns)?;
//!     let delete_plan = target_provider
//!         .delete_from(&session_state, vec![delete_filter])
//!         .await?;
//!     ...
//! };
//! ```
//!
//! `try_key_probe_delete` (`physical_plans.rs:831-875`) only fires for
//! `PositionBased` tables — it explicitly returns `None` whenever the
//! Cayenne provider is `Int64Pk` or `RowConverterBased`. That means MERGE on
//! every PK-keyed table (i.e. every CDC accelerator with `primary_key` set)
//! falls into `build_delete_filter` (`physical_plans.rs:739-823`) which
//! constructs an N-element `IN (val1, val2, …, valN)` filter expression for
//! a single-column key (or an N-row OR-of-ANDs tree for composite keys), then
//! routes through `provider.delete_from(state, vec![delete_filter])`.
//!
//! From the storage layer's perspective `DELETE WHERE id IN (val1..valN)`
//! issued directly is the same code path that MERGE eventually drives on a
//! PK table. So this bench can exhibit the slow path without needing to set
//! up a SQL `MERGE INTO` test rig.
//!
//! ## What this bench measures
//!
//! Two engines, same shape:
//!
//! - **Cayenne**: a `PrimaryKey(id)` table loaded with `rows` rows, then a
//!   single `DELETE FROM t WHERE id IN (10 % of rows)` followed by a
//!   `SELECT SUM(value) FROM t` to amortize the per-scan deletion-vector
//!   probe (iter-3's bitmap→treemap conversion lives on this path too).
//! - **DuckDB**: the same `id BIGINT PRIMARY KEY` table with the same
//!   `DELETE FROM t WHERE id IN (...)` and a follow-up SUM. DuckDB rewrites
//!   the affected blocks; Cayenne writes a deletion vector and applies it
//!   at read time. Both paths are publicly documented; this bench measures
//!   the wall-clock cost of each end-to-end.
//!
//! Three table sizes mirror the existing `vs_duckdb_delete.rs` shape so the
//! results sit alongside it. IN-list cardinality scales with table size:
//! 10 % of rows for the same "delete a chunk" semantics. The legacy filter
//! path's cost is `O(N_match × N_rows)` worst case (per-row evaluation),
//! and `build_delete_filter` allocates ~N_match `ScalarValue::lit(…)` Expr
//! nodes plus a `Vec<Expr>` of the same size — this overhead is visible in
//! the `cayenne/delete_in_list/<rows>` lane.
//!
//! ## How to read
//!
//! `cargo bench --bench vs_duckdb_in_list_delete -p cayenne --features duckdb-bench`.
//!
//! - `cayenne/delete_in_list/<rows>` vs `duckdb/delete_in_list/<rows>` —
//!   per-engine wall time for `DELETE WHERE id IN (...)`. The ratio is the
//!   headroom from extending `try_key_probe_delete` to PK-based tables.
//! - `cayenne/scan_after_in_list_delete/<rows>` vs
//!   `duckdb/scan_after_in_list_delete/<rows>` — full-table SUM immediately
//!   after the delete. Cayenne pays the per-file deletion-vector
//!   bitmap→treemap conversion (iter-3 finding); DuckDB scans the already-
//!   rewritten blocks.

#![cfg(feature = "duckdb-bench")]
#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]

#[path = "vs_duckdb_helpers/common.rs"]
mod common;

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::UInt64Array;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion_expr::{col, lit};
use tokio::runtime::Runtime;

use common::{
    CAYENNE_LANES, CayenneFixture, DuckDbFixture, Metastore, capture_comparison_plans,
    cayenne_insert, cayenne_query, duckdb_insert_parquet, duckdb_query_scalar, make_batch, schema,
    setup_cayenne_pk_for, setup_duckdb_pk, write_parquet,
};

const TABLE_SIZES: &[usize] = &[16_384, 131_072, 1_048_576];

/// Delete 10 % of rows via a single explicit IN-list. The list is built from
/// the lower decile of the id space so the delete touches a clear, easy-to-
/// reason-about block of data on both engines.
fn build_in_list_ids(rows: usize) -> Vec<i64> {
    let count = (rows / 10).max(1);
    (0..count as i64).collect()
}

async fn cayenne_delete_in_list(fixture: &CayenneFixture, ids: &[i64]) -> u64 {
    let ctx = SessionContext::new();
    let id_literals: Vec<datafusion_expr::Expr> = ids.iter().map(|v| lit(*v)).collect();
    let filter = col("id").in_list(id_literals, false);
    let plan = fixture
        .table
        .delete_from(&ctx.state(), vec![filter])
        .await
        .expect("cayenne delete plan");
    let results = datafusion_physical_plan::collect(plan, ctx.task_ctx())
        .await
        .expect("cayenne delete collect");
    results
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<UInt64Array>())
        .and_then(|a| a.values().first())
        .copied()
        .unwrap_or(0)
}

fn duckdb_delete_in_list(fixture: &DuckDbFixture, table: &str, ids: &[i64]) {
    let mut sql = format!("DELETE FROM {table} WHERE id IN (");
    for (i, id) in ids.iter().enumerate() {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&id.to_string());
    }
    sql.push_str(");");
    fixture
        .conn
        .execute_batch(&sql)
        .expect("duckdb delete in list");
}

async fn load_cayenne(lane: Metastore, rows: usize) -> CayenneFixture {
    let fixture = setup_cayenne_pk_for("in_list_del_bench", lane).await;
    let batch = make_batch(schema(), 0, rows);
    let _ = cayenne_insert(&fixture.table, batch).await;
    fixture
}

fn load_duckdb(parquet_path: &std::path::Path) -> DuckDbFixture {
    let fixture = setup_duckdb_pk("in_list_del_bench");
    duckdb_insert_parquet(&fixture.conn, "in_list_del_bench", parquet_path);
    fixture
}

fn bench_in_list_delete(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("vs_duckdb_in_list_delete");
    group.sample_size(10);

    let parquet_dir = tempfile::tempdir().expect("parquet dir");

    for &rows in TABLE_SIZES {
        group.throughput(Throughput::Elements(rows as u64));

        let parquet_path = parquet_dir.path().join(format!("rows_{rows}.parquet"));
        let batch = make_batch(schema(), 0, rows);
        write_parquet(&batch, &parquet_path);

        let ids = build_in_list_ids(rows);

        // --- delete (rebuild fixture on each iteration so deletes don't compound) ---
        for &lane in CAYENNE_LANES {
            let lane_label = lane.lane();
            group.bench_with_input(
                BenchmarkId::new(format!("{lane_label}/delete_in_list"), rows),
                &rows,
                |b, &_| {
                    b.iter_batched(
                        || rt.block_on(load_cayenne(lane, rows)),
                        |fixture| {
                            rt.block_on(async {
                                let deleted = cayenne_delete_in_list(&fixture, &ids).await;
                                black_box(deleted);
                            });
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }

        group.bench_with_input(
            BenchmarkId::new("duckdb/delete_in_list", rows),
            &rows,
            |b, &_| {
                b.iter_batched(
                    || load_duckdb(&parquet_path),
                    |fixture| {
                        duckdb_delete_in_list(&fixture, "in_list_del_bench", &ids);
                        black_box(());
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        // --- scan_after_in_list_delete (load + delete once outside the timed region,
        //     then time only the scan path that probes the deletion vector). ---
        let mut cayenne_fixtures = Vec::new();
        for &lane in CAYENNE_LANES {
            let fixture = Arc::new(rt.block_on(async {
                let fixture = load_cayenne(lane, rows).await;
                let deleted = cayenne_delete_in_list(&fixture, &ids).await;
                assert!(
                    deleted > 0,
                    "expected the IN-list delete to remove some rows"
                );
                fixture
            }));
            cayenne_fixtures.push((lane, lane.lane(), fixture));
        }

        let duckdb_fixture = Arc::new({
            let fx = load_duckdb(&parquet_path);
            duckdb_delete_in_list(&fx, "in_list_del_bench", &ids);
            fx
        });

        rt.block_on(capture_comparison_plans(
            &format!("in_list_delete/{rows}/scan_after_in_list_delete"),
            &cayenne_fixtures
                .iter()
                .find(|(lane, _, _)| *lane == Metastore::Sqlite)
                .expect("sqlite cayenne lane should exist")
                .2
                .table,
            &duckdb_fixture.conn,
            "SELECT SUM(value) FROM t",
            "SELECT SUM(value) FROM in_list_del_bench",
        ));

        for (_, lane_label, cayenne_fixture) in &cayenne_fixtures {
            let fixture = Arc::clone(cayenne_fixture);
            group.bench_with_input(
                BenchmarkId::new(format!("{lane_label}/scan_after_in_list_delete"), rows),
                &rows,
                |b, &_| {
                    b.iter(|| {
                        rt.block_on(async {
                            let batches =
                                cayenne_query(&fixture.table, "SELECT SUM(value) FROM t").await;
                            black_box(batches);
                        });
                    });
                },
            );
        }

        let df = Arc::clone(&duckdb_fixture);
        group.bench_with_input(
            BenchmarkId::new("duckdb/scan_after_in_list_delete", rows),
            &rows,
            |b, &_| {
                b.iter(|| {
                    let v =
                        duckdb_query_scalar(&df.conn, "SELECT SUM(value) FROM in_list_del_bench");
                    black_box(v);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_in_list_delete);
criterion_main!(benches);
