// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

//! Ingest throughput: Cayenne vs DuckDB.
//!
//! Both engines load from the same pre-materialized parquet file
//! (written once outside the timed region) so the measurement is
//! apples-to-apples — both pay parquet decode cost on top of the
//! engine's write path:
//! * `cayenne` — `ctx.read_parquet(...)` → `CayenneTableProvider::insert_into`
//! * `duckdb`  — `INSERT INTO ... SELECT * FROM read_parquet(...)`,
//!               DuckDB's recommended bulk-ingestion path
//!
//! Both engines materialize to local disk (Cayenne to a temp directory,
//! DuckDB to a temp `.duckdb` file) so the comparison is file-mode vs
//! file-mode — see `tools/testoperator/dispatch/perf-cayenne-vs-duckdb/README.md`
//! for why that's the only fair pairing (Cayenne does not support
//! in-memory mode).

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]

#[path = "vs_duckdb_helpers/common.rs"]
mod common;

use std::hint::black_box;
use std::sync::Arc;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::Runtime;

use common::{
    CAYENNE_LANES, Metastore, capture_comparison_plans, capture_parquet_ingest_plans,
    cayenne_insert_from_parquet, duckdb_insert_parquet, make_batch, schema, setup_cayenne_for,
    setup_duckdb, write_parquet,
};

const ROW_COUNTS: &[usize] = &[1_024, 16_384, 131_072];

fn bench_bulk_ingest(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("vs_duckdb_bulk_ingest");
    group.sample_size(10);

    let parquet_dir = tempfile::tempdir().expect("parquet dir");

    for &rows in ROW_COUNTS {
        group.throughput(Throughput::Elements(rows as u64));

        let parquet_path = parquet_dir.path().join(format!("rows_{rows}.parquet"));
        let batch = make_batch(schema(), 0, rows);
        write_parquet(&batch, &parquet_path);

        let ingest_plan_cayenne_fixture =
            rt.block_on(setup_cayenne_for("ingest_bench", Metastore::Sqlite));
        let ingest_plan_duckdb_fixture = setup_duckdb("ingest_bench");
        rt.block_on(capture_parquet_ingest_plans(
            &format!("ingest/{rows}/parquet_insert"),
            &ingest_plan_cayenne_fixture.table,
            &ingest_plan_duckdb_fixture.conn,
            "ingest_bench",
            &parquet_path,
        ));

        let plan_cayenne_fixture =
            rt.block_on(setup_cayenne_for("ingest_bench", Metastore::Sqlite));
        let _ = rt.block_on(cayenne_insert_from_parquet(
            &plan_cayenne_fixture.table,
            &parquet_path,
        ));
        let plan_duckdb_fixture = setup_duckdb("ingest_bench");
        duckdb_insert_parquet(&plan_duckdb_fixture.conn, "ingest_bench", &parquet_path);
        rt.block_on(capture_comparison_plans(
            &format!("ingest/{rows}/post_load_count"),
            &plan_cayenne_fixture.table,
            &plan_duckdb_fixture.conn,
            "SELECT COUNT(*) FROM t",
            "SELECT COUNT(*) FROM ingest_bench",
        ));

        for &lane in CAYENNE_LANES {
            let lane_label = lane.lane();
            let path = parquet_path.clone();
            group.bench_with_input(BenchmarkId::new(lane_label, rows), &rows, |b, &_rows| {
                b.iter_batched(
                    || rt.block_on(setup_cayenne_for("ingest_bench", lane)),
                    |fixture| {
                        rt.block_on(async {
                            let written = cayenne_insert_from_parquet(&fixture.table, &path).await;
                            black_box((fixture, written));
                        });
                    },
                    BatchSize::PerIteration,
                );
            });
        }

        let path = parquet_path.clone();
        group.bench_with_input(BenchmarkId::new("duckdb", rows), &rows, |b, &_rows| {
            b.iter_batched(
                || setup_duckdb("ingest_bench"),
                |fixture| {
                    duckdb_insert_parquet(&fixture.conn, "ingest_bench", &path);
                    black_box(fixture);
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

fn bench_incremental_append(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("vs_duckdb_incremental_append");
    group.sample_size(10);

    // Each batch appended on top of an existing table — simulates the
    // streaming ingestion path where many small batches arrive over time.
    let per_batch_rows: usize = 4_096;
    let batches_count: usize = 16;
    group.throughput(Throughput::Elements(
        (per_batch_rows * batches_count) as u64,
    ));

    let parquet_dir = tempfile::tempdir().expect("parquet dir");
    let mut parquet_paths = Vec::with_capacity(batches_count);
    for i in 0..batches_count {
        let batch = make_batch(schema(), (i * per_batch_rows) as i64, per_batch_rows);
        let path = parquet_dir.path().join(format!("batch_{i}.parquet"));
        write_parquet(&batch, &path);
        parquet_paths.push(path);
    }
    let parquet_paths = Arc::new(parquet_paths);

    let ingest_plan_cayenne_fixture =
        rt.block_on(setup_cayenne_for("incr_bench", Metastore::Sqlite));
    let ingest_plan_duckdb_fixture = setup_duckdb("incr_bench");
    rt.block_on(capture_parquet_ingest_plans(
        "incremental_append/parquet_insert",
        &ingest_plan_cayenne_fixture.table,
        &ingest_plan_duckdb_fixture.conn,
        "incr_bench",
        &parquet_paths[0],
    ));

    let plan_cayenne_fixture = rt.block_on(setup_cayenne_for("incr_bench", Metastore::Sqlite));
    for path in parquet_paths.iter() {
        let _ = rt.block_on(cayenne_insert_from_parquet(
            &plan_cayenne_fixture.table,
            path,
        ));
    }
    let plan_duckdb_fixture = setup_duckdb("incr_bench");
    for path in parquet_paths.iter() {
        duckdb_insert_parquet(&plan_duckdb_fixture.conn, "incr_bench", path);
    }
    rt.block_on(capture_comparison_plans(
        "incremental_append/post_load_count",
        &plan_cayenne_fixture.table,
        &plan_duckdb_fixture.conn,
        "SELECT COUNT(*) FROM t",
        "SELECT COUNT(*) FROM incr_bench",
    ));

    for &lane in CAYENNE_LANES {
        let lane_label = lane.lane();
        let cayenne_paths = Arc::clone(&parquet_paths);
        group.bench_function(lane_label, |b| {
            let paths = Arc::clone(&cayenne_paths);
            b.iter_batched(
                || rt.block_on(setup_cayenne_for("incr_bench", lane)),
                |fixture| {
                    rt.block_on(async {
                        for path in paths.iter() {
                            let _ = cayenne_insert_from_parquet(&fixture.table, path).await;
                        }
                        black_box(fixture);
                    });
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.bench_function("duckdb", |b| {
        let paths = Arc::clone(&parquet_paths);
        b.iter_batched(
            || setup_duckdb("incr_bench"),
            |fixture| {
                for path in paths.iter() {
                    duckdb_insert_parquet(&fixture.conn, "incr_bench", path);
                }
                black_box(fixture);
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_bulk_ingest, bench_incremental_append);
criterion_main!(benches);
