// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

//! Scan + aggregate throughput: Cayenne vs DuckDB.
//!
//! Each table is loaded once with a deterministic dataset (outside the
//! timed region) and then exercised with three representative read
//! shapes:
//! * `count_star`  — `SELECT COUNT(*) FROM t`. Pure cardinality.
//! * `sum_value`   — `SELECT SUM(value) FROM t`. Numeric column scan +
//!                    aggregate; exercises decompression and SIMD paths.
//! * `filter_sum`  — `SELECT SUM(value) FROM t WHERE id BETWEEN ? AND ?`.
//!                    Exercises filter pushdown and predicate evaluation
//!                    in both engines.

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]

#[path = "vs_duckdb_helpers/common.rs"]
mod common;

use std::hint::black_box;
use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::Runtime;

use common::{
    CAYENNE_LANES, CayenneFixture, DuckDbFixture, Metastore, capture_comparison_plans,
    cayenne_insert, cayenne_query, duckdb_insert_parquet, duckdb_query_scalar, make_batch, schema,
    setup_cayenne_for, setup_duckdb, write_parquet,
};

const ROW_COUNTS: &[usize] = &[16_384, 131_072, 1_048_576];

async fn load_cayenne(lane: Metastore, rows: usize) -> CayenneFixture {
    let fixture = setup_cayenne_for("scan_bench", lane).await;
    let batch = make_batch(schema(), 0, rows);
    let _ = cayenne_insert(&fixture.table, batch).await;
    fixture
}

fn load_duckdb(parquet_path: &std::path::Path) -> DuckDbFixture {
    let fixture = setup_duckdb("scan_bench");
    duckdb_insert_parquet(&fixture.conn, "scan_bench", parquet_path);
    fixture
}

fn bench_scan(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("vs_duckdb_scan");
    group.sample_size(10);

    let parquet_dir = tempfile::tempdir().expect("parquet dir");

    for &rows in ROW_COUNTS {
        group.throughput(Throughput::Elements(rows as u64));

        let parquet_path = parquet_dir.path().join(format!("rows_{rows}.parquet"));
        let batch = make_batch(schema(), 0, rows);
        write_parquet(&batch, &parquet_path);

        // Load once, query many times — match the steady-state read pattern.
        let plan_cayenne_fixture = Arc::new(rt.block_on(load_cayenne(Metastore::Sqlite, rows)));
        let duckdb_fixture = Arc::new(load_duckdb(&parquet_path));

        rt.block_on(capture_comparison_plans(
            &format!("scan/{rows}/count_star"),
            &plan_cayenne_fixture.table,
            &duckdb_fixture.conn,
            "SELECT COUNT(*) FROM t",
            "SELECT COUNT(*) FROM scan_bench",
        ));

        rt.block_on(capture_comparison_plans(
            &format!("scan/{rows}/sum_value"),
            &plan_cayenne_fixture.table,
            &duckdb_fixture.conn,
            "SELECT SUM(value) FROM t",
            "SELECT SUM(value) FROM scan_bench",
        ));

        // --- count_star ---
        let mut cayenne_fixtures = Vec::new();
        for &lane in CAYENNE_LANES {
            cayenne_fixtures.push((lane.lane(), Arc::new(rt.block_on(load_cayenne(lane, rows)))));
        }

        for (lane_label, cayenne_fixture) in &cayenne_fixtures {
            let fixture = Arc::clone(cayenne_fixture);
            group.bench_with_input(
                BenchmarkId::new(format!("{lane_label}/count_star"), rows),
                &rows,
                |b, &_rows| {
                    b.iter(|| {
                        rt.block_on(async {
                            let batches =
                                cayenne_query(&fixture.table, "SELECT COUNT(*) FROM t").await;
                            black_box(batches);
                        });
                    });
                },
            );
        }

        let df = Arc::clone(&duckdb_fixture);
        group.bench_with_input(
            BenchmarkId::new("duckdb/count_star", rows),
            &rows,
            |b, &_rows| {
                b.iter(|| {
                    let v = duckdb_query_scalar(&df.conn, "SELECT COUNT(*) FROM scan_bench");
                    black_box(v);
                });
            },
        );

        // --- sum_value ---
        for (lane_label, cayenne_fixture) in &cayenne_fixtures {
            let fixture = Arc::clone(cayenne_fixture);
            group.bench_with_input(
                BenchmarkId::new(format!("{lane_label}/sum_value"), rows),
                &rows,
                |b, &_rows| {
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
            BenchmarkId::new("duckdb/sum_value", rows),
            &rows,
            |b, &_rows| {
                b.iter(|| {
                    let v = duckdb_query_scalar(&df.conn, "SELECT SUM(value) FROM scan_bench");
                    black_box(v);
                });
            },
        );

        // --- filter_sum (selects ~10% of rows in the middle of the range) ---
        let lo = (rows as i64) * 45 / 100;
        let hi = (rows as i64) * 55 / 100;
        let cayenne_sql = format!("SELECT SUM(value) FROM t WHERE id BETWEEN {lo} AND {hi}");
        let duckdb_sql =
            format!("SELECT SUM(value) FROM scan_bench WHERE id BETWEEN {lo} AND {hi}");

        rt.block_on(capture_comparison_plans(
            &format!("scan/{rows}/filter_sum"),
            &plan_cayenne_fixture.table,
            &duckdb_fixture.conn,
            &cayenne_sql,
            &duckdb_sql,
        ));

        for (lane_label, cayenne_fixture) in &cayenne_fixtures {
            let fixture = Arc::clone(cayenne_fixture);
            let cayenne_sql_owned = cayenne_sql.clone();
            group.bench_with_input(
                BenchmarkId::new(format!("{lane_label}/filter_sum"), rows),
                &rows,
                |b, &_rows| {
                    b.iter(|| {
                        rt.block_on(async {
                            let batches = cayenne_query(&fixture.table, &cayenne_sql_owned).await;
                            black_box(batches);
                        });
                    });
                },
            );
        }

        let df = Arc::clone(&duckdb_fixture);
        let duckdb_sql_owned = duckdb_sql.clone();
        group.bench_with_input(
            BenchmarkId::new("duckdb/filter_sum", rows),
            &rows,
            |b, &_rows| {
                b.iter(|| {
                    let v = duckdb_query_scalar(&df.conn, &duckdb_sql_owned);
                    black_box(v);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_scan);
criterion_main!(benches);
