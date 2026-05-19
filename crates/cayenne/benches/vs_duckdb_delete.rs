// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

//! Delete + re-scan throughput: Cayenne vs DuckDB.
//!
//! Cayenne's deletion-vector path is one of its biggest architectural
//! wins over DuckDB — DuckDB rewrites the affected blocks; Cayenne
//! writes an Arrow IPC deletion vector and applies it transparently at
//! read time. This bench measures the delta on the two halves of that
//! tradeoff:
//!
//! 1. `delete`            — wall time to execute a `DELETE FROM t WHERE …`
//!                          touching ~10% of rows.
//! 2. `scan_after_delete` — full-table `SELECT SUM(value) FROM t` immediately
//!                          after the delete, exercising the read-time
//!                          deletion-vector filter on Cayenne and DuckDB's
//!                          rewritten blocks.

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]

#[path = "vs_duckdb_helpers/common.rs"]
mod common;

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::UInt64Array;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::catalog::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion_expr::{col, lit};
use tokio::runtime::Runtime;

use common::{
    CayenneFixture, DuckDbFixture, capture_comparison_plans, cayenne_insert, cayenne_query,
    duckdb_insert_parquet, duckdb_query_scalar, make_batch, schema, setup_cayenne_pk,
    setup_duckdb_pk, write_parquet,
};

const TABLE_SIZES: &[usize] = &[16_384, 131_072, 1_048_576];

async fn cayenne_delete_range(fixture: &CayenneFixture, lo: i64, hi: i64) -> u64 {
    let ctx = SessionContext::new();
    let filter = col("id").gt_eq(lit(lo)).and(col("id").lt_eq(lit(hi)));
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

fn duckdb_delete_range(fixture: &DuckDbFixture, table: &str, lo: i64, hi: i64) {
    fixture
        .conn
        .execute_batch(&format!(
            "DELETE FROM {table} WHERE id BETWEEN {lo} AND {hi};"
        ))
        .expect("duckdb delete");
}

async fn load_cayenne(rows: usize) -> CayenneFixture {
    let fixture = setup_cayenne_pk("del_bench").await;
    let batch = make_batch(schema(), 0, rows);
    let _ = cayenne_insert(&fixture.table, batch).await;
    fixture
}

fn load_duckdb(parquet_path: &std::path::Path) -> DuckDbFixture {
    let fixture = setup_duckdb_pk("del_bench");
    duckdb_insert_parquet(&fixture.conn, "del_bench", parquet_path);
    fixture
}

fn bench_delete(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("vs_duckdb_delete");
    group.sample_size(10);

    let parquet_dir = tempfile::tempdir().expect("parquet dir");

    for &rows in TABLE_SIZES {
        group.throughput(Throughput::Elements(rows as u64));

        let parquet_path = parquet_dir.path().join(format!("rows_{rows}.parquet"));
        let batch = make_batch(schema(), 0, rows);
        write_parquet(&batch, &parquet_path);

        // Delete the middle ~10% of rows; both engines see the same range.
        let lo = (rows as i64) * 45 / 100;
        let hi = (rows as i64) * 55 / 100;
        let cayenne_delete_sql = format!("DELETE FROM t WHERE id BETWEEN {lo} AND {hi}");
        let duckdb_delete_sql = format!("DELETE FROM del_bench WHERE id BETWEEN {lo} AND {hi}");

        let plan_cayenne_fixture = rt.block_on(load_cayenne(rows));
        let plan_duckdb_fixture = load_duckdb(&parquet_path);
        rt.block_on(capture_comparison_plans(
            &format!("delete/{rows}/delete"),
            &plan_cayenne_fixture.table,
            &plan_duckdb_fixture.conn,
            &cayenne_delete_sql,
            &duckdb_delete_sql,
        ));

        // --- delete (timed; setup is re-run per iteration to keep state clean) ---
        group.bench_with_input(BenchmarkId::new("cayenne/delete", rows), &rows, |b, &_| {
            b.iter_batched(
                || rt.block_on(load_cayenne(rows)),
                |fixture| {
                    rt.block_on(async {
                        let deleted = cayenne_delete_range(&fixture, lo, hi).await;
                        black_box((fixture, deleted));
                    });
                },
                BatchSize::PerIteration,
            );
        });
        let path = parquet_path.clone();
        group.bench_with_input(BenchmarkId::new("duckdb/delete", rows), &rows, |b, &_| {
            b.iter_batched(
                || load_duckdb(&path),
                |fixture| {
                    duckdb_delete_range(&fixture, "del_bench", lo, hi);
                    black_box(fixture);
                },
                BatchSize::PerIteration,
            );
        });

        // --- scan_after_delete (load + delete once outside the timed region,
        //     then query many times to measure read-time filtering cost) ---
        let cayenne_fixture = Arc::new(rt.block_on(async {
            let fixture = load_cayenne(rows).await;
            let _ = cayenne_delete_range(&fixture, lo, hi).await;
            fixture
        }));
        let duckdb_fixture = Arc::new({
            let fixture = load_duckdb(&parquet_path);
            duckdb_delete_range(&fixture, "del_bench", lo, hi);
            fixture
        });

        rt.block_on(capture_comparison_plans(
            &format!("delete/{rows}/scan_after_delete"),
            &cayenne_fixture.table,
            &duckdb_fixture.conn,
            "SELECT SUM(value) FROM t",
            "SELECT SUM(value) FROM del_bench",
        ));

        let cf = Arc::clone(&cayenne_fixture);
        group.bench_with_input(
            BenchmarkId::new("cayenne/scan_after_delete", rows),
            &rows,
            |b, &_| {
                b.iter(|| {
                    rt.block_on(async {
                        let batches = cayenne_query(&cf.table, "SELECT SUM(value) FROM t").await;
                        black_box(batches);
                    });
                });
            },
        );
        let df = Arc::clone(&duckdb_fixture);
        group.bench_with_input(
            BenchmarkId::new("duckdb/scan_after_delete", rows),
            &rows,
            |b, &_| {
                b.iter(|| {
                    let v = duckdb_query_scalar(&df.conn, "SELECT SUM(value) FROM del_bench");
                    black_box(v);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_delete);
criterion_main!(benches);
