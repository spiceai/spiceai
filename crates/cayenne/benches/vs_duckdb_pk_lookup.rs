// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

//! PK equality lookups: Cayenne vs DuckDB.
//!
//! Models the "interactive query" workload — many small queries that
//! resolve a single row by primary key. The PK is `id BIGINT` for both
//! engines (DuckDB declares it as `PRIMARY KEY`, Cayenne tracks it via
//! its `primary_key` table option which routes through the Int64Pk
//! deletion strategy).
//!
//! Three lookup patterns:
//! * `single_pk`     — `WHERE id = ?`. Tight loop, measures per-lookup latency.
//! * `pk_in_list`    — `WHERE id IN (?, ?, ?, ..., ?)`. Batch of 32 keys.
//! * `pk_range`      — `WHERE id BETWEEN ? AND ?`. Range scan of 32 keys.

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]

#[path = "vs_duckdb_helpers/common.rs"]
mod common;

use std::hint::black_box;
use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use tokio::runtime::Runtime;

use common::{
    CayenneFixture, DuckDbFixture, cayenne_insert, cayenne_query, duckdb_insert_parquet,
    duckdb_query_scalar, make_batch, schema, setup_cayenne_pk, setup_duckdb_pk, write_parquet,
};

const TABLE_SIZES: &[usize] = &[16_384, 131_072, 1_048_576];
const BATCH_KEYS: usize = 32;

async fn load_cayenne(rows: usize) -> CayenneFixture {
    let fixture = setup_cayenne_pk("pk_bench").await;
    let batch = make_batch(schema(), 0, rows);
    let _ = cayenne_insert(&fixture.table, batch).await;
    fixture
}

fn load_duckdb(rows: usize, parquet_path: &std::path::Path) -> DuckDbFixture {
    let fixture = setup_duckdb_pk("pk_bench");
    let _ = rows;
    duckdb_insert_parquet(&fixture.conn, "pk_bench", parquet_path);
    fixture
}

fn bench_pk_lookup(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("vs_duckdb_pk_lookup");
    group.sample_size(20);

    let parquet_dir = tempfile::tempdir().expect("parquet dir");

    for &rows in TABLE_SIZES {
        let parquet_path = parquet_dir.path().join(format!("rows_{rows}.parquet"));
        let batch = make_batch(schema(), 0, rows);
        write_parquet(&batch, &parquet_path);

        let cayenne_fixture = Arc::new(rt.block_on(load_cayenne(rows)));
        let duckdb_fixture = Arc::new(load_duckdb(rows, &parquet_path));

        // Pick a stable key in the middle so neither engine's caching is
        // accidentally over-counted at edges.
        let target_id = (rows / 2) as i64;
        let target_lo = target_id - (BATCH_KEYS as i64) / 2;
        let target_hi = target_id + (BATCH_KEYS as i64) / 2;

        // --- single PK lookup ---
        let cayenne_sql = format!("SELECT value FROM t WHERE id = {target_id}");
        let duckdb_sql = format!("SELECT value FROM pk_bench WHERE id = {target_id}");
        let cf = Arc::clone(&cayenne_fixture);
        let s = cayenne_sql.clone();
        group.bench_with_input(
            BenchmarkId::new("cayenne/single_pk", rows),
            &rows,
            |b, &_rows| {
                b.iter(|| {
                    rt.block_on(async {
                        let batches = cayenne_query(&cf.table, &s).await;
                        black_box(batches);
                    });
                });
            },
        );
        let df = Arc::clone(&duckdb_fixture);
        let s = duckdb_sql.clone();
        group.bench_with_input(
            BenchmarkId::new("duckdb/single_pk", rows),
            &rows,
            |b, &_rows| {
                b.iter(|| {
                    let v = duckdb_query_scalar(&df.conn, &s);
                    black_box(v);
                });
            },
        );

        // --- IN-list lookup ---
        let ids: Vec<String> = (target_lo..target_hi).map(|i| i.to_string()).collect();
        let in_list = ids.join(",");
        let cayenne_sql = format!("SELECT SUM(value) FROM t WHERE id IN ({in_list})");
        let duckdb_sql = format!("SELECT SUM(value) FROM pk_bench WHERE id IN ({in_list})");

        let cf = Arc::clone(&cayenne_fixture);
        let s = cayenne_sql.clone();
        group.bench_with_input(
            BenchmarkId::new("cayenne/pk_in_list", rows),
            &rows,
            |b, &_rows| {
                b.iter(|| {
                    rt.block_on(async {
                        let batches = cayenne_query(&cf.table, &s).await;
                        black_box(batches);
                    });
                });
            },
        );
        let df = Arc::clone(&duckdb_fixture);
        let s = duckdb_sql.clone();
        group.bench_with_input(
            BenchmarkId::new("duckdb/pk_in_list", rows),
            &rows,
            |b, &_rows| {
                b.iter(|| {
                    let v = duckdb_query_scalar(&df.conn, &s);
                    black_box(v);
                });
            },
        );

        // --- PK range scan ---
        let cayenne_sql = format!(
            "SELECT SUM(value) FROM t WHERE id BETWEEN {target_lo} AND {target_hi}"
        );
        let duckdb_sql = format!(
            "SELECT SUM(value) FROM pk_bench WHERE id BETWEEN {target_lo} AND {target_hi}"
        );

        let cf = Arc::clone(&cayenne_fixture);
        let s = cayenne_sql.clone();
        group.bench_with_input(
            BenchmarkId::new("cayenne/pk_range", rows),
            &rows,
            |b, &_rows| {
                b.iter(|| {
                    rt.block_on(async {
                        let batches = cayenne_query(&cf.table, &s).await;
                        black_box(batches);
                    });
                });
            },
        );
        let df = Arc::clone(&duckdb_fixture);
        let s = duckdb_sql.clone();
        group.bench_with_input(
            BenchmarkId::new("duckdb/pk_range", rows),
            &rows,
            |b, &_rows| {
                b.iter(|| {
                    let v = duckdb_query_scalar(&df.conn, &s);
                    black_box(v);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_pk_lookup);
criterion_main!(benches);
