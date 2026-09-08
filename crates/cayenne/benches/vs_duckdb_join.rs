// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

//! Two-table inner join: Cayenne vs DuckDB.
//!
//! Fact table `t` (default schema: id, name, value) joined against a small
//! "dim" table `d` (id, region). Two query shapes are measured per row count:
//!
//! - `join_agg`: aggregate (`SUM(t.value)`) grouped by the dim's `region`
//!   — exercises the hash join + aggregate kernels end-to-end.
//! - `join_filter`: filter `WHERE d.region = 'NA'` before aggregating —
//!   exercises join-side pushdown (the optimizer should restrict the build
//!   side to one region before probing).
//!
//! Lanes (compile-time gated):
//! - `cayenne`       — Cayenne with the SQLite metastore (default)
//! - `cayenne_turso` — Cayenne with the Turso metastore (--features turso)
//! - `duckdb`        — DuckDB file-mode

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_possible_truncation)]

#[path = "vs_duckdb_helpers/common.rs"]
mod common;

use std::hint::black_box;
use std::path::Path;
use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::Runtime;

use common::{
    CAYENNE_LANES, CayenneFixture, DuckDbFixture, Metastore, cayenne_insert, cayenne_query_join,
    dim_schema, duckdb_insert_dim_rows, duckdb_insert_parquet, duckdb_query_count, make_batch,
    make_dim_batch, schema, setup_cayenne_dim_for, setup_cayenne_for, setup_duckdb_with_dim,
    write_parquet,
};

const FACT_ROWS: &[usize] = &[16_384, 131_072];
const DIM_ROWS: usize = 256;

struct CayenneJoinFixture {
    fact: CayenneFixture,
    dim: CayenneFixture,
}

async fn load_cayenne(lane: Metastore, fact_rows: usize) -> CayenneJoinFixture {
    let fact = setup_cayenne_for("join_fact_bench", lane).await;
    let _ = cayenne_insert(&fact.table, make_batch(schema(), 0, fact_rows)).await;

    let dim = setup_cayenne_dim_for("join_dim_bench", lane).await;
    let _ = cayenne_insert(&dim.table, make_dim_batch(dim_schema(), DIM_ROWS)).await;

    CayenneJoinFixture { fact, dim }
}

fn load_duckdb(fact_parquet: &Path, dim_batch: &arrow::array::RecordBatch) -> DuckDbFixture {
    let fixture = setup_duckdb_with_dim("join_fact_bench", "join_dim_bench");
    duckdb_insert_parquet(&fixture.conn, "join_fact_bench", fact_parquet);
    duckdb_insert_dim_rows(&fixture.conn, "join_dim_bench", dim_batch);
    fixture
}

fn bench_join(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("vs_duckdb_join");
    group.sample_size(10);

    let parquet_dir = tempfile::tempdir().expect("parquet dir");
    let dim_batch = make_dim_batch(dim_schema(), DIM_ROWS);

    for &fact_rows in FACT_ROWS {
        group.throughput(Throughput::Elements(fact_rows as u64));

        let parquet_path = parquet_dir.path().join(format!("fact_{fact_rows}.parquet"));
        write_parquet(&make_batch(schema(), 0, fact_rows), &parquet_path);

        let duckdb_fixture = Arc::new(load_duckdb(&parquet_path, &dim_batch));

        let cayenne_join_agg = "SELECT d.region, SUM(t.value) FROM t JOIN d ON t.id = d.id \
            GROUP BY d.region";
        let duckdb_join_agg = "SELECT d.region, SUM(t.value) FROM join_fact_bench t \
            JOIN join_dim_bench d ON t.id = d.id GROUP BY d.region";

        let cayenne_join_filter = "SELECT SUM(t.value) FROM t JOIN d ON t.id = d.id \
            WHERE d.region = 'NA'";
        let duckdb_join_filter = "SELECT SUM(t.value) FROM join_fact_bench t \
            JOIN join_dim_bench d ON t.id = d.id WHERE d.region = 'NA'";

        for &lane in CAYENNE_LANES {
            let lane_label = lane.lane();
            let cayenne_fixture = Arc::new(rt.block_on(load_cayenne(lane, fact_rows)));

            let cf = Arc::clone(&cayenne_fixture);
            group.bench_with_input(
                BenchmarkId::new(format!("{lane_label}/join_agg"), fact_rows),
                &fact_rows,
                |b, &_rows| {
                    b.iter(|| {
                        rt.block_on(async {
                            let batches =
                                cayenne_query_join(&cf.fact.table, &cf.dim.table, cayenne_join_agg)
                                    .await;
                            black_box(batches);
                        });
                    });
                },
            );

            let cf = Arc::clone(&cayenne_fixture);
            group.bench_with_input(
                BenchmarkId::new(format!("{lane_label}/join_filter"), fact_rows),
                &fact_rows,
                |b, &_rows| {
                    b.iter(|| {
                        rt.block_on(async {
                            let batches = cayenne_query_join(
                                &cf.fact.table,
                                &cf.dim.table,
                                cayenne_join_filter,
                            )
                            .await;
                            black_box(batches);
                        });
                    });
                },
            );
        }

        let df = Arc::clone(&duckdb_fixture);
        group.bench_with_input(
            BenchmarkId::new("duckdb/join_agg", fact_rows),
            &fact_rows,
            |b, &_rows| {
                b.iter(|| {
                    let n = duckdb_query_count(&df.conn, duckdb_join_agg);
                    black_box(n);
                });
            },
        );

        let df = Arc::clone(&duckdb_fixture);
        group.bench_with_input(
            BenchmarkId::new("duckdb/join_filter", fact_rows),
            &fact_rows,
            |b, &_rows| {
                b.iter(|| {
                    let n = duckdb_query_count(&df.conn, duckdb_join_filter);
                    black_box(n);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_join);
criterion_main!(benches);
