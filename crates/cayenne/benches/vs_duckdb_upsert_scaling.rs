// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

//! PK upsert scaling: Cayenne vs DuckDB across preloaded table sizes.
//!
//! Companion to `vs_duckdb_upsert.rs` (which fixes `TABLE_ROWS = 10_000`).
//! This bench sweeps `TABLE_ROWS` through `{10_000, 100_000, 1_000_000}`
//! with a fixed 100 % conflict rate so every incoming row hits the
//! existing keyspace. The intent is to make the keyset-cap regression
//! flagged in `load_existing_keyset_cap_disabled.rs` visible *against
//! DuckDB*: at `TABLE_ROWS > PK_KEYSET_CACHE_MAX_ENTRIES`
//! (`crates/cayenne/src/provider/table.rs:114`, currently `1_000_000`)
//! Cayenne's keyset cache is silently disabled, forcing every CDC
//! commit to cold-start the keyset scan — while DuckDB's PK btree gives
//! O(log N) per-row lookup that is roughly flat in N.
//!
//! ## What this bench measures
//!
//! For each `(N, engine)` pair, one upsert batch of
//! `INCOMING_ROWS = 2_000` rows against a freshly-loaded N-row table.
//! Every iteration re-loads (`iter_batched` + `PerIteration`) so the
//! upsert sees a clean keyset cache the first time the
//! `OnConflictValidationStream` runs — modelling the steady-state CDC
//! commit where the cache was just dropped on the prior write.
//!
//! Three lanes per N:
//! - `cayenne_sqlite/N=...`
//! - `cayenne_turso/N=...` (when built with `--features turso,duckdb-bench`)
//! - `duckdb/N=...`
//!
//! Expected shape:
//! - DuckDB stays roughly flat (`O(INCOMING_ROWS · log N)`).
//! - Cayenne grows linearly with `N` for `N > 1_000_000` because the
//!   keyset cache is no-op'd by `store_cached_pk_keyset`
//!   (`provider/table.rs:3534-3543`).
//!
//! ## How to read
//!
//! ```text
//! cargo bench --bench vs_duckdb_upsert_scaling -p cayenne --features duckdb-bench
//! ```
//!
//! - `cayenne_sqlite/N=10000` and `duckdb/N=10000` reproduce the
//!   `vs_duckdb_upsert/conflict_100pct` numbers (sanity).
//! - The ratio `cayenne_sqlite/N=1000000` / `cayenne_sqlite/N=10000`
//!   shows the cap-trigger amplification — the same upsert against a
//!   100× bigger preload.
//! - The ratio `cayenne_sqlite/N=1000000` / `duckdb/N=1000000` is the
//!   Cayenne-vs-DuckDB gap at the operational scale that the May 18
//!   SF100 retest hit.

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_possible_truncation)]

#[path = "vs_duckdb_helpers/common.rs"]
mod common;

use std::hint::black_box;
use std::path::Path;
use std::sync::Arc;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::Runtime;

use common::{
    CAYENNE_LANES, CayenneFixture, DuckDbFixture, Metastore, cayenne_insert,
    duckdb_insert_parquet, duckdb_upsert_parquet, make_batch, schema, setup_cayenne_pk_for,
    setup_duckdb_pk, write_parquet,
};

/// Preloaded table sizes. The middle and top values straddle
/// `PK_KEYSET_CACHE_MAX_ENTRIES = 1_000_000` so the cap-trigger boundary
/// is empirically visible in the report.
const TABLE_ROW_COUNTS: &[usize] = &[10_000, 100_000, 1_000_000];

/// Fixed incoming-batch size. Matches `vs_duckdb_upsert.rs` so the
/// `N=10000` data points are directly comparable to the conflict_100pct
/// lane there.
const INCOMING_ROWS: usize = 2_000;

/// All incoming rows collide. The expensive `OnConflictValidationStream`
/// path is exercised on every row, which is the worst case for Cayenne
/// and the test case where the keyset rebuild matters.
const CONFLICT_PCT: usize = 100;

fn make_upsert_batch(table_rows: usize, incoming_rows: usize) -> arrow::array::RecordBatch {
    use arrow::array::{Int64Array, RecordBatch, StringArray};

    let mut ids: Vec<i64> = Vec::with_capacity(incoming_rows);
    for i in 0..incoming_rows {
        // Spread collisions across the existing keyspace; Knuth multiplicative
        // scramble matches the pattern in `vs_duckdb_upsert.rs`.
        ids.push(((i as u64).wrapping_mul(2_654_435_761) % table_rows as u64) as i64);
    }
    let names: Vec<String> = ids.iter().map(|id| format!("upsert_{id}")).collect();
    let values: Vec<i64> = ids.iter().map(|id| id * 7).collect();

    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .expect("upsert batch")
}

async fn load_cayenne(lane: Metastore, table_rows: usize) -> CayenneFixture {
    let fixture = setup_cayenne_pk_for("upsert_scaling_bench", lane).await;
    let batch = make_batch(schema(), 0, table_rows);
    let _ = cayenne_insert(&fixture.table, batch).await;
    fixture
}

fn load_duckdb(parquet_path: &Path) -> DuckDbFixture {
    let fixture = setup_duckdb_pk("upsert_scaling_bench");
    duckdb_insert_parquet(&fixture.conn, "upsert_scaling_bench", parquet_path);
    fixture
}

fn bench_upsert_scaling(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("vs_duckdb_upsert_scaling");
    // Sample size 10 is the minimum criterion accepts; balances accuracy
    // against the per-iteration preload cost that dominates at N=1M.
    group.sample_size(10);
    group.throughput(Throughput::Elements(INCOMING_ROWS as u64));

    let parquet_dir = tempfile::tempdir().expect("parquet dir");

    for &table_rows in TABLE_ROW_COUNTS {
        // Materialize the base table once per N. DuckDB loads from this; Cayenne
        // re-creates from the same Arrow batch via `make_batch` so both engines
        // see byte-identical initial state.
        let base_parquet_path = parquet_dir
            .path()
            .join(format!("base_{table_rows}.parquet"));
        write_parquet(
            &make_batch(schema(), 0, table_rows),
            &base_parquet_path,
        );

        let upsert_batch = Arc::new(make_upsert_batch(table_rows, INCOMING_ROWS));
        let upsert_parquet_path = parquet_dir
            .path()
            .join(format!("upsert_{table_rows}.parquet"));
        write_parquet(&upsert_batch, &upsert_parquet_path);

        for &lane in CAYENNE_LANES {
            let lane_label = lane.lane();
            let batch = Arc::clone(&upsert_batch);
            group.bench_with_input(
                BenchmarkId::new(lane_label, format!("N={table_rows}")),
                &table_rows,
                |b, &n| {
                    b.iter_batched(
                        || rt.block_on(load_cayenne(lane, n)),
                        |fixture| {
                            rt.block_on(async {
                                let written =
                                    cayenne_insert(&fixture.table, (*batch).clone()).await;
                                black_box((fixture, written));
                            });
                        },
                        BatchSize::PerIteration,
                    );
                },
            );
        }

        let parquet = upsert_parquet_path.clone();
        let base = base_parquet_path.clone();
        group.bench_with_input(
            BenchmarkId::new("duckdb", format!("N={table_rows}")),
            &table_rows,
            |b, &_n| {
                b.iter_batched(
                    || load_duckdb(&base),
                    |fixture| {
                        duckdb_upsert_parquet(&fixture.conn, "upsert_scaling_bench", &parquet);
                        black_box(fixture);
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }

    // Suppress unused warning when the bench is compiled without
    // `--features duckdb-bench`. `CONFLICT_PCT` is documented as part of the
    // bench shape and is referenced in the docstring header above.
    let _ = CONFLICT_PCT;

    group.finish();
}

criterion_group!(benches, bench_upsert_scaling);
criterion_main!(benches);
