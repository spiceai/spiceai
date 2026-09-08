// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

//! PK conflict resolution throughput: Cayenne vs DuckDB.
//!
//! Both engines start with the same N rows preloaded under a single-column
//! `id` primary key. The timed region applies an incoming batch where a tunable
//! fraction of the rows collide with existing keys; the rest are new.
//!
//! - Cayenne side: `OnConflict::Upsert` on `id`. Collisions land on the
//!   deletion-index + inline rewrite path; non-collisions land on the regular
//!   append path. Both ultimately commit through the metastore, so this bench
//!   directly stresses the SQLite single-writer mutex that Finding 1 in the
//!   CH-benCH retest hypothesized as a cross-dataset bottleneck.
//! - DuckDB side: `INSERT INTO ... ON CONFLICT (id) DO UPDATE SET ...` from a
//!   parquet source — DuckDB's documented upsert path.
//!
//! Conflict fractions covered: 0 % (pure insert into a PK'd table), 50 %, and
//! 100 % (every incoming row replaces an existing one).
//!
//! ## Iter 9 extension (parameterized large-N for keyset-cap validation)
//! Add a `bench_upsert_keyset_cap` (or rename) that runs only under `#[cfg(feature="duckdb-bench")]`
//! with TABLE_ROWS in {10_000, 100_000, 1_000_000, 3_000_000}, conflict_pct=100% fixed,
//! sample_size(10), and throughput by elements. Expect DuckDB wall time ~flat (PK index O(log N));
//! Cayenne time stays flat for tables that fit under the byte-budget cap
//! (`PK_KEYSET_CACHE_MAX_BYTES = 256 MiB`, ~4 M narrow-PK rows) and grows with N
//! once the budget is exceeded and `load_existing_keyset` must rebuild from scratch on every commit.
//! Fixture materialization cost (~30 s+ per 1M+ variant) is why this is *not* in default CI suite.

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
    CAYENNE_LANES, CayenneFixture, DuckDbFixture, Metastore, cayenne_insert, duckdb_insert_parquet,
    duckdb_upsert_parquet, make_batch, schema, setup_cayenne_pk_for, setup_duckdb_pk,
    write_parquet,
};

const TABLE_ROWS: usize = 10_000;
const INCOMING_ROWS: usize = 2_000;
const CONFLICT_PERCENTS: &[usize] = &[0, 50, 100];

/// Build an incoming batch where `conflict_pct` % of rows collide with the
/// existing `0..TABLE_ROWS` keyspace. New rows start at `TABLE_ROWS` and grow
/// upward, guaranteeing they don't collide either with existing rows or with
/// each other.
fn make_upsert_batch(
    conflict_pct: usize,
    table_rows: usize,
    incoming_rows: usize,
) -> arrow::array::RecordBatch {
    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use std::sync::Arc;

    let conflict_count = incoming_rows * conflict_pct / 100;
    let new_count = incoming_rows - conflict_count;

    let mut ids: Vec<i64> = Vec::with_capacity(incoming_rows);
    for i in 0..conflict_count {
        // Spread collisions across the existing keyspace.
        ids.push(((i as u64).wrapping_mul(2654435761) % table_rows as u64) as i64);
    }
    for i in 0..new_count {
        ids.push((table_rows + i) as i64);
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

async fn load_cayenne(lane: Metastore) -> CayenneFixture {
    let fixture = setup_cayenne_pk_for("upsert_bench", lane).await;
    let batch = make_batch(schema(), 0, TABLE_ROWS);
    let _ = cayenne_insert(&fixture.table, batch).await;
    fixture
}

fn load_duckdb(parquet_path: &Path) -> DuckDbFixture {
    let fixture = setup_duckdb_pk("upsert_bench");
    duckdb_insert_parquet(&fixture.conn, "upsert_bench", parquet_path);
    fixture
}

fn bench_upsert(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("vs_duckdb_upsert");
    group.sample_size(10);
    group.throughput(Throughput::Elements(INCOMING_ROWS as u64));

    let parquet_dir = tempfile::tempdir().expect("parquet dir");

    // Materialize the base table once as parquet — DuckDB will load from it,
    // Cayenne re-uses the same Arrow batch through `cayenne_insert`. Both
    // engines see identical initial state.
    let base_parquet_path = parquet_dir.path().join("base.parquet");
    write_parquet(&make_batch(schema(), 0, TABLE_ROWS), &base_parquet_path);

    for &conflict_pct in CONFLICT_PERCENTS {
        let upsert_batch = Arc::new(make_upsert_batch(conflict_pct, TABLE_ROWS, INCOMING_ROWS));
        let upsert_parquet_path = parquet_dir
            .path()
            .join(format!("upsert_{conflict_pct}.parquet"));
        write_parquet(&upsert_batch, &upsert_parquet_path);

        for &lane in CAYENNE_LANES {
            let lane_label = lane.lane();
            let batch = Arc::clone(&upsert_batch);
            group.bench_with_input(
                BenchmarkId::new(lane_label, format!("conflict_{conflict_pct}pct")),
                &conflict_pct,
                |b, &_pct| {
                    b.iter_batched(
                        || rt.block_on(load_cayenne(lane)),
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
            BenchmarkId::new("duckdb", format!("conflict_{conflict_pct}pct")),
            &conflict_pct,
            |b, &_pct| {
                b.iter_batched(
                    || load_duckdb(&base),
                    |fixture| {
                        duckdb_upsert_parquet(&fixture.conn, "upsert_bench", &parquet);
                        black_box(fixture);
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_upsert);
criterion_main!(benches);
