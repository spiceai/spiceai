// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

//! CDC-style append-burst throughput: Cayenne vs DuckDB.
//!
//! Models the apply-loop pattern from the runtime's CDC pipeline: many small
//! bursts arriving back-to-back, each going through the engine's full
//! per-burst commit path (Cayenne: inline + staged-WAL finalize + listing
//! refresh + stats persist; DuckDB: per-statement WAL append + B-tree update).
//!
//! Each iteration writes `burst_count` bursts of `burst_rows` rows each. The
//! timed region covers all bursts so per-burst fixed cost is amortized into
//! the throughput number. The total row count is the Criterion throughput
//! denominator, so the result is directly comparable across engines and
//! lanes.
//!
//! Lanes (compile-time gated):
//! - `cayenne`       — Cayenne with the SQLite metastore (default)
//! - `cayenne_turso` — Cayenne with the Turso metastore (--features turso)
//! - `duckdb`        — DuckDB file-mode with `INSERT INTO ... VALUES (...)`

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_possible_truncation)]

#[path = "vs_duckdb_helpers/common.rs"]
mod common;

use std::hint::black_box;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::Runtime;

use common::{
    CAYENNE_LANES, cayenne_insert, duckdb_insert_rows, make_batch, schema, setup_cayenne_for,
    setup_duckdb,
};

const BURST_ROWS: usize = 64;
const BURST_COUNTS: &[usize] = &[16, 64, 256];

fn bench_burst(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("vs_duckdb_burst");
    group.sample_size(10);

    for &burst_count in BURST_COUNTS {
        let total_rows = (burst_count * BURST_ROWS) as u64;
        group.throughput(Throughput::Elements(total_rows));

        // Pre-build the burst payload so the timed region only pays the
        // engine's commit cost, not Arrow row construction. Each burst gets a
        // distinct id range so PK collisions never happen on the no-PK path.
        let batches: Vec<_> = (0..burst_count)
            .map(|i| make_batch(schema(), (i * BURST_ROWS) as i64, BURST_ROWS))
            .collect();

        for &lane in CAYENNE_LANES {
            let lane_label = lane.lane();
            let batches_setup = batches.clone();
            group.bench_with_input(
                BenchmarkId::new(lane_label, burst_count),
                &burst_count,
                |b, &_burst_count| {
                    b.iter_batched(
                        || {
                            let fixture = rt.block_on(setup_cayenne_for("burst_bench", lane));
                            (fixture, batches_setup.clone())
                        },
                        |(fixture, burst_batches)| {
                            rt.block_on(async {
                                for batch in burst_batches {
                                    let _ = cayenne_insert(&fixture.table, batch).await;
                                }
                            });
                            black_box(fixture);
                        },
                        BatchSize::PerIteration,
                    );
                },
            );
        }

        let batches_setup = batches.clone();
        group.bench_with_input(
            BenchmarkId::new("duckdb", burst_count),
            &burst_count,
            |b, &_burst_count| {
                b.iter_batched(
                    || (setup_duckdb("burst_bench"), batches_setup.clone()),
                    |(fixture, burst_batches)| {
                        for batch in burst_batches {
                            duckdb_insert_rows(&fixture.conn, "burst_bench", &batch);
                        }
                        black_box(fixture);
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_burst);
criterion_main!(benches);
