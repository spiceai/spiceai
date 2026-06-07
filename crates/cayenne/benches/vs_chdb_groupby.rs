// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

//! GROUP BY aggregation throughput: Cayenne vs chDB (embedded ClickHouse).
//!
//! Sibling of `vs_duckdb_groupby`, run on the SAME dataset and the SAME query
//! at the SAME group cardinalities so the chDB lane drops straight into the
//! three-way comparison. Aggregation kernels are cardinality-sensitive:
//! low-cardinality groups stay in cache and stress the hash-aggregate
//! intrinsics; high-cardinality groups thrash the hash table — ClickHouse's
//! two-level aggregation vs DuckDB's vs Cayenne/DataFusion's is exactly what
//! this surfaces.
//!
//! Lanes:
//! - `cayenne`       — Cayenne with the SQLite metastore (default)
//! - `cayenne_turso` — Cayenne with the Turso metastore (--features turso)
//! - `chdb`          — chDB (embedded ClickHouse), MergeTree engine

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_possible_truncation)]

#[path = "vs_duckdb_helpers/common.rs"]
mod common;
#[path = "vs_chdb_helpers/chdb_common.rs"]
mod chdb_common;

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::RecordBatch;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::Runtime;

use chdb_common::setup_chdb_from_parquet;
use common::{
    CAYENNE_LANES, CayenneFixture, Metastore, cayenne_insert, cayenne_query, make_batch_grouped,
    schema, setup_cayenne_for, write_parquet,
};

const ROW_COUNTS: &[usize] = &[16_384, 131_072];
const GROUP_CARDINALITIES: &[usize] = &[8, 1_024, 16_384];

async fn load_cayenne(lane: Metastore, rows: usize, groups: usize) -> CayenneFixture {
    let fixture = setup_cayenne_for("groupby_bench", lane).await;
    let batch = make_batch_grouped(schema(), 0, rows, groups);
    let _ = cayenne_insert(&fixture.table, batch).await;
    fixture
}

fn bench_groupby(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("vs_chdb_groupby");
    group.sample_size(10);

    let parquet_dir = tempfile::tempdir().expect("parquet dir");

    let cayenne_sql = "SELECT name, COUNT(*), SUM(value) FROM t GROUP BY name";
    let chdb_sql = "SELECT name, COUNT(*), SUM(value) FROM groupby_bench GROUP BY name";

    for &rows in ROW_COUNTS {
        for &groups in GROUP_CARDINALITIES {
            let effective_groups = groups.min(rows);
            group.throughput(Throughput::Elements(rows as u64));

            // Same grouped-parquet generator the vs_duckdb_groupby bench uses,
            // so chDB aggregates byte-identical input.
            let parquet_path = parquet_dir
                .path()
                .join(format!("rows_{rows}_groups_{effective_groups}.parquet"));
            let batch: RecordBatch = make_batch_grouped(schema(), 0, rows, effective_groups);
            write_parquet(&batch, &parquet_path);

            // One process-global chDB engine: build, validate, time, drop —
            // all within this (rows, groups) iteration.
            let chdb_fixture = setup_chdb_from_parquet("groupby_bench", &parquet_path);

            // Precondition gate: the GROUP BY must return exactly the expected
            // number of distinct groups, else the bench measures the wrong
            // aggregation (e.g. a load that collapsed the name column).
            let chdb_groups = chdb_fixture.query_emit_count(chdb_sql);
            assert_eq!(
                chdb_groups, effective_groups,
                "chdb GROUP BY returned {chdb_groups} groups, expected {effective_groups} \
                 — the grouped dataset did not load as expected"
            );

            for &lane in CAYENNE_LANES {
                let lane_label = lane.lane();
                let cayenne_fixture =
                    Arc::new(rt.block_on(load_cayenne(lane, rows, effective_groups)));
                let cf = Arc::clone(&cayenne_fixture);
                group.bench_with_input(
                    BenchmarkId::new(
                        format!("{lane_label}/group_by_name"),
                        format!("rows={rows}/groups={effective_groups}"),
                    ),
                    &rows,
                    |b, &_rows| {
                        b.iter(|| {
                            rt.block_on(async {
                                let batches = cayenne_query(&cf.table, cayenne_sql).await;
                                black_box(batches);
                            });
                        });
                    },
                );
            }

            group.bench_with_input(
                BenchmarkId::new(
                    "chdb/group_by_name",
                    format!("rows={rows}/groups={effective_groups}"),
                ),
                &rows,
                |b, &_rows| {
                    b.iter(|| {
                        let n = chdb_fixture.query_emit_count(chdb_sql);
                        black_box(n);
                    });
                },
            );

            drop(chdb_fixture);
        }
    }

    group.finish();
}

criterion_group!(benches, bench_groupby);
criterion_main!(benches);
