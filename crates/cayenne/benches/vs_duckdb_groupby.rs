// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

//! GROUP BY aggregation throughput: Cayenne vs DuckDB.
//!
//! Aggregation kernels are sensitive to group cardinality: low-cardinality
//! groups stay in CPU caches and stress hash-aggregate intrinsics;
//! high-cardinality groups thrash the hash table and stress probe / rehash
//! paths. This bench runs the same query at three cardinalities for each
//! row count, so the engine-to-engine delta and the cardinality sensitivity
//! both show up in the Criterion report.
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

use arrow::array::RecordBatch;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::Runtime;

use common::{
    CAYENNE_LANES, CayenneFixture, DuckDbFixture, Metastore, capture_comparison_plans,
    cayenne_insert, cayenne_query, duckdb_insert_parquet, duckdb_query_count, make_batch_grouped,
    schema, setup_cayenne_for, setup_duckdb, write_parquet,
};

const ROW_COUNTS: &[usize] = &[16_384, 131_072];
const GROUP_CARDINALITIES: &[usize] = &[8, 1_024, 16_384];

async fn load_cayenne(lane: Metastore, rows: usize, groups: usize) -> CayenneFixture {
    let fixture = setup_cayenne_for("groupby_bench", lane).await;
    let batch = make_batch_grouped(schema(), 0, rows, groups);
    let _ = cayenne_insert(&fixture.table, batch).await;
    fixture
}

fn load_duckdb(parquet_path: &Path) -> DuckDbFixture {
    let fixture = setup_duckdb("groupby_bench");
    duckdb_insert_parquet(&fixture.conn, "groupby_bench", parquet_path);
    fixture
}

fn bench_groupby(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("vs_duckdb_groupby");
    group.sample_size(10);

    let parquet_dir = tempfile::tempdir().expect("parquet dir");

    for &rows in ROW_COUNTS {
        for &groups in GROUP_CARDINALITIES {
            let effective_groups = groups.min(rows);
            group.throughput(Throughput::Elements(rows as u64));

            let parquet_path = parquet_dir
                .path()
                .join(format!("rows_{rows}_groups_{effective_groups}.parquet"));
            let batch: RecordBatch = make_batch_grouped(schema(), 0, rows, effective_groups);
            write_parquet(&batch, &parquet_path);

            let duckdb_fixture = Arc::new(load_duckdb(&parquet_path));

            let cayenne_sql = "SELECT name, COUNT(*), SUM(value) FROM t GROUP BY name";
            let duckdb_sql =
                "SELECT name, COUNT(*), SUM(value) FROM groupby_bench GROUP BY name";

            // Plan capture uses the SQLite lane — Turso would emit the same
            // DataFusion plan because the metastore only affects metadata I/O,
            // not query planning.
            let plan_fixture = Arc::new(rt.block_on(load_cayenne(
                Metastore::Sqlite,
                rows,
                effective_groups,
            )));
            rt.block_on(capture_comparison_plans(
                &format!("groupby/{rows}/groups_{effective_groups}/group_by_name"),
                &plan_fixture.table,
                &duckdb_fixture.conn,
                cayenne_sql,
                duckdb_sql,
            ));

            for &lane in CAYENNE_LANES {
                let lane_label = lane.lane();
                let cayenne_fixture = Arc::new(rt.block_on(load_cayenne(
                    lane,
                    rows,
                    effective_groups,
                )));
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

            let df = Arc::clone(&duckdb_fixture);
            group.bench_with_input(
                BenchmarkId::new(
                    "duckdb/group_by_name",
                    format!("rows={rows}/groups={effective_groups}"),
                ),
                &rows,
                |b, &_rows| {
                    b.iter(|| {
                        let n = duckdb_query_count(&df.conn, duckdb_sql);
                        black_box(n);
                    });
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_groupby);
criterion_main!(benches);
