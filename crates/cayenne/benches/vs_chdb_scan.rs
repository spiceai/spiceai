// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

//! Scan + aggregate throughput: Cayenne vs chDB (embedded ClickHouse).
//!
//! Sibling of `vs_duckdb_scan`. chDB is the canonical embedded-OLAP reference
//! for scan/aggregate workloads, so this bench measures the chDB lane on the
//! SAME dataset and the SAME three read shapes as `vs_duckdb_scan`, letting
//! Cayenne be compared against both DuckDB and chDB in one report:
//! * `count_star`  — `SELECT COUNT(*) FROM t`. Pure cardinality.
//! * `sum_value`   — `SELECT SUM(value) FROM t`. Numeric column scan +
//!                    aggregate; exercises ClickHouse's vectorized SIMD path.
//! * `filter_sum`  — `SELECT SUM(value) FROM t WHERE id BETWEEN ? AND ?`.
//!                    Exercises predicate evaluation + MergeTree skip-index.
//!
//! Both the Cayenne lane and the chDB lane are timed here. The Cayenne lane is
//! byte-for-byte the same fixture/query the `vs_duckdb_scan` Cayenne lane runs,
//! so its numbers carry across reports unchanged — the only new signal is the
//! `chdb/*` lane. (Once the deferred patch folds chDB into `vs_duckdb_scan`,
//! this standalone bench can be retired; see the plan doc.)
//!
//! Lanes:
//! - `cayenne`       — Cayenne with the SQLite metastore (default)
//! - `cayenne_turso` — Cayenne with the Turso metastore (--features turso)
//! - `chdb`          — chDB (embedded ClickHouse), MergeTree engine

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]

#[path = "vs_duckdb_helpers/common.rs"]
mod common;
#[path = "vs_chdb_helpers/chdb_common.rs"]
mod chdb_common;

use std::hint::black_box;
use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::Runtime;

use chdb_common::setup_chdb_from_parquet;
use common::{
    CAYENNE_LANES, CayenneFixture, Metastore, cayenne_insert, cayenne_query, cayenne_query_warm,
    make_batch, schema, setup_cayenne_for, warm_session_for, write_parquet,
};

const ROW_COUNTS: &[usize] = &[16_384, 131_072, 1_048_576];

async fn load_cayenne(lane: Metastore, rows: usize) -> CayenneFixture {
    let fixture = setup_cayenne_for("scan_bench", lane).await;
    let batch = make_batch(schema(), 0, rows);
    let _ = cayenne_insert(&fixture.table, batch).await;
    fixture
}

fn bench_scan(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("vs_chdb_scan");
    group.sample_size(10);

    let parquet_dir = tempfile::tempdir().expect("parquet dir");

    for &rows in ROW_COUNTS {
        group.throughput(Throughput::Elements(rows as u64));

        // The exact same parquet generator the vs_duckdb_scan bench writes, so
        // all three engines ingest byte-identical input.
        let parquet_path = parquet_dir.path().join(format!("rows_{rows}.parquet"));
        let batch = make_batch(schema(), 0, rows);
        write_parquet(&batch, &parquet_path);

        // Load once, query many times — match the steady-state read pattern.
        // chDB owns a single process-global engine, so exactly one fixture is
        // live at a time: it's built here, used across all three shapes, and
        // dropped at the end of this row-count iteration before the next.
        let chdb_fixture = setup_chdb_from_parquet("scan_bench", &parquet_path);

        let mut cayenne_fixtures = Vec::with_capacity(CAYENNE_LANES.len());
        for &lane in CAYENNE_LANES {
            cayenne_fixtures.push((lane.lane(), Arc::new(rt.block_on(load_cayenne(lane, rows)))));
        }
        let warm_ctx = Arc::new(warm_session_for(&cayenne_fixtures[0].1.table));

        // Sanity-check chDB loaded the expected cardinality before timing —
        // a fixture that silently loaded zero rows (e.g. a parquet path typo)
        // would make every chdb/* number a meaningless measurement of an empty
        // table. This is the precondition gate the bench-templates require.
        let chdb_count = chdb_fixture.query_scalar("SELECT COUNT(*) FROM scan_bench");
        assert_eq!(
            chdb_count, rows as i64,
            "chdb loaded {chdb_count} rows, expected {rows} — parquet ingest is broken, \
             the bench would measure an empty table"
        );

        // --- count_star ---
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
        let wc = Arc::clone(&warm_ctx);
        group.bench_with_input(
            BenchmarkId::new("cayenne_warm/count_star", rows),
            &rows,
            |b, &_rows| {
                b.iter(|| {
                    rt.block_on(async {
                        let batches = cayenne_query_warm(&wc, "SELECT COUNT(*) FROM t").await;
                        black_box(batches);
                    });
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("chdb/count_star", rows),
            &rows,
            |b, &_rows| {
                b.iter(|| {
                    let v = chdb_fixture.query_scalar("SELECT COUNT(*) FROM scan_bench");
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
        let wc = Arc::clone(&warm_ctx);
        group.bench_with_input(
            BenchmarkId::new("cayenne_warm/sum_value", rows),
            &rows,
            |b, &_rows| {
                b.iter(|| {
                    rt.block_on(async {
                        let batches = cayenne_query_warm(&wc, "SELECT SUM(value) FROM t").await;
                        black_box(batches);
                    });
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("chdb/sum_value", rows),
            &rows,
            |b, &_rows| {
                b.iter(|| {
                    let v = chdb_fixture.query_scalar("SELECT SUM(value) FROM scan_bench");
                    black_box(v);
                });
            },
        );

        // --- filter_sum (selects ~10% of rows in the middle of the range) ---
        let lo = (rows as i64) * 45 / 100;
        let hi = (rows as i64) * 55 / 100;
        let cayenne_sql = format!("SELECT SUM(value) FROM t WHERE id BETWEEN {lo} AND {hi}");
        let chdb_sql = format!("SELECT SUM(value) FROM scan_bench WHERE id BETWEEN {lo} AND {hi}");

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
        let wc = Arc::clone(&warm_ctx);
        let cayenne_sql_warm = cayenne_sql.clone();
        group.bench_with_input(
            BenchmarkId::new("cayenne_warm/filter_sum", rows),
            &rows,
            |b, &_rows| {
                b.iter(|| {
                    rt.block_on(async {
                        let batches = cayenne_query_warm(&wc, &cayenne_sql_warm).await;
                        black_box(batches);
                    });
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("chdb/filter_sum", rows),
            &rows,
            |b, &_rows| {
                b.iter(|| {
                    let v = chdb_fixture.query_scalar(&chdb_sql);
                    black_box(v);
                });
            },
        );

        // Explicit drop before the next row-count iteration builds the next
        // chDB engine — only one libchdb instance may be live per process.
        drop(chdb_fixture);
        drop(cayenne_fixtures);
        drop(warm_ctx);
    }

    group.finish();
}

criterion_group!(benches, bench_scan);
criterion_main!(benches);
