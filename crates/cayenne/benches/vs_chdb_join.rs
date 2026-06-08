// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

//! Two-table inner join: Cayenne vs chDB (embedded ClickHouse).
//!
//! Sibling of `vs_duckdb_join`, same dataset and same two query shapes:
//!
//! - `join_agg`: aggregate (`SUM(t.value)`) grouped by the dim's `region` —
//!   exercises the hash join + aggregate kernels end-to-end.
//! - `join_filter`: filter `WHERE d.region = 'NA'` before aggregating —
//!   exercises join-side pushdown.
//!
//! Both tables are loaded into chDB from the SAME parquet/dim data the
//! `vs_duckdb_join` lanes ingest, so the join plans run on identical input.
//!
//! Lanes:
//! - `cayenne`       — Cayenne with the SQLite metastore (default)
//! - `cayenne_turso` — Cayenne with the Turso metastore (--features turso)
//! - `chdb`          — chDB (embedded ClickHouse), MergeTree engine

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_possible_truncation)]

#[path = "vs_chdb_helpers/chdb_common.rs"]
mod chdb_common;
#[path = "vs_duckdb_helpers/common.rs"]
mod common;

use std::hint::black_box;
use std::path::Path;
use std::sync::Arc;

use arrow::array::RecordBatch;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::Runtime;

use chdb_common::{ChdbFixture, setup_chdb_from_parquet};
use common::{
    CAYENNE_LANES, CayenneFixture, Metastore, cayenne_insert, cayenne_query_join, dim_schema,
    make_batch, make_dim_batch, schema, setup_cayenne_dim_for, setup_cayenne_for, write_parquet,
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

/// chDB owns one process-global engine, so the fact and dim tables must live
/// in the SAME session. This loads the fact parquet, then the dim parquet,
/// into one fixture's session.
fn load_chdb(fact_parquet: &Path, dim_parquet: &Path) -> ChdbFixture {
    let fixture = setup_chdb_from_parquet("join_fact_bench", fact_parquet);
    // Reuse the same session for the dim table: create + load it in-place.
    fixture
        .session
        .execute(
            "CREATE TABLE join_dim_bench (id Int64, region String) ENGINE = MergeTree() ORDER BY id",
            None,
        )
        .expect("chdb create dim table");
    let dim_display = dim_parquet.to_string_lossy();
    fixture
        .session
        .execute(
            &format!("INSERT INTO join_dim_bench SELECT * FROM file('{dim_display}', 'Parquet')"),
            None,
        )
        .expect("chdb insert dim parquet");
    fixture
}

fn bench_join(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("vs_chdb_join");
    group.sample_size(10);

    let parquet_dir = tempfile::tempdir().expect("parquet dir");

    // Materialize the dim batch to parquet once — chDB ingests it via file(),
    // matching how the fact table is loaded (and how DuckDB's lane works).
    let dim_batch: RecordBatch = make_dim_batch(dim_schema(), DIM_ROWS);
    let dim_parquet = parquet_dir.path().join("dim.parquet");
    write_parquet(&dim_batch, &dim_parquet);

    let cayenne_join_agg = "SELECT d.region, SUM(t.value) FROM t JOIN d ON t.id = d.id \
        GROUP BY d.region";
    let chdb_join_agg = "SELECT d.region, SUM(t.value) FROM join_fact_bench AS t \
        JOIN join_dim_bench AS d ON t.id = d.id GROUP BY d.region";

    let cayenne_join_filter = "SELECT SUM(t.value) FROM t JOIN d ON t.id = d.id \
        WHERE d.region = 'NA'";
    let chdb_join_filter = "SELECT SUM(t.value) FROM join_fact_bench AS t \
        JOIN join_dim_bench AS d ON t.id = d.id WHERE d.region = 'NA'";

    for &fact_rows in FACT_ROWS {
        group.throughput(Throughput::Elements(fact_rows as u64));

        let parquet_path = parquet_dir.path().join(format!("fact_{fact_rows}.parquet"));
        write_parquet(&make_batch(schema(), 0, fact_rows), &parquet_path);

        let chdb_fixture = load_chdb(&parquet_path, &dim_parquet);

        // Precondition: the join must produce the 4 region groups, else the
        // join silently degenerated (e.g. a mismatched key dtype dropped all
        // matches) and the timed numbers would be meaningless.
        let chdb_regions = chdb_fixture.query_emit_count(chdb_join_agg);
        assert_eq!(
            chdb_regions, 4,
            "chdb join_agg returned {chdb_regions} region groups, expected 4 — \
             the fact/dim join did not match as expected"
        );

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

        group.bench_with_input(
            BenchmarkId::new("chdb/join_agg", fact_rows),
            &fact_rows,
            |b, &_rows| {
                b.iter(|| {
                    let n = chdb_fixture.query_emit_count(chdb_join_agg);
                    black_box(n);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("chdb/join_filter", fact_rows),
            &fact_rows,
            |b, &_rows| {
                b.iter(|| {
                    let v = chdb_fixture.query_scalar(chdb_join_filter);
                    black_box(v);
                });
            },
        );

        drop(chdb_fixture);
    }

    group.finish();
}

criterion_group!(benches, bench_join);
criterion_main!(benches);
