// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! PREDICATE-AWARE maintained aggregate — Cayenne vs chDB (embedded ClickHouse)
//! on the CH-benCH q1/q6 shape (filtered single-table grouped aggregate).
//!
//! Sibling of `vs_duckdb_maintained_filtered_groupby`, split into its own binary
//! because chDB (bundled full ClickHouse) and DuckDB (bundled) cannot both be
//! driven in one process — linking + running both aborts at startup. So chDB runs
//! alone here; DuckDB runs alone there. Same workload, same conclusion: chDB has
//! no CDC delta and no cross-query maintained state, so it re-scans + re-filters +
//! re-aggregates O(rows) every query, while Cayenne serves O(groups) from the
//! maintained filtered view. (Same moat as the DuckDB bet — F1 × F5.)
//!
//! - `chdb_rescan`   — `SELECT grp, SUM(value), COUNT(*) FROM t_filtered
//!   WHERE delivery > 1000 GROUP BY grp` against a chDB MergeTree table.
//! - `cayenne_serve` — the same answer from the real `MaintainedAggregateRegistry`.
//!
//! The Cayenne-side fixture is shared with the DuckDB sibling via
//! `maintained_filtered_helpers/common.rs`. The two engines are cross-checked
//! (group count + total SUM + total COUNT over the filtered set must agree) before
//! any timing.

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]

#[path = "vs_chdb_helpers/chdb_common.rs"]
mod chdb_common;
#[path = "maintained_filtered_helpers/common.rs"]
mod common;

use std::hint::black_box;

use arrow::array::RecordBatch;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

use chdb_common::setup_chdb_with_schema;
use common::{ROW_COUNTS, cayenne_result, load_registry, row_batch};

fn write_parquet(batch: &RecordBatch, path: &std::path::Path) {
    use datafusion::parquet::arrow::ArrowWriter;
    let file = std::fs::File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(batch).expect("write parquet batch");
    writer.close().expect("close parquet");
}

const CHDB_GROUPBY: &str =
    "SELECT grp, SUM(value), COUNT(*) FROM t_filtered WHERE delivery > 1000 GROUP BY grp";
const CHDB_SUM: &str = "SELECT SUM(value) FROM t_filtered WHERE delivery > 1000";
const CHDB_COUNT: &str = "SELECT COUNT(*) FROM t_filtered WHERE delivery > 1000";

fn bench_filtered_groupby(c: &mut Criterion) {
    let mut group = c.benchmark_group("vs_chdb_maintained_filtered_groupby");
    group.sample_size(10);

    let parquet_dir = tempfile::tempdir().expect("parquet dir");

    for &rows in ROW_COUNTS {
        let registry = load_registry(rows);
        let cayenne = cayenne_result(&registry, 1);

        let parquet_path = parquet_dir.path().join(format!("filtered_{rows}.parquet"));
        write_parquet(&row_batch(0, rows), &parquet_path);
        // chDB table schema must match the parquet (pk, grp, delivery, value),
        // ORDER BY pk to mirror Cayenne's single-PK physical ordering.
        let chdb = setup_chdb_with_schema(
            "t_filtered",
            &parquet_path,
            "pk Int64, grp Int64, delivery Int64, value Int64",
            "pk",
        );

        // Cross-check chDB and Cayenne agree on the filtered aggregate before timing.
        let chdb_groups = chdb.query_emit_count(CHDB_GROUPBY);
        let chdb_sum = chdb.query_scalar(CHDB_SUM);
        let chdb_count = chdb.query_scalar(CHDB_COUNT);
        let cayenne_sum: i64 = cayenne.values().map(|(sum, _)| *sum).sum();
        let cayenne_count: i64 = cayenne.values().map(|(_, count)| *count).sum();
        assert_eq!(
            chdb_groups,
            cayenne.len(),
            "chdb returned {chdb_groups} groups, cayenne {} at rows={rows}",
            cayenne.len()
        );
        assert_eq!(
            chdb_sum, cayenne_sum,
            "filtered SUM mismatch chdb vs cayenne at rows={rows}"
        );
        assert_eq!(
            chdb_count, cayenne_count,
            "filtered COUNT mismatch chdb vs cayenne at rows={rows}"
        );

        group.bench_with_input(BenchmarkId::new("chdb_rescan", rows), &rows, |b, _| {
            b.iter(|| black_box(chdb.query_emit_count(CHDB_GROUPBY)));
        });

        group.bench_with_input(BenchmarkId::new("cayenne_serve", rows), &rows, |b, _| {
            b.iter(|| black_box(cayenne_result(&registry, 1)));
        });

        drop(chdb);
    }

    group.finish();
}

criterion_group!(benches, bench_filtered_groupby);
criterion_main!(benches);
