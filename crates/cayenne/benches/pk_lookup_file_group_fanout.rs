/*
Copyright 2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! PK point-lookup `target_partitions` sensitivity.
//!
//! `vs_duckdb_pk_lookup/single_pk/1048576` measures Cayenne at ~1.9 ms vs
//! DuckDB at ~146 µs — a 13× gap. The captured EXPLAIN ANALYZE
//! (`target/cayenne_vs_duckdb_plans/pk_lookup_1048576_single_pk.md`) shows:
//!
//!   - 16 `file_groups` across 2 physical `.vortex` files (auto byte-range split
//!     by DataFusion's `ListingTable::repartitioned` because both files are
//!     above `repartition_file_min_size=10 MB` and `target_partitions=16`).
//!   - `time_elapsed_opening = 12.80 ms` summed / 16 ≈ 0.8 ms wall per group
//!     for the Vortex footer read.
//!   - `time_elapsed_scanning_total = 11.24 ms` summed / 16 ≈ 0.7 ms wall per
//!     group for chunk scan.
//!   - Net wall time ≈ max(per-group time) + filter overhead ≈ 1.9 ms.
//!
//! For a highly selective predicate (`id = K` matches exactly one row), the
//! 16-way fan-out is **net harmful** even though it improves throughput for
//! full scans:
//!
//!   - Each file_group pays a full Vortex footer open (~50 µs) — these
//!     parallelise poorly inside one tokio task pool, and even when they do,
//!     the wall-clock floor is dominated by the slowest open, not the sum.
//!   - Vortex's per-chunk min/max pruning, where engaged, only matters for
//!     the *one* file_group that actually contains `K`. The other 15 do
//!     redundant work and pay the open cost regardless.
//!   - DataFusion does not do file-level min/max pruning above
//!     `ListingTable::scan` — the entire file list is handed to
//!     `DataSourceExec`, byte-range-split, and scanned in parallel.
//!
//! This bench measures how Cayenne's PK-lookup wall time varies with
//! `target_partitions` over the same 1 M-row table. The hypothesis: 1-2
//! partitions are *faster* than 16 for a point lookup because they
//! amortise the per-group footer-open cost. If true, the right fix is for
//! `CayenneTableProvider::scan` to override `target_partitions` to 1 when
//! the WHERE clause contains a high-selectivity equality on a PK column.
//!
//! Three table sizes (`16_384`, `131_072`, `1_048_576`) bracket the bench
//! sizes used by `vs_duckdb_pk_lookup`.
//!
//! Lanes per size:
//!   - `default_target_partitions` — `SessionContext::new()`, picks up
//!     `num_cpus` (typically 16 on aarch64).
//!   - `target_partitions_8`        — `SessionConfig::new().with_target_partitions(8)`.
//!   - `target_partitions_4`        — `with_target_partitions(4)`.
//!   - `target_partitions_2`        — `with_target_partitions(2)`.
//!   - `target_partitions_1`        — `with_target_partitions(1)`. Should
//!     remove byte-range fan-out entirely.
//!
//! `cargo bench --bench pk_lookup_file_group_fanout -p cayenne --features duckdb-bench`.

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_possible_truncation)]

#[path = "vs_duckdb_helpers/common.rs"]
mod common;

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::RecordBatch;
use cayenne::CayenneTableProvider;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::datasource::TableProvider;
use datafusion::execution::config::SessionConfig;
use datafusion::prelude::SessionContext;
use tokio::runtime::Runtime;

use common::{CayenneFixture, cayenne_insert, make_batch, schema, setup_cayenne_pk};

const TABLE_SIZES: &[usize] = &[16_384, 131_072, 1_048_576];
const TARGET_PARTITIONS: &[(&str, usize)] = &[
    ("target_partitions_1", 1),
    ("target_partitions_2", 2),
    ("target_partitions_4", 4),
    ("target_partitions_8", 8),
];

async fn load_cayenne(rows: usize) -> CayenneFixture {
    let fixture = setup_cayenne_pk("pk_fanout_bench").await;
    let batch = make_batch(schema(), 0, rows);
    let _ = cayenne_insert(&fixture.table, batch).await;
    fixture
}

/// Run the SAME query at an EXPLICIT target_partitions setting. Mirrors
/// `common::cayenne_query` but lets the caller pick `target_partitions`.
async fn cayenne_query_with_partitions(
    table: &Arc<CayenneTableProvider>,
    sql: &str,
    target_partitions: Option<usize>,
) -> Vec<RecordBatch> {
    let mut config = SessionConfig::new();
    if let Some(n) = target_partitions {
        config = config.with_target_partitions(n);
    }
    let ctx = SessionContext::new_with_config(config);
    ctx.register_table("t", Arc::clone(table) as Arc<dyn TableProvider>)
        .expect("register table");
    let df = ctx.sql(sql).await.expect("cayenne sql");
    df.collect().await.expect("cayenne collect")
}

fn bench_target_partitions(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("pk_lookup_file_group_fanout");
    group.sample_size(20);

    for &rows in TABLE_SIZES {
        let fixture = Arc::new(rt.block_on(load_cayenne(rows)));
        let target_id = (rows / 2) as i64;
        let sql = format!("SELECT value FROM t WHERE id = {target_id}");

        // Default lane: whatever `num_cpus` picks (typically 16 on aarch64).
        let cf = Arc::clone(&fixture);
        let s = sql.clone();
        group.bench_with_input(
            BenchmarkId::new("default_target_partitions", rows),
            &rows,
            |b, &_rows| {
                b.iter(|| {
                    rt.block_on(async {
                        let batches = cayenne_query_with_partitions(&cf.table, &s, None).await;
                        black_box(batches);
                    });
                });
            },
        );

        for &(name, n) in TARGET_PARTITIONS {
            let cf = Arc::clone(&fixture);
            let s = sql.clone();
            group.bench_with_input(BenchmarkId::new(name, rows), &rows, |b, &_rows| {
                b.iter(|| {
                    rt.block_on(async {
                        let batches =
                            cayenne_query_with_partitions(&cf.table, &s, Some(n)).await;
                        black_box(batches);
                    });
                });
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_target_partitions);
criterion_main!(benches);
