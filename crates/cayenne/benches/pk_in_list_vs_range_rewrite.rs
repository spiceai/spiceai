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

//! `IN-list` vs equivalent `BETWEEN` range — IN-list-to-range rewrite
//! opportunity.
//!
//! `vs_duckdb_pk_lookup/1048576` reports:
//!   - `pk_range` (`id BETWEEN 524272 AND 524303`):  ~2.16 ms (Cayenne) vs 284 µs (DuckDB)
//!   - `pk_in_list` (`id IN (524272, ..., 524303)`):  ~3.28 ms (Cayenne) vs 307 µs (DuckDB)
//!
//! **For 32 *consecutive* integer keys, IN-list is ~50 % slower than the
//! semantically-identical range.** Both produce the same 32 result rows; the
//! file-prune and chunk-pruning behaviour should be identical. The wall-time
//! delta is therefore the per-row predicate evaluation cost:
//!
//!   - Range: two `i64` comparisons per row.
//!   - IN-list: a 32-element set membership check per row.
//!
//! With Vortex chunk pruning narrowing to 1-2 row groups (~8 K rows each),
//! that's ~16 K comparisons (range) vs ~256 K comparisons (IN-list) per
//! query — explains the 1.1 ms wall delta.
//!
//! **The fix:** detect IN-lists of *consecutive* integers and rewrite to
//! `BETWEEN`. The classical condition: integer-typed column with a sorted
//! list of length N where `list[N-1] - list[0] + 1 == N` and no duplicates.
//! This is a logical-optimizer rule (or a `unwrap_cast_in_comparison`-style
//! analyzer pass).
//!
//! ## What this bench measures
//!
//! Three lanes per table size, each retrieves the same 32 rows:
//!
//! 1. `in_list_consecutive`  — `WHERE id IN (k, k+1, ..., k+31)`.
//!    Current Cayenne path; the rewrite candidate.
//! 2. `between_range`        — `WHERE id BETWEEN k AND k+31`.
//!    The post-rewrite path; the "after" measurement.
//! 3. `in_list_sparse`       — `WHERE id IN (k, k+1024, k+2048, ...)`.
//!    32 keys spread over a wide range; **not** rewritable to a single
//!    range. The cost floor for IN-list when the optimization doesn't
//!    apply. Confirms that the optimization is a strict win only for
//!    dense IN-lists.
//!
//! Hypothesis: `between_range < in_list_consecutive`, and the rewrite
//! captures the gap. `in_list_sparse` should be similar to or worse than
//! `in_list_consecutive`, validating that the sparseness check is needed.
//!
//! `cargo bench --bench pk_in_list_vs_range_rewrite -p cayenne --features duckdb-bench`.

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
use datafusion::prelude::SessionContext;
use tokio::runtime::Runtime;

use common::{CayenneFixture, cayenne_insert, make_batch, schema, setup_cayenne_pk};

const TABLE_SIZES: &[usize] = &[131_072, 1_048_576];
const KEY_BATCH: i64 = 32;

async fn load_cayenne(rows: usize) -> CayenneFixture {
    let fixture = setup_cayenne_pk("pk_in_list_rewrite").await;
    let batch = make_batch(schema(), 0, rows);
    let _ = cayenne_insert(&fixture.table, batch).await;
    fixture
}

async fn run_query(table: &Arc<CayenneTableProvider>, sql: &str) -> Vec<RecordBatch> {
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(table) as Arc<dyn TableProvider>)
        .expect("register table");
    let df = ctx.sql(sql).await.expect("cayenne sql");
    df.collect().await.expect("cayenne collect")
}

fn bench_in_list_vs_range(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("pk_in_list_vs_range_rewrite");
    group.sample_size(20);

    for &rows in TABLE_SIZES {
        let fixture = Arc::new(rt.block_on(load_cayenne(rows)));
        let target_id = (rows / 2) as i64;
        let lo = target_id - KEY_BATCH / 2;
        let hi_inclusive = lo + KEY_BATCH - 1;

        // Consecutive IN-list: matches BETWEEN exactly. The rewrite candidate.
        let consecutive_keys: Vec<String> = (lo..=hi_inclusive).map(|i| i.to_string()).collect();
        let consecutive_in_list = consecutive_keys.join(",");
        let consecutive_sql =
            format!("SELECT SUM(value) FROM t WHERE id IN ({consecutive_in_list})");

        // Equivalent range — the post-rewrite shape.
        let range_sql =
            format!("SELECT SUM(value) FROM t WHERE id BETWEEN {lo} AND {hi_inclusive}");

        // Sparse IN-list with the same 32 keys spread over a wide range.
        // Same result-row count is not the goal here — what matters is the
        // per-row evaluation cost shape. We use one key from each of 32
        // chunks across the table's id space so the pruner cannot collapse.
        let stride = (rows as i64 / KEY_BATCH).max(1);
        let sparse_keys: Vec<String> = (0..KEY_BATCH).map(|i| (i * stride).to_string()).collect();
        let sparse_in_list = sparse_keys.join(",");
        let sparse_sql = format!("SELECT SUM(value) FROM t WHERE id IN ({sparse_in_list})");

        let cf = Arc::clone(&fixture);
        let s = consecutive_sql.clone();
        group.bench_with_input(
            BenchmarkId::new("in_list_consecutive", rows),
            &rows,
            |b, &_rows| {
                b.iter(|| {
                    rt.block_on(async {
                        let batches = run_query(&cf.table, &s).await;
                        black_box(batches);
                    });
                });
            },
        );

        let cf = Arc::clone(&fixture);
        let s = range_sql.clone();
        group.bench_with_input(
            BenchmarkId::new("between_range", rows),
            &rows,
            |b, &_rows| {
                b.iter(|| {
                    rt.block_on(async {
                        let batches = run_query(&cf.table, &s).await;
                        black_box(batches);
                    });
                });
            },
        );

        let cf = Arc::clone(&fixture);
        let s = sparse_sql.clone();
        group.bench_with_input(
            BenchmarkId::new("in_list_sparse", rows),
            &rows,
            |b, &_rows| {
                b.iter(|| {
                    rt.block_on(async {
                        let batches = run_query(&cf.table, &s).await;
                        black_box(batches);
                    });
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_in_list_vs_range);
criterion_main!(benches);
