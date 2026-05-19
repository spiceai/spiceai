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

//! PK point-lookup `SessionContext` cache amortization sensitivity.
//!
//! Iter 6 measured Cayenne PK lookups at ~1.91 ms wall vs DuckDB at 146 µs
//! for a 1 M-row Int64 PK. Breakdown from the captured EXPLAIN ANALYZE:
//! `time_elapsed_opening = 12.80 ms` summed across 16 file_groups. That's
//! ~0.8 ms wall per `file_group` to read the Vortex file footer.
//!
//! **But Vortex's [`DefaultFilesMetadataCache`] (50 MiB default,
//! `datafusion-execution/src/cache/cache_manager.rs:272`) is wired**:
//! `vortex-datafusion/src/persistent/format.rs:498` passes
//! `state.runtime_env().cache_manager.get_file_metadata_cache()` to the
//! opener. After the first scan, the file footer should be cached and
//! re-used by subsequent scans **on the same `SessionContext`**.
//!
//! The helper `common::cayenne_query` creates `SessionContext::new()` per
//! call. Every benchmark iteration is therefore a **cold-cache** lookup —
//! the cache is created, populated by the scan, and dropped at the end of
//! the call. The 12.80 ms summed footer-open cost re-fires every iteration.
//!
//! In production a long-lived `SessionContext` serves many queries; the
//! second through Nth point lookups against the same table see a warm
//! `FileMetadataCache` and should skip the footer read entirely.
//!
//! ## What this bench measures
//!
//! Two lanes per table size:
//!
//! - `cold_session_per_query` — mirrors `common::cayenne_query`:
//!   `SessionContext::new()` inside the timed iteration. Equivalent to today's
//!   `vs_duckdb_pk_lookup` lane.
//! - `warm_session_reused`     — one `SessionContext` created in the
//!   `setup` closure of `b.iter_batched`; criterion's `BatchSize::PerIteration`
//!   discards the setup time from the measurement so we measure the steady-state
//!   warm-cache cost of one query against one already-registered table.
//!
//! For the 1 M-row case the warm lane should be substantially lower if the
//! `FileMetadataCache` is wired end-to-end. If the two lanes are within noise
//! we have a separate problem — the cache is created but not consulted (likely
//! because Vortex's opener requires `file.has_statistics()` to populate
//! `CachedVortexMetadata`, or because each new query rebuilds the ListingTable
//! and the scan resolves a different `ObjectMeta`).
//!
//! `cargo bench --bench pk_lookup_session_cache_warmup -p cayenne --features duckdb-bench`.

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_possible_truncation)]

#[path = "vs_duckdb_helpers/common.rs"]
mod common;

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::RecordBatch;
use cayenne::CayenneTableProvider;
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use tokio::runtime::Runtime;

use common::{CayenneFixture, cayenne_insert, make_batch, schema, setup_cayenne_pk};

const TABLE_SIZES: &[usize] = &[16_384, 131_072, 1_048_576];

async fn load_cayenne(rows: usize) -> CayenneFixture {
    let fixture = setup_cayenne_pk("pk_cache_bench").await;
    let batch = make_batch(schema(), 0, rows);
    let _ = cayenne_insert(&fixture.table, batch).await;
    fixture
}

/// Build a fresh `SessionContext` and run one query — the **cold-cache**
/// path the existing `common::cayenne_query` takes.
async fn cold_session_query(
    table: &Arc<CayenneTableProvider>,
    sql: &str,
) -> Vec<RecordBatch> {
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(table) as Arc<dyn TableProvider>)
        .expect("register table");
    let df = ctx.sql(sql).await.expect("cayenne sql");
    df.collect().await.expect("cayenne collect")
}

/// Build a `SessionContext` once, register the table once, and run one
/// query against it. The caller controls reuse: by passing the same `ctx`
/// across `b.iter` calls we measure the **warm-cache** path.
async fn warm_session_query(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    let df = ctx.sql(sql).await.expect("cayenne sql");
    df.collect().await.expect("cayenne collect")
}

async fn make_warm_session(table: &Arc<CayenneTableProvider>) -> SessionContext {
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(table) as Arc<dyn TableProvider>)
        .expect("register table");
    ctx
}

fn bench_session_cache(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("pk_lookup_session_cache_warmup");
    group.sample_size(20);

    for &rows in TABLE_SIZES {
        let fixture = Arc::new(rt.block_on(load_cayenne(rows)));
        let target_id = (rows / 2) as i64;
        let sql = format!("SELECT value FROM t WHERE id = {target_id}");

        // ---- Cold lane: fresh SessionContext per iteration ----
        let cf = Arc::clone(&fixture);
        let s = sql.clone();
        group.bench_with_input(
            BenchmarkId::new("cold_session_per_query", rows),
            &rows,
            |b, &_rows| {
                b.iter(|| {
                    rt.block_on(async {
                        let batches = cold_session_query(&cf.table, &s).await;
                        black_box(batches);
                    });
                });
            },
        );

        // ---- Warm lane: SessionContext reused across iterations ----
        let cf = Arc::clone(&fixture);
        let s = sql.clone();
        // Pre-warm the cache: run one query against a held context, then
        // reuse that same context in the timed iter loop.
        let warm_ctx = rt.block_on(async {
            let ctx = make_warm_session(&cf.table).await;
            // Warm-up call — populates the FileMetadataCache.
            let _ = warm_session_query(&ctx, &s).await;
            ctx
        });
        let warm_ctx = Arc::new(warm_ctx);
        group.bench_with_input(
            BenchmarkId::new("warm_session_reused", rows),
            &rows,
            |b, &_rows| {
                let wc = Arc::clone(&warm_ctx);
                let s2 = s.clone();
                b.iter_batched(
                    || {
                        // PerIteration setup: nothing to do; the context is
                        // already warmed. Return a clone of the Arc so the
                        // routine has a cheap handle.
                        Arc::clone(&wc)
                    },
                    |ctx| {
                        rt.block_on(async {
                            let batches = warm_session_query(&ctx, &s2).await;
                            black_box(batches);
                        });
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_session_cache);
criterion_main!(benches);
