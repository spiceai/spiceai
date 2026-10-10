/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Measures Rust allocator live bytes while Cayenne reopens persisted indexes.
//! Native allocations and memory-mapped pages are outside this measurement.

#![expect(clippy::expect_used, reason = "test setup and assertions")]

#[expect(
    dead_code,
    reason = "shared helpers for every Cayenne test binary; this one uses only some"
)]
mod common;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::lookup_index::IndexPersistence;
use cayenne::metadata::VortexConfig;
use common::lookup_index::{
    TableSpec, counters, insert, int64_column, open_table, overwrite, query, runtime_with_pool,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use cayenne::MetadataCatalog;
use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;

/// Requested bytes allocated through Rust and not yet freed.
///
/// Each integration-test file is its own binary and nextest runs a process per
/// test, so this counts only this test's own work.
struct CountingAllocator;

static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
static PEAK_BYTES: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let live = LIVE_BYTES.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK_BYTES.fetch_max(live, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            let live = if new_size >= layout.size() {
                let added = new_size - layout.size();
                LIVE_BYTES.fetch_add(added, Ordering::Relaxed) + added
            } else {
                let removed = layout.size() - new_size;
                LIVE_BYTES.fetch_sub(removed, Ordering::Relaxed) - removed
            };
            PEAK_BYTES.fetch_max(live, Ordering::Relaxed);
        }
        new_ptr
    }
}

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator;

fn live_bytes() -> usize {
    LIVE_BYTES.load(Ordering::Relaxed)
}

/// Records actual allocation growth at the point the engine requests admission.
#[derive(Debug)]
struct ObservingPool {
    inner: GreedyMemoryPool,
    baseline: usize,
}

impl std::fmt::Display for ObservingPool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("persisted-index memory observation")
    }
}

impl MemoryPool for ObservingPool {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }
    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }
    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
    }
    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
    }
    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> datafusion_common::Result<()> {
        let result = self.inner.try_grow(reservation, additional);
        if result.is_err() {
            println!(
                "admission refused: consumer={} reserved={} requested_additional={additional} live_rust_growth={}",
                reservation.consumer().name(),
                self.reserved(),
                live_bytes().saturating_sub(self.baseline)
            );
        }
        result
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn opening_persisted_runs_respects_the_memory_budget() {
    const MIB: usize = 1024 * 1024;
    const ROWS: i64 = 150_000;
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let indexes: &[&[&str]] = &[&["id"]];
    let spec = || TableSpec {
        name: "persisted_heap",
        schema: Arc::clone(&schema),
        indexes,
        upsert_key: None,
        config: VortexConfig::default(),
        persistence: Some(IndexPersistence::Enabled),
    };
    let (writer_env, writer_pool) = runtime_with_pool(1024 * MIB);
    let table = open_table(&fixture, writer_env, spec()).await;
    for part in 0..3 {
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(
                part * ROWS..(part + 1) * ROWS,
            ))],
        )
        .expect("batch");
        if part == 0 {
            overwrite(&table, vec![batch]).await;
        } else {
            insert(&table, "persisted_heap", batch).await;
        }
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let metadata = fixture
            .catalog
            .get_table("persisted_heap")
            .await
            .expect("table");
        let runs = fixture
            .catalog
            .list_index_runs(&metadata.table_id)
            .await
            .expect("runs");
        if runs.len() >= 3 {
            println!(
                "persisted fixture: runs={} encoded_bytes={}",
                runs.len(),
                runs.iter().map(|run| run.size_bytes).sum::<u64>()
            );
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "runs did not persist"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    drop(table);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while writer_pool.reserved() != 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "writer reservations still live: {}",
            writer_pool.reserved()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Catalog sizes are advisory; admission must use the object returned by
    // the store. Deliberately under-report them for every reopening below.
    let metastore = rusqlite::Connection::open(fixture.db_path()).expect("metastore");
    assert_eq!(
        metastore
            .execute("UPDATE cayenne_index_run SET size_bytes = 1", [])
            .expect("under-report sizes"),
        3
    );
    drop(metastore);
    // The first budget refuses the encoded body. The second can hold one
    // encoded run, but cannot hold all decoded runs and their working memory.
    for limit in [MIB, 4 * MIB] {
        let baseline = live_bytes();
        let pool = Arc::new(ObservingPool {
            inner: GreedyMemoryPool::new(limit),
            baseline,
        });
        let env = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool) as Arc<dyn MemoryPool>)
            .build_arc()
            .expect("observed runtime");
        PEAK_BYTES.store(baseline, Ordering::Relaxed);
        let reopened = open_table(&fixture, env, spec()).await;
        assert_eq!(
            counters(&reopened).index_bytes,
            0,
            "loaded runs must be refused"
        );
        let peak = PEAK_BYTES.load(Ordering::Relaxed).saturating_sub(baseline);
        println!(
            "persisted reopen Rust allocator: baseline={baseline} peak_growth={peak} pool_reserved={} limit={limit}",
            pool.reserved()
        );
        // Catalog and provider initialization are outside the index reservation.
        assert!(
            peak <= limit + MIB,
            "persisted decoding exceeded the budget: {peak}"
        );
        drop(reopened);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while pool.reserved() != 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "reopen reservations still live"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    let (env, pool) = runtime_with_pool(64 * MIB);
    let reopened = open_table(&fixture, env, spec()).await;
    let resident_bytes = counters(&reopened).index_bytes;
    assert!(
        resident_bytes > 0,
        "bounded successful load must publish runs"
    );
    assert!(pool.reserved() >= usize::try_from(resident_bytes).expect("index bytes fit"));
    let verification = reopened
        .verify_lookup_index_against_read_back()
        .await
        .expect("verify loaded runs");
    println!(
        "bounded successful reopen: {verification:?} reserved={} indexed={resident_bytes}",
        pool.reserved()
    );
    assert!(verification.agrees(), "{verification:?}");
    assert_eq!(verification.uncovered_files, 0);
    assert_eq!(
        int64_column(
            &query(
                &reopened,
                "persisted_heap",
                "SELECT id FROM persisted_heap WHERE id = 7"
            )
            .await
        ),
        vec![7]
    );
}
