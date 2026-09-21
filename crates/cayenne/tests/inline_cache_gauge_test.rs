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

//! Does `cayenne_inline_cache_bytes` report what the inline cache actually holds?
//!
//! The gauge is what an operator attributing resident memory reads, and nothing
//! related it to the memory the process gives up. It was computed by summing
//! `RecordBatch::get_array_memory_size` over the cached batches, which sums
//! `Buffer::capacity()` once per buffer *reference* — and `capacity()` is the
//! whole parent allocation however narrow a slice the reference covers. Every
//! buffer in an IPC-decoded batch is a slice of the one allocation the message
//! body was read into, so the gauge billed that allocation once per buffer and
//! reported ~19x what a real table's cache held: 554 MB against a 211 MB
//! process heap.
//!
//! The assertion is that impossibility, measured rather than argued: a cache
//! cannot hold more than the process allocated for it. The comparison is
//! deliberately one-sided and slack — live heap growth also carries the
//! serialized envelopes and the metastore's own work, which the cache is not
//! charged for — because what has to be caught is an over-report by an order of
//! magnitude, not a rounding difference.

#![cfg(not(windows))]
#![allow(clippy::expect_used)]

mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::CreateTableOptions;
use cayenne::{CayenneTableProvider, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;

/// Live heap bytes: everything allocated and not yet freed.
///
/// Each integration-test file is its own binary and nextest runs a process per
/// test, so this counts only this test's own work.
struct CountingAllocator;

static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            LIVE_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
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
            LIVE_BYTES.fetch_add(new_size, Ordering::Relaxed);
            LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        }
        new_ptr
    }
}

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator;

fn live_bytes() -> usize {
    LIVE_BYTES.load(Ordering::Relaxed)
}

/// Appends, each landing as its own inline entry and its own decoded batch.
///
/// Sized against the checkpoint thresholds, which is what bounds how much a
/// cache can hold: `DEFAULT_INLINE_FLUSH_MAX_SEGMENTS` (64 entries) and
/// `DEFAULT_INLINE_FLUSH_MAX_ROWS` (10,000) both trip well before
/// `DEFAULT_INLINE_FLUSH_MAX_BYTES`, and a checkpoint would flush the corpus to
/// a file and empty the cache mid-measurement. Two warm-up entries and 55
/// measured ones, at 160 rows each, sit under every threshold with room to
/// spare.
const WARMUP_APPENDS: usize = 2;
const APPENDS: usize = 55;
const ROWS_PER_APPEND: usize = 160;

/// A row's fixed value payload: seven `i64`s and four strings of this width.
const STRING_WIDTH: usize = 32;
const ROW_PAYLOAD_BYTES: usize = 7 * 8 + 4 * STRING_WIDTH;

/// Eleven columns, nineteen Arrow buffers — the buffer count per batch is
/// exactly the factor the old sum over-reported by, so a narrower fixture would
/// understate the bug.
fn wide_schema() -> Arc<Schema> {
    let mut fields = vec![Field::new("id", DataType::Int64, false)];
    for i in 0..2 {
        fields.push(Field::new(format!("plain_{i}"), DataType::Int64, false));
    }
    for i in 0..4 {
        fields.push(Field::new(format!("nullable_{i}"), DataType::Int64, true));
    }
    for i in 0..4 {
        fields.push(Field::new(format!("text_{i}"), DataType::Utf8, false));
    }
    Arc::new(Schema::new(fields))
}

/// The first id of the `entry`-th append.
fn entry_first_id(entry: usize) -> i64 {
    i64::try_from(entry * ROWS_PER_APPEND).expect("the id range fits in i64")
}

/// One inline entry's worth of rows, with ids from `first_id`.
fn rows(schema: &Arc<Schema>, first_id: i64) -> RecordBatch {
    let count = i64::try_from(ROWS_PER_APPEND).expect("the row count fits in i64");
    let ids: Vec<i64> = (first_id..first_id + count).collect();
    let mut columns: Vec<arrow::array::ArrayRef> = vec![Arc::new(Int64Array::from(ids.clone()))];
    for i in 0..2_i64 {
        columns.push(Arc::new(Int64Array::from(
            ids.iter().map(|id| id * (i + 2)).collect::<Vec<_>>(),
        )));
    }
    for i in 0..4_i64 {
        // A mix of null and present, so the validity buffers are materialized.
        columns.push(Arc::new(Int64Array::from(
            ids.iter()
                .map(|id| (id % (i + 2) == 0).then_some(*id))
                .collect::<Vec<_>>(),
        )));
    }
    let width = STRING_WIDTH;
    for _ in 0..4 {
        columns.push(Arc::new(StringArray::from(
            ids.iter()
                .map(|id| format!("{id:0>width$}"))
                .collect::<Vec<_>>(),
        )));
    }
    RecordBatch::try_new(Arc::clone(schema), columns).expect("the fixture rows should build")
}

#[test]
fn the_inline_cache_reports_no_more_than_the_process_holds() -> Result<(), String> {
    common::run_with_backend_blocking(common::BackendType::Sqlite, |fixture| async move {
        let schema = wide_schema();
        let ctx = SessionContext::new();
        let table = Arc::new(
            CayenneTableProvider::create_table(
                Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
                CreateTableOptions {
                    table_name: "inline_cache_gauge".to_string(),
                    schema: Arc::clone(&schema),
                    primary_key: vec![],
                    on_conflict: None,
                    base_path: fixture.data_path.to_string_lossy().to_string(),
                    partition_column: None,
                    vortex_config: cayenne::metadata::VortexConfig::default(),
                },
                ctx.runtime_env(),
            )
            .await?,
        );
        ctx.register_table(
            "inline_cache_gauge",
            Arc::clone(&table) as Arc<dyn TableProvider>,
        )?;

        // Warm the write and scan paths, so their one-off allocation — plan
        // structures, the metastore's own pages, the decoder's scratch — is
        // below the baseline rather than inside the measured window.
        for entry in 0..WARMUP_APPENDS {
            common::insert_batch(&table, rows(&schema, entry_first_id(entry))).await?;
        }
        drop(
            ctx.sql("SELECT * FROM inline_cache_gauge")
                .await?
                .collect()
                .await?,
        );

        let live_before = live_bytes();
        let reported_before = table.inline_cache_resident_bytes();

        for entry in 0..APPENDS {
            common::insert_batch(&table, rows(&schema, 1_000_000 + entry_first_id(entry))).await?;
        }
        // The cache is populated by a scan, not by the write: read the corpus
        // back so the batches under measurement are the ones a query serves.
        let scanned = ctx
            .sql("SELECT * FROM inline_cache_gauge")
            .await?
            .collect()
            .await?;
        let scanned_rows: usize = scanned.iter().map(RecordBatch::num_rows).sum();
        drop(scanned);

        let live_after = live_bytes();
        let sample_started = std::time::Instant::now();
        let reported_after = table.inline_cache_resident_bytes();
        let sample_took = sample_started.elapsed();

        let reported_growth = reported_after.saturating_sub(reported_before);
        let live_growth =
            u64::try_from(live_after.saturating_sub(live_before)).expect("live bytes fit in u64");
        let inserted_payload = u64::try_from(APPENDS * ROWS_PER_APPEND * ROW_PAYLOAD_BYTES)
            .expect("the inserted payload fits in u64");
        let cached_batches = APPENDS + WARMUP_APPENDS;

        println!(
            "{APPENDS} appends of {ROWS_PER_APPEND} rows ({scanned_rows} rows visible): the gauge \
             grew by {reported_growth} B, the process by {live_growth} B; the rows themselves \
             carry {inserted_payload} B of values. One sample over {cached_batches} batches took \
             {sample_took:?}."
        );

        assert_eq!(
            scanned_rows,
            (APPENDS + WARMUP_APPENDS) * ROWS_PER_APPEND,
            "the appends did not all land inline and visible, so the cache under measurement is \
             not the corpus this test thinks it is"
        );

        // The cache holds the corpus, so it cannot report less than the values
        // in it — the guard against a dedupe that over-matches and under-reports.
        assert!(
            reported_growth >= inserted_payload,
            "the cache reports {reported_growth} B for {APPENDS} entries carrying \
             {inserted_payload} B of values; either it is under-reporting what it holds, or a \
             checkpoint flushed the corpus out of it mid-measurement"
        );

        // The decisive one. Live growth is everything the appends left resident
        // — the decoded batches AND the serialized envelopes beside them AND the
        // metastore's pages — so the cache alone exceeding it is impossible, and
        // the old per-reference sum exceeded it several times over.
        assert!(
            reported_growth <= live_growth,
            "the inline cache reports {reported_growth} B but the process only gave up \
             {live_growth} B in total across those {APPENDS} appends — the cache is being \
             charged for the same allocation once per buffer that points into it"
        );

        Ok::<(), Box<dyn std::error::Error>>(())
    })
}
