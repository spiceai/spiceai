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

//! Does the SQL results cache hold roughly what it reports holding?
//!
//! `max_size` is enforced against a figure the cache computes per entry, and
//! until now nothing checked that figure against the memory the process
//! actually gives up to those entries. It has been wrong by orders of magnitude
//! twice, both in shipped releases: an entry built from a `LIMIT`'s zero-copy
//! slice pinned the whole scan batch it came from
//! (<https://github.com/spiceai/spiceai/issues/12921>), and a 0-row entry was
//! billed 82 bytes while holding tens of kilobytes
//! (<https://github.com/spiceai/spiceai/issues/12931>). Both were found by
//! measuring a running `spiced`, because no test related the two numbers.
//!
//! This is that test. It drives real SQL through a real runtime — so the schema
//! and the input-table set are the ones `DataFusion` builds per plan, freshly
//! allocated per query, which is the condition that makes them worth sharing —
//! and compares the cache's own reported size against live heap bytes.
//!
//! The bar is deliberately a ratio, not a byte count. An entry is weighed before
//! the allocator has been asked for anything, so exactness is not available at
//! any price; what `max_size` needs is a figure *proportional* to what an entry
//! holds, and that is what a ratio checks.

#![cfg(not(windows))]
#![allow(clippy::expect_used)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use app::AppBuilder;
use runtime::Runtime;
use runtime_request_context::{Protocol, RequestContext, UserAgent};
use spicepod::component::dataset::Dataset;
use spicepod::acceleration::{Acceleration, Mode, RefreshMode};
use spicepod::component::runtime::{Runtime as SpicepodRuntime, TaskHistory};

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

/// Distinct queries to cache. Enough that per-entry cost dominates the noise of
/// a running runtime, few enough to stay a test rather than a benchmark.
const ENTRIES: usize = 3_000;

/// Rows in the fixture. Only needs to cover the ids the queries look up.
const FIXTURE_ROWS: usize = 4_000;

/// A CSV-backed dataset with an `id` column to look up by.
fn lookup_dataset(dir: &std::path::Path) -> Dataset {
    let csv = dir.join("lookup.csv");
    let mut body = String::from("id,payload\n");
    for i in 0..FIXTURE_ROWS {
        let _ = writeln!(body, "{i},row-{i}");
    }
    std::fs::write(&csv, body).expect("the fixture CSV should be writable");
    let mut dataset = Dataset::new(format!("file://{}", csv.display()), "lookup");
    // Accelerated in memory, refreshed once at load. An unaccelerated file
    // dataset is re-read per query and its cache entries do not accumulate, so
    // this also matches the configuration the per-entry figures were measured
    // against.
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("arrow".to_string()),
        mode: Mode::Memory,
        refresh_mode: Some(RefreshMode::Full),
        ..Acceleration::default()
    });
    dataset
}

async fn run_query(rt: &Arc<Runtime>, query: &str) {
    use futures::StreamExt;
    let mut result = rt
        .datafusion()
        .query_builder(query)
        .build()
        .run()
        .await
        .expect("the query should run");
    while let Some(batch) = result.data.next().await {
        batch.expect("the query should not error mid-stream");
    }
}

/// Fills the cache with `ENTRIES` distinct point lookups and compares what it
/// says it holds against what the process actually holds.
///
/// Every query returns 0 rows — the ids are outside the fixture — so no Arrow
/// arrays are stored and what is left is the per-entry cost the accounting has
/// historically got most wrong.
#[tokio::test]
async fn the_cache_holds_about_what_it_reports_holding() {
    let request_context = Arc::new(
        RequestContext::builder(Protocol::Internal)
            .with_user_agent(UserAgent::from_ua_str("spiceci/results_cache_growth"))
            .build(),
    );

    request_context
        .scope(async {
            let dir = tempfile::tempdir().expect("a temporary directory for the fixture");
            let app = AppBuilder::new("results_cache_growth")
                .with_dataset(lookup_dataset(dir.path()))
                // `with_runtime` replaces the whole runtime block, so it must come
                // before `with_sql_cache` or it discards the cache config.
                .with_runtime(SpicepodRuntime {
                    // A row per query would otherwise grow the heap alongside
                    // the cache and be counted as the cache's.
                    task_history: TaskHistory {
                        enabled: false,
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .with_sql_cache(spicepod::component::caching::SQLResultsCacheConfig {
                    enabled: true,
                    // Large enough that nothing is evicted, so the comparison is
                    // between what is held and what is reported, not between
                    // what survived two different eviction decisions.
                    max_size: Some("512MiB".to_string()),
                    // The default is 1s, which would expire entries mid-test —
                    // the count would then plateau at whatever a second's worth
                    // of queries is, and measure the query rate rather than the
                    // cache.
                    item_ttl: Some("10m".to_string()),
                    ..Default::default()
                })
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            tokio::time::timeout(Duration::from_mins(2), Arc::clone(&rt).load_components())
                .await
                .expect("the dataset should load within two minutes");

            // The accelerator has to finish its first refresh before a query
            // against it will run at all.
            let ready = tokio::time::timeout(Duration::from_mins(2), async {
                while !rt.status().is_ready() {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await;
            ready.expect("the runtime should report ready within two minutes");

            let cache = rt
                .datafusion()
                .results_cache_provider()
                .expect("the results cache should be configured");

            // The builder silently drops the cache config if the runtime block
            // is set after it, and the symptom is subtle: entries expire at the
            // 1s default TTL and the count plateaus at the query rate. Assert
            // the settings actually took, rather than measuring the wrong thing.
            assert_eq!(
                cache.ttl(),
                Duration::from_mins(10),
                "the configured item_ttl did not reach the cache provider"
            );
            assert_eq!(
                cache.max_size(),
                512 * 1024 * 1024,
                "the configured max_size did not reach the cache provider"
            );

            // Warm the plan and codegen paths, so their one-off allocation is
            // below the baseline rather than inside the measured window.
            for i in 0..50 {
                run_query(&rt, &format!("SELECT 1 FROM lookup WHERE id = {} LIMIT 1", 9_000_000 + i))
                    .await;
            }
            cache.run_pending_tasks().await;

            let live_before = live_bytes();
            let reported_before = cache.size().await;
            let items_before = cache.item_count().await;

            for i in 0..ENTRIES {
                run_query(
                    &rt,
                    &format!("SELECT 1 FROM lookup WHERE id = {} LIMIT 1", 1_000_000 + i),
                )
                .await;
            }

            // moka's counters are eventually consistent; settle them before
            // reading, or the reported figure lags the entries that are in fact
            // resident.
            cache.run_pending_tasks().await;

            let live_after = live_bytes();
            let reported_after = cache.size().await;
            let items_after = cache.item_count().await;

            let stored = items_after - items_before;
            assert!(
                stored >= ENTRIES as u64 * 9 / 10,
                "expected about {ENTRIES} entries to be stored, but the cache holds {stored} more \
                 than it did; the queries were not distinct or were not cached"
            );

            let reported_growth = reported_after.saturating_sub(reported_before);
            let actual_growth = live_after.saturating_sub(live_before) as u64;

            // The pools that share schemas and input-table sets across entries
            // hold real memory that no entry is charged for. It is bounded — one
            // copy per distinct shape, not one per entry — so it shows up here
            // as a fixed addend, not as growth per entry.
            let pooled = arrow_tools::schema_intern::global().stats().value_bytes as u64
                + arrow_tools::table_set_intern::global().stats().value_bytes as u64;

            println!(
                "entries {stored}: reported {reported_growth} B, actually held {actual_growth} B \
                 ({} B/entry reported, {} B/entry held), pooled and uncharged {pooled} B",
                reported_growth / stored,
                actual_growth / stored,
            );

            assert!(
                actual_growth <= reported_growth * 3,
                "the cache reports {reported_growth} B but the process gave up {actual_growth} B \
                 to those {stored} entries — more than 3x what the budget was told, so `max_size` \
                 does not bound what the cache costs"
            );
            assert!(
                reported_growth <= actual_growth * 3,
                "the cache reports {reported_growth} B but the process only gave up \
                 {actual_growth} B for {stored} entries — the budget is over-charging by more \
                 than 3x, which evicts entries that would have fit"
            );

            Ok::<(), anyhow::Error>(())
        })
        .await
        .expect("the growth comparison should complete");
}
