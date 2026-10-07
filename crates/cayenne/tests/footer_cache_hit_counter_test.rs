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

//! Does the file metadata cache give back everything an evicted footer held?
//!
//! Cayenne's Vortex scans read file footers through `DataFusion`'s file metadata
//! cache, and Cayenne writes every refresh and compaction under fresh paths, so a
//! path is cached once and evicted once. The cache keeps a hit counter per entry
//! in a map beside its LRU queue, outside its own memory accounting. When eviction
//! popped the queue entry and left the counter behind, the map gained one
//! `(Path, usize)` pair per file the process had ever read, for the life of the
//! process — 7.6 MiB to 27.0 MiB of unaccounted heap over 10,000 refreshes at a
//! constant 1,600 live entries (spiceai/datafusion#245, for #12952). `DataFusion`
//! 55's generic `DefaultCache` prunes the map in `evict_entries`; this pins that.
//!
//! The leak is invisible to the cache's API: `list_entries` and `memory_used`
//! report only the queue. So a counting allocator measures the heap across a
//! stream of cold reads, each a missed `get` and a `put` of a new path, after the
//! cache is full: the live entries and the bytes they hold are then constant, so
//! anything that grows is held outside the accounting.
//!
//! It is its own integration binary because it installs a global allocator, and
//! it is one test because the counter is process-wide.

use std::alloc::{GlobalAlloc, Layout, System};
use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use chrono::{DateTime, Utc};
use datafusion_common::HashMap;
use datafusion_execution::cache::Cache;
use datafusion_execution::cache::cache_manager::{CachedFileMetadataEntry, FileMetadata};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use object_store::ObjectMeta;
use object_store::path::Path;

struct CountingAllocator;

static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);

// SAFETY: delegates every call to `System` and only adds bookkeeping.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded unchanged.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            LIVE_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        // SAFETY: forwarded unchanged.
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: forwarded unchanged.
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

/// What one cached footer reports to the cache. Its real allocation is tiny, so
/// the measurement is of the cache's own bookkeeping rather than of footers.
const FOOTER_BYTES: usize = 8 * 1024;
/// The cache's limit: room for a few dozen footers.
const CACHE_LIMIT: usize = 256 * 1024;
/// Cold reads that fill the cache before the measurement starts.
const WARM_UP_READS: usize = 2_000;
/// Cold reads inside the measurement.
const MEASURED_READS: usize = 20_000;
/// The live-heap growth the measured reads may cause. A counter left behind per
/// evicted path costs at least the path's bytes — over 100 here — so the leak
/// is more than 2 MB, and steady state is no growth at all.
const MAX_GROWTH: usize = 64 * 1024;

struct Footer;

impl FileMetadata for Footer {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn memory_size(&self) -> usize {
        FOOTER_BYTES
    }

    fn extra_info(&self) -> HashMap<String, String> {
        HashMap::new()
    }
}

/// The path of the `n`th file, shaped like a Cayenne data file under a fresh
/// refresh directory, so every read is of a path the cache has never seen.
fn data_file(n: usize) -> Path {
    Path::from(format!(
        "cayenne/data/orders/0199a1b2-c3d4-7e5f-8a9b-{n:012x}/part-00000/data-00000.vortex"
    ))
}

/// A cold footer read the way Cayenne's Vortex scan makes it: a lookup that
/// misses, then the footer read from the file put into the cache.
fn read_footer(cache: &dyn Cache<Path, CachedFileMetadataEntry>, n: usize) {
    let location = data_file(n);
    assert!(
        cache.get(&location).is_none(),
        "{location} is a fresh path, so its lookup has to miss"
    );
    let meta = ObjectMeta {
        location: location.clone(),
        last_modified: DateTime::<Utc>::UNIX_EPOCH,
        size: 1_048_576,
        e_tag: None,
        version: None,
    };
    cache.put(
        &location,
        CachedFileMetadataEntry::new(meta, Arc::new(Footer)),
    );
}

#[test]
fn an_evicted_footer_leaves_no_hit_counter_behind() {
    // The runtime builds its environment this way, with
    // `runtime.query.metadata_cache_limit` when one is set.
    let runtime_env = RuntimeEnvBuilder::default()
        .with_metadata_cache_limit(CACHE_LIMIT)
        .build_arc()
        .expect("build the runtime environment");
    let cache = runtime_env.cache_manager.get_file_metadata_cache();
    assert_eq!(
        cache.cache_limit(),
        CACHE_LIMIT,
        "the environment has to hand out the cache the limit was set on"
    );

    for n in 0..WARM_UP_READS {
        read_footer(cache.as_ref(), n);
    }
    let resident = cache.len();
    assert!(
        resident > 1 && resident < WARM_UP_READS / 10,
        "the cache has to be full and evicting before the measurement, or it measures \
         nothing; it holds {resident} footers after {WARM_UP_READS} reads"
    );

    let before = live_bytes();
    for n in WARM_UP_READS..WARM_UP_READS + MEASURED_READS {
        read_footer(cache.as_ref(), n);
    }
    let after = live_bytes();

    assert_eq!(
        cache.len(),
        resident,
        "a full cache evicts one footer per footer it admits"
    );
    let growth = after.saturating_sub(before);
    eprintln!(
        "{MEASURED_READS} cold footer reads, {resident} footers resident: live heap \
         {before} -> {after} bytes (growth {growth}, allowed {MAX_GROWTH})"
    );
    assert!(
        growth <= MAX_GROWTH,
        "{MEASURED_READS} cold footer reads into a full cache of {resident} footers grew the \
         live heap by {growth} bytes ({before} -> {after}), where a cache that frees what it \
         evicts stays flat: the cache keeps something per evicted path, outside the memory \
         limit it enforces"
    );
}
