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

//! Tests that measure process-wide live bytes share a counting global allocator.
//! Each test needs its own process: nextest provides it, and the guard at the
//! start of each test enforces that requirement.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

#[expect(
    dead_code,
    reason = "shared Cayenne helpers; isolated tests use only some"
)]
mod common;

mod footer_cache_hit_counter_test;
mod inline_cache_gauge_test;
mod lookup_index_memory_test;
mod mem_tier_limit_accounting_test;

/// Requested Rust heap bytes; native allocations and mapped pages are excluded.
struct CountingAllocator;

static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
static PEAK_BYTES: AtomicUsize = AtomicUsize::new(0);

// SAFETY: every call delegates to `System` with unchanged arguments.
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

pub(crate) fn live_bytes() -> usize {
    LIVE_BYTES.load(Ordering::Relaxed)
}

pub(crate) fn peak_bytes() -> usize {
    PEAK_BYTES.load(Ordering::Relaxed)
}

pub(crate) fn reset_peak_bytes(baseline: usize) {
    PEAK_BYTES.store(baseline, Ordering::Relaxed);
}
