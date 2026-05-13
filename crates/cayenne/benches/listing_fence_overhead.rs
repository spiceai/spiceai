// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0

//! Microbenchmark for the `listing_table` synchronization pattern in
//! `CayenneTableProvider::scan()`.
//!
//! Step 3 of issue #10125 replaced `Arc<std::sync::RwLock<Arc<ListingTable>>>`
//! with `Arc<ArcSwap<ListingTable>>` + a separate
//! `Arc<tokio::sync::RwLock<()>>` fence. The previous "grab Arc under brief
//! sync guard, drop guard, then `.scan().await`" pattern is replaced with a
//! fence read held across the `.scan().await` so that concurrent writer
//! barriers cannot interleave with the listing operation.
//!
//! This bench isolates the per-scan synchronization cost of each pattern
//! (no DataFusion listing, no I/O, no filesystem state — just the lock
//! primitives) and runs both inside the same tokio runtime, so the `.await`
//! that the new pattern requires does not double-count `Runtime::block_on`
//! overhead.

#![allow(clippy::expect_used)]

use std::hint::black_box;
use std::sync::Arc;

use arc_swap::ArcSwap;
use criterion::{Criterion, criterion_group, criterion_main};
use tokio::runtime::Runtime;

/// Stand-in for `Arc<ListingTable>` so the comparison focuses on lock cost
/// rather than the size of the inner type.
type Inner = String;

fn make_inner() -> Arc<Inner> {
    Arc::new("listing_table_placeholder".to_string())
}

// ----------------------------------------------------------------------------
// Uncontended single-task access.
// ----------------------------------------------------------------------------
//
// Steady-state read path: one task calling scan() with no concurrent reader
// or writer. Measures the per-call overhead of acquiring the synchronization
// primitive + loading the Arc. Both patterns run inside the same tokio
// runtime via `to_async` so the async pattern doesn't pay block_on overhead.

fn bench_uncontended_old_pattern(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let lock: Arc<std::sync::RwLock<Arc<Inner>>> =
        Arc::new(std::sync::RwLock::new(make_inner()));

    c.bench_function("uncontended/old_sync_rwlock_then_arc_clone", |b| {
        b.to_async(&rt).iter(|| async {
            let guard = lock.read().expect("read");
            let snapshot = Arc::clone(&guard);
            drop(guard);
            black_box(snapshot);
        });
    });
}

fn bench_uncontended_new_pattern(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let arc_swap: Arc<ArcSwap<Inner>> = Arc::new(ArcSwap::new(make_inner()));
    let fence: Arc<tokio::sync::RwLock<()>> = Arc::new(tokio::sync::RwLock::new(()));

    c.bench_function("uncontended/new_fence_read_then_arcswap_load", |b| {
        b.to_async(&rt).iter(|| async {
            let _fence_guard = fence.read().await;
            let snapshot = arc_swap.load_full();
            black_box(snapshot);
        });
    });
}

// ----------------------------------------------------------------------------
// Concurrent-reader access, no writer.
// ----------------------------------------------------------------------------
//
// Multi-tenant steady state: several scans share the same partition. Both
// std::sync::RwLock and tokio::sync::RwLock allow parallel readers, but each
// adds different atomic-counter overhead per acquisition.

fn bench_concurrent_readers_old_pattern(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let lock: Arc<std::sync::RwLock<Arc<Inner>>> =
        Arc::new(std::sync::RwLock::new(make_inner()));

    // Background reader that keeps a read guard outstanding most of the time.
    let bg_lock = Arc::clone(&lock);
    let bg = std::thread::spawn(move || {
        loop {
            let _guard = bg_lock.read().expect("bg read");
            std::thread::yield_now();
            if Arc::strong_count(&bg_lock) == 1 {
                break;
            }
        }
    });

    c.bench_function("concurrent_readers/old_sync_rwlock", |b| {
        b.to_async(&rt).iter(|| async {
            let guard = lock.read().expect("read");
            let snapshot = Arc::clone(&guard);
            drop(guard);
            black_box(snapshot);
        });
    });

    drop(lock);
    let _ = bg.join();
}

fn bench_concurrent_readers_new_pattern(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let arc_swap: Arc<ArcSwap<Inner>> = Arc::new(ArcSwap::new(make_inner()));
    let fence: Arc<tokio::sync::RwLock<()>> = Arc::new(tokio::sync::RwLock::new(()));

    let bg_fence = Arc::clone(&fence);
    let bg = std::thread::spawn(move || {
        let rt = Runtime::new().expect("bg runtime");
        rt.block_on(async {
            loop {
                let _fence_guard = bg_fence.read().await;
                tokio::task::yield_now().await;
                if Arc::strong_count(&bg_fence) == 1 {
                    break;
                }
            }
        });
    });

    c.bench_function("concurrent_readers/new_fence_arcswap", |b| {
        b.to_async(&rt).iter(|| async {
            let _fence_guard = fence.read().await;
            let snapshot = arc_swap.load_full();
            black_box(snapshot);
        });
    });

    drop(fence);
    let _ = bg.join();
}

criterion_group!(
    benches,
    bench_uncontended_old_pattern,
    bench_uncontended_new_pattern,
    bench_concurrent_readers_old_pattern,
    bench_concurrent_readers_new_pattern,
);
criterion_main!(benches);
