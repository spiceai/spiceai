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

//! Regression bench: per-snapshot `Arc<DeletionIndex::empty()>` allocation in
//! `CayenneTableProvider::apply_partial_deletion_filter`
//! (`src/provider/table.rs:7535,7555`).
//!
//! Today, every protected-snapshot scan path allocates a fresh empty deletion
//! index just to satisfy `Int64PkDeletionFilterExec::new`'s
//! `insert_records: Arc<DeletionIndex>` parameter:
//!
//! ```ignore
//! let empty_insert_records = Arc::new(DeletionIndex::empty());
//! Ok(Arc::new(Int64PkDeletionFilterExec::new(
//!     plan,
//!     Arc::clone(deleted_pk_values),
//!     empty_insert_records,
//!     pk_column_index,
//!     Some(min_delete_seq_to_apply),
//! )))
//! ```
//!
//! The empty index is identical every time and immutable. Each construction
//! allocates:
//!
//! - One `Arc<HashMap<i64, i64>>` header + the empty `HashMap` (no buckets
//!   until first insert) — but the `Arc` allocator metadata is still ~24 B
//!   per call.
//! - One `BloomFilter` sized at `MIN_BLOOM_CAPACITY = 64` — `bits / 8` ≈ 8 B
//!   payload plus the `Vec` allocation header.
//! - The outer `Arc<DeletionIndex>` wrapper.
//!
//! With N protected snapshots and Q scans per second, that's `2 × N × Q`
//! heap allocations per second whose contents are bit-identical.
//!
//! Cayenne's protected-snapshot count typically grows to a handful between
//! compactions, but high-QPS workloads (point lookups, multi-table joins
//! that scan the same table several times) compound this — 5 snapshots ×
//! 5 K QPS ≈ 50 K wasted allocations/sec.
//!
//! ## What this bench measures
//!
//! Pure CPU shape. Two lanes:
//!
//! - `current_per_call_alloc` — mirrors today's `apply_partial_deletion_filter`
//!   line 7535: `Arc::new(DeletionIndex::empty())` per call.
//! - `shared_static_arc` — proposed fix: hold the empty index in a
//!   `LazyLock<Arc<DeletionIndex>>` and `Arc::clone(&EMPTY_INDEX)` per call.
//!
//! Both produce a semantically-identical `Arc<DeletionIndex>`. The difference
//! is one heap allocation (data + bloom + outer Arc metadata) per call vs a
//! single atomic refcount increment.
//!
//! Parameterised by `protected_snapshots_per_scan` to model the per-scan
//! amplification a query with N snapshots incurs (the filter is applied
//! once per snapshot, so the alloc fires N times per query).
//!
//! `cargo bench --bench apply_partial_filter_empty_alloc -p cayenne`.

#![allow(clippy::expect_used)]

use std::hint::black_box;
use std::sync::Arc;
use std::sync::LazyLock;

use cayenne::provider::deletion_index::{DeletionIndex, KeyDeletionIndex};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

/// Per-scan amplification — how many protected snapshots the filter wraps.
/// 1 is the no-amplification floor; 4 is the typical
/// `compaction_trigger_protected_snapshots` warn threshold; 16 models a busy
/// table that hasn't compacted in a while.
const SNAPSHOTS_PER_SCAN: &[usize] = &[1, 4, 16];

/// Process-wide empty index shared across every
/// `apply_partial_deletion_filter` call site. Mirrors the proposed
/// implementation: built once at first access, cloned via `Arc::clone`
/// afterwards.
static EMPTY_DELETION_INDEX: LazyLock<Arc<DeletionIndex>> =
    LazyLock::new(|| Arc::new(DeletionIndex::empty()));
static EMPTY_KEY_DELETION_INDEX: LazyLock<Arc<KeyDeletionIndex>> =
    LazyLock::new(|| Arc::new(KeyDeletionIndex::empty()));

fn bench_int64_pk(c: &mut Criterion) {
    let mut group = c.benchmark_group("apply_partial_filter_empty_alloc_int64");
    for &n in SNAPSHOTS_PER_SCAN {
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(
            BenchmarkId::new("current_per_call_alloc", n),
            &n,
            |b, &snapshots| {
                b.iter(|| {
                    for _ in 0..snapshots {
                        let arc = Arc::new(DeletionIndex::empty());
                        black_box(arc);
                    }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("shared_static_arc", n),
            &n,
            |b, &snapshots| {
                // Touch the LazyLock once outside the iter so the bench
                // measures only the steady-state Arc::clone cost (mirrors
                // long-running process behaviour).
                let _warmup = Arc::clone(&EMPTY_DELETION_INDEX);
                b.iter(|| {
                    for _ in 0..snapshots {
                        let arc = Arc::clone(&EMPTY_DELETION_INDEX);
                        black_box(arc);
                    }
                });
            },
        );
    }
    group.finish();
}

fn bench_row_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("apply_partial_filter_empty_alloc_row_key");
    for &n in SNAPSHOTS_PER_SCAN {
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(
            BenchmarkId::new("current_per_call_alloc", n),
            &n,
            |b, &snapshots| {
                b.iter(|| {
                    for _ in 0..snapshots {
                        let arc = Arc::new(KeyDeletionIndex::empty());
                        black_box(arc);
                    }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("shared_static_arc", n),
            &n,
            |b, &snapshots| {
                let _warmup = Arc::clone(&EMPTY_KEY_DELETION_INDEX);
                b.iter(|| {
                    for _ in 0..snapshots {
                        let arc = Arc::clone(&EMPTY_KEY_DELETION_INDEX);
                        black_box(arc);
                    }
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_int64_pk, bench_row_key);
criterion_main!(benches);
