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

//! Regression bench: per-scan cost of converting per-file deletion vectors from
//! `RoaringBitmap` to `RoaringTreemap` in
//! `crates/cayenne/src/provider/vortex_format.rs:151-182`.
//!
//! Every `DeletionFilteringVortexFormat::create_physical_plan` call walks the
//! `FileScanConfig`'s file groups, looks up each file's deletion bitmap in the
//! `deletion_cache` (a `ArcSwap<HashMap<String, Arc<RoaringBitmap>>>`), and —
//! for every file that has deletions — rebuilds a fresh `RoaringTreemap`:
//!
//! ```ignore
//! // attach_access_plan_to_file, vortex_format.rs:164
//! let exclude: RoaringTreemap = bitmap.iter().map(u64::from).collect();
//! let access_plan = VortexAccessPlan::default()
//!     .with_selection(Selection::ExcludeRoaring(exclude));
//! ```
//!
//! The cache stores `Arc<RoaringBitmap>` (u32-keyed, compact form) because the
//! pre-cached deletion vectors were loaded as `RoaringBitmap`. The Vortex
//! `Selection::ExcludeRoaring` API consumes a `RoaringTreemap` (u64-keyed) for
//! billion-row tables. The conversion `bitmap.iter().map(u64::from).collect()`
//! materializes every deleted row id from the source bitmap, builds a fresh
//! `RoaringTreemap` containing the same elements, and discards both at the end
//! of the scan setup.
//!
//! Two consequences:
//!
//! 1. **Per-scan, per-file fixed cost**: a table with 1000 files where every
//!    file carries 1000 deletions pays 1000 * (per-file conversion cost) on
//!    every scan, *even when the underlying deletions are unchanged across
//!    scans*. The deletion cache invalidates only on writes, but the converted
//!    form is rebuilt per scan.
//! 2. **Quadratic-ish in deletion density**: as deletion rate per file rises
//!    (e.g. after a large delete-by-predicate or a slow checkpoint absorption),
//!    each per-file conversion grows linearly with the deletion count.
//!
//! The TigerStyle remedy is to store the converted form directly in the cache.
//! Two options:
//! - cache `Arc<RoaringTreemap>` instead of `Arc<RoaringBitmap>`, paying the
//!   conversion once at deletion-cache publish time. The cache is published
//!   under the write fence; readers only ever see the converted form.
//! - cache both shapes as `(Arc<RoaringBitmap>, OnceCell<Arc<RoaringTreemap>>)`
//!   and lazily fill the treemap on first scan. Same amortization, slightly
//!   more memory.
//!
//! Either fix drops the per-scan cost to `Arc::clone()` on the converted bitmap
//! — a single atomic refcount bump, independent of deletion count.
//!
//! ## What this bench measures
//!
//! Pure shape — no metastore, no Cayenne setup, no Vortex scan. Models the
//! conversion that every scan-time `attach_access_plan_to_file` invocation
//! performs on a single file's deletion bitmap.
//!
//! Two lanes per deletion count:
//!
//! - `convert_per_scan/<deletions>` — mirrors today's
//!   `bitmap.iter().map(u64::from).collect::<RoaringTreemap>()` on every scan.
//!   Wall time is the iterator walk plus the new treemap allocation.
//! - `cached_arc_clone/<deletions>` — models the proposed cache: a single
//!   pre-built `Arc<RoaringTreemap>` cloned per scan. Wall time is one
//!   `Arc::clone` — a single atomic refcount bump.
//!
//! Deletion counts mirror realistic file-level deletion densities:
//!
//! - 100      deletions: a few CDC deletes scattered across files.
//! - 1 K      deletions: typical mid-life file under steady deletion load.
//! - 10 K     deletions: a file approaching the rewrite-by-compaction threshold.
//! - 100 K    deletions: a "delete-heavy" file before compaction absorbs them.
//! - 1 M      deletions: extreme — a near-empty file kept alive by zone-map
//!   relevance for some other column.
//!
//! Per-file densities multiply: at 1000 files * 10 K deletions/file the
//! per-scan tax is 1000 * `convert_per_scan/10000`.
//!
//! ## How to read
//!
//! `cargo bench --bench deletion_vector_bitmap_to_treemap -p cayenne`.
//!
//! - `convert_per_scan/100000` — per-file fixed cost on a delete-heavy file.
//!   Multiply by your `num_files_with_deletions` to get the per-scan floor.
//! - The ratio `convert_per_scan/N` ÷ `cached_arc_clone/N` is the headroom
//!   from the fix. At N=1 K the ratio is dominated by the
//!   `RoaringTreemap::new()` allocation; at N≥10 K it is dominated by the
//!   `bitmap.iter()` walk plus `RoaringTreemap::insert` per element.

#![allow(clippy::expect_used)]

use std::hint::black_box;
use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use roaring::{RoaringBitmap, RoaringTreemap};

/// Deletion counts spanning realistic per-file shapes.
const DELETION_COUNTS: &[usize] = &[100, 1_000, 10_000, 100_000, 1_000_000];

/// Build a `RoaringBitmap` modelling realistic deletion locality. We scatter
/// keys with a Knuth multiplicative scramble across roughly 4×N to mimic CDC
/// deletes that touch sparse rows in a file (rather than a contiguous prefix
/// that compresses pathologically well).
fn build_bitmap(n: usize) -> RoaringBitmap {
    let mut bitmap = RoaringBitmap::new();
    for i in 0..n {
        let scrambled = (i as u32).wrapping_mul(0x9E37_79B9_u32);
        bitmap.insert(scrambled & 0x00FF_FFFF); // limit to 16M-row range
    }
    bitmap
}

/// Mirror the exact production conversion at
/// `vortex_format.rs:164`:
/// `bitmap.iter().map(u64::from).collect::<RoaringTreemap>()`.
fn convert_to_treemap(bitmap: &RoaringBitmap) -> RoaringTreemap {
    bitmap.iter().map(u64::from).collect()
}

fn bench_convert_per_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("deletion_vector_bitmap_to_treemap_convert_per_scan");
    for &n in DELETION_COUNTS {
        let bitmap = build_bitmap(n);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                // Exactly the body of `attach_access_plan_to_file` for one
                // file that has deletions. Discard the result via black_box
                // so the optimizer cannot lift the conversion out of the
                // iteration loop.
                let treemap = convert_to_treemap(&bitmap);
                black_box(treemap);
            });
        });
    }
    group.finish();
}

fn bench_cached_arc_clone(c: &mut Criterion) {
    let mut group = c.benchmark_group("deletion_vector_bitmap_to_treemap_cached_arc_clone");
    for &n in DELETION_COUNTS {
        let bitmap = build_bitmap(n);
        // Pre-build the treemap once, share via Arc — models the fix where
        // the deletion cache stores `Arc<RoaringTreemap>` directly.
        let treemap: Arc<RoaringTreemap> = Arc::new(convert_to_treemap(&bitmap));
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                // Per-scan cost in the proposed cache shape: one `Arc::clone`
                // (a single atomic refcount bump) regardless of deletion count.
                let cloned = Arc::clone(&treemap);
                black_box(cloned);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_convert_per_scan, bench_cached_arc_clone);
criterion_main!(benches);
