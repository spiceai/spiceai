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
//! `RoaringBitmap` to `RoaringTreemap`.
//!
//! Older versions of `DeletionFilteringVortexFormat::create_physical_plan` walked
//! the `FileScanConfig`'s file groups and, for every file with deletions, rebuilt
//! a fresh `RoaringTreemap` from the cached `RoaringBitmap`:
//!
//! ```ignore
//! // attach_access_plan_to_file
//! let exclude: RoaringTreemap = bitmap.iter().map(u64::from).collect();
//! let access_plan = VortexAccessPlan::default()
//!     .with_selection(Selection::ExcludeRoaring(exclude));
//! ```
//!
//! The cache stored `Arc<RoaringBitmap>` (u32-keyed, compact form) but Vortex's
//! `Selection::ExcludeRoaring` API consumes a `RoaringTreemap` (u64-keyed). The
//! conversion `bitmap.iter().map(u64::from).collect()` materialized every
//! deleted row id, built a fresh `RoaringTreemap`, and discarded both at the
//! end of scan setup — paid per file per scan, even when deletions had not
//! changed.
//!
//! The production path now stores the prebuilt access plan directly. See
//! `PositionDeletionVector::new` ([`crate::provider::deletion_strategy::PositionDeletionVector::new`],
//! at `provider/deletion_strategy.rs:48-60`):
//!
//! ```ignore
//! let exclude: RoaringTreemap = row_ids.iter().map(u64::from).collect();
//! let access_plan = Arc::new(
//!     VortexAccessPlan::default().with_selection(Selection::ExcludeRoaring(exclude)),
//! );
//! ```
//!
//! Subsequent scans call `.access_plan()` ([`provider/deletion_strategy.rs:87`])
//! which returns an `Arc::clone(&self.access_plan)`. The treemap conversion is
//! paid once at deletion-snapshot publish time, never again per scan.
//!
//! ## What this bench measures
//!
//! Pure shape — no metastore, no Cayenne setup, no Vortex scan. Models the
//! conversion that scan-time `attach_access_plan_to_file` invocations would
//! have performed under the older code.
//!
//! Two lanes per deletion count:
//!
//! - `convert_per_scan_baseline/<deletions>` — mirrors the older
//!   `bitmap.iter().map(u64::from).collect::<RoaringTreemap>()` on every scan.
//!   Wall time is the iterator walk plus the new treemap allocation.
//! - `cached_arc_clone/<deletions>` — current behavior: a single pre-built
//!   `Arc<RoaringTreemap>` cloned per scan. Wall time is one `Arc::clone` —
//!   a single atomic refcount bump.
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
//! ## How to read
//!
//! `cargo bench --bench deletion_vector_bitmap_to_treemap -p cayenne`.
//!
//! - `convert_per_scan_baseline/100000` — per-file fixed cost on a
//!   delete-heavy file under the older code.
//! - The ratio `convert_per_scan_baseline/N` ÷ `cached_arc_clone/N` is the
//!   headroom the prebuilt-access-plan fix delivered.

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
    let mut group =
        c.benchmark_group("deletion_vector_bitmap_to_treemap_convert_per_scan_baseline");
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
        // Pre-build the treemap once, share via Arc — mirrors the current
        // production shape where `PositionDeletionVector` stores an
        // `Arc<VortexAccessPlan>` containing the converted treemap.
        let treemap: Arc<RoaringTreemap> = Arc::new(convert_to_treemap(&bitmap));
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                // Per-scan cost in the current production shape: one `Arc::clone`
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
