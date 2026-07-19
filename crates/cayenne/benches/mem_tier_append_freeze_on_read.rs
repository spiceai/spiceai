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

//! Shape bench for the QUEUED "freeze-on-read mem-tier" idea: the append cost of
//! the current segment log vs an `im::Vector` collection vs a mutable-tail
//! (freeze-on-read) model.
//!
//! `MemTier` is immutable and swapped via `ArcSwap`, so every CDC apply builds a
//! NEW segment list. Today `append_segment_with_source_position`
//! (`crates/cayenne/src/provider/mem_tier.rs:471`) does:
//! ```ignore
//! let mut segments = Vec::with_capacity(self.segments.len() + 1);
//! segments.extend(self.segments.iter().cloned());   // O(N) MemSegment clones
//! segments.push(new_segment);
//! ```
//! i.e. an O(N) clone of the whole `Vec<MemSegment>` per apply, so N applies
//! before a checkpoint/seal cost O(N^2) segment clones in aggregate. Each
//! `MemSegment` clone is a handful of `Arc`/HAMT-root refcount bumps (the batch
//! data is `Arc`-shared, not deep-copied), but at high segment counts (the
//! documented "material past ~1k-10k segments" regime) the N^2 refcount traffic
//! is real.
//!
//! Two proposed changes this bench measures:
//! 1. **`im::Vector` collection** — its `clone()` is O(1) (structural sharing;
//!    the elements are NOT re-cloned) and `push_back` is O(log n), so an apply
//!    costs O(log n) instead of O(N).
//! 2. **Mutable-tail freeze-on-read** — an apply mutates the current tail
//!    segment in place until it is frozen (by a read/scan); only a frozen tail
//!    forces a new segment. This coalesces `FREEZE_EVERY` applies into one
//!    segment, so the collection grows N/FREEZE_EVERY times and most applies are
//!    a cheap in-place mutation with no collection touch at all.
//!
//! ## Lanes (per apply count N)
//! - `vec_clone_push` — the CURRENT shape: `Vec<Segment>`, each apply clones the
//!   whole vec then pushes. O(N) per apply, O(N^2) total.
//! - `imvec_clone_push` — change (1) alone: `im::Vector<Segment>`, O(1) clone +
//!   O(log n) push per apply. Still one segment per apply.
//! - `mutable_tail_imvec` — changes (1)+(2): coalesce `FREEZE_EVERY` applies into
//!   a mutable tail; freeze into the `im::Vector` only on the freeze boundary.
//!
//! ## How to read
//! `cargo bench --bench mem_tier_append_freeze_on_read -p cayenne`.
//! `vec_clone_push` should blow up super-linearly with N (the N^2 clone chain);
//! `imvec_clone_push` should be near-linear; `mutable_tail_imvec` lowest. The
//! gap at N=4096 is the append-path headroom the freeze-on-read change would buy
//! at high segment counts — the data point for deciding whether that
//! (correctness-sensitive) change is worth its cost. NOTE: this measures ONLY
//! the append/collection cost; the real change's hard part is the freeze/scan
//! ordering (a mutable tail a scan may read), which is a correctness problem to
//! be modeled with loom, not a throughput one.

#![allow(clippy::expect_used)]

use std::hint::black_box;
use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

/// Apply counts bracketing the regime where the O(N^2) clone chain matters:
/// small (noise), ~1k (onset), ~4k (clearly material).
const APPLY_COUNTS: &[usize] = &[256, 1_024, 4_096];

/// Applies coalesced into one segment by the mutable-tail model before a
/// (simulated) read freezes it. Models a read/scan cadence: 16 applies land in
/// the tail per freeze, so the frozen collection grows N/16 times.
const FREEZE_EVERY: usize = 16;

/// Stand-in for `MemSegment`: the batch data and per-apply source position are
/// `Arc`-shared (cloning bumps refcounts, no deep copy), the tombstone delta is
/// an `im::HashMap` (O(1) root clone), plus the plain counter fields. Cloning
/// this mirrors the cost of cloning a real `MemSegment` in the append vec.
#[derive(Clone)]
struct Segment {
    batches: Arc<Vec<u64>>,
    tombstones: im::HashMap<u64, u64>,
    source_position: Option<Arc<str>>,
    data_sequence: i64,
    bytes: u64,
    rows: u64,
    superseded: bool,
}

fn make_segment(i: usize) -> Segment {
    Segment {
        batches: Arc::new(vec![i as u64; 8]),
        tombstones: im::HashMap::new(),
        source_position: None,
        data_sequence: i as i64,
        bytes: 8 * 8,
        rows: 8,
        superseded: false,
    }
}

/// Mutable tail for the freeze-on-read model: owns its batch data (a growable
/// `Vec`, not yet `Arc`-shared) so an apply appends in place; frozen into an
/// immutable `Segment` only when a read forces it.
struct SegmentMut {
    batches: Vec<u64>,
    tombstones: im::HashMap<u64, u64>,
    data_sequence: i64,
    bytes: u64,
    rows: u64,
}

impl SegmentMut {
    fn new(i: usize) -> Self {
        Self {
            batches: vec![i as u64; 8],
            tombstones: im::HashMap::new(),
            data_sequence: i as i64,
            bytes: 8 * 8,
            rows: 8,
        }
    }

    /// An in-place apply into the (unfrozen) tail: append the batch, bump the
    /// running counters. No collection touch, no allocation beyond the vec grow.
    fn apply_in_place(&mut self, i: usize) {
        self.batches.extend_from_slice(&[i as u64; 8]);
        self.bytes += 8 * 8;
        self.rows += 8;
        self.data_sequence = i as i64;
    }

    fn freeze(self) -> Segment {
        Segment {
            batches: Arc::new(self.batches),
            tombstones: self.tombstones,
            source_position: None,
            data_sequence: self.data_sequence,
            bytes: self.bytes,
            rows: self.rows,
            superseded: false,
        }
    }
}

/// CURRENT shape (`mem_tier.rs:471`): each apply rebuilds the whole `Vec`.
fn vec_clone_push(applies: usize) -> usize {
    let mut segments: Vec<Segment> = Vec::new();
    for i in 0..applies {
        let mut next = Vec::with_capacity(segments.len() + 1);
        next.extend(segments.iter().cloned());
        next.push(make_segment(i));
        segments = next;
    }
    segments.len()
}

/// Change (1): `im::Vector` — O(1) structural-share clone + O(log n) push_back.
fn imvec_clone_push(applies: usize) -> usize {
    let mut segments: im::Vector<Segment> = im::Vector::new();
    for i in 0..applies {
        let mut next = segments.clone();
        next.push_back(make_segment(i));
        segments = next;
    }
    segments.len()
}

/// Changes (1)+(2): mutable tail, freeze into the `im::Vector` every
/// `FREEZE_EVERY` applies.
fn mutable_tail_imvec(applies: usize) -> usize {
    let mut frozen: im::Vector<Segment> = im::Vector::new();
    let mut tail: Option<SegmentMut> = None;
    for i in 0..applies {
        match tail.as_mut() {
            Some(t) => t.apply_in_place(i),
            None => tail = Some(SegmentMut::new(i)),
        }
        // A read every FREEZE_EVERY applies freezes the tail into the collection.
        if (i + 1) % FREEZE_EVERY == 0
            && let Some(t) = tail.take()
        {
            let mut next = frozen.clone();
            next.push_back(t.freeze());
            frozen = next;
        }
    }
    if let Some(t) = tail.take() {
        frozen.push_back(t.freeze());
    }
    frozen.len()
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("mem_tier_append_freeze_on_read");
    for &applies in APPLY_COUNTS {
        group.throughput(Throughput::Elements(applies as u64));

        group.bench_with_input(BenchmarkId::new("vec_clone_push", applies), &applies, |b, &n| {
            b.iter(|| black_box(vec_clone_push(n)));
        });
        group.bench_with_input(BenchmarkId::new("imvec_clone_push", applies), &applies, |b, &n| {
            b.iter(|| black_box(imvec_clone_push(n)));
        });
        group.bench_with_input(
            BenchmarkId::new("mutable_tail_imvec", applies),
            &applies,
            |b, &n| {
                b.iter(|| black_box(mutable_tail_imvec(n)));
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
