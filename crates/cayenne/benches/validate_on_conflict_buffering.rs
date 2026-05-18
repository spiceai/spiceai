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

//! Regression bench: per-commit cost of `validate_on_conflict`'s
//! unbounded buffering on the CDC ingestion path.
//!
//! `CayenneTableProvider::validate_on_conflict`
//! (`crates/cayenne/src/provider/table.rs:3491-3571`) drains the *entire*
//! incoming CDC batch into three heap-resident structures **before any
//! Vortex file is written**:
//!
//! ```ignore
//! while let Some(batch_result) = stream.next().await {
//!     ...
//!     incoming_keys.extend(kept_keys.iter().cloned()); // HashSet<OwnedRow>
//!     all_kept_keys.extend(kept_keys);                 // HashSet<OwnedRow>
//!     if let Some(batch) = filtered_batch {
//!         filtered_batches.push(batch);                // Vec<RecordBatch>
//!     }
//! }
//! ```
//!
//! Triggered on every CDC commit when
//! `pk_conflict_detection: Auto` (the default). The CH-benCH SF100
//! retest reported 6 of 14 tables accumulating hundreds of MB of
//! un-drained WAL under sustained write load — this materialization is
//! one of the largest per-commit fixed costs, and it sits on the
//! critical path **before** any Vortex write begins.
//!
//! With `cdc_max_coalesced_bytes: 256 MB` (the SF100 spicepod default),
//! one coalesced burst allocates up to that much heap on the input
//! decode side, plus an `OwnedRow` for every row (~16-64 bytes
//! depending on PK shape), plus a `HashSet<OwnedRow>` entry for every
//! row. For PK-heavy tables (customer with ~500-byte rows updating per
//! Payment, stock with ~10 updates per NewOrder) this is the
//! commit-rate bottleneck after the metastore round trip.
//!
//! The TigerStyle remedy is a **bounded staging buffer**: pre-allocate
//! a fixed cap (e.g. 64 MiB), stream batches through dedup with only a
//! sliding window of keys, and apply backpressure to the upstream CDC
//! source when full. Today there is no cap and no backpressure.
//!
//! ## What this bench measures
//!
//! Pure shape — no Vortex, no metastore, no Cayenne setup. Models the
//! drain-into-Vec + grow-HashSet pattern on a synthetic CDC stream of
//! M batches × K rows each, using a fixed PK width that matches Arrow
//! `RowConverter::convert_columns` output (16 bytes — same shape as
//! the production `OwnedRow` for a single `Int64` or `Decimal` PK).
//!
//! Two lanes:
//!
//! - `current_unbounded_accumulation/<M>` mirrors today's
//!   `validate_on_conflict`. Heap grows linearly with `M·K`.
//! - `bounded_streaming/<M>` processes each batch in isolation, drops
//!   `filtered_batches` after handing off, and uses a sliding `dedup_window`
//!   of only the most recent batch's keys. Heap stays constant at `K`
//!   entries regardless of `M`.
//!
//! ## How to read
//!
//! `cargo bench --bench validate_on_conflict_buffering -p cayenne`.
//! Compare:
//!
//! - `current_unbounded_accumulation/M=512` (≈ a 256 MB CDC burst at
//!   1 KiB/row) — wall time scales linearly with `M` because each
//!   batch adds K HashSet inserts plus a `RecordBatch` clone into the
//!   growing Vec. The slope per batch is the per-commit overhead the
//!   SF100 retest is paying.
//! - `bounded_streaming/M=512` — wall time is roughly constant per
//!   batch, scaling with total `M·K` work but with no per-batch alloc
//!   growth. The gap visualizes the achievable per-commit cost.
//!
//! The ratio between lanes at `M=512` is the per-commit-cost overhead
//! that the materialization adds. At `M=512, K=1024` it is the cost
//! difference between writing 512 batches one-at-a-time vs first
//! collecting them all into a Vec then writing.

#![allow(clippy::expect_used)]

use std::collections::HashSet;
use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

/// Number of rows per batch — matches a typical CDC envelope after
/// `cdc_max_coalesced_envelopes: 1024`. Each "row" is one fixed-width
/// PK encoding.
const ROWS_PER_BATCH: usize = 1024;

/// Fixed PK width. 16 bytes matches Arrow `RowConverter` output for a
/// single `Int64` or `Decimal128` column with the standard row encoding
/// header. Most TPC-C / CH-benCH PKs are single integer columns; the
/// widest realistic PK in CH-benCH is `customer(c_w_id, c_d_id, c_id)`
/// which encodes to ~24 bytes — same order of magnitude.
const PK_WIDTH: usize = 16;

/// Batch counts straddling the typical CDC burst sizes:
/// - 8 batches ≈ a single small append (8 K rows total).
/// - 64 batches ≈ a moderate coalesced burst (64 K rows).
/// - 512 batches ≈ a full `cdc_max_coalesced_bytes: 256 MB` burst at
///   ~512 KiB per batch.
const BATCH_COUNTS: &[usize] = &[8, 64, 512];

type PkKey = [u8; PK_WIDTH];

/// Stand-in for `RecordBatch` — a `Box<[u8]>` payload sized to roughly
/// match a 1 KiB-per-row Arrow batch. The exact shape does not matter;
/// what matters is that pushing into a `Vec<Batch>` clones an
/// `Arc`-equivalent (here, moves a `Box`) and holding many of them
/// retains heap memory.
struct Batch {
    keys: Vec<PkKey>,
    /// Dummy payload representing the column data — `Box<[u8]>` so the
    /// allocation is real and `Vec<Batch>` retains memory linearly
    /// with `M`.
    _payload: Box<[u8]>,
}

fn make_batch(batch_idx: usize) -> Batch {
    let mut keys = Vec::with_capacity(ROWS_PER_BATCH);
    let base = (batch_idx as u64).wrapping_mul(ROWS_PER_BATCH as u64);
    for r in 0..ROWS_PER_BATCH {
        let mut key = [0u8; PK_WIDTH];
        key[..8].copy_from_slice(&(base + r as u64).to_le_bytes());
        keys.push(key);
    }
    Batch {
        keys,
        _payload: vec![0u8; ROWS_PER_BATCH].into_boxed_slice(),
    }
}

/// Mirrors `validate_on_conflict` (`table.rs:3491-3571`): drain stream
/// into Vec<Batch>, grow HashSet<PkKey> across batches, retain
/// everything until the caller pulls.
fn current_unbounded_accumulation(m: usize) -> usize {
    let mut filtered_batches: Vec<Batch> = Vec::new();
    let mut incoming_keys: HashSet<PkKey> = HashSet::with_capacity(1024);
    let mut all_kept_keys: HashSet<PkKey> = HashSet::with_capacity(1024);

    for batch_idx in 0..m {
        let batch = make_batch(batch_idx);

        // Per-row dedup: every key from this batch goes into both
        // hashsets, mirroring the `incoming_keys.extend(kept_keys.iter().cloned())`
        // + `all_kept_keys.extend(kept_keys)` pattern.
        for key in &batch.keys {
            if !incoming_keys.contains(key) {
                incoming_keys.insert(*key);
                all_kept_keys.insert(*key);
            }
        }

        // Retain the batch in the growing Vec.
        filtered_batches.push(batch);
    }

    // The function does not free `filtered_batches`/`incoming_keys` —
    // they are returned to the caller and only freed after the
    // downstream Vortex write completes.
    let kept = filtered_batches.iter().map(|b| b.keys.len()).sum::<usize>();
    black_box(&filtered_batches);
    black_box(&incoming_keys);
    black_box(&all_kept_keys);
    kept
}

/// Bounded streaming alternative: dedup window is at most one batch
/// (or up to a small fixed cap), `filtered_batches` is never retained.
/// Each batch is handed off to a hypothetical downstream consumer and
/// immediately dropped.
fn bounded_streaming(m: usize) -> usize {
    let mut total_kept = 0usize;
    // Sliding window of recent keys, bounded at `ROWS_PER_BATCH`. In
    // production this would be a `parking_lot::Mutex<RingBuf<PkKey>>`
    // sized at a few × batch_size, or an LSM-style bloom filter.
    let mut window: HashSet<PkKey> = HashSet::with_capacity(ROWS_PER_BATCH);

    for batch_idx in 0..m {
        let batch = make_batch(batch_idx);

        window.clear();
        for key in &batch.keys {
            if window.insert(*key) {
                total_kept += 1;
            }
        }

        // Hand off batch to downstream — modeled as `black_box` so the
        // optimizer cannot drop the work. Then the batch is dropped
        // immediately, freeing its heap.
        black_box(&batch);
    }

    total_kept
}

fn bench_validate_on_conflict_buffering(c: &mut Criterion) {
    let mut group = c.benchmark_group("validate_on_conflict_buffering");
    for &m in BATCH_COUNTS {
        let total_rows = u64::try_from(m * ROWS_PER_BATCH).unwrap_or(u64::MAX);
        group.throughput(Throughput::Elements(total_rows));

        group.bench_with_input(
            BenchmarkId::new("current_unbounded_accumulation", m),
            &m,
            |b, &m| {
                b.iter(|| current_unbounded_accumulation(black_box(m)));
            },
        );

        group.bench_with_input(BenchmarkId::new("bounded_streaming", m), &m, |b, &m| {
            b.iter(|| bounded_streaming(black_box(m)));
        });
    }
    group.finish();
}

criterion_group!(benches, bench_validate_on_conflict_buffering);
criterion_main!(benches);
