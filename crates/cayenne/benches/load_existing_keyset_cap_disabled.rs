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

//! Regression bench: per-CDC-commit cost of rebuilding the existing-PK keyset
//! from scratch when the in-memory cache is exceeded by the byte-budget cap
//! `PK_KEYSET_CACHE_MAX_BYTES` (256 MiB).
//!
//! `CayenneTableProvider::prepare_stream_for_insert`
//! (`crates/cayenne/src/provider/table.rs:3935`) calls `take_cached_pk_keyset`,
//! and on a miss falls through to `load_existing_keyset`
//! (`provider/table.rs:3637`) which scans the main listing table **plus every
//! protected snapshot's PK columns** to rebuild a
//! `HashMap<OwnedRow, RowLocation>`. After the write,
//! `OnConflictValidationStream::store_existing_keyset`
//! (`provider/table.rs:1626`) tries to restore the cache via
//! `store_cached_pk_keyset` — but that function applies a byte-budget cap:
//!
//! ```ignore
//! // table.rs
//! const PK_KEYSET_CACHE_MAX_BYTES: usize = 256 * 1024 * 1024; // 256 MiB
//!
//! fn store_cached_pk_keyset(&self, keyset: HashMap<OwnedRow, RowLocation>) {
//!     if estimated_bytes(&keyset) > PK_KEYSET_CACHE_MAX_BYTES {
//!         *self.pk_keyset_cache.lock() = None;   // <-- cache *dropped*
//!         return;
//!     }
//!     *self.pk_keyset_cache.lock() = Some(keyset);
//! }
//! ```
//!
//! At ~40-64 bytes per entry (key bytes + `RowLocation` + `HashMap` overhead),
//! the 256 MiB byte budget accommodates ~4 M entries for narrow int64 PKs and
//! proportionally fewer for wide composite PKs. SF100 CH-benCH tables fall
//! into two regimes against this budget: `customer` (~3 M) now stays cached
//! across commits, while `stock` (~10 M), `new_order` (~9 M), and
//! `order_line` (~300 M) still exceed the budget — every CDC commit on those
//! tables pays a cold-start scan of the main listing table plus every
//! protected snapshot.
//!
//! The May 18 2026 SF100 retest reported "file write" times of 113 s
//! (new_order), 61 s (stock), 30.5 s (order_line) per 256 MB CDC batch. The
//! `load_existing_keyset` rebuild dominates that wall time on every PK-mode
//! table that exceeds the budget because it touches `O(rows +
//! protected_snapshot_count · rows_per_protected_snapshot)` rows and pays one
//! heap alloc + one `HashMap::insert` per row.
//!
//! ## TigerStyle remedy
//!
//! Three layered options ranked by effort:
//!
//! - **(A) Byte-budget cap — landed.** Replaced the entry-count cap with a
//!   byte-budget cap (`PK_KEYSET_CACHE_MAX_BYTES`, default 256 MiB). Recovers
//!   the cache for narrow-PK tables up to ~4 M rows. For larger tables
//!   (`stock`, `new_order`, `order_line`) the cap still kicks in and the
//!   rebuild cost modelled by this bench still applies.
//! - **(B) Existence-bloom fallback.** Above the budget, maintain a
//!   space-bounded bloom filter of existing PKs and replace the `HashMap` probe
//!   with `bloom.contains(key)` → conditional targeted lookup. Bloom at 1 % FPR
//!   is ~9.6 bits/key (~360 MB for 300 M keys). Targeted lookups touch
//!   `O(incoming_batch_size · log N)` rows instead of `O(total_rows)`.
//! - **(C) Source-side existence query.** Skip the in-Cayenne keyset entirely
//!   and round-trip the incoming batch's PKs to the federated source's PK
//!   index. Eliminates protected-snapshot amplification permanently but adds a
//!   Postgres round-trip per CDC commit.
//!
//! ## What this bench measures
//!
//! Pure shape — no Cayenne setup, no real I/O. Models the inner loop of
//! [`crate::provider::table::CayenneTableProvider::process_stream_into_keyset`]
//! (`provider/table.rs:3809`): for each row in each scanned snapshot, do one
//! `Box::<[u8]>` heap allocation (mirrors `rows.row(idx).owned()` for a
//! composite-PK or row-converted key) and one `HashMap::insert`.
//!
//! Three lanes per `(rows_per_snapshot, snapshot_count)`:
//!
//! - `full_rebuild_when_over_budget` — mirrors `load_existing_keyset` on
//!   tables that exceed the byte budget: allocate one `Box<[u8]>` per row,
//!   insert into a fresh `HashMap` for the `(main + snapshot_count)`
//!   snapshots. Cost scales as `O((1 + N) · M)`.
//! - `cached_clone_of_warm_keyset` — mirrors the cache-hit path now available
//!   for tables that fit within the budget: clone a pre-built `HashMap` of
//!   the full keyset. Wall time is one `HashMap::clone` — what narrow-PK
//!   tables pay per commit after the byte-budget cap landed.
//! - `bloom_prefilter_then_targeted_lookups` — mirrors option (B): a
//!   pre-built bloom filter answers existence in O(1) bits per probe;
//!   incoming batch is fixed at 1024 keys, of which ~10 require a targeted
//!   `HashMap::get` after a bloom hit at 1 % FPR.
//!
//! ## How to read
//!
//! `cargo bench --bench load_existing_keyset_cap_disabled -p cayenne`.
//!
//! - `full_rebuild_when_over_budget/M=3000000/N=87` — what `customer`-shaped
//!   CDC upserts would pay if the table fell over the byte budget; for
//!   `stock`-shaped (M=10 M) and larger this is what production pays today.
//! - `cached_clone_of_warm_keyset/M=3000000/N=87` is essentially independent
//!   of N (the clone size is `M`, not `N · M`): showing the gap is the
//!   total wall-time the byte-budget cap saves per commit on tables that
//!   fit. For tables above the budget the rebuild lane still applies.
//! - `bloom_prefilter_then_targeted_lookups/M=...` collapses to ~µs at any
//!   M: 1024 bloom probes + ~10 HashMap lookups. Shows the achievable
//!   floor for option (B).

use std::collections::HashMap;
use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

/// Rows in the main listing table. Brackets the SF100 PK-mode tables:
/// `customer` (~3 M), `stock` (~10 M), `new_order` (~9 M). `order_line`
/// (~300 M) is out of scope for the pure-shape bench — see the docstring's
/// extrapolation note. Smaller sizes also included to show the slope.
const ROWS_PER_SNAPSHOT: &[usize] = &[10_000, 100_000, 1_000_000, 3_000_000];

/// Protected-snapshot counts. May 18 2026 retest reached customer=87,
/// oorder=50, stock=25, order_line=6 by end of an 11-minute run.
const SNAPSHOT_COUNTS: &[usize] = &[0, 4, 16, 64];

/// Composite-PK keys are typically 16-32 bytes through `RowConverter`. Pick
/// 24 to bracket the customer-shape (Utf8 + Int64 columns) and stock-shape
/// (multi-Int32 columns).
const KEY_BYTES: usize = 24;

/// Incoming batch size used by the bloom-prefilter lane. 1024 matches the
/// SF100 spicepod default for `cdc_max_coalesced_envelopes` and bounds the
/// targeted-lookup cost at the upper end.
const INCOMING_BATCH_KEYS: usize = 1024;

/// Construct a deterministic byte key of width [`KEY_BYTES`] from an integer.
/// Used for both the pre-built keyset (warm path) and the per-row inserts
/// (cold path), so the two lanes compare like-for-like.
fn make_key(idx: usize) -> Box<[u8]> {
    let mut bytes = vec![0u8; KEY_BYTES];
    let idx_u64 = u64::try_from(idx).unwrap_or(u64::MAX);
    bytes[..8].copy_from_slice(&idx_u64.to_be_bytes());
    // Vary the tail so hash distribution isn't degenerate.
    let tail_idx = idx.wrapping_mul(2_654_435_761);
    let tail = u64::try_from(tail_idx).unwrap_or(u64::MAX).to_be_bytes();
    bytes[8..16].copy_from_slice(&tail);
    bytes.into_boxed_slice()
}

/// Mimics `RowLocation` from `table.rs` — the value half of the cached
/// keyset. 24 bytes on 64-bit platforms (1-byte enum + 7 bytes pad + 8-byte
/// file id + 8-byte row id). Kept inline for the bench so HashMap layout
/// matches production.
#[derive(Clone, Copy)]
#[expect(dead_code)]
struct RowLocation {
    source: u8,
    data_file_id: i64,
    row_id: i64,
}

fn build_warm_keyset(total_rows: usize) -> HashMap<Box<[u8]>, RowLocation> {
    let mut map = HashMap::with_capacity(total_rows);
    for i in 0..total_rows {
        map.insert(
            make_key(i),
            RowLocation {
                source: 0,
                data_file_id: 0,
                row_id: i64::try_from(i).unwrap_or(i64::MAX),
            },
        );
    }
    map
}

/// Lane A: cold rebuild. Mirrors `load_existing_keyset` →
/// `process_stream_into_keyset` for `(1 + snapshot_count)` snapshots, each
/// of size `rows_per_snapshot`. For each row: one `Box<[u8]>` alloc + one
/// `HashMap::insert`. Models the production path when the keyset cache is
/// disabled by the `PK_KEYSET_CACHE_MAX_BYTES` byte budget.
fn run_full_rebuild(rows_per_snapshot: usize, snapshot_count: usize) -> usize {
    let total_snapshots = snapshot_count + 1;
    let mut keyset: HashMap<Box<[u8]>, RowLocation> = HashMap::with_capacity(rows_per_snapshot);
    for snapshot_idx in 0..total_snapshots {
        let row_id_base = snapshot_idx * rows_per_snapshot;
        for row_offset in 0..rows_per_snapshot {
            let global_idx = row_id_base + row_offset;
            // mimics `rows.row(row_idx).owned()`: one heap alloc per row.
            let key = make_key(global_idx);
            // mimics `keyset.insert(key, RowLocation { ... })`.
            keyset.insert(
                key,
                RowLocation {
                    source: 0,
                    data_file_id: 0,
                    row_id: i64::try_from(row_offset).unwrap_or(i64::MAX),
                },
            );
        }
    }
    keyset.len()
}

/// Lane B: warm cache hit. Models what the cache-hit path *would* pay if
/// the entry-count cap weren't disabling the cache for production-sized
/// tables. `HashMap::clone` is the take-then-restore cost — one allocation +
/// one memcpy of the bucket vector + per-entry key clone.
fn run_cached_clone(warm: &HashMap<Box<[u8]>, RowLocation>) -> usize {
    let cloned = warm.clone();
    cloned.len()
}

/// Lane C: bloom prefilter, then targeted lookups for confirmed-positives.
/// Models option (B) from the bench docstring. Bloom built once over the
/// full keyset; per-CDC-commit work is `INCOMING_BATCH_KEYS` bloom probes
/// plus a small constant number of confirmed-positive `HashMap::get` calls.
///
/// `existence_rate` simulates how many of the incoming batch's keys
/// actually exist in the table. 0.01 = 1 % of incoming keys conflict — a
/// typical INSERT-mostly CDC workload. Bloom false-positive rate is also
/// 1 %, so confirmed-positive count is ~`existence_rate * batch +
/// fpr * batch`.
fn run_bloom_targeted(
    bloom: &BloomFilter,
    warm: &HashMap<Box<[u8]>, RowLocation>,
    incoming_keys: &[Box<[u8]>],
) -> usize {
    let mut hits = 0_usize;
    for key in incoming_keys {
        if bloom.maybe_contains(key) {
            if warm.contains_key(key) {
                hits += 1;
            }
        }
    }
    hits
}

/// Minimal Bloom filter: one hash function, two probes per insert/query.
/// Replicates the bench shape without pulling in another dependency.
struct BloomFilter {
    bits: Vec<u64>,
    bit_mask: usize,
}

impl BloomFilter {
    fn new(expected_items: usize) -> Self {
        // 9.6 bits per item for ~1 % FPR with two probes. Round up to a
        // power of two so the bit-index can mask instead of mod.
        let target_bits = expected_items.saturating_mul(10).next_power_of_two();
        let words = target_bits / 64;
        Self {
            bits: vec![0; words.max(1)],
            bit_mask: target_bits.saturating_sub(1),
        }
    }

    fn insert(&mut self, key: &[u8]) {
        let (h1, h2) = Self::hashes(key);
        for h in [h1, h2] {
            let bit = usize::try_from(h).unwrap_or(usize::MAX) & self.bit_mask;
            self.bits[bit / 64] |= 1 << (bit % 64);
        }
    }

    fn maybe_contains(&self, key: &[u8]) -> bool {
        let (h1, h2) = Self::hashes(key);
        for h in [h1, h2] {
            let bit = usize::try_from(h).unwrap_or(usize::MAX) & self.bit_mask;
            if self.bits[bit / 64] & (1 << (bit % 64)) == 0 {
                return false;
            }
        }
        true
    }

    fn hashes(key: &[u8]) -> (u64, u64) {
        // Two cheap FNV-style hashes for the bench. Production would use a
        // proper double-hash like xxHash + a derived linear-combination seed.
        let mut h1: u64 = 0xcbf29ce484222325;
        let mut h2: u64 = 0x100000001b3;
        for &b in key {
            h1 ^= u64::from(b);
            h1 = h1.wrapping_mul(0x100000001b3);
            h2 = h2.wrapping_add(u64::from(b));
            h2 = h2.wrapping_mul(0xcbf29ce484222325);
        }
        (h1, h2)
    }
}

fn build_incoming_batch(total_rows: usize) -> Vec<Box<[u8]>> {
    // 1 % of incoming keys exist (in-range); 99 % are fresh inserts. Matches
    // an INSERT-mostly CDC workload shape.
    let existing_count = INCOMING_BATCH_KEYS / 100;
    let new_count = INCOMING_BATCH_KEYS - existing_count;
    let mut batch = Vec::with_capacity(INCOMING_BATCH_KEYS);
    for i in 0..existing_count {
        // pick rows from the warm keyset; spread across the index space
        let stride = total_rows / existing_count.max(1);
        batch.push(make_key(i * stride));
    }
    for i in 0..new_count {
        // outside the warm range so they're true bloom-negatives
        batch.push(make_key(total_rows + i));
    }
    batch
}

fn bench_keyset_cap(c: &mut Criterion) {
    let mut group = c.benchmark_group("load_existing_keyset_cap_disabled");
    group.sample_size(10);

    for &rows_per_snapshot in ROWS_PER_SNAPSHOT {
        // The warm keyset and bloom are built once outside the timed region —
        // they model the cache that the *next* commit would hit if the cap
        // weren't there. The cold-rebuild lane builds its keyset from scratch
        // every iteration.
        let warm = build_warm_keyset(rows_per_snapshot);
        let incoming = build_incoming_batch(rows_per_snapshot);
        let mut bloom = BloomFilter::new(rows_per_snapshot.max(1));
        for key in warm.keys() {
            bloom.insert(key);
        }

        for &snapshot_count in SNAPSHOT_COUNTS {
            let total_rows = rows_per_snapshot.saturating_mul(snapshot_count + 1);
            group.throughput(Throughput::Elements(
                u64::try_from(total_rows).unwrap_or(u64::MAX),
            ));

            let id = format!("M={rows_per_snapshot}/N={snapshot_count}");

            group.bench_with_input(
                BenchmarkId::new("full_rebuild_when_over_budget", &id),
                &snapshot_count,
                |b, &snapshot_count| {
                    b.iter(|| {
                        let n = run_full_rebuild(rows_per_snapshot, snapshot_count);
                        black_box(n);
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("cached_clone_of_warm_keyset", &id),
                &snapshot_count,
                |b, _| {
                    b.iter(|| {
                        let n = run_cached_clone(&warm);
                        black_box(n);
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("bloom_prefilter_then_targeted_lookups", &id),
                &snapshot_count,
                |b, _| {
                    b.iter(|| {
                        let n = run_bloom_targeted(&bloom, &warm, &incoming);
                        black_box(n);
                    });
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_keyset_cap);
criterion_main!(benches);
