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

//! Seq-prefix BAKE shape bench: what a bake pass costs, and how that cost moves
//! with the partition count it plans across.
//!
//! ## Why this exists and why it is not the existing compaction benches
//!
//! `compaction_write_amplification` and `compaction_sort_serialization` both use a
//! single `Int64` primary key. That routes the deletion filter to
//! `Int64PkDeletionFilterExec`, which has a value-range disjoint prune
//! (`int64_branch_disjoint_from_deletions`) and needs no row encoding. The CH-benCH
//! table whose bakes dominate an SF1000 run — `order_line`, PK
//! `(ol_w_id, ol_d_id, ol_o_id, ol_number)` — takes the OTHER branch:
//! `PkDeletionSnapshot::RowConverterBased` -> `KeyBasedDeletionFilterExec`, which
//! row-encodes every batch and has NO range prune (see the deliberate absence of
//! `deleted_key_range()` on `KeyDeletionIndex`: its keys are XXH3-128 hashes, and a
//! hash window cannot be compared against a value window without risking an unsound
//! prune). So the existing benches measure the branch that has the optimisation
//! while production runs the branch that does not. This bench uses a four-column
//! composite PK for that reason.
//!
//! ## Sizing: DO NOT GUESS
//!
//! A bake previously reported duration with no volume, so bake GB/pass was
//! unknown and any volume chosen here would have been arbitrary. Subset compaction
//! on the same tables in the same SF1000 run measured 149-234 MB/s at 0.77-1.84
//! GB/pass, and a bake's p90 was 100-223 s -- numbers that cannot be reconciled
//! without knowing what a bake actually reads. `cayenne_compaction_merged_bytes`
//! under `kind="bake"` now supplies it.
//!
//! Fill in [`BAKE_INPUT_BYTES_PER_PASS`] and [`ROWS_PER_SNAPSHOT`] from a real run
//! before trusting any number this bench produces:
//!
//! ```text
//! GB/pass  = cayenne_compaction_merged_bytes_sum{kind="bake",table="order_line"}
//!            / cayenne_compaction_duration_ms_count{kind="bake",table="order_line"}
//! ```
//!
//! ## Partition sweep: one process per value
//!
//! `SPICE_CAYENNE_COMPACTION_TARGET_PARTITIONS` is resolved through a `LazyLock`
//! (`provider::compaction::COMPACTION_TARGET_PARTITIONS`), so it is read ONCE per
//! process. Sweeping it inside one criterion run would have every group observe
//! whichever value was read first. The sweep is therefore a shell loop over
//! separate invocations:
//!
//! ```shell
//! for p in 1 2 4 8 16 32 64; do
//!   SPICE_CAYENNE_COMPACTION_TARGET_PARTITIONS=$p \
//!     cargo bench -p cayenne --bench bake_partition_shape -- --save-baseline "p$p"
//! done
//! ```
//!
//! ## What the sweep is looking for
//!
//! `execute_stream` wraps a multi-arm `UnionExec` in `CoalescePartitionsExec`
//! (datafusion `execution_plan.rs`), so the read fans out, funnels through ONE
//! stream plus a per-batch cast, and the Vortex sink then re-shards to
//! `encode_shards`. If the funnel bounds the pass, wall time flattens once
//! partitions exceed a small number while the encode side keeps being handed more
//! shards. `compaction_sort_serialization` puts the price of a serial middle at
//! ~40-50x at 4 M elements (a bounded constant factor: serial is ~linear at
//! 16.8-23.7 ns/element, and the ratio widens because the parallel lane amortises
//! its fixed overhead, not because serial degrades).
//!
//! Note two real serial fallbacks this bench must avoid tripping accidentally,
//! since either would measure the wrong thing:
//! - `snapshot_shard_count` returns 1 outright when the table `has_sort_columns()`
//!   -- a sorted write must go through one writer or the global order scatters.
//! - `subset_merge_write_shape(keeps_positions_serial: true, ..)` returns
//!   `(1, None)`, i.e. position-delete tables write serially. The bake is gated to
//!   key mode so it always passes `false`, but a fixture that configured position
//!   deletes would silently serialize.

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};

/// Bytes a single bake pass reads, from `cayenne_compaction_merged_bytes`
/// under `kind="bake"`. `None` until a run has reported it -- the bench refuses to
/// invent a volume, because a wrong regime produces a confident wrong answer.
const BAKE_INPUT_BYTES_PER_PASS: Option<u64> = None;

/// Rows per protected snapshot, derived from the same run alongside
/// [`BAKE_INPUT_BYTES_PER_PASS`].
const ROWS_PER_SNAPSHOT: Option<usize> = None;

/// Protected snapshots a bake needs before a settled prefix exists:
/// `BAKE_KEEP_RECENT_SNAPSHOTS` (3) + 2. Fewer and the pass is a no-op, so a
/// fixture below this measures the early-out rather than the merge.
const MIN_PROTECTED_SNAPSHOTS: usize = 5;

fn bake_partition_shape(c: &mut Criterion) {
    let Some(bytes_per_pass) = BAKE_INPUT_BYTES_PER_PASS else {
        // Deliberately not a panic: the bench is committed ahead of the run that
        // supplies its sizing, and a red bench would be indistinguishable from a
        // real regression in CI.
        eprintln!(
            "bake_partition_shape: SKIPPED -- BAKE_INPUT_BYTES_PER_PASS is unset. \
             Fill it (and ROWS_PER_SNAPSHOT) from cayenne_compaction_merged_bytes \
             under kind=\"bake\" in an SF1000 run; see this file's module docs."
        );
        return;
    };
    let rows_per_snapshot = ROWS_PER_SNAPSHOT.unwrap_or_default();

    let mut group = c.benchmark_group("bake_partition_shape");
    // TODO(sizing): build the fixture on `compaction_write_amplification`'s shape
    // -- temp sqlite catalog + `CayenneTableProvider::create_table` + `write_delta`
    // per protected snapshot -- but with:
    //   - primary_key = (ol_w_id, ol_d_id, ol_o_id, ol_number) so the pass takes
    //     the RowConverterBased / KeyBasedDeletionFilterExec branch,
    //   - an order_line-width schema (~10 columns) so bytes-per-row is realistic,
    //   - MIN_PROTECTED_SNAPSHOTS snapshots sized to bytes_per_pass, with keys
    //     overlapping across deltas so upserts leave tombstones for the prune,
    //   - no sort_columns and key (not position) deletion, per the serial
    //     fallbacks named in the module docs.
    // Then time `bake_seq_prefix_protected_snapshots()` (it is `#[doc(hidden)] pub`
    // for exactly this) and assert it returned `Ok(true)`, since a silent
    // `Ok(false)` early-out would otherwise benchmark nothing.
    group.bench_function("placeholder", |b| {
        b.iter(|| black_box((bytes_per_pass, rows_per_snapshot, MIN_PROTECTED_SNAPSHOTS)));
    });
    group.finish();
}

criterion_group!(benches, bake_partition_shape);
criterion_main!(benches);
