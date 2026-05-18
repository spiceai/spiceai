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

//! Regression bench: per-upsert cost of the inline-memtable rewrite path.
//!
//! `CayenneTableProvider::build_inlined_data_rewrite_for_pk_keys`
//! (`crates/cayenne/src/provider/table.rs:3917-3987`) is invoked from
//! every upsert / on-conflict insert whose deleted-PK set is non-empty
//! and the table has pending inline rows
//! (`apply_on_conflict_deletions` -> rewrite branch). On each call it
//! performs:
//!
//! ```ignore
//! let inlined_data = self.catalog.get_inlined_data(&id).await?;          // 1 metastore RTT
//! let legacy_inlined_deletions = self.load_inlined_deletion_maps().await?; // 1 more metastore RTT
//! for entry in inlined_data {
//!     let batches = deserialize_ipc_to_batch(&entry.data_ipc)?;            // Arrow IPC decode
//!     for batch in batches {
//!         let Some(visible_batch) = self.filter_inlined_batch_for_deletions(...)?
//!         else { continue };
//!         let (filtered_batch, removed_rows) =
//!             self.filter_inlined_batch_for_pk_deletions(...);             // new PK filter
//!         ...
//!     }
//! }
//! ```
//!
//! The just-committed `read_inlined_batches` cache (keyed by an
//! `AtomicU64` inline generation) eliminates the same IPC-decode +
//! deletion-filter work for the **scan** path, but the upsert rewrite
//! path bypasses the cache and pays the full cost on every commit.
//! `commit_inlined_data_mutation` -> `build_inlined_data_rewrite_for_pk_keys`
//! is the inner loop of an on-conflict CDC stream where every envelope
//! upserts a single PK; at that shape the redundant decode dominates
//! the per-upsert CPU budget.
//!
//! Two consequences:
//!
//! 1. **Per-upsert fixed cost**: each upsert against a table with
//!    1 MiB of inlined data pays ~100 µs–1 ms of IPC decode plus two
//!    metastore round-trips, even though `read_inlined_batches` may
//!    have decoded the same payload milliseconds earlier.
//! 2. **Cache-coherence asymmetry**: writers serially invalidate the
//!    scan cache (good), but each writer's *own* rewrite step then
//!    re-pays the decode cost the cache was designed to amortize.
//!
//! The TigerStyle remedy is to share the existing
//! `read_inlined_batches` cache: have `build_inlined_data_rewrite_for_pk_keys`
//! call `read_inlined_batches` and apply only the new PK filter on top,
//! rather than re-reading and re-decoding `cayenne_inlined_data`.
//!
//! ## What this bench measures
//!
//! Pure CPU shape — no metastore, no Cayenne setup. Models the
//! per-upsert decode + double-filter cost.
//!
//! Two lanes per inline data size:
//!
//! - `decode_and_filter_per_upsert/<rows>` — mirrors today's
//!   `build_inlined_data_rewrite_for_pk_keys`: deserialize the IPC
//!   payload, build a deletion-mask (legacy inline deletes, modelled
//!   as empty since legacy writes are gated on a separate code path),
//!   and apply a PK-set filter producing the rewritten batch.
//! - `cached_filter_per_upsert/<rows>` — models the proposed share:
//!   start from pre-decoded `Vec<RecordBatch>` (as if reusing the
//!   scan cache), then apply only the new PK filter.
//!
//! Inline sizes mirror `inline_memtable_read_overhead`:
//!
//! - 1 KiB: a single small CDC envelope.
//! - 100 KiB: a few dozen envelopes, typical between checkpoints.
//! - 1 MiB: near the inline-memtable flush threshold.
//!
//! ## How to read
//!
//! `cargo bench --bench inline_upsert_rewrite_overhead -p cayenne`.
//!
//! - `decode_and_filter_per_upsert/1MiB` is the per-upsert CPU cost a
//!   high-conflict CDC stream pays today. At 1000 upserts/sec this is
//!   the latency floor below which p99 cannot go.
//! - `cached_filter_per_upsert/1MiB` is the achievable floor if the
//!   rewrite path reuses the scan cache. The ratio is the QPS
//!   headroom from the sharing fix.

#![allow(clippy::expect_used)]

use std::collections::HashSet;
use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

/// Row counts straddling realistic inline-memtable sizes:
/// - 64    rows ≈ ~1 KiB IPC payload (one envelope).
/// - 4096  rows ≈ ~100 KiB.
/// - 32768 rows ≈ ~1 MiB (near the typical
///   `inline_flush_max_bytes` threshold).
const INLINE_ROW_COUNTS: &[usize] = &[64, 4_096, 32_768];

/// Fraction of inline rows whose PK is in the upsert delete-set on each
/// rewrite. 10 % matches the shape of a CDC stream that occasionally
/// re-keys but is mostly net-new rows; the absolute filter cost is
/// linear in this fraction, but the IPC decode is paid in full
/// regardless.
const UPSERT_HIT_FRACTION: f64 = 0.10;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn make_batch(rows: usize) -> RecordBatch {
    let ids: Vec<i64> = (0..rows as i64).collect();
    let names: Vec<String> = (0..rows).map(|i| format!("row_{i}")).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .expect("batch")
}

/// PK set the upsert is rewriting. Picks every Nth row, where N is
/// chosen so `UPSERT_HIT_FRACTION` of the rows match. The actual
/// rewrite work `build_inlined_data_rewrite_for_pk_keys` does scales
/// with the **filter mask construction**, not with the number of hits,
/// because the mask is built row-by-row.
fn upsert_pk_set(rows: usize) -> HashSet<i64> {
    let hits = ((rows as f64) * UPSERT_HIT_FRACTION).max(1.0) as usize;
    let stride = rows / hits.max(1);
    (0..rows).step_by(stride.max(1)).map(|i| i as i64).collect()
}

/// Serialize a `RecordBatch` to Arrow IPC bytes — matches the
/// production storage shape (`cayenne_inlined_data.data_ipc` blob).
fn serialize_ipc(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema()).expect("writer");
        writer.write(batch).expect("write");
        writer.finish().expect("finish");
    }
    buf
}

/// Mirrors `deserialize_ipc_to_batch` (`table.rs:793`): decode the IPC
/// stream into one or more `RecordBatch`es.
fn deserialize_ipc(blob: &[u8]) -> Vec<RecordBatch> {
    let reader = StreamReader::try_new(blob, None).expect("ipc reader");
    reader
        .collect::<arrow::error::Result<Vec<_>>>()
        .expect("decode")
}

/// Mirrors `filter_inlined_batch_for_pk_deletions` for the Int64 PK
/// strategy: build a `keep_mask` Vec<bool> by probing each row's PK
/// against the upsert delete-set, then materialize the filtered batch.
fn apply_pk_filter(batch: &RecordBatch, deleted: &HashSet<i64>) -> RecordBatch {
    let pk_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 PK");
    let mut keep_mask = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        keep_mask.push(!deleted.contains(&pk_array.value(row)));
    }
    let mask = BooleanArray::from(keep_mask);
    filter_record_batch(batch, &mask).expect("filter")
}

/// Lane A: today's per-upsert pattern — decode IPC blob, run
/// (modelled) legacy-deletion filter, then PK-set filter, then
/// materialize the rewritten batch.
fn decode_and_filter_per_upsert(blob: &[u8], deleted: &HashSet<i64>) -> usize {
    let batches = deserialize_ipc(blob);
    let mut total_rows = 0_usize;
    for batch in &batches {
        // legacy-deletion filter is modelled as a no-op here: in steady
        // state writes go through `commit_inlined_data_mutation` which
        // never writes `cayenne_inlined_delete`. The decode cost is
        // paid in full regardless of legacy-delete population, so this
        // accurately captures the per-upsert ceiling.
        let filtered = apply_pk_filter(batch, deleted);
        total_rows += filtered.num_rows();
    }
    black_box(&batches);
    total_rows
}

/// Lane B: cached pre-decoded batches — apply only the new PK filter
/// (no IPC decode, no extra metastore round-trip).
fn cached_filter_per_upsert(cached: &Arc<Vec<RecordBatch>>, deleted: &HashSet<i64>) -> usize {
    let mut total_rows = 0_usize;
    for batch in cached.iter() {
        let filtered = apply_pk_filter(batch, deleted);
        total_rows += filtered.num_rows();
    }
    total_rows
}

fn bench_inline_upsert_rewrite(c: &mut Criterion) {
    let mut group = c.benchmark_group("inline_upsert_rewrite_overhead");
    for &rows in INLINE_ROW_COUNTS {
        let batch = make_batch(rows);
        let blob = serialize_ipc(&batch);
        let cached = Arc::new(vec![batch.clone()]);
        let deleted = upsert_pk_set(rows);

        group.throughput(Throughput::Elements(
            u64::try_from(rows).unwrap_or(u64::MAX),
        ));

        group.bench_with_input(
            BenchmarkId::new("decode_and_filter_per_upsert", rows),
            &(blob.clone(), deleted.clone()),
            |b, (blob, deleted)| {
                b.iter(|| decode_and_filter_per_upsert(black_box(blob.as_slice()), deleted));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("cached_filter_per_upsert", rows),
            &(Arc::clone(&cached), deleted),
            |b, (cached, deleted)| {
                b.iter(|| cached_filter_per_upsert(black_box(cached), deleted));
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_inline_upsert_rewrite);
criterion_main!(benches);
