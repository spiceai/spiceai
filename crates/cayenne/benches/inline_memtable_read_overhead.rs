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

//! Regression bench: per-scan cost of the inline-memtable read path.
//!
//! Older versions of `CayenneTableProvider::read_inlined_batches` re-decoded
//! the inline data on every `scan()` against a table with a non-empty inline
//! memtable:
//!
//! ```ignore
//! let inlined = self.catalog.get_inlined_data(&id).await?;             // 1 metastore RTT
//! let inlined_deletions = self.load_inlined_deletion_maps().await?;     // 1 more metastore RTT
//! for entry in &inlined {
//!     let entry_batches = deserialize_ipc_to_batch(&entry.data_ipc)?;   // Arrow IPC decode
//!     for batch in entry_batches {
//!         if let Some(filtered) = self.filter_inlined_batch_for_deletions(...) {
//!             batches.push(filtered);
//!         }
//!     }
//! }
//! ```
//!
//! There was no in-memory cache of the deserialized `Vec<RecordBatch>`, even
//! though the inlined state is static between writes and checkpoints. A CDC
//! table with 1 MiB of inlined data paid ~100 µs–1 ms of IPC decode per scan
//! plus 2 metastore RTTs.
//!
//! The production path now caches the decoded batches by inline generation —
//! `inlined_generation: Arc<AtomicU64>` ([`provider/table.rs:1079`]) is bumped
//! by every `commit_inlined_mutation` and `clear_inlined_data_and_deletes`,
//! and `inlined_cache: Arc<ArcSwap<InlinedCache>>` stores the
//! `(generation, Arc<Vec<RecordBatch>>)` pair. `read_inlined_batches`
//! atomic-loads the generation; on a match it returns the cached `Arc`
//! (wait-free); on a miss it rebuilds and stores. The two metastore RTTs
//! and the IPC decode happen only when the generation actually changed.
//!
//! ## What this bench measures
//!
//! Pure shape — no metastore, no Cayenne setup. Models the CPU-side cost of
//! the read path: Arrow IPC deserialize + per-row deletion-mask probe.
//!
//! Two lanes per inline data size:
//!
//! - `decode_per_scan_baseline/<rows>` — mirrors the older `read_inlined_batches`:
//!   re-deserialize the IPC payload on every iteration and rebuild the
//!   filtered batch. The "metastore round trip" is not modeled because the
//!   pool parallelizes it; what remains is the CPU-bound IPC decode.
//! - `cached_arc_clone/<rows>` — current behavior: a single pre-decoded
//!   `Arc<Vec<RecordBatch>>` cloned per scan. Wall time is one `Arc::clone`
//!   plus the downstream usage (the `black_box`).
//!
//! Inline sizes:
//!
//! - 1 KiB: a single small CDC envelope.
//! - 100 KiB: a few dozen envelopes, typical between checkpoints.
//! - 1 MiB: near the inline-memtable flush threshold.
//!
//! ## How to read
//!
//! `cargo bench --bench inline_memtable_read_overhead -p cayenne`.
//!
//! - `decode_per_scan_baseline/1MiB` is the per-scan fixed cost a
//!   freshness-probe table would pay between checkpoints without the cache.
//! - `cached_arc_clone/1MiB` is the current floor. The ratio is the
//!   QPS headroom the cache delivered.

#![allow(clippy::expect_used)]

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
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

/// Lane A: today's per-scan pattern — re-deserialize the IPC blob on
/// every scan and pretend to hand the batches to the downstream
/// MemorySourceConfig.
fn decode_per_scan_baseline(blob: &[u8]) -> usize {
    let batches = deserialize_ipc(blob);
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    black_box(&batches);
    total_rows
}

/// Lane B: cached pre-decoded batches — one `Arc::clone` per scan.
fn cached_arc_clone(cached: &Arc<Vec<RecordBatch>>) -> usize {
    let clone = Arc::clone(cached);
    let total_rows: usize = clone.iter().map(RecordBatch::num_rows).sum();
    black_box(&clone);
    total_rows
}

fn bench_inline_memtable_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("inline_memtable_read_overhead");
    for &rows in INLINE_ROW_COUNTS {
        let batch = make_batch(rows);
        let blob = serialize_ipc(&batch);
        let cached = Arc::new(vec![batch.clone()]);

        group.throughput(Throughput::Elements(
            u64::try_from(rows).unwrap_or(u64::MAX),
        ));

        group.bench_with_input(
            BenchmarkId::new("decode_per_scan_baseline", rows),
            &blob,
            |b, blob| {
                b.iter(|| decode_per_scan_baseline(black_box(blob.as_slice())));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("cached_arc_clone", rows),
            &cached,
            |b, cached| {
                b.iter(|| cached_arc_clone(black_box(cached)));
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_inline_memtable_read);
criterion_main!(benches);
