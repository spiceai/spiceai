// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Anchor for the sharded-CDC-apply per-batch `RowConverter` reuse — a
//! replication-lag (apply-throughput) lever.
//!
//! ## What this measures and why
//!
//! At `cdc_mem_tier_shards > 1`, every CDC apply splits each batch into per-shard
//! sub-batches (`split_batch_by_pk_shard`), which encodes the PK columns to
//! `OwnedRow` bytes (`RowConverter::convert_columns`) to route rows by
//! `hash(pk) % n`. The pre-fix code rebuilt a fresh `RowConverter` **per batch**
//! via `build_pk_converter` — a redundant `RowConverter::new` — even though the
//! whole apply already holds an identical converter (`validate_and_append_sharded`
//! passes one in). The fix threads that converter into the split and drops the
//! rebuild.
//!
//! Replication lag is a THROUGHPUT phenomenon (lag stays bounded only while
//! sustained apply drain ≥ arrival), and the per-apply CPU is dominated by exactly
//! this validation/encode work. The rebuild's fixed cost bites hardest in the
//! **small-batch, high-frequency** CDC regime (single-row / few-row transactions
//! at high rate) — the regime where a table falls behind and lag grows. So this
//! bench measures the removed per-batch cost across batch sizes:
//! - `rebuild_per_batch` — `RowConverter::new(fields)` + `convert_columns` (pre-fix).
//! - `reuse_converter` — `convert_columns` on a shared converter (post-fix).
//!
//! The delta is the per-batch CPU the fix recovers; multiplied by the CDC batch
//! rate it is the apply-throughput headroom (and thus lag slack) recovered. A
//! byte-identical correctness gate (reuse ≡ rebuild) runs before any timing — a
//! faster-but-different routing would be a correctness bug, not a win.
//!
//! Composite PK `(Int64 id, Utf8 tenant)` — a common CDC shape that exercises the
//! `RowConverter`. (The sharded split encodes the PK to `OwnedRow` bytes for any
//! PK type — there is no `Int64` fast path in the split itself — but a composite
//! PK is the shape where the per-call converter build cost is most representative.)

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow_row::{RowConverter, SortField};
use arrow_schema::DataType;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

/// A realistic composite CDC primary key: `(Int64, Utf8)`.
fn sort_fields() -> Vec<SortField> {
    vec![
        SortField::new(DataType::Int64),
        SortField::new(DataType::Utf8),
    ]
}

/// `rows` PK tuples with well-spread values (so hashing/routing is realistic).
fn pk_columns(rows: usize) -> Vec<ArrayRef> {
    let ids = Int64Array::from_iter_values((0..rows as i64).map(|i| i.wrapping_mul(2_654_435_761)));
    let tenants = StringArray::from_iter_values((0..rows).map(|i| format!("tenant-{}", i % 32)));
    vec![Arc::new(ids), Arc::new(tenants)]
}

fn bench_converter_reuse(c: &mut Criterion) {
    // Correctness gate (runs before any timing): reusing a shared converter must
    // produce byte-identical `OwnedRow`s to rebuilding one per call, or the shard
    // routing would differ — a correctness regression, not a throughput win.
    {
        // Reusing ONE converter across DIFFERENT batches must be byte-identical to
        // rebuilding one per batch (identical OwnedRows ⇒ identical shard routing)
        // — the exact property the fix relies on.
        let shared = RowConverter::new(sort_fields()).expect("rc");
        for &rows in &[1usize, 64] {
            let cols = pk_columns(rows);
            let reused = shared.convert_columns(&cols).expect("conv");
            let rebuilt = RowConverter::new(sort_fields())
                .expect("rc")
                .convert_columns(&cols)
                .expect("conv");
            assert!(
                reused.iter().eq(rebuilt.iter()),
                "reuse across batches must be byte-identical to per-batch rebuild"
            );
        }
    }

    let mut group = c.benchmark_group("mem_tier_shard_split_converter");
    for &rows in &[1usize, 8, 64, 512] {
        let cols = pk_columns(rows);

        // Pre-fix: a fresh RowConverter every batch, then encode.
        group.bench_with_input(
            BenchmarkId::new("rebuild_per_batch", rows),
            &rows,
            |b, _| {
                b.iter(|| {
                    let converter = RowConverter::new(sort_fields()).expect("rc");
                    black_box(converter.convert_columns(black_box(&cols)).expect("conv"))
                });
            },
        );

        // Post-fix: reuse the apply's already-built converter, then encode.
        let shared = RowConverter::new(sort_fields()).expect("rc");
        group.bench_with_input(BenchmarkId::new("reuse_converter", rows), &rows, |b, _| {
            b.iter(|| black_box(shared.convert_columns(black_box(&cols)).expect("conv")));
        });
    }
    group.finish();
}

criterion_group!(benches, bench_converter_reuse);
criterion_main!(benches);
