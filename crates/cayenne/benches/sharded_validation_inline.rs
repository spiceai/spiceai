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

//! Anchor for skipping the per-apply OS-thread spawn when a sharded CDC apply
//! is small — a replication-lag (apply-throughput) lever.
//!
//! ## What this measures and why
//!
//! At `cdc_mem_tier_shards > 1`, `validate_and_append_sharded` fans per-shard
//! validation across scoped OS threads (`std::thread::scope`). That pays for FAT
//! applies (the case the design targets), but under replication lag the applies
//! are small and frequent, and an OS-thread spawn is ~tens of µs — so at high
//! transaction rates the spawn, not the validation, bounds per-table apply
//! throughput. Two lanes here, feeding two thresholds in the provider:
//!
//! ### 1. Pure spawn overhead (`sharded_validation_inline`) — the ≤1-shard elision
//!
//! A single-row CDC transaction routes to exactly ONE shard, so the pre-fix code
//! spawned one OS thread to do ~ns of work. The per-shard work is IDENTICAL in
//! both lanes, so the delta is the pure `std::thread::scope`+spawn/join overhead.
//!
//! - `scope_spawn_one` — one shard's validation on a spawned OS thread (pre-fix).
//! - `inline_one` — the same work on the caller thread (post-fix).
//!
//! ### 2. Multi-shard crossover (`sharded_validation_crossover`) — the small-apply threshold
//!
//! When ≥2 shards are non-empty the ≤1-shard elision does not fire, yet a SMALL
//! multi-row transaction (a handful of rows hashed across a few shards) still
//! spawns one OS thread per non-empty shard to validate ~a-few rows each — the
//! spawn dwarfs the work. Parallelism only pays once per-shard validation work
//! exceeds the spawn cost. This lane sweeps per-shard row count `R` over N=4
//! shards and compares the two execution strategies with a FAITHFUL per-row work
//! model — the dominant real per-row cost in `validate_one_shard` is the PK
//! `RowConverter` encode plus the `incoming_keys` `HashSet<OwnedRow>` dedup
//! insert, reproduced here exactly:
//!
//! - `inline_4shards/R` — validate all 4 shards sequentially on the caller thread.
//! - `spawn_4shards/R`  — `std::thread::scope` one thread per shard (the pre-fix path).
//!
//! The crossover `R*` (where `spawn` first beats `inline`) sets the provider's
//! total-row inline threshold: applies with `total_rows ≤ T` for `T` chosen
//! conservatively below `4·R*` skip the spawn; fat applies keep the parallel path.

#![allow(clippy::expect_used)]

use std::collections::HashSet;
use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow_row::{OwnedRow, RowConverter, SortField};
use arrow_schema::DataType;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

/// A tiny stand-in for one shard's validation payload on a 1-row shard. The exact
/// work is identical in both lanes; the bench isolates the thread-spawn overhead,
/// not this computation.
fn tiny_shard_work() -> u64 {
    let mut acc = 0x5125_2026_0703_0001u64;
    for i in 0..8u64 {
        acc = acc.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(i);
    }
    acc
}

fn bench_inline_vs_spawn(c: &mut Criterion) {
    let mut group = c.benchmark_group("sharded_validation_inline");

    // Pre-fix: the ≤1-non-empty-shard apply still spawned one scoped OS thread.
    group.bench_function("scope_spawn_one", |b| {
        b.iter(|| {
            std::thread::scope(|scope| {
                let handle = scope.spawn(|| black_box(tiny_shard_work()));
                black_box(handle.join().expect("join"))
            })
        });
    });

    // Post-fix: the identical validation runs on the caller thread, no spawn.
    group.bench_function("inline_one", |b| {
        b.iter(|| black_box(tiny_shard_work()));
    });

    group.finish();
}

/// The number of shards the crossover sweep fans across — matches the common
/// multi-shard config (`cdc_mem_tier_shards = 4`) so the spawn count per apply is
/// representative.
const N_SHARDS: usize = 4;

/// One shard's real per-row validation cost, reproduced faithfully: encode `r`
/// Int64 primary keys through the shared `RowConverter` and dedup them into the
/// per-shard `incoming_keys` set — the two dominant per-row costs in
/// `validate_one_shard`. Returns the deduped count so the work can't be elided.
fn validate_proxy(pk: &ArrayRef, converter: &RowConverter) -> usize {
    let rows = converter
        .convert_columns(std::slice::from_ref(pk))
        .expect("convert PK column");
    let mut seen: HashSet<OwnedRow> = HashSet::with_capacity(rows.num_rows());
    for row in rows.iter() {
        seen.insert(row.owned());
    }
    seen.len()
}

fn bench_crossover(c: &mut Criterion) {
    let converter =
        RowConverter::new(vec![SortField::new(DataType::Int64)]).expect("build PK converter");
    let mut group = c.benchmark_group("sharded_validation_crossover");

    for r in [1usize, 2, 4, 8, 16, 32, 64, 128] {
        // Distinct keys per shard so the dedup set actually fills (worst case for
        // the inline lane, i.e., conservative for the crossover).
        let pk: ArrayRef = Arc::new(Int64Array::from((0..r as i64).collect::<Vec<_>>()));

        // Inline: all N shards validated sequentially on the caller thread.
        group.bench_with_input(BenchmarkId::new("inline_4shards", r), &r, |b, _| {
            b.iter(|| {
                let mut acc = 0usize;
                for _ in 0..N_SHARDS {
                    acc += validate_proxy(black_box(&pk), &converter);
                }
                black_box(acc)
            });
        });

        // Spawn: one scoped OS thread per shard (the pre-threshold parallel path).
        group.bench_with_input(BenchmarkId::new("spawn_4shards", r), &r, |b, _| {
            b.iter(|| {
                std::thread::scope(|scope| {
                    let handles: Vec<_> = (0..N_SHARDS)
                        .map(|_| scope.spawn(|| validate_proxy(black_box(&pk), &converter)))
                        .collect();
                    let mut acc = 0usize;
                    for h in handles {
                        acc += h.join().expect("join shard");
                    }
                    black_box(acc)
                })
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_inline_vs_spawn, bench_crossover);
criterion_main!(benches);
