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
//! touches ≤1 shard — a replication-lag (apply-throughput) lever.
//!
//! ## What this measures and why
//!
//! At `cdc_mem_tier_shards > 1`, `validate_and_append_sharded` fans per-shard
//! validation across scoped OS threads (`std::thread::scope`). That pays for FAT
//! applies (the case the design targets), but under replication lag the applies
//! are small and frequent — a single-row CDC transaction routes to exactly ONE
//! shard, so the code spawned one OS thread to do ~ns of work. The fix validates
//! inline (on the caller thread) whenever ≤1 shard is non-empty, eliminating the
//! spawn for that (very common) shape.
//!
//! Replication lag is a THROUGHPUT phenomenon (lag stays bounded only while
//! sustained apply drain ≥ arrival), and an OS-thread spawn is ~tens of µs — so
//! at high single-row-transaction rates the spawn, not the validation, bounds
//! per-table apply throughput. This bench isolates exactly that overhead: the
//! per-shard work is identical in both lanes; only the `std::thread::scope` +
//! spawn/join wrapper differs.
//!
//! - `scope_spawn_one` — one shard's validation on a spawned OS thread (pre-fix).
//! - `inline_one` — the same work on the caller thread (post-fix).
//!
//! The delta is the per-(≤1-shard)-apply CPU/latency the fix recovers. `≥2`
//! non-empty shards keep the parallel path (fat applies still amortize the spawn).

#![allow(clippy::expect_used)]

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};

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

criterion_group!(benches, bench_inline_vs_spawn);
criterion_main!(benches);
