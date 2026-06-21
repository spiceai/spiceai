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

//! Incremental-view-maintenance (IVM) retraction anchor for the Cayenne
//! maintained-aggregate path.
//!
//! ## What this measures and why
//!
//! Cayenne already maintains grouped `COUNT`/`SUM`/`AVG` incrementally from the
//! CDC stream ([`cayenne::maintained_aggregate::MaintainedAggregateRegistry`]),
//! and the optimizer serves a matching query from that state instead of
//! re-scanning. The maintainer's own doc states the one gap: an UPDATE/DELETE
//! that "needs a retraction but cannot provide the old row values marks the view
//! stale" — i.e. the next query falls back to a full O(N) recompute.
//!
//! This bench anchors the magnitude of closing that gap with a **per-key
//! contribution index** (DBSP's "indexed input"): the maintained view keeps
//! `pk -> (group, contribution)`, so an UPDATE/DELETE retracts the old value
//! from the index — no CDC before-image required. That is what makes the lever
//! work for PostgreSQL logical replication and MongoDB change streams in their
//! default config (they deliver only the primary key on UPDATE/DELETE).
//!
//! Two lanes:
//! - `ivm_maintained_insert` — drives the **real** `MaintainedAggregateRegistry`
//!   insert path ([`MaintainedAggregateRegistry::apply_insert_batches`]) so the
//!   existing maintained-aggregate throughput is tracked against regressions.
//! - `ivm_recompute_vs_maintain` — the asymptote: a full re-aggregate
//!   (single-thread, and a parallel partition+merge modelling a vectorized
//!   multi-core re-scan) vs applying a small delta to a maintained view with the
//!   proposed per-key-index retraction. Recompute is O(N); maintain is O(delta),
//!   serve is O(groups) — so the ratio widens with table size.
//!
//! The retraction logic here (`MaintainedView`) is the reference for the
//! follow-up `maintained_aggregate.rs` change; this bench's `assert`-guarded
//! correctness check (sums identical to a full recompute over
//! inserts+updates+deletes) is run before any timing.
//!
//! ## Measured (Apple Silicon, 12 perf cores; full sweep + the real DuckDB
//! comparison live in the PR description). Per-query answer cost:
//! maintained serve is O(groups), flat ~16 us across 1000x of N, while a
//! re-scan is O(N):
//!
//! | N | DuckDB v1.5.3 re-scan (12-core) | serve (O(G), flat) | speedup |
//! |---|---|---|---|
//! | 100k | 1.0 ms | 16 us | 62x |
//! | 1M | 2.0 ms | 16 us | 124x |
//! | 10M | 6.5 ms | 16 us | 404x |
//! | 100M | 70 ms | 16 us | 4353x |
//!
//! Reproduce the DuckDB side with the duckdb CLI:
//! ```sql
//! PRAGMA threads=12;
//! CREATE OR REPLACE TABLE t AS
//!   SELECT i AS pk, (hash(i*2654435761) % 1000)::INT AS g,
//!          ((hash(i) % 2001)::BIGINT - 1000) AS x
//!   FROM range(100000000) tbl(i);
//! .timer on
//! SELECT sum(s) FROM (SELECT g, SUM(x) AS s FROM t GROUP BY g);
//! ```

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_precision_loss)]

use std::collections::{BTreeMap, HashMap};
use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use cayenne::maintained_aggregate::{
    MaintainedAggregateExpr, MaintainedAggregateFunction, MaintainedAggregateRegistry,
    MaintainedAggregateSpec,
};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

const GROUP_COUNT: usize = 1_000;
const DELTA_ROWS: usize = 100;
const THREAD_COUNT: usize = 12;
const SEED: u64 = 0x5125_2026_0621_0001;

// ----------------------------- workload (model) -----------------------------

/// Deterministic, zero-dependency SplitMix64 so the workload is reproducible.
struct Rng {
    state: u64,
}

impl Rng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        assert!(bound > 0, "below() requires a positive bound");
        self.next_u64() % bound
    }
}

/// `Upsert` covers INSERT and UPDATE (Cayenne models both as upsert-by-PK);
/// `Delete` carries only the key, as PG/Mongo CDC deliver it by default.
#[derive(Clone, Copy)]
enum Op {
    Upsert { pk: u64, group: u32, value: i64 },
    Delete { pk: u64 },
}

/// Column-oriented base table for `SUM(value) GROUP BY group`.
struct Columns {
    group: Vec<u32>,
    value: Vec<i64>,
}

impl Columns {
    fn len(&self) -> usize {
        self.group.len()
    }
}

fn build_base(row_count: usize, group_count: usize, rng: &mut Rng) -> Columns {
    assert!(row_count > 0, "row_count must be positive");
    let mut columns = Columns {
        group: Vec::with_capacity(row_count),
        value: Vec::with_capacity(row_count),
    };
    for _ in 0..row_count {
        columns.group.push(rng.below(group_count as u64) as u32);
        columns.value.push((rng.below(2001) as i64) - 1000);
    }
    assert_eq!(columns.len(), row_count, "built the requested rows");
    columns
}

/// ~50% insert (new PK), ~30% update (existing PK), ~20% delete (existing PK).
fn generate_ops(op_count: usize, base_rows: usize, group_count: usize, rng: &mut Rng) -> Vec<Op> {
    assert!(op_count > 0, "op_count must be positive");
    let mut ops = Vec::with_capacity(op_count);
    let mut next_new_pk = base_rows as u64;
    for _ in 0..op_count {
        let roll = rng.below(100);
        let op = if roll < 50 {
            let pk = next_new_pk;
            next_new_pk += 1;
            Op::Upsert {
                pk,
                group: rng.below(group_count as u64) as u32,
                value: (rng.below(2001) as i64) - 1000,
            }
        } else if roll < 80 {
            Op::Upsert {
                pk: rng.below(base_rows as u64),
                group: rng.below(group_count as u64) as u32,
                value: (rng.below(2001) as i64) - 1000,
            }
        } else {
            Op::Delete {
                pk: rng.below(base_rows as u64),
            }
        };
        ops.push(op);
    }
    ops
}

/// The proposed maintained state: per-group accumulators + the per-key
/// contribution index that enables retraction with no before-image.
#[derive(Clone)]
struct MaintainedView {
    accumulators: HashMap<u32, i128>,
    contribution_index: HashMap<u64, (u32, i64)>,
}

impl MaintainedView {
    fn with_capacity(keys: usize, groups: usize) -> Self {
        Self {
            accumulators: HashMap::with_capacity(groups),
            contribution_index: HashMap::with_capacity(keys),
        }
    }

    fn apply_upsert(&mut self, pk: u64, group: u32, value: i64) {
        if let Some((old_group, old_value)) = self.contribution_index.insert(pk, (group, value)) {
            let slot = self
                .accumulators
                .get_mut(&old_group)
                .expect("a group in the index must have an accumulator");
            *slot -= i128::from(old_value);
        }
        *self.accumulators.entry(group).or_insert(0) += i128::from(value);
    }

    fn apply_delete(&mut self, pk: u64) {
        if let Some((old_group, old_value)) = self.contribution_index.remove(&pk) {
            let slot = self
                .accumulators
                .get_mut(&old_group)
                .expect("a group in the index must have an accumulator");
            *slot -= i128::from(old_value);
        }
    }

    fn apply(&mut self, op: &Op) {
        match *op {
            Op::Upsert { pk, group, value } => self.apply_upsert(pk, group, value),
            Op::Delete { pk } => self.apply_delete(pk),
        }
    }

    /// O(G) "serve the maintained result": snapshot non-zero groups.
    fn serve(&self) -> Vec<(u32, i128)> {
        let mut out: Vec<(u32, i128)> = self
            .accumulators
            .iter()
            .filter(|(_, sum)| **sum != 0)
            .map(|(group, sum)| (*group, *sum))
            .collect();
        out.sort_by_key(|(group, _)| *group);
        out
    }

    fn checksum(&self) -> i128 {
        self.accumulators.values().copied().sum()
    }
}

fn build_maintained(base: &Columns) -> MaintainedView {
    let mut view = MaintainedView::with_capacity(base.len() * 2, GROUP_COUNT);
    for index in 0..base.len() {
        view.apply_upsert(index as u64, base.group[index], base.value[index]);
    }
    view
}

/// Full re-aggregation, single thread — models Cayenne's current stale-recompute.
fn recompute(columns: &Columns) -> HashMap<u32, i128> {
    let mut sums: HashMap<u32, i128> = HashMap::new();
    for index in 0..columns.len() {
        *sums.entry(columns.group[index]).or_insert(0) += i128::from(columns.value[index]);
    }
    sums
}

/// Full re-aggregation, parallel — models a vectorized multi-core re-scan.
/// Still O(N); only the constant shrinks.
fn recompute_parallel(columns: &Columns, thread_count: usize) -> HashMap<u32, i128> {
    assert!(thread_count >= 1, "need at least one thread");
    let row_count = columns.len();
    let chunk = row_count.div_ceil(thread_count);
    assert!(chunk > 0, "chunk must be positive");
    let partials: Vec<HashMap<u32, i128>> = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(thread_count);
        for worker in 0..thread_count {
            let start = worker * chunk;
            if start >= row_count {
                break;
            }
            let end = ((worker + 1) * chunk).min(row_count);
            let groups = &columns.group[start..end];
            let values = &columns.value[start..end];
            handles.push(scope.spawn(move || {
                let mut local: HashMap<u32, i128> = HashMap::new();
                for index in 0..groups.len() {
                    *local.entry(groups[index]).or_insert(0) += i128::from(values[index]);
                }
                local
            }));
        }
        handles
            .into_iter()
            .map(|handle| handle.join().expect("recompute worker panicked"))
            .collect()
    });
    let mut merged: HashMap<u32, i128> = HashMap::new();
    for partial in partials {
        for (group, sum) in partial {
            *merged.entry(group).or_insert(0) += sum;
        }
    }
    merged
}

/// Correctness gate: the per-key-index retraction must equal a full recompute
/// over the effective dataset after inserts + updates + deletes. Run before any
/// timing — a fast wrong answer is worthless.
fn assert_retraction_matches_recompute() {
    let mut rng = Rng::new(SEED ^ 0xC0FFEE);
    let base = build_base(20_000, GROUP_COUNT, &mut rng);
    let mut view = build_maintained(&base);
    let mut mirror: BTreeMap<u64, (u32, i64)> = BTreeMap::new();
    for index in 0..base.len() {
        mirror.insert(index as u64, (base.group[index], base.value[index]));
    }

    let delta = generate_ops(2_000, base.len(), GROUP_COUNT, &mut rng);
    let mut saw_update = false;
    let mut saw_delete = false;
    for op in &delta {
        match *op {
            Op::Upsert { pk, group, value } => {
                saw_update |= mirror.contains_key(&pk);
                view.apply_upsert(pk, group, value);
                mirror.insert(pk, (group, value));
            }
            Op::Delete { pk } => {
                saw_delete = true;
                view.apply_delete(pk);
                mirror.remove(&pk);
            }
        }
    }
    assert!(saw_update, "delta must exercise the UPDATE retraction path");
    assert!(saw_delete, "delta must exercise the DELETE retraction path");

    let mut truth: HashMap<u32, i128> = HashMap::new();
    for (group, value) in mirror.values() {
        *truth.entry(*group).or_insert(0) += i128::from(*value);
    }
    let mut groups: Vec<u32> = view.accumulators.keys().chain(truth.keys()).copied().collect();
    groups.sort_unstable();
    groups.dedup();
    for group in groups {
        let lhs = view.accumulators.get(&group).copied().unwrap_or(0);
        let rhs = truth.get(&group).copied().unwrap_or(0);
        assert_eq!(
            lhs, rhs,
            "per-key-index retraction diverged from recompute for group {group}"
        );
    }
}

// --------------------------- lane 1: real registry --------------------------

fn registry_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("group", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]))
}

fn sum_spec() -> MaintainedAggregateSpec {
    MaintainedAggregateSpec {
        group_by: vec!["group".to_string()],
        aggregates: vec![MaintainedAggregateExpr {
            function: MaintainedAggregateFunction::Sum,
            column: Some("value".to_string()),
        }],
    }
}

fn registry_batch(rows: usize, groups: usize) -> RecordBatch {
    let group_count = groups.max(1);
    let group_col: Vec<i64> = (0..rows).map(|i| (i % group_count) as i64).collect();
    let value_col: Vec<i64> = (0..rows).map(|i| (i as i64 % 2001) - 1000).collect();
    RecordBatch::try_new(
        registry_schema(),
        vec![
            Arc::new(Int64Array::from(group_col)),
            Arc::new(Int64Array::from(value_col)),
        ],
    )
    .expect("registry batch should be valid")
}

/// Track the real `MaintainedAggregateRegistry` insert throughput so the
/// existing maintained-aggregate path is guarded against regressions.
fn bench_real_registry_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("ivm_maintained_insert");
    for &rows in &[16_384usize, 131_072] {
        for &cardinality in &[8usize, 1_024] {
            let groups = cardinality.min(rows);
            group.throughput(Throughput::Elements(rows as u64));
            group.bench_with_input(
                BenchmarkId::from_parameter(format!("rows={rows}/groups={groups}")),
                &rows,
                |b, &rows| {
                    b.iter_batched(
                        || {
                            let registry =
                                MaintainedAggregateRegistry::try_new(&[sum_spec()], &registry_schema())
                                    .expect("registry construction");
                            (registry, registry_batch(rows, groups))
                        },
                        |(registry, batch)| {
                            registry
                                .apply_insert_batches(1, &[batch])
                                .expect("apply_insert_batches");
                        },
                        criterion::BatchSize::SmallInput,
                    );
                },
            );
        }
    }
    group.finish();
}

// --------------------------- lane 2: the asymptote --------------------------

fn bench_recompute_vs_maintain(c: &mut Criterion) {
    assert_retraction_matches_recompute();

    let mut group = c.benchmark_group("ivm_recompute_vs_maintain");
    for &rows in &[100_000usize, 1_000_000] {
        let mut rng = Rng::new(SEED ^ rows as u64);
        let base = build_base(rows, GROUP_COUNT, &mut rng);
        let maintained = build_maintained(&base);
        let delta = generate_ops(DELTA_ROWS, rows, GROUP_COUNT, &mut rng);
        group.throughput(Throughput::Elements(rows as u64));

        group.bench_with_input(
            BenchmarkId::new("recompute_1thread", rows),
            &rows,
            |b, _| b.iter(|| black_box(recompute(&base).values().copied().sum::<i128>())),
        );
        group.bench_with_input(
            BenchmarkId::new("recompute_12thread", rows),
            &rows,
            |b, _| {
                b.iter(|| {
                    black_box(
                        recompute_parallel(&base, THREAD_COUNT)
                            .values()
                            .copied()
                            .sum::<i128>(),
                    )
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("maintain_delta", rows),
            &rows,
            |b, _| {
                b.iter_batched(
                    || maintained.clone(),
                    |mut view| {
                        for op in &delta {
                            view.apply(op);
                        }
                        black_box(view.checksum());
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
        group.bench_with_input(BenchmarkId::new("serve", rows), &rows, |b, _| {
            b.iter(|| black_box(maintained.serve().len()));
        });
    }
    group.finish();
}

criterion_group!(benches, bench_real_registry_insert, bench_recompute_vs_maintain);
criterion_main!(benches);
