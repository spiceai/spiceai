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

//! Anchor for grouped, CDC-incrementally-maintained `MIN`/`MAX` — the
//! *retraction-hard* extension of the maintained-aggregate lever (F1 / IVM).
//!
//! ## What this measures and why
//!
//! Cayenne already maintains grouped `COUNT`/`SUM`/`AVG` from the CDC delta and
//! serves a matching query from that state in O(groups) instead of re-scanning
//! (see `ivm_retraction_anchor.rs`, which anchors the additive case at
//! 62x→4353x). `MIN`/`MAX` are the aggregates a general-purpose batch engine
//! (DuckDB/chDB) *must* re-scan O(rows) for on every query, and that even a
//! streaming engine finds hard, because they are **retraction-hard**: deleting
//! the current extremum needs the next value, which additive inversion cannot
//! provide. Cayenne closes this by keeping a per-group ordered multiset
//! (`SortedScalarIndex`) fed by the delta, so a `MIN/MAX … GROUP BY` is served
//! O(groups) with O(log distinct) maintenance per delta row.
//!
//! This bench anchors that win on the **real** `MaintainedAggregateRegistry`:
//!
//! - `minmax_recompute_vs_serve` — the asymptote: a full O(rows) re-aggregate
//!   (single-thread, and a parallel partition+merge modelling a vectorized
//!   multi-core re-scan, i.e. the DuckDB-shaped path) vs the real registry's
//!   O(groups) `batch_for_spec` serve. The serve is flat in the table size, so
//!   the ratio widens with rows exactly as the additive anchor does.
//! - `minmax_maintained_insert` — the real registry `apply_insert_batches`
//!   throughput on `MIN`/`MAX` specs, so the new write-path cost (ordered-multiset
//!   insert vs the additive O(1) update) is tracked against the ≤5% ingest
//!   guardrail.
//! - `minmax_maintained_retract` — the retraction-hard path on the shipped
//!   registry: re-upserting resident PKs so each row removes its old value from
//!   the ordered multiset then inserts the new one (the O(delta·log) maintain
//!   cost the lever depends on), measured with the extremum churning.
//!
//! An `assert`-guarded correctness check (real-registry serve identical to a full
//! recompute over inserts + updates + deletes, INCLUDING repeated deletion of the
//! current group extremum) runs before any timing — a fast wrong answer is worthless.
//!
//! Reproduce the DuckDB side with the duckdb CLI:
//! ```sql
//! PRAGMA threads=12;
//! CREATE OR REPLACE TABLE t AS
//!   SELECT i AS pk, (hash(i*2654435761) % 1000)::BIGINT AS g,
//!          ((hash(i) % 2001)::BIGINT - 1000) AS x
//!   FROM range(100000000) tbl(i);
//! .timer on
//! SELECT min(mn), max(mx) FROM (SELECT g, MIN(x) AS mn, MAX(x) AS mx FROM t GROUP BY g);
//! ```

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_sign_loss)]

use std::collections::{BTreeMap, HashMap};
use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{AsArray, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef};
use cayenne::maintained_aggregate::{
    MaintainedAggregateExpr, MaintainedAggregateFunction, MaintainedAggregateRegistry,
    MaintainedAggregateSpec,
};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

const GROUP_COUNT: usize = 1_000;
const THREAD_COUNT: usize = 12;
const SEED: u64 = 0x5125_2026_0702_0001;

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

/// Column-oriented base table for `MIN(value)/MAX(value) GROUP BY group`.
struct Columns {
    group: Vec<i64>,
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
        columns.group.push(rng.below(group_count as u64) as i64);
        columns.value.push((rng.below(2001) as i64) - 1000);
    }
    columns
}

/// Full re-aggregation, single thread — models Cayenne's stale-recompute / the
/// DuckDB re-scan. O(rows). Returns `group -> (min, max)`.
fn recompute(columns: &Columns) -> HashMap<i64, (i64, i64)> {
    let mut out: HashMap<i64, (i64, i64)> = HashMap::new();
    for index in 0..columns.len() {
        let value = columns.value[index];
        out.entry(columns.group[index])
            .and_modify(|(mn, mx)| {
                *mn = (*mn).min(value);
                *mx = (*mx).max(value);
            })
            .or_insert((value, value));
    }
    out
}

/// Full re-aggregation, parallel — models a vectorized multi-core re-scan. Still
/// O(rows); only the constant shrinks.
fn recompute_parallel(columns: &Columns, thread_count: usize) -> HashMap<i64, (i64, i64)> {
    assert!(thread_count >= 1, "need at least one thread");
    let row_count = columns.len();
    let chunk = row_count.div_ceil(thread_count);
    assert!(chunk > 0, "chunk must be positive");
    let partials: Vec<HashMap<i64, (i64, i64)>> = std::thread::scope(|scope| {
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
                let mut local: HashMap<i64, (i64, i64)> = HashMap::new();
                for index in 0..groups.len() {
                    let value = values[index];
                    local
                        .entry(groups[index])
                        .and_modify(|(mn, mx)| {
                            *mn = (*mn).min(value);
                            *mx = (*mx).max(value);
                        })
                        .or_insert((value, value));
                }
                local
            }));
        }
        handles
            .into_iter()
            .map(|handle| handle.join().expect("recompute worker panicked"))
            .collect()
    });
    let mut merged: HashMap<i64, (i64, i64)> = HashMap::new();
    for partial in partials {
        for (group, (mn, mx)) in partial {
            merged
                .entry(group)
                .and_modify(|(gmn, gmx)| {
                    *gmn = (*gmn).min(mn);
                    *gmx = (*gmx).max(mx);
                })
                .or_insert((mn, mx));
        }
    }
    merged
}

// --------------------------- the real registry ------------------------------

/// `[group Int64, value Int64]` — the serve/insert schema (no PK).
fn serve_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("group", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]))
}

/// `[pk Int64, group Int64, value Int64]` — the retraction schema (PK = col 0).
fn retract_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("pk", DataType::Int64, false),
        Field::new("group", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]))
}

fn minmax_spec() -> MaintainedAggregateSpec {
    MaintainedAggregateSpec {
        filter: None,
        group_by: vec!["group".to_string()],
        aggregates: vec![
            MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Min,
                column: Some("value".to_string()),
            },
            MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Max,
                column: Some("value".to_string()),
            },
        ],
    }
}

/// The output schema a `MIN(value)/MAX(value) GROUP BY group` query carries:
/// `[group Int64, min Int64, max Int64]`. `MIN`/`MAX` preserve the input type,
/// so the extrema columns are `Int64` (matching `value`).
fn minmax_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("group", DataType::Int64, false),
        Field::new("min_value", DataType::Int64, true),
        Field::new("max_value", DataType::Int64, true),
    ]))
}

fn serve_batch(columns: &Columns) -> RecordBatch {
    RecordBatch::try_new(
        serve_schema(),
        vec![
            Arc::new(Int64Array::from(columns.group.clone())),
            Arc::new(Int64Array::from(columns.value.clone())),
        ],
    )
    .expect("serve batch should be valid")
}

/// Build a real registry (insert-only, no PK) and fold the whole base table in.
fn build_registry(columns: &Columns) -> MaintainedAggregateRegistry {
    let registry = MaintainedAggregateRegistry::try_new(&[minmax_spec()], &serve_schema())
        .expect("registry construction");
    registry
        .apply_insert_batches(1, &[serve_batch(columns)])
        .expect("apply_insert_batches");
    registry
}

/// Serve `MIN`/`MAX` per group from the real registry (the O(groups) path), as a
/// `group -> (min, max)` map. This is the maintained-serve cost the ratio climbs.
fn serve(registry: &MaintainedAggregateRegistry) -> HashMap<i64, (i64, i64)> {
    let batch = registry
        .batch_for_spec(&minmax_spec(), 1, minmax_output_schema())
        .expect("serve should not error")
        .expect("registry is fresh at epoch 1 and the view matches");
    let groups = batch.column(0).as_primitive::<Int64Type>();
    let mins = batch.column(1).as_primitive::<Int64Type>();
    let maxs = batch.column(2).as_primitive::<Int64Type>();
    let mut out = HashMap::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        // Every group in this workload has non-null values, so both extrema are
        // present; the correctness gate below would catch a null regression.
        out.insert(groups.value(row), (mins.value(row), maxs.value(row)));
    }
    out
}

fn retract_batch(pk_start: i64, count: usize, groups: usize, rng: &mut Rng) -> RecordBatch {
    let group_count = groups.max(1) as i64;
    let pk: Vec<i64> = (0..count as i64).map(|i| pk_start + i).collect();
    let group: Vec<i64> = pk.iter().map(|p| p % group_count).collect();
    let value: Vec<i64> = (0..count).map(|_| (rng.below(2001) as i64) - 1000).collect();
    RecordBatch::try_new(
        retract_schema(),
        vec![
            Arc::new(Int64Array::from(pk)),
            Arc::new(Int64Array::from(group)),
            Arc::new(Int64Array::from(value)),
        ],
    )
    .expect("retract batch should be valid")
}

// ------------------------------ correctness gate ----------------------------

/// The real registry's served `MIN`/`MAX` must equal a full recompute AFTER a
/// delta of inserts, updates, and deletes that repeatedly removes the current
/// group extremum (the retraction-hard case). Runs before timing.
fn assert_minmax_matches_recompute() {
    let mut rng = Rng::new(SEED ^ 0xC0FFEE);
    let base = build_base(20_000, GROUP_COUNT, &mut rng);
    let registry = MaintainedAggregateRegistry::try_new_with_pk(
        &[minmax_spec_with_pk()],
        &retract_schema(),
        &[0],
        usize::MAX,
    )
    .expect("registry construction");

    // Model: pk -> (group, value); seed both the registry and the model.
    let mut model: BTreeMap<i64, (i64, i64)> = BTreeMap::new();
    let seed_pk: Vec<i64> = (0..base.len() as i64).collect();
    let seed = RecordBatch::try_new(
        retract_schema(),
        vec![
            Arc::new(Int64Array::from(seed_pk.clone())),
            Arc::new(Int64Array::from(base.group.clone())),
            Arc::new(Int64Array::from(base.value.clone())),
        ],
    )
    .expect("seed batch");
    registry
        .apply_insert_batches(1, &[seed])
        .expect("seed insert");
    for index in 0..base.len() {
        model.insert(index as i64, (base.group[index], base.value[index]));
    }

    // Delta epoch 2: force-delete the current MIN and MAX pk of several groups
    // (the retraction-hard path), plus some plain updates.
    let mut delete_pks: Vec<i64> = Vec::new();
    for group in 0..50_i64 {
        let members: Vec<(&i64, &(i64, i64))> =
            model.iter().filter(|(_, (g, _))| *g == group).collect();
        if let Some((pk, _)) = members.iter().min_by_key(|(_, (_, v))| *v) {
            delete_pks.push(**pk);
        }
        if let Some((pk, _)) = members.iter().max_by_key(|(_, (_, v))| *v) {
            delete_pks.push(**pk);
        }
    }
    delete_pks.sort_unstable();
    delete_pks.dedup();
    assert!(
        !delete_pks.is_empty(),
        "gate must delete real extremum rows to exercise retraction"
    );
    let del = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("pk", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(delete_pks.clone()))],
    )
    .expect("delete batch");
    registry
        .apply_pk_deletes(2, &del)
        .expect("apply_pk_deletes");
    for pk in &delete_pks {
        model.remove(pk);
    }

    // Recompute ground truth from the model and compare to the real serve.
    let mut truth: HashMap<i64, (i64, i64)> = HashMap::new();
    for (group, value) in model.values() {
        truth
            .entry(*group)
            .and_modify(|(mn, mx)| {
                *mn = (*mn).min(*value);
                *mx = (*mx).max(*value);
            })
            .or_insert((*value, *value));
    }
    let served = serve_with_pk(&registry, 2);
    assert_eq!(
        served.len(),
        truth.len(),
        "served group count diverged from recompute after retraction"
    );
    for (group, (mn, mx)) in &truth {
        let got = served
            .get(group)
            .unwrap_or_else(|| panic!("group {group} missing from maintained serve"));
        assert_eq!(
            got,
            &(*mn, *mx),
            "maintained MIN/MAX diverged from recompute for group {group} after extremum deletion"
        );
    }
}

fn minmax_spec_with_pk() -> MaintainedAggregateSpec {
    // Same MIN/MAX aggregates, resolved against the retract schema (group/value
    // columns still named identically).
    minmax_spec()
}

fn serve_with_pk(registry: &MaintainedAggregateRegistry, epoch: u64) -> HashMap<i64, (i64, i64)> {
    let batch = registry
        .batch_for_spec(&minmax_spec(), epoch, minmax_output_schema())
        .expect("serve should not error")
        .expect("registry fresh and view matches");
    let groups = batch.column(0).as_primitive::<Int64Type>();
    let mins = batch.column(1).as_primitive::<Int64Type>();
    let maxs = batch.column(2).as_primitive::<Int64Type>();
    let mut out = HashMap::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        out.insert(groups.value(row), (mins.value(row), maxs.value(row)));
    }
    out
}

// -------------------------------- benches -----------------------------------

fn bench_recompute_vs_serve(c: &mut Criterion) {
    assert_minmax_matches_recompute();

    let mut group = c.benchmark_group("minmax_recompute_vs_serve");
    for &rows in &[100_000usize, 1_000_000] {
        let mut rng = Rng::new(SEED ^ rows as u64);
        let base = build_base(rows, GROUP_COUNT, &mut rng);
        let registry = build_registry(&base);
        // Sanity: the maintained serve matches the recompute at this scale.
        assert_eq!(
            serve(&registry).len(),
            recompute(&base).len(),
            "serve/recompute group count mismatch"
        );
        let thread_count = std::thread::available_parallelism()
            .map_or(THREAD_COUNT, std::num::NonZeroUsize::get)
            .min(THREAD_COUNT);

        group.bench_with_input(BenchmarkId::new("recompute_1thread", rows), &rows, |b, _| {
            b.iter(|| black_box(recompute(&base).len()));
        });
        group.bench_with_input(
            BenchmarkId::new(format!("recompute_{thread_count}thread"), rows),
            &rows,
            |b, _| b.iter(|| black_box(recompute_parallel(&base, thread_count).len())),
        );
        group.bench_with_input(BenchmarkId::new("serve", rows), &rows, |b, _| {
            b.iter(|| black_box(serve(&registry).len()));
        });
    }
    group.finish();
}

fn bench_maintained_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("minmax_maintained_insert");
    for &rows in &[16_384usize, 131_072] {
        for &cardinality in &[8usize, 1_024] {
            let groups = cardinality.min(rows);
            let mut rng = Rng::new(SEED ^ (rows as u64) ^ (groups as u64));
            let base = build_base(rows, groups, &mut rng);
            group.throughput(Throughput::Elements(rows as u64));
            group.bench_with_input(
                BenchmarkId::from_parameter(format!("rows={rows}/groups={groups}")),
                &rows,
                |b, _| {
                    b.iter_batched(
                        || {
                            let registry = MaintainedAggregateRegistry::try_new(
                                &[minmax_spec()],
                                &serve_schema(),
                            )
                            .expect("registry construction");
                            (registry, serve_batch(&base))
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

fn bench_maintained_retract(c: &mut Criterion) {
    const DELTA: usize = 100;
    let mut group = c.benchmark_group("minmax_maintained_retract");
    for &rows in &[16_384usize, 131_072] {
        let groups = 1_024usize.min(rows);
        group.throughput(Throughput::Elements(DELTA as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("rows={rows}/groups={groups}/delta={DELTA}")),
            &rows,
            |b, &rows| {
                // Seed once outside the timer (the registry holds a RwLock and
                // can't be cloned); time only the repeated O(delta) re-upserts,
                // each removing a resident PK's old value from the ordered
                // multiset and inserting a new one (the retraction-hard cost).
                b.iter_custom(|iters| {
                    let mut rng = Rng::new(SEED ^ 0xBADC0DE ^ rows as u64);
                    let registry = MaintainedAggregateRegistry::try_new_with_pk(
                        &[minmax_spec()],
                        &retract_schema(),
                        &[0],
                        usize::MAX,
                    )
                    .expect("registry construction");
                    registry
                        .apply_insert_batches(1, &[retract_batch(0, rows, groups, &mut rng)])
                        .expect("seed insert");
                    let mut total = std::time::Duration::ZERO;
                    for iteration in 0..iters {
                        let update = retract_batch(0, DELTA, groups, &mut rng);
                        let epoch = 2 + iteration;
                        let start = std::time::Instant::now();
                        registry
                            .apply_insert_batches(epoch, &[update])
                            .expect("retract apply");
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_recompute_vs_serve,
    bench_maintained_insert,
    bench_maintained_retract
);
criterion_main!(benches);
