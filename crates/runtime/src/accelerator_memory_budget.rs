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

//! Cgroup-aware coordinated memory budget for `DuckDB` accelerators and the
//! `DataFusion` query pool.
//!
//! Spice sizes memory in two places that don't know about each other:
//! - the `DataFusion` query memory pool defaults to 90% of host/container RAM
//!   (`DEFAULT_QUERY_MEMORY_PERCENT`, see `datafusion::builder`), and
//! - each *distinct* `DuckDB` accelerator instance defaults to `DuckDB`'s own
//!   ~80% of RAM `memory_limit` when the operator sets no `duckdb_memory_limit`.
//!
//! `DuckDB` instances are keyed by database identity: one per distinct resolved
//! file path, plus a single shared in-memory instance for all memory-mode
//! datasets. A Spicepod with N datasets each on its own `DuckDB` file therefore
//! declares N independent 80%-of-RAM ceilings which, stacked on the 90% query
//! pool, massively over-commit host RAM and risk an OOM kill under load.
//!
//! [`plan`] is a pure function (no I/O — trivially unit-testable) that, from a
//! cgroup-aware total and an already-deduped instance summary, computes a
//! coordinated split so the sum of memory *ceilings* (query pool + every
//! `DuckDB` instance) stays within RAM. It:
//! - reduces the query-pool default (via [`AcceleratorMemoryPlan::query_pool_cap_bytes`],
//!   applied by `datafusion::builder::effective_query_memory_limit` as a `min`-cap), and
//! - caps each un-limited `DuckDB` instance (via [`AcceleratorMemoryPlan::per_instance_cap_bytes`],
//!   published through [`publish_duckdb_budget`] and applied by the `DuckDB` accelerator).
//!
//! An explicit `runtime.query.memory_limit` (honored verbatim) and per-dataset
//! `duckdb_memory_limit` (kept as that instance's ceiling) always override the
//! coordination. There is no config knob — coordination is always on and always
//! warns when it engages.

use std::sync::atomic::{AtomicU64, Ordering};

/// Share (%) of the splittable query region handed to the query pool when it is
/// contested with un-limited `DuckDB` instances; the remainder is split equally
/// across those instances. A neutral 50/50 — tune per workload with explicit
/// `runtime.query.memory_limit` / `duckdb_memory_limit`.
const DUCKDB_QUERY_SPLIT_PERCENT: u64 = 50;

/// The auto query-pool cap is never pushed below `base / N` of the (Cayenne-aware)
/// query default, so a DuckDB-heavy pod can't starve queries.
const DUCKDB_COORD_QUERY_MIN_FRACTION: u64 = 4;

/// Per-instance floor for an auto-capped `DuckDB` instance, so `DuckDB` stays usable
/// even on a tiny host or with many instances (at the cost of a possible residual
/// over-commit, which is surfaced as a stronger warning).
const DUCKDB_MIN_INSTANCE_CAP_BYTES: u64 = 128 * 1024 * 1024;

/// Floor for the auto query-pool cap in the all-explicit case, so it can shrink well
/// below `base / DUCKDB_COORD_QUERY_MIN_FRACTION` to fit fixed explicit ceilings but
/// never reaches 0 — a 0-byte `GreedyMemoryPool` is unusable. When explicit ceilings
/// leave less than this, the residual over-commit is surfaced in the warning.
const DUCKDB_MIN_QUERY_POOL_BYTES: u64 = 256 * 1024 * 1024;

/// `DuckDB`'s own default `memory_limit` as a fraction of the RAM it sees. Applied to
/// HOST RAM (see [`duckdb_default_per_instance_bytes`]) — `DuckDB` sizes its default
/// from host RAM, not the cgroup limit — so the coordinated projection stays accurate
/// even in a container where host RAM exceeds the cgroup total.
const DUCKDB_DEFAULT_MEMORY_PERCENT: u64 = 80;

const MIB: u64 = 1024 * 1024;

/// Process-global per-instance `DuckDB` `memory_limit` (bytes) computed by [`plan`]
/// and published at startup (and on hot-reload). `0` = unset. Read by the `DuckDB`
/// accelerator when a dataset omits `duckdb_memory_limit`. Mirrors
/// `cayenne::set_global_memory_budget`.
static DUCKDB_AUTO_MEMORY_LIMIT_BYTES: AtomicU64 = AtomicU64::new(0);

/// Process-global aggregate `DuckDB` ceiling (bytes) — the sum of all `DuckDB`
/// instance limits after coordination. Read by the Cayenne in-memory CDC tier
/// sampler so a co-resident Cayenne tier can't float back into `DuckDB`'s reserved
/// room. `0` = no `DuckDB` accelerators.
static DUCKDB_TOTAL_RESERVATION_BYTES: AtomicU64 = AtomicU64::new(0);

/// Publishes the coordinated `DuckDB` budget once at startup (and on `apply_app`
/// reload). Passing `(0, 0)` clears it (e.g. a reload that removed all `DuckDB`
/// accelerators).
pub fn publish_duckdb_budget(per_instance_cap_bytes: u64, total_reservation_bytes: u64) {
    DUCKDB_AUTO_MEMORY_LIMIT_BYTES.store(per_instance_cap_bytes, Ordering::Relaxed);
    DUCKDB_TOTAL_RESERVATION_BYTES.store(total_reservation_bytes, Ordering::Relaxed);
}

/// The coordinated per-instance `DuckDB` `memory_limit` as a DuckDB-accepted string
/// (floored whole MiB, e.g. `"1234MiB"`), or `None` when unset. `MiB` parses under
/// both `byte_unit::Byte::parse_str(_, true)` and `DuckDB`'s own `SET memory_limit`.
///
/// Only the `DuckDB` accelerator reads this (`#[cfg(feature = "duckdb")]`), so it —
/// and its formatter — are gated to that feature to stay dead-code-free when the
/// accelerator is not compiled in.
#[cfg(feature = "duckdb")]
#[must_use]
pub fn duckdb_auto_memory_limit_option() -> Option<String> {
    format_duckdb_memory_limit(DUCKDB_AUTO_MEMORY_LIMIT_BYTES.load(Ordering::Relaxed))
}

/// Formats a per-instance cap as a floored whole-MiB `DuckDB` `memory_limit` string
/// (`0` ⇒ `None`). Split out from the global read so it can be unit-tested purely.
#[cfg(feature = "duckdb")]
fn format_duckdb_memory_limit(bytes: u64) -> Option<String> {
    match bytes {
        0 => None,
        bytes => Some(format!("{}MiB", bytes / MIB)),
    }
}

/// The coordinated aggregate `DuckDB` ceiling in bytes (`0` when no `DuckDB`
/// accelerators are configured).
#[must_use]
pub fn duckdb_total_reservation_bytes() -> u64 {
    DUCKDB_TOTAL_RESERVATION_BYTES.load(Ordering::Relaxed)
}

/// A deduped-by-instance summary of the `DuckDB` accelerators in an app. Built by a
/// thin adapter over `app.datasets` and `app.views` — either can carry an
/// `acceleration` block, and every `DuckDB` instance they declare is counted here
/// (see `builder::duckdb_budget_inputs`). The planner takes only these numbers so it
/// stays pure and testable.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DuckDbBudgetInputs {
    /// Distinct `DuckDB` instances with NO explicit `duckdb_memory_limit` on any of
    /// the datasets or views sharing them.
    pub num_unset_instances: u32,
    /// Distinct `DuckDB` instances with an explicit `duckdb_memory_limit`.
    pub num_explicit_instances: u32,
    /// Sum of the explicit ceilings — one per explicit instance (the max value if the
    /// components on the same instance disagree).
    pub sum_explicit_bytes: u64,
    /// Some instance has an inconsistent `duckdb_memory_limit` across the datasets
    /// and views that share it — mixed set/unset, or different explicit values.
    /// `DuckDB`'s `memory_limit` is per-instance (last one created wins), so the
    /// effective limit is ambiguous — surfaced in the warning.
    pub has_mixed_instance: bool,
    /// Human labels (in-memory / file path) of the un-limited instances, for the warning.
    pub unset_instance_labels: Vec<String>,
}

/// Whether [`plan`] changed anything. `NoOp` ⇒ no `DuckDB` accelerators, degenerate
/// host, or the un-coordinated ceilings already fit (no warning, no changes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanOutcome {
    NoOp,
    Applied,
}

/// The coordinated plan. Consumed by `builder.rs` to size the query pool, publish
/// the `DuckDB` cap, and emit the warning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceleratorMemoryPlan {
    pub outcome: PlanOutcome,
    /// Single equal-split cap for every un-limited `DuckDB` instance (`0` ⇒ publish nothing).
    pub per_instance_cap_bytes: u64,
    /// `min`-cap for the query pool (`None` ⇒ leave the builder default untouched,
    /// e.g. an explicit `runtime.query.memory_limit` or a `NoOp`).
    pub query_pool_cap_bytes: Option<u64>,
    /// Aggregate `DuckDB` ceiling (explicit + capped), for the Cayenne tier sampler.
    pub duckdb_reservation_bytes: u64,
    /// The effective query pool after coordination (explicit value, or the auto cap) — for reporting.
    pub effective_query_pool_bytes: u64,
    /// The un-coordinated ceiling (query default + explicit + N×80%) — for the warning.
    pub projected_ceiling_bytes: u64,
    /// The auto caps could not fit within `base_query_budget` (floors bound) — a stronger warning.
    pub residual_overcommit: bool,
}

impl AcceleratorMemoryPlan {
    /// A `NoOp` plan: no query-pool reduction and no per-instance cap. Still carries
    /// `duckdb_reservation_bytes` — the true (un-coordinated) `DuckDB` ceiling — so a
    /// co-resident Cayenne in-memory tier accounts for it even when the config
    /// already fits and we apply nothing.
    fn noop(
        effective_query_pool_bytes: u64,
        projected_ceiling_bytes: u64,
        duckdb_reservation_bytes: u64,
    ) -> Self {
        Self {
            outcome: PlanOutcome::NoOp,
            per_instance_cap_bytes: 0,
            query_pool_cap_bytes: None,
            duckdb_reservation_bytes,
            effective_query_pool_bytes,
            projected_ceiling_bytes,
            residual_overcommit: false,
        }
    }
}

/// `DuckDB`'s own default `memory_limit` for one un-limited instance — `~80%` of the
/// RAM `DuckDB` sees. Computed from HOST RAM (pass `resource_monitor::get_host_memory`),
/// NOT the cgroup total: `DuckDB` sizes its default from host RAM, so in a container the
/// real per-instance ceiling can exceed the cgroup limit. The coordinated budget must
/// project *this* value — using the (smaller) cgroup total would under-estimate the
/// ceiling and wrongly no-op, leaving the container OOM risk this feature prevents.
#[must_use]
pub fn duckdb_default_per_instance_bytes(host_memory: u64) -> u64 {
    host_memory.saturating_mul(DUCKDB_DEFAULT_MEMORY_PERCENT) / 100
}

/// Computes the coordinated memory plan.
///
/// * `total_memory` — cgroup-aware total (from `resource_monitor::get_total_memory`);
///   the ceiling everything must fit within.
/// * `duckdb_default_per_instance` — `DuckDB`'s own default ceiling for one un-limited
///   instance, from HOST RAM (see [`duckdb_default_per_instance_bytes`]) so the
///   projection is correct in containers, not the cgroup total.
/// * `base_query_budget` — what the query pool WOULD be with no `DuckDB` coordination,
///   i.e. `effective_query_memory_limit(None, cayenne_active, cdc, None)` — 90% of
///   RAM (non-Cayenne) or the reduced 70%-based Cayenne region. Splitting *this*
///   (not `total`) confines `DuckDB` to the query region and preserves the Cayenne
///   tier/headroom.
/// * `query_explicit` — parsed `runtime.query.memory_limit` (`Some` ⇒ honored verbatim).
/// * `inputs` — the deduped `DuckDB` instance summary.
#[must_use]
pub fn plan(
    total_memory: u64,
    duckdb_default_per_instance: u64,
    base_query_budget: u64,
    query_explicit: Option<u64>,
    inputs: &DuckDbBudgetInputs,
) -> AcceleratorMemoryPlan {
    let num_duckdb =
        u64::from(inputs.num_unset_instances) + u64::from(inputs.num_explicit_instances);
    let unset = u64::from(inputs.num_unset_instances);
    let sum_explicit = inputs.sum_explicit_bytes;
    let base = base_query_budget;

    // Un-coordinated projection (each un-limited instance grabbing DuckDB's own
    // ~80%-of-HOST-RAM default, on top of the query default / explicit limit and the
    // honored explicit ceilings). This is what the warning quotes and what the
    // NoOp/coordination decision turns on.
    let projected_ceiling_bytes = query_explicit
        .unwrap_or(base)
        .saturating_add(sum_explicit)
        .saturating_add(duckdb_default_per_instance.saturating_mul(unset));

    let effective_query_default = query_explicit.unwrap_or(base);

    // No DuckDB accelerators, or a degenerate host: reserve nothing.
    if num_duckdb == 0 || total_memory == 0 {
        return AcceleratorMemoryPlan::noop(effective_query_default, projected_ceiling_bytes, 0);
    }

    // The naive ceilings already fit within RAM (any un-limited instance forces
    // 90%+80% ≥ 170%, so this branch mostly spares benign all-explicit / small
    // explicit-query pods). Apply no caps and don't reduce the query pool — but
    // STILL publish the true DuckDB ceiling (honored explicit limits plus DuckDB's
    // own ~80% default for any un-limited instance) so a co-resident Cayenne
    // in-memory tier can't float into memory DuckDB will actually use.
    if projected_ceiling_bytes <= total_memory {
        let uncoordinated_reservation =
            sum_explicit.saturating_add(duckdb_default_per_instance.saturating_mul(unset));
        return AcceleratorMemoryPlan::noop(
            effective_query_default,
            projected_ceiling_bytes,
            uncoordinated_reservation,
        );
    }

    let base_free = base.saturating_sub(sum_explicit);
    let query_floor = base / DUCKDB_COORD_QUERY_MIN_FRACTION;

    // Query target: honor an explicit limit; otherwise take a share of the
    // splittable region.
    let (query_target, query_pool_cap) = match query_explicit {
        // Contested with un-limited instances: give the query pool ~half, floored so
        // queries aren't starved — the un-limited instances (whose caps we DO control)
        // absorb the squeeze instead.
        None if unset > 0 => {
            let preferred =
                (base_free.saturating_mul(DUCKDB_QUERY_SPLIT_PERCENT) / 100).max(query_floor);
            // Once every un-limited instance is squeezed to its own floor, the query
            // pool is the only remaining give. `fits` is the largest pool that still
            // leaves each of them that floor within `base`. When honoring the query
            // floor would push past `base` but `fits` would not, shrink below the
            // floor to land inside the budget — the same trade the all-explicit
            // branch makes. Only when `fits` is unusably small (or shrinking wouldn't
            // avoid the over-commit anyway) is the preferred share kept, so a
            // hopeless pod doesn't get a starved query pool AND an over-commit.
            let fits = base
                .saturating_sub(sum_explicit)
                .saturating_sub(DUCKDB_MIN_INSTANCE_CAP_BYTES.saturating_mul(unset));
            let target = if fits < preferred && fits >= DUCKDB_MIN_QUERY_POOL_BYTES {
                fits
            } else {
                preferred
            }
            .min(base);
            (target, Some(target))
        }
        // All instances explicit: their ceilings are FIXED, so the query pool takes
        // what they leave (base − Σexplicit) — shrinking well below the query floor
        // rather than being forced up to it and thereby DELIBERATELY over-committing
        // — but never below `DUCKDB_MIN_QUERY_POOL_BYTES`, because a 0-byte query pool
        // is unusable. When Σexplicit is large enough that this floor bumps the query
        // pool above what fits (Σexplicit ≥ base), `residual_overcommit` below flags
        // it and the n==0 warning reports the (small) reduced query pool.
        None => {
            let target = base_free.max(DUCKDB_MIN_QUERY_POOL_BYTES).min(base);
            (target, Some(target))
        }
        Some(q) => (q, None),
    };

    // Per-instance cap: equal split of whatever the query target and honored
    // explicit ceilings leave, floored so DuckDB stays usable.
    let duckdb_pool_total = base
        .saturating_sub(query_target)
        .saturating_sub(sum_explicit);
    let per_instance_cap_bytes = duckdb_pool_total
        .checked_div(unset)
        .map_or(0, |per_instance| {
            // Floor to whole MiB so the published cap, the aggregate reservation, and
            // the warning all equal what DuckDB is actually set to — `SET memory_limit`
            // is emitted as floored MiB (see `format_duckdb_memory_limit`). The 128 MiB
            // floor is already MiB-aligned, so `max` before flooring is exact.
            (per_instance.max(DUCKDB_MIN_INSTANCE_CAP_BYTES) / MIB) * MIB
        });

    let duckdb_reservation_bytes =
        sum_explicit.saturating_add(per_instance_cap_bytes.saturating_mul(unset));

    // Residual: the floors (query floor / per-instance floor) forced the applied
    // ceilings past the splittable region — still over-committed. Surfaced louder.
    let applied_sum = query_target
        .saturating_add(sum_explicit)
        .saturating_add(per_instance_cap_bytes.saturating_mul(unset));
    let residual_overcommit = applied_sum > base;

    AcceleratorMemoryPlan {
        outcome: PlanOutcome::Applied,
        per_instance_cap_bytes,
        query_pool_cap_bytes: query_pool_cap,
        duckdb_reservation_bytes,
        effective_query_pool_bytes: query_target,
        projected_ceiling_bytes,
        residual_overcommit,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AcceleratorMemoryPlan, DUCKDB_MIN_INSTANCE_CAP_BYTES, DuckDbBudgetInputs, PlanOutcome, plan,
    };

    const GIB: u64 = 1024 * 1024 * 1024;
    const MIB: u64 = 1024 * 1024;

    /// Bare-metal-equivalent `plan()` where host RAM == cgroup total, so `DuckDB`'s
    /// per-instance default is `total * 80 / 100`.
    fn plan_bare(
        total: u64,
        base: u64,
        query: Option<u64>,
        inputs: &DuckDbBudgetInputs,
    ) -> AcceleratorMemoryPlan {
        plan(total, total.saturating_mul(80) / 100, base, query, inputs)
    }

    /// Whole-MiB floor — per-instance caps are floored so they match `DuckDB`'s
    /// applied `SET memory_limit` (see `format_duckdb_memory_limit`).
    fn floor_mib(bytes: u64) -> u64 {
        (bytes / MIB) * MIB
    }

    /// 32 GiB host, 90% non-Cayenne query default.
    fn total_and_base() -> (u64, u64) {
        let total = 32 * GIB;
        let base = total * 90 / 100;
        (total, base)
    }

    fn unset(n: u32) -> DuckDbBudgetInputs {
        DuckDbBudgetInputs {
            num_unset_instances: n,
            unset_instance_labels: (0..n).map(|i| format!("db{i}")).collect(),
            ..Default::default()
        }
    }

    #[test]
    fn no_duckdb_is_noop() {
        let (total, base) = total_and_base();
        let p = plan_bare(total, base, None, &DuckDbBudgetInputs::default());
        assert_eq!(p.outcome, PlanOutcome::NoOp);
        assert_eq!(p.per_instance_cap_bytes, 0);
        assert_eq!(p.query_pool_cap_bytes, None);
        assert_eq!(p.duckdb_reservation_bytes, 0);
    }

    #[test]
    fn degenerate_host_is_noop() {
        let p = plan_bare(0, 0, None, &unset(3));
        assert_eq!(p.outcome, PlanOutcome::NoOp);
        assert_eq!(p.per_instance_cap_bytes, 0);
    }

    /// Container case: `DuckDB` sizes its default `memory_limit` from HOST RAM, which
    /// exceeds the cgroup limit. A small explicit query pool + one un-limited instance
    /// projects to *fit* under a cgroup-based default (would wrongly `NoOp`) but
    /// over-commits under the real host-based default — so coordination must engage.
    #[test]
    fn container_host_ram_default_engages_coordination() {
        let cgroup_total = 16 * GIB;
        let host_memory = 64 * GIB; // host >> cgroup (containerized)
        let host_default = super::duckdb_default_per_instance_bytes(host_memory); // 51.2 GiB
        let base = cgroup_total * 90 / 100;
        let q = 2 * GIB; // small explicit query pool

        let inputs = DuckDbBudgetInputs {
            num_unset_instances: 1,
            ..Default::default()
        };

        // Host-based (correct): projected = 2 + 51.2 = 53.2 GiB > 16 GiB cgroup → Applied.
        let p = plan(cgroup_total, host_default, base, Some(q), &inputs);
        assert_eq!(
            p.outcome,
            PlanOutcome::Applied,
            "the host-RAM default must engage coordination in a container"
        );
        assert!(p.per_instance_cap_bytes > 0);

        // Counterfactual: a cgroup-based default (16 GiB * 80% = 12.8 GiB) projects to
        // 2 + 12.8 = 14.8 GiB ≤ 16 GiB and would wrongly NoOp — the bug host RAM fixes.
        let cgroup_default = cgroup_total * 80 / 100;
        let wrong = plan(cgroup_total, cgroup_default, base, Some(q), &inputs);
        assert_eq!(
            wrong.outcome,
            PlanOutcome::NoOp,
            "a cgroup-based default under-estimates and skips coordination"
        );
    }

    /// A single un-limited instance (file or memory): query and `DuckDB` each get
    /// half of the splittable region; nothing exceeds `base`.
    #[test]
    fn single_unset_instance_splits_in_half() {
        let (total, base) = total_and_base();
        let p = plan_bare(total, base, None, &unset(1));
        assert_eq!(p.outcome, PlanOutcome::Applied);
        assert_eq!(p.effective_query_pool_bytes, base / 2);
        assert_eq!(p.per_instance_cap_bytes, floor_mib(base - base / 2));
        assert_eq!(p.query_pool_cap_bytes, Some(base / 2));
        assert!(!p.residual_overcommit);
        // The whole point: query + DuckDB ≤ base ≤ 90% of host, leaving ≥10% headroom.
        assert!(p.effective_query_pool_bytes + p.duckdb_reservation_bytes <= base);
        assert!(p.effective_query_pool_bytes + p.duckdb_reservation_bytes <= total);
    }

    /// N un-limited file instances split `DuckDB`'s half equally.
    #[test]
    fn n_unset_instances_split_equally() {
        let (total, base) = total_and_base();
        for n in [2_u32, 3, 5, 8] {
            let p = plan_bare(total, base, None, &unset(n));
            assert_eq!(p.outcome, PlanOutcome::Applied);
            let duckdb_half = base - p.effective_query_pool_bytes;
            assert_eq!(
                p.per_instance_cap_bytes,
                floor_mib(duckdb_half / u64::from(n))
            );
            assert!(
                p.effective_query_pool_bytes + p.duckdb_reservation_bytes <= base,
                "n={n} overcommits base"
            );
        }
    }

    /// Every published per-instance cap is whole-MiB aligned, so the warning and
    /// the aggregate reservation exactly match `DuckDB`'s floored `SET memory_limit`.
    #[test]
    fn per_instance_cap_is_mib_aligned() {
        let (total, base) = total_and_base();
        for n in 1..=8_u32 {
            let p = plan_bare(total, base, None, &unset(n));
            assert_eq!(
                p.per_instance_cap_bytes % MIB,
                0,
                "n={n}: cap {} not MiB-aligned",
                p.per_instance_cap_bytes
            );
            assert_eq!(
                p.duckdb_reservation_bytes % MIB,
                0,
                "n={n}: reservation not MiB-aligned"
            );
        }
    }

    /// One explicit + one un-limited instance: explicit honored, the unset one gets
    /// the remainder, query keeps its half.
    #[test]
    fn one_explicit_one_unset() {
        let (total, base) = total_and_base();
        let inputs = DuckDbBudgetInputs {
            num_unset_instances: 1,
            num_explicit_instances: 1,
            sum_explicit_bytes: 2 * GIB,
            unset_instance_labels: vec!["db-unset".to_string()],
            ..Default::default()
        };
        let p = plan_bare(total, base, None, &inputs);
        assert_eq!(p.outcome, PlanOutcome::Applied);
        // reservation = explicit + one capped instance
        assert_eq!(
            p.duckdb_reservation_bytes,
            2 * GIB + p.per_instance_cap_bytes
        );
        assert!(p.effective_query_pool_bytes + p.duckdb_reservation_bytes <= base);
    }

    /// All-explicit, query unset but small explicit ceilings ⇒ already fits ⇒ `NoOp`.
    /// The true `DuckDB` ceiling is still reserved so a co-resident Cayenne tier
    /// accounts for it even though nothing is capped.
    #[test]
    fn all_explicit_small_fits_is_noop() {
        let (total, base) = total_and_base();
        let inputs = DuckDbBudgetInputs {
            num_explicit_instances: 2,
            sum_explicit_bytes: GIB, // 1 GiB total, well under the 10% headroom
            ..Default::default()
        };
        let p = plan_bare(total, base, None, &inputs);
        assert_eq!(p.outcome, PlanOutcome::NoOp);
        assert_eq!(p.per_instance_cap_bytes, 0);
        assert_eq!(p.query_pool_cap_bytes, None);
        assert_eq!(p.duckdb_reservation_bytes, GIB); // reserved despite the NoOp
    }

    /// A `NoOp` with an un-limited instance that fits (tiny explicit query pool) still
    /// reserves `DuckDB`'s own ~80% default for that instance — otherwise a co-resident
    /// Cayenne tier could float into the memory `DuckDB` will use.
    #[test]
    fn noop_reserves_duckdb_default_for_unset_instance() {
        let (total, base) = total_and_base();
        let q = total / 10; // 10% explicit query pool; 10% + 80% = 90% ≤ total ⇒ NoOp
        let inputs = DuckDbBudgetInputs {
            num_unset_instances: 1,
            ..Default::default()
        };
        let p = plan_bare(total, base, Some(q), &inputs);
        assert_eq!(p.outcome, PlanOutcome::NoOp);
        assert_eq!(p.per_instance_cap_bytes, 0); // nothing capped
        assert_eq!(p.duckdb_reservation_bytes, total * 80 / 100);
    }

    /// All-explicit, query unset, explicit ceilings large enough that 90%+explicit
    /// over-commits ⇒ query pool is reduced to fit; no per-instance injection.
    #[test]
    fn all_explicit_large_reduces_query() {
        let (total, base) = total_and_base();
        let inputs = DuckDbBudgetInputs {
            num_explicit_instances: 1,
            sum_explicit_bytes: 20 * GIB, // 90% (28.8) + 20 = 48.8 > 32 → over-commit
            ..Default::default()
        };
        let p = plan_bare(total, base, None, &inputs);
        assert_eq!(p.outcome, PlanOutcome::Applied);
        assert_eq!(p.per_instance_cap_bytes, 0); // nothing to inject
        assert_eq!(p.effective_query_pool_bytes, base - 20 * GIB);
        assert_eq!(p.query_pool_cap_bytes, Some(base - 20 * GIB));
    }

    /// All instances explicit with ceilings summing to just under `base`: the query
    /// pool shrinks to exactly what they leave (BELOW the query floor) and the plan
    /// FITS, rather than being forced to the floor and deliberately over-committing.
    #[test]
    fn all_explicit_near_base_shrinks_query_below_floor_without_overcommit() {
        let (total, base) = total_and_base();
        let sum_explicit = base - GIB; // base_free = 1 GiB, well below the base/4 floor
        let inputs = DuckDbBudgetInputs {
            num_explicit_instances: 3,
            sum_explicit_bytes: sum_explicit,
            ..Default::default()
        };
        let p = plan_bare(total, base, None, &inputs);
        assert_eq!(p.outcome, PlanOutcome::Applied);
        assert_eq!(p.per_instance_cap_bytes, 0); // nothing to inject (all explicit)
        assert_eq!(p.effective_query_pool_bytes, GIB); // exactly base − Σexplicit
        assert!(
            !p.residual_overcommit,
            "fits by shrinking the query pool below the floor"
        );
        assert!(p.effective_query_pool_bytes + p.duckdb_reservation_bytes <= base);
    }

    /// All-explicit where the explicit ceilings meet/exceed the query region: the
    /// query pool is floored at a usable minimum (never 0 — a 0-byte pool is
    /// unusable) and the residual over-commit is flagged.
    #[test]
    fn all_explicit_exceeding_base_floors_query_pool_not_zero() {
        let (total, base) = total_and_base();
        let inputs = DuckDbBudgetInputs {
            num_explicit_instances: 1,
            sum_explicit_bytes: base, // base_free saturates to 0
            ..Default::default()
        };
        let p = plan_bare(total, base, None, &inputs);
        assert_eq!(p.outcome, PlanOutcome::Applied);
        assert_eq!(p.per_instance_cap_bytes, 0);
        assert_eq!(
            p.effective_query_pool_bytes,
            super::DUCKDB_MIN_QUERY_POOL_BYTES
        );
        assert_eq!(
            p.query_pool_cap_bytes,
            Some(super::DUCKDB_MIN_QUERY_POOL_BYTES)
        );
        assert!(
            p.effective_query_pool_bytes > 0,
            "query pool must never be capped to 0"
        );
        assert!(
            p.residual_overcommit,
            "explicit ceilings alone meet/exceed the query region"
        );
    }

    /// An explicit query limit is honored verbatim (never auto-reduced); the unset
    /// instances share whatever it leaves within `base`.
    #[test]
    fn explicit_query_is_honored() {
        let (total, base) = total_and_base();
        let q = 10 * GIB;
        let p = plan_bare(total, base, Some(q), &unset(2));
        assert_eq!(p.outcome, PlanOutcome::Applied);
        assert_eq!(p.effective_query_pool_bytes, q);
        assert_eq!(p.query_pool_cap_bytes, None); // never override an explicit limit
        assert_eq!(p.per_instance_cap_bytes, floor_mib((base - q) / 2));
    }

    /// An explicit query limit that itself over-commits: honored, `DuckDB` floored,
    /// residual over-commit flagged.
    #[test]
    fn explicit_query_overcommits_flags_residual() {
        let (total, base) = total_and_base();
        let q = base; // query takes the whole splittable region
        let p = plan_bare(total, base, Some(q), &unset(2));
        assert_eq!(p.outcome, PlanOutcome::Applied);
        assert_eq!(p.effective_query_pool_bytes, q);
        assert_eq!(p.query_pool_cap_bytes, None);
        assert_eq!(p.per_instance_cap_bytes, DUCKDB_MIN_INSTANCE_CAP_BYTES); // floored
        assert!(p.residual_overcommit);
    }

    /// Large explicit ceilings alongside ONE un-limited instance: honoring the base/4
    /// query floor would over-commit, but a split that fits exists (query pool below
    /// the floor + the un-limited instance at its own floor). Take it instead of
    /// deliberately over-committing.
    #[test]
    fn explicit_plus_unset_shrinks_query_below_floor_to_fit() {
        let (total, base) = total_and_base();
        // Leaves exactly the per-instance floor once the query pool shrinks to fit,
        // while base/4 (the query floor) is far larger than what actually remains.
        let sum_explicit = base - 2 * GIB;
        let inputs = DuckDbBudgetInputs {
            num_unset_instances: 1,
            num_explicit_instances: 1,
            sum_explicit_bytes: sum_explicit,
            unset_instance_labels: vec!["db-unset".to_string()],
            ..Default::default()
        };
        let p = plan_bare(total, base, None, &inputs);
        assert_eq!(p.outcome, PlanOutcome::Applied);
        assert!(
            p.effective_query_pool_bytes < base / 4,
            "query pool should shrink below the floor to fit"
        );
        assert_eq!(p.per_instance_cap_bytes, DUCKDB_MIN_INSTANCE_CAP_BYTES);
        assert!(
            !p.residual_overcommit,
            "a fitting split exists, so nothing should be over-committed"
        );
        assert!(p.effective_query_pool_bytes + p.duckdb_reservation_bytes <= base);
    }

    /// Many un-limited instances on a small host ⇒ the per-instance floor binds and
    /// the residual over-commit is surfaced.
    #[test]
    fn tiny_host_many_instances_floors_and_flags() {
        let total = 2 * GIB;
        let base = total * 90 / 100;
        let p = plan_bare(total, base, None, &unset(64));
        assert_eq!(p.outcome, PlanOutcome::Applied);
        assert_eq!(p.per_instance_cap_bytes, DUCKDB_MIN_INSTANCE_CAP_BYTES);
        assert!(p.residual_overcommit);
    }

    /// Cayenne-active base (70% region) is split without ever touching the tier/headroom:
    /// query + `DuckDB` ceilings stay within the 70% base.
    #[test]
    fn cayenne_base_splits_within_region() {
        let total = 64 * GIB;
        let base = total * 70 / 100; // Cayenne query region
        let p = plan_bare(total, base, None, &unset(2));
        assert_eq!(p.outcome, PlanOutcome::Applied);
        assert!(p.effective_query_pool_bytes + p.duckdb_reservation_bytes <= base);
        // ...and therefore leaves the Cayenne tier (host/5) + headroom (host/10) intact.
        assert!(p.effective_query_pool_bytes + p.duckdb_reservation_bytes <= total * 70 / 100);
    }

    /// Property: for every non-residual `Applied` plan, query + all `DuckDB` ceilings
    /// never exceed the splittable base (hence ≤ total with headroom preserved).
    #[test]
    fn applied_without_residual_never_overcommits_base() {
        let (total, base) = total_and_base();
        for n in 1..=16_u32 {
            let p: AcceleratorMemoryPlan = plan_bare(total, base, None, &unset(n));
            if p.outcome == PlanOutcome::Applied && !p.residual_overcommit {
                assert!(
                    p.effective_query_pool_bytes + p.duckdb_reservation_bytes <= base,
                    "n={n}: {} + {} > base {base}",
                    p.effective_query_pool_bytes,
                    p.duckdb_reservation_bytes
                );
            }
        }
    }

    /// The published cap formats as a floored whole-MiB `DuckDB` `memory_limit`
    /// string (and `0` clears it). Tested on the pure formatter, not the global.
    #[cfg(feature = "duckdb")]
    #[test]
    fn cap_formats_as_floored_mib() {
        assert_eq!(super::format_duckdb_memory_limit(0), None);
        assert_eq!(
            super::format_duckdb_memory_limit(1234 * 1024 * 1024).as_deref(),
            Some("1234MiB")
        );
        // Floors sub-MiB remainders down to a whole MiB.
        assert_eq!(
            super::format_duckdb_memory_limit(1234 * 1024 * 1024 + 1023 * 1024).as_deref(),
            Some("1234MiB")
        );
    }
}
