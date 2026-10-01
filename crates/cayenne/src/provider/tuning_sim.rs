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

//! Closed-loop simulation harness for the adaptive goal-driven controller in
//! [`super::tuning`]. Test-only.
//!
//! The harness drives the **real** accounting ([`IngestStats`]), the **real**
//! decision ([`decide_with_goals`]) and the **real** actuator write
//! ([`LiveActuators::apply`]) — plus a faithful copy of `CayenneContext::retune`'s
//! dwell / fresh-sample / infeasibility bookkeeping — against a small synthetic
//! plant, in simulated time. The tick period is the live compaction interval,
//! exactly as the background compactor paces `on_background_tick`
//! (`compaction.rs`, the `tokio::time::sleep(current)` loop).
//!
//! The plant is deliberately simple and only claims to be *monotone in the
//! directions the controller's own rule comments assume* (more shards / bigger
//! memtable ⇒ faster apply; more small files ⇒ slower queries; more shards ⇒
//! more CPU and more files; a reserve sheds queries; …). Nothing measured here
//! is a performance claim about Cayenne. What it measures is whether the
//! controller's *logic* honors the invariants below on every schedule:
//!
//! - **I1** every applied value stays inside its static `[floor, ceiling]` and
//!   the decision never panics;
//! - **I2** the memory rule dominates (under high pressure no memory-consuming
//!   actuator is raised, and a shrink lowers the value);
//! - **I3** direction coherence (the sign of a move matches its `reason`);
//! - **I4** no thrash on a stationary plant (bounded direction reversals), and
//!   no single move is a "big jump" (> 50 % of the current value);
//! - **I5** liveness: a feasible goal is met by the end of a long phase;
//! - **I6** signal fidelity: a goal the plant meets is never reported violated
//!   by estimator quantization, and an unavailable metric never freezes the
//!   controller in its aggressive state;
//! - **I7** the Bao floor: the closed loop is never worse than the untouched
//!   warm-start config by more than a small tolerance, on any phase.
//!
//! Every scenario prints one `SIM …` line (and `SIM_VIOLATION …` lines) so an
//! external runner can parse the eval; the tests then assert on the totals.
//! Everything is deterministic (seeded RNG, simulated clock, no I/O).

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    clippy::cast_possible_wrap,
    clippy::float_cmp,
    clippy::too_many_lines,
    clippy::struct_excessive_bools,
    clippy::similar_names,
    clippy::many_single_char_names,
    clippy::unreadable_literal,
    clippy::items_after_statements,
    clippy::print_stdout,
    clippy::expect_used,
    clippy::needless_pass_by_value
)]

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::Duration;

use super::tuning::{
    Actuator, ActuatorValues, Adjustment, GOAL_INFEASIBLE_STUCK_TICKS, Goals, IngestSnapshot,
    IngestStats, LiveActuators, MIN_DWELL, QueryObservations, TuningBounds, WARMUP_BATCHES,
    WriteSample, adaptive_inline_flush_bounds, adaptive_mem_tier_bounds,
    adaptive_target_file_size_bounds, decide_with_goals,
};
use crate::metadata::StorageClass;

const MIB: i64 = 1024 * 1024;
const MS_PER_HOUR: i64 = 3_600_000;
const WINDOW_MS: i64 = 60_000;

/// The controller's memory thresholds, restated here (the constants are private
/// to `tuning.rs`). The harness is frozen, so a drift would show as a spurious
/// I2 finding rather than a silently weakened gate.
const MEM_PRESSURE_HIGH: f64 = 0.85;

/// I4: max direction reversals per actuator per simulated hour on a stationary
/// phase, after the settle horizon.
const MAX_REVERSALS_PER_HOUR: u32 = 2;
/// I4b: a single SHRINK may not remove more than this fraction of an actuator's
/// current value (the direction of the L-20 incident, 1 GiB → 67 MiB; the legacy
/// ×2/3 step is the controller's own "no big jumps" bound). Growth is reported
/// as a metric (`grow_step`) but not asserted: every raise is memory-gated and
/// clamped by a ceiling the static tier already deemed safe.
const MAX_RELATIVE_SHRINK: f64 = 0.5;
/// I7: AUTO cost may exceed STATIC cost by this factor plus this absolute slack.
const BAO_TOLERANCE_FACTOR: f64 = 1.02;
const BAO_TOLERANCE_ABS: f64 = 0.01;
/// Settle horizon (ms) after a phase starts before I4 reversals are counted.
const SETTLE_MS: i64 = 2 * WINDOW_MS;

// ---------------------------------------------------------------------------
// Deterministic RNG
// ---------------------------------------------------------------------------

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self((seed.wrapping_mul(0x9E37_79B9_7F4A_7C15)).max(1))
    }

    fn next_u64(&mut self) -> u64 {
        // xorshift64*
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    fn range_f64(&mut self, lo: f64, hi: f64) -> f64 {
        lo + (hi - lo) * self.next_f64()
    }

    fn range_u64(&mut self, lo: u64, hi: u64) -> u64 {
        if hi <= lo {
            return lo;
        }
        lo + self.next_u64() % (hi - lo + 1)
    }

    fn chance(&mut self, p: f64) -> bool {
        self.next_f64() < p
    }
}

// ---------------------------------------------------------------------------
// Plant + workload
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
struct Plant {
    cores: usize,
    /// Rows/s a single write shard applies with an unbounded memtable.
    mu1_rows_per_s: f64,
    /// Diminishing-returns exponent on write concurrency.
    conc_exp: f64,
    /// Fixed metastore publish cost per memtable flush (ms), amortized over the
    /// memtable size.
    commit_ms: f64,
    /// Synchronous mem-tier spill stall (ms) per `64 × batch_bytes / mem_tier_cap`.
    spill_ms: f64,
    base_lag_s: f64,
    p99_base_ms: f64,
    p99_per_small_file_ms: f64,
    cpu_base: f64,
    cpu_per_qps: f64,
    cpu_ingest_per_shard: f64,
    cpu_compaction: f64,
    mem_budget_bytes: i64,
    mem_fixed_bytes: i64,
    shard_bytes: i64,
    data_storage: StorageClass,
    metastore_storage: StorageClass,
}

impl Plant {
    fn default_ssd() -> Self {
        Self {
            cores: 8,
            mu1_rows_per_s: 20_000.0,
            conc_exp: 0.7,
            commit_ms: 150.0,
            spill_ms: 20.0,
            base_lag_s: 0.3,
            p99_base_ms: 40.0,
            p99_per_small_file_ms: 4.0,
            cpu_base: 0.15,
            cpu_per_qps: 0.02,
            cpu_ingest_per_shard: 0.03,
            cpu_compaction: 0.05,
            mem_budget_bytes: 4096 * MIB,
            mem_fixed_bytes: 1024 * MIB,
            shard_bytes: 8 * MIB,
            data_storage: StorageClass::LocalSsd,
            metastore_storage: StorageClass::LocalSsd,
        }
    }

    fn service_rows_per_s(&self, write_concurrency: usize) -> f64 {
        self.mu1_rows_per_s * (write_concurrency.max(1) as f64).powf(self.conc_exp)
    }
}

#[derive(Clone, Copy, Debug)]
struct Phase {
    name: &'static str,
    duration_ms: i64,
    rows_per_s: f64,
    row_bytes: u64,
    batches_per_s: f64,
    delete_fraction: f64,
    /// Arrival jitter as a coefficient of variation (0 = metronome).
    arrival_cv: f64,
    queries_per_s: f64,
    /// Workload-side floor added to the plant's p99 (ms).
    query_ms_offset: f64,
    /// I4 reversals are asserted on this phase.
    stationary: bool,
    /// I5: a static config known to meet every active goal on this phase (the
    /// harness verifies it, so a misconfigured scenario fails loudly).
    feasible_with: Option<ActuatorValues>,
    /// I6b: by the end of this phase the controller must have handed back what
    /// the tighten tiers took (write concurrency ≤ warm start, compaction
    /// interval ≥ warm start).
    expect_relax: bool,
}

impl Phase {
    fn steady(name: &'static str, duration_s: i64, rows_per_s: f64) -> Self {
        Self {
            name,
            duration_ms: duration_s * 1000,
            rows_per_s,
            row_bytes: 200,
            batches_per_s: 20.0,
            delete_fraction: 0.0,
            arrival_cv: 0.0,
            queries_per_s: 0.0,
            query_ms_offset: 0.0,
            stationary: true,
            feasible_with: None,
            expect_relax: false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Set {
    Train,
    HeldOut,
}

impl Set {
    fn as_str(self) -> &'static str {
        match self {
            Self::Train => "train",
            Self::HeldOut => "heldout",
        }
    }
}

struct Scenario {
    name: &'static str,
    set: Set,
    seed: u64,
    plant: Plant,
    goals: Goals,
    init: ActuatorValues,
    bounds: TuningBounds,
    phases: Vec<Phase>,
    /// I6a: the plant meets every goal throughout, so any goal-attributed move
    /// or an infeasibility verdict is a false violation.
    plant_meets_goals: bool,
}

#[derive(Clone, Copy, Debug)]
enum Mode {
    /// The closed loop runs.
    Auto,
    /// Actuators frozen at the given values (STATIC-DEFAULT when = `init`, or an
    /// oracle grid point).
    Fixed(ActuatorValues),
}

// ---------------------------------------------------------------------------
// Warm-start values and bounds (mirrors `CayenneContext::new`)
// ---------------------------------------------------------------------------

fn warm_start() -> ActuatorValues {
    ActuatorValues {
        inline_flush_max_bytes: 8 * MIB,
        inline_flush_max_rows: 8192,
        inline_flush_max_segments: 64,
        compaction_background_interval_ms: 10_000,
        compaction_trigger_files: 4,
        bake_deletion_index_trigger: 100_000,
        write_concurrency: 4,
        mem_tier_max_bytes: 256 * MIB,
        target_vortex_file_size_bytes: 256 * MIB,
        query_admission_reserve: 0,
    }
}

fn bounds_for(cores: usize) -> TuningBounds {
    TuningBounds {
        inline_flush_max_bytes: (2 * MIB, 128 * MIB),
        compaction_background_interval_ms: (2_000, 60_000),
        compaction_trigger_files: (2, 32),
        bake_deletion_index_trigger: (1_000, 5_000_000),
        write_concurrency: (1, cores),
        mem_tier_max_bytes: (64 * MIB, 2048 * MIB),
        target_vortex_file_size_bytes: (128 * MIB, 1024 * MIB),
        query_admission_reserve: (0, cores),
    }
}

fn lag_goal(secs: f64) -> Goals {
    Goals::from_targets(Some(secs), None, None, None, Duration::from_mins(1))
}

fn freshness_goal(secs: f64) -> Goals {
    Goals::from_targets(None, Some(secs), None, None, Duration::from_mins(1))
}

fn latency_goal(ms: f64) -> Goals {
    Goals::from_targets(None, None, Some(ms), None, Duration::from_mins(1))
}

fn lag_and_latency_goal(lag_secs: f64, ms: f64) -> Goals {
    Goals::from_targets(Some(lag_secs), None, Some(ms), None, Duration::from_mins(1))
}

fn lag_and_qph_goal(lag_secs: f64, qph: f64) -> Goals {
    Goals::from_targets(
        Some(lag_secs),
        None,
        None,
        Some(qph),
        Duration::from_mins(1),
    )
}

// ---------------------------------------------------------------------------
// The simulation
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct MoveRec {
    tick: u64,
    now_ms: i64,
    phase: usize,
    actuator: Actuator,
    old: u64,
    new: u64,
    reason: &'static str,
}

#[derive(Clone, Debug, Default)]
struct PhaseMetrics {
    name: &'static str,
    ticks: u64,
    moves: u64,
    /// Time-integral of the true goal violation (cost), per tick mean.
    cost: f64,
    /// Fraction of ticks on which at least one active goal was violated (true
    /// plant values).
    violated_frac: f64,
    /// The same fraction over the last quarter of the phase — "still violated at
    /// the end", the I5 liveness verdict.
    violated_last_quarter: f64,
    /// First simulated ms (relative to phase start) at which every active goal
    /// was met on the true plant values, if ever.
    time_to_meet_ms: Option<i64>,
    /// Direction reversals per actuator after the settle horizon.
    reversals: HashMap<&'static str, u32>,
    /// Largest single-move shrink as a fraction of the value before the move.
    max_relative_shrink: f64,
    /// Largest single-move growth as a fraction of the value before the move.
    max_relative_grow: f64,
    /// First simulated ms (relative to phase start) at which every active goal
    /// was met again AFTER the first violated tick of the phase — the recovery
    /// time; `None` if never violated or never recovered.
    time_to_recover_ms: Option<i64>,
    mean_lag_s: f64,
    max_lag_s: f64,
    mean_p99_ms: f64,
    infeasible_fired: bool,
    final_values: Option<ActuatorValues>,
}

#[derive(Clone, Debug, Default)]
struct RunResult {
    phases: Vec<PhaseMetrics>,
    moves: Vec<MoveRec>,
    violations: Vec<String>,
    trace_hash: u64,
    ticks: u64,
}

struct Sim<'a> {
    sc: &'a Scenario,
    mode: Mode,
    rng: Rng,
    stats: IngestStats,
    obs: QueryObservations,
    live: LiveActuators,
    now_ms: i64,
    tick: u64,
    backlog_ms: f64,
    small_files: f64,
    batch_credit: f64,
    query_counter: u64,
    // Faithful copy of `CayenneContext::retune` bookkeeping.
    last_adjust_ms: Option<i64>,
    last_adjust_samples: u64,
    goal_stuck_ticks: u64,
    infeasible_fired: bool,
    // Records.
    moves: Vec<MoveRec>,
    violations: Vec<String>,
    hasher: DefaultHasher,
}

struct TickObs {
    lag_true_s: f64,
    fresh_true_s: f64,
    p99_true_ms: f64,
    qph_true: Option<f64>,
    cpu: f64,
    mem: f64,
    read_amp: usize,
}

impl<'a> Sim<'a> {
    fn new(sc: &'a Scenario, mode: Mode) -> Self {
        let init = match mode {
            Mode::Auto => sc.init,
            Mode::Fixed(v) => v,
        };
        Self {
            sc,
            mode,
            rng: Rng::new(sc.seed),
            stats: IngestStats::new(),
            obs: QueryObservations::new(),
            live: LiveActuators::new(init),
            now_ms: 1_700_000_000_000, // an arbitrary epoch anchor
            tick: 0,
            backlog_ms: 0.0,
            small_files: 0.0,
            batch_credit: 0.0,
            query_counter: 0,
            last_adjust_ms: None,
            last_adjust_samples: 0,
            goal_stuck_ticks: 0,
            infeasible_fired: false,
            moves: Vec::new(),
            violations: Vec::new(),
            hasher: DefaultHasher::new(),
        }
    }

    /// One batch of CDC apply on the plant, recorded through the real accounting.
    fn apply_batch(&mut self, phase: &Phase, cur: &ActuatorValues, cpu_prev: f64) -> f64 {
        let p = &self.sc.plant;
        let gap_mean_ms = 1000.0 / phase.batches_per_s.max(0.001);
        let jitter = if phase.arrival_cv > 0.0 {
            // A two-point distribution with the requested CV keeps the mean gap.
            if self.rng.chance(0.5) {
                1.0 + phase.arrival_cv
            } else {
                (1.0 - phase.arrival_cv).max(0.05)
            }
        } else {
            1.0
        };
        let gap_ms = gap_mean_ms * jitter;
        let rows = (phase.rows_per_s * gap_ms / 1000.0).round().max(1.0);
        let bytes = rows * phase.row_bytes as f64;
        let mu = p.service_rows_per_s(cur.write_concurrency);
        let memtable = cur.inline_flush_max_bytes.max(1) as f64;
        let commit = p.commit_ms * bytes / memtable;
        let spill = if cur.mem_tier_max_bytes > 0 {
            p.spill_ms * 64.0 * bytes / cur.mem_tier_max_bytes as f64
        } else {
            0.0
        };
        // CPU contention slows the apply once the box is saturated.
        let contention = 1.0 + 2.0 * (cpu_prev - 0.8).max(0.0);
        let apply_ms = (rows * 1000.0 / mu + commit + spill) * contention;
        self.backlog_ms = (self.backlog_ms + apply_ms - gap_ms).max(0.0);
        let lag_ms = self.backlog_ms + p.base_lag_s * 1000.0;
        let t_batch = self.now_ms;
        let commit_ts = t_batch - lag_ms.round() as i64;
        let delete_rows = (rows * phase.delete_fraction).round() as u64;
        self.stats.record_write(WriteSample {
            rows: rows as u64,
            bytes: bytes as u64,
            apply: Duration::from_secs_f64(apply_ms / 1000.0),
            arrival_gap: Some(Duration::from_secs_f64(gap_ms / 1000.0)),
            delete_rows,
        });
        self.stats.observe_source_commit_ts_ms(commit_ts);
        self.stats.fold_row_freshness(t_batch, Some(commit_ts));
        self.stats.set_last_visible_ts_ms(t_batch);
        // Each memtable flush emits one file per write shard.
        self.small_files += bytes / memtable * cur.write_concurrency.max(1) as f64;
        self.now_ms += gap_ms.round().max(1.0) as i64;
        apply_ms
    }

    fn observe(&mut self, phase: &Phase, cur: &ActuatorValues, tick_ms: i64) -> TickObs {
        let p = &self.sc.plant;
        let read_amp = self.small_files.floor() as usize;
        let reserve = cur.query_admission_reserve.min(p.cores) as f64;
        let shed = 1.0 - reserve / p.cores as f64;
        let served_qps = phase.queries_per_s * shed;
        let cpu_ingest = {
            let rho = (phase.rows_per_s / p.service_rows_per_s(cur.write_concurrency)).min(1.5);
            p.cpu_ingest_per_shard * cur.write_concurrency.max(1) as f64 * rho.min(1.0)
        };
        let cpu_compaction = p.cpu_compaction
            * (2000.0 / cur.compaction_background_interval_ms.max(1) as f64).sqrt();
        let cpu =
            (p.cpu_base + p.cpu_per_qps * served_qps + cpu_ingest + cpu_compaction).clamp(0.0, 1.5);
        let served_qps = if cpu > 1.0 {
            served_qps / cpu
        } else {
            served_qps
        };
        let p99_true = p.p99_base_ms
            + phase.query_ms_offset
            + p.p99_per_small_file_ms * read_amp as f64
            + 1000.0 * (cpu - 0.95).max(0.0);
        // Record the tick's queries into the real histogram: 2 % at the true p99,
        // the rest well under it, so the histogram's p99 lands on the bucket that
        // contains the true p99 (exactly the estimator the controller reads).
        let n_q = (served_qps * tick_ms as f64 / 1000.0).round() as u64;
        let n_rec = n_q.min(64);
        for _ in 0..n_rec {
            self.query_counter += 1;
            let lat = if self.query_counter.is_multiple_of(50) {
                p99_true
            } else {
                p99_true * 0.4
            };
            self.obs.record_query(lat);
        }
        let qph_true = if phase.queries_per_s > 0.0 {
            Some(served_qps * 3600.0)
        } else {
            None
        };
        let resident_tier = (cur.mem_tier_max_bytes.max(0) as f64)
            .min(phase.rows_per_s * phase.row_bytes as f64 * 10.0);
        let mem = (p.mem_fixed_bytes as f64
            + cur.inline_flush_max_bytes.max(0) as f64
            + resident_tier
            + cur.write_concurrency as f64 * p.shard_bytes as f64)
            / p.mem_budget_bytes as f64;
        let lag_true_s = self.backlog_ms / 1000.0 + p.base_lag_s;
        TickObs {
            lag_true_s,
            fresh_true_s: lag_true_s,
            p99_true_ms: p99_true,
            qph_true,
            cpu,
            mem,
            read_amp,
        }
    }

    fn snapshot(&self, obs: &TickObs) -> IngestSnapshot {
        // Mirrors `CayenneContext::ingest_snapshot`.
        let mut snap = self.stats.snapshot();
        let now_ms = self.now_ms;
        snap.replication_lag_secs = self.stats.replication_lag_secs(now_ms);
        snap.freshness_secs = self
            .stats
            .peak_row_freshness_secs(now_ms)
            .or_else(|| self.stats.freshness_secs(now_ms));
        snap.query_latency_p99_ms = self.obs.p99_latency_ms();
        snap.qph = obs.qph_true;
        snap.cpu_pressure = Some(obs.cpu);
        snap.cpu_burstable = false;
        snap.data_storage = self.sc.plant.data_storage;
        snap.metastore_storage = self.sc.plant.metastore_storage;
        snap.data_write_mbps = None;
        snap.metastore_write_mbps = None;
        snap
    }

    /// Faithful copy of `CayenneContext::retune` + `track_goal_feasibility`.
    fn control_step(&mut self, snap: &IngestSnapshot, phase_idx: usize) -> Option<Adjustment> {
        let cur = self.live.values();
        let since_last = self.last_adjust_ms.map_or(Duration::MAX, |t| {
            Duration::from_millis((self.now_ms - t).max(0) as u64)
        });
        let samples_at_last_move = self.last_adjust_samples;
        let b = self.sc.bounds;
        let goals = self.sc.goals;
        let adj = decide_with_goals(
            snap,
            &cur,
            &b,
            since_last,
            MIN_DWELL,
            samples_at_last_move,
            &goals,
        );
        // Infeasibility tracker.
        if goals.any_active() {
            let ingest_fresh = snap.samples > samples_at_last_move;
            let violated = goals.any_actionable_violation(snap, ingest_fresh);
            if adj.is_some() || !violated {
                self.goal_stuck_ticks = 0;
            } else {
                let dwell = goals.control_dwell(MIN_DWELL);
                if snap.samples >= WARMUP_BATCHES && since_last >= dwell {
                    self.goal_stuck_ticks += 1;
                    if self.goal_stuck_ticks == GOAL_INFEASIBLE_STUCK_TICKS {
                        self.infeasible_fired = true;
                    }
                }
            }
        }
        let adj = adj?;
        let old = actuator_value(&cur, adj.actuator);
        self.check_move(snap, &cur, &adj, old, phase_idx);
        self.live.apply(&adj);
        self.last_adjust_ms = Some(self.now_ms);
        self.last_adjust_samples = snap.samples;
        (self.tick, adj.actuator.as_str(), adj.new_value).hash(&mut self.hasher);
        self.moves.push(MoveRec {
            tick: self.tick,
            now_ms: self.now_ms,
            phase: phase_idx,
            actuator: adj.actuator,
            old,
            new: adj.new_value,
            reason: adj.reason,
        });
        Some(adj)
    }

    fn check_move(
        &mut self,
        snap: &IngestSnapshot,
        cur: &ActuatorValues,
        adj: &Adjustment,
        old: u64,
        phase_idx: usize,
    ) {
        let (lo, hi) = actuator_bounds(&self.sc.bounds, adj.actuator);
        // I1 — in bounds.
        if adj.new_value < lo || adj.new_value > hi {
            self.violations.push(format!(
                "I1 phase={phase_idx} tick={} {} -> {} outside [{lo}, {hi}] ({})",
                self.tick,
                adj.actuator.as_str(),
                adj.new_value,
                adj.reason
            ));
        }
        // I3 — direction coherence.
        match expected_sign(adj.reason) {
            Some(sign) => {
                let actual = adj.new_value.cmp(&old);
                let ok = match sign {
                    1 => actual == std::cmp::Ordering::Greater,
                    -1 => actual == std::cmp::Ordering::Less,
                    _ => true,
                };
                if !ok {
                    self.violations.push(format!(
                        "I3 phase={phase_idx} tick={} {} {old} -> {} contradicts reason '{}'",
                        self.tick,
                        adj.actuator.as_str(),
                        adj.new_value,
                        adj.reason
                    ));
                }
            }
            None => self.violations.push(format!(
                "I3 phase={phase_idx} tick={} unclassified reason '{}' for {}",
                self.tick,
                adj.reason,
                adj.actuator.as_str()
            )),
        }
        // I2 — memory rule dominance.
        if snap.mem_pressure.is_some_and(|p| p > MEM_PRESSURE_HIGH)
            && adj.actuator.consumes_memory()
            && adj.new_value > old
        {
            self.violations.push(format!(
                "I2 phase={phase_idx} tick={} raised {} {old} -> {} at mem pressure {:.3} ({})",
                self.tick,
                adj.actuator.as_str(),
                adj.new_value,
                snap.mem_pressure.unwrap_or(-1.0),
                adj.reason
            ));
        }
        let _ = cur;
    }

    fn run(mut self) -> RunResult {
        let phases = self.sc.phases.clone();
        let mut out = RunResult::default();
        let goals = self.sc.goals;
        let mut cpu_prev = self.sc.plant.cpu_base;
        for (pi, phase) in phases.iter().enumerate() {
            let phase_start = self.now_ms;
            let phase_end = phase_start + phase.duration_ms;
            let mut pm = PhaseMetrics {
                name: phase.name,
                ..PhaseMetrics::default()
            };
            let mut cost_sum = 0.0;
            let mut violated_ticks = 0u64;
            let mut tick_violated: Vec<bool> = Vec::new();
            let mut lag_sum = 0.0;
            let mut p99_sum = 0.0;
            let mut last_sign: HashMap<&'static str, i8> = HashMap::new();
            let moves_before = self.moves.len();
            while self.now_ms < phase_end {
                let cur = self.live.values();
                // Ticks are paced by the live compaction interval (compaction.rs).
                let tick_ms = (cur.compaction_background_interval_ms.max(1000)) as i64;
                // Apply the batches that arrive during this tick.
                let gap_mean_ms = 1000.0 / phase.batches_per_s.max(0.001);
                self.batch_credit += tick_ms as f64 / gap_mean_ms;
                let tick_start = self.now_ms;
                while self.batch_credit >= 1.0 && self.now_ms < tick_start + tick_ms {
                    self.apply_batch(phase, &cur, cpu_prev);
                    self.batch_credit -= 1.0;
                }
                self.now_ms = tick_start + tick_ms;
                // Background compaction at the tick: merge a pick of small files.
                if self.small_files.floor() as usize >= cur.compaction_trigger_files {
                    self.small_files = (self.small_files - 32.0).max(0.0);
                }
                let obs = self.observe(phase, &cur, tick_ms);
                cpu_prev = obs.cpu;
                self.stats.set_mem_pressure(obs.mem);
                self.stats.set_read_amp(obs.read_amp);
                let snap = self.snapshot(&obs);
                self.tick += 1;
                pm.ticks += 1;
                // True-plant cost for this tick.
                let (viol, c) = true_cost(&goals, &obs, &cur, self.sc.plant.cores);
                cost_sum += c;
                if viol > 0.0 {
                    violated_ticks += 1;
                    tick_violated.push(true);
                } else {
                    tick_violated.push(false);
                    if pm.time_to_meet_ms.is_none() {
                        pm.time_to_meet_ms = Some(self.now_ms - phase_start);
                    }
                    if violated_ticks > 0 && pm.time_to_recover_ms.is_none() {
                        pm.time_to_recover_ms = Some(self.now_ms - phase_start);
                    }
                }
                lag_sum += obs.lag_true_s;
                pm.max_lag_s = pm.max_lag_s.max(obs.lag_true_s);
                p99_sum += obs.p99_true_ms;
                if matches!(self.mode, Mode::Auto)
                    && let Some(adj) = self.control_step(&snap, pi)
                {
                    let rec = self.moves.last().expect("just pushed");
                    let key = adj.actuator.as_str();
                    let sign: i8 = if rec.new > rec.old { 1 } else { -1 };
                    // A move of magnitude 1 is the minimal integer step (e.g. a
                    // reserve slot 1 -> 0): never a "jump", whatever its ratio.
                    if rec.old > 0 && rec.new.abs_diff(rec.old) > 1 {
                        let rel = (rec.new as f64 - rec.old as f64).abs() / rec.old as f64;
                        if sign < 0 {
                            pm.max_relative_shrink = pm.max_relative_shrink.max(rel);
                        } else {
                            pm.max_relative_grow = pm.max_relative_grow.max(rel);
                        }
                    }
                    if self.now_ms - phase_start > SETTLE_MS {
                        if let Some(prev) = last_sign.get(key)
                            && *prev != sign
                        {
                            *pm.reversals.entry(key).or_insert(0) += 1;
                        }
                        last_sign.insert(key, sign);
                    } else {
                        last_sign.insert(key, sign);
                    }
                }
            }
            pm.moves = (self.moves.len() - moves_before) as u64;
            pm.cost = if pm.ticks > 0 {
                cost_sum / pm.ticks as f64
            } else {
                0.0
            };
            pm.violated_frac = if pm.ticks > 0 {
                violated_ticks as f64 / pm.ticks as f64
            } else {
                0.0
            };
            pm.violated_last_quarter = {
                let n = tick_violated.len();
                let start = n - n / 4;
                let tail = &tick_violated[start..];
                if tail.is_empty() {
                    0.0
                } else {
                    tail.iter().filter(|v| **v).count() as f64 / tail.len() as f64
                }
            };
            pm.mean_lag_s = if pm.ticks > 0 {
                lag_sum / pm.ticks as f64
            } else {
                0.0
            };
            pm.mean_p99_ms = if pm.ticks > 0 {
                p99_sum / pm.ticks as f64
            } else {
                0.0
            };
            pm.infeasible_fired = self.infeasible_fired;
            pm.final_values = Some(self.live.values());
            out.phases.push(pm);
        }
        let v = self.live.values();
        (
            v.inline_flush_max_bytes,
            v.compaction_background_interval_ms,
            v.compaction_trigger_files,
            v.write_concurrency,
            v.mem_tier_max_bytes,
            v.query_admission_reserve,
        )
            .hash(&mut self.hasher);
        out.trace_hash = self.hasher.finish();
        out.moves = self.moves;
        out.violations = self.violations;
        out.ticks = self.tick;
        out
    }
}

fn actuator_value(cur: &ActuatorValues, a: Actuator) -> u64 {
    match a {
        Actuator::InlineFlushBytes => cur.inline_flush_max_bytes.max(0) as u64,
        Actuator::MemTierMaxBytes => cur.mem_tier_max_bytes.max(0) as u64,
        Actuator::CompactionIntervalMs => cur.compaction_background_interval_ms,
        Actuator::CompactionTriggerFiles => cur.compaction_trigger_files as u64,
        Actuator::BakeDeletionIndexTrigger => cur.bake_deletion_index_trigger as u64,
        Actuator::WriteConcurrency => cur.write_concurrency as u64,
        Actuator::TargetVortexFileSize => cur.target_vortex_file_size_bytes.max(0) as u64,
        Actuator::QueryAdmissionReserve => cur.query_admission_reserve as u64,
    }
}

fn actuator_bounds(b: &TuningBounds, a: Actuator) -> (u64, u64) {
    match a {
        Actuator::InlineFlushBytes => (
            b.inline_flush_max_bytes.0.max(0) as u64,
            b.inline_flush_max_bytes.1.max(0) as u64,
        ),
        Actuator::MemTierMaxBytes => (
            b.mem_tier_max_bytes.0.max(0) as u64,
            b.mem_tier_max_bytes.1.max(0) as u64,
        ),
        Actuator::CompactionIntervalMs => b.compaction_background_interval_ms,
        Actuator::CompactionTriggerFiles => (
            b.compaction_trigger_files.0 as u64,
            b.compaction_trigger_files.1 as u64,
        ),
        Actuator::BakeDeletionIndexTrigger => (
            b.bake_deletion_index_trigger.0 as u64,
            b.bake_deletion_index_trigger.1 as u64,
        ),
        Actuator::WriteConcurrency => (b.write_concurrency.0 as u64, b.write_concurrency.1 as u64),
        Actuator::TargetVortexFileSize => (
            b.target_vortex_file_size_bytes.0.max(0) as u64,
            b.target_vortex_file_size_bytes.1.max(0) as u64,
        ),
        Actuator::QueryAdmissionReserve => (
            b.query_admission_reserve.0 as u64,
            b.query_admission_reserve.1 as u64,
        ),
    }
}

/// The direction a `reason` string promises: `+1` raise, `-1` lower. `None` for
/// an unclassified reason (a finding in itself — every reason must say which way
/// it moves).
fn expected_sign(reason: &str) -> Option<i8> {
    if reason.contains("release a reserved") {
        return Some(-1);
    }
    if reason.contains("reserve query-admission") {
        return Some(1);
    }
    const SHRINK: [&str; 7] = [
        "shrink",
        "collapse",
        "shed",
        "compact more",
        "lower compaction trigger",
        "lower the bake trigger",
        "release",
    ];
    const GROW: [&str; 9] = [
        "enlarge",
        "pre-grow",
        "raise",
        "grow",
        "larger memtable",
        "relax the compaction trigger",
        "back off",
        "reserve",
        "lengthen",
    ];
    if SHRINK.iter().any(|k| reason.contains(k)) {
        return Some(-1);
    }
    if GROW.iter().any(|k| reason.contains(k)) {
        return Some(1);
    }
    None
}

/// True-plant cost for one tick as `(violation, total)`: `violation` is the sum
/// over active goals of the normalized violation (0 when every goal is met);
/// `total` adds a tiny resource term so an oracle prefers the cheaper of two
/// configs that both meet the goals. Only `violation` decides "met".
fn true_cost(goals: &Goals, obs: &TickObs, cur: &ActuatorValues, cores: usize) -> (f64, f64) {
    let lower = |target: f64, m: f64| ((m - target) / target).max(0.0);
    let mut c = 0.0;
    if let Some(g) = goals.replication_lag {
        c += lower(g.target, obs.lag_true_s);
    }
    if let Some(g) = goals.freshness {
        c += lower(g.target, obs.fresh_true_s);
    }
    if let Some(g) = goals.query_latency_p99 {
        c += lower(g.target, obs.p99_true_ms);
    }
    if let Some(g) = goals.qph
        && let Some(q) = obs.qph_true
    {
        c += ((g.target - q) / g.target).max(0.0);
    }
    let resource = 0.001
        * (cur.write_concurrency as f64 / cores.max(1) as f64
            + 2000.0 / cur.compaction_background_interval_ms.max(1) as f64);
    (c, c + resource)
}

// ---------------------------------------------------------------------------
// Scenario-level evaluation
// ---------------------------------------------------------------------------

struct Eval {
    name: &'static str,
    set: Set,
    legacy: bool,
    auto: RunResult,
    static_: RunResult,
    violations: Vec<String>,
}

fn evaluate(sc: &Scenario) -> Eval {
    let auto = Sim::new(sc, Mode::Auto).run();
    let static_ = Sim::new(sc, Mode::Fixed(sc.init)).run();
    let mut violations = auto.violations.clone();

    // Precondition: the loop actually moved something wherever a goal was
    // violated at some point (else AUTO was silently STATIC).
    let any_violation_seen = auto.phases.iter().any(|p| p.violated_frac > 0.0);
    if sc.goals.any_active() && any_violation_seen && auto.moves.is_empty() {
        violations.push("PRECONDITION no actuator moved although a goal was violated".to_string());
    }

    for (pi, phase) in sc.phases.iter().enumerate() {
        let pa = &auto.phases[pi];
        let ps = &static_.phases[pi];
        let hours = phase.duration_ms as f64 / MS_PER_HOUR as f64;
        // I4 — reversals on a stationary phase.
        if phase.stationary {
            for (k, n) in &pa.reversals {
                let allowed = (f64::from(MAX_REVERSALS_PER_HOUR) * hours).ceil().max(1.0) as u32;
                if *n > allowed {
                    violations.push(format!(
                        "I4 phase={pi}({}) {k} reversed direction {n} times (allowed {allowed})",
                        phase.name
                    ));
                }
            }
        }
        // I4b — no big shrink jumps.
        if pa.max_relative_shrink > MAX_RELATIVE_SHRINK {
            let worst = auto
                .moves
                .iter()
                .filter(|m| m.phase == pi && m.new < m.old && m.old > 0)
                .max_by(|a, b| {
                    let ra = (a.old as f64 - a.new as f64) / a.old as f64;
                    let rb = (b.old as f64 - b.new as f64) / b.old as f64;
                    ra.partial_cmp(&rb).expect("finite")
                })
                .map(|m| {
                    format!(
                        "{} {} -> {} ({})",
                        m.actuator.as_str(),
                        m.old,
                        m.new,
                        m.reason
                    )
                })
                .unwrap_or_default();
            violations.push(format!(
                "I4b phase={pi}({}) max relative shrink {:.2} > {MAX_RELATIVE_SHRINK}: {worst}",
                phase.name, pa.max_relative_shrink
            ));
        }
        // I5 — liveness on a feasible phase.
        if let Some(cfg) = phase.feasible_with {
            let probe = Sim::new(sc, Mode::Fixed(cfg)).run();
            let pf = &probe.phases[pi];
            assert!(
                pf.violated_frac < 0.5,
                "scenario '{}' phase {pi} is misconfigured: feasible_with does not meet the goals \
                 (violated {:.0}% of ticks)",
                sc.name,
                pf.violated_frac * 100.0
            );
            if pa.violated_last_quarter > 0.5 {
                violations.push(format!(
                    "I5 phase={pi}({}) feasible goal still violated over the last quarter of the \
                     phase ({:.0}% of its ticks): time_to_recover={:?} violated_frac={:.2} \
                     infeasible_fired={} final={:?}",
                    phase.name,
                    pa.violated_last_quarter * 100.0,
                    pa.time_to_recover_ms,
                    pa.violated_frac,
                    pa.infeasible_fired,
                    pa.final_values.map(|v| (
                        v.write_concurrency,
                        v.inline_flush_max_bytes / MIB,
                        v.compaction_background_interval_ms,
                        v.mem_tier_max_bytes / MIB
                    ))
                ));
            }
        }
        // I6b — relax liveness. "Handed back" for the interval means at least the
        // warm start or the goal dwell, whichever is smaller: in goal mode the
        // dwell (window / 8, floored at 5 s) is the controller's own cadence and
        // the relax ceiling, and a warm start above it is tolerated but never
        // restored.
        let goal_dwell_ms =
            (u64::try_from(sc.goals.convergence_window.as_millis()).unwrap_or(u64::MAX) / 8)
                .max(5_000);
        let interval_reference = sc.init.compaction_background_interval_ms.min(goal_dwell_ms);
        if phase.expect_relax
            && let Some(v) = pa.final_values
            && (v.write_concurrency > sc.init.write_concurrency
                || v.compaction_background_interval_ms < interval_reference)
        {
            violations.push(format!(
                "I6b phase={pi}({}) resources not handed back: write_concurrency={} (init {}) \
                 compaction_interval={} (init {})",
                phase.name,
                v.write_concurrency,
                sc.init.write_concurrency,
                v.compaction_background_interval_ms,
                sc.init.compaction_background_interval_ms
            ));
        }
        // I7 — Bao floor.
        if pa.cost > ps.cost * BAO_TOLERANCE_FACTOR + BAO_TOLERANCE_ABS {
            violations.push(format!(
                "I7 phase={pi}({}) AUTO cost {:.4} > STATIC cost {:.4} (max lag auto {:.1}s vs static {:.1}s)",
                phase.name, pa.cost, ps.cost, pa.max_lag_s, ps.max_lag_s
            ));
        }
    }
    // I6a — false violation.
    if sc.plant_meets_goals {
        let goal_moves: Vec<&MoveRec> = auto
            .moves
            .iter()
            .filter(|m| m.reason.contains("goal"))
            .collect();
        if let Some(m) = goal_moves.first() {
            violations.push(format!(
                "I6a plant meets every goal but the controller moved {} {} -> {} ({}) [{} goal moves]",
                m.actuator.as_str(),
                m.old,
                m.new,
                m.reason,
                goal_moves.len()
            ));
        }
        if auto.phases.iter().any(|p| p.infeasible_fired) {
            violations
                .push("I6a plant meets every goal but the SLO was declared infeasible".to_string());
        }
    }
    Eval {
        name: sc.name,
        set: sc.set,
        legacy: !sc.goals.any_active(),
        auto,
        static_,
        violations,
    }
}

fn print_eval(e: &Eval) {
    let reversals: Vec<String> = e
        .auto
        .phases
        .iter()
        .map(|p| {
            let mut r: Vec<String> = p
                .reversals
                .iter()
                .map(|(k, v)| format!("{k}:{v}"))
                .collect();
            r.sort();
            format!("{}[{}]", p.name, r.join(","))
        })
        .collect();
    let costs: Vec<String> = e
        .auto
        .phases
        .iter()
        .zip(e.static_.phases.iter())
        .map(|(a, s)| {
            format!(
                "{}(auto={:.4},static={:.4},ttr={:?},vlq={:.2},maxlag={:.1}s,moves={},shrink={:.2},grow={:.2})",
                a.name,
                a.cost,
                s.cost,
                a.time_to_recover_ms,
                a.violated_last_quarter,
                a.max_lag_s,
                a.moves,
                a.max_relative_shrink,
                a.max_relative_grow
            )
        })
        .collect();
    println!(
        "SIM name={} set={} ticks={} moves={} viol={} reversals={} phases={} hash={:016x}",
        e.name,
        e.set.as_str(),
        e.auto.ticks,
        e.auto.moves.len(),
        e.violations.len(),
        reversals.join(" "),
        costs.join(" "),
        e.auto.trace_hash
    );
    for v in &e.violations {
        if e.legacy {
            println!("SIM_LEGACY_VIOLATION {} {}", e.name, v);
        } else {
            println!("SIM_VIOLATION {} {}", e.name, v);
        }
    }
    for m in e.auto.moves.iter().take(60) {
        println!(
            "SIM_MOVE {} tick={} t={}s phase={} {} {} -> {} : {}",
            e.name,
            m.tick,
            (m.now_ms - 1_700_000_000_000) / 1000,
            m.phase,
            m.actuator.as_str(),
            m.old,
            m.new,
            m.reason
        );
    }
}

// ---------------------------------------------------------------------------
// Scenario catalog
// ---------------------------------------------------------------------------

/// Warm-start values that the S1/S5 load can be met with (verified by I5's probe).
fn feasible_fast() -> ActuatorValues {
    ActuatorValues {
        inline_flush_max_bytes: 128 * MIB,
        write_concurrency: 8,
        mem_tier_max_bytes: 2048 * MIB,
        ..warm_start()
    }
}

fn train_scenarios() -> Vec<Scenario> {
    let plant = Plant::default_ssd();
    let bounds = bounds_for(plant.cores);
    vec![
        // S1 — a lag goal on a load the warm start cannot keep up with, that the
        // reachable configs can: the loop must converge (I5) and beat STATIC (I7).
        Scenario {
            name: "s1_lag_goal_steady_feasible",
            set: Set::Train,
            seed: 1,
            plant,
            goals: lag_goal(5.0),
            init: warm_start(),
            bounds,
            phases: vec![Phase {
                feasible_with: Some(feasible_fast()),
                ..Phase::steady("behind", 900, 60_000.0)
            }],
            plant_meets_goals: false,
        },
        // S2 — a lag goal comfortably met at ρ≈0.9 on the warm start. Relaxing a
        // shard pushes the plant past saturation: this is the relax↔tighten
        // limit-cycle probe (I4 reversals, I7 vs a STATIC that never falls behind).
        Scenario {
            name: "s2_relax_headroom_limit_cycle",
            set: Set::Train,
            seed: 2,
            plant,
            goals: lag_goal(5.0),
            init: warm_start(),
            bounds,
            phases: vec![Phase::steady("steady_rho_0_9", 3600, 38_000.0)],
            plant_meets_goals: true,
        },
        // S3 — a query-latency goal of 150 ms on a table whose true p99 sits at
        // ~120 ms (inside the histogram's (100, 200] bucket): the estimator reports
        // 200 ms. The plant meets the goal; any goal move is a false violation (I6a).
        Scenario {
            name: "s3_p99_bucket_false_violation",
            set: Set::Train,
            seed: 3,
            plant,
            goals: latency_goal(150.0),
            init: warm_start(),
            bounds,
            phases: vec![Phase {
                queries_per_s: 5.0,
                query_ms_offset: 70.0,
                ..Phase::steady("light_queries", 1800, 10_000.0)
            }],
            plant_meets_goals: true,
        },
        // S4 — lag goal behind under CPU contention from queries, no query goal:
        // the query-admission reserve is the remaining lever. Bang-bang probe (I4).
        Scenario {
            name: "s4_admission_reserve_bangbang",
            set: Set::Train,
            seed: 4,
            plant,
            goals: lag_goal(5.0),
            init: warm_start(),
            bounds,
            phases: vec![Phase {
                queries_per_s: 30.0,
                ..Phase::steady("contended", 2400, 60_000.0)
            }],
            plant_meets_goals: false,
        },
        // S5 — decision cadence: a long healthy phase lets the relax tier stretch
        // the compaction interval (and so the tick) toward 60 s; the shift that
        // follows must still re-converge within the phase (I5).
        Scenario {
            name: "s5_cadence_after_relax",
            set: Set::Train,
            seed: 5,
            plant,
            goals: lag_goal(5.0),
            init: warm_start(),
            bounds,
            phases: vec![
                Phase::steady("healthy", 900, 10_000.0),
                Phase {
                    feasible_with: Some(feasible_fast()),
                    ..Phase::steady("shift_behind", 900, 60_000.0)
                },
            ],
            plant_meets_goals: false,
        },
        // S6 — a query-latency goal configured on a table that serves no queries
        // (p99 unavailable) next to a lag goal: after the behind phase the light
        // phase must hand resources back (I6b).
        Scenario {
            name: "s6_unavailable_metric_blocks_relax",
            set: Set::Train,
            seed: 6,
            plant,
            goals: lag_and_latency_goal(5.0, 200.0),
            init: warm_start(),
            bounds,
            phases: vec![
                Phase {
                    stationary: false,
                    ..Phase::steady("behind", 600, 60_000.0)
                },
                Phase {
                    expect_relax: true,
                    ..Phase::steady("light", 1800, 5_000.0)
                },
            ],
            plant_meets_goals: false,
        },
        // S7 — freshness goal violated with the apply behind: the freshness-shrink
        // lever's additive step against a small current tier (I4b no-big-jumps).
        Scenario {
            name: "s7_freshness_shrink_step",
            set: Set::Train,
            seed: 7,
            plant,
            goals: freshness_goal(3.0),
            init: warm_start(),
            bounds,
            phases: vec![Phase {
                stationary: false,
                ..Phase::steady("behind", 600, 60_000.0)
            }],
            plant_meets_goals: false,
        },
        // S8 — the legacy (no goals) ladder, behind then healthy. Its trace hash is
        // the G2 guardrail: the goal-driven work must leave it byte-identical.
        Scenario {
            name: "s8_legacy_no_goals",
            set: Set::Train,
            seed: 8,
            plant,
            goals: Goals::none(),
            init: warm_start(),
            bounds,
            phases: vec![
                Phase {
                    stationary: false,
                    ..Phase::steady("behind", 600, 60_000.0)
                },
                Phase::steady("healthy", 900, 10_000.0),
            ],
            plant_meets_goals: false,
        },
    ]
}

fn heldout_scenarios() -> Vec<Scenario> {
    let plant16 = Plant {
        cores: 16,
        ..Plant::default_ssd()
    };
    let plant_ebs = Plant {
        data_storage: StorageClass::Ebs,
        metastore_storage: StorageClass::Ebs,
        commit_ms: 400.0,
        ..Plant::default_ssd()
    };
    let bounds16 = bounds_for(16);
    let bounds8 = bounds_for(8);
    vec![
        // HO1 — healthy → behind → healthy on a 16-core box with a tighter lag goal.
        Scenario {
            name: "ho1_lag_goal_three_phase_16core",
            set: Set::HeldOut,
            seed: 1001,
            plant: plant16,
            goals: lag_goal(3.0),
            init: warm_start(),
            bounds: bounds16,
            phases: vec![
                Phase::steady("healthy_a", 600, 8_000.0),
                Phase {
                    feasible_with: Some(ActuatorValues {
                        write_concurrency: 16,
                        ..feasible_fast()
                    }),
                    ..Phase::steady("behind", 1200, 70_000.0)
                },
                Phase {
                    expect_relax: false,
                    ..Phase::steady("healthy_b", 1800, 30_000.0)
                },
            ],
            plant_meets_goals: false,
        },
        // HO2 — query-latency goal 300 ms, true p99 ≈ 260 ms (bucket (200, 500]).
        Scenario {
            name: "ho2_p99_bucket_300ms",
            set: Set::HeldOut,
            seed: 1002,
            plant: Plant::default_ssd(),
            goals: latency_goal(300.0),
            init: warm_start(),
            bounds: bounds8,
            phases: vec![Phase {
                queries_per_s: 8.0,
                query_ms_offset: 210.0,
                ..Phase::steady("queries", 2400, 15_000.0)
            }],
            plant_meets_goals: true,
        },
        // HO3 — admission reserve on 16 cores with a heavier query load.
        Scenario {
            name: "ho3_admission_reserve_16core",
            set: Set::HeldOut,
            seed: 1003,
            plant: plant16,
            goals: lag_goal(5.0),
            init: warm_start(),
            bounds: bounds16,
            phases: vec![Phase {
                queries_per_s: 45.0,
                ..Phase::steady("contended", 2400, 90_000.0)
            }],
            plant_meets_goals: false,
        },
        // HO4 — mutation-heavy stream on EBS (an axis the train set never shows).
        Scenario {
            name: "ho4_mutation_heavy_ebs",
            set: Set::HeldOut,
            seed: 1004,
            plant: plant_ebs,
            goals: lag_goal(8.0),
            init: warm_start(),
            bounds: bounds8,
            phases: vec![
                Phase {
                    delete_fraction: 0.5,
                    stationary: false,
                    ..Phase::steady("behind_mutations", 900, 45_000.0)
                },
                Phase {
                    delete_fraction: 0.5,
                    ..Phase::steady("steady_mutations", 1800, 20_000.0)
                },
            ],
            plant_meets_goals: false,
        },
        // HO5 — a QPH goal beside the lag goal on a table with no queries.
        Scenario {
            name: "ho5_qph_unavailable_blocks_relax",
            set: Set::HeldOut,
            seed: 1005,
            plant: Plant::default_ssd(),
            goals: lag_and_qph_goal(5.0, 1000.0),
            init: warm_start(),
            bounds: bounds8,
            phases: vec![
                Phase {
                    stationary: false,
                    ..Phase::steady("behind", 600, 60_000.0)
                },
                Phase {
                    expect_relax: true,
                    ..Phase::steady("light", 1800, 5_000.0)
                },
            ],
            plant_meets_goals: false,
        },
        // HO6 — legacy ladder on EBS (G2 companion).
        Scenario {
            name: "ho6_legacy_no_goals_ebs",
            set: Set::HeldOut,
            seed: 1006,
            plant: plant_ebs,
            goals: Goals::none(),
            init: warm_start(),
            bounds: bounds8,
            phases: vec![
                Phase {
                    stationary: false,
                    ..Phase::steady("behind", 600, 50_000.0)
                },
                Phase::steady("healthy", 900, 8_000.0),
            ],
            plant_meets_goals: false,
        },
    ]
}

fn run_set(set: Set, scenarios: &[Scenario]) -> usize {
    let mut total = 0usize;
    let mut legacy = 0usize;
    for sc in scenarios {
        let e = evaluate(sc);
        print_eval(&e);
        // The legacy (no-goal) ladder is outside this run's search space — its
        // trace is pinned by G2 — so its findings are reported, not counted.
        if sc.goals.any_active() {
            total += e.violations.len();
        } else {
            legacy += e.violations.len();
        }
    }
    println!(
        "SIM_SUMMARY set={} scenarios={} violations={total} legacy_informational={legacy}",
        set.as_str(),
        scenarios.len()
    );
    total
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// TRAIN set: the loop's primary correctness metric (`violations`, minimize to 0).
#[test]
fn sim_train_invariants() {
    let total = run_set(Set::Train, &train_scenarios());
    assert_eq!(
        total, 0,
        "controller invariant violations on the TRAIN set: {total}"
    );
}

/// HELD-OUT set: never used to develop a fix; the report's headline.
#[test]
fn sim_heldout_invariants() {
    let total = run_set(Set::HeldOut, &heldout_scenarios());
    assert_eq!(
        total, 0,
        "controller invariant violations on the HELD-OUT set: {total}"
    );
}

/// G2 — the legacy (no goals) ladder's trace must not change. The hashes are
/// pinned from the base commit's run (`journal.md` iter 0).
#[test]
fn sim_legacy_trace_hash_pinned() {
    let scenarios: Vec<Scenario> = train_scenarios()
        .into_iter()
        .chain(heldout_scenarios())
        .filter(|s| !s.goals.any_active())
        .collect();
    // Print every hash first so a mismatch still leaves the full record in the log.
    let hashes: Vec<(&'static str, String)> = scenarios
        .iter()
        .map(|sc| {
            let auto = Sim::new(sc, Mode::Auto).run();
            let h = format!("{:016x}", auto.trace_hash);
            println!("SIM_LEGACY_HASH {} {h}", sc.name);
            (sc.name, h)
        })
        .collect();
    for (name, h) in hashes {
        match name {
            "s8_legacy_no_goals" => {
                assert_eq!(h, LEGACY_HASH_S8, "legacy ladder trace changed (s8)");
            }
            "ho6_legacy_no_goals_ebs" => {
                assert_eq!(h, LEGACY_HASH_HO6, "legacy ladder trace changed (ho6)");
            }
            other => panic!("unpinned legacy scenario {other}"),
        }
    }
}

/// Pinned at the base commit `3f55c4cdf8` (`journal.md` iter 0). `PENDING` until
/// the first run records a value.
const LEGACY_HASH_S8: &str = "d05d04e1f9dd1c6a";
const LEGACY_HASH_HO6: &str = "73b60aaccd07b0fa";

/// P1 — property sweep over random (snapshot, values, bounds, goals): the decision
/// never panics, never leaves the bounds, and every move's direction matches its
/// reason (I1, I3). No plant.
#[test]
fn sim_property_sweep_no_panic_in_bounds_coherent() {
    let mut rng = Rng::new(42);
    let mut panics = 0u32;
    let mut out_of_bounds = 0u32;
    let mut incoherent = Vec::new();
    let mut moves = 0u32;
    const N: u32 = 20_000;
    for i in 0..N {
        let cores = rng.range_u64(1, 64) as usize;
        let mut b = bounds_for(cores);
        // Randomly collapse (pin) some bounds, and randomize the ranges.
        let pin = |rng: &mut Rng, (lo, hi): (i64, i64)| -> (i64, i64) {
            if rng.chance(0.15) {
                let v = lo + ((hi - lo) as f64 * rng.next_f64()) as i64;
                (v, v)
            } else {
                (lo, hi)
            }
        };
        b.inline_flush_max_bytes = pin(&mut rng, b.inline_flush_max_bytes);
        b.mem_tier_max_bytes = if rng.chance(0.1) {
            (0, 0)
        } else {
            pin(&mut rng, b.mem_tier_max_bytes)
        };
        b.target_vortex_file_size_bytes = if rng.chance(0.1) {
            (0, 0)
        } else {
            pin(&mut rng, b.target_vortex_file_size_bytes)
        };
        if rng.chance(0.1) {
            b.compaction_background_interval_ms = (0, 0);
        }
        let within_i64 =
            |rng: &mut Rng, (lo, hi): (i64, i64)| lo + ((hi - lo) as f64 * rng.next_f64()) as i64;
        let within_u64 = |rng: &mut Rng, (lo, hi): (u64, u64)| rng.range_u64(lo, hi);
        let within_usize =
            |rng: &mut Rng, (lo, hi): (usize, usize)| rng.range_u64(lo as u64, hi as u64) as usize;
        let inline = within_i64(&mut rng, b.inline_flush_max_bytes);
        let cur = ActuatorValues {
            inline_flush_max_bytes: inline,
            inline_flush_max_rows: (inline / 256).max(64),
            inline_flush_max_segments: 64,
            compaction_background_interval_ms: within_u64(
                &mut rng,
                b.compaction_background_interval_ms,
            ),
            compaction_trigger_files: within_usize(&mut rng, b.compaction_trigger_files),
            bake_deletion_index_trigger: within_usize(&mut rng, b.bake_deletion_index_trigger),
            write_concurrency: within_usize(&mut rng, b.write_concurrency),
            mem_tier_max_bytes: within_i64(&mut rng, b.mem_tier_max_bytes),
            target_vortex_file_size_bytes: within_i64(&mut rng, b.target_vortex_file_size_bytes),
            query_admission_reserve: within_usize(&mut rng, b.query_admission_reserve),
        };
        let opt = |rng: &mut Rng, lo: f64, hi: f64| {
            if rng.chance(0.3) {
                None
            } else {
                Some(rng.range_f64(lo, hi))
            }
        };
        let arrival_gap_ms = rng.range_f64(1.0, 5_000.0);
        let apply_ms = rng.range_f64(0.1, 10_000.0);
        let storage = |rng: &mut Rng| match rng.range_u64(0, 3) {
            0 => StorageClass::LocalSsd,
            1 => StorageClass::Ebs,
            2 => StorageClass::Tmpfs,
            _ => StorageClass::Unknown,
        };
        let snap = IngestSnapshot {
            rows_per_sec: rng.range_f64(0.0, 1e6),
            bytes_per_sec: rng.range_f64(-1.0, 1e9),
            apply_ms,
            arrival_gap_ms,
            apply_vs_arrival: apply_ms / arrival_gap_ms,
            read_amp: rng.range_u64(0, 64) as usize,
            bake_residual: rng
                .chance(0.5)
                .then(|| rng.range_u64(0, 10_000_000) as usize),
            bake_gap_ms: rng.chance(0.5).then(|| rng.range_u64(0, 600_000) as i64),
            mem_pressure: opt(&mut rng, 0.0, 1.3),
            delete_fraction: rng.range_f64(0.0, 1.0),
            arrival_cv: rng.range_f64(0.0, 3.0),
            samples: rng.range_u64(0, 100_000),
            replication_lag_secs: opt(&mut rng, 0.0, 600.0),
            freshness_secs: opt(&mut rng, 0.0, 600.0),
            query_latency_p99_ms: opt(&mut rng, 1.0, 60_000.0),
            qph: opt(&mut rng, 0.0, 1e5),
            cpu_pressure: opt(&mut rng, 0.0, 1.5),
            cpu_burstable: rng.chance(0.2),
            io_latency_ms: opt(&mut rng, 0.0, 5_000.0),
            publish_latency_ms: opt(&mut rng, 0.0, 5_000.0),
            io_latency_fast_ms: opt(&mut rng, 0.0, 20_000.0),
            publish_latency_fast_ms: opt(&mut rng, 0.0, 20_000.0),
            data_storage: storage(&mut rng),
            metastore_storage: storage(&mut rng),
            data_write_mbps: opt(&mut rng, 10.0, 4_000.0),
            metastore_write_mbps: opt(&mut rng, 10.0, 4_000.0),
        };
        let goals = Goals::from_targets(
            opt(&mut rng, 0.1, 100.0),
            opt(&mut rng, 0.1, 100.0),
            opt(&mut rng, 1.0, 10_000.0),
            opt(&mut rng, 1.0, 1e5),
            Duration::from_secs(rng.range_u64(1, 600)),
        );
        let since_last = Duration::from_millis(rng.range_u64(0, 120_000));
        let samples_at_last = rng.range_u64(0, 100_000);
        let res = catch_unwind(AssertUnwindSafe(|| {
            decide_with_goals(
                &snap,
                &cur,
                &b,
                since_last,
                MIN_DWELL,
                samples_at_last,
                &goals,
            )
        }));
        match res {
            Err(_) => {
                panics += 1;
                if panics <= 3 {
                    println!("SIM_PANIC iter={i} cur={cur:?} bounds={b:?} snap={snap:?}");
                }
            }
            Ok(Some(adj)) => {
                moves += 1;
                let (lo, hi) = actuator_bounds(&b, adj.actuator);
                if adj.new_value < lo || adj.new_value > hi {
                    out_of_bounds += 1;
                }
                let old = actuator_value(&cur, adj.actuator);
                match expected_sign(adj.reason) {
                    Some(1) if adj.new_value <= old => incoherent.push(format!(
                        "{} {old}->{} '{}'",
                        adj.actuator.as_str(),
                        adj.new_value,
                        adj.reason
                    )),
                    Some(-1) if adj.new_value >= old => incoherent.push(format!(
                        "{} {old}->{} '{}'",
                        adj.actuator.as_str(),
                        adj.new_value,
                        adj.reason
                    )),
                    None => incoherent.push(format!("unclassified '{}'", adj.reason)),
                    _ => {}
                }
            }
            Ok(None) => {}
        }
    }
    println!(
        "SIM_PROPERTY iters={N} moves={moves} panics={panics} out_of_bounds={out_of_bounds} incoherent={}",
        incoherent.len()
    );
    for s in incoherent.iter().take(10) {
        println!("SIM_INCOHERENT {s}");
    }
    assert_eq!(panics, 0, "decide_with_goals panicked");
    assert_eq!(out_of_bounds, 0, "a move left its bounds");
    assert!(
        incoherent.is_empty(),
        "direction-incoherent moves: {}",
        incoherent.len()
    );
}

/// P2 — the adaptive bounds derivations never produce `floor > ceiling` (which
/// would make `clamp` panic on the tick) for any warm-start value.
#[test]
fn sim_bounds_derivations_are_ordered() {
    let mut rng = Rng::new(7);
    let mut bad = Vec::new();
    // Silence the per-panic report while probing (the count is the result).
    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    for _ in 0..2_000 {
        let initial = rng.range_u64(0, 8 * 1024 * MIB as u64) as i64;
        let r = catch_unwind(AssertUnwindSafe(|| {
            adaptive_target_file_size_bounds(initial)
        }));
        match r {
            Err(_) => bad.push(format!("target_file_size_bounds({initial}) panicked")),
            Ok((lo, hi)) if lo > hi => {
                bad.push(format!("target_file_size_bounds({initial}) = ({lo}, {hi})"));
            }
            Ok(_) => {}
        }
        let (lo, hi) = adaptive_inline_flush_bounds(initial);
        if lo > hi {
            bad.push(format!("inline_flush_bounds({initial}) = ({lo}, {hi})"));
        }
        let (lo, hi) = adaptive_mem_tier_bounds(initial);
        if lo > hi {
            bad.push(format!("mem_tier_bounds({initial}) = ({lo}, {hi})"));
        }
    }
    std::panic::set_hook(prev_hook);
    println!("SIM_BOUNDS checked=2000 bad={}", bad.len());
    for b in bad.iter().take(5) {
        println!("SIM_BOUNDS_BAD {b}");
    }
    assert!(bad.is_empty(), "unordered/panicking bounds: {}", bad.len());
}

/// Phase O — regret vs a per-phase static oracle (grid search) for every scenario
/// with a goal. Report only; the loop's Phase C metric is the invariant count.
#[test]
fn sim_regret_report() {
    let scenarios: Vec<Scenario> = train_scenarios()
        .into_iter()
        .chain(heldout_scenarios())
        .filter(|s| s.goals.any_active())
        .collect();
    let ws = [1usize, 2, 4, 8, 16];
    let bs = [2 * MIB, 8 * MIB, 32 * MIB, 128 * MIB];
    let cis = [2_000u64, 10_000, 30_000, 60_000];
    let ms = [64 * MIB, 256 * MIB, 1024 * MIB, 2048 * MIB];
    for sc in &scenarios {
        let auto = Sim::new(sc, Mode::Auto).run();
        let static_ = Sim::new(sc, Mode::Fixed(sc.init)).run();
        let mut oracle_costs = vec![f64::INFINITY; sc.phases.len()];
        let mut oracle_cfg: Vec<Option<ActuatorValues>> = vec![None; sc.phases.len()];
        for &w in &ws {
            if w > sc.bounds.write_concurrency.1 {
                continue;
            }
            for &b in &bs {
                for &ci in &cis {
                    for &m in &ms {
                        let cfg = ActuatorValues {
                            inline_flush_max_bytes: b,
                            inline_flush_max_rows: (b / 200).max(64),
                            compaction_background_interval_ms: ci,
                            write_concurrency: w,
                            mem_tier_max_bytes: m,
                            ..sc.init
                        };
                        let r = Sim::new(sc, Mode::Fixed(cfg)).run();
                        for (pi, p) in r.phases.iter().enumerate() {
                            if p.cost < oracle_costs[pi] {
                                oracle_costs[pi] = p.cost;
                                oracle_cfg[pi] = Some(cfg);
                            }
                        }
                    }
                }
            }
        }
        for (pi, p) in auto.phases.iter().enumerate() {
            let o = oracle_costs[pi];
            let s = static_.phases[pi].cost;
            let regret = (p.cost - o) / o.max(0.05);
            let gap_closed = if s > o + 1e-9 {
                (s - p.cost) / (s - o)
            } else {
                f64::NAN
            };
            let oc = oracle_cfg[pi].map_or(String::from("-"), |c| {
                format!(
                    "w={},b={}MiB,ci={}ms,m={}MiB",
                    c.write_concurrency,
                    c.inline_flush_max_bytes / MIB,
                    c.compaction_background_interval_ms,
                    c.mem_tier_max_bytes / MIB
                )
            });
            println!(
                "SIM_REGRET name={} set={} phase={}({}) auto={:.4} static={:.4} oracle={:.4} regret={:.3} gap_closed={:.3} ttr={:?} oracle_cfg={oc}",
                sc.name,
                sc.set.as_str(),
                pi,
                p.name,
                p.cost,
                s,
                o,
                regret,
                gap_closed,
                p.time_to_recover_ms
            );
        }
    }
}
