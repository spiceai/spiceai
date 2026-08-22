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

//! Tiered merge-tree compaction picker and background scheduler.
//!
//! Steady streaming ingestion produces many small Vortex files in the current
//! snapshot directory: each inline-memtable checkpoint emits one ~8 MB file,
//! and each non-inline write emits at least one Vortex file. Read fan-out and
//! object-store listing cost both grow linearly with file count.
//!
//! The picker buckets files by size into tiers — small, mid, large — and emits
//! a [`CompactionCandidate`] when the smallest non-empty tier has enough files
//! whose combined size is worth a rewrite. The warm-tier runner (in
//! [`crate::provider::table`]) rewrites **only** `candidate.paths` for
//! key-delete / append-only tables with no protected snapshots, and carries
//! unpicked settled files into the new snapshot via hardlink (local FS) or copy
//! (S3 / cross-device) — warm subset compaction. Position-delete tables, tables
//! with configured `sort_columns`, and tables carrying protected snapshots (which
//! the rewrite has to fold) still full-rewrite the current snapshot. The rewrite
//! goes through
//! `write_to_snapshot`, which honors `target_partitions` and the configured
//! target file size, so a pass typically produces one or a small number of
//! consolidated Vortex files for the picked tier.
//!
//! The module also owns [`BackgroundCompactor`], a per-table tokio task that
//! periodically invokes the runner. The task is `Semaphore`-gated so a fleet of
//! tables can't overwhelm the writer pool.

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Weak};
use std::time::{Duration, Instant};

use datafusion_execution::runtime_env::RuntimeEnv;
use parking_lot::{Mutex, RwLock};
use tokio::runtime::Handle;
use tokio::sync::{Notify, Semaphore};
use tokio::task::JoinHandle;

/// Process-wide handle to the dedicated compaction runtime, injected once at
/// startup by the binary (see `spiced`'s runtime setup). All Cayenne tables in
/// the process share it.
///
/// Compaction — both the size-tiered protected-snapshot merge and the full
/// snapshot rewrite — is CPU-heavy and runs in the background. Isolating it on
/// its own runtime keeps it off the query (compute) and CDC (refresh) runtimes
/// so a rewrite can't steal worker threads from latency-sensitive work. The
/// runtime is created with low thread priority, so compaction soaks up spare
/// cores without starving queries or ingest.
///
/// Replaceable so test binaries that create and drop multiple runtimes in one
/// process do not keep spawning onto a handle from an already-dropped runtime.
/// When unset — unit tests, embedders that don't wire it up, or
/// `dedicated_thread_pool=disabled` — compaction falls back to [`tokio::spawn`]
/// on the ambient runtime, preserving prior behavior.
static COMPACTION_RUNTIME: LazyLock<RwLock<Option<Handle>>> = LazyLock::new(|| RwLock::new(None));

/// Process-wide budget bounding how many Cayenne interval background
/// compactions run at once. Every table in the process draws on it, however it
/// was created.
///
/// Two engines open Cayenne tables — the accelerator, and `CREATE TABLE …
/// PARTITIONED BY` — and a catalog-level table has no narrower owner to charge
/// than the process itself. One budget for both is what holds total compaction
/// concurrency at the CPU budget regardless of the mix: a budget per engine
/// would let a process running both oversubscribe the writer pool by a factor
/// of two, and a budget per table would fan out without any ceiling at all.
///
/// Sized like the other Cayenne maintenance budgets, from
/// [`compaction_budget_permits`]. Post-write compaction
/// (`schedule_post_write_compaction`) is scheduled independently and does not
/// draw on this.
static COMPACTION_BUDGET: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(compaction_budget_permits())));

static COMPACTION_SHUTTING_DOWN: AtomicBool = AtomicBool::new(false);
static IN_FLIGHT_COMPACTION_PASSES: LazyLock<CompactionPassTracker> =
    LazyLock::new(CompactionPassTracker::default);

#[derive(Default)]
struct CompactionPassTracker {
    count: AtomicUsize,
    notify: Notify,
}

impl CompactionPassTracker {
    fn start(&self) -> CompactionPassGuard<'_> {
        self.count.fetch_add(1, Ordering::AcqRel);
        CompactionPassGuard { tracker: self }
    }

    fn finish(&self) {
        if self.count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.notify.notify_waiters();
        }
    }

    fn in_flight(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }

    async fn drain(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self.in_flight() == 0 {
                return true;
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return self.in_flight() == 0;
            }

            let notified = self.notify.notified();
            tokio::pin!(notified);
            // Register before the second count check so a pass that finishes
            // after the first check cannot notify between `in_flight()` and the
            // await setup, which would otherwise sleep until the timeout.
            notified.as_mut().enable();
            if self.in_flight() == 0 {
                return true;
            }

            if tokio::time::timeout(remaining, notified).await.is_err() {
                return self.in_flight() == 0;
            }
        }
    }
}

/// RAII marker for one active Cayenne maintenance pass that may be inside a
/// Vortex read/write pipeline on the compaction runtime.
pub(crate) struct CompactionPassGuard<'a> {
    tracker: &'a CompactionPassTracker,
}

impl Drop for CompactionPassGuard<'_> {
    fn drop(&mut self) {
        self.tracker.finish();
    }
}

/// Mark a Vortex-producing Cayenne maintenance pass as active unless compaction
/// shutdown has already begun.
///
/// The background schedulers themselves are long-lived sleep loops; tracking
/// those tasks would make shutdown wait forever. Track only the bounded pass
/// bodies (subset/full compaction, mem-tier checkpoint, cold promotion) so
/// runtime shutdown can avoid dropping Vortex CPU tasks while they are still
/// being awaited.
pub(crate) fn try_track_compaction_pass() -> Option<CompactionPassGuard<'static>> {
    let guard = IN_FLIGHT_COMPACTION_PASSES.start();
    if COMPACTION_SHUTTING_DOWN.load(Ordering::Acquire) {
        drop(guard);
        None
    } else {
        Some(guard)
    }
}

/// The process-wide compaction budget every Cayenne table's interval
/// background compactor draws on. See [`COMPACTION_BUDGET`].
#[must_use]
pub fn compaction_budget() -> Arc<Semaphore> {
    Arc::clone(&COMPACTION_BUDGET)
}

/// Permits in the process-wide compaction budget — its ceiling, which the
/// semaphore itself cannot report (it only exposes *available* permits).
#[must_use]
pub fn compaction_budget_permits() -> usize {
    cpu_budget::cpu_budget().cayenne_compaction_permits()
}

/// How Cayenne sizes the partition fan-out of its own maintenance passes
/// (compaction and the seq-prefix bake).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MaintenanceFanOut {
    /// One partition per output file the pass is estimated to produce, so the
    /// read fan-out matches the number of writers that will consume it. A merge
    /// small enough to land in one file therefore plans one partition rather
    /// than fanning out and immediately coalescing back.
    PerOutputFile,
    /// The same count for every pass, whatever its size.
    Fixed(usize),
}

/// Ceiling on any maintenance fan-out, and the value used when a pass has no
/// size estimate to derive one from.
///
/// `SessionConfig::default()` would resolve `target_partitions` to
/// `available_parallelism()`. The intent at these call sites is the logical CPU
/// count — it is the parallel-encode shard ceiling, and encoding is CPU-bound,
/// so more shards than cores buys no throughput and only inflates file count.
/// But `available_parallelism()` reports the *node's* cores whenever a pod sets
/// `requests.cpu` without `limits.cpu`, so it overstates that ceiling on exactly
/// the deployments where it matters. The CPU budget expresses the same intent
/// and gets the entitlement right. The operator's
/// `runtime.query.target_partitions` stays a read-path knob and is still not
/// inherited: raising it would not speed an encode.
static MAINTENANCE_FAN_OUT_BUDGET: LazyLock<usize> =
    LazyLock::new(|| cpu_budget::cpu_budget().target_partitions().max(1));

/// Resolved fan-out policy, read once per process.
///
/// `SPICE_CAYENNE_COMPACTION_TARGET_PARTITIONS` selects it so a benchmark sweep
/// can vary the fan-out per run without a rebuild: a whole number pins every
/// pass to that width, and `auto` derives it per pass from the output size. The
/// variable's name lags this scope, which started at compaction alone; renaming
/// it while a sweep is queued would make a run silently fall back to the budget
/// and read as a duplicate control, so the two change together afterwards.
static MAINTENANCE_FAN_OUT: LazyLock<MaintenanceFanOut> = LazyLock::new(|| {
    resolve_maintenance_fan_out(
        std::env::var("SPICE_CAYENNE_COMPACTION_TARGET_PARTITIONS")
            .ok()
            .as_deref(),
        *MAINTENANCE_FAN_OUT_BUDGET,
    )
});

/// Output files a write of `estimated_bytes` is expected to produce, before any
/// concurrency cap.
///
/// The unit is `target_size / 16` floored at 16 MiB and capped at `target_size`,
/// so the count answers "how many encode-efficient shards would this write
/// fill?" rather than "how many full target files?". `estimated_bytes` is
/// compression-blind in-memory Arrow bytes, which biases the count upward.
///
/// Single source of truth for that count: the encoder shard count
/// (`snapshot_shard_count`) and the [`MaintenanceFanOut::PerOutputFile`] read
/// fan-out both derive from it, so the two cannot drift apart — the whole point
/// of the policy is that they agree.
#[must_use]
pub(crate) fn estimated_output_files(estimated_bytes: u64, target_size_bytes: usize) -> u64 {
    /// Smallest span of bytes worth handing a separate encoder.
    const MIN_ENCODE_SHARD_BYTES: u64 = 16 * 1024 * 1024;
    // `target_size_bytes` comes from a configured MiB value and is never 0, but
    // guard anyway so a misconfiguration cannot divide by zero.
    let target = u64::try_from(target_size_bytes).unwrap_or(u64::MAX).max(1);
    let unit = (target / 16).clamp(MIN_ENCODE_SHARD_BYTES.min(target), target);
    (estimated_bytes / unit).max(1)
}

/// Partitions a maintenance pass with no size estimate plans across.
#[must_use]
pub(crate) fn internal_target_partitions() -> usize {
    match *MAINTENANCE_FAN_OUT {
        MaintenanceFanOut::Fixed(partitions) => partitions,
        MaintenanceFanOut::PerOutputFile => *MAINTENANCE_FAN_OUT_BUDGET,
    }
}

/// Partitions a maintenance pass plans across given the bytes it will merge.
///
/// `estimated_bytes` of 0 means "no estimate" (the caller could not price its
/// inputs) and falls back to [`internal_target_partitions`] rather than
/// deriving 1 from an absence — a pass must never be serialized because its
/// size was unknown.
#[must_use]
pub(crate) fn internal_target_partitions_for_output(
    estimated_bytes: u64,
    target_size_bytes: usize,
) -> usize {
    match *MAINTENANCE_FAN_OUT {
        MaintenanceFanOut::Fixed(partitions) => partitions,
        MaintenanceFanOut::PerOutputFile if estimated_bytes == 0 => internal_target_partitions(),
        MaintenanceFanOut::PerOutputFile => {
            let files = estimated_output_files(estimated_bytes, target_size_bytes);
            let ceiling = *MAINTENANCE_FAN_OUT_BUDGET;
            usize::try_from(files).unwrap_or(ceiling).clamp(1, ceiling)
        }
    }
}

/// Pure resolution behind [`MAINTENANCE_FAN_OUT`], separated so the override
/// parsing is testable without mutating the process environment (which is
/// `unsafe` in edition 2024 and racy across parallel tests).
///
/// A value that is absent, unparseable, or zero falls back to the budget: an
/// experiment knob must never be able to stop this work from running. The
/// resolved policy is always logged, so a run whose override never reached the
/// process is visible in its own log rather than silently reading as a control.
fn resolve_maintenance_fan_out(override_value: Option<&str>, budget: usize) -> MaintenanceFanOut {
    let budget = budget.max(1);
    let (resolved, source) = match override_value.map(str::trim) {
        None => (MaintenanceFanOut::Fixed(budget), "its CPU budget"),
        Some(raw) if raw.eq_ignore_ascii_case("auto") => (
            MaintenanceFanOut::PerOutputFile,
            "`SPICE_CAYENNE_COMPACTION_TARGET_PARTITIONS=auto`",
        ),
        Some(raw) => match raw.parse::<usize>() {
            Ok(partitions) if partitions > 0 => (
                MaintenanceFanOut::Fixed(partitions),
                "`SPICE_CAYENNE_COMPACTION_TARGET_PARTITIONS`",
            ),
            _ => {
                tracing::warn!(
                    "Ignoring `SPICE_CAYENNE_COMPACTION_TARGET_PARTITIONS={raw}`: expected `auto` or a whole number above zero."
                );
                (MaintenanceFanOut::Fixed(budget), "its CPU budget")
            }
        },
    };
    match resolved {
        MaintenanceFanOut::Fixed(partitions) => tracing::info!(
            "Cayenne is planning its compaction and bake passes across {partitions} partitions, from {source}."
        ),
        MaintenanceFanOut::PerOutputFile => tracing::info!(
            "Cayenne is sizing each compaction and bake pass to the output files it is estimated to produce (ceiling {budget}), from {source}."
        ),
    }
    resolved
}

/// Encode shards a MAINTENANCE write is pinned to, when pinned at all.
///
/// `snapshot_shard_count` sizes the encode fan-out from the write's bytes and
/// caps it at `cayenne_write_concurrency` (unset default 4, and an adaptive
/// actuator that can raise it). Microbenchmarks on the real CH-benCHmark table
/// shapes say that fan-out is a pessimization for a compaction output at every
/// size measured: on a 1.63 GiB `stock` pass — six target files' worth, where a
/// size-derived policy would ask for six shards — one shard takes 162 ms against
/// 389 ms at four and 1.9 s at sixteen. Sixteen is inside the core count on the
/// measuring host, so oversubscription does not explain it.
///
/// This exists to test that in CI against the metrics that matter (freshness,
/// convergence, QPH) rather than to assert it: unset leaves the sized behaviour
/// exactly as it is, so one binary serves both arms of the comparison. It pins
/// only `Maintenance` writes — `Delta` writes are the latency-bound CDC encode,
/// and serializing those would trade ingest for compaction, which is the wrong
/// direction.
static MAINTENANCE_ENCODE_SHARDS: LazyLock<Option<usize>> = LazyLock::new(|| {
    resolve_maintenance_encode_shards(
        std::env::var("SPICE_CAYENNE_MAINTENANCE_ENCODE_SHARDS")
            .ok()
            .as_deref(),
    )
});

/// Pure resolution behind [`MAINTENANCE_ENCODE_SHARDS`], separated so the
/// parsing is testable without mutating the process environment (which is
/// `unsafe` in edition 2024 and racy across parallel tests).
///
/// Anything unusable leaves the sized behaviour in place rather than pinning to a
/// guess: an experiment knob must not be able to change the shipped policy by
/// being mistyped.
fn resolve_maintenance_encode_shards(override_value: Option<&str>) -> Option<usize> {
    let raw = override_value?;
    match raw.trim().parse::<usize>() {
        Ok(shards) if shards > 0 => {
            tracing::info!(
                "Cayenne is pinning every compaction and bake encode to {shards} shard(s), from `SPICE_CAYENNE_MAINTENANCE_ENCODE_SHARDS`; CDC delta encodes keep their sized fan-out."
            );
            Some(shards)
        }
        _ => {
            tracing::warn!(
                "Ignoring `SPICE_CAYENNE_MAINTENANCE_ENCODE_SHARDS={raw}`: expected a whole number above zero, so maintenance encodes keep their size-derived fan-out."
            );
            None
        }
    }
}

/// How many of a seq-prefix bake's input files to rewrite concurrently, or
/// `None` to keep the single-stream union rewrite.
///
/// The bake's cost is not its byte count but its *duration*: the deletion index
/// it exists to shrink grows at the tombstone rate for as long as a pass runs, so
/// a pass that takes minutes leaves an index proportional to those minutes. The
/// union rewrite funnels every input through one `CoalescePartitionsExec` stream,
/// which is why a pass moves 120-205 MB/s on a host whose encode budget is 95%
/// idle.
///
/// Per-file is the right shape rather than a wider encode fan-out: the
/// `encode_fanout` bench lane is monotonically WORSE with more shards per write
/// (1 shard 71.7 ms -> 64 shards 173.2 ms on the `stock` shape), so the win has to
/// come from concurrent single-shard writes, not from splitting one.
static PER_FILE_BAKE_CONCURRENCY: LazyLock<Option<usize>> = LazyLock::new(|| {
    resolve_pinned_usize(
        "SPICE_CAYENNE_PER_FILE_BAKE",
        std::env::var("SPICE_CAYENNE_PER_FILE_BAKE").ok().as_deref(),
    )
});

/// Concurrency for the per-file seq-prefix bake, or `None` when it is off.
#[must_use]
pub(crate) fn per_file_bake_concurrency() -> Option<usize> {
    *PER_FILE_BAKE_CONCURRENCY
}

/// Experiment pin for how many of the newest protected snapshots the bake leaves
/// unbaked (`K`, default [`crate::provider::table::BAKE_KEEP_RECENT_SNAPSHOTS`]).
///
/// `K` is a TUNING parameter, not a correctness one: soundness rests on
/// `bake_clean_prefix_holds`, which re-validates the prune against whatever prefix
/// was selected, so lowering `K` can only make that gate decline — never resurrect
/// a row. What `K` buys is a settled cutoff: the newest snapshots are still taking
/// the live delete stream, so baking them rewrites rows about to be superseded.
///
/// Worth measuring because `K` bounds the deletion index's floor. The index retains
/// every tombstone above `T`, and `T` stops at the snapshot older than this tail, so
/// the floor is roughly `tombstone_rate x (K x snapshot_interval + pass_duration)`.
/// For `stock` that measured `3 x 19.3s = 58s` of kept tail against a 128s pass.
///
/// Note what `K` does NOT buy: it is not the write-amplification lever its own
/// comment implies. A pass re-reads the previous pass's consolidated output every
/// time — measured at 69-96% of pass bytes — so excluding the K newest (small)
/// snapshots leaves the dominant cost untouched.
static PINNED_BAKE_KEEP_RECENT: LazyLock<Option<usize>> = LazyLock::new(|| {
    // Accepts 0 (bake every protected snapshot), so this cannot reuse the
    // positive-only pin parser.
    let raw = std::env::var("SPICE_CAYENNE_PIN_BAKE_KEEP_RECENT").ok()?;
    match raw.trim().parse::<usize>() {
        Ok(keep) => {
            tracing::info!(
                "Cayenne is pinning `SPICE_CAYENNE_PIN_BAKE_KEEP_RECENT` to {keep}; the bake will leave {keep} newest protected snapshot(s) unbaked."
            );
            Some(keep)
        }
        Err(_) => {
            tracing::warn!(
                "Ignoring `SPICE_CAYENNE_PIN_BAKE_KEEP_RECENT={raw}`: expected a whole number (0 or above)."
            );
            None
        }
    }
});

/// How many newest protected snapshots the bake leaves unbaked.
#[must_use]
pub(crate) fn bake_keep_recent_snapshots() -> usize {
    PINNED_BAKE_KEEP_RECENT.unwrap_or(crate::provider::table::BAKE_KEEP_RECENT_SNAPSHOTS)
}

/// Shards a maintenance write should use, or `None` to size it from its bytes.
#[must_use]
pub(crate) fn maintenance_encode_shards() -> Option<usize> {
    *MAINTENANCE_ENCODE_SHARDS
}

/// Experiment pins for the two adaptive actuators an A/B on maintenance has to
/// hold still.
///
/// The controller moves `write_concurrency` between 1 and 4 continuously, so a
/// run's value is wherever it happened to be at scrape time. That is fine in
/// production and fatal for a comparison: in a pinned-shard run and its control
/// both arms spent time at both values and one table ended up inverted between
/// them, so the treatment barely differed from the control and the result was
/// uninterpretable. Pinning makes an arm mean what it says.
///
/// Unset leaves the controller in charge, so a run without these is the shipped
/// behaviour.
static PINNED_WRITE_CONCURRENCY: LazyLock<Option<usize>> = LazyLock::new(|| {
    resolve_pinned_usize(
        "SPICE_CAYENNE_PIN_WRITE_CONCURRENCY",
        std::env::var("SPICE_CAYENNE_PIN_WRITE_CONCURRENCY")
            .ok()
            .as_deref(),
    )
});

static PINNED_BAKE_DELETION_INDEX_TRIGGER: LazyLock<Option<usize>> = LazyLock::new(|| {
    resolve_pinned_usize(
        "SPICE_CAYENNE_PIN_BAKE_DELETION_INDEX_TRIGGER",
        std::env::var("SPICE_CAYENNE_PIN_BAKE_DELETION_INDEX_TRIGGER")
            .ok()
            .as_deref(),
    )
});

/// Encode concurrency the controller is not allowed to move, if pinned.
#[must_use]
pub(crate) fn pinned_write_concurrency() -> Option<usize> {
    *PINNED_WRITE_CONCURRENCY
}

/// Tombstone count that triggers a bake, if pinned.
///
/// This is the knob that decides what a bake is worth. A committed bake rewrites
/// the clean sequence prefix, which on the benchmark's large tables is most of the
/// table: `stock` moved 175 GB across four committed bakes against a ~30 GB table,
/// with zero `committed_prune_skipped`, so the cost is inherent to the design
/// rather than a malfunction. A low trigger buys a small index shrink for a
/// near-full rewrite.
#[must_use]
pub(crate) fn pinned_bake_deletion_index_trigger() -> Option<usize> {
    *PINNED_BAKE_DELETION_INDEX_TRIGGER
}

/// Shared pure parser for the pins, so each is testable without mutating the
/// process environment. Anything unusable leaves the controller in charge rather
/// than pinning to a guess.
fn resolve_pinned_usize(var: &str, raw: Option<&str>) -> Option<usize> {
    let raw = raw?;
    match raw.trim().parse::<usize>() {
        Ok(value) if value > 0 => {
            tracing::info!(
                "Cayenne is pinning `{var}` to {value}; the adaptive controller will not move it."
            );
            Some(value)
        }
        _ => {
            tracing::warn!(
                "Ignoring `{var}={raw}`: expected a whole number above zero, so the adaptive controller keeps this actuator."
            );
            None
        }
    }
}

/// Prevent new Cayenne compaction-runtime maintenance passes from starting.
/// Existing pass guards remain counted and can be drained via
/// [`drain_compaction_tasks`].
pub fn begin_compaction_shutdown() {
    COMPACTION_SHUTTING_DOWN.store(true, Ordering::Release);
}

/// Allow Cayenne compaction-runtime maintenance passes to start again.
///
/// Runtime construction calls this so tests or embedded runtimes that create a
/// fresh runtime after shutting down a prior one do not inherit stale global
/// shutdown state. Normal `spiced` startup also resets via
/// [`set_compaction_runtime_handle`].
pub fn reset_compaction_shutdown() {
    COMPACTION_SHUTTING_DOWN.store(false, Ordering::Release);
}

/// Inject the dedicated compaction runtime handle. Called once at process
/// startup. Later calls replace the previous handle so tests that create a new
/// runtime after dropping an old one do not retain stale global state.
pub fn set_compaction_runtime_handle(handle: Handle) {
    reset_compaction_shutdown();
    let mut guard = COMPACTION_RUNTIME.write();
    if guard.is_some() {
        tracing::debug!(
            target: "cayenne::compaction",
            "Replacing compaction runtime handle"
        );
    }
    *guard = Some(handle);
}

/// Spawn a compaction task onto the dedicated compaction runtime if one has
/// been injected, otherwise onto the ambient runtime via [`tokio::spawn`].
///
/// Returns the [`JoinHandle`] so callers can abort the task (e.g. the
/// background compactor aborts on drop). [`JoinHandle::abort`] works across
/// runtimes, so storing and aborting the handle is valid regardless of which
/// runtime the task landed on.
pub(crate) fn spawn_compaction<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let handle = COMPACTION_RUNTIME.read().clone();
    spawn_on(handle.as_ref(), future)
}

/// Wait for active Vortex-producing Cayenne maintenance passes to finish.
///
/// Runtime shutdown uses this before the dedicated compaction Tokio runtime is
/// dropped. That gives in-flight Vortex writes a bounded chance to drain
/// naturally; otherwise Tokio can drop the runtime underneath pending Vortex
/// `Task`s, which can panic in `vortex-io`.
pub async fn drain_compaction_tasks(timeout: Duration) -> bool {
    IN_FLIGHT_COMPACTION_PASSES.drain(timeout).await
}

/// Number of Vortex-producing Cayenne maintenance passes currently in flight.
#[must_use]
pub fn in_flight_compaction_tasks() -> usize {
    IN_FLIGHT_COMPACTION_PASSES.in_flight()
}

/// Spawn `future` on `handle` if provided, otherwise on the ambient runtime via
/// [`tokio::spawn`]. Extracted from [`spawn_compaction`] so the routing decision
/// is unit-testable with a local handle, without setting the process-global
/// [`COMPACTION_RUNTIME`] (which would pollute sibling tests in the binary).
fn spawn_on<F>(handle: Option<&Handle>, future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    match handle {
        Some(handle) => handle.spawn(future),
        None => tokio::spawn(future),
    }
}

/// Process-wide dedicated compaction memory environment, injected once at
/// startup by the binary when Cayenne acceleration is configured (and dedicated
/// thread pools are enabled).
///
/// Carries a [`RuntimeEnv`] whose memory pool is a separate budget carved from
/// `runtime.query.memory_limit` (sized in the runtime's `DataFusion` builder)
/// while sharing the query environment's object-store registry — so compaction
/// reads and writes the same stores but accounts its working memory against an
/// isolated, bounded pool that cannot starve queries.
///
/// Replaceable for the same reason as [`COMPACTION_RUNTIME`]: integration tests
/// can create multiple runtime environments in one process.
///
/// When unset (no Cayenne, dedicated pools disabled, unit tests, other
/// embedders) compaction falls back to the shared query environment via
/// [`super::context::CayenneContext::runtime_env`], preserving prior behavior.
static COMPACTION_RUNTIME_ENV: LazyLock<RwLock<Option<Arc<RuntimeEnv>>>> =
    LazyLock::new(|| RwLock::new(None));

/// Inject the dedicated compaction memory environment. Called once at process
/// startup. Later calls replace the previous environment so tests do not retain
/// stale global state.
pub fn set_compaction_runtime_env(env: Arc<RuntimeEnv>) {
    let mut guard = COMPACTION_RUNTIME_ENV.write();
    if guard.is_some() {
        tracing::debug!(
            target: "cayenne::compaction",
            "Replacing compaction runtime env"
        );
    }
    *guard = Some(env);
}

/// The dedicated compaction memory environment, if one was injected.
pub(crate) fn compaction_runtime_env() -> Option<Arc<RuntimeEnv>> {
    COMPACTION_RUNTIME_ENV.read().clone()
}

/// Tier thresholds derived from `target_vortex_file_size_mb`.
///
/// `small_max_bytes` = `target_vortex_file_size_bytes` / 4 — anything below
///   counts as "small" and is eligible for L0 → L1 compaction.
/// `mid_max_bytes` = `target_vortex_file_size_bytes` — anything below counts as
///   "mid" and is eligible for L1 → L2 compaction.
/// Files at or above `mid_max_bytes` are considered settled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CompactionTiers {
    pub small_max_bytes: u64,
    pub mid_max_bytes: u64,
}

impl CompactionTiers {
    #[must_use]
    pub(crate) fn from_target_file_size_bytes(target_file_size_bytes: u64) -> Self {
        // target / 4 is the small/mid boundary. A misconfigured target of 0
        // still produces deterministic tiers.
        let small_max_bytes = target_file_size_bytes / 4;
        Self {
            small_max_bytes,
            mid_max_bytes: target_file_size_bytes,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Tier {
    Small,
    Mid,
}

impl Tier {
    fn classify(size_bytes: u64, tiers: &CompactionTiers) -> Option<Self> {
        if size_bytes < tiers.small_max_bytes {
            Some(Self::Small)
        } else if size_bytes < tiers.mid_max_bytes {
            Some(Self::Mid)
        } else {
            // Settled — not a compaction candidate.
            None
        }
    }

    #[must_use]
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Small => "small",
            Self::Mid => "mid",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CompactionPickerConfig {
    /// Minimum number of files in a tier required to consider compaction.
    pub trigger_files: usize,
    /// Maximum number of file paths retained in the candidate for tracing and
    /// selection. When the candidate is a proper subset of the current snapshot
    /// (key-delete, no protected snapshots, no sort columns),
    /// `compact_current_snapshot_small_files` rewrites only those paths and
    /// hard-links the rest; otherwise the runner falls back to a full-snapshot
    /// rewrite.
    pub max_files_per_pick: usize,
    /// Tier thresholds derived from `target_vortex_file_size_mb`.
    pub tiers: CompactionTiers,
}

impl CompactionPickerConfig {
    /// Convenience constructor matching the config fields surfaced on
    /// `VortexConfig`.
    #[must_use]
    pub(crate) fn new(
        trigger_files: usize,
        max_files_per_pick: usize,
        target_file_size_bytes: u64,
    ) -> Self {
        Self {
            trigger_files: trigger_files.max(2),
            max_files_per_pick: max_files_per_pick.max(2),
            tiers: CompactionTiers::from_target_file_size_bytes(target_file_size_bytes),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct FileEntry<P> {
    pub path: P,
    pub size_bytes: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct CompactionCandidate<P> {
    pub tier: Tier,
    pub paths: Vec<P>,
    pub total_bytes: u64,
}

/// Pick a compaction candidate from a list of files and their sizes.
///
/// Pure function — no IO. Algorithm:
/// 1. Bucket files into `Small` and `Mid` tiers (anything at/above
///    `mid_max_bytes` is settled).
/// 2. For each tier in order Small → Mid:
///    - if `count >= trigger_files` AND tier bytes reach that tier's threshold,
///      sort ascending by size, take the first `max_files_per_pick`, return
///      them as the candidate.
/// 3. Otherwise return `None`.
///
/// Picking the smallest files first keeps the candidate focused on the tier
/// with the most file-count pressure; the current runner still performs a
/// whole-snapshot rewrite after the candidate is selected.
#[must_use]
pub(crate) fn pick_candidates<P: Clone>(
    files: impl IntoIterator<Item = FileEntry<P>>,
    cfg: &CompactionPickerConfig,
) -> Option<CompactionCandidate<P>> {
    let files = files.into_iter();
    // Reserve based on the iterator's size_hint so the buckets do not
    // re-allocate as they grow when the caller knows the total file count.
    let hint = files.size_hint().0;
    let mut small = Vec::with_capacity(hint);
    let mut mid = Vec::with_capacity(hint);

    for entry in files {
        match Tier::classify(entry.size_bytes, &cfg.tiers) {
            Some(Tier::Small) => small.push(entry),
            Some(Tier::Mid) => mid.push(entry),
            None => {}
        }
    }

    pick_from_bucket(Tier::Small, &mut small, cfg)
        .or_else(|| pick_from_bucket(Tier::Mid, &mut mid, cfg))
}

fn pick_from_bucket<P: Clone>(
    tier: Tier,
    bucket: &mut [FileEntry<P>],
    cfg: &CompactionPickerConfig,
) -> Option<CompactionCandidate<P>> {
    if bucket.len() < cfg.trigger_files {
        return None;
    }

    // Threshold check uses the WHOLE tier's bytes.
    //
    // For the Small tier the primary goal (as documented) is to relieve
    // *file-count* pressure (many tiny objects hurt LIST performance, scan
    // overhead, and S3 costs). We therefore trigger on count (`>= trigger_files`)
    // as long as the tier has accumulated at least one "full small file" worth
    // of data (`>= small_max_bytes`). This is much more responsive to file
    // count than requiring `small_max * trigger_files` total bytes.
    //
    // For Mid we keep the higher `mid_max_bytes` threshold because those
    // files are already closer to the target size and the goal is more about
    // reaching good file sizes.
    let tier_total_bytes: u64 = bucket.iter().map(|entry| entry.size_bytes).sum();
    let byte_threshold = match tier {
        Tier::Small => cfg.tiers.small_max_bytes,
        Tier::Mid => cfg.tiers.mid_max_bytes,
    };
    if tier_total_bytes < byte_threshold {
        return None;
    }

    // We only need the K smallest entries — `select_nth_unstable_by_key` runs
    // in O(N) expected time vs O(N log N) for a full sort, and the candidate
    // downstream consumers (path collection + byte sum) don't depend on
    // ordering within the picked set. See `compaction_picker` bench.
    let max_pick = cfg.max_files_per_pick.min(bucket.len());
    if max_pick < bucket.len() {
        let _ = bucket.select_nth_unstable_by_key(max_pick, |entry| entry.size_bytes);
    }
    let picked = &bucket[..max_pick];
    let picked_bytes: u64 = picked.iter().map(|entry| entry.size_bytes).sum();
    let paths = picked.iter().map(|entry| entry.path.clone()).collect();
    Some(CompactionCandidate {
        tier,
        paths,
        total_bytes: picked_bytes,
    })
}

/// Trait the background compactor uses to invoke a per-table compaction pass.
///
/// Implemented by `CayenneTableProvider`. Decouples the scheduler from the
/// provider so we can unit-test the scheduler with a stub.
#[async_trait::async_trait]
pub(crate) trait CompactionRunner: Send + Sync {
    /// Run one compaction trigger. Returns `Ok(true)` if any compaction
    /// occurred. Errors are reported via the return value; the scheduler logs
    /// and continues on Err.
    async fn run_compaction_trigger(&self) -> Result<bool, String>;

    /// Identifier used in log messages.
    fn compaction_target_name(&self) -> &str;

    /// Called once per background wake, before draining the compaction backlog.
    /// A hook for per-tick maintenance — in particular the dynamic auto-tuning
    /// control step (sample the environment + ingest/query response, apply at
    /// most one bounded knob change) and its metric emission. Default no-op so
    /// other [`CompactionRunner`] impls (e.g. test stubs) need not implement it.
    fn on_background_tick(&self) {}

    /// The (possibly dynamically-tuned) background interval to use for the NEXT
    /// wake. `None` keeps the spawn-time interval. Lets the auto-tuner widen or
    /// tighten the compaction cadence at runtime. Default `None`.
    fn background_interval_hint(&self) -> Option<Duration> {
        None
    }
}

/// Maximum protected-snapshot merge passes a single table runs per wake-up
/// before yielding back to the interval cadence. Each pass merges one size-tier,
/// so under backlog this drains up to `MAX_DRAIN_PASSES_PER_WAKE` tiers per tick
/// (vs. exactly one before), while still bounding how long one table can hold
/// the compaction runtime away from its peers. Passes stop early as soon as a
/// table is caught up (`run_compaction_trigger` returns `Ok(false)`).
const MAX_DRAIN_PASSES_PER_WAKE: usize = 64;

/// Per-table background compactor.
///
/// Owns a tokio task that wakes every `interval`, then drains its
/// protected-snapshot backlog by running up to [`MAX_DRAIN_PASSES_PER_WAKE`]
/// merge passes — each acquiring a permit from the shared semaphore and calling
/// `runner.run_compaction_trigger()` — until it is caught up. Cancellation
/// happens via [`Drop`]: dropping the `BackgroundCompactor` fires the shutdown
/// `Notify`, then moves bounded task draining to a detached OS thread so dropping
/// the provider never blocks a Tokio worker thread.
///
/// The runner is held via `Weak` so the task does not keep the
/// `CayenneTableProvider` alive past its caller's `Arc` lifetime.
pub(crate) struct BackgroundCompactor {
    handle: Option<tokio::task::JoinHandle<()>>,
    shutdown: Arc<Notify>,
}

impl BackgroundCompactor {
    /// Spawn a background compaction task. Returns `None` if `interval` is
    /// zero, indicating the task is disabled.
    pub(crate) fn spawn(
        runner: Weak<dyn CompactionRunner>,
        interval: Duration,
        semaphore: Arc<Semaphore>,
    ) -> Option<Self> {
        if interval.is_zero() {
            return None;
        }

        let shutdown = Arc::new(Notify::new());
        let shutdown_task = Arc::clone(&shutdown);

        // Spawn onto the dedicated compaction runtime (low priority, isolated
        // from the query and refresh runtimes) when one has been injected;
        // otherwise fall back to the ambient runtime.
        let handle = spawn_compaction(async move {
            // The interval is re-read from the runner each wake so the dynamic
            // auto-tuner can widen/tighten the compaction cadence at runtime
            // (defaults to the spawn-time interval when no hint is given).
            let mut current = interval;
            'wake: loop {
                tokio::select! {
                    () = tokio::time::sleep(current) => {}
                    () = shutdown_task.notified() => break,
                }

                let Some(runner) = runner.upgrade() else {
                    // Provider dropped — task exits naturally.
                    break;
                };

                // Per-wake hook: the dynamic auto-tuning control step (+metrics).
                // Runs before draining and before re-reading the interval so a
                // just-applied cadence change takes effect on the next sleep.
                runner.on_background_tick();
                if let Some(next) = runner.background_interval_hint() {
                    current = next;
                }

                // Drain the protected-snapshot backlog instead of doing a single
                // tier-merge per tick. Each `run_compaction_trigger` merges only
                // the lowest size-tier, so one pass per `interval` tops out at
                // ~one tier every few seconds — far below the ingest rate at high
                // tpmC, letting the protected set run away (5k+ files at SF-1000)
                // and read-amp balloon. Keep running passes until one reports
                // nothing left to merge (`Ok(false)`) or we hit the per-wake cap,
                // re-acquiring the shared permit each pass so peer tables still
                // interleave fairly between merges.
                for _ in 0..MAX_DRAIN_PASSES_PER_WAKE {
                    // Acquire a permit, gating concurrent background compactions
                    // across all tables sharing the semaphore. Observe shutdown
                    // *during* acquisition so a drop fired mid-drain stops the loop
                    // promptly instead of running up to `MAX_DRAIN_PASSES_PER_WAKE`
                    // more passes (each a multi-second full-snapshot rewrite at
                    // scale) before the next outer tick notices. This only gates the
                    // gap *between* passes: an already in-flight
                    // `run_compaction_trigger` below is intentionally never
                    // interrupted, so a pass still drains to completion on drop (see
                    // `COMPACTOR_SHUTDOWN_DRAIN` and the drain-in-flight test).
                    let acquire_start = Instant::now();
                    let _permit = tokio::select! {
                        biased;
                        () = shutdown_task.notified() => break 'wake,
                        acquired = Arc::clone(&semaphore).acquire_owned() => match acquired {
                            Ok(permit) => permit,
                            // Semaphore closed — provider tree shutting down.
                            Err(_) => break 'wake,
                        },
                    };
                    // Attribute the wait for a compaction slot: a high value means
                    // peer tables saturate the fleet-wide semaphore, starving this
                    // table's compaction (protected set / read-amp run away).
                    telemetry::cayenne::track_compaction_acquire_wait(
                        acquire_start.elapsed(),
                        &[telemetry::KeyValue::new(
                            "table",
                            runner.compaction_target_name().to_string(),
                        )],
                    );

                    match runner.run_compaction_trigger().await {
                        Ok(true) => {
                            tracing::debug!(
                                target: "cayenne::compaction",
                                table = runner.compaction_target_name(),
                                "Background compaction pass completed"
                            );
                            // Made progress — keep draining. The permit is
                            // released at the end of this iteration so peers can
                            // interleave before the next pass re-acquires it.
                        }
                        // Caught up (no tier qualifies) — stop draining and wait
                        // for the next tick rather than spinning on empty passes.
                        Ok(false) => break,
                        Err(e) => {
                            tracing::warn!(
                                target: "cayenne::compaction",
                                table = runner.compaction_target_name(),
                                "Background compaction failed: {e}"
                            );
                            break;
                        }
                    }
                }
            }
        });

        Some(Self {
            handle: Some(handle),
            shutdown,
        })
    }
}

/// How long the detached drain thread lets an in-flight compaction finish its
/// current Vortex write before force-aborting. Bounded so shutdown can never hang.
///
/// Sized to outlast the large seq-prefix/subset passes seen during SF100
/// CH-benCH: individual passes can legitimately run for 60-90s while the
/// benchmark is still applying CDC. Aborting those writes mid-flight can leave
/// Vortex layout tasks awaiting CPU jobs that Tokio has cancelled, which panics
/// in `vortex-io` ("Runtime dropped task without completing it").
///
/// This is still a mitigation: a pass that exceeds the window may be aborted.
/// The durable performance fix is to keep each pass shorter (incremental merge
/// width / bake sizing) so shutdown and provider replacement rarely need this
/// backstop.
const COMPACTOR_SHUTDOWN_DRAIN: Duration = Duration::from_mins(2);

fn drain_and_abort_compactor(handle: &JoinHandle<()>) {
    // Let an in-flight compaction finish its current write before the
    // surrounding runtime tears down. vortex-io panics ("Runtime dropped task
    // without completing it") if a task's runtime is dropped while the task is
    // still pending, and the sharded encode keeps several such IO tasks in
    // flight per compaction — so force-aborting mid-write races the runtime
    // shutdown and panics. Poll `is_finished()` with plain sleeps so this never
    // depends on the (possibly already-shutting-down) runtime timer and cannot
    // hang past the deadline.
    let deadline = Instant::now() + COMPACTOR_SHUTDOWN_DRAIN;
    while !handle.is_finished() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    // Whether it drained or hit the deadline, ensure the task is gone.
    handle.abort();
}

fn spawn_compactor_drain_thread(handle: JoinHandle<()>) {
    let handle = Arc::new(Mutex::new(Some(handle)));
    let handle_for_thread = Arc::clone(&handle);

    match std::thread::Builder::new()
        .name("cayenne-compactor-drain".to_string())
        .spawn(move || {
            let Some(handle) = handle_for_thread.lock().take() else {
                return;
            };
            drain_and_abort_compactor(&handle);
        }) {
        Ok(join_handle) => drop(join_handle),
        Err(error) => {
            if let Some(handle) = handle.lock().take() {
                handle.abort();
            }
            tracing::warn!(target: "cayenne::compaction", "Failed to spawn background compactor drain thread; aborted task immediately: {error}");
        }
    }
}

// Cleanup starts in `Drop`: the shutdown signal is fired, then the current pass
// is given a bounded window to drain on a detached thread before the
// `JoinHandle` is aborted. Callers don't need explicit `shutdown` / `join`
// methods — when the provider's last `Arc` drops, the
// `OnceLock<BackgroundCompactor>` inside drops too, which runs the impl below.

impl Drop for BackgroundCompactor {
    fn drop(&mut self) {
        // Signal the loop to stop after its current pass.
        self.shutdown.notify_one();
        let Some(handle) = self.handle.take() else {
            return;
        };
        spawn_compactor_drain_thread(handle);
    }
}

/// Trait the background mem-tier checkpointer uses to flush a memory-mode
/// table's RAM tier on a periodic tick.
///
/// Implemented by `CayenneTableProvider`. Decouples the scheduler from the
/// provider (parallel to [`CompactionRunner`]) so the scheduler is unit-testable
/// with a stub, and keeps the runtime's slot-advancer concern out of this module
/// — the provider's tick takes the per-table checkpoint lock and calls the
/// existing `checkpoint_mem_tier`, which fires the slot advancer post-fence.
#[async_trait::async_trait]
pub(crate) trait MemTierCheckpointRunner: Send + Sync {
    /// Run one periodic mem-tier checkpoint. A no-op when the table is not in
    /// memory mode, is unarmed, or its tier is empty. Errors are logged by the
    /// implementation (a failed checkpoint must NOT advance the slot — the
    /// deferred committers stay queued and the next tick retries).
    async fn run_mem_tier_checkpoint_tick(&self);

    /// Identifier used in log messages.
    fn mem_tier_checkpoint_target_name(&self) -> &str;

    /// The (possibly re-read) interval to use for the NEXT wake. `None` keeps the
    /// spawn-time interval. Mirrors [`CompactionRunner::background_interval_hint`]
    /// so a future auto-tuner can widen/tighten the cadence at runtime.
    fn checkpoint_interval_hint(&self) -> Option<Duration> {
        None
    }
}

/// Per-table background mem-tier checkpointer (`cdc_durability: memory`).
///
/// Owns a tokio task that wakes every `interval` and runs ONE checkpoint tick
/// (`run_mem_tier_checkpoint_tick`), which flushes the RAM tier to a durable
/// Vortex file and advances the deferred source slot ack. Modeled on
/// [`BackgroundCompactor`]: a `Weak` runner so the task never pins the provider,
/// a `select!` over `sleep(interval)` vs a shutdown `Notify`, the interval
/// re-read each wake, and `Drop`-fires-shutdown + a bounded detached-thread drain
/// so dropping the provider never blocks a Tokio worker.
///
/// Unlike the compactor there is NO shared semaphore and NO multi-pass drain
/// loop: a single `checkpoint_mem_tier` flushes the entire tier in one call, and
/// the per-table `mem_checkpoint_lock` (taken inside the tick) is the only
/// serialization needed — it already excludes the write-path spill and the
/// event-driven checkpoints, so two checkpoints for one table can never overlap.
pub(crate) struct BackgroundMemTierCheckpointer {
    handle: Option<tokio::task::JoinHandle<()>>,
    shutdown: Arc<Notify>,
}

impl BackgroundMemTierCheckpointer {
    /// Spawn the periodic checkpoint task. Returns `None` if `interval` is zero
    /// (the task is disabled — the write-path caps still bound hot tables).
    pub(crate) fn spawn(
        runner: Weak<dyn MemTierCheckpointRunner>,
        interval: Duration,
    ) -> Option<Self> {
        if interval.is_zero() {
            return None;
        }

        let shutdown = Arc::new(Notify::new());
        let shutdown_task = Arc::clone(&shutdown);

        // Spawn onto the dedicated compaction runtime (shared low-priority
        // background runtime) when one is injected, otherwise the ambient
        // runtime — same routing as the compactor so background work stays off
        // the query/refresh runtimes.
        let handle = spawn_compaction(async move {
            // Re-read the interval each wake so a future auto-tuner can adjust the
            // cadence (defaults to the spawn-time interval when no hint is given).
            let mut current = interval;
            loop {
                tokio::select! {
                    () = tokio::time::sleep(current) => {}
                    () = shutdown_task.notified() => break,
                }

                let Some(runner) = runner.upgrade() else {
                    // Provider dropped — task exits naturally.
                    break;
                };

                if let Some(next) = runner.checkpoint_interval_hint() {
                    current = next;
                }

                tracing::trace!(
                    target: "cayenne::mem_tier",
                    table = runner.mem_tier_checkpoint_target_name(),
                    "Periodic mem-tier checkpoint wake",
                );

                // One checkpoint per tick. The tick itself is a no-op on an empty
                // or unarmed tier and takes the per-table lock only when there is
                // something to flush, so an idle table costs one cheap wake.
                let Some(_pass) = try_track_compaction_pass() else {
                    break;
                };
                runner.run_mem_tier_checkpoint_tick().await;
            }
        });

        Some(Self {
            handle: Some(handle),
            shutdown,
        })
    }
}

fn drain_and_abort_checkpointer(handle: &JoinHandle<()>) {
    // Let an in-flight checkpoint finish its current Vortex write before the
    // surrounding runtime tears down, for the same vortex-io reason as the
    // compactor drain (a task whose runtime is dropped mid-write panics).
    let deadline = Instant::now() + COMPACTOR_SHUTDOWN_DRAIN;
    while !handle.is_finished() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    handle.abort();
}

fn spawn_checkpointer_drain_thread(handle: JoinHandle<()>) {
    let handle = Arc::new(Mutex::new(Some(handle)));
    let handle_for_thread = Arc::clone(&handle);

    match std::thread::Builder::new()
        .name("cayenne-memtier-checkpoint-drain".to_string())
        .spawn(move || {
            let Some(handle) = handle_for_thread.lock().take() else {
                return;
            };
            drain_and_abort_checkpointer(&handle);
        }) {
        Ok(join_handle) => drop(join_handle),
        Err(error) => {
            if let Some(handle) = handle.lock().take() {
                handle.abort();
            }
            tracing::warn!(target: "cayenne::mem_tier", "Failed to spawn background mem-tier checkpointer drain thread; aborted task immediately: {error}");
        }
    }
}

impl Drop for BackgroundMemTierCheckpointer {
    fn drop(&mut self) {
        self.shutdown.notify_one();
        let Some(handle) = self.handle.take() else {
            return;
        };
        spawn_checkpointer_drain_thread(handle);
    }
}

/// Periodic driver for the cold-tier promotion worker (storage-cascade bottom
/// tier). Implemented by `CayenneTableProvider`; mirrors [`MemTierCheckpointRunner`]
/// so the scheduler is decoupled from the provider and unit-testable with a stub.
#[async_trait::async_trait]
pub(crate) trait ColdTierPromotionRunner: Send + Sync {
    /// Run one promotion tick. A no-op when the cold tier is disabled or the warm
    /// tier has not crossed its promotion threshold. Errors are logged by the
    /// implementation (a failed promotion leaves the warm tier intact and the
    /// next tick retries).
    async fn run_cold_tier_promotion_tick(&self);

    /// Identifier used in log messages.
    fn cold_tier_promotion_target_name(&self) -> &str;

    /// The (possibly re-read) interval for the NEXT wake. `None` keeps the
    /// spawn-time interval.
    fn cold_tier_promotion_interval_hint(&self) -> Option<Duration> {
        None
    }
}

/// Per-table background cold-tier promoter (storage-cascade bottom tier).
///
/// Owns a tokio task that wakes every `interval` and runs ONE promotion tick
/// (`run_cold_tier_promotion_tick`), which graduates the warm tier to the cold
/// object store when its size/file thresholds are crossed. Modeled exactly on
/// [`BackgroundMemTierCheckpointer`]: a `Weak` runner so the task never pins the
/// provider, a `select!` over `sleep(interval)` vs a shutdown `Notify`, the
/// interval re-read each wake, and `Drop`-fires-shutdown + a bounded detached
/// drain. Runs on the shared low-priority compaction runtime via
/// [`spawn_compaction`], on its OWN cadence — so the heavy whole-table
/// graduation never blocks the compaction tick or the query/refresh runtimes.
pub(crate) struct BackgroundColdTierPromoter {
    handle: Option<tokio::task::JoinHandle<()>>,
    shutdown: Arc<Notify>,
}

impl BackgroundColdTierPromoter {
    /// Spawn the periodic promotion task. Returns `None` if `interval` is zero
    /// (the task is disabled).
    pub(crate) fn spawn(
        runner: Weak<dyn ColdTierPromotionRunner>,
        interval: Duration,
    ) -> Option<Self> {
        if interval.is_zero() {
            return None;
        }

        let shutdown = Arc::new(Notify::new());
        let shutdown_task = Arc::clone(&shutdown);

        let handle = spawn_compaction(async move {
            let mut current = interval;
            loop {
                tokio::select! {
                    () = tokio::time::sleep(current) => {}
                    () = shutdown_task.notified() => break,
                }

                let Some(runner) = runner.upgrade() else {
                    break;
                };

                if let Some(next) = runner.cold_tier_promotion_interval_hint() {
                    current = next;
                }

                tracing::trace!(
                    target: "cayenne::compaction",
                    table = runner.cold_tier_promotion_target_name(),
                    "Datalake background tiering check: wake",
                );

                let Some(_pass) = try_track_compaction_pass() else {
                    break;
                };
                runner.run_cold_tier_promotion_tick().await;
            }
        });

        Some(Self {
            handle: Some(handle),
            shutdown,
        })
    }
}

impl Drop for BackgroundColdTierPromoter {
    fn drop(&mut self) {
        self.shutdown.notify_one();
        let Some(handle) = self.handle.take() else {
            return;
        };
        spawn_checkpointer_drain_thread(handle);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entries(sizes: &[u64]) -> Vec<FileEntry<String>> {
        sizes
            .iter()
            .enumerate()
            .map(|(idx, &size)| FileEntry {
                path: format!("file_{idx:04}.vortex"),
                size_bytes: size,
            })
            .collect()
    }

    /// Helper: target file size of 256 MiB, matching the default.
    fn default_cfg() -> CompactionPickerConfig {
        CompactionPickerConfig::new(8, 32, 256 * 1024 * 1024)
    }

    /// `spawn_on(Some(handle))` runs the task on the provided runtime's worker
    /// thread — the dedicated-compaction-runtime path. Asserted via the worker
    /// thread name rather than touching the process-global `COMPACTION_RUNTIME`.
    #[test]
    fn spawn_on_uses_provided_handle() {
        let dedicated = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("test-compaction-rt")
            .enable_all()
            .build()
            .expect("build dedicated runtime");
        let handle = dedicated.handle().clone();

        let thread_name = dedicated.block_on(async move {
            spawn_on(Some(&handle), async {
                std::thread::current().name().map(String::from)
            })
            .await
            .expect("spawned task completes")
        });

        assert_eq!(
            thread_name.as_deref(),
            Some("test-compaction-rt"),
            "task should run on the provided runtime's worker thread"
        );
    }

    /// `spawn_on(None)` falls back to the ambient runtime via `tokio::spawn`,
    /// preserving prior behavior when no dedicated compaction runtime is set.
    #[tokio::test(flavor = "multi_thread")]
    async fn spawn_on_falls_back_to_ambient_runtime() {
        let value = spawn_on(None, async { 7_u8 })
            .await
            .expect("spawned task completes");
        assert_eq!(value, 7, "fallback path should run and return the value");
    }

    #[test]
    fn tiers_derived_from_target_size() {
        let tiers = CompactionTiers::from_target_file_size_bytes(128 * 1024 * 1024);
        assert_eq!(tiers.small_max_bytes, 32 * 1024 * 1024);
        assert_eq!(tiers.mid_max_bytes, 128 * 1024 * 1024);
    }

    #[test]
    fn tier_classify_assigns_correct_buckets() {
        let tiers = CompactionTiers::from_target_file_size_bytes(128 * 1024 * 1024);
        assert_eq!(Tier::classify(1, &tiers), Some(Tier::Small));
        assert_eq!(
            Tier::classify(32 * 1024 * 1024 - 1, &tiers),
            Some(Tier::Small)
        );
        assert_eq!(Tier::classify(32 * 1024 * 1024, &tiers), Some(Tier::Mid));
        assert_eq!(
            Tier::classify(128 * 1024 * 1024 - 1, &tiers),
            Some(Tier::Mid)
        );
        assert_eq!(Tier::classify(128 * 1024 * 1024, &tiers), None);
        assert_eq!(Tier::classify(u64::MAX, &tiers), None);
    }

    #[test]
    fn picker_handles_empty_input() {
        let cfg = default_cfg();
        assert!(pick_candidates(std::iter::empty::<FileEntry<String>>(), &cfg).is_none());
    }

    #[test]
    fn picker_returns_none_when_below_trigger_count() {
        let cfg = default_cfg();
        // 7 small files of 5 MiB each — below trigger_files = 8.
        let files = entries(&[5 * 1024 * 1024; 7]);
        assert!(pick_candidates(files.iter().cloned(), &cfg).is_none());
    }

    #[test]
    fn picker_returns_none_when_total_bytes_below_target() {
        let cfg = default_cfg();
        // 8 small files of 1 MiB each — meets trigger_files but total = 8 MiB,
        // well below the 64 MiB Small-tier byte threshold (target_size / 4).
        let files = entries(&[1024 * 1024; 8]);
        assert!(pick_candidates(files.iter().cloned(), &cfg).is_none());
    }

    #[test]
    fn picker_picks_small_tier_first() {
        let cfg = default_cfg();
        // 8 small (16 MiB) + 8 mid (64 MiB). Both tiers are eligible (total
        // 128 MiB and 512 MiB respectively). The picker should choose Small
        // first.
        let mut sizes = vec![16 * 1024 * 1024; 8];
        sizes.extend(vec![64 * 1024 * 1024; 8]);
        let files = entries(&sizes);
        let candidate = pick_candidates(files.iter().cloned(), &cfg).expect("expected a candidate");
        assert_eq!(candidate.tier, Tier::Small);
        assert_eq!(candidate.paths.len(), 8);
        assert_eq!(candidate.total_bytes, 8 * 16 * 1024 * 1024);
    }

    #[test]
    fn picker_caps_at_max_files_per_pick() {
        // Target = 64 MiB → mid_max = 64 MiB, small_max = 16 MiB.
        // 10 small files of 10 MiB each. The whole Small tier totals 100 MiB,
        // which is above the 16 MiB Small-tier threshold, so the picker has
        // work and then caps the retained candidate paths at max_files_per_pick.
        let cfg = CompactionPickerConfig::new(2, 8, 64 * 1024 * 1024);
        let files = entries(&[10 * 1024 * 1024; 10]);
        let candidate = pick_candidates(files.iter().cloned(), &cfg).expect("expected a candidate");
        assert_eq!(
            candidate.paths.len(),
            8,
            "picker should grab exactly max_files_per_pick files"
        );
        assert_eq!(candidate.total_bytes, 8 * 10 * 1024 * 1024);
    }

    #[test]
    fn picker_returns_none_when_only_one_file_above_target() {
        let cfg = default_cfg();
        let files = entries(&[512 * 1024 * 1024]);
        assert!(pick_candidates(files.iter().cloned(), &cfg).is_none());
    }

    #[test]
    fn picker_picks_smallest_files_first_within_tier() {
        // Cap max_files_per_pick = 8 so the picker MUST choose, and pick
        // sizes that make the smallest 8 exceed mid_max — otherwise the picker
        // correctly skips. Target = 128 MiB → small_max = 32 MiB.
        // Sizes 17..28 MiB are all in Small (< 32 MiB); smallest 8 sum to
        // 17+18+19+20+21+22+23+24 = 164 MiB > 128.
        let cfg = CompactionPickerConfig::new(8, 8, 128 * 1024 * 1024);
        let sizes_mib: [u64; 12] = [25, 17, 27, 19, 28, 21, 23, 18, 26, 20, 22, 24];
        let sizes: Vec<u64> = sizes_mib.iter().map(|m| m * 1024 * 1024).collect();
        let files = entries(&sizes);
        let candidate = pick_candidates(files.iter().cloned(), &cfg).expect("expected a candidate");
        assert_eq!(candidate.tier, Tier::Small);
        assert_eq!(candidate.paths.len(), 8);

        // The 8 smallest by size: 17..24 (MiB).
        let expected_bytes: u64 = (17_u64..=24).map(|mb| mb * 1024 * 1024).sum();
        assert_eq!(candidate.total_bytes, expected_bytes);
    }

    #[test]
    fn picker_promotes_to_mid_tier_when_small_tier_drained() {
        let cfg = default_cfg();
        // Simulate post-merge state: small tier is empty, mid tier has 8 files
        // totaling > 256 MiB.
        let files = entries(&[64 * 1024 * 1024; 8]);
        let candidate = pick_candidates(files.iter().cloned(), &cfg).expect("expected a candidate");
        assert_eq!(candidate.tier, Tier::Mid);
    }

    #[test]
    fn picker_skips_settled_files() {
        let cfg = default_cfg();
        // All files at exactly target size — none are candidates.
        let files = entries(&[256 * 1024 * 1024; 16]);
        assert!(pick_candidates(files.iter().cloned(), &cfg).is_none());
    }

    #[test]
    fn picker_threshold_uses_tier_total_not_picked_subset() {
        // Regression: 100 files of 2 MiB each (200 MiB tier total) used to be
        // skipped because the smallest 32 only sum to 64 MiB. The eligibility
        // check should consider the whole tier's bytes, not just the picked
        // subset — otherwise tiny-but-numerous files would never trigger
        // compaction.
        let cfg = CompactionPickerConfig::new(8, 32, 128 * 1024 * 1024);
        let files = entries(&[2 * 1024 * 1024; 100]);
        let candidate = pick_candidates(files.iter().cloned(), &cfg)
            .expect("expected a candidate from 100 small files");
        assert_eq!(candidate.tier, Tier::Small);
        assert_eq!(candidate.paths.len(), 32);
        // `total_bytes` on the candidate reports the picked subset, not the
        // whole tier — 32 * 2 MiB.
        assert_eq!(candidate.total_bytes, 32 * 2 * 1024 * 1024);
    }

    #[test]
    fn picker_config_enforces_minimum_trigger_files() {
        // trigger_files=0 should be clamped to 2 (a single file can't be
        // compacted).
        let cfg = CompactionPickerConfig::new(0, 32, 128 * 1024 * 1024);
        assert!(cfg.trigger_files >= 2);
    }

    #[test]
    fn picker_config_enforces_minimum_max_files_per_pick() {
        // max_files_per_pick=0 should be clamped to 2 as well.
        let cfg = CompactionPickerConfig::new(8, 0, 128 * 1024 * 1024);
        assert!(cfg.max_files_per_pick >= 2);
    }

    // ------------------------------------------------------------------
    // Active compaction pass tracker tests
    // ------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn compaction_pass_tracker_drains_when_guard_drops() {
        let tracker = Arc::new(CompactionPassTracker::default());
        let guard = tracker.start();
        assert_eq!(tracker.in_flight(), 1);

        let drain_tracker = Arc::clone(&tracker);
        let drain = tokio::spawn(async move { drain_tracker.drain(Duration::from_secs(5)).await });

        // Give the drain task a chance to observe the active pass and register
        // for notification, then finish the pass.
        tokio::task::yield_now().await;
        drop(guard);

        let drained = tokio::time::timeout(Duration::from_secs(1), drain)
            .await
            .expect("drain should not wait for its full timeout")
            .expect("drain task should not panic");
        assert!(drained);
        assert_eq!(tracker.in_flight(), 0);
    }

    // ------------------------------------------------------------------
    // BackgroundCompactor smoke tests
    // ------------------------------------------------------------------

    struct CountingRunner {
        name: String,
        calls: Arc<std::sync::atomic::AtomicU32>,
    }

    #[async_trait::async_trait]
    impl CompactionRunner for CountingRunner {
        async fn run_compaction_trigger(&self) -> Result<bool, String> {
            self.calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(false)
        }

        fn compaction_target_name(&self) -> &str {
            &self.name
        }
    }

    #[tokio::test(start_paused = true)]
    async fn background_compactor_ticks_at_interval_and_stops_on_shutdown() {
        let calls = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let runner = Arc::new(CountingRunner {
            name: "test_table".to_string(),
            calls: Arc::clone(&calls),
        });

        let weak: Weak<dyn CompactionRunner> =
            Arc::downgrade(&runner) as Weak<dyn CompactionRunner>;
        let semaphore = Arc::new(Semaphore::new(1));
        let compactor = BackgroundCompactor::spawn(weak, Duration::from_secs(1), semaphore)
            .expect("scheduler should spawn with non-zero interval");

        // Advance a few intervals.
        for _ in 0..3 {
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;
        }

        // Dropping the compactor signals shutdown and aborts the task.
        drop(compactor);

        let observed = calls.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            (1..=5).contains(&observed),
            "expected background task to fire between 1 and 5 times, got {observed}"
        );
    }

    /// Runner whose compaction takes a beat and records when it *finishes*.
    struct DrainRunner {
        name: String,
        started: Arc<std::sync::atomic::AtomicU32>,
        completed: Arc<std::sync::atomic::AtomicBool>,
    }

    #[async_trait::async_trait]
    impl CompactionRunner for DrainRunner {
        async fn run_compaction_trigger(&self) -> Result<bool, String> {
            self.started
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(200)).await;
            self.completed
                .store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(true)
        }

        fn compaction_target_name(&self) -> &str {
            &self.name
        }
    }

    /// Dropping the compactor while a compaction is in flight must let it finish
    /// (drain) rather than abort it mid-write, while `Drop` itself returns
    /// promptly from the caller's thread.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn background_compactor_drains_in_flight_compaction_on_drop() {
        use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

        let started = Arc::new(AtomicU32::new(0));
        let completed = Arc::new(AtomicBool::new(false));
        let runner = Arc::new(DrainRunner {
            name: "drain_test".to_string(),
            started: Arc::clone(&started),
            completed: Arc::clone(&completed),
        });
        let weak: Weak<dyn CompactionRunner> =
            Arc::downgrade(&runner) as Weak<dyn CompactionRunner>;
        let semaphore = Arc::new(Semaphore::new(1));
        let compactor = BackgroundCompactor::spawn(weak, Duration::from_millis(10), semaphore)
            .expect("scheduler should spawn with non-zero interval");

        // Wait until a compaction is actually in flight (started, not finished).
        for _ in 0..400 {
            if started.load(Ordering::SeqCst) > 0 && !completed.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            started.load(Ordering::SeqCst) > 0 && !completed.load(Ordering::SeqCst),
            "a compaction should be in flight before we drop the compactor"
        );

        drop(compactor);

        for _ in 0..400 {
            if completed.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        assert!(
            completed.load(Ordering::SeqCst),
            "Drop must drain the in-flight compaction to completion, not abort it mid-write"
        );
    }

    #[test]
    fn background_compactor_returns_none_when_interval_is_zero() {
        let runner = Arc::new(CountingRunner {
            name: "test_table".to_string(),
            calls: Arc::new(std::sync::atomic::AtomicU32::new(0)),
        });
        let weak: Weak<dyn CompactionRunner> =
            Arc::downgrade(&runner) as Weak<dyn CompactionRunner>;
        let semaphore = Arc::new(Semaphore::new(1));
        assert!(BackgroundCompactor::spawn(weak, Duration::ZERO, semaphore).is_none());
    }

    #[test]
    fn actuator_pins_are_opt_in_and_reject_nonsense() {
        assert_eq!(resolve_pinned_usize("V", None), None);
        assert_eq!(resolve_pinned_usize("V", Some("4")), Some(4));
        assert_eq!(resolve_pinned_usize("V", Some(" 16 ")), Some(16));
        for raw in ["0", "-1", "", "auto"] {
            assert_eq!(
                resolve_pinned_usize("V", Some(raw)),
                None,
                "pin {raw:?} must leave the controller in charge"
            );
        }
    }

    #[test]
    fn maintenance_encode_pin_is_opt_in() {
        assert_eq!(resolve_maintenance_encode_shards(None), None);
        assert_eq!(resolve_maintenance_encode_shards(Some("1")), Some(1));
        assert_eq!(resolve_maintenance_encode_shards(Some(" 4 ")), Some(4));
    }

    #[test]
    fn an_unusable_maintenance_pin_leaves_the_sized_policy_alone() {
        for raw in ["0", "-1", "", "auto", "serial"] {
            assert_eq!(
                resolve_maintenance_encode_shards(Some(raw)),
                None,
                "pin {raw:?} must not change the shipped policy"
            );
        }
    }

    #[test]
    fn fan_out_defaults_to_the_cpu_budget() {
        assert_eq!(
            resolve_maintenance_fan_out(None, 64),
            MaintenanceFanOut::Fixed(64)
        );
    }

    #[test]
    fn fan_out_honors_a_pinned_sweep_override() {
        // 256 is above the budget on purpose: a sweep has to be able to probe
        // oversubscription, not only undersubscription.
        for (raw, want) in [("1", 1), ("8", 8), (" 4 ", 4), ("256", 256)] {
            assert_eq!(
                resolve_maintenance_fan_out(Some(raw), 64),
                MaintenanceFanOut::Fixed(want)
            );
        }
    }

    #[test]
    fn auto_selects_per_output_file_sizing() {
        for raw in ["auto", "AUTO", " Auto "] {
            assert_eq!(
                resolve_maintenance_fan_out(Some(raw), 64),
                MaintenanceFanOut::PerOutputFile
            );
        }
    }

    #[test]
    fn an_unusable_override_falls_back_rather_than_stalling_the_pass() {
        for raw in ["0", "-1", "", "4.5", "some"] {
            assert_eq!(
                resolve_maintenance_fan_out(Some(raw), 64),
                MaintenanceFanOut::Fixed(64),
                "override {raw:?} should fall back to the budget"
            );
        }
    }

    #[test]
    fn a_degenerate_budget_still_plans_one_partition() {
        // A zero would propagate into the encoder shard cap as "no writers".
        assert_eq!(
            resolve_maintenance_fan_out(None, 0),
            MaintenanceFanOut::Fixed(1)
        );
        assert_eq!(
            resolve_maintenance_fan_out(Some("0"), 0),
            MaintenanceFanOut::Fixed(1)
        );
    }

    #[test]
    fn output_file_estimate_tracks_the_encode_shard_unit() {
        // A 512 MiB target gives unit = max(512/16, 16) MiB = 32 MiB.
        const TARGET: usize = 512 * 1024 * 1024;
        const MIB: u64 = 1024 * 1024;
        assert_eq!(estimated_output_files(0, TARGET), 1, "an empty write still writes one file");
        assert_eq!(estimated_output_files(20 * MIB, TARGET), 1, "under one unit");
        assert_eq!(estimated_output_files(32 * MIB, TARGET), 1);
        assert_eq!(estimated_output_files(64 * MIB, TARGET), 2);
        assert_eq!(estimated_output_files(320 * MIB, TARGET), 10);
        // A target below the 16 MiB floor clamps the unit to the target itself.
        assert_eq!(estimated_output_files(4 * MIB, 1024 * 1024), 4);
        // A zeroed target must not divide by zero.
        assert_eq!(estimated_output_files(4 * MIB, 0), 4 * MIB);
    }
}
