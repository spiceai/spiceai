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
//! whose combined size is worth a rewrite. The current runner (in
//! [`crate::provider::table`]) uses that candidate as an eligibility and
//! observability signal, then atomically rewrites the entire current snapshot.
//! The rewrite goes through `write_to_snapshot`, which honors `target_partitions`
//! and the configured target file size, so a pass typically produces one or a
//! small number of consolidated Vortex files rather than guaranteeing exactly
//! one.
//!
//! The module also owns [`BackgroundCompactor`], a per-table tokio task that
//! periodically invokes the runner. The task is `Semaphore`-gated so a fleet of
//! tables can't overwhelm the writer pool.

use std::future::Future;
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

/// Inject the dedicated compaction runtime handle. Called once at process
/// startup. Later calls replace the previous handle so tests that create a new
/// runtime after dropping an old one do not retain stale global state.
pub fn set_compaction_runtime_handle(handle: Handle) {
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
    /// selection. The current runner still rewrites the whole snapshot once a
    /// candidate is found.
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
            'wake: loop {
                tokio::select! {
                    () = tokio::time::sleep(interval) => {}
                    () = shutdown_task.notified() => break,
                }

                let Some(runner) = runner.upgrade() else {
                    // Provider dropped — task exits naturally.
                    break;
                };

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
                    // across all tables sharing the semaphore.
                    let Ok(_permit) = Arc::clone(&semaphore).acquire_owned().await else {
                        // Semaphore closed — provider tree shutting down.
                        break 'wake;
                    };

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
/// Sized to outlast a realistic compaction pass: a pass rewrites the whole
/// current snapshot (see `run_one_compaction_pass` / `rewrite_current_snapshot_for_compaction`),
/// so its duration scales with table size. At large scale factors that rewrite
/// can take well over the original 5s, so the abort fired mid-write and vortex-io
/// panicked ("Runtime dropped task without completing it"). 30s covers a realistic
/// large-table pass and coincides with the runtime's connection-drain window.
///
/// This is a mitigation, not a cure: a pass that still exceeds the window aborts
/// mid-write and panics on shutdown. The durable fix is incremental (tiered) merge
/// of the picked candidate files instead of a full-snapshot rewrite, which keeps a
/// pass short enough to always drain — see the picker's `CompactionCandidate`.
const COMPACTOR_SHUTDOWN_DRAIN: Duration = Duration::from_secs(30);

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
}
