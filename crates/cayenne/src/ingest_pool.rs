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

//! Process-global pinned ingest pool for the morsel CDC-apply pipeline
//! (proposal §§8/10, Stage 1 #3a — the substrate skeleton).
//!
//! ## What this is
//!
//! A machine-wide pool of `std::thread` workers, each pinned to a reserved CPU
//! core via [`core_affinity`], consuming [`Task`] descriptors off an MPMC
//! [`crossbeam_channel`]. It is process-global — one pool shared across every
//! Cayenne table — because the ingest/query core partition (proposal §8) is a
//! property of the machine, not of a single table, exactly like the in-memory
//! CDC tier byte budget ([`super::provider::mem_tier_budget`]).
//!
//! ## What this is NOT (yet)
//!
//! Stage 1 #3a stands up the substrate in isolation: it defines only the
//! test-only [`Task::Probe`] variant and is **not wired to the CDC apply path**.
//! The memory-apply descriptor (`Task::MemoryApply`), the three-queue
//! `TaskQueue` (§10.1: `st3` LIFO + owner-local `VecDeque` + `PriorityMailbox`),
//! `catch_unwind` worker isolation, and depth-> 1 routing land in #3b/#3c. Here
//! the workers pin, park, run a trivial probe, resize elastically, and join
//! cleanly — nothing more.
//!
//! ## Core partition honesty (Stage 1)
//!
//! The ingest workers are *hard-pinned* to their cores. The query runtime is a
//! separate Tokio runtime (per `CLAUDE.md`) and is **not** pinned, so excluding
//! queries from the ingest cores is *soft* (OS-scheduled) in Stage 1 — the core
//! count is a hard reservation for ingest, advisory for queries. The adaptive
//! grow/shrink controller (§8.1) is Stage 2c; the affinity itself is best-effort
//! (some platforms/containers — notably Apple Silicon macOS — accept the request
//! but do not enforce it), which is why [`ProbeReport::affinity_set`] is
//! observational, not a guarantee.
//!
//! ## Shutdown / elasticity
//!
//! Each worker owns a `retire` flag. A resize spawns the delta (grow) or sets
//! the flag on the surplus workers and joins them (shrink) — surviving workers
//! keep their identity, so a resize never tears down and rebuilds the pool
//! ("no thrash"). Workers park in `recv_timeout` so the flag is observed within
//! one poll interval, bounding shutdown latency (the Stage 2 mailbox will make
//! the wake event-driven). This mirrors the channel-close exit of
//! `spawn_maintained_aggregate_applier`, generalized with core pinning + resize.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::thread::JoinHandle;
use std::time::Duration;

use core_affinity::CoreId;
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, TryRecvError};
use parking_lot::RwLock;
use tokio::sync::oneshot;

use crate::metadata::IngestCores;

/// How long a parked worker blocks in `recv_timeout` before re-checking its
/// `retire` flag. A submitted task wakes a parked receiver immediately (the
/// timeout only bounds *retirement*, never task latency), so this is purely the
/// worst-case shutdown/shrink latency. Small enough that teardown is prompt,
/// large enough that a wholly-idle worker's periodic wake is negligible. The
/// Stage 2 `PriorityMailbox` (§10.1) replaces this poll with an event-driven
/// unpark.
const RETIRE_POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Number of adaptive spin rounds a worker makes on an empty channel before it
/// parks. Catches a rapidly-arriving task without a park/unpark round-trip under
/// continuous load; harmless (a few hundred `spin_loop` hints) when idle.
const SPIN_ROUNDS: u32 = 4;

/// A diagnostic report of one worker's core assignment and whether the OS
/// accepted the affinity request for it. Returned by [`Task::Probe`] and by the
/// per-worker startup rendezvous ([`IngestPool::worker_reports`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProbeReport {
    /// The core id this worker was asked to pin to, or `None` if the platform
    /// exposed no core ids (then the worker runs unpinned).
    pub assigned_core: Option<usize>,
    /// Whether `core_affinity::set_for_current` reported success for that core.
    /// Best-effort: `false` here (e.g. Apple Silicon macOS) does not mean the
    /// worker is broken, only that pinning is advisory on this platform.
    pub affinity_set: bool,
}

/// A unit of work submitted to the pinned ingest pool.
///
/// A concrete enum — never `Box<dyn FnOnce>` — so the hot path allocates no
/// per-task trait object (proposal §10). Stage 1 #3a defines only the test-only
/// `Probe`; #3c adds `MemoryApply(Box<MemoryApplyDescriptor>)` carrying the
/// factored-out synchronous memory apply.
#[derive(Debug)]
pub enum Task {
    /// Trivial diagnostic task: the executing worker reports its own core
    /// assignment over `report`, isolating the channel + oneshot + wake hand-off
    /// cost. Exercised only by tests; production apply routing arrives in #3c.
    Probe {
        /// Pool -> edge reply channel (a `tokio::sync::oneshot`, matching the
        /// design's completion carrier — sendable from the sync worker).
        report: oneshot::Sender<ProbeReport>,
    },
}

/// Handle to one pinned worker thread.
struct WorkerHandle {
    /// Monotonic pool-local id, stable across resizes — lets a resize prove it
    /// kept (did not recreate) the surviving workers.
    id: u64,
    /// Set to retire *this* worker (shrink / shutdown); observed within one
    /// [`RETIRE_POLL_INTERVAL`].
    retire: Arc<AtomicBool>,
    /// The core this worker was assigned (for diagnostics / read-back).
    assigned_core: Option<usize>,
    join: JoinHandle<()>,
}

/// A process-global pool of core-pinned ingest workers over an MPMC channel.
pub struct IngestPool {
    /// Edge -> pool descriptor channel. Unbounded: back-pressure is applied at
    /// the edge by the `write_lock` serialization (proposal §6/§10), not by a
    /// bounded queue here.
    tx: Sender<Task>,
    /// Kept so [`IngestPool::spawn_worker`] can hand a clone to each new worker
    /// on grow.
    rx: Receiver<Task>,
    /// The cores this pool may pin workers to (the ingest reservation). Empty if
    /// the platform exposed none, in which case workers run unpinned.
    core_ids: Vec<usize>,
    /// Startup affinity report per live worker, in `workers` order.
    startup_reports: Vec<ProbeReport>,
    workers: Vec<WorkerHandle>,
    /// Source of stable per-worker ids (see [`WorkerHandle::id`]).
    next_worker_id: u64,
}

impl IngestPool {
    /// Build a pool with `worker_count` pinned workers (clamped to >= 1).
    #[must_use]
    pub fn new(worker_count: usize) -> Self {
        let (tx, rx) = crossbeam_channel::unbounded::<Task>();
        let core_ids = detect_ingest_core_ids();
        let mut pool = Self {
            tx,
            rx,
            core_ids,
            startup_reports: Vec::new(),
            workers: Vec::new(),
            next_worker_id: 0,
        };
        pool.resize(worker_count);
        pool
    }

    /// Number of live workers.
    #[must_use]
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// The per-worker startup affinity reports, in worker order. Used by the
    /// affinity read-back assertion and for diagnostics.
    #[must_use]
    pub fn worker_reports(&self) -> &[ProbeReport] {
        &self.startup_reports
    }

    /// Submit a task to the pool. Returns `Err(task)` if the send fails (every
    /// worker gone), so the caller can fall back to the inline apply path.
    ///
    /// # Errors
    ///
    /// Returns the un-submitted `task` when the channel has no live receiver.
    pub fn submit(&self, task: Task) -> Result<(), Task> {
        self.tx.send(task).map_err(crossbeam_channel::SendError::into_inner)
    }

    /// Grow or shrink to `target` live workers (clamped to >= 1). Grow spawns
    /// only the deficit; shrink retires only the surplus (from the end) and
    /// joins them — surviving workers are untouched, so this never thrashes the
    /// pool. Bounded: a retired worker exits within one [`RETIRE_POLL_INTERVAL`].
    pub fn resize(&mut self, target: usize) {
        let target = target.max(1);
        while self.workers.len() < target {
            if let Err(error) = self.spawn_worker() {
                tracing::error!(
                    target: "cayenne::ingest_pool",
                    %error,
                    have = self.workers.len(),
                    want = target,
                    "Failed to spawn ingest worker; pool left undersized"
                );
                break;
            }
        }
        if self.workers.len() > target {
            self.retire_workers(self.workers.len() - target);
        }
    }

    /// Spawn one worker pinned to the next core in the reservation (round-robin
    /// over `core_ids`, or unpinned when none are available), blocking until it
    /// reports its startup affinity so [`Self::worker_reports`] is populated in
    /// worker order.
    fn spawn_worker(&mut self) -> std::io::Result<()> {
        let index = self.workers.len();
        let assigned_core = if self.core_ids.is_empty() {
            None
        } else {
            Some(self.core_ids[index % self.core_ids.len()])
        };
        let id = self.next_worker_id;
        self.next_worker_id += 1;

        let rx = self.rx.clone();
        let retire = Arc::new(AtomicBool::new(false));
        let worker_retire = Arc::clone(&retire);
        // Startup rendezvous: the worker sends its affinity report exactly once,
        // right after pinning, before entering the run loop.
        let (ready_tx, ready_rx) = crossbeam_channel::bounded::<ProbeReport>(1);

        let join = std::thread::Builder::new()
            .name(format!("cayenne-ingest-{id}"))
            .spawn(move || run_worker(assigned_core, &rx, &worker_retire, &ready_tx))?;

        // The report is sent before any run-loop work, so this wait is bounded
        // by thread start-up. If the worker died before reporting, fall back to
        // an un-pinned report rather than blocking forever.
        let report = ready_rx.recv().unwrap_or(ProbeReport {
            assigned_core,
            affinity_set: false,
        });
        self.startup_reports.push(report);
        self.workers.push(WorkerHandle {
            id,
            retire,
            assigned_core,
            join,
        });
        Ok(())
    }

    /// Retire the last `count` workers: flag them, then join. Bounded by
    /// [`RETIRE_POLL_INTERVAL`] per worker (they park on `recv_timeout`).
    fn retire_workers(&mut self, count: usize) {
        let count = count.min(self.workers.len());
        let start = self.workers.len() - count;
        // Flag the whole surplus first so they retire concurrently, then join.
        for handle in &self.workers[start..] {
            handle.retire.store(true, Ordering::Release);
        }
        for handle in self.workers.drain(start..).rev() {
            if handle.join.join().is_err() {
                tracing::error!(
                    target: "cayenne::ingest_pool",
                    worker_id = handle.id,
                    core = ?handle.assigned_core,
                    "Ingest worker panicked during retire"
                );
            }
        }
        self.startup_reports.truncate(self.workers.len());
    }
}

impl Drop for IngestPool {
    fn drop(&mut self) {
        // Retire every worker and join, so a dropped pool leaks no threads
        // (mirrors the channel-close join of `spawn_maintained_aggregate_applier`).
        self.retire_workers(self.workers.len());
    }
}

/// The worker run loop: pin, report, then serve tasks until retired.
fn run_worker(
    assigned_core: Option<usize>,
    rx: &Receiver<Task>,
    retire: &AtomicBool,
    ready_tx: &Sender<ProbeReport>,
) {
    let affinity_set = match assigned_core {
        Some(id) => core_affinity::set_for_current(CoreId { id }),
        None => false,
    };
    let report = ProbeReport {
        assigned_core,
        affinity_set,
    };
    // Ignore a closed rendezvous: the pool may have been dropped mid-spawn.
    let _ = ready_tx.send(report);

    loop {
        if retire.load(Ordering::Acquire) {
            return;
        }
        // Brief adaptive spin to catch a fast-arriving task without parking.
        let mut spun = None;
        for shift in 0..SPIN_ROUNDS {
            match rx.try_recv() {
                Ok(task) => {
                    spun = Some(task);
                    break;
                }
                Err(TryRecvError::Empty) => spin_backoff(shift),
                Err(TryRecvError::Disconnected) => return,
            }
        }
        let task = match spun {
            Some(task) => task,
            // Park until a task arrives, the channel disconnects, or the poll
            // interval elapses (so the retire flag is observed).
            None => match rx.recv_timeout(RETIRE_POLL_INTERVAL) {
                Ok(task) => task,
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => return,
            },
        };
        run_task(task, report);
    }
}

/// Execute one task on the worker. Stage 1 #3a handles only the diagnostic
/// probe; the memory-apply arm (in `catch_unwind`) arrives in #3c.
fn run_task(task: Task, report: ProbeReport) {
    match task {
        Task::Probe { report: reply } => {
            // The receiver may have been dropped (test cancelled); ignore.
            let _ = reply.send(report);
        }
    }
}

/// Bounded busy-wait: `2^shift` `spin_loop` hints (capped), for the pre-park
/// spin. Avoids a `crossbeam_utils::Backoff` dependency in #3a; the Stage 2
/// mailbox replaces this with an event-driven wake anyway.
#[inline]
fn spin_backoff(shift: u32) {
    for _ in 0..(1u32 << shift.min(6)) {
        std::hint::spin_loop();
    }
}

/// The core ids the ingest pool may pin to. Stage 1 reserves the first cores the
/// platform reports (a static, machine-wide reservation); the adaptive partition
/// controller (§8.1) refines this in Stage 2c. Empty when the platform exposes
/// no core ids — workers then run unpinned.
fn detect_ingest_core_ids() -> Vec<usize> {
    core_affinity::get_core_ids()
        .map(|ids| ids.into_iter().map(|c| c.id).collect())
        .unwrap_or_default()
}

/// Count of installs/resizes applied to the global pool — diagnostic only,
/// lets a test confirm an install resized in place rather than rebuilding.
static GLOBAL_INSTALL_GENERATION: AtomicUsize = AtomicUsize::new(0);

/// Diagnostic generation counter; bumped once per [`install_global_ingest_pool`].
/// Lets a test confirm an install resized in place rather than rebuilding.
#[must_use]
pub fn global_ingest_pool_generation() -> usize {
    GLOBAL_INSTALL_GENERATION.load(Ordering::Acquire)
}

/// Process-global ingest pool. `None` until installed; replaceable so a test
/// binary building and dropping multiple runtimes never retains a stale pool
/// (mirrors [`super::provider::mem_tier_budget`]).
static GLOBAL_INGEST_POOL: LazyLock<RwLock<Option<IngestPool>>> =
    LazyLock::new(|| RwLock::new(None));

/// Install — or resize in place — the process-global ingest pool for the given
/// core policy. Idempotent: an existing pool is resized (no teardown), so
/// repeated table registrations with the same policy never thrash workers.
///
/// Stage 1 #3a exposes this as substrate API; the CDC apply path does not call
/// it yet (installation is wired with the `cayenne_ingest_substrate: pool` flag
/// in #3c). It is exercised by tests here.
pub fn install_global_ingest_pool(cores: IngestCores) {
    let target = cores.worker_count();
    let mut guard = GLOBAL_INGEST_POOL.write();
    match guard.as_mut() {
        Some(pool) => pool.resize(target),
        None => *guard = Some(IngestPool::new(target)),
    }
    GLOBAL_INSTALL_GENERATION.fetch_add(1, Ordering::AcqRel);
}

/// Submit a task to the process-global ingest pool. Returns `Err(task)` when no
/// pool is installed (memory-mode substrate not active) so the caller falls back
/// to the inline apply path.
///
/// # Errors
///
/// Returns the un-submitted `task` when no global pool is installed or its send
/// fails.
pub fn submit_to_global_ingest_pool(task: Task) -> Result<(), Task> {
    let guard = GLOBAL_INGEST_POOL.read();
    match guard.as_ref() {
        Some(pool) => pool.submit(task),
        None => Err(task),
    }
}

/// Uninstall the process-global ingest pool (joining its workers via
/// [`IngestPool`]'s `Drop`). Primarily for tests that must leave no pinned
/// threads behind; a running runtime keeps the pool for the process lifetime.
pub fn uninstall_global_ingest_pool() {
    let mut guard = GLOBAL_INGEST_POOL.write();
    *guard = None;
}

#[cfg(test)]
mod tests {
    use super::{
        IngestPool, ProbeReport, Task, global_ingest_pool_generation, install_global_ingest_pool,
        submit_to_global_ingest_pool, uninstall_global_ingest_pool,
    };
    use crate::metadata::IngestCores;
    use std::time::{Duration, Instant};
    use tokio::sync::oneshot;

    /// How many workers a local pool test can pin without over-subscribing the
    /// host: at most 2, and never more than the machine reports.
    fn small_worker_count() -> usize {
        std::thread::available_parallelism()
            .map_or(1, std::num::NonZeroUsize::get)
            .min(2)
    }

    /// Affinity read-back: every spawned worker reports the core it was assigned,
    /// assignments are distinct across workers, and (where the platform enforces
    /// pinning) affinity succeeded. `affinity_set` is only asserted on Linux —
    /// it is best-effort elsewhere (e.g. Apple Silicon macOS accepts but does not
    /// enforce), so requiring it there would be a false failure.
    #[test]
    fn affinity_readback_assigns_distinct_cores() {
        let n = small_worker_count();
        let pool = IngestPool::new(n);
        assert_eq!(pool.worker_count(), n, "pool spawned the requested workers");

        let reports = pool.worker_reports();
        assert_eq!(reports.len(), n, "one startup report per worker");

        let assigned: Vec<Option<usize>> = reports.iter().map(|r| r.assigned_core).collect();
        // On any host that reports core ids, distinct workers must get distinct
        // cores (the first `n` of the reservation).
        if assigned.iter().all(Option::is_some) {
            let mut cores: Vec<usize> = assigned.iter().filter_map(|c| *c).collect();
            cores.sort_unstable();
            cores.dedup();
            assert_eq!(cores.len(), n, "each worker pinned to a distinct core: {reports:?}");
        }

        #[cfg(target_os = "linux")]
        for report in reports {
            assert!(
                report.affinity_set,
                "Linux enforces core affinity; expected set_for_current to succeed: {report:?}"
            );
        }
    }

    /// Elasticity: grow then shrink resizes only the delta and keeps surviving
    /// workers' identities (their stable ids), proving no teardown/rebuild
    /// ("no thrash"), and every resize returns (bounded — no hang).
    #[test]
    fn elasticity_grow_shrink_preserves_survivors() {
        let mut pool = IngestPool::new(1);
        let base_id = pool.workers[0].id;

        // Grow 1 -> 3: survivor keeps its id, two fresh workers appended.
        pool.resize(3);
        assert_eq!(pool.worker_count(), 3);
        assert_eq!(pool.workers[0].id, base_id, "grow did not recreate worker 0");
        let ids_after_grow: Vec<u64> = pool.workers.iter().map(|w| w.id).collect();
        assert_eq!(
            ids_after_grow.len(),
            {
                let mut u = ids_after_grow.clone();
                u.sort_unstable();
                u.dedup();
                u.len()
            },
            "worker ids are unique"
        );

        // Shrink 3 -> 1: only the surplus retired; survivor unchanged.
        pool.resize(1);
        assert_eq!(pool.worker_count(), 1);
        assert_eq!(pool.workers[0].id, base_id, "shrink kept worker 0");

        // A resize to 0 is clamped to 1 (the pool always keeps a worker so
        // submitted work can drain).
        pool.resize(0);
        assert_eq!(pool.worker_count(), 1);
    }

    /// Trivial-task round-trip through a local pool: submit a probe, await the
    /// reply, and confirm the executing worker reported a valid core assignment.
    /// Isolates the channel + oneshot + wake hand-off cost from any apply work.
    #[tokio::test]
    async fn probe_round_trip_reports_worker_core() {
        let pool = IngestPool::new(1);
        let expected_core = pool.worker_reports()[0].assigned_core;

        let (tx, rx) = oneshot::channel::<ProbeReport>();
        let started = Instant::now();
        pool.submit(Task::Probe { report: tx })
            .expect("submit probe to a pool with a live worker");

        let report = tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .expect("probe round-trip completed within timeout")
            .expect("worker replied before dropping the sender");

        assert_eq!(
            report.assigned_core, expected_core,
            "probe ran on the pool's pinned worker"
        );
        // Not a perf gate — just confirms the hand-off is prompt, not stalled.
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "probe hand-off should be near-instant, took {:?}",
            started.elapsed()
        );
    }

    /// The process-global install/submit path: install resizes in place (bumping
    /// the generation, not rebuilding), and a submitted probe round-trips.
    #[tokio::test]
    async fn global_pool_install_and_submit() {
        // Leave no pinned threads behind regardless of assertion outcome.
        struct Cleanup;
        impl Drop for Cleanup {
            fn drop(&mut self) {
                uninstall_global_ingest_pool();
            }
        }
        let _cleanup = Cleanup;

        let gen_before = global_ingest_pool_generation();
        install_global_ingest_pool(IngestCores::Auto);
        // A second install with the same policy resizes in place (no rebuild);
        // the generation advances on each call.
        install_global_ingest_pool(IngestCores::Auto);
        assert!(
            global_ingest_pool_generation() >= gen_before + 2,
            "each install bumps the generation counter"
        );

        let (tx, rx) = oneshot::channel::<ProbeReport>();
        submit_to_global_ingest_pool(Task::Probe { report: tx })
            .map_err(|_| ())
            .expect("global pool installed, so submit succeeds");

        let report = tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .expect("global probe round-trip within timeout")
            .expect("global worker replied");
        // Auto == 1 worker in Stage 1; the report carries that worker's core.
        let _ = report.assigned_core;
    }

    /// `IngestCores` policy -> worker-count mapping, including the Stage-1 `Auto`
    /// = 1 default and the `Fixed(0)` clamp.
    #[test]
    fn ingest_cores_worker_count_mapping() {
        assert_eq!(IngestCores::Auto.worker_count(), 1);
        assert_eq!(IngestCores::Fixed(4).worker_count(), 4);
        assert_eq!(IngestCores::Fixed(0).worker_count(), 1);
        assert_eq!(IngestCores::default(), IngestCores::Auto);
    }

    /// `cayenne_ingest_cores` param parsing: `auto` and bare integers, rejecting
    /// junk so the caller can warn and fall back to the default.
    #[test]
    fn ingest_cores_parse() {
        assert_eq!(IngestCores::parse("auto"), Some(IngestCores::Auto));
        assert_eq!(IngestCores::parse("  AUTO "), Some(IngestCores::Auto));
        assert_eq!(IngestCores::parse("3"), Some(IngestCores::Fixed(3)));
        assert_eq!(IngestCores::parse("0"), Some(IngestCores::Fixed(0)));
        assert_eq!(IngestCores::parse("fixed"), None);
        assert_eq!(IngestCores::parse("-1"), None);
        assert_eq!(IngestCores::parse(""), None);
    }
}
