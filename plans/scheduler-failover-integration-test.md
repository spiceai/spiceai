# Scheduler failover integration test — design & follow-ups

Covers the automated tests added for the distributed-scheduler failover work
(stateless/HA schedulers backed by shared object-store job state).

## What is tested

1. **`SharedJobState` unit tests** (`crates/runtime/src/cluster/shared_job_state.rs`)
   — ownership/fencing at the metadata layer, no real execution graph needed:
   - `is_terminal` classification, including **corrupt/undecodable status → terminal**
     (so a job whose persisted status can't be decoded is never taken over).
   - `try_acquire_job` refuses unknown jobs, self-owned jobs, and corrupt-status jobs.
   - `remove_job` deletes metadata then graph.

2. **Failover integration tests** (`crates/runtime/tests/cluster/scheduler_failover.rs`)
   — end-to-end: a distributed async query whose driving scheduler is lost is
   recovered by another scheduler sharing the same object-store job state and
   **driven to completion with correct results**.

## Why a "withheld executor" instead of a blocked datasource

The reliable hold needs the query to be provably in flight when the scheduler is
lost. A literal *blocked datasource* turns out not to be reachable at the place
it would need to block:

- The executor runs a **serialized physical plan** (`U::try_decode(task.plan)`),
  not a re-planned query. A generic custom `TableProvider` is **not** re-resolved
  on the executor — only Iceberg is (the codec replays `IcebergClusterTableProvider`
  scans; it errors for any other provider).
- File/CSV/Parquet sources are plain serialized scans that just read files
  (they don't block).
- Flight/SQL sources are **federated** (pushed down, scan runs scheduler-side),
  so they wouldn't block at executor execution either.

So instead the test withholds an executor from the submitting scheduler: the
single executor attaches to the recovery scheduler (`s2`), and a job submitted to
`s1` registers as `Running` but **deterministically stalls** — `s1` has no
executor to dispatch tasks to (executor dispatch is per-scheduler via PollWork
RPCs). Recovery lets `s2`, which owns the executor, complete the job. This proves
completion-on-recovery with no timing race and no custom source/codec changes.

Key viability facts confirmed:
- Executor dispatch is per-scheduler, so `s1` genuinely cannot run the job.
- `execute_job` does **not** mark the job terminal on cancellation, so a stopped
  scheduler leaves the job `Running` in the shared store for `recover_orphaned_jobs`.

## SIGKILL vs SIGTERM — in-process limitation

In-process, the runtime can only be stopped **gracefully** (`Runtime::shutdown`):
the registry loop's cancel arm always calls `shutdown()`, which removes the
cluster entry and deletes the heartbeat (SIGTERM semantics). The registry task
also holds a strong `Arc<Runtime>` (a reference cycle), so dropping the handle
won't stop the heartbeat either.

A **true SIGKILL** — heartbeat goes stale *without* deregistration, so recovery
happens via heartbeat-TTL expiry (`DEFAULT_TTL_MS = 30s`) rather than immediate
deregistration — requires an abrupt process kill. Both in-process tests therefore
drive recovery via graceful shutdown and assert the same recovery + completion
contract; the SIGKILL-named test documents the TTL-expiry path it does not cover.

The real SIGKILL path is exercised manually by a standalone failover demo (a
terminal TUI that runs a local cluster, kills a scheduler with SIGKILL/SIGTERM,
and shows the job being recovered by the other scheduler) — it kills actual
`spiced` processes and observes TTL-based recovery.

## Follow-up: subprocess harness for true SIGKILL

To cover the TTL-expiry path in CI, add a subprocess-based e2e test that:
- spawns real `spiced` scheduler + executor processes sharing a `file://` (or S3)
  state location,
- submits an async query via HTTP `/v1/queries` to `s1`,
- sends `SIGKILL` to `s1` (and `SIGTERM` for the graceful variant),
- polls `/v1/queries/{id}` via `s2` until `Succeeded` and asserts the result.

This is heavier (needs the built binary + subprocess/HTTP orchestration, slower,
more CI-sensitive) so it is intentionally separate from the in-process suite.

A cheaper alternative is a small `#[doc(hidden)]` test-support method on `Runtime`
that abruptly aborts the cluster-registry task without deregistering, letting the
in-process test exercise TTL-expiry recovery without subprocesses.
