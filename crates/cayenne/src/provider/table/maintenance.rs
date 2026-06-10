//! Post-write maintenance scheduling, background compaction/checkpoint runners.
//!
//! `schedule_post_write_maintenance` coalesces stats/retention/listing work into
//! [`PostWriteMaintenance`] and drains it on a debounced background loop
//! (`run_maintenance_state`, which also drains deferred tombstone flips and the
//! metastore WAL). `maybe_compact_small_files` is the inline compaction trigger
//! (`compaction_lock` via `try_lock`; callers coordinate `write_lock`).
//! `spawn_background_compaction` / `spawn_background_mem_tier_checkpoint` start
//! the per-table background tasks (the checkpoint tick takes
//! `mem_checkpoint_lock` to serialize with write-path spills).
//!
//! Mechanically split out of `provider/table.rs`; code is moved verbatim.

use super::{
    Arc, AtomicBool, CatalogResult, CayenneTableProvider, ColumnStatsAccumulator, Duration,
    HashMap, HashSet, Ordering, POST_WRITE_MAINTENANCE_DEBOUNCE,
    PROTECTED_SNAPSHOT_AGE_WARNING_KEY_LIMIT, ParkingMutex, Result, RowCountUpdate, SystemTime,
    UNIX_EPOCH, VecDeque,
};

#[derive(Debug, Default)]
pub(super) struct BoundedWarningKeys {
    pub(super) seen: HashSet<String>,
    pub(super) insertion_order: VecDeque<String>,
}

impl BoundedWarningKeys {
    pub(super) fn insert_new(&mut self, key: String, limit: usize) -> bool {
        if self.seen.contains(&key) {
            return false;
        }

        if self.seen.len() >= limit
            && let Some(oldest_key) = self.insertion_order.pop_front()
        {
            self.seen.remove(&oldest_key);
        }

        self.insertion_order.push_back(key.clone());
        self.seen.insert(key)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SnapshotMaintenanceTrigger {
    ProtectedSnapshotCount {
        protected_snapshot_count: usize,
        trigger_count: usize,
    },
    ProtectedSnapshotAge {
        protected_snapshot_count: usize,
        oldest_snapshot_age: Duration,
        trigger_age: Duration,
    },
}

pub(super) fn should_warn_protected_snapshot_age(
    warning_keys: &ParkingMutex<BoundedWarningKeys>,
    snapshot_id: &str,
    warning_kind: &'static str,
) -> bool {
    let key = format!("{warning_kind}:{snapshot_id}");
    warning_keys
        .lock()
        .insert_new(key, PROTECTED_SNAPSHOT_AGE_WARNING_KEY_LIMIT)
}

pub(super) fn protected_snapshot_age(
    warning_keys: &ParkingMutex<BoundedWarningKeys>,
    snapshot_id: &str,
    now: SystemTime,
) -> Option<Duration> {
    // Protected snapshot ids are generated as UUIDv7 values by
    // `commit_on_conflict_publish`; imported or future ids that
    // do not preserve that invariant are ignored for age-triggered maintenance
    // and still participate in count-triggered maintenance.
    let Ok(snapshot_uuid) = uuid::Uuid::parse_str(snapshot_id) else {
        if should_warn_protected_snapshot_age(warning_keys, snapshot_id, "invalid_uuid") {
            tracing::warn!(
                snapshot_id,
                "Cayenne protected snapshot id is not a valid UUID; ignoring it for age-based maintenance"
            );
        }
        return None;
    };
    let Some(timestamp) = snapshot_uuid.get_timestamp() else {
        if should_warn_protected_snapshot_age(warning_keys, snapshot_id, "missing_uuid_timestamp") {
            tracing::warn!(
                snapshot_id,
                "Cayenne protected snapshot id does not contain a UUID timestamp; ignoring it for age-based maintenance"
            );
        }
        return None;
    };
    let (seconds, nanos) = timestamp.to_unix();
    let Some(snapshot_time) = UNIX_EPOCH.checked_add(Duration::new(seconds, nanos)) else {
        if should_warn_protected_snapshot_age(warning_keys, snapshot_id, "timestamp_overflow") {
            tracing::warn!(
                snapshot_id,
                "Cayenne protected snapshot timestamp overflowed SystemTime; ignoring it for age-based maintenance"
            );
        }
        return None;
    };
    if let Ok(age) = now.duration_since(snapshot_time) {
        Some(age)
    } else {
        if should_warn_protected_snapshot_age(warning_keys, snapshot_id, "future_timestamp") {
            tracing::warn!(
                snapshot_id,
                "Cayenne protected snapshot timestamp is in the future; ignoring it for age-based maintenance"
            );
        }
        None
    }
}

pub(super) fn oldest_protected_snapshot_age(
    warning_keys: &ParkingMutex<BoundedWarningKeys>,
    protected_snapshots: &HashMap<String, i64>,
    now: SystemTime,
) -> Option<Duration> {
    protected_snapshots
        .keys()
        .filter_map(|snapshot_id| protected_snapshot_age(warning_keys, snapshot_id, now))
        .max()
}

pub(super) fn duration_millis_saturating(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

pub(super) fn protected_snapshot_maintenance_trigger(
    warning_keys: &ParkingMutex<BoundedWarningKeys>,
    protected_snapshots: &HashMap<String, i64>,
    trigger_count: usize,
    trigger_age: Option<Duration>,
    now: SystemTime,
) -> Option<SnapshotMaintenanceTrigger> {
    let protected_snapshot_count = protected_snapshots.len();
    let trigger_count = trigger_count.max(1);
    if protected_snapshot_count >= trigger_count {
        return Some(SnapshotMaintenanceTrigger::ProtectedSnapshotCount {
            protected_snapshot_count,
            trigger_count,
        });
    }

    // Age parsing only runs below the count trigger; above it, the cheaper
    // count trigger short-circuits before scanning snapshot ids.
    let trigger_age = trigger_age?;
    let oldest_snapshot_age =
        oldest_protected_snapshot_age(warning_keys, protected_snapshots, now)?;
    if oldest_snapshot_age >= trigger_age {
        Some(SnapshotMaintenanceTrigger::ProtectedSnapshotAge {
            protected_snapshot_count,
            oldest_snapshot_age,
            trigger_age,
        })
    } else {
        None
    }
}

#[derive(Default)]
pub(super) struct PostWriteMaintenanceState {
    pub(super) stats: Option<Arc<ColumnStatsAccumulator>>,
    pub(super) refresh_listing: bool,
    /// Set when the writer wants retention filters applied. Coalesces — multiple
    /// writes scheduling retention collapse to one scan per debounce window.
    pub(super) retention_requested: bool,
    /// Net change to the live row count across the coalesced writes
    /// (`inserted - superseded - deleted`). Accumulated alongside `stats` and
    /// applied as a [`RowCountUpdate::Delta`] when the stats are persisted.
    pub(super) live_rows_delta: i64,
}

impl PostWriteMaintenanceState {
    pub(super) fn is_empty(&self) -> bool {
        self.stats.is_none()
            && !self.refresh_listing
            && !self.retention_requested
            && self.live_rows_delta == 0
    }
}

pub(super) enum RetentionFailureAction {
    Requeue,
    ReturnError,
}

#[derive(Default)]
pub(super) struct PostWriteMaintenance {
    pub(super) state: ParkingMutex<PostWriteMaintenanceState>,
    pub(super) scheduled: AtomicBool,
}

impl CayenneTableProvider {
    /// Inline tiered-merge-tree trigger.
    ///
    /// Lists Vortex files in the current snapshot directory along with their
    /// sizes, runs the picker, and — if a candidate exists — rewrites the
    /// entire current snapshot into a fresh one. Re-evaluates after each pass,
    /// up to `compaction_max_levels` consecutive rewrites, so a tier can
    /// promote (small → mid → settled) within one trigger.
    ///
    /// Best-effort by design: errors are returned to the caller for logging,
    /// but never bubble up to fail the originating write or query. The
    /// per-table `compaction_lock` is acquired with `try_lock` — if another
    /// pass is already in flight (inline or background), we skip this trigger
    /// rather than queueing more work.
    ///
    /// **Callers are responsible for write-lock coordination.** Inline callers
    /// (in `mutation_writer`) hold `write_lock` already, so they call this
    /// directly. The background scheduler's [`super::compaction::CompactionRunner`]
    /// adapter `try_lock`s `write_lock` before delegating here. Tests use the
    /// `#[doc(hidden)] pub` exposure for direct access — no concurrent writers
    /// in single-table test setups.
    ///
    /// Returns `Ok(true)` if at least one snapshot rewrite occurred.
    #[doc(hidden)]
    pub async fn maybe_compact_small_files(&self) -> Result<bool> {
        let Ok(_guard) = self.compaction_lock.try_lock() else {
            tracing::trace!(
                table = self.table_metadata.table_name.as_str(),
                "Skipping compaction trigger: another pass already running",
            );
            return Ok(false);
        };

        let max_passes = self.context.compaction_max_levels();
        let mut total_passes = 0_usize;

        for _ in 0..max_passes {
            if !self.run_one_compaction_pass().await? {
                break;
            }
            total_passes += 1;
        }

        Ok(total_passes > 0)
    }

    pub(crate) fn schedule_post_write_compaction(&self) {
        let cfg = self.context.compaction_picker_config();
        let maintenance_trigger = self.protected_snapshot_maintenance_trigger();
        if self.new_files_since_last_compaction.load(Ordering::Relaxed) < cfg.trigger_files
            && maintenance_trigger.is_none()
        {
            return;
        }

        if self
            .post_write_compaction_scheduled
            .swap(true, Ordering::AcqRel)
        {
            return;
        }

        let table = self.clone_for_write();
        // Run the compaction pass (size-tiered protected-snapshot merge and/or
        // full snapshot rewrite) on the dedicated compaction runtime, isolated
        // from the query (compute) and CDC (refresh) runtimes.
        super::compaction::spawn_compaction(async move {
            tokio::task::yield_now().await;
            let result = super::compaction::CompactionRunner::run_compaction_trigger(&table).await;
            table
                .post_write_compaction_scheduled
                .store(false, Ordering::Release);

            match result {
                Ok(true) => {
                    tracing::debug!(
                        table = table.table_metadata.table_name.as_str(),
                        "Post-write compaction pass completed"
                    );
                }
                Ok(false) => {}
                Err(e) => {
                    tracing::warn!(
                        table = table.table_metadata.table_name.as_str(),
                        "Post-write compaction trigger failed: {e}"
                    );
                }
            }
        });
    }

    pub(crate) fn schedule_inline_checkpoint_if_memtable_pressure_exceeded(&self) {
        if self
            .inline_checkpoint_scheduled
            .swap(true, Ordering::AcqRel)
        {
            return;
        }

        let table = self.clone_for_write();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            let result = async {
                let _write_guard = table.write_lock.lock().await;
                table
                    .checkpoint_inlined_data_if_memtable_pressure_exceeded()
                    .await
            }
            .await;

            table
                .inline_checkpoint_scheduled
                .store(false, Ordering::Release);

            if let Err(e) = result {
                tracing::warn!(
                    table = table.table_metadata.table_name.as_str(),
                    "Auto-checkpoint of inline memtable failed: {e}"
                );
            }
        });
    }

    pub(crate) fn schedule_post_write_maintenance(
        &self,
        stats: Option<Arc<ColumnStatsAccumulator>>,
        refresh_listing: bool,
        retention_requested: bool,
        live_rows_delta: i64,
    ) {
        if stats.is_none() && !refresh_listing && !retention_requested && live_rows_delta == 0 {
            return;
        }

        {
            let mut maintenance_state = self.post_write_maintenance.state.lock();
            if let Some(stats) = stats {
                if let Some(existing) = &maintenance_state.stats {
                    existing.merge_from(&stats);
                } else {
                    maintenance_state.stats = Some(stats);
                }
            }
            maintenance_state.refresh_listing |= refresh_listing;
            maintenance_state.retention_requested |= retention_requested;
            maintenance_state.live_rows_delta = maintenance_state
                .live_rows_delta
                .saturating_add(live_rows_delta);
        }

        if self
            .post_write_maintenance
            .scheduled
            .swap(true, Ordering::AcqRel)
        {
            return;
        }

        let table = self.clone_for_write();
        tokio::spawn(async move {
            table.run_post_write_maintenance_loop().await;
        });
    }

    /// Synchronously drain any pending post-write maintenance, including any
    /// iteration the background loop is currently executing.
    ///
    /// Public for two callers:
    ///   1. Tests that assert on the post-retention state (where retention is
    ///      scheduled asynchronously via [`Self::schedule_post_write_maintenance`]
    ///      and runs after a 100 ms debounce by default).
    ///   2. Coordinated shutdown — callers that want to make sure no scheduled
    ///      retention is lost when the table is dropped.
    ///
    /// Loops until both (a) the queued maintenance state is empty AND (b) the
    /// background loop is not active (so no iteration is mid-flight). Within
    /// each pass, queued state is drained synchronously; if retention fails
    /// while flushing, the error is returned instead of re-queueing, avoiding
    /// an unbounded synchronous retry loop during tests or shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error if retention maintenance fails while the explicit flush
    /// is draining queued work.
    pub async fn flush_pending_maintenance(&self) -> CatalogResult<()> {
        loop {
            let state = {
                let mut guard = self.post_write_maintenance.state.lock();
                std::mem::take(&mut *guard)
            };
            if !state.is_empty() {
                self.run_maintenance_state(state, RetentionFailureAction::ReturnError)
                    .await?;
                continue;
            }
            if !self
                .post_write_maintenance
                .scheduled
                .load(Ordering::Acquire)
            {
                return Ok(());
            }
            // The background loop has the state lock and is mid-iteration.
            // Wait briefly and re-check; we cannot drain its work from here,
            // but we can spin until it finishes its current pass.
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    /// Apply one snapshot of accumulated maintenance state.
    ///
    /// Extracted from [`Self::run_post_write_maintenance_loop`] so
    /// [`Self::flush_pending_maintenance`] can reuse the same work.
    ///
    /// Listing-table refresh is deferred until after retention so the pass
    /// rebuilds the listing at most once, even when both
    /// `state.refresh_listing` is set and retention deletes rows.
    pub(super) async fn run_maintenance_state(
        &self,
        state: PostWriteMaintenanceState,
        retention_failure_action: RetentionFailureAction,
    ) -> CatalogResult<()> {
        let had_stats = state.stats.is_some();
        if let Some(stats) = state.stats {
            // The net live-row delta (inserts minus supersedes/deletes) was
            // accumulated alongside the coalesced stats. Retention deletes below
            // are not yet netted here (TPC-H has none); compaction's `Set` reset
            // bounds any resulting drift.
            self.persist_table_stats(&stats, RowCountUpdate::Delta(state.live_rows_delta))
                .await;
        }

        let mut retention_deleted = 0_u64;
        if state.retention_requested {
            match self.apply_retention_filters().await {
                Ok(deleted) => {
                    retention_deleted = deleted;
                    if deleted > 0 {
                        tracing::info!(
                            table = self.table_metadata.table_name.as_str(),
                            "Background retention deleted {deleted} row(s)"
                        );
                    }
                }
                Err(e) => {
                    match retention_failure_action {
                        RetentionFailureAction::Requeue => {
                            // Re-queue so the next debounce cycle retries. A
                            // persistently failing retention scan would
                            // otherwise leave expired rows undeleted
                            // indefinitely; re-queueing makes delivery eventual
                            // and the repeated error log observable. Logged at
                            // `error` (not `warn`) because the retry semantics
                            // turn a single failure into a steady signal worth
                            // alerting on.
                            tracing::error!(
                                table = self.table_metadata.table_name.as_str(),
                                "Background retention scan failed: {e}. Re-queueing for retry."
                            );
                            self.post_write_maintenance.state.lock().retention_requested = true;
                        }
                        RetentionFailureAction::ReturnError => return Err(e),
                    }
                }
            }
        }

        // One refresh per pass, deferred until after retention so deleted
        // rows are reflected in the rebuilt listing table.
        if (state.refresh_listing || retention_deleted > 0)
            && let Err(e) = self.refresh_listing_table().await
        {
            tracing::warn!(
                table = self.table_metadata.table_name.as_str(),
                "Post-write listing refresh failed: {e}"
            );
        }

        // Position capture (deletion_mode: position): once the listing reflects
        // the newly written files, upgrade their keyset entries to
        // `FilePositioned` so subsequent upserts tombstone by position. Runs only
        // when the listing changed (new/rewritten files exist). Best-effort: a
        // failure leaves entries `FileUnlocated`, which correctly falls back to
        // key-based deletes, so it is logged rather than propagated.
        if (state.refresh_listing || retention_deleted > 0)
            && self.should_capture_positions()
            && let Err(e) = self.capture_new_file_positions().await
        {
            tracing::warn!(
                table = self.table_metadata.table_name.as_str(),
                "Position capture pass failed: {e}"
            );
        }

        if state.refresh_listing || had_stats || retention_deleted > 0 {
            self.schedule_post_write_compaction();
        }

        // b1★ (cycle-4): persist any durable tombstone flips that the staged-batch
        // fold left owed. On a busy table the next batch's Stage-A drains these,
        // but an idle table needs this bounded backstop so the durable `published`
        // flag converges within a maintenance debounce instead of only on reopen.
        // No-op when the queue is empty (the common case).
        self.drain_pending_durable_tombstone_flips().await;

        // cycle-8 TASK A2: drain the per-table metastore WAL off the hot commit
        // path. With the inline auto-checkpoint DISABLED
        // (`wal_autocheckpoint_pages = 0`), this background-tick checkpoint is the
        // SOLE WAL drain — no checkpoint ever fires inside a hot Stage-A/Stage-B
        // COMMIT's WAL-write-locked window (which would land a blocking fsync
        // there). It runs on a dedicated connection (never a pool writer slot),
        // is PASSIVE by default (never blocks writers or waits for readers; a busy
        // WAL just leaves frames for the next tick), and self-escalates to
        // TRUNCATE only when the WAL exceeds its size cap. Best-effort: logged,
        // never propagated to fail the originating write. This runs every
        // maintenance debounce, which fires whenever a write schedules
        // maintenance — i.e. continuously under CDC load — so the WAL drains
        // promptly even though the inline backstop is off.
        if let Err(e) = self.catalog.checkpoint_wal().await {
            tracing::warn!(
                table = self.table_metadata.table_name.as_str(),
                "Background WAL checkpoint failed: {e}"
            );
        }

        Ok(())
    }

    pub(super) async fn run_post_write_maintenance_loop(self) {
        loop {
            tokio::time::sleep(POST_WRITE_MAINTENANCE_DEBOUNCE).await;

            let state = {
                let mut guard = self.post_write_maintenance.state.lock();
                std::mem::take(&mut *guard)
            };

            if let Err(e) = self
                .run_maintenance_state(state, RetentionFailureAction::Requeue)
                .await
            {
                tracing::error!(
                    table = self.table_metadata.table_name.as_str(),
                    "Post-write maintenance failed: {e}"
                );
            }

            self.post_write_maintenance
                .scheduled
                .store(false, Ordering::Release);

            if self.post_write_maintenance.state.lock().is_empty() {
                return;
            }

            if self
                .post_write_maintenance
                .scheduled
                .swap(true, Ordering::AcqRel)
            {
                return;
            }
        }
    }
}

#[async_trait::async_trait]
impl super::compaction::CompactionRunner for CayenneTableProvider {
    async fn run_compaction_trigger(&self) -> std::result::Result<bool, String> {
        // Routes to the fast protected-snapshot subset compaction, which only
        // rewrites immutable protected snapshots and CAS-swaps them in the
        // catalog. Key-delete tables can run concurrently with appends because
        // post-fence deletes still apply to the merged snapshot by sequence.
        // Position-delete tables serialize the rewrite inside
        // `compact_protected_snapshots_subset`, because their tombstones are
        // file-path scoped and would otherwise be lost if they target a file
        // that is swapped away by the merge.

        // Cheap lock-free early-out first: skip acquiring `compaction_lock` /
        // `listing_fence` and building a session context unless the protected
        // set already has enough runs to be worth merging. `protected_snapshots`
        // is an `ArcSwap`, so `load()` is a cheap atomic read. The authoritative
        // re-check (and size-tiering) still happens under the fence inside
        // `compact_protected_snapshots_subset`; this guard only avoids wasted
        // work on the common path where nothing has accumulated yet.
        let min_inputs = self.context.compaction_trigger_protected_snapshots().max(2);
        let protected_len = self.protected_snapshots.load().len();
        if protected_len < min_inputs {
            tracing::trace!(
                target: "cayenne::compaction",
                table = self.table_metadata.table_name.as_str(),
                protected_len,
                min_inputs,
                "Skipping fast protected-snapshot compaction: protected set below trigger floor",
            );
            return Ok(false);
        }

        self.compact_protected_snapshots_subset(usize::MAX)
            .await
            .map_err(|e| e.to_string())
    }

    fn compaction_target_name(&self) -> &str {
        &self.table_metadata.table_name
    }

    fn on_background_tick(&self) {
        // Observe the query-health signal — protected-snapshot runs a scan must
        // merge (read amplification, the ingest→query coupling) — and cgroup
        // memory pressure, so the controller's snapshot reflects fresh data.
        let read_amp = self.protected_snapshots.load().len();
        self.context.observe_environment(read_amp);

        // Emit observability gauges every tick (regardless of whether dynamic
        // tuning is enabled — the accounting is always recorded).
        let table = self.table_metadata.table_name.clone();
        let snap = self.context.ingest_snapshot();
        let knobs = self.context.live_knob_values();
        telemetry::track_cayenne_autotune_state(
            &telemetry::CayenneAutotuneState {
                rows_per_sec: snap.rows_per_sec,
                bytes_per_sec: snap.bytes_per_sec,
                apply_vs_arrival: snap.apply_vs_arrival,
                read_amp: u64::try_from(read_amp).unwrap_or(0),
                mem_pressure: snap.mem_pressure.unwrap_or(-1.0),
                apply_ms: snap.apply_ms,
                inline_flush_max_bytes: u64::try_from(knobs.inline_flush_max_bytes.max(0))
                    .unwrap_or(0),
                compaction_interval_ms: knobs.compaction_background_interval_ms,
                compaction_trigger_files: u64::try_from(knobs.compaction_trigger_files)
                    .unwrap_or(0),
                target_file_size_mb: u64::try_from(
                    self.context.target_file_size_bytes() / (1024 * 1024),
                )
                .unwrap_or(0),
                write_concurrency: u64::try_from(knobs.write_concurrency).unwrap_or(0),
            },
            &[telemetry::KeyValue::new("table", table.clone())],
        );

        // The closed-loop control step. A no-op when dynamic tuning is disabled
        // (returns `None`); otherwise applies at most one bounded knob change.
        if let Some(adj) = self.context.retune(super::tuning::MIN_DWELL) {
            telemetry::track_cayenne_autotune_adjustment(&[
                telemetry::KeyValue::new("table", table.clone()),
                telemetry::KeyValue::new("knob", adj.knob.as_str()),
            ]);
            tracing::info!(
                target: "cayenne::tuning",
                table = table.as_str(),
                knob = adj.knob.as_str(),
                new_value = adj.new_value,
                reason = adj.reason,
                "Cayenne dynamic auto-tune adjustment applied",
            );
        }
    }

    fn background_interval_hint(&self) -> Option<std::time::Duration> {
        self.context.compaction_background_interval()
    }
}

impl CayenneTableProvider {
    /// Spawn the background compaction task for this provider, if not already
    /// spawned and if the configured interval is non-zero.
    /// Must be called after the provider has been wrapped in an `Arc` — the
    /// scheduler holds a `Weak<Self>` so it does not extend the provider's
    /// lifetime. The returned compactor is owned by the provider itself
    /// (stored in `background_compactor`); when the last `Arc` to the provider
    /// is dropped, the compactor drops and the task aborts.
    ///
    /// Returns `true` if a task was spawned by this call, `false` otherwise
    /// (interval = 0, or a previous call already spawned one).
    pub fn spawn_background_compaction(
        self: &Arc<Self>,
        semaphore: Arc<tokio::sync::Semaphore>,
    ) -> bool {
        if self.background_compactor.get().is_some() {
            return false;
        }
        let Some(interval) = self.context.compaction_background_interval() else {
            return false;
        };
        let Some(compactor) = super::compaction::BackgroundCompactor::spawn(
            Arc::downgrade(self) as std::sync::Weak<dyn super::compaction::CompactionRunner>,
            interval,
            semaphore,
        ) else {
            return false;
        };
        // OnceLock::set fails only if already initialized — race here is fine,
        // the lost compactor drops and aborts its own task.
        self.background_compactor.set(compactor).is_ok()
    }

    /// Spawn the periodic mem-tier checkpoint task for this provider, if memory
    /// mode is active, the configured interval is non-zero, and no task is
    /// already spawned. Must be called after the provider is wrapped in an `Arc`
    /// (the scheduler holds a `Weak<Self>`); the task is owned by the provider
    /// (stored in `background_mem_tier_checkpointer`) and aborts when the last
    /// `Arc` drops. A no-op (`false`) for file-mode tables and partitioned
    /// tables (which are never `is_cdc_memory_mode()`).
    ///
    /// Returns `true` if a task was spawned by this call, `false` otherwise.
    #[must_use]
    pub fn spawn_background_mem_tier_checkpoint(self: &Arc<Self>) -> bool {
        if self.background_mem_tier_checkpointer.get().is_some() {
            return false;
        }
        // Gate on memory mode so file-mode and partitioned tables spawn nothing.
        // The runtime arms the slot advancer lazily on the first replayable
        // burst, so we do NOT gate on `has_slot_advancer()` here (it would be
        // false at spawn time and the table would never get a checkpointer);
        // `run_mem_tier_checkpoint_tick` re-checks the advancer each tick.
        if !self.is_cdc_memory_mode() {
            return false;
        }
        let Some(interval) = self.context.mem_tier_checkpoint_interval() else {
            return false;
        };
        let Some(checkpointer) = super::compaction::BackgroundMemTierCheckpointer::spawn(
            Arc::downgrade(self) as std::sync::Weak<dyn super::compaction::MemTierCheckpointRunner>,
            interval,
        ) else {
            return false;
        };
        // OnceLock::set fails only if already initialized — race here is fine,
        // the lost checkpointer drops and aborts its own task.
        self.background_mem_tier_checkpointer
            .set(checkpointer)
            .is_ok()
    }
}

#[async_trait::async_trait]
impl super::compaction::MemTierCheckpointRunner for CayenneTableProvider {
    async fn run_mem_tier_checkpoint_tick(&self) {
        // Only memory-mode tables that the runtime has armed have a deferred
        // slot ack to advance; everything else has nothing to flush here.
        if !self.is_cdc_memory_mode() || !self.has_slot_advancer() {
            return;
        }
        // Cheap lock-free early-out: skip taking the checkpoint lock on an idle
        // tick. `checkpoint_mem_tier` also early-returns `Ok(0)` on an empty
        // tier, so this is purely to avoid contending the lock with the write
        // path when there is nothing to do.
        if self.mem_tier.load().is_empty() {
            return;
        }
        // Serialize against the write-path spill and the event-driven
        // checkpoints (all take this lock) so two checkpoints for one table can
        // never run concurrently. A blocking `lock()` (not `try_lock`) is correct:
        // if a spill is mid-flight, this tick simply waits and then finds an
        // empty/smaller tier — it never races the clear.
        let _guard = self.mem_checkpoint_lock.lock().await;
        if let Err(e) = self.checkpoint_mem_tier().await {
            // A failed checkpoint must NOT advance the slot — `checkpoint_mem_tier`
            // already guarantees that (the advancer fires only post-fence). The
            // deferred committers stay queued and the next tick retries; surface
            // it as a warning rather than flipping refresh status from a
            // background task.
            tracing::warn!(
                target: "cayenne::mem_tier",
                table = %self.table_metadata.table_name,
                "Periodic mem-tier checkpoint failed (deferred slot ack not advanced; will retry next tick): {e}"
            );
        }
    }

    fn mem_tier_checkpoint_target_name(&self) -> &str {
        &self.table_metadata.table_name
    }
}
