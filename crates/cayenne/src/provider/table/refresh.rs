//! Session contexts, retention filters, deletion-cache refresh, and listing refresh.
//!
//! Listing refresh is the core: `refresh_listing_table` swaps the
//! `ListingTable` under `listing_fence.write()` (the `_under_held_fence`
//! variant assumes the caller holds it, e.g. the cross-partition coordinator),
//! and `publish_current_snapshot_files_changed_under_held_fence` delta-applies
//! or evicts the `DataFusion` list-files cache. Retention runs via
//! `apply_retention_filters` (the sink serializes on `write_lock` internally);
//! `refresh()` reloads deletion vectors / protected snapshots / the snapshot id
//! from the catalog while holding `write_lock`.
//!
//! Mechanically split out of `provider/table.rs`; code is moved verbatim.

use datafusion_execution::cache::TableScopedPath;
use datafusion_physical_plan::ExecutionPlan;

use super::{
    Arc, CachedFileList, CatalogError, CatalogResult, CayenneDeletionSink, CayenneTableProvider,
    DFSchema, Error, ExecutionProps, Expr, FilterExec, HashMap, HashSet, Int64PkDeletionSnapshot,
    ListingTableUrl, ObjectMeta, ObjectStoreExt, ObjectStorePath, Ordering,
    PkDeletionStrategyWithCache, PositionBitmap, Result, RowConverterDeletionSnapshot, RuntimeEnv,
    STAGING_WAL_FILENAME, SessionConfig, SessionContext, conjunction,
};

impl CayenneTableProvider {
    /// Create the `SessionContext` for Cayenne-internal writes and maintenance
    /// (snapshot writes, compaction, keyset/deletion scans), backed by the
    /// shared `RuntimeEnv`.
    ///
    /// The shared `RuntimeEnv` (from [`CayenneContext`]) already has the S3 object
    /// store registered during construction, so all sessions created here inherit
    /// it automatically. This also shares the `list_files` cache and other
    /// runtime-level caches with the main Spice query engine.
    ///
    /// Deliberately built from `SessionConfig::default()` rather than the
    /// operator's query session, so `target_partitions` resolves to the host's
    /// available parallelism (≈ logical CPU count). On the write path this value
    /// is the **parallel-encode shard ceiling**: `VortexFormat::build_shard_spec`
    /// clamps the requested `cayenne_write_concurrency` to it. Encoding a snapshot
    /// is CPU-bound, so allowing more shards than cores buys no encode throughput
    /// and only inflates the per-snapshot file count (read amplification) — hence
    /// the operator's `query.target_partitions` is a read-path knob and is
    /// intentionally NOT inherited here; raising it would not speed the encode.
    /// Per-table write fan-out is requested via `cayenne_write_concurrency` (and
    /// capped at this ceiling); object-store upload concurrency, which *can*
    /// usefully exceed the core count, is governed separately by
    /// `cayenne_upload_concurrency`.
    pub(super) fn create_session_context(&self) -> SessionContext {
        SessionContext::new_with_config_rt(
            SessionConfig::default(),
            Arc::clone(self.context.runtime_env()),
        )
    }

    /// Like [`Self::create_session_context`], but backed by the dedicated
    /// compaction memory environment when one has been injected (Cayenne
    /// configured + dedicated thread pools enabled).
    ///
    /// Compaction thereby accounts its working memory against a separate,
    /// bounded pool carved from `runtime.query.memory_limit`, so a large
    /// snapshot rewrite cannot starve concurrent queries. Falls back to the
    /// shared query environment when no dedicated compaction env is set.
    pub(super) fn create_compaction_session_context(&self) -> SessionContext {
        let runtime_env = super::compaction::compaction_runtime_env()
            .unwrap_or_else(|| Arc::clone(self.context.runtime_env()));
        SessionContext::new_with_config_rt(SessionConfig::default(), runtime_env)
    }

    /// Wrap a plan with a `FilterExec` that enforces the retention filter.
    ///
    /// Snapshot file-scan planning follows `ListingTable` semantics for
    /// non-partition filters: they only influence the file-limit heuristic, not
    /// the actual scan. Adding a `FilterExec` above `DataSourceExec` allows
    /// `DataFusion`'s physical optimizer to push the predicate into
    /// `VortexSource::try_pushdown_filters`, enabling file-level pruning via
    /// min/max stats and row-level filtering.
    pub(super) fn wrap_plan_with_retention_filter(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        retention_filter: &Expr,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, datafusion_common::DataFusionError> {
        let arrow_schema = plan.schema();
        let df_schema = DFSchema::try_from(arrow_schema.as_ref().clone())?;
        let execution_props = ExecutionProps::new();

        let physical_filter = datafusion_physical_expr::create_physical_expr(
            retention_filter,
            &df_schema,
            &execution_props,
        )?;

        let filter_exec = FilterExec::try_new(physical_filter, plan)?;

        tracing::trace!(
            table = %self.table_metadata.table_name,
            filter = %retention_filter,
            "Applied retention_filter FilterExec at scan time"
        );

        Ok(Arc::new(filter_exec))
    }

    /// Wrap an in-memory scan branch (the durable inline corpus or the RAM CDC
    /// tier) with a `FilterExec` applying the query's scan-level `filters`.
    ///
    /// # Why
    ///
    /// The file-backed branches of a Cayenne scan get their predicate at
    /// execution time from `VortexSource::try_pushdown_filters` (`DataFusion`'s
    /// physical optimizer pushes the post-scan `FilterExec` into the source).
    /// The inline / RAM-tier branches are `MemorySourceConfig` execs, which do
    /// NOT support filter pushdown — so without this wrapper the post-scan
    /// `FilterExec` can never be removed (its `if_all` pushdown fails on the
    /// un-filterable memory branch) and survives, re-filtering *every* row in
    /// the union, including the already-pruned file output.
    ///
    /// Adding the predicate as a `FilterExec` directly above each memory branch
    /// makes that branch report the parent filters as fully handled during
    /// pushdown, so the redundant post-scan `FilterExec` is dropped while the
    /// inline / RAM rows are still correctly filtered. The predicate is the
    /// query's own filter, evaluated by a real `FilterExec`, so it applies to
    /// every returned row exactly — preserving correctness under active inline
    /// CDC.
    ///
    /// Best-effort: if the predicate cannot be built against this branch's
    /// (projected) schema, the branch is returned unwrapped. Correctness still
    /// holds because the post-scan `FilterExec` then survives and filters it;
    /// only the redundant-filter optimization is skipped for that scan.
    pub(super) fn wrap_memory_branch_with_scan_filters(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        filters: &[Expr],
    ) -> Arc<dyn ExecutionPlan> {
        if filters.is_empty() {
            return plan;
        }

        let arrow_schema = plan.schema();
        let Ok(df_schema) = DFSchema::try_from(arrow_schema.as_ref().clone()) else {
            return plan;
        };
        let execution_props = ExecutionProps::new();

        let Some(predicate) = conjunction(filters.iter().cloned()) else {
            return plan;
        };

        match datafusion_physical_expr::create_physical_expr(
            &predicate,
            &df_schema,
            &execution_props,
        )
        .and_then(|physical_filter| FilterExec::try_new(physical_filter, Arc::clone(&plan)))
        {
            Ok(filter_exec) => {
                tracing::trace!(
                    table = %self.table_metadata.table_name,
                    "Applied scan filters to in-memory CDC branch so the post-scan FilterExec can be dropped"
                );
                Arc::new(filter_exec)
            }
            Err(e) => {
                tracing::trace!(
                    table = %self.table_metadata.table_name,
                    error = %e,
                    "Could not pre-filter in-memory CDC branch; leaving post-scan FilterExec to filter it"
                );
                plan
            }
        }
    }

    /// Apply retention filters by running the configured delete sink against
    /// the current table state.
    ///
    /// The sole caller is the post-write maintenance loop (see
    /// [`Self::run_maintenance_state`]), which runs outside any writer's
    /// `write_lock`. The deletion sink is built with
    /// `Some(Arc::clone(&self.write_lock))` so the sink itself serializes
    /// against concurrent inserts / listing refreshes for the duration of the
    /// scan — same exclusion guarantee the inline-retention path used to
    /// provide, just held inside the sink rather than the writer.
    pub(crate) async fn apply_retention_filters(&self) -> CatalogResult<u64> {
        use data_components::delete::DeletionSink;

        if self.retention_filters.is_empty() {
            return Ok(0);
        }

        let filters = self.retention_filters.clone();
        let sink = CayenneDeletionSink::new(
            self.table_metadata.clone(),
            Arc::clone(&self.catalog),
            Arc::clone(&self.listing_table),
            Arc::clone(&self.table_metadata.schema),
            &filters,
            self.pk_deletion_strategy.clone(),
            Arc::clone(&self.table_memory),
            self.pk_row_converter.as_ref().map(Arc::clone),
            self.pk_column_indices.clone(),
            Vec::new(), // Retention filters don't need to scan protected snapshots
            Arc::clone(self.context.runtime_env()),
            Some(Arc::clone(&self.write_lock)),
            Arc::clone(&self.seq_allocator),
        );

        let deleted_count =
            sink.delete_from()
                .await
                .map_err(|err| CatalogError::InvalidOperation {
                    message: "Failed to execute retention filters.".to_string(),
                    source: err,
                })?;

        // Refresh deletion cache after applying retention filters
        if deleted_count > 0 {
            self.clear_cached_pk_keyset();
            if self.pk_deletion_strategy.is_position_based() {
                self.clear_scan_file_statistics_cache();
            }
            self.refresh_deletion_cache().await?;
        }

        Ok(deleted_count)
    }

    /// Refresh the cached deletion vectors by reloading from the catalog.
    ///
    /// This should be called after operations that modify deletion vectors:
    /// - After applying retention filters
    /// - After manual delete operations
    /// - After compaction that removes deleted rows
    ///
    /// # Errors
    ///
    /// Returns an error if deletion vectors cannot be loaded from the catalog.
    pub(super) async fn refresh_deletion_cache(&self) -> CatalogResult<()> {
        let fresh_strategy = Self::load_deletion_vectors_all(
            &self.table_metadata.table_id,
            Arc::clone(&self.catalog),
            self.pk_deletion_strategy.strategy(),
        )
        .await?;

        self.pk_deletion_strategy
            .refresh_from(&fresh_strategy, &self.table_metadata.table_name)?;
        self.refresh_deletion_memory_accounting();
        self.clear_cached_pk_keyset();

        tracing::debug!(
            "Refreshed deletion cache for table {} (strategy: {:?})",
            self.table_metadata.table_name,
            self.pk_deletion_strategy.strategy(),
        );

        Ok(())
    }

    /// Check if there are pending deletions based on the current deletion strategy.
    ///
    /// This is used to determine if inserts need special handling:
    /// - Position-based deletions use per-file deletion vectors (no special handling needed)
    /// - PK-based deletions use anti-deletions (write to new snapshot with higher sequence)
    ///
    pub(crate) fn has_pending_deletions(&self) -> bool {
        match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::PositionBased {
                cached_deleted_row_ids,
            } => !cached_deleted_row_ids.load().is_empty(),
            PkDeletionStrategyWithCache::Int64Pk {
                deletion_snapshot, ..
            } => deletion_snapshot.load().tombstones.has_deletions(),
            PkDeletionStrategyWithCache::RowConverterBased {
                deletion_snapshot, ..
            } => deletion_snapshot.load().tombstones.has_deletions(),
        }
    }

    /// Returns a reference to the primary key deletion strategy and its caches.
    #[must_use]
    pub(crate) fn pk_deletion_strategy(&self) -> &PkDeletionStrategyWithCache {
        &self.pk_deletion_strategy
    }

    /// Clear all cached deletion vectors and insert records.
    ///
    /// This should be called after compaction operations that have applied all deletions
    /// and written a clean snapshot.
    ///
    pub(crate) fn clear_all_deletion_caches(&self) {
        // Clear caches based on the current strategy.
        // ArcSwap stores publish a fresh empty snapshot atomically; readers see either
        // the old or new state and never block.
        match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::PositionBased {
                cached_deleted_row_ids,
            } => {
                cached_deleted_row_ids.store(Arc::new(HashMap::new()));
            }
            PkDeletionStrategyWithCache::Int64Pk {
                deletion_snapshot,
                position_deletions,
            } => {
                deletion_snapshot.store(Arc::new(Int64PkDeletionSnapshot::empty()));
                position_deletions.store(Arc::new(PositionBitmap::new()));
            }
            PkDeletionStrategyWithCache::RowConverterBased {
                deletion_snapshot,
                position_deletions,
            } => {
                deletion_snapshot.store(Arc::new(RowConverterDeletionSnapshot::empty()));
                position_deletions.store(Arc::new(PositionBitmap::new()));
            }
        }

        // Clear protected snapshots - after compaction all data is in the main snapshot
        self.protected_snapshots.store(Arc::new(HashMap::new()));

        self.clear_cached_pk_keyset();

        // Compaction folded the deletions into rewritten files, so the in-memory
        // key/position delete state is empty again — release its reservation.
        // (clear_cached_pk_keyset already reset the keyset's reservation.)
        self.table_memory.set_deletion_bytes(0);

        tracing::debug!(
            "Cleared all deletion and insert records caches for table {}",
            self.table_metadata.table_name
        );
    }

    /// Drop the in-memory inline-memtable view: zero the cached row count
    /// and bump `inlined_generation` so the next scan rebuilds from the
    /// (presumably now-empty) catalog.
    ///
    /// Call this after a catalog operation that wipes
    /// `cayenne_inlined_data` / `cayenne_inlined_delete` rows for the
    /// table outside the inline-mutation path — e.g. `commit_overwrite`,
    /// which clears those tables atomically with the snapshot pointer
    /// flip but does not flow through `commit_inlined_data_mutation`.
    /// Without this bump, scans keep serving the pre-overwrite cache and
    /// row counts read high (old inline rows + new snapshot rows).
    ///
    /// STRUCTURAL: the underlying corpus was wiped/replaced wholesale, so the
    /// next miss must full-rebuild — the append-only delta path (which assumes
    /// the cached entries are still a valid base) would be unsound.
    pub(crate) fn invalidate_inlined_cache(&self) {
        self.inlined_row_count.store(0, Ordering::Relaxed);
        // cycle-5 TASK 1: the corpus was wiped/replaced, so pending tombstone
        // removals reference rows that no longer exist — drop them. The structural
        // bump below fences off any concurrent delta cache built against the old
        // base (it carries the old epoch and is rejected on the next read), so a
        // racing delta that already snapshotted the queue cannot mis-store.
        self.pending_tombstone_deltas.lock().drain_through(u64::MAX);
        self.bump_inlined_structural_epoch();
    }

    /// Get the current snapshot ID.
    ///
    /// This returns the live snapshot ID which may differ from `table_metadata.current_snapshot_id`
    /// after compaction operations.
    ///
    pub(in crate::provider) fn get_current_snapshot_id(&self) -> String {
        let guard = self.current_snapshot_id.read();
        guard.clone()
    }

    /// Update the current snapshot ID after a compaction operation.
    ///
    /// This must be called after `commit_compaction` to keep the in-memory snapshot ID
    /// in sync with the catalog.
    ///
    pub(crate) fn update_current_snapshot_id(&self, new_snapshot_id: &str) {
        {
            let mut guard = self.current_snapshot_id.write();
            if guard.as_str() != new_snapshot_id {
                *guard = new_snapshot_id.to_string();
            }
        }

        // Any snapshot rewrite (compaction, sort, etc.) means the "new files
        // since last compaction" counter should be reset. The next accumulation
        // phase starts from a clean slate.
        self.new_files_since_last_compaction
            .store(0, Ordering::Relaxed);
        tracing::debug!(
            "Updated current snapshot ID for table {} to {}",
            self.table_metadata.table_name,
            new_snapshot_id
        );
    }

    /// Refresh in-memory query state by reloading from the catalog (source of truth).
    ///
    /// This keeps existing `Arc<CayenneTableProvider>` handles usable after catalog refreshes
    /// by updating mutable state in place instead of swapping provider objects.
    ///
    /// Acquires the write lock to prevent racing with in-progress writes/deletes. While holding
    /// the lock, reloads deletion vectors, protected snapshots, and the listing table
    /// directly from the catalog — NOT from the `source` provider, which may contain
    /// stale state captured before the lock was acquired.
    ///
    /// The `source` parameter is used to validate that the table ID matches.
    ///
    /// # Errors
    ///
    /// Returns an error if `source` refers to a different table (mismatched table IDs)
    /// or if reloading from the catalog fails.
    pub async fn refresh(&self, source: &Self) -> Result<()> {
        if self.table_metadata.table_id != source.table_metadata.table_id {
            return Err(Error::Internal {
                table: self.table_metadata.table_name.clone(),
                message: format!(
                    "Cannot refresh table {} from different table {}",
                    self.table_metadata.table_id, source.table_metadata.table_id,
                ),
            });
        }

        // Acquire the write lock so no insert/delete is in-flight while we reload state.
        let _write_guard = self.write_lock.lock().await;

        // Reload deletion vectors from the catalog (SQLite) — the source of truth.
        // This picks up any deletions committed by writes that completed after the
        // source provider was opened.
        let fresh_strategy = Self::load_deletion_vectors_all(
            &self.table_metadata.table_id,
            Arc::clone(&self.catalog),
            self.pk_deletion_strategy.strategy(),
        )
        .await
        .map_err(|e| Error::Internal {
            table: self.table_metadata.table_name.clone(),
            message: format!("Failed to reload deletion vectors during refresh: {e}"),
        })?;

        self.pk_deletion_strategy
            .refresh_from(&fresh_strategy, &self.table_metadata.table_name)?;
        self.clear_cached_pk_keyset();

        // Reload protected snapshots from the catalog.
        let fresh_protected_snapshots = Self::load_protected_snapshots(
            Arc::clone(&self.catalog),
            &self.table_metadata.table_id,
            &self.pk_deletion_strategy,
        )
        .await
        .map_err(|e| Error::Internal {
            table: self.table_metadata.table_name.clone(),
            message: format!("Failed to reload protected snapshots during refresh: {e}"),
        })?;

        self.protected_snapshots
            .store(Arc::new(fresh_protected_snapshots));

        // Reload the current snapshot ID from the catalog.
        let fresh_metadata = self
            .catalog
            .get_table(&self.table_metadata.table_name)
            .await
            .map_err(|e| Error::Internal {
                table: self.table_metadata.table_name.clone(),
                message: format!("Failed to reload table metadata during refresh: {e}"),
            })?;
        self.update_current_snapshot_id(&fresh_metadata.current_snapshot_id);

        // Rebuild the listing table from the fresh snapshot ID on disk.
        self.refresh_listing_table().await?;

        tracing::debug!(
            "Refreshed in-memory state for table {} from catalog",
            self.table_metadata.table_name
        );

        Ok(())
    }

    /// Delete rows matching the given primary key values.
    ///
    /// # Errors
    ///
    /// Returns an error as this operation is not yet implemented.
    pub fn delete_by_primary_key(&self, _key_values: Vec<Vec<u8>>) -> Result<u64> {
        // Implementation would:
        // 1. Scan data files for matching primary keys
        // 2. Create/update deletion vectors
        // 3. Write deletion vector files
        // 4. Add delete file entries to catalog
        // 5. Return number of rows deleted
        Err(Error::Unsupported {
            operation: "delete_by_primary_key",
        })
    }

    /// Update rows matching the given primary key values.
    ///
    /// # Errors
    ///
    /// Returns an error as this operation is not yet implemented.
    pub fn update_by_primary_key(
        &self,
        _key_values: Vec<Vec<u8>>,
        _new_values: Vec<arrow::array::RecordBatch>,
    ) -> Result<u64> {
        // Implementation would:
        // 1. Delete old rows using deletion vectors
        // 2. Insert new rows
        // 3. Return number of rows updated
        Err(Error::Unsupported {
            operation: "update_by_primary_key",
        })
    }

    /// Refresh the underlying `ListingTable` to pick up new files and update statistics.
    ///
    /// This method should be called after insert operations to ensure that:
    /// - The `ListingTable` discovers newly written Vortex files
    /// - Table statistics (row counts, column stats) are updated and aggregated across all files
    /// - Query plans can use fresh statistics for optimization (partition pruning, filter pushdown)
    ///
    /// # Statistics Handling
    ///
    /// Vortex automatically computes column statistics (min, max, `null_count`, `distinct_count`) when
    /// writing files. These statistics are embedded in Vortex file footers. The `ListingTable`
    /// aggregates these statistics across all files to provide table-level statistics to `DataFusion`'s
    /// query optimizer.
    ///
    /// When `sort_columns` is configured, sorted data produces tighter min/max bounds, making
    /// zone map pruning more effective for range queries.
    ///
    /// # Errors
    ///
    /// Returns an error if the listing table cannot be refreshed.
    pub(crate) async fn refresh_listing_table(&self) -> Result<()> {
        // Acquire the listing fence for the duration of the swap. Single-partition
        // path; the cross-partition append coordinator (issue #10125 step 6)
        // uses `refresh_listing_table_under_held_fence` instead so it can hold
        // every participating partition's fence across one barrier window.
        let _fence = self.listing_fence.write().await;
        self.refresh_listing_table_under_held_fence().await
    }

    /// Drop every entry in [`Self::scan_file_statistics`].
    ///
    /// Calls must follow any operation that adds, removes, or updates
    /// position-based deletion vectors so the next stats-driven query (e.g.
    /// `COUNT(*)`) reinvokes `infer_stats`, which in turn reapplies the
    /// `VortexAccessPlanProvider` and observes the fresh deletion bitmap.
    pub(crate) fn invalidate_scan_file_statistics(&self) {
        self.scan_file_statistics.clear();
    }

    /// Refresh the listing table, ASSUMING the caller already holds
    /// [`Self::listing_fence`] for write.
    ///
    /// Cross-partition coordinators (#10125 step 6) lock every participating
    /// partition's fence in sorted order and call this method on each so the
    /// listing-table swap happens under one combined barrier. Single-partition
    /// callers should use [`Self::refresh_listing_table`].
    ///
    /// # Errors
    ///
    /// Returns an error if the listing table cannot be reconstructed.
    pub(crate) async fn refresh_listing_table_under_held_fence(&self) -> Result<()> {
        // Construct URL to current snapshot using the live snapshot ID
        // (which may differ from table_metadata after compaction)
        let current_snapshot = self.get_current_snapshot_id();
        let snapshot_dir_url = Self::snapshot_dir_url(
            &self.table_metadata.path,
            &self.table_metadata.table_id,
            &current_snapshot,
        );

        // Invalidate the list-files cache for the snapshot directory so the next
        // scan discovers newly written files
        Self::invalidate_list_files_cache(self.context.runtime_env(), &snapshot_dir_url);

        let new_listing_table = Self::create_listing_table(
            &snapshot_dir_url,
            Arc::<arrow_schema::Schema>::clone(&self.table_metadata.schema),
            self.context.file_format(),
            &self.pk_deletion_strategy,
        )?;

        self.listing_table.store(new_listing_table);

        if let Err(error) = self
            .catalog
            .clear_snapshot_file_statistics_except(&self.table_metadata.table_id, &current_snapshot)
            .await
        {
            tracing::debug!(
                table = %self.table_metadata.table_name,
                error = %error,
                "Failed to clear stale per-file snapshot statistics after listing refresh"
            );
        }

        tracing::debug!(
            "Refreshed listing table for {} (under held fence) to pick up new files",
            self.table_metadata.table_name
        );

        Ok(())
    }

    /// Publish file additions/removals in the current snapshot without
    /// rebuilding the `ListingTable` object.
    ///
    /// Query scan planning lists snapshot files directly through `DataFusion`'s
    /// list-files cache. The table path is unchanged for ordinary append commits,
    /// so making the newly moved files visible only requires updating that cache.
    ///
    /// # Incremental contract
    ///
    /// When the preceding current-snapshot move recorded the exact files it added
    /// (in `last_moved_snapshot_files`) AND a listing is already cached for this
    /// snapshot directory, the additions are merged onto the cached listing
    /// (DELTA-APPLY) — avoiding a full re-LIST of a directory that, at high
    /// read-amplification, holds hundreds–thousands of Vortex files. Otherwise
    /// (no recorded additions — a compaction/overwrite refresh or a standalone
    /// publish — or a cold cache, or any inconsistency) the whole directory entry
    /// is EVICTED so the next scan lists fresh. File REMOVALS (compaction,
    /// retention) always go through the evict path. The recorded additions are
    /// `take()`n here, so they are consumed exactly once and a later publish
    /// without a preceding move can never apply a stale delta.
    ///
    /// Both the recording move and this consume run inside the SAME held
    /// `listing_fence.write()`, so the hand-off cannot race a scan (which holds
    /// `listing_fence.read()` across its listing call) and cannot cross a fence
    /// boundary.
    pub(crate) fn publish_current_snapshot_files_changed_under_held_fence(&self) {
        let current_snapshot = self.get_current_snapshot_id();
        let snapshot_dir_url = Self::snapshot_dir_url(
            &self.table_metadata.path,
            &self.table_metadata.table_id,
            &current_snapshot,
        );

        // Consume any additions the preceding move recorded (take-and-clear).
        // Re-check the recorded snapshot id against the live current snapshot: a
        // stale entry left by a move whose publish was skipped must never be
        // applied onto a different snapshot's listing.
        let additions = self.last_moved_snapshot_files.lock().take();
        let applied_delta = match additions {
            Some((recorded_snapshot, metas))
                if recorded_snapshot == current_snapshot && !metas.is_empty() =>
            {
                Self::apply_list_files_cache_additions(
                    self.context.runtime_env(),
                    &snapshot_dir_url,
                    &metas,
                )
            }
            _ => false,
        };

        // Fallback: evict the whole directory entry so the next scan re-LISTs.
        // Always taken when the delta could not be applied (cold cache, no
        // recorded additions, removals, or a parse miss).
        if !applied_delta {
            Self::invalidate_list_files_cache(self.context.runtime_env(), &snapshot_dir_url);
        }

        telemetry::track_cayenne_list_files_cache_publish(
            applied_delta,
            &[telemetry::KeyValue::new(
                "table",
                self.table_metadata.table_name.clone(),
            )],
        );
        tracing::trace!(
            table = self.table_metadata.table_name.as_str(),
            snapshot_id = current_snapshot.as_str(),
            mode = if applied_delta { "delta" } else { "evict" },
            "Published current snapshot file changes"
        );
    }

    /// Merge `additions` onto the cached list-files entry for
    /// `snapshot_dir_url` instead of evicting it, returning `true` iff the delta
    /// was applied.
    ///
    /// Returns `false` (so the caller falls back to a full eviction + re-LIST)
    /// when the list cache is absent, the URL cannot be parsed, or — critically —
    /// there is NO existing entry for this directory. A cold-cache miss must not
    /// seed a partial listing (it would hide every pre-existing file from the
    /// next scan); the evict fallback lets the next scan LIST the full directory.
    ///
    /// When an entry exists, the new metas are appended to its files (deduped by
    /// `location`, so a re-published file is not double-listed) and `put` back.
    /// Listing-cache filtering is prefix-based and order-independent, so no sort
    /// is required. On the pinned `DataFusion` fork the `ListFilesCache` value
    /// type is `CachedFileList` (a wrapper around `Arc<Vec<ObjectMeta>>`); the
    /// non-extra `CacheAccessor::{get,put}` variants are used here.
    pub(super) fn apply_list_files_cache_additions(
        runtime_env: &Arc<RuntimeEnv>,
        snapshot_dir_url: &str,
        additions: &[ObjectMeta],
    ) -> bool {
        let Some(cache) = runtime_env.cache_manager.get_list_files_cache() else {
            return false;
        };
        let Ok(table_url) = ListingTableUrl::parse(snapshot_dir_url) else {
            return false;
        };
        let key = TableScopedPath {
            table: None,
            path: table_url.prefix().clone(),
        };

        // Only delta-apply onto an EXISTING listing. A cold miss falls back to
        // eviction so the next scan lists the whole directory (seeding a partial
        // listing here would drop every file not in `additions`).
        let Some(existing) = cache.get(&key) else {
            return false;
        };

        let mut files: Vec<ObjectMeta> = existing.files.as_ref().clone();
        let mut seen: HashSet<ObjectStorePath> = files.iter().map(|m| m.location.clone()).collect();
        for meta in additions {
            if seen.insert(meta.location.clone()) {
                files.push(meta.clone());
            }
        }

        cache.put(&key, CachedFileList::new(files));
        true
    }

    /// Acquire the listing fence and publish current-snapshot file changes.
    #[doc(hidden)]
    pub async fn publish_current_snapshot_files_changed(&self) {
        let _fence = self.listing_fence.write().await;
        self.publish_current_snapshot_files_changed_under_held_fence();
    }

    /// Acquire `listing_fence` for write and return an owned guard.
    ///
    /// Used by the cross-partition append coordinator (#10125 step 6) so it
    /// can hold fences across every participating partition for the duration
    /// of one barrier window.
    pub async fn lock_listing_fence_write_owned(&self) -> tokio::sync::OwnedRwLockWriteGuard<()> {
        Arc::clone(&self.listing_fence).write_owned().await
    }

    /// Return the absolute path to the table's data root. Used by the
    /// cross-partition coordinator to derive the top-level partitioned-WAL
    /// directory (`<table_root>/_partitioned_wal/`).
    #[must_use]
    pub fn table_path_str(&self) -> &str {
        &self.table_metadata.path
    }

    #[must_use]
    pub(crate) fn staging_wal_path_for_recovery_for(
        &self,
        staging_snapshot_id: &str,
    ) -> std::path::PathBuf {
        let staging_dir = Self::snapshot_dir_path(
            &self.table_metadata.path,
            &self.table_metadata.table_id,
            staging_snapshot_id,
        );
        staging_dir.join(STAGING_WAL_FILENAME)
    }

    /// Invalidate the `list_files_cache` entry for the given snapshot directory URL.
    ///
    /// `DataFusion`'s `ListingTableUrl` caches directory listings in the `RuntimeEnv`'s
    /// `CacheManager` with infinite TTL. After files are added or removed from a
    /// snapshot directory, the stale cache entry must be evicted so the next scan
    /// lists files fresh from the filesystem / object store.
    pub(crate) fn invalidate_list_files_cache(
        runtime_env: &Arc<RuntimeEnv>,
        snapshot_dir_url: &str,
    ) {
        let Some(cache) = runtime_env.cache_manager.get_list_files_cache() else {
            return;
        };

        // Parse the URL the same way `ListingTableUrl::parse` does to derive
        // the `object_store::path::Path` prefix used as the cache key.
        let Ok(table_url) = ListingTableUrl::parse(snapshot_dir_url) else {
            tracing::warn!(
                "Failed to parse snapshot URL for cache invalidation: {snapshot_dir_url}"
            );
            return;
        };

        let key = TableScopedPath {
            table: None,
            path: table_url.prefix().clone(),
        };

        if cache.remove(&key).is_some() {
            tracing::debug!("Invalidated list-files cache for {snapshot_dir_url}");
        }
    }
}
