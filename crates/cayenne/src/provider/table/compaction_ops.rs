//! Snapshot compaction passes and protected-snapshot subset compaction.
//!
//! Entry points: [`CayenneTableProvider::run_one_compaction_pass`] (full
//! current-snapshot rewrite, called under `compaction_lock` from
//! `maybe_compact_small_files`), `sort_and_rewrite_data` (sorted rewrite), and
//! [`CayenneTableProvider::compact_protected_snapshots_subset`] (size-tiered
//! merge of immutable protected snapshots, CAS-committed via
//! `swap_protected_snapshots`). Listing-table and protected-set swaps publish
//! under `listing_fence.write()`; position-delete tables additionally gate on
//! `write_lock`/`visibility_lock` before a subset merge.
//!
//! Mechanically split out of `provider/table.rs`; code is moved verbatim.

use datafusion_catalog::TableProvider;
use futures::{StreamExt, TryStreamExt};

use super::{
    Arc, CayenneTableProvider, Error, ExecutionPlan, HashSet, Instant, ObjectStoreExt,
    ObjectStorePath, Ordering, PROTECTED_MERGE_MAX_WIDTH, PROTECTED_TIER_BASE_BYTES,
    PROTECTED_TIER_GROWTH, Result, STAGING_WAL_FILENAME, STAGING_WAL_TMP_FILENAME,
    SendableRecordBatchStream, SessionContext, SnapshotMaintenanceTrigger, SystemTime, UnionExec,
    duration_millis_saturating, protected_snapshot_maintenance_trigger,
    protected_snapshot_size_tier, select_protected_snapshot_merge_tier, subset_merge_write_shape,
};

impl CayenneTableProvider {
    /// Sort a record batch stream using `DataFusion`'s `SortExec` for optimal performance.
    ///
    /// This is used during refresh operations to sort the **entire refresh corpus** before it's
    /// chunked and written to files, ensuring optimal zone map statistics across all Vortex files.
    ///
    /// # External Sort with Disk Spilling
    ///
    /// Uses `DataFusion`'s `SortExec` which provides:
    /// - **Automatic disk spilling**: Handles datasets larger than available memory
    /// - **Streaming external merge sort**: Processes data incrementally without loading all into RAM
    /// - **SIMD-optimized kernels**: Hardware-accelerated sorting (NEON on arm64, AVX2/AVX-512 on amd64)
    /// - **Configurable spill compression**: Supports zstd, `lz4_frame`, or uncompressed spill files
    /// - **Memory management**: Integrates with `DataFusion`'s memory pool and reservation system
    ///
    /// # Configuration
    ///
    /// Spill behavior is controlled by runtime configuration:
    /// - `sort_spill_reservation_bytes`: Memory reserved for merge operations (default: 10MB)
    /// - `sort_in_place_threshold_bytes`: Size below which data is sorted in-place (default: 1MB)
    /// - `spill_compression`: Compression codec for spill files (uncompressed, `lz4_frame`, zstd)
    /// - `temp_directory`: Directory for spill files (configured in runtime)
    ///
    /// # Performance
    ///
    /// - Small datasets (<1MB): Sorted in-place in memory, no allocations
    /// - Medium datasets (1MB-available memory): In-memory sort with single merge
    /// - Large datasets (>available memory): External merge sort with disk spilling
    /// - All cases use SIMD-optimized Arrow kernels and parallel sorting via rayon
    ///
    /// # Errors
    ///
    /// Returns an error if sorting fails or if configured sort columns don't exist.
    pub(super) fn sort_stream(
        &self,
        stream: SendableRecordBatchStream,
    ) -> Result<SendableRecordBatchStream> {
        use datafusion_execution::TaskContext;

        // Create a task context with default memory pool and runtime settings
        // This will use the configured spill directory and compression settings
        let task_ctx = Arc::new(TaskContext::default());

        tracing::debug!(
            "Sorting refresh data by columns {:?} for table {} using DataFusion SortExec with disk spilling support",
            self.context.sort_columns(),
            self.table_metadata.table_name
        );

        // Use the common stream sorting utility
        let sorted_stream =
            util::stream_utils::sort_stream(stream, self.context.sort_columns(), &task_ctx)?;

        Ok(sorted_stream)
    }

    /// Sort and rewrite data by reading from the current listing table, writing
    /// sorted data to a new snapshot, and atomically swapping.
    ///
    /// This method:
    /// 1. Reads all data from the current listing table
    /// 2. Sorts the data using `DataFusion`'s `SortExec` (with disk spilling)
    /// 3. Writes sorted data to a **new** snapshot directory (avoids deleting
    ///    files that the lazy `SortExec` stream still needs to read)
    /// 4. Atomically commits the new snapshot in the catalog
    /// 5. Updates in-memory state and triggers old snapshot cleanup
    ///
    /// This ensures zone maps have non-overlapping min/max ranges for optimal pruning.
    ///
    /// # Safety
    ///
    /// Must not run concurrently with CDC on an inlining upsert table: like a
    /// compaction pass it checkpoints the inline memtable
    /// (`visible_file_stream_for_rewrite` → `checkpoint_inlined_data`), which
    /// flushes inline rows to a file WITHOUT applying an inert
    /// (`published = false`) staged tombstone and then clears every tombstone —
    /// resurfacing the old row once the in-flight upsert publishes. The guard
    /// below defers (returns `Ok(())` without rewriting) while a staged inline-
    /// conflict tombstone is unpublished (`pending_inline_tombstones > 0`) or a
    /// staged append is mid-finalization (`has_inflight_staging_appends()`),
    /// mirroring `run_one_compaction_pass`. Today the only non-test caller path
    /// cannot hit this, but the method is `pub`, so the guard makes it safe by
    /// construction.
    ///
    /// # Errors
    ///
    /// Returns an error if reading, sorting, or rewriting fails.
    pub async fn sort_and_rewrite_data(&self, target_size_bytes: usize) -> Result<()> {
        if self.pending_inline_tombstones.load(Ordering::Acquire) > 0
            || self.has_inflight_staging_appends()
        {
            tracing::debug!(
                table = %self.table_metadata.table_name,
                "Deferring sort-and-rewrite: a staged inline-conflict tombstone or append finalization is in flight"
            );
            return Ok(());
        }

        tracing::info!(
            "Sorting and rewriting data for table {} by columns {:?}",
            self.table_metadata.table_name,
            self.context.sort_columns()
        );

        // Create a session context and scan the logical table view to get all
        // currently visible rows. The rewrite commit clears deletion/protected
        // snapshot state, so the input stream must have already applied it.
        let ctx = self.create_session_context();
        let stream = self.visible_file_stream_for_rewrite(&ctx).await?;

        // Sort the stream using our existing sort logic
        let sorted_stream = self.sort_stream(stream)?;

        // Write sorted data to a new snapshot directory. Because SortExec lazily
        // reads input files via DataSourceExec, writing to a separate directory
        // avoids the need to either:
        //  - delete old files first (which would break the lazy read), or
        //  - collect all sorted data into memory before writing
        let new_snapshot_id = uuid::Uuid::now_v7().to_string();
        let is_s3 = self.table_metadata.path.starts_with("s3://");

        let cleanup_failed_snapshot = async {
            if is_s3 {
                let snapshot_url = Self::snapshot_dir_url(
                    &self.table_metadata.path,
                    &self.table_metadata.table_id,
                    &new_snapshot_id,
                );

                match url::Url::parse(&snapshot_url) {
                    Ok(url) => {
                        let Some(config) = self.object_store_config.as_ref() else {
                            tracing::warn!(
                                "Skipping failed sort-rewrite S3 cleanup for table {} because object_store_config is missing",
                                self.table_metadata.table_name
                            );
                            return;
                        };

                        let snapshot_host = url.host_str().unwrap_or_default();
                        let config_host = config.url.host_str().unwrap_or_default();
                        if !snapshot_host.is_empty()
                            && !config_host.is_empty()
                            && snapshot_host != config_host
                        {
                            tracing::warn!(
                                "Skipping failed sort-rewrite S3 cleanup for table {} because snapshot host {} does not match configured object store host {}",
                                self.table_metadata.table_name,
                                snapshot_host,
                                config_host
                            );
                            return;
                        }

                        let path = url.path().trim_start_matches('/');
                        let prefix = ObjectStorePath::from(path);
                        if let Err(e) = self.delete_prefix_with_object_store(&prefix).await {
                            tracing::warn!(
                                "Failed to clean up failed sort-rewrite snapshot {} for table {}: {e}",
                                new_snapshot_id,
                                self.table_metadata.table_name
                            );
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to parse snapshot URL for failed sort-rewrite cleanup {} on table {}: {e}",
                            snapshot_url,
                            self.table_metadata.table_name
                        );
                    }
                }
            } else {
                let snapshot_dir = self.snapshot_dir_path_for(&new_snapshot_id);
                if let Err(e) = tokio::fs::remove_dir_all(&snapshot_dir).await {
                    tracing::warn!(
                        "Failed to clean up failed sort-rewrite snapshot dir {} for table {}: {e}",
                        snapshot_dir.display(),
                        self.table_metadata.table_name
                    );
                }
            }
        };

        // For local paths, ensure the snapshot directory exists.
        // S3 doesn't require directory creation (object storage creates paths on write).
        if !is_s3 {
            let snapshot_dir = self.snapshot_dir_path_for(&new_snapshot_id);
            Self::ensure_snapshot_dir_exists(&snapshot_dir).await?;
        }

        let (total_rows, chunk_count, _stats_acc) = self
            // Sorted rewrite: `has_sort_columns()` already forces a single shard
            // (and `target_partitions = 1`), so no size estimate is needed.
            .write_to_snapshot(
                sorted_stream,
                target_size_bytes,
                &new_snapshot_id,
                1,
                None,
                super::delta_encoding::WriteClass::Maintenance,
            )
            .await?;

        if total_rows == 0 {
            tracing::debug!(
                "No data to sort-rewrite for table {}",
                self.table_metadata.table_name
            );
            // Clean up empty snapshot directory for local paths
            if !is_s3 {
                let snapshot_dir = self.snapshot_dir_path_for(&new_snapshot_id);
                let _ = tokio::fs::remove_dir(&snapshot_dir).await;
            }
            return Ok(());
        }

        // Sync the snapshot directory for durability before committing metadata.
        if !is_s3 {
            let snapshot_dir = self.snapshot_dir_path_for(&new_snapshot_id);
            if let Err(e) = Self::sync_snapshot_dir(&snapshot_dir).await {
                cleanup_failed_snapshot.await;
                return Err(Error::Catalog { source: e });
            }
        }

        // Pre-create the listing table before committing to catalog.
        // This ensures that if listing table creation fails, we haven't committed
        // the catalog yet, avoiding an inconsistent state.
        let snapshot_dir_url = Self::snapshot_dir_url(
            &self.table_metadata.path,
            &self.table_metadata.table_id,
            &new_snapshot_id,
        );
        let new_listing_table = Self::create_listing_table(
            &snapshot_dir_url,
            Arc::clone(&self.table_metadata.schema),
            self.context.file_format(),
            &self.pk_deletion_strategy,
        )?;

        // Atomically update the catalog to point to the new sorted snapshot.
        // commit_compaction clears delete files and insert records, which is
        // correct here since the sort rewrites all live data into the new snapshot.
        if let Err(e) = self.commit_snapshot_rewrite(&new_snapshot_id).await {
            cleanup_failed_snapshot.await;
            return Err(Error::Catalog { source: e });
        }

        // Now that catalog is committed, update the in-memory listing table.
        // Hold listing_fence for write across the Arc swap so any concurrent
        // scan() picks up either the old or the new listing atomically.
        {
            let _fence = self.listing_fence.write().await;
            self.listing_table.store(new_listing_table);
        }

        // Update in-memory state to match the new catalog
        self.update_current_snapshot_id(&new_snapshot_id);
        self.clear_all_deletion_caches();

        // Old snapshot directories are cleaned up in the background
        self.trigger_old_snapshot_cleanup(&new_snapshot_id).await;

        tracing::info!(
            "Rewrote {} rows in {} sorted chunk(s) for table {}",
            total_rows,
            chunk_count,
            self.table_metadata.table_name
        );

        Ok(())
    }
}

impl CayenneTableProvider {
    /// Single compaction pass — list, pick, rewrite.
    ///
    /// Returns `Ok(true)` if the pass produced a new snapshot.
    pub(super) async fn run_one_compaction_pass(&self) -> Result<bool> {
        use super::compaction::{FileEntry, pick_candidates};
        let pass_start = std::time::Instant::now();

        if self.has_inflight_staging_appends() {
            tracing::trace!(
                target: "cayenne::compaction",
                table = self.table_metadata.table_name.as_str(),
                "Skipping compaction trigger: staged append finalization is in flight",
            );
            return Ok(false);
        }

        // Cheap early-out using in-memory counters. During the common
        // "accumulation phase" of many small appends we have not yet created
        // enough new files or protected snapshots to possibly cross a
        // compaction threshold. This avoids the expensive full snapshot
        // listing (S3 LIST or local readdir of potentially thousands of files)
        // on every post-write trigger.
        let cfg = self.context.compaction_picker_config();
        let maintenance_trigger = self.protected_snapshot_maintenance_trigger();
        if self.new_files_since_last_compaction.load(Ordering::Relaxed) < cfg.trigger_files
            && maintenance_trigger.is_none()
        {
            return Ok(false);
        }

        if let Some(trigger) = maintenance_trigger {
            self.log_snapshot_maintenance_trigger(trigger);
            self.rewrite_current_snapshot_for_compaction_tracked()
                .await?;
            return Ok(true);
        }

        let snapshot_id = self.get_current_snapshot_id();
        let files = self
            .list_compaction_candidate_files_with_sizes(&snapshot_id)
            .await?;

        if files.len() < 2 {
            return Ok(false);
        }
        let Some(candidate) = pick_candidates(
            files.iter().map(|(path, size)| FileEntry {
                path: path.as_str(),
                size_bytes: *size,
            }),
            &cfg,
        ) else {
            return Ok(false);
        };

        tracing::info!(
            target: "cayenne::compaction",
            table = self.table_metadata.table_name.as_str(),
            tier = candidate.tier.as_str(),
            picked_files = candidate.paths.len(),
            picked_bytes = candidate.total_bytes,
            total_files = files.len(),
            "Running tiered compaction pass"
        );

        // `candidate.paths` identifies the files that triggered this pass and
        // is used for tracing/metrics. The rewrite intentionally consolidates
        // the full current snapshot so compaction preserves a single coherent
        // snapshot boundary instead of mixing old and newly written file sets.
        self.rewrite_current_snapshot_for_compaction_tracked()
            .await?;

        tracing::info!(
            target: "cayenne::compaction",
            table = self.table_metadata.table_name.as_str(),
            tier = candidate.tier.as_str(),
            duration_ms = pass_start.elapsed().as_millis(),
            "Completed tiered compaction pass"
        );
        Ok(true)
    }

    /// List Vortex files in the current snapshot directory with their sizes.
    ///
    /// Local filesystem: uses [`tokio::fs::read_dir`].
    /// S3 (and S3 Express One Zone): uses the configured `ObjectStore::list`.
    ///
    /// Only entries whose name ends in `.vortex` are returned, which matches
    /// the file naming used by [`Self::write_to_snapshot`]. Hidden files
    /// (those starting with `.`) and staging WAL artifacts are filtered out.
    ///
    /// Exposed as `#[doc(hidden)] pub` so the crate's integration tests can
    /// assert on file counts after compaction without forcing this internal
    /// diagnostic helper into the documented public surface area.
    #[doc(hidden)]
    pub async fn list_snapshot_files_with_sizes(
        &self,
        snapshot_id: &str,
    ) -> Result<Vec<(String, u64)>> {
        if self.table_metadata.path.starts_with("s3://") {
            self.list_snapshot_files_with_sizes_s3(snapshot_id).await
        } else {
            self.list_snapshot_files_with_sizes_local(snapshot_id).await
        }
    }

    pub(super) async fn list_compaction_candidate_files_with_sizes(
        &self,
        current_snapshot_id: &str,
    ) -> Result<Vec<(String, u64)>> {
        let protected_snapshot_ids: Vec<String> =
            self.protected_snapshots.load().keys().cloned().collect();

        let mut seen_snapshot_ids = HashSet::with_capacity(protected_snapshot_ids.len() + 1);
        let mut files = Vec::new();

        for snapshot_id in
            std::iter::once(current_snapshot_id.to_string()).chain(protected_snapshot_ids)
        {
            if !seen_snapshot_ids.insert(snapshot_id.clone()) {
                continue;
            }

            files.extend(
                self.list_snapshot_files_with_sizes(&snapshot_id)
                    .await?
                    .into_iter()
                    .map(|(path, size)| (format!("{snapshot_id}/{path}"), size)),
            );
        }

        Ok(files)
    }

    pub(super) fn protected_snapshot_maintenance_trigger(
        &self,
    ) -> Option<SnapshotMaintenanceTrigger> {
        let protected_snapshots = self.protected_snapshots.load();
        protected_snapshot_maintenance_trigger(
            &self.protected_snapshot_age_warning_keys,
            &protected_snapshots,
            self.context.compaction_trigger_protected_snapshots(),
            self.context.compaction_trigger_snapshot_age(),
            SystemTime::now(),
        )
    }

    pub(super) fn log_snapshot_maintenance_trigger(&self, trigger: SnapshotMaintenanceTrigger) {
        match trigger {
            SnapshotMaintenanceTrigger::ProtectedSnapshotCount {
                protected_snapshot_count,
                trigger_count,
            } => tracing::info!(
                target: "cayenne::compaction",
                table = self.table_metadata.table_name.as_str(),
                protected_snapshot_count,
                trigger_count,
                "Running protected snapshot maintenance compaction because the count trigger fired"
            ),
            SnapshotMaintenanceTrigger::ProtectedSnapshotAge {
                protected_snapshot_count,
                oldest_snapshot_age,
                trigger_age,
            } => tracing::info!(
                target: "cayenne::compaction",
                table = self.table_metadata.table_name.as_str(),
                protected_snapshot_count,
                oldest_snapshot_age_ms = duration_millis_saturating(oldest_snapshot_age),
                trigger_age_ms = duration_millis_saturating(trigger_age),
                "Running protected snapshot maintenance compaction because the age trigger fired"
            ),
        }
    }

    pub(super) async fn list_snapshot_files_with_sizes_local(
        &self,
        snapshot_id: &str,
    ) -> Result<Vec<(String, u64)>> {
        let snapshot_dir = self.snapshot_dir_path_for(snapshot_id);
        let mut entries = match tokio::fs::read_dir(&snapshot_dir).await {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Vec::new());
            }
            Err(e) => return Err(e.into()),
        };

        let mut files = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let file_type = entry.file_type().await?;
            if !file_type.is_file() {
                continue;
            }

            let name = entry.file_name();
            let Some(name_str) = name.to_str() else {
                continue;
            };

            if !Self::is_compactable_data_file(name_str) {
                continue;
            }

            let metadata = entry.metadata().await?;
            files.push((name_str.to_string(), metadata.len()));
        }

        Ok(files)
    }

    pub(super) async fn list_snapshot_files_with_sizes_s3(
        &self,
        snapshot_id: &str,
    ) -> Result<Vec<(String, u64)>> {
        let Some(prefix) = self.snapshot_object_store_prefix(snapshot_id)? else {
            return Ok(Vec::new());
        };

        let config = self.require_object_store()?;
        // Stream-iterate so a large snapshot directory doesn't materialize the
        // full `ObjectMeta` list in memory on the write path — only the small
        // `(name, size)` pairs the picker needs are retained.
        let mut stream = config.store.list(Some(&prefix));
        let mut files = Vec::new();
        while let Some(meta) = stream.try_next().await.map_err(|e| Error::ObjectStore {
            operation: "list snapshot objects for compaction",
            table: self.table_metadata.table_name.clone(),
            source: e,
        })? {
            let path_str = meta.location.as_ref();
            let name = path_str.rsplit_once('/').map_or(path_str, |(_, name)| name);

            if !Self::is_compactable_data_file(name) {
                continue;
            }
            files.push((name.to_string(), meta.size));
        }

        Ok(files)
    }

    /// Returns true if the file name looks like a compactable Vortex data file
    /// (and not a hidden file or staging-WAL artifact).
    pub(super) fn is_compactable_data_file(name: &str) -> bool {
        if name.starts_with('.') {
            return false;
        }
        if name == STAGING_WAL_FILENAME || name == STAGING_WAL_TMP_FILENAME {
            return false;
        }
        name.ends_with(".vortex")
    }

    /// Rewrite the current snapshot into a fresh one, consolidating its files.
    ///
    /// When `sort_columns` are configured, compaction sorts the merged stream
    /// before writing the replacement snapshot. Ordinary writes intentionally
    /// stay unsorted so CDC/append throughput is `O(write_size)`; the background
    /// compactor pays the sort cost and restores tight file-level zone maps.
    ///
    /// On success the catalog is atomically pointed at the new snapshot, the
    /// in-memory listing table is swapped, deletion caches are cleared, and
    /// old snapshot dirs are reaped in the background.
    /// Runs a snapshot-rewrite compaction pass and records its telemetry: pass
    /// duration with a `completed`/`failed` result dimension (the histogram's
    /// count doubles as the pass counter) and, on a memory-exhaustion failure,
    /// the dedicated-pool exhaustion counter. This is the single entry point the
    /// background and post-write compaction triggers call.
    pub(super) async fn rewrite_current_snapshot_for_compaction_tracked(&self) -> Result<()> {
        let pass_start = Instant::now();
        let result = self.rewrite_current_snapshot_for_compaction().await;

        let table = self.table_metadata.table_name.clone();
        let result_label = if result.is_ok() {
            "completed"
        } else {
            "failed"
        };
        telemetry::track_cayenne_compaction_duration(
            pass_start.elapsed(),
            &[
                telemetry::KeyValue::new("table", table.clone()),
                telemetry::KeyValue::new("result", result_label),
            ],
        );
        if let Err(e) = &result
            && matches!(
                e,
                Error::DataFusion { source }
                    if matches!(source, datafusion_common::DataFusionError::ResourcesExhausted(_))
            )
        {
            telemetry::track_cayenne_compaction_memory_exhausted(&[telemetry::KeyValue::new(
                "table", table,
            )]);
        }
        result
    }

    pub(super) async fn rewrite_current_snapshot_for_compaction(&self) -> Result<()> {
        let compaction_start = std::time::Instant::now();
        // Use the dedicated compaction memory environment (carved budget) when
        // injected, so this rewrite accounts its memory against the isolated
        // compaction pool rather than competing with queries for the query pool.
        let ctx = self.create_compaction_session_context();
        let mut stream = self.visible_file_stream_for_rewrite(&ctx).await?;

        if self.context.has_sort_columns() {
            tracing::info!(
                target: "cayenne::compaction",
                table = self.table_metadata.table_name.as_str(),
                sort_columns = ?self.context.sort_columns(),
                "Sorting compaction rewrite before writing consolidated output files"
            );
            stream = self.sort_stream(stream)?;
        }

        // Compaction is the file-count reduction path. Ordinary appends shard
        // across the session's target partitions for encode throughput, but a
        // rewrite that preserves that fan-out can leave nearly as many small
        // files behind as it started with.
        let target_partitions = 1;

        let new_snapshot_id = uuid::Uuid::now_v7().to_string();
        let is_s3 = self.table_metadata.path.starts_with("s3://");

        if !is_s3 {
            let snapshot_dir = self.snapshot_dir_path_for(&new_snapshot_id);
            Self::ensure_snapshot_dir_exists(&snapshot_dir).await?;
        }

        let target_size_bytes = self.context.target_file_size_bytes();
        let write_result = self
            .write_to_snapshot(
                stream,
                target_size_bytes,
                &new_snapshot_id,
                target_partitions,
                // Compaction already pins `target_partitions = 1` (single output
                // file is the whole point), so the shard count is forced to 1
                // regardless; no size estimate needed.
                None,
                super::delta_encoding::WriteClass::Maintenance,
            )
            .await;

        let (total_rows, _writer_ops, stats_acc) = match write_result {
            Ok(result) => result,
            Err(e) => {
                self.cleanup_failed_compaction_snapshot(&new_snapshot_id, is_s3)
                    .await;
                return Err(e);
            }
        };

        if total_rows == 0 {
            // No live rows in the source — clean up the empty new snapshot
            // dir and skip the catalog commit. Subsequent triggers will keep
            // returning the same empty state and pick None, so this is rare.
            self.cleanup_failed_compaction_snapshot(&new_snapshot_id, is_s3)
                .await;
            return Ok(());
        }

        if !is_s3 {
            let snapshot_dir = self.snapshot_dir_path_for(&new_snapshot_id);
            if let Err(e) = Self::sync_snapshot_dir(&snapshot_dir).await {
                self.cleanup_failed_compaction_snapshot(&new_snapshot_id, is_s3)
                    .await;
                return Err(Error::Catalog { source: e });
            }
        }

        let snapshot_dir_url = Self::snapshot_dir_url(
            &self.table_metadata.path,
            &self.table_metadata.table_id,
            &new_snapshot_id,
        );
        let new_listing_table = Self::create_listing_table(
            &snapshot_dir_url,
            Arc::clone(&self.table_metadata.schema),
            self.context.file_format(),
            &self.pk_deletion_strategy,
        )?;

        if let Err(e) = self.commit_snapshot_rewrite(&new_snapshot_id).await {
            self.cleanup_failed_compaction_snapshot(&new_snapshot_id, is_s3)
                .await;
            return Err(Error::Catalog { source: e });
        }

        // Hold the listing fence across the listing-table swap and the
        // current-snapshot-id update so new plan-build calls observe the
        // swap atomically. Deletion caches and stats are touched under the
        // fence too — readers that already hold a snapshot of these (loaded
        // during plan-build under read fence) won't observe a torn state.
        {
            let _fence = self.listing_fence.write().await;
            self.listing_table.store(new_listing_table);
            self.update_current_snapshot_id(&new_snapshot_id);
            self.clear_all_deletion_caches();

            // Persist accumulated stats from the rewrite — keeps DataFusion's
            // synchronous statistics path consistent with the new snapshot. The
            // rewrite materializes exactly the live rows, so its min/max + NDV +
            // count are authoritative: replace the aggregate, correcting any
            // drift the incremental merges/deltas accumulated.
            self.replace_table_stats_after_rewrite(&stats_acc).await;
        }

        // Checkpoint the PK existence index for fast restart (best-effort). The
        // new current snapshot now holds all live rows, and protected snapshots /
        // inline entries were just cleared, so a bloom of this snapshot's keys is a
        // complete checkpoint tagged with `new_snapshot_id`.
        if self.upsert_bloom_eligible() {
            self.persist_pk_bloom_checkpoint(&new_snapshot_id, total_rows)
                .await;
        }

        // Cleanup must wait for in-flight scans whose plan-build already
        // captured file paths from the OLD snapshot to finish executing.
        // The fence guarantees no NEW plan-build sees the old listing
        // table, but plan-execute holds no fence. `trigger_old_snapshot_cleanup`
        // delays the actual `remove_dir_all` by its built-in grace period
        // (`OLD_SNAPSHOT_CLEANUP_GRACE`) so the at-risk window
        // (plan-build → plan-execute) closes naturally.
        self.trigger_old_snapshot_cleanup(&new_snapshot_id).await;

        tracing::info!(
            target: "cayenne::compaction",
            table = self.table_metadata.table_name.as_str(),
            rows = total_rows,
            new_snapshot_id = new_snapshot_id.as_str(),
            duration_ms = compaction_start.elapsed().as_millis(),
            "Snapshot compaction completed"
        );

        Ok(())
    }

    pub(super) async fn visible_file_stream_for_rewrite(
        &self,
        ctx: &SessionContext,
    ) -> Result<SendableRecordBatchStream> {
        if self.cached_inlined_row_count() > 0 {
            self.checkpoint_inlined_data().await?;
        }

        let state = ctx.state();
        let plan = TableProvider::scan(self, &state, None, &[], None).await?;
        let stream = datafusion_physical_plan::execute_stream(plan, state.task_ctx())?;
        Ok(stream)
    }

    /// Fast, write-lock-free consolidation of a size-tiered subset of the
    /// immutable protected snapshots into a single new protected snapshot.
    ///
    /// Unlike current-snapshot small-file compaction, this rewrites only
    /// protected snapshots (data written after deletions) — it never touches
    /// the current snapshot `S0`, its pointer, or its delete files `D1..Dn`.
    /// Those delete files still apply to `S0` and are intentionally preserved.
    ///
    /// Algorithm (fenced immutable-input protocol):
    /// 1. Short fence read: snapshot the protected set + the live deletion
    ///    snapshot + its `max_delete_seq` as one coherent `(inputs, fence_seq)`
    ///    pair, so the rewrite applies exactly the deletions visible at the
    ///    fence and the merged snapshot can be tagged consistently.
    /// 2. Size-tier selection (outside the fence): assign inputs to LSM-style
    ///    size tiers and merge only the lowest tier that has accumulated enough
    ///    same-size runs, bounding write amplification. See
    ///    [`select_protected_snapshot_merge_tier`].
    /// 3. Rewrite outside the lock: union-scan each selected input applying its
    ///    own partial deletion filter (`delete_seq > threshold_at_creation`),
    ///    exactly as a read would, streaming the result into a fresh snapshot.
    /// 4. CAS commit: [`MetadataCatalog::swap_protected_snapshots`] atomically
    ///    deactivates the input snapshots and activates the merged one — only
    ///    if every input is still active. On a lost race the rewritten output
    ///    is discarded and a later trigger retries.
    ///
    /// ## Why the merged snapshot's threshold is the fence `max_delete_seq`
    ///
    /// Each input's rows have all deletions with `seq > threshold_at_creation`
    /// physically applied during the rewrite (deletions with `seq <= threshold`
    /// correctly do NOT apply — those rows are newer than the deletion). After
    /// the merge every input has therefore resolved all deletions up to the
    /// fence's `max_delete_seq`, so the new snapshot is tagged with that value
    /// and the read path only applies strictly-newer deletions (`seq > fence`)
    /// to it going forward. This preserves the sequence-ordering invariant with
    /// no resurrection and no over-deletion.
    ///
    /// `max_inputs` bounds how many of the oldest protected snapshots are
    /// considered as merge candidates before size-tiering; pass `usize::MAX` to
    /// consider the whole set.
    ///
    /// Returns `Ok(true)` if a merge was committed, `Ok(false)` if there was
    /// nothing to do (fewer than two protected snapshots, no qualifying tier,
    /// or the CAS lost a race).
    #[doc(hidden)]
    pub async fn compact_protected_snapshots_subset(&self, max_inputs: usize) -> Result<bool> {
        // Position deletes are file-path scoped. If a protected-snapshot rewrite
        // races a writer that adds a position tombstone for one of the input
        // files, the old file can be swapped away before that tombstone is
        // physically applied to the merged output. Serialize those rewrites with
        // writers and staged visibility flips. Key-delete tables remain safe to
        // compact without the writer gate because post-fence deletes are carried
        // by sequence number and still apply to the merged snapshot.
        let serialize_position_deletes = self.should_capture_positions();
        let _position_write_guard = if serialize_position_deletes {
            if let Ok(guard) = self.write_lock_arc().try_lock_owned() {
                Some(guard)
            } else {
                tracing::trace!(
                    target: "cayenne::compaction",
                    table = self.table_metadata.table_name.as_str(),
                    "Skipping protected-snapshot subset compaction: writer active on position-delete table",
                );
                return Ok(false);
            }
        } else {
            None
        };
        let _position_visibility_guard = if serialize_position_deletes {
            Some(self.visibility_lock_arc().lock_owned().await)
        } else {
            None
        };

        let Ok(_guard) = self.compaction_lock.try_lock() else {
            tracing::trace!(
                table = self.table_metadata.table_name.as_str(),
                "Skipping protected-snapshot subset compaction: another pass already running",
            );
            return Ok(false);
        };

        let compaction_start = std::time::Instant::now();

        // --- Phase 1: short fence read — choose a coherent input set. ---
        // Capture the protected set, each input's deletion threshold, the live
        // deletion snapshot, and the current max delete sequence together under
        // the read fence so the rewrite applies exactly the deletions visible at
        // the fence and the merged snapshot can be tagged consistently.
        let (candidates, fence_max_delete_seq, deletion_snapshot) = {
            let _fence = self.listing_fence.read().await;
            let protected = self.protected_snapshots.load_full();
            if protected.len() < 2 {
                return Ok(false);
            }

            // Protected snapshot ids are UUIDv7, so lexical order == creation
            // order. Consider the oldest `max_inputs` (at least 2) snapshots.
            let mut ids: Vec<String> = protected.keys().cloned().collect();
            ids.sort();
            let take = ids.len().min(max_inputs.max(2));
            ids.truncate(take);

            let candidates: Vec<(String, i64)> = ids
                .into_iter()
                .map(|id| {
                    let threshold = protected.get(&id).copied().unwrap_or(0);
                    (id, threshold)
                })
                .collect();

            let deletion_snapshot = self.pk_deletion_snapshot();
            // Derive the fence from the SAME loaded deletion snapshot used for
            // the Phase 2 rewrite. A separate, independent max-delete-sequence
            // load (a fresh `pk_deletion_snapshot().max_sequence_number()`)
            // here can observe a NEWER ArcSwap version than `deletion_snapshot`:
            // any deletion that lands between the two loads would then be tagged
            // as already-applied (`seq <= fence`) without ever being applied
            // during the rewrite, permanently masking it and resurrecting the
            // rows it deletes. Both values must come from one coherent load.
            let fence_max_delete_seq = deletion_snapshot.max_sequence_number().unwrap_or(0);
            (candidates, fence_max_delete_seq, deletion_snapshot)
        };
        let phase1_fence_ms = compaction_start.elapsed().as_millis();

        // Per-input sizing: list each candidate snapshot's on-disk Vortex bytes
        // + file count. Sizes drive the size-tier selection and reveal the size
        // distribution (e.g. one carried-forward merged snapshot dwarfing the
        // small new deltas). This is diagnostic I/O outside the fence.
        let sizing_start = std::time::Instant::now();
        let mut sized_candidates: Vec<(String, i64, u64)> = Vec::with_capacity(candidates.len());
        for (snapshot_id, threshold) in &candidates {
            let bytes = match self.list_snapshot_files_with_sizes(snapshot_id).await {
                Ok(files) => files.iter().map(|(_, sz)| *sz).sum(),
                Err(e) => {
                    tracing::warn!(
                        target: "cayenne::compaction",
                        table = self.table_metadata.table_name.as_str(),
                        snapshot_id = snapshot_id.as_str(),
                        "Failed to size protected-snapshot merge input for tiering: {e}"
                    );
                    // Treat as tier 0 (unknown/small) so it stays a merge
                    // candidate rather than being skipped as "large".
                    0
                }
            };
            sized_candidates.push((snapshot_id.clone(), *threshold, bytes));
        }
        let sizing_ms = sizing_start.elapsed().as_millis();

        // --- Size-tier selection (replaces the single-threshold PoC skip). ---
        // Consolidate only the lowest size tier that has accumulated at least
        // `min_runs` same-size runs, capped at `PROTECTED_MERGE_MAX_WIDTH`. This
        // bounds write amplification to O(log N) per byte (a run is rewritten
        // only when its tier fills up and it levels up) and read amplification
        // to at most `min_runs - 1` un-merged runs per tier — instead of folding
        // the large carried-forward blob back in on every pass.
        let min_runs = self.context.compaction_trigger_protected_snapshots().max(2);
        let inputs = select_protected_snapshot_merge_tier(
            &sized_candidates,
            min_runs,
            PROTECTED_MERGE_MAX_WIDTH,
            PROTECTED_TIER_BASE_BYTES,
            PROTECTED_TIER_GROWTH,
        );

        if inputs.len() < 2 {
            tracing::debug!(
                target: "cayenne::compaction",
                table = self.table_metadata.table_name.as_str(),
                candidates = sized_candidates.len(),
                min_runs,
                tier_base_bytes = PROTECTED_TIER_BASE_BYTES,
                "Skipping fast protected-snapshot compaction: no size tier has enough runs to merge"
            );
            return Ok(false);
        }

        // Diagnostics over the SELECTED (single-tier) input set.
        let selected_ids: std::collections::HashSet<&str> =
            inputs.iter().map(|(id, _)| id.as_str()).collect();
        let selected_sizes: Vec<&(String, i64, u64)> = sized_candidates
            .iter()
            .filter(|(id, _, _)| selected_ids.contains(id.as_str()))
            .collect();
        let total_input_bytes: u64 = selected_sizes.iter().map(|(_, _, b)| *b).sum();
        let largest_input_bytes = selected_sizes.iter().map(|(_, _, b)| *b).max().unwrap_or(0);
        // Percent of selected bytes contributed by the single largest run,
        // computed with integer (u128) math to avoid any float cast. With
        // size-tiering this should stay low (runs are same-tier), in contrast to
        // the ~99% dominance of the old fold-everything pass.
        let dominance_pct: u64 = if total_input_bytes > 0 {
            u64::try_from(u128::from(largest_input_bytes) * 100 / u128::from(total_input_bytes))
                .unwrap_or(100)
        } else {
            0
        };
        let selected_tier = protected_snapshot_size_tier(
            largest_input_bytes,
            PROTECTED_TIER_BASE_BYTES,
            PROTECTED_TIER_GROWTH,
        );

        tracing::info!(
            target: "cayenne::compaction",
            table = self.table_metadata.table_name.as_str(),
            input_count = inputs.len(),
            candidate_count = sized_candidates.len(),
            selected_tier,
            min_runs,
            fence_max_delete_seq,
            total_input_bytes,
            largest_input_bytes,
            dominance_pct,
            phase1_fence_ms,
            sizing_ms,
            "Running fast protected-snapshot subset compaction"
        );

        // --- Phase 2: rewrite outside the lock. ---
        // Build a UNION over the selected inputs, applying each input's own
        // partial deletion filter exactly as the read path does, then stream
        // the merged rows into a fresh snapshot dir.
        let phase2_start = std::time::Instant::now();
        let ctx = self.create_compaction_session_context();
        let state = ctx.state();
        let pk_indices = self.pk_column_indices.clone();

        let mut plans: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(inputs.len());
        for (snapshot_id, threshold) in &inputs {
            let plan = self
                .create_snapshot_scan_plan(&state, snapshot_id, None, &[], None)
                .await?;
            let filtered = self.apply_partial_deletion_filter(
                plan,
                &pk_indices,
                *threshold,
                &deletion_snapshot,
            )?;
            plans.push(filtered);
        }

        let merged_plan: Arc<dyn ExecutionPlan> = if plans.len() == 1 {
            plans.remove(0)
        } else {
            UnionExec::try_new(plans)?
        };
        let stream = datafusion_physical_plan::execute_stream(merged_plan, state.task_ctx())?;
        let plan_build_ms = phase2_start.elapsed().as_millis();

        let new_snapshot_id = uuid::Uuid::now_v7().to_string();
        let is_s3 = self.table_metadata.path.starts_with("s3://");

        if !is_s3 {
            let snapshot_dir = self.snapshot_dir_path_for(&new_snapshot_id);
            Self::ensure_snapshot_dir_exists(&snapshot_dir).await?;
        }

        let target_size_bytes = self.context.target_file_size_bytes();
        // Size-aware parallel merge encode (EFF-1 / Pattern 12). Passing the
        // selected inputs' total bytes lets `snapshot_shard_count` size the
        // encoder fan-out as floor(bytes / target_file_size), min 1 — a
        // second shard is earned at >= 2x the target size — capped at the
        // write concurrency and the process-global encode budget:
        //
        // - a merge whose output fits one target file stays exactly ONE
        //   shard / one output file, so the read fan-out this compaction
        //   exists to collapse is unchanged for small merges;
        // - a merge spanning multiple target files was always going to emit
        //   multiple files; it now encodes them in parallel (PK-hash
        //   clustered, like the append path) instead of streaming the whole
        //   tier through one core.
        //
        // Two single-writer safety cases are preserved:
        // - sorted tables: `snapshot_shard_count` returns 1 when
        //   `has_sort_columns()` — sharding a globally sorted stream would
        //   scatter its order across files;
        // - position-delete tables — BOTH families: PK tables whose resolved
        //   `deletion_mode` is `position` (`serialize_position_deletes`, the
        //   same predicate this function's writer/visibility guards use
        //   above) and PK-less tables on the legacy `PositionBased` strategy.
        //   Their tombstones are file-path scoped and the rewrite's position
        //   bake-in assumes a single output sequence, so they keep the serial
        //   single-WRITER shape explicitly (even a serial writer still rolls
        //   multiple files past the target size — see the
        //   `subset_merge_write_shape` docs).
        let keeps_positions_serial =
            serialize_position_deletes || self.pk_deletion_strategy.is_position_based();
        let (target_partitions, estimated_bytes) = subset_merge_write_shape(
            keeps_positions_serial,
            state.config().target_partitions(),
            total_input_bytes,
        );
        let write_start = std::time::Instant::now();
        let write_result = self
            .write_to_snapshot(
                stream,
                target_size_bytes,
                &new_snapshot_id,
                target_partitions,
                // Total bytes of the selected tier inputs — the size estimate
                // that drives the shard-count floor above. `None` (single
                // serial writer) for position-delete tables.
                estimated_bytes,
                // Compaction re-encodes for the long term: always the full
                // (Maintenance) encoding cascade, never the cheap delta tier.
                super::delta_encoding::WriteClass::Maintenance,
            )
            .await;

        let (total_rows, _writer_ops, _stats_acc) = match write_result {
            Ok(result) => result,
            Err(e) => {
                self.cleanup_failed_compaction_snapshot(&new_snapshot_id, is_s3)
                    .await;
                return Err(e);
            }
        };
        let write_ms = write_start.elapsed().as_millis();

        // Sync the new snapshot dir for durability before the catalog commit.
        if !is_s3 {
            let snapshot_dir = self.snapshot_dir_path_for(&new_snapshot_id);
            if let Err(e) = Self::sync_snapshot_dir(&snapshot_dir).await {
                self.cleanup_failed_compaction_snapshot(&new_snapshot_id, is_s3)
                    .await;
                return Err(Error::Catalog { source: e });
            }
        }

        // --- Phase 3: CAS commit. ---
        let phase2_rewrite_ms = phase2_start.elapsed().as_millis();
        let phase3_start = std::time::Instant::now();
        let old_ids: Vec<String> = inputs.iter().map(|(id, _)| id.clone()).collect();
        let swapped = match self
            .catalog
            .swap_protected_snapshots(
                &self.table_metadata.table_id,
                &old_ids,
                &new_snapshot_id,
                fence_max_delete_seq,
            )
            .await
        {
            Ok(swapped) => swapped,
            Err(e) => {
                self.cleanup_failed_compaction_snapshot(&new_snapshot_id, is_s3)
                    .await;
                return Err(Error::Catalog { source: e });
            }
        };

        if !swapped {
            // An input snapshot was concurrently consumed by another compaction.
            // The catalog is unchanged; discard the rewritten output and let a
            // later trigger retry against the new protected set.
            tracing::debug!(
                target: "cayenne::compaction",
                table = self.table_metadata.table_name.as_str(),
                "Protected-snapshot subset swap aborted (inputs no longer active); discarding output"
            );
            self.cleanup_failed_compaction_snapshot(&new_snapshot_id, is_s3)
                .await;
            return Ok(false);
        }

        // Catalog committed — bring the in-memory protected set into agreement
        // under the scan fence. Scans capture the deletion snapshot and
        // protected-snapshot map while holding `listing_fence.read()`, so the
        // compaction-side map swap must hold `listing_fence.write()` or readers
        // can combine a pre-compaction deletion snapshot with the post-compaction
        // protected set.
        {
            let _fence = self.listing_fence.write().await;
            self.protected_snapshots.rcu(|current| {
                let mut new_map = (**current).clone();
                for (id, _) in &inputs {
                    new_map.remove(id);
                }
                new_map.insert(new_snapshot_id.clone(), fence_max_delete_seq);
                Arc::new(new_map)
            });
        }

        // Reap the replaced protected-snapshot dirs in the background after the
        // grace period. The current snapshot is preserved (passed as the
        // "current" arg); cleanup reads the LIVE protected set after the grace
        // sleep, which now excludes the merged inputs and includes the new
        // snapshot, so only the merged-away dirs are removed.
        let current_snapshot_id = self.get_current_snapshot_id();
        self.trigger_old_snapshot_cleanup(&current_snapshot_id)
            .await;

        tracing::info!(
            target: "cayenne::compaction",
            table = self.table_metadata.table_name.as_str(),
            merged_inputs = inputs.len(),
            rows = total_rows,
            new_snapshot_id = new_snapshot_id.as_str(),
            fence_max_delete_seq,
            total_input_bytes,
            largest_input_bytes,
            dominance_pct,
            phase1_fence_ms,
            sizing_ms,
            plan_build_ms,
            write_ms,
            phase2_rewrite_ms,
            phase3_commit_ms = phase3_start.elapsed().as_millis(),
            duration_ms = compaction_start.elapsed().as_millis(),
            "Fast protected-snapshot subset compaction completed"
        );

        // Record the merged *output* size so operators (and the adaptive tuner's
        // observability) can see whether compaction is trending toward the target
        // file size or stalling below it. Compare to cayenne_autotune_target_file_size_mb.
        // The new snapshot's actual on-disk Vortex bytes are the meaningful figure:
        // deletions + re-encoding/compression make the output materially smaller
        // than the summed inputs, so reporting the input sum would overstate it.
        // Best-effort — fall back to the input sum only if sizing the output fails.
        let merged_output_bytes = match self.list_snapshot_files_with_sizes(&new_snapshot_id).await
        {
            Ok(files) => files.iter().map(|(_, sz)| *sz).sum(),
            Err(e) => {
                tracing::debug!(
                    target: "cayenne::compaction",
                    table = self.table_metadata.table_name.as_str(),
                    new_snapshot_id = new_snapshot_id.as_str(),
                    "Failed to size compaction output snapshot for the merged-bytes \
                     metric; falling back to total input bytes: {e}"
                );
                total_input_bytes
            }
        };
        telemetry::track_cayenne_compaction_merged_bytes(
            merged_output_bytes,
            &[telemetry::KeyValue::new(
                "table",
                self.table_metadata.table_name.clone(),
            )],
        );

        Ok(true)
    }

    pub(super) async fn cleanup_failed_compaction_snapshot(
        &self,
        new_snapshot_id: &str,
        is_s3: bool,
    ) {
        if is_s3 {
            match self.snapshot_object_store_prefix(new_snapshot_id) {
                Ok(Some(prefix)) => {
                    if let Err(e) = self.delete_prefix_with_object_store(&prefix).await {
                        tracing::warn!(
                            "Failed to clean up failed compaction snapshot prefix {} for table {}: {e}",
                            new_snapshot_id,
                            self.table_metadata.table_name
                        );
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(
                        "Failed to resolve compaction-cleanup prefix for snapshot {} on table {}: {e}",
                        new_snapshot_id,
                        self.table_metadata.table_name
                    );
                }
            }
        } else {
            let snapshot_dir = self.snapshot_dir_path_for(new_snapshot_id);
            if let Err(e) = tokio::fs::remove_dir_all(&snapshot_dir).await
                && e.kind() != std::io::ErrorKind::NotFound
            {
                tracing::warn!(
                    "Failed to clean up failed compaction snapshot dir {} for table {}: {e}",
                    snapshot_dir.display(),
                    self.table_metadata.table_name
                );
            }
        }
    }
}
