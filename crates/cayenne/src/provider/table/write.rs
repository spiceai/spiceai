//! Write path: snapshot writes, encode sharding, and provider write-clones.
//!
//! [`CayenneTableProvider::write_to_snapshot`] streams batches into a snapshot
//! directory via the Vortex sink, with size-aware intra-write shard sizing
//! (`snapshot_shard_count` / `write_shard_format`) bounded by the process-global
//! encode budget. `insert_to_new_snapshot_with_sequence` is the
//! write-to-protected-snapshot path used when PK deletions are pending
//! (publishes the protected entry under `scan_state_lock`). `clone_for_write`
//! produces the Arc-sharing clone writers use; `reserve_sequences_local` is the
//! per-table sequence handout (lever B2). Callers hold `write_lock`.
//!
//! Mechanically split out of `provider/table.rs`; code is moved verbatim.

use datafusion_catalog::TableProvider;

use super::{
    Arc, CatalogResult, CayenneTableProvider, ColumnStatsAccumulator, DEFAULT_WRITE_CONCURRENCY,
    ExecutionPlan, InsertOp, PkDeletionSnapshot, RecordBatchStreamAdapter, Result,
    SendableRecordBatchStream, StreamExt, StreamingExec, VortexFormat, WriteShardConfig, collect,
    format_bytes, format_bytes_per_sec, pk_deletion_snapshot_for_strategy, reserve_sequences_in,
};

impl CayenneTableProvider {
    /// Insert data to a NEW snapshot with a specific sequence number.
    ///
    /// This is used when inserting while pending PK-based deletions exist.
    /// By writing to a new snapshot with a higher sequence number, we ensure:
    /// - Old data in previous snapshots is filtered by deletions (`delete_seq` >= `old_snapshot_seq`)
    /// - New data in this snapshot is visible (`new_snapshot_seq` > `delete_seq`)
    ///
    /// This achieves Iceberg-style sequence ordering without rewriting existing files.
    pub(crate) async fn insert_to_new_snapshot_with_sequence(
        &self,
        stream: SendableRecordBatchStream,
        sequence_number: i64,
        target_partitions: usize,
        estimated_bytes: Option<u64>,
    ) -> CatalogResult<(u64, Arc<ColumnStatsAccumulator>)> {
        let target_size_bytes = self.context.target_file_size_bytes();

        // Generate a new snapshot ID
        let new_snapshot_id = uuid::Uuid::now_v7().to_string();

        // Write data to the new snapshot
        let (total_rows, chunk_count, stats_acc) = self
            .write_to_snapshot(
                stream,
                target_size_bytes,
                &new_snapshot_id,
                target_partitions,
                estimated_bytes,
                super::delta_encoding::WriteClass::Delta,
            )
            .await?;

        // Sync the new snapshot directory for durability before recording the
        // sequence number in the catalog. This is required for the same reason
        // as in the sort-rewrite and normal append paths: the Vortex files must
        // be durably present before the catalog metadata that makes them
        // visible (via sequence number / protected snapshot) is committed.
        let is_s3 = self.table_metadata.path.starts_with("s3://");
        if !is_s3 {
            let snapshot_dir = self.snapshot_dir_path_for(&new_snapshot_id);
            Self::sync_snapshot_dir(&snapshot_dir).await?;
        }

        tracing::debug!(
            "Insert to new snapshot {} completed, wrote {} rows to Vortex in {} chunk(s)",
            new_snapshot_id,
            total_rows,
            chunk_count
        );

        // Record the snapshot's sequence number in the catalog
        self.catalog
            .set_snapshot_sequence(
                &self.table_metadata.table_id,
                &new_snapshot_id,
                sequence_number,
            )
            .await?;

        // Protect this snapshot using its OWN allocated `sequence_number` as the
        // deletion threshold: the same value just persisted to
        // `cayenne_snapshot_sequence` and reloaded by `load_protected_snapshots` on
        // restart. The in-memory threshold MUST match the persisted one, or the
        // partial-deletion filter (`delete_seq > threshold`) returns different rows
        // before vs after a reload. A live max-delete-sequence read (the deletion
        // snapshot's `max_sequence_number()`) must NOT be
        // used here: an unrelated deletion committed between this snapshot's sequence
        // allocation and this publish can raise the global max past this snapshot's
        // own sequence, so the in-memory threshold would skip a delete that the
        // reloaded threshold applies. See `load_protected_snapshots` for the matching
        // rationale.
        //
        // We do NOT clear old protected snapshots because they may still hold valid
        // data; each applies its own partial deletion filter from when it was created.
        // Publish under `scan_state_lock` so scans that capture the deletion view,
        // protected map, and inlined data under `.read()` observe a consistent view.
        self.commit_protected_snapshot_with_scan_lock(&new_snapshot_id, sequence_number)
            .await;

        // The listing table stays as-is. Protected snapshots are handled at scan time.
        // See the doc comment above for why we do NOT update current_snapshot.

        Ok((total_rows, stats_acc))
    }

    /// Durably record a newly written snapshot's sequence number in the catalog
    /// (syncing the snapshot directory first on local filesystems).
    ///
    /// This does NOT publish the in-memory protected-snapshot entry — that is
    /// committed by the caller via [`Self::commit_on_conflict_publish`] so the
    /// deletion-view store and the protected insert flip atomically under
    /// `scan_state_lock`.
    pub(crate) async fn record_written_snapshot_sequence(
        &self,
        snapshot_id: &str,
        sequence_number: i64,
    ) -> CatalogResult<()> {
        let is_s3 = self.table_metadata.path.starts_with("s3://");
        if !is_s3 {
            let snapshot_dir = self.snapshot_dir_path_for(snapshot_id);
            Self::sync_snapshot_dir(&snapshot_dir).await?;
        }

        self.catalog
            .set_snapshot_sequence(&self.table_metadata.table_id, snapshot_id, sequence_number)
            .await?;

        Ok(())
    }

    pub(super) fn pk_deletion_snapshot(&self) -> PkDeletionSnapshot {
        pk_deletion_snapshot_for_strategy(&self.pk_deletion_strategy)
    }

    /// Write a stream of record batches to a specific snapshot directory.
    ///
    /// This is used during compaction operations where data needs to be persisted
    /// to a new snapshot.
    ///
    /// # Arguments
    ///
    /// * `stream` - The stream of record batches to write
    /// * `target_size_bytes` - Configured writer target file size (for write behavior/logging)
    /// * `snapshot_id` - The snapshot ID to write to
    /// * `target_partitions` - Upper bound on intra-write shard writers (the
    ///   host's logical-core count); also the ceiling the Vortex sink clamps to.
    /// * `estimated_bytes` - Caller's estimate of the total bytes this write will
    ///   produce, used to size the intra-write shard count (small writes stay a
    ///   single file). `None` ⇒ unknown size ⇒ shard across the full write
    ///   concurrency (prior behavior). The stream is consumed lazily, so the
    ///   total size cannot be measured here — it must come from the caller.
    ///
    /// # Returns
    ///
    /// A tuple of (total rows written, number of writer operations)
    ///
    /// # Errors
    ///
    /// Returns an error if the write operation fails.
    pub(crate) async fn write_to_snapshot(
        &self,
        stream: SendableRecordBatchStream,
        target_size_bytes: usize,
        snapshot_id: &str,
        target_partitions: usize,
        estimated_bytes: Option<u64>,
        write_class: super::delta_encoding::WriteClass,
    ) -> Result<(u64, usize, Arc<ColumnStatsAccumulator>)> {
        use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
        use std::time::Instant;

        // Bound aggregate encode concurrency across all tables. Per-table
        // `cayenne_write_concurrency` is sized in isolation (a conservative unset
        // default of `DEFAULT_WRITE_CONCURRENCY`, but raisable per table), so
        // simultaneous CDC writes across a fleet of tables would otherwise sum and
        // oversubscribe the machine. Acquire this write's
        // shard permits from the process-global budget before sharding the
        // encode, held until the write completes. No-op (ungated) when no budget
        // is installed (unit tests, embedders). See `write_budget`.
        let shard_count =
            self.snapshot_shard_count(target_partitions, target_size_bytes, estimated_bytes);
        // `shard_count` is the *requested* fan-out; the Vortex sink clamps the
        // actual encode to `target_partitions` (`VortexFormat::build_shard_spec`).
        // Acquire permits for that clamped count so a `cayenne_write_concurrency`
        // configured above `target_partitions` can't over-subscribe the global
        // budget and throttle other tables. (`acquire_encode_permits` also caps to
        // the budget total, but clamping here keeps the request honest even if the
        // budget is ever sized below the core count, e.g. reserved query threads.)
        let encode_shards = shard_count.min(target_partitions.max(1));
        let _encode_permits = super::write_budget::acquire_encode_permits(encode_shards).await;

        // Construct snapshot directory URL
        let snapshot_dir_url = Self::snapshot_dir_url(
            &self.table_metadata.path,
            &self.table_metadata.table_id,
            snapshot_id,
        );

        // Build the write-path Vortex format for this snapshot. Unsorted writes
        // are sharded across `target_partitions` concurrent encoders (PK-hashed
        // for keyed tables, round-robin otherwise) so the encode saturates all
        // cores; sorted rewrites and single-writer configs fall back to one
        // serial writer. Scans are unaffected — they never read the write-shard
        // config (only `create_writer_physical_plan` does). The shard count is
        // size-aware: a write smaller than one target file stays a single shard
        // regardless of `target_partitions` (see `snapshot_shard_count`).
        //
        // Delta writes additionally resolve a `cayenne_delta_encoding` level:
        // small fresh deltas encode with a light scheme set (skipping the
        // per-file BtrBlocks strategy search + FSST training that dominate
        // small-write encode cost) and are re-encoded properly when compaction
        // folds them. Maintenance writes (compaction, rewrites, overwrites)
        // always use the full default strategy. See `provider::delta_encoding`.
        let encoding_level = super::delta_encoding::effective_level(
            self.context.delta_encoding(),
            write_class,
            estimated_bytes,
            target_size_bytes,
        );
        let write_format = match super::delta_encoding::strategy_builder_for_level(encoding_level) {
            Some(strategy) => self.context.write_format_with_strategy(
                strategy,
                self.write_shard_config(target_partitions, target_size_bytes, estimated_bytes),
            ),
            None => self.write_shard_format(target_partitions, target_size_bytes, estimated_bytes),
        };

        // Create a new ListingTable pointing to the snapshot directory
        let snapshot_listing_table = Self::create_listing_table(
            &snapshot_dir_url,
            Arc::clone(&self.table_metadata.schema),
            &write_format,
            &self.pk_deletion_strategy,
        )?;

        // Create session context once with object store registered (if S3).
        let session_state = Arc::new(self.create_session_context().state());

        // Progress tracking for S3 Express uploads
        let is_s3_storage = self.table_metadata.path.starts_with("s3://");
        let start_time = Instant::now();
        let last_progress_ms = Arc::new(AtomicU64::new(0));
        let total_bytes_written = Arc::new(AtomicUsize::new(0));
        let total_rows_written = Arc::new(AtomicU64::new(0));

        // Column stats accumulator — updated per batch during writes
        let stats_accumulator = Arc::new(ColumnStatsAccumulator::new(&self.table_metadata.schema));

        // Log when starting S3 upload process
        if is_s3_storage {
            tracing::info!(
                "Starting S3 upload to snapshot {} for table {} (writer target file size: {})",
                snapshot_id,
                self.table_metadata.table_name,
                format_bytes(target_size_bytes)
            );
        }

        let tracked_schema = Arc::clone(&self.table_metadata.schema);
        let tracked_stream = {
            let total_bytes_written = Arc::clone(&total_bytes_written);
            let total_rows_written = Arc::clone(&total_rows_written);
            let last_progress_ms = Arc::clone(&last_progress_ms);
            let stats_acc = Arc::clone(&stats_accumulator);
            let table_name = self.table_metadata.table_name.clone();
            let start = start_time;

            stream.map(move |batch_result| {
                if let Ok(batch) = &batch_result {
                    total_bytes_written.fetch_add(batch.get_array_memory_size(), Ordering::Relaxed);
                    total_rows_written.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                    stats_acc.update(batch);

                    if is_s3_storage {
                        let elapsed = start.elapsed();
                        let elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
                        let last_logged = last_progress_ms.load(Ordering::Relaxed);
                        if elapsed_ms.saturating_sub(last_logged) >= 10_000 {
                            let bytes_so_far = total_bytes_written.load(Ordering::Relaxed);
                            let throughput = if elapsed.as_secs_f64() > 0.0 {
                                #[expect(clippy::cast_precision_loss)]
                                let bytes_per_sec = bytes_so_far as f64 / elapsed.as_secs_f64();
                                format_bytes_per_sec(bytes_per_sec)
                            } else {
                                "calculating...".to_string()
                            };
                            tracing::info!(
                                "S3 upload for {}: streamed {} in {:.1}s, {}",
                                table_name,
                                format_bytes(bytes_so_far),
                                elapsed.as_secs_f64(),
                                throughput
                            );
                            last_progress_ms.store(elapsed_ms, Ordering::Relaxed);
                        }
                    }
                }
                batch_result
            })
        };

        let tracked_stream =
            RecordBatchStreamAdapter::new(Arc::clone(&tracked_schema), tracked_stream);
        // The Vortex sink performs intra-write sharding internally (see
        // `write_shard_format` / `ShardSpec`), so the writer input is a single
        // coordinated stream — no upstream round-robin repartition, which would
        // only be coalesced back to one stream before the `DataSinkExec` anyway.
        let writer_input_plan: Arc<dyn ExecutionPlan> =
            Arc::new(StreamingExec::new(tracked_schema, Box::pin(tracked_stream)));

        let insert_plan = snapshot_listing_table
            .insert_into(session_state.as_ref(), writer_input_plan, InsertOp::Append)
            .await?;

        collect(insert_plan, session_state.task_ctx()).await?;

        let total_rows = total_rows_written.load(Ordering::Relaxed);
        // Files added ≈ number of concurrent shard writers (each writes ≥1 file
        // when it receives rows). Drives only the compaction-trigger heuristic.
        // Must use the SAME size-aware count `write_shard_format` used above, so
        // a small write that produced a single file reports `1` here (not the
        // full write concurrency), keeping the heuristic accurate.
        let writer_ops = if total_rows > 0 {
            self.snapshot_shard_count(target_partitions, target_size_bytes, estimated_bytes)
        } else {
            0
        };

        // Log final summary for S3 Express uploads
        if is_s3_storage {
            let elapsed = start_time.elapsed();
            let total_bytes = total_bytes_written.load(Ordering::Relaxed);
            let throughput = if elapsed.as_secs_f64() > 0.0 {
                #[expect(clippy::cast_precision_loss)]
                let bytes_per_sec = total_bytes as f64 / elapsed.as_secs_f64();
                format_bytes_per_sec(bytes_per_sec)
            } else {
                "N/A".to_string()
            };
            tracing::info!(
                "Completed S3 upload for {} to snapshot {}: {} rows across {} writer operation(s) ({}) in {:.1}s, {}",
                self.table_metadata.table_name,
                snapshot_id,
                total_rows,
                writer_ops,
                format_bytes(total_bytes),
                elapsed.as_secs_f64(),
                throughput
            );
        }

        // Track new files created in the *current* (non-staging) snapshot for
        // the cheap early-out in the compaction trigger. Only count files
        // landed in the live snapshot; staging writes are tracked separately
        // via the staging_may_have_files flag.
        if !Self::is_staging_snapshot_id(snapshot_id) && writer_ops > 0 {
            self.record_current_snapshot_files_added(writer_ops);
        }

        Ok((total_rows, writer_ops, stats_accumulator))
    }

    /// Effective number of concurrent shard writers the Vortex sink will use for
    /// a snapshot write. Returns `1` for sorted rewrites (sharding a
    /// globally-sorted stream would scatter its order across files); otherwise
    /// the count is *size-aware*:
    ///
    /// - `estimated_bytes == Some(n)`: shard into `n / encode_shard_unit`
    ///   writers, clamped to `[1, write_concurrency]`, where the unit is
    ///   `target_size_bytes / 16` floored at 16 MiB (and never above
    ///   `target_size_bytes`). Sharding by the full target *file* size
    ///   serialized every write smaller than one target file (256 MB on the
    ///   tuned tables) onto a single encode core — orders of magnitude coarser
    ///   than the row-group/part scale at which columnar engines parallelize
    ///   encode. The unit still keeps small CDC deltas as a single file —
    ///   sharding a ~100-row delta into N files buys no encode parallelism and
    ///   only multiplies the per-scan read amplification of the resulting
    ///   protected snapshot — while a checkpoint-sized flush (≥2× the unit)
    ///   earns real fan-out; compaction (pinned to a single output shard)
    ///   merges the transient sub-target files it emits.
    /// - `estimated_bytes == None`: the write size is unknown (opaque stream),
    ///   so preserve the prior behavior and shard across the full
    ///   `write_concurrency`. Callers that materialize the data (or buffer a
    ///   lower bound on it) supply a real estimate; only genuinely-unsized
    ///   streams fall back here.
    ///
    /// **Units (deliberately asymmetric).** `estimated_bytes` is *uncompressed
    /// in-memory Arrow* size (`RecordBatch::get_array_memory_size`), while
    /// `target_size_bytes` is the target *on-disk Vortex* file size. Vortex
    /// compresses, so `arrow_bytes / vortex_target` over-counts the files a write
    /// will actually produce — i.e. it biases toward *more* shards. That is the
    /// intended, safe direction: the surplus is bounded by `write_concurrency`,
    /// extra encode parallelism is free on a multi-core host, and the transient
    /// sub-target files it emits are merged by compaction (which is pinned to a
    /// single output shard). The opposite error — discounting for compression and
    /// then *under*-sharding a genuinely large, incompressible write into one
    /// oversized, serially-encoded file — is the costly one, so we do not apply a
    /// compression factor here. A faithful on-disk count would require an
    /// *empirical* bytes/row ratio derived from the table's existing files; the
    /// raw-Arrow estimate is preferred over a guessed constant. In practice the
    /// over-count rarely bites: steady-state CDC deltas are smaller than one
    /// target file and floor to a single shard regardless of the unit.
    ///
    /// Also drives the compaction-trigger file-add heuristic, so the same count
    /// must be used both to build the write format and to report files added.
    pub(super) fn snapshot_shard_count(
        &self,
        session_target_partitions: usize,
        target_size_bytes: usize,
        estimated_bytes: Option<u64>,
    ) -> usize {
        if self.context.has_sort_columns() {
            return 1;
        }
        let write_concurrency = self.snapshot_write_concurrency(session_target_partitions);
        match estimated_bytes {
            Some(bytes) => {
                // Encode-shard unit (see the doc comment): `target_size / 16`
                // floored at 16 MiB and capped at `target_size`, so the count
                // is "how many encode-efficient shards would this write fill?"
                // rather than "how many full target files?". The estimate is
                // (compression-blind) in-memory Arrow bytes — see the doc
                // comment on the deliberate Arrow-vs-Vortex unit asymmetry
                // that biases this toward more shards. `target_size_bytes` is
                // derived from a configured MiB value and is never 0, but
                // guard against it so a misconfiguration can't divide-by-zero.
                const MIN_ENCODE_SHARD_BYTES: u64 = 16 * 1024 * 1024;
                let target = u64::try_from(target_size_bytes).unwrap_or(u64::MAX).max(1);
                let unit = (target / 16).clamp(MIN_ENCODE_SHARD_BYTES.min(target), target);
                let files = (bytes / unit).max(1);
                let upper = u64::try_from(write_concurrency).unwrap_or(u64::MAX);
                usize::try_from(files.min(upper)).unwrap_or(write_concurrency)
            }
            None => write_concurrency,
        }
    }

    /// Build the write-path Vortex format, enabling intra-write sharding
    /// (parallel encode) for this snapshot write. Sorted rewrites and
    /// single-writer configs return the base (scan) format unchanged
    /// (`ShardSpec::Single`). Keyed/upsert tables hash rows by primary key so
    /// each output file is PK-clustered; other tables shard round-robin.
    ///
    /// Returned formats are write-only: the scan path keeps using
    /// `context.file_format()` and never observes the write-shard config.
    ///
    /// The shard count is size-aware (see [`Self::snapshot_shard_count`]):
    /// `target_size_bytes` / `estimated_bytes` decide how many writers a write
    /// of this size earns, so the same values must be passed here and to the
    /// `writer_ops` heuristic in [`Self::write_to_snapshot`].
    pub(super) fn write_shard_format(
        &self,
        session_target_partitions: usize,
        target_size_bytes: usize,
        estimated_bytes: Option<u64>,
    ) -> Arc<VortexFormat> {
        let base = self.context.file_format();
        match self.write_shard_config(
            session_target_partitions,
            target_size_bytes,
            estimated_bytes,
        ) {
            Some(config) => Arc::new(base.with_write_shard(config)),
            None => Arc::clone(base),
        }
    }

    /// The intra-write shard configuration this write earns, or `None` for a
    /// single serial writer. Shared by [`Self::write_shard_format`] (default
    /// encoding) and the delta-encoding strategy-override path
    /// (`CayenneContext::write_format_with_strategy`) so the two write paths
    /// produce identically-sharded output formats.
    pub(super) fn write_shard_config(
        &self,
        session_target_partitions: usize,
        target_size_bytes: usize,
        estimated_bytes: Option<u64>,
    ) -> Option<WriteShardConfig> {
        let shard_count = self.snapshot_shard_count(
            session_target_partitions,
            target_size_bytes,
            estimated_bytes,
        );
        if shard_count <= 1 {
            return None;
        }
        let shard_key_columns = self
            .pk_column_indices
            .iter()
            .filter_map(|&i| {
                self.table_metadata
                    .schema
                    .fields()
                    .get(i)
                    .map(|f| f.name().clone())
            })
            .collect::<Vec<_>>();
        Some(WriteShardConfig {
            write_concurrency: shard_count,
            shard_key_columns,
        })
    }

    /// Requested number of intra-write shards (parallel encoders) for a snapshot
    /// write: the per-table `cayenne_write_concurrency` override if set, else a
    /// conservative default of [`DEFAULT_WRITE_CONCURRENCY`] (capped at the host
    /// core count so tiny hosts don't over-shard).
    ///
    /// The default is deliberately NOT the host core count. `write_concurrency` is
    /// sized per table in isolation, so a default of "all cores" makes independent
    /// tables oversubscribe the box under concurrent CDC — the aggregate is the
    /// sum across every writing table, not the per-table value. A small default
    /// keeps that sum sane out of the box; users raise `cayenne_write_concurrency`
    /// explicitly when a table needs more encode parallelism (and the process-wide
    /// encode budget still bounds the aggregate — see `provider::write_budget`).
    ///
    /// This is the *requested* count. `VortexFormat::build_shard_spec` then clamps
    /// it to the write session's `target_partitions` — the host's logical-core
    /// count (see [`Self::create_session_context`]) — so a configured value above
    /// the core count is capped, not honored. That ceiling is intentional:
    /// parallel encode is CPU-bound, so extra shards would only add files (read
    /// amplification) without speeding the write.
    pub(super) fn snapshot_write_concurrency(&self, session_target_partitions: usize) -> usize {
        let default = DEFAULT_WRITE_CONCURRENCY.min(session_target_partitions.max(1));
        self.context.write_concurrency().unwrap_or(default).max(1)
    }

    /// Create a clone of necessary fields for parallel write tasks.
    ///
    /// This method clones only the Arc references needed for writing,
    /// which is cheap (just atomic reference count increments).
    ///
    /// # Note on Retention Filters
    ///
    /// Retention filters are preserved in the clone because they need to be applied
    /// by `insert()` at the end of each write operation. The `insert()` method holds
    /// the write lock and applies retention atomically after all parallel chunk writes
    /// complete.
    ///
    /// This design provides ACID semantics:
    /// - Retention filters are table-wide predicates (e.g., "delete rows older than 30 days")
    /// - They must scan all table data, not just the newly written chunks
    /// - The write lock ensures atomicity: all writes + retention happen as one operation
    pub(crate) fn clone_for_write(&self) -> Self {
        Self {
            table_metadata: self.table_metadata.clone(),
            catalog: Arc::clone(&self.catalog),
            listing_table: Arc::clone(&self.listing_table),
            listing_fence: Arc::clone(&self.listing_fence),
            scan_file_statistics: Arc::clone(&self.scan_file_statistics),
            table_statistics: Arc::clone(&self.table_statistics),
            table_statistics_persistence_lock: Arc::clone(&self.table_statistics_persistence_lock),
            context: Arc::clone(&self.context),
            retention_filters: self.retention_filters.clone(),
            time_retention_filter_builder: self.time_retention_filter_builder.clone(),
            pk_deletion_strategy: self.pk_deletion_strategy.clone(),
            pk_row_converter: self.pk_row_converter.as_ref().map(Arc::clone),
            pk_column_indices: self.pk_column_indices.clone(),
            write_lock: Arc::clone(&self.write_lock), // Shared across all clones for same table
            visibility_lock: Arc::clone(&self.visibility_lock),
            scan_state_lock: Arc::clone(&self.scan_state_lock),
            object_store_config: self.object_store_config.clone(),
            object_store_registered_runtime_envs: Arc::clone(
                &self.object_store_registered_runtime_envs,
            ),
            current_snapshot_id: Arc::clone(&self.current_snapshot_id),
            protected_snapshots: Arc::clone(&self.protected_snapshots),
            protected_snapshot_age_warning_keys: Arc::clone(
                &self.protected_snapshot_age_warning_keys,
            ),
            pk_keyset_cache: Arc::clone(&self.pk_keyset_cache),
            table_memory: Arc::clone(&self.table_memory),
            inline_checkpoint_scheduled: Arc::clone(&self.inline_checkpoint_scheduled),
            inlined_row_count: Arc::clone(&self.inlined_row_count),
            inlined_generation: Arc::clone(&self.inlined_generation),
            inlined_structural_epoch: Arc::clone(&self.inlined_structural_epoch),
            pending_inline_tombstones: Arc::clone(&self.pending_inline_tombstones),
            published_inlined_seq: Arc::clone(&self.published_inlined_seq),
            // Shared so every writer clone of the same table allocates from one
            // monotone source (lever B2) — memory and the DB row never diverge.
            seq_allocator: Arc::clone(&self.seq_allocator),
            inlined_locally_published: Arc::clone(&self.inlined_locally_published),
            pending_durable_tombstone_flips: Arc::clone(&self.pending_durable_tombstone_flips),
            pending_tombstone_deltas: Arc::clone(&self.pending_tombstone_deltas),
            inlined_cache: Arc::clone(&self.inlined_cache),
            // Shared so every writer clone appends to / checkpoints the SAME
            // in-memory CDC tier and observes the same slot-advancer handle.
            mem_tier: Arc::clone(&self.mem_tier),
            mem_checkpoint_lock: Arc::clone(&self.mem_checkpoint_lock),
            slot_advancer: Arc::clone(&self.slot_advancer),
            mem_tier_max_bytes: self.mem_tier_max_bytes,
            mem_tier_max_age_ms: self.mem_tier_max_age_ms,
            staging_wal_present: Arc::clone(&self.staging_wal_present),
            staging_may_have_files: Arc::clone(&self.staging_may_have_files),
            inflight_staging_appends: Arc::clone(&self.inflight_staging_appends),
            new_files_since_last_compaction: Arc::clone(&self.new_files_since_last_compaction),
            // Shared so a writer clone's move records the published files where the
            // (same-table) publish on any clone can delta-apply them.
            last_moved_snapshot_files: Arc::clone(&self.last_moved_snapshot_files),
            // Shared so inline (write-driven) and background compaction
            // attempts on the same table coordinate, even across clones.
            compaction_lock: Arc::clone(&self.compaction_lock),
            post_write_compaction_scheduled: Arc::clone(&self.post_write_compaction_scheduled),
            post_write_maintenance: Arc::clone(&self.post_write_maintenance),
            background_compactor: Arc::clone(&self.background_compactor),
            // Shared so the single periodic checkpoint task (spawned on the
            // original `Arc`) survives writer clones and its drop signal is shared.
            background_mem_tier_checkpointer: Arc::clone(&self.background_mem_tier_checkpointer),
        }
    }

    /// Reserve `count` (>= 1) consecutive sequence numbers in-memory (lever B2).
    /// Returns the FIRST of the contiguous block `[first, first + count)`.
    ///
    /// Acquires the metastore writer ONLY when the in-memory block is exhausted
    /// (amortized ~1/`SEQ_RESERVE_BLOCK` reservations); the common path is a
    /// `tokio::sync::Mutex` lock + arithmetic with no `await`, so it removes the
    /// per-batch writer-queue wait that `stage_seq_reserve` measured.
    ///
    /// # Correctness (monotonicity-on-reopen)
    ///
    /// The refill durably bumps the DB high-water (`reserve_sequence_numbers`,
    /// an fsynced autocommit `UPDATE … += bump … RETURNING`) and sets
    /// `persisted_hi = new_hi` BEFORE advancing `next` past the reserved range,
    /// so the invariant `next - 1 <= persisted_hi` holds at every point an
    /// outside observer (another task, or the next process after a crash) can
    /// read either value. A crash therefore wastes at most `SEQ_RESERVE_BLOCK -
    /// 1` sequences (the unused tail of the last block) but reseeds from a DB
    /// row that is `>=` every value ever handed out — no value is ever reissued.
    /// The lock is held across the refill `await`, so concurrent bursts cannot
    /// double-refill or observe a torn `persisted_hi`, and a multi-writer race
    /// is resolved by the metastore-serialized `+=` returning the authoritative
    /// high-water (each process re-bases `next` to the start of the block it
    /// durably claimed, so processes get disjoint blocks).
    pub(crate) async fn reserve_sequences_local(&self, count: u32) -> CatalogResult<i64> {
        reserve_sequences_in(
            &self.seq_allocator,
            &self.catalog,
            &self.table_metadata.table_id,
            &self.table_metadata.table_name,
            count,
        )
        .await
    }
}
