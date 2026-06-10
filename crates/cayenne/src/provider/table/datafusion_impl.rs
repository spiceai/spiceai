//! `TableProvider` trait implementation and file-based delete entry points.
//!
//! `scan()` holds `listing_fence.read()` across plan-build so the file listing,
//! deletion snapshot, protected-snapshot map, and mem-tier capture are atomic
//! against a writer barrier, and captures the (deletion view, protected map,
//! inlined view) triple under `scan_state_lock.read()`. `insert_into` delegates
//! to `CayenneDataSink`; `delete_from` routes between file-based, position-based
//! and key-based deletion sinks (the sinks take `write_lock` themselves).
//! The sync `statistics`/`supports_filters_pushdown` never take the fence —
//! they read `ArcSwap`/cached snapshots.
//!
//! Mechanically split out of `provider/table.rs`; code is moved verbatim.

use std::any::Any;

use data_components::delete::DeletionSink;
use datafusion_catalog::{Session, TableProvider};
use datafusion_physical_plan::ExecutionPlan;

use super::{
    Arc, CayenneAccelerationExec, CayenneDataSink, CayenneDeletionSink, CayenneTableProvider,
    CoalescePartitionsExec, Constraints, Cow, DataSinkExec, DeletionExec, Expr,
    FileBasedDeletionSink, GlobalLimitExec, HashMap, InlineAwareDeletionSink, InsertOp, Instant,
    ListingTable, LocalLimitExec, LogicalPlan, ObjectStoreExt, Operator, Ordering,
    PkKeysetInvalidatingDeletionSink, ProtectedSnapshotScan, RecordBatch, SchemaRef, SessionConfig,
    TableProviderFilterPushDown, TableType, TryStreamExt, UnionExec, async_trait,
    expr_applicable_for_cols, rewritten_scan_filters, round_robin_repartition_if_needed,
};

#[async_trait]
impl TableProvider for CayenneTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::<arrow_schema::Schema>::clone(&self.table_metadata.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn constraints(&self) -> Option<&Constraints> {
        None
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        // Register object store with the session's runtime env if configured for S3 Express One Zone.
        // This ensures the session can access S3 when the underlying ListingTable reads data.
        if let Some(ref config) = self.object_store_config {
            self.register_object_store_for_runtime(state.runtime_env(), config);
        }

        // Warm the inlined cache before taking the consistency guards so the
        // read under `scan_state_lock` is a cheap cache hit in the common case.
        // If a writer invalidates the cache after this point, the guarded
        // capture below drops the guard, rebuilds, and retries.
        if self.cached_inlined_row_count() > 0 {
            self.read_inlined_batches().await.map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to warm inlined data cache for table {}: {e}",
                    self.table_metadata.table_name
                ))
            })?;
        }

        // Hold listing_fence.read() for the remainder of this scan's plan-build
        // so the deletion snapshot, the protected_snapshots set, and the current
        // snapshot's file listing are all captured atomically against a
        // concurrent writer barrier. A staged CDC upsert publishes its file move,
        // its deletion caches, and protected_snapshots under listing_fence.write()
        // (CayenneCdcWrite::finish), so holding the read fence across all three
        // captures here guarantees this scan observes that publish either fully
        // or not at all — never the new snapshot's rows without the deletes that
        // hide the old versions. Concurrent scans share the read fence; only a
        // writer barrier holding the write fence blocks them, and vice versa.
        let listing_fence_wait_start = Instant::now();
        let _fence = self.listing_fence.read().await;
        self.record_listing_fence_wait_duration(listing_fence_wait_start.elapsed());

        // Capture the in-memory CDC tier under the SAME held read fence (an O(1)
        // `Arc` load — no copy), so a memory-mode scan observes the RAM rows
        // atomically against a concurrent append/checkpoint that swaps the tier
        // under `listing_fence.write()`. Empty (and skipped below) in file mode,
        // so the file-mode plan is byte-identical.
        let mem_tier_snapshot = self.mem_tier.load_full();

        // Capture the (deletion view, protected snapshot map, inlined data)
        // triple atomically under `scan_state_lock.read()`. This serializes with
        // non-staged publish paths that update the deletion view and protected
        // snapshot map without taking the listing fence.
        let (deletion_snapshot, protected_map, inlined_view) = loop {
            let captured = {
                let _view_guard = self.scan_state_lock.read().await;
                self.try_read_inlined_view_for_scan().map(|inlined_view| {
                    (
                        self.pk_deletion_snapshot(),
                        self.protected_snapshots.load_full(),
                        inlined_view,
                    )
                })
            };

            if let Some(captured) = captured {
                break captured;
            }

            self.read_inlined_batches().await.map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to read inlined data for table {}: {e}",
                    self.table_metadata.table_name
                ))
            })?;
        };
        let deletion_snapshot = deletion_snapshot.with_mem_tier_tombstones(&mem_tier_snapshot);
        let need_pk_deletion = deletion_snapshot.has_deletions();

        // For PK-based deletion, we need to ensure PK columns are included in the projection
        // so we can filter by key. We may need to strip them out afterward if they weren't
        // originally requested.
        let (effective_projection, pk_indices_in_projection, need_projection_strip) =
            if need_pk_deletion {
                if let Some(proj) = projection {
                    // Check which PK columns are missing from the projection
                    let mut extended_proj: Vec<usize> = proj.clone();
                    let mut pk_indices: Vec<usize> =
                        Vec::with_capacity(self.pk_column_indices.len());
                    let mut added_columns = false;

                    for &pk_idx in &self.pk_column_indices {
                        if let Some(pos) = extended_proj.iter().position(|&p| p == pk_idx) {
                            // PK column already in projection
                            pk_indices.push(pos);
                        } else {
                            // PK column not in projection - add it at the end
                            pk_indices.push(extended_proj.len());
                            extended_proj.push(pk_idx);
                            added_columns = true;
                        }
                    }

                    (Some(extended_proj), pk_indices, added_columns)
                } else {
                    // No projection means all columns are selected
                    (None, self.pk_column_indices.clone(), false)
                }
            } else {
                // No PK-based deletion needed, use original projection
                let pk_indices = if let Some(proj) = projection {
                    self.pk_column_indices
                        .iter()
                        .filter_map(|&orig_idx| {
                            proj.iter().position(|&proj_idx| proj_idx == orig_idx)
                        })
                        .collect()
                } else {
                    self.pk_column_indices.clone()
                };
                (projection.cloned(), pk_indices, false)
            };

        // Time-based retention: build a keep filter at scan time.
        // Prefer the builder (produces correctly-typed timestamps matching the
        // column's timezone) over the legacy Expr+simplify path.
        // Injected at two layers:
        // 1. Appended to scan filters for file-level statistics pruning (Vortex should_prune)
        // 2. Wrapped as a physical FilterExec for row-level filtering
        let retention_keep_filter = if let Some(ref builder) = self.time_retention_filter_builder {
            let filter = builder.keep_filter();
            let filter = util::expr::simplify_expr(filter, &self.table_metadata.schema)?;
            Some(filter)
        } else {
            None
        };

        // Ensure columns referenced by the retention filter are in the projection.
        // Similar to PK column handling: if the user's query doesn't SELECT the time
        // column, we add it for FilterExec and strip it afterward.
        let (effective_projection, need_projection_strip) =
            if let Some(ref keep_filter) = retention_keep_filter {
                self.extend_projection_for_retention_filter(
                    effective_projection,
                    keep_filter,
                    need_projection_strip,
                )
            } else {
                (effective_projection, need_projection_strip)
            };

        // Build effective scan filters: user filters + optional retention filter.
        // Also rewrite IN-lists of consecutive integers to BETWEEN ranges — both
        // are semantically equivalent but the range path is ~50 % cheaper per
        // row (two `i64` comparisons vs an N-element set probe). See
        // `pk_in_list_vs_range_rewrite` bench.
        let effective_filters = rewritten_scan_filters(filters, retention_keep_filter.as_ref());
        let mut scan_filters_owned = effective_filters.unwrap_or_else(|| filters.to_vec());
        let mem_tier_pruning_filters = scan_filters_owned.clone();
        if let Some(tombstone_filter) = self.vortex_key_delete_pushdown_filter(&deletion_snapshot) {
            tracing::trace!(
                table = %self.table_metadata.table_name,
                "Injected sparse key-delete tombstone exclusion into Vortex scan filters"
            );
            scan_filters_owned.push(tombstone_filter);
        }
        let scan_filters: &[Expr] = &scan_filters_owned;
        if retention_keep_filter.is_some() {
            tracing::trace!(
                table = %self.table_metadata.table_name,
                total_filters = scan_filters.len(),
                "Injected time_retention keep-filter into scan filters"
            );
        }

        let mem_tier_pruning_predicate = super::file_pruning::build_listing_pruning_predicate(
            &self.table_metadata.schema,
            &mem_tier_pruning_filters,
        )?;
        let inlined_batches = self
            .pruned_inlined_batches(
                &inlined_view,
                &mem_tier_snapshot,
                mem_tier_pruning_predicate.as_ref(),
            )
            .map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to apply in-memory CDC tier tombstone visibility to inlined data for table {}: {e}",
                    self.table_metadata.table_name
                ))
            })?;

        // For PK point lookups (e.g. `WHERE pk_col = K`), force the inner
        // `ListingTable` to use `target_partitions = 1` so DataFusion does NOT
        // byte-range-split the matching file across N file_groups. The fan-out
        // pays per-group Vortex footer-open cost (~50 µs each) without speeding
        // up the lookup because only one chunk in one file_group actually
        // contains K. See `pk_lookup_file_group_fanout` bench.
        let scan_listing_config_override;
        let scan_listing_config = if self.is_pk_selective_scan(scan_filters) {
            scan_listing_config_override = state.config().clone().with_target_partitions(1);
            &scan_listing_config_override
        } else {
            state.config()
        };

        // `listing_fence.read()` is already held (acquired at the top of this
        // scan so the deletion snapshot, protected_snapshots, and the file
        // listing are captured atomically against a concurrent writer barrier —
        // #10125 §6.4). The plan is built from the live current_snapshot_id so it
        // can apply per-scan DataFusion config (target_partitions, etc.).
        let current_snapshot_id = self.get_current_snapshot_id();
        let listing_scan_start = Instant::now();
        let main_plan_result = self
            .create_snapshot_scan_plan_with_config(
                state,
                &current_snapshot_id,
                effective_projection.as_ref(),
                scan_filters,
                limit,
                scan_listing_config,
            )
            .await;
        self.record_listing_scan_duration(listing_scan_start.elapsed());
        let main_plan = main_plan_result?;
        // Note: we deliberately keep `_fence` alive until after the main plan
        // has been built (i.e. until end of this function). Direct scan
        // planning resolves the file listing eagerly, so the fence really only
        // needs to outlive `create_snapshot_scan_plan(...).await`; we hold it
        // slightly longer for clarity and to avoid micro-optimizing a
        // microsecond-scale wait.

        // Check for protected snapshots that need to be scanned with partial deletion filter.
        let protected_snapshot_plans = self
            .scan_protected_snapshots(ProtectedSnapshotScan {
                state,
                projection: effective_projection.as_ref(),
                filters: scan_filters,
                limit,
                pk_indices_in_projection: &pk_indices_in_projection,
                protected_snapshots: protected_map,
                deletion_snapshot: &deletion_snapshot,
            })
            .await?;

        // Build a MemoryExec plan for the inlined data captured under the
        // read guard above (consistent with the deletion view used for the
        // file-backed deletion filter).
        let inlined_plan: Option<Arc<dyn ExecutionPlan>> = if inlined_batches.is_empty() {
            None
        } else {
            // Apply projection to inlined batches if needed
            let proj_schema = if let Some(ref proj) = effective_projection {
                let schema_fields = self.table_metadata.schema.fields();
                let fields: Vec<arrow_schema::FieldRef> = proj
                    .iter()
                    .map(|&i| Arc::clone(&schema_fields[i]))
                    .collect();
                Arc::new(arrow_schema::Schema::new(fields))
            } else {
                Arc::clone(&self.table_metadata.schema)
            };

            let projected_batches: Vec<RecordBatch> = inlined_batches
                .into_iter()
                .map(|batch| {
                    if let Some(ref proj) = effective_projection {
                        batch.project(proj).map_err(|e| {
                            datafusion_common::DataFusionError::Execution(format!(
                                "Failed to project inlined batch for table {}: {e}",
                                self.table_metadata.table_name
                            ))
                        })
                    } else {
                        Ok(batch)
                    }
                })
                .collect::<datafusion_common::Result<Vec<_>>>()?;

            if projected_batches.is_empty() {
                None
            } else {
                let inline_exec: Arc<dyn ExecutionPlan> =
                    datafusion::datasource::memory::MemorySourceConfig::try_new_exec(
                        &[projected_batches],
                        proj_schema,
                        None,
                    )?;
                // Pre-filter the inline branch with the query's scan filters so
                // the post-scan FilterExec can be pushed away (this MemoryExec
                // does not support filter pushdown on its own). The user filters
                // are evaluated by a real FilterExec, so every returned inline
                // row matches the predicate — correct under active inline CDC.
                Some(self.wrap_memory_branch_with_scan_filters(inline_exec, filters))
            }
        };

        // Build a MemoryExec for the in-memory CDC tier captured under the read
        // fence, applying the tier's own tombstones merge-on-read (the same
        // `filter_inlined_batch_for_deletions` path the durable inline corpus
        // uses; only the tombstone SOURCE differs — the in-RAM map vs the
        // metastore). `None` (and skipped) in file mode, where the tier is empty.
        let mem_plan: Option<Arc<dyn ExecutionPlan>> = self
            .build_mem_tier_scan_plan(
                &mem_tier_snapshot,
                effective_projection.as_ref(),
                mem_tier_pruning_predicate.as_ref(),
            )?
            .map(|mem_exec| self.wrap_memory_branch_with_scan_filters(mem_exec, filters));

        // Build the final plan:
        // - If protected snapshots exist: deletion filter on main, UNION with snapshots
        // - Otherwise: apply deletion filter directly to main plan
        // - If inlined data exists: UNION with inlined data plan
        let plan = if protected_snapshot_plans.is_empty() {
            self.apply_deletion_filter_with_insert_records(
                main_plan,
                &pk_indices_in_projection,
                &deletion_snapshot,
            )?
        } else {
            let filtered_main_plan = self.apply_deletion_filter(
                main_plan,
                &pk_indices_in_projection,
                &deletion_snapshot,
            )?;

            let mut all_plans = vec![filtered_main_plan];
            all_plans.extend(protected_snapshot_plans);
            UnionExec::try_new(all_plans)?
        };

        // Union inlined data if present
        let plan: Arc<dyn ExecutionPlan> = if let Some(inline_exec) = inlined_plan {
            UnionExec::try_new(vec![plan, inline_exec])?
        } else {
            plan
        };

        // Union the in-memory CDC tier if present (memory mode).
        let plan: Arc<dyn ExecutionPlan> = if let Some(mem_exec) = mem_plan {
            UnionExec::try_new(vec![plan, mem_exec])?
        } else {
            plan
        };

        // Wrap with FilterExec for time retention. DataFusion's physical optimizer
        // pushes FilterExec predicates down through the plan tree (including through
        // UnionExec) into each child's VortexSource via `try_pushdown_filters`,
        // enabling file-level pruning via min/max stats and row-level filtering.
        let plan: Arc<dyn ExecutionPlan> = if let Some(ref keep_filter) = retention_keep_filter {
            self.wrap_plan_with_retention_filter(plan, keep_filter)?
        } else {
            plan
        };

        let target_partitions = state.config().target_partitions();
        let mut plan: Arc<dyn ExecutionPlan> = if scan_filters.is_empty() && limit.is_none() {
            round_robin_repartition_if_needed(Arc::clone(&plan), target_partitions)?.unwrap_or(plan)
        } else {
            plan
        };

        plan = if let Some(limit) = limit {
            let local_limit: Arc<dyn ExecutionPlan> = Arc::new(LocalLimitExec::new(plan, limit));
            let single_partition: Arc<dyn ExecutionPlan> =
                Arc::new(CoalescePartitionsExec::new(local_limit));
            Arc::new(GlobalLimitExec::new(single_partition, 0, Some(limit)))
        } else {
            plan
        };

        // Strip extra columns (PK or retention time column) added to the projection
        // but not originally requested by the query.
        if need_projection_strip && let Some(orig_proj) = projection {
            return self.create_projection_strip(plan, orig_proj.len());
        }

        Ok(Arc::new(CayenneAccelerationExec::new(plan)))
    }

    // Filter-pushdown exactness contract (read before changing the arms below):
    //
    // * Partition-column filters → `Exact`. Partition pruning eliminates whole
    //   partition directories during file listing, so every row the scan can
    //   return provably satisfies the predicate. There is no inline/RAM-tier
    //   partitioning, but partition tables also never inline (the staged path is
    //   forced for partitioned tables — see `write_cdc_pipelined`), so a
    //   partition filter can never leak an unfiltered inline row.
    //
    // * All other (data-column) filters → `Inexact`, deliberately, NOT `Exact`.
    //   The file-backed branch applies a data predicate ONLY when DataFusion's
    //   post-scan `FilterExec` is pushed into `VortexSource::try_pushdown_filters`
    //   by the physical optimizer; nothing in `scan()` itself can set the Vortex
    //   predicate (its fields are crate-private to `vortex-datafusion`). Reporting
    //   `Exact` would make DataFusion DROP that `FilterExec` at the logical level,
    //   leaving the file branch unfiltered for any predicate Vortex cannot convert
    //   (`can_be_pushed_down == false`) — a correctness bug. So we keep `Inexact`
    //   ("uncertain ⇒ Inexact").
    //
    //   The post-scan `FilterExec` is still NOT a permanent tax under active
    //   inline CDC: `scan()` now wraps each in-memory branch (inline corpus + RAM
    //   tier) with its own `FilterExec` for the same predicate
    //   (`wrap_memory_branch_with_scan_filters`). Once every union child applies
    //   the predicate (Vortex via pushdown, memory branches via their own
    //   `FilterExec`), the physical `FilterPushdown` rule's `if_all` succeeds and
    //   removes the redundant post-scan `FilterExec` for every Vortex-convertible
    //   predicate — achieving the drop without the `Exact` correctness hazard.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion_common::Result<Vec<TableProviderFilterPushDown>> {
        let options = Self::create_listing_options(
            self.context.file_format(),
            &self.pk_deletion_strategy,
            &SessionConfig::default(),
        );
        let partition_column_names = options
            .table_partition_cols
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>();

        filters
            .iter()
            .map(|filter| {
                if !partition_column_names.is_empty()
                    && expr_applicable_for_cols(&partition_column_names, filter)
                {
                    Ok(TableProviderFilterPushDown::Exact)
                } else {
                    Ok(TableProviderFilterPushDown::Inexact)
                }
            })
            .collect()
    }

    fn statistics(&self) -> Option<datafusion_common::Statistics> {
        // Prefer the metastore-persisted table statistics (loaded from Vortex
        // file footers) when present — they cover columns the ListingTable
        // does not expose synchronously without rescanning footers.
        //
        // `live_rows_delta` in `schedule_post_write_maintenance` updates the
        // persisted `num_rows` for both staged and inline write paths, so the
        // cached stats reflect the live row count after maintenance runs.
        if let Some(stats) = self.cached_table_statistics_for_optimizer() {
            return Some(stats);
        }

        // Defensive: if we somehow reach here with inline rows present (no
        // persisted stats AND `cached_table_statistics_for_optimizer` returned
        // None), the ListingTable alone would under-count the inline rows, so
        // return None rather than mislead the optimizer with a file-only count.
        if self.inlined_row_count.load(Ordering::Relaxed) > 0 {
            return None;
        }

        // Position deletes are applied inside the Vortex access plan. If no
        // cached table stats are available, the synchronous ListingTable stats
        // are raw file-footer stats and do not account for the pending bitmap.
        if self.pk_deletion_strategy.is_position_based() && self.has_pending_deletions() {
            return None;
        }

        // Fall back to the underlying ListingTable stats. Synchronous method:
        // wait-free ArcSwap snapshot is sufficient.
        let listing_table = self.listing_table.load_full();
        listing_table.statistics()
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        None
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        overwrite: InsertOp,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let is_s3 = self.table_metadata.path.starts_with("s3://");

        if is_s3 {
            tracing::info!(
                "Cayenne insert_into called for S3 table {} (overwrite: {:?})",
                self.table_metadata.table_name,
                overwrite
            );
        }

        // Register object store with the session's runtime env if configured for S3 Express One Zone.
        // This ensures the session can access S3 when the underlying ListingTable writes data.
        if let Some(ref config) = self.object_store_config {
            self.register_object_store_for_runtime(state.runtime_env(), config);
        } else if is_s3 {
            tracing::warn!(
                "S3 table {} has no object_store_config! Writes will fail.",
                self.table_metadata.table_name
            );
        }

        // For appends on local paths, ensure the snapshot directory exists before writing.
        // S3 creates paths on write automatically so this is only needed for local storage.
        if overwrite != InsertOp::Overwrite && !is_s3 {
            let current_snapshot = self.get_current_snapshot_id();
            let snapshot_dir = Self::snapshot_dir_path(
                &self.table_metadata.path,
                &self.table_metadata.table_id,
                &current_snapshot,
            );
            Self::ensure_snapshot_dir_exists(&snapshot_dir)
                .await
                .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
        }

        // Delegate entirely to CayenneDataSink which handles:
        // - Overwrite: new snapshot creation, catalog commit, state updates, cleanup
        // - Append: write lock, PK validation, on-conflict deletions, new snapshot
        //   when needed, retention filters, sort-and-rewrite, listing table refresh
        let sink = Arc::new(CayenneDataSink::new(
            self.clone_for_write(),
            overwrite,
            Arc::clone(&self.table_metadata.schema),
            Arc::clone(&self.context),
        ));

        Ok(Arc::new(DataSinkExec::new(input, sink, None)))
    }

    async fn delete_from(
        &self,
        _state: &dyn Session,
        filters: Vec<Expr>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        if self.file_based_deletes_preferred(&filters) {
            // File-based retention operates on listing table files. Materialize
            // pending inline rows first so retention can reason about file stats.
            {
                let _guard = self.write_lock.lock().await;
                self.checkpoint_inlined_data_if_present_for_delete().await?;
            }

            tracing::debug!(
                "Table '{}': using file-based retention delete path",
                self.table_metadata.table_name,
            );
            return self.delete_using_files(&filters);
        }

        if self.pk_deletion_strategy.is_position_based() {
            // Position-based deletion vectors target file-local row positions,
            // so no-PK inline rows must still be materialized before deletion.
            {
                let _guard = self.write_lock.lock().await;
                self.checkpoint_inlined_data_if_present_for_delete().await?;
            }

            return self.delete_using_deletion_vectors(&filters);
        }

        let file_sink = self.build_deletion_vector_sink(&filters, None)?;
        Ok(Arc::new(DeletionExec::new(Arc::new(
            InlineAwareDeletionSink {
                table: self.clone_for_write(),
                file_sink,
                filters,
            },
        ))))
    }

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let schema = self.schema();
        let table_source = Arc::new(datafusion::datasource::DefaultTableSource::new(Arc::new(
            self.clone_for_write(),
        )));
        let mut plan =
            datafusion_expr::LogicalPlanBuilder::scan("__update_source", table_source, None)?
                .build()?;

        if let Some(combined) = filters.clone().into_iter().reduce(Expr::and) {
            plan = datafusion_expr::LogicalPlanBuilder::from(plan)
                .filter(combined)?
                .build()?;
        }

        let assignment_by_col: HashMap<&str, &Expr> = assignments
            .iter()
            .map(|(name, expr)| (name.as_str(), expr))
            .collect();
        let mut proj_exprs = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let col_name = field.name();
            if let Some(expr) = assignment_by_col.get(col_name.as_str()) {
                proj_exprs.push((*expr).clone().alias(col_name));
            } else {
                proj_exprs.push(datafusion_expr::col(col_name));
            }
        }
        plan = datafusion_expr::LogicalPlanBuilder::from(plan)
            .project(proj_exprs)?
            .build()?;

        let source_plan = state.create_physical_plan(&plan).await?;
        let session_state = state
            .as_any()
            .downcast_ref::<datafusion::execution::SessionState>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "Session is not a SessionState".to_string(),
                )
            })?
            .clone();

        Ok(Arc::new(data_components::update::UpdateExec::new(
            source_plan,
            Arc::new(self.clone_for_write()),
            session_state,
            filters,
        )))
    }
}

impl CayenneTableProvider {
    /// File-level delete path.
    ///
    /// Creates a [`FileBasedDeletionSink`] that discovers eligible files
    /// (where `max(col) < threshold_value`) and deletes them from the main
    /// snapshot and all protected snapshot directories.
    pub(super) fn delete_using_files(
        &self,
        filters: &[Expr],
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        if filters.len() != 1 {
            return Err(datafusion_common::DataFusionError::Internal(format!(
                "delete_using_files requires exactly one filter, got {}",
                filters.len(),
            )));
        }
        let filter = &filters[0];

        // Build protected snapshot listing tables for PK-based strategies only.
        // Position-based tables have no protected snapshots.
        let protected_snapshot_tables = if self.pk_deletion_strategy.is_position_based() {
            None
        } else {
            Some(self.build_protected_snapshot_listing_tables()?)
        };

        let sink: Arc<dyn DeletionSink> = Arc::new(FileBasedDeletionSink::new(
            Arc::clone(&self.listing_table),
            protected_snapshot_tables,
            filter.clone(),
            self.table_metadata.table_name.clone(),
            Arc::clone(&self.catalog),
            Arc::clone(&self.protected_snapshots),
            self.table_metadata.table_id.clone(),
            self.table_metadata.path.clone(),
            Arc::clone(self.context.runtime_env()),
            Arc::clone(&self.write_lock),
            Arc::clone(&self.listing_fence),
        ));
        Ok(Arc::new(DeletionExec::new(Arc::new(
            PkKeysetInvalidatingDeletionSink {
                table: self.clone_for_write(),
                inner: sink,
            },
        ))))
    }

    /// Main deletion-vector path via [`CayenneDeletionSink`].
    pub(super) fn delete_using_deletion_vectors(
        &self,
        filters: &[Expr],
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let sink: Arc<dyn DeletionSink> =
            Arc::new(self.build_deletion_vector_sink(filters, Some(Arc::clone(&self.write_lock)))?);
        Ok(Arc::new(DeletionExec::new(Arc::new(
            PkKeysetInvalidatingDeletionSink {
                table: self.clone_for_write(),
                inner: sink,
            },
        ))))
    }

    pub(super) fn build_deletion_vector_sink(
        &self,
        filters: &[Expr],
        write_lock: Option<Arc<tokio::sync::Mutex<()>>>,
    ) -> datafusion_common::Result<CayenneDeletionSink> {
        let snapshot_tables: Vec<Arc<ListingTable>> = self
            .build_protected_snapshot_listing_tables()?
            .into_iter()
            .map(|(_, table)| table)
            .collect();

        Ok(CayenneDeletionSink::new(
            self.table_metadata.clone(),
            Arc::clone(&self.catalog),
            Arc::clone(&self.listing_table),
            Arc::clone(&self.table_metadata.schema),
            filters,
            self.pk_deletion_strategy.clone(),
            Arc::clone(&self.table_memory),
            self.pk_row_converter.as_ref().map(Arc::clone),
            self.pk_column_indices.clone(),
            snapshot_tables,
            Arc::clone(self.context.runtime_env()),
            write_lock,
            Arc::clone(&self.seq_allocator),
        ))
    }

    /// Delete rows by hash-probing key columns against a set of matched keys.
    ///
    /// Fast path for `MERGE INTO` on `PositionBased` tables. Bypasses filter
    /// construction and the O(N) filter-per-file evaluation. Instead, scans
    /// each file and performs O(1) `HashSet` lookups per row.
    ///
    /// Acquires the write lock to prevent concurrent writes/refreshes.
    ///
    /// # Errors
    ///
    /// Returns an error if the listing table lock cannot be read or if the
    /// underlying position-based deletion scan/persist operation fails.
    pub async fn delete_matched_rows_by_key_probe(
        &self,
        matched_keys: std::collections::HashSet<Vec<datafusion_common::ScalarValue>>,
        key_columns: &[String],
    ) -> datafusion_common::Result<u64> {
        let _write_guard = self.write_lock.lock().await;

        // MERGE key-probe deletes operate on listing-table files only, so
        // pending inlined rows must be materialized first.
        self.checkpoint_inlined_data_if_present_for_delete().await?;

        let ctx = self.create_session_context();
        // Wait-free ArcSwap snapshot. Refreshes are serialized against this
        // path by `self.write_lock`, held above.
        let listing_table = self.listing_table.load_full();

        // PositionBased tables have no protected snapshots, so we only scan the main listing table.
        let all_tables = vec![listing_table];

        // Build the deletion sink with write_lock=None (we already hold it).
        let sink = CayenneDeletionSink::new(
            self.table_metadata.clone(),
            Arc::clone(&self.catalog),
            Arc::clone(&self.listing_table),
            Arc::clone(&self.table_metadata.schema),
            &[], // no filters — positions are resolved by key probe
            self.pk_deletion_strategy.clone(),
            Arc::clone(&self.table_memory),
            self.pk_row_converter.as_ref().map(Arc::clone),
            self.pk_column_indices.clone(),
            Vec::new(), // no protected snapshots for PositionBased
            Arc::clone(self.context.runtime_env()),
            None, // write lock already held above
            Arc::clone(&self.seq_allocator),
        );

        let deleted = sink
            .delete_by_key_hash_probe(&ctx, &all_tables, matched_keys, key_columns)
            .await
            .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
        if deleted > 0 {
            self.clear_cached_pk_keyset();
            self.clear_scan_file_statistics_cache();
        }
        Ok(deleted)
    }

    /// Returns `true` if this table uses the `PositionBased` deletion strategy.
    #[must_use]
    pub fn is_position_based(&self) -> bool {
        self.pk_deletion_strategy.is_position_based()
    }

    /// Build listing tables for all protected snapshots.
    ///
    /// Returns a vec of `(snapshot_id, listing_table)` pairs.
    pub(super) fn build_protected_snapshot_listing_tables(
        &self,
    ) -> datafusion_common::Result<Vec<(String, Arc<ListingTable>)>> {
        let protected_snapshots = self.protected_snapshots.load();

        let mut result = Vec::with_capacity(protected_snapshots.len());
        for snapshot_id in protected_snapshots.keys() {
            let snapshot_url = Self::snapshot_dir_url(
                &self.table_metadata.path,
                &self.table_metadata.table_id,
                snapshot_id,
            );

            let listing_table = Self::create_listing_table(
                &snapshot_url,
                Arc::clone(&self.table_metadata.schema),
                self.context.file_format(),
                &self.pk_deletion_strategy,
            )
            .map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to create listing table for protected snapshot {snapshot_id}: {e}"
                ))
            })?;
            result.push((snapshot_id.clone(), listing_table));
        }
        Ok(result)
    }

    /// Returns `true` if deletes can use whole-file deletion instead of per-row deletion vectors.
    ///
    /// Requirements:
    /// - Time-based retention is configured (`time_retention_filter_builder`).
    /// - The table is **not** backed by S3 storage.
    /// - The filter is a single `retention_col < threshold` expression matching
    ///   the configured retention column. Non-retention deletes (e.g. CDC
    ///   change-batch `DELETE WHERE pk = value`) fall through to the
    ///   deletion-vector path to preserve correct DELETE semantics.
    pub(super) fn file_based_deletes_preferred(&self, filters: &[Expr]) -> bool {
        let Some(ref builder) = self.time_retention_filter_builder else {
            return false;
        };

        if self.table_metadata.path.starts_with("s3://") {
            return false;
        }

        // Only use file-based path when the filter is a retention-pattern delete
        // on the configured retention column: `col < threshold`.
        let is_retention_filter = filters.len() == 1
            && super::retention::extract_retention_column_and_threshold(&filters[0])
                .is_ok_and(|(col, op, _)| col == builder.column_name() && op == Operator::Lt);

        if !is_retention_filter {
            tracing::debug!(
                "Table '{}': delete filter does not match retention pattern (`{} < threshold`)",
                self.table_metadata.table_name,
                builder.column_name(),
            );
        }

        is_retention_filter
    }
}

/// Formats a byte count as a human-readable string (e.g., "1.23 GiB").
pub(super) fn format_bytes(bytes: usize) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;

    #[expect(clippy::cast_precision_loss)]
    let bytes_f64 = bytes as f64;

    if bytes_f64 >= GIB {
        format!("{:.2} GiB", bytes_f64 / GIB)
    } else if bytes_f64 >= MIB {
        format!("{:.2} MiB", bytes_f64 / MIB)
    } else if bytes_f64 >= KIB {
        format!("{:.2} KiB", bytes_f64 / KIB)
    } else {
        format!("{bytes} B")
    }
}

/// Formats bytes per second as a human-readable throughput string.
pub(super) fn format_bytes_per_sec(bytes_per_sec: f64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;

    if bytes_per_sec >= GIB {
        format!("{:.2} GiB/s", bytes_per_sec / GIB)
    } else if bytes_per_sec >= MIB {
        format!("{:.2} MiB/s", bytes_per_sec / MIB)
    } else if bytes_per_sec >= KIB {
        format!("{:.2} KiB/s", bytes_per_sec / KIB)
    } else {
        format!("{bytes_per_sec:.0} B/s")
    }
}
