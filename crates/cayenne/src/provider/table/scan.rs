//! Snapshot scan planning, deletion filtering, and scan-filter rewrites.
//!
//! Builds the file-backed branches of a Cayenne scan:
//! `create_snapshot_scan_plan_with_config` lists a snapshot directory
//! (`list_files_for_snapshot_scan`, with footer-stats pruning and the per-file
//! statistics cache) and produces the Vortex `DataSourceExec`; the deletion
//! filter execs (`apply_deletion_filter*`, `apply_partial_deletion_filter`) are
//! layered on top, with the b3 disjoint-PK-range skip. Callers (`scan()`,
//! compaction, keyset loads) hold `listing_fence.read()`/`write()` themselves —
//! nothing here takes locks. Also: deletion-vector loading at open time and the
//! IN-list→BETWEEN scan-filter rewrites.
//!
//! Mechanically split out of `provider/table.rs`; code is moved verbatim.

use crate::catalog::MetadataCatalog;
use datafusion_catalog::Session;
use futures::{Stream, StreamExt};

use super::{
    Arc, ArcSwap, CachedFileMetadata, CatalogError, CatalogResult, CayenneAccelerationExec,
    CayenneTableProvider, Column, Constraints, DFPrecision, DataFusionResult, DeletionIndex,
    Duration, EmptyExec, ExecutionPlan, Expr, Field, FileFormat, FileGroup, FileScanConfig,
    FileScanConfigBuilder, HashMap, InsertRecordHandling, Int64PkDeletionFilterExec,
    Int64PkDeletionSnapshot, KeyBasedDeletionFilterExec, KeyDeletionIndex, ListingOptions,
    ListingTableUrl, MAX_PK_SELECTIVE_INLIST_VALUES, MAX_PK_SELECTIVE_RANGE_SPAN,
    MAX_VORTEX_KEY_DELETE_PUSHDOWN, MIN_CONSECUTIVE_INLIST_REWRITE_VALUES, ObjectStore,
    ObjectStoreExt, Operator, PartitionedFile, PhysicalExpr, PkDeletionSnapshot,
    PkDeletionStrategy, PkDeletionStrategyWithCache, PositionBitmap, PositionDeletionVector,
    ProjectionExec, ProtectedSnapshotScan, RoaringBitmap, RowConverterDeletionSnapshot,
    ScalarValue, SchemaBuilder, SchemaRef, SessionConfig, SnapshotFileStatistics, Statistics,
    TableSchema, compute_all_files_statistics, create_lex_ordering, expr_applicable_for_cols,
    project_schema, pruned_partition_list, task,
};

pub(super) struct SnapshotScanListingRequest<'a> {
    pub(super) state: &'a dyn Session,
    pub(super) table_url: &'a ListingTableUrl,
    pub(super) options: &'a ListingOptions,
    pub(super) partition_filters: &'a [Expr],
    pub(super) data_filters: &'a [Expr],
    pub(super) snapshot_id: &'a str,
    pub(super) limit: Option<usize>,
    pub(super) scan_schema: SchemaRef,
}

pub(super) struct SnapshotFilesForScan {
    pub(super) file_groups: Vec<FileGroup>,
    pub(super) statistics: Statistics,
    pub(super) grouped_by_partition: bool,
}

impl CayenneTableProvider {
    /// Load both position-based and key-based deletion vectors from the catalog.
    ///
    /// This method queries the catalog for delete files and loads them into memory,
    /// constructing the appropriate `PkDeletionStrategy` variant with embedded caches:
    /// - `PositionBased`: Cache of `HashMap<String, RoaringBitmap>` (file path -> row positions)
    /// - `Int64Pk`: Cache of `HashMap<i64, i64>` (PK -> max delete sequence) + insert records
    /// - `RowConverterBased`: Cache of `HashMap<Box<[u8]>, i64>` (serialized PK bytes -> max delete sequence) + insert records
    ///
    /// # Returns
    ///
    /// The fully constructed `PkDeletionStrategy` with all caches populated.
    pub(super) async fn load_deletion_vectors_all(
        table_id: &str,
        catalog: Arc<dyn MetadataCatalog>,
        strategy: PkDeletionStrategy,
    ) -> CatalogResult<PkDeletionStrategyWithCache> {
        use super::delete::detect_deletion_type_and_read;

        // Query catalog for delete files
        let delete_files = catalog
            .get_table_delete_files(table_id)
            .await
            .map_err(|e| CatalogError::InvalidOperation {
                message: "Failed to load deletion vectors from catalog.".to_string(),
                source: Box::new(e),
            })?;

        // Load insert records from catalog (only for PK-based strategies)
        let insert_records_bytes = if strategy == PkDeletionStrategy::PositionBased {
            HashMap::new()
        } else {
            catalog.get_insert_records(table_id).await.map_err(|e| {
                CatalogError::InvalidOperation {
                    message: "Failed to load insert records from catalog.".to_string(),
                    source: Box::new(e),
                }
            })?
        };

        // Early return for empty case - construct strategy with empty caches
        if delete_files.is_empty() && insert_records_bytes.is_empty() {
            return Ok(PkDeletionStrategyWithCache::empty_for(strategy));
        }

        // Parse insert records based on strategy
        let (insert_records_pk_i64, insert_records_row_keys) = match strategy {
            PkDeletionStrategy::PositionBased => (HashMap::new(), HashMap::new()),
            PkDeletionStrategy::Int64Pk => {
                // Convert insert record bytes to i64
                let int64_pks: HashMap<i64, i64> = insert_records_bytes
                    .iter()
                    .filter_map(|(bytes, &seq)| {
                        if bytes.len() >= 8 {
                            let mut arr = [0_u8; 8];
                            arr.copy_from_slice(&bytes[..8]);
                            Some((i64::from_be_bytes(arr), seq))
                        } else {
                            tracing::warn!(
                                "Skipping invalid Int64 insert record key with length {} (expected at least 8 bytes)",
                                bytes.len()
                            );
                            None
                        }
                    })
                    .collect();
                (int64_pks, HashMap::new())
            }
            PkDeletionStrategy::RowConverterBased => {
                // Use the byte keys directly
                (HashMap::new(), insert_records_bytes)
            }
        };

        // Early return if only insert records exist (no delete files)
        if delete_files.is_empty() {
            return Ok(match strategy {
                PkDeletionStrategy::PositionBased => {
                    PkDeletionStrategyWithCache::empty_position_based()
                }
                PkDeletionStrategy::Int64Pk => PkDeletionStrategyWithCache::Int64Pk {
                    deletion_snapshot: Arc::new(ArcSwap::from_pointee(
                        Int64PkDeletionSnapshot::from_index(DeletionIndex::from_maps(
                            HashMap::new(),
                            insert_records_pk_i64,
                        )),
                    )),
                    // No delete files => no position deletes either.
                    position_deletions: Arc::new(ArcSwap::from_pointee(PositionBitmap::new())),
                },
                PkDeletionStrategy::RowConverterBased => {
                    PkDeletionStrategyWithCache::RowConverterBased {
                        deletion_snapshot: Arc::new(ArcSwap::from_pointee(
                            RowConverterDeletionSnapshot::from_index(KeyDeletionIndex::from_maps(
                                HashMap::new(),
                                insert_records_row_keys,
                            )),
                        )),
                        position_deletions: Arc::new(ArcSwap::from_pointee(PositionBitmap::new())),
                    }
                }
            });
        }

        // Read deletion vector files in a blocking task, detecting type from schema
        // Returns (HashMap<String, RoaringBitmap>, HashMap<Box<[u8]>, i64>) where:
        // - per_file_row_ids: file path -> bitmap of deleted row positions
        // - deleted_row_keys: PK bytes -> max delete sequence
        let (per_file_row_ids, deleted_row_keys) =
            task::spawn_blocking(move || detect_deletion_type_and_read(delete_files))
                .await
                .map_err(|err| CatalogError::InvalidOperation {
                    message: "Deletion vector reader task panicked or was cancelled.".to_string(),
                    source: Box::new(err),
                })
                .and_then(|result| {
                    result.map_err(|err| CatalogError::InvalidOperation {
                        message: "Failed to read deletion vectors.".to_string(),
                        source: Box::new(err),
                    })
                })?;

        // Construct the appropriate cache variant with populated caches
        let cache = match strategy {
            PkDeletionStrategy::PositionBased => {
                let total_deletions: u64 = per_file_row_ids.values().map(RoaringBitmap::len).sum();
                tracing::debug!(
                    "Cached deletion vectors for table_id {table_id}: {} position-based deletions across {} files",
                    total_deletions,
                    per_file_row_ids.len(),
                );
                // Wrap each per-file deletion vector in an Arc so future snapshot
                // publishes only clone the small outer map entries, not every
                // file's full bitmap/access-plan data. See `PositionBitmap`'s
                // docstring for the perf rationale.
                let cached_map = per_file_row_ids
                    .into_iter()
                    .map(|(path, bitmap)| (path, Arc::new(PositionDeletionVector::new(bitmap))))
                    .collect();
                PkDeletionStrategyWithCache::PositionBased {
                    cached_deleted_row_ids: Arc::new(ArcSwap::from_pointee(cached_map)),
                }
            }
            PkDeletionStrategy::Int64Pk => {
                // Int64 PK - convert row_keys (which contain Int64 bytes) to i64
                // TODO: Optimize to store Int64 PK values directly in deletion files
                let int64_pks: HashMap<i64, i64> = deleted_row_keys
                    .iter()
                    .filter_map(|(bytes, &seq)| {
                        if bytes.len() >= 8 {
                            // RowConverter uses big-endian for i64 with sign bit flipped
                            let mut arr = [0_u8; 8];
                            arr.copy_from_slice(&bytes[..8]);
                            Some((i64::from_be_bytes(arr), seq))
                        } else {
                            tracing::warn!(
                                "Skipping invalid Int64 deletion key with length {} (expected at least 8 bytes)",
                                bytes.len()
                            );
                            None
                        }
                    })
                    .collect();
                tracing::debug!(
                    "Cached deletion vectors for table_id {table_id}: {} int64-pk, {} int64-insert",
                    int64_pks.len(),
                    insert_records_pk_i64.len(),
                );
                PkDeletionStrategyWithCache::Int64Pk {
                    deletion_snapshot: Arc::new(ArcSwap::from_pointee(
                        Int64PkDeletionSnapshot::from_index(DeletionIndex::from_maps(
                            int64_pks,
                            insert_records_pk_i64,
                        )),
                    )),
                    // Position-delete files (written under `deletion_mode: position`
                    // for located rows) load here; empty for key-mode tables.
                    position_deletions: Arc::new(ArcSwap::from_pointee(
                        per_file_row_ids
                            .into_iter()
                            .map(|(path, bitmap)| {
                                (path, Arc::new(PositionDeletionVector::new(bitmap)))
                            })
                            .collect::<PositionBitmap>(),
                    )),
                }
            }
            PkDeletionStrategy::RowConverterBased => {
                tracing::debug!(
                    "Cached deletion vectors for table_id {table_id}: {} key-based, {} key-insert",
                    deleted_row_keys.len(),
                    insert_records_row_keys.len(),
                );
                PkDeletionStrategyWithCache::RowConverterBased {
                    deletion_snapshot: Arc::new(ArcSwap::from_pointee(
                        RowConverterDeletionSnapshot::from_index(KeyDeletionIndex::from_maps(
                            deleted_row_keys,
                            insert_records_row_keys,
                        )),
                    )),
                    // Position-delete files (written under `deletion_mode: position`
                    // for located rows) load here; empty for key-mode tables.
                    position_deletions: Arc::new(ArcSwap::from_pointee(
                        per_file_row_ids
                            .into_iter()
                            .map(|(path, bitmap)| {
                                (path, Arc::new(PositionDeletionVector::new(bitmap)))
                            })
                            .collect::<PositionBitmap>(),
                    )),
                }
            }
        };

        Ok(cache)
    }

    /// Load protected snapshots from the catalog.
    ///
    /// Protected snapshots use their persisted per-snapshot sequence number as
    /// the deletion threshold. Scans apply only deletion vectors with
    /// `delete_seq > threshold` for each protected snapshot.
    pub(super) async fn load_protected_snapshots(
        catalog: Arc<dyn MetadataCatalog>,
        table_id: &str,
        strategy: &PkDeletionStrategyWithCache,
    ) -> CatalogResult<HashMap<String, i64>> {
        // Only PK-based strategies support sequence-ordered snapshot protection.
        // Position-based deletion vectors are per-file and don't need protected snapshots.
        if strategy.is_position_based() {
            return Ok(HashMap::new());
        }

        let snapshot_sequences = catalog.get_all_snapshot_sequences(table_id).await?;

        if snapshot_sequences.is_empty() {
            return Ok(HashMap::new());
        }

        // Treat ALL snapshots as protected, using each snapshot's own persisted
        // `sequence_number` as its deletion threshold.
        //
        // Each snapshot's `sequence_number` was allocated (via `increment_sequence_number`)
        // BEFORE the same round's deletions were created. Therefore:
        // - All deletions from PRIOR rounds have `delete_seq < sequence_number`
        // - All deletions from the SAME or LATER rounds have `delete_seq > sequence_number`
        //
        // The partial deletion filter uses `delete_seq > threshold`, so setting the
        // threshold to `sequence_number` correctly:
        // - Skips deletions from prior rounds (already accounted for at snapshot creation)
        // - Applies deletions from the same or later rounds
        //
        // Previously, this function computed a single global `max_delete_seq` from ALL
        // deletions and filtered out snapshots where `seq <= max_delete_seq`. This was
        // incorrect because later rounds' deletions raised the global max, causing earlier
        // snapshots to be incorrectly dropped and their data lost on restart.

        tracing::debug!(
            "Loaded {} protected snapshot(s) for table_id {table_id}",
            snapshot_sequences.len(),
        );

        Ok(snapshot_sequences)
    }

    /// Extend the projection to include columns referenced by `filter` that aren't
    /// already present. Returns the (possibly extended) projection and whether any
    /// columns were added (meaning a projection strip is needed later).
    pub(super) fn extend_projection_for_retention_filter(
        &self,
        projection: Option<Vec<usize>>,
        filter: &Expr,
        already_extended: bool,
    ) -> (Option<Vec<usize>>, bool) {
        let Some(mut proj) = projection else {
            return (None, already_extended);
        };
        let mut added = already_extended;
        for col_ref in filter.column_refs() {
            if let Some((idx, _)) = self.table_metadata.schema.column_with_name(col_ref.name())
                && !proj.contains(&idx)
            {
                proj.push(idx);
                added = true;
            }
        }
        (Some(proj), added)
    }

    /// When filtering by PK, we may have added PK columns to the scan that weren't in the
    /// original projection. This creates a `ProjectionExec` that only outputs the originally
    /// requested columns.
    #[expect(clippy::unused_self)]
    pub(super) fn create_projection_strip(
        &self,
        input: Arc<dyn ExecutionPlan>,
        num_columns_to_keep: usize,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let input_schema = input.schema();
        let mut projection_expr: Vec<(Arc<dyn PhysicalExpr>, String)> =
            Vec::with_capacity(num_columns_to_keep);

        for idx in 0..num_columns_to_keep {
            let field = input_schema.field(idx);
            let col_name = field.name().clone();
            projection_expr.push((
                Arc::new(Column::new(&col_name, idx)) as Arc<dyn PhysicalExpr>,
                col_name,
            ));
        }

        let projection = ProjectionExec::try_new(projection_expr, input)?;
        Ok(Arc::new(CayenneAccelerationExec::new(Arc::new(projection))))
    }

    /// Scan protected snapshots with partial deletion filtering.
    ///
    /// Protected snapshots skip deletions that existed when they were created
    /// (deletions with seq <= `max_delete_seq_at_creation`), but newer deletions
    /// (seq > `max_delete_seq_at_creation`) are still applied.
    pub(super) async fn scan_protected_snapshots(
        &self,
        scan: ProtectedSnapshotScan<'_>,
    ) -> datafusion_common::Result<Vec<Arc<dyn ExecutionPlan>>> {
        // Warn only on genuine amplification (compaction losing ground), not the
        // normal steady-state equilibrium. Under sustained CDC the protected-
        // snapshot count naturally sits at/above the compaction TRIGGER — fresh
        // deltas arrive during each merge pass — so a threshold equal to the
        // trigger fired on nearly every scan (pure noise). Derive the threshold
        // purely from the CONFIGURED trigger (no hardcoded absolute, so it
        // respects a custom `compaction_trigger_protected_snapshots`): warn once
        // the count reaches a multiple of the trigger, i.e. compaction has fallen
        // well behind. The per-scan DEBUG below already covers the informational
        // "scan includes protected snapshots" case.
        const PROTECTED_SNAPSHOT_WARN_TRIGGER_MULTIPLE: usize = 8;

        // `protected_snapshots` is captured by the caller together with the deletion snapshot
        if scan.protected_snapshots.is_empty() {
            return Ok(Vec::new());
        }

        tracing::trace!(
            table = %self.table_metadata.table_name,
            protected_snapshot_count = scan.protected_snapshots.len(),
            "Scanning protected snapshots for Cayenne table"
        );
        tracing::debug!(
            table = %self.table_metadata.table_name,
            protected_snapshot_count = scan.protected_snapshots.len(),
            "Cayenne scan includes protected snapshots"
        );
        let protected_snapshot_trigger = self.context.compaction_trigger_protected_snapshots();
        let protected_snapshot_warn_threshold =
            protected_snapshot_trigger.saturating_mul(PROTECTED_SNAPSHOT_WARN_TRIGGER_MULTIPLE);
        if scan.protected_snapshots.len() >= protected_snapshot_warn_threshold {
            tracing::warn!(
                table = %self.table_metadata.table_name,
                protected_snapshot_count = scan.protected_snapshots.len(),
                protected_snapshot_warn_threshold,
                "Cayenne scan has high protected snapshot amplification"
            );
        }

        let mut plans = Vec::with_capacity(scan.protected_snapshots.len());

        for (snapshot_id, max_delete_seq_at_creation) in scan.protected_snapshots.iter() {
            let plan = self
                .create_snapshot_scan_plan(
                    scan.state,
                    snapshot_id,
                    scan.projection,
                    scan.filters,
                    scan.limit,
                )
                .await?;

            // Apply partial deletion filter - only deletions with seq > max_delete_seq_at_creation
            let filtered_plan = self.apply_partial_deletion_filter(
                plan,
                scan.pk_indices_in_projection,
                *max_delete_seq_at_creation,
                scan.deletion_snapshot,
            )?;

            plans.push(filtered_plan);
        }

        Ok(plans)
    }

    pub(super) fn snapshot_scan_schema(
        file_schema: &SchemaRef,
        options: &ListingOptions,
    ) -> SchemaRef {
        // `SchemaBuilder::from(&Schema)` clones the metadata HashMap, but we then
        // overwrite that metadata via `.with_metadata(...)` below. Building from
        // `Fields` skips the wasted first clone.
        let mut builder = SchemaBuilder::from(file_schema.fields());
        for (name, data_type) in &options.table_partition_cols {
            builder.push(Field::new(name, data_type.clone(), false));
        }
        Arc::new(
            builder
                .finish()
                .with_metadata(file_schema.metadata().clone()),
        )
    }

    pub(super) fn snapshot_file_table_schema(
        file_schema: &SchemaRef,
        options: &ListingOptions,
    ) -> TableSchema {
        TableSchema::new(
            Arc::clone(file_schema),
            options
                .table_partition_cols
                .iter()
                .map(|(name, data_type)| Arc::new(Field::new(name, data_type.clone(), false)))
                .collect(),
        )
    }

    pub(super) async fn create_snapshot_scan_plan(
        &self,
        state: &dyn Session,
        snapshot_id: &str,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        self.create_snapshot_scan_plan_with_config(
            state,
            snapshot_id,
            projection,
            filters,
            limit,
            state.config(),
        )
        .await
    }

    pub(super) async fn create_snapshot_scan_plan_with_config(
        &self,
        state: &dyn Session,
        snapshot_id: &str,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        scan_config: &SessionConfig,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let snapshot_dir_url = Self::snapshot_dir_url(
            &self.table_metadata.path,
            &self.table_metadata.table_id,
            snapshot_id,
        );
        let table_url = ListingTableUrl::parse(&snapshot_dir_url)?;
        let options = Self::create_listing_options(
            self.context.file_format(),
            &self.pk_deletion_strategy,
            scan_config,
        );
        let scan_schema = Self::snapshot_scan_schema(&self.table_metadata.schema, &options);

        let partition_column_names = options
            .table_partition_cols
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>();
        let (partition_filters, data_filters): (Vec<_>, Vec<_>) =
            filters.iter().cloned().partition(|filter| {
                !partition_column_names.is_empty()
                    && expr_applicable_for_cols(&partition_column_names, filter)
            });
        let statistic_file_limit = if data_filters.is_empty() { limit } else { None };

        let SnapshotFilesForScan {
            file_groups: mut partitioned_file_lists,
            statistics,
            grouped_by_partition,
        } = self
            .list_files_for_snapshot_scan(&SnapshotScanListingRequest {
                state,
                table_url: &table_url,
                options: &options,
                partition_filters: &partition_filters,
                data_filters: &data_filters,
                snapshot_id,
                limit: statistic_file_limit,
                scan_schema: Arc::clone(&scan_schema),
            })
            .await?;

        if partitioned_file_lists.is_empty() {
            let projected_schema = project_schema(&scan_schema, projection)?;
            return Ok(Arc::new(EmptyExec::new(projected_schema)));
        }

        let output_ordering = create_lex_ordering(
            &scan_schema,
            &options.file_sort_order,
            state.execution_props(),
        )?;
        if state
            .config_options()
            .execution
            .split_file_groups_by_statistics
            && let Some(first_output_ordering) = output_ordering.first()
        {
            match FileScanConfig::split_groups_by_statistics_with_target_partitions(
                &scan_schema,
                &partitioned_file_lists,
                first_output_ordering,
                options.target_partitions,
            ) {
                Ok(new_groups) if new_groups.len() <= options.target_partitions => {
                    partitioned_file_lists = new_groups;
                }
                Ok(_) => {
                    tracing::debug!(
                        table = %self.table_metadata.table_name,
                        "Attempted to split file groups by statistics, but there were more file groups than target_partitions; falling back to unordered"
                    );
                }
                Err(e) => {
                    tracing::debug!(
                        table = %self.table_metadata.table_name,
                        "Failed to split file groups by statistics: {e}"
                    );
                }
            }
        }

        let file_source = options.format.file_source(Self::snapshot_file_table_schema(
            &self.table_metadata.schema,
            &options,
        ));

        options
            .format
            .create_physical_plan(
                state,
                FileScanConfigBuilder::new(table_url.object_store(), file_source)
                    .with_file_groups(partitioned_file_lists)
                    .with_constraints(Constraints::default())
                    .with_statistics(statistics)
                    .with_projection_indices(projection.cloned())?
                    .with_limit(limit)
                    .with_output_ordering(output_ordering)
                    .with_partitioned_by_file_group(grouped_by_partition)
                    .build(),
            )
            .await
    }

    pub(super) async fn list_files_for_snapshot_scan(
        &self,
        request: &SnapshotScanListingRequest<'_>,
    ) -> datafusion_common::Result<SnapshotFilesForScan> {
        let collect_stats = request.options.collect_stat
            && !(self.pk_deletion_strategy.is_position_based() && self.has_pending_deletions());
        let store = request
            .state
            .runtime_env()
            .object_store(request.table_url)?;
        let meta_fetch_concurrency = request
            .state
            .config_options()
            .execution
            .meta_fetch_concurrency;
        let file_list = pruned_partition_list(
            request.state,
            store.as_ref(),
            request.table_url,
            request.partition_filters,
            &request.options.file_extension,
            &request.options.table_partition_cols,
        )
        .await?;

        let listing_pruning_predicate = if collect_stats && !request.data_filters.is_empty() {
            super::file_pruning::build_listing_pruning_predicate(
                &request.scan_schema,
                request.data_filters,
            )?
        } else {
            None
        };

        let table_name = self.table_metadata.table_name.clone();
        let files = file_list
            .map(|part_file| async {
                let part_file = part_file?;
                let statistics = if collect_stats {
                    self.collect_scan_file_statistics(
                        request.state,
                        request.snapshot_id,
                        &store,
                        request.options.format.as_ref(),
                        &part_file,
                    )
                    .await?
                } else {
                    Arc::new(Statistics::new_unknown(&self.table_metadata.schema))
                };
                let part_file = part_file.with_statistics(statistics);
                if let Some(ref predicate) = listing_pruning_predicate
                    && super::file_pruning::should_prune_partitioned_file(
                        &part_file,
                        &request.scan_schema,
                        predicate,
                    )?
                {
                    tracing::debug!(
                        table = %table_name,
                        file = %part_file.object_meta.location,
                        "Pruned Vortex file at listing time via footer statistics"
                    );
                    telemetry::track_cayenne_scan_files(
                        1,
                        1,
                        &[telemetry::KeyValue::new("table", table_name.clone())],
                    );
                    return Ok(None);
                }
                telemetry::track_cayenne_scan_files(
                    1,
                    0,
                    &[telemetry::KeyValue::new("table", table_name.clone())],
                );
                Ok(Some(part_file))
            })
            .buffer_unordered(meta_fetch_concurrency)
            .filter_map(|result| async move {
                match result {
                    Ok(Some(file)) => Some(Ok(file)),
                    Ok(None) => None,
                    Err(err) => Some(Err(err)),
                }
            });

        let (file_group, inexact_stats) =
            Self::collect_scan_files_with_limit(files, request.limit, collect_stats).await?;

        let threshold = request
            .state
            .config_options()
            .optimizer
            .preserve_file_partitions;
        let (file_groups, grouped_by_partition) = if threshold > 0
            && !request.options.table_partition_cols.is_empty()
        {
            let grouped = file_group.group_by_partition_values(request.options.target_partitions);
            if grouped.len() >= threshold {
                (grouped, true)
            } else {
                let all_files = grouped
                    .into_iter()
                    .flat_map(FileGroup::into_inner)
                    .collect::<Vec<_>>();
                (
                    FileGroup::new(all_files).split_files(request.options.target_partitions),
                    false,
                )
            }
        } else {
            (
                file_group.split_files(request.options.target_partitions),
                false,
            )
        };

        let (file_groups, statistics) = compute_all_files_statistics(
            file_groups,
            Arc::clone(&request.scan_schema),
            collect_stats,
            inexact_stats,
        )?;

        Ok(SnapshotFilesForScan {
            file_groups,
            statistics,
            grouped_by_partition,
        })
    }

    pub(super) async fn collect_scan_file_statistics(
        &self,
        state: &dyn Session,
        snapshot_id: &str,
        store: &Arc<dyn ObjectStore>,
        format: &dyn FileFormat,
        part_file: &PartitionedFile,
    ) -> datafusion_common::Result<Arc<Statistics>> {
        if let Some(cached) = self
            .scan_file_statistics
            .get(&part_file.object_meta.location)
            && cached.is_valid_for(&part_file.object_meta)
        {
            return Ok(cached.statistics);
        }

        let file_path = part_file.object_meta.location.to_string();
        let file_size_bytes = i64::try_from(part_file.object_meta.size).unwrap_or(i64::MAX);

        if let Ok(Some(persisted)) = self
            .catalog
            .get_snapshot_file_statistics(&self.table_metadata.table_id, snapshot_id, &file_path)
            .await
            && persisted.file_size_bytes == file_size_bytes
            && let Some(statistics) = crate::stats::statistics_from_persisted_blob(
                &persisted.statistics_blob,
                &self.table_metadata.schema,
                persisted.num_rows,
            )
        {
            self.scan_file_statistics.put(
                &part_file.object_meta.location,
                CachedFileMetadata::new(
                    part_file.object_meta.clone(),
                    Arc::clone(&statistics),
                    None,
                ),
            );
            return Ok(statistics);
        }

        let statistics = Arc::new(
            format
                .infer_stats(
                    state,
                    store,
                    Arc::clone(&self.table_metadata.schema),
                    &part_file.object_meta,
                )
                .await?,
        );

        if let Some(blob) = crate::stats::statistics_to_persisted_blob(
            statistics.as_ref(),
            &self.table_metadata.schema,
        ) {
            let num_rows = match statistics.num_rows {
                DFPrecision::Exact(rows) | DFPrecision::Inexact(rows) => {
                    i64::try_from(rows).unwrap_or(0)
                }
                DFPrecision::Absent => 0,
            };
            if let Err(error) = self
                .catalog
                .upsert_snapshot_file_statistics(&SnapshotFileStatistics {
                    table_id: self.table_metadata.table_id.clone(),
                    snapshot_id: snapshot_id.to_string(),
                    file_path,
                    file_size_bytes,
                    num_rows,
                    statistics_blob: blob,
                })
                .await
            {
                tracing::debug!(
                    table = %self.table_metadata.table_name,
                    error = %error,
                    "Failed to persist per-file snapshot statistics; continuing with footer stats"
                );
            }
        }

        self.scan_file_statistics.put(
            &part_file.object_meta.location,
            CachedFileMetadata::new(part_file.object_meta.clone(), Arc::clone(&statistics), None),
        );

        Ok(statistics)
    }

    pub(super) async fn collect_scan_files_with_limit(
        files: impl Stream<Item = DataFusionResult<PartitionedFile>>,
        limit: Option<usize>,
        collect_stats: bool,
    ) -> DataFusionResult<(FileGroup, bool)> {
        let mut file_group = FileGroup::default();
        let mut all_files = Box::pin(files.fuse());
        let mut reached_limit = false;
        let mut num_rows = DFPrecision::Absent;

        while let Some(file_result) = all_files.next().await {
            if reached_limit {
                break;
            }

            let file = file_result?;
            if collect_stats && let Some(file_stats) = &file.statistics {
                num_rows = if file_group.is_empty() {
                    file_stats.num_rows
                } else {
                    num_rows.add(&file_stats.num_rows)
                };
            }

            file_group.push(file);

            if let Some(limit) = limit
                && let DFPrecision::Exact(row_count) = num_rows
                && row_count > limit
            {
                reached_limit = true;
            }
        }

        let inexact_stats = if reached_limit {
            match all_files.next().await {
                Some(Ok(_)) => true,
                Some(Err(err)) => return Err(err),
                None => false,
            }
        } else {
            false
        };

        Ok((file_group, inexact_stats))
    }

    pub(super) fn record_listing_fence_wait_duration(&self, duration: Duration) {
        telemetry::track_cayenne_listing_fence_wait_duration(
            duration,
            &[telemetry::KeyValue::new(
                "dataset",
                self.table_metadata.table_name.clone(),
            )],
        );
    }

    pub(super) fn record_listing_scan_duration(&self, duration: Duration) {
        telemetry::track_cayenne_listing_scan_duration(
            duration,
            &[telemetry::KeyValue::new(
                "dataset",
                self.table_metadata.table_name.clone(),
            )],
        );
    }

    /// Closed `[min,max]` of the projected single Int64 PK column over this
    /// branch's scan, if known **exactly**. Returns `None` on any uncertainty
    /// — Inexact/Absent bounds (mutable overlays, no footer stats), a
    /// multi-column PK, or a non-Int64 PK — in which case the caller must NOT
    /// skip the deletion filter. This is read off the already-built plan at
    /// plan time (the file-scan config attaches per-column `Statistics` via
    /// `FileScanConfigBuilder::with_statistics`), so it costs no extra IO.
    ///
    /// Used by [`b3 sub-lever 1`](Self::apply_deletion_filter) to prove a
    /// branch's scanned PK window is disjoint from every deletion and shed the
    /// filter for that branch. The returned bound is a sound *superset* of the
    /// branch's actual PK values (the file `[min,max]` bounds them, and the
    /// post-delete true range is a subset), so a disjoint verdict against the
    /// deleted-key range is conservative. Composite / hash-keyed indexes are
    /// handled by the per-batch bloom sweep instead (the multi-PK guard here
    /// returns `None`, so this never fires for them).
    pub(super) fn branch_int64_pk_range(
        plan: &Arc<dyn ExecutionPlan>,
        pk_indices_in_projection: &[usize],
    ) -> Option<(i64, i64)> {
        // Single Int64 PK only; composite PKs return None (handled per-batch).
        let [pk_idx] = pk_indices_in_projection else {
            return None;
        };
        // DF53 accessor for whole-plan (all-partition) statistics.
        let stats = plan.partition_statistics(None).ok()?;
        let col = stats.column_statistics.get(*pk_idx)?;
        let (DFPrecision::Exact(lo), DFPrecision::Exact(hi)) = (&col.min_value, &col.max_value)
        else {
            return None;
        };
        match (lo, hi) {
            (ScalarValue::Int64(Some(lo)), ScalarValue::Int64(Some(hi))) => Some((*lo, *hi)),
            _ => None,
        }
    }

    /// `true` iff the branch's Exact Int64 PK scan window is provably disjoint
    /// from `tombstones`' deleted-key range — i.e. no scanned PK can equal any
    /// deleted PK, so the deletion filter would remove zero rows and can be
    /// skipped (b3 sub-lever 1). `false` on any uncertainty (Inexact/Absent
    /// stats, composite PK, zero deletions, or overlapping ranges) → keep the
    /// filter. Conservative by construction; see `branch_int64_pk_range` and
    /// `DeletionIndex::deleted_key_range`.
    pub(super) fn int64_branch_disjoint_from_deletions(
        plan: &Arc<dyn ExecutionPlan>,
        pk_indices_in_projection: &[usize],
        tombstones: &DeletionIndex,
    ) -> bool {
        if let (Some((del_lo, del_hi)), Some((scan_lo, scan_hi))) = (
            tombstones.deleted_key_range(),
            Self::branch_int64_pk_range(plan, pk_indices_in_projection),
        ) {
            // Closed-interval disjoint test.
            scan_hi < del_lo || scan_lo > del_hi
        } else {
            false
        }
    }

    /// Apply partial deletion filter - only deletions with seq > threshold are applied.
    ///
    /// This is used for protected snapshots which should skip deletions that existed
    /// when they were created, but still honor newer deletions.
    pub(super) fn apply_partial_deletion_filter(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        pk_indices_in_projection: &[usize],
        min_delete_seq_to_apply: i64,
        deletion_snapshot: &PkDeletionSnapshot,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        // Previously this rebuilt a per-snapshot `DeletionIndex` via
        // `HashMap::collect` + bloom-filter rebuild — O(N · M) per scan
        // where N = #protected_snapshots and M = total deletion entries.
        // The fix shares the existing index across snapshots and pushes
        // the per-snapshot `min_seq` filter into the probe loop, which
        // already pays a bloom prefilter; one integer comparison per
        // confirmed match is amortized to ~zero per row. See
        // `crates/cayenne/benches/apply_partial_deletion_filter_per_scan.rs`
        // for the before-numbers.
        match deletion_snapshot {
            PkDeletionSnapshot::Int64Pk { tombstones } => {
                if tombstones
                    .max_sequence_number()
                    .is_none_or(|max_sequence| max_sequence <= min_delete_seq_to_apply)
                {
                    return Ok(Arc::new(CayenneAccelerationExec::new(plan)));
                }

                // [b3 sub-lever 1] Orthogonal to the sequence cutoff above: if
                // this branch's Exact Int64 PK scan window is disjoint from the
                // deleted-key range, no scanned PK is deletable, so the filter
                // would remove zero rows. The range is a superset of the
                // applicable-by-sequence keys, so a disjoint verdict is sound
                // even under the protected `min_delete_seq_to_apply` cutoff.
                if Self::int64_branch_disjoint_from_deletions(
                    &plan,
                    pk_indices_in_projection,
                    tombstones,
                ) {
                    return Ok(Arc::new(CayenneAccelerationExec::new(plan)));
                }

                let pk_column_index =
                    pk_indices_in_projection.first().copied().ok_or_else(|| {
                        datafusion_common::DataFusionError::Internal(
                            "Int64 PK strategy requires exactly one PK column index".to_string(),
                        )
                    })?;

                Ok(Arc::new(Int64PkDeletionFilterExec::new(
                    plan,
                    Arc::clone(tombstones),
                    InsertRecordHandling::Ignore,
                    pk_column_index,
                    Some(min_delete_seq_to_apply),
                )))
            }
            PkDeletionSnapshot::RowConverterBased { tombstones } => {
                if let Some(ref row_converter) = self.pk_row_converter {
                    if tombstones
                        .max_sequence_number()
                        .is_none_or(|max_sequence| max_sequence <= min_delete_seq_to_apply)
                    {
                        return Ok(Arc::new(CayenneAccelerationExec::new(plan)));
                    }

                    Ok(Arc::new(KeyBasedDeletionFilterExec::new(
                        plan,
                        Arc::clone(tombstones),
                        InsertRecordHandling::Ignore,
                        pk_indices_in_projection.to_vec(),
                        Arc::clone(row_converter),
                        Some(min_delete_seq_to_apply),
                    )))
                } else {
                    Ok(Arc::new(CayenneAccelerationExec::new(plan)))
                }
            }
            PkDeletionSnapshot::PositionBased => {
                // Position-based doesn't use protected snapshots
                Ok(Arc::new(CayenneAccelerationExec::new(plan)))
            }
        }
    }

    /// Apply deletion filter to a plan based on the current deletion strategy.
    pub(super) fn apply_deletion_filter(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        pk_indices_in_projection: &[usize],
        deletion_snapshot: &PkDeletionSnapshot,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        match deletion_snapshot {
            PkDeletionSnapshot::Int64Pk { tombstones } => {
                // Protected snapshots already handle new data without filtering,
                // so insert records are ignored here.
                if tombstones.has_deletions()
                    // [b3 sub-lever 1] Skip the filter when this branch's Exact
                    // Int64 PK window is disjoint from the deleted-key range
                    // (zero rows would be removed); fall through to `Ok(plan)`.
                    && !Self::int64_branch_disjoint_from_deletions(
                        &plan,
                        pk_indices_in_projection,
                        tombstones,
                    )
                {
                    let pk_column_index =
                        pk_indices_in_projection.first().copied().ok_or_else(|| {
                            datafusion_common::DataFusionError::Internal(
                                "Int64 PK strategy requires exactly one PK column index"
                                    .to_string(),
                            )
                        })?;

                    return Ok(Arc::new(Int64PkDeletionFilterExec::new(
                        plan,
                        Arc::clone(tombstones),
                        InsertRecordHandling::Ignore,
                        pk_column_index,
                        None,
                    )));
                }
            }
            PkDeletionSnapshot::RowConverterBased { tombstones } => {
                if let Some(ref row_converter) = self.pk_row_converter
                    && tombstones.has_deletions()
                {
                    return Ok(Arc::new(KeyBasedDeletionFilterExec::new(
                        plan,
                        Arc::clone(tombstones),
                        InsertRecordHandling::Ignore,
                        pk_indices_in_projection.to_vec(),
                        Arc::clone(row_converter),
                        None,
                    )));
                }
            }
            PkDeletionSnapshot::PositionBased => {
                // Position-based deletions are handled at the Vortex scan level; no manual filtering is needed
            }
        }

        // No deletions to apply (position-based deletions are handled at Vortex scan level).
        Ok(plan)
    }

    /// Apply deletion filter including insert records (for main scan path, not protected snapshots).
    /// Unlike `apply_deletion_filter` which uses empty insert records, this passes the full
    /// cached insert records needed for the main plan.
    pub(super) fn apply_deletion_filter_with_insert_records(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        pk_indices_in_projection: &[usize],
        deletion_snapshot: &PkDeletionSnapshot,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        match deletion_snapshot {
            PkDeletionSnapshot::Int64Pk { tombstones } => {
                // [b3 sub-lever 1] Skip the filter when this branch's Exact
                // Int64 PK window is disjoint from the deleted-key range. The
                // gate is keyed on the deleted-key range, which already includes
                // every re-inserted PK (a re-insert carries a delete_sequence),
                // so an upserted PK in range keeps the filter and its visibility
                // is resolved by the unchanged `tombstone_visible` probe; the
                // skip only decides whether the probe runs, never how it decides.
                if tombstones.has_deletions()
                    && !Self::int64_branch_disjoint_from_deletions(
                        &plan,
                        pk_indices_in_projection,
                        tombstones,
                    )
                {
                    tracing::debug!(
                        "Applying Int64 PK deletion filter ({} deleted keys, {} insert records) to scan of table {}",
                        tombstones.delete_len(),
                        tombstones.insert_len(),
                        self.table_metadata.table_name
                    );

                    let pk_column_index =
                        pk_indices_in_projection.first().copied().ok_or_else(|| {
                            datafusion_common::DataFusionError::Internal(
                                "Int64 PK strategy requires exactly one PK column index"
                                    .to_string(),
                            )
                        })?;

                    return Ok(Arc::new(Int64PkDeletionFilterExec::new(
                        plan,
                        Arc::clone(tombstones),
                        InsertRecordHandling::Apply,
                        pk_column_index,
                        None,
                    )));
                }
            }
            PkDeletionSnapshot::RowConverterBased { tombstones } => {
                if let Some(ref row_converter) = self.pk_row_converter
                    && tombstones.has_deletions()
                {
                    tracing::debug!(
                        "Applying RowConverter-based deletion filter ({} deleted keys, {} insert records) to scan of table {}",
                        tombstones.delete_len(),
                        tombstones.insert_len(),
                        self.table_metadata.table_name
                    );

                    return Ok(Arc::new(KeyBasedDeletionFilterExec::new(
                        plan,
                        Arc::clone(tombstones),
                        InsertRecordHandling::Apply,
                        pk_indices_in_projection.to_vec(),
                        Arc::clone(row_converter),
                        None,
                    )));
                }
            }
            PkDeletionSnapshot::PositionBased => {
                // Position-based deletions are handled at the Vortex scan level
            }
        }

        Ok(plan)
    }

    /// Returns `true` iff `filters` contain a `pk_column = literal` equality on
    /// every primary-key column. For such point lookups, `ListingTable`'s
    /// default byte-range fan-out (`target_partitions = num_cpus`) pays per
    /// file-group footer-open cost the lookup never needs. Caller uses this to
    /// build the scan-side `ListingTable` with `target_partitions = 1`. See
    /// `pk_lookup_file_group_fanout` bench (1.6 ms → 898 µs at 1 M rows).
    pub(super) fn is_pk_point_lookup(&self, filters: &[Expr]) -> bool {
        if self.pk_column_indices.is_empty() {
            return false;
        }
        let pk_names: Vec<&str> = self
            .pk_column_indices
            .iter()
            .map(|&idx| self.table_metadata.schema.field(idx).name().as_str())
            .collect();

        pk_names.iter().all(|pk_name| {
            filters
                .iter()
                .any(|expr| pk_column_equals_literal(expr, pk_name))
        })
    }

    /// Returns `true` when scan filters are selective enough that byte-range
    /// file-group fan-out is wasted work: point equality on every PK column,
    /// or (single-column Int64 PK only) a small `IN` list or tight `BETWEEN`.
    pub(super) fn is_pk_selective_scan(&self, filters: &[Expr]) -> bool {
        if self.is_pk_point_lookup(filters) {
            return true;
        }
        if self.pk_column_indices.len() != 1 {
            return false;
        }
        let pk_name = self
            .table_metadata
            .schema
            .field(self.pk_column_indices[0])
            .name();
        filters
            .iter()
            .any(|expr| pk_selective_in_or_range(expr, pk_name))
    }

    /// Build a Vortex-pushdown tombstone exclusion filter for sparse Int64 PK
    /// deletes on the main scan path. Rows guaranteed hidden by the deletion
    /// index are expressed as `pk NOT IN (...)` so Vortex can prune chunks and
    /// listing can drop non-overlapping files before decode.
    pub(super) fn vortex_key_delete_pushdown_filter(
        &self,
        deletion_snapshot: &PkDeletionSnapshot,
    ) -> Option<Expr> {
        let PkDeletionSnapshot::Int64Pk { tombstones } = deletion_snapshot else {
            return None;
        };
        if self.pk_column_indices.len() != 1 {
            return None;
        }
        let pk_name = self
            .table_metadata
            .schema
            .field(self.pk_column_indices[0])
            .name();
        super::file_pruning::tombstone_exclusion_filter(
            pk_name,
            tombstones,
            InsertRecordHandling::Apply,
            None,
            MAX_VORTEX_KEY_DELETE_PUSHDOWN,
        )
    }
}

/// Walks `expr` looking for `Column(name) = Literal` (or the flipped form).
/// Conjunctions (`AND`) are descended into so `DataFusion`'s split-conjunction
/// or coalesced `BinaryExpr(And, _, _)` predicates are both matched.
/// `Cast`/`TryCast` wrappers around either side are unwrapped because
/// type-coercion routinely wraps the literal in a `Cast` to match the
/// column's data type.
pub(super) fn pk_column_equals_literal(expr: &Expr, pk_name: &str) -> bool {
    match expr {
        Expr::BinaryExpr(bin) if bin.op == Operator::Eq => {
            (matches_column(&bin.left, pk_name) && is_literal_like(&bin.right))
                || (matches_column(&bin.right, pk_name) && is_literal_like(&bin.left))
        }
        Expr::BinaryExpr(bin) if bin.op == Operator::And => {
            pk_column_equals_literal(&bin.left, pk_name)
                || pk_column_equals_literal(&bin.right, pk_name)
        }
        _ => false,
    }
}

pub(super) fn matches_column(expr: &Expr, name: &str) -> bool {
    match expr {
        Expr::Column(col) => col.name == name,
        Expr::Cast(c) => matches_column(&c.expr, name),
        Expr::TryCast(c) => matches_column(&c.expr, name),
        _ => false,
    }
}

pub(super) fn is_literal_like(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_, _) => true,
        Expr::Cast(c) => is_literal_like(&c.expr),
        Expr::TryCast(c) => is_literal_like(&c.expr),
        _ => false,
    }
}

/// `true` when `expr` is a selective single-column Int64 PK `IN` or `BETWEEN`.
pub(super) fn pk_selective_in_or_range(expr: &Expr, pk_name: &str) -> bool {
    match expr {
        Expr::InList(in_list) => {
            if in_list.negated || !matches_column(&in_list.expr, pk_name) {
                return false;
            }
            let len = in_list.list.len();
            if len == 0 || len > MAX_PK_SELECTIVE_INLIST_VALUES {
                return false;
            }
            in_list
                .list
                .iter()
                .all(|item| extract_integer_literal(item).is_some())
        }
        Expr::Between(between) => {
            if between.negated || !matches_column(&between.expr, pk_name) {
                return false;
            }
            match (
                extract_integer_literal(&between.low),
                extract_integer_literal(&between.high),
            ) {
                (Some(lo), Some(hi)) => {
                    let span = hi.checked_sub(lo).and_then(|d| d.checked_add(1));
                    span.is_some_and(|span| span <= MAX_PK_SELECTIVE_RANGE_SPAN)
                }
                _ => false,
            }
        }
        Expr::BinaryExpr(bin) if bin.op == Operator::And => {
            pk_selective_in_or_range(&bin.left, pk_name)
                || pk_selective_in_or_range(&bin.right, pk_name)
        }
        _ => false,
    }
}

/// If `expr` is an `InList` of integer literals over consecutive values, rewrite
/// to `col BETWEEN min AND max`. BETWEEN is ~50 % faster than IN-list at the
/// per-row predicate evaluation level (two `i64` comparisons vs an N-element
/// `HashSet` membership probe) and is semantically equivalent. See
/// `pk_in_list_vs_range_rewrite` bench. Non-rewritable inputs (negated list,
/// short list, non-integer literals, sparse values, duplicate values) are
/// returned unchanged.
pub(crate) fn rewrite_consecutive_inlist_to_range(expr: Expr) -> Expr {
    rewrite_consecutive_inlist_to_range_if_needed(&expr).unwrap_or(expr)
}

pub(super) fn rewrite_consecutive_inlist_to_range_if_needed(expr: &Expr) -> Option<Expr> {
    let Expr::InList(in_list) = &expr else {
        return None;
    };
    if in_list.negated || in_list.list.len() < MIN_CONSECUTIVE_INLIST_REWRITE_VALUES {
        return None;
    }
    let original_len = in_list.list.len();
    let mut values: Vec<i64> = Vec::with_capacity(original_len);
    for item in &in_list.list {
        let v = extract_integer_literal(item)?;
        values.push(v);
    }
    values.sort_unstable();
    values.dedup();
    if values.len() != original_len {
        return None;
    }
    // Safe: sorted+deduped+len>=2 guarantees both ends exist.
    let min = values[0];
    let max = values[values.len() - 1];
    let span = max.checked_sub(min).and_then(|d| d.checked_add(1))?;
    if usize::try_from(span).ok() != Some(values.len()) {
        return None;
    }
    let col_expr = (*in_list.expr).clone();
    let lit_min = Expr::Literal(ScalarValue::Int64(Some(min)), None);
    let lit_max = Expr::Literal(ScalarValue::Int64(Some(max)), None);
    Some(Expr::Between(datafusion_expr::expr::Between::new(
        Box::new(col_expr),
        false,
        Box::new(lit_min),
        Box::new(lit_max),
    )))
}

pub(super) fn rewritten_scan_filters(
    filters: &[Expr],
    retention_keep_filter: Option<&Expr>,
) -> Option<Vec<Expr>> {
    if let Some(keep_filter) = retention_keep_filter {
        return Some(
            filters
                .iter()
                .map(|filter| {
                    rewrite_consecutive_inlist_to_range_if_needed(filter)
                        .unwrap_or_else(|| filter.clone())
                })
                .chain(std::iter::once(keep_filter.clone()))
                .collect(),
        );
    }

    for (index, filter) in filters.iter().enumerate() {
        if let Some(rewritten_filter) = rewrite_consecutive_inlist_to_range_if_needed(filter) {
            let mut effective_filters = Vec::with_capacity(filters.len());
            effective_filters.extend(filters[..index].iter().cloned());
            effective_filters.push(rewritten_filter);
            effective_filters.extend(filters[index + 1..].iter().map(|filter| {
                rewrite_consecutive_inlist_to_range_if_needed(filter)
                    .unwrap_or_else(|| filter.clone())
            }));
            return Some(effective_filters);
        }
    }

    None
}

/// Returns `Some(v)` if `expr` is an integer-typed literal (possibly wrapped
/// in a `Cast`/`TryCast`). `i8`/`i16`/`i32` widen to `i64`. Any other shape
/// returns `None`.
pub(super) fn extract_integer_literal(expr: &Expr) -> Option<i64> {
    let raw = match expr {
        Expr::Literal(s, _) => s,
        Expr::Cast(c) => match &*c.expr {
            Expr::Literal(s, _) => s,
            _ => return None,
        },
        Expr::TryCast(c) => match &*c.expr {
            Expr::Literal(s, _) => s,
            _ => return None,
        },
        _ => return None,
    };
    match raw {
        ScalarValue::Int64(Some(v)) => Some(*v),
        ScalarValue::Int32(Some(v)) => Some(i64::from(*v)),
        ScalarValue::Int16(Some(v)) => Some(i64::from(*v)),
        ScalarValue::Int8(Some(v)) => Some(i64::from(*v)),
        _ => None,
    }
}
