//! Provider construction, table creation, and CDC durability accessors.
//!
//! Entry points: [`CayenneTableProvider::new`] / `new_internal` (called by
//! `CayenneTableProviderBuilder::open`) and [`CayenneTableProvider::create_table`].
//! Open-time work includes seeding the sequence allocator and inline-visibility
//! watermark from the catalog high-water, crash recovery via
//! `ensure_no_incomplete_write`, and activating orphan inline tombstones
//! (`publish_orphan_inlined_deletes`). Also hosts the memory-mode
//! (`cdc_durability: memory`) gates and the runtime's `SlotAdvancer` wiring.
//!
//! Mechanically split out of `provider/table.rs`; code is moved verbatim.

use crate::catalog::MetadataCatalog;

use super::{
    Arc, ArcSwap, AtomicBool, AtomicI64, AtomicU64, AtomicUsize, BoundedWarningKeys,
    CachedTableStatistics, CatalogResult, CayenneContext, CayenneMemoryAccount,
    CayenneTableProvider, CayenneTableProviderBuilder, CreateTableOptions,
    DefaultFileStatisticsCache, Error, Expr, HashSet, InlinedCache, ParkingMutex,
    PendingTombstoneDeltas, PkDeletionStrategy, PostWriteMaintenance, Result,
    RowConverter, RuntimeEnv, RwLock, SeqAllocator, SortField, TableMetadata,
};

impl CayenneTableProvider {
    /// Create a new Cayenne table provider.
    ///
    /// For more configuration options, use [`CayenneTableProviderBuilder`].
    ///
    /// # Errors
    ///
    /// Returns an error if the table cannot be found in the catalog or if the listing
    /// table cannot be created.
    pub async fn new(
        table_name: &str,
        catalog: Arc<dyn MetadataCatalog>,
        runtime_env: Arc<RuntimeEnv>,
    ) -> Result<Self> {
        CayenneTableProviderBuilder::new(catalog, runtime_env)
            .open(table_name)
            .await
    }

    /// Create a new table provider with explicit retention filters.
    ///
    /// For more configuration options, use [`CayenneTableProviderBuilder`].
    ///
    /// # Errors
    ///
    /// Returns an error if the table cannot be found in the catalog or if the listing
    /// table cannot be created.
    pub async fn new_with_retention(
        table_name: &str,
        catalog: Arc<dyn MetadataCatalog>,
        retention_filters: Vec<Expr>,
        runtime_env: Arc<RuntimeEnv>,
    ) -> Result<Self> {
        CayenneTableProviderBuilder::new(catalog, runtime_env)
            .with_retention_filters(retention_filters)
            .open(table_name)
            .await
    }

    /// Internal constructor used by the builder.
    pub(super) async fn new_internal(
        table_name: &str,
        catalog: Arc<dyn MetadataCatalog>,
        retention_filters: Vec<Expr>,
        time_retention_filter_builder: Option<super::retention::TimeRetentionFilterBuilder>,
        object_store_config: Option<crate::metadata::ObjectStoreConfig>,
        runtime_env: Arc<RuntimeEnv>,
        context: Option<Arc<CayenneContext>>,
    ) -> CatalogResult<Self> {
        let table_metadata = catalog.get_table(table_name).await?;

        // Use the provided context (for partition cache sharing) or build a
        // fresh one from this table's VortexConfig and the shared RuntimeEnv.
        let context = context.unwrap_or_else(|| {
            CayenneContext::new(&table_metadata.vortex_config, runtime_env, table_name)
        });

        if table_metadata.path.starts_with("s3://") && object_store_config.is_none() {
            return Err(Error::Internal {
                table: table_name.to_string(),
                message: "Table uses S3 storage but no object_store_config was provided"
                    .to_string(),
            }
            .into());
        }

        // Construct URL to current snapshot
        // Directory structure: [table_path]/[table_id]/[snapshot_id]/
        // All tables have a snapshot ID (created on table initialization)
        let snapshot_dir_url = Self::snapshot_dir_url(
            &table_metadata.path,
            &table_metadata.table_id,
            &table_metadata.current_snapshot_id,
        );

        // Determine if this table has a primary key for key-based deletion
        let has_primary_key = !table_metadata.primary_key.is_empty();

        // Determine PK deletion strategy kind and build RowConverter if needed
        let (pk_deletion_strategy_kind, pk_row_converter, pk_column_indices) = if has_primary_key {
            let schema = &table_metadata.schema;
            let mut indices = Vec::with_capacity(table_metadata.primary_key.len());
            let mut pk_fields = Vec::with_capacity(table_metadata.primary_key.len());

            for pk_col in &table_metadata.primary_key {
                let (idx, field) =
                    schema
                        .column_with_name(pk_col)
                        .ok_or_else(|| Error::DataValidation {
                            table: table_name.to_string(),
                            message: format!("Primary key column '{pk_col}' not found in schema"),
                        })?;
                indices.push(idx);
                pk_fields.push(field.clone());
            }

            // Check if we can use the optimized Int64 PK strategy:
            // - Single column primary key
            // - Column type is Int64
            if pk_fields.len() == 1
                && *pk_fields[0].data_type() == arrow::datatypes::DataType::Int64
            {
                // Optimized path: single Int64 PK - no RowConverter needed
                (PkDeletionStrategy::Int64Pk, None, indices)
            } else {
                // General path: composite or non-integer PK - use RowConverter
                let sort_fields: Vec<SortField> = pk_fields
                    .iter()
                    .map(|f| SortField::new(f.data_type().clone()))
                    .collect();

                let row_converter = RowConverter::new(sort_fields).map_err(Error::from)?;

                (
                    PkDeletionStrategy::RowConverterBased,
                    Some(Arc::new(row_converter)),
                    indices,
                )
            }
        } else {
            (PkDeletionStrategy::PositionBased, None, Vec::new())
        };

        // Load deletion vectors and insert records once at initialization
        // to avoid repeated SQLite queries on every scan.
        // Returns the fully constructed PkDeletionStrategy with embedded caches.
        let table_id = table_metadata.table_id.clone();
        let catalog_for_load = Arc::clone(&catalog);
        let pk_deletion_strategy =
            Self::load_deletion_vectors_all(&table_id, catalog_for_load, pk_deletion_strategy_kind)
                .await?;

        let listing_table = Self::create_listing_table(
            &snapshot_dir_url,
            Arc::<arrow_schema::Schema>::clone(&table_metadata.schema),
            context.file_format(),
            &pk_deletion_strategy,
        )?;
        let table_statistics = Self::load_table_statistics(&catalog, &table_metadata).await;

        // Load protected snapshots from catalog.
        // Protected snapshots are those with sequence > max_delete_sequence.
        // They contain data written after deletions and should skip deletion filtering.
        let protected_snapshots =
            Self::load_protected_snapshots(Arc::clone(&catalog), &table_id, &pk_deletion_strategy)
                .await?;
        let inlined_row_count = catalog.get_inlined_data_count(&table_id).await?;

        // Every inlined entry persisted at open time is already published, and
        // all have `sequence_number <= current_sequence_number`. Seed the
        // visibility watermark there so existing inlined data is visible while
        // future inline writes (which commit at a strictly higher sequence) stay
        // gated until published.
        let initial_inlined_seq = table_metadata.current_sequence_number;

        // Seed the in-memory sequence allocator (lever B2) from the DB
        // high-water. `next = persisted_hi + 1` is the first unused sequence;
        // the allocator invariant `next - 1 == persisted_hi` holds at open with
        // zero handed-out values. A reopen after a crash reseeds from the (only
        // ever monotonically increasing) DB row, so every previously handed-out
        // value is strictly below the new `next` — nothing is reissued.
        let persisted_hi = table_metadata.current_sequence_number;
        let seq_allocator = Arc::new(tokio::sync::Mutex::new(SeqAllocator {
            next: persisted_hi + 1,
            persisted_hi,
        }));

        let force_staging_probe_on_startup = table_metadata.path.starts_with("s3://");

        // Register the S3 object store in the shared RuntimeEnv once during
        // construction. Every code path that creates a SessionContext from
        // `self.context.runtime_env()` (e.g. `create_session_context`, keyset
        // loading, deletion sinks) will automatically inherit the store.
        if let Some(ref config) = object_store_config {
            Self::register_object_store_if_needed(context.runtime_env(), config);
        }

        let mut object_store_registered_runtime_envs = HashSet::new();
        if object_store_config.is_some() {
            object_store_registered_runtime_envs
                .insert(Self::runtime_env_cache_key(context.runtime_env()));
        }

        let table_memory = Arc::new(CayenneMemoryAccount::new(
            &table_metadata.table_id,
            &context.runtime_env().memory_pool,
        ));

        // Per-table in-memory CDC tier caps (`cdc_durability: memory`). A
        // non-positive `cdc_mem_tier_max_bytes` means "no explicit per-table
        // byte cap" — the process-global byte budget still bounds aggregate RAM,
        // so this is `u64::MAX` (effectively unbounded per table). The age cap
        // is passed straight through (0 = age trigger disabled).
        let mem_tier_max_bytes = if table_metadata.vortex_config.cdc_mem_tier_max_bytes > 0 {
            u64::try_from(table_metadata.vortex_config.cdc_mem_tier_max_bytes).unwrap_or(u64::MAX)
        } else {
            u64::MAX
        };
        let mem_tier_max_age_ms = table_metadata.vortex_config.cdc_mem_tier_max_age_ms;

        let provider = Self {
            current_snapshot_id: Arc::new(RwLock::new(table_metadata.current_snapshot_id.clone())),
            table_metadata,
            catalog,
            listing_table: Arc::new(ArcSwap::new(listing_table)),
            listing_fence: Arc::new(tokio::sync::RwLock::new(())),
            scan_file_statistics: Arc::new(DefaultFileStatisticsCache::default()),
            table_statistics: Arc::new(RwLock::new(CachedTableStatistics {
                optimizer_inexact: table_statistics
                    .as_ref()
                    .map(|s| Self::statistics_to_inexact(s.clone())),
                optimizer: table_statistics,
                raw: None, // will be populated on first load/persist
            })),
            table_statistics_persistence_lock: Arc::new(tokio::sync::Mutex::new(())),
            retention_filters,
            time_retention_filter_builder,
            context,
            pk_deletion_strategy,
            pk_row_converter,
            pk_column_indices,
            write_lock: Arc::new(tokio::sync::Mutex::new(())),
            visibility_lock: Arc::new(tokio::sync::Mutex::new(())),
            scan_state_lock: Arc::new(tokio::sync::RwLock::new(())),
            object_store_config,
            object_store_registered_runtime_envs: Arc::new(ParkingMutex::new(
                object_store_registered_runtime_envs,
            )),
            protected_snapshots: Arc::new(ArcSwap::from_pointee(protected_snapshots)),
            protected_snapshot_age_warning_keys: Arc::new(ParkingMutex::new(
                BoundedWarningKeys::default(),
            )),
            pk_keyset_cache: Arc::new(ParkingMutex::new(None)),
            table_memory,
            inline_checkpoint_scheduled: Arc::new(AtomicBool::new(false)),
            inlined_row_count: Arc::new(AtomicI64::new(inlined_row_count)),
            inlined_generation: Arc::new(AtomicU64::new(0)),
            inlined_structural_epoch: Arc::new(AtomicU64::new(0)),
            pending_inline_tombstones: Arc::new(AtomicU64::new(0)),
            published_inlined_seq: Arc::new(AtomicI64::new(initial_inlined_seq)),
            seq_allocator,
            inlined_locally_published: Arc::new(ParkingMutex::new(HashSet::new())),
            pending_durable_tombstone_flips: Arc::new(ParkingMutex::new(Vec::new())),
            pending_tombstone_deltas: Arc::new(
                ParkingMutex::new(PendingTombstoneDeltas::default()),
            ),
            inlined_cache: Arc::new(ArcSwap::new(Arc::new(InlinedCache {
                // Sentinel: first `read_inlined_batches` / `cached_inlined_view` call always misses.
                // The `u64::MAX` structural epoch can never match the live counter, so the first
                // touch always full-rebuilds (never takes the append-only delta path off the sentinel).
                generation: u64::MAX,
                structural_epoch: u64::MAX,
                materialized_through_sequence: i64::MIN,
                tombstone_delta_seq: 0,
                batches: Arc::new(Vec::new()),
                view: Arc::new(Vec::new()),
            }))),
            mem_tier: Arc::new(ArcSwap::from_pointee(
                crate::provider::mem_tier::MemTier::empty(),
            )),
            mem_checkpoint_lock: Arc::new(tokio::sync::Mutex::new(())),
            slot_advancer: Arc::new(ParkingMutex::new(None)),
            mem_tier_max_bytes,
            mem_tier_max_age_ms,
            // Local providers can use `ensure_no_incomplete_write`'s
            // non-destructive fast path: it probes `_staging/` and returns if
            // the directory is absent or empty. Starting every provider in the
            // dirty state makes concurrent read-only opens race while removing
            // and recreating the same staging directory. S3 keeps the forced
            // probe because the fast path intentionally avoids an object-store
            // list when both flags are clear.
            staging_wal_present: Arc::new(AtomicBool::new(force_staging_probe_on_startup)),
            staging_may_have_files: Arc::new(AtomicBool::new(force_staging_probe_on_startup)),
            inflight_staging_appends: Arc::new(ParkingMutex::new(HashSet::new())),
            new_files_since_last_compaction: Arc::new(AtomicUsize::new(0)),
            last_moved_snapshot_files: Arc::new(ParkingMutex::new(None)),
            compaction_lock: Arc::new(tokio::sync::Mutex::new(())),
            post_write_compaction_scheduled: Arc::new(AtomicBool::new(false)),
            post_write_maintenance: Arc::new(PostWriteMaintenance::default()),
            background_compactor: Arc::new(std::sync::OnceLock::new()),
            background_mem_tier_checkpointer: Arc::new(std::sync::OnceLock::new()),
        };

        provider.refresh_deletion_memory_accounting();

        // Fail construction if a staging WAL exists — the table may contain
        // partial data from an interrupted append and must be resolved first.
        provider.ensure_no_incomplete_write().await?;

        // Recovery (above) completes any interrupted staged append by moving its
        // replacement files into their snapshot. Any inline tombstone still
        // `published = false` in the metastore therefore belongs to a staged
        // inline-conflict upsert whose replacement is now durable — activate it
        // so the upsert applies exactly once across the crash (replacement
        // visible, old inline copy hidden) instead of leaving a duplicate. There
        // are no in-flight runtime writers at open time, so this cannot race a
        // live stage. Skipped for position-based tables (no inline tombstones).
        if !provider.pk_deletion_strategy.is_position_based() {
            let flipped = provider
                .catalog
                .publish_orphan_inlined_deletes(&provider.table_metadata.table_id)
                .await
                .map_err(|source| Error::Catalog { source })?;
            if flipped > 0 {
                tracing::info!(
                    table = %provider.table_metadata.table_name,
                    flipped,
                    "Activated staged inline tombstone(s) recovered from an interrupted upsert"
                );
                // Structural: recovery activated tombstones that re-filter inline
                // entries. (At open time the cache is still the empty sentinel, so
                // this only sets the baseline epoch; kept structural for
                // correctness if the cache was ever warmed before this point.)
                provider.bump_inlined_structural_epoch();
            }
        }

        Ok(provider)
    }

    /// Create a new table in Cayenne.
    ///
    /// For more configuration options, use [`CayenneTableProviderBuilder`].
    ///
    /// # Errors
    ///
    /// Returns an error if the table cannot be created in the catalog.
    pub async fn create_table(
        catalog: Arc<dyn MetadataCatalog>,
        options: CreateTableOptions,
        runtime_env: Arc<RuntimeEnv>,
    ) -> Result<Self> {
        CayenneTableProviderBuilder::new(catalog, runtime_env)
            .create(options)
            .await
            .map_err(Into::into)
    }

    /// Create a new table in Cayenne with retention filters applied to subsequent writes.
    ///
    /// For more configuration options, use [`CayenneTableProviderBuilder`].
    ///
    /// # Errors
    ///
    /// Returns an error if the table cannot be created in the catalog.
    pub async fn create_table_with_retention(
        catalog: Arc<dyn MetadataCatalog>,
        options: CreateTableOptions,
        retention_filters: Vec<Expr>,
        runtime_env: Arc<RuntimeEnv>,
    ) -> Result<Self> {
        CayenneTableProviderBuilder::new(catalog, runtime_env)
            .with_retention_filters(retention_filters)
            .create(options)
            .await
            .map_err(Into::into)
    }

    /// Get a reference to the catalog.
    ///
    /// This is useful for testing and advanced use cases that need direct catalog access.
    #[must_use]
    pub fn catalog(&self) -> &Arc<dyn MetadataCatalog> {
        &self.catalog
    }

    /// Get the table metadata.
    #[must_use]
    pub fn metadata(&self) -> &TableMetadata {
        &self.table_metadata
    }

    /// The configured CDC durability mode for this table.
    #[must_use]
    pub fn cdc_durability(&self) -> crate::metadata::CdcDurability {
        self.table_metadata.vortex_config.cdc_durability
    }

    /// Whether this table runs the in-memory CDC durability path. Memory mode is
    /// only honored for the key-based merge-on-read shape on a non-partitioned
    /// table — partitioned tables (whose visibility flip can't be deferred) and
    /// position-only PK-less tables keep the durable path even if the param is
    /// set, so the durable semantics there are untouched.
    #[must_use]
    pub fn is_cdc_memory_mode(&self) -> bool {
        self.cdc_durability().is_memory()
            && self.table_metadata.partition_column.is_none()
            && !self.pk_deletion_strategy.is_position_based()
    }

    /// Install the runtime's [`SlotAdvancer`] handle so `checkpoint_mem_tier` can
    /// advance the source slot after a durable checkpoint (memory mode). Called
    /// once by the runtime when it wires up a memory-mode CDC stream; a no-op
    /// effect in file mode (the checkpoint path simply never fires the callback).
    pub fn install_slot_advancer(
        &self,
        advancer: Arc<dyn crate::provider::mem_tier::SlotAdvancer>,
    ) {
        *self.slot_advancer.lock() = Some(advancer);
    }

    /// Clear the installed [`SlotAdvancer`], forcing subsequent CDC appends onto
    /// the durable path until the runtime re-arms memory-mode deferral for an
    /// all-deferrable upsert burst.
    pub fn clear_slot_advancer(&self) {
        *self.slot_advancer.lock() = None;
    }

    /// Whether the runtime has installed a [`SlotAdvancer`] — i.e. armed in-memory
    /// deferral after confirming a replayable source committer. The write path
    /// gates mem-mode engagement on this (in addition to [`Self::is_cdc_memory_mode`])
    /// so a non-replayable source never buffers un-acked rows in RAM.
    #[must_use]
    pub fn has_slot_advancer(&self) -> bool {
        self.slot_advancer.lock().is_some()
    }

    /// The per-table mem-tier checkpoint lock, for the write path to serialize
    /// spills (only one checkpoint in flight at a time — the OOM-safety guard).
    pub(crate) fn mem_checkpoint_lock_for_writer(&self) -> Arc<tokio::sync::Mutex<()>> {
        Arc::clone(&self.mem_checkpoint_lock)
    }
}
