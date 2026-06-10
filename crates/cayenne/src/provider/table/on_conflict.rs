//! On-conflict (upsert) validation stream and conflict application.
//!
//! [`CayenneTableProvider::prepare_stream_for_insert`] wraps an insert stream in
//! an [`OnConflictValidationStream`], which validates each batch against the
//! cached PK existence index (`apply_on_conflict_to_batch`) and accumulates the
//! computed deletions into [`PostValidationState`] for the writer to apply.
//! The keyset is taken from / restored to `pk_keyset_cache`; callers run under
//! the table `write_lock`. Also hosts the inline-aware and keyset-invalidating
//! deletion-sink wrappers used by `delete_from`.
//!
//! Mechanically split out of `provider/table.rs`; code is moved verbatim.

use datafusion_physical_plan::RecordBatchStream;

use super::{
    Arc, CachedPkIndex, CayenneDeletionSink, CayenneTableProvider, Context, DeletionSink, Error,
    Expr, HashMap, HashSet, InlinedData, Instant, Int64PkDeletionSnapshot, OnConflict, OwnedRow,
    ParkingMutex, Pin, PkConflictDetection, PkDeletionStrategyWithCache, PkExistenceRef, Poll,
    RecordBatch, Result, RowConverter, RowConverterDeletionSnapshot, RowLocation, SchemaRef,
    SendableRecordBatchStream, Stream, StreamExt, TryStreamExt, async_trait,
};

/// Extension trait to extract `UpsertOptions` from `OnConflict`.
///
/// The upstream `OnConflict` enum only contains `ColumnReference`, but our on-conflict
/// logic requires `UpsertOptions`. This trait provides a compatibility shim.
pub(super) trait OnConflictExt {
    /// Returns `UpsertOptions` for this `OnConflict` variant.
    /// Currently returns default options; future versions may store options in `OnConflict`.
    fn get_upsert_options(&self) -> UpsertOptions;
}

impl OnConflictExt for OnConflict {
    fn get_upsert_options(&self) -> UpsertOptions {
        UpsertOptions::default()
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct UpsertOptions {
    pub(super) remove_duplicates: bool,
    pub(super) last_write_wins: bool,
}

impl UpsertOptions {
    pub(super) fn is_default(self) -> bool {
        !self.remove_duplicates && !self.last_write_wins
    }
}

#[derive(Default)]
pub(super) struct ExtractedPrimaryKeys {
    pub(super) int64_pk: Vec<i64>,
    pub(super) row_keys: Vec<Box<[u8]>>,
}

#[derive(Default)]
pub(super) struct InlinedDataRewrite {
    pub(super) updated_data: Vec<InlinedData>,
    pub(super) deleted_inlined_ids: Vec<String>,
    pub(super) removed_rows: usize,
}

impl InlinedDataRewrite {
    #[must_use]
    pub(super) fn is_empty(&self) -> bool {
        self.updated_data.is_empty() && self.deleted_inlined_ids.is_empty()
    }
}

pub(super) struct InlineAwareDeletionSink {
    pub(super) table: CayenneTableProvider,
    pub(super) file_sink: CayenneDeletionSink,
    pub(super) filters: Vec<Expr>,
}

pub(super) struct PkKeysetInvalidatingDeletionSink {
    pub(super) table: CayenneTableProvider,
    pub(super) inner: Arc<dyn DeletionSink>,
}

#[async_trait]
impl DeletionSink for PkKeysetInvalidatingDeletionSink {
    async fn delete_from(
        &self,
    ) -> std::result::Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let deleted = self.inner.delete_from().await?;
        if deleted > 0 {
            // Keyset clear-on-delete avoidance (cycle-4 incremental lever).
            //
            // This is a FILTER-based DELETE (`DELETE … WHERE <predicate>`), so
            // the deleted PK set is NOT enumerable at this call site — only the
            // count is. We therefore cannot surgically `remove` keys from the
            // Exact keyset here. But for an `Upsert` table we do not need to:
            // leaving a deleted key STALE-PRESENT in the existence index only
            // ever produces a redundant key-based delete tombstone on a later
            // re-insert of that PK, which masks no prior version (none exists)
            // and is harmless — exactly the false-positive invariant documented
            // on `PkBloom` (in `pk_cache.rs`) and exercised on the upsert
            // existence path in `apply_on_conflict_to_batch` (both the Exact and
            // the Bloom arm keep the row and emit at most a
            // no-op delete). So for upsert tables we SKIP the clear entirely and
            // keep the stale-superset index — eliminating the O(live-rows)
            // `load_existing_keyset` cold rebuild the next CDC insert batch would
            // otherwise pay (measured 277 ms × 244 = 68 s/600 s on `new_order`).
            //
            // `DoNothing` tables need an EXACT answer (a stale-present entry would
            // wrongly DROP a genuinely new row in `apply_on_conflict_to_batch`),
            // and their keys are not enumerable on this filter path, so
            // they keep the conservative full clear and rebuild next batch.
            // `upsert_bloom_eligible()` is precisely "is this an `Upsert` table".
            if !self.table.upsert_bloom_eligible() {
                self.table.clear_cached_pk_keyset();
            }
            // Drop the per-file stats `CayenneTableProvider::collect_scan_file_statistics`
            // caches. Without this, a follow-up `COUNT(*)` (or any other stats-driven
            // query) is served the row count we computed *before* this delete added
            // its rows to the position-based deletion vector, so the count is stale —
            // see `tests/position_based_deletion_test.rs::test_position_based_sequential_deletes`.
            // (Independent of the keyset: always invalidate so counts stay fresh.)
            self.table.invalidate_scan_file_statistics();
        }
        Ok(deleted)
    }
}

#[async_trait]
impl DeletionSink for InlineAwareDeletionSink {
    async fn delete_from(
        &self,
    ) -> std::result::Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let _write_guard = self.table.write_lock.lock().await;

        let inlined_deleted = self
            .table
            .delete_inlined_rows_matching_filters(&self.filters)
            .await?;
        let file_deleted = self.file_sink.delete_from().await?;

        let deleted = inlined_deleted.checked_add(file_deleted).ok_or_else(|| {
            Box::new(datafusion_common::DataFusionError::Execution(
                "Deleted row count overflowed u64".to_string(),
            )) as Box<dyn std::error::Error + Send + Sync>
        })?;

        if deleted > 0 {
            // Keyset clear-on-delete avoidance (cycle-4 incremental lever) — see
            // the detailed rationale on `PkKeysetInvalidatingDeletionSink::delete_from`.
            // This is a FILTER-based DELETE, so the deleted PK set is not in hand
            // here. For an `Upsert` table a stale-present existence entry only
            // yields a harmless redundant delete on a later re-insert (the
            // `PkBloom` false-positive invariant, see `pk_cache.rs`), so we SKIP
            // the clear and avoid the O(live-rows) `load_existing_keyset` rebuild
            // the next insert batch would pay. `DoNothing` tables need exactness
            // (a stale entry would wrongly drop a new row) and keep the full clear.
            if !self.table.upsert_bloom_eligible() {
                self.table.clear_cached_pk_keyset();
            }
            if file_deleted > 0 && self.table.pk_deletion_strategy.is_position_based() {
                self.table.clear_scan_file_statistics_cache();
            }
        }

        Ok(deleted)
    }
}

pub(super) struct BatchValidationResult {
    pub(super) filtered_batch: Option<RecordBatch>,
    /// Per-file position deletes for located conflict rows: file path -> deleted
    /// file-local row positions. Empty unless `deletion_mode: position`.
    pub(super) delete_specs: Vec<(Arc<str>, Vec<u64>)>,
    pub(super) kept_keys: HashSet<OwnedRow>,
    /// File-backed Int64 PK values being deleted (for `Int64Pk` strategy).
    pub(super) deleted_pk_i64: Vec<i64>,
    /// File-backed row key bytes being deleted (for `RowConverterBased` strategy).
    pub(super) deleted_row_keys: Vec<Box<[u8]>>,
    /// Inlined Int64 PK values being deleted.
    pub(super) deleted_inlined_pk_i64: Vec<i64>,
    /// Inlined row key bytes being deleted.
    pub(super) deleted_inlined_row_keys: Vec<Box<[u8]>>,
}

pub(crate) struct PreparedInsertStream {
    pub(crate) stream: SendableRecordBatchStream,
    pub(super) post_validation: Arc<ParkingMutex<Option<PostValidationState>>>,
    pub(super) may_have_on_conflict_deletions: bool,
}

impl PreparedInsertStream {
    pub(super) fn immediate(stream: SendableRecordBatchStream) -> Self {
        Self {
            stream,
            post_validation: Arc::new(ParkingMutex::new(Some(PostValidationState::default()))),
            may_have_on_conflict_deletions: false,
        }
    }

    pub(super) fn deferred(
        stream: SendableRecordBatchStream,
        post_validation: Arc<ParkingMutex<Option<PostValidationState>>>,
        may_have_on_conflict_deletions: bool,
    ) -> Self {
        Self {
            stream,
            post_validation,
            may_have_on_conflict_deletions,
        }
    }

    pub(crate) fn post_validation(&self) -> Arc<ParkingMutex<Option<PostValidationState>>> {
        Arc::clone(&self.post_validation)
    }

    #[must_use]
    pub(crate) const fn may_have_on_conflict_deletions(&self) -> bool {
        self.may_have_on_conflict_deletions
    }
}

#[derive(Default)]
pub(crate) struct OnConflictDeletions {
    /// Per-file position deletes: file path -> deleted file-local row positions.
    /// Routed to the position-vector write path; empty unless `deletion_mode: position`.
    pub(crate) delete_specs: HashMap<Arc<str>, Vec<u64>>,
    /// Deleted file-backed Int64 PK values (for `Int64Pk` strategy).
    pub(crate) deleted_pk_i64: Vec<i64>,
    /// Deleted file-backed row keys (for `RowConverterBased` strategy).
    pub(crate) deleted_row_keys: Vec<Box<[u8]>>,
    /// Deleted inlined Int64 PK values.
    pub(crate) deleted_inlined_pk_i64: Vec<i64>,
    /// Deleted inlined row keys.
    pub(crate) deleted_inlined_row_keys: Vec<Box<[u8]>>,
}

impl OnConflictDeletions {
    /// Total number of existing rows superseded (deleted) by this upsert across
    /// all strategies (position deletes + file-backed + inlined). Used to net the
    /// live row count: an upsert that replaces N existing rows adds
    /// `inserted - N` live rows, not `inserted`.
    pub(crate) fn total_superseded(&self) -> usize {
        self.delete_specs.values().map(Vec::len).sum::<usize>()
            + self.deleted_pk_i64.len()
            + self.deleted_row_keys.len()
            + self.deleted_inlined_pk_i64.len()
            + self.deleted_inlined_row_keys.len()
    }
}

/// `apply_on_conflict_deletions` performs all durable deletion-vector and
/// inlined-data rewrite I/O but returns the computed in-memory visibility
/// updates instead of storing them, so the stores can be committed
/// synchronously — together with the protected snapshot publish — under a
/// single `scan_state_lock.write()`. This keeps the scan-excluding guard held
/// for microseconds rather than across durable writes.
pub(crate) struct OnConflictUpdate {
    pub(super) deletion_update: OnConflictDeletionUpdate,
    /// Set when `apply_on_conflict_deletions` durably wrote an inline tombstone
    /// (via `add_inlined_delete`) to hide the prior inline copy of an upserted
    /// PK. Publishing must then bump `inlined_generation` (under
    /// `scan_state_lock`) so the next scan rebuilds the inline view and observes
    /// the tombstone atomically with the deletion-cache + protected-snapshot
    /// flips. A tombstone only adds a hide-marker — it appends no inline DATA
    /// rows and changes no row count — so unlike the previous inline-rewrite
    /// path there is no visibility watermark to advance.
    pub(super) inlined_tombstone_written: bool,
}

impl OnConflictUpdate {
    pub(super) fn none() -> Self {
        Self {
            deletion_update: OnConflictDeletionUpdate::None,
            inlined_tombstone_written: false,
        }
    }

    pub(super) fn from_deletion_update(deletion_update: OnConflictDeletionUpdate) -> Self {
        Self {
            deletion_update,
            inlined_tombstone_written: false,
        }
    }

    pub(super) fn with_inlined_tombstone_written(mut self, written: bool) -> Self {
        self.inlined_tombstone_written = written;
        self
    }

    pub(super) fn is_empty(&self) -> bool {
        matches!(self.deletion_update, OnConflictDeletionUpdate::None)
            && !self.inlined_tombstone_written
    }
}

pub(super) enum OnConflictDeletionUpdate {
    /// No key-based deletion-cache change (pure position deletes or no deletes).
    None,
    /// New `Int64Pk` deletion snapshot to publish.
    Int64Pk(Arc<Int64PkDeletionSnapshot>),
    /// New `RowConverterBased` deletion snapshot to publish.
    RowConverter(Arc<RowConverterDeletionSnapshot>),
}

#[derive(Default)]
pub(crate) struct PostValidationState {
    pub(crate) on_conflict_deletions: OnConflictDeletions,
    pub(crate) validated_keys: HashSet<OwnedRow>,
}

pub(super) struct OnConflictContext<'a> {
    pub(super) pk_indices: &'a [usize],
    pub(super) converter: &'a RowConverter,
    pub(super) on_conflict: &'a OnConflict,
    pub(super) upsert_options: &'a UpsertOptions,
    pub(super) existing: PkExistenceRef<'a>,
    pub(super) incoming_keys: &'a HashSet<OwnedRow>,
}

pub(super) struct OnConflictValidationStream {
    pub(super) table: CayenneTableProvider,
    pub(super) inner: SendableRecordBatchStream,
    pub(super) schema: SchemaRef,
    pub(super) pk_indices: Vec<usize>,
    pub(super) converter: RowConverter,
    pub(super) on_conflict: OnConflict,
    pub(super) upsert_options: UpsertOptions,
    pub(super) existing_keys: Option<CachedPkIndex>,
    pub(super) incoming_keys: HashSet<OwnedRow>,
    pub(super) kept_keys: HashSet<OwnedRow>,
    pub(super) delete_specs: HashMap<Arc<str>, Vec<u64>>,
    pub(super) deleted_pk_i64: Vec<i64>,
    pub(super) deleted_row_keys: Vec<Box<[u8]>>,
    pub(super) deleted_inlined_pk_i64: Vec<i64>,
    pub(super) deleted_inlined_row_keys: Vec<Box<[u8]>>,
    pub(super) post_validation: Arc<ParkingMutex<Option<PostValidationState>>>,
    pub(super) finalized: bool,
}

impl OnConflictValidationStream {
    pub(super) fn new(
        table: CayenneTableProvider,
        inner: SendableRecordBatchStream,
        pk_indices: Vec<usize>,
        converter: RowConverter,
        existing_keys: CachedPkIndex,
        on_conflict: OnConflict,
        post_validation: Arc<ParkingMutex<Option<PostValidationState>>>,
    ) -> Self {
        let schema = inner.schema();
        let upsert_options = on_conflict.get_upsert_options();
        Self {
            table,
            inner,
            schema,
            pk_indices,
            converter,
            on_conflict,
            upsert_options,
            existing_keys: Some(existing_keys),
            incoming_keys: HashSet::with_capacity(1024),
            kept_keys: HashSet::with_capacity(1024),
            delete_specs: HashMap::new(),
            deleted_pk_i64: Vec::new(),
            deleted_row_keys: Vec::new(),
            deleted_inlined_pk_i64: Vec::new(),
            deleted_inlined_row_keys: Vec::new(),
            post_validation,
            finalized: false,
        }
    }

    pub(super) fn process_batch(
        &mut self,
        batch: RecordBatch,
    ) -> datafusion_common::Result<Option<RecordBatch>> {
        if batch.num_rows() == 0 {
            return Ok(None);
        }

        let existing_index = self.existing_keys.as_ref().ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(format!(
                "On-conflict validation for table {} was polled after finalization",
                self.table.table_name()
            ))
        })?;
        let existing = match existing_index {
            CachedPkIndex::Exact(keyset) => PkExistenceRef::Exact(&keyset.keys),
            CachedPkIndex::Bloom(bloom) => PkExistenceRef::Bloom(bloom),
        };

        let mut ctx = OnConflictContext {
            pk_indices: &self.pk_indices,
            converter: &self.converter,
            on_conflict: &self.on_conflict,
            upsert_options: &self.upsert_options,
            existing,
            incoming_keys: &self.incoming_keys,
        };

        let validation_start = Instant::now();
        let validation_result = self.table.apply_on_conflict_to_batch(batch, &mut ctx);
        record_cayenne_write_phase(
            self.table.table_name(),
            "apply_on_conflict_validation",
            validation_start,
        );

        let BatchValidationResult {
            filtered_batch,
            delete_specs: batch_delete_specs,
            kept_keys,
            deleted_pk_i64,
            deleted_row_keys,
            deleted_inlined_pk_i64,
            deleted_inlined_row_keys,
        } = validation_result.map_err(datafusion_common::DataFusionError::from)?;

        for (file_path, rows) in batch_delete_specs {
            self.delete_specs.entry(file_path).or_default().extend(rows);
        }

        self.deleted_pk_i64.extend(deleted_pk_i64);
        self.deleted_row_keys.extend(deleted_row_keys);
        self.deleted_inlined_pk_i64.extend(deleted_inlined_pk_i64);
        self.deleted_inlined_row_keys
            .extend(deleted_inlined_row_keys);

        self.incoming_keys.extend(kept_keys.iter().cloned());
        self.kept_keys.extend(kept_keys);

        Ok(filtered_batch)
    }

    pub(super) fn store_existing_keyset(&mut self) {
        if let Some(existing_keys) = self.existing_keys.take() {
            self.table.store_cached_pk_index(existing_keys);
        }
    }

    pub(super) fn finish_success(&mut self) {
        if self.finalized {
            return;
        }

        self.store_existing_keyset();
        let post_validation = PostValidationState {
            on_conflict_deletions: OnConflictDeletions {
                delete_specs: std::mem::take(&mut self.delete_specs),
                deleted_pk_i64: std::mem::take(&mut self.deleted_pk_i64),
                deleted_row_keys: std::mem::take(&mut self.deleted_row_keys),
                deleted_inlined_pk_i64: std::mem::take(&mut self.deleted_inlined_pk_i64),
                deleted_inlined_row_keys: std::mem::take(&mut self.deleted_inlined_row_keys),
            },
            validated_keys: std::mem::take(&mut self.kept_keys),
        };
        *self.post_validation.lock() = Some(post_validation);
        self.finalized = true;
    }

    pub(super) fn finish_after_error(&mut self) {
        if self.finalized {
            return;
        }

        self.store_existing_keyset();
        self.finalized = true;
    }
}

pub(crate) fn record_cayenne_write_phase(table_name: &str, phase: &'static str, start: Instant) {
    let elapsed = start.elapsed();
    tracing::debug!(
        table = table_name,
        phase,
        duration_ms = elapsed.as_millis(),
        "Cayenne write phase completed"
    );
    telemetry::track_cayenne_write_phase_duration(
        elapsed,
        &[
            telemetry::KeyValue::new("table", table_name.to_string()),
            telemetry::KeyValue::new("phase", phase),
        ],
    );
}

impl Unpin for OnConflictValidationStream {}

impl futures::Stream for OnConflictValidationStream {
    type Item = datafusion_common::Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.finalized {
            return Poll::Ready(None);
        }

        loop {
            match this.inner.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    this.finish_success();
                    return Poll::Ready(None);
                }
                Poll::Ready(Some(Err(err))) => {
                    this.finish_after_error();
                    return Poll::Ready(Some(Err(err)));
                }
                Poll::Ready(Some(Ok(batch))) => match this.process_batch(batch) {
                    Ok(Some(filtered_batch)) => return Poll::Ready(Some(Ok(filtered_batch))),
                    Ok(None) => {}
                    Err(err) => {
                        this.finish_after_error();
                        return Poll::Ready(Some(Err(err)));
                    }
                },
            }
        }
    }
}

impl RecordBatchStream for OnConflictValidationStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl CayenneTableProvider {
    /// Prepare an incoming stream for insert by validating `on_conflict` constraints.
    ///
    /// If a primary key is configured, this method:
    /// 1. Loads existing keys from the table (respecting deletion visibility)
    /// 2. Validates incoming rows against `on_conflict` behavior (drop/upsert)
    /// 3. Returns a prepared stream with conflicts resolved and deletion specs
    ///
    /// If no primary key is configured, returns the stream unchanged with empty deletion specs.
    /// If `pk_conflict_detection` is `none`, returns the stream unchanged and trusts the source
    /// to enforce PK uniqueness; no existing data is scanned.
    pub(crate) async fn prepare_stream_for_insert(
        &self,
        stream: SendableRecordBatchStream,
    ) -> Result<PreparedInsertStream> {
        let Some(pk_indices) = self.primary_key_indices()? else {
            return Ok(PreparedInsertStream::immediate(stream));
        };

        if self.context.pk_conflict_detection() == PkConflictDetection::None {
            tracing::trace!(
                table = %self.table_metadata.table_name,
                "Skipping Cayenne primary-key conflict detection for append"
            );
            return Ok(PreparedInsertStream::immediate(stream));
        }

        let converter = self.build_pk_converter(&pk_indices)?;
        let existing_keys = if let Some(existing_keys) = self.take_cached_pk_index() {
            tracing::trace!(
                "prepare_stream_for_insert: reused {} cached existing keys for table {}",
                existing_keys.len(),
                self.table_metadata.table_name
            );
            existing_keys
        } else {
            // The full-table keyset rebuild is the dominant CDC-upsert cost for
            // tables whose keyset exceeds the cache budget, yet it runs *before*
            // the `vortex_write` phase timer and is otherwise invisible in
            // per-phase telemetry. Time it explicitly (emitted only on a cache
            // miss / cold rebuild) so retests attribute the cost correctly.
            let keyset_rebuild_start = Instant::now();
            // Fast path: reconstruct the index from the persisted bloom checkpoint
            // (+ bounded post-checkpoint delta) and skip the full-table keyset
            // scan. Falls back to the full scan on any miss/mismatch/corruption.
            let existing_keys = match self
                .try_load_persisted_pk_index(&pk_indices, &converter)
                .await
            {
                Ok(Some(index)) => index,
                _ => {
                    CachedPkIndex::Exact(self.load_existing_keyset(&pk_indices, &converter).await?)
                }
            };
            record_cayenne_write_phase(
                self.table_metadata.table_name.as_str(),
                "keyset_rebuild",
                keyset_rebuild_start,
            );
            tracing::debug!(
                "prepare_stream_for_insert: loaded {} existing keys for table {}",
                existing_keys.len(),
                self.table_metadata.table_name
            );
            existing_keys
        };

        let on_conflict = self
            .table_metadata
            .on_conflict
            .clone()
            .unwrap_or(OnConflict::DoNothingAll);

        let may_have_on_conflict_deletions = matches!(on_conflict, OnConflict::Upsert(_));
        let post_validation = Arc::new(ParkingMutex::new(None));
        let validation_stream = OnConflictValidationStream::new(
            self.clone_for_write(),
            stream,
            pk_indices,
            converter,
            existing_keys,
            on_conflict,
            Arc::clone(&post_validation),
        );

        Ok(PreparedInsertStream::deferred(
            Box::pin(validation_stream) as SendableRecordBatchStream,
            post_validation,
            may_have_on_conflict_deletions,
        ))
    }

    pub(super) fn apply_on_conflict_to_batch(
        &self,
        batch: RecordBatch,
        ctx: &mut OnConflictContext<'_>,
    ) -> Result<BatchValidationResult> {
        use arrow::array::Int64Array;

        let pk_columns: Vec<_> = ctx
            .pk_indices
            .iter()
            .map(|idx| Arc::clone(batch.column(*idx)))
            .collect();

        let rows = ctx.converter.convert_columns(&pk_columns)?;

        // For Int64Pk strategy, get direct access to the PK column for value extraction
        let int64_pk_array: Option<&Int64Array> =
            if self.pk_deletion_strategy.is_int64_pk() && pk_columns.len() == 1 {
                pk_columns[0].as_any().downcast_ref::<Int64Array>()
            } else {
                None
            };

        let deduplicate_batch = !ctx.upsert_options.is_default();
        let mut keep_mask = Vec::with_capacity(batch.num_rows());
        let mut kept_keys: HashSet<OwnedRow> = if deduplicate_batch {
            HashSet::new()
        } else {
            HashSet::with_capacity(batch.num_rows())
        };
        let mut row_keys: Vec<OwnedRow> = if deduplicate_batch {
            Vec::with_capacity(batch.num_rows())
        } else {
            Vec::new()
        };
        let mut delete_specs: HashMap<Arc<str>, Vec<u64>> = HashMap::new();
        let mut deleted_pk_i64: Vec<i64> = Vec::new();
        let mut deleted_row_keys: Vec<Box<[u8]>> = Vec::new();
        let mut deleted_inlined_pk_i64: Vec<i64> = Vec::new();
        let mut deleted_inlined_row_keys: Vec<Box<[u8]>> = Vec::new();

        for row_idx in 0..batch.num_rows() {
            let has_null = pk_columns.iter().any(|col| col.is_null(row_idx));
            if has_null {
                return Err(Error::DataValidation {
                    table: self.table_metadata.table_name.clone(),
                    message: "Primary key values must be non-null".to_string(),
                });
            }

            let key = rows.row(row_idx).owned();
            if ctx.incoming_keys.contains(&key) {
                return Err(Error::DataValidation {
                    table: self.table_metadata.table_name.clone(),
                    message: "Incoming data contains duplicate primary key across batches"
                        .to_string(),
                });
            }

            let keep_row = match ctx.existing {
                PkExistenceRef::Exact(existing_keys) => {
                    if let Some(existing) = existing_keys.get(&key) {
                        match ctx.on_conflict {
                            OnConflict::DoNothingAll | OnConflict::DoNothing(_) => false,
                            OnConflict::Upsert(_) => {
                                let is_inlined_conflict = matches!(existing, RowLocation::Inlined);
                                match &self.pk_deletion_strategy {
                                    PkDeletionStrategyWithCache::Int64Pk { .. } => {
                                        if let Some(arr) = int64_pk_array {
                                            if is_inlined_conflict {
                                                deleted_inlined_pk_i64.push(arr.value(row_idx));
                                            } else {
                                                deleted_pk_i64.push(arr.value(row_idx));
                                            }
                                        }
                                    }
                                    PkDeletionStrategyWithCache::RowConverterBased { .. } => {
                                        // Convert the OwnedRow's byte view into a `Box<[u8]>` for the
                                        // delete-list — `deleted_row_keys` and `deleted_inlined_row_keys`
                                        // are typed `Vec<Box<[u8]>>` so they can be forwarded to the
                                        // `commit_on_conflict_deletions` catalog call without a second
                                        // re-encoding. This is one allocation per conflict row; the
                                        // arena-indexed key design discussed in iter 3 would amortize it.
                                        let row_key = key.as_ref().to_vec().into_boxed_slice();
                                        if is_inlined_conflict {
                                            deleted_inlined_row_keys.push(row_key);
                                        } else {
                                            deleted_row_keys.push(row_key);
                                        }
                                    }
                                    PkDeletionStrategyWithCache::PositionBased { .. } => {
                                        // Position-based doesn't need PK values
                                    }
                                }

                                // A located file row gets a per-file position
                                // delete (pushed into the Vortex scan). Unlocated /
                                // inlined rows are covered by the key-based lists
                                // populated above. Exactly one delete kind per
                                // conflict, so no double-masking.
                                if let RowLocation::FilePositioned {
                                    file_path,
                                    position,
                                } = existing
                                {
                                    delete_specs
                                        .entry(Arc::clone(file_path))
                                        .or_default()
                                        .push(*position);
                                }
                                true
                            }
                        }
                    } else {
                        true
                    }
                }
                PkExistenceRef::Bloom(bloom) => {
                    // Over-budget upsert table: existence is approximate. The bloom
                    // is only built for `OnConflict::Upsert` (a false positive is a
                    // harmless redundant delete for upsert, but would wrongly drop a
                    // new row under DoNothing), so the row is always kept here.
                    debug_assert!(
                        matches!(ctx.on_conflict, OnConflict::Upsert(_)),
                        "bloom existence index is only valid for upsert tables"
                    );
                    // A bloom hit (possibly a false positive) emits a key-based
                    // delete to BOTH the file and inline lists, so the prior version
                    // is masked wherever it lives. A false positive matches nothing
                    // and is a no-op. No `delete_specs` — we have no row location.
                    if bloom.maybe_contains(key.as_ref()) {
                        match &self.pk_deletion_strategy {
                            PkDeletionStrategyWithCache::Int64Pk { .. } => {
                                if let Some(arr) = int64_pk_array {
                                    let value = arr.value(row_idx);
                                    deleted_pk_i64.push(value);
                                    deleted_inlined_pk_i64.push(value);
                                }
                            }
                            PkDeletionStrategyWithCache::RowConverterBased { .. } => {
                                let row_key = key.as_ref().to_vec().into_boxed_slice();
                                deleted_row_keys.push(row_key.clone());
                                deleted_inlined_row_keys.push(row_key);
                            }
                            PkDeletionStrategyWithCache::PositionBased { .. } => {}
                        }
                    }
                    true
                }
            };

            if deduplicate_batch {
                row_keys.push(key);
            } else if keep_row {
                kept_keys.insert(key);
            }
            keep_mask.push(keep_row);
        }

        if deduplicate_batch {
            {
                let mut seen: HashMap<&[u8], usize> = HashMap::new();
                for (row_idx, key) in row_keys.iter().enumerate() {
                    if !keep_mask[row_idx] {
                        continue;
                    }

                    let key_bytes: &[u8] = key.as_ref();
                    if let Some(existing_idx) = seen.get(key_bytes) {
                        if ctx.upsert_options.last_write_wins {
                            keep_mask[*existing_idx] = false;
                            seen.insert(key_bytes, row_idx);
                        } else if ctx.upsert_options.remove_duplicates {
                            keep_mask[row_idx] = false;
                        } else {
                            return Err(Error::DataValidation {
                                table: self.table_metadata.table_name.clone(),
                                message: "Duplicate primary key found in batch".to_string(),
                            });
                        }
                    } else {
                        seen.insert(key_bytes, row_idx);
                    }
                }
            }

            kept_keys = row_keys
                .into_iter()
                .zip(&keep_mask)
                .filter(|(_, keep)| **keep)
                .map(|(key, _)| key)
                .collect();
        }

        let filtered_batch = Self::filter_validated_batch(batch, keep_mask)?;

        Ok(BatchValidationResult {
            filtered_batch,
            delete_specs: delete_specs.into_iter().collect(),
            kept_keys,
            deleted_pk_i64,
            deleted_row_keys,
            deleted_inlined_pk_i64,
            deleted_inlined_row_keys,
        })
    }

    pub(super) fn filter_validated_batch(
        batch: RecordBatch,
        keep_mask: Vec<bool>,
    ) -> Result<Option<RecordBatch>> {
        if keep_mask.iter().all(|v| !*v) {
            return Ok(None);
        }

        if keep_mask.iter().all(|v| *v) {
            return Ok(Some(batch));
        }

        let filter_array = arrow::array::BooleanArray::from(keep_mask);
        let filtered_batch = arrow::compute::filter_record_batch(&batch, &filter_array)?;

        Ok(Some(filtered_batch))
    }
}
