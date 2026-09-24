/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Construction and fenced publication of resident covering artifacts.
//!
//! # Writer-path inventory
//!
//! | Writer path | Existing data-publication boundary | Covering action in this step |
//! | --- | --- | --- |
//! | Full refresh (`overwrite.rs`) | `publish_overwrite_snapshot_fenced` | Capture the Vortex observer's final file order, prepare before the flip, then swap the immutable catalog in that flip. |
//! | CDC append / staged upsert (`mutation_writer.rs`, `staged_upsert.rs`) | Their fenced durable publication | No artifact is advertised yet; step 05 attaches complete source manifests and explicit uncovered sources before the optional scan path can select them. |
//! | Mem-tier checkpoint (`sink.rs`) | Checkpoint publish after file finalization | No artifact is advertised yet for the same reason. |
//! | Compaction / rewrite (`compaction_writer.rs`) | Snapshot/file-set replacement | No artifact is advertised yet; its new file identities must never inherit old row references. |
//!
//! The observer fires after Vortex has established its file-local physical row
//! positions. Incoming record-batch order is therefore never used as a source
//! ordinal: batches are reassembled by the reported `(file, first_row_position)`
//! and rejected when they leave a gap or overlap.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwapOption;
use arrow::alloc::Allocation;
use arrow::array::{Array, ArrayData, ArrayRef, RecordBatch, RecordBatchOptions, make_array};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer};
use parking_lot::Mutex;
use vortex_datafusion::VortexWriteObserver;

use super::super::lookup_index::{IndexedFile, KeySpec};
use super::super::memory_account::{CayenneMemoryAccount, LookupIndexReservation};
use super::{
    AllocationOwner, CoveredRowRef, CoveringIndexCatalog, EncodedKey, Error, IndexCatalog,
    IndexDefinition, IndexRun, IndexedSource, KeyDirectory, KeyDirectoryEntry, KeyPage, KeyPageId,
    KeyPageLease, MemoryPageStore, PageLease, PayloadPage, PayloadPageId, PayloadPageLease,
    ReservationToken, Result, RunId, SourceId,
};

/// Target retained Arrow bytes in one payload page.
const PAYLOAD_PAGE_TARGET_BYTES: usize = 64 * 1024;
/// Maximum number of source rows in one payload page.
const PAYLOAD_PAGE_MAX_ROWS: usize = 1024;
/// Target encoded bytes in one sorted key page.
const KEY_PAGE_TARGET_BYTES: usize = 32 * 1024;
/// Maximum entries in one sorted key page.
const KEY_PAGE_MAX_ENTRIES: usize = 256;
/// Maximum entries sorted in one independently bounded run.
const SORT_RUN_MAX_ENTRIES: usize = 65_536;

/// A complete, unpublished resident representation of one immutable source.
///
/// The store owns payload pages once, while every run owns only key navigation
/// pages and row references into those shared values. A caller publishes all
/// three components together or drops this value, never a partial source.
#[derive(Debug)]
pub(crate) struct BuiltCoveredSource {
    source: IndexedSource,
    runs: Arc<[IndexRun]>,
    page_store: Arc<MemoryPageStore>,
    non_null_key_count: usize,
}

impl BuiltCoveredSource {
    /// Source metadata proved against all constructed payload pages.
    #[must_use]
    pub(crate) fn source(&self) -> &IndexedSource {
        &self.source
    }

    /// Complete sorted runs, including an explicit empty run where necessary.
    #[must_use]
    pub(crate) fn runs(&self) -> &[IndexRun] {
        &self.runs
    }

    /// Store owning every referenced key and payload page.
    #[must_use]
    pub(crate) fn page_store(&self) -> &Arc<MemoryPageStore> {
        &self.page_store
    }

    /// Exact number of physical rows with a non-NULL full key for this
    /// definition. Finalization compares it to the run pages before staging.
    #[must_use]
    pub(crate) const fn non_null_key_count(&self) -> usize {
        self.non_null_key_count
    }

    /// Split this unpublished source into the atomically publishable pieces.
    #[must_use]
    pub(crate) fn into_parts(self) -> (IndexedSource, Arc<[IndexRun]>, Arc<MemoryPageStore>) {
        (self.source, self.runs, self.page_store)
    }
}

/// Immutable covering catalogs staged by a full-refresh write.
///
/// `None` is intentional: an optional build refusal must replace an older
/// catalog with explicit uncovered state rather than leave old physical row
/// references live for replacement data.
#[derive(Debug)]
struct StagedCatalog {
    snapshot_id: String,
    catalog: Option<Arc<CoveringIndexCatalog>>,
}

/// Table-local state for optional file-backed covering artifacts.
///
/// Page construction and all executor awaits happen after the pending builder
/// is removed from its mutex. The publication lock protects only immutable
/// pointer swaps, inside the table's existing data-visibility fence.
pub(crate) struct CoveringIndexState {
    table_id: Arc<str>,
    definitions: Arc<[IndexDefinition]>,
    account: Arc<CayenneMemoryAccount>,
    catalog: ArcSwapOption<CoveringIndexCatalog>,
    generation: AtomicU64,
    publish_lock: Mutex<()>,
    pending: Mutex<Option<Arc<IncrementalCoveringIndexBuilder>>>,
    staged: Mutex<Option<StagedCatalog>>,
    rejection: Mutex<Option<Error>>,
    scan_input_version: Arc<AtomicU64>,
}

impl std::fmt::Debug for CoveringIndexState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CoveringIndexState")
            .field("table_id", &self.table_id)
            .field("definitions", &self.definitions.len())
            .field("generation", &self.generation.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl CoveringIndexState {
    /// Resolve existing address-index declarations into covering definitions.
    /// An optional-resolution failure does not alter the smaller address index.
    pub(crate) fn new(
        table_id: impl Into<Arc<str>>,
        schema: arrow_schema::SchemaRef,
        specs: &[KeySpec],
        account: Arc<CayenneMemoryAccount>,
        scan_input_version: Arc<AtomicU64>,
    ) -> Option<Arc<Self>> {
        if specs.is_empty() {
            return None;
        }
        let definitions = specs
            .iter()
            .map(|spec| IndexDefinition::resolve(Arc::clone(&schema), spec))
            .collect::<Result<Vec<_>>>()
            .ok()?;
        Some(Arc::new(Self {
            table_id: table_id.into(),
            definitions: definitions.into(),
            account,
            catalog: ArcSwapOption::empty(),
            generation: AtomicU64::new(0),
            publish_lock: Mutex::new(()),
            pending: Mutex::new(None),
            staged: Mutex::new(None),
            rejection: Mutex::new(None),
            scan_input_version,
        }))
    }

    /// The immutable catalog captured under the table listing fence.
    #[must_use]
    pub(crate) fn published(&self) -> Option<Arc<CoveringIndexCatalog>> {
        self.catalog.load_full()
    }

    /// Start a full-refresh artifact that captures final Vortex row positions.
    pub(crate) fn begin_incremental_build(
        self: &Arc<Self>,
        snapshot_id: &str,
    ) -> Arc<IncrementalCoveringIndexBuilder> {
        let builder = Arc::new(IncrementalCoveringIndexBuilder::new(
            Arc::clone(&self.table_id),
            snapshot_id.to_string(),
            Arc::clone(&self.account),
        ));
        *self.pending.lock() = Some(Arc::clone(&builder));
        builder
    }

    /// Build off publication locks and stage a whole replacement catalog, or an
    /// explicit uncovered replacement when optional construction is refused.
    pub(crate) async fn stage_pending(&self, snapshot_id: &str, files: &[IndexedFile]) {
        let pending = {
            let mut pending = self.pending.lock();
            if pending
                .as_ref()
                .is_some_and(|builder| builder.snapshot_id() == snapshot_id)
            {
                pending.take()
            } else {
                None
            }
        };
        let Some(builder) = pending else {
            // A later refresh owns the pending slot. Its staged replacement is
            // newer than this completion and must remain untouched.
            return;
        };
        let catalog = match builder
            .finish(
                Arc::clone(&self.definitions),
                Arc::clone(&self.account),
                files,
            )
            .await
        {
            Ok(catalog) => {
                *self.rejection.lock() = None;
                Some(Arc::new(catalog))
            }
            Err(error) => {
                *self.rejection.lock() = Some(error);
                Some(Arc::new(CoveringIndexCatalog::uncovered(
                    snapshot_id,
                    self.source_manifest(snapshot_id, files),
                )))
            }
        };
        *self.staged.lock() = Some(StagedCatalog {
            snapshot_id: snapshot_id.to_string(),
            catalog,
        });
    }

    /// Stage explicit uncovered state when the final manifest cannot be read.
    /// The data writer still commits; the optional catalog simply cannot make a
    /// completeness claim for an unknown source set.
    pub(crate) fn stage_uncovered(&self, snapshot_id: &str) {
        *self.pending.lock() = None;
        *self.rejection.lock() = Some(Error::Unavailable {
            operation: "final covering source manifest could not be listed".to_string(),
        });
        *self.staged.lock() = Some(StagedCatalog {
            snapshot_id: snapshot_id.to_string(),
            catalog: Some(Arc::new(CoveringIndexCatalog::uncovered(snapshot_id, []))),
        });
    }

    /// Swap the prepared immutable catalog inside the existing data flip.
    /// Stale or missing staged work publishes uncovered state and therefore
    /// cannot resurrect a catalog after a newer refresh.
    pub(crate) fn promote_staged(&self, snapshot_id: &str) {
        let staged = self.staged.lock().take();
        let catalog = staged.and_then(|staged| {
            (staged.snapshot_id == snapshot_id)
                .then_some(staged.catalog)
                .flatten()
        });
        let _publishing = self.publish_lock.lock();
        self.catalog.store(catalog);
        self.generation.fetch_add(1, Ordering::Release);
        self.scan_input_version.fetch_add(1, Ordering::Release);
    }

    /// Drop all unpublished capture/pages after an aborted writer.
    pub(crate) fn discard_pending(&self) {
        *self.pending.lock() = None;
        *self.staged.lock() = None;
        *self.rejection.lock() = None;
    }

    fn source_manifest(&self, snapshot_id: &str, files: &[IndexedFile]) -> Vec<SourceId> {
        files
            .iter()
            .map(|file| {
                SourceId::file(
                    Arc::clone(&self.table_id),
                    snapshot_id.to_string(),
                    file.path.clone(),
                    file.size,
                    file.last_modified_ms,
                )
            })
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn rejection(&self) -> Option<String> {
        self.rejection.lock().as_ref().map(ToString::to_string)
    }
}

/// Captures Vortex write callbacks by final file-local positions.
pub(crate) struct IncrementalCoveringIndexBuilder {
    table_id: Arc<str>,
    snapshot_id: String,
    account: Arc<CayenneMemoryAccount>,
    files: Mutex<BTreeMap<String, BTreeMap<u64, PendingCapturedBatch>>>,
    failure: Mutex<Option<String>>,
}

impl std::fmt::Debug for IncrementalCoveringIndexBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IncrementalCoveringIndexBuilder")
            .field("table_id", &self.table_id)
            .field("snapshot_id", &self.snapshot_id)
            .finish_non_exhaustive()
    }
}

/// A writer batch retained until page construction transfers its values into
/// independently owned, accounted payload buffers.
#[derive(Debug)]
struct PendingCapturedBatch {
    batch: RecordBatch,
    _reservation: LookupIndexReservation,
}

impl IncrementalCoveringIndexBuilder {
    fn new(table_id: Arc<str>, snapshot_id: String, account: Arc<CayenneMemoryAccount>) -> Self {
        Self {
            table_id,
            snapshot_id,
            account,
            files: Mutex::new(BTreeMap::new()),
            failure: Mutex::new(None),
        }
    }

    #[must_use]
    pub(crate) fn snapshot_id(&self) -> &str {
        &self.snapshot_id
    }

    fn record_failure(&self, message: String) {
        let mut failure = self.failure.lock();
        if failure.is_none() {
            *failure = Some(message);
        }
    }

    async fn finish(
        &self,
        definitions: Arc<[IndexDefinition]>,
        account: Arc<CayenneMemoryAccount>,
        files: &[IndexedFile],
    ) -> Result<CoveringIndexCatalog> {
        if let Some(failure) = self.failure.lock().clone() {
            return Err(Error::InvalidContract { message: failure });
        }
        let mut captured = std::mem::take(&mut *self.files.lock());
        let listed = files
            .iter()
            .map(|file| file.path.as_str())
            .collect::<HashSet<_>>();
        let observed = captured.keys().map(String::as_str).collect::<HashSet<_>>();
        if listed != observed {
            return Err(Error::InvalidContract {
                message: format!(
                    "covering write capture and final file manifest differ ({} observed, {} listed)",
                    observed.len(),
                    listed.len()
                ),
            });
        }

        let mut by_definition = (0..definitions.len())
            .map(|_| Vec::with_capacity(files.len()))
            .collect::<Vec<_>>();
        for file in files {
            let batches = captured
                .remove(&file.path)
                .ok_or_else(|| Error::InvalidContract {
                    message: format!(
                        "final file {} was not captured by the write observer",
                        file.path
                    ),
                })?;
            let (batches, physical_rows) = ordered_physical_batches(&file.path, batches)?;
            // Keep the capture reservations alive through this await. The source
            // builder first admits independently owned page buffers, then these
            // temporary input pins can drop without an accounting gap.
            let source_batches = batches
                .iter()
                .map(|captured| captured.batch.clone())
                .collect::<Vec<_>>();
            let source = SourceId::file(
                Arc::clone(&self.table_id),
                self.snapshot_id.clone(),
                file.path.clone(),
                file.size,
                file.last_modified_ms,
            );
            let built = build_sources(
                source,
                definitions.to_vec(),
                source_batches,
                Arc::clone(&account),
            )
            .await?;
            drop(batches);
            if built.len() != definitions.len() {
                return Err(Error::InvalidContract {
                    message: "covering source builder returned a different definition count"
                        .to_string(),
                });
            }
            for (definition_index, source) in built.into_iter().enumerate() {
                validate_finalized_source(&source, physical_rows)?;
                by_definition[definition_index].push(source);
            }
        }
        let catalogs = definitions
            .iter()
            .cloned()
            .zip(by_definition)
            .map(|(definition, sources)| {
                IndexCatalog::from_built_sources(definition, sources).map(Arc::new)
            })
            .collect::<Result<Vec<_>>>()?;
        CoveringIndexCatalog::new(self.snapshot_id.clone(), catalogs)
    }
}

impl VortexWriteObserver for IncrementalCoveringIndexBuilder {
    fn batch_written(
        &self,
        file_path: &object_store::path::Path,
        first_row_position: u64,
        batch: &RecordBatch,
    ) {
        if batch.num_rows() == 0 {
            return;
        }
        if self.failure.lock().is_some() {
            return;
        }
        let bytes = match retained_buffer_bytes(batch) {
            Ok(bytes) => bytes,
            Err(error) => {
                self.record_failure(format!("unable to account write capture: {error}"));
                return;
            }
        };
        let Some(reservation) = self.account.try_reserve_lookup_index(bytes) else {
            self.record_failure(format!(
                "unable to admit {bytes} bytes for covering write capture"
            ));
            return;
        };
        let path: &str = file_path.as_ref();
        let mut files = self.files.lock();
        let batches = files.entry(path.to_string()).or_default();
        if batches
            .insert(
                first_row_position,
                PendingCapturedBatch {
                    batch: batch.clone(),
                    _reservation: reservation,
                },
            )
            .is_some()
        {
            drop(files);
            self.record_failure(format!(
                "file {path} reported more than one covering batch at physical row {first_row_position}"
            ));
        }
    }
}

/// Reassemble source batches in their final Vortex physical order.
fn ordered_physical_batches(
    path: &str,
    batches: BTreeMap<u64, PendingCapturedBatch>,
) -> Result<(Vec<PendingCapturedBatch>, usize)> {
    let mut expected_position = 0u64;
    let mut total_rows = 0usize;
    let mut ordered = Vec::with_capacity(batches.len());
    for (first_position, batch) in batches {
        if first_position != expected_position {
            return Err(Error::InvalidContract {
                message: format!(
                    "file {path} has non-contiguous covering capture: expected row {expected_position}, got {first_position}"
                ),
            });
        }
        let rows = u64::try_from(batch.batch.num_rows()).map_err(|_| Error::Overflow {
            operation: "covering capture batch row count",
        })?;
        expected_position = expected_position.checked_add(rows).ok_or(Error::Overflow {
            operation: "covering capture physical row position",
        })?;
        total_rows = total_rows
            .checked_add(batch.batch.num_rows())
            .ok_or(Error::Overflow {
                operation: "covering capture source row count",
            })?;
        ordered.push(batch);
    }
    Ok((ordered, total_rows))
}

/// Compare the observer's physical count with the pages and exact per-key run
/// entry count before an artifact crosses the staging boundary.
fn validate_finalized_source(source: &BuiltCoveredSource, physical_rows: usize) -> Result<()> {
    if source.source().row_count() != physical_rows {
        return Err(Error::InvalidContract {
            message: format!(
                "source payload row count {} differs from observed physical rows {physical_rows}",
                source.source().row_count()
            ),
        });
    }
    let run_entries = source.runs().iter().try_fold(0usize, |count, run| {
        run.directory()
            .entries()
            .iter()
            .try_fold(count, |count, page| {
                count
                    .checked_add(page.entry_count())
                    .ok_or(Error::Overflow {
                        operation: "finalized per-key non-NULL count",
                    })
            })
    })?;
    if run_entries != source.non_null_key_count() {
        return Err(Error::InvalidContract {
            message: format!(
                "source key-run entries {run_entries} differ from encoded non-NULL keys {}",
                source.non_null_key_count()
            ),
        });
    }
    Ok(())
}

#[derive(Debug)]
struct BuildEntry {
    key: EncodedKey,
    row_ref: CoveredRowRef,
}

/// Build payload pages once and all sorted key pages for one index definition.
///
/// Construction runs on the bounded Rayon pool. It is optional: any admission,
/// Arrow ownership, or schema failure drops every unpublished allocation and
/// returns a typed error for the caller to turn into unavailable coverage.
pub(crate) async fn build_source(
    source: SourceId,
    definition: IndexDefinition,
    batches: Vec<RecordBatch>,
    account: Arc<CayenneMemoryAccount>,
) -> Result<BuiltCoveredSource> {
    super::CayenneIndexExecutor::shared()?
        .execute(move || build_source_sync(source, &definition, batches, &account))
        .await
}

/// Build several index definitions over one source without copying payloads.
///
/// Each returned store has independent key pages and directories but clones the
/// same immutable payload leases, so Arrow allocation charges remain owned once
/// by their buffer-backed allocation owners.
pub(crate) async fn build_sources(
    source: SourceId,
    definitions: Vec<IndexDefinition>,
    batches: Vec<RecordBatch>,
    account: Arc<CayenneMemoryAccount>,
) -> Result<Vec<BuiltCoveredSource>> {
    let (first, rest) = definitions
        .split_first()
        .ok_or_else(|| Error::InvalidContract {
            message: "covering source build requires at least one index definition".to_string(),
        })?;
    let first_built = build_source(source, first.clone(), batches, Arc::clone(&account)).await?;
    let mut built = Vec::with_capacity(definitions.len());
    for definition in rest {
        if !definition.schema().matches(first_built.source().schema()) {
            return Err(Error::InvalidContract {
                message: "covering indexes over one source require the exact same schema"
                    .to_string(),
            });
        }
        let source = first_built.source().clone();
        let payload_leases = first_built.page_store().payload_page_leases();
        let definition = definition.clone();
        let account = Arc::clone(&account);
        let additional = super::CayenneIndexExecutor::shared()?
            .execute(move || {
                build_index_over_shared_payload(source, &definition, payload_leases, &account)
            })
            .await?;
        built.push(additional);
    }
    built.insert(0, first_built);
    Ok(built)
}

fn build_source_sync(
    source: SourceId,
    definition: &IndexDefinition,
    batches: Vec<RecordBatch>,
    account: &Arc<CayenneMemoryAccount>,
) -> Result<BuiltCoveredSource> {
    let schema = definition.schema().clone();
    let mut payload_pages = Vec::new();
    let mut payload_leases = BTreeMap::new();
    let mut entries = Vec::new();
    let mut row_count = 0usize;

    for batch in batches {
        if batch.schema_ref().as_ref() != schema.schema().as_ref() {
            return Err(Error::InvalidContract {
                message: "source batch schema differs from its exact captured schema".to_string(),
            });
        }

        let mut start = 0usize;
        while start < batch.num_rows() {
            let remaining = batch.num_rows().checked_sub(start).ok_or(Error::Overflow {
                operation: "payload-page remaining rows",
            })?;
            let (owned, consumed) = build_payload_batch(&batch, start, remaining, account)?;
            let page_number = u32::try_from(payload_pages.len()).map_err(|_| Error::Overflow {
                operation: "payload page identifier",
            })?;
            let page_id = PayloadPageId::new(source.clone(), page_number);
            let encoded = definition.encode_source_batch(&owned)?;
            if encoded.len() != owned.num_rows() {
                return Err(Error::InvalidContract {
                    message: "source key encoder returned a mismatched row count".to_string(),
                });
            }

            for (row_in_page, key) in encoded.into_iter().enumerate() {
                let ordinal = u64::try_from(row_count).map_err(|_| Error::Overflow {
                    operation: "source row ordinal",
                })?;
                row_count = row_count.checked_add(1).ok_or(Error::Overflow {
                    operation: "source row count",
                })?;
                if let Some(key) = key {
                    entries.push(BuildEntry {
                        key,
                        row_ref: CoveredRowRef::new(
                            source.clone(),
                            page_id.clone(),
                            row_in_page,
                            ordinal,
                        )?,
                    });
                }
            }

            let page = Arc::new(PayloadPage::new(schema.clone(), owned)?);
            let page_token = reserve_token(
                account,
                std::mem::size_of::<PayloadPage>()
                    .checked_add(std::mem::size_of::<PayloadPageId>())
                    .ok_or(Error::Overflow {
                        operation: "payload-page bookkeeping bytes",
                    })?,
                "payload-page bookkeeping",
            )?;
            payload_leases.insert(page_id.clone(), PageLease::new(page, page_token));
            payload_pages.push(page_id);
            start = start.checked_add(consumed).ok_or(Error::Overflow {
                operation: "payload-page source offset",
            })?;
        }
    }

    let non_null_key_count = entries.len();
    let (key_leases, runs) = build_key_runs(&source, entries, account)?;
    let indexed_source = IndexedSource::new(source, schema, row_count, payload_pages)?;
    let store = Arc::new(MemoryPageStore::new(key_leases, payload_leases)?);
    validate_complete_source(&indexed_source, &runs, &store, non_null_key_count)?;
    Ok(BuiltCoveredSource {
        source: indexed_source,
        runs: runs.into(),
        page_store: store,
        non_null_key_count,
    })
}

fn build_index_over_shared_payload(
    source: IndexedSource,
    definition: &IndexDefinition,
    payload_leases: BTreeMap<PayloadPageId, PayloadPageLease>,
    account: &Arc<CayenneMemoryAccount>,
) -> Result<BuiltCoveredSource> {
    if !source.schema().matches(definition.schema()) {
        return Err(Error::InvalidContract {
            message: "shared payload schema differs from the index definition".to_string(),
        });
    }
    let mut entries = Vec::new();
    let mut expected_ordinal = 0usize;
    for page_id in source.payload_pages() {
        let lease = payload_leases
            .get(page_id)
            .ok_or_else(|| Error::InvalidContract {
                message: format!("shared payload page {page_id:?} is absent from its store"),
            })?;
        let batch = lease.page().batch();
        let keys = definition.encode_source_batch(batch)?;
        for (row_in_page, key) in keys.into_iter().enumerate() {
            let ordinal = u64::try_from(expected_ordinal).map_err(|_| Error::Overflow {
                operation: "shared payload source row ordinal",
            })?;
            expected_ordinal = expected_ordinal.checked_add(1).ok_or(Error::Overflow {
                operation: "shared payload source row count",
            })?;
            if let Some(key) = key {
                entries.push(BuildEntry {
                    key,
                    row_ref: CoveredRowRef::new(
                        source.source().clone(),
                        page_id.clone(),
                        row_in_page,
                        ordinal,
                    )?,
                });
            }
        }
    }
    if expected_ordinal != source.row_count() {
        return Err(Error::InvalidContract {
            message: "shared payload row count differs from its source metadata".to_string(),
        });
    }
    let non_null_key_count = entries.len();
    let (key_leases, runs) = build_key_runs(source.source(), entries, account)?;
    let store = Arc::new(MemoryPageStore::new(key_leases, payload_leases)?);
    validate_complete_source(&source, &runs, &store, non_null_key_count)?;
    Ok(BuiltCoveredSource {
        source,
        runs: runs.into(),
        page_store: store,
        non_null_key_count,
    })
}

/// Compact and independently own the largest page that fits the row/byte limit.
fn build_payload_batch(
    batch: &RecordBatch,
    start: usize,
    remaining: usize,
    account: &Arc<CayenneMemoryAccount>,
) -> Result<(RecordBatch, usize)> {
    let mut rows = remaining.min(PAYLOAD_PAGE_MAX_ROWS);
    loop {
        let slice = batch.slice(start, rows);
        // The temporary reservation is held over the copy and final ownership
        // transfer. This is deliberately conservative: a sliced parent can be
        // much larger than the values retained by its child page.
        let scratch = reserve_token(
            account,
            retained_buffer_bytes(&slice)?,
            "payload-page compaction scratch",
        )?;
        let compacted = arrow_tools::record_batch::compact_retained_buffers(&slice);
        if arrow_tools::record_batch::rests_on_unowned_memory(&compacted) {
            return Err(Error::Unavailable {
                operation: "payload page still retains unowned Arrow memory after compaction"
                    .to_string(),
            });
        }
        let bytes = retained_buffer_bytes(&compacted)?;
        if bytes <= PAYLOAD_PAGE_TARGET_BYTES || rows == 1 {
            let owned = own_batch_buffers(&compacted, account)?;
            drop(scratch);
            return Ok((owned, rows));
        }
        drop(scratch);
        rows /= 2;
    }
}

fn build_key_runs(
    source: &SourceId,
    mut entries: Vec<BuildEntry>,
    account: &Arc<CayenneMemoryAccount>,
) -> Result<(BTreeMap<KeyPageId, KeyPageLease>, Vec<IndexRun>)> {
    let mut pages = BTreeMap::new();
    let mut runs = Vec::new();
    let mut next_page = 0u32;

    if entries.is_empty() {
        let directory = KeyDirectory::new(source.clone(), Vec::new())?;
        let token = reserve_token(
            account,
            std::mem::size_of::<KeyDirectory>(),
            "empty key directory",
        )?;
        runs.push(IndexRun::new(source.clone(), RunId::new(0), directory)?.with_metadata(0, token));
        return Ok((pages, runs));
    }

    for (run_index, unsorted) in entries.chunks_mut(SORT_RUN_MAX_ENTRIES).enumerate() {
        unsorted.sort_unstable_by(|left, right| {
            left.key.cmp(&right.key).then_with(|| {
                left.row_ref
                    .source_row_ordinal()
                    .cmp(&right.row_ref.source_row_ordinal())
            })
        });
        let max_duplicates = max_duplicate_key_count(unsorted)?;
        let mut directory_entries = Vec::new();
        let mut page_start = 0usize;
        while page_start < unsorted.len() {
            let mut page_end = page_start;
            let mut key_bytes = 0usize;
            while page_end < unsorted.len() && page_end - page_start < KEY_PAGE_MAX_ENTRIES {
                let next_len = unsorted[page_end].key.as_bytes().len();
                let next_total = key_bytes.checked_add(next_len).ok_or(Error::Overflow {
                    operation: "key-page encoded byte count",
                })?;
                if page_end > page_start && next_total > KEY_PAGE_TARGET_BYTES {
                    break;
                }
                key_bytes = next_total;
                page_end = page_end.checked_add(1).ok_or(Error::Overflow {
                    operation: "key-page entry count",
                })?;
            }
            let page_id = KeyPageId::new(source.clone(), next_page);
            next_page = next_page.checked_add(1).ok_or(Error::Overflow {
                operation: "key page identifier",
            })?;
            let page = build_key_page(&unsorted[page_start..page_end])?;
            let first = EncodedKey::from_page_bytes(page.key(0)?);
            let last_index = page.len().checked_sub(1).ok_or(Error::InvalidContract {
                message: "constructed key page is empty".to_string(),
            })?;
            let last = EncodedKey::from_page_bytes(page.key(last_index)?);
            let entry_count = page.len();
            let token = reserve_token(account, page.retained_bytes()?, "key-page bytes")?;
            pages.insert(page_id.clone(), PageLease::new(Arc::new(page), token));
            directory_entries.push(KeyDirectoryEntry::new(page_id, first, last, entry_count)?);
            page_start = page_end;
        }

        let directory_bytes = directory_retained_bytes(&directory_entries)?;
        let directory = KeyDirectory::new(source.clone(), directory_entries)?;
        let token = reserve_token(account, directory_bytes, "key-directory bytes")?;
        let run = u32::try_from(run_index).map_err(|_| Error::Overflow {
            operation: "sorted run identifier",
        })?;
        runs.push(
            IndexRun::new(source.clone(), RunId::new(run), directory)?
                .with_metadata(max_duplicates, token),
        );
    }
    Ok((pages, runs))
}

fn build_key_page(entries: &[BuildEntry]) -> Result<KeyPage> {
    if entries.is_empty() {
        return Err(Error::InvalidContract {
            message: "cannot construct an empty key page".to_string(),
        });
    }
    let key_bytes = entries.iter().try_fold(0usize, |total, entry| {
        total
            .checked_add(entry.key.as_bytes().len())
            .ok_or(Error::Overflow {
                operation: "key-page encoded allocation",
            })
    })?;
    let mut encoded = Vec::with_capacity(key_bytes);
    let mut offsets = Vec::with_capacity(entries.len().checked_add(1).ok_or(Error::Overflow {
        operation: "key-page offset allocation",
    })?);
    let mut row_refs = Vec::with_capacity(entries.len());
    offsets.push(0);
    for entry in entries {
        encoded.extend_from_slice(entry.key.as_bytes());
        offsets.push(encoded.len());
        row_refs.push(entry.row_ref.clone());
    }
    KeyPage::new(encoded.into(), offsets, row_refs)
}

fn max_duplicate_key_count(entries: &[BuildEntry]) -> Result<usize> {
    let mut maximum = 0usize;
    let mut current = 0usize;
    let mut prior: Option<&EncodedKey> = None;
    for entry in entries {
        if prior == Some(&entry.key) {
            current = current.checked_add(1).ok_or(Error::Overflow {
                operation: "duplicate-key multiplicity",
            })?;
        } else {
            current = 1;
            prior = Some(&entry.key);
        }
        maximum = maximum.max(current);
    }
    Ok(maximum)
}

fn directory_retained_bytes(entries: &[KeyDirectoryEntry]) -> Result<usize> {
    let entry_bytes = entries
        .len()
        .checked_mul(std::mem::size_of::<KeyDirectoryEntry>())
        .ok_or(Error::Overflow {
            operation: "key-directory entry bytes",
        })?;
    entries.iter().try_fold(entry_bytes, |total, entry| {
        total
            .checked_add(entry.first_key().as_bytes().len())
            .and_then(|bytes| bytes.checked_add(entry.last_key().as_bytes().len()))
            .ok_or(Error::Overflow {
                operation: "key-directory bound bytes",
            })
    })
}

fn validate_complete_source(
    source: &IndexedSource,
    runs: &[IndexRun],
    store: &MemoryPageStore,
    expected_non_null_key_count: usize,
) -> Result<()> {
    let mut payload_rows = 0usize;
    let payload_leases = store.payload_page_leases();
    for page_id in source.payload_pages() {
        let page = payload_leases
            .get(page_id)
            .ok_or_else(|| Error::InvalidContract {
                message: format!("constructed source is missing payload page {page_id:?}"),
            })?;
        payload_rows = payload_rows
            .checked_add(page.page().batch().num_rows())
            .ok_or(Error::Overflow {
                operation: "constructed payload row count",
            })?;
    }
    if payload_rows != source.row_count() {
        return Err(Error::InvalidContract {
            message: format!(
                "constructed payload rows {payload_rows} differ from source rows {}",
                source.row_count()
            ),
        });
    }
    let mut total_entries = 0usize;
    for run in runs {
        if run.source() != source.source() {
            return Err(Error::InvalidContract {
                message: "constructed run belongs to a different source".to_string(),
            });
        }
        for directory in run.directory().entries() {
            if !store.contains_key_page(directory.page()) {
                return Err(Error::InvalidContract {
                    message: format!(
                        "constructed directory references missing page {:?}",
                        directory.page()
                    ),
                });
            }
            total_entries =
                total_entries
                    .checked_add(directory.entry_count())
                    .ok_or(Error::Overflow {
                        operation: "constructed key entry count",
                    })?;
        }
    }
    if total_entries != expected_non_null_key_count {
        return Err(Error::InvalidContract {
            message: format!(
                "constructed key entries {total_entries} differ from expected non-NULL keys {expected_non_null_key_count}"
            ),
        });
    }
    if source.row_count() > 0 && runs.is_empty() {
        return Err(Error::InvalidContract {
            message: "nonempty source has no complete key run".to_string(),
        });
    }
    Ok(())
}

fn reserve_token(
    account: &Arc<CayenneMemoryAccount>,
    bytes: usize,
    operation: &str,
) -> Result<ReservationToken> {
    let reservation =
        account
            .try_reserve_lookup_index(bytes)
            .ok_or_else(|| Error::Unavailable {
                operation: format!("unable to admit {bytes} bytes for {operation}"),
            })?;
    Ok(AllocationOwner::new(reservation).token())
}

/// Allocation owner for one immutable Arrow buffer.
///
/// Field order releases the retained backing buffer before its reservation.
/// It carries no page reference, so result arrays retaining this allocation
/// cannot create a page/lease ownership cycle.
#[derive(Debug)]
struct BufferAllocation {
    _backing: Buffer,
    _reservation: LookupIndexReservation,
}

impl std::panic::RefUnwindSafe for BufferAllocation {}

fn own_batch_buffers(
    batch: &RecordBatch,
    account: &Arc<CayenneMemoryAccount>,
) -> Result<RecordBatch> {
    let mut allocations = HashMap::new();
    let columns = batch
        .columns()
        .iter()
        .map(|array| own_array_buffers(array, account, &mut allocations))
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new_with_options(
        batch.schema(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(|source| Error::Arrow { source })
}

fn own_array_buffers(
    array: &ArrayRef,
    account: &Arc<CayenneMemoryAccount>,
    allocations: &mut HashMap<(usize, usize), Buffer>,
) -> Result<ArrayRef> {
    Ok(make_array(own_array_data(
        &array.to_data(),
        account,
        allocations,
    )?))
}

fn own_array_data(
    data: &ArrayData,
    account: &Arc<CayenneMemoryAccount>,
    allocations: &mut HashMap<(usize, usize), Buffer>,
) -> Result<ArrayData> {
    let buffers = data
        .buffers()
        .iter()
        .map(|buffer| own_buffer(buffer, account, allocations))
        .collect::<Result<Vec<_>>>()?;
    let children = data
        .child_data()
        .iter()
        .map(|child| own_array_data(child, account, allocations))
        .collect::<Result<Vec<_>>>()?;
    let nulls = data
        .nulls()
        .map(|nulls| {
            let buffer = own_buffer(nulls.buffer(), account, allocations)?;
            Ok::<NullBuffer, Error>(NullBuffer::new(BooleanBuffer::new(
                buffer,
                nulls.inner().offset(),
                nulls.len(),
            )))
        })
        .transpose()?;
    ArrayData::builder(data.data_type().clone())
        .len(data.len())
        .offset(data.offset())
        .buffers(buffers)
        .child_data(children)
        .nulls(nulls)
        .build()
        .map_err(|source| Error::Arrow { source })
}

fn own_buffer(
    buffer: &Buffer,
    account: &Arc<CayenneMemoryAccount>,
    allocations: &mut HashMap<(usize, usize), Buffer>,
) -> Result<Buffer> {
    let capacity = buffer.capacity();
    if capacity == 0 {
        if buffer.is_empty() {
            return Ok(buffer.clone());
        }
        return Err(Error::Unavailable {
            operation: "Arrow buffer has no accountable owned capacity".to_string(),
        });
    }
    let offset = buffer.ptr_offset();
    let end = offset.checked_add(buffer.len()).ok_or(Error::Overflow {
        operation: "Arrow buffer slice bounds",
    })?;
    if end > capacity {
        return Err(Error::InvalidContract {
            message: "Arrow buffer slice exceeds its backing allocation".to_string(),
        });
    }
    let key = (buffer.data_ptr().as_ptr() as usize, capacity);
    let root = if let Some(existing) = allocations.get(&key) {
        existing.clone()
    } else {
        let reservation =
            account
                .try_reserve_lookup_index(capacity)
                .ok_or_else(|| Error::Unavailable {
                    operation: format!(
                        "unable to admit {capacity} bytes for an owned Arrow payload buffer"
                    ),
                })?;
        let allocation: Arc<dyn Allocation> = Arc::new(BufferAllocation {
            _backing: buffer.clone(),
            _reservation: reservation,
        });
        // SAFETY: `BufferAllocation` owns an immutable clone of the original
        // backing allocation. `data_ptr` and `capacity` describe that exact
        // allocation, and all returned slices stay within its checked bounds.
        let wrapped =
            unsafe { Buffer::from_custom_allocation(buffer.data_ptr(), capacity, allocation) };
        allocations.insert(key, wrapped.clone());
        wrapped
    };
    Ok(root.slice_with_length(offset, buffer.len()))
}

fn retained_buffer_bytes(batch: &RecordBatch) -> Result<usize> {
    let mut buffers = HashSet::new();
    let mut total = 0usize;
    for column in batch.columns() {
        collect_buffer_bytes(&column.to_data(), &mut buffers, &mut total)?;
    }
    Ok(total)
}

fn collect_buffer_bytes(
    data: &ArrayData,
    seen: &mut HashSet<(usize, usize)>,
    total: &mut usize,
) -> Result<()> {
    for buffer in data.buffers() {
        account_buffer_bytes(buffer, seen, total)?;
    }
    if let Some(nulls) = data.nulls() {
        account_buffer_bytes(nulls.buffer(), seen, total)?;
    }
    for child in data.child_data() {
        collect_buffer_bytes(child, seen, total)?;
    }
    Ok(())
}

fn account_buffer_bytes(
    buffer: &Buffer,
    seen: &mut HashSet<(usize, usize)>,
    total: &mut usize,
) -> Result<()> {
    let capacity = buffer.capacity();
    if capacity == 0 && !buffer.is_empty() {
        return Err(Error::Unavailable {
            operation: "Arrow buffer has no accountable owned capacity".to_string(),
        });
    }
    if seen.insert((buffer.data_ptr().as_ptr() as usize, capacity)) {
        *total = total.checked_add(capacity).ok_or(Error::Overflow {
            operation: "retained Arrow buffer bytes",
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod publication_tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};

    use super::*;

    fn schema() -> arrow_schema::SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, true),
            Field::new("value", DataType::Utf8, false),
        ]))
    }

    fn batch(keys: Vec<Option<i64>>, values: Vec<&str>) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(StringArray::from(values)),
            ],
        )
        .expect("publication fixture batch")
    }

    fn state(bytes: usize) -> Arc<CoveringIndexState> {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes));
        let account = Arc::new(CayenneMemoryAccount::new("covering-publication", &pool));
        let specs = vec![KeySpec::new(vec!["key".to_string()]).expect("nonempty key")];
        CoveringIndexState::new(
            "table",
            schema(),
            &specs,
            account,
            Arc::new(AtomicU64::new(0)),
        )
        .expect("covering state")
    }

    fn file(path: &str) -> IndexedFile {
        IndexedFile {
            path: path.to_string(),
            size: 42,
            last_modified_ms: 7,
        }
    }

    #[tokio::test]
    async fn publication_reassembles_final_physical_order_and_rekeys_catalog() {
        let state = state(8 * 1024 * 1024);
        let builder = state.begin_incremental_build("snapshot-a");
        let path = object_store::path::Path::from("snapshot-a/data.vortex");
        // Arrival is intentionally reverse physical order. The observer's
        // position, not stream arrival, must determine every source ordinal.
        builder.batch_written(&path, 2, &batch(vec![Some(3), None], vec!["c", "null"]));
        builder.batch_written(&path, 0, &batch(vec![Some(1), Some(2)], vec!["a", "b"]));
        state
            .stage_pending("snapshot-a", &[file(path.as_ref())])
            .await;
        state.promote_staged("snapshot-a");

        let catalog = state.published().expect("complete catalog published");
        assert_eq!(catalog.snapshot_id(), "snapshot-a");
        assert_eq!(catalog.source_manifest().len(), 1);
        let index = &catalog.catalogs()[0];
        assert_eq!(index.sources().len(), 1);
        assert_eq!(index.runs().len(), 1);
        assert_eq!(
            index.runs()[0]
                .directory()
                .entries()
                .iter()
                .map(KeyDirectoryEntry::entry_count)
                .sum::<usize>(),
            3,
            "only the one NULL key is omitted from an otherwise complete physical source"
        );
        assert_eq!(state.generation(), 1);
    }

    #[tokio::test]
    async fn stale_staging_cannot_replace_a_newer_refresh_catalog() {
        let state = state(8 * 1024 * 1024);
        let old = state.begin_incremental_build("snapshot-old");
        let old_path = object_store::path::Path::from("snapshot-old/data.vortex");
        old.batch_written(&old_path, 0, &batch(vec![Some(1)], vec!["old"]));

        let new = state.begin_incremental_build("snapshot-new");
        let new_path = object_store::path::Path::from("snapshot-new/data.vortex");
        new.batch_written(&new_path, 0, &batch(vec![Some(2)], vec!["new"]));
        state
            .stage_pending("snapshot-old", &[file(old_path.as_ref())])
            .await;
        state
            .stage_pending("snapshot-new", &[file(new_path.as_ref())])
            .await;
        state.promote_staged("snapshot-new");

        let catalog = state.published().expect("new catalog stays published");
        assert_eq!(catalog.snapshot_id(), "snapshot-new");
        assert_eq!(state.generation(), 1);
    }

    #[tokio::test]
    async fn refused_capture_publishes_explicit_uncovered_state() {
        let state = state(1);
        let builder = state.begin_incremental_build("snapshot");
        let path = object_store::path::Path::from("snapshot/data.vortex");
        builder.batch_written(&path, 0, &batch(vec![Some(1)], vec!["value"]));
        state
            .stage_pending("snapshot", &[file(path.as_ref())])
            .await;
        state.promote_staged("snapshot");

        let catalog = state
            .published()
            .expect("an uncovered source manifest is published");
        assert!(
            !catalog.is_complete(),
            "optional admission refusal leaves data publishable but uncovered"
        );
        assert!(
            state.rejection().is_some(),
            "the optional refusal remains a structured internal rejection"
        );
    }
}
