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

//! Pinned catalog, coverage decisions, and bounded probing contracts.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, new_empty_array};
use arrow_schema::DataType;
use async_trait::async_trait;

use super::{
    BuiltCoveredSource, CoveredRowRef, CoveringPageStore, EncodedKey, Error, IndexDefinition,
    IndexRun, KeyDirectoryEntry, KeyPageId, KeyPageLease, LiteralSeekSpan, MemoryPageStore,
    PayloadPageId, PayloadPageLease, PreparedLiteralSeek, Result, SchemaIdentity, SourceId,
};
use super::{SourceRole, VisibilityAdapter};
use crate::provider::TimeRetentionFilterBuilder;
use crate::provider::scan::SnapshotScanRef;

/// Immutable source coverage available to a captured view.
#[derive(Clone, Debug)]
pub(crate) struct IndexedSource {
    source: SourceId,
    schema: SchemaIdentity,
    row_count: usize,
    payload_pages: Arc<[super::PayloadPageId]>,
}

impl IndexedSource {
    /// Validate that every payload page is generation-qualified for source.
    pub(crate) fn new(
        source: SourceId,
        schema: SchemaIdentity,
        row_count: usize,
        payload_pages: Vec<super::PayloadPageId>,
    ) -> Result<Self> {
        if payload_pages.iter().any(|page| page.source() != &source) {
            return Err(Error::InvalidContract {
                message: format!("a payload page does not belong to source {source:?}"),
            });
        }
        Ok(Self {
            source,
            schema,
            row_count,
            payload_pages: payload_pages.into(),
        })
    }

    /// Source generation identity.
    #[must_use]
    pub(crate) fn source(&self) -> &SourceId {
        &self.source
    }

    /// Schema identity for values returned from this source.
    #[must_use]
    pub(crate) fn schema(&self) -> &SchemaIdentity {
        &self.schema
    }

    /// Source row count, including rows omitted from an equality run for NULL keys.
    #[must_use]
    pub(crate) const fn row_count(&self) -> usize {
        self.row_count
    }

    /// Payload pages containing this source's covered values.
    #[must_use]
    pub(crate) fn payload_pages(&self) -> &[super::PayloadPageId] {
        &self.payload_pages
    }
}

/// Atomically publishable, immutable collection of complete source runs.
#[derive(Debug)]
pub(crate) struct IndexCatalog {
    definition: IndexDefinition,
    sources: BTreeMap<SourceId, IndexedSource>,
    runs: Arc<[IndexRun]>,
    page_store: Arc<dyn CoveringPageStore>,
}

impl IndexCatalog {
    /// Construct a catalog only when every run names a known source.
    pub(crate) fn new(
        definition: IndexDefinition,
        sources: Vec<IndexedSource>,
        runs: Vec<IndexRun>,
        page_store: Arc<dyn CoveringPageStore>,
    ) -> Result<Self> {
        let mut source_map = BTreeMap::new();
        for source in sources {
            if source_map.insert(source.source.clone(), source).is_some() {
                return Err(Error::InvalidContract {
                    message: "covering-index catalog contains a source more than once".to_string(),
                });
            }
        }
        if runs
            .iter()
            .any(|run| !source_map.contains_key(run.source()))
        {
            return Err(Error::InvalidContract {
                message: "covering-index run refers to a source absent from the catalog"
                    .to_string(),
            });
        }
        Ok(Self {
            definition,
            sources: source_map,
            runs: runs.into(),
            page_store,
        })
    }

    /// Assemble complete source artifacts for one definition into one immutable
    /// catalog. Each source keeps its own `MemoryPageStore`; the catalog routes
    /// batched page requests by generation-qualified source ID, so independent
    /// writer artifacts never overwrite each other's page IDs.
    pub(crate) fn from_built_sources(
        definition: IndexDefinition,
        built_sources: Vec<BuiltCoveredSource>,
    ) -> Result<Self> {
        let mut sources = Vec::with_capacity(built_sources.len());
        let mut runs = Vec::new();
        let mut stores = BTreeMap::new();
        for built in built_sources {
            let (source, source_runs, store) = built.into_parts();
            let source_id = source.source().clone();
            if stores.insert(source_id.clone(), store).is_some() {
                return Err(Error::InvalidContract {
                    message: format!(
                        "covering-index catalog contains source {source_id:?} more than once"
                    ),
                });
            }
            sources.push(source);
            runs.extend(source_runs.iter().cloned());
        }
        let page_store: Arc<dyn CoveringPageStore> = Arc::new(SourcePageStores { stores });
        Self::new(definition, sources, runs, page_store)
    }

    /// Shared index definition for every run in this catalog.
    #[must_use]
    pub(crate) fn definition(&self) -> &IndexDefinition {
        &self.definition
    }

    /// Indexed source generations.
    #[must_use]
    pub(crate) fn sources(&self) -> &BTreeMap<SourceId, IndexedSource> {
        &self.sources
    }

    /// Sorted runs, preserving duplicate physical rows.
    #[must_use]
    pub(crate) fn runs(&self) -> &[IndexRun] {
        &self.runs
    }

    /// Store that owns the admitted immutable pages.
    #[must_use]
    pub(crate) fn page_store(&self) -> &Arc<dyn CoveringPageStore> {
        &self.page_store
    }
}

/// Immutable collection of all configured covering definitions for one source
/// manifest. It is the single object a table publisher swaps, so a captured
/// scan can retain the old catalog while a refresh installs a wholly new one.
#[derive(Debug)]
pub(crate) struct CoveringIndexCatalog {
    snapshot_id: Arc<str>,
    catalogs: Arc<[Arc<IndexCatalog>]>,
    source_manifest: BTreeSet<SourceId>,
    complete: bool,
}

impl CoveringIndexCatalog {
    /// Assemble one catalog per definition and prove every catalog names the
    /// same complete immutable source manifest before publication.
    pub(crate) fn new(
        snapshot_id: impl Into<Arc<str>>,
        catalogs: Vec<Arc<IndexCatalog>>,
    ) -> Result<Self> {
        let source_manifest = catalogs.first().map_or_else(BTreeSet::new, |catalog| {
            catalog.sources().keys().cloned().collect()
        });
        if catalogs.iter().any(|catalog| {
            catalog.sources().keys().cloned().collect::<BTreeSet<_>>() != source_manifest
        }) {
            return Err(Error::InvalidContract {
                message: "covering-index definitions do not cover the same source manifest"
                    .to_string(),
            });
        }
        Ok(Self {
            snapshot_id: snapshot_id.into(),
            catalogs: catalogs.into(),
            source_manifest,
            complete: true,
        })
    }

    /// Publish explicit uncovered sources when optional admission or validation
    /// refuses an artifact. The table still has an immutable catalog identity to
    /// capture and rekey on, but `try_cover` must decline it rather than mistake
    /// a missing catalog for an empty table.
    #[must_use]
    pub(crate) fn uncovered(
        snapshot_id: impl Into<Arc<str>>,
        source_manifest: impl IntoIterator<Item = SourceId>,
    ) -> Self {
        Self {
            snapshot_id: snapshot_id.into(),
            catalogs: Arc::new([]),
            source_manifest: source_manifest.into_iter().collect(),
            complete: false,
        }
    }

    /// Snapshot whose file manifest these artifacts describe.
    #[must_use]
    pub(crate) fn snapshot_id(&self) -> &str {
        &self.snapshot_id
    }

    /// Complete source manifest shared by every configured definition.
    #[must_use]
    pub(crate) fn source_manifest(&self) -> &BTreeSet<SourceId> {
        &self.source_manifest
    }

    /// Whether every source in the manifest has complete runs and payloads.
    #[must_use]
    pub(crate) const fn is_complete(&self) -> bool {
        self.complete
    }

    /// Individual definition catalogs, pinned together by this catalog.
    #[must_use]
    pub(crate) fn catalogs(&self) -> &[Arc<IndexCatalog>] {
        &self.catalogs
    }

    /// Find a structurally compatible definition without relying on a label or
    /// hash collision.
    #[must_use]
    pub(crate) fn catalog_for(&self, definition: &IndexDefinition) -> Option<&Arc<IndexCatalog>> {
        self.catalogs
            .iter()
            .find(|catalog| catalog.definition().matches(definition))
    }
}

/// Routes page loads to the immutable store that owns each source generation.
///
/// All state is immutable and every awaited page load happens after the routing
/// maps have been read, so publication never holds a synchronous lock across
/// I/O or encoding work.
#[derive(Debug)]
struct SourcePageStores {
    stores: BTreeMap<SourceId, Arc<MemoryPageStore>>,
}

#[async_trait]
impl CoveringPageStore for SourcePageStores {
    async fn load_key_pages(&self, ids: &[KeyPageId]) -> Result<Vec<KeyPageLease>> {
        let mut groups: BTreeMap<&SourceId, Vec<(usize, KeyPageId)>> = BTreeMap::new();
        for (position, id) in ids.iter().enumerate() {
            groups
                .entry(id.source())
                .or_default()
                .push((position, id.clone()));
        }
        let mut loaded = vec![None; ids.len()];
        for (source, requested) in groups {
            let store = self.stores.get(source).ok_or_else(|| Error::MissingPage {
                page: format!("source {source:?}"),
            })?;
            let requested_ids = requested
                .iter()
                .map(|(_, id)| id.clone())
                .collect::<Vec<_>>();
            let leases = store.load_key_pages(&requested_ids).await?;
            for ((position, _), lease) in requested.into_iter().zip(leases) {
                loaded[position] = Some(lease);
            }
        }
        loaded
            .into_iter()
            .enumerate()
            .map(|(position, lease)| {
                lease.ok_or_else(|| Error::InvalidContract {
                    message: format!("key-page routing omitted request {position}"),
                })
            })
            .collect()
    }

    async fn load_payload_pages(&self, ids: &[PayloadPageId]) -> Result<Vec<PayloadPageLease>> {
        let mut groups: BTreeMap<&SourceId, Vec<(usize, PayloadPageId)>> = BTreeMap::new();
        for (position, id) in ids.iter().enumerate() {
            groups
                .entry(id.source())
                .or_default()
                .push((position, id.clone()));
        }
        let mut loaded = vec![None; ids.len()];
        for (source, requested) in groups {
            let store = self.stores.get(source).ok_or_else(|| Error::MissingPage {
                page: format!("source {source:?}"),
            })?;
            let requested_ids = requested
                .iter()
                .map(|(_, id)| id.clone())
                .collect::<Vec<_>>();
            let leases = store.load_payload_pages(&requested_ids).await?;
            for ((position, _), lease) in requested.into_iter().zip(leases) {
                loaded[position] = Some(lease);
            }
        }
        loaded
            .into_iter()
            .enumerate()
            .map(|(position, lease)| {
                lease.ok_or_else(|| Error::InvalidContract {
                    message: format!("payload-page routing omitted request {position}"),
                })
            })
            .collect()
    }
}

/// Closed reasons that optional covering access is unavailable at plan time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CoverageUnavailableReason {
    /// A nonempty scan-visible source has no compatible index run or payload page.
    MissingSource,
    /// The requested definition differs from the captured catalog definition.
    DefinitionMismatch,
    /// Key, expression, or query-schema conversion lacks a proven lossless adapter.
    UnsupportedTypeOrExpression,
    /// Existing delete, retention, or transaction visibility cannot be reproduced.
    IncompleteVisibility,
    /// Admission or bounded working memory is unavailable.
    UnavailableResources,
    /// An active transaction or distributed execution cannot retain this local view.
    TransactionOrDistributedContext,
    /// The ordinary scan must still read the source to complete configured
    /// end-to-end integrity verification.
    IntegrityPreflight,
}

/// Result of attempting to prove complete optional coverage.
#[derive(Clone, Debug)]
pub(crate) enum CoverageDecision {
    /// A complete pinned read view can use its covering pages.
    Complete(Arc<CoveringReadView>),
    /// Normal scanning must remain the chosen path.
    Unavailable(CoverageUnavailableReason),
}

/// Whether a manifest entry is known to be empty at the captured scan point.
///
/// `Unknown` deliberately does not mean empty. A source that normal scanning
/// can reach but whose row count was not materialized must still have a
/// compatible covering artifact before the optional path can run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SourceRows {
    /// The source has an exact captured physical row count.
    Known(usize),
    /// The scan can read this source, but no exact row count was captured.
    Unknown,
}

impl SourceRows {
    #[must_use]
    const fn is_known_empty(self) -> bool {
        matches!(self, Self::Known(0))
    }
}

/// One source a normal scan of a captured view can reach.
///
/// This is intentionally a map entry, not a derived listing at probe time: a
/// file rewrite, cold promotion, or in-memory batch replacement after capture
/// must not change what the covering proof considers complete.
#[derive(Clone, Debug)]
pub(crate) struct CapturedSource {
    source: SourceId,
    role: SourceRole,
    rows: SourceRows,
    visibility: VisibilityAdapter,
}

impl CapturedSource {
    /// Create one explicit captured source and its source-role visibility rule.
    #[must_use]
    pub(crate) fn new(
        source: SourceId,
        role: SourceRole,
        rows: SourceRows,
        visibility: VisibilityAdapter,
    ) -> Self {
        Self {
            source,
            role,
            rows,
            visibility,
        }
    }

    /// Generation-qualified source identity.
    #[must_use]
    pub(crate) fn source(&self) -> &SourceId {
        &self.source
    }

    /// Scan branch whose rules govern this source.
    #[must_use]
    pub(crate) const fn role(&self) -> SourceRole {
        self.role
    }

    /// Exact source cardinality, where the capture has one.
    #[must_use]
    pub(crate) const fn rows(&self) -> SourceRows {
        self.rows
    }

    /// Retained source-role deletion/removal interpretation.
    #[must_use]
    pub(crate) fn visibility(&self) -> &VisibilityAdapter {
        &self.visibility
    }
}

/// Scan-visible source manifest and other state held by a covering read.
///
/// Step 05 wires this to `ScanView` and the existing visibility captures. The
/// manifest itself is already explicit here so callers cannot silently treat a
/// partially built catalog as coverage for a captured scan.
pub(crate) struct CoveringReadView {
    catalog: Arc<IndexCatalog>,
    catalog_complete: bool,
    eligibility_failure: Option<CoverageUnavailableReason>,
    source_manifest: BTreeMap<SourceId, CapturedSource>,
    query_schema: SchemaIdentity,
    /// Retains the configured retention rule, not a precomputed cutoff. A
    /// covering executor builds the expression when it evaluates a query so
    /// time-based retention keeps the same moving-boundary semantics as the
    /// ordinary scan.
    retention_filter: Option<TimeRetentionFilterBuilder>,
    /// Pins the table snapshot directories while a covering operator outlives
    /// the `ScanView` that created it. Source adapters retain their own
    /// deletion/cache arcs; this guard retains the file paths themselves.
    scan_guard: Option<Arc<SnapshotScanRef>>,
}

impl std::fmt::Debug for CoveringReadView {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CoveringReadView")
            .field("catalog_sources", &self.catalog.sources().len())
            .field("catalog_complete", &self.catalog_complete)
            .field("eligibility_failure", &self.eligibility_failure)
            .field("manifest_sources", &self.source_manifest.len())
            .field("has_retention_filter", &self.retention_filter.is_some())
            .field("has_scan_guard", &self.scan_guard.is_some())
            .finish_non_exhaustive()
    }
}

impl CoveringReadView {
    /// Construct the position-only form used by page-store unit tests. Production
    /// scan capture must use [`Self::capture`], which retains a scan guard and
    /// the actual source-role adapters.
    pub(crate) fn new(
        catalog: Arc<IndexCatalog>,
        source_manifest: impl IntoIterator<Item = SourceId>,
        query_schema: SchemaIdentity,
    ) -> Self {
        let visibility = VisibilityAdapter::position_only_file();
        let source_manifest = source_manifest
            .into_iter()
            .map(|source| {
                let rows = catalog
                    .sources()
                    .get(&source)
                    .map_or(SourceRows::Unknown, |indexed| {
                        SourceRows::Known(indexed.row_count())
                    });
                (
                    source.clone(),
                    CapturedSource::new(source, SourceRole::Warm, rows, visibility.clone()),
                )
            })
            .collect();
        Self {
            catalog,
            catalog_complete: true,
            eligibility_failure: None,
            source_manifest,
            query_schema,
            retention_filter: None,
            scan_guard: None,
        }
    }

    /// Capture a complete manifest and all state needed to interpret it later.
    ///
    /// This validates duplicate identities at construction time; a missing
    /// source is never later inferred to be an empty source.
    pub(crate) fn capture(
        catalog: Arc<IndexCatalog>,
        catalog_complete: bool,
        sources: impl IntoIterator<Item = CapturedSource>,
        query_schema: SchemaIdentity,
        scan_guard: Arc<SnapshotScanRef>,
        eligibility_failure: Option<CoverageUnavailableReason>,
        retention_filter: Option<TimeRetentionFilterBuilder>,
    ) -> Result<Self> {
        let mut source_manifest = BTreeMap::new();
        for source in sources {
            if source_manifest
                .insert(source.source().clone(), source)
                .is_some()
            {
                return Err(Error::InvalidContract {
                    message: "captured covering source manifest has a duplicate source identity"
                        .to_string(),
                });
            }
        }
        Ok(Self {
            catalog,
            catalog_complete,
            eligibility_failure,
            source_manifest,
            query_schema,
            retention_filter,
            scan_guard: Some(scan_guard),
        })
    }

    /// The atomically captured catalog.
    #[must_use]
    pub(crate) fn catalog(&self) -> &Arc<IndexCatalog> {
        &self.catalog
    }

    /// Every source a normal scan of this view could read.
    #[must_use]
    pub(crate) fn source_manifest(&self) -> &BTreeMap<SourceId, CapturedSource> {
        &self.source_manifest
    }

    /// Query schema required for exact output adaptation.
    #[must_use]
    pub(crate) fn query_schema(&self) -> &SchemaIdentity {
        &self.query_schema
    }

    /// Construct the current time-based retention predicate for covered rows.
    ///
    /// This deliberately runs when the query is evaluated, rather than when
    /// the read view or its index artifact was captured. Freezing a cutoff here
    /// would let a paused plan return rows that a normal scan would now hide.
    #[must_use]
    pub(crate) fn retention_keep_filter(&self) -> Option<datafusion::logical_expr::Expr> {
        self.retention_filter
            .as_ref()
            .map(TimeRetentionFilterBuilder::keep_filter)
    }

    /// Return the captured source-role adapter for `source`.
    #[must_use]
    pub(crate) fn visibility_for(&self, source: &SourceId) -> Option<&VisibilityAdapter> {
        self.source_manifest
            .get(source)
            .map(CapturedSource::visibility)
    }

    /// Apply the captured source's ordinary scan visibility to candidate payload
    /// rows. Index scan and join execution call this before residual predicates;
    /// callers must pass original physical source ordinals, not candidate indexes.
    pub(crate) fn select_visible_rows(
        &self,
        source: &SourceId,
        batch: &RecordBatch,
        physical_ordinals: &[u64],
    ) -> Result<Vec<usize>> {
        self.visibility_for(source)
            .ok_or_else(|| Error::InvalidContract {
                message: format!("covered source {source:?} is absent from the captured manifest"),
            })?
            .select_visible_rows(batch, physical_ordinals)
    }
}

/// Prove whether a captured view has complete, compatible index coverage.
///
/// This is deliberately conservative. Every captured source must have its
/// compatible artifact and source-role visibility rule; a nonempty source is
/// never inferred from an absent catalog entry. Required output/filter columns
/// must also have a proven captured-schema adapter before a planner can select
/// the optional path.
pub(crate) fn try_cover(
    view: Arc<CoveringReadView>,
    definition: &IndexDefinition,
    required_columns: &[usize],
) -> Result<CoverageDecision> {
    if let Some(reason) = view.eligibility_failure {
        return Ok(CoverageDecision::Unavailable(reason));
    }
    if !view.catalog_complete {
        return Ok(CoverageDecision::Unavailable(
            CoverageUnavailableReason::MissingSource,
        ));
    }
    if !view.catalog.definition().matches(definition) {
        return Ok(CoverageDecision::Unavailable(
            CoverageUnavailableReason::DefinitionMismatch,
        ));
    }
    if !schema_adaptation_is_supported(view.catalog.definition().schema(), view.query_schema())
        || required_columns.iter().any(|column| {
            *column >= view.query_schema.schema().fields().len()
                || view
                    .query_schema()
                    .column_mapping()
                    .get(*column)
                    .is_none_or(|source| *source >= definition.schema().schema().fields().len())
        })
    {
        return Ok(CoverageDecision::Unavailable(
            CoverageUnavailableReason::UnsupportedTypeOrExpression,
        ));
    }

    let catalog_sources = view
        .catalog
        .sources()
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    let captured_nonempty_or_unknown = view
        .source_manifest()
        .iter()
        .filter(|(_, source)| !source.rows().is_known_empty())
        .map(|(source, _)| source.clone())
        .collect::<BTreeSet<_>>();
    if catalog_sources != captured_nonempty_or_unknown {
        return Ok(CoverageDecision::Unavailable(
            CoverageUnavailableReason::MissingSource,
        ));
    }

    for (source_id, captured) in view.source_manifest() {
        if !captured.visibility().is_complete() {
            return Ok(CoverageDecision::Unavailable(
                CoverageUnavailableReason::IncompleteVisibility,
            ));
        }
        if captured.visibility().role() != Some(captured.role()) {
            return Err(Error::InvalidContract {
                message: format!(
                    "captured source {source_id:?} has a visibility adapter for a different source role"
                ),
            });
        }
        let Some(source) = view.catalog.sources().get(source_id) else {
            if captured.rows().is_known_empty() {
                continue;
            }
            return Ok(CoverageDecision::Unavailable(
                CoverageUnavailableReason::MissingSource,
            ));
        };
        if !source.schema().matches(definition.schema()) {
            return Ok(CoverageDecision::Unavailable(
                CoverageUnavailableReason::UnsupportedTypeOrExpression,
            ));
        }
        if let SourceRows::Known(rows) = captured.rows()
            && rows != source.row_count()
        {
            return Err(Error::InvalidContract {
                message: format!(
                    "covering source {source_id:?} has {} rows but its captured manifest has {rows}",
                    source.row_count()
                ),
            });
        }
        if captured.rows().is_known_empty() {
            continue;
        }
        if source.payload_pages().is_empty()
            || !view
                .catalog
                .runs()
                .iter()
                .any(|run| run.source() == source_id)
        {
            return Ok(CoverageDecision::Unavailable(
                CoverageUnavailableReason::MissingSource,
            ));
        }
    }

    Ok(CoverageDecision::Complete(view))
}

fn schema_adaptation_is_supported(source: &SchemaIdentity, query: &SchemaIdentity) -> bool {
    if source.schema().fields().len() != query.schema().fields().len()
        || source.column_mapping().len() != query.column_mapping().len()
    {
        return false;
    }
    source
        .schema()
        .fields()
        .iter()
        .zip(query.schema().fields())
        .all(|(source, query)| {
            source.name() == query.name()
                && source.is_nullable() == query.is_nullable()
                && source.metadata() == query.metadata()
                && data_type_adaptation_is_supported(source.data_type(), query.data_type())
        })
}

fn data_type_adaptation_is_supported(source: &DataType, query: &DataType) -> bool {
    source == query
        || matches!(
            (source, query),
            (DataType::Utf8, DataType::Utf8View)
                | (DataType::Utf8View, DataType::Utf8)
                | (DataType::Binary, DataType::BinaryView)
                | (DataType::BinaryView, DataType::Binary)
        )
}

/// One non-NULL full equality key correlated with its originating row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProbeRequest {
    request_ordinal: usize,
    key: EncodedKey,
    required_columns: Arc<[usize]>,
}

impl ProbeRequest {
    /// Construct a correlated full-key request with its stable input ordinal.
    #[must_use]
    pub(crate) fn new(
        request_ordinal: usize,
        key: EncodedKey,
        required_columns: Vec<usize>,
    ) -> Self {
        Self {
            request_ordinal,
            key,
            required_columns: required_columns.into(),
        }
    }

    /// Stable ordinal of the outer/probe input row.
    #[must_use]
    pub(crate) const fn request_ordinal(&self) -> usize {
        self.request_ordinal
    }

    /// Encoded correlated full key.
    #[must_use]
    pub(crate) fn key(&self) -> &EncodedKey {
        &self.key
    }

    /// Source columns required by later gather/filter work.
    #[must_use]
    pub(crate) fn required_columns(&self) -> &[usize] {
        &self.required_columns
    }
}

/// One duplicate-preserving match from a request to a covered physical row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProbeMatch {
    request_ordinal: usize,
    row_ref: CoveredRowRef,
}

impl ProbeMatch {
    /// Pair a request ordinal with a generation-qualified physical row reference.
    #[must_use]
    pub(crate) fn new(request_ordinal: usize, row_ref: CoveredRowRef) -> Self {
        Self {
            request_ordinal,
            row_ref,
        }
    }

    /// Input request ordinal.
    #[must_use]
    pub(crate) const fn request_ordinal(&self) -> usize {
        self.request_ordinal
    }

    /// Matched physical row; repeated rows remain separate matches.
    #[must_use]
    pub(crate) fn row_ref(&self) -> &CoveredRowRef {
        &self.row_ref
    }
}

/// Bounded outcome of advancing a `ProbeCursor`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ProbeStep {
    /// More matching rows are available after this nonempty chunk.
    Matches(Vec<ProbeMatch>),
    /// No match was emitted in this bounded attempt, but the cursor is not exhausted.
    Pending,
    /// Every source/run/page has been proven consumed.
    Exhausted,
}

/// One distinct request key's work against one key page.
#[derive(Debug)]
struct ProbePageWork {
    source: SourceId,
    page: KeyPageId,
    expected_entries: usize,
    key: EncodedKey,
    request_ordinals: Arc<[usize]>,
}

/// A loaded key page held only while the cursor consumes its matching range.
#[derive(Debug)]
struct ActiveProbePage {
    work_index: usize,
    lease: KeyPageLease,
    next_entry: usize,
    end_entry: usize,
    next_request: usize,
}

/// Resumable state for page-bounded probing.
///
/// Work is sorted and deduplicated by key while retaining all originating
/// request ordinals. The cursor holds at most one key-page lease while walking
/// its duplicate span, so a very common key is emitted in bounded chunks.
#[derive(Debug)]
pub(crate) struct ProbeCursor {
    view: Arc<CoveringReadView>,
    work: Vec<ProbePageWork>,
    next_work: usize,
    active: Option<ActiveProbePage>,
}

impl ProbeCursor {
    /// Begin a bounded probe over a pinned view.
    #[must_use]
    pub(crate) fn new(view: Arc<CoveringReadView>, requests: Vec<ProbeRequest>) -> Self {
        let mut distinct = BTreeMap::<EncodedKey, Vec<usize>>::new();
        for request in requests {
            distinct
                .entry(request.key().clone())
                .or_default()
                .push(request.request_ordinal());
        }
        let mut work = Vec::new();
        for (key, request_ordinals) in distinct {
            let request_ordinals: Arc<[usize]> = request_ordinals.into();
            for run in view.catalog().runs() {
                for entry in run.directory().candidate_pages(&key) {
                    work.push(ProbePageWork {
                        source: run.source().clone(),
                        page: entry.page().clone(),
                        expected_entries: entry.entry_count(),
                        key: key.clone(),
                        request_ordinals: Arc::clone(&request_ordinals),
                    });
                }
            }
        }
        Self {
            view,
            work,
            next_work: 0,
            active: None,
        }
    }

    /// Advance no farther than `max_rows` and `byte_budget` permit.
    pub(crate) async fn next_matches(
        &mut self,
        max_rows: usize,
        byte_budget: usize,
    ) -> Result<ProbeStep> {
        if max_rows == 0 || byte_budget == 0 {
            return Err(Error::InvalidContract {
                message: "probe chunk limits must both be greater than zero".to_string(),
            });
        }

        let mut matches = Vec::new();
        let mut emitted_bytes = 0usize;
        loop {
            if let Some(active) = self.active.as_mut() {
                let work =
                    self.work
                        .get(active.work_index)
                        .ok_or_else(|| Error::InvalidContract {
                            message: "probe cursor lost its active page work".to_string(),
                        })?;
                if active.next_entry == active.end_entry {
                    self.active = None;
                    self.next_work = self.next_work.checked_add(1).ok_or(Error::Overflow {
                        operation: "probe page work index",
                    })?;
                    continue;
                }
                let row_ref = active.lease.page().row_ref(active.next_entry)?;
                validate_probe_row_ref(&self.view, &work.source, row_ref)?;
                let match_bytes = std::mem::size_of::<ProbeMatch>();
                let exceeds_bytes = emitted_bytes
                    .checked_add(match_bytes)
                    .is_none_or(|bytes| bytes > byte_budget);
                if !matches.is_empty() && (matches.len() == max_rows || exceeds_bytes) {
                    return Ok(ProbeStep::Matches(matches));
                }
                matches.push(ProbeMatch::new(
                    *work
                        .request_ordinals
                        .get(active.next_request)
                        .ok_or_else(|| Error::InvalidContract {
                            message: "probe cursor lost a request ordinal".to_string(),
                        })?,
                    row_ref.clone(),
                ));
                emitted_bytes = emitted_bytes.saturating_add(match_bytes);
                active.next_request =
                    active.next_request.checked_add(1).ok_or(Error::Overflow {
                        operation: "probe request ordinal index",
                    })?;
                if active.next_request == work.request_ordinals.len() {
                    active.next_request = 0;
                    active.next_entry =
                        active.next_entry.checked_add(1).ok_or(Error::Overflow {
                            operation: "probe key-page entry index",
                        })?;
                }
                if matches.len() == max_rows {
                    return Ok(ProbeStep::Matches(matches));
                }
                continue;
            }

            let Some(work) = self.work.get(self.next_work) else {
                return if matches.is_empty() {
                    Ok(ProbeStep::Exhausted)
                } else {
                    Ok(ProbeStep::Matches(matches))
                };
            };
            let mut leases = self
                .view
                .catalog()
                .page_store()
                .load_key_pages(std::slice::from_ref(&work.page))
                .await?;
            let lease = leases.pop().ok_or_else(|| Error::InvalidContract {
                message: "page store returned no lease for one requested key page".to_string(),
            })?;
            if !leases.is_empty() {
                return Err(Error::InvalidContract {
                    message: "page store returned too many leases for one requested key page"
                        .to_string(),
                });
            }
            let (first, end) = exact_key_range_for_page(
                &work.page,
                work.expected_entries,
                lease.page(),
                &work.key,
            )?;
            if first == end {
                self.next_work = self.next_work.checked_add(1).ok_or(Error::Overflow {
                    operation: "probe page work index",
                })?;
                continue;
            }
            self.active = Some(ActiveProbePage {
                work_index: self.next_work,
                lease,
                next_entry: first,
                end_entry: end,
                next_request: 0,
            });
        }
    }
}

/// Start a bounded duplicate-preserving probe over a pinned view.
#[must_use]
pub(crate) fn probe_many(view: Arc<CoveringReadView>, requests: Vec<ProbeRequest>) -> ProbeCursor {
    ProbeCursor::new(view, requests)
}

/// One bounded payload gather result. `consumed` tells the caller where the
/// next request chunk begins without silently dropping repeated row references.
#[derive(Debug)]
pub(crate) struct GatherBatch {
    batch: RecordBatch,
    consumed: usize,
}

impl GatherBatch {
    /// Requested projection in the exact requested row-reference order.
    #[must_use]
    pub(crate) fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    /// Number of input references represented in `batch`.
    #[must_use]
    pub(crate) const fn consumed(&self) -> usize {
        self.consumed
    }
}

/// Gather projected payload values for a bounded prefix of row references.
///
/// The implementation loads each distinct payload page once, uses Arrow's
/// interleave kernel rather than scalar cells, and restores the caller's exact
/// input order (including repeated references).
pub(crate) async fn gather(
    view: &CoveringReadView,
    row_refs: &[CoveredRowRef],
    projection: &[usize],
    max_rows: usize,
    byte_budget: usize,
) -> Result<GatherBatch> {
    if max_rows == 0 || byte_budget == 0 {
        return Err(Error::InvalidContract {
            message: "gather chunk limits must both be greater than zero".to_string(),
        });
    }
    let mut count = row_refs.len().min(max_rows);
    loop {
        let batch = gather_prefix(view, &row_refs[..count], projection).await?;
        if count <= 1 || batch.get_array_memory_size() <= byte_budget {
            return Ok(GatherBatch {
                batch,
                consumed: count,
            });
        }
        count = count.checked_add(1).ok_or(Error::Overflow {
            operation: "gather prefix shrink",
        })? / 2;
    }
}

async fn gather_prefix(
    view: &CoveringReadView,
    row_refs: &[CoveredRowRef],
    projection: &[usize],
) -> Result<RecordBatch> {
    let output_schema = view
        .query_schema()
        .schema()
        .project(projection)
        .map_err(|source| Error::Arrow { source })?;
    if row_refs.is_empty() {
        let columns = output_schema
            .fields()
            .iter()
            .map(|field| new_empty_array(field.data_type()))
            .collect();
        return RecordBatch::try_new_with_options(
            Arc::new(output_schema),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(0)),
        )
        .map_err(|source| Error::Arrow { source });
    }

    let source_projection = projection
        .iter()
        .map(|column| {
            view.query_schema()
                .column_mapping()
                .get(*column)
                .copied()
                .ok_or_else(|| Error::InvalidContract {
                    message: format!("projection column {column} is outside the captured schema"),
                })
        })
        .collect::<Result<Vec<_>>>()?;
    let source_schema = view
        .catalog()
        .definition()
        .schema()
        .schema()
        .project(&source_projection)
        .map_err(|source| Error::Arrow { source })?;

    let mut grouped = BTreeMap::<PayloadPageId, Vec<(usize, usize)>>::new();
    for (output, row_ref) in row_refs.iter().enumerate() {
        validate_probe_row_ref(view, row_ref.source(), row_ref)?;
        grouped
            .entry(row_ref.payload_page().clone())
            .or_default()
            .push((output, row_ref.row_in_page()));
    }
    let page_ids = grouped.keys().cloned().collect::<Vec<_>>();
    let leases = view
        .catalog()
        .page_store()
        .load_payload_pages(&page_ids)
        .await?;
    if leases.len() != page_ids.len() {
        return Err(Error::InvalidContract {
            message: format!(
                "page store returned {} payload leases for {} requested pages",
                leases.len(),
                page_ids.len()
            ),
        });
    }

    let mut position = vec![None; row_refs.len()];
    let mut pages = Vec::with_capacity(leases.len());
    for (page_index, (id, lease)) in page_ids.iter().zip(leases.iter()).enumerate() {
        let source =
            view.catalog()
                .sources()
                .get(id.source())
                .ok_or_else(|| Error::InvalidContract {
                    message: format!("payload page {id:?} belongs to an unknown source"),
                })?;
        if !lease.page().schema().matches(source.schema()) {
            return Err(Error::InvalidContract {
                message: format!("payload page {id:?} has a mismatched source schema"),
            });
        }
        for (output, row) in grouped.get(id).ok_or_else(|| Error::InvalidContract {
            message: format!("payload page {id:?} disappeared from its gather group"),
        })? {
            if *row >= lease.page().batch().num_rows() {
                return Err(Error::InvalidContract {
                    message: format!("row {row} is outside payload page {id:?}"),
                });
            }
            position[*output] = Some((page_index, *row));
        }
        pages.push(lease.page().batch());
    }
    let positions = position
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| Error::InvalidContract {
            message: "gather output has an unassigned row position".to_string(),
        })?;
    let columns = source_projection
        .iter()
        .map(|column| gather_column(&pages, &positions, *column))
        .collect::<Result<Vec<_>>>()?;
    let source_batch = RecordBatch::try_new_with_options(
        Arc::new(source_schema),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(row_refs.len())),
    )
    .map_err(|source| Error::Arrow { source })?;
    arrow_tools::record_batch::try_cast_to(source_batch, Arc::new(output_schema)).map_err(
        |source| Error::InvalidContract {
            message: format!(
                "failed to adapt gathered payload to the captured query schema: {source}"
            ),
        },
    )
}

fn gather_column(
    pages: &[&RecordBatch],
    positions: &[(usize, usize)],
    column: usize,
) -> Result<ArrayRef> {
    let arrays = pages
        .iter()
        .map(|page| {
            page.columns()
                .get(column)
                .map(AsRef::as_ref)
                .ok_or_else(|| Error::InvalidContract {
                    message: format!("projection column {column} is outside a payload page schema"),
                })
        })
        .collect::<Result<Vec<&dyn Array>>>()?;
    arrow::compute::interleave(&arrays, positions).map_err(|source| Error::Arrow { source })
}

fn validate_probe_row_ref(
    view: &CoveringReadView,
    source_id: &SourceId,
    row_ref: &CoveredRowRef,
) -> Result<()> {
    if row_ref.source() != source_id || row_ref.payload_page().source() != source_id {
        return Err(Error::InvalidContract {
            message: "probe row reference crosses source generations".to_string(),
        });
    }
    let source = view
        .catalog()
        .sources()
        .get(source_id)
        .ok_or_else(|| Error::InvalidContract {
            message: format!("probe row reference names unknown source {source_id:?}"),
        })?;
    if !source.payload_pages().contains(row_ref.payload_page()) {
        return Err(Error::InvalidContract {
            message: format!(
                "probe row reference names payload page {:?} absent from its source",
                row_ref.payload_page()
            ),
        });
    }
    if row_ref.source_row_ordinal()
        >= u64::try_from(source.row_count()).map_err(|_| Error::Overflow {
            operation: "source row count conversion",
        })?
    {
        return Err(Error::InvalidContract {
            message: "probe row reference is outside its source row count".to_string(),
        });
    }
    Ok(())
}

/// Prepare compact literal-seek spans without materializing matching row refs.
///
/// The catalog and its page-store Arc pin descriptors across suspension. A
/// descriptor-list allocation must be admitted by the caller before this API is
/// used in the query path; step 06 will turn an admission refusal into optional
/// coverage decline rather than choosing a scan after new-path execution began.
pub(crate) async fn prepare_literal_seek(
    view: &CoveringReadView,
    key: &EncodedKey,
) -> Result<PreparedLiteralSeek> {
    let mut spans = Vec::new();
    let mut total = 0usize;

    for run in view.catalog.runs() {
        let entries = run.directory().candidate_pages(key);
        if entries.is_empty() {
            continue;
        }
        let first_entry = entries.first().ok_or_else(|| Error::InvalidContract {
            message: "literal seek lost its first candidate page".to_string(),
        })?;
        let last_entry = entries.last().ok_or_else(|| Error::InvalidContract {
            message: "literal seek lost its last candidate page".to_string(),
        })?;
        let mut ids = vec![first_entry.page().clone()];
        if last_entry.page() != first_entry.page() {
            ids.push(last_entry.page().clone());
        }
        let leases = view.catalog.page_store().load_key_pages(&ids).await?;
        if leases.len() != ids.len() {
            return Err(Error::InvalidContract {
                message: format!(
                    "page store returned {} key leases for {} requested pages",
                    leases.len(),
                    ids.len()
                ),
            });
        }

        let first_lease = leases.first().ok_or_else(|| Error::InvalidContract {
            message: "page store returned no boundary lease for a literal seek".to_string(),
        })?;
        let last_lease = leases.last().ok_or_else(|| Error::InvalidContract {
            message: "page store returned no final boundary lease for a literal seek".to_string(),
        })?;
        let (first_offset, first_end) = exact_key_range(first_entry, first_lease.page(), key)?;
        if first_entry.page() == last_entry.page() {
            if first_offset == first_end {
                continue;
            }
            let matched = first_end.checked_sub(first_offset).ok_or(Error::Overflow {
                operation: "literal seek entry count",
            })?;
            total = total.checked_add(matched).ok_or(Error::Overflow {
                operation: "literal seek total entry count",
            })?;
            spans.push(LiteralSeekSpan::single_page(
                run.source().clone(),
                run.run(),
                first_entry.page().clone(),
                first_offset,
                first_end,
            )?);
            continue;
        }

        let (last_start, last_end) = exact_key_range(last_entry, last_lease.page(), key)?;
        if first_offset == first_end || last_start == last_end {
            return Err(Error::InvalidContract {
                message: "literal seek directory bounds do not match their boundary key pages"
                    .to_string(),
            });
        }
        if first_end != first_entry.entry_count() || last_start != 0 {
            return Err(Error::InvalidContract {
                message: "literal seek duplicate range is not contiguous across key pages"
                    .to_string(),
            });
        }
        let mut matched = first_end.checked_sub(first_offset).ok_or(Error::Overflow {
            operation: "literal seek first boundary count",
        })?;
        for entry in &entries[1..entries.len() - 1] {
            matched = matched
                .checked_add(entry.entry_count())
                .ok_or(Error::Overflow {
                    operation: "literal seek interior page count",
                })?;
        }
        matched = matched.checked_add(last_end).ok_or(Error::Overflow {
            operation: "literal seek final boundary count",
        })?;
        total = total.checked_add(matched).ok_or(Error::Overflow {
            operation: "literal seek total entry count",
        })?;
        spans.push(LiteralSeekSpan::new(
            run.source().clone(),
            run.run(),
            first_entry.page().clone(),
            first_offset,
            last_entry.page().clone(),
            last_end,
        )?);
    }

    PreparedLiteralSeek::new(spans, total)
}

/// Return the exact lower/upper offsets for key in one already-pinned page.
fn exact_key_range(
    entry: &KeyDirectoryEntry,
    page: &super::KeyPage,
    key: &EncodedKey,
) -> Result<(usize, usize)> {
    if page.is_empty() {
        return Err(Error::InvalidContract {
            message: format!(
                "directory entry for {:?} names an empty key page",
                entry.page()
            ),
        });
    }
    if page.len() != entry.entry_count() {
        return Err(Error::InvalidContract {
            message: format!(
                "directory for {:?} says {} entries but page has {}",
                entry.page(),
                entry.entry_count(),
                page.len()
            ),
        });
    }
    let lower = page_lower_bound(page, key)?;
    let upper = page_upper_bound(page, key)?;
    Ok((lower, upper))
}

fn exact_key_range_for_page(
    page_id: &KeyPageId,
    expected_entries: usize,
    page: &super::KeyPage,
    key: &EncodedKey,
) -> Result<(usize, usize)> {
    if page.is_empty() {
        return Err(Error::InvalidContract {
            message: format!("probe key page {page_id:?} is empty"),
        });
    }
    if page.len() != expected_entries {
        return Err(Error::InvalidContract {
            message: format!(
                "directory for {page_id:?} says {expected_entries} entries but page has {}",
                page.len()
            ),
        });
    }
    Ok((page_lower_bound(page, key)?, page_upper_bound(page, key)?))
}

fn page_lower_bound(page: &super::KeyPage, key: &EncodedKey) -> Result<usize> {
    let (mut low, mut high) = (0usize, page.len());
    while low < high {
        let middle = low.checked_add((high - low) / 2).ok_or(Error::Overflow {
            operation: "literal seek lower-bound midpoint",
        })?;
        if page.key(middle)? < key.as_bytes() {
            low = middle.checked_add(1).ok_or(Error::Overflow {
                operation: "literal seek lower-bound increment",
            })?;
        } else {
            high = middle;
        }
    }
    Ok(low)
}

fn page_upper_bound(page: &super::KeyPage, key: &EncodedKey) -> Result<usize> {
    let (mut low, mut high) = (0usize, page.len());
    while low < high {
        let middle = low.checked_add((high - low) / 2).ok_or(Error::Overflow {
            operation: "literal seek upper-bound midpoint",
        })?;
        if page.key(middle)? <= key.as_bytes() {
            low = middle.checked_add(1).ok_or(Error::Overflow {
                operation: "literal seek upper-bound increment",
            })?;
        } else {
            high = middle;
        }
    }
    Ok(low)
}
