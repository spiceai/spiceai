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

use super::{
    CoveredRowRef, CoveringPageStore, EncodedKey, Error, IndexDefinition, IndexRun,
    KeyDirectoryEntry, LiteralSeekSpan, PreparedLiteralSeek, Result, SchemaIdentity, SourceId,
};

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
}

/// Result of attempting to prove complete optional coverage.
#[derive(Clone, Debug)]
pub(crate) enum CoverageDecision {
    /// A complete pinned read view can use its covering pages.
    Complete(Arc<CoveringReadView>),
    /// Normal scanning must remain the chosen path.
    Unavailable(CoverageUnavailableReason),
}

/// Scan-visible source manifest and other state held by a covering read.
///
/// Step 05 wires this to `ScanView` and the existing visibility captures. The
/// manifest itself is already explicit here so callers cannot silently treat a
/// partially built catalog as coverage for a captured scan.
#[derive(Debug)]
pub(crate) struct CoveringReadView {
    catalog: Arc<IndexCatalog>,
    source_manifest: BTreeSet<SourceId>,
    query_schema: SchemaIdentity,
}

impl CoveringReadView {
    /// Capture an immutable catalog and the full source manifest a scan can read.
    pub(crate) fn new(
        catalog: Arc<IndexCatalog>,
        source_manifest: impl IntoIterator<Item = SourceId>,
        query_schema: SchemaIdentity,
    ) -> Self {
        Self {
            catalog,
            source_manifest: source_manifest.into_iter().collect(),
            query_schema,
        }
    }

    /// The atomically captured catalog.
    #[must_use]
    pub(crate) fn catalog(&self) -> &Arc<IndexCatalog> {
        &self.catalog
    }

    /// Every source a normal scan of this view could read.
    #[must_use]
    pub(crate) fn source_manifest(&self) -> &BTreeSet<SourceId> {
        &self.source_manifest
    }

    /// Query schema required for exact output adaptation.
    #[must_use]
    pub(crate) fn query_schema(&self) -> &SchemaIdentity {
        &self.query_schema
    }
}

/// Prove whether a captured view has complete, compatible index coverage.
///
/// This is deliberately conservative. A nonempty source must have an admitted
/// payload page and at least one run, and every required output/filter column
/// must be represented by the captured query schema. Later steps extend the
/// proof with existing visibility and transaction state before a planner can
/// select the optional path.
#[expect(
    clippy::unnecessary_wraps,
    reason = "the staged contract reserves typed errors for visibility validation before planning"
)]
pub(crate) fn try_cover(
    view: Arc<CoveringReadView>,
    definition: &IndexDefinition,
    required_columns: &[usize],
) -> Result<CoverageDecision> {
    if !view.catalog.definition().matches(definition) {
        return Ok(CoverageDecision::Unavailable(
            CoverageUnavailableReason::DefinitionMismatch,
        ));
    }
    if required_columns
        .iter()
        .any(|column| *column >= view.query_schema.schema().fields().len())
    {
        return Ok(CoverageDecision::Unavailable(
            CoverageUnavailableReason::UnsupportedTypeOrExpression,
        ));
    }

    for source_id in view.source_manifest() {
        let Some(source) = view.catalog.sources().get(source_id) else {
            return Ok(CoverageDecision::Unavailable(
                CoverageUnavailableReason::MissingSource,
            ));
        };
        if !source.schema().matches(definition.schema()) {
            return Ok(CoverageDecision::Unavailable(
                CoverageUnavailableReason::UnsupportedTypeOrExpression,
            ));
        }
        if source.row_count() == 0 {
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

/// Resumable state for page-bounded probing.
///
/// The actual page traversal is intentionally not implemented in this contract
/// step. The cursor's API distinguishes a temporary empty chunk from a proven
/// empty result so an executor cannot mistake a page boundary for exhaustion.
#[derive(Debug)]
pub(crate) struct ProbeCursor {
    _view: Arc<CoveringReadView>,
    _requests: Arc<[ProbeRequest]>,
}

impl ProbeCursor {
    /// Begin a bounded probe over a pinned view.
    #[must_use]
    pub(crate) fn new(view: Arc<CoveringReadView>, requests: Vec<ProbeRequest>) -> Self {
        Self {
            _view: view,
            _requests: requests.into(),
        }
    }

    /// Advance no farther than `max_rows` and `byte_budget` permit.
    ///
    /// The page-run executor lands in step 06. Returning a typed error here
    /// preserves the no-fallback-after-emission invariant; it is never reported
    /// as an empty or exhausted probe.
    pub(crate) async fn next_matches(
        &mut self,
        _max_rows: usize,
        _byte_budget: usize,
    ) -> Result<ProbeStep> {
        std::future::ready(()).await;
        Err(Error::NotImplemented {
            operation: "covering-index page probe",
        })
    }
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
        let ids = entries
            .iter()
            .map(|entry| entry.page().clone())
            .collect::<Vec<_>>();
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

        for (entry, lease) in entries.iter().zip(leases.iter()) {
            let (first, end) = exact_key_range(entry, lease.page(), key)?;
            if first == end {
                continue;
            }
            let matched = end.checked_sub(first).ok_or(Error::Overflow {
                operation: "literal seek entry count",
            })?;
            total = total.checked_add(matched).ok_or(Error::Overflow {
                operation: "literal seek total entry count",
            })?;
            spans.push(LiteralSeekSpan::single_page(
                run.source().clone(),
                run.run(),
                entry.page().clone(),
                first,
                end,
            )?);
        }
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
