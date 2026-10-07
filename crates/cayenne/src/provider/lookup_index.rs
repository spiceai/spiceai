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

//! Secondary index over a table's Vortex files, mapping an exact equality key
//! to the physical `(file, file-local row position)` addresses that hold it.
//!
//! Each key is a [`TieredIndex`] of immutable runs, and a run covers the files
//! it was built from. Every write builds a run from exactly the rows it writes,
//! as the Vortex sink reports them, and publishes it when the write returns,
//! just before the write becomes visible. That holds for every write: a CDC
//! append, a full-refresh overwrite, a compaction's rewrite, a memory-tier
//! checkpoint. So a file is indexed from the moment a scan can read it, and
//! nothing an append, a compaction or a refresh does leaves the index stale.
//! The one exception is an append of more than 2^20 rows,
//! which finishes its run in the background: its files are read in full until
//! the run publishes.
//! A file no run covers — written before a restart, or by a write whose run
//! could not be built — is read in full, and a lookup that meets one asks for
//! a background build that reads back only the files not yet covered, paced
//! so it takes a bounded share of a core. With the hidden
//! `SPICE_CAYENNE_INDEX_PERSISTENCE=enabled` switch (for testing), each run also
//! persists as a file under the table's `_lookup_index` directory, registered
//! in the metastore, so a reopened table loads its runs instead of reading its
//! files back; otherwise nothing is persisted.
//!
//! Declared with the acceleration's `indexes`, one key per entry:
//!
//! ```yaml
//! acceleration:
//!   engine: cayenne
//!   indexes:
//!     order_id: enabled
//!     '(tenant_id, service_id)': enabled
//! ```
//!
//! With no entry the whole module is inert. The runs' resident bytes, and every
//! run's working memory while it is built, are reserved against the query
//! memory pool; when the pool cannot fit them the run is dropped and its files
//! are read in full. This index is always safe to go without, which is what
//! lets it degrade rather than fail.
//!
//! Footguns this code depends on:
//!
//! * A run is keyed by file NAME, not path: a write's files move between its
//!   staging directory and the snapshot directories, which changes their path
//!   but not their name or any row position. Cayenne names every data file
//!   after the write that produced it (a UUID v7) plus a partition and a
//!   sequence, so no two files a table writes share a name, and a name the
//!   index covers always refers to the rows it was built from.
//! * Coverage is judged per file, against the view a scan pinned in the same
//!   fenced instant as its file list. A covered file is read only at its
//!   candidate positions; an uncovered one is read in full. An incomplete
//!   index therefore narrows less but never answers with a false empty.
//! * The index answers `key -> candidate positions` only. Every original
//!   predicate still runs, so a candidate that fails `Active = 1` is discarded
//!   by the scan's own filter rather than by the index.
//! * A key is matched by its stored value. A predicate that casts the COLUMN can
//!   hold for stored values other than the literal (`CAST(score AS BIGINT) = 5`
//!   holds for 5.2), so such predicates must never reach [`LookupIndexState::probe`].
//! * A runtime key set is usable only after its dynamic filter is complete and
//!   only through conjunctions. A list nested under `OR` or `CASE` is not a
//!   complete necessary condition and must not become a row selection.
//! * A write's run is only as good as the positions the sink reports: batches
//!   land in a file in the order the sink reports them. A read-back build takes
//!   every position from Vortex's own `row_idx()` instead, and
//!   [`verify_against_read_back`] diffs the two.
//! * A selection of N row positions is not a promise of N decoded rows. Vortex
//!   reads whole encoded segments and dictionaries that cover those positions.

use tracing::Instrument;
use tracing::instrument::WithSubscriber;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use super::memory_account::{CayenneMemoryAccount, LookupIndexReservation};
use crate::catalog::MetadataCatalog;
use crate::metadata::IndexRunRecord;
use arc_swap::ArcSwap;
use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::UInt64Type;
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, FieldRef};
use async_trait::async_trait;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion_common::{ScalarValue, Statistics};
use datafusion_datasource::{PartitionedFile, file_groups::FileGroup};
use datafusion_physical_expr::expressions::{
    Column, DynamicFilterPhysicalExpr, InListExpr, Literal,
};
use datafusion_physical_expr::utils::split_conjunction;
use datafusion_physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use futures::StreamExt;
use key_index::tiered::{Candidate, IndexRun, IndexView, RunBuilder, TieredIndex};
use key_index::{KeyEncoder, KeyField};
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use parking_lot::Mutex;
use vortex::VortexSessionDefault;
use vortex::array::VortexSessionExecute;
use vortex::arrow::ArrowSessionExt;
use vortex::buffer::Buffer;
use vortex::dtype::Nullability;
use vortex::file::OpenOptionsSessionExt;
use vortex::layout::layouts::row_idx::row_idx;
use vortex_datafusion::{
    VortexAccessPlan, VortexAccessPlanProvider, VortexRuntimeAccessPlanProvider, include_by_index,
};
use vortex_session::VortexSession;

/// Runtime index scans accept only small exact build-side key sets, and a
/// literal lookup at most this many key tuples.
pub(crate) const RUNTIME_INDEX_MAX_KEYS: usize = 2_048;
/// Candidate rows may scale with the table, but stay within a fixed memory bound.
const RUNTIME_INDEX_MIN_ROWS: usize = 2_048;
const RUNTIME_INDEX_MAX_ROWS: usize = 1_000_000;

/// Runs a key holds before a background merge folds a size tier together: a
/// lookup probes every run, so their number bounds its cost.
const MERGE_ABOVE_RUNS: usize = 8;

/// Name of the file-local row position column the read-back build projects.
const READ_BACK_POSITION_COLUMN: &str = "__cayenne_lookup_row_idx";

/// How much of what a lookup reads its index covers. `EXPLAIN` shows it as
/// counts (`uncovered_files` of `candidate_files`, or the batch counterparts
/// in memory mode); the table's counters keep it per lookup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Coverage {
    /// No file (in memory mode, no batch) the lookup reads is indexed yet, so
    /// every one is read in full.
    Unindexed,
    /// Some are narrowed to their candidate rows by the index, and the rest
    /// are read in full.
    Partial,
    /// Every one the lookup reads is indexed.
    Full,
}

impl Coverage {
    /// The coverage of a read that included indexed parts (`indexed`) and
    /// unindexed ones (`unindexed`). A read of nothing is fully covered.
    pub(crate) const fn of(indexed: bool, unindexed: bool) -> Self {
        match (indexed, unindexed) {
            (false, true) => Self::Unindexed,
            (true, true) => Self::Partial,
            _ => Self::Full,
        }
    }
}

/// Whether an index served a lookup, and how much of what it read the index
/// covered. `EXPLAIN` names the index only when it served the lookup
/// ([`LookupIndexExplain::served_by`]). `NotApplicable` is a planning
/// decision, not a probe, so it is never counted as one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LookupIndexExplainOutcome {
    NotApplicable,
    Probed(Coverage),
}

/// Why a lookup on an indexed table scanned instead of using its index,
/// reported in `EXPLAIN` beside `lookup_index=none`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LookupIndexScanReason {
    /// The filters give no indexed key a value for every one of its columns:
    /// an equality, an `IN` list, or an `OR` of equalities on each.
    NoKeyPinned,
    /// The columns' values combine into more key tuples than a lookup probes.
    TooManyKeys,
    /// The key tuples match more candidate rows than a lookup reads.
    TooManyCandidates,
    /// A value cannot be cast to its key column's type.
    ValueNotIndexable,
}

impl LookupIndexScanReason {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::NoKeyPinned => "no_key_pinned",
            Self::TooManyKeys => "too_many_keys",
            Self::TooManyCandidates => "too_many_candidates",
            Self::ValueNotIndexable => "value_not_indexable",
        }
    }
}

/// Stable, scan-local lookup-index evidence carried into `EXPLAIN`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LookupIndexExplain {
    pub(crate) shape: Option<String>,
    pub(crate) outcome: LookupIndexExplainOutcome,
    pub(crate) candidate_files: Option<usize>,
    /// Files the scan reads in full because no run covers them yet, which are
    /// part of `candidate_files`: all of them when the index covers none of
    /// what the lookup reads, `0` when it covers all of it.
    pub(crate) uncovered_files: Option<usize>,
    /// Files the index answered for (file mode): read at their candidate
    /// rows, or skipped because none holds the key. Not shown in `EXPLAIN`;
    /// with `uncovered_files` it decides the coverage of a merged decision.
    pub(crate) indexed_files: Option<usize>,
    /// Memory mode's counterparts of `candidate_files` and `uncovered_files`,
    /// over the in-memory batches a lookup reads.
    pub(crate) candidate_batches: Option<usize>,
    pub(crate) uncovered_batches: Option<usize>,
    pub(crate) candidate_rows: Option<u64>,
    /// Why the lookup scanned, when an indexed table's lookup did.
    pub(crate) reason: Option<LookupIndexScanReason>,
}

impl LookupIndexExplain {
    /// One decision for a scan that read several snapshots, over all of their
    /// files: `none` when no file read was indexed, `full` when every one
    /// was, else `partial`, with their file counts summed. A snapshot that
    /// read no file, or that the index did not apply to, leaves the others'
    /// decision as it is.
    #[must_use]
    pub(crate) fn merge(self, other: Self) -> Self {
        use LookupIndexExplainOutcome::{NotApplicable, Probed};
        let sum = |a: Option<usize>, b: Option<usize>| match (a, b) {
            (None, None) => None,
            (a, b) => Some(a.unwrap_or(0) + b.unwrap_or(0)),
        };
        let sum_files = sum(self.candidate_files, other.candidate_files);
        let sum_uncovered = sum(self.uncovered_files, other.uncovered_files);
        let sum_indexed = sum(self.indexed_files, other.indexed_files);
        let (outcome, reason) = match (self.outcome, other.outcome) {
            (Probed(_), Probed(_)) => (
                Probed(Coverage::of(
                    sum_indexed.unwrap_or(0) > 0,
                    sum_uncovered.unwrap_or(0) > 0,
                )),
                None,
            ),
            (Probed(_), NotApplicable) => (self.outcome, None),
            (NotApplicable, Probed(_)) => (other.outcome, None),
            (NotApplicable, NotApplicable) => (NotApplicable, self.reason.or(other.reason)),
        };
        // Every snapshot reports the selection's candidate rows, not its own.
        let sum_rows = self.candidate_rows.max(other.candidate_rows);
        Self {
            shape: self.shape.or(other.shape),
            outcome,
            candidate_files: sum_files,
            uncovered_files: sum_uncovered,
            indexed_files: sum_indexed,
            candidate_batches: sum(self.candidate_batches, other.candidate_batches),
            uncovered_batches: sum(self.uncovered_batches, other.uncovered_batches),
            candidate_rows: sum_rows,
            reason,
        }
    }

    /// The key whose index served the lookup, or `None` when no index did:
    /// none matched, or the one that matched declined (see `reason`).
    pub(crate) fn served_by(&self) -> Option<&str> {
        match self.outcome {
            LookupIndexExplainOutcome::Probed(_) => self.shape.as_deref(),
            LookupIndexExplainOutcome::NotApplicable => None,
        }
    }

    pub(crate) fn not_applicable(shape: Option<String>) -> Self {
        Self {
            shape,
            outcome: LookupIndexExplainOutcome::NotApplicable,
            candidate_files: None,
            uncovered_files: None,
            indexed_files: None,
            candidate_batches: None,
            uncovered_batches: None,
            candidate_rows: None,
            reason: None,
        }
    }

    /// An indexed table's lookup that scanned, and why.
    pub(crate) fn scanned(shape: Option<String>, reason: LookupIndexScanReason) -> Self {
        Self {
            reason: Some(reason),
            ..Self::not_applicable(shape)
        }
    }

    pub(crate) fn selection(
        shape: String,
        outcome: LookupIndexExplainOutcome,
        candidate_files: Option<usize>,
        candidate_rows: u64,
    ) -> Self {
        Self {
            shape: Some(shape),
            outcome,
            candidate_files,
            uncovered_files: None,
            indexed_files: None,
            candidate_batches: None,
            uncovered_batches: None,
            candidate_rows: Some(candidate_rows),
            reason: None,
        }
    }
}

/// One key the table is indexed on: the columns of one `indexes` entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct KeySpec {
    columns: Vec<String>,
    /// `order_id` or `(tenant_id, service_id)` — the `shape` metric dimension.
    label: String,
}

impl KeySpec {
    /// The key over `columns`, or `None` for an entry that names no column.
    pub(crate) fn new(columns: Vec<String>) -> Option<Self> {
        let label = match columns.as_slice() {
            [] => return None,
            [column] => column.clone(),
            columns => format!("({})", columns.join(", ")),
        };
        Some(Self { columns, label })
    }

    /// One key per distinct column set, in the order first seen.
    pub(crate) fn from_indexes(indexes: &[Vec<String>]) -> Vec<Self> {
        let mut specs: Vec<Self> = Vec::new();
        for columns in indexes {
            if let Some(spec) = Self::new(columns.clone())
                && !specs
                    .iter()
                    .any(|existing| existing.columns == spec.columns)
            {
                specs.push(spec);
            }
        }
        specs
    }

    pub(crate) fn columns(&self) -> &[String] {
        &self.columns
    }

    /// This key over `columns`, the table's spelling of its columns, keeping
    /// the label `indexes` gave it.
    pub(crate) fn with_columns(self, columns: Vec<String>) -> Self {
        Self {
            columns,
            label: self.label,
        }
    }

    pub(crate) fn label(&self) -> &str {
        &self.label
    }
}

/// Which file set of a snapshot a listing saw: the table's directory generation
/// and listing-cache epoch, sampled before listing, and a digest of the
/// protected snapshots whose files are part of the set. One of them moves
/// whenever that set of files changes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct FileSetVersion {
    pub(crate) dir_generation: u64,
    pub(crate) listing_epoch: u64,
    /// A digest of the protected snapshots whose files are part of the set.
    pub(crate) protected: u64,
}

/// A data file of the indexed snapshot, recorded exactly as the scan lists it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IndexedFile {
    pub(crate) path: String,
}

/// One key column, resolved against the table schema.
#[derive(Clone, Debug)]
pub(crate) struct KeyColumn {
    /// The column's name in the table schema.
    pub(crate) name: String,
    /// The column's stored type. Build and probe both cast to it, so an encoding
    /// never depends on how a value reached the index.
    pub(crate) data_type: DataType,
    /// Whether the stored column admits nulls.
    nullable: bool,
}

impl KeyColumn {
    /// Resolves a configured key column: an exact name wins, then a unique
    /// case-insensitive match. Two case-insensitive candidates are an error
    /// rather than a guess, because building from one column and probing with
    /// the other would miss rows.
    pub(crate) fn resolve(schema: &arrow_schema::Schema, configured: &str) -> Result<Self, String> {
        let field = if let Ok(field) = schema.field_with_name(configured) {
            field
        } else {
            let mut candidates = schema
                .fields()
                .iter()
                .filter(|f| f.name().eq_ignore_ascii_case(configured));
            let field = candidates
                .next()
                .ok_or_else(|| format!("key column '{configured}' is not in the table schema"))?;
            if candidates.next().is_some() {
                return Err(format!(
                    "key column '{configured}' matches more than one table column when case is ignored"
                ));
            }
            field.as_ref()
        };
        Ok(Self {
            name: field.name().clone(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
        })
    }

    /// The field of this column as a scan of the table's files returns it.
    fn stored_field(&self) -> FieldRef {
        Arc::new(Field::new(
            &self.name,
            self.data_type.clone(),
            self.nullable,
        ))
    }
}

/// Casts `array` to `data_type`, or returns it unchanged when it already matches.
pub(crate) fn cast_to(array: &ArrayRef, data_type: &DataType) -> Result<ArrayRef, String> {
    if array.data_type() == data_type {
        return Ok(Arc::clone(array));
    }
    arrow::compute::cast(array, data_type)
        .map_err(|e| format!("cast {} -> {data_type}: {e}", array.data_type()))
}

/// A data file's name: its identity across the moves and hardlinks between a
/// write's staging directory and the snapshot directories, which change its
/// path but not its name or any row position. See the module's note on file
/// names.
pub(crate) fn file_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

/// The type a key column is encoded in: its stored type, or a dictionary's
/// value type (see [`key_index::key_type`]).
fn encoded_type(data_type: &DataType) -> DataType {
    key_index::key_type(data_type).clone()
}

/// Whether a column of `data_type` can be indexed. The index encodes a key
/// by its stored value, so every type it accepts must have a total,
/// value-preserving byte order; nested types do not.
pub(crate) fn supported_key_type(data_type: &DataType) -> Result<(), String> {
    KeyEncoder::new(vec![KeyField::new(encoded_type(data_type), true)])
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Every key's shape, in spec order.
type Shapes = Arc<Vec<Arc<Shape>>>;

/// Every key's shape under a table schema, from
/// [`LookupIndexState::shapes_for`].
pub(crate) struct KeyShapes(Vec<Shape>);

/// One indexed key: its resolved columns and its tiered index, whose runs
/// cover the files the table's writes and read-back builds indexed.
struct Shape {
    label: String,
    columns: Vec<KeyColumn>,
    /// Per column, the type the key is encoded in.
    encoded_types: Vec<DataType>,
    encoder: KeyEncoder,
    /// Shared with the shape that replaces this one when a schema change
    /// leaves the key's encoding as it was (see
    /// [`LookupIndexState::adopt_shapes`]).
    index: Arc<TieredIndex>,
}

impl Shape {
    fn new(
        spec: &KeySpec,
        schema: &arrow_schema::Schema,
        word_bits: Option<u32>,
    ) -> Result<Self, String> {
        let columns = spec
            .columns()
            .iter()
            .map(|name| KeyColumn::resolve(schema, name))
            .collect::<Result<Vec<_>, _>>()?;
        let encoded_types: Vec<DataType> = columns
            .iter()
            .map(|column| encoded_type(&column.data_type))
            .collect();
        let encoder = KeyEncoder::new(
            columns
                .iter()
                .zip(&encoded_types)
                .map(|(column, data_type)| KeyField::new(data_type.clone(), column.nullable))
                .collect(),
        )
        .map_err(|e| format!("key {} cannot be indexed: {e}", spec.label()))?;
        let encoder = match word_bits {
            Some(bits) => encoder.with_word_bits(bits),
            None => encoder,
        };
        Ok(Self {
            label: spec.label().to_string(),
            columns,
            encoded_types,
            index: Arc::new(TieredIndex::new(encoder.clone())),
            encoder,
        })
    }

    /// Identifies this key's persisted runs: its resolved columns, the words its encoder
    /// gives keys (its encoded types, their nullability and how keys become
    /// words) and the persisted format. A reopened table therefore reads back
    /// only runs written by the same key, encoding and format, and deletes the
    /// rest: a key column relaxed to nullable in place changes every key's word.
    /// The hash is the identity, so it is 128 bits (see
    /// [`hash_index::hash_key_128`]).
    fn persisted_key(&self) -> u128 {
        let mut descriptor = Vec::new();
        descriptor.extend_from_slice(&key_index::persist::VERSION.to_le_bytes());
        descriptor.extend_from_slice(&self.encoder.word_identity().to_le_bytes());
        descriptor.extend_from_slice(&(self.columns.len() as u64).to_le_bytes());
        for column in &self.columns {
            descriptor.extend_from_slice(&(column.name.len() as u64).to_le_bytes());
            descriptor.extend_from_slice(column.name.as_bytes());
        }
        hash_index::hash_key_128(&descriptor)
    }

    /// The directory this key's persisted runs live in.
    fn persisted_dir(&self) -> String {
        format!("{:032x}", self.persisted_key())
    }

    /// A builder for one run of this key.
    fn run_builder(&self) -> RunBuilder {
        RunBuilder::new(self.encoder.clone())
    }

    /// The key columns of a written or read-back batch, in the encoded types.
    fn key_columns(&self, batch: &RecordBatch) -> Result<Vec<ArrayRef>, String> {
        self.cast_key_columns(&self.raw_key_columns(batch)?)
    }

    /// The key columns of a batch as written, before any cast: a reference
    /// to each column, not a copy.
    fn raw_key_columns(&self, batch: &RecordBatch) -> Result<Vec<ArrayRef>, String> {
        self.columns
            .iter()
            .map(|column| {
                batch
                    .column_by_name(&column.name)
                    .map(Arc::clone)
                    .ok_or_else(|| format!("key column '{}' is not in the batch", column.name))
            })
            .collect()
    }

    /// [`Self::raw_key_columns`] cast to the encoded types.
    fn cast_key_columns(&self, raw: &[ArrayRef]) -> Result<Vec<ArrayRef>, String> {
        raw.iter()
            .zip(&self.encoded_types)
            .map(|(array, data_type)| cast_to(array, data_type))
            .collect()
    }

    /// The encoded keys of scalar tuples, one per tuple in `columns` order:
    /// `None` for a tuple with a NULL value, which no equality predicate
    /// matches. `Err` when a value does not cast exactly to its column's
    /// stored type, so the index cannot answer and the scan must run.
    fn encode_keys(&self, keys: &[Vec<ScalarValue>]) -> Result<Vec<Option<Vec<u8>>>, String> {
        let mut columns: Vec<Vec<ScalarValue>> = (0..self.columns.len())
            .map(|_| Vec::with_capacity(keys.len()))
            .collect();
        for key in keys {
            if key.len() != self.columns.len() {
                return Err(format!(
                    "a key of {} values for {} key columns",
                    key.len(),
                    self.columns.len()
                ));
            }
            for (at, (value, column)) in key.iter().zip(&self.columns).enumerate() {
                // As a write does: to the stored type, strictly.
                let value = value
                    .cast_to(&column.data_type)
                    .map_err(|e| format!("cast {value} to {}: {e}", column.data_type))?;
                columns[at].push(value);
            }
        }
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let arrays = columns
            .into_iter()
            .zip(&self.encoded_types)
            .map(|(values, data_type)| {
                let array = ScalarValue::iter_to_array(values).map_err(|e| e.to_string())?;
                cast_to(&array, data_type)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let bound = self.encoder.bind(&arrays).map_err(|e| e.to_string())?;
        Ok((0..keys.len())
            .map(|row| {
                (!bound.has_null(row)).then(|| {
                    let mut key = Vec::new();
                    bound.encode_row(row, &mut key);
                    key
                })
            })
            .collect())
    }
}

/// What a scan pins: every key's view, taken together at one instant.
///
/// A view answers only for the files it covers; a file it does not cover
/// must be read in full. That per-file rule is what keeps the index usable
/// while files come and go.
pub(crate) struct LookupIndexView {
    /// The keys the views were taken of. A probe encodes its keys with these,
    /// never with the table's current shapes, so a key's encoding and the
    /// runs it is looked up in always agree.
    shapes: Shapes,
    views: Vec<IndexView>,
    /// Rows the runs hold, for the runtime probe's row bound.
    rows: usize,
}

impl LookupIndexView {
    /// Every key's current view of `shapes`, taken together.
    fn of(shapes: Shapes) -> Self {
        let views: Vec<IndexView> = shapes.iter().map(|shape| shape.index.view()).collect();
        let rows = views
            .first()
            .map_or(0, |view| view.run_list().iter().map(|run| run.len()).sum());
        Self {
            shapes,
            views,
            rows,
        }
    }

    /// Whether every key's view covers the file `path` names.
    pub(crate) fn covers(&self, path: &str) -> bool {
        let name = file_name(path);
        self.views.iter().all(|view| view.covers(name))
    }

    /// Candidates of `keys` (encoded; `None` matches nothing) under key
    /// `shape`, grouped by file name, each file's positions sorted and
    /// distinct; only those in the files `only` names, when it is given.
    /// `None` when more than `max_rows` would be selected: callers never use
    /// a partial answer.
    fn probe_keys(
        &self,
        shape: usize,
        keys: &[Option<Vec<u8>>],
        max_rows: Option<usize>,
        only: Option<&HashSet<&str>>,
    ) -> Option<ProbeHit> {
        let present: Vec<&[u8]> = keys.iter().flatten().map(Vec::as_slice).collect();
        let mut by_file: HashMap<String, Vec<u64>> = HashMap::new();
        let mut selected = 0_usize;
        let mut over = false;
        self.views[shape].candidates_batch(&present, |_, Candidate { file, position }| {
            if only.is_some_and(|only| !only.contains(file)) {
                return;
            }
            selected += 1;
            over |= max_rows.is_some_and(|limit| selected > limit);
            if !over {
                by_file.entry(file.to_string()).or_default().push(position);
            }
        });
        if over {
            return None;
        }
        let mut rows = 0;
        for positions in by_file.values_mut() {
            positions.sort_unstable();
            positions.dedup();
            rows += positions.len();
        }
        Some(ProbeHit {
            shape: self.shapes[shape].label.clone(),
            per_file: by_file,
            rows,
        })
    }
}

struct ProbeHit {
    shape: String,
    /// Candidate positions by file name.
    per_file: HashMap<String, Vec<u64>>,
    rows: usize,
}

/// A resolved row selection awaiting the scan's own file list. Nothing is
/// applied until [`Self::restrict`] sees that list. Cloned for each snapshot
/// a scan reads: it names files, so each clone narrows only its own.
#[derive(Clone)]
pub(crate) struct LookupSelection {
    state: Arc<LookupIndexState>,
    index: Arc<LookupIndexView>,
    shape: String,
    per_file: HashMap<String, Vec<u64>>,
    rows: usize,
}

/// The result of probing a fully pinned lookup-index key.
pub(crate) enum LookupProbe {
    Selection(LookupSelection),
    Fallback(LookupIndexExplain),
}

impl LookupSelection {
    /// The label of the key it probed.
    pub(crate) fn shape(&self) -> &str {
        &self.shape
    }

    /// Narrows a scan's file groups and returns the access-plan provider that
    /// carries the candidate positions into the Vortex scan, and the decision
    /// for `EXPLAIN`, which the caller merges over every snapshot the scan
    /// reads and records once (see [`LookupIndexState::record_lookup`]).
    ///
    /// Each file is judged on its own. A file the pinned view covers is kept
    /// only when it holds a candidate, and then read at those positions. A
    /// file it does not cover is kept and read in full. `table_plans` is the
    /// provider the scan would otherwise attach, which carries the table's
    /// position-delete vectors. Also returns whether any scan file was not
    /// covered, so the caller can have it indexed.
    pub(crate) fn restrict(
        self,
        file_groups: Vec<FileGroup>,
        table_plans: Arc<dyn VortexAccessPlanProvider>,
    ) -> (
        Vec<FileGroup>,
        Option<Arc<dyn VortexAccessPlanProvider>>,
        LookupIndexExplain,
        bool,
    ) {
        let mut uncovered = 0_usize;
        let mut covered = 0_usize;
        let file_groups: Vec<FileGroup> = file_groups
            .into_iter()
            .filter_map(|group| {
                let files: Vec<PartitionedFile> = group
                    .into_inner()
                    .into_iter()
                    .filter(|file| {
                        let path: &str = file.object_meta.location.as_ref();
                        if self.index.covers(path) {
                            covered += 1;
                            self.per_file.contains_key(file_name(path))
                        } else {
                            uncovered += 1;
                            true
                        }
                    })
                    .collect();
                (!files.is_empty()).then(|| FileGroup::new(files))
            })
            .collect();
        let coverage = Coverage::of(covered > 0, uncovered > 0);
        let candidate_files: usize = file_groups.iter().map(FileGroup::len).sum();
        let candidate_rows = match coverage {
            Coverage::Unindexed => None,
            // Every file is indexed and none holds the key: nothing is read.
            _ if candidate_files == 0 => Some(0),
            _ => Some(u64::try_from(self.rows).unwrap_or(u64::MAX)),
        };
        let explain = LookupIndexExplain {
            shape: Some(self.shape),
            outcome: LookupIndexExplainOutcome::Probed(coverage),
            candidate_files: Some(candidate_files),
            uncovered_files: Some(uncovered),
            indexed_files: Some(covered),
            candidate_batches: None,
            uncovered_batches: None,
            candidate_rows,
            reason: None,
        };
        // Nothing to narrow when no file read is indexed, or none is left.
        if covered == 0 || candidate_files == 0 {
            return (file_groups, None, explain, uncovered > 0);
        }
        let provider = LookupAccessPlanProvider {
            state: self.state,
            index: self.index,
            selections: self.per_file,
            table: table_plans,
        };
        (
            file_groups,
            Some(Arc::new(provider)),
            explain,
            uncovered > 0,
        )
    }
}

/// Per-file row selections handed to the Vortex scan, composed with the
/// table's own per-file access plans.
///
/// A file carries exactly one access plan, and position-delete vectors travel
/// in it. So the lookup's candidate positions are intersected with whatever
/// the table's provider would have attached — a deleted candidate is never
/// selected — instead of replacing it.
struct LookupAccessPlanProvider {
    state: Arc<LookupIndexState>,
    index: Arc<LookupIndexView>,
    /// Candidate positions by file name.
    selections: HashMap<String, Vec<u64>>,
    table: Arc<dyn VortexAccessPlanProvider>,
}

/// Runtime row selection derived from a completed hash-join dynamic filter.
pub(crate) struct RuntimeLookupSelection {
    index: Arc<LookupIndexView>,
    /// The ready-made plan for each covered file holding a candidate row, by
    /// file name, so a file open shares it instead of copying the positions.
    plans: HashMap<String, Arc<VortexAccessPlan>>,
    /// The plan for a covered file that holds no candidate row.
    empty: Arc<VortexAccessPlan>,
}

impl RuntimeLookupSelection {
    /// Whether the file `path` names may hold a row of the selection: a file
    /// the index does not cover is read as planned, and a covered one only
    /// when it holds a candidate.
    pub(crate) fn may_hold(&self, path: &str) -> bool {
        !self.index.covers(path) || self.plans.contains_key(file_name(path))
    }

    fn new(index: Arc<LookupIndexView>, per_file: HashMap<String, Vec<u64>>) -> Self {
        let plans = per_file
            .into_iter()
            .map(|(name, positions)| {
                let plan = VortexAccessPlan::default()
                    .with_selection(include_by_index(&Buffer::from(positions)));
                (name, Arc::new(plan))
            })
            .collect();
        Self {
            index,
            plans,
            empty: Arc::new(
                VortexAccessPlan::default().with_selection(include_by_index(&Buffer::empty())),
            ),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RuntimeLookupFilterIdentity {
    expression_id: u64,
    generation: u64,
    spec: usize,
}

/// What a runtime probe decided.
enum RuntimeProbe {
    /// A selection for the covered files. `uncovered` when some scan file is
    /// not covered, which is read as planned and should be indexed.
    Selection {
        selection: RuntimeLookupSelection,
        uncovered: bool,
    },
    /// The index cannot answer these keys within its bounds.
    Declined,
    /// No run covers any of the scan's files; a build may make the next
    /// lookup usable.
    IndexUnusable,
}

/// The runtime selection for one filter identity. The first file opener
/// resolves the cell; the others await it rather than probing again.
type RuntimeLookupCell = Arc<tokio::sync::OnceCell<Option<Arc<RuntimeLookupSelection>>>>;

/// Adds exact dynamic-filter keys to the table's ordinary per-file access plan.
///
/// The provider is installed while the physical scan is built, but probes the
/// lookup index only when Vortex opens a file. At that point a collect-left hash
/// join may have populated its `DynamicFilterPhysicalExpr` with an exact `IN`
/// list. The expression generation prevents an early, unresolved filter from
/// becoming a permanent decision for later file openers.
///
/// The index is the view the scan pinned, captured in the same fenced instant
/// as its files. A join can run long after it was planned, and later writes may
/// publish newer runs meanwhile; the scan still probes the view that matches
/// what it reads.
pub(crate) struct DynamicLookupAccessPlanProvider {
    state: Arc<LookupIndexState>,
    /// The view the scan pinned.
    index: Arc<LookupIndexView>,
    /// Every file the scan reads, so the probe's outcome is decided once,
    /// before any file opens.
    scan_files: Arc<[ObjectMeta]>,
    request_build: Option<Arc<dyn Fn() + Send + Sync>>,
    selection: Mutex<Option<(RuntimeLookupFilterIdentity, RuntimeLookupCell)>>,
}

impl DynamicLookupAccessPlanProvider {
    pub(crate) fn new(
        state: Arc<LookupIndexState>,
        index: Arc<LookupIndexView>,
        scan_files: Arc<[ObjectMeta]>,
        request_build: Option<Arc<dyn Fn() + Send + Sync>>,
    ) -> Self {
        Self {
            state,
            index,
            scan_files,
            request_build,
            selection: Mutex::default(),
        }
    }

    /// The selection a completed dynamic filter in `predicate` resolves to,
    /// or `None` when no index can answer it.
    pub(crate) async fn selection(
        &self,
        predicate: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Option<Arc<RuntimeLookupSelection>> {
        self.resolve(predicate).await
    }

    /// The selection for the scan's completed dynamic filter, probed at most
    /// once per filter generation.
    ///
    /// Every file open calls this, so the cache is keyed on the filter's identity
    /// alone and checked before any key is read. The lock is held only to find
    /// or install the cell; the probe itself runs on the blocking pool.
    async fn resolve(
        &self,
        predicate: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Option<Arc<RuntimeLookupSelection>> {
        let predicate = predicate?;
        let (spec, dynamic) = self
            .state
            .specs
            .iter()
            .enumerate()
            .find_map(|(spec, key)| {
                dynamic_in_list_expr(predicate, &key.columns).map(|dynamic| (spec, dynamic))
            })?;
        dynamic.wait_complete().await;
        let identity = RuntimeLookupFilterIdentity {
            expression_id: dynamic.expression_id()?,
            generation: dynamic.snapshot_generation(),
            spec,
        };
        let cell = {
            let mut cache = self.selection.lock();
            match cache.as_ref() {
                Some((cached, cell)) if *cached == identity => Arc::clone(cell),
                _ => {
                    let cell = RuntimeLookupCell::default();
                    *cache = Some((identity, Arc::clone(&cell)));
                    cell
                }
            }
        };
        cell.get_or_init(|| self.probe(dynamic, identity))
            .await
            .clone()
    }

    async fn probe(
        &self,
        dynamic: &DynamicFilterPhysicalExpr,
        identity: RuntimeLookupFilterIdentity,
    ) -> Option<Arc<RuntimeLookupSelection>> {
        let current = dynamic.current().ok()?;
        // Keys read from a newer generation than the identity would be cached
        // under the wrong one.
        if dynamic.snapshot_generation() != identity.generation {
            return None;
        }
        let state = Arc::clone(&self.state);
        let index = Arc::clone(&self.index);
        let scan_files = Arc::clone(&self.scan_files);
        let probed = tokio::task::spawn_blocking(move || {
            let spec = &state.specs[identity.spec];
            let Some(keys) = in_list_keys(&current, &spec.columns) else {
                // Non-literal or more than `RUNTIME_INDEX_MAX_KEYS` keys: no
                // index could answer it.
                state.record_runtime_fallback();
                return RuntimeProbe::Declined;
            };
            state.probe_runtime_filter(identity.spec, &index, &scan_files, &keys)
        })
        .await;
        match probed {
            Ok(RuntimeProbe::Selection {
                selection,
                uncovered,
            }) => {
                if uncovered {
                    self.request_build();
                }
                Some(Arc::new(selection))
            }
            Ok(RuntimeProbe::Declined) => None,
            Ok(RuntimeProbe::IndexUnusable) => {
                self.request_build();
                None
            }
            Err(error) => {
                tracing::debug!(%error, "Runtime secondary index probe did not complete; scanning instead");
                None
            }
        }
    }

    fn request_build(&self) {
        if let Some(request_build) = &self.request_build {
            request_build();
        }
    }
}

impl std::fmt::Debug for DynamicLookupAccessPlanProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamicLookupAccessPlanProvider")
            .field("files", &self.scan_files.len())
            .field("resolved", &self.selection.lock().is_some())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for LookupAccessPlanProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LookupAccessPlanProvider")
            .field("files", &self.selections.len())
            .field("table", &self.table)
            .finish_non_exhaustive()
    }
}

/// Finds a dynamic filter with a matching `IN` shape without materializing keys.
/// Bounds and hash-membership expressions deliberately decline: only an
/// `InListExpr` is an enumerable key set for the lookup index. Traversal is
/// limited to conjunctions, where selecting candidates for one conjunct is
/// conservative; extracting a list from `OR`, `CASE`, or another expression
/// could omit rows.
///
/// `DataFusion` represents a multi-column join key as one `struct(...) IN`
/// expression. Extracting each struct literal preserves the build rows' tuple
/// correlation; independent per-column lists must never be combined here.
fn dynamic_in_list_expr<'a>(
    expr: &'a Arc<dyn PhysicalExpr>,
    columns: &[String],
) -> Option<&'a DynamicFilterPhysicalExpr> {
    split_conjunction(expr).into_iter().find_map(|conjunct| {
        let dynamic = conjunct.downcast_ref::<DynamicFilterPhysicalExpr>()?;
        let current = dynamic.current().ok()?;
        matching_in_list(&current, columns)?;
        Some(dynamic)
    })
}

fn matching_in_list<'a>(
    expr: &'a Arc<dyn PhysicalExpr>,
    columns: &[String],
) -> Option<&'a InListExpr> {
    split_conjunction(expr).into_iter().find_map(|conjunct| {
        conjunct
            .downcast_ref::<InListExpr>()
            .filter(|in_list| !in_list.negated() && in_list_matches_columns(in_list, columns))
    })
}

fn in_list_keys(expr: &Arc<dyn PhysicalExpr>, columns: &[String]) -> Option<Vec<Vec<ScalarValue>>> {
    let in_list = matching_in_list(expr, columns)?;
    collect_runtime_keys(in_list.list(), columns.len())
}

/// Retains only bounded distinct keys. An oversized list declines the entire
/// lookup without reading the remaining literals or exposing a partial selection.
fn collect_runtime_keys<'a>(
    values: impl IntoIterator<Item = &'a Arc<dyn PhysicalExpr>>,
    num_columns: usize,
) -> Option<Vec<Vec<ScalarValue>>> {
    let mut keys = HashSet::new();
    for value in values {
        let scalar = value.downcast_ref::<Literal>()?.value();
        let key = if num_columns == 1 {
            if scalar.is_null() {
                continue;
            }
            vec![scalar.clone()]
        } else {
            let ScalarValue::Struct(struct_array) = scalar else {
                return None;
            };
            if struct_array.len() != 1 || struct_array.num_columns() != num_columns {
                return None;
            }
            if struct_array.is_null(0) {
                continue;
            }
            let key = struct_array
                .columns()
                .iter()
                .map(|array| ScalarValue::try_from_array(array, 0).ok())
                .collect::<Option<Vec<_>>>()?;
            if key.iter().any(ScalarValue::is_null) {
                continue;
            }
            key
        };
        if keys.insert(key) && keys.len() > RUNTIME_INDEX_MAX_KEYS {
            return None;
        }
    }
    Some(keys.into_iter().collect())
}

fn in_list_matches_columns(in_list: &InListExpr, columns: &[String]) -> bool {
    match columns {
        [column] => in_list
            .expr()
            .downcast_ref::<Column>()
            .is_some_and(|candidate| candidate.name() == column),
        [] => false,
        columns => in_list
            .expr()
            .downcast_ref::<ScalarFunctionExpr>()
            .filter(|function| function.name().eq_ignore_ascii_case("struct"))
            .is_some_and(|function| {
                function.args().len() == columns.len()
                    && function.args().iter().zip(columns).all(|(arg, column)| {
                        arg.downcast_ref::<Column>()
                            .is_some_and(|candidate| candidate.name() == column)
                    })
            }),
    }
}

impl VortexAccessPlanProvider for LookupAccessPlanProvider {
    fn access_plan_for_file(&self, file: &PartitionedFile) -> Option<Arc<VortexAccessPlan>> {
        let path: &str = file.object_meta.location.as_ref();
        let table_plan = self.table.access_plan_for_file(file);
        if !self.index.covers(path) {
            // A file the index does not cover is read exactly as the table
            // would read it.
            return table_plan;
        }
        // `restrict` kept a covered file only when it holds a candidate.
        let candidates = self
            .selections
            .get(file_name(path))
            .map_or(&[][..], Vec::as_slice);
        let selected = VortexAccessPlan::default()
            .with_selection(include_by_index(&Buffer::copy_from(candidates)));
        self.state
            .counters
            .access_plans_attached
            .fetch_add(1, Ordering::Relaxed);
        Some(Arc::new(match table_plan {
            Some(table_plan) => selected.intersect(&table_plan),
            None => selected,
        }))
    }

    fn adjust_statistics(&self, object: &ObjectMeta, statistics: Statistics) -> Statistics {
        self.table.adjust_statistics(object, statistics)
    }
}

#[async_trait]
impl VortexRuntimeAccessPlanProvider for DynamicLookupAccessPlanProvider {
    /// The opener intersects this with the file's planning-time plan, which
    /// carries its position-delete vectors, so a deleted row is never selected.
    async fn runtime_access_plan_for_file(
        &self,
        file: &PartitionedFile,
        predicate: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Option<Arc<VortexAccessPlan>> {
        let selection = self.resolve(predicate).await?;
        let path: &str = file.object_meta.location.as_ref();
        // A file the pinned view does not cover is read as planned.
        if !selection.index.covers(path) {
            return None;
        }
        self.state
            .counters
            .access_plans_attached
            .fetch_add(1, Ordering::Relaxed);
        Some(Arc::clone(
            selection
                .plans
                .get(file_name(path))
                .unwrap_or(&selection.empty),
        ))
    }
}

/// Probe and build accounting for one table. The probe outcomes also go to
/// OpenTelemetry; these process-local counters are what a correctness check can
/// assert on to prove a query really used row selection instead of silently
/// scanning.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LookupIndexCounters {
    /// Probes where none of the files the scan reads is indexed yet (coverage
    /// `none`).
    pub none: u64,
    /// Probes where some files the scan reads are indexed and the rest are
    /// read in full (coverage `partial`).
    pub partial: u64,
    /// Probes where every file the scan reads is indexed (coverage `full`),
    /// including those whose key no file holds, which read nothing.
    pub full: u64,
    /// Runtime key sets declined because their shape or cost cannot be bounded.
    pub runtime_fallback: u64,
    /// Candidate files summed over probes that narrowed a scan.
    pub candidate_files: u64,
    /// Candidate row positions summed over probes that narrowed a scan.
    pub candidate_rows: u64,
    /// Files that were actually handed a Vortex row selection.
    pub access_plans_attached: u64,
    /// Resident bytes of the published index, as reserved against the table's
    /// `DataFusion` memory pool. Zero when nothing is published.
    pub index_bytes: u64,
    /// Background builds started from a lookup.
    pub builds_started: u64,
    /// Builds, background or write-time, whose index was published.
    pub builds_published: u64,
    /// Builds that ended without an index: refused by the memory pool, failed,
    /// or overtaken by a newer index.
    pub builds_unpublished: u64,
}

/// A problem that keeps recurring is logged at `warn` the first time, then
/// at `debug` until [`Self::reset`], so a table that keeps hitting it does not
/// flood the log.
#[derive(Debug, Default)]
pub(crate) struct WarnOnce(AtomicBool);

impl WarnOnce {
    /// Logs `message` for `table`, at `warn` unless it already has since the
    /// last reset.
    pub(crate) fn report(&self, table: &str, message: &str) {
        if self.0.swap(true, Ordering::Relaxed) {
            tracing::debug!(table = %table, "{message}");
        } else {
            tracing::warn!(table = %table, "{message}");
        }
    }

    /// Whether it has warned since the last reset.
    #[cfg(test)]
    pub(crate) fn warned(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }

    /// The next report warns again.
    pub(crate) fn reset(&self) {
        self.0.store(false, Ordering::Relaxed);
    }
}

#[derive(Default)]
pub(crate) struct Counters {
    none: AtomicU64,
    partial: AtomicU64,
    full: AtomicU64,
    runtime_fallback: AtomicU64,
    candidate_files: AtomicU64,
    pub(crate) candidate_rows: AtomicU64,
    access_plans_attached: AtomicU64,
    builds_started: AtomicU64,
    pub(crate) builds_published: AtomicU64,
    pub(crate) builds_unpublished: AtomicU64,
}

impl Counters {
    pub(crate) fn snapshot(&self, index_bytes: u64) -> LookupIndexCounters {
        LookupIndexCounters {
            none: self.none.load(Ordering::Relaxed),
            partial: self.partial.load(Ordering::Relaxed),
            full: self.full.load(Ordering::Relaxed),
            runtime_fallback: self.runtime_fallback.load(Ordering::Relaxed),
            candidate_files: self.candidate_files.load(Ordering::Relaxed),
            candidate_rows: self.candidate_rows.load(Ordering::Relaxed),
            access_plans_attached: self.access_plans_attached.load(Ordering::Relaxed),
            index_bytes,
            builds_started: self.builds_started.load(Ordering::Relaxed),
            builds_published: self.builds_published.load(Ordering::Relaxed),
            builds_unpublished: self.builds_unpublished.load(Ordering::Relaxed),
        }
    }

    fn record(&self, coverage: Coverage) {
        let counter = match coverage {
            Coverage::Unindexed => &self.none,
            Coverage::Partial => &self.partial,
            Coverage::Full => &self.full,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

/// The shortest pause between the end of one background build and the start of
/// the next.
const MIN_BUILD_INTERVAL: Duration = Duration::from_secs(1);

/// A background build waits at least this many times as long as the previous one
/// took, so however often a table's files change, rebuilding its index takes a
/// bounded share of one core.
const BUILD_INTERVAL_MULTIPLE: u32 = 10;

/// The longest a table waits between background builds, however many in a row
/// ended without an index.
const MAX_BUILD_INTERVAL: Duration = Duration::from_hours(1);

/// When a table's one background build may run.
#[derive(Debug, Default)]
struct BuildSchedule {
    in_flight: bool,
    /// No background build starts before this instant.
    not_before: Option<Instant>,
    /// Builds in a row that ended without publishing an index. Each one doubles
    /// the pause, so a table the pool cannot fit is not re-read on every lookup.
    unpublished: u32,
}

impl BuildSchedule {
    /// Whether a background build may start at `now`.
    fn can_start(&self, now: Instant) -> bool {
        !self.in_flight && self.not_before.is_none_or(|not_before| now >= not_before)
    }

    /// Records a build that ran for `took` and ended at `now`, and sets when the
    /// next may start.
    fn finished(&mut self, now: Instant, took: Duration, published: bool) {
        self.in_flight = false;
        self.unpublished = if published {
            0
        } else {
            self.unpublished.saturating_add(1)
        };
        let interval = MIN_BUILD_INTERVAL.max(took.saturating_mul(BUILD_INTERVAL_MULTIPLE));
        let backoff = interval
            .saturating_mul(1u32 << self.unpublished.min(16))
            .min(MAX_BUILD_INTERVAL.max(interval));
        self.not_before = Some(now + backoff);
    }
}

/// A table's secondary indexes over its Vortex files: one tiered index per
/// key, the view scans pin, the reservation that holds the runs' bytes, and
/// the schedule of the one background build that indexes uncovered files.
pub(crate) struct LookupIndexState {
    table_name: String,
    specs: Vec<KeySpec>,
    /// One per spec, replaced together when a schema change alters a key's
    /// encoding ([`Self::adopt_shapes`]).
    shapes: ArcSwap<Vec<Arc<Shape>>>,
    word_bits: Option<u32>,
    /// The pool a build's working memory is reserved against.
    pool: Arc<dyn MemoryPool>,
    /// The table's account, which holds the runs' resident bytes.
    account: Arc<CayenneMemoryAccount>,
    /// Every key's current view, pinned together. Always set: an empty view
    /// covers no file, so a lookup reads everything and asks for a build.
    index: ArcSwap<LookupIndexView>,
    /// Serializes changing the runs, `index` and `reservation`, so the
    /// published view always matches the runs and their bytes are always
    /// charged. Probes never take it.
    publish_lock: Mutex<()>,
    /// The live runs' resident bytes, charged to the account.
    reservation: Mutex<Option<LookupIndexReservation>>,
    /// The snapshot and file set last reconciled against, so an unchanged one
    /// does no work and every reconcile sees a distinct file set.
    reconciled: Mutex<Option<(String, FileSetVersion)>>,
    /// The table's current data files as last reconciled, which coverage is
    /// reported against.
    live_files: Mutex<Option<Arc<HashSet<String>>>>,
    /// Whether a background merge is running.
    merging: AtomicBool,
    schedule: Mutex<BuildSchedule>,
    /// Whether runs have been published yet, so only the first is logged at
    /// `info`.
    published_once: AtomicBool,
    /// The pool's refusal of runs, reported once until runs fit again.
    refusal: WarnOnce,
    /// Where the runs persist, when they do.
    persisted_runs: std::sync::OnceLock<Arc<PersistedRuns>>,
    /// A write that could not be indexed, reported once until runs publish
    /// again.
    write_failure: WarnOnce,
    counters: Counters,
    /// The table's scan-input version. Scan views pin the published view, so
    /// every change to it must invalidate the cached views, or scans keep
    /// serving a view that pinned older runs.
    scan_input_version: Arc<AtomicU64>,
}

impl LookupIndexState {
    /// The index state for `specs`, or `None` when the table declares no index.
    ///
    /// # Errors
    ///
    /// When a key column cannot be indexed. Configuration validation reports
    /// that before a table is created or opened.
    pub(crate) fn new(
        table_name: &str,
        specs: Vec<KeySpec>,
        schema: &arrow_schema::Schema,
        pool: Arc<dyn MemoryPool>,
        account: Arc<CayenneMemoryAccount>,
        scan_input_version: Arc<AtomicU64>,
        word_bits: Option<u32>,
    ) -> Result<Option<Arc<Self>>, String> {
        if specs.is_empty() {
            return Ok(None);
        }
        let shapes = specs
            .iter()
            .map(|spec| Shape::new(spec, schema, word_bits).map(Arc::new))
            .collect::<Result<Vec<_>, _>>()?;
        let labels: Vec<&str> = specs.iter().map(KeySpec::label).collect();
        tracing::info!(
            table = %table_name,
            "Dataset '{table_name}' (cayenne): maintaining secondary indexes on {}",
            labels.join(", ")
        );
        let shapes = Arc::new(shapes);
        let state = Arc::new(Self {
            index: ArcSwap::from_pointee(LookupIndexView::of(Arc::clone(&shapes))),
            table_name: table_name.to_string(),
            specs,
            shapes: ArcSwap::new(shapes),
            word_bits,
            pool,
            account,
            publish_lock: Mutex::new(()),
            reservation: Mutex::new(None),
            reconciled: Mutex::new(None),
            live_files: Mutex::new(None),
            merging: AtomicBool::new(false),
            schedule: Mutex::new(BuildSchedule::default()),
            published_once: AtomicBool::new(false),
            refusal: WarnOnce::default(),
            persisted_runs: std::sync::OnceLock::new(),
            write_failure: WarnOnce::default(),
            counters: Counters::default(),
            scan_input_version,
        });
        Ok(Some(state))
    }

    pub(crate) fn published(&self) -> Arc<LookupIndexView> {
        self.index.load_full()
    }

    /// Every key's shape under `schema`, the table's schema after a change,
    /// for [`Self::adopt_shapes`] to swap in once the change is committed.
    ///
    /// # Errors
    ///
    /// When a key column can no longer be indexed under `schema`, as opening
    /// the table with it would report.
    pub(crate) fn shapes_for(&self, schema: &arrow_schema::Schema) -> Result<KeyShapes, String> {
        self.specs
            .iter()
            .map(|spec| Shape::new(spec, schema, self.word_bits))
            .collect::<Result<Vec<_>, _>>()
            .map(KeyShapes)
    }

    /// Swaps in `shapes`, from [`Self::shapes_for`] under the table's new
    /// schema. A key whose encoding is unchanged keeps its index. A key whose
    /// encoding changed (a key column relaxed to nullable, or widened to
    /// another type) gets an empty index, as if none of the table's files had
    /// been indexed: its runs no longer match its keys, so they are dropped,
    /// every file reads as uncovered, and the next lookup
    /// rebuilds the index from the files. A run still being built under the old
    /// encoding is refused when it is published.
    pub(crate) fn adopt_shapes(self: &Arc<Self>, shapes: KeyShapes) {
        let mut rebuilt: Vec<String> = Vec::new();
        {
            let _publishing = self.publish_lock.lock();
            let current = self.shapes.load_full();
            let shapes: Vec<Arc<Shape>> = shapes
                .0
                .into_iter()
                .zip(current.iter())
                .map(|(mut shape, old)| {
                    if shape.encoder.word_identity() == old.encoder.word_identity() {
                        shape.index = Arc::clone(&old.index);
                    } else {
                        rebuilt.push(shape.label.clone());
                    }
                    Arc::new(shape)
                })
                .collect();
            self.shapes.store(Arc::new(shapes));
            self.charge(self.run_bytes());
            self.repin();
        }
        if rebuilt.is_empty() {
            return;
        }
        self.report_coverage();
        tracing::info!(
            table = %self.table_name,
            "{}",
            rebuilt_index_message(&self.table_name, &rebuilt)
        );
    }

    pub(crate) fn counters(&self) -> LookupIndexCounters {
        let index_bytes = self
            .reservation
            .lock()
            .as_ref()
            .map_or(0, LookupIndexReservation::bytes);
        self.counters
            .snapshot(u64::try_from(index_bytes).unwrap_or(u64::MAX))
    }

    /// Pins every key's current view as the published one. The caller holds
    /// `publish_lock`.
    fn repin(&self) {
        let view = LookupIndexView::of(self.shapes.load_full());
        if let Some(persisted_runs) = self.persisted_runs.get() {
            persisted_runs.schedule(
                view.shapes
                    .iter()
                    .map(|shape| shape.persisted_dir())
                    .zip(view.views.iter().cloned())
                    .collect(),
            );
        }
        self.index.store(Arc::new(view));
        self.scan_input_version.fetch_add(1, Ordering::Release);
    }

    /// Persists every key's runs as run files under `root`, registered in
    /// the table's metastore, and loads the registered ones, so a reopened
    /// table reads back only the files no persisted run covers. `live` lists the
    /// files a reader can see now. A persisted run that cannot be read is deleted
    /// and its files are indexed again.
    pub(crate) async fn open_persisted_runs(
        self: &Arc<Self>,
        store: Arc<dyn ObjectStore>,
        catalog: Arc<dyn MetadataCatalog>,
        table_id: String,
        location: &datafusion::datasource::listing::ListingTableUrl,
        live: Vec<String>,
    ) {
        let coordinator = PersistenceCoordinator::for_location(location.to_string());
        let mut generation = Arc::clone(&coordinator.owner).lock_owned().await;
        let owner = Arc::new(());
        *generation = Arc::downgrade(&owner);
        let root = location.prefix().clone();
        let keys: Vec<String> = self
            .shapes
            .load()
            .iter()
            .map(|shape| shape.persisted_dir())
            .collect();
        if !distinct_dirs(keys.iter().map(String::as_str)) {
            tracing::debug!(table = %self.table_name, "Secondary index runs are not persisted: two keys share a run directory");
            return;
        }
        let persisted_runs = Arc::new(PersistedRuns::new(
            self.table_name.clone(),
            store,
            catalog,
            table_id,
            root,
            keys,
            (coordinator, owner),
        ));
        let state = Arc::clone(self);
        // Cancellation while waiting does no work; after acquisition the worker
        // retains the operation lock until every mutation has completed.
        let result = tokio::spawn(
            async move {
                let _generation = generation;
                state.open_persisted_runs_owned(persisted_runs, live).await;
            }
            .instrument(tracing::Span::current())
            .with_current_subscriber(),
        )
        .await;
        if let Err(error) = result {
            tracing::debug!(table = %self.table_name, %error, "Persisted secondary index initialization did not complete");
        }
    }

    async fn open_persisted_runs_owned(
        self: &Arc<Self>,
        persisted_runs: Arc<PersistedRuns>,
        live: Vec<String>,
    ) {
        if self
            .persisted_runs
            .set(Arc::clone(&persisted_runs))
            .is_err()
        {
            return;
        }
        // A failed load leaves syncing off for this open, as if the table did
        // not persist: its files are indexed in the background, and the runs
        // stay where they are for the next open.
        let live: HashSet<&str> = live.iter().map(|path| file_name(path)).collect();
        *self.live_files.lock() = Some(Arc::new(
            live.iter().map(|&name| name.to_string()).collect(),
        ));
        let mut loaded = match persisted_runs.load(Some(&self.account)).await {
            Ok(loaded) => loaded,
            Err(error) => {
                if matches!(error, PersistedReadError::BudgetRefused) {
                    self.report_refusal();
                }
                self.report_coverage();
                tracing::debug!(table = %self.table_name, %error, "Secondary index runs were not loaded, and are not persisted until the table reopens");
                return;
            }
        };
        let loaded_count: usize = loaded.runs.iter().map(Vec::len).sum();
        {
            let publishing = self.publish_lock.lock();
            // Publishing can temporarily duplicate the existing run metadata.
            // Reserve that scratch before changing any visible view.
            let Some(scratch) = self.run_bytes().checked_mul(8) else {
                self.report_refusal();
                return;
            };
            let Some(reservation) = loaded.reservation.as_mut() else {
                self.report_refusal();
                return;
            };
            let Some(peak) = reservation.bytes().checked_add(scratch) else {
                self.report_refusal();
                return;
            };
            if !reservation.try_resize(peak) {
                self.report_refusal();
                return;
            }
            let Some(reservation) = loaded.reservation.take() else {
                self.report_refusal();
                return;
            };
            let transferred = {
                let mut held = self.reservation.lock();
                if let Some(held) = held.as_mut() {
                    held.absorb(reservation)
                } else {
                    *held = Some(reservation);
                    Ok(())
                }
            };
            if let Err(reservation) = transferred {
                loaded.reservation = Some(reservation);
                self.report_refusal();
                return;
            }
            for (shape, runs) in self.shapes.load().iter().zip(loaded.runs) {
                shape.index.publish_visible(runs, &live);
            }
            // From here every change to the runs is persisted, and the first
            // sync removes the persisted runs of files that are gone.
            persisted_runs.loaded.store(true, Ordering::Release);
            self.repin();
            // Encoded buffers, decode scratch and replaced local views are
            // gone. Each decode bound includes its run and table-filter share,
            // and existing resident bytes retain extra publication headroom,
            // so settling to the published bytes only shrinks the charge.
            let settled = self.charge(self.run_bytes());
            debug_assert!(settled, "published runs fit their admitted decode bounds");
            drop(publishing);
            self.report_coverage();
        }
        if loaded_count > 0 {
            // A file counts as covered once every key's runs hold it.
            let view = self.published();
            let covered = live.iter().filter(|file| view.covers(file)).count();
            tracing::info!(
                table = %self.table_name,
                "{}",
                persisted_runs_loaded_message(&self.table_name, loaded.bytes, covered, live.len())
            );
        }
    }

    /// Reports, per key, how many of the table's current data files its runs
    /// cover and how many they do not yet, on `cayenne_lookup_index_files`.
    /// Nothing is reported until the table's files are known: at open, when
    /// it loads persisted runs, or else at the first scan.
    fn report_coverage(&self) {
        for (label, covered, uncovered) in self.coverage().unwrap_or_default() {
            for (coverage, files) in [("covered", covered), ("uncovered", uncovered)] {
                telemetry::cayenne::track_lookup_index_files(
                    u64::try_from(files).unwrap_or(u64::MAX),
                    &[
                        telemetry::KeyValue::new("table", self.table_name.clone()),
                        telemetry::KeyValue::new("shape", label.clone()),
                        telemetry::KeyValue::new("coverage", coverage),
                    ],
                );
            }
        }
    }

    /// Per key, its label and how many of the table's current data files its
    /// runs cover and do not cover; `None` before a scan has listed them.
    fn coverage(&self) -> Option<Vec<(String, usize, usize)>> {
        let live = self.live_files.lock().clone()?;
        Some(
            self.shapes
                .load()
                .iter()
                .map(|shape| {
                    let view = shape.index.view();
                    let covered = live.iter().filter(|file| view.covers(file)).count();
                    (shape.label.clone(), covered, live.len() - covered)
                })
                .collect(),
        )
    }

    /// Resident bytes of every key's runs.
    fn run_bytes(&self) -> usize {
        self.shapes
            .load()
            .iter()
            .map(|shape| shape.index.view().run_heap_bytes())
            .sum()
    }

    /// Charges `bytes` for the runs, replacing the previous charge. `false`,
    /// keeping the previous charge, when the pool cannot fit the growth. The
    /// caller holds `publish_lock`.
    fn charge(&self, bytes: usize) -> bool {
        let mut reservation = self.reservation.lock();
        if let Some(reservation) = reservation.as_mut() {
            return reservation.try_resize(bytes);
        }
        match self.account.try_reserve_lookup_index(bytes) {
            Some(charged) => {
                *reservation = Some(charged);
                true
            }
            None => false,
        }
    }

    /// Reports that the pool refused a run, once until runs fit again.
    fn report_refusal(&self) {
        self.counters
            .builds_unpublished
            .fetch_add(1, Ordering::Relaxed);
        self.refusal
            .report(&self.table_name, &refused_build_message(&self.table_name));
    }

    /// Publishes one run per key over the same files, and pins the new view.
    /// `live`, when given, is the complete file set a reader can see now: the
    /// runs' files are then marked seen (a read-back of visible files).
    /// Without it the runs are for a write that is about to become visible.
    /// The runs are dropped, leaving their files uncovered, when the pool
    /// cannot fit them. Returns whether they were published.
    fn publish_runs(self: &Arc<Self>, runs: Vec<IndexRun>, live: Option<&HashSet<&str>>) -> bool {
        let rows = runs.first().map_or(0, IndexRun::len);
        let files = runs.first().map_or(0, |run| run.files().len());
        let added: usize = runs.iter().map(IndexRun::heap_bytes).sum();
        {
            let publishing = self.publish_lock.lock();
            if !self.charge(self.run_bytes().saturating_add(added)) {
                drop(publishing);
                self.report_refusal();
                return false;
            }
            for (shape, run) in self.shapes.load().iter().zip(runs) {
                match live {
                    Some(live) => shape.index.publish_visible(vec![run], live),
                    None => shape.index.publish(vec![run], &[]),
                }
            }
            // A run over files that already left the set was dropped, as is a
            // run built under a key's previous encoding, so charge what is
            // actually held.
            self.charge(self.run_bytes());
            self.repin();
        }
        self.refusal.reset();
        self.write_failure.reset();
        self.report_coverage();
        self.counters
            .builds_published
            .fetch_add(1, Ordering::Relaxed);
        let message = format!(
            "Dataset '{}' (cayenne): indexed {rows} rows in {files} files for its secondary index",
            self.table_name
        );
        if self.published_once.swap(true, Ordering::Relaxed) {
            tracing::trace!(table = %self.table_name, "{message}");
        } else {
            tracing::info!(table = %self.table_name, "{message}");
        }
        self.maybe_merge();
        true
    }

    /// Folds runs together in the background once a key holds more than
    /// [`MERGE_ABOVE_RUNS`], so a lookup's cost stays bounded as writes add
    /// runs and the rows of retired files are dropped, and rebuilds a key's
    /// filter once it is overfull (a publish never rebuilds it, so no write
    /// waits for that). One at a time.
    fn maybe_merge(self: &Arc<Self>) {
        let busy = self.shapes.load().iter().any(|shape| {
            shape.index.view().runs() > MERGE_ABOVE_RUNS || shape.index.filter_overfull()
        });
        if !busy || self.merging.swap(true, Ordering::AcqRel) {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            self.merging.store(false, Ordering::Release);
            return;
        };
        let state = Arc::clone(self);
        // Merging decodes and re-encodes runs: CPU work for the blocking pool.
        runtime.spawn_blocking(move || {
            for shape in state.shapes.load_full().iter() {
                shape.index.rebuild_overfull_filter();
                while shape.index.view().runs() > MERGE_ABOVE_RUNS {
                    match shape.index.merge_step() {
                        Ok(true) => {}
                        Ok(false) => break,
                        Err(error) => {
                            tracing::debug!(table = %state.table_name, %error, "Secondary index runs could not be merged; lookups probe more runs until a later merge");
                            break;
                        }
                    }
                }
            }
            {
                let _publishing = state.publish_lock.lock();
                // A merge only drops rows, so the charge shrinks or stays.
                state.charge(state.run_bytes());
                state.repin();
            }
            state.merging.store(false, Ordering::Release);
        });
    }

    /// Retires the files that have left the table's current file set
    /// (compacted away, replaced by a refresh, moved to the cold tier), once
    /// per distinct snapshot and file set. `live` is every warm file of
    /// `snapshot_id` at `file_set`.
    pub(crate) fn reconcile<'a>(
        self: &Arc<Self>,
        snapshot_id: &str,
        file_set: FileSetVersion,
        live: impl Iterator<Item = &'a str>,
    ) {
        {
            let mut reconciled = self.reconciled.lock();
            if reconciled
                .as_ref()
                .is_some_and(|(snapshot, set)| snapshot == snapshot_id && *set == file_set)
            {
                return;
            }
            *reconciled = Some((snapshot_id.to_string(), file_set));
        }
        let live: HashSet<&str> = live.map(file_name).collect();
        *self.live_files.lock() = Some(Arc::new(
            live.iter().map(|&name| name.to_string()).collect(),
        ));
        let publishing = self.publish_lock.lock();
        let retired: usize = self
            .shapes
            .load()
            .iter()
            .map(|shape| shape.index.reconcile(&live))
            .sum();
        if retired > 0 {
            self.charge(self.run_bytes());
            self.repin();
        }
        drop(publishing);
        self.report_coverage();
        // The runs a rewrite replaced have retired: now an overfull filter
        // is rebuilt over only the keys still live.
        self.maybe_merge();
    }

    /// An observer for one write: it builds a run per key from exactly the
    /// rows written, and [`Self::finish_write`] publishes them when the write
    /// returns. Whether the write `replaces` files decides what happens when
    /// indexing falls behind the write; see [`RunObserver`].
    pub(crate) fn write_observer(self: &Arc<Self>, replaces: bool) -> Arc<RunObserver> {
        self.write_observer_with(replaces, QUEUED_BYTES, None)
    }

    /// [`Self::write_observer`] with a queue of up to `capacity` bytes, whose
    /// thread starts indexing only once `gate` (when given) receives, so a
    /// test can fill the queue.
    fn write_observer_with(
        self: &Arc<Self>,
        replaces: bool,
        capacity: usize,
        gate: Option<std::sync::mpsc::Receiver<()>>,
    ) -> Arc<RunObserver> {
        let shapes = self.shapes.load_full();
        let shared = Arc::new(ObserverShared {
            state: Arc::clone(self),
            builders: Mutex::new(Some(
                shapes.iter().map(|shape| shape.run_builder()).collect(),
            )),
            shapes,
            reservation: Mutex::new(self.build_reservation()),
            failure: Mutex::new(None),
            lock_wait_ns: AtomicU64::new(0),
            encode_ns: AtomicU64::new(0),
            inline_batches: AtomicU64::new(0),
            queued_bytes: AtomicUsize::new(0),
            queue_limit: capacity,
        });
        let (sender, receiver) = std::sync::mpsc::channel::<IndexJob>();
        let (drained_tx, drained_rx) = tokio::sync::oneshot::channel();
        let indexer = Arc::clone(&shared);
        let spawned = std::thread::Builder::new()
            .name("cayenne-index-write".to_string())
            .spawn(move || {
                if let Some(gate) = gate {
                    let _ = gate.recv();
                }
                for job in receiver {
                    indexer.index(&job);
                    indexer.queued_bytes.fetch_sub(job.bytes, Ordering::AcqRel);
                }
                // The observer may be gone already (its write failed).
                let _ = drained_tx.send(());
            });
        let queue = match spawned {
            Ok(_) => Queue::Open(sender),
            // No thread: the writer indexes every batch itself.
            Err(_) => Queue::Inline,
        };
        Arc::new(RunObserver {
            shared,
            replaces,
            queue: Mutex::new(queue),
            drained: Mutex::new(Some(drained_rx)),
        })
    }

    /// Publishes the runs `observer` built during its write, which has
    /// returned and is about to become visible. A run missing rows is never
    /// published: on any failure the write's files stay uncovered and are
    /// read in full until a background build indexes them.
    ///
    /// A write that `replaces` files — a compaction's rewrite, a full refresh
    /// — always finishes its run here, before its caller makes it visible, so
    /// the files it swaps in are covered from the moment they are visible and
    /// no reader falls back to scanning them. An additive write (an append) of
    /// more than [`DEFER_FINISH_ROWS`] rows is finished in the background
    /// instead, so the append does not wait while its keys are sorted: its
    /// files are uncovered, and read in full, until the run is published —
    /// as a write's run always is, whether or not the write is visible by
    /// then; reconcile retires the files of a write that never becomes
    /// visible.
    pub(crate) async fn finish_write(self: &Arc<Self>, observer: &RunObserver) {
        observer.drain().await;
        let replaces = observer.replaces;
        let observer = &*observer.shared;
        let builders = observer.builders.lock().take();
        let failure = observer.failure.lock().take();
        let published = match (failure, builders) {
            (Some(ObserverFailure::Refused), _) => {
                self.report_refusal();
                false
            }
            (Some(ObserverFailure::Failed(cause)), _) => {
                self.write_unindexed(&cause);
                false
            }
            (Some(ObserverFailure::Behind), _) => {
                self.counters
                    .builds_unpublished
                    .fetch_add(1, Ordering::Relaxed);
                tracing::debug!(table = %self.table_name, "An append outpaced its secondary index, so its files are read in full until they are indexed in the background");
                false
            }
            (None, Some(builders))
                if builders
                    .first()
                    .is_some_and(|builder| !builder.files().is_empty()) =>
            {
                let rows = builders.first().map_or(0, RunBuilder::rows);
                if !replaces && rows > DEFER_FINISH_ROWS {
                    self.finish_in_background(builders, observer.reservation.lock().take());
                    return;
                }
                let finishing = Instant::now();
                let finished = finish_runs(builders).await;
                super::table::record_cayenne_write_phase(
                    &self.table_name,
                    "lookup_index",
                    finishing,
                );
                tracing::debug!(
                    table = %self.table_name,
                    rows,
                    encode_ms = observer.encode_ns.load(Ordering::Relaxed) / 1_000_000,
                    lock_wait_ms = observer.lock_wait_ns.load(Ordering::Relaxed) / 1_000_000,
                    inline_batches = observer.inline_batches.load(Ordering::Relaxed),
                    finish_ms = finishing.elapsed().as_millis(),
                    "Built a write's secondary index runs"
                );
                match finished {
                    Ok(runs) => self.publish_runs(runs, None),
                    Err(cause) => {
                        self.write_unindexed(&cause);
                        false
                    }
                }
            }
            (None, _) => false,
        };
        if !published {
            tracing::trace!(table = %self.table_name, "A write's secondary index runs were not published");
        }
        // Whatever happened, the builders' working memory goes with them.
        observer.reservation.lock().free();
    }

    /// [`Self::finish_write`] for a large write, off the write path. The
    /// write's working memory stays reserved until its run is published or
    /// dropped.
    fn finish_in_background(
        self: &Arc<Self>,
        builders: Vec<RunBuilder>,
        reservation: MemoryReservation,
    ) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let state = Arc::clone(self);
        runtime.spawn(async move {
            let rows = builders.first().map_or(0, RunBuilder::rows);
            let finishing = Instant::now();
            match finish_runs(builders).await {
                Ok(runs) => {
                    state.publish_runs(runs, None);
                }
                Err(cause) => state.write_unindexed(&cause),
            }
            tracing::debug!(
                table = %state.table_name,
                rows,
                finish_ms = finishing.elapsed().as_millis(),
                "Built a large write's secondary index runs in the background"
            );
            drop(reservation);
        });
    }

    /// Reports a write whose rows could not be indexed: at `warn` once until
    /// runs publish again, then at `debug`.
    fn write_unindexed(&self, cause: &str) {
        self.counters
            .builds_unpublished
            .fetch_add(1, Ordering::Relaxed);
        let message = format!(
            "Dataset '{}' (cayenne): failed to index a write's rows for its secondary index, so lookups read that write's files in full until they are indexed in the background. Cause: {cause}",
            self.table_name
        );
        self.write_failure.report(&self.table_name, &message);
    }

    /// The right to index the table's uncovered files in the background, or
    /// `None` when a build is running or the schedule says not yet.
    pub(crate) fn claim_build(self: &Arc<Self>) -> Option<BuildClaim> {
        let now = Instant::now();
        {
            let mut schedule = self.schedule.lock();
            if !schedule.can_start(now) {
                return None;
            }
            schedule.in_flight = true;
        }
        self.counters.builds_started.fetch_add(1, Ordering::Relaxed);
        Some(BuildClaim {
            state: Arc::clone(self),
            started: now,
            settled: false,
        })
    }

    /// Working memory for one build, reserved against the query pool.
    fn build_reservation(&self) -> MemoryReservation {
        MemoryConsumer::new(format!("cayenne_index_build:{}", self.table_name)).register(&self.pool)
    }

    /// The index of the key these filters fully pin, if any. `values_for`
    /// gives a column's equality values: one for `=`, several for `IN`.
    fn matched_shape(
        &self,
        values_for: &dyn Fn(&str) -> Option<Vec<ScalarValue>>,
    ) -> Option<usize> {
        self.specs.iter().position(|spec| {
            spec.columns
                .iter()
                .all(|column| values_for(column).is_some())
        })
    }

    /// The label of key `shape`.
    fn shape_label(&self, shape: usize) -> &str {
        self.specs[shape].label()
    }

    /// Resolves a candidate row selection for `values_for` (see
    /// [`Self::matched_shape`]) against the view the scan pinned. A fallback
    /// carries the reason for `EXPLAIN`; the caller records the final outcome
    /// once [`LookupSelection::restrict`] has seen its files.
    ///
    /// `values_for` must only answer for predicates that compare the bare
    /// column with values: see the module's note on column-side casts. The
    /// keys are every combination of the columns' values, all probed in one
    /// batch; more than [`RUNTIME_INDEX_MAX_KEYS`] combinations, or more than
    /// [`RUNTIME_INDEX_MAX_ROWS`] candidates for several keys, decline.
    pub(crate) fn probe(
        self: &Arc<Self>,
        index: &Arc<LookupIndexView>,
        values_for: &dyn Fn(&str) -> Option<Vec<ScalarValue>>,
    ) -> LookupProbe {
        let Some(shape) = self.matched_shape(values_for) else {
            return LookupProbe::Fallback(LookupIndexExplain::scanned(
                None,
                LookupIndexScanReason::NoKeyPinned,
            ));
        };
        let label = self.shape_label(shape).to_string();
        // `matched_shape` found every column pinned, so a missing product is
        // one past the bound.
        let Some(keys) = key_tuples(&self.specs[shape].columns, values_for) else {
            return LookupProbe::Fallback(LookupIndexExplain::scanned(
                Some(label),
                LookupIndexScanReason::TooManyKeys,
            ));
        };
        // One key keeps every candidate, as an equality lookup always has;
        // several are bounded like a runtime key set.
        let max_rows = (keys.len() > 1).then_some(RUNTIME_INDEX_MAX_ROWS);
        let Ok(encoded) = index.shapes[shape].encode_keys(&keys) else {
            return LookupProbe::Fallback(LookupIndexExplain::scanned(
                Some(label),
                LookupIndexScanReason::ValueNotIndexable,
            ));
        };
        let Some(hit) = index.probe_keys(shape, &encoded, max_rows, None) else {
            return LookupProbe::Fallback(LookupIndexExplain::scanned(
                Some(label),
                LookupIndexScanReason::TooManyCandidates,
            ));
        };
        LookupProbe::Selection(LookupSelection {
            state: Arc::clone(self),
            index: Arc::clone(index),
            shape: hit.shape,
            per_file: hit.per_file,
            rows: hit.rows,
        })
    }

    /// Probes a completed hash-join dynamic filter's `keys` for key `spec` as
    /// one batched lookup, recording exactly one outcome.
    ///
    /// Single-column membership arrives as `column IN (...)`; composite
    /// membership arrives as `struct(columns...) IN (struct literals...)`, so
    /// tuple correlation is preserved without a Cartesian product. `index` is
    /// the view the scan pinned. It answers for the files it covers; files it
    /// does not cover are read as planned, and a build is requested for them.
    fn probe_runtime_filter(
        &self,
        spec: usize,
        index: &Arc<LookupIndexView>,
        scan_files: &[ObjectMeta],
        keys: &[Vec<ScalarValue>],
    ) -> RuntimeProbe {
        let label = self.shape_label(spec);
        // The view spans every file of the table; this scan answers only for
        // its own, so only their candidates count, against the row bound too.
        let read: HashSet<&str> = scan_files
            .iter()
            .map(|file| file_name(file.location.as_ref()))
            .collect();
        let covered = read.iter().filter(|name| index.covers(name)).count();
        if covered == 0 {
            self.record_probe(label, Coverage::Unindexed);
            return RuntimeProbe::IndexUnusable;
        }
        let max_rows = RUNTIME_INDEX_MIN_ROWS
            .max(index.rows / 1_000)
            .min(RUNTIME_INDEX_MAX_ROWS);
        let hit = index.shapes[spec]
            .encode_keys(keys)
            .ok()
            .and_then(|encoded| index.probe_keys(spec, &encoded, Some(max_rows), Some(&read)));
        let Some(hit) = hit else {
            self.record_runtime_fallback();
            return RuntimeProbe::Declined;
        };
        // An uncovered file is read in full whatever the covered ones hold, so
        // a probe reads nothing only when every file the scan reads is covered.
        let uncovered = read.len() - covered;
        if hit.rows == 0 && uncovered == 0 {
            // Every file is indexed and none holds a key: nothing is read.
            self.record_probe(&hit.shape, Coverage::Full);
        } else {
            self.record_selection(
                &hit.shape,
                Coverage::of(true, uncovered > 0),
                (hit.per_file.len() + uncovered) as u64,
                hit.rows as u64,
            );
        }
        RuntimeProbe::Selection {
            selection: RuntimeLookupSelection::new(Arc::clone(index), hit.per_file),
            uncovered: uncovered > 0,
        }
    }

    fn record_probe(&self, shape: &str, coverage: Coverage) {
        record_probe_outcome(&self.table_name, &self.counters, shape, coverage);
    }

    /// Records a file-mode lookup once, from the decision its scan reached
    /// over every snapshot it read.
    pub(crate) fn record_lookup(&self, explain: &LookupIndexExplain) {
        let (Some(shape), LookupIndexExplainOutcome::Probed(coverage)) =
            (&explain.shape, explain.outcome)
        else {
            return;
        };
        if coverage == Coverage::Unindexed {
            self.record_probe(shape, coverage);
        } else {
            self.record_selection(
                shape,
                coverage,
                explain.candidate_files.unwrap_or(0) as u64,
                explain.candidate_rows.unwrap_or(0),
            );
        }
    }

    fn record_selection(&self, shape: &str, coverage: Coverage, files: u64, rows: u64) {
        self.counters
            .candidate_files
            .fetch_add(files, Ordering::Relaxed);
        self.counters
            .candidate_rows
            .fetch_add(rows, Ordering::Relaxed);
        self.record_probe(shape, coverage);
    }

    fn record_runtime_fallback(&self) {
        self.counters
            .runtime_fallback
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// Every key tuple the columns' equality values pin: their cartesian
/// product, `None` past [`RUNTIME_INDEX_MAX_KEYS`] tuples.
pub(crate) fn key_tuples(
    columns: &[String],
    values_for: &dyn Fn(&str) -> Option<Vec<ScalarValue>>,
) -> Option<Vec<Vec<ScalarValue>>> {
    let mut tuples: Vec<Vec<ScalarValue>> = vec![Vec::with_capacity(columns.len())];
    for column in columns {
        let values = values_for(column)?;
        if tuples.len().saturating_mul(values.len()) > RUNTIME_INDEX_MAX_KEYS {
            return None;
        }
        tuples = tuples
            .into_iter()
            .flat_map(|tuple| {
                values.iter().map(move |value| {
                    let mut tuple = tuple.clone();
                    tuple.push(value.clone());
                    tuple
                })
            })
            .collect();
    }
    Some(tuples)
}

/// Finishes one run per key. Sorting a large write's keys is CPU work, so it
/// runs on the blocking pool.
async fn finish_runs(builders: Vec<RunBuilder>) -> Result<Vec<IndexRun>, String> {
    tokio::task::spawn_blocking(move || {
        builders
            .into_iter()
            .map(RunBuilder::finish)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())
    })
    .await
    .map_err(|e| format!("index build task failed: {e}"))?
}

/// Why a write's runs will not be published.
#[derive(Clone, Debug)]
enum ObserverFailure {
    /// The pool could not fit the runs' working memory.
    Refused,
    /// The rows could not be indexed.
    Failed(String),
    /// An append outpaced its indexing, which dropped the run rather than
    /// hold the write back.
    Behind,
}

/// Bytes of key columns a write may have queued for its indexing thread
/// before it is behind. A write arrives in bursts much faster than its keys
/// encode, so the bound is on memory, not batches: a 1.2M-row append queues
/// about 50 MB.
const QUEUED_BYTES: usize = 256 << 20;

/// One written batch's key columns, as written, for the indexing thread.
struct IndexJob {
    file: String,
    first_row_position: u64,
    /// Per key, its columns before any cast.
    columns: Vec<Vec<ArrayRef>>,
    /// The columns' memory, counted in [`ObserverShared::queued_bytes`].
    bytes: usize,
}

/// Where a write's batches go.
enum Queue {
    /// To the write's indexing thread.
    Open(std::sync::mpsc::Sender<IndexJob>),
    /// Nowhere else: the writer indexes each batch itself.
    Inline,
    /// Nowhere: the write's index is finished.
    Closed,
}

/// Builds one write's runs from the rows as they are written, from the file
/// and file-local position the Vortex sink reports for each batch.
///
/// The writer only queues each batch's key columns (references, not copies)
/// for a thread of the write's own that encodes them, so the write does not
/// wait while its keys are encoded and sorted. When the thread falls so far
/// behind that the queue holds [`QUEUED_BYTES`], the write never waits either:
/// - an additive write (an append) drops its run, and its files are indexed
///   in the background like any uncovered file;
/// - a replacing write (a compaction, an overwrite) indexes that batch
///   itself, so its files are still covered when it becomes visible.
pub(crate) struct RunObserver {
    shared: Arc<ObserverShared>,
    replaces: bool,
    queue: Mutex<Queue>,
    /// Fires once the indexing thread has indexed every queued batch.
    drained: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

/// What a write's writer and its indexing thread share.
struct ObserverShared {
    state: Arc<LookupIndexState>,
    /// The keys the builders were made for.
    shapes: Shapes,
    /// One builder per key; `None` once taken, or dropped on a failure.
    builders: Mutex<Option<Vec<RunBuilder>>>,
    /// The builders' working memory.
    reservation: Mutex<MemoryReservation>,
    /// The first failure. The observer cannot return an error, and a run
    /// missing rows must never be published, so it is recorded and checked
    /// before publication.
    failure: Mutex<Option<ObserverFailure>>,
    /// Nanoseconds spent waiting for `builders`, and encoding keys into them,
    /// summed over every batch: how much the index cost, and how much of that
    /// is contention.
    lock_wait_ns: AtomicU64,
    encode_ns: AtomicU64,
    /// Batches a replacing write indexed itself because the queue was full.
    inline_batches: AtomicU64,
    /// Bytes of key columns queued and not yet indexed, and the most allowed.
    queued_bytes: AtomicUsize,
    queue_limit: usize,
}

impl std::fmt::Debug for RunObserver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunObserver")
            .field("table", &self.shared.state.table_name)
            .finish_non_exhaustive()
    }
}

impl RunObserver {
    /// Closes the queue and waits until every queued batch is indexed.
    async fn drain(&self) {
        *self.queue.lock() = Queue::Closed;
        let drained = self.drained.lock().take();
        if let Some(drained) = drained {
            // An error means the thread is gone, having indexed what it had.
            let _ = drained.await;
        }
    }
}

impl ObserverShared {
    fn fail(&self, failure: ObserverFailure) {
        // Free the builders' memory now: nothing will be published.
        *self.builders.lock() = None;
        self.reservation.lock().free();
        let mut slot = self.failure.lock();
        if slot.is_none() {
            *slot = Some(failure);
        }
    }

    /// Adds one batch's keys to the builders.
    fn index(&self, job: &IndexJob) {
        let waiting = Instant::now();
        let mut slot = self.builders.lock();
        let encoding = Instant::now();
        self.lock_wait_ns.fetch_add(
            u64::try_from((encoding - waiting).as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        // Failed already: the rest of the write goes unindexed.
        let Some(builders) = slot.as_mut() else {
            return;
        };
        let indexed = builders
            .iter_mut()
            .zip(self.shapes.iter())
            .zip(&job.columns)
            .try_for_each(|((builder, shape), raw)| {
                let columns = shape.cast_key_columns(raw)?;
                builder
                    .add_batch(&job.file, job.first_row_position, &columns)
                    .map_err(|e| e.to_string())
            });
        // Finishing a run needs about as much again while it sorts.
        let bytes = builders
            .iter()
            .map(RunBuilder::heap_bytes)
            .sum::<usize>()
            .saturating_mul(2);
        drop(slot);
        self.encode_ns.fetch_add(
            u64::try_from(encoding.elapsed().as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        if let Err(cause) = indexed {
            self.fail(ObserverFailure::Failed(cause));
        } else if self.reservation.lock().try_resize(bytes).is_err() {
            self.fail(ObserverFailure::Refused);
        }
    }
}

impl vortex_datafusion::VortexWriteObserver for RunObserver {
    fn batch_written(
        &self,
        file_path: &object_store::path::Path,
        first_row_position: u64,
        batch: &RecordBatch,
    ) {
        let shared = &self.shared;
        let columns: Result<Vec<Vec<ArrayRef>>, String> = shared
            .shapes
            .iter()
            .map(|shape| shape.raw_key_columns(batch))
            .collect();
        let columns = match columns {
            Ok(columns) => columns,
            Err(cause) => {
                shared.fail(ObserverFailure::Failed(cause));
                return;
            }
        };
        let bytes = columns
            .iter()
            .flatten()
            .map(|array: &ArrayRef| array.get_array_memory_size())
            .sum();
        let job = IndexJob {
            file: file_name(file_path.as_ref()).to_string(),
            first_row_position,
            columns,
            bytes,
        };
        let queue = self.queue.lock();
        let job = match &*queue {
            Queue::Open(sender) => {
                let queued = shared.queued_bytes.fetch_add(bytes, Ordering::AcqRel);
                if queued.saturating_add(bytes) <= shared.queue_limit {
                    match sender.send(job) {
                        Ok(()) => return,
                        // The thread is gone: index here.
                        Err(std::sync::mpsc::SendError(job)) => {
                            shared.queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
                            job
                        }
                    }
                } else {
                    shared.queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
                    if !self.replaces {
                        drop(queue);
                        shared.fail(ObserverFailure::Behind);
                        return;
                    }
                    shared.inline_batches.fetch_add(1, Ordering::Relaxed);
                    job
                }
            }
            Queue::Inline => job,
            Queue::Closed => {
                drop(queue);
                // A late batch has nowhere to go, and a run missing it must
                // not be published.
                shared.fail(ObserverFailure::Failed(
                    "a batch was written after the write's index was finished".to_string(),
                ));
                return;
            }
        };
        drop(queue);
        shared.index(&job);
    }
}

/// Rows above which an append's run is finished off the write path. Finishing
/// sorts the write's keys: about 140 ms for 1.2M rows and 3.5 s for 20M,
/// measured in `spiced`.
const DEFER_FINISH_ROWS: usize = 1 << 20;

/// Whether a table's secondary index runs persist as run files. Hidden,
/// for testing: `SPICE_CAYENNE_INDEX_PERSISTENCE=enabled`.
pub(crate) const PERSISTENCE_ENV: &str = "SPICE_CAYENNE_INDEX_PERSISTENCE";

/// Serializes persistence for one durable table location across provider opens.
/// The weak owner fences work queued by providers that have been replaced.
struct PersistenceCoordinator {
    owner: Arc<tokio::sync::Mutex<std::sync::Weak<()>>>,
}

impl PersistenceCoordinator {
    fn for_location(location: String) -> Arc<Self> {
        type Registry = HashMap<String, std::sync::Weak<PersistenceCoordinator>>;
        static REGISTRY: std::sync::OnceLock<Mutex<Registry>> = std::sync::OnceLock::new();
        let mut registry = REGISTRY.get_or_init(Mutex::default).lock();
        registry.retain(|_, coordinator| coordinator.strong_count() > 0);
        if let Some(coordinator) = registry.get(&location).and_then(std::sync::Weak::upgrade) {
            return coordinator;
        }
        let coordinator = Arc::new(Self {
            owner: Arc::new(tokio::sync::Mutex::new(std::sync::Weak::new())),
        });
        registry.insert(location, Arc::downgrade(&coordinator));
        coordinator
    }
}

/// Every key's runs, persisted one file per run under the table's
/// `_lookup_index` directory, which snapshot cleanup never sweeps.
///
/// A persisted run is named after the run's content, so persisting is a stateless
/// sync of the directory against the live runs: write the missing, delete the
/// rest. It holds file names only, which the module's note on file names makes
/// safe to trust: at load a run covers only the files still live.
pub(crate) struct PersistedRuns {
    table_name: String,
    store: Arc<dyn ObjectStore>,
    /// Records which runs are persisted. A run is registered only once its
    /// file is written, and unregistered before its file is deleted, so every
    /// registered run has a complete file.
    catalog: Arc<dyn MetadataCatalog>,
    table_id: String,
    /// The directory every key's run directory sits in.
    root: object_store::path::Path,
    /// Per key, the name of the directory its runs persist in.
    keys: Vec<String>,
    /// Whether the persisted runs have been loaded. Until then a sync would
    /// delete them.
    loaded: AtomicBool,
    /// The latest views to sync, each with its key's directory, taken by the
    /// running sync.
    pending: Mutex<Option<Vec<(String, IndexView)>>>,
    /// Whether a sync is running.
    syncing: AtomicBool,
    coordinator: Arc<PersistenceCoordinator>,
    owner: Arc<()>,
}

/// Field order keeps decoded runs charged until their allocations are freed.
struct LoadedRuns {
    runs: Vec<Vec<IndexRun>>,
    bytes: u64,
    reservation: Option<LookupIndexReservation>,
}

#[derive(Debug, snafu::Snafu)]
enum PersistedReadError {
    #[snafu(display("The query memory pool refused persisted secondary index runs"))]
    BudgetRefused,
    #[snafu(display("Persisted index loading stopped: {message}"))]
    Interrupted { message: String },
    #[snafu(display("{message}"))]
    Unreadable { message: String },
}

impl PersistedReadError {
    fn unreadable(message: impl Into<String>) -> Self {
        Self::Unreadable {
            message: message.into(),
        }
    }
}

impl PersistedRuns {
    fn new(
        table_name: String,
        store: Arc<dyn ObjectStore>,
        catalog: Arc<dyn MetadataCatalog>,
        table_id: String,
        root: object_store::path::Path,
        keys: Vec<String>,
        ownership: (Arc<PersistenceCoordinator>, Arc<()>),
    ) -> Self {
        Self {
            coordinator: ownership.0,
            owner: ownership.1,
            table_name,
            store,
            catalog,
            table_id,
            root,
            keys,
            loaded: AtomicBool::new(false),
            pending: Mutex::new(None),
            syncing: AtomicBool::new(false),
        }
    }

    /// Removes every registered and orphan run when the table has no file-backed indexes.
    /// Loading with no configured keys applies the same cleanup as removing
    /// individual keys, without reading any run into memory.
    pub(crate) async fn remove_all(
        table_name: String,
        store: Arc<dyn ObjectStore>,
        catalog: Arc<dyn MetadataCatalog>,
        table_id: String,
        root: object_store::path::Path,
        location: String,
    ) {
        let coordinator = PersistenceCoordinator::for_location(location);
        let mut generation = Arc::clone(&coordinator.owner).lock_owned().await;
        let owner = Arc::new(());
        *generation = Arc::downgrade(&owner);
        let runs = Self::new(
            table_name.clone(),
            store,
            catalog,
            table_id,
            root,
            Vec::new(),
            (coordinator, owner),
        );
        let result = tokio::spawn(async move {
            let _generation = generation;
            if let Err(error) = runs.load(None).await {
                tracing::debug!(table = %runs.table_name, %error, "Persisted secondary index runs of removed indexes were not deleted; the next open retries");
            }
        }.instrument(tracing::Span::current()).with_current_subscriber()).await;
        if let Err(error) = result {
            tracing::debug!(table = %table_name, %error, "Persisted secondary index removal did not complete");
        }
    }

    /// Stops replaced providers from persisting when persistence is disabled.
    pub(crate) async fn fence(location: String) {
        let coordinator = PersistenceCoordinator::for_location(location);
        *coordinator.owner.lock().await = std::sync::Weak::new();
    }

    /// Persists `views`' runs in the background, coalescing with any sync
    /// already running.
    fn schedule(self: &Arc<Self>, views: Vec<(String, IndexView)>) {
        if !self.loaded.load(Ordering::Acquire)
            || !distinct_dirs(views.iter().map(|(dir, _)| dir.as_str()))
        {
            return;
        }
        *self.pending.lock() = Some(views);
        if self.syncing.swap(true, Ordering::AcqRel) {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            self.syncing.store(false, Ordering::Release);
            return;
        };
        let persisted_runs = Arc::clone(self);
        runtime.spawn(async move { persisted_runs.sync().await });
    }

    async fn sync(&self) {
        loop {
            let views = self.pending.lock().take();
            let Some(views) = views else {
                self.syncing.store(false, Ordering::Release);
                // Views scheduled after the take above found a sync running.
                if self.pending.lock().is_some() && !self.syncing.swap(true, Ordering::AcqRel) {
                    continue;
                }
                return;
            };
            let generation = self.coordinator.owner.lock().await;
            if !std::sync::Weak::ptr_eq(&generation, &Arc::downgrade(&self.owner)) {
                self.pending.lock().take();
                self.syncing.store(false, Ordering::Release);
                return;
            }
            if let Err(error) = self.sync_views(&views).await {
                tracing::debug!(table = %self.table_name, %error, "Persisted secondary index runs were not synced; the next change retries");
            }
        }
    }

    fn path(&self, key: &str, run_name: &str) -> object_store::path::Path {
        self.root.clone().join(key).join(run_name)
    }

    async fn sync_views(&self, views: &[(String, IndexView)]) -> Result<(), String> {
        let registered = self
            .catalog
            .list_index_runs(&self.table_id)
            .await
            .map_err(|e| format!("list persisted runs: {e}"))?;
        let mut existing: HashSet<(String, String)> = registered
            .into_iter()
            .map(|record| (record.index_key, record.run_name))
            .collect();
        for (key, view) in views {
            for run in view.run_list() {
                let name = run_file_name(&run);
                if existing.remove(&(key.clone(), name.clone())) {
                    continue;
                }
                let row_count = run.len() as u64;
                let bytes = tokio::task::spawn_blocking(move || run.to_bytes())
                    .await
                    .map_err(|e| format!("encode persisted run: {e}"))?;
                let record = IndexRunRecord {
                    table_id: self.table_id.clone(),
                    index_key: key.clone(),
                    run_name: name.clone(),
                    row_count,
                    size_bytes: bytes.len() as u64,
                };
                let path = self.path(key, &name);
                self.store
                    .put(&path, bytes.into())
                    .await
                    .map_err(|e| format!("write {path}: {e}"))?;
                self.catalog
                    .register_index_run(&record)
                    .await
                    .map_err(|e| format!("register {path}: {e}"))?;
            }
        }
        // What is left is registered but no longer wanted: runs merged or
        // retired since, and runs of a key the table no longer has.
        for (key, name) in existing {
            self.remove(&key, &name).await?;
        }
        Ok(())
    }

    /// Unregisters a run, then attempts to delete its file. An orphan left by
    /// a failed deletion is eligible for cleanup during a successful open.
    async fn remove(&self, key: &str, name: &str) -> Result<(), String> {
        let path = self.path(key, name);
        self.catalog
            .remove_index_run(&self.table_id, key, name)
            .await
            .map_err(|e| format!("unregister {path}: {e}"))?;
        match self.store.delete(&path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => {
                tracing::debug!(table = %self.table_name, run_file = %path, %error, "An unregistered persisted secondary index run was not deleted; a successful open can retry cleanup");
                Ok(())
            }
        }
    }

    /// Every key's persisted runs. A registered run whose file cannot be read
    /// is unregistered and deleted, and a file no run is registered for, left
    /// by a write that stopped before registering it, is deleted. An error
    /// when the registered runs cannot be listed: nothing is loaded, and
    /// nothing may be synced, since every run would then look unwanted.
    async fn load(
        &self,
        account: Option<&Arc<CayenneMemoryAccount>>,
    ) -> Result<LoadedRuns, PersistedReadError> {
        let reservation = match account {
            Some(account) => Some(
                account
                    .try_reserve_lookup_index(0)
                    .ok_or(PersistedReadError::BudgetRefused)?,
            ),
            None => None,
        };
        let mut loaded = LoadedRuns {
            runs: self.keys.iter().map(|_| Vec::new()).collect(),
            bytes: 0,
            reservation,
        };
        let registered = self
            .catalog
            .list_index_runs(&self.table_id)
            .await
            .map_err(|e| PersistedReadError::unreadable(format!("list persisted runs: {e}")))?;
        // A run whose removal failed may still be registered, so the orphan
        // sweep must retain its file even when the run is not loaded.
        let mut kept: HashSet<object_store::path::Path> = HashSet::new();
        for record in registered {
            let path = self.path(&record.index_key, &record.run_name);
            let Some(slot) = self.keys.iter().position(|key| *key == record.index_key) else {
                // Persisted for a key the table no longer has.
                if let Err(error) = self.remove(&record.index_key, &record.run_name).await {
                    tracing::debug!(table = %self.table_name, run_file = %path, %error, "A persisted secondary index run of a removed index was not deleted; the next sync retries");
                    kept.insert(path);
                }
                continue;
            };
            let account = account.ok_or_else(|| {
                PersistedReadError::unreadable("No memory account for persisted index loading")
            })?;
            match self.read(&path, account).await {
                Ok((run, size_bytes, reservation)) => {
                    let Some(held) = loaded.reservation.as_mut() else {
                        drop(run);
                        drop(reservation);
                        return Err(PersistedReadError::BudgetRefused);
                    };
                    if let Err(reservation) = held.absorb(reservation) {
                        drop(run);
                        drop(reservation);
                        return Err(PersistedReadError::BudgetRefused);
                    }
                    kept.insert(path);
                    loaded.bytes = loaded.bytes.saturating_add(size_bytes);
                    loaded.runs[slot].push(run);
                }
                Err(
                    error @ (PersistedReadError::BudgetRefused
                    | PersistedReadError::Interrupted { .. }),
                ) => {
                    // Valid runs remain registered and syncing stays disabled.
                    // Drop all newly decoded runs without sweeping their files.
                    return Err(error);
                }
                Err(error) => {
                    tracing::debug!(table = %self.table_name, run_file = %path, %error, "Deleting a persisted secondary index run that cannot be read; its files are indexed again");
                    if let Err(error) = self.remove(&record.index_key, &record.run_name).await {
                        tracing::debug!(table = %self.table_name, run_file = %path, %error, "An unreadable persisted secondary index run was not deleted; the next sync retries");
                        kept.insert(path);
                    }
                }
            }
        }
        self.delete_unregistered(&kept).await;
        Ok(loaded)
    }

    async fn read(
        &self,
        path: &object_store::path::Path,
        account: &Arc<CayenneMemoryAccount>,
    ) -> Result<(IndexRun, u64, LookupIndexReservation), PersistedReadError> {
        let store = Arc::clone(&self.store);
        let path = path.clone();
        let account = Arc::clone(account);
        // Dropping the caller's future leaves this task running. Its buffers
        // retain their reservations until the read and decode actually finish.
        tokio::spawn(async move {
            use futures::TryStreamExt;
            let result = store
                .get(&path)
                .await
                .map_err(|error| PersistedReadError::unreadable(error.to_string()))?;
            if result.range.start != 0 || result.range.end != result.meta.size {
                return Err(PersistedReadError::unreadable("incomplete object range"));
            }
            let size_bytes = result.meta.size;
            let size =
                usize::try_from(size_bytes).map_err(|_| PersistedReadError::BudgetRefused)?;
            // A collected buffer can coexist with the stream's current chunk.
            let read_bytes = size
                .checked_mul(2)
                .and_then(|size| size.checked_add(8192))
                .ok_or(PersistedReadError::BudgetRefused)?;
            let reservation = account
                .try_reserve_lookup_index(read_bytes)
                .ok_or(PersistedReadError::BudgetRefused)?;
            let admitted = match result.payload {
                object_store::GetResultPayload::File(mut file, _) => {
                    // The blocking read owns the guard too: runtime shutdown
                    // can cancel its waiter without stopping this closure.
                    tokio::task::spawn_blocking(move || {
                        use std::io::{Read, Seek, SeekFrom};
                        file.seek(SeekFrom::Start(0))
                            .map_err(|error| PersistedReadError::unreadable(error.to_string()))?;
                        let mut bytes = vec![0; size];
                        file.read_exact(&mut bytes)
                            .map_err(|error| PersistedReadError::unreadable(error.to_string()))?;
                        let mut extra = [0_u8];
                        if file
                            .read(&mut extra)
                            .map_err(|error| PersistedReadError::unreadable(error.to_string()))?
                            != 0
                        {
                            return Err(PersistedReadError::unreadable(
                                "object grew while reading",
                            ));
                        }
                        Ok((bytes, reservation))
                    })
                    .await
                    .map_err(|error| PersistedReadError::Interrupted {
                        message: error.to_string(),
                    })??
                }
                object_store::GetResultPayload::Stream(mut stream) => {
                    let mut bytes = Vec::with_capacity(size);
                    while let Some(chunk) = stream
                        .try_next()
                        .await
                        .map_err(|error| PersistedReadError::unreadable(error.to_string()))?
                    {
                        if chunk.len() > size.saturating_sub(bytes.len()) {
                            return Err(PersistedReadError::unreadable(
                                "object grew while reading",
                            ));
                        }
                        bytes.extend_from_slice(&chunk);
                    }
                    if bytes.len() != size {
                        return Err(PersistedReadError::unreadable("incomplete object body"));
                    }
                    (bytes, reservation)
                }
            };
            tokio::task::spawn_blocking(move || {
                // Capture the tuple whole, including when a queued closure is
                // dropped before it runs: bytes must drop before their guard.
                let mut admitted = admitted;
                let decoded = IndexRun::decode_memory_bound(&admitted.0)
                    .map_err(|error| PersistedReadError::unreadable(error.to_string()))?;
                let peak = admitted
                    .0
                    .capacity()
                    .checked_add(decoded)
                    .ok_or(PersistedReadError::BudgetRefused)?;
                if !admitted.1.try_resize(peak) {
                    return Err(PersistedReadError::BudgetRefused);
                }
                let run = IndexRun::from_bytes(&admitted.0)
                    .map_err(|error| PersistedReadError::unreadable(error.to_string()))?;
                drop(admitted.0);
                // Decoder scratch is gone. Keep only resident bytes and the
                // run's share of publication headroom while later runs load.
                let publish = run
                    .publication_memory_bound()
                    .map_err(|error| PersistedReadError::unreadable(error.to_string()))?;
                debug_assert!(publish <= decoded, "publication fits decode headroom");
                admitted.1.try_resize(publish);
                Ok((run, size_bytes, admitted.1))
            })
            .await
            .map_err(|error| PersistedReadError::Interrupted {
                message: error.to_string(),
            })?
        })
        .await
        .map_err(|error| PersistedReadError::Interrupted {
            message: error.to_string(),
        })?
    }

    /// Deletes every file under the root that `kept` does not list. Runs only
    /// before the first sync, so no write is in flight.
    async fn delete_unregistered(&self, kept: &HashSet<object_store::path::Path>) {
        use futures::TryStreamExt;
        let listed: Vec<ObjectMeta> = match self.store.list(Some(&self.root)).try_collect().await {
            Ok(listed) => listed,
            Err(error) => {
                tracing::debug!(table = %self.table_name, %error, "Unregistered persisted secondary index runs were not listed; the next open retries");
                return;
            }
        };
        for meta in listed {
            if kept.contains(&meta.location) {
                continue;
            }
            if let Err(error) = self.store.delete(&meta.location).await {
                tracing::debug!(table = %self.table_name, run_file = %meta.location, %error, "An unregistered persisted secondary index run was not deleted; the next open retries");
            }
        }
    }
}

/// The line a reopened table logs once it has loaded its persisted index.
fn persisted_runs_loaded_message(
    table_name: &str,
    bytes: u64,
    covered: usize,
    files: usize,
) -> String {
    #[expect(
        clippy::cast_precision_loss,
        reason = "a size shown to one decimal place of a MiB"
    )]
    let mib = bytes as f64 / f64::from(1_u32 << 20);
    let coverage = if covered >= files {
        format!("covering all {files} of its files")
    } else {
        format!(
            "covering {covered} of its {files} files; the other {} are indexed in the background",
            files - covered
        )
    };
    format!(
        "Dataset '{table_name}' (cayenne): loaded its secondary index from disk ({mib:.1} MiB), {coverage}"
    )
}

/// Whether every key has a run directory of its own. Keys sharing one would
/// load each other's runs, and a lookup on one could then miss rows the
/// other's runs hold, so a table whose keys collide persists nothing.
fn distinct_dirs<'a>(mut dirs: impl Iterator<Item = &'a str>) -> bool {
    let mut unique = HashSet::new();
    dirs.all(|dir| unique.insert(dir))
}

/// A persisted run's name: a digest of the run's files and size, so the same run
/// always has the same name.
fn run_file_name(run: &IndexRun) -> String {
    let mut descriptor = Vec::new();
    for file in run.files() {
        descriptor.extend_from_slice(&(file.len() as u64).to_le_bytes());
        descriptor.extend_from_slice(file.as_bytes());
    }
    descriptor.extend_from_slice(&(run.len() as u64).to_le_bytes());
    format!("{:016x}.run", hash_index::hash_key_bytes(&[&descriptor]))
}

/// Counts one probe's coverage, and reports the probe on
/// `cayenne_lookup_index_probe_total`.
pub(crate) fn record_probe_outcome(
    table_name: &str,
    counters: &Counters,
    shape: &str,
    coverage: Coverage,
) {
    counters.record(coverage);
    telemetry::cayenne::track_lookup_index_probe(&[
        telemetry::KeyValue::new("table", table_name.to_string()),
        telemetry::KeyValue::new("shape", shape.to_string()),
    ]);
}

/// The line a table logs when a schema change alters the encoding of the keys
/// `labels` name, so their indexes are rebuilt.
fn rebuilt_index_message(table_name: &str, labels: &[String]) -> String {
    format!(
        "Dataset '{table_name}' (cayenne): the schema change altered the key columns of its secondary index on {}, so the index is rebuilt from the table's files as lookups need it; until then those lookups read every file",
        labels.join(", ")
    )
}

/// What the warning says when the memory pool refuses to fit an index.
fn refused_build_message(table_name: &str) -> String {
    format!(
        "Dataset '{table_name}' (cayenne): its secondary index was not built because the query memory pool cannot fit it now, so lookups on it scan until a later attempt fits. Raise `runtime.query.memory_limit` or remove the entry from `indexes`. See: https://spiceai.org/docs/components/data-accelerators/cayenne"
    )
}

/// The right to run a table's one background build.
///
/// Dropping it without settling — the query that claimed it was cancelled while
/// listing files, say — frees the slot for the next lookup, so a cancelled query
/// can never leave the table unindexed.
pub(crate) struct BuildClaim {
    state: Arc<LookupIndexState>,
    started: Instant,
    settled: bool,
}

impl BuildClaim {
    /// Ends a build that published its runs, or found nothing to index.
    fn published(mut self) {
        self.finish(true);
    }

    /// Ends a build that published nothing — refused, failed, or unable to list
    /// its files — and backs off the next attempt.
    pub(crate) fn unpublished(mut self) {
        self.state
            .counters
            .builds_unpublished
            .fetch_add(1, Ordering::Relaxed);
        self.finish(false);
    }

    /// Settles a build the pool refused, which [`LookupIndexState::report_refusal`]
    /// has already counted as unpublished.
    fn refused(mut self) {
        self.finish(false);
    }

    fn finish(&mut self, published: bool) {
        self.settled = true;
        self.state
            .schedule
            .lock()
            .finished(Instant::now(), self.started.elapsed(), published);
    }
}

impl Drop for BuildClaim {
    fn drop(&mut self) {
        if !self.settled {
            self.state.schedule.lock().in_flight = false;
        }
    }
}

/// Lists the files a reader can see now, for a build to publish against.
/// `None` when they cannot be listed.
pub(crate) type ListLive =
    Box<dyn FnOnce() -> futures::future::BoxFuture<'static, Option<Vec<String>>> + Send>;

/// Indexes `files` — the current snapshot's files no run covers — in the
/// background by reading them back, and publishes the runs against the file
/// set `list_live` reports once they are built, so a file that left the set
/// meanwhile is never covered. Failures leave those files read in full
/// until the schedule allows another attempt.
pub(crate) fn spawn_build(
    claim: BuildClaim,
    store: Arc<dyn ObjectStore>,
    files: Vec<IndexedFile>,
    list_live: ListLive,
) {
    tokio::spawn(async move {
        let state = Arc::clone(&claim.state);
        let table = state.table_name.clone();
        if files.is_empty() {
            claim.published();
            return;
        }
        tracing::debug!(table = %table, files = files.len(), "Indexing files no secondary index run covers");
        let first_failure = state.schedule.lock().unpublished == 0;
        let built = match read_back(&state, &store, &files).await {
            Ok(Some(builders)) => finish_runs(builders).await.map(Some),
            Ok(None) => Ok(None),
            Err(error) => Err(error),
        };
        match built {
            Ok(Some(runs)) => {
                let Some(live) = list_live().await else {
                    claim.unpublished();
                    return;
                };
                let live: HashSet<&str> = live.iter().map(|path| file_name(path)).collect();
                if state.publish_runs(runs, Some(&live)) {
                    claim.published();
                } else {
                    // `publish_runs` reported the pool's refusal, which counts it.
                    claim.refused();
                }
            }
            Ok(None) => {
                state.report_refusal();
                claim.refused();
            }
            Err(error) => {
                if first_failure {
                    tracing::warn!(
                        table = %table,
                        "Dataset '{table}' (cayenne): failed to build its secondary index, so lookups read its unindexed files in full until a later attempt succeeds. Cause: {error}"
                    );
                } else {
                    tracing::debug!(table = %table, %error, "Secondary index build failed again");
                }
                claim.unpublished();
            }
        }
    });
}

/// One run builder per key over `files`, read back from the finished files.
/// `None` when the pool refuses the builders' working memory.
///
/// Every position comes from Vortex's `row_idx()` rather than from counting the
/// rows a scan returns, so this build shares no assumption about row order with
/// the write-time runs it verifies. Decoding and encoding is CPU work, so the
/// read runs on the blocking pool and drives its file reads from there.
async fn read_back(
    state: &Arc<LookupIndexState>,
    store: &Arc<dyn ObjectStore>,
    files: &[IndexedFile],
) -> Result<Option<Vec<RunBuilder>>, String> {
    let (state, store, files) = (Arc::clone(state), Arc::clone(store), files.to_vec());
    let runtime = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || runtime.block_on(read_back_files(&state, &store, &files)))
        .await
        .map_err(|e| format!("index build task failed: {e}"))?
}

async fn read_back_files(
    state: &LookupIndexState,
    store: &Arc<dyn ObjectStore>,
    files: &[IndexedFile],
) -> Result<Option<Vec<RunBuilder>>, String> {
    use vortex::expr::{get_item, pack, root};

    let shapes = state.shapes.load_full();
    let mut builders: Vec<RunBuilder> = shapes.iter().map(|shape| shape.run_builder()).collect();
    let reservation = state.build_reservation();
    // Every key column of every key, read once per file.
    let mut columns: Vec<&KeyColumn> = Vec::new();
    for shape in shapes.iter() {
        for column in &shape.columns {
            if !columns.iter().any(|seen| seen.name == column.name) {
                columns.push(column);
            }
        }
    }
    let session = VortexSession::default();
    let mut target_fields: Vec<FieldRef> =
        columns.iter().map(|column| column.stored_field()).collect();
    target_fields.push(Arc::new(Field::new(
        READ_BACK_POSITION_COLUMN,
        DataType::UInt64,
        false,
    )));
    let target = Field::new_struct("", target_fields, false);
    let projection = pack(
        columns
            .iter()
            .map(|column| (column.name.clone(), get_item(column.name.as_str(), root())))
            .chain(std::iter::once((
                READ_BACK_POSITION_COLUMN.to_string(),
                row_idx(),
            ))),
        Nullability::NonNullable,
    );

    for file in files {
        let name = file_name(&file.path);
        // Covered even when it holds no row, or only NULL keys.
        for builder in &mut builders {
            builder.add_file(name).map_err(|e| e.to_string())?;
        }
        let vxf = session
            .open_options()
            .open_object_store(store, object_store::path::Path::from(file.path.as_str()))
            .await
            .map_err(|e| format!("open {}: {e}", file.path))?;
        let file_projection = projection
            .bind(vxf.dtype())
            .map_err(|e| format!("bind projection {}: {e}", file.path))?;
        let mut stream = vxf
            .scan()
            .map_err(|e| format!("scan {}: {e}", file.path))?
            .with_projection(file_projection)
            .into_stream()
            .map_err(|e| format!("stream {}: {e}", file.path))?;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| format!("read {}: {e}", file.path))?;
            if chunk.is_empty() {
                continue;
            }
            let mut ctx = session.create_execution_ctx();
            let array = session
                .arrow()
                .execute_arrow(chunk, Some(&target), &mut ctx)
                .map_err(|e| format!("to arrow {}: {e}", file.path))?;
            let batch = RecordBatch::from(
                array
                    .as_struct_opt()
                    .ok_or_else(|| format!("{}: scan did not return a struct", file.path))?,
            );
            let positions = batch
                .column_by_name(READ_BACK_POSITION_COLUMN)
                .and_then(|column| column.as_primitive_opt::<UInt64Type>())
                .ok_or_else(|| format!("{}: row positions are not UInt64", file.path))?;
            for (builder, shape) in builders.iter_mut().zip(shapes.iter()) {
                let key_columns = shape.key_columns(&batch)?;
                builder
                    .add_batch_at(name, positions.values(), &key_columns)
                    .map_err(|e| e.to_string())?;
            }
            let bytes = builders
                .iter()
                .map(RunBuilder::heap_bytes)
                .sum::<usize>()
                .saturating_mul(2);
            if reservation.try_resize(bytes).is_err() {
                return Ok(None);
            }
        }
    }
    Ok(Some(builders))
}

/// A diff of the published runs against a read-back of the files they cover.
///
/// A write's runs trust that the sink reports each batch's position in its
/// file, so they are only as good as that invariant. A read-back takes every
/// position from Vortex's own `row_idx()`, so diffing the two turns that
/// invariant from an assumption into a check.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LookupIndexVerification {
    /// The snapshot whose files were compared.
    pub snapshot_id: String,
    /// Files of the snapshot the published runs cover, all of them compared.
    /// More than one means the writer's file-roll path — where positions
    /// restart — was exercised.
    pub files: usize,
    /// Files of the snapshot no run covers, which lookups read in full.
    pub uncovered_files: usize,
    /// Distinct keys per lookup shape, as `(shape, published, read-back)`.
    pub keys_per_shape: Vec<(String, usize, usize)>,
    /// Row addresses per lookup shape, as `(shape, published, read-back)`.
    pub postings_per_shape: Vec<(String, usize, usize)>,
    /// Distinct keys whose row addresses were compared.
    pub keys_compared: usize,
    /// Every disagreement found, described in full.
    pub mismatches: Vec<String>,
}

impl LookupIndexVerification {
    /// `true` when the two agree on every key and every row address.
    #[must_use]
    pub fn agrees(&self) -> bool {
        self.mismatches.is_empty()
    }
}

/// How many entry disagreements a verification spells out before summarizing.
const MAX_REPORTED_MISMATCHES: usize = 20;

/// `(key word, file name, position)` entries of one key's runs, restricted
/// to `files`.
type Entries = std::collections::BTreeSet<(u64, String, u64)>;

fn entries_of(runs: &[Arc<IndexRun>], files: &HashSet<&str>) -> Entries {
    let mut entries = Entries::new();
    for run in runs {
        run.for_each_row(|word, file, position| {
            if files.contains(file) {
                entries.insert((word, file.to_string(), position));
            }
        });
    }
    entries
}

fn distinct_keys(entries: &Entries) -> usize {
    let mut keys = 0;
    let mut last: Option<u64> = None;
    for &(word, _, _) in entries {
        if last != Some(word) {
            keys += 1;
            last = Some(word);
        }
    }
    keys
}

/// Compares two indexes' entries for one key, appending every disagreement.
fn diff_entries(
    label: &str,
    published: &Entries,
    read: &Entries,
    report: &mut LookupIndexVerification,
) {
    let (published_keys, read_keys) = (distinct_keys(published), distinct_keys(read));
    report
        .keys_per_shape
        .push((label.to_string(), published_keys, read_keys));
    report
        .postings_per_shape
        .push((label.to_string(), published.len(), read.len()));
    report.keys_compared += read_keys;
    let mut disagreements = 0usize;
    for entry in published.symmetric_difference(read) {
        disagreements += 1;
        if disagreements <= MAX_REPORTED_MISMATCHES {
            let side = if read.contains(entry) {
                "read-back"
            } else {
                "published"
            };
            report.mismatches.push(format!(
                "{label}: only the {side} index has key {:?} at {} row {}",
                entry.0, entry.1, entry.2
            ));
        }
    }
    if disagreements > MAX_REPORTED_MISMATCHES {
        report.mismatches.push(format!(
            "{label}: {} further entries disagree",
            disagreements - MAX_REPORTED_MISMATCHES
        ));
    }
}

/// Diffs `view`'s runs against a read-back of the `files` it covers: every
/// file a lookup on current snapshot `snapshot_id` reads, its own and the
/// protected snapshots'.
pub(crate) async fn verify_against_read_back(
    state: &Arc<LookupIndexState>,
    view: Arc<LookupIndexView>,
    snapshot_id: String,
    store: &Arc<dyn ObjectStore>,
    files: Vec<IndexedFile>,
) -> Result<LookupIndexVerification, String> {
    let (covered, uncovered): (Vec<IndexedFile>, Vec<IndexedFile>) =
        files.into_iter().partition(|file| view.covers(&file.path));
    let builders = read_back(state, store, &covered)
        .await?
        .ok_or_else(|| "the memory pool refused the read-back build".to_string())?;
    let read = finish_runs(builders).await?;
    // Decoding every entry of both is CPU work, like building them.
    tokio::task::spawn_blocking(move || {
        let names: HashSet<&str> = covered.iter().map(|file| file_name(&file.path)).collect();
        let mut report = LookupIndexVerification {
            snapshot_id,
            files: covered.len(),
            uncovered_files: uncovered.len(),
            ..LookupIndexVerification::default()
        };
        for ((shape, read), key) in view.views.iter().zip(read).zip(view.shapes.iter()) {
            let published = entries_of(&shape.run_list(), &names);
            let read = entries_of(&[Arc::new(read)], &names);
            diff_entries(&key.label, &published, &read, &mut report);
        }
        report
    })
    .await
    .map_err(|e| format!("index verification task failed: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use datafusion::execution::memory_pool::{GreedyMemoryPool, UnboundedMemoryPool};

    fn unbounded_pool() -> Arc<dyn MemoryPool> {
        Arc::new(UnboundedMemoryPool::default())
    }

    fn account(pool: &Arc<dyn MemoryPool>) -> Arc<CayenneMemoryAccount> {
        Arc::new(CayenneMemoryAccount::new("lookup_index_test", pool))
    }

    fn spec(columns: &[&str]) -> KeySpec {
        KeySpec::new(columns.iter().map(|c| (*c).to_string()).collect()).expect("columns")
    }

    /// Counts the build requests a runtime lookup makes.
    #[derive(Default)]
    struct BuildRequests(Arc<AtomicU64>);

    impl BuildRequests {
        fn callback(&self) -> Arc<dyn Fn() + Send + Sync> {
            let count = Arc::clone(&self.0);
            Arc::new(move || {
                count.fetch_add(1, Ordering::Relaxed);
            })
        }

        fn count(&self) -> u64 {
            self.0.load(Ordering::Relaxed)
        }
    }

    /// A scan's view of an indexed test file: size 1, modified at the epoch.
    fn scan_file(path: &str) -> ObjectMeta {
        let mut file = PartitionedFile::new(path.to_string(), 1).object_meta;
        file.last_modified = chrono::DateTime::from_timestamp_millis(0).expect("epoch");
        file
    }

    /// A hash join's dynamic filter after its build side completed with `values`.
    fn completed_dynamic_filter(
        column: &Arc<dyn PhysicalExpr>,
        values: &[i64],
    ) -> Arc<dyn PhysicalExpr> {
        let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::clone(column)],
            Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
        ));
        dynamic
            .update(tenant_in_list(column, values))
            .expect("runtime filter");
        dynamic.mark_complete();
        dynamic
    }

    fn tenant_in_list(column: &Arc<dyn PhysicalExpr>, values: &[i64]) -> Arc<dyn PhysicalExpr> {
        let schema = arrow_schema::Schema::new(vec![Field::new("tenant", DataType::Int64, true)]);
        Arc::new(
            InListExpr::try_new(
                Arc::clone(column),
                values
                    .iter()
                    .map(|value| {
                        Arc::new(Literal::new(ScalarValue::Int64(Some(*value))))
                            as Arc<dyn PhysicalExpr>
                    })
                    .collect(),
                false,
                &schema,
            )
            .expect("IN list"),
        )
    }

    #[test]
    fn keys_are_labelled_by_their_columns_and_deduplicated() {
        assert!(KeySpec::new(Vec::new()).is_none());
        assert_eq!(spec(&["order_id"]).label(), "order_id");
        assert_eq!(spec(&["tenant", "service"]).label(), "(tenant, service)");
        let specs = KeySpec::from_indexes(&[
            vec!["a".to_string()],
            vec!["a".to_string(), "b".to_string()],
            vec!["a".to_string()],
            Vec::new(),
        ]);
        assert_eq!(
            specs.iter().map(KeySpec::label).collect::<Vec<_>>(),
            vec!["a", "(a, b)"]
        );
    }

    #[tokio::test]
    async fn runtime_filter_cache_tracks_expression_generation() {
        let pool = unbounded_pool();
        let schema = arrow_schema::Schema::new(vec![Field::new("tenant", DataType::Int64, true)]);
        let state = LookupIndexState::new(
            "dynamic_generation",
            vec![spec(&["tenant"])],
            &schema,
            Arc::clone(&pool),
            account(&pool),
            Arc::default(),
            None,
        )
        .expect("indexable")
        .expect("state");
        let builds = BuildRequests::default();
        let provider = DynamicLookupAccessPlanProvider::new(
            Arc::clone(&state),
            state.published(),
            Arc::new([]),
            Some(builds.callback()),
        );
        let column = Arc::new(Column::new("tenant", 0)) as Arc<dyn PhysicalExpr>;
        let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::clone(&column)],
            Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
        ));
        let predicate = Arc::clone(&dynamic) as Arc<dyn PhysicalExpr>;

        assert!(provider.resolve(Some(&predicate)).await.is_none());
        assert_eq!(
            state.counters().none,
            0,
            "an unresolved filter is not probed"
        );
        assert_eq!(builds.count(), 0);

        dynamic
            .update(tenant_in_list(&column, &[1, 2]))
            .expect("first update");
        dynamic.mark_complete();
        assert!(provider.resolve(Some(&predicate)).await.is_none());
        assert_eq!(state.counters().none, 1);
        assert_eq!(builds.count(), 1);
        assert!(provider.resolve(Some(&predicate)).await.is_none());
        assert_eq!(
            state.counters().none,
            1,
            "one generation is probed only once"
        );
        assert_eq!(builds.count(), 1);

        dynamic
            .update(tenant_in_list(&column, &[3]))
            .expect("second update");
        assert!(provider.resolve(Some(&predicate)).await.is_none());
        assert_eq!(
            state.counters().none,
            2,
            "a later generation is resolved independently"
        );
        assert_eq!(builds.count(), 2);
    }

    #[test]
    fn runtime_filter_refuses_in_lists_nested_in_case() {
        let column = Arc::new(Column::new("tenant", 0)) as Arc<dyn PhysicalExpr>;
        let in_list = tenant_in_list(&column, &[1, 2]);
        let case = Arc::new(
            datafusion_physical_expr::expressions::CaseExpr::try_new(
                None,
                vec![(
                    Arc::new(Literal::new(ScalarValue::Boolean(Some(true))))
                        as Arc<dyn PhysicalExpr>,
                    in_list,
                )],
                Some(Arc::new(Literal::new(ScalarValue::Boolean(Some(false))))),
            )
            .expect("CASE"),
        ) as Arc<dyn PhysicalExpr>;
        let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![column],
            Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
        ));
        dynamic.update(case).expect("update");
        dynamic.mark_complete();
        let predicate = dynamic as Arc<dyn PhysicalExpr>;

        assert!(dynamic_in_list_expr(&predicate, &["tenant".to_string()]).is_none());
    }

    #[test]
    fn runtime_filter_column_names_are_case_sensitive() {
        let column = Arc::new(Column::new("tenant", 0)) as Arc<dyn PhysicalExpr>;
        let in_list = tenant_in_list(&column, &[1, 2]);
        let in_list = in_list.downcast_ref::<InListExpr>().expect("IN list");

        assert!(in_list_matches_columns(in_list, &["tenant".to_string()]));
        assert!(!in_list_matches_columns(in_list, &["Tenant".to_string()]));
    }

    #[test]
    fn runtime_key_extraction_refuses_too_many_distinct_keys() {
        let column = Arc::new(Column::new("tenant", 0)) as Arc<dyn PhysicalExpr>;
        let values = (0..=RUNTIME_INDEX_MAX_KEYS)
            .map(|value| i64::try_from(value).expect("small key"))
            .collect::<Vec<_>>();
        let in_list = tenant_in_list(&column, &values);
        let keys = in_list_keys(&in_list, &["tenant".to_string()]);
        assert!(
            keys.is_none(),
            "extraction materialized {} keys instead of declining at the distinct-key bound",
            keys.as_ref().map_or(0, Vec::len)
        );
    }

    #[test]
    fn runtime_key_extraction_deduplicates_before_applying_the_bound() {
        let column = Arc::new(Column::new("tenant", 0)) as Arc<dyn PhysicalExpr>;
        let values = (0..RUNTIME_INDEX_MAX_KEYS)
            .cycle()
            .take(RUNTIME_INDEX_MAX_KEYS * 3)
            .map(|value| i64::try_from(value).expect("small key"))
            .collect::<Vec<_>>();
        let in_list = tenant_in_list(&column, &values);
        let keys = in_list_keys(&in_list, &["tenant".to_string()]).expect("bounded distinct keys");
        assert_eq!(keys.len(), RUNTIME_INDEX_MAX_KEYS);
        let expected: HashSet<_> = (0..RUNTIME_INDEX_MAX_KEYS)
            .map(|value| {
                vec![ScalarValue::Int64(Some(
                    i64::try_from(value).expect("small key"),
                ))]
            })
            .collect();
        assert_eq!(keys.into_iter().collect::<HashSet<_>>(), expected);
    }

    #[test]
    fn runtime_key_extraction_stops_at_the_first_excess_distinct_key() {
        let visited = std::cell::Cell::new(0);
        let values = (0..RUNTIME_INDEX_MAX_KEYS * 2)
            .map(|value| {
                Arc::new(Literal::new(ScalarValue::Int64(Some(
                    i64::try_from(value).expect("small key"),
                )))) as Arc<dyn PhysicalExpr>
            })
            .collect::<Vec<_>>();
        let keys =
            collect_runtime_keys(values.iter().inspect(|_| visited.set(visited.get() + 1)), 1);
        assert!(
            keys.is_none(),
            "an oversized list must not return partial keys"
        );
        assert_eq!(visited.get(), RUNTIME_INDEX_MAX_KEYS + 1);
    }

    #[test]
    fn runtime_key_extraction_preserves_correlated_non_null_tuples() {
        let fields = vec![
            Arc::new(Field::new("tenant", DataType::Int64, true)),
            Arc::new(Field::new("service", DataType::Utf8, true)),
        ];
        let literal = |tenant: Option<i64>, service: Option<&str>| {
            Arc::new(Literal::new(ScalarValue::Struct(Arc::new(
                arrow::array::StructArray::new(
                    fields.clone().into(),
                    vec![
                        Arc::new(Int64Array::from(vec![tenant])),
                        Arc::new(StringArray::from(vec![service])),
                    ],
                    None,
                ),
            )))) as Arc<dyn PhysicalExpr>
        };
        let mut values = vec![literal(Some(1), Some("a")); RUNTIME_INDEX_MAX_KEYS * 2];
        values.extend([
            literal(Some(2), Some("b")),
            literal(None, Some("a")),
            literal(Some(1), None),
        ]);
        let keys = collect_runtime_keys(&values, 2).expect("two distinct non-null tuples");
        assert_eq!(
            keys.into_iter().collect::<HashSet<_>>(),
            HashSet::from([
                vec![ScalarValue::Int64(Some(1)), ScalarValue::from("a")],
                vec![ScalarValue::Int64(Some(2)), ScalarValue::from("b")],
            ])
        );
        let oversized = (0..RUNTIME_INDEX_MAX_KEYS * 2)
            .map(|value| literal(Some(i64::try_from(value).expect("small key")), Some("a")))
            .collect::<Vec<_>>();
        let visited = std::cell::Cell::new(0);
        assert!(
            collect_runtime_keys(
                oversized.iter().inspect(|_| visited.set(visited.get() + 1)),
                2,
            )
            .is_none()
        );
        assert_eq!(visited.get(), RUNTIME_INDEX_MAX_KEYS + 1);
    }

    #[test]
    fn a_column_matching_by_case_only_twice_is_refused() {
        let schema = arrow_schema::Schema::new(vec![
            Field::new("TenantId", DataType::Utf8, false),
            Field::new("tenantid", DataType::Utf8, false),
            Field::new("Service", DataType::Utf8, true),
        ]);
        assert_eq!(
            KeyColumn::resolve(&schema, "tenantid").expect("exact").name,
            "tenantid"
        );
        KeyColumn::resolve(&schema, "TENANTID").expect_err("a name matching two columns by case");
        let service = KeyColumn::resolve(&schema, "service").expect("unique by case");
        assert_eq!(service.name, "Service");
        assert!(service.nullable);
    }

    /// Floating-point columns can be indexed: the key encoding gives every
    /// pair of floats SQL can hold equal one encoding.
    #[test]
    fn floating_point_key_columns_can_be_indexed() {
        for data_type in [DataType::Float16, DataType::Float32, DataType::Float64] {
            let schema =
                arrow_schema::Schema::new(vec![Field::new("score", data_type.clone(), false)]);
            KeyColumn::resolve(&schema, "score").expect("a float column resolves");
            supported_key_type(&data_type).expect("a float key is supported");
        }
    }

    #[test]
    fn background_builds_are_paced_and_back_off_while_they_publish_nothing() {
        let start = Instant::now();
        let mut schedule = BuildSchedule::default();
        assert!(schedule.can_start(start));
        schedule.in_flight = true;
        assert!(!schedule.can_start(start), "one build at a time");

        // A published build waits the minimum, or ten times its own duration.
        schedule.finished(start, Duration::from_millis(10), true);
        assert!(!schedule.can_start(start + Duration::from_millis(999)));
        assert!(schedule.can_start(start + MIN_BUILD_INTERVAL));
        schedule.finished(start, Duration::from_secs(3), true);
        assert!(!schedule.can_start(start + Duration::from_secs(29)));
        assert!(schedule.can_start(start + Duration::from_secs(30)));

        // Each build in a row that publishes nothing doubles the wait.
        let mut waits = Vec::new();
        for _ in 0..4 {
            schedule.in_flight = true;
            schedule.finished(start, Duration::from_millis(10), false);
            waits.push(schedule.not_before.expect("scheduled") - start);
        }
        assert_eq!(
            waits,
            vec![
                Duration::from_secs(2),
                Duration::from_secs(4),
                Duration::from_secs(8),
                Duration::from_secs(16)
            ]
        );
        for _ in 0..40 {
            schedule.finished(start, Duration::from_millis(10), false);
        }
        assert_eq!(
            schedule.not_before.expect("scheduled") - start,
            MAX_BUILD_INTERVAL,
            "the backoff is capped"
        );
        schedule.finished(start, Duration::from_millis(10), true);
        assert_eq!(
            schedule.unpublished, 0,
            "a published index resets the backoff"
        );
    }

    fn keyed_schema() -> Arc<arrow_schema::Schema> {
        Arc::new(arrow_schema::Schema::new(vec![
            Field::new("tenant", DataType::Int64, true),
            Field::new("service", DataType::Utf8, true),
            Field::new("value", DataType::Int64, false),
        ]))
    }

    fn keyed_batch(tenants: &[Option<i64>], services: &[Option<&str>]) -> RecordBatch {
        let values: Vec<i64> = (0..tenants.len())
            .map(|row| i64::try_from(row).expect("small"))
            .collect();
        RecordBatch::try_new(
            keyed_schema(),
            vec![
                Arc::new(Int64Array::from(tenants.to_vec())),
                Arc::new(StringArray::from(services.to_vec())),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .expect("batch")
    }

    fn keyed_state(pool: &Arc<dyn MemoryPool>) -> Arc<LookupIndexState> {
        LookupIndexState::new(
            "keyed",
            vec![spec(&["tenant"]), spec(&["tenant", "service"])],
            &keyed_schema(),
            Arc::clone(pool),
            account(pool),
            Arc::default(),
            None,
        )
        .expect("indexable")
        .expect("state")
    }

    /// A table whose writes keep failing to index warns once, not on every
    /// write, until runs publish again.
    #[tokio::test]
    async fn a_failing_write_warns_once_until_runs_publish() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 << 20));
        let state = keyed_state(&pool);
        state.write_unindexed("first");
        assert!(state.write_failure.warned(), "the first failure warns");
        state.write_unindexed("second");
        write(
            &state,
            &[("a.vortex", 0, keyed_batch(&[Some(1)], &[Some("x")]))],
        )
        .await;
        assert!(
            !state.write_failure.warned(),
            "a failure after runs published warns again"
        );
        state.write_unindexed("after a publish");
        assert_eq!(state.counters().builds_unpublished, 3);
    }

    /// A join's runtime probe answers for the files its scan reads, and no
    /// other: a key held only by a file outside the scan selects nothing there,
    /// so the probe is fully covered and reads nothing, not a selection of a
    /// file the scan never opens.
    #[tokio::test]
    async fn a_runtime_probe_counts_only_the_files_its_scan_reads() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 << 20));
        let state = keyed_state(&pool);
        write(
            &state,
            &[("a.vortex", 0, keyed_batch(&[Some(1)], &[Some("x")]))],
        )
        .await;
        write(
            &state,
            &[("b.vortex", 0, keyed_batch(&[Some(2)], &[Some("y")]))],
        )
        .await;
        let view = state.published();
        let scan = [scan_file("table/snapshot/a.vortex")];
        let before = state.counters();
        let probed =
            state.probe_runtime_filter(0, &view, &scan, &[vec![ScalarValue::Int64(Some(2))]]);
        let after = state.counters();
        assert!(
            matches!(
                probed,
                RuntimeProbe::Selection {
                    uncovered: false,
                    ..
                }
            ),
            "the scan's one file is covered"
        );
        assert_eq!(
            (
                after.full - before.full,
                after.partial - before.partial,
                after.candidate_files - before.candidate_files
            ),
            (1, 0, 0),
            "a key only in a file the scan does not read is a fully covered probe that reads nothing: {before:?} -> {after:?}"
        );
    }

    /// A schema change resets the index of exactly the keys whose encoding it
    /// changes: `service` becoming `LargeUtf8` changes `(tenant, service)`,
    /// whose index then covers nothing until it is rebuilt, and leaves
    /// `tenant`'s index as it was. A key column of a type the index cannot hold
    /// is refused before anything is swapped.
    #[tokio::test]
    async fn a_schema_change_resets_only_the_keys_whose_encoding_it_changes() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 << 20));
        let state = keyed_state(&pool);
        write(
            &state,
            &[(
                "a.vortex",
                0,
                keyed_batch(&[Some(1), Some(2)], &[Some("x"), Some("y")]),
            )],
        )
        .await;
        let covers = |state: &LookupIndexState| -> Vec<bool> {
            state
                .shapes
                .load()
                .iter()
                .map(|shape| shape.index.view().covers("a.vortex"))
                .collect()
        };
        assert_eq!(covers(&state), vec![true, true]);

        let nested = arrow_schema::Schema::new(vec![
            Field::new(
                "tenant",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            ),
            Field::new("service", DataType::Utf8, true),
            Field::new("value", DataType::Int64, false),
        ]);
        let refused = state
            .shapes_for(&nested)
            .err()
            .expect("a nested key is refused");
        assert!(refused.contains("tenant"), "{refused}");
        assert_eq!(
            covers(&state),
            vec![true, true],
            "a refused change swaps nothing"
        );

        let large = arrow_schema::Schema::new(vec![
            Field::new("tenant", DataType::Int64, true),
            Field::new("service", DataType::LargeUtf8, true),
            Field::new("value", DataType::Int64, false),
        ]);
        state.adopt_shapes(state.shapes_for(&large).expect("indexable"));
        assert_eq!(
            covers(&state),
            vec![true, false],
            "only the key whose encoding changed is reset"
        );
        let view = state.published();
        assert!(
            !view.covers("table/snapshot/a.vortex"),
            "the published view must not cover a file the reset key does not"
        );
    }

    fn path(name: &str) -> object_store::path::Path {
        object_store::path::Path::from(format!("table/snapshot/{name}"))
    }

    /// Writes `batches` through a write observer, each as `(file, first
    /// position, batch)`, and publishes the write's runs.
    async fn write(state: &Arc<LookupIndexState>, batches: &[(&str, u64, RecordBatch)]) {
        use vortex_datafusion::VortexWriteObserver;
        let observer = state.write_observer(false);
        for (file, first, batch) in batches {
            observer.batch_written(&path(file), *first, batch);
        }
        state.finish_write(&observer).await;
    }

    /// With its indexing thread behind and its queue full, an append drops
    /// its run (its files are read in full until a background build indexes
    /// them) rather than wait, while a rewrite indexes the overflow itself, so
    /// every file it swaps in is covered.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_full_queue_drops_an_append_but_not_a_rewrite() {
        use vortex_datafusion::VortexWriteObserver;
        for replaces in [false, true] {
            let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
            let state = keyed_state(&pool);
            let (go, gate) = std::sync::mpsc::channel();
            // A one-byte queue: every batch finds it full.
            let observer = state.write_observer_with(replaces, 1, Some(gate));
            let files: Vec<String> = (0..6).map(|i| format!("f{i}.vortex")).collect();
            for (i, file) in files.iter().enumerate() {
                let tenant = i64::try_from(i).expect("small");
                let services: Vec<String> = (0..10).map(|row| format!("s{i}-{row}")).collect();
                let services: Vec<Option<&str>> =
                    services.iter().map(|s| Some(s.as_str())).collect();
                observer.batch_written(
                    &path(file),
                    0,
                    &keyed_batch(&[Some(tenant); 10], &services),
                );
            }
            go.send(()).expect("release the indexing thread");
            let unpublished = state.counters.builds_unpublished.load(Ordering::Relaxed);
            state.finish_write(&observer).await;
            let covered: Vec<bool> = files
                .iter()
                .map(|file| state.published().covers(&format!("table/snapshot/{file}")))
                .collect();
            let unpublished =
                state.counters.builds_unpublished.load(Ordering::Relaxed) - unpublished;
            if replaces {
                assert_eq!(
                    (
                        covered,
                        observer.shared.inline_batches.load(Ordering::Relaxed),
                        unpublished
                    ),
                    (vec![true; 6], 6, 0),
                    "a rewrite must cover every file it writes"
                );
            } else {
                assert_eq!(
                    (covered, unpublished),
                    (vec![false; 6], 1),
                    "an append that outpaced its index must drop its run"
                );
            }
        }
    }

    /// A write whose rows cannot all be indexed publishes no run, so none of
    /// its files is covered and a lookup reads them in full. The case that
    /// matters is a file whose first batch indexed and whose second did not:
    /// publishing that run would claim the file while missing the second
    /// batch's rows, and a lookup for them would skip the file.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_write_that_fails_to_index_covers_none_of_its_files() {
        use vortex_datafusion::VortexWriteObserver;
        // A batch without the `service` key column.
        let unkeyed = RecordBatch::try_new(
            Arc::new(arrow_schema::Schema::new(vec![Field::new(
                "tenant",
                DataType::Int64,
                true,
            )])),
            vec![Arc::new(Int64Array::from(vec![Some(7_i64); 3]))],
        )
        .expect("batch");
        for replaces in [false, true] {
            let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
            let state = keyed_state(&pool);
            let observer = state.write_observer(replaces);
            observer.batch_written(
                &path("good.vortex"),
                0,
                &keyed_batch(&[Some(1); 3], &[Some("a"), Some("b"), Some("c")]),
            );
            observer.batch_written(
                &path("split.vortex"),
                0,
                &keyed_batch(&[Some(7); 3], &[Some("x"), Some("y"), Some("z")]),
            );
            observer.batch_written(&path("split.vortex"), 3, &unkeyed);
            let unpublished = state.counters.builds_unpublished.load(Ordering::Relaxed);
            state.finish_write(&observer).await;
            let covered: Vec<bool> = ["good.vortex", "split.vortex"]
                .iter()
                .map(|file| state.published().covers(&format!("table/snapshot/{file}")))
                .collect();
            assert_eq!(
                (
                    covered,
                    state.counters.builds_unpublished.load(Ordering::Relaxed) - unpublished
                ),
                (vec![false, false], 1),
                "a write with an unindexed batch (replaces: {replaces}) must cover none of its files"
            );
            assert_eq!(
                state.counters().index_bytes,
                0,
                "an unpublished run holds nothing"
            );
        }
    }

    /// Coverage counts the table's current files per key: none before a scan
    /// has listed them, then the files a published run holds against the
    /// rest, following publishes and retirements.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn coverage_counts_each_keys_covered_and_uncovered_files() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let state = keyed_state(&pool);
        assert_eq!(state.coverage(), None, "no file set listed yet");
        let files = ["a.vortex", "b.vortex", "c.vortex"];
        state.reconcile(
            "s1",
            FileSetVersion::default(),
            files
                .iter()
                .map(|file| format!("table/snapshot/{file}"))
                .collect::<Vec<_>>()
                .iter()
                .map(String::as_str),
        );
        assert_eq!(
            state.coverage(),
            Some(vec![
                ("tenant".to_string(), 0, 3),
                ("(tenant, service)".to_string(), 0, 3)
            ])
        );
        write(
            &state,
            &[(
                "a.vortex",
                0,
                keyed_batch(&[Some(1); 2], &[Some("x"), Some("y")]),
            )],
        )
        .await;
        assert_eq!(
            state.coverage(),
            Some(vec![
                ("tenant".to_string(), 1, 2),
                ("(tenant, service)".to_string(), 1, 2)
            ])
        );
    }

    #[test]
    fn the_loaded_runs_message_names_the_table_its_size_and_its_coverage() {
        assert_eq!(
            persisted_runs_loaded_message("orders", 5 << 20, 20, 20),
            "Dataset 'orders' (cayenne): loaded its secondary index from disk (5.0 MiB), covering all 20 of its files"
        );
        assert_eq!(
            persisted_runs_loaded_message("orders", 3 << 19, 18, 20),
            "Dataset 'orders' (cayenne): loaded its secondary index from disk (1.5 MiB), covering 18 of its 20 files; the other 2 are indexed in the background"
        );
        assert_eq!(
            persisted_runs_loaded_message("orders", 5 << 20, 4, 4),
            "Dataset 'orders' (cayenne): loaded its secondary index from disk (5.0 MiB), covering all 4 of its files"
        );
    }

    /// A batch that arrives after its write's index was finished has no run
    /// to join: its file stays uncovered while the finished run still covers
    /// the files it holds in full.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_batch_after_the_write_finished_leaves_its_file_uncovered() {
        use vortex_datafusion::VortexWriteObserver;
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let state = keyed_state(&pool);
        let observer = state.write_observer(false);
        observer.batch_written(
            &path("first.vortex"),
            0,
            &keyed_batch(&[Some(1); 2], &[Some("a"), Some("b")]),
        );
        state.finish_write(&observer).await;
        observer.batch_written(
            &path("late.vortex"),
            0,
            &keyed_batch(&[Some(2); 2], &[Some("c"), Some("d")]),
        );
        let view = state.published();
        assert!(view.covers("table/snapshot/first.vortex"));
        assert!(
            !view.covers("table/snapshot/late.vortex"),
            "a late batch's file must be read in full"
        );
    }

    /// An append arrives in a burst far faster than its keys encode, and the
    /// queue absorbs it: an ordinary append of many batches is covered, not
    /// dropped as behind.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_burst_of_batches_is_queued_not_dropped() {
        use vortex_datafusion::VortexWriteObserver;
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let state = keyed_state(&pool);
        let observer = state.write_observer(false);
        let names: Vec<String> = (0..1_000).map(|row| format!("s{row}")).collect();
        let services: Vec<Option<&str>> = names.iter().map(|name| Some(name.as_str())).collect();
        for batch in 0..200_u64 {
            let tenant = i64::try_from(batch).expect("small");
            observer.batch_written(
                &path("burst.vortex"),
                batch * 1_000,
                &keyed_batch(&[Some(tenant); 1_000], &services),
            );
        }
        let unpublished = state.counters.builds_unpublished.load(Ordering::Relaxed);
        state.finish_write(&observer).await;
        assert_eq!(
            (
                state.published().covers("table/snapshot/burst.vortex"),
                state.counters.builds_unpublished.load(Ordering::Relaxed) - unpublished,
            ),
            (true, 0),
            "an ordinary append must be indexed, not dropped as behind"
        );
    }

    /// A write too large to finish on the write path has its run published
    /// from the background even when its files are not yet visible — a
    /// compaction can still be committing when its run is ready — so its
    /// files end up covered rather than dropped as already gone.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_large_write_is_covered_even_before_it_is_visible() {
        use vortex_datafusion::VortexWriteObserver;
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let state = keyed_state(&pool);
        let rows = DEFER_FINISH_ROWS + 1;
        let tenants: Vec<Option<i64>> = (0..rows)
            .map(|row| Some(i64::try_from(row % 1_000).expect("small")))
            .collect();
        let names: Vec<String> = (0..rows).map(|row| format!("s{row}")).collect();
        let services: Vec<Option<&str>> = names.iter().map(|name| Some(name.as_str())).collect();
        let observer = state.write_observer(false);
        observer.batch_written(&path("large.vortex"), 0, &keyed_batch(&tenants, &services));
        // No reconcile ever reports the file visible.
        state.finish_write(&observer).await;
        let deadline = Instant::now() + Duration::from_mins(1);
        while !state.published().covers("table/snapshot/large.vortex") {
            assert!(
                Instant::now() < deadline,
                "a large write's run was never published"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let view = state.published();
        assert_eq!(view.rows, rows, "the run holds every row of the write");
    }

    fn tenant_values(values: &[i64]) -> impl Fn(&str) -> Option<Vec<ScalarValue>> + '_ {
        move |column: &str| {
            (column == "tenant").then(|| {
                values
                    .iter()
                    .map(|v| ScalarValue::Int64(Some(*v)))
                    .collect()
            })
        }
    }

    fn selection(probe: LookupProbe) -> LookupSelection {
        match probe {
            LookupProbe::Selection(selection) => selection,
            LookupProbe::Fallback(explain) => panic!("expected a selection, got {explain:?}"),
        }
    }

    fn group(names: &[&str]) -> Vec<FileGroup> {
        vec![FileGroup::new(
            names
                .iter()
                .map(|name| PartitionedFile::new(path(name).to_string(), 1))
                .collect(),
        )]
    }

    fn no_table_plans() -> Arc<dyn VortexAccessPlanProvider> {
        Arc::new(NoPlans)
    }

    #[derive(Debug)]
    struct NoPlans;

    impl VortexAccessPlanProvider for NoPlans {
        fn access_plan_for_file(&self, _: &PartitionedFile) -> Option<Arc<VortexAccessPlan>> {
            None
        }

        fn adjust_statistics(&self, _: &ObjectMeta, statistics: Statistics) -> Statistics {
            statistics
        }
    }

    /// `(file, position)` rows of one key.
    type FileRows = Vec<(String, u64)>;

    /// Every write, file roll and batch boundary lands where a brute-force
    /// map of the written rows says, for single and composite keys, single
    /// values and `IN` lists, and NULL keys never match.
    #[tokio::test]
    async fn written_rows_probe_like_a_brute_force_map() {
        let pool = unbounded_pool();
        let state = keyed_state(&pool);
        let mut truth: std::collections::BTreeMap<(i64, Option<String>), FileRows> =
            std::collections::BTreeMap::new();
        for write_no in 0..3_i64 {
            let mut batches = Vec::new();
            for file_no in 0..2 {
                let file = format!("w{write_no}_p00{file_no}_00000.vortex");
                let mut first = 0_u64;
                for batch_no in 0..3_i64 {
                    let rows = 50;
                    let tenants: Vec<Option<i64>> = (0..rows)
                        .map(|row| (row % 11 != 0).then_some((row * 7 + batch_no + write_no) % 13))
                        .collect();
                    let services: Vec<Option<String>> = (0..rows)
                        .map(|row| (row % 5 != 0).then(|| format!("s{}", row % 3)))
                        .collect();
                    let service_refs: Vec<Option<&str>> =
                        services.iter().map(Option::as_deref).collect();
                    for (row, tenant) in tenants.iter().enumerate() {
                        if let Some(tenant) = tenant {
                            truth
                                .entry((*tenant, services[row].clone()))
                                .or_default()
                                .push((file.clone(), first + row as u64));
                        }
                    }
                    batches.push((file.clone(), first, keyed_batch(&tenants, &service_refs)));
                    first += rows.cast_unsigned();
                }
            }
            let batches: Vec<(&str, u64, RecordBatch)> = batches
                .iter()
                .map(|(file, first, batch)| (file.as_str(), *first, batch.clone()))
                .collect();
            write(&state, &batches).await;
        }
        let view = state.published();
        let flatten = |per_file: &HashMap<String, Vec<u64>>| {
            let mut rows: Vec<(String, u64)> = per_file
                .iter()
                .flat_map(|(file, positions)| positions.iter().map(|p| (file.clone(), *p)))
                .collect();
            rows.sort();
            rows
        };
        // Single tenants, and IN lists of them.
        for tenants in [
            vec![0],
            vec![5],
            vec![12],
            vec![99],
            vec![1, 4, 9],
            vec![3, 3, 99],
        ] {
            let hit = selection(state.probe(&view, &tenant_values(&tenants)));
            let mut expected: Vec<(String, u64)> = truth
                .iter()
                .filter(|((tenant, _), _)| tenants.contains(tenant))
                .flat_map(|(_, rows)| rows.clone())
                .collect();
            expected.sort();
            expected.dedup();
            assert_eq!(flatten(&hit.per_file), expected, "tenants {tenants:?}");
        }
        // Composite keys: every (tenant, service) combination, and a NULL.
        let composite = |tenant: i64, services: &[Option<&str>]| {
            let services: Vec<ScalarValue> = services
                .iter()
                .map(|service| ScalarValue::Utf8(service.map(str::to_string)))
                .collect();
            move |column: &str| match column {
                "tenant" => Some(vec![ScalarValue::Int64(Some(tenant))]),
                "service" => Some(services.clone()),
                _ => None,
            }
        };
        for tenant in 0..13 {
            let values = composite(tenant, &[Some("s0"), Some("s2"), None]);
            // The single-column key is declared first, so pin it out.
            let shape = state.matched_shape(&values).expect("a key");
            assert_eq!(state.shape_label(shape), "tenant");
            let keys = key_tuples(&["tenant".to_string(), "service".to_string()], &values)
                .expect("bounded");
            let encoded = state.shapes.load()[1].encode_keys(&keys).expect("encodes");
            assert_eq!(encoded.iter().filter(|key| key.is_none()).count(), 1);
            let hit = view.probe_keys(1, &encoded, None, None).expect("unbounded");
            let mut expected: Vec<(String, u64)> = ["s0", "s2"]
                .iter()
                .filter_map(|service| truth.get(&(tenant, Some((*service).to_string()))))
                .flatten()
                .cloned()
                .collect();
            expected.sort();
            assert_eq!(flatten(&hit.per_file), expected, "tenant {tenant}");
        }
        // A NULL literal matches nothing.
        let null = |column: &str| (column == "tenant").then(|| vec![ScalarValue::Int64(None)]);
        assert!(selection(state.probe(&view, &null)).per_file.is_empty());
    }

    /// Every key persists its runs in a directory of its own, named by a
    /// 128-bit digest; keys that shared one would load each other's runs, so
    /// a set of directories with a repeat is refused.
    #[test]
    fn every_key_persists_in_a_directory_of_its_own() {
        let pool = unbounded_pool();
        let state = keyed_state(&pool);
        let dirs: Vec<String> = state
            .shapes
            .load()
            .iter()
            .map(|shape| shape.persisted_dir())
            .collect();
        assert_eq!(dirs.len(), 2);
        assert!(
            dirs.iter()
                .all(|dir| dir.len() == 32 && dir.chars().all(|c| c.is_ascii_hexdigit())),
            "{dirs:?}"
        );
        assert!(distinct_dirs(dirs.iter().map(String::as_str)), "{dirs:?}");
        assert!(!distinct_dirs(
            [dirs[0].as_str(), dirs[1].as_str(), dirs[0].as_str()].into_iter()
        ));
    }

    #[tokio::test]
    async fn a_scan_reads_uncovered_files_in_full_and_narrows_covered_ones() {
        let pool = unbounded_pool();
        let state = keyed_state(&pool);
        write(
            &state,
            &[(
                "a.vortex",
                0,
                keyed_batch(&[Some(1), Some(2), Some(1)], &[None, None, None]),
            )],
        )
        .await;
        let view = state.published();
        assert!(view.covers(path("a.vortex").as_ref()));
        assert!(
            view.covers("another/dir/a.vortex"),
            "coverage follows the file name, not the directory it moved to"
        );
        assert!(!view.covers(path("b.vortex").as_ref()));

        // Key 1 is at rows 0 and 2 of `a`; `b` is unindexed, so read whole.
        let (groups, provider, explain, uncovered) =
            selection(state.probe(&view, &tenant_values(&[1])))
                .restrict(group(&["a.vortex", "b.vortex"]), no_table_plans());
        assert!(uncovered);
        assert_eq!(
            explain.outcome,
            LookupIndexExplainOutcome::Probed(Coverage::Partial)
        );
        assert_eq!(
            (explain.candidate_files, explain.uncovered_files),
            (Some(2), Some(1)),
            "the uncovered file is a candidate, and reported as read in full"
        );
        let files: Vec<&PartitionedFile> = groups.iter().flat_map(FileGroup::iter).collect();
        assert_eq!(files.len(), 2);
        let provider = provider.expect("a selection");
        let plan_a = provider.access_plan_for_file(files[0]).expect("covered");
        assert_eq!(
            format!("{:?}", plan_a.selection()),
            format!(
                "{:?}",
                Some(&include_by_index(&Buffer::from(vec![0_u64, 2])))
            ),
        );
        assert!(
            provider.access_plan_for_file(files[1]).is_none(),
            "an uncovered file is read as the table reads it"
        );

        // A key in no covered file drops `a` but still reads `b`: the coverage
        // is partial.
        let (groups, _, explain, uncovered) = selection(state.probe(&view, &tenant_values(&[7])))
            .restrict(group(&["a.vortex", "b.vortex"]), no_table_plans());
        assert!(uncovered);
        assert_eq!(
            explain.outcome,
            LookupIndexExplainOutcome::Probed(Coverage::Partial)
        );
        assert_eq!(
            (explain.candidate_files, explain.uncovered_files),
            (Some(1), Some(1))
        );
        let names: Vec<String> = groups
            .iter()
            .flat_map(FileGroup::iter)
            .map(|file| file.object_meta.location.to_string())
            .collect();
        assert_eq!(names, vec![path("b.vortex").to_string()]);

        // Only covered files: full coverage, and an answer of no rows reads
        // nothing.
        let (groups, provider, explain, uncovered) =
            selection(state.probe(&view, &tenant_values(&[7])))
                .restrict(group(&["a.vortex"]), no_table_plans());
        assert!(!uncovered && groups.is_empty() && provider.is_none());
        assert_eq!(
            explain.outcome,
            LookupIndexExplainOutcome::Probed(Coverage::Full)
        );

        // Only uncovered files: nothing to narrow.
        let (_, provider, explain, uncovered) = selection(state.probe(&view, &tenant_values(&[1])))
            .restrict(group(&["b.vortex"]), no_table_plans());
        assert!(uncovered && provider.is_none());
        assert_eq!(
            explain.outcome,
            LookupIndexExplainOutcome::Probed(Coverage::Unindexed)
        );
        assert_eq!(
            (explain.candidate_files, explain.uncovered_files),
            (Some(1), Some(1))
        );
    }

    /// A lookup over several snapshots is counted once, with the coverage
    /// `EXPLAIN` reports for the whole scan: a current snapshot the index
    /// covers and a protected one it does not make a partly covered lookup,
    /// and a current snapshot with no files leaves the protected one's `none`.
    #[tokio::test]
    async fn a_lookup_over_several_snapshots_is_counted_with_its_merged_coverage() {
        let pool = unbounded_pool();
        let state = keyed_state(&pool);
        write(&state, &[("a.vortex", 0, keyed_batch(&[Some(1)], &[None]))]).await;
        let view = state.published();
        let counts = |state: &LookupIndexState| {
            let counters = state.counters.snapshot(0);
            (counters.none, counters.partial, counters.full)
        };
        for (current, protected, expected) in [
            (&["a.vortex"][..], &["b.vortex"][..], Coverage::Partial),
            (&[][..], &["b.vortex"][..], Coverage::Unindexed),
        ] {
            let before = counts(&state);
            let selection = selection(state.probe(&view, &tenant_values(&[1])));
            let (_, _, protected_explain, _) = selection
                .clone()
                .restrict(group(protected), no_table_plans());
            let (_, _, current_explain, _) = selection.restrict(group(current), no_table_plans());
            let explain = current_explain.merge(protected_explain);
            state.record_lookup(&explain);
            let after = counts(&state);
            let recorded = (after.0 - before.0, after.1 - before.1, after.2 - before.2);
            let reported = match explain.outcome {
                LookupIndexExplainOutcome::Probed(Coverage::Unindexed) => (1, 0, 0),
                LookupIndexExplainOutcome::Probed(Coverage::Partial) => (0, 1, 0),
                LookupIndexExplainOutcome::Probed(Coverage::Full) => (0, 0, 1),
                LookupIndexExplainOutcome::NotApplicable => (0, 0, 0),
            };
            assert_eq!(
                explain.outcome,
                LookupIndexExplainOutcome::Probed(expected),
                "{current:?} + {protected:?}"
            );
            assert_eq!(
                recorded, reported,
                "{current:?} + {protected:?}: the metric matches EXPLAIN"
            );
        }
    }

    /// A scan over several snapshots, one of them read in full, is one
    /// decision: partly covered, with the snapshot read in full counted among
    /// its candidate and uncovered files. Two fully covered snapshots stay
    /// full, and two unindexed ones stay none.
    #[test]
    fn a_selection_merged_with_an_unindexed_snapshot_counts_its_files() {
        let full = |files: usize| LookupIndexExplain {
            uncovered_files: Some(0),
            indexed_files: Some(files),
            ..LookupIndexExplain::selection(
                "TenantId".to_string(),
                LookupIndexExplainOutcome::Probed(Coverage::Full),
                Some(files),
                4,
            )
        };
        let unindexed = LookupIndexExplain {
            candidate_files: Some(3),
            uncovered_files: Some(3),
            indexed_files: Some(0),
            candidate_rows: None,
            ..LookupIndexExplain::selection(
                "TenantId".to_string(),
                LookupIndexExplainOutcome::Probed(Coverage::Unindexed),
                None,
                0,
            )
        };
        assert_eq!(
            full(2).merge(full(1)).outcome,
            LookupIndexExplainOutcome::Probed(Coverage::Full)
        );
        assert_eq!(
            unindexed.clone().merge(unindexed.clone()).outcome,
            LookupIndexExplainOutcome::Probed(Coverage::Unindexed)
        );
        for merged in [full(2).merge(unindexed.clone()), unindexed.merge(full(2))] {
            assert_eq!(
                merged.outcome,
                LookupIndexExplainOutcome::Probed(Coverage::Partial)
            );
            assert_eq!(
                (
                    merged.candidate_files,
                    merged.uncovered_files,
                    merged.candidate_rows
                ),
                (Some(5), Some(3), Some(4)),
                "{merged:?}"
            );
        }
    }

    #[tokio::test]
    async fn retired_files_release_their_memory() {
        let pool = unbounded_pool();
        let state = keyed_state(&pool);
        write(
            &state,
            &[("a.vortex", 0, keyed_batch(&[Some(1)], &[Some("x")]))],
        )
        .await;
        write(
            &state,
            &[("b.vortex", 0, keyed_batch(&[Some(2)], &[Some("y")]))],
        )
        .await;
        let charged = state.counters().index_bytes;
        assert!(charged > 0, "published runs are charged");
        assert_eq!(pool.reserved() as u64, charged);
        let reconcile = |generation: u64, live: &[&str]| {
            let live: Vec<String> = live.iter().map(|name| path(name).to_string()).collect();
            state.reconcile(
                "snapshot",
                FileSetVersion {
                    dir_generation: generation,
                    listing_epoch: 0,
                    protected: 0,
                },
                live.iter().map(String::as_str),
            );
        };
        reconcile(1, &["a.vortex", "b.vortex"]);
        // `a` was compacted away.
        reconcile(2, &["b.vortex"]);
        let view = state.published();
        assert!(!view.covers("a.vortex") && view.covers("b.vortex"));
        let after = state.counters().index_bytes;
        assert!(
            after < charged,
            "{after} bytes still charged after retiring a run of {charged}"
        );
        assert_eq!(pool.reserved() as u64, after);
        reconcile(3, &[]);
        assert_eq!(state.counters().index_bytes, 0);
        assert_eq!(pool.reserved(), 0);
    }

    #[tokio::test]
    async fn a_write_the_pool_cannot_fit_stays_uncovered() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64));
        let state = keyed_state(&pool);
        let tenants: Vec<Option<i64>> = (0..1_000).map(Some).collect();
        let services: Vec<Option<&str>> = vec![Some("s"); 1_000];
        write(&state, &[("a.vortex", 0, keyed_batch(&tenants, &services))]).await;
        assert!(!state.published().covers("a.vortex"));
        let counters = state.counters();
        assert_eq!(
            (counters.builds_published, counters.builds_unpublished),
            (0, 1)
        );
        assert_eq!(pool.reserved(), 0, "the refused write released its memory");
    }

    #[tokio::test]
    async fn a_failed_write_publishes_nothing() {
        use vortex_datafusion::VortexWriteObserver;
        let pool = unbounded_pool();
        let state = keyed_state(&pool);
        let observer = state.write_observer(false);
        observer.batch_written(&path("a.vortex"), 0, &keyed_batch(&[Some(1)], &[None]));
        // A batch without the key columns cannot be indexed.
        let other = RecordBatch::try_new(
            Arc::new(arrow_schema::Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .expect("batch");
        observer.batch_written(&path("a.vortex"), 1, &other);
        state.finish_write(&observer).await;
        assert!(
            !state.published().covers("a.vortex"),
            "a run missing rows is never published"
        );
    }

    #[tokio::test]
    async fn a_runtime_probe_selects_in_covered_files_and_requests_the_rest() {
        let pool = unbounded_pool();
        let state = keyed_state(&pool);
        write(
            &state,
            &[(
                "a.vortex",
                0,
                keyed_batch(&[Some(1), Some(2), Some(1)], &[None, None, None]),
            )],
        )
        .await;
        let builds = BuildRequests::default();
        let scan_files: Arc<[ObjectMeta]> = Arc::new([
            scan_file(path("a.vortex").as_ref()),
            scan_file(path("b.vortex").as_ref()),
        ]);
        let provider = DynamicLookupAccessPlanProvider::new(
            Arc::clone(&state),
            state.published(),
            scan_files,
            Some(builds.callback()),
        );
        let column = Arc::new(Column::new("tenant", 0)) as Arc<dyn PhysicalExpr>;
        let predicate = completed_dynamic_filter(&column, &[1]);
        let file = |name: &str| PartitionedFile::new(path(name).to_string(), 1);
        let covered = provider
            .runtime_access_plan_for_file(&file("a.vortex"), Some(&predicate))
            .await
            .expect("a covered file is selected");
        assert_eq!(
            format!("{:?}", covered.selection()),
            format!(
                "{:?}",
                Some(&include_by_index(&Buffer::from(vec![0_u64, 2])))
            ),
        );
        assert!(
            provider
                .runtime_access_plan_for_file(&file("b.vortex"), Some(&predicate))
                .await
                .is_none(),
            "an uncovered file is read as planned"
        );
        assert_eq!(
            builds.count(),
            1,
            "the uncovered file is sent to be indexed"
        );
        assert_eq!(
            state.counters().partial,
            1,
            "one probe per filter generation"
        );
    }

    #[tokio::test]
    async fn a_claim_dropped_before_its_build_runs_frees_the_slot() {
        let pool = unbounded_pool();
        let state = keyed_state(&pool);
        let claim = state.claim_build().expect("first claim");
        assert!(state.claim_build().is_none(), "one build at a time");
        drop(claim);
        let claim = state
            .claim_build()
            .expect("a dropped claim frees the slot without delaying the next");
        claim.unpublished();
        assert!(
            state.claim_build().is_none(),
            "a build that published nothing delays the next"
        );
        assert_eq!(state.counters().builds_started, 2);
        assert_eq!(state.counters().builds_unpublished, 1);
    }

    #[test]
    fn nested_key_columns_are_refused() {
        let nested = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        assert!(supported_key_type(&nested).is_err());
        for supported in [
            DataType::Int64,
            DataType::Utf8,
            DataType::Utf8View,
            DataType::Decimal128(20, 2),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        ] {
            supported_key_type(&supported).expect("indexable");
        }
    }
}
