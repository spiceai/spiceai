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
//! One immutable snapshot is indexed in memory; the index is bound to that
//! snapshot's exact file set, and any deviation falls back to the ordinary scan.
//! A full refresh builds the replacement during the write and publishes it in
//! the flip that makes the snapshot visible, so there is no window in which
//! lookups lose the index. Anything else that changes the files — an append, a
//! compaction, a restart — leaves the index stale; it is dropped, and a later
//! lookup rebuilds it in the background, paced so rebuilding takes a bounded
//! share of a core. Nothing is persisted.
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
//! With no entry the whole module is inert. Every build reserves its working
//! memory, and every published index its resident bytes, against the query
//! memory pool; when the pool cannot fit them the index is not built and the
//! table keeps scanning. This index is always safe to go without, which is what
//! lets it degrade rather than fail.
//!
//! Each key is held as sorted, compressed Vortex arrays: one per key column in
//! its stored type and one packed `(file, position)` column, ordered by key and
//! then by address. Resident size is therefore close to the compressed size of the key
//! columns. A lookup finds the block of [`BLOCK_ROWS`] entries its key can fall in
//! from the row-encoded key retained for the start of every block, decodes only
//! that block, and compares row-encoded keys inside it.
//!
//! Footguns this code depends on:
//!
//! * Row positions are file-local physical positions in unfiltered scan order.
//!   They are only valid for the exact file they were captured from, which is
//!   why [`LookupSelection::validate`] compares path, size and modification time
//!   before a selection is attached.
//! * The index answers `key -> candidate positions` only. Every original
//!   predicate still runs, so a candidate that fails `Active = 1` is discarded
//!   by the scan's own filter rather than by the index.
//! * A key is matched by its stored value. A predicate that casts the COLUMN can
//!   hold for stored values other than the literal (`CAST(score AS BIGINT) = 5`
//!   holds for 5.2), so such predicates must never reach [`LookupIndexState::probe`].
//! * The build sorts with Arrow's lexicographic sort and a lookup compares
//!   `RowConverter` bytes. Those orders agree for every type the converter
//!   supports, and the build re-checks them while it records block heads: a block
//!   searched in the wrong order would answer with a false empty.
//! * A selection of N row positions is not a promise of N decoded rows. Vortex
//!   reads whole encoded segments and dictionaries that cover those positions.

use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use super::memory_account::{CayenneMemoryAccount, LookupIndexReservation};
use crate::row_converter::{RowConverter, SortField};
use arc_swap::{ArcSwap, ArcSwapOption};
use arrow::array::{Array, ArrayRef, AsArray, UInt32Array, UInt64Array};
use arrow::compute::SortColumn;
use arrow::datatypes::UInt64Type;
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion_common::{ScalarValue, Statistics};
use datafusion_datasource::{PartitionedFile, file_groups::FileGroup};
use futures::StreamExt;
use object_store::{ObjectMeta, ObjectStore};
use parking_lot::Mutex;
use vortex::VortexSessionDefault;
use vortex::array::arrays::ChunkedArray;
use vortex::array::{ExecutionCtx, IntoArray, VortexSessionExecute};
use vortex::arrow::ArrowSessionExt;
use vortex::buffer::Buffer;
use vortex::compressor::{BtrBlocksCompressor, BtrBlocksCompressorBuilder};
use vortex::dtype::Nullability;
use vortex::file::OpenOptionsSessionExt;
use vortex::layout::layouts::row_idx::row_idx;
use vortex_datafusion::{VortexAccessPlan, VortexAccessPlanProvider};
use vortex_scan::selection::Selection;
use vortex_session::VortexSession;

/// Bits reserved for the file-local row position inside a packed posting.
const POSITION_BITS: u32 = 40;
const POSITION_MASK: u64 = (1u64 << POSITION_BITS) - 1;
/// File ids above this would not survive the shift into a packed posting.
const MAX_FILE_ID: u32 = (1u32 << (u64::BITS - POSITION_BITS)) - 1;

/// Sorted entries per block. A lookup decodes the block(s) its key can fall in,
/// and one row-encoded key is retained per block to find them, so this trades
/// resident head bytes against the work of every lookup.
const BLOCK_ROWS: usize = 256;

/// Sorted entries compressed together. A multiple of [`BLOCK_ROWS`], so blocks
/// never straddle chunks, and small enough that a build holds one sorted chunk of
/// the key columns at a time rather than a second sorted copy of all of them.
const COMPRESS_CHUNK_ROWS: usize = BLOCK_ROWS * 256;

/// Name of the file-local row position column the read-back build projects.
const READ_BACK_POSITION_COLUMN: &str = "__cayenne_lookup_row_idx";

/// How a probe ended. [`Self::as_str`] is the `outcome` dimension on
/// `cayenne_lookup_index_probe_total`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProbeOutcome {
    /// A row selection was attached to the scan.
    Selected,
    /// The key has no posting, so the scan reads no file.
    Empty,
    /// No index covers the rows the lookup reads, so it read them in full.
    Unbuilt,
    /// The scan's files are not the ones the index was built from.
    SnapshotMismatch,
}

/// The lookup-index decision shown on `CayenneAccelerationExec` in `EXPLAIN`.
///
/// This is deliberately separate from [`ProbeOutcome`]: `NotApplicable` is a
/// planning decision, not a probe, so it must not inflate the probe counters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LookupIndexExplainOutcome {
    NotApplicable,
    Selected,
    Empty,
    Unbuilt,
    SnapshotMismatch,
}

impl LookupIndexExplainOutcome {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::NotApplicable => "not_applicable",
            Self::Selected => "selected",
            Self::Empty => "empty",
            Self::Unbuilt => "unbuilt",
            Self::SnapshotMismatch => "snapshot_mismatch",
        }
    }
}

/// Stable, scan-local lookup-index evidence carried into `EXPLAIN`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LookupIndexExplain {
    pub(crate) shape: Option<String>,
    pub(crate) outcome: LookupIndexExplainOutcome,
    pub(crate) candidate_files: Option<usize>,
    pub(crate) candidate_rows: Option<u64>,
}

impl LookupIndexExplain {
    pub(crate) fn not_applicable(shape: Option<String>) -> Self {
        Self {
            shape,
            outcome: LookupIndexExplainOutcome::NotApplicable,
            candidate_files: None,
            candidate_rows: None,
        }
    }

    pub(crate) fn fallback(shape: String, outcome: LookupIndexExplainOutcome) -> Self {
        Self {
            shape: Some(shape),
            outcome,
            candidate_files: None,
            candidate_rows: None,
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
            candidate_rows: Some(candidate_rows),
        }
    }
}

impl ProbeOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Selected => "selected",
            Self::Empty => "empty",
            Self::Unbuilt => "unbuilt",
            Self::SnapshotMismatch => "snapshot_mismatch",
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

    pub(crate) fn label(&self) -> &str {
        &self.label
    }
}

/// Which file set of a snapshot a listing saw: the table's directory generation
/// and listing-cache epoch, sampled before listing. One of them moves whenever
/// files are added to, or rewritten under, the same snapshot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct FileSetVersion {
    pub(crate) dir_generation: u64,
    pub(crate) listing_epoch: u64,
}

/// A data file of the indexed snapshot, recorded exactly as the scan lists it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IndexedFile {
    pub(crate) path: String,
    pub(crate) size: u64,
    pub(crate) last_modified_ms: i64,
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
        if matches!(
            field.data_type(),
            DataType::Float16 | DataType::Float32 | DataType::Float64
        ) {
            return Err(format!(
                "lookup index column '{}' has unsupported floating-point type {}; use an integer, decimal, string, or other exact-equality type",
                field.name(),
                field.data_type()
            ));
        }
        Ok(Self {
            name: field.name().clone(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
        })
    }

    /// The field of this column inside the index, where nulls never appear.
    fn indexed_field(&self) -> Field {
        Field::new(&self.name, self.data_type.clone(), false)
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

fn postings_field() -> Field {
    Field::new("postings", DataType::UInt64, false)
}

/// The byte-comparable encoding of one key's columns.
pub(crate) fn key_converter(columns: &[KeyColumn]) -> Result<RowConverter, String> {
    RowConverter::new(
        columns
            .iter()
            .map(|column| SortField::new(column.data_type.clone()))
            .collect(),
    )
    .map_err(|e| {
        let types: Vec<String> = columns.iter().map(|c| c.data_type.to_string()).collect();
        format!("key ({}) cannot be row-encoded: {e}", types.join(", "))
    })
}

/// Casts `array` to `data_type`, or returns it unchanged when it already matches.
pub(crate) fn cast_to(array: &ArrayRef, data_type: &DataType) -> Result<ArrayRef, String> {
    if array.data_type() == data_type {
        return Ok(Arc::clone(array));
    }
    arrow::compute::cast(array, data_type)
        .map_err(|e| format!("cast {} -> {data_type}: {e}", array.data_type()))
}

/// The first index in `0..len` for which `pred` is false, for a `pred` that is
/// true on a prefix of the range.
fn partition_point(len: usize, pred: impl Fn(usize) -> bool) -> usize {
    let (mut lo, mut hi) = (0usize, len);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if pred(mid) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

fn usize_of(bytes: u64) -> usize {
    usize::try_from(bytes).unwrap_or(usize::MAX)
}

/// Splits a packed posting into its file id and file-local row position.
fn unpack(packed: u64) -> (usize, u64) {
    (usize_of(packed >> POSITION_BITS), packed & POSITION_MASK)
}

/// Decodes `range` of an index array into Arrow as `field`'s type.
fn decode(
    session: &VortexSession,
    array: &vortex::array::ArrayRef,
    range: Range<usize>,
    field: &Field,
    ctx: &mut ExecutionCtx,
) -> Result<ArrayRef, String> {
    let slice = array
        .slice(range)
        .map_err(|e| format!("slice {}: {e}", field.name()))?;
    session
        .arrow()
        .execute_arrow(slice, Some(field), ctx)
        .map_err(|e| format!("decode {}: {e}", field.name()))
}

/// Measured cost of one index build, reported separately from query execution.
#[derive(Clone, Debug)]
pub(crate) struct BuildStats {
    pub(crate) duration: Duration,
    pub(crate) files: usize,
    pub(crate) rows: u64,
    pub(crate) distinct_keys: usize,
    pub(crate) rss_before: Option<u64>,
    pub(crate) rss_after: Option<u64>,
    pub(crate) per_key_entries: Vec<(String, usize, usize)>,
}

/// What one key shape answers for a pinned key.
enum ShapeProbe {
    /// Every packed posting of the key, sorted; empty when no row holds it.
    Postings(Vec<u64>),
    /// The key could not be resolved against this index, so it proves nothing.
    Unanswerable,
}

/// One indexed key over the snapshot, as sorted compressed arrays.
struct ShapeIndex {
    label: String,
    columns: Vec<KeyColumn>,
    converter: RowConverter,
    /// One array per key column, in `columns` order, sorted by key and then by
    /// posting.
    keys: Vec<vortex::array::ArrayRef>,
    /// The packed postings, in the same order as `keys`.
    postings: vortex::array::ArrayRef,
    len: usize,
    /// The row-encoded key of the first entry of every block, concatenated.
    heads: Vec<u8>,
    /// Block `i`'s head is `heads[head_offsets[i]..head_offsets[i + 1]]`.
    head_offsets: Vec<usize>,
    distinct_keys: usize,
}

impl ShapeIndex {
    fn blocks(&self) -> usize {
        self.head_offsets.len().saturating_sub(1)
    }

    fn head(&self, block: usize) -> &[u8] {
        &self.heads[self.head_offsets[block]..self.head_offsets[block + 1]]
    }

    /// Resident bytes: the compressed arrays' buffers plus the retained heads.
    fn resident_bytes(&self) -> usize {
        self.keys
            .iter()
            .map(|keys| usize_of(keys.nbytes()))
            .fold(usize_of(self.postings.nbytes()), usize::saturating_add)
            .saturating_add(self.heads.capacity())
            .saturating_add(self.head_offsets.capacity() * std::mem::size_of::<usize>())
    }

    /// The entries of every block that can hold `key`.
    ///
    /// A block whose head is below `key` may hold it; so may a block whose head
    /// equals it, and the block before the first such block may end with it.
    fn candidate_range(&self, key: &[u8]) -> Range<usize> {
        let blocks = self.blocks();
        let below = partition_point(blocks, |block| self.head(block) < key);
        let through = partition_point(blocks, |block| self.head(block) <= key);
        if through == 0 {
            return 0..0;
        }
        let start = below.saturating_sub(1) * BLOCK_ROWS;
        let end = (through * BLOCK_ROWS).min(self.len);
        start..end
    }

    /// The packed postings of the key whose column values are `values`, in
    /// `columns` order.
    fn probe(&self, session: &VortexSession, values: &[ScalarValue]) -> ShapeProbe {
        if values.len() != self.columns.len() {
            return ShapeProbe::Unanswerable;
        }
        let mut literals = Vec::with_capacity(values.len());
        for (value, column) in values.iter().zip(&self.columns) {
            let Some(literal) = value
                .cast_to(&column.data_type)
                .ok()
                .and_then(|v| v.to_array_of_size(1).ok())
            else {
                return ShapeProbe::Unanswerable;
            };
            // A NULL literal never satisfies an equality predicate.
            if literal.is_null(0) {
                return ShapeProbe::Postings(Vec::new());
            }
            literals.push(literal);
        }
        let Ok(rows) = self.converter.convert_columns(&literals) else {
            return ShapeProbe::Unanswerable;
        };
        let key = rows.row(0);
        let range = self.candidate_range(key.as_ref());
        if range.is_empty() {
            return ShapeProbe::Postings(Vec::new());
        }
        match self.postings_in(session, range, key.as_ref()) {
            Ok(postings) => ShapeProbe::Postings(postings),
            Err(error) => {
                tracing::debug!(shape = %self.label, %error, "Point-lookup index block could not be read; scanning instead");
                ShapeProbe::Unanswerable
            }
        }
    }

    /// Decodes every key column of the entries `range`.
    fn decode_keys(
        &self,
        session: &VortexSession,
        range: Range<usize>,
        ctx: &mut ExecutionCtx,
    ) -> Result<Vec<ArrayRef>, String> {
        self.keys
            .iter()
            .zip(&self.columns)
            .map(|(keys, column)| {
                decode(session, keys, range.clone(), &column.indexed_field(), ctx)
            })
            .collect()
    }

    /// Decodes the packed postings of the entries `range`.
    fn decode_postings(
        &self,
        session: &VortexSession,
        range: Range<usize>,
        ctx: &mut ExecutionCtx,
    ) -> Result<UInt64Array, String> {
        decode(session, &self.postings, range, &postings_field(), ctx)?
            .as_primitive_opt::<UInt64Type>()
            .cloned()
            .ok_or_else(|| "postings did not decode as UInt64".to_string())
    }

    /// The postings of `key` within the entries `range`.
    fn postings_in(
        &self,
        session: &VortexSession,
        range: Range<usize>,
        key: &[u8],
    ) -> Result<Vec<u64>, String> {
        let mut ctx = session.create_execution_ctx();
        let rows = self
            .converter
            .convert_columns(&self.decode_keys(session, range.clone(), &mut ctx)?)
            .map_err(|e| format!("encode block: {e}"))?;
        let lo = partition_point(rows.num_rows(), |row| rows.row(row).as_ref() < key);
        let hi = partition_point(rows.num_rows(), |row| rows.row(row).as_ref() <= key);
        if lo == hi {
            return Ok(Vec::new());
        }
        let postings =
            self.decode_postings(session, range.start + lo..range.start + hi, &mut ctx)?;
        Ok(postings.values().to_vec())
    }

    /// Every entry as `(row-encoded key, file path, position)`, sorted.
    fn resolved_entries(
        &self,
        index: &SnapshotLookupIndex,
    ) -> Result<Vec<(Vec<u8>, String, u64)>, String> {
        if self.len == 0 {
            return Ok(Vec::new());
        }
        let session = &index.session;
        let mut ctx = session.create_execution_ctx();
        let keys = self.decode_keys(session, 0..self.len, &mut ctx)?;
        let postings = self.decode_postings(session, 0..self.len, &mut ctx)?;
        let rows = self
            .converter
            .convert_columns(&keys)
            .map_err(|e| format!("encode entries: {e}"))?;
        let mut entries = Vec::with_capacity(self.len);
        for (row, &packed) in postings.values().iter().enumerate() {
            let (file_id, position) = unpack(packed);
            let path = index
                .files
                .get(file_id)
                .map_or_else(|| format!("<unknown file {file_id}>"), |f| f.path.clone());
            entries.push((rows.row(row).as_ref().to_vec(), path, position));
        }
        entries.sort();
        Ok(entries)
    }
}

/// An index over exactly one immutable snapshot file set.
pub(crate) struct SnapshotLookupIndex {
    snapshot_id: String,
    files: Vec<IndexedFile>,
    file_ids: HashMap<String, u32>,
    shapes: Vec<ShapeIndex>,
    session: VortexSession,
    stats: BuildStats,
    /// The file set this index covers. A scan that finds a file the index lacks
    /// while the table's file set has moved on proves the index stale.
    file_set: FileSetVersion,
    /// The index's resident bytes in the table's memory account, released when
    /// the index is dropped.
    reservation: LookupIndexReservation,
}

impl SnapshotLookupIndex {
    pub(crate) fn snapshot_id(&self) -> &str {
        &self.snapshot_id
    }

    /// Resolves `filters` against one indexed key, returning the candidate row
    /// addresses grouped by file path. `None` means no indexed key is fully
    /// pinned to literals by these filters, or none of the pinned ones could be
    /// answered, so the ordinary scan must run.
    fn probe(&self, scalar_for: &dyn Fn(&str) -> Option<ScalarValue>) -> Option<ProbeHit> {
        for shape in &self.shapes {
            let Some(values) = shape
                .columns
                .iter()
                .map(|column| scalar_for(&column.name))
                .collect::<Option<Vec<_>>>()
            else {
                continue;
            };
            let postings = match shape.probe(&self.session, &values) {
                ShapeProbe::Postings(postings) => postings,
                ShapeProbe::Unanswerable => continue,
            };

            // Grouped under the index's own path strings, so each candidate
            // file's path is copied once rather than once per posting.
            let mut by_file: HashMap<&str, Vec<u64>> = HashMap::new();
            for &packed in &postings {
                let (file_id, position) = unpack(packed);
                // A posting that cannot be resolved to a file means the index is
                // not internally consistent; refuse the probe rather than
                // returning a partial selection.
                let file = self.files.get(file_id)?;
                by_file
                    .entry(file.path.as_str())
                    .or_default()
                    .push(position);
            }
            let per_file = by_file
                .into_iter()
                .map(|(path, mut positions)| {
                    positions.sort_unstable();
                    positions.dedup();
                    (path.to_string(), positions)
                })
                .collect();

            return Some(ProbeHit {
                shape: shape.label.clone(),
                per_file,
                rows: postings.len(),
            });
        }
        None
    }
}

struct ProbeHit {
    shape: String,
    per_file: HashMap<String, Vec<u64>>,
    rows: usize,
}

/// A resolved row selection awaiting validation against the scan's own file
/// list. Nothing is applied until [`Self::restrict`] accepts that file list.
pub(crate) struct LookupSelection {
    state: Arc<LookupIndexState>,
    index: Arc<SnapshotLookupIndex>,
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
    /// Narrows a scan's file groups to the files that hold a candidate row, and
    /// returns the access-plan provider that carries their positions into the
    /// Vortex scan. Either way the probe's outcome is recorded, so a run that
    /// silently fell back is visible in the metrics.
    ///
    /// The selection is honored ONLY for the exact snapshot and files it was
    /// captured from: for any other file set the groups come back untouched and
    /// no provider is returned, so a stale or incomplete index can never turn
    /// into a false empty result. `table_plans` is the provider the scan would
    /// otherwise attach, which carries the table's position-delete vectors.
    ///
    /// `current` is the table's file-set version now. A file the index lacks,
    /// under the index's own snapshot, while the file set has moved on since the
    /// index was listed, means files were added or rewritten under that snapshot
    /// — an append, say — so the index is dropped and a lookup can schedule its
    /// rebuild. When the file set has not moved, the scan's list is simply older
    /// than the index, and nothing is dropped.
    pub(crate) fn restrict(
        self,
        snapshot_id: &str,
        file_groups: Vec<FileGroup>,
        table_plans: Arc<dyn VortexAccessPlanProvider>,
        current: FileSetVersion,
    ) -> (
        Vec<FileGroup>,
        Option<Arc<dyn VortexAccessPlanProvider>>,
        LookupIndexExplain,
    ) {
        if !self.validate(snapshot_id, file_groups.iter().flat_map(FileGroup::iter)) {
            self.state
                .record_probe(&self.shape, ProbeOutcome::SnapshotMismatch);
            if self.index.snapshot_id == snapshot_id && self.index.file_set != current {
                self.state.discard_stale(&self.index);
            }
            return (
                file_groups,
                None,
                LookupIndexExplain::fallback(
                    self.shape,
                    LookupIndexExplainOutcome::SnapshotMismatch,
                ),
            );
        }
        let file_groups: Vec<FileGroup> = file_groups
            .into_iter()
            .filter_map(|group| {
                let files: Vec<PartitionedFile> = group
                    .into_inner()
                    .into_iter()
                    .filter(|file| {
                        let path: &str = file.object_meta.location.as_ref();
                        self.per_file.contains_key(path)
                    })
                    .collect();
                (!files.is_empty()).then(|| FileGroup::new(files))
            })
            .collect();
        let candidate_files: usize = file_groups.iter().map(FileGroup::len).sum();
        if candidate_files == 0 {
            self.state.record_probe(&self.shape, ProbeOutcome::Empty);
            return (
                file_groups,
                None,
                LookupIndexExplain::selection(
                    self.shape,
                    LookupIndexExplainOutcome::Empty,
                    Some(0),
                    0,
                ),
            );
        }
        self.state
            .record_selection(&self.shape, candidate_files as u64, self.rows as u64);
        let provider = LookupAccessPlanProvider {
            state: self.state,
            selections: self.per_file,
            table: table_plans,
        };
        (
            file_groups,
            Some(Arc::new(provider)),
            LookupIndexExplain::selection(
                self.shape,
                LookupIndexExplainOutcome::Selected,
                Some(candidate_files),
                u64::try_from(self.rows).unwrap_or(u64::MAX),
            ),
        )
    }

    /// Accepts this selection only for the exact snapshot and files it was built
    /// from. `files` is the scan's own (already pruned) list: a file pruned away
    /// by statistics provably holds no matching row, so its absence is fine,
    /// while a file the index has never seen means the snapshot moved under us.
    fn validate<'a>(
        &self,
        snapshot_id: &str,
        files: impl Iterator<Item = &'a PartitionedFile>,
    ) -> bool {
        if self.index.snapshot_id != snapshot_id {
            return false;
        }
        for file in files {
            let path: &str = file.object_meta.location.as_ref();
            let Some(&id) = self.index.file_ids.get(path) else {
                return false;
            };
            let Some(indexed) = self.index.files.get(id as usize) else {
                return false;
            };
            if indexed.size != file.object_meta.size
                || indexed.last_modified_ms != file.object_meta.last_modified.timestamp_millis()
            {
                return false;
            }
        }
        true
    }
}

/// Per-file row selections handed to the Vortex scan, composed with the table's
/// own per-file access plans.
///
/// A file carries exactly one access plan, and position-delete vectors travel
/// in it. So the lookup's candidate positions are intersected with whatever the
/// table's provider would have attached — a deleted candidate is never selected
/// — instead of replacing it.
struct LookupAccessPlanProvider {
    state: Arc<LookupIndexState>,
    selections: HashMap<String, Vec<u64>>,
    table: Arc<dyn VortexAccessPlanProvider>,
}

impl std::fmt::Debug for LookupAccessPlanProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LookupAccessPlanProvider")
            .field("files", &self.selections.len())
            .field("table", &self.table)
            .finish_non_exhaustive()
    }
}

/// Whether `selection` keeps the row at `position`.
fn selection_keeps(selection: &Selection, position: u64) -> bool {
    match selection {
        Selection::All => true,
        Selection::IncludeByIndex(rows) => rows.binary_search(&position).is_ok(),
        Selection::ExcludeByIndex(rows) => rows.binary_search(&position).is_err(),
        Selection::IncludeRoaring(rows) => rows.contains(position),
        Selection::ExcludeRoaring(rows) => !rows.contains(position),
    }
}

impl VortexAccessPlanProvider for LookupAccessPlanProvider {
    fn access_plan_for_file(&self, file: &PartitionedFile) -> Option<Arc<VortexAccessPlan>> {
        let path: &str = file.object_meta.location.as_ref();
        let table_plan = self.table.access_plan_for_file(file);
        let Some(candidates) = self.selections.get(path) else {
            // Not a file this lookup selected from: leave it exactly as the table
            // would read it.
            return table_plan;
        };
        let positions = match table_plan.as_deref().and_then(VortexAccessPlan::selection) {
            None | Some(Selection::All) => Buffer::copy_from(candidates.as_slice()),
            Some(table_selection) => candidates
                .iter()
                .copied()
                .filter(|&position| selection_keeps(table_selection, position))
                .collect::<Buffer<u64>>(),
        };
        self.state
            .counters
            .access_plans_attached
            .fetch_add(1, Ordering::Relaxed);
        Some(Arc::new(
            VortexAccessPlan::default().with_selection(Selection::IncludeByIndex(positions)),
        ))
    }

    fn adjust_statistics(&self, object: &ObjectMeta, statistics: Statistics) -> Statistics {
        self.table.adjust_statistics(object, statistics)
    }
}

/// Probe and build accounting for one table. The probe outcomes also go to
/// OpenTelemetry; these process-local counters are what a correctness check can
/// assert on to prove a query really used row selection instead of silently
/// scanning.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LookupIndexCounters {
    /// Probes that attached a row selection to the scan.
    pub selected: u64,
    /// Probes whose key has no posting at all, so the file branch reads nothing.
    pub empty: u64,
    /// Probes on an indexed key shape before any index was published.
    pub unbuilt: u64,
    /// Probes refused because the scan's files are not the indexed ones.
    pub snapshot_mismatch: u64,
    /// Candidate files summed over selected probes.
    pub candidate_files: u64,
    /// Candidate row positions summed over selected probes.
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

#[derive(Default)]
pub(crate) struct Counters {
    selected: AtomicU64,
    empty: AtomicU64,
    unbuilt: AtomicU64,
    snapshot_mismatch: AtomicU64,
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
            selected: self.selected.load(Ordering::Relaxed),
            empty: self.empty.load(Ordering::Relaxed),
            unbuilt: self.unbuilt.load(Ordering::Relaxed),
            snapshot_mismatch: self.snapshot_mismatch.load(Ordering::Relaxed),
            candidate_files: self.candidate_files.load(Ordering::Relaxed),
            candidate_rows: self.candidate_rows.load(Ordering::Relaxed),
            access_plans_attached: self.access_plans_attached.load(Ordering::Relaxed),
            index_bytes,
            builds_started: self.builds_started.load(Ordering::Relaxed),
            builds_published: self.builds_published.load(Ordering::Relaxed),
            builds_unpublished: self.builds_unpublished.load(Ordering::Relaxed),
        }
    }

    fn record(&self, outcome: ProbeOutcome) {
        let counter = match outcome {
            ProbeOutcome::Selected => &self.selected,
            ProbeOutcome::Empty => &self.empty,
            ProbeOutcome::Unbuilt => &self.unbuilt,
            ProbeOutcome::SnapshotMismatch => &self.snapshot_mismatch,
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

/// A table's secondary indexes over its Vortex files: the published index, a
/// write-time index waiting for its snapshot, and the schedule of the one
/// background build that replaces a missing or stale index.
///
/// The published index is never for a snapshot newer than the table's visible
/// one — a write-time index is promoted inside the visibility flip — so an index
/// whose snapshot is not the visible one is always stale.
pub(crate) struct LookupIndexState {
    table_name: String,
    specs: Vec<KeySpec>,
    /// The pool a build's working memory is reserved against.
    pool: Arc<dyn MemoryPool>,
    /// The table's account, which holds each live index's resident bytes.
    account: Arc<CayenneMemoryAccount>,
    index: ArcSwapOption<SnapshotLookupIndex>,
    /// Bumped every time `index` changes, so a build can tell whether anything
    /// was published or dropped while it ran without holding the index itself.
    generation: AtomicU64,
    /// Serializes changing `index` and `generation`, so an older build never
    /// overwrites a newer index. Probes never take it.
    publish_lock: Mutex<()>,
    /// A finished write-time index whose snapshot is not visible yet.
    staged: Mutex<Option<Arc<SnapshotLookupIndex>>>,
    /// A write-time build in progress for a snapshot that is not visible yet.
    pending: Mutex<Option<Arc<IncrementalIndexBuilder>>>,
    schedule: Mutex<BuildSchedule>,
    /// Whether an index has been published yet, so only the first is logged at
    /// `info`.
    published_once: AtomicBool,
    /// Write-time indexes over snapshots other than the visible current one,
    /// keyed by snapshot id: every protected snapshot a checkpoint, upsert or
    /// merge wrote, and a compaction rewrite's replacement snapshot until its
    /// flip promotes it. See [`Self::register_snapshot`].
    snapshots: ArcSwap<HashMap<String, Arc<SnapshotIndexEntry>>>,
    /// Serializes changes to `snapshots`. Probes never take it.
    snapshots_lock: Mutex<()>,
    /// Whether a failed per-snapshot build has been logged at `warn` yet. Such
    /// builds run on every checkpoint, so later failures are logged at `debug`.
    snapshot_build_warned: AtomicBool,
    counters: Counters,
}

/// A write-time index over one snapshot directory other than the current one.
struct SnapshotIndexEntry {
    index: Arc<SnapshotLookupIndex>,
    /// Set once the snapshot has been seen in the table's protected set. A live
    /// entry whose snapshot has left that set was folded away and is dropped; an
    /// entry that was never live belongs to a write still publishing.
    live: AtomicBool,
}

/// Registered indexes whose snapshot has not been published yet. Only a write
/// in flight holds one, so more than this means writes were abandoned without
/// discarding their index; the oldest are dropped.
const MAX_UNPUBLISHED_SNAPSHOT_INDEXES: usize = 32;

impl LookupIndexState {
    /// The index state for `specs`, or `None` when the table declares no index.
    pub(crate) fn new(
        table_name: &str,
        specs: Vec<KeySpec>,
        pool: Arc<dyn MemoryPool>,
        account: Arc<CayenneMemoryAccount>,
    ) -> Option<Arc<Self>> {
        if specs.is_empty() {
            return None;
        }
        let labels: Vec<&str> = specs.iter().map(KeySpec::label).collect();
        tracing::info!(
            table = %table_name,
            "Dataset '{table_name}' (cayenne): maintaining secondary indexes on {}",
            labels.join(", ")
        );
        Some(Arc::new(Self {
            table_name: table_name.to_string(),
            specs,
            pool,
            account,
            index: ArcSwapOption::empty(),
            generation: AtomicU64::new(0),
            publish_lock: Mutex::new(()),
            staged: Mutex::new(None),
            pending: Mutex::new(None),
            schedule: Mutex::new(BuildSchedule::default()),
            published_once: AtomicBool::new(false),
            snapshots: ArcSwap::from_pointee(HashMap::new()),
            snapshots_lock: Mutex::new(()),
            snapshot_build_warned: AtomicBool::new(false),
            counters: Counters::default(),
        }))
    }

    pub(crate) fn published(&self) -> Option<Arc<SnapshotLookupIndex>> {
        self.index.load_full()
    }

    pub(crate) fn counters(&self) -> LookupIndexCounters {
        let index_bytes = self
            .index
            .load()
            .as_ref()
            .map_or(0, |index| index.reservation.bytes());
        self.counters
            .snapshot(u64::try_from(index_bytes).unwrap_or(u64::MAX))
    }

    /// The right to build an index for the visible snapshot `snapshot_id` in the
    /// background, or `None` when one is published for it, a build is running,
    /// or the schedule says not yet.
    pub(crate) fn claim_build(self: &Arc<Self>, snapshot_id: &str) -> Option<BuildClaim> {
        let generation = self.generation.load(Ordering::Acquire);
        if self
            .index
            .load()
            .as_ref()
            .is_some_and(|index| index.snapshot_id == snapshot_id)
        {
            return None;
        }
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
            generation,
            started: now,
            settled: false,
        })
    }

    /// Changes the published index. The caller holds `publish_lock`.
    fn store_index(&self, index: Option<Arc<SnapshotLookupIndex>>) {
        self.index.store(index);
        self.generation.fetch_add(1, Ordering::Release);
    }

    /// Makes `index` the published index if `expected` still is. Returns whether
    /// it did.
    fn replace_if_current(
        &self,
        expected: Option<&Arc<SnapshotLookupIndex>>,
        index: Option<Arc<SnapshotLookupIndex>>,
    ) -> bool {
        let _publishing = self.publish_lock.lock();
        let current = self.index.load();
        let unchanged = match (current.as_ref(), expected) {
            (None, None) => true,
            (Some(current), Some(expected)) => Arc::ptr_eq(current, expected),
            _ => false,
        };
        if unchanged {
            self.store_index(index);
        }
        unchanged
    }

    /// Publishes a finished background build claimed at `generation`, unless a
    /// different index was published while it ran: that one came from a later
    /// refresh or build. An empty slot is always filled — whatever the build
    /// meant to replace was dropped as stale in the meantime.
    fn publish_unless_overtaken(&self, generation: u64, index: Arc<SnapshotLookupIndex>) -> bool {
        let _publishing = self.publish_lock.lock();
        let publish =
            self.index.load().is_none() || self.generation.load(Ordering::Acquire) == generation;
        if publish {
            self.store_index(Some(index));
        }
        publish
    }

    /// Drops `stale` if it is still the published index, returning its memory
    /// and letting a lookup schedule the rebuild.
    fn discard_stale(&self, stale: &Arc<SnapshotLookupIndex>) {
        if self.replace_if_current(Some(stale), None) {
            tracing::debug!(
                table = %self.table_name,
                snapshot_id = %stale.snapshot_id,
                "Dropped a stale secondary index"
            );
        }
    }

    /// Starts a write-time build for a snapshot that is not visible yet. The
    /// returned observer is handed to the write; [`Self::stage_pending`] finishes
    /// it once the snapshot's files are final.
    pub(crate) fn begin_incremental_build(
        self: &Arc<Self>,
        snapshot_id: &str,
        schema: &arrow_schema::Schema,
    ) -> Option<Arc<IncrementalIndexBuilder>> {
        let reservation = self.build_reservation();
        let builder = match IncrementalIndexBuilder::new(
            self.table_name.clone(),
            snapshot_id.to_string(),
            &self.specs,
            reservation,
            schema,
        ) {
            Ok(builder) => Arc::new(builder),
            Err(error) => {
                tracing::warn!(
                    table = %self.table_name,
                    "Dataset '{}' (cayenne): failed to start building its secondary index during the refresh, so lookups on it scan until an index is built from the refreshed files. Cause: {error}",
                    self.table_name
                );
                return None;
            }
        };
        *self.pending.lock() = Some(Arc::clone(&builder));
        Some(builder)
    }

    /// Working memory for one build, reserved against the query pool.
    fn build_reservation(&self) -> MemoryReservation {
        MemoryConsumer::new(format!("cayenne_index_build:{}", self.table_name)).register(&self.pool)
    }

    /// Finishes a write-time build and holds the index until its snapshot becomes
    /// visible, when [`Self::promote_staged`] publishes it inside the same flip.
    ///
    /// Nothing is staged for a refused or failed build, or for a file set that
    /// does not match the listing. The snapshot must still publish in that case:
    /// data availability never depends on this index.
    pub(crate) async fn stage_pending(
        &self,
        snapshot_id: &str,
        files: Vec<IndexedFile>,
        file_set: FileSetVersion,
    ) {
        let Some(builder) = self.pending.lock().take() else {
            return;
        };
        if builder.snapshot_id() != snapshot_id {
            return;
        }
        let account = Arc::clone(&self.account);
        let started = Instant::now();
        // Sorting and compressing the index is CPU work that runs for seconds on a
        // large table, so it goes to the blocking pool rather than the runtime.
        let finished =
            match tokio::task::spawn_blocking(move || builder.finish(&files, &account, file_set))
                .await
            {
                Ok(finished) => finished,
                Err(error) => Err(format!("index build task failed: {error}")),
            };
        super::table::record_cayenne_write_phase(&self.table_name, "lookup_index", started);
        match finished {
            Ok(Some(index)) => {
                *self.staged.lock() = Some(Arc::new(index));
            }
            Ok(None) => {
                self.counters
                    .builds_unpublished
                    .fetch_add(1, Ordering::Relaxed);
                self.schedule_after_unpublished(started.elapsed());
                tracing::warn!(
                    table = %self.table_name,
                    snapshot_id = %snapshot_id,
                    "{}",
                    refused_build_message(&self.table_name)
                );
            }
            Err(error) => {
                self.counters
                    .builds_unpublished
                    .fetch_add(1, Ordering::Relaxed);
                self.schedule_after_unpublished(started.elapsed());
                tracing::warn!(
                    table = %self.table_name,
                    snapshot_id = %snapshot_id,
                    "Dataset '{}' (cayenne): failed to build its secondary index during the refresh, so lookups on it scan until an index is built from the refreshed files. Cause: {error}",
                    self.table_name
                );
            }
        }
    }

    /// A refresh whose write-time index was not published leaves the refreshed
    /// files to a background build; this makes that build wait as if it had
    /// followed a failed one, rather than start on the next lookup and very
    /// likely fail the same way.
    fn schedule_after_unpublished(&self, took: Duration) {
        let mut schedule = self.schedule.lock();
        let in_flight = schedule.in_flight;
        schedule.finished(Instant::now(), took, false);
        schedule.in_flight = in_flight;
    }

    /// Publishes the staged write-time index for `snapshot_id`. Called inside the
    /// flip that makes `snapshot_id` visible, so probes never see an index for a
    /// snapshot newer than the one they can read. Synchronous and lock-light.
    pub(crate) fn promote_staged(&self, snapshot_id: &str) {
        let Some(index) = self.staged.lock().take() else {
            return;
        };
        if index.snapshot_id != snapshot_id {
            return;
        }
        {
            let _publishing = self.publish_lock.lock();
            self.store_index(Some(Arc::clone(&index)));
        }
        self.record_published(&index);
    }

    /// Drops a write-time build and any staged index whose write did not commit.
    pub(crate) fn discard_pending(&self) {
        *self.pending.lock() = None;
        *self.staged.lock() = None;
    }

    /// Starts a write-time build for a NEW snapshot directory that a checkpoint,
    /// upsert or compaction is about to write. Unlike
    /// [`Self::begin_incremental_build`] it claims no table-wide slot: several
    /// such writes can be in flight at once, and each finishes its own build
    /// with [`Self::finish_snapshot_build`].
    pub(crate) fn begin_snapshot_build(
        &self,
        snapshot_id: &str,
        schema: &arrow_schema::Schema,
    ) -> Option<Arc<IncrementalIndexBuilder>> {
        match IncrementalIndexBuilder::new(
            self.table_name.clone(),
            snapshot_id.to_string(),
            &self.specs,
            self.build_reservation(),
            schema,
        ) {
            Ok(builder) => Some(Arc::new(builder)),
            Err(error) => {
                self.snapshot_build_failed(snapshot_id, &error);
                None
            }
        }
    }

    /// Finishes a build begun by [`Self::begin_snapshot_build`] once its
    /// snapshot's files are final, and registers the index under the snapshot
    /// id. `files` is that snapshot's file set as the scan lists it.
    ///
    /// Registration happens BEFORE the write's visibility flip, which is what
    /// lets a scan find the index for every snapshot it can see. A registered
    /// index is only consulted for a snapshot a scan is actually reading, and a
    /// snapshot directory is never written again once published, so an index
    /// registered early can never be applied to files it does not describe.
    ///
    /// Best-effort by construction: a refused or failed build leaves that
    /// snapshot to the ordinary scan, and the write publishes regardless.
    pub(crate) async fn finish_snapshot_build(
        &self,
        builder: Arc<IncrementalIndexBuilder>,
        files: Vec<IndexedFile>,
        file_set: FileSetVersion,
    ) {
        let snapshot_id = builder.snapshot_id().to_string();
        let account = Arc::clone(&self.account);
        let started = Instant::now();
        // A compaction rewrite indexes the whole table, which is seconds of sort
        // and compression, so it runs on the blocking pool like the refresh build.
        let finished =
            match tokio::task::spawn_blocking(move || builder.finish(&files, &account, file_set))
                .await
            {
                Ok(finished) => finished,
                Err(error) => Err(format!("index build task failed: {error}")),
            };
        super::table::record_cayenne_write_phase(&self.table_name, "lookup_index", started);
        match finished {
            Ok(Some(index)) => self.register_snapshot(Arc::new(index)),
            Ok(None) => {
                self.counters
                    .builds_unpublished
                    .fetch_add(1, Ordering::Relaxed);
                if self.snapshot_build_warned.swap(true, Ordering::Relaxed) {
                    tracing::debug!(table = %self.table_name, snapshot_id = %snapshot_id, "{}", refused_build_message(&self.table_name));
                } else {
                    tracing::warn!(table = %self.table_name, snapshot_id = %snapshot_id, "{}", refused_build_message(&self.table_name));
                }
            }
            Err(error) => self.snapshot_build_failed(&snapshot_id, &error),
        }
    }

    fn snapshot_build_failed(&self, snapshot_id: &str, error: &str) {
        self.counters
            .builds_unpublished
            .fetch_add(1, Ordering::Relaxed);
        if self.snapshot_build_warned.swap(true, Ordering::Relaxed) {
            tracing::debug!(
                table = %self.table_name,
                snapshot_id = %snapshot_id,
                %error,
                "Secondary index build for a written snapshot failed again"
            );
        } else {
            tracing::warn!(
                table = %self.table_name,
                snapshot_id = %snapshot_id,
                "Dataset '{}' (cayenne): failed to build its secondary index for newly written rows, so lookups read those rows without the index until compaction rewrites them. Cause: {error}",
                self.table_name
            );
        }
    }

    /// Adds `index` to the per-snapshot indexes, replacing any for the same
    /// snapshot. Unpublished entries beyond [`MAX_UNPUBLISHED_SNAPSHOT_INDEXES`]
    /// can only come from writes abandoned without discarding their index, so
    /// the oldest of those are dropped.
    fn register_snapshot(&self, index: Arc<SnapshotLookupIndex>) {
        {
            let _changing = self.snapshots_lock.lock();
            let mut next = HashMap::clone(&self.snapshots.load());
            next.insert(
                index.snapshot_id.clone(),
                Arc::new(SnapshotIndexEntry {
                    index: Arc::clone(&index),
                    live: AtomicBool::new(false),
                }),
            );
            let mut unpublished: Vec<String> = next
                .iter()
                .filter(|(_, entry)| !entry.live.load(Ordering::Acquire))
                .map(|(id, _)| id.clone())
                .collect();
            if unpublished.len() > MAX_UNPUBLISHED_SNAPSHOT_INDEXES {
                // Snapshot ids are UUIDv7, so lexicographic order is creation order.
                unpublished.sort_unstable();
                let excess = unpublished.len() - MAX_UNPUBLISHED_SNAPSHOT_INDEXES;
                for id in unpublished.into_iter().take(excess) {
                    next.remove(&id);
                }
            }
            self.snapshots.store(Arc::new(next));
        }
        self.record_published(&index);
    }

    /// Records that `snapshot_id` joined the table's protected set, so its index
    /// is dropped once the snapshot later leaves it. Called where the snapshot
    /// is published.
    pub(crate) fn mark_snapshot_live(&self, snapshot_id: &str) {
        if let Some(entry) = self.snapshots.load().get(snapshot_id) {
            entry.live.store(true, Ordering::Release);
        }
    }

    /// Drops the index of a snapshot whose write was abandoned.
    pub(crate) fn discard_snapshot(&self, snapshot_id: &str) {
        if !self.snapshots.load().contains_key(snapshot_id) {
            return;
        }
        let _changing = self.snapshots_lock.lock();
        let mut next = HashMap::clone(&self.snapshots.load());
        if next.remove(snapshot_id).is_some() {
            self.snapshots.store(Arc::new(next));
        }
    }

    /// Drops every published per-snapshot index whose snapshot `is_live` no
    /// longer reports: a merge or rewrite folded it away. Entries not yet
    /// published are kept, because their write is still in flight.
    pub(crate) fn retain_live_snapshots(&self, is_live: impl Fn(&str) -> bool) {
        let folded =
            |id: &str, entry: &SnapshotIndexEntry| entry.live.load(Ordering::Acquire) && !is_live(id);
        if !self
            .snapshots
            .load()
            .iter()
            .any(|(id, entry)| folded(id, entry))
        {
            return;
        }
        let _changing = self.snapshots_lock.lock();
        let mut next = HashMap::clone(&self.snapshots.load());
        next.retain(|id, entry| !folded(id, entry));
        self.snapshots.store(Arc::new(next));
    }

    /// Publishes the registered index of `snapshot_id` as the index of the
    /// table's current snapshot. Called inside the flip that makes a compaction
    /// rewrite's snapshot current, exactly as a refresh publishes its staged
    /// index. Returns whether there was one to publish.
    pub(crate) fn promote_snapshot(&self, snapshot_id: &str) -> bool {
        let entry = {
            let _changing = self.snapshots_lock.lock();
            let mut next = HashMap::clone(&self.snapshots.load());
            let entry = next.remove(snapshot_id);
            if entry.is_some() {
                self.snapshots.store(Arc::new(next));
            }
            entry
        };
        let Some(entry) = entry else {
            return false;
        };
        let _publishing = self.publish_lock.lock();
        self.store_index(Some(Arc::clone(&entry.index)));
        true
    }

    /// Resolves a candidate row selection for a lookup reading the snapshot
    /// `snapshot_id` that is not the table's current one — a protected snapshot
    /// written by a checkpoint, upsert or merge. `None` means the scan of that
    /// snapshot reads it in full: no key is pinned, the snapshot has no index,
    /// or the key could not be answered.
    ///
    /// `scalar_for` must only answer for predicates that compare the bare column
    /// with a value: see the module's note on column-side casts.
    pub(crate) fn probe_snapshot(
        self: &Arc<Self>,
        snapshot_id: &str,
        scalar_for: &dyn Fn(&str) -> Option<ScalarValue>,
    ) -> Option<LookupSelection> {
        let shape = self.matched_shape(scalar_for)?;
        let Some(entry) = self.snapshots.load().get(snapshot_id).map(Arc::clone) else {
            // A snapshot written before this process started, or whose build was
            // refused.
            self.record_probe(shape, ProbeOutcome::Unbuilt);
            return None;
        };
        // The scan found this snapshot in the protected set it captured.
        entry.live.store(true, Ordering::Release);
        let hit = entry.index.probe(scalar_for)?;
        Some(LookupSelection {
            state: Arc::clone(self),
            index: Arc::clone(&entry.index),
            shape: hit.shape,
            per_file: hit.per_file,
            rows: hit.rows,
        })
    }

    fn record_published(&self, index: &SnapshotLookupIndex) {
        self.counters
            .builds_published
            .fetch_add(1, Ordering::Relaxed);
        let stats = &index.stats;
        let first = !self.published_once.swap(true, Ordering::Relaxed);
        let message = format!(
            "Dataset '{}' (cayenne): built its secondary index over {} rows in {} files, holding {} bytes",
            self.table_name,
            stats.rows,
            stats.files,
            index.reservation.bytes()
        );
        if first {
            tracing::info!(
                table = %self.table_name,
                snapshot_id = %index.snapshot_id,
                build_ms = stats.duration.as_millis(),
                distinct_keys = stats.distinct_keys,
                "{message}"
            );
        } else {
            tracing::debug!(
                table = %self.table_name,
                snapshot_id = %index.snapshot_id,
                build_ms = stats.duration.as_millis(),
                distinct_keys = stats.distinct_keys,
                rss_before = ?stats.rss_before,
                rss_after = ?stats.rss_after,
                per_key = ?stats.per_key_entries,
                "{message}"
            );
        }
    }

    /// The indexed key these filters fully pin to literals, if any.
    pub(crate) fn matched_shape(
        &self,
        scalar_for: &dyn Fn(&str) -> Option<ScalarValue>,
    ) -> Option<&str> {
        self.specs
            .iter()
            .find(|spec| {
                spec.columns
                    .iter()
                    .all(|column| scalar_for(column).is_some())
            })
            .map(KeySpec::label)
    }

    /// Resolves a candidate row selection for `scalar_for` on a table whose
    /// visible snapshot is `visible_snapshot`. A fallback carries the reason for
    /// `EXPLAIN`; the caller records the final outcome once it has validated a
    /// selection against its own file list.
    ///
    /// `scalar_for` must only answer for predicates that compare the bare column
    /// with a value: see the module's note on column-side casts.
    pub(crate) fn probe(
        self: &Arc<Self>,
        visible_snapshot: &str,
        scalar_for: &dyn Fn(&str) -> Option<ScalarValue>,
    ) -> LookupProbe {
        let Some(shape) = self.matched_shape(scalar_for) else {
            return LookupProbe::Fallback(LookupIndexExplain::not_applicable(None));
        };
        let Some(index) = self.published() else {
            self.record_probe(shape, ProbeOutcome::Unbuilt);
            return LookupProbe::Fallback(LookupIndexExplain::fallback(
                shape.to_string(),
                LookupIndexExplainOutcome::Unbuilt,
            ));
        };
        if index.snapshot_id != visible_snapshot {
            self.record_probe(shape, ProbeOutcome::SnapshotMismatch);
            self.discard_stale(&index);
            return LookupProbe::Fallback(LookupIndexExplain::fallback(
                shape.to_string(),
                LookupIndexExplainOutcome::SnapshotMismatch,
            ));
        }

        let Some(hit) = index.probe(scalar_for) else {
            return LookupProbe::Fallback(LookupIndexExplain::not_applicable(Some(
                shape.to_string(),
            )));
        };
        LookupProbe::Selection(LookupSelection {
            state: Arc::clone(self),
            index,
            shape: hit.shape,
            per_file: hit.per_file,
            rows: hit.rows,
        })
    }

    fn record_probe(&self, shape: &str, outcome: ProbeOutcome) {
        record_probe_outcome(&self.table_name, &self.counters, shape, outcome);
    }

    fn record_selection(&self, shape: &str, files: u64, rows: u64) {
        self.counters
            .candidate_files
            .fetch_add(files, Ordering::Relaxed);
        self.counters
            .candidate_rows
            .fetch_add(rows, Ordering::Relaxed);
        self.record_probe(shape, ProbeOutcome::Selected);
    }
}

/// Counts one probe's outcome and reports it on
/// `cayenne_lookup_index_probe_total`.
pub(crate) fn record_probe_outcome(
    table_name: &str,
    counters: &Counters,
    shape: &str,
    outcome: ProbeOutcome,
) {
    counters.record(outcome);
    telemetry::cayenne::track_lookup_index_probe(&[
        telemetry::KeyValue::new("table", table_name.to_string()),
        telemetry::KeyValue::new("shape", shape.to_string()),
        telemetry::KeyValue::new("outcome", outcome.as_str()),
    ]);
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
    /// The table's index generation when the build was claimed. The build's
    /// result is published only if nothing newer has been published since.
    generation: u64,
    started: Instant,
    settled: bool,
}

impl BuildClaim {
    /// Ends a build that published its index.
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

    /// Ends a build whose index a newer one replaced while it ran. The table has
    /// an index, so the next build is paced but not backed off.
    fn overtaken(mut self) {
        self.state
            .counters
            .builds_unpublished
            .fetch_add(1, Ordering::Relaxed);
        self.finish(true);
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

/// Builds the index for `snapshot_id` in the background and publishes it only
/// once complete, unless a newer index was published meanwhile. Failures leave
/// the table on the ordinary scan until the schedule allows another attempt.
pub(crate) fn spawn_build(
    claim: BuildClaim,
    snapshot_id: String,
    store: Arc<dyn ObjectStore>,
    files: Vec<IndexedFile>,
    schema: Arc<arrow_schema::Schema>,
    file_set: FileSetVersion,
) {
    tokio::spawn(async move {
        let state = Arc::clone(&claim.state);
        let table = state.table_name.clone();
        tracing::debug!(
            table = %table,
            snapshot_id = %snapshot_id,
            files = files.len(),
            "Building secondary index"
        );
        let first_failure = state.schedule.lock().unpublished == 0;
        match build(
            &state,
            snapshot_id.clone(),
            &store,
            files,
            &schema,
            file_set,
        )
        .await
        {
            Ok(Some(index)) => {
                let index = Arc::new(index);
                if state.publish_unless_overtaken(claim.generation, Arc::clone(&index)) {
                    state.record_published(&index);
                    claim.published();
                } else {
                    claim.overtaken();
                }
            }
            Ok(None) => {
                if first_failure {
                    tracing::warn!(table = %table, snapshot_id = %snapshot_id, "{}", refused_build_message(&table));
                } else {
                    tracing::debug!(table = %table, snapshot_id = %snapshot_id, "{}", refused_build_message(&table));
                }
                claim.unpublished();
            }
            Err(error) => {
                if first_failure {
                    tracing::warn!(
                        table = %table,
                        snapshot_id = %snapshot_id,
                        "Dataset '{table}' (cayenne): failed to build its secondary index, so lookups on it scan until a later attempt succeeds. Cause: {error}"
                    );
                } else {
                    tracing::debug!(table = %table, snapshot_id = %snapshot_id, %error, "Secondary index build failed again");
                }
                claim.unpublished();
            }
        }
    });
}

/// Where a batch's rows sit in their file.
#[derive(Clone, Copy)]
enum RowPositions<'a> {
    /// Rows occupy consecutive positions from this one — what a writer reports.
    Contiguous(u64),
    /// One position per row — what a read-back scan's `row_idx()` reports.
    Explicit(&'a UInt64Array),
}

impl RowPositions<'_> {
    fn position(self, row: u32) -> u64 {
        match self {
            Self::Contiguous(start) => start + u64::from(row),
            Self::Explicit(positions) => positions.value(row as usize),
        }
    }
}

/// Entries for one key shape, accumulated until the snapshot is complete.
struct ShapeBuild {
    label: String,
    columns: Vec<KeyColumn>,
    /// Per key column, the compacted, null-free chunks in arrival order.
    keys: Vec<Vec<ArrayRef>>,
    /// The packed posting of every retained row, in arrival order.
    postings: Vec<u64>,
    /// Bytes held by `keys` and `postings`.
    retained: usize,
}

impl ShapeBuild {
    fn new(label: String, columns: Vec<KeyColumn>) -> Self {
        let keys = columns.iter().map(|_| Vec::new()).collect();
        Self {
            label,
            columns,
            keys,
            postings: Vec::new(),
            retained: 0,
        }
    }

    /// Drops every accumulated entry.
    fn release(&mut self) {
        for chunks in &mut self.keys {
            *chunks = Vec::new();
        }
        self.postings = Vec::new();
        self.retained = 0;
    }

    /// What [`Self::finish`] holds beyond the accumulated entries while it
    /// sorts: one concatenated copy of the key columns and the sort order.
    fn sort_working_bytes(&self) -> usize {
        let keys = self
            .retained
            .saturating_sub(self.postings.len() * std::mem::size_of::<u64>());
        keys.saturating_add(self.postings.len() * std::mem::size_of::<u32>())
    }

    /// Appends the rows of `batch` whose key columns are all non-null.
    ///
    /// Returns the bytes this retained. The key columns are copied, so a batch
    /// that is a slice of a larger buffer does not keep that buffer alive.
    fn ingest(
        &mut self,
        file_id: u32,
        positions: RowPositions<'_>,
        batch: &RecordBatch,
        file_path: &str,
    ) -> Result<usize, String> {
        let columns = self
            .columns
            .iter()
            .map(|key| {
                let array = batch.column_by_name(&key.name).ok_or_else(|| {
                    format!("{file_path}: column '{}' not in the batch", key.name)
                })?;
                cast_to(array, &key.data_type)
                    .map_err(|e| format!("{file_path}: {}: {e}", key.name))
            })
            .collect::<Result<Vec<ArrayRef>, String>>()?;
        let num_rows = u32::try_from(batch.num_rows())
            .map_err(|_| format!("{file_path}: batch has more than u32::MAX rows"))?;

        // A NULL can never satisfy an equality predicate, so an incomplete key
        // is simply not indexed.
        let mut keep: Option<arrow::array::BooleanArray> = None;
        for column in columns.iter().filter(|column| column.null_count() > 0) {
            let valid = arrow::compute::is_not_null(column.as_ref()).map_err(|e| e.to_string())?;
            keep = Some(match keep {
                None => valid,
                Some(keep) => arrow::compute::and(&keep, &valid).map_err(|e| e.to_string())?,
            });
        }
        let indices = match keep {
            None => UInt32Array::from_iter_values(0..num_rows),
            Some(keep) => UInt32Array::from_iter_values(
                keep.values()
                    .set_indices()
                    .filter_map(|row| u32::try_from(row).ok()),
            ),
        };

        self.postings.reserve(indices.len());
        for &row in indices.values() {
            let position = positions.position(row);
            if position > POSITION_MASK {
                return Err(format!(
                    "{file_path}: row position {position} does not fit the index's address"
                ));
            }
            self.postings
                .push((u64::from(file_id) << POSITION_BITS) | position);
        }
        let mut retained = indices.len() * std::mem::size_of::<u64>();
        for (column, chunks) in columns.iter().zip(&mut self.keys) {
            let kept = arrow::compute::take(column.as_ref(), &indices, None)
                .map_err(|e| format!("{file_path}: {e}"))?;
            retained += kept.get_array_memory_size();
            chunks.push(kept);
        }
        self.retained = self.retained.saturating_add(retained);
        Ok(retained)
    }

    /// Sorts the entries and compresses them into a [`ShapeIndex`], one chunk of
    /// `chunk_rows` sorted entries at a time.
    fn finish(
        self,
        session: &VortexSession,
        compressor: &BtrBlocksCompressor,
        chunk_rows: usize,
    ) -> Result<ShapeIndex, String> {
        let Self {
            label,
            columns,
            keys,
            postings,
            ..
        } = self;
        let converter = key_converter(&columns)?;
        let keys = keys
            .into_iter()
            .zip(&columns)
            .map(|(chunks, column)| {
                if chunks.is_empty() {
                    return Ok(arrow::array::new_empty_array(&column.data_type));
                }
                let parts: Vec<&dyn Array> = chunks.iter().map(AsRef::as_ref).collect();
                arrow::compute::concat(&parts).map_err(|e| format!("{}: {e}", column.name))
            })
            .collect::<Result<Vec<ArrayRef>, String>>()?;
        let postings: ArrayRef = Arc::new(UInt64Array::from(postings));

        let sort_columns: Vec<SortColumn> = keys
            .iter()
            .chain(std::iter::once(&postings))
            .map(|values| SortColumn {
                values: Arc::clone(values),
                options: None,
            })
            .collect();
        let order = arrow::compute::lexsort_to_indices(&sort_columns, None)
            .map_err(|e| format!("sort {label}: {e}"))?;
        drop(sort_columns);

        let mut fields: Vec<Field> = columns.iter().map(KeyColumn::indexed_field).collect();
        fields.push(postings_field());
        let mut ctx = session.create_execution_ctx();
        let mut heads = BlockHeads::new();
        let mut compressed: Vec<Vec<vortex::array::ArrayRef>> =
            fields.iter().map(|_| Vec::new()).collect();
        let len = postings.len();
        let mut start = 0usize;
        while start < len {
            let rows = chunk_rows.min(len - start);
            let indices = order.slice(start, rows);
            let chunk = keys
                .iter()
                .chain(std::iter::once(&postings))
                .map(|array| {
                    arrow::compute::take(array.as_ref(), &indices, None)
                        .map_err(|e| format!("sort {label}: {e}"))
                })
                .collect::<Result<Vec<ArrayRef>, String>>()?;
            heads
                .extend(&converter, &chunk[..columns.len()])
                .map_err(|e| format!("{label}: {e}"))?;
            for ((array, field), out) in chunk.into_iter().zip(&fields).zip(&mut compressed) {
                let imported = session
                    .arrow()
                    .from_arrow_array(array, field)
                    .map_err(|e| format!("import {}: {e}", field.name()))?;
                out.push(
                    compressor
                        .compress(&imported, &mut ctx)
                        .map_err(|e| format!("compress {}: {e}", field.name()))?,
                );
            }
            start += rows;
        }
        drop((order, keys, postings));

        let assemble = |chunks: Vec<vortex::array::ArrayRef>, field: &Field| {
            if chunks.len() == 1 {
                return chunks
                    .into_iter()
                    .next()
                    .ok_or_else(|| format!("{}: no chunk", field.name()));
            }
            let Some(dtype) = chunks.first().map(|chunk| chunk.dtype().clone()) else {
                return session
                    .arrow()
                    .from_arrow_array(arrow::array::new_empty_array(field.data_type()), field)
                    .map_err(|e| format!("import {}: {e}", field.name()));
            };
            ChunkedArray::try_new(chunks, dtype)
                .map(IntoArray::into_array)
                .map_err(|e| format!("chunk {}: {e}", field.name()))
        };
        let mut arrays = compressed
            .into_iter()
            .zip(&fields)
            .map(|(chunks, field)| assemble(chunks, field))
            .collect::<Result<Vec<_>, String>>()?;
        let postings = arrays
            .pop()
            .ok_or_else(|| format!("{label}: no postings array"))?;
        let (heads, head_offsets, distinct_keys) = heads.finish();
        Ok(ShapeIndex {
            label,
            converter,
            keys: arrays,
            postings,
            columns,
            len,
            heads,
            head_offsets,
            distinct_keys,
        })
    }
}

/// The row-encoded key at the start of every block of sorted entries, and the
/// number of distinct keys, fed one sorted chunk at a time.
///
/// Encodes one block at a time, so the whole index is never row-encoded at
/// once. Fails if any entry encodes below its predecessor: the sort and the
/// encoding disagree for this key's types, and a lookup would miss rows.
struct BlockHeads {
    heads: Vec<u8>,
    /// Block `i`'s head is `heads[offsets[i]..offsets[i + 1]]`.
    offsets: Vec<usize>,
    distinct_keys: usize,
    /// The last key seen, which continues the order check and the distinct count
    /// from one chunk into the next.
    previous: Option<Vec<u8>>,
}

impl BlockHeads {
    fn new() -> Self {
        Self {
            heads: Vec::new(),
            offsets: vec![0],
            distinct_keys: 0,
            previous: None,
        }
    }

    /// Records the next sorted entries, given as one array per key column, which
    /// start on a block boundary.
    fn extend(&mut self, converter: &RowConverter, keys: &[ArrayRef]) -> Result<(), String> {
        let len = keys.first().map_or(0, Array::len);
        let mut start = 0usize;
        while start < len {
            let block = BLOCK_ROWS.min(len - start);
            let slices: Vec<ArrayRef> = keys
                .iter()
                .map(|column| column.slice(start, block))
                .collect();
            let rows = converter
                .convert_columns(&slices)
                .map_err(|e| format!("encode block: {e}"))?;
            self.heads.extend_from_slice(rows.row(0).as_ref());
            self.offsets.push(self.heads.len());
            for row in 0..rows.num_rows() {
                let current = rows.row(row);
                let prior_row = row.checked_sub(1).map(|prior| rows.row(prior));
                let prior = match &prior_row {
                    Some(prior_row) => Some(prior_row.as_ref()),
                    None => self.previous.as_deref(),
                };
                match prior.map(|prior| prior.cmp(current.as_ref())) {
                    None | Some(std::cmp::Ordering::Less) => self.distinct_keys += 1,
                    Some(std::cmp::Ordering::Equal) => {}
                    Some(std::cmp::Ordering::Greater) => {
                        return Err(
                            "sorted keys are out of order in their row encoding".to_string()
                        );
                    }
                }
            }
            self.previous = Some(rows.row(rows.num_rows() - 1).as_ref().to_vec());
            start += block;
        }
        Ok(())
    }

    fn finish(mut self) -> (Vec<u8>, Vec<usize>, usize) {
        self.heads.shrink_to_fit();
        (self.heads, self.offsets, self.distinct_keys)
    }
}

/// Accumulates postings for one snapshot.
///
/// Shared by both builders — the read-back build that scans finished files and
/// the write-time build fed by the Vortex sink — so the two cannot drift apart
/// in how they select, pack or sort entries.
struct BuildState {
    shapes: Vec<ShapeBuild>,
    file_ids: HashMap<String, u32>,
    /// `file_id -> path`, in assignment order.
    file_order: Vec<String>,
    rows: u64,
    /// The accumulated entries' bytes, reserved against the query memory pool
    /// while the build holds them.
    reservation: MemoryReservation,
    /// Set once the pool refuses to fit the accumulated entries. They are dropped
    /// at once, and the index is abandoned rather than published with postings
    /// missing.
    refused: bool,
    /// Sorted entries compressed together; [`COMPRESS_CHUNK_ROWS`] outside tests.
    chunk_rows: usize,
}

impl BuildState {
    fn new(
        specs: &[KeySpec],
        reservation: MemoryReservation,
        schema: &arrow_schema::Schema,
    ) -> Result<Self, String> {
        let shapes = specs
            .iter()
            .map(|spec| {
                let columns = spec
                    .columns
                    .iter()
                    .map(|column| KeyColumn::resolve(schema, column))
                    .collect::<Result<Vec<_>, String>>()?;
                Ok(ShapeBuild::new(spec.label.clone(), columns))
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(Self {
            shapes,
            file_ids: HashMap::new(),
            file_order: Vec::new(),
            rows: 0,
            reservation,
            refused: false,
            chunk_rows: COMPRESS_CHUNK_ROWS,
        })
    }

    /// Compresses `rows` sorted entries at a time, so a test can cross chunks
    /// without millions of rows.
    #[cfg(test)]
    fn with_chunk_rows(mut self, rows: usize) -> Self {
        self.chunk_rows = rows;
        self
    }

    /// Ids are assigned on first sight, so the write-time builder does not need
    /// to know the file set in advance.
    fn file_id(&mut self, path: &str) -> Result<u32, String> {
        if let Some(id) = self.file_ids.get(path) {
            return Ok(*id);
        }
        let id = u32::try_from(self.file_order.len())
            .ok()
            .filter(|&id| id <= MAX_FILE_ID)
            .ok_or_else(|| "too many files for the index's address".to_string())?;
        self.file_ids.insert(path.to_string(), id);
        self.file_order.push(path.to_string());
        Ok(id)
    }

    /// The key columns the read-back build projects, once each.
    fn key_columns(&self) -> Vec<KeyColumn> {
        let mut columns: Vec<KeyColumn> = Vec::new();
        for shape in &self.shapes {
            for column in &shape.columns {
                if !columns.iter().any(|c| c.name == column.name) {
                    columns.push(column.clone());
                }
            }
        }
        columns
    }

    fn ingest(
        &mut self,
        file_id: u32,
        positions: RowPositions<'_>,
        batch: &RecordBatch,
        file_path: &str,
    ) -> Result<(), String> {
        if self.refused {
            return Ok(());
        }
        if let RowPositions::Explicit(explicit) = positions
            && explicit.len() != batch.num_rows()
        {
            return Err(format!(
                "{file_path}: {} row positions for {} rows",
                explicit.len(),
                batch.num_rows()
            ));
        }
        for shape in &mut self.shapes {
            let retained = shape.ingest(file_id, positions, batch, file_path)?;
            if self.reservation.try_grow(retained).is_err() {
                self.refuse();
                return Ok(());
            }
        }
        self.rows += batch.num_rows() as u64;
        Ok(())
    }

    /// Abandons the build because the pool cannot fit it, returning what it
    /// accumulated right away rather than when the build ends.
    fn refuse(&mut self) {
        self.refused = true;
        for shape in &mut self.shapes {
            shape.release();
        }
        self.reservation.free();
    }

    /// `Ok(None)` means the memory pool refused the build or the finished index.
    ///
    /// `files` is the snapshot's file set as the SCAN lists it, at `file_set`.
    /// Requiring it to match the set the build actually saw is what makes a
    /// write-time index safe to publish: a file the build never observed would
    /// otherwise be served with no postings at all, which is a false empty rather
    /// than a fallback.
    #[expect(
        clippy::too_many_arguments,
        reason = "each argument is a distinct part of the published index"
    )]
    fn into_index(
        self,
        snapshot_id: String,
        files: &[IndexedFile],
        started: Instant,
        rss_before: Option<u64>,
        session: VortexSession,
        account: &Arc<CayenneMemoryAccount>,
        file_set: FileSetVersion,
    ) -> Result<Option<SnapshotLookupIndex>, String> {
        let Self {
            shapes,
            file_ids,
            file_order,
            rows,
            reservation,
            refused,
            chunk_rows,
        } = self;
        if refused {
            return Ok(None);
        }
        let listed: HashSet<&str> = files.iter().map(|f| f.path.as_str()).collect();
        let observed: HashSet<&str> = file_order.iter().map(String::as_str).collect();
        if listed != observed {
            return Err(format!(
                "indexed file set does not match the snapshot listing ({} observed, {} listed)",
                observed.len(),
                listed.len()
            ));
        }
        // Re-key the postings' file ids onto the listing's order so `files[id]`
        // resolves, whatever order the build happened to see the files in.
        let mut files_by_id: Vec<IndexedFile> = Vec::with_capacity(file_order.len());
        for path in &file_order {
            let file = files
                .iter()
                .find(|f| &f.path == path)
                .ok_or_else(|| format!("listing lost {path}"))?;
            files_by_id.push(file.clone());
        }

        let compressor = BtrBlocksCompressorBuilder::default().build();
        let mut built = Vec::with_capacity(shapes.len());
        let mut resident = 0usize;
        for shape in shapes {
            // Sorting holds a concatenated copy of the key columns and the sort
            // order next to the accumulated chunks, so that is reserved first.
            let accumulated = shape.retained;
            let working = shape.sort_working_bytes();
            if reservation.try_grow(working).is_err() {
                return Ok(None);
            }
            let shape = shape.finish(&session, &compressor, chunk_rows)?;
            reservation.shrink(accumulated.saturating_add(working).min(reservation.size()));
            resident = resident.saturating_add(shape.resident_bytes());
            built.push(shape);
        }
        let Some(resident_reservation) = account.try_reserve_lookup_index(resident) else {
            return Ok(None);
        };
        drop(reservation);

        let per_key_entries = built
            .iter()
            .map(|shape| (shape.label.clone(), shape.distinct_keys, shape.len))
            .collect();
        let file_count = files_by_id.len();
        Ok(Some(SnapshotLookupIndex {
            snapshot_id,
            files: files_by_id,
            file_ids,
            stats: BuildStats {
                duration: started.elapsed(),
                files: file_count,
                rows,
                distinct_keys: built.iter().map(|shape| shape.distinct_keys).sum(),
                rss_before,
                rss_after: super::tuning::proc_self_rss_bytes(),
                per_key_entries,
            },
            shapes: built,
            session,
            file_set,
            reservation: resident_reservation,
        }))
    }
}

/// Result of diffing a write-time index against a read-back build of the same
/// snapshot.
///
/// The write-time index trusts that the writer appends batches in arrival order,
/// so its positions are only as good as that invariant. The read-back build takes
/// every position from Vortex's own `row_idx()`, so diffing the two turns that
/// invariant from an assumption into a check.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LookupIndexVerification {
    /// The snapshot both indexes describe.
    pub snapshot_id: String,
    /// Files the snapshot spans. More than one means the writer's file-roll
    /// path — where positions restart — was actually exercised.
    pub files: usize,
    /// Distinct keys per lookup shape, as `(shape, write-time, read-back)`.
    pub keys_per_shape: Vec<(String, usize, usize)>,
    /// Postings per lookup shape, as `(shape, write-time, read-back)`.
    pub postings_per_shape: Vec<(String, usize, usize)>,
    /// Distinct keys whose postings were compared value-for-value.
    pub keys_compared: usize,
    /// Every disagreement found, described in full.
    pub mismatches: Vec<String>,
}

impl LookupIndexVerification {
    /// `true` when the two builds agree on every key and every row address.
    #[must_use]
    pub fn agrees(&self) -> bool {
        self.mismatches.is_empty()
    }
}

/// How many entry disagreements a verification spells out before summarizing.
const MAX_REPORTED_MISMATCHES: usize = 20;

/// Compares two indexes over the same snapshot, entry for entry. File ids are
/// build-order dependent, so the comparison goes through resolved file paths.
fn diff_indexes(
    write_time: &SnapshotLookupIndex,
    read_back: &SnapshotLookupIndex,
) -> LookupIndexVerification {
    let mut report = LookupIndexVerification {
        snapshot_id: write_time.snapshot_id.clone(),
        files: write_time.files.len(),
        ..LookupIndexVerification::default()
    };
    if write_time.snapshot_id != read_back.snapshot_id {
        report.mismatches.push(format!(
            "snapshot ids differ: {} vs {}",
            write_time.snapshot_id, read_back.snapshot_id
        ));
        return report;
    }
    if write_time.shapes.len() != read_back.shapes.len() {
        report
            .mismatches
            .push("different key-shape counts".to_string());
        return report;
    }

    for (write_shape, read_shape) in write_time.shapes.iter().zip(&read_back.shapes) {
        let label = &write_shape.label;
        report.keys_per_shape.push((
            label.clone(),
            write_shape.distinct_keys,
            read_shape.distinct_keys,
        ));
        report
            .postings_per_shape
            .push((label.clone(), write_shape.len, read_shape.len));
        if write_shape.distinct_keys != read_shape.distinct_keys {
            report.mismatches.push(format!(
                "{label}: {} keys write-time vs {} read-back",
                write_shape.distinct_keys, read_shape.distinct_keys
            ));
        }

        let entries = write_shape
            .resolved_entries(write_time)
            .and_then(|w| read_shape.resolved_entries(read_back).map(|r| (w, r)));
        let (written, read) = match entries {
            Ok(entries) => entries,
            Err(error) => {
                report
                    .mismatches
                    .push(format!("{label}: could not read entries: {error}"));
                continue;
            }
        };
        report.keys_compared += read_shape.distinct_keys;
        if written.len() != read.len() {
            report.mismatches.push(format!(
                "{label}: {} postings write-time vs {} read-back",
                written.len(),
                read.len()
            ));
        }
        let mut disagreements = 0usize;
        for (index, (w, r)) in written.iter().zip(&read).enumerate() {
            if w == r {
                continue;
            }
            disagreements += 1;
            if disagreements <= MAX_REPORTED_MISMATCHES {
                report.mismatches.push(format!(
                    "{label} entry {index}: write-time ({:?}, {}) vs read-back ({:?}, {})",
                    w.1, w.2, r.1, r.2
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
    report
}

/// Diffs `published` against a fresh read-back build of the same snapshot's
/// `files`.
pub(crate) async fn verify_against_read_back(
    state: &Arc<LookupIndexState>,
    published: Arc<SnapshotLookupIndex>,
    store: &Arc<dyn ObjectStore>,
    files: Vec<IndexedFile>,
    schema: &Arc<arrow_schema::Schema>,
) -> Result<LookupIndexVerification, String> {
    let snapshot_id = published.snapshot_id.clone();
    let read_back = build(state, snapshot_id, store, files, schema, published.file_set)
        .await?
        .ok_or_else(|| "the memory pool refused the read-back build".to_string())?;
    // Decoding every entry of both indexes is CPU work, like building them.
    tokio::task::spawn_blocking(move || diff_indexes(&published, &read_back))
        .await
        .map_err(|e| format!("index verification task failed: {e}"))
}

/// Builds the index from the rows as they are WRITTEN, instead of reading the
/// finished files back afterwards.
///
/// The Vortex sink already knows which file each batch lands in and how many
/// rows precede it there; this turns that into postings directly, so a refresh
/// publishes its snapshot and its index together and no query is ever served by
/// a full scan while an index is rebuilt.
///
/// Every shard writer calls in concurrently, so the accumulator is behind one
/// mutex. Key extraction is small next to Vortex encode, but this is the obvious
/// place to shard if it ever shows up in a profile.
pub(crate) struct IncrementalIndexBuilder {
    table_name: String,
    snapshot_id: String,
    /// `None` once the accumulator has been taken by `finish`.
    state: Mutex<Option<BuildState>>,
    /// First failure seen inside the observer. The trait cannot return an error,
    /// and a partially-built index must never be published, so the failure is
    /// recorded and checked before publication.
    failure: Mutex<Option<String>>,
    started: Instant,
    rss_before: Option<u64>,
}

impl std::fmt::Debug for IncrementalIndexBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncrementalIndexBuilder")
            .field("table", &self.table_name)
            .field("snapshot_id", &self.snapshot_id)
            .finish_non_exhaustive()
    }
}

impl IncrementalIndexBuilder {
    fn new(
        table_name: String,
        snapshot_id: String,
        specs: &[KeySpec],
        reservation: MemoryReservation,
        schema: &arrow_schema::Schema,
    ) -> Result<Self, String> {
        Ok(Self {
            table_name,
            snapshot_id,
            state: Mutex::new(Some(BuildState::new(specs, reservation, schema)?)),
            failure: Mutex::new(None),
            started: Instant::now(),
            rss_before: super::tuning::proc_self_rss_bytes(),
        })
    }

    pub(crate) fn snapshot_id(&self) -> &str {
        &self.snapshot_id
    }

    fn record_failure(&self, message: String) {
        let mut failure = self.failure.lock();
        if failure.is_none() {
            tracing::warn!(
                table = %self.table_name,
                snapshot_id = %self.snapshot_id,
                error = %message,
                "Write-time point-lookup index build failed; the snapshot will publish unindexed"
            );
            *failure = Some(message);
        }
    }

    /// Consumes the accumulated postings. `files` is the snapshot's file set as
    /// the scan lists it, at `file_set`, which also supplies the sizes
    /// and modification times the scan-time snapshot check compares.
    fn finish(
        &self,
        files: &[IndexedFile],
        account: &Arc<CayenneMemoryAccount>,
        file_set: FileSetVersion,
    ) -> Result<Option<SnapshotLookupIndex>, String> {
        if let Some(failure) = self.failure.lock().clone() {
            return Err(failure);
        }
        let state = self
            .state
            .lock()
            .take()
            .ok_or_else(|| "index accumulator already consumed".to_string())?;
        state.into_index(
            self.snapshot_id.clone(),
            files,
            self.started,
            self.rss_before,
            VortexSession::default(),
            account,
            file_set,
        )
    }
}

impl vortex_datafusion::VortexWriteObserver for IncrementalIndexBuilder {
    fn batch_written(
        &self,
        file_path: &object_store::path::Path,
        first_row_position: u64,
        batch: &RecordBatch,
    ) {
        let path: &str = file_path.as_ref();
        let mut slot = self.state.lock();
        // `None` once the build has been consumed; a late batch then has nowhere
        // to go, and publishing a partial index is never acceptable.
        let Some(state) = slot.as_mut() else {
            drop(slot);
            self.record_failure("batch written after the index build was consumed".to_string());
            return;
        };
        if state.refused {
            return;
        }
        let file_id = match state.file_id(path) {
            Ok(id) => id,
            Err(e) => {
                drop(slot);
                self.record_failure(e);
                return;
            }
        };
        if let Err(e) = state.ingest(
            file_id,
            RowPositions::Contiguous(first_row_position),
            batch,
            path,
        ) {
            drop(slot);
            self.record_failure(e);
        }
    }
}

/// Builds the index by reading the snapshot's finished files.
///
/// Every position comes from Vortex's `row_idx()` rather than from counting the
/// rows a scan returns, so this build shares no assumption about row order with
/// the write-time one it verifies.
///
/// Decoding and ingesting is CPU work for the whole table, so the build runs on
/// the blocking pool and drives its file reads from there rather than occupying
/// a runtime worker between reads.
async fn build(
    state: &Arc<LookupIndexState>,
    snapshot_id: String,
    store: &Arc<dyn ObjectStore>,
    files: Vec<IndexedFile>,
    schema: &Arc<arrow_schema::Schema>,
    file_set: FileSetVersion,
) -> Result<Option<SnapshotLookupIndex>, String> {
    let (state, store, schema) = (Arc::clone(state), Arc::clone(store), Arc::clone(schema));
    let runtime = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        runtime.block_on(read_back(
            &state,
            snapshot_id,
            &store,
            files,
            &schema,
            file_set,
        ))
    })
    .await
    .map_err(|e| format!("index build task failed: {e}"))?
}

async fn read_back(
    state: &LookupIndexState,
    snapshot_id: String,
    store: &Arc<dyn ObjectStore>,
    files: Vec<IndexedFile>,
    schema: &arrow_schema::Schema,
    file_set: FileSetVersion,
) -> Result<Option<SnapshotLookupIndex>, String> {
    use vortex::expr::{get_item, pack, root};

    let started = Instant::now();
    let rss_before = super::tuning::proc_self_rss_bytes();
    let mut build = BuildState::new(&state.specs, state.build_reservation(), schema)?;
    let columns = build.key_columns();
    let session = VortexSession::default();

    let mut target_fields: Vec<FieldRef> = columns.iter().map(KeyColumn::stored_field).collect();
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

    for file in &files {
        let file_id = build.file_id(&file.path)?;

        let vxf = session
            .open_options()
            .open_object_store(store, &file.path)
            .await
            .map_err(|e| format!("open {}: {e}", file.path))?;

        let mut stream = vxf
            .scan()
            .map_err(|e| format!("scan {}: {e}", file.path))?
            .with_projection(projection.clone())
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
                .ok_or_else(|| format!("{}: row positions are not UInt64", file.path))?
                .clone();
            build.ingest(
                file_id,
                RowPositions::Explicit(&positions),
                &batch,
                &file.path,
            )?;
            if build.refused {
                return Ok(None);
            }
        }
    }

    build.into_index(
        snapshot_id,
        &files,
        started,
        rss_before,
        session,
        &state.account,
        file_set,
    )
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

    fn build_reservation(pool: &Arc<dyn MemoryPool>) -> MemoryReservation {
        MemoryConsumer::new("lookup_index_test_build").register(pool)
    }

    fn spec(columns: &[&str]) -> KeySpec {
        KeySpec::new(columns.iter().map(|c| (*c).to_string()).collect()).expect("columns")
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

    #[test]
    fn packed_postings_round_trip_file_and_position() {
        let packed = (u64::from(MAX_FILE_ID) << POSITION_BITS) | POSITION_MASK;
        assert_eq!(packed >> POSITION_BITS, u64::from(MAX_FILE_ID));
        assert_eq!(packed & POSITION_MASK, POSITION_MASK);
    }

    #[test]
    fn a_table_selection_removes_deleted_candidates() {
        let deleted: roaring::RoaringTreemap = [3u64, 9].into_iter().collect();
        let exclude = Selection::ExcludeRoaring(deleted);
        let kept: Vec<u64> = [1u64, 3, 5, 9]
            .into_iter()
            .filter(|&p| selection_keeps(&exclude, p))
            .collect();
        assert_eq!(kept, vec![1, 5]);
        let include = Selection::IncludeByIndex(Buffer::from_iter([5u64, 9]));
        assert!(selection_keeps(&include, 9));
        assert!(!selection_keeps(&include, 1));
        assert!(selection_keeps(&Selection::All, 1));
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

    #[test]
    fn floating_point_key_columns_are_refused() {
        for data_type in [DataType::Float16, DataType::Float32, DataType::Float64] {
            let schema =
                arrow_schema::Schema::new(vec![Field::new("score", data_type.clone(), false)]);
            let error = KeyColumn::resolve(&schema, "score").expect_err("float index rejected");
            assert!(
                error.contains("unsupported floating-point type")
                    && error.contains(&data_type.to_string()),
                "unexpected error for {data_type}: {error}"
            );
        }
    }

    fn keyed_schema() -> Arc<arrow_schema::Schema> {
        Arc::new(arrow_schema::Schema::new(vec![
            Field::new("tenant", DataType::Int64, true),
            Field::new("service", DataType::Utf8, true),
            Field::new("region", DataType::Utf8, true),
        ]))
    }

    fn keyed_batch(
        tenants: Vec<Option<i64>>,
        services: Vec<Option<String>>,
        regions: Vec<Option<String>>,
    ) -> RecordBatch {
        RecordBatch::try_new(
            keyed_schema(),
            vec![
                Arc::new(Int64Array::from(tenants)),
                Arc::new(StringArray::from(services)),
                Arc::new(StringArray::from(regions)),
            ],
        )
        .expect("batch")
    }

    fn scalar(column: &str, value: &str) -> Option<ScalarValue> {
        match column {
            "tenant" => Some(ScalarValue::Int64(value.parse().ok())),
            "service" | "region" => Some(ScalarValue::Utf8(
                (value != "NULL").then(|| value.to_string()),
            )),
            _ => None,
        }
    }

    /// Every key of every width, including keys that straddle and coincide with
    /// block heads, resolves to exactly the addresses a brute-force map holds,
    /// and absent or NULL keys resolve to nothing.
    #[test]
    fn probes_match_a_brute_force_map_for_every_key_width() {
        let columns = ["tenant", "service", "region"];
        for width in 1..=columns.len() {
            let key_columns = &columns[..width];
            let pool = unbounded_pool();
            let mut build = BuildState::new(
                &[spec(key_columns)],
                build_reservation(&pool),
                &keyed_schema(),
            )
            .expect("build state")
            .with_chunk_rows(BLOCK_ROWS * 2);

            let mut expected: HashMap<Vec<String>, Vec<(String, u64)>> = HashMap::new();
            let mut files = Vec::new();
            let mut state = 0x9E37_79B9_7F4A_7C15u64;
            for file in 0..3u64 {
                let path = format!("snapshot/file-{file}.vortex");
                files.push(IndexedFile {
                    path: path.clone(),
                    size: 1,
                    last_modified_ms: 0,
                });
                let file_id = build.file_id(&path).expect("file id");
                let mut position = 0u64;
                for _batch in 0..4 {
                    let (mut tenants, mut services, mut regions) =
                        (Vec::new(), Vec::new(), Vec::new());
                    for _ in 0..(BLOCK_ROWS * 3 / 2) {
                        state = state
                            .wrapping_mul(6_364_136_223_846_793_005)
                            .wrapping_add(1_442_695_040_888_963_407);
                        // Few distinct values, so most keys repeat across blocks
                        // and files.
                        let tenant = i64::try_from((state >> 33) % 7).expect("small");
                        let service = format!("s{}", (state >> 13) % 40);
                        let region = format!("r{}", (state >> 23) % 3);
                        let null = (state >> 7).is_multiple_of(23);
                        let values = [tenant.to_string(), service.clone(), region.clone()];
                        // The nullable column is the first key column at every
                        // width, so a NULL always leaves the row unindexed.
                        if !null {
                            expected
                                .entry(values[..width].to_vec())
                                .or_default()
                                .push((path.clone(), position));
                        }
                        tenants.push(if null { None } else { Some(tenant) });
                        services.push(Some(service));
                        regions.push(Some(region));
                        position += 1;
                    }
                    let start = position - tenants.len() as u64;
                    build
                        .ingest(
                            file_id,
                            RowPositions::Contiguous(start),
                            &keyed_batch(tenants, services, regions),
                            &path,
                        )
                        .expect("ingest");
                }
            }

            let index = build
                .into_index(
                    "snapshot".to_string(),
                    &files,
                    Instant::now(),
                    None,
                    VortexSession::default(),
                    &account(&pool),
                    FileSetVersion::default(),
                )
                .expect("finish")
                .expect("the unbounded pool fits it");
            assert!(
                index.shapes[0].blocks() > 4,
                "fixture must span many blocks"
            );
            assert_eq!(
                index.shapes[0].distinct_keys,
                expected.len(),
                "width {width}"
            );

            for (values, addresses) in &expected {
                let hit = index
                    .probe(&|column| {
                        let at = key_columns.iter().position(|c| *c == column)?;
                        scalar(column, &values[at])
                    })
                    .expect("pinned key");
                let mut found: Vec<(String, u64)> = hit
                    .per_file
                    .iter()
                    .flat_map(|(path, positions)| positions.iter().map(|p| (path.clone(), *p)))
                    .collect();
                found.sort();
                let mut wanted = addresses.clone();
                wanted.sort();
                assert_eq!(found, wanted, "width {width}, key {values:?}");
            }

            // Tenants run 0..7, so 99 is absent at every width.
            let absent = ["99", "absent", "nowhere"];
            let miss = index
                .probe(&|column| {
                    let at = key_columns.iter().position(|c| *c == column)?;
                    scalar(column, absent[at])
                })
                .expect("pinned key");
            assert!(miss.per_file.is_empty(), "width {width}");
            let null = index
                .probe(&|column| {
                    let at = key_columns.iter().position(|c| *c == column)?;
                    scalar(column, if at == 0 { "NULL" } else { "s1" })
                })
                .expect("pinned key");
            assert!(null.per_file.is_empty(), "width {width}");
            assert!(
                index
                    .probe(&|column| (column == "service").then(|| ScalarValue::from("s1")))
                    .is_none()
                    || width == 1,
                "a key with an unpinned column must not be answered"
            );
        }
    }

    /// A single shifted address is reported, so the read-back verification can
    /// actually fail.
    #[test]
    fn verification_reports_a_shifted_address() {
        let files = vec![IndexedFile {
            path: "snapshot/file.vortex".to_string(),
            size: 1,
            last_modified_ms: 0,
        }];
        let pool = unbounded_pool();
        let index_with = |shift: u64| {
            let mut build = BuildState::new(
                &[spec(&["tenant", "service"])],
                build_reservation(&pool),
                &keyed_schema(),
            )
            .expect("build state");
            let file_id = build.file_id(&files[0].path).expect("file id");
            build
                .ingest(
                    file_id,
                    RowPositions::Contiguous(shift),
                    &keyed_batch(
                        vec![Some(1), Some(2)],
                        vec![Some("a".to_string()), Some("b".to_string())],
                        vec![None, None],
                    ),
                    &files[0].path,
                )
                .expect("ingest");
            build
                .into_index(
                    "snapshot".to_string(),
                    &files,
                    Instant::now(),
                    None,
                    VortexSession::default(),
                    &account(&pool),
                    FileSetVersion::default(),
                )
                .expect("finish")
                .expect("the unbounded pool fits it")
        };
        assert!(diff_indexes(&index_with(0), &index_with(0)).agrees());
        let report = diff_indexes(&index_with(1), &index_with(0));
        assert!(!report.agrees());
        assert_eq!(
            report.keys_per_shape,
            vec![("(tenant, service)".to_string(), 2, 2)]
        );
    }

    /// A build the pool cannot fit gives back what it accumulated at once, and
    /// a published index holds its bytes in the account only while it lives.
    #[test]
    fn index_memory_is_admitted_by_the_pool_and_released_with_the_index() {
        let rows = 4_096usize;
        let batch = keyed_batch(
            (0..rows).map(|i| i64::try_from(i).ok()).collect(),
            (0..rows).map(|i| Some(format!("service-{i:08}"))).collect(),
            vec![None; rows],
        );
        let files = vec![IndexedFile {
            path: "snapshot/file.vortex".to_string(),
            size: 1,
            last_modified_ms: 0,
        }];

        let small: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16 * 1024));
        let mut refused = BuildState::new(
            &[spec(&["tenant", "service"])],
            build_reservation(&small),
            &keyed_schema(),
        )
        .expect("build state");
        refused
            .ingest(0, RowPositions::Contiguous(0), &batch, &files[0].path)
            .expect("ingest");
        assert!(refused.refused, "a 16 KiB pool cannot fit 4,096 keys");
        assert_eq!(small.reserved(), 0, "a refused build holds nothing");
        assert!(refused.shapes[0].postings.is_empty());

        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 * 1024 * 1024));
        let table = account(&pool);
        let mut fits = BuildState::new(
            &[spec(&["tenant", "service"])],
            build_reservation(&pool),
            &keyed_schema(),
        )
        .expect("build state");
        let file_id = fits.file_id(&files[0].path).expect("file id");
        fits.ingest(file_id, RowPositions::Contiguous(0), &batch, &files[0].path)
            .expect("ingest");
        assert!(pool.reserved() > 0, "accumulated entries are reserved");
        let index = fits
            .into_index(
                "snapshot".to_string(),
                &files,
                Instant::now(),
                None,
                VortexSession::default(),
                &table,
                FileSetVersion::default(),
            )
            .expect("finish")
            .expect("fits");
        let resident = index.reservation.bytes();
        assert!(resident > 0);
        assert_eq!(
            pool.reserved(),
            resident,
            "only the resident index stays reserved"
        );
        assert_eq!(table.snapshot().lookup_index, resident);
        drop(index);
        assert_eq!(pool.reserved(), 0, "dropping the index releases its bytes");
        assert_eq!(table.snapshot().lookup_index, 0);
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

    fn tiny_index(
        pool: &Arc<dyn MemoryPool>,
        table: &Arc<CayenneMemoryAccount>,
        snapshot: &str,
    ) -> Arc<SnapshotLookupIndex> {
        let files = vec![IndexedFile {
            path: format!("{snapshot}/file.vortex"),
            size: 1,
            last_modified_ms: 0,
        }];
        let mut build = BuildState::new(
            &[spec(&["tenant"])],
            build_reservation(pool),
            &keyed_schema(),
        )
        .expect("build state");
        let file_id = build.file_id(&files[0].path).expect("file id");
        build
            .ingest(
                file_id,
                RowPositions::Contiguous(0),
                &keyed_batch(vec![Some(1)], vec![Some("a".to_string())], vec![None]),
                &files[0].path,
            )
            .expect("ingest");
        Arc::new(
            build
                .into_index(
                    snapshot.to_string(),
                    &files,
                    Instant::now(),
                    None,
                    VortexSession::default(),
                    table,
                    FileSetVersion::default(),
                )
                .expect("finish")
                .expect("fits"),
        )
    }

    /// A background build publishes into a slot emptied while it ran — the index
    /// it meant to replace went stale — but never over an index published by
    /// someone else in the meantime.
    #[tokio::test]
    async fn a_build_fills_an_emptied_slot_but_never_overwrites_a_newer_index() {
        let pool = unbounded_pool();
        let table = account(&pool);
        let state = LookupIndexState::new(
            "overtaken",
            vec![spec(&["tenant"])],
            Arc::clone(&pool),
            Arc::clone(&table),
        )
        .expect("state");
        let old = tiny_index(&pool, &table, "s1");
        state.store_index(Some(Arc::clone(&old)));

        // A build claimed while `old` was published; `old` then goes stale.
        let claimed = state.generation.load(Ordering::Acquire);
        state.discard_stale(&old);
        drop(old);
        assert!(state.published().is_none());
        assert_eq!(
            table.snapshot().lookup_index,
            0,
            "a dropped stale index holds nothing"
        );
        let rebuilt = tiny_index(&pool, &table, "s2");
        assert!(state.publish_unless_overtaken(claimed, Arc::clone(&rebuilt)));
        assert!(
            state
                .published()
                .is_some_and(|index| Arc::ptr_eq(&index, &rebuilt))
        );

        // A build claimed before a newer index is published never replaces it.
        let claimed = state.generation.load(Ordering::Acquire);
        let newer = tiny_index(&pool, &table, "s3");
        state.store_index(Some(Arc::clone(&newer)));
        assert!(!state.publish_unless_overtaken(claimed, tiny_index(&pool, &table, "s2")));
        assert!(
            state
                .published()
                .is_some_and(|index| Arc::ptr_eq(&index, &newer))
        );
    }

    #[tokio::test]
    async fn a_claim_dropped_before_its_build_runs_frees_the_slot() {
        let pool = unbounded_pool();
        let state = LookupIndexState::new(
            "claims",
            vec![spec(&["tenant"])],
            Arc::clone(&pool),
            account(&pool),
        )
        .expect("state");
        let claim = state.claim_build("snapshot").expect("first claim");
        assert!(
            state.claim_build("snapshot").is_none(),
            "one build at a time"
        );
        drop(claim);
        let claim = state
            .claim_build("snapshot")
            .expect("a dropped claim frees the slot without delaying the next");
        claim.unpublished();
        assert!(
            state.claim_build("snapshot").is_none(),
            "a build that published nothing delays the next"
        );
        assert_eq!(state.counters().builds_started, 2);
        assert_eq!(state.counters().builds_unpublished, 1);
    }
}
