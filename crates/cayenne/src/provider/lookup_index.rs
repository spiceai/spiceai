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

//! Secondary point-lookup index mapping an exact composite equality
//! key to the physical `(file, file-local row position)` addresses that hold it.
//!
//! One immutable snapshot is indexed in memory; the index is bound to that
//! snapshot's exact file set, and any deviation falls back to the ordinary scan.
//! A full refresh builds the replacement during the write and publishes it with
//! the snapshot, so there is no window in which lookups lose the index. Nothing
//! is persisted — a restart rebuilds from the finished files.
//!
//! Configured per table by `cayenne_lookup_index_keys`, each entry one composite
//! equality key:
//!
//! ```yaml
//! acceleration:
//!   engine: cayenne
//!   params:
//!     cayenne_lookup_index_keys: 'TenantId+ServiceId,TenantId+PoolId'
//! ```
//!
//! Unset, the whole module is inert. `cayenne_lookup_index_max_bytes` caps the
//! estimated footprint; unset it derives from the table's memory configuration.
//! A build that exceeds the cap is abandoned and the table keeps scanning —
//! this index is always safe to drop, which is what lets it degrade rather than
//! fail.
//!
//! Each key is held as sorted, compressed Vortex arrays: its two columns in their
//! stored types and one packed `(file, position)` column, ordered by key and then
//! by address. Resident size is therefore close to the compressed size of the key
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
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use crate::row_converter::{RowConverter, SortField};
use arc_swap::ArcSwapOption;
use arrow::array::{Array, ArrayRef, AsArray, UInt32Array, UInt64Array};
use arrow::compute::SortColumn;
use arrow::datatypes::UInt64Type;
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, FieldRef};
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

/// Floor used when a table's derived budget would be smaller: the PK keyset's
/// default budget, the structure this one is sized alongside.
const DEFAULT_BUDGET_FLOOR: usize = super::context::DEFAULT_PK_KEYSET_CACHE_MAX_BYTES;

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
enum ProbeOutcome {
    /// A row selection was attached to the scan.
    Selected,
    /// The key has no posting, so the scan reads no file.
    Empty,
    /// No index has been published yet.
    Unbuilt,
    /// The scan's files are not the ones the index was built from.
    SnapshotMismatch,
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

/// One composite key the index is maintained on, in predicate order.
#[derive(Clone, Debug)]
pub(crate) struct KeySpec {
    columns: [String; 2],
    /// `"TenantId+ServiceId"` — the `shape` metric dimension.
    label: String,
}

impl KeySpec {
    /// Parses one `"ColA+ColB"` entry of `cayenne_lookup_index_keys`.
    fn parse(raw: &str) -> Option<Self> {
        let mut parts = raw.split('+').map(str::trim).filter(|p| !p.is_empty());
        let first = parts.next()?.to_string();
        let second = parts.next()?.to_string();
        if parts.next().is_some() {
            return None;
        }
        let label = format!("{first}+{second}");
        Some(Self {
            columns: [first, second],
            label,
        })
    }

    /// Every usable key in a table's `cayenne_lookup_index_keys`.
    pub(crate) fn parse_all(entries: &[String], table_name: &str) -> Vec<Self> {
        let mut specs = Vec::new();
        for entry in entries
            .iter()
            .flat_map(|e| e.split(','))
            .map(str::trim)
            .filter(|e| !e.is_empty())
        {
            if let Some(spec) = Self::parse(entry) {
                specs.push(spec);
            } else {
                tracing::warn!(
                    table = %table_name,
                    entry = %entry,
                    "cayenne_lookup_index_keys entry is not 'ColA+ColB'; ignored"
                );
            }
        }
        specs
    }
}

/// Per-table index state, keyed by table id so the table provider itself stays
/// untouched.
static STATES: LazyLock<Mutex<HashMap<String, Arc<LookupIndexState>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// This table's index state, or `None` when `cayenne_lookup_index_keys` is unset
/// or names no usable key.
pub(crate) fn state_for(
    table_id: &str,
    table_name: &str,
    config: &crate::metadata::VortexConfig,
) -> Option<Arc<LookupIndexState>> {
    if config.lookup_index_keys.is_empty() {
        return None;
    }
    let mut states = STATES.lock();
    if let Some(state) = states.get(table_id) {
        return Some(Arc::clone(state));
    }
    let specs = KeySpec::parse_all(&config.lookup_index_keys, table_name);
    if specs.is_empty() {
        return None;
    }
    let state = Arc::new(LookupIndexState {
        table_name: table_name.to_string(),
        specs,
        max_bytes: AtomicUsize::new(
            config
                .lookup_index_max_bytes
                .unwrap_or(DEFAULT_BUDGET_FLOOR),
        ),
        configured_max_bytes: config.lookup_index_max_bytes,
        account: Mutex::new(None),
        index: ArcSwapOption::empty(),
        build_in_flight: AtomicBool::new(false),
        pending: Mutex::new(None),
        counters: Counters::default(),
    });
    tracing::info!(
        table = %table_name,
        table_id = %table_id,
        keys = ?state.specs.iter().map(|s| s.label.as_str()).collect::<Vec<_>>(),
        max_bytes = ?config.lookup_index_max_bytes,
        "Cayenne point-lookup index enabled"
    );
    states.insert(table_id.to_string(), Arc::clone(&state));
    Some(state)
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
struct KeyColumn {
    /// The column's name in the table schema.
    name: String,
    /// The column's stored type. Build and probe both cast to it, so an encoding
    /// never depends on how a value reached the index.
    data_type: DataType,
    /// Whether the stored column admits nulls.
    nullable: bool,
}

impl KeyColumn {
    /// Resolves a configured key column: an exact name wins, then a unique
    /// case-insensitive match. Two case-insensitive candidates are an error
    /// rather than a guess, because building from one column and probing with
    /// the other would miss rows.
    fn resolve(schema: &arrow_schema::Schema, configured: &str) -> Result<Self, String> {
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

/// The byte-comparable encoding of one `(first, second)` key.
fn key_converter(columns: &[KeyColumn; 2]) -> Result<RowConverter, String> {
    RowConverter::new(vec![
        SortField::new(columns[0].data_type.clone()),
        SortField::new(columns[1].data_type.clone()),
    ])
    .map_err(|e| {
        format!(
            "key ({}, {}) cannot be row-encoded: {e}",
            columns[0].data_type, columns[1].data_type
        )
    })
}

/// Casts `array` to `data_type`, or returns it unchanged when it already matches.
fn cast_to(array: &ArrayRef, data_type: &DataType) -> Result<ArrayRef, String> {
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
    pub(crate) approx_bytes: usize,
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

/// One composite key over the snapshot, as sorted compressed arrays.
struct ShapeIndex {
    label: String,
    columns: [KeyColumn; 2],
    converter: RowConverter,
    /// The key columns and packed postings, sorted by key and then by posting.
    first: vortex::array::ArrayRef,
    second: vortex::array::ArrayRef,
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
        usize_of(self.first.nbytes())
            .saturating_add(usize_of(self.second.nbytes()))
            .saturating_add(usize_of(self.postings.nbytes()))
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

    /// The packed postings of the key `first + second`.
    fn probe(
        &self,
        session: &VortexSession,
        first: &ScalarValue,
        second: &ScalarValue,
    ) -> ShapeProbe {
        let literal = |scalar: &ScalarValue, column: &KeyColumn| {
            scalar
                .cast_to(&column.data_type)
                .ok()?
                .to_array_of_size(1)
                .ok()
        };
        let (Some(first), Some(second)) = (
            literal(first, &self.columns[0]),
            literal(second, &self.columns[1]),
        ) else {
            return ShapeProbe::Unanswerable;
        };
        // A NULL literal never satisfies an equality predicate.
        if first.is_null(0) || second.is_null(0) {
            return ShapeProbe::Postings(Vec::new());
        }
        let Ok(rows) = self.converter.convert_columns(&[first, second]) else {
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

    /// Decodes both key columns of the entries `range`.
    fn decode_keys(
        &self,
        session: &VortexSession,
        range: Range<usize>,
        ctx: &mut ExecutionCtx,
    ) -> Result<[ArrayRef; 2], String> {
        Ok([
            decode(
                session,
                &self.first,
                range.clone(),
                &self.columns[0].indexed_field(),
                ctx,
            )?,
            decode(
                session,
                &self.second,
                range,
                &self.columns[1].indexed_field(),
                ctx,
            )?,
        ])
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
            let Some(first) = scalar_for(&shape.columns[0].name) else {
                continue;
            };
            let Some(second) = scalar_for(&shape.columns[1].name) else {
                continue;
            };
            let postings = match shape.probe(&self.session, &first, &second) {
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
    pub(crate) fn restrict(
        self,
        snapshot_id: &str,
        file_groups: Vec<FileGroup>,
        table_plans: Arc<dyn VortexAccessPlanProvider>,
    ) -> (Vec<FileGroup>, Option<Arc<dyn VortexAccessPlanProvider>>) {
        if !self.validate(snapshot_id, file_groups.iter().flat_map(FileGroup::iter)) {
            self.state
                .record_probe(&self.shape, ProbeOutcome::SnapshotMismatch);
            return (file_groups, None);
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
            return (file_groups, None);
        }
        self.state
            .record_selection(&self.shape, candidate_files as u64, self.rows as u64);
        let provider = LookupAccessPlanProvider {
            state: self.state,
            selections: self.per_file,
            table: table_plans,
        };
        (file_groups, Some(Arc::new(provider)))
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

/// Probe accounting for one table. Metrics also go to OpenTelemetry; these
/// process-local counters are what a correctness check can assert on to prove a
/// query really used file/row selection instead of silently scanning.
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
}

#[derive(Default)]
struct Counters {
    selected: AtomicU64,
    empty: AtomicU64,
    unbuilt: AtomicU64,
    snapshot_mismatch: AtomicU64,
    candidate_files: AtomicU64,
    candidate_rows: AtomicU64,
    access_plans_attached: AtomicU64,
    index_bytes: AtomicU64,
}

impl Counters {
    fn snapshot(&self) -> LookupIndexCounters {
        LookupIndexCounters {
            selected: self.selected.load(Ordering::Relaxed),
            empty: self.empty.load(Ordering::Relaxed),
            unbuilt: self.unbuilt.load(Ordering::Relaxed),
            snapshot_mismatch: self.snapshot_mismatch.load(Ordering::Relaxed),
            candidate_files: self.candidate_files.load(Ordering::Relaxed),
            candidate_rows: self.candidate_rows.load(Ordering::Relaxed),
            access_plans_attached: self.access_plans_attached.load(Ordering::Relaxed),
            index_bytes: self.index_bytes.load(Ordering::Relaxed),
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

/// Probe accounting for `table_name`, or `None` when the table has no index
/// state (the table configures no lookup index).
#[must_use]
pub fn counters_for_table(table_name: &str) -> Option<LookupIndexCounters> {
    STATES
        .lock()
        .values()
        .find(|state| state.table_name == table_name)
        .map(|state| state.counters.snapshot())
}

pub(crate) struct LookupIndexState {
    table_name: String,
    specs: Vec<KeySpec>,
    /// An explicit `cayenne_lookup_index_max_bytes`, which outranks the figure
    /// derived from the table's memory configuration.
    configured_max_bytes: Option<usize>,
    /// Cap on the index's bytes, both while it is being built and once it is
    /// resident. Seeded from the table's Cayenne memory configuration.
    max_bytes: AtomicUsize,
    /// Publishes the index's resident bytes into the table's `DataFusion` pool
    /// reservation, so a query plans against the budget this index is actually
    /// consuming instead of one that ignores it.
    account: Mutex<Option<Arc<super::memory_account::CayenneMemoryAccount>>>,
    index: ArcSwapOption<SnapshotLookupIndex>,
    build_in_flight: AtomicBool,
    /// A write-time build in progress for a snapshot that is not visible yet.
    pending: Mutex<Option<Arc<IncrementalIndexBuilder>>>,
    counters: Counters,
}

impl LookupIndexState {
    pub(crate) fn published(&self) -> Option<Arc<SnapshotLookupIndex>> {
        self.index.load_full()
    }

    /// `true` when this snapshot has no published index and no build is running,
    /// claiming the build slot for the caller.
    pub(crate) fn claim_build(&self, snapshot_id: &str) -> bool {
        if self
            .index
            .load()
            .as_ref()
            .is_some_and(|index| index.snapshot_id == snapshot_id)
        {
            return false;
        }
        self.build_in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Releases the build slot taken by [`Self::claim_build`], whether the build
    /// finished or could not start.
    pub(crate) fn release_build(&self) {
        self.build_in_flight.store(false, Ordering::Release);
    }

    /// Installs this table's memory-derived budget and the pool account the
    /// index reserves against.
    ///
    /// The index is sized the same way the PK keyset is — both are long-lived
    /// per-table resident state outside query execution — so it inherits that
    /// knob rather than introducing a second, unrelated one. An explicit
    /// `cayenne_lookup_index_max_bytes` still wins.
    pub(crate) fn set_budget_from(
        &self,
        configured_bytes: usize,
        account: &Arc<super::memory_account::CayenneMemoryAccount>,
    ) {
        if let Some(override_bytes) = self.configured_max_bytes {
            self.max_bytes.store(override_bytes, Ordering::Relaxed);
        } else {
            self.max_bytes.store(
                configured_bytes.max(DEFAULT_BUDGET_FLOOR),
                Ordering::Relaxed,
            );
        }
        let mut slot = self.account.lock();
        if slot.is_none() {
            *slot = Some(Arc::clone(account));
        }
    }

    fn max_bytes(&self) -> usize {
        self.max_bytes.load(Ordering::Relaxed)
    }

    /// Publishes the index's resident bytes into the table's pool reservation.
    fn account_bytes(&self, bytes: usize) {
        self.counters
            .index_bytes
            .store(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::Relaxed);
        if let Some(account) = self.account.lock().as_ref() {
            account.set_lookup_index_bytes(bytes);
        }
    }

    /// Starts a write-time build for a snapshot that is not visible yet. The
    /// returned observer is handed to the write; [`Self::publish_pending`]
    /// promotes it once the snapshot's files are final.
    pub(crate) fn begin_incremental_build(
        self: &Arc<Self>,
        snapshot_id: &str,
        schema: &arrow_schema::Schema,
    ) -> Option<Arc<IncrementalIndexBuilder>> {
        let builder = match IncrementalIndexBuilder::new(
            self.table_name.clone(),
            snapshot_id.to_string(),
            &self.specs,
            self.max_bytes(),
            schema,
        ) {
            Ok(builder) => Arc::new(builder),
            Err(error) => {
                tracing::warn!(
                    table = %self.table_name,
                    %error,
                    "Write-time point-lookup index could not start; the table stays unindexed"
                );
                return None;
            }
        };
        *self.pending.lock() = Some(Arc::clone(&builder));
        Some(builder)
    }

    /// Promotes a completed write-time build, so the index is in place BEFORE the
    /// snapshot becomes visible and no query is served by a full scan in between.
    ///
    /// Nothing is published for a failed or capped build, or for a file set that
    /// does not match the listing. The snapshot must still publish in that case:
    /// data availability never depends on this index.
    pub(crate) async fn publish_pending(&self, snapshot_id: &str, files: Vec<IndexedFile>) {
        let Some(builder) = self.pending.lock().take() else {
            return;
        };
        if builder.snapshot_id() != snapshot_id {
            return;
        }
        // Sorting and compressing the index is CPU work that runs for seconds on a
        // large table, so it goes to the blocking pool rather than the runtime.
        let finished = match tokio::task::spawn_blocking(move || builder.finish(&files)).await {
            Ok(finished) => finished,
            Err(error) => Err(format!("index build task failed: {error}")),
        };
        match finished {
            Ok(Some(index)) => self.publish(
                index,
                "Published write-time Cayenne point-lookup index with its snapshot",
            ),
            Ok(None) => {
                tracing::warn!(
                    table = %self.table_name,
                    snapshot_id = %snapshot_id,
                    max_bytes = self.max_bytes(),
                    "Write-time point-lookup index exceeded its byte cap; not published"
                );
            }
            Err(error) => {
                tracing::warn!(
                    table = %self.table_name,
                    snapshot_id = %snapshot_id,
                    %error,
                    "Write-time point-lookup index not published"
                );
            }
        }
    }

    /// Records a finished index's cost and makes it the one probes read.
    fn publish(&self, index: SnapshotLookupIndex, message: &'static str) {
        let stats = &index.stats;
        let dimensions = [telemetry::KeyValue::new("table", self.table_name.clone())];
        telemetry::cayenne::track_lookup_index_build_duration(stats.duration, &dimensions);
        telemetry::cayenne::track_lookup_index_bytes(
            u64::try_from(stats.approx_bytes).unwrap_or(u64::MAX),
            &dimensions,
        );
        tracing::info!(
            table = %self.table_name,
            snapshot_id = %index.snapshot_id(),
            build_ms = stats.duration.as_millis(),
            files = stats.files,
            rows = stats.rows,
            distinct_keys = stats.distinct_keys,
            approx_bytes = stats.approx_bytes,
            rss_before = ?stats.rss_before,
            rss_after = ?stats.rss_after,
            per_key = ?stats.per_key_entries,
            "{message}"
        );
        self.account_bytes(stats.approx_bytes);
        self.index.store(Some(Arc::new(index)));
    }

    /// Drops a write-time build whose write did not commit.
    pub(crate) fn discard_pending(&self) {
        *self.pending.lock() = None;
    }

    /// The indexed key these filters fully pin to literals, if any. Used to
    /// attribute a probe to a lookup shape even when no index is published yet.
    fn matched_shape(&self, scalar_for: &dyn Fn(&str) -> Option<ScalarValue>) -> Option<&str> {
        self.specs
            .iter()
            .find(|spec| {
                spec.columns
                    .iter()
                    .all(|column| scalar_for(column).is_some())
            })
            .map(|spec| spec.label.as_str())
    }

    /// Resolves a candidate row selection for `scalar_for`. Returns `None`
    /// whenever the ordinary scan must be used; the caller records the final
    /// outcome once it has validated the selection against its own file list.
    ///
    /// `scalar_for` must only answer for predicates that compare the bare column
    /// with a value: see the module's note on column-side casts.
    pub(crate) fn probe(
        self: &Arc<Self>,
        scalar_for: &dyn Fn(&str) -> Option<ScalarValue>,
    ) -> Option<LookupSelection> {
        let Some(index) = self.published() else {
            // Only count a probe once the filters actually name an indexed key,
            // so ordinary analytical scans do not show up as index misses.
            if let Some(shape) = self.matched_shape(scalar_for) {
                self.record_probe(shape, ProbeOutcome::Unbuilt);
            }
            return None;
        };

        let hit = index.probe(scalar_for)?;
        Some(LookupSelection {
            state: Arc::clone(self),
            index,
            shape: hit.shape,
            per_file: hit.per_file,
            rows: hit.rows,
        })
    }

    fn record_probe(&self, shape: &str, outcome: ProbeOutcome) {
        self.counters.record(outcome);
        telemetry::cayenne::track_lookup_index_probe(&[
            telemetry::KeyValue::new("table", self.table_name.clone()),
            telemetry::KeyValue::new("shape", shape.to_string()),
            telemetry::KeyValue::new("outcome", outcome.as_str()),
        ]);
    }

    fn record_selection(&self, shape: &str, files: u64, rows: u64) {
        self.counters
            .candidate_files
            .fetch_add(files, Ordering::Relaxed);
        self.counters
            .candidate_rows
            .fetch_add(rows, Ordering::Relaxed);
        let dimensions = [
            telemetry::KeyValue::new("table", self.table_name.clone()),
            telemetry::KeyValue::new("shape", shape.to_string()),
        ];
        telemetry::cayenne::track_lookup_index_candidate_files(files, &dimensions);
        telemetry::cayenne::track_lookup_index_candidate_rows(rows, &dimensions);
        self.record_probe(shape, ProbeOutcome::Selected);
    }
}

/// Builds the index for `snapshot_id` in the background and publishes it only
/// once complete. Failures leave the table on the ordinary scan.
pub(crate) fn spawn_build(
    state: Arc<LookupIndexState>,
    snapshot_id: String,
    store: Arc<dyn ObjectStore>,
    files: Vec<IndexedFile>,
    schema: Arc<arrow_schema::Schema>,
) {
    tokio::spawn(async move {
        let table = state.table_name.clone();
        tracing::info!(
            table = %table,
            snapshot_id = %snapshot_id,
            files = files.len(),
            "Building Cayenne point-lookup index"
        );
        match build(&state, snapshot_id.clone(), &store, files, &schema).await {
            Ok(Some(index)) => state.publish(index, "Published Cayenne point-lookup index"),
            Ok(None) => {
                tracing::warn!(
                    table = %table,
                    snapshot_id = %snapshot_id,
                    max_bytes = state.max_bytes(),
                    "Cayenne point-lookup index exceeded its byte cap; not published"
                );
            }
            Err(error) => {
                tracing::warn!(
                    table = %table,
                    snapshot_id = %snapshot_id,
                    %error,
                    "Cayenne point-lookup index build failed; not published"
                );
            }
        }
        state.release_build();
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
    columns: [KeyColumn; 2],
    /// Compacted, null-free key column chunks and their packed postings, in
    /// arrival order.
    first: Vec<ArrayRef>,
    second: Vec<ArrayRef>,
    postings: Vec<u64>,
}

impl ShapeBuild {
    /// Appends the rows of `batch` whose key columns are both non-null.
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
        let column = |key: &KeyColumn| {
            let array = batch
                .column_by_name(&key.name)
                .ok_or_else(|| format!("{file_path}: column '{}' not in the batch", key.name))?;
            cast_to(array, &key.data_type).map_err(|e| format!("{file_path}: {}: {e}", key.name))
        };
        let first = column(&self.columns[0])?;
        let second = column(&self.columns[1])?;
        let num_rows = u32::try_from(batch.num_rows())
            .map_err(|_| format!("{file_path}: batch has more than u32::MAX rows"))?;

        // A NULL can never satisfy an equality predicate, so an incomplete key
        // is simply not indexed.
        let indices = if first.null_count() == 0 && second.null_count() == 0 {
            UInt32Array::from_iter_values(0..num_rows)
        } else {
            let keep = arrow::compute::and(
                &arrow::compute::is_not_null(first.as_ref()).map_err(|e| e.to_string())?,
                &arrow::compute::is_not_null(second.as_ref()).map_err(|e| e.to_string())?,
            )
            .map_err(|e| e.to_string())?;
            UInt32Array::from_iter_values(
                keep.values()
                    .set_indices()
                    .filter_map(|row| u32::try_from(row).ok()),
            )
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
        let first = arrow::compute::take(first.as_ref(), &indices, None)
            .map_err(|e| format!("{file_path}: {e}"))?;
        let second = arrow::compute::take(second.as_ref(), &indices, None)
            .map_err(|e| format!("{file_path}: {e}"))?;
        let retained = first.get_array_memory_size()
            + second.get_array_memory_size()
            + indices.len() * std::mem::size_of::<u64>();
        self.first.push(first);
        self.second.push(second);
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
            first,
            second,
            postings,
        } = self;
        let converter = key_converter(&columns)?;
        let concat = |chunks: Vec<ArrayRef>, column: &KeyColumn| -> Result<ArrayRef, String> {
            if chunks.is_empty() {
                return Ok(arrow::array::new_empty_array(&column.data_type));
            }
            let parts: Vec<&dyn Array> = chunks.iter().map(AsRef::as_ref).collect();
            arrow::compute::concat(&parts).map_err(|e| format!("{}: {e}", column.name))
        };
        let first = concat(first, &columns[0])?;
        let second = concat(second, &columns[1])?;
        let postings: ArrayRef = Arc::new(UInt64Array::from(postings));

        let order = arrow::compute::lexsort_to_indices(
            &[
                SortColumn {
                    values: Arc::clone(&first),
                    options: None,
                },
                SortColumn {
                    values: Arc::clone(&second),
                    options: None,
                },
                SortColumn {
                    values: Arc::clone(&postings),
                    options: None,
                },
            ],
            None,
        )
        .map_err(|e| format!("sort {label}: {e}"))?;

        let fields = [
            columns[0].indexed_field(),
            columns[1].indexed_field(),
            postings_field(),
        ];
        let mut ctx = session.create_execution_ctx();
        let mut heads = BlockHeads::new();
        let mut compressed: [Vec<vortex::array::ArrayRef>; 3] = Default::default();
        let len = first.len();
        let mut start = 0usize;
        while start < len {
            let rows = chunk_rows.min(len - start);
            let indices = order.slice(start, rows);
            let sorted = [&first, &second, &postings].map(|array| {
                arrow::compute::take(array.as_ref(), &indices, None)
                    .map_err(|e| format!("sort {label}: {e}"))
            });
            let [first_chunk, second_chunk, postings_chunk] = sorted;
            let chunk = [first_chunk?, second_chunk?, postings_chunk?];
            heads
                .extend(&converter, &chunk[0], &chunk[1])
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
        drop((order, first, second, postings));

        let [first_chunks, second_chunks, posting_chunks] = compressed;
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
        let (heads, head_offsets, distinct_keys) = heads.finish();
        Ok(ShapeIndex {
            label,
            converter,
            first: assemble(first_chunks, &fields[0])?,
            second: assemble(second_chunks, &fields[1])?,
            postings: assemble(posting_chunks, &fields[2])?,
            columns,
            len,
            heads,
            head_offsets,
            distinct_keys,
        })
    }
}

/// The row-encoded key at the start of every block of sorted `(first, second)`
/// entries, and the number of distinct keys, fed one sorted chunk at a time.
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

    /// Records the next sorted entries, which start on a block boundary.
    fn extend(
        &mut self,
        converter: &RowConverter,
        first: &ArrayRef,
        second: &ArrayRef,
    ) -> Result<(), String> {
        let len = first.len();
        let mut start = 0usize;
        while start < len {
            let block = BLOCK_ROWS.min(len - start);
            let rows = converter
                .convert_columns(&[first.slice(start, block), second.slice(start, block)])
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
    /// Bytes held by the accumulated entries.
    accumulated_bytes: usize,
    max_bytes: usize,
    /// Set once a byte count passes the cap; the index is then abandoned rather
    /// than published with silently-dropped postings.
    capped: bool,
    /// Sorted entries compressed together; [`COMPRESS_CHUNK_ROWS`] outside tests.
    chunk_rows: usize,
}

impl BuildState {
    fn new(
        specs: &[KeySpec],
        max_bytes: usize,
        schema: &arrow_schema::Schema,
    ) -> Result<Self, String> {
        let shapes = specs
            .iter()
            .map(|spec| {
                Ok(ShapeBuild {
                    label: spec.label.clone(),
                    columns: [
                        KeyColumn::resolve(schema, &spec.columns[0])?,
                        KeyColumn::resolve(schema, &spec.columns[1])?,
                    ],
                    first: Vec::new(),
                    second: Vec::new(),
                    postings: Vec::new(),
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(Self {
            shapes,
            file_ids: HashMap::new(),
            file_order: Vec::new(),
            rows: 0,
            accumulated_bytes: 0,
            max_bytes,
            capped: false,
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
        if self.capped {
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
            self.accumulated_bytes = self.accumulated_bytes.saturating_add(retained);
        }
        self.rows += batch.num_rows() as u64;
        if self.accumulated_bytes > self.max_bytes {
            self.capped = true;
        }
        Ok(())
    }

    /// `Ok(None)` means the byte cap stopped the build.
    ///
    /// `files` is the snapshot's file set as the SCAN lists it. Requiring it to
    /// match the set the build actually saw is what makes a write-time index
    /// safe to publish: a file the build never observed would otherwise be
    /// served with no postings at all, which is a false empty rather than a
    /// fallback.
    fn into_index(
        self,
        snapshot_id: String,
        files: &[IndexedFile],
        started: Instant,
        rss_before: Option<u64>,
        session: VortexSession,
    ) -> Result<Option<SnapshotLookupIndex>, String> {
        let Self {
            shapes,
            file_ids,
            file_order,
            rows,
            max_bytes,
            capped,
            chunk_rows,
            ..
        } = self;
        if capped {
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
            let shape = shape.finish(&session, &compressor, chunk_rows)?;
            resident = resident.saturating_add(shape.resident_bytes());
            built.push(shape);
        }
        if resident > max_bytes {
            return Ok(None);
        }

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
                approx_bytes: resident,
                rss_before,
                rss_after: super::tuning::proc_self_rss_bytes(),
                per_key_entries,
            },
            shapes: built,
            session,
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
    state: &LookupIndexState,
    published: Arc<SnapshotLookupIndex>,
    store: &Arc<dyn ObjectStore>,
    files: Vec<IndexedFile>,
    schema: &arrow_schema::Schema,
) -> Result<LookupIndexVerification, String> {
    let snapshot_id = published.snapshot_id.clone();
    let read_back = build(state, snapshot_id, store, files, schema)
        .await?
        .ok_or_else(|| "read-back build exceeded the byte cap".to_string())?;
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
        max_bytes: usize,
        schema: &arrow_schema::Schema,
    ) -> Result<Self, String> {
        Ok(Self {
            table_name,
            snapshot_id,
            state: Mutex::new(Some(BuildState::new(specs, max_bytes, schema)?)),
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
    /// the scan lists it, which also supplies the sizes and modification times
    /// the scan-time snapshot check compares.
    fn finish(&self, files: &[IndexedFile]) -> Result<Option<SnapshotLookupIndex>, String> {
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
        if state.capped {
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
async fn build(
    state: &LookupIndexState,
    snapshot_id: String,
    store: &Arc<dyn ObjectStore>,
    files: Vec<IndexedFile>,
    schema: &arrow_schema::Schema,
) -> Result<Option<SnapshotLookupIndex>, String> {
    use vortex::expr::{get_item, pack, root};

    let started = Instant::now();
    let rss_before = super::tuning::proc_self_rss_bytes();
    let mut build = BuildState::new(&state.specs, state.max_bytes(), schema)?;
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
            if build.capped {
                return Ok(None);
            }
        }
    }

    // Sorting and compressing the index is CPU work that runs for seconds on a
    // large table, so it goes to the blocking pool rather than the runtime.
    match tokio::task::spawn_blocking(move || {
        build.into_index(snapshot_id, &files, started, rss_before, session)
    })
    .await
    {
        Ok(index) => index,
        Err(error) => Err(format!("index build task failed: {error}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};

    #[test]
    fn key_spec_requires_exactly_two_columns() {
        assert!(KeySpec::parse("TenantId").is_none());
        assert!(KeySpec::parse("A+B+C").is_none());
        let spec = KeySpec::parse(" TenantId + ServiceId ").expect("two columns");
        assert_eq!(
            spec.columns,
            ["TenantId".to_string(), "ServiceId".to_string()]
        );
        assert_eq!(spec.label, "TenantId+ServiceId");
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

    fn keyed_batch(tenants: Vec<Option<i64>>, services: Vec<Option<String>>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(arrow_schema::Schema::new(vec![
                Field::new("tenant", DataType::Int64, true),
                Field::new("service", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(tenants)),
                Arc::new(StringArray::from(services)),
            ],
        )
        .expect("batch")
    }

    /// Every key, including keys that straddle and coincide with block heads,
    /// resolves to exactly the addresses a brute-force map holds, and absent or
    /// NULL keys resolve to nothing.
    #[test]
    fn probes_match_a_brute_force_map_across_blocks_and_files() {
        let schema = arrow_schema::Schema::new(vec![
            Field::new("tenant", DataType::Int64, true),
            Field::new("service", DataType::Utf8, true),
        ]);
        let spec = KeySpec::parse("tenant+service").expect("spec");
        let mut build = BuildState::new(&[spec], usize::MAX, &schema)
            .expect("build state")
            .with_chunk_rows(BLOCK_ROWS * 2);

        let mut expected: HashMap<(i64, String), Vec<(String, u64)>> = HashMap::new();
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
                let mut tenants = Vec::new();
                let mut services = Vec::new();
                for _ in 0..(BLOCK_ROWS * 3 / 2) {
                    state = state
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(1_442_695_040_888_963_407);
                    // Few distinct keys, so most keys repeat across blocks and files.
                    let tenant = i64::try_from((state >> 33) % 7).expect("small");
                    let service = format!("s{}", (state >> 13) % 40);
                    let null = (state >> 7).is_multiple_of(23);
                    if !null {
                        expected
                            .entry((tenant, service.clone()))
                            .or_default()
                            .push((path.clone(), position));
                    }
                    tenants.push(if null { None } else { Some(tenant) });
                    services.push(Some(service));
                    position += 1;
                }
                let start = position - tenants.len() as u64;
                build
                    .ingest(
                        file_id,
                        RowPositions::Contiguous(start),
                        &keyed_batch(tenants, services),
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
            )
            .expect("finish")
            .expect("under the cap");
        assert!(
            index.shapes[0].blocks() > 4,
            "fixture must span many blocks"
        );
        assert_eq!(index.shapes[0].distinct_keys, expected.len());

        for ((tenant, service), addresses) in &expected {
            let hit = index
                .probe(&|column| match column {
                    "tenant" => Some(ScalarValue::Int64(Some(*tenant))),
                    "service" => Some(ScalarValue::Utf8(Some(service.clone()))),
                    _ => None,
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
            assert_eq!(found, wanted, "key ({tenant}, {service})");
        }

        let miss = index
            .probe(&|column| match column {
                "tenant" => Some(ScalarValue::Int64(Some(3))),
                "service" => Some(ScalarValue::Utf8(Some("absent".to_string()))),
                _ => None,
            })
            .expect("pinned key");
        assert!(miss.per_file.is_empty());
        let null = index
            .probe(&|column| match column {
                "tenant" => Some(ScalarValue::Int64(None)),
                "service" => Some(ScalarValue::Utf8(Some("s1".to_string()))),
                _ => None,
            })
            .expect("pinned key");
        assert!(null.per_file.is_empty());
    }

    /// A single shifted address is reported, so the read-back verification can
    /// actually fail.
    #[test]
    fn verification_reports_a_shifted_address() {
        let schema = arrow_schema::Schema::new(vec![
            Field::new("tenant", DataType::Int64, true),
            Field::new("service", DataType::Utf8, true),
        ]);
        let files = vec![IndexedFile {
            path: "snapshot/file.vortex".to_string(),
            size: 1,
            last_modified_ms: 0,
        }];
        let index_with = |shift: u64| {
            let spec = KeySpec::parse("tenant+service").expect("spec");
            let mut build = BuildState::new(&[spec], usize::MAX, &schema).expect("build state");
            let file_id = build.file_id(&files[0].path).expect("file id");
            build
                .ingest(
                    file_id,
                    RowPositions::Contiguous(shift),
                    &keyed_batch(
                        vec![Some(1), Some(2)],
                        vec![Some("a".to_string()), Some("b".to_string())],
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
                )
                .expect("finish")
                .expect("under the cap")
        };
        assert!(diff_indexes(&index_with(0), &index_with(0)).agrees());
        let report = diff_indexes(&index_with(1), &index_with(0));
        assert!(!report.agrees());
        assert_eq!(
            report.keys_per_shape,
            vec![("tenant+service".to_string(), 2, 2)]
        );
    }
}
