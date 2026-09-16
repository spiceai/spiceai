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
//! Footguns this code depends on:
//!
//! * Row positions are file-local physical positions in unfiltered scan order.
//!   They are only valid for the exact file they were captured from, which is
//!   why [`LookupSelection::validate`] compares path, size and modification time
//!   before a selection is attached.
//! * The index answers `key -> candidate positions` only. Every original
//!   predicate still runs, so a candidate that fails `Active = 1` is discarded
//!   by the scan's own filter rather than by the index.
//! * A selection of N row positions is not a promise of N decoded rows. Vortex
//!   reads whole encoded segments and dictionaries that cover those positions.

// `vortex::arrow::IntoArrowArray::into_arrow_preferred` is deprecated in favour of
// `execute_arrow(ctx)`; the delete path has the same pending migration. Use `expect`
// (not `allow`) so it resurfaces once that migration lands.
#![expect(deprecated)]

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use crate::row_converter::{RowConverter, SortField};
use arc_swap::ArcSwapOption;
use arrow::array::{Array, ArrayRef};
use arrow::record_batch::RecordBatch;
use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use futures::StreamExt;
use object_store::ObjectStore;
use vortex::VortexSessionDefault;
use vortex::arrow::IntoArrowArray;
use vortex::buffer::Buffer;
use vortex::file::OpenOptionsSessionExt;
use vortex_session::VortexSession;

/// Floor used when a table's derived budget would be smaller. Matches the PK
/// keyset's own floor, the structure this one is sized alongside.
const DEFAULT_BUDGET_FLOOR: usize = 256 * 1024 * 1024;

/// Bits reserved for the file-local row position inside a packed posting.
const POSITION_BITS: u32 = 40;
const POSITION_MASK: u64 = (1u64 << POSITION_BITS) - 1;

/// Why a probe did not attach a row selection. Doubles as the `outcome`
/// dimension on `cayenne_lookup_index_probe_total`.
pub(crate) mod outcome {
    pub(crate) const SELECTED: &str = "selected";
    pub(crate) const EMPTY: &str = "empty";
    pub(crate) const UNBUILT: &str = "unbuilt";
    pub(crate) const SNAPSHOT_MISMATCH: &str = "snapshot_mismatch";
    pub(crate) const DELETIONS: &str = "deletions";
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
    let mut states = STATES.lock().ok()?;
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

/// Row addresses for one composite key. Most keys have a single match, so the
/// first posting is stored inline.
struct Postings {
    first: u64,
    rest: Option<Box<PostingTail>>,
}

/// Overflow postings for a non-unique key. Boxed so `Postings` stays 16 bytes
/// wide for the single-match keys that dominate the table.
#[derive(Default)]
struct PostingTail(Vec<u64>);

impl Postings {
    fn new(packed: u64) -> Self {
        Self {
            first: packed,
            rest: None,
        }
    }

    fn push(&mut self, packed: u64) {
        self.rest.get_or_insert_with(Box::default).0.push(packed);
    }

    fn iter(&self) -> impl Iterator<Item = u64> + '_ {
        std::iter::once(self.first).chain(self.rest.iter().flat_map(|tail| tail.0.iter().copied()))
    }
}

/// One composite-key index over the snapshot.
struct KeyMap {
    spec: KeySpec,
    entries: HashMap<(u32, u32), Postings>,
    postings: usize,
}

impl KeyMap {
    /// Structural estimate, kept O(1) so the byte cap can be checked per batch:
    /// one hash-table slot per distinct key plus the spilled posting lists.
    fn approx_bytes(&self) -> usize {
        const SLOT_BYTES: usize = 32;
        const SPILLED_POSTING_BYTES: usize = 24;
        self.entries.len() * SLOT_BYTES
            + self.postings.saturating_sub(self.entries.len()) * SPILLED_POSTING_BYTES
    }
}

/// Interned distinct values of ONE key column.
///
/// Values are `RowConverter`-encoded, the same encoding the primary-key path
/// uses, so the index is type-general rather than string-only and inherits that
/// codec's null and ordering semantics. Interning is per COLUMN, not per row:
/// these keys are composites of columns with far fewer distinct values than
/// rows, so a row-wise encoding would re-store the shared column on every row.
///
/// Every array is cast to the column's stored type before encoding, so the same
/// value encodes identically whether it arrived from the write stream or from a
/// scan that decoded it as a view type.
struct ColumnDictionary {
    /// The column's stored type. Both the build and the probe cast to it, so an
    /// encoding never depends on how the value reached us.
    data_type: DataType,
    converter: RowConverter,
    ids: HashMap<Arc<[u8]>, u32>,
    /// `id -> encoded value`, so a key can be read back out of the index. Two
    /// builds intern in different orders, so comparing them needs the values.
    values: Vec<Arc<[u8]>>,
    value_bytes: usize,
}

impl ColumnDictionary {
    fn new(data_type: DataType) -> Result<Self, String> {
        let converter = RowConverter::new(vec![SortField::new(data_type.clone())])
            .map_err(|e| format!("row converter for {data_type}: {e}"))?;
        Ok(Self {
            data_type,
            converter,
            ids: HashMap::new(),
            values: Vec::new(),
            value_bytes: 0,
        })
    }

    /// Encodes a whole column, returning one id per row and `None` for nulls.
    ///
    /// A NULL can never satisfy an equality predicate, so null rows are not
    /// indexed at all rather than interned under an encoded-null key.
    fn intern_column(&mut self, array: &ArrayRef) -> Result<Vec<Option<u32>>, String> {
        let array = cast_to(array, &self.data_type)?;
        let rows = self
            .converter
            .convert_columns(std::slice::from_ref(&array))
            .map_err(|e| format!("encode column: {e}"))?;
        let mut ids = Vec::with_capacity(array.len());
        for row in 0..array.len() {
            if array.is_null(row) {
                ids.push(None);
                continue;
            }
            ids.push(Some(self.intern(rows.row(row).as_ref())?));
        }
        Ok(ids)
    }

    fn intern(&mut self, encoded: &[u8]) -> Result<u32, String> {
        if let Some(id) = self.ids.get(encoded) {
            return Ok(*id);
        }
        let id = u32::try_from(self.values.len())
            .map_err(|_| "column dictionary overflowed u32".to_string())?;
        let shared: Arc<[u8]> = Arc::from(encoded);
        self.value_bytes += encoded.len();
        self.ids.insert(Arc::clone(&shared), id);
        self.values.push(shared);
        Ok(id)
    }

    /// The id of a literal the query pinned, or `None` when this column holds no
    /// such value. The literal is cast to the column's stored type first, so
    /// `id = '5'` against an `Int64` column resolves rather than silently missing.
    fn lookup_scalar(&self, scalar: &ScalarValue) -> Option<u32> {
        let casted = scalar.cast_to(&self.data_type).ok()?;
        let array = casted.to_array_of_size(1).ok()?;
        if array.is_null(0) {
            return None;
        }
        let rows = self
            .converter
            .convert_columns(std::slice::from_ref(&array))
            .ok()?;
        self.ids.get(rows.row(0).as_ref()).copied()
    }

    fn value(&self, id: u32) -> Option<&[u8]> {
        self.values.get(id as usize).map(Arc::as_ref)
    }

    /// Resident bytes, computed from ALLOCATED capacity rather than occupancy.
    ///
    /// This figure is reserved against the `DataFusion` memory pool, so it has to
    /// bound what the allocator actually holds — a figure derived from `len()`
    /// understates a hash table by its whole load-factor headroom, and reserving
    /// too little is worse than not reserving at all.
    fn approx_bytes(&self) -> usize {
        const ARC_SLICE_PTR: usize = std::mem::size_of::<Arc<[u8]>>();
        // Arc<[u8]> allocation: two atomic counters plus the payload.
        const ARC_HEADER: usize = 2 * std::mem::size_of::<usize>();
        let table = self.ids.capacity() * (ARC_SLICE_PTR + std::mem::size_of::<u32>() + 1);
        let reverse = self.values.capacity() * ARC_SLICE_PTR;
        let payload = self.value_bytes + self.values.len() * ARC_HEADER;
        table + reverse + payload
    }
}

/// The per-column dictionaries backing one table's indexes, keyed by column name
/// so a column shared between two lookup shapes is interned once.
struct Dictionaries {
    columns: HashMap<String, ColumnDictionary>,
}

impl Dictionaries {
    fn new(columns: &[String], schema: &arrow_schema::Schema) -> Result<Self, String> {
        let mut out = HashMap::new();
        for column in columns {
            let field = schema
                .fields()
                .iter()
                .find(|f| f.name() == column || f.name().eq_ignore_ascii_case(column))
                .ok_or_else(|| format!("key column '{column}' is not in the table schema"))?;
            out.insert(
                column.clone(),
                ColumnDictionary::new(field.data_type().clone())?,
            );
        }
        Ok(Self { columns: out })
    }

    fn get(&self, column: &str) -> Option<&ColumnDictionary> {
        self.columns.get(column)
    }

    fn get_mut(&mut self, column: &str) -> Option<&mut ColumnDictionary> {
        self.columns.get_mut(column)
    }

    fn distinct_values(&self) -> usize {
        self.columns.values().map(|d| d.values.len()).sum()
    }

    fn approx_bytes(&self) -> usize {
        self.columns
            .values()
            .map(ColumnDictionary::approx_bytes)
            .sum()
    }
}

/// Casts `array` to `data_type`, or returns it unchanged when it already matches.
fn cast_to(array: &ArrayRef, data_type: &DataType) -> Result<ArrayRef, String> {
    if array.data_type() == data_type {
        return Ok(Arc::clone(array));
    }
    arrow::compute::cast(array, data_type)
        .map_err(|e| format!("cast {} -> {data_type}: {e}", array.data_type()))
}

/// Measured cost of one index build, reported separately from query execution.
#[derive(Clone, Debug)]
pub(crate) struct BuildStats {
    pub(crate) duration: Duration,
    pub(crate) files: usize,
    pub(crate) rows: u64,
    pub(crate) distinct_strings: usize,
    pub(crate) approx_bytes: usize,
    pub(crate) rss_before: Option<u64>,
    pub(crate) rss_after: Option<u64>,
    pub(crate) per_key_entries: Vec<(String, usize, usize)>,
}

/// An index over exactly one immutable snapshot file set.
pub(crate) struct SnapshotLookupIndex {
    snapshot_id: String,
    files: Vec<IndexedFile>,
    file_ids: HashMap<String, u32>,
    dictionaries: Dictionaries,
    maps: Vec<KeyMap>,
    stats: BuildStats,
}

impl SnapshotLookupIndex {
    pub(crate) fn snapshot_id(&self) -> &str {
        &self.snapshot_id
    }

    pub(crate) fn stats(&self) -> &BuildStats {
        &self.stats
    }

    /// Resolves `filters` against one indexed key, returning the candidate row
    /// addresses grouped by file path. `None` means no indexed key is fully
    /// pinned to literals by these filters.
    fn probe(&self, scalar_for: &dyn Fn(&str) -> Option<ScalarValue>) -> Option<ProbeHit> {
        for map in &self.maps {
            let Some(first) = scalar_for(&map.spec.columns[0]) else {
                continue;
            };
            let Some(second) = scalar_for(&map.spec.columns[1]) else {
                continue;
            };
            let (Some(first_dict), Some(second_dict)) = (
                self.dictionaries.get(&map.spec.columns[0]),
                self.dictionaries.get(&map.spec.columns[1]),
            ) else {
                continue;
            };

            let mut per_file: HashMap<String, Vec<u64>> = HashMap::new();
            let mut rows = 0usize;
            // A literal this column never held resolves to no id, which is a
            // complete miss rather than a reason to fall back.
            if let (Some(a), Some(b)) = (
                first_dict.lookup_scalar(&first),
                second_dict.lookup_scalar(&second),
            ) && let Some(postings) = map.entries.get(&(a, b))
            {
                for packed in postings.iter() {
                    let file_id = (packed >> POSITION_BITS) as usize;
                    let position = packed & POSITION_MASK;
                    let Some(file) = self.files.get(file_id) else {
                        // A posting that cannot be resolved to a file means the
                        // index is not internally consistent; refuse the probe
                        // rather than returning a partial selection.
                        return None;
                    };
                    per_file
                        .entry(file.path.clone())
                        .or_default()
                        .push(position);
                    rows += 1;
                }
            }

            for positions in per_file.values_mut() {
                positions.sort_unstable();
                positions.dedup();
            }

            return Some(ProbeHit {
                shape: map.spec.label.clone(),
                per_file,
                rows,
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
/// list. Nothing is applied until [`Self::validate`] accepts that file list.
pub(crate) struct LookupSelection {
    state: Arc<LookupIndexState>,
    index: Arc<SnapshotLookupIndex>,
    shape: String,
    per_file: HashMap<String, Vec<u64>>,
    rows: usize,
}

impl LookupSelection {
    /// Records the outcome of a probe that did NOT end in an attached
    /// selection, so a run that silently fell back is visible in the metrics.
    pub(crate) fn record_outcome(&self, outcome: &str) {
        self.state.record_probe(&self.shape, outcome);
    }

    /// Records an attached selection and the work it left for the scan.
    pub(crate) fn record_selected(&self, files: u64, rows: u64) {
        self.state.record_selection(&self.shape, files, rows);
    }

    pub(crate) fn candidate_rows(&self) -> usize {
        self.rows
    }

    /// Accepts this selection only for the exact snapshot and files it was built
    /// from. `files` is the scan's own (already pruned) list: a file pruned away
    /// by statistics provably holds no matching row, so its absence is fine,
    /// while a file the index has never seen means the snapshot moved under us.
    pub(crate) fn validate<'a>(
        &self,
        snapshot_id: &str,
        files: impl Iterator<Item = &'a datafusion_datasource::PartitionedFile>,
    ) -> bool {
        if self.index.snapshot_id != snapshot_id {
            return false;
        }
        for file in files {
            let path = file.object_meta.location.to_string();
            let Some(&id) = self.index.file_ids.get(&path) else {
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

    /// Sorted file-local row positions to read from `path`, or `None` when the
    /// file holds no candidate and should be dropped from the scan.
    pub(crate) fn positions_for(&self, path: &str) -> Option<&[u64]> {
        self.per_file.get(path).map(Vec::as_slice)
    }
}

/// Per-file row selections handed to the Vortex scan.
pub(crate) struct LookupAccessPlanProvider {
    state: Arc<LookupIndexState>,
    selections: HashMap<String, Buffer<u64>>,
}

impl std::fmt::Debug for LookupAccessPlanProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LookupAccessPlanProvider")
            .field("files", &self.selections.len())
            .finish_non_exhaustive()
    }
}

impl LookupAccessPlanProvider {
    pub(crate) fn new(selection: &LookupSelection) -> Self {
        let selections = selection
            .per_file
            .iter()
            .map(|(path, positions)| (path.clone(), Buffer::copy_from(positions.as_slice())))
            .collect();
        Self {
            state: Arc::clone(&selection.state),
            selections,
        }
    }
}

impl vortex_datafusion::VortexAccessPlanProvider for LookupAccessPlanProvider {
    fn access_plan_for_file(
        &self,
        file: &datafusion_datasource::PartitionedFile,
    ) -> Option<Arc<vortex_datafusion::VortexAccessPlan>> {
        let path = file.object_meta.location.to_string();
        let positions = self.selections.get(&path)?;
        self.state
            .counters
            .access_plans_attached
            .fetch_add(1, Ordering::Relaxed);
        Some(Arc::new(
            vortex_datafusion::VortexAccessPlan::default().with_selection(
                vortex_scan::selection::Selection::IncludeByIndex(positions.clone()),
            ),
        ))
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
    /// Probes refused because position deletes also need the file's access plan.
    pub deletions: u64,
    /// Candidate files summed over selected probes.
    pub candidate_files: u64,
    /// Candidate row positions summed over selected probes.
    pub candidate_rows: u64,
    /// Files that were actually handed a Vortex row selection.
    pub access_plans_attached: u64,
    /// Estimated resident bytes of the published index, as reserved against the
    /// table's `DataFusion` memory pool. Zero when nothing is published.
    pub index_bytes: u64,
}

#[derive(Default)]
struct Counters {
    selected: AtomicU64,
    empty: AtomicU64,
    unbuilt: AtomicU64,
    snapshot_mismatch: AtomicU64,
    deletions: AtomicU64,
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
            deletions: self.deletions.load(Ordering::Relaxed),
            candidate_files: self.candidate_files.load(Ordering::Relaxed),
            candidate_rows: self.candidate_rows.load(Ordering::Relaxed),
            access_plans_attached: self.access_plans_attached.load(Ordering::Relaxed),
            index_bytes: self.index_bytes.load(Ordering::Relaxed),
        }
    }

    fn record(&self, outcome: &str) {
        let counter = match outcome {
            outcome::SELECTED => &self.selected,
            outcome::EMPTY => &self.empty,
            outcome::UNBUILT => &self.unbuilt,
            outcome::SNAPSHOT_MISMATCH => &self.snapshot_mismatch,
            outcome::DELETIONS => &self.deletions,
            _ => return,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

/// Probe accounting for `table_name`, or `None` when the table has no index
/// state (the table configures no lookup index).
#[must_use]
pub fn counters_for_table(table_name: &str) -> Option<LookupIndexCounters> {
    let states = STATES.lock().ok()?;
    states
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
    /// Cap on the index's estimated resident bytes. Seeded from the table's
    /// Cayenne memory configuration and overridden by the environment.
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
    /// Columns the build must read: the union of every indexed key's columns.
    pub(crate) fn key_columns(&self) -> Vec<String> {
        let mut columns: Vec<String> = Vec::new();
        for spec in &self.specs {
            for column in &spec.columns {
                if !columns.iter().any(|c| c == column) {
                    columns.push(column.clone());
                }
            }
        }
        columns
    }

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

    fn release_build(&self) {
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
        if let Ok(mut slot) = self.account.lock()
            && slot.is_none()
        {
            *slot = Some(Arc::clone(account));
        }
    }

    fn max_bytes(&self) -> usize {
        self.max_bytes.load(Ordering::Relaxed)
    }

    /// Publishes the index's resident bytes into the table's pool reservation.
    ///
    /// The estimate is computed from ALLOCATED capacity, not occupancy: it is
    /// what the pool plans against, and a figure derived from occupancy
    /// understates every hash table by its load-factor headroom.
    fn account_bytes(&self, bytes: usize) {
        self.counters
            .index_bytes
            .store(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::Relaxed);
        if let Ok(slot) = self.account.lock()
            && let Some(account) = slot.as_ref()
        {
            account.set_lookup_index_bytes(bytes);
        }
    }

    /// Releases a build slot claimed by a caller that could not start the build.
    pub(crate) fn abandon_build(&self) {
        self.release_build();
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
        let mut pending = self.pending.lock().ok()?;
        *pending = Some(Arc::clone(&builder));
        Some(builder)
    }

    /// Promotes a completed write-time build, so the index is in place BEFORE the
    /// snapshot becomes visible and no query is served by a full scan in between.
    ///
    /// Returns `false` when nothing was published — a failed or capped build, or
    /// a file set that does not match the listing. The snapshot must still
    /// publish in that case: data availability never depends on this index.
    pub(crate) fn publish_pending(&self, snapshot_id: &str, files: &[IndexedFile]) -> bool {
        let Ok(mut slot) = self.pending.lock() else {
            return false;
        };
        let Some(builder) = slot.take() else {
            return false;
        };
        drop(slot);
        if builder.snapshot_id() != snapshot_id {
            return false;
        }
        match builder.finish(files) {
            Ok(Some(index)) => {
                let stats = index.stats().clone();
                let dimensions = [telemetry::KeyValue::new("table", self.table_name.clone())];
                telemetry::cayenne::track_lookup_index_build_duration(stats.duration, &dimensions);
                telemetry::cayenne::track_lookup_index_bytes(
                    u64::try_from(stats.approx_bytes).unwrap_or(u64::MAX),
                    &dimensions,
                );
                tracing::info!(
                    table = %self.table_name,
                    snapshot_id = %snapshot_id,
                    build_ms = stats.duration.as_millis(),
                    files = stats.files,
                    rows = stats.rows,
                    distinct_strings = stats.distinct_strings,
                    approx_bytes = stats.approx_bytes,
                    rss_before = ?stats.rss_before,
                    rss_after = ?stats.rss_after,
                    per_key = ?stats.per_key_entries,
                    "Published write-time Cayenne point-lookup index with its snapshot"
                );
                self.account_bytes(stats.approx_bytes);
                self.index.store(Some(Arc::new(index)));
                true
            }
            Ok(None) => {
                tracing::warn!(
                    table = %self.table_name,
                    snapshot_id = %snapshot_id,
                    max_bytes = self.max_bytes(),
                    "Write-time point-lookup index exceeded its byte cap; not published"
                );
                false
            }
            Err(error) => {
                tracing::warn!(
                    table = %self.table_name,
                    snapshot_id = %snapshot_id,
                    %error,
                    "Write-time point-lookup index not published"
                );
                false
            }
        }
    }

    /// Drops a write-time build whose write did not commit.
    pub(crate) fn discard_pending(&self) {
        if let Ok(mut slot) = self.pending.lock() {
            *slot = None;
        }
    }

    pub(crate) fn published_for(&self, snapshot_id: &str) -> Option<Arc<SnapshotLookupIndex>> {
        self.published()
            .filter(|index| index.snapshot_id == snapshot_id)
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
    pub(crate) fn probe(
        self: &Arc<Self>,
        scalar_for: &dyn Fn(&str) -> Option<ScalarValue>,
    ) -> Option<LookupSelection> {
        let Some(index) = self.published() else {
            // Only count a probe once the filters actually name an indexed key,
            // so ordinary analytical scans do not show up as index misses.
            if let Some(shape) = self.matched_shape(scalar_for) {
                self.record_probe(shape, outcome::UNBUILT);
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

    pub(crate) fn record_probe(&self, shape: &str, outcome: &str) {
        self.counters.record(outcome);
        telemetry::cayenne::track_lookup_index_probe(&[
            telemetry::KeyValue::new("table", self.table_name.clone()),
            telemetry::KeyValue::new("shape", shape.to_string()),
            telemetry::KeyValue::new("outcome", outcome.to_string()),
        ]);
    }

    pub(crate) fn record_selection(&self, shape: &str, files: u64, rows: u64) {
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
        self.record_probe(shape, outcome::SELECTED);
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
            Ok(Some(index)) => {
                let build_stats = index.stats().clone();
                let dimensions = [telemetry::KeyValue::new("table", table.clone())];
                telemetry::cayenne::track_lookup_index_build_duration(
                    build_stats.duration,
                    &dimensions,
                );
                telemetry::cayenne::track_lookup_index_bytes(
                    u64::try_from(build_stats.approx_bytes).unwrap_or(u64::MAX),
                    &dimensions,
                );
                tracing::info!(
                    table = %table,
                    snapshot_id = %snapshot_id,
                    build_ms = build_stats.duration.as_millis(),
                    files = build_stats.files,
                    rows = build_stats.rows,
                    distinct_strings = build_stats.distinct_strings,
                    approx_bytes = build_stats.approx_bytes,
                    rss_before = ?build_stats.rss_before,
                    rss_after = ?build_stats.rss_after,
                    per_key = ?build_stats.per_key_entries,
                    "Published Cayenne point-lookup index"
                );
                state.account_bytes(build_stats.approx_bytes);
                state.index.store(Some(Arc::new(index)));
            }
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

/// `Ok(None)` means the byte cap stopped the build.
/// Accumulates postings for one snapshot.
///
/// Shared by both builders — the read-back build that scans finished files and
/// the write-time build fed by the Vortex sink — so the two cannot drift apart
/// in how they intern keys, pack postings or account for memory.
struct BuildState {
    dictionaries: Dictionaries,
    maps: Vec<KeyMap>,
    file_ids: HashMap<String, u32>,
    /// `file_id -> path`, in assignment order.
    file_order: Vec<String>,
    rows: u64,
    max_bytes: usize,
    /// Set once the estimate passes the cap; the index is then abandoned rather
    /// than published with silently-dropped postings.
    capped: bool,
}

impl BuildState {
    fn new(
        specs: &[KeySpec],
        max_bytes: usize,
        schema: &arrow_schema::Schema,
    ) -> Result<Self, String> {
        let mut columns: Vec<String> = Vec::new();
        for spec in specs {
            for column in &spec.columns {
                if !columns.iter().any(|c| c == column) {
                    columns.push(column.clone());
                }
            }
        }
        Ok(Self {
            dictionaries: Dictionaries::new(&columns, schema)?,
            maps: specs
                .iter()
                .map(|spec| KeyMap {
                    spec: spec.clone(),
                    entries: HashMap::new(),
                    postings: 0,
                })
                .collect(),
            file_ids: HashMap::new(),
            file_order: Vec::new(),
            rows: 0,
            max_bytes,
            capped: false,
        })
    }

    /// Ids are assigned on first sight, so the write-time builder does not need
    /// to know the file set in advance.
    fn file_id(&mut self, path: &str) -> Result<u32, String> {
        if let Some(id) = self.file_ids.get(path) {
            return Ok(*id);
        }
        let id = u32::try_from(self.file_order.len()).map_err(|_| "too many files".to_string())?;
        self.file_ids.insert(path.to_string(), id);
        self.file_order.push(path.to_string());
        Ok(id)
    }

    /// Columns the build has to read: the union of every indexed key's columns.
    fn key_columns(&self) -> Vec<String> {
        let mut columns: Vec<String> = Vec::new();
        for map in &self.maps {
            for column in &map.spec.columns {
                if !columns.iter().any(|c| c == column) {
                    columns.push(column.clone());
                }
            }
        }
        columns
    }

    fn ingest(
        &mut self,
        file_id: u32,
        start_position: u64,
        batch: &RecordBatch,
        file_path: &str,
    ) -> Result<(), String> {
        if self.capped {
            return Ok(());
        }
        index_batch(
            batch,
            file_id,
            start_position,
            &mut self.dictionaries,
            &mut self.maps,
            file_path,
        )?;
        self.rows += batch.num_rows() as u64;
        if approx_bytes(&self.dictionaries, &self.maps) > self.max_bytes {
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
    ) -> Result<Option<SnapshotLookupIndex>, String> {
        if self.capped {
            return Ok(None);
        }
        let listed: HashSet<&str> = files.iter().map(|f| f.path.as_str()).collect();
        let observed: HashSet<&str> = self.file_order.iter().map(String::as_str).collect();
        if listed != observed {
            return Err(format!(
                "indexed file set does not match the snapshot listing ({} observed, {} listed)",
                observed.len(),
                listed.len()
            ));
        }
        // Re-key the postings' file ids onto the listing's order so `files[id]`
        // resolves, whatever order the build happened to see the files in.
        let mut files_by_id: Vec<IndexedFile> = Vec::with_capacity(self.file_order.len());
        for path in &self.file_order {
            let file = files
                .iter()
                .find(|f| &f.path == path)
                .ok_or_else(|| format!("listing lost {path}"))?;
            files_by_id.push(file.clone());
        }
        let file_ids = self.file_ids;
        let approx = approx_bytes(&self.dictionaries, &self.maps);
        let per_key_entries = self
            .maps
            .iter()
            .map(|map| (map.spec.label.clone(), map.entries.len(), map.postings))
            .collect();
        let file_count = files_by_id.len();

        Ok(Some(SnapshotLookupIndex {
            snapshot_id,
            files: files_by_id,
            file_ids,
            stats: BuildStats {
                duration: started.elapsed(),
                files: file_count,
                rows: self.rows,
                distinct_strings: self.dictionaries.distinct_values(),
                approx_bytes: approx,
                rss_before,
                rss_after: resident_bytes(),
                per_key_entries,
            },
            dictionaries: self.dictionaries,
            maps: self.maps,
        }))
    }
}

/// Result of diffing a write-time index against a read-back build of the same
/// snapshot.
///
/// The write-time index trusts that the writer appends batches in arrival order,
/// so its positions are only as good as that invariant. Diffing it against an
/// index built by actually scanning the finished files is what turns that from
/// an assumption into a check.
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
    /// Keys whose postings were compared value-for-value.
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

/// Compares two indexes over the same snapshot, key for key and address for
/// address. Ids are build-order dependent, so the comparison goes through the
/// interned strings and the resolved file paths.
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
    if write_time.maps.len() != read_back.maps.len() {
        report
            .mismatches
            .push("different key-shape counts".to_string());
        return report;
    }

    for (write_map, read_map) in write_time.maps.iter().zip(&read_back.maps) {
        report.keys_per_shape.push((
            write_map.spec.label.clone(),
            write_map.entries.len(),
            read_map.entries.len(),
        ));
        report.postings_per_shape.push((
            write_map.spec.label.clone(),
            write_map.postings,
            read_map.postings,
        ));
        if write_map.entries.len() != read_map.entries.len() {
            report.mismatches.push(format!(
                "{}: {} keys write-time vs {} read-back",
                write_map.spec.label,
                write_map.entries.len(),
                read_map.entries.len()
            ));
        }

        let (Some(read_first), Some(read_second), Some(write_first), Some(write_second)) = (
            read_back.dictionaries.get(&read_map.spec.columns[0]),
            read_back.dictionaries.get(&read_map.spec.columns[1]),
            write_time.dictionaries.get(&write_map.spec.columns[0]),
            write_time.dictionaries.get(&write_map.spec.columns[1]),
        ) else {
            report
                .mismatches
                .push(format!("{}: missing a dictionary", write_map.spec.label));
            continue;
        };

        for (&(a, b), read_postings) in &read_map.entries {
            let (Some(first), Some(second)) = (read_first.value(a), read_second.value(b)) else {
                report
                    .mismatches
                    .push(format!("{}: unresolvable key ids", write_map.spec.label));
                continue;
            };
            report.keys_compared += 1;
            let expected = resolve_postings(read_back, read_postings);
            let found = write_first
                .ids
                .get(first)
                .copied()
                .zip(write_second.ids.get(second).copied())
                .and_then(|(x, y)| write_map.entries.get(&(x, y)))
                .map(|postings| resolve_postings(write_time, postings));
            match found {
                Some(found) if found == expected => {}
                Some(found) => report.mismatches.push(format!(
                    "{} [{first:?}, {second:?}]: write-time {found:?} vs read-back {expected:?}",
                    write_map.spec.label
                )),
                None => report.mismatches.push(format!(
                    "{} [{first:?}, {second:?}]: missing from the write-time index",
                    write_map.spec.label
                )),
            }
        }
    }
    report
}

/// Postings as sorted `(file path, file-local position)` pairs, so two builds
/// that assigned different file ids still compare equal.
fn resolve_postings(index: &SnapshotLookupIndex, postings: &Postings) -> Vec<(String, u64)> {
    let mut out: Vec<(String, u64)> = postings
        .iter()
        .map(|packed| {
            let file_id = (packed >> POSITION_BITS) as usize;
            let path = index
                .files
                .get(file_id)
                .map_or_else(|| format!("<unknown file {file_id}>"), |f| f.path.clone());
            (path, packed & POSITION_MASK)
        })
        .collect();
    out.sort();
    out
}

/// Diffs the published index against a fresh read-back build of the same
/// snapshot's files.
pub(crate) async fn verify_against_read_back(
    state: &LookupIndexState,
    store: &Arc<dyn ObjectStore>,
    files: Vec<IndexedFile>,
    schema: &arrow_schema::Schema,
) -> Result<LookupIndexVerification, String> {
    let published = state
        .published()
        .ok_or_else(|| "no published index to verify".to_string())?;
    let snapshot_id = published.snapshot_id.clone();
    let read_back = build(state, snapshot_id, store, files, schema)
        .await?
        .ok_or_else(|| "read-back build exceeded the byte cap".to_string())?;
    Ok(diff_indexes(&published, &read_back))
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
            rss_before: resident_bytes(),
        })
    }

    pub(crate) fn snapshot_id(&self) -> &str {
        &self.snapshot_id
    }

    fn record_failure(&self, message: String) {
        if let Ok(mut failure) = self.failure.lock()
            && failure.is_none()
        {
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
        if let Some(failure) = self.failure.lock().map_err(|e| e.to_string())?.clone() {
            return Err(failure);
        }
        let state = self
            .state
            .lock()
            .map_err(|e| e.to_string())?
            .take()
            .ok_or_else(|| "index accumulator already consumed".to_string())?;
        state.into_index(
            self.snapshot_id.clone(),
            files,
            self.started,
            self.rss_before,
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
        let path = file_path.to_string();
        let Ok(mut slot) = self.state.lock() else {
            self.record_failure("index accumulator lock poisoned".to_string());
            return;
        };
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
        let file_id = match state.file_id(&path) {
            Ok(id) => id,
            Err(e) => {
                drop(slot);
                self.record_failure(e);
                return;
            }
        };
        if let Err(e) = state.ingest(file_id, first_row_position, batch, &path) {
            drop(slot);
            self.record_failure(e);
        }
    }
}

async fn build(
    state: &LookupIndexState,
    snapshot_id: String,
    store: &Arc<dyn ObjectStore>,
    files: Vec<IndexedFile>,
    schema: &arrow_schema::Schema,
) -> Result<Option<SnapshotLookupIndex>, String> {
    let started = Instant::now();
    let rss_before = resident_bytes();
    let mut build = BuildState::new(&state.specs, state.max_bytes(), schema)?;
    let columns = build.key_columns();
    let session = VortexSession::default();

    for file in &files {
        let file_id = build.file_id(&file.path)?;

        let vxf = session
            .open_options()
            .open_object_store(store, &file.path)
            .await
            .map_err(|e| format!("open {}: {e}", file.path))?;

        let mut scan_builder = vxf.scan().map_err(|e| format!("scan {}: {e}", file.path))?;
        {
            use vortex::expr::{root, select};
            let projected: Vec<&str> = columns.iter().map(String::as_str).collect();
            scan_builder = scan_builder.with_projection(select(projected, root()));
        }

        let mut stream = scan_builder
            .into_stream()
            .map_err(|e| format!("stream {}: {e}", file.path))?;

        // An unfiltered ordered scan yields rows in physical order, so a manual
        // counter is the file-local position — the same convention the write-time
        // builder gets straight from the sink.
        let mut position: u64 = 0;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| format!("read {}: {e}", file.path))?;
            let array = chunk
                .into_arrow_preferred()
                .map_err(|e| format!("to arrow {}: {e}", file.path))?;
            if array.is_empty() {
                continue;
            }
            let struct_array = array
                .as_any()
                .downcast_ref::<arrow::array::StructArray>()
                .ok_or_else(|| format!("{}: scan did not return a StructArray", file.path))?;
            let batch = RecordBatch::from(struct_array);

            build.ingest(file_id, position, &batch, &file.path)?;
            position += batch.num_rows() as u64;
            if build.capped {
                return Ok(None);
            }
        }
    }

    build.into_index(snapshot_id, &files, started, rss_before)
}

fn index_batch(
    batch: &RecordBatch,
    file_id: u32,
    start: u64,
    dictionaries: &mut Dictionaries,
    maps: &mut [KeyMap],
    file_path: &str,
) -> Result<(), String> {
    let num_rows = batch.num_rows();

    // Interned ids for every indexed column, resolved once per batch.
    let mut per_column: HashMap<String, Vec<Option<u32>>> = HashMap::new();
    for map in maps.iter() {
        for column in &map.spec.columns {
            if per_column.contains_key(column) {
                continue;
            }
            let index = batch
                .schema()
                .column_with_name(column)
                .map(|(index, _)| index)
                .or_else(|| {
                    batch
                        .schema()
                        .fields()
                        .iter()
                        .position(|f| f.name().eq_ignore_ascii_case(column))
                })
                .ok_or_else(|| format!("{file_path}: column '{column}' not in the scan output"))?;
            let ids = dictionaries
                .get_mut(column)
                .ok_or_else(|| format!("no dictionary for key column '{column}'"))?
                .intern_column(batch.column(index))
                .map_err(|e| format!("{file_path}: {column}: {e}"))?;
            per_column.insert(column.clone(), ids);
        }
    }

    for map in maps.iter_mut() {
        let first = &per_column[&map.spec.columns[0]];
        let second = &per_column[&map.spec.columns[1]];
        for row in 0..num_rows {
            // A NULL can never satisfy an equality predicate, so an incomplete
            // key is simply not indexed.
            let (Some(a), Some(b)) = (first[row], second[row]) else {
                continue;
            };
            let packed = (u64::from(file_id) << POSITION_BITS) | (start + row as u64);
            match map.entries.entry((a, b)) {
                Entry::Occupied(mut occupied) => occupied.get_mut().push(packed),
                Entry::Vacant(vacant) => {
                    vacant.insert(Postings::new(packed));
                }
            }
            map.postings += 1;
        }
    }

    Ok(())
}

/// Structural estimate of the resident index, not a measured allocator
/// footprint: key bytes, one hash-table slot per entry, and posting lists.
fn approx_bytes(dictionaries: &Dictionaries, maps: &[KeyMap]) -> usize {
    dictionaries.approx_bytes() + maps.iter().map(KeyMap::approx_bytes).sum::<usize>()
}

/// Process resident set size, when the platform exposes it cheaply.
fn resident_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
        let pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
        Some(pages * 4096)
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn postings_keep_every_duplicate_key_match() {
        let mut postings = Postings::new(1);
        postings.push(7);
        postings.push(9);
        assert_eq!(postings.iter().collect::<Vec<_>>(), vec![1, 7, 9]);
    }

    #[test]
    fn packed_postings_round_trip_file_and_position() {
        let packed = (u64::from(3u32) << POSITION_BITS) | 0x000c_1fff;
        assert_eq!(packed >> POSITION_BITS, 3);
        assert_eq!(packed & POSITION_MASK, 0x000c_1fff);
    }
}
