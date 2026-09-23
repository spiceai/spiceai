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

//! Per-dataset metastore snapshot serialization.
//!
//! The legacy Cayenne snapshot format archived the entire `cayenne.db` `SQLite`
//! file. That approach forced a one-dataset-per-metadata-directory limitation
//! (multiple datasets sharing a metastore would clobber each other on extract)
//! and made snapshots non-portable across nodes whose data directories did not
//! match the writer's absolute paths.
//!
//! This module replaces that with a portable, per-dataset metastore "slice":
//!
//! * **Export**: `export_dataset(metastore, dataset, anchor)` collects every
//!   metastore row that belongs to `dataset` (the `cayenne_table` row keyed
//!   by `table_name`, plus all rows in dependent tables that reference that
//!   `table_id`) and emits a versioned JSON document. Path columns are
//!   rewritten to be relative to `anchor` so the slice does not embed
//!   filesystem-specific paths.
//!
//! * **Import**: `import_dataset(metastore, slice, anchor)` atomically
//!   replaces any local rows for the same `table_name` with the slice's
//!   contents inside a single `BEGIN IMMEDIATE` transaction. Path columns
//!   are rewritten back to absolute form anchored at the local `anchor`.
//!   FK `ON DELETE CASCADE` removes the dataset's prior dependent rows when
//!   the existing `cayenne_table` row is deleted.
//!
//! The slice format is **versioned** (`format_version: 1`) so future
//! changes can be detected and rejected with a clear error.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::{Deserialize, Serialize};

use super::{
    EXPECTED_TABLES, ExecuteParams, MetastoreBackend, MetastoreValue, QueryParams, QueryRowParams,
};
use crate::catalog::{CatalogError, CatalogResult};

/// The version an unpartitioned dataset's slice is written at, and the lowest
/// this build reads. Unchanged since the format existed, so a reader that
/// predates partition children still restores every slice it could before.
pub const SLICE_FORMAT_VERSION: u32 = 1;

/// The version a slice is written at when it carries rows a
/// [`SLICE_FORMAT_VERSION`] import does **not** clear before inserting, and the
/// highest this build reads.
///
/// That import clears the parent `cayenne_table` row — whose `ON DELETE
/// CASCADE` reaches every dependent table that keys `table_id` as text — plus
/// `cayenne_insert_record` by hand. Everything else the slice may now carry
/// falls outside both: a partitioned dataset's child `cayenne_table` rows, and
/// the [`blob_keyed_tables`][super::blob_keyed_tables] other than
/// `cayenne_insert_record`, which have no foreign key back to the parent.
///
/// Such a reader restores the payload correctly the *first* time — it inserts
/// every row the slice carries — and then cannot restore it again, because its
/// own cleanup never reaches those rows and the next insert collides with them.
/// `CayenneSnapshotEngine` imports into the node's live catalog, and a
/// `refresh_mode: snapshot` replica restores repeatedly, so that is not a
/// one-off: the replica is stranded on whichever snapshot it loaded first.
/// Measured by running `trunk`'s own `import_dataset` twice against one
/// `SqliteMetastore` with slices this build's exporter produced:
///
/// ```text
/// partitioned:  #1 -> Ok   #2 -> Err(UNIQUE constraint failed: cayenne_table.table_name)
/// write-backs:  #1 -> Ok   #2 -> Err(UNIQUE constraint failed: cayenne_pending_write_back.table_id, …pk_bytes)
/// ```
///
/// Refusing the payload outright is the legible form of the same outcome: the
/// reader says the archive needs a newer build instead of half-working for one
/// cycle. A slice is written at the *lowest* version that expresses it, so a
/// dataset carrying none of those rows still produces [`SLICE_FORMAT_VERSION`]
/// and an older reader keeps restoring it.
pub const SLICE_FORMAT_VERSION_FULL_CLEANUP: u32 = 2;

/// The one table outside `cayenne_table`'s cascade that a
/// [`SLICE_FORMAT_VERSION`] import clears by hand. Naming it here is what lets
/// [`DatasetMetastoreSlice::rows_an_older_import_cannot_clear`] derive the rest
/// of the set from the registry rather than from a second hand-kept list — so a
/// table added to [`blob_keyed_tables`][super::blob_keyed_tables] later starts
/// requiring [`SLICE_FORMAT_VERSION_FULL_CLEANUP`] without anyone remembering
/// to say so.
const OLDER_IMPORT_CLEARS: &str = "cayenne_insert_record";

/// Engine identifier embedded in slices to detect cross-engine misuse.
pub const SLICE_ENGINE: &str = "cayenne";

/// Refuse a slice this build does not read.
///
/// One definition for both entry points — parsing an archive's slice and
/// importing it — so the two cannot come to disagree about the range, and one
/// message, so a reword cannot drop the upgrade instruction from whichever
/// caller is not the one being edited. `unsupported_version_message` is pure so
/// the wording is asserted in a test rather than only read.
fn ensure_supported_format_version(version: u32) -> CatalogResult<()> {
    if (SLICE_FORMAT_VERSION..=SLICE_FORMAT_VERSION_FULL_CLEANUP).contains(&version) {
        return Ok(());
    }
    Err(CatalogError::Database {
        message: unsupported_version_message(version),
    })
}

/// The message a slice this build cannot read is refused with.
fn unsupported_version_message(version: u32) -> String {
    format!(
        "refusing the metastore slice: unsupported format_version {version} (this build reads {SLICE_FORMAT_VERSION} to {SLICE_FORMAT_VERSION_FULL_CLEANUP}); upgrade the runtime that reads this snapshot"
    )
}

/// JSON-friendly mirror of [`MetastoreValue`]. Blobs are base64-encoded so the
/// document remains valid UTF-8 JSON.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "t", content = "v")]
pub enum SliceValue {
    /// 64-bit signed integer.
    #[serde(rename = "i")]
    Integer(i64),
    /// UTF-8 text.
    #[serde(rename = "s")]
    Text(String),
    /// Boolean.
    #[serde(rename = "b")]
    Bool(bool),
    /// Binary blob, base64-encoded for JSON-friendliness.
    #[serde(rename = "x")]
    Blob(String),
    /// SQL NULL.
    #[serde(rename = "n")]
    Null,
}

impl From<&MetastoreValue> for SliceValue {
    fn from(v: &MetastoreValue) -> Self {
        match v {
            MetastoreValue::Integer(i) => SliceValue::Integer(*i),
            MetastoreValue::Text(s) => SliceValue::Text(s.clone()),
            MetastoreValue::Bool(b) => SliceValue::Bool(*b),
            MetastoreValue::Blob(b) => SliceValue::Blob(BASE64.encode(b)),
            MetastoreValue::Null => SliceValue::Null,
        }
    }
}

impl SliceValue {
    /// Convert back to a `MetastoreValue`.
    ///
    /// # Errors
    ///
    /// Returns an error if a `Blob` variant contains invalid base64.
    pub fn into_metastore_value(self) -> CatalogResult<MetastoreValue> {
        Ok(match self {
            SliceValue::Integer(i) => MetastoreValue::Integer(i),
            SliceValue::Text(s) => MetastoreValue::Text(s),
            SliceValue::Bool(b) => MetastoreValue::Bool(b),
            SliceValue::Blob(b64) => {
                let bytes = BASE64
                    .decode(b64.as_bytes())
                    .map_err(|e| CatalogError::Database {
                        message: format!("invalid base64 blob in metastore slice: {e}"),
                    })?;
                MetastoreValue::Blob(bytes)
            }
            SliceValue::Null => MetastoreValue::Null,
        })
    }
}

/// One row of a slice's per-table contents. Ordered to match the column order
/// in [`EXPECTED_TABLES`].
pub type SliceRow = Vec<SliceValue>;

/// Versioned, dataset-scoped slice of the Cayenne metastore.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetMetastoreSlice {
    /// Slice format version: [`SLICE_FORMAT_VERSION`], or
    /// [`SLICE_FORMAT_VERSION_FULL_CLEANUP`] when the slice carries partition
    /// child tables. A build reads both and writes the lower of the two
    /// whenever the payload allows it.
    pub format_version: u32,
    /// Engine identifier; must equal [`SLICE_ENGINE`] (`"cayenne"`).
    pub engine: String,
    /// Logical dataset name (matches `cayenne_table.table_name`).
    pub dataset_name: String,
    /// Wall-clock timestamp (milliseconds since epoch) when the slice was exported.
    pub exported_at_ms: i64,
    /// Map of metastore table name -> rows. Each row is positional;
    /// column order must match the corresponding [`EXPECTED_TABLES`] entry.
    pub tables: BTreeMap<String, Vec<SliceRow>>,
}

impl DatasetMetastoreSlice {
    /// Marshal to a JSON byte vector suitable for embedding in a snapshot
    /// archive.
    ///
    /// # Errors
    ///
    /// Propagates JSON serialization errors.
    pub fn to_json_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Parse from a JSON byte slice. Validates `format_version` and `engine`.
    ///
    /// # Errors
    ///
    /// Returns an error if the JSON is malformed, the format version is
    /// unsupported, or the engine identifier mismatches.
    pub fn from_json_bytes(bytes: &[u8]) -> CatalogResult<Self> {
        let slice: Self = serde_json::from_slice(bytes).map_err(|e| CatalogError::Database {
            message: format!("failed to parse metastore slice JSON: {e}"),
        })?;
        ensure_supported_format_version(slice.format_version)?;
        if slice.engine != SLICE_ENGINE {
            return Err(CatalogError::Database {
                message: format!(
                    "metastore slice engine mismatch: expected '{SLICE_ENGINE}', got '{}'",
                    slice.engine
                ),
            });
        }
        Ok(slice)
    }
}

/// Returns the (`path_column_index`, `path_is_relative_column_index`) for tables
/// that store filesystem paths. Returns `None` for tables without path columns.
fn path_columns_for_table(table_name: &str) -> Option<(usize, usize)> {
    // `path_is_relative` always follows `path`.
    let path = match table_name {
        "cayenne_table" | "cayenne_delete_file" => CAYENNE_TABLE_PATH_INDEX,
        "cayenne_partition" => CAYENNE_PARTITION_PATH_INDEX,
        _ => return None,
    };
    Some((path, path + 1))
}

/// Returns the column index that holds `table_id` for each metastore table.
/// `cayenne_table` itself stores it at index 0; dependent tables store it at
/// index 1.
fn table_id_column_index(table_name: &str) -> usize {
    match table_name {
        "cayenne_table" => 0,
        _ => 1,
    }
}

/// Rewrite a path to be relative to `anchor`, if it is currently absolute and
/// lives under `anchor`. Returns the original path unchanged otherwise.
///
/// This is intentionally lenient: paths outside the anchor (which would
/// indicate misconfiguration on the writer) are left untouched and surface
/// later as "file not found" on the reader if the absolute path does not
/// resolve there. We log a warning so the operator notices.
fn make_relative(abs: &str, anchor: &Path) -> String {
    let p = Path::new(abs);
    if let Ok(rel) = p.strip_prefix(anchor) {
        rel.to_string_lossy().into_owned()
    } else {
        tracing::warn!(
            "cayenne metastore export: path {abs:?} is not under anchor {anchor:?}; \
             leaving as-is — slice will not be portable to readers with a different data directory"
        );
        abs.to_string()
    }
}

/// Rewrite a (possibly relative) path back to absolute, anchored at `anchor`.
/// Paths that already are absolute are returned unchanged (defensive: handles
/// the lenient case in [`make_relative`]).
fn make_absolute(rel_or_abs: &str, anchor: &Path) -> String {
    let p = Path::new(rel_or_abs);
    if p.is_absolute() {
        rel_or_abs.to_string()
    } else {
        anchor.join(p).to_string_lossy().into_owned()
    }
}

/// Lookup `table_id` for the given dataset, returning `None` if not found.
async fn lookup_table_id(
    metastore: &impl MetastoreBackend,
    dataset_name: &str,
) -> CatalogResult<Option<String>> {
    let rows = metastore
        .query(
            QueryParams {
                sql: "SELECT table_id FROM cayenne_table WHERE table_name = ?",
                params: vec![MetastoreValue::Text(dataset_name.to_string())],
            },
            |row| row.get_string(0),
        )
        .await?;
    Ok(rows.into_iter().next())
}

/// Column indices into a slice row, following [`EXPECTED_TABLES`]' column
/// order. `column_indices_match_the_expected_schema` pins each against that
/// list, so a column inserted ahead of one of these fails a test rather than
/// silently shifting what a validator reads.
const CAYENNE_TABLE_NAME_INDEX: usize = 1;
const CAYENNE_TABLE_PATH_INDEX: usize = 2;
const CAYENNE_PARTITION_VALUES_INDEX: usize = 3;
const CAYENNE_PARTITION_PATH_INDEX: usize = 5;

/// The text at `index`, or `None` when the column is absent or not text.
fn text_at(row: &SliceRow, index: usize) -> Option<&str> {
    match row.get(index) {
        Some(SliceValue::Text(value)) => Some(value),
        _ => None,
    }
}

/// The rows of `slice`'s copy of `table`, or an empty slice when it has none.
fn slice_rows<'a>(slice: &'a DatasetMetastoreSlice, table: &str) -> &'a [SliceRow] {
    slice.tables.get(table).map_or(&[], Vec::as_slice)
}

/// Read every row of `expected` belonging to `table_id`, as slice rows.
///
/// `sql` is built once per table by the caller rather than per `table_id`: a
/// partitioned dataset calls this once per (table, child) pair.
async fn rows_for_table_id(
    metastore: &impl MetastoreBackend,
    expected: &super::ExpectedTable,
    sql: &str,
    table_id: &str,
) -> CatalogResult<Vec<SliceRow>> {
    let n_columns = expected.columns.len();
    let table_id_param = super::table_id_filter_value(expected.table_id_encoding, table_id);
    metastore
        .query(
            QueryParams {
                sql,
                params: vec![table_id_param],
            },
            move |row| {
                let mut out = Vec::with_capacity(n_columns);
                for i in 0..n_columns {
                    out.push(SliceValue::from(&row.get_value(i)?));
                }
                Ok(out)
            },
        )
        .await
}

/// Export this dataset's rows from the metastore as a versioned slice.
///
/// Path columns are rewritten relative to `data_dir_anchor` so the resulting
/// slice is portable to readers with a different data directory, provided
/// they re-anchor at their own data directory on import.
///
/// # Errors
///
/// Returns an error if the dataset does not exist, or if any underlying
/// metastore query fails.
pub async fn export_dataset(
    metastore: &impl MetastoreBackend,
    dataset_name: &str,
    data_dir_anchor: &Path,
) -> CatalogResult<DatasetMetastoreSlice> {
    let table_id = lookup_table_id(metastore, dataset_name)
        .await?
        .ok_or_else(|| CatalogError::Database {
            message: format!(
                "cannot export metastore slice: dataset '{dataset_name}' not found in cayenne_table"
            ),
        })?;

    // A partitioned table's partitions are catalog tables of their own, so the
    // slice must span the parent *and* every child: restoring the parent alone
    // leaves `cayenne_partition` rows whose child tables do not exist, and the
    // dataset then fails to open at `infer_existing_partitions` with
    // `TableNotFound`.
    let child_ids = super::partition_child_table_ids(metastore, dataset_name, &table_id).await?;

    // Parent first, so the parent's `cayenne_table` row leads the slice.
    let table_ids: Vec<&str> = std::iter::once(table_id.as_str())
        .chain(child_ids.iter().map(String::as_str))
        .collect();

    let mut tables: BTreeMap<String, Vec<SliceRow>> = BTreeMap::new();

    for expected in EXPECTED_TABLES {
        let sql = format!(
            "SELECT {} FROM {} WHERE table_id = ?",
            expected.columns.join(", "),
            expected.name
        );
        let mut rows: Vec<SliceRow> = Vec::new();
        for id in &table_ids {
            rows.extend(rows_for_table_id(metastore, expected, &sql, id).await?);
        }

        // Rewrite path columns to be relative to anchor. If `make_relative`
        // could not strip the anchor (path is outside `data_dir_anchor`), it
        // returns the original absolute path — in that case we leave
        // `path_is_relative=false` so the slice stays internally consistent.
        if let Some((path_idx, rel_idx)) = path_columns_for_table(expected.name) {
            for r in &mut rows {
                if let Some(SliceValue::Text(abs)) = r.get(path_idx).cloned() {
                    let rel = make_relative(&abs, data_dir_anchor);
                    let is_relative = rel != abs;
                    r[path_idx] = SliceValue::Text(rel);
                    r[rel_idx] = SliceValue::Bool(is_relative);
                }
            }
        }

        tables.insert(expected.name.to_string(), rows);
    }

    let mut slice = DatasetMetastoreSlice {
        format_version: SLICE_FORMAT_VERSION,
        engine: SLICE_ENGINE.to_string(),
        dataset_name: dataset_name.to_string(),
        exported_at_ms: chrono::Utc::now().timestamp_millis(),
        tables,
    };

    // The lowest version that expresses this payload, read off the payload
    // rather than off `child_ids`: partition children are only one of the two
    // things a slice can now carry that an older import cannot clear.
    if slice.rows_an_older_import_cannot_clear().is_some() {
        slice.format_version = SLICE_FORMAT_VERSION_FULL_CLEANUP;
    }

    // Refuse to *write* a slice the reader would refuse. A partition whose
    // child table could not be matched would otherwise be published as the
    // store's `current-snapshot-id` and only fail at restore, which is a
    // disaster-recovery bootstrap — the worst moment to discover it. Failing
    // the export instead aborts the snapshot and leaves the previous, good one
    // in place.
    slice.validate()?;

    Ok(slice)
}

impl DatasetMetastoreSlice {
    /// The `table_name`s this slice carries, parent first.
    ///
    /// Import replaces whole tables, so these are exactly the local rows it
    /// must clear before inserting. `dataset_name` is included even when the
    /// slice carries no `cayenne_table` row for it, so a malformed slice still
    /// clears the dataset it claims to replace rather than merging into it.
    fn table_names(&self) -> Vec<&str> {
        let mut names = vec![self.dataset_name.as_str()];
        for row in slice_rows(self, "cayenne_table") {
            if let Some(name) = text_at(row, CAYENNE_TABLE_NAME_INDEX)
                && !names.contains(&name)
            {
                names.push(name);
            }
        }
        names
    }

    /// The `cayenne_table` rows this slice carries that are not the dataset's
    /// own — i.e. its partition children.
    ///
    /// Three places turn on this distinction (the names import must clear, the
    /// rows an older import cannot clear, and the per-child validation), so it
    /// is spelled once.
    fn child_table_rows(&self) -> impl Iterator<Item = &SliceRow> {
        slice_rows(self, "cayenne_table").iter().filter(|row| {
            text_at(row, CAYENNE_TABLE_NAME_INDEX) != Some(self.dataset_name.as_str())
        })
    }

    /// The first table this slice carries rows in that a
    /// [`SLICE_FORMAT_VERSION`] import would not clear before inserting.
    ///
    /// Those rows are what makes a restore unrepeatable on such a reader: it
    /// inserts them, its own cleanup never reaches them, and the next restore
    /// collides with them. Two sources exist, and both are new in the slice
    /// rather than new in the metastore:
    ///
    /// * a partition child `cayenne_table` row — the cleanup deletes the parent
    ///   by name, and `ON DELETE CASCADE` does not reach a sibling row;
    /// * a row in a [`blob_keyed_tables`][super::blob_keyed_tables] table other
    ///   than [`OLDER_IMPORT_CLEARS`] — those have no foreign key back to the
    ///   parent at all, which is why the cleanup names one of them by hand.
    ///
    /// Derived from the registry rather than from a list, so a blob-keyed table
    /// added later is covered without anyone remembering to add it here.
    fn rows_an_older_import_cannot_clear(&self) -> Option<&'static str> {
        if self.child_table_rows().next().is_some() {
            return Some("cayenne_table");
        }
        super::blob_keyed_tables()
            .find(|table| {
                table.name != OLDER_IMPORT_CLEARS && !slice_rows(self, table.name).is_empty()
            })
            .map(|table| table.name)
    }

    /// Refuse a slice that is not this dataset and its own partition children,
    /// or whose partitions have no child table to be restored with.
    ///
    /// Both halves guard the same thing from opposite sides: that a restore
    /// touches exactly the dataset's own tables, and that it leaves a dataset
    /// that opens.
    ///
    /// *No extra tables.* Import replaces every table the slice names, so the
    /// set of names it may carry is what bounds what a restore can overwrite.
    /// One `cayenne.db` is the catalog for every Cayenne dataset in the
    /// instance, so a slice naming a table that is not a child of the dataset
    /// it claims to be would replace an unrelated dataset's row — a legal name,
    /// a successful insert, and the victim's rows simply gone. Both names the
    /// child derivation can produce are accepted, since a partition created by
    /// an older runtime still answers to the legacy one.
    ///
    /// *No missing children.* Every `cayenne_partition` row names a directory a
    /// child `cayenne_table` row is rooted at, and a slice carrying the
    /// partition but not the child restores to a dataset that cannot open —
    /// `infer_existing_partitions` propagates `TableNotFound`.
    ///
    /// *A version that matches the payload.* A slice carrying anything
    /// [`Self::rows_an_older_import_cannot_clear`] names declares
    /// [`SLICE_FORMAT_VERSION_FULL_CLEANUP`], which is what stops a reader that
    /// predates those rows from accepting one it can restore only once.
    ///
    /// *A parent to restore.* A slice that names a dataset but carries no
    /// `cayenne_table` row for it satisfies both halves above vacuously, while
    /// import still clears the local dataset by that name — so it deletes the
    /// dataset, inserts nothing, and reports a successful restore.
    ///
    /// *Each child paired with its own partition.* A child's name and its path
    /// are checked together rather than as two independent sets: a slice that
    /// swaps two children's paths satisfies both sets and restores each
    /// partition rooted at the other's directory.
    /// `infer_existing_partitions` opens a child by the name derived from its
    /// partition's own values, so it then answers each partition with the other
    /// one's rows — a wrong result, not a failure to open.
    ///
    /// # Errors
    ///
    /// Returns a [`CatalogError::Database`] naming the offending table or
    /// partition.
    fn validate(&self) -> CatalogResult<()> {
        let refuse = |detail: String| {
            Err(CatalogError::Database {
                message: format!(
                    "refusing the metastore slice for dataset '{}': {detail}",
                    self.dataset_name
                ),
            })
        };

        let table_rows = slice_rows(self, "cayenne_table");

        // The parent's own `table_id`, so a child's partition rows (there are
        // none today, but nothing here depends on that) cannot widen the
        // accepted set. Required rather than optional: with no parent row every
        // check below passes over an empty set, and import goes on to clear the
        // local dataset named in the payload and insert nothing in its place.
        let Some(parent_table_id) = table_rows
            .iter()
            .find(|row| text_at(row, CAYENNE_TABLE_NAME_INDEX) == Some(self.dataset_name.as_str()))
            .and_then(|row| text_at(row, table_id_column_index("cayenne_table")))
        else {
            return refuse(
                "it carries no readable 'cayenne_table' row for the dataset itself, so importing it would delete the local dataset and restore nothing in its place".to_string(),
            );
        };

        // The parent's partitions: for each name a child may legally take, the
        // directory that child must be rooted at. The names come from
        // `partition_child_candidate_names`, which is also what
        // `metastore::partition_child_table_ids` binds its lookup to, so the
        // rule that resolves a child and the rule that accepts a restored one
        // cannot drift apart.
        //
        // Keyed by name rather than collected into two independent sets: the
        // name and the path have to be checked *together*, or a slice that
        // swaps two children's paths satisfies both sets on its own.
        let mut expected_children: HashMap<String, &str> = HashMap::new();
        let mut partition_paths: Vec<&str> = Vec::new();
        let partition_owner_index = table_id_column_index("cayenne_partition");
        // Every `table_id` the slice carries a `cayenne_table` row for. A
        // partition row may name any of them — `export_dataset` collects each
        // table's rows for the parent *and* every child — but nothing outside
        // that set: `import_dataset` inserts every row the slice carries, and
        // an id from neither may name a table this node already has, which
        // would attach the partition to an unrelated local dataset. The table
        // rows below are held to the matching rule, so an id in this set is an
        // id the import is already entitled to write.
        let slice_table_ids: HashSet<&str> = table_rows
            .iter()
            .filter_map(|row| text_at(row, table_id_column_index("cayenne_table")))
            .collect();

        for row in slice_rows(self, "cayenne_partition") {
            let Some(owner) = text_at(row, partition_owner_index) else {
                return refuse(
                    "one of its partition rows has no readable owning table".to_string(),
                );
            };
            if !slice_table_ids.contains(owner) {
                return refuse(format!(
                    "it carries a partition owned by '{owner}', which is not one of the tables the slice restores, and importing it would attach that partition to an unrelated dataset"
                ));
            }
            // A child's own partition rows stay out of the parent's expected
            // set: they describe that child's partitions, not names a child of
            // *this* dataset may take.
            if owner != parent_table_id {
                continue;
            }
            let Some(partition_path) = text_at(row, CAYENNE_PARTITION_PATH_INDEX) else {
                continue;
            };
            partition_paths.push(partition_path);
            let Some(values_json) = text_at(row, CAYENNE_PARTITION_VALUES_INDEX) else {
                continue;
            };
            let Ok(values) = serde_json::from_str::<Vec<String>>(values_json) else {
                continue;
            };
            for name in crate::partition_naming::partition_child_candidate_names(
                &self.dataset_name,
                &values,
            ) {
                expected_children.insert(name, partition_path);
            }
        }

        // Both sides were rewritten relative to the exporter's anchor, so a
        // child's path is comparable to its partition's without re-anchoring.
        let mut partitions_with_a_child: HashSet<&str> = HashSet::new();
        for row in table_rows {
            let Some(name) = text_at(row, CAYENNE_TABLE_NAME_INDEX) else {
                return refuse("one of its table rows has no readable name".to_string());
            };
            if name == self.dataset_name {
                continue;
            }
            let Some(partition_path) = expected_children.get(name).copied() else {
                return refuse(format!(
                    "it carries a table '{name}' that is not one of this dataset's partition children, and importing it would replace an unrelated dataset's metadata"
                ));
            };
            let child_path = text_at(row, CAYENNE_TABLE_PATH_INDEX);
            if child_path != Some(partition_path) {
                return refuse(format!(
                    "its child table '{name}' is rooted at '{}' but the partition it belongs to is at '{partition_path}', so the restored dataset would read that partition's rows from another partition's directory",
                    child_path.unwrap_or("no readable path")
                ));
            }
            partitions_with_a_child.insert(partition_path);
        }

        if let Some(orphan) = partition_paths
            .iter()
            .find(|path| !partitions_with_a_child.contains(*path))
        {
            return refuse(format!(
                "its partition at '{orphan}' has no child table row, so the restored dataset could not be opened"
            ));
        }

        // The payload and the version it declares must agree. A reader that
        // predates these rows is protected only by refusing the version, so a
        // slice carrying them under the older one would reach the very build
        // the bump exists for.
        if let Some(table) = self.rows_an_older_import_cannot_clear()
            && self.format_version < SLICE_FORMAT_VERSION_FULL_CLEANUP
        {
            return refuse(format!(
                "it carries rows in '{table}' that a format_version {SLICE_FORMAT_VERSION} import does not clear, but declares format_version {}; such a slice is written at {SLICE_FORMAT_VERSION_FULL_CLEANUP}",
                self.format_version
            ));
        }

        Ok(())
    }
}

/// Atomically import a dataset slice into the metastore.
///
/// If `slice.dataset_name` already exists in the local `cayenne_table`, that
/// row is deleted (cascading to all dependent rows) before the slice's rows
/// are inserted. Path columns are re-anchored at `data_dir_anchor`.
///
/// The entire import runs inside a single `BEGIN IMMEDIATE` transaction; on
/// any error the local metastore is left unchanged.
///
/// # Errors
///
/// Returns an error if any DML fails or the slice is internally inconsistent.
pub async fn import_dataset(
    metastore: &impl MetastoreBackend,
    slice: &DatasetMetastoreSlice,
    data_dir_anchor: &Path,
) -> CatalogResult<()> {
    ensure_supported_format_version(slice.format_version)?;
    if slice.engine != SLICE_ENGINE {
        return Err(CatalogError::Database {
            message: format!(
                "refusing to import metastore slice: engine '{}' != '{SLICE_ENGINE}'",
                slice.engine
            ),
        });
    }

    slice.validate()?;

    // The reader's own copy of this dataset may be partitioned differently from
    // the slice's: a child whose partition the slice does not carry is named
    // after a key the slice never mentions, so clearing by the slice's table
    // names alone would leave it behind, holding a stale schema and a file
    // manifest whose files the restore replaced. Resolve those children the
    // same way the catalog does when it drops a partitioned table. Read before
    // the transaction opens, as `drop_table` does, so a partition created
    // concurrently with a restore can still be missed.
    let mut stale_child_ids: Vec<String> = Vec::new();
    if let Some(local_parent_id) = lookup_table_id(metastore, &slice.dataset_name).await? {
        stale_child_ids =
            super::partition_child_table_ids(metastore, &slice.dataset_name, &local_parent_id)
                .await?;
    }

    let txn = metastore.begin_transaction().await?;

    for child_id in &stale_child_ids {
        super::clear_blob_keyed_marker_rows(txn.as_ref(), child_id).await?;
        txn.execute(ExecuteParams {
            sql: "DELETE FROM cayenne_table WHERE table_id = ?",
            params: vec![MetastoreValue::Text(child_id.clone())],
        })
        .await?;
    }

    // Wholesale-replace any existing rows for every table the slice carries —
    // the dataset and, for a partitioned dataset, each of its per-partition
    // child tables. `cayenne_table`'s `ON DELETE CASCADE` clears the dependent
    // rows of every table whose foreign key still references it — but the
    // BLOB-keyed marker tables have no such foreign key, so resolve each
    // existing `table_id` and clear their rows explicitly first, inside the same
    // transaction, before the row is removed.
    for table_name in slice.table_names() {
        // A scalar subquery always returns exactly one row, so absence arrives
        // as `Null` rather than as the same `Err` a real failure produces.
        // `query_row_values` reports "no rows" and an execution error through
        // one variant, so matching `Err(_)` to `None` here would read an
        // interrupted or busy SELECT as "this dataset is not present" and skip
        // the marker cleanup below, leaving the parent's BLOB-keyed rows behind
        // while its `cayenne_table` row is replaced.
        let existing_table_id = match txn
            .query_row_values(QueryRowParams {
                sql: "SELECT (SELECT table_id FROM cayenne_table WHERE table_name = ?)",
                params: vec![MetastoreValue::Text(table_name.to_string())],
            })
            .await?
            .into_iter()
            .next()
        {
            Some(MetastoreValue::Text(id)) => Some(id),
            _ => None,
        };

        // A child name the slice supplies is only allowed to displace a local
        // row that local discovery already agreed is this dataset's child.
        //
        // The two sides do not derive it the same way, and that asymmetry is
        // deliberate on the discovery side: `partition_child_table_ids` requires
        // the local row's `path` to equal the partition's before it counts as a
        // child, precisely because the legacy naming convention
        // (`{parent}_{values}`) can also spell an unrelated table an operator
        // happens to have accelerated into the same metastore — partitioning
        // `events` by year spells `events_2024`. `validate` cannot apply that
        // rule: it sees only names and paths the slice itself supplies. So a
        // name that reaches here without being locally discovered is either that
        // collision or a slice that disagrees with this node about what its
        // children are, and in both cases the row stays: the slice's own INSERT
        // then fails on `cayenne_table(table_name)` and the whole restore rolls
        // back, which is the safe direction to be wrong in. The catalog's
        // `drop_table` declines the same collision for the same reason.
        let is_parent = table_name == slice.dataset_name;
        let is_local_child = existing_table_id
            .as_deref()
            .is_some_and(|id| stale_child_ids.iter().any(|child| child == id));
        if !is_parent && !is_local_child {
            continue;
        }

        if let Some(existing_table_id) = existing_table_id.as_deref() {
            super::clear_blob_keyed_marker_rows(txn.as_ref(), existing_table_id).await?;
        }

        txn.execute(ExecuteParams {
            sql: "DELETE FROM cayenne_table WHERE table_name = ?",
            params: vec![MetastoreValue::Text(table_name.to_string())],
        })
        .await?;
    }

    for expected in EXPECTED_TABLES {
        let Some(rows) = slice.tables.get(expected.name) else {
            continue;
        };
        if rows.is_empty() {
            continue;
        }
        let path_cols = path_columns_for_table(expected.name);

        // Build INSERT statement. We use positional ? placeholders matching
        // the EXPECTED_TABLES column order.
        let placeholders = vec!["?"; expected.columns.len()].join(", ");
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            expected.name,
            expected.columns.join(", "),
            placeholders
        );

        for row in rows {
            if row.len() != expected.columns.len() {
                return Err(CatalogError::Database {
                    message: format!(
                        "metastore slice row for table {} has {} columns, expected {}",
                        expected.name,
                        row.len(),
                        expected.columns.len()
                    ),
                });
            }

            // Convert SliceValue -> MetastoreValue, applying path rewriting.
            let mut params: Vec<MetastoreValue> = Vec::with_capacity(row.len());
            for (i, v) in row.iter().cloned().enumerate() {
                let mut mv = v.into_metastore_value()?;
                if let Some((path_idx, rel_idx)) = path_cols {
                    if i == path_idx {
                        if let MetastoreValue::Text(p) = &mv {
                            mv = MetastoreValue::Text(make_absolute(p, data_dir_anchor));
                        }
                    } else if i == rel_idx {
                        // We always re-store as absolute on import; flip the flag
                        // back to false so the catalog code paths see the same
                        // shape they always have.
                        mv = MetastoreValue::Bool(false);
                    }
                }
                params.push(mv);
            }

            txn.execute(ExecuteParams { sql: &sql, params }).await?;
        }
    }

    txn.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metastore::sqlite::SqliteMetastore;

    async fn fresh_metastore() -> (Arc<SqliteMetastore>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db_path = tmp.path().join("cayenne.db");
        let metastore = Arc::new(SqliteMetastore::new(format!(
            "sqlite://{}",
            db_path.display()
        )));
        metastore.init_schema().await.expect("init_schema");
        (metastore, tmp)
    }

    use std::sync::Arc;

    fn sample_table_row(table_id: &str, table_name: &str, abs_path: &str) -> Vec<MetastoreValue> {
        vec![
            MetastoreValue::Text(table_id.to_string()),
            MetastoreValue::Text(table_name.to_string()),
            MetastoreValue::Text(abs_path.to_string()),
            MetastoreValue::Bool(false),
            MetastoreValue::Text("{\"fields\":[]}".to_string()),
            MetastoreValue::Null,
            MetastoreValue::Null,
            MetastoreValue::Text(String::new()),
            MetastoreValue::Null,
            MetastoreValue::Null,
            MetastoreValue::Integer(0),
        ]
    }

    /// A `cayenne_partition` row for a single-column partition holding
    /// `partition_value`.
    ///
    /// `partition_key` is derived from the value exactly as `create_partition`
    /// derives it (`cayenne_catalog.rs` writes `PartitionMetadata::composite_key`
    /// into that column), so a fixture built here matches the convention the
    /// runtime's own child lookup reads.
    fn sample_partition_row(
        partition_id: &str,
        table_id: &str,
        abs_path: &str,
        partition_value: &str,
    ) -> Vec<MetastoreValue> {
        let values = vec![partition_value.to_string()];
        vec![
            MetastoreValue::Text(partition_id.to_string()),
            MetastoreValue::Text(table_id.to_string()),
            MetastoreValue::Text(r#"["part_col"]"#.to_string()),
            MetastoreValue::Text(serde_json::to_string(&values).expect("a string list serializes")),
            MetastoreValue::Text(crate::metadata::composite_partition_key(&values)),
            MetastoreValue::Text(abs_path.to_string()),
            MetastoreValue::Bool(false),
            MetastoreValue::Integer(100),
            MetastoreValue::Integer(1024),
        ]
    }

    /// The child table name the runtime derives for a single-column partition
    /// holding `partition_value` under `parent`.
    fn child_table_name(parent: &str, partition_value: &str) -> String {
        crate::partition_naming::partition_child_table_name(
            parent,
            &crate::metadata::composite_partition_key(&[partition_value.to_string()]),
        )
    }

    /// A partitioned dataset as it really exists: the parent plus, for each
    /// partition, the child `cayenne_table` row rooted at the partition's own
    /// directory. Use [`insert_dataset_without_children`] to build the broken
    /// shape deliberately.
    async fn insert_dataset(
        ms: &SqliteMetastore,
        dataset: &str,
        anchor: &Path,
        partitions: &[(&str, &str, &str)], // (partition_id, partition_key, file)
    ) {
        insert_dataset_without_children(ms, dataset, anchor, partitions).await;
        for (_pid, pk, file) in partitions {
            insert_partition_child(ms, dataset, pk, anchor, file).await;
        }
    }

    async fn insert_dataset_without_children(
        ms: &SqliteMetastore,
        dataset: &str,
        anchor: &Path,
        partitions: &[(&str, &str, &str)], // (partition_id, partition_key, file)
    ) {
        let table_id = format!("tid-{dataset}");
        let table_path = anchor
            .join(format!("{dataset}.dir"))
            .to_string_lossy()
            .into_owned();
        ms.execute(ExecuteParams {
            sql: "INSERT INTO cayenne_table (table_id, table_name, path, path_is_relative, schema_json, primary_key_json, on_conflict_json, current_snapshot_id, partition_column, vortex_config_json, current_sequence_number) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params: sample_table_row(&table_id, dataset, &table_path),
        })
        .await
        .expect("insert table");
        for (pid, pk, file) in partitions {
            let abs = anchor.join(file).to_string_lossy().into_owned();
            ms.execute(ExecuteParams {
                sql: "INSERT INTO cayenne_partition (partition_id, table_id, partition_columns_json, partition_values_json, partition_key, path, path_is_relative, record_count, file_size_bytes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params: sample_partition_row(pid, &table_id, &abs, pk),
            })
            .await
            .expect("insert partition");
        }
    }

    #[tokio::test]
    async fn round_trip_preserves_rows_and_relocates_paths() {
        let (ms_a, tmp_a) = fresh_metastore().await;
        let anchor_a = tmp_a.path();
        insert_dataset(
            &ms_a,
            "trips",
            anchor_a,
            &[
                ("p1", "k1", "trips/part-001"),
                ("p2", "k2", "trips/part-002"),
            ],
        )
        .await;

        let slice = export_dataset(ms_a.as_ref(), "trips", anchor_a)
            .await
            .expect("export");
        assert_eq!(slice.format_version, SLICE_FORMAT_VERSION_FULL_CLEANUP);
        assert_eq!(slice.engine, SLICE_ENGINE);
        assert_eq!(
            slice.tables["cayenne_table"].len(),
            3,
            "the parent plus one child per partition"
        );
        assert_eq!(slice.tables["cayenne_partition"].len(), 2);

        for row in &slice.tables["cayenne_partition"] {
            if let SliceValue::Text(p) = &row[5] {
                assert!(
                    !std::path::Path::new(p).is_absolute(),
                    "slice partition path should be relative: {p}"
                );
            }
        }

        let (ms_b, tmp_b) = fresh_metastore().await;
        let anchor_b = tmp_b.path();
        import_dataset(ms_b.as_ref(), &slice, anchor_b)
            .await
            .expect("import");

        let partitions: Vec<(String, String)> = ms_b
            .query(
                QueryParams {
                    sql: "SELECT partition_key, path FROM cayenne_partition WHERE table_id = 'tid-trips'",
                    params: vec![],
                },
                |row| Ok((row.get_string(0)?, row.get_string(1)?)),
            )
            .await
            .expect("query partitions");
        assert_eq!(partitions.len(), 2);
        for (_pk, path) in &partitions {
            let anchor_str: String = anchor_b.to_string_lossy().to_string();
            assert!(
                path.starts_with(&anchor_str),
                "path {path} should be under {anchor_str}"
            );
        }
    }

    #[tokio::test]
    async fn import_replaces_prior_dataset_rows_wholesale() {
        let (ms, tmp) = fresh_metastore().await;
        let anchor = tmp.path();
        insert_dataset(
            &ms,
            "trips",
            anchor,
            &[
                ("old1", "k1", "old1"),
                ("old2", "k2", "old2"),
                ("old3", "k3", "old3"),
            ],
        )
        .await;

        let mut tables: BTreeMap<String, Vec<SliceRow>> = BTreeMap::new();
        tables.insert(
            "cayenne_table".to_string(),
            vec![
                sample_table_row("tid-trips", "trips", "trips.dir")
                    .iter()
                    .map(SliceValue::from)
                    .collect(),
                sample_table_row("tid-trips-newk", &child_table_name("trips", "newk"), "new1")
                    .iter()
                    .map(SliceValue::from)
                    .collect(),
            ],
        );
        tables.insert(
            "cayenne_partition".to_string(),
            vec![
                sample_partition_row("new1", "tid-trips", "new1", "newk")
                    .iter()
                    .map(SliceValue::from)
                    .collect(),
            ],
        );
        let slice = DatasetMetastoreSlice {
            format_version: SLICE_FORMAT_VERSION_FULL_CLEANUP,
            engine: SLICE_ENGINE.to_string(),
            dataset_name: "trips".to_string(),
            exported_at_ms: 0,
            tables,
        };

        import_dataset(ms.as_ref(), &slice, anchor)
            .await
            .expect("import");

        let rows: Vec<String> = ms
            .query(
                QueryParams {
                    sql: "SELECT partition_id FROM cayenne_partition WHERE table_id = 'tid-trips' ORDER BY partition_id",
                    params: vec![],
                },
                |row| row.get_string(0),
            )
            .await
            .expect("q");
        assert_eq!(rows, vec!["new1".to_string()]);
    }

    #[tokio::test]
    async fn import_leaves_other_datasets_untouched() {
        let (ms, tmp) = fresh_metastore().await;
        let anchor = tmp.path();
        insert_dataset(&ms, "trips", anchor, &[("t1", "k1", "t1")]).await;
        insert_dataset(&ms, "riders", anchor, &[("r1", "k1", "r1")]).await;

        let slice = export_dataset(ms.as_ref(), "trips", anchor)
            .await
            .expect("export");
        import_dataset(ms.as_ref(), &slice, anchor)
            .await
            .expect("import");

        let riders: Vec<String> = ms
            .query(
                QueryParams {
                    sql: "SELECT partition_id FROM cayenne_partition WHERE table_id = 'tid-riders'",
                    params: vec![],
                },
                |row| row.get_string(0),
            )
            .await
            .expect("q riders");
        assert_eq!(riders, vec!["r1".to_string()]);
    }

    /// A `cayenne_insert_record` row (BLOB `table_id`) survives an
    /// export→import round-trip into a fresh metastore — the export filter and
    /// the verbatim re-insert both handle the BLOB key — and a subsequent
    /// wholesale-replace import (whose slice carries no insert-records) clears
    /// the prior insert-record via the explicit delete that replaced the
    /// dropped `ON DELETE CASCADE`.
    #[tokio::test]
    async fn insert_record_blob_round_trips_and_wholesale_replace_clears_it() {
        let (ms_a, tmp_a) = fresh_metastore().await;
        let anchor_a = tmp_a.path();

        // Realistic UUID table_id so the 16-byte BLOB encoding path is taken.
        let table_id = uuid::Uuid::now_v7().to_string();
        let table_path = anchor_a.join("ds.dir").to_string_lossy().into_owned();
        ms_a.execute(ExecuteParams {
            sql: "INSERT INTO cayenne_table (table_id, table_name, path, path_is_relative, schema_json, primary_key_json, on_conflict_json, current_snapshot_id, partition_column, vortex_config_json, current_sequence_number) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params: sample_table_row(&table_id, "ds", &table_path),
        })
        .await
        .expect("insert table");
        // Plant an insert-record with the BLOB-encoded table_id (the write path).
        ms_a.execute(ExecuteParams {
            sql: "INSERT INTO cayenne_insert_record (table_id, pk_bytes, sequence_number) VALUES (?, ?, ?)",
            params: vec![
                MetastoreValue::Blob(crate::metastore::table_id_to_key_bytes(&table_id)),
                MetastoreValue::Blob(7_i64.to_be_bytes().to_vec()),
                MetastoreValue::Integer(13),
            ],
        })
        .await
        .expect("insert insert_record");

        let slice = export_dataset(ms_a.as_ref(), "ds", anchor_a)
            .await
            .expect("export");
        assert_eq!(
            slice.tables["cayenne_insert_record"].len(),
            1,
            "the BLOB-keyed insert-record must be captured by the export filter"
        );

        // Import into a fresh metastore; the BLOB table_id round-trips.
        let (ms_b, tmp_b) = fresh_metastore().await;
        import_dataset(ms_b.as_ref(), &slice, tmp_b.path())
            .await
            .expect("import");
        let seq: i64 = ms_b
            .query_row(
                QueryRowParams {
                    sql: "SELECT sequence_number FROM cayenne_insert_record WHERE table_id = ?",
                    params: vec![MetastoreValue::Blob(
                        crate::metastore::table_id_to_key_bytes(&table_id),
                    )],
                },
                |row| row.get_i64(0),
            )
            .await
            .expect("read imported insert_record");
        assert_eq!(
            seq, 13,
            "the insert-record sequence must survive the round-trip"
        );

        // Re-import a slice for the SAME dataset that carries NO insert-records:
        // the wholesale-replace must clear the prior insert-record explicitly
        // (the FK + cascade is gone).
        let mut tables: BTreeMap<String, Vec<SliceRow>> = BTreeMap::new();
        tables.insert(
            "cayenne_table".to_string(),
            vec![
                sample_table_row(&table_id, "ds", "ds.dir")
                    .iter()
                    .map(SliceValue::from)
                    .collect(),
            ],
        );
        let replace_slice = DatasetMetastoreSlice {
            format_version: SLICE_FORMAT_VERSION,
            engine: SLICE_ENGINE.to_string(),
            dataset_name: "ds".to_string(),
            exported_at_ms: 0,
            tables,
        };
        import_dataset(ms_b.as_ref(), &replace_slice, tmp_b.path())
            .await
            .expect("wholesale-replace import");
        let remaining: i64 = ms_b
            .query_row(
                QueryRowParams {
                    sql: "SELECT COUNT(*) FROM cayenne_insert_record WHERE table_id = ?",
                    params: vec![MetastoreValue::Blob(
                        crate::metastore::table_id_to_key_bytes(&table_id),
                    )],
                },
                |row| row.get_i64(0),
            )
            .await
            .expect("count after replace");
        assert_eq!(
            remaining, 0,
            "wholesale-replace import must clear prior insert-records (no cascade)"
        );
    }

    #[tokio::test]
    async fn rejects_unsupported_format_version() {
        let (ms, tmp) = fresh_metastore().await;
        let mut slice = DatasetMetastoreSlice {
            format_version: 99,
            engine: SLICE_ENGINE.to_string(),
            dataset_name: "trips".to_string(),
            exported_at_ms: 0,
            tables: BTreeMap::new(),
        };
        let err = import_dataset(ms.as_ref(), &slice, tmp.path())
            .await
            .expect_err("should fail");
        assert!(err.to_string().contains("unsupported"), "err={err}");

        slice.format_version = SLICE_FORMAT_VERSION;
        slice.engine = "duckdb".to_string();
        let err = import_dataset(ms.as_ref(), &slice, tmp.path())
            .await
            .expect_err("should fail");
        assert!(err.to_string().contains("engine"), "err={err}");
    }

    #[tokio::test]
    async fn json_round_trip() {
        let (ms, tmp) = fresh_metastore().await;
        insert_dataset(&ms, "trips", tmp.path(), &[("p1", "k1", "f1")]).await;
        let slice = export_dataset(ms.as_ref(), "trips", tmp.path())
            .await
            .expect("export");
        let bytes = slice.to_json_bytes().expect("to_json");
        let parsed = DatasetMetastoreSlice::from_json_bytes(&bytes).expect("from_json");
        assert_eq!(parsed.dataset_name, "trips");
        assert_eq!(parsed.tables.len(), slice.tables.len());
    }
    /// Insert a per-partition child table: its own `cayenne_table` row, named
    /// by the partition-naming derivation and rooted at the partition's own
    /// directory, exactly as the partition creator writes it.
    async fn insert_partition_child(
        ms: &SqliteMetastore,
        parent: &str,
        partition_value: &str,
        anchor: &Path,
        dir: &str,
    ) {
        let child_name = child_table_name(parent, partition_value);
        let child_id = format!("tid-child-{parent}-{partition_value}");
        let child_path = anchor.join(dir).to_string_lossy().into_owned();
        ms.execute(ExecuteParams {
            sql: "INSERT INTO cayenne_table (table_id, table_name, path, path_is_relative, schema_json, primary_key_json, on_conflict_json, current_snapshot_id, partition_column, vortex_config_json, current_sequence_number) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params: sample_table_row(&child_id, &child_name, &child_path),
        })
        .await
        .expect("insert child table");
        ms.execute(ExecuteParams {
            sql: "INSERT INTO cayenne_snapshot_sequence (table_id, snapshot_id, sequence_number) VALUES (?, ?, ?)",
            params: vec![
                MetastoreValue::Text(child_id.clone()),
                MetastoreValue::Text(format!("snap-{partition_value}")),
                MetastoreValue::Integer(7),
            ],
        })
        .await
        .expect("insert child snapshot sequence");
    }

    async fn table_names(ms: &SqliteMetastore) -> Vec<String> {
        ms.query(
            QueryParams {
                sql: "SELECT table_name FROM cayenne_table ORDER BY table_name",
                params: vec![],
            },
            |row| row.get_string(0),
        )
        .await
        .expect("query table names")
    }

    /// Regression test for #13241: the slice must span the partition child
    /// tables, or a restored partitioned dataset has `cayenne_partition` rows
    /// whose child tables do not exist and cannot open.
    #[tokio::test]
    async fn round_trip_restores_a_partitioned_dataset_with_its_child_tables() {
        let (ms_a, tmp_a) = fresh_metastore().await;
        let anchor_a = tmp_a.path();
        insert_dataset(
            &ms_a,
            "events",
            anchor_a,
            &[("p1", "k1", "events.dir/k1"), ("p2", "k2", "events.dir/k2")],
        )
        .await;
        let child_k1 = child_table_name("events", "k1");
        let child_k2 = child_table_name("events", "k2");

        let slice = export_dataset(ms_a.as_ref(), "events", anchor_a)
            .await
            .expect("export");

        let exported: Vec<String> = slice.tables["cayenne_table"]
            .iter()
            .filter_map(|r| match &r[CAYENNE_TABLE_NAME_INDEX] {
                SliceValue::Text(t) => Some(t.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            exported,
            vec!["events".to_string(), child_k1.clone(), child_k2.clone()],
            "slice must carry the parent and both children, parent first"
        );
        assert_eq!(
            slice.tables["cayenne_snapshot_sequence"].len(),
            2,
            "each child's dependent rows travel with it"
        );

        let (ms_b, tmp_b) = fresh_metastore().await;
        import_dataset(ms_b.as_ref(), &slice, tmp_b.path())
            .await
            .expect("import");

        let mut want = vec!["events".to_string(), child_k1, child_k2];
        want.sort();
        assert_eq!(table_names(ms_b.as_ref()).await, want);

        // The children's paths are re-anchored at the reader's data directory,
        // which is what makes the restored dataset openable there.
        let child_paths: Vec<String> = ms_b
            .query(
                QueryParams {
                    sql: "SELECT path FROM cayenne_table WHERE table_name != 'events' ORDER BY table_name",
                    params: vec![],
                },
                |row| row.get_string(0),
            )
            .await
            .expect("query child paths");
        let anchor_b = tmp_b.path().to_string_lossy().to_string();
        for path in &child_paths {
            assert!(
                path.starts_with(&anchor_b),
                "child path {path} should be under {anchor_b}"
            );
        }

        // Every partition's directory is claimed by one restored child table,
        // which is the condition `infer_existing_partitions` needs to open them.
        let partition_paths: Vec<String> = ms_b
            .query(
                QueryParams {
                    sql: "SELECT path FROM cayenne_partition ORDER BY partition_key",
                    params: vec![],
                },
                |row| row.get_string(0),
            )
            .await
            .expect("query partition paths");
        for path in &partition_paths {
            assert!(
                child_paths.contains(path),
                "partition at {path} has no restored child table; children are {child_paths:?}"
            );
        }
    }

    /// A re-import must replace the children too, not merge into whatever the
    /// reader already had under those names.
    #[tokio::test]
    async fn importing_over_an_existing_partitioned_dataset_replaces_its_children() {
        let (ms_a, tmp_a) = fresh_metastore().await;
        insert_dataset(
            &ms_a,
            "events",
            tmp_a.path(),
            &[("p1", "k1", "events.dir/k1")],
        )
        .await;
        let slice = export_dataset(ms_a.as_ref(), "events", tmp_a.path())
            .await
            .expect("export");

        // The reader already holds the same dataset, with a stale extra row on
        // the child that the slice does not carry.
        let (ms_b, tmp_b) = fresh_metastore().await;
        insert_dataset(
            &ms_b,
            "events",
            tmp_b.path(),
            &[("p1", "k1", "events.dir/k1")],
        )
        .await;
        ms_b.execute(ExecuteParams {
            sql: "INSERT INTO cayenne_snapshot_sequence (table_id, snapshot_id, sequence_number) VALUES (?, ?, ?)",
            params: vec![
                MetastoreValue::Text("tid-child-events-k1".to_string()),
                MetastoreValue::Text("stale".to_string()),
                MetastoreValue::Integer(99),
            ],
        })
        .await
        .expect("insert stale child row");

        import_dataset(ms_b.as_ref(), &slice, tmp_b.path())
            .await
            .expect("import");

        let snapshot_ids: Vec<String> = ms_b
            .query(
                QueryParams {
                    sql: "SELECT snapshot_id FROM cayenne_snapshot_sequence ORDER BY snapshot_id",
                    params: vec![],
                },
                |row| row.get_string(0),
            )
            .await
            .expect("query");
        assert_eq!(
            snapshot_ids,
            vec!["snap-k1".to_string()],
            "the stale child row must be cleared, not merged with the slice's"
        );
    }

    /// A partition whose child table cannot be matched must stop the snapshot
    /// at the writer, not at the reader: an archive published anyway becomes
    /// the store's `current-snapshot-id` and only fails during a
    /// disaster-recovery bootstrap.
    #[tokio::test]
    async fn refuses_to_export_a_partition_with_no_child_table() {
        let (ms, tmp) = fresh_metastore().await;
        insert_dataset_without_children(
            &ms,
            "events",
            tmp.path(),
            &[("p1", "k1", "events.dir/k1")],
        )
        .await;

        let err = export_dataset(ms.as_ref(), "events", tmp.path())
            .await
            .expect_err("a partition with no child table must stop the export");
        let message = err.to_string();
        assert!(message.contains("has no child table row"), "err={message}");
    }

    /// The same shape arriving from an archive — a slice an older runtime
    /// wrote before children were exported — is refused on import rather than
    /// restored into a dataset that cannot open.
    #[tokio::test]
    async fn refuses_to_import_a_partitioned_slice_with_no_child_tables() {
        let mut tables: BTreeMap<String, Vec<SliceRow>> = BTreeMap::new();
        tables.insert(
            "cayenne_table".to_string(),
            vec![
                sample_table_row("tid-events", "events", "events.dir")
                    .iter()
                    .map(SliceValue::from)
                    .collect(),
            ],
        );
        tables.insert(
            "cayenne_partition".to_string(),
            vec![
                sample_partition_row("p1", "tid-events", "events.dir/k1", "k1")
                    .iter()
                    .map(SliceValue::from)
                    .collect(),
            ],
        );
        let slice = DatasetMetastoreSlice {
            format_version: SLICE_FORMAT_VERSION,
            engine: SLICE_ENGINE.to_string(),
            dataset_name: "events".to_string(),
            exported_at_ms: 0,
            tables,
        };

        let (ms, tmp) = fresh_metastore().await;
        let err = import_dataset(ms.as_ref(), &slice, tmp.path())
            .await
            .expect_err("a partition with no child table must be refused");
        let message = err.to_string();
        assert!(message.contains("has no child table row"), "err={message}");
        assert!(
            table_names(ms.as_ref()).await.is_empty(),
            "a refused import must leave the reader's metastore untouched"
        );
    }

    /// The positional constants this module reads slice rows by are indices
    /// into `EXPECTED_TABLES`' column order. Nothing in the metastore's own
    /// schema validation checks positions — it compares column *names* — so a
    /// column inserted ahead of one of these would silently shift what
    /// `validate` reads, and a validator that fails open is worse than none.
    #[test]
    fn column_indices_match_the_expected_schema() {
        let index_of = |table: &str, column: &str| -> usize {
            let expected = EXPECTED_TABLES
                .iter()
                .find(|t| t.name == table)
                .expect("table should be in EXPECTED_TABLES");
            expected
                .columns
                .iter()
                .position(|c| *c == column)
                .expect("column should be in the expected schema")
        };

        assert_eq!(
            CAYENNE_TABLE_NAME_INDEX,
            index_of("cayenne_table", "table_name")
        );
        assert_eq!(CAYENNE_TABLE_PATH_INDEX, index_of("cayenne_table", "path"));
        assert_eq!(
            CAYENNE_PARTITION_VALUES_INDEX,
            index_of("cayenne_partition", "partition_values_json")
        );
        assert_eq!(
            CAYENNE_PARTITION_PATH_INDEX,
            index_of("cayenne_partition", "path")
        );

        assert_eq!(table_id_column_index("cayenne_table"), 0);
        assert_eq!(
            table_id_column_index("cayenne_partition"),
            index_of("cayenne_partition", "table_id")
        );

        // `path_is_relative` always follows `path`, which is what
        // `path_columns_for_table` derives its pair from.
        for table in ["cayenne_table", "cayenne_delete_file", "cayenne_partition"] {
            let (path, is_relative) = path_columns_for_table(table).expect("table stores a path");
            assert_eq!(path, index_of(table, "path"), "{table}.path");
            assert_eq!(
                is_relative,
                index_of(table, "path_is_relative"),
                "{table}.path_is_relative"
            );
        }
    }

    /// A slice may only replace the dataset it names and that dataset's own
    /// partition children. One naming any other table would delete an
    /// unrelated dataset's metadata out of the shared metastore.
    #[tokio::test]
    async fn refuses_a_slice_naming_a_table_that_is_not_its_own_child() {
        let (ms, tmp) = fresh_metastore().await;
        insert_dataset(&ms, "payroll", tmp.path(), &[]).await;

        let mut tables: BTreeMap<String, Vec<SliceRow>> = BTreeMap::new();
        tables.insert(
            "cayenne_table".to_string(),
            vec![
                sample_table_row("tid-events", "events", "events.dir")
                    .iter()
                    .map(SliceValue::from)
                    .collect(),
                // Not a partition child of `events` by any derivation.
                sample_table_row("tid-evil", "payroll", "attacker.dir")
                    .iter()
                    .map(SliceValue::from)
                    .collect(),
            ],
        );
        let slice = DatasetMetastoreSlice {
            format_version: SLICE_FORMAT_VERSION,
            engine: SLICE_ENGINE.to_string(),
            dataset_name: "events".to_string(),
            exported_at_ms: 0,
            tables,
        };

        let err = import_dataset(ms.as_ref(), &slice, tmp.path())
            .await
            .expect_err("a slice naming an unrelated table must be refused");
        assert!(
            err.to_string()
                .contains("is not one of this dataset's partition children"),
            "err={err}"
        );
        assert_eq!(
            table_names(ms.as_ref()).await,
            vec!["payroll".to_string()],
            "the unrelated dataset must survive a refused import intact"
        );
        let payroll_id: Vec<String> = ms
            .query(
                QueryParams {
                    sql: "SELECT table_id FROM cayenne_table WHERE table_name = 'payroll'",
                    params: vec![],
                },
                |row| row.get_string(0),
            )
            .await
            .expect("query");
        assert_eq!(payroll_id, vec!["tid-payroll".to_string()]);
    }

    /// The import's existing-row lookup must be able to say "absent" without
    /// saying "failed" — `query_row_values` reports both through one variant,
    /// so the bare `WHERE` form cannot distinguish a missing dataset from an
    /// interrupted or busy SELECT.
    #[tokio::test]
    async fn an_absent_table_reads_as_null_not_as_an_error() {
        let (ms, _tmp) = fresh_metastore().await;
        let txn = ms.begin_transaction().await.expect("begin");

        let bare = txn
            .query_row_values(QueryRowParams {
                sql: "SELECT table_id FROM cayenne_table WHERE table_name = ?",
                params: vec![MetastoreValue::Text("absent".to_string())],
            })
            .await;
        assert!(
            bare.is_err(),
            "the bare form reports absence as an error, which is why it cannot be propagated: {bare:?}"
        );

        let scalar = txn
            .query_row_values(QueryRowParams {
                sql: "SELECT (SELECT table_id FROM cayenne_table WHERE table_name = ?)",
                params: vec![MetastoreValue::Text("absent".to_string())],
            })
            .await
            .expect("a scalar subquery always returns one row");
        assert!(
            matches!(scalar.as_slice(), [MetastoreValue::Null]),
            "absence must arrive as one NULL value, got {scalar:?}"
        );
    }

    /// `export_dataset` collects every table's rows for the parent *and* each
    /// child, so a slice may legitimately carry `cayenne_partition` rows owned
    /// by a child. Refusing those would reject what export itself produces.
    #[tokio::test]
    async fn accepts_a_slice_whose_partition_is_owned_by_one_of_its_own_children() {
        let (ms, tmp) = fresh_metastore().await;
        insert_dataset(&ms, "events", tmp.path(), &[("p1", "k1", "events.dir/k1")]).await;
        let child = child_table_name("events", "k1");
        let child_id: Vec<String> = ms
            .query(
                QueryParams {
                    sql: "SELECT table_id FROM cayenne_table WHERE table_name = ?",
                    params: vec![MetastoreValue::Text(child.clone())],
                },
                |row| row.get_string(0),
            )
            .await
            .expect("query");
        let child_id = child_id.first().expect("the child exists").clone();

        // A partition of the child itself, as a nested export would carry.
        ms.execute(ExecuteParams {
            sql: "INSERT INTO cayenne_partition (partition_id, table_id, partition_columns_json, partition_values_json, partition_key, path, path_is_relative, record_count, file_size_bytes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params: sample_partition_row(
                "child-p",
                &child_id,
                &tmp.path().join("events.dir/k1/inner").to_string_lossy(),
                "inner",
            ),
        })
        .await
        .expect("insert child partition");

        let slice = export_dataset(ms.as_ref(), "events", tmp.path())
            .await
            .expect("export");
        assert!(
            slice.tables["cayenne_partition"].iter().any(|row| matches!(
                row.get(table_id_column_index("cayenne_partition")),
                Some(SliceValue::Text(id)) if id == &child_id
            )),
            "the exported slice must carry the child's own partition row"
        );

        slice
            .validate()
            .expect("a slice carrying its own child's partition row is valid");
    }

    #[tokio::test]
    async fn refuses_a_slice_whose_partition_is_owned_by_another_dataset() {
        let (ms, tmp) = fresh_metastore().await;
        insert_dataset(&ms, "payroll", tmp.path(), &[]).await;

        let mut tables: BTreeMap<String, Vec<SliceRow>> = BTreeMap::new();
        tables.insert(
            "cayenne_table".to_string(),
            vec![
                sample_table_row("tid-events", "events", "events.dir")
                    .iter()
                    .map(SliceValue::from)
                    .collect(),
            ],
        );
        // A partition row owned by the *local* `payroll`, not by the dataset
        // this slice is for. Every `cayenne_table` row here is legitimate, so
        // the table-row rule never sees it.
        tables.insert(
            "cayenne_partition".to_string(),
            vec![
                sample_partition_row("foreign-p", "tid-payroll", "attacker.dir", "2024")
                    .iter()
                    .map(SliceValue::from)
                    .collect(),
            ],
        );
        let slice = DatasetMetastoreSlice {
            format_version: SLICE_FORMAT_VERSION,
            engine: SLICE_ENGINE.to_string(),
            dataset_name: "events".to_string(),
            exported_at_ms: 0,
            tables,
        };

        let err = import_dataset(ms.as_ref(), &slice, tmp.path())
            .await
            .expect_err("a slice carrying another dataset's partition must be refused");
        assert!(
            err.to_string()
                .contains("is not one of the tables the slice restores"),
            "err={err}"
        );

        let foreign: Vec<String> = ms
            .query(
                QueryParams {
                    sql: "SELECT partition_id FROM cayenne_partition WHERE table_id = 'tid-payroll'",
                    params: vec![],
                },
                |row| row.get_string(0),
            )
            .await
            .expect("query");
        assert!(
            foreign.is_empty(),
            "the unrelated dataset must gain no partition from a refused import, got {foreign:?}"
        );
    }

    /// Insert one `cayenne_pending_write_back` marker for `table_id`, keyed as
    /// the production writer keys it: `cayenne_catalog`'s
    /// `blob_keyed_table_id_value`, i.e. the raw UUID bytes.
    ///
    /// Deliberately spelled out rather than read off the table's
    /// `table_id_encoding`. Taking the bind from the registry the exporter also
    /// reads would make this test agree with the exporter whatever the registry
    /// says — declaring this table `Text` then writes text, reads text, and
    /// passes, which is precisely the bug these tests exist to catch.
    async fn insert_write_back_marker(ms: &SqliteMetastore, table_id: &str, sequence_number: i64) {
        ms.execute(ExecuteParams {
            sql: "INSERT INTO cayenne_pending_write_back (table_id, pk_bytes, sequence_number) VALUES (?, ?, ?)",
            params: vec![
                MetastoreValue::Blob(crate::metastore::table_id_to_key_bytes(table_id)),
                MetastoreValue::Blob(vec![1_u8, 2, 3]),
                MetastoreValue::Integer(sequence_number),
            ],
        })
        .await
        .expect("insert write-back marker");
    }

    async fn write_back_sequences(ms: &SqliteMetastore) -> Vec<i64> {
        ms.query(
            QueryParams {
                sql: "SELECT sequence_number FROM cayenne_pending_write_back ORDER BY sequence_number",
                params: vec![],
            },
            |row| row.get_i64(0),
        )
        .await
        .expect("read write-back markers")
    }

    /// A slice must carry the undelivered write-back markers of the dataset it
    /// describes. The export used to read zero of them — it bound `TEXT` against
    /// this table's `BLOB` `table_id`, which matches nothing — so every
    /// acknowledged-but-undelivered federated write vanished from the snapshot.
    #[tokio::test]
    async fn round_trip_carries_durable_write_back_markers() {
        let (ms_a, tmp_a) = fresh_metastore().await;
        insert_dataset(
            &ms_a,
            "trips",
            tmp_a.path(),
            &[("p1", "k1", "trips/part-001")],
        )
        .await;
        insert_write_back_marker(&ms_a, "tid-trips", 7).await;

        let slice = export_dataset(ms_a.as_ref(), "trips", tmp_a.path())
            .await
            .expect("export");
        assert_eq!(
            slice.tables["cayenne_pending_write_back"].len(),
            1,
            "the slice must carry the undelivered write-back marker"
        );

        let (ms_b, tmp_b) = fresh_metastore().await;
        import_dataset(ms_b.as_ref(), &slice, tmp_b.path())
            .await
            .expect("import");
        assert_eq!(
            write_back_sequences(&ms_b).await,
            vec![7],
            "the restored dataset must inherit the marker at its own sequence"
        );
    }

    /// A reader's own write-back markers must not survive an import of the
    /// dataset they described; see
    /// [`metastore::clear_blob_keyed_marker_rows`][super::clear_blob_keyed_marker_rows]
    /// for why nothing else removes them.
    #[tokio::test]
    async fn import_clears_the_reader_s_own_write_back_markers() {
        let (ms_a, tmp_a) = fresh_metastore().await;
        insert_dataset(
            &ms_a,
            "trips",
            tmp_a.path(),
            &[("p1", "k1", "trips/part-001")],
        )
        .await;

        let slice = export_dataset(ms_a.as_ref(), "trips", tmp_a.path())
            .await
            .expect("export");
        assert!(
            slice.tables["cayenne_pending_write_back"].is_empty(),
            "the exporting side has no markers"
        );

        let (ms_b, tmp_b) = fresh_metastore().await;
        insert_dataset(
            &ms_b,
            "trips",
            tmp_b.path(),
            &[("p1", "k1", "trips/part-001")],
        )
        .await;
        insert_write_back_marker(&ms_b, "tid-trips", 11).await;
        assert_eq!(write_back_sequences(&ms_b).await, vec![11]);

        import_dataset(ms_b.as_ref(), &slice, tmp_b.path())
            .await
            .expect("import");
        assert!(
            write_back_sequences(&ms_b).await.is_empty(),
            "the reader's own markers must not survive the dataset they described"
        );
    }

    /// Register `table_name` in `ms` as a table of its own, rooted at `path`.
    async fn insert_bare_table(ms: &SqliteMetastore, table_id: &str, table_name: &str, path: &str) {
        ms.execute(ExecuteParams {
            sql: "INSERT INTO cayenne_table (table_id, table_name, path, path_is_relative, schema_json, primary_key_json, on_conflict_json, current_snapshot_id, partition_column, vortex_config_json, current_sequence_number) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params: sample_table_row(table_id, table_name, path),
        })
        .await
        .expect("insert table");
    }

    async fn table_row(ms: &SqliteMetastore, table_name: &str) -> Vec<(String, String)> {
        ms.query(
            QueryParams {
                sql: "SELECT table_id, path FROM cayenne_table WHERE table_name = ?",
                params: vec![MetastoreValue::Text(table_name.to_string())],
            },
            |row| Ok((row.get_string(0)?, row.get_string(1)?)),
        )
        .await
        .expect("read table row")
    }

    /// Import must not delete a local table just because the slice names it.
    ///
    /// The legacy child convention (`{parent}_{values}`) can spell a table an
    /// operator independently accelerated into the same metastore —
    /// partitioning `events` by year spells `events_2024` — and `validate`
    /// cannot tell the two apart, because every name and path it compares comes
    /// out of the slice. Only local discovery can, and it does: it requires the
    /// local row's path to equal the partition's. So a slice-named child that
    /// local discovery did not produce leaves the row alone and the restore
    /// rolls back on `cayenne_table(table_name)` rather than replacing an
    /// unrelated dataset. `drop_table` declines the same collision, which is
    /// what `a_table_colliding_with_the_legacy_partition_name_is_not_dropped`
    /// pins on the catalog side.
    #[tokio::test]
    async fn import_leaves_a_table_that_merely_collides_with_a_legacy_child_name() {
        let legacy =
            crate::partition_naming::legacy_partition_child_table_name("events", &["2024".into()]);

        // The writer: `events` partitioned on "2024", its child carrying the
        // legacy name an older runtime would have written.
        let (ms_a, tmp_a) = fresh_metastore().await;
        insert_dataset_without_children(
            &ms_a,
            "events",
            tmp_a.path(),
            &[("p1", "2024", "events/y2024")],
        )
        .await;
        insert_bare_table(
            &ms_a,
            "tid-legacy-child",
            &legacy,
            &tmp_a.path().join("events/y2024").to_string_lossy(),
        )
        .await;
        let slice = export_dataset(ms_a.as_ref(), "events", tmp_a.path())
            .await
            .expect("export");
        assert!(
            slice.table_names().contains(&legacy.as_str()),
            "the slice must carry the legacy-named child for this test to mean anything"
        );

        // The reader: its own `events`, plus an unrelated dataset that happens
        // to be called `events_2024` and is rooted nowhere near the partition.
        let (ms_b, tmp_b) = fresh_metastore().await;
        insert_dataset_without_children(&ms_b, "events", tmp_b.path(), &[]).await;
        let victim_path = tmp_b
            .path()
            .join("somewhere/else")
            .to_string_lossy()
            .into_owned();
        insert_bare_table(&ms_b, "tid-victim", &legacy, &victim_path).await;

        let err = import_dataset(ms_b.as_ref(), &slice, tmp_b.path())
            .await
            .expect_err("a slice colliding with an unrelated table must not be applied");
        assert!(
            err.to_string().contains("cayenne_table.table_name"),
            "expected the restore to roll back on the name collision, got: {err}"
        );
        assert_eq!(
            table_row(&ms_b, &legacy).await,
            vec![("tid-victim".to_string(), victim_path)],
            "the unrelated dataset must keep its own table_id and path"
        );
    }

    /// A slice that names a dataset but carries no `cayenne_table` row for it
    /// must be refused, not applied.
    ///
    /// Import's delete step is driven by `table_names()`, which includes
    /// `dataset_name` whether or not the slice carries a row for it, so such a
    /// payload removes the local dataset (and, by cascade, its partition
    /// children), inserts nothing, and commits — a restore that reports success
    /// while destroying the data it claimed to replace.
    #[tokio::test]
    async fn refuses_a_slice_that_carries_no_row_for_the_dataset_itself() {
        let mut tables: BTreeMap<String, Vec<SliceRow>> = BTreeMap::new();
        tables.insert("cayenne_table".to_string(), vec![]);
        tables.insert("cayenne_partition".to_string(), vec![]);
        let slice = DatasetMetastoreSlice {
            format_version: SLICE_FORMAT_VERSION,
            engine: SLICE_ENGINE.to_string(),
            dataset_name: "events".to_string(),
            exported_at_ms: 0,
            tables,
        };
        assert!(
            slice.table_names().contains(&"events"),
            "import would clear 'events' for this payload, which is what makes refusing it necessary"
        );

        let (ms, tmp) = fresh_metastore().await;
        insert_dataset(&ms, "events", tmp.path(), &[("p1", "k1", "events.dir/k1")]).await;
        let before = table_names(ms.as_ref()).await;

        let err = import_dataset(ms.as_ref(), &slice, tmp.path())
            .await
            .expect_err("a slice with no row for its own dataset must be refused");
        assert!(
            err.to_string()
                .contains("carries no readable 'cayenne_table' row"),
            "err={err}"
        );
        assert_eq!(
            table_names(ms.as_ref()).await,
            before,
            "a refused import must leave the dataset and its partition children in place"
        );
    }

    /// Two children whose paths are swapped must be refused.
    ///
    /// Every name the validator accepts is present and every partition
    /// directory has *a* child row rooted at it, so a validator that compares
    /// the names and the paths as two independent sets sees nothing wrong. The
    /// restore then roots each partition's child at the other partition's
    /// directory, and `infer_existing_partitions` — which opens a child by the
    /// name derived from the partition's own values — answers that partition
    /// with the other one's rows.
    #[tokio::test]
    async fn refuses_a_slice_whose_children_swap_their_partition_directories() {
        let (ms_a, tmp_a) = fresh_metastore().await;
        insert_dataset(
            &ms_a,
            "events",
            tmp_a.path(),
            &[("p1", "k1", "events.dir/k1"), ("p2", "k2", "events.dir/k2")],
        )
        .await;
        let mut slice = export_dataset(ms_a.as_ref(), "events", tmp_a.path())
            .await
            .expect("export");

        let rows = slice
            .tables
            .get_mut("cayenne_table")
            .expect("the slice carries cayenne_table rows");
        let children: Vec<usize> = rows
            .iter()
            .enumerate()
            .filter(|(_, row)| text_at(row, CAYENNE_TABLE_NAME_INDEX) != Some("events"))
            .map(|(i, _)| i)
            .collect();
        assert_eq!(children.len(), 2, "expected one child row per partition");
        // Only the paths move. Every name the slice carries, and every path,
        // is the one it carried before — so both sets a set-wise validator
        // compares are untouched and only the pairing between them is wrong.
        let (first, second) = (children[0], children[1]);
        let first_path = rows[first][CAYENNE_TABLE_PATH_INDEX].clone();
        rows[first][CAYENNE_TABLE_PATH_INDEX] = rows[second][CAYENNE_TABLE_PATH_INDEX].clone();
        rows[second][CAYENNE_TABLE_PATH_INDEX] = first_path;

        let (ms_b, tmp_b) = fresh_metastore().await;
        let err = import_dataset(ms_b.as_ref(), &slice, tmp_b.path())
            .await
            .expect_err("a slice whose children swap their partitions must be refused");
        assert!(
            err.to_string()
                .contains("from another partition's directory"),
            "err={err}"
        );
        assert!(
            table_names(ms_b.as_ref()).await.is_empty(),
            "a refused import must leave the reader's metastore untouched"
        );
    }

    /// A slice is written at the lowest version that expresses it, and the
    /// bump is driven by the rows an older import cannot clear — *not* by
    /// whether the dataset is partitioned.
    ///
    /// Both sources are covered, because each was found separately and the
    /// second only after the first was fixed. Measured against `trunk`'s own
    /// `import_dataset`, two consecutive imports into one metastore gave:
    ///
    /// ```text
    /// partition children: #1 -> Ok  #2 -> UNIQUE constraint failed: cayenne_table.table_name
    /// write-back markers: #1 -> Ok  #2 -> UNIQUE constraint failed: cayenne_pending_write_back.table_id, …pk_bytes
    /// ```
    #[tokio::test]
    async fn a_slice_declares_the_lowest_version_that_expresses_it() {
        let (ms, tmp) = fresh_metastore().await;
        insert_dataset(&ms, "flat", tmp.path(), &[]).await;
        let flat = export_dataset(ms.as_ref(), "flat", tmp.path())
            .await
            .expect("export the unpartitioned dataset");
        assert_eq!(
            flat.format_version, SLICE_FORMAT_VERSION,
            "a slice carrying nothing an older import leaves behind must stay readable by that import"
        );

        insert_dataset(&ms, "events", tmp.path(), &[("p1", "k1", "events.dir/k1")]).await;
        let partitioned = export_dataset(ms.as_ref(), "events", tmp.path())
            .await
            .expect("export the partitioned dataset");
        assert_eq!(
            partitioned.format_version, SLICE_FORMAT_VERSION_FULL_CLEANUP,
            "a slice carrying partition children must declare the version that refuses an older reader"
        );

        // Unpartitioned, but carrying a write-back marker: the same bump, from
        // the other source. `flat` above is the control — the only difference
        // between the two exports is the marker row.
        insert_write_back_marker(&ms, "tid-flat", 11).await;
        let with_markers = export_dataset(ms.as_ref(), "flat", tmp.path())
            .await
            .expect("re-export the unpartitioned dataset");
        assert_eq!(
            with_markers
                .tables
                .get("cayenne_pending_write_back")
                .map_or(0, Vec::len),
            1,
            "the marker must be in the slice for this case to mean anything"
        );
        assert_eq!(
            with_markers.format_version, SLICE_FORMAT_VERSION_FULL_CLEANUP,
            "an older import clears only cayenne_insert_record by hand, so a slice carrying any other blob-keyed row must declare the newer version"
        );

        // Every version this build writes, it also reads.
        for slice in [&flat, &partitioned, &with_markers] {
            let bytes = slice.to_json_bytes().expect("serialize");
            let parsed = DatasetMetastoreSlice::from_json_bytes(&bytes)
                .expect("this build reads every version it writes");
            assert_eq!(parsed.format_version, slice.format_version);
        }
    }

    /// A slice may not carry rows an older import cannot clear while declaring
    /// the older version.
    ///
    /// The bump protects that reader, and that reader decides purely on the
    /// declared version — so a payload disagreeing with its own version would
    /// reach exactly the build the bump exists for. Both sources are checked,
    /// since each is a separate way to build the disagreement.
    #[tokio::test]
    async fn refuses_rows_an_older_import_cannot_clear_under_the_older_version() {
        let (ms, tmp) = fresh_metastore().await;
        insert_dataset(&ms, "events", tmp.path(), &[("p1", "k1", "events.dir/k1")]).await;
        insert_dataset(&ms, "flat", tmp.path(), &[]).await;
        insert_write_back_marker(&ms, "tid-flat", 11).await;

        for dataset in ["events", "flat"] {
            let mut slice = export_dataset(ms.as_ref(), dataset, tmp.path())
                .await
                .expect("export");
            assert_eq!(
                slice.format_version, SLICE_FORMAT_VERSION_FULL_CLEANUP,
                "'{dataset}' must export at the newer version for this case to mean anything"
            );
            slice.format_version = SLICE_FORMAT_VERSION;

            let (reader, reader_tmp) = fresh_metastore().await;
            let err = import_dataset(reader.as_ref(), &slice, reader_tmp.path())
                .await
                .expect_err("a slice declaring the older version must be refused");
            assert!(
                err.to_string()
                    .contains("import does not clear, but declares format_version"),
                "dataset={dataset} err={err}"
            );
            assert!(
                table_names(reader.as_ref()).await.is_empty(),
                "a refused import must leave the reader's metastore untouched"
            );
        }
    }

    /// `OLDER_IMPORT_CLEARS` is matched against `ExpectedTable::name` by string,
    /// so a rename of that table would silently stop excluding it and flip every
    /// marker-carrying slice from one format version to the other with no
    /// compile error. Pin the name against the registry it is matched in.
    #[test]
    fn older_import_clears_names_a_blob_keyed_table() {
        assert!(
            super::super::blob_keyed_tables().any(|table| table.name == OLDER_IMPORT_CLEARS),
            "OLDER_IMPORT_CLEARS must name one of the blob-keyed tables it is filtered out of"
        );
    }

    /// The refusal a reader too old for a slice shows its operator. Built by a
    /// pure function and asserted here so a reword cannot quietly drop the
    /// version range or the action to take.
    #[test]
    fn the_unsupported_version_message_names_the_range_and_the_fix() {
        let message = unsupported_version_message(99);
        assert!(message.contains("format_version 99"), "{message}");
        assert!(
            message.contains(&format!(
                "reads {SLICE_FORMAT_VERSION} to {SLICE_FORMAT_VERSION_FULL_CLEANUP}"
            )),
            "{message}"
        );
        assert!(message.contains("upgrade the runtime"), "{message}");
    }
}
