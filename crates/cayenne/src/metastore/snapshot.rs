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

use std::collections::BTreeMap;
use std::path::Path;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::{Deserialize, Serialize};

use super::{
    EXPECTED_TABLES, ExecuteParams, MetastoreBackend, MetastoreValue, QueryParams, QueryRowParams,
};
use crate::catalog::{CatalogError, CatalogResult};

/// Slice format that carries one dataset and no per-partition child tables.
pub const SLICE_FORMAT_BASE: u32 = 1;

/// Slice format that additionally carries the dataset's per-partition child
/// `cayenne_table` rows and their dependent rows.
///
/// A reader that understands only [`SLICE_FORMAT_BASE`] would import the parent
/// and its `cayenne_partition` rows while dropping the children on the floor,
/// which is precisely the unrestorable state this format exists to prevent — so
/// a slice that carries children declares this version and an older reader
/// refuses it outright.
pub const SLICE_FORMAT_PARTITIONED: u32 = 2;

/// Newest slice format this build writes. A slice is written at the *lowest*
/// version that can express it (see [`export_dataset`]), so an unpartitioned
/// dataset still produces a [`SLICE_FORMAT_BASE`] slice that older readers
/// accept.
pub const SLICE_FORMAT_VERSION: u32 = SLICE_FORMAT_PARTITIONED;

/// Oldest slice format this build reads.
pub const SLICE_FORMAT_MIN_SUPPORTED: u32 = SLICE_FORMAT_BASE;

/// Whether this build can read a slice written at `format_version`.
#[must_use]
pub fn slice_format_is_supported(format_version: u32) -> bool {
    (SLICE_FORMAT_MIN_SUPPORTED..=SLICE_FORMAT_VERSION).contains(&format_version)
}

/// Engine identifier embedded in slices to detect cross-engine misuse.
pub const SLICE_ENGINE: &str = "cayenne";

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
    /// Slice format version. Must equal [`SLICE_FORMAT_VERSION`] for this build.
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
        if !slice_format_is_supported(slice.format_version) {
            return Err(CatalogError::Database {
                message: format!(
                    "unsupported metastore slice format_version {} (this build understands {SLICE_FORMAT_MIN_SUPPORTED}..={SLICE_FORMAT_VERSION})",
                    slice.format_version
                ),
            });
        }
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
    match table_name {
        "cayenne_table" | "cayenne_delete_file" => Some((2, 3)),
        "cayenne_partition" => Some((5, 6)),
        _ => None,
    }
}

/// Returns the column index that holds `table_id` for each metastore table.
/// `cayenne_table` itself stores it at index 0; child tables store it at index 1.
///
/// Currently informational — wholesale-replace import preserves the slice's
/// own `table_id` values verbatim, so no remap is needed. Kept for the future
/// case where we might want to re-key on import.
#[expect(
    dead_code,
    reason = "retained for future re-keying on import; see doc above"
)]
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

/// Column index of `table_name` in a `cayenne_table` row. Follows
/// [`EXPECTED_TABLES`]' column order.
const CAYENNE_TABLE_NAME_INDEX: usize = 1;
/// Column index of `path` in a `cayenne_table` row.
const CAYENNE_TABLE_PATH_INDEX: usize = 2;
/// Column index of `path` in a `cayenne_partition` row.
const CAYENNE_PARTITION_PATH_INDEX: usize = 5;

/// One partition of a partitioned table, as much of it as child-table discovery
/// and slice validation need.
struct PartitionRef {
    /// Composite key recorded in `cayenne_partition.partition_key`.
    partition_key: String,
    /// The partition's value strings, for the legacy child-table name.
    partition_values: Vec<String>,
    /// The partition's own directory. A child table is rooted at it.
    path: String,
}

/// The partitions `dataset_name` owns, in metastore order.
async fn partitions_of(
    metastore: &impl MetastoreBackend,
    table_id: &str,
) -> CatalogResult<Vec<PartitionRef>> {
    let raw: Vec<(String, String, String)> = metastore
        .query(
            QueryParams {
                sql: "SELECT partition_key, partition_values_json, path FROM cayenne_partition WHERE table_id = ?",
                params: vec![MetastoreValue::Text(table_id.to_string())],
            },
            |row| Ok((row.get_string(0)?, row.get_string(1)?, row.get_string(2)?)),
        )
        .await?;

    raw.into_iter()
        .map(|(partition_key, values_json, path)| {
            // A partition whose value list will not parse cannot be matched to
            // its child table, and a slice that silently omits a child is the
            // defect this discovery exists to close — so refuse rather than
            // export an incomplete slice.
            let partition_values: Vec<String> =
                serde_json::from_str(&values_json).map_err(|e| CatalogError::Database {
                    message: format!(
                        "cannot export metastore slice: partition '{partition_key}' has unreadable partition_values_json: {e}"
                    ),
                })?;
            Ok(PartitionRef {
                partition_key,
                partition_values,
                path,
            })
        })
        .collect()
}

/// `table_id`s of `dataset_name`'s per-partition child tables.
///
/// A partitioned table's partitions are catalog tables of their own: each has
/// its own `cayenne_table` row, its own `table_id`, and its own dependent rows.
/// Matched by [`crate::partition_naming::PARTITION_CHILD_LOOKUP_SQL`], the same
/// rule the catalog drops children by, so export and drop cannot disagree about
/// what belongs to the dataset. Empty for an unpartitioned table.
async fn partition_child_table_ids(
    metastore: &impl MetastoreBackend,
    dataset_name: &str,
    partitions: &[PartitionRef],
) -> CatalogResult<Vec<String>> {
    let mut child_ids = Vec::new();
    for partition in partitions {
        let matched: Vec<String> = metastore
            .query(
                QueryParams {
                    sql: crate::partition_naming::PARTITION_CHILD_LOOKUP_SQL,
                    params: vec![
                        MetastoreValue::Text(crate::partition_naming::partition_child_table_name(
                            dataset_name,
                            &partition.partition_key,
                        )),
                        MetastoreValue::Text(
                            crate::partition_naming::legacy_partition_child_table_name(
                                dataset_name,
                                &partition.partition_values,
                            ),
                        ),
                        MetastoreValue::Text(partition.path.clone()),
                    ],
                },
                |row| row.get_string(0),
            )
            .await?;
        // `cayenne_table(table_name)` is unique and a partition owns its
        // directory, so a child matches at most one partition — but exporting
        // one twice would duplicate its rows and fail the import's INSERT on
        // that same uniqueness, so do not depend on it holding.
        for id in matched {
            if !child_ids.contains(&id) {
                child_ids.push(id);
            }
        }
    }
    Ok(child_ids)
}

/// Read every row of `expected` belonging to `table_id`, as slice rows.
async fn rows_for_table_id(
    metastore: &impl MetastoreBackend,
    expected: &super::ExpectedTable,
    table_id: &str,
) -> CatalogResult<Vec<SliceRow>> {
    let n_columns = expected.columns.len();
    // `cayenne_insert_record.table_id` is stored as the raw-UUID-bytes BLOB
    // (see `metastore::table_id_to_key_bytes`), so its filter must bind a BLOB
    // — a TEXT bind never matches a BLOB column in SQLite. Every other table
    // keeps `table_id` as TEXT.
    let table_id_param = if expected.name == "cayenne_insert_record" {
        MetastoreValue::Blob(super::table_id_to_key_bytes(table_id))
    } else {
        MetastoreValue::Text(table_id.to_string())
    };
    let sql = format!(
        "SELECT {} FROM {} WHERE table_id = ?",
        expected.columns.join(", "),
        expected.name
    );
    metastore
        .query(
            QueryParams {
                sql: &sql,
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
    let partitions = partitions_of(metastore, &table_id).await?;
    let child_ids = partition_child_table_ids(metastore, dataset_name, &partitions).await?;

    // Parent first, so the parent's `cayenne_table` row leads the slice.
    let table_ids: Vec<&str> = std::iter::once(table_id.as_str())
        .chain(child_ids.iter().map(String::as_str))
        .collect();

    let mut tables: BTreeMap<String, Vec<SliceRow>> = BTreeMap::new();

    for expected in EXPECTED_TABLES {
        let mut rows: Vec<SliceRow> = Vec::new();
        for id in &table_ids {
            rows.extend(rows_for_table_id(metastore, expected, id).await?);
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

    // Write at the lowest version that can express this slice, so a dataset
    // with no partition children still produces a slice an older reader reads.
    let format_version = if child_ids.is_empty() {
        SLICE_FORMAT_BASE
    } else {
        SLICE_FORMAT_PARTITIONED
    };

    Ok(DatasetMetastoreSlice {
        format_version,
        engine: SLICE_ENGINE.to_string(),
        dataset_name: dataset_name.to_string(),
        exported_at_ms: chrono::Utc::now().timestamp_millis(),
        tables,
    })
}

/// The `table_name`s carried by the slice's `cayenne_table` rows, parent first.
///
/// Import replaces whole tables, so these are exactly the local rows it must
/// clear before inserting. `slice.dataset_name` is included even when the slice
/// carries no `cayenne_table` row for it, so a malformed slice still clears the
/// dataset it claims to replace rather than merging into it.
fn table_names_in_slice(slice: &DatasetMetastoreSlice) -> Vec<String> {
    let mut names = vec![slice.dataset_name.clone()];
    for row in slice.tables.get("cayenne_table").into_iter().flatten() {
        if let Some(SliceValue::Text(name)) = row.get(CAYENNE_TABLE_NAME_INDEX)
            && !names.contains(name)
        {
            names.push(name.clone());
        }
    }
    names
}

/// Refuse a slice whose partitions have no child tables to be restored with.
///
/// Every `cayenne_partition` row names a directory that a child `cayenne_table`
/// row is rooted at. A slice carrying the partition but not the child restores
/// to a dataset that cannot open — `infer_existing_partitions` propagates
/// `TableNotFound` — so this refuses before the transaction opens rather than
/// leaving that state behind. Slices written before the child rows were
/// exported (format [`SLICE_FORMAT_BASE`], from a partitioned dataset) fail
/// here, which is the honest outcome: they were never restorable.
fn validate_partition_children(slice: &DatasetMetastoreSlice) -> CatalogResult<()> {
    let partition_paths: Vec<&String> = slice
        .tables
        .get("cayenne_partition")
        .into_iter()
        .flatten()
        .filter_map(|row| match row.get(CAYENNE_PARTITION_PATH_INDEX) {
            Some(SliceValue::Text(path)) => Some(path),
            _ => None,
        })
        .collect();
    if partition_paths.is_empty() {
        return Ok(());
    }

    // Both sides were rewritten relative to the exporter's anchor, so the
    // child's path is comparable to its partition's without re-anchoring.
    let child_paths: Vec<&String> = slice
        .tables
        .get("cayenne_table")
        .into_iter()
        .flatten()
        .filter(|row| {
            !matches!(
                row.get(CAYENNE_TABLE_NAME_INDEX),
                Some(SliceValue::Text(name)) if *name == slice.dataset_name
            )
        })
        .filter_map(|row| match row.get(CAYENNE_TABLE_PATH_INDEX) {
            Some(SliceValue::Text(path)) => Some(path),
            _ => None,
        })
        .collect();

    if let Some(orphan) = partition_paths
        .iter()
        .find(|path| !child_paths.contains(path))
    {
        return Err(CatalogError::Database {
            message: format!(
                "refusing to import metastore slice for dataset '{}': its partition at '{orphan}' has no child table row in the slice, so the restored dataset could not be opened. Re-create the snapshot with a runtime that exports partition child tables (slice format {SLICE_FORMAT_PARTITIONED})",
                slice.dataset_name
            ),
        });
    }

    Ok(())
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
    if !slice_format_is_supported(slice.format_version) {
        return Err(CatalogError::Database {
            message: format!(
                "refusing to import metastore slice: unsupported format_version {} (this build understands {SLICE_FORMAT_MIN_SUPPORTED}..={SLICE_FORMAT_VERSION})",
                slice.format_version
            ),
        });
    }
    if slice.engine != SLICE_ENGINE {
        return Err(CatalogError::Database {
            message: format!(
                "refusing to import metastore slice: engine '{}' != '{SLICE_ENGINE}'",
                slice.engine
            ),
        });
    }

    validate_partition_children(slice)?;

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
        let local_partitions = partitions_of(metastore, &local_parent_id).await?;
        stale_child_ids =
            partition_child_table_ids(metastore, &slice.dataset_name, &local_partitions).await?;
    }

    let txn = metastore.begin_transaction().await?;

    for child_id in &stale_child_ids {
        txn.execute(ExecuteParams {
            sql: "DELETE FROM cayenne_insert_record WHERE table_id = ?",
            params: vec![MetastoreValue::Blob(super::table_id_to_key_bytes(child_id))],
        })
        .await?;
        txn.execute(ExecuteParams {
            sql: "DELETE FROM cayenne_table WHERE table_id = ?",
            params: vec![MetastoreValue::Text(child_id.clone())],
        })
        .await?;
    }

    // Wholesale-replace any existing rows for every table the slice carries —
    // the dataset and, for a partitioned dataset, each of its per-partition
    // child tables. `cayenne_table`'s `ON DELETE CASCADE` clears the dependent
    // rows of every table whose foreign key still references it — but
    // `cayenne_insert_record` no longer has that foreign key (its `table_id` is
    // a raw-bytes BLOB; see `metastore::table_id_to_key_bytes`), so resolve each
    // existing `table_id` and clear its insert-records explicitly first, inside
    // the same transaction, before the row is removed.
    for table_name in table_names_in_slice(slice) {
        if let Ok(values) = txn
            .query_row_values(QueryRowParams {
                sql: "SELECT table_id FROM cayenne_table WHERE table_name = ?",
                params: vec![MetastoreValue::Text(table_name.clone())],
            })
            .await
            && let Some(MetastoreValue::Text(existing_table_id)) = values.into_iter().next()
        {
            txn.execute(ExecuteParams {
                sql: "DELETE FROM cayenne_insert_record WHERE table_id = ?",
                params: vec![MetastoreValue::Blob(super::table_id_to_key_bytes(
                    &existing_table_id,
                ))],
            })
            .await?;
        }

        txn.execute(ExecuteParams {
            sql: "DELETE FROM cayenne_table WHERE table_name = ?",
            params: vec![MetastoreValue::Text(table_name)],
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

    fn sample_partition_row(
        partition_id: &str,
        table_id: &str,
        abs_path: &str,
        partition_key: &str,
    ) -> Vec<MetastoreValue> {
        vec![
            MetastoreValue::Text(partition_id.to_string()),
            MetastoreValue::Text(table_id.to_string()),
            MetastoreValue::Text("[]".to_string()),
            MetastoreValue::Text("[]".to_string()),
            MetastoreValue::Text(partition_key.to_string()),
            MetastoreValue::Text(abs_path.to_string()),
            MetastoreValue::Bool(false),
            MetastoreValue::Integer(100),
            MetastoreValue::Integer(1024),
        ]
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
        assert_eq!(slice.format_version, SLICE_FORMAT_PARTITIONED);
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
                sample_table_row(
                    "tid-trips-newk",
                    &crate::partition_naming::partition_child_table_name("trips", "newk"),
                    "new1",
                )
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
            format_version: SLICE_FORMAT_VERSION,
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
        partition_key: &str,
        anchor: &Path,
        dir: &str,
    ) -> String {
        let child_name = crate::partition_naming::partition_child_table_name(parent, partition_key);
        let child_id = format!("tid-child-{parent}-{partition_key}");
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
                MetastoreValue::Text(format!("snap-{partition_key}")),
                MetastoreValue::Integer(7),
            ],
        })
        .await
        .expect("insert child snapshot sequence");
        child_name
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
        let child_k1 = crate::partition_naming::partition_child_table_name("events", "k1");
        let child_k2 = crate::partition_naming::partition_child_table_name("events", "k2");

        let slice = export_dataset(ms_a.as_ref(), "events", anchor_a)
            .await
            .expect("export");

        assert_eq!(
            slice.format_version, SLICE_FORMAT_PARTITIONED,
            "a slice carrying child tables must declare the partitioned format so an older reader refuses it"
        );
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

    /// A slice written before child tables were exported is refused rather than
    /// restored into a dataset that cannot open.
    #[tokio::test]
    async fn refuses_a_partitioned_slice_with_no_child_tables() {
        let (ms_a, tmp_a) = fresh_metastore().await;
        insert_dataset_without_children(
            &ms_a,
            "events",
            tmp_a.path(),
            &[("p1", "k1", "events.dir/k1")],
        )
        .await;
        let slice = export_dataset(ms_a.as_ref(), "events", tmp_a.path())
            .await
            .expect("export");
        assert_eq!(
            slice.format_version, SLICE_FORMAT_BASE,
            "a dataset whose partitions have no child tables is still a base-format slice"
        );

        let (ms_b, tmp_b) = fresh_metastore().await;
        let err = import_dataset(ms_b.as_ref(), &slice, tmp_b.path())
            .await
            .expect_err("a partition with no child table must be refused");
        let message = err.to_string();
        assert!(
            message.contains("has no child table row in the slice"),
            "err={message}"
        );
        assert!(
            table_names(ms_b.as_ref()).await.is_empty(),
            "a refused import must leave the reader's metastore untouched"
        );
    }

    #[tokio::test]
    async fn reads_a_base_format_slice_and_refuses_a_newer_one() {
        assert!(slice_format_is_supported(SLICE_FORMAT_BASE));
        assert!(slice_format_is_supported(SLICE_FORMAT_PARTITIONED));
        assert!(!slice_format_is_supported(SLICE_FORMAT_VERSION + 1));
        assert!(!slice_format_is_supported(0));
    }
}
