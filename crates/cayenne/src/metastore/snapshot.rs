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

use super::{EXPECTED_TABLES, ExecuteParams, MetastoreBackend, MetastoreValue, QueryParams};
use crate::catalog::{CatalogError, CatalogResult};

/// Current slice format version. Incremented on incompatible format changes.
pub const SLICE_FORMAT_VERSION: u32 = 1;

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
        if slice.format_version != SLICE_FORMAT_VERSION {
            return Err(CatalogError::Database {
                message: format!(
                    "unsupported metastore slice format_version {} (this build understands only {SLICE_FORMAT_VERSION})",
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

    let mut tables: BTreeMap<String, Vec<SliceRow>> = BTreeMap::new();

    for expected in EXPECTED_TABLES {
        let n_columns = expected.columns.len();
        let (sql, params) = if expected.name == "cayenne_table" {
            (
                format!(
                    "SELECT {} FROM {} WHERE table_name = ?",
                    expected.columns.join(", "),
                    expected.name
                ),
                vec![MetastoreValue::Text(dataset_name.to_string())],
            )
        } else {
            (
                format!(
                    "SELECT {} FROM {} WHERE table_id = ?",
                    expected.columns.join(", "),
                    expected.name
                ),
                vec![MetastoreValue::Text(table_id.clone())],
            )
        };

        let path_cols = path_columns_for_table(expected.name);

        let rows: Vec<SliceRow> = metastore
            .query(QueryParams { sql: &sql, params }, move |row| {
                let mut out = Vec::with_capacity(n_columns);
                for i in 0..n_columns {
                    out.push(SliceValue::from(&row.get_value(i)?));
                }
                Ok(out)
            })
            .await?;

        // Rewrite path columns to be relative to anchor. If `make_relative`
        // could not strip the anchor (path is outside `data_dir_anchor`), it
        // returns the original absolute path — in that case we leave
        // `path_is_relative=false` so the slice stays internally consistent.
        let rows: Vec<SliceRow> = if let Some((path_idx, rel_idx)) = path_cols {
            rows.into_iter()
                .map(|mut r| {
                    if let Some(SliceValue::Text(abs)) = r.get(path_idx).cloned() {
                        let rel = make_relative(&abs, data_dir_anchor);
                        let is_relative = rel != abs;
                        r[path_idx] = SliceValue::Text(rel);
                        r[rel_idx] = SliceValue::Bool(is_relative);
                    }
                    r
                })
                .collect()
        } else {
            rows
        };

        tables.insert(expected.name.to_string(), rows);
    }

    Ok(DatasetMetastoreSlice {
        format_version: SLICE_FORMAT_VERSION,
        engine: SLICE_ENGINE.to_string(),
        dataset_name: dataset_name.to_string(),
        exported_at_ms: chrono::Utc::now().timestamp_millis(),
        tables,
    })
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
    if slice.format_version != SLICE_FORMAT_VERSION {
        return Err(CatalogError::Database {
            message: format!(
                "refusing to import metastore slice: unsupported format_version {}",
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

    let txn = metastore.begin_transaction().await?;

    // Wholesale-replace any existing rows for this dataset.
    txn.execute(ExecuteParams {
        sql: "DELETE FROM cayenne_table WHERE table_name = ?",
        params: vec![MetastoreValue::Text(slice.dataset_name.clone())],
    })
    .await?;

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

    async fn insert_dataset(
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
        assert_eq!(slice.format_version, SLICE_FORMAT_VERSION);
        assert_eq!(slice.engine, SLICE_ENGINE);
        assert_eq!(slice.tables["cayenne_table"].len(), 1);
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
}
