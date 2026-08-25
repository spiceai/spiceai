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

//! Cayenne-specific snapshot engine.
//!
//! Cayenne stores per-table metadata in a shared SQLite/libSQL database
//! (`cayenne.db`). Shipping that file as part of a Cayenne snapshot is
//! problematic for three reasons:
//!
//! 1. **Path portability** (#10642): `cayenne_table.path`,
//!    `cayenne_partition.path` and `cayenne_delete_file.path` store absolute
//!    filesystem paths from the writer; readers with a different data
//!    directory cannot resolve them.
//!
//! 2. **Multi-dataset clobbering**: `cayenne.db` contains rows for *every*
//!    dataset sharing the metadata directory. Two datasets snapshotting the
//!    same `cayenne.db` and extracting on a fresh reader would each clobber
//!    the other's metastore rows, which is why
//!    `validate_cayenne_snapshot_consistency` currently rejects multi-dataset
//!    metastore directories.
//!
//! 3. **Init race / sidecars** (#10649): the reader's eager metastore
//!    initialization opens `cayenne.db`, creating `cayenne.db-wal` /
//!    `-shm` sidecars before snapshot extraction runs, breaking the
//!    archive's checksum verification.
//!
//! `CayenneSnapshotEngine` fixes all three by **never** archiving
//! `cayenne.db*`. Instead, on the create side it serializes a per-dataset
//! metastore "slice" (versioned JSON, see
//! [`cayenne::metastore::snapshot::DatasetMetastoreSlice`]) and inserts it
//! into the tar at a well-known archive path. On the extract side it reads
//! the slice back and atomically imports it into the local metastore.
//!
//! Path columns in the slice are rewritten relative to the writer's data
//! directory at export time and re-anchored at the reader's data directory
//! on import, making the snapshot portable across nodes with different
//! local layouts.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use cayenne::MetadataCatalog;
use cayenne::metastore::snapshot::{DatasetMetastoreSlice, SliceValue};
use runtime_acceleration::snapshot::engine::{
    DirectoryArchiveExtra, DirectorySnapshotPlan, SnapshotEngine, SnapshotEngineError,
};
use snafu::{ResultExt, Snafu};
use tokio::fs;

/// Well-known archive entry path for a Cayenne dataset's metastore slice.
/// The dataset name is included so multiple per-dataset slices can coexist
/// in the same tar in the (currently unused, but designed-for) future where
/// a snapshot covers more than one dataset.
/// Archive path for the per-dataset metastore slice JSON.
///
/// Uses the `metadata/` prefix so it lines up with
/// `AccelerationLayout::cayenne`'s metadata-directory mapping. On extract,
/// `download_to_directories` writes it under the local metadata directory as
/// `<metadata_dir>/<dataset_name>.slice.json`.
fn slice_archive_path(dataset_name: &str) -> String {
    format!("metadata/{dataset_name}.slice.json")
}

/// File names (relative to `metadata_dir`) that must be excluded from the
/// archive. Cayenne always opens the metastore in WAL journal mode, so the
/// `-wal` and `-shm` sidecars may be present alongside `cayenne.db`.
const METASTORE_FILES: &[&str] = &["cayenne.db", "cayenne.db-wal", "cayenne.db-shm"];

/// Position of `column` within `table`'s rows, per the positional contract
/// [`DatasetMetastoreSlice`] declares (rows follow [`EXPECTED_TABLES`] column order).
/// Looked up by name so a schema addition cannot silently shift a hard-coded index onto
/// the wrong column.
fn column_index(table: &str, column: &str) -> Option<usize> {
    cayenne::metastore::EXPECTED_TABLES
        .iter()
        .find(|t| t.name == table)
        .and_then(|t| t.columns.iter().position(|c| *c == column))
}

fn slice_text(row: &[SliceValue], index: usize) -> Option<&str> {
    match row.get(index) {
        Some(SliceValue::Text(value)) => Some(value.as_str()),
        _ => None,
    }
}

/// Every data file the slice's **current** snapshot references, resolved to a local path
/// under `anchor`.
///
/// The scan resolves a manifest row as `{table_path}/{table_id}/{snapshot_id}/{file_path}`
/// (`CayenneTableProvider::snapshot_dir_path`), and `file_path` is a bare file name rather
/// than a path, which is why it carries no `path_is_relative` flag of its own. Only the
/// current snapshot matters: rows for retired snapshots describe directories a restore
/// never reads.
///
/// A slice with no current snapshot (a table that has never published) references
/// nothing, which is a valid answer rather than an error.
fn referenced_data_files(
    slice: &DatasetMetastoreSlice,
    anchor: &std::path::Path,
) -> Result<Vec<PathBuf>, String> {
    let table_row = slice
        .tables
        .get("cayenne_table")
        .and_then(|rows| rows.first())
        .ok_or_else(|| "the slice carries no `cayenne_table` row".to_string())?;

    let idx = |table: &str, column: &str| -> Result<usize, String> {
        column_index(table, column)
            .ok_or_else(|| format!("`{table}` has no `{column}` column in this build"))
    };

    let table_id = slice_text(table_row, idx("cayenne_table", "table_id")?)
        .ok_or_else(|| "the slice's `cayenne_table` row has no `table_id`".to_string())?;
    let table_path = slice_text(table_row, idx("cayenne_table", "path")?)
        .ok_or_else(|| "the slice's `cayenne_table` row has no `path`".to_string())?;
    let path_is_relative = matches!(
        table_row.get(idx("cayenne_table", "path_is_relative")?),
        Some(SliceValue::Bool(true))
    );
    let Some(current_snapshot_id) =
        slice_text(table_row, idx("cayenne_table", "current_snapshot_id")?)
    else {
        return Ok(Vec::new());
    };

    let table_root = if path_is_relative {
        anchor.join(table_path)
    } else {
        PathBuf::from(table_path)
    };
    let snapshot_dir = table_root.join(table_id).join(current_snapshot_id);

    let snapshot_id_idx = idx("cayenne_snapshot_file", "snapshot_id")?;
    let file_path_idx = idx("cayenne_snapshot_file", "file_path")?;

    let mut files: Vec<PathBuf> = slice
        .tables
        .get("cayenne_snapshot_file")
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .filter(|row| slice_text(row, snapshot_id_idx) == Some(current_snapshot_id))
        .filter_map(|row| slice_text(row, file_path_idx))
        .map(|file| snapshot_dir.join(file))
        .collect();

    // Deletion vectors too. A data file that comes back without the deletion vector that
    // hides its dead rows is worse than a missing data file: a missing KEY-based vector is
    // *tolerated* by the scan rather than failing it (see
    // `cayenne::provider::delete::vector_io`), so those deletions silently stop applying and
    // the deleted rows come back. Unlike the manifest's bare file names, these carry a full
    // path plus the same `path_is_relative` flag `cayenne_table` uses, so they resolve the
    // same way.
    let delete_path_idx = idx("cayenne_delete_file", "path")?;
    let delete_relative_idx = idx("cayenne_delete_file", "path_is_relative")?;
    files.extend(
        slice
            .tables
            .get("cayenne_delete_file")
            .map(Vec::as_slice)
            .unwrap_or_default()
            .iter()
            .filter_map(|row| {
                let path = slice_text(row, delete_path_idx)?;
                Some(
                    if matches!(row.get(delete_relative_idx), Some(SliceValue::Bool(true))) {
                        anchor.join(path)
                    } else {
                        PathBuf::from(path)
                    },
                )
            }),
    );

    Ok(files)
}

/// Map a local path to the archive path it would be written under, given the
/// `(directory, archive_prefix)` pairs the archive was built from.
///
/// Returns `None` for a path under none of them — which is itself a finding: the archive
/// cannot contain a file it was never asked to walk.
fn archive_path_for(path: &Path, dirs: &[(PathBuf, String)]) -> Option<String> {
    dirs.iter().find_map(|(dir, prefix)| {
        let relative = path.strip_prefix(dir).ok()?;
        let relative = relative.to_string_lossy();
        Some(if prefix.is_empty() {
            relative.into_owned()
        } else {
            format!("{prefix}{relative}")
        })
    })
}

/// The expected files that the finished archive does not contain, by their local paths.
fn missing_members(
    expected: &[PathBuf],
    dirs: &[(PathBuf, String)],
    members: &HashSet<String>,
) -> Vec<String> {
    expected
        .iter()
        .filter(|path| {
            archive_path_for(path, dirs).is_none_or(|archive_path| !members.contains(&archive_path))
        })
        .map(|path| path.display().to_string())
        .collect()
}

/// The subset of `files` not present on disk, capped for a readable message, with the total.
///
/// Used on the RESTORE side, where the archive is already gone and the extracted tree is
/// the only thing to check.
async fn absent_on_disk(files: &[PathBuf]) -> (Vec<String>, usize) {
    const MAX_NAMED: usize = 5;

    let mut present_by_dir: HashMap<PathBuf, HashSet<std::ffi::OsString>> = HashMap::new();
    for dir in files.iter().filter_map(|f| f.parent()) {
        if present_by_dir.contains_key(dir) {
            continue;
        }
        let dir_owned = dir.to_path_buf();
        let entries = tokio::task::spawn_blocking(move || {
            let mut names = HashSet::new();
            if let Ok(read) = std::fs::read_dir(&dir_owned) {
                for entry in read.flatten() {
                    if entry.file_type().is_ok_and(|t| t.is_symlink()) {
                        continue;
                    }
                    names.insert(entry.file_name());
                }
            }
            names
        })
        .await
        .unwrap_or_default();
        present_by_dir.insert(dir.to_path_buf(), entries);
    }

    let mut named = Vec::new();
    let mut total = 0usize;
    for file in files {
        let present = match (file.parent(), file.file_name()) {
            (Some(dir), Some(name)) => present_by_dir
                .get(dir)
                .is_some_and(|names| names.contains(name)),
            _ => false,
        };
        if !present {
            total += 1;
            if named.len() < MAX_NAMED {
                named.push(file.display().to_string());
            }
        }
    }
    (named, total)
}

/// Check that every data file `slice`'s current snapshot references was extracted under
/// `anchor`.
async fn verify_slice_against_disk(
    slice: &DatasetMetastoreSlice,
    anchor: &std::path::Path,
) -> Result<(), String> {
    let files = referenced_data_files(slice, anchor)?;
    let (named, total) = absent_on_disk(&files).await;
    if total == 0 {
        return Ok(());
    }
    Err(format!(
        "{total} of the {} files its current snapshot references are missing (for example: {})",
        files.len(),
        named.join(", ")
    ))
}

/// Errors raised by the Cayenne snapshot engine.
#[derive(Debug, Snafu)]
pub enum CayenneSnapshotError {
    #[snafu(display("Cayenne metastore export failed for dataset '{dataset}': {source}"))]
    Export {
        dataset: String,
        source: cayenne::CatalogError,
    },

    #[snafu(display("Cayenne metastore import failed for dataset '{dataset}': {source}"))]
    Import {
        dataset: String,
        source: cayenne::CatalogError,
    },

    #[snafu(display("Failed to serialize Cayenne metastore slice for '{dataset}': {source}"))]
    Serialize {
        dataset: String,
        source: serde_json::Error,
    },

    #[snafu(display(
        "Cayenne snapshot at {path:?} is missing the per-dataset metastore slice. \
         The snapshot was likely produced by an older Spice that shipped the \
         raw cayenne.db file; that format is no longer supported. \
         Recreate the snapshot from a current writer."
    ))]
    MissingSlice { path: PathBuf },

    #[snafu(display("Failed to read metastore slice from {path:?}: {source}"))]
    ReadSlice {
        path: PathBuf,
        source: std::io::Error,
    },
}

/// Snapshot engine for Cayenne accelerators.
///
/// Holds an [`Arc<dyn MetadataCatalog>`] so it can call
/// [`MetadataCatalog::export_dataset_slice`] / `import_dataset_slice` against
/// the same metastore the accelerator is using at runtime.
pub struct CayenneSnapshotEngine {
    /// Cayenne metastore (sqlite or libsql) the engine talks to.
    catalog: Arc<dyn MetadataCatalog>,
    /// The data files the most recent `prepare_directory_snapshot` promised the archive
    /// would contain, for `verify_directory_snapshot` to check the finished archive
    /// against. Holds the resolved paths rather than the whole slice: the slice carries a
    /// stats blob per file and is the larger part of a big table's metastore, and nothing
    /// past this point reads any other part of it. One snapshot of a dataset runs at a
    /// time (the manager holds the accelerator write lock across both calls), so a single
    /// slot is enough; `verify` takes the value so nothing is retained afterwards.
    expected_files: std::sync::Mutex<Option<Vec<PathBuf>>>,
    /// Logical dataset name (the value of `cayenne_table.table_name`).
    dataset_name: String,
    /// Local data directory anchor used to rewrite path columns relative
    /// on export and absolute on import. The export-side anchor must contain
    /// the absolute paths stored in the metastore as a strict prefix; the
    /// import-side anchor is where the new paths will be re-rooted.
    data_dir_anchor: PathBuf,
}

impl CayenneSnapshotEngine {
    pub fn new(
        catalog: Arc<dyn MetadataCatalog>,
        dataset_name: impl Into<String>,
        data_dir_anchor: PathBuf,
    ) -> Self {
        Self {
            catalog,
            expected_files: std::sync::Mutex::new(None),
            dataset_name: dataset_name.into(),
            data_dir_anchor,
        }
    }

    /// Returns the dataset name this engine snapshots.
    #[must_use]
    pub fn dataset_name(&self) -> &str {
        &self.dataset_name
    }

    /// Returns the data-dir anchor used for path rewriting.
    #[must_use]
    pub fn data_dir_anchor(&self) -> &std::path::Path {
        &self.data_dir_anchor
    }

    /// Convenience: turn a `CayenneSnapshotError` into a
    /// `SnapshotEngineError::Generic` (or its closest analog) so the trait
    /// signature stays clean.
    fn engine_err(err: &CayenneSnapshotError) -> SnapshotEngineError {
        // SnapshotEngineError doesn't have a Cayenne variant; surface as a
        // generic boxed error via Display (the trait error is non-exhaustive
        // at the call site, which renders Display).
        SnapshotEngineError::from_display(err.to_string())
    }
}

#[async_trait]
impl SnapshotEngine for CayenneSnapshotEngine {
    async fn prepare_for_upload(
        &self,
        source_path: &std::path::Path,
        _dataset_name: &str,
    ) -> Result<PathBuf, SnapshotEngineError> {
        // Cayenne snapshots are directory-layout, not file-layout, so
        // prepare_for_upload should never be called on this engine. Keep
        // a passthrough for defense.
        Ok(source_path.to_path_buf())
    }

    fn supports_compaction(&self) -> bool {
        false
    }

    async fn prepare_directory_snapshot(
        &self,
        _dirs: &[(PathBuf, String)],
        dataset_name: &str,
    ) -> Result<DirectorySnapshotPlan, SnapshotEngineError> {
        // Sanity: refuse to snapshot a dataset other than the one we were
        // constructed for.
        if dataset_name != self.dataset_name {
            return Err(SnapshotEngineError::from_display(format!(
                "CayenneSnapshotEngine constructed for dataset '{}' but asked to snapshot '{}'",
                self.dataset_name, dataset_name
            )));
        }

        // 1. Export the per-dataset metastore slice.
        let slice = self
            .catalog
            .export_dataset_slice(&self.dataset_name, &self.data_dir_anchor)
            .await
            .context(ExportSnafu {
                dataset: self.dataset_name.clone(),
            })
            .map_err(|e| Self::engine_err(&e))?;

        // 2. Serialize to JSON.
        let bytes = slice
            .to_json_bytes()
            .context(SerializeSnafu {
                dataset: self.dataset_name.clone(),
            })
            .map_err(|e| Self::engine_err(&e))?;

        // 2b. Resolve, now, the files this slice promises the archive will contain, so
        // `verify_directory_snapshot` can check the finished archive against exactly the
        // metadata it was built to match.
        {
            let expected = referenced_data_files(&slice, &self.data_dir_anchor)
                .map_err(SnapshotEngineError::from_display)?;
            let mut stash = self
                .expected_files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *stash = Some(expected);
        }

        // 3. Build a plan: skip the cayenne.db* files, add the slice as an extra.
        let skip = METASTORE_FILES.iter().map(PathBuf::from).collect();
        let extras = vec![DirectoryArchiveExtra {
            archive_path: slice_archive_path(&self.dataset_name),
            bytes,
        }];

        Ok(DirectorySnapshotPlan {
            skip_relative_paths: skip,
            extra_entries: extras,
        })
    }

    async fn verify_directory_snapshot(
        &self,
        dirs: &[(PathBuf, String)],
        members: &HashSet<String>,
        dataset_name: &str,
    ) -> Result<(), SnapshotEngineError> {
        if dataset_name != self.dataset_name {
            return Err(SnapshotEngineError::from_display(format!(
                "CayenneSnapshotEngine constructed for dataset '{}' but asked to verify '{}'",
                self.dataset_name, dataset_name
            )));
        }

        let expected = {
            let mut stash = self
                .expected_files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            stash.take()
        };
        let Some(expected) = expected else {
            // Nothing was captured, so this archive was not built from a slice and there
            // is nothing to check it against.
            return Ok(());
        };

        // Checked against the archive's own member list, not the filesystem it was built
        // from. Those answer different questions: a file can be present on disk and absent
        // from the tar — the walker skips symlinks, a path resolved outside the archived
        // directories is never visited, and a file recreated after the walk passed it looks
        // present either way. Only membership proves the archive can be restored.
        let missing = missing_members(&expected, dirs, members);
        if missing.is_empty() {
            return Ok(());
        }
        Err(SnapshotEngineError::from_display(format!(
            "the snapshot archive of '{}' is missing {} of the {} files its current snapshot references (for example: {})",
            self.dataset_name,
            missing.len(),
            expected.len(),
            missing
                .iter()
                .take(5)
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        )))
    }

    async fn finalize_directory_snapshot(
        &self,
        dirs: &[(PathBuf, String)],
        dataset_name: &str,
    ) -> Result<(), SnapshotEngineError> {
        if dataset_name != self.dataset_name {
            return Err(SnapshotEngineError::from_display(format!(
                "CayenneSnapshotEngine constructed for dataset '{}' but asked to extract '{}'",
                self.dataset_name, dataset_name
            )));
        }

        // The archive was extracted via prefix mappings, so the slice
        // landed at `<metadata_dir>/<dataset_name>.slice.json` (its
        // archive path uses the same `metadata/` prefix that
        // `AccelerationLayout::cayenne` configures).
        let slice_filename = format!("{dataset_name}.slice.json");
        let metadata_candidates: Vec<PathBuf> = dirs
            .iter()
            .filter(|(_, prefix)| prefix.starts_with("metadata"))
            .map(|(target_dir, _)| target_dir.join(&slice_filename))
            .collect();
        // Fallback: search every dir, in case the layout prefix list
        // changes shape in the future.
        let candidate_paths: Vec<PathBuf> = if metadata_candidates.is_empty() {
            dirs.iter()
                .map(|(target_dir, _)| target_dir.join(&slice_filename))
                .collect()
        } else {
            metadata_candidates
        };

        let mut slice_path: Option<PathBuf> = None;
        for cand in &candidate_paths {
            match fs::try_exists(cand).await {
                Ok(true) => {
                    slice_path = Some(cand.clone());
                    break;
                }
                Ok(false) => {} // try the next candidate
                Err(err) => {
                    // A real I/O error here (permissions, transient failure)
                    // would otherwise be silently swallowed and surface as a
                    // misleading `MissingSlice` below. Fail loudly instead.
                    return Err(SnapshotEngineError::from_display(format!(
                        "CayenneSnapshotEngine: failed to stat candidate slice path {}: {err}",
                        cand.display(),
                    )));
                }
            }
        }
        let slice_path = slice_path.ok_or_else(|| {
            Self::engine_err(&CayenneSnapshotError::MissingSlice {
                path: candidate_paths
                    .first()
                    .cloned()
                    .unwrap_or_else(|| PathBuf::from(&slice_filename)),
            })
        })?;

        let bytes = fs::read(&slice_path)
            .await
            .context(ReadSliceSnafu {
                path: slice_path.clone(),
            })
            .map_err(|e| Self::engine_err(&e))?;
        let slice = DatasetMetastoreSlice::from_json_bytes(&bytes).map_err(|e| {
            Self::engine_err(&CayenneSnapshotError::Import {
                dataset: self.dataset_name.clone(),
                source: e,
            })
        })?;

        // Refuse an archive whose data files do not match its metadata BEFORE importing:
        // the import replaces this dataset's metastore rows wholesale, so letting an
        // incomplete archive through would leave the local metastore describing files
        // that are not there. Failing here leaves the metastore untouched, the
        // acceleration empty, and the next refresh rebuilds it from source.
        //
        // Also the backstop for an archive written by a build that could not verify at
        // creation time.
        if let Err(reason) = verify_slice_against_disk(&slice, &self.data_dir_anchor).await {
            // Clear what was extracted before giving up. `has_existing_acceleration` reads
            // any entry under the data directory as "an acceleration is already here", so
            // leaving a half-restored tree behind would make every later cold start skip
            // the bootstrap — turning one bad archive into a permanently un-restorable
            // volume. Only the data directory is cleared; the metadata directory is shared
            // with every other Cayenne dataset in the pod.
            if let Err(cleanup) = fs::remove_dir_all(&self.data_dir_anchor).await {
                tracing::warn!(
                    "Failed to clear the partially extracted acceleration of '{}' at {} after refusing its snapshot; remove it before restarting or the next start will skip the bootstrap: {cleanup}",
                    self.dataset_name,
                    self.data_dir_anchor.display()
                );
            }
            return Err(SnapshotEngineError::from_display(format!(
                "the snapshot of '{}' is incomplete, so it was not restored and the acceleration starts empty: {reason}",
                self.dataset_name
            )));
        }

        self.catalog
            .import_dataset_slice(&slice, &self.data_dir_anchor)
            .await
            .context(ImportSnafu {
                dataset: self.dataset_name.clone(),
            })
            .map_err(|e| Self::engine_err(&e))?;

        // Best-effort: remove the slice file so it doesn't sit in the data
        // directory after import. Its information now lives in the local
        // metastore.
        let _ = fs::remove_file(&slice_path).await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cayenne::CayenneCatalog;
    use cayenne::metadata::CreateTableOptions;
    use std::sync::Arc;

    async fn fresh_catalog(dir: &std::path::Path) -> Arc<CayenneCatalog> {
        let conn = format!("sqlite://{}/cayenne.db", dir.display());
        let catalog = Arc::new(CayenneCatalog::new(conn).expect("catalog"));
        catalog.init().await.expect("init");
        catalog
    }

    fn schema() -> Arc<arrow_schema::Schema> {
        Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]))
    }

    /// Builds the minimal slice shape `referenced_data_files` reads: one `cayenne_table`
    /// row plus manifest rows, positioned per `EXPECTED_TABLES`.
    fn slice_with_manifest(
        table_id: &str,
        current_snapshot_id: Option<&str>,
        files: &[(&str, &str)],
    ) -> DatasetMetastoreSlice {
        let table_columns = cayenne::metastore::EXPECTED_TABLES
            .iter()
            .find(|t| t.name == "cayenne_table")
            .expect("cayenne_table is a known metastore table");
        let mut table_row = vec![SliceValue::Null; table_columns.columns.len()];
        let put = |row: &mut Vec<SliceValue>, column: &str, value: SliceValue| {
            let idx = column_index("cayenne_table", column).expect("known column");
            row[idx] = value;
        };
        put(
            &mut table_row,
            "table_id",
            SliceValue::Text(table_id.to_string()),
        );
        // Relative to the anchor, which is what `export_dataset` writes for a table
        // stored under the data directory.
        put(&mut table_row, "path", SliceValue::Text(String::new()));
        put(&mut table_row, "path_is_relative", SliceValue::Bool(true));
        if let Some(id) = current_snapshot_id {
            put(
                &mut table_row,
                "current_snapshot_id",
                SliceValue::Text(id.to_string()),
            );
        }

        let file_columns = cayenne::metastore::EXPECTED_TABLES
            .iter()
            .find(|t| t.name == "cayenne_snapshot_file")
            .expect("cayenne_snapshot_file is a known metastore table");
        let manifest = files
            .iter()
            .map(|(snapshot_id, file_path)| {
                let mut row = vec![SliceValue::Null; file_columns.columns.len()];
                let snap_idx =
                    column_index("cayenne_snapshot_file", "snapshot_id").expect("known column");
                let path_idx =
                    column_index("cayenne_snapshot_file", "file_path").expect("known column");
                row[snap_idx] = SliceValue::Text((*snapshot_id).to_string());
                row[path_idx] = SliceValue::Text((*file_path).to_string());
                row
            })
            .collect();

        let mut tables = std::collections::BTreeMap::new();
        tables.insert("cayenne_table".to_string(), vec![table_row]);
        tables.insert("cayenne_snapshot_file".to_string(), manifest);

        DatasetMetastoreSlice {
            format_version: cayenne::metastore::snapshot::SLICE_FORMAT_VERSION,
            engine: "cayenne".to_string(),
            dataset_name: "trips".to_string(),
            exported_at_ms: 0,
            tables,
        }
    }

    /// One `cayenne_delete_file` row whose `path` is relative to the data-dir anchor.
    fn delete_row(path: &str) -> Vec<SliceValue> {
        let columns = cayenne::metastore::EXPECTED_TABLES
            .iter()
            .find(|t| t.name == "cayenne_delete_file")
            .expect("cayenne_delete_file is a known metastore table");
        let mut row = vec![SliceValue::Null; columns.columns.len()];
        let path_idx = column_index("cayenne_delete_file", "path").expect("known column");
        let rel_idx =
            column_index("cayenne_delete_file", "path_is_relative").expect("known column");
        row[path_idx] = SliceValue::Text(path.to_string());
        row[rel_idx] = SliceValue::Bool(true);
        row
    }

    #[tokio::test]
    async fn verification_passes_when_every_referenced_file_is_present() {
        let tmp = tempfile::tempdir().expect("tmp");
        let anchor = tmp.path();
        let snapshot_dir = anchor.join("tbl-1").join("snap-1");
        std::fs::create_dir_all(&snapshot_dir).expect("mkdir snapshot");
        std::fs::write(snapshot_dir.join("a.vortex"), b"a").expect("write a");
        std::fs::write(snapshot_dir.join("b.vortex"), b"b").expect("write b");

        let slice = slice_with_manifest(
            "tbl-1",
            Some("snap-1"),
            &[("snap-1", "a.vortex"), ("snap-1", "b.vortex")],
        );
        verify_slice_against_disk(&slice, anchor)
            .await
            .expect("a complete archive verifies");
    }

    /// The shape a compaction cleanup produces if it unlinks the exported snapshot's
    /// files while the archive is still being written.
    #[tokio::test]
    async fn verification_fails_when_a_referenced_file_went_missing() {
        let tmp = tempfile::tempdir().expect("tmp");
        let anchor = tmp.path();
        let snapshot_dir = anchor.join("tbl-1").join("snap-1");
        std::fs::create_dir_all(&snapshot_dir).expect("mkdir snapshot");
        std::fs::write(snapshot_dir.join("a.vortex"), b"a").expect("write a");

        let slice = slice_with_manifest(
            "tbl-1",
            Some("snap-1"),
            &[("snap-1", "a.vortex"), ("snap-1", "gone.vortex")],
        );
        let reason = verify_slice_against_disk(&slice, anchor)
            .await
            .expect_err("a missing referenced file must be refused");
        assert!(reason.contains("gone.vortex"), "{reason}");
        assert!(reason.contains("1 of the 2"), "{reason}");
    }

    /// Only the current snapshot is read on restore, so a retired snapshot whose files
    /// were legitimately swept must not fail verification.
    #[tokio::test]
    async fn verification_ignores_retired_snapshots() {
        let tmp = tempfile::tempdir().expect("tmp");
        let anchor = tmp.path();
        let snapshot_dir = anchor.join("tbl-1").join("snap-2");
        std::fs::create_dir_all(&snapshot_dir).expect("mkdir snapshot");
        std::fs::write(snapshot_dir.join("a.vortex"), b"a").expect("write a");

        let slice = slice_with_manifest(
            "tbl-1",
            Some("snap-2"),
            &[("snap-1", "swept.vortex"), ("snap-2", "a.vortex")],
        );
        verify_slice_against_disk(&slice, anchor)
            .await
            .expect("rows for a retired snapshot are not consulted");
    }

    /// A deletion vector missing from a restored archive is the worst shape available: the
    /// scan TOLERATES a missing key-based vector rather than failing, so those deletions
    /// stop applying and the rows they hid come back.
    #[tokio::test]
    async fn verification_fails_when_a_deletion_vector_went_missing() {
        let tmp = tempfile::tempdir().expect("tmp");
        let anchor = tmp.path();
        let snapshot_dir = anchor.join("tbl-1").join("snap-1");
        std::fs::create_dir_all(snapshot_dir.join("deletions")).expect("mkdir deletions");
        std::fs::write(snapshot_dir.join("a.vortex"), b"rows").expect("write data file");

        let mut slice = slice_with_manifest("tbl-1", Some("snap-1"), &[("snap-1", "a.vortex")]);
        slice.tables.insert(
            "cayenne_delete_file".to_string(),
            vec![delete_row("tbl-1/snap-1/deletions/dv-1.arrow")],
        );

        let reason = verify_slice_against_disk(&slice, anchor)
            .await
            .expect_err("a missing deletion vector must be refused");
        assert!(reason.contains("dv-1.arrow"), "{reason}");
    }

    #[tokio::test]
    async fn verification_passes_when_the_deletion_vector_is_present() {
        let tmp = tempfile::tempdir().expect("tmp");
        let anchor = tmp.path();
        let snapshot_dir = anchor.join("tbl-1").join("snap-1");
        std::fs::create_dir_all(snapshot_dir.join("deletions")).expect("mkdir deletions");
        std::fs::write(snapshot_dir.join("a.vortex"), b"rows").expect("write data file");
        std::fs::write(snapshot_dir.join("deletions").join("dv-1.arrow"), b"dv").expect("write dv");

        let mut slice = slice_with_manifest("tbl-1", Some("snap-1"), &[("snap-1", "a.vortex")]);
        slice.tables.insert(
            "cayenne_delete_file".to_string(),
            vec![delete_row("tbl-1/snap-1/deletions/dv-1.arrow")],
        );

        verify_slice_against_disk(&slice, anchor)
            .await
            .expect("a complete archive verifies");
    }

    /// A symlinked data file is skipped by the archiver, so verification must not count it
    /// as present — otherwise the check passes for a file the tar does not contain.
    #[tokio::test]
    async fn a_symlinked_data_file_counts_as_missing() {
        let tmp = tempfile::tempdir().expect("tmp");
        let anchor = tmp.path();
        let snapshot_dir = anchor.join("tbl-1").join("snap-1");
        std::fs::create_dir_all(&snapshot_dir).expect("mkdir snapshot");
        let real = tmp.path().join("elsewhere.vortex");
        std::fs::write(&real, b"rows").expect("write real file");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&real, snapshot_dir.join("a.vortex")).expect("symlink");

        let slice = slice_with_manifest("tbl-1", Some("snap-1"), &[("snap-1", "a.vortex")]);
        let reason = verify_slice_against_disk(&slice, anchor)
            .await
            .expect_err("a symlink is not archived, so it must not verify");
        assert!(reason.contains("a.vortex"), "{reason}");
    }

    #[tokio::test]
    async fn a_table_that_never_published_references_nothing() {
        let tmp = tempfile::tempdir().expect("tmp");
        let slice = slice_with_manifest("tbl-1", None, &[]);
        verify_slice_against_disk(&slice, tmp.path())
            .await
            .expect("no current snapshot means nothing to verify");
    }

    #[tokio::test]
    async fn create_directory_snapshot_skips_cayenne_db_and_emits_slice() {
        let tmp = tempfile::tempdir().expect("tmp");
        let metadata_dir = tmp.path().join("metadata");
        std::fs::create_dir_all(&metadata_dir).expect("mkdir metadata");
        let data_dir = tmp.path().join("data").join("trips");
        std::fs::create_dir_all(&data_dir).expect("mkdir data");

        let catalog = fresh_catalog(&metadata_dir).await;
        catalog
            .create_table(CreateTableOptions {
                table_name: "trips".to_string(),
                schema: schema(),
                primary_key: vec![],
                on_conflict: None,
                base_path: data_dir.to_string_lossy().into_owned(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            })
            .await
            .expect("create_table");

        // Pre-populate cayenne.db so it shows up under metadata_dir.
        // The catalog's init has already written cayenne.db; nothing more to do.

        let engine = CayenneSnapshotEngine::new(
            catalog as Arc<dyn MetadataCatalog>,
            "trips",
            data_dir.clone(),
        );

        let dirs = vec![
            (metadata_dir.clone(), "metadata/".to_string()),
            (data_dir.clone(), "data/".to_string()),
        ];

        let plan = engine
            .prepare_directory_snapshot(&dirs, "trips")
            .await
            .expect("prepare_directory_snapshot");

        // Expect cayenne.db files to be in skip list.
        assert!(
            plan.skip_relative_paths
                .contains(&PathBuf::from("cayenne.db"))
        );
        assert!(
            plan.skip_relative_paths
                .contains(&PathBuf::from("cayenne.db-wal"))
        );
        assert!(
            plan.skip_relative_paths
                .contains(&PathBuf::from("cayenne.db-shm"))
        );

        // Expect exactly one extra entry: the slice JSON.
        assert_eq!(plan.extra_entries.len(), 1);
        let extra = &plan.extra_entries[0];
        assert_eq!(extra.archive_path, "metadata/trips.slice.json");

        // Sanity: the JSON parses as a versioned slice.
        let slice =
            cayenne::metastore::snapshot::DatasetMetastoreSlice::from_json_bytes(&extra.bytes)
                .expect("parse slice");
        assert_eq!(slice.dataset_name, "trips");
    }

    #[tokio::test]
    async fn refuses_mismatched_dataset() {
        let tmp = tempfile::tempdir().expect("tmp");
        let metadata_dir = tmp.path().join("metadata");
        std::fs::create_dir_all(&metadata_dir).expect("mkdir metadata");
        let catalog = fresh_catalog(&metadata_dir).await;

        let engine = CayenneSnapshotEngine::new(
            catalog as Arc<dyn MetadataCatalog>,
            "trips",
            tmp.path().to_path_buf(),
        );

        let err = engine
            .prepare_directory_snapshot(&[], "riders")
            .await
            .expect_err("must reject mismatched dataset name");
        assert!(err.to_string().contains("trips"));
        assert!(err.to_string().contains("riders"));
    }

    #[tokio::test]
    async fn finalize_missing_slice_returns_clear_error() {
        let tmp = tempfile::tempdir().expect("tmp");
        let metadata_dir = tmp.path().join("metadata");
        std::fs::create_dir_all(&metadata_dir).expect("mkdir metadata");
        let catalog = fresh_catalog(&metadata_dir).await;

        let engine = CayenneSnapshotEngine::new(
            catalog as Arc<dyn MetadataCatalog>,
            "trips",
            tmp.path().to_path_buf(),
        );

        // No slice file present in metadata_dir.
        let dirs = vec![(metadata_dir.clone(), "metadata/".to_string())];
        let err = engine
            .finalize_directory_snapshot(&dirs, "trips")
            .await
            .expect_err("must error when slice is missing");
        let msg = err.to_string();
        assert!(
            msg.contains("missing the per-dataset metastore slice"),
            "msg={msg}"
        );
        assert!(msg.contains("older Spice"), "msg={msg}");
    }
}
