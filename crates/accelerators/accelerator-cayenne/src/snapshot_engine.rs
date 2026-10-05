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
//!    the other's metastore rows — including those of a dataset that never
//!    snapshots at all.
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
//! the slice back and atomically imports it into the local metastore,
//! replacing only that dataset's rows. Datasets sharing a metadata directory
//! therefore snapshot independently: each may enable, disable, only create or
//! only bootstrap without constraining the others.
//!
//! Path columns in the slice are rewritten relative to the writer's data
//! directory at export time and re-anchored at the reader's data directory
//! on import, making the snapshot portable across nodes with different
//! local layouts.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use cayenne::MetadataCatalog;
use cayenne::metastore::EXPECTED_TABLES;
use cayenne::metastore::snapshot::{DatasetMetastoreSlice, SliceValue};
use runtime_acceleration::snapshot::engine::{
    DirectoryArchiveExtra, DirectoryArchiveFile, DirectorySnapshotPlan, SnapshotEngine,
    SnapshotEngineError,
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

    #[snafu(display(
        "Failed to list the data directory {} of dataset '{dataset}' for its snapshot: {source}",
        path.display()
    ))]
    ListDataDir {
        dataset: String,
        path: PathBuf,
        source: std::io::Error,
    },
}

/// Position of `column` in `table`'s slice rows, per [`EXPECTED_TABLES`].
fn slice_column(table: &str, column: &str) -> Option<usize> {
    EXPECTED_TABLES
        .iter()
        .find(|t| t.name == table)
        .and_then(|t| t.columns.iter().position(|c| *c == column))
}

fn slice_text(row: &[SliceValue], index: Option<usize>) -> Option<&str> {
    match index.and_then(|i| row.get(i)) {
        Some(SliceValue::Text(text)) => Some(text.as_str()),
        _ => None,
    }
}

/// Snapshot ids the slice references: the current snapshot, the protected
/// snapshots, and the snapshots whose `deletions/` directory holds a
/// referenced deletion file.
fn referenced_snapshot_ids(slice: &DatasetMetastoreSlice, anchor: &Path) -> HashSet<String> {
    let mut ids = HashSet::new();
    let current = slice_column("cayenne_table", "current_snapshot_id");
    for row in slice.tables.get("cayenne_table").into_iter().flatten() {
        ids.extend(slice_text(row, current).map(str::to_string));
    }
    let snapshot_id = slice_column("cayenne_snapshot_sequence", "snapshot_id");
    for row in slice
        .tables
        .get("cayenne_snapshot_sequence")
        .into_iter()
        .flatten()
    {
        ids.extend(slice_text(row, snapshot_id).map(str::to_string));
    }
    let path = slice_column("cayenne_delete_file", "path");
    for row in slice
        .tables
        .get("cayenne_delete_file")
        .into_iter()
        .flatten()
    {
        let Some(path) = slice_text(row, path) else {
            continue;
        };
        let relative = Path::new(path)
            .strip_prefix(anchor)
            .unwrap_or(Path::new(path));
        if let Some(first) = relative.components().next() {
            ids.insert(first.as_os_str().to_string_lossy().into_owned());
        }
    }
    ids
}

/// Entries under the dataset's data directory that its snapshot must not
/// archive: snapshot directories the slice does not reference (retired ones
/// are removed by the sweep while the archive is being written), staging
/// state, and `deletions/` directories of unreferenced snapshots. Paths are
/// relative to `anchor`.
async fn unreferenced_data_entries(
    anchor: &Path,
    slice: &DatasetMetastoreSlice,
) -> std::io::Result<HashSet<PathBuf>> {
    let table_row = slice
        .tables
        .get("cayenne_table")
        .and_then(|rows| rows.first());
    let mut skip = HashSet::new();
    // A partitioned table keeps its partitions directly under the data
    // directory; archive it as a whole, except the partitions' persisted
    // index runs. The slice registers none of them, so they cannot be
    // listed one by one, and the background sync deletes their files while
    // the archive is written; a restored partition rebuilds its index.
    if table_row
        .and_then(|row| slice_text(row, slice_column("cayenne_table", "partition_column")))
        .is_some()
    {
        lookup_index_dirs(anchor, Path::new(""), &mut skip).await?;
        return Ok(skip);
    }
    let table_id = table_row
        .and_then(|row| slice_text(row, slice_column("cayenne_table", "table_id")))
        .map(str::to_string);
    let referenced = referenced_snapshot_ids(slice, anchor);
    let mut entries = match tokio::fs::read_dir(anchor).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(skip),
        Err(err) => return Err(err),
    };
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name().to_string_lossy().into_owned();
        if Some(&name) == table_id.as_ref() {
            // `<table_id>/<snapshot_id>/`: keep the referenced snapshots, and
            // the persisted secondary index runs under `<table_id>/_lookup_index/`
            // are archived file by file instead (see `persisted_run_files`).
            let mut children = tokio::fs::read_dir(entry.path()).await?;
            while let Some(child) = children.next_entry().await? {
                let child_name = child.file_name().to_string_lossy().into_owned();
                if !referenced.contains(&child_name) {
                    skip.insert(PathBuf::from(&name).join(child_name));
                }
            }
        } else if !referenced.contains(&name) {
            // `<snapshot_id>/deletions/` of a snapshot nothing references.
            skip.insert(PathBuf::from(name));
        }
    }
    Ok(skip)
}

/// Every `_lookup_index` directory under `anchor`, relative to it, added to
/// `found`. `relative` is the directory being searched.
async fn lookup_index_dirs(
    anchor: &Path,
    relative: &Path,
    found: &mut HashSet<PathBuf>,
) -> std::io::Result<()> {
    let mut pending = vec![relative.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let mut entries = match tokio::fs::read_dir(anchor.join(&dir)).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err),
        };
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            let path = dir.join(entry.file_name());
            if entry.file_name() == cayenne::LOOKUP_INDEX_DIR_NAME {
                found.insert(path);
            } else {
                pending.push(path);
            }
        }
    }
    Ok(())
}

/// The persisted secondary index runs the slice registers, as files the
/// archive takes only if they still exist: the background sync deletes a
/// merged or retired run's file at any time, without waiting for a snapshot,
/// so one may be gone by the time the archive reaches it. A run left out
/// restores as a registration with no file, which the restored table drops,
/// indexing that run's files again. `data_prefix` is the data directory's
/// path inside the archive.
fn persisted_run_files(
    anchor: &Path,
    data_prefix: &str,
    slice: &DatasetMetastoreSlice,
) -> Vec<DirectoryArchiveFile> {
    let (table_id, index_key, run_name) = (
        slice_column("cayenne_index_run", "table_id"),
        slice_column("cayenne_index_run", "index_key"),
        slice_column("cayenne_index_run", "run_name"),
    );
    slice
        .tables
        .get("cayenne_index_run")
        .into_iter()
        .flatten()
        .filter_map(|row| {
            let relative = PathBuf::from(slice_text(row, table_id)?)
                .join(cayenne::LOOKUP_INDEX_DIR_NAME)
                .join(slice_text(row, index_key)?)
                .join(slice_text(row, run_name)?);
            Some(DirectoryArchiveFile {
                source: anchor.join(&relative),
                archive_path: format!("{data_prefix}{}", relative.to_string_lossy()),
            })
        })
        .collect()
}

/// Snapshot engine for Cayenne accelerators.
///
/// Holds an [`Arc<dyn MetadataCatalog>`] so it can call
/// [`MetadataCatalog::export_dataset_slice`] / `import_dataset_slice` against
/// the same metastore the accelerator is using at runtime.
pub struct CayenneSnapshotEngine {
    /// Cayenne metastore (sqlite or libsql) the engine talks to.
    catalog: Arc<dyn MetadataCatalog>,
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
    /// Nothing to flush here: Cayenne snapshots are directory-layout, and only the
    /// file-layout path invokes this hook. What Cayenne does have to capture — the
    /// per-dataset metastore slice — is exported by `prepare_directory_snapshot`.
    async fn checkpoint_live(
        &self,
        _live_path: &std::path::Path,
        _dataset_name: &str,
    ) -> Result<(), SnapshotEngineError> {
        Ok(())
    }

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
        dirs: &[(PathBuf, String)],
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

        // 3. Build a plan: skip the cayenne.db* files and every data entry
        //    the slice does not reference, add the slice as an extra.
        let mut skip: HashSet<PathBuf> = METASTORE_FILES.iter().map(PathBuf::from).collect();
        skip.extend(
            unreferenced_data_entries(&self.data_dir_anchor, &slice)
                .await
                .context(ListDataDirSnafu {
                    dataset: self.dataset_name.clone(),
                    path: self.data_dir_anchor.clone(),
                })
                .map_err(|e| Self::engine_err(&e))?,
        );
        let extras = vec![DirectoryArchiveExtra {
            archive_path: slice_archive_path(&self.dataset_name),
            bytes,
        }];
        // The persisted index runs go in file by file, under the data
        // directory's prefix in the archive.
        let optional_files = dirs
            .iter()
            .find(|(dir, _)| *dir == self.data_dir_anchor)
            .map(|(_, prefix)| persisted_run_files(&self.data_dir_anchor, prefix, &slice))
            .unwrap_or_default();

        Ok(DirectorySnapshotPlan {
            skip_relative_paths: skip,
            extra_entries: extras,
            optional_files,
        })
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

    fn slice_with(
        table_id: &str,
        current: &str,
        protected: &[&str],
        delete_paths: &[&str],
    ) -> DatasetMetastoreSlice {
        slice_with_partition(table_id, current, protected, delete_paths, None)
    }

    fn slice_with_partition(
        table_id: &str,
        current: &str,
        protected: &[&str],
        delete_paths: &[&str],
        partition_column: Option<&str>,
    ) -> DatasetMetastoreSlice {
        use cayenne::metastore::snapshot::{SLICE_ENGINE, SLICE_FORMAT_VERSION};
        let row = |table: &str, values: &[(&str, &str)]| -> Vec<SliceValue> {
            let columns = EXPECTED_TABLES
                .iter()
                .find(|t| t.name == table)
                .expect("known table")
                .columns;
            columns
                .iter()
                .map(|c| {
                    values
                        .iter()
                        .find(|(name, _)| name == c)
                        .map_or(SliceValue::Null, |(_, v)| {
                            SliceValue::Text((*v).to_string())
                        })
                })
                .collect()
        };
        let mut tables = std::collections::BTreeMap::new();
        let mut table_values = vec![("table_id", table_id), ("current_snapshot_id", current)];
        if let Some(column) = partition_column {
            table_values.push(("partition_column", column));
        }
        tables.insert(
            "cayenne_table".to_string(),
            vec![row("cayenne_table", &table_values)],
        );
        tables.insert(
            "cayenne_snapshot_sequence".to_string(),
            protected
                .iter()
                .map(|id| row("cayenne_snapshot_sequence", &[("snapshot_id", id)]))
                .collect(),
        );
        tables.insert(
            "cayenne_delete_file".to_string(),
            delete_paths
                .iter()
                .map(|path| row("cayenne_delete_file", &[("path", path)]))
                .collect(),
        );
        DatasetMetastoreSlice {
            format_version: SLICE_FORMAT_VERSION,
            engine: SLICE_ENGINE.to_string(),
            dataset_name: "trips".to_string(),
            exported_at_ms: 0,
            tables,
        }
    }

    /// Retired snapshot directories, staging state and the `deletions/`
    /// directories of unreferenced snapshots are skipped; the current and
    /// protected snapshots and referenced `deletions/` directories are kept.
    #[tokio::test]
    async fn unreferenced_data_entries_keeps_only_referenced_snapshots() {
        let tmp = tempfile::tempdir().expect("tmp");
        let anchor = tmp.path();
        for dir in [
            "tid/current",
            "tid/protected",
            "tid/retired",
            "tid/_staging",
            "current/deletions",
            "old/deletions",
        ] {
            std::fs::create_dir_all(anchor.join(dir)).expect("mkdir");
        }
        std::fs::write(anchor.join("current/deletions/delete_1.arrow"), b"dv").expect("write");
        let slice = slice_with(
            "tid",
            "current",
            &["protected"],
            &["current/deletions/delete_1.arrow"],
        );
        let skip = unreferenced_data_entries(anchor, &slice)
            .await
            .expect("list");
        let expected: HashSet<PathBuf> = ["tid/retired", "tid/_staging", "old"]
            .iter()
            .map(PathBuf::from)
            .collect();
        assert_eq!(skip, expected);

        // A partitioned table is archived as a whole, except each
        // partition's persisted index runs: the slice registers none of them,
        // and the background sync deletes their files while the archive is
        // written, so they are left out and rebuilt after a restore.
        for dir in [
            "us/ptid/current",
            "us/ptid/_lookup_index/key",
            "eu/_lookup_index",
        ] {
            std::fs::create_dir_all(anchor.join("partitioned").join(dir)).expect("mkdir");
        }
        std::fs::write(
            anchor.join("partitioned/us/ptid/_lookup_index/key/run.run"),
            b"run",
        )
        .expect("write");
        let partitioned = slice_with_partition("tid", "current", &[], &[], Some("region"));
        let expected: HashSet<PathBuf> = ["us/ptid/_lookup_index", "eu/_lookup_index"]
            .iter()
            .map(PathBuf::from)
            .collect();
        assert_eq!(
            unreferenced_data_entries(&anchor.join("partitioned"), &partitioned)
                .await
                .expect("list"),
            expected
        );
    }

    /// A full refresh retires the previous snapshot directory; the snapshot
    /// taken right after it must not archive that directory (the sweep removes
    /// it while the archive is written), and the archive must still restore.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn snapshot_plan_skips_the_retired_snapshot_directory() {
        use arrow::array::{Int64Array, RecordBatch};
        use cayenne::CayenneTableProviderBuilder;
        use datafusion::datasource::TableProvider;
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::logical_expr::dml::InsertOp;
        use datafusion::physical_plan::collect;
        use datafusion::prelude::SessionContext;
        use runtime_acceleration::snapshot::directory_archive::{
            ExtractOptions, archive_directories_to_file_with_plan,
            extract_archive_file_with_options,
        };

        let tmp = tempfile::tempdir().expect("tmp");
        let metadata_dir = tmp.path().join("writer").join("metadata");
        let data_dir = tmp.path().join("writer").join("trips");
        std::fs::create_dir_all(&metadata_dir).expect("mkdir metadata");
        std::fs::create_dir_all(&data_dir).expect("mkdir data");
        let catalog = fresh_catalog(&metadata_dir).await;
        let ctx = SessionContext::new();
        let table = CayenneTableProviderBuilder::new(
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
            ctx.runtime_env(),
        )
        .create(CreateTableOptions {
            table_name: "trips".to_string(),
            schema: schema(),
            primary_key: vec![],
            on_conflict: None,
            base_path: data_dir.to_string_lossy().into_owned(),
            partition_column: None,
            vortex_config: cayenne::metadata::VortexConfig {
                inline_max_rows: 0,
                inline_max_bytes: 0,
                ..cayenne::metadata::VortexConfig::default()
            },
        })
        .await
        .expect("create table");
        let write = |ids: Vec<i64>, op: InsertOp| {
            let table = &table;
            let ctx = &ctx;
            async move {
                let batch = RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(ids))])
                    .expect("batch");
                let input =
                    MemorySourceConfig::try_new_exec(&[vec![batch]], schema(), None).expect("exec");
                let plan = table
                    .insert_into(&ctx.state(), input, op)
                    .await
                    .expect("plan");
                collect(plan, ctx.task_ctx()).await.expect("write");
            }
        };
        write((1..=100).collect(), InsertOp::Append).await;
        let first = catalog
            .get_table("trips")
            .await
            .expect("meta")
            .current_snapshot_id;
        // The full refresh: a new snapshot replaces the first, which is retired.
        write((1..=50).collect(), InsertOp::Overwrite).await;
        let second = catalog
            .get_table("trips")
            .await
            .expect("meta")
            .current_snapshot_id;
        assert_ne!(first, second);
        let table_id = catalog.get_table("trips").await.expect("meta").table_id;
        assert!(
            data_dir.join(&table_id).join(&first).is_dir(),
            "the retired directory is still on disk when the snapshot starts"
        );

        let engine = CayenneSnapshotEngine::new(
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
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
            .expect("prepare");
        assert!(
            plan.skip_relative_paths
                .contains(&PathBuf::from(&table_id).join(&first))
        );
        assert!(
            !plan
                .skip_relative_paths
                .contains(&PathBuf::from(&table_id).join(&second))
        );

        let tar = tmp.path().join("snapshot.tar");
        let skip: Vec<PathBuf> = plan.skip_relative_paths.into_iter().collect();
        let extras: Vec<(String, Vec<u8>)> = plan
            .extra_entries
            .into_iter()
            .map(|e| (e.archive_path, e.bytes))
            .collect();
        archive_directories_to_file_with_plan(&dirs, &tar, &skip, &extras, &[])
            .await
            .expect("archive");

        let reader_root = tmp.path().join("reader");
        let reader_metadata = reader_root.join("metadata");
        let reader_data = reader_root.join("trips");
        std::fs::create_dir_all(&reader_metadata).expect("mkdir");
        std::fs::create_dir_all(&reader_data).expect("mkdir");
        let reader_catalog = fresh_catalog(&reader_metadata).await;
        extract_archive_file_with_options(
            &tar,
            &reader_root,
            ExtractOptions {
                prefix_mappings: Some(vec![
                    ("metadata/".to_string(), reader_metadata.clone()),
                    ("data/".to_string(), reader_data.clone()),
                ]),
                ..ExtractOptions::skip_existing()
            },
        )
        .await
        .expect("extract");
        assert!(
            !reader_data.join(&table_id).join(&first).exists(),
            "the retired snapshot must not be in the archive"
        );
        assert!(reader_data.join(&table_id).join(&second).is_dir());
        CayenneSnapshotEngine::new(
            Arc::clone(&reader_catalog) as Arc<dyn MetadataCatalog>,
            "trips",
            reader_data.clone(),
        )
        .finalize_directory_snapshot(
            &[
                (reader_metadata.clone(), "metadata/".to_string()),
                (reader_data.clone(), "data/".to_string()),
            ],
            "trips",
        )
        .await
        .expect("import");
        let restored = CayenneTableProviderBuilder::new(
            Arc::clone(&reader_catalog) as Arc<dyn MetadataCatalog>,
            ctx.runtime_env(),
        )
        .open("trips")
        .await
        .expect("open");
        let rows: usize = SessionContext::new()
            .read_table(Arc::new(restored) as Arc<dyn TableProvider>)
            .expect("read")
            .collect()
            .await
            .expect("collect")
            .iter()
            .map(RecordBatch::num_rows)
            .sum();
        assert_eq!(rows, 50, "the archive restores the refreshed table");
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

    mod persisted_runs {
        //! Persisted secondary index runs across an acceleration snapshot,
        //! written and restored the way the runtime does it: the engine's plan
        //! applied to the layout's two directories, archived, and extracted
        //! into a reader's own directories.

        use super::*;
        use arrow::array::{Int64Array, RecordBatch, StringArray};
        use cayenne::lookup_index::IndexPersistence;
        use cayenne::metadata::VortexConfig;
        use cayenne::provider::CayenneContext;
        use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder};
        use datafusion::datasource::TableProvider;
        use datafusion::execution::runtime_env::RuntimeEnv;
        use datafusion::prelude::SessionContext;
        use runtime_acceleration::snapshot::directory_archive::{
            ExtractOptions, archive_directories_to_file_with_plan,
            extract_archive_file_with_options,
        };
        use std::path::Path;
        use std::time::{Duration, Instant};

        const NAME: &str = "orders";

        fn orders_schema() -> Arc<arrow_schema::Schema> {
            Arc::new(arrow_schema::Schema::new(vec![
                arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
                arrow_schema::Field::new("tenant", arrow_schema::DataType::Int64, false),
                arrow_schema::Field::new("service", arrow_schema::DataType::Utf8, false),
            ]))
        }

        fn rows(offset: i64, count: i64) -> RecordBatch {
            let ids: Vec<i64> = (offset..offset + count).collect();
            RecordBatch::try_new(
                orders_schema(),
                vec![
                    Arc::new(Int64Array::from(ids.clone())),
                    Arc::new(Int64Array::from(
                        ids.iter().map(|id| id % 97).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        ids.iter().map(|id| format!("sv-{id}")).collect::<Vec<_>>(),
                    )),
                ],
            )
            .expect("batch")
        }

        async fn open(
            env: &Arc<RuntimeEnv>,
            catalog: Arc<CayenneCatalog>,
            data_dir: &Path,
            persistence: IndexPersistence,
        ) -> Arc<CayenneTableProvider> {
            let config = VortexConfig {
                target_vortex_file_size_mb: 1,
                ..VortexConfig::default()
            };
            let context = CayenneContext::new(&config, Arc::clone(env), NAME);
            let catalog: Arc<dyn MetadataCatalog> = catalog;
            Arc::new(
                CayenneTableProviderBuilder::new(catalog, Arc::clone(env))
                    .with_context(context)
                    .with_index_persistence(persistence)
                    .with_secondary_indexes(vec![vec!["tenant".to_string(), "service".to_string()]])
                    .create(CreateTableOptions {
                        table_name: NAME.to_string(),
                        schema: orders_schema(),
                        primary_key: vec![],
                        on_conflict: None,
                        base_path: data_dir.to_string_lossy().into_owned(),
                        partition_column: None,
                        vortex_config: config,
                    })
                    .await
                    .expect("create or reopen table"),
            )
        }

        async fn append(table: &Arc<CayenneTableProvider>, batch: RecordBatch) {
            let ctx = SessionContext::new();
            ctx.register_table(NAME, Arc::clone(table) as Arc<dyn TableProvider>)
                .expect("register");
            ctx.register_batch("src", batch).expect("source");
            ctx.sql(&format!("INSERT INTO {NAME} SELECT * FROM src"))
                .await
                .expect("plan insert")
                .collect()
                .await
                .expect("insert");
        }

        /// Rows returned by an index lookup of row `id`'s key.
        async fn lookup(table: &Arc<CayenneTableProvider>, id: i64) -> usize {
            let ctx = SessionContext::new();
            ctx.register_table(NAME, Arc::clone(table) as Arc<dyn TableProvider>)
                .expect("register");
            ctx.sql(&format!(
                "SELECT id FROM {NAME} WHERE tenant = {} AND service = 'sv-{id}'",
                id % 97
            ))
            .await
            .expect("plan lookup")
            .collect()
            .await
            .expect("lookup")
            .iter()
            .map(RecordBatch::num_rows)
            .sum()
        }

        fn run_files(dir: &Path) -> usize {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return 0;
            };
            entries
                .map(|entry| entry.expect("dir entry").path())
                .map(|path| {
                    if path.is_dir() {
                        run_files(&path)
                    } else {
                        usize::from(path.extension().is_some_and(|ext| ext == "run"))
                    }
                })
                .sum()
        }

        /// The table's registered runs once at least `runs` are registered and
        /// every run file on disk is a registered one.
        async fn persisted(
            catalog: &Arc<CayenneCatalog>,
            data_dir: &Path,
            runs: usize,
        ) -> Vec<(String, String)> {
            let table_id = catalog.get_table(NAME).await.expect("table").table_id;
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                let registered = catalog.list_index_runs(&table_id).await.expect("list runs");
                if registered.len() >= runs && run_files(data_dir) == registered.len() {
                    let mut names: Vec<(String, String)> = registered
                        .into_iter()
                        .map(|run| (run.index_key, run.run_name))
                        .collect();
                    names.sort();
                    return names;
                }
                assert!(
                    Instant::now() < deadline,
                    "the runs were not persisted: {} registered and {} run files, expected at least {runs}",
                    registered.len(),
                    run_files(data_dir)
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }

        /// The writer's two directories, as the runtime hands them to the
        /// snapshot engine.
        fn snapshot_dirs(meta: &Path, data: &Path) -> Vec<(PathBuf, String)> {
            vec![
                (meta.to_path_buf(), "metadata/".to_string()),
                (data.to_path_buf(), "data/".to_string()),
            ]
        }

        /// The engine's plan for a snapshot of the writer's directories: the
        /// metastore slice is exported here.
        async fn snapshot_plan(
            catalog: &Arc<CayenneCatalog>,
            meta: &Path,
            data: &Path,
        ) -> DirectorySnapshotPlan {
            CayenneSnapshotEngine::new(
                Arc::clone(catalog) as Arc<dyn MetadataCatalog>,
                NAME,
                data.to_path_buf(),
            )
            .prepare_directory_snapshot(&snapshot_dirs(meta, data), NAME)
            .await
            .expect("prepare snapshot")
        }

        /// Writes the archive `plan` describes, as the runtime does.
        async fn archive_with(
            plan: &DirectorySnapshotPlan,
            meta: &Path,
            data: &Path,
            archive: &Path,
        ) -> Result<u64, runtime_acceleration::snapshot::directory_archive::ArchiveError> {
            let extras: Vec<(String, Vec<u8>)> = plan
                .extra_entries
                .iter()
                .map(|extra| (extra.archive_path.clone(), extra.bytes.clone()))
                .collect();
            let optional_files: Vec<(PathBuf, String)> = plan
                .optional_files
                .iter()
                .map(|file| (file.source.clone(), file.archive_path.clone()))
                .collect();
            archive_directories_to_file_with_plan(
                &snapshot_dirs(meta, data),
                archive,
                &plan.skip_relative_paths.iter().cloned().collect::<Vec<_>>(),
                &extras,
                &optional_files,
            )
            .await
        }

        /// Archives the writer's two directories as an acceleration snapshot.
        async fn snapshot(catalog: &Arc<CayenneCatalog>, meta: &Path, data: &Path, archive: &Path) {
            let plan = snapshot_plan(catalog, meta, data).await;
            archive_with(&plan, meta, data, archive)
                .await
                .expect("archive");
        }

        /// Every run file under `dir`.
        fn run_file_paths(dir: &Path) -> Vec<PathBuf> {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return Vec::new();
            };
            entries
                .map(|entry| entry.expect("dir entry").path())
                .flat_map(|path| {
                    if path.is_dir() {
                        run_file_paths(&path)
                    } else if path.extension().is_some_and(|ext| ext == "run") {
                        vec![path]
                    } else {
                        Vec::new()
                    }
                })
                .collect()
        }

        /// Bootstraps a reader from `archive` into `root`'s own directories.
        async fn restore(archive: &Path, root: &Path) -> (Arc<CayenneCatalog>, PathBuf) {
            let (meta, data) = (root.join("metadata"), root.join("data"));
            std::fs::create_dir_all(&meta).expect("mkdir");
            std::fs::create_dir_all(&data).expect("mkdir");
            let catalog = fresh_catalog(&meta).await;
            extract_archive_file_with_options(
                archive,
                &meta,
                ExtractOptions {
                    prefix_mappings: Some(vec![
                        ("metadata/".to_string(), meta.clone()),
                        ("data/".to_string(), data.clone()),
                    ]),
                    ..ExtractOptions::skip_existing()
                },
            )
            .await
            .expect("extract");
            CayenneSnapshotEngine::new(
                Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
                NAME,
                data.clone(),
            )
            .finalize_directory_snapshot(
                &[
                    (meta.clone(), "metadata/".to_string()),
                    (data.clone(), "data/".to_string()),
                ],
                NAME,
            )
            .await
            .expect("finalize snapshot");
            (catalog, data)
        }

        /// A writer with two persisted runs; returns its catalog and data dir.
        async fn writer(
            env: &Arc<RuntimeEnv>,
            root: &Path,
        ) -> (Arc<CayenneCatalog>, PathBuf, PathBuf) {
            let (meta, data) = (root.join("metadata"), root.join("data"));
            std::fs::create_dir_all(&meta).expect("mkdir");
            std::fs::create_dir_all(&data).expect("mkdir");
            let catalog = fresh_catalog(&meta).await;
            let table = open(env, Arc::clone(&catalog), &data, IndexPersistence::Enabled).await;
            append(&table, rows(0, 20_000)).await;
            append(&table, rows(20_000, 5_000)).await;
            persisted(&catalog, &data, 2).await;
            (catalog, meta, data)
        }

        /// A table's persisted secondary index runs travel with its
        /// acceleration snapshot: the run files are archived with the data
        /// directory, their registrations with the metastore slice, and a
        /// reader that bootstraps into directories of its own reopens covered
        /// by them — no build, and a first lookup answered from the index. The
        /// same restore without persistence starts uncovered.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn persisted_index_runs_travel_with_a_snapshot() {
            let env = Arc::new(RuntimeEnv::default());
            let tmp = tempfile::tempdir().expect("tmp");
            let (catalog, meta, data) = writer(&env, &tmp.path().join("w")).await;
            let table_id = catalog.get_table(NAME).await.expect("table").table_id;
            let expected = persisted(&catalog, &data, 2).await;
            let archive = tmp.path().join("snapshot.tar");
            snapshot(&catalog, &meta, &data, &archive).await;

            let (reader_catalog, reader_data) = restore(&archive, &tmp.path().join("r")).await;
            assert_eq!(
                reader_catalog
                    .get_table(NAME)
                    .await
                    .expect("table")
                    .table_id,
                table_id,
                "the restore keeps the table's id, which its run files are filed under"
            );
            assert_eq!(
                persisted(&reader_catalog, &reader_data, expected.len()).await,
                expected,
                "the snapshot carries every run, registered and on disk"
            );
            let reader = open(
                &env,
                reader_catalog,
                &reader_data,
                IndexPersistence::Enabled,
            )
            .await;
            let verification = reader
                .verify_lookup_index_against_read_back()
                .await
                .expect("verify");
            assert!(verification.agrees(), "{verification:?}");
            let counters = reader.lookup_index_counters().expect("indexed");
            assert_eq!(
                (verification.uncovered_files, counters.builds_started),
                (0, 0),
                "a restored table must be covered by its persisted runs, not by a build: {verification:?}"
            );
            assert_eq!(lookup(&reader, 24_007).await, 1);
            let after = reader.lookup_index_counters().expect("indexed");
            assert_eq!(
                (after.full - counters.full, after.none - counters.none),
                (1, 0),
                "the first lookup after the restore did not use the loaded index: {after:?}"
            );
            drop(reader);

            let (control_catalog, control_data) = restore(&archive, &tmp.path().join("c")).await;
            let control = open(
                &env,
                control_catalog,
                &control_data,
                IndexPersistence::Disabled,
            )
            .await;
            let verification = control
                .verify_lookup_index_against_read_back()
                .await
                .expect("verify");
            assert!(
                verification.files == 0 && verification.uncovered_files > 0,
                "without persistence a restored table starts uncovered: {verification:?}"
            );
        }

        /// The background sync deletes a merged or retired run's file whenever
        /// it likes, including while a snapshot is being written. A run file
        /// that disappears under the archive must not fail the snapshot: here
        /// a thread keeps deleting and rewriting the registered run files
        /// while snapshots are taken, and every one of them must succeed.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn a_run_file_deleted_while_the_snapshot_is_written_does_not_fail_it() {
            const SNAPSHOTS: usize = 200;
            let env = Arc::new(RuntimeEnv::default());
            let tmp = tempfile::tempdir().expect("tmp");
            let (catalog, meta, data) = writer(&env, &tmp.path().join("w")).await;
            let runs: Vec<(PathBuf, Vec<u8>)> = run_file_paths(&data)
                .into_iter()
                .map(|path| {
                    let bytes = std::fs::read(&path).expect("read run");
                    (path, bytes)
                })
                .collect();
            assert_eq!(runs.len(), 2, "the writer persisted two runs");
            let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let churn = {
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || {
                    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                        // Rewritten the way the object store writes one: to a
                        // temporary file, then renamed into place.
                        for (path, bytes) in &runs {
                            let _ = std::fs::remove_file(path);
                            let staged = path.with_extension("staged");
                            std::fs::write(&staged, bytes).expect("stage run");
                            std::fs::rename(&staged, path).expect("rewrite run");
                        }
                    }
                })
            };
            let archive = tmp.path().join("snapshot.tar");
            let mut failures = Vec::new();
            for _ in 0..SNAPSHOTS {
                let plan = snapshot_plan(&catalog, &meta, &data).await;
                if let Err(error) = archive_with(&plan, &meta, &data, &archive).await {
                    failures.push(error.to_string());
                }
            }
            stop.store(true, std::sync::atomic::Ordering::Relaxed);
            churn.join().expect("churn thread");
            assert!(
                failures.is_empty(),
                "{} of {SNAPSHOTS} snapshots failed; first: {:?}",
                failures.len(),
                failures.first()
            );
        }

        /// A run the metastore slice registers but whose file was deleted
        /// before the archive reached it is left out of the snapshot. The
        /// restored table then holds a registration with no file: it drops
        /// that registration, answers lookups correctly by reading the lost
        /// run's files in full, and indexes them again in the background
        /// until every file is covered.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn a_snapshot_missing_a_registered_run_file_restores_and_heals() {
            let env = Arc::new(RuntimeEnv::default());
            let tmp = tempfile::tempdir().expect("tmp");
            let (catalog, meta, data) = writer(&env, &tmp.path().join("w")).await;
            let plan = snapshot_plan(&catalog, &meta, &data).await;
            // After the slice is exported, before the archive is written.
            let lost = run_file_paths(&data)
                .into_iter()
                .next()
                .expect("a persisted run");
            std::fs::remove_file(&lost).expect("delete a registered run file");
            let archive = tmp.path().join("snapshot.tar");
            archive_with(&plan, &meta, &data, &archive)
                .await
                .expect("archive");

            let (reader_catalog, reader_data) = restore(&archive, &tmp.path().join("r")).await;
            let table_id = reader_catalog
                .get_table(NAME)
                .await
                .expect("table")
                .table_id;
            assert_eq!(
                reader_catalog
                    .list_index_runs(&table_id)
                    .await
                    .expect("list runs")
                    .len(),
                2,
                "the slice still registers the lost run"
            );
            assert_eq!(run_files(&reader_data), 1, "the archive holds one run file");
            let reader = open(
                &env,
                Arc::clone(&reader_catalog),
                &reader_data,
                IndexPersistence::Enabled,
            )
            .await;
            assert_eq!(
                persisted(&reader_catalog, &reader_data, 1).await.len(),
                1,
                "opening drops the registration whose file is missing"
            );
            let restored = reader
                .verify_lookup_index_against_read_back()
                .await
                .expect("verify");
            assert!(
                restored.agrees() && restored.files > 0 && restored.uncovered_files > 0,
                "the remaining run covers its files and the lost run's are uncovered: {restored:?}"
            );
            // A key from each write: correct whether its files are covered or
            // read in full, and the lookups request the missing files' build.
            for id in [7, 9_999, 20_011, 24_999] {
                assert_eq!(lookup(&reader, id).await, 1, "lookup of row {id}");
            }
            let deadline = Instant::now() + Duration::from_mins(1);
            let healed = loop {
                let verification = reader
                    .verify_lookup_index_against_read_back()
                    .await
                    .expect("verify");
                if verification.uncovered_files == 0 {
                    break verification;
                }
                assert!(
                    Instant::now() < deadline,
                    "the restored index never healed: {verification:?}"
                );
                lookup(&reader, 7).await;
                tokio::time::sleep(Duration::from_millis(100)).await;
            };
            assert!(healed.agrees(), "{healed:?}");
            for id in [7, 9_999, 20_011, 24_999] {
                assert_eq!(
                    lookup(&reader, id).await,
                    1,
                    "lookup of row {id} after healing"
                );
            }
            assert!(
                persisted(&reader_catalog, &reader_data, 2).await.len() >= 2,
                "the rebuilt runs are persisted again"
            );
        }

        /// A snapshot taken while the index is behind — a write whose run was
        /// not persisted yet — restores with that write's files uncovered.
        /// Lookups stay correct by reading them in full, and the first lookup
        /// that meets them starts the background build that indexes them, so
        /// the restored table catches up with no step of its own.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn a_snapshot_taken_behind_the_index_catches_up_after_restore() {
            let env = Arc::new(RuntimeEnv::default());
            let tmp = tempfile::tempdir().expect("tmp");
            let (catalog, meta, data) = writer(&env, &tmp.path().join("w")).await;
            // A third write whose run is never persisted: the writer reopened
            // without persistence leaves the other two runs where they are.
            let behind = open(
                &env,
                Arc::clone(&catalog),
                &data,
                IndexPersistence::Disabled,
            )
            .await;
            append(&behind, rows(25_000, 5_000)).await;
            drop(behind);
            assert_eq!(
                persisted(&catalog, &data, 2).await.len(),
                2,
                "the third run is not persisted"
            );
            let archive = tmp.path().join("snapshot.tar");
            snapshot(&catalog, &meta, &data, &archive).await;

            let (reader_catalog, reader_data) = restore(&archive, &tmp.path().join("r")).await;
            let reader = open(
                &env,
                reader_catalog,
                &reader_data,
                IndexPersistence::Enabled,
            )
            .await;
            let restored = reader
                .verify_lookup_index_against_read_back()
                .await
                .expect("verify");
            assert!(
                restored.files > 0 && restored.uncovered_files > 0,
                "the restored runs cover the persisted writes only: {restored:?}"
            );

            // A key the unpersisted write holds: found by reading its files in
            // full, and the lookup that did so asks for them to be indexed.
            // Two queries meet the uncovered files at once: each is correct,
            // and only one background build starts between them.
            let (first, second) = tokio::join!(lookup(&reader, 27_003), lookup(&reader, 28_004));
            assert_eq!((first, second), (1, 1), "lookups are correct while behind");
            let deadline = Instant::now() + Duration::from_mins(1);
            let caught_up = loop {
                let verification = reader
                    .verify_lookup_index_against_read_back()
                    .await
                    .expect("verify");
                if verification.uncovered_files == 0 {
                    break verification;
                }
                assert!(
                    Instant::now() < deadline,
                    "the restored index never caught up: {verification:?}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            };
            assert!(caught_up.agrees(), "{caught_up:?}");
            let counters = reader.lookup_index_counters().expect("indexed");
            assert_eq!(
                counters.builds_started, 1,
                "two concurrent lookups started exactly one background build: {counters:?}"
            );
            let before = counters;
            assert_eq!(lookup(&reader, 27_003).await, 1);
            let after = reader.lookup_index_counters().expect("indexed");
            assert_eq!(
                (after.full - before.full, after.none - before.none),
                (1, 0),
                "once caught up, the lookup is answered from the index: {after:?}"
            );
        }
    }
}
