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

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use cayenne::MetadataCatalog;
use cayenne::metastore::EXPECTED_TABLES;
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

/// `path` relative to `anchor`, whether the slice stores it relative (as the
/// export writes it) or absolute. `None` for a path outside `anchor`, or one
/// that could climb out of it.
fn relative_to_anchor(path: &str, anchor: &Path) -> Option<PathBuf> {
    let path = Path::new(path);
    let relative = path.strip_prefix(anchor).unwrap_or(path);
    relative
        .components()
        .all(|c| matches!(c, std::path::Component::Normal(_)))
        .then(|| relative.to_path_buf())
}

/// Snapshot ids the slice references for the table `table_id`, whose data is
/// written under `root` (relative to `anchor`): its current snapshot, its
/// protected snapshots, and the snapshots whose `deletions/` directory holds
/// one of its referenced deletion files.
fn referenced_snapshot_ids(
    slice: &DatasetMetastoreSlice,
    anchor: &Path,
    table_id: &str,
    root: &Path,
) -> HashSet<String> {
    let rows = |table: &str| {
        let id = slice_column(table, "table_id");
        slice
            .tables
            .get(table)
            .into_iter()
            .flatten()
            .filter(move |row| slice_text(row, id) == Some(table_id))
    };
    let mut ids = HashSet::new();
    let current = slice_column("cayenne_table", "current_snapshot_id");
    for row in rows("cayenne_table") {
        ids.extend(slice_text(row, current).map(str::to_string));
    }
    let snapshot_id = slice_column("cayenne_snapshot_sequence", "snapshot_id");
    for row in rows("cayenne_snapshot_sequence") {
        ids.extend(slice_text(row, snapshot_id).map(str::to_string));
    }
    let path = slice_column("cayenne_delete_file", "path");
    for row in rows("cayenne_delete_file") {
        let Some(relative) = slice_text(row, path).and_then(|p| relative_to_anchor(p, anchor))
        else {
            continue;
        };
        let Ok(in_root) = relative.strip_prefix(root) else {
            continue;
        };
        if let Some(first) = in_root.components().next() {
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
///
/// A partitioned acceleration's partitions are child tables of their own,
/// each rooted at its Hive directory under the data directory, with the same
/// layout as the parent: each is pruned against its own references, and a
/// partition directory is never skipped from the root that encloses it, and
/// write state such as `_partitioned_wal/` is skipped like `_staging`. A
/// table partitioned through `partition_column`, or a slice whose tables do
/// not have distinct roots, is archived whole.
async fn unreferenced_data_entries(
    anchor: &Path,
    slice: &DatasetMetastoreSlice,
) -> std::io::Result<HashSet<PathBuf>> {
    let mut skip = HashSet::new();
    let tables = slice
        .tables
        .get("cayenne_table")
        .map(Vec::as_slice)
        .unwrap_or_default();
    let name = slice_column("cayenne_table", "table_name");
    let parent = tables
        .iter()
        .position(|row| slice_text(row, name) == Some(slice.dataset_name.as_str()))
        .or_else(|| (!tables.is_empty()).then_some(0));
    if parent
        .and_then(|i| {
            slice_text(
                &tables[i],
                slice_column("cayenne_table", "partition_column"),
            )
        })
        .is_some()
    {
        return Ok(skip);
    }

    // Each table's `table_id` and data root. The parent is rooted at `anchor`;
    // a child at its own `path`, and one outside `anchor` is not archived.
    let table_id = slice_column("cayenne_table", "table_id");
    let table_path = slice_column("cayenne_table", "path");
    let mut roots: Vec<(String, PathBuf)> = Vec::new();
    let mut seen_roots: HashSet<PathBuf> = HashSet::new();
    for (i, row) in tables.iter().enumerate() {
        let Some(id) = slice_text(row, table_id) else {
            continue;
        };
        let root = if Some(i) == parent {
            PathBuf::new()
        } else {
            match slice_text(row, table_path).and_then(|p| relative_to_anchor(p, anchor)) {
                Some(root) => root,
                None => continue,
            }
        };
        if !seen_roots.insert(root.clone()) {
            return Ok(skip);
        }
        roots.push((id.to_string(), root));
    }

    // Partition directories, and every directory above one, stay in the
    // archive whatever the enclosing table references.
    let partition_path = slice_column("cayenne_partition", "path");
    let mut keep: HashSet<PathBuf> = HashSet::new();
    let partition_roots = slice
        .tables
        .get("cayenne_partition")
        .into_iter()
        .flatten()
        .filter_map(|row| {
            slice_text(row, partition_path).and_then(|p| relative_to_anchor(p, anchor))
        });
    for root in roots
        .iter()
        .map(|(_, root)| root.clone())
        .chain(partition_roots)
    {
        keep.extend(
            root.ancestors()
                .filter(|a| !a.as_os_str().is_empty())
                .map(Path::to_path_buf),
        );
    }

    for (id, root) in &roots {
        let referenced = referenced_snapshot_ids(slice, anchor, id, root);
        let mut entries = match tokio::fs::read_dir(anchor.join(root)).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err),
        };
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name().to_string_lossy().into_owned();
            let relative = root.join(&name);
            if keep.contains(&relative) {
                continue;
            }
            if name == *id {
                // `<table_id>/<snapshot_id>/`: keep the referenced snapshots.
                let mut children = tokio::fs::read_dir(entry.path()).await?;
                while let Some(child) = children.next_entry().await? {
                    let child_name = child.file_name().to_string_lossy().into_owned();
                    if !referenced.contains(&child_name) {
                        skip.insert(relative.join(child_name));
                    }
                }
            } else if !referenced.contains(&name) {
                // `<snapshot_id>/deletions/` of a snapshot nothing references.
                skip.insert(relative);
            }
        }
    }
    Ok(skip)
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

        Ok(DirectorySnapshotPlan {
            skip_relative_paths: skip,
            extra_entries: extras,
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

    /// One row of `table`, with each named column set to its text value and
    /// every other column `NULL`.
    fn slice_row(table: &str, values: &[(&str, &str)]) -> Vec<SliceValue> {
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
        let mut tables = std::collections::BTreeMap::new();
        let mut table_values = vec![("table_id", table_id), ("current_snapshot_id", current)];
        if let Some(column) = partition_column {
            table_values.push(("partition_column", column));
        }
        tables.insert(
            "cayenne_table".to_string(),
            vec![slice_row("cayenne_table", &table_values)],
        );
        tables.insert(
            "cayenne_snapshot_sequence".to_string(),
            protected
                .iter()
                .map(|id| {
                    slice_row(
                        "cayenne_snapshot_sequence",
                        &[("table_id", table_id), ("snapshot_id", id)],
                    )
                })
                .collect(),
        );
        tables.insert(
            "cayenne_delete_file".to_string(),
            delete_paths
                .iter()
                .map(|path| {
                    slice_row(
                        "cayenne_delete_file",
                        &[("table_id", table_id), ("path", path)],
                    )
                })
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

        // A partitioned table is archived as a whole.
        let partitioned = slice_with_partition("tid", "current", &[], &[], Some("region"));
        assert!(
            unreferenced_data_entries(anchor, &partitioned)
                .await
                .expect("list")
                .is_empty()
        );
    }

    /// The accelerator creates a partitioned acceleration's parent table
    /// unpartitioned and wraps it, so in production the parent row's
    /// `partition_column` is empty and only the slice's `cayenne_partition` rows
    /// say it is partitioned. Its partitions' Hive directories sit directly
    /// under the data directory, with each child's snapshots inside, and every
    /// one of them has to reach the archive.
    #[tokio::test]
    async fn a_partitioned_slice_keeps_its_partition_directories_without_a_partition_column() {
        let tmp = tempfile::tempdir().expect("tmp");
        let anchor = tmp.path();
        for dir in [
            "tid/current",
            "bucket=alpha/child-a/snap-a",
            "bucket=beta/child-b/snap-b",
        ] {
            std::fs::create_dir_all(anchor.join(dir)).expect("mkdir");
        }
        let mut slice = slice_with("tid", "current", &[], &[]);
        slice.tables.insert(
            "cayenne_partition".to_string(),
            ["bucket=alpha", "bucket=beta"]
                .iter()
                .map(|dir| {
                    slice_row(
                        "cayenne_partition",
                        &[
                            ("table_id", "tid"),
                            ("path", &anchor.join(dir).to_string_lossy()),
                        ],
                    )
                })
                .collect(),
        );

        let skip = unreferenced_data_entries(anchor, &slice)
            .await
            .expect("list");
        for dir in ["bucket=alpha", "bucket=beta"] {
            assert!(
                !skip.contains(&PathBuf::from(dir)),
                "partition directory '{dir}' must be archived, but the plan skips it: {skip:?}"
            );
        }
    }

    /// A partition child is pruned against its own references, under its own
    /// root: its retired snapshot, staging state and unreferenced `deletions/`
    /// are skipped, and nothing it or the parent references is. The partition
    /// directory itself is never skipped from the parent's root, although the
    /// parent references nothing by that name.
    #[tokio::test]
    async fn a_partition_child_is_pruned_against_its_own_references() {
        let tmp = tempfile::tempdir().expect("tmp");
        let anchor = tmp.path();
        for dir in [
            "tid/current",
            "tid/retired",
            "_partitioned_wal",
            "region=eu/cid/child-current",
            "region=eu/cid/child-protected",
            "region=eu/cid/child-retired",
            "region=eu/cid/_staging",
            "region=eu/child-current/deletions",
            "region=eu/child-retired/deletions",
            "region=us/did/us-current",
            "region=us/did/us-retired",
        ] {
            std::fs::create_dir_all(anchor.join(dir)).expect("mkdir");
        }
        let mut slice = slice_with("tid", "current", &[], &[]);
        let child = |id: &str, root: &str, current: &str| {
            slice_row(
                "cayenne_table",
                &[
                    ("table_id", id),
                    ("path", root),
                    ("current_snapshot_id", current),
                ],
            )
        };
        let cayenne_table = slice.tables.get_mut("cayenne_table").expect("parent");
        // One child path as the export writes it (relative), one absolute.
        cayenne_table.push(child("cid", "region=eu", "child-current"));
        cayenne_table.push(child(
            "did",
            &anchor.join("region=us").to_string_lossy(),
            "us-current",
        ));
        slice
            .tables
            .get_mut("cayenne_snapshot_sequence")
            .expect("sequence")
            .push(slice_row(
                "cayenne_snapshot_sequence",
                &[("table_id", "cid"), ("snapshot_id", "child-protected")],
            ));
        slice
            .tables
            .get_mut("cayenne_delete_file")
            .expect("deletes")
            .push(slice_row(
                "cayenne_delete_file",
                &[
                    ("table_id", "cid"),
                    ("path", "region=eu/child-current/deletions/delete_1.arrow"),
                ],
            ));

        let skip = unreferenced_data_entries(anchor, &slice)
            .await
            .expect("list");
        let expected: HashSet<PathBuf> = [
            "tid/retired",
            // An in-flight cross-partition commit anchor is write state, like
            // `_staging`: restored beside a committed slice, it would describe
            // a commit the slice does not hold.
            "_partitioned_wal",
            "region=eu/cid/child-retired",
            "region=eu/cid/_staging",
            "region=eu/child-retired",
            "region=us/did/us-retired",
        ]
        .iter()
        .map(PathBuf::from)
        .collect();
        assert_eq!(skip, expected);
    }

    /// Two tables in one slice with the same data root cannot be pruned
    /// separately (each would skip the other's snapshots), so the dataset is
    /// archived whole.
    #[tokio::test]
    async fn tables_sharing_a_root_are_archived_whole() {
        let tmp = tempfile::tempdir().expect("tmp");
        let anchor = tmp.path();
        for dir in ["tid/current", "tid/retired", "cid/child-current"] {
            std::fs::create_dir_all(anchor.join(dir)).expect("mkdir");
        }
        let mut slice = slice_with("tid", "current", &[], &[]);
        slice
            .tables
            .get_mut("cayenne_table")
            .expect("parent")
            .push(slice_row(
                "cayenne_table",
                &[
                    ("table_id", "cid"),
                    ("path", &anchor.to_string_lossy()),
                    ("current_snapshot_id", "child-current"),
                ],
            ));
        assert!(
            unreferenced_data_entries(anchor, &slice)
                .await
                .expect("list")
                .is_empty()
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
        archive_directories_to_file_with_plan(&dirs, &tar, &skip, &extras)
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

    /// A partitioned acceleration round-trips through a real archive, partition
    /// data included. The parent is created the way the accelerator creates it,
    /// with `partition_column: None`, and its partitions through
    /// `CayennePartitionCreator`, so each child table is rooted at its Hive
    /// directory under the data directory. One partition is then fully
    /// refreshed, which retires its first snapshot directory: the plan must skip
    /// it, as it does for an unpartitioned table, because the sweep removes it
    /// while the archive is written. After the archive is extracted and its
    /// slice imported, every partition reads back its current rows.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_partitioned_dataset_round_trips_through_an_archive_with_its_partition_data() {
        use arrow::array::{Int64Array, RecordBatch, StringArray};
        use cayenne::{CayennePartitionCreator, CayenneTableProviderBuilder};
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::logical_expr::dml::InsertOp;
        use datafusion::physical_plan::collect;
        use datafusion::prelude::{SessionContext, col};
        use datafusion::scalar::ScalarValue;
        use datafusion_table_providers::UnsupportedTypeAction;
        use runtime_acceleration::snapshot::directory_archive::{
            ExtractOptions, archive_directories_to_file_with_plan,
            extract_archive_file_with_options,
        };
        use runtime_table_partition::creator::PartitionCreator;
        use runtime_table_partition::expression::PartitionedBy;

        let schema: Arc<arrow_schema::Schema> = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("bucket", arrow_schema::DataType::Utf8, false),
        ]));
        // Write-entry inlining off, so each partition's rows land in files under
        // its Hive directory — the files the archive has to carry.
        let vortex_config = cayenne::metadata::VortexConfig {
            inline_max_rows: 0,
            inline_max_bytes: 0,
            ..cayenne::metadata::VortexConfig::default()
        };
        let ctx = SessionContext::new();
        let creator_over = |catalog: &Arc<CayenneCatalog>, data_dir: &std::path::Path, table_id| {
            CayennePartitionCreator::new(
                "trips".to_string(),
                data_dir.to_path_buf(),
                vec![PartitionedBy {
                    name: "bucket".to_string(),
                    expression: col("bucket"),
                }],
                Arc::clone(&schema),
                Arc::clone(catalog) as Arc<dyn MetadataCatalog>,
                table_id,
                UnsupportedTypeAction::Error,
                Vec::new(),
                None,
                vortex_config.clone(),
                None,
                Vec::new(),
                None,
                ctx.runtime_env(),
            )
        };

        let tmp = tempfile::tempdir().expect("tmp");
        let metadata_dir = tmp.path().join("writer").join("metadata");
        let data_dir = tmp.path().join("writer").join("trips");
        std::fs::create_dir_all(&metadata_dir).expect("mkdir metadata");
        std::fs::create_dir_all(&data_dir).expect("mkdir data");
        let catalog = fresh_catalog(&metadata_dir).await;
        CayenneTableProviderBuilder::new(
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
            ctx.runtime_env(),
        )
        .create(CreateTableOptions {
            table_name: "trips".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec![],
            on_conflict: None,
            base_path: data_dir.to_string_lossy().into_owned(),
            // As the accelerator creates a partitioned acceleration's parent.
            partition_column: None,
            vortex_config: vortex_config.clone(),
        })
        .await
        .expect("create parent");
        let table_id = catalog.get_table("trips").await.expect("meta").table_id;
        let creator = creator_over(&catalog, &data_dir, table_id);
        // The Hive directory a `bucket` partition is written under.
        let partition_dir = |bucket: &str| -> PathBuf {
            runtime_table_partition::creator::filename::to_hive_partition_dir(&[(
                PartitionedBy {
                    name: "bucket".to_string(),
                    expression: col("bucket"),
                },
                ScalarValue::Utf8(Some(bucket.to_string())),
            )])
            .expect("partition dir")
        };
        // `<partition dir>/<child table_id>/<snapshot_id>` directories on disk.
        let snapshot_dirs = |bucket: &str| -> HashSet<PathBuf> {
            let partition_dir = partition_dir(bucket);
            let mut dirs = HashSet::new();
            for child in std::fs::read_dir(data_dir.join(&partition_dir)).expect("read partition") {
                let child = child.expect("entry");
                if !child.path().is_dir() {
                    continue;
                }
                for snapshot in std::fs::read_dir(child.path()).expect("read child") {
                    let snapshot = snapshot.expect("entry");
                    // `_staging` is write state, not a snapshot.
                    if snapshot.path().is_dir()
                        && !snapshot.file_name().to_string_lossy().starts_with('_')
                    {
                        dirs.insert(
                            partition_dir
                                .join(child.file_name())
                                .join(snapshot.file_name()),
                        );
                    }
                }
            }
            dirs
        };
        let mut retired = HashSet::new();
        for (bucket, rows, op) in [
            ("alpha", 10_usize, InsertOp::Append),
            ("beta", 20, InsertOp::Append),
            // The full refresh of `alpha` retires its first snapshot directory.
            ("alpha", 5, InsertOp::Overwrite),
        ] {
            if op == InsertOp::Overwrite {
                retired = snapshot_dirs("alpha");
                assert_eq!(retired.len(), 1, "alpha has one snapshot: {retired:?}");
            }
            let partition = creator
                .create_partition(vec![ScalarValue::Utf8(Some(bucket.to_string()))])
                .await
                .expect("create partition");
            let ids: Vec<i64> = (0..i64::try_from(rows).expect("small")).collect();
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(StringArray::from(vec![bucket; rows])),
                ],
            )
            .expect("batch");
            let input = MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)
                .expect("exec");
            let plan = partition
                .table_provider
                .insert_into(&ctx.state(), input, op)
                .await
                .expect("plan");
            collect(plan, ctx.task_ctx()).await.expect("write");
        }
        let current: HashSet<PathBuf> = snapshot_dirs("alpha")
            .difference(&retired)
            .cloned()
            .collect();
        assert_eq!(
            current.len(),
            1,
            "the refresh wrote one new snapshot and the retired one is still on disk"
        );

        let dirs = vec![
            (metadata_dir.clone(), "metadata/".to_string()),
            (data_dir.clone(), "data/".to_string()),
        ];
        let plan = CayenneSnapshotEngine::new(
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
            "trips",
            data_dir.clone(),
        )
        .prepare_directory_snapshot(&dirs, "trips")
        .await
        .expect("prepare");
        for dir in &retired {
            assert!(
                plan.skip_relative_paths.contains(dir),
                "the retired partition snapshot {dir:?} must be skipped: {:?}",
                plan.skip_relative_paths
            );
        }
        for dir in current.iter().chain(&snapshot_dirs("beta")) {
            assert!(
                !plan
                    .skip_relative_paths
                    .iter()
                    .any(|skipped| dir.starts_with(skipped)),
                "the current partition snapshot {dir:?} must be archived: {:?}",
                plan.skip_relative_paths
            );
        }
        let tar = tmp.path().join("snapshot.tar");
        let skip: Vec<PathBuf> = plan.skip_relative_paths.into_iter().collect();
        let extras: Vec<(String, Vec<u8>)> = plan
            .extra_entries
            .into_iter()
            .map(|e| (e.archive_path, e.bytes))
            .collect();
        archive_directories_to_file_with_plan(&dirs, &tar, &skip, &extras)
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
        for dir in &retired {
            assert!(
                !reader_data.join(dir).exists(),
                "the retired partition snapshot {dir:?} must not be in the archive"
            );
        }
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

        let reader_table_id = reader_catalog
            .get_table("trips")
            .await
            .expect("the restored parent is registered")
            .table_id;
        let mut read_back: Vec<(String, usize)> = Vec::new();
        for partition in creator_over(&reader_catalog, &reader_data, reader_table_id)
            .infer_existing_partitions()
            .await
            .expect("every restored partition opens")
        {
            let [ScalarValue::Utf8(Some(bucket))] = partition.partition_values.as_slice() else {
                panic!(
                    "expected one Utf8 partition value, got {:?}",
                    partition.partition_values
                );
            };
            let rows: usize = SessionContext::new()
                .read_table(Arc::clone(&partition.table_provider))
                .expect("read")
                .collect()
                .await
                .expect("collect")
                .iter()
                .map(RecordBatch::num_rows)
                .sum();
            read_back.push((bucket.clone(), rows));
        }
        read_back.sort();
        assert_eq!(
            read_back,
            vec![("alpha".to_string(), 5), ("beta".to_string(), 20)],
            "every partition reads back its current rows"
        );
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
