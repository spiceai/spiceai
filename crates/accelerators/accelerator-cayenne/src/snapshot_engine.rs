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
//! Cayenne stores per-table metadata in a shared SQLite database
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

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};

use async_trait::async_trait;
use cayenne::MetadataCatalog;
use cayenne::metastore::EXPECTED_TABLES;
use cayenne::metastore::snapshot::{DatasetMetastoreSlice, SliceValue};
use datafusion::datasource::TableProvider;
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
/// archive: `cayenne.db` and the files the metastore keeps beside it. The
/// `SQLite` metastore runs in WAL journal mode, so its `-wal` and `-shm`
/// sidecars may be present. A reader holds its own copies of these files, so
/// an archived one fails the restore's checksum check.
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

/// The snapshots a slice references, grouped by `table_id` in one pass over
/// its rows, so that a slice with many partition tables is not rescanned once
/// per table.
struct SnapshotReferences<'a> {
    /// Each table's current and protected snapshot ids.
    ids: HashMap<&'a str, HashSet<&'a str>>,
    /// Each table's deletion files, relative to the anchor.
    delete_files: HashMap<&'a str, Vec<PathBuf>>,
}

impl<'a> SnapshotReferences<'a> {
    fn new(slice: &'a DatasetMetastoreSlice, anchor: &Path) -> Self {
        let rows = |table: &str| {
            let id = slice_column(table, "table_id");
            slice
                .tables
                .get(table)
                .into_iter()
                .flatten()
                .filter_map(move |row| slice_text(row, id).map(|id| (id, row)))
        };
        let mut ids: HashMap<&str, HashSet<&str>> = HashMap::new();
        let current = slice_column("cayenne_table", "current_snapshot_id");
        for (id, row) in rows("cayenne_table") {
            let entry = ids.entry(id).or_default();
            entry.extend(slice_text(row, current));
        }
        let snapshot_id = slice_column("cayenne_snapshot_sequence", "snapshot_id");
        for (id, row) in rows("cayenne_snapshot_sequence") {
            let entry = ids.entry(id).or_default();
            entry.extend(slice_text(row, snapshot_id));
        }
        let mut delete_files: HashMap<&str, Vec<PathBuf>> = HashMap::new();
        let path = slice_column("cayenne_delete_file", "path");
        for (id, row) in rows("cayenne_delete_file") {
            if let Some(relative) =
                slice_text(row, path).and_then(|p| relative_to_anchor(p, anchor))
            {
                delete_files.entry(id).or_default().push(relative);
            }
        }
        Self { ids, delete_files }
    }

    /// Snapshot ids the slice references for the table `table_id`, whose data
    /// is written under `root` (relative to the anchor): its current snapshot,
    /// its protected snapshots, and the snapshots whose `deletions/` directory
    /// holds one of its referenced deletion files.
    fn for_table(&self, table_id: &str, root: &Path) -> HashSet<String> {
        let mut ids: HashSet<String> = self
            .ids
            .get(table_id)
            .into_iter()
            .flatten()
            .map(|id| (*id).to_string())
            .collect();
        for relative in self.delete_files.get(table_id).into_iter().flatten() {
            let Ok(in_root) = relative.strip_prefix(root) else {
                continue;
            };
            if let Some(first) = in_root.components().next() {
                ids.insert(first.as_os_str().to_string_lossy().into_owned());
            }
        }
        ids
    }
}

/// The slice's `cayenne_table` rows, and the index of the dataset's own
/// (parent) table among them: the row named after the dataset, else the first.
fn slice_tables(slice: &DatasetMetastoreSlice) -> (&[Vec<SliceValue>], Option<usize>) {
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
    (tables, parent)
}

/// Each table's `table_id` and data root, relative to `anchor`. The parent is
/// rooted at `anchor`; a child at its own `path`, and one outside `anchor` is
/// left out. `None` when two tables share a root, so neither can be pruned
/// without skipping the other's snapshots.
fn table_roots(anchor: &Path, slice: &DatasetMetastoreSlice) -> Option<Vec<(String, PathBuf)>> {
    let (tables, parent) = slice_tables(slice);
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
            return None;
        }
        roots.push((id.to_string(), root));
    }
    Some(roots)
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
/// not have distinct roots, is archived whole, except its persisted index
/// runs: the background sync deletes their files while the archive is
/// written, so they are left out here and the registered ones are archived
/// file by file instead (see `persisted_run_files`).
async fn unreferenced_data_entries(
    anchor: &Path,
    slice: &DatasetMetastoreSlice,
) -> std::io::Result<HashSet<PathBuf>> {
    let mut skip = HashSet::new();
    let (tables, parent) = slice_tables(slice);
    let partitioned = parent
        .and_then(|i| {
            slice_text(
                &tables[i],
                slice_column("cayenne_table", "partition_column"),
            )
        })
        .is_some();
    let roots = match table_roots(anchor, slice) {
        Some(roots) if !partitioned => roots,
        _ => {
            lookup_index_dirs(anchor, Path::new(""), &mut skip).await?;
            return Ok(skip);
        }
    };

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

    let slice_references = SnapshotReferences::new(slice, anchor);
    for (id, root) in &roots {
        let referenced = slice_references.for_table(id, root);
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
                // `<table_id>/<snapshot_id>/`: keep the referenced snapshots, and
                // the persisted secondary index runs under `<table_id>/_lookup_index/`
                // are archived file by file instead (see `persisted_run_files`).
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
/// indexing that run's files again. A run is read from beside its table, under
/// that table's root (see `table_roots`). `data_prefix` is the data
/// directory's path inside the archive.
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
    let roots: HashMap<String, PathBuf> = table_roots(anchor, slice)
        .unwrap_or_default()
        .into_iter()
        .collect();
    slice
        .tables
        .get("cayenne_index_run")
        .into_iter()
        .flatten()
        .filter_map(|row| {
            let (table_id, index_key, run_name) = (
                slice_text(row, table_id)?,
                slice_text(row, index_key)?,
                slice_text(row, run_name)?,
            );
            if ![table_id, index_key, run_name]
                .into_iter()
                .all(is_file_name)
            {
                return None;
            }
            let relative = roots
                .get(table_id)
                .map_or_else(PathBuf::new, PathBuf::clone)
                .join(table_id)
                .join(cayenne::LOOKUP_INDEX_DIR_NAME)
                .join(index_key)
                .join(run_name);
            Some(DirectoryArchiveFile {
                source: anchor.join(&relative),
                archive_path: format!("{data_prefix}{}", relative.to_string_lossy()),
            })
        })
        .collect()
}

/// Metastore identifiers name one file or directory, never a path.
fn is_file_name(value: &str) -> bool {
    if value.contains(['/', '\\', '\0']) {
        return false;
    }
    let mut components = Path::new(value).components();
    matches!(components.next(), Some(std::path::Component::Normal(name)) if name == value)
        && components.next().is_none()
}

/// The provider serving each dataset's restored snapshot, by dataset name. Held
/// weakly so a replaced provider is not kept alive.
#[derive(Default)]
pub(crate) struct ServingProviders(parking_lot::Mutex<HashMap<String, Weak<dyn TableProvider>>>);

impl ServingProviders {
    /// Record `provider` as the one serving `dataset`'s restored snapshot.
    pub(crate) fn record(&self, dataset: String, provider: &Arc<dyn TableProvider>) {
        self.0.lock().insert(dataset, Arc::downgrade(provider));
    }

    fn get(&self, dataset: &str) -> Option<Arc<dyn TableProvider>> {
        self.0.lock().get(dataset).and_then(Weak::upgrade)
    }
}

/// Snapshot engine for Cayenne accelerators.
///
/// Holds an [`Arc<dyn MetadataCatalog>`] so it can call
/// [`MetadataCatalog::export_dataset_slice`] / `import_dataset_slice` against
/// the same metastore the accelerator is using at runtime.
pub struct CayenneSnapshotEngine {
    /// Cayenne metastore the engine talks to.
    catalog: Arc<dyn MetadataCatalog>,
    /// Logical dataset name (the value of `cayenne_table.table_name`).
    dataset_name: String,
    /// Local data directory anchor used to rewrite path columns relative
    /// on export and absolute on import. The export-side anchor must contain
    /// the absolute paths stored in the metastore as a strict prefix; the
    /// import-side anchor is where the new paths will be re-rooted.
    data_dir_anchor: PathBuf,
    /// The providers a restore quiesces before extracting.
    serving: Arc<ServingProviders>,
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
            serving: Arc::default(),
        }
    }

    /// Quiesce the provider `serving` records for this dataset before a restore.
    #[must_use]
    pub(crate) fn with_serving_providers(mut self, serving: Arc<ServingProviders>) -> Self {
        self.serving = serving;
        self
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

    /// Nothing to check: Cayenne snapshots are directory-layout, and only the
    /// file-layout restore invokes this hook.
    async fn verify_downloaded_file(
        &self,
        _downloaded: &std::path::Path,
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

    /// Quiesce the serving instance. It may have retired the directory of the
    /// snapshot being restored (a partial extraction left by a restart), and its
    /// sweep would delete files extraction keeps or writes there. It keeps
    /// serving queries until the reload swaps it out.
    async fn prepare_directory_restore(
        &self,
        _dirs: &[(PathBuf, String)],
        dataset_name: &str,
    ) -> Result<(), SnapshotEngineError> {
        if dataset_name != self.dataset_name {
            return Err(SnapshotEngineError::from_display(format!(
                "CayenneSnapshotEngine constructed for dataset '{}' but asked to restore '{}'",
                self.dataset_name, dataset_name
            )));
        }
        if let Some(provider) = self.serving.get(dataset_name) {
            crate::change_sink::quiesce_table_maintenance(&provider).await;
        }
        Ok(())
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
    use std::collections::HashMap;
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

    #[test]
    fn persisted_run_metadata_requires_single_file_names() {
        let columns = EXPECTED_TABLES
            .iter()
            .find(|table| table.name == "cayenne_index_run")
            .expect("index run schema")
            .columns;
        let valid: Vec<_> = columns
            .iter()
            .map(|column| match *column {
                "table_id" => SliceValue::Text("table".to_string()),
                "index_key" => SliceValue::Text("key".to_string()),
                "run_name" => SliceValue::Text("kept.run".to_string()),
                _ => SliceValue::Null,
            })
            .collect();
        for field in ["table_id", "index_key", "run_name"] {
            let slot = columns
                .iter()
                .position(|column| *column == field)
                .expect("path field");
            for malformed in [
                "",
                ".",
                "..",
                "/outside",
                "../outside",
                "nested/file",
                "name/",
                "name/.",
                "name\\file",
                "C:\\outside",
                "bad\0name",
            ] {
                let mut invalid = valid.clone();
                invalid[slot] = SliceValue::Text(malformed.to_string());
                let mut slice = slice_with("table", "snapshot", &[], &[]);
                slice.tables.insert(
                    "cayenne_index_run".to_string(),
                    vec![invalid, valid.clone()],
                );
                let files = persisted_run_files(Path::new("data"), "data/", &slice);
                assert_eq!(files.len(), 1, "{field}={malformed:?}");
                assert_eq!(
                    files[0].source,
                    Path::new("data/table/_lookup_index/key/kept.run")
                );
            }
        }
    }

    #[tokio::test]
    async fn snapshot_omits_absolute_persisted_run_paths() {
        let tmp = tempfile::tempdir().expect("fixture");
        let root = std::fs::canonicalize(tmp.path()).expect("canonical fixture root");
        let anchor = root.join("data");
        std::fs::create_dir(&anchor).expect("data directory");
        std::fs::write(anchor.join("rows.vortex"), b"table data").expect("table file");
        let outside = root.join("outside.txt");
        std::fs::write(&outside, b"OUTSIDE_DATA_DIRECTORY").expect("outside file");
        let mut slice = slice_with("table", "snapshot", &[], &[]);
        let columns = EXPECTED_TABLES
            .iter()
            .find(|table| table.name == "cayenne_index_run")
            .expect("index run schema")
            .columns;
        let row = columns
            .iter()
            .map(|column| match *column {
                "table_id" => SliceValue::Text("table".to_string()),
                "index_key" => SliceValue::Text("key".to_string()),
                "run_name" => SliceValue::Text(outside.to_string_lossy().into_owned()),
                _ => SliceValue::Null,
            })
            .collect();
        slice
            .tables
            .insert("cayenne_index_run".to_string(), vec![row]);
        let files: Vec<_> = persisted_run_files(&anchor, "data/", &slice)
            .into_iter()
            .map(|file| (file.source, file.archive_path))
            .collect();
        let destination = tmp.path().join("snapshot.tar");
        runtime_acceleration::snapshot::directory_archive::archive_directories_to_file_with_plan(
            &[(anchor, "data/".to_string())],
            &destination,
            &[],
            &[],
            &files,
        )
        .await
        .expect("archive table");
        let bytes = std::fs::read(destination).expect("archive bytes");
        println!(
            "archive_bytes={} outside_bytes_archived={}",
            bytes.len(),
            bytes
                .windows(b"OUTSIDE_DATA_DIRECTORY".len())
                .any(|bytes| bytes == b"OUTSIDE_DATA_DIRECTORY")
        );
        assert!(
            !bytes
                .windows(b"OUTSIDE_DATA_DIRECTORY".len())
                .any(|bytes| bytes == b"OUTSIDE_DATA_DIRECTORY"),
            "snapshot included outside bytes"
        );
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

    /// A partition child's persisted index runs live beside it, under its own
    /// root. Its `_lookup_index/` is left out of the directory walk like the
    /// parent's, and its registered runs are archived file by file from that
    /// root, not from where the parent's would be.
    #[tokio::test]
    async fn a_partition_child_archives_its_registered_runs_from_its_own_root() {
        let tmp = tempfile::tempdir().expect("tmp");
        let anchor = tmp.path();
        for dir in [
            "tid/current",
            "tid/_lookup_index/key",
            "region=eu/cid/child-current",
            "region=eu/cid/_lookup_index/key",
        ] {
            std::fs::create_dir_all(anchor.join(dir)).expect("mkdir");
        }
        std::fs::write(anchor.join("tid/_lookup_index/key/parent.run"), b"run").expect("write");
        std::fs::write(
            anchor.join("region=eu/cid/_lookup_index/key/child.run"),
            b"run",
        )
        .expect("write");
        let mut slice = slice_with("tid", "current", &[], &[]);
        slice
            .tables
            .get_mut("cayenne_table")
            .expect("parent")
            .push(slice_row(
                "cayenne_table",
                &[
                    ("table_id", "cid"),
                    ("path", "region=eu"),
                    ("current_snapshot_id", "child-current"),
                ],
            ));
        let run = |table_id: &str, run_name: &str| {
            slice_row(
                "cayenne_index_run",
                &[
                    ("table_id", table_id),
                    ("index_key", "key"),
                    ("run_name", run_name),
                ],
            )
        };
        slice.tables.insert(
            "cayenne_index_run".to_string(),
            vec![run("tid", "parent.run"), run("cid", "child.run")],
        );

        let skip = unreferenced_data_entries(anchor, &slice)
            .await
            .expect("list");
        let expected: HashSet<PathBuf> = ["tid/_lookup_index", "region=eu/cid/_lookup_index"]
            .iter()
            .map(PathBuf::from)
            .collect();
        assert_eq!(skip, expected);

        let files: Vec<_> = persisted_run_files(anchor, "data/", &slice)
            .into_iter()
            .map(|file| (file.source, file.archive_path))
            .collect();
        assert_eq!(
            files,
            vec![
                (
                    anchor.join("tid/_lookup_index/key/parent.run"),
                    "data/tid/_lookup_index/key/parent.run".to_string(),
                ),
                (
                    anchor.join("region=eu/cid/_lookup_index/key/child.run"),
                    "data/region=eu/cid/_lookup_index/key/child.run".to_string(),
                ),
            ]
        );
    }

    /// Tables sharing a root are archived whole, but not their persisted index
    /// runs, whose files the background sync may delete mid-archive; the
    /// registered ones are archived file by file instead.
    #[tokio::test]
    async fn tables_sharing_a_root_leave_their_index_runs_to_the_file_list() {
        let tmp = tempfile::tempdir().expect("tmp");
        let anchor = tmp.path();
        for dir in ["tid/current", "tid/_lookup_index/key", "cid/_lookup_index"] {
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
        let expected: HashSet<PathBuf> = ["tid/_lookup_index", "cid/_lookup_index"]
            .iter()
            .map(PathBuf::from)
            .collect();
        assert_eq!(
            unreferenced_data_entries(anchor, &slice)
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
        use runtime_acceleration::snapshot::directory_archive::archive_directories_to_file_with_plan;

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

        let (reader_data, rows) = restore(&tar, &tmp.path().join("reader")).await;
        assert!(
            !reader_data.join(&table_id).join(&first).exists(),
            "the retired snapshot must not be in the archive"
        );
        assert!(reader_data.join(&table_id).join(&second).is_dir());
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
        let optional_files: Vec<_> = plan
            .optional_files
            .into_iter()
            .map(|file| (file.source, file.archive_path))
            .collect();
        archive_directories_to_file_with_plan(&dirs, &tar, &skip, &extras, &optional_files)
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

    mod persisted_runs {
        //! Persisted secondary index runs across an acceleration snapshot,
        //! written and restored the way the runtime does it: the engine's plan
        //! applied to the layout's two directories, archived, and extracted
        //! into a reader's own directories. Optional run files travel on Unix;
        //! other platforms omit them and rebuild from the restored data files.

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
        /// by them on Unix — no build, and a first lookup answered from the index.
        /// Other platforms omit optional run files and answer correctly from the
        /// restored data. The same restore without persistence starts uncovered.
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
            if cfg!(unix) {
                assert_eq!(
                    persisted(&reader_catalog, &reader_data, expected.len()).await,
                    expected,
                    "the snapshot carries every run, registered and on disk"
                );
            } else {
                assert_eq!(
                    run_files(&reader_data),
                    0,
                    "optional run files are omitted without a race-free open"
                );
            }
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
            if cfg!(unix) {
                assert_eq!(
                    (verification.uncovered_files, counters.builds_started),
                    (0, 0),
                    "a restored table must be covered by its persisted runs, not by a build: {verification:?}"
                );
            } else {
                assert!(
                    verification.files == 0 && verification.uncovered_files > 0,
                    "a restore without optional runs must start uncovered: {verification:?}"
                );
            }
            assert_eq!(lookup(&reader, 24_007).await, 1);
            let after = reader.lookup_index_counters().expect("indexed");
            if cfg!(unix) {
                assert_eq!(
                    (after.full - counters.full, after.none - counters.none),
                    (1, 0),
                    "the first lookup after the restore did not use the loaded index: {after:?}"
                );
            }
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
        /// until every file is covered. Platforms without a race-free optional
        /// file open omit every run, so all restored files start uncovered.
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
            let retained_runs = usize::from(cfg!(unix));
            assert_eq!(
                run_files(&reader_data),
                retained_runs,
                "only safely opened run files are archived"
            );
            let reader = open(
                &env,
                Arc::clone(&reader_catalog),
                &reader_data,
                IndexPersistence::Enabled,
            )
            .await;
            assert_eq!(
                persisted(&reader_catalog, &reader_data, retained_runs)
                    .await
                    .len(),
                retained_runs,
                "opening drops registrations whose files are missing"
            );
            let restored = reader
                .verify_lookup_index_against_read_back()
                .await
                .expect("verify");
            assert!(
                restored.agrees()
                    && (if cfg!(unix) {
                        restored.files > 0
                    } else {
                        restored.files == 0
                    })
                    && restored.uncovered_files > 0,
                "retained runs cover their files and missing runs leave files uncovered: {restored:?}"
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
            let expected_runs = if cfg!(unix) { 2 } else { 1 };
            assert!(
                persisted(&reader_catalog, &reader_data, expected_runs)
                    .await
                    .len()
                    >= expected_runs,
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
                (if cfg!(unix) {
                    restored.files > 0
                } else {
                    restored.files == 0
                }) && restored.uncovered_files > 0,
                "only safely archived runs cover restored files: {restored:?}"
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

        /// Replaces the table's rows with `batch`, as a full refresh does.
        async fn overwrite(table: &Arc<CayenneTableProvider>, batch: RecordBatch) {
            use datafusion::dataframe::DataFrameWriteOptions;
            use datafusion::logical_expr::dml::InsertOp;
            let ctx = SessionContext::new();
            ctx.register_table(NAME, Arc::clone(table) as Arc<dyn TableProvider>)
                .expect("register");
            ctx.read_batch(batch)
                .expect("read batch")
                .write_table(
                    NAME,
                    DataFrameWriteOptions::new().with_insert_operation(InsertOp::Overwrite),
                )
                .await
                .expect("overwrite");
        }

        /// Archives the writer's directories the way the runtime does after a
        /// refresh: under the index persistence hold, then restores the archive
        /// into `root` and reopens it there.
        async fn snapshot_and_reopen(
            env: &Arc<RuntimeEnv>,
            table: &Arc<CayenneTableProvider>,
            catalog: &Arc<CayenneCatalog>,
            (meta, data): (&Path, &Path),
            root: &Path,
        ) -> Arc<CayenneTableProvider> {
            let archive = root.with_extension("tar");
            {
                let _hold = table.persist_lookup_index_for_snapshot().await;
                snapshot(catalog, meta, data, &archive).await;
            }
            let (reader_catalog, reader_data) = restore(&archive, root).await;
            open(env, reader_catalog, &reader_data, IndexPersistence::Enabled).await
        }

        /// A snapshot taken the moment a full refresh returns carries the
        /// refresh's index runs, so a table restored from it covers every file
        /// without a build. The runs of a write persist in the background; the
        /// runtime snapshots straight after a refresh, and without the hold the
        /// archive held only the runs of the replaced files in 7 of 10 rounds
        /// (`covered_files=0 uncovered_files=8`). Regression test for #14803.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn a_snapshot_right_after_a_full_refresh_carries_its_runs() {
            if !cfg!(unix) {
                return;
            }
            let env = Arc::new(RuntimeEnv::default());
            let tmp = tempfile::tempdir().expect("tmp");
            let (catalog, meta, data) = writer(&env, &tmp.path().join("w")).await;
            let table = open(&env, Arc::clone(&catalog), &data, IndexPersistence::Enabled).await;
            for round in 0..5_i64 {
                let first = round * 1_000_000;
                overwrite(&table, rows(first, 400_000)).await;
                let reader = snapshot_and_reopen(
                    &env,
                    &table,
                    &catalog,
                    (&meta, &data),
                    &tmp.path().join(format!("r{round}")),
                )
                .await;
                let restored = reader
                    .verify_lookup_index_against_read_back()
                    .await
                    .expect("verify");
                let counters = reader.lookup_index_counters().expect("indexed");
                assert!(
                    restored.agrees()
                        && restored.files > 1
                        && restored.uncovered_files == 0
                        && counters.builds_started == 0,
                    "round {round}: the restore must be covered by the refresh's runs, not a build: {restored:?} {counters:?}"
                );
                assert_eq!(lookup(&reader, first + 123_457).await, 1);
                let after = reader.lookup_index_counters().expect("indexed");
                assert_eq!(
                    (after.full - counters.full, after.none - counters.none),
                    (1, 0),
                    "round {round}: the first lookup is answered from the restored index: {after:?}"
                );
            }
        }

        /// While the snapshot hold is held, a write's runs are not persisted, so
        /// no run file the archive lists is replaced or deleted under it; once
        /// the hold drops, they are.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn the_snapshot_hold_keeps_the_persisted_runs_until_it_drops() {
            let env = Arc::new(RuntimeEnv::default());
            let tmp = tempfile::tempdir().expect("tmp");
            let (catalog, _meta, data) = writer(&env, &tmp.path().join("w")).await;
            let table = open(&env, Arc::clone(&catalog), &data, IndexPersistence::Enabled).await;
            let before = persisted(&catalog, &data, 2).await;
            let hold = table.persist_lookup_index_for_snapshot().await;
            overwrite(&table, rows(500_000, 5_000)).await;
            // Asserting that a sync does NOT run takes a window of time: the
            // write's sync is spawned when the write returns, so it has had
            // this long to replace the runs were the hold not blocking it.
            tokio::time::sleep(Duration::from_millis(500)).await;
            let table_id = catalog.get_table(NAME).await.expect("table").table_id;
            let mut registered: Vec<(String, String)> = catalog
                .list_index_runs(&table_id)
                .await
                .expect("list runs")
                .into_iter()
                .map(|run| (run.index_key, run.run_name))
                .collect();
            registered.sort();
            assert_eq!(
                registered, before,
                "no run is registered or removed while held"
            );
            assert_eq!(
                run_files(&data),
                before.len(),
                "no run file changes while held"
            );
            drop(hold);
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                let now = persisted(&catalog, &data, 1).await;
                if now != before {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "the overwrite's runs were never persisted after the hold dropped"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }

        /// A table restored without its index runs (here, the archive lost
        /// them) indexes every uncovered file in `catch_up_lookup_index`, and
        /// is fully covered when it returns: no lookup reads a file in full.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn catching_up_indexes_a_restore_that_lost_its_runs() {
            let env = Arc::new(RuntimeEnv::default());
            let tmp = tempfile::tempdir().expect("tmp");
            let (catalog, meta, data) = writer(&env, &tmp.path().join("w")).await;
            let archive = tmp.path().join("snapshot.tar");
            snapshot(&catalog, &meta, &data, &archive).await;
            let (reader_catalog, reader_data) = restore(&archive, &tmp.path().join("r")).await;
            for run in run_file_paths(&reader_data) {
                std::fs::remove_file(run).expect("drop a restored run file");
            }
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
                restored.uncovered_files > 0,
                "the restore starts uncovered: {restored:?}"
            );

            reader.catch_up_lookup_index().await;
            let caught_up = reader
                .verify_lookup_index_against_read_back()
                .await
                .expect("verify");
            let counters = reader.lookup_index_counters().expect("indexed");
            assert!(
                caught_up.agrees()
                    && caught_up.files > 0
                    && caught_up.uncovered_files == 0
                    && counters.builds_started == 1,
                "catching up returns once every file is indexed: {caught_up:?} {counters:?}"
            );
            assert_eq!(lookup(&reader, 24_007).await, 1);
            let after = reader.lookup_index_counters().expect("indexed");
            assert_eq!(
                (after.full - counters.full, after.none - counters.none),
                (1, 0),
                "the first lookup is answered from the index: {after:?}"
            );
        }

        /// Catching up a table its runs already cover starts no build.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn catching_up_a_covered_table_builds_nothing() {
            if !cfg!(unix) {
                return;
            }
            let env = Arc::new(RuntimeEnv::default());
            let tmp = tempfile::tempdir().expect("tmp");
            let (catalog, meta, data) = writer(&env, &tmp.path().join("w")).await;
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
            reader.catch_up_lookup_index().await;
            let verification = reader
                .verify_lookup_index_against_read_back()
                .await
                .expect("verify");
            let counters = reader.lookup_index_counters().expect("indexed");
            assert_eq!(
                (verification.uncovered_files, counters.builds_started),
                (0, 0),
                "{verification:?} {counters:?}"
            );
        }
    }

    /// A key-upsert table holding one row, plus `orphans` orphan-eligible key
    /// deletion vectors (sequence 0) in its current snapshot's `deletions/`.
    async fn table_with_orphan_dvs(
        metadata_dir: &std::path::Path,
        data_dir: &std::path::Path,
        orphans: usize,
    ) -> (
        Arc<CayenneCatalog>,
        Arc<cayenne::CayenneTableProvider>,
        Vec<PathBuf>,
    ) {
        use arrow::array::{BinaryArray, Int64Array, RecordBatch};
        use cayenne::CayenneTableProviderBuilder;
        use cayenne::metadata::{DeleteFile, DeletionMode, DeletionType};
        use datafusion::datasource::TableProvider;
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::logical_expr::dml::InsertOp;
        use datafusion::physical_plan::collect;
        use datafusion::prelude::SessionContext;
        use datafusion_table_providers::util::{
            column_reference::ColumnReference, on_conflict::OnConflict,
        };

        let catalog = fresh_catalog(metadata_dir).await;
        let ctx = SessionContext::new();
        let table = Arc::new(
            CayenneTableProviderBuilder::new(
                Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
                ctx.runtime_env(),
            )
            .create(CreateTableOptions {
                table_name: "trips".to_string(),
                schema: schema(),
                primary_key: vec!["id".to_string()],
                on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                    "id".to_string(),
                ]))),
                base_path: data_dir.to_string_lossy().into_owned(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig {
                    inline_max_rows: 0,
                    deletion_mode: DeletionMode::Key,
                    compaction_trigger_files: 1_000_000,
                    compaction_trigger_protected_snapshots: 1_000_000,
                    compaction_trigger_snapshot_age_ms: 0,
                    compaction_background_interval_ms: 0,
                    ..cayenne::metadata::VortexConfig::default()
                },
            })
            .await
            .expect("create table"),
        );
        let batch = RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vec![1_i64]))])
            .expect("batch");
        let input = MemorySourceConfig::try_new_exec(&[vec![batch]], schema(), None).expect("exec");
        let plan = table
            .insert_into(&ctx.state(), input, InsertOp::Append)
            .await
            .expect("plan");
        collect(plan, ctx.task_ctx()).await.expect("write");
        table.drain_in_flight_maintenance().await.expect("drain");

        let deletions = data_dir.join(table.current_snapshot_id()).join("deletions");
        std::fs::create_dir_all(&deletions).expect("mkdir deletions");
        let dv_schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "row_key",
            arrow_schema::DataType::Binary,
            false,
        )]));
        let mut paths = Vec::with_capacity(orphans);
        for _ in 0..orphans {
            let id = uuid::Uuid::now_v7().to_string();
            let path = deletions.join(format!("delete_{id}.arrow"));
            let empty = RecordBatch::try_new(
                Arc::clone(&dv_schema),
                vec![Arc::new(BinaryArray::from(Vec::<&[u8]>::new()))],
            )
            .expect("dv batch");
            let mut writer = arrow::ipc::writer::FileWriter::try_new(
                std::fs::File::create(&path).expect("create dv"),
                &dv_schema,
            )
            .expect("dv writer");
            writer.write(&empty).expect("write dv");
            writer.finish().expect("finish dv");
            catalog
                .add_delete_file(DeleteFile {
                    delete_file_id: id,
                    table_id: table.metadata().table_id.clone(),
                    source_data_file_path: None,
                    path: path.to_string_lossy().into_owned(),
                    path_is_relative: false,
                    format: "arrow_ipc".to_string(),
                    delete_count: 0,
                    file_size_bytes: 0,
                    deletion_type: DeletionType::KeyBased,
                    sequence_number: 0,
                    reinsert_sequence: None,
                })
                .await
                .expect("add delete file");
            paths.push(path);
        }
        (catalog, table, paths)
    }

    /// Extracts `tar` under `root` into a fresh metastore and opens the table;
    /// returns the reader's data directory and the restored row count.
    async fn restore(tar: &std::path::Path, root: &std::path::Path) -> (PathBuf, usize) {
        use arrow::array::RecordBatch;
        use cayenne::CayenneTableProviderBuilder;
        use datafusion::datasource::TableProvider;
        use datafusion::prelude::SessionContext;
        use runtime_acceleration::snapshot::directory_archive::{
            ExtractOptions, extract_archive_file_with_options,
        };

        let metadata_dir = root.join("metadata");
        let data_dir = root.join("trips");
        std::fs::create_dir_all(&metadata_dir).expect("mkdir metadata");
        std::fs::create_dir_all(&data_dir).expect("mkdir data");
        let catalog = fresh_catalog(&metadata_dir).await;
        extract_archive_file_with_options(
            tar,
            root,
            ExtractOptions {
                prefix_mappings: Some(vec![
                    ("metadata/".to_string(), metadata_dir.clone()),
                    ("data/".to_string(), data_dir.clone()),
                ]),
                ..ExtractOptions::skip_existing()
            },
        )
        .await
        .expect("extract");
        CayenneSnapshotEngine::new(
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
            "trips",
            data_dir.clone(),
        )
        .finalize_directory_snapshot(
            &[
                (metadata_dir, "metadata/".to_string()),
                (data_dir.clone(), "data/".to_string()),
            ],
            "trips",
        )
        .await
        .expect("import");
        let ctx = SessionContext::new();
        let restored = CayenneTableProviderBuilder::new(
            catalog as Arc<dyn MetadataCatalog>,
            ctx.runtime_env(),
        )
        .open("trips")
        .await
        .expect("open");
        let rows = ctx
            .read_table(Arc::new(restored) as Arc<dyn TableProvider>)
            .expect("read")
            .collect()
            .await
            .expect("collect")
            .iter()
            .map(RecordBatch::num_rows)
            .sum();
        (data_dir, rows)
    }

    /// Regression test for #14787: the orphaned-DV sweep unlinked deletion
    /// vectors while a snapshot archived them. Under a file-deletion hold the sweep
    /// waits, the archive keeps every file the slice lists, and it restores.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn orphan_dv_sweep_waits_for_the_file_deletion_hold() {
        use runtime_acceleration::snapshot::directory_archive::archive_directories_to_file_with_plan;

        let tmp = tempfile::tempdir().expect("tmp");
        let metadata_dir = tmp.path().join("writer").join("metadata");
        let data_dir = tmp.path().join("writer").join("trips");
        std::fs::create_dir_all(&metadata_dir).expect("mkdir metadata");
        std::fs::create_dir_all(&data_dir).expect("mkdir data");
        let (catalog, table, orphans) = table_with_orphan_dvs(&metadata_dir, &data_dir, 3).await;

        let hold = table.hold_file_deletions().await;
        let mut sweep = tokio::spawn({
            let table = Arc::clone(&table);
            async move { table.drain_orphan_dv_sweep(1).await }
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(500), &mut sweep)
                .await
                .is_err(),
            "the sweep must wait for the file-deletion hold"
        );

        let dirs = vec![
            (metadata_dir, "metadata/".to_string()),
            (data_dir.clone(), "data/".to_string()),
        ];
        let plan =
            CayenneSnapshotEngine::new(catalog as Arc<dyn MetadataCatalog>, "trips", data_dir)
                .prepare_directory_snapshot(&dirs, "trips")
                .await
                .expect("prepare");
        let skip: Vec<PathBuf> = plan.skip_relative_paths.into_iter().collect();
        let extras: Vec<(String, Vec<u8>)> = plan
            .extra_entries
            .into_iter()
            .map(|e| (e.archive_path, e.bytes))
            .collect();
        let tar = tmp.path().join("snapshot.tar");
        let optional_files: Vec<_> = plan
            .optional_files
            .into_iter()
            .map(|file| (file.source, file.archive_path))
            .collect();
        archive_directories_to_file_with_plan(&dirs, &tar, &skip, &extras, &optional_files)
            .await
            .expect("archive");
        assert!(orphans.iter().all(|path| path.exists()));

        drop(hold);
        tokio::time::timeout(std::time::Duration::from_secs(30), sweep)
            .await
            .expect("the sweep resumes once the hold drops")
            .expect("sweep task");
        assert!(orphans.iter().all(|path| !path.exists()));

        let (_, rows) = restore(&tar, &tmp.path().join("reader")).await;
        assert_eq!(rows, 1, "the archive restores the table");
    }

    /// The runtime's snapshot attempt holds file deletions on the Cayenne table it
    /// snapshots.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn snapshot_attempt_holds_cayenne_file_deletions() {
        assert_snapshot_attempt_holds_file_deletions(&HashMap::new()).await;
    }

    /// The same through the upsert-dedup wrapper the engine adds for
    /// `upsert_remove_duplicates`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn snapshot_attempt_holds_file_deletions_through_upsert_dedup() {
        let options = HashMap::from([("upsert_remove_duplicates".to_string(), "true".to_string())]);
        assert_snapshot_attempt_holds_file_deletions(&options).await;
    }

    /// Runs the real snapshot attempt over the table wrapped as the engine wraps
    /// it, with an orphaned-DV sweep started inside the attempt.
    async fn assert_snapshot_attempt_holds_file_deletions(options: &HashMap<String, String>) {
        use async_trait::async_trait;
        use datafusion::common::TableReference;
        use datafusion::datasource::TableProvider;
        use runtime_acceleration::dataset_checkpoint::{DatasetCheckpointer, Result};
        use runtime_acceleration::snapshot::ForceCreate;

        /// Starts the orphaned-DV sweep from inside the attempt and records
        /// whether it finished while the attempt ran.
        struct ProbeCheckpointer {
            table: Arc<cayenne::CayenneTableProvider>,
            probe: std::sync::Mutex<Option<(bool, tokio::task::JoinHandle<()>)>>,
        }

        #[async_trait]
        impl DatasetCheckpointer for ProbeCheckpointer {
            async fn exists(&self) -> bool {
                true
            }
            async fn checkpoint(&self, _: &arrow_schema::SchemaRef, _: Option<&str>) -> Result<()> {
                let table = Arc::clone(&self.table);
                let mut sweep = tokio::spawn(async move { table.drain_orphan_dv_sweep(1).await });
                let finished =
                    tokio::time::timeout(std::time::Duration::from_millis(500), &mut sweep)
                        .await
                        .is_ok();
                *self.probe.lock().expect("probe") = Some((finished, sweep));
                Ok(())
            }
            async fn get_schema(&self) -> Result<Option<arrow_schema::SchemaRef>> {
                Ok(None)
            }
            async fn last_checkpoint_time(&self) -> Result<Option<std::time::SystemTime>> {
                Ok(None)
            }
            async fn get_refresh_sql(&self) -> Result<Option<String>> {
                Ok(None)
            }
            async fn set_schema(&self, _: &arrow_schema::SchemaRef) -> Result<()> {
                Ok(())
            }
            async fn delete(&self) -> Result<()> {
                Ok(())
            }
        }

        let tmp = tempfile::tempdir().expect("tmp");
        let metadata_dir = tmp.path().join("metadata");
        let data_dir = tmp.path().join("trips");
        std::fs::create_dir_all(&metadata_dir).expect("mkdir metadata");
        std::fs::create_dir_all(&data_dir).expect("mkdir data");
        let (_catalog, table, orphans) = table_with_orphan_dvs(&metadata_dir, &data_dir, 3).await;

        let probe = Arc::new(ProbeCheckpointer {
            table: Arc::clone(&table),
            probe: std::sync::Mutex::new(None),
        });
        let checkpointer: Arc<dyn DatasetCheckpointer> = Arc::clone(&probe) as _;
        let write = data_accelerator_api::upsert_dedup::wrap_with_upsert_dedup_if_needed(
            table,
            options,
            datafusion::common::Constraints::default(),
        );
        let accelerator: Arc<dyn TableProvider> = Arc::new(
            data_components::poly::PolyTableProvider::new_with_schema_metadata(
                Arc::clone(&write),
                write,
                HashMap::new(),
            ),
        )
        .into_table();
        runtime_table::accelerated::snapshots::create_checkpoint_and_snapshot(
            &checkpointer,
            None,
            &schema(),
            &Arc::new(tokio::sync::Mutex::new(())),
            &TableReference::bare("trips"),
            &Arc::new(std::sync::atomic::AtomicI64::new(0)),
            ForceCreate(false),
            Some(&accelerator),
            None,
            None,
        )
        .await;

        let (finished, sweep) = probe
            .probe
            .lock()
            .expect("probe")
            .take()
            .expect("the attempt ran");
        assert!(
            !finished,
            "the sweep must wait while the snapshot attempt runs"
        );
        tokio::time::timeout(std::time::Duration::from_secs(30), sweep)
            .await
            .expect("the sweep resumes after the attempt")
            .expect("sweep task");
        assert!(orphans.iter().all(|path| !path.exists()));
    }
}
