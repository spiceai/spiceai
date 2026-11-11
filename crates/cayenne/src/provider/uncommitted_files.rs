/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! The files a writer of a table in the object-native location has put in a
//! snapshot directory and no commit names yet.
//!
//! Such a table's live file set is its committed manifest, never a listing of
//! its directories. A writer that builds a snapshot (a compaction output, an
//! overwrite, a protected snapshot, a staged append) therefore names that
//! snapshot's files from what it wrote itself: every file the Vortex sink closes
//! ([`UncommittedFilesObserver`]) and every committed file it copies in is
//! recorded here, under the snapshot it went to, until the writer records them
//! in the manifest or discards the snapshot.
//!
//! An append on an object store writes each file once, at its final key in
//! the snapshot directory it commits to, which other writers may be adding to
//! as well ([`UncommittedFiles::begin_final_key_write`]). Its files are held
//! under the write's own key, not under that snapshot, so another writer that
//! records the snapshot's files from this record never commits them: only the
//! append's own commit names them.
//!
//! Deletion vectors a write puts in the dataset's store are held by name
//! ([`UncommittedFiles::hold_deletion_files`]) from before their upload until
//! their writer cleans them up after a failure, a commit that drops them
//! releases them, or the orphan sweep finds a commit naming them.
//!
//! The same record is the writer's intent for the orphan sweep: a file listed
//! under the table's prefix that no retained snapshot names is reclaimed only
//! once it is older than the age floor *and* no write of this process still
//! holds it here, so a write whose upload or commit is still in flight is never
//! reclaimed under it. Another process that took the dataset over cannot commit
//! these files: its takeover raised the dataset log's lease, which fences every
//! commit of this one.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use arrow::array::RecordBatch;
use parking_lot::Mutex;
use vortex_datafusion::VortexWriteObserver;

/// Files written into snapshot directories and not yet named by a commit, by
/// snapshot id, then file name, with their size in bytes.
#[derive(Debug, Default)]
pub(crate) struct UncommittedFiles {
    by_snapshot: Mutex<HashMap<String, BTreeMap<String, u64>>>,
    /// Appends writing at their final keys, by the write's own key.
    final_key_writes: Mutex<HashMap<String, FinalKeyWrite>>,
    /// Names of deletion-vector files written and not known to be committed.
    deletion_files: Mutex<HashSet<String>>,
}

/// One append writing its files at their final keys in `directory`.
#[derive(Debug)]
struct FinalKeyWrite {
    /// The snapshot directory the files are written into and committed to.
    directory: String,
    files: BTreeMap<String, u64>,
    /// The commit naming the files was accepted: discarding the write must
    /// not delete them.
    committed: bool,
}

/// What discarding a write at final keys leaves to delete.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum DiscardedFinalKeyWrite {
    /// No such write is in flight.
    Unknown,
    /// Its commit was accepted: its files are the table's.
    Committed,
    /// It never committed: these files of `directory` are its own.
    Uncommitted {
        directory: String,
        files: Vec<String>,
    },
}

impl UncommittedFiles {
    /// Records `name` (`size` bytes) as written into `snapshot_id`, or into the
    /// write at final keys whose key `snapshot_id` is.
    pub(crate) fn note(&self, snapshot_id: &str, name: &str, size: u64) {
        if let Some(write) = self.final_key_writes.lock().get_mut(snapshot_id) {
            write.files.insert(name.to_string(), size);
            return;
        }
        self.by_snapshot
            .lock()
            .entry(snapshot_id.to_string())
            .or_default()
            .insert(name.to_string(), size);
    }

    /// The files written into `snapshot_id`, or by the write at final keys whose
    /// key it is, as `(name, size)` sorted by name. A snapshot's files never
    /// include those of a write at final keys into its directory.
    pub(crate) fn files(&self, snapshot_id: &str) -> Vec<(String, u64)> {
        if let Some(write) = self.final_key_writes.lock().get(snapshot_id) {
            return write
                .files
                .iter()
                .map(|(name, size)| (name.clone(), *size))
                .collect();
        }
        self.by_snapshot
            .lock()
            .get(snapshot_id)
            .map(|files| {
                files
                    .iter()
                    .map(|(name, size)| (name.clone(), *size))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Whether this process is still building `snapshot_id`, or writing files
    /// into its directory at their final keys.
    pub(crate) fn holds_snapshot(&self, snapshot_id: &str) -> bool {
        if self.by_snapshot.lock().contains_key(snapshot_id) {
            return true;
        }
        self.final_key_writes
            .lock()
            .iter()
            .any(|(key, write)| key == snapshot_id || write.directory == snapshot_id)
    }

    /// Starts the write `write_key`, which puts its files at their final keys in
    /// `directory`'s directory: from now on the files it closes are held under
    /// `write_key` (see [`Self::note`]) until it commits or is discarded.
    pub(crate) fn begin_final_key_write(&self, write_key: &str, directory: &str) {
        self.final_key_writes.lock().insert(
            write_key.to_string(),
            FinalKeyWrite {
                directory: directory.to_string(),
                files: BTreeMap::new(),
                committed: false,
            },
        );
    }

    /// The snapshot directory the write `write_key` puts its files in, when it
    /// writes at final keys.
    pub(crate) fn final_key_directory(&self, write_key: &str) -> Option<String> {
        self.final_key_writes
            .lock()
            .get(write_key)
            .map(|write| write.directory.clone())
    }

    /// The commit naming the files of the write `write_key` was accepted.
    pub(crate) fn mark_final_key_write_committed(&self, write_key: &str) {
        if let Some(write) = self.final_key_writes.lock().get_mut(write_key) {
            write.committed = true;
        }
    }

    /// Ends the write `write_key`, returning what is left to delete: its files
    /// when it never committed, nothing when its commit was accepted. A write
    /// whose commit failed must end with [`Self::end_final_key_write`] instead,
    /// since its commit may have landed.
    pub(crate) fn discard_final_key_write(&self, write_key: &str) -> DiscardedFinalKeyWrite {
        match self.final_key_writes.lock().remove(write_key) {
            None => DiscardedFinalKeyWrite::Unknown,
            Some(write) if write.committed => DiscardedFinalKeyWrite::Committed,
            Some(write) => DiscardedFinalKeyWrite::Uncommitted {
                directory: write.directory,
                files: write.files.into_keys().collect(),
            },
        }
    }

    /// Stops holding the files of the write `write_key` without deleting any:
    /// they are committed, or their commit's outcome is decided by the table's
    /// state from here on (the orphan sweep reclaims them if it never named
    /// them).
    pub(crate) fn end_final_key_write(&self, write_key: &str) {
        self.final_key_writes.lock().remove(write_key);
    }

    /// Holds the deletion-vector files `names` a write is about to upload.
    pub(crate) fn hold_deletion_files<'a>(&self, names: impl IntoIterator<Item = &'a str>) {
        self.deletion_files
            .lock()
            .extend(names.into_iter().map(str::to_string));
    }

    /// Stops holding the deletion-vector files `names`: their writer deleted
    /// them, or a commit dropped them after naming them.
    pub(crate) fn release_deletion_files<'a>(&self, names: impl IntoIterator<Item = &'a str>) {
        let mut held = self.deletion_files.lock();
        for name in names {
            held.remove(name);
        }
    }

    /// The deletion-vector files still held, by name.
    pub(crate) fn held_deletion_files(&self) -> HashSet<String> {
        self.deletion_files.lock().clone()
    }

    /// Stops holding the files `names` of `snapshot_id`, which its writer
    /// deleted before any commit named them (a rewrite replaced them). The
    /// snapshot stays held: its writer is still building it.
    pub(crate) fn drop_files<'a>(
        &self,
        snapshot_id: &str,
        names: impl IntoIterator<Item = &'a str>,
    ) {
        if let Some(files) = self.by_snapshot.lock().get_mut(snapshot_id) {
            for name in names {
                files.remove(name);
            }
        }
    }

    /// Stops tracking `snapshot_id`: its files are now named by a commit, or the
    /// writer discarded the snapshot. A write at final keys into its directory
    /// is not touched: only that write's own commit or discard ends it.
    pub(crate) fn forget(&self, snapshot_id: &str) {
        self.by_snapshot.lock().remove(snapshot_id);
    }

    /// Every `(snapshot directory, file name)` still held, for the orphan sweep.
    pub(crate) fn held(&self) -> HashSet<(String, String)> {
        let mut held: HashSet<(String, String)> = self
            .by_snapshot
            .lock()
            .iter()
            .flat_map(|(snapshot, files)| {
                files
                    .keys()
                    .map(move |name| (snapshot.clone(), name.clone()))
            })
            .collect();
        held.extend(self.final_key_writes.lock().values().flat_map(|write| {
            write
                .files
                .keys()
                .map(|name| (write.directory.clone(), name.clone()))
        }));
        held
    }

    /// The name of every file still held, data files and deletion vectors: in
    /// a flat table a data file's name is its key relative to the table, so a
    /// retired key held here is still some write's upload.
    pub(crate) fn held_keys(&self) -> HashSet<String> {
        let mut held: HashSet<String> = self.held().into_iter().map(|(_, name)| name).collect();
        held.extend(self.held_deletion_files());
        held
    }

    /// The snapshot ids still being built.
    pub(crate) fn snapshots(&self) -> HashSet<String> {
        self.by_snapshot.lock().keys().cloned().collect()
    }
}

/// Records every file one write closes into [`UncommittedFiles`] under the
/// snapshot it writes.
#[derive(Debug)]
pub(crate) struct UncommittedFilesObserver {
    pub(crate) files: Arc<UncommittedFiles>,
    pub(crate) snapshot_id: String,
}

impl VortexWriteObserver for UncommittedFilesObserver {
    fn batch_written(
        &self,
        _file_path: &object_store::path::Path,
        _first_row_position: u64,
        _batch: &RecordBatch,
    ) {
    }

    fn file_written(&self, file_path: &object_store::path::Path, size: u64) {
        if let Some(name) = file_path.filename() {
            self.files.note(&self.snapshot_id, name, size);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_closed_file_is_held_under_its_snapshot_until_forgotten() {
        let files = Arc::new(UncommittedFiles::default());
        let observer = UncommittedFilesObserver {
            files: Arc::clone(&files),
            snapshot_id: "s1".to_string(),
        };
        observer.file_written(
            &object_store::path::Path::from("data/t/s1/w_00001.vortex"),
            20,
        );
        observer.file_written(
            &object_store::path::Path::from("data/t/s1/w_00000.vortex"),
            10,
        );
        files.note("s2", "c.vortex", 5);
        assert_eq!(
            files.files("s1"),
            vec![
                ("w_00000.vortex".to_string(), 10),
                ("w_00001.vortex".to_string(), 20)
            ]
        );
        assert!(
            files
                .held()
                .contains(&("s2".to_string(), "c.vortex".to_string()))
        );
        files.forget("s1");
        assert!(files.files("s1").is_empty());
        assert!(!files.holds_snapshot("s1"));
        assert_eq!(files.snapshots(), HashSet::from(["s2".to_string()]));
    }

    /// A write at final keys into a snapshot another writer also records is
    /// never part of that snapshot's files, so the other writer cannot commit
    /// them, while the orphan sweep sees them held in the snapshot's directory.
    #[test]
    fn a_final_key_write_is_held_apart_from_the_snapshot_it_writes_into() {
        let files = Arc::new(UncommittedFiles::default());
        files.note("cur", "flush.vortex", 1);
        files.begin_final_key_write("_staging/w1", "cur");
        let observer = UncommittedFilesObserver {
            files: Arc::clone(&files),
            snapshot_id: "_staging/w1".to_string(),
        };
        observer.file_written(&object_store::path::Path::from("t/cur/w1_00000.vortex"), 7);
        assert_eq!(files.files("cur"), vec![("flush.vortex".to_string(), 1)]);
        assert_eq!(
            files.files("_staging/w1"),
            vec![("w1_00000.vortex".to_string(), 7)]
        );
        assert_eq!(
            files.final_key_directory("_staging/w1").as_deref(),
            Some("cur")
        );
        assert!(
            files
                .held()
                .contains(&("cur".to_string(), "w1_00000.vortex".to_string()))
        );
        // The other writer records the snapshot and forgets it: the write goes on.
        files.forget("cur");
        assert!(files.holds_snapshot("cur"));
        assert_eq!(
            files.discard_final_key_write("_staging/w1"),
            DiscardedFinalKeyWrite::Uncommitted {
                directory: "cur".to_string(),
                files: vec!["w1_00000.vortex".to_string()],
            }
        );
        assert!(files.held().is_empty());

        files.begin_final_key_write("_staging/w2", "cur");
        files.note("_staging/w2", "w2_00000.vortex", 3);
        files.mark_final_key_write_committed("_staging/w2");
        assert_eq!(
            files.discard_final_key_write("_staging/w2"),
            DiscardedFinalKeyWrite::Committed
        );
        assert_eq!(
            files.discard_final_key_write("_staging/w2"),
            DiscardedFinalKeyWrite::Unknown
        );
    }

    #[test]
    fn deletion_files_are_held_until_released() {
        let files = UncommittedFiles::default();
        files.hold_deletion_files(["delete_a.arrow", "delete_b.arrow"]);
        files.release_deletion_files(["delete_a.arrow"]);
        assert_eq!(
            files.held_deletion_files(),
            HashSet::from(["delete_b.arrow".to_string()])
        );
    }
}
