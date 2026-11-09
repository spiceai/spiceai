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
}

impl UncommittedFiles {
    /// Records `name` (`size` bytes) as written into `snapshot_id`.
    pub(crate) fn note(&self, snapshot_id: &str, name: &str, size: u64) {
        self.by_snapshot
            .lock()
            .entry(snapshot_id.to_string())
            .or_default()
            .insert(name.to_string(), size);
    }

    /// The files written into `snapshot_id`, as `(name, size)` sorted by name.
    pub(crate) fn files(&self, snapshot_id: &str) -> Vec<(String, u64)> {
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

    /// Whether this process is still building `snapshot_id`.
    pub(crate) fn holds_snapshot(&self, snapshot_id: &str) -> bool {
        self.by_snapshot.lock().contains_key(snapshot_id)
    }

    /// Stops tracking `snapshot_id`: its files are now named by a commit, or the
    /// writer discarded the snapshot.
    pub(crate) fn forget(&self, snapshot_id: &str) {
        self.by_snapshot.lock().remove(snapshot_id);
    }

    /// Every `(snapshot id, file name)` still held, for the orphan sweep.
    pub(crate) fn held(&self) -> HashSet<(String, String)> {
        self.by_snapshot
            .lock()
            .iter()
            .flat_map(|(snapshot, files)| {
                files
                    .keys()
                    .map(move |name| (snapshot.clone(), name.clone()))
            })
            .collect()
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
}
