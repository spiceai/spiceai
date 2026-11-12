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

//! The orphan sweep of a table in the object-native location: the one place a
//! listing of the table's data is used, and only to find garbage.
//!
//! The table's data is flat: its data files sit directly under the table's
//! directory (`<write_id>_<n>.vortex`), its deletion vectors under
//! `deletions/`, a staged write in `_staging/<id>/`, and the lookup index in
//! `_lookup_index/`, which this sweep never touches.
//!
//! A listed object is deleted only when all of these hold:
//!
//! - no committed entry of the table names it, live or retired: a flat data
//!   file by its key (the manifest rows of every snapshot, and the retirement
//!   record, whose files belong to the retirement sweep until it forgets them),
//!   a deletion vector by its name;
//! - no write of this process still holds it ([`super::uncommitted_files`]),
//!   whatever its age, so an upload or a commit still in flight keeps its files
//!   (a deletion vector is held from before its upload until a commit names it,
//!   its writer deletes it, or a commit drops it);
//! - it is older than the age floor, measured on the store's clock: its
//!   `last_modified` against the time the store stamped on the dataset's head
//!   when it was last written, never the writer's clock or the time in its
//!   name. An object stamped after the head is young, so an object no commit
//!   since names waits for the next commit or the next open.
//!
//! The sweep does not run while the table's state cannot be read (a commit whose
//! outcome is unknown stops the dataset's log until it reads its state back),
//! while its retirement record is not settled (a change applied but not
//! committed, or a transaction reserving the table),
//! while an earlier write of the table could not learn whether it committed, or
//! while a transaction across datasets is prepared on the table: a prepared
//! transaction names files its commit would add. A file is never adopted.
//!
//! Multipart uploads started under the table's prefix and never completed are
//! aborted on the same floor, measured from the time the store says each was
//! initiated, when the store can list them ([`UnfinishedUploads`]).
//!
//! The sweep runs when the table opens, so the objects a write that crashed or
//! was fenced left behind are reclaimed even if the table is never written
//! again, and then at most every [`ORPHAN_SWEEP_INTERVAL`], after a commit or on
//! the background maintenance tick. It deletes in batched requests
//! ([`crate::dataset_storage::delete_in_batches`]) and counts what it reclaimed
//! under `op="orphan_file_sweep"`.

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use object_store::path::Path;

/// How old an unnamed object must be, on the store's clock, before the orphan
/// sweep deletes it. Far above the longest upload and commit resolution a
/// writer takes; a write still in progress in this process is protected
/// whatever its age.
pub(crate) const ORPHAN_AGE_FLOOR: Duration = Duration::from_hours(1);

/// The least time between two orphan sweeps of one table.
pub(crate) const ORPHAN_SWEEP_INTERVAL: Duration = Duration::from_mins(15);

/// A multipart upload a store has started and neither completed nor aborted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnfinishedUpload {
    /// The object the upload completes into.
    pub location: Path,
    /// The store's id of the upload.
    pub upload_id: String,
    /// When the store initiated the upload, on the store's clock.
    pub initiated: DateTime<Utc>,
}

/// A store's unfinished multipart uploads: listed under a prefix and aborted.
/// `object_store` neither lists nor exposes them, and a store without lifecycle
/// rules (S3 Express One Zone directory buckets) keeps them, and bills for
/// their parts, until they are aborted.
#[async_trait]
pub trait UnfinishedUploads: Send + Sync + std::fmt::Debug {
    /// Every upload under `prefix` not completed or aborted yet.
    async fn list(&self, prefix: &Path) -> object_store::Result<Vec<UnfinishedUpload>>;

    /// Abort `upload`, deleting its parts.
    async fn abort(&self, upload: &UnfinishedUpload) -> object_store::Result<()>;
}

/// What one orphan sweep did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OrphanSweep {
    /// Why the sweep did not run, when it did not.
    pub skipped: Option<&'static str>,
    /// Objects deleted, as keys under the table's prefix.
    pub deleted: Vec<String>,
    /// Unnamed objects kept because they are younger than the floor.
    pub kept_young: usize,
    /// Unnamed objects kept because a write in this process holds them.
    pub kept_in_flight: usize,
    /// Unfinished multipart uploads aborted.
    pub aborted_uploads: Vec<String>,
    /// Objects the sweep tried to delete that may still be stored; the next
    /// sweep retries them.
    pub failed: usize,
}

/// Where a listed object sits, relative to the table's prefix.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Placement<'a> {
    /// `<file>` directly under the table's directory: a data file of a flat
    /// table, keyed by its name.
    Flat { file: &'a str },
    /// `deletions/<file>.arrow`: a deletion vector of a flat table.
    Deletion { file: &'a str },
    /// `_staging/<id>/<file>`: `snapshot` is `_staging/<id>`.
    Staging { snapshot: String, file: &'a str },
    /// Anything else: the lookup index's, another sweep's, or not a data file.
    Other,
}

/// Classifies a key under the table's prefix.
pub(crate) fn placement(relative: &str) -> Placement<'_> {
    let mut parts = relative.split('/');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(dir), ..) if dir == super::constants::LOOKUP_INDEX_DIR_NAME => Placement::Other,
        (Some(file), None, None, None)
            if !file.is_empty() && !file.starts_with('_') && !file.starts_with('.') =>
        {
            Placement::Flat { file }
        }
        (Some(dir), Some(file), None, None) if is_deletion_vector_file(dir, file) => {
            Placement::Deletion { file }
        }
        (Some(staging), Some(id), Some(file), None)
            if staging == super::constants::STAGING_DIR_NAME =>
        {
            Placement::Staging {
                snapshot: format!("{staging}/{id}"),
                file,
            }
        }
        _ => Placement::Other,
    }
}

fn is_deletion_vector_file(dir: &str, file: &str) -> bool {
    dir == super::delete::vector_io::DELETION_DIR_NAME
        && std::path::Path::new(file)
            .extension()
            .is_some_and(|extension| extension == "arrow")
}

/// The file name of a deletion vector at `relative` (a key under the dataset's
/// data path): `<snapshot>/deletions/<file>.arrow`.
pub(crate) fn deletion_vector_name(relative: &str) -> Option<&str> {
    let mut parts = relative.split('/');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(_), Some(dir), Some(file), None) if is_deletion_vector_file(dir, file) => Some(file),
        _ => None,
    }
}

/// Whether an unnamed object last modified at `last_modified` is old enough to
/// delete when the store's clock reads `store_now` (the time it stamped on the
/// dataset's head). An object stamped after `store_now` (clock steps, a write
/// after the head's) is young.
pub(crate) fn older_than_floor(
    last_modified: DateTime<Utc>,
    store_now: DateTime<Utc>,
    floor: Duration,
) -> bool {
    store_now
        .signed_duration_since(last_modified)
        .to_std()
        .is_ok_and(|age| age >= floor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placement_separates_what_this_sweep_reclaims_from_other_sweeps() {
        assert_eq!(placement("cur/a.vortex"), Placement::Other);
        assert_eq!(placement("subset-src-x/d.vortex"), Placement::Other);
        assert_eq!(
            placement("_staging/w1/c.vortex"),
            Placement::Staging {
                snapshot: "_staging/w1".to_string(),
                file: "c.vortex"
            }
        );
        // A flat table's data files and deletion vectors.
        assert_eq!(
            placement("01a1_0.vortex"),
            Placement::Flat {
                file: "01a1_0.vortex"
            }
        );
        assert_eq!(
            placement("deletions/delete_01a1.arrow"),
            Placement::Deletion {
                file: "delete_01a1.arrow"
            }
        );
        // The lookup index, a deeper key, another file under `deletions/`, and
        // a reserved name at the top are ignored.
        assert_eq!(placement("_lookup_index/k/run.arrow"), Placement::Other);
        assert_eq!(placement("_lookup_index/x"), Placement::Other);
        assert_eq!(placement("deletions/e.vortex"), Placement::Other);
        assert_eq!(placement("cur/sub/f.vortex"), Placement::Other);
        assert_eq!(placement("_store_clock"), Placement::Other);
    }

    #[test]
    fn deletion_vectors_are_recognized_beside_the_table_prefix() {
        assert_eq!(
            deletion_vector_name("01a1bf18/deletions/delete_01a1.arrow"),
            Some("delete_01a1.arrow")
        );
        // A data file, a deeper key, another directory and another extension
        // are not deletion vectors.
        assert_eq!(deletion_vector_name("table/snap/a.vortex"), None);
        assert_eq!(deletion_vector_name("snap/deletions/sub/d.arrow"), None);
        assert_eq!(deletion_vector_name("snap/other/d.arrow"), None);
        assert_eq!(deletion_vector_name("snap/deletions/d.vortex"), None);
        assert_eq!(deletion_vector_name("_store_clock"), None);
    }

    #[test]
    fn the_floor_is_measured_on_the_store_clock_and_a_stamp_after_the_head_is_young() {
        let now = DateTime::parse_from_rfc3339("2026-11-08T12:00:00Z")
            .expect("time")
            .with_timezone(&Utc);
        let floor = Duration::from_secs(3600);
        assert!(older_than_floor(
            now - chrono::Duration::seconds(3600),
            now,
            floor
        ));
        assert!(!older_than_floor(
            now - chrono::Duration::seconds(3599),
            now,
            floor
        ));
        assert!(!older_than_floor(
            now + chrono::Duration::seconds(5),
            now,
            floor
        ));
    }
}
