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
//! A listed object is deleted only when all of these hold:
//!
//! - no snapshot the table's state names it (every manifest row of the table),
//!   and it sits in a live (current or protected) snapshot directory, a staging
//!   directory without a staging WAL, a compaction's scratch directory, or a
//!   snapshot directory nothing refers to. A retired directory (the
//!   retired-directory sweep's: one this process retired, or one the committed
//!   retirement record names until that sweep forgets it) and a directory an
//!   open reader holds or listed recently are skipped whole;
//! - no write of this process still holds it ([`super::uncommitted_files`]),
//!   whatever its age, so an upload or a commit still in flight keeps its files;
//! - it is older than the age floor, measured on the store's clock: its
//!   `last_modified` against the time the store stamps on an object this sweep
//!   writes, never the writer's clock or the time in its name.
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

use std::collections::HashSet;
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

/// The object the sweep writes to read the store's clock, at the table's prefix.
pub(crate) const STORE_CLOCK_OBJECT: &str = "_store_clock";

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
}

/// Where a listed object sits, relative to the table's prefix.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Placement<'a> {
    /// `<snapshot>/<file>` in a current or protected snapshot directory.
    Live { snapshot: &'a str, file: &'a str },
    /// `_staging/<id>/<file>`: `snapshot` is `_staging/<id>`.
    Staging { snapshot: String, file: &'a str },
    /// `<scratch>/<file>` in a compaction's scratch directory.
    Scratch { snapshot: &'a str, file: &'a str },
    /// `<snapshot>/<file>` in a snapshot directory that is not live: an
    /// abandoned compaction or overwrite output, or a retired one.
    NotLive { snapshot: &'a str, file: &'a str },
    /// Anything else: another sweep's, or not a data file.
    Other,
}

/// The directory names a compaction gives the snapshots it builds and scans
/// itself before any commit names them.
const SCRATCH_PREFIXES: [&str; 1] = ["subset-src-"];

/// Classifies `relative` (a key under the table's prefix) against the table's
/// `live` snapshot ids.
pub(crate) fn placement<'a>(relative: &'a str, live: &HashSet<String>) -> Placement<'a> {
    let mut parts = relative.split('/');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(staging), Some(id), Some(file), None)
            if staging == super::constants::STAGING_DIR_NAME =>
        {
            Placement::Staging {
                snapshot: format!("{staging}/{id}"),
                file,
            }
        }
        (Some(snapshot), Some(file), None, None) if live.contains(snapshot) => {
            Placement::Live { snapshot, file }
        }
        (Some(snapshot), Some(file), None, None)
            if SCRATCH_PREFIXES
                .iter()
                .any(|prefix| snapshot.starts_with(prefix)) =>
        {
            Placement::Scratch { snapshot, file }
        }
        (Some(snapshot), Some(file), None, None) if uuid::Uuid::parse_str(snapshot).is_ok() => {
            Placement::NotLive { snapshot, file }
        }
        _ => Placement::Other,
    }
}

/// Whether an unnamed object last modified at `last_modified` is old enough to
/// delete when the store's clock reads `store_now`. An object stamped after
/// `store_now` (clock steps, a later write) is young.
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
        let live: HashSet<String> = ["cur".to_string(), "prot".to_string()].into();
        assert_eq!(
            placement("cur/a.vortex", &live),
            Placement::Live {
                snapshot: "cur",
                file: "a.vortex"
            }
        );
        assert_eq!(
            placement("prot/b.vortex", &live),
            Placement::Live {
                snapshot: "prot",
                file: "b.vortex"
            }
        );
        assert_eq!(
            placement("_staging/w1/c.vortex", &live),
            Placement::Staging {
                snapshot: "_staging/w1".to_string(),
                file: "c.vortex"
            }
        );
        assert_eq!(
            placement("subset-src-x/d.vortex", &live),
            Placement::Scratch {
                snapshot: "subset-src-x",
                file: "d.vortex"
            }
        );
        let old = "01a1bf18-c3b4-7562-86c7-7222e98235ad";
        assert_eq!(
            placement(&format!("{old}/e.vortex"), &live),
            Placement::NotLive {
                snapshot: old,
                file: "e.vortex"
            }
        );
        // A directory that is not a snapshot's, a deeper key, and the clock
        // object are ignored.
        assert_eq!(placement("deletions/e.arrow", &live), Placement::Other);
        assert_eq!(placement("cur/sub/f.vortex", &live), Placement::Other);
        assert_eq!(placement(STORE_CLOCK_OBJECT, &live), Placement::Other);
    }

    #[test]
    fn the_floor_is_measured_on_the_store_clock_and_a_future_stamp_is_young() {
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
