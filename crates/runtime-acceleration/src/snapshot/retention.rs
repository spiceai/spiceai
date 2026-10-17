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

//! Snapshot retention: the entries a dataset's snapshot writer drops from
//! `metadata.json` when it publishes, and when their files may be deleted.
//!
//! A dataset keeps its snapshots of the last [`SNAPSHOT_RETENTION`], and always
//! its current snapshot (a rollback can make an older one current), the snapshot
//! it is pinned to, and its newest one. The writer drops the other entries in the conditional write that
//! publishes a snapshot and records when it dropped each, so a reader that read
//! the entry before the drop can still download its file for [`FILE_GRACE`].

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;

use super::{DatasetMetadata, SnapshotEntry, pin};

/// How long a dataset keeps a snapshot that is not its current, pinned or newest one.
pub(super) const SNAPSHOT_RETENTION: Duration = Duration::from_hours(28 * 24);

/// How long a dropped snapshot's file stays, so a reader that is still
/// downloading it finishes. A file no entry names is deleted only once it is
/// this old, so an upload in progress is never deleted, and an entry is dropped
/// for a missing file only once it is this old, for the same reason.
pub(super) const FILE_GRACE: Duration = Duration::from_hours(24);

/// The dataset property that records when the writer dropped a snapshot:
/// `dropped-snapshot:<snapshot URI>` holds the drop time in milliseconds since
/// the UNIX epoch. Every version of Spice that rewrites `metadata.json` keeps
/// `properties` as it reads it, so the record survives a rewrite by an older
/// version.
const DROPPED_PROPERTY_PREFIX: &str = "dropped-snapshot:";

/// The time the retention rules measure ages against, in milliseconds since the
/// UNIX epoch.
#[derive(Clone)]
pub(super) struct CleanupClock(Arc<dyn Fn() -> i64 + Send + Sync>);

impl CleanupClock {
    pub(super) fn now_ms(&self) -> i64 {
        (self.0)()
    }

    /// A clock that reads `offset` later than the system clock.
    #[cfg(test)]
    pub(super) fn ahead_by(offset: Duration) -> Self {
        let offset_ms = i64::try_from(offset.as_millis()).unwrap_or(i64::MAX);
        Self(Arc::new(move || {
            Utc::now().timestamp_millis().saturating_add(offset_ms)
        }))
    }
}

impl Default for CleanupClock {
    fn default() -> Self {
        Self(Arc::new(|| Utc::now().timestamp_millis()))
    }
}

/// What [`apply`] changed in a dataset's entry.
#[derive(Debug, Default, Clone)]
pub(super) struct Retention {
    /// Entries dropped because they are older than the retention.
    pub(super) aged_out: Vec<SnapshotEntry>,
    /// Entries dropped because their file is gone.
    pub(super) lost: Vec<SnapshotEntry>,
    /// Snapshots dropped at least the file grace ago, whose files may now be
    /// deleted. Their drop records were removed.
    pub(super) expired: Vec<String>,
    /// The pinned snapshot, kept although its file is gone.
    pub(super) pinned_gone: Option<SnapshotEntry>,
}

/// Applies retention to `dataset` at `now_ms`.
///
/// Drops every entry that is not the current, pinned or newest snapshot and is
/// either older than `retention` or names a snapshot in `gone`, and records when
/// it dropped each aged-out entry. Removes the drop records that are at least
/// `grace` old and returns their snapshots in [`Retention::expired`].
pub(super) fn apply(
    dataset: &mut DatasetMetadata,
    now_ms: i64,
    gone: &HashSet<String>,
    retention: Duration,
    grace: Duration,
) -> Retention {
    let current = dataset.current_snapshot_id;
    let pinned = pin::pinned_snapshot_id(dataset);
    let newest = dataset
        .snapshots
        .iter()
        .map(|entry| entry.snapshot_id)
        .max();
    let oldest_kept_ms = now_ms.saturating_sub(millis(retention));

    let mut result = Retention::default();
    dataset.snapshots.retain(|entry| {
        if Some(entry.snapshot_id) == pinned && gone.contains(&entry.snapshot) {
            result.pinned_gone = Some(entry.clone());
        }
        if [current, pinned, newest].contains(&Some(entry.snapshot_id)) {
            return true;
        }
        if gone.contains(&entry.snapshot) {
            result.lost.push(entry.clone());
            return false;
        }
        if entry.timestamp_ms < oldest_kept_ms {
            result.aged_out.push(entry.clone());
            return false;
        }
        true
    });

    let expired_by_ms = now_ms.saturating_sub(millis(grace));
    dataset.properties.retain(|key, value| {
        let Some(snapshot) = key.strip_prefix(DROPPED_PROPERTY_PREFIX) else {
            return true;
        };
        match value.parse::<i64>() {
            Ok(dropped_at_ms) if dropped_at_ms <= expired_by_ms => {
                result.expired.push(snapshot.to_string());
                false
            }
            _ => true,
        }
    });
    for entry in &result.aged_out {
        dataset.properties.insert(
            format!("{DROPPED_PROPERTY_PREFIX}{}", entry.snapshot),
            now_ms.to_string(),
        );
    }
    result.expired.sort_unstable();
    result
}

/// The snapshots whose files `dataset` still needs: its entries and the
/// snapshots it dropped less than the file grace ago.
pub(super) fn named_snapshots(dataset: &DatasetMetadata) -> impl Iterator<Item = &str> {
    dataset
        .snapshots
        .iter()
        .map(|entry| entry.snapshot.as_str())
        .chain(
            dataset
                .properties
                .keys()
                .filter_map(|key| key.strip_prefix(DROPPED_PROPERTY_PREFIX)),
        )
}

pub(super) fn millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    const DAY_MS: i64 = 24 * 60 * 60 * 1000;
    const NOW: i64 = 100 * DAY_MS;

    fn entry(snapshot_id: u64, age_ms: i64) -> SnapshotEntry {
        SnapshotEntry {
            snapshot_id,
            timestamp_ms: NOW - age_ms,
            snapshot: format!("s3://bucket/snapshots/{snapshot_id}.cayenne"),
            snapshot_checksum: String::new(),
            snapshot_checksum_algorithm: "SHA256".to_string(),
            snapshot_size: 0,
            snapshot_engine: Some("cayenne".to_string()),
            snapshot_row_count: None,
            snapshot_last_updated_at_ms: None,
        }
    }

    fn dataset(entries: Vec<SnapshotEntry>, current: u64) -> DatasetMetadata {
        DatasetMetadata {
            name: "orders".to_string(),
            snapshots: entries,
            current_snapshot_id: Some(current),
            properties: HashMap::from([("writer-generation".to_string(), "3".to_string())]),
            ..Default::default()
        }
    }

    fn ids(entries: &[SnapshotEntry]) -> Vec<u64> {
        entries.iter().map(|entry| entry.snapshot_id).collect()
    }

    fn apply_now(dataset: &mut DatasetMetadata, gone: &HashSet<String>) -> Retention {
        apply(dataset, NOW, gone, SNAPSHOT_RETENTION, FILE_GRACE)
    }

    /// A snapshot exactly 28 days old stays, as does a younger one; one a
    /// millisecond older is dropped, and its drop time is recorded beside the
    /// properties already there.
    #[test]
    fn a_snapshot_older_than_twenty_eight_days_is_dropped_and_its_drop_recorded() {
        let mut dataset = dataset(
            vec![
                entry(1, 28 * DAY_MS + 1),
                entry(2, 28 * DAY_MS),
                entry(3, 20 * DAY_MS),
                entry(4, 0),
            ],
            4,
        );

        let retention = apply_now(&mut dataset, &HashSet::new());

        assert_eq!(ids(&retention.aged_out), vec![1]);
        assert_eq!(ids(&dataset.snapshots), vec![2, 3, 4]);
        assert_eq!(
            dataset.properties,
            HashMap::from([
                ("writer-generation".to_string(), "3".to_string()),
                (
                    "dropped-snapshot:s3://bucket/snapshots/1.cayenne".to_string(),
                    NOW.to_string()
                ),
            ])
        );
        assert!(retention.expired.is_empty());
    }

    /// After a rollback the current snapshot can be the oldest; it stays at any
    /// age, as does the newest when the writer stopped publishing long ago.
    #[test]
    fn the_current_and_the_newest_snapshot_stay_at_any_age() {
        let mut dataset = dataset(
            vec![
                entry(1, 40 * DAY_MS),
                entry(2, 35 * DAY_MS),
                entry(3, 30 * DAY_MS),
            ],
            1,
        );

        let retention = apply_now(&mut dataset, &HashSet::new());

        assert_eq!(ids(&retention.aged_out), vec![2]);
        assert_eq!(ids(&dataset.snapshots), vec![1, 3]);
    }

    /// The pinned snapshot stays at any age, also after a version of Spice without
    /// pins made another snapshot current, and also when its file is gone.
    #[test]
    fn the_pinned_snapshot_stays_at_any_age_and_without_its_file() {
        let mut dataset = dataset(
            vec![entry(1, 40 * DAY_MS), entry(2, 35 * DAY_MS), entry(3, 0)],
            2,
        );
        dataset
            .properties
            .insert("pinned-snapshot-id".to_string(), "1".to_string());
        let gone: HashSet<String> = ["s3://bucket/snapshots/1.cayenne".to_string()].into();

        let retention = apply_now(&mut dataset, &gone);

        assert_eq!(ids(&dataset.snapshots), vec![1, 2, 3]);
        assert!(retention.lost.is_empty() && retention.aged_out.is_empty());
        assert_eq!(
            retention.pinned_gone.map(|entry| entry.snapshot_id),
            Some(1)
        );
    }

    /// An entry whose file is gone is dropped at any age, without a drop record,
    /// unless it is the current or the newest snapshot.
    #[test]
    fn an_entry_whose_file_is_gone_is_dropped() {
        let mut dataset = dataset(vec![entry(1, DAY_MS), entry(2, DAY_MS), entry(3, 0)], 2);
        let gone: HashSet<String> = [1, 2, 3]
            .map(|id| format!("s3://bucket/snapshots/{id}.cayenne"))
            .into();

        let retention = apply_now(&mut dataset, &gone);

        assert_eq!(ids(&retention.lost), vec![1]);
        assert!(retention.aged_out.is_empty());
        assert_eq!(ids(&dataset.snapshots), vec![2, 3]);
        assert_eq!(dataset.properties.len(), 1, "{:?}", dataset.properties);
    }

    /// A dropped snapshot's file becomes due for deletion 24 hours after the drop,
    /// and its record goes with it.
    #[test]
    fn a_dropped_snapshot_is_due_for_deletion_24_hours_after_the_drop() {
        let mut dataset = dataset(vec![entry(3, 0)], 3);
        dataset.properties.insert(
            "dropped-snapshot:s3://bucket/snapshots/1.cayenne".to_string(),
            (NOW - DAY_MS).to_string(),
        );
        dataset.properties.insert(
            "dropped-snapshot:s3://bucket/snapshots/2.cayenne".to_string(),
            (NOW - DAY_MS + 1).to_string(),
        );

        let retention = apply_now(&mut dataset, &HashSet::new());

        assert_eq!(
            retention.expired,
            vec!["s3://bucket/snapshots/1.cayenne".to_string()]
        );
        let mut named: Vec<&str> = named_snapshots(&dataset).collect();
        named.sort_unstable();
        assert_eq!(
            named,
            vec![
                "s3://bucket/snapshots/2.cayenne",
                "s3://bucket/snapshots/3.cayenne"
            ]
        );
    }
}
