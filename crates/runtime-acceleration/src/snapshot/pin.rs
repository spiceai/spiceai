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

//! Pinning: a dataset whose current snapshot was chosen through the API stays on
//! it while its writer publishes newer snapshots, until it is unpinned.
//!
//! The pinned snapshot is also the dataset's `current-snapshot-id`, so every
//! reader loads it, including versions of Spice without pins.

use super::DatasetMetadata;

/// The dataset property that holds the id of the snapshot the dataset is pinned
/// to. Every version of Spice that rewrites `metadata.json` keeps `properties` as
/// it reads it, so the pin survives a rewrite by an older version.
const PINNED_PROPERTY: &str = "pinned-snapshot-id";

/// The snapshot `dataset` is pinned to.
pub(super) fn pinned_snapshot_id(dataset: &DatasetMetadata) -> Option<u64> {
    dataset
        .properties
        .get(PINNED_PROPERTY)
        .and_then(|id| id.parse().ok())
}

/// The snapshot `dataset` is pinned to while it is also the current one. A
/// reader treats only such a pin as a reason to load an older snapshot.
pub(super) fn current_pin(dataset: &DatasetMetadata) -> Option<u64> {
    pinned_snapshot_id(dataset).filter(|pinned| dataset.current_snapshot_id == Some(*pinned))
}

/// Makes `snapshot_id` current and pins `dataset` to it. Returns whether
/// `dataset` changed.
pub(super) fn pin(dataset: &mut DatasetMetadata, snapshot_id: u64) -> bool {
    if current_pin(dataset) == Some(snapshot_id) {
        return false;
    }
    dataset.current_snapshot_id = Some(snapshot_id);
    dataset
        .properties
        .insert(PINNED_PROPERTY.to_string(), snapshot_id.to_string());
    true
}

/// Removes the pin and makes the newest snapshot current. Returns whether
/// `dataset` changed.
pub(super) fn unpin(dataset: &mut DatasetMetadata) -> bool {
    let newest = dataset
        .snapshots
        .iter()
        .map(|entry| entry.snapshot_id)
        .max();
    let removed = dataset.properties.remove(PINNED_PROPERTY).is_some();
    let moved = dataset.current_snapshot_id != newest;
    dataset.current_snapshot_id = newest;
    removed || moved
}

/// What a publish made current.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AfterPublish {
    /// The dataset is not pinned: the published snapshot is current.
    Current,
    /// The dataset is pinned: its pinned snapshot stays current.
    Pinned {
        snapshot_id: u64,
        /// The snapshot that was current instead of the pinned one, made current by
        /// a version of Spice without pins.
        restored_from: Option<u64>,
    },
    /// The dataset was pinned to a snapshot it no longer lists, so the pin was
    /// removed and the published snapshot is current.
    PinRemoved { snapshot_id: u64 },
}

/// Makes `published` current unless `dataset` is pinned to a snapshot it lists.
pub(super) fn make_current_after_publish(
    dataset: &mut DatasetMetadata,
    published: u64,
) -> AfterPublish {
    let Some(pinned) = pinned_snapshot_id(dataset) else {
        dataset.current_snapshot_id = Some(published);
        return AfterPublish::Current;
    };
    if !dataset
        .snapshots
        .iter()
        .any(|entry| entry.snapshot_id == pinned)
    {
        dataset.properties.remove(PINNED_PROPERTY);
        dataset.current_snapshot_id = Some(published);
        return AfterPublish::PinRemoved {
            snapshot_id: pinned,
        };
    }
    let restored_from = dataset
        .current_snapshot_id
        .filter(|current| *current != pinned);
    dataset.current_snapshot_id = Some(pinned);
    AfterPublish::Pinned {
        snapshot_id: pinned,
        restored_from,
    }
}

#[cfg(test)]
mod tests {
    use super::super::SnapshotEntry;
    use super::*;

    fn entry(id: u64) -> SnapshotEntry {
        SnapshotEntry {
            snapshot_id: id,
            timestamp_ms: 0,
            snapshot: format!("s3://bucket/snapshots/{id}.duckdb"),
            snapshot_checksum: String::new(),
            snapshot_checksum_algorithm: "SHA256".to_string(),
            snapshot_size: 0,
            snapshot_engine: None,
            snapshot_row_count: None,
            snapshot_last_updated_at_ms: None,
        }
    }

    fn dataset(ids: &[u64], current: u64) -> DatasetMetadata {
        DatasetMetadata {
            name: "orders".to_string(),
            snapshots: ids.iter().copied().map(entry).collect(),
            current_snapshot_id: Some(current),
            ..Default::default()
        }
    }

    #[test]
    fn a_pinned_snapshot_stays_current_when_a_newer_one_is_published() {
        let mut dataset = dataset(&[1, 2, 3], 3);
        assert!(pin(&mut dataset, 1));
        assert_eq!(dataset.current_snapshot_id, Some(1));
        assert!(!pin(&mut dataset, 1), "pinning again writes nothing");

        dataset.snapshots.push(entry(4));
        assert_eq!(
            make_current_after_publish(&mut dataset, 4),
            AfterPublish::Pinned {
                snapshot_id: 1,
                restored_from: None
            }
        );
        assert_eq!(current_pin(&dataset), Some(1));
    }

    #[test]
    fn a_publish_restores_a_pin_that_a_version_without_pins_moved() {
        let mut dataset = dataset(&[1, 2, 3], 3);
        pin(&mut dataset, 1);

        // A version without pins publishes snapshot 4: it rewrites the JSON with the new
        // entry current and keeps `properties` as it read them.
        let mut written = serde_json::to_value(&dataset).expect("serialize");
        written["snapshots"]
            .as_array_mut()
            .expect("snapshots")
            .push(serde_json::to_value(entry(4)).expect("serialize entry"));
        written["current-snapshot-id"] = serde_json::json!(4);
        let mut dataset: DatasetMetadata = serde_json::from_value(written).expect("deserialize");
        assert_eq!(pinned_snapshot_id(&dataset), Some(1));
        assert_eq!(current_pin(&dataset), None, "a moved pin is not current");

        dataset.snapshots.push(entry(5));
        assert_eq!(
            make_current_after_publish(&mut dataset, 5),
            AfterPublish::Pinned {
                snapshot_id: 1,
                restored_from: Some(4)
            }
        );
        assert_eq!(current_pin(&dataset), Some(1));
    }

    #[test]
    fn a_pin_to_a_snapshot_no_longer_listed_is_removed_by_the_next_publish() {
        let mut dataset = dataset(&[2, 3], 3);
        dataset
            .properties
            .insert(PINNED_PROPERTY.to_string(), "1".to_string());

        assert_eq!(
            make_current_after_publish(&mut dataset, 3),
            AfterPublish::PinRemoved { snapshot_id: 1 }
        );
        assert_eq!(dataset.current_snapshot_id, Some(3));
        assert_eq!(pinned_snapshot_id(&dataset), None);
    }

    #[test]
    fn unpinning_makes_the_newest_snapshot_current() {
        let mut dataset = dataset(&[1, 2, 3], 3);
        pin(&mut dataset, 1);

        assert!(unpin(&mut dataset));
        assert_eq!(dataset.current_snapshot_id, Some(3));
        assert_eq!(pinned_snapshot_id(&dataset), None);
        assert!(!unpin(&mut dataset), "unpinning again writes nothing");
    }

    #[test]
    fn unpinning_moves_a_current_snapshot_chosen_without_a_pin_to_the_newest() {
        let mut dataset = dataset(&[1, 2, 3], 2);
        assert!(unpin(&mut dataset));
        assert_eq!(dataset.current_snapshot_id, Some(3));
    }
}
