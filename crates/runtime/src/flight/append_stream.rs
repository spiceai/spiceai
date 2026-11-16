/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

//! The `app_metadata` an append stream (`DoExchange`) carries so a subscriber
//! knows when its snapshot is complete and which updates it already holds.
//!
//! The producer tags the truncate that opens a subscription's snapshot with
//! `begin` and its epoch, and follows the last snapshot batch with a schema
//! message tagged `end`, the epoch, and the sequence number of the last commit
//! the snapshot holds. Every update after that carries the epoch and its
//! commit's sequence number. All of it is additive: a subscriber that does not
//! read `app_metadata` sees the same stream as before (a schema message is not
//! data), and a subscriber reading a producer that sends no `begin` keeps the
//! old behavior.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Which part of the stream a message is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StreamMarker {
    Begin,
    End,
    Update,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StreamMetadata {
    #[serde(rename = "spice_append_stream")]
    pub(crate) marker: StreamMarker,
    pub(crate) epoch: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) sequence: Option<u64>,
}

impl StreamMetadata {
    pub(crate) fn begin(epoch: &str) -> Self {
        Self {
            marker: StreamMarker::Begin,
            epoch: epoch.to_string(),
            sequence: None,
        }
    }

    pub(crate) fn end(epoch: &str, snapshot_sequence: u64) -> Self {
        Self {
            marker: StreamMarker::End,
            epoch: epoch.to_string(),
            sequence: Some(snapshot_sequence),
        }
    }

    pub(crate) fn update(epoch: &str, sequence: u64) -> Self {
        Self {
            marker: StreamMarker::Update,
            epoch: epoch.to_string(),
            sequence: Some(sequence),
        }
    }

    pub(crate) fn to_bytes(&self) -> Bytes {
        // Serializing a struct of strings and integers does not fail.
        Bytes::from(serde_json::to_vec(self).unwrap_or_default())
    }

    /// The metadata on a message, or `None` for a message without it, or with
    /// `app_metadata` that is not this protocol's.
    pub(crate) fn parse(app_metadata: &[u8]) -> Option<Self> {
        if app_metadata.is_empty() {
            return None;
        }
        serde_json::from_slice(app_metadata).ok()
    }
}

/// What a subscriber does with one message of its subscription.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Step {
    /// Apply the batch. `ready` is whether the dataset is ready once it is.
    Apply { ready: bool },
    /// The snapshot is complete: the dataset is ready.
    SnapshotComplete,
    /// The batch is a commit the snapshot already holds.
    Skip,
    /// Nothing to do.
    Ignore,
    /// The stream is not one this subscription can follow (an update from
    /// another producer epoch): take a fresh snapshot.
    Resnapshot(String),
}

/// One subscription's place in the stream.
#[derive(Debug, Default)]
pub(crate) struct Subscription {
    /// The epoch the snapshot began under, once a `begin` is seen.
    epoch: Option<String>,
    /// The last commit the snapshot holds, once its `end` is seen.
    snapshot_sequence: Option<u64>,
}

impl Subscription {
    /// Whether the producer marks its snapshot and the snapshot is still
    /// loading.
    pub(crate) fn awaiting_end(&self) -> bool {
        self.epoch.is_some() && self.snapshot_sequence.is_none()
    }

    /// Whether the producer marks its snapshot.
    pub(crate) fn epoch(&self) -> Option<&str> {
        self.epoch.as_deref()
    }

    /// A record batch, with the metadata of its message.
    pub(crate) fn on_batch(&mut self, metadata: Option<StreamMetadata>) -> Step {
        let Some(metadata) = metadata else {
            // An unmarked batch: a snapshot row while a marked snapshot loads,
            // else a producer that does not mark its stream.
            return Step::Apply {
                ready: !self.awaiting_end(),
            };
        };
        match metadata.marker {
            StreamMarker::Begin => {
                self.epoch = Some(metadata.epoch);
                self.snapshot_sequence = None;
                Step::Apply { ready: false }
            }
            StreamMarker::End => self.on_marker(Some(metadata)),
            StreamMarker::Update => {
                let Some(epoch) = self.epoch.as_deref() else {
                    return Step::Apply { ready: true };
                };
                if epoch != metadata.epoch {
                    return Step::Resnapshot(format!(
                        "an update from producer epoch {} arrived on a snapshot from epoch {epoch}",
                        metadata.epoch
                    ));
                }
                match (self.snapshot_sequence, metadata.sequence) {
                    (Some(snapshot), Some(sequence)) if sequence <= snapshot => Step::Skip,
                    (Some(_), _) => Step::Apply { ready: true },
                    (None, _) => Step::Apply { ready: false },
                }
            }
        }
    }

    /// A message without a record batch (a schema message), with its metadata.
    pub(crate) fn on_marker(&mut self, metadata: Option<StreamMetadata>) -> Step {
        let Some(metadata) = metadata else {
            return Step::Ignore;
        };
        if metadata.marker != StreamMarker::End {
            return Step::Ignore;
        }
        match self.epoch.as_deref() {
            // An `end` without a `begin` is not a snapshot this subscriber saw
            // start, so it is not taken to complete one.
            None => Step::Ignore,
            Some(epoch) if epoch != metadata.epoch => Step::Resnapshot(format!(
                "the snapshot from producer epoch {epoch} ended under epoch {}",
                metadata.epoch
            )),
            Some(_) => {
                self.snapshot_sequence = Some(metadata.sequence.unwrap_or(0));
                Step::SnapshotComplete
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata(bytes: &Bytes) -> Option<StreamMetadata> {
        StreamMetadata::parse(bytes)
    }

    #[test]
    fn metadata_round_trips_and_ignores_foreign_app_metadata() {
        let end = StreamMetadata::end("e1", 7);
        assert_eq!(metadata(&end.to_bytes()), Some(end));
        assert_eq!(StreamMetadata::parse(b""), None);
        assert_eq!(StreamMetadata::parse(b"not json"), None);
        assert_eq!(StreamMetadata::parse(br#"{"other":"field"}"#), None);
    }

    #[test]
    fn ready_waits_for_the_end_of_a_marked_snapshot() {
        let mut subscription = Subscription::default();
        assert_eq!(
            subscription.on_batch(Some(StreamMetadata::begin("e1"))),
            Step::Apply { ready: false }
        );
        assert_eq!(subscription.on_batch(None), Step::Apply { ready: false });
        assert!(subscription.awaiting_end());
        assert_eq!(
            subscription.on_marker(Some(StreamMetadata::end("e1", 3))),
            Step::SnapshotComplete
        );
        assert_eq!(
            subscription.on_batch(Some(StreamMetadata::update("e1", 4))),
            Step::Apply { ready: true }
        );
    }

    #[test]
    fn an_unmarked_producer_keeps_every_batch_ready() {
        let mut subscription = Subscription::default();
        assert_eq!(subscription.on_batch(None), Step::Apply { ready: true });
        assert_eq!(subscription.on_marker(None), Step::Ignore);
        assert_eq!(
            subscription.on_marker(Some(StreamMetadata::end("e1", 3))),
            Step::Ignore,
            "an end without a begin completes nothing"
        );
        assert_eq!(subscription.on_batch(None), Step::Apply { ready: true });
    }

    #[test]
    fn commits_the_snapshot_holds_are_skipped() {
        let mut subscription = Subscription::default();
        subscription.on_batch(Some(StreamMetadata::begin("e1")));
        subscription.on_marker(Some(StreamMetadata::end("e1", 5)));
        assert_eq!(
            subscription.on_batch(Some(StreamMetadata::update("e1", 4))),
            Step::Skip
        );
        assert_eq!(
            subscription.on_batch(Some(StreamMetadata::update("e1", 5))),
            Step::Skip
        );
        assert_eq!(
            subscription.on_batch(Some(StreamMetadata::update("e1", 6))),
            Step::Apply { ready: true }
        );
    }

    #[test]
    fn an_update_from_another_epoch_takes_a_fresh_snapshot() {
        let mut subscription = Subscription::default();
        subscription.on_batch(Some(StreamMetadata::begin("e1")));
        subscription.on_marker(Some(StreamMetadata::end("e1", 5)));
        assert!(matches!(
            subscription.on_batch(Some(StreamMetadata::update("e2", 1))),
            Step::Resnapshot(_)
        ));
        let mut subscription = Subscription::default();
        subscription.on_batch(Some(StreamMetadata::begin("e1")));
        assert!(matches!(
            subscription.on_marker(Some(StreamMetadata::end("e2", 1))),
            Step::Resnapshot(_)
        ));
    }
}
