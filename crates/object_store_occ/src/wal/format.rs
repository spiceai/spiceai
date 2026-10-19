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

use std::collections::BTreeMap;

use bytes::Bytes;
use prost::Message;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, ensure};

use super::{CodecSnafu, InvalidRecordSnafu, LimitSnafu, Result, WorkerSnafu, wire};
use crate::store::MAX_VALUE_BYTES;

pub(super) const FORMAT: u32 = 2;
pub(super) const MAX_BATCH_BYTES: usize = 128 * 1024;
pub(super) const MAX_KEY_BYTES: usize = 1024;
pub(super) const MAX_BATCH_KEYS: usize = 1024;

/// Persisted bounds shared by every client of a transaction domain.
///
/// Checkpoints shorten replay, but do not reset `max_commits`: history is retained
/// for snapshots and commit resolution. Reaching that bound stops new commits.
/// Abandoned uploads and superseded checkpoint artifacts are not counted and
/// require a future reclamation protocol.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub max_state_bytes: usize,
    pub max_keys: usize,
    pub max_commits: u64,
    pub max_replay_commits: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_state_bytes: 64 * 1024 * 1024,
            max_keys: 65_536,
            max_commits: 65_536,
            max_replay_commits: 128,
        }
    }
}

impl Limits {
    pub(super) fn validate(self) -> Result<()> {
        let ceiling = Self::default();
        ensure!(
            self.max_state_bytes > 0
                && self.max_state_bytes <= ceiling.max_state_bytes
                && self.max_keys > 0
                && self.max_keys <= ceiling.max_keys
                && self.max_commits > 0
                && self.max_commits <= ceiling.max_commits
                && self.max_replay_commits > 0
                && self.max_replay_commits <= ceiling.max_replay_commits,
            LimitSnafu {
                resource: "domain limits"
            }
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Reference {
    pub id: [u8; 16],
    pub digest: [u8; 32],
}

impl Reference {
    pub fn new(id: [u8; 16], bytes: &Bytes) -> Self {
        Self {
            id,
            digest: *blake3::hash(bytes).as_bytes(),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct Head {
    pub format: u32,
    pub incarnation: [u8; 16],
    pub limits: Limits,
    pub sequence: u64,
    pub commit: Option<Reference>,
    pub checkpoint: Option<CheckpointReference>,
    pub state_bytes: usize,
    pub keys: usize,
}

impl Head {
    pub fn validate(&self) -> Result<()> {
        self.limits.validate()?;
        ensure!(
            self.format == FORMAT,
            InvalidRecordSnafu {
                reason: "head format"
            }
        );
        ensure!(
            self.sequence <= self.limits.max_commits
                && (self.sequence == 0) == self.commit.is_none()
                && self.state_bytes <= self.limits.max_state_bytes
                && self.keys <= self.limits.max_keys,
            InvalidRecordSnafu {
                reason: "head bounds"
            }
        );
        let watermark = self.checkpoint.as_ref().map_or(0, |c| c.sequence);
        ensure!(
            watermark <= self.sequence,
            InvalidRecordSnafu {
                reason: "checkpoint watermark"
            }
        );
        ensure!(
            self.sequence - watermark <= self.limits.max_replay_commits,
            InvalidRecordSnafu {
                reason: "replay bound"
            }
        );
        if let Some(checkpoint) = &self.checkpoint {
            ensure!(
                (checkpoint.sequence == 0) == checkpoint.commit.is_none(),
                InvalidRecordSnafu {
                    reason: "checkpoint ancestor"
                }
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(super) struct LogRecord {
    pub format: u32,
    pub incarnation: [u8; 16],
    pub id: [u8; 16],
    pub base_head: [u8; 16],
    pub sequence: u64,
    pub parent: Option<Reference>,
    pub changes: BTreeMap<String, Option<Bytes>>,
    pub state_bytes: usize,
    pub keys: usize,
}

impl LogRecord {
    pub fn validate(&self, head: &Head, reference: &Reference, sequence: u64) -> Result<()> {
        ensure!(
            self.format == FORMAT
                && self.incarnation == head.incarnation
                && self.id == reference.id
                && self.sequence == sequence
                && sequence > 0
                && (sequence == 1) == self.parent.is_none()
                && self.state_bytes <= head.limits.max_state_bytes
                && self.keys <= head.limits.max_keys,
            InvalidRecordSnafu {
                reason: "WAL identity or ancestry"
            }
        );
        validate_changes(&self.changes)?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(super) struct CheckpointReference {
    pub object: Reference,
    pub sequence: u64,
    pub commit: Option<Reference>,
}

#[derive(Debug)]
pub(super) struct Checkpoint {
    pub format: u32,
    pub incarnation: [u8; 16],
    pub sequence: u64,
    pub commit: Option<Reference>,
    pub pages: Vec<Reference>,
    pub state_bytes: usize,
    pub keys: usize,
}

#[derive(Debug)]
pub(super) struct Page {
    pub format: u32,
    pub incarnation: [u8; 16],
    pub entries: BTreeMap<String, Bytes>,
}

/// Serializable identity of an immutable commit attempt, saved before submission.
///
/// Treat receipts as trusted application recovery data, not authentication tokens.
/// They bind the domain, generation, complete WAL bytes and exact head publication.
/// A different transaction evaluation needs a new receipt; never edit these fields.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CommitReceipt {
    pub(super) format: u32,
    pub(super) prefix: String,
    pub(super) incarnation: [u8; 16],
    pub(super) base_head: [u8; 16],
    pub(super) sequence: u64,
    pub(super) record: Reference,
}

impl CommitReceipt {
    #[must_use]
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
}

pub(super) fn validate_key(key: &str) -> Result<()> {
    ensure!(
        !key.is_empty() && key.len() <= MAX_KEY_BYTES,
        LimitSnafu {
            resource: "key length"
        }
    );
    Ok(())
}

pub(super) fn validate_changes(changes: &BTreeMap<String, Option<Bytes>>) -> Result<()> {
    ensure!(
        !changes.is_empty() && changes.len() <= MAX_BATCH_KEYS,
        LimitSnafu {
            resource: "batch key count"
        }
    );
    let mut size = 0_usize;
    for (key, value) in changes {
        validate_key(key)?;
        let length = key
            .len()
            .saturating_add(value.as_ref().map_or(0, Bytes::len));
        size = size.saturating_add(length);
        ensure!(
            size <= MAX_BATCH_BYTES,
            LimitSnafu {
                resource: "batch bytes"
            }
        );
    }
    Ok(())
}

pub(super) fn state_size(entries: &BTreeMap<String, Bytes>, limits: Limits) -> Result<usize> {
    ensure!(
        entries.len() <= limits.max_keys,
        LimitSnafu {
            resource: "state key count"
        }
    );
    let mut size = 0_usize;
    for (key, value) in entries {
        validate_key(key)?;
        size = size.saturating_add(key.len()).saturating_add(value.len());
        ensure!(
            size <= limits.max_state_bytes,
            LimitSnafu {
                resource: "state bytes"
            }
        );
    }
    Ok(size)
}

/// A stored record and the protobuf message that encodes it.
pub(super) trait Wire: Sized {
    type Message: Message + Default;

    fn to_wire(&self) -> Self::Message;

    fn from_wire(message: Self::Message) -> Result<Self>;
}

/// Encodes `value`, refusing a record larger than a state value may be.
pub(super) fn encode<T: Wire>(value: &T) -> Result<Bytes> {
    let message = value.to_wire();
    ensure!(
        message.encoded_len() <= MAX_VALUE_BYTES,
        LimitSnafu {
            resource: "encoded record bytes"
        }
    );
    Ok(Bytes::from(message.encode_to_vec()))
}

/// Decodes a record on the calling thread.
pub(super) fn decode_now<T: Wire>(bytes: Bytes) -> Result<T> {
    T::from_wire(T::Message::decode(bytes).context(CodecSnafu)?)
}

pub(super) async fn decode<T: Wire + Send + 'static>(bytes: Bytes) -> Result<T> {
    cpu(move || decode_now(bytes)).await
}

pub(super) async fn cpu<T: Send + 'static>(
    f: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    let (send, receive) = tokio::sync::oneshot::channel();
    rayon::spawn(move || {
        let _ = send.send(f());
    });
    receive.await.context(WorkerSnafu)?
}

fn wire_count(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn count(value: u64, reason: &'static str) -> Result<usize> {
    usize::try_from(value)
        .ok()
        .context(InvalidRecordSnafu { reason })
}

fn fixed<const N: usize>(bytes: &[u8], reason: &'static str) -> Result<[u8; N]> {
    <[u8; N]>::try_from(bytes)
        .ok()
        .context(InvalidRecordSnafu { reason })
}

fn required<T>(value: Option<T>, reason: &'static str) -> Result<T> {
    value.context(InvalidRecordSnafu { reason })
}

impl Limits {
    fn to_wire(self) -> wire::Limits {
        wire::Limits {
            max_state_bytes: wire_count(self.max_state_bytes),
            max_keys: wire_count(self.max_keys),
            max_commits: self.max_commits,
            max_replay_commits: self.max_replay_commits,
        }
    }

    fn from_wire(limits: wire::Limits) -> Result<Self> {
        Ok(Self {
            max_state_bytes: count(limits.max_state_bytes, "limits")?,
            max_keys: count(limits.max_keys, "limits")?,
            max_commits: limits.max_commits,
            max_replay_commits: limits.max_replay_commits,
        })
    }
}

impl Reference {
    fn to_wire(&self) -> wire::Reference {
        wire::Reference {
            id: Bytes::copy_from_slice(&self.id),
            digest: Bytes::copy_from_slice(&self.digest),
        }
    }

    fn from_wire(reference: &wire::Reference) -> Result<Self> {
        Ok(Self {
            id: fixed(&reference.id, "reference identity")?,
            digest: fixed(&reference.digest, "reference digest")?,
        })
    }
}

impl CheckpointReference {
    fn to_wire(&self) -> wire::CheckpointReference {
        wire::CheckpointReference {
            object: Some(self.object.to_wire()),
            sequence: self.sequence,
            commit: self.commit.as_ref().map(Reference::to_wire),
        }
    }

    fn from_wire(reference: wire::CheckpointReference) -> Result<Self> {
        Ok(Self {
            object: Reference::from_wire(&required(reference.object, "checkpoint object")?)?,
            sequence: reference.sequence,
            commit: reference
                .commit
                .as_ref()
                .map(Reference::from_wire)
                .transpose()?,
        })
    }
}

impl Wire for Head {
    type Message = wire::Head;

    fn to_wire(&self) -> wire::Head {
        wire::Head {
            format: self.format,
            incarnation: Bytes::copy_from_slice(&self.incarnation),
            limits: Some(self.limits.to_wire()),
            sequence: self.sequence,
            commit: self.commit.as_ref().map(Reference::to_wire),
            checkpoint: self.checkpoint.as_ref().map(CheckpointReference::to_wire),
            state_bytes: wire_count(self.state_bytes),
            keys: wire_count(self.keys),
        }
    }

    fn from_wire(head: wire::Head) -> Result<Self> {
        Ok(Self {
            format: head.format,
            incarnation: fixed(&head.incarnation, "head incarnation")?,
            limits: Limits::from_wire(required(head.limits, "head limits")?)?,
            sequence: head.sequence,
            commit: head.commit.as_ref().map(Reference::from_wire).transpose()?,
            checkpoint: head
                .checkpoint
                .map(CheckpointReference::from_wire)
                .transpose()?,
            state_bytes: count(head.state_bytes, "head bounds")?,
            keys: count(head.keys, "head bounds")?,
        })
    }
}

impl Wire for LogRecord {
    type Message = wire::LogRecord;

    fn to_wire(&self) -> wire::LogRecord {
        wire::LogRecord {
            format: self.format,
            incarnation: Bytes::copy_from_slice(&self.incarnation),
            id: Bytes::copy_from_slice(&self.id),
            base_head: Bytes::copy_from_slice(&self.base_head),
            sequence: self.sequence,
            parent: self.parent.as_ref().map(Reference::to_wire),
            changes: self
                .changes
                .iter()
                .map(|(key, value)| wire::Change {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect(),
            state_bytes: wire_count(self.state_bytes),
            keys: wire_count(self.keys),
        }
    }

    fn from_wire(record: wire::LogRecord) -> Result<Self> {
        let mut changes = BTreeMap::new();
        for change in record.changes {
            ensure!(
                changes
                    .last_key_value()
                    .is_none_or(|(last, _)| *last < change.key),
                InvalidRecordSnafu {
                    reason: "WAL change order"
                }
            );
            changes.insert(change.key, change.value);
        }
        Ok(Self {
            format: record.format,
            incarnation: fixed(&record.incarnation, "WAL incarnation")?,
            id: fixed(&record.id, "WAL identity")?,
            base_head: fixed(&record.base_head, "WAL base head")?,
            sequence: record.sequence,
            parent: record
                .parent
                .as_ref()
                .map(Reference::from_wire)
                .transpose()?,
            changes,
            state_bytes: count(record.state_bytes, "WAL bounds")?,
            keys: count(record.keys, "WAL bounds")?,
        })
    }
}

impl Wire for Checkpoint {
    type Message = wire::Checkpoint;

    fn to_wire(&self) -> wire::Checkpoint {
        wire::Checkpoint {
            format: self.format,
            incarnation: Bytes::copy_from_slice(&self.incarnation),
            sequence: self.sequence,
            commit: self.commit.as_ref().map(Reference::to_wire),
            pages: self.pages.iter().map(Reference::to_wire).collect(),
            state_bytes: wire_count(self.state_bytes),
            keys: wire_count(self.keys),
        }
    }

    fn from_wire(checkpoint: wire::Checkpoint) -> Result<Self> {
        Ok(Self {
            format: checkpoint.format,
            incarnation: fixed(&checkpoint.incarnation, "checkpoint incarnation")?,
            sequence: checkpoint.sequence,
            commit: checkpoint
                .commit
                .as_ref()
                .map(Reference::from_wire)
                .transpose()?,
            pages: checkpoint
                .pages
                .iter()
                .map(Reference::from_wire)
                .collect::<Result<_>>()?,
            state_bytes: count(checkpoint.state_bytes, "checkpoint bounds")?,
            keys: count(checkpoint.keys, "checkpoint bounds")?,
        })
    }
}

impl Wire for Page {
    type Message = wire::Page;

    fn to_wire(&self) -> wire::Page {
        wire::Page {
            format: self.format,
            incarnation: Bytes::copy_from_slice(&self.incarnation),
            entries: self
                .entries
                .iter()
                .map(|(key, value)| wire::Entry {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect(),
        }
    }

    fn from_wire(page: wire::Page) -> Result<Self> {
        let mut entries = BTreeMap::new();
        for entry in page.entries {
            ensure!(
                entries
                    .last_key_value()
                    .is_none_or(|(last, _)| *last < entry.key),
                InvalidRecordSnafu {
                    reason: "checkpoint key order"
                }
            );
            entries.insert(entry.key, entry.value);
        }
        Ok(Self {
            format: page.format,
            incarnation: fixed(&page.incarnation, "checkpoint page incarnation")?,
            entries,
        })
    }
}
