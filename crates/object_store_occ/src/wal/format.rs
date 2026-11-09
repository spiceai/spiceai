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
use std::io::Write;

use bytes::Bytes;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use snafu::{ResultExt, ensure};

use super::{CodecSnafu, InvalidRecordSnafu, LimitSnafu, Result, WorkerSnafu};
use crate::store::MAX_VALUE_BYTES;

pub(super) const FORMAT: u32 = 1;
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CheckpointReference {
    pub object: Reference,
    pub sequence: u64,
    pub commit: Option<Reference>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Checkpoint {
    pub format: u32,
    pub incarnation: [u8; 16],
    pub sequence: u64,
    pub commit: Option<Reference>,
    pub pages: Vec<Reference>,
    pub state_bytes: usize,
    pub keys: usize,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

/// Bounds serializer output before allocation; decode is bounded by `StateStore`.
struct BoundedWriter(Vec<u8>);

impl Write for BoundedWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > MAX_VALUE_BYTES - self.0.len() {
            return Err(std::io::Error::other("state record exceeds byte limit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(super) fn encode<T: Serialize>(value: &T) -> Result<Bytes> {
    let mut writer = BoundedWriter(Vec::new());
    serde_json::to_writer(&mut writer, value).context(CodecSnafu)?;
    Ok(Bytes::from(writer.0))
}

pub(super) async fn decode<T: DeserializeOwned + Send + 'static>(bytes: Bytes) -> Result<T> {
    cpu(move || serde_json::from_slice(&bytes).context(CodecSnafu)).await
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
