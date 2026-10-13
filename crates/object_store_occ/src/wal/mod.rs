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

//! Transactional K/V state with an immutable WAL and materialized MVCC snapshots.
//!
//! One conditional head publication commits a batch across all keys in a domain.
//! Transactions validate the entire head read by their snapshot (including absent
//! keys and predicate reads). Conflicts require a fresh transaction evaluation;
//! transport retries use the same [`PreparedCommit`]. Save its [`CommitReceipt`]
//! before submitting, including before spawning cancellable work.
//!
//! Snapshots own their values and remain stable across commits and checkpoints.
//! Recovery follows only committed, hash-verified ancestry. Checkpoints reduce
//! replay without discarding WAL history or outcome evidence. There is no GC,
//! restore, ownership-transfer, or cross-domain transaction API. Never externally
//! overwrite/delete the head or immutable objects, apply bucket expiration, or
//! reuse the exclusive namespace. These restrictions are part of the protocol.
//!
//! Persisted [`Limits`] bound live state, per-operation replay, and total committed
//! WAL commit count. The history limit eventually stops writes; checkpoints do not
//! reset it. Abandoned uploads and superseded checkpoint pages/manifests are
//! retained and are not bounded by committed-state
//! limits. Snapshot creation materializes the domain; callers must bound concurrent
//! snapshots/operations. This internal library is not a production retention or
//! distributed ownership service. Durability and atomic CAS require a qualified
//! backend; in-memory/local test adapters do not certify cloud or power-loss safety.

mod format;

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path;
use serde::de::DeserializeOwned;
use snafu::{OptionExt, ResultExt, Snafu, ensure};

use crate::store::{ExpectedRevision, Revision, StateChange, StateStore, WriteId, WriteOutcome};
use format::{
    Checkpoint, CheckpointReference, FORMAT, Head, LogRecord, MAX_BATCH_BYTES, MAX_BATCH_KEYS,
    Page, Reference, cpu, decode, encode, state_size, validate_changes, validate_key,
};
pub use format::{CommitReceipt, Limits};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("State storage operation failed: {source}"))]
    Storage { source: crate::store::Error },
    #[snafu(display("Failed to encode or decode transactional state: {source}"))]
    Codec { source: serde_json::Error },
    #[snafu(display("Invalid transactional state record: {reason}"))]
    InvalidRecord { reason: &'static str },
    #[snafu(display("Transactional state limit exceeded: {resource}"))]
    Limit { resource: &'static str },
    #[snafu(display("The transaction or snapshot belongs to another store handle"))]
    WrongStore,
    #[snafu(display("The receipt belongs to another domain or has an invalid format"))]
    InvalidReceipt,
    #[snafu(display("The domain's persisted limits differ from the requested limits"))]
    IncompatibleLimits,
    #[snafu(display(
        "Cannot confirm immutable state upload for '{key}'; retain the receipt before retrying"
    ))]
    UnconfirmedUpload { key: Path },
    #[snafu(display(
        "Cannot confirm transaction-domain initialization; retry opening the same namespace"
    ))]
    UnconfirmedInitialization,
    #[snafu(display("Transaction head publication had an uncertain outcome: {source}"))]
    Publication { source: object_store::Error },
    #[snafu(display("Transactional state worker stopped: {source}"))]
    Worker {
        source: tokio::sync::oneshot::error::RecvError,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

struct Domain {
    state: Arc<dyn StateStore>,
    prefix: Path,
    incarnation: [u8; 16],
    limits: Limits,
}

impl std::fmt::Debug for Domain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Domain")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct HeadVersion {
    head: Head,
    revision: Revision,
    write_id: [u8; 16],
}

/// An independently opened transaction domain. Clones share handle identity.
#[derive(Debug, Clone)]
pub struct WalStateStore {
    domain: Arc<Domain>,
}

/// An owned, immutable view of one committed transaction version.
#[derive(Debug, Clone)]
pub struct Snapshot {
    domain: Arc<Domain>,
    version: Arc<HeadVersion>,
    values: Arc<BTreeMap<String, Bytes>>,
}

impl Snapshot {
    #[must_use]
    pub fn sequence(&self) -> u64 {
        self.version.head.sequence
    }

    #[must_use]
    pub fn get(&self, key: &str) -> Option<&Bytes> {
        self.values.get(key)
    }

    /// Iterates the snapshot in key order, without copying values.
    pub fn scan_prefix<'a>(
        &'a self,
        prefix: &'a str,
    ) -> impl Iterator<Item = (&'a str, &'a Bytes)> {
        self.values
            .range(prefix.to_owned()..)
            .take_while(move |(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.as_str(), value))
    }

    #[must_use]
    pub fn transaction(&self) -> Transaction {
        Transaction {
            snapshot: self.clone(),
            changes: BTreeMap::new(),
            change_bytes: 0,
        }
    }
}

/// Reads one snapshot plus its own buffered writes. No I/O occurs before prepare.
#[derive(Debug)]
pub struct Transaction {
    snapshot: Snapshot,
    changes: BTreeMap<String, Option<Bytes>>,
    change_bytes: usize,
}

impl Transaction {
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&Bytes> {
        self.changes
            .get(key)
            .map_or_else(|| self.snapshot.get(key), Option::as_ref)
    }

    /// Reads a predicate against the snapshot and buffered writes.
    ///
    /// # Errors
    /// Returns an error if the CPU worker stops.
    pub async fn scan_prefix(&self, prefix: &str) -> Result<BTreeMap<String, Bytes>> {
        let snapshot = self.snapshot.clone();
        let changes = self.changes.clone();
        let prefix = prefix.to_owned();
        cpu(move || {
            let mut result: BTreeMap<_, _> = snapshot
                .scan_prefix(&prefix)
                .map(|(key, value)| (key.to_owned(), value.clone()))
                .collect();
            for (key, value) in changes {
                if key.starts_with(&prefix) {
                    match value {
                        Some(value) => {
                            result.insert(key, value);
                        }
                        None => {
                            result.remove(&key);
                        }
                    }
                }
            }
            Ok(result)
        })
        .await
    }

    /// Buffers a value, including an empty byte string.
    ///
    /// # Errors
    /// Rejects empty/oversized keys or a batch exceeding its byte/key bounds.
    pub fn put(&mut self, key: impl Into<String>, value: Bytes) -> Result<()> {
        self.change(key.into(), Some(value))
    }

    /// Buffers a tombstone. Deleting an absent key is allowed.
    ///
    /// # Errors
    /// Rejects empty/oversized keys or a batch exceeding its byte/key bounds.
    pub fn delete(&mut self, key: impl Into<String>) -> Result<()> {
        self.change(key.into(), None)
    }

    fn change(&mut self, key: String, value: Option<Bytes>) -> Result<()> {
        validate_key(&key)?;
        let previous = self.changes.get(&key);
        let previous_size = previous.map_or(0, |v| key.len() + v.as_ref().map_or(0, Bytes::len));
        let size = (self.change_bytes - previous_size)
            .saturating_add(key.len())
            .saturating_add(value.as_ref().map_or(0, Bytes::len));
        ensure!(
            size <= MAX_BATCH_BYTES,
            LimitSnafu {
                resource: "batch bytes"
            }
        );
        ensure!(
            previous.is_some() || self.changes.len() < MAX_BATCH_KEYS,
            LimitSnafu {
                resource: "batch key count"
            }
        );
        self.changes.insert(key, value);
        self.change_bytes = size;
        Ok(())
    }

    /// Freezes one immutable attempt. Persist its receipt before calling commit.
    ///
    /// # Errors
    /// Rejects an empty batch, exhausted history/replay/state bounds, or encoding
    /// errors. A full replay tail requires checkpointing and a fresh snapshot.
    pub async fn prepare(self) -> Result<PreparedCommit> {
        cpu(move || {
            validate_changes(&self.changes)?;
            let base = &self.snapshot.version.head;
            let sequence = base.sequence.checked_add(1).context(LimitSnafu {
                resource: "sequence",
            })?;
            ensure!(
                sequence <= base.limits.max_commits,
                LimitSnafu {
                    resource: "retained commit history"
                }
            );
            let watermark = base.checkpoint.as_ref().map_or(0, |c| c.sequence);
            ensure!(
                sequence - watermark <= base.limits.max_replay_commits,
                LimitSnafu {
                    resource: "WAL tail; checkpoint required"
                }
            );
            let mut state_bytes = base.state_bytes;
            let mut keys = base.keys;
            for (key, value) in &self.changes {
                if let Some(old) = self.snapshot.get(key) {
                    state_bytes -= key.len() + old.len();
                    keys -= 1;
                }
                if let Some(value) = value {
                    state_bytes = state_bytes
                        .saturating_add(key.len())
                        .saturating_add(value.len());
                    keys += 1;
                }
            }
            ensure!(
                state_bytes <= base.limits.max_state_bytes,
                LimitSnafu {
                    resource: "state bytes"
                }
            );
            ensure!(
                keys <= base.limits.max_keys,
                LimitSnafu {
                    resource: "state key count"
                }
            );
            let id = *WriteId::new().as_bytes();
            let record = LogRecord {
                format: FORMAT,
                incarnation: base.incarnation,
                id,
                base_head: self.snapshot.version.write_id,
                sequence,
                parent: base.commit.clone(),
                changes: self.changes,
                state_bytes,
                keys,
            };
            let bytes = encode(&record)?;
            let reference = Reference::new(id, &bytes);
            let receipt = CommitReceipt {
                format: FORMAT,
                prefix: self.snapshot.domain.prefix.to_string(),
                incarnation: base.incarnation,
                base_head: record.base_head,
                sequence,
                record: reference.clone(),
            };
            let mut head = base.clone();
            head.sequence = sequence;
            head.commit = Some(reference);
            head.state_bytes = state_bytes;
            head.keys = keys;
            Ok(PreparedCommit {
                snapshot: self.snapshot,
                receipt,
                bytes,
                head_bytes: encode(&head)?,
            })
        })
        .await
    }
}

/// A frozen attempt that may be retried only against its original head revision.
#[derive(Debug)]
pub struct PreparedCommit {
    snapshot: Snapshot,
    receipt: CommitReceipt,
    bytes: Bytes,
    head_bytes: Bytes,
}

impl PreparedCommit {
    #[must_use]
    pub fn receipt(&self) -> &CommitReceipt {
        &self.receipt
    }
}

/// Resolution from authoritative, retained history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resolution {
    Committed {
        sequence: u64,
    },
    /// The attempt is absent and its original head revision is permanently fenced.
    Rejected,
    /// The original head still exists; a delayed publication may still apply.
    Pending,
}

#[derive(Debug)]
#[must_use]
pub enum CommitOutcome {
    Committed {
        sequence: u64,
    },
    /// A new transaction evaluation is required; this attempt cannot publish.
    Conflict,
    /// Preserve the receipt and resolve before retrying the logical transaction.
    Unknown {
        source: Option<Error>,
    },
}

impl WalStateStore {
    /// Opens or conditionally initializes an exclusive namespace.
    ///
    /// All clients must request the same persisted limits. Namespace reuse,
    /// external deletion/overwrite and mixed formats are unsupported.
    ///
    /// # Errors
    /// Fails on invalid state, incompatible limits or unconfirmed initialization.
    pub async fn open(state: Arc<dyn StateStore>, prefix: Path, limits: Limits) -> Result<Self> {
        limits.validate()?;
        ensure!(
            !prefix.as_ref().is_empty(),
            InvalidRecordSnafu {
                reason: "empty domain prefix"
            }
        );
        let key = prefix.clone().join("head");
        let mut record = state.read(&key).await.context(StorageSnafu)?;
        if record.is_none() {
            let head = Head {
                format: FORMAT,
                incarnation: *WriteId::new().as_bytes(),
                limits,
                sequence: 0,
                commit: None,
                checkpoint: None,
                state_bytes: 0,
                keys: 0,
            };
            let bytes = encode(&head)?;
            let _outcome = state
                .compare_exchange(
                    &key,
                    ExpectedRevision::Absent,
                    StateChange::Set(bytes),
                    WriteId::new(),
                )
                .await
                .context(StorageSnafu)?;
            record = state.read(&key).await.context(StorageSnafu)?;
        }
        let record = record.context(UnconfirmedInitializationSnafu)?;
        let head: Head = decode(record.value.context(InvalidRecordSnafu {
            reason: "head tombstone",
        })?)
        .await?;
        head.validate()?;
        ensure!(head.limits == limits, IncompatibleLimitsSnafu);
        Ok(Self {
            domain: Arc::new(Domain {
                state,
                prefix,
                incarnation: head.incarnation,
                limits,
            }),
        })
    }

    /// Recovers a fresh committed version. Existing snapshots remain unchanged.
    ///
    /// # Errors
    /// Missing, corrupt, incompatible or out-of-bounds history fails closed.
    pub async fn snapshot(&self) -> Result<Snapshot> {
        let version = self.head().await?;
        let values = self.recover(&version.head).await?;
        Ok(Snapshot {
            domain: Arc::clone(&self.domain),
            version: Arc::new(version),
            values: Arc::new(values),
        })
    }

    /// Begins a transaction on a fresh snapshot.
    ///
    /// # Errors
    /// Returns the same recovery errors as [`Self::snapshot`].
    pub async fn begin(&self) -> Result<Transaction> {
        Ok(self.snapshot().await?.transaction())
    }

    /// Stages the WAL, then publishes its head using the original snapshot CAS.
    ///
    /// # Errors
    /// Returns an error for wrong-handle use or a staging/pre-submission failure.
    /// An error/cancellation does not undo an earlier submission of this attempt:
    /// always keep its receipt. Post-submission uncertainty is an explicit outcome.
    pub async fn commit(&self, attempt: &PreparedCommit) -> Result<CommitOutcome> {
        self.check_snapshot(&attempt.snapshot)?;
        self.stage("wal", &attempt.receipt.record, attempt.bytes.clone())
            .await?;
        let outcome = self
            .domain
            .state
            .compare_exchange(
                &self.domain.prefix.clone().join("head"),
                ExpectedRevision::Exact(&attempt.snapshot.version.revision),
                StateChange::Set(attempt.head_bytes.clone()),
                WriteId::from_bytes(attempt.receipt.record.id),
            )
            .await
            .context(StorageSnafu)?;
        let cause = match outcome {
            WriteOutcome::Applied => {
                return Ok(CommitOutcome::Committed {
                    sequence: attempt.receipt.sequence,
                });
            }
            WriteOutcome::Conflict => None,
            WriteOutcome::Unknown { source } => Some(Error::Publication { source }),
        };
        Ok(match self.resolve(&attempt.receipt).await {
            Ok(Resolution::Committed { sequence }) => CommitOutcome::Committed { sequence },
            Ok(Resolution::Rejected) => CommitOutcome::Conflict,
            Ok(Resolution::Pending) => CommitOutcome::Unknown { source: cause },
            Err(source) => CommitOutcome::Unknown {
                source: Some(source),
            },
        })
    }

    /// Resolves a saved receipt, including after reopening with an independent client.
    ///
    /// The complete WAL lineage is retained through checkpoints. A checkpoint-only
    /// head change also fences an old attempt, even with no new logical commit.
    ///
    /// # Errors
    /// Rejects foreign/invalid receipts, unavailable history and corrupt records.
    pub async fn resolve(&self, receipt: &CommitReceipt) -> Result<Resolution> {
        ensure!(
            receipt.format == FORMAT
                && receipt.prefix == self.domain.prefix.as_ref()
                && receipt.incarnation == self.domain.incarnation
                && receipt.sequence > 0
                && receipt.sequence <= self.domain.limits.max_commits,
            InvalidReceiptSnafu
        );
        let version = self.head().await?;
        ensure!(
            receipt.sequence - 1 <= version.head.sequence,
            InvalidReceiptSnafu
        );
        let mut sequence = version.head.sequence;
        let mut cursor = version.head.commit.clone();
        while sequence >= receipt.sequence {
            let reference = cursor.context(InvalidRecordSnafu {
                reason: "missing WAL ancestry",
            })?;
            let log = self.log(&version.head, &reference, sequence).await?;
            if sequence == receipt.sequence {
                if reference == receipt.record {
                    ensure!(log.base_head == receipt.base_head, InvalidReceiptSnafu);
                    return Ok(Resolution::Committed { sequence });
                }
                return Ok(Resolution::Rejected);
            }
            cursor = log.parent;
            sequence -= 1;
        }
        Ok(if version.write_id == receipt.base_head {
            Resolution::Pending
        } else {
            Resolution::Rejected
        })
    }

    /// Writes immutable checkpoint pages and conditionally publishes their reference.
    ///
    /// Concurrent commits/checkpoints invalidate this snapshot's head revision.
    /// There is no automatic rebase and no WAL deletion. A conflict or unknown
    /// outcome has the conservative semantics of [`WriteOutcome`]; reopen a fresh
    /// snapshot before attempting another checkpoint.
    ///
    /// # Errors
    /// Fails on wrong-handle snapshots, staging failures, or encoding errors.
    pub async fn checkpoint(&self, snapshot: &Snapshot) -> Result<WriteOutcome> {
        self.check_snapshot(snapshot)?;
        let values = Arc::clone(&snapshot.values);
        let incarnation = self.domain.incarnation;
        // Build only page boundaries here. Each encoded page is bounded and staged
        // individually rather than retaining a second serialized copy of the domain.
        let pages = cpu(move || {
            let mut pages = Vec::new();
            let mut page = BTreeMap::new();
            let mut size = 0;
            for (key, value) in values.iter() {
                let entry_size = key.len() + value.len();
                if !page.is_empty()
                    && (size + entry_size > MAX_BATCH_BYTES || page.len() == MAX_BATCH_KEYS)
                {
                    pages.push(std::mem::take(&mut page));
                    size = 0;
                }
                size += entry_size;
                page.insert(key.clone(), value.clone());
            }
            if !page.is_empty() {
                pages.push(page);
            }
            Ok(pages)
        })
        .await?;
        let mut references = Vec::with_capacity(pages.len());
        for entries in pages {
            let (reference, bytes) = cpu(move || {
                let bytes = encode(&Page {
                    format: FORMAT,
                    incarnation,
                    entries,
                })?;
                Ok((Reference::new(*WriteId::new().as_bytes(), &bytes), bytes))
            })
            .await?;
            self.stage("pages", &reference, bytes).await?;
            references.push(reference);
        }
        let head = snapshot.version.head.clone();
        let (reference, bytes, head_bytes) = cpu(move || {
            let checkpoint = Checkpoint {
                format: FORMAT,
                incarnation,
                sequence: head.sequence,
                commit: head.commit.clone(),
                pages: references,
                state_bytes: head.state_bytes,
                keys: head.keys,
            };
            let bytes = encode(&checkpoint)?;
            let reference = Reference::new(*WriteId::new().as_bytes(), &bytes);
            let mut next = head;
            next.checkpoint = Some(CheckpointReference {
                object: reference.clone(),
                sequence: checkpoint.sequence,
                commit: checkpoint.commit,
            });
            Ok((reference, bytes, encode(&next)?))
        })
        .await?;
        self.stage("checkpoints", &reference, bytes).await?;
        self.domain
            .state
            .compare_exchange(
                &self.domain.prefix.clone().join("head"),
                ExpectedRevision::Exact(&snapshot.version.revision),
                StateChange::Set(head_bytes),
                WriteId::new(),
            )
            .await
            .context(StorageSnafu)
    }

    fn check_snapshot(&self, snapshot: &Snapshot) -> Result<()> {
        ensure!(Arc::ptr_eq(&self.domain, &snapshot.domain), WrongStoreSnafu);
        Ok(())
    }

    async fn head(&self) -> Result<HeadVersion> {
        let record = self
            .domain
            .state
            .read(&self.domain.prefix.clone().join("head"))
            .await
            .context(StorageSnafu)?
            .context(InvalidRecordSnafu {
                reason: "missing head",
            })?;
        let head: Head = decode(record.value.context(InvalidRecordSnafu {
            reason: "head tombstone",
        })?)
        .await?;
        head.validate()?;
        ensure!(
            head.incarnation == self.domain.incarnation && head.limits == self.domain.limits,
            InvalidRecordSnafu {
                reason: "domain identity changed"
            }
        );
        Ok(HeadVersion {
            head,
            revision: record.revision,
            write_id: *record.write_id.as_bytes(),
        })
    }

    fn object_key(&self, kind: &str, reference: &Reference) -> Path {
        self.domain
            .prefix
            .clone()
            .join(kind)
            .join(uuid::Uuid::from_bytes(reference.id).to_string())
    }

    async fn stage(&self, kind: &str, reference: &Reference, bytes: Bytes) -> Result<()> {
        let key = self.object_key(kind, reference);
        let outcome = self
            .domain
            .state
            .compare_exchange(
                &key,
                ExpectedRevision::Absent,
                StateChange::Set(bytes.clone()),
                WriteId::new(),
            )
            .await
            .context(StorageSnafu)?;
        if matches!(outcome, WriteOutcome::Applied) {
            return Ok(());
        }
        let stored = self.domain.state.read(&key).await.context(StorageSnafu)?;
        let value = stored
            .and_then(|r| r.value)
            .context(UnconfirmedUploadSnafu { key })?;
        cpu(move || {
            ensure!(
                value == bytes,
                InvalidRecordSnafu {
                    reason: "immutable object collision"
                }
            );
            Ok(())
        })
        .await
    }

    async fn immutable<T: DeserializeOwned + Send + 'static>(
        &self,
        kind: &str,
        reference: &Reference,
    ) -> Result<T> {
        let key = self.object_key(kind, reference);
        let record = self
            .domain
            .state
            .read(&key)
            .await
            .context(StorageSnafu)?
            .context(InvalidRecordSnafu {
                reason: "missing immutable object",
            })?;
        let bytes = record.value.context(InvalidRecordSnafu {
            reason: "immutable object tombstone",
        })?;
        let digest = reference.digest;
        cpu(move || {
            ensure!(
                *blake3::hash(&bytes).as_bytes() == digest,
                InvalidRecordSnafu {
                    reason: "content digest mismatch"
                }
            );
            serde_json::from_slice(&bytes).context(CodecSnafu)
        })
        .await
    }

    async fn log(&self, head: &Head, reference: &Reference, sequence: u64) -> Result<LogRecord> {
        let log: LogRecord = self.immutable("wal", reference).await?;
        log.validate(head, reference, sequence)?;
        Ok(log)
    }

    async fn recover(&self, head: &Head) -> Result<BTreeMap<String, Bytes>> {
        let mut values = BTreeMap::new();
        let mut watermark = 0;
        let mut ancestor = None;
        if let Some(reference) = &head.checkpoint {
            let checkpoint: Checkpoint = self.immutable("checkpoints", &reference.object).await?;
            ensure!(
                checkpoint.format == FORMAT
                    && checkpoint.incarnation == head.incarnation
                    && checkpoint.sequence == reference.sequence
                    && checkpoint.commit == reference.commit
                    && checkpoint.pages.len() <= head.limits.max_keys
                    && checkpoint.state_bytes <= head.limits.max_state_bytes
                    && checkpoint.keys <= head.limits.max_keys,
                InvalidRecordSnafu {
                    reason: "checkpoint identity or bounds"
                }
            );
            watermark = checkpoint.sequence;
            ancestor = checkpoint.commit;
            for page_ref in checkpoint.pages {
                let page: Page = self.immutable("pages", &page_ref).await?;
                ensure!(
                    page.format == FORMAT
                        && page.incarnation == head.incarnation
                        && !page.entries.is_empty()
                        && page.entries.len() <= MAX_BATCH_KEYS,
                    InvalidRecordSnafu {
                        reason: "checkpoint page"
                    }
                );
                let limits = head.limits;
                values = cpu(move || {
                    if let (Some((last, _)), Some((first, _))) =
                        (values.last_key_value(), page.entries.first_key_value())
                    {
                        ensure!(
                            last < first,
                            InvalidRecordSnafu {
                                reason: "checkpoint key order"
                            }
                        );
                    }
                    let page_size = state_size(&page.entries, limits)?;
                    ensure!(
                        page_size <= MAX_BATCH_BYTES,
                        InvalidRecordSnafu {
                            reason: "checkpoint page size"
                        }
                    );
                    values.extend(page.entries);
                    state_size(&values, limits)?;
                    Ok(values)
                })
                .await?;
            }
            let limits = head.limits;
            values = cpu(move || {
                ensure!(
                    state_size(&values, limits)? == checkpoint.state_bytes
                        && values.len() == checkpoint.keys,
                    InvalidRecordSnafu {
                        reason: "checkpoint totals"
                    }
                );
                Ok(values)
            })
            .await?;
        }
        let mut cursor = head.commit.clone();
        let mut sequence = head.sequence;
        let mut logs = Vec::new();
        while sequence > watermark {
            let reference = cursor.as_ref().context(InvalidRecordSnafu {
                reason: "missing WAL ancestry",
            })?;
            let log = self.log(head, reference, sequence).await?;
            cursor.clone_from(&log.parent);
            logs.push(log);
            sequence -= 1;
        }
        ensure!(
            cursor == ancestor,
            InvalidRecordSnafu {
                reason: "checkpoint is not a WAL ancestor"
            }
        );
        let head = head.clone();
        cpu(move || {
            for log in logs.into_iter().rev() {
                for (key, value) in log.changes {
                    match value {
                        Some(value) => {
                            values.insert(key, value);
                        }
                        None => {
                            values.remove(&key);
                        }
                    }
                }
                ensure!(
                    state_size(&values, head.limits)? == log.state_bytes
                        && values.len() == log.keys,
                    InvalidRecordSnafu {
                        reason: "WAL state totals"
                    }
                );
            }
            ensure!(
                state_size(&values, head.limits)? == head.state_bytes && values.len() == head.keys,
                InvalidRecordSnafu {
                    reason: "head state totals"
                }
            );
            Ok(values)
        })
        .await
    }
}
