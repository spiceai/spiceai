// Copyright 2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Prototype inline head log. Acceptance is exclusively the head CAS.
//!
//! Persist [`PreparedAttempt`] before calling [`Writer::commit`]. After an error,
//! retain it and call [`Writer::resolve`]; never replace an undecided logical write.
//! This prototype retains all immutable history. It deliberately implements no GC:
//! opaque actions do not provide enough information to establish reachability.

mod format;
mod reader;

use std::sync::Arc;

use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt, UpdateVersion, path::Path};
use prost::Message;
use serde::{Deserialize, Serialize};
use snafu::Snafu;

use crate::{Expected, conditional_put, probe_conditional_writes};
use format::{Batch, Checkpoint, decode_record};
pub use format::{Group, Head, HeadLogConfig, Record, Spilled, WindowRecord};
pub use reader::{DEFAULT_READ_CONCURRENCY, Opened, Poll, Reader, RecordWithCommit};

/// A protocol, transport, or encoding error. An error never implies rejection.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid head log: {reason}"))]
    Invalid { reason: String },
    #[snafu(display("Head log storage error: {source}"))]
    Storage { source: object_store::Error },
    #[snafu(display("Head log conditional write error: {source}"))]
    Conditional {
        source: crate::ConditionalWriteError,
    },
    #[snafu(display("Head log decoding error: {source}"))]
    Decode { source: prost::DecodeError },
    #[snafu(display(
        "Identical CAS retry budget exhausted; retain the prepared attempt and resolve again"
    ))]
    RetryExhausted,
}

impl From<object_store::Error> for Error {
    fn from(source: object_store::Error) -> Self {
        Self::Storage { source }
    }
}
impl From<prost::DecodeError> for Error {
    fn from(source: prost::DecodeError) -> Self {
        Self::Decode { source }
    }
}
impl From<crate::ConditionalWriteError> for Error {
    fn from(source: crate::ConditionalWriteError) -> Self {
        Self::Conditional { source }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// A persisted exact CAS, bound to one log incarnation and base version.
/// Fields are private so callers cannot accidentally change the retry bytes.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedAttempt {
    prefix: String,
    incarnation: Vec<u8>,
    base_commit: u64,
    generation: u64,
    attempt_id: Vec<u8>,
    e_tag: Option<String>,
    version: Option<String>,
    head_bytes: Bytes,
}

impl PreparedAttempt {
    #[must_use]
    pub fn attempt_id(&self) -> &[u8] {
        &self.attempt_id
    }

    fn expected(&self) -> Expected {
        Expected::Version(UpdateVersion {
            e_tag: self.e_tag.clone(),
            version: self.version.clone(),
        })
    }
}

/// The outcome of one exact attempt. Only `Rejected` permits a new logical attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Accepted { commit: u64 },
    Rejected,
    Unresolved,
    Fenced { head_lease: u64 },
}

/// A decoded head together with the exact conditional-write version.
#[derive(Clone, Debug)]
pub struct HeadState {
    pub head: Head,
    pub version: UpdateVersion,
}

/// Creates an empty log; does not take writer ownership.
pub struct HeadLog;

impl HeadLog {
    /// # Errors
    /// Returns an error for invalid budgets, an existing head, or storage failure.
    pub async fn create(
        store: Arc<dyn ObjectStore>,
        prefix: Path,
        config: HeadLogConfig,
    ) -> Result<Head> {
        config.validate()?;
        ensure_store(store.as_ref(), &prefix).await?;
        let head = Head {
            format: 1,
            incarnation: uuid::Uuid::new_v4().as_bytes().to_vec(),
            ..Head::default()
        };
        let bytes = head.encode_to_vec();
        check_budget(&bytes, config)?;
        conditional_put(
            store.as_ref(),
            &prefix.clone().join("head"),
            bytes.into(),
            &Expected::Absent,
        )
        .await?;
        Ok(head)
    }
}

/// One serial writer. `&mut self` serializes prepare/commit/checkpoint operations.
/// Lease arbitration is external; the head generation supplies fencing.
pub struct Writer {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    config: HeadLogConfig,
    generation: u64,
    state: HeadState,
    pending: Option<Vec<u8>>,
    checkpoint: Option<u64>,
    sealing_error: Option<Error>,
    /// The newest batch this writer has created or confirmed, as its first
    /// commit number. Its records can be dropped without another request.
    sealed: Option<u64>,
    /// At most one batch copy runs beside the writer, off the acknowledgement.
    sealing: Option<(u64, tokio::task::JoinHandle<Result<()>>)>,
}

impl Writer {
    /// Acquire the head with a strictly larger generation.
    /// # Errors
    /// Returns an error on a concurrent takeover, invalid generation, or storage failure.
    pub async fn take_over(
        store: Arc<dyn ObjectStore>,
        prefix: Path,
        config: HeadLogConfig,
        new_generation: u64,
    ) -> Result<Self> {
        config.validate()?;
        ensure_store(store.as_ref(), &prefix).await?;
        let mut state = read_head(store.as_ref(), &prefix).await?;
        if state.head.lease >= new_generation {
            return invalid("takeover generation must exceed head lease");
        }
        state.head.lease = new_generation;
        let bytes = state.head.encode_to_vec();
        check_budget(&bytes, config)?;
        let result = conditional_put(
            store.as_ref(),
            &prefix.clone().join("head"),
            bytes.into(),
            &Expected::Version(state.version),
        )
        .await;
        // A lost takeover response is not guessed from the generation: another
        // candidate might have used it. The caller must arbitrate and resume.
        let version = UpdateVersion::from(result?);
        ensure_version(&version)?;
        Ok(Self {
            store,
            prefix,
            config,
            generation: new_generation,
            state: HeadState {
                head: state.head,
                version,
            },
            pending: None,
            checkpoint: None,
            sealing_error: None,
            sealed: None,
            sealing: None,
        })
    }

    /// Recover an externally arbitrated generation after restart. If a newer
    /// owner exists, this handle permits historical resolution but cannot prepare
    /// or upload. Resolve persisted attempts before preparing another write.
    /// # Errors
    /// Returns an error for a generation never acquired, or a failed read.
    pub async fn resume(
        store: Arc<dyn ObjectStore>,
        prefix: Path,
        config: HeadLogConfig,
        generation: u64,
    ) -> Result<Self> {
        config.validate()?;
        ensure_store(store.as_ref(), &prefix).await?;
        let state = read_head(store.as_ref(), &prefix).await?;
        if generation == 0 || state.head.lease < generation {
            return invalid("generation has not acquired the head");
        }
        Ok(Self {
            store,
            prefix,
            config,
            generation,
            state,
            pending: None,
            checkpoint: None,
            sealing_error: None,
            sealed: None,
            sealing: None,
        })
    }

    #[must_use]
    pub fn state(&self) -> &HeadState {
        &self.state
    }

    /// A failure of background sealing after acceptance, observed when the copy
    /// is collected. Commit still returns Accepted; a later prepare retries the
    /// immutable copy before dropping.
    #[must_use]
    pub fn sealing_error(&self) -> Option<&Error> {
        self.sealing_error.as_ref()
    }

    /// Wait for the background batch copy, if any, and record its outcome.
    pub async fn wait_for_sealing(&mut self) {
        if let Some((start, task)) = self.sealing.take() {
            let result = match task.await {
                Ok(result) => result,
                Err(error) => invalid(&format!("batch copy task failed: {error}")),
            };
            self.record_sealing(start, result);
        }
    }

    fn record_sealing(&mut self, start: u64, result: Result<()>) {
        match result {
            Ok(()) => {
                self.sealed = Some(start);
                self.sealing_error = None;
            }
            Err(error) => self.sealing_error = Some(error),
        }
    }

    /// Start copying the oldest final batch beside the writer, unless it is
    /// already copied or a copy is running. A finished copy is collected first.
    fn start_sealing(&mut self) {
        if let Some((start, task)) = self.sealing.take_if(|(_, task)| task.is_finished()) {
            let result = match futures::FutureExt::now_or_never(task) {
                Some(Ok(result)) => result,
                Some(Err(error)) => invalid(&format!("batch copy task failed: {error}")),
                None => invalid("finished batch copy did not complete"),
            };
            self.record_sealing(start, result);
        }
        if self.sealing.is_some() {
            return;
        }
        let Ok(Some((start, _, bytes))) = self.plan_seal() else {
            return;
        };
        if self.sealed == Some(start) {
            return;
        }
        let store = Arc::clone(&self.store);
        let path = numbered(&self.prefix, "wal", start);
        self.sealing = Some((
            start,
            tokio::spawn(async move { immutable(store.as_ref(), &path, bytes).await }),
        ));
    }

    /// Construct and return the EXACT CAS to persist before sending. Required
    /// immutable copies are made from the writer's observed, published head.
    /// # Errors
    /// Returns an error if another attempt is pending, a bound is exceeded, or copying fails.
    pub async fn prepare(&mut self, payload: Bytes) -> Result<PreparedAttempt> {
        if self.state.head.lease != self.generation {
            return invalid("writer is fenced");
        }
        if self.pending.is_some() {
            return invalid("resolve the pending attempt before preparing another");
        }
        let attempt_id = uuid::Uuid::new_v4().as_bytes().to_vec();
        let mut record = Record {
            attempt_id: attempt_id.clone(),
            generation: self.generation,
            payload,
            spilled: None,
        };
        let mut encoded = record.encode_to_vec();
        // A large record would let a few records fill a batch; its payload
        // goes to an immutable object before the CAS, and the window keeps
        // only the reference.
        if encoded.len() > self.config.spill_above_bytes
            && self.config.spill_above_bytes < self.config.max_record_bytes
        {
            let digest = blake3::hash(&record.payload);
            let spilled = Spilled {
                digest: digest.as_bytes().to_vec(),
                len: record.payload.len() as u64,
            };
            immutable(
                self.store.as_ref(),
                &spilled_path(&self.prefix, self.generation, &spilled.digest)?,
                std::mem::take(&mut record.payload),
            )
            .await?;
            record.spilled = Some(spilled);
            encoded = record.encode_to_vec();
        }
        if encoded.len() > self.config.max_record_bytes {
            return invalid("encoded record exceeds max_record_bytes");
        }
        let observed_start = self.state.head.window_start();
        let mut head = self.state.head.clone();
        if let Some(floor) = self.checkpoint {
            head.floor = floor;
            head.batches.retain(|end| *end > floor);
        }
        head.commit = head.commit.checked_add(1).ok_or_else(|| Error::Invalid {
            reason: "commit overflow".into(),
        })?;
        head.window.push(WindowRecord { encoded });
        if head.window.len() > self.config.batch_max_records
            || head.encoded_len() > self.config.head_budget_bytes
        {
            let count = self.seal_oldest().await?.ok_or_else(|| Error::Invalid {
                reason:
                    "head budget exhausted without a final batch; checkpoint or increase budgets"
                        .into(),
            })?;
            let end = observed_start + count as u64 - 1;
            head.window.drain(..count);
            head.batches.push(end);
            if head.batches.len() > self.config.max_listed_batches {
                return invalid("batch index is full; publish a checkpoint before committing");
            }
        }
        head.validate()?;
        let head_bytes = Bytes::from(head.encode_to_vec());
        check_budget(&head_bytes, self.config)?;
        let observed = &self.state.head;
        let attempt = PreparedAttempt {
            prefix: self.prefix.to_string(),
            incarnation: observed.incarnation.clone(),
            base_commit: observed.commit,
            generation: self.generation,
            attempt_id,
            e_tag: self.state.version.e_tag.clone(),
            version: self.state.version.version.clone(),
            head_bytes,
        };
        self.pending = Some(attempt.attempt_id.clone());
        Ok(attempt)
    }

    /// Fold a queue into one opaque protobuf repeated-bytes payload. The returned
    /// receipt must be persisted before calling commit, just like prepare.
    /// # Errors
    /// Returns the same errors as [`Self::prepare`].
    pub async fn prepare_group(&mut self, payloads: Vec<Bytes>) -> Result<PreparedAttempt> {
        self.prepare(Group { payloads }.encode_to_vec().into())
            .await
    }

    /// Fold a queue into one record and invoke the caller's durable receipt
    /// persistence callback BEFORE sending. The callback must sync its receipt.
    /// # Errors
    /// Returns preparation, persistence, transport, or undecided-retry errors.
    pub async fn commit_group(
        &mut self,
        payloads: Vec<Bytes>,
        persist: impl FnOnce(&PreparedAttempt) -> Result<()>,
    ) -> Result<Outcome> {
        let attempt = self.prepare_group(payloads).await?;
        persist(&attempt)?;
        self.commit(&attempt).await
    }

    /// Send the original bytes and condition. All failures, including 412, are
    /// resolved before retrying. Success advances memory without re-reading the head.
    /// # Errors
    /// On error retain the receipt: acceptance may be unknown.
    pub async fn commit(&mut self, attempt: &PreparedAttempt) -> Result<Outcome> {
        self.validate_attempt(attempt)?;
        // Recovered receipts are pending before any I/O, including a head read
        // that may fail. An undecided logical write cannot be replaced.
        self.pending = Some(attempt.attempt_id.clone());
        for _ in 0..3 {
            if let Ok(result) = conditional_put(
                self.store.as_ref(),
                &self.prefix.clone().join("head"),
                attempt.head_bytes.clone().into(),
                &attempt.expected(),
            )
            .await
            {
                let head = Head::decode(attempt.head_bytes.clone())?;
                let version = UpdateVersion::from(result);
                ensure_version(&version)?;
                self.state = HeadState { head, version };
                self.pending = None;
                self.checkpoint = None;
                // Acceptance cannot be undone by a maintenance failure. The
                // copy runs beside the writer; the head drops records only
                // after their batch exists.
                self.start_sealing();
                return Ok(Outcome::Accepted {
                    commit: attempt.base_commit + 1,
                });
            }
            let state = read_head(self.store.as_ref(), &self.prefix).await?;
            if let Some(outcome) = self.classify(attempt, state).await? {
                return Ok(outcome);
            }
        }
        Err(Error::RetryExhausted)
    }

    /// Resolve from retained history; if the base version still exists, retry
    /// ONLY the saved bytes with the saved condition.
    /// # Errors
    /// An unreadable head or missing referenced object returns an error; no CAS is sent.
    pub async fn resolve(&mut self, attempt: &PreparedAttempt) -> Result<Outcome> {
        self.validate_attempt(attempt)?;
        // Recovered receipts are pending before any I/O, including a head read
        // that may fail. An undecided logical write cannot be replaced.
        self.pending = Some(attempt.attempt_id.clone());
        let state = read_head(self.store.as_ref(), &self.prefix).await?;
        match self.classify(attempt, state).await? {
            Some(outcome) => Ok(outcome),
            None => self.commit(attempt).await,
        }
    }

    fn validate_attempt(&self, attempt: &PreparedAttempt) -> Result<()> {
        if self
            .pending
            .as_ref()
            .is_some_and(|id| id != &attempt.attempt_id)
        {
            return invalid("another attempt is still pending");
        }
        if attempt.prefix != self.prefix.as_ref()
            || attempt.incarnation != self.state.head.incarnation
            || attempt.generation != self.generation
        {
            return invalid("attempt belongs to another log or writer generation");
        }
        ensure_version(&UpdateVersion {
            e_tag: attempt.e_tag.clone(),
            version: attempt.version.clone(),
        })?;
        let head = Head::decode(attempt.head_bytes.clone())?;
        head.validate()?;
        let last = head.window.last().ok_or_else(|| Error::Invalid {
            reason: "prepared head has no record".into(),
        })?;
        let record = decode_record(&last.encoded)?;
        if head.incarnation != attempt.incarnation
            || head.commit.checked_sub(1) != Some(attempt.base_commit)
            || head.lease != attempt.generation
            || record.attempt_id != attempt.attempt_id
            || record.generation != attempt.generation
        {
            return invalid("inconsistent prepared receipt");
        }
        Ok(())
    }

    async fn classify(
        &mut self,
        attempt: &PreparedAttempt,
        state: HeadState,
    ) -> Result<Option<Outcome>> {
        let head = &state.head;
        if head.incarnation != attempt.incarnation {
            return invalid("log incarnation changed");
        }
        let target = attempt.base_commit + 1;
        let outcome = if target <= head.floor {
            let checkpoint = load_checkpoint(self.store.as_ref(), &self.prefix, head).await?;
            if checkpoint.attempts.contains(&attempt.attempt_id) {
                Some(Outcome::Accepted { commit: target })
            } else {
                Some(Outcome::Unresolved)
            }
        } else if head.commit >= target {
            let records = reader::records_since(
                self.store.as_ref(),
                &self.prefix,
                head,
                target - 1,
                reader::DEFAULT_READ_CONCURRENCY,
            )
            .await?;
            let record = records.first().ok_or_else(|| Error::Invalid {
                reason: "resolution record missing".into(),
            })?;
            Some(if record.record.attempt_id == attempt.attempt_id {
                Outcome::Accepted { commit: target }
            } else {
                Outcome::Rejected
            })
        } else if head.commit == attempt.base_commit
            && state.version
                == match attempt.expected() {
                    Expected::Version(v) => v,
                    Expected::Absent => unreachable!(),
                }
        {
            None
        } else {
            Some(Outcome::Rejected)
        };
        let outcome = match outcome {
            Some(Outcome::Accepted { commit }) => Some(Outcome::Accepted { commit }),
            _ if head.lease > self.generation => Some(Outcome::Fenced {
                head_lease: head.lease,
            }),
            other => other,
        };
        self.state = state;
        if matches!(
            outcome,
            Some(Outcome::Accepted { .. } | Outcome::Rejected | Outcome::Fenced { .. })
        ) {
            self.pending = None;
            self.checkpoint = None;
        }
        Ok(outcome)
    }

    /// Make sure the deterministic FINAL prefix of the observed head exists as
    /// a batch, with one create per batch. Returns its size.
    async fn seal_oldest(&mut self) -> Result<Option<usize>> {
        let Some((start, count, bytes)) = self.plan_seal()? else {
            return Ok(None);
        };
        if self
            .sealing
            .as_ref()
            .is_some_and(|(running, _)| *running == start)
        {
            self.wait_for_sealing().await;
        }
        if self.sealed != Some(start) {
            immutable(
                self.store.as_ref(),
                &numbered(&self.prefix, "wal", start),
                bytes,
            )
            .await?;
            self.sealed = Some(start);
            self.sealing_error = None;
        }
        Ok(Some(count))
    }

    /// The oldest final batch of the observed head: its first commit, record
    /// count and encoding. The prefix depends only on the oldest records.
    fn plan_seal(&self) -> Result<Option<(u64, usize, Bytes)>> {
        let head = &self.state.head;
        let mut size = 0usize;
        let mut count = 0;
        for entry in head.window.iter().take(self.config.batch_max_records) {
            if entry.encoded.len() > self.config.batch_budget_bytes {
                return invalid("observed record exceeds batch budget");
            }
            if entry.encoded.len() > self.config.batch_budget_bytes - size {
                break;
            }
            size += entry.encoded.len();
            count += 1;
        }
        if count == 0 || (count == head.window.len() && count < self.config.batch_max_records) {
            return Ok(None);
        }
        let batch = Batch {
            records: head.window[..count]
                .iter()
                .map(|entry| entry.encoded.clone())
                .collect(),
        };
        Ok(Some((
            head.window_start(),
            count,
            batch.encode_to_vec().into(),
        )))
    }

    /// Upload immutable caller data into this writer's generation. No garbage
    /// collection runs in this prototype, so pending uploads cannot be retired.
    /// # Errors
    /// Returns an error on an invalid name, different existing bytes, or storage failure.
    pub async fn upload(&mut self, name: &str, bytes: Bytes) -> Result<ObjectRef> {
        if self.state.head.lease != self.generation {
            return invalid("writer is fenced");
        }
        let name = Path::parse(name).map_err(|e| Error::Invalid {
            reason: e.to_string(),
        })?;
        if name.as_ref().is_empty() {
            return invalid("empty object name");
        }
        let path = Path::parse(format!(
            "{}/objects/{:020}/{}",
            self.prefix, self.generation, name
        ))
        .map_err(|e| Error::Invalid {
            reason: e.to_string(),
        })?;
        immutable(self.store.as_ref(), &path, bytes.clone()).await?;
        Ok(ObjectRef {
            path,
            digest: *blake3::hash(&bytes).as_bytes(),
        })
    }

    /// Stage a caller checkpoint and evidence; the NEXT commit publishes its floor.
    /// All accepted attempt ids up to `at` are retained without an expiry horizon.
    /// # Errors
    /// Returns an error for a non-boundary commit, a pending attempt, or storage failure.
    pub async fn publish_checkpoint(&mut self, at: u64, root: Bytes) -> Result<()> {
        if self.pending.is_some() {
            return invalid("cannot checkpoint with a pending attempt");
        }
        let head = &self.state.head;
        if at == 0
            || at <= head.floor
            || (at != head.window_start() - 1 && !head.batches.contains(&at))
        {
            return invalid("checkpoint must advance to a published batch boundary");
        }
        let mut attempts = if head.floor > 0 {
            load_checkpoint(self.store.as_ref(), &self.prefix, head)
                .await?
                .attempts
        } else {
            Vec::new()
        };
        attempts.extend(
            reader::records_since(
                self.store.as_ref(),
                &self.prefix,
                head,
                head.floor,
                reader::DEFAULT_READ_CONCURRENCY,
            )
            .await?
            .into_iter()
            .filter(|entry| entry.commit <= at)
            .map(|entry| entry.record.attempt_id),
        );
        let checkpoint = Checkpoint {
            root,
            attempts,
            incarnation: head.incarnation.clone(),
            commit: at,
        };
        immutable(
            self.store.as_ref(),
            &numbered(&self.prefix, "checkpoints", at),
            checkpoint.encode_to_vec().into(),
        )
        .await?;
        self.checkpoint = Some(at);
        Ok(())
    }
}

/// An immutable upload's location and content digest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectRef {
    pub path: Path,
    pub digest: [u8; 32],
}

fn invalid<T>(reason: &str) -> Result<T> {
    Err(Error::Invalid {
        reason: reason.into(),
    })
}
fn check_budget(bytes: &[u8], config: HeadLogConfig) -> Result<()> {
    if bytes.len() > config.head_budget_bytes {
        return invalid("encoded head exceeds head_budget_bytes; checkpoint required");
    }
    Ok(())
}
fn ensure_version(version: &UpdateVersion) -> Result<()> {
    if version.e_tag.is_none() {
        return invalid("head store must return an ETag for conditional polling and CAS");
    }
    Ok(())
}
fn spilled_path(prefix: &Path, generation: u64, digest: &[u8]) -> Result<Path> {
    let digest = <[u8; 32]>::try_from(digest).map_err(|_| Error::Invalid {
        reason: "spilled payload digest is not 32 bytes".into(),
    })?;
    Ok(prefix
        .clone()
        .join("objects")
        .join(format!("{generation:020}"))
        .join("records")
        .join(blake3::Hash::from_bytes(digest).to_hex().as_str()))
}
fn numbered(prefix: &Path, directory: &str, number: u64) -> Path {
    prefix.clone().join(directory).join(format!("{number:020}"))
}
async fn read_head(store: &dyn ObjectStore, prefix: &Path) -> Result<HeadState> {
    let result = store.get(&prefix.clone().join("head")).await?;
    decode_head(result).await
}
async fn decode_head(result: object_store::GetResult) -> Result<HeadState> {
    let version = UpdateVersion {
        e_tag: result.meta.e_tag.clone(),
        version: result.meta.version.clone(),
    };
    ensure_version(&version)?;
    let head = Head::decode(result.bytes().await?)?;
    head.validate()?;
    Ok(HeadState { head, version })
}
async fn immutable(store: &dyn ObjectStore, path: &Path, bytes: Bytes) -> Result<()> {
    match conditional_put(store, path, bytes.clone().into(), &Expected::Absent).await {
        Ok(_) => Ok(()),
        Err(error) => {
            // Even a transport error may mean the create landed.
            match store.get(path).await {
                Ok(result) => {
                    if result.bytes().await? == bytes {
                        Ok(())
                    } else {
                        invalid("immutable object already exists with different bytes")
                    }
                }
                Err(object_store::Error::NotFound { .. }) => Err(error.into()),
                Err(error) => Err(error.into()),
            }
        }
    }
}
async fn load_checkpoint(
    store: &dyn ObjectStore,
    prefix: &Path,
    head: &Head,
) -> Result<Checkpoint> {
    let bytes = store
        .get(&numbered(prefix, "checkpoints", head.floor))
        .await?
        .bytes()
        .await?;
    let checkpoint = Checkpoint::decode(bytes)?;
    if checkpoint.incarnation != head.incarnation || checkpoint.commit != head.floor {
        return invalid("checkpoint does not match head");
    }
    Ok(checkpoint)
}

async fn ensure_store(store: &dyn ObjectStore, prefix: &Path) -> Result<()> {
    let support = probe_conditional_writes(store, prefix).await;
    if !support.is_enforced() {
        return invalid(&format!(
            "conditional write probe refused store: {support:?}"
        ));
    }
    Ok(())
}
