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

//! Conditional storage for small authoritative state records.
//!
//! This internal protocol is independent of [`crate::ObjectState`]'s JSON format.
//! Callers must use an exclusive namespace; mixing formats is rejected on read.
//! The runtime does not select this protocol automatically.
//!
//! Atomicity is per key. Transactions, leases, durable operation history, reader
//! protection, and garbage collection belong above this layer. A backend must
//! provide fresh reads and atomic conditional writes; this adapter never emulates
//! those guarantees or falls back to unconditional writes. Its durability is the
//! backend's durability. In particular, local filesystem use does not establish
//! a power-loss durability guarantee.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion};
use snafu::{ResultExt, Snafu, ensure};
use uuid::Uuid;

/// Maximum value size, excluding the protocol envelope.
pub const MAX_VALUE_BYTES: usize = 1_048_576;
const MAGIC: &[u8; 7] = b"SPICEKV";
const FORMAT_VERSION: u8 = 1;
const HEADER_BYTES: usize = 25;
const MAX_RECORD_BYTES: usize = HEADER_BYTES + MAX_VALUE_BYTES;

/// Errors that establish a failed read or reject a write before submission.
///
/// Once a write is submitted, backend errors are returned through
/// [`WriteOutcome`], not this type.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to read state key '{key}': {source}"))]
    Read {
        key: Path,
        source: object_store::Error,
    },
    #[snafu(display("State key '{key}' exceeds the {MAX_VALUE_BYTES}-byte value limit"))]
    TooLarge { key: Path },
    #[snafu(display("State key '{key}' has an invalid record envelope"))]
    InvalidRecord { key: Path },
    #[snafu(display("State key '{key}' uses unsupported record version {version}"))]
    UnsupportedVersion { key: Path, version: u8 },
    #[snafu(display("State key '{key}' has no version token for conditional writes"))]
    MissingVersion { key: Path },
    #[snafu(display("The revision for state key '{key}' belongs to another key or store handle"))]
    InvalidRevision { key: Path },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Identity of one conditional write request, allocated before submitting it.
///
/// Retrying must preserve the entire request: key, expected revision, change,
/// and ID. A rebased mutation requires a new ID, even when its value is identical.
/// IDs provide correlation, not exactly-once execution or durable deduplication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteId(Uuid);

impl WriteId {
    /// Allocates a fresh request identity.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Restores an identity saved with the original request before submission.
    /// This does not make the request safe to rebase or change.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }

    /// Returns the stable identity bytes for application-owned recovery records.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

impl Default for WriteId {
    fn default() -> Self {
        Self::new()
    }
}

/// Opaque version returned by a fresh read.
///
/// Revisions are valid only for the same key and handle (or its clones).
/// Independently constructed handles must obtain their own revisions, even
/// when they address the same backend. Both provider version fields are retained.
#[derive(Debug, Clone)]
pub struct Revision {
    scope: Arc<()>,
    key: Path,
    version: UpdateVersion,
}

/// One version of a record. A tombstone is present with `value == None`.
#[derive(Debug, Clone)]
pub struct StateRecord {
    pub revision: Revision,
    pub write_id: WriteId,
    pub value: Option<Bytes>,
}

/// Precondition for a write. A tombstone satisfies `Exact`, never `Absent`.
#[derive(Debug, Clone, Copy)]
pub enum ExpectedRevision<'a> {
    Absent,
    Exact(&'a Revision),
}

/// Replaces the value or publishes a tombstone without physically deleting a key.
#[derive(Debug, Clone)]
pub enum StateChange {
    Set(Bytes),
    Tombstone,
}

/// Outcome of submitting a conditional write.
#[derive(Debug)]
#[must_use]
pub enum WriteOutcome {
    /// The backend acknowledged the write. A subsequent read obtains a revision
    /// and may observe a later writer; this outcome does not promise exclusivity.
    Applied,
    /// The final conditional request failed its precondition. A backend client
    /// may have retried after losing an earlier success response, so this does
    /// NOT prove that this invocation had no effect. Resolve its write ID before
    /// deciding whether to repeat a logical operation.
    Conflict,
    /// The write may or may not have applied. Even unsupported-operation errors
    /// are preserved here: arbitrary backend adapters may already have sent I/O.
    /// There is no unconditional-write fallback.
    Unknown { source: object_store::Error },
}

/// Small, per-key state storage with explicit concurrency preconditions.
///
/// No method has a default implementation: wrappers must forward the contract.
/// Dropping a write future does not roll it back; cancellation leaves an unknown
/// outcome. Keep the write ID before submission. A matching current record can
/// identify a request, but a different record does not prove absence from history.
/// Applications needing that proof must maintain commit lineage separately.
#[async_trait]
pub trait StateStore: Send + Sync {
    /// Reads directly from the backend; `None` means no record, not a tombstone.
    ///
    /// # Errors
    /// Returns an error on failed I/O, missing version tokens, incompatible
    /// records, or values exceeding the size limit.
    async fn read(&self, key: &Path) -> Result<Option<StateRecord>>;

    /// Submits exactly one adapter-level conditional write, with no automatic
    /// rebase. The underlying backend client may perform transport retries.
    ///
    /// # Errors
    /// Returns an error before submission for an invalid revision or oversized
    /// value. Inspect [`WriteOutcome`] for every submitted write.
    async fn compare_exchange(
        &self,
        key: &Path,
        expected: ExpectedRevision<'_>,
        change: StateChange,
        write_id: WriteId,
    ) -> Result<WriteOutcome>;
}

/// Implements [`StateStore`] over an object store supporting conditional writes.
///
/// Use an object-store prefix wrapper for namespace isolation. Only this protocol
/// may write to that namespace. Physical deletion of records or tombstones is not
/// supported: reclamation requires a higher-level incarnation/fencing protocol.
#[derive(Debug, Clone)]
pub struct ObjectStoreState {
    store: Arc<dyn ObjectStore>,
    scope: Arc<()>,
}

impl ObjectStoreState {
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            scope: Arc::new(()),
        }
    }

    fn revision(&self, key: &Path, version: UpdateVersion) -> Result<Revision> {
        ensure!(
            version.e_tag.as_ref().is_some_and(|v| !v.is_empty())
                || version.version.as_ref().is_some_and(|v| !v.is_empty()),
            MissingVersionSnafu { key: key.clone() }
        );
        Ok(Revision {
            scope: Arc::clone(&self.scope),
            key: key.clone(),
            version,
        })
    }
}

#[async_trait]
impl StateStore for ObjectStoreState {
    async fn read(&self, key: &Path) -> Result<Option<StateRecord>> {
        let result = match self.store.get(key).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(source) => {
                return Err(Error::Read {
                    key: key.clone(),
                    source,
                });
            }
        };
        ensure!(
            usize::try_from(result.meta.size).is_ok_and(|size| size <= MAX_RECORD_BYTES),
            TooLargeSnafu { key: key.clone() }
        );
        let revision = self.revision(
            key,
            UpdateVersion {
                e_tag: result.meta.e_tag.clone(),
                version: result.meta.version.clone(),
            },
        )?;

        // Bound both advertised length and streamed bytes before materializing
        // the record; a backend's length metadata is not an allocation budget.
        let mut bytes = BytesMut::new();
        let mut stream = result.into_stream();
        while let Some(chunk) = stream
            .try_next()
            .await
            .context(ReadSnafu { key: key.clone() })?
        {
            ensure!(
                chunk.len() <= MAX_RECORD_BYTES - bytes.len(),
                TooLargeSnafu { key: key.clone() }
            );
            bytes.extend_from_slice(&chunk);
        }
        let bytes = bytes.freeze();
        ensure!(
            bytes.len() >= HEADER_BYTES && bytes.starts_with(MAGIC),
            InvalidRecordSnafu { key: key.clone() }
        );
        ensure!(
            bytes[7] == FORMAT_VERSION,
            UnsupportedVersionSnafu {
                key: key.clone(),
                version: bytes[7]
            }
        );
        let write_id = WriteId(
            Uuid::from_slice(&bytes[9..HEADER_BYTES])
                .map_err(|_| Error::InvalidRecord { key: key.clone() })?,
        );
        let value = match bytes[8] {
            0 if bytes.len() == HEADER_BYTES => None,
            1 => Some(bytes.slice(HEADER_BYTES..)),
            _ => return InvalidRecordSnafu { key: key.clone() }.fail(),
        };
        Ok(Some(StateRecord {
            revision,
            write_id,
            value,
        }))
    }

    async fn compare_exchange(
        &self,
        key: &Path,
        expected: ExpectedRevision<'_>,
        change: StateChange,
        write_id: WriteId,
    ) -> Result<WriteOutcome> {
        let mode = match expected {
            ExpectedRevision::Absent => PutMode::Create,
            ExpectedRevision::Exact(revision) => {
                ensure!(
                    Arc::ptr_eq(&self.scope, &revision.scope) && *key == revision.key,
                    InvalidRevisionSnafu { key: key.clone() }
                );
                PutMode::Update(revision.version.clone())
            }
        };
        let value = match change {
            StateChange::Set(value) => Some(value),
            StateChange::Tombstone => None,
        };
        let value_len = value.as_ref().map_or(0, Bytes::len);
        ensure!(
            value_len <= MAX_VALUE_BYTES,
            TooLargeSnafu { key: key.clone() }
        );
        let mut encoded = BytesMut::with_capacity(HEADER_BYTES + value_len);
        encoded.extend_from_slice(MAGIC);
        encoded.extend_from_slice(&[FORMAT_VERSION, u8::from(value.is_some())]);
        encoded.extend_from_slice(write_id.as_bytes());
        if let Some(value) = value {
            encoded.extend_from_slice(&value);
        }
        match self
            .store
            .put_opts(key, encoded.freeze().into(), PutOptions::from(mode))
            .await
        {
            Ok(_) => Ok(WriteOutcome::Applied),
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => Ok(WriteOutcome::Conflict),
            Err(source) => Ok(WriteOutcome::Unknown { source }),
        }
    }
}
