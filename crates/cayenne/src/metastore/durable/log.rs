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

//! The authoritative copy of a catalog in object storage.
//!
//! A catalog is a sequence of committed transactions numbered from 1. Object
//! storage holds an *image* of the catalog database at some sequence plus the
//! change set of every later transaction, so any replica of the state at the
//! latest sequence can be rebuilt from it.
//!
//! The change sets live in *epochs*: each epoch is one
//! [`object_store_occ::wal::WalStateStore`] domain, and a transaction is
//! durable once its change set is committed to the current epoch. An epoch
//! holds at most a few dozen transactions. Rotation uploads a fresh image,
//! *seals* the current epoch with a final commit naming its successor, and
//! starts the successor. Because the domain's optimistic concurrency covers its
//! whole head, the seal fences every other commit to the old epoch. Each domain
//! therefore stays far inside the WAL's replay and history bounds, and once the
//! root has moved on, the epoch and image two rotations back hold nothing a
//! recovery reads, so they are deleted.
//!
//! ```text
//! <prefix>/root                   {catalog_id, epoch}       conditional writes
//! <prefix>/epochs/<epoch>/…       WalStateStore domain:
//!     base                        image + the tail of the previous epoch
//!     log/<sequence>              one transaction's change set (inline or a reference)
//!     sealed                      the successor epoch and its base
//! <prefix>/images/<id>            immutable catalog database images
//! <prefix>/changes/<epoch>/<id>   immutable change sets too large to inline
//! ```
//!
//! An image is taken from a read connection without blocking writers, so it
//! may lag the epoch it is rotated from. The successor's base records that lag
//! as a *tail*: the sequences from just past the image to the seal, which stay
//! in the sealed epoch until the next rotation.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use object_store_occ::store::{
    ExpectedRevision, ObjectStoreState, StateChange, StateStore, WriteId, WriteOutcome,
};
use object_store_occ::wal::{CommitOutcome, Limits, Snapshot, WalStateStore};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu, ensure};
use uuid::Uuid;

const FORMAT: u32 = 1;

/// Change sets up to this size are stored in the epoch itself; larger ones are
/// uploaded as their own object first. The WAL encodes values as JSON, so this
/// keeps a commit's record well inside its 1 MiB bound.
const INLINE_CHANGE_BYTES: usize = 24 * 1024;

/// Rotation is requested once an epoch holds this many transactions, or this
/// many bytes of change sets.
const ROTATE_AFTER_COMMITS: u64 = 48;
const ROTATE_AFTER_BYTES: u64 = 64 * 1024 * 1024;

/// An epoch accepts no transaction past this count, leaving room for its seal
/// inside the domain's 128-commit replay bound: a rotation that keeps failing
/// stops writes with an error rather than exceeding the bound.
const MAX_EPOCH_COMMITS: u64 = 120;

/// How long a commit whose outcome is uncertain keeps being retried before
/// the catalog stops accepting writes.
const UNCERTAIN_COMMIT_DEADLINE: Duration = Duration::from_secs(600);

/// Why the catalog in object storage could not be read or written.
#[derive(Debug, Snafu)]
#[expect(missing_docs, reason = "each variant's display message documents it")]
pub enum Error {
    #[snafu(display("Failed to read the catalog from {location}: {source}"))]
    Read {
        location: String,
        #[snafu(source(from(object_store::Error, Box::new)))]
        source: Box<object_store::Error>,
    },

    #[snafu(display("Failed to write the catalog to {location}: {source}"))]
    Write {
        location: String,
        #[snafu(source(from(object_store::Error, Box::new)))]
        source: Box<object_store::Error>,
    },

    #[snafu(display("Failed to access the catalog state in {location}: {source}"))]
    State {
        location: String,
        #[snafu(source(from(object_store_occ::store::Error, Box::new)))]
        source: Box<object_store_occ::store::Error>,
    },

    #[snafu(display("Failed to access the catalog log in {location}: {source}"))]
    Log {
        location: String,
        #[snafu(source(from(object_store_occ::wal::Error, Box::new)))]
        source: Box<object_store_occ::wal::Error>,
    },

    #[snafu(display("The catalog in {location} is malformed: {reason}"))]
    Malformed { location: String, reason: String },

    #[snafu(display(
        "The catalog in {location} was written by a newer version of Spice (format {format}). Upgrade Spice."
    ))]
    NewerFormat { location: String, format: u32 },

    #[snafu(display(
        "Another process committed to the catalog in {location}, so this process stopped writing to it. Only one Spice instance may write a `mode: datalake` table; restart this instance once the other has stopped."
    ))]
    Fenced { location: String },

    #[snafu(display(
        "A catalog commit to {location} has had an unknown outcome for {seconds} s, so this process stopped writing to it. Restart Spice to recover the committed state."
    ))]
    Uncertain { location: String, seconds: u64 },

    #[snafu(display(
        "The catalog in {location} holds {commits} commits since its last rotation, and rotation has not succeeded; writes resume once object storage accepts the rotation."
    ))]
    RotationRequired { location: String, commits: u64 },

    #[snafu(display(
        "A catalog image at sequence {image} is older than the current epoch, which starts after {start}"
    ))]
    StaleImage { image: u64, start: u64 },
}

/// The result of a catalog log operation.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A content-addressed immutable object under the catalog prefix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ObjectRef {
    /// Path relative to the catalog prefix.
    pub key: String,
    pub size: u64,
    #[serde(with = "hex_digest")]
    pub digest: [u8; 32],
}

mod hex_digest {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};
    use std::fmt::Write;

    pub fn serialize<S: Serializer>(digest: &[u8; 32], s: S) -> Result<S::Ok, S::Error> {
        let mut hex = String::with_capacity(64);
        for byte in digest {
            let _ = write!(hex, "{byte:02x}");
        }
        s.serialize_str(&hex)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<[u8; 32], D::Error> {
        let hex = String::deserialize(d)?;
        let mut digest = [0_u8; 32];
        if hex.len() != 64 {
            return Err(D::Error::custom("digest must be 64 hex characters"));
        }
        for (i, byte) in digest.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).map_err(D::Error::custom)?;
        }
        Ok(digest)
    }
}

/// The sequences a sealed epoch still holds for its successor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Tail {
    pub epoch: Uuid,
    /// The last sequence of the tail; the first is the image's sequence + 1.
    pub to: u64,
}

/// Where an epoch's state starts: an image, then its tail.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Base {
    pub image: ObjectRef,
    pub image_sequence: u64,
    pub tail: Option<Tail>,
}

impl Base {
    /// The last sequence the base covers; the epoch's own logs follow it.
    fn end(&self) -> u64 {
        self.tail.map_or(self.image_sequence, |t| t.to)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Seal {
    next: Uuid,
    base: Base,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Root {
    format: u32,
    catalog_id: Uuid,
    epoch: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum LogValue {
    Inline(Vec<u8>),
    Object(ObjectRef),
}

/// One durable transaction's change set, still to be fetched.
#[derive(Debug, Clone)]
pub(crate) struct PendingLog {
    pub sequence: u64,
    value: LogValue,
}

/// What a replica at `local` needs to reach the latest durable state.
#[derive(Debug, Clone)]
pub(crate) struct Recovery {
    /// The image to start from, when the replica is not usable as a start.
    pub image: Option<(ObjectRef, u64)>,
    /// The change sets to apply after the start, in sequence order.
    pub logs: Vec<PendingLog>,
}

/// The object store and prefix a catalog lives under.
#[derive(Clone)]
pub struct CatalogLocation {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    url: String,
}

impl std::fmt::Debug for CatalogLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogLocation")
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
}

impl CatalogLocation {
    /// A catalog under `prefix` in `store`; `url` names it in messages.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, prefix: Path, url: impl Into<String>) -> Self {
        Self {
            store,
            prefix,
            url: url.into(),
        }
    }

    /// The location as the user configured it, for messages.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    fn path(&self, key: &str) -> Path {
        key.split('/').fold(self.prefix.clone(), Path::join)
    }

    fn epoch_prefix(&self, epoch: Uuid) -> Path {
        self.prefix.clone().join("epochs").join(epoch.to_string())
    }

    /// Upload `bytes` as a new immutable object under `dir`.
    async fn put_immutable(&self, dir: &str, bytes: Bytes) -> Result<ObjectRef> {
        let key = format!("{dir}/{}", Uuid::now_v7());
        let digest = *blake3::hash(&bytes).as_bytes();
        let size = bytes.len() as u64;
        self.store
            .put_opts(
                &self.path(&key),
                PutPayload::from_bytes(bytes),
                PutOptions::from(PutMode::Create),
            )
            .await
            .context(WriteSnafu {
                location: &self.url,
            })?;
        Ok(ObjectRef { key, size, digest })
    }

    /// Read an immutable object back, verifying its digest.
    pub(crate) async fn get_immutable(&self, object: &ObjectRef) -> Result<Bytes> {
        let bytes = self
            .store
            .get(&self.path(&object.key))
            .await
            .context(ReadSnafu {
                location: &self.url,
            })?
            .bytes()
            .await
            .context(ReadSnafu {
                location: &self.url,
            })?;
        ensure!(
            bytes.len() as u64 == object.size && *blake3::hash(&bytes).as_bytes() == object.digest,
            MalformedSnafu {
                location: &self.url,
                reason: format!("object '{}' does not match its digest", object.key),
            }
        );
        Ok(bytes)
    }

    async fn delete_prefix(&self, prefix: Path) -> Result<()> {
        let locations = self
            .store
            .list(Some(&prefix))
            .map(|meta| meta.map(|m| m.location))
            .boxed();
        let mut deleted = self.store.delete_stream(locations);
        while let Some(result) = deleted.next().await {
            match result {
                Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
                Err(source) => {
                    return Err(Error::Write {
                        location: self.url.clone(),
                        source: Box::new(source),
                    });
                }
            }
        }
        Ok(())
    }

    /// Delete an epoch's domain and the change sets uploaded for it.
    async fn delete_epoch(&self, epoch: Uuid) -> Result<()> {
        self.delete_prefix(self.epoch_prefix(epoch)).await?;
        self.delete_prefix(self.prefix.clone().join("changes").join(epoch.to_string()))
            .await
    }

    async fn delete_object(&self, object: &ObjectRef) -> Result<()> {
        match self.store.delete(&self.path(&object.key)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(source) => Err(Error::Write {
                location: self.url.clone(),
                source: Box::new(source),
            }),
        }
    }
}

struct Epoch {
    id: Uuid,
    domain: WalStateStore,
    snapshot: Snapshot,
    base: Base,
    /// The last durable sequence.
    last: u64,
    bytes: u64,
}

impl Epoch {
    fn commits(&self) -> u64 {
        self.snapshot.sequence()
    }
}

/// A rotation whose seal committed but whose successor has not started yet.
#[derive(Clone)]
struct Successor {
    next: Uuid,
    base: Base,
    /// The sealed epoch's own base, which the successor no longer needs.
    previous: Base,
    sealed: Uuid,
    sealed_holds_tail: bool,
}

struct State {
    epoch: Epoch,
    /// Set between a rotation's seal and its successor's start; the current
    /// epoch is sealed meanwhile, so no write can commit until it starts.
    successor: Option<Successor>,
    /// Set once this process may no longer write the catalog.
    stopped: Option<Error>,
}

/// A writer's handle on the durable catalog.
pub(crate) struct DurableLog {
    location: CatalogLocation,
    state_store: Arc<dyn StateStore>,
    catalog_id: Uuid,
    state: tokio::sync::Mutex<State>,
}

impl std::fmt::Debug for DurableLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DurableLog")
            .field("location", &self.location)
            .field("catalog_id", &self.catalog_id)
            .finish_non_exhaustive()
    }
}

fn log_key(sequence: u64) -> String {
    format!("log/{sequence:020}")
}

fn encode_json<T: Serialize>(value: &T, location: &CatalogLocation) -> Result<Bytes> {
    serde_json::to_vec(value)
        .map(Bytes::from)
        .map_err(|e| Error::Malformed {
            location: location.url.clone(),
            reason: e.to_string(),
        })
}

fn decode_json<T: for<'de> Deserialize<'de>>(
    bytes: &[u8],
    what: &str,
    location: &CatalogLocation,
) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|e| Error::Malformed {
        location: location.url.clone(),
        reason: format!("{what}: {e}"),
    })
}

impl DurableLog {
    fn root_key(location: &CatalogLocation) -> Path {
        location.prefix.clone().join("root")
    }

    async fn open_domain(
        location: &CatalogLocation,
        state_store: &Arc<dyn StateStore>,
        epoch: Uuid,
    ) -> Result<WalStateStore> {
        WalStateStore::open(
            Arc::clone(state_store),
            location.epoch_prefix(epoch),
            Limits::default(),
        )
        .await
        .context(LogSnafu {
            location: &location.url,
        })
    }

    async fn read_root(
        location: &CatalogLocation,
        state_store: &Arc<dyn StateStore>,
    ) -> Result<Option<(Root, object_store_occ::store::Revision)>> {
        let Some(record) =
            state_store
                .read(&Self::root_key(location))
                .await
                .context(StateSnafu {
                    location: &location.url,
                })?
        else {
            return Ok(None);
        };
        let value = record.value.context(MalformedSnafu {
            location: &location.url,
            reason: "the root was deleted",
        })?;
        let root: Root = decode_json(&value, "root", location)?;
        ensure!(
            root.format <= FORMAT,
            NewerFormatSnafu {
                location: &location.url,
                format: root.format,
            }
        );
        Ok(Some((root, record.revision)))
    }

    /// Open the existing catalog at `location`, or `None` when it has none.
    ///
    /// Follows sealed epochs to the current one, completing a rotation an
    /// earlier process was interrupted in.
    pub(crate) async fn open(location: CatalogLocation) -> Result<Option<Self>> {
        let state_store: Arc<dyn StateStore> =
            Arc::new(ObjectStoreState::new(Arc::clone(&location.store)));
        let Some((root, mut revision)) = Self::read_root(&location, &state_store).await? else {
            return Ok(None);
        };
        let mut id = root.epoch;
        loop {
            let domain = Self::open_domain(&location, &state_store, id).await?;
            let snapshot = domain.snapshot().await.context(LogSnafu {
                location: &location.url,
            })?;
            if let Some(seal) = snapshot.get("sealed") {
                let seal: Seal = decode_json(seal, "seal", &location)?;
                Self::start_epoch(&location, &state_store, seal.next, &seal.base).await?;
                // Moving the root on is best-effort: recovery follows seals.
                let next = Root {
                    epoch: seal.next,
                    ..root
                };
                let moved = state_store
                    .compare_exchange(
                        &Self::root_key(&location),
                        ExpectedRevision::Exact(&revision),
                        StateChange::Set(encode_json(&next, &location)?),
                        WriteId::new(),
                    )
                    .await;
                if matches!(moved, Ok(WriteOutcome::Applied))
                    && let Some((_, current)) = Self::read_root(&location, &state_store).await?
                {
                    revision = current;
                }
                id = seal.next;
                continue;
            }
            let base: Base = decode_json(
                snapshot.get("base").context(MalformedSnafu {
                    location: &location.url,
                    reason: format!("epoch {id} has no base"),
                })?,
                "base",
                &location,
            )?;
            let (last, bytes) = Self::epoch_extent(&snapshot, &base, &location)?;
            return Ok(Some(Self {
                state_store,
                catalog_id: root.catalog_id,
                state: tokio::sync::Mutex::new(State {
                    epoch: Epoch {
                        id,
                        domain,
                        snapshot,
                        base,
                        last,
                        bytes,
                    },
                    successor: None,
                    stopped: None,
                }),
                location,
            }));
        }
    }

    fn epoch_extent(
        snapshot: &Snapshot,
        base: &Base,
        location: &CatalogLocation,
    ) -> Result<(u64, u64)> {
        let mut last = base.end();
        let mut bytes = 0_u64;
        for (key, value) in snapshot.scan_prefix("log/") {
            let sequence: u64 = key["log/".len()..].parse().map_err(|_| Error::Malformed {
                location: location.url.clone(),
                reason: format!("log key '{key}'"),
            })?;
            ensure!(
                sequence == last + 1,
                MalformedSnafu {
                    location: &location.url,
                    reason: format!("log sequence {sequence} follows {last}"),
                }
            );
            last = sequence;
            bytes += value.len() as u64;
        }
        Ok((last, bytes))
    }

    /// Initialize epoch `id` with `base`, idempotently.
    async fn start_epoch(
        location: &CatalogLocation,
        state_store: &Arc<dyn StateStore>,
        id: Uuid,
        base: &Base,
    ) -> Result<(WalStateStore, Snapshot)> {
        let domain = Self::open_domain(location, state_store, id).await?;
        let snapshot = domain.snapshot().await.context(LogSnafu {
            location: &location.url,
        })?;
        if let Some(existing) = snapshot.get("base") {
            let existing: Base = decode_json(existing, "base", location)?;
            ensure!(
                &existing == base,
                MalformedSnafu {
                    location: &location.url,
                    reason: format!("epoch {id} has a different base"),
                }
            );
            return Ok((domain, snapshot));
        }
        let mut tx = snapshot.transaction();
        tx.put("base", encode_json(base, location)?)
            .context(LogSnafu {
                location: &location.url,
            })?;
        let attempt = tx.prepare().await.context(LogSnafu {
            location: &location.url,
        })?;
        let snapshot =
            Self::commit_until_known(location, state_store, id, &domain, &attempt).await?;
        Ok((domain, snapshot))
    }

    /// Create catalog `catalog_id` whose first state is `image`, at sequence 0.
    ///
    /// Returns `None` when another process created one first.
    pub(crate) async fn create(
        location: CatalogLocation,
        image: Bytes,
        catalog_id: Uuid,
    ) -> Result<Option<Self>> {
        let state_store: Arc<dyn StateStore> =
            Arc::new(ObjectStoreState::new(Arc::clone(&location.store)));
        let image = location.put_immutable("images", image).await?;
        let base = Base {
            image,
            image_sequence: 0,
            tail: None,
        };
        let id = Uuid::now_v7();
        let (domain, snapshot) = Self::start_epoch(&location, &state_store, id, &base).await?;
        let root = Root {
            format: FORMAT,
            catalog_id,
            epoch: id,
        };
        let outcome = state_store
            .compare_exchange(
                &Self::root_key(&location),
                ExpectedRevision::Absent,
                StateChange::Set(encode_json(&root, &location)?),
                WriteId::new(),
            )
            .await
            .context(StateSnafu {
                location: &location.url,
            })?;
        match outcome {
            WriteOutcome::Applied => {}
            WriteOutcome::Conflict | WriteOutcome::Unknown { .. } => {
                // Whoever's root is there now wins; an unknown outcome of our own
                // write resolves the same way, by reading it back.
                match Self::read_root(&location, &state_store).await? {
                    Some((stored, _)) if stored.epoch == id => {}
                    _ => {
                        let _ = location.delete_epoch(id).await;
                        let _ = location.delete_object(&base.image).await;
                        return Ok(None);
                    }
                }
            }
        }
        Ok(Some(Self {
            location,
            state_store,
            catalog_id: root.catalog_id,
            state: tokio::sync::Mutex::new(State {
                epoch: Epoch {
                    id,
                    domain,
                    snapshot,
                    base,
                    last: 0,
                    bytes: 0,
                },
                successor: None,
                stopped: None,
            }),
        }))
    }

    pub(crate) fn location(&self) -> &CatalogLocation {
        &self.location
    }

    pub(crate) fn catalog_id(&self) -> Uuid {
        self.catalog_id
    }

    /// The last durable sequence.
    pub(crate) async fn last_sequence(&self) -> u64 {
        self.state.lock().await.epoch.last
    }

    /// Whether the current epoch is due for rotation.
    pub(crate) async fn rotation_due(&self) -> bool {
        let state = self.state.lock().await;
        state.stopped.is_none()
            && (state.successor.is_some()
                || state.epoch.commits() >= ROTATE_AFTER_COMMITS
                || state.epoch.bytes >= ROTATE_AFTER_BYTES)
    }

    /// What a replica at `local` (its last applied sequence, or `None` for no
    /// usable replica) must download and apply to reach the latest state.
    pub(crate) async fn recovery(&self, local: Option<u64>) -> Result<Recovery> {
        let state = self.state.lock().await;
        let epoch = &state.epoch;
        let base = &epoch.base;
        let usable = local.filter(|l| *l >= base.image_sequence && *l <= epoch.last);
        let start = usable.unwrap_or(base.image_sequence);
        let image = usable
            .is_none()
            .then(|| (base.image.clone(), base.image_sequence));
        let mut logs = Vec::new();
        if let Some(tail) = base.tail
            && start < tail.to
        {
            let domain = Self::open_domain(&self.location, &self.state_store, tail.epoch).await?;
            let snapshot = domain.snapshot().await.context(LogSnafu {
                location: &self.location.url,
            })?;
            Self::collect_logs(&snapshot, start + 1, tail.to, &mut logs, &self.location)?;
        }
        Self::collect_logs(
            &epoch.snapshot,
            start.max(base.end()) + 1,
            epoch.last,
            &mut logs,
            &self.location,
        )?;
        Ok(Recovery { image, logs })
    }

    fn collect_logs(
        snapshot: &Snapshot,
        from: u64,
        to: u64,
        logs: &mut Vec<PendingLog>,
        location: &CatalogLocation,
    ) -> Result<()> {
        for sequence in from..=to {
            let bytes = snapshot.get(&log_key(sequence)).context(MalformedSnafu {
                location: &location.url,
                reason: format!("sequence {sequence} is missing"),
            })?;
            let value: LogValue = postcard::from_bytes(bytes).map_err(|e| Error::Malformed {
                location: location.url.clone(),
                reason: format!("sequence {sequence}: {e}"),
            })?;
            logs.push(PendingLog { sequence, value });
        }
        Ok(())
    }

    /// The change set bytes of a pending log.
    pub(crate) async fn fetch(&self, log: &PendingLog) -> Result<Bytes> {
        match &log.value {
            LogValue::Inline(bytes) => Ok(Bytes::copy_from_slice(bytes)),
            LogValue::Object(object) => self.location.get_immutable(object).await,
        }
    }

    /// Commit `attempt`, retrying an uncertain outcome with the same attempt
    /// until it is known, and return the successor snapshot.
    async fn commit_until_known(
        location: &CatalogLocation,
        state_store: &Arc<dyn StateStore>,
        epoch: Uuid,
        domain: &WalStateStore,
        attempt: &object_store_occ::wal::PreparedCommit,
    ) -> Result<Snapshot> {
        let started = std::time::Instant::now();
        let mut delay = Duration::from_millis(50);
        loop {
            let error = match domain.commit_and_advance(attempt).await {
                Ok((CommitOutcome::Committed { .. }, Some(snapshot))) => return Ok(snapshot),
                Ok((CommitOutcome::Committed { .. }, None)) => {
                    return domain.snapshot().await.context(LogSnafu {
                        location: &location.url,
                    });
                }
                Ok((CommitOutcome::Conflict, _)) => {
                    return FencedSnafu {
                        location: &location.url,
                    }
                    .fail();
                }
                Ok((CommitOutcome::Unknown { source }, _)) => {
                    source.map_or_else(|| "no response".to_string(), |e| e.to_string())
                }
                Err(source) => source.to_string(),
            };
            // A newer writer deletes an epoch once it has sealed it and moved
            // on, so an epoch whose head is gone can never take this commit.
            let head = state_store
                .read(&location.epoch_prefix(epoch).join("head"))
                .await;
            if matches!(head, Ok(None)) {
                return FencedSnafu {
                    location: &location.url,
                }
                .fail();
            }
            let elapsed = started.elapsed();
            ensure!(
                elapsed < UNCERTAIN_COMMIT_DEADLINE,
                UncertainSnafu {
                    location: &location.url,
                    seconds: elapsed.as_secs(),
                }
            );
            tracing::warn!(
                "A catalog commit to {} has an unknown outcome, so it is being retried and writes wait for it. Cause: {error}",
                location.url
            );
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(5));
        }
    }

    /// Make `changes` durable as the next transaction, returning its sequence.
    ///
    /// Returns only once the outcome is known: a commit whose outcome is
    /// uncertain is retried until object storage confirms or refuses it.
    ///
    /// # Errors
    ///
    /// [`Error::Fenced`] when another process committed to the catalog, and
    /// [`Error::Uncertain`] when an outcome stayed unknown past the deadline;
    /// both stop all later writes. Other errors leave the catalog unchanged.
    pub(crate) async fn publish(&self, changes: Vec<u8>) -> Result<u64> {
        let mut state = self.state.lock().await;
        if let Some(stopped) = &state.stopped {
            return Err(Error::Fenced {
                location: match stopped {
                    Error::Uncertain { location, .. } | Error::Fenced { location } => {
                        location.clone()
                    }
                    _ => self.location.url.clone(),
                },
            });
        }
        if let Some(successor) = state.successor.clone() {
            let cleanup = self.start_successor(&mut state, successor).await?;
            if let Err(error) = self.finish_rotation(cleanup).await {
                tracing::warn!(
                    "Failed to remove the catalog objects that compacting the catalog log in {} replaced, so they stay in object storage; the catalog itself is unaffected. Cause: {error}",
                    self.location.url
                );
            }
        }
        let commits = state.epoch.commits();
        ensure!(
            commits < MAX_EPOCH_COMMITS,
            RotationRequiredSnafu {
                location: &self.location.url,
                commits,
            }
        );
        let size = changes.len() as u64;
        let value = if changes.len() <= INLINE_CHANGE_BYTES {
            LogValue::Inline(changes)
        } else {
            LogValue::Object(
                self.location
                    .put_immutable(&format!("changes/{}", state.epoch.id), Bytes::from(changes))
                    .await?,
            )
        };
        let encoded = postcard::to_stdvec(&value).map_err(|e| Error::Malformed {
            location: self.location.url.clone(),
            reason: e.to_string(),
        })?;
        let sequence = state.epoch.last + 1;
        let mut tx = state.epoch.snapshot.transaction();
        ensure!(
            tx.get("sealed").is_none(),
            FencedSnafu {
                location: &self.location.url,
            }
        );
        tx.put(log_key(sequence), Bytes::from(encoded))
            .context(LogSnafu {
                location: &self.location.url,
            })?;
        let attempt = tx.prepare().await.context(LogSnafu {
            location: &self.location.url,
        })?;
        match Self::commit_until_known(
            &self.location,
            &self.state_store,
            state.epoch.id,
            &state.epoch.domain,
            &attempt,
        )
        .await
        {
            Ok(snapshot) => {
                state.epoch.snapshot = snapshot;
                state.epoch.last = sequence;
                state.epoch.bytes += size;
                Ok(sequence)
            }
            Err(error @ (Error::Fenced { .. } | Error::Uncertain { .. })) => {
                tracing::error!("{error}");
                state.stopped = Some(Error::Fenced {
                    location: self.location.url.clone(),
                });
                Err(error)
            }
            Err(error) => Err(error),
        }
    }

    /// Rotate to a new epoch whose base is `image`, a database image at
    /// sequence `image_sequence`.
    ///
    /// Writers wait only for the seal and the successor's first commit, not for
    /// the image upload.
    ///
    /// # Errors
    ///
    /// Returns an error when the image is older than the current epoch's base
    /// or object storage refuses a step; the current epoch then stays current.
    pub(crate) async fn rotate(&self, image: Bytes, image_sequence: u64) -> Result<()> {
        if let Some(successor) = self.state.lock().await.successor.clone() {
            // An earlier rotation sealed the epoch; finish it first.
            let mut state = self.state.lock().await;
            let cleanup = self.start_successor(&mut state, successor).await?;
            drop(state);
            return self.finish_rotation(cleanup).await;
        }
        let image = self.location.put_immutable("images", image).await?;
        let mut state = self.state.lock().await;
        let start = state.epoch.base.end();
        let tail = if image_sequence == state.epoch.last {
            None
        } else if image_sequence >= start && image_sequence < state.epoch.last {
            Some(Tail {
                epoch: state.epoch.id,
                to: state.epoch.last,
            })
        } else {
            // Older than this epoch's own logs, so its tail would span two
            // sealed epochs; or newer than the durable state, which no image is.
            drop(state);
            let _ = self.location.delete_object(&image).await;
            return StaleImageSnafu {
                image: image_sequence,
                start,
            }
            .fail();
        };
        let successor = Successor {
            next: Uuid::now_v7(),
            sealed_holds_tail: tail.is_some(),
            base: Base {
                image,
                image_sequence,
                tail,
            },
            previous: state.epoch.base.clone(),
            sealed: state.epoch.id,
        };
        let seal = Seal {
            next: successor.next,
            base: successor.base.clone(),
        };
        let mut tx = state.epoch.snapshot.transaction();
        tx.put("sealed", encode_json(&seal, &self.location)?)
            .context(LogSnafu {
                location: &self.location.url,
            })?;
        let attempt = tx.prepare().await.context(LogSnafu {
            location: &self.location.url,
        })?;
        match Self::commit_until_known(
            &self.location,
            &self.state_store,
            state.epoch.id,
            &state.epoch.domain,
            &attempt,
        )
        .await
        {
            Ok(snapshot) => {
                state.epoch.snapshot = snapshot;
                state.successor = Some(successor.clone());
            }
            Err(error) => {
                if matches!(error, Error::Fenced { .. } | Error::Uncertain { .. }) {
                    state.stopped = Some(Error::Fenced {
                        location: self.location.url.clone(),
                    });
                }
                return Err(error);
            }
        }
        let cleanup = self.start_successor(&mut state, successor).await?;
        drop(state);
        self.finish_rotation(cleanup).await
    }

    /// Start the successor of a sealed epoch and make it current. On failure
    /// the successor stays pending, and the next write or rotation retries.
    async fn start_successor(&self, state: &mut State, successor: Successor) -> Result<Successor> {
        let (domain, snapshot) = Self::start_epoch(
            &self.location,
            &self.state_store,
            successor.next,
            &successor.base,
        )
        .await?;
        state.epoch = Epoch {
            id: successor.next,
            domain,
            snapshot,
            last: successor.base.end(),
            base: successor.base.clone(),
            bytes: 0,
        };
        state.successor = None;
        Ok(successor)
    }

    /// Point the root at a started successor, then delete what no recovery
    /// reads any more: the sealed epoch's own base image and tail epoch, and
    /// the sealed epoch itself when the successor holds no tail in it.
    async fn finish_rotation(&self, done: Successor) -> Result<()> {
        self.advance_root(done.next).await?;
        if let Some(tail) = done.previous.tail {
            self.location.delete_epoch(tail.epoch).await?;
        }
        self.location.delete_object(&done.previous.image).await?;
        if !done.sealed_holds_tail {
            self.location.delete_epoch(done.sealed).await?;
        }
        Ok(())
    }

    async fn advance_root(&self, epoch: Uuid) -> Result<()> {
        let (root, revision) = Self::read_root(&self.location, &self.state_store)
            .await?
            .context(MalformedSnafu {
                location: &self.location.url,
                reason: "the root disappeared",
            })?;
        let next = Root { epoch, ..root };
        let outcome = self
            .state_store
            .compare_exchange(
                &Self::root_key(&self.location),
                ExpectedRevision::Exact(&revision),
                StateChange::Set(encode_json(&next, &self.location)?),
                WriteId::new(),
            )
            .await
            .context(StateSnafu {
                location: &self.location.url,
            })?;
        match outcome {
            WriteOutcome::Applied => Ok(()),
            WriteOutcome::Conflict | WriteOutcome::Unknown { .. } => {
                match Self::read_root(&self.location, &self.state_store).await? {
                    Some((stored, _)) if stored.epoch == epoch => Ok(()),
                    _ => MalformedSnafu {
                        location: &self.location.url,
                        reason: "the root moved during rotation",
                    }
                    .fail(),
                }
            }
        }
    }
}
