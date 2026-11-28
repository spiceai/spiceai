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

//! Where cached blocks live on disk. The node cache uses append-only slab
//! files ([`slab::SlabStore`]). The node cache above a store owns admission,
//! the budget and the read-frequency estimate; a store keeps, returns and
//! verifies bytes, makes removals durable, and picks what to reclaim in its
//! own unit by that estimate.

pub(crate) mod codec;
#[cfg(unix)]
pub(crate) mod objects;
#[cfg(unix)]
pub(crate) mod slab;

use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};

use crate::Result;

/// The identity of one cached block: the object it belongs to, that object's
/// size (a rewritten object of another size never matches), the block size and
/// the block's index within the object.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlockKey {
    pub store: String,
    pub path: String,
    pub object_size: u64,
    pub block_size: u64,
    pub index: u64,
}

impl BlockKey {
    /// Canonical byte encoding, stored in each entry and compared on read so a
    /// digest collision is a miss rather than another block's bytes.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.store.len() + self.path.len() + 40);
        for s in [&self.store, &self.path] {
            out.extend_from_slice(&(s.len() as u64).to_le_bytes());
            out.extend_from_slice(s.as_bytes());
        }
        out.extend_from_slice(&self.object_size.to_le_bytes());
        out.extend_from_slice(&self.block_size.to_le_bytes());
        out.extend_from_slice(&self.index.to_le_bytes());
        out
    }

    /// Parses [`BlockKey::encode`]'s output; `None` for anything else.
    #[must_use]
    pub fn decode(buf: &[u8]) -> Option<Self> {
        let mut at = 0usize;
        let mut string = || -> Option<String> {
            let len =
                usize::try_from(u64::from_le_bytes(buf.get(at..at + 8)?.try_into().ok()?)).ok()?;
            at += 8;
            let s = std::str::from_utf8(buf.get(at..at.checked_add(len)?)?).ok()?;
            at += len;
            Some(s.to_owned())
        };
        let store = string()?;
        let path = string()?;
        let rest = buf.get(at..)?;
        if rest.len() != 24 {
            return None;
        }
        let u = |i: usize| u64::from_le_bytes(rest[i..i + 8].try_into().unwrap_or_default());
        Some(Self {
            store,
            path,
            object_size: u(0),
            block_size: u(8),
            index: u(16),
        })
    }

    /// 128-bit digest of [`BlockKey::encode`], used as the file name and the
    /// in-memory index key.
    #[must_use]
    pub fn digest(&self) -> Digest {
        let hash = blake3::hash(&self.encode());
        let mut d = [0u8; 16];
        d.copy_from_slice(&hash.as_bytes()[..16]);
        Digest(d)
    }
}

/// See [`BlockKey::digest`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Digest(pub [u8; 16]);

impl Digest {
    #[must_use]
    pub fn to_hex(&self) -> String {
        use std::fmt::Write as _;
        let mut s = String::with_capacity(32);
        for b in self.0 {
            let _ = write!(s, "{b:02x}");
        }
        s
    }

    #[must_use]
    pub fn from_hex(s: &str) -> Option<Self> {
        if s.len() != 32 || !s.is_ascii() {
            return None;
        }
        let mut d = [0u8; 16];
        for (i, out) in d.iter_mut().enumerate() {
            *out = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).ok()?;
        }
        Some(Digest(d))
    }
}

/// The object a block belongs to: every part of a [`BlockKey`] but the block
/// index.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectKey {
    pub store: Arc<str>,
    pub path: Arc<str>,
    pub object_size: u64,
    pub block_size: u64,
}

impl ObjectKey {
    /// The key of block `index` of this object.
    #[must_use]
    pub fn block(&self, index: u64) -> BlockKey {
        BlockKey {
            store: self.store.to_string(),
            path: self.path.to_string(),
            object_size: self.object_size,
            block_size: self.block_size,
            index,
        }
    }

    /// Number of blocks the object has.
    #[must_use]
    pub fn blocks(&self) -> u64 {
        self.object_size.div_ceil(self.block_size.max(1))
    }
}

/// The object metadata kept with every cached object, so a restarted cache
/// serves an object's metadata without asking the store: its e-tag and its
/// modification time, each `None` when the store did not report it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct StoredMeta {
    pub e_tag: Option<String>,
    pub last_modified: Option<DateTime<Utc>>,
}

impl StoredMeta {
    /// No e-tag and no modification time.
    pub const NONE: StoredMeta = StoredMeta {
        e_tag: None,
        last_modified: None,
    };

    /// Appends the encoding: a flags byte (bit 0 an e-tag, bit 1 a
    /// modification time), then the time as seconds (i64) and nanoseconds
    /// (u32), then the e-tag as a u16 length and its bytes, little-endian.
    /// With an e-tag longer than `u16::MAX` bytes nothing is kept (flags 0):
    /// the metadata reads back as unknown, never as an object without one.
    pub(crate) fn encode_into(&self, out: &mut Vec<u8>) {
        let e_tag = self
            .e_tag
            .as_deref()
            .map(|t| u16::try_from(t.len()).ok().map(|len| (len, t)));
        let e_tag = match e_tag {
            Some(None) => {
                out.push(0);
                return;
            }
            Some(Some(t)) => Some(t),
            None => None,
        };
        let mut flags = 0u8;
        if e_tag.is_some() {
            flags |= 1;
        }
        if self.last_modified.is_some() {
            flags |= 2;
        }
        out.push(flags);
        if let Some(t) = self.last_modified {
            out.extend_from_slice(&t.timestamp().to_le_bytes());
            out.extend_from_slice(&t.timestamp_subsec_nanos().to_le_bytes());
        }
        if let Some((len, t)) = e_tag {
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(t.as_bytes());
        }
    }

    /// The encoding alone.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_into(&mut out);
        out
    }

    /// Parses one encoding at the start of `buf`; returns it and the number
    /// of bytes it took, or `None` for anything else.
    pub(crate) fn decode(buf: &[u8]) -> Option<(Self, usize)> {
        let flags = *buf.first()?;
        if flags & !3 != 0 {
            return None;
        }
        let mut at = 1usize;
        let mut meta = StoredMeta::NONE;
        if flags & 2 != 0 {
            let secs = i64::from_le_bytes(buf.get(at..at + 8)?.try_into().ok()?);
            let nanos = u32::from_le_bytes(buf.get(at + 8..at + 12)?.try_into().ok()?);
            meta.last_modified = Some(DateTime::<Utc>::from_timestamp(secs, nanos)?);
            at += 12;
        }
        if flags & 1 != 0 {
            let len = usize::from(u16::from_le_bytes(buf.get(at..at + 2)?.try_into().ok()?));
            at += 2;
            let tag = std::str::from_utf8(buf.get(at..at.checked_add(len)?)?).ok()?;
            meta.e_tag = Some(tag.to_owned());
            at += len;
        }
        Some((meta, at))
    }

    /// True when neither the e-tag nor the modification time is known.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.e_tag.is_none() && self.last_modified.is_none()
    }
}

/// What a store knows about an object it holds blocks of.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectInfo {
    pub size: u64,
    pub meta: StoredMeta,
}

/// An estimate of how often a block is read: the node cache's frequency
/// sketch. A store keeps what is read often over what is not when it chooses
/// what to reclaim. Shared, so a store can weigh its entries off the async
/// threads.
pub type BlockHeat = Arc<dyn Fn(&Digest) -> u32 + Send + Sync>;

/// What one [`BlockStore::reclaim`] call freed: bytes on disk, and the error
/// that stopped it early, if any.
#[derive(Debug, Default)]
pub struct Reclaimed {
    pub freed: u64,
    pub error: Option<crate::Error>,
}

/// What one [`BlockStore::get_parts`] call read.
#[derive(Debug, Default)]
pub struct Parts {
    /// One entry per requested part, in request order: the verified bytes,
    /// or `None` for a block that is not resident, did not verify (and was
    /// dropped), was asked for an empty or out-of-range part, or was in a
    /// read that failed.
    pub found: Vec<Option<Bytes>>,
    /// The errors of the call: reads that failed, each with the number of
    /// parts it left `None`, and removals of corrupt entries that could not
    /// be made durable (zero parts).
    pub errors: Vec<PartsError>,
    /// The longest any one disk read of the call took. A call coalesces
    /// adjacent blocks into one read and runs independent reads at once, so
    /// this, not the call's duration, is what a slow disk shows in.
    pub slowest_read: Duration,
}

/// One error of a [`BlockStore::get_parts`] call.
#[derive(Debug)]
pub struct PartsError {
    /// Parts the error left unread.
    pub parts: usize,
    pub error: crate::Error,
}

/// Counters every store reports.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StoreCounters {
    /// Entries whose checksum, length or key did not verify on read; each was
    /// returned as a miss and dropped.
    pub corrupt_reads: u64,
    /// Block bytes of the entries counted in `corrupt_reads`.
    pub corrupt_bytes: u64,
    /// Slabs dropped at open because one of their files did not verify or
    /// could not be read.
    pub quarantined_slabs: u64,
}

/// A place to keep immutable blocks on local disk.
///
/// Every `get` verifies the entry (key, length, CRC32C of the data) and returns
/// `Ok(None)` for an entry that does not verify, after dropping it. An `Err`
/// is an I/O failure the caller counts and serves from the object store.
///
/// No method has a default: a wrapper must forward every one.
#[async_trait]
pub trait BlockStore: Send + Sync + Debug {
    /// Stores `bytes` under `key`, with the metadata of the object the block
    /// belongs to. Returns once the entry is written.
    async fn put(&self, key: &BlockKey, bytes: Bytes, meta: &StoredMeta) -> Result<()>;

    /// The block's bytes, verified, or `None`. The entry is returned at the
    /// length it was stored with; [`BlockStore::get_parts`] also checks that
    /// length against the key.
    async fn get(&self, key: &BlockKey) -> Result<Option<Bytes>>;

    /// For each `(key, part)`, the bytes of `part` of the block, verified
    /// chunk by chunk, in input order. An entry whose length is not the
    /// key's block length is corrupt and dropped. A failed read leaves only
    /// its own parts unread; parts that verified are still returned.
    async fn get_parts(&self, reads: &[(BlockKey, Range<u64>)]) -> Parts;

    /// True when the index lists the block. Reads nothing from disk, so the
    /// block may still fail verification on `get`.
    fn contains(&self, key: &BlockKey) -> bool;

    /// Drops the entry if present. The removal survives a restart.
    async fn remove(&self, key: &BlockKey) -> Result<()>;

    /// Drops every resident block of `objects`, as one batch, and returns
    /// the block bytes dropped. The removal survives a restart. Blocks
    /// written after it returns are kept.
    async fn remove_objects(&self, objects: &[ObjectKey]) -> Result<u64>;

    /// Blocks of `object` the index lists. Reads nothing from disk.
    fn resident_blocks(&self, object: &ObjectKey) -> u64;

    /// Bytes of block data the index holds (headers and padding excluded).
    fn resident_bytes(&self) -> u64;

    /// Entries the index holds.
    fn len(&self) -> u64;

    fn is_empty(&self) -> bool;

    /// Bytes the store occupies on disk, headers, padding and space not yet
    /// reclaimed included: what a budget is checked against.
    fn disk_bytes(&self) -> u64;

    /// The part of [`BlockStore::disk_bytes`] reserved by `put` calls that
    /// have not returned yet. A caller that admits each put's bytes before
    /// calling it subtracts these, so a put in progress is counted once.
    fn pending_bytes(&self) -> u64;

    /// Frees space in the store's own unit (one entry, or one whole slab with
    /// every entry still in it), one unit each time `over()` holds, the units
    /// whose entries are worth the least per byte on disk by `heat` first.
    /// Stops once `over()` does not hold, nothing more can be freed now, or a
    /// unit fails to go (the error is returned with the bytes freed before
    /// it).
    async fn reclaim(&self, heat: BlockHeat, over: &(dyn Fn() -> bool + Send + Sync)) -> Reclaimed;

    /// The paths of namespace `store`, at `prefix` or under it on a segment
    /// boundary (every path for an empty prefix), the store holds at least one
    /// block of.
    fn paths_under(&self, store: &str, prefix: &str) -> Vec<Arc<str>>;
    /// The objects at `path` of namespace `store` the store holds at least one
    /// block of: one per object size held (normally one).
    fn object_keys(&self, store: &str, path: &str) -> Vec<ObjectKey>;

    /// The size and metadata of object `path` of namespace `store` whose
    /// blocks of `block_size` bytes the store holds, when it holds blocks of
    /// exactly one size of that object.
    fn object_info(&self, store: &str, path: &str, block_size: u64) -> Option<ObjectInfo>;

    fn counters(&self) -> StoreCounters;

    /// Flushes buffered state: seals the slab being written and writes its
    /// index once its writes finish, so a clean reopen reads indexes only.
    async fn close(&self) -> Result<()>;
}
