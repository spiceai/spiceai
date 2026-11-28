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

//! Packed slab files: `<dir>/slabs/slab-<id>.dat`, append-only, each entry in
//! the shared format (4 KiB header page, then the data padded to 4 KiB).
//!
//! - **Writes** reserve an offset in the one active slab under a lock and write
//!   outside it, so writes run concurrently. A full slab is sealed: once its
//!   last reserved write completes, its index (`slab-<id>.idx`: the objects its
//!   entries belong to, and the digest, offset, length and object of every
//!   entry, with a CRC32C) is written to a temporary name and renamed into
//!   place.
//! - **Open** loads every sealed slab's `.idx` (one read per slab) and scans
//!   a slab without one (the slab active at a crash) from its start, following
//!   entry lengths. An entry is listed only if its header, key and data CRC32C
//!   verify, so a torn entry is never listed, and the scan stops at the first
//!   entry that does not (a torn write, or a write that failed after
//!   reserving its region): past it, entry boundaries cannot be trusted. The
//!   entries after it are dropped. The scanned slab is then sealed and its
//!   `.idx` written. Slabs are never appended to after a reopen, and each
//!   counts its whole file length on disk.
//! - **Quarantine.** A slab whose `.idx` does not verify, whose `.del` holds
//!   a record that does not verify (a torn append included), or whose files
//!   cannot be read is dropped at open, its `.dat` deleted first, and the
//!   other slabs open normally. An unverifiable tombstone file fails closed:
//!   skipping the bad record could bring a removed block back. Slab numbers
//!   are never reused, so a slab whose deletion failed is retried at the next
//!   open rather than overwritten.
//! - **Removal is durable.** `remove`, `remove_objects`, an entry found
//!   corrupt on read, and a copy of a block superseded by a newer copy append
//!   the entry's offset to the slab's tombstone file (`slab-<id>.del`, 12-byte
//!   records with a CRC32C each, synced, and its directory synced when the
//!   append created it), and open drops tombstoned entries. The bytes stay on
//!   disk until the slab is reclaimed, which deletes its tombstones with it.
//!   A slab whose tombstones cannot be appended is dropped whole instead, its
//!   `.dat` first, so no entry forgotten in memory is listed again at the next
//!   open. A write still running in a dropped slab neither recreates its
//!   `.dat` nor lists its entry, and the slab's bytes stay counted until the
//!   last such write ends.
//! - **Owner only.** The directory is mode `0700` and every file `0600`,
//!   whatever the process umask.
//! - **Space** is reclaimed by whole slab: [`BlockStore::reclaim`] drops the
//!   sealed slabs whose live entries are worth the least per byte of their
//!   file first, an entry's worth being its length times one plus its
//!   read-frequency estimate (the oldest slab on ties), with every entry still
//!   in them. The slabs are weighed once per call, off the async threads, and
//!   again only once that order is used up. When no sealed slab can go, it
//!   drops the active slab once no write is pending in it, so a cache that
//!   stopped writing can still shrink. A slab stays listed, entries and all,
//!   until its `.dat` is gone, so a removal meanwhile still tombstones its
//!   entries. There is no compaction (copying live entries out of a mostly
//!   dead slab).
//! - **CPU work runs off the async threads.** Encoding an entry and checking
//!   its CRC32C run in the same blocking task as the file I/O.
//! - **Objects.** The store counts resident blocks per object (namespace,
//!   path, size, block size), rebuilt at open from the `.idx` object tables,
//!   so the node cache finds an object's size after a restart without asking
//!   the object store.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::ops::Range;
use std::os::unix::fs::{DirBuilderExt, FileExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use snafu::{OptionExt, ResultExt, ensure};

use super::codec::{self, HEADER_LEN, entry_len};
use super::objects::Objects;
use super::{
    BlockHeat, BlockKey, BlockStore, Digest, ObjectInfo, ObjectKey, Parts, PartsError, Reclaimed,
    StoreCounters, StoredMeta,
};
use crate::Result;
use crate::error::{BlockTooLargeSnafu, IoSnafu, JoinSnafu, NoSlabIdSnafu};

/// Default slab size.
pub const DEFAULT_SLAB_BYTES: u64 = 256 << 20;
/// Slab indexes loaded concurrently at open.
const OPEN_CONCURRENCY: usize = 8;
/// Open slab descriptors kept.
const FD_CACHE: usize = 512;
const IDX_MAGIC: [u8; 4] = *b"OSCI";
const IDX_VERSION: u16 = 4;
/// Buffers at most this large are kept for cache-hit reads.
const READ_BUFFER_LEN: usize = 2 << 20;
/// The open path already limits disk work to `OPEN_CONCURRENCY`; retaining
/// twice that many read buffers caps idle pool memory at 32 MiB. Concurrent
/// reads past the cap allocate temporary buffers that are dropped on return.
const READ_POOL_BUFFERS: usize = OPEN_CONCURRENCY * 2;
/// Most blocking tasks one `get_parts` call spreads its independent reads
/// over.
const READ_TASKS: usize = 16;
/// Digest, offset, data length, object ordinal.
const IDX_ENTRY: usize = 16 + 8 + 4 + 4;
/// Offset and its CRC32C.
const TOMBSTONE: usize = 8 + 4;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Loc {
    slab: u32,
    offset: u64,
    data_len: u32,
    object: Arc<ObjectKey>,
}

impl Loc {
    /// True when this is the copy at `offset` of slab `slab`.
    fn is_copy(&self, slab: u32, offset: u64) -> bool {
        self.slab == slab && self.offset == offset
    }
}

#[derive(Debug, Clone)]
struct IdxEntry {
    digest: Digest,
    offset: u64,
    data_len: u32,
    object: Arc<ObjectKey>,
}

/// A sealed slab with no write pending, and its entries: ready for its `.idx`.
type ReadyIdx = Option<(u32, Arc<Vec<IdxEntry>>)>;

/// A slab's entries and the metadata of the objects they belong to.
type Listed = (Vec<IdxEntry>, Vec<(Arc<ObjectKey>, StoredMeta)>);

/// A slab a reclaim may take: its number, bytes on disk and entries.
type Candidate = (u32, u64, Arc<Vec<IdxEntry>>);

/// Where a slab is in its life. A slab's `.idx` is written only once it is
/// sealed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Takes appends: the active slab.
    #[default]
    Open,
    /// Takes no more appends; its `.idx` is not written yet.
    Sealed,
    /// Its `.idx` is written, or being written.
    Indexed,
}

#[derive(Debug, Default)]
struct SlabState {
    /// End of the reserved region (active slab only grows it).
    reserved_end: u64,
    /// Reserved writes not yet finished.
    pending: usize,
    phase: Phase,
    /// A reclaim is deleting the slab's files; no other reclaim takes it.
    reclaiming: bool,
    /// Being dropped whole ([`SlabStore::discard_slab`]), or dropped while
    /// writes were pending in it: its `.dat` is deleted and it lists no new
    /// entry. A slab dropped with writes pending leaves the map when the last
    /// of them ends.
    discarded: bool,
    /// Entries whose write completed, in completion order. Shared, so a
    /// reclaim weighs sealed slabs without copying their entries.
    entries: Arc<Vec<IdxEntry>>,
    /// Data bytes of entries the index still points at in this slab.
    live_bytes: u64,
}

impl SlabState {
    fn is_sealed(&self) -> bool {
        self.phase != Phase::Open
    }

    /// Stops appends to the slab.
    fn seal(&mut self) {
        if self.phase == Phase::Open {
            self.phase = Phase::Sealed;
        }
    }

    /// The slab's entries for its `.idx`, once: when it is sealed and no write
    /// is pending in it.
    fn take_ready(&mut self, id: u32) -> ReadyIdx {
        if self.phase != Phase::Sealed || self.pending > 0 || self.discarded {
            return None;
        }
        self.phase = Phase::Indexed;
        Some((id, Arc::clone(&self.entries)))
    }
}

#[derive(Debug, Default)]
struct Slabs {
    map: BTreeMap<u32, SlabState>,
    active: Option<u32>,
    /// The next slab number; past `u32::MAX` no slab can be created.
    next_id: u64,
}

impl Slabs {
    /// Ends one write reserved in slab `id`. Returns the slab when that was
    /// the last write of a discarded slab, which leaves the map with it: the
    /// caller stops counting its bytes on disk.
    fn end_write(&mut self, id: u32) -> Option<SlabState> {
        let state = self.map.get_mut(&id)?;
        state.pending = state.pending.saturating_sub(1);
        if state.discarded && state.pending == 0 {
            self.map.remove(&id)
        } else {
            None
        }
    }
}

/// A reserved region of a slab whose write is not recorded yet. It moves into
/// the blocking write and comes back with the result. Dropped unrecorded (the
/// `put` was cancelled, or its write panicked), it still ends the reservation,
/// after the write itself finished, so the slab can be reclaimed but is never
/// deleted under a running write.
#[derive(Debug)]
struct Reservation {
    slabs: Arc<Mutex<Slabs>>,
    disk: Arc<AtomicU64>,
    id: u32,
    open: bool,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if self.open
            && let Some(gone) = self.slabs.lock().end_write(self.id)
        {
            self.disk.fetch_sub(gone.reserved_end, Ordering::Relaxed);
        }
    }
}

/// An entry whose write succeeded, to list.
struct Written<'a> {
    key: &'a BlockKey,
    meta: &'a StoredMeta,
    digest: Digest,
    offset: u64,
    data_len: u32,
}

/// What listing a finished write leaves to do.
#[derive(Debug, Default)]
struct Completed {
    /// The entry was listed; false when its slab was discarded meanwhile.
    listed: bool,
    /// The older copy of the block the entry superseded (slab, offset), to
    /// tombstone.
    superseded: Option<(u32, u64)>,
    /// The slab, if it is now ready for its `.idx`.
    ready: ReadyIdx,
}

#[derive(Debug, Default)]
struct ReadBufferPool {
    buffers: Mutex<Vec<Vec<u8>>>,
}

impl ReadBufferPool {
    /// A buffer holding at least `len` bytes. Pooled buffers are zeroed once
    /// when allocated and reused as they are: a read overwrites `0..len`, and
    /// only that prefix is ever exposed.
    fn take(self: &Arc<Self>, len: usize) -> ReadBuffer {
        if len <= READ_BUFFER_LEN {
            let bytes = self
                .buffers
                .lock()
                .pop()
                .unwrap_or_else(|| vec![0u8; READ_BUFFER_LEN]);
            return ReadBuffer {
                bytes,
                len,
                pool: Some(Arc::clone(self)),
            };
        }
        ReadBuffer {
            bytes: vec![0u8; len],
            len,
            pool: None,
        }
    }

    #[cfg(test)]
    fn available(&self) -> usize {
        self.buffers.lock().len()
    }
}

/// A read buffer whose storage returns to its pool only when the last
/// [`Bytes`] slice backed by it is dropped.
struct ReadBuffer {
    bytes: Vec<u8>,
    /// The prefix of `bytes` the last read filled.
    len: usize,
    pool: Option<Arc<ReadBufferPool>>,
}

impl ReadBuffer {
    fn into_bytes(mut self) -> Bytes {
        if self.pool.is_some() {
            Bytes::from_owner(self)
        } else {
            let mut bytes = std::mem::take(&mut self.bytes);
            bytes.truncate(self.len);
            Bytes::from(bytes)
        }
    }
}

impl AsRef<[u8]> for ReadBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

impl Drop for ReadBuffer {
    fn drop(&mut self) {
        let Some(pool) = self.pool.take() else {
            return;
        };
        let mut buffers = pool.buffers.lock();
        if buffers.len() < READ_POOL_BUFFERS {
            buffers.push(std::mem::take(&mut self.bytes));
        }
    }
}

/// Locks are taken in the order `slabs`, `index`, `objects`: none is taken
/// while one after it is held. `fds` and `tombstones` are taken with none of
/// the others held.
#[derive(Debug)]
pub struct SlabStore {
    root: PathBuf,
    slab_bytes: u64,
    /// Shared, so a reclaim weighs slabs off the async threads.
    index: Arc<RwLock<HashMap<Digest, Loc>>>,
    /// Objects with resident blocks.
    objects: Arc<Mutex<Objects>>,
    slabs: Arc<Mutex<Slabs>>,
    fds: Arc<Mutex<HashMap<u32, Arc<File>>>>,
    read_pool: Arc<ReadBufferPool>,
    /// Held across each batch of tombstone appends, so an append that finds
    /// a slab's `.del` already there knows the append that created it synced
    /// its directory entry.
    tombstones: Arc<Mutex<()>>,
    resident: AtomicU64,
    /// Sum of every slab's reserved region: bytes on disk, dead entries and
    /// padding included. Shared with reservations: the last write of a
    /// discarded slab stops counting it.
    disk: Arc<AtomicU64>,
    /// The part of `disk` reserved by puts that have not returned.
    pending: AtomicU64,
    corrupt: AtomicU64,
    /// Block bytes of the entries counted in `corrupt`.
    corrupt_bytes: AtomicU64,
    /// Slabs dropped at open because a file of theirs did not verify or could
    /// not be read.
    quarantined: u64,
}

/// A put's reserved bytes, counted in [`SlabStore::pending_bytes`] until the
/// put returns or is dropped.
struct PendingBytes<'a> {
    counter: &'a AtomicU64,
    bytes: u64,
}

impl Drop for PendingBytes<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

fn dat_path(root: &Path, id: u32) -> PathBuf {
    root.join(format!("slab-{id:08}.dat"))
}

fn idx_path(root: &Path, id: u32) -> PathBuf {
    root.join(format!("slab-{id:08}.idx"))
}

fn del_path(root: &Path, id: u32) -> PathBuf {
    root.join(format!("slab-{id:08}.del"))
}

/// The offsets tombstoned in slab `id`. Every record must be complete and
/// verify; otherwise the caller discards the slab.
fn read_tombstones(root: &Path, id: u32) -> std::io::Result<HashSet<u64>> {
    let path = del_path(root, id);
    let buf = match std::fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(HashSet::new()),
        Err(e) => return Err(e),
    };
    let (records, remainder) = buf.as_chunks::<TOMBSTONE>();
    if !remainder.is_empty()
        || records
            .iter()
            .any(|record| codec::crc32c(&record[..8]).to_le_bytes() != record[8..])
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("invalid tombstone record in {}", path.display()),
        ));
    }
    Ok(records
        .iter()
        .map(|record| {
            let [o0, o1, o2, o3, o4, o5, o6, o7, ..] = *record;
            u64::from_le_bytes([o0, o1, o2, o3, o4, o5, o6, o7])
        })
        .collect())
}

/// Appends tombstones for `offsets` of slab `id` in one write and syncs them,
/// and the directory when this append created the file. Callers serialize
/// appends (`SlabStore::tombstones`), and open syncs the directory, so a
/// `.del` found in place already has a durable directory entry.
///
/// A torn earlier append (a partial record at the end) is cut off first, so
/// the new records start on a record boundary instead of being misread. If
/// the slab was reclaimed meanwhile, the file this created is removed again:
/// a reclaim removes the `.dat` before the `.del`, so checking for the `.dat`
/// after the append leaves no tombstones without their slab, whichever ran
/// first.
fn append_tombstones(root: &Path, id: u32, offsets: &[u64]) -> std::io::Result<()> {
    use std::io::Write as _;
    let mut buf = Vec::with_capacity(offsets.len() * TOMBSTONE);
    for o in offsets {
        let b = o.to_le_bytes();
        buf.extend_from_slice(&b);
        buf.extend_from_slice(&codec::crc32c(&b).to_le_bytes());
    }
    let path = del_path(root, id);
    let created = match std::fs::symlink_metadata(&path) {
        Ok(_) => false,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => true,
        Err(e) => return Err(e),
    };
    let mut f = owner_open(OpenOptions::new().create(true).append(true), &path)?;
    let len = f.metadata()?.len();
    let torn = len % TOMBSTONE as u64;
    if torn != 0 {
        f.set_len(len - torn)?;
    }
    f.write_all(&buf)?;
    f.sync_data()?;
    if created {
        File::open(root)?.sync_all()?;
    }
    if !dat_path(root, id).exists() {
        match std::fs::remove_file(&path) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e),
            _ => {}
        }
    }
    Ok(())
}

/// The error of a write into a slab dropped while the write ran.
fn discarded_error(dat: PathBuf) -> crate::Error {
    crate::Error::Io {
        action: "write",
        path: dat,
        source: std::io::Error::other(
            "the slab was dropped while the entry was written, after its tombstones could not be written",
        ),
    }
}

fn open_slab(
    fds: &Mutex<HashMap<u32, Arc<File>>>,
    root: &Path,
    id: u32,
    create: bool,
) -> std::io::Result<Arc<File>> {
    if let Some(f) = fds.lock().get(&id) {
        return Ok(Arc::clone(f));
    }
    let path = dat_path(root, id);
    let file = owner_open(
        OpenOptions::new().read(true).write(true).create(create),
        &path,
    )?;
    let file = Arc::new(file);
    let mut map = fds.lock();
    // A reclaim unlinks the slab before it drops the slab's descriptor from
    // the cache (`drop_files`), so a file still linked here is either cached
    // before that drop or not at all. A descriptor of an unlinked file kept in
    // the cache would hold its space on the volume.
    if file.metadata()?.nlink() == 0 {
        return Ok(file);
    }
    if map.len() >= FD_CACHE
        && let Some(victim) = map.keys().next().copied()
    {
        map.remove(&victim);
    }
    Ok(Arc::clone(map.entry(id).or_insert(file)))
}

/// Opens `path` as mode `0600`. The mode given at creation is narrowed by the
/// umask, so it is set again on the open file.
pub(crate) fn owner_open(options: &mut OpenOptions, path: &Path) -> std::io::Result<File> {
    let file = options.mode(0o600).open(path)?;
    file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    Ok(file)
}

/// Creates `path` (and missing parents) as mode `0700`, and sets that mode on
/// a directory that already existed.
pub(crate) fn owner_dir(path: &Path) -> std::io::Result<()> {
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
}

/// The `.idx` format (version 4), little-endian:
///
/// ```text
/// magic "OSCI", version u16, reserved u16, entry count u32, object count u32
/// objects: store length u32, store, path length u32, path, object size u64, block size u64,
///          metadata (StoredMeta::encode)
/// entries: digest [16], offset u64, data length u32, object ordinal u32
/// CRC32C u32 of everything before it
/// ```
fn encode_idx(entries: &[IdxEntry], meta_of: impl Fn(&ObjectKey) -> StoredMeta) -> Vec<u8> {
    let mut ordinals: HashMap<&ObjectKey, u32> = HashMap::new();
    let mut objects: Vec<&ObjectKey> = Vec::new();
    for e in entries {
        ordinals.entry(&e.object).or_insert_with(|| {
            objects.push(&e.object);
            u32::try_from(objects.len() - 1).unwrap_or(u32::MAX)
        });
    }
    let u32_len = |n: usize| u32::try_from(n).unwrap_or(u32::MAX).to_le_bytes();
    let mut out = Vec::with_capacity(16 + objects.len() * 96 + entries.len() * IDX_ENTRY + 4);
    out.extend_from_slice(&IDX_MAGIC);
    out.extend_from_slice(&IDX_VERSION.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&u32_len(entries.len()));
    out.extend_from_slice(&u32_len(objects.len()));
    for o in &objects {
        for s in [&o.store, &o.path] {
            out.extend_from_slice(&u32_len(s.len()));
            out.extend_from_slice(s.as_bytes());
        }
        out.extend_from_slice(&o.object_size.to_le_bytes());
        out.extend_from_slice(&o.block_size.to_le_bytes());
        meta_of(o).encode_into(&mut out);
    }
    for e in entries {
        out.extend_from_slice(&e.digest.0);
        out.extend_from_slice(&e.offset.to_le_bytes());
        out.extend_from_slice(&e.data_len.to_le_bytes());
        out.extend_from_slice(&ordinals.get(&*e.object).copied().unwrap_or(0).to_le_bytes());
    }
    let crc = codec::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

/// A cursor over a buffer that returns `None` instead of reading past its
/// end.
struct Reader<'a> {
    buf: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let out = self.buf.get(self.at..self.at.checked_add(n)?)?;
        self.at += n;
        Some(out)
    }
    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }
    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }
    fn string(&mut self) -> Option<&'a str> {
        let len = self.u32()? as usize;
        std::str::from_utf8(self.take(len)?).ok()
    }
}

/// `None` for an index that does not verify or has another version; the slab
/// is then scanned.
fn decode_idx(buf: &[u8]) -> Option<Listed> {
    if buf.len() < 20 || buf[0..4] != IDX_MAGIC || buf[4..6] != IDX_VERSION.to_le_bytes() {
        return None;
    }
    let (body, crc) = buf.split_at(buf.len() - 4);
    if codec::crc32c(body).to_le_bytes() != crc {
        return None;
    }
    let mut r = Reader { buf: body, at: 8 };
    let count = r.u32()? as usize;
    let object_count = r.u32()? as usize;
    let mut objects = Vec::with_capacity(object_count.min(1 << 16));
    let mut metas = Vec::with_capacity(object_count.min(1 << 16));
    for _ in 0..object_count {
        let store = Arc::from(r.string()?);
        let path = Arc::from(r.string()?);
        let key = Arc::new(ObjectKey {
            store,
            path,
            object_size: r.u64()?,
            block_size: r.u64()?,
        });
        let (meta, used) = StoredMeta::decode(r.buf.get(r.at..)?)?;
        r.at += used;
        if !meta.is_empty() {
            metas.push((Arc::clone(&key), meta));
        }
        objects.push(key);
    }
    let rest = r.take(count.checked_mul(IDX_ENTRY)?)?;
    if r.at != body.len() {
        return None;
    }
    let mut out = Vec::with_capacity(count);
    for chunk in rest.as_chunks::<IDX_ENTRY>().0 {
        let mut d = [0u8; 16];
        d.copy_from_slice(&chunk[0..16]);
        let ordinal = u32::from_le_bytes(chunk[28..32].try_into().ok()?) as usize;
        out.push(IdxEntry {
            digest: Digest(d),
            offset: u64::from_le_bytes(chunk[16..24].try_into().ok()?),
            data_len: u32::from_le_bytes(chunk[24..28].try_into().ok()?),
            object: Arc::clone(objects.get(ordinal)?),
        });
    }
    Some((out, metas))
}

fn write_idx(
    root: &Path,
    id: u32,
    entries: &[IdxEntry],
    meta_of: impl Fn(&ObjectKey) -> StoredMeta,
) -> std::io::Result<()> {
    let final_path = idx_path(root, id);
    let tmp = root.join(format!("slab-{id:08}.idx.tmp"));
    let written = owner_open(
        OpenOptions::new().write(true).create(true).truncate(true),
        &tmp,
    )
    .and_then(|mut file| {
        use std::io::Write as _;
        file.write_all(&encode_idx(entries, meta_of))?;
        // Synced before the rename, so a crash never leaves an `.idx` in
        // place whose contents are not on disk.
        file.sync_all()?;
        drop(file);
        std::fs::rename(&tmp, &final_path)?;
        File::open(root)?.sync_all()
    });
    if let Err(e) = written {
        // A temporary left behind is counted in no budget and removed only
        // by the next open. (A directory in its place is not removed.)
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    // The slab may have been reclaimed while its index was written. A reclaim
    // removes the `.dat` before the `.idx`, so checking for the `.dat` after
    // the rename leaves no index without its slab, whichever ran first.
    if !dat_path(root, id).exists() {
        match std::fs::remove_file(&final_path) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e),
            _ => {}
        }
    }
    Ok(())
}

/// True when the data of the entry whose header `h` sits at `offset` matches
/// the header's CRC32C.
fn data_verifies(file: &File, offset: u64, h: &codec::Header) -> std::io::Result<bool> {
    let mut data = vec![0u8; h.data_len];
    file.read_exact_at(&mut data, offset + HEADER_LEN as u64)?;
    Ok(codec::crc32c(&data) == h.data_crc)
}

/// Walks the entries of a slab with no `.idx` from its start, following entry
/// lengths, and returns them with the file's length. An entry is listed only
/// if its header, its key and its data CRC32C verify, with its data inside
/// the file, so a torn entry (a crash mid-write) is not listed and cannot come
/// back on a later open.
///
/// The walk stops at the first entry that does not verify (a torn write, or a
/// write that failed after reserving its region). Past it, entry boundaries
/// cannot be trusted: a complete entry stored inside another entry's data
/// verifies as well as a real one, and would serve bytes that were never
/// written under its key. The entries after it are dropped, so they are
/// later misses; the file still counts its whole length on disk.
fn scan_slab(path: &Path) -> std::io::Result<(Listed, u64)> {
    let file = owner_open(OpenOptions::new().read(true).write(true), path)?;
    let len = file.metadata()?.len();
    let page_len = HEADER_LEN as u64;
    let mut out = Vec::new();
    let mut objects: HashMap<ObjectKey, (Arc<ObjectKey>, StoredMeta)> = HashMap::new();
    let mut offset = 0u64;
    let mut page = vec![0u8; HEADER_LEN];
    while offset + page_len <= len {
        file.read_exact_at(&mut page, offset)?;
        let header =
            codec::decode_header(&page).filter(|h| offset + page_len + h.data_len as u64 <= len);
        let key = header
            .as_ref()
            .and_then(|h| BlockKey::decode(&codec::header_key(&page, h)));
        let (Some(h), Some(key)) = (header, key) else {
            break;
        };
        if !data_verifies(&file, offset, &h)? {
            break;
        }
        let entry_len = entry_len(h.data_len as u64);
        let object = ObjectKey {
            store: Arc::from(key.store.as_str()),
            path: Arc::from(key.path.as_str()),
            object_size: key.object_size,
            block_size: key.block_size,
        };
        let meta = StoredMeta::decode(codec::header_meta(&page, &h))
            .map(|(m, _)| m)
            .unwrap_or_default();
        let slot = objects
            .entry(object.clone())
            .or_insert_with(|| (Arc::new(object), StoredMeta::NONE));
        if !meta.is_empty() {
            slot.1 = meta;
        }
        out.push(IdxEntry {
            digest: key.digest(),
            offset,
            data_len: u32::try_from(h.data_len).unwrap_or(u32::MAX),
            object: Arc::clone(&slot.0),
        });
        offset += entry_len;
    }
    let metas = objects
        .into_values()
        .filter(|(_, m)| !m.is_empty())
        .collect();
    Ok(((out, metas), len))
}

/// One slab found at open: its number, its entries (tombstoned ones
/// dropped), the metadata of their objects, and its `.dat` file's length.
type Loaded = (u32, Vec<IdxEntry>, Vec<(Arc<ObjectKey>, StoredMeta)>, u64);

/// Deletes a slab's files, the `.dat` first. If the `.dat` cannot be deleted,
/// its `.idx` and `.del` stay too, so the next open quarantines it again
/// instead of loading its entries without their tombstones.
fn quarantine_slab(root: &Path, id: u32) {
    let gone = |path: PathBuf| match std::fs::remove_file(path) {
        Ok(()) => true,
        Err(e) => e.kind() == std::io::ErrorKind::NotFound,
    };
    if gone(dat_path(root, id)) {
        gone(idx_path(root, id));
        gone(del_path(root, id));
    }
}

/// One slab's entries, tombstoned ones dropped, the metadata of their
/// objects, and the length of its `.dat`. An `.idx` or `.del` that does not
/// verify, or a file that cannot be read, is an error: the caller
/// quarantines the slab.
fn load_slab(root: &Path, id: u32) -> Result<Loaded> {
    let idx = idx_path(root, id);
    let dat = dat_path(root, id);
    let from_idx = match std::fs::read(&idx) {
        Ok(buf) => Some(
            decode_idx(&buf)
                .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidData))
                .context(IoSnafu {
                    action: "verify",
                    path: &idx,
                })?,
        ),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => {
            return Err(e).context(IoSnafu {
                action: "read",
                path: idx,
            });
        }
    };
    let ((mut entries, metas), len) = if let Some(listed) = from_idx {
        let len = std::fs::metadata(&dat)
            .context(IoSnafu {
                action: "stat",
                path: dat,
            })?
            .len();
        (listed, len)
    } else {
        let ((entries, metas), len) = scan_slab(&dat).context(IoSnafu {
            action: "scan",
            path: dat,
        })?;
        let by_object: HashMap<&ObjectKey, &StoredMeta> =
            metas.iter().map(|(key, meta)| (&**key, meta)).collect();
        write_idx(root, id, &entries, |object| {
            by_object
                .get(object)
                .map(|meta| (*meta).clone())
                .unwrap_or_default()
        })
        .context(IoSnafu {
            action: "write",
            path: idx_path(root, id),
        })?;
        drop(by_object);
        ((entries, metas), len)
    };
    let dead = read_tombstones(root, id).context(IoSnafu {
        action: "read",
        path: del_path(root, id),
    })?;
    if !dead.is_empty() {
        entries.retain(|entry| !dead.contains(&entry.offset));
    }
    Ok((id, entries, metas, len))
}

/// The slabs of `ids` that load; the others are quarantined.
fn load_slabs(root: &Path, ids: &[u32]) -> Vec<Loaded> {
    let mut out = Vec::with_capacity(ids.len());
    for &id in ids {
        match load_slab(root, id) {
            Ok(loaded) => out.push(loaded),
            Err(_) => quarantine_slab(root, id),
        }
    }
    out
}

impl SlabStore {
    /// Opens (creating if needed) the layout under `dir` and rebuilds the index
    /// from the slab indexes, scanning any slab without one. A slab that does
    /// not load is quarantined (see the module documentation).
    ///
    /// # Errors
    ///
    /// An I/O error creating or listing the slab directory.
    pub async fn open(dir: &Path, slab_bytes: u64) -> Result<Arc<Self>> {
        let root = dir.join("slabs");
        let r = root.clone();
        let ids = tokio::task::spawn_blocking(move || -> Result<Vec<u32>> {
            owner_dir(&r).context(IoSnafu {
                action: "create",
                path: r.clone(),
            })?;
            let mut ids = Vec::new();
            for entry in std::fs::read_dir(&r).context(IoSnafu {
                action: "list",
                path: r.clone(),
            })? {
                let entry = entry.context(IoSnafu {
                    action: "list",
                    path: r.clone(),
                })?;
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name.ends_with(".tmp") {
                    let _ = std::fs::remove_file(entry.path());
                } else if let Some(id) = name
                    .strip_prefix("slab-")
                    .and_then(|s| s.strip_suffix(".dat"))
                {
                    if let Ok(id) = id.parse::<u32>() {
                        ids.push(id);
                    }
                } else if let Some(id) = name
                    .strip_prefix("slab-")
                    .and_then(|s| s.strip_suffix(".idx").or_else(|| s.strip_suffix(".del")))
                    && id.parse::<u32>().is_ok_and(|id| !dat_path(&r, id).exists())
                {
                    // An index or tombstones whose slab is gone (a reclaim
                    // raced their write): a later slab with the same number
                    // must not inherit them.
                    let _ = std::fs::remove_file(entry.path());
                }
            }
            // So a `.del` created before a crash has a durable directory
            // entry before an append finds it in place (see
            // `append_tombstones`).
            File::open(&r)
                .and_then(|dir| dir.sync_all())
                .context(IoSnafu {
                    action: "sync",
                    path: r,
                })?;
            ids.sort_unstable();
            Ok(ids)
        })
        .await
        .context(JoinSnafu)??;

        let per_task = ids.len().div_ceil(OPEN_CONCURRENCY).max(1);
        let mut tasks = Vec::new();
        for group in ids.chunks(per_task) {
            let group = group.to_vec();
            let r = root.clone();
            tasks.push(tokio::task::spawn_blocking(move || load_slabs(&r, &group)));
        }
        let mut loaded = Vec::with_capacity(ids.len());
        for t in tasks {
            loaded.extend(t.await.context(JoinSnafu)?);
        }
        // Building the maps of a full cache of small entries takes seconds of
        // CPU, so it runs on a blocking thread like the loads.
        tokio::task::spawn_blocking(move || {
            Arc::new(Self::assemble(root, slab_bytes, &ids, loaded))
        })
        .await
        .context(JoinSnafu)
    }

    /// The store [`Self::open`] builds from the slabs `loaded` of the slab
    /// numbers `ids`: the entry index, the object table and the slab table.
    fn assemble(root: PathBuf, slab_bytes: u64, ids: &[u32], mut loaded: Vec<Loaded>) -> Self {
        loaded.sort_unstable_by_key(|(id, _, _, _)| *id);
        let mut quarantined = (ids.len() - loaded.len()) as u64;
        // Past every slab found, quarantined ones included: a slab whose
        // files could not be deleted keeps its number.
        let mut slabs = Slabs {
            next_id: ids.last().map_or(0, |&id| u64::from(id) + 1),
            ..Slabs::default()
        };

        // Object metadata, the newest slab's when several slabs list it.
        let mut metas: HashMap<ObjectKey, StoredMeta> = HashMap::new();
        for (_, _, listed, _) in &mut loaded {
            for (key, meta) in listed.drain(..) {
                metas.insert((*key).clone(), meta);
            }
        }
        let listed: usize = loaded.iter().map(|(_, entries, _, _)| entries.len()).sum();
        let mut index: HashMap<Digest, Loc> = HashMap::with_capacity(listed);
        // Copies of a block listed more than once (a write repeated before its
        // older copy was tombstoned): the newest is kept, and the others are
        // tombstoned below, so removing the block later removes every copy.
        let mut superseded: BTreeMap<u32, Vec<u64>> = BTreeMap::new();
        for (id, entries, _, _) in &loaded {
            for e in entries {
                let old = index.insert(
                    e.digest,
                    Loc {
                        slab: *id,
                        offset: e.offset,
                        data_len: e.data_len,
                        object: Arc::clone(&e.object),
                    },
                );
                if let Some(old) = old {
                    superseded.entry(old.slab).or_default().push(old.offset);
                }
            }
        }
        if !superseded.is_empty() {
            // A slab whose superseded copies cannot be tombstoned is
            // quarantined, so no copy of it can come back once the newer one
            // is removed.
            let mut failed = Vec::new();
            for (id, offsets) in superseded {
                if append_tombstones(&root, id, &offsets).is_err() {
                    quarantine_slab(&root, id);
                    failed.push(id);
                }
            }
            if !failed.is_empty() {
                quarantined += failed.len() as u64;
                loaded.retain(|(id, _, _, _)| !failed.contains(id));
                index.retain(|_, loc| !failed.contains(&loc.slab));
            }
        }
        let mut live: HashMap<u32, u64> = HashMap::new();
        let mut resident = 0u64;
        // Entries of one object in one slab share that slab's key: count the
        // resident blocks per such key (hashed by address, not by strings),
        // then look each up once in the object table, which hands out one
        // shared key per object.
        let mut per_key: HashMap<*const ObjectKey, (Arc<ObjectKey>, u64)> = HashMap::new();
        for loc in index.values() {
            *live.entry(loc.slab).or_default() += u64::from(loc.data_len);
            resident += u64::from(loc.data_len);
            per_key
                .entry(Arc::as_ptr(&loc.object))
                .or_insert_with(|| (Arc::clone(&loc.object), 0))
                .1 += 1;
        }
        let mut objects = Objects::default();
        let shared: HashMap<*const ObjectKey, Arc<ObjectKey>> = per_key
            .into_iter()
            .map(|(ptr, (o, n))| {
                let meta = metas.get(&*o).unwrap_or(&StoredMeta::NONE);
                let key = objects.acquire(&o.store, &o.path, o.object_size, o.block_size, n, meta);
                (ptr, key)
            })
            .collect();
        for loc in index.values_mut() {
            if let Some(key) = shared.get(&Arc::as_ptr(&loc.object)) {
                loc.object = Arc::clone(key);
            }
        }
        let mut disk = 0u64;
        for (id, entries, _, file_len) in loaded {
            // A reopened slab is sealed; its size on disk is its file's
            // length, holes, a torn tail and a larger earlier slab size
            // included.
            let reserved_end = file_len;
            disk += reserved_end;
            slabs.map.insert(
                id,
                SlabState {
                    reserved_end,
                    pending: 0,
                    phase: Phase::Indexed,
                    reclaiming: false,
                    discarded: false,
                    entries: Arc::new(entries),
                    live_bytes: live.get(&id).copied().unwrap_or(0),
                },
            );
        }
        Self {
            root,
            slab_bytes,
            index: Arc::new(RwLock::new(index)),
            objects: Arc::new(Mutex::new(objects)),
            slabs: Arc::new(Mutex::new(slabs)),
            fds: Arc::new(Mutex::new(HashMap::new())),
            read_pool: Arc::new(ReadBufferPool::default()),
            tombstones: Arc::new(Mutex::new(())),
            resident: AtomicU64::new(resident),
            disk: Arc::new(AtomicU64::new(disk)),
            pending: AtomicU64::new(0),
            corrupt: AtomicU64::new(0),
            corrupt_bytes: AtomicU64::new(0),
            quarantined,
        }
    }

    /// Number of slab files.
    #[must_use]
    pub fn slab_count(&self) -> usize {
        self.slabs.lock().map.len()
    }

    /// The slab file and offset of the entry's first data byte. Exposed for
    /// corruption tests.
    #[must_use]
    pub fn data_location(&self, key: &BlockKey) -> Option<(PathBuf, u64)> {
        let loc = self.index.read().get(&key.digest())?.clone();
        Some((
            dat_path(&self.root, loc.slab),
            loc.offset + HEADER_LEN as u64,
        ))
    }

    /// Accounts for `loc` having left the index: one resident block of its
    /// object less, and its data bytes off `resident` and, given the slab
    /// table, off its slab's live bytes.
    fn unlisted(&self, objects: &mut Objects, slabs: Option<&mut Slabs>, loc: &Loc) {
        objects.release(&loc.object);
        self.resident
            .fetch_sub(u64::from(loc.data_len), Ordering::Relaxed);
        if let Some(s) = slabs.and_then(|slabs| slabs.map.get_mut(&loc.slab)) {
            s.live_bytes = s.live_bytes.saturating_sub(u64::from(loc.data_len));
        }
    }

    /// Drops from the index each of `digests` whose listed copy `listed`
    /// accepts, under one hold of the locks, and returns the copies dropped.
    fn forget_where<'a>(
        &self,
        digests: impl IntoIterator<Item = &'a Digest>,
        listed: impl Fn(&Loc) -> bool,
    ) -> Vec<Loc> {
        let mut slabs = self.slabs.lock();
        let mut index = self.index.write();
        let mut objects = self.objects.lock();
        let mut forgotten = Vec::new();
        for d in digests {
            if !index.get(d).is_some_and(&listed) {
                continue;
            }
            if let Some(loc) = index.remove(d) {
                self.unlisted(&mut objects, Some(&mut *slabs), &loc);
                forgotten.push(loc);
            }
        }
        forgotten
    }

    /// Drops the block from the index only while it still lists the copy at
    /// `read` (same slab and offset). A read that found its copy gone or
    /// corrupt must not drop a newer copy written since it looked the block
    /// up (after a reclaim took the old slab, say).
    fn forget_copy(&self, d: &Digest, read: &Loc) -> Option<Loc> {
        self.forget_where([d], |l| l.is_copy(read.slab, read.offset))
            .pop()
    }

    /// Forgets the entries, whichever copy the index lists, records their
    /// tombstones (one synced append per slab), and deletes slabs left with
    /// nothing live. Every entry is forgotten in memory whatever fails;
    /// returns the block bytes forgotten, or the first error.
    async fn forget_durably(&self, digests: &[Digest]) -> Result<u64> {
        let forgotten = self.forget_where(digests, |_| true);
        let bytes = forgotten.iter().map(|loc| u64::from(loc.data_len)).sum();
        let mut by_slab: BTreeMap<u32, Vec<u64>> = BTreeMap::new();
        for loc in forgotten {
            by_slab.entry(loc.slab).or_default().push(loc.offset);
        }
        self.tombstone(by_slab).await?;
        Ok(bytes)
    }

    /// Records tombstones for entries no longer in the index (every slab's
    /// append in one blocking task, each synced) and deletes slabs left with
    /// nothing live. A slab whose tombstones cannot be appended is dropped
    /// whole ([`SlabStore::discard_slab`]): its entries, forgotten in memory,
    /// would otherwise be listed again at the next open. Returns the first
    /// error of a slab that could not be made durable either way.
    async fn tombstone(&self, by_slab: BTreeMap<u32, Vec<u64>>) -> Result<()> {
        if by_slab.is_empty() {
            return Ok(());
        }
        let ids: Vec<u32> = by_slab.keys().copied().collect();
        let root = self.root.clone();
        let serial = Arc::clone(&self.tombstones);
        let joined = tokio::task::spawn_blocking(move || {
            let _one = serial.lock();
            by_slab
                .into_iter()
                .map(|(id, offsets)| (id, append_tombstones(&root, id, &offsets)))
                .collect::<Vec<_>>()
        })
        .await;
        // A panicked batch leaves every slab of it unknown, so each is
        // dropped as if its append failed.
        let appended = joined.unwrap_or_else(|e| {
            ids.into_iter()
                .map(|id| (id, Err(std::io::Error::other(e.to_string()))))
                .collect()
        });
        let mut first_err = None;
        for (id, appended) in appended {
            if let Err(source) = appended {
                if self.discard_slab(id).await.is_err() {
                    first_err.get_or_insert(crate::Error::Io {
                        action: "write",
                        path: del_path(&self.root, id),
                        source,
                    });
                }
                continue;
            }
            if let Err(e) = self.delete_if_dead(id).await {
                first_err.get_or_insert(e);
            }
        }
        first_err.map_or(Ok(()), Err)
    }

    /// Drops slab `id` whole: deletes its `.dat` (see
    /// [`SlabStore::drop_dat`]), then forgets every entry the index lists in
    /// it, and the slab. Its `.idx` and `.del` are deleted too, or else by the
    /// next open, which deletes those of a slab without a `.dat`. A slab with
    /// writes pending stays in the map, discarded, until the last of them
    /// ends; none of them recreates its `.dat` or lists its entry (see
    /// [`BlockStore::put`]). An error deleting the `.dat` leaves the slab as
    /// it was.
    async fn discard_slab(&self, id: u32) -> Result<()> {
        // Marked before the `.dat` goes, so a write that opens the slab after
        // the delete (and so creates the file again) finds the mark.
        if let Some(s) = self.slabs.lock().map.get_mut(&id) {
            s.discarded = true;
        }
        if let Err(e) = self.drop_dat(id).await {
            if let Some(s) = self.slabs.lock().map.get_mut(&id) {
                s.discarded = false;
            }
            return Err(e);
        }
        for path in [idx_path(&self.root, id), del_path(&self.root, id)] {
            let _ = tokio::fs::remove_file(path).await;
        }
        let gone = {
            let mut slabs = self.slabs.lock();
            if slabs.active == Some(id) {
                slabs.active = None;
            }
            let gone = match slabs.map.get_mut(&id) {
                Some(s) if s.pending > 0 => {
                    s.seal();
                    None
                }
                Some(_) => slabs.map.remove(&id),
                None => None,
            };
            let mut index = self.index.write();
            let mut objects = self.objects.lock();
            index.retain(|_, loc| {
                if loc.slab != id {
                    return true;
                }
                self.unlisted(&mut objects, None, loc);
                false
            });
            gone
        };
        if let Some(state) = gone {
            self.disk.fetch_sub(state.reserved_end, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Deletes a sealed, fully written slab with no live bytes left. Its bytes
    /// stay counted until its files are gone; if removing them fails, the slab
    /// goes back with nothing live in it, so the next reclaim retries it
    /// first.
    async fn delete_if_dead(&self, id: u32) -> Result<()> {
        let dead = {
            let mut slabs = self.slabs.lock();
            let dead = slabs.map.get(&id).is_some_and(|s| {
                s.is_sealed()
                    && s.pending == 0
                    && !s.reclaiming
                    && !s.discarded
                    && s.live_bytes == 0
            });
            if dead { slabs.map.remove(&id) } else { None }
        };
        let Some(mut state) = dead else {
            return Ok(());
        };
        if let Err(e) = self.drop_files(id).await {
            state.live_bytes = 0;
            self.slabs.lock().map.insert(id, state);
            return Err(e);
        }
        self.disk.fetch_sub(state.reserved_end, Ordering::Relaxed);
        Ok(())
    }

    /// Removes the slab's `.dat`, and only then its cached descriptor, so a
    /// read opening the slab meanwhile never caches a descriptor of the
    /// removed file (see `open_slab`).
    async fn drop_dat(&self, id: u32) -> Result<()> {
        let path = dat_path(&self.root, id);
        match tokio::fs::remove_file(&path).await {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                return Err(e).context(IoSnafu {
                    action: "remove",
                    path,
                });
            }
            _ => {}
        }
        self.fds.lock().remove(&id);
        Ok(())
    }

    /// Removes the slab's files, the `.dat` first ([`SlabStore::drop_dat`]).
    async fn drop_files(&self, id: u32) -> Result<()> {
        self.drop_dat(id).await?;
        for path in [idx_path(&self.root, id), del_path(&self.root, id)] {
            match tokio::fs::remove_file(&path).await {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                    return Err(e).context(IoSnafu {
                        action: "remove",
                        path,
                    });
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// The sealed slabs a reclaim may take, in the order it takes them: the
    /// least worth per byte of file first, the oldest on ties. A slab's worth
    /// is that of its live entries, each entry's data length times one plus
    /// its read-frequency estimate; a slab of dead entries is worth 0. Weighed
    /// on a blocking thread: it reads every entry of every candidate.
    async fn reclaim_order(&self, heat: BlockHeat) -> Result<VecDeque<u32>> {
        let candidates: Vec<Candidate> = self
            .slabs
            .lock()
            .map
            .iter()
            .filter(|(_, s)| s.is_sealed() && s.pending == 0 && !s.reclaiming && !s.discarded)
            .map(|(id, s)| (*id, s.reserved_end, Arc::clone(&s.entries)))
            .collect();
        if candidates.is_empty() {
            return Ok(VecDeque::new());
        }
        let index = Arc::clone(&self.index);
        tokio::task::spawn_blocking(move || {
            // (value, bytes on disk, id).
            let mut weighed: Vec<(u128, u64, u32)> = candidates
                .iter()
                .map(|(id, disk, entries)| {
                    // The index lock is taken per slab, never across the whole
                    // walk, so reads and fills are not held up behind it.
                    let index = index.read();
                    let value: u128 = entries
                        .iter()
                        .filter(|e| {
                            index
                                .get(&e.digest)
                                .is_some_and(|l| l.is_copy(*id, e.offset))
                        })
                        .map(|e| u128::from(e.data_len) * (1 + u128::from(heat(&e.digest))))
                        .sum();
                    (value, (*disk).max(1), *id)
                })
                .collect();
            // Value per byte, compared by cross-multiplying.
            weighed.sort_unstable_by(|a, b| {
                let lhs = a.0.saturating_mul(u128::from(b.1));
                let rhs = b.0.saturating_mul(u128::from(a.1));
                lhs.cmp(&rhs).then(a.2.cmp(&b.2))
            });
            weighed.into_iter().map(|(_, _, id)| id).collect()
        })
        .await
        .context(JoinSnafu)
    }

    /// Reclaims sealed slab `victim`, or the active slab for `None`, dropping
    /// every entry still in it, if it is still idle (no write pending, no
    /// other reclaim on it). Taking the active slab lets a cache that writes
    /// nothing still shrink (another writer filled the volume). Returns the
    /// slab's bytes on disk, or `None` when it cannot be reclaimed now.
    async fn reclaim_slab(&self, victim: Option<u32>) -> Result<Option<u64>> {
        let id = {
            // Marked, so no other reclaim takes it. A sealed slab takes no new
            // writes. The active slab stops being active in the same step, so
            // the next write starts a new slab.
            let mut slabs = self.slabs.lock();
            let active = slabs.active;
            let Some(id) = victim.or(active) else {
                return Ok(None);
            };
            let Some(state) = slabs.map.get_mut(&id).filter(|s| {
                s.pending == 0
                    && !s.reclaiming
                    && !s.discarded
                    && (s.is_sealed() || active == Some(id))
            }) else {
                return Ok(None);
            };
            state.seal();
            state.reclaiming = true;
            if active == Some(id) {
                slabs.active = None;
            }
            id
        };
        // The files go first, the slab and its entries after: until the `.dat`
        // is gone, a removal still finds the entries and tombstones them, so
        // none comes back at the next open if the delete fails or the process
        // dies first.
        if let Err(e) = self.drop_files(id).await {
            if let Some(s) = self.slabs.lock().map.get_mut(&id) {
                s.reclaiming = false;
            }
            return Err(e);
        }
        let mut slabs = self.slabs.lock();
        let Some(state) = slabs.map.remove(&id) else {
            // Discarded meanwhile, which forgot it.
            return Ok(None);
        };
        {
            let mut index = self.index.write();
            let mut objects = self.objects.lock();
            for e in state.entries.iter() {
                if index
                    .get(&e.digest)
                    .is_some_and(|l| l.is_copy(id, e.offset))
                    && let Some(loc) = index.remove(&e.digest)
                {
                    self.unlisted(&mut objects, None, &loc);
                }
            }
        }
        drop(slabs);
        let size = state.reserved_end;
        self.disk.fetch_sub(size, Ordering::Relaxed);
        Ok(Some(size))
    }

    /// Reserves space for an entry of `entry_len` bytes; returns the
    /// reservation, the offset, and a slab that was sealed by this call and is
    /// ready for its `.idx` (no writes pending).
    fn reserve(&self, entry_len: u64) -> Result<(Reservation, u64, ReadyIdx)> {
        let mut slabs = self.slabs.lock();
        let mut ready = None;
        let fits = slabs
            .active
            .and_then(|id| slabs.map.get(&id))
            .is_some_and(|s| s.reserved_end + entry_len <= self.slab_bytes);
        if !fits {
            let id = u32::try_from(slabs.next_id)
                .ok()
                .context(NoSlabIdSnafu { path: &self.root })?;
            if let Some(old) = slabs.active.take()
                && let Some(s) = slabs.map.get_mut(&old)
            {
                s.seal();
                ready = s.take_ready(old);
            }
            slabs.next_id += 1;
            slabs.map.insert(id, SlabState::default());
            slabs.active = Some(id);
        }
        let id = slabs.active.unwrap_or_default();
        let state = slabs.map.entry(id).or_default();
        let offset = state.reserved_end;
        state.reserved_end += entry_len;
        self.disk.fetch_add(entry_len, Ordering::Relaxed);
        state.pending += 1;
        let reservation = Reservation {
            slabs: Arc::clone(&self.slabs),
            disk: Arc::clone(&self.disk),
            id,
            open: true,
        };
        Ok((reservation, offset, ready))
    }

    /// Ends the write of `reservation` and lists `written`, the entry when its
    /// write succeeded: in the slab's entries and in the index, its object
    /// counted resident and an older copy of the block unlisted. Listing
    /// happens under the slab table's lock, so a discard of the slab either
    /// comes after it and forgets the entry, or before it, and the entry is
    /// not listed.
    fn complete(&self, mut reservation: Reservation, written: Option<Written<'_>>) -> Completed {
        reservation.open = false;
        let id = reservation.id;
        let mut out = Completed::default();
        let mut slabs = self.slabs.lock();
        let discarded = slabs.map.get(&id).is_none_or(|s| s.discarded);
        if let Some(w) = written.filter(|_| !discarded) {
            let object = {
                let mut index = self.index.write();
                let mut objects = self.objects.lock();
                let k = w.key;
                let object =
                    objects.acquire(&k.store, &k.path, k.object_size, k.block_size, 1, w.meta);
                let old = index.insert(
                    w.digest,
                    Loc {
                        slab: id,
                        offset: w.offset,
                        data_len: w.data_len,
                        object: Arc::clone(&object),
                    },
                );
                self.resident
                    .fetch_add(u64::from(w.data_len), Ordering::Relaxed);
                if let Some(old) = old {
                    self.unlisted(&mut objects, Some(&mut *slabs), &old);
                    out.superseded = Some((old.slab, old.offset));
                }
                object
            };
            if let Some(state) = slabs.map.get_mut(&id) {
                state.live_bytes += u64::from(w.data_len);
                Arc::make_mut(&mut state.entries).push(IdxEntry {
                    digest: w.digest,
                    offset: w.offset,
                    data_len: w.data_len,
                    object,
                });
            }
            out.listed = true;
        }
        if let Some(gone) = slabs.end_write(id) {
            self.disk.fetch_sub(gone.reserved_end, Ordering::Relaxed);
        }
        out.ready = slabs.map.get_mut(&id).and_then(|s| s.take_ready(id));
        out
    }

    /// Reads and verifies the copy of block `key` (digest `d`) at `loc`,
    /// which must be `expected` bytes long, off the async threads. A copy
    /// whose slab is gone or cut short is forgotten, and a corrupt one is
    /// tombstoned, each only while the index still lists that copy.
    async fn get_copy(
        &self,
        key: &BlockKey,
        d: &Digest,
        loc: Loc,
        expected: u64,
    ) -> Result<Option<Bytes>> {
        let part = 0..usize::try_from(expected).unwrap_or(usize::MAX);
        let mut parts = self
            .read_wanted(1, vec![Wanted::new(0, key, *d, loc, part, expected)])
            .await;
        if let Some(failed) = parts.errors.into_iter().next() {
            return Err(failed.error);
        }
        Ok(parts.found.pop().flatten())
    }

    async fn get_parts_impl(&self, reads: &[(BlockKey, Range<u64>)]) -> Parts {
        let located: Vec<Option<(Digest, Loc)>> = {
            let index = self.index.read();
            reads
                .iter()
                .map(|(key, part)| {
                    // An empty part is a caller error, never a reason to read.
                    if part.start >= part.end {
                        return None;
                    }
                    let digest = key.digest();
                    index.get(&digest).map(|loc| (digest, loc.clone()))
                })
                .collect()
        };
        let wanted = reads
            .iter()
            .zip(located)
            .enumerate()
            .filter_map(|(pos, ((key, part), located))| {
                let (digest, loc) = located?;
                let part = usize::try_from(part.start).ok()?..usize::try_from(part.end).ok()?;
                Some(Wanted::new(pos, key, digest, loc, part, expected_len(key)))
            })
            .collect();
        self.read_wanted(reads.len(), wanted).await
    }

    /// Reads the `wanted` parts (positions below `n`) and settles what the
    /// reads found: copies whose slab is gone are forgotten, and corrupt ones
    /// are tombstoned together, in one durable write per slab.
    async fn read_wanted(&self, n: usize, wanted: Vec<Wanted>) -> Parts {
        let mut parts = Parts {
            found: vec![None; n],
            ..Parts::default()
        };
        let mut corrupt = Vec::new();
        let mut plans = Vec::with_capacity(wanted.len());
        for w in wanted {
            match w.plan() {
                Planned::Read(plan) => plans.push(plan),
                Planned::Corrupt(digest, loc) => corrupt.push((digest, loc)),
                Planned::Invalid => {}
            }
        }
        for run in self.read_runs(coalesce(plans)).await {
            parts.slowest_read = parts.slowest_read.max(run.took);
            match run.read {
                Ok(reads) => {
                    for (plan, read) in run.plans.into_iter().zip(reads) {
                        match read {
                            PartRead::Verified(bytes) => parts.found[plan.pos] = Some(bytes),
                            PartRead::Gone => {
                                self.forget_copy(&plan.digest, &plan.loc);
                            }
                            PartRead::Corrupt => corrupt.push((plan.digest, plan.loc)),
                        }
                    }
                }
                Err(error) => parts.errors.push(PartsError {
                    parts: run.parts,
                    error,
                }),
            }
        }
        if let Err(error) = self.forget_corrupt(&corrupt).await {
            parts.errors.push(PartsError { parts: 0, error });
        }
        parts
    }

    /// Counts and tombstones corrupt copies, each only while the index still
    /// lists it. Tombstoned, not only forgotten, so a sealed slab's `.idx`
    /// does not list the entry again after a restart.
    async fn forget_corrupt(&self, corrupt: &[(Digest, Loc)]) -> Result<()> {
        let mut by_slab: BTreeMap<u32, Vec<u64>> = BTreeMap::new();
        for (digest, loc) in corrupt {
            self.corrupt.fetch_add(1, Ordering::Relaxed);
            self.corrupt_bytes
                .fetch_add(u64::from(loc.data_len), Ordering::Relaxed);
            if let Some(forgotten) = self.forget_copy(digest, loc) {
                by_slab
                    .entry(forgotten.slab)
                    .or_default()
                    .push(forgotten.offset);
            }
        }
        self.tombstone(by_slab).await
    }

    /// Runs `runs` on at most [`READ_TASKS`] blocking tasks at once, each
    /// task reading its share of the runs in turn. A run that fails, or whose
    /// task panicked, fails alone.
    async fn read_runs(&self, runs: Vec<Vec<PartPlan>>) -> Vec<RunRead> {
        if runs.is_empty() {
            return Vec::new();
        }
        let per_task = runs.len().div_ceil(READ_TASKS);
        let mut tasks = Vec::with_capacity(runs.len().div_ceil(per_task));
        let mut runs = runs.into_iter().peekable();
        while runs.peek().is_some() {
            let share: Vec<Vec<PartPlan>> = runs.by_ref().take(per_task).collect();
            let fds = Arc::clone(&self.fds);
            let root = self.root.clone();
            let pool = Arc::clone(&self.read_pool);
            let parts: usize = share.iter().map(Vec::len).sum();
            let task = tokio::task::spawn_blocking(move || {
                share
                    .into_iter()
                    .map(|run| read_one_run(&fds, &root, &pool, run))
                    .collect::<Vec<RunRead>>()
            });
            tasks.push(async move { (parts, task.await) });
        }
        let mut out = Vec::new();
        for (parts, joined) in futures::future::join_all(tasks).await {
            match joined {
                Ok(reads) => out.extend(reads),
                Err(source) => out.push(RunRead {
                    plans: Vec::new(),
                    parts,
                    read: Err(crate::Error::Join { source }),
                    took: Duration::ZERO,
                }),
            }
        }
        out
    }

    async fn persist_idx(&self, ready: ReadyIdx) -> Result<()> {
        let Some((id, entries)) = ready else {
            return Ok(());
        };
        let root = self.root.clone();
        let objects = Arc::clone(&self.objects);
        tokio::task::spawn_blocking(move || {
            // An object whose blocks all left meanwhile has no metadata to
            // record; its entries are gone from the index too.
            write_idx(&root, id, &entries, |o| {
                objects.lock().meta_of(o).unwrap_or_default()
            })
        })
        .await
        .context(JoinSnafu)?
        .context(IoSnafu {
            action: "write",
            path: idx_path(&self.root, id),
        })
    }
}

/// One requested part of a located copy, before it is checked.
struct Wanted {
    pos: usize,
    enc: Vec<u8>,
    digest: Digest,
    loc: Loc,
    part: Range<usize>,
    /// The block's length, from its key.
    expected: u64,
}

enum Planned {
    Read(PartPlan),
    /// The index lists a copy of the wrong length.
    Corrupt(Digest, Loc),
    /// A part the block does not hold: a caller error, so not a reason to
    /// drop the copy.
    Invalid,
}

impl Wanted {
    fn new(
        pos: usize,
        key: &BlockKey,
        digest: Digest,
        loc: Loc,
        part: Range<usize>,
        expected: u64,
    ) -> Self {
        Self {
            pos,
            enc: key.encode(),
            digest,
            loc,
            part,
            expected,
        }
    }

    /// Checks the part against the block's length before any read, so a
    /// copy that cannot be read is never coalesced with its neighbours.
    fn plan(self) -> Planned {
        if self.part.start > self.part.end || self.part.end as u64 > self.expected {
            return Planned::Invalid;
        }
        if u64::from(self.loc.data_len) != self.expected {
            return Planned::Corrupt(self.digest, self.loc);
        }
        let Some(covering) = codec::covering(self.loc.data_len as usize, &self.part) else {
            return Planned::Invalid;
        };
        Planned::Read(PartPlan {
            pos: self.pos,
            enc: self.enc,
            digest: self.digest,
            loc: self.loc,
            part: self.part,
            covering,
        })
    }
}

/// A checked part read: `part` lies within the copy at `loc`, whose length
/// is the block's, and `covering` is its chunk-aligned range.
struct PartPlan {
    pos: usize,
    enc: Vec<u8>,
    digest: Digest,
    loc: Loc,
    part: Range<usize>,
    covering: Range<usize>,
}

enum PartRead {
    Gone,
    Corrupt,
    Verified(Bytes),
}

/// What one run's read found, one result per plan, and how long it took.
struct RunRead {
    plans: Vec<PartPlan>,
    /// The parts the run covers. A task that panicked reports its runs as
    /// one, with their plans lost.
    parts: usize,
    read: Result<Vec<PartRead>>,
    took: Duration,
}

/// Groups plans, in order, into runs of adjacent entries of one slab, each
/// read with one spanning read.
fn coalesce(plans: Vec<PartPlan>) -> Vec<Vec<PartPlan>> {
    let mut runs: Vec<Vec<PartPlan>> = Vec::new();
    for plan in plans {
        if let Some(run) = runs.last_mut()
            && let Some(previous) = run.last()
            && previous.pos + 1 == plan.pos
            && previous.loc.slab == plan.loc.slab
            && previous.loc.offset + entry_len(u64::from(previous.loc.data_len)) == plan.loc.offset
        {
            run.push(plan);
        } else {
            runs.push(vec![plan]);
        }
    }
    runs
}

fn read_one_run(
    fds: &Mutex<HashMap<u32, Arc<File>>>,
    root: &Path,
    pool: &Arc<ReadBufferPool>,
    run: Vec<PartPlan>,
) -> RunRead {
    let started = Instant::now();
    let Some(slab) = run.first().map(|plan| plan.loc.slab) else {
        return RunRead {
            plans: run,
            parts: 0,
            read: Ok(Vec::new()),
            took: Duration::ZERO,
        };
    };
    let read = match open_slab(fds, root, slab, false) {
        Ok(file) => read_run(&file, pool, &run),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Ok(run.iter().map(|_| PartRead::Gone).collect())
        }
        Err(e) => Err(e),
    };
    RunRead {
        parts: run.len(),
        read: read.context(IoSnafu {
            action: "read",
            path: dat_path(root, slab),
        }),
        plans: run,
        took: started.elapsed(),
    }
}

fn expected_len(key: &BlockKey) -> u64 {
    let start = key.index.saturating_mul(key.block_size);
    start.saturating_add(key.block_size).min(key.object_size) - start.min(key.object_size)
}

fn read_buffer(
    file: &File,
    pool: &Arc<ReadBufferPool>,
    len: usize,
    offset: u64,
) -> std::io::Result<Option<ReadBuffer>> {
    let mut buffer = pool.take(len);
    match file.read_exact_at(&mut buffer.bytes[..len], offset) {
        Ok(()) => Ok(Some(buffer)),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e),
    }
}

/// Reads adjacent entries with a header read followed by one read spanning
/// only the chunks that cover their requested parts. Every returned
/// slice is backed by that spanning buffer and keeps it out of the pool.
/// Each entry is verified on its own, so one that does not verify leaves its
/// neighbours' results intact.
fn read_run(
    file: &File,
    pool: &Arc<ReadBufferPool>,
    run: &[PartPlan],
) -> std::io::Result<Vec<PartRead>> {
    let (Some(first), Some(last)) = (run.first(), run.last()) else {
        return Ok(Vec::new());
    };

    let mut first_page = [0u8; HEADER_LEN];
    match file.read_exact_at(&mut first_page, first.loc.offset) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof && run.len() > 1 => {
            return read_separately(file, pool, run);
        }
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Ok(vec![PartRead::Corrupt]);
        }
        Err(e) => return Err(e),
    }
    let region_start = first
        .loc
        .offset
        .checked_add(HEADER_LEN as u64)
        .and_then(|at| at.checked_add(first.covering.start as u64))
        .ok_or_else(|| std::io::Error::other("read offset is out of range"))?;
    let region_end = last
        .loc
        .offset
        .checked_add(HEADER_LEN as u64)
        .and_then(|at| at.checked_add(last.covering.end as u64))
        .ok_or_else(|| std::io::Error::other("read offset is out of range"))?;
    let region_len = usize::try_from(region_end.saturating_sub(region_start))
        .map_err(|_| std::io::Error::other("read length is out of range"))?;
    let Some(buffer) = read_buffer(file, pool, region_len, region_start)? else {
        if run.len() > 1 {
            return read_separately(file, pool, run);
        }
        return Ok(vec![PartRead::Corrupt]);
    };
    let region = buffer.into_bytes();
    Ok(run
        .iter()
        .enumerate()
        .map(|(index, plan)| {
            let page = if index == 0 {
                Some(&first_page[..])
            } else {
                plan.loc
                    .offset
                    .checked_sub(region_start)
                    .and_then(|at| usize::try_from(at).ok())
                    .and_then(|start| region.get(start..start.checked_add(HEADER_LEN)?))
            };
            page.and_then(|page| verified_part(page, &region, region_start, plan))
                .map_or(PartRead::Corrupt, PartRead::Verified)
        })
        .collect())
}

/// The requested part of `plan`'s entry, whose header page is `page` and
/// whose covering chunks lie in `region` (read from slab offset
/// `region_start`), when the header and those chunks verify.
fn verified_part(page: &[u8], region: &Bytes, region_start: u64, plan: &PartPlan) -> Option<Bytes> {
    let header = codec::decode_header(page)?;
    if header.data_len != plan.loc.data_len as usize {
        return None;
    }
    let data_start = plan
        .loc
        .offset
        .checked_add(HEADER_LEN as u64)?
        .checked_add(plan.covering.start as u64)?
        .checked_sub(region_start)
        .and_then(|at| usize::try_from(at).ok())?;
    let data = region.get(data_start..data_start.checked_add(plan.covering.len())?)?;
    if !codec::verify_part(page, &header, &plan.enc, &plan.covering, data) {
        return None;
    }
    let from = data_start + plan.part.start - plan.covering.start;
    let to = from.checked_add(plan.part.len())?;
    (to <= region.len()).then(|| region.slice(from..to))
}

fn read_separately(
    file: &File,
    pool: &Arc<ReadBufferPool>,
    run: &[PartPlan],
) -> std::io::Result<Vec<PartRead>> {
    let mut out = Vec::with_capacity(run.len());
    for plan in run {
        let mut read = read_run(file, pool, std::slice::from_ref(plan))?;
        out.push(read.pop().unwrap_or(PartRead::Corrupt));
    }
    Ok(out)
}

#[async_trait]
impl BlockStore for SlabStore {
    async fn put(&self, key: &BlockKey, bytes: Bytes, meta: &StoredMeta) -> Result<()> {
        let enc = key.encode();
        let entry_len = entry_len(bytes.len() as u64);
        ensure!(
            entry_len <= self.slab_bytes,
            BlockTooLargeSnafu {
                len: bytes.len(),
                max: usize::try_from(self.slab_bytes).unwrap_or(usize::MAX) - HEADER_LEN,
            }
        );
        codec::check_entry(enc.len(), bytes.len())?;
        let data_len = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
        let meta_enc = meta.encode();
        // No await between reserving and handing the reservation to the
        // blocking write: whatever happens to this future, the reservation
        // ends once the write does. A slab this call sealed gets its index
        // after the write.
        let (reservation, offset, sealed) = self.reserve(entry_len)?;
        let id = reservation.id;
        // Held to the end of this call, so a caller that admitted the entry
        // counts it once while the call runs.
        self.pending.fetch_add(entry_len, Ordering::Relaxed);
        let _pending = PendingBytes {
            counter: &self.pending,
            bytes: entry_len,
        };

        let fds = Arc::clone(&self.fds);
        let slabs = Arc::clone(&self.slabs);
        let root = self.root.clone();
        let joined = tokio::task::spawn_blocking(move || {
            // Encoded here rather than on the async thread: it copies the
            // block and checksums it. `check_entry` above means it cannot fail.
            // One positional write per entry: header, data and padding
            // together, page-aligned at both ends.
            let written = codec::encode_entry(&enc, &meta_enc, &bytes).and_then(|entry| {
                drop(bytes);
                let dat = dat_path(&root, id);
                let file = open_slab(&fds, &root, id, true).context(IoSnafu {
                    action: "write",
                    path: &dat,
                })?;
                // A discard marks the slab before it deletes the `.dat`, so a
                // write that opened it after the delete (creating it again)
                // sees the mark, and removes what it created.
                if slabs.lock().map.get(&id).is_none_or(|s| s.discarded) {
                    drop(file);
                    let _ = std::fs::remove_file(&dat);
                    fds.lock().remove(&id);
                    return Err(discarded_error(dat));
                }
                file.write_all_at(&entry, offset).context(IoSnafu {
                    action: "write",
                    path: dat,
                })
            });
            (written, reservation)
        })
        .await;
        // A panicked write dropped its reservation, which ended it.
        let (written, reservation) = match joined {
            Ok((written, reservation)) => (written, Some(reservation)),
            Err(e) => (Err(e).context(JoinSnafu), None),
        };

        let entry = written.is_ok().then(|| Written {
            key,
            meta,
            digest: key.digest(),
            offset,
            data_len,
        });
        let completed = reservation.map(|r| self.complete(r, entry));
        let written = match &completed {
            Some(c) if written.is_ok() && !c.listed => {
                Err(discarded_error(dat_path(&self.root, id)))
            }
            _ => written,
        };
        let (ready, superseded) = completed.map_or((None, None), |c| (c.ready, c.superseded));
        let ready_idx = self.persist_idx(ready).await;
        let sealed_idx = self.persist_idx(sealed).await;
        // The older copy is tombstoned, so it cannot come back after a restart
        // once this copy is removed.
        let tombstoned = match superseded {
            Some((slab, offset)) => self.tombstone(BTreeMap::from([(slab, vec![offset])])).await,
            None => Ok(()),
        };
        // The first error: the entry's own write, then the index writes, then
        // the older copy's tombstone. An index that failed is rebuilt by
        // scanning the slab at the next open.
        written.and(ready_idx).and(sealed_idx).and(tombstoned)
    }

    fn contains(&self, key: &BlockKey) -> bool {
        self.index.read().contains_key(&key.digest())
    }

    async fn get(&self, key: &BlockKey) -> Result<Option<Bytes>> {
        let d = key.digest();
        let Some(loc) = self.index.read().get(&d).cloned() else {
            return Ok(None);
        };
        let stored = u64::from(loc.data_len);
        self.get_copy(key, &d, loc, stored).await
    }

    async fn get_parts(&self, reads: &[(BlockKey, Range<u64>)]) -> Parts {
        self.get_parts_impl(reads).await
    }

    async fn remove(&self, key: &BlockKey) -> Result<()> {
        self.forget_durably(&[key.digest()]).await.map(|_| ())
    }

    /// Finds the objects' entries from each slab's entry list (shared, so no
    /// lock is held while walking them) rather than by enumerating block
    /// indexes, which for a huge object would be unbounded.
    async fn remove_objects(&self, objects: &[ObjectKey]) -> Result<u64> {
        if objects.is_empty() {
            return Ok(0);
        }
        let wanted: HashSet<&ObjectKey> = objects.iter().collect();
        let lists: Vec<Arc<Vec<IdxEntry>>> = self
            .slabs
            .lock()
            .map
            .values()
            .map(|s| Arc::clone(&s.entries))
            .collect();
        let digests: HashSet<Digest> = lists
            .iter()
            .flat_map(|l| l.iter())
            .filter(|e| wanted.contains(&*e.object))
            .map(|e| e.digest)
            .collect();
        let digests: Vec<Digest> = digests.into_iter().collect();
        self.forget_durably(&digests).await
    }

    fn resident_bytes(&self) -> u64 {
        self.resident.load(Ordering::Relaxed)
    }

    fn len(&self) -> u64 {
        self.index.read().len() as u64
    }

    fn is_empty(&self) -> bool {
        self.index.read().is_empty()
    }

    fn disk_bytes(&self) -> u64 {
        self.disk.load(Ordering::Relaxed)
    }

    fn pending_bytes(&self) -> u64 {
        self.pending.load(Ordering::Relaxed)
    }

    fn resident_blocks(&self, object: &ObjectKey) -> u64 {
        self.objects.lock().resident_blocks(object)
    }

    /// Takes slabs in [`SlabStore::reclaim_order`], weighed once and again
    /// only once that order is used up; when a fresh order has no slab that
    /// can go, the active slab, once idle.
    async fn reclaim(&self, heat: BlockHeat, over: &(dyn Fn() -> bool + Send + Sync)) -> Reclaimed {
        let mut out = Reclaimed::default();
        let mut order = VecDeque::new();
        loop {
            if !over() {
                return out;
            }
            let mut weighed = false;
            let taken = loop {
                if let Some(id) = order.pop_front() {
                    match self.reclaim_slab(Some(id)).await {
                        Ok(Some(freed)) => break Some(freed),
                        Ok(None) => continue,
                        Err(e) => {
                            out.error = Some(e);
                            return out;
                        }
                    }
                }
                if weighed {
                    break None;
                }
                weighed = true;
                match self.reclaim_order(Arc::clone(&heat)).await {
                    Ok(next) => order = next,
                    Err(e) => {
                        out.error = Some(e);
                        return out;
                    }
                }
            };
            let taken = match taken {
                Some(freed) => Some(freed),
                None => match self.reclaim_slab(None).await {
                    Ok(freed) => freed,
                    Err(e) => {
                        out.error = Some(e);
                        return out;
                    }
                },
            };
            match taken {
                Some(freed) => out.freed += freed,
                None => return out,
            }
        }
    }

    fn paths_under(&self, store: &str, prefix: &str) -> Vec<Arc<str>> {
        self.objects.lock().paths_under(store, prefix)
    }

    fn object_keys(&self, store: &str, path: &str) -> Vec<ObjectKey> {
        self.objects.lock().of_path(store, path)
    }

    fn object_info(&self, store: &str, path: &str, block_size: u64) -> Option<ObjectInfo> {
        self.objects.lock().info_of(store, path, block_size)
    }

    fn counters(&self) -> StoreCounters {
        StoreCounters {
            corrupt_reads: self.corrupt.load(Ordering::Relaxed),
            corrupt_bytes: self.corrupt_bytes.load(Ordering::Relaxed),
            quarantined_slabs: self.quarantined,
        }
    }

    /// Seals the active slab and writes its `.idx` once its writes finish, so a
    /// clean reopen reads indexes only.
    async fn close(&self) -> Result<()> {
        let ready = {
            let mut slabs = self.slabs.lock();
            let active = slabs.active.take();
            active.and_then(|id| {
                let s = slabs.map.get_mut(&id)?;
                s.seal();
                s.take_ready(id)
            })
        };
        self.persist_idx(ready).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The lengths of `found`, which print far shorter than the bytes.
    fn lens(found: &[Option<Bytes>]) -> Vec<Option<usize>> {
        found.iter().map(|b| b.as_ref().map(Bytes::len)).collect()
    }

    fn key(i: u64) -> BlockKey {
        BlockKey {
            store: "s3://bucket/".to_string(),
            path: "t/a.vortex".to_string(),
            object_size: 1 << 30,
            block_size: 1 << 20,
            index: i,
        }
    }

    /// Block 0 of a 2 MiB object stored at 512 KiB instead of 1 MiB, then a
    /// valid block 1 right after it in the same slab: a read of both drops
    /// only block 0, durably, and serves block 1.
    #[tokio::test]
    async fn wrong_length_entry_does_not_poison_an_adjacent_entry() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let store = SlabStore::open(tmp.path(), 8 << 20).await.expect("open");
        let block = |index| BlockKey {
            object_size: 2 << 20,
            ..key(index)
        };
        let (first, second) = (block(0), block(1));
        let valid = Bytes::from(
            (0..1 << 20)
                .map(|i| u8::try_from(i % 241).expect("byte"))
                .collect::<Vec<_>>(),
        );
        store
            .put(
                &first,
                Bytes::from(vec![0x51; 512 << 10]),
                &StoredMeta::NONE,
            )
            .await
            .expect("put short entry");
        store
            .put(&second, valid.clone(), &StoredMeta::NONE)
            .await
            .expect("put valid entry");
        let (a, b) = {
            let index = store.index.read();
            let a = index.get(&first.digest()).cloned().expect("first listed");
            let b = index.get(&second.digest()).cloned().expect("second listed");
            (a, b)
        };
        assert_eq!(a.slab, b.slab, "both entries are in one slab");
        assert_eq!(
            a.offset + entry_len(u64::from(a.data_len)),
            b.offset,
            "the entries are adjacent"
        );

        let parts = store
            .get_parts(&[(first.clone(), 0..1 << 20), (second.clone(), 0..1 << 20)])
            .await;

        assert!(parts.errors.is_empty(), "{:?}", parts.errors);
        assert_eq!(lens(&parts.found), vec![None, Some(1 << 20)]);
        assert!(
            parts.found[1] == Some(valid.clone()),
            "block 1 bytes differ"
        );
        assert_eq!(store.counters().corrupt_reads, 1);
        assert!(!store.contains(&first));
        assert!(store.contains(&second));
        store.close().await.expect("close");
        drop(store);
        let reopened = SlabStore::open(tmp.path(), 8 << 20).await.expect("reopen");
        assert!(!reopened.contains(&first), "the short entry came back");
        let found = reopened.get_parts(&[(second, 0..1 << 20)]).await.found;
        assert!(found == vec![Some(valid)], "block 1 after reopen");
    }

    /// A run whose slab cannot be opened fails alone: the other run's part is
    /// still returned, the error names the failed slab, and nothing is
    /// dropped.
    #[tokio::test]
    async fn failed_run_does_not_fail_other_runs() {
        let tmp = tempfile::tempdir().expect("tempdir");
        // One 1 MiB entry per slab.
        let store = SlabStore::open(tmp.path(), (1 << 20) + 8192)
            .await
            .expect("open");
        let bytes = Bytes::from(
            (0..1 << 20)
                .map(|i| u8::try_from(i % 233).expect("byte"))
                .collect::<Vec<_>>(),
        );
        let (bad, good) = (key(0), key(1));
        for k in [&bad, &good] {
            store
                .put(k, bytes.clone(), &StoredMeta::NONE)
                .await
                .expect("put");
        }
        let slab_of = |k: &BlockKey| store.index.read().get(&k.digest()).expect("listed").slab;
        let (bad_slab, good_slab) = (slab_of(&bad), slab_of(&good));
        assert_ne!(bad_slab, good_slab, "the entries are in two slabs");
        // A directory where the slab file was: opening it fails, and not
        // as a missing file.
        store.fds.lock().remove(&bad_slab);
        let bad_path = dat_path(&store.root, bad_slab);
        std::fs::rename(&bad_path, tmp.path().join("moved.dat")).expect("move slab");
        std::fs::create_dir(&bad_path).expect("dir in place of slab");

        let parts = store
            .get_parts(&[(bad.clone(), 0..4096), (good.clone(), 4096..8192)])
            .await;

        assert_eq!(lens(&parts.found), vec![None, Some(4096)]);
        assert!(
            parts.found[1] == Some(bytes.slice(4096..8192)),
            "good bytes"
        );
        assert_eq!(parts.errors.len(), 1, "{:?}", parts.errors);
        assert_eq!(parts.errors[0].parts, 1);
        let message = parts.errors[0].error.to_string();
        assert!(
            message.contains(&bad_path.display().to_string()),
            "{message}"
        );
        assert_eq!(store.counters().corrupt_reads, 0);
        assert!(store.contains(&bad), "a failed read dropped the block");
    }

    /// Empty, inverted and past-the-end parts are caller errors: they read
    /// nothing and drop nothing, alone or beside a valid part.
    #[tokio::test]
    #[expect(
        clippy::reversed_empty_ranges,
        reason = "an inverted part is under test"
    )]
    async fn invalid_parts_do_not_tombstone_entries() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let store = SlabStore::open(tmp.path(), 8 << 20).await.expect("open");
        let bytes = Bytes::from(
            (0..1 << 20)
                .map(|i| u8::try_from(i % 239).expect("byte"))
                .collect::<Vec<_>>(),
        );
        let keys = [key(0), key(1), key(2)];
        for k in &keys {
            store
                .put(k, bytes.clone(), &StoredMeta::NONE)
                .await
                .expect("put");
        }

        let parts = store
            .get_parts(&[
                (keys[0].clone(), 17..17),
                (keys[1].clone(), 23..11),
                (keys[2].clone(), 0..(1 << 20) + 1),
            ])
            .await;
        assert!(parts.errors.is_empty(), "{:?}", parts.errors);
        assert_eq!(lens(&parts.found), vec![None, None, None]);
        let parts = store
            .get_parts(&[(keys[0].clone(), 5..5), (keys[1].clone(), 100..200)])
            .await;
        assert_eq!(lens(&parts.found), vec![None, Some(100)]);
        assert!(parts.found[1] == Some(bytes.slice(100..200)), "valid part");

        assert_eq!(store.counters().corrupt_reads, 0);
        for k in &keys {
            assert!(store.contains(k));
            assert_eq!(store.get(k).await.expect("get"), Some(bytes.clone()));
        }
        store.close().await.expect("close");
        drop(store);
        let reopened = SlabStore::open(tmp.path(), 8 << 20).await.expect("reopen");
        assert_eq!(reopened.len(), 3, "an invalid part tombstoned an entry");
    }

    #[tokio::test]
    async fn returned_bytes_hold_their_read_buffer_out_of_the_pool() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let store = SlabStore::open(tmp.path(), 8 << 20).await.expect("open");
        let bytes = Bytes::from(
            (0..1 << 20)
                .map(|i| u8::try_from(i % 251).expect("byte"))
                .collect::<Vec<_>>(),
        );
        let key = key(0);
        store
            .put(&key, bytes.clone(), &StoredMeta::NONE)
            .await
            .expect("put");

        let held_range = 123u64..16_507;
        let held_start = usize::try_from(held_range.start).expect("held start");
        let held_end = usize::try_from(held_range.end).expect("held end");
        let mut held = store
            .get_parts(&[(key.clone(), held_range.clone())])
            .await
            .found;
        let held = held.pop().flatten().expect("resident part");
        assert_eq!(held, bytes.slice(held_start..held_end));
        assert_eq!(store.read_pool.available(), 0, "held buffer was pooled");

        for i in 0..READ_POOL_BUFFERS * 4 {
            let start = (i % 16) * (64 << 10) + (i * 97) % 4096;
            let end = start + 1024;
            let range = u64::try_from(start).expect("start")..u64::try_from(end).expect("end");
            let mut got = store.get_parts(&[(key.clone(), range)]).await.found;
            let got = got.pop().flatten().expect("resident part");
            assert_eq!(got, bytes.slice(start..end), "later hit {i}");
        }
        assert_eq!(held, bytes.slice(held_start..held_end));
        assert_eq!(
            store.read_pool.available(),
            1,
            "later hits did not reuse a buffer"
        );
        drop(held);
        assert_eq!(
            store.read_pool.available(),
            2,
            "held buffer was not returned"
        );
    }

    /// A read that looked a block up, then found that copy's slab gone or the
    /// copy corrupt, drops the block only while the index still lists that
    /// copy. A newer copy written in between (after a reclaim took the old
    /// slab, say) stays listed and served. The read is driven from the stale
    /// location directly, so the interleaving is exact.
    #[tokio::test]
    async fn stale_read_does_not_forget_a_newer_copy() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let store = SlabStore::open(tmp.path(), 8 << 20).await.expect("open");
        let k = key(1);
        let d = k.digest();
        let new = Bytes::from(vec![2u8; 10_000]);
        store
            .put(&k, Bytes::from(vec![1u8; 10_000]), &StoredMeta::NONE)
            .await
            .expect("put");
        let stale = store.index.read().get(&d).cloned().expect("listed");
        store
            .put(&k, new.clone(), &StoredMeta::NONE)
            .await
            .expect("put again");
        let current = store.index.read().get(&d).cloned().expect("listed");
        assert_ne!(
            (stale.slab, stale.offset),
            (current.slab, current.offset),
            "the second put wrote a second copy"
        );

        // The stale copy's slab is gone.
        let gone = Loc {
            slab: stale.slab + 1000,
            ..stale.clone()
        };
        assert_eq!(
            store.get_copy(&k, &d, gone, 10_000).await.expect("read"),
            None
        );
        assert_eq!(store.len(), 1, "a vanished stale copy dropped the block");

        // The stale copy is corrupt.
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(dat_path(&store.root, stale.slab))
            .expect("open slab");
        let at = stale.offset + HEADER_LEN as u64;
        let mut byte = [0u8; 1];
        file.read_exact_at(&mut byte, at).expect("read byte");
        byte[0] ^= 0xFF;
        file.write_all_at(&byte, at).expect("corrupt byte");
        drop(file);
        assert_eq!(
            store.get_copy(&k, &d, stale, 10_000).await.expect("read"),
            None
        );
        assert_eq!(
            store.counters().corrupt_reads,
            1,
            "the corrupt copy was seen"
        );

        assert_eq!(store.len(), 1, "a corrupt stale copy dropped the block");
        assert_eq!(store.get(&k).await.expect("get"), Some(new));

        // Nor did the corrupt stale copy tombstone the new one. Closed, so the
        // reopen reads the index (a rescan stops at the corrupt first entry).
        store.close().await.expect("close");
        drop(store);
        let reopened = SlabStore::open(tmp.path(), 8 << 20).await.expect("reopen");
        assert_eq!(reopened.len(), 1);
        assert_eq!(reopened.counters().quarantined_slabs, 0);
    }
}
