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

//! The node cache: one block store and one budget shared by every object
//! store it wraps.
//!
//! A read of a cacheable object is served in fixed-size blocks keyed by the
//! store's namespace, the path, the object size, the block size and the
//! block index:
//!
//! 1. **Every block is claimed** in the in-flight map first, under the
//!    object's eviction generation. A block another read (or a prefetch) of
//!    the same generation holds is awaited, for at most [`SHARED_FETCH_WAIT`];
//!    a wait that times out, or whose fetch failed, reads those blocks from
//!    the store directly. Claiming before looking up is what makes one fetch
//!    per block hold: a block leaves the map only after its entry is written.
//!    A fetch from before an eviction (a delete, rename, copy or upload of the
//!    object through the wrapper) is never handed to a read that started after
//!    it.
//! 2. **Claimed blocks are looked up** in the block store, which verifies the
//!    entry's key, length and CRC32C. An entry that does not verify, and any
//!    block-store error, is counted and treated as a miss. A hit is handed to
//!    reads waiting on the block.
//! 3. **Claimed blocks that missed are fetched**: consecutive ones go out as
//!    one ranged GET, split into parts of at most [`MAX_GET_BYTES`], with
//!    [`GETS_IN_FLIGHT`] parts of one read in flight. Each part runs as its
//!    own task, so a cancelled read does not cancel a fetch other reads wait
//!    on. A part whose read was cancelled gives its blocks up one shared wait
//!    later, so a GET that never completes holds no claim once no read waits
//!    on it.
//! 4. **Entry writes happen after the read.** A fetched block is handed to
//!    waiting readers and queued for writing; the read returns without waiting
//!    for the write. A full queue drops the fill (counted); a failed write is
//!    counted. Neither delays nor fails the read. A block stays in the in-flight
//!    map, with its bytes, until its write finishes, so a read arriving in
//!    between gets the bytes without a second fetch.
//!
//! Admission:
//!
//! - **Scans bypass admission.** A read of more than an eighth of the
//!   budget, or of more than [`MAX_MATERIALIZED_BYTES`] whatever the budget
//!   (a scan), is passed to the wrapped store, so its bytes stream through
//!   without being held. It writes nothing and is not counted in the
//!   frequency sketch, so it cannot push a hot set out.
//! - **Entry writes have two lanes**, demand first: a demand fill is dropped
//!   and counted when its queue is full; a prefetch fill waits for room.
//!
//! Prefetch ([`NodeCache::prefetch`], in `prefetch`) runs in the background,
//! fills only free room and yields to queries.
//!
//! Space and eviction:
//!
//! - **Every block a query reads is counted** in a frequency sketch
//!   ([`crate::frequency`]); the block store reclaims the slabs whose live
//!   blocks are worth the least per byte by that estimate first. The sketch's
//!   periodic halving runs on a blocking thread, not on the read.
//! - **The budget follows the volume.** The capacity (high watermark) is the
//!   configured budget, or less when the volume would otherwise keep less
//!   than its reserve free: [`VOLUME_RESERVE_PERMILLE`] of its total, at
//!   least [`VOLUME_RESERVE_MIN_BYTES`]. The low watermark is
//!   [`LOW_WATERMARK_PERMILLE`] of the budget, or the capacity less
//!   [`VOLUME_REFILL_PERMILLE`] of the volume when that is less, so a cache
//!   other writers pushed below the reserve frees that much more. The volume
//!   is read again every [`BUDGET_CHECK_INTERVAL`], by a background task and
//!   by entry writes. A write that would cross the high watermark, or a check
//!   that finds the cache over it, reclaims until the cache is at the low
//!   watermark.
//! - **Eviction by reference.** [`NodeCache::evict_unreferenced`] removes the
//!   objects under a prefix of a store that its caller no longer references.
//! - **Eviction before change.** A delete, rename, copy or upload through the
//!   wrapper of a cacheable path first evicts the path durably, and fills of
//!   it are refused until the store's call returns. An eviction that cannot
//!   be made durable turns the cache off for the namespace (reads go to the
//!   store, nothing is filled) and the store's call still goes ahead.
//! - **Per-object ordering.** Every read of an object holds a ticket with the
//!   object's eviction generation, and its entry writes carry it. Evicting an
//!   object advances the generation and waits for writes of the object already
//!   running; a write carrying an older generation, or one made while the
//!   object is held for a change, is dropped, so no fill that started before
//!   an eviction lands after it.

mod prefetch;

pub use prefetch::Prefetched;

use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use futures::future::{self, BoxFuture, Either, FutureExt};
use futures::{StreamExt, TryStreamExt, stream};
use object_store::path::Path;
use object_store::{GetOptions, ObjectStore};
use parking_lot::Mutex;
use snafu::{ResultExt, ensure};
use tokio::sync::{Notify, Semaphore, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use self::prefetch::PrefetchJob;
use crate::Result;
use crate::block_store::codec::{HEADER_LEN, entry_len};
use crate::block_store::{
    BlockHeat, BlockKey, BlockStore, Digest, ObjectInfo, ObjectKey, StoredMeta,
};
use crate::error::{InvalidConfigSnafu, NoRuntimeSnafu};
use crate::frequency::FrequencySketch;
use crate::lru::Lru;
use crate::store::{Cacheable, CachedObjectStore, Sizes};
use crate::volume::{Reserve, Statvfs, VolumeSpace, Watermarks, watermarks};

/// Size of the blocks objects are cached in.
const BLOCK_BYTES: u64 = 1 << 20;
/// How long a read waits on another read's fetch of the same block before
/// reading the store itself.
const SHARED_FETCH_WAIT: Duration = Duration::from_secs(1);
/// Largest ranged GET a run of missing blocks is split into.
const MAX_GET_BYTES: u64 = 8 << 20;
/// Ranged GETs one read keeps in flight.
const GETS_IN_FLIGHT: usize = 16;
/// Entry writes queued per lane behind the read path.
const WRITE_QUEUE: usize = 64;
/// Entry writes running at once.
const CONCURRENT_WRITES: usize = 4;
/// How often the volume's free space is read again.
const BUDGET_CHECK_INTERVAL: Duration = Duration::from_secs(1);
/// Free space, in thousandths of the volume's total, the cache leaves to
/// other writers.
const VOLUME_RESERVE_PERMILLE: u64 = 150;
/// The least free space the cache leaves to other writers.
const VOLUME_RESERVE_MIN_BYTES: u64 = 4 << 30;
/// Free space, in thousandths of the volume's total, a cache pushed under the
/// reserve gives back on top of it.
const VOLUME_REFILL_PERMILLE: u64 = 50;
/// The low watermark, in thousandths of the budget.
const LOW_WATERMARK_PERMILLE: u64 = 900;
/// Paths per namespace remembered as written through the wrapper; the
/// oldest is forgotten first.
pub(crate) const MAX_WRITTEN: usize = 1 << 20;
/// Blocks one prefetch GET covers at most.
const PREFETCH_CHUNK_BLOCKS: u64 = 4;
/// Prefetch GETs one prefetch job keeps in flight.
const PREFETCH_CHUNKS_IN_FLIGHT: usize = 8;
/// Prefetch jobs (one object each) run at once, so a job whose store stalls
/// does not hold up the others.
const PREFETCH_JOBS_IN_FLIGHT: usize = 4;
/// Prefetch jobs queued or running at most; a prefetch past them is declined.
const PREFETCH_QUEUE_JOBS: usize = 4096;
/// How long after the last query read ends a prefetch chunk may start.
const PREFETCH_QUIET: Duration = Duration::from_millis(20);
/// A query's disk read slower than this makes prefetch back off.
const PREFETCH_SLOW_READ: Duration = Duration::from_millis(20);
/// How long prefetch backs off after a slow disk read.
const PREFETCH_BACKOFF: Duration = Duration::from_secs(1);
/// The most bytes one read served through the cache holds in memory; a
/// larger read streams from the wrapped store, whatever the budget.
pub(crate) const MAX_MATERIALIZED_BYTES: u64 = 64 << 20;
/// A read of more than this fraction of the budget is a scan.
const SCAN_SHARE: u64 = 8;
/// Block-store lookups one read issues at once.
const LOOKUPS_IN_FLIGHT: usize = 16;

/// Where the cache lives and how much it may hold.
#[derive(Debug, Clone)]
pub struct NodeCacheConfig {
    /// Directory for the cache's files; created if missing, readable by its
    /// owner only.
    pub dir: PathBuf,
    /// Bytes on disk the cache may use at most, entry headers and space not
    /// yet reclaimed included. Less when the volume is short of free space.
    pub budget_bytes: u64,
    /// Where the free space of the volume holding `dir` is read from.
    pub volume: Arc<dyn VolumeSpace>,
}

impl NodeCacheConfig {
    /// A cache in `dir` of at most `budget_bytes`, reading the volume's free
    /// space from the operating system.
    #[must_use]
    pub fn new(dir: impl Into<PathBuf>, budget_bytes: u64) -> Self {
        Self {
            dir: dir.into(),
            budget_bytes,
            volume: Arc::new(Statvfs),
        }
    }
}

/// The cache's internal settings. Defaults are the crate's constants; only
/// the crate's tests change them, through [`crate::testing`].
#[derive(Debug, Clone)]
pub struct Tuning {
    pub block_bytes: u64,
    pub shared_fetch_wait: Duration,
    pub max_get_bytes: u64,
    pub gets_in_flight: usize,
    pub write_queue: usize,
    pub concurrent_writes: usize,
    pub budget_check_interval: Duration,
    pub volume_reserve: Reserve,
    pub low_watermark_permille: u64,
    /// Reads of more than this many bytes are scans; `None` for an eighth of
    /// the budget. Never more than [`MAX_MATERIALIZED_BYTES`].
    pub scan_bytes: Option<u64>,
    pub prefetch_chunk_blocks: u64,
    pub prefetch_chunks_in_flight: usize,
    pub prefetch_jobs_in_flight: usize,
    pub prefetch_queue_jobs: usize,
    pub prefetch_quiet: Duration,
    pub prefetch_slow_read: Duration,
    pub prefetch_backoff: Duration,
}

impl Default for Tuning {
    fn default() -> Self {
        Self {
            block_bytes: BLOCK_BYTES,
            shared_fetch_wait: SHARED_FETCH_WAIT,
            max_get_bytes: MAX_GET_BYTES,
            gets_in_flight: GETS_IN_FLIGHT,
            write_queue: WRITE_QUEUE,
            concurrent_writes: CONCURRENT_WRITES,
            budget_check_interval: BUDGET_CHECK_INTERVAL,
            volume_reserve: Reserve {
                permille: VOLUME_RESERVE_PERMILLE,
                min_bytes: VOLUME_RESERVE_MIN_BYTES,
                refill_permille: VOLUME_REFILL_PERMILLE,
            },
            low_watermark_permille: LOW_WATERMARK_PERMILLE,
            scan_bytes: None,
            prefetch_chunk_blocks: PREFETCH_CHUNK_BLOCKS,
            prefetch_chunks_in_flight: PREFETCH_CHUNKS_IN_FLIGHT,
            prefetch_jobs_in_flight: PREFETCH_JOBS_IN_FLIGHT,
            prefetch_queue_jobs: PREFETCH_QUEUE_JOBS,
            prefetch_quiet: PREFETCH_QUIET,
            prefetch_slow_read: PREFETCH_SLOW_READ,
            prefetch_backoff: PREFETCH_BACKOFF,
        }
    }
}

/// Counters since the cache was opened, and its current contents.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NodeCacheStats {
    /// Blocks reads served through the cache asked for.
    pub accesses: u64,
    /// Of `accesses`, blocks served without this read fetching them: from
    /// resident blocks, or from a fetch another read or a prefetch had in
    /// flight.
    pub hits: u64,
    /// Block bytes reads fetched from object stores to fill the cache.
    pub fetched_bytes_query: u64,
    /// Block bytes prefetch fetched from object stores.
    pub fetched_bytes_prefetch: u64,
    /// Block bytes written into the cache.
    pub fill_bytes: u64,
    /// Bytes on disk freed to stay within the capacity.
    pub evicted_bytes_size: u64,
    /// Block bytes evicted because their object is no longer referenced, or
    /// was deleted, renamed or overwritten through the wrapper.
    pub evicted_bytes_unreferenced: u64,
    /// Block bytes dropped because their entry did not verify.
    pub evicted_bytes_corrupt: u64,
    /// Cache errors, each served from the object store instead: block-store
    /// read errors, entries that did not verify (checksum, length, key),
    /// failed entry writes and evictions. [`NodeCache::last_error`] holds the
    /// most recent error's message.
    pub fallbacks: u64,
    /// Reads passed to the object store untouched: HEAD requests, conditional
    /// reads, paths the `cacheable` predicate rejects, empty or invalid
    /// ranges, ranges past the object's end, and reads of a namespace the
    /// cache is off for.
    pub bypass_reads: u64,
    /// Reads that admitted nothing: scans, passed to the object store.
    pub admission_bypass_reads: u64,
    /// Fetched blocks not written: the demand write queue was full, the
    /// budget had no room that could be reclaimed, or the object was evicted
    /// while the block was being fetched.
    pub dropped_fills: u64,
    /// Waits on another read's fetch that timed out and read the store.
    pub shared_wait_timeouts: u64,
    /// Block bytes prefetch did not fetch because they did not fit the free
    /// room below the low watermark.
    pub prefetch_declined_bytes: u64,
    /// Slabs dropped when the cache was opened because one of their files did
    /// not verify or could not be read; their blocks are read from the store.
    pub quarantined_slabs: u64,
    /// Bytes on disk the cache's files take, headers, padding and removed
    /// entries not yet reclaimed included.
    pub used_bytes: u64,
    /// The current capacity (high watermark): a write that would take
    /// `used_bytes` over it reclaims first.
    pub capacity_bytes: u64,
    /// The current low watermark: what reclaiming brings `used_bytes` down to.
    pub low_watermark_bytes: u64,
    /// Bytes of block data resident.
    pub resident_bytes: u64,
    /// Blocks resident.
    pub entries: u64,
}

#[derive(Debug, Default)]
struct Counters {
    accesses: AtomicU64,
    hits: AtomicU64,
    fetched_query: AtomicU64,
    fetched_prefetch: AtomicU64,
    fill_bytes: AtomicU64,
    evicted_size: AtomicU64,
    evicted_unreferenced: AtomicU64,
    evicted_corrupt: AtomicU64,
    cache_errors: AtomicU64,
    bypass_reads: AtomicU64,
    admission_bypass_reads: AtomicU64,
    dropped_fills: AtomicU64,
    shared_wait_timeouts: AtomicU64,
    prefetch_declined_bytes: AtomicU64,
}

fn add(counter: &AtomicU64, n: u64) {
    counter.fetch_add(n, Ordering::Relaxed);
}

/// The state of one block being fetched.
#[derive(Debug, Clone)]
enum Slot {
    Pending,
    Ready(Bytes),
    Failed,
}

/// A block another read is fetching: its position in this read and the
/// fetch's state.
type Waiter = (usize, watch::Receiver<Slot>);

/// A block a read leads, looked up: its position, its claim, and its bytes
/// on a hit.
type Looked = (usize, SlotGuard, Option<Bytes>);

/// A block's digest and the eviction generation of the read that claimed it:
/// reads of another generation never share a fetch.
type FlightKey = (Digest, u64);

#[derive(Debug, Default)]
struct InFlight {
    next_id: u64,
    map: HashMap<FlightKey, (u64, watch::Receiver<Slot>)>,
}

/// The reads and entry writes of one object in progress, how many times the
/// object was evicted while any were, and the changes (deletes, renames,
/// uploads) of it in progress, during which nothing of it is written.
#[derive(Debug, Default)]
struct Gate {
    tickets: u64,
    putting: u64,
    held: u64,
    generation: u64,
}

/// Namespace → path → gate, for objects with reads in progress.
#[derive(Debug, Default)]
struct Gates {
    stores: HashMap<Arc<str>, HashMap<Arc<str>, Gate>>,
}

impl Gates {
    fn get_mut(&mut self, store: &str, path: &str) -> Option<&mut Gate> {
        self.stores.get_mut(store)?.get_mut(path)
    }

    /// Drops the gate once nothing holds it.
    fn prune(&mut self, store: &str, path: &str) {
        let Some(paths) = self.stores.get_mut(store) else {
            return;
        };
        if paths
            .get(path)
            .is_some_and(|g| g.tickets == 0 && g.putting == 0 && g.held == 0)
        {
            paths.remove(path);
        }
        if paths.is_empty() {
            self.stores.remove(store);
        }
    }
}

/// What the cache keeps per namespace: the store last wrapped under it (for
/// prefetch), the object sizes known, the paths uploaded through the wrapper,
/// and whether the cache is off for it.
#[derive(Debug)]
pub(crate) struct Namespace {
    pub(crate) name: Arc<str>,
    inner: Mutex<Option<Arc<dyn ObjectStore>>>,
    pub(crate) sizes: Mutex<Sizes>,
    /// Paths written through the wrapper, at most [`MAX_WRITTEN`], the
    /// least recently written forgotten first.
    written: Mutex<Lru<Arc<str>, ()>>,
    /// Set when an eviction could not be made durable: the namespace's reads
    /// go to the store and nothing of it is written, until the cache is
    /// opened again.
    disabled: AtomicBool,
}

impl Namespace {
    fn new(name: Arc<str>) -> Self {
        Self {
            name,
            inner: Mutex::new(None),
            sizes: Mutex::new(Sizes::default()),
            written: Mutex::new(Lru::new(MAX_WRITTEN)),
            disabled: AtomicBool::new(false),
        }
    }

    pub(crate) fn is_disabled(&self) -> bool {
        self.disabled.load(Ordering::SeqCst)
    }

    pub(crate) fn note_written(&self, path: &Path) {
        self.written.lock().insert(Arc::from(path.as_ref()), ());
    }
}

/// Paths of one namespace held for a change through the wrapper (a delete,
/// rename, copy or upload): each was evicted, and no fill of it is written
/// until it is released. Dropping the hold releases every path still held.
#[derive(Debug)]
pub(crate) struct Hold {
    core: Arc<Core>,
    ns: Arc<Namespace>,
    paths: Mutex<HashMap<Arc<str>, u64>>,
}

impl Hold {
    /// Holds `paths`, waits for entry writes of them already running, then
    /// removes their blocks durably. Returns the block bytes removed. An
    /// error making the removal durable turns the cache off for the
    /// namespace, is counted and recorded as the last error, and is returned;
    /// the paths stay held either way.
    pub(crate) async fn evict(&self, paths: Vec<Arc<str>>) -> Result<u64> {
        if paths.is_empty() {
            return Ok(0);
        }
        {
            let mut gates = self.core.gates.lock();
            let mut held = self.paths.lock();
            let by_path = gates.stores.entry(Arc::clone(&self.ns.name)).or_default();
            for path in &paths {
                let gate = by_path.entry(Arc::clone(path)).or_default();
                gate.held += 1;
                gate.generation += 1;
                *held.entry(Arc::clone(path)).or_default() += 1;
            }
        }
        for path in &paths {
            self.core.until_no_puts(&self.ns.name, path).await;
        }
        self.core.remove_paths(&self.ns, &paths).await
    }

    /// Releases one hold of `path`; fills of the object from reads that
    /// started while it was held stay dropped.
    pub(crate) fn release(&self, path: &str) {
        let mut held = self.paths.lock();
        let Some(n) = held.get_mut(path) else {
            return;
        };
        *n -= 1;
        if *n == 0 {
            held.remove(path);
        }
        drop(held);
        self.core.unhold(&self.ns.name, path, 1);
    }
}

impl Drop for Hold {
    fn drop(&mut self) {
        let held = std::mem::take(&mut *self.paths.lock());
        for (path, n) in held {
            self.core.unhold(&self.ns.name, &path, n);
        }
    }
}

/// When a cache's watermarks were last computed, and what they were.
#[derive(Debug, Clone, Copy)]
struct Limits {
    marks: Watermarks,
    at: Option<Instant>,
}

/// When query reads last ended, and until when a slow disk read holds
/// prefetch back.
#[derive(Debug, Default)]
struct Foreground {
    last_end: Option<Instant>,
    slow_until: Option<Instant>,
}

/// The directory lock the cache holds; nothing on platforms without one.
#[cfg(unix)]
type DirLock = crate::dir_lock::DirLock;
#[cfg(not(unix))]
type DirLock = std::convert::Infallible;

#[derive(Debug)]
struct Core {
    config: NodeCacheConfig,
    tuning: Tuning,
    store: Arc<dyn BlockStore>,
    /// The runtime the cache was opened on: background work (entry writes,
    /// the sketch's halving) is spawned on it.
    runtime: tokio::runtime::Handle,
    inflight: Mutex<InFlight>,
    counters: Counters,
    /// Entry writes queued or running, and prefetch jobs queued or running.
    pending: AtomicU64,
    idle: Notify,
    sketch: Arc<FrequencySketch>,
    limits: Mutex<Limits>,
    /// Bytes of entry writes admitted under the budget and not yet finished.
    admitted: Mutex<u64>,
    /// One reclaim at a time, so concurrent writes do not each reclaim.
    reclaim: tokio::sync::Mutex<()>,
    gates: Mutex<Gates>,
    /// Signalled when an entry write of any object finishes.
    gate_changed: Notify,
    /// Per-namespace state, created by the first call that names the
    /// namespace and kept while the cache is open.
    namespaces: Mutex<HashMap<Arc<str>, Arc<Namespace>>>,
    /// The message of the most recent error counted in `cache_errors`.
    last_error: Mutex<Option<String>>,
    /// Query reads waiting on the store or the disk.
    fg_waiting: AtomicU64,
    foreground: Mutex<Foreground>,
    /// Signalled when a query read ends, and on close.
    fg_changed: Notify,
    /// Queued prefetch jobs, oldest first.
    prefetch_queue: Mutex<VecDeque<PrefetchJob>>,
    /// Entry bytes of prefetch jobs queued or running that are not yet
    /// admitted, skipped or dropped: later prefetches count them against the
    /// room below the low watermark.
    prefetch_unadmitted: AtomicU64,
    /// Prefetch jobs queued or running.
    prefetch_jobs: AtomicU64,
    prefetch_wake: Notify,
    /// Cancelled when the cache is closed or dropped: queued entry writes are
    /// skipped and prefetch stops.
    closed: CancellationToken,
    /// The directory lock; `None` for a caller-built block store. Held by
    /// the core rather than the [`NodeCache`], so it is released only once
    /// the last task that can touch the cache's files (an entry write that
    /// was running when the cache was dropped) is gone: a cache opened on the
    /// directory afterwards never shares its files with such a write.
    _dir_lock: Option<DirLock>,
}

/// The entry-write queues, in priority order.
#[derive(Debug, Clone)]
struct Lanes {
    demand: mpsc::Sender<WriteJob>,
    prefetch: mpsc::Sender<WriteJob>,
}

/// A process-wide disk cache of immutable object-store blocks. Built once per
/// node and shared by every store it [wraps](NodeCache::wrap).
#[derive(Debug)]
pub struct NodeCache {
    core: Arc<Core>,
    lanes: Lanes,
    budget_task: JoinHandle<()>,
    prefetch_task: JoinHandle<()>,
}

impl Drop for NodeCache {
    fn drop(&mut self) {
        self.core.close_signal();
        self.budget_task.abort();
        self.prefetch_task.abort();
    }
}

/// Leadership of one block's fetch. Dropping it before the block is ready
/// marks the fetch failed (waiters read the store); dropping it in any case
/// removes the block from the in-flight map.
#[derive(Debug)]
struct SlotGuard {
    core: Arc<Core>,
    key: FlightKey,
    id: u64,
    tx: watch::Sender<Slot>,
}

impl SlotGuard {
    fn ready(&self, bytes: Bytes) {
        self.tx.send_replace(Slot::Ready(bytes));
    }
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        self.tx.send_if_modified(|s| {
            let pending = matches!(s, Slot::Pending);
            if pending {
                *s = Slot::Failed;
            }
            pending
        });
        let mut inflight = self.core.inflight.lock();
        if inflight
            .map
            .get(&self.key)
            .is_some_and(|(id, _)| *id == self.id)
        {
            inflight.map.remove(&self.key);
        }
    }
}

/// A read's hold on its object's gate, with the eviction generation it saw;
/// the read's entry writes share it.
#[derive(Debug)]
struct Ticket {
    core: Arc<Core>,
    ns: Arc<Namespace>,
    path: Arc<str>,
    generation: u64,
}

impl Drop for Ticket {
    fn drop(&mut self) {
        let mut gates = self.core.gates.lock();
        if let Some(g) = gates.get_mut(&self.ns.name, &self.path) {
            g.tickets = g.tickets.saturating_sub(1);
        }
        gates.prune(&self.ns.name, &self.path);
    }
}

/// An entry write of an object in progress; an eviction of the object waits
/// for it.
#[derive(Debug)]
struct PutGuard<'a> {
    ticket: &'a Ticket,
}

impl Drop for PutGuard<'_> {
    fn drop(&mut self) {
        let core = &self.ticket.core;
        {
            let mut gates = core.gates.lock();
            if let Some(g) = gates.get_mut(&self.ticket.ns.name, &self.ticket.path) {
                g.putting = g.putting.saturating_sub(1);
            }
        }
        core.gate_changed.notify_waiters();
    }
}

/// An entry write's share of the budget, held until the write is done. The
/// block store counts the entry on disk from the moment it reserves it, and
/// reports it as pending until the put returns; committed disk use subtracts
/// the pending bytes, so the entry is counted once throughout.
#[derive(Debug)]
struct Admitted {
    core: Arc<Core>,
    bytes: u64,
}

impl Drop for Admitted {
    fn drop(&mut self) {
        let mut admitted = self.core.admitted.lock();
        *admitted = admitted.saturating_sub(self.bytes);
    }
}

/// Counts queued or running work (an entry write, a prefetch job) until it
/// is done or dropped.
#[derive(Debug)]
struct Pending(Arc<Core>);

impl Pending {
    fn new(core: &Arc<Core>) -> Self {
        core.pending.fetch_add(1, Ordering::SeqCst);
        Self(Arc::clone(core))
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        if self.0.pending.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.0.idle.notify_waiters();
        }
    }
}

/// A query read in progress; prefetch does not start chunks while any is.
#[derive(Debug)]
pub(crate) struct ForegroundGuard(Arc<Core>);

impl Drop for ForegroundGuard {
    fn drop(&mut self) {
        let core = &self.0;
        core.foreground.lock().last_end = Some(Instant::now());
        if core.fg_waiting.fetch_sub(1, Ordering::SeqCst) == 1 {
            core.fg_changed.notify_waiters();
        }
    }
}

#[derive(Debug)]
struct WriteJob {
    key: BlockKey,
    bytes: Bytes,
    meta: Arc<StoredMeta>,
    /// Room admitted before the block was fetched; `None` to admit at write
    /// time.
    reserved: Option<Admitted>,
    ticket: Arc<Ticket>,
    _slot: SlotGuard,
    _pending: Pending,
}

/// Why a cached read did not produce bytes.
#[derive(Debug)]
pub(crate) enum ReadError {
    /// The object store's own error, returned to the caller as is.
    Store(object_store::Error),
    /// The store returned another size than the one the blocks are keyed by:
    /// the caller reads the store directly.
    SizeChanged,
}

/// The bytes of a read served through the cache, one `Bytes` per block
/// touched, and the object's metadata as reported by a GET the read made
/// (`None` when every block was resident or fetched by another read).
pub(crate) struct Served {
    pub parts: Vec<Bytes>,
    pub fetched_meta: Option<Arc<StoredMeta>>,
}

/// Blocks by their position in a read.
type Positioned = Vec<(usize, Bytes)>;

/// Blocks a read fetched, and the object's metadata as the GET reported it.
type Fetched = (Positioned, Option<Arc<StoredMeta>>);

/// One object a read is for.
pub(crate) struct ReadCtx<'a> {
    pub ns: &'a Arc<Namespace>,
    pub path: &'a Path,
    pub size: u64,
    pub inner: &'a Arc<dyn ObjectStore>,
}

/// The key of object `path` of `size` bytes in `ns`, cached in blocks of
/// `block_bytes`.
fn object_key(ns: &Namespace, path: &Path, size: u64, block_bytes: u64) -> ObjectKey {
    ObjectKey {
        store: Arc::clone(&ns.name),
        path: Arc::from(path.as_ref()),
        object_size: size,
        block_size: block_bytes,
    }
}

fn to_usize(n: u64) -> usize {
    usize::try_from(n).unwrap_or(usize::MAX)
}

/// Byte range of block `index` (a block of the object, so `index *
/// block_bytes < size`) of an object of `size` bytes. The end saturates
/// before it is capped at `size`, so it is exact for sizes up to `u64::MAX`.
fn block_span(block_bytes: u64, size: u64, index: u64) -> Range<u64> {
    let start = index.saturating_mul(block_bytes);
    start..start.saturating_add(block_bytes).min(size)
}

/// Length of block `index` of an object of `size` bytes ([`block_span`]).
fn block_len(block_bytes: u64, size: u64, index: u64) -> u64 {
    let span = block_span(block_bytes, size, index);
    span.end - span.start
}

/// Splits `items` into runs of at most `max_len`, each item `adjacent` to the
/// one before it in its run.
fn runs<T: Copy>(
    items: impl IntoIterator<Item = T>,
    max_len: usize,
    adjacent: impl Fn(T, T) -> bool,
) -> Vec<Vec<T>> {
    let mut out: Vec<Vec<T>> = Vec::new();
    for item in items {
        match out.last_mut() {
            Some(run)
                if run.len() < max_len && run.last().is_some_and(|&last| adjacent(last, item)) =>
            {
                run.push(item);
            }
            _ => out.push(vec![item]),
        }
    }
    out
}

/// Returns the first `Some` that `ready` gives, asking again each time
/// `notify` is signalled. The wait is registered before each check, so a
/// signal between the check and the wait is not lost.
async fn wait_until<T>(notify: &Notify, mut ready: impl FnMut() -> Option<T>) -> T {
    loop {
        let notified = notify.notified();
        let mut notified = std::pin::pin!(notified);
        notified.as_mut().enable();
        if let Some(found) = ready() {
            return found;
        }
        notified.await;
    }
}

fn join_error(e: &tokio::task::JoinError) -> object_store::Error {
    object_store::Error::Generic {
        store: "NodeCache",
        source: format!("a block fetch task failed: {e}").into(),
    }
}

/// One ranged GET covering `blocks` (block indexes) of an object, returned
/// as one `Bytes` per block (slices of the response, no copies), with the
/// object's metadata as the store reported it.
async fn fetch_blocks(
    inner: &Arc<dyn ObjectStore>,
    path: &Path,
    size: u64,
    block_bytes: u64,
    blocks: Range<u64>,
) -> Result<(Vec<Bytes>, StoredMeta), ReadError> {
    let start = blocks.start.saturating_mul(block_bytes);
    // Saturates before the cap: `blocks.end` is one past the object's last
    // block, so the product can exceed `u64::MAX` for the largest objects.
    let end = blocks.end.saturating_mul(block_bytes).min(size);
    let result = inner
        .get_opts(path, GetOptions::new().with_range(Some(start..end)))
        .await
        .map_err(ReadError::Store)?;
    if result.meta.size != size {
        return Err(ReadError::SizeChanged);
    }
    let meta = StoredMeta {
        e_tag: result.meta.e_tag.clone(),
        last_modified: Some(result.meta.last_modified),
    };
    let bytes = result.bytes().await.map_err(ReadError::Store)?;
    if bytes.len() as u64 != end - start {
        return Err(ReadError::SizeChanged);
    }
    let blocks = blocks
        .map(|i| {
            let span = block_span(block_bytes, size, i);
            bytes.slice(to_usize(span.start - start)..to_usize(span.end - start))
        })
        .collect();
    Ok((blocks, meta))
}

/// Opens the cache in `config.dir` with `tuning`: the block store under
/// `slabs/`, the directory locked against other caches.
///
/// # Errors
///
/// See [`NodeCache::open`].
pub async fn open_with(config: NodeCacheConfig, tuning: Tuning) -> Result<Arc<NodeCache>> {
    validate(&config, &tuning)?;
    #[cfg(unix)]
    {
        use crate::block_store::slab::SlabStore;
        use crate::error::JoinSnafu;
        // Locked before anything in the directory is touched: the slab
        // store's open removes temporary files another cache could be
        // writing.
        let dir = config.dir.clone();
        let lock = tokio::task::spawn_blocking(move || DirLock::acquire(&dir))
            .await
            .context(JoinSnafu)??;
        let slab_bytes = slab_bytes(config.budget_bytes, tuning.block_bytes);
        let store = SlabStore::open(&config.dir, slab_bytes).await?;
        let cache = NodeCache::build(config, tuning, store, Some(lock))?;
        // The capacity reported from the start reflects the volume's free
        // space, not the budget alone.
        cache.core.watermarks(true).await;
        Ok(cache)
    }
    #[cfg(not(unix))]
    {
        crate::error::UnsupportedPlatformSnafu.fail()
    }
}

/// A cache over a block store the caller built (tests wrap the slab store
/// with injected faults). `config.dir` is not locked; it is only where the
/// volume's free space is read.
///
/// # Errors
///
/// An invalid configuration, or no Tokio runtime to run entry writes on.
pub fn with_block_store(
    config: NodeCacheConfig,
    tuning: Tuning,
    store: Arc<dyn BlockStore>,
) -> Result<Arc<NodeCache>> {
    validate(&config, &tuning)?;
    NodeCache::build(config, tuning, store, None)
}

impl NodeCache {
    /// Opens the cache in `config.dir`, creating the directory (mode `0700`)
    /// if needed. A slab whose files do not verify is dropped and the rest
    /// are kept ([`NodeCacheStats::quarantined_slabs`]).
    ///
    /// # Errors
    ///
    /// An invalid configuration; [`crate::Error::Locked`] when another cache
    /// has the directory open (including a dropped cache in this process
    /// whose last entry writes are still finishing); an I/O error creating
    /// or listing the directory (for example, a directory that is not
    /// writable); [`crate::Error::UnsupportedPlatform`] on a platform without
    /// Unix file APIs.
    pub async fn open(config: NodeCacheConfig) -> Result<Arc<Self>> {
        open_with(config, Tuning::default()).await
    }

    fn build(
        config: NodeCacheConfig,
        tuning: Tuning,
        store: Arc<dyn BlockStore>,
        lock: Option<DirLock>,
    ) -> Result<Arc<Self>> {
        let runtime = tokio::runtime::Handle::try_current().context(NoRuntimeSnafu)?;
        let (demand, demand_rx) = mpsc::channel(tuning.write_queue);
        let (prefetch, prefetch_rx) = mpsc::channel(tuning.write_queue);
        let lanes = Lanes { demand, prefetch };
        let cap = config.budget_bytes;
        let low_permille = tuning.low_watermark_permille;
        let reserve = tuning.volume_reserve;
        let sketch = Arc::new(FrequencySketch::new(
            config.budget_bytes / tuning.block_bytes,
        ));
        let core = Arc::new(Core {
            config,
            tuning,
            store,
            runtime: runtime.clone(),
            inflight: Mutex::new(InFlight::default()),
            counters: Counters::default(),
            pending: AtomicU64::new(0),
            idle: Notify::new(),
            sketch,
            limits: Mutex::new(Limits {
                marks: watermarks(cap, low_permille, 0, None, reserve),
                at: None,
            }),
            admitted: Mutex::new(0),
            reclaim: tokio::sync::Mutex::new(()),
            gates: Mutex::new(Gates::default()),
            gate_changed: Notify::new(),
            namespaces: Mutex::new(HashMap::new()),
            last_error: Mutex::new(None),
            fg_waiting: AtomicU64::new(0),
            foreground: Mutex::new(Foreground::default()),
            fg_changed: Notify::new(),
            prefetch_queue: Mutex::new(VecDeque::new()),
            prefetch_unadmitted: AtomicU64::new(0),
            prefetch_jobs: AtomicU64::new(0),
            prefetch_wake: Notify::new(),
            closed: CancellationToken::new(),
            _dir_lock: lock,
        });
        runtime.spawn(run_writer(Arc::clone(&core), demand_rx, prefetch_rx));
        let budget_task = runtime.spawn(run_budget(Arc::downgrade(&core)));
        let prefetch_task = runtime.spawn(prefetch::run(Arc::clone(&core), lanes.clone()));
        Ok(Arc::new(Self {
            core,
            lanes,
            budget_task,
            prefetch_task,
        }))
    }

    /// An object store that serves reads of `cacheable` paths of `inner`
    /// through this cache and passes everything else to `inner`.
    ///
    /// `namespace` identifies the backend `inner` reads in every block key,
    /// so two backends never share entries; it is also how
    /// [`NodeCache::prefetch`] and [`NodeCache::evict_unreferenced`] find
    /// `inner`'s objects. Derive it from everything that decides which bytes
    /// a path names: the scheme, the endpoint, the bucket or container, and
    /// the prefix. Two stores with the same URL on different endpoints need
    /// different namespaces. Never put credentials in it: it is written to
    /// disk in every entry.
    ///
    /// Wrapping a namespace again replaces the store its prefetches read
    /// from: the last `inner` wrapped is used.
    #[must_use]
    pub fn wrap(
        self: &Arc<Self>,
        namespace: &str,
        inner: Arc<dyn ObjectStore>,
        cacheable: Cacheable,
    ) -> Arc<dyn ObjectStore> {
        let ns = self.core.namespace(namespace);
        *ns.inner.lock() = Some(Arc::clone(&inner));
        Arc::new(CachedObjectStore::new(
            Arc::clone(self),
            ns,
            inner,
            cacheable,
        ))
    }

    /// Counters and current contents.
    #[must_use]
    pub fn stats(&self) -> NodeCacheStats {
        let c = &self.core.counters;
        let load = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let store = &self.core.store;
        let store_counters = store.counters();
        let marks = self.core.limits.lock().marks;
        NodeCacheStats {
            accesses: load(&c.accesses),
            hits: load(&c.hits),
            fetched_bytes_query: load(&c.fetched_query),
            fetched_bytes_prefetch: load(&c.fetched_prefetch),
            fill_bytes: load(&c.fill_bytes),
            evicted_bytes_size: load(&c.evicted_size),
            evicted_bytes_unreferenced: load(&c.evicted_unreferenced),
            evicted_bytes_corrupt: load(&c.evicted_corrupt) + store_counters.corrupt_bytes,
            fallbacks: load(&c.cache_errors) + store_counters.corrupt_reads,
            bypass_reads: load(&c.bypass_reads),
            admission_bypass_reads: load(&c.admission_bypass_reads),
            dropped_fills: load(&c.dropped_fills),
            shared_wait_timeouts: load(&c.shared_wait_timeouts),
            prefetch_declined_bytes: load(&c.prefetch_declined_bytes),
            quarantined_slabs: store_counters.quarantined_slabs,
            used_bytes: store.disk_bytes(),
            capacity_bytes: marks.high,
            low_watermark_bytes: marks.low,
            resident_bytes: store.resident_bytes(),
            entries: store.len(),
        }
    }

    /// The message of the most recent cache error counted in
    /// [`NodeCacheStats::fallbacks`] (an entry that did not verify is counted
    /// without one), or `None` when there was none.
    #[must_use]
    pub fn last_error(&self) -> Option<String> {
        self.core.last_error.lock().clone()
    }

    /// The size of the blocks the cache reads, stores and prefetches; a
    /// prefetch of any byte of a block fetches the whole block.
    #[must_use]
    pub fn block_bytes(&self) -> u64 {
        self.core.tuning.block_bytes
    }

    /// The free space the cache leaves to other writers on a volume of
    /// `volume_total` bytes: its capacity is 0 while the volume has no more
    /// than this free beside the cache's own files.
    #[must_use]
    pub fn volume_reserve_bytes(&self, volume_total: u64) -> u64 {
        self.core.tuning.volume_reserve.bytes(volume_total)
    }

    /// True when `path` was uploaded through a store wrapped under
    /// `namespace` (a successful put, or a completed multipart upload) and
    /// has not been deleted, renamed or overwritten through it since. At most
    /// the [`MAX_WRITTEN`] most recent uploads of a namespace are
    /// remembered; an older one reads as not written here.
    #[must_use]
    pub fn written_here(&self, namespace: &str, path: &Path) -> bool {
        self.core
            .existing_namespace(namespace)
            .is_some_and(|ns| ns.written.lock().contains(path.as_ref()))
    }

    /// Records that the object `path` in `namespace` is `size` bytes, so a
    /// read of it needs no HEAD to find its blocks: a caller that knows the
    /// sizes of the objects it reads (from a manifest) notes them, after a
    /// restart in particular. A size that is wrong only sends reads of the
    /// object to the store: every fill checks the size the store reports. At
    /// most 65,536 sizes are kept per namespace, the least recently used
    /// dropped first.
    pub fn note_size(&self, namespace: &str, path: &Path, size: u64) {
        self.core
            .namespace(namespace)
            .sizes
            .lock()
            .insert_size(path, size);
    }

    /// Returns once no entry write and no prefetch is queued or running.
    pub async fn wait_idle(&self) {
        let core = &self.core;
        wait_until(&core.idle, || {
            (core.pending.load(Ordering::SeqCst) == 0).then_some(())
        })
        .await;
    }

    /// Closes the cache cleanly: stops prefetch, skips entry writes still
    /// queued, waits for those running, then seals the slab being written
    /// and writes its index, so the next open reads indexes only and does not
    /// rescan it. Reads keep working afterwards, without filling the cache.
    /// Dropping the cache without closing it leaves the slab being written to
    /// be rescanned at the next open.
    ///
    /// # Errors
    ///
    /// An I/O error writing an index; the slab is then rescanned at the next
    /// open.
    pub async fn close(&self) -> Result<()> {
        self.core.close_signal();
        self.wait_idle().await;
        self.core.store.close().await
    }

    /// Removes the cached objects under `prefix` (the empty path for the
    /// whole namespace) of the store wrapped under `namespace` for which
    /// `keep` returns false, and returns the block bytes removed. Meant to be
    /// called on every refresh of the metastore's head, with `keep` answering
    /// from the current snapshot and any retention the caller keeps for older
    /// snapshots: the cache keeps no retention of its own.
    ///
    /// Considered are the objects with resident blocks and those with reads
    /// in progress. Removal waits for entry writes of an object already
    /// running, is durable, and a fill of the object that started before it
    /// is dropped instead of written. Removed blocks' disk space is reclaimed
    /// with their slabs. An error making the removal durable turns the cache
    /// off for the namespace and is counted in [`NodeCacheStats::fallbacks`];
    /// it is not returned.
    pub async fn evict_unreferenced(
        &self,
        namespace: &str,
        prefix: &Path,
        keep: impl Fn(&Path) -> bool,
    ) -> u64 {
        let core = &self.core;
        // Created if missing: blocks from before a restart belong to a
        // namespace no call may have named yet.
        let ns = core.namespace(namespace);
        let mut paths: HashSet<Arc<str>> = core
            .store
            .paths_under(namespace, prefix.as_ref())
            .into_iter()
            .collect();
        let reading: Vec<Arc<str>> = core
            .gates
            .lock()
            .stores
            .get(namespace)
            .map(|reading| reading.keys().cloned().collect())
            .unwrap_or_default();
        paths.extend(reading);
        // A path that does not parse cannot be asked about, so it is kept.
        let due: Vec<Arc<str>> = paths
            .into_iter()
            .filter(|p| {
                Path::parse(&**p).is_ok_and(|path| path.prefix_matches(prefix) && !keep(&path))
            })
            .collect();
        let hold = Hold {
            core: Arc::clone(core),
            ns,
            paths: Mutex::new(HashMap::new()),
        };
        hold.evict(due).await.unwrap_or(0)
    }

    #[cfg(test)]
    pub(crate) fn namespace_for_test(&self, name: &str) -> Arc<Namespace> {
        self.core.namespace(name)
    }

    /// A hold on paths of `ns`, empty until [`Hold::evict`] adds them.
    pub(crate) fn hold(&self, ns: &Arc<Namespace>) -> Hold {
        Hold {
            core: Arc::clone(&self.core),
            ns: Arc::clone(ns),
            paths: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn count_bypass(&self) {
        add(&self.core.counters.bypass_reads, 1);
    }

    pub(crate) fn count_scan(&self) {
        add(&self.core.counters.admission_bypass_reads, 1);
    }

    /// Reads larger than this many bytes are scans: passed to the store, not
    /// admitted, never held in memory.
    pub(crate) fn scan_threshold(&self) -> u64 {
        self.core
            .tuning
            .scan_bytes
            .unwrap_or(self.core.config.budget_bytes / SCAN_SHARE)
            .min(MAX_MATERIALIZED_BYTES)
    }

    /// Marks a read as waiting on the store or the disk until the guard
    /// drops; prefetch starts no chunk meanwhile.
    pub(crate) fn foreground(&self) -> ForegroundGuard {
        self.core.fg_waiting.fetch_add(1, Ordering::SeqCst);
        ForegroundGuard(Arc::clone(&self.core))
    }

    /// The size and metadata of an object with resident blocks: what lets a
    /// restarted cache serve an object without asking the store.
    pub(crate) fn resident_info(&self, store: &str, path: &Path) -> Option<ObjectInfo> {
        self.core
            .store
            .object_info(store, path.as_ref(), self.core.tuning.block_bytes)
    }

    fn max_blocks_per_get(&self) -> usize {
        to_usize((self.core.tuning.max_get_bytes / self.core.tuning.block_bytes).max(1))
    }

    /// The bytes of `range` (non-empty, within the object) of `ctx`'s object,
    /// as one `Bytes` per block touched, in order.
    ///
    /// Boxed so callers inside `async_trait` methods see a `Send` future at a
    /// concrete lifetime (the streams of borrowing closures inside defeat the
    /// compiler's higher-ranked `Send` inference otherwise).
    pub(crate) fn read<'a>(
        &'a self,
        ctx: &'a ReadCtx<'a>,
        range: Range<u64>,
    ) -> BoxFuture<'a, Result<Served, ReadError>> {
        self.read_blocks(ctx, range).boxed()
    }

    async fn read_blocks(&self, ctx: &ReadCtx<'_>, range: Range<u64>) -> Result<Served, ReadError> {
        let b = self.core.tuning.block_bytes;
        let first = range.start / b;
        let last = (range.end - 1) / b;
        let object = object_key(ctx.ns, ctx.path, ctx.size, b);
        let keys: Vec<BlockKey> = (first..=last).map(|i| object.block(i)).collect();
        let span = |j: usize| block_span(b, ctx.size, first + j as u64);
        add(&self.core.counters.accesses, keys.len() as u64);

        // The ticket comes before any lookup, so an eviction of the object
        // that starts after it fences this read's writes.
        let ticket = self.core.enter(ctx.ns, ctx.path.as_ref());
        // Claim every block before looking it up. A block leaves the in-flight
        // map only after its entry write finished, so a read that claims a
        // block either waits on the fetch in progress or finds the entry on
        // disk: never a second fetch.
        let (leading, waiting) = self.core.claim(&keys, true, ticket.generation);
        let mut blocks: Vec<Option<Bytes>> = vec![None; keys.len()];
        let keys_ref = &keys;
        let looked: Vec<Looked> = stream::iter(leading)
            .map(|(j, slot)| async move {
                let started = Instant::now();
                let found = self
                    .core
                    .lookup(&keys_ref[j], block_len(b, ctx.size, first + j as u64))
                    .await;
                self.core.note_disk_read(started.elapsed());
                (j, slot, found)
            })
            .buffered(LOOKUPS_IN_FLIGHT)
            .collect()
            .await;
        let mut hit = 0;
        let mut to_fetch = Vec::new();
        for (j, slot, found) in looked {
            match found {
                Some(bytes) => {
                    // Hand the resident block to reads waiting on this one.
                    slot.ready(bytes.clone());
                    hit += 1;
                    blocks[j] = Some(bytes);
                }
                None => to_fetch.push((j, slot)),
            }
        }

        let (fetched, shared) = futures::join!(
            self.fill(ctx, &keys, to_fetch, &ticket),
            self.core.await_shared(waiting)
        );
        let (fetched, mut fetched_meta) = fetched?;
        for (j, bytes) in fetched {
            blocks[j] = Some(bytes);
        }
        let mut unresolved = Vec::new();
        for (j, got) in shared {
            match got {
                Some(bytes) => {
                    hit += 1;
                    blocks[j] = Some(bytes);
                }
                None => unresolved.push(j),
            }
        }
        add(&self.core.counters.hits, hit);
        if !unresolved.is_empty() {
            let (direct, meta) = self.fetch_direct(ctx, first, &unresolved).await?;
            for (j, bytes) in direct {
                blocks[j] = Some(bytes);
            }
            fetched_meta = fetched_meta.or(meta);
        }

        let parts = blocks
            .into_iter()
            .enumerate()
            .map(|(j, blk)| {
                let s = span(j);
                // Every position was filled above or the read returned an error.
                let blk = blk.ok_or_else(|| {
                    ReadError::Store(object_store::Error::Generic {
                        store: "NodeCache",
                        source: format!(
                            "block {} of '{}' was not read",
                            first + j as u64,
                            ctx.path
                        )
                        .into(),
                    })
                })?;
                let from = range.start.max(s.start) - s.start;
                let to = range.end.min(s.end) - s.start;
                Ok(blk.slice(to_usize(from)..to_usize(to)))
            })
            .collect::<Result<Vec<Bytes>, ReadError>>()?;
        Ok(Served {
            parts,
            fetched_meta,
        })
    }

    /// Fetches the blocks this read leads, fills waiting readers and queues
    /// the entry writes.
    async fn fill(
        &self,
        ctx: &ReadCtx<'_>,
        keys: &[BlockKey],
        lead: Vec<(usize, SlotGuard)>,
        ticket: &Arc<Ticket>,
    ) -> Result<Fetched, ReadError> {
        if lead.is_empty() {
            return Ok((Vec::new(), None));
        }
        let positions: Vec<usize> = lead.iter().map(|(j, _)| *j).collect();
        let mut guards: HashMap<usize, SlotGuard> = lead.into_iter().collect();
        let parts: Vec<Vec<(usize, BlockKey, SlotGuard)>> =
            runs(positions, self.max_blocks_per_get(), |a, b| a + 1 == b)
                .into_iter()
                .map(|run| {
                    run.into_iter()
                        .filter_map(|j| guards.remove(&j).map(|g| (j, keys[j].clone(), g)))
                        .collect()
                })
                .collect();
        // Dropped with this read. A part whose read is gone (the query was
        // cancelled) keeps fetching for one more shared wait, as long as any
        // read already waiting on it waits, and then gives its blocks up.
        let (_reader, reader_alive) = watch::channel(());
        let filled: Vec<(Positioned, Arc<StoredMeta>)> = stream::iter(parts)
            .map(|part| {
                let task = FillPart {
                    core: Arc::clone(&self.core),
                    writes: self.lanes.demand.clone(),
                    inner: Arc::clone(ctx.inner),
                    path: ctx.path.clone(),
                    size: ctx.size,
                    part,
                    reader: reader_alive.clone(),
                    ticket: Arc::clone(ticket),
                };
                async move {
                    tokio::spawn(task.run())
                        .await
                        .unwrap_or_else(|e| Err(ReadError::Store(join_error(&e))))
                }
            })
            .buffered(self.core.tuning.gets_in_flight)
            .try_collect()
            .await?;
        let meta = filled.first().map(|(_, m)| Arc::clone(m));
        Ok((filled.into_iter().flat_map(|(b, _)| b).collect(), meta))
    }

    /// Reads blocks from the store without the cache: blocks whose shared
    /// fetch timed out or failed.
    async fn fetch_direct(
        &self,
        ctx: &ReadCtx<'_>,
        first: u64,
        positions: &[usize],
    ) -> Result<Fetched, ReadError> {
        let b = self.core.tuning.block_bytes;
        let fetched: Vec<(Positioned, Option<StoredMeta>)> = stream::iter(runs(
            positions.iter().copied(),
            self.max_blocks_per_get(),
            |a, b| a + 1 == b,
        ))
        .map(|run| async move {
            let (Some(&lo), Some(&hi)) = (run.first(), run.last()) else {
                return Ok((Vec::new(), None));
            };
            let blocks = first + lo as u64..first + hi as u64 + 1;
            let (bytes, meta) = fetch_blocks(ctx.inner, ctx.path, ctx.size, b, blocks).await?;
            Ok::<_, ReadError>((run.into_iter().zip(bytes).collect(), Some(meta)))
        })
        .buffered(self.core.tuning.gets_in_flight)
        .try_collect()
        .await?;
        let meta = fetched.iter().find_map(|(_, m)| m.clone()).map(Arc::new);
        Ok((fetched.into_iter().flat_map(|(b, _)| b).collect(), meta))
    }
}

/// One ranged GET of consecutive blocks this read leads; runs as its own task.
struct FillPart {
    core: Arc<Core>,
    writes: mpsc::Sender<WriteJob>,
    inner: Arc<dyn ObjectStore>,
    path: Path,
    size: u64,
    part: Vec<(usize, BlockKey, SlotGuard)>,
    /// Closed when the read that started this part is dropped.
    reader: watch::Receiver<()>,
    ticket: Arc<Ticket>,
}

impl FillPart {
    async fn run(self) -> Result<(Positioned, Arc<StoredMeta>), ReadError> {
        let FillPart {
            core,
            writes,
            inner,
            path,
            size,
            part,
            mut reader,
            ticket,
        } = self;
        let (Some(lo), Some(hi)) = (part.first(), part.last()) else {
            return Ok((Vec::new(), Arc::new(StoredMeta::NONE)));
        };
        let blocks = lo.1.index..hi.1.index + 1;
        let fetch = fetch_blocks(&inner, &path, size, core.tuning.block_bytes, blocks);
        // Every read waiting on these blocks when their reader went away gives
        // up within one shared wait, so a GET that never completes holds the
        // blocks no longer than that. Dropping the guards fails the slots, and
        // later readers lead a fetch of their own.
        let abandoned = async {
            while reader.changed().await.is_ok() {}
            tokio::time::sleep(core.tuning.shared_fetch_wait).await;
        };
        let (bytes, meta) = match future::select(std::pin::pin!(fetch), std::pin::pin!(abandoned))
            .await
        {
            Either::Left((fetched, _)) => fetched?,
            Either::Right(((), _)) => {
                return Err(ReadError::Store(object_store::Error::Generic {
                    store: "NodeCache",
                    source: format!("the read of '{path}' that started this fetch was cancelled")
                        .into(),
                }));
            }
        };
        add(
            &core.counters.fetched_query,
            bytes.iter().map(|b| b.len() as u64).sum(),
        );
        let meta = Arc::new(meta);
        let mut out = Vec::with_capacity(bytes.len());
        for ((j, key, slot), block) in part.into_iter().zip(bytes) {
            slot.ready(block.clone());
            let job = WriteJob {
                key,
                bytes: block.clone(),
                meta: Arc::clone(&meta),
                reserved: None,
                ticket: Arc::clone(&ticket),
                _slot: slot,
                _pending: Pending::new(&core),
            };
            // Never wait for room: a fill that does not fit the queue is
            // dropped so the read is not delayed by disk writes.
            if writes.try_send(job).is_err() {
                add(&core.counters.dropped_fills, 1);
            }
            out.push((j, block));
        }
        Ok((out, meta))
    }
}

impl Core {
    /// Cancels `closed`, stops prefetch and wakes everything waiting on it.
    fn close_signal(&self) {
        self.closed.cancel();
        self.prefetch_queue.lock().clear();
        self.fg_changed.notify_waiters();
        self.prefetch_wake.notify_waiters();
    }

    fn is_closed(&self) -> bool {
        self.closed.is_cancelled()
    }

    /// Records the time a query's disk read took; a slow one holds prefetch
    /// back for the back-off period.
    fn note_disk_read(&self, took: Duration) {
        if took > self.tuning.prefetch_slow_read {
            let until = Instant::now() + self.tuning.prefetch_backoff;
            let mut fg = self.foreground.lock();
            fg.slow_until = Some(fg.slow_until.map_or(until, |t| t.max(until)));
        }
    }

    /// The namespace's state, created if this is its first use.
    fn namespace(&self, name: &str) -> Arc<Namespace> {
        let mut namespaces = self.namespaces.lock();
        if let Some(ns) = namespaces.get(name) {
            return Arc::clone(ns);
        }
        let name: Arc<str> = Arc::from(name);
        let ns = Arc::new(Namespace::new(Arc::clone(&name)));
        namespaces.insert(name, Arc::clone(&ns));
        ns
    }

    fn existing_namespace(&self, name: &str) -> Option<Arc<Namespace>> {
        self.namespaces.lock().get(name).cloned()
    }

    /// Counts a cache error and records its message as the last one.
    fn record_error(&self, message: String) {
        add(&self.counters.cache_errors, 1);
        *self.last_error.lock() = Some(message);
    }

    /// The resident block, verified, or `None`. Errors and entries of the
    /// wrong length are counted and read as misses.
    async fn lookup(&self, key: &BlockKey, expected_len: u64) -> Option<Bytes> {
        match self.store.get(key).await {
            Ok(Some(bytes)) if bytes.len() as u64 == expected_len => Some(bytes),
            Ok(Some(bytes)) => {
                add(&self.counters.evicted_corrupt, bytes.len() as u64);
                self.record_error(format!(
                    "A cached block of '{}' is {} bytes instead of {expected_len}",
                    key.path,
                    bytes.len()
                ));
                if let Err(e) = self.store.remove(key).await {
                    self.record_error(e.to_string());
                }
                None
            }
            Ok(None) => None,
            Err(e) => {
                self.record_error(e.to_string());
                None
            }
        }
    }

    /// Counts a query's read of each block in the frequency sketch. When a
    /// read completes the sketch's sample period, its halving is spawned on a
    /// blocking thread: it walks every counter.
    fn record_reads(&self, digests: &[Digest]) {
        let mut due = false;
        for d in digests {
            due |= self.sketch.record(d);
        }
        if due {
            let sketch = Arc::clone(&self.sketch);
            drop(self.runtime.spawn_blocking(move || sketch.halve()));
        }
    }

    /// Claims the blocks for a caller holding a ticket of eviction
    /// `generation`, counting a read of each in the frequency sketch when
    /// `record`: returns those this caller leads and those it waits on.
    fn claim(
        self: &Arc<Self>,
        keys: &[BlockKey],
        record: bool,
        generation: u64,
    ) -> (Vec<(usize, SlotGuard)>, Vec<Waiter>) {
        let digests: Vec<Digest> = keys.iter().map(BlockKey::digest).collect();
        if record {
            self.record_reads(&digests);
        }
        let mut lead = Vec::new();
        let mut wait = Vec::new();
        let mut inflight = self.inflight.lock();
        for (j, digest) in digests.into_iter().enumerate() {
            let key = (digest, generation);
            if let Some((_, rx)) = inflight.map.get(&key) {
                wait.push((j, rx.clone()));
                continue;
            }
            let (tx, rx) = watch::channel(Slot::Pending);
            inflight.next_id += 1;
            let id = inflight.next_id;
            inflight.map.insert(key, (id, rx));
            lead.push((
                j,
                SlotGuard {
                    core: Arc::clone(self),
                    key,
                    id,
                    tx,
                },
            ));
        }
        (lead, wait)
    }

    /// A ticket on the object's gate, created if no read of it is in progress.
    fn enter(self: &Arc<Self>, ns: &Arc<Namespace>, path: &str) -> Arc<Ticket> {
        let mut gates = self.gates.lock();
        let paths = gates.stores.entry(Arc::clone(&ns.name)).or_default();
        let path: Arc<str> = match paths.get_key_value(path) {
            Some((k, _)) => Arc::clone(k),
            None => Arc::from(path),
        };
        let gate = paths.entry(Arc::clone(&path)).or_default();
        gate.tickets += 1;
        let generation = gate.generation;
        Arc::new(Ticket {
            core: Arc::clone(self),
            ns: Arc::clone(ns),
            path,
            generation,
        })
    }

    /// Starts an entry write under `ticket`, or `None` when the object was
    /// evicted since the ticket was taken, is held for a change, or its
    /// namespace is off.
    fn begin_put<'a>(&self, ticket: &'a Ticket) -> Option<PutGuard<'a>> {
        let mut gates = self.gates.lock();
        let gate = gates.get_mut(&ticket.ns.name, &ticket.path)?;
        if gate.generation != ticket.generation || gate.held > 0 || ticket.ns.is_disabled() {
            return None;
        }
        gate.putting += 1;
        Some(PutGuard { ticket })
    }

    /// True when the object was evicted since `ticket` was taken, or its
    /// namespace is off.
    fn evicted_since(&self, ticket: &Ticket) -> bool {
        ticket.ns.is_disabled()
            || self
                .gates
                .lock()
                .get_mut(&ticket.ns.name, &ticket.path)
                .is_none_or(|g| g.generation != ticket.generation)
    }

    /// Releases `n` holds of the object, advancing its generation so fills of
    /// reads that started while it was held stay dropped.
    fn unhold(&self, store: &str, path: &str, n: u64) {
        let mut gates = self.gates.lock();
        if let Some(gate) = gates.get_mut(store, path) {
            gate.held = gate.held.saturating_sub(n);
            gate.generation += 1;
        }
        gates.prune(store, path);
    }

    /// Removes every resident block of `paths` of `ns`, durably, and forgets
    /// their sizes and that they were written here. Returns the block bytes
    /// removed. A removal that is not durable turns the cache off for the
    /// namespace: its blocks could be listed again after a restart.
    async fn remove_paths(&self, ns: &Namespace, paths: &[Arc<str>]) -> Result<u64> {
        {
            let mut sizes = ns.sizes.lock();
            let mut written = ns.written.lock();
            for path in paths {
                sizes.remove_str(path);
                written.remove(&**path);
            }
        }
        let objects: Vec<ObjectKey> = paths
            .iter()
            .flat_map(|path| self.store.object_keys(&ns.name, path))
            .collect();
        match self.store.remove_objects(&objects).await {
            Ok(bytes) => {
                add(&self.counters.evicted_unreferenced, bytes);
                Ok(bytes)
            }
            Err(e) => {
                self.disable(ns, &e);
                Err(e)
            }
        }
    }

    /// Turns the cache off for `ns` after an eviction could not be made
    /// durable: reads go to the store, fills in progress are dropped.
    fn disable(&self, ns: &Namespace, cause: &crate::Error) {
        ns.disabled.store(true, Ordering::SeqCst);
        self.record_error(format!(
            "Failed to evict objects of namespace '{}' from the node cache, so the cache is off for that namespace until it is opened again: {cause}",
            ns.name
        ));
        let mut gates = self.gates.lock();
        if let Some(paths) = gates.stores.get_mut(&*ns.name) {
            for gate in paths.values_mut() {
                gate.generation += 1;
            }
        }
    }

    /// Waits until no entry write of the object is running.
    async fn until_no_puts(&self, store: &str, path: &str) {
        wait_until(&self.gate_changed, || {
            self.gates
                .lock()
                .get_mut(store, path)
                .is_none_or(|g| g.putting == 0)
                .then_some(())
        })
        .await;
    }

    /// Waits, until one shared deadline, for blocks other reads are fetching.
    /// `None` for a block whose fetch failed or did not finish in time.
    async fn await_shared(&self, wait: Vec<Waiter>) -> Vec<(usize, Option<Bytes>)> {
        if wait.is_empty() {
            return Vec::new();
        }
        let deadline = Instant::now() + self.tuning.shared_fetch_wait;
        let results = futures::future::join_all(wait.into_iter().map(|(j, mut rx)| async move {
            match tokio::time::timeout_at(deadline, rx.wait_for(|s| !matches!(s, Slot::Pending)))
                .await
            {
                Ok(Ok(slot)) => match &*slot {
                    Slot::Ready(bytes) => (j, Some(bytes.clone()), false),
                    Slot::Pending | Slot::Failed => (j, None, false),
                },
                // The leader went away without a result.
                Ok(Err(_)) => (j, None, false),
                Err(_) => (j, None, true),
            }
        }))
        .await;
        let timeouts = results
            .iter()
            .filter(|(_, _, timed_out)| *timed_out)
            .count();
        add(&self.counters.shared_wait_timeouts, timeouts as u64);
        results
            .into_iter()
            .map(|(j, bytes, _)| (j, bytes))
            .collect()
    }

    /// Writes one entry, reclaiming space first when the budget needs it
    /// (unless room was admitted before the fetch). A write whose object was
    /// evicted since its read started is dropped.
    async fn write(self: &Arc<Self>, job: WriteJob) {
        if self.is_closed() {
            return;
        }
        let WriteJob {
            key,
            bytes,
            meta,
            reserved,
            ticket,
            _slot: slot,
            _pending: pending,
        } = job;
        // Declared in this order so the claim is released before the write
        // stops counting as running: once `wait_idle` returns, no block of a
        // finished or dropped write is still offered from the in-flight map.
        let _pending = pending;
        let _slot = slot;
        let len = bytes.len() as u64;
        let room = match reserved {
            Some(room) => Some(room),
            None => self.make_room(entry_len(len)).await,
        };
        let Some(admitted) = room else {
            add(&self.counters.dropped_fills, 1);
            return;
        };
        let Some(_putting) = self.begin_put(&ticket) else {
            add(&self.counters.dropped_fills, 1);
            return;
        };
        let put = self.store.put(&key, bytes, &meta).await;
        // The store counts the entry as settled from the moment `put` returns;
        // the admission ends here too, so the entry is counted twice for as
        // short a time as possible.
        drop(admitted);
        match put {
            Ok(()) => add(&self.counters.fill_bytes, len),
            Err(e) => self.record_error(e.to_string()),
        }
    }

    /// The watermarks, computed again from the volume's space when the last
    /// computation is older than the check interval (or always, with
    /// `force`). A volume that cannot be read leaves the configured budget.
    async fn watermarks(&self, force: bool) -> Watermarks {
        {
            let limits = self.limits.lock();
            if !force
                && limits
                    .at
                    .is_some_and(|at| at.elapsed() < self.tuning.budget_check_interval)
            {
                return limits.marks;
            }
        }
        let volume = Arc::clone(&self.config.volume);
        let dir = self.config.dir.clone();
        let space = tokio::task::spawn_blocking(move || volume.space(&dir))
            .await
            .ok()
            .and_then(std::result::Result::ok);
        let marks = watermarks(
            self.config.budget_bytes,
            self.tuning.low_watermark_permille,
            self.store.disk_bytes(),
            space,
            self.tuning.volume_reserve,
        );
        *self.limits.lock() = Limits {
            marks,
            at: Some(Instant::now()),
        };
        marks
    }

    /// Disk use counting entry writes admitted but not finished, each once:
    /// every put the cache makes was admitted first, so a running put is
    /// counted in `admitted` and its pending bytes are not counted again.
    fn committed_with(&self, admitted: u64) -> u64 {
        self.store
            .disk_bytes()
            .saturating_sub(self.store.pending_bytes())
            + admitted
    }

    fn committed_bytes(&self) -> u64 {
        let admitted = *self.admitted.lock();
        self.committed_with(admitted)
    }

    /// Admits an entry of `entry_bytes` if it keeps committed disk use within
    /// `limit`. The check and the admission are one step, so concurrent
    /// writes cannot together overshoot.
    fn admit(self: &Arc<Self>, entry_bytes: u64, limit: u64) -> Option<Admitted> {
        let mut admitted = self.admitted.lock();
        if self.committed_with(*admitted) + entry_bytes > limit {
            return None;
        }
        *admitted += entry_bytes;
        Some(Admitted {
            core: Arc::clone(self),
            bytes: entry_bytes,
        })
    }

    /// Reclaims while `over()` holds, or until nothing more can be
    /// reclaimed, and returns the bytes freed on disk. The caller holds
    /// `reclaim`.
    async fn reclaim_while(&self, over: impl Fn(&Self) -> bool + Send + Sync) -> u64 {
        let sketch = Arc::clone(&self.sketch);
        let heat: BlockHeat = Arc::new(move |d: &Digest| sketch.estimate(d));
        let reclaimed = self.store.reclaim(heat, &|| over(self)).await;
        add(&self.counters.evicted_size, reclaimed.freed);
        if let Some(e) = reclaimed.error {
            self.record_error(e.to_string());
        }
        reclaimed.freed
    }

    /// Admits an entry of `entry_bytes`: if it would take the cache over its
    /// high watermark, reclaims down to the low one first. `None` when it
    /// still does not fit. The admission is held until the entry's write is
    /// done.
    async fn make_room(self: &Arc<Self>, entry_bytes: u64) -> Option<Admitted> {
        let marks = self.watermarks(false).await;
        if let Some(room) = self.admit(entry_bytes, marks.high) {
            return Some(room);
        }
        let _one = self.reclaim.lock().await;
        if let Some(room) = self.admit(entry_bytes, marks.high) {
            return Some(room);
        }
        if self.committed_bytes() + entry_bytes > marks.high {
            let low = marks.low.saturating_sub(entry_bytes);
            self.reclaim_while(|c| c.committed_bytes() > low).await;
        }
        // Reclaiming and admitting are separate steps, so the room made can be
        // gone by the admission: another write admitted without this lock as
        // the room appeared, or an entry counted twice for the moment between
        // its `put` returning and its admission ending. Reclaim on until the
        // admission (which checks and takes the room in one step) succeeds or
        // nothing more can be reclaimed.
        loop {
            if let Some(room) = self.admit(entry_bytes, marks.high) {
                return Some(room);
            }
            let fits = |c: &Self| c.committed_bytes() + entry_bytes <= marks.high;
            if self.reclaim_while(|c| !fits(c)).await == 0 {
                return self.admit(entry_bytes, marks.high);
            }
        }
    }

    /// The periodic check: reads the volume again and, if the cache is over
    /// the high watermark (another writer filled the volume), reclaims to the
    /// low one.
    async fn enforce_budget(&self) {
        let marks = self.watermarks(true).await;
        if self.committed_bytes() > marks.high {
            let _one = self.reclaim.lock().await;
            self.reclaim_while(|c| c.committed_bytes() > marks.low)
                .await;
        }
    }
}

/// The next entry write, from the demand lane first, then the prefetch lane;
/// `None` once both lanes are closed.
async fn next_job(
    demand: &mut mpsc::Receiver<WriteJob>,
    prefetch: &mut mpsc::Receiver<WriteJob>,
) -> Option<WriteJob> {
    future::poll_fn(|cx| {
        let mut open = false;
        for rx in [&mut *demand, &mut *prefetch] {
            match rx.poll_recv(cx) {
                Poll::Ready(Some(job)) => return Poll::Ready(Some(job)),
                Poll::Ready(None) => {}
                Poll::Pending => open = true,
            }
        }
        if open {
            Poll::Pending
        } else {
            Poll::Ready(None)
        }
    })
    .await
}

/// Runs queued entry writes, `concurrent_writes` at a time, until the cache
/// is dropped. A write slot is taken first and then given to the most urgent
/// lane with a job.
async fn run_writer(
    core: Arc<Core>,
    mut demand: mpsc::Receiver<WriteJob>,
    mut prefetch: mpsc::Receiver<WriteJob>,
) {
    let permits = Arc::new(Semaphore::new(core.tuning.concurrent_writes));
    loop {
        let Ok(permit) = Arc::clone(&permits).acquire_owned().await else {
            break;
        };
        let Some(job) = next_job(&mut demand, &mut prefetch).await else {
            break;
        };
        let core = Arc::clone(&core);
        tokio::spawn(async move {
            core.write(job).await;
            drop(permit);
        });
    }
}

/// Checks the budget against the volume every `budget_check_interval` until
/// the cache is dropped.
async fn run_budget(core: Weak<Core>) {
    let Some(interval) = core.upgrade().map(|c| c.tuning.budget_check_interval) else {
        return;
    };
    let mut ticks = tokio::time::interval(interval);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticks.tick().await;
        let Some(core) = core.upgrade() else {
            return;
        };
        core.enforce_budget().await;
    }
}

fn validate(config: &NodeCacheConfig, tuning: &Tuning) -> Result<()> {
    let max_block = u64::from(u32::MAX) - HEADER_LEN as u64;
    ensure!(
        tuning.block_bytes > 0 && tuning.block_bytes <= max_block,
        InvalidConfigSnafu {
            reason: format!(
                "block size {} must be between 1 and {max_block} bytes",
                tuning.block_bytes
            ),
        }
    );
    ensure!(
        config.budget_bytes > 0,
        InvalidConfigSnafu {
            reason: "the budget must be larger than 0 bytes".to_string(),
        }
    );
    ensure!(
        tuning.max_get_bytes >= tuning.block_bytes,
        InvalidConfigSnafu {
            reason: format!(
                "the largest GET ({} bytes) must hold at least one block ({} bytes)",
                tuning.max_get_bytes, tuning.block_bytes
            ),
        }
    );
    ensure!(
        tuning.gets_in_flight > 0
            && tuning.write_queue > 0
            && tuning.concurrent_writes > 0
            && tuning.prefetch_chunk_blocks > 0
            && tuning.prefetch_chunks_in_flight > 0
            && tuning.prefetch_jobs_in_flight > 0,
        InvalidConfigSnafu {
            reason: "GETs in flight, the write queue, concurrent writes, prefetch chunk blocks, prefetch chunks in flight and prefetch jobs in flight must each be at least 1"
                .to_string(),
        }
    );
    ensure!(
        !tuning.budget_check_interval.is_zero()
            && tuning.volume_reserve.permille <= 1000
            && tuning.volume_reserve.refill_permille <= 1000
            && (1..=1000).contains(&tuning.low_watermark_permille),
        InvalidConfigSnafu {
            reason: "the budget check interval must be above zero, the volume reserve and refill at most 1000 thousandths, and the low watermark between 1 and 1000 thousandths".to_string(),
        }
    );
    Ok(())
}

/// The slab size: a quarter of `budget`, at most 256 MiB, at least one entry
/// of `block_bytes`.
#[cfg(unix)]
fn slab_bytes(budget: u64, block_bytes: u64) -> u64 {
    let entry = entry_len(block_bytes);
    let quarter = (budget / 4).min(crate::block_store::slab::DEFAULT_SLAB_BYTES);
    let aligned = quarter - quarter % HEADER_LEN as u64;
    aligned.max(entry)
}
