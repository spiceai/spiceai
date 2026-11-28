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

//! Prefetch: fetching blocks before a query asks for them, in the background.
//!
//! [`NodeCache::prefetch`] queues a job and returns. One background task runs
//! the jobs in the order they were queued, up to
//! [`super::PREFETCH_JOBS_IN_FLIGHT`] objects at a time, so one whose store
//! stalls does not hold up the rest; each job is fetched in chunks of
//! [`super::PREFETCH_CHUNK_BLOCKS`] blocks with
//! [`super::PREFETCH_CHUNKS_IN_FLIGHT`] chunks in flight.
//!
//! **Free room only.** A block is queued only when its entry fits below the
//! low watermark as the cache stands when [`NodeCache::prefetch`] is called,
//! less the entries of blocks already queued, and fetched only when it still
//! fits when its turn comes, room reserved before the fetch: a prefetch never
//! evicts anything, and never brings a reclaim closer for a query's fill. At
//! most [`super::PREFETCH_QUEUE_JOBS`] jobs are queued or running. A block that
//! does not fit, or whose job would exceed that, is counted in
//! [`NodeCacheStats::prefetch_declined_bytes`] and not fetched.
//!
//! **Ordered against eviction.** A job takes its object's eviction ticket
//! when it is queued: if the object is evicted by reference before the job
//! runs (or while it runs), the job fetches nothing more and writes nothing.
//!
//! **Queries first.** A chunk starts only when no query read is waiting on
//! the store or the disk, no query read ended within
//! [`super::PREFETCH_QUIET`], and no query's disk read in the last
//! [`super::PREFETCH_BACKOFF`] took longer than
//! [`super::PREFETCH_SLOW_READ`]. Chunks already in flight finish.
//!
//! **One fetch per block.** A chunk claims its blocks in the same in-flight
//! map reads use: a block a read is fetching is left to that read, and a read
//! of a block a chunk is fetching waits for the chunk.

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::StreamExt;
use futures::future::{self, Either};
use futures::stream::FuturesUnordered;
use object_store::ObjectStore;
use object_store::path::Path;
use snafu::OptionExt;
use tokio::time::Instant;

#[cfg(doc)]
use super::NodeCacheStats;
use super::{
    Admitted, Core, Lanes, Namespace, NodeCache, Pending, Ticket, WriteJob, add, block_len,
    fetch_blocks, object_key, runs, to_usize, wait_until,
};
use crate::Result;
use crate::block_store::codec::entry_len;
use crate::block_store::{BlockKey, ObjectKey, StoredMeta};
use crate::error::UnknownStoreSnafu;

/// One object to prefetch.
pub(super) struct PrefetchJob {
    ns: Arc<Namespace>,
    inner: Arc<dyn ObjectStore>,
    path: Path,
    size: u64,
    /// The object's key for blocks of the cache's block size.
    object: ObjectKey,
    /// Block index ranges, sorted and disjoint.
    blocks: Vec<Range<u64>>,
    /// The object's eviction ticket, taken when the job was queued.
    ticket: Arc<Ticket>,
    queued: Queued,
    _pending: Pending,
}

/// A prefetch job's place in the queue's bounds: one job, and the entry
/// bytes of its blocks not yet admitted, skipped or dropped.
struct Queued {
    core: Arc<Core>,
    bytes: AtomicU64,
}

impl Queued {
    fn new(core: &Arc<Core>, bytes: u64) -> Self {
        core.prefetch_unadmitted.fetch_add(bytes, Ordering::SeqCst);
        core.prefetch_jobs.fetch_add(1, Ordering::SeqCst);
        Self {
            core: Arc::clone(core),
            bytes: AtomicU64::new(bytes),
        }
    }

    /// Stops counting `n` of the job's bytes against later prefetches.
    fn release(&self, n: u64) {
        let Ok(had) = self
            .bytes
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |b| {
                Some(b.saturating_sub(n))
            })
        else {
            return;
        };
        self.core
            .prefetch_unadmitted
            .fetch_sub(had.min(n), Ordering::SeqCst);
    }
}

impl Drop for Queued {
    fn drop(&mut self) {
        let left = *self.bytes.get_mut();
        self.core
            .prefetch_unadmitted
            .fetch_sub(left, Ordering::SeqCst);
        self.core.prefetch_jobs.fetch_sub(1, Ordering::SeqCst);
    }
}

impl std::fmt::Debug for PrefetchJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrefetchJob")
            .field("store", &self.ns.name)
            .field("path", &self.path)
            .field("size", &self.size)
            .field("blocks", &self.blocks)
            .finish_non_exhaustive()
    }
}

/// The block index ranges `ranges` (byte ranges; `None` for the whole
/// object) cover in an object of `size` bytes, clipped to the object, sorted
/// and merged.
fn block_ranges(block_bytes: u64, size: u64, ranges: Option<Vec<Range<u64>>>) -> Vec<Range<u64>> {
    let whole = std::iter::once(0..size);
    let ranges: Box<dyn Iterator<Item = Range<u64>>> = match ranges {
        Some(r) => Box::new(r.into_iter()),
        None => Box::new(whole),
    };
    let mut blocks: Vec<Range<u64>> = ranges
        .map(|r| r.start..r.end.min(size))
        .filter(|r| r.start < r.end)
        .map(|r| r.start / block_bytes..(r.end - 1) / block_bytes + 1)
        .collect();
    blocks.sort_unstable_by_key(|r| r.start);
    let mut merged: Vec<Range<u64>> = Vec::with_capacity(blocks.len());
    for r in blocks {
        match merged.last_mut() {
            Some(last) if r.start <= last.end => last.end = last.end.max(r.end),
            _ => merged.push(r),
        }
    }
    merged
}

/// Chunks of at most `per` consecutive blocks over `blocks`, lazily.
fn chunks(blocks: &[Range<u64>], per: u64) -> impl Iterator<Item = Range<u64>> + '_ {
    blocks.iter().flat_map(move |r| {
        (r.start..r.end)
            .step_by(to_usize(per))
            .map(move |s| s..s.saturating_add(per).min(r.end))
    })
}

/// What one [`NodeCache::prefetch`] call did with the blocks it was asked for
/// that were not resident, in block bytes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Prefetched {
    /// Queued for a background fetch.
    pub queued_bytes: u64,
    /// Not queued: the cache had no room for them, or its queue was full.
    pub declined_bytes: u64,
}

impl NodeCache {
    /// Queues a background fetch of `ranges` (byte ranges; `None` for the
    /// whole object) of the object `path` of `size` bytes in the store
    /// wrapped under `namespace`, notes its size (as
    /// [`NodeCache::note_size`]), and returns the block bytes this call queued
    /// and declined at once.
    ///
    /// Queued are the blocks not resident, in order, as many as fit the free
    /// room below the low watermark as the cache stands, less the blocks other
    /// prefetches have queued; nothing is evicted for them, and a block the
    /// room no longer holds when its turn comes is not fetched. Nothing is
    /// queued while 4,096 prefetch jobs are queued or running (the blocks are
    /// declined). Nothing is queued (and 0 returned) for an
    /// object uploaded through the wrapper ([`NodeCache::written_here`]), for a namespace the
    /// cache is off for, and after [`NodeCache::close`]. Blocks being fetched
    /// by a read when the job runs are left to the read.
    ///
    /// # Errors
    ///
    /// [`crate::Error::UnknownStore`] when no store was wrapped under
    /// `namespace`.
    pub fn prefetch(
        self: &Arc<Self>,
        namespace: &str,
        path: &Path,
        size: u64,
        ranges: Option<Vec<Range<u64>>>,
    ) -> Result<Prefetched> {
        let core = &self.core;
        let ns = core
            .existing_namespace(namespace)
            .context(UnknownStoreSnafu { namespace })?;
        let inner = ns
            .inner
            .lock()
            .clone()
            .context(UnknownStoreSnafu { namespace })?;
        ns.sizes.lock().insert_size(path, size);
        if core.is_closed() || ns.is_disabled() || ns.written.lock().contains(path.as_ref()) {
            return Ok(Prefetched::default());
        }
        let b = core.tuning.block_bytes;
        let object = object_key(&ns, path, size, b);
        // A wholly resident object has nothing to queue, which the count of
        // its resident blocks shows without a lookup per block.
        if core.store.resident_blocks(&object) >= object.blocks() {
            return Ok(Prefetched::default());
        }
        // The room is computed and the job queued under the queue's lock, so
        // concurrent prefetches together stay within it.
        let mut queue = core.prefetch_queue.lock();
        let full = core.prefetch_jobs.load(Ordering::SeqCst)
            >= u64::try_from(core.tuning.prefetch_queue_jobs).unwrap_or(u64::MAX);
        let mut room = {
            let committed = core.committed_bytes();
            core.limits
                .lock()
                .marks
                .low
                .saturating_sub(committed)
                .saturating_sub(core.prefetch_unadmitted.load(Ordering::SeqCst))
        };
        let mut blocks: Vec<Range<u64>> = Vec::new();
        let (mut queued, mut queued_entries, mut declined) = (0u64, 0u64, 0u64);
        for r in block_ranges(b, size, ranges) {
            for i in r {
                if core.store.contains(&object.block(i)) {
                    continue;
                }
                let len = block_len(b, size, i);
                match room.checked_sub(entry_len(len)) {
                    Some(left) if declined == 0 && !full => room = left,
                    _ => {
                        declined += len;
                        continue;
                    }
                }
                queued += len;
                queued_entries += entry_len(len);
                match blocks.last_mut() {
                    Some(last) if last.end == i => last.end = i + 1,
                    _ => blocks.push(i..i + 1),
                }
            }
        }
        add(&core.counters.prefetch_declined_bytes, declined);
        if blocks.is_empty() {
            return Ok(Prefetched {
                queued_bytes: 0,
                declined_bytes: declined,
            });
        }
        let ticket = core.enter(&ns, path.as_ref());
        let job = PrefetchJob {
            ns,
            inner,
            path: path.clone(),
            size,
            object,
            blocks,
            ticket,
            queued: Queued::new(core, queued_entries),
            _pending: Pending::new(core),
        };
        queue.push_back(job);
        drop(queue);
        core.prefetch_wake.notify_one();
        Ok(Prefetched {
            queued_bytes: queued,
            declined_bytes: declined,
        })
    }
}

/// Runs queued prefetch jobs, up to `prefetch_jobs_in_flight` at a time,
/// until the cache is dropped. The jobs run inside this task, so aborting it
/// stops them all.
pub(super) async fn run(core: Arc<Core>, lanes: Lanes) {
    let limit = core.tuning.prefetch_jobs_in_flight;
    let mut running = FuturesUnordered::new();
    let start = |job: PrefetchJob| {
        let (core, lanes) = (Arc::clone(&core), lanes.clone());
        async move { core.run_job(&lanes, &job).await }
    };
    loop {
        if running.len() >= limit {
            running.next().await;
            continue;
        }
        if running.is_empty() {
            let job = core.next_job().await;
            running.push(start(job));
            continue;
        }
        // A queued job is popped only in the poll that returns it, so
        // dropping this wait after a job finished loses none.
        let next = core.next_job();
        match future::select(std::pin::pin!(next), running.next()).await {
            Either::Left((job, _)) => running.push(start(job)),
            Either::Right(_) => {}
        }
    }
}

impl Core {
    /// The next queued prefetch job, waiting for one.
    async fn next_job(&self) -> PrefetchJob {
        wait_until(&self.prefetch_wake, || {
            self.prefetch_queue.lock().pop_front()
        })
        .await
    }

    async fn run_job(self: &Arc<Self>, lanes: &Lanes, job: &PrefetchJob) {
        if self.evicted_since(&job.ticket) {
            return;
        }
        futures::stream::iter(chunks(&job.blocks, self.tuning.prefetch_chunk_blocks))
            .for_each_concurrent(self.tuning.prefetch_chunks_in_flight, |c| {
                self.prefetch_chunk(lanes, job, c)
            })
            .await;
    }

    /// Waits until a prefetch chunk may start: no query read waiting, none
    /// ended within the quiet period, no slow disk read within the back-off
    /// period. `false` once the cache is closed.
    async fn wait_until_quiet(&self) -> bool {
        loop {
            wait_until(&self.fg_changed, || {
                (self.is_closed() || self.fg_waiting.load(Ordering::SeqCst) == 0).then_some(())
            })
            .await;
            if self.is_closed() {
                return false;
            }
            let resume = {
                let fg = self.foreground.lock();
                let quiet = fg.last_end.map(|t| t + self.tuning.prefetch_quiet);
                match (quiet, fg.slow_until) {
                    (Some(a), Some(b)) => Some(a.max(b)),
                    (a, b) => a.or(b),
                }
            };
            match resume {
                Some(at) if at > Instant::now() => {
                    // A read ending meanwhile moves the quiet period on, which
                    // the next pass sees.
                    let sleep = tokio::time::sleep_until(at);
                    let closed = self.closed.cancelled();
                    let _ = future::select(std::pin::pin!(sleep), std::pin::pin!(closed)).await;
                }
                _ => return true,
            }
        }
    }

    /// Room for one prefetched block of `data_len` bytes below the low
    /// watermark, or `None` (counted) when it does not fit as the cache
    /// stands.
    async fn prefetch_room(self: &Arc<Self>, data_len: u64) -> Option<Admitted> {
        let marks = self.watermarks(false).await;
        let room = self.admit(entry_len(data_len), marks.low);
        if room.is_none() {
            add(&self.counters.prefetch_declined_bytes, data_len);
        }
        room
    }

    /// Fetches and queues the writes of one chunk of `job`.
    async fn prefetch_chunk(self: &Arc<Self>, lanes: &Lanes, job: &PrefetchJob, chunk: Range<u64>) {
        if !self.wait_until_quiet().await {
            return;
        }
        let b = self.tuning.block_bytes;
        let mut wanted: Vec<(BlockKey, Admitted)> = Vec::new();
        for i in chunk {
            let k = job.object.block(i);
            let len = block_len(b, job.size, i);
            // Admitted below, or skipped: no longer held against the room of
            // later prefetches either way.
            job.queued.release(entry_len(len));
            if self.store.contains(&k) {
                continue;
            }
            if let Some(room) = self.prefetch_room(len).await {
                wanted.push((k, room));
            }
        }
        // An eviction of the object since the job was queued fences it:
        // nothing is fetched.
        if wanted.is_empty() || self.evicted_since(&job.ticket) {
            return;
        }
        let ticket = &job.ticket;
        let keys: Vec<BlockKey> = wanted.iter().map(|(k, _)| k.clone()).collect();
        // Not counted in the sketch: a prefetch is not a read. Blocks someone
        // else is fetching are left to them.
        let (lead, _others) = self.claim(&keys, false, ticket.generation);
        let mut rooms: Vec<Option<Admitted>> = wanted.into_iter().map(|(_, r)| Some(r)).collect();
        // A block written between the residency check and the claim.
        let lead: Vec<_> = lead
            .into_iter()
            .filter(|(j, _)| !self.store.contains(&keys[*j]))
            .collect();
        let mut guards: std::collections::HashMap<usize, _> = lead.into_iter().collect();
        let mut positions: Vec<usize> = guards.keys().copied().collect();
        positions.sort_unstable();
        // Runs of consecutive block indexes, so one ranged GET covers exactly
        // the blocks of a run: blocks skipped (resident, declined, fetched by
        // a read) leave gaps in the indexes that consecutive positions do not
        // show.
        for run in runs(positions, usize::MAX, |a, b| {
            keys[a].index + 1 == keys[b].index
        }) {
            let (Some(&lo), Some(&hi)) = (run.first(), run.last()) else {
                continue;
            };
            let blocks = keys[lo].index..keys[hi].index + 1;
            let fetch = fetch_blocks(&job.inner, &job.path, job.size, b, blocks);
            let fetched = match future::select(
                std::pin::pin!(fetch),
                std::pin::pin!(self.closed.cancelled()),
            )
            .await
            {
                Either::Left((Ok(fetched), _)) => fetched,
                // A failed fetch or a closed cache: the claims drop, so
                // waiting reads fetch for themselves.
                Either::Left((Err(_), _)) | Either::Right(_) => return,
            };
            let (bytes, meta): (Vec<bytes::Bytes>, StoredMeta) = fetched;
            let fetched_len: u64 = bytes.iter().map(|x| x.len() as u64).sum();
            add(&self.counters.fetched_prefetch, fetched_len);
            let meta = Arc::new(meta);
            for (j, block) in run.into_iter().zip(bytes) {
                let (Some(slot), Some(reserved)) = (guards.remove(&j), rooms[j].take()) else {
                    continue;
                };
                slot.ready(block.clone());
                let write = WriteJob {
                    key: keys[j].clone(),
                    bytes: block,
                    meta: Arc::clone(&meta),
                    reserved: Some(reserved),
                    ticket: Arc::clone(ticket),
                    _slot: slot,
                    _pending: Pending::new(self),
                };
                // Waits for room in the queue; fails only once the writer is
                // gone (the cache was dropped).
                let _ = lanes.prefetch.send(write).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_ranges_clip_sort_and_merge() {
        let mib = 1 << 20;
        // Whole object, the last block partial.
        assert_eq!(block_ranges(mib, 5 * mib / 2, None), vec![0..3]);
        // Out of order, overlapping, adjacent, empty, and past the end.
        let ranges = vec![
            7 * mib..8 * mib + 1,
            0..1,
            mib - 1..mib + 1,
            3 * mib..3 * mib,
            20 * mib..21 * mib,
            2 * mib..3 * mib,
        ];
        assert_eq!(block_ranges(mib, 10 * mib, Some(ranges)), vec![0..3, 7..9]);
        assert_eq!(block_ranges(mib, 0, None), Vec::<Range<u64>>::new());
        assert_eq!(
            chunks(&[0..10, 12..13], 4).collect::<Vec<_>>(),
            vec![0..4, 4..8, 8..10, 12..13]
        );
    }
}
