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

//! The three-structure work-stealing `TaskQueue` and its priority hand-off
//! primitive (proposal §10.1, Stage 1 #3c — the morsel execution substrate).
//!
//! ## The three structures (why not one queue)
//!
//! The ingest pool needs *both* work-stealing (to load-balance farmed-out
//! bursts) *and* a top-priority hand-off (to keep a stream's serial receiver
//! draining without burying it behind queued bursts). No single queue expresses
//! both, so §10.1 builds three per-worker structures polled in a fixed order:
//!
//! 1. [`LocalQueue::stay_local`] — an owner-only, non-stealable [`VecDeque`]
//!    holding the steady-state receiver continuation at top priority. `st3`'s
//!    LIFO-owner end only *approximates* non-stealable (a stalled owner's
//!    freshest item can still be stolen), so a guaranteed-non-stealable slot
//!    needs its own structure.
//! 2. [`LocalQueue::moveable`] — an [`st3::lifo::Worker`] (LIFO-owner pop for
//!    cache locality, FIFO steal for balance), overflowing to a global MPMC
//!    queue; holds all stealable work (farmed bursts + intra-burst shard
//!    sub-tasks).
//! 3. [`PriorityMailbox`] — a summary-gated set of single-slot mailboxes checked
//!    *before* general stealing; holds a stream's receiver baton when its host
//!    offloads it. Its earning property is that an *empty* poll costs a single
//!    relaxed load.
//!
//! ## Stage 1 scope (depth = 1)
//!
//! At Stage 1 depth = 1 the CDC apply is routed through the pinned
//! [`crate::ingest_pool::IngestPool`] over its MPMC channel (submit → run),
//! which does **not** exercise this queue's steal paths or the receiver-baton
//! inversion — that traffic arrives in Stage 2 (the coalescing receiver + depth
//! above 1). This module is landed now, with its hard-to-verify part (the mailbox
//! atomic orderings) model-checked under `loom`, so Stage 2 builds on a proven
//! substrate rather than introducing the lock-free hand-off and the depth at
//! once.
//!
//! ## Overflow queue substitution (documented deviation)
//!
//! §10.1 names a `crossbeam_deque::Injector` for the global overflow. Stage 1
//! #3c uses an unbounded `crossbeam_channel` instead — already a crate
//! dependency (the `IngestPool` descriptor channel), functionally the same MPMC
//! overflow role, and it keeps the new-dependency set to exactly `st3` +
//! `crossbeam-utils` per the #3c plan. The overflow is off the steal-locality
//! fast path (polled last, only when `moveable` overflows), so the choice is not
//! performance-load-bearing at Stage 1; Stage 2 may swap in `Injector` if
//! steal-batch locality from the global queue proves to matter.

use std::collections::VecDeque;
use std::ptr::NonNull;
use std::sync::Arc;
use std::thread::Thread;

use crossbeam_utils::CachePadded;
use parking_lot::Mutex;
use st3::lifo::{Stealer, Worker as MoveableWorker};

#[cfg(cayenne_loom)]
use loom::sync::atomic::{AtomicPtr, AtomicU32, AtomicU64, Ordering};
#[cfg(not(cayenne_loom))]
use std::sync::atomic::{AtomicPtr, AtomicU32, AtomicU64, Ordering};

/// A dense, small stream index assigned at stream registration and freed on
/// teardown. It sizes [`PriorityMailbox`]'s summary word, so Stage 1 supports up
/// to 64 concurrent streams (one summary word); streams ≈ CDC slots, so this is
/// the expected regime. A two-level / word-array summary is a Stage 2 extension
/// (documented on [`PriorityMailbox::SUMMARY_STREAM_CAP`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId(pub usize);

/// An exclusive, non-duplicated pointer to a stream's persistent receiver state
/// (`Arc`'d/leaked at registration, so arm/claim are pointer-only and there is
/// nothing to reclaim). Exclusivity is sound *because* of the mailbox invariant
/// — a stream's receiver lives in exactly one place at a time (running, in a
/// worker's `stay_local`, or parked in the mailbox), never two copies — so the
/// holder of a `Baton` has unique access to `R`.
///
/// It is deliberately **not** `Send`: at Stage 1 a claimed baton is used on the
/// same worker that claimed it (the baton's *pointer* crosses threads through
/// the mailbox's `AtomicPtr`, which is itself `Send`/`Sync`; the `Baton` value
/// never moves across a thread boundary). Stage 2's cross-core hand-off keeps
/// that shape — the receiver's small parser state moves, never the batch.
#[derive(Debug)]
pub struct Baton<R>(NonNull<R>);

impl<R> Baton<R> {
    /// Wrap a non-null receiver pointer as a baton.
    #[must_use]
    pub fn new(ptr: NonNull<R>) -> Self {
        Self(ptr)
    }

    /// The raw receiver pointer.
    #[must_use]
    pub fn as_ptr(&self) -> *mut R {
        self.0.as_ptr()
    }

    /// Exclusive access to the receiver.
    ///
    /// # Safety
    ///
    /// The caller must uphold the mailbox invariant: this baton is the only
    /// live handle to the receiver (it was just claimed from the mailbox or is
    /// held by this worker before being armed), so the returned `&mut R` is
    /// unaliased.
    #[must_use]
    pub unsafe fn as_mut(&mut self) -> &mut R {
        // SAFETY: guaranteed unaliased by the never-duplicated invariant.
        unsafe { self.0.as_mut() }
    }
}

/// The receiver-baton hand-off (`priority_shared`), shared across all workers in
/// an [`Arc`] via [`TaskQueue`].
///
/// INVARIANT (caller-guaranteed): a stream's receiver lives in exactly one place
/// at a time — running, in a worker's `stay_local`, or parked here — never two
/// copies. Free, because the receiver is the *serial* coalesce head (proposal
/// §8). This is what makes each slot a plain swap: one producer per slot at a
/// time, no ABA, and re-arm without coordination.
///
/// MEMORY ORDERING (proven under `loom`, see the `loom_tests` module):
/// - [`arm`](Self::arm): `slots[s].store(baton, Release)` *then*
///   `summary.fetch_or(bit, Release)` — the baton write is published before the
///   announcement bit.
/// - [`try_claim`](Self::try_claim): `summary.load(Relaxed)` (the empty-poll
///   gate) → `slots[i].swap(null, Acquire)` (pairs with the arm store, so the
///   claimer observes the fully-written receiver) → `summary.fetch_and(!bit,
///   Release)` (clear the announcement).
///
/// The `Relaxed` summary gate is sound only because a *parked* worker's
/// happens-before with the producer arrives through the park/unpark pair — so a
/// producer that arms MUST unpark exactly one idle worker (see
/// [`TaskQueue::offload_receiver`] / [`TaskQueue::push_moveable`]). A worker that
/// is *spinning* (not parked) re-reads the `Relaxed` gate and then performs the
/// `Acquire` swap, which is the actual synchronizing edge for the baton payload.
pub struct PriorityMailbox<R> {
    /// Bit `s` set ⇔ `slots[s]` holds an unclaimed baton. The whole empty poll
    /// is one relaxed load of this; written only on arm/claim (rare), so it
    /// stays `Shared` in every core's L1 — no coherence traffic when idle.
    /// `CachePadded` because it takes a cross-core load on every idle poll.
    summary: CachePadded<AtomicU64>,
    /// One single-slot mailbox per stream; `null` = empty. `CachePadded` so a
    /// producer arming its own stream never false-shares another stream's slot.
    slots: Box<[CachePadded<AtomicPtr<R>>]>,
    /// Shared round-robin scan origin: a just-claimed stream goes to the back of
    /// the scan order, bounding a waiting stream's wait by the number of *other*
    /// waiting streams (a rotating-priority arbiter, starvation-free by
    /// construction — proposal §10.1 "Fairness"). Touched only inside the
    /// `word != 0` branch (an actual claim), never on the empty poll.
    claim_cursor: CachePadded<AtomicU32>,
}

// The mailbox is auto-`Send`/`Sync` for any `R`: it transits `R` only as raw
// *pointers* through `AtomicPtr` (which is `Send`+`Sync` for all `R`), never as
// `R` or `Baton<R>` values — so no `R: Send`/`Sync` bound is needed to share it.

impl<R> PriorityMailbox<R> {
    /// The maximum number of streams a single summary word supports. Stage 1
    /// caps the mailbox here; a table set beyond this is a configuration the
    /// two-level summary (Stage 2) would lift. `StreamId`s are dense, so the cap
    /// is the concurrent-stream count, not an id ceiling.
    pub const SUMMARY_STREAM_CAP: usize = 64;

    /// Build a mailbox sized for `stream_count` streams (clamped to at least 1,
    /// at most [`Self::SUMMARY_STREAM_CAP`]).
    #[must_use]
    pub fn new(stream_count: usize) -> Self {
        let n = stream_count.clamp(1, Self::SUMMARY_STREAM_CAP);
        let mut slots = Vec::with_capacity(n);
        for _ in 0..n {
            slots.push(CachePadded::new(AtomicPtr::new(std::ptr::null_mut())));
        }
        Self {
            summary: CachePadded::new(AtomicU64::new(0)),
            slots: slots.into_boxed_slice(),
            claim_cursor: CachePadded::new(AtomicU32::new(0)),
        }
    }

    /// Number of stream slots.
    #[must_use]
    pub fn stream_capacity(&self) -> usize {
        self.slots.len()
    }

    /// Producer: the worker currently hosting `stream` parks its receiver here.
    /// Store the baton *then* announce it (both `Release`) so a claimer that sees
    /// the announcement bit also sees the fully-written baton.
    ///
    /// The caller MUST hold the never-duplicated invariant: only the worker that
    /// holds `stream`'s receiver may `arm` it, and it cannot hold it while the
    /// slot is still occupied — so `arm` and the prior claim never overlap, and
    /// the winning `swap` in `try_claim` *is* the re-arm point (no generation
    /// counter needed).
    #[expect(
        clippy::needless_pass_by_value,
        reason = "arm takes OWNERSHIP of the baton — it moves into the mailbox slot; the caller must relinquish it (the never-duplicated invariant)"
    )]
    pub fn arm(&self, stream: StreamId, baton: Baton<R>) {
        debug_assert!(
            stream.0 < self.slots.len(),
            "StreamId {} out of range for mailbox of {} slots",
            stream.0,
            self.slots.len()
        );
        self.slots[stream.0].store(baton.as_ptr(), Ordering::Release);
        self.summary
            .fetch_or(1u64 << stream.0, Ordering::Release);
    }

    /// Consumer: any idle worker, inside its poll loop. The empty path is a
    /// single relaxed load. On a hit it returns the claimed stream + baton and
    /// re-arms the slot (the winning swap empties it) and clears the summary bit.
    #[must_use]
    pub fn try_claim(&self) -> Option<(StreamId, Baton<R>)> {
        let word = self.summary.load(Ordering::Relaxed);
        if word == 0 {
            return None; // the common case: one relaxed load, no cursor touch
        }
        let start = self.claim_cursor.load(Ordering::Relaxed) % 64;
        // Scan set bits starting from `start` (round-robin origin), not bit 0.
        let mut s = word.rotate_right(start);
        while s != 0 {
            // `bit` stays `u32` (< 64) end-to-end — no usize↔u32 cast.
            let bit: u32 = (s.trailing_zeros() + start) % 64;
            let i = bit as usize;
            // A concurrent claimer may have taken this slot; a null swap means we
            // lost the race for `i`, so move to the next set bit.
            let p = self.slots[i].swap(std::ptr::null_mut(), Ordering::Acquire);
            if let Some(ptr) = NonNull::new(p) {
                self.summary.fetch_and(!(1u64 << bit), Ordering::Release);
                // Claimed ⇒ send this stream to the back of the scan order.
                self.claim_cursor
                    .store(bit.wrapping_add(1), Ordering::Relaxed);
                return Some((StreamId(i), Baton::new(ptr)));
            }
            s &= s - 1; // clear the lowest set bit; try the next
        }
        None
    }
}

impl<R> std::fmt::Debug for PriorityMailbox<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PriorityMailbox")
            .field("streams", &self.slots.len())
            .field("summary", &self.summary.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// The set of parked workers, used to wake **exactly one** idle worker when new
/// work is published (the happens-before edge that makes the mailbox's `Relaxed`
/// gate sound — see [`PriorityMailbox`]).
///
/// Stage 1 note: this structure is **not on the depth = 1 hot path** — the CDC
/// apply is routed through [`crate::ingest_pool::IngestPool`], whose own
/// `recv`/`recv_timeout` provides the wake. It exists so the §10.1 `TaskQueue`
/// is complete and testable; Stage 2 (the coalescing receiver at depth > 1) is
/// the first real consumer and may harden the register/park race handling (a
/// registered-but-since-woken handle can absorb a spurious `unpark_one`, which
/// wastes a wakeup but never loses correctness — the worker simply re-polls).
#[derive(Debug, Default)]
pub struct ParkSet {
    idle: Mutex<Vec<Thread>>,
}

impl ParkSet {
    /// Build an empty park set.
    #[must_use]
    pub fn new() -> Self {
        Self {
            idle: Mutex::new(Vec::new()),
        }
    }

    /// Register the current thread as about-to-park. The caller MUST re-poll its
    /// queues after registering and before calling [`std::thread::park`], so a
    /// wake published between the last poll and the park is not lost (an
    /// `unpark` delivered before `park` makes the next `park` return
    /// immediately).
    pub fn register(&self, handle: Thread) {
        self.idle.lock().push(handle);
    }

    /// Wake exactly one registered (parked) worker, if any. New work → one wake.
    pub fn unpark_one(&self) {
        let handle = self.idle.lock().pop();
        if let Some(handle) = handle {
            handle.unpark();
        }
    }

    /// Number of registered (about-to-park / parked) handles — diagnostics only.
    #[must_use]
    pub fn parked_len(&self) -> usize {
        self.idle.lock().len()
    }
}

/// Work yielded by [`LocalQueue::next`]: either a stealable/queued task
/// descriptor, or a claimed receiver baton (the worker becomes that stream's
/// host and drains it). The two are distinct currencies — the mailbox holds
/// receiver batons, the deques hold task descriptors — so a single poll returns
/// the union.
#[derive(Debug)]
pub enum Work<T, R> {
    /// A task descriptor from `stay_local`, `moveable`, a steal, or overflow.
    Task(T),
    /// A receiver baton claimed from the [`PriorityMailbox`]: this worker is now
    /// the host of `StreamId` and should drain it.
    Receiver(StreamId, Baton<R>),
}

/// The shared queue state, held in an [`Arc`] across every ingest worker.
///
/// `T` is the task-descriptor type (`ingest_pool::Task` in production; a trivial
/// type in tests). `R` is the receiver-state pointee for the [`PriorityMailbox`]
/// (Stage 2's coalescer; a dummy in Stage 1 tests).
pub struct TaskQueue<T, R> {
    /// The priority receiver-baton hand-off (polled before general stealing).
    pub priority: PriorityMailbox<R>,
    /// One stealer per worker, indexed by worker id; a worker steals from the
    /// others' `moveable` deques (FIFO steal for balance).
    stealers: Box<[Stealer<T>]>,
    /// Global overflow: where a worker's bounded `moveable` spills. Unbounded
    /// MPMC (documented substitution for §10.1's `Injector`, see the module
    /// docs).
    overflow_tx: crossbeam_channel::Sender<T>,
    overflow_rx: crossbeam_channel::Receiver<T>,
    /// Unpark-exactly-one on newly published work.
    parking: ParkSet,
}

impl<T, R> std::fmt::Debug for TaskQueue<T, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskQueue")
            .field("workers", &self.stealers.len())
            .field("priority", &self.priority)
            .field("parked", &self.parking.parked_len())
            .finish_non_exhaustive()
    }
}

impl<T: Send, R> TaskQueue<T, R> {
    /// Build a queue for `worker_count` workers over `stream_count` mailbox
    /// streams, returning the shared [`TaskQueue`] plus one owned
    /// [`LocalQueue`] per worker (each pinned to its thread for life).
    ///
    /// `moveable_capacity` bounds each worker's `st3` deque (rounded up to a
    /// power of two by `st3`); overflow spills to the global MPMC queue.
    #[must_use]
    pub fn build(
        worker_count: usize,
        stream_count: usize,
        moveable_capacity: usize,
    ) -> (Arc<Self>, Vec<LocalQueue<T, R>>) {
        let worker_count = worker_count.max(1);
        let mut moveables = Vec::with_capacity(worker_count);
        let mut stealers = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let moveable = MoveableWorker::new(moveable_capacity.max(1));
            stealers.push(moveable.stealer());
            moveables.push(moveable);
        }
        let (overflow_tx, overflow_rx) = crossbeam_channel::unbounded::<T>();
        let shared = Arc::new(Self {
            priority: PriorityMailbox::new(stream_count),
            stealers: stealers.into_boxed_slice(),
            overflow_tx,
            overflow_rx,
            parking: ParkSet::new(),
        });
        let locals = moveables
            .into_iter()
            .enumerate()
            .map(|(id, moveable)| LocalQueue {
                id,
                stay_local: VecDeque::new(),
                moveable,
                q: Arc::clone(&shared),
            })
            .collect();
        (shared, locals)
    }

    /// Arm a receiver baton on the priority mailbox and wake one idle worker so
    /// the `Relaxed` summary gate is observed (proposal §10.1).
    pub fn offload_receiver(&self, stream: StreamId, baton: Baton<R>) {
        self.priority.arm(stream, baton);
        self.parking.unpark_one();
    }

    /// Register + wake accessors for the worker run loop (see [`ParkSet`]).
    pub fn register_parking(&self, handle: Thread) {
        self.parking.register(handle);
    }

    /// Wake one idle worker (used after pushing to overflow from a non-owner).
    pub fn wake_one(&self) {
        self.parking.unpark_one();
    }
}

/// A per-worker handle: owns the non-stealable `stay_local` ring and the
/// stealable `moveable` deque, and references the shared [`TaskQueue`]. `!Sync`
/// and pinned to one thread for life (the owner-only structures are unsynchronized).
pub struct LocalQueue<T, R> {
    id: usize,
    /// Owner-only, non-stealable: the steady-state receiver continuation at top
    /// priority (proposal §10.1). A plain `VecDeque`, no synchronization.
    stay_local: VecDeque<T>,
    /// Stealable work: LIFO-owner pop for cache locality, FIFO steal for
    /// balance; overflows to the global queue.
    moveable: MoveableWorker<T>,
    q: Arc<TaskQueue<T, R>>,
}

impl<T: Send, R> LocalQueue<T, R> {
    /// This worker's id (its index into the shared stealer array).
    #[must_use]
    pub fn id(&self) -> usize {
        self.id
    }

    /// The shared queue.
    #[must_use]
    pub fn shared(&self) -> &Arc<TaskQueue<T, R>> {
        &self.q
    }

    /// Poll for the next unit of work in the fixed §10.1 order:
    /// `stay_local → priority → moveable → steal victims → overflow`. Priority
    /// is polled *before* the worker's own `moveable` every iteration, so a
    /// steady baton stream is never buried under queued bursts. Returns `None`
    /// when everything is empty (the caller then registers + parks).
    ///
    /// (Named `next_work`, not `next`, so it is not mistaken for
    /// `Iterator::next` — a `LocalQueue` is not an iterator; it yields either a
    /// task or a receiver baton and is polled from the worker run loop.)
    pub fn next_work(&mut self) -> Option<Work<T, R>> {
        // 1. Owner-only continuation (non-stealable, top priority).
        if let Some(task) = self.stay_local.pop_front() {
            return Some(Work::Task(task));
        }
        // 2. Priority receiver-baton hand-off (before general stealing).
        if let Some((stream, baton)) = self.q.priority.try_claim() {
            return Some(Work::Receiver(stream, baton));
        }
        // 3. Own stealable deque (LIFO pop for locality).
        if let Some(task) = self.moveable.pop() {
            return Some(Work::Task(task));
        }
        // 4. Steal from other workers' deques (FIFO steal for balance).
        for (victim, stealer) in self.q.stealers.iter().enumerate() {
            if victim == self.id {
                continue;
            }
            // Steal roughly half, popping one to run now; the rest lands in our
            // `moveable` for subsequent `next()` calls. `Empty`/`Busy` (nothing
            // here, or a concurrent steal) just falls through to the next victim.
            if let Ok((task, _)) = stealer.steal_and_pop(&self.moveable, |n| n - n / 2) {
                return Some(Work::Task(task));
            }
        }
        // 5. Global overflow (last, off the steal-locality fast path).
        if let Ok(task) = self.q.overflow_rx.try_recv() {
            return Some(Work::Task(task));
        }
        None
    }

    /// Push onto the owner-only, non-stealable `stay_local` ring (the
    /// steady-state receiver keeps itself here). No wake needed: the owner is
    /// running.
    pub fn push_stay_local(&mut self, task: T) {
        self.stay_local.push_back(task);
    }

    /// Push a farmed burst / shard sub-task onto the stealable `moveable` deque,
    /// spilling to the global overflow when the bounded deque is full, and wake
    /// one idle worker so a steal or overflow drain can begin.
    pub fn push_moveable(&mut self, task: T) {
        if let Err(task) = self.moveable.push(task) {
            // Bounded deque full — spill to the unbounded global overflow.
            // `send` on an unbounded channel only fails if every receiver is
            // gone; the shared `overflow_rx` is held for the queue's life, so
            // this cannot fail in practice.
            let _ = self.q.overflow_tx.send(task);
        }
        self.q.parking.unpark_one();
    }

    /// Offload this worker's current receiver baton to the priority mailbox so
    /// an idle worker promptly takes over draining (the inline big-batch
    /// inversion — proposal §10.1). Wakes exactly one worker.
    pub fn offload_receiver(&self, stream: StreamId, baton: Baton<R>) {
        self.q.offload_receiver(stream, baton);
    }
}

impl<T, R> std::fmt::Debug for LocalQueue<T, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalQueue")
            .field("id", &self.id)
            .field("stay_local", &self.stay_local.len())
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Normal (non-loom) tests: single-threaded logic + a multi-threaded stress that
// also serves as the `miri` target (pointer provenance + data-race checks).
// ---------------------------------------------------------------------------
#[cfg(all(test, not(cayenne_loom)))]
mod tests {
    use super::{Baton, PriorityMailbox, StreamId, TaskQueue, Work};
    use std::ptr::NonNull;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};

    /// A receiver payload the arm→claim edge must publish coherently: the
    /// claimer, after the `Acquire` swap, must observe the `stamp` the producer
    /// wrote before arming.
    #[derive(Debug)]
    struct Receiver {
        stream: usize,
        stamp: u64,
    }

    #[test]
    fn empty_mailbox_poll_is_none() {
        let mb: PriorityMailbox<Receiver> = PriorityMailbox::new(8);
        assert!(mb.try_claim().is_none());
        assert_eq!(mb.stream_capacity(), 8);
    }

    #[test]
    fn arm_then_claim_roundtrips_baton_and_payload() {
        let mb: PriorityMailbox<Receiver> = PriorityMailbox::new(4);
        let mut r = Box::new(Receiver { stream: 2, stamp: 0 });
        r.stamp = 0xDEAD_BEEF;
        let ptr = NonNull::from(r.as_mut());
        mb.arm(StreamId(2), Baton::new(ptr));

        let (claimed, mut baton) = mb.try_claim().expect("armed slot is claimable");
        assert_eq!(claimed, StreamId(2));
        // SAFETY: single claimed baton, unaliased by the invariant.
        let recv = unsafe { baton.as_mut() };
        assert_eq!(recv.stream, 2);
        assert_eq!(recv.stamp, 0xDEAD_BEEF);
        // Slot is re-armed empty by the winning swap.
        assert!(mb.try_claim().is_none());
    }

    #[test]
    fn claim_cursor_rotates_to_avoid_positional_starvation() {
        let mb: PriorityMailbox<Receiver> = PriorityMailbox::new(4);
        let mut recvs: Vec<Box<Receiver>> = (0..4)
            .map(|i| Box::new(Receiver { stream: i, stamp: i as u64 }))
            .collect();
        // Arm streams 0 and 1.
        for i in [0usize, 1] {
            let ptr = NonNull::from(recvs[i].as_mut());
            mb.arm(StreamId(i), Baton::new(ptr));
        }
        // First claim starts at cursor 0 → stream 0; cursor advances to 1.
        let (first, _) = mb.try_claim().expect("first claim");
        assert_eq!(first, StreamId(0));
        // Re-arm stream 0 while stream 1 still waits.
        let ptr0 = NonNull::from(recvs[0].as_mut());
        mb.arm(StreamId(0), Baton::new(ptr0));
        // Next claim must serve the *waiting* stream 1 (cursor is at 1), not
        // re-serve the just-re-armed stream 0 — the anti-starvation property.
        let (second, _) = mb.try_claim().expect("second claim");
        assert_eq!(second, StreamId(1), "cursor sends a just-claimed stream to the back");
    }

    #[test]
    fn local_queue_poll_order_stay_local_before_moveable() {
        let (_q, mut locals) = TaskQueue::<u64, Receiver>::build(1, 1, 64);
        let lq = &mut locals[0];
        lq.push_moveable(10);
        lq.push_stay_local(20);
        // stay_local is polled first even though it was pushed last.
        assert!(matches!(lq.next_work(), Some(Work::Task(20))));
        assert!(matches!(lq.next_work(), Some(Work::Task(10))));
        assert!(lq.next_work().is_none());
    }

    #[test]
    fn local_queue_priority_before_own_moveable() {
        let (q, mut locals) = TaskQueue::<u64, Receiver>::build(1, 2, 64);
        let lq = &mut locals[0];
        lq.push_moveable(99);
        let mut r = Box::new(Receiver { stream: 1, stamp: 7 });
        let ptr = NonNull::from(r.as_mut());
        q.offload_receiver(StreamId(1), Baton::new(ptr));
        // Priority is polled before the worker's own moveable.
        match lq.next_work() {
            Some(Work::Receiver(s, _)) => assert_eq!(s, StreamId(1)),
            other => panic!("expected receiver baton first, got {other:?}"),
        }
        assert!(matches!(lq.next_work(), Some(Work::Task(99))));
    }

    #[test]
    fn moveable_overflow_spills_to_global_and_drains() {
        // Tiny deque so pushes overflow to the global queue.
        let (_q, mut locals) = TaskQueue::<u64, Receiver>::build(1, 1, 1);
        let lq = &mut locals[0];
        for i in 0..8 {
            lq.push_moveable(i);
        }
        let mut drained = Vec::new();
        while let Some(Work::Task(t)) = lq.next_work() {
            drained.push(t);
        }
        drained.sort_unstable();
        assert_eq!(drained, (0..8).collect::<Vec<_>>(), "no task lost across overflow");
    }

    #[test]
    fn steal_balances_from_a_busy_worker() {
        let (_q, mut locals) = TaskQueue::<u64, Receiver>::build(2, 1, 64);
        // Load worker 0's moveable; worker 1 is idle and must steal.
        for i in 0..6 {
            locals[0].push_moveable(i);
        }
        let mut stolen = 0;
        // Worker 1 steals until it and the victim are drained.
        loop {
            match locals[1].next_work() {
                Some(Work::Task(_)) => stolen += 1,
                Some(Work::Receiver(..)) => unreachable!(),
                None => break,
            }
        }
        assert!(stolen > 0, "idle worker stole at least one task from the busy one");
    }

    /// Multi-threaded arm/claim stress — the `miri` target. Each of `S` streams
    /// is armed and claimed repeatedly; every claimed baton must expose the
    /// payload the arming thread wrote (the arm→claim publish edge), and no two
    /// claims may see the same live baton (the never-duplicated invariant).
    #[test]
    fn concurrent_arm_claim_publishes_payload_no_duplication() {
        const STREAMS: usize = 4;
        const ROUNDS: usize = 200;

        let mb: Arc<PriorityMailbox<Receiver>> = Arc::new(PriorityMailbox::new(STREAMS));
        // Persistent per-stream receivers (leaked-equivalent: owned for the test
        // lifetime, pointer transits the mailbox).
        let recvs: Vec<Box<Receiver>> = (0..STREAMS)
            .map(|s| Box::new(Receiver { stream: s, stamp: 0 }))
            .collect();
        let recv_ptrs: Vec<usize> = recvs
            .iter()
            .map(|r| std::ptr::from_ref::<Receiver>(r.as_ref()) as usize)
            .collect();

        let claims = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(2));

        std::thread::scope(|scope| {
            // Producer: cycle each stream's baton in and out, stamping it before
            // each arm. Only one baton per stream is ever live (arm only after a
            // claim removed the previous one).
            let mb_p = Arc::clone(&mb);
            let barrier_p = Arc::clone(&barrier);
            let recv_ptrs_p = recv_ptrs.clone();
            scope.spawn(move || {
                barrier_p.wait();
                for round in 0..ROUNDS {
                    for (s, &addr) in recv_ptrs_p.iter().enumerate() {
                        let ptr = addr as *mut Receiver;
                        // SAFETY: this baton for stream `s` is not live in the
                        // mailbox now (the consumer claimed the previous one, or
                        // it is the first arm), so we hold unique access.
                        unsafe {
                            (*ptr).stamp = (round as u64) << 8 | s as u64;
                        }
                        // Spin until the slot is free to re-arm (previous baton
                        // claimed), preserving one-live-baton-per-stream.
                        loop {
                            let bit = 1u64 << s;
                            if super::atomic_summary_bit_clear(&mb_p, bit) {
                                break;
                            }
                            std::hint::spin_loop();
                        }
                        mb_p.arm(
                            StreamId(s),
                            Baton::new(NonNull::new(ptr).expect("receiver ptr is non-null")),
                        );
                    }
                }
            });

            // Consumer: claim as fast as possible; each claim must read a stamp
            // whose low byte equals the claimed stream (payload coherently
            // published by arm's Release before the summary bit).
            let mb_c = Arc::clone(&mb);
            let claims_c = Arc::clone(&claims);
            let barrier_c = Arc::clone(&barrier);
            scope.spawn(move || {
                barrier_c.wait();
                let total = STREAMS * ROUNDS;
                while claims_c.load(Ordering::Relaxed) < total {
                    if let Some((StreamId(s), mut baton)) = mb_c.try_claim() {
                        // SAFETY: exactly one claimed baton, unaliased.
                        let recv = unsafe { baton.as_mut() };
                        assert_eq!(
                            recv.stamp & 0xFF,
                            s as u64,
                            "claimed baton payload matches its stream (publish edge held)"
                        );
                        assert_eq!(recv.stream, s, "baton points at its own receiver");
                        claims_c.fetch_add(1, Ordering::Relaxed);
                    } else {
                        std::hint::spin_loop();
                    }
                }
            });
        });

        assert_eq!(claims.load(Ordering::Relaxed), STREAMS * ROUNDS);
    }
}

/// Test-only probe: is every bit in `mask` clear in the summary word? Used by
/// the concurrent stress to preserve one-live-baton-per-stream (re-arm only
/// after the previous baton was claimed). Not part of the public API.
#[cfg(all(test, not(cayenne_loom)))]
fn atomic_summary_bit_clear<R>(mb: &PriorityMailbox<R>, mask: u64) -> bool {
    mb.summary.load(Ordering::Relaxed) & mask == 0
}

// ---------------------------------------------------------------------------
// loom model-check of the mailbox arm/claim orderings. Compiled ONLY under
// `RUSTFLAGS="--cfg cayenne_loom"`; run with:
//   RUSTFLAGS="--cfg cayenne_loom" cargo test -p cayenne --lib task_queue::loom_tests --release
// ---------------------------------------------------------------------------
#[cfg(all(test, cayenne_loom))]
mod loom_tests {
    use super::{Baton, PriorityMailbox, StreamId};
    use loom::cell::UnsafeCell;
    use loom::sync::atomic::{AtomicUsize, Ordering};
    use loom::sync::Arc;
    use std::ptr::NonNull;

    /// A minimal receiver whose `stamp` the producer writes before arming; the
    /// claimer must observe it after the `Acquire` swap (the publish edge). The
    /// payload lives in a `loom::cell::UnsafeCell` so loom *tracks* the
    /// non-atomic access and flags a data race if the arm→claim atomic edge
    /// failed to establish happens-before between the producer's write and the
    /// consumer's read.
    struct Receiver {
        stamp: UnsafeCell<u64>,
    }

    /// Publish edge: one producer arms stream 0 after stamping its receiver; one
    /// consumer spins `try_claim` until it wins. loom explores every interleaving
    /// of `arm` (`store(Release)` → `fetch_or(Release)`) against `try_claim`
    /// (`load(Relaxed)` gate → `swap(Acquire)` → `fetch_and(Release)`) and
    /// checks the consumer observes the stamp the producer wrote *before* arming
    /// — i.e. the `Acquire` swap synchronizes-with the `Release` store.
    #[test]
    fn arm_claim_publishes_payload() {
        loom::model(|| {
            let mb: Arc<PriorityMailbox<Receiver>> = Arc::new(PriorityMailbox::new(1));
            let r = Box::into_raw(Box::new(Receiver {
                stamp: UnsafeCell::new(0),
            }));

            let mb_p = Arc::clone(&mb);
            let producer = loom::thread::spawn(move || {
                // SAFETY: only this producer touches `r` before arming it, and
                // the slot is empty (first and only arm), so access is unique.
                unsafe { (*r).stamp.with_mut(|p| *p = 0xA5) };
                mb_p.arm(StreamId(0), Baton::new(NonNull::new(r).unwrap()));
            });

            // Consumer: spin until the single arm becomes visible. Bounded in
            // every interleaving — the producer arms in finitely many steps.
            let claimed = loop {
                if let Some((StreamId(s), mut baton)) = mb.try_claim() {
                    assert_eq!(s, 0);
                    // SAFETY: single claimed baton, unaliased by the invariant;
                    // the `Acquire` swap synchronizes-with the producer's arm.
                    break unsafe { baton.as_mut().stamp.with(|p| *p) };
                }
                loom::thread::yield_now();
            };
            assert_eq!(claimed, 0xA5, "claim observed the pre-arm stamp write");

            producer.join().unwrap();
            // SAFETY: the baton was claimed exactly once; no live aliases remain.
            unsafe { drop(Box::from_raw(r)) };
        });
    }

    /// No duplicate claim: one stream is armed, then two consumers race a single
    /// `try_claim` each. The single-slot `swap(null)` guarantees exactly one wins
    /// a non-null baton; the loser's swap returns null. loom checks the total
    /// successful claim count is exactly 1 across all interleavings (the
    /// never-duplicated invariant holds structurally, no generation counter).
    ///
    /// The baton is armed BEFORE the consumers spawn (so each consumer does ONE
    /// bounded `try_claim`, no spin loop) — this keeps loom's state space to the
    /// two racing swaps, which is exactly the property under test; the publish
    /// *edge* (arm concurrent with claim) is covered separately by
    /// [`arm_claim_publishes_payload`].
    #[test]
    fn concurrent_claims_never_duplicate() {
        loom::model(|| {
            let mb: Arc<PriorityMailbox<Receiver>> = Arc::new(PriorityMailbox::new(1));
            let total = Arc::new(AtomicUsize::new(0));
            let r = Box::into_raw(Box::new(Receiver {
                stamp: UnsafeCell::new(0xBEEF),
            }));
            // Arm before spawning: the two consumers then race a single claim.
            mb.arm(StreamId(0), Baton::new(NonNull::new(r).unwrap()));

            let consumer = |mb: Arc<PriorityMailbox<Receiver>>, total: Arc<AtomicUsize>| {
                loom::thread::spawn(move || {
                    if mb.try_claim().is_some() {
                        total.fetch_add(1, Ordering::Relaxed);
                    }
                })
            };
            let c0 = consumer(Arc::clone(&mb), Arc::clone(&total));
            let c1 = consumer(Arc::clone(&mb), Arc::clone(&total));

            c0.join().unwrap();
            c1.join().unwrap();

            assert_eq!(total.load(Ordering::Relaxed), 1, "exactly one claim wins");
            // SAFETY: claimed exactly once; no live aliases.
            unsafe { drop(Box::from_raw(r)) };
        });
    }
}
