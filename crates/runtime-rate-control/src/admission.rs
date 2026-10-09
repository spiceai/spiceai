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

//! First-in, first-out admission to a controller's local limits.
//!
//! A request waits for a concurrency slot, then for cells from each local
//! quota. Each of the two stages has an [`AdmissionQueue`]: a request that has
//! to wait joins it, and only the request at its head waits on the limit
//! itself, so a request is admitted after every request that joined the queue
//! before it. A request that arrives while the queue is empty tries the limit
//! at once and skips the queue if the limit admits it, so an uncontended limit
//! costs no lock.
//!
//! That first try and the join after a failed one are two steps, so another
//! request's first try can be admitted between them. Only a request whose own
//! arrival overlaps can pass this way; once a request has joined, nothing
//! passes it. Making the two steps one would put every try behind the queue's
//! lock, and under contention that lock's first-in, first-out handoff becomes
//! the bottleneck.
//!
//! The request at the head reads its charge when it gets there and re-reads it
//! while it waits, so it pays what the adaptive admission coefficient asks for
//! when it is admitted. A charge fixed on arrival would strand a request that
//! queued while the origin was failing: it would still ask for most of a bucket
//! after the origin recovered, while requests arriving later at the healthy
//! charge took each cell as it refilled.
//!
//! The queue matters as much. Without it, every waiter computes the same ready
//! time, wakes and races for the cell, and a loser waits again with no bound on
//! how often.
//!
//! The leased cluster buckets are not part of this: they register demand at
//! entry and pace their own waiters.

use std::{
    fmt,
    num::NonZeroU32,
    ops::Add,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use governor::{
    Quota, RateLimiter,
    clock::{Clock, Reference},
    middleware::NoOpMiddleware,
    nanos::Nanos,
    state::{InMemoryState, NotKeyed},
};
use tokio::{
    sync::{Mutex, MutexGuard, Notify, OwnedSemaphorePermit, Semaphore, TryAcquireError},
    time::Instant,
};

use crate::{Error, Result};

/// A governor clock on tokio time, so a quota's ready time and the sleep that
/// waits for it read the same clock, and a paused test clock pauses both.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct TokioClock;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TokioInstant(Instant);

impl Add<Nanos> for TokioInstant {
    type Output = Self;

    fn add(self, other: Nanos) -> Self {
        // Overflows only centuries past the limiter's start; saturate rather
        // than panic.
        self.0.checked_add(other.into()).map_or(self, Self)
    }
}

impl Reference for TokioInstant {
    fn duration_since(&self, earlier: Self) -> Nanos {
        self.0.saturating_duration_since(earlier.0).into()
    }

    fn saturating_sub(&self, duration: Nanos) -> Self {
        self.0.checked_sub(duration.into()).map_or(*self, Self)
    }
}

impl Clock for TokioClock {
    type Instant = TokioInstant;

    fn now(&self) -> Self::Instant {
        TokioInstant(Instant::now())
    }
}

type GovernorRateLimiter =
    RateLimiter<NotKeyed, InMemoryState, TokioClock, NoOpMiddleware<TokioInstant>>;

/// The first-in, first-out queue in front of one limit.
#[derive(Debug, Default)]
pub(crate) struct AdmissionQueue {
    /// The head of the queue. Tokio's mutex grants it in the order it was
    /// asked for.
    head: Mutex<()>,
    /// Requests in the queue, its head included.
    waiting: AtomicUsize,
}

impl AdmissionQueue {
    /// Where a request that has just arrived stands: in the queue when others
    /// are already waiting, so it cannot overtake them, and outside it
    /// otherwise, free to take the limit at once if the limit allows.
    pub(crate) async fn arrive(&self) -> Place<'_> {
        let turn = if self.waiting.load(Ordering::SeqCst) == 0 {
            None
        } else {
            Some(self.join().await)
        };
        Place { queue: self, turn }
    }

    async fn join(&self) -> Turn<'_> {
        // Counted before the wait, and uncounted on drop, so a request
        // cancelled while it waits leaves the count right.
        let waiting = Waiting::register(&self.waiting);
        let head = self.head.lock().await;
        Turn {
            _head: head,
            _waiting: waiting,
        }
    }
}

/// Where one request stands with respect to an [`AdmissionQueue`].
pub(crate) struct Place<'a> {
    queue: &'a AdmissionQueue,
    turn: Option<Turn<'a>>,
}

impl Place<'_> {
    /// Whether this request holds the head of the queue.
    fn at_head(&self) -> bool {
        self.turn.is_some()
    }

    /// Wait in the queue until this request reaches its head.
    async fn wait_for_head(&mut self) {
        if self.turn.is_none() {
            self.turn = Some(self.queue.join().await);
        }
    }
}

/// The head of an [`AdmissionQueue`], held until the request is admitted.
struct Turn<'a> {
    _head: MutexGuard<'a, ()>,
    _waiting: Waiting<'a>,
}

/// One count in [`AdmissionQueue::waiting`], removed on drop.
struct Waiting<'a>(&'a AtomicUsize);

impl<'a> Waiting<'a> {
    fn register(waiting: &'a AtomicUsize) -> Self {
        waiting.fetch_add(1, Ordering::SeqCst);
        Self(waiting)
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// One local token bucket, waited on by the head of its controller's quota
/// queue.
pub(crate) struct LocalQuota {
    limiter: GovernorRateLimiter,
    /// The bucket size in cells: the most one request can be charged.
    capacity: u32,
    /// How often a request waiting on this bucket re-reads its charge. `None`
    /// when the charge cannot change while it waits.
    reread: Option<Duration>,
}

impl fmt::Debug for LocalQuota {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalQuota")
            .field("capacity", &self.capacity)
            .field("reread", &self.reread)
            .finish_non_exhaustive()
    }
}

impl LocalQuota {
    pub(crate) fn new(quota: Quota, reread: Option<Duration>) -> Self {
        Self {
            limiter: RateLimiter::direct_with_clock(quota, TokioClock),
            capacity: quota.burst_size().get(),
            reread,
        }
    }

    /// Take `cells(capacity)` cells from the bucket, and return how many were
    /// taken. If the bucket does not hold them, wait in `place`'s queue and
    /// then at its head until it does. `cells` is read again each time the
    /// request tries, so a charge that falls while it waits takes effect.
    ///
    /// Every request that waits on this bucket uses the same queue, so the
    /// cells the head waits for are never taken by a request behind it.
    ///
    /// # Errors
    ///
    /// [`Error::InsufficientCapacity`] when the charge is larger than the
    /// whole bucket.
    pub(crate) async fn take(
        &self,
        place: &mut Place<'_>,
        cells: impl Fn(u32) -> u32,
    ) -> Result<u32> {
        loop {
            let weight = cells(self.capacity);
            let Some(nonzero) = NonZeroU32::new(weight) else {
                return Ok(0);
            };
            let Err(not_until) = self
                .limiter
                .check_n(nonzero)
                .map_err(|_| Error::InsufficientCapacity { weight })?
            else {
                return Ok(weight);
            };
            if place.at_head() {
                let ready_in = not_until.wait_time_from(TokioClock.now());
                let wait = self.reread.map_or(ready_in, |reread| ready_in.min(reread));
                tokio::time::sleep(wait).await;
            } else {
                place.wait_for_head().await;
            }
        }
    }
}

/// The local concurrency limit: a semaphore that a request takes from at once
/// when nobody is waiting, and otherwise from the head of its queue.
#[derive(Debug)]
pub(crate) struct ConcurrencyLimit {
    semaphore: Arc<Semaphore>,
    /// The semaphore size in permits: the most one request can hold.
    capacity: u32,
    queue: AdmissionQueue,
    /// Signalled whenever a request returns its permits or reports an outcome,
    /// so the head re-reads its charge against what is now free.
    changed: Notify,
}

impl ConcurrencyLimit {
    pub(crate) fn new(permits: usize) -> Arc<Self> {
        Arc::new(Self {
            semaphore: Arc::new(Semaphore::new(permits)),
            capacity: u32::try_from(permits).unwrap_or(u32::MAX),
            queue: AdmissionQueue::default(),
            changed: Notify::new(),
        })
    }

    pub(crate) fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Wake the head of the queue to re-read its charge. Called when a request
    /// reports an outcome, which moves the adaptive coefficient, and when it
    /// returns its permits.
    pub(crate) fn recheck(&self) {
        self.changed.notify_waiters();
    }

    /// Wait, in arrival order, until `permits(capacity)` permits are free, then
    /// hold them until the returned permit drops. `permits` is read again each
    /// time the request at the head wakes.
    ///
    /// The head is waiting only because requests in flight hold permits, and
    /// each of them wakes it when it reports its outcome and again when it
    /// returns them. A request can stay in flight for a long time, though, so
    /// the head also wakes when `falls_to_in(free)` says the charge, with
    /// nothing else changing, will have fallen to the `free` permits.
    ///
    /// # Errors
    ///
    /// Only if the semaphore is closed, which this crate never does.
    pub(crate) async fn acquire(
        self: &Arc<Self>,
        permits: impl Fn(u32) -> u32,
        falls_to_in: impl Fn(u32) -> Option<Duration>,
    ) -> Result<ConcurrencyPermit, TryAcquireError> {
        let mut place = self.queue.arrive().await;
        if !place.at_head() {
            match self.try_acquire(permits(self.capacity)) {
                Err(TryAcquireError::NoPermits) => place.wait_for_head().await,
                admitted_or_closed => return admitted_or_closed,
            }
        }
        loop {
            // Listen before reading the semaphore, so a change between the
            // read and the wait still wakes this request.
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();

            match self.try_acquire(permits(self.capacity)) {
                Err(TryAcquireError::NoPermits) => {
                    let free =
                        u32::try_from(self.semaphore.available_permits()).unwrap_or(u32::MAX);
                    match falls_to_in(free) {
                        // The floor keeps a charge that rounding holds a hair
                        // above the free permits from spinning.
                        Some(fits_in) => {
                            let _changed_or_fits = tokio::time::timeout(
                                fits_in.max(Duration::from_millis(1)),
                                changed,
                            )
                            .await;
                        }
                        None => changed.await,
                    }
                }
                admitted_or_closed => return admitted_or_closed,
            }
        }
    }

    fn try_acquire(self: &Arc<Self>, permits: u32) -> Result<ConcurrencyPermit, TryAcquireError> {
        let permit = Arc::clone(&self.semaphore).try_acquire_many_owned(permits)?;
        Ok(ConcurrencyPermit {
            permit: Some(permit),
            limit: Arc::clone(self),
        })
    }
}

/// Permits held against a [`ConcurrencyLimit`]; dropping it returns them and
/// wakes the head of the queue.
#[derive(Debug)]
pub(crate) struct ConcurrencyPermit {
    permit: Option<OwnedSemaphorePermit>,
    limit: Arc<ConcurrencyLimit>,
}

impl ConcurrencyPermit {
    /// The number of permits held.
    pub(crate) fn permits(&self) -> u32 {
        self.permit.as_ref().map_or(0, |permit| {
            u32::try_from(permit.num_permits()).unwrap_or(u32::MAX)
        })
    }
}

impl Drop for ConcurrencyPermit {
    fn drop(&mut self) {
        // Return the permits before waking the head, so it sees them.
        drop(self.permit.take());
        self.limit.recheck();
    }
}
