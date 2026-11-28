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

//! State slots and transaction reservations.

use super::{super::state_reader::DatasetStateReader, Domain, Head, Inner, Stop, subject};
use arc_swap::ArcSwapOption;
use parking_lot::Mutex;
use std::sync::Arc;
use tokio::sync::Notify;

// The slot transfers ownership, so storage I/O never holds a mutex guard.
// A cancelled operation returns its state, including any uncertainty marker.
pub(super) struct StateSlot {
    pub(super) inner: Mutex<Option<Inner>>,
    pub(super) available: Notify,
    /// Held by a transaction across datasets from its `Prepare` to its
    /// `Resolve`: every other caller waits, so nothing on this log is refused
    /// by the reservation.
    pub(super) reserved: std::sync::atomic::AtomicBool,
    /// The state as of the last lease returned while the log served reads, or
    /// `None` while it serves none. Reads load it without taking the state, so a
    /// read never waits for a lease holder's storage I/O, and never sees a change
    /// whose commit is still in flight: a commit applies its record to the
    /// lease's state, which is published only when the lease is returned.
    pub(super) published: ArcSwapOption<DatasetStateReader>,
    /// Whether [`Self::published`] is a replica's state, which may lack the
    /// writer's latest commits until the replica follows them. Set before
    /// publishing replica state and cleared after publishing writer state.
    pub(super) replica: std::sync::atomic::AtomicBool,
    /// Runs in [`Self::published`] between its reservation check and its load,
    /// so a test can take a reservation there.
    #[cfg(test)]
    pub(super) before_load: Mutex<Option<SlotHook>>,
    /// Runs in [`Self::publish`] between publishing the role and state.
    #[cfg(test)]
    pub(super) between_role_and_state: Mutex<Option<SlotHook>>,
    /// Runs after a takeover opens the data-write gate.
    #[cfg(test)]
    pub(super) after_writer_gate_open: Mutex<Option<SlotHook>>,
}
#[cfg(test)]
pub(super) type SlotHook = Box<dyn FnOnce(&StateSlot) + Send>;
pub(super) struct StateLease<'a> {
    slot: &'a Arc<StateSlot>,
    inner: Option<Inner>,
}
impl StateSlot {
    pub(super) async fn take(self: &Arc<Self>) -> StateLease<'_> {
        self.take_as(false).await
    }

    /// The state for the transaction holding the reservation.
    pub(super) async fn take_reserved(self: &Arc<Self>) -> StateLease<'_> {
        self.take_as(true).await
    }

    async fn take_as(self: &Arc<Self>, holder: bool) -> StateLease<'_> {
        use std::sync::atomic::Ordering;
        loop {
            let notified = self.available.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let inner = if holder || !self.reserved.load(Ordering::Acquire) {
                self.inner.lock().take()
            } else {
                None
            };
            if let Some(inner) = inner {
                return StateLease {
                    slot: self,
                    inner: Some(inner),
                };
            }
            notified.await;
        }
    }

    /// The published state, unless a transaction across datasets holds the
    /// reservation: its tables are read once it resolves, as every other caller
    /// waits for it. The reservation is checked again after the load, because
    /// one taken in between may already have published this dataset's resolved
    /// state while the transaction still holds its other participants. A state
    /// published under a reservation is stored after the reservation is taken,
    /// so a load that returns it sees the reservation on the second check.
    pub(super) fn published(&self) -> Option<Arc<DatasetStateReader>> {
        use std::sync::atomic::Ordering;
        if self.reserved.load(Ordering::Acquire) {
            return None;
        }
        #[cfg(test)]
        if let Some(hook) = self.before_load.lock().take() {
            hook(self);
        }
        let published = self.published.load_full();
        if self.reserved.load(Ordering::Acquire) {
            return None;
        }
        published
    }

    /// [`Self::published`], when it is the state of the instance that writes
    /// the log. The role is checked again after the state load, so a change to
    /// replica cannot make its state appear to be the writer's. A change to
    /// writer publishes its state before exposing that role.
    pub(super) fn published_by_writer(&self) -> Option<Arc<DatasetStateReader>> {
        use std::sync::atomic::Ordering;
        if self.replica.load(Ordering::Acquire) {
            return None;
        }
        let published = self.published();
        if self.replica.load(Ordering::Acquire) {
            return None;
        }
        published
    }

    /// Publish `inner`'s state for reads, or withdraw it while the log serves
    /// none. The lease's state is shared, not copied: a change copies a shared
    /// state first ([`Inner::apply_record`]), so a state that changed is never
    /// the one already published, and a lease that changed nothing publishes
    /// nothing.
    fn publish(&self, inner: &Inner) {
        if !matches!(inner.stop, None | Some(Stop::Replica)) {
            self.published.store(None);
            return;
        }
        let state_changed = !self
            .published
            .load()
            .as_ref()
            .is_some_and(|published| Arc::ptr_eq(published, &inner.reader));
        if matches!(inner.head, Head::Replica(_)) {
            self.replica
                .store(true, std::sync::atomic::Ordering::Release);
            #[cfg(test)]
            if let Some(hook) = self.between_role_and_state.lock().take() {
                hook(self);
            }
            if state_changed {
                self.published.store(Some(Arc::clone(&inner.reader)));
            }
        } else {
            if state_changed {
                self.published.store(Some(Arc::clone(&inner.reader)));
            }
            #[cfg(test)]
            if let Some(hook) = self.between_role_and_state.lock().take() {
                hook(self);
            }
            self.replica
                .store(false, std::sync::atomic::Ordering::Release);
        }
    }

    /// Wait for the reservation and hold it until the returned guard drops.
    pub(super) async fn reserve(self: &Arc<Self>) -> Reservation<'_> {
        use std::sync::atomic::Ordering;
        let lease = self.take().await;
        self.reserved.store(true, Ordering::Release);
        drop(lease);
        Reservation { slot: self }
    }

    fn release(&self) {
        self.reserved
            .store(false, std::sync::atomic::Ordering::Release);
        self.available.notify_waiters();
    }
}
pub(super) struct Reservation<'a> {
    slot: &'a StateSlot,
}
impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        self.slot.release();
    }
}
/// A domain's reservation held through its `Arc`, so a transaction whose
/// outcome is not yet recorded keeps it after its coordinator returns.
pub(super) struct HeldReservation(pub(super) Arc<Domain>);
impl HeldReservation {
    pub(super) async fn take(domain: &Arc<Domain>) -> Self {
        std::mem::forget(domain.inner.reserve().await);
        Self(Arc::clone(domain))
    }
}
impl Drop for HeldReservation {
    fn drop(&mut self) {
        self.0.inner.release();
    }
}
impl std::ops::Deref for StateLease<'_> {
    type Target = Inner;
    fn deref(&self) -> &Inner {
        match &self.inner {
            Some(inner) => inner,
            None => unreachable!("a catalog state lease owns its state until drop"),
        }
    }
}
impl std::ops::DerefMut for StateLease<'_> {
    fn deref_mut(&mut self) -> &mut Inner {
        match &mut self.inner {
            Some(inner) => inner,
            None => unreachable!("a catalog state lease owns its state until drop"),
        }
    }
}
impl Drop for StateLease<'_> {
    fn drop(&mut self) {
        // A commit that did not finish, including one whose caller was cancelled,
        // leaves the log stopped; a background task brings it back.
        if let Some(inner) = self.inner.as_mut() {
            self.slot.publish(inner);
            if inner.needs_recovery()
                && let Ok(runtime) = tokio::runtime::Handle::try_current()
            {
                inner.recovering = true;
                runtime.spawn(recover_in_background(Arc::downgrade(self.slot)));
            }
        }
        *self.slot.inner.lock() = self.inner.take();
        // Every waiter re-checks: one may wait on the reservation, not the state.
        self.slot.available.notify_waiters();
    }
}

/// Bring a stopped log back: an unresolved commit by reopening the log and
/// resuming from what committed, a fenced one by reloading the committed state
/// for its reads. Retries with a doubling wait until it succeeds or the catalog
/// is dropped.
async fn recover_in_background(slot: std::sync::Weak<StateSlot>) {
    let mut attempt: u32 = 0;
    loop {
        let Some(strong) = slot.upgrade() else {
            return;
        };
        let (delay, done) = {
            // Past a transaction's reservation: only the reservation's holder
            // commits while it is held, so a stopped log is its holder's, and the
            // holder may be waiting for this very recovery to learn its outcome.
            let mut lease = strong.take().await;
            let delay = lease.config.retry_delay;
            match lease.recover().await {
                Ok(()) => {
                    lease.recovering = false;
                    (delay, true)
                }
                Err(error) => {
                    let wait = delay.saturating_mul(2_u32.saturating_pow(attempt.min(6)));
                    tracing::warn!(
                        "{}",
                        recovery_retry_message(&lease.dataset, &error.to_string(), wait)
                    );
                    (wait, false)
                }
            }
        };
        drop(strong);
        if done {
            return;
        }
        attempt = attempt.saturating_add(1);
        tokio::time::sleep(delay).await;
    }
}

/// Logged each time reading the state back fails.
fn recovery_retry_message(dataset: &str, cause: &str, wait: std::time::Duration) -> String {
    format!(
        "{} could not read its state back after an unconfirmed change, so it still accepts no change; it retries in {} ms. Check that the state location is reachable. Cause: {cause}",
        subject(dataset),
        wait.as_millis()
    )
}

#[cfg(test)]
mod tests {
    use super::{ArcSwapOption, DatasetStateReader, Mutex, Notify, StateSlot};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn reader() -> Arc<DatasetStateReader> {
        Arc::new(DatasetStateReader::from_state(
            cayenne_metastore_actions::state::DatasetState::default(),
            Arc::new(object_store::memory::InMemory::new()),
            object_store::path::Path::from("catalog"),
        ))
    }

    /// A reservation taken after a read checked for one, which publishes this
    /// dataset's resolved state before the read loads it, keeps the read from
    /// returning that state until the reservation is released.
    #[test]
    fn a_read_does_not_return_a_state_published_under_a_reservation() {
        let before = reader();
        let resolved = reader();
        let slot = StateSlot {
            inner: Mutex::new(None),
            available: Notify::new(),
            reserved: AtomicBool::new(false),
            published: ArcSwapOption::from(Some(Arc::clone(&before))),
            replica: AtomicBool::new(false),
            before_load: Mutex::new(None),
            between_role_and_state: Mutex::new(None),
            after_writer_gate_open: Mutex::new(None),
        };
        let published = Arc::clone(&resolved);
        *slot.before_load.lock() = Some(Box::new(move |slot: &StateSlot| {
            slot.reserved.store(true, Ordering::Release);
            slot.published.store(Some(published));
        }));
        assert!(
            slot.published().is_none(),
            "a read that loaded the state while a reservation was held returned it"
        );
        slot.release();
        let after = slot
            .published()
            .expect("the state once the reservation is released");
        assert!(Arc::ptr_eq(&after, &resolved));
    }
}
