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

use super::{Domain, Inner, recover_in_background};
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
}

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
        if let Some(inner) = self.inner.as_mut()
            && inner.needs_recovery()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            inner.recovering = true;
            runtime.spawn(recover_in_background(Arc::downgrade(self.slot)));
        }
        *self.slot.inner.lock() = self.inner.take();
        // Every waiter re-checks: one may wait on the reservation, not the state.
        self.slot.available.notify_waiters();
    }
}
