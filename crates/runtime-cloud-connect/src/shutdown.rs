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

//! Latched shutdown signal.
//!
//! Pairs an `AtomicBool` with a `Notify` so that a signal raised before
//! a waiter calls `wait()` is still observed. A bare `Notify` loses
//! `notify_waiters()` calls that arrive before any waiter is registered,
//! which can deadlock shutdown if the signal races with the driver's
//! reconnect loop.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Notify;

/// A latched, multi-consumer shutdown signal.
#[derive(Debug, Default)]
pub(crate) struct Shutdown {
    fired: AtomicBool,
    notify: Notify,
}

impl Shutdown {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Latch the signal and wake every current and future `wait()` /
    /// `notified()` caller.
    pub(crate) fn trigger(&self) {
        self.fired.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Has shutdown been signaled?
    pub(crate) fn is_triggered(&self) -> bool {
        self.fired.load(Ordering::SeqCst)
    }

    /// Resolve when shutdown is signaled. Returns immediately if the
    /// signal has already fired.
    pub(crate) async fn wait(&self) {
        if self.is_triggered() {
            return;
        }
        // Register the waiter before re-checking the flag — closing the
        // race where `trigger()` lands between the load above and the
        // `notified()` registration.
        let notified = self.notify.notified();
        if self.is_triggered() {
            return;
        }
        notified.await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn wait_returns_immediately_after_trigger() {
        let s = Shutdown::new();
        s.trigger();
        // Must not hang.
        tokio::time::timeout(std::time::Duration::from_millis(100), s.wait())
            .await
            .expect("wait should return after trigger");
        assert!(s.is_triggered());
    }

    #[tokio::test]
    async fn wait_resolves_when_trigger_arrives_after() {
        let s = Shutdown::new();
        let waiter = Arc::clone(&s);
        let handle = tokio::spawn(async move { waiter.wait().await });
        // Give the waiter a moment to register.
        tokio::task::yield_now().await;
        s.trigger();
        tokio::time::timeout(std::time::Duration::from_millis(100), handle)
            .await
            .expect("waiter should resolve")
            .expect("waiter task did not panic");
    }

    #[tokio::test]
    async fn trigger_before_first_waiter_is_not_lost() {
        // This is the race the previous `Notify`-only impl had: trigger
        // before any waiter registers, then start waiting — the bare
        // `notify_waiters()` would be lost. The latched flag fixes it.
        let s = Shutdown::new();
        s.trigger();
        let waiter = Arc::clone(&s);
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            tokio::spawn(async move { waiter.wait().await }),
        )
        .await
        .expect("waiter must observe the latched signal")
        .expect("waiter task did not panic");
    }
}
