/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Request/response correlation for the scheduler↔executor control stream.
//!
//! The control stream is a single bidirectional `mpsc` pair per executor connection.
//! Some scheduler→executor commands need a typed reply correlated by `request_id`
//! (metrics, partition-update acks, etc.). [`CorrelatedResponses`] holds the
//! `request_id → oneshot::Sender<Resp>` map per response type, and
//! [`send_correlated`] drives the full register-send-await-cleanup lifecycle so
//! callers don't reimplement it per RPC.

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use snafu::prelude::*;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

/// Per-response-type pending-request registry. Keyed by `request_id`.
///
/// Cheap to clone (internal `Arc<DashMap>`). One instance per response type per
/// connection (e.g. one for `MetricsResponse`, one for `Ack`).
#[derive(Debug)]
pub struct CorrelatedResponses<Resp> {
    pending: Arc<DashMap<String, oneshot::Sender<Resp>>>,
}

impl<Resp> Default for CorrelatedResponses<Resp> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Resp> Clone for CorrelatedResponses<Resp> {
    fn clone(&self) -> Self {
        Self {
            pending: Arc::clone(&self.pending),
        }
    }
}

impl<Resp> CorrelatedResponses<Resp> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            pending: Arc::new(DashMap::new()),
        }
    }

    /// Registers a pending request and returns the receiver to await its response.
    /// Prefer [`send_correlated`] for the common case; this is exposed for callers
    /// that need to interleave the register/send steps.
    pub fn register(&self, id: String) -> oneshot::Receiver<Resp> {
        let (tx, rx) = oneshot::channel();
        self.pending.insert(id, tx);
        rx
    }

    /// Drops the pending entry without delivering. Used to clean up on send
    /// failure or timeout.
    pub fn remove(&self, id: &str) {
        self.pending.remove(id);
    }

    /// Atomically removes the pending entry and delivers the response.
    /// Returns `true` if the `request_id` was known and the receiver hadn't dropped.
    pub fn deliver(&self, id: &str, resp: Resp) -> bool {
        match self.pending.remove(id) {
            Some((_, sender)) => sender.send(resp).is_ok(),
            None => false,
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

/// Failure modes for [`send_correlated`].
#[derive(Debug, Snafu)]
pub enum CorrelationError {
    #[snafu(display("Failed to send request: control stream channel closed"))]
    SendFailed,

    #[snafu(display("Response channel closed before delivery"))]
    Cancelled,

    #[snafu(display("Timed out after {duration:?} waiting for response"))]
    Timeout { duration: Duration },
}

/// Drives the full lifecycle of a correlated request: generates a `request_id`,
/// registers a pending response slot, builds and sends the request, waits for the
/// response (optionally bounded by a timeout), and cleans up the slot on every
/// failure path.
///
/// `build_req` receives the generated `request_id` so the caller can place it into
/// the appropriate field of whatever request message type it's constructing.
///
/// Pass `Some(duration)` to bound the wait; `None` to wait indefinitely (the
/// caller is then responsible for ensuring the pending entry doesn't leak — e.g.
/// the message channel itself will drop the sender on disconnect).
pub async fn send_correlated<Req, Resp>(
    request_tx: &mpsc::Sender<Req>,
    pending: &CorrelatedResponses<Resp>,
    build_req: impl FnOnce(String) -> Req,
    timeout: Option<Duration>,
) -> Result<Resp, CorrelationError> {
    let id = Uuid::new_v4().to_string();
    let rx = pending.register(id.clone());

    if request_tx.send(build_req(id.clone())).await.is_err() {
        pending.remove(&id);
        return SendFailedSnafu.fail();
    }

    let recv_result = match timeout {
        Some(duration) => match tokio::time::timeout(duration, rx).await {
            Ok(inner) => inner,
            Err(_) => {
                pending.remove(&id);
                return TimeoutSnafu { duration }.fail();
            }
        },
        None => rx.await,
    };

    match recv_result {
        Ok(resp) => Ok(resp),
        // Sender was dropped without delivering (e.g. pending entry was
        // explicitly removed via [`CorrelatedResponses::remove`]).
        Err(_) => CancelledSnafu.fail(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn register_then_deliver_completes_receiver() {
        let pending: CorrelatedResponses<u32> = CorrelatedResponses::new();
        let rx = pending.register("a".to_string());
        assert_eq!(pending.len(), 1);

        assert!(pending.deliver("a", 42));
        assert_eq!(rx.await.unwrap(), 42);
        assert!(pending.is_empty());
    }

    #[tokio::test]
    async fn deliver_unknown_id_returns_false() {
        let pending: CorrelatedResponses<u32> = CorrelatedResponses::new();
        assert!(!pending.deliver("missing", 1));
    }

    #[tokio::test]
    async fn remove_cancels_pending_receiver() {
        let pending: CorrelatedResponses<u32> = CorrelatedResponses::new();
        let rx = pending.register("a".to_string());
        pending.remove("a");
        // Sender was dropped, receiver sees Err.
        assert!(rx.await.is_err());
    }

    #[tokio::test]
    async fn send_correlated_happy_path() {
        let (tx, mut rx) = mpsc::channel::<(String, u32)>(8);
        let pending: CorrelatedResponses<u32> = CorrelatedResponses::new();
        let pending_for_responder = pending.clone();

        // Simulated responder: takes the (id, value) off the channel and delivers.
        tokio::spawn(async move {
            while let Some((id, value)) = rx.recv().await {
                pending_for_responder.deliver(&id, value * 2);
            }
        });

        let resp = send_correlated(
            &tx,
            &pending,
            |id| (id, 21),
            Some(Duration::from_secs(1)),
        )
        .await
        .unwrap();

        assert_eq!(resp, 42);
        assert!(pending.is_empty());
    }

    #[tokio::test]
    async fn send_correlated_no_timeout_waits_until_delivered() {
        let (tx, mut rx) = mpsc::channel::<(String, u32)>(8);
        let pending: CorrelatedResponses<u32> = CorrelatedResponses::new();
        let pending_for_responder = pending.clone();

        tokio::spawn(async move {
            while let Some((id, value)) = rx.recv().await {
                tokio::time::sleep(Duration::from_millis(20)).await;
                pending_for_responder.deliver(&id, value + 1);
            }
        });

        let resp = send_correlated(&tx, &pending, |id| (id, 7), None).await.unwrap();
        assert_eq!(resp, 8);
        assert!(pending.is_empty());
    }

    #[tokio::test]
    async fn send_correlated_send_failure_cleans_up() {
        let (tx, rx) = mpsc::channel::<String>(1);
        drop(rx); // close the receiver so send fails
        let pending: CorrelatedResponses<u32> = CorrelatedResponses::new();

        let err = send_correlated(&tx, &pending, |id| id, Some(Duration::from_secs(1)))
            .await
            .unwrap_err();
        assert!(matches!(err, CorrelationError::SendFailed));
        assert!(pending.is_empty());
    }

    #[tokio::test]
    async fn send_correlated_timeout_cleans_up() {
        let (tx, _rx) = mpsc::channel::<String>(1);
        let pending: CorrelatedResponses<u32> = CorrelatedResponses::new();

        let err = send_correlated(&tx, &pending, |id| id, Some(Duration::from_millis(50)))
            .await
            .unwrap_err();
        assert!(matches!(err, CorrelationError::Timeout { .. }));
        assert!(pending.is_empty());
    }
}
