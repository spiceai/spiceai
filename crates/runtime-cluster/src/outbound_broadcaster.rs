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

//! Executor-side broadcaster for unsolicited scheduler messages.
//!
//! Owns the per-scheduler outbound senders so the executor runtime can fan a
//! single `PartitionsLoaded` ack out to every scheduler it's currently
//! connected to, without the runtime needing a handle on the control-stream
//! manager.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use runtime_proto::{
    BytesArray, ExecutorControlMessage, PartitionsLoaded,
    executor_control_message::Message as ExecutorMessage,
};
use tokio::sync::{RwLock, mpsc};

/// Per-scheduler outbound senders, shared between the control-stream manager
/// and the executor runtime. The manager populates this map on each
/// (re)connect and clears entries on disconnect; the runtime uses it to
/// broadcast unsolicited messages (e.g. `PartitionsLoaded`) to every
/// scheduler the executor is currently connected to.
///
/// Cloning is cheap (just `Arc::clone`).
#[derive(Clone, Debug, Default)]
pub struct ExecutorOutboundBroadcaster {
    inner: Arc<ExecutorOutboundBroadcasterInner>,
}

#[derive(Debug, Default)]
struct ExecutorOutboundBroadcasterInner {
    streams: RwLock<HashMap<String, mpsc::Sender<ExecutorControlMessage>>>,
    executor_id: RwLock<String>,
}

impl ExecutorOutboundBroadcaster {
    #[must_use]
    pub fn new(executor_id: String) -> Self {
        Self {
            inner: Arc::new(ExecutorOutboundBroadcasterInner {
                streams: RwLock::new(HashMap::new()),
                executor_id: RwLock::new(executor_id),
            }),
        }
    }

    /// Updates the executor id stamped on outbound messages. Called once the
    /// executor's advertise address is finalised.
    pub async fn set_executor_id(&self, executor_id: String) {
        *self.inner.executor_id.write().await = executor_id;
    }

    pub async fn register(
        &self,
        scheduler_address: String,
        tx: mpsc::Sender<ExecutorControlMessage>,
    ) {
        self.inner
            .streams
            .write()
            .await
            .insert(scheduler_address, tx);
    }

    pub async fn unregister(&self, scheduler_address: &str) {
        self.inner.streams.write().await.remove(scheduler_address);
    }

    /// Broadcasts a `PartitionsLoaded` message to every connected scheduler.
    /// Returns the number of schedulers the message was queued for.
    ///
    /// Uses `send().await` with a short timeout rather than `try_send`. The
    /// scheduler's readiness gate depends on this ack arriving, so silently
    /// dropping it (e.g. on a transiently full channel) could leave a dataset
    /// stuck in `Refreshing` until the next refresh. The timeout keeps a
    /// stuck/slow scheduler from blocking the broadcast to its peers.
    pub async fn broadcast_partitions_loaded(
        &self,
        table_name: String,
        partition_expr_bytes: Vec<Vec<u8>>,
    ) -> usize {
        let executor_id = self.inner.executor_id.read().await.clone();
        let payload = ExecutorControlMessage {
            executor_id,
            message: Some(ExecutorMessage::PartitionsLoaded(PartitionsLoaded {
                table_name,
                partition_expr_bytes: Some(BytesArray {
                    items: partition_expr_bytes,
                }),
            })),
        };

        // Snapshot the (address, sender) pairs so we don't hold the read lock
        // across awaits — a slow scheduler shouldn't block register/unregister.
        let targets: Vec<(String, mpsc::Sender<ExecutorControlMessage>)> = {
            let streams = self.inner.streams.read().await;
            streams
                .iter()
                .map(|(addr, tx)| (addr.clone(), tx.clone()))
                .collect()
        };

        let mut sent = 0usize;
        for (scheduler_address, tx) in targets {
            match tokio::time::timeout(Duration::from_secs(5), tx.send(payload.clone())).await {
                Ok(Ok(())) => sent += 1,
                Ok(Err(err)) => {
                    tracing::debug!(
                        "PartitionsLoaded send to {scheduler_address} failed (channel closed): {err}"
                    );
                }
                Err(_) => {
                    tracing::warn!(
                        scheduler = %scheduler_address,
                        "Timed out sending PartitionsLoaded; scheduler may miss this ack until next refresh"
                    );
                }
            }
        }
        sent
    }
}
