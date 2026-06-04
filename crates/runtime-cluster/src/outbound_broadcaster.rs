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
//!
//! Also caches the latest `PartitionsLoaded` payload per table and replays
//! the cache to each scheduler when its control stream (re)connects, so
//! readiness acks emitted before any scheduler was connected (e.g. small
//! datasets that finish loading faster than the control stream is
//! established) are not lost.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use runtime_proto::{
    BytesArray, ExecutorControlMessage, ExecutorStatistics, PartitionsLoaded,
    executor_control_message::Message as ExecutorMessage,
};
use tokio::sync::{RwLock, mpsc};

/// Per-scheduler timeout for queueing an outbound message. Keeps a stuck/slow
/// scheduler from blocking sends to its peers.
const SEND_TIMEOUT: Duration = Duration::from_secs(5);

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
    /// Latest `PartitionsLoaded` payload per table, replayed to schedulers on
    /// (re)connect. Small/fast datasets can finish their initial load before
    /// any scheduler control stream is established; without this replay the
    /// readiness ack would be broadcast to zero schedulers and lost, leaving
    /// the dataset (and the cluster) stuck in `Refreshing` forever.
    latest_partitions_loaded: RwLock<HashMap<String, Vec<Vec<u8>>>>,
}

impl ExecutorOutboundBroadcaster {
    #[must_use]
    pub fn new(executor_id: String) -> Self {
        Self {
            inner: Arc::new(ExecutorOutboundBroadcasterInner {
                streams: RwLock::new(HashMap::new()),
                executor_id: RwLock::new(executor_id),
                latest_partitions_loaded: RwLock::new(HashMap::new()),
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

    /// Replays the latest cached `PartitionsLoaded` ack for every table to a
    /// single scheduler over `tx`, that scheduler's just-established control
    /// stream sender. The control-stream client calls this right after the
    /// bidirectional stream is established. Datasets that finished their
    /// initial load before the control stream existed (fast initial loads of
    /// small datasets), or before a restarted scheduler reconnected (losing
    /// its in-memory load tracker), would otherwise never deliver their
    /// readiness ack and stay `Refreshing` forever.
    ///
    /// Takes the sender explicitly (rather than looking it up by address) so
    /// the replay is pinned to the connection that triggered it — on a fast
    /// disconnect/reconnect it fails fast on the dead channel instead of
    /// filling the successor connection's bounded channel before that
    /// stream is established and draining it.
    ///
    /// Must only be called once the stream is established — the channel is
    /// bounded, so replaying more tables than its capacity before anything
    /// consumes it would wedge the connection setup.
    ///
    /// Ordering: `broadcast_partitions_loaded` writes the cache *before*
    /// snapshotting `streams`, the sender is registered before the stream is
    /// established, and this replay reads the cache after both. A concurrent
    /// broadcast therefore always reaches a connecting scheduler — via the
    /// replay (cache write happened before the replay read), the live
    /// broadcast (snapshot saw the registered sender), or both. Duplicates
    /// are fine: the scheduler-side `PartitionLoadTracker` treats acks as
    /// idempotent replacements.
    ///
    /// Returns the number of acks queued for the scheduler.
    pub async fn replay_partitions_loaded(
        &self,
        scheduler_address: &str,
        tx: &mpsc::Sender<ExecutorControlMessage>,
    ) -> usize {
        let cached: Vec<(String, Vec<Vec<u8>>)> = {
            let cache = self.inner.latest_partitions_loaded.read().await;
            cache
                .iter()
                .map(|(table, bytes)| (table.clone(), bytes.clone()))
                .collect()
        };
        if cached.is_empty() {
            return 0;
        }

        let executor_id = self.inner.executor_id.read().await.clone();
        let mut replayed = 0usize;
        for (table_name, partition_expr_bytes) in cached {
            let payload =
                partitions_loaded_message(executor_id.clone(), table_name, partition_expr_bytes);
            match tokio::time::timeout(SEND_TIMEOUT, tx.send(payload)).await {
                Ok(Ok(())) => replayed += 1,
                Ok(Err(err)) => {
                    tracing::debug!(
                        "PartitionsLoaded replay to {scheduler_address} failed (channel closed): {err}"
                    );
                }
                Err(_) => {
                    tracing::warn!(
                        scheduler = %scheduler_address,
                        "Timed out replaying PartitionsLoaded; scheduler may miss this ack until next refresh"
                    );
                }
            }
        }
        tracing::info!(
            scheduler = %scheduler_address,
            count = replayed,
            "Replayed cached PartitionsLoaded acks to connected scheduler"
        );
        replayed
    }

    pub async fn unregister(&self, scheduler_address: &str) {
        self.inner.streams.write().await.remove(scheduler_address);
    }

    /// Broadcasts a `PartitionsLoaded` message to every connected scheduler.
    /// Returns the number of schedulers the message was queued for.
    ///
    /// The payload is also cached (latest per table) so it can be replayed to
    /// schedulers that connect later — see [`Self::replay_partitions_loaded`].
    /// A return value of `0` therefore no longer means the ack is lost, only
    /// deferred.
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
        // Cache before snapshotting streams — see the ordering note on
        // `replay_partitions_loaded` for why this guarantees a concurrently
        // connecting scheduler can't miss the ack.
        self.inner
            .latest_partitions_loaded
            .write()
            .await
            .insert(table_name.clone(), partition_expr_bytes.clone());

        let executor_id = self.inner.executor_id.read().await.clone();
        let payload = partitions_loaded_message(executor_id, table_name, partition_expr_bytes);

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
            match tokio::time::timeout(SEND_TIMEOUT, tx.send(payload.clone())).await {
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

    /// Broadcasts a per-table [`ExecutorStatistics`] report to every connected
    /// scheduler. Decoupled from `PartitionsLoaded` (readiness) so it can be sent
    /// periodically for any table the executor serves, including cayenne catalog
    /// tables. Best-effort with a short per-scheduler timeout.
    pub async fn broadcast_executor_statistics(
        &self,
        table_name: String,
        statistics: Vec<u8>,
        column_names: Vec<String>,
    ) -> usize {
        let executor_id = self.inner.executor_id.read().await.clone();
        let payload = ExecutorControlMessage {
            executor_id,
            message: Some(ExecutorMessage::ExecutorStatistics(ExecutorStatistics {
                table_name,
                statistics,
                column_names,
            })),
        };

        let targets: Vec<(String, mpsc::Sender<ExecutorControlMessage>)> = {
            let streams = self.inner.streams.read().await;
            streams
                .iter()
                .map(|(addr, tx)| (addr.clone(), tx.clone()))
                .collect()
        };

        let mut sent = 0usize;
        for (scheduler_address, tx) in targets {
            match tokio::time::timeout(SEND_TIMEOUT, tx.send(payload.clone())).await {
                Ok(Ok(())) => sent += 1,
                Ok(Err(err)) => {
                    tracing::debug!(
                        "ExecutorStatistics send to {scheduler_address} failed (channel closed): {err}"
                    );
                }
                Err(_) => {
                    tracing::debug!(
                        scheduler = %scheduler_address,
                        "Timed out sending ExecutorStatistics"
                    );
                }
            }
        }
        sent
    }
}

fn partitions_loaded_message(
    executor_id: String,
    table_name: String,
    partition_expr_bytes: Vec<Vec<u8>>,
) -> ExecutorControlMessage {
    ExecutorControlMessage {
        executor_id,
        message: Some(ExecutorMessage::PartitionsLoaded(PartitionsLoaded {
            table_name,
            partition_expr_bytes: Some(BytesArray {
                items: partition_expr_bytes,
            }),
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expect_partitions_loaded(msg: ExecutorControlMessage) -> PartitionsLoaded {
        match msg.message {
            Some(ExecutorMessage::PartitionsLoaded(loaded)) => loaded,
            other => panic!("expected PartitionsLoaded, got {other:?}"),
        }
    }

    /// Regression test for the small-dataset readiness race (#11152): a
    /// dataset that finishes loading before any scheduler control stream is
    /// connected broadcasts to zero schedulers; the cached ack must be
    /// replayed when a scheduler connects afterwards.
    #[tokio::test]
    async fn replay_delivers_ack_broadcast_before_any_scheduler_connected() {
        let broadcaster = ExecutorOutboundBroadcaster::new("exec-1".to_string());

        let sent = broadcaster
            .broadcast_partitions_loaded("spice.public.region".to_string(), vec![vec![1, 2]])
            .await;
        assert_eq!(sent, 0, "no scheduler is connected yet");

        let (tx, mut rx) = mpsc::channel(8);
        broadcaster
            .register("scheduler-1".to_string(), tx.clone())
            .await;
        let replayed = broadcaster
            .replay_partitions_loaded("scheduler-1", &tx)
            .await;
        assert_eq!(replayed, 1);

        let msg = rx.recv().await.expect("replayed ack");
        assert_eq!(msg.executor_id, "exec-1");
        let loaded = expect_partitions_loaded(msg);
        assert_eq!(loaded.table_name, "spice.public.region");
        assert_eq!(
            loaded
                .partition_expr_bytes
                .expect("partition bytes present")
                .items,
            vec![vec![1, 2]]
        );
    }

    /// Re-broadcasting for the same table must overwrite the cached payload
    /// so a later-connecting scheduler sees only the latest assignment set.
    #[tokio::test]
    async fn replay_sends_latest_payload_per_table() {
        let broadcaster = ExecutorOutboundBroadcaster::new("exec-1".to_string());

        broadcaster
            .broadcast_partitions_loaded("spice.public.orders".to_string(), vec![vec![1]])
            .await;
        broadcaster
            .broadcast_partitions_loaded("spice.public.orders".to_string(), vec![vec![2], vec![3]])
            .await;

        let (tx, mut rx) = mpsc::channel(8);
        // Only the latest payload is cached — exactly one replay per table.
        assert_eq!(
            broadcaster
                .replay_partitions_loaded("scheduler-1", &tx)
                .await,
            1
        );

        let loaded = expect_partitions_loaded(rx.recv().await.expect("replayed ack"));
        assert_eq!(loaded.table_name, "spice.public.orders");
        assert_eq!(
            loaded
                .partition_expr_bytes
                .expect("partition bytes present")
                .items,
            vec![vec![2], vec![3]]
        );
        assert!(
            rx.try_recv().is_err(),
            "expected a single replayed ack for the table"
        );
    }

    /// A live broadcast to a connected scheduler still works, and a second
    /// scheduler connecting later gets the same ack via replay (covers
    /// scheduler restart/reconnect, which loses the in-memory load tracker).
    #[tokio::test]
    async fn live_broadcast_delivered_and_replayed_to_late_scheduler() {
        let broadcaster = ExecutorOutboundBroadcaster::new("exec-1".to_string());

        let (tx1, mut rx1) = mpsc::channel(8);
        broadcaster
            .register("scheduler-1".to_string(), tx1.clone())
            .await;
        // Nothing cached yet — connecting must not emit anything.
        assert_eq!(
            broadcaster
                .replay_partitions_loaded("scheduler-1", &tx1)
                .await,
            0
        );
        assert!(rx1.try_recv().is_err());

        let sent = broadcaster
            .broadcast_partitions_loaded("spice.public.lineitem".to_string(), vec![vec![7]])
            .await;
        assert_eq!(sent, 1);
        let loaded = expect_partitions_loaded(rx1.recv().await.expect("live ack"));
        assert_eq!(loaded.table_name, "spice.public.lineitem");

        let (tx2, mut rx2) = mpsc::channel(8);
        broadcaster
            .register("scheduler-2".to_string(), tx2.clone())
            .await;
        assert_eq!(
            broadcaster
                .replay_partitions_loaded("scheduler-2", &tx2)
                .await,
            1
        );
        let loaded = expect_partitions_loaded(rx2.recv().await.expect("replayed ack"));
        assert_eq!(loaded.table_name, "spice.public.lineitem");
        assert_eq!(
            loaded
                .partition_expr_bytes
                .expect("partition bytes present")
                .items,
            vec![vec![7]]
        );
    }

    /// Replaying onto a torn-down stream (receiver dropped) fails fast and
    /// reports zero queued acks — the next reconnect replays.
    #[tokio::test]
    async fn replay_on_closed_channel_queues_nothing() {
        let broadcaster = ExecutorOutboundBroadcaster::new("exec-1".to_string());
        broadcaster
            .broadcast_partitions_loaded("spice.public.region".to_string(), vec![vec![1]])
            .await;
        let (tx, rx) = mpsc::channel(8);
        drop(rx);
        assert_eq!(
            broadcaster
                .replay_partitions_loaded("scheduler-1", &tx)
                .await,
            0
        );
    }
}
