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
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use runtime_proto::{
    BytesArray, ExecutorControlMessage, ExecutorStatistics, FlightEndpoint, PartitionsLoaded,
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

/// A load of one table, started with [`ExecutorOutboundBroadcaster::start_load`].
/// The table counts as loading until the guard of its newest load drops, so a
/// load whose task is cancelled or panics also ends.
#[derive(Debug)]
#[must_use = "the load ends when the guard drops"]
pub struct TableLoad {
    inner: Arc<ExecutorOutboundBroadcasterInner>,
    table: String,
    load_sequence: u64,
}

impl TableLoad {
    /// The sequence of this load (see [`ExecutorOutboundBroadcaster::next_load_sequence`]).
    #[must_use]
    pub fn load_sequence(&self) -> u64 {
        self.load_sequence
    }
}

impl Drop for TableLoad {
    fn drop(&mut self) {
        let mut loads = self.inner.loads_in_flight.lock();
        if loads.get(&self.table) == Some(&self.load_sequence) {
            loads.remove(&self.table);
        }
    }
}

/// A cached `PartitionsLoaded` payload and the load it reports.
#[derive(Debug, Clone)]
struct CachedAck {
    load_sequence: u64,
    partition_expr_bytes: Vec<Vec<u8>>,
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
    latest_partitions_loaded: RwLock<HashMap<String, CachedAck>>,
    /// The last load sequence handed out; see [`ExecutorOutboundBroadcaster::next_load_sequence`].
    load_sequence: AtomicU64,
    /// Per table with a load running, the sequence of the newest load started;
    /// see [`ExecutorOutboundBroadcaster::start_load`].
    loads_in_flight: parking_lot::Mutex<HashMap<String, u64>>,
    /// The executor's Flight endpoint URL once it serves queries, replayed to
    /// schedulers on (re)connect. A scheduler drops an executor's endpoint when
    /// its control stream ends, so every new stream must carry it again.
    flight_endpoint: RwLock<Option<String>>,
}

impl ExecutorOutboundBroadcaster {
    #[must_use]
    pub fn new(executor_id: String) -> Self {
        Self {
            inner: Arc::new(ExecutorOutboundBroadcasterInner {
                streams: RwLock::new(HashMap::new()),
                executor_id: RwLock::new(executor_id),
                latest_partitions_loaded: RwLock::new(HashMap::new()),
                load_sequence: AtomicU64::new(0),
                loads_in_flight: parking_lot::Mutex::default(),
                flight_endpoint: RwLock::new(None),
            }),
        }
    }

    /// Returns a sequence number for a load this process starts, greater than
    /// every one returned before. A `PartitionsLoaded` ack carries the number
    /// of the load it reports, so schedulers can tell an older load's ack that
    /// arrives late from a newer one's.
    #[must_use]
    pub fn next_load_sequence(&self) -> u64 {
        self.inner.load_sequence.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Starts a load of `table`. The table counts as loading until the
    /// [`TableLoad`] of the newest load started for it drops; drop it after
    /// broadcasting the load's `PartitionsLoaded`.
    pub fn start_load(&self, table: &str) -> TableLoad {
        let load_sequence = self.next_load_sequence();
        self.inner
            .loads_in_flight
            .lock()
            .insert(table.to_string(), load_sequence);
        TableLoad {
            inner: Arc::clone(&self.inner),
            table: table.to_string(),
            load_sequence,
        }
    }

    /// Whether a load of `table` started with [`Self::start_load`] has not ended.
    #[must_use]
    pub fn is_loading(&self, table: &str) -> bool {
        self.inner.loads_in_flight.lock().contains_key(table)
    }

    /// The partitions in the newest `PartitionsLoaded` of `table`, if this
    /// process has reported one.
    pub async fn loaded_partitions(&self, table: &str) -> Option<Vec<Vec<u8>>> {
        self.inner
            .latest_partitions_loaded
            .read()
            .await
            .get(table)
            .map(|cached| cached.partition_expr_bytes.clone())
    }

    /// Sends the newest `PartitionsLoaded` of `table` to every connected
    /// scheduler again. Returns the number of schedulers it reached.
    pub async fn rebroadcast_partitions_loaded(&self, table: &str) -> usize {
        let cached = self
            .inner
            .latest_partitions_loaded
            .read()
            .await
            .get(table)
            .cloned();
        match cached {
            Some(cached) => {
                self.broadcast_partitions_loaded(
                    table.to_string(),
                    cached.partition_expr_bytes,
                    cached.load_sequence,
                )
                .await
            }
            None => 0,
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
        let cached: Vec<(String, CachedAck)> = {
            let cache = self.inner.latest_partitions_loaded.read().await;
            cache
                .iter()
                .map(|(table, ack)| (table.clone(), ack.clone()))
                .collect()
        };
        if cached.is_empty() {
            return 0;
        }

        let executor_id = self.inner.executor_id.read().await.clone();
        let total = cached.len();
        let mut replayed = 0usize;
        for (table_name, ack) in cached {
            let payload = partitions_loaded_message(
                executor_id.clone(),
                table_name,
                ack.partition_expr_bytes,
                ack.load_sequence,
            );
            // Fail fast on a closed channel or a timed-out send: both mean
            // this connection is gone or wedged, so the remaining sends are
            // guaranteed to fail too — without the break, a large cache would
            // keep the replay task spinning for `tables * SEND_TIMEOUT`. The
            // cache is retained; the next (re)connect replays everything.
            match tokio::time::timeout(SEND_TIMEOUT, tx.send(payload)).await {
                Ok(Ok(())) => replayed += 1,
                Ok(Err(err)) => {
                    tracing::debug!(
                        "PartitionsLoaded replay to {scheduler_address} aborted (channel closed): {err}"
                    );
                    break;
                }
                Err(_) => {
                    tracing::warn!(
                        scheduler = %scheduler_address,
                        "Timed out replaying PartitionsLoaded; aborting replay, will retry on next reconnect"
                    );
                    break;
                }
            }
        }
        if replayed == total {
            tracing::info!(
                scheduler = %scheduler_address,
                count = replayed,
                "Replayed cached PartitionsLoaded acks to connected scheduler"
            );
        } else {
            tracing::warn!(
                scheduler = %scheduler_address,
                replayed,
                total,
                "Replayed only part of the cached PartitionsLoaded acks; will retry on next reconnect"
            );
        }
        replayed
    }

    pub async fn unregister(&self, scheduler_address: &str) {
        self.inner.streams.write().await.remove(scheduler_address);
    }

    /// Sends the executor's Flight endpoint to every connected scheduler and
    /// caches it for schedulers that connect later — see
    /// [`Self::replay_flight_endpoint`]. Returns the number of schedulers it is
    /// sent to.
    ///
    /// The cache is written before the streams are snapshotted, with the same
    /// ordering argument as [`Self::replay_partitions_loaded`]. Each send waits
    /// for room in its stream rather than timing out, in its own task so a
    /// stalled scheduler cannot hold up the others: a scheduler without the
    /// endpoint cannot send scans to this executor. A send fails only when its
    /// stream ends, and the next stream to that scheduler replays the endpoint.
    pub async fn announce_flight_endpoint(&self, executor_url: String) -> usize {
        *self.inner.flight_endpoint.write().await = Some(executor_url.clone());

        let executor_id = self.inner.executor_id.read().await.clone();
        let payload = flight_endpoint_message(executor_id, executor_url);
        let targets: Vec<(String, mpsc::Sender<ExecutorControlMessage>)> = {
            let streams = self.inner.streams.read().await;
            streams
                .iter()
                .map(|(addr, tx)| (addr.clone(), tx.clone()))
                .collect()
        };

        let sent = targets.len();
        for (scheduler_address, tx) in targets {
            let payload = payload.clone();
            tokio::spawn(async move {
                if let Err(err) = tx.send(payload).await {
                    tracing::debug!(
                        "FlightEndpoint send to {scheduler_address} failed (channel closed): {err}"
                    );
                }
            });
        }
        sent
    }

    /// Sends the cached Flight endpoint, if any, to a single scheduler over
    /// `tx`, that scheduler's just-established control stream sender. Returns
    /// whether it was sent. Waits for room in the stream, like
    /// [`Self::announce_flight_endpoint`], and fails only when the stream ends.
    ///
    /// Must only be called once the stream is established, like
    /// [`Self::replay_partitions_loaded`].
    pub async fn replay_flight_endpoint(
        &self,
        scheduler_address: &str,
        tx: &mpsc::Sender<ExecutorControlMessage>,
    ) -> bool {
        let Some(executor_url) = self.inner.flight_endpoint.read().await.clone() else {
            return false;
        };
        let executor_id = self.inner.executor_id.read().await.clone();
        let payload = flight_endpoint_message(executor_id, executor_url);
        if let Err(err) = tx.send(payload).await {
            tracing::debug!(
                "FlightEndpoint replay to {scheduler_address} aborted (channel closed): {err}"
            );
            return false;
        }
        true
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
        load_sequence: u64,
    ) -> usize {
        // Cache before snapshotting streams — see the ordering note on
        // `replay_partitions_loaded` for why this guarantees a concurrently
        // connecting scheduler can't miss the ack. The cache keeps the ack of
        // the newest load, whichever arrives last.
        {
            let mut cache = self.inner.latest_partitions_loaded.write().await;
            if cache
                .get(&table_name)
                .is_none_or(|cached| cached.load_sequence <= load_sequence)
            {
                cache.insert(
                    table_name.clone(),
                    CachedAck {
                        load_sequence,
                        partition_expr_bytes: partition_expr_bytes.clone(),
                    },
                );
            }
        }

        let executor_id = self.inner.executor_id.read().await.clone();
        let payload =
            partitions_loaded_message(executor_id, table_name, partition_expr_bytes, load_sequence);

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
            process_id: String::new(),
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

fn flight_endpoint_message(executor_id: String, executor_url: String) -> ExecutorControlMessage {
    ExecutorControlMessage {
        executor_id,
        message: Some(ExecutorMessage::FlightEndpoint(FlightEndpoint {
            executor_url,
        })),
        process_id: String::new(),
    }
}

fn partitions_loaded_message(
    executor_id: String,
    table_name: String,
    partition_expr_bytes: Vec<Vec<u8>>,
    load_sequence: u64,
) -> ExecutorControlMessage {
    ExecutorControlMessage {
        executor_id,
        message: Some(ExecutorMessage::PartitionsLoaded(PartitionsLoaded {
            table_name,
            partition_expr_bytes: Some(BytesArray {
                items: partition_expr_bytes,
            }),
            load_sequence,
        })),
        process_id: String::new(),
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
            .broadcast_partitions_loaded("spice.public.region".to_string(), vec![vec![1, 2]], 1)
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

    /// A table counts as loading until the newest load started for it ends.
    #[test]
    fn a_table_loads_until_its_newest_load_ends() {
        let broadcaster = ExecutorOutboundBroadcaster::new("exec-1".to_string());
        let first = broadcaster.start_load("t");
        let second = broadcaster.start_load("t");
        assert!(second.load_sequence() > first.load_sequence());
        assert!(broadcaster.is_loading("t"));
        drop(first);
        assert!(broadcaster.is_loading("t"), "the second load is running");
        drop(second);
        assert!(!broadcaster.is_loading("t"));
    }

    /// A load whose task is cancelled before it finishes ends, so later
    /// repairs of the table are not skipped as if it still ran.
    #[tokio::test]
    async fn a_cancelled_load_ends() {
        let broadcaster = ExecutorOutboundBroadcaster::new("exec-1".to_string());
        let load = broadcaster.start_load("t");
        let task = tokio::spawn(async move {
            let _load = load;
            std::future::pending::<()>().await;
        });
        task.abort();
        assert!(task.await.is_err_and(|err| err.is_cancelled()));
        assert!(!broadcaster.is_loading("t"));
    }

    /// The ack of an older load broadcast after a newer one's does not
    /// replace the newer one in the cache.
    #[tokio::test]
    async fn replay_sends_the_newest_load() {
        let broadcaster = ExecutorOutboundBroadcaster::new("exec-1".to_string());
        let older = broadcaster.next_load_sequence();
        let newer = broadcaster.next_load_sequence();
        assert!(newer > older);

        broadcaster
            .broadcast_partitions_loaded("spice.public.orders".to_string(), vec![vec![2]], newer)
            .await;
        broadcaster
            .broadcast_partitions_loaded("spice.public.orders".to_string(), Vec::new(), older)
            .await;

        let (tx, mut rx) = mpsc::channel(8);
        broadcaster
            .replay_partitions_loaded("scheduler-1", &tx)
            .await;
        let loaded = expect_partitions_loaded(rx.recv().await.expect("replayed ack"));
        assert_eq!(loaded.load_sequence, newer);
        assert_eq!(
            loaded
                .partition_expr_bytes
                .expect("partition bytes present")
                .items,
            vec![vec![2]]
        );
    }

    /// Re-broadcasting for the same table must overwrite the cached payload
    /// so a later-connecting scheduler sees only the latest assignment set.
    #[tokio::test]
    async fn replay_sends_latest_payload_per_table() {
        let broadcaster = ExecutorOutboundBroadcaster::new("exec-1".to_string());

        broadcaster
            .broadcast_partitions_loaded("spice.public.orders".to_string(), vec![vec![1]], 1)
            .await;
        broadcaster
            .broadcast_partitions_loaded(
                "spice.public.orders".to_string(),
                vec![vec![2], vec![3]],
                2,
            )
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
        rx1.try_recv()
            .expect_err("nothing should be emitted when the cache is empty");

        let sent = broadcaster
            .broadcast_partitions_loaded("spice.public.lineitem".to_string(), vec![vec![7]], 1)
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
    /// reports zero queued acks — even with multiple cached tables, the
    /// replay aborts on the first failed send. The next reconnect replays.
    #[tokio::test]
    async fn replay_on_closed_channel_queues_nothing() {
        let broadcaster = ExecutorOutboundBroadcaster::new("exec-1".to_string());
        broadcaster
            .broadcast_partitions_loaded("spice.public.region".to_string(), vec![vec![1]], 1)
            .await;
        broadcaster
            .broadcast_partitions_loaded("spice.public.orders".to_string(), vec![vec![2]], 1)
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

    fn expect_flight_endpoint(msg: ExecutorControlMessage) -> FlightEndpoint {
        match msg.message {
            Some(ExecutorMessage::FlightEndpoint(endpoint)) => endpoint,
            other => panic!("expected FlightEndpoint, got {other:?}"),
        }
    }

    /// The endpoint reaches the schedulers connected when it is announced, and
    /// each scheduler that connects later through the replay.
    #[tokio::test]
    async fn flight_endpoint_reaches_connected_and_later_schedulers() {
        let broadcaster = ExecutorOutboundBroadcaster::new("exec-1".to_string());
        let (tx1, mut rx1) = mpsc::channel(8);
        broadcaster
            .register("scheduler-1".to_string(), tx1.clone())
            .await;
        assert!(
            !broadcaster
                .replay_flight_endpoint("scheduler-1", &tx1)
                .await,
            "nothing to replay before the endpoint is announced"
        );

        assert_eq!(
            broadcaster
                .announce_flight_endpoint("https://127.0.0.1:50051".to_string())
                .await,
            1
        );
        let msg = rx1.recv().await.expect("announced endpoint");
        assert_eq!(msg.executor_id, "exec-1");
        assert_eq!(
            expect_flight_endpoint(msg).executor_url,
            "https://127.0.0.1:50051"
        );

        let (tx2, mut rx2) = mpsc::channel(8);
        broadcaster
            .register("scheduler-2".to_string(), tx2.clone())
            .await;
        assert!(
            broadcaster
                .replay_flight_endpoint("scheduler-2", &tx2)
                .await
        );
        assert_eq!(
            expect_flight_endpoint(rx2.recv().await.expect("replayed endpoint")).executor_url,
            "https://127.0.0.1:50051"
        );
    }

    /// A stream that stays full for longer than [`SEND_TIMEOUT`] still receives
    /// the endpoint, from the announcement and from the replay, once the
    /// scheduler drains it.
    #[tokio::test(start_paused = true)]
    async fn flight_endpoint_waits_for_a_full_stream() {
        let broadcaster = ExecutorOutboundBroadcaster::new("exec-1".to_string());
        let filler = || flight_endpoint_message("exec-1".to_string(), "filler".to_string());

        let (tx1, mut rx1) = mpsc::channel(1);
        tx1.send(filler()).await.expect("fill the stream");
        broadcaster
            .register("scheduler-1".to_string(), tx1.clone())
            .await;
        assert_eq!(
            broadcaster
                .announce_flight_endpoint("https://127.0.0.1:50051".to_string())
                .await,
            1
        );
        tokio::time::sleep(SEND_TIMEOUT * 2).await;
        assert_eq!(
            expect_flight_endpoint(rx1.recv().await.expect("filler")).executor_url,
            "filler"
        );
        let endpoint = tokio::time::timeout(SEND_TIMEOUT, rx1.recv())
            .await
            .expect("the endpoint follows the filler")
            .expect("announced endpoint");
        assert_eq!(
            expect_flight_endpoint(endpoint).executor_url,
            "https://127.0.0.1:50051"
        );

        let (tx2, mut rx2) = mpsc::channel(1);
        tx2.send(filler()).await.expect("fill the stream");
        let replay = {
            let broadcaster = broadcaster.clone();
            tokio::spawn(async move {
                broadcaster
                    .replay_flight_endpoint("scheduler-2", &tx2)
                    .await
            })
        };
        tokio::time::sleep(SEND_TIMEOUT * 2).await;
        assert_eq!(
            expect_flight_endpoint(rx2.recv().await.expect("filler")).executor_url,
            "filler"
        );
        let endpoint = tokio::time::timeout(SEND_TIMEOUT, rx2.recv())
            .await
            .expect("the endpoint follows the filler")
            .expect("replayed endpoint");
        assert_eq!(
            expect_flight_endpoint(endpoint).executor_url,
            "https://127.0.0.1:50051"
        );
        assert!(replay.await.expect("replay task"));
    }
}
