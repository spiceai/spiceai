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

//! Hands batches to Drasi without making the producer wait for them.
//!
//! Used by both forwarding paths, for the same reason in two shapes: awaiting a
//! Drasi round-trip inline couples the producer's throughput to Drasi's
//! availability. On the CDC path that means replication acknowledgement is gated
//! on a reaction engine; on the runtime-table path it means the OpenTelemetry
//! export loop parks behind a retry budget.
//!
//! A batch the sink will not accept is written to a durable
//! [dead-letter store](crate::drasi::dead_letter) and retried until it lands.
//! Because both Drasi wire formats treat an insert or update as a full-state
//! replace, redelivery must not be overtaken by newer changes for the same row —
//! so once anything is pending, every subsequent batch is appended behind it and
//! normal delivery resumes only once the store drains.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::RecordBatch;
use runtime_drasi::{DrasiChangeRows, DrasiSink};
use runtime_metrics::drasi::{
    DrasiForwardMetrics, OUTCOME_DEAD_LETTERED, OUTCOME_DELIVERED, OUTCOME_DISCARDED,
    OUTCOME_RETRIED,
};
use tokio::sync::mpsc;

use crate::drasi::dead_letter::{DeadLetterStore, RETRY_INTERVAL};

/// Batches a component may hold in memory before overflow spills to the
/// durable store.
///
/// Bounded on purpose: the alternative is holding batches in memory for as long
/// as Drasi is unreachable. Overflow is retained on disk rather than dropped —
/// see [`DeliveryQueue::enqueue`].
pub(crate) const DEFAULT_QUEUE_DEPTH: usize = 64;

/// One batch awaiting delivery.
///
/// Owns its operation codes and key column names because it crosses a channel —
/// the change batch they were read from is released as soon as it is queued.
pub(crate) struct QueuedBatch {
    pub op_codes: Vec<String>,
    pub primary_key_columns: Vec<Vec<String>>,
    pub data: RecordBatch,
    pub source_commit_ts_ms: Option<i64>,
    /// When this batch was handed to the queue, milliseconds since the Unix
    /// epoch. Delivery latency uses this when the batch has no source commit
    /// time. Persisted with the dead-letter batch so a restart does not
    /// restamp it.
    pub queued_at_ms: i64,
}

impl QueuedBatch {
    /// Builds a batch whose rows all share one operation and one key.
    pub(crate) fn uniform(
        op_code: &str,
        key: &[String],
        data: RecordBatch,
        source_commit_ts_ms: Option<i64>,
    ) -> Self {
        let rows = data.num_rows();
        Self {
            op_codes: vec![op_code.to_string(); rows],
            primary_key_columns: vec![key.to_vec(); rows],
            data,
            source_commit_ts_ms,
            queued_at_ms: now_ms(),
        }
    }
}

/// Milliseconds since the Unix epoch.
pub(crate) fn now_ms() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis());
    i64::try_from(millis).unwrap_or(i64::MAX)
}

/// Milliseconds from the change to `acked_at_ms`.
///
/// The change's time is its source commit time when the batch has one, and the
/// time it was queued otherwise. Recorded only when Drasi acknowledges the
/// batch.
#[must_use]
pub(crate) fn delivery_latency_ms(
    source_commit_ts_ms: Option<i64>,
    queued_at_ms: i64,
    acked_at_ms: i64,
) -> f64 {
    let start = source_commit_ts_ms.unwrap_or(queued_at_ms);
    // `saturating_sub` only stops at `i64::MIN`. A commit time ahead of the
    // acknowledgement is clock skew, and a negative latency is not a duration.
    let delta = acked_at_ms.saturating_sub(start).max(0);
    #[expect(clippy::cast_precision_loss)]
    {
        delta as f64
    }
}

/// A bounded queue drained by one task, so delivery order per component is
/// preserved and the producer never waits.
#[derive(Debug)]
pub(crate) struct DeliveryQueue {
    jobs: mpsc::Sender<QueuedBatch>,
    dead_lettered: Arc<AtomicU64>,
    component: String,
    transport: &'static str,
    /// Also held here, not just by the tasks: overflow is retained by the
    /// producer, which is the only place that still has the batch.
    store: Option<Arc<DeadLetterStore>>,
    metrics: DrasiForwardMetrics,
    /// In-memory batches still waiting, including the one a delivery attempt
    /// is holding. The gauge reads this atomic at scrape time.
    depth: Arc<AtomicU64>,
}

impl DeliveryQueue {
    /// Starts a delivery task backed by `store`, which retains and retries what
    /// Drasi will not accept.
    ///
    /// Must be called from within a Tokio runtime.
    pub(crate) fn spawn(
        sink: Arc<DrasiSink>,
        component: String,
        depth: usize,
        store: Option<Arc<DeadLetterStore>>,
    ) -> Self {
        Self::spawn_measuring(
            sink,
            component,
            depth,
            store,
            RETRY_INTERVAL,
            DrasiForwardMetrics::global(),
        )
    }

    /// [`Self::spawn`] with an explicit retry interval and meter.
    ///
    /// Production passes [`RETRY_INTERVAL`] and [`DrasiForwardMetrics::global`].
    /// Tests pass a short interval and a meter they can collect, and drive the
    /// same delivery and retry tasks.
    pub(crate) fn spawn_measuring(
        sink: Arc<DrasiSink>,
        component: String,
        depth: usize,
        store: Option<Arc<DeadLetterStore>>,
        retry_every: std::time::Duration,
        metrics: DrasiForwardMetrics,
    ) -> Self {
        let (jobs, receiver) = mpsc::channel(depth);
        let dead_lettered = Arc::new(AtomicU64::new(0));
        let transport = sink.transport_name();
        let queue_depth = metrics.queue_depth_slot(&component);
        // A component with no store still publishes the dead-letter series at
        // zero. A store publishes its own occupancy, including batches
        // recovered at open, so resetting it here would hide them.
        if store.is_none() {
            metrics
                .dead_letter_slot(&component)
                .store(0, Ordering::Relaxed);
        }

        tokio::spawn(deliver(
            receiver,
            Arc::clone(&sink),
            component.clone(),
            transport,
            Arc::clone(&dead_lettered),
            store.clone(),
            metrics.clone(),
            Arc::clone(&queue_depth),
        ));

        if let Some(store) = store.clone() {
            tokio::spawn(retry_pending(
                sink,
                component.clone(),
                transport,
                store,
                retry_every,
                metrics.clone(),
            ));
        }

        Self {
            jobs,
            dead_lettered,
            component,
            transport,
            store,
            metrics,
            depth: queue_depth,
        }
    }

    /// Queues `batch`, falling back to the durable store when the queue is full.
    ///
    /// Never blocks on Drasi. It *does* wait on one local file write when the
    /// queue is full, which is the point: under `queued` delivery the
    /// replication position is released before delivery, so a batch dropped
    /// here is unrecoverable. Bounded backpressure onto a durable store is the
    /// correct trade against silent loss.
    ///
    /// The write is awaited rather than spawned so a later batch cannot reach
    /// the sink ahead of it — redelivery is a full-state replace, so the
    /// stop-the-line ordering has to hold from the moment of overflow.
    pub(crate) async fn enqueue(&self, batch: QueuedBatch) {
        let overflow = match self.jobs.try_send(batch) {
            Ok(()) => {
                add_depth(&self.depth);
                return;
            }
            Err(
                mpsc::error::TrySendError::Full(batch) | mpsc::error::TrySendError::Closed(batch),
            ) => batch,
        };

        let Some(store) = &self.store else {
            self.dead_letter(
                "the delivery queue is full and no dead-letter store is available, so Drasi is not keeping up or is unreachable",
            );
            return;
        };

        if let Err(e) = store.append(&overflow).await {
            self.dead_letter(&format!(
                "the delivery queue is full and the batch could not be retained: {e}"
            ));
        } else {
            // Retained without a delivery attempt. A later attempt records its
            // own outcome.
            self.metrics
                .record_batch(&self.component, self.transport, OUTCOME_DEAD_LETTERED);
        }
    }

    /// Records a batch that will not be delivered and could not be retained.
    pub(crate) fn dead_letter(&self, reason: &str) {
        record_dead_letter(&self.dead_lettered, &self.component, reason);
        self.metrics
            .record_batch(&self.component, self.transport, OUTCOME_DEAD_LETTERED);
    }

    /// How many batches were dropped without reaching the durable store — a
    /// full in-memory queue, or a store that could not be written.
    ///
    /// Batches the store accepted are retried and are *not* counted here; see
    /// [`DeadLetterStore::discarded`](crate::drasi::dead_letter::DeadLetterStore::discarded)
    /// for the ones it gave up on.
    #[cfg(test)]
    pub(crate) fn dead_lettered(&self) -> u64 {
        self.dead_lettered.load(Ordering::Relaxed)
    }
}

/// Counts a dead-lettered batch, logging sparsely.
///
/// These conditions persist across batches — an unreachable Drasi stays
/// unreachable — so logging each occurrence would spam the log at write rate.
fn record_dead_letter(count: &AtomicU64, component: &str, reason: &str) {
    let total = count.fetch_add(1, Ordering::Relaxed) + 1;
    if total == 1 || total.is_multiple_of(1000) {
        tracing::warn!(
            "Dropped a Drasi change batch for {component} ({total} dropped so far): {reason}"
        );
    }
}

/// The three things that can happen to a batch handed to Drasi.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// Drasi accepted it.
    Delivered,
    /// It failed in a way that could clear — retain and retry.
    Retain,
    /// It failed in a way that never clears. Retaining it would block every
    /// later batch behind something that can never succeed, so it is counted
    /// and discarded instead.
    Discard,
}

/// Delivers one batch, classifying any failure.
async fn forward(sink: &DrasiSink, job: &QueuedBatch) -> Result<(), runtime_drasi::Error> {
    let op_codes: Vec<&str> = job.op_codes.iter().map(String::as_str).collect();
    let primary_key_columns: Vec<Vec<&str>> = job
        .primary_key_columns
        .iter()
        .map(|key| key.iter().map(String::as_str).collect())
        .collect();

    let rows = DrasiChangeRows {
        op_codes,
        primary_key_columns,
        data: &job.data,
        source_commit_ts_ms: job.source_commit_ts_ms,
    };

    sink.forward(&rows).await
}

/// Classifies a delivery result.
fn classify(result: Result<(), runtime_drasi::Error>) -> (Outcome, Option<String>) {
    match result {
        Ok(()) => (Outcome::Delivered, None),
        Err(e) => {
            let outcome = match e.retryable() {
                runtime_drasi::Retryable::Transient => Outcome::Retain,
                runtime_drasi::Retryable::Permanent => Outcome::Discard,
            };
            (outcome, Some(e.to_string()))
        }
    }
}

fn add_depth(depth: &AtomicU64) {
    depth.fetch_add(1, Ordering::Relaxed);
}

fn sub_depth(depth: &AtomicU64) {
    let mut current = depth.load(Ordering::Relaxed);
    loop {
        let next = current.saturating_sub(1);
        match depth.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

fn record_ack(metrics: &DrasiForwardMetrics, component: &str, transport: &str, job: &QueuedBatch) {
    metrics.record_batch(component, transport, OUTCOME_DELIVERED);
    metrics.record_latency_ms(
        component,
        transport,
        delivery_latency_ms(job.source_commit_ts_ms, job.queued_at_ms, now_ms()),
    );
}

/// Drains one component's queue, delivering each batch in order.
#[expect(clippy::too_many_arguments)]
async fn deliver(
    mut jobs: mpsc::Receiver<QueuedBatch>,
    sink: Arc<DrasiSink>,
    component: String,
    transport: &'static str,
    dead_lettered: Arc<AtomicU64>,
    store: Option<Arc<DeadLetterStore>>,
    metrics: DrasiForwardMetrics,
    depth: Arc<AtomicU64>,
) {
    while let Some(job) = jobs.recv().await {
        // Anything already pending is older than this batch. Delivering now
        // would apply a full-state replace out of order, so queue behind it.
        if let Some(store) = &store
            && !store.is_empty().await
        {
            if let Err(e) = store.append(&job).await {
                record_dead_letter(&dead_lettered, &component, &e.to_string());
            }
            // Appended, or dropped because it could not be retained. Either way
            // this was not a delivery attempt.
            metrics.record_batch(&component, transport, OUTCOME_DEAD_LETTERED);
            sub_depth(&depth);
            continue;
        }

        let (outcome, message) = classify(forward(&sink, &job).await);
        let message = message.unwrap_or_default();

        match (outcome, &store) {
            (Outcome::Delivered, _) => {
                record_ack(&metrics, &component, transport, &job);
            }
            (Outcome::Retain, Some(store)) => {
                if let Err(e) = store.append(&job).await {
                    record_dead_letter(&dead_lettered, &component, &e.to_string());
                    metrics.record_batch(&component, transport, OUTCOME_DEAD_LETTERED);
                } else {
                    tracing::warn!(
                        "Retaining a Drasi change batch for {component} for redelivery: {message}"
                    );
                    metrics.record_batch(&component, transport, OUTCOME_RETRIED);
                }
            }
            // `Discard` can never be delivered, and retaining it would park it
            // at the head of a stop-the-line queue forever. `Retain` with no
            // store has nowhere to be kept, so it is counted as dead-lettered.
            (Outcome::Discard, _) => {
                record_dead_letter(&dead_lettered, &component, &message);
                metrics.record_batch(&component, transport, OUTCOME_DISCARDED);
            }
            (Outcome::Retain, None) => {
                record_dead_letter(&dead_lettered, &component, &message);
                metrics.record_batch(&component, transport, OUTCOME_DEAD_LETTERED);
            }
        }
        sub_depth(&depth);
    }
}

/// Retries whatever the store is holding, until it drains.
async fn retry_pending(
    sink: Arc<DrasiSink>,
    component: String,
    transport: &'static str,
    store: Arc<DeadLetterStore>,
    retry_every: std::time::Duration,
    metrics: DrasiForwardMetrics,
) {
    loop {
        tokio::time::sleep(retry_every).await;

        if store.is_empty().await {
            continue;
        }

        tracing::debug!(
            "Retrying undelivered Drasi change batches for {component} ({} discarded so far)",
            store.discarded()
        );

        let sink = Arc::clone(&sink);
        let metrics = metrics.clone();
        store
            .drain(|job| {
                let sink = Arc::clone(&sink);
                let component = component.clone();
                let metrics = metrics.clone();
                async move {
                    let (outcome, message) = classify(forward(&sink, &job).await);
                    match outcome {
                        Outcome::Delivered => {
                            record_ack(&metrics, &component, transport, &job);
                        }
                        Outcome::Retain => {
                            tracing::debug!(
                                "Drasi redelivery for {component} still failing: {}",
                                message.unwrap_or_default()
                            );
                            metrics.record_batch(&component, transport, OUTCOME_RETRIED);
                        }
                        Outcome::Discard => {
                            tracing::warn!(
                                "Discarding a Drasi change batch for {component} that can never be delivered: {}",
                                message.unwrap_or_default()
                            );
                            metrics.record_batch(&component, transport, OUTCOME_DISCARDED);
                        }
                    }
                    outcome
                }
            })
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use runtime_drasi::{DrasiSinkConfig, ElementMapping, OnDeliveryError, TransportConfig};
    use std::time::Duration;

    fn batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["1", "2"]))],
        )
        .expect("valid batch")
    }

    /// Points at a port nothing listens on, so delivery never drains the queue.
    fn unreachable_sink() -> Arc<DrasiSink> {
        Arc::new(
            DrasiSink::try_new(DrasiSinkConfig {
                dataset: "orders".to_string(),
                source_id: "spice-cdc".to_string(),
                mapping: ElementMapping::new("orders", vec!["orders".to_string()]),
                transport: TransportConfig::Http {
                    endpoint: url::Url::parse("http://127.0.0.1:1").expect("valid url"),
                    request_timeout: Duration::from_millis(50),
                },
                on_delivery_error: OnDeliveryError::Skip,
            })
            .expect("builds"),
        )
    }

    #[test]
    fn latency_uses_the_source_commit_time_when_the_batch_has_one() {
        assert_eq!(delivery_latency_ms(Some(1_000), 5_000, 1_500), 500.0);
        assert_eq!(
            delivery_latency_ms(None, 5_000, 5_040),
            40.0,
            "without a source commit time, latency starts when the batch was queued"
        );
        assert_eq!(
            delivery_latency_ms(Some(9_000), 1_000, 8_000),
            0.0,
            "a commit time ahead of the acknowledgement does not record a negative latency"
        );
    }

    #[test]
    fn uniform_repeats_the_operation_and_key_per_row() {
        let queued = QueuedBatch::uniform("c", &["id".to_string()], batch(), Some(7));

        assert_eq!(queued.op_codes, vec!["c".to_string(), "c".to_string()]);
        assert_eq!(queued.primary_key_columns.len(), 2);
        assert_eq!(queued.primary_key_columns[0], vec!["id".to_string()]);
        assert_eq!(queued.source_commit_ts_ms, Some(7));
    }

    /// Without a store there is nowhere to put overflow, so it is counted.
    ///
    /// On the current-thread runtime the store-less `enqueue` never yields, so
    /// the delivery task cannot run during the loop: the first 4 batches fill
    /// the queue and every one after them is overflow. That makes the count
    /// exact, and pins that overflow (not a delivery failure, which the same
    /// counter also records) is what was counted.
    #[tokio::test(flavor = "current_thread")]
    async fn a_full_queue_without_a_store_counts_the_drop() {
        let queue = DeliveryQueue::spawn(unreachable_sink(), "orders".to_string(), 4, None);

        for _ in 0..64 {
            queue
                .enqueue(QueuedBatch::uniform(
                    "c",
                    &["id".to_string()],
                    batch(),
                    None,
                ))
                .await;
        }

        assert_eq!(
            queue.dead_lettered(),
            60,
            "a full queue must dead-letter each overflowing batch rather than block the producer"
        );
    }

    /// Under `queued` delivery the replication position is released before
    /// delivery, so overflow that never reaches the disk is unrecoverable: a
    /// configured store must take it rather than the drop count.
    #[tokio::test]
    async fn a_full_queue_retains_overflow_in_the_store() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(
            crate::drasi::dead_letter::DeadLetterStore::open(
                dir.path().to_path_buf(),
                "orders".to_string(),
                crate::drasi::dead_letter::DEFAULT_MAX_BATCHES,
            )
            .await
            .expect("opens"),
        );

        let queue = DeliveryQueue::spawn(
            unreachable_sink(),
            "orders".to_string(),
            2,
            Some(Arc::clone(&store)),
        );

        for _ in 0..32 {
            queue
                .enqueue(QueuedBatch::uniform(
                    "c",
                    &["id".to_string()],
                    batch(),
                    None,
                ))
                .await;
        }

        assert!(
            !store.is_empty().await,
            "overflow must be retained on disk, not counted and dropped"
        );
    }

    /// A permanent failure can never clear, so retaining it would put it at the
    /// head of a stop-the-line queue forever and block every later change.
    #[test]
    fn a_permanent_failure_is_discarded_not_retained() {
        let permanent = runtime_drasi::Error::Delivery {
            dataset: "orders".to_string(),
            source_id: "spice-cdc".to_string(),
            endpoint: "http://drasi:9000".to_string(),
            message: "rejected".to_string(),
            retryable: runtime_drasi::Retryable::Permanent,
        };
        assert_eq!(classify(Err(permanent)).0, Outcome::Discard);
    }

    #[test]
    fn a_transient_failure_is_retained_for_retry() {
        let transient = runtime_drasi::Error::Delivery {
            dataset: "orders".to_string(),
            source_id: "spice-cdc".to_string(),
            endpoint: "http://drasi:9000".to_string(),
            message: "connection refused".to_string(),
            retryable: runtime_drasi::Retryable::Transient,
        };
        assert_eq!(classify(Err(transient)).0, Outcome::Retain);
        assert_eq!(classify(Ok(())).0, Outcome::Delivered);
    }

    #[tokio::test]
    async fn dead_letter_is_counted() {
        let queue = DeliveryQueue::spawn(unreachable_sink(), "orders".to_string(), 4, None);
        assert_eq!(queue.dead_lettered(), 0);

        queue.dead_letter("test");
        queue.dead_letter("test");

        assert_eq!(queue.dead_lettered(), 2);
    }
}
