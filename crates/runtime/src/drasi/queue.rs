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

use arrow::array::RecordBatch;
use runtime_drasi::{DrasiChangeRows, DrasiSink};
use tokio::sync::mpsc;

use crate::drasi::dead_letter::{DeadLetterStore, RETRY_INTERVAL};

/// Batches a component may have awaiting delivery before new ones are dropped.
///
/// Bounded on purpose: the alternative to dropping is holding batches in memory
/// for as long as Drasi is unreachable.
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
        }
    }
}

/// A bounded queue drained by one task, so delivery order per component is
/// preserved and the producer never waits.
#[derive(Debug)]
pub(crate) struct DeliveryQueue {
    jobs: mpsc::Sender<QueuedBatch>,
    dead_lettered: Arc<AtomicU64>,
    component: String,
    /// Also held here, not just by the tasks: overflow is retained by the
    /// producer, which is the only place that still has the batch.
    store: Option<Arc<DeadLetterStore>>,
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
        let (jobs, receiver) = mpsc::channel(depth);
        let dead_lettered = Arc::new(AtomicU64::new(0));

        tokio::spawn(deliver(
            receiver,
            Arc::clone(&sink),
            component.clone(),
            Arc::clone(&dead_lettered),
            store.clone(),
        ));

        if let Some(store) = store.clone() {
            tokio::spawn(retry_pending(sink, component.clone(), store));
        }

        Self {
            jobs,
            dead_lettered,
            component,
            store,
        }
    }

    /// Queues `batch`, falling back to the durable store when the queue is full.
    ///
    /// Never blocks on Drasi. It *does* wait on one local file write when the
    /// queue is full, which is the point: overflow used to be counted and
    /// dropped, and under `queued` delivery the replication position has
    /// already been released, so a dropped batch was unrecoverable. Bounded
    /// backpressure onto a durable store is the correct trade against silent
    /// loss.
    pub(crate) async fn enqueue(&self, batch: QueuedBatch) {
        let overflow = match self.jobs.try_send(batch) {
            Ok(()) => return,
            Err(mpsc::error::TrySendError::Full(batch)
            | mpsc::error::TrySendError::Closed(batch)) => batch,
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
        }
    }

    /// Records a batch that will not be delivered.
    pub(crate) fn dead_letter(&self, reason: &str) {
        record_dead_letter(&self.dead_lettered, &self.component, reason);
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

/// Drains one component's queue, delivering each batch in order.
async fn deliver(
    mut jobs: mpsc::Receiver<QueuedBatch>,
    sink: Arc<DrasiSink>,
    component: String,
    dead_lettered: Arc<AtomicU64>,
    store: Option<Arc<DeadLetterStore>>,
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
            continue;
        }

        let (outcome, message) = classify(forward(&sink, &job).await);
        let message = message.unwrap_or_default();

        match (outcome, &store) {
            (Outcome::Delivered, _) => {}
            (Outcome::Retain, Some(store)) => {
                if let Err(e) = store.append(&job).await {
                    record_dead_letter(&dead_lettered, &component, &e.to_string());
                } else {
                    tracing::warn!(
                        "Retaining a Drasi change batch for {component} for redelivery: {message}"
                    );
                }
            }
            // Two different reasons, one outcome — the batch is gone and the
            // count is what records the gap. `Discard` can never be delivered,
            // and retaining it would park it at the head of a stop-the-line
            // queue forever, blocking every later change behind something that
            // cannot succeed. `Retain` with no store has nowhere to be kept.
            (Outcome::Discard, _) | (Outcome::Retain, None) => {
                record_dead_letter(&dead_lettered, &component, &message);
            }
        }
    }
}

/// Retries whatever the store is holding, until it drains.
async fn retry_pending(sink: Arc<DrasiSink>, component: String, store: Arc<DeadLetterStore>) {
    loop {
        tokio::time::sleep(RETRY_INTERVAL).await;

        if store.is_empty().await {
            continue;
        }

        tracing::debug!(
            "Retrying undelivered Drasi change batches for {component} ({} discarded so far)",
            store.discarded()
        );

        let sink = Arc::clone(&sink);
        store
            .drain(|job| {
                let sink = Arc::clone(&sink);
                let component = component.clone();
                async move {
                    let (outcome, message) = classify(forward(&sink, &job).await);
                    if outcome == Outcome::Retain {
                        tracing::debug!(
                            "Drasi redelivery for {component} still failing: {}",
                            message.unwrap_or_default()
                        );
                    } else if outcome == Outcome::Discard {
                        tracing::warn!(
                            "Discarding a Drasi change batch for {component} that can never be delivered: {}",
                            message.unwrap_or_default()
                        );
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
    use runtime_drasi::{
        DrasiSinkConfig, ElementMapping, OnDeliveryError, TransportConfig,
    };
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
    fn uniform_repeats_the_operation_and_key_per_row() {
        let queued = QueuedBatch::uniform("c", &["id".to_string()], batch(), Some(7));

        assert_eq!(queued.op_codes, vec!["c".to_string(), "c".to_string()]);
        assert_eq!(queued.primary_key_columns.len(), 2);
        assert_eq!(queued.primary_key_columns[0], vec!["id".to_string()]);
        assert_eq!(queued.source_commit_ts_ms, Some(7));
    }

    /// Without a store there is nowhere to put overflow, so it is counted.
    #[tokio::test]
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

        assert!(
            queue.dead_lettered() > 0,
            "a full queue must dead-letter rather than block the producer"
        );
    }

    /// The regression this exists for: overflow used to be counted and dropped
    /// even when a store was configured. Under `queued` delivery the
    /// replication position is already released, so a dropped batch is
    /// unrecoverable — it has to reach the disk.
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
