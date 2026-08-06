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

        if let Some(store) = store {
            tokio::spawn(retry_pending(sink, component.clone(), store));
        }

        Self {
            jobs,
            dead_lettered,
            component,
        }
    }

    /// Queues `batch`, or dead-letters it when the queue is full.
    ///
    /// Never blocks and never fails: a full queue means Drasi is not keeping up,
    /// and the whole point of queueing is that the producer does not wait for
    /// it.
    pub(crate) fn enqueue(&self, batch: QueuedBatch) {
        if self.jobs.try_send(batch).is_err() {
            self.dead_letter("the delivery queue is full, so Drasi is not keeping up or is unreachable");
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

/// Delivers one batch, reporting whether it landed.
async fn forward(sink: &DrasiSink, job: &QueuedBatch) -> Result<(), String> {
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

    sink.forward(&rows).await.map_err(|e| e.to_string())
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

        if let Err(message) = forward(&sink, &job).await {
            match &store {
                Some(store) => {
                    if let Err(e) = store.append(&job).await {
                        record_dead_letter(&dead_lettered, &component, &e.to_string());
                    } else {
                        tracing::warn!(
                            "Retaining a Drasi change batch for {component} for redelivery: {message}"
                        );
                    }
                }
                None => record_dead_letter(&dead_lettered, &component, &message),
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
                    match forward(&sink, &job).await {
                        Ok(()) => true,
                        Err(message) => {
                            tracing::debug!(
                                "Drasi redelivery for {component} still failing: {message}"
                            );
                            false
                        }
                    }
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

    /// The contract the CDC path depends on: enqueueing returns immediately even
    /// when Drasi is unreachable, so replication is never gated on it.
    #[tokio::test]
    async fn a_full_queue_dead_letters_rather_than_blocking() {
        let queue = DeliveryQueue::spawn(unreachable_sink(), "orders".to_string(), 4, None);

        for _ in 0..64 {
            queue.enqueue(QueuedBatch::uniform(
                "c",
                &["id".to_string()],
                batch(),
                None,
            ));
        }

        assert!(
            queue.dead_lettered() > 0,
            "a full queue must dead-letter rather than block the producer"
        );
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
