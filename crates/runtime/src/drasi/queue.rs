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
//! A batch that still cannot be delivered after the sink exhausts its retries is
//! **dead-lettered**: counted, logged sparsely, and dropped. Retention and
//! replay of dead-lettered batches are not implemented — see
//! [`DeliveryQueue::dead_lettered`].

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::RecordBatch;
use runtime_drasi::{DrasiChangeRows, DrasiSink};
use tokio::sync::mpsc;

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
    /// Starts a delivery task for `sink`.
    ///
    /// Must be called from within a Tokio runtime.
    pub(crate) fn spawn(sink: Arc<DrasiSink>, component: String, depth: usize) -> Self {
        let (jobs, receiver) = mpsc::channel(depth);
        let dead_lettered = Arc::new(AtomicU64::new(0));

        tokio::spawn(deliver(
            receiver,
            sink,
            component.clone(),
            Arc::clone(&dead_lettered),
        ));

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

    /// How many batches have been dropped undelivered.
    ///
    /// Counted rather than retained: holding undelivered batches would grow
    /// without bound during an outage, and replaying them needs a durable store
    /// this does not have. A non-zero count means Drasi's view of this component
    /// has gaps.
    #[cfg_attr(not(test), expect(dead_code, reason = "read by tests and future metrics"))]
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

/// Drains one component's queue, delivering each batch in order.
async fn deliver(
    mut jobs: mpsc::Receiver<QueuedBatch>,
    sink: Arc<DrasiSink>,
    component: String,
    dead_lettered: Arc<AtomicU64>,
) {
    while let Some(job) = jobs.recv().await {
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

        if let Err(e) = sink.forward(&rows).await {
            record_dead_letter(&dead_lettered, &component, &e.to_string());
        }
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
        let queue = DeliveryQueue::spawn(unreachable_sink(), "orders".to_string(), 4);

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
        let queue = DeliveryQueue::spawn(unreachable_sink(), "orders".to_string(), 4);
        assert_eq!(queue.dead_lettered(), 0);

        queue.dead_letter("test");
        queue.dead_letter("test");

        assert_eq!(queue.dead_lettered(), 2);
    }
}
