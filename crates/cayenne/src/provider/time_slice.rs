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

//! A scan stream that gives its query runtime worker back to other queries on a
//! time slice.
//!
//! Every query's partitions run as tasks on one Tokio runtime, and a task runs
//! until its stream returns `Pending`. A stream returns `Pending` on its own only
//! when it waits for I/O or exhausts Tokio's cooperative budget, which `DataFusion`
//! charges one unit per batch and which allows 128 units per poll. A partition of
//! a large scan is often fewer batches than that, so a query whose batches are
//! expensive (a JSON projection over every row, say) keeps a worker for its whole
//! partition, and with one task per core it keeps every worker: a query that
//! needs a few milliseconds waits for it to finish.
//!
//! [`TimeSliced`] yields once its task has run for [`SLICE`] since it last
//! yielded, before producing the next batch. The yield goes through
//! [`tokio::task::yield_now`], which reschedules the task behind the work already
//! queued on its worker rather than in the slot that runs next, so the queries
//! sharing the runtime take turns batch by batch. It changes when batches are
//! produced, never which batches or in what order.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use arrow_schema::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::physical_plan::RecordBatchStream;
use datafusion_execution::SendableRecordBatchStream;
use futures::Stream;

/// How long a task runs a scan before it gives its worker to other tasks. Short
/// next to the few milliseconds a small query needs, long next to the
/// microseconds a reschedule costs.
pub(crate) const SLICE: Duration = Duration::from_millis(2);

/// A [`SendableRecordBatchStream`] that yields its worker every [`SLICE`].
pub(crate) struct TimeSliced {
    inner: SendableRecordBatchStream,
    /// When the task last resumed after this stream yielded or waited; `None`
    /// until the next poll starts a slice.
    slice_start: Option<Instant>,
    /// The yield in progress, polled to completion before the next batch.
    yielding: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl TimeSliced {
    pub(crate) fn new(inner: SendableRecordBatchStream) -> Self {
        Self {
            inner,
            slice_start: None,
            yielding: None,
        }
    }
}

impl Stream for TimeSliced {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        if let Some(yielding) = this.yielding.as_mut() {
            if yielding.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
            this.yielding = None;
        }
        let now = Instant::now();
        match this.slice_start {
            Some(start) if now.duration_since(start) >= SLICE => {
                this.slice_start = None;
                let mut yielding: Pin<Box<dyn Future<Output = ()> + Send>> =
                    Box::pin(tokio::task::yield_now());
                if yielding.as_mut().poll(cx).is_pending() {
                    this.yielding = Some(yielding);
                    return Poll::Pending;
                }
            }
            Some(_) => {}
            None => this.slice_start = Some(now),
        }
        let polled = this.inner.as_mut().poll_next(cx);
        if polled.is_pending() {
            // The task gives up its worker while the scan waits, so the next
            // poll starts a new slice.
            this.slice_start = None;
        }
        polled
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl RecordBatchStream for TimeSliced {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::StreamExt;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    fn batch(v: i64) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vec![v, v + 1]))])
            .expect("batch")
    }

    /// A stream of `n` batches, each taking `work` of busy CPU to produce.
    fn busy_stream(n: i64, work: Duration) -> SendableRecordBatchStream {
        let batches = futures::stream::iter(0..n).map(move |i| {
            let until = Instant::now() + work;
            while Instant::now() < until {
                std::hint::spin_loop();
            }
            Ok(batch(i * 10))
        });
        Box::pin(RecordBatchStreamAdapter::new(schema(), batches))
    }

    #[tokio::test]
    async fn every_batch_arrives_unchanged_and_in_order() {
        let expected: Vec<RecordBatch> = busy_stream(40, Duration::from_micros(300))
            .map(|b| b.expect("batch"))
            .collect()
            .await;
        let got: Vec<RecordBatch> = TimeSliced::new(busy_stream(40, Duration::from_micros(300)))
            .map(|b| b.expect("batch"))
            .collect()
            .await;
        assert_eq!(got, expected);
        assert_eq!(got.len(), 40);
    }

    /// On one worker, a short task spawned while a long scan runs finishes
    /// within a slice or two of the scan's batches, not after the whole scan.
    #[tokio::test(flavor = "current_thread")]
    async fn a_short_task_runs_while_a_long_scan_is_in_progress() {
        let produced = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&produced);
        let scan = tokio::spawn(async move {
            let mut stream = TimeSliced::new(busy_stream(200, Duration::from_millis(1)));
            while let Some(b) = stream.next().await {
                b.expect("batch");
                counter.fetch_add(1, Ordering::Relaxed);
            }
        });
        // Let the scan start before the short task is queued behind it.
        tokio::task::yield_now().await;
        let short = tokio::spawn({
            let produced = Arc::clone(&produced);
            async move { produced.load(Ordering::Relaxed) }
        });
        let seen_by_short = short.await.expect("short task");
        scan.await.expect("scan");
        assert_eq!(produced.load(Ordering::Relaxed), 200);
        assert!(
            seen_by_short < 20,
            "the short task ran only after {seen_by_short} of 200 batches"
        );
    }

    /// The same scan without the wrapper keeps the worker until it is done, which
    /// is what the wrapper is for: the short task sees every batch produced.
    #[tokio::test(flavor = "current_thread")]
    async fn without_the_wrapper_the_short_task_waits_for_the_whole_scan() {
        let produced = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&produced);
        let scan = tokio::spawn(async move {
            let mut stream = busy_stream(100, Duration::from_micros(500));
            while let Some(b) = stream.next().await {
                b.expect("batch");
                counter.fetch_add(1, Ordering::Relaxed);
            }
        });
        tokio::task::yield_now().await;
        let short = tokio::spawn({
            let produced = Arc::clone(&produced);
            async move { produced.load(Ordering::Relaxed) }
        });
        let seen_by_short = short.await.expect("short task");
        scan.await.expect("scan");
        assert_eq!(seen_by_short, 100);
    }
}
