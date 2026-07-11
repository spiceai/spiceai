/*
Copyright 2025 The Spice.ai OSS Authors

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

//! Streaming execution plan for Cayenne write operations.
//!
//! This module provides `StreamingExec`, an execution plan that forwards
//! record batches from a stream without buffering.

use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::execution::SendableRecordBatchStream as DFStream;
use datafusion_physical_plan::DisplayAs;
use datafusion_physical_plan::DisplayFormatType;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::PlanProperties;
use datafusion_physical_plan::RecordBatchStream;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType, Partitioning};
use futures::Stream;
use futures::StreamExt;
use futures::stream::unfold;
use parking_lot::Mutex;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// A streaming execution plan that forwards batches without buffering.
///
/// This is used during chunk writes to efficiently stream data to the Vortex writer
/// without unnecessary buffering or copies.
pub struct StreamingExec {
    /// Arrow schema for the data
    pub schema: SchemaRef,
    /// The input stream wrapped in a (sync) mutex solely for one-time ownership
    /// transfer in `execute`. The mutex is *never* held across an `.await` point.
    /// We use `parking_lot::Mutex` (fast, no poisoning) because the take is a
    /// short synchronous operation at the start of plan execution.
    pub stream: Mutex<Option<DFStream>>,
    /// Plan properties
    pub properties: Arc<PlanProperties>,
}

impl StreamingExec {
    /// Create a new `StreamingExec` from a record batch stream.
    pub fn new(schema: SchemaRef, stream: DFStream) -> Self {
        use datafusion_physical_expr::EquivalenceProperties;

        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        );

        Self {
            schema,
            stream: Mutex::new(Some(stream)),
            properties: Arc::new(properties),
        }
    }
}

impl std::fmt::Debug for StreamingExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamingExec").finish()
    }
}

impl DisplayAs for StreamingExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "StreamingExec")
    }
}

impl ExecutionPlan for StreamingExec {
    fn name(&self) -> &'static str {
        "StreamingExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion_execution::TaskContext>,
    ) -> datafusion_common::Result<DFStream> {
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

        // Take ownership of the inner stream under a *synchronous* lock
        // (parking_lot). The lock is released immediately after the take;
        // it is **never** held across an `.await`. This satisfies the project
        // rule "Never hold locks across `.await`" and removes per-batch lock
        // acquisition + potential scheduler convoying during high-throughput
        // or mixed read/write ingestion.
        let schema = Arc::clone(&self.schema);
        let mut guard = self.stream.try_lock().ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "Stream is locked (concurrent access detected)".to_string(),
            )
        })?;

        let inner_stream = guard.take().ok_or_else(|| {
            datafusion_common::DataFusionError::Execution("Stream already consumed".to_string())
        })?;

        // Forward using `futures::stream::unfold`. The inner
        // `SendableRecordBatchStream` is owned directly by the state machine.
        // No mutex of any kind is involved in the per-batch `poll` path.
        // We avoid the `async_stream::stream!` macro (project guideline:
        // breaks rust-analyzer, harder to debug).
        let forward = unfold(inner_stream, |mut s: DFStream| async move {
            // next() -> Option<DFResult<RecordBatch>>
            s.next().await.map(|item| (item, s))
        });

        let adapter = RecordBatchStreamAdapter::new(schema, Box::pin(forward));
        Ok(Box::pin(adapter))
    }
}

/// Shared, resumable source that splits one stream into sequential row-bounded
/// chunks. Generic — no cold-tier/bloom knowledge; callers supply the row cap
/// and the reason (see `write_stream_to_cold`, which uses it to keep each cold
/// file under the per-file PK-bloom budget).
///
/// Successive [`RowChunkStream`]s pull from this one source; each stops after
/// its row budget, leaving the source positioned for the next chunk. Splitting a
/// globally-sorted stream at row boundaries preserves global order across chunks
/// (and yields disjoint per-chunk min/max ranges).
///
/// CONTRACT: **consume chunks sequentially** — poll one [`RowChunkStream`] to
/// completion before minting the next. Concurrent chunks would race the shared
/// inner stream. The inner stream lives behind a `parking_lot::Mutex` polled
/// **synchronously** inside `poll_next` and released before returning — it is
/// never held across an `.await` (poll returns immediately), matching this
/// module's `StreamingExec` convention. A `None` inner marks the source
/// exhausted.
pub(crate) struct RowChunkedSource {
    schema: SchemaRef,
    inner: Mutex<Option<DFStream>>,
}

impl RowChunkedSource {
    pub(crate) fn new(schema: SchemaRef, stream: DFStream) -> Arc<Self> {
        Arc::new(Self {
            schema,
            inner: Mutex::new(Some(stream)),
        })
    }

    /// `true` once the underlying stream has yielded end-of-stream (so a caller's
    /// chunk loop knows to stop minting further chunks).
    pub(crate) fn is_exhausted(&self) -> bool {
        self.inner.lock().is_none()
    }

    /// The next chunk: a stream that forwards batches from the shared source
    /// until it has emitted at least `row_cap` rows, then ends without consuming
    /// further. The boundary is at batch granularity, so the final batch may
    /// overshoot `row_cap` by up to one batch — set `row_cap` with headroom below
    /// any hard limit to absorb that.
    pub(crate) fn next_chunk(self: &Arc<Self>, row_cap: usize) -> RowChunkStream {
        RowChunkStream {
            source: Arc::clone(self),
            emitted: 0,
            row_cap,
        }
    }
}

/// One row-bounded chunk over a shared [`RowChunkedSource`]; see its docs.
pub(crate) struct RowChunkStream {
    source: Arc<RowChunkedSource>,
    emitted: usize,
    row_cap: usize,
}

impl Stream for RowChunkStream {
    type Item = datafusion_common::Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.emitted >= this.row_cap {
            // Chunk budget reached; end WITHOUT pulling further so the next
            // chunk resumes from the source's current position.
            return Poll::Ready(None);
        }
        let mut guard = this.source.inner.lock();
        let Some(inner) = guard.as_mut() else {
            return Poll::Ready(None);
        };
        match inner.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                this.emitted = this.emitted.saturating_add(batch.num_rows());
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => {
                *guard = None;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for RowChunkStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.source.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::stream;

    fn id_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn id_batch(schema: &SchemaRef, start: i64, n: i64) -> RecordBatch {
        let arr = Int64Array::from_iter_values(start..start + n);
        RecordBatch::try_new(Arc::clone(schema), vec![Arc::new(arr)])
            .expect("build test record batch")
    }

    fn source_stream(schema: &SchemaRef, batches: Vec<RecordBatch>) -> DFStream {
        let s = stream::iter(batches.into_iter().map(Ok));
        Box::pin(RecordBatchStreamAdapter::new(Arc::clone(schema), s))
    }

    async fn drain_ids(mut chunk: RowChunkStream) -> Vec<i64> {
        let mut out = Vec::new();
        while let Some(item) = chunk.next().await {
            let batch = item.expect("chunk batch is Ok");
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id column is Int64");
            out.extend(col.values().iter().copied());
        }
        out
    }

    /// A source larger than the row cap splits into multiple chunks; every row is
    /// preserved exactly once and in order (contiguous slices of the input), and
    /// no chunk exceeds the cap by more than one batch (batch-granularity cut).
    #[tokio::test]
    async fn chunk_source_splits_at_row_cap_preserving_order_and_rows() {
        let schema = id_schema();
        // 5 batches x 100 rows = ids 0..500.
        let batches: Vec<_> = (0..5).map(|i| id_batch(&schema, i * 100, 100)).collect();
        let source = RowChunkedSource::new(Arc::clone(&schema), source_stream(&schema, batches));

        let row_cap = 250usize;
        let batch_rows = 100usize;
        let mut all = Vec::new();
        let mut chunks = 0usize;
        while !source.is_exhausted() {
            let got = drain_ids(source.next_chunk(row_cap)).await;
            if got.is_empty() {
                // Exact-boundary empty tail: harmless, produces no files.
                break;
            }
            assert!(
                got.len() <= row_cap + batch_rows,
                "chunk exceeded cap + one batch: {}",
                got.len()
            );
            all.extend(got);
            chunks += 1;
        }
        assert!(chunks >= 2, "expected a split, got {chunks} chunk(s)");
        assert_eq!(
            all,
            (0..500).collect::<Vec<_>>(),
            "rows must be preserved exactly once, in order"
        );
        assert!(source.is_exhausted());
    }

    /// A source at or below the cap stays a single chunk (today's layout for the
    /// common case) and drains fully.
    #[tokio::test]
    async fn chunk_source_single_chunk_when_under_cap() {
        let schema = id_schema();
        let batches = vec![id_batch(&schema, 0, 100), id_batch(&schema, 100, 100)];
        let source = RowChunkedSource::new(Arc::clone(&schema), source_stream(&schema, batches));

        assert!(!source.is_exhausted());
        let first = drain_ids(source.next_chunk(10_000)).await;
        assert_eq!(first, (0..200).collect::<Vec<_>>());
        assert!(
            source.is_exhausted(),
            "source must be exhausted after a single under-cap chunk"
        );
    }

    /// An empty source yields an immediately-empty chunk and marks exhausted.
    #[tokio::test]
    async fn chunk_source_empty_source() {
        let schema = id_schema();
        let source = RowChunkedSource::new(Arc::clone(&schema), source_stream(&schema, vec![]));
        let got = drain_ids(source.next_chunk(100)).await;
        assert!(got.is_empty());
        assert!(source.is_exhausted());
    }
}
