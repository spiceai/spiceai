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
//! record batches from a stream without buffering, plus the bounded chunking
//! ([`ChunkedSource`]) and run-bounded sorting ([`bounded_sort_stream`])
//! primitives used by cold-tier promotion.

use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::execution::SendableRecordBatchStream as DFStream;
use datafusion_execution::TaskContext;
use datafusion_physical_plan::DisplayAs;
use datafusion_physical_plan::DisplayFormatType;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::PlanProperties;
use datafusion_physical_plan::RecordBatchStream;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType, Partitioning};
use datafusion_physical_plan::metrics::MetricsSet;
use futures::Stream;
use futures::StreamExt;
use futures::stream::unfold;
use parking_lot::Mutex;
use std::future::Future; // DIAG(cold-stall) kick-in: poll the self-wake timer
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Instant;

use super::stall_watchdog::{InputPollProbe, POLL_RET_BATCH, POLL_RET_EOF, POLL_RET_PENDING};

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

/// Cap for one [`ChunkedSource`] chunk. Boundaries are batch-granular in both
/// modes, so a chunk may overshoot its cap by up to one batch — give the cap
/// headroom below any hard limit.
#[derive(Clone, Copy, Debug)]
pub(crate) enum ChunkCap {
    /// End the chunk once it has emitted at least this many rows.
    Rows(usize),
    /// End the chunk once it has emitted at least this many bytes, measured by
    /// [`RecordBatch::get_array_memory_size`]. A sliced batch reports its full
    /// shared buffers, which errs toward *smaller* chunks — the conservative
    /// direction for bounding downstream memory.
    Bytes(usize),
}

impl ChunkCap {
    fn limit(self) -> usize {
        match self {
            ChunkCap::Rows(limit) | ChunkCap::Bytes(limit) => limit,
        }
    }

    fn measure(self, batch: &RecordBatch) -> usize {
        match self {
            ChunkCap::Rows(_) => batch.num_rows(),
            ChunkCap::Bytes(_) => batch.get_array_memory_size(),
        }
    }
}

/// Splits one stream into sequential bounded chunks over a shared source.
/// Generic (no cold-tier/bloom/clustering knowledge); the caller supplies the
/// cap: [`ChunkCap::Rows`] for the per-file PK-bloom row cap in
/// `write_stream_to_cold`, [`ChunkCap::Bytes`] for [`bounded_sort_stream`]
/// runs. Splitting a globally-sorted stream at chunk boundaries preserves
/// global order.
///
/// CONTRACT: consume chunks sequentially — drain one [`ChunkStream`] before
/// minting the next. Chunks share the single inner stream, so concurrent
/// chunks would interleave batches between chunks and race the stream's one
/// stored waker (the losing task hangs). A mint-time `debug_assert` on an
/// active-chunk flag (cleared on [`ChunkStream`] drop) fails loudly on
/// violations. The inner stream is polled synchronously under a
/// `parking_lot::Mutex` (never held across `.await`).
pub(crate) struct ChunkedSource {
    schema: SchemaRef,
    inner: Mutex<Option<DFStream>>,
    /// `true` while a minted [`ChunkStream`] is alive (see CONTRACT above).
    chunk_active: AtomicBool,
}

impl ChunkedSource {
    pub(crate) fn new(schema: SchemaRef, stream: DFStream) -> Arc<Self> {
        Arc::new(Self {
            schema,
            inner: Mutex::new(Some(stream)),
            chunk_active: AtomicBool::new(false),
        })
    }

    /// `true` once the underlying stream has ended (caller stops minting chunks).
    pub(crate) fn is_exhausted(&self) -> bool {
        self.inner.lock().is_none()
    }

    /// A chunk that forwards batches until it has emitted `>= cap` (rows or
    /// bytes per [`ChunkCap`]), then ends. The next chunk resumes exactly where
    /// this one stopped — provided chunks are consumed sequentially (CONTRACT).
    pub(crate) fn next_chunk(self: &Arc<Self>, cap: ChunkCap) -> ChunkStream {
        let was_active = self.chunk_active.swap(true, Ordering::AcqRel);
        // Dev/test: fail loud (panic) so violations are caught in CI.
        debug_assert!(
            !was_active,
            "ChunkedSource contract violation: minted a new chunk while the previous chunk is still live — chunks must be consumed sequentially"
        );
        // Release: a `debug_assert` is compiled out, so without this a violation
        // would degrade to a concurrent chunk racing the inner stream's single
        // stored waker — the losing task parks forever (a SILENT hang, exactly the
        // cold-promotion freeze signature). Instead, poison the offending chunk so
        // it yields a typed error on first poll: the promotion aborts loudly and
        // retries next tick rather than hanging with `write_lock` held.
        if was_active {
            tracing::error!(
                target: "cayenne::compaction",
                "ChunkedSource contract violation: minted a chunk while another is still live — poisoning it (yields an error, not a silent hang). Chunks MUST be consumed sequentially."
            );
        }
        ChunkStream {
            source: Arc::clone(self),
            consumed: 0,
            cap,
            poisoned: was_active,
        }
    }
}

/// One bounded chunk over a shared [`ChunkedSource`]; see its CONTRACT docs.
pub(crate) struct ChunkStream {
    source: Arc<ChunkedSource>,
    consumed: usize,
    cap: ChunkCap,
    /// Set when this chunk was minted while another was still live (a CONTRACT
    /// violation). It yields a typed error on first poll instead of racing the
    /// shared inner-stream waker and hanging. It also never owned `chunk_active`,
    /// so its `Drop` must not clear that flag (which the live chunk still holds).
    poisoned: bool,
}

impl Drop for ChunkStream {
    fn drop(&mut self) {
        // A poisoned chunk never legitimately held the active flag — leave it so
        // the still-live chunk keeps ownership.
        if !self.poisoned {
            self.source.chunk_active.store(false, Ordering::Release);
        }
    }
}

impl Stream for ChunkStream {
    type Item = datafusion_common::Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.poisoned {
            // Contract violation (concurrent chunk) — abort instead of hanging.
            return Poll::Ready(Some(Err(datafusion_common::DataFusionError::Execution(
                "ChunkedSource contract violation: a chunk was minted before the previous one \
                 was drained; aborting to avoid a silent hang (chunks must be consumed sequentially)"
                    .to_string(),
            ))));
        }
        if this.consumed >= this.cap.limit() {
            // Budget reached — end without pulling, so the next chunk resumes here.
            return Poll::Ready(None);
        }
        let mut guard = this.source.inner.lock();
        let Some(inner) = guard.as_mut() else {
            return Poll::Ready(None);
        };
        match inner.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                this.consumed = this.consumed.saturating_add(this.cap.measure(&batch));
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

impl RecordBatchStream for ChunkStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.source.schema)
    }
}

/// Rows/bytes one sort run consumed, shared between the run's input tap (which
/// counts) and the [`bounded_sort_stream`] driver (which checks row
/// conservation once the run's sorted output completes).
#[derive(Default)]
struct RunInputCounters {
    rows: AtomicUsize,
    bytes: AtomicUsize,
}

/// Input tap for one sort run: forwards a [`ChunkStream`] into `SortExec`,
/// counting rows/bytes and logging the `input consumed` event when the
/// sub-stream ends (in streaming mode the run's row count is only known here).
struct RunInputStream {
    chunk: ChunkStream,
    counters: Arc<RunInputCounters>,
    table_name: Arc<str>,
    run_idx: usize,
    started: Instant,
    ended: bool,
    /// Optional shared counter (rows the scan has fed to ALL runs so far),
    /// surfaced to the stall watchdog. Comparing this against the sink-side
    /// `progress` counter pinpoints a stalled promotion as scan-side (input
    /// frozen ⇒ the scan feeding this run is parked) vs sort-internal (input
    /// advancing/complete but no sorted output). Diagnostics only.
    input_rows_total: Option<Arc<std::sync::atomic::AtomicU64>>,
    /// Poll-boundary probe (entered/inflight/last_return) surfaced to the stall
    /// watchdog to separate SortExec-not-polling vs polled-Pending vs
    /// stuck-inside-poll when `input_rows_total` is frozen. Diagnostics only.
    input_poll: Option<Arc<InputPollProbe>>,
    /// DIAG(cold-stall) kick-in: periodic self-wake timer. When the underlying scan
    /// returns Pending (possibly with a lost drain-wake), we arm a ~1.5s timer on
    /// `cx` so the SortExec re-polls this input and drains any stranded-ready
    /// backlog instead of freezing forever. REVERT before merge.
    kick: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl Stream for RunInputStream {
    type Item = datafusion_common::Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        // Poll-boundary probe: mark a poll in flight BEFORE delegating to the
        // inner (scan) stream. If the inner poll blocks synchronously (stuck in
        // the ChunkStream mutex or the sync execute_arrow decode), `inflight`
        // stays > 0 — the "stuck inside the poll" signal. On return we record the
        // outcome so the watchdog can tell Pending-then-parked from not-polling.
        if let Some(p) = this.input_poll.as_ref() {
            p.entered.fetch_add(1, Ordering::Relaxed);
            p.inflight.fetch_add(1, Ordering::Relaxed);
        }
        let polled = Pin::new(&mut this.chunk).poll_next(cx);
        if let Some(p) = this.input_poll.as_ref() {
            p.inflight.fetch_sub(1, Ordering::Relaxed);
            p.last_return.store(
                match &polled {
                    Poll::Pending => POLL_RET_PENDING,
                    Poll::Ready(None) => POLL_RET_EOF,
                    Poll::Ready(Some(_)) => POLL_RET_BATCH,
                },
                Ordering::Relaxed,
            );
        }
        match polled {
            Poll::Ready(Some(Ok(batch))) => {
                this.counters
                    .rows
                    .fetch_add(batch.num_rows(), Ordering::Relaxed);
                this.counters
                    .bytes
                    .fetch_add(batch.get_array_memory_size(), Ordering::Relaxed);
                if let Some(total) = this.input_rows_total.as_ref() {
                    total.fetch_add(
                        u64::try_from(batch.num_rows()).unwrap_or(0),
                        Ordering::Relaxed,
                    );
                }
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(None) => {
                if !this.ended {
                    this.ended = true;
                    tracing::info!(
                        target: "cayenne::compaction",
                        table = this.table_name.as_ref(),
                        run_idx = this.run_idx,
                        input_rows = this.counters.rows.load(Ordering::Relaxed),
                        input_bytes = this.counters.bytes.load(Ordering::Relaxed),
                        read_ms = this.started.elapsed().as_millis(),
                        "Bounded sort run input consumed"
                    );
                }
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error))),
            Poll::Pending => {
                // DIAG(cold-stall) kick-in: arm a ~1.5s self-wake so a lost drain-wake
                // self-heals — the SortExec re-polls this input and drains any
                // stranded-ready backlog instead of freezing. REVERT before merge.
                let mut kick =
                    Box::pin(tokio::time::sleep(std::time::Duration::from_millis(1500)));
                if kick.as_mut().poll(cx).is_pending() {
                    this.kick = Some(kick);
                }
                Poll::Pending
            }
        }
    }
}

impl RecordBatchStream for RunInputStream {
    fn schema(&self) -> SchemaRef {
        self.chunk.schema()
    }
}

/// One in-flight sort run in the [`bounded_sort_stream`] driver.
struct BoundedSortRun {
    stream: DFStream,
    counters: Arc<RunInputCounters>,
    run_idx: usize,
    started: Instant,
    output_rows: usize,
    output_batches: usize,
    /// The run's `SortExec` plan, kept so its spill metrics can be read once the
    /// run's output stream is fully drained.
    plan: Option<Arc<dyn ExecutionPlan>>,
}

/// Driver state for [`bounded_sort_stream`]'s `unfold` loop.
struct BoundedSortState {
    source: Arc<ChunkedSource>,
    current: Option<BoundedSortRun>,
    next_run_idx: usize,
    /// Set after an error is emitted; the driver then ends the stream instead
    /// of minting further runs (never emit partial output silently).
    failed: bool,
}

/// Sort a stream in sequential byte-bounded runs: split the input at
/// `run_size_bytes` boundaries ([`ChunkCap::Bytes`], ≤1 batch overshoot) and
/// sort each run through `util::stream_utils::sort_stream` (`SortExec`:
/// memory-pool-accounted, disk-spilling). The scan overlaps each run's sort
/// insert phase; runs are strictly sequential (run N+1's input resumes where
/// run N's ended).
///
/// CONTRACT (load-bearing): the output is sorted *within each run only* — it
/// has NO global order. It must never feed a path that advertises a
/// lexicographic ordering (e.g. the warm rewrite path, whose files advertise
/// `file_sort_order`) or otherwise relies on global order. Cold promotion is
/// safe because cold files are pruned by per-file min/max + PK bloom and
/// advertise no ordering; bounding the sort trades slight Z-order range
/// overlap across runs for bounded sort memory and first-batch latency.
///
/// Rows are conserved: each run's consumed row count is checked against its
/// emitted row count, and a mismatch terminates the stream with an error.
/// Empty `sort_columns` returns the input unchanged.
pub(crate) fn bounded_sort_stream(
    table_name: &str,
    stream: DFStream,
    sort_columns: Vec<String>,
    task_ctx: &Arc<TaskContext>,
    run_size_bytes: usize,
    input_rows_total: Option<Arc<std::sync::atomic::AtomicU64>>,
    input_poll: Option<Arc<InputPollProbe>>,
) -> DFStream {
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

    if sort_columns.is_empty() {
        return stream;
    }
    let schema = stream.schema();
    let table_name: Arc<str> = Arc::from(table_name);
    let task_ctx = Arc::clone(task_ctx);
    let state = BoundedSortState {
        source: ChunkedSource::new(Arc::clone(&schema), stream),
        current: None,
        next_run_idx: 0,
        failed: false,
    };

    // Sequential `unfold` driver (not the banned `stream!` macro): drain the
    // current run's sorted output; when it ends, verify row conservation and
    // mint the next run — checking `is_exhausted` first so input ending
    // exactly at a run boundary doesn't spin up an empty trailing SortExec.
    let runs = unfold(state, move |mut state| {
        let sort_columns = sort_columns.clone();
        let task_ctx = Arc::clone(&task_ctx);
        let table_name = Arc::clone(&table_name);
        let input_rows_total = input_rows_total.clone();
        let input_poll = input_poll.clone();
        async move {
            loop {
                if let Some(run) = state.current.as_mut() {
                    match run.stream.next().await {
                        Some(Ok(batch)) => {
                            if run.output_batches == 0 {
                                tracing::info!(
                                    target: "cayenne::compaction",
                                    table = table_name.as_ref(),
                                    run_idx = run.run_idx,
                                    first_batch_ms = run.started.elapsed().as_millis(),
                                    "Bounded sort run first batch"
                                );
                            }
                            run.output_rows = run.output_rows.saturating_add(batch.num_rows());
                            run.output_batches = run.output_batches.saturating_add(1);
                            return Some((Ok(batch), state));
                        }
                        Some(Err(error)) => {
                            // Drop the failed run so the next poll ends the
                            // stream instead of re-entering it (which would
                            // surface a misleading row-mismatch error too).
                            state.current = None;
                            state.failed = true;
                            return Some((Err(error), state));
                        }
                        None => {
                            let input_rows = run.counters.rows.load(Ordering::Relaxed);
                            let (run_idx, output_rows, output_batches) =
                                (run.run_idx, run.output_rows, run.output_batches);
                            let total_ms = run.started.elapsed().as_millis();
                            // Spill metrics for this run's SortExec are final now
                            // that its output stream has ended — the direct signal
                            // for "slow because the sort spilled under memory
                            // pressure" vs "slow for another reason". Read before
                            // clearing `state.current` (which drops `run`).
                            let metrics = run.plan.as_ref().and_then(|p| p.metrics());
                            let spill_count = metrics.as_ref().and_then(MetricsSet::spill_count);
                            let spilled_bytes =
                                metrics.as_ref().and_then(MetricsSet::spilled_bytes);
                            state.current = None;
                            if input_rows != output_rows {
                                // Data-correctness backstop: a sort must never
                                // change the row count. Abort instead of
                                // writing possibly-wrong cold data.
                                state.failed = true;
                                let error = datafusion_common::DataFusionError::Execution(format!(
                                    "Bounded sort run {run_idx} for table {table_name} consumed {input_rows} rows but emitted {output_rows}; aborting cold write to avoid data loss"
                                ));
                                return Some((Err(error), state));
                            }
                            tracing::info!(
                                target: "cayenne::compaction",
                                table = table_name.as_ref(),
                                run_idx,
                                output_rows,
                                output_batches,
                                total_ms,
                                spill_count = ?spill_count,
                                spilled_bytes = ?spilled_bytes,
                                "Bounded sort run complete"
                            );
                            continue;
                        }
                    }
                }
                if state.failed || state.source.is_exhausted() {
                    return None;
                }
                let run_idx = state.next_run_idx;
                state.next_run_idx = state.next_run_idx.saturating_add(1);
                tracing::info!(
                    target: "cayenne::compaction",
                    table = table_name.as_ref(),
                    run_idx,
                    run_size_bytes,
                    "Bounded sort run starting"
                );
                let counters = Arc::new(RunInputCounters::default());
                let input: DFStream = Box::pin(RunInputStream {
                    chunk: state.source.next_chunk(ChunkCap::Bytes(run_size_bytes)),
                    counters: Arc::clone(&counters),
                    table_name: Arc::clone(&table_name),
                    run_idx,
                    started: Instant::now(),
                    ended: false,
                    input_rows_total: input_rows_total.clone(),
                    input_poll: input_poll.clone(),
                    kick: None,
                });
                match util::stream_utils::sort_stream_with_plan(input, &sort_columns, &task_ctx) {
                    Ok((sorted, plan)) => {
                        state.current = Some(BoundedSortRun {
                            stream: sorted,
                            counters,
                            run_idx,
                            started: Instant::now(),
                            output_rows: 0,
                            output_batches: 0,
                            plan,
                        });
                    }
                    Err(error) => {
                        state.failed = true;
                        return Some((Err(error), state));
                    }
                }
            }
        }
    });
    Box::pin(RecordBatchStreamAdapter::new(schema, runs))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::prelude::SessionContext;
    use futures::stream;

    fn id_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn nullable_id_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]))
    }

    fn id_batch(schema: &SchemaRef, start: i64, n: i64) -> RecordBatch {
        let arr = Int64Array::from_iter_values(start..start + n);
        RecordBatch::try_new(Arc::clone(schema), vec![Arc::new(arr)])
            .expect("build test record batch")
    }

    fn id_batch_of(schema: &SchemaRef, values: &[i64]) -> RecordBatch {
        let arr = Int64Array::from_iter_values(values.iter().copied());
        RecordBatch::try_new(Arc::clone(schema), vec![Arc::new(arr)])
            .expect("build test record batch")
    }

    fn source_stream(schema: &SchemaRef, batches: Vec<RecordBatch>) -> DFStream {
        let s = stream::iter(batches.into_iter().map(Ok));
        Box::pin(RecordBatchStreamAdapter::new(Arc::clone(schema), s))
    }

    struct PendingOnceBeforeEofStream {
        schema: SchemaRef,
        batch: Option<RecordBatch>,
        returned_pending: bool,
    }

    impl PendingOnceBeforeEofStream {
        fn new(schema: &SchemaRef, batch: RecordBatch) -> Self {
            Self {
                schema: Arc::clone(schema),
                batch: Some(batch),
                returned_pending: false,
            }
        }
    }

    impl Stream for PendingOnceBeforeEofStream {
        type Item = datafusion_common::Result<RecordBatch>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if let Some(batch) = self.batch.take() {
                return Poll::Ready(Some(Ok(batch)));
            }
            if !self.returned_pending {
                self.returned_pending = true;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(None)
        }
    }

    impl RecordBatchStream for PendingOnceBeforeEofStream {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    async fn drain_ids<S>(mut stream: S) -> Vec<i64>
    where
        S: Stream<Item = datafusion_common::Result<RecordBatch>> + Unpin,
    {
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            let batch = item.expect("stream batch is Ok");
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id column is Int64");
            out.extend(col.values().iter().copied());
        }
        out
    }

    fn test_task_ctx() -> Arc<TaskContext> {
        SessionContext::new().task_ctx()
    }

    /// Splits past the cap; rows preserved exactly once and in order, no chunk
    /// over cap + one batch.
    #[tokio::test]
    async fn chunk_source_splits_at_row_cap_preserving_order_and_rows() {
        let schema = id_schema();
        // 5 batches x 100 rows = ids 0..500.
        let batches: Vec<_> = (0..5).map(|i| id_batch(&schema, i * 100, 100)).collect();
        let source = ChunkedSource::new(Arc::clone(&schema), source_stream(&schema, batches));

        let row_cap = 250usize;
        let batch_rows = 100usize;
        let mut all = Vec::new();
        let mut chunks = 0usize;
        while !source.is_exhausted() {
            let got = drain_ids(source.next_chunk(ChunkCap::Rows(row_cap))).await;
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

    /// At or below the cap: a single chunk that drains fully.
    #[tokio::test]
    async fn chunk_source_single_chunk_when_under_cap() {
        let schema = id_schema();
        let batches = vec![id_batch(&schema, 0, 100), id_batch(&schema, 100, 100)];
        let source = ChunkedSource::new(Arc::clone(&schema), source_stream(&schema, batches));

        assert!(!source.is_exhausted());
        let first = drain_ids(source.next_chunk(ChunkCap::Rows(10_000))).await;
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
        let source = ChunkedSource::new(Arc::clone(&schema), source_stream(&schema, vec![]));
        let got = drain_ids(source.next_chunk(ChunkCap::Rows(100))).await;
        assert!(got.is_empty());
        assert!(source.is_exhausted());
    }

    /// Byte cap splits at batch granularity: with a cap of two batches' bytes,
    /// each chunk carries exactly two of the five batches (last chunk one).
    #[tokio::test]
    async fn chunk_source_splits_at_byte_cap() {
        let schema = id_schema();
        let batch_bytes = id_batch(&schema, 0, 100).get_array_memory_size();
        let batches: Vec<_> = (0..5).map(|i| id_batch(&schema, i * 100, 100)).collect();
        let source = ChunkedSource::new(Arc::clone(&schema), source_stream(&schema, batches));

        let cap = ChunkCap::Bytes(batch_bytes * 2);
        let mut all = Vec::new();
        let mut chunk_sizes = Vec::new();
        while !source.is_exhausted() {
            let got = drain_ids(source.next_chunk(cap)).await;
            if got.is_empty() {
                break;
            }
            chunk_sizes.push(got.len());
            all.extend(got);
        }
        assert_eq!(chunk_sizes, vec![200, 200, 100], "two batches per chunk");
        assert_eq!(all, (0..500).collect::<Vec<_>>());
        assert!(source.is_exhausted());
    }

    /// A single batch larger than the byte cap forms a one-batch chunk — the
    /// cap is checked before each pull, so there is no infinite loop.
    #[tokio::test]
    async fn chunk_source_byte_cap_smaller_than_one_batch() {
        let schema = id_schema();
        let batches: Vec<_> = (0..3).map(|i| id_batch(&schema, i * 10, 10)).collect();
        let source = ChunkedSource::new(Arc::clone(&schema), source_stream(&schema, batches));

        let mut chunk_sizes = Vec::new();
        while !source.is_exhausted() {
            let got = drain_ids(source.next_chunk(ChunkCap::Bytes(1))).await;
            if got.is_empty() {
                break;
            }
            chunk_sizes.push(got.len());
        }
        assert_eq!(chunk_sizes, vec![10, 10, 10], "one batch per chunk");
    }

    /// Zero-row batches are forwarded (never looped on) and never satisfy a
    /// row cap by themselves — the chunk keeps draining to input end.
    #[tokio::test]
    async fn chunk_source_forwards_zero_row_batches() {
        let schema = id_schema();
        let batches = vec![
            id_batch(&schema, 0, 0),
            id_batch(&schema, 0, 5),
            id_batch(&schema, 0, 0),
        ];
        let source = ChunkedSource::new(Arc::clone(&schema), source_stream(&schema, batches));
        let got = drain_ids(source.next_chunk(ChunkCap::Rows(100))).await;
        assert_eq!(got, (0..5).collect::<Vec<_>>());
        assert!(source.is_exhausted());
    }

    /// An error mid-chunk is forwarded to the chunk's consumer.
    #[tokio::test]
    async fn chunk_source_propagates_error() {
        let schema = id_schema();
        let items: Vec<datafusion_common::Result<RecordBatch>> = vec![
            Ok(id_batch(&schema, 0, 10)),
            Err(datafusion_common::DataFusionError::Execution(
                "boom".to_string(),
            )),
        ];
        let s: DFStream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            stream::iter(items),
        ));
        let source = ChunkedSource::new(Arc::clone(&schema), s);
        let mut chunk = source.next_chunk(ChunkCap::Rows(100));
        let first = chunk.next().await.expect("first item present");
        assert_eq!(first.expect("first batch ok").num_rows(), 10);
        let second = chunk.next().await.expect("error item present");
        let error = second.expect_err("second item must be the propagated error");
        assert!(error.to_string().contains("boom"), "unexpected: {error}");
    }

    /// Minting a second chunk while the first is still live violates the
    /// sequential-consumption contract and must fail loudly (debug builds).
    #[cfg(debug_assertions)]
    #[tokio::test]
    #[should_panic(expected = "contract violation")]
    async fn chunk_source_mint_while_active_panics() {
        let schema = id_schema();
        let batches = vec![id_batch(&schema, 0, 10)];
        let source = ChunkedSource::new(Arc::clone(&schema), source_stream(&schema, batches));
        let _live = source.next_chunk(ChunkCap::Rows(5));
        let _second = source.next_chunk(ChunkCap::Rows(5));
    }

    /// Two byte-bounded runs, each internally sorted; the concatenation is NOT
    /// globally sorted (run 1's key range sits above run 2's), proving the
    /// sort was bounded per run. Rows are conserved.
    #[tokio::test]
    async fn bounded_sort_sorts_within_runs_only() {
        let schema = id_schema();
        let batch_bytes = id_batch_of(&schema, &[0, 0, 0, 0]).get_array_memory_size();
        // Run 1 = first two batches (keys 8..16), run 2 = last two (keys 0..8).
        let batches = vec![
            id_batch_of(&schema, &[15, 13, 11, 9]),
            id_batch_of(&schema, &[14, 12, 10, 8]),
            id_batch_of(&schema, &[7, 5, 3, 1]),
            id_batch_of(&schema, &[6, 4, 2, 0]),
        ];
        let input = source_stream(&schema, batches);
        let sorted = bounded_sort_stream(
            "test_table",
            input,
            vec!["id".to_string()],
            &test_task_ctx(),
            batch_bytes * 2,
            None,
            None,
        );
        let got = drain_ids(sorted).await;
        let expected: Vec<i64> = (8..16).chain(0..8).collect();
        assert_eq!(
            got, expected,
            "each run sorted internally; no global order across runs"
        );
    }

    /// Exact run-boundary EOF is discovered by the next run because
    /// `ChunkStream` ends on the cap without lookahead. If that next run sees
    /// `Pending` before EOF, a correct upstream wake must let `SortExec` poll
    /// again and finish instead of parking forever.
    #[tokio::test]
    async fn bounded_sort_exact_cap_then_pending_eof_does_not_deadlock() {
        use std::time::Duration;

        let schema = id_schema();
        let batch = id_batch_of(&schema, &[3, 1, 2, 0]);
        let run_size = batch.get_array_memory_size();
        let input: DFStream = Box::pin(PendingOnceBeforeEofStream::new(&schema, batch));
        let input_poll = Arc::new(InputPollProbe::default());

        let sorted = bounded_sort_stream(
            "test_table",
            input,
            vec!["id".to_string()],
            &test_task_ctx(),
            run_size,
            None,
            Some(Arc::clone(&input_poll)),
        );

        let got = tokio::time::timeout(Duration::from_secs(5), drain_ids(sorted))
            .await
            .expect("bounded sort must not hang after exact cap then Pending-before-EOF");
        assert_eq!(got, vec![0, 1, 2, 3]);
        assert_eq!(
            input_poll.inflight.load(Ordering::Relaxed),
            0,
            "no input poll should remain stuck in ChunkStream"
        );
        assert_eq!(
            input_poll.last_return.load(Ordering::Relaxed),
            POLL_RET_EOF,
            "the probe run should wake and observe EOF after Pending"
        );
    }

    /// Ignored stress harness for the `buffer_unordered`/`FuturesUnordered`
    /// discussion around independently-spawned futures completing after the
    /// parent stream has parked on `Pending`. It intentionally uses plain Tokio
    /// `JoinHandle`s first: if this fails, the problem is in the futures/Tokio
    /// composition itself; if it passes, the next target is Vortex's `Task`
    /// wrapper or the real scan pipeline.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "stress harness for a suspected buffer_unordered tail-wake race; run explicitly"]
    async fn buffer_unordered_spawned_join_tail_wake_stress() {
        use std::env;
        use std::sync::atomic::AtomicUsize;
        use std::time::Duration;
        use tokio::sync::watch;

        let iterations = env::var("BUFFER_UNORDERED_STRESS_ITERS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(10_000);
        let tasks = env::var("BUFFER_UNORDERED_STRESS_TASKS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(512);
        let concurrency = env::var("BUFFER_UNORDERED_STRESS_CONCURRENCY")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(128);

        for iter in 0..iterations {
            let completed = Arc::new(AtomicUsize::new(0));
            let yielded = Arc::new(AtomicUsize::new(0));
            let (gate_tx, gate_rx) = watch::channel(false);

            let handles: Vec<_> = (0..tasks)
                .map(|task_idx| {
                    let mut gate_rx = gate_rx.clone();
                    let completed = Arc::clone(&completed);
                    tokio::spawn(async move {
                        while !*gate_rx.borrow_and_update() {
                            gate_rx.changed().await.expect("gate sender lives");
                        }
                        if task_idx % 3 == 0 {
                            tokio::task::yield_now().await;
                        }
                        completed.fetch_add(1, Ordering::Relaxed);
                        task_idx
                    })
                })
                .collect();
            drop(gate_rx);

            let stream = stream::iter(handles).buffer_unordered(concurrency);
            tokio::pin!(stream);

            // Poll once before opening the gate so `buffer_unordered` fills its
            // in-flight set and parks with no completed join handles.
            let first = tokio::time::timeout(Duration::from_millis(25), stream.next()).await;
            assert!(
                first.is_err(),
                "iteration {iter}: stream produced before the task gate opened"
            );

            gate_tx.send(true).expect("receivers live");
            let drained = tokio::time::timeout(Duration::from_secs(5), async {
                let mut count = 0usize;
                while let Some(item) = stream.next().await {
                    let _task_idx = item.expect("spawned task should not panic");
                    yielded.fetch_add(1, Ordering::Relaxed);
                    count += 1;
                }
                count
            })
            .await;

            assert!(
                matches!(drained, Ok(n) if n == tasks),
                "iteration {iter}: buffer_unordered did not drain after gate release; drained={drained:?} completed={} yielded={}",
                completed.load(Ordering::Relaxed),
                yielded.load(Ordering::Relaxed),
            );
        }
    }

    /// Same tail-wake stress as [`buffer_unordered_spawned_join_tail_wake_stress`],
    /// but using Vortex's `Task` wrapper (`Handle::spawn` + oneshot receiver),
    /// which is the shape used by the Vortex scan builder before
    /// `buffer_unordered`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "stress harness for Vortex Task + buffer_unordered tail-wake race; run explicitly"]
    async fn buffer_unordered_vortex_task_tail_wake_stress() {
        use std::env;
        use std::sync::atomic::AtomicUsize;
        use std::time::Duration;
        use tokio::sync::watch;
        use vortex::io::runtime::Handle;

        let iterations = env::var("BUFFER_UNORDERED_STRESS_ITERS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(10_000);
        let tasks = env::var("BUFFER_UNORDERED_STRESS_TASKS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(512);
        let concurrency = env::var("BUFFER_UNORDERED_STRESS_CONCURRENCY")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(128);
        let handle = Handle::find().expect("tokio-backed Vortex runtime handle");

        for iter in 0..iterations {
            let completed = Arc::new(AtomicUsize::new(0));
            let yielded = Arc::new(AtomicUsize::new(0));
            let (gate_tx, gate_rx) = watch::channel(false);

            let tasks_vec: Vec<_> = (0..tasks)
                .map(|task_idx| {
                    let mut gate_rx = gate_rx.clone();
                    let completed = Arc::clone(&completed);
                    handle.spawn(async move {
                        while !*gate_rx.borrow_and_update() {
                            gate_rx.changed().await.expect("gate sender lives");
                        }
                        if task_idx % 3 == 0 {
                            tokio::task::yield_now().await;
                        }
                        completed.fetch_add(1, Ordering::Relaxed);
                        task_idx
                    })
                })
                .collect();
            drop(gate_rx);

            let stream = stream::iter(tasks_vec).buffer_unordered(concurrency);
            tokio::pin!(stream);

            let first = tokio::time::timeout(Duration::from_millis(25), stream.next()).await;
            assert!(
                first.is_err(),
                "iteration {iter}: stream produced before the task gate opened"
            );

            gate_tx.send(true).expect("receivers live");
            let drained = tokio::time::timeout(Duration::from_secs(5), async {
                let mut count = 0usize;
                while let Some(task_idx) = stream.next().await {
                    yielded.fetch_add(1, Ordering::Relaxed);
                    count += 1;
                    let _ = task_idx;
                }
                count
            })
            .await;

            assert!(
                matches!(drained, Ok(n) if n == tasks),
                "iteration {iter}: Vortex Task buffer_unordered did not drain after gate release; drained={drained:?} completed={} yielded={}",
                completed.load(Ordering::Relaxed),
                yielded.load(Ordering::Relaxed),
            );
        }
    }

    /// NULLs in the sort column: rows conserved, NULLs sorted last within each
    /// run (the `sort_stream` ascending default).
    #[tokio::test]
    async fn bounded_sort_conserves_rows_with_nulls() {
        let schema = nullable_id_schema();
        let batch = |values: Vec<Option<i64>>| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(values))],
            )
            .expect("build nullable test batch")
        };
        let batches = vec![
            batch(vec![Some(3), None, Some(1)]),
            batch(vec![None, Some(2), None]),
        ];
        let input = source_stream(&schema, batches);
        let sorted = bounded_sort_stream(
            "test_table",
            input,
            vec!["id".to_string()],
            &test_task_ctx(),
            usize::MAX,
            None,
            None,
        );
        let mut rows = 0usize;
        let mut nulls = 0usize;
        let mut stream = sorted;
        while let Some(item) = stream.next().await {
            let batch = item.expect("sorted batch ok");
            rows += batch.num_rows();
            nulls += batch.column(0).null_count();
        }
        assert_eq!(rows, 6, "row conservation across runs incl. NULLs");
        assert_eq!(nulls, 3, "NULLs preserved");
    }

    /// An empty input stream produces an empty output and no error.
    #[tokio::test]
    async fn bounded_sort_empty_input() {
        let schema = id_schema();
        let input = source_stream(&schema, vec![]);
        let sorted = bounded_sort_stream(
            "test_table",
            input,
            vec!["id".to_string()],
            &test_task_ctx(),
            1024,
            None,
            None,
        );
        let got = drain_ids(sorted).await;
        assert!(got.is_empty());
    }

    /// Empty sort columns: passthrough, batches unchanged and in order.
    #[tokio::test]
    async fn bounded_sort_empty_columns_passthrough() {
        let schema = id_schema();
        let batches = vec![
            id_batch_of(&schema, &[5, 3, 4]),
            id_batch_of(&schema, &[2, 0, 1]),
        ];
        let input = source_stream(&schema, batches);
        let sorted = bounded_sort_stream(
            "test_table",
            input,
            Vec::new(),
            &test_task_ctx(),
            1,
            None,
            None,
        );
        let got = drain_ids(sorted).await;
        assert_eq!(got, vec![5, 3, 4, 2, 0, 1], "passthrough preserves order");
    }

    /// A scan error mid-run terminates the output with that error; no further
    /// runs are minted (no silent partial output).
    #[tokio::test]
    async fn bounded_sort_propagates_error_and_stops() {
        let schema = id_schema();
        let items: Vec<datafusion_common::Result<RecordBatch>> = vec![
            Ok(id_batch(&schema, 0, 10)),
            Err(datafusion_common::DataFusionError::Execution(
                "scan failed".to_string(),
            )),
            Ok(id_batch(&schema, 10, 10)),
        ];
        let input: DFStream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            stream::iter(items),
        ));
        let mut sorted = bounded_sort_stream(
            "test_table",
            input,
            vec!["id".to_string()],
            &test_task_ctx(),
            usize::MAX,
            None,
            None,
        );
        let mut saw_error = false;
        while let Some(item) = sorted.next().await {
            if let Err(error) = item {
                assert!(
                    error.to_string().contains("scan failed"),
                    "unexpected error: {error}"
                );
                saw_error = true;
                break;
            }
        }
        assert!(saw_error, "the scan error must be propagated");
        assert!(
            sorted.next().await.is_none(),
            "the stream must end after the error (no further runs)"
        );
    }

    /// Reproduction guard for the cold-promotion multi-run deadlock observed in
    /// CI (a `run_idx>=1` `SortExec` starts but never emits; the promotion holds
    /// `write_lock` and the runtime never becomes ready). Recreates the
    /// distinguishing conditions the other tests lack: MANY spilling sort runs
    /// over an ASYNC input (returns `Pending` before every batch, like a real
    /// scan), with TWO sorts running CONCURRENTLY against one small SHARED memory
    /// pool (mirrors order_line + stock promotions contending on the compaction
    /// pool). Must finish within the timeout — a hang here is the CI deadlock.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn bounded_sort_concurrent_multi_run_spilling_does_not_deadlock() {
        use datafusion::execution::memory_pool::GreedyMemoryPool;
        use datafusion::execution::runtime_env::RuntimeEnvBuilder;
        use datafusion::prelude::SessionConfig;
        use std::time::Duration;

        let schema = id_schema();
        let batch_rows = 8192i64;
        let n_batches = 1000usize; // ~64 MiB/stream so a 32 MiB pool genuinely spills
        let batch_bytes = id_batch(&schema, 0, batch_rows).get_array_memory_size();

        // One SHARED pool sized to force real disk spilling (not an immediate
        // ResourcesExhausted) across both concurrent sorts — the disk-spill path
        // is what differs run-0 (works) from run-1 (hung) at scale.
        let rtenv = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(32 * 1024 * 1024)))
            .build_arc()
            .expect("build runtime env");
        let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), rtenv).task_ctx();

        // Async input: `yield_now` (returns Pending once) before each batch to
        // exercise real task parking/waking across run boundaries.
        let async_input = |schema: SchemaRef| -> DFStream {
            let batches: Vec<datafusion_common::Result<RecordBatch>> = (0..n_batches)
                .map(|i| {
                    #[expect(clippy::cast_possible_wrap, reason = "test indices are tiny")]
                    let start = i as i64 * batch_rows;
                    Ok(id_batch(&schema, start, batch_rows))
                })
                .collect();
            Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&schema),
                stream::iter(batches).then(|b| async move {
                    tokio::task::yield_now().await;
                    b
                }),
            ))
        };

        let run_size = batch_bytes * 8; // many spilling runs per stream
        let s1 = bounded_sort_stream(
            "t1",
            async_input(Arc::clone(&schema)),
            vec!["id".to_string()],
            &ctx,
            run_size,
            None,
            None,
        );
        let s2 = bounded_sort_stream(
            "t2",
            async_input(Arc::clone(&schema)),
            vec!["id".to_string()],
            &ctx,
            run_size,
            None,
            None,
        );

        // Errors (e.g. ResourcesExhausted) are NOT the deadlock — the test only
        // guards against a hang. Returns (rows, errored) so we can confirm the
        // run actually sorted/spilled rather than erroring out immediately.
        async fn drain(mut s: DFStream) -> (usize, bool) {
            let mut rows = 0usize;
            let mut errored = false;
            while let Some(item) = s.next().await {
                match item {
                    Ok(b) => rows += b.num_rows(),
                    Err(_) => {
                        errored = true;
                        break;
                    }
                }
            }
            (rows, errored)
        }

        let res = tokio::time::timeout(Duration::from_secs(30), async {
            tokio::join!(drain(s1), drain(s2))
        })
        .await;
        // eprintln (not tracing) so it survives the test harness's thread-local subscriber.
        eprintln!("bounded_sort concurrent repro result: {res:?}");
        assert!(
            res.is_ok(),
            "concurrent multi-run spilling bounded sort timed out — reproduces the cold-promotion deadlock"
        );
    }

    /// Characterizes what pure sort-side compaction-pool contention does — and
    /// crucially what it does NOT do. The CI cold-promotion freeze (dump-enabled
    /// run 29615122014) showed three concurrent promotions pinning the shared
    /// `GreedyMemoryPool` at ~95% and HANGING with no error. This test recreates
    /// the ratio (3 concurrent sorts, each run's working set ~42% of the pool,
    /// like CI's 12.58 GB run vs 30 GB pool) and shows that in ISOLATION the pool
    /// does NOT deadlock: the losing sort gets `ResourcesExhausted` and the others
    /// complete (observed: two finish, one errors, none hang). So `GreedyMemoryPool`
    /// errors on exhaustion — the CI HANG must involve a component that WAITS
    /// rather than errors (the parked cross-tier scan holding pool memory while the
    /// sort waits for its input — a circular wait), not sort-side contention alone.
    /// This guards that regression while documenting where the real deadlock lives.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_promotions_pool_contention_errors_does_not_deadlock() {
        use datafusion::execution::memory_pool::GreedyMemoryPool;
        use datafusion::execution::runtime_env::RuntimeEnvBuilder;
        use datafusion::prelude::SessionConfig;
        use std::time::Duration;

        let schema = id_schema();
        let batch_rows = 8192i64;
        let batch_bytes = id_batch(&schema, 0, batch_rows).get_array_memory_size();

        // Shared pool mirroring the compaction pool. run_size ~42% of it, so one
        // promotion's run fits but three concurrent cannot coexist.
        let pool_bytes = 32 * 1024 * 1024usize;
        let run_size = (pool_bytes * 42) / 100; // ~13.4 MiB, like CI's 12.58GB/30GB
        // Each stream feeds several runs' worth so the sorts spill and stay
        // resident long enough to contend (≈4 runs/stream).
        let n_batches = (run_size * 4) / batch_bytes.max(1);

        let rtenv = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(pool_bytes)))
            .build_arc()
            .expect("build runtime env");
        let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), rtenv).task_ctx();

        let async_input = |schema: SchemaRef, n: usize| -> DFStream {
            let batches: Vec<datafusion_common::Result<RecordBatch>> = (0..n)
                .map(|i| {
                    #[expect(clippy::cast_possible_wrap, reason = "test indices are tiny")]
                    let start = i as i64 * batch_rows;
                    Ok(id_batch(&schema, start, batch_rows))
                })
                .collect();
            Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&schema),
                stream::iter(batches).then(|b| async move {
                    tokio::task::yield_now().await;
                    b
                }),
            ))
        };

        async fn drain(mut s: DFStream) -> (usize, bool) {
            let (mut rows, mut errored) = (0usize, false);
            while let Some(item) = s.next().await {
                match item {
                    Ok(b) => rows += b.num_rows(),
                    Err(_) => {
                        errored = true;
                        break;
                    }
                }
            }
            (rows, errored)
        }

        let mk = |name: &'static str| {
            bounded_sort_stream(
                name,
                async_input(Arc::clone(&schema), n_batches),
                vec!["id".to_string()],
                &ctx,
                run_size,
                None,
                None,
            )
        };
        let (s1, s2, s3) = (mk("customer"), mk("order_line"), mk("stock"));

        let res = tokio::time::timeout(Duration::from_secs(40), async {
            tokio::join!(drain(s1), drain(s2), drain(s3))
        })
        .await;
        eprintln!(
            "3-concurrent pool-contention: pool={pool_bytes} run_size={run_size} n_batches={n_batches} result={res:?}"
        );
        // Key finding: sort-side pool contention does NOT hang — it errors. So
        // this must NOT time out (a timeout would mean the pool itself deadlocks,
        // which it does not; the CI hang comes from the waiting scan, elsewhere).
        assert!(
            res.is_ok(),
            "GreedyMemoryPool contention unexpectedly hung; it should surface ResourcesExhausted, not deadlock"
        );
    }

    /// Reproduction attempt for the DEFINITIVE freeze signature (frozen instrumented
    /// CI run 29627694719): with every vortex read completing, the scan `DFStream`
    /// returns `Pending` forever at a `bounded_sort` run≥1 boundary, starving the
    /// run's `SortExec`. Leading suspect: `RepartitionExec` backpressure across the
    /// inter-run polling pause (run N's `SortExec` sorts/emits while run N+1 isn't
    /// pulling, so the scan plan's bounded channels fill and its producer tasks
    /// block). This drives `bounded_sort` over a scan-shaped plan (memory source →
    /// RoundRobin `RepartitionExec` → `CoalescePartitionsExec`) with a small run
    /// size so there are MANY run boundaries. A hang (timeout) reproduces the freeze.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn bounded_sort_over_repartitioned_scan_no_run_boundary_deadlock() {
        use std::time::Duration;

        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::datasource::source::DataSourceExec;
        use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
        use datafusion::physical_plan::repartition::RepartitionExec;
        use datafusion::physical_plan::{ExecutionPlan, execute_stream};

        let schema = id_schema();
        let batch_rows = 8192i64;
        let n_batches = 400usize;
        let batch_bytes = id_batch(&schema, 0, batch_rows).get_array_memory_size();
        let batches: Vec<RecordBatch> = (0..n_batches)
            .map(|i| {
                #[expect(clippy::cast_possible_wrap, reason = "test indices are tiny")]
                let start = i as i64 * batch_rows;
                id_batch(&schema, start, batch_rows)
            })
            .collect();

        let ctx = test_task_ctx();
        // Scan-shaped plan: memory source → RoundRobin repartition → coalesce to 1.
        let src: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[batches], Arc::clone(&schema), None)
                .expect("memory source"),
        )));
        let repart: Arc<dyn ExecutionPlan> = Arc::new(
            RepartitionExec::try_new(src, Partitioning::RoundRobinBatch(4)).expect("repartition"),
        );
        let coalesced: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(repart));
        let stream = execute_stream(coalesced, Arc::clone(&ctx)).expect("execute_stream");

        // Small run size ⇒ many runs ⇒ many inter-run polling pauses of the scan stream.
        let run_size = batch_bytes.saturating_mul(4);
        let sorted = bounded_sort_stream(
            "t",
            stream,
            vec!["id".to_string()],
            &ctx,
            run_size,
            None,
            None,
        );

        let res = tokio::time::timeout(Duration::from_secs(30), drain_ids(sorted)).await;
        eprintln!(
            "repartitioned-scan repro: n_batches={n_batches} run_size={run_size} ok={}",
            res.is_ok()
        );
        assert!(
            res.is_ok(),
            "bounded_sort over a repartitioned scan hung at a run boundary — reproduces the scan-DFStream Pending-at-run-boundary freeze"
        );
        let out = res.expect("no timeout");
        assert_eq!(
            out.len(),
            n_batches * batch_rows as usize,
            "row count preserved across runs"
        );
    }
}
