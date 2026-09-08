# Refresh transform pipelining

**Status:** Proposal
**Scope:** Accelerated-table refresh path (`crates/runtime/src/accelerated_table`), the embedding transform (`crates/runtime-search`), and the FTS/vector index transform (`crates/runtime-datafusion-index`).

This document proposes overlapping the stages of a dataset refresh — source fetch, embedding, FTS/vector indexing, and the accelerator write — which today run as a strictly serial, pull-one-batch → fully-transform → write-one-batch loop. It is one lever in a larger startup/refresh-latency effort; §5 covers how it composes with the others.

## 1. Problem statement

A refresh materializes a `SendableRecordBatchStream` into the accelerator sink. The stream is **pull-driven and linear**: the sink asks for the next batch, which forces the batch fully through every transform before another batch is pulled.

The consuming loop is `write_streaming_data_update` (`crates/runtime/src/accelerated_table/refresh_task.rs:920`). It wraps the data stream in a `RecordBatchStreamAdapter` built from `stream::unfold` (`refresh_task.rs:943`) whose body does a single `stream.next().await` per step (`refresh_task.rs:962`), then hands the wrapped stream to `sink.insert_into(record_batch_stream, overwrite)` (`refresh_task.rs:1025`). `insert_into` pulls one batch at a time; each pull propagates down the operator chain.

The chain that a pull traverses (for an embeddings + FTS dataset) is:

```
source scan  →  EmbeddingTableExec  →  [CoalescePartitionsExec]  →  IndexerExec  →  sink.insert_into
 (io_runtime)     (embed per batch)      (single partition)         (FTS commit)     (accelerator write)
```

Each stage blocks the next:

- **Embedding stage.** `EmbeddingTableExec::execute` (`crates/runtime-search/src/embeddings/execution_plan.rs:117`) wraps its input in `to_sendable_stream` (`:173`), which — inside a `stream!` macro (`:179`, itself flagged for removal by `CLAUDE.md`) — does `base_stream.next().await` then `compute_additional_embedding_columns(&batch, …).await` (`:183`) **before yielding**. Within a batch, columns embed serially: `for (col, cfg) in embedded_columns` (`:271`). So the source's next batch is not fetched while the current batch is embedding.
- **Index stage.** `IndexerExec::execute` (`crates/runtime-datafusion-index/src/analyzer/index_table_scan.rs:472`) uses `input.and_then(|batch| async move { for idx in &indexes { idx.compute_index(vec![b]).await } })` (`:483`, `:511`). `Stream::and_then` polls each item's future to completion before pulling the next item — so batch N+1 is not fetched/embedded while batch N is being indexed. `IndexerExec` also requires `Distribution::SinglePartition` (`:410`) and `maintains_input_order = true` (`:414`), so the whole tail is one ordered partition.
- **Write stage.** The sink write for batch N (`refresh_task.rs:1025`, under `accelerator_write_mutex` at `:1024`) completes before the `unfold` loop pulls batch N+1.

The single stream is produced by `get_data` → `df.execute_stream()` (`crates/runtime/src/dataconnector/mod.rs:797`), executed on the CPU/refresh runtime via `run_record_batch_stream_on_runtime` (`refresh_task.rs:1639`) with source I/O dispatched to `io_runtime` (`refresh_task.rs:1650`). **Note:** running stages on different runtimes does *not* by itself create overlap — the pull dependency serializes them regardless of which pool each future is polled on.

**Cost model.** Per batch, wall-clock today is `t_fetch + t_embed + t_index + t_write` (sum). With a pipelining boundary that lets stage N+1's batch advance while stage N's batch is still in flight, steady-state per-batch wall-clock approaches `max(t_fetch, t_embed, t_index, t_write)`. For an embeddings dataset backed by a remote model, `t_embed` (network round-trip) dominates and the source scan + accelerator write are pure idle time today; for GitHub, `t_fetch` (paginated API latency) dominates and the write is idle. The pipelined bound is the largest single stage, not their sum.

## 2. Current architecture (how the stream is assembled)

1. **Provider wrapping** (`crates/runtime/src/init/dataset.rs:1273-1297`): `EmbeddingConnector` wraps the base connector when `ds.has_embeddings()`, then `FullTextConnector` wraps that when `ds.has_full_text_column()`. Resulting `TableProvider` stack is `IndexedTableProvider(EmbeddingTable(base))`.
2. **Scan → physical plan.** `IndexedTableProvider::scan` builds `IndexerExec` over `EmbeddingTable::scan`'s plan (`EmbeddingTableExec` over the base scan). Because `IndexerExec` requires `SinglePartition` (`index_table_scan.rs:410`), DataFusion inserts a `CoalescePartitionsExec` beneath it if the base scan is multi-partition. `EmbeddingTableExec` inherits the base partitioning unchanged (`execution_plan.rs:161`).
3. **Execution.** `get_data` (`dataconnector/mod.rs:741`) builds a `DataFrame` (projection scan, or refresh SQL with `include_computed_columns`), applies watermark/window filters (`:781`), and calls `df.execute_stream()` (`:797`). The returned stream's single output partition is what the sink drains.
4. **Drive + write.** `write_streaming_data_update` (`refresh_task.rs:920`) drains it batch-by-batch and writes into the sink (`:1025`), accumulating `RefreshStat` (rows/bytes) as it goes (`:966-967`).

## 3. Proposed design

Introduce a **bounded, order-preserving prefetch boundary** so an upstream stage can run ahead of a downstream consumer, plus convert the embedding stage's internal loop to **ordered concurrency**. Two complementary mechanisms, both order-preserving:

### 3a. `PrefetchExec` — a reusable order-preserving prefetch operator

A thin `ExecutionPlan` wrapper that spawns its input onto the current (CPU/refresh) runtime, pushing batches into a **bounded** `tokio::sync::mpsc` channel of capacity `k`; the output stream drains the channel. This decouples the producer's polling cadence from the consumer's: while the sink writes batch N, the producer is already fetching/embedding/indexing batches N+1…N+k. FIFO channel ⇒ **strict order preserved** (required for FTS, §4a). Back-pressure is automatic — the producer blocks on `send` when the channel is full, bounding memory.

Placement (most valuable first):

- **Between the transform tail and the sink** — wrap the stream returned by `get_data` before `write_streaming_data_update` drains it (or insert `PrefetchExec` as the plan root in `IndexedTableProvider`/`EmbeddingTable::scan`). Overlaps *(fetch+embed+index)* with *(accelerator write)*. Single insertion, benefits every accelerated dataset.
- **Below `IndexerExec`** (between embed and index). Overlaps *(fetch+embed)* with *(index)*. Must remain `SinglePartition` + order-preserving, which `PrefetchExec` is, so it satisfies `IndexerExec`'s `required_input_distribution`.

Sketch (manual `Stream`, bounded channel, no `stream!` macro, no lock held across `.await`):

```rust
/// Order-preserving prefetch: runs `input` ahead of the consumer by up to
/// `depth` batches via a bounded channel. FIFO ⇒ output order == input order.
pub struct PrefetchStream {
    rx: tokio::sync::mpsc::Receiver<DataFusionResult<RecordBatch>>,
    schema: SchemaRef,
    _drive: AbortOnDropHandle, // aborts the producer if the consumer is dropped
}

impl PrefetchStream {
    fn new(mut input: SendableRecordBatchStream, depth: usize, handle: &Handle) -> Self {
        let schema = input.schema();
        // Bounded: producer blocks when `depth` batches are buffered (back-pressure).
        // `depth` is always ≥ 1 here — depth 0 means "disabled", and the planner
        // omits `PrefetchExec` entirely rather than constructing one (§6).
        debug_assert!(depth >= 1, "PrefetchExec must not be built for depth 0");
        let (tx, rx) = tokio::sync::mpsc::channel(depth);
        let drive = handle.spawn(async move {
            while let Some(item) = input.next().await {
                // `is_err()` ⇒ consumer gone; stop producing.
                if tx.send(item).await.is_err() {
                    break;
                }
            }
        });
        Self { rx, schema, _drive: AbortOnDropHandle::new(drive) }
    }
}

impl Stream for PrefetchStream {
    type Item = DataFusionResult<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}
impl RecordBatchStream for PrefetchStream {
    fn schema(&self) -> SchemaRef { Arc::clone(&self.schema) }
}
```

`handle` must be the CPU/refresh runtime handle (`self.cpu_runtime`, `refresh_task.rs:1622`), never the main runtime — per the separate-runtimes rule in `CLAUDE.md`. `AbortOnDropHandle` guarantees the producer task cannot outlive a cancelled/failed refresh.

### 3b. Ordered concurrency inside the embedding stage

Rewrite `to_sendable_stream` (`execution_plan.rs:173`) to pull up to `k` source batches ahead and embed them concurrently while **preserving order**, replacing the `stream!` macro with `futures::StreamExt::buffered`:

```rust
fn to_sendable_stream(base_stream: SendableRecordBatchStream, /* … */) -> impl Stream<Item = DataFusionResult<RecordBatch>> {
    base_stream
        .map(move |batch_result| {
            let (schema, cols, models) = (/* clones */);
            async move {
                let batch = batch_result?;
                let embeddings = compute_additional_embedding_columns(&batch, &cols, models)
                    .await
                    .map_err(|e| DataFusionError::External(e.to_string().into()))?;
                construct_record_batch(&batch, &schema, &embeddings)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
            }
        })
        .buffered(k) // ordered: yields in input order, polls up to k futures concurrently
}
```

`buffered` (not `buffer_unordered`) keeps emission order, so the stage is safe to feed `IndexerExec`. Independently, the per-column loop (`:271`) can join across columns (`try_join_all`) since columns are row-independent — this is a separate change but shares the same file.

**Why two mechanisms.** `buffered` overlaps *futures the operator owns* (embedding round-trips) — ideal for the embed stage where the concurrency is API round-trips. `PrefetchExec` overlaps *across operator boundaries* (a producer subtree vs. the sink) — needed because the sink is not a future the plan owns. Together they overlap fetch↔embed↔index↔write.

## 4. Ordering & correctness constraints

Data correctness is the top priority (`CLAUDE.md`). Every mechanism above is **order-preserving**; the constraints below are why.

### 4a. FTS/vector indexing is order-sensitive — never reorder

`IndexerExec` declares `maintains_input_order = true` (`index_table_scan.rs:414`) and the FTS `update_index` is a per-batch **upsert** (delete-by-PK then insert+commit — see `crates/search/src/generation/text_search/index.rs:226`). If two batches in one refresh carry the same primary key, the *last* write wins; reordering them would change which row survives — a silent correctness bug. Therefore:

- Use `buffered`/FIFO-channel prefetch, **never** `buffer_unordered`, anywhere upstream of `IndexerExec`.
- `PrefetchExec` must report `maintains_input_order = vec![true]` and `SinglePartition`-compatible output so the optimizer does not treat it as a reorder point.

Embeddings alone are row-independent (each row embeds to its own vector), so the embed stage *could* tolerate reordering — but because it usually sits upstream of FTS, we still keep it ordered.

### 4b. Append-mode watermark is order-independent (safe)

Append refresh computes the post-refresh max timestamp and lag (`refresh_task.rs:901-916`) as a running `max` over batches. `max` is commutative/associative, so batch order does not affect the watermark — reordering would be safe *for the watermark*. (It is still forbidden by §4a when FTS is present.) The per-batch stat accumulation (`refresh_task.rs:966-967`, rows/bytes sums) is likewise order-independent.

### 4c. Atomicity & failure

Streaming inserts already write batches incrementally; a mid-stream error can leave partial data unless the accelerator's `insert_into` is transactional — this is true today and prefetch does not change it. Prefetch **improves** failure containment in one way: `AbortOnDropHandle` ensures the producer subtree is torn down when the refresh future is cancelled (shutdown `CancellationToken`) or the consumer errors, so no orphaned fetch/embed work continues. Errors propagate in-order through the channel (they are `Result` items), so the first error still surfaces before any later batch.

### 4d. Memory bounds

Buffered depth `k` costs up to `k × (batch_size + embedded_columns × vector_bytes)`. Embedded batches are large — a `FixedSizeList<F32, 1536>` adds ~6 KiB/row, ~48 MiB per 8192-row batch per column. So depth must be **small (1–4)** and the channel **bounded** (never `try_collect`, never an unbounded queue — forbidden by `CLAUDE.md`). The existing per-batch `resource_monitor.check_memory_usage` (`refresh_task.rs:978`) continues to fire on the consumer side; the bounded channel is the primary guard.

## 5. Interaction with the other refresh-latency levers

- **FTS commit-once-per-refresh (`on_write_complete`).** Moving tantivy `commit()`/`reader.reload()` out of the per-batch `update_index` (`index.rs:226`) into the `on_write_complete` hook (`crates/runtime-datafusion-index/src/lib.rs:77`) turns `compute_index` into cheap `add_document` calls. **This lever is now implemented for FTS**: `FullTextDatabaseIndex` opens a deferred-commit window on `on_write_start`, stages documents during the scan, and commits once on `on_write_complete` (rolling back on `on_write_failed`); the CDC path, which never invokes the hooks, still commits per change. That makes `t_index` small and steady, so prefetch below `IndexerExec` cleanly overlaps a fast index-append with the sink write. The two changes are complementary and independently landable.
- **Embedding cross-column / cross-batch concurrency.** §3b's `buffered(k)` is the cross-batch half; joining the per-column loop (`execution_plan.rs:271`) is the cross-column half. Both raise the offered concurrency into the embedding model.
- **Remote embedding concurrency.** Pipelining is a *multiplier* on model-side concurrency: `buffered(k)` only helps if the model can service `k` batches' worth of in-flight sub-requests. The remote `RateController` default (`max_concurrent_requests = 4`, `crates/llms/src/openai/mod.rs`) is the current ceiling; raising/exposing it and setting `k` together is what actually shrinks `t_embed`.
- **GitHub incremental append.** Pipelining does not reduce the *volume* GitHub fetches — that is the incremental-append lever (server-side time filter + `orderBy` early-exit). But it overlaps the serial cursor-walk latency with the accelerator write, and the two compose: incremental cuts pages fetched, prefetch overlaps the pages that remain.

## 6. Config surface

One knob, following `CLAUDE.md` (no booleans in user-facing config; conservative default):

- `runtime.acceleration.refresh_prefetch_depth: Option<usize>` — batches a stage may run ahead. `None`/unset ⇒ default `1` (one batch ahead: overlaps adjacent stages at ~2× buffered memory, the safe default). `0` ⇒ disabled (today's behavior, an escape hatch) — at depth 0 no `PrefetchExec` is inserted at all, rather than one with a depth-1 channel. Larger values trade memory for deeper overlap. Mirrors the existing `Option<usize>` precedent of `runtime.dataset_load_parallelism` (`crates/spicepod/src/component/runtime.rs:43`).

Alternative if named modes are preferred over a raw depth: an enum `refresh_pipelining: serial | pipelined` (default `serial` during rollout, then `pipelined`), with depth derived from available memory — keeps the door open to auto-tuning without a boolean. Recommend shipping the `usize` depth first (simpler, directly testable) and layering the enum later only if auto-tuning lands.

The embedding `buffered(k)` depth should be driven by the same knob (or a fixed small constant initially) so operators reason about one number.

## 7. Risks, alternatives, rollout

**Risks.** (1) Memory growth if `k` is set high on wide/embedded datasets — mitigated by the bounded channel + small default + resource monitor (§4d). (2) A subtle reorder if `PrefetchExec` is mis-declared to the optimizer — mitigated by `maintains_input_order = true` and an integration test asserting FTS upsert last-writer-wins under prefetch (§4a). (3) Producer task lifecycle — mitigated by `AbortOnDropHandle` and honoring the shutdown `CancellationToken`.

**Alternatives considered.**
- *Multi-partition indexing* (drop `SinglePartition`, shard FTS writers) — larger change, breaks the limit-pushdown rationale (`index_table_scan.rs:407`), and requires sharded tantivy writers. Out of scope; prefetch is orthogonal and lands first.
- *`buffer_unordered`* — rejected: reorders, unsafe upstream of FTS (§4a).
- *Rely on DataFusion partitioning for parallelism* — the tail is forced to `SinglePartition` by `IndexerExec`, so partition parallelism cannot help the index/write stages; an explicit prefetch boundary is required.

**Measurement.**
- The startup sampler already logs per-dataset load progress every 30s until all datasets settle (`crates/runtime/src/init/dataset.rs:214-265`) and the aggregate dispatch summary (`:198-212`) — use these plus `REFRESH_ROWS_WRITTEN`/`REFRESH_BYTES_WRITTEN` (`refresh_task.rs:973-975`) and `REFRESH_LAG_MS` (`:908`) as the primary time-to-materialized signals.
- `testoperator` bench with an embeddings and an FTS spicepod (naming per `CLAUDE.md`: `{connector}-{accelerator}-{variant}`), comparing `refresh_prefetch_depth` 0 vs 1 vs 4, asserting equal row counts and identical FTS/vector results (correctness gate) alongside wall-clock.
- A GitHub dataset bench measuring time-to-ready with prefetch on/off.

**Rollout.** Land `PrefetchExec` + the `buffered` embed rewrite behind `refresh_prefetch_depth` defaulting to `0` (disabled). Validate correctness (FTS ordering, watermark, row counts) and memory on the benches above. Flip the default to `1` once green. Consider `2–4` as a later default only after the memory profile on wide embedded datasets is characterized. The FTS commit-once change (§5) should land first or together, since it is what makes deeper prefetch worthwhile.
