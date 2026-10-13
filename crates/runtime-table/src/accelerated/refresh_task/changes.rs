/*
Copyright 2024-2025 The Spice.ai OSS Authors

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
mod ingress;
mod policy;

use super::DatasetMetricLabels;
use super::RefreshTask;
use crate::accelerated::refresh::Refresh;
use crate::accelerated::refresh_completion::RefreshCompletion;
use arrow::array::RecordBatch;
#[cfg(test)]
use arrow::array::{Array, Int32Array, Int64Array, StringArray};
#[cfg(test)]
use arrow::datatypes::DataType;
use arrow::datatypes::{Field, Schema, SchemaRef};
#[cfg(test)]
use arrow_tools::record_batch::try_cast_to;
#[cfg(test)]
use arrow_tools::schema_evolution::{self, EvolutionContext, SchemaEvolution};
use cache::Caching;
use data_components::cdc::{self, ChangesStream};
#[cfg(test)]
use data_components::cdc::{ChangeBatch, ChangeOperation};
#[cfg(any(feature = "debezium", feature = "kafka"))]
use data_components::kafka::{
    Error as KafkaError, rdkafka::error::KafkaError as RdKafkaError,
    rdkafka::types::RDKafkaErrorCode,
};
use datafusion::common::TableReference;
#[cfg(test)]
use datafusion::error::DataFusionError;
#[cfg(test)]
use datafusion::execution::{SessionState, context::SessionContext};
#[cfg(test)]
use datafusion::logical_expr::Expr;
#[cfg(test)]
use datafusion::logical_expr::lit;
#[cfg(test)]
use datafusion::physical_plan::collect;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
#[cfg(test)]
use futures::StreamExt;
use futures::stream;
#[cfg(all(test, not(windows)))]
use runtime_acceleration::change_sink::provider::partitioned_widening_refusal;
#[cfg(test)]
use runtime_acceleration::change_sink::provider::{
    cdc::{ChangeOperationType, contiguous_row_span, encode_primary_key, group_into_sub_batches},
    delete_matching_rows_from_arrow_provider,
    deletion::build_pk_only_batch_from_change_batch,
};
#[cfg(test)]
use runtime_acceleration::change_sink::source_policy::{CdcPolicy, SchemaDecision};
#[cfg(test)]
use runtime_acceleration::change_sink::{
    ChangeBatch as LogicalChangeBatch, Publication, Recovery, SchemaEvolutionSupport, WriteOptions,
};
use runtime_acceleration::change_sink::{ChangeSink, DurabilityObserver, StorageDurability};
use runtime_acceleration::dataupdate::{StreamingDataUpdate, UpdateType};
use runtime_component::dataset::OnSchemaChange;
use runtime_component::dataset::acceleration::RefreshMode;
#[cfg(test)]
use runtime_component::schema_evolution::{evolution_allowed, widening_plan_kind};
#[cfg(test)]
use runtime_datafusion::error::find_datafusion_root;
use runtime_datafusion::error::format_datafusion_error;
use runtime_metrics::acceleration as metrics;
use runtime_status as status;
#[cfg(all(test, not(windows)))]
use runtime_table_partition::provider::PartitionTableProvider;
#[cfg(test)]
use snafu::OptionExt;
#[cfg(test)]
use snafu::ResultExt;
#[cfg(test)]
use spice_table::SpiceTable;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant, SystemTime};
use telemetry::timing::MultiTimeMeasurement;
use tokio::sync::RwLock;

#[cfg(test)]
type PendingApplyFinalize = Publication;

#[cfg(test)]
struct PendingFinalizeCommit {
    finalize: PendingApplyFinalize,
    committers: Vec<Box<dyn cdc::CommitChange + Send + Sync>>,
    ready_after_finalize: bool,
    durability: StorageDurability,
}

/// Real source committers tagged with local storage fences. Entries are enqueued
/// only after successful publication and acknowledged only after durability.
type DeferredCommitQueue =
    Arc<tokio::sync::Mutex<VecDeque<(u64, Vec<Box<dyn cdc::CommitChange + Send + Sync>>)>>>;

type DeferredCommitDrain =
    futures::future::BoxFuture<'static, std::result::Result<(), cdc::CommitError>>;

struct SourceDurabilityObserver {
    queue: DeferredCommitQueue,
    durable_fence: AtomicU64,
    durability_known: AtomicBool,
    pending_count: Arc<AtomicUsize>,
    // Retain the in-flight source call and its durable prefix across waiter
    // cancellation. The queue lock is never held during a source network call.
    drain: tokio::sync::Mutex<Option<DeferredCommitDrain>>,
    dataset_name: TableReference,
    runtime_status: Arc<status::RuntimeStatus>,
}

impl SourceDurabilityObserver {
    fn new(dataset_name: TableReference, runtime_status: Arc<status::RuntimeStatus>) -> Self {
        Self {
            queue: Arc::new(tokio::sync::Mutex::new(VecDeque::new())),
            durable_fence: AtomicU64::new(0),
            durability_known: AtomicBool::new(false),
            pending_count: Arc::new(AtomicUsize::new(0)),
            drain: tokio::sync::Mutex::new(None),
            dataset_name,
            runtime_status,
        }
    }

    async fn enqueue(&self, fence: u64, committers: Vec<Box<dyn cdc::CommitChange + Send + Sync>>) {
        if committers.is_empty() {
            return;
        }
        {
            let mut queue = self.queue.lock().await;
            // The oldest queued fence must stay fixed so an in-progress
            // checkpoint can release it even while newer publications arrive.
            // Only a compatible singleton tail behind that head can absorb.
            let merged = if queue.len() > 1
                && let (Some((tail_fence, tail)), [incoming]) =
                    (queue.back_mut(), committers.as_slice())
                && let [retained] = tail.as_mut_slice()
                && retained.supports_deferral()
                && incoming.supports_deferral()
                && retained.as_any().is_some()
                && incoming.as_any().is_some()
                && retained.try_absorb(incoming.as_ref())
            {
                *tail_fence = (*tail_fence).max(fence);
                true
            } else {
                false
            };
            if merged {
                drop(committers);
            } else {
                self.pending_count
                    .fetch_add(committers.len(), Ordering::AcqRel);
                queue.push_back((fence, committers));
            }
        }
        // A checkpoint may finish before the publication receipt reaches the
        // source. Retry against its remembered fence, including on an idle source.
        self.retry().await;
    }

    fn pending_count(&self) -> usize {
        self.pending_count.load(Ordering::Acquire)
    }

    async fn retry(&self) {
        if self.durability_known.load(Ordering::Acquire) {
            self.on_durable(self.durable_fence.load(Ordering::Acquire))
                .await;
        }
    }

    async fn finish_drain(&self, drain: &mut Option<DeferredCommitDrain>) -> bool {
        let Some(pending) = drain.as_mut() else {
            return true;
        };
        let result = pending.await;
        *drain = None;
        if let Err(e) = result {
            if !self.runtime_status.is_shutdown() {
                tracing::warn!(
                    "Deferred CDC commit failed for {} (source slot will retry before any later immediate commit): {e}",
                    self.dataset_name
                );
            }
            return false;
        }
        true
    }

    async fn is_empty(&self, trace: Option<&CdcFlushTrace<'_>>, stage: &'static str) -> bool {
        // A detached ready prefix still counts as pending acknowledgement.
        let drain_start = Instant::now();
        let drain = self.drain.lock().await;
        if let Some(trace) = trace {
            trace.record(stage, "observer_drain_lock", drain_start);
        }
        let queue_start = Instant::now();
        // A retained requeue future can already own the next queue-lock permit.
        let empty = drain.is_none() && self.queue.lock().await.is_empty();
        if let Some(trace) = trace {
            trace.record(stage, "observer_queue_lock", queue_start);
        }
        empty
    }
}

#[async_trait::async_trait]
impl DurabilityObserver for SourceDurabilityObserver {
    async fn on_durable(&self, fence: u64) {
        self.durable_fence.fetch_max(fence, Ordering::AcqRel);
        self.durability_known.store(true, Ordering::Release);
        let mut drain = self.drain.lock().await;
        if !self.finish_drain(&mut drain).await {
            return;
        }
        let durable_epoch = self.durable_fence.load(Ordering::Acquire);
        // Pull out every committer whose epoch is now durable, preserving FIFO
        // order. Hold the lock only to splice out the ready prefix, not across
        // the (network) commits.
        let ready: VecDeque<(u64, Vec<Box<dyn cdc::CommitChange + Send + Sync>>)> = {
            let mut queue = self.queue.lock().await;
            let mut ready = VecDeque::new();
            while let Some((epoch, _)) = queue.front() {
                if *epoch <= durable_epoch {
                    let ready_item = queue.pop_front().unwrap_or_else(|| unreachable!());
                    ready.push_back(ready_item);
                } else {
                    break;
                }
            }
            ready
        };

        // Every committer in `ready` is at or below the durable fence, so folding the
        // whole prefix to a single max-LSN commit and acking once is equivalent
        // to acking each epoch in turn — O(epochs) work becomes one `fetch_max`.
        // A dataset's deferred queue holds a single committer type, so this is
        // all-or-nothing: only when *every* committer is coalescable do we
        // collapse to a single entry tagged with the highest folded epoch.
        // Order-sensitive or fallible sources are left with their per-epoch
        // structure completely untouched, preserving the in-order,
        // requeue-on-failure drain byte for byte.
        let ready = if prefix_is_coalescable(&ready) {
            // `prefix_is_coalescable` guaranteed a non-empty prefix, so `max` is
            // always `Some` here; `unwrap_or(0)` is just the lint-clean spelling
            // of that (this crate denies `unwrap`/`expect` in non-test code). The
            // fold only ever reduces a non-empty input, so `folded` is non-empty.
            let max_epoch = ready.iter().map(|(epoch, _)| *epoch).max().unwrap_or(0);
            let count = ready
                .iter()
                .map(|(_, committers)| committers.len())
                .sum::<usize>();
            let folded = fold_committers(ready.into_iter().flat_map(|(_, cs)| cs).collect());
            self.pending_count
                .fetch_sub(count - folded.len(), Ordering::AcqRel);
            VecDeque::from([(max_epoch, folded)])
        } else {
            ready
        };

        if !ready.is_empty() {
            *drain = Some(Box::pin(commit_deferred_prefix(
                Arc::clone(&self.queue),
                Arc::clone(&self.pending_count),
                ready,
            )));
            self.finish_drain(&mut drain).await;
        }
    }
}

async fn commit_deferred_prefix(
    queue: DeferredCommitQueue,
    pending_count: Arc<AtomicUsize>,
    mut ready: VecDeque<(u64, Vec<Box<dyn cdc::CommitChange + Send + Sync>>)>,
) -> std::result::Result<(), cdc::CommitError> {
    while let Some((epoch, committers)) = ready.pop_front() {
        let mut committers = committers.into_iter();
        while let Some(committer) = committers.next() {
            if let Err(error) = committer.commit().await {
                let mut uncommitted = vec![committer];
                uncommitted.extend(committers);
                let mut to_requeue = VecDeque::new();
                to_requeue.push_back((epoch, uncommitted));
                to_requeue.append(&mut ready);

                // Requeue before reporting the failure so no later source
                // acknowledgement can skip the failed durable prefix.
                let mut queue = queue.lock().await;
                while let Some(item) = to_requeue.pop_back() {
                    queue.push_front(item);
                }
                return Err(error);
            }
            pending_count.fetch_sub(1, Ordering::AcqRel);
        }
    }
    Ok(())
}

/// Whether the whole deferred-drain prefix opts into coalescing — i.e. every
/// committer is coalesce-identifiable (`as_any` is `Some`, which only the
/// infallible, order-insensitive committers override). Empty prefix -> `false`.
/// A dataset's queue holds a single committer type, so in practice this is
/// all-or-nothing; requiring *all* of them (not just the first) is a cheap guard
/// that keeps a hypothetical mixed queue on the safe per-epoch, in-order,
/// requeue-on-failure drain rather than wrongly collapsing epochs.
fn prefix_is_coalescable(
    ready: &VecDeque<(u64, Vec<Box<dyn cdc::CommitChange + Send + Sync>>)>,
) -> bool {
    ready.iter().any(|(_, committers)| !committers.is_empty())
        && ready
            .iter()
            .flat_map(|(_, committers)| committers.iter())
            .all(|committer| committer.as_any().is_some())
}

/// Coalesce a run of consecutive committers via [`cdc::CommitChange::try_absorb`]:
/// each is folded into the previous retained committer where the source permits
/// (a shared-slot member folds to its max LSN), collapsing an N-envelope burst
/// to as few as one commit — which turns the ordered background commit chain
/// into a single `fetch_max` for that source. Anything that refuses to fold
/// (the default for order-sensitive sources) is retained in order, so those
/// connectors are byte-identical.
fn fold_committers(
    committers: Vec<Box<dyn cdc::CommitChange + Send + Sync>>,
) -> Vec<Box<dyn cdc::CommitChange + Send + Sync>> {
    // Fast path — nothing can fold: a lone committer, or a run whose committers
    // don't opt into coalescing (`as_any` is `None`, the default). Return the
    // input untouched so the common no-coalesce burst allocates nothing, keeping
    // the pre-existing apply cost.
    if committers.len() <= 1 || committers.first().is_none_or(|c| c.as_any().is_none()) {
        return committers;
    }
    let mut folded: Vec<Box<dyn cdc::CommitChange + Send + Sync>> =
        Vec::with_capacity(committers.len());
    for committer in committers {
        // The same committer type can carry different source identities.
        // A refused merge retains both commits in their original order.
        if let Some(last) = folded.last_mut()
            && last.try_absorb(committer.as_ref())
        {
            continue;
        }
        folded.push(committer);
    }
    folded
}

#[cfg(test)]
fn committers_all_support_deferral(
    committers: &[Box<dyn cdc::CommitChange + Send + Sync>],
) -> bool {
    !committers.is_empty()
        && committers
            .iter()
            .all(|committer| committer.supports_deferral())
}

/// Truncate, unknown operations, and unsupported deferred deletes require a
/// publication barrier and the target's synchronous write path.
#[cfg(test)]
fn change_batch_requires_durable_cdc_path(
    change_batch: &ChangeBatch,
    sink_absorbs_in_memory_deletes: bool,
) -> bool {
    (0..change_batch.record.num_rows()).any(|row| match change_batch.op(row) {
        ChangeOperation::Truncate | ChangeOperation::Unknown(_) => true,
        ChangeOperation::Delete => {
            !sink_absorbs_in_memory_deletes || !change_batch.has_primary_keys(row)
        }
        ChangeOperation::Create | ChangeOperation::Update | ChangeOperation::Read => false,
    })
}

struct CdcFlushTrace<'a> {
    dataset: &'a TableReference,
    id: u64,
    started: Instant,
}

impl<'a> CdcFlushTrace<'a> {
    fn new(dataset: &'a TableReference) -> Option<Self> {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        if !tracing::enabled!(target: "changesink_diagnostic", tracing::Level::DEBUG) {
            return None;
        }
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed).saturating_add(1);
        if id == 1025 {
            tracing::debug!(target: "changesink_diagnostic", limit = 1024, "CDC flush phase trace limit reached; further calls are not traced");
        }
        if id > 1024 {
            return None;
        }
        tracing::debug!(target: "changesink_diagnostic", dataset = %dataset, flush_id = id, "CDC flush phase trace started");
        Some(Self {
            dataset,
            id,
            started: Instant::now(),
        })
    }

    fn record(&self, stage: &'static str, phase: &'static str, start: Instant) {
        tracing::debug!(
            target: "changesink_diagnostic",
            dataset = %self.dataset,
            flush_id = self.id,
            stage,
            phase,
            elapsed_ms = start.elapsed().as_secs_f64() * 1000.0,
            since_start_ms = self.started.elapsed().as_secs_f64() * 1000.0,
            "CDC flush phase completed"
        );
    }
}

async fn flush_pending_source_commits(
    sink: &ChangeSink,
    observer: &SourceDurabilityObserver,
    dataset_name: &TableReference,
    runtime_status: &status::RuntimeStatus,
) -> Option<String> {
    // Bounded checkpoint retries before a still-non-empty queue is declared
    // fatal (declared before the first statement to satisfy pedantic
    // `items_after_statements`).
    const MAX_CHECKPOINT_ATTEMPTS: usize = 3;

    let trace = CdcFlushTrace::new(dataset_name);
    if observer.is_empty(trace.as_ref(), "initial_check").await {
        return None;
    }

    // Retry transient source acknowledgements and storage fences before allowing
    // a later immediate commit. A nonempty queue must never be skipped.
    for attempt in 1..=MAX_CHECKPOINT_ATTEMPTS {
        let flush_start = Instant::now();
        let flush_result = sink.flush().await;
        if let Some(trace) = &trace {
            trace.record("flush", "sink_flush", flush_start);
        }
        match flush_result {
            Ok(()) => {
                let retry_start = Instant::now();
                observer.retry().await;
                if let Some(trace) = &trace {
                    trace.record("retry", "observer_retry", retry_start);
                }
                if observer.is_empty(trace.as_ref(), "post_retry_check").await {
                    return None;
                }
                if attempt < MAX_CHECKPOINT_ATTEMPTS {
                    tracing::debug!(
                        "Deferred CDC commits still queued for {dataset_name} after checkpoint attempt {attempt}/{MAX_CHECKPOINT_ATTEMPTS}; re-checkpointing to seal the straggler epoch"
                    );
                }
            }
            Err(e) => {
                let error_message = format!(
                    "Failed to flush CDC changes for {dataset_name} before advancing source commit: {e}"
                );
                if runtime_status.is_shutdown() {
                    tracing::debug!("{error_message}");
                } else {
                    tracing::error!("{error_message}");
                }
                return Some(error_message);
            }
        }
    }

    let error_message = format!(
        "Failed to checkpoint in-memory CDC tier for {dataset_name}: deferred source commits remain after {MAX_CHECKPOINT_ATTEMPTS} durable checkpoints"
    );
    tracing::error!("{error_message}");
    Some(error_message)
}

struct ApplyContext<'a> {
    refresh_sql: Option<&'a str>,
    dataset_name: &'a TableReference,
    /// The dataset's refresh configuration, needed to rebuild the accelerator
    /// through the full-refresh path when the source reports its change history
    /// is gone (see [`RefreshTask::rebuild_from_source`]).
    refresh: &'a Arc<RwLock<Refresh>>,
    /// Prebuilt per-dataset metric labels reused by hot record sites in the apply loop
    /// (see [`DatasetMetricLabels`]).
    metric_labels: &'a DatasetMetricLabels,
    caching: Option<&'a Weak<Caching>>,
    refresh_completion: Option<&'a RefreshCompletion>,
    initial_load_completed: &'a Arc<AtomicBool>,
    #[cfg(test)]
    write_ctx: &'a SessionContext,
    #[cfg(test)]
    write_session_state: &'a SessionState,
    commit_timeout: Duration,
    #[cfg(test)]
    pending_finalize: &'a mut Option<PendingFinalizeCommit>,
    pending_commit: &'a mut Option<tokio::task::JoinHandle<Result<(), String>>>,
    /// Source acknowledgement gated by the target's deferred durability fences.
    deferred_commits: Option<&'a Arc<SourceDurabilityObserver>>,
}

#[cfg(test)]
struct WriteChangeOutcome {
    result: WriteChangeResult,
    pending_finalize: Option<PendingApplyFinalize>,
    durability: StorageDurability,
}

/// Outcome of applying one coalesced same-schema group of a run.
#[cfg(test)]
enum CoalescedRunOutcome {
    /// Written (and committers handed off); continue with the next group.
    Applied,
    /// Fatal: drop unacked committers and stop the stream. Required whenever
    /// a group is discarded without acknowledging — continuing would let a
    /// later burst ack past the gap, and a source that tracks delivered
    /// envelopes (MySQL shared dump) would skip the window on reconnect.
    Stop,
}

/// Extracts the primary key value from the data, as a tuple of (String, Expr).
///
/// # Example
///
/// ```ignore
/// let data: RecordBatch = get_record_batch();
/// let key = "id";
/// let key_col = data.column(0);
/// let result = extract_primary_key!(key_col, key, data_schema, Int32Array, "Int32");
/// if let Ok((str_value, expr_value)) = result {
///    println!("Primary key value as String: {}", str_value);
///    println!("Primary key value as DataFusion expression: {}", expr_value);
/// }
/// ```
#[cfg(test)]
macro_rules! extract_primary_key {
    ($key_col:expr, $key:expr, $data_schema:expr, $array_type:ty, $data_type_str:expr, $row:expr) => {{
        let key_col = $key_col.as_any().downcast_ref::<$array_type>().context(
            crate::accelerated::PrimaryKeyArrayDataTypeMismatchSnafu {
                field_name: $key.to_string(),
                expected_data_type: $data_type_str.to_string(),
                schema: Arc::clone(&$data_schema),
            },
        )?;
        if key_col.is_null($row) {
            return crate::accelerated::PrimaryKeyNullValueSnafu {
                field_name: $key.to_string(),
            }
            .fail();
        }
        Ok((key_col.value($row).to_string(), lit(key_col.value($row))))
    }};
}

/// Tunables for the CDC source-stream → apply pipeline.
///
/// Resolved once at process start, in this priority order:
/// 1. `runtime.params.cdc_*` from the spicepod (installed via
///    [`set_cdc_config`]).
/// 2. `SPICE_CDC_*` environment variables (useful for tests and ad-hoc
///    tuning).
/// 3. Built-in defaults.
///
/// Out-of-range or unparseable values fall back to the next source with a
/// `tracing::warn!` so misconfiguration is visible rather than silent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CdcConfig {
    /// Admission depth for the table owner's data queue. Inputs retain their
    /// lazy representation until the owner prepares a bounded burst.
    pub prefetch_buffer: usize,
    /// Hard upper bound on the number of `ChangeEnvelope`s coalesced into
    /// a single accelerator write. Coalescing amortizes per-envelope
    /// plan construction over the whole burst.
    pub max_coalesced_envelopes: usize,
    /// Best-effort byte budget for a coalesced burst. A single envelope may
    /// exceed this on its own; otherwise the next envelope is carried into the
    /// next burst before we allocate a concatenated batch.
    pub max_coalesced_bytes: usize,
    /// CDC owner linger window in milliseconds. When `> 0`, the drain
    /// keeps accumulating envelopes into a single coalesced write until
    /// `max_coalesced_envelopes` / `max_coalesced_bytes` is reached, or this
    /// window elapses — whichever comes first. The window is measured from the
    /// START of the previous apply, so time spent applying the previous burst
    /// counts toward the budget.
    pub max_coalesce_age_ms: u64,
    /// Maximum time to wait for the previous source-side commit before
    /// surfacing ingestion as stalled.
    pub commit_timeout: Duration,
    /// Upper bound on the number of primary keys packed into a single durable
    /// `delete_from` execution. A Delete sub-batch of `N` keyed rows is applied
    /// as `⌈N/cap⌉` independent delete plans instead of one monolithic
    /// predicate (a `~50k`-comparison OR-tree over 16,384 keys pegged prefetch
    /// and triggered walsender timeouts). Chunking makes the cost linear and
    /// interruptible; keys across chunks are distinct, so semantics are
    /// preserved (each key deleted exactly once).
    pub delete_subbatch_max: usize,
}

const CDC_PREFETCH_BUFFER_DEFAULT: usize = 128;
// Prefetch depth is the REAL coalescing ceiling: the burst drain is a non-blocking
// `try_recv` loop (no await), so a batch can only grow as large as what is already
// buffered in this channel. With the old 1024 max the 4096 envelope cap never bound.
// Raised 1024 -> 16384 so high-throughput tables form larger bursts, amortizing the
// fixed per-batch publish cost (one EBS directory `sync_all()` per batch per table)
// over more rows. `max_coalesced_bytes` (128 MiB default) bounds the burst DRAINED
// from the channel — not what sits in it. The channel's bound is this envelope
// count, and an envelope carries a batch of any width, so the size of what is
// queued ahead of apply is bounded by nothing. `cdc_prefetch_buffer_bytes`
// estimates it, on the same decode-free scale `max_coalesced_bytes` budgets
// against; a large value means memory no budget accounts for and is worth
// investigating. The drain never waits, so low-load latency is unchanged
// (burst.len()==1).
const CDC_PREFETCH_BUFFER_MAX: usize = 16384;
const CDC_MAX_COALESCED_ENVELOPES_DEFAULT: usize = 256;
// Raised 4096 -> 16384 to match the prefetch ceiling (otherwise it would re-clip the burst).
const CDC_MAX_COALESCED_ENVELOPES_MAX: usize = 16384;
const CDC_MAX_COALESCED_BYTES_DEFAULT: usize = 128 * 1024 * 1024;
const CDC_MAX_COALESCED_BYTES_MAX: usize = 1024 * 1024 * 1024;
const CDC_MAX_COALESCE_AGE_MS_DEFAULT: u64 = 0;
const CDC_COMMIT_TIMEOUT_MS_DEFAULT: usize = 30_000;
const CDC_COMMIT_TIMEOUT_MS_MAX: usize = 3_600_000;
// A delete burst can be as large as a coalesced burst's row count, so bound the
// per-plan key count well below that. 2,048 keeps each durable `delete_from`
// cheap and interruptible while still amortizing plan construction; the MAX is
// a sanity guard on operator overrides (a larger value re-approaches the
// monolithic predicate this cap exists to avoid).
const CDC_DELETE_SUBBATCH_MAX_DEFAULT: usize = 2_048;
const CDC_DELETE_SUBBATCH_MAX_MAX: usize = 65_536;

#[derive(Debug, Default)]
struct BoundedWarningKeys {
    seen: std::collections::HashSet<String>,
    insertion_order: std::collections::VecDeque<String>,
}

impl BoundedWarningKeys {
    fn insert_new(&mut self, key: String, limit: usize) -> bool {
        if limit == 0 || self.seen.contains(&key) {
            return false;
        }

        if self.seen.len() >= limit
            && let Some(oldest_key) = self.insertion_order.pop_front()
        {
            self.seen.remove(&oldest_key);
        }

        self.insertion_order.push_back(key.clone());
        self.seen.insert(key)
    }
}

impl Default for CdcConfig {
    fn default() -> Self {
        Self {
            prefetch_buffer: CDC_PREFETCH_BUFFER_DEFAULT,
            max_coalesced_envelopes: CDC_MAX_COALESCED_ENVELOPES_DEFAULT,
            max_coalesced_bytes: CDC_MAX_COALESCED_BYTES_DEFAULT,
            max_coalesce_age_ms: CDC_MAX_COALESCE_AGE_MS_DEFAULT,
            commit_timeout: Duration::from_millis(CDC_COMMIT_TIMEOUT_MS_DEFAULT as u64),
            delete_subbatch_max: CDC_DELETE_SUBBATCH_MAX_DEFAULT,
        }
    }
}

/// Process-wide CDC tunables. Set once at runtime startup from spicepod
/// config; repeated calls with the same value are ignored quietly so tests
/// and multi-runtime processes don't emit noise. A different later value is
/// ignored with a warning because active CDC streams may already be using the
/// first config.
static CDC_CONFIG: std::sync::OnceLock<CdcConfig> = std::sync::OnceLock::new();

const SCHEMA_EVOLUTION_WARNING_KEY_LIMIT: usize = 1024;

/// Once-per-(dataset, change) gate for schema-evolution warnings so the apply
/// loop doesn't repeat the same warning on every batch of a high-rate stream.
static SCHEMA_EVOLUTION_WARNING_KEYS: std::sync::LazyLock<parking_lot::Mutex<BoundedWarningKeys>> =
    std::sync::LazyLock::new(|| parking_lot::Mutex::new(BoundedWarningKeys::default()));

pub(crate) fn schema_evolution_first_warn(key: String) -> bool {
    SCHEMA_EVOLUTION_WARNING_KEYS
        .lock()
        .insert_new(key, SCHEMA_EVOLUTION_WARNING_KEY_LIMIT)
}

/// Per-dataset CDC schema-evolution settings, installed at dataset
/// registration (the apply loop holds no handle to the dataset component).
/// An absent entry behaves as `on_schema_change: block` — today's code paths
/// verbatim.
#[derive(Debug, Clone)]
pub struct CdcSchemaEvolution {
    pub policy: OnSchemaChange,
    /// Column names referenced by the dataset's primary key / unique / index
    /// constraints — the classifier's constraint guard.
    pub constraint_columns: Vec<String>,
}

static CDC_SCHEMA_EVOLUTION: std::sync::LazyLock<
    std::sync::RwLock<HashMap<TableReference, Arc<CdcSchemaEvolution>>>,
> = std::sync::LazyLock::new(|| std::sync::RwLock::new(HashMap::new()));

/// Install the dataset's `on_schema_change` policy and constraint columns for
/// the CDC apply loop. Call at dataset registration, before the changes
/// stream starts; re-installing overwrites (hot reload / richer constraint
/// sets win by being installed last). Installing [`OnSchemaChange::Block`] is
/// equivalent to no entry.
pub fn install_cdc_schema_evolution(dataset_name: &TableReference, settings: CdcSchemaEvolution) {
    if let Ok(mut registry) = CDC_SCHEMA_EVOLUTION.write() {
        registry.insert(dataset_name.clone(), Arc::new(settings));
    }
}

/// Remove a dataset's CDC schema-evolution settings (dataset removal/reload).
pub fn remove_cdc_schema_evolution(dataset_name: &TableReference) {
    if let Ok(mut registry) = CDC_SCHEMA_EVOLUTION.write() {
        registry.remove(dataset_name);
    }
}

fn cdc_schema_evolution_for(dataset_name: &TableReference) -> Option<Arc<CdcSchemaEvolution>> {
    CDC_SCHEMA_EVOLUTION
        .read()
        .ok()
        .and_then(|registry| registry.get(dataset_name).cloned())
}

/// Fast path: the CDC data struct matches the accelerator schema by name and
/// type in order. Nullability is ignored — the CDC `data` struct is built
/// nullable-everywhere by design (DELETE old-tuples carry nulls).
/// Whether the accelerated table already stores exactly what this CDC batch carries.
///
/// A field whose types differ is re-tested under the engine's own creation-time rewrites
/// (`engine_type_rewrites`): the accelerated table holds the rewritten type, so a
/// difference the engine itself imposes is a match, not a schema change. Only the
/// differing fields are rewritten, and only one `DataType` at a time.
fn cdc_data_schema_matches(
    target: &SchemaRef,
    incoming: &SchemaRef,
    engine_type_rewrites: arrow_tools::type_rewrite::TypeRewriteRules,
) -> bool {
    target.fields().len() == incoming.fields().len()
        && target.fields().iter().zip(incoming.fields()).all(|(t, i)| {
            t.name() == i.name()
                && (t.data_type() == i.data_type()
                    || arrow_tools::type_rewrite::rewrite_data_type(
                        i.data_type(),
                        engine_type_rewrites,
                    ) == *t.data_type())
        })
}

/// Re-tighten the nullable-everywhere CDC data struct to the accelerator's
/// nullability for name-matched fields so the classifier doesn't report a
/// nullability relax on every non-nullable field; added fields stay nullable.
fn align_nullability_for_classify(target: &SchemaRef, incoming: &SchemaRef) -> Schema {
    let fields: Vec<Field> = incoming
        .fields()
        .iter()
        .map(|f| match target.field_with_name(f.name()) {
            Ok(t) => f.as_ref().clone().with_nullable(t.is_nullable()),
            Err(_) => f.as_ref().clone(),
        })
        .collect();
    Schema::new_with_metadata(fields, incoming.metadata().clone())
}

/// Install the CDC configuration resolved from spicepod
/// `runtime.params.cdc_*`. Should be called exactly once during runtime
/// startup, before any CDC stream is started. Subsequent calls are ignored.
pub fn set_cdc_config(config: CdcConfig) {
    if let Err(new_config) = CDC_CONFIG.set(config)
        && let Some(existing) = CDC_CONFIG.get()
        && *existing != new_config
    {
        tracing::warn!(
            "CDC config already initialized with {existing:?}; ignoring different config {new_config:?}"
        );
    }
}

/// Returns the active CDC tunables, computing them on first access from
/// (in order) the spicepod-installed config, env-var overrides, then
/// built-in defaults.
pub(crate) fn cdc_config() -> CdcConfig {
    if let Some(cfg) = CDC_CONFIG.get() {
        return *cfg;
    }
    CdcConfig {
        prefetch_buffer: parse_env_usize(
            "SPICE_CDC_PREFETCH_BUFFER",
            CDC_PREFETCH_BUFFER_DEFAULT,
            CDC_PREFETCH_BUFFER_MAX,
        ),
        max_coalesced_envelopes: parse_env_usize(
            "SPICE_CDC_MAX_COALESCED_ENVELOPES",
            CDC_MAX_COALESCED_ENVELOPES_DEFAULT,
            CDC_MAX_COALESCED_ENVELOPES_MAX,
        ),
        max_coalesced_bytes: parse_env_usize(
            "SPICE_CDC_MAX_COALESCED_BYTES",
            CDC_MAX_COALESCED_BYTES_DEFAULT,
            CDC_MAX_COALESCED_BYTES_MAX,
        ),
        max_coalesce_age_ms: parse_env_u64(
            "SPICE_CDC_MAX_COALESCE_AGE_MS",
            CDC_MAX_COALESCE_AGE_MS_DEFAULT,
        ),
        commit_timeout: Duration::from_millis(parse_env_usize(
            "SPICE_CDC_COMMIT_TIMEOUT_MS",
            CDC_COMMIT_TIMEOUT_MS_DEFAULT,
            CDC_COMMIT_TIMEOUT_MS_MAX,
        ) as u64),
        delete_subbatch_max: parse_env_usize(
            "SPICE_CDC_DELETE_SUBBATCH_MAX",
            CDC_DELETE_SUBBATCH_MAX_DEFAULT,
            CDC_DELETE_SUBBATCH_MAX_MAX,
        ),
    }
}

/// Resolve a single CDC tunable from `runtime.params`, falling back to the
/// matching env var and then `default` when the param is missing,
/// unparseable, or out of range.
fn resolve_cdc_param<S: std::hash::BuildHasher>(
    params: &std::collections::HashMap<String, String, S>,
    key: &'static str,
    env_var: &'static str,
    default: usize,
    max: usize,
) -> usize {
    if let Some(raw) = params.get(key) {
        match raw.trim().parse::<usize>() {
            Ok(n) if (1..=max).contains(&n) => return n,
            Ok(n) => {
                tracing::warn!(
                    "runtime.params.{key}={n} is out of range [1, {max}]; falling back to {env_var}/default"
                );
            }
            Err(e) => {
                tracing::warn!(
                    "runtime.params.{key}={raw:?} is not a valid usize ({e}); falling back to {env_var}/default"
                );
            }
        }
    }
    parse_env_usize(env_var, default, max)
}

/// Resolve a millisecond CDC tunable from `runtime.params`, falling back to the
/// matching env var and then `default` when the param is missing or unparseable.
fn resolve_cdc_param_u64<S: std::hash::BuildHasher>(
    params: &std::collections::HashMap<String, String, S>,
    key: &'static str,
    env_var: &'static str,
    default: u64,
) -> u64 {
    if let Some(raw) = params.get(key) {
        match raw.trim().parse::<u64>() {
            Ok(n) => return n,
            Err(e) => {
                tracing::warn!(
                    "runtime.params.{key}={raw:?} is not a valid u64 ({e}); falling back to {env_var}/default"
                );
            }
        }
    }
    parse_env_u64(env_var, default)
}

/// Every `cdc_*` key [`cdc_config_from_params`] reads from `runtime.params`.
/// Exposed as the authoritative list for this family; the startup unknown-param
/// check merges it into the full `runtime.params` vocabulary
/// (`known_runtime_params`) used to recognize keys and scope "did you mean"
/// suggestions across the whole section. Keep in sync with the keys read in
/// [`cdc_config_from_params`].
pub const CDC_RUNTIME_PARAMS: &[&str] = &[
    "cdc_prefetch_buffer",
    "cdc_max_coalesced_envelopes",
    "cdc_max_coalesced_bytes",
    "cdc_max_coalesce_age_ms",
    "cdc_commit_timeout_ms",
    "cdc_delete_subbatch_max",
];

/// Build a [`CdcConfig`] from the spicepod `runtime.params` map, reading
/// the `cdc_prefetch_buffer`, `cdc_max_coalesced_envelopes`,
/// `cdc_max_coalesced_bytes`, `cdc_max_coalesce_age_ms`, and
/// `cdc_commit_timeout_ms` keys.
/// Missing/unparseable/out-of-range params fall back to the corresponding
/// `SPICE_CDC_*` env var, then defaults.
#[must_use]
pub fn cdc_config_from_params<S: std::hash::BuildHasher>(
    params: &std::collections::HashMap<String, String, S>,
) -> CdcConfig {
    CdcConfig {
        prefetch_buffer: resolve_cdc_param(
            params,
            "cdc_prefetch_buffer",
            "SPICE_CDC_PREFETCH_BUFFER",
            CDC_PREFETCH_BUFFER_DEFAULT,
            CDC_PREFETCH_BUFFER_MAX,
        ),
        max_coalesced_envelopes: resolve_cdc_param(
            params,
            "cdc_max_coalesced_envelopes",
            "SPICE_CDC_MAX_COALESCED_ENVELOPES",
            CDC_MAX_COALESCED_ENVELOPES_DEFAULT,
            CDC_MAX_COALESCED_ENVELOPES_MAX,
        ),
        max_coalesced_bytes: resolve_cdc_param(
            params,
            "cdc_max_coalesced_bytes",
            "SPICE_CDC_MAX_COALESCED_BYTES",
            CDC_MAX_COALESCED_BYTES_DEFAULT,
            CDC_MAX_COALESCED_BYTES_MAX,
        ),
        max_coalesce_age_ms: resolve_cdc_param_u64(
            params,
            "cdc_max_coalesce_age_ms",
            "SPICE_CDC_MAX_COALESCE_AGE_MS",
            CDC_MAX_COALESCE_AGE_MS_DEFAULT,
        ),
        commit_timeout: Duration::from_millis(resolve_cdc_param(
            params,
            "cdc_commit_timeout_ms",
            "SPICE_CDC_COMMIT_TIMEOUT_MS",
            CDC_COMMIT_TIMEOUT_MS_DEFAULT,
            CDC_COMMIT_TIMEOUT_MS_MAX,
        ) as u64),
        delete_subbatch_max: resolve_cdc_param(
            params,
            "cdc_delete_subbatch_max",
            "SPICE_CDC_DELETE_SUBBATCH_MAX",
            CDC_DELETE_SUBBATCH_MAX_DEFAULT,
            CDC_DELETE_SUBBATCH_MAX_MAX,
        ),
    }
}

/// Extract the subset of [`CDC_RUNTIME_PARAMS`] keys present in `params`
#[must_use]
pub fn extract_cdc_param_overrides<S: std::hash::BuildHasher>(
    params: &std::collections::HashMap<String, String, S>,
) -> Option<std::collections::HashMap<String, String>> {
    let extracted: std::collections::HashMap<String, String> = CDC_RUNTIME_PARAMS
        .iter()
        .filter_map(|&key| params.get(key).map(|v| (key.to_string(), v.clone())))
        .collect();
    if extracted.is_empty() {
        None
    } else {
        Some(extracted)
    }
}

/// Overlay per-dataset `cdc_*` params on top of an already-resolved global [`CdcConfig`].
#[must_use]
pub(crate) fn cdc_config_overlay(
    base: CdcConfig,
    dataset_params: &std::collections::HashMap<String, String>,
) -> CdcConfig {
    CdcConfig {
        prefetch_buffer: overlay_usize(
            dataset_params,
            "cdc_prefetch_buffer",
            base.prefetch_buffer,
            CDC_PREFETCH_BUFFER_MAX,
        ),
        max_coalesced_envelopes: overlay_usize(
            dataset_params,
            "cdc_max_coalesced_envelopes",
            base.max_coalesced_envelopes,
            CDC_MAX_COALESCED_ENVELOPES_MAX,
        ),
        max_coalesced_bytes: overlay_usize(
            dataset_params,
            "cdc_max_coalesced_bytes",
            base.max_coalesced_bytes,
            CDC_MAX_COALESCED_BYTES_MAX,
        ),
        max_coalesce_age_ms: overlay_u64(
            dataset_params,
            "cdc_max_coalesce_age_ms",
            base.max_coalesce_age_ms,
        ),
        commit_timeout: Duration::from_millis(overlay_usize(
            dataset_params,
            "cdc_commit_timeout_ms",
            usize::try_from(base.commit_timeout.as_millis()).unwrap_or(CDC_COMMIT_TIMEOUT_MS_MAX),
            CDC_COMMIT_TIMEOUT_MS_MAX,
        ) as u64),
        delete_subbatch_max: overlay_usize(
            dataset_params,
            "cdc_delete_subbatch_max",
            base.delete_subbatch_max,
            CDC_DELETE_SUBBATCH_MAX_MAX,
        ),
    }
}

fn overlay_usize(
    params: &std::collections::HashMap<String, String>,
    key: &'static str,
    base: usize,
    max: usize,
) -> usize {
    let Some(raw) = params.get(key) else {
        return base;
    };
    match raw.trim().parse::<usize>() {
        Ok(n) if (1..=max).contains(&n) => n,
        Ok(n) => {
            tracing::warn!(
                "dataset acceleration.params.{key}={n} is out of range [1, {max}]; keeping global value {base}"
            );
            base
        }
        Err(e) => {
            tracing::warn!(
                "dataset acceleration.params.{key}={raw:?} is not a valid usize ({e}); keeping global value {base}"
            );
            base
        }
    }
}

fn overlay_u64(
    params: &std::collections::HashMap<String, String>,
    key: &'static str,
    base: u64,
) -> u64 {
    let Some(raw) = params.get(key) else {
        return base;
    };
    match raw.trim().parse::<u64>() {
        Ok(n) => n,
        Err(e) => {
            tracing::warn!(
                "dataset acceleration.params.{key}={raw:?} is not a valid u64 ({e}); keeping global value {base}"
            );
            base
        }
    }
}

/// Parse a positive `usize` from `var`, falling back to `default` on missing,
/// unparseable, or out-of-range (`<1` or `> max`) values. Logs a warning
/// when an explicit value is rejected so misconfiguration is visible.
fn parse_env_usize(var: &'static str, default: usize, max: usize) -> usize {
    match std::env::var(var) {
        Err(_) => default,
        Ok(raw) => match raw.trim().parse::<usize>() {
            Ok(n) if (1..=max).contains(&n) => n,
            Ok(n) => {
                tracing::warn!("{var}={n} is out of range [1, {max}]; using default {default}");
                default
            }
            Err(e) => {
                tracing::warn!(
                    "{var}={raw:?} failed to parse as usize ({e}); using default {default}"
                );
                default
            }
        },
    }
}

fn parse_env_u64(var: &'static str, default: u64) -> u64 {
    match std::env::var(var) {
        Err(_) => default,
        Ok(raw) => match raw.trim().parse::<u64>() {
            Ok(n) => n,
            Err(e) => {
                tracing::warn!(
                    "{var}={raw:?} failed to parse as u64 ({e}); using default {default}"
                );
                default
            }
        },
    }
}

impl RefreshTask {
    /// Drives the dataset's CDC changes stream into the accelerator until the
    /// stream ends or the task is cancelled.
    ///
    /// # Errors
    ///
    /// Returns an error if a change batch cannot be applied to the accelerator —
    /// a schema change the dataset's `on_schema_change` policy refuses, or a
    /// write failure — or if the stream itself fails unrecoverably.
    pub async fn start_changes_stream(
        &self,
        refresh: Arc<RwLock<Refresh>>,
        changes_stream: ChangesStream,
        caching: Option<Weak<Caching>>,
        refresh_completion: Option<RefreshCompletion>,
        initial_load_completed: Arc<AtomicBool>,
    ) -> crate::accelerated::Result<()> {
        // Effective CDC config = global (already env+default folded) with any
        // per-dataset `cdc_*` overrides layered on top.
        let mut effective = cdc_config();
        if let Some(overrides) = self.cdc_param_overrides.as_ref() {
            effective = cdc_config_overlay(effective, overrides);
        }
        self.start_changes_stream_with_config(
            effective,
            refresh,
            changes_stream,
            caching,
            refresh_completion,
            initial_load_completed,
        )
        .await
    }

    /// Inner driver for [`Self::start_changes_stream`] with an explicit
    /// [`CdcConfig`]. Split out to simplify testing.
    async fn start_changes_stream_with_config(
        &self,
        cdc_cfg: CdcConfig,
        refresh: Arc<RwLock<Refresh>>,
        changes_stream: ChangesStream,
        caching: Option<Weak<Caching>>,
        refresh_completion: Option<RefreshCompletion>,
        initial_load_completed: Arc<AtomicBool>,
    ) -> crate::accelerated::Result<()> {
        self.consume_changes(
            cdc_cfg,
            refresh,
            changes_stream,
            caching,
            refresh_completion,
            initial_load_completed,
        )
        .await
    }

    /// Signal the dataset Ready: flip `initial_load_completed`, wake readiness
    /// waiters, then publish the `Ready` component status — in that order, so a
    /// waiter woken by the completion observes the completed flag. The single
    /// definition of the readiness side effect for every apply path (write,
    /// post-finalize, and readiness-only heartbeat runs).
    async fn signal_dataset_ready(&self, context: &ApplyContext<'_>) {
        context
            .initial_load_completed
            .store(true, Ordering::Relaxed);
        if let Some(refresh_completion) = context.refresh_completion {
            // A CDC apply answers no trigger, so it is recorded without a
            // request id: it releases every waiter taken before it and none
            // taken after.
            refresh_completion.record_untriggered();
        }
        self.update_component_status(status::ComponentStatus::Ready)
            .await;
    }

    /// Re-read the source into the accelerator as one atomic replacement, in
    /// answer to [`cdc::ChangeEnvelope::history_unavailable`]. Returns `false`
    /// when the rebuild failed and the stream must stop.
    ///
    /// This is deliberately the ordinary `refresh_mode: full` path — one
    /// `RefreshTask::run` with the mode overridden to
    /// [`RefreshMode::Full`] — so the replacement is the same atomic
    /// overwrite (`InsertOp::Overwrite`) a full refresh performs, and readers
    /// keep seeing the pre-rebuild table until it swaps. Clearing the table and
    /// letting the change stream refill it would be visible to queries as an
    /// empty, then partially-filled, table.
    ///
    /// The changes that follow this rebuild may predate the re-read, since the
    /// source resumes from wherever its log still starts. That is safe: the log
    /// is ordered and complete from that point, so replaying it converges on the
    /// source's current state — transiently stale, exactly like ordinary CDC
    /// catch-up lag. What could *not* converge, and is what this exists to fix,
    /// is a row deleted at the source while the history was gone: no change row
    /// for it will ever arrive, so only re-reading the table removes it.
    async fn rebuild_from_source(&self, context: &ApplyContext<'_>) -> bool {
        let mut refresh = context.refresh.read().await.clone();
        refresh.mode = RefreshMode::Full;

        tracing::warn!(
            "Dataset {}: the source can no longer supply the changes needed to continue, so the acceleration is being rebuilt from the source. This re-reads the table.",
            context.dataset_name,
        );

        if let Err(e) = self.run(refresh).await {
            let error_message = format!(
                "Failed to rebuild the acceleration for {} from the source after its change history became unavailable: {e}",
                context.dataset_name,
            );
            tracing::error!("{error_message}");
            self.set_refresh_status(
                context.refresh_sql,
                status::ComponentStatus::error_with_message(error_message),
            )
            .await;
            return false;
        }

        tracing::info!(
            "Dataset {}: rebuilt the acceleration from the source; resuming change streaming.",
            context.dataset_name,
        );
        true
    }

    /// Replace the accelerator from a [`cdc::ChangeBatch::rebuild_from_this_batch`]
    /// snapshot already carried on the `history_unavailable` envelope. This is
    /// the same atomic `InsertOp::Overwrite` a full refresh uses, but it does
    /// not list the source again — so the replacement rows and the envelope's
    /// applied-key / position commit stay on one snapshot.
    async fn rebuild_from_batches(
        &self,
        context: &ApplyContext<'_>,
        batches: Vec<RecordBatch>,
        schema: SchemaRef,
    ) -> bool {
        let label_sets = self.get_dataset_label_sets(&RefreshMode::Full).await;
        let _timer = MultiTimeMeasurement::new(&metrics::REFRESH_DURATION_MS, &label_sets);

        tracing::warn!(
            "Dataset {}: the source can no longer supply the changes needed to continue, so the acceleration is being replaced from the rebuild signal's snapshot. This does not re-read the source.",
            context.dataset_name,
        );

        let stream = RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            stream::iter(batches.into_iter().map(Ok)),
        );
        let update = StreamingDataUpdate::new(Box::pin(stream), UpdateType::Overwrite);
        if let Err(e) = self
            .write_streaming_data_update(Some(SystemTime::now()), update, context.refresh_sql, None)
            .await
        {
            let error_message = format!(
                "Failed to replace the acceleration for {} from the rebuild signal's snapshot: {e}",
                context.dataset_name,
            );
            tracing::error!("{error_message}");
            self.set_refresh_status(
                context.refresh_sql,
                status::ComponentStatus::error_with_message(error_message),
            )
            .await;
            return false;
        }

        tracing::info!(
            "Dataset {}: replaced the acceleration from the rebuild signal's snapshot; resuming change streaming.",
            context.dataset_name,
        );
        true
    }

    async fn run_finalize_side_effects(
        &self,
        context: &mut ApplyContext<'_>,
        committers: Vec<Box<dyn cdc::CommitChange + Send + Sync>>,
        ready_after_finalize: bool,
        durability: StorageDurability,
    ) -> bool {
        if ready_after_finalize {
            self.signal_dataset_ready(context).await;
        }

        if let Some(cache_provider_ref) = context.caching
            && let Some(cache_provider) = cache_provider_ref.upgrade()
            && let Err(e) = cache_provider
                .invalidate_for_table(context.dataset_name.clone())
                .await
            && !self.runtime_status.is_shutdown()
        {
            tracing::error!(
                "Failed to invalidate cached results for dataset {}: {e}",
                context.dataset_name
            );
        }

        self.acknowledge_published(context, committers, durability)
            .await
    }

    async fn acknowledge_published(
        &self,
        context: &mut ApplyContext<'_>,
        committers: Vec<Box<dyn cdc::CommitChange + Send + Sync>>,
        durability: StorageDurability,
    ) -> bool {
        if !committers.is_empty() {
            if let Some(previous_commit) = context.pending_commit.take() {
                let commit_wait_start = Instant::now();
                if let Some(error_message) = join_pending_commit(
                    previous_commit,
                    context.dataset_name,
                    self.runtime_status.is_shutdown(),
                    context.commit_timeout,
                )
                .await
                {
                    self.set_refresh_status(
                        context.refresh_sql,
                        status::ComponentStatus::error_with_message(error_message),
                    )
                    .await;
                    return false;
                }
                record_cdc_fixed_cost(context.metric_labels, "commit_wait", commit_wait_start);
            }

            if let StorageDurability::Deferred(fence) = durability {
                let Some(observer) = context.deferred_commits else {
                    self.set_refresh_status(
                        context.refresh_sql,
                        status::ComponentStatus::error_with_message(
                            "Deferred CDC write has no source durability observer".to_string(),
                        ),
                    )
                    .await;
                    return false;
                };
                observer.enqueue(fence, committers).await;
            } else {
                if let Some(observer) = context.deferred_commits
                    && let Some(error_message) = flush_pending_source_commits(
                        self.change_sink().await,
                        observer,
                        context.dataset_name,
                        &self.runtime_status,
                    )
                    .await
                {
                    self.set_refresh_status(
                        context.refresh_sql,
                        status::ComponentStatus::error_with_message(error_message),
                    )
                    .await;
                    return false;
                }
                // NotPromised retains the provider's publication-based source
                // acknowledgement policy; it does not assert storage durability.
                *context.pending_commit = Some(spawn_ordered_commit_task(
                    committers,
                    Arc::clone(&self.runtime_status),
                    context.dataset_name.clone(),
                ));
            }
        }
        true
    }

    /// Drain storage and source completion before a full-refresh overwrite that
    /// does not submit row changes through the sink.
    async fn prepare_rebuild(&self, context: &mut ApplyContext<'_>) -> bool {
        // The source metadata FIFO is drained before entering this barrier.
        if let Some(commit) = context.pending_commit.take()
            && let Some(message) = join_pending_commit(
                commit,
                context.dataset_name,
                self.runtime_status.is_shutdown(),
                context.commit_timeout,
            )
            .await
        {
            self.set_refresh_status(
                context.refresh_sql,
                status::ComponentStatus::error_with_message(message),
            )
            .await;
            return false;
        }
        let sink = self.change_sink().await;
        if let Err(error) = sink.flush().await {
            self.set_refresh_status(
                context.refresh_sql,
                status::ComponentStatus::error_with_message(format_datafusion_error(&error)),
            )
            .await;
            return false;
        }
        if let Some(observer) = context.deferred_commits
            && let Some(error_message) = flush_pending_source_commits(
                sink,
                observer,
                context.dataset_name,
                &self.runtime_status,
            )
            .await
        {
            self.set_refresh_status(
                context.refresh_sql,
                status::ComponentStatus::error_with_message(error_message),
            )
            .await;
            return false;
        }
        true
    }

    /// Apply a contiguous run of successful envelopes as a single coalesced
    /// write, then append their commits to the ordered background commit chain.
    #[cfg(test)]
    async fn apply_envelope_run(
        &self,
        context: &mut ApplyContext<'_>,
        mut envelopes: Vec<cdc::ChangeEnvelope>,
    ) -> bool {
        debug_assert!(
            !envelopes.is_empty(),
            "run must contain at least one envelope"
        );

        // Runs before the heartbeat retain below: a signal may ride a zero-row
        // envelope, which the retain would otherwise strip — and the rebuild
        // would never happen. See `trim_to_rebuild_signal` for why the prefix
        // goes with it.
        let history_unavailable = trim_to_rebuild_signal(&mut envelopes);
        if history_unavailable && !self.prepare_rebuild(context).await {
            return false;
        }

        // Read after the trim and before the retain. After, because a readiness
        // flag from a discarded envelope must not reach `signal_dataset_ready`,
        // which wakes everything blocked on initial load using a flag describing
        // a source state the rebuild is about to replace. That narrows, but does
        // not close, the stale-serving window: on the `history_unavailable` path
        // the dataset becomes Ready regardless, because `rebuild_from_source` is
        // the ordinary full refresh and `RefreshTask::run` sets `Ready` (and with
        // it `initial_load_completed`) on success, before the source has
        // reconnected at the captured head and confirmed catch-up by lag. Closing
        // the window needs the rebuild to run without that terminal Ready
        // transition, tracked in #13028. Before the retain, because it drops
        // heartbeats whose ready flag IS meant to count.
        //
        // Split envelopes into (committers, batches, ready_flags) preserving
        // arrival order. Committers will be drained sequentially in the
        // background commit task; per-source semantics (e.g., PG `Standby
        // Status Update` carrying the latest LSN, Kafka per-partition
        // offsets) require this ordering.
        let any_ready = envelopes.iter().any(cdc::ChangeEnvelope::is_dataset_ready);

        // Readiness heartbeats carry no source acknowledgement and must not force
        // storage flushes. Zero-row envelopes with real committers retain their
        // publication and durability ordering.
        envelopes.retain(|env| !env.is_no_op_heartbeat());

        // Readiness-only run: every envelope was a heartbeat. A classic
        // `history_unavailable` signal is itself a no-op heartbeat, so it is
        // gone here and the replacement is a federated re-read. A listing
        // snapshot rides a real committer and survives into `into_parts`.
        if envelopes.is_empty() {
            if history_unavailable && !self.rebuild_from_source(context).await {
                return false;
            }
            if any_ready {
                if let Some(pending) = context.pending_finalize.as_mut() {
                    // A previous durable burst's Stage-B publish is still
                    // pending; readiness follows its completion, mirroring the
                    // `!current_finalize_pending` gate on the write path.
                    pending.ready_after_finalize = true;
                } else {
                    self.signal_dataset_ready(context).await;
                }
            }
            return true;
        }
        // Time the deferred-batch build: sources that defer the decode (MySQL
        // binlog and Postgres logical-replication rows) pay one `spawn_blocking`
        // round trip per burst here — a cost otherwise invisible between the
        // recv_wait and coalesce stage timers.
        let decode_start = Instant::now();
        // Build on the per-dataset apply task, off the source's shared
        // read/route path. A deferred build can fail on per-row value typing that
        // only surfaces at build time (e.g. an unmergeable unchanged-TOAST column
        // under REPLICA IDENTITY DEFAULT); treat it as terminal for this dataset,
        // mirroring the eager path's pump-side fatal. The burst's committers are
        // dropped unacked, so the source re-streams on reconnect.
        let parts = match cdc::into_parts_offloaded_burst(envelopes).await {
            Ok(parts) => parts,
            Err(e) => {
                let error_message = format!(
                    "Failed to build CDC change batch for {}: {e}",
                    context.dataset_name,
                );
                tracing::error!("{error_message}");
                self.set_refresh_status(
                    context.refresh_sql,
                    status::ComponentStatus::error_with_message(error_message),
                )
                .await;
                return false;
            }
        };
        record_cdc_fixed_cost(context.metric_labels, "decode", decode_start);

        // The source has lost the history that explains what changed while it was
        // away. Prefer the snapshot already on the signal (`rebuild_from_this_batch`)
        // so replacement rows and the applied-key commit are the same listing; a
        // later federated scan can invent extras the listing did not mark applied.
        if history_unavailable {
            let listing_rebuild = parts
                .iter()
                .any(|(_, batch, _, _)| batch.rebuild_from_this_batch());
            if listing_rebuild {
                let rebuild_batches: Vec<RecordBatch> = parts
                    .iter()
                    .filter(|(_, batch, _, _)| batch.rebuild_from_this_batch())
                    .map(|(_, batch, _, _)| batch.data_batch())
                    .collect();
                let schema = rebuild_batches
                    .first()
                    .map_or_else(|| self.accelerator.schema(), RecordBatch::schema);
                let nonempty: Vec<RecordBatch> = rebuild_batches
                    .into_iter()
                    .filter(|batch| batch.num_rows() > 0)
                    .collect();
                if !self.rebuild_from_batches(context, nonempty, schema).await {
                    return false;
                }
            } else if !self.rebuild_from_source(context).await {
                return false;
            }
        }

        // Readiness and the history-unavailable signal were both folded in before
        // the heartbeat retain, so their per-envelope flags are spent here.
        // Listing-rebuild rows were the overwrite payload and must not also upsert.
        let mut rebuild_committers: Vec<Box<dyn cdc::CommitChange + Send + Sync>> = Vec::new();
        let mut committers: Vec<Box<dyn cdc::CommitChange + Send + Sync>> = Vec::new();
        let mut batches: Vec<ChangeBatch> = Vec::new();
        for (committer, batch, _is_ready, _history_unavailable) in parts {
            if batch.rebuild_from_this_batch() {
                rebuild_committers.push(committer);
            } else {
                committers.push(committer);
                batches.push(batch);
            }
        }

        if batches.is_empty() {
            rebuild_committers.append(&mut committers);
            return self
                .run_finalize_side_effects(
                    context,
                    rebuild_committers,
                    any_ready,
                    StorageDurability::NotPromised,
                )
                .await;
        }

        // The rebuild overwrite has already succeeded. Finalize its committer
        // now, independently of later CDC groups: if a later group fails to
        // coalesce/write, `apply_coalesced_run` drops those committers unacked,
        // and an `AppliedKeysCommitter::drop` would otherwise release keys whose
        // rows are already present — making the next backfill append them again.
        if !rebuild_committers.is_empty()
            && !self
                .run_finalize_side_effects(
                    context,
                    rebuild_committers,
                    false,
                    StorageDurability::NotPromised,
                )
                .await
        {
            return false;
        }

        // Mixed-schema runs (mid-stream schema evolution): `concat_change_batches`
        // requires equal schemas. When the dataset's policy allows evolution,
        // split the run into contiguous same-schema groups applied in order —
        // the common case stays a single group. With `block` (or no installed
        // settings) the run is one group and a mixed-schema concat fails the
        // write path and stops the stream so the source redelivers after the
        // member re-registers.
        let split_on_schema_change = cdc_schema_evolution_for(context.dataset_name)
            .is_some_and(|evolution| !matches!(evolution.policy, OnSchemaChange::Block));
        let groups = group_run_by_schema(batches, committers, split_on_schema_change);
        let last_group = groups.len().saturating_sub(1);
        for (group_idx, (group_batches, group_committers)) in groups.into_iter().enumerate() {
            // Exact applied-row count for this group, summed from the just-built
            // batches (no extra build — `into_parts` already built them);
            // `num_rows_hint()` would over-count a PK-changing UPDATE as two
            // rows. Computed before `apply_coalesced_run` consumes the batches,
            // but recorded only AFTER the group applies, so a Stop
            // failure can't inflate the throughput metric with rows that were
            // never written.
            let group_rows = group_batches
                .iter()
                .map(|b| b.record.num_rows() as u64)
                .fold(0_u64, u64::saturating_add);
            match self
                .apply_coalesced_run(
                    context,
                    group_batches,
                    group_committers,
                    any_ready && group_idx == last_group,
                )
                .await
            {
                CoalescedRunOutcome::Applied => {
                    metrics::CDC_APPLY_BURST_ROWS_TOTAL
                        .add(group_rows, context.metric_labels.dataset());
                }
                CoalescedRunOutcome::Stop => return false,
            }
        }
        true
    }

    /// Concatenate one same-schema group of `ChangeBatch`es into a single
    /// accelerator write, then hand the group's committers to the ordered
    /// background commit chain. Split out of [`Self::apply_envelope_run`] so
    /// mixed-schema runs can apply per contiguous same-schema group.
    #[cfg(test)]
    async fn apply_coalesced_run(
        &self,
        context: &mut ApplyContext<'_>,
        batches: Vec<ChangeBatch>,
        committers: Vec<Box<dyn cdc::CommitChange + Send + Sync>>,
        mark_ready: bool,
    ) -> CoalescedRunOutcome {
        // The group's batches are about to be concatenated into one write, so
        // its committers can coalesce too: for a shared-slot source this folds
        // the whole burst to a single max-LSN commit (see `fold_committers`),
        // shrinking both the immediate ordered commit chain and any deferred
        // queue entry below. Order-sensitive sources fold to a no-op.
        let committers = fold_committers(committers);
        let coalesce_start = Instant::now();
        // Fast path: a single envelope (low-load / serial behavior). Skips
        // concat allocation entirely so the no-coalesce path matches the
        // pre-pipelining cost exactly.
        let coalesced_batch = if batches.len() == 1 {
            batches.into_iter().next().unwrap_or_else(|| unreachable!())
        } else {
            match concat_change_batches(&batches) {
                Ok(b) => b,
                Err(e) => {
                    let error_message = format!(
                        "Failed to coalesce {} CDC envelopes for {}: {e}",
                        batches.len(),
                        context.dataset_name,
                    );
                    tracing::error!("{error_message}");
                    self.set_refresh_status(
                        context.refresh_sql,
                        status::ComponentStatus::error_with_message(error_message),
                    )
                    .await;
                    // Drop committers without acking and stop the stream. A
                    // continued stream would leave a source delivered
                    // watermark ahead of this unapplied window; reconnect
                    // replay would then skip it. Stopping drops the receiver
                    // so the member re-registers and delivered resets with
                    // committed (see `AckSlot::routes`).
                    return CoalescedRunOutcome::Stop;
                }
            }
        };
        record_cdc_fixed_cost(context.metric_labels, "coalesce", coalesce_start);

        let sink = self.change_sink().await;
        let requires_durable_cdc_path = !committers_all_support_deferral(&committers)
            || change_batch_requires_durable_cdc_path(
                &coalesced_batch,
                sink.capabilities().deferred_deletes,
            );
        if requires_durable_cdc_path
            && let Some(observer) = context.deferred_commits
            && let Some(error_message) = flush_pending_source_commits(
                sink,
                observer,
                context.dataset_name,
                &self.runtime_status,
            )
            .await
        {
            self.set_refresh_status(
                context.refresh_sql,
                status::ComponentStatus::error_with_message(error_message),
            )
            .await;
            return CoalescedRunOutcome::Stop;
        }
        let recovery = if requires_durable_cdc_path {
            Recovery::Durable
        } else {
            Recovery::Replayable
        };

        let write_start = Instant::now();
        match self
            .write_change_with_context(
                coalesced_batch,
                context.write_ctx,
                context.write_session_state,
                recovery,
            )
            .await
        {
            Ok(write_outcome) => {
                record_cdc_fixed_cost(context.metric_labels, "write", write_start);

                if let Some(previous_pending) = context.pending_finalize.take() {
                    let finalize_start = Instant::now();
                    if let Some(error_message) = join_pending_finalize(
                        previous_pending.finalize,
                        context.dataset_name,
                        self.runtime_status.is_shutdown(),
                    )
                    .await
                    {
                        self.set_refresh_status(
                            context.refresh_sql,
                            status::ComponentStatus::error_with_message(error_message),
                        )
                        .await;
                        return CoalescedRunOutcome::Stop;
                    }
                    record_cdc_fixed_cost(context.metric_labels, "finalize_wait", finalize_start);

                    if !self
                        .run_finalize_side_effects(
                            context,
                            previous_pending.committers,
                            previous_pending.ready_after_finalize,
                            previous_pending.durability,
                        )
                        .await
                    {
                        return CoalescedRunOutcome::Stop;
                    }
                }

                let current_finalize_pending = write_outcome.pending_finalize.is_some();
                if mark_ready && !current_finalize_pending {
                    self.signal_dataset_ready(context).await;
                }
                if write_outcome.result == WriteChangeResult::DataWritten
                    && !current_finalize_pending
                    && let Some(cache_provider_ref) = context.caching
                    && let Some(cache_provider) = cache_provider_ref.upgrade()
                    && let Err(e) = cache_provider
                        .invalidate_for_table(context.dataset_name.clone())
                        .await
                    && !self.runtime_status.is_shutdown()
                {
                    tracing::error!(
                        "Failed to invalidate cached results for dataset {}: {e}",
                        context.dataset_name
                    );
                }

                if let Some(finalize) = write_outcome.pending_finalize {
                    *context.pending_finalize = Some(PendingFinalizeCommit {
                        finalize,
                        committers,
                        ready_after_finalize: mark_ready,
                        durability: write_outcome.durability,
                    });
                } else if !self
                    .acknowledge_published(context, committers, write_outcome.durability)
                    .await
                {
                    return CoalescedRunOutcome::Stop;
                }
            }
            Err(e) => {
                let error_message = format_datafusion_error(&e);
                self.set_refresh_status(
                    context.refresh_sql,
                    status::ComponentStatus::error_with_message(error_message),
                )
                .await;
                if !self.runtime_status.is_shutdown() {
                    tracing::error!("Error writing change for {}: {e}", context.dataset_name);
                }
                // Drop committers without acking, and stop this stream before
                // any later envelope can commit past the uncommitted gap.
                return CoalescedRunOutcome::Stop;
            }
        }
        CoalescedRunOutcome::Applied
    }

    #[cfg(test)]
    async fn write_change(
        &self,
        change_batch: ChangeBatch,
    ) -> crate::accelerated::Result<WriteChangeResult> {
        let ctx = SessionContext::new();
        let session_state = ctx.state();
        let outcome = self
            .write_change_with_context(change_batch, &ctx, &session_state, Recovery::Durable)
            .await?;
        if let Some(publication) = outcome.pending_finalize {
            publication
                .wait()
                .await
                .context(crate::accelerated::FailedToWriteDataSnafu)?;
        }
        Ok(outcome.result)
    }

    #[cfg(test)]
    async fn write_change_with_context(
        &self,
        change_batch: ChangeBatch,
        _ctx: &SessionContext,
        _session_state: &SessionState,
        recovery: Recovery,
    ) -> crate::accelerated::Result<WriteChangeOutcome> {
        // Classify the whole input before submission: a delete or truncate in
        // this batch must not run before a later upsert's schema is refused.
        // Delete-only inputs do not write the incoming data schema.
        if (0..change_batch.record.num_rows()).any(|row| {
            matches!(
                change_batch.op(row),
                ChangeOperation::Create | ChangeOperation::Update | ChangeOperation::Read
            )
        }) {
            self.maybe_evolve_schema_for_cdc(&change_batch.data_schema())
                .await?;
        }

        let receipt = self
            .change_sink()
            .await
            .submit(
                LogicalChangeBatch::cdc(change_batch),
                WriteOptions {
                    recovery,
                    delete_batch_size: self.cdc_delete_subbatch_max(),
                },
            )
            .await
            .map_err(find_datafusion_root)
            .context(crate::accelerated::FailedToWriteDataSnafu)?;
        // Ready can contain a completed failure. Observe it before source effects.
        let pending_finalize = if receipt.publication.is_ready() {
            receipt
                .published()
                .await
                .map_err(find_datafusion_root)
                .context(crate::accelerated::FailedToWriteDataSnafu)?;
            None
        } else {
            Some(receipt.publication)
        };
        if receipt.changed {
            self.update_last_updated_at();
        }
        if let Some(ref callback) = self.on_stream_batch_process_callback {
            let mut callback_guard = callback.lock().await;
            callback_guard().await;
        }
        Ok(WriteChangeOutcome {
            result: if receipt.changed {
                WriteChangeResult::DataWritten
            } else {
                WriteChangeResult::NoChange
            },
            pending_finalize,
            durability: receipt.durability,
        })
    }

    /// Detect a widening schema change between the incoming CDC data struct
    /// and the accelerator schema and act per the dataset's installed
    /// `on_schema_change` policy:
    ///
    /// Called once per upsert-bearing burst from `write_change_with_context`,
    /// before any of the burst's sub-batches is applied, so a refusal here leaves
    /// the acceleration untouched.
    ///
    /// Live-capable sinks evolve before submission. Targets requiring recreation
    /// refuse the input. Restart-capable targets keep their current schema and
    /// warn. The `fail` policy always rejects drift; `block` keeps the schema.
    fn cdc_policy(&self) -> policy::RefreshCdcPolicy {
        policy::RefreshCdcPolicy {
            dataset: self.dataset_name.clone(),
            settings: cdc_schema_evolution_for(&self.dataset_name),
            type_rewrites: self.engine_type_rewrites,
        }
    }

    #[cfg(test)]
    async fn maybe_evolve_schema_for_cdc(
        &self,
        incoming: &SchemaRef,
    ) -> crate::accelerated::Result<()> {
        let policy = self.cdc_policy();
        let sink = self.change_sink().await;
        if let SchemaDecision::Evolve(plan) = policy
            .classify(incoming, &self.accelerator.schema(), sink.capabilities())
            .context(crate::accelerated::FailedToWriteDataSnafu)?
        {
            sink.evolve_schema(&plan)
                .await
                .context(crate::accelerated::FailedToWriteDataSnafu)?;
            policy.applied(&plan);
        }
        Ok(())
    }

    /// Effective per-plan delete-key cap for this dataset: the process-global
    /// [`CdcConfig`] with any per-dataset `cdc_*` overrides layered on. Read
    /// once per Delete sub-batch (not per row), so runtime overrides apply
    /// without threading config through the write path. Floored at 1 so
    /// `chunks()` never sees a zero.
    fn cdc_delete_subbatch_max(&self) -> usize {
        let base = cdc_config();
        let effective = match self.cdc_param_overrides.as_ref() {
            Some(overrides) => cdc_config_overlay(base, overrides),
            None => base,
        };
        effective.delete_subbatch_max.max(1)
    }
}

/// One equal-schema group from [`group_run_by_schema`]: the batches and their
/// matching commit handles, kept in arrival order.
#[cfg(test)]
type SchemaGroupedRun = (
    Vec<ChangeBatch>,
    Vec<Box<dyn cdc::CommitChange + Send + Sync>>,
);

/// Split a contiguous envelope run into groups of equal-schema
/// `ChangeBatch`es (preserving arrival order) so each group can be
/// concatenated into one accelerator write. A mid-stream schema evolution
/// produces exactly one boundary: every batch before the source adopted the
/// wider schema, then every batch after. With `split == false` the whole run
/// is a single group — zero-cost for the `block` policy.
#[cfg(test)]
fn group_run_by_schema(
    batches: Vec<ChangeBatch>,
    committers: Vec<Box<dyn cdc::CommitChange + Send + Sync>>,
    split: bool,
) -> Vec<SchemaGroupedRun> {
    if !split || batches.len() <= 1 {
        return vec![(batches, committers)];
    }
    let mut groups: Vec<SchemaGroupedRun> = Vec::new();
    for (batch, committer) in batches.into_iter().zip(committers) {
        let same_schema_as_last_group = groups.last().is_some_and(|(group_batches, _)| {
            group_batches
                .last()
                .is_some_and(|last| last.record.schema() == batch.record.schema())
        });
        if same_schema_as_last_group {
            let Some((group_batches, group_committers)) = groups.last_mut() else {
                unreachable!("same_schema_as_last_group implies a last group");
            };
            group_batches.push(batch);
            group_committers.push(committer);
        } else {
            groups.push((vec![batch], vec![committer]));
        }
    }
    groups
}

/// Concatenate the underlying `RecordBatch`es of multiple `ChangeBatch`es
/// into a single `ChangeBatch` so a coalesced burst can be applied with one
/// `insert_into` call. All batches in a single CDC stream share the same
/// `changes_schema(table_schema)`, so the schema check inside
/// `arrow::compute::concat_batches` will not fail in normal operation; if it
/// does we surface the error and the caller stops the stream so the source
/// redelivers after the member re-registers.
#[cfg(test)]
fn concat_change_batches(batches: &[ChangeBatch]) -> crate::accelerated::Result<ChangeBatch> {
    debug_assert!(
        !batches.is_empty(),
        "concat_change_batches requires at least one batch",
    );

    let schema = batches[0].record.schema();
    let records: Vec<&RecordBatch> = batches.iter().map(|b| &b.record).collect();
    let combined = arrow::compute::concat_batches(&schema, records)
        .context(crate::accelerated::FailedToBuildRecordBatchSnafu)?;
    // The coalesced batch keeps the newest constituent commit timestamp: it rides
    // the batch into the accelerator (`write_cdc_append_stream_with_source_commit_ts`),
    // where it feeds the replication-lag and freshness signals the adaptive tuner's
    // goals are stated against. The max is the most recent, and zero-row envelopes
    // are excluded because their timestamp is not evidence that data up to that
    // point was received.
    let source_commit_ts_ms = batches
        .iter()
        .filter(|batch| !batch.is_heartbeat())
        .filter_map(ChangeBatch::source_commit_ts_ms)
        .max();
    ChangeBatch::try_new(combined)
        .map(|batch| batch.with_source_commit_ts_ms(source_commit_ts_ms))
        .map_err(|e| {
            // ChangeBatchError isn't part of the AcceleratedTable Error enum;
            // wrap it in FailedToBuildRecordBatch so the caller's status path
            // doesn't have to learn about a new variant.
            crate::accelerated::Error::FailedToBuildRecordBatch {
                source: arrow::error::ArrowError::ExternalError(Box::new(e)),
            }
        })
}

fn elapsed_ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

fn record_cdc_fixed_cost(labels: &DatasetMetricLabels, phase: &'static str, start: Instant) {
    metrics::CDC_APPLY_FIXED_COST_MS.record(elapsed_ms(start), &labels.tagged("phase", phase));
}

#[cfg(test)]
async fn join_pending_finalize(
    handle: PendingApplyFinalize,
    dataset_name: &TableReference,
    is_shutdown: bool,
) -> Option<String> {
    classify_finalize_result(handle.wait().await, dataset_name, is_shutdown)
}

/// Publication failure must never release source committers, including during
/// shutdown. The sink retains ownership of storage work independently.
#[cfg(test)]
fn classify_finalize_result(
    result: Result<(), DataFusionError>,
    dataset_name: &TableReference,
    is_shutdown: bool,
) -> Option<String> {
    let Err(error) = result else {
        return None;
    };
    let message = format!("CDC apply finalizer for {dataset_name} failed: {error}");
    if is_shutdown {
        tracing::debug!("{message}");
    } else {
        tracing::error!("{message}");
    }
    Some(message)
}

/// Await an in-flight source acknowledgement task. Surfaces
/// panics loudly (we must never silently swallow a commit-task panic — that
/// would leave the dataset healthy while source-side offsets stop advancing)
/// but treats cancellation during shutdown as expected.
async fn join_pending_commit(
    mut handle: tokio::task::JoinHandle<Result<(), String>>,
    dataset_name: &TableReference,
    is_shutdown: bool,
    commit_timeout: Duration,
) -> Option<String> {
    tokio::select! {
        result = &mut handle => {
            match result {
                Err(e) if e.is_panic() => {
                    let error_message =
                        format!("CDC commit task for {dataset_name} panicked: {e}");
                    tracing::error!("{error_message}");
                    Some(error_message)
                }
                Err(e) if e.is_cancelled() && is_shutdown => {
                    tracing::debug!("CDC commit task for {dataset_name} was cancelled (likely shutdown)");
                    None
                }
                Err(e) => {
                    let error_message =
                        format!("CDC commit task for {dataset_name} ended unexpectedly: {e}");
                    tracing::error!("{error_message}");
                    Some(error_message)
                }
                Ok(Ok(())) => None,
                Ok(Err(error_message)) => Some(error_message),
            }
        }
        () = tokio::time::sleep(commit_timeout) => {
            handle.abort();
            if is_shutdown {
                tracing::debug!(
                    "CDC commit task for {dataset_name} timed out during shutdown after {}ms",
                    commit_timeout.as_millis()
                );
                None
            } else {
                let error_message = format!(
                    "CDC commit task for {dataset_name} did not finish within {}ms",
                    commit_timeout.as_millis()
                );
                tracing::error!("{error_message}");
                Some(error_message)
            }
        }
    }
}

fn spawn_ordered_commit_task(
    committers: Vec<Box<dyn cdc::CommitChange + Send + Sync>>,
    runtime_status: Arc<status::RuntimeStatus>,
    commit_dataset: TableReference,
) -> tokio::task::JoinHandle<Result<(), String>> {
    tokio::spawn(async move {
        // Publication and any required durability fence precede this task.
        // The source drains its previous commit task before spawning the next,
        // so a later acknowledgement cannot pass an earlier failed write.
        for committer in committers {
            if let Err(e) = committer.commit().await
                && !runtime_status.is_shutdown()
            {
                let error_message =
                    format!("Failed to commit CDC change envelope for {commit_dataset}: {e}");
                tracing::error!("{error_message}");
                return Err(error_message);
            }
        }
        Ok(())
    })
}

#[cfg(test)]
pub(crate) fn get_primary_key_value(
    data: &RecordBatch,
    key: &str,
) -> crate::accelerated::Result<(String, Expr)> {
    get_primary_key_value_at_row(data, 0, key)
}

#[cfg(test)]
pub(crate) fn get_primary_key_value_at_row(
    data: &RecordBatch,
    row: usize,
    key: &str,
) -> crate::accelerated::Result<(String, Expr)> {
    let data_schema = data.schema();
    let (primary_key_idx, field) = data_schema.column_with_name(key).ok_or_else(|| {
        crate::accelerated::PrimaryKeyExpectedSchemaToHaveFieldSnafu {
            field_name: key.to_string(),
            schema: Arc::clone(&data_schema),
        }
        .build()
    })?;

    let key_col = data.column(primary_key_idx);
    match field.data_type() {
        DataType::Int32 => {
            extract_primary_key!(key_col, key, data_schema, Int32Array, "Int32", row)
        }
        DataType::Int64 => {
            extract_primary_key!(key_col, key, data_schema, Int64Array, "Int64", row)
        }
        DataType::Utf8 => {
            extract_primary_key!(key_col, key, data_schema, StringArray, "String", row)
        }
        _ => crate::accelerated::PrimaryKeyTypeNotYetSupportedSnafu {
            data_type: field.data_type().to_string(),
        }
        .fail(),
    }
}

/// Trim a run to start at its [`cdc::ChangeEnvelope::history_unavailable`]
/// signal, reporting whether it had one.
///
/// The rebuild runs before the whole run, so anything the run carries AHEAD of
/// the signal would be applied on top of a table that was re-read past it —
/// writing a value the source has already moved on from. Streaming resumes at
/// the position the source captured when it raised the signal, which is at or
/// after those envelopes, so nothing replays over the regression and it is
/// durable.
///
/// Discarding them is exact rather than lossy: the signal means the source can
/// no longer explain what changed, and the re-read observes the source at a
/// point at or after every envelope that preceded the signal, so the re-read
/// already reflects them. Their committers are dropped unacked, which only holds
/// a source position back — the rebuild's own boundary commit carries the new
/// one.
///
/// The LAST signal is the barrier, not the first: one rebuild answers every
/// signal in the run (the caller performs exactly one), so envelopes between two
/// signals are subsumed by it just as the leading ones are.
///
/// A no-op on every ordinary run, which carries no signal at all.
#[cfg(test)]
fn trim_to_rebuild_signal(envelopes: &mut Vec<cdc::ChangeEnvelope>) -> bool {
    let Some(signal) = envelopes
        .iter()
        .rposition(cdc::ChangeEnvelope::history_unavailable)
    else {
        return false;
    };
    envelopes.drain(..signal);
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteChangeResult {
    DataWritten,
    NoChange,
}

#[derive(PartialEq)]
enum StreamErrorType {
    Transient,
    Fatal,
}

/// Logs and classifies [`StreamError`] errors for a dataset.
/// Returns `true` if the error is transient and the stream can continue normally.
/// These errors are generally nonfatal and often indicate that the consumer should retry or continue polling.
fn handle_stream_error(err: &cdc::StreamError, dataset_name: &TableReference) -> StreamErrorType {
    #[cfg(any(feature = "debezium", feature = "kafka"))]
    if matches!(err, cdc::StreamError::Kafka(KafkaError::EmptyBatch)) {
        return StreamErrorType::Transient;
    }

    #[cfg(any(feature = "debezium", feature = "kafka"))]
    if let cdc::StreamError::Kafka(KafkaError::UnableToReceiveMessage { source }) = err {
        match source {
            RdKafkaError::MessageConsumption(RDKafkaErrorCode::PollExceeded) => {
                tracing::warn!(
                    "Kafka poll interval exceeded for dataset '{dataset_name}': connection lost or consumer too slow. Retrying."
                );
                return StreamErrorType::Transient;
            }
            RdKafkaError::MessageConsumption(RDKafkaErrorCode::BrokerTransportFailure) => {
                tracing::warn!(
                    "Connection to Kafka broker for dataset '{dataset_name}' was lost or is invalid. Retrying."
                );
                return StreamErrorType::Transient;
            }
            RdKafkaError::MessageConsumption(RDKafkaErrorCode::OperationTimedOut) => {
                tracing::error!(
                    "Kafka operation timed out while retrieving message for dataset '{dataset_name}'. Retrying."
                );
                return StreamErrorType::Transient;
            }
            RdKafkaError::MessageConsumption(RDKafkaErrorCode::AllBrokersDown) => {
                tracing::warn!(
                    "All Kafka brokers are down for dataset '{dataset_name}'. Check broker status and network connectivity. Retrying."
                );
                return StreamErrorType::Transient;
            }
            RdKafkaError::MessageConsumption(RDKafkaErrorCode::UnknownTopicOrPartition) => {
                tracing::error!(
                    "Kafka topic not found for dataset '{dataset_name}': check if the topic exists and is spelled correctly."
                );
            }
            _ => {
                tracing::error!(
                    "A Kafka error occurred for dataset '{dataset_name}': {source}. Check your Kafka broker and network connectivity."
                );
            }
        }
        return StreamErrorType::Fatal;
    }

    tracing::error!("Changes stream error for {dataset_name}: {err}");
    StreamErrorType::Fatal
}

#[cfg(test)]
mod tests {
    use super::ingress::{
        PREBUILD_GROUP_MAX_BYTES, PREBUILD_GROUP_MAX_ENVELOPES, SourceItem, take_ready_group,
    };
    use super::*;
    use arrow::array::{ArrayRef, Int32Array, ListArray, StringArray, StructArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use data_components::arrow::write::MemTable;
    use data_components::cdc::changes_schema;
    use datafusion::datasource::TableProvider;
    use futures::FutureExt;
    use spice_table::IndexLayer;

    use std::sync::Arc;

    #[test]
    fn cdc_config_from_params_resolves_max_coalesce_age_ms() {
        let config = cdc_config_from_params(&std::collections::HashMap::from([(
            "cdc_max_coalesce_age_ms".to_string(),
            "90000".to_string(),
        )]));

        assert_eq!(config.max_coalesce_age_ms, 90_000);
    }

    /// Change D: the coalescing/prefetch ceilings were raised 4096/1024 -> 16384.
    /// A burst configuration LARGER than the old 4096 cap (e.g. 8000-16384) must
    /// now be accepted verbatim by `cdc_config_from_params` — proving the cap is
    /// 16384, not the old 4096 (which would re-clip the burst) and not the 256
    /// default (a silent fallback). Both `cdc_max_coalesced_envelopes` and the
    /// prefetch buffer (the REAL coalescing ceiling, since the drain `try_recv`s
    /// only what is already buffered) must accept the raised values.
    #[test]
    fn cdc_config_from_params_accepts_burst_above_old_4096_cap() {
        // Sanity: the consts were actually raised to 16384.
        assert_eq!(
            CDC_MAX_COALESCED_ENVELOPES_MAX, 16_384,
            "max coalesced envelopes ceiling must be raised to 16384"
        );
        assert_eq!(
            CDC_PREFETCH_BUFFER_MAX, 16_384,
            "prefetch-buffer ceiling must be raised to 16384"
        );

        for n in [8_000_usize, 12_000, 16_000, 16_384] {
            let config = cdc_config_from_params(&std::collections::HashMap::from([
                ("cdc_max_coalesced_envelopes".to_string(), n.to_string()),
                ("cdc_prefetch_buffer".to_string(), n.to_string()),
            ]));

            assert_eq!(
                config.max_coalesced_envelopes, n,
                "cdc_max_coalesced_envelopes={n} (above the old 4096 cap, within the new 16384 ceiling) must be accepted verbatim, not clipped"
            );
            assert!(
                config.max_coalesced_envelopes > 4_096,
                "the configured burst {n} must NOT be silently clipped back to the old 4096 cap"
            );
            assert_ne!(
                config.max_coalesced_envelopes, CDC_MAX_COALESCED_ENVELOPES_DEFAULT,
                "an in-range burst {n} must NOT silently fall back to the 256 default"
            );

            assert_eq!(
                config.prefetch_buffer, n,
                "cdc_prefetch_buffer={n} (within the new 16384 ceiling) must be accepted verbatim"
            );
            assert!(
                config.prefetch_buffer > 1_024,
                "the configured prefetch {n} must NOT be silently clipped back to the old 1024 cap"
            );
        }
    }

    /// A burst configuration ABOVE the new 16384 ceiling is out of range and must
    /// fall back to the default (256) — it must NOT be silently clamped to the new
    /// max, nor to the old 4096 cap. (Guarded so a `SPICE_CDC_*` env override in
    /// the test environment, which `resolve_cdc_param` consults on fallback,
    /// doesn't make this flaky.)
    #[test]
    fn cdc_config_from_params_rejects_burst_above_new_16384_ceiling() {
        let env_overridden = std::env::var("SPICE_CDC_MAX_COALESCED_ENVELOPES").is_ok();
        if env_overridden {
            return;
        }
        let over = CDC_MAX_COALESCED_ENVELOPES_MAX + 1;
        let config = cdc_config_from_params(&std::collections::HashMap::from([(
            "cdc_max_coalesced_envelopes".to_string(),
            over.to_string(),
        )]));

        assert_eq!(
            config.max_coalesced_envelopes, CDC_MAX_COALESCED_ENVELOPES_DEFAULT,
            "an out-of-range burst {over} must fall back to the default, not be clamped"
        );
        assert_ne!(
            config.max_coalesced_envelopes, 4_096,
            "out-of-range fallback must not resurrect the old 4096 cap"
        );
    }

    #[test]
    fn cdc_config_overlay_dataset_beats_global_for_known_keys() {
        let base = CdcConfig {
            prefetch_buffer: 4096,
            max_coalesced_envelopes: 8000,
            max_coalesced_bytes: 64 * 1024 * 1024,
            max_coalesce_age_ms: 250,
            commit_timeout: Duration::from_secs(30),
            delete_subbatch_max: CDC_DELETE_SUBBATCH_MAX_DEFAULT,
        };
        let overlaid = cdc_config_overlay(
            base,
            &std::collections::HashMap::from([
                ("cdc_max_coalesce_age_ms".to_string(), "4000".to_string()),
                ("cdc_prefetch_buffer".to_string(), "1024".to_string()),
            ]),
        );

        // overridden
        assert_eq!(overlaid.max_coalesce_age_ms, 4000);
        assert_eq!(overlaid.prefetch_buffer, 1024);
        // untouched
        assert_eq!(
            overlaid.max_coalesced_envelopes,
            base.max_coalesced_envelopes
        );
        assert_eq!(overlaid.max_coalesced_bytes, base.max_coalesced_bytes);
        assert_eq!(overlaid.commit_timeout, base.commit_timeout);
    }

    #[test]
    fn cdc_config_overlay_empty_params_returns_base() {
        let base = CdcConfig::default();
        let overlaid = cdc_config_overlay(base, &std::collections::HashMap::new());
        assert_eq!(overlaid, base);
    }

    #[test]
    fn cdc_config_overlay_keeps_base_on_unparseable_value() {
        let base = CdcConfig {
            prefetch_buffer: 4096,
            ..CdcConfig::default()
        };
        let overlaid = cdc_config_overlay(
            base,
            &std::collections::HashMap::from([(
                "cdc_prefetch_buffer".to_string(),
                "not-a-number".to_string(),
            )]),
        );
        assert_eq!(
            overlaid.prefetch_buffer, base.prefetch_buffer,
            "unparseable dataset value must fall back to the global value, not the built-in default"
        );
    }

    #[test]
    fn cdc_config_overlay_keeps_base_on_out_of_range_value() {
        let base = CdcConfig {
            max_coalesced_envelopes: 8000,
            ..CdcConfig::default()
        };
        let over = CDC_MAX_COALESCED_ENVELOPES_MAX + 1;
        let overlaid = cdc_config_overlay(
            base,
            &std::collections::HashMap::from([(
                "cdc_max_coalesced_envelopes".to_string(),
                over.to_string(),
            )]),
        );
        assert_eq!(
            overlaid.max_coalesced_envelopes, base.max_coalesced_envelopes,
            "out-of-range dataset value must fall back to the global value, not be clamped"
        );
    }

    #[test]
    fn extract_cdc_param_overrides_filters_to_known_keys_only() {
        let extracted = extract_cdc_param_overrides(&std::collections::HashMap::from([
            ("cdc_max_coalesce_age_ms".to_string(), "4000".to_string()),
            ("unrelated_param".to_string(), "value".to_string()),
            ("cdc_prefetch_buffer".to_string(), "1024".to_string()),
        ]))
        .expect("non-empty cdc_* keys must return Some");

        assert_eq!(extracted.len(), 2);
        assert_eq!(
            extracted.get("cdc_max_coalesce_age_ms"),
            Some(&"4000".to_string())
        );
        assert_eq!(
            extracted.get("cdc_prefetch_buffer"),
            Some(&"1024".to_string())
        );
        assert!(!extracted.contains_key("unrelated_param"));
    }

    #[test]
    fn extract_cdc_param_overrides_returns_none_when_no_cdc_keys_present() {
        let extracted = extract_cdc_param_overrides(&std::collections::HashMap::from([(
            "unrelated_param".to_string(),
            "value".to_string(),
        )]));
        assert!(extracted.is_none(), "no recognized keys must return None");
    }

    // The refusal stops a CDC dataset dead, so its wording is the operator's only account
    // of what happened and what to do. Assert the load-bearing parts rather than the shape:
    // the dataset name (quoted — it is the user's string), the reason, and BOTH recovery
    // arms. Dropping the `file_update` arm was a real defect: `recreates_on_schema_mismatch`
    // makes a restart rebuild the table in that mode, and telling those operators a restart
    // will not help sends them to unnecessary manual recovery.
    #[cfg(not(windows))]
    #[test]
    fn partitioned_widening_refusal_names_both_recovery_arms() {
        let msg = partitioned_widening_refusal("sales.orders", "column `total` widened i32 -> i64");

        assert!(
            msg.contains("'sales.orders'"),
            "the dataset must be named and quoted: {msg}"
        );
        assert!(
            msg.contains("column `total` widened i32 -> i64"),
            "the refused change must be described: {msg}"
        );
        assert!(
            msg.contains("the source keeps its position"),
            "the operator has to know the change was not lost: {msg}"
        );
        // Load-bearing, not reassurance: the refusal is preflighted over the whole
        // burst, so an operator reading this can act on an intact table. While it was
        // raised from the first upsert instead, a `DELETE k, INSERT k` burst had already
        // committed its delete by the time the message appeared, and this sentence would
        // have been false exactly when it mattered most (#13455).
        assert!(
            msg.contains("No part of the batch was applied")
                && msg.contains("still holds every row it held before it"),
            "the operator has to know the acceleration was left intact, which is what makes \
             'the source keeps its position' a safe outcome rather than a partial mutation: {msg}"
        );
        assert!(
            msg.contains("`mode: file_update`")
                && msg.contains("`mode: file_create`")
                && msg.contains("`mode: memory`")
                && msg.contains("restart Spice to apply it"),
            "restart is the cheapest remedy in every mode that does not reopen a stored table, and \
             all three must be offered it — `file_update` recreates, `file_create` starts from an \
             empty directory, `memory` never persisted one: {msg}"
        );
        // The restart arm sits two sentences after "still holds every row it held before it".
        // Under the two modes that are ephemeral to replication that promise and that remedy
        // disagree whenever the source's initial snapshot is disabled: the acceleration boots
        // empty and no snapshot replays the history. Naming the cost is what keeps the
        // reassurance honest, so it is asserted rather than left to review.
        assert!(
            msg.contains("reloads the rows only where the source can replay them")
                && msg.contains("set it to `always` before restarting"),
            "a restart rebuilds the schema but not necessarily the rows, so the message must name \
             that cost and the setting that fixes it rather than let the 'still holds every row' \
             reassurance imply the rows return unconditionally: {msg}"
        );
        // Both arms, because this formatter serves the generic `refresh_mode: changes` apply path
        // and the refusal is connector-blind: it keys on `CayenneWriteTarget::Partitioned`, a
        // property of the acceleration, so every changes-capable source reaches this one string.
        // Only half of them declare the setting — `connector-postgres`, `connector-mysql` and
        // `connector-dynamodb` do; Debezium, MongoDB and `cdc_ingest` declare no initial-snapshot
        // parameter at all. Naming the setting alone sent that second half to a key their
        // connector does not have, having just promised their history would come back.
        assert!(
            msg.contains("`pg_replication_initial_snapshot`")
                && msg.contains("`mysql_replication_initial_snapshot`")
                && msg.contains("`dynamodb_replication_initial_snapshot`"),
            "the setting must be named for exactly the connectors that declare it, and spelled out \
             — a shared-suffix abbreviation reads as though `pg_` and `mysql_` were whole keys: \
             {msg}"
        );
        assert!(
            msg.contains("Debezium, MongoDB and `cdc_ingest` have no such setting"),
            "the sources with no initial-snapshot setting must be told so, and told what restoring \
             their history actually takes, rather than left to look for a key they do not have: \
             {msg}"
        );
        assert!(
            msg.contains("replaying the source from an earlier position or")
                && msg.contains("reloading the dataset with a full refresh"),
            "the no-setting arm needs a remedy, not just the bad news: {msg}"
        );
        // No mode may be advertised as row-safe without the snapshot, and the carve-out is not the
        // same on every connector. `file_update` is classified persistent, so `auto` forces no
        // resume snapshot for it while its recreate still empties the table (#13546) — and only
        // `connector-postgres` forces one for the ephemeral modes at all, so on MySQL and DynamoDB
        // `auto` leaves every mode holding later changes only. Asserting one arm alone let the
        // message read as though `auto` were row-safe for `memory` and `file_create` everywhere.
        assert!(
            msg.contains("skips the snapshot under `mode: file_update` on PostgreSQL")
                && msg.contains("skips it under every mode on MySQL and DynamoDB"),
            "`file_update` is not row-safe under `auto` on PostgreSQL, and no mode is on MySQL or \
             DynamoDB, so the message must carve out neither: {msg}"
        );
        assert!(
            msg.contains("Under `mode: file` a restart reopens the stored table and refuses again"),
            "`mode: file` is the only mode a restart does not repair, and it must be told so or the \
             operator loops; naming the others here instead would send them to a needless drop and \
             recreate: {msg}"
        );
        assert!(
            msg.contains("drop and recreate the dataset"),
            "the manual remedy must survive alongside the restart one: {msg}"
        );
        assert!(
            msg.contains("Removing `partition_by` on its own does not recover it"),
            "removing the setting alone opens the unpartitioned parent table while the rows stay in \
             the partition children, and nothing recreates the acceleration, so the message must not \
             offer it as a remedy by itself: {msg}"
        );
        assert!(
            msg.contains("https://spiceai.org/docs/components/data-accelerators/cayenne"),
            "user-facing errors carry a docs link: {msg}"
        );
        assert!(
            !msg.contains('\n'),
            "log/error messages stay on one line: {msg}"
        );
    }

    #[test]
    fn bounded_warning_keys_eviction_allows_rewarning_old_keys() {
        let mut warning_keys = BoundedWarningKeys::default();

        assert!(warning_keys.insert_new("dataset_a".to_string(), 2));
        assert!(!warning_keys.insert_new("dataset_a".to_string(), 2));
        assert!(warning_keys.insert_new("dataset_b".to_string(), 2));
        assert!(warning_keys.insert_new("dataset_c".to_string(), 2));

        assert_eq!(warning_keys.seen.len(), 2);
        assert!(!warning_keys.insert_new("dataset_c".to_string(), 2));
        assert!(warning_keys.insert_new("dataset_a".to_string(), 2));
    }

    fn create_test_data_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ])
    }

    fn create_test_change_batch(
        ops: Vec<&str>,
        primary_keys: &[Vec<&str>],
        ids: Vec<i32>,
        names: Vec<Option<&str>>,
    ) -> ChangeBatch {
        assert_eq!(
            ops.len(),
            primary_keys.len(),
            "ops and primary_keys must have same length"
        );
        assert_eq!(ops.len(), ids.len(), "ops and ids must have same length");
        assert_eq!(
            ops.len(),
            names.len(),
            "ops and names must have same length"
        );

        let data_schema = create_test_data_schema();
        let schema = changes_schema(&data_schema);

        // Create op column
        let op_array: ArrayRef = Arc::new(StringArray::from(ops));

        // Create primary_keys column (List of Strings)
        let mut pk_offsets = vec![0i32];
        let mut pk_values = Vec::new();

        for pk_vec in primary_keys {
            for &pk in pk_vec {
                pk_values.push(pk);
            }
            pk_offsets.push(
                pk_offsets.last().expect("offsets should not be empty")
                    + i32::try_from(pk_vec.len()).expect("pk_vec.len() fits in i32"),
            );
        }

        let pk_values_array = StringArray::from(pk_values);
        let pk_field = Arc::new(Field::new("item", DataType::Utf8, false));
        let pk_array: ArrayRef = Arc::new(
            ListArray::try_new(
                pk_field,
                arrow::buffer::OffsetBuffer::new(pk_offsets.into()),
                Arc::new(pk_values_array),
                None,
            )
            .expect("Failed to create ListArray"),
        );

        // Create data column (Struct)
        let id_array: ArrayRef = Arc::new(Int32Array::from(ids));
        let name_array: ArrayRef = Arc::new(StringArray::from(names));

        let data_fields = vec![
            (Arc::new(Field::new("id", DataType::Int32, false)), id_array),
            (
                Arc::new(Field::new("name", DataType::Utf8, true)),
                name_array,
            ),
        ];
        let data_array: ArrayRef = Arc::new(StructArray::from(data_fields));

        let record = RecordBatch::try_new(Arc::new(schema), vec![op_array, pk_array, data_array])
            .expect("Failed to create RecordBatch");

        ChangeBatch::try_new(record).expect("Failed to create ChangeBatch")
    }

    /// A coalesced burst keeps the newest source-commit timestamp of its
    /// constituents. Without it every multi-envelope burst reached the
    /// accelerator with `None`, so Cayenne's replication-lag goal read nothing
    /// (`cayenne_ingest_replication_lag_seconds` had no series in eight 3-node
    /// SF-1 lab arms on 2026-09-27, while the runtime's own
    /// `dataset_acceleration_cdc_received_commit_unix_time_ms`, computed from
    /// the envelopes before concatenation, was populated) and its freshness goal
    /// fell back to a wall-clock age.
    #[test]
    fn concat_change_batches_keeps_the_newest_source_commit_ts() {
        let older = create_test_change_batch(vec!["c"], &[vec!["1"]], vec![1], vec![Some("a")])
            .with_source_commit_ts_ms(Some(1_700_000_000_000));
        let newest = create_test_change_batch(vec!["u"], &[vec!["2"]], vec![2], vec![Some("b")])
            .with_source_commit_ts_ms(Some(1_700_000_005_000));
        let unstamped = create_test_change_batch(vec!["d"], &[vec!["3"]], vec![3], vec![None]);
        // A zero-row envelope that survived the no-op-heartbeat retain (it rides a
        // real committer) is not evidence of received data, so its newer stamp
        // must not advance the coalesced batch's timestamp — the same exclusion
        // the runtime's received/applied frontier applies.
        let zero_row = create_test_change_batch(vec![], &[], vec![], vec![])
            .with_source_commit_ts_ms(Some(1_700_000_099_000));
        assert!(zero_row.is_heartbeat());
        // Arrival order is not commit order: the last row-bearing constituent
        // carries an OLDER stamp than an earlier one, so taking the last stamp
        // (rather than the max) would be wrong.
        let oldest_last =
            create_test_change_batch(vec!["c"], &[vec!["4"]], vec![4], vec![Some("d")])
                .with_source_commit_ts_ms(Some(1_699_999_000_000));

        let combined = concat_change_batches(&[older, newest, unstamped, zero_row, oldest_last])
            .expect("concat");
        assert_eq!(combined.record.num_rows(), 4, "every row is carried");
        let data = combined.data_batch();
        let ids = data
            .column_by_name("id")
            .and_then(|column| column.as_any().downcast_ref::<Int32Array>())
            .expect("id column is Int32")
            .values()
            .to_vec();
        assert_eq!(ids, vec![1, 2, 3, 4], "rows keep their arrival order");
        assert_eq!(
            combined.source_commit_ts_ms(),
            Some(1_700_000_005_000),
            "the coalesced batch carries the newest row-bearing constituent commit timestamp"
        );

        // A burst with no stamped constituent stays unstamped: no lag information.
        let a = create_test_change_batch(vec!["c"], &[vec!["1"]], vec![1], vec![Some("a")]);
        let b = create_test_change_batch(vec!["c"], &[vec!["2"]], vec![2], vec![Some("b")]);
        assert_eq!(
            concat_change_batches(&[a, b])
                .expect("concat")
                .source_commit_ts_ms(),
            None
        );
    }

    #[test]
    fn test_empty_batch() {
        let change_batch = create_test_change_batch(vec![], &[], vec![], vec![]);

        let result = group_into_sub_batches(&change_batch);

        assert!(result.is_empty(), "Empty batch should return empty vector");
    }

    #[test]
    fn build_pk_only_batch_projects_just_the_key_columns() {
        let change_batch = create_test_change_batch(
            vec!["d", "d"],
            &[vec!["id"], vec!["id"]],
            vec![1, 2],
            vec![Some("Alice"), Some("Bob")],
        );

        let keys = build_pk_only_batch_from_change_batch(&change_batch, &[0, 1])
            .expect("should not error")
            .expect("keyed rows produce a batch");

        assert_eq!(
            keys.num_columns(),
            1,
            "only the 'id' key column, not 'name'"
        );
        assert_eq!(keys.schema().field(0).name(), "id");
        let id_col = keys
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id column is Int32");
        assert_eq!(id_col.values(), &[1, 2]);
    }

    #[test]
    fn build_pk_only_batch_selects_requested_rows_only() {
        let change_batch = create_test_change_batch(
            vec!["d", "d", "d"],
            &[vec!["id"], vec!["id"], vec!["id"]],
            vec![10, 20, 30],
            vec![Some("A"), Some("B"), Some("C")],
        );

        let keys = build_pk_only_batch_from_change_batch(&change_batch, &[0, 2])
            .expect("should not error")
            .expect("keyed rows produce a batch");

        let id_col = keys
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id column is Int32");
        assert_eq!(id_col.values(), &[10, 30]);
    }

    #[test]
    fn build_pk_only_batch_empty_row_indices_returns_none() {
        let change_batch =
            create_test_change_batch(vec!["d"], &[vec!["id"]], vec![1], vec![Some("Alice")]);

        let result =
            build_pk_only_batch_from_change_batch(&change_batch, &[]).expect("should not error");
        assert!(result.is_none());
    }

    #[test]
    fn test_single_row() {
        let change_batch =
            create_test_change_batch(vec!["c"], &[vec!["id"]], vec![1], vec![Some("Alice")]);

        let result = group_into_sub_batches(&change_batch);

        assert_eq!(result.len(), 1, "Should have one sub-batch");
        assert_eq!(result[0].0, ChangeOperationType::Upsert);
        assert_eq!(result[0].1, vec![0]);
    }

    #[test]
    fn test_same_operation_different_primary_keys() {
        let change_batch = create_test_change_batch(
            vec!["c", "c", "c"],
            &[vec!["id"], vec!["id"], vec!["id"]],
            vec![1, 2, 3],
            vec![Some("Alice"), Some("Bob"), Some("Charlie")],
        );

        let result = group_into_sub_batches(&change_batch);

        assert_eq!(
            result.len(),
            1,
            "Should have one sub-batch for same operation type with different keys"
        );
        assert_eq!(result[0].0, ChangeOperationType::Upsert);
        assert_eq!(result[0].1, vec![0, 1, 2]);
    }

    #[test]
    fn test_different_operation_types_no_pk_conflict_merges() {
        // U(pk1), D(pk2), U(pk3) — no PK conflicts across buckets,
        // so upserts merge and deletes merge.
        let change_batch = create_test_change_batch(
            vec!["c", "d", "c"],
            &[vec!["id"], vec!["id"], vec!["id"]],
            vec![1, 2, 3],
            vec![Some("Alice"), Some("Bob"), Some("Charlie")],
        );

        let result = group_into_sub_batches(&change_batch);

        assert_eq!(
            result.len(),
            2,
            "Non-conflicting ops should merge into 2 batches"
        );

        assert_eq!(result[0].0, ChangeOperationType::Upsert);
        assert_eq!(result[0].1, vec![0, 2]);

        assert_eq!(result[1].0, ChangeOperationType::Delete);
        assert_eq!(result[1].1, vec![1]);
    }

    #[test]
    fn test_duplicate_primary_key_replaces_in_place() {
        let change_batch = create_test_change_batch(
            vec!["c", "c", "c"],
            &[vec!["id"], vec!["id"], vec!["id"]],
            vec![1, 1, 2], // First two rows have same id value
            vec![Some("Alice"), Some("Alice_v2"), Some("Bob")],
        );

        let result = group_into_sub_batches(&change_batch);

        // Last-write-wins: row 0 (pk1,v1) is replaced by row 1 (pk1,v2)
        // within the same upsert bucket, so only one sub-batch remains.
        assert_eq!(
            result.len(),
            1,
            "Same-bucket PK collision should replace, not split"
        );

        assert_eq!(result[0].0, ChangeOperationType::Upsert);
        assert_eq!(result[0].1, vec![1, 2]);
    }

    #[test]
    fn test_upsert_operations_grouped_together() {
        // create, update, and read should all map to Upsert
        let change_batch = create_test_change_batch(
            vec!["c", "u", "r"],
            &[vec!["id"], vec!["id"], vec!["id"]],
            vec![1, 2, 3],
            vec![Some("A"), Some("B"), Some("C")],
        );

        let result = group_into_sub_batches(&change_batch);

        assert_eq!(
            result.len(),
            1,
            "Create, update, and read should be grouped as Upsert"
        );
        assert_eq!(result[0].0, ChangeOperationType::Upsert);
        assert_eq!(result[0].1, vec![0, 1, 2]);
    }

    #[test]
    fn test_all_operation_types() {
        let change_batch = create_test_change_batch(
            vec!["c", "u", "r", "d", "t"],
            &[vec!["id"], vec!["id"], vec!["id"], vec!["id"], vec!["id"]],
            vec![1, 2, 3, 4, 5],
            vec![Some("A"), Some("B"), Some("C"), Some("D"), Some("E")],
        );

        let result = group_into_sub_batches(&change_batch);

        assert_eq!(
            result.len(),
            3,
            "Should have 3 sub-batches: Upsert, Delete, Truncate"
        );

        assert_eq!(result[0].0, ChangeOperationType::Upsert);
        assert_eq!(result[0].1, vec![0, 1, 2]);

        assert_eq!(result[1].0, ChangeOperationType::Delete);
        assert_eq!(result[1].1, vec![3]);

        assert_eq!(result[2].0, ChangeOperationType::Truncate);
        assert_eq!(result[2].1, vec![4]);
    }

    #[test]
    fn test_multiple_duplicate_keys_in_sequence() {
        let change_batch = create_test_change_batch(
            vec!["c", "c", "c", "c"],
            &[vec!["id"], vec!["id"], vec!["id"], vec!["id"]],
            vec![1, 1, 2, 1],
            vec![Some("A"), Some("A2"), Some("B"), Some("A3")],
        );

        let result = group_into_sub_batches(&change_batch);

        // Last-write-wins: pk1 appears at rows 0, 1, 3 — each successive
        // occurrence replaces the previous in-place. pk2 at row 2 is kept.
        // The final bucket is ordered by row index to preserve contiguous-slice fast paths.
        assert_eq!(result.len(), 1);

        assert_eq!(result[0].0, ChangeOperationType::Upsert);
        assert_eq!(result[0].1, vec![2, 3]);
        assert_eq!(contiguous_row_span(&result[0].1), Some((2, 2)));
    }

    #[test]
    fn test_composite_primary_keys() {
        let change_batch = create_test_change_batch(
            vec!["c", "c", "c"],
            &[vec!["id", "name"], vec!["id", "name"], vec!["id", "name"]],
            vec![1, 2, 1],
            vec![Some("Alice"), Some("Bob"), Some("Alice")],
        );

        let result = group_into_sub_batches(&change_batch);

        // Last-write-wins: composite key (1,"Alice") at row 0 is replaced
        // by row 2. Key (2,"Bob") at row 1 is distinct and kept.
        assert_eq!(
            result.len(),
            1,
            "Same composite key should replace, not split"
        );
        assert_eq!(result[0].0, ChangeOperationType::Upsert);
        assert_eq!(result[0].1, vec![1, 2]);
        assert_eq!(contiguous_row_span(&result[0].1), Some((1, 2)));
    }

    #[test]
    fn test_primary_key_encoding_distinguishes_composite_string_boundaries() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("left", DataType::Utf8, false),
            Field::new("right", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["ab", "a"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["c", "bc"])) as ArrayRef,
            ],
        )
        .expect("record batch should be created");

        let first_key = encode_primary_key(&batch, &[0, 1], 0);
        let second_key = encode_primary_key(&batch, &[0, 1], 1);

        assert_ne!(
            first_key, second_key,
            "composite keys ('ab', 'c') and ('a', 'bc') must not collapse to the same grouping key"
        );
    }

    #[test]
    fn test_alternating_operations_no_pk_conflict_merges() {
        // U(pk1), D(pk2), U(pk3), D(pk4) — all distinct PKs,
        // so upserts merge and deletes merge.
        let change_batch = create_test_change_batch(
            vec!["c", "d", "c", "d"],
            &[vec!["id"], vec!["id"], vec!["id"], vec!["id"]],
            vec![1, 2, 3, 4],
            vec![Some("A"), Some("B"), Some("C"), Some("D")],
        );

        let result = group_into_sub_batches(&change_batch);

        assert_eq!(
            result.len(),
            2,
            "Alternating operations with distinct PKs should merge into 2 batches"
        );

        assert_eq!(result[0].0, ChangeOperationType::Upsert);
        assert_eq!(result[0].1, vec![0, 2]);

        assert_eq!(result[1].0, ChangeOperationType::Delete);
        assert_eq!(result[1].1, vec![1, 3]);
    }

    #[test]
    fn test_cross_op_pk_conflict_flushes_only_conflicting_bucket() {
        // U(pk1), D(pk2), D(pk1) — pk1 conflicts with upserts bucket,
        // so upserts is flushed but deletes keeps accumulating.
        let change_batch = create_test_change_batch(
            vec!["c", "d", "d"],
            &[vec!["id"], vec!["id"], vec!["id"]],
            vec![1, 2, 1],
            vec![Some("A"), Some("B"), Some("A_del")],
        );

        let result = group_into_sub_batches(&change_batch);

        assert_eq!(result.len(), 2, "Should flush upserts, then merge deletes");
        assert_eq!(result[0].0, ChangeOperationType::Upsert);
        assert_eq!(result[0].1, vec![0]);
        assert_eq!(result[1].0, ChangeOperationType::Delete);
        assert_eq!(result[1].1, vec![1, 2]);
    }

    #[test]
    fn test_truncate_barrier_flushes_all_buckets() {
        // U(pk1), D(pk2), T, U(pk3) — truncate flushes both active
        // buckets, emits the truncate row, then a new upsert batch starts.
        let change_batch = create_test_change_batch(
            vec!["c", "d", "t", "c"],
            &[vec!["id"], vec!["id"], vec!["id"], vec!["id"]],
            vec![1, 2, 99, 3],
            vec![Some("A"), Some("B"), Some("T"), Some("C")],
        );

        let result = group_into_sub_batches(&change_batch);

        assert_eq!(result.len(), 4);
        assert_eq!(result[0].0, ChangeOperationType::Upsert);
        assert_eq!(result[0].1, vec![0]);
        assert_eq!(result[1].0, ChangeOperationType::Delete);
        assert_eq!(result[1].1, vec![1]);
        assert_eq!(result[2].0, ChangeOperationType::Truncate);
        assert_eq!(result[2].1, vec![2]);
        assert_eq!(result[3].0, ChangeOperationType::Upsert);
        assert_eq!(result[3].1, vec![3]);
    }

    #[test]
    fn test_same_pk_upsert_then_delete_conflict_forces_flush() {
        // U(pk1), D(pk1) — pk1 is in upserts when delete arrives,
        // so upserts is flushed first, then delete goes to its bucket.
        let change_batch = create_test_change_batch(
            vec!["c", "d"],
            &[vec!["id"], vec!["id"]],
            vec![1, 1],
            vec![Some("A"), Some("A_del")],
        );

        let result = group_into_sub_batches(&change_batch);

        assert_eq!(result.len(), 2, "PK conflict across ops forces flush");
        assert_eq!(result[0].0, ChangeOperationType::Upsert);
        assert_eq!(result[0].1, vec![0]);
        assert_eq!(result[1].0, ChangeOperationType::Delete);
        assert_eq!(result[1].1, vec![1]);
    }

    #[test]
    fn test_last_write_wins_keeps_only_latest_row() {
        // 5 upserts to the same PK — only the last row should survive.
        let change_batch = create_test_change_batch(
            vec!["c", "u", "u", "u", "u"],
            &[vec!["id"], vec!["id"], vec!["id"], vec!["id"], vec!["id"]],
            vec![1, 1, 1, 1, 1],
            vec![Some("v1"), Some("v2"), Some("v3"), Some("v4"), Some("v5")],
        );

        let result = group_into_sub_batches(&change_batch);

        assert_eq!(
            result.len(),
            1,
            "All same-PK upserts should collapse to one batch"
        );
        assert_eq!(result[0].0, ChangeOperationType::Upsert);
        assert_eq!(
            result[0].1,
            vec![4],
            "Only the last row (index 4) should survive"
        );
    }

    #[test]
    fn test_last_write_wins_cross_bucket_still_flushes() {
        // U(pk1), D(pk2), U(pk1) — the second U(pk1) replaces the first
        // within the upsert bucket (no cross-bucket conflict for pk1 in
        // deletes). D(pk2) stays in its own bucket.
        let change_batch = create_test_change_batch(
            vec!["c", "d", "u"],
            &[vec!["id"], vec!["id"], vec!["id"]],
            vec![1, 2, 1],
            vec![Some("A"), Some("B"), Some("A_v2")],
        );

        let result = group_into_sub_batches(&change_batch);

        // pk1 never appears in the delete bucket, so no cross-bucket flush.
        // Same-bucket replace: row 0 replaced by row 2 for pk1.
        assert_eq!(result.len(), 2, "Upsert bucket (deduped) + delete bucket");
        assert_eq!(result[0].0, ChangeOperationType::Upsert);
        assert_eq!(result[0].1, vec![2]);
        assert_eq!(result[1].0, ChangeOperationType::Delete);
        assert_eq!(result[1].1, vec![1]);
    }

    #[test]
    fn test_full_pk_lifecycle_upsert_delete_upsert() {
        // U(pk1) → D(pk1) → U(pk1) — row created, deleted, re-created.
        // Two consecutive cross-bucket flushes for the same PK.
        let change_batch = create_test_change_batch(
            vec!["c", "d", "c"],
            &[vec!["id"], vec!["id"], vec!["id"]],
            vec![1, 1, 1],
            vec![Some("v1"), Some("v1_del"), Some("v2")],
        );

        let result = group_into_sub_batches(&change_batch);

        assert_eq!(
            result.len(),
            3,
            "Full lifecycle needs 3 ordered sub-batches"
        );
        assert_eq!(result[0].0, ChangeOperationType::Upsert);
        assert_eq!(result[0].1, vec![0]);
        assert_eq!(result[1].0, ChangeOperationType::Delete);
        assert_eq!(result[1].1, vec![1]);
        assert_eq!(result[2].0, ChangeOperationType::Upsert);
        assert_eq!(result[2].1, vec![2]);
    }

    #[test]
    fn test_truncate_resets_dedup_state() {
        // U(pk1,v1), U(pk1,v2), T, U(pk1,v3), U(pk1,v4) — dedup works
        // independently on each side of the truncate barrier. The post-
        // truncate pk1 must not collide with the pre-truncate pk1.
        let change_batch = create_test_change_batch(
            vec!["c", "u", "t", "c", "u"],
            &[vec!["id"], vec!["id"], vec!["id"], vec!["id"], vec!["id"]],
            vec![1, 1, 99, 1, 1],
            vec![Some("v1"), Some("v2"), Some("T"), Some("v3"), Some("v4")],
        );

        let result = group_into_sub_batches(&change_batch);

        assert_eq!(
            result.len(),
            3,
            "Deduped upsert + truncate + deduped upsert"
        );
        assert_eq!(result[0].0, ChangeOperationType::Upsert);
        assert_eq!(
            result[0].1,
            vec![1],
            "Pre-truncate: only v2 survives (last-write-wins)"
        );
        assert_eq!(result[1].0, ChangeOperationType::Truncate);
        assert_eq!(result[1].1, vec![2]);
        assert_eq!(result[2].0, ChangeOperationType::Upsert);
        assert_eq!(
            result[2].1,
            vec![4],
            "Post-truncate: only v4 survives (last-write-wins)"
        );
    }

    fn make_mem_table() -> Arc<MemTable> {
        let schema = Arc::new(create_test_data_schema());
        Arc::new(MemTable::try_new(schema, vec![vec![]]).expect("mem table should be created"))
    }

    fn make_refresh_task(accelerator: Arc<dyn TableProvider>) -> RefreshTask {
        make_refresh_task_named("test", accelerator)
    }

    /// `make_refresh_task` with an explicit dataset name. Tests that install
    /// process-global per-dataset state (the CDC schema-evolution registry)
    /// must use a unique name so they don't change the behavior of other
    /// tests running concurrently against the shared "test" dataset.
    fn make_refresh_task_named(name: &str, accelerator: Arc<dyn TableProvider>) -> RefreshTask {
        use crate::accelerated::refresh_task::RefreshTaskBuilder;
        use crate::federated::FederatedTable;
        use tokio::runtime::Handle;
        use tokio::sync::Mutex;

        let federated = Arc::new(FederatedTable::new_unchecked(Arc::clone(&accelerator)));
        RefreshTaskBuilder::new(
            runtime_status::RuntimeStatus::new(),
            datafusion::common::TableReference::bare(name.to_string()),
            federated,
            None,
            accelerator,
            Handle::current(),
            Arc::new(Mutex::new(())),
        )
        .build()
    }

    fn make_refresh_task_with_source(
        name: &str,
        federated: Arc<dyn TableProvider>,
        accelerator: Arc<dyn TableProvider>,
    ) -> RefreshTask {
        use crate::accelerated::refresh_task::RefreshTaskBuilder;
        use crate::federated::FederatedTable;
        use tokio::runtime::Handle;
        use tokio::sync::Mutex;

        let federated = Arc::new(FederatedTable::new_unchecked(federated));
        RefreshTaskBuilder::new(
            runtime_status::RuntimeStatus::new(),
            datafusion::common::TableReference::bare(name.to_string()),
            federated,
            None,
            accelerator,
            Handle::current(),
            Arc::new(Mutex::new(())),
        )
        .build()
    }

    fn id_name_batch(ids: &[i32], names: &[&str]) -> RecordBatch {
        let schema = Arc::new(create_test_data_schema());
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(
                    names.iter().map(|name| Some(*name)).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("id/name batch should build")
    }

    async fn names_in_table(table: Arc<dyn TableProvider>) -> Vec<String> {
        let ctx = SessionContext::new();
        let batches = ctx
            .read_table(table)
            .expect("read table")
            .collect()
            .await
            .expect("collect table");
        batches
            .iter()
            .flat_map(|batch| {
                let names = batch
                    .column_by_name("name")
                    .expect("name")
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("name is Utf8");
                (0..names.len()).map(|i| names.value(i).to_string())
            })
            .collect()
    }

    /// `make_refresh_task` with per-dataset `cdc_*` param overrides applied, so
    /// a test can pin `cdc_delete_subbatch_max` regardless of the process-global
    /// [`CdcConfig`].
    fn make_refresh_task_with_cdc_params(
        accelerator: Arc<dyn TableProvider>,
        cdc_params: std::collections::HashMap<String, String>,
    ) -> RefreshTask {
        use crate::accelerated::refresh_task::RefreshTaskBuilder;
        use crate::federated::FederatedTable;
        use tokio::runtime::Handle;
        use tokio::sync::Mutex;

        let federated = Arc::new(FederatedTable::new_unchecked(Arc::clone(&accelerator)));
        RefreshTaskBuilder::new(
            runtime_status::RuntimeStatus::new(),
            datafusion::common::TableReference::bare("test".to_string()),
            federated,
            None,
            accelerator,
            Handle::current(),
            Arc::new(Mutex::new(())),
        )
        .with_cdc_param_overrides(Some(Arc::new(cdc_params)))
        .build()
    }

    /// Regression test for #13014, CDC leg. `DuckDB` stores every timezone-aware
    /// timestamp at microsecond precision, so a Postgres `timestamptz` CDC stream
    /// arrives as `Timestamp(ns, "UTC")` against a `Timestamp(us, "UTC")` accelerated
    /// table forever. `classify` reads that as `Incompatible`, so before the engine's
    /// own rewrites were consulted here, `on_schema_change: fail` rejected the first
    /// batch and stopped replication for a schema that never changed.
    #[tokio::test]
    async fn cdc_schema_evolution_accepts_an_engine_required_timestamp_rewrite() {
        use crate::accelerated::refresh_task::RefreshTaskBuilder;
        use crate::federated::FederatedTable;
        use arrow::datatypes::TimeUnit;
        use cayenne::CAYENNE_TYPE_REWRITE_RULES;

        /// `DuckDB`'s normalization of a timezone-aware timestamp to microseconds.
        /// Spelled out rather than imported because the `DuckDB` accelerator sits
        /// above this crate.
        static DUCKDB_LIKE_RULES: arrow_tools::type_rewrite::TypeRewriteRules =
            &[&arrow_tools::type_rewrite::TimestampTzToMicrosecond];

        let stored = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]));
        let incoming = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
        ]));

        let build = |name: &str, rules: arrow_tools::type_rewrite::TypeRewriteRules| {
            let accelerator: Arc<dyn TableProvider> = Arc::new(
                MemTable::try_new(Arc::clone(&stored), vec![vec![]])
                    .expect("mem table should be created"),
            );
            let dataset = datafusion::common::TableReference::bare(name.to_string());
            install_cdc_schema_evolution(
                &dataset,
                CdcSchemaEvolution {
                    policy: OnSchemaChange::Fail,
                    constraint_columns: vec![],
                },
            );
            let federated = Arc::new(FederatedTable::new_unchecked(Arc::clone(&accelerator)));
            RefreshTaskBuilder::new(
                runtime_status::RuntimeStatus::new(),
                dataset,
                federated,
                None,
                accelerator,
                tokio::runtime::Handle::current(),
                Arc::new(tokio::sync::Mutex::new(())),
            )
            .with_engine_type_rewrites(rules)
            .build()
        };

        let task = build("cdc_engine_rewrite_accepted", DUCKDB_LIKE_RULES);
        task.maybe_evolve_schema_for_cdc(&incoming).await.expect(
            "an engine-required rewrite is not a schema change and must not fail the write",
        );

        // The upgrade case for #13018. A Cayenne table created before the engine
        // preserved timestamp units stores microseconds; Cayenne now creates such a
        // column as nanoseconds, but this table's stored type does not change. Its
        // rules must still explain that, or upgrading stops replication on the first
        // batch of an unchanged Postgres `timestamptz` stream. `classify` cannot save
        // it: nanosecond is excluded as a widening target because rescaling to ns
        // overflows i64 past ~2262, so us -> ns is `Incompatible`, not `Widening`.
        let task = build("cdc_legacy_microsecond_table", CAYENNE_TYPE_REWRITE_RULES);
        task.maybe_evolve_schema_for_cdc(&incoming)
            .await
            .expect("a pre-existing microsecond Cayenne table must keep replicating after upgrade");

        // Neuter: with no engine rules the same pair is classified as incompatible and
        // `on_schema_change: fail` rejects it - so the pass above is the rules working,
        // not a comparison that never saw a difference.
        let task = build("cdc_engine_rewrite_rejected", &[]);
        let Err(e) = task.maybe_evolve_schema_for_cdc(&incoming).await else {
            panic!("expected `on_schema_change: fail` to reject the unnormalized ns -> us change")
        };
        assert!(
            e.to_string().contains("incompatible schema change"),
            "unexpected error: {e}"
        );
    }

    /// Regression test for #13549, CDC leg. A source that declares a `MAP`'s `entries` field
    /// nullable — which the Arrow map layout forbids — is not reporting a schema change, so
    /// `on_schema_change: fail` must not reject the batch. The surrounding `Map` pair is
    /// otherwise identical, so the entries flag is the only thing under test.
    #[tokio::test]
    async fn cdc_schema_evolution_accepts_a_nonconforming_map_entries_declaration() {
        let map_of = |entries_nullable: bool| {
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("keys", DataType::Utf8, false),
                            Field::new("values", DataType::Utf8, true),
                        ]
                        .into(),
                    ),
                    entries_nullable,
                )),
                false,
            )
        };
        let schema_with = |entries_nullable: bool| {
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("headers", map_of(entries_nullable), true),
            ]))
        };

        let dataset = "cdc_map_entries_accepted";
        install_cdc_schema_evolution(
            &datafusion::common::TableReference::bare(dataset.to_string()),
            CdcSchemaEvolution {
                policy: OnSchemaChange::Fail,
                constraint_columns: vec![],
            },
        );
        let task = make_refresh_task_named(
            dataset,
            Arc::new(MemTable::try_new(schema_with(false), vec![vec![]]).expect("mem table")),
        );

        task.maybe_evolve_schema_for_cdc(&schema_with(true))
            .await
            .expect(
                "an entries declaration the Arrow map layout forbids is not a schema change and must not fail the write",
            );

        // Control: a genuinely different map — its values `Utf8` -> `Int64` — reaches
        // the classifier on the same task and `on_schema_change: fail` rejects it, so
        // the pass above is the entries rule (#13549) and not an early return.
        let int_values = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("keys", DataType::Utf8, false),
                        Field::new("values", DataType::Int64, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        let changed = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("headers", int_values, true),
        ]));
        let Err(e) = task.maybe_evolve_schema_for_cdc(&changed).await else {
            panic!("expected `on_schema_change: fail` to reject a map whose value type changed")
        };
        let message = e.to_string();
        assert!(
            message.contains(
                "incompatible schema change detected on the CDC stream for cdc_map_entries_accepted"
            ) && message.contains("The type of `headers` changed from"),
            "unexpected error: {message}"
        );
    }

    #[tokio::test]
    async fn test_write_change_upsert_returns_data_written() {
        let task = make_refresh_task(make_mem_table() as Arc<dyn TableProvider>);
        let change_batch =
            create_test_change_batch(vec!["c"], &[vec!["id"]], vec![1], vec![Some("Alice")]);
        assert_eq!(
            task.write_change(change_batch)
                .await
                .expect("write_change should succeed"),
            WriteChangeResult::DataWritten
        );
    }

    #[tokio::test]
    async fn test_write_change_reuses_cached_insert_plan_for_upserts() {
        let insert_plan_calls = Arc::new(AtomicUsize::new(0));
        let insert_execution_calls = Arc::new(AtomicUsize::new(0));
        let provider = Arc::new(CountingInsertProvider {
            inner: make_mem_table() as Arc<dyn TableProvider>,
            insert_plan_calls: Arc::clone(&insert_plan_calls),
            insert_execution_calls: Arc::clone(&insert_execution_calls),
        });
        let task = make_refresh_task(provider as Arc<dyn TableProvider>);
        let ctx = SessionContext::new();
        let session_state = ctx.state();

        let first_batch =
            create_test_change_batch(vec!["c"], &[vec!["id"]], vec![1], vec![Some("Alice")]);
        let second_batch =
            create_test_change_batch(vec!["c"], &[vec!["id"]], vec![2], vec![Some("Bob")]);

        assert_eq!(
            task.write_change_with_context(first_batch, &ctx, &session_state, Recovery::Durable)
                .await
                .expect("first write_change should succeed")
                .result,
            WriteChangeResult::DataWritten
        );
        assert_eq!(
            task.write_change_with_context(second_batch, &ctx, &session_state, Recovery::Durable)
                .await
                .expect("second write_change should succeed")
                .result,
            WriteChangeResult::DataWritten
        );

        assert_eq!(
            insert_plan_calls.load(AtomicOrdering::SeqCst),
            1,
            "CDC upserts should reuse the cached insert_into plan"
        );
        assert_eq!(
            insert_execution_calls.load(AtomicOrdering::SeqCst),
            2,
            "the cached plan should still be executed once per write"
        );
    }

    #[tokio::test]
    async fn test_write_change_delete_returns_data_written() {
        let task = make_refresh_task(make_mem_table() as Arc<dyn TableProvider>);
        let change_batch =
            create_test_change_batch(vec!["d"], &[vec!["id"]], vec![1], vec![Some("Alice")]);
        assert_eq!(
            task.write_change(change_batch)
                .await
                .expect("write_change should succeed"),
            WriteChangeResult::DataWritten
        );
    }

    #[tokio::test]
    async fn test_empty_returns_no_change() {
        let task = make_refresh_task(make_mem_table() as Arc<dyn TableProvider>);
        // Any unrecognized op string maps to ChangeOperation::Unknown
        let change_batch = create_test_change_batch(vec![], &[], vec![], vec![]);
        assert_eq!(
            task.write_change(change_batch)
                .await
                .expect("write_change should succeed"),
            WriteChangeResult::NoChange
        );
    }

    #[tokio::test]
    async fn test_write_change_mixed_keyed_and_keyless_deletes() {
        let schema = Arc::new(create_test_data_schema());
        let initial = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("keyed"), Some("keyless")])) as ArrayRef,
            ],
        )
        .expect("initial batch should be created");
        let table = Arc::new(
            MemTable::try_new(Arc::clone(&schema), vec![vec![initial]])
                .expect("mem table should be created"),
        );
        let task = make_refresh_task(Arc::clone(&table) as Arc<dyn TableProvider>);

        let change_batch = create_test_change_batch(
            vec!["d", "d"],
            &[vec![], vec!["id"]],
            vec![2, 1],
            vec![Some("keyless"), Some("changed")],
        );

        assert_eq!(
            task.write_change(change_batch)
                .await
                .expect("mixed delete should succeed"),
            WriteChangeResult::DataWritten
        );

        let ctx = SessionContext::new();
        let scan = table
            .scan(&ctx.state(), None, &[], None)
            .await
            .expect("scan should succeed");
        let remaining = collect(scan, ctx.task_ctx())
            .await
            .expect("collect should succeed");
        let remaining_rows: usize = remaining.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(remaining_rows, 0);
    }

    #[tokio::test]
    async fn test_delete_burst_chunks_at_subbatch_cap() {
        // A single Delete sub-batch of N keyed rows must be applied as
        // ⌈N/cap⌉ independent durable `delete_from` plans, and every cap must
        // reach the identical end state (each key deleted exactly once).
        const N: usize = 10;
        let schema = Arc::new(create_test_data_schema());

        for cap in [1usize, 3, 4, 10, 100] {
            let ids: Vec<i32> = (0..i32::try_from(N).expect("N fits in i32")).collect();
            let names: Vec<Option<&str>> = vec![Some("row"); N];
            let initial = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from(ids.clone())) as ArrayRef,
                    Arc::new(StringArray::from(names.clone())) as ArrayRef,
                ],
            )
            .expect("initial batch should be created");
            let mem = Arc::new(
                MemTable::try_new(Arc::clone(&schema), vec![vec![initial]])
                    .expect("mem table should be created"),
            );
            let delete_plan_calls = Arc::new(AtomicUsize::new(0));
            let provider = Arc::new(CountingDeleteProvider {
                inner: Arc::clone(&mem) as Arc<dyn TableProvider>,
                delete_plan_calls: Arc::clone(&delete_plan_calls),
            }) as Arc<dyn TableProvider>;
            let task = make_refresh_task_with_cdc_params(
                provider,
                std::collections::HashMap::from([(
                    "cdc_delete_subbatch_max".to_string(),
                    cap.to_string(),
                )]),
            );

            let ops = vec!["d"; N];
            let pks: Vec<Vec<&str>> = vec![vec!["id"]; N];
            let change_batch = create_test_change_batch(ops, &pks, ids, names);

            assert_eq!(
                task.write_change(change_batch)
                    .await
                    .expect("delete burst should succeed"),
                WriteChangeResult::DataWritten
            );

            let expected_plans = N.div_ceil(cap);
            assert_eq!(
                delete_plan_calls.load(AtomicOrdering::SeqCst),
                expected_plans,
                "N={N} keys with cap={cap} should execute ceil(N/cap)={expected_plans} delete plans"
            );

            let ctx = SessionContext::new();
            let scan = mem
                .scan(&ctx.state(), None, &[], None)
                .await
                .expect("scan should succeed");
            let remaining = collect(scan, ctx.task_ctx())
                .await
                .expect("collect should succeed");
            let remaining_rows: usize = remaining.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(remaining_rows, 0, "cap={cap}: every key should be deleted");
        }
    }

    #[tokio::test]
    async fn test_keyless_delete_unwraps_indexed_provider() {
        let schema = Arc::new(create_test_data_schema());
        let initial = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("row")])) as ArrayRef,
            ],
        )
        .expect("initial batch should be created");
        let table = Arc::new(
            MemTable::try_new(Arc::clone(&schema), vec![vec![initial.clone()]])
                .expect("mem table should be created"),
        );
        let wrapped = SpiceTable::over(
            Arc::new(IndexLayer::new()),
            Arc::clone(&table) as Arc<dyn TableProvider>,
        ) as Arc<dyn TableProvider>;

        let deleted = delete_matching_rows_from_arrow_provider(&wrapped, &initial)
            .await
            .expect("delete should succeed through wrapper");
        assert_eq!(deleted, Some(1));

        let ctx = SessionContext::new();
        let scan = table
            .scan(&ctx.state(), None, &[], None)
            .await
            .expect("scan should succeed");
        let remaining = collect(scan, ctx.task_ctx())
            .await
            .expect("collect should succeed");
        let remaining_rows: usize = remaining.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(remaining_rows, 0);
    }

    #[test]
    fn test_group_into_sub_batches_no_pks_single_batch() {
        let batch = create_test_change_batch(
            vec!["c", "c", "c"],
            &[vec![], vec![], vec![]],
            vec![1, 2, 3],
            vec![Some("a"), Some("b"), Some("c")],
        );

        let result = group_into_sub_batches(&batch);

        // No PKs + all same op → 1 sub-batch with all rows
        assert_eq!(result.len(), 1, "Should produce 1 sub-batch when no PKs");
        assert_eq!(result[0].0, ChangeOperationType::Upsert);
        assert_eq!(result[0].1, vec![0, 1, 2]);
    }

    #[test]
    fn test_group_into_sub_batches_no_pks_mixed_ops() {
        // Mixed ops with no PKs: should split only on op type boundaries
        let ops = vec!["c", "c", "c", "d", "d", "c", "c"];
        let primary_keys: Vec<Vec<&str>> = vec![vec![]; 7];
        let ids = vec![1, 2, 3, 4, 5, 6, 7];
        let names = vec![
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("e"),
            Some("f"),
            Some("g"),
        ];
        let batch = create_test_change_batch(ops, &primary_keys, ids, names);

        let result = group_into_sub_batches(&batch);

        // Should split into 3 groups: [c,c,c], [d,d], [c,c]
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].0, ChangeOperationType::Upsert);
        assert_eq!(result[0].1.len(), 3);
        assert_eq!(result[1].0, ChangeOperationType::Delete);
        assert_eq!(result[1].1.len(), 2);
        assert_eq!(result[2].0, ChangeOperationType::Upsert);
        assert_eq!(result[2].1.len(), 2);
    }

    // ---------------------------------------------------------------------
    // Tests for nullable-schema ChangeBatch handling.
    //
    // Postgres CDC produces ChangeBatches whose `data` struct has all fields
    // promoted to nullable (so DELETE rows with absent non-PK columns can be
    // written without Arrow rejecting nulls in non-nullable fields).
    // `try_cast_to` in `process_upsert_batch` restores the
    // accelerator's original nullability before the write.
    // ---------------------------------------------------------------------

    /// Build a `ChangeBatch` where every field in the `data` struct is
    /// nullable — matching what `build_change_batch` now produces for
    /// Postgres native CDC.
    fn create_nullable_change_batch(
        ops: Vec<&str>,
        primary_keys: &[Vec<&str>],
        ids: Vec<i32>,
        names: Vec<Option<&str>>,
    ) -> ChangeBatch {
        let nullable_data_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]);
        let schema = changes_schema(&nullable_data_schema);

        let op_array: ArrayRef = Arc::new(StringArray::from(ops));

        let mut pk_offsets = vec![0i32];
        let mut pk_values: Vec<&str> = vec![];
        for pk_vec in primary_keys {
            for &pk in pk_vec {
                pk_values.push(pk);
            }
            pk_offsets.push(
                pk_offsets.last().copied().unwrap_or(0)
                    + i32::try_from(pk_vec.len()).expect("fits in i32"),
            );
        }
        let pk_field = Arc::new(Field::new("item", DataType::Utf8, false));
        let pk_array: ArrayRef = Arc::new(
            ListArray::try_new(
                pk_field,
                arrow::buffer::OffsetBuffer::new(pk_offsets.into()),
                Arc::new(StringArray::from(pk_values)),
                None,
            )
            .expect("pk list"),
        );

        let id_array: ArrayRef = Arc::new(Int32Array::from(ids));
        let name_array: ArrayRef = Arc::new(StringArray::from(names));
        let data_array: ArrayRef = Arc::new(StructArray::from(vec![
            (Arc::new(Field::new("id", DataType::Int32, true)), id_array),
            (
                Arc::new(Field::new("name", DataType::Utf8, true)),
                name_array,
            ),
        ]));

        let record = RecordBatch::try_new(Arc::new(schema), vec![op_array, pk_array, data_array])
            .expect("record batch");
        ChangeBatch::try_new(record).expect("change batch")
    }

    /// `try_cast_to` promotes nullable fields to non-nullable
    /// when the target schema declares them as such, and leaves already-
    /// matching fields untouched.
    #[test]
    fn test_coerce_batch_nullability_promotes_fields() {
        // All-nullable source batch (Postgres CDC output style).
        let src_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&src_schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("Alice"), None])) as ArrayRef,
            ],
        )
        .expect("batch");

        // Target: `id` is NOT NULL, `name` is nullable.
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let coerced = try_cast_to(batch, Arc::clone(&target_schema)).expect("coerce");

        assert!(
            !coerced
                .schema()
                .field_with_name("id")
                .expect("id field exists")
                .is_nullable(),
            "id should be promoted to non-nullable"
        );
        assert!(
            coerced
                .schema()
                .field_with_name("name")
                .expect("name field exists")
                .is_nullable(),
            "name should remain nullable"
        );
        assert_eq!(coerced.num_rows(), 2, "row count unchanged");
    }

    /// `try_cast_to` is a no-op when the batch schema already
    /// matches the target nullability.
    #[test]
    fn test_coerce_batch_nullability_no_op_when_already_matches() {
        let schema = Arc::new(create_test_data_schema());
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("Alice")])) as ArrayRef,
            ],
        )
        .expect("batch");

        let coerced = try_cast_to(batch.clone(), Arc::clone(&schema)).expect("coerce");
        assert_eq!(
            coerced.schema(),
            batch.schema(),
            "schema should be identical when already matching"
        );
    }

    /// A `ChangeBatch` whose `data` struct uses all-nullable fields (as
    /// Postgres native CDC produces) must be successfully written to an
    /// accelerator whose schema declares `id` as NOT NULL.
    ///
    /// Before the fix this would have caused a Vortex dtype mismatch that
    /// silently killed the write task. The `try_cast_to` step in
    /// `process_upsert_batch` makes the write succeed.
    #[tokio::test]
    async fn test_write_change_nullable_batch_against_non_nullable_accelerator() {
        // Accelerator schema: `id` is NOT NULL (create_test_data_schema).
        let task = make_refresh_task(make_mem_table() as Arc<dyn TableProvider>);

        // ChangeBatch schema: all fields nullable (Postgres CDC style).
        let change_batch = create_nullable_change_batch(
            vec!["c", "c"],
            &[vec!["id"], vec!["id"]],
            vec![1, 2],
            vec![Some("Alice"), Some("Bob")],
        );

        assert_eq!(
            task.write_change(change_batch)
                .await
                .expect("write must succeed with nullable batch against non-nullable accelerator"),
            WriteChangeResult::DataWritten,
        );
    }

    // ---------------------------------------------------------------------
    // Tests for `start_changes_stream` (the CDC source-stream → apply
    // pipeline). These exercise correctness of the prefetch-channel design:
    // ordering, commit-after-write, error continuation, clean termination,
    // dataset-ready signaling, actual pipelining behavior under a slow
    // accelerator, and prompt reader cancellation when the consumer goes
    // away. Together they nail down the invariants the broader CDC stack
    // relies on (PG WAL, Kafka/Debezium, DynamoDB Streams).
    // ---------------------------------------------------------------------

    use async_trait::async_trait;
    use data_components::cdc::{
        ChangeEnvelope, CommitChange, CommitError, StreamError as CdcStreamError,
    };
    use datafusion::catalog::Session;
    use datafusion::error::Result as DataFusionResult;
    use datafusion::execution::TaskContext;
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    };
    use datafusion::prelude::Expr;
    use futures::stream::{self as fstream};
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::task::{Context, Poll};
    use std::time::Duration;
    use tokio::sync::Mutex as TokioMutex;
    use tokio::sync::Notify;

    /// Records when each envelope is committed and in what order.
    /// Used to assert the apply→commit ordering invariant.
    #[derive(Default)]
    struct CommitLog {
        // (envelope_id, commit_outcome)
        events: TokioMutex<Vec<(i32, Result<(), String>)>>,
    }

    impl CommitLog {
        fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }

        async fn ids(&self) -> Vec<i32> {
            self.events.lock().await.iter().map(|(id, _)| *id).collect()
        }
    }

    struct TrackingCommitter {
        id: i32,
        log: Arc<CommitLog>,
        outcome: Result<(), String>,
    }

    #[async_trait]
    impl CommitChange for TrackingCommitter {
        async fn commit(&self) -> Result<(), CommitError> {
            self.log
                .events
                .lock()
                .await
                .push((self.id, self.outcome.clone()));
            match &self.outcome {
                Ok(()) => Ok(()),
                Err(msg) => Err(CommitError::UnableToCommitChange {
                    source: msg.clone().into(),
                }),
            }
        }
    }

    struct DeferrableTrackingCommitter {
        id: i32,
        log: Arc<CommitLog>,
        outcome: Result<(), String>,
    }

    #[async_trait]
    impl CommitChange for DeferrableTrackingCommitter {
        async fn commit(&self) -> Result<(), CommitError> {
            self.log
                .events
                .lock()
                .await
                .push((self.id, self.outcome.clone()));
            match &self.outcome {
                Ok(()) => Ok(()),
                Err(msg) => Err(CommitError::UnableToCommitChange {
                    source: msg.clone().into(),
                }),
            }
        }

        fn supports_deferral(&self) -> bool {
            true
        }
    }

    /// A coalescable, infallible committer mirroring `SharedLsnCommitter`'s
    /// max-fold shape: absorbs siblings by keeping the higher value, records the
    /// value it finally commits. Used to exercise `fold_committers` and the
    /// cross-epoch drain collapse without pulling in the Postgres crate.
    struct FoldableCommitter {
        value: u64,
        log: Arc<TokioMutex<Vec<u64>>>,
    }

    #[async_trait]
    impl CommitChange for FoldableCommitter {
        async fn commit(&self) -> Result<(), CommitError> {
            self.log.lock().await.push(self.value);
            Ok(())
        }

        fn supports_deferral(&self) -> bool {
            true
        }

        fn try_absorb(&mut self, other: &dyn CommitChange) -> bool {
            match other
                .as_any()
                .and_then(<dyn std::any::Any>::downcast_ref::<FoldableCommitter>)
            {
                Some(other) if Arc::ptr_eq(&self.log, &other.log) => {
                    self.value = self.value.max(other.value);
                    true
                }
                _ => false,
            }
        }

        fn as_any(&self) -> Option<&dyn std::any::Any> {
            Some(self)
        }
    }

    fn deferred_foldable(
        value: u64,
        log: &Arc<TokioMutex<Vec<u64>>>,
    ) -> Box<dyn cdc::CommitChange + Send + Sync> {
        Box::new(FoldableCommitter {
            value,
            log: Arc::clone(log),
        })
    }

    fn deferred_observer() -> SourceDurabilityObserver {
        SourceDurabilityObserver::new(
            TableReference::bare("test"),
            runtime_status::RuntimeStatus::new(),
        )
    }

    #[tokio::test]
    async fn deferred_metadata_keeps_oldest_fence_and_covers_merged_tail() {
        let observer = deferred_observer();
        let log = Arc::new(TokioMutex::new(Vec::new()));
        for fence in 1..=384 {
            observer
                .enqueue(fence, vec![deferred_foldable(fence, &log)])
                .await;
            assert!(observer.pending_count() <= 2);
        }
        assert!(log.lock().await.is_empty(), "publication is not durability");
        {
            let queue = observer.queue.lock().await;
            assert_eq!(queue.iter().map(|(f, _)| *f).collect::<Vec<_>>(), [1, 384]);
        }
        observer.on_durable(1).await;
        assert_eq!(*log.lock().await, [1]);
        for fence in 385..=768 {
            observer
                .enqueue(fence, vec![deferred_foldable(fence, &log)])
                .await;
            assert!(observer.pending_count() <= 2);
        }
        observer.on_durable(384).await;
        assert_eq!(
            *log.lock().await,
            [1, 384],
            "the next oldest fence stays fixed"
        );
        observer.on_durable(767).await;
        assert_eq!(
            *log.lock().await,
            [1, 384],
            "a partial tail fence cannot acknowledge it"
        );
        observer.on_durable(768).await;
        assert_eq!(*log.lock().await, [1, 384, 768]);
        assert_eq!(observer.pending_count(), 0);
    }

    #[tokio::test]
    async fn deferred_metadata_continuous_publication_does_not_starve_acknowledgement() {
        let observer = deferred_observer();
        let log = Arc::new(TokioMutex::new(Vec::new()));
        for fence in 1..=10_000 {
            observer
                .enqueue(fence, vec![deferred_foldable(fence, &log)])
                .await;
            assert!(
                observer.pending_count() <= 2,
                "fixed retained metadata bound"
            );
            if fence % 32 == 0 {
                let durable = fence - 16;
                observer.on_durable(durable).await;
                let log = log.lock().await;
                let last = *log.last().expect("an old fixed fence is durable");
                assert!(last <= durable, "never acknowledge beyond durability");
                assert!(
                    fence - last <= 64,
                    "old checkpoints must make progress under continuous input"
                );
            }
        }
        observer.on_durable(10_000).await;
        assert_eq!(log.lock().await.last(), Some(&10_000));
        assert_eq!(observer.pending_count(), 0);
    }

    #[tokio::test]
    async fn deferred_metadata_preserves_mixed_failure_order() {
        let observer = deferred_observer();
        let log = Arc::new(TokioMutex::new(Vec::new()));
        let failures = CommitLog::new();
        observer.enqueue(1, vec![deferred_foldable(1, &log)]).await;
        observer
            .enqueue(
                2,
                vec![Box::new(DeferrableTrackingCommitter {
                    id: 2,
                    log: Arc::clone(&failures),
                    outcome: Err("retry me".into()),
                })],
            )
            .await;
        for fence in 3..=4 {
            observer
                .enqueue(fence, vec![deferred_foldable(fence, &log)])
                .await;
        }
        observer
            .enqueue(
                5,
                vec![Box::new(DeferrableTrackingCommitter {
                    id: 5,
                    log: Arc::clone(&failures),
                    outcome: Ok(()),
                })],
            )
            .await;
        assert_eq!(observer.pending_count(), 4);
        observer.on_durable(5).await;
        observer.retry().await;
        assert_eq!(
            *log.lock().await,
            [1],
            "a failed predecessor fences the folded tail"
        );
        assert_eq!(failures.ids().await, [2, 2]);
        assert_eq!(observer.pending_count(), 3);
        let queue = observer.queue.lock().await;
        assert_eq!(queue.iter().map(|(f, _)| *f).collect::<Vec<_>>(), [2, 4, 5]);
    }

    #[tokio::test]
    async fn deferred_metadata_preserves_incompatible_source_identity() {
        let observer = deferred_observer();
        let first = Arc::new(TokioMutex::new(Vec::new()));
        let second = Arc::new(TokioMutex::new(Vec::new()));
        observer
            .enqueue(1, vec![deferred_foldable(1, &first)])
            .await;
        observer
            .enqueue(2, vec![deferred_foldable(2, &first)])
            .await;
        observer
            .enqueue(3, vec![deferred_foldable(3, &second)])
            .await;
        assert_eq!(observer.pending_count(), 3);
        observer.on_durable(3).await;
        assert_eq!(*first.lock().await, [2]);
        assert_eq!(*second.lock().await, [3]);
        assert_eq!(observer.pending_count(), 0);
    }

    #[tokio::test]
    async fn deferred_metadata_leaves_multi_committer_epochs_intact() {
        let observer = deferred_observer();
        let log = Arc::new(TokioMutex::new(Vec::new()));
        observer.enqueue(1, vec![deferred_foldable(1, &log)]).await;
        observer
            .enqueue(
                2,
                vec![deferred_foldable(2, &log), deferred_foldable(3, &log)],
            )
            .await;
        observer.enqueue(3, vec![deferred_foldable(4, &log)]).await;
        assert_eq!(observer.pending_count(), 4);
        let queue = observer.queue.lock().await;
        assert_eq!(
            queue.iter().map(|(f, c)| (*f, c.len())).collect::<Vec<_>>(),
            [(1, 1), (2, 2), (3, 1)]
        );
    }

    #[tokio::test]
    async fn deferred_metadata_late_enqueue_retries_durable_fence() {
        let observer = deferred_observer();
        let log = Arc::new(TokioMutex::new(Vec::new()));
        observer.on_durable(10).await;
        for fence in 1..=10 {
            observer
                .enqueue(fence, vec![deferred_foldable(fence, &log)])
                .await;
            assert_eq!(observer.pending_count(), 0);
            assert_eq!(log.lock().await.last(), Some(&fence));
        }
    }

    struct SuspendedDeferredCommitter {
        id: i32,
        log: Arc<CommitLog>,
        attempts: Arc<AtomicUsize>,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        failed: Arc<tokio::sync::Notify>,
        fail_first: bool,
    }

    #[async_trait]
    impl CommitChange for SuspendedDeferredCommitter {
        async fn commit(&self) -> Result<(), CommitError> {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                self.entered.notify_one();
                self.release.notified().await;
            }
            let fails = self.fail_first && attempt == 0;
            self.log.events.lock().await.push((
                self.id,
                if fails {
                    Err("retry source".into())
                } else {
                    Ok(())
                },
            ));
            if fails {
                self.failed.notify_one();
                return Err(CommitError::UnableToCommitChange {
                    source: "retry source".into(),
                });
            }
            Ok(())
        }

        fn supports_deferral(&self) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn deferred_metadata_cancelled_drain_resumes_source_future_in_order() {
        let observer = Arc::new(deferred_observer());
        let log = CommitLog::new();
        let attempts = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        for id in 1..=4 {
            let committer: Box<dyn CommitChange + Send + Sync> = if id == 2 {
                Box::new(SuspendedDeferredCommitter {
                    id,
                    log: Arc::clone(&log),
                    attempts: Arc::clone(&attempts),
                    entered: Arc::clone(&entered),
                    release: Arc::clone(&release),
                    failed: Arc::new(tokio::sync::Notify::new()),
                    fail_first: false,
                })
            } else {
                Box::new(DeferrableTrackingCommitter {
                    id,
                    log: Arc::clone(&log),
                    outcome: Ok(()),
                })
            };
            observer
                .enqueue(u64::try_from(id).expect("fence"), vec![committer])
                .await;
        }
        let task_observer = Arc::clone(&observer);
        let task = tokio::spawn(async move { task_observer.on_durable(3).await });
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .expect("second source call suspends");
        task.abort();
        assert!(
            task.await
                .expect_err("cancelled drain waiter")
                .is_cancelled()
        );
        assert_eq!(log.ids().await, [1]);
        assert_eq!(observer.pending_count(), 3);
        assert!(!observer.is_empty(None, "cancelled").await);
        release.notify_one();
        observer.retry().await;
        assert_eq!(log.ids().await, [1, 2, 3]);
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "resume, do not restart the source call"
        );
        assert_eq!(observer.pending_count(), 1);
        assert!(!observer.is_empty(None, "not_durable").await);
        observer.on_durable(4).await;
        assert_eq!(log.ids().await, [1, 2, 3, 4]);
        assert_eq!(observer.pending_count(), 0);
        assert!(observer.is_empty(None, "complete").await);
    }

    #[tokio::test]
    async fn deferred_metadata_cancelled_failure_requeue_retains_prefix() {
        let observer = Arc::new(deferred_observer());
        let log = CommitLog::new();
        let attempts = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let failed = Arc::new(tokio::sync::Notify::new());
        observer
            .enqueue(
                1,
                vec![Box::new(SuspendedDeferredCommitter {
                    id: 1,
                    log: Arc::clone(&log),
                    attempts: Arc::clone(&attempts),
                    entered: Arc::clone(&entered),
                    release: Arc::clone(&release),
                    failed: Arc::clone(&failed),
                    fail_first: true,
                })],
            )
            .await;
        observer
            .enqueue(
                2,
                vec![Box::new(DeferrableTrackingCommitter {
                    id: 2,
                    log: Arc::clone(&log),
                    outcome: Ok(()),
                })],
            )
            .await;
        let task_observer = Arc::clone(&observer);
        let task = tokio::spawn(async move { task_observer.on_durable(2).await });
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .expect("source call suspends");
        let queue = observer.queue.lock().await;
        assert!(queue.is_empty(), "durable prefix is owned by the drain");
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(5), failed.notified())
            .await
            .expect("source call failed before requeue");
        task.abort();
        assert!(
            task.await
                .expect_err("cancelled requeue waiter")
                .is_cancelled()
        );
        drop(queue);
        assert_eq!(observer.pending_count(), 2);
        assert!(!observer.is_empty(None, "requeue_pending").await);
        observer.retry().await;
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "finish the interrupted requeue first"
        );
        assert_eq!(observer.pending_count(), 2);
        {
            let queue = observer.queue.lock().await;
            assert_eq!(
                queue.iter().map(|(fence, _)| *fence).collect::<Vec<_>>(),
                [1, 2]
            );
        }
        observer.retry().await;
        assert_eq!(log.ids().await, [1, 1, 2]);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(observer.pending_count(), 0);
        assert!(observer.is_empty(None, "complete").await);
    }

    #[tokio::test]
    async fn deferred_metadata_cancelled_enqueue_retains_published_acknowledgement() {
        let observer = Arc::new(deferred_observer());
        let log = Arc::new(TokioMutex::new(Vec::new()));
        observer.enqueue(1, vec![deferred_foldable(1, &log)]).await;
        observer.enqueue(2, vec![deferred_foldable(2, &log)]).await;
        observer.on_durable(0).await;
        let drain = observer.drain.lock().await;
        let task_observer = Arc::clone(&observer);
        let task_log = Arc::clone(&log);
        let task = tokio::spawn(async move {
            task_observer
                .enqueue(3, vec![deferred_foldable(3, &task_log)])
                .await;
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while observer
                .queue
                .lock()
                .await
                .back()
                .is_none_or(|(fence, _)| *fence != 3)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("enqueue reaches its durability retry");
        task.abort();
        assert!(
            task.await
                .expect_err("enqueue waiter cancelled")
                .is_cancelled()
        );
        drop(drain);
        assert!(log.lock().await.is_empty());
        assert_eq!(observer.pending_count(), 2);
        observer.on_durable(2).await;
        assert_eq!(*log.lock().await, [1]);
        observer.on_durable(3).await;
        assert_eq!(*log.lock().await, [1, 3]);
        assert_eq!(observer.pending_count(), 0);
    }

    #[test]
    fn fold_committers_collapses_a_coalescable_run() {
        let log = Arc::new(TokioMutex::new(Vec::new()));
        let committers: Vec<Box<dyn cdc::CommitChange + Send + Sync>> = (1..=5u64)
            .map(|value| {
                Box::new(FoldableCommitter {
                    value,
                    log: Arc::clone(&log),
                }) as Box<dyn cdc::CommitChange + Send + Sync>
            })
            .collect();
        let folded = fold_committers(committers);
        assert_eq!(folded.len(), 1, "a coalescable run folds to one committer");
    }

    #[test]
    fn fold_committers_leaves_non_coalescable_committers_untouched() {
        let log = CommitLog::new();
        let committers: Vec<Box<dyn cdc::CommitChange + Send + Sync>> = (1..=3i32)
            .map(|id| {
                Box::new(TrackingCommitter {
                    id,
                    log: Arc::clone(&log),
                    outcome: Ok(()),
                }) as Box<dyn cdc::CommitChange + Send + Sync>
            })
            .collect();
        let folded = fold_committers(committers);
        assert_eq!(
            folded.len(),
            3,
            "order-sensitive committers keep their per-item structure"
        );
    }

    /// The drain-collapse gate: only an all-coalescable prefix collapses. A mixed
    /// prefix (coalescable followed by non-coalescable) must NOT collapse, so it
    /// keeps the safe per-epoch, requeue-on-failure drain.
    #[test]
    fn prefix_is_coalescable_requires_every_committer_to_opt_in() {
        let u64_log = Arc::new(TokioMutex::new(Vec::new()));
        let cc_log = CommitLog::new();
        let foldable = || {
            Box::new(FoldableCommitter {
                value: 1,
                log: Arc::clone(&u64_log),
            }) as Box<dyn cdc::CommitChange + Send + Sync>
        };
        let plain = || {
            Box::new(TrackingCommitter {
                id: 1,
                log: Arc::clone(&cc_log),
                outcome: Ok(()),
            }) as Box<dyn cdc::CommitChange + Send + Sync>
        };

        assert!(!prefix_is_coalescable(&VecDeque::new()), "empty prefix");
        assert!(
            !prefix_is_coalescable(&VecDeque::from([(1u64, vec![])])),
            "prefix of only empty committer vecs"
        );
        assert!(
            prefix_is_coalescable(&VecDeque::from([
                (1u64, vec![foldable()]),
                (2u64, vec![foldable()]),
            ])),
            "every committer coalescable"
        );
        assert!(
            !prefix_is_coalescable(&VecDeque::from([
                (1u64, vec![foldable()]),
                (2u64, vec![plain()]),
            ])),
            "mixed prefix must not collapse (the first-only-check hazard)"
        );
        assert!(
            !prefix_is_coalescable(&VecDeque::from([(1u64, vec![plain()])])),
            "no committer coalescable"
        );
    }

    /// The cross-epoch drain collapse: a coalescable dataset's whole
    /// `epoch <= durable` prefix folds to ONE commit carrying the max value —
    /// O(epochs) work becomes a single ack.
    #[cfg(not(windows))]
    #[tokio::test]
    async fn slot_advancer_collapses_coalescable_epochs_to_one_commit() {
        let log = Arc::new(TokioMutex::new(Vec::new()));
        let queue: DeferredCommitQueue = Arc::new(TokioMutex::new(VecDeque::new()));
        for epoch in 1..=4u64 {
            queue.lock().await.push_back((
                epoch,
                vec![Box::new(FoldableCommitter {
                    value: epoch * 10,
                    log: Arc::clone(&log),
                })
                    as Box<dyn cdc::CommitChange + Send + Sync>],
            ));
        }
        let advancer = SourceDurabilityObserver {
            queue: Arc::clone(&queue),
            pending_count: Arc::new(AtomicUsize::new(4)),
            ..SourceDurabilityObserver::new(
                TableReference::bare("test"),
                runtime_status::RuntimeStatus::new(),
            )
        };
        advancer.on_durable(4).await;
        assert_eq!(
            *log.lock().await,
            vec![40],
            "the four durable epochs fold to a single commit carrying the max LSN"
        );
        assert!(
            queue.lock().await.is_empty(),
            "the whole coalesced prefix is drained"
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn test_memory_cdc_durable_path_required_for_delete_truncate_and_unknown() {
        let upsert = create_test_change_batch(
            vec!["c", "u", "r"],
            &[vec!["id"], vec!["id"], vec!["id"]],
            vec![1, 2, 3],
            vec![Some("create"), Some("update"), Some("read")],
        );
        assert!(
            !change_batch_requires_durable_cdc_path(&upsert, false),
            "upsert-only bursts may use memory CDC durability"
        );

        // Sink cannot absorb deletes in RAM (capability=false): every
        // non-upsert op forces the durable path — the historical behavior.
        for op in ["d", "t", "x"] {
            let batch =
                create_test_change_batch(vec![op], &[vec!["id"]], vec![1], vec![Some("row")]);
            assert!(
                change_batch_requires_durable_cdc_path(&batch, false),
                "operation {op} must force the durable CDC path when the sink cannot absorb deletes"
            );
        }
    }

    #[cfg(not(windows))]
    #[test]
    fn test_memory_cdc_delete_burst_stays_on_mem_path_when_sink_absorbs() {
        // A keyed delete-bearing burst stays on the mem path when the sink
        // absorbs deletes in RAM (capability=true) — including mixed
        // upsert+delete bursts, the high-load coalesced shape.
        let delete_only =
            create_test_change_batch(vec!["d"], &[vec!["id"]], vec![1], vec![Some("row")]);
        assert!(
            !change_batch_requires_durable_cdc_path(&delete_only, true),
            "a keyed delete burst must stay on the mem path when the sink absorbs deletes"
        );

        let mixed = create_test_change_batch(
            vec!["c", "d", "u"],
            &[vec!["id"], vec!["id"], vec!["id"]],
            vec![1, 2, 3],
            vec![Some("create"), Some("delete"), Some("update")],
        );
        assert!(
            !change_batch_requires_durable_cdc_path(&mixed, true),
            "a mixed upsert+delete burst must stay on the mem path when the sink absorbs deletes"
        );

        // Truncate and Unknown are never absorbable — durable regardless of
        // the delete capability.
        for op in ["t", "x"] {
            let batch =
                create_test_change_batch(vec![op], &[vec!["id"]], vec![1], vec![Some("row")]);
            assert!(
                change_batch_requires_durable_cdc_path(&batch, true),
                "operation {op} must force the durable CDC path even when deletes are absorbable"
            );
        }

        // A keyless delete row has nothing to tombstone — durable even with
        // the capability on.
        let keyless_delete =
            create_test_change_batch(vec!["d"], &[vec![]], vec![1], vec![Some("row")]);
        assert!(
            change_batch_requires_durable_cdc_path(&keyless_delete, true),
            "a keyless delete row must force the durable CDC path"
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn test_memory_cdc_deferral_requires_every_committer_to_support_deferral() {
        let log = CommitLog::new();
        let deferrable: Box<dyn CommitChange + Send + Sync> =
            Box::new(DeferrableTrackingCommitter {
                id: 1,
                log: Arc::clone(&log),
                outcome: Ok(()),
            });
        assert!(committers_all_support_deferral(&[deferrable]));

        let log = CommitLog::new();
        let deferrable: Box<dyn CommitChange + Send + Sync> =
            Box::new(DeferrableTrackingCommitter {
                id: 1,
                log: Arc::clone(&log),
                outcome: Ok(()),
            });
        let non_deferrable: Box<dyn CommitChange + Send + Sync> = Box::new(TrackingCommitter {
            id: 2,
            log,
            outcome: Ok(()),
        });
        assert!(
            !committers_all_support_deferral(&[deferrable, non_deferrable]),
            "one non-deferrable committer forces the durable CDC path"
        );
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_slot_advancer_requeues_failed_deferred_committers() {
        let log = CommitLog::new();
        let queue: DeferredCommitQueue = Arc::new(TokioMutex::new(VecDeque::new()));
        queue.lock().await.push_back((
            5,
            vec![
                Box::new(DeferrableTrackingCommitter {
                    id: 1,
                    log: Arc::clone(&log),
                    outcome: Ok(()),
                }),
                Box::new(DeferrableTrackingCommitter {
                    id: 2,
                    log: Arc::clone(&log),
                    outcome: Err("commit failed".to_string()),
                }),
                Box::new(DeferrableTrackingCommitter {
                    id: 3,
                    log: Arc::clone(&log),
                    outcome: Ok(()),
                }),
            ],
        ));
        queue.lock().await.push_back((
            6,
            vec![Box::new(DeferrableTrackingCommitter {
                id: 4,
                log: Arc::clone(&log),
                outcome: Ok(()),
            })],
        ));

        let advancer = SourceDurabilityObserver {
            queue: Arc::clone(&queue),
            pending_count: Arc::new(AtomicUsize::new(4)),
            ..SourceDurabilityObserver::new(
                TableReference::bare("test"),
                runtime_status::RuntimeStatus::new(),
            )
        };
        advancer.on_durable(5).await;

        assert_eq!(
            log.ids().await,
            vec![1, 2],
            "advancer stops at the failed committer"
        );
        let queue = queue.lock().await;
        assert_eq!(queue.len(), 2, "failed and future epochs remain queued");
        assert_eq!(queue[0].0, 5);
        assert_eq!(
            queue[0].1.len(),
            2,
            "failed plus untried committers requeue"
        );
        assert_eq!(queue[1].0, 6);
    }

    /// A storage fence can become durable before publication lets the source
    /// enqueue its committer. Enqueue must retry that fence even on an idle source.
    #[tokio::test]
    async fn test_observer_acks_committers_pushed_after_durability() {
        let log = CommitLog::new();
        let queue: DeferredCommitQueue = Arc::new(TokioMutex::new(VecDeque::new()));
        let advancer = SourceDurabilityObserver {
            queue: Arc::clone(&queue),
            ..SourceDurabilityObserver::new(
                TableReference::bare("test"),
                runtime_status::RuntimeStatus::new(),
            )
        };

        // Epoch 1's committers ARE queued; epoch 2's are not yet (the apply loop
        // hasn't pushed them). A checkpoint that snapshotted `flushed_epoch = 2`
        // fires ahead of the push.
        advancer
            .enqueue(
                1,
                vec![Box::new(DeferrableTrackingCommitter {
                    id: 1,
                    log: Arc::clone(&log),
                    outcome: Ok(()),
                })],
            )
            .await;
        advancer.on_durable(2).await;

        // Only epoch 1 acked (it was present and <= 2); epoch 2 is NOT acked early
        // because its committers were not yet queued.
        assert_eq!(
            log.ids().await,
            vec![1],
            "only the queued, durable-covered committer acks; the unqueued epoch is not advanced early"
        );
        assert!(
            queue.lock().await.is_empty(),
            "the drained prefix is removed; nothing was invented for the unqueued epoch"
        );

        advancer
            .enqueue(
                2,
                vec![Box::new(DeferrableTrackingCommitter {
                    id: 2,
                    log: Arc::clone(&log),
                    outcome: Ok(()),
                })],
            )
            .await;
        advancer.on_durable(2).await;
        assert_eq!(
            log.ids().await,
            vec![1, 2],
            "enqueue observes the durable fence without acknowledging epoch 1 again"
        );
        assert!(queue.lock().await.is_empty(), "queue fully drained");
    }

    fn make_tracked_envelope(id: i32, log: Arc<CommitLog>, is_ready: bool) -> ChangeEnvelope {
        let batch = create_test_change_batch(vec!["c"], &[vec!["id"]], vec![id], vec![Some("row")]);
        ChangeEnvelope::new(
            Box::new(TrackingCommitter {
                id,
                log,
                outcome: Ok(()),
            }),
            batch,
            is_ready,
        )
    }

    /// Stream wrapper that signals on Drop. Used to verify the reader task
    /// is torn down when the consumer goes away.
    struct DropSignalStream<S> {
        inner: S,
        notify_on_drop: Arc<Notify>,
    }

    impl<S> Drop for DropSignalStream<S> {
        fn drop(&mut self) {
            self.notify_on_drop.notify_waiters();
        }
    }

    impl<S: futures::Stream + Unpin> futures::Stream for DropSignalStream<S> {
        type Item = S::Item;
        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Pin::new(&mut self.inner).poll_next(cx)
        }
    }

    /// Builds a `ChangesStream` from a vector of pre-built items. Items are
    /// yielded in order; the stream then ends.
    fn make_changes_stream(items: Vec<Result<ChangeEnvelope, CdcStreamError>>) -> ChangesStream {
        fstream::iter(items).boxed()
    }

    /// Builds a `ChangesStream` that yields each item only after `delay`, so
    /// item N arrives at roughly `N * delay`.
    fn make_delayed_changes_stream(
        items: Vec<Result<ChangeEnvelope, CdcStreamError>>,
        delay: Duration,
    ) -> ChangesStream {
        fstream::iter(items)
            .then(move |item| async move {
                tokio::time::sleep(delay).await;
                item
            })
            .boxed()
    }

    /// A baseline `CdcConfig` for tests with caps high enough that only the
    /// `max_coalesce_age_ms` field under test governs flushing.
    fn test_cdc_config(max_coalesce_age_ms: u64) -> CdcConfig {
        CdcConfig {
            prefetch_buffer: 128,
            max_coalesced_envelopes: 256,
            max_coalesced_bytes: 128 * 1024 * 1024,
            max_coalesce_age_ms,
            commit_timeout: Duration::from_secs(30),
            delete_subbatch_max: CDC_DELETE_SUBBATCH_MAX_DEFAULT,
        }
    }

    /// Run a changes stream with an explicit `CdcConfig`, bypassing the process-global `cdc_config()`
    async fn run_changes_stream_with_config(
        task: &RefreshTask,
        cfg: CdcConfig,
        stream: ChangesStream,
    ) -> crate::accelerated::Result<()> {
        let refresh = Arc::new(RwLock::new(crate::accelerated::refresh::Refresh::default()));
        task.start_changes_stream_with_config(
            cfg,
            refresh,
            stream,
            None,
            None,
            Arc::new(AtomicBool::new(false)),
        )
        .await
    }

    /// With a large `max_coalesce_age_ms`, the apply loop lingers and coalesces
    /// several slowly-arriving envelopes into a single accelerator write rather
    /// than one write per envelope.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_linger_coalesces_delayed_items_into_one_write() {
        let insert_plan_calls = Arc::new(AtomicUsize::new(0));
        let insert_execution_calls = Arc::new(AtomicUsize::new(0));
        let provider = Arc::new(CountingInsertProvider {
            inner: make_mem_table() as Arc<dyn TableProvider>,
            insert_plan_calls: Arc::clone(&insert_plan_calls),
            insert_execution_calls: Arc::clone(&insert_execution_calls),
        });
        let task = make_refresh_task(provider as Arc<dyn TableProvider>);
        let log = CommitLog::new();

        // 4 envelopes ~100ms apart (~400ms total) — far inside the 5s window.
        let items: Vec<Result<ChangeEnvelope, CdcStreamError>> = (1..=4)
            .map(|id| Ok(make_tracked_envelope(id, Arc::clone(&log), false)))
            .collect();
        let stream = make_delayed_changes_stream(items, Duration::from_millis(100));

        run_changes_stream_with_config(&task, test_cdc_config(5_000), stream)
            .await
            .expect("changes stream should succeed");

        // One plan execution == one accelerator write. The linger window must
        // fold all four delayed envelopes into a single write. (`insert_plan_calls`
        // would be 1 regardless, since the insert plan is built once and cached
        // — see `CountingInsertProvider`.)
        assert_eq!(
            insert_execution_calls.load(AtomicOrdering::SeqCst),
            1,
            "a large linger window must coalesce all delayed envelopes into one write"
        );
        assert_eq!(
            log.ids().await,
            vec![1, 2, 3, 4],
            "all envelopes must still commit in arrival order"
        );
    }

    /// With `max_coalesce_age_ms = 0` (default), the apply loop does NOT wait:
    /// each slowly-arriving envelope is applied on its own, so the writes are
    /// NOT all coalesced.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_no_linger_applies_delayed_items_separately() {
        let insert_plan_calls = Arc::new(AtomicUsize::new(0));
        let insert_execution_calls = Arc::new(AtomicUsize::new(0));
        let provider = Arc::new(CountingInsertProvider {
            inner: make_mem_table() as Arc<dyn TableProvider>,
            insert_plan_calls: Arc::clone(&insert_plan_calls),
            insert_execution_calls: Arc::clone(&insert_execution_calls),
        });
        let task = make_refresh_task(provider as Arc<dyn TableProvider>);
        let log = CommitLog::new();

        let items: Vec<Result<ChangeEnvelope, CdcStreamError>> = (1..=4)
            .map(|id| Ok(make_tracked_envelope(id, Arc::clone(&log), false)))
            .collect();
        let stream = make_delayed_changes_stream(items, Duration::from_millis(100));

        run_changes_stream_with_config(&task, test_cdc_config(0), stream)
            .await
            .expect("changes stream should succeed");

        // Each delayed envelope arrives after the previous one has been applied,
        // so without a linger window each is written on its own — one plan
        // execution per envelope. `insert_plan_calls` can't see this: the insert
        // plan is built once and cached (see `CountingInsertProvider`).
        assert_eq!(
            insert_execution_calls.load(AtomicOrdering::SeqCst),
            4,
            "without a linger window, each delayed envelope must be written on its own"
        );
        assert_eq!(log.ids().await, vec![1, 2, 3, 4]);
    }

    /// When the buffered burst already meets/exceeds the byte budget, the linger
    /// phase must NOT wait: no further envelope could be admitted (any would trip
    /// the byte cap and be carried), so waiting only delays an already-full
    /// write. Here the first envelope alone exceeds a 1-byte budget, the linger
    /// window is huge (60s), and the source then parks open — so a buggy linger
    /// would block the write for the full 60s. The write must instead land
    /// promptly, well inside the window.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_over_byte_budget_burst_does_not_linger() {
        // Stream yields one envelope, then parks forever (keeps the channel open
        // so a buggy linger blocks on `rx.recv()` rather than seeing EOF).
        struct YieldOnceThenParkStream {
            yielded: bool,
            log: Arc<CommitLog>,
        }
        impl futures::Stream for YieldOnceThenParkStream {
            type Item = Result<ChangeEnvelope, CdcStreamError>;
            fn poll_next(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                if self.yielded {
                    Poll::Pending
                } else {
                    self.yielded = true;
                    Poll::Ready(Some(Ok(make_tracked_envelope(
                        1,
                        Arc::clone(&self.log),
                        false,
                    ))))
                }
            }
        }

        let insert_execution_calls = Arc::new(AtomicUsize::new(0));
        let provider = Arc::new(CountingInsertProvider {
            inner: make_mem_table() as Arc<dyn TableProvider>,
            insert_plan_calls: Arc::new(AtomicUsize::new(0)),
            insert_execution_calls: Arc::clone(&insert_execution_calls),
        });
        let task = make_refresh_task(provider as Arc<dyn TableProvider>);
        let log = CommitLog::new();

        // 1-byte budget so a single real envelope is already over budget, paired
        // with a 60s linger window the fix must refuse to wait out.
        let cfg = CdcConfig {
            prefetch_buffer: 128,
            max_coalesced_envelopes: 256,
            max_coalesced_bytes: 1,
            max_coalesce_age_ms: 60_000,
            commit_timeout: Duration::from_secs(30),
            delete_subbatch_max: CDC_DELETE_SUBBATCH_MAX_DEFAULT,
        };

        let stream: ChangesStream = YieldOnceThenParkStream {
            yielded: false,
            log: Arc::clone(&log),
        }
        .boxed();

        let join =
            tokio::spawn(async move { run_changes_stream_with_config(&task, cfg, stream).await });

        // The write must land far inside the 60s linger window. A 5s deadline is
        // generous for the immediate write yet nowhere near the buggy 60s wait.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while insert_execution_calls.load(AtomicOrdering::SeqCst) == 0 {
            assert!(
                std::time::Instant::now() <= deadline,
                "over-budget burst was held by the linger window instead of writing immediately",
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            insert_execution_calls.load(AtomicOrdering::SeqCst),
            1,
            "the over-budget envelope must be written exactly once, on its own"
        );
        assert_eq!(log.ids().await, vec![1]);

        // Source parks forever; tear the apply task down.
        join.abort();
    }

    /// Counts every poll on the inner stream, and lets us pull on demand via
    /// an inner channel. This makes pipeline overlap directly observable.
    async fn run_changes_stream(
        task: &RefreshTask,
        stream: ChangesStream,
        refresh_completion: Option<RefreshCompletion>,
        initial_load_completed: Arc<AtomicBool>,
    ) -> crate::accelerated::Result<()> {
        let refresh = Arc::new(RwLock::new(crate::accelerated::refresh::Refresh::default()));
        task.start_changes_stream(
            refresh,
            stream,
            None,
            refresh_completion,
            initial_load_completed,
        )
        .await
    }

    // -- Correctness: ordering ------------------------------------------------

    #[tokio::test]
    async fn test_start_changes_stream_processes_envelopes_in_order() {
        let task = make_refresh_task(make_mem_table() as Arc<dyn TableProvider>);
        let log = CommitLog::new();
        let stream = make_changes_stream(vec![
            Ok(make_tracked_envelope(1, Arc::clone(&log), false)),
            Ok(make_tracked_envelope(2, Arc::clone(&log), false)),
            Ok(make_tracked_envelope(3, Arc::clone(&log), false)),
            Ok(make_tracked_envelope(4, Arc::clone(&log), false)),
        ]);

        run_changes_stream(&task, stream, None, Arc::new(AtomicBool::new(false)))
            .await
            .expect("start_changes_stream should succeed");

        assert_eq!(
            log.ids().await,
            vec![1, 2, 3, 4],
            "envelopes must be committed in arrival order"
        );
    }

    // -- Correctness: commit-after-write ordering -----------------------------

    /// Wraps a `TableProvider` and counts each `insert_into` call.
    #[derive(Debug)]
    struct CountingInsertProvider {
        inner: Arc<dyn TableProvider>,
        insert_plan_calls: Arc<AtomicUsize>,
        insert_execution_calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl TableProvider for CountingInsertProvider {
        fn schema(&self) -> arrow::datatypes::SchemaRef {
            self.inner.schema()
        }

        fn table_type(&self) -> datafusion::datasource::TableType {
            self.inner.table_type()
        }

        async fn scan(
            &self,
            state: &dyn Session,
            projection: Option<&Vec<usize>>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            self.inner.scan(state, projection, filters, limit).await
        }

        async fn insert_into(
            &self,
            state: &dyn Session,
            input: Arc<dyn ExecutionPlan>,
            insert_op: InsertOp,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            self.insert_plan_calls.fetch_add(1, AtomicOrdering::SeqCst);
            let inner_plan = self.inner.insert_into(state, input, insert_op).await?;
            Ok(Arc::new(CountingExec {
                inner: inner_plan,
                insert_execution_calls: Arc::clone(&self.insert_execution_calls),
            }))
        }
    }

    /// Delegating [`ExecutionPlan`] that bumps a counter every time it is
    /// executed. Wrapping the plan returned by `insert_into` lets a test count
    /// accelerator writes
    #[derive(Debug)]
    struct CountingExec {
        inner: Arc<dyn ExecutionPlan>,
        insert_execution_calls: Arc<AtomicUsize>,
    }

    impl DisplayAs for CountingExec {
        fn fmt_as(
            &self,
            _t: DisplayFormatType,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            write!(f, "CountingExec")
        }
    }

    impl ExecutionPlan for CountingExec {
        fn name(&self) -> &'static str {
            "CountingExec"
        }
        fn properties(&self) -> &Arc<PlanProperties> {
            self.inner.properties()
        }
        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(
                &Arc<dyn datafusion::physical_plan::PhysicalExpr>,
            ) -> datafusion::error::Result<
                datafusion::common::tree_node::TreeNodeRecursion,
            >,
        ) -> datafusion::error::Result<datafusion::common::tree_node::TreeNodeRecursion> {
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![&self.inner]
        }
        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            let inner = children
                .into_iter()
                .next()
                .expect("CountingExec expects exactly one child ExecutionPlan");
            Ok(Arc::new(CountingExec {
                inner,
                insert_execution_calls: Arc::clone(&self.insert_execution_calls),
            }))
        }
        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> DataFusionResult<SendableRecordBatchStream> {
            self.insert_execution_calls
                .fetch_add(1, AtomicOrdering::SeqCst);
            self.inner.execute(partition, context)
        }
    }

    /// Wraps a `TableProvider` and counts each `delete_from` call, delegating
    /// the delete to the inner provider. Lets a test assert that an N-key
    /// delete burst is applied as `⌈N/cap⌉` independent durable plans.
    #[derive(Debug)]
    struct CountingDeleteProvider {
        inner: Arc<dyn TableProvider>,
        delete_plan_calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl TableProvider for CountingDeleteProvider {
        fn schema(&self) -> arrow::datatypes::SchemaRef {
            self.inner.schema()
        }

        fn table_type(&self) -> datafusion::datasource::TableType {
            self.inner.table_type()
        }

        async fn scan(
            &self,
            state: &dyn Session,
            projection: Option<&Vec<usize>>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            self.inner.scan(state, projection, filters, limit).await
        }

        async fn insert_into(
            &self,
            state: &dyn Session,
            input: Arc<dyn ExecutionPlan>,
            insert_op: InsertOp,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            self.inner.insert_into(state, input, insert_op).await
        }

        async fn delete_from(
            &self,
            state: &dyn Session,
            filters: Vec<Expr>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            self.delete_plan_calls.fetch_add(1, AtomicOrdering::SeqCst);
            self.inner.delete_from(state, filters).await
        }

        // Forward the behaviorally-meaningful optional methods to `inner` so the
        // wrapper mirrors the wrapped provider rather than silently reverting to
        // trait defaults (the wrapper-delegation footgun — statistics in
        // particular changes planning).
        fn constraints(&self) -> Option<&datafusion::common::Constraints> {
            self.inner.constraints()
        }

        fn supports_filters_pushdown(
            &self,
            filters: &[&Expr],
        ) -> DataFusionResult<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
            self.inner.supports_filters_pushdown(filters)
        }

        fn statistics(&self) -> Option<datafusion::common::Statistics> {
            self.inner.statistics()
        }
    }

    /// Wraps a `TableProvider` and records each `insert_into` call.
    /// Together with `CommitLog`, this lets us assert that for every
    /// envelope `id`, the write event happens strictly before the commit.
    #[derive(Debug)]
    struct WriteOrderRecordingProvider {
        inner: Arc<dyn TableProvider>,
        write_log: Arc<TokioMutex<Vec<String>>>,
    }

    #[async_trait]
    impl TableProvider for WriteOrderRecordingProvider {
        fn schema(&self) -> arrow::datatypes::SchemaRef {
            self.inner.schema()
        }
        fn table_type(&self) -> datafusion::datasource::TableType {
            self.inner.table_type()
        }
        async fn scan(
            &self,
            state: &dyn Session,
            projection: Option<&Vec<usize>>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            self.inner.scan(state, projection, filters, limit).await
        }
        async fn insert_into(
            &self,
            state: &dyn Session,
            input: Arc<dyn ExecutionPlan>,
            insert_op: InsertOp,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            self.write_log.lock().await.push("write".to_string());
            self.inner.insert_into(state, input, insert_op).await
        }
    }

    #[derive(Debug)]
    struct FailFirstWriteProvider {
        inner: Arc<dyn TableProvider>,
        failures_remaining: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl TableProvider for FailFirstWriteProvider {
        fn schema(&self) -> arrow::datatypes::SchemaRef {
            self.inner.schema()
        }

        fn table_type(&self) -> datafusion::datasource::TableType {
            self.inner.table_type()
        }

        async fn scan(
            &self,
            state: &dyn Session,
            projection: Option<&Vec<usize>>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            self.inner.scan(state, projection, filters, limit).await
        }

        async fn insert_into(
            &self,
            state: &dyn Session,
            input: Arc<dyn ExecutionPlan>,
            insert_op: InsertOp,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            if self
                .failures_remaining
                .fetch_update(
                    AtomicOrdering::SeqCst,
                    AtomicOrdering::SeqCst,
                    |remaining| remaining.checked_sub(1),
                )
                .is_ok()
            {
                return Err(datafusion::error::DataFusionError::Execution(
                    "synthetic write failure".to_string(),
                ));
            }

            self.inner.insert_into(state, input, insert_op).await
        }
    }

    /// Records "commit" into a shared log when its `commit()` runs, so we
    /// can assert the interleaved write/commit sequence in
    /// `test_start_changes_stream_commits_after_write`.
    struct SequencedCommitter {
        id: i32,
        log: Arc<TokioMutex<Vec<String>>>,
    }
    #[async_trait]
    impl CommitChange for SequencedCommitter {
        async fn commit(&self) -> Result<(), CommitError> {
            self.log.lock().await.push(format!("commit:{}", self.id));
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_start_changes_stream_commits_after_write() {
        let write_log: Arc<TokioMutex<Vec<String>>> = Arc::new(TokioMutex::new(Vec::new()));
        let provider = Arc::new(WriteOrderRecordingProvider {
            inner: make_mem_table() as Arc<dyn TableProvider>,
            write_log: Arc::clone(&write_log),
        });
        let task = make_refresh_task(provider as Arc<dyn TableProvider>);

        // Use a single shared log; both `insert_into` and `commit()` push
        // markers, so we can read off the interleaved write/commit sequence.
        let combined: Arc<TokioMutex<Vec<String>>> = Arc::clone(&write_log);

        let mk = |id: i32| -> ChangeEnvelope {
            let batch =
                create_test_change_batch(vec!["c"], &[vec!["id"]], vec![id], vec![Some("row")]);
            ChangeEnvelope::new(
                Box::new(SequencedCommitter {
                    id,
                    log: Arc::clone(&combined),
                }),
                batch,
                false,
            )
        };

        let stream = make_changes_stream(vec![Ok(mk(1)), Ok(mk(2)), Ok(mk(3))]);
        run_changes_stream(&task, stream, None, Arc::new(AtomicBool::new(false)))
            .await
            .expect("start_changes_stream should succeed");

        let observed = combined.lock().await.clone();
        // Coalescing depends on how many envelopes the reader has already
        // buffered when the applier drains with `try_recv`, so Tokio
        // scheduling can legitimately produce one or more writes here. The
        // invariant is that no commit happens before a write, and committers
        // run in stream order.
        assert_eq!(
            observed[0], "write",
            "a write must happen before the first commit"
        );
        let commits: Vec<&str> = observed
            .iter()
            .filter_map(|event| event.strip_prefix("commit:"))
            .collect();
        assert_eq!(
            commits,
            vec!["1", "2", "3"],
            "committers must run in stream order",
        );
        assert!(
            observed.iter().any(|event| event == "write"),
            "at least one accelerator write should occur",
        );
    }

    // -- Correctness: error path continues the loop ---------------------------

    #[tokio::test]
    async fn test_start_changes_stream_continues_after_stream_error() {
        let task = make_refresh_task(make_mem_table() as Arc<dyn TableProvider>);
        let log = CommitLog::new();

        // Sandwich a fatal stream error between two healthy envelopes; both
        // valid envelopes must still be committed (the loop logs the error
        // and continues — it does not abort).
        let stream = make_changes_stream(vec![
            Ok(make_tracked_envelope(1, Arc::clone(&log), false)),
            Err(CdcStreamError::Arrow("synthetic test failure".into())),
            Ok(make_tracked_envelope(2, Arc::clone(&log), false)),
        ]);

        run_changes_stream(&task, stream, None, Arc::new(AtomicBool::new(false)))
            .await
            .expect("start_changes_stream should not propagate stream errors");

        assert_eq!(
            log.ids().await,
            vec![1, 2],
            "both pre- and post-error envelopes must be committed"
        );
    }

    /// Succeeds for the first `allow` writes, then fails. Used to let a listing
    /// rebuild overwrite land and then fail a later CDC upsert in the same run.
    #[derive(Debug)]
    struct FailAfterNWrites {
        inner: Arc<dyn TableProvider>,
        writes_seen: Arc<AtomicUsize>,
        allow: usize,
    }

    #[async_trait]
    impl TableProvider for FailAfterNWrites {
        fn schema(&self) -> arrow::datatypes::SchemaRef {
            self.inner.schema()
        }

        fn table_type(&self) -> datafusion::datasource::TableType {
            self.inner.table_type()
        }

        async fn scan(
            &self,
            state: &dyn Session,
            projection: Option<&Vec<usize>>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            self.inner.scan(state, projection, filters, limit).await
        }

        async fn insert_into(
            &self,
            state: &dyn Session,
            input: Arc<dyn ExecutionPlan>,
            insert_op: InsertOp,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            let seen = self.writes_seen.fetch_add(1, AtomicOrdering::SeqCst);
            if seen >= self.allow {
                return Err(datafusion::error::DataFusionError::Execution(
                    "synthetic write failure after allowed writes".to_string(),
                ));
            }
            self.inner.insert_into(state, input, insert_op).await
        }
    }

    #[tokio::test]
    async fn test_apply_envelope_run_skips_commits_after_coalesced_write_failure() {
        let failures_remaining = Arc::new(AtomicUsize::new(1));
        let provider = Arc::new(FailFirstWriteProvider {
            inner: make_mem_table() as Arc<dyn TableProvider>,
            failures_remaining: Arc::clone(&failures_remaining),
        });
        let task = make_refresh_task(provider as Arc<dyn TableProvider>);
        let log = CommitLog::new();
        let dataset_name = TableReference::bare("test");
        let metric_labels = DatasetMetricLabels::new(&dataset_name);
        let initial_load_completed = Arc::new(AtomicBool::new(false));
        let mut pending_finalize = None;
        let mut pending_commit = None;
        let write_ctx = SessionContext::new();
        let write_session_state = write_ctx.state();
        // Only consulted when a source reports its history is unavailable, which
        // these cases never do; the default carries `RefreshMode::Full`, which is
        // what a rebuild would override it to anyway.
        let refresh = Arc::new(RwLock::new(Refresh::default()));
        let mut context = ApplyContext {
            refresh_sql: None,
            refresh: &refresh,
            dataset_name: &dataset_name,
            metric_labels: &metric_labels,
            caching: None,
            refresh_completion: None,
            initial_load_completed: &initial_load_completed,
            write_ctx: &write_ctx,
            write_session_state: &write_session_state,
            commit_timeout: Duration::from_secs(5),
            pending_finalize: &mut pending_finalize,
            pending_commit: &mut pending_commit,
            deferred_commits: None,
        };

        assert!(
            !task
                .apply_envelope_run(
                    &mut context,
                    vec![
                        make_tracked_envelope(1, Arc::clone(&log), false),
                        make_tracked_envelope(2, Arc::clone(&log), false),
                    ],
                )
                .await,
            "write failures should stop the stream so later commits cannot skip an uncommitted gap"
        );
        assert!(
            context.pending_commit.is_none(),
            "failed writes must not spawn commit tasks"
        );
        assert_eq!(
            log.ids().await,
            Vec::<i32>::new(),
            "failed coalesced writes must not commit any envelope in the run"
        );
        assert!(
            task.runtime_status
                .get_component_status("dataset:test")
                .expect("failure should set dataset status")
                .is_error(),
            "write failure should mark dataset refresh status as error"
        );
        assert!(
            !initial_load_completed.load(Ordering::Relaxed),
            "failed writes must not mark initial load complete"
        );
    }

    // -- Schema evolution: policy gate, classification alignment, and the
    // mixed-schema per-group fallback ------------------------------------------

    /// CDC data struct carrying an extra trailing nullable `age` column — the
    /// shape the `postgres_replication` source emits after adopting a mid-stream
    /// ADD COLUMN.
    fn create_widened_change_batch(id: i32, age: i32) -> ChangeBatch {
        create_widened_change_batch_ops(&["c"], &[id], age)
    }

    /// A CDC burst under the `age`-widened data schema carrying one row per entry
    /// of `ops` (Debezium op codes: `c` create, `d` delete, `t` truncate).
    ///
    /// The widening is a property of the burst's schema, not of any one row, so
    /// every op in the burst arrives under it — which is what makes a mixed burst
    /// able to commit its destructive half before the widening is ever judged.
    fn create_widened_change_batch_ops(ops: &[&str], ids: &[i32], age: i32) -> ChangeBatch {
        assert_eq!(ops.len(), ids.len(), "ops and ids must have same length");

        let data_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, true),
        ]);
        let schema = changes_schema(&data_schema);
        let op_array: ArrayRef = Arc::new(StringArray::from(ops.to_vec()));
        let pk_field = Arc::new(Field::new("item", DataType::Utf8, false));
        // One primary-key entry (`id`) per row.
        let pk_offsets: Vec<i32> =
            (0..=i32::try_from(ops.len()).expect("op count fits in i32")).collect();
        let pk_array: ArrayRef = Arc::new(
            ListArray::try_new(
                pk_field,
                arrow::buffer::OffsetBuffer::new(pk_offsets.into()),
                Arc::new(StringArray::from(vec!["id"; ops.len()])),
                None,
            )
            .expect("pk list"),
        );
        let data_fields = vec![
            (
                Arc::new(Field::new("id", DataType::Int32, false)),
                Arc::new(Int32Array::from(ids.to_vec())) as ArrayRef,
            ),
            (
                Arc::new(Field::new("name", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![Some("row"); ops.len()])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("age", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![age; ops.len()])) as ArrayRef,
            ),
        ];
        let data_array: ArrayRef = Arc::new(StructArray::from(data_fields));
        let record = RecordBatch::try_new(Arc::new(schema), vec![op_array, pk_array, data_array])
            .expect("record batch");
        ChangeBatch::try_new(record).expect("change batch")
    }

    fn make_widened_tracked_envelope(id: i32, log: Arc<CommitLog>) -> ChangeEnvelope {
        ChangeEnvelope::new(
            Box::new(TrackingCommitter {
                id,
                log,
                outcome: Ok(()),
            }),
            create_widened_change_batch(id, 30),
            false,
        )
    }

    #[test]
    fn test_evolution_allowed_per_policy_set() {
        let ctx = EvolutionContext {
            constraint_columns: &[],
        };
        let current = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let added_only = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]);
        let SchemaEvolution::Widening(additive) =
            schema_evolution::classify(&current, &added_only, &ctx)
        else {
            panic!("expected additive widening");
        };
        let widened = Schema::new(vec![Field::new("a", DataType::Int64, false)]);
        let SchemaEvolution::Widening(typed) = schema_evolution::classify(&current, &widened, &ctx)
        else {
            panic!("expected type widening");
        };

        assert!(evolution_allowed(
            OnSchemaChange::AppendNewColumns,
            &additive
        ));
        assert!(!evolution_allowed(OnSchemaChange::AppendNewColumns, &typed));
        assert!(evolution_allowed(OnSchemaChange::SyncAllColumns, &additive));
        assert!(evolution_allowed(OnSchemaChange::SyncAllColumns, &typed));
        // `drop_and_recreate` evolves the full widening set in place like `sync_all_columns`.
        assert!(evolution_allowed(
            OnSchemaChange::DropAndRecreate,
            &additive
        ));
        assert!(evolution_allowed(OnSchemaChange::DropAndRecreate, &typed));
        assert!(!evolution_allowed(OnSchemaChange::Block, &additive));
        assert!(!evolution_allowed(OnSchemaChange::Fail, &additive));
        assert_eq!(widening_plan_kind(&additive), "added_columns");
        assert_eq!(widening_plan_kind(&typed), "widened_types");
    }

    #[test]
    fn test_align_nullability_prevents_false_relax_classification() {
        // The CDC data struct is nullable-everywhere by design; without
        // alignment the classifier would report a nullability relax on every
        // non-nullable accelerator field and block append_new_columns.
        let target: SchemaRef = Arc::new(create_test_data_schema());
        let incoming: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let ctx = EvolutionContext {
            constraint_columns: &[],
        };
        let aligned = align_nullability_for_classify(&target, &incoming);
        assert!(matches!(
            schema_evolution::classify(&target, &aligned, &ctx),
            SchemaEvolution::Identical
        ));

        // An added trailing column stays nullable and classifies additive-only.
        let wider: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, true),
        ]));
        let aligned = align_nullability_for_classify(&target, &wider);
        let SchemaEvolution::Widening(plan) = schema_evolution::classify(&target, &aligned, &ctx)
        else {
            panic!("expected widening");
        };
        assert!(plan.is_additive_only());
        assert_eq!(plan.added_columns[0].name(), "age");
    }

    #[test]
    fn test_group_run_by_schema_splits_on_schema_boundary() {
        let log = CommitLog::new();
        let make_committers =
            |ids: std::ops::RangeInclusive<i32>| -> Vec<Box<dyn cdc::CommitChange + Send + Sync>> {
                ids.map(|id| {
                    Box::new(TrackingCommitter {
                        id,
                        log: Arc::clone(&log),
                        outcome: Ok(()),
                    }) as Box<dyn cdc::CommitChange + Send + Sync>
                })
                .collect()
            };

        let batches = vec![
            create_test_change_batch(vec!["c"], &[vec!["id"]], vec![1], vec![Some("a")]),
            create_test_change_batch(vec!["c"], &[vec!["id"]], vec![2], vec![Some("b")]),
            create_widened_change_batch(3, 30),
        ];
        let groups = group_run_by_schema(batches, make_committers(1..=3), true);
        assert_eq!(groups.len(), 2, "one schema boundary -> two groups");
        assert_eq!(groups[0].0.len(), 2);
        assert_eq!(
            groups[0].1.len(),
            2,
            "committers must travel with their group"
        );
        assert_eq!(groups[1].0.len(), 1);
        assert_eq!(groups[1].1.len(), 1);

        // split == false (block policy / no settings): single group verbatim.
        let batches = vec![
            create_test_change_batch(vec!["c"], &[vec!["id"]], vec![1], vec![Some("a")]),
            create_widened_change_batch(2, 30),
        ];
        let groups = group_run_by_schema(batches, make_committers(1..=2), false);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].0.len(), 2);
    }

    /// Mixed-schema coalesced run (mid-stream column add): with an evolution
    /// policy installed, the run falls back to per-schema-group applies
    /// instead of failing the whole run on the concat error — both envelopes
    /// apply and commit in stream order. (The `MemTable` accelerator can't
    /// evolve mid-stream, so the wider batch narrow-casts with a warning;
    /// restart-time evolution applies the change.)
    #[tokio::test]
    async fn test_apply_envelope_run_mixed_schemas_applies_per_group_under_policy() {
        let dataset_name = TableReference::bare("schema_evo_mixed_groups");
        let metric_labels = DatasetMetricLabels::new(&dataset_name);
        install_cdc_schema_evolution(
            &dataset_name,
            CdcSchemaEvolution {
                policy: OnSchemaChange::AppendNewColumns,
                constraint_columns: vec!["id".to_string()],
            },
        );

        let task = make_refresh_task_named(
            "schema_evo_mixed_groups",
            make_mem_table() as Arc<dyn TableProvider>,
        );
        let log = CommitLog::new();
        let initial_load_completed = Arc::new(AtomicBool::new(false));
        let mut pending_finalize = None;
        let mut pending_commit = None;
        let write_ctx = SessionContext::new();
        let write_session_state = write_ctx.state();
        // Only consulted when a source reports its history is unavailable, which
        // these cases never do; the default carries `RefreshMode::Full`, which is
        // what a rebuild would override it to anyway.
        let refresh = Arc::new(RwLock::new(Refresh::default()));
        let mut context = ApplyContext {
            refresh_sql: None,
            refresh: &refresh,
            dataset_name: &dataset_name,
            metric_labels: &metric_labels,
            caching: None,
            refresh_completion: None,
            initial_load_completed: &initial_load_completed,
            write_ctx: &write_ctx,
            write_session_state: &write_session_state,
            commit_timeout: Duration::from_secs(5),
            pending_finalize: &mut pending_finalize,
            pending_commit: &mut pending_commit,
            deferred_commits: None,
        };

        let applied = task
            .apply_envelope_run(
                &mut context,
                vec![
                    make_tracked_envelope(1, Arc::clone(&log), false),
                    make_widened_tracked_envelope(2, Arc::clone(&log)),
                ],
            )
            .await;

        // Reset the process-global registry entry.
        remove_cdc_schema_evolution(&dataset_name);

        assert!(applied, "mixed-schema run must apply per group, not fail");
        if let Some(handle) = context.pending_commit.take() {
            handle
                .await
                .expect("commit task join")
                .expect("commit task should succeed");
        }
        assert_eq!(
            log.ids().await,
            vec![1, 2],
            "both schema groups must commit in stream order"
        );
    }

    /// Without an evolution policy installed, a mixed-schema run fails
    /// concat, commits nothing, marks the dataset error, and stops the
    /// stream so a source that tracks delivered envelopes can re-register
    /// and redeliver the window.
    #[tokio::test]
    async fn test_apply_envelope_run_mixed_schemas_without_policy_stops_stream() {
        let dataset_name = TableReference::bare("schema_evo_mixed_block");
        let metric_labels = DatasetMetricLabels::new(&dataset_name);
        let task = make_refresh_task_named(
            "schema_evo_mixed_block",
            make_mem_table() as Arc<dyn TableProvider>,
        );
        let log = CommitLog::new();
        let initial_load_completed = Arc::new(AtomicBool::new(false));
        let mut pending_finalize = None;
        let mut pending_commit = None;
        let write_ctx = SessionContext::new();
        let write_session_state = write_ctx.state();
        // Only consulted when a source reports its history is unavailable, which
        // these cases never do; the default carries `RefreshMode::Full`, which is
        // what a rebuild would override it to anyway.
        let refresh = Arc::new(RwLock::new(Refresh::default()));
        let mut context = ApplyContext {
            refresh_sql: None,
            refresh: &refresh,
            dataset_name: &dataset_name,
            metric_labels: &metric_labels,
            caching: None,
            refresh_completion: None,
            initial_load_completed: &initial_load_completed,
            write_ctx: &write_ctx,
            write_session_state: &write_session_state,
            commit_timeout: Duration::from_secs(5),
            pending_finalize: &mut pending_finalize,
            pending_commit: &mut pending_commit,
            deferred_commits: None,
        };

        let applied = task
            .apply_envelope_run(
                &mut context,
                vec![
                    make_tracked_envelope(1, Arc::clone(&log), false),
                    make_widened_tracked_envelope(2, Arc::clone(&log)),
                ],
            )
            .await;

        assert!(
            !applied,
            "concat failure must stop the stream so later commits cannot skip an uncommitted gap"
        );
        assert!(
            context.pending_commit.is_none(),
            "skipped runs must not commit any envelope"
        );
        assert_eq!(log.ids().await, Vec::<i32>::new());
        assert!(
            task.runtime_status
                .get_component_status("dataset:schema_evo_mixed_block")
                .expect("concat failure should set dataset status")
                .is_error(),
            "mixed-schema concat failure under block must mark the dataset status as error"
        );
    }

    // -- Partitioned Cayenne: a widened CDC batch is refused, never acked -----

    #[cfg(not(windows))]
    use data_accelerator_api::upsert_dedup::wrap_with_upsert_dedup_if_needed;
    #[cfg(not(windows))]
    use data_components::poly::PolyTableProvider;
    #[cfg(not(windows))]
    use datafusion::common::Constraints;
    #[cfg(not(windows))]
    use datafusion::logical_expr::TableProviderFilterPushDown;
    #[cfg(not(windows))]
    use datafusion::scalar::ScalarValue;
    #[cfg(not(windows))]
    use runtime_table_partition::creator::{Error as PartitionCreatorError, PartitionCreator};
    #[cfg(not(windows))]
    use runtime_table_partition::{Partition, expression::PartitionedBy};

    /// A [`PartitionCreator`] that starts empty and backs each new partition
    /// with a writable in-memory table.
    ///
    /// Writable on purpose: with the refusal removed, the widened batch is cast
    /// down and lands here, which is the reported defect. A creator that
    /// refused the write would make the regression tests below pass for the
    /// wrong reason — the run would abort on the failed write rather than on
    /// the refusal.
    ///
    /// `direct_writes` selects the partition write behavior independently of the
    /// sink's schema-evolution capability.
    #[cfg(not(windows))]
    #[derive(Debug)]
    struct MemPartitionCreator {
        direct_writes: bool,
    }

    #[cfg(not(windows))]
    #[async_trait]
    impl PartitionCreator for MemPartitionCreator {
        fn accepts_direct_partition_writes(&self) -> bool {
            self.direct_writes
        }

        async fn create_partition(
            &self,
            partition_values: Vec<ScalarValue>,
        ) -> Result<Partition, PartitionCreatorError> {
            Ok(Partition {
                partition_values,
                table_provider: Arc::new(
                    MemTable::try_new(Arc::new(create_test_data_schema()), vec![vec![]])
                        .expect("partition mem table should be created"),
                ),
            })
        }

        async fn infer_existing_partitions(&self) -> Result<Vec<Partition>, PartitionCreatorError> {
            Ok(Vec::new())
        }

        fn supports_filters_pushdown(
            &self,
            filters: &[&Expr],
        ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
            Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
        }
    }

    /// A partitioned accelerator stack, built the way the Cayenne accelerator
    /// builds one: the partition provider, optionally behind the upsert-dedup
    /// wrapper, under a `PolyTableProvider` and then an index layer.
    ///
    /// Writes must pass through every layer, including deduplication. Schema
    /// policy comes from the sink bound to this composed target.
    #[cfg(not(windows))]
    async fn partitioned_accelerator(dedup: bool, direct_writes: bool) -> Arc<dyn TableProvider> {
        let partitioned = Arc::new(
            PartitionTableProvider::new(
                Arc::new(MemPartitionCreator { direct_writes }),
                vec![PartitionedBy {
                    name: "name".to_string(),
                    expression: datafusion::prelude::col("name"),
                }],
                Arc::new(create_test_data_schema()),
            )
            .await
            .expect("partition provider should be created"),
        );
        let write_provider: Arc<dyn TableProvider> = if dedup {
            wrap_with_upsert_dedup_if_needed(
                partitioned,
                &HashMap::from([("upsert_remove_duplicates".to_string(), "true".to_string())]),
                Constraints::default(),
            )
        } else {
            partitioned
        };
        let poly = Arc::new(PolyTableProvider::new(
            Arc::clone(&write_provider),
            write_provider,
        ))
        .into_table() as Arc<dyn TableProvider>;
        SpiceTable::over(Arc::new(IndexLayer::new()), poly) as Arc<dyn TableProvider>
    }

    /// The `id` values the acceleration currently holds, ascending.
    ///
    /// Reads through the whole provider stack the CDC apply writes to, so it sees
    /// what a query against the dataset would see.
    #[cfg(not(windows))]
    async fn accelerator_row_ids(accelerator: &Arc<dyn TableProvider>) -> Vec<i32> {
        let ctx = SessionContext::new();
        let scan = accelerator
            .scan(&ctx.state(), None, &[], None)
            .await
            .expect("scanning the acceleration should succeed");
        let batches = collect(scan, ctx.task_ctx())
            .await
            .expect("collecting the acceleration should succeed");
        let mut ids: Vec<i32> = batches
            .iter()
            .flat_map(|batch| {
                let column = batch
                    .column_by_name("id")
                    .expect("the acceleration carries an id column");
                column
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("id is Int32")
                    .iter()
                    .flatten()
                    .collect::<Vec<i32>>()
            })
            .collect();
        ids.sort_unstable();
        ids
    }

    /// Apply one widened single-upsert CDC envelope under `on_schema_change:
    /// append_new_columns`, and report `(run applied, committed envelope ids,
    /// dataset status is error)`.
    #[cfg(not(windows))]
    async fn apply_widened_envelope(
        name: &str,
        accelerator: Arc<dyn TableProvider>,
        schema_evolution: SchemaEvolutionSupport,
    ) -> (bool, Vec<i32>, bool) {
        apply_widened_burst(
            name,
            accelerator,
            create_widened_change_batch(1, 30),
            None,
            schema_evolution,
        )
        .await
    }

    /// Apply one widened CDC burst under `on_schema_change: append_new_columns`,
    /// optionally seeding the acceleration with `seed` first, and report
    /// `(run applied, committed envelope ids, dataset status is error)`.
    ///
    /// `seed` is applied through the ordinary CDC write path rather than written
    /// behind it, so a test asserting the acceleration is unmutated is asserting
    /// against rows the apply loop itself put there.
    #[cfg(not(windows))]
    async fn apply_widened_burst(
        name: &str,
        accelerator: Arc<dyn TableProvider>,
        burst: ChangeBatch,
        seed: Option<ChangeBatch>,
        schema_evolution: SchemaEvolutionSupport,
    ) -> (bool, Vec<i32>, bool) {
        let dataset_name = TableReference::bare(name.to_string());
        let metric_labels = DatasetMetricLabels::new(&dataset_name);
        install_cdc_schema_evolution(
            &dataset_name,
            CdcSchemaEvolution {
                policy: OnSchemaChange::AppendNewColumns,
                constraint_columns: vec!["id".to_string()],
            },
        );

        let task = make_refresh_task_named(name, Arc::clone(&accelerator));
        let mut sink_context = runtime_acceleration::change_sink::ChangeSinkContext::new(
            dataset_name.clone(),
            accelerator,
        );
        sink_context.write_lock = Arc::clone(&task.accelerator_write_mutex);
        let backend = runtime_acceleration::change_sink::provider::ProviderChangeSinkBackend::new(
            sink_context,
        )
        .with_schema_evolution(schema_evolution);
        assert!(
            task.change_sink
                .set(ChangeSink::new(
                    Arc::new(backend),
                    SessionContext::new(),
                    &tokio::runtime::Handle::current(),
                    1,
                ))
                .is_ok(),
            "fixture binds exactly one sink before writing"
        );

        // Seeded through the same apply path the burst under test uses, and before
        // the widening policy can refuse anything: a seed that silently failed to
        // land would leave the "acceleration is unmutated" assertion vacuously
        // true, so it is applied here where its own error still surfaces.
        if let Some(seed) = seed {
            task.write_change(seed)
                .await
                .expect("seeding the acceleration must succeed");
            assert!(
                !accelerator_row_ids(&task.accelerator).await.is_empty(),
                "the seed must be readable before the burst runs, or an assertion that the \
                 acceleration is unmutated holds for the wrong reason"
            );
        }

        let log = CommitLog::new();
        let initial_load_completed = Arc::new(AtomicBool::new(false));
        let mut pending_finalize = None;
        let mut pending_commit = None;
        let write_ctx = SessionContext::new();
        let write_session_state = write_ctx.state();
        let refresh = Arc::new(RwLock::new(Refresh::default()));
        let mut context = ApplyContext {
            refresh_sql: None,
            refresh: &refresh,
            dataset_name: &dataset_name,
            metric_labels: &metric_labels,
            caching: None,
            refresh_completion: None,
            initial_load_completed: &initial_load_completed,
            write_ctx: &write_ctx,
            write_session_state: &write_session_state,
            commit_timeout: Duration::from_secs(5),
            pending_finalize: &mut pending_finalize,
            pending_commit: &mut pending_commit,
            deferred_commits: None,
        };

        let applied = task
            .apply_envelope_run(
                &mut context,
                vec![ChangeEnvelope::new(
                    Box::new(TrackingCommitter {
                        id: 1,
                        log: Arc::clone(&log),
                        outcome: Ok(()),
                    }),
                    burst,
                    false,
                )],
            )
            .await;

        // Reset the process-global registry entry.
        remove_cdc_schema_evolution(&dataset_name);

        if let Some(handle) = context.pending_commit.take() {
            handle
                .await
                .expect("commit task join")
                .expect("commit task should succeed");
        }
        let status_is_error = task
            .runtime_status
            .get_component_status(&format!("dataset:{name}"))
            .is_some_and(|status| status.is_error());
        (applied, log.ids().await, status_is_error)
    }

    /// Regression test for #13051. A partitioned Cayenne acceleration cannot
    /// evolve in place and a restart will not repair it either, so a widening
    /// CDC batch must fail the apply rather than be cast down to the old
    /// schema and reported as applied — the acknowledgement is what makes the
    /// dropped values unrecoverable.
    ///
    /// Run for both stack shapes: the upsert-dedup wrapper must not hide the
    /// partition provider from the refusal.
    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_widened_cdc_batch_on_partitioned_cayenne_is_never_acked() {
        for dedup in [false, true] {
            let (applied, committed, status_is_error) = apply_widened_envelope(
                &format!("schema_evo_partitioned_cayenne_dedup_{dedup}"),
                partitioned_accelerator(dedup, true).await,
                SchemaEvolutionSupport::Recreate,
            )
            .await;

            assert_eq!(
                committed,
                Vec::<i32>::new(),
                "dedup={dedup}: the source position must not advance past a widening the acceleration cannot apply"
            );
            assert!(
                !applied,
                "dedup={dedup}: the run must stop rather than continue past an uncommitted gap"
            );
            assert!(
                status_is_error,
                "dedup={dedup}: refusing the widening must surface as a dataset error, not a silent skip"
            );
        }
    }

    /// Regression test for #13455. A refused widening must leave the acceleration
    /// exactly as it found it.
    ///
    /// `group_into_sub_batches` splits a burst by operation and dispatches the
    /// pieces in order, putting a `Delete` ahead of the `Upsert` that recreates
    /// the same key and flushing a `Truncate` as a barrier. While the refusal was
    /// raised from inside `process_upsert_batch`, both of those destructive halves
    /// had already committed by the time it fired — and because the refusal stops
    /// the run without acknowledging, the source redelivered the same burst, which
    /// re-applied the same destructive half and was refused again. The rows the
    /// burst was replacing were gone for good while the acceleration stayed
    /// queryable, so queries returned wrong results indefinitely.
    ///
    /// Both burst shapes come off real sources: `DELETE k, INSERT k` is how
    /// Debezium/Postgres reports an ordinary primary-key update, and `TRUNCATE,
    /// INSERT` is a reload.
    ///
    /// The non-acknowledgement assertions alone do not catch this — they were
    /// satisfied by the broken behavior, since a burst that refuses mid-way
    /// acknowledges nothing either. The row-level assertion is the one that fails
    /// without the preflight.
    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_refused_widening_burst_leaves_the_partitioned_acceleration_intact() {
        for (case, ops) in [
            ("delete_then_insert", ["d", "c"]),
            ("truncate_then_insert", ["t", "c"]),
        ] {
            let accelerator = partitioned_accelerator(false, true).await;
            let (applied, committed, status_is_error) = apply_widened_burst(
                &format!("schema_evo_partitioned_burst_{case}"),
                Arc::clone(&accelerator),
                create_widened_change_batch_ops(&ops, &[7, 7], 30),
                // Seeded under the pre-widening schema, in the partition the
                // burst's own rows land in, so the destructive half of the burst
                // has something of the operator's to destroy.
                Some(create_test_change_batch(
                    vec!["c"],
                    &[vec!["id"]],
                    vec![7],
                    vec![Some("row")],
                )),
                SchemaEvolutionSupport::Recreate,
            )
            .await;

            assert_eq!(
                accelerator_row_ids(&accelerator).await,
                vec![7],
                "{case}: a refused burst must not have applied its destructive half — the source \
                 redelivers this burst unchanged, so anything it committed here is re-committed \
                 on every retry and the rows it deleted never come back"
            );
            assert_eq!(
                committed,
                Vec::<i32>::new(),
                "{case}: the source position must not advance past a widening the acceleration \
                 cannot apply"
            );
            assert!(
                !applied,
                "{case}: the run must stop rather than continue past an uncommitted gap"
            );
            assert!(
                status_is_error,
                "{case}: refusing the widening must surface as a dataset error, not a silent skip"
            );
        }
    }

    /// A widened burst carrying no upsert keeps applying: `process_delete_batch`
    /// and `process_truncate` work from the primary keys alone and never write the
    /// incoming data schema into the acceleration, so there is nothing for the
    /// widening to be refused over — and refusing anyway would stall replication
    /// on a burst that applies cleanly.
    ///
    /// This is the boundary of the preflight above rather than incidental: the
    /// preflight is reached by every burst, so without the upsert gate a
    /// delete-only burst would start classifying, which no burst shape did before.
    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_widened_delete_only_burst_still_applies_to_a_partitioned_cayenne() {
        for (case, ops) in [("delete_only", ["d"]), ("truncate_only", ["t"])] {
            let accelerator = partitioned_accelerator(false, true).await;
            let (applied, committed, status_is_error) = apply_widened_burst(
                &format!("schema_evo_partitioned_destructive_only_{case}"),
                Arc::clone(&accelerator),
                create_widened_change_batch_ops(&ops, &[7], 30),
                Some(create_test_change_batch(
                    vec!["c"],
                    &[vec!["id"]],
                    vec![7],
                    vec![Some("row")],
                )),
                SchemaEvolutionSupport::Recreate,
            )
            .await;

            assert_eq!(
                accelerator_row_ids(&accelerator).await,
                Vec::<i32>::new(),
                "{case}: the burst applies, so the seeded row is removed"
            );
            assert_eq!(
                committed,
                vec![1],
                "{case}: a burst that applied in full must acknowledge its source"
            );
            assert!(applied, "{case}: the run must continue");
            assert!(!status_is_error, "{case}: the dataset must not be errored");
        }
    }

    /// Control: the refusal is specific to a Cayenne partitioned write target.
    ///
    /// An unpartitioned accelerator keeps the documented cast-and-warn
    /// behavior, which a restart repairs, so its envelope still commits — and
    /// so does a partitioned provider whose creator does not accept direct
    /// partition writes, which is what distinguishes a Cayenne accelerator's
    /// partitions from any other engine's. Without these, the test above would
    /// also pass if the refusal fired for every dataset, or for every
    /// partitioned one.
    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_widened_cdc_batch_outside_a_partitioned_cayenne_still_commits() {
        for (case, accelerator) in [
            ("unpartitioned", make_mem_table() as Arc<dyn TableProvider>),
            (
                "partitioned_non_cayenne",
                partitioned_accelerator(false, false).await,
            ),
        ] {
            let (applied, committed, status_is_error) = apply_widened_envelope(
                &format!("schema_evo_{case}"),
                accelerator,
                SchemaEvolutionSupport::Restart,
            )
            .await;

            assert_eq!(
                committed,
                vec![1],
                "{case}: must keep committing under the restart-repaired fallback"
            );
            assert!(applied, "{case}: the run must continue");
            assert!(!status_is_error, "{case}: the dataset must not be errored");
        }
    }

    // -- Correctness: clean termination on stream end -------------------------

    #[tokio::test]
    async fn test_start_changes_stream_terminates_on_stream_end() {
        let task = make_refresh_task(make_mem_table() as Arc<dyn TableProvider>);
        let log = CommitLog::new();

        // Empty stream: returns None immediately. start_changes_stream must
        // exit cleanly (does not hang).
        let stream = make_changes_stream(vec![]);

        let res = tokio::time::timeout(
            Duration::from_secs(5),
            run_changes_stream(&task, stream, None, Arc::new(AtomicBool::new(false))),
        )
        .await
        .expect("must not hang on empty stream");
        res.expect("must return Ok on empty stream");
        assert!(log.ids().await.is_empty());
    }

    #[tokio::test]
    async fn test_join_pending_commit_reports_panic_during_shutdown() {
        let dataset_name = TableReference::bare("test");
        let handle = tokio::spawn(async {
            panic!("synthetic commit panic");
        });

        let error_message =
            join_pending_commit(handle, &dataset_name, true, Duration::from_secs(5))
                .await
                .expect("panic must be reported even during shutdown");

        assert!(
            error_message.contains("CDC commit task for test panicked"),
            "unexpected error message: {error_message}",
        );
    }

    #[tokio::test]
    async fn test_join_pending_commit_ignores_cancel_during_shutdown() {
        let dataset_name = TableReference::bare("test");
        let handle = tokio::spawn(std::future::pending::<Result<(), String>>());
        handle.abort();

        let result = join_pending_commit(handle, &dataset_name, true, Duration::from_secs(5)).await;

        assert!(
            result.is_none(),
            "cancelled commit task should be ignored during shutdown"
        );
    }

    // -- Correctness: dataset-ready signaling ---------------------------------

    #[tokio::test]
    async fn test_start_changes_stream_signals_dataset_ready() {
        let task = make_refresh_task(make_mem_table() as Arc<dyn TableProvider>);
        let log = CommitLog::new();
        let initial_load = Arc::new(AtomicBool::new(false));
        let refresh_completion = RefreshCompletion::new();

        // Take the waiter before the stream runs, and await it only after the
        // stream has finished — the ordering that loses an edge-triggered
        // wakeup entirely (#13086).
        let waiter = refresh_completion.next();

        let stream = make_changes_stream(vec![
            Ok(make_tracked_envelope(1, Arc::clone(&log), false)),
            Ok(make_tracked_envelope(2, Arc::clone(&log), true)), // ready=true
            Ok(make_tracked_envelope(3, Arc::clone(&log), false)),
        ]);
        run_changes_stream(
            &task,
            stream,
            Some(refresh_completion.clone()),
            Arc::clone(&initial_load),
        )
        .await
        .expect("start_changes_stream should succeed");

        assert!(
            initial_load.load(Ordering::Relaxed),
            "initial_load_completed must flip to true once a ready envelope is processed"
        );
        tokio::time::timeout(Duration::from_secs(5), waiter.wait())
            .await
            .expect("a ready envelope must release a waiter taken before the stream ran");
    }

    // -- Correctness: readiness heartbeats bypass the write/durability path ---

    /// Build a zero-row readiness heartbeat envelope over the unit-test data
    /// schema, as CDC connectors emit (#11777) roughly once a second on a
    /// caught-up source.
    fn make_heartbeat_envelope(is_ready: bool) -> ChangeEnvelope {
        let schema = Arc::new(create_test_data_schema());
        cdc::build_heartbeat_envelope(&schema, cdc::now_unix_ms(), is_ready)
            .expect("heartbeat envelope builds")
    }

    /// Observations recorded on `dataset_acceleration_refresh_duration_ms` for
    /// one dataset at one `mode`. The histogram's sample count is what says a
    /// re-read happened; its buckets are what say how long it took.
    fn refresh_duration_samples(registry: &prometheus::Registry, dataset: &str, mode: &str) -> u64 {
        for family in registry.gather() {
            if family.name() != "dataset_acceleration_refresh_duration_ms"
                || family.get_field_type() != prometheus::proto::MetricType::HISTOGRAM
            {
                continue;
            }
            for series in family.get_metric() {
                let labels = series.get_label();
                let matches = |key: &str, value: &str| {
                    labels
                        .iter()
                        .any(|label| label.name() == key && label.value() == value)
                };
                if matches("dataset", dataset)
                    && matches("mode", mode)
                    && let Some(histogram) = series.get_histogram().as_ref()
                {
                    return histogram.get_sample_count();
                }
            }
        }
        0
    }

    /// A rebuild is the `refresh_mode: changes` equivalent of a refresh, and is
    /// reported as one: it runs through `RefreshTask::run` at `RefreshMode::Full`,
    /// so it lands on `dataset_acceleration_refresh_duration_ms{mode="full"}` with
    /// the duration of the re-read. That is the whole alerting story for a
    /// changes-mode dataset, because nothing else it does emits a `full` point —
    /// so the assertion is on the labelled series, not just on the metric.
    ///
    /// Guards the coupling rather than the arithmetic: `rebuild_from_source`
    /// re-entering the refresh path is what puts a re-read on an operator's
    /// dashboard at all, and it is only two lines from being a bespoke reload
    /// that reports nothing.
    #[tokio::test]
    async fn a_rebuild_is_reported_as_a_full_refresh_of_the_changes_dataset() {
        let registry = crate::accelerated::refresh_task::test_prometheus_registry().clone();
        let dataset = "rebuild_reported_as_full_refresh";
        let task = make_refresh_task_named(dataset, make_mem_table() as Arc<dyn TableProvider>);

        let dataset_name = TableReference::bare(dataset);
        let metric_labels = DatasetMetricLabels::new(&dataset_name);
        let initial_load_completed = Arc::new(AtomicBool::new(true));
        let mut pending_finalize = None;
        let mut pending_commit = None;
        let write_ctx = SessionContext::new();
        let write_session_state = write_ctx.state();
        // Seated at `Changes`, the mode a dataset on this path actually carries.
        // `Refresh::default()` is `Full`, and starting there would leave the test
        // green even with `rebuild_from_source`'s `Changes` -> `Full` override
        // deleted — while production would reach `run_once`'s
        // `RefreshMode::Changes => unreachable!` instead. The override is the link
        // under test, so the fixture has to make its absence observable.
        let refresh = Arc::new(RwLock::new(Refresh {
            mode: RefreshMode::Changes,
            ..Refresh::default()
        }));
        let mut context = ApplyContext {
            refresh_sql: None,
            refresh: &refresh,
            dataset_name: &dataset_name,
            metric_labels: &metric_labels,
            caching: None,
            refresh_completion: None,
            initial_load_completed: &initial_load_completed,
            write_ctx: &write_ctx,
            write_session_state: &write_session_state,
            commit_timeout: Duration::from_secs(5),
            pending_finalize: &mut pending_finalize,
            pending_commit: &mut pending_commit,
            deferred_commits: None,
        };

        assert_eq!(
            refresh_duration_samples(&registry, dataset, "full"),
            0,
            "control: nothing has re-read the source yet"
        );

        let schema = Arc::new(create_test_data_schema());
        let signal =
            cdc::build_history_unavailable_envelope(&schema).expect("rebuild signal builds");
        assert!(
            task.apply_envelope_run(&mut context, vec![signal]).await,
            "the rebuild must succeed, so the sample below is of a re-read that landed"
        );

        assert_eq!(
            refresh_duration_samples(&registry, dataset, "full"),
            1,
            "a rebuild must be timed as one full refresh of '{dataset}', or a changes-mode \
             dataset re-reads its whole source with nothing to show for it"
        );
    }

    /// Coverage for a `refresh_sql` dataset, whose accelerator is created with
    /// the projected schema while the rebuild signal still carries source rows:
    /// the accelerator write narrows to the accelerated schema by name, so the
    /// replacement lands with the projected columns.
    #[tokio::test]
    async fn listing_rebuild_lands_on_a_projected_accelerator_schema() {
        let dataset = "listing_rebuild_projected_schema";
        let source_schema = Arc::new(create_test_data_schema());
        let projected_schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let federated = Arc::new(
            MemTable::try_new(
                Arc::clone(&source_schema),
                vec![vec![id_name_batch(&[99], &["stale-federated"])]],
            )
            .expect("federated mem table"),
        );
        let accelerator = Arc::new(
            MemTable::try_new(
                Arc::clone(&projected_schema),
                vec![vec![
                    RecordBatch::try_new(
                        Arc::clone(&projected_schema),
                        vec![Arc::new(Int32Array::from(vec![0]))],
                    )
                    .expect("projected accelerator batch"),
                ]],
            )
            .expect("accelerator mem table"),
        );
        let task = make_refresh_task_with_source(
            dataset,
            Arc::clone(&federated) as Arc<dyn TableProvider>,
            Arc::clone(&accelerator) as Arc<dyn TableProvider>,
        );

        let dataset_name = TableReference::bare(dataset);
        let metric_labels = DatasetMetricLabels::new(&dataset_name);
        let initial_load_completed = Arc::new(AtomicBool::new(true));
        let mut pending_finalize = None;
        let mut pending_commit = None;
        let write_ctx = SessionContext::new();
        let write_session_state = write_ctx.state();
        let refresh = Arc::new(RwLock::new(Refresh {
            mode: RefreshMode::Changes,
            ..Refresh::default()
        }));
        let mut context = ApplyContext {
            refresh_sql: Some("SELECT id FROM listing_rebuild_projected_schema"),
            refresh: &refresh,
            dataset_name: &dataset_name,
            metric_labels: &metric_labels,
            caching: None,
            refresh_completion: None,
            initial_load_completed: &initial_load_completed,
            write_ctx: &write_ctx,
            write_session_state: &write_session_state,
            commit_timeout: Duration::from_secs(5),
            pending_finalize: &mut pending_finalize,
            pending_commit: &mut pending_commit,
            deferred_commits: None,
        };

        let listing = id_name_batch(&[1], &["existing"]);
        let signal = cdc::ChangeEnvelope::from_parts(
            Box::new(cdc::NoOpCommitter),
            cdc::wrap_data_as_change_batch(&source_schema, &listing)
                .expect("listing snapshot wraps")
                .with_rebuild_from_this_batch(true),
            false,
            true,
        );
        assert!(
            task.apply_envelope_run(&mut context, vec![signal]).await,
            "a listing rebuild must land on a dataset whose accelerator is a refresh_sql projection"
        );

        let ctx = SessionContext::new();
        let batches = ctx
            .read_table(Arc::clone(&accelerator) as Arc<dyn TableProvider>)
            .expect("read accelerator")
            .collect()
            .await
            .expect("collect accelerator");
        let ids: Vec<i32> = batches
            .iter()
            .flat_map(|batch| {
                let ids = batch
                    .column_by_name("id")
                    .expect("id")
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("id is Int32");
                (0..ids.len()).map(|i| ids.value(i)).collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(
            ids,
            vec![1],
            "the replacement must be the snapshot's rows, narrowed to the accelerated columns"
        );
    }

    /// Copilot harness dual: a later federated scan seeing `b` after the
    /// captured listing `{a}` would write `b` and then backfill `b` again.
    /// Listing-driven overwrite must use the envelope rows, not the federated table.
    #[tokio::test]
    async fn listing_rebuild_overwrites_from_envelope_not_federated_scan() {
        // Before anything records: the acceleration meter binds whichever global
        // provider is installed the first time one of its metrics is touched, so
        // a sample taken before this call never reaches this registry.
        let registry = crate::accelerated::refresh_task::test_prometheus_registry().clone();
        let dataset = "listing_rebuild_same_snapshot";
        let schema = Arc::new(create_test_data_schema());
        let federated = Arc::new(
            MemTable::try_new(
                Arc::clone(&schema),
                vec![vec![id_name_batch(&[99], &["stale-federated"])]],
            )
            .expect("federated mem table"),
        );
        let accelerator = Arc::new(
            MemTable::try_new(
                Arc::clone(&schema),
                vec![vec![id_name_batch(&[0], &["old"])]],
            )
            .expect("accelerator mem table"),
        );
        let task = make_refresh_task_with_source(
            dataset,
            Arc::clone(&federated) as Arc<dyn TableProvider>,
            Arc::clone(&accelerator) as Arc<dyn TableProvider>,
        );

        let dataset_name = TableReference::bare(dataset);
        let metric_labels = DatasetMetricLabels::new(&dataset_name);
        let initial_load_completed = Arc::new(AtomicBool::new(true));
        let mut pending_finalize = None;
        let mut pending_commit = None;
        let write_ctx = SessionContext::new();
        let write_session_state = write_ctx.state();
        let refresh = Arc::new(RwLock::new(Refresh {
            mode: RefreshMode::Changes,
            ..Refresh::default()
        }));
        let mut context = ApplyContext {
            refresh_sql: None,
            refresh: &refresh,
            dataset_name: &dataset_name,
            metric_labels: &metric_labels,
            caching: None,
            refresh_completion: None,
            initial_load_completed: &initial_load_completed,
            write_ctx: &write_ctx,
            write_session_state: &write_session_state,
            commit_timeout: Duration::from_secs(5),
            pending_finalize: &mut pending_finalize,
            pending_commit: &mut pending_commit,
            deferred_commits: None,
        };

        assert_eq!(
            refresh_duration_samples(&registry, dataset, "full"),
            0,
            "control: nothing has replaced the acceleration yet"
        );

        let listing = id_name_batch(&[1], &["existing"]);
        let signal = cdc::ChangeEnvelope::from_parts(
            Box::new(cdc::NoOpCommitter),
            cdc::wrap_data_as_change_batch(&schema, &listing)
                .expect("listing snapshot wraps")
                .with_rebuild_from_this_batch(true),
            false,
            true,
        );
        assert!(
            task.apply_envelope_run(&mut context, vec![signal]).await,
            "listing-driven rebuild must succeed"
        );

        let names = names_in_table(Arc::clone(&accelerator) as Arc<dyn TableProvider>).await;
        assert_eq!(
            names,
            vec!["existing".to_string()],
            "overwrite must use the listing snapshot, not the federated table or prior accelerator rows, got {names:?}"
        );
        assert_eq!(
            refresh_duration_samples(&registry, dataset, "full"),
            1,
            "a listing-driven replace is still one full refresh of '{dataset}'"
        );
    }

    /// Copilot: after a listing rebuild overwrite succeeds, its committer must
    /// finalize independently of later envelopes. Otherwise a later write failure
    /// drops the rebuild committer unacked and `AppliedKeysCommitter::drop`
    /// releases keys whose rows are already present.
    #[tokio::test]
    async fn listing_rebuild_commits_before_a_later_write_failure() {
        let dataset = "listing_rebuild_commits_before_later_fail";
        let schema = Arc::new(create_test_data_schema());
        let federated = Arc::new(
            MemTable::try_new(
                Arc::clone(&schema),
                vec![vec![id_name_batch(&[99], &["stale-federated"])]],
            )
            .expect("federated mem table"),
        );
        let accelerator = Arc::new(
            MemTable::try_new(
                Arc::clone(&schema),
                vec![vec![id_name_batch(&[0], &["old"])]],
            )
            .expect("accelerator mem table"),
        );
        let writes_seen = Arc::new(AtomicUsize::new(0));
        let provider = Arc::new(FailAfterNWrites {
            inner: Arc::clone(&accelerator) as Arc<dyn TableProvider>,
            writes_seen: Arc::clone(&writes_seen),
            allow: 1, // rebuild overwrite lands; the later upsert fails
        });
        let task = make_refresh_task_with_source(
            dataset,
            Arc::clone(&federated) as Arc<dyn TableProvider>,
            provider as Arc<dyn TableProvider>,
        );

        let dataset_name = TableReference::bare(dataset);
        let metric_labels = DatasetMetricLabels::new(&dataset_name);
        let initial_load_completed = Arc::new(AtomicBool::new(true));
        let mut pending_finalize = None;
        let mut pending_commit = None;
        let write_ctx = SessionContext::new();
        let write_session_state = write_ctx.state();
        let refresh = Arc::new(RwLock::new(Refresh {
            mode: RefreshMode::Changes,
            ..Refresh::default()
        }));
        let mut context = ApplyContext {
            refresh_sql: None,
            refresh: &refresh,
            dataset_name: &dataset_name,
            metric_labels: &metric_labels,
            caching: None,
            refresh_completion: None,
            initial_load_completed: &initial_load_completed,
            write_ctx: &write_ctx,
            write_session_state: &write_session_state,
            commit_timeout: Duration::from_secs(5),
            pending_finalize: &mut pending_finalize,
            pending_commit: &mut pending_commit,
            deferred_commits: None,
        };

        let log = CommitLog::new();
        let listing = id_name_batch(&[1], &["existing"]);
        let rebuild = cdc::ChangeEnvelope::from_parts(
            Box::new(TrackingCommitter {
                id: 1,
                log: Arc::clone(&log),
                outcome: Ok(()),
            }),
            cdc::wrap_data_as_change_batch(&schema, &listing)
                .expect("listing snapshot wraps")
                .with_rebuild_from_this_batch(true),
            false,
            true,
        );
        let later = make_tracked_envelope(2, Arc::clone(&log), false);

        assert!(
            !task
                .apply_envelope_run(&mut context, vec![rebuild, later])
                .await,
            "the later upsert must fail the run after the rebuild overwrite"
        );

        // Drain the rebuild's deferred commit (spawned before the later failure).
        if let Some(handle) = context.pending_commit.take() {
            handle
                .await
                .expect("rebuild commit task join")
                .expect("rebuild commit must succeed");
        }
        assert_eq!(
            log.ids().await,
            vec![1],
            "rebuild committer must finalize after overwrite even when a later write fails"
        );
        assert_eq!(
            writes_seen.load(AtomicOrdering::SeqCst),
            2,
            "rebuild overwrite + failed later upsert = two insert_into attempts"
        );
    }

    /// A rebuild signal is an ordering barrier, not a jump to the front of the
    /// run: what the run carries ahead of it is subsumed by the re-read and must
    /// go with it. See `trim_to_rebuild_signal`.
    #[test]
    fn a_run_is_trimmed_to_its_last_rebuild_signal() {
        let log = Arc::new(CommitLog::default());
        let schema = Arc::new(create_test_data_schema());
        let signal =
            || cdc::build_history_unavailable_envelope(&schema).expect("rebuild signal builds");

        // Stale changes queued behind the signal are dropped; the signal and
        // everything after it (post-capture changes, which DO replay) survive.
        let mut run = vec![
            make_tracked_envelope(1, Arc::clone(&log), false),
            // Ready, and discarded: `apply_envelope_run` reads `any_ready` from
            // the TRIMMED run, so this flag must not survive to mark the dataset
            // Ready the moment the replacement lands — before the source has
            // reconnected at the captured head and caught up.
            make_tracked_envelope(2, Arc::clone(&log), true),
            signal(),
            make_tracked_envelope(3, Arc::clone(&log), false),
        ];
        assert!(trim_to_rebuild_signal(&mut run));
        assert_eq!(run.len(), 2, "only the signal and its suffix survive");
        assert!(run[0].history_unavailable(), "the signal leads the run");
        assert!(!run[1].history_unavailable());
        assert!(
            !run.iter().any(cdc::ChangeEnvelope::is_dataset_ready),
            "a readiness flag from a discarded envelope must not survive the trim"
        );

        // Two signals in one coalesced run still get exactly one rebuild, so the
        // envelopes BETWEEN them are subsumed by it too and must not survive.
        let mut run = vec![
            make_tracked_envelope(4, Arc::clone(&log), false),
            signal(),
            make_tracked_envelope(5, Arc::clone(&log), false),
            signal(),
            make_tracked_envelope(6, Arc::clone(&log), false),
        ];
        assert!(trim_to_rebuild_signal(&mut run));
        assert_eq!(run.len(), 2, "the run is trimmed to the LAST signal");
        assert!(run[0].history_unavailable());

        // A signal already at the front loses nothing — the stream-head case
        // every source takes when it raises the signal at subscribe time.
        let mut run = vec![signal(), make_tracked_envelope(7, Arc::clone(&log), false)];
        assert!(trim_to_rebuild_signal(&mut run));
        assert_eq!(run.len(), 2);

        // An ordinary run is untouched, and reports no rebuild.
        let mut run = vec![
            make_tracked_envelope(8, Arc::clone(&log), false),
            make_tracked_envelope(9, Arc::clone(&log), false),
        ];
        assert!(!trim_to_rebuild_signal(&mut run));
        assert_eq!(run.len(), 2);
    }

    /// A run of pure readiness heartbeats must flip the dataset Ready without
    /// ever reaching the accelerator write path — no insert plan is built and
    /// no write executes (#12007: heartbeats forcing the durable CDC path per
    /// beat made Cayenne duplicate rows).
    #[tokio::test]
    async fn test_heartbeat_only_stream_signals_ready_without_touching_the_accelerator() {
        let insert_plan_calls = Arc::new(AtomicUsize::new(0));
        let insert_execution_calls = Arc::new(AtomicUsize::new(0));
        let provider = Arc::new(CountingInsertProvider {
            inner: make_mem_table() as Arc<dyn TableProvider>,
            insert_plan_calls: Arc::clone(&insert_plan_calls),
            insert_execution_calls: Arc::clone(&insert_execution_calls),
        });
        let task = make_refresh_task(provider as Arc<dyn TableProvider>);
        let initial_load = Arc::new(AtomicBool::new(false));
        let refresh_completion = RefreshCompletion::new();

        let stream = make_changes_stream(vec![
            Ok(make_heartbeat_envelope(false)),
            Ok(make_heartbeat_envelope(true)),
            Ok(make_heartbeat_envelope(false)),
        ]);
        run_changes_stream(
            &task,
            stream,
            Some(refresh_completion.clone()),
            Arc::clone(&initial_load),
        )
        .await
        .expect("heartbeat-only stream should succeed");

        assert!(
            initial_load.load(Ordering::Relaxed),
            "a ready heartbeat must still flip initial_load_completed"
        );
        assert_eq!(
            insert_plan_calls.load(AtomicOrdering::SeqCst),
            0,
            "readiness heartbeats must never reach the accelerator write path"
        );
        assert_eq!(
            insert_execution_calls.load(AtomicOrdering::SeqCst),
            0,
            "readiness heartbeats must never execute a write"
        );
    }

    /// Heartbeats interleaved with real change envelopes must not disturb the
    /// data path: every row lands, every real committer commits in stream
    /// order, and the ready flag carried by a heartbeat is honored.
    #[tokio::test]
    async fn test_heartbeats_interleaved_with_data_preserve_apply_and_commit_order() {
        let task = make_refresh_task(make_mem_table() as Arc<dyn TableProvider>);
        let log = CommitLog::new();
        let initial_load = Arc::new(AtomicBool::new(false));
        let refresh_completion = RefreshCompletion::new();

        let stream = make_changes_stream(vec![
            Ok(make_tracked_envelope(1, Arc::clone(&log), false)),
            Ok(make_heartbeat_envelope(false)),
            Ok(make_tracked_envelope(2, Arc::clone(&log), false)),
            Ok(make_heartbeat_envelope(true)),
            Ok(make_tracked_envelope(3, Arc::clone(&log), false)),
        ]);
        run_changes_stream(
            &task,
            stream,
            Some(refresh_completion.clone()),
            Arc::clone(&initial_load),
        )
        .await
        .expect("mixed stream should succeed");

        assert_eq!(
            log.ids().await,
            vec![1, 2, 3],
            "real committers must commit exactly once, in stream order, with heartbeats stripped"
        );
        assert!(
            initial_load.load(Ordering::Relaxed),
            "the ready flag carried by a heartbeat must be honored"
        );
    }

    // -- Pipelining: verify reader prefetches under a slow apply --------------

    /// `TableProvider` that delays each `insert_into` to simulate a slow
    /// accelerator. Used to expose pipeline overlap: while the apply task
    /// is sleeping inside `insert_into`, the reader task should be free to
    /// drain ahead and fill the prefetch channel.
    #[derive(Debug)]
    struct SlowProvider {
        inner: Arc<dyn TableProvider>,
        delay: Duration,
        writes_started: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl TableProvider for SlowProvider {
        fn schema(&self) -> arrow::datatypes::SchemaRef {
            self.inner.schema()
        }
        fn table_type(&self) -> datafusion::datasource::TableType {
            self.inner.table_type()
        }
        async fn scan(
            &self,
            state: &dyn Session,
            projection: Option<&Vec<usize>>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            self.inner.scan(state, projection, filters, limit).await
        }
        async fn insert_into(
            &self,
            state: &dyn Session,
            input: Arc<dyn ExecutionPlan>,
            insert_op: InsertOp,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            self.writes_started.fetch_add(1, AtomicOrdering::SeqCst);
            tokio::time::sleep(self.delay).await;
            self.inner.insert_into(state, input, insert_op).await
        }
    }

    /// A stream wrapper that increments a counter every time `poll_next`
    /// produces a new item. This makes "items pulled from source" directly
    /// observable.
    struct CountingStream<S> {
        inner: S,
        pulled: Arc<AtomicUsize>,
    }

    impl<S: futures::Stream + Unpin> futures::Stream for CountingStream<S> {
        type Item = S::Item;
        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            match Pin::new(&mut self.inner).poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    self.pulled.fetch_add(1, AtomicOrdering::SeqCst);
                    Poll::Ready(Some(item))
                }
                other => other,
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_start_changes_stream_pipelines_reads_with_writes() {
        // 6 envelopes, accelerator delays 80ms per write. With pipelining,
        // the reader should pull all 6 items into the prefetch channel
        // within the first apply window, well before the writes complete.
        // Without pipelining (serial), pulls and writes would alternate and
        // we'd see at most ~1 pull worth of headroom.
        let writes_started = Arc::new(AtomicUsize::new(0));
        let pulled = Arc::new(AtomicUsize::new(0));

        let slow = Arc::new(SlowProvider {
            inner: make_mem_table() as Arc<dyn TableProvider>,
            delay: Duration::from_millis(80),
            writes_started: Arc::clone(&writes_started),
        });
        let task = make_refresh_task(slow as Arc<dyn TableProvider>);

        let log = CommitLog::new();
        let envelopes: Vec<Result<ChangeEnvelope, CdcStreamError>> = (1..=6)
            .map(|id| Ok(make_tracked_envelope(id, Arc::clone(&log), false)))
            .collect();

        let inner = fstream::iter(envelopes);
        let counting = CountingStream {
            inner: Box::pin(inner),
            pulled: Arc::clone(&pulled),
        };
        let stream: ChangesStream = counting.boxed();

        let task_handle = tokio::spawn(async move {
            run_changes_stream(&task, stream, None, Arc::new(AtomicBool::new(false))).await
        });

        // Wait until the first write has started — that means the apply task
        // has consumed one envelope from the channel and is now in the slow
        // insert. Give it a generous window so this isn't flaky on loaded
        // CI; the assertion below still requires real pipelining.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while writes_started.load(AtomicOrdering::SeqCst) == 0 {
            assert!(
                std::time::Instant::now() <= deadline,
                "apply task never started writing",
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // Poll until the reader has prefetched at least 2 items ahead of the
        // applier, or time out. The invariant we care about — reader ahead of
        // applier under a slow accelerator — must hold during the 80ms apply
        // window; we just don't want to depend on hitting any specific
        // moment in that window. Polling avoids fixed-sleep flakiness under
        // CI scheduling variance.
        let prefetch_deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let p = pulled.load(AtomicOrdering::SeqCst);
            let w = writes_started.load(AtomicOrdering::SeqCst);
            if p >= w + 2 {
                break;
            }
            assert!(
                std::time::Instant::now() <= prefetch_deadline,
                "expected reader to prefetch ahead of applier; pulled={p}, writes_started={w}",
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        task_handle
            .await
            .expect("task join")
            .expect("changes stream should succeed");
        // Final invariant: every envelope was committed exactly once, in order.
        assert_eq!(log.ids().await, vec![1, 2, 3, 4, 5, 6]);
    }

    /// An envelope deferred past the burst byte cap is carried into the next
    /// burst. `max_coalesced_bytes: 1` puts every envelope after the first over
    /// budget, so this drives the carry path on every iteration.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn carried_envelopes_are_discharged_from_the_prefetch_counter_exactly_once() {
        let task = make_refresh_task(make_mem_table() as Arc<dyn TableProvider>);
        let log = CommitLog::new();

        let envelopes: Vec<Result<ChangeEnvelope, CdcStreamError>> = (1..=6)
            .map(|id| Ok(make_tracked_envelope(id, Arc::clone(&log), false)))
            .collect();
        let stream: ChangesStream = fstream::iter(envelopes).boxed();

        let cfg = CdcConfig {
            prefetch_buffer: 128,
            max_coalesced_envelopes: 256,
            // Every envelope after the first exceeds this, so each one is carried
            // rather than folded into the burst - the path under test.
            max_coalesced_bytes: 1,
            max_coalesce_age_ms: 0,
            commit_timeout: Duration::from_secs(30),
            delete_subbatch_max: CDC_DELETE_SUBBATCH_MAX_DEFAULT,
        };

        run_changes_stream_with_config(&task, cfg, stream)
            .await
            .expect("changes stream should succeed");

        // Carrying must not lose, duplicate, or reorder an envelope either.
        assert_eq!(log.ids().await, vec![1, 2, 3, 4, 5, 6]);
    }

    // -- Reliability: reader exits when consumer is dropped -------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_start_changes_stream_reader_exits_on_consumer_drop() {
        // Build a stream that yields one item, then PARKS forever (returns
        // Pending and never wakes). If the reader were not racing on
        // tx.closed(), aborting the parent task would leave the reader
        // stuck in stream.next() and the source would never be dropped.
        struct ParkingForeverStream {
            yielded: bool,
            log: Arc<CommitLog>,
        }
        impl futures::Stream for ParkingForeverStream {
            type Item = Result<ChangeEnvelope, CdcStreamError>;
            fn poll_next(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                if self.yielded {
                    // Pending forever — never registers a waker.
                    Poll::Pending
                } else {
                    self.yielded = true;
                    let env = make_tracked_envelope(1, Arc::clone(&self.log), false);
                    Poll::Ready(Some(Ok(env)))
                }
            }
        }

        let task = make_refresh_task(make_mem_table() as Arc<dyn TableProvider>);
        let log = CommitLog::new();
        let drop_signal = Arc::new(Notify::new());

        let parking = ParkingForeverStream {
            yielded: false,
            log: Arc::clone(&log),
        };
        let drop_signaling = DropSignalStream {
            inner: Box::pin(parking),
            notify_on_drop: Arc::clone(&drop_signal),
        };
        let stream: ChangesStream = drop_signaling.boxed();

        let join = tokio::spawn(async move {
            run_changes_stream(&task, stream, None, Arc::new(AtomicBool::new(false))).await
        });

        // Wait for the first envelope to commit so we know the apply loop is
        // active and the reader is now parked in stream.next().
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if !log.ids().await.is_empty() {
                break;
            }
            assert!(
                std::time::Instant::now() <= deadline,
                "first envelope never committed",
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // Register the drop notifier BEFORE aborting. `Notify::notify_waiters`
        // does not buffer — if we created the `notified()` future after
        // `abort()` returned, the reader could already have torn down the
        // stream and called `notify_waiters` with no waiters registered,
        // which would lose the signal and make this test wait the full
        // timeout for nothing.
        let dropped_fut = drop_signal.notified();
        tokio::pin!(dropped_fut);

        // Abort the parent task. This drops `rx`, which closes `tx`, which
        // must wake the reader's `tokio::select!` and cause it to exit —
        // dropping the source stream as it goes. Without the select-on-
        // tx.closed() guard, the reader would remain alive forever holding
        // the source.
        join.abort();

        let dropped = tokio::time::timeout(Duration::from_secs(2), &mut dropped_fut)
            .await
            .is_ok();
        assert!(
            dropped,
            "reader task did not drop its source stream within 2s after parent abort — \
             this regression would leak source connections at shutdown"
        );
    }

    #[test]
    fn test_get_primary_key_value_null_int32_returns_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let id_array: ArrayRef = Arc::new(Int32Array::from(vec![None]));
        let batch =
            RecordBatch::try_new(schema, vec![id_array]).expect("Failed to create RecordBatch");

        let result = get_primary_key_value(&batch, "id");
        let err =
            result.expect_err("NULL primary key should return an error, not silently produce 0");
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("NULL"),
            "Error should mention NULL: {err_msg}"
        );
    }

    /// A CDC delete keyed on a `NULL` string primary key is refused rather than
    /// turned into `name IN ('')`, which would delete whatever row holds an empty
    /// key. Drives the production delete-predicate builder the CDC apply uses.
    #[test]
    fn a_cdc_delete_with_a_null_utf8_primary_key_is_an_error() {
        use runtime_acceleration::change_sink::provider::deletion::build_batch_delete_expr_from_change_batch;

        let change_batch =
            create_test_change_batch(vec!["d"], &[vec!["name"]], vec![1], vec![None]);

        let err = build_batch_delete_expr_from_change_batch(&change_batch, &[0], "test_dataset")
            .expect_err("a NULL primary key must not become a delete predicate");
        let datafusion::error::DataFusionError::External(source) = &err else {
            panic!("expected the predicate builder's NULL-key error, got: {err}");
        };
        assert!(
            matches!(
                source.downcast_ref::<data_components::pk_filter_expr::Error>(),
                Some(data_components::pk_filter_expr::Error::PrimaryKeyNullValue {
                    field_name,
                    row: 0,
                }) if field_name == "name"
            ),
            "expected the NULL-key error for 'name' at row 0, got: {err}"
        );
        assert_eq!(
            err.to_string(),
            "External error: Primary key column 'name' has NULL value at row 0"
        );
    }

    #[test]
    fn test_get_primary_key_value_non_null_succeeds() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let id_array: ArrayRef = Arc::new(Int32Array::from(vec![42]));
        let batch =
            RecordBatch::try_new(schema, vec![id_array]).expect("Failed to create RecordBatch");

        let result = get_primary_key_value(&batch, "id");
        assert!(result.is_ok(), "Non-null PK should succeed");
        let (str_val, _expr) = result.expect("already asserted Ok");
        assert_eq!(str_val, "42");
    }

    // ----- the reader's build groups -----

    /// The consume loop's source: a change stream whose panic ends it as an item.
    fn caught(stream: cdc::ChangesStream) -> impl futures::Stream<Item = SourceItem> + Unpin {
        std::panic::AssertUnwindSafe(stream).catch_unwind()
    }

    /// A deferred [`cdc::ChangeRows`] that builds a one-row batch.
    struct OneRow {
        encoded_len: usize,
    }

    impl cdc::ChangeRows for OneRow {
        fn is_empty(&self) -> bool {
            false
        }
        fn num_rows_hint(&self) -> usize {
            1
        }
        fn encoded_len(&self) -> usize {
            self.encoded_len
        }
        fn source_commit_ts_ms(&self) -> Option<i64> {
            None
        }
        fn is_heartbeat(&self) -> bool {
            false
        }
        fn build(self: Box<Self>) -> Result<cdc::ChangeBatch, cdc::ChangeBatchError> {
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
            let data = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
            )
            .expect("one-row batch");
            cdc::wrap_data_as_change_batch(&schema, &data)
        }
    }

    fn deferred_envelope_of(encoded_len: usize) -> cdc::ChangeEnvelope {
        cdc::ChangeEnvelope::new_from_rows(
            Box::new(cdc::NoOpCommitter),
            Box::new(OneRow { encoded_len }),
            false,
        )
    }

    fn deferred_envelope() -> cdc::ChangeEnvelope {
        deferred_envelope_of(8)
    }

    fn eager_envelope() -> cdc::ChangeEnvelope {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let data = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
        )
        .expect("one-row batch");
        cdc::ChangeEnvelope::new(
            Box::new(cdc::NoOpCommitter),
            cdc::wrap_data_as_change_batch(&schema, &data).expect("change batch"),
            false,
        )
    }

    #[test]
    fn an_eager_envelope_is_not_held_for_a_build_group() {
        let mut stream = caught(Box::pin(futures::stream::iter(vec![
            Ok(deferred_envelope()),
            Ok(deferred_envelope()),
        ])));
        let (group, ended, _overflow) = take_ready_group(
            Ok(eager_envelope()),
            &mut stream,
            PREBUILD_GROUP_MAX_ENVELOPES,
        );
        assert_eq!(group.len(), 1);
        assert!(!ended);
    }

    #[test]
    fn a_build_group_takes_what_is_ready_and_reports_the_end_of_the_stream() {
        let mut stream = caught(Box::pin(futures::stream::iter(vec![
            Ok(deferred_envelope()),
            Err(cdc::StreamError::External("transient".to_string())),
            Ok(eager_envelope()),
        ])));
        let (group, ended, _overflow) = take_ready_group(
            Ok(deferred_envelope()),
            &mut stream,
            PREBUILD_GROUP_MAX_ENVELOPES,
        );
        assert_eq!(group.len(), 4);
        assert!(
            group[2].is_err(),
            "a stream error keeps its place in the group"
        );
        assert!(ended);
    }

    #[test]
    fn a_build_group_stops_at_what_is_not_ready_yet() {
        let ready = futures::stream::iter(vec![Ok(deferred_envelope()), Ok(deferred_envelope())]);
        let mut stream = caught(Box::pin(ready.chain(futures::stream::pending())));
        let (group, ended, _overflow) = take_ready_group(
            Ok(deferred_envelope()),
            &mut stream,
            PREBUILD_GROUP_MAX_ENVELOPES,
        );
        assert_eq!(group.len(), 3);
        assert!(!ended);
    }

    #[test]
    fn a_build_group_takes_no_more_than_the_room_it_is_given() {
        let mut stream = caught(Box::pin(futures::stream::iter(
            (0..10).map(|_| Ok(deferred_envelope())),
        )));
        let (group, ended, _overflow) = take_ready_group(Ok(deferred_envelope()), &mut stream, 4);
        assert_eq!(group.len(), 4);
        assert!(!ended);
    }

    #[test]
    fn a_build_group_stops_at_its_byte_budget() {
        // Each envelope estimates a third of the budget: the group closes before
        // appending an envelope that would push the combined size over the limit,
        // and carries that envelope for the next group.
        let third = PREBUILD_GROUP_MAX_BYTES / 3 + 1;
        let mut stream = caught(Box::pin(futures::stream::iter(
            (0..10).map(move |_| Ok(deferred_envelope_of(third))),
        )));
        let (group, ended, overflow) = take_ready_group(
            Ok(deferred_envelope_of(third)),
            &mut stream,
            PREBUILD_GROUP_MAX_ENVELOPES,
        );
        assert_eq!(group.len(), 2);
        assert!(
            overflow.is_some(),
            "third envelope is carried for the next group"
        );
        assert!(!ended);
    }

    #[test]
    fn a_build_group_does_not_combine_two_near_limit_envelopes() {
        // Two ready ~7 MiB envelopes must not form a 14 MiB group over the 8 MiB
        // bound; the second is carried alone into the next group.
        let near = 7 * 1024 * 1024;
        let mut stream = caught(Box::pin(futures::stream::iter(vec![Ok(
            deferred_envelope_of(near),
        )])));
        let (group, ended, overflow) = take_ready_group(
            Ok(deferred_envelope_of(near)),
            &mut stream,
            PREBUILD_GROUP_MAX_ENVELOPES,
        );
        assert_eq!(group.len(), 1);
        assert!(overflow.is_some());
        assert!(!ended);
        let (group2, ended2, overflow2) = take_ready_group(
            overflow
                .expect("carried")
                .expect("an envelope, not a panic"),
            &mut stream,
            PREBUILD_GROUP_MAX_ENVELOPES,
        );
        assert_eq!(group2.len(), 1);
        assert!(overflow2.is_none());
        // The carried envelope alone is under the byte budget, so the second call
        // polls the stream again and observes it is exhausted.
        assert!(ended2);
    }

    #[test]
    fn a_build_group_allows_an_individually_oversized_envelope_alone() {
        let over = PREBUILD_GROUP_MAX_BYTES + 1;
        let mut stream = caught(Box::pin(futures::stream::iter(vec![Ok(
            deferred_envelope_of(8),
        )])));
        let (group, ended, overflow) = take_ready_group(
            Ok(deferred_envelope_of(over)),
            &mut stream,
            PREBUILD_GROUP_MAX_ENVELOPES,
        );
        assert_eq!(group.len(), 1);
        // `first` alone already meets the budget, so the loop does not poll; the
        // follow-on stays in the stream for the next group.
        assert!(overflow.is_none());
        assert!(!ended);
        assert!(stream.next().now_or_never().flatten().is_some());
    }

    /// What a deferred envelope's build does in the pipeline tests below.
    #[derive(Clone, Copy)]
    enum TestBuild {
        Succeeds,
        Fails,
        Panics,
    }

    /// A deferred [`cdc::ChangeRows`] carrying one tracked row; counts its builds.
    struct TrackedRows {
        id: i32,
        build: TestBuild,
        builds: Arc<AtomicUsize>,
    }

    impl cdc::ChangeRows for TrackedRows {
        fn is_empty(&self) -> bool {
            false
        }
        fn num_rows_hint(&self) -> usize {
            1
        }
        fn encoded_len(&self) -> usize {
            8
        }
        fn source_commit_ts_ms(&self) -> Option<i64> {
            None
        }
        fn is_heartbeat(&self) -> bool {
            false
        }
        fn build(self: Box<Self>) -> Result<cdc::ChangeBatch, cdc::ChangeBatchError> {
            self.builds.fetch_add(1, AtomicOrdering::SeqCst);
            match self.build {
                TestBuild::Succeeds => Ok(create_test_change_batch(
                    vec!["c"],
                    &[vec!["id"]],
                    vec![self.id],
                    vec![Some("row")],
                )),
                TestBuild::Fails => Err(cdc::ChangeBatchError::DeferredBuild {
                    message: "test build failure".to_string(),
                }),
                TestBuild::Panics => panic!("test build panicked"),
            }
        }
    }

    fn make_deferred_tracked_envelope(
        id: i32,
        log: &Arc<CommitLog>,
        builds: &Arc<AtomicUsize>,
        build: TestBuild,
    ) -> ChangeEnvelope {
        ChangeEnvelope::new_from_rows(
            Box::new(TrackingCommitter {
                id,
                log: Arc::clone(log),
                outcome: Ok(()),
            }),
            Box::new(TrackedRows {
                id,
                build,
                builds: Arc::clone(builds),
            }),
            false,
        )
    }

    /// Holds the first `insert_into` until `builds` reaches `needed`, and records
    /// whether it gave up waiting instead.
    #[derive(Debug)]
    struct FirstWriteHeldForBuilds {
        inner: Arc<dyn TableProvider>,
        builds: Arc<AtomicUsize>,
        needed: usize,
        writes_started: Arc<AtomicUsize>,
        gave_up: Arc<AtomicBool>,
    }

    #[async_trait]
    impl TableProvider for FirstWriteHeldForBuilds {
        fn schema(&self) -> arrow::datatypes::SchemaRef {
            self.inner.schema()
        }
        fn table_type(&self) -> datafusion::datasource::TableType {
            self.inner.table_type()
        }
        async fn scan(
            &self,
            state: &dyn Session,
            projection: Option<&Vec<usize>>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            self.inner.scan(state, projection, filters, limit).await
        }
        async fn insert_into(
            &self,
            state: &dyn Session,
            input: Arc<dyn ExecutionPlan>,
            insert_op: InsertOp,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            if self.writes_started.fetch_add(1, AtomicOrdering::SeqCst) == 0 {
                let deadline = std::time::Instant::now() + Duration::from_secs(5);
                while self.builds.load(AtomicOrdering::SeqCst) < self.needed {
                    if std::time::Instant::now() > deadline {
                        self.gave_up.store(true, AtomicOrdering::SeqCst);
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
            self.inner.insert_into(state, input, insert_op).await
        }
    }

    /// The deferred build of envelopes that arrive while a write is in flight
    /// happens during that write, not after it: the first write is held until
    /// the next two envelopes are built, which the apply loop could only do once
    /// the write it is stuck in had finished.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_reader_builds_arriving_envelopes_while_a_write_is_in_flight() {
        let builds = Arc::new(AtomicUsize::new(0));
        let writes_started = Arc::new(AtomicUsize::new(0));
        let gave_up = Arc::new(AtomicBool::new(false));
        let provider = Arc::new(FirstWriteHeldForBuilds {
            inner: make_mem_table() as Arc<dyn TableProvider>,
            builds: Arc::clone(&builds),
            needed: 3,
            writes_started: Arc::clone(&writes_started),
            gave_up: Arc::clone(&gave_up),
        });
        let task = make_refresh_task(provider as Arc<dyn TableProvider>);
        let log = CommitLog::new();

        let (source, stream) = futures::channel::mpsc::unbounded();
        let stream: ChangesStream = stream.boxed();
        source
            .unbounded_send(Ok(make_deferred_tracked_envelope(
                1,
                &log,
                &builds,
                TestBuild::Succeeds,
            )))
            .expect("queue envelope 1");
        let join = tokio::spawn(async move {
            run_changes_stream_with_config(&task, test_cdc_config(0), stream).await
        });

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while writes_started.load(AtomicOrdering::SeqCst) == 0 {
            assert!(
                std::time::Instant::now() <= deadline,
                "the first write never started"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        for id in [2, 3] {
            source
                .unbounded_send(Ok(make_deferred_tracked_envelope(
                    id,
                    &log,
                    &builds,
                    TestBuild::Succeeds,
                )))
                .expect("queue envelope");
        }
        drop(source);

        join.await
            .expect("task join")
            .expect("changes stream should succeed");
        assert!(
            !gave_up.load(AtomicOrdering::SeqCst),
            "envelopes 2 and 3 were not built while the first write was in flight (builds: {})",
            builds.load(AtomicOrdering::SeqCst)
        );
        assert_eq!(
            builds.load(AtomicOrdering::SeqCst),
            3,
            "each envelope builds once"
        );
        assert_eq!(log.ids().await, vec![1, 2, 3]);
    }

    /// Run deferred envelopes with ids from 1 through the real reader and apply
    /// loop: the first alone, then `later` while the first write is held, so the
    /// reader builds them (the hold lasts until `later_builds` more builds have run,
    /// and records whether it had to give up instead). Returns the committed ids,
    /// the dataset status, and whether the hold gave up.
    async fn run_with_later_envelopes_built_in_the_reader(
        name: &str,
        later: &[TestBuild],
        later_builds: usize,
    ) -> (Vec<i32>, Option<runtime_status::ComponentStatus>, bool) {
        let builds = Arc::new(AtomicUsize::new(0));
        let writes_started = Arc::new(AtomicUsize::new(0));
        let gave_up = Arc::new(AtomicBool::new(false));
        let provider = Arc::new(FirstWriteHeldForBuilds {
            inner: make_mem_table() as Arc<dyn TableProvider>,
            builds: Arc::clone(&builds),
            needed: 1 + later_builds,
            writes_started: Arc::clone(&writes_started),
            gave_up: Arc::clone(&gave_up),
        });
        let task = Arc::new(make_refresh_task_named(
            name,
            provider as Arc<dyn TableProvider>,
        ));
        let log = CommitLog::new();

        let (source, stream) = futures::channel::mpsc::unbounded();
        let stream: ChangesStream = stream.boxed();
        source
            .unbounded_send(Ok(make_deferred_tracked_envelope(
                1,
                &log,
                &builds,
                TestBuild::Succeeds,
            )))
            .expect("queue envelope 1");
        let run_task = Arc::clone(&task);
        let join = tokio::spawn(async move {
            run_changes_stream_with_config(&run_task, test_cdc_config(0), stream).await
        });

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while writes_started.load(AtomicOrdering::SeqCst) == 0 {
            assert!(
                std::time::Instant::now() <= deadline,
                "the first write never started"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        for (id, kind) in (2..).zip(later) {
            source
                .unbounded_send(Ok(make_deferred_tracked_envelope(id, &log, &builds, *kind)))
                .expect("queue envelope");
        }
        drop(source);
        let _ = join.await.expect("task join");

        let status = task
            .runtime_status
            .get_dataset_status(&TableReference::bare(name.to_string()));
        (
            log.ids().await,
            status,
            gave_up.load(AtomicOrdering::SeqCst),
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_deferred_build_stops_the_dataset_with_nothing_committed_from_it_on() {
        // Envelopes 2–4 are built in the reader (all three builds run, the failing
        // one included) while envelope 1's write is held.
        let (committed, status, gave_up) = run_with_later_envelopes_built_in_the_reader(
            "prebuild_failed_build",
            &[TestBuild::Succeeds, TestBuild::Fails, TestBuild::Succeeds],
            3,
        )
        .await;
        assert!(!gave_up, "envelopes 2-4 must be built in the reader");
        assert!(
            committed.iter().all(|id| *id < 3),
            "nothing at or after the failed envelope may commit, got {committed:?}"
        );
        let message = status
            .as_ref()
            .and_then(runtime_status::ComponentStatus::error_message)
            .unwrap_or_default();
        assert!(
            message.contains("test build failure"),
            "the dataset must fail with the build's own error, got {status:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_lost_deferred_build_stops_the_dataset_with_nothing_committed_from_it_on() {
        // Envelopes 2 and 3 reach the reader as one group while envelope 1's write
        // is held; envelope 2's build panics, which loses the group's build task
        // after that one build ran.
        let (committed, status, gave_up) = run_with_later_envelopes_built_in_the_reader(
            "prebuild_lost_build",
            &[TestBuild::Panics, TestBuild::Succeeds],
            1,
        )
        .await;
        assert!(!gave_up, "envelope 2 must be built in the reader");
        assert!(
            committed.iter().all(|id| *id < 2),
            "nothing at or after the lost build may commit, got {committed:?}"
        );
        let message = status
            .as_ref()
            .and_then(runtime_status::ComponentStatus::error_message)
            .unwrap_or_default();
        assert!(
            message.contains("deferred CDC batch build task failed"),
            "the dataset must fail with the lost build's reason, got {status:?}"
        );
    }

    /// While the apply loop is idle the reader forwards deferred envelopes
    /// unbuilt, and the apply loop builds them in its burst.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_idle_apply_loop_builds_its_burst_itself() {
        let task = make_refresh_task_named(
            "prebuild_idle_apply",
            make_mem_table() as Arc<dyn TableProvider>,
        );
        let log = CommitLog::new();
        let builds = Arc::new(AtomicUsize::new(0));
        let items: Vec<Result<ChangeEnvelope, CdcStreamError>> = (1..=4)
            .map(|id| {
                Ok(make_deferred_tracked_envelope(
                    id,
                    &log,
                    &builds,
                    TestBuild::Succeeds,
                ))
            })
            .collect();
        run_changes_stream_with_config(&task, test_cdc_config(0), make_changes_stream(items))
            .await
            .expect("changes stream should succeed");
        assert_eq!(
            builds.load(AtomicOrdering::SeqCst),
            4,
            "each envelope builds once"
        );
        assert_eq!(log.ids().await, vec![1, 2, 3, 4]);
    }
}
