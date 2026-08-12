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
use super::DatasetMetricLabels;
use super::RefreshTask;
use super::{collect_all_indexes, indexes_from_federated};
use crate::accelerated::refresh::Refresh;
use crate::accelerated::refresh_task::deletion::{
    build_batch_delete_expr_from_change_batch, build_pk_only_batch_from_change_batch,
};
use arrow::array::{
    Array, ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray, UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_tools::record_batch::try_cast_to;
use arrow_tools::schema_evolution::{self, EvolutionContext, SchemaEvolution};
use cache::Caching;
#[cfg(not(windows))]
use cayenne::{CayenneCdcWrite, CayenneTableProvider};
use data_components::arrow::{IndexedMemTable, write::MemTable};
use data_components::cdc::{self, ChangeBatch, ChangeOperation, ChangesStream};
use data_components::index_maintenance::perform_index_maintenance;
#[cfg(any(feature = "debezium", feature = "kafka"))]
use data_components::kafka::{
    Error as KafkaError, rdkafka::error::KafkaError as RdKafkaError,
    rdkafka::types::RDKafkaErrorCode,
};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionState;
#[cfg(test)]
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::lit;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::TableReference;
use datafusion::{execution::context::SessionContext, physical_plan::collect};
use futures::{StreamExt, stream};
use runtime_acceleration::dataupdate::StreamingDataUpdateExecutionPlan;
use runtime_component::dataset::OnSchemaChange;
use runtime_component::schema_evolution::{
    SCHEMA_EVOLUTION_APPLIED, SCHEMA_EVOLUTION_DETECTED, SCHEMA_EVOLUTION_FAILED,
    emit_schema_evolution_event, evolution_allowed, schema_evolution_labels, widening_plan_kind,
};
use runtime_datafusion::error::{find_datafusion_root, format_datafusion_error};
use runtime_datafusion::execution_plan::schema_cast::SchemaCastScanExec;
use runtime_metrics::acceleration as metrics;
use runtime_status as status;
use runtime_table_partition::provider::PartitionTableProvider;
#[cfg(test)]
use snafu::OptionExt;
use snafu::ResultExt;
use spice_table::{LayerWalk, SpiceTable, find_concrete};
use std::collections::{HashMap, VecDeque};
use std::hash::BuildHasherDefault;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, RwLock};

type PendingApplyFinalize = tokio::task::JoinHandle<crate::accelerated::Result<()>>;

struct PendingFinalizeCommit {
    finalize: PendingApplyFinalize,
    committers: Vec<Box<dyn cdc::CommitChange + Send + Sync>>,
    ready_after_finalize: bool,
}

/// Source committers deferred by the in-memory CDC durability mode
/// (`cdc_durability: memory`), tagged with the mem-tier epoch they belong to.
///
/// In memory mode the source slot ack is NOT advanced per-batch. Instead each
/// applied batch's committers are pushed here tagged with the batch's mem-tier
/// epoch, and they run (advancing the slot) only when a Cayenne checkpoint
/// reports that epoch durable via [`CayenneSlotAdvancer::on_checkpoint_durable`].
/// This upholds the load-bearing invariant — the slot advances ONLY after the
/// covering checkpoint's Vortex+metastore writes are durable — so a crash that
/// discards the RAM tier always leaves the slot at or below the last durable
/// epoch and the source re-streams the un-checkpointed tail (exactly-once via
/// the PK-idempotent apply). Shared (`Arc`) between the apply loop (which pushes)
/// and the slot advancer installed on the provider (which drains).
type DeferredCommitQueue =
    Arc<tokio::sync::Mutex<VecDeque<(u64, Vec<Box<dyn cdc::CommitChange + Send + Sync>>)>>>;

/// The runtime's [`cayenne::SlotAdvancer`] implementation. Installed on a
/// memory-mode Cayenne provider; when a checkpoint reports an epoch durable it
/// drains every deferred committer with `epoch <= durable_epoch` from the shared
/// [`DeferredCommitQueue`] and runs each `commit()` in order, advancing the
/// source slot — exactly as the per-batch committer would have, only gated
/// behind the durable fence.
struct CayenneSlotAdvancer {
    queue: DeferredCommitQueue,
    dataset_name: TableReference,
    runtime_status: Arc<status::RuntimeStatus>,
}

#[async_trait::async_trait]
impl cayenne::SlotAdvancer for CayenneSlotAdvancer {
    async fn on_checkpoint_durable(&self, durable_epoch: u64) {
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

        // Cross-epoch coalescing is legal here (unlike at push time): every
        // committer in `ready` is at or below the durable fence, so folding the
        // whole prefix to a single max-LSN commit and acking once is equivalent
        // to acking each epoch in turn — O(epochs) work becomes one `fetch_max`.
        // A dataset's deferred queue holds a single committer type, so this is
        // all-or-nothing: only when *every* committer is coalescable do we
        // collapse to a single entry tagged with the highest folded epoch.
        // Order-sensitive or fallible sources are left with their per-epoch
        // structure completely untouched, preserving the in-order,
        // requeue-on-failure drain byte for byte.
        let mut ready = if prefix_is_coalescable(&ready) {
            // `prefix_is_coalescable` guaranteed a non-empty prefix, so `max` is
            // always `Some` here; `unwrap_or(0)` is just the lint-clean spelling
            // of that (this crate denies `unwrap`/`expect` in non-test code). The
            // fold only ever reduces a non-empty input, so `folded` is non-empty.
            let max_epoch = ready.iter().map(|(epoch, _)| *epoch).max().unwrap_or(0);
            let folded = fold_committers(ready.into_iter().flat_map(|(_, cs)| cs).collect());
            VecDeque::from([(max_epoch, folded)])
        } else {
            ready
        };

        while let Some((epoch, committers)) = ready.pop_front() {
            let mut committers = committers.into_iter();
            while let Some(committer) = committers.next() {
                if let Err(e) = committer.commit().await {
                    let mut uncommitted = vec![committer];
                    uncommitted.extend(committers);
                    let mut to_requeue = VecDeque::new();
                    to_requeue.push_back((epoch, uncommitted));
                    to_requeue.append(&mut ready);

                    let mut queue = self.queue.lock().await;
                    while let Some(item) = to_requeue.pop_back() {
                        queue.push_front(item);
                    }

                    // A failed source ack must remain queued. A later immediate
                    // commit is required to observe the non-empty queue and stop
                    // rather than advancing the source past this durable-but-not-
                    // acked checkpoint.
                    if !self.runtime_status.is_shutdown() {
                        tracing::warn!(
                            "Deferred CDC commit failed for {} (source slot will retry before any later immediate commit): {e}",
                            self.dataset_name
                        );
                    }
                    return;
                }
            }
        }
    }
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
        if let Some(last) = folded.last_mut() {
            if last.try_absorb(committer.as_ref()) {
                continue;
            }
            // Within a single dataset's run every coalesce-capable committer
            // targets the same stream position (e.g. one Postgres member slot),
            // so two of the same concrete type must always absorb; a failure
            // means two members' commits were routed into one dataset's run — an
            // upstream (pump) bug, not something to paper over here.
            debug_assert!(
                !matches!(
                    (last.as_any(), committer.as_any()),
                    (Some(a), Some(b)) if a.type_id() == b.type_id()
                ),
                "two coalesce-capable committers of the same type failed to absorb"
            );
        }
        folded.push(committer);
    }
    folded
}

#[cfg(not(windows))]
async fn deferred_commit_queue_is_empty(queue: &DeferredCommitQueue) -> bool {
    queue.lock().await.is_empty()
}

#[cfg(not(windows))]
fn committers_all_support_deferral(
    committers: &[Box<dyn cdc::CommitChange + Send + Sync>],
) -> bool {
    !committers.is_empty()
        && committers
            .iter()
            .all(|committer| committer.supports_deferral())
}

/// Op-granular durable-path decision for one coalesced burst:
/// - `Truncate`/`Unknown` rows always force the durable path (whole burst).
/// - `Delete` rows force it only when the sink cannot absorb key deletes in
///   RAM (`sink_absorbs_in_memory_deletes`, the Cayenne
///   `supports_in_memory_cdc_deletes` capability) or the row carries no
///   primary key (nothing to tombstone — the keyless durable path deletes by
///   full-row match).
/// - `Upsert` rows never force it.
#[cfg(not(windows))]
fn change_batch_requires_durable_cdc_path(
    change_batch: &ChangeBatch,
    sink_absorbs_in_memory_deletes: bool,
) -> bool {
    (0..change_batch.record.num_rows()).any(|row| {
        match ChangeOperationType::from_operation(&change_batch.op(row)) {
            ChangeOperationType::Truncate | ChangeOperationType::Unknown => true,
            ChangeOperationType::Delete => {
                !sink_absorbs_in_memory_deletes || !change_batch.has_primary_keys(row)
            }
            ChangeOperationType::Upsert => false,
        }
    })
}

#[cfg(not(windows))]
async fn checkpoint_pending_memory_cdc_commits(
    cayenne: &CayenneTableProvider,
    queue: &DeferredCommitQueue,
    dataset_name: &TableReference,
    runtime_status: &status::RuntimeStatus,
) -> Option<String> {
    // Bounded checkpoint retries before a still-non-empty queue is declared
    // fatal (declared before the first statement to satisfy pedantic
    // `items_after_statements`).
    const MAX_CHECKPOINT_ATTEMPTS: usize = 3;

    if deferred_commit_queue_is_empty(queue).await {
        return None;
    }

    // A queue still non-empty AFTER a successful checkpoint is, in practice,
    // transient rather than a real invariant violation: the apply loop enqueues a
    // batch's committer only AFTER `write_change` returns its epoch, so the
    // covering checkpoint can fire (and drain nothing) before the committer is
    // queued — the late-enqueue race. `checkpoint_mem_tier` re-fires the slot
    // advancer for the last durable epoch even on an empty tier (Cayenne #11644
    // fix), which releases such a committer; but a concurrent background
    // checkpoint or a straggler epoch can still need one more checkpoint to seal.
    // Retry a bounded number of times before declaring failure — the only
    // correctness requirement is that the source slot must NOT advance past a
    // still-un-durable RAM batch, and waiting (re-checkpointing) preserves that
    // trivially. Only a queue that survives every attempt is fatal (#11644).
    for attempt in 1..=MAX_CHECKPOINT_ATTEMPTS {
        match cayenne.checkpoint_mem_tier().await {
            Ok(_) => {
                if deferred_commit_queue_is_empty(queue).await {
                    return None;
                }
                if runtime_status.is_shutdown() {
                    tracing::debug!(
                        "Deferred CDC commits remain for {dataset_name} during shutdown after mem-tier checkpoint"
                    );
                    return None;
                }
                if attempt < MAX_CHECKPOINT_ATTEMPTS {
                    tracing::debug!(
                        "Deferred CDC commits still queued for {dataset_name} after checkpoint attempt {attempt}/{MAX_CHECKPOINT_ATTEMPTS}; re-checkpointing to seal the straggler epoch"
                    );
                }
            }
            Err(e) => {
                if runtime_status.is_shutdown() {
                    tracing::debug!(
                        "Failed to checkpoint in-memory CDC tier for {dataset_name} during shutdown: {e}"
                    );
                    return None;
                }
                let error_message = format!(
                    "Failed to checkpoint in-memory CDC tier for {dataset_name} before advancing source commit: {e}"
                );
                tracing::error!("{error_message}");
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

pub(super) struct CdcInsertPlanCache {
    target_schema: SchemaRef,
    streaming_plan: Arc<StreamingDataUpdateExecutionPlan>,
    insert_plan: Arc<dyn ExecutionPlan>,
}

impl CdcInsertPlanCache {
    async fn try_new(
        accelerator: &Arc<dyn TableProvider>,
        session_state: &SessionState,
        target_schema: SchemaRef,
    ) -> Result<Self, DataFusionError> {
        let streaming_plan = Arc::new(StreamingDataUpdateExecutionPlan::new_empty(Arc::clone(
            &target_schema,
        )));
        let streaming_exec: Arc<dyn ExecutionPlan> =
            Arc::<StreamingDataUpdateExecutionPlan>::clone(&streaming_plan);
        let cast_plan: Arc<dyn ExecutionPlan> = Arc::new(SchemaCastScanExec::new(
            streaming_exec,
            Arc::clone(&target_schema),
        ));
        let insert_plan = accelerator
            .insert_into(session_state, cast_plan, InsertOp::Append)
            .await?;

        Ok(Self {
            target_schema,
            streaming_plan,
            insert_plan,
        })
    }

    fn matches_schema(&self, schema: &SchemaRef) -> bool {
        self.target_schema.as_ref() == schema.as_ref()
    }
}

struct ApplyContext<'a> {
    refresh_sql: Option<&'a str>,
    dataset_name: &'a TableReference,
    /// Prebuilt per-dataset metric labels reused by hot record sites in the apply loop
    /// (see [`DatasetMetricLabels`]).
    metric_labels: &'a DatasetMetricLabels,
    caching: Option<&'a Weak<Caching>>,
    ready_sender: Option<&'a Arc<Notify>>,
    initial_load_completed: &'a Arc<AtomicBool>,
    write_ctx: &'a SessionContext,
    write_session_state: &'a SessionState,
    commit_timeout: Duration,
    pending_finalize: &'a mut Option<PendingFinalizeCommit>,
    pending_commit: &'a mut Option<tokio::task::JoinHandle<Result<(), String>>>,
    /// Shared queue of source committers DEFERRED by in-memory CDC durability
    /// (`cdc_durability: memory`). When a write returns a mem-tier epoch, its
    /// committers are pushed here (tagged with the epoch) instead of committed
    /// now; the [`CayenneSlotAdvancer`] drains them after the covering
    /// checkpoint is durable. `None` for file-mode streams (committers spawn
    /// immediately, as before).
    deferred_commits: Option<&'a DeferredCommitQueue>,
}

struct WriteChangeOutcome {
    result: WriteChangeResult,
    pending_finalize: Option<PendingApplyFinalize>,
    /// Highest Cayenne in-memory CDC tier epoch this write landed in
    /// (`cdc_durability: memory`), or `None` for every durable-path write. When
    /// set, [`RefreshTask::apply_envelope_run`] DEFERS the source commit: instead
    /// of advancing the slot now, it queues this batch's committers tagged with
    /// the epoch, and runs them only when a checkpoint reports the epoch durable.
    in_memory_epoch: Option<u64>,
}

impl WriteChangeOutcome {
    fn new(result: WriteChangeResult, pending_finalize: Option<PendingApplyFinalize>) -> Self {
        Self {
            result,
            pending_finalize,
            in_memory_epoch: None,
        }
    }

    fn with_in_memory_epoch(mut self, epoch: Option<u64>) -> Self {
        self.in_memory_epoch = epoch;
        self
    }
}

/// Per-upsert-sub-batch outcome: the optional backgrounded finalize plus the
/// optional in-memory CDC tier epoch the batch landed in.
struct UpsertOutcome {
    pending_finalize: Option<PendingApplyFinalize>,
    in_memory_epoch: Option<u64>,
}

/// Outcome of applying one coalesced same-schema group of a run.
enum CoalescedRunOutcome {
    /// Written (and committers handed off); continue with the next group.
    Applied,
    /// The group was skipped without acking (concat failure) — the rest of
    /// the run must also be skipped so later commits can't advance the source
    /// offset past the unapplied envelopes. The stream itself continues.
    SkipRun,
    /// Fatal: stop the stream.
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
    /// Channel depth between the CDC source-stream reader and the apply
    /// loop. Each slot holds one decoded `ChangeEnvelope`, so peak
    /// prefetch memory is `prefetch_buffer * max_batch_bytes`.
    pub prefetch_buffer: usize,
    /// Hard upper bound on the number of `ChangeEnvelope`s coalesced into
    /// a single accelerator write. Coalescing amortizes per-envelope
    /// plan construction over the whole burst.
    pub max_coalesced_envelopes: usize,
    /// Best-effort byte budget for a coalesced burst. A single envelope may
    /// exceed this on its own; otherwise the next envelope is carried into the
    /// next burst before we allocate a concatenated batch.
    pub max_coalesced_bytes: usize,
    /// CDC apply-loop linger window in milliseconds. When `> 0`, the drain
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
const CAYENNE_CDC_SYNCHRONOUS_FALLBACK_WARNING_KEY_LIMIT: usize = 1024;

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

#[cfg(not(windows))]
static CAYENNE_CDC_SYNCHRONOUS_FALLBACK_WARNING_KEYS: std::sync::LazyLock<
    parking_lot::Mutex<BoundedWarningKeys>,
> = std::sync::LazyLock::new(|| parking_lot::Mutex::new(BoundedWarningKeys::default()));

const SCHEMA_EVOLUTION_WARNING_KEY_LIMIT: usize = 1024;

/// Once-per-(dataset, change) gate for schema-evolution warnings so the apply
/// loop doesn't repeat the same warning on every batch of a high-rate stream.
static SCHEMA_EVOLUTION_WARNING_KEYS: std::sync::LazyLock<parking_lot::Mutex<BoundedWarningKeys>> =
    std::sync::LazyLock::new(|| parking_lot::Mutex::new(BoundedWarningKeys::default()));

fn schema_evolution_first_warn(key: String) -> bool {
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
fn cdc_data_schema_matches(target: &SchemaRef, incoming: &SchemaRef) -> bool {
    target.fields().len() == incoming.fields().len()
        && target
            .fields()
            .iter()
            .zip(incoming.fields())
            .all(|(t, i)| t.name() == i.name() && t.data_type() == i.data_type())
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
fn cdc_config() -> CdcConfig {
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
        ready_sender: Option<Arc<Notify>>,
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
            ready_sender,
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
        ready_sender: Option<Arc<Notify>>,
        initial_load_completed: Arc<AtomicBool>,
    ) -> crate::accelerated::Result<()> {
        let dataset_name = self.dataset_name.clone();
        // Prebuilt dataset metric labels reused by this stream's hot record sites
        // (reader send-wait, apply cycle, coalesce flush, burst, fixed-cost
        // phases). Clone is an `Arc` refcount bump — see `DatasetMetricLabels`.
        let metric_labels = self.dataset_metric_labels.clone();
        let sql = refresh.read().await.display_sql();

        self.set_refresh_status(sql.as_deref(), status::ComponentStatus::Refreshing)
            .await;

        // Pipeline source-stream reads with apply+commit by running the source
        // in its own task on the refresh runtime and feeding a bounded channel.
        // While the apply loop writes batch N to the accelerator and commits
        // its source-side offset, the reader task can already be pulling and
        // decoding batch N+1 (network/CPU work that would otherwise be idle).
        // The bounded channel provides natural backpressure: when the apply
        // loop is the bottleneck, the reader parks on `send` and stops pulling.
        // That bounds the number of envelopes in flight, not their size — a full
        // channel holds `prefetch_buffer` batches of whatever width the source
        // produces, which at a large scale factor can be a substantial and
        // otherwise unmeasured share of the process. `cdc_prefetch_buffer_bytes`
        // estimates it.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<
            Result<cdc::ChangeEnvelope, cdc::StreamError>,
        >(cdc_cfg.prefetch_buffer);
        // Weak handle so the apply loop can read the channel's live occupancy
        // (`max_capacity - capacity`) for the backpressure gauge without keeping a
        // strong `Sender` alive — an extra strong sender would stop `rx.recv()`
        // ever returning `None`, hanging end-of-stream. `upgrade()` only succeeds
        // while the reader's real sender lives, so it can never resurrect a closed
        // channel.
        let tx_probe = tx.downgrade();
        // Estimated size of what is queued in the channel above. The capacity
        // bound counts envelopes, so this is not derivable from occupancy: a
        // mid-range envelope count can hold anything from kilobytes to gigabytes
        // depending on how wide the source's batches are. The reader adds an
        // envelope's encoded size as it hands it over and the apply loop
        // subtracts it on receipt, so the value tracks what is queued ahead of
        // apply. `encoded_len` is a decode-free estimate, not measured resident
        // bytes — see `CDC_PREFETCH_BUFFER_BYTES` for what it does and does not
        // claim.
        let prefetch_bytes = Arc::new(AtomicU64::new(0));
        let reader_prefetch_bytes = Arc::clone(&prefetch_bytes);
        // Zeroes the gauge once this stream is gone, however it goes (see
        // `PrefetchBytesGaugeReset`). Held for the whole function so the reset
        // also covers the finalize/commit drain below, not just the apply loop.
        let _prefetch_gauge_reset = PrefetchBytesGaugeReset {
            labels: metric_labels.clone(),
        };

        let reader_dataset = dataset_name.clone();
        let reader_metric_labels = metric_labels.clone();
        let reader_handle = tokio::spawn(async move {
            let mut stream = changes_stream;
            let send_labels = reader_metric_labels.dataset();
            // `select!` on `tx.closed()` lets the reader exit promptly even
            // when it is parked in `stream.next()`. This matters at shutdown:
            // when the parent task is aborted, its locals (including `rx`)
            // are dropped, which closes `tx`. Without this select, a reader
            // blocked on the source (e.g., a Postgres replication recv) would
            // remain alive holding the source connection until the next item
            // happens to arrive. With it, the reader notices the consumer is
            // gone and tears down its source connection immediately.
            loop {
                tokio::select! {
                    biased;
                    () = tx.closed() => {
                        tracing::debug!(
                            "CDC consumer for {reader_dataset} dropped; reader exiting"
                        );
                        return;
                    }
                    item = stream.next() => {
                        let Some(item) = item else { return; };
                        // Charge the envelope before handing it over: once `send`
                        // returns the apply loop may already have taken it and
                        // subtracted, and crediting afterwards could then drive
                        // the counter negative. `encoded_len` does not force a
                        // deferred envelope to build.
                        let queued_bytes = match &item {
                            Ok(envelope) => envelope.encoded_len() as u64,
                            Err(_) => 0,
                        };
                        reader_prefetch_bytes.fetch_add(queued_bytes, Ordering::Relaxed);
                        // Time blocked on send: non-zero => the prefetch channel is
                        // full and the apply loop can't drain fast enough (apply-bound).
                        let send_start = Instant::now();
                        let send_res = tx.send(item).await;
                        if send_res.is_err() {
                            // Nobody will receive it, so nobody will subtract it.
                            discharge_prefetch_bytes(&reader_prefetch_bytes, queued_bytes);
                        }
                        metrics::CDC_READER_SEND_WAIT_MS.record(elapsed_ms(send_start), send_labels);
                        if send_res.is_err() {
                            tracing::debug!(
                                "CDC consumer for {reader_dataset} dropped; reader exiting"
                            );
                            return;
                        }
                    }
                }
            }
        });

        // The previous burst's source-side commit task. Commits are network
        // round-trips to the source (PG `Standby Status Update`, Kafka offset
        // commit, DynamoDB shard checkpoint) that don't need to gate the next
        // apply once the accelerator write has succeeded. Before publishing a
        // new commit task we drain the previous one with `commit_timeout`, so
        // commit(N) overlaps apply(N+1) without accumulating an unbounded chain
        // of tasks if the source-side commit path stalls. Commit task errors
        // are returned through `join_pending_commit` so source offsets cannot
        // silently stop advancing.
        let mut pending_commit: Option<tokio::task::JoinHandle<Result<(), String>>> = None;
        let mut pending_finalize: Option<PendingFinalizeCommit> = None;
        // Previous iteration's recv-start, for the apply-cadence metric
        // (`cdc_apply_cycle_ms`): the period between successive burst applies.
        let mut prev_recv_start: Option<Instant> = None;
        let mut carried_item: Option<Result<cdc::ChangeEnvelope, cdc::StreamError>> = None;
        // Receipt time of `carried_item`, captured when it is carried so the next
        // iteration attributes its wait from true receipt rather than after it sat
        // through this burst's apply. `_ms` (wall clock) feeds the arrival-lag gauge;
        // `_at` (monotonic) feeds the coalesce batch-age. Only read when the next burst
        // starts from the carry; always set together alongside `carried_item`.
        let mut carried_received_ms: Option<i64> = None;
        let mut carried_received_at: Option<Instant> = None;
        let mut last_cycle_start = Instant::now();
        let write_ctx = SessionContext::new();
        let write_session_state = write_ctx.state();
        let recv_wait_labels = metric_labels.dataset();

        // In-memory CDC durability (`cdc_durability: memory`): if this stream's
        // Cayenne provider is memory-capable, set up the deferred-commit queue.
        // The slot advancer is installed per all-deferrable upsert-only burst and
        // cleared for durable-only bursts, so non-replayable sources and deletes
        // never buffer un-acked rows in RAM.
        //
        // Excludes `mode: memory` (memory-resident) tables: they never checkpoint
        // to durable Vortex, so the deferred committers' durability fence
        // (`on_checkpoint_durable`) would never fire — the queue would grow
        // unbounded and the source slot would stall. Memory mode is ephemeral
        // (reload-from-source on restart), so its in-RAM CDC writes take the
        // immediate-commit path below (`in_memory_epoch` with no queue), advancing
        // the slot right after the write — correct because a restart re-snapshots.
        let deferred_commits: Option<DeferredCommitQueue> = {
            #[cfg(not(windows))]
            {
                self.cayenne_accelerator()
                    .filter(|cayenne| {
                        cayenne.is_cdc_memory_mode() && !cayenne.is_memory_resident_mode()
                    })
                    .map(|_cayenne| {
                        Arc::new(tokio::sync::Mutex::new(VecDeque::new())) as DeferredCommitQueue
                    })
            }
            #[cfg(windows)]
            {
                None
            }
        };

        loop {
            // Time how long the apply loop blocks waiting for the next batch
            // from the source-reader channel. Large => source-bound (slot read /
            // WAL decode can't keep up); near-zero => apply-bound (the
            // accelerator write is the bottleneck). Carried-item iterations
            // record ~0, which is correct — no wait occurred. Pairs with
            // CDC_APPLY_BURST_DURATION_MS for full per-batch attribution.
            let recv_start = Instant::now();
            // Apply cadence: period between successive burst recv-starts (ground-truths
            // the per-stage attribution, which overstates the cycle where phases overlap).
            if let Some(prev) = prev_recv_start {
                metrics::CDC_APPLY_CYCLE_MS.record(elapsed_ms(prev), recv_wait_labels);
            }
            prev_recv_start = Some(recv_start);
            let from_carried = carried_item.is_some();
            let next_item = match carried_item.take() {
                Some(item) => Some(item),
                // While waiting for the next source item, also drive any deferred
                // Stage-B finalize from the previous durable burst to completion.
                // The finalize task runs on its own, but its post-finalize side
                // effects (dataset-ready signal, cache invalidation, and the
                // source-offset commit of the finalized burst's committers) are
                // otherwise only applied on the NEXT burst or at end-of-stream. On
                // an idle source — e.g. between the initial snapshot and the first
                // live change in an HTAP workload — that next burst never comes, so
                // without draining the finalize here the dataset would never report
                // ready and the source slot would never advance. `biased` polls the
                // source first, so a busy stream always prefers progress on new data
                // and keeps the finalize pipelined (joined by the next write); only
                // a genuinely idle wait drains the finalize early.
                None => loop {
                    let Some(mut pending) = pending_finalize.take() else {
                        break rx.recv().await;
                    };
                    tokio::select! {
                        biased;
                        item = rx.recv() => {
                            // Source produced an item first: keep the finalize
                            // deferred (the upcoming write path joins it) and
                            // process the item, preserving Stage-A/Stage-B overlap.
                            pending_finalize = Some(pending);
                            break item;
                        }
                        join_result = &mut pending.finalize => {
                            let finalize_error = classify_finalize_result(
                                join_result,
                                &dataset_name,
                                self.runtime_status.is_shutdown(),
                            );
                            if let Some(error_message) = finalize_error {
                                self.set_refresh_status(
                                    sql.as_deref(),
                                    status::ComponentStatus::error_with_message(error_message),
                                )
                                .await;
                                rx.close();
                                reader_handle.abort();
                                break None;
                            }
                            let mut context = ApplyContext {
                                refresh_sql: sql.as_deref(),
                                dataset_name: &dataset_name,
                                metric_labels: &metric_labels,
                                caching: caching.as_ref(),
                                ready_sender: ready_sender.as_ref(),
                                initial_load_completed: &initial_load_completed,
                                write_ctx: &write_ctx,
                                write_session_state: &write_session_state,
                                commit_timeout: cdc_cfg.commit_timeout,
                                pending_finalize: &mut pending_finalize,
                                pending_commit: &mut pending_commit,
                                deferred_commits: deferred_commits.as_ref(),
                            };
                            if !self
                                .run_finalize_side_effects(
                                    &mut context,
                                    pending.committers,
                                    pending.ready_after_finalize,
                                )
                                .await
                            {
                                rx.close();
                                reader_handle.abort();
                                break None;
                            }
                            // Finalize drained (pending_finalize is now None);
                            // loop back to wait for the next item without it.
                        }
                    }
                },
            };
            metrics::CDC_SOURCE_RECV_WAIT_MS.record(elapsed_ms(recv_start), recv_wait_labels);
            // Discharge what this receive took out, before sampling, so the byte
            // gauge and the envelope occupancy below describe the same thing: the
            // backlog still queued, not counting the item now in hand.
            //
            // A CARRIED item is deliberately not discharged here: it left the
            // channel on the previous iteration's `try_recv` and was discharged
            // there. Charging it out twice drove the counter below zero, and an
            // unsigned wrap made the gauge read ~1.8e19.
            if !from_carried && let Some(item) = next_item.as_ref() {
                discharge_prefetch_bytes(&prefetch_bytes, cdc_item_budget_bytes(item) as u64);
            }
            // Sample prefetch-channel occupancy at the moment the apply loop wakes
            // (the just-received `first` is out of the buffer; whatever remains is
            // the backlog the reader has queued ahead). Near capacity => apply-bound.
            if let Some(tx) = tx_probe.upgrade() {
                let capacity = tx.max_capacity() as u64;
                let occupancy = capacity.saturating_sub(tx.capacity() as u64);
                metrics::CDC_PREFETCH_BUFFER_OCCUPANCY.record(occupancy, recv_wait_labels);
                metrics::CDC_PREFETCH_BUFFER_CAPACITY.record(capacity, recv_wait_labels);
                // Sampled next to the envelope count deliberately: read together
                // they say whether a full channel is holding a little or a lot,
                // which the count alone cannot.
                metrics::CDC_PREFETCH_BUFFER_BYTES
                    .record(prefetch_bytes.load(Ordering::Relaxed), recv_wait_labels);
            }
            let Some(first) = next_item else {
                break;
            };
            // Staleness of this envelope AT ARRIVAL (now − its source commit ts):
            // lag already present before the accelerator acts, separating source-side
            // lag from lag the apply path adds (`cdc_source_arrival_lag_ms`).
            if let Ok(env) = &first
                // Exclude heartbeats: their server-clock timestamp would advance the
                // received frontier past data not actually received mid-backlog,
                // corrupting the rate ladder (see ChangeBatch::is_heartbeat).
                && !env.is_heartbeat()
                && let Some(commit_ts_ms) = env.source_commit_ts_ms()
            {
                // Ingress frontier (received commit ts) is recorded once per burst in
                // `apply_burst` using the freshest commit timestamp across the coalesced
                // burst, so it can be compared to the applied frontier (egress) without
                // ever appearing to lag it.
                //
                // Arrival lag is per-burst-first: a carried first was received on the
                // PREVIOUS iteration, so use its captured receipt time; a fresh first is
                // arriving now. Measuring a carried item at process time would fold this
                // burst's apply wait into its arrival lag and — because byte-cap pressure
                // carries an item on nearly every burst — systematically under-sample the
                // histogram in exactly the backlogged regime the metric is meant to diagnose.
                let received_ms = if from_carried {
                    carried_received_ms
                } else {
                    util::time::system_time_to_unix_ms(std::time::SystemTime::now())
                };
                if let Some(now_ms) = received_ms {
                    // `saturating_sub` guards against overflow; `.max(0)` clamps future timestamps
                    // (clock skew / bad source clock) to 0 so we never record negative arrival lag.
                    #[expect(
                        clippy::cast_precision_loss,
                        reason = "arrival lag in ms as f64 for the histogram; sub-ms precision is irrelevant at second/minute-scale backlogs"
                    )]
                    let arrival_lag_ms = now_ms.saturating_sub(commit_ts_ms).max(0) as f64;
                    metrics::CDC_SOURCE_ARRIVAL_LAG_MS.record(arrival_lag_ms, recv_wait_labels);
                }
            }
            // First envelope of this burst is now in hand; time from here until the
            // apply below is the per-batch queued/coalescing latency
            // (`cdc_coalesce_batch_age_ms`). `flush_reason` records what ended the
            // coalesce (`cdc_coalesce_flush_total`). A carried first was received on the
            // previous iteration and waited through the prior burst's apply, so anchor
            // its batch age at that captured receipt rather than "now" (which would
            // undercount the queued term exactly under byte-cap backlog).
            let batch_first_received = if from_carried {
                carried_received_at.unwrap_or_else(Instant::now)
            } else {
                Instant::now()
            };
            let mut linger_hit_deadline = false;
            let mut shutdown_flush = false;
            // Coalesce a contiguous run of buffered envelopes into one
            // accelerator write, in two phases.
            //
            // Phase 1 (always): a non-blocking `try_recv` loop with no `await`,
            // draining whatever is already buffered. With `max_coalesce_age_ms
            // == 0` (default) this is the entire drain, so low load applies a
            // single envelope immediately
            //
            // Phase 2 (linger, only when `max_coalesce_age_ms > 0`): keep
            // awaiting more envelopes until the envelope cap, the byte budget, or
            // a deadline anchored at the START of the previous apply
            // (`last_cycle_start`) is reached.
            let mut burst: Vec<Result<cdc::ChangeEnvelope, cdc::StreamError>> =
                Vec::with_capacity(8);
            let mut burst_bytes = cdc_item_budget_bytes(&first);
            burst.push(first);
            let max_burst = cdc_cfg.max_coalesced_envelopes;
            let max_burst_bytes = cdc_cfg.max_coalesced_bytes;
            // Set when the source-reader channel closes mid-linger: apply the
            // buffered burst, then exit the outer loop (the `rx.recv()` at the
            // top would otherwise observe the same end-of-stream next iteration).
            let mut channel_closed = false;

            while burst.len() < max_burst {
                match rx.try_recv() {
                    Ok(item) => {
                        let item_bytes = cdc_item_budget_bytes(&item);
                        // Out of the channel, so out of the channel's byte count —
                        // whether it joins this burst or is carried to the next.
                        // A carried item is discharged HERE and not again when the
                        // next iteration picks it up.
                        discharge_prefetch_bytes(&prefetch_bytes, item_bytes as u64);
                        if burst_bytes > 0
                            && item_bytes > 0
                            && burst_bytes.saturating_add(item_bytes) > max_burst_bytes
                        {
                            carried_item = Some(item);
                            carried_received_ms =
                                util::time::system_time_to_unix_ms(std::time::SystemTime::now());
                            carried_received_at = Some(Instant::now());
                            break;
                        }
                        burst_bytes = burst_bytes.saturating_add(item_bytes);
                        burst.push(item);
                    }
                    Err(_) => break,
                }
            }

            // Don't linger when the burst is already at/over the byte budget:
            if cdc_cfg.max_coalesce_age_ms > 0
                && carried_item.is_none()
                && burst.len() < max_burst
                && burst_bytes < max_burst_bytes
            {
                // METRIC 4 (`cdc_linger_wait_ms`): wall-clock spent in the Phase-2
                // linger window accumulating envelopes before applying the burst.
                // Recorded only on this branch — when linger is disabled
                // (`max_coalesce_age_ms == 0`, the default) there is no wait to
                // attribute and the histogram stays empty.
                let linger_start = Instant::now();
                let deadline =
                    last_cycle_start + Duration::from_millis(cdc_cfg.max_coalesce_age_ms);
                while burst.len() < max_burst && burst_bytes < max_burst_bytes {
                    // Flush immediately on shutdown rather than waiting out the
                    // window — teardown must not block on intentional linger.
                    if self.runtime_status.is_shutdown() {
                        shutdown_flush = true;
                        break;
                    }
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        linger_hit_deadline = true;
                        break;
                    }
                    // `rx.recv()` is cancel-safe, so a timeout drops no envelope.
                    match tokio::time::timeout(remaining, rx.recv()).await {
                        Ok(Some(item)) => {
                            let item_bytes = cdc_item_budget_bytes(&item);
                            // Out of the channel, so out of the channel's byte
                            // count — burst or carried, it is no longer queued.
                            // A carried item is discharged HERE, once.
                            discharge_prefetch_bytes(&prefetch_bytes, item_bytes as u64);
                            if burst_bytes > 0
                                && item_bytes > 0
                                && burst_bytes.saturating_add(item_bytes) > max_burst_bytes
                            {
                                carried_item = Some(item);
                                carried_received_ms = util::time::system_time_to_unix_ms(
                                    std::time::SystemTime::now(),
                                );
                                carried_received_at = Some(Instant::now());
                                break;
                            }
                            burst_bytes = burst_bytes.saturating_add(item_bytes);
                            burst.push(item);
                        }
                        Ok(None) => {
                            channel_closed = true;
                            break;
                        }
                        // The linger window elapsed with no further envelope — this
                        // is a deadline flush (the common low-volume case), the same
                        // outcome as the `remaining.is_zero()` check above.
                        Err(_elapsed) => {
                            linger_hit_deadline = true;
                            break;
                        }
                    }
                }
                metrics::CDC_LINGER_WAIT_MS.record(elapsed_ms(linger_start), recv_wait_labels);
            }

            // Attribute what ended coalescing and how long the head-of-batch
            // envelope was queued, before the write begins. Priority: a carried
            // item means the next envelope overflowed the byte budget; else a full
            // burst is the envelope cap; else channel-close / shutdown / the linger
            // deadline; else Phase-1 drained the buffer (or linger was disabled).
            let flush_reason = if carried_item.is_some() {
                "byte_cap"
            } else if burst.len() >= max_burst {
                "envelope_cap"
            } else if channel_closed {
                "channel_closed"
            } else if shutdown_flush {
                "shutdown"
            } else if linger_hit_deadline {
                "deadline"
            } else {
                "buffer_drained"
            };
            metrics::CDC_COALESCE_BATCH_AGE_MS
                .record(elapsed_ms(batch_first_received), recv_wait_labels);
            metrics::CDC_COALESCE_FLUSH_TOTAL.add(1, &metric_labels.tagged("reason", flush_reason));

            // Mark the start of this burst's processing cycle: the next burst's
            // linger deadline is measured from here, so the apply below counts as
            // accumulation age.
            last_cycle_start = Instant::now();

            let mut apply_context = ApplyContext {
                refresh_sql: sql.as_deref(),
                dataset_name: &dataset_name,
                metric_labels: &metric_labels,
                caching: caching.as_ref(),
                ready_sender: ready_sender.as_ref(),
                initial_load_completed: &initial_load_completed,
                write_ctx: &write_ctx,
                write_session_state: &write_session_state,
                commit_timeout: cdc_cfg.commit_timeout,
                pending_finalize: &mut pending_finalize,
                pending_commit: &mut pending_commit,
                deferred_commits: deferred_commits.as_ref(),
            };
            // Which cap closed this burst — the tuning signal for `cdc_max_coalesced_envelopes` /
            // `cdc_max_coalesced_bytes` / `cdc_max_coalesce_age_ms`
            let close_reason = if burst.len() >= max_burst {
                "envelope_cap"
            } else if carried_item.is_some() || burst_bytes >= max_burst_bytes {
                "byte_cap"
            } else if channel_closed {
                "stream_end"
            } else if self.runtime_status.is_shutdown() {
                // The linger loop exits early on shutdown; without this arm those
                // bursts would misreport as `age_deadline` and skew tuning signals.
                "shutdown"
            } else if cdc_cfg.max_coalesce_age_ms > 0 {
                "age_deadline"
            } else {
                "drained"
            };
            if !self
                .apply_burst(&mut apply_context, burst, close_reason)
                .await
            {
                rx.close();
                reader_handle.abort();
                break;
            }
            if channel_closed {
                break;
            }
        }

        if let Some(pending) = pending_finalize.take() {
            if let Some(error_message) = join_pending_finalize(
                pending.finalize,
                &dataset_name,
                self.runtime_status.is_shutdown(),
            )
            .await
            {
                self.set_refresh_status(
                    sql.as_deref(),
                    status::ComponentStatus::error_with_message(error_message),
                )
                .await;
            } else {
                let mut context = ApplyContext {
                    refresh_sql: sql.as_deref(),
                    dataset_name: &dataset_name,
                    metric_labels: &metric_labels,
                    caching: caching.as_ref(),
                    ready_sender: ready_sender.as_ref(),
                    initial_load_completed: &initial_load_completed,
                    write_ctx: &write_ctx,
                    write_session_state: &write_session_state,
                    commit_timeout: cdc_cfg.commit_timeout,
                    pending_finalize: &mut pending_finalize,
                    pending_commit: &mut pending_commit,
                    deferred_commits: deferred_commits.as_ref(),
                };
                self.run_finalize_side_effects(
                    &mut context,
                    pending.committers,
                    pending.ready_after_finalize,
                )
                .await;
            }
        }

        // Drain the final in-flight commit before reporting end-of-stream so
        // we don't leave the source-side offset un-acked.
        if let Some(prev) = pending_commit.take()
            && let Some(error_message) = join_pending_commit(
                prev,
                &dataset_name,
                self.runtime_status.is_shutdown(),
                cdc_cfg.commit_timeout,
            )
            .await
        {
            self.set_refresh_status(
                sql.as_deref(),
                status::ComponentStatus::error_with_message(error_message),
            )
            .await;
        }

        // rx returned None: the reader dropped its sender. Three causes:
        //   1) source stream returned None (clean end-of-stream),
        //   2) reader saw `tx.closed()` and exited (consumer was dropped),
        //   3) reader panicked.
        // (1) and (2) join Ok; (3) joins Err with `is_panic()` true. We must
        // surface (3) loudly — silently swallowing it would leave the dataset
        // appearing healthy/ready while CDC ingestion has stopped. Cancelled
        // joins are expected during shutdown and do not need to escalate.
        match reader_handle.await {
            Ok(()) => {
                if !self.runtime_status.is_shutdown() {
                    tracing::warn!("Changes stream ended for dataset {dataset_name}");
                }
            }
            Err(e) if e.is_cancelled() => {
                tracing::debug!(
                    "CDC reader task for {dataset_name} was cancelled (likely shutdown)"
                );
            }
            Err(e) if !self.runtime_status.is_shutdown() => {
                let err_msg = format!("CDC reader task ended unexpectedly: {e}");
                tracing::error!("{err_msg} (dataset={dataset_name})");
                self.set_refresh_status(
                    sql.as_deref(),
                    status::ComponentStatus::error_with_message(err_msg),
                )
                .await;
            }
            Err(_) => {
                // Shutdown in progress and reader did not exit cleanly —
                // expected during teardown; nothing to escalate.
            }
        }

        Ok(())
    }

    /// Apply a single coalesced burst of CDC items drained from the prefetch
    /// channel. Splits the burst into contiguous runs of `Ok` envelopes
    /// (which can be coalesced into one accelerator write) and `Err` items
    /// (handled one-by-one as today). Within an `Ok` run we concatenate the
    /// underlying `RecordBatch`es into a single `ChangeBatch` and call
    /// `write_change` once — turning N small writes into one larger write
    /// and amortizing the per-envelope `SessionContext` + `insert_into`
    /// planning cost. After a successful write we append the run's committers
    /// to the ordered background commit chain so source acknowledgements stay
    /// monotonic without blocking catch-up apply work.
    async fn apply_burst(
        &self,
        context: &mut ApplyContext<'_>,
        burst: Vec<Result<cdc::ChangeEnvelope, cdc::StreamError>>,
        close_reason: &'static str,
    ) -> bool {
        let burst_start = Instant::now();
        let burst_envelopes = u64::try_from(burst.len()).unwrap_or(u64::MAX);
        let burst_bytes = burst
            .iter()
            .map(cdc_item_budget_bytes)
            .fold(0_usize, usize::saturating_add);
        let labels = context.metric_labels.dataset();
        metrics::CDC_APPLY_BURST_ENVELOPES.record(burst_envelopes, labels);
        metrics::CDC_APPLY_BURST_BYTES
            .record(u64::try_from(burst_bytes).unwrap_or(u64::MAX), labels);
        // CDC_APPLY_BURST_ROWS_TOTAL is recorded from the built batches in
        // `apply_envelope_run` (exact applied-row count) — `num_rows_hint()`
        // over-counts a PK-changing UPDATE's delete+upsert as two rows.

        // Freshest upstream commit timestamp in this burst, for the CDC
        // replication-lag gauge. Computed here (before the burst is consumed by the
        // apply loop below) but RECORDED only after the burst's Ok runs apply
        // successfully — the gauge reflects APPLIED data, so a failed apply must not
        // report artificially fresh lag. `source_commit_ts_ms` is stamped by the
        // source connector (Postgres commit time, MongoDB change-stream cluster
        // time, Debezium source ts); the max over the burst is the most recent.
        // Sources that don't stamp a timestamp leave it `None`.
        let max_commit_ts_ms = burst
            .iter()
            .filter_map(|item| item.as_ref().ok())
            // Exclude heartbeats: a keepalive interleaved in a backlogged burst carries
            // the server clock, which would inflate the applied frontier + lag gauge
            // (applied appearing to outrun received). See ChangeBatch::is_heartbeat.
            .filter(|env| !env.is_heartbeat())
            .filter_map(cdc::ChangeEnvelope::source_commit_ts_ms)
            .max();
        if let Some(ts) = max_commit_ts_ms {
            metrics::CDC_RECEIVED_COMMIT_UNIX_TIME_MS.record(ts, labels);
        }

        // Walk the burst preserving arrival order, processing contiguous
        // runs of Ok envelopes together and Err items individually so error
        // handling and ordering semantics match the pre-coalesce behavior.
        let mut iter = burst.into_iter().peekable();
        while let Some(item) = iter.next() {
            match item {
                Ok(first_env) => {
                    let mut envelopes = Vec::with_capacity(8);
                    envelopes.push(first_env);
                    while let Some(Ok(_)) = iter.peek() {
                        let Some(Ok(next)) = iter.next() else {
                            unreachable!("peeked Ok above");
                        };
                        envelopes.push(next);
                    }

                    if !self.apply_envelope_run(context, envelopes).await {
                        metrics::CDC_APPLY_BURST_DURATION_MS
                            .record(elapsed_ms(burst_start), labels);
                        return false;
                    }
                }
                Err(e) => {
                    // Transient errors (e.g., Kafka poll timeout) keep the
                    // refresh status healthy; fatal errors flip status to
                    // Error but we do not abort the loop, matching the
                    // pre-coalesce contract.
                    if handle_stream_error(&e, context.dataset_name) == StreamErrorType::Transient {
                        continue;
                    }

                    let error_message = format_datafusion_error(&e);
                    self.set_refresh_status(
                        context.refresh_sql,
                        status::ComponentStatus::error_with_message(error_message),
                    )
                    .await;
                }
            }
        }
        // Per-burst row count is not logged here: it's the exact
        // `CDC_APPLY_BURST_ROWS_TOTAL` metric recorded in `apply_envelope_run`
        // (from the built batches), not the pre-apply `num_rows_hint` upper bound.
        tracing::debug!(
            dataset = %context.dataset_name,
            envelopes = burst_envelopes,
            bytes = burst_bytes,
            close_reason,
            apply_ms = elapsed_ms(burst_start),
            "Applied coalesced CDC change burst"
        );
        metrics::CDC_APPLY_BURST_DURATION_MS.record(elapsed_ms(burst_start), labels);

        // Record CDC progress only now that the burst's Ok runs have applied (the
        // early `return false` above skips it, so a failed apply never reports fresh
        // progress). The raw applied-commit watermark is emitted whenever the burst
        // carried a source timestamp; the derived lag additionally needs a readable
        // wall clock (skipped on pre-epoch / overflow rather than reporting a
        // misleading 0ms).
        if let Some(max_commit_ts_ms) = max_commit_ts_ms {
            metrics::CDC_APPLIED_COMMIT_UNIX_TIME_MS.record(max_commit_ts_ms, labels);
            if let Some(now_ms) = util::time::system_time_to_unix_ms(std::time::SystemTime::now()) {
                metrics::CDC_REPLICATION_LAG_MS
                    .record(now_ms.saturating_sub(max_commit_ts_ms).max(0), labels);
            }
        }
        true
    }

    /// Run the post-finalize side effects for a deferred Stage-B finalize that
    /// has already completed successfully: signal dataset readiness (when this
    /// burst carried the initial-load marker), invalidate cached query results,
    /// and hand the burst's now-durable source committers to the ordered
    /// background commit chain so the source offset/slot advances.
    ///
    /// Shared by every site that drains a [`PendingFinalizeCommit`]: the next
    /// burst's write path, the idle-source race at the top of the apply loop,
    /// and the end-of-stream drain. The caller is responsible for joining the
    /// finalize task itself (and surfacing any finalize error) before calling
    /// this; here the finalize is known to have succeeded. Returns `false` when
    /// a fatal commit error was surfaced and the stream should stop.
    /// Signal the dataset Ready: flip `initial_load_completed`, wake readiness
    /// waiters, then publish the `Ready` component status — in that order, so a
    /// waiter woken by the notify observes the completed flag. The single
    /// definition of the readiness side effect for every apply path (write,
    /// post-finalize, and readiness-only heartbeat runs).
    async fn signal_dataset_ready(&self, context: &ApplyContext<'_>) {
        context
            .initial_load_completed
            .store(true, Ordering::Relaxed);
        if let Some(sender) = context.ready_sender {
            sender.notify_waiters();
        }
        self.update_component_status(status::ComponentStatus::Ready)
            .await;
    }

    async fn run_finalize_side_effects(
        &self,
        context: &mut ApplyContext<'_>,
        committers: Vec<Box<dyn cdc::CommitChange + Send + Sync>>,
        ready_after_finalize: bool,
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

        if !committers.is_empty() {
            #[cfg(not(windows))]
            if let Some(queue) = context.deferred_commits
                && let Some(cayenne) = self.cayenne_accelerator()
                && let Some(error_message) = checkpoint_pending_memory_cdc_commits(
                    cayenne,
                    queue,
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

            *context.pending_commit = Some(spawn_ordered_commit_task(
                committers,
                Arc::clone(&self.runtime_status),
                context.dataset_name.clone(),
            ));
        }
        true
    }

    /// Apply a contiguous run of successful envelopes as a single coalesced
    /// write, then append their commits to the ordered background commit chain.
    async fn apply_envelope_run(
        &self,
        context: &mut ApplyContext<'_>,
        mut envelopes: Vec<cdc::ChangeEnvelope>,
    ) -> bool {
        debug_assert!(
            !envelopes.is_empty(),
            "run must contain at least one envelope"
        );

        // Split envelopes into (committers, batches, ready_flags) preserving
        // arrival order. Committers will be drained sequentially in the
        // background commit task; per-source semantics (e.g., PG `Standby
        // Status Update` carrying the latest LSN, Kafka per-partition
        // offsets) require this ordering.
        let any_ready = envelopes.iter().any(cdc::ChangeEnvelope::is_dataset_ready);

        // Strip zero-row readiness heartbeats from the write/durability path
        // (#12007). Lag-based readiness (#11777) makes CDC connectors emit a
        // heartbeat roughly every second on a caught-up source; the heartbeat's
        // committer is a no-op by construction, but a no-op committer does not
        // support deferral, so leaving heartbeats in the burst forced
        // `requires_durable_cdc_path` — a mem-tier checkpoint plus a durable
        // write transition per heartbeat. Under load those once-a-second forced
        // checkpoints raced Cayenne's pipelined Stage-B staged-append finalize,
        // and its staged-WAL crash recovery "recovered" (double-published or
        // rolled back) in-flight appends — duplicating rows. A heartbeat's only
        // observable effect is its ready flag, already folded into `any_ready`
        // above; dropping its committer is exact because `is_no_op_heartbeat`
        // requires `CommitChange::is_no_op` (nothing to acknowledge). Zero-row
        // envelopes carrying a REAL committer (e.g. the MySQL snapshot-boundary
        // envelope persisting the initial resume token) are not heartbeats
        // under that predicate and keep durability-then-commit ordering.
        envelopes.retain(|env| !env.is_no_op_heartbeat());

        // Readiness-only run: every envelope was a heartbeat. Honor the ready
        // flag and stop — there is nothing to write and nothing to commit, so
        // the run must not touch the write path or force a checkpoint.
        if envelopes.is_empty() {
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

        // Readiness was already folded into `any_ready` before the heartbeat
        // retain, so the per-envelope flag is spent here.
        let (committers, batches): (
            Vec<Box<dyn cdc::CommitChange + Send + Sync>>,
            Vec<ChangeBatch>,
        ) = parts
            .into_iter()
            .map(|(committer, batch, _is_ready)| (committer, batch))
            .unzip();

        // Mixed-schema runs (mid-stream schema evolution): `concat_change_batches`
        // requires equal schemas. When the dataset's policy allows evolution,
        // split the run into contiguous same-schema groups applied in order —
        // the common case stays a single group. With `block` (or no installed
        // settings) the run is one group and a mixed-schema concat keeps
        // today's error/skip behavior verbatim.
        let split_on_schema_change = cdc_schema_evolution_for(context.dataset_name)
            .is_some_and(|evolution| !matches!(evolution.policy, OnSchemaChange::Block));
        let groups = group_run_by_schema(batches, committers, split_on_schema_change);
        let last_group = groups.len().saturating_sub(1);
        for (group_idx, (group_batches, group_committers)) in groups.into_iter().enumerate() {
            // Exact applied-row count for this group, summed from the just-built
            // batches (no extra build — `into_parts` already built them);
            // `num_rows_hint()` would over-count a PK-changing UPDATE as two
            // rows. Computed before `apply_coalesced_run` consumes the batches,
            // but recorded only AFTER the group applies, so a SkipRun/Stop
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
                // A skipped group's committers were dropped without acking —
                // later groups must not apply (their commits would advance the
                // source offset past the skipped, unapplied envelopes).
                CoalescedRunOutcome::SkipRun => return true,
                CoalescedRunOutcome::Stop => return false,
            }
        }
        true
    }

    /// Concatenate one same-schema group of `ChangeBatch`es into a single
    /// accelerator write, then hand the group's committers to the ordered
    /// background commit chain. Split out of [`Self::apply_envelope_run`] so
    /// mixed-schema runs can apply per contiguous same-schema group.
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
                    // Drop committers without acking — the source will
                    // re-send these envelopes on reconnect, and CDC apply
                    // is idempotent at the upsert/delete level.
                    return CoalescedRunOutcome::SkipRun;
                }
            }
        };
        record_cdc_fixed_cost(context.metric_labels, "coalesce", coalesce_start);

        #[cfg(not(windows))]
        let can_defer_current_burst = committers_all_support_deferral(&committers);
        // Capability probe: a key-mode memory-tier Cayenne sink absorbs Delete
        // events as RAM tombstones (deferring their durability to the covering
        // checkpoint exactly like upserts), so delete-bearing bursts stay on
        // the mem path instead of flipping the table durable per burst. Every
        // other sink reports `false` (here: no Cayenne provider resolves) and
        // keeps the old behavior.
        #[cfg(not(windows))]
        let sink_absorbs_in_memory_deletes = self
            .cayenne_accelerator()
            .is_some_and(CayenneTableProvider::supports_in_memory_cdc_deletes);
        #[cfg(not(windows))]
        let requires_durable_cdc_path = !can_defer_current_burst
            || change_batch_requires_durable_cdc_path(
                &coalesced_batch,
                sink_absorbs_in_memory_deletes,
            );

        #[cfg(not(windows))]
        if let Some(queue) = context.deferred_commits
            && let Some(cayenne) = self.cayenne_accelerator()
        {
            if requires_durable_cdc_path {
                if let Some(error_message) = checkpoint_pending_memory_cdc_commits(
                    cayenne,
                    queue,
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
                cayenne.clear_slot_advancer();
            } else {
                cayenne.install_slot_advancer(Arc::new(CayenneSlotAdvancer {
                    queue: Arc::clone(queue),
                    dataset_name: context.dataset_name.clone(),
                    runtime_status: Arc::clone(&self.runtime_status),
                }));
            }
        }

        let write_start = Instant::now();
        match self
            .write_change_with_context(
                coalesced_batch,
                context.write_ctx,
                context.write_session_state,
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

                let mut committers = Some(committers);

                if let Some(finalize) = write_outcome.pending_finalize {
                    *context.pending_finalize = Some(PendingFinalizeCommit {
                        finalize,
                        committers: committers.take().unwrap_or_default(),
                        ready_after_finalize: mark_ready,
                    });
                }

                if let Some(committers) = committers {
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
                            return CoalescedRunOutcome::Stop;
                        }
                        record_cdc_fixed_cost(
                            context.metric_labels,
                            "commit_wait",
                            commit_wait_start,
                        );
                    }

                    // In-memory CDC durability: DEFER this batch's committers behind
                    // the covering checkpoint rather than advancing the slot now. The
                    // batch's data is in RAM only; the slot must not advance until a
                    // checkpoint reports its epoch durable, or a crash would lose the
                    // un-acked-but-slot-advanced tail. Push the committers tagged with
                    // the epoch onto the shared queue; the `CayenneSlotAdvancer`
                    // drains and runs them after the durable checkpoint fence.
                    if let (Some(epoch), Some(queue)) =
                        (write_outcome.in_memory_epoch, context.deferred_commits)
                    {
                        if !committers.is_empty() {
                            queue.lock().await.push_back((epoch, committers));
                        }
                    } else {
                        #[cfg(not(windows))]
                        if let Some(queue) = context.deferred_commits
                            && let Some(cayenne) = self.cayenne_accelerator()
                            && let Some(error_message) = checkpoint_pending_memory_cdc_commits(
                                cayenne,
                                queue,
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

                        *context.pending_commit = Some(spawn_ordered_commit_task(
                            committers,
                            Arc::clone(&self.runtime_status),
                            context.dataset_name.clone(),
                        ));
                    }
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
        self.write_change_with_context(change_batch, &ctx, &session_state)
            .await
            .map(|outcome| outcome.result)
    }

    async fn write_change_with_context(
        &self,
        change_batch: ChangeBatch,
        ctx: &SessionContext,
        session_state: &SessionState,
    ) -> crate::accelerated::Result<WriteChangeOutcome> {
        let dataset_name = self.dataset_name.clone();

        let sub_batches = group_into_sub_batches(&change_batch);

        tracing::trace!(
            "Processing append/change stream batch: dataset={}, rows={}, sub-batches={}",
            self.dataset_name,
            change_batch.record.num_rows(),
            sub_batches.len()
        );

        let mut had_change = false;
        let mut pending_finalize: Option<PendingApplyFinalize> = None;
        // Highest in-memory CDC tier epoch across this coalesced write's upsert
        // sub-batches (`cdc_durability: memory`). The slot deferral keys on the
        // max: draining committers up to the highest epoch covers every earlier
        // one (epochs are monotone). `None` if no sub-batch took the RAM path.
        //
        // SINGLE EPOCH AXIS (sharded mem tier, cayenne §3.4 Fix 1): the epoch
        // cayenne returns is ONE monotone per-apply quantity regardless of the
        // mem-tier shard count — at N==1 it is the single `MemTier::epoch`, and at
        // N>1 it is a shared per-apply slot-ack epoch stamped identically across all
        // shards (NOT a per-shard max, which would be incommensurable across
        // bursts). A given table's shard count is fixed for its lifetime, so this
        // axis is consistent within one table's FIFO commit queue. The cayenne
        // checkpoint reports the MAX captured epoch on the SAME axis (safe because
        // the capture is all-shards-atomic over each shard's full prefix; a MIN
        // would under-ack and stall the slot), so this `max`-then-`on_checkpoint_durable`
        // (`<=` FIFO drain) is correct unchanged for both N==1 and N>1.
        let mut max_in_memory_epoch: Option<u64> = None;
        for (op_type, row_indices) in sub_batches {
            if let Some(finalize) = pending_finalize.take()
                && let Some(error_message) = join_pending_finalize(
                    finalize,
                    &self.dataset_name,
                    self.runtime_status.is_shutdown(),
                )
                .await
            {
                return Err(crate::accelerated::Error::FailedToWriteData {
                    source: DataFusionError::Execution(error_message),
                });
            }

            match op_type {
                ChangeOperationType::Delete => {
                    let op_start = Instant::now();
                    let absorbed_epoch = self
                        .process_delete_batch(&change_batch, &row_indices, ctx, session_state)
                        .await?;
                    // An absorbed delete is RAM-only until the covering
                    // checkpoint, so its epoch must defer this burst's source
                    // commit exactly like an in-memory upsert sub-batch.
                    if let Some(epoch) = absorbed_epoch {
                        max_in_memory_epoch =
                            Some(max_in_memory_epoch.map_or(epoch, |cur| cur.max(epoch)));
                    }
                    tracing::trace!(
                        dataset = %dataset_name,
                        op = "delete",
                        rows = row_indices.len(),
                        duration_ms = elapsed_ms(op_start),
                        "Append/change stream sub-batch processed"
                    );
                    had_change = true;
                }
                ChangeOperationType::Upsert => {
                    let op_start = Instant::now();
                    let outcome = self
                        .process_upsert_batch(&change_batch, &row_indices, ctx, session_state)
                        .await?;
                    pending_finalize = outcome.pending_finalize;
                    if let Some(epoch) = outcome.in_memory_epoch {
                        max_in_memory_epoch =
                            Some(max_in_memory_epoch.map_or(epoch, |cur| cur.max(epoch)));
                    }
                    tracing::trace!(
                        dataset = %dataset_name,
                        op = "upsert",
                        rows = row_indices.len(),
                        duration_ms = elapsed_ms(op_start),
                        "Append/change stream batch sub-batch processed"
                    );
                    had_change = true;
                }
                ChangeOperationType::Truncate => {
                    self.process_truncate(ctx, session_state).await?;
                    had_change = true;
                }
                ChangeOperationType::Unknown => {
                    tracing::error!("Unknown change operation type for {dataset_name}");
                }
            }
        }

        if let Some(ref callback) = self.on_stream_batch_process_callback {
            let mut callback_guard = callback.lock().await;
            let future = callback_guard();
            future.await;
        }

        if had_change {
            Ok(
                WriteChangeOutcome::new(WriteChangeResult::DataWritten, pending_finalize)
                    .with_in_memory_epoch(max_in_memory_epoch),
            )
        } else {
            Ok(WriteChangeOutcome::new(WriteChangeResult::NoChange, None))
        }
    }

    async fn process_upsert_batch(
        &self,
        change_batch: &ChangeBatch,
        row_indices: &[usize],
        ctx: &SessionContext,
        session_state: &SessionState,
    ) -> crate::accelerated::Result<UpsertOutcome> {
        let data_batch = change_batch.data_batch();

        // Mid-stream schema evolution (policy != block): evolve Cayenne live,
        // or surface the detected change loudly for engines that need a
        // restart, BEFORE the narrowing cast below silently drops the change.
        self.maybe_evolve_schema_for_cdc(&data_batch.schema())
            .await?;

        let target_schema = self.accelerator.schema();

        let selected_batch = select_rows(&data_batch, row_indices)?;
        // CDC sources may produce a nullable schema even for fields declared NOT NULL in the
        // accelerator (e.g. Postgres DELETE rows where non-PK columns are absent from the WAL
        // old-tuple). Promote those fields to non-nullable so the batch dtype matches
        // acceleration schema. SchemaCastScanExec handles type coercion;
        // this step only adjusts nullability metadata.
        let selected_batch =
            try_cast_to(selected_batch, Arc::clone(&target_schema)).map_err(|e| {
                crate::accelerated::Error::FailedToBuildRecordBatch {
                    source: arrow::error::ArrowError::SchemaError(e.to_string()),
                }
            })?;

        let record_batch_stream = Box::pin(RecordBatchStreamAdapter::new(
            selected_batch.schema(),
            Box::pin(stream::once(async move { Ok(selected_batch) })),
        ));

        #[cfg(not(windows))]
        if let Some(cayenne) = self.cayenne_accelerator() {
            let task_ctx = ctx.task_ctx();
            let cayenne_write = cayenne
                .write_cdc_append_stream_with_source_commit_ts(
                    record_batch_stream,
                    change_batch.source_commit_ts_ms(),
                    &task_ctx,
                )
                .await
                .map_err(DataFusionError::from)
                .map_err(find_datafusion_root)
                .context(crate::accelerated::FailedToWriteDataSnafu)?;

            self.update_last_updated_at();

            // In-memory CDC tier epoch (`cdc_durability: memory`), captured before
            // `cayenne_write` is consumed. `None` for durable-path writes.
            let in_memory_epoch = cayenne_write.in_memory_epoch();

            if cayenne_write.has_pending_finalize() {
                // A pending finalize is the durable Stage-B path — never memory
                // mode (an in-memory-staged write has nothing to finalize), so no
                // epoch to defer here.
                record_cdc_apply_path(&self.dataset_metric_labels, "durable_append");
                return Ok(UpsertOutcome {
                    pending_finalize: Some(spawn_cayenne_finalize(cayenne_write)),
                    in_memory_epoch: None,
                });
            }
            record_cdc_apply_path(&self.dataset_metric_labels, "inmem_append");

            cayenne_write
                .finish()
                .await
                .map_err(DataFusionError::from)
                .map_err(find_datafusion_root)
                .context(crate::accelerated::FailedToWriteDataSnafu)?;

            return Ok(UpsertOutcome {
                pending_finalize: None,
                in_memory_epoch,
            });
        }

        #[cfg(not(windows))]
        self.warn_if_cayenne_cdc_synchronous_fallback();

        let _lock_guard = self.accelerator_write_mutex.lock().await;

        let (streaming_plan, insert_plan) = {
            let mut cache_guard = self.cdc_insert_plan_cache.lock().await;
            let rebuild_cache = cache_guard
                .as_ref()
                .is_none_or(|cache| !cache.matches_schema(&target_schema));
            if rebuild_cache {
                *cache_guard = Some(
                    CdcInsertPlanCache::try_new(
                        &self.accelerator,
                        session_state,
                        Arc::clone(&target_schema),
                    )
                    .await
                    .map_err(find_datafusion_root)
                    .context(crate::accelerated::FailedToWriteDataSnafu)?,
                );
            }

            let cache = cache_guard.as_ref().ok_or_else(|| {
                crate::accelerated::Error::FailedToWriteData {
                    source: DataFusionError::Execution(
                        "CDC insert plan cache was not initialized".to_string(),
                    ),
                }
            })?;
            cache
                .streaming_plan
                .set_stream(record_batch_stream)
                .map_err(find_datafusion_root)
                .context(crate::accelerated::FailedToWriteDataSnafu)?;
            (
                Arc::clone(&cache.streaming_plan),
                Arc::clone(&cache.insert_plan),
            )
        };

        let collect_result = collect(insert_plan, ctx.task_ctx())
            .await
            .map_err(find_datafusion_root)
            .context(crate::accelerated::FailedToWriteDataSnafu);
        streaming_plan
            .clear_stream()
            .map_err(find_datafusion_root)
            .context(crate::accelerated::FailedToWriteDataSnafu)?;
        collect_result?;
        perform_change_write_maintenance(&self.accelerator).await?;

        self.update_last_updated_at();

        Ok(UpsertOutcome {
            pending_finalize: None,
            in_memory_epoch: None,
        })
    }

    /// Detect a widening schema change between the incoming CDC data struct
    /// and the accelerator schema and act per the dataset's installed
    /// `on_schema_change` policy:
    ///
    /// - Cayenne + allowed widening ⇒ evolve LIVE via the provider's
    ///   `evolve_schema_live` (fence + flush + metastore update + in-memory
    ///   schema swap, idempotent) and continue — the cast below becomes a
    ///   pass-through to the evolved schema. A failed evolve stops the stream;
    ///   the source redelivers and the idempotent evolve self-heals.
    /// - Other engines ⇒ NO mid-life engine DDL: keep today's narrowing cast,
    ///   warn once, count the failure — restart-time evolution applies it.
    /// - `fail` policy ⇒ terminal actionable error.
    /// - `block` policy / no installed settings ⇒ return immediately (today's
    ///   code path verbatim).
    async fn maybe_evolve_schema_for_cdc(
        &self,
        incoming_data_schema: &SchemaRef,
    ) -> crate::accelerated::Result<()> {
        let Some(evolution) = cdc_schema_evolution_for(&self.dataset_name) else {
            return Ok(());
        };
        if matches!(evolution.policy, OnSchemaChange::Block) {
            return Ok(());
        }
        let target_schema = self.accelerator.schema();
        if cdc_data_schema_matches(&target_schema, incoming_data_schema) {
            return Ok(());
        }
        let aligned = align_nullability_for_classify(&target_schema, incoming_data_schema);
        let ctx = EvolutionContext {
            constraint_columns: &evolution.constraint_columns,
        };
        let dataset = self.dataset_name.to_string();
        match schema_evolution::classify(&target_schema, &aligned, &ctx) {
            SchemaEvolution::Identical => Ok(()),
            SchemaEvolution::Widening(plan) => {
                let kind = widening_plan_kind(&plan);
                let change = plan.describe();
                SCHEMA_EVOLUTION_DETECTED
                    .add(1, &schema_evolution_labels(&dataset, kind, "cdc_stream"));
                if matches!(evolution.policy, OnSchemaChange::Fail) {
                    SCHEMA_EVOLUTION_FAILED
                        .add(1, &schema_evolution_labels(&dataset, kind, "fail_policy"));
                    emit_schema_evolution_event(&dataset, "fail_policy", &change, true);
                    return Err(crate::accelerated::Error::FailedToWriteData {
                        source: DataFusionError::Execution(format!(
                            "schema change detected on the CDC stream for {dataset} ({change}) and `on_schema_change: fail` is set. \
                             Revert the source schema change, or set `on_schema_change: append_new_columns`/`sync_all_columns` to evolve"
                        )),
                    });
                }
                if !evolution_allowed(evolution.policy, &plan) {
                    SCHEMA_EVOLUTION_FAILED.add(
                        1,
                        &schema_evolution_labels(&dataset, kind, "blocked_by_policy"),
                    );
                    if schema_evolution_first_warn(format!("{dataset}|policy|{change}")) {
                        tracing::warn!(
                            dataset = %dataset,
                            "widening schema change detected on the CDC stream ({change}) but `on_schema_change: {}` only evolves added columns; values continue to be cast to the current schema. Set `on_schema_change: sync_all_columns` to evolve types",
                            evolution.policy
                        );
                        emit_schema_evolution_event(&dataset, "blocked_by_policy", &change, true);
                    }
                    return Ok(());
                }
                #[cfg(not(windows))]
                if let Some(cayenne) = self.cayenne_accelerator() {
                    cayenne
                        .evolve_schema_live(&plan)
                        .await
                        .map_err(DataFusionError::from)
                        .map_err(find_datafusion_root)
                        .context(crate::accelerated::FailedToWriteDataSnafu)?;
                    SCHEMA_EVOLUTION_APPLIED
                        .add(1, &schema_evolution_labels(&dataset, kind, "cdc_live"));
                    tracing::info!(
                        dataset = %dataset,
                        "applied live schema evolution from the CDC stream: {change}"
                    );
                    emit_schema_evolution_event(&dataset, "cdc_live", &change, false);
                    return Ok(());
                }
                SCHEMA_EVOLUTION_FAILED.add(
                    1,
                    &schema_evolution_labels(&dataset, kind, "restart_required"),
                );
                if schema_evolution_first_warn(format!("{dataset}|restart|{change}")) {
                    tracing::warn!(
                        dataset = %dataset,
                        "widening schema change detected on the CDC stream ({change}) but this acceleration engine cannot evolve mid-stream; incoming values are cast to the current schema (new columns dropped) until restart. Restart Spice to apply the evolution"
                    );
                    emit_schema_evolution_event(&dataset, "restart_required", &change, true);
                }
                Ok(())
            }
            SchemaEvolution::Incompatible { reason } => {
                SCHEMA_EVOLUTION_DETECTED.add(
                    1,
                    &schema_evolution_labels(&dataset, "incompatible", "cdc_stream"),
                );
                if matches!(evolution.policy, OnSchemaChange::Fail) {
                    SCHEMA_EVOLUTION_FAILED.add(
                        1,
                        &schema_evolution_labels(&dataset, "incompatible", "fail_policy"),
                    );
                    emit_schema_evolution_event(&dataset, "fail_policy", &reason, true);
                    return Err(crate::accelerated::Error::FailedToWriteData {
                        source: DataFusionError::Execution(format!(
                            "incompatible schema change detected on the CDC stream for {dataset}: {reason}. `on_schema_change: fail` is set"
                        )),
                    });
                }
                SCHEMA_EVOLUTION_FAILED.add(
                    1,
                    &schema_evolution_labels(&dataset, "incompatible", "incompatible"),
                );
                if schema_evolution_first_warn(format!("{dataset}|incompatible|{reason}")) {
                    tracing::warn!(
                        dataset = %dataset,
                        "incompatible schema change detected on the CDC stream: {reason}. Values continue to be cast to the current schema"
                    );
                    emit_schema_evolution_event(&dataset, "incompatible", &reason, true);
                }
                Ok(())
            }
        }
    }

    /// Resolve the inner [`CayenneTableProvider`] from the accelerator, peeling
    /// the wrappers it is created behind. Non-partitioned Cayenne tables are
    /// wrapped in `PolyTableProvider` (read/write split), optionally
    /// `UpsertDedupTableProvider` (when `remove_duplicates`/`last_write_wins` is
    /// set), and `IndexLayer` (vector indexes). A direct downcast to
    /// `CayenneTableProvider` misses through any of these, so without peeling the
    /// CDC apply silently falls back to the synchronous `insert_into` path and
    /// loses pipelined finalization (backgrounded publish, no blocking
    /// `apply_on_conflict_deletions`).
    ///
    /// Uses [`LayerWalk::Write`], which steps only through wrappers whose
    /// `insert_into` is a pass-through (`PolyTableProvider` to its writer side,
    /// `IndexLayer`), as each layer's `route` declares.
    ///
    /// NOTE: `UpsertDedupTableProvider` is opaque to the write walk. Unlike
    /// `PolyTableProvider` (delegates writes) and `IndexLayer`
    /// (`insert_into` is a pass-through), it *rewrites* the write on insert
    /// (dedup / last-write-wins via `UpsertDedupExec`). Routing CDC past it to the
    /// inner provider would bypass that transform, so a dedup-configured table
    /// instead stays on the synchronous path (through the wrapper, preserving its
    /// semantics) and emits the fallback warning below.
    #[cfg(not(windows))]
    fn cayenne_accelerator(&self) -> Option<&CayenneTableProvider> {
        find_concrete::<CayenneTableProvider>(self.accelerator.as_ref(), LayerWalk::Write)
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

    /// Warn once per table when the CDC apply takes the synchronous fallback for
    /// a Cayenne-engine dataset — i.e. [`Self::cayenne_accelerator`] could not
    /// unwrap the inner provider through its wrappers, so pipelined finalization
    /// is silently disabled. For non-Cayenne accelerators the synchronous path is
    /// expected, so this stays quiet.
    #[cfg(not(windows))]
    fn warn_if_cayenne_cdc_synchronous_fallback(&self) {
        // Mirrors the per-engine-module `SPICE_ACCELERATOR_METADATA_KEY` (defined
        // privately in each accelerator module); the accelerator's schema
        // metadata carries the engine name.
        const ACCELERATOR_METADATA_KEY: &str = "spice.accelerator";
        let is_cayenne = self
            .accelerator
            .schema()
            .metadata()
            .get(ACCELERATOR_METADATA_KEY)
            .map(String::as_str)
            == Some("cayenne");
        if !is_cayenne {
            return;
        }
        let first_for_table = CAYENNE_CDC_SYNCHRONOUS_FALLBACK_WARNING_KEYS
            .lock()
            .insert_new(
                self.dataset_name.to_string(),
                CAYENNE_CDC_SYNCHRONOUS_FALLBACK_WARNING_KEY_LIMIT,
            );
        if first_for_table {
            tracing::warn!(
                dataset = %self.dataset_name,
                "Cayenne CDC fell back to the synchronous write path: cayenne_accelerator() could not unwrap the inner CayenneTableProvider through its provider wrappers. Pipelined finalization (backgrounded publish, no blocking apply_on_conflict_deletions) is DISABLED for this table — an unrecognized provider wrapper likely needs peeling in cayenne_accelerator()."
            );
        }
    }

    async fn process_truncate(
        &self,
        ctx: &SessionContext,
        session_state: &SessionState,
    ) -> crate::accelerated::Result<()> {
        let dataset_name = &self.dataset_name;
        tracing::info!("Processing TRUNCATE for {dataset_name}");

        let _lock_guard = self.accelerator_write_mutex.lock().await;
        // Some accelerator impls (notably DuckDB) treat an empty filter list as
        // a no-op to guard against accidental full-table deletes. To get
        // uniform "wipe the whole table" semantics we pass an always-true
        // literal, which is emitted as `DELETE FROM <table> WHERE TRUE` and
        // applied consistently across engines.
        let delete_plan = self
            .accelerator
            .delete_from(session_state, vec![lit(true)])
            .await
            .map_err(find_datafusion_root)
            .context(crate::accelerated::FailedToWriteDataSnafu)?;
        collect(delete_plan, ctx.task_ctx())
            .await
            .map_err(find_datafusion_root)
            .context(crate::accelerated::FailedToWriteDataSnafu)?;
        perform_change_write_maintenance(&self.accelerator).await?;

        self.update_last_updated_at();
        Ok(())
    }

    /// Apply one Delete sub-batch. Returns the Cayenne in-memory CDC tier
    /// epoch when the deletes were ABSORBED as RAM tombstones
    /// (`cdc_durability: memory`, key-mode) — the caller must defer the
    /// burst's source commit on that epoch exactly like an in-memory upsert —
    /// or `None` when the deletes were applied durably (the historical path).
    async fn process_delete_batch(
        &self,
        change_batch: &ChangeBatch,
        row_indices: &[usize],
        ctx: &SessionContext,
        session_state: &SessionState,
    ) -> crate::accelerated::Result<Option<u64>> {
        let dataset_name = &self.dataset_name;

        if row_indices.is_empty() {
            return Ok(None);
        }

        // Distribution of delete-burst sizes — the count of primary-keyed rows,
        // i.e. the keys that feed the chunked durable `delete_from`. Recorded
        // once per sub-batch on both the absorb and durable paths so the fleet
        // can see how large delete bursts get — i.e. whether
        // `cdc_delete_subbatch_max` ever binds. Keyless delete rows carry no key
        // (they take the row-match path, not `delete_from`) and are excluded, so
        // a mixed keyed+keyless burst is not over-counted as keys.
        let keyed_count = row_indices
            .iter()
            .filter(|&&row| change_batch.has_primary_keys(row))
            .count();
        metrics::CDC_KEYS_PER_DELETE_BURST.record(
            u64::try_from(keyed_count).unwrap_or(u64::MAX),
            self.dataset_metric_labels.dataset(),
        );

        // In-memory absorption: when the burst-level gate kept this burst on
        // the mem path, the slot advancer is armed and a capable Cayenne sink
        // turns the delete rows into mem-tier tombstones, deferring their
        // durability to the covering checkpoint. Any fall-through (capability
        // lost, inextractable keys, budget refusal after spill) lands on the
        // durable path below — safe in either ack mode, since durable deletes
        // never sit ahead of the source slot — and records the reason so the
        // eventual composite-key absorb fix can be aimed at the right cause.
        #[cfg(not(windows))]
        let fallthrough_reason: &'static str = 'absorb: {
            let Some(cayenne) = self.cayenne_accelerator() else {
                break 'absorb "no_capability";
            };
            if !cayenne.supports_in_memory_cdc_deletes() {
                break 'absorb "no_capability";
            }
            if !cayenne.has_slot_advancer() {
                break 'absorb "no_advancer";
            }
            if !row_indices
                .iter()
                .all(|&row| change_batch.has_primary_keys(row))
            {
                break 'absorb "inextractable_keys";
            }
            let selected_batch = select_rows(&change_batch.data_batch(), row_indices)?;
            let absorbed = cayenne
                .write_cdc_delete_keys_in_memory(&selected_batch)
                .await
                .map_err(DataFusionError::from)
                .map_err(find_datafusion_root)
                .context(crate::accelerated::FailedToWriteDataSnafu)?;
            if let Some(epoch) = absorbed {
                tracing::trace!(
                    dataset = %dataset_name,
                    rows = row_indices.len(),
                    epoch,
                    "Delete sub-batch absorbed into the in-memory CDC tier"
                );
                record_cdc_apply_path(&self.dataset_metric_labels, "inmem_delete");
                self.update_last_updated_at();
                return Ok(Some(epoch));
            }
            // Gate passed but the sink declined the RAM write. In practice this
            // is the mem-tier byte budget refusing after a spill attempt; a
            // deeper key-extraction miss (all rows carried PKs at this layer) is
            // far rarer, so attribute the fall-through to `budget`.
            "budget"
        };

        // On Windows the in-memory absorb path is compiled out entirely, so
        // every delete is durable — attribute it to the missing capability.
        #[cfg(windows)]
        let fallthrough_reason: &'static str = "no_capability";

        metrics::CDC_DELETE_ABSORB_FALLTHROUGH.add(
            1,
            &self
                .dataset_metric_labels
                .tagged("reason", fallthrough_reason),
        );

        // Durable delete fallback (in-memory absorb declined, e.g. deletes cleared
        // the slot-advancer). This synchronous path — lock wait, delete, then
        // whole-burst maintenance — is far more expensive than in-memory absorption
        // and, before these sub-phases, was invisible in the write-phase breakdown
        // (it is not a cayenne write_phase). Decompose it so a table pinned here
        // (new_order) shows WHERE its apply time goes.
        record_cdc_apply_path(&self.dataset_metric_labels, "durable_delete");
        let (keyless_rows, keyed_rows): (Vec<_>, Vec<_>) = row_indices
            .iter()
            .copied()
            .partition(|row| !change_batch.has_primary_keys(*row));

        let mut wrote = false;
        // Serialized with every other writer (and compaction); under contention this
        // acquire alone can dominate a delete burst.
        let lock_start = Instant::now();
        let _lock_guard = self.accelerator_write_mutex.lock().await;
        record_cdc_fixed_cost(
            &self.dataset_metric_labels,
            "durable_delete_lock_wait",
            lock_start,
        );
        let apply_start = Instant::now();

        if !keyless_rows.is_empty() {
            let selected_batch = select_rows(&change_batch.data_batch(), &keyless_rows)?;
            if delete_matching_rows_from_arrow_provider(&self.accelerator, &selected_batch)
                .await?
                .is_some()
            {
                wrote = true;
            } else {
                return Err(crate::accelerated::Error::NoPrimaryKeysDefined {
                    dataset_name: dataset_name.to_string(),
                });
            }
        }

        // Cap the number of keys per durable `delete_from`: a burst of N keyed
        // rows becomes ⌈N/cap⌉ independent, interruptible plans instead of one
        // monolithic OR-tree predicate (the ~89s / ~50k-comparison execution
        // that pegged prefetch and tripped walsender timeouts). The keys across
        // chunks are distinct PKs, so each is deleted exactly once and
        // cross-chunk ordering is not load-bearing; all chunks run under the
        // single `_lock_guard` held for this call, so the burst stays isolated.
        let cap = self.cdc_delete_subbatch_max();
        let dataset_name_str = dataset_name.to_string();
        for chunk in keyed_rows.chunks(cap) {
            let combined = build_batch_delete_expr_from_change_batch(
                change_batch,
                chunk,
                dataset_name_str.as_str(),
            )?;

            if let Some(combined) = combined {
                // The CDC apply loop discards the "rows affected" count. Cayenne can
                // handle key-delete CDC batches through a count-skipping path; non-
                // Cayenne accelerators and shapes Cayenne declines fall back to the
                // generic `delete_from` below. The predicate handed to either path
                // is this chunk's (≤ `cap` keys), so the per-plan bound holds on
                // both the fast and fallback paths.
                let handled_by_cayenne_cdc_path = {
                    #[cfg(not(windows))]
                    {
                        if let Some(cayenne) = self.cayenne_accelerator() {
                            cayenne
                                .delete_from_cdc_fast(std::slice::from_ref(&combined))
                                .await
                                .map_err(find_datafusion_root)
                                .context(crate::accelerated::FailedToWriteDataSnafu)?
                                .is_some()
                        } else {
                            false
                        }
                    }
                    #[cfg(windows)]
                    {
                        false
                    }
                };

                if handled_by_cayenne_cdc_path {
                    // Cayenne's fast CDC-delete path bypasses `TableProvider::delete_from`
                    // entirely, so it never reaches `IndexLayer::delete_from`'s
                    // index-aware handling on either side — drive index deletion explicitly
                    // here instead, across both the accelerator and federated sides (an
                    // external-store vector/search index, e.g. S3 Vectors, is attached only
                    // on the federated side; see `collect_all_indexes`). Best-effort: an index
                    // failure is logged, not propagated, so it can't block the (already-applied)
                    // accelerator-side delete above.
                    if let Some(keys) = build_pk_only_batch_from_change_batch(change_batch, chunk)?
                    {
                        for index in collect_all_indexes(&self.accelerator, &self.federated) {
                            if let Err(e) = index.delete_by_keys(keys.clone()).await {
                                tracing::error!(
                                    "Index '{}' failed to delete entries for a CDC delete via the Cayenne fast path (best-effort, continuing): {e}",
                                    index.name()
                                );
                            }
                        }
                    }
                } else {
                    let delete_plan = self
                        .accelerator
                        .delete_from(session_state, vec![combined])
                        .await
                        .map_err(find_datafusion_root)
                        .context(crate::accelerated::FailedToWriteDataSnafu)?;
                    collect(delete_plan, ctx.task_ctx())
                        .await
                        .map_err(find_datafusion_root)
                        .context(crate::accelerated::FailedToWriteDataSnafu)?;

                    // `self.accelerator.delete_from` above already drives any
                    // `IndexLayer` wrapping the accelerator itself (e.g. the DuckDB
                    // vector engine) through its own index-aware handling. It cannot reach an
                    // index attached only on the federated side (e.g. S3 Vectors, Elasticsearch)
                    // — that's a distinct `TableProvider` chain — so drive those explicitly here.
                    // Best-effort: logged, not propagated.
                    if let Some(keys) = build_pk_only_batch_from_change_batch(change_batch, chunk)?
                    {
                        for index in indexes_from_federated(&self.federated) {
                            if let Err(e) = index.delete_by_keys(keys.clone()).await {
                                tracing::error!(
                                    "Index '{}' failed to delete entries for a CDC delete (best-effort, continuing): {e}",
                                    index.name()
                                );
                            }
                        }
                    }
                }
                wrote = true;
            }
        }
        record_cdc_fixed_cost(
            &self.dataset_metric_labels,
            "durable_delete_apply",
            apply_start,
        );

        if wrote {
            // Whole-burst maintenance (compaction trigger) runs synchronously here —
            // a prime contributor to a long durable-delete burst; time it separately.
            let maint_start = Instant::now();
            perform_change_write_maintenance(&self.accelerator).await?;
            record_cdc_fixed_cost(
                &self.dataset_metric_labels,
                "durable_delete_maintenance",
                maint_start,
            );
            self.update_last_updated_at();
        }

        Ok(None)
    }
}

/// One equal-schema group from [`group_run_by_schema`]: the batches and their
/// matching commit handles, kept in arrival order.
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
/// does we surface the error and let the caller skip committing those
/// envelopes (the source will redeliver them).
fn concat_change_batches(batches: &[ChangeBatch]) -> crate::accelerated::Result<ChangeBatch> {
    debug_assert!(
        !batches.is_empty(),
        "concat_change_batches requires at least one batch",
    );

    let schema = batches[0].record.schema();
    let records: Vec<&RecordBatch> = batches.iter().map(|b| &b.record).collect();
    let combined = arrow::compute::concat_batches(&schema, records)
        .context(crate::accelerated::FailedToBuildRecordBatchSnafu)?;
    ChangeBatch::try_new(combined).map_err(|e| {
        // ChangeBatchError isn't part of the AcceleratedTable Error enum;
        // wrap it in FailedToBuildRecordBatch so the caller's status path
        // doesn't have to learn about a new variant.
        crate::accelerated::Error::FailedToBuildRecordBatch {
            source: arrow::error::ArrowError::ExternalError(Box::new(e)),
        }
    })
}

fn cdc_item_budget_bytes(item: &Result<cdc::ChangeEnvelope, cdc::StreamError>) -> usize {
    // A coalescing byte-budget proxy, NOT a true in-memory Arrow size:
    // `encoded_len` answers WITHOUT forcing a build — a deferred (e.g. Postgres)
    // envelope from a schema-aware estimate of its buffered wire size, a built
    // one from its actual Arrow size. Used only to bound how much a single burst
    // accumulates before applying; the real Arrow build is deferred to apply
    // time (`into_parts_offloaded_burst`), off the source's shared read path.
    item.as_ref().map_or(0, cdc::ChangeEnvelope::encoded_len)
}

/// Zeroes `cdc_prefetch_buffer_bytes` for one dataset when the CDC stream that
/// feeds it goes away.
///
/// The gauge is only ever recorded from inside the apply loop, so its last
/// reading outlives that loop. Every exit — a `break` out to the finalize drain,
/// or the whole future being dropped mid-`await` when the refresh task is
/// cancelled — drops the receiver and everything still queued behind it, but
/// leaves the exported value describing a backlog that no longer exists. An
/// operator reading a torn-down dataset would see prefetch memory that was
/// already freed, which is the same class of lie the gauge exists to stop
/// telling. `Drop` is what covers the cancellation path; resetting at each
/// `break` would not, since an aborted task never reaches one.
struct PrefetchBytesGaugeReset {
    labels: DatasetMetricLabels,
}

impl Drop for PrefetchBytesGaugeReset {
    fn drop(&mut self) {
        metrics::CDC_PREFETCH_BUFFER_BYTES.record(0, self.labels.dataset());
    }
}

/// Subtract from the CDC prefetch byte counter without wrapping.
///
/// Charge and discharge are meant to be symmetric, but `u64::fetch_sub` past
/// zero wraps to ~1.8e19, which turns a small accounting slip into a reading no
/// operator can interpret — and which looks nothing like "slightly wrong". A
/// gauge that fails should fail toward zero, where the error stays proportional
/// to the mistake, so saturate rather than wrap.
fn discharge_prefetch_bytes(counter: &AtomicU64, bytes: u64) {
    let previous = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(bytes))
    });
    // Saturating in release is the right failure mode for a gauge, but it also
    // HIDES the bug that motivated it: discharging one envelope twice used to
    // wrap the counter to ~1.8e19, and saturation would instead quietly clamp to
    // zero and look plausible. Every charge has exactly one discharge, so a
    // discharge larger than the balance is a real accounting error - fail loudly
    // where a test can see it, and stay soft where an operator would only see a
    // gauge.
    debug_assert!(
        previous.is_ok_and(|balance| balance >= bytes),
        "CDC prefetch byte counter underflowed: discharged {bytes} against a balance of \
         {previous:?}. Each envelope must be discharged exactly once - a carried item \
         is discharged at the try_recv that removed it, not again when the next \
         iteration adopts it."
    );
}

fn elapsed_ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

fn record_cdc_fixed_cost(labels: &DatasetMetricLabels, phase: &'static str, start: Instant) {
    metrics::CDC_APPLY_FIXED_COST_MS.record(elapsed_ms(start), &labels.tagged("phase", phase));
}

/// Count which apply path a change sub-batch took. `inmem_append` / `inmem_delete`
/// defer durability to the checkpoint (cheap); `durable_append` / `durable_delete`
/// take the synchronous durable path (whole-burst commit + maintenance) — the
/// expensive path a table is forced onto when a burst can't defer (e.g. deletes
/// clear the slot-advancer). Reveals WHY a table's apply time is high, which the
/// phase-coverage gap can only hint at.
fn record_cdc_apply_path(labels: &DatasetMetricLabels, path: &'static str) {
    metrics::CDC_APPLY_PATH_TOTAL.add(1, &labels.tagged("path", path));
}

fn select_rows(
    data_batch: &RecordBatch,
    row_indices: &[usize],
) -> crate::accelerated::Result<RecordBatch> {
    if let Some((offset, length)) = contiguous_row_span(row_indices) {
        return Ok(data_batch.slice(offset, length));
    }

    let indices = row_indices
        .iter()
        .map(|&i| {
            u32::try_from(i).map_err(|e| {
                arrow::error::ArrowError::InvalidArgumentError(format!(
                    "CDC row index {i} exceeds UInt32 take index range: {e}"
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .context(crate::accelerated::FailedToBuildRecordBatchSnafu)?;
    let indices_array = UInt32Array::from(indices);

    let selected_columns: Vec<ArrayRef> = data_batch
        .columns()
        .iter()
        .map(|col| arrow::compute::take(col.as_ref(), &indices_array, None))
        .collect::<Result<Vec<_>, _>>()
        .context(crate::accelerated::FailedToBuildRecordBatchSnafu)?;

    RecordBatch::try_new(data_batch.schema(), selected_columns)
        .context(crate::accelerated::FailedToBuildRecordBatchSnafu)
}

async fn delete_matching_rows_from_arrow_provider(
    provider: &Arc<dyn TableProvider>,
    rows: &RecordBatch,
) -> crate::accelerated::Result<Option<u64>> {
    // Peel any layers stacked on the accelerator to reach the provider that
    // actually holds the rows.
    if let Some(table) = provider.downcast_ref::<SpiceTable>() {
        return Box::pin(delete_matching_rows_from_arrow_provider(
            table.below(),
            rows,
        ))
        .await;
    }

    if let Some(table) = provider.downcast_ref::<MemTable>() {
        return table
            .delete_matching_rows(rows)
            .await
            .map(Some)
            .map_err(find_datafusion_root)
            .context(crate::accelerated::FailedToWriteDataSnafu);
    }

    if let Some(table) = provider.downcast_ref::<IndexedMemTable>() {
        return table
            .delete_matching_rows(rows)
            .await
            .map(Some)
            .map_err(find_datafusion_root)
            .context(crate::accelerated::FailedToWriteDataSnafu);
    }

    if let Some(partitioned) = provider.downcast_ref::<PartitionTableProvider>() {
        let mut deleted = 0_u64;
        let mut matched_arrow_provider = false;
        for partition_provider in partitioned.partition_table_providers().await {
            if let Some(partition_deleted) = Box::pin(delete_matching_rows_from_arrow_provider(
                &partition_provider,
                rows,
            ))
            .await?
            {
                deleted += partition_deleted;
                matched_arrow_provider = true;
            }
        }

        return Ok(matched_arrow_provider.then_some(deleted));
    }

    Ok(None)
}

async fn perform_change_write_maintenance(
    provider: &Arc<dyn TableProvider>,
) -> crate::accelerated::Result<()> {
    // Peel any layers stacked on the accelerator to reach the provider that
    // actually performs maintenance.
    if let Some(table) = provider.downcast_ref::<SpiceTable>() {
        return Box::pin(perform_change_write_maintenance(table.below())).await;
    }

    if let Some(partitioned) = provider.downcast_ref::<PartitionTableProvider>() {
        for partition_provider in partitioned.partition_table_providers().await {
            Box::pin(perform_change_write_maintenance(&partition_provider)).await?;
        }
        return Ok(());
    }

    perform_index_maintenance(provider.as_ref())
        .await
        .map(|_| ())
        .map_err(find_datafusion_root)
        .context(crate::accelerated::FailedToWriteDataSnafu)
}

fn contiguous_row_span(row_indices: &[usize]) -> Option<(usize, usize)> {
    let first = *row_indices.first()?;
    if row_indices
        .iter()
        .enumerate()
        .all(|(offset, &row)| row == first + offset)
    {
        Some((first, row_indices.len()))
    } else {
        None
    }
}

#[cfg(not(windows))]
fn spawn_cayenne_finalize(cayenne_write: CayenneCdcWrite) -> PendingApplyFinalize {
    tokio::spawn(async move {
        cayenne_write
            .finish()
            .await
            .map(|_| ())
            .map_err(DataFusionError::from)
            .map_err(find_datafusion_root)
            .context(crate::accelerated::FailedToWriteDataSnafu)
    })
}

async fn join_pending_finalize(
    handle: PendingApplyFinalize,
    dataset_name: &TableReference,
    is_shutdown: bool,
) -> Option<String> {
    classify_finalize_result(handle.await, dataset_name, is_shutdown)
}

/// Classify a resolved CDC finalize join result into an optional error message,
/// treating shutdown-time failures/cancellations as expected. Split out from
/// [`join_pending_finalize`] so the idle-source race in the apply loop — which
/// resolves the finalize [`tokio::task::JoinHandle`] via `select!` rather than
/// awaiting it directly — can share the exact same classification.
fn classify_finalize_result(
    result: std::result::Result<crate::accelerated::Result<()>, tokio::task::JoinError>,
    dataset_name: &TableReference,
    is_shutdown: bool,
) -> Option<String> {
    match result {
        Ok(Ok(())) => None,
        Ok(Err(e)) if is_shutdown => {
            tracing::debug!("CDC apply finalizer for {dataset_name} failed during shutdown: {e}");
            None
        }
        Ok(Err(e)) => {
            let error_message = format!("CDC apply finalizer for {dataset_name} failed: {e}");
            tracing::error!("{error_message}");
            Some(error_message)
        }
        Err(e) if e.is_cancelled() && is_shutdown => {
            tracing::debug!(
                "CDC apply finalizer for {dataset_name} was cancelled (likely shutdown)"
            );
            None
        }
        Err(e) => {
            let error_message =
                format!("CDC apply finalizer for {dataset_name} ended unexpectedly: {e}");
            tracing::error!("{error_message}");
            Some(error_message)
        }
    }
}

/// Await an in-flight commit task spawned by `apply_envelope_run`. Surfaces
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
        // Safe catch-up mode: this task is spawned only after the accelerator
        // write is safe to acknowledge. For Cayenne staged appends, the
        // committers are held until the apply finalizer has made the replacement
        // files visible; for non-staged writes, the write return itself is the
        // visibility point. `apply_envelope_run` drains the previous commit task
        // with timeout/backpressure before spawning this one, so source progress
        // is acknowledged in order.
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

/// An active batch accumulating row indices for a single operation type.
/// Tracks primary keys so that same-PK collisions within the bucket apply
/// last-write-wins deduplication (the newer row replaces the older one)
struct OpBatchAccumulator {
    rows: Vec<usize>,
    needs_sort: bool,
    /// Maps encoded PK to index into `rows`, enabling replacement on same-bucket PK collision.
    pk_to_pos: HashMap<Vec<u8>, usize, BuildHasherDefault<twox_hash::XxHash3_64>>,
}

impl OpBatchAccumulator {
    fn new() -> Self {
        Self {
            rows: Vec::new(),
            needs_sort: false,
            pk_to_pos: HashMap::default(),
        }
    }

    /// Returns `true` if `pk` is already tracked in this bucket.
    fn contains_pk(&self, pk: &[u8]) -> bool {
        self.pk_to_pos.contains_key(pk)
    }

    /// Insert `row_id` under `pk`. If the PK already exists in this bucket,
    /// the previous row index is replaced in-place (last-write-wins).
    /// See [`group_into_sub_batches`] for the rationale.
    fn insert_or_replace(&mut self, pk: Vec<u8>, row_id: usize) {
        if let Some(&pos) = self.pk_to_pos.get(&pk) {
            // Same-bucket collision: replace the earlier row with the newer
            // one. The old row is superseded because CDC rows carry the
            // full row state.
            if pos + 1 < self.rows.len() {
                self.needs_sort = true;
            }
            self.rows[pos] = row_id;
        } else {
            let pos = self.rows.len();
            self.rows.push(row_id);
            self.pk_to_pos.insert(pk, pos);
        }
    }

    /// Drain accumulated rows into `out` under the given operation type and
    /// reset PK tracking.
    fn flush_into(
        &mut self,
        op: ChangeOperationType,
        out: &mut Vec<(ChangeOperationType, Vec<usize>)>,
    ) {
        if !self.rows.is_empty() {
            if self.needs_sort {
                self.rows.sort_unstable();
                self.needs_sort = false;
            }
            out.push((op, std::mem::take(&mut self.rows)));
            self.pk_to_pos.clear();
        }
    }
}

/// Groups rows into sub-batches based on operation type and primary key
/// conflicts across active operation buckets.
///
/// Uses a streaming conflict-window algorithm with **last-write-wins
/// deduplication**: two active buckets (upsert, delete) accumulate rows
/// concurrently. When an incoming row's PK already exists in the *other*
/// bucket, that bucket is flushed to preserve cross-operation ordering.
/// When the PK collides within the *same* bucket the earlier row index is
/// replaced in-place — CDC rows are full-state snapshots, so only the
/// latest row per PK is required and intermediate states can be safely dropped.
///
/// For deletes a same-bucket PK collision is unexpected in practice (a
/// source would have to emit two consecutive deletes for the same key
/// without an intervening upsert), but is still safe — deleting the same
/// PK twice is idempotent. We use the same replace path for both operation
/// types to keep the logic simple.
///
/// Truncate and Unknown act as barriers that flush everything.
#[must_use]
fn group_into_sub_batches(change_batch: &ChangeBatch) -> Vec<(ChangeOperationType, Vec<usize>)> {
    let num_rows = change_batch.record.num_rows();
    if num_rows == 0 {
        return vec![];
    }

    // Extract data batch and PK column indices once, instead of per-row.
    let data_batch = change_batch.data_batch();
    let pk_column_names = change_batch.primary_keys(0);
    let pk_col_indices: Vec<usize> = pk_column_names
        .iter()
        .filter_map(|name| data_batch.schema().index_of(name).ok())
        .collect();
    let has_pks = !pk_col_indices.is_empty();

    let mut upserts = OpBatchAccumulator::new();
    let mut deletes = OpBatchAccumulator::new();
    let mut out: Vec<(ChangeOperationType, Vec<usize>)> = Vec::new();

    for row_id in 0..num_rows {
        let op = change_batch.op(row_id);
        let op_type = ChangeOperationType::from_operation(&op);

        // Truncate and Unknown are barriers — flush everything, emit the
        // barrier row, and continue.
        if op_type == ChangeOperationType::Truncate || op_type == ChangeOperationType::Unknown {
            upserts.flush_into(ChangeOperationType::Upsert, &mut out);
            deletes.flush_into(ChangeOperationType::Delete, &mut out);
            out.push((op_type, vec![row_id]));
            continue;
        }

        // When PKs are available, use last-write-wins within the same
        // bucket (CDC rows are full-state snapshots so only the latest
        // row per PK matters) and flush only on *cross-bucket* conflicts
        // to preserve inter-operation ordering.
        if has_pks {
            let primary_key = encode_primary_key(&data_batch, &pk_col_indices, row_id);

            // Cross-bucket conflict: the *other* bucket already has this
            // PK, so flush it to preserve operation ordering.
            match op_type {
                ChangeOperationType::Upsert => {
                    if deletes.contains_pk(&primary_key) {
                        deletes.flush_into(ChangeOperationType::Delete, &mut out);
                    }
                }
                ChangeOperationType::Delete => {
                    if upserts.contains_pk(&primary_key) {
                        upserts.flush_into(ChangeOperationType::Upsert, &mut out);
                    }
                }
                ChangeOperationType::Truncate | ChangeOperationType::Unknown => {
                    unreachable!("unexpected op type {op_type:?} after barrier check")
                }
            }

            // Same-bucket collision: replace the old row (last-write-wins).
            let batch = match op_type {
                ChangeOperationType::Upsert => &mut upserts,
                ChangeOperationType::Delete => &mut deletes,
                ChangeOperationType::Truncate | ChangeOperationType::Unknown => {
                    unreachable!("unexpected op type {op_type:?} after barrier check")
                }
            };
            batch.insert_or_replace(primary_key, row_id);
        } else {
            // No PKs — fall back to grouping consecutive same-op rows
            // (can't detect conflicts without keys).
            match op_type {
                ChangeOperationType::Upsert => {
                    deletes.flush_into(ChangeOperationType::Delete, &mut out);
                    upserts.rows.push(row_id);
                }
                ChangeOperationType::Delete => {
                    upserts.flush_into(ChangeOperationType::Upsert, &mut out);
                    deletes.rows.push(row_id);
                }
                ChangeOperationType::Truncate | ChangeOperationType::Unknown => {
                    unreachable!("unexpected op type {op_type:?} after barrier check")
                }
            }
        }
    }

    // Flush remaining active batches.
    upserts.flush_into(ChangeOperationType::Upsert, &mut out);
    deletes.flush_into(ChangeOperationType::Delete, &mut out);

    out
}

fn encode_primary_key(
    data_batch: &RecordBatch,
    pk_col_indices: &[usize],
    row_id: usize,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(pk_col_indices.len().saturating_mul(16));
    for &col_idx in pk_col_indices {
        key.extend_from_slice(&col_idx.to_le_bytes());
        encode_array_value(data_batch.column(col_idx).as_ref(), row_id, &mut key);
    }
    key
}

macro_rules! encode_primitive_value {
    ($array:expr, $row_id:expr, $array_type:ty, $key:expr) => {{
        if let Some(array) = $array.as_any().downcast_ref::<$array_type>() {
            $key.extend_from_slice(&array.value($row_id).to_le_bytes());
            return;
        }
    }};
}

fn encode_bytes(bytes: &[u8], key: &mut Vec<u8>) {
    key.extend_from_slice(&bytes.len().to_le_bytes());
    key.extend_from_slice(bytes);
}

fn encode_array_value(array: &dyn Array, row_id: usize, key: &mut Vec<u8>) {
    if array.is_null(row_id) {
        key.push(0);
        return;
    }
    key.push(1);

    match array.data_type() {
        DataType::Boolean => {
            if let Some(array) = array.as_any().downcast_ref::<arrow::array::BooleanArray>() {
                key.push(u8::from(array.value(row_id)));
                return;
            }
        }
        DataType::Int8 => {
            encode_primitive_value!(array, row_id, arrow::array::Int8Array, key);
        }
        DataType::Int16 => {
            encode_primitive_value!(array, row_id, arrow::array::Int16Array, key);
        }
        DataType::Int32 => {
            encode_primitive_value!(array, row_id, Int32Array, key);
        }
        DataType::Int64 => {
            encode_primitive_value!(array, row_id, Int64Array, key);
        }
        DataType::UInt8 => {
            encode_primitive_value!(array, row_id, arrow::array::UInt8Array, key);
        }
        DataType::UInt16 => {
            encode_primitive_value!(array, row_id, arrow::array::UInt16Array, key);
        }
        DataType::UInt32 => {
            encode_primitive_value!(array, row_id, UInt32Array, key);
        }
        DataType::UInt64 => {
            encode_primitive_value!(array, row_id, arrow::array::UInt64Array, key);
        }
        DataType::Float32 => {
            if let Some(array) = array.as_any().downcast_ref::<arrow::array::Float32Array>() {
                key.extend_from_slice(&array.value(row_id).to_bits().to_le_bytes());
                return;
            }
        }
        DataType::Float64 => {
            if let Some(array) = array.as_any().downcast_ref::<arrow::array::Float64Array>() {
                key.extend_from_slice(&array.value(row_id).to_bits().to_le_bytes());
                return;
            }
        }
        DataType::Utf8 => {
            if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
                encode_bytes(array.value(row_id).as_bytes(), key);
                return;
            }
        }
        DataType::LargeUtf8 => {
            if let Some(array) = array
                .as_any()
                .downcast_ref::<arrow::array::LargeStringArray>()
            {
                encode_bytes(array.value(row_id).as_bytes(), key);
                return;
            }
        }
        DataType::Date32 => {
            encode_primitive_value!(array, row_id, arrow::array::Date32Array, key);
        }
        DataType::Date64 => {
            encode_primitive_value!(array, row_id, arrow::array::Date64Array, key);
        }
        DataType::Time32(_) => {
            if let Some(array) = array
                .as_any()
                .downcast_ref::<arrow::array::Time32SecondArray>()
            {
                key.extend_from_slice(&array.value(row_id).to_le_bytes());
                return;
            }
            encode_primitive_value!(array, row_id, arrow::array::Time32MillisecondArray, key);
        }
        DataType::Time64(_) => {
            if let Some(array) = array
                .as_any()
                .downcast_ref::<arrow::array::Time64MicrosecondArray>()
            {
                key.extend_from_slice(&array.value(row_id).to_le_bytes());
                return;
            }
            encode_primitive_value!(array, row_id, arrow::array::Time64NanosecondArray, key);
        }
        DataType::Timestamp(_, _) => {
            if let Some(array) = array
                .as_any()
                .downcast_ref::<arrow::array::TimestampSecondArray>()
            {
                key.extend_from_slice(&array.value(row_id).to_le_bytes());
                return;
            }
            if let Some(array) = array
                .as_any()
                .downcast_ref::<arrow::array::TimestampMillisecondArray>()
            {
                key.extend_from_slice(&array.value(row_id).to_le_bytes());
                return;
            }
            if let Some(array) = array
                .as_any()
                .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            {
                key.extend_from_slice(&array.value(row_id).to_le_bytes());
                return;
            }
            encode_primitive_value!(array, row_id, arrow::array::TimestampNanosecondArray, key);
        }
        DataType::Decimal128(_, _) => {
            encode_primitive_value!(array, row_id, arrow::array::Decimal128Array, key);
        }
        _ => {}
    }

    if let Ok(value) = arrow::util::display::array_value_to_string(array, row_id) {
        key.push(0xfe);
        encode_bytes(value.as_bytes(), key);
    } else {
        key.push(0xff);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteChangeResult {
    DataWritten,
    NoChange,
}

// Used to group batch changes into sub-batches
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeOperationType {
    Upsert, // Create, Update, or Read
    Delete,
    Truncate,
    Unknown,
}

impl ChangeOperationType {
    fn from_operation(op: &ChangeOperation) -> Self {
        match op {
            ChangeOperation::Create | ChangeOperation::Update | ChangeOperation::Read => {
                Self::Upsert
            }
            ChangeOperation::Delete => Self::Delete,
            ChangeOperation::Truncate => Self::Truncate,
            ChangeOperation::Unknown(_) => Self::Unknown,
        }
    }
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
    use super::*;
    use arrow::array::{ArrayRef, Int32Array, ListArray, StringArray, StructArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use data_components::arrow::write::MemTable;
    use data_components::cdc::changes_schema;
    use datafusion::datasource::TableProvider;
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
            datafusion::sql::TableReference::bare(name.to_string()),
            federated,
            None,
            accelerator,
            Handle::current(),
            Arc::new(Mutex::new(())),
        )
        .build()
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
            datafusion::sql::TableReference::bare("test".to_string()),
            federated,
            None,
            accelerator,
            Handle::current(),
            Arc::new(Mutex::new(())),
        )
        .with_cdc_param_overrides(Some(Arc::new(cdc_params)))
        .build()
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
            task.write_change_with_context(first_batch, &ctx, &session_state)
                .await
                .expect("first write_change should succeed")
                .result,
            WriteChangeResult::DataWritten
        );
        assert_eq!(
            task.write_change_with_context(second_batch, &ctx, &session_state)
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
                Some(other) => {
                    self.value = self.value.max(other.value);
                    true
                }
                None => false,
            }
        }

        fn as_any(&self) -> Option<&dyn std::any::Any> {
            Some(self)
        }
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
        let advancer = CayenneSlotAdvancer {
            queue: Arc::clone(&queue),
            dataset_name: TableReference::bare("test"),
            runtime_status: runtime_status::RuntimeStatus::new(),
        };
        <CayenneSlotAdvancer as cayenne::SlotAdvancer>::on_checkpoint_durable(&advancer, 4).await;
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

        let advancer = CayenneSlotAdvancer {
            queue: Arc::clone(&queue),
            dataset_name: TableReference::bare("test"),
            runtime_status: runtime_status::RuntimeStatus::new(),
        };
        <CayenneSlotAdvancer as cayenne::SlotAdvancer>::on_checkpoint_durable(&advancer, 5).await;

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

    /// A1-T3 — the checkpoint↔push ordering seam. A periodic mem-tier checkpoint
    /// can fire `on_checkpoint_durable(N)` AFTER the tier reached epoch N but
    /// BEFORE the apply loop has pushed batch N's committers onto the queue (the
    /// push at `changes.rs` happens after `append_to_mem_tier` returns the epoch).
    /// The advancer must only DELAY such a committer's ack — draining whatever is
    /// present at or below the durable epoch and leaving the rest for a later
    /// drain — never advance the slot for an unqueued epoch and never double-ack.
    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_slot_advancer_delays_committers_pushed_after_checkpoint() {
        let log = CommitLog::new();
        let queue: DeferredCommitQueue = Arc::new(TokioMutex::new(VecDeque::new()));
        let advancer = CayenneSlotAdvancer {
            queue: Arc::clone(&queue),
            dataset_name: TableReference::bare("test"),
            runtime_status: runtime_status::RuntimeStatus::new(),
        };

        // Epoch 1's committers ARE queued; epoch 2's are not yet (the apply loop
        // hasn't pushed them). A checkpoint that snapshotted `flushed_epoch = 2`
        // fires ahead of the push.
        queue.lock().await.push_back((
            1,
            vec![Box::new(DeferrableTrackingCommitter {
                id: 1,
                log: Arc::clone(&log),
                outcome: Ok(()),
            })],
        ));
        <CayenneSlotAdvancer as cayenne::SlotAdvancer>::on_checkpoint_durable(&advancer, 2).await;

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

        // Now the apply loop pushes epoch 2's committers (after its data became
        // durable). The NEXT checkpoint (or queue-non-empty re-check) drains them —
        // exactly-once, no double-ack of epoch 1.
        queue.lock().await.push_back((
            2,
            vec![Box::new(DeferrableTrackingCommitter {
                id: 2,
                log: Arc::clone(&log),
                outcome: Ok(()),
            })],
        ));
        <CayenneSlotAdvancer as cayenne::SlotAdvancer>::on_checkpoint_durable(&advancer, 2).await;
        assert_eq!(
            log.ids().await,
            vec![1, 2],
            "the late-pushed committer acks on the next drain; epoch 1 is not re-acked"
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
        ready_sender: Option<Arc<Notify>>,
        initial_load_completed: Arc<AtomicBool>,
    ) -> crate::accelerated::Result<()> {
        let refresh = Arc::new(RwLock::new(crate::accelerated::refresh::Refresh::default()));
        task.start_changes_stream(refresh, stream, None, ready_sender, initial_load_completed)
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
        let mut context = ApplyContext {
            refresh_sql: None,
            dataset_name: &dataset_name,
            metric_labels: &metric_labels,
            caching: None,
            ready_sender: None,
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
        let data_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, true),
        ]);
        let schema = changes_schema(&data_schema);
        let op_array: ArrayRef = Arc::new(StringArray::from(vec!["c"]));
        let pk_field = Arc::new(Field::new("item", DataType::Utf8, false));
        let pk_array: ArrayRef = Arc::new(
            ListArray::try_new(
                pk_field,
                arrow::buffer::OffsetBuffer::new(vec![0i32, 1].into()),
                Arc::new(StringArray::from(vec!["id"])),
                None,
            )
            .expect("pk list"),
        );
        let data_fields = vec![
            (
                Arc::new(Field::new("id", DataType::Int32, false)),
                Arc::new(Int32Array::from(vec![id])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("name", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![Some("row")])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("age", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![age])) as ArrayRef,
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
        let mut context = ApplyContext {
            refresh_sql: None,
            dataset_name: &dataset_name,
            metric_labels: &metric_labels,
            caching: None,
            ready_sender: None,
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

    /// Without an evolution policy installed, a mixed-schema run keeps
    /// today's behavior verbatim: the concat fails, the run is skipped with
    /// no commits (the source redelivers), and the dataset status is error.
    #[tokio::test]
    async fn test_apply_envelope_run_mixed_schemas_without_policy_keeps_error_skip() {
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
        let mut context = ApplyContext {
            refresh_sql: None,
            dataset_name: &dataset_name,
            metric_labels: &metric_labels,
            caching: None,
            ready_sender: None,
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
            applied,
            "concat failure skips the run but does not stop the stream"
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
        let notify = Arc::new(Notify::new());

        // Subscribe BEFORE running so we don't miss the notify_waiters signal.
        let notified = {
            let n = Arc::clone(&notify);
            tokio::spawn(async move {
                let waiter = n.notified();
                tokio::pin!(waiter);
                tokio::time::timeout(Duration::from_secs(5), &mut waiter)
                    .await
                    .is_ok()
            })
        };
        // Yield so the subscriber registers before we proceed.
        tokio::task::yield_now().await;

        let stream = make_changes_stream(vec![
            Ok(make_tracked_envelope(1, Arc::clone(&log), false)),
            Ok(make_tracked_envelope(2, Arc::clone(&log), true)), // ready=true
            Ok(make_tracked_envelope(3, Arc::clone(&log), false)),
        ]);
        run_changes_stream(
            &task,
            stream,
            Some(Arc::clone(&notify)),
            Arc::clone(&initial_load),
        )
        .await
        .expect("start_changes_stream should succeed");

        assert!(
            initial_load.load(Ordering::Relaxed),
            "initial_load_completed must flip to true once a ready envelope is processed"
        );
        assert!(
            notified.await.expect("ready notifier task must finish"),
            "ready_sender.notify_waiters() must fire when a ready envelope is processed"
        );
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
        let notify = Arc::new(Notify::new());

        let stream = make_changes_stream(vec![
            Ok(make_heartbeat_envelope(false)),
            Ok(make_heartbeat_envelope(true)),
            Ok(make_heartbeat_envelope(false)),
        ]);
        run_changes_stream(
            &task,
            stream,
            Some(Arc::clone(&notify)),
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
        let notify = Arc::new(Notify::new());

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
            Some(Arc::clone(&notify)),
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

    /// Regression test for the CDC prefetch byte counter.
    ///
    /// An envelope pulled from the channel but deferred past the burst byte cap
    /// is stashed in `carried_item` and adopted by the NEXT iteration. It leaves
    /// the channel exactly once, at the `try_recv` that removed it, so it must be
    /// discharged exactly once. Discharging it again when the outer receive
    /// adopted it drove the counter below zero, and the unsigned wrap made
    /// `cdc_prefetch_buffer_bytes` report ~1.8e19 for every table with carry-over
    /// activity - which is how it was found, on a lab run rather than here.
    ///
    /// `max_coalesced_bytes: 1` puts every envelope after the first over budget,
    /// so this drives the carry path on every iteration. The accounting invariant
    /// is enforced by the `debug_assert!` in `discharge_prefetch_bytes`, which is
    /// live in test builds: a double discharge panics here rather than saturating
    /// quietly to zero and looking plausible.
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

    #[test]
    fn test_get_primary_key_value_null_utf8_returns_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        let name_array: ArrayRef = Arc::new(StringArray::from(vec![Option::<&str>::None]));
        let batch =
            RecordBatch::try_new(schema, vec![name_array]).expect("Failed to create RecordBatch");

        let result = get_primary_key_value(&batch, "name");
        assert!(
            result.is_err(),
            "NULL primary key should return an error, not silently produce empty string"
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
}
