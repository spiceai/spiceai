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

//! Compatible CDC burst preparation for the table owner. Source acknowledgement
//! and source control remain outside this module.
//!
//! The owner task drains queued inputs into a burst (`CdcBurst` or
//! `AppendBurst`) until a limit or a compatibility boundary ends it. For CDC,
//! `CdcBurst::prepare` then decodes and checks every input, and splits the
//! burst into `PreparedCdc` groups, before any input mutates storage. Producers
//! opt in with a `CdcIngress` or `AppendIngress` lane handle.

use std::collections::HashSet;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, RecordBatch};
use arrow::compute::kernels::cmp::not_distinct;
use arrow::compute::{cast, filter_record_batch};
use arrow::datatypes::{DataType, Float64Type, SchemaRef};
use arrow::util::display::array_value_to_string;
use data_components::cdc::{self, ChangeOperation};
use datafusion::common::{ScalarValue, TableReference};
use datafusion::error::{DataFusionError, Result};
use opentelemetry::KeyValue;
use runtime_metrics::acceleration as metrics;

use super::source_policy::{CdcPolicy, SchemaDecision};
use super::{
    CdcRows, ChangeBatch, ChangeCapabilities, Recovery, RowsWrite, SchemaEvolutionSupport,
    ScopeKind, SetKey, WriteOptions,
};

/// Refuse a change whose numeric values the accelerated table's column types
/// cannot hold exactly.
///
/// A change proceeds without evolving the table when the source schema widened
/// but the policy or the engine does not evolve it mid-stream, and it is then
/// cast to the table's current types. A cast that rounds or overflows (`1.5` into
/// an `Int32` column) would store a value the source does not hold and
/// acknowledge the change. Refusing keeps the source position instead, so the
/// change is applied once the table is evolved, at the next restart.
///
/// Each numeric column whose type differs from the target's is cast to the
/// target type and back; every value must come back unchanged. `-0.0` stored as
/// `0` counts as unchanged, as in every SQL cast to an integer.
fn ensure_numbers_cast_exactly(data: &RecordBatch, target: &SchemaRef) -> Result<()> {
    for (index, field) in data.schema().fields().iter().enumerate() {
        let Ok(target_field) = target.field_with_name(field.name()) else {
            continue;
        };
        let (from, to) = (field.data_type(), target_field.data_type());
        if from == to || !from.is_numeric() || !to.is_numeric() {
            continue;
        }
        let column = data.column(index);
        let restored = cast(&cast(column, to)?, from)?;
        let unchanged = not_distinct(column, &restored)?;
        if unchanged.true_count() == unchanged.len() {
            continue;
        }
        // `not_distinct` orders floats totally, so `-0.0` restored as `0.0`
        // reads as changed; it is the one difference an integer cast may make.
        let is_zero = |array: &ArrayRef, row: usize| -> Result<bool> {
            let value = cast(&array.slice(row, 1), &DataType::Float64)?;
            let value = value.as_primitive::<Float64Type>();
            Ok(value.is_valid(0) && value.value(0) == 0.0)
        };
        let zero_kept = |row: usize| -> Result<bool> {
            Ok(from.is_floating() && is_zero(column, row)? && is_zero(&restored, row)?)
        };
        for row in (0..unchanged.len()).filter(|&row| !unchanged.value(row)) {
            if zero_kept(row)? {
                continue;
            }
            let value = array_value_to_string(column, row)?;
            return Err(DataFusionError::Execution(format!(
                "Failed to apply a change to column '{}': its value {value} ({from}) cannot be stored in the accelerated column's type {to} without loss, so the change is refused rather than stored rounded. The source column's type changed and the acceleration has not adopted it: with `on_schema_change: sync_all_columns`, restart Spice to apply the schema change. See: https://spiceai.org/docs/features/data-acceleration",
                field.name()
            )));
        }
    }
    Ok(())
}

/// Bounds for one coalesced burst, set by the producer on its lane. A burst
/// ends when any bound is reached, or when the queue is empty and `max_age`
/// is zero.
#[derive(Clone, Copy, Debug)]
pub struct CoalescingLimits {
    /// Maximum number of submitted inputs in one burst.
    pub max_inputs: usize,
    /// Maximum estimated bytes in one burst. A single input can exceed it.
    pub max_bytes: usize,
    /// How long the owner waits for more input, measured from the earlier of
    /// the previous apply cycle and the first input's admission.
    pub max_age: Duration,
}

/// Opt-in append batching for one producer lane. It contains no row queue,
/// source acknowledgement, or replacement policy.
///
/// The caching-mode writer sets one as the `lane` of each `ChangeBatch` it
/// submits, so that many small cache fills, appends and replacements, become
/// one write.
#[derive(Debug)]
pub struct AppendIngress {
    pub limits: CoalescingLimits,
    pub(crate) dataset: TableReference,
}

impl AppendIngress {
    #[must_use]
    pub fn new(dataset: &TableReference, limits: CoalescingLimits) -> Self {
        Self {
            dataset: dataset.clone(),
            limits,
        }
    }
}

/// The owner's single admission drain uses the same limits and boundary rules
/// for CDC and opted-in appends. An active burst is not another data queue.
///
/// Implemented by `CdcBurst` and `AppendBurst`; used by the owner's drain loop.
pub(crate) trait CoalescingBurst: Send {
    /// The lane's limits. `None` means the burst does not wait for more input.
    fn limits(&self) -> Option<CoalescingLimits>;
    /// Number of inputs in the burst.
    fn len(&self) -> usize;
    /// Estimated bytes in the burst.
    fn bytes(&self) -> usize;
    /// The burst accepts no more input.
    fn is_full(&self) -> bool;
    /// Returns `Break` with the unconsumed input when this burst must end.
    fn push(&mut self, next: ChangeBatch, options: WriteOptions) -> ControlFlow<ChangeBatch>;
    /// The CDC lane that receives drain metrics. `None` for appends.
    fn cdc_metrics(&self) -> Option<&Arc<CdcIngress>>;
}

pub(crate) struct AppendBurst {
    write: RowsWrite,
    options: WriteOptions,
    ingress: Arc<AppendIngress>,
    count: usize,
    bytes: usize,
    scopes: BurstScopes,
}

/// The scopes of one coalesced Rows burst. The backend deletes every replaced
/// scope before it appends any row, which equals applying the members in
/// arrival order only while no replaced scope can hold another member's rows.
/// A member that could overlap a replaced scope starts the next burst instead.
#[derive(Default)]
struct BurstScopes {
    /// The column set shared by every indexed scope.
    columns: Option<Vec<usize>>,
    appended: HashSet<Vec<ScalarValue>>,
    replaced: HashSet<Vec<ScalarValue>>,
    /// An append scope that cannot be indexed may overlap a later replacement.
    unindexed_append: bool,
    /// A replacement that cannot be indexed admits no later member.
    closed: bool,
}

impl BurstScopes {
    /// Admit the scope of `batch`, if it has one, and record it.
    fn admit(&mut self, batch: &ChangeBatch) -> bool {
        let Some((scope, kind)) = batch.scoped_input() else {
            return !self.closed;
        };
        if self.closed || !self.permits(scope, kind) {
            return false;
        }
        self.record(scope, kind);
        true
    }

    fn permits(&self, scope: &SetKey, kind: ScopeKind) -> bool {
        let values = self.indexed_values(scope);
        if kind == ScopeKind::Replace {
            values.is_some_and(|values| {
                !self.unindexed_append
                    && !self.appended.contains(&values)
                    && !self.replaced.contains(&values)
            })
        } else {
            self.replaced.is_empty()
                || values.is_some_and(|values| !self.replaced.contains(&values))
        }
    }

    /// The scope's values when it hashes exactly and shares the burst's columns.
    fn indexed_values(&self, scope: &SetKey) -> Option<Vec<ScalarValue>> {
        let (columns, values) = scope.hash_identity()?;
        self.columns
            .as_ref()
            .is_none_or(|shared| *shared == columns)
            .then_some(values)
    }

    fn record(&mut self, scope: &SetKey, kind: ScopeKind) {
        match scope.hash_identity() {
            Some((columns, values))
                if self
                    .columns
                    .as_ref()
                    .is_none_or(|shared| *shared == columns) =>
            {
                self.columns.get_or_insert(columns);
                if kind == ScopeKind::Replace {
                    self.replaced.insert(values);
                } else {
                    self.appended.insert(values);
                }
            }
            _ if kind == ScopeKind::Replace => self.closed = true,
            _ => self.unindexed_append = true,
        }
    }
}

impl AppendBurst {
    /// Start a burst with a row write on a lane. As `push` does, returns
    /// `Break` with the unchanged input when it is CDC or has no lane.
    pub(crate) fn new(batch: ChangeBatch, options: WriteOptions) -> ControlFlow<ChangeBatch, Self> {
        let bytes = batch.estimated_bytes();
        let (write, ingress) = batch.into_laned_rows()?;
        let mut scopes = BurstScopes::default();
        for input in write.scoped_inputs() {
            scopes.record(input.scope(), input.kind());
        }
        ControlFlow::Continue(Self {
            write,
            options,
            ingress,
            count: 1,
            bytes,
            scopes,
        })
    }

    pub(crate) fn dataset(&self) -> &TableReference {
        &self.ingress.dataset
    }

    pub(crate) fn finish(self) -> (RowsWrite, WriteOptions) {
        (self.write, self.options)
    }
}

impl CoalescingBurst for AppendBurst {
    fn limits(&self) -> Option<CoalescingLimits> {
        Some(self.ingress.limits)
    }

    fn len(&self) -> usize {
        self.count
    }

    fn bytes(&self) -> usize {
        self.bytes
    }

    fn is_full(&self) -> bool {
        self.count >= self.ingress.limits.max_inputs.max(1)
            || self.bytes >= self.ingress.limits.max_bytes.max(1)
    }

    fn push(&mut self, next: ChangeBatch, options: WriteOptions) -> ControlFlow<ChangeBatch> {
        let bytes = next.estimated_bytes();
        if self.is_full()
            || self.options.recovery != options.recovery
            || self.options.delete_batch_size != options.delete_batch_size
            || !next
                .append_ingress()
                .is_some_and(|ingress| Arc::ptr_eq(&self.ingress, ingress))
            || self.bytes.saturating_add(bytes) > self.ingress.limits.max_bytes.max(1)
            || !self.write.accepts(&next)
            || !self.scopes.admit(&next)
        {
            return ControlFlow::Break(next);
        }
        self.write.merge(next)?;
        self.count += 1;
        self.bytes = self.bytes.saturating_add(bytes);
        ControlFlow::Continue(())
    }

    fn cdc_metrics(&self) -> Option<&Arc<CdcIngress>> {
        None
    }
}

/// One source lane's policy, limits, and owner-maintained queue accounting.
/// Identity is a coalescing boundary; independent sources need distinct handles.
///
/// The CDC refresh task creates one for each stream and attaches it to every
/// input with `ChangeBatch::cdc_rows`. Only inputs that share this handle
/// coalesce. It also records the CDC queue and apply metrics.
pub struct CdcIngress {
    /// Schema policy the owner uses when it prepares a burst.
    pub policy: Arc<dyn CdcPolicy>,
    /// Burst limits for this lane.
    pub limits: CoalescingLimits,
    labels: [KeyValue; 1],
    queued: parking_lot::Mutex<(usize, usize)>,
    applying: AtomicBool,
}

impl std::fmt::Debug for CdcIngress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CdcIngress")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl CdcIngress {
    #[must_use]
    pub fn new(
        dataset: &TableReference,
        policy: Arc<dyn CdcPolicy>,
        limits: CoalescingLimits,
    ) -> Self {
        Self {
            policy,
            limits,
            labels: [KeyValue::new("dataset", dataset.to_string())],
            queued: parking_lot::Mutex::new((0, 0)),
            applying: AtomicBool::new(false),
        }
    }

    /// Whether the owner is applying a burst from this lane. The producer
    /// builds deferred rows ahead only then: an idle or lingering owner builds
    /// its burst in one handoff, and building ahead would only add handoffs.
    #[must_use]
    pub fn is_applying(&self) -> bool {
        self.applying.load(Ordering::Acquire)
    }

    pub fn record_queue(&self, occupancy: usize, capacity: usize, bytes: usize) {
        metrics::CDC_PREFETCH_BUFFER_OCCUPANCY.record(occupancy as u64, &self.labels);
        metrics::CDC_PREFETCH_BUFFER_CAPACITY.record(capacity as u64, &self.labels);
        metrics::CDC_PREFETCH_BUFFER_BYTES.record(bytes as u64, &self.labels);
    }

    pub(crate) fn queue_enter(&self, bytes: usize, capacity: usize) {
        let mut queued = self.queued.lock();
        queued.0 = queued.0.saturating_add(1);
        queued.1 = queued.1.saturating_add(bytes);
        self.record_queue(queued.0, capacity, queued.1);
    }

    pub(crate) fn queue_leave(&self, bytes: usize, capacity: usize) {
        let mut queued = self.queued.lock();
        debug_assert!(queued.0 > 0 && queued.1 >= bytes);
        queued.0 = queued.0.saturating_sub(1);
        queued.1 = queued.1.saturating_sub(bytes);
        self.record_queue(queued.0, capacity, queued.1);
    }

    pub fn record_send_wait(&self, elapsed: Duration) {
        metrics::CDC_READER_SEND_WAIT_MS.record(elapsed.as_secs_f64() * 1000.0, &self.labels);
    }

    pub fn record_receive_wait(&self, start: Instant) {
        metrics::CDC_SOURCE_RECV_WAIT_MS.record(elapsed_ms(start), &self.labels);
    }

    pub fn record_cycle(&self, start: Instant) {
        metrics::CDC_APPLY_CYCLE_MS.record(elapsed_ms(start), &self.labels);
    }

    pub fn record_linger(&self, start: Instant) {
        metrics::CDC_LINGER_WAIT_MS.record(elapsed_ms(start), &self.labels);
    }

    pub fn record_fixed_cost(&self, phase: &'static str, start: Instant) {
        metrics::CDC_APPLY_FIXED_COST_MS.record(
            elapsed_ms(start),
            &[self.labels[0].clone(), KeyValue::new("phase", phase)],
        );
    }

    pub fn record_drain(
        &self,
        count: usize,
        bytes: usize,
        first_received: Instant,
        reason: &'static str,
    ) {
        metrics::CDC_APPLY_BURST_ENVELOPES.record(count as u64, &self.labels);
        metrics::CDC_APPLY_BURST_BYTES.record(bytes as u64, &self.labels);
        metrics::CDC_COALESCE_BATCH_AGE_MS.record(elapsed_ms(first_received), &self.labels);
        metrics::CDC_COALESCE_FLUSH_TOTAL.add(
            1,
            &[self.labels[0].clone(), KeyValue::new("reason", reason)],
        );
    }

    pub fn record_applied(&self, rows: usize) {
        metrics::CDC_APPLY_BURST_ROWS_TOTAL.add(rows as u64, &self.labels);
    }

    pub fn record_duration(&self, start: Instant) {
        metrics::CDC_APPLY_BURST_DURATION_MS.record(elapsed_ms(start), &self.labels);
    }
}

/// Marks a lane's burst as applying until dropped. `Drop` also covers a
/// cancelled or panicking apply, so the producer never keeps building ahead
/// for an apply that is gone.
pub(crate) struct ApplyingGuard(Arc<CdcIngress>);

impl ApplyingGuard {
    #[must_use]
    pub(crate) fn enter(ingress: &Arc<CdcIngress>) -> Self {
        ingress.applying.store(true, Ordering::Release);
        Self(Arc::clone(ingress))
    }
}

impl Drop for ApplyingGuard {
    fn drop(&mut self) {
        self.0.applying.store(false, Ordering::Release);
    }
}

/// A group applied once and fanned out to `input_count` result observers. Each
/// observer must receive the same publication and durability milestones.
///
/// Produced by `CdcBurst::prepare`. A burst splits into groups at barriers
/// (truncate, empty input) and, when the policy asks, at schema changes.
pub struct PreparedCdc {
    /// The concatenated, decoded CDC rows of the group.
    pub batch: ChangeBatch,
    /// Options for the group. Set to `Recovery::Durable` when the group has
    /// an operation that the backend cannot defer.
    pub options: WriteOptions,
    /// Number of submitted inputs in the group.
    pub input_count: usize,
    /// Number of CDC rows in the group, for metrics.
    pub rows: usize,
    /// The lane of the inputs, if any.
    pub ingress: Option<Arc<CdcIngress>>,
    /// Apply the group as is, or evolve the table schema first.
    pub schema: SchemaDecision,
}

/// Row data drained out of the owner's admission queue. This is an active
/// execution batch, not another producer queue.
///
/// Inputs join only if they share the first input's `CdcIngress`, its
/// `delete_batch_size` and its `Rebuildable` status, fit the limits, and are
/// not barriers.
pub struct CdcBurst {
    inputs: Vec<CdcRows>,
    options: WriteOptions,
    ingress: Option<Arc<CdcIngress>>,
    bytes: usize,
    barrier: bool,
}

impl CdcBurst {
    /// Start a burst with one CDC input.
    #[must_use]
    pub fn new(first: CdcRows, options: WriteOptions) -> Self {
        let ingress = first.ingress().cloned();
        let barrier = first.as_built().is_some_and(cdc_barrier);
        let bytes = first.estimated_bytes();
        Self {
            inputs: vec![first],
            options,
            ingress,
            bytes,
            barrier,
        }
    }

    #[must_use]
    pub fn ingress(&self) -> Option<&Arc<CdcIngress>> {
        self.ingress.as_ref()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.inputs.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inputs.is_empty()
    }

    #[must_use]
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// A singleton may exceed the budget. Further input must fit both caps.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.barrier
            || self.ingress.as_ref().is_none_or(|ingress| {
                self.inputs.len() >= ingress.limits.max_inputs.max(1)
                    || self.bytes >= ingress.limits.max_bytes.max(1)
            })
    }

    /// Does not consume the next command; the owner can carry an incompatible
    /// command forward without losing its reply or callback.
    #[must_use]
    pub fn accepts(&self, next: &ChangeBatch, options: WriteOptions) -> bool {
        if self.is_full()
            || is_barrier(next)
            || self.options.delete_batch_size != options.delete_batch_size
        {
            return false;
        }
        let (Some(ingress), ChangeBatch::Cdc(rows)) = (&self.ingress, next) else {
            return false;
        };
        if !rows
            .ingress()
            .is_some_and(|other| Arc::ptr_eq(ingress, other))
        {
            return false;
        }
        if (self.options.recovery == Recovery::Rebuildable)
            != (options.recovery == Recovery::Rebuildable)
        {
            return false;
        }
        self.bytes.saturating_add(next.estimated_bytes()) <= ingress.limits.max_bytes.max(1)
    }

    /// Returns `Continue` after accepting input, or `Break` with the unchanged
    /// input when a limit or compatibility boundary ends this burst.
    pub fn push(&mut self, next: ChangeBatch, options: WriteOptions) -> ControlFlow<ChangeBatch> {
        if !self.accepts(&next, options) {
            return ControlFlow::Break(next);
        }
        let rows = match next {
            ChangeBatch::Cdc(rows) => rows,
            other => return ControlFlow::Break(other),
        };
        self.bytes = self.bytes.saturating_add(rows.estimated_bytes());
        if options.recovery == Recovery::Durable {
            self.options.recovery = Recovery::Durable;
        }
        self.inputs.push(rows);
        ControlFlow::Continue(())
    }

    /// Decode every input before classification or mutation. Deferred sources
    /// share one blocking-pool handoff; a burst whose inputs are all built,
    /// eagerly or ahead by their producer, takes no task hop.
    ///
    /// # Errors
    /// Returns an error if decoding or schema classification fails, an input has
    /// an unknown operation or rebuild marker, or Arrow batches cannot be combined.
    pub async fn prepare(
        self,
        target: SchemaRef,
        capabilities: ChangeCapabilities,
    ) -> Result<Vec<PreparedCdc>> {
        let decode_start = Instant::now();
        let materialized = self.inputs.iter().all(CdcRows::is_materialized);
        let inputs = self.inputs;
        let decode = move || -> Result<Vec<cdc::ChangeBatch>> {
            inputs
                .into_iter()
                .map(|rows| {
                    rows.into_lazy_parts()
                        .0
                        .into_built()
                        .map_err(|error| DataFusionError::External(Box::new(error)))
                })
                .collect()
        };
        let batches = if materialized {
            decode()?
        } else {
            tokio::task::spawn_blocking(decode)
                .await
                .map_err(|error| {
                    DataFusionError::Execution(format!("CDC burst decode failed: {error}"))
                })??
        };
        if let Some(ingress) = &self.ingress {
            ingress.record_fixed_cost("decode", decode_start);
        }

        // No decoder failure, unknown operation, or source rebuild marker may
        // be discovered after another input in this burst has already mutated.
        for batch in &batches {
            if batch.rebuild_from_this_batch() {
                return Err(DataFusionError::Execution(
                    "A CDC rebuild snapshot must be handled by its source adapter".into(),
                ));
            }
            if (0..batch.record.num_rows())
                .any(|row| matches!(batch.op(row), ChangeOperation::Unknown(_)))
            {
                return Err(DataFusionError::Execution(
                    "Unknown CDC operation; no input in this burst was applied".into(),
                ));
            }
        }
        let coalesce_start = Instant::now();
        let split_schema = self
            .ingress
            .as_ref()
            .is_some_and(|ingress| ingress.policy.split_on_schema_change());
        let mut groups: Vec<Vec<cdc::ChangeBatch>> = Vec::new();
        for batch in batches {
            let barrier = cdc_barrier(&batch);
            let compatible = groups
                .last()
                .and_then(|group| group.last())
                .is_some_and(|previous| {
                    !barrier
                        && !cdc_barrier(previous)
                        && (!split_schema || previous.record.schema() == batch.record.schema())
                });
            if compatible {
                if let Some(group) = groups.last_mut() {
                    group.push(batch);
                }
            } else {
                groups.push(vec![batch]);
            }
        }
        let mut target = target;
        let mut prepared = Vec::with_capacity(groups.len());
        for batches in groups {
            let input_count = batches.len();
            let rows = batches
                .iter()
                .map(|batch| batch.record.num_rows())
                .fold(0_usize, usize::saturating_add);
            let batch = concat(batches)?;
            let upserts = (0..batch.record.num_rows()).any(|row| {
                matches!(
                    batch.op(row),
                    ChangeOperation::Create | ChangeOperation::Update | ChangeOperation::Read
                )
            });
            let schema = if upserts && let Some(ingress) = &self.ingress {
                ingress
                    .policy
                    .classify(&batch.data_schema(), &target, capabilities)?
            } else {
                SchemaDecision::Proceed
            };
            if let SchemaDecision::Evolve(plan) = &schema
                && capabilities.schema_evolution == SchemaEvolutionSupport::Live
            {
                target = Arc::clone(&plan.evolved_schema);
            }
            if upserts
                && matches!(schema, SchemaDecision::Proceed)
                && !batch
                    .data_schema()
                    .fields()
                    .iter()
                    .zip(target.fields().iter())
                    .all(|(incoming, current)| {
                        incoming.name() == current.name()
                            && incoming.data_type() == current.data_type()
                    })
            {
                // A delete applies by primary key, which is never widened in
                // place, so only the rows that write their values are checked.
                let writes: BooleanArray = (0..batch.record.num_rows())
                    .map(|row| {
                        Some(matches!(
                            batch.op(row),
                            ChangeOperation::Create
                                | ChangeOperation::Update
                                | ChangeOperation::Read
                        ))
                    })
                    .collect();
                ensure_numbers_cast_exactly(
                    &filter_record_batch(&batch.data_batch(), &writes)?,
                    &target,
                )?;
            }
            let mut options = self.options;
            if requires_durable_path(&batch, capabilities.deferred_deletes) {
                options.recovery = Recovery::Durable;
            }
            prepared.push(PreparedCdc {
                batch: ChangeBatch::cdc_with_ingress(
                    cdc::LazyChangeBatch::ready(batch),
                    self.ingress.clone(),
                ),
                options,
                input_count,
                rows,
                ingress: self.ingress.clone(),
                schema,
            });
        }
        if let Some(ingress) = &self.ingress {
            ingress.record_fixed_cost("coalesce", coalesce_start);
        }
        Ok(prepared)
    }
}

impl CoalescingBurst for CdcBurst {
    fn limits(&self) -> Option<CoalescingLimits> {
        self.ingress.as_ref().map(|ingress| ingress.limits)
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn bytes(&self) -> usize {
        self.bytes()
    }

    fn is_full(&self) -> bool {
        self.is_full()
    }

    fn push(&mut self, next: ChangeBatch, options: WriteOptions) -> ControlFlow<ChangeBatch> {
        self.push(next, options)
    }

    fn cdc_metrics(&self) -> Option<&Arc<CdcIngress>> {
        self.ingress.as_ref()
    }
}

fn is_barrier(batch: &ChangeBatch) -> bool {
    match batch {
        ChangeBatch::Append { .. }
        | ChangeBatch::AppendScoped { .. }
        | ChangeBatch::Replace { .. } => true,
        ChangeBatch::Cdc(rows) => rows.as_built().is_some_and(cdc_barrier),
    }
}

fn cdc_barrier(batch: &cdc::ChangeBatch) -> bool {
    batch.record.num_rows() == 0
        || batch.rebuild_from_this_batch()
        || (0..batch.record.num_rows()).any(|row| {
            matches!(
                batch.op(row),
                ChangeOperation::Truncate | ChangeOperation::Unknown(_)
            )
        })
}

fn requires_durable_path(batch: &cdc::ChangeBatch, deferred_deletes: bool) -> bool {
    (0..batch.record.num_rows()).any(|row| match batch.op(row) {
        ChangeOperation::Truncate | ChangeOperation::Unknown(_) => true,
        ChangeOperation::Delete => !deferred_deletes || !batch.has_primary_keys(row),
        ChangeOperation::Create | ChangeOperation::Update | ChangeOperation::Read => false,
    })
}

fn concat(mut batches: Vec<cdc::ChangeBatch>) -> Result<cdc::ChangeBatch> {
    if batches.len() == 1 {
        return batches
            .pop()
            .ok_or_else(|| DataFusionError::Internal("Missing CDC batch".into()));
    }
    let first = batches
        .first()
        .ok_or_else(|| DataFusionError::Internal("Empty CDC burst".into()))?;
    let schema = first.record.schema();
    let timestamp = batches
        .iter()
        .filter_map(cdc::ChangeBatch::source_commit_ts_ms)
        .max();
    let record =
        arrow::compute::concat_batches(&schema, batches.iter().map(|batch| &batch.record))?;
    let batch = cdc::ChangeBatch::try_new(record)
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    Ok(batch.with_source_commit_ts_ms(timestamp))
}

fn elapsed_ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Decimal128Array, Float64Array, Int32Array, Int64Array};
    use arrow::datatypes::{Field, Schema};

    use super::*;

    fn check(source: ArrayRef, target: DataType) -> Result<(), String> {
        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "k",
                source.data_type().clone(),
                true,
            )])),
            vec![source],
        )
        .expect("batch");
        let target = Arc::new(Schema::new(vec![Field::new("k", target, true)]));
        ensure_numbers_cast_exactly(&data, &target).map_err(|e| e.to_string())
    }

    /// Regression test for #14718: a change cast to a narrower accelerated
    /// column type is refused when the cast would change a value.
    #[test]
    fn a_change_proceeds_only_when_every_value_casts_exactly() {
        let f64s = |v: Vec<Option<f64>>| -> ArrayRef { Arc::new(Float64Array::from(v)) };
        assert_eq!(
            check(
                f64s(vec![Some(2.0), None, Some(-0.0), Some(-7.0)]),
                DataType::Int32
            ),
            Ok(())
        );
        assert_eq!(
            check(
                Arc::new(Int64Array::from(vec![i64::from(i32::MIN)])),
                DataType::Int32
            ),
            Ok(())
        );
        assert_eq!(
            check(Arc::new(Int32Array::from(vec![1, -1])), DataType::Int64),
            Ok(())
        );
        assert_eq!(
            check(
                Arc::new(Int64Array::from(vec![12_345])),
                DataType::Decimal128(10, 2)
            ),
            Ok(())
        );

        for (source, target, value, from) in [
            (
                f64s(vec![Some(2.0), Some(1.5)]),
                DataType::Int32,
                "1.5",
                "Float64",
            ),
            (
                f64s(vec![Some(f64::NAN)]),
                DataType::Int32,
                "NaN",
                "Float64",
            ),
            (
                f64s(vec![Some(f64::INFINITY)]),
                DataType::Int64,
                "inf",
                "Float64",
            ),
            (
                Arc::new(Int64Array::from(vec![i64::from(i32::MAX) + 1])) as ArrayRef,
                DataType::Int32,
                "2147483648",
                "Int64",
            ),
            (
                Arc::new(Int64Array::from(vec![9_007_199_254_740_993])) as ArrayRef,
                DataType::Float64,
                "9007199254740993",
                "Int64",
            ),
            (
                Arc::new(
                    Decimal128Array::from(vec![12_345])
                        .with_precision_and_scale(10, 2)
                        .expect("decimal"),
                ) as ArrayRef,
                DataType::Decimal128(10, 1),
                "123.45",
                "Decimal128(10, 2)",
            ),
        ] {
            let to = target.to_string();
            let message = check(source, target).expect_err("lossy cast is refused");
            assert!(
                message.contains(&format!(
                    "Failed to apply a change to column 'k': its value {value} ({from}) cannot be stored in the accelerated column's type {to} without loss"
                )),
                "{message}"
            );
        }
    }
}
