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

use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use data_components::cdc::{self, ChangeOperation};
use datafusion::common::TableReference;
use datafusion::error::{DataFusionError, Result};
use opentelemetry::KeyValue;
use runtime_metrics::acceleration as metrics;

use super::source_policy::{CdcPolicy, SchemaDecision};
use super::{
    ChangeBatch, ChangeCapabilities, ChangePayload, Recovery, SchemaEvolutionSupport, WriteOptions,
};

#[derive(Clone, Copy, Debug)]
pub struct CoalescingLimits {
    pub max_inputs: usize,
    pub max_bytes: usize,
    pub max_age: Duration,
}

/// Opt-in append batching for one producer lane. It contains no row queue,
/// source acknowledgement, or replacement policy.
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
pub(crate) trait CoalescingBurst: Send {
    fn limits(&self) -> Option<CoalescingLimits>;
    fn len(&self) -> usize;
    fn bytes(&self) -> usize;
    fn is_full(&self) -> bool;
    /// Returns `Break` with the unconsumed input when this burst must end.
    fn push(&mut self, next: ChangeBatch, options: WriteOptions) -> ControlFlow<ChangeBatch>;
    fn cdc_metrics(&self) -> Option<&Arc<CdcIngress>>;
}

pub(crate) struct AppendBurst {
    batch: ChangeBatch,
    options: WriteOptions,
    ingress: Arc<AppendIngress>,
    count: usize,
    bytes: usize,
}

impl AppendBurst {
    pub(crate) fn new(batch: ChangeBatch, options: WriteOptions) -> Result<Self> {
        let ingress = batch.append_ingress().cloned().ok_or_else(|| {
            DataFusionError::Internal("Append batching requires a producer lane".into())
        })?;
        if !batch.can_merge_append(&batch) {
            return Err(DataFusionError::Internal(
                "Only Rows appends can enter an append burst".into(),
            ));
        }
        let bytes = batch.estimated_bytes();
        Ok(Self {
            batch,
            options,
            ingress,
            count: 1,
            bytes,
        })
    }

    pub(crate) fn dataset(&self) -> &TableReference {
        &self.ingress.dataset
    }

    pub(crate) fn finish(self) -> (ChangeBatch, WriteOptions) {
        (self.batch, self.options)
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
        {
            return ControlFlow::Break(next);
        }
        self.batch.merge_append(next)?;
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
pub struct CdcIngress {
    pub policy: Arc<dyn CdcPolicy>,
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
pub struct PreparedCdc {
    pub batch: ChangeBatch,
    pub options: WriteOptions,
    pub input_count: usize,
    pub rows: usize,
    pub ingress: Option<Arc<CdcIngress>>,
    pub schema: SchemaDecision,
}

/// Row data drained out of the owner's admission queue. This is an active
/// execution batch, not another producer queue.
pub struct CdcBurst {
    inputs: Vec<ChangeBatch>,
    options: WriteOptions,
    ingress: Option<Arc<CdcIngress>>,
    bytes: usize,
    barrier: bool,
}

impl CdcBurst {
    /// Start a burst with one CDC input.
    ///
    /// # Errors
    /// Returns an error if the first input is not CDC data.
    pub fn new(first: ChangeBatch, options: WriteOptions) -> Result<Self> {
        let ChangePayload::Cdc(rows) = first.payload() else {
            return Err(DataFusionError::Internal(
                "A CDC burst requires CDC input".into(),
            ));
        };
        let ingress = rows.ingress().cloned();
        let barrier = is_barrier(&first);
        let bytes = first.estimated_bytes();
        Ok(Self {
            inputs: vec![first],
            options,
            ingress,
            bytes,
            barrier,
        })
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
        let (Some(ingress), ChangePayload::Cdc(rows)) = (&self.ingress, next.payload()) else {
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
        self.bytes = self.bytes.saturating_add(next.estimated_bytes());
        if options.recovery == Recovery::Durable {
            self.options.recovery = Recovery::Durable;
        }
        self.inputs.push(next);
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
        let materialized = self.inputs.iter().all(
            |batch| matches!(batch.payload(), ChangePayload::Cdc(rows) if rows.is_materialized()),
        );
        let inputs = self.inputs;
        let decode = move || -> Result<Vec<cdc::ChangeBatch>> {
            inputs
                .into_iter()
                .map(|batch| {
                    let (ChangePayload::Cdc(rows), None) = batch.into_parts() else {
                        return Err(DataFusionError::Internal(
                            "Non-CDC input in a CDC burst".into(),
                        ));
                    };
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
    match batch.payload() {
        ChangePayload::Rows { .. } => true,
        ChangePayload::Cdc(rows) => rows.as_built().is_some_and(cdc_barrier),
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
