/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Durable federated write-back delivery worker (#11838).
//!
//! One worker per durable-write-back Cayenne dataset, spawned onto the
//! [`AcceleratedTable`](super::AcceleratedTable)'s `handlers` (aborted on drop).
//! It reconciles the dirty-key markers a committed write leaves in
//! `cayenne_pending_write_back` to the federated source, in strict order:
//!
//! 1. **List** a batch of markers from the two claim queues (see *Claim
//!    scheduling* below) — a plain read, NOT an atomic reservation; concurrency
//!    safety comes from the compare-and-clear in step 4, not from claiming these
//!    markers here. Each marker carries the [`WriteBackOp`] of the commit that
//!    dirtied it.
//! 2. **Read** those keys' *current* committed values from the accelerator
//!    (a fenced point scan), AFTER the list.
//! 3. **Deliver** to the source idempotently, from the marked intent:
//!    - a key the read returned is upserted with that value;
//!    - a key the read did not return, marked [`WriteBackOp::Delete`], is
//!      deleted at the source;
//!    - a key the read did not return, marked [`WriteBackOp::Upsert`], is not
//!      delivered at all and its marker is **kept**, to be retried on a later
//!      pass (see [`classify_delivery`]);
//!    - a key the read DID return, marked [`WriteBackOp::Delete`], is deferred
//!      to the next pass with its marker intact, because a commit publishes its
//!      marker before the delete is scan-visible (see [`classify_delivery`]).
//!
//!    If the source cannot do a native upsert (it answers `Replace` with
//!    `NotImplemented`), delivery falls back to a delete-then-insert emulation
//!    over the keys being upserted — a temporary path that reopens the #11915
//!    window, kept only until every durable-write-back source supports native
//!    upsert.
//! 4. **Compare-and-clear** the markers whose stored sequence is still at or
//!    below the sequence listed in step 1 — a newer commit that bumped a marker
//!    during delivery leaves it in place, so the stale delivery never clears a
//!    fresh mark.
//!
//! Delivery failure never blocks accelerator commits; the dirty set simply
//! grows until the next successful pass. Marking happens only in a commit
//! transaction (never in the CDC apply path), so an echo of our own write cannot
//! spawn a fresh delivery.
//!
//! # Absence is not a delete, and it is not a reason to give up
//!
//! Delivery acts on the operation each marker records, never on whether the
//! accelerator still holds the key. A key can be missing for reasons that are
//! not a deletion — the read could not see it yet, or something evicted it —
//! and turning any of those into a source `DELETE` destroys rows nobody deleted.
//! A committed `DELETE` marks its keys explicitly, which is what makes the
//! source delete safe to issue.
//!
//! Absence is equally not grounds for *dropping* the marker. The marker is the
//! only durable record that an acknowledged write has not reached the source, so
//! retiring one because a pass could not read its key would silently lose that
//! write — the source keeps its previous value and nothing is left to reconcile
//! it. A marker is therefore cleared only by delivery (or by `drop_table`). The
//! accelerator is expected to hold every acknowledged write, so a key that
//! cannot be read is a condition to wait out, not evidence to act on. Retention
//! is the one thing that legitimately evicts a live row, which is why a
//! durable-write-back dataset refuses to configure it
//! (`validate_durable_write_back_retention`, in `accelerator-cayenne`).
//!
//! # Claim scheduling
//!
//! Because markers are never retired, an undeliverable one stays in the table
//! indefinitely — and a single claim taking the oldest N markers would hand
//! every pass the same wedged set once N of them accumulated, starving every
//! newer write behind them. The markers carry their own schedule instead
//! (`delivery_attempts`, `last_delivery_attempt`), and each pass draws from two
//! queues on separate budgets:
//!
//! * **fresh** ([`FRESH_CLAIM_BATCH`]) — never attempted, plus the one immediate
//!   retry a first miss is owed, oldest commit first. The common deferral is a
//!   delete whose marker is published before the delete is scan-visible, which
//!   resolves in milliseconds; it must not have to wait out a rotation.
//! * **deferred** ([`DEFERRED_CLAIM_BATCH`]) — everything that has missed twice
//!   or more, least-recently-attempted first, so waiting longer monotonically
//!   improves a marker's position and nothing can be starved.
//!
//! Both queues are delivered as one batch: one point scan, one delivery, one
//! compare-and-clear. The split governs what is *claimed*, not how it is sent.
//!
//! # Known limitation — mixed writers
//!
//! The present-key upsert is an unconditional `ON CONFLICT (pk) DO UPDATE`: it
//! overwrites the source row with the accelerator's value regardless of what
//! the source currently holds. A second writer that mutates the same source row
//! directly (not through this accelerator) can therefore be clobbered — this
//! worker does no compare-and-set against the source. Durable write-back is
//! safe only when the accelerator is the sole writer of the rows it delivers.

use std::collections::HashSet;
use std::sync::Arc;

use std::time::{Duration, Instant};

use arrow::array::{Array, ArrayRef, BooleanArray};
use arrow::record_batch::RecordBatch;
use cayenne::{CayenneTableProvider, PendingWriteBackMarker, WriteBackOp};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, col, lit};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::scalar::ScalarValue;
use parking_lot::Mutex;
use tokio::task::JoinHandle;
use util::fibonacci_backoff::FibonacciBackoffBuilder;

use super::write::write_back::execute_insert;
use crate::federated::FederatedTable;

/// Markers claimed per delivery pass, across both queues — the point scan and
/// the delivery it feeds are sized by this, not by either queue alone.
const CLAIM_BATCH: usize = 1024;
/// The deferred rotation's share of a pass. A minority share, so a large wedged
/// set can slow fresh delivery but never crowd it out; at this size a thousand
/// wedged markers still rotate fully through in a few seconds.
const DEFERRED_CLAIM_BATCH: usize = CLAIM_BATCH / 8;
/// The fresh queue takes the rest. A pass that cannot fill the deferred share —
/// the healthy case, where nothing is wedged — does not hand the remainder back
/// to the fresh queue, so the batch is a ceiling rather than a target.
const FRESH_CLAIM_BATCH: usize = CLAIM_BATCH - DEFERRED_CLAIM_BATCH;
/// Idle poll interval when the dirty set is empty (not a failure — the error
/// backoff must not grow on empty polls).
const POLL_INTERVAL: Duration = Duration::from_secs(1);
/// How often to restate that markers are stuck. The deferred rotation re-claims
/// them every pass, so an unthrottled warning would repeat every second for as
/// long as the condition lasts.
const STALL_WARNING_INTERVAL: Duration = Duration::from_mins(5);

pub(crate) struct WriteBackWorker {
    /// A write-clone of the durable-write-back Cayenne provider — shares the
    /// live table's catalog, listing fence, and keyset, so the marker CRUD and
    /// the point scan observe committed state.
    provider: Arc<CayenneTableProvider>,
    federated: Arc<FederatedTable>,
    /// Primary-key column names, in key order.
    pk_columns: Vec<String>,
    dataset_name: String,
    /// When the stuck-marker warning was last emitted. Purely a log rate limit —
    /// the delivery schedule itself lives on the markers, so nothing here is
    /// load-bearing across a restart. Only ever touched between awaits.
    stall_warned_at: Mutex<Option<Instant>>,
}

impl WriteBackWorker {
    /// Spawn the delivery loop; the returned handle is pushed onto the
    /// accelerated table's `handlers` and aborted when the table drops.
    pub(crate) fn spawn(
        provider: CayenneTableProvider,
        federated: Arc<FederatedTable>,
        dataset_name: String,
    ) -> JoinHandle<()> {
        let pk_columns = provider.pk_column_names();
        let worker = Self {
            provider: Arc::new(provider),
            federated,
            pk_columns,
            dataset_name,
            stall_warned_at: Mutex::new(None),
        };
        tokio::spawn(async move { worker.run().await })
    }

    async fn run(&self) {
        // v1 delivers single-column primary keys (the common `id` shape). A
        // composite/absent key can't be turned into a simple `pk IN (...)`
        // filter here; leave those markers for a follow-up rather than deliver
        // incorrectly.
        if self.pk_columns.len() != 1 {
            tracing::warn!(
                dataset = %self.dataset_name,
                pk_columns = self.pk_columns.len(),
                "durable write-back worker: only single-column primary keys are supported in v1; \
                 markers for this dataset will accumulate undelivered"
            );
            return;
        }

        // Infinite Fibonacci backoff on delivery ERRORS (delivery must never
        // permanently give up). Rebuilt after every successful pass so a
        // transient failure never leaves us stuck at a long delay, and never
        // advanced by an empty poll (an empty dirty set is not a failure).
        let mut backoff = FibonacciBackoffBuilder::new().max_retries(None).build();
        loop {
            match self.deliver_batch().await {
                Ok(claimed_fresh) => {
                    // Success — reset the error backoff.
                    backoff = FibonacciBackoffBuilder::new().max_retries(None).build();
                    if claimed_fresh < FRESH_CLAIM_BATCH {
                        // Fresh queue drained (fewer than a full batch remained);
                        // idle-poll for the next commit — NOT an error, so the
                        // backoff stays reset. Pacing on the fresh queue alone is
                        // deliberate: the deferred rotation is never "drained",
                        // so pacing on the whole claim would spin at full tilt
                        // for as long as anything was stuck.
                        tokio::time::sleep(POLL_INTERVAL).await;
                    }
                    // Else a full fresh batch was claimed — more may remain; loop
                    // immediately to keep draining.
                }
                Err(e) => {
                    let delay = backoff.next_duration().unwrap_or(POLL_INTERVAL);
                    tracing::warn!(
                        dataset = %self.dataset_name,
                        error = %e,
                        "durable write-back delivery failed; retrying in {delay:?}"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// One claim → read → deliver → clear pass. Returns how many markers were
    /// claimed from the **fresh** queue, which is what paces the loop; the
    /// deferred rotation has no drained state to pace on.
    async fn deliver_batch(&self) -> DataFusionResult<usize> {
        let fresh = self
            .provider
            .list_fresh_dirty_keys(FRESH_CLAIM_BATCH)
            .await
            .map_err(to_df_err)?;
        let deferred = self
            .provider
            .list_deferred_dirty_keys(DEFERRED_CLAIM_BATCH)
            .await
            .map_err(to_df_err)?;
        self.warn_if_markers_are_stuck(&deferred);

        let claimed_fresh = fresh.len();
        // The two queues partition the marker set (`delivery_attempts <= 1`
        // against `> 1`), so concatenating cannot produce a duplicate key — and
        // from here on the pass treats them identically.
        let mut claimed = fresh;
        claimed.extend(deferred);
        if claimed.is_empty() {
            return Ok(0);
        }

        let pk_bytes: Vec<Vec<u8>> = claimed
            .iter()
            .map(|marker| marker.pk_bytes.clone())
            .collect();
        let pk_arrays = self.provider.decode_pk_keys(&pk_bytes).map_err(to_df_err)?;
        let Some(pk_values) = pk_arrays.into_iter().next() else {
            return Ok(0);
        };
        let filter = pk_in_filter(&self.pk_columns[0], &pk_values)?;

        // Read the claimed keys' current committed values from the accelerator,
        // AFTER the claim (a newer commit bumps the marker above the claimed
        // sequence, so the clear below no-ops for it). Build the context from the
        // provider's SHARED RuntimeEnv (object-store registrations for S3, memory
        // pool, caches) — a fresh `SessionContext::new()` would lose them and fail
        // object-store-backed scans.
        let ctx =
            SessionContext::new_with_config_rt(SessionConfig::new(), self.provider.runtime_env());
        // `Arc<CayenneTableProvider>` coerces to the `Arc<dyn TableProvider>`
        // `read_table` expects at the call argument below.
        let accelerator = Arc::clone(&self.provider);
        let current = ctx
            .read_table(accelerator)?
            .filter(filter.clone())?
            .collect()
            .await?;
        let session_state = ctx.state();

        // Decide what to deliver for each claimed key from the marked operation
        // and whether the post-claim read still returned it.
        let pk_col = self.pk_columns[0].as_str();
        let plan = classify_delivery(pk_col, &claimed, &pk_values, &current)?;
        // The read returns the deferred keys' rows too; they must not be
        // upserted, so drop them before anything is delivered.
        let current = retain_deliverable_rows(pk_col, &current, &plan.deferred)?;
        let has_present = current.iter().any(|batch| batch.num_rows() > 0);

        if !plan.deferred.is_empty() {
            tracing::debug!(
                dataset = %self.dataset_name,
                keys = plan.deferred.len(),
                "durable write-back: {} key(s) marked deleted are still readable in the accelerator; leaving their markers for the next pass rather than guessing between a re-created row and a delete that is committed but not yet visible",
                plan.deferred.len(),
            );
        }

        if !plan.absent.is_empty() {
            tracing::debug!(
                dataset = %self.dataset_name,
                keys = plan.absent.len(),
                "durable write-back: {} committed key(s) were not readable in the accelerator; keeping their markers to retry, since nothing but delivery may retire one",
                plan.absent.len(),
            );
        }

        let federated_provider = self.federated.table_provider().await;

        // Attempt an upsert. If the federated source does not support it
        // (`DataFusionError::NotImplemented`) fall back to delete-then-insert.
        if has_present {
            match execute_insert(
                Arc::clone(&federated_provider),
                self.provider.table_schema(),
                current.clone(),
                InsertOp::Replace,
                &session_state,
                None,
            )
            .await
            {
                Ok(()) => {}
                Err(e) if matches!(e, DataFusionError::NotImplemented(_)) => {
                    tracing::warn!(
                        dataset = %self.dataset_name,
                        error = %e,
                        "durable write-back: source does not support InsertOp::Replace; falling back to delete-then-insert delivery"
                    );
                    // Delete only the keys about to be re-inserted. A blanket
                    // delete over every claimed key would also remove the keys
                    // this pass deliberately left alone.
                    let present_filter =
                        col(pk_col).in_list(plan.present.iter().cloned().map(lit).collect(), false);
                    let _ = datafusion::physical_plan::collect(
                        federated_provider
                            .delete_from(&session_state, vec![present_filter])
                            .await?,
                        session_state.task_ctx(),
                    )
                    .await?;
                    execute_insert(
                        Arc::clone(&federated_provider),
                        self.provider.table_schema(),
                        current,
                        InsertOp::Append,
                        &session_state,
                        None,
                    )
                    .await?;
                }
                Err(e) => return Err(e),
            }
        }

        // Keys a committed DELETE removed. `classify_delivery` puts a key in
        // exactly one of these sets, so this delete cannot undo the upsert above.
        if !plan.deleted.is_empty() {
            let delete_filter =
                col(pk_col).in_list(plan.deleted.into_iter().map(lit).collect(), false);
            let delete_plan = federated_provider
                .delete_from(&session_state, vec![delete_filter])
                .await?;
            let _ =
                datafusion::physical_plan::collect(delete_plan, session_state.task_ctx()).await?;
        }

        // Ack: clear only markers this pass actually delivered, and only while
        // their stored sequence is still at/below the claimed one — a commit that
        // re-dirtied the key during this pass bumped it above what was claimed,
        // so its marker survives and is delivered next pass. Everything withheld
        // keeps its marker: nothing but delivery may retire one.
        self.provider
            .clear_dirty_keys(&plan.clearable)
            .await
            .map_err(to_df_err)?;
        // Charge the withheld markers an attempt, which is what moves them into
        // the deferred rotation and orders them within it. Recorded after the
        // clear so a delivery failure above aborts the pass without charging a
        // key that was never judged.
        self.provider
            .record_delivery_attempts(&plan.withheld)
            .await
            .map_err(to_df_err)?;
        Ok(claimed_fresh)
    }

    /// Restate, at most every [`STALL_WARNING_INTERVAL`], that markers are stuck.
    ///
    /// A non-empty deferred rotation means acknowledged writes have failed to
    /// reach the source repeatedly. Nothing is lost — the markers are kept and
    /// retried — but it will not resolve on its own if the accelerator has
    /// genuinely dropped those rows, so it needs to be visible rather than
    /// silently patient. The count covers both reasons a pass withholds a key
    /// (unreadable, or delete-marked and still readable), so the message names
    /// the outcome rather than guessing which.
    fn warn_if_markers_are_stuck(&self, deferred: &[PendingWriteBackMarker]) {
        let Some(worst) = deferred.iter().map(|m| m.delivery_attempts).max() else {
            return;
        };
        {
            let mut warned_at = self.stall_warned_at.lock();
            let now = Instant::now();
            if warned_at.is_some_and(|at| now.duration_since(at) < STALL_WARNING_INTERVAL) {
                return;
            }
            *warned_at = Some(now);
        }
        tracing::warn!(
            dataset = %self.dataset_name,
            keys = deferred.len(),
            attempts = worst,
            "Dataset '{}': {} committed row(s) have not been written back to the federated source after repeated attempts (one of them {} times), so the source still holds their previous values. Their markers are kept and keep being retried, and nothing has been deleted at the source; if this does not clear on its own, the accelerator is missing rows it acknowledged. See: https://spiceai.org/docs/reference/spicepod/datasets#acceleration",
            self.dataset_name,
            deferred.len(),
            worst,
        );
    }
}

/// Build `pk_col IN (values…)` from a decoded primary-key array.
fn pk_in_filter(pk_col: &str, values: &ArrayRef) -> DataFusionResult<Expr> {
    let mut list: Vec<Expr> = Vec::with_capacity(values.len());
    for index in 0..values.len() {
        list.push(lit(ScalarValue::try_from_array(values.as_ref(), index)?));
    }
    Ok(col(pk_col).in_list(list, false))
}

/// Drop the deferred keys' rows from the accelerator read, so the upsert
/// delivers only what this pass classified as present. The read is filtered by
/// primary key alone, so it returns the deferred keys' rows too, and upserting
/// one would push a row the user deleted back to the source.
fn retain_deliverable_rows(
    pk_col: &str,
    current: &[RecordBatch],
    deferred: &[ScalarValue],
) -> DataFusionResult<Vec<RecordBatch>> {
    if deferred.is_empty() {
        return Ok(current.to_vec());
    }
    let deferred: HashSet<&ScalarValue> = deferred.iter().collect();
    let mut kept = Vec::with_capacity(current.len());
    for batch in current {
        let Some(column) = batch.column_by_name(pk_col) else {
            return Err(DataFusionError::Execution(format!(
                "durable write-back: primary-key column '{pk_col}' missing from the accelerator read"
            )));
        };
        let mut mask = Vec::with_capacity(column.len());
        for row in 0..column.len() {
            let key = ScalarValue::try_from_array(column.as_ref(), row)?;
            mask.push(!deferred.contains(&key));
        }
        let filtered = arrow::compute::filter_record_batch(batch, &BooleanArray::from(mask))?;
        if filtered.num_rows() > 0 {
            kept.push(filtered);
        }
    }
    Ok(kept)
}

/// What one delivery pass should do with each claimed key.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct DeliveryPlan {
    /// Keys the post-claim read returned; delivered as an upsert of that value.
    pub(crate) present: Vec<ScalarValue>,
    /// Keys a committed `DELETE` removed; delivered as a source delete.
    pub(crate) deleted: Vec<ScalarValue>,
    /// Keys the read did not return that no committed `DELETE` marked. Nothing
    /// is delivered for these and their markers are kept — see the module's
    /// "Absence is not a delete, and it is not a reason to give up".
    pub(crate) absent: Vec<ScalarValue>,
    /// Keys marked deleted that the read still returned. Nothing is delivered
    /// for these and their markers are kept — see [`classify_delivery`]. Kept
    /// apart from `absent` because these DO have rows in the read, which must be
    /// dropped before the upsert (see [`retain_deliverable_rows`]).
    pub(crate) deferred: Vec<ScalarValue>,
    /// The claimed markers this pass may clear once it has delivered: the
    /// `present` and `deleted` ones.
    pub(crate) clearable: Vec<PendingWriteBackMarker>,
    /// The claimed markers this pass delivered nothing for: the `absent` and
    /// `deferred` ones, which keep their markers and are charged one delivery
    /// attempt so the schedule can rotate them.
    pub(crate) withheld: Vec<PendingWriteBackMarker>,
}

/// Decide what to deliver for each claimed key.
///
/// A key the read returned whose marker records an [`WriteBackOp::Upsert`] is
/// upserted with that value: the read is the authority on the value.
///
/// A key the read did not return is deleted at the source **only** when its
/// marker records a [`WriteBackOp::Delete`]. Absence on its own proves nothing:
/// a read that could not see the row yet looks identical to a deletion, and
/// deleting on that evidence destroys rows nobody deleted — which is why this
/// returns them as `absent` rather than as deletes.
///
/// An absent key whose marker records an [`WriteBackOp::Upsert`] keeps its
/// marker indefinitely. Retiring it would discard the only durable record of an
/// acknowledged write, leaving the source on its previous value with nothing
/// left to reconcile it — and no bound on the retries makes that safe, since a
/// miss that has recurred a hundred times is no more evidence the row is gone
/// than the first one was. The marker is charged an attempt instead, which moves
/// it into the claim rotation that keeps it from starving newer writes (see the
/// module's *Claim scheduling*).
///
/// A key the read returned whose marker records a [`WriteBackOp::Delete`] is
/// **deferred**: nothing is delivered and its marker is withheld from the clear,
/// because the two things that produce this combination are indistinguishable
/// here and one of them is destructive to guess at.
///
/// * A later commit re-created the key. That commit bumped the marker above the
///   claimed sequence, so the clear would no-op for it anyway and the next pass
///   delivers the new value — deferring costs one pass of latency.
/// * The delete is committed but not yet scan-visible. A commit publishes its
///   marker to the metastore before the caches and the in-memory tier reflect
///   the delete, and delivery takes no lock against a writer, so a pass landing
///   in that window reads the deleted row as still present. Upserting it and
///   clearing the marker would drop the delete permanently: the accelerator
///   goes on to remove the row while the source keeps it, with no marker left
///   to reconcile them.
///
/// Deferring converges in both cases on the next pass, which is why this one
/// stays in the fresh claim queue for its first retry rather than being demoted
/// to the rotation.
///
/// `claimed` and `claimed_pks` are parallel: `claimed_pks[i]` is the decoded key
/// of `claimed[i]`.
pub(crate) fn classify_delivery(
    pk_col: &str,
    claimed: &[PendingWriteBackMarker],
    claimed_pks: &ArrayRef,
    current: &[RecordBatch],
) -> DataFusionResult<DeliveryPlan> {
    if claimed.len() != claimed_pks.len() {
        return Err(DataFusionError::Execution(format!(
            "durable write-back: {} claimed markers do not line up with {} decoded keys",
            claimed.len(),
            claimed_pks.len()
        )));
    }

    let mut present: HashSet<ScalarValue> = HashSet::new();
    for batch in current {
        let Some(column) = batch.column_by_name(pk_col) else {
            return Err(DataFusionError::Execution(format!(
                "durable write-back: primary-key column '{pk_col}' missing from the accelerator read"
            )));
        };
        for row in 0..column.len() {
            present.insert(ScalarValue::try_from_array(column.as_ref(), row)?);
        }
    }

    let mut plan = DeliveryPlan::default();
    for (index, marker) in claimed.iter().enumerate() {
        let key = ScalarValue::try_from_array(claimed_pks.as_ref(), index)?;
        match (present.contains(&key), marker.op) {
            (true, WriteBackOp::Upsert) => plan.present.push(key),
            (false, WriteBackOp::Delete) => plan.deleted.push(key),
            (true, WriteBackOp::Delete) => {
                plan.deferred.push(key);
                plan.withheld.push(marker.clone());
                continue;
            }
            (false, WriteBackOp::Upsert) => {
                plan.absent.push(key);
                plan.withheld.push(marker.clone());
                continue;
            }
        }
        plan.clearable.push(marker.clone());
    }
    Ok(plan)
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "passed to `Result::map_err`, which moves the error value in"
)]
fn to_df_err(e: cayenne::provider::Error) -> DataFusionError {
    DataFusionError::Execution(format!("durable write-back: {e}"))
}

#[cfg(test)]
mod tests {
    use super::{DeliveryPlan, classify_delivery};
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use cayenne::{PendingWriteBackMarker, WriteBackOp};
    use datafusion::scalar::ScalarValue;
    use std::sync::Arc;

    const PK_COL: &str = "id";

    fn marker(pk: i64, op: WriteBackOp) -> PendingWriteBackMarker {
        attempted_marker(pk, op, 0)
    }

    fn attempted_marker(pk: i64, op: WriteBackOp, attempts: i64) -> PendingWriteBackMarker {
        PendingWriteBackMarker {
            // The classifier reads the op and relies on the caller's decoded key
            // array for identity, so the exact encoding here is immaterial.
            pk_bytes: pk.to_be_bytes().to_vec(),
            sequence_number: pk,
            op,
            delivery_attempts: attempts,
        }
    }

    fn keys(pks: &[i64]) -> ArrayRef {
        Arc::new(Int64Array::from(pks.to_vec()))
    }

    /// The rows an accelerator read returned for the keys still present.
    fn read_returning(pks: &[i64]) -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            PK_COL,
            DataType::Int64,
            false,
        )]));
        vec![RecordBatch::try_new(schema, vec![keys(pks)]).expect("valid single-column key batch")]
    }

    fn scalars(pks: &[i64]) -> Vec<ScalarValue> {
        pks.iter().map(|pk| ScalarValue::Int64(Some(*pk))).collect()
    }

    /// The regression this whole change exists for: a key that is simply gone
    /// from the accelerator — a read that could not see it, something that
    /// evicted it — carries an upsert marker, and must NEVER be deleted at the
    /// source. Its marker is kept so the write can still be delivered.
    #[test]
    fn an_absent_key_without_a_delete_marker_is_never_deleted_at_the_source() {
        let claimed = vec![marker(1, WriteBackOp::Upsert)];
        let plan = classify_delivery(PK_COL, &claimed, &keys(&[1]), &read_returning(&[]))
            .expect("classification succeeds");

        assert_eq!(
            plan,
            DeliveryPlan {
                present: vec![],
                deleted: vec![],
                absent: scalars(&[1]),
                deferred: vec![],
                clearable: vec![],
                withheld: claimed,
            },
            "an absent upsert-marked key must deliver nothing and keep its marker, never be deleted"
        );
    }

    /// An acknowledged write whose row the read could not see keeps its marker
    /// however many passes have already missed it. Retiring one would throw away
    /// the only durable record of that write, leaving the source on its previous
    /// value with nothing left to reconcile it — and a miss that has recurred a
    /// hundred times is no more evidence the row is gone than the first was, so
    /// no attempt count makes retiring it safe.
    #[test]
    fn an_absent_upsert_marker_survives_any_number_of_failed_attempts() {
        for attempts in [0, 1, 2, 50, 10_000] {
            let claimed = vec![attempted_marker(1, WriteBackOp::Upsert, attempts)];
            let plan = classify_delivery(PK_COL, &claimed, &keys(&[1]), &read_returning(&[]))
                .expect("classification succeeds");

            assert_eq!(
                plan.absent,
                scalars(&[1]),
                "a marker attempted {attempts} times is still owed delivery"
            );
            assert!(
                plan.clearable.is_empty(),
                "the marker must survive after {attempts} attempts: clearing it loses the write"
            );
            assert_eq!(plan.withheld, claimed);
        }
    }

    /// Every claimed marker leaves the pass in exactly one of the two sets, so a
    /// marker can neither be dropped without being delivered nor be both cleared
    /// and charged an attempt.
    #[test]
    fn every_claimed_marker_is_either_cleared_or_withheld() {
        let claimed = vec![
            marker(1, WriteBackOp::Upsert),
            marker(2, WriteBackOp::Delete),
            marker(3, WriteBackOp::Upsert),
            marker(4, WriteBackOp::Delete),
        ];
        let plan = classify_delivery(
            PK_COL,
            &claimed,
            &keys(&[1, 2, 3, 4]),
            &read_returning(&[1, 4]),
        )
        .expect("classification succeeds");

        let mut accounted: Vec<&PendingWriteBackMarker> =
            plan.clearable.iter().chain(plan.withheld.iter()).collect();
        accounted.sort_by_key(|marker| marker.sequence_number);
        assert_eq!(
            accounted,
            claimed.iter().collect::<Vec<_>>(),
            "each claimed marker must be accounted for exactly once"
        );
    }

    #[test]
    fn an_absent_key_with_a_delete_marker_is_deleted_at_the_source() {
        let claimed = vec![marker(1, WriteBackOp::Delete)];
        let plan = classify_delivery(PK_COL, &claimed, &keys(&[1]), &read_returning(&[]))
            .expect("classification succeeds");

        assert_eq!(plan.deleted, scalars(&[1]));
        assert!(plan.present.is_empty() && plan.absent.is_empty());
        assert_eq!(
            plan.clearable, claimed,
            "the delete was delivered, so its marker is retired"
        );
    }

    /// A delete-marked key the read still returned is ambiguous — a later
    /// commit re-created it, or the delete is committed but not yet
    /// scan-visible — so the pass delivers nothing and keeps the marker.
    ///
    /// Clearing it would lose the delete outright in the second case: the
    /// accelerator goes on to remove the row while the source keeps it, with no
    /// marker left to reconcile them.
    #[test]
    fn a_present_key_whose_marker_says_delete_is_deferred_with_its_marker_kept() {
        let claimed = vec![marker(1, WriteBackOp::Delete)];
        let plan = classify_delivery(PK_COL, &claimed, &keys(&[1]), &read_returning(&[1]))
            .expect("classification succeeds");

        assert_eq!(plan.deferred, scalars(&[1]));
        assert!(
            plan.deleted.is_empty(),
            "a key the accelerator still holds must not be deleted at the source"
        );
        assert!(
            plan.present.is_empty(),
            "a deleted key must not be upserted back to the source"
        );
        assert!(
            plan.clearable.is_empty(),
            "the marker must survive the pass so the delete is delivered once it is visible"
        );
    }

    /// The deferred key's row is dropped from what the upsert delivers — the
    /// read is filtered by primary key alone, so it returns that row too.
    #[test]
    fn a_deferred_key_is_dropped_from_the_rows_delivered_as_upserts() {
        let deliverable =
            super::retain_deliverable_rows(PK_COL, &read_returning(&[1, 2]), &scalars(&[2]))
                .expect("filtering succeeds");

        let remaining: Vec<i64> = deliverable
            .iter()
            .flat_map(|batch| {
                let column = batch
                    .column_by_name(PK_COL)
                    .expect("the key column is present")
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("an Int64 key column")
                    .clone();
                (0..column.len()).map(move |row| column.value(row))
            })
            .collect();
        assert_eq!(
            remaining,
            vec![1],
            "only the non-deferred key's row may be upserted"
        );
    }

    #[test]
    fn a_present_key_with_an_upsert_marker_is_upserted() {
        let claimed = vec![marker(1, WriteBackOp::Upsert)];
        let plan = classify_delivery(PK_COL, &claimed, &keys(&[1]), &read_returning(&[1]))
            .expect("classification succeeds");

        assert_eq!(plan.present, scalars(&[1]));
        assert!(plan.deleted.is_empty() && plan.absent.is_empty());
    }

    /// One pass mixes every outcome; the upsert and delete sets must stay
    /// disjoint so the two deliveries cannot fight over a key.
    #[test]
    fn a_mixed_batch_splits_into_disjoint_upserts_and_deletes() {
        let claimed = vec![
            marker(1, WriteBackOp::Upsert), // present → upsert
            marker(2, WriteBackOp::Delete), // absent → delete
            marker(3, WriteBackOp::Upsert), // absent → withheld
            marker(4, WriteBackOp::Delete), // present → deferred
        ];
        let plan = classify_delivery(
            PK_COL,
            &claimed,
            &keys(&[1, 2, 3, 4]),
            &read_returning(&[1, 4]),
        )
        .expect("classification succeeds");

        assert_eq!(plan.present, scalars(&[1]));
        assert_eq!(plan.deleted, scalars(&[2]));
        assert_eq!(plan.absent, scalars(&[3]));
        assert_eq!(plan.deferred, scalars(&[4]));
        assert_eq!(
            plan.clearable,
            vec![
                marker(1, WriteBackOp::Upsert),
                marker(2, WriteBackOp::Delete),
            ],
            "only the two markers this pass delivered may be cleared"
        );
        assert_eq!(
            plan.withheld,
            vec![
                marker(3, WriteBackOp::Upsert),
                marker(4, WriteBackOp::Delete),
            ],
            "both undelivered markers are kept and charged an attempt"
        );
        for key in &plan.deleted {
            assert!(
                !plan.present.contains(key),
                "the delete and upsert key sets must be disjoint"
            );
        }
    }

    #[test]
    fn an_empty_read_with_no_delete_markers_delivers_nothing() {
        let claimed = vec![
            marker(1, WriteBackOp::Upsert),
            marker(2, WriteBackOp::Upsert),
        ];
        let plan = classify_delivery(PK_COL, &claimed, &keys(&[1, 2]), &[])
            .expect("classification succeeds");

        assert!(
            plan.present.is_empty() && plan.deleted.is_empty(),
            "a read that returned nothing must not be read as 'every key was deleted'"
        );
        assert_eq!(plan.absent, scalars(&[1, 2]));
        assert!(
            plan.clearable.is_empty(),
            "a read that returned nothing must retire no marker"
        );
    }

    #[test]
    fn markers_and_decoded_keys_that_do_not_line_up_are_rejected() {
        let claimed = vec![marker(1, WriteBackOp::Delete)];
        let err = classify_delivery(PK_COL, &claimed, &keys(&[1, 2]), &read_returning(&[]))
            .expect_err("a length mismatch must not be classified");

        assert!(
            err.to_string().contains("do not line up"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn a_read_missing_the_primary_key_column_is_an_error() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![keys(&[1])]).expect("valid batch");
        let claimed = vec![marker(1, WriteBackOp::Upsert)];

        let err = classify_delivery(PK_COL, &claimed, &keys(&[1]), &[batch])
            .expect_err("a read without the key column must not be classified");

        assert!(
            err.to_string()
                .contains("missing from the accelerator read"),
            "unexpected error: {err}"
        );
    }
}
