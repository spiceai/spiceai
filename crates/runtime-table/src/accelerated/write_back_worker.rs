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
//! 1. **List** a batch of the oldest markers (`list_dirty_keys`) — a plain
//!    read, NOT an atomic reservation; concurrency safety comes from the
//!    compare-and-clear in step 4, not from claiming these markers here.
//!    Each marker carries the [`WriteBackOp`] of the commit that dirtied it.
//! 2. **Read** those keys' *current* committed values from the accelerator
//!    (a fenced point scan), AFTER the list.
//! 3. **Deliver** to the source idempotently, from the marked intent:
//!    - a key the read returned is upserted with that value;
//!    - a key the read did not return, marked [`WriteBackOp::Delete`], is
//!      deleted at the source;
//!    - a key the read did not return, marked [`WriteBackOp::Upsert`], is not
//!      delivered at all, and its marker is retired only after a second
//!      consecutive absence (see [`classify_delivery`]);
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
//! # Absence is not a delete
//!
//! Delivery acts on the operation each marker records, never on whether the
//! accelerator still holds the key. A key can be missing for reasons that are
//! not a deletion — a retention policy pruned it, or the read could not see it —
//! and turning any of those into a source `DELETE` destroys rows nobody deleted.
//! A committed `DELETE` marks its keys explicitly, which is what makes the
//! source delete safe to issue.
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

use std::time::Duration;

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

/// Markers claimed per delivery pass.
const CLAIM_BATCH: usize = 1024;
/// Idle poll interval when the dirty set is empty (not a failure — the error
/// backoff must not grow on empty polls).
const POLL_INTERVAL: Duration = Duration::from_secs(1);

pub(crate) struct WriteBackWorker {
    /// A write-clone of the durable-write-back Cayenne provider — shares the
    /// live table's catalog, listing fence, and keyset, so the marker CRUD and
    /// the point scan observe committed state.
    provider: Arc<CayenneTableProvider>,
    federated: Arc<FederatedTable>,
    /// Primary-key column names, in key order.
    pk_columns: Vec<String>,
    dataset_name: String,
    /// `pk_bytes` of the upsert markers the last pass found absent and held for
    /// one retry (see [`classify_delivery`]). Only ever touched between awaits.
    previously_absent: Mutex<HashSet<Vec<u8>>>,
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
            previously_absent: Mutex::new(HashSet::new()),
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
                Ok(delivered) => {
                    // Success — reset the error backoff.
                    backoff = FibonacciBackoffBuilder::new().max_retries(None).build();
                    if delivered < CLAIM_BATCH {
                        // Dirty set drained (fewer than a full batch remained);
                        // idle-poll for the next commit — NOT an error, so the
                        // backoff stays reset.
                        tokio::time::sleep(POLL_INTERVAL).await;
                    }
                    // Else a full batch was claimed — more may remain; loop
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

    /// One claim → read → deliver → clear pass. Returns the number of markers
    /// delivered (0 when the dirty set is empty).
    async fn deliver_batch(&self) -> DataFusionResult<usize> {
        let claimed = self
            .provider
            .list_dirty_keys(CLAIM_BATCH)
            .await
            .map_err(to_df_err)?;
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
        // Snapshot the keys the last pass held for a retry; the guard is dropped
        // before any await below.
        let previously_absent = self.previously_absent.lock().clone();
        let plan = classify_delivery(pk_col, &claimed, &pk_values, &current, &previously_absent)?;
        *self.previously_absent.lock() = plan.absent_retry_keys.iter().cloned().collect();
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

        if !plan.absent_retry.is_empty() {
            tracing::debug!(
                dataset = %self.dataset_name,
                keys = plan.absent_retry.len(),
                "durable write-back: {} committed key(s) were not readable in the accelerator; holding their markers for one more pass in case the commit is not yet visible",
                plan.absent_retry.len(),
            );
        }

        if !plan.undelivered.is_empty() {
            tracing::warn!(
                dataset = %self.dataset_name,
                keys = plan.undelivered.len(),
                "Dataset '{}': {} committed row(s) are no longer in the accelerator and were not written back, so the source may still hold their previous values. Nothing was deleted at the source: a row can go missing without having been deleted (a retention policy removed it, or the read could not see it), and only a committed DELETE authorizes deleting it at the source. See: https://spiceai.org/docs/reference/spicepod/datasets#acceleration",
                self.dataset_name,
                plan.undelivered.len(),
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

        // Ack: clear only markers still at/below the claimed sequence. Keys that
        // were not delivered are cleared too — their marker describes a commit
        // that has been superseded, and keeping it would grow the dirty set
        // without bound while re-warning every pass. A commit that re-dirtied the
        // key during this pass bumped its sequence above what was claimed, so its
        // marker survives the clear and is delivered next pass. Deferred keys are
        // excluded outright: their delete has not been delivered, so clearing
        // their marker would lose it (see `classify_delivery`).
        self.provider
            .clear_dirty_keys(&plan.clearable)
            .await
            .map_err(to_df_err)?;
        Ok(plan.clearable.len())
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
    /// Keys the read did not return that no committed `DELETE` marked, seen
    /// absent on a previous pass too. Nothing is delivered for these and their
    /// markers are retired — see the module's "Absence is not a delete".
    pub(crate) undelivered: Vec<ScalarValue>,
    /// Keys the read did not return that no committed `DELETE` marked, absent
    /// for the FIRST time. Nothing is delivered and their markers are held for
    /// one more pass, in case the miss was a not-yet-visible commit rather than
    /// a row that is really gone.
    pub(crate) absent_retry: Vec<ScalarValue>,
    /// `pk_bytes` of the `absent_retry` markers, for the next pass to recognize
    /// a second sighting.
    pub(crate) absent_retry_keys: Vec<Vec<u8>>,
    /// Keys marked deleted that the read still returned. Nothing is delivered
    /// for these and their markers must NOT be cleared — see
    /// [`classify_delivery`].
    pub(crate) deferred: Vec<ScalarValue>,
    /// The claimed markers this pass may clear once it has delivered: every
    /// claimed marker except the deferred ones.
    pub(crate) clearable: Vec<PendingWriteBackMarker>,
}

/// Decide what to deliver for each claimed key.
///
/// A key the read returned whose marker records an [`WriteBackOp::Upsert`] is
/// upserted with that value: the read is the authority on the value.
///
/// A key the read did not return is deleted at the source **only** when its
/// marker records a [`WriteBackOp::Delete`]. Absence on its own proves nothing:
/// a retention prune or a read that could not see the row looks identical to a
/// deletion, and deleting on that evidence destroys rows nobody deleted — which
/// is why this returns them as `undelivered` rather than as deletes.
///
/// An absent key whose marker records an [`WriteBackOp::Upsert`] is given one
/// retry before its marker is retired. Retiring it on the first sighting would
/// discard the only durable record of an acknowledged write whenever the miss
/// was the same not-yet-visible-commit window described below, leaving the
/// source on its previous value with nothing left to reconcile it. `Absence` can
/// equally be permanent (a retention prune), so the marker cannot be held
/// forever either — a second consecutive absence retires it, which keeps a
/// pruned key from blocking the queue. `previously_absent` carries the
/// `pk_bytes` the last pass deferred this way.
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
/// Deferring converges in both cases on the next pass, so it cannot grow the
/// dirty set without bound.
///
/// `claimed` and `claimed_pks` are parallel: `claimed_pks[i]` is the decoded key
/// of `claimed[i]`.
pub(crate) fn classify_delivery(
    pk_col: &str,
    claimed: &[PendingWriteBackMarker],
    claimed_pks: &ArrayRef,
    current: &[RecordBatch],
    previously_absent: &HashSet<Vec<u8>>,
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
            (true, WriteBackOp::Delete) => {
                plan.deferred.push(key);
                // Withheld from `clearable`: this marker must survive the pass.
                continue;
            }
            (false, WriteBackOp::Delete) => plan.deleted.push(key),
            (false, WriteBackOp::Upsert) => {
                if previously_absent.contains(&marker.pk_bytes) {
                    // Absent twice running: treat it as really gone and retire
                    // the marker, so a pruned key cannot block the queue.
                    plan.undelivered.push(key);
                } else {
                    plan.absent_retry.push(key);
                    plan.absent_retry_keys.push(marker.pk_bytes.clone());
                    // Withheld from `clearable`: give the write one more pass.
                    continue;
                }
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
    use std::collections::HashSet;
    use std::sync::Arc;

    const PK_COL: &str = "id";

    fn marker(pk: i64, op: WriteBackOp) -> PendingWriteBackMarker {
        PendingWriteBackMarker {
            // The classifier reads the op and relies on the caller's decoded key
            // array for identity, so the exact encoding here is immaterial.
            pk_bytes: pk.to_be_bytes().to_vec(),
            sequence_number: pk,
            op,
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
    /// from the accelerator — a retention prune, a read that could not see it —
    /// carries an upsert marker, and must NEVER be deleted at the source.
    ///
    /// On a second consecutive absence its marker is retired, so a genuinely
    /// pruned key cannot block the queue.
    #[test]
    fn an_absent_key_without_a_delete_marker_is_never_deleted_at_the_source() {
        let claimed = vec![marker(1, WriteBackOp::Upsert)];
        let seen_absent_before: HashSet<Vec<u8>> =
            claimed.iter().map(|m| m.pk_bytes.clone()).collect();
        let plan = classify_delivery(
            PK_COL,
            &claimed,
            &keys(&[1]),
            &read_returning(&[]),
            &seen_absent_before,
        )
        .expect("classification succeeds");

        assert_eq!(
            plan,
            DeliveryPlan {
                present: vec![],
                deleted: vec![],
                undelivered: scalars(&[1]),
                deferred: vec![],
                absent_retry: vec![],
                absent_retry_keys: vec![],
                clearable: claimed,
            },
            "an absent upsert-marked key must be reported as undelivered, never deleted"
        );
    }

    /// An acknowledged write whose row the read could not see gets one more
    /// pass before its marker is retired. Retiring it on the first sighting
    /// would throw away the only durable record of that write whenever the miss
    /// was the not-yet-visible-commit window, leaving the source on its previous
    /// value with nothing left to reconcile it.
    #[test]
    fn an_absent_upsert_key_is_held_for_one_retry_before_its_marker_is_retired() {
        let claimed = vec![marker(1, WriteBackOp::Upsert)];
        let first = classify_delivery(
            PK_COL,
            &claimed,
            &keys(&[1]),
            &read_returning(&[]),
            &HashSet::new(),
        )
        .expect("classification succeeds");

        assert_eq!(first.absent_retry, scalars(&[1]));
        assert!(
            first.undelivered.is_empty(),
            "a first absence must not retire the marker"
        );
        assert!(
            first.clearable.is_empty(),
            "the marker must survive so the write can still be delivered"
        );

        // The next pass, fed what this one deferred, retires it.
        let second = classify_delivery(
            PK_COL,
            &claimed,
            &keys(&[1]),
            &read_returning(&[]),
            &first.absent_retry_keys.iter().cloned().collect(),
        )
        .expect("classification succeeds");

        assert_eq!(second.undelivered, scalars(&[1]));
        assert_eq!(
            second.clearable, claimed,
            "a second consecutive absence retires the marker, so a pruned key cannot block the queue"
        );
    }

    #[test]
    fn an_absent_key_with_a_delete_marker_is_deleted_at_the_source() {
        let claimed = vec![marker(1, WriteBackOp::Delete)];
        let plan = classify_delivery(
            PK_COL,
            &claimed,
            &keys(&[1]),
            &read_returning(&[]),
            &HashSet::new(),
        )
        .expect("classification succeeds");

        assert_eq!(plan.deleted, scalars(&[1]));
        assert!(plan.present.is_empty() && plan.undelivered.is_empty());
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
        let plan = classify_delivery(
            PK_COL,
            &claimed,
            &keys(&[1]),
            &read_returning(&[1]),
            &HashSet::new(),
        )
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
        let plan = classify_delivery(
            PK_COL,
            &claimed,
            &keys(&[1]),
            &read_returning(&[1]),
            &HashSet::new(),
        )
        .expect("classification succeeds");

        assert_eq!(plan.present, scalars(&[1]));
        assert!(plan.deleted.is_empty() && plan.undelivered.is_empty());
    }

    /// One pass mixes every outcome; the upsert and delete sets must stay
    /// disjoint so the two deliveries cannot fight over a key.
    #[test]
    fn a_mixed_batch_splits_into_disjoint_upserts_and_deletes() {
        let claimed = vec![
            marker(1, WriteBackOp::Upsert), // present → upsert
            marker(2, WriteBackOp::Delete), // absent → delete
            marker(3, WriteBackOp::Upsert), // absent (twice) → undelivered
            marker(4, WriteBackOp::Delete), // present → deferred
        ];
        // Key 3 was already absent on the previous pass, so this one retires it.
        let seen_absent_before: HashSet<Vec<u8>> =
            std::iter::once(marker(3, WriteBackOp::Upsert).pk_bytes).collect();
        let plan = classify_delivery(
            PK_COL,
            &claimed,
            &keys(&[1, 2, 3, 4]),
            &read_returning(&[1, 4]),
            &seen_absent_before,
        )
        .expect("classification succeeds");

        assert_eq!(plan.present, scalars(&[1]));
        assert_eq!(plan.deleted, scalars(&[2]));
        assert_eq!(plan.undelivered, scalars(&[3]));
        assert_eq!(plan.deferred, scalars(&[4]));
        assert_eq!(
            plan.clearable,
            vec![
                marker(1, WriteBackOp::Upsert),
                marker(2, WriteBackOp::Delete),
                marker(3, WriteBackOp::Upsert),
            ],
            "every marker except the deferred one may be cleared"
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
        let seen_absent_before: HashSet<Vec<u8>> =
            claimed.iter().map(|m| m.pk_bytes.clone()).collect();
        let plan = classify_delivery(PK_COL, &claimed, &keys(&[1, 2]), &[], &seen_absent_before)
            .expect("classification succeeds");

        assert!(
            plan.present.is_empty() && plan.deleted.is_empty(),
            "a read that returned nothing must not be read as 'every key was deleted'"
        );
        assert_eq!(plan.undelivered, scalars(&[1, 2]));
    }

    #[test]
    fn markers_and_decoded_keys_that_do_not_line_up_are_rejected() {
        let claimed = vec![marker(1, WriteBackOp::Delete)];
        let err = classify_delivery(
            PK_COL,
            &claimed,
            &keys(&[1, 2]),
            &read_returning(&[]),
            &HashSet::new(),
        )
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

        let err = classify_delivery(PK_COL, &claimed, &keys(&[1]), &[batch], &HashSet::new())
            .expect_err("a read without the key column must not be classified");

        assert!(
            err.to_string()
                .contains("missing from the accelerator read"),
            "unexpected error: {err}"
        );
    }
}
