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
//! 2. **Read** those keys' *current* committed values from the accelerator
//!    (a fenced point scan), AFTER the list.
//! 3. **Deliver** to the source idempotently. Partition keys by delete-only
//!    vs upsert, processes separately. If the source cannot do a native upsert
//!    (it answers `Replace` with `NotImplemented`), delivery falls back to the
//!    older delete-then-insert emulation over all claimed keys - a temporary path
//!    that reopens the #11915 window, kept only until every durable-write-back
//!    source supports native upsert.
//! 4. **Compare-and-clear** the markers whose stored sequence is still at or
//!    below the sequence listed in step 1 — a newer commit that bumped a marker
//!    during delivery leaves it in place, so the stale delivery never clears a
//!    fresh mark.
//!
//! Delivery failure never blocks accelerator commits; the dirty set simply
//! grows until the next successful pass. Marking happens only in the
//! commit-publish transaction (never in the CDC apply path), so an echo of our
//! own write cannot spawn a fresh delivery.
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

use arrow::array::{Array, ArrayRef};
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use cayenne::CayenneTableProvider;
use data_connector_api::write_back::WriteBackDeliverer;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::SessionState;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, col, lit};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::scalar::ScalarValue;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_datasource::source::DataSourceExec;
use runtime_datafusion::execution_plan::schema_cast::SchemaCastScanExec;
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
    /// Connector-owned delivery, when the source provides it (`PostgreSQL`, so it
    /// can stamp each delivery transaction's id for the CDC echo filter). `None`
    /// keeps the `TableProvider` delivery path below, byte-identical.
    deliverer: Option<Arc<dyn WriteBackDeliverer>>,
    /// Primary-key column names, in key order.
    pk_columns: Vec<String>,
    dataset_name: String,
}

impl WriteBackWorker {
    /// Spawn the delivery loop; the returned handle is pushed onto the
    /// accelerated table's `handlers` and aborted when the table drops.
    pub(crate) fn spawn(
        provider: CayenneTableProvider,
        federated: Arc<FederatedTable>,
        dataset_name: String,
        deliverer: Option<Arc<dyn WriteBackDeliverer>>,
    ) -> JoinHandle<()> {
        let pk_columns = provider.pk_column_names();
        let worker = Self {
            provider: Arc::new(provider),
            federated,
            deliverer,
            pk_columns,
            dataset_name,
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

        let pk_bytes: Vec<Vec<u8>> = claimed.iter().map(|(bytes, _)| bytes.clone()).collect();
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

        // Split the claimed keys by whether the post-claim read still returned
        // them. Present and absent key sets are disjoint, so the upsert and the
        // delete below touch disjoint source rows.
        let pk_col = self.pk_columns[0].as_str();
        let absent = absent_claimed_keys(pk_col, &pk_values, &current)?;
        let has_present = current.iter().any(|batch| batch.num_rows() > 0);

        if let Some(deliverer) = &self.deliverer {
            // Connector-owned delivery (Postgres): the connector owns each
            // delivery transaction so it can stamp the transaction id for the CDC
            // echo filter. The upsert leg covers all present rows in one source
            // transaction; the absent keys delete in their own. Both are
            // idempotent, so a failed pass replays the whole thing.
            if has_present {
                // Match the `TableProvider` path's cast: the fallback below wraps
                // the batches in `SchemaCastScanExec` to the source schema before
                // insert, so the deliverer receives equivalently-cast rows. The
                // deliverer reports the source schema so the worker need not
                // resolve the federated `TableProvider`'s schema itself.
                let cast = cast_batches_to(
                    deliverer.target_schema(),
                    self.provider.table_schema(),
                    current.clone(),
                    &session_state,
                )
                .await?;
                deliverer
                    .deliver_upserts(cast)
                    .await
                    .map_err(delivery_to_df_err)?;
            }
            if !absent.is_empty() {
                let keys = keys_to_array(absent)?;
                deliverer
                    .deliver_deletes(keys, pk_col)
                    .await
                    .map_err(delivery_to_df_err)?;
            }
        } else {
            let federated_provider = self.federated.table_provider().await;

            // Attempt a Upsert. If federated source does not support it `DataFusionError::NotImplemented`
            // Fallback to delete and append.
            let mut fallback_delivered = false;
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
                        let _ = datafusion::physical_plan::collect(
                            federated_provider
                                .delete_from(&session_state, vec![filter])
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
                        // The blanket delete above already removed the absent keys, so
                        // skip the absent-only delete below.
                        fallback_delivered = true;
                    }
                    Err(e) => return Err(e),
                }
            }

            // Absent keys → delete. Genuinely gone from the accelerator (the read did
            // not return them), so this delete is correct rather than a blanket first
            // step. Skipped when the fallback above already deleted every claimed key.
            if !fallback_delivered && !absent.is_empty() {
                let absent_filter =
                    col(pk_col).in_list(absent.into_iter().map(lit).collect(), false);
                let delete_plan = federated_provider
                    .delete_from(&session_state, vec![absent_filter])
                    .await?;
                let _ = datafusion::physical_plan::collect(delete_plan, session_state.task_ctx())
                    .await?;
            }
        }

        // Ack: clear only markers still at/below the claimed sequence.
        self.provider
            .clear_dirty_keys(&claimed)
            .await
            .map_err(to_df_err)?;
        Ok(claimed.len())
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

/// The claimed primary keys that the post-claim accelerator read did NOT return
/// — the keys that are genuinely gone and must be deleted from the source.
///
/// `claimed_pks` are all the keys listed this pass; `current` holds the rows the
/// read returned for the keys still present. Absent = claimed − present, so the
/// caller can upsert `current` and delete only the absent keys, never issuing a
/// delete for a key that still exists (the spurious delete #11915 depended on).
fn absent_claimed_keys(
    pk_col: &str,
    claimed_pks: &ArrayRef,
    current: &[RecordBatch],
) -> DataFusionResult<Vec<ScalarValue>> {
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

    let mut absent: Vec<ScalarValue> = Vec::new();
    for row in 0..claimed_pks.len() {
        let key = ScalarValue::try_from_array(claimed_pks.as_ref(), row)?;
        if !present.contains(&key) {
            absent.push(key);
        }
    }
    Ok(absent)
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "passed to `Result::map_err`, which moves the error value in"
)]
fn to_df_err(e: cayenne::provider::Error) -> DataFusionError {
    DataFusionError::Execution(format!("durable write-back: {e}"))
}

/// Map a connector-owned delivery failure into the worker's error type. The
/// worker only needs the message for its log and its retry backoff — a failed
/// pass replays whole, so the variant is immaterial.
#[expect(
    clippy::needless_pass_by_value,
    reason = "passed to `Result::map_err`, which moves the error value in"
)]
fn delivery_to_df_err(e: data_connector_api::write_back::DeliveryError) -> DataFusionError {
    DataFusionError::Execution(format!("durable write-back: {e}"))
}

/// Cast `batches` from `input_schema` to `target_schema`, collecting the result.
///
/// Mirrors [`execute_insert`]'s cast (`SchemaCastScanExec` to the source
/// provider's schema) so the connector-owned deliverer receives rows shaped
/// exactly as the `TableProvider` path would have inserted them — a type or
/// column difference between the accelerator and the source is reconciled here,
/// not left to fail mid-delivery.
async fn cast_batches_to(
    target_schema: SchemaRef,
    input_schema: SchemaRef,
    batches: Vec<RecordBatch>,
    session_state: &SessionState,
) -> DataFusionResult<Vec<RecordBatch>> {
    let memory_source = MemorySourceConfig::try_new(&[batches], input_schema, None)?;
    let source: Arc<dyn datafusion::physical_plan::ExecutionPlan> =
        Arc::new(DataSourceExec::new(Arc::new(memory_source)));
    let input: Arc<dyn datafusion::physical_plan::ExecutionPlan> =
        Arc::new(SchemaCastScanExec::new(source, target_schema));
    datafusion::physical_plan::collect(input, session_state.task_ctx()).await
}

/// Build a single-column key array from the absent primary keys, for the
/// deliverer's delete leg.
fn keys_to_array(keys: Vec<ScalarValue>) -> DataFusionResult<ArrayRef> {
    ScalarValue::iter_to_array(keys)
}

#[cfg(test)]
mod tests {
    use super::{absent_claimed_keys, keys_to_array, pk_in_filter};
    use arrow::array::{Int32Array, StringArray};
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::scalar::ScalarValue;
    use std::sync::Arc;

    fn id_batch(ids: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids))])
            .expect("id batch builds")
    }

    /// Absent = claimed − present: keys the post-claim read did not return.
    #[test]
    fn absent_is_claimed_minus_present() {
        let claimed: arrow::array::ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
        // The read returned rows for 1 and 3 only.
        let present = vec![id_batch(vec![1, 3])];

        let absent = absent_claimed_keys("id", &claimed, &present).expect("partitions");
        assert_eq!(
            absent,
            vec![ScalarValue::Int32(Some(2)), ScalarValue::Int32(Some(4))],
            "only the keys absent from the read are returned, in claim order"
        );
    }

    /// Every claimed key still present → nothing to delete.
    #[test]
    fn no_absent_keys_when_all_present() {
        let claimed: arrow::array::ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let present = vec![id_batch(vec![1, 2])];
        let absent = absent_claimed_keys("id", &claimed, &present).expect("partitions");
        assert!(
            absent.is_empty(),
            "no key is absent when all were read back"
        );
    }

    /// An empty read → every claimed key is absent (all deleted at the source).
    #[test]
    fn all_absent_when_read_is_empty() {
        let claimed: arrow::array::ArrayRef = Arc::new(Int32Array::from(vec![5, 6, 7]));
        let present: Vec<RecordBatch> = vec![];
        let absent = absent_claimed_keys("id", &claimed, &present).expect("partitions");
        assert_eq!(absent.len(), 3, "all claimed keys are absent");
    }

    /// A missing primary-key column in the read is an error, not a silent
    /// misclassification of every key as absent.
    #[test]
    fn missing_pk_column_in_read_is_an_error() {
        let claimed: arrow::array::ArrayRef = Arc::new(Int32Array::from(vec![1]));
        let schema = Arc::new(Schema::new(vec![Field::new("other", DataType::Utf8, true)]));
        let present = vec![
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![Some("x")]))])
                .expect("batch"),
        ];
        absent_claimed_keys("id", &claimed, &present)
            .expect_err("a missing pk column must error rather than delete every key");
    }

    /// The absent keys round-trip back into an array for the deliverer's delete
    /// leg, preserving order and count.
    #[test]
    fn keys_to_array_round_trips_absent_scalars() {
        let keys = vec![ScalarValue::Int32(Some(2)), ScalarValue::Int32(Some(4))];
        let array = keys_to_array(keys).expect("array builds");
        assert_eq!(array.len(), 2);
        let ints = array
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 array");
        assert_eq!(ints.value(0), 2);
        assert_eq!(ints.value(1), 4);
    }

    /// The delete filter is `pk IN (keys…)`, the same shape the fallback delete
    /// builds.
    #[test]
    fn pk_in_filter_lists_the_keys() {
        let values: arrow::array::ArrayRef = Arc::new(Int32Array::from(vec![8, 9]));
        let expr = pk_in_filter("id", &values).expect("filter builds");
        let rendered = format!("{expr}");
        assert!(rendered.contains("id"), "names the pk column: {rendered}");
        assert!(
            rendered.contains('8') && rendered.contains('9'),
            "lists the keys: {rendered}"
        );
    }
}
