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

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, Weak};
use std::time::{Duration, Instant};

use arrow::array::{Array, ArrayRef};
use arrow::record_batch::RecordBatch;
use cayenne::CayenneTableProvider;
use data_connector_api::write_back::WriteBackDeliverer;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, col, lit};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::scalar::ScalarValue;
use opentelemetry::KeyValue;
use parking_lot::Mutex;
use runtime_metrics::acceleration as metrics;
use tokio::task::JoinHandle;
use util::fibonacci_backoff::FibonacciBackoffBuilder;

use super::write::write_back::execute_insert;
use crate::federated::FederatedTable;

/// Markers claimed per delivery pass.
const CLAIM_BATCH: usize = 1024;
/// Idle poll interval when the dirty set is empty (not a failure — the error
/// backoff must not grow on empty polls).
const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// A generation token for workers that publish the same dataset-labelled gauge.
struct WriteBackGaugeOwner;

/// Live write-back workers in spawn order for each dataset.
///
/// Hot reload starts a successor before aborting its predecessor. Both workers
/// use the same metric labels, so only the newest live generation may publish;
/// otherwise a delayed predecessor teardown can overwrite the successor's
/// backlog with zero. Keeping the predecessor in the stack lets it resume
/// ownership if a candidate replacement fails before installation.
#[derive(Default)]
struct WriteBackGaugeOwners {
    by_dataset: HashMap<String, Vec<Weak<WriteBackGaugeOwner>>>,
}

impl WriteBackGaugeOwners {
    fn register(&mut self, dataset_name: &str, owner: &Arc<WriteBackGaugeOwner>) {
        let owners = self.by_dataset.entry(dataset_name.to_string()).or_default();
        owners.retain(|candidate| candidate.strong_count() > 0);
        owners.push(Arc::downgrade(owner));
    }

    fn is_current(&self, dataset_name: &str, owner: &Arc<WriteBackGaugeOwner>) -> bool {
        self.by_dataset
            .get(dataset_name)
            .and_then(|owners| owners.last())
            .is_some_and(|current| current.as_ptr() == Arc::as_ptr(owner))
    }

    /// Remove `owner`; return whether its teardown should zero the gauge.
    fn unregister(&mut self, dataset_name: &str, owner: &Arc<WriteBackGaugeOwner>) -> bool {
        let owner_ptr = Arc::as_ptr(owner);
        let Some(owners) = self.by_dataset.get_mut(dataset_name) else {
            return false;
        };
        let was_current = owners
            .last()
            .is_some_and(|current| current.as_ptr() == owner_ptr);
        owners.retain(|candidate| candidate.as_ptr() != owner_ptr && candidate.strong_count() > 0);
        let no_owner_remains = owners.is_empty();
        if no_owner_remains {
            self.by_dataset.remove(dataset_name);
        }
        was_current && no_owner_remains
    }
}

static WRITE_BACK_GAUGE_OWNERS: LazyLock<Mutex<WriteBackGaugeOwners>> =
    LazyLock::new(|| Mutex::new(WriteBackGaugeOwners::default()));

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
    /// The backlog gauge's label set, built once.
    dataset_labels: [KeyValue; 1],
    gauge_owner: Arc<WriteBackGaugeOwner>,
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
        let gauge_owner = Arc::new(WriteBackGaugeOwner);
        WRITE_BACK_GAUGE_OWNERS
            .lock()
            .register(&dataset_name, &gauge_owner);
        let worker = Self {
            provider: Arc::new(provider),
            federated,
            deliverer,
            pk_columns,
            dataset_labels: [KeyValue::new("dataset", dataset_name.clone())],
            dataset_name,
            gauge_owner,
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
        let mut last_backlog_sample = None;
        loop {
            match self.deliver_batch().await {
                Ok(0) => {
                    // Nothing was claimed, which proves the dirty set is empty —
                    // publish that without paying for a count.
                    backoff = FibonacciBackoffBuilder::new().max_retries(None).build();
                    self.publish_backlog(0);
                    last_backlog_sample = Some(Instant::now());
                    tokio::time::sleep(POLL_INTERVAL).await;
                }
                Ok(delivered) => {
                    // Success — reset the error backoff.
                    backoff = FibonacciBackoffBuilder::new().max_retries(None).build();
                    if delivered < CLAIM_BATCH {
                        // Dirty set drained (fewer than a full batch remained);
                        // idle-poll for the next commit — NOT an error, so the
                        // backoff stays reset.
                        if self.count_and_publish_backlog().await {
                            last_backlog_sample = Some(Instant::now());
                        }
                        tokio::time::sleep(POLL_INTERVAL).await;
                    } else if last_backlog_sample
                        .is_none_or(|sampled_at| sampled_at.elapsed() >= POLL_INTERVAL)
                    {
                        // A count scans the remaining dirty set, so sample at
                        // most once per poll interval while full batches drain.
                        if self.count_and_publish_backlog().await {
                            last_backlog_sample = Some(Instant::now());
                        }
                    }
                }
                Err(e) => {
                    let delay = backoff.next_duration().unwrap_or(POLL_INTERVAL);
                    tracing::warn!(
                        dataset = %self.dataset_name,
                        error = %e,
                        "durable write-back delivery failed; retrying in {delay:?}"
                    );
                    if self.count_and_publish_backlog().await {
                        last_backlog_sample = Some(Instant::now());
                    }
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// Publish an already-known undelivered-marker backlog for this dataset.
    fn publish_backlog(&self, pending: i64) {
        let owners = WRITE_BACK_GAUGE_OWNERS.lock();
        if !owners.is_current(&self.dataset_name, &self.gauge_owner) {
            return;
        }
        metrics::WRITE_BACK_PENDING_KEYS.record(pending, &self.dataset_labels);
    }

    /// Count this dataset's undelivered markers and publish them.
    ///
    /// A delivery that can never succeed surfaces only as a backlog that never
    /// falls, so the failing pass has to publish it too — that is the state the
    /// gauge exists to make visible.
    async fn count_and_publish_backlog(&self) -> bool {
        match self.provider.dirty_key_count().await {
            Ok(pending) => {
                self.publish_backlog(pending);
                true
            }
            Err(e) => {
                tracing::debug!(
                    dataset = %self.dataset_name,
                    error = %e,
                    "durable write-back: could not read the undelivered-marker backlog for the gauge"
                );
                false
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
        let pk_values = pk_arrays.into_iter().next().ok_or_else(|| {
            DataFusionError::Internal(
                "durable write-back marker decode returned no primary-key column".to_string(),
            )
        })?;
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
            // idempotent, so a failed pass replays the whole thing. Casting each
            // batch to the source schema is the deliverer's own concern (see
            // `WriteBackDeliverer::deliver_upserts`), not the worker's.
            if has_present {
                deliverer
                    .deliver_upserts(current.clone())
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

impl Drop for WriteBackWorker {
    fn drop(&mut self) {
        let mut owners = WRITE_BACK_GAUGE_OWNERS.lock();
        if owners.unregister(&self.dataset_name, &self.gauge_owner) {
            // No live worker owns these labels. Synchronous gauges retain their
            // last value, so clear the removed dataset's stale backlog.
            metrics::WRITE_BACK_PENDING_KEYS.record(0, &self.dataset_labels);
        }
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
    DataFusionError::Context(
        "durable write-back".to_string(),
        Box::new(DataFusionError::Execution(e.to_string())),
    )
}

/// Build a single-column key array from the absent primary keys, for the
/// deliverer's delete leg.
fn keys_to_array(keys: Vec<ScalarValue>) -> DataFusionResult<ArrayRef> {
    ScalarValue::iter_to_array(keys)
}

#[cfg(test)]
mod tests {
    use super::{
        WriteBackGaugeOwner, WriteBackGaugeOwners, absent_claimed_keys, keys_to_array, pk_in_filter,
    };
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

    #[test]
    fn gauge_ownership_returns_to_a_live_predecessor() {
        let mut owners = WriteBackGaugeOwners::default();
        let predecessor = Arc::new(WriteBackGaugeOwner);
        let successor = Arc::new(WriteBackGaugeOwner);

        owners.register("orders", &predecessor);
        owners.register("orders", &successor);
        assert!(!owners.is_current("orders", &predecessor));
        assert!(owners.is_current("orders", &successor));

        assert!(!owners.unregister("orders", &successor));
        assert!(owners.is_current("orders", &predecessor));
        assert!(owners.unregister("orders", &predecessor));
    }

    #[test]
    fn stale_predecessor_teardown_cannot_zero_the_current_gauge() {
        let mut owners = WriteBackGaugeOwners::default();
        let predecessor = Arc::new(WriteBackGaugeOwner);
        let successor = Arc::new(WriteBackGaugeOwner);

        owners.register("orders", &predecessor);
        owners.register("orders", &successor);

        assert!(!owners.unregister("orders", &predecessor));
        assert!(owners.is_current("orders", &successor));
        assert!(owners.unregister("orders", &successor));
    }
}

/// End-to-end coverage of [`WriteBackWorker::deliver_batch`] against a real
/// (local, no external dependency) `CayenneTableProvider` and a fake
/// [`WriteBackDeliverer`] — proving the routing this module's tests above
/// don't reach: present rows select `deliver_upserts`, absent keys select
/// `deliver_deletes`, a delivery error retains the dirty-key markers, and a
/// fully successful pass clears them.
#[cfg(test)]
mod deliverer_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use arrow::array::{ArrayRef, Int32Array, StringArray};
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use cayenne::metadata::{CreateTableOptions, VortexConfig};
    use cayenne::{
        CayenneCatalog, CayenneTableProvider, CayenneTableProviderBuilder, CayenneTransaction,
        MetadataCatalog,
    };
    use data_connector_api::write_back::{DeliveryError, DeliveryResult};
    use datafusion::datasource::{MemTable, TableProvider};
    use datafusion::execution::SendableRecordBatchStream;
    use datafusion::logical_expr::{col, lit};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::prelude::SessionContext;
    use datafusion_table_providers::util::column_reference::ColumnReference;
    use datafusion_table_providers::util::on_conflict::OnConflict;
    use opentelemetry::KeyValue;
    use parking_lot::Mutex;

    use super::{CLAIM_BATCH, WriteBackDeliverer, WriteBackGaugeOwner, WriteBackWorker};
    use crate::federated::FederatedTable;

    /// Records every call it receives; `arm_failure` makes the next call
    /// (upsert or delete, whichever comes first) return an error instead.
    #[derive(Default)]
    struct FakeDeliverer {
        upserts: Mutex<Vec<Vec<RecordBatch>>>,
        deletes: Mutex<Vec<(ArrayRef, String)>>,
        fail_next: AtomicBool,
    }

    impl FakeDeliverer {
        fn arc() -> Arc<Self> {
            Arc::new(Self::default())
        }

        fn arm_failure(&self) {
            self.fail_next.store(true, Ordering::SeqCst);
        }

        fn maybe_fail(&self) -> DeliveryResult<()> {
            if self.fail_next.swap(false, Ordering::SeqCst) {
                return Err(DeliveryError::Delivery {
                    message: "injected delivery failure".to_string(),
                    source: Box::new(std::io::Error::other("injected")),
                });
            }
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl WriteBackDeliverer for FakeDeliverer {
        async fn deliver_upserts(&self, rows: Vec<RecordBatch>) -> DeliveryResult {
            self.maybe_fail()?;
            self.upserts.lock().push(rows);
            Ok(())
        }

        async fn deliver_deletes(&self, keys: ArrayRef, pk_column: &str) -> DeliveryResult {
            self.maybe_fail()?;
            self.deletes.lock().push((keys, pk_column.to_string()));
            Ok(())
        }
    }

    fn orders_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    /// A durable-write-back-enabled `CayenneTableProvider` over a fresh tempdir
    /// (sqlite metadata catalog + local Vortex data path) — no external
    /// dependency, following the recipe in `cayenne::provider::sink` tests.
    async fn new_provider(temp_dir: &tempfile::TempDir) -> CayenneTableProvider {
        let ctx = SessionContext::new();
        let metadata_dir = format!("{}/metadata", temp_dir.path().to_str().expect("path"));
        let data_dir = format!("{}/data", temp_dir.path().to_str().expect("path"));
        std::fs::create_dir_all(&metadata_dir).expect("metadata dir");
        let catalog = Arc::new(
            CayenneCatalog::new(format!("sqlite://{metadata_dir}/cayenne.db")).expect("catalog"),
        ) as Arc<dyn MetadataCatalog>;
        catalog.init().await.expect("catalog init");

        let options = CreateTableOptions {
            table_name: "orders".to_string(),
            schema: orders_schema(),
            primary_key: vec!["id".to_string()],
            on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                "id".to_string(),
            ]))),
            base_path: data_dir,
            partition_column: None,
            vortex_config: VortexConfig::default(),
        };
        CayenneTableProviderBuilder::new(catalog, ctx.runtime_env())
            .with_durable_write_back(true)
            .create(options)
            .await
            .expect("table created")
    }

    /// Commit one row through a Cayenne transaction.
    ///
    /// A transaction commit is the only writer of `cayenne_pending_write_back`
    /// markers, so it is the only write the delivery worker can observe. A bare
    /// `insert_into` marks nothing, leaving every test below with an empty
    /// dirty set.
    async fn insert_row(provider: &CayenneTableProvider, id: i32, name: &str) {
        let batch = RecordBatch::try_new(
            orders_schema(),
            vec![
                Arc::new(Int32Array::from(vec![id])),
                Arc::new(StringArray::from(vec![Some(name)])),
            ],
        )
        .expect("batch builds");
        let stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            batch.schema(),
            futures::stream::iter([Ok(batch)]),
        ));

        let table_id = provider.table_id().to_string();
        let txn = CayenneTransaction::new();
        txn.register(
            table_id.clone(),
            provider.transaction_write_token().await,
            provider.clone_for_write_operations(),
        );
        let token = txn
            .take_token(&table_id)
            .expect("the registered participant's write token");
        let staged = provider
            .begin_staged_upsert_occ(token, stream, 1)
            .await
            .expect("stage the upsert");
        txn.set_staged(&table_id, staged);
        txn.commit().await.expect("commit the transaction");
    }

    /// Physically remove a row WITHOUT marking it.
    ///
    /// The counterpart to `insert_row`: the delete path records no write-back
    /// marker, so the absent-key test keeps the marker its insert produced
    /// while the row itself is gone — the "claimed key the post-claim read
    /// cannot find" case. Routing this through `insert_row` would destroy it.
    async fn delete_row(provider: &CayenneTableProvider, id: i32) {
        let ctx = SessionContext::new();
        let plan = provider
            .delete_from(&ctx.state(), vec![col("id").eq(lit(id))])
            .await
            .expect("delete plan");
        datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .expect("delete executes");
    }

    fn make_worker(
        provider: CayenneTableProvider,
        deliverer: Option<Arc<dyn WriteBackDeliverer>>,
    ) -> WriteBackWorker {
        let pk_columns = provider.pk_column_names();
        // No test below reaches the `TableProvider` fallback path (every case
        // sets a deliverer), so an empty `MemTable` is a valid placeholder for
        // the field `WriteBackWorker` still requires.
        let federated = Arc::new(FederatedTable::new_unchecked(Arc::new(
            MemTable::try_new(orders_schema(), vec![vec![]]).expect("mem table"),
        )));
        let dataset_name = "orders".to_string();
        // These tests drive `deliver_batch` directly and never `run`, so nothing
        // here publishes the backlog gauge. The owner is left unregistered
        // rather than mutating the process-wide owner stack from a test; `Drop`
        // treats an unregistered owner as owning nothing.
        WriteBackWorker {
            provider: Arc::new(provider),
            federated,
            deliverer,
            pk_columns,
            dataset_labels: [KeyValue::new("dataset", dataset_name.clone())],
            dataset_name,
            gauge_owner: Arc::new(WriteBackGaugeOwner),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn present_rows_deliver_via_upsert_and_clear_their_markers() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let provider = new_provider(&temp_dir).await;
        insert_row(&provider, 1, "widget").await;

        let deliverer = FakeDeliverer::arc();
        let worker = make_worker(
            provider,
            Some(Arc::clone(&deliverer) as Arc<dyn WriteBackDeliverer>),
        );

        let claimed = worker.deliver_batch().await.expect("pass succeeds");
        assert_eq!(claimed, 1, "the one dirty marker was claimed");
        assert_eq!(
            deliverer.upserts.lock().len(),
            1,
            "the present row was delivered via deliver_upserts"
        );
        assert!(
            deliverer.deletes.lock().is_empty(),
            "no keys were absent, so deliver_deletes is never called"
        );

        let remaining = worker
            .provider
            .list_dirty_keys(CLAIM_BATCH)
            .await
            .expect("list after delivery");
        assert!(remaining.is_empty(), "the marker was cleared on success");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn absent_keys_deliver_via_delete() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let provider = new_provider(&temp_dir).await;
        insert_row(&provider, 2, "gadget").await;
        // Physically remove the row before the delivery pass claims its
        // marker: the post-claim read then finds it absent.
        delete_row(&provider, 2).await;

        let deliverer = FakeDeliverer::arc();
        let worker = make_worker(
            provider,
            Some(Arc::clone(&deliverer) as Arc<dyn WriteBackDeliverer>),
        );

        let claimed = worker.deliver_batch().await.expect("pass succeeds");
        assert_eq!(claimed, 1, "the one dirty marker was claimed");
        assert!(
            deliverer.upserts.lock().is_empty(),
            "no keys are present, so deliver_upserts is never called"
        );
        assert_eq!(
            deliverer.deletes.lock().len(),
            1,
            "the absent key was delivered via deliver_deletes"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_delivery_error_retains_the_dirty_key_marker() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let provider = new_provider(&temp_dir).await;
        insert_row(&provider, 3, "thing").await;

        let deliverer = FakeDeliverer::arc();
        deliverer.arm_failure();
        let worker = make_worker(
            provider,
            Some(Arc::clone(&deliverer) as Arc<dyn WriteBackDeliverer>),
        );

        worker
            .deliver_batch()
            .await
            .expect_err("an armed delivery failure surfaces as an error");

        let remaining = worker
            .provider
            .list_dirty_keys(CLAIM_BATCH)
            .await
            .expect("list after the failed pass");
        assert_eq!(
            remaining.len(),
            1,
            "the marker is retained so the pass can be replayed"
        );
    }
}
