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
use cayenne::CayenneTableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, col, lit};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::scalar::ScalarValue;
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
            let absent_filter = col(pk_col).in_list(absent.into_iter().map(lit).collect(), false);
            let delete_plan = federated_provider
                .delete_from(&session_state, vec![absent_filter])
                .await?;
            let _ =
                datafusion::physical_plan::collect(delete_plan, session_state.task_ctx()).await?;
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

/// End-to-end delivery for a `BIGINT` primary key (#13396).
///
/// The classifier tests above are pure; these drive the real chain a committed
/// write travels — transactional commit on a durable-write-back Cayenne table,
/// the markers that commit writes, the worker's decode and point scan, and the
/// upsert that lands at the federated source — and assert the row arrives with
/// the value that was committed.
///
/// A single `Int64` key is the shape that was broken: it runs the converter-free
/// `Int64Pk` deletion strategy, so the provider stores no `pk_row_converter` and
/// every delivery pass failed to decode its own markers. Nothing here needs a
/// container: the federated side is an in-process provider that records what it
/// is handed.
#[cfg(test)]
mod delivery_e2e_tests {
    use super::{CLAIM_BATCH, WriteBackWorker};
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use async_trait::async_trait;
    use cayenne::metadata::CreateTableOptions;
    use cayenne::{
        CayenneCatalog, CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog,
    };
    use datafusion::catalog::Session;
    use datafusion::datasource::{TableProvider, TableType};
    use datafusion::error::Result as DataFusionResult;
    use datafusion::execution::{SendableRecordBatchStream, TaskContext};
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
    use datafusion::physical_plan::metrics::MetricsSet;
    use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_datasource::sink::{DataSink, DataSinkExec};
    use datafusion_table_providers::util::{
        column_reference::ColumnReference, on_conflict::OnConflict,
    };
    use futures::StreamExt;
    use parking_lot::Mutex;
    use std::sync::Arc;

    use crate::federated::FederatedTable;

    /// The federated source, in process: every row the delivery worker upserts is
    /// recorded, so a test can assert exactly what reached it.
    #[derive(Debug)]
    struct RecordingSource {
        schema: SchemaRef,
        delivered: Arc<Mutex<Vec<RecordBatch>>>,
    }

    impl RecordingSource {
        fn new_arc(schema: SchemaRef) -> (Arc<dyn TableProvider>, Arc<Mutex<Vec<RecordBatch>>>) {
            let delivered = Arc::new(Mutex::new(Vec::new()));
            let provider = Arc::new(Self {
                schema,
                delivered: Arc::clone(&delivered),
            }) as Arc<dyn TableProvider>;
            (provider, delivered)
        }
    }

    #[async_trait]
    impl TableProvider for RecordingSource {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
        fn table_type(&self) -> TableType {
            TableType::Base
        }
        fn supports_filters_pushdown(
            &self,
            filters: &[&Expr],
        ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
            Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
        }
        async fn scan(
            &self,
            _state: &dyn Session,
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            Err(datafusion::error::DataFusionError::NotImplemented(
                "the delivery worker never reads the federated source".to_string(),
            ))
        }
        /// Accepts `Replace` directly, which is the native-upsert path delivery
        /// prefers; answering `NotImplemented` here would silently exercise the
        /// delete-then-insert fallback instead of the path under test.
        async fn insert_into(
            &self,
            _state: &dyn Session,
            input: Arc<dyn ExecutionPlan>,
            _insert_op: InsertOp,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            let sink = Arc::new(RecordingSink {
                schema: Arc::clone(&self.schema),
                delivered: Arc::clone(&self.delivered),
            });
            Ok(Arc::new(DataSinkExec::new(input, sink, None)))
        }
    }

    #[derive(Debug)]
    struct RecordingSink {
        schema: SchemaRef,
        delivered: Arc<Mutex<Vec<RecordBatch>>>,
    }

    impl DisplayAs for RecordingSink {
        fn fmt_as(
            &self,
            _t: DisplayFormatType,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            write!(f, "RecordingSink")
        }
    }

    #[async_trait]
    impl DataSink for RecordingSink {
        fn metrics(&self) -> Option<MetricsSet> {
            None
        }
        fn schema(&self) -> &SchemaRef {
            &self.schema
        }
        async fn write_all(
            &self,
            mut data: SendableRecordBatchStream,
            _context: &Arc<TaskContext>,
        ) -> DataFusionResult<u64> {
            let mut rows = 0u64;
            while let Some(batch) = data.next().await {
                let batch = batch?;
                rows += batch.num_rows() as u64;
                self.delivered.lock().push(batch);
            }
            Ok(rows)
        }
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]))
    }

    /// A durable-write-back Cayenne table with a single `BIGINT` primary key.
    async fn bigint_keyed_table(
        dir: &std::path::Path,
        schema: &SchemaRef,
    ) -> (CayenneTableProvider, Arc<CayenneCatalog>) {
        let catalog = Arc::new(
            CayenneCatalog::new(format!("sqlite://{}", dir.join("cayenne.db").display()))
                .expect("catalog"),
        );
        catalog.init().await.expect("catalog init");

        let data_path = dir.join("data");
        std::fs::create_dir_all(&data_path).expect("data dir");

        let ctx = SessionContext::new();
        let options = CreateTableOptions {
            table_name: "bigint_write_back".to_string(),
            schema: Arc::clone(schema),
            primary_key: vec!["id".to_string()],
            on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                "id".to_string(),
            ]))),
            base_path: data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: cayenne::metadata::VortexConfig::default(),
        };
        let catalog_for_builder = Arc::clone(&catalog) as Arc<dyn cayenne::MetadataCatalog>;
        let table = CayenneTableProviderBuilder::new(catalog_for_builder, ctx.runtime_env())
            .with_durable_write_back(true)
            .create(options)
            .await
            .expect("durable-write-back table");
        (table, catalog)
    }

    /// Commit `rows` through a Cayenne transaction, which is the only path that
    /// writes dirty-key markers — a non-transactional insert marks nothing, so a
    /// test that skipped the transaction would have no markers to deliver.
    async fn commit_in_transaction(table: &CayenneTableProvider, batch: RecordBatch) {
        use runtime_request_context::{Protocol, RequestContextBuilder};

        let token = table.transaction_write_token().await;
        let txn = cayenne::CayenneTransaction::new();
        txn.register(
            table.table_id().to_string(),
            token,
            table.clone_for_write_operations(),
        );

        let request_context = Arc::new(RequestContextBuilder::new(Protocol::Internal).build());
        request_context.insert_extension(txn.clone());
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().with_extension(Arc::clone(&request_context)),
        );

        let input = Arc::new(
            datafusion::datasource::MemTable::try_new(batch.schema(), vec![vec![batch]])
                .expect("input table"),
        );
        let plan = table
            .insert_into(
                &ctx.state(),
                input
                    .scan(&ctx.state(), None, &[], None)
                    .await
                    .expect("input scan"),
                InsertOp::Append,
            )
            .await
            .expect("stage the write");
        datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .expect("execute the staged write");

        txn.commit().await.expect("commit the transaction");
    }

    fn delivered_rows(delivered: &Arc<Mutex<Vec<RecordBatch>>>) -> Vec<(i64, String)> {
        let mut rows = Vec::new();
        for batch in delivered.lock().iter() {
            let ids = batch
                .column_by_name("id")
                .expect("delivered id column")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id is Int64");
            let values = batch
                .column_by_name("value")
                .expect("delivered value column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("value is Utf8");
            for row in 0..batch.num_rows() {
                rows.push((ids.value(row), values.value(row).to_string()));
            }
        }
        rows.sort_unstable();
        rows
    }

    /// The regression #13396 describes: on a `BIGINT`-keyed table every delivery
    /// pass failed to decode its own markers, so acknowledged writes never
    /// reached the source. Reverting `decode_pk_keys`' converter fallback makes
    /// this fail with "requires a primary-key `RowConverter`" and nothing is
    /// delivered.
    #[tokio::test]
    async fn a_committed_write_to_a_bigint_keyed_table_reaches_the_federated_source() {
        let dir = tempfile::tempdir().expect("temp dir");
        let schema = test_schema();
        let (table, _catalog) = bigint_keyed_table(dir.path(), &schema).await;

        // Boundary values whose `OwnedRow` encoding is order-preserving rather
        // than a plain big-endian copy, so a decode that dropped the transform
        // cannot pass by accident.
        let ids: Vec<i64> = vec![i64::MIN, -1, 0, 42, i64::MAX];
        let values: Vec<String> = ids.iter().map(|id| format!("row_{id}")).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids.clone())),
                Arc::new(StringArray::from(values.clone())),
            ],
        )
        .expect("batch");
        commit_in_transaction(&table, batch).await;

        let markers = table.list_dirty_keys(CLAIM_BATCH).await.expect("markers");
        assert_eq!(
            markers.len(),
            ids.len(),
            "the commit must mark every key it wrote, or there is nothing to deliver"
        );

        let (federated, delivered) = RecordingSource::new_arc(Arc::clone(&schema));
        let worker = WriteBackWorker {
            pk_columns: table.pk_column_names(),
            provider: Arc::new(table.clone_for_write_operations()),
            federated: Arc::new(FederatedTable::Immediate(federated)),
            dataset_name: "bigint_write_back".to_string(),
        };

        let delivered_count = worker
            .deliver_batch()
            .await
            .expect("delivery pass succeeds");
        assert_eq!(delivered_count, ids.len(), "every marker should be acked");

        let expected: Vec<(i64, String)> = {
            let mut rows: Vec<(i64, String)> = ids.iter().copied().zip(values).collect();
            rows.sort_unstable();
            rows
        };
        assert_eq!(
            delivered_rows(&delivered),
            expected,
            "every committed row must reach the federated source with its committed value"
        );

        assert!(
            table
                .list_dirty_keys(CLAIM_BATCH)
                .await
                .expect("markers after delivery")
                .is_empty(),
            "delivered markers must be cleared, or the next pass redelivers them forever"
        );
    }
}
