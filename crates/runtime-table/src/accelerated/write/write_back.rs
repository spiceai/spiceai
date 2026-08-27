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

//! Write-back execution path for [`WriteMode::WriteBack`].
//!
//! A write-back dataset accepts writes only inside a transaction, and only
//! `INSERT`/`UPDATE`: the transaction stages to the accelerator, publishes
//! atomically at `COMMIT`, and writes the dirty-key markers in that same commit,
//! which is the only record the delivery worker
//! ([`write_back_worker`](crate::accelerated::write_back_worker)) can reconcile
//! to the federated source. The caller returns once the accelerator commit
//! completes; the source may lag briefly behind it.
//!
//! Writes outside a transaction, and `DELETE` in any form, are refused: neither
//! can be recorded for delivery. Outside a transaction nothing writes a marker,
//! so a write would reach the accelerator with nothing able to carry it to the
//! source or even report that it had not arrived; `DELETE` has no
//! transaction-aware sink, so a deletion cannot be recorded at all.
//!
//! Implemented as a [`DataSink`] so that the write only occurs when the returned
//! [`ExecutionPlan`] is executed, not merely planned: if the caller cancels
//! before execution, the accelerator is not modified.
//!
//! [`WriteMode::WriteBack`]: super::WriteMode::WriteBack

use std::sync::Arc;

use arrow::array::UInt64Array;
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use data_components::delete::{DeletionExec, DeletionSink};
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_datasource::sink::{DataSink, DataSinkExec};
use datafusion_datasource::source::DataSourceExec;
use futures::StreamExt;
use runtime_datafusion::execution_plan::schema_cast::SchemaCastScanExec;

use runtime_datafusion::extension::request_context::resolve_request_context;

use crate::accelerated::refresh::Refresher;

/// Whether a Cayenne transaction is active on the live execution context.
///
/// A write-back dataset only accepts writes that have one. The transaction is
/// what makes the write durable end to end: it stages to the accelerator,
/// publishes atomically at `COMMIT`, and writes the dirty-key markers in that
/// same commit — the only record the delivery worker can reconcile to the
/// federated source. Without one the write would land on the accelerator with
/// nothing recording that the source still owes it, so it is refused rather
/// than accepted under a durability guarantee that would not hold.
fn request_in_transaction(context: &TaskContext) -> bool {
    resolve_request_context(context, false)
        .is_some_and(|rc| rc.extension::<cayenne::CayenneTransaction>().is_some())
}

/// The error a write-back dataset returns for a write that is not in a
/// transaction.
fn write_requires_transaction(dataset_name: &str, statement: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "Failed to {statement} dataset '{dataset_name}': a dataset with 'acceleration.write_mode: write_back' accepts writes only inside a transaction, because only a transaction records the write durably for delivery to the federated source. Send the statement as one 'BEGIN; ...; COMMIT;' body. See: https://spiceai.org/docs/reference/spicepod/datasets#acceleration"
    ))
}

pub(crate) fn validate_insert_op(insert_op: InsertOp) -> DataFusionResult<()> {
    match insert_op {
        InsertOp::Append => Ok(()),
        InsertOp::Overwrite | InsertOp::Replace => Err(DataFusionError::Plan(
            "Write-back accelerated tables currently support append writes only".to_string(),
        )),
    }
}

/// Creates a `DataSinkExec` plan for write-back inserts.
pub(crate) fn insert_write_back(
    state: &dyn Session,
    input: Arc<dyn ExecutionPlan>,
    overwrite: InsertOp,
    accelerator: Arc<dyn TableProvider>,
    refresher: Arc<Refresher>,
    schema: SchemaRef,
    dataset_name: &str,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    let session_state = state
        .as_any()
        .downcast_ref::<SessionState>()
        .ok_or_else(|| {
            DataFusionError::Internal(
                "Session is not a SessionState in insert_write_back".to_string(),
            )
        })?
        .clone();
    let sink = Arc::new(WriteBackDataSink {
        dataset_name: dataset_name.to_string(),
        accelerator,
        refresher,
        overwrite,
        schema,
        session_state,
    });
    Ok(Arc::new(DataSinkExec::new(input, sink, None)))
}

struct WriteBackDataSink {
    dataset_name: String,
    accelerator: Arc<dyn TableProvider>,
    refresher: Arc<Refresher>,
    overwrite: InsertOp,
    schema: SchemaRef,
    session_state: SessionState,
}

impl std::fmt::Debug for WriteBackDataSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteBackDataSink").finish_non_exhaustive()
    }
}

impl DisplayAs for WriteBackDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WriteBackDataSink")
    }
}

#[async_trait]
impl DataSink for WriteBackDataSink {
    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> DataFusionResult<u64> {
        // Refuse before touching the accelerator: a write with no transaction
        // cannot be recorded for delivery, so accepting it would diverge the two
        // sides on the first failed background push (see
        // `request_in_transaction`).
        if !request_in_transaction(context) {
            return Err(write_requires_transaction(&self.dataset_name, "write to"));
        }

        // Drain the input stream, counting as it goes: the caller is owed the
        // affected-row count, and the accelerator write is planned over the
        // batches rather than the stream.
        let input_schema = data.schema();
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut row_count: u64 = 0;
        while let Some(batch_result) = data.next().await {
            let batch = batch_result?;
            row_count = row_count.saturating_add(batch.num_rows() as u64);
            batches.push(batch);
        }

        // Write to the accelerator synchronously using the caller's task
        // context so session configuration/runtime env (object store,
        // extensions, limits) is preserved. The caller blocks until this
        // completes, matching the "write reaches local storage before the
        // response is returned" contract of write-back caching.
        execute_insert(
            Arc::clone(&self.accelerator),
            input_schema,
            batches,
            self.overwrite,
            &self.session_state,
            Some(Arc::clone(context)),
        )
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "Write-back: failed to persist write to accelerator: {e}"
            ))
        })?;

        self.refresher.set_initial_load_completed(true);

        // The delivery worker reconciles this write to the federated source from
        // the dirty-key markers the commit wrote. There is no second path: a
        // fire-and-forget push here would duplicate the worker's delivery and
        // could land out of order with it.
        Ok(row_count)
    }
}

/// Refuses a `DELETE` on a write-back accelerated table.
///
/// A write-back dataset delivers each committed row to its federated source from
/// the durable dirty-key markers a transactional commit writes, and a transaction
/// accepts only `INSERT`/`UPDATE` — `DELETE` has no transaction-aware sink, so
/// there is no way to record a deletion for delivery. Applying one to the
/// accelerator alone would diverge it from the source silently, which is what
/// #13398 was about; guessing at delivery time from a key later being unreadable
/// is what deleted rows nobody deleted.
///
/// Deleting at the source is not a safe workaround while write-back is enabled:
/// a committed write for the same key may still be undelivered, and this
/// dataset's delivery upserts unconditionally, so it would put the row back
/// after the source-side delete. The message therefore describes the transition
/// — take the dataset out of write-back and let its backlog drain first — rather
/// than suggesting the delete alone is safe.
pub(crate) fn delete_not_supported(dataset_name: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "Failed to delete from dataset '{dataset_name}': DELETE is not supported while 'acceleration.write_mode: write_back' is enabled, because a delete cannot be recorded for delivery to the federated source. To delete these rows at the source, first take this dataset out of write-back and wait for its `dataset_acceleration_write_back_pending_keys` metric to reach zero, so no committed write is still waiting to be delivered; then delete at the source and let the change stream refresh the accelerator. See: https://spiceai.org/docs/reference/spicepod/datasets#acceleration"
    ))
}

/// Creates a `DeletionExec` plan for write-back updates.
///
/// The update stages to the accelerator inside the caller's transaction and is
/// carried to the federated source by the delivery worker, from the markers that
/// transaction's commit writes. An update outside a transaction is refused.
pub(crate) async fn update_write_back(
    state: &dyn Session,
    assignments: Vec<(String, Expr)>,
    filters: Vec<Expr>,
    accelerator: Arc<dyn TableProvider>,
    dataset_name: &str,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    let accelerator_plan = accelerator.update(state, assignments, filters).await?;
    Ok(Arc::new(DeletionExec::new(Arc::new(WriteBackUpdateSink {
        accelerator_plan,
        dataset_name: dataset_name.to_string(),
    }))))
}

struct WriteBackUpdateSink {
    accelerator_plan: Arc<dyn ExecutionPlan>,
    dataset_name: String,
}

#[async_trait]
impl DeletionSink for WriteBackUpdateSink {
    async fn delete_from(
        &self,
        context: Arc<TaskContext>,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        // Refuse before touching the accelerator, for the same reason a write
        // does: with no transaction there is nothing to record the update for
        // delivery, and the two sides would diverge on the first failed push.
        if !request_in_transaction(&context) {
            return Err(Box::new(write_requires_transaction(
                &self.dataset_name,
                "update",
            )));
        }

        // Execute under the LIVE execution context so the update STAGES to the
        // accelerator and is published at COMMIT. The delivery worker reconciles
        // it to the source from the dirty-key markers that commit wrote — there is
        // no second path.
        let batches = datafusion::physical_plan::collect(
            Arc::clone(&self.accelerator_plan),
            Arc::clone(&context),
        )
        .await?;
        Ok(extract_dml_count(&batches))
    }
}

/// Extracts the affected-row count from a DML result batch (delete or update output).
pub(super) fn extract_dml_count(batches: &[RecordBatch]) -> u64 {
    reported_dml_count(batches).unwrap_or(0)
}

/// The affected-row count a DML plan reported, or `None` when it reported none.
/// A caller that has to distinguish "wrote nothing" from "said nothing" — the
/// delivery worker, deciding whether it may retire a marker — needs the
/// difference that [`extract_dml_count`]'s zero collapses.
fn reported_dml_count(batches: &[RecordBatch]) -> Option<u64> {
    batches
        .iter()
        .flat_map(RecordBatch::columns)
        .find_map(|arr| {
            arr.as_any()
                .downcast_ref::<UInt64Array>()
                .and_then(|a| a.values().first().copied())
        })
}

/// Plan and run an insert of `batches` into `table`, casting them onto the
/// target's schema first so a difference between the accelerator's and the
/// source's schemas cannot write the wrong bytes.
///
/// Returns the affected-row count the plan reported, or `None` when it reported
/// none — a distinction the delivery worker depends on, since only a count it
/// actually saw can tell it the source took fewer rows than it was sent.
pub(crate) async fn execute_insert(
    table: Arc<dyn TableProvider>,
    input_schema: SchemaRef,
    batches: Vec<RecordBatch>,
    overwrite: InsertOp,
    session_state: &SessionState,
    task_context: Option<Arc<TaskContext>>,
) -> DataFusionResult<Option<u64>> {
    let memory_source = MemorySourceConfig::try_new(&[batches], input_schema, None)?;
    let source: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(memory_source)));
    let input: Arc<dyn ExecutionPlan> = Arc::new(SchemaCastScanExec::new(source, table.schema()));

    let plan = table.insert_into(session_state, input, overwrite).await?;
    let task_ctx = task_context.unwrap_or_else(|| session_state.task_ctx());
    let batches = datafusion::physical_plan::collect(plan, task_ctx).await?;
    Ok(reported_dml_count(&batches))
}

#[cfg(test)]
mod tests {
    use super::super::count_exec;
    use super::{WriteBackUpdateSink, extract_dml_count};
    use arrow::array::{StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use data_components::delete::DeletionSink;
    use datafusion::execution::TaskContext;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    fn task_ctx_in_transaction() -> Arc<TaskContext> {
        use datafusion::prelude::SessionConfig;
        use runtime_request_context::{Protocol, RequestContextBuilder};

        let request_context = Arc::new(RequestContextBuilder::new(Protocol::Internal).build());
        request_context.insert_extension(cayenne::CayenneTransaction::new());
        let config = SessionConfig::new().with_extension(request_context);
        Arc::new(TaskContext::default().with_session_config(config))
    }

    // ── extract_dml_count ────────────────────────────────────────────────

    #[test]
    fn extract_dml_count_empty_slice_returns_zero() {
        assert_eq!(extract_dml_count(&[]), 0);
    }

    #[test]
    fn extract_dml_count_single_batch_returns_value() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(UInt64Array::from(vec![99]))],
        )
        .expect("valid schema and array");
        assert_eq!(extract_dml_count(&[batch]), 99);
    }

    #[test]
    fn extract_dml_count_non_uint64_column_returns_zero() {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec!["hello"]))],
        )
        .expect("valid schema and array");
        assert_eq!(extract_dml_count(&[batch]), 0);
    }

    #[test]
    fn extract_dml_count_empty_uint64_array_returns_zero() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(UInt64Array::from(vec![] as Vec<u64>))],
        )
        .expect("valid schema and array");
        assert_eq!(extract_dml_count(&[batch]), 0);
    }

    // ── Write contract ───────────────────────────────────────────────────

    /// A write-back dataset accepts writes only inside a transaction: only a
    /// transaction records the write durably for delivery to the federated
    /// source. Outside one, the write is refused before it touches the
    /// accelerator, rather than landing locally and being pushed once in the
    /// background with nothing recording whether it arrived.
    #[tokio::test]
    async fn an_update_outside_a_transaction_is_refused() {
        let sink = WriteBackUpdateSink {
            accelerator_plan: count_exec(1),
            dataset_name: "orders".to_string(),
        };

        let error = sink
            .delete_from(SessionContext::new().task_ctx())
            .await
            .expect_err("a write-back update outside a transaction must be refused");
        let message = error.to_string();
        for expected in ["'orders'", "write_back", "BEGIN", "COMMIT"] {
            assert!(
                message.contains(expected),
                "the refusal must contain {expected:?}: {message}"
            );
        }
    }

    /// Inside a transaction the update stages to the accelerator and returns its
    /// count; the delivery worker carries it to the source from the markers that
    /// commit writes, so nothing is pushed from here.
    #[tokio::test]
    async fn an_update_in_a_transaction_stages_and_reports_its_count() {
        let sink = WriteBackUpdateSink {
            accelerator_plan: count_exec(7),
            dataset_name: "orders".to_string(),
        };

        let count = sink
            .delete_from(task_ctx_in_transaction())
            .await
            .expect("a transactional update stages");
        assert_eq!(count, 7, "the accelerator's affected-row count is returned");
    }

    /// `DELETE` has no transaction-aware sink, so a write-back dataset cannot
    /// record one for delivery and refuses it outright, pointing at the source.
    #[test]
    fn delete_on_a_write_back_dataset_is_refused() {
        let message = super::delete_not_supported("orders").to_string();
        for expected in [
            "'orders'",
            "DELETE is not supported",
            // The transition, not a bare "delete at the source": a committed
            // write for the same key may still be undelivered, and delivery
            // would put the row back after a source-side delete.
            "take this dataset out of write-back",
            "dataset_acceleration_write_back_pending_keys",
            "change stream",
        ] {
            assert!(
                message.contains(expected),
                "the refusal must contain {expected:?}: {message}"
            );
        }
    }
}
