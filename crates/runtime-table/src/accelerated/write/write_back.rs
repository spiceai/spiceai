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
//! Writes are applied to the local accelerator first (fast path, returning
//! to the caller once the accelerator commit completes), then asynchronously
//! forwarded to the federated source. The federated source may lag briefly;
//! failures to persist back to the source are logged but do not affect the
//! synchronous response.
//!
//! Implemented as a [`DataSink`] so that:
//!
//! 1. The write only occurs when the returned [`ExecutionPlan`] is executed,
//!    not merely planned. If the caller cancels before execution, neither
//!    the accelerator nor the federated source is modified.
//! 2. The input batches are consumed exactly once — the same batches written
//!    to the accelerator are forwarded to the federated source, so the two
//!    sides cannot diverge due to non-deterministic input plans.
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
use crate::federated::FederatedTable;

/// Whether a Cayenne transaction is active on the live execution context. When it is, the
/// write STAGES to the accelerator (published/rolled back atomically at COMMIT) and the
/// delivery worker reconciles it to the source from the dirty-key markers written in the
/// commit transaction — so the sink must NOT also fire-and-forget the write to the source
/// (that would push staged-but-uncommitted rows and duplicate the worker's delivery).
/// Mirrors the INSERT path (`WriteBackDataSink::write_all`).
fn request_in_transaction(context: &TaskContext) -> bool {
    resolve_request_context(context, false)
        .is_some_and(|rc| rc.extension::<cayenne::CayenneTransaction>().is_some())
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
    federated: Arc<FederatedTable>,
    refresher: Arc<Refresher>,
    schema: SchemaRef,
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
        accelerator,
        federated,
        refresher,
        overwrite,
        schema,
        session_state,
    });
    Ok(Arc::new(DataSinkExec::new(input, sink, None)))
}

struct WriteBackDataSink {
    accelerator: Arc<dyn TableProvider>,
    federated: Arc<FederatedTable>,
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
        // Consume the input stream exactly once and buffer the batches so they
        // can be replayed to both the accelerator (synchronously) and the
        // federated source (asynchronously). This guarantees both sides see
        // identical data and the input plan is never executed twice.
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
            Arc::clone(&input_schema),
            batches.clone(),
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

        // Durable federated write-back (#11838): a write inside a Cayenne
        // transaction STAGES to the accelerator and is reconciled to the source
        // by the delivery worker (from the dirty-key markers written in the same
        // commit) — NOT by this fire-and-forget forward. Forwarding here would
        // push staged-but-uncommitted batches to the source before COMMIT (and
        // duplicate what the worker delivers), so skip it when a transaction is
        // active on this request context. Non-transaction writes keep the
        // ordinary write-back forward.
        let in_transaction = resolve_request_context(context, false)
            .is_some_and(|rc| rc.extension::<cayenne::CayenneTransaction>().is_some());
        if in_transaction {
            return Ok(row_count);
        }

        // Forward the same buffered batches to the federated source in the
        // background. Failures are logged but do not affect the synchronous
        // response.
        let federated = Arc::clone(&self.federated);
        let overwrite = self.overwrite;
        let session_state = self.session_state.clone();
        tokio::spawn(async move {
            let federated_provider = federated.table_provider().await;
            if let Err(e) = execute_insert(
                federated_provider,
                input_schema,
                batches,
                overwrite,
                &session_state,
                None,
            )
            .await
            {
                tracing::error!("Write-back: failed to persist write to federated source: {e}");
            }
        });

        Ok(row_count)
    }
}

/// Creates a `DeletionExec` plan for write-back deletes.
///
/// The accelerator delete executes synchronously; the federated delete is
/// forwarded asynchronously in the background. Failures on the federated side
/// are logged but do not affect the synchronous response.
pub(crate) async fn delete_write_back(
    state: &dyn Session,
    filters: Vec<Expr>,
    accelerator: Arc<dyn TableProvider>,
    federated: Arc<FederatedTable>,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    let session_state = state
        .as_any()
        .downcast_ref::<SessionState>()
        .ok_or_else(|| {
            DataFusionError::Internal(
                "Session is not a SessionState in delete_write_back".to_string(),
            )
        })?
        .clone();
    let markers_own_delivery =
        super::dual_write::accelerator_owns_write_back_delivery(&accelerator);
    let accelerator_plan = accelerator.delete_from(state, filters.clone()).await?;
    Ok(Arc::new(DeletionExec::new(Arc::new(
        WriteBackDeletionSink {
            accelerator_plan,
            federated,
            filters,
            session_state,
            markers_own_delivery,
        },
    ))))
}

struct WriteBackDeletionSink {
    accelerator_plan: Arc<dyn ExecutionPlan>,
    federated: Arc<FederatedTable>,
    filters: Vec<Expr>,
    session_state: SessionState,
    /// The accelerator records this delete as a durable write-back marker, so
    /// the delivery worker owns reconciling it to the source and the forward
    /// below must stand down.
    markers_own_delivery: bool,
}

#[async_trait]
impl DeletionSink for WriteBackDeletionSink {
    async fn delete_from(
        &self,
        context: Arc<TaskContext>,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        // Execute under the LIVE execution context so a delete inside a Cayenne
        // transaction STAGES rather than publishing immediately (see request_in_transaction).
        let batches = datafusion::physical_plan::collect(
            Arc::clone(&self.accelerator_plan),
            Arc::clone(&context),
        )
        .await?;
        let count = extract_dml_count(&batches);

        // Inside a transaction, the delivery worker reconciles the change to the source.
        if request_in_transaction(&context) {
            return Ok(count);
        }

        // A durable-write-back table recorded this delete's keys as markers in
        // the same metastore transaction as the delete, and the delivery worker
        // reconciles them. Forwarding here as well would deliver one mutation
        // through two unordered paths: besides the duplicate DML, a forward that
        // lands late can delete a row a later marker-based upsert had already
        // re-created at the source — after that upsert's marker was cleared, so
        // nothing is left to reconcile it back. The marker path is also the
        // durable one; this forward is lost outright if the process dies.
        if self.markers_own_delivery {
            return Ok(count);
        }

        let federated = Arc::clone(&self.federated);
        let filters = self.filters.clone();
        let session_state = self.session_state.clone();
        tokio::spawn(async move {
            let provider = federated.table_provider().await;
            match provider.delete_from(&session_state, filters).await {
                Ok(plan) => {
                    if let Err(e) =
                        datafusion::physical_plan::collect(plan, session_state.task_ctx()).await
                    {
                        tracing::error!(
                            "Write-back: failed to persist delete to federated source: {e}"
                        );
                    }
                }
                Err(e) => {
                    tracing::error!("Write-back: failed to plan delete on federated source: {e}");
                }
            }
        });

        Ok(count)
    }
}

/// Creates a `DeletionExec` plan for write-back updates.
///
/// The accelerator update executes synchronously; the federated update is
/// forwarded asynchronously in the background. Failures on the federated side
/// are logged but do not affect the synchronous response.
pub(crate) async fn update_write_back(
    state: &dyn Session,
    assignments: Vec<(String, Expr)>,
    filters: Vec<Expr>,
    accelerator: Arc<dyn TableProvider>,
    federated: Arc<FederatedTable>,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    let session_state = state
        .as_any()
        .downcast_ref::<SessionState>()
        .ok_or_else(|| {
            DataFusionError::Internal(
                "Session is not a SessionState in update_write_back".to_string(),
            )
        })?
        .clone();
    let accelerator_plan = accelerator
        .update(state, assignments.clone(), filters.clone())
        .await?;
    Ok(Arc::new(DeletionExec::new(Arc::new(WriteBackUpdateSink {
        accelerator_plan,
        federated,
        assignments,
        filters,
        session_state,
    }))))
}

struct WriteBackUpdateSink {
    accelerator_plan: Arc<dyn ExecutionPlan>,
    federated: Arc<FederatedTable>,
    assignments: Vec<(String, Expr)>,
    filters: Vec<Expr>,
    session_state: SessionState,
}

#[async_trait]
impl DeletionSink for WriteBackUpdateSink {
    async fn delete_from(
        &self,
        context: Arc<TaskContext>,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        // Execute the accelerator plan under the LIVE execution context so a write inside
        // a Cayenne transaction STAGES (and is published/rolled back atomically at COMMIT)
        // rather than publishing immediately. Using a fresh context here would make the
        // accelerator's transaction-aware sink publish, defeating gated rollback.
        let batches = datafusion::physical_plan::collect(
            Arc::clone(&self.accelerator_plan),
            Arc::clone(&context),
        )
        .await?;
        let count = extract_dml_count(&batches);

        // Inside a transaction, the delivery worker reconciles the committed change to the
        // source from the commit-transaction markers; do NOT also fire-and-forget it here.
        if request_in_transaction(&context) {
            return Ok(count);
        }

        let federated = Arc::clone(&self.federated);
        let assignments = self.assignments.clone();
        let filters = self.filters.clone();
        let session_state = self.session_state.clone();
        tokio::spawn(async move {
            let provider = federated.table_provider().await;
            match provider.update(&session_state, assignments, filters).await {
                Ok(plan) => {
                    if let Err(e) =
                        datafusion::physical_plan::collect(plan, session_state.task_ctx()).await
                    {
                        tracing::error!(
                            "Write-back: failed to persist update to federated source: {e}"
                        );
                    }
                }
                Err(e) => {
                    tracing::error!("Write-back: failed to plan update on federated source: {e}");
                }
            }
        });

        Ok(count)
    }
}

/// Extracts the affected-row count from a DML result batch (delete or update output).
pub(super) fn extract_dml_count(batches: &[RecordBatch]) -> u64 {
    batches
        .iter()
        .flat_map(RecordBatch::columns)
        .find_map(|arr| {
            arr.as_any()
                .downcast_ref::<UInt64Array>()
                .and_then(|a| a.values().first().copied())
        })
        .unwrap_or(0)
}

/// Builds an in-memory execution plan from buffered batches and executes
/// an `insert_into` against the supplied table provider. The input plan is
/// cast to the target provider's schema so differences between the
/// accelerator and federated source schemas (extra columns, differing
/// types) don't cause incorrect writes.
pub(crate) async fn execute_insert(
    table: Arc<dyn TableProvider>,
    input_schema: SchemaRef,
    batches: Vec<RecordBatch>,
    overwrite: InsertOp,
    session_state: &SessionState,
    task_context: Option<Arc<TaskContext>>,
) -> DataFusionResult<()> {
    let memory_source = MemorySourceConfig::try_new(&[batches], input_schema, None)?;
    let source: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(memory_source)));
    let input: Arc<dyn ExecutionPlan> = Arc::new(SchemaCastScanExec::new(source, table.schema()));

    let plan = table.insert_into(session_state, input, overwrite).await?;
    let task_ctx = task_context.unwrap_or_else(|| session_state.task_ctx());
    let _ = datafusion::physical_plan::collect(plan, task_ctx).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{WriteBackDeletionSink, WriteBackUpdateSink, extract_dml_count};
    use arrow::array::{StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow_schema::SchemaRef;
    use async_trait::async_trait;
    use data_components::delete::DeletionSink;
    use datafusion::catalog::Session;
    use datafusion::datasource::{TableProvider, TableType};
    use datafusion::error::{DataFusionError, Result as DataFusionResult};
    use datafusion::execution::{SendableRecordBatchStream, TaskContext};
    use datafusion::logical_expr::Expr;
    use datafusion::physical_expr::EquivalenceProperties;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
    };
    use datafusion::prelude::SessionContext;
    use datafusion_datasource::memory::MemorySourceConfig;
    use datafusion_datasource::source::DataSourceExec;
    use std::sync::Arc;

    use crate::federated::FederatedTable;

    fn count_exec(n: u64) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(UInt64Array::from(vec![n]))],
        )
        .expect("valid schema and array");
        let memory =
            MemorySourceConfig::try_new(&[vec![batch]], schema, None).expect("valid memory source");
        Arc::new(DataSourceExec::new(Arc::new(memory)))
    }

    struct ErrorExec {
        properties: Arc<PlanProperties>,
        message: String,
    }

    impl ErrorExec {
        fn new_arc(message: impl Into<String>) -> Arc<dyn ExecutionPlan> {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "count",
                DataType::UInt64,
                false,
            )]));
            let properties = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&schema)),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ));
            Arc::new(Self {
                properties,
                message: message.into(),
            })
        }
    }

    impl std::fmt::Debug for ErrorExec {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "ErrorExec({})", self.message)
        }
    }

    impl DisplayAs for ErrorExec {
        fn fmt_as(
            &self,
            _t: DisplayFormatType,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            write!(f, "ErrorExec")
        }
    }

    impl ExecutionPlan for ErrorExec {
        fn name(&self) -> &'static str {
            "ErrorExec"
        }
        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }
        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }
        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> DataFusionResult<SendableRecordBatchStream> {
            Err(DataFusionError::Execution(self.message.clone()))
        }
    }

    struct MockTableProvider {
        schema: SchemaRef,
        plan: Arc<dyn ExecutionPlan>,
    }

    impl MockTableProvider {
        fn new_arc(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn TableProvider> {
            Arc::new(Self {
                schema: Arc::new(Schema::new(vec![Field::new(
                    "count",
                    DataType::UInt64,
                    false,
                )])),
                plan,
            })
        }
    }

    impl std::fmt::Debug for MockTableProvider {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "MockTableProvider")
        }
    }

    #[async_trait]
    impl TableProvider for MockTableProvider {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
        fn table_type(&self) -> TableType {
            TableType::Base
        }
        async fn scan(
            &self,
            _state: &dyn Session,
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            Err(DataFusionError::NotImplemented("scan".to_string()))
        }
        async fn delete_from(
            &self,
            _state: &dyn Session,
            _filters: Vec<Expr>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            Ok(Arc::clone(&self.plan))
        }
        async fn update(
            &self,
            _state: &dyn Session,
            _assignments: Vec<(String, Expr)>,
            _filters: Vec<Expr>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            Ok(Arc::clone(&self.plan))
        }
    }

    /// A federated `TableProvider` that sets `forwarded` whenever one of its write methods
    /// runs, so a test can assert whether the write-back sink's fire-and-forget federated
    /// forward was spawned.
    struct SignalingTableProvider {
        schema: SchemaRef,
        forwarded: Arc<std::sync::atomic::AtomicBool>,
    }

    impl SignalingTableProvider {
        fn new_arc(forwarded: Arc<std::sync::atomic::AtomicBool>) -> Arc<dyn TableProvider> {
            Arc::new(Self {
                schema: Arc::new(Schema::new(vec![Field::new(
                    "count",
                    DataType::UInt64,
                    false,
                )])),
                forwarded,
            })
        }
    }

    /// Drain any fire-and-forget task the sink may have spawned. `#[tokio::test]` uses a
    /// current-thread runtime, so a `tokio::spawn`ed task only makes progress when the test
    /// yields; after enough yields the `forwarded` flag deterministically reflects whether
    /// the forward ran — no wall-clock timeout, so a slow/busy CI cannot cause a false pass.
    /// (The positive-control tests below prove this drain is sufficient: they observe the
    /// flag set through the same path.)
    async fn drain_spawned_tasks() {
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
    }

    impl std::fmt::Debug for SignalingTableProvider {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "SignalingTableProvider")
        }
    }

    #[async_trait]
    impl TableProvider for SignalingTableProvider {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
        fn table_type(&self) -> TableType {
            TableType::Base
        }
        async fn scan(
            &self,
            _state: &dyn Session,
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            Err(DataFusionError::NotImplemented("scan".to_string()))
        }
        async fn delete_from(
            &self,
            _state: &dyn Session,
            _filters: Vec<Expr>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            self.forwarded
                .store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(count_exec(0))
        }
        async fn update(
            &self,
            _state: &dyn Session,
            _assignments: Vec<(String, Expr)>,
            _filters: Vec<Expr>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            self.forwarded
                .store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(count_exec(0))
        }
    }

    /// Build a `TaskContext` whose session config carries a `RequestContext` with an active
    /// `CayenneTransaction` — the exact shape `resolve_request_context` reads at execution
    /// time, so the sinks' `request_in_transaction` check sees a live transaction.
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

    // ── WriteBackDeletionSink ────────────────────────────────────────────

    #[tokio::test]
    async fn write_back_deletion_count_comes_from_accelerator() {
        let session_state = SessionContext::new().state();
        let federated = Arc::new(FederatedTable::Immediate(MockTableProvider::new_arc(
            count_exec(0),
        )));
        let sink = WriteBackDeletionSink {
            accelerator_plan: count_exec(42),
            federated,
            filters: vec![],
            session_state: session_state.clone(),
            markers_own_delivery: false,
        };

        let count = sink
            .delete_from(session_state.task_ctx())
            .await
            .expect("deletion should succeed");
        assert_eq!(count, 42);
    }

    #[tokio::test]
    async fn write_back_deletion_accelerator_error_propagates() {
        let session_state = SessionContext::new().state();
        let federated = Arc::new(FederatedTable::Immediate(MockTableProvider::new_arc(
            count_exec(0),
        )));
        let sink = WriteBackDeletionSink {
            accelerator_plan: ErrorExec::new_arc("accelerator delete failed"),
            federated,
            filters: vec![],
            session_state: session_state.clone(),
            markers_own_delivery: false,
        };

        let err = sink
            .delete_from(session_state.task_ctx())
            .await
            .expect_err("deletion should fail");
        assert!(err.to_string().contains("accelerator delete failed"));
    }

    // ── WriteBackUpdateSink ──────────────────────────────────────────────

    #[tokio::test]
    async fn write_back_update_count_comes_from_accelerator() {
        let session_state = SessionContext::new().state();
        let federated = Arc::new(FederatedTable::Immediate(MockTableProvider::new_arc(
            count_exec(0),
        )));
        let sink = WriteBackUpdateSink {
            accelerator_plan: count_exec(7),
            federated,
            assignments: vec![],
            filters: vec![],
            session_state: session_state.clone(),
        };

        let count = sink
            .delete_from(session_state.task_ctx())
            .await
            .expect("update should succeed");
        assert_eq!(count, 7);
    }

    #[tokio::test]
    async fn write_back_update_accelerator_error_propagates() {
        let session_state = SessionContext::new().state();
        let federated = Arc::new(FederatedTable::Immediate(MockTableProvider::new_arc(
            count_exec(0),
        )));
        let sink = WriteBackUpdateSink {
            accelerator_plan: ErrorExec::new_arc("accelerator update failed"),
            federated,
            assignments: vec![],
            filters: vec![],
            session_state: session_state.clone(),
        };

        let err = sink
            .delete_from(session_state.task_ctx())
            .await
            .expect_err("update should fail");
        assert!(err.to_string().contains("accelerator update failed"));
    }

    // ── transaction-aware federated forward (skip while in a transaction) ─

    #[tokio::test]
    async fn write_back_deletion_forwards_when_not_in_transaction() {
        // Positive control: outside a transaction the delete is published, so the
        // fire-and-forget federated forward runs (proving the mock detects it).
        let session_state = SessionContext::new().state();
        let forwarded = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let federated = Arc::new(FederatedTable::Immediate(SignalingTableProvider::new_arc(
            Arc::clone(&forwarded),
        )));
        let sink = WriteBackDeletionSink {
            accelerator_plan: count_exec(3),
            federated,
            filters: vec![],
            session_state: session_state.clone(),
            markers_own_delivery: false,
        };

        let count = sink
            .delete_from(session_state.task_ctx())
            .await
            .expect("deletion should succeed");
        assert_eq!(count, 3);
        drain_spawned_tasks().await;
        assert!(
            forwarded.load(std::sync::atomic::Ordering::SeqCst),
            "federated forward must run when not in a transaction"
        );
    }

    #[tokio::test]
    async fn write_back_deletion_skips_forward_in_transaction() {
        // Inside a transaction the delete only STAGES; the delivery worker reconciles it
        // from the commit markers, so the sink must NOT fire the federated forward (that
        // would push a staged-but-uncommitted delete to the source).
        let session_state = SessionContext::new().state();
        let forwarded = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let federated = Arc::new(FederatedTable::Immediate(SignalingTableProvider::new_arc(
            Arc::clone(&forwarded),
        )));
        let sink = WriteBackDeletionSink {
            accelerator_plan: count_exec(42),
            federated,
            filters: vec![],
            session_state,
            markers_own_delivery: false,
        };

        let count = sink
            .delete_from(task_ctx_in_transaction())
            .await
            .expect("deletion should succeed");
        assert_eq!(count, 42);
        drain_spawned_tasks().await;
        assert!(
            !forwarded.load(std::sync::atomic::Ordering::SeqCst),
            "federated forward must be skipped inside a transaction"
        );
    }

    /// A durable-write-back table records the delete as a marker and its
    /// delivery worker reconciles it, so this forward must stand down even
    /// outside a transaction. Running both would deliver one mutation through
    /// two unordered paths, and a forward that lands late can remove a row a
    /// later marker-based upsert had already re-created at the source.
    #[tokio::test]
    async fn write_back_deletion_skips_forward_when_markers_own_delivery() {
        let session_state = SessionContext::new().state();
        let forwarded = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let federated = Arc::new(FederatedTable::Immediate(SignalingTableProvider::new_arc(
            Arc::clone(&forwarded),
        )));
        let sink = WriteBackDeletionSink {
            accelerator_plan: count_exec(7),
            federated,
            filters: vec![],
            session_state: session_state.clone(),
            markers_own_delivery: true,
        };

        let count = sink
            .delete_from(session_state.task_ctx())
            .await
            .expect("deletion should succeed");
        assert_eq!(count, 7);
        drain_spawned_tasks().await;
        assert!(
            !forwarded.load(std::sync::atomic::Ordering::SeqCst),
            "the delivery worker owns this delete; the forward must not also run"
        );
    }

    #[tokio::test]
    async fn write_back_update_forwards_when_not_in_transaction() {
        let session_state = SessionContext::new().state();
        let forwarded = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let federated = Arc::new(FederatedTable::Immediate(SignalingTableProvider::new_arc(
            Arc::clone(&forwarded),
        )));
        let sink = WriteBackUpdateSink {
            accelerator_plan: count_exec(5),
            federated,
            assignments: vec![],
            filters: vec![],
            session_state: session_state.clone(),
        };

        let count = sink
            .delete_from(session_state.task_ctx())
            .await
            .expect("update should succeed");
        assert_eq!(count, 5);
        drain_spawned_tasks().await;
        assert!(
            forwarded.load(std::sync::atomic::Ordering::SeqCst),
            "federated forward must run when not in a transaction"
        );
    }

    #[tokio::test]
    async fn write_back_update_skips_forward_in_transaction() {
        let session_state = SessionContext::new().state();
        let forwarded = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let federated = Arc::new(FederatedTable::Immediate(SignalingTableProvider::new_arc(
            Arc::clone(&forwarded),
        )));
        let sink = WriteBackUpdateSink {
            accelerator_plan: count_exec(7),
            federated,
            assignments: vec![],
            filters: vec![],
            session_state,
        };

        let count = sink
            .delete_from(task_ctx_in_transaction())
            .await
            .expect("update should succeed");
        assert_eq!(count, 7);
        drain_spawned_tasks().await;
        assert!(
            !forwarded.load(std::sync::atomic::Ordering::SeqCst),
            "federated forward must be skipped inside a transaction"
        );
    }
}
