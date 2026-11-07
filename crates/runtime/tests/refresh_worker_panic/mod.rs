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

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use arrow::array::{ArrayRef, Int32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::TableReference;
use datafusion::datasource::TableProvider;
use datafusion::datasource::memory::MemTable;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_table_providers::util::retriable_error::RetriableError;
use runtime::accelerated::refresh::{Refresh, RefreshSQL};
use runtime::accelerated::{Error as AcceleratedError, RefreshTaskRunner};
use runtime::component::dataset::acceleration::RefreshMode;
use runtime::federated::FederatedTable;
use runtime::status;
use tokio::runtime::Handle;
use tokio::sync::{Mutex, RwLock, Semaphore};
use tokio::time::{Duration, timeout};

/// Distinct, non-sequential request ids: a completion is reported under the id
/// of the request that produced it (#13544), so a runner that echoed a
/// constant, or the wrong one of the two in flight, would still pass with 1
/// and 2.
const PANICKING_REQUEST: u64 = 7;
const SUCCEEDING_REQUEST: u64 = 11;

#[derive(Debug)]
struct PanickingOnceTableProvider {
    inner: Arc<MemTable>,
    should_panic: AtomicBool,
}

impl PanickingOnceTableProvider {
    fn new(inner: Arc<MemTable>) -> Self {
        Self {
            inner,
            should_panic: AtomicBool::new(true),
        }
    }
}

#[async_trait]
impl TableProvider for PanickingOnceTableProvider {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn constraints(&self) -> Option<&datafusion::common::Constraints> {
        self.inner.constraints()
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        self.inner.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<datafusion::physical_plan::Statistics> {
        self.inner.statistics()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        assert!(
            !self.should_panic.swap(false, Ordering::SeqCst),
            "intentional panic for refresh worker test"
        );

        self.inner.scan(state, projection, filters, limit).await
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: datafusion::logical_expr::dml::InsertOp,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.inner.insert_into(state, input, insert_op).await
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.inner.get_column_default(column)
    }
}

#[tokio::test]
async fn refresh_worker_recovers_from_panic() -> Result<(), String> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        false,
    )]));

    let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let batch =
        RecordBatch::try_new(Arc::clone(&schema), vec![values]).map_err(|e| e.to_string())?;

    let federated_mem_table = Arc::new(
        MemTable::try_new(Arc::clone(&schema), vec![vec![batch.clone()]])
            .map_err(|e| e.to_string())?,
    );

    let accelerator_mem_table = Arc::new(
        MemTable::try_new(Arc::clone(&schema), vec![Vec::new()]).map_err(|e| e.to_string())?,
    );

    let dataset_name = TableReference::bare("panic_dataset");

    let refresh_defaults = Refresh::new(RefreshMode::Append).refresh_sql(RefreshSQL::new(
        dataset_name.clone(),
        runtime::accelerated::refresh::RefreshSQLColumns::Named(vec![
            datafusion::sql::sqlparser::ast::Ident::new("value"),
        ]),
        vec![],
        None,
    ));
    let refresh_state = Arc::new(RwLock::new(refresh_defaults));

    let runtime_status = status::RuntimeStatus::new();

    let panicking_provider: Arc<dyn TableProvider> = Arc::new(PanickingOnceTableProvider::new(
        Arc::clone(&federated_mem_table),
    ));
    let federated_table = Arc::new(FederatedTable::new_unchecked(Arc::clone(
        &panicking_provider,
    )));

    let accelerator_provider: Arc<dyn TableProvider> = accelerator_mem_table;

    let mut runner = RefreshTaskRunner::builder(
        runtime_status,
        dataset_name.clone(),
        federated_table,
        None,
        refresh_state,
        accelerator_provider,
        Handle::current(),
        Arc::new(Mutex::new(())),
    )
    .build();

    let (start_refresh, mut on_refresh_complete) =
        runner.start().expect("Should start refresh task");

    start_refresh
        .send((PANICKING_REQUEST, None))
        .await
        .map_err(|e| e.to_string())?;

    let (first_id, first_result) = timeout(Duration::from_secs(10), on_refresh_complete.recv())
        .await
        .map_err(|_| "timed out waiting for panic result".to_string())?
        .ok_or_else(|| "refresh worker channel closed unexpectedly".to_string())?;

    assert_eq!(
        first_id, PANICKING_REQUEST,
        "a panicking refresh must still be reported under the request that started it"
    );

    match first_result {
        Ok(_) => return Err("expected panic error from first refresh".to_string()),
        Err(AcceleratedError::RefreshWorkerPanicked {
            dataset_name,
            message,
        }) => {
            assert_eq!(dataset_name, "panic_dataset");
            assert!(
                message.contains("intentional panic"),
                "unexpected panic message: {message}"
            );
        }
        Err(other) => return Err(format!("unexpected error from first refresh: {other}")),
    }

    start_refresh
        .send((SUCCEEDING_REQUEST, None))
        .await
        .map_err(|e| e.to_string())?;

    let (second_id, second_result) = timeout(Duration::from_secs(10), on_refresh_complete.recv())
        .await
        .map_err(|_| "timed out waiting for successful refresh".to_string())?
        .ok_or_else(|| "refresh worker channel closed unexpectedly".to_string())?;

    assert_eq!(
        second_id, SUCCEEDING_REQUEST,
        "the second completion must be reported under the second request, not the first"
    );

    second_result.map_err(|e| format!("second refresh returned error: {e}"))?;

    runner.abort();

    Ok(())
}

/// A source whose every scan fails with an error the refresh retries.
#[derive(Debug)]
struct UnavailableTableProvider {
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for UnavailableTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        datafusion::logical_expr::TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Err(datafusion::error::DataFusionError::External(Box::new(
            RetriableError::DataRetrievalError {
                source: datafusion::error::DataFusionError::Execution(
                    "source unavailable".to_string(),
                ),
            },
        )))
    }
}

fn value_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        false,
    )]))
}

fn source_table() -> Result<Arc<dyn TableProvider>, String> {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let batch = RecordBatch::try_new(value_schema(), vec![values]).map_err(|e| e.to_string())?;
    Ok(Arc::new(
        MemTable::try_new(value_schema(), vec![vec![batch]]).map_err(|e| e.to_string())?,
    ))
}

/// An append-refresh runner for `name` that reads `source`, retrying failed refreshes,
/// and the accelerator it appends to.
fn append_refresh_runner(
    name: &str,
    source: Arc<dyn TableProvider>,
    semaphore: Arc<Semaphore>,
) -> Result<(RefreshTaskRunner, Arc<MemTable>), String> {
    let dataset_name = TableReference::bare(name);
    let refresh = Refresh::new(RefreshMode::Append)
        .refresh_sql(RefreshSQL::new(
            dataset_name.clone(),
            runtime::accelerated::refresh::RefreshSQLColumns::Named(vec![
                datafusion::sql::sqlparser::ast::Ident::new("value"),
            ]),
            vec![],
            None,
        ))
        .with_retry(true, None);
    let accelerator =
        Arc::new(MemTable::try_new(value_schema(), vec![Vec::new()]).map_err(|e| e.to_string())?);
    let runner = RefreshTaskRunner::builder(
        status::RuntimeStatus::new(),
        dataset_name,
        Arc::new(FederatedTable::new_unchecked(source)),
        None,
        Arc::new(RwLock::new(refresh)),
        Arc::clone(&accelerator) as Arc<dyn TableProvider>,
        Handle::current(),
        Arc::new(Mutex::new(())),
    )
    .with_semaphore(semaphore)
    .build();
    Ok((runner, accelerator))
}

async fn row_count(table: &MemTable) -> usize {
    let mut rows = 0;
    for partition in &table.batches {
        rows += partition
            .read()
            .await
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>();
    }
    rows
}

/// The refresher can be handing the runner a request while the runner reports a
/// finished one, so the runner must keep taking requests while completions wait unread.
#[tokio::test]
async fn a_runner_takes_requests_while_its_completions_are_unread() -> Result<(), String> {
    let (mut runner, accelerator) =
        append_refresh_runner("unread", source_table()?, Arc::new(Semaphore::new(1)))?;
    let (start_refresh, mut on_refresh_complete) = runner.start().map_err(|e| e.to_string())?;

    for request_id in 1..=4 {
        timeout(
            Duration::from_secs(5),
            start_refresh.send((request_id, None)),
        )
        .await
        .map_err(|_| format!("request {request_id} was not taken while completions were unread"))?
        .map_err(|e| e.to_string())?;
        // Each refresh appends the 3 source rows. Waiting for them means each request
        // is reported rather than replaced by the next one.
        let expected = 3 * usize::try_from(request_id).map_err(|e| e.to_string())?;
        timeout(Duration::from_secs(10), async {
            while row_count(&accelerator).await < expected {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .map_err(|_| format!("refresh {request_id} did not write its rows"))?;
    }

    let mut completed = Vec::new();
    while completed.len() < 4 {
        let (request_id, result) = timeout(Duration::from_secs(10), on_refresh_complete.recv())
            .await
            .map_err(|_| format!("timed out after completions {completed:?}"))?
            .ok_or_else(|| "refresh worker channel closed unexpectedly".to_string())?;
        result.map_err(|e| format!("refresh {request_id} failed: {e}"))?;
        completed.push(request_id);
    }
    assert_eq!(completed, vec![1, 2, 3, 4]);

    runner.abort();
    Ok(())
}

/// A refresh waiting out a retry backoff must not hold the refresh slot that other
/// datasets' refreshes share.
#[tokio::test]
async fn a_refresh_waiting_to_retry_leaves_the_shared_refresh_slot_free() -> Result<(), String> {
    let semaphore = Arc::new(Semaphore::new(1));
    let (mut failing, _) = append_refresh_runner(
        "failing",
        Arc::new(UnavailableTableProvider {
            schema: value_schema(),
        }),
        Arc::clone(&semaphore),
    )?;
    let (mut healthy, _) = append_refresh_runner("healthy", source_table()?, semaphore)?;
    let (start_failing, _failing_completions) = failing.start().map_err(|e| e.to_string())?;
    let (start_healthy, mut healthy_completions) = healthy.start().map_err(|e| e.to_string())?;

    start_failing
        .send((1, None))
        .await
        .map_err(|e| e.to_string())?;
    // The first attempt fails at once, so the failing refresh is now waiting to retry.
    tokio::time::sleep(Duration::from_millis(300)).await;
    start_healthy
        .send((1, None))
        .await
        .map_err(|e| e.to_string())?;

    let (_, result) = timeout(Duration::from_secs(10), healthy_completions.recv())
        .await
        .map_err(|_| "the healthy refresh waited on the retrying one".to_string())?
        .ok_or_else(|| "refresh worker channel closed unexpectedly".to_string())?;
    result.map_err(|e| format!("healthy refresh failed: {e}"))?;

    failing.abort();
    healthy.abort();
    Ok(())
}
