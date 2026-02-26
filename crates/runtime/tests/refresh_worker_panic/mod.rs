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

use std::any::Any;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use arrow::array::{ArrayRef, Int32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::datasource::memory::MemTable;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::TableReference;
use runtime::accelerated_table::refresh::Refresh;
use runtime::accelerated_table::{Error as AcceleratedError, RefreshTaskRunner};
use runtime::component::dataset::acceleration::RefreshMode;
use runtime::federated_table::FederatedTable;
use runtime::status;
use tokio::runtime::Handle;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{Duration, timeout};

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
    fn as_any(&self) -> &dyn Any {
        self
    }

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

    let refresh_defaults = Refresh::new(RefreshMode::Append)
        .sql(format!("SELECT value FROM {}", dataset_name.table()));
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

    start_refresh.send(None).await.map_err(|e| e.to_string())?;

    let first_result = timeout(Duration::from_secs(10), on_refresh_complete.recv())
        .await
        .map_err(|_| "timed out waiting for panic result".to_string())?
        .ok_or_else(|| "refresh worker channel closed unexpectedly".to_string())?;

    match first_result {
        Ok(()) => return Err("expected panic error from first refresh".to_string()),
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

    start_refresh.send(None).await.map_err(|e| e.to_string())?;

    let second_result = timeout(Duration::from_secs(10), on_refresh_complete.recv())
        .await
        .map_err(|_| "timed out waiting for successful refresh".to_string())?
        .ok_or_else(|| "refresh worker channel closed unexpectedly".to_string())?;

    second_result.map_err(|e| format!("second refresh returned error: {e}"))?;

    runner.abort();

    Ok(())
}
