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

use app::AppBuilder;
use runtime::{
    Runtime,
    component::dataset::{Dataset, builder::DatasetBuilder},
    datasets_health_monitor::DatasetsHealthMonitor,
    status::ComponentStatus,
};
use spicepod::component::dataset::CheckAvailability;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::{ExecutionPlan, empty::EmptyExec};
use datafusion::sql::TableReference;

use crate::utils::{register_test_connectors, wait_until_true};

/// A [`TableProvider`] whose `scan` fails while `fail` is set — used to simulate
/// a source going unavailable and then recovering, without any external system.
#[derive(Debug)]
struct ToggleableProvider {
    schema: SchemaRef,
    fail: Arc<AtomicBool>,
}

#[async_trait]
impl TableProvider for ToggleableProvider {
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
        if self.fail.load(Ordering::SeqCst) {
            return Err(DataFusionError::Execution(
                "simulated source outage".to_string(),
            ));
        }
        Ok(Arc::new(EmptyExec::new(Arc::clone(&self.schema))))
    }
}

async fn get_test_dataset_with_check_availability_disabled() -> Result<Dataset, anyhow::Error> {
    let file_path = if std::fs::exists("./tests/file/datatypes.parquet")? {
        "./tests/file/datatypes.parquet"
    } else if std::fs::exists("./crates/runtime/tests/file/datatypes.parquet")? {
        "./crates/runtime/tests/file/datatypes.parquet"
    } else {
        return Err(anyhow::anyhow!("Could not find datatypes.parquet file"));
    };

    let mut spicepod_dataset =
        spicepod::component::dataset::Dataset::new(format!("file:{file_path}"), "datatypes");
    spicepod_dataset.check_availability = CheckAvailability::Disabled;

    let app = AppBuilder::new("test")
        .with_dataset(spicepod_dataset.clone())
        .build();
    let rt = Runtime::builder().with_app(app).build().await;

    let dataset = DatasetBuilder::try_from(spicepod_dataset)?
        .with_app(Arc::new(AppBuilder::new("test").build()))
        .with_runtime(Arc::new(rt))
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build dataset: {e}"))?;

    Ok(dataset)
}

async fn get_test_dataset_with_acceleration() -> Result<Dataset, anyhow::Error> {
    let file_path = if std::fs::exists("./tests/file/datatypes.parquet")? {
        "./tests/file/datatypes.parquet"
    } else if std::fs::exists("./crates/runtime/tests/file/datatypes.parquet")? {
        "./crates/runtime/tests/file/datatypes.parquet"
    } else {
        return Err(anyhow::anyhow!("Could not find datatypes.parquet file"));
    };

    let mut spicepod_dataset =
        spicepod::component::dataset::Dataset::new(format!("file:{file_path}"), "datatypes");
    spicepod_dataset.acceleration = Some(spicepod::acceleration::Acceleration::default());

    let app = AppBuilder::new("test")
        .with_dataset(spicepod_dataset.clone())
        .build();
    let rt = Runtime::builder().with_app(app).build().await;

    let dataset = DatasetBuilder::try_from(spicepod_dataset)?
        .with_app(Arc::new(AppBuilder::new("test").build()))
        .with_runtime(Arc::new(rt))
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build dataset: {e}"))?;

    Ok(dataset)
}

#[tokio::test]
async fn dataset_check_availability_register_skipped_when_disabled() -> Result<(), anyhow::Error> {
    register_test_connectors().await;

    // Create a test runtime to get DataFusion instance
    let app = AppBuilder::new("dataset_check_availability_test").build();
    let rt = Runtime::builder().with_app(app).build().await;

    // Create DatasetsHealthMonitor directly
    let monitor = DatasetsHealthMonitor::new(rt.datafusion());

    // Create dataset with availability monitor disabled
    let dataset = get_test_dataset_with_check_availability_disabled().await?;

    // Try to register the dataset - should be skipped
    let result = monitor.register_dataset(&dataset).await;
    result.expect("Should register dataset without error");

    // Check that monitored_datasets is empty
    let monitored_datasets = monitor.monitored_datasets.lock().await;
    assert!(monitored_datasets.is_empty());

    Ok(())
}

#[tokio::test]
async fn dataset_check_availability_register_skipped_when_accelerated() -> Result<(), anyhow::Error>
{
    register_test_connectors().await;

    // Create a test runtime to get DataFusion instance
    let app = AppBuilder::new("dataset_check_availability_test").build();
    let rt = Runtime::builder().with_app(app).build().await;

    // Create DatasetsHealthMonitor directly
    let monitor = DatasetsHealthMonitor::new(rt.datafusion());

    // Create dataset with acceleration enabled (which should skip monitoring)
    let dataset = get_test_dataset_with_acceleration().await?;

    // Try to register the dataset - should be skipped due to acceleration
    let result = monitor.register_dataset(&dataset).await;
    result.expect("Should register dataset without error");

    // Check that monitored_datasets is empty
    let monitored_datasets = monitor.monitored_datasets.lock().await;
    assert!(monitored_datasets.is_empty());

    Ok(())
}

/// End-to-end: a non-accelerated dataset whose source becomes unavailable is
/// moved to `Error` status by the availability monitor within its configured
/// `check_availability_interval`, and returns to `Ready` once the source
/// recovers. This is what `GET /v1/datasets?status=true` reports.
#[tokio::test]
async fn availability_monitor_marks_dataset_error_on_source_outage() -> Result<(), anyhow::Error> {
    register_test_connectors().await;

    let app = AppBuilder::new("availability_status_test").build();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    let df = rt.datafusion();
    let status = df.runtime_status();

    // Register a toggleable provider under the dataset name so the monitor's
    // connectivity probe (a `scan(.., LIMIT 1)`) resolves to it.
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let fail = Arc::new(AtomicBool::new(false));
    let table_ref = TableReference::bare("toggle_source");
    df.ctx
        .register_table(
            table_ref.clone(),
            Arc::new(ToggleableProvider {
                schema: Arc::clone(&schema),
                fail: Arc::clone(&fail),
            }),
        )
        .expect("register toggleable provider");

    // Build a non-accelerated dataset with a short (1s) availability interval.
    let mut spicepod_dataset = spicepod::component::dataset::Dataset::new("sink", "toggle_source");
    spicepod_dataset.check_availability_interval = Some("1s".to_string());
    let dataset: Dataset = DatasetBuilder::try_from(spicepod_dataset)?
        .with_app(Arc::new(
            AppBuilder::new("availability_status_test").build(),
        ))
        .with_runtime(Arc::clone(&rt))
        .build()
        .map_err(|e| anyhow::anyhow!("failed to build dataset: {e}"))?;
    assert_eq!(
        dataset.check_availability_interval,
        Some(Duration::from_secs(1))
    );

    // Simulate the dataset having loaded successfully (its healthy state).
    status.update_dataset(&table_ref, ComponentStatus::Ready);

    let monitor = DatasetsHealthMonitor::new(Arc::clone(&df));
    monitor
        .register_dataset(&dataset)
        .await
        .expect("register dataset for monitoring");
    monitor.start();

    // Source is reachable: the dataset stays Ready across a couple of intervals.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        status.get_dataset_status(&table_ref),
        Some(ComponentStatus::Ready),
        "dataset must stay Ready while the source is reachable"
    );

    // Source goes down -> the next probe must move the dataset to Error.
    fail.store(true, Ordering::SeqCst);
    let became_error = wait_until_true(Duration::from_secs(15), || {
        let status = Arc::clone(&status);
        let table_ref = table_ref.clone();
        async move {
            status
                .get_dataset_status(&table_ref)
                .is_some_and(|s| s.is_error())
        }
    })
    .await;
    assert!(
        became_error,
        "dataset should be Error within a few intervals after the source becomes unavailable"
    );
    // The error surfaces the source failure to the user.
    let err_status = status.get_dataset_status(&table_ref).expect("status");
    assert!(
        err_status
            .error_message()
            .is_some_and(|m| m.contains("simulated source outage")),
        "error status should carry the underlying source error, got {err_status:?}"
    );

    // Source recovers -> the dataset returns to Ready.
    fail.store(false, Ordering::SeqCst);
    let recovered = wait_until_true(Duration::from_secs(15), || {
        let status = Arc::clone(&status);
        let table_ref = table_ref.clone();
        async move {
            matches!(
                status.get_dataset_status(&table_ref),
                Some(ComponentStatus::Ready)
            )
        }
    })
    .await;
    assert!(
        recovered,
        "dataset should return to Ready after the source recovers"
    );

    Ok(())
}
