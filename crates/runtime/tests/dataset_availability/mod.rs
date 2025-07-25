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
};
use spicepod::component::dataset::CheckAvailability;
use std::sync::Arc;

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
        .map_err(|e| anyhow::anyhow!("Failed to build dataset: {}", e))?;

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
        .map_err(|e| anyhow::anyhow!("Failed to build dataset: {}", e))?;

    Ok(dataset)
}

#[tokio::test]
async fn dataset_check_availability_register_skipped_when_disabled() -> Result<(), anyhow::Error> {
    // Create a test runtime to get DataFusion instance
    let app = AppBuilder::new("dataset_check_availability_test").build();
    let rt = Runtime::builder().with_app(app).build().await;

    // Create DatasetsHealthMonitor directly
    let monitor = DatasetsHealthMonitor::new(rt.datafusion());

    // Create dataset with availability monitor disabled
    let dataset = get_test_dataset_with_check_availability_disabled().await?;

    // Try to register the dataset - should be skipped
    let result = monitor.register_dataset(&dataset).await;
    assert!(result.is_ok());

    // Check that monitored_datasets is empty
    let monitored_datasets = monitor.monitored_datasets.lock().await;
    assert!(monitored_datasets.is_empty());

    Ok(())
}

#[tokio::test]
async fn dataset_check_availability_register_skipped_when_accelerated() -> Result<(), anyhow::Error>
{
    // Create a test runtime to get DataFusion instance
    let app = AppBuilder::new("dataset_check_availability_test").build();
    let rt = Runtime::builder().with_app(app).build().await;

    // Create DatasetsHealthMonitor directly
    let monitor = DatasetsHealthMonitor::new(rt.datafusion());

    // Create dataset with acceleration enabled (which should skip monitoring)
    let dataset = get_test_dataset_with_acceleration().await?;

    // Try to register the dataset - should be skipped due to acceleration
    let result = monitor.register_dataset(&dataset).await;
    assert!(result.is_ok());

    // Check that monitored_datasets is empty
    let monitored_datasets = monitor.monitored_datasets.lock().await;
    assert!(monitored_datasets.is_empty());

    Ok(())
}
