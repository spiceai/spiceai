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
use runtime::{Runtime, datasets_health_monitor::DatasetsHealthMonitor};
use spicepod::component::dataset::Dataset;
use std::sync::Arc;

fn get_test_dataset() -> Result<Dataset, anyhow::Error> {
    let file_path = if std::fs::exists("./tests/file/datatypes.parquet")? {
        "./tests/file/datatypes.parquet"
    } else if std::fs::exists("./crates/runtime/tests/file/datatypes.parquet")? {
        "./crates/runtime/tests/file/datatypes.parquet"
    } else {
        return Err(anyhow::anyhow!("Could not find datatypes.parquet file"));
    };

    Ok(Dataset::new(format!("file:{file_path}"), "datatypes"))
}

fn get_test_dataset_with_availability_monitor_disabled() -> Result<Dataset, anyhow::Error> {
    let file_path = if std::fs::exists("./tests/file/datatypes.parquet")? {
        "./tests/file/datatypes.parquet"
    } else if std::fs::exists("./crates/runtime/tests/file/datatypes.parquet")? {
        "./crates/runtime/tests/file/datatypes.parquet"
    } else {
        return Err(anyhow::anyhow!("Could not find datatypes.parquet file"));
    };

    let mut dataset = Dataset::new(format!("file:{file_path}"), "datatypes");
    dataset.availability_monitor_enabled = false;
    Ok(dataset)
}


#[tokio::test]
async fn dataset_availability_monitor_register_skipped_when_disabled() -> Result<(), anyhow::Error>
{
    // Create a test runtime to get DataFusion instance
    let app = AppBuilder::new("dataset_availability_monitor_test")
        .with_dataset(get_test_dataset()?)
        .build();

    let rt = Runtime::builder().with_app(app).build().await;

    // Create DatasetsHealthMonitor directly
    let monitor = DatasetsHealthMonitor::new(Arc::clone(&rt.df));

    // Create dataset with availability monitor disabled
    let dataset = get_test_dataset_with_availability_monitor_disabled()?;

    // Try to register the dataset - should be skipped
    let result = monitor.register_dataset(&dataset).await;
    assert!(result.is_ok());

    // Check that monitored_datasets is empty
    let monitored_datasets = monitor.monitored_datasets.lock().await;
    assert!(monitored_datasets.is_empty());

    Ok(())
}

#[tokio::test]
async fn dataset_availability_monitor_register_succeeds_when_enabled() -> Result<(), anyhow::Error>
{
    // Create a test runtime to get DataFusion instance
    let app = AppBuilder::new("dataset_availability_monitor_test")
        .with_dataset(get_test_dataset()?)
        .build();

    let rt = Runtime::builder().with_app(app).build().await;

    // Create DatasetsHealthMonitor directly
    let monitor = DatasetsHealthMonitor::new(Arc::clone(&rt.df));

    // Create dataset with availability monitor enabled (default)
    let dataset = get_test_dataset()?;

    // Try to register the dataset - should succeed
    let result = monitor.register_dataset(&dataset).await;
    assert!(result.is_ok());

    // Check that monitored_datasets contains the dataset
    let monitored_datasets = monitor.monitored_datasets.lock().await;
    assert_eq!(monitored_datasets.len(), 1);
    assert!(monitored_datasets.contains_key("datatypes"));

    Ok(())
}

#[tokio::test]
async fn dataset_availability_monitor_register_skipped_when_accelerated()
-> Result<(), anyhow::Error> {
    // Create a test runtime to get DataFusion instance
    let app = AppBuilder::new("dataset_availability_monitor_test")
        .with_dataset(get_test_dataset()?)
        .build();

    let rt = Runtime::builder().with_app(app).build().await;

    // Create DatasetsHealthMonitor directly
    let monitor = DatasetsHealthMonitor::new(Arc::clone(&rt.df));

    // Create dataset with acceleration enabled (which should skip monitoring)
    let mut dataset = get_test_dataset()?;
    dataset.acceleration = Some(spicepod::acceleration::Acceleration::default());

    // Try to register the dataset - should be skipped due to acceleration
    let result = monitor.register_dataset(&dataset).await;
    assert!(result.is_ok());

    // Check that monitored_datasets is empty
    let monitored_datasets = monitor.monitored_datasets.lock().await;
    assert!(monitored_datasets.is_empty());

    Ok(())
}

#[tokio::test]
async fn dataset_availability_monitor_disabled_when_builder_not_called() -> Result<(), anyhow::Error>
{
    let app = AppBuilder::new("dataset_availability_monitor_test")
        .with_dataset(get_test_dataset()?)
        .build();

    // Build runtime WITHOUT calling with_datasets_health_monitor
    let rt = Runtime::builder().with_app(app).build().await;

    // Monitor should be disabled since with_datasets_health_monitor wasn't called
    assert!(rt.datasets_health_monitor.is_none());
    Ok(())
}
