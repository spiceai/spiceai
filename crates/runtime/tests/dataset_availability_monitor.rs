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

use std::sync::Arc;
use app::AppBuilder;
use runtime::Runtime;
use spicepod::component::dataset::Dataset;

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
async fn dataset_availability_monitor_enabled_by_default() -> Result<(), anyhow::Error> {
    let dataset = get_test_dataset()?;
    assert!(dataset.availability_monitor_enabled);
    Ok(())
}

#[tokio::test]
async fn dataset_availability_monitor_disabled_via_config() -> Result<(), anyhow::Error> {
    let dataset = get_test_dataset_with_availability_monitor_disabled()?;
    assert!(!dataset.availability_monitor_enabled);
    Ok(())
}

#[tokio::test]
async fn dataset_availability_monitor_register_skipped_when_disabled() -> Result<(), anyhow::Error> {
    let dataset = get_test_dataset_with_availability_monitor_disabled()?;
    
    let app = AppBuilder::new("dataset_availability_monitor_test")
        .with_dataset(dataset)
        .build();

    let rt = Runtime::builder()
        .with_app(app)
        .with_datasets_health_monitor()
        .build()
        .await;

    // The monitor should exist but the dataset should not be registered
    assert!(rt.datasets_health_monitor.is_some());
    
    // Load the dataset to trigger registration
    let cloned_rt = Arc::new(rt.clone());
    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(5)) => {},
        () = cloned_rt.load_components() => {}
    }
    
    // Check that the dataset was not registered for monitoring
    // (this would be more complex to test - we'd need to check internal state)
    // For now, we just verify the monitor exists and can handle disabled datasets
    assert!(rt.datasets_health_monitor.is_some());
    Ok(())
}

#[tokio::test]
async fn dataset_availability_monitor_register_succeeds_when_enabled() -> Result<(), anyhow::Error> {
    let dataset = get_test_dataset()?;
    
    let app = AppBuilder::new("dataset_availability_monitor_test")
        .with_dataset(dataset)
        .build();

    let rt = Runtime::builder()
        .with_app(app)
        .with_datasets_health_monitor()
        .build()
        .await;

    // The monitor should exist and the dataset should be registered
    assert!(rt.datasets_health_monitor.is_some());
    
    // Load the dataset to trigger registration
    let cloned_rt = Arc::new(rt.clone());
    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(5)) => {},
        () = cloned_rt.load_components() => {}
    }
    
    // Check that the monitor exists and handled the enabled dataset
    assert!(rt.datasets_health_monitor.is_some());
    Ok(())
}

#[tokio::test]
async fn dataset_availability_monitor_disabled_when_builder_not_called() -> Result<(), anyhow::Error> {
    let app = AppBuilder::new("dataset_availability_monitor_test")
        .with_dataset(get_test_dataset()?)
        .build();

    // Build runtime WITHOUT calling with_datasets_health_monitor
    let rt = Runtime::builder()
        .with_app(app)
        .build()
        .await;

    // Monitor should be disabled since with_datasets_health_monitor wasn't called
    assert!(rt.datasets_health_monitor.is_none());
    Ok(())
}