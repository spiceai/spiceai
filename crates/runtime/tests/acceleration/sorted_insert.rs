/*
Copyright 2025 The Spice.ai OSS Authors

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

//! Tests for sorted insert functionality across different accelerators.

use app::AppBuilder;
use datafusion::assert_batches_eq;
use runtime::Runtime;
use spicepod::{component::dataset::Dataset as SpicepodDataset, acceleration::Acceleration, param::Params};
use std::fs;
use std::path::PathBuf;

use crate::{configure_test_datafusion, init_tracing, utils::runtime_ready_check};

/// Test that Arrow accelerator properly sorts data when sort_columns is configured.
#[tokio::test]
async fn test_arrow_sorted_insert() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    // Create a temporary directory for test data
    let temp_dir = tempfile::tempdir()?;
    let data_file = temp_dir.path().join("sorted_data.csv");

    // Write initial CSV data (unsorted by timestamp)
    let csv_data = "id,value,timestamp\n\
                    1,first,5\n\
                    2,second,2\n\
                    3,third,8\n\
                    4,fourth,1\n\
                    5,fifth,4\n";
    
    fs::write(&data_file, csv_data)?;

    // Create dataset with Arrow acceleration and sort_columns
    let file_path = data_file.to_string_lossy().to_string();
    let mut dataset = SpicepodDataset::new(format!("file:{file_path}"), "sorted_data");
    
    let mut dataset_params = Params::default();
    dataset_params.insert("file_format".to_string(), "csv".to_string());
    dataset.params = Some(dataset_params);
    
    let mut accel_params = Params::default();
    accel_params.insert("sort_columns".to_string(), "timestamp".to_string());
    
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: None, // None means Arrow
        params: Some(accel_params),
        ..Default::default()
    });

    let app = AppBuilder::new("arrow_sorted_insert_test")
        .with_dataset(dataset)
        .build();

    configure_test_datafusion();
    let rt = Runtime::builder().with_app(app).build().await;

    rt.load_components().await;
    runtime_ready_check(&rt).await;

    // Wait for data to load and be sorted
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Query all data - should be sorted by timestamp
    let query_result = rt
        .datafusion()
        .query_builder("SELECT id, value, timestamp FROM sorted_data ORDER BY timestamp")
        .build()
        .run()
        .await?;

    let batches: Vec<_> = query_result.data.try_collect().await?;

    let expected = vec![
        "+----+--------+-----------+",
        "| id | value  | timestamp |",
        "+----+--------+-----------+",
        "| 4  | fourth | 1         |",
        "| 2  | second | 2         |",
        "| 5  | fifth  | 4         |",
        "| 1  | first  | 5         |",
        "| 3  | third  | 8         |",
        "+----+--------+-----------+",
    ];

    assert_batches_eq!(expected, &batches);

    // Test range query to verify sorted data
    let range_query = rt
        .datafusion()
        .query_builder("SELECT id, value, timestamp FROM sorted_data WHERE timestamp >= 2 AND timestamp <= 5 ORDER BY timestamp")
        .build()
        .run()
        .await?;

    let batches: Vec<_> = range_query.data.try_collect().await?;

    let expected = vec![
        "+----+--------+-----------+",
        "| id | value  | timestamp |",
        "+----+--------+-----------+",
        "| 2  | second | 2         |",
        "| 5  | fifth  | 4         |",
        "| 1  | first  | 5         |",
        "+----+--------+-----------+",
    ];

    assert_batches_eq!(expected, &batches);

    Ok(())
}
