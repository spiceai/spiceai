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

//! Azure Blob File System (ABFS) connector tests.
//!
//! These tests verify the Azure Blob Storage integration, including patches from
//! the `spiceai/arrow-rs` fork that optimize Parquet reading on Azure.

use crate::{RecordBatch, init_tracing, utils::test_request_context};

use anyhow::anyhow;
use app::AppBuilder;
use azure_storage_blobs::prelude::*;
use bollard::secret::HealthConfig;
use datafusion::assert_batches_eq;
use futures::TryStreamExt;
use runtime::Runtime;
use spicepod::{component::dataset::Dataset, param::Params as DatasetParams};
use std::sync::Arc;
use tracing::instrument;

use crate::{
    configure_test_datafusion,
    docker::{ContainerRunnerBuilder, RunningContainer},
};

#[instrument]
pub async fn start_azurite_docker_container() -> Result<RunningContainer<'static>, anyhow::Error> {
    let running_container = ContainerRunnerBuilder::new("spice_test_azurite")
        .image("mcr.microsoft.com/azure-storage/azurite:latest".to_string())
        .add_port_binding(10001, 10001)
        .add_port_binding(10000, 10000)
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD-SHELL".to_string(),
                "netstat -tulpn | grep 10000".to_string(),
            ]),
            interval: Some(250_000_000), // 250ms
            timeout: Some(100_000_000),  // 100ms
            retries: Some(5),
            start_period: Some(500_000_000), // 100ms
            start_interval: None,
        })
        .build()?
        .run(None)
        .await?;

    tokio::time::sleep(std::time::Duration::from_millis(5000)).await;
    Ok(running_container)
}

pub async fn upload_sample_file() -> Result<(), anyhow::Error> {
    let container_client = ClientBuilder::emulator().container_client("testcontainer");
    container_client.create().await?;
    tracing::trace!("Storage container created");
    tracing::trace!("Uploading sample file");
    let sample_file = include_str!("../test_data/taxi_sample.csv");
    let blob_client = container_client.blob_client("taxi_sample.csv");

    blob_client
        .put_block_blob(sample_file)
        .content_type("text/csv")
        .await?;
    tracing::trace!("Sample file uploaded");
    Ok(())
}

pub async fn prepare_container() -> Result<RunningContainer<'static>, anyhow::Error> {
    let azurite_container = start_azurite_docker_container().await?;
    tracing::info!("Azurite container started");
    tracing::info!("Uploading sample file to Azure Blob Storage");
    match upload_sample_file().await {
        Ok(()) => Ok(azurite_container),
        Err(e) => {
            azurite_container.stop().await?;
            azurite_container.remove().await?;
            Err(e)
        }
    }
}

#[tokio::test]
async fn test_spice_with_abfs() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    tracing::info!("Starting AzureBlobFS connector test");
    let azurite_container = prepare_container().await?;

    let res = test_request_context().scope(run_queries()).await;
    tracing::info!("Test completed");
    azurite_container.stop().await?;
    azurite_container.remove().await?;
    res.map_err(|e| anyhow::anyhow!(e))
}

fn make_test_query(table_name: &str) -> String {
    format!("SELECT DISTINCT(\"VendorID\") FROM {table_name} ORDER BY \"VendorID\" DESC")
}

async fn run_queries() -> Result<(), anyhow::Error> {
    let mut emulator_dataset = Dataset::new("abfs://testcontainer/taxi_sample.csv", "emulator");
    let emulator_params = DatasetParams::from_string_map(
        vec![("abfs_use_emulator".to_string(), "true".to_string())]
            .into_iter()
            .collect(),
    );
    emulator_dataset.params = Some(emulator_params);

    let mut abfs_dataset = Dataset::new(
        "abfs://data/taxi_small_samples/taxi_sample.csv",
        "abfs_prefix",
    );
    let abfs_params = DatasetParams::from_string_map(
        vec![
            (
                "abfs_account".to_string(),
                "spiceaidemodatasets".to_string(),
            ),
            // `skip_signature` is required for Anonymous blob access
            ("abfs_skip_signature".to_string(), "true".to_string()),
        ]
        .into_iter()
        .collect(),
    );
    abfs_dataset.params = Some(abfs_params);

    let app = AppBuilder::new("azure_connector_test")
        .with_dataset(emulator_dataset)
        .with_dataset(abfs_dataset)
        .build();

    configure_test_datafusion();
    let rt = Runtime::builder().with_app(app).build().await;

    let cloned_rt = Arc::new(rt.clone());

    // Set a timeout for the test
    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
            return Err(anyhow!("Timed out waiting for datasets to load".to_string()));
        }
        () = cloned_rt.load_components() => {}
    }

    let queries = vec![
        ("abfs_prefix", make_test_query("abfs_prefix")),
        ("emulator", make_test_query("emulator")),
    ];

    let expected_results = [
        "+----------+",
        "| VendorID |",
        "+----------+",
        "| 2        |",
        "| 1        |",
        "+----------+",
    ];

    for (dataset_name, query) in queries {
        tracing::info!("Running query: {}", dataset_name);

        let query_result = rt
            .datafusion()
            .query_builder(&query)
            .build()
            .run()
            .await
            .map_err(|e| anyhow!(format!("query `{query}` to plan: {e}")))?;

        let data = query_result
            .data
            .try_collect::<Vec<RecordBatch>>()
            .await
            .map_err(|e| anyhow!(format!("query `{query}` to collect: {e}")))?;

        assert_batches_eq!(&expected_results, &data);
    }

    Ok(())
}

/// Test that verifies Parquet reading from Azure Blob Storage works correctly.
///
/// **Critical for**: `arrow-rs` fork (`spiceai/arrow-rs`, spiceai-57.2)
///
/// This test exercises the `ParquetObjectReader::new_with_meta` optimization added
/// in the arrow-rs fork. This optimization passes `ObjectMeta` directly to avoid
/// suffix range requests, which are not supported by Azure Blob Storage.
///
/// **What happens without the patch**: Reading Parquet files from Azure would fail
/// or be inefficient because Azure doesn't support suffix range requests (reading
/// from the end of a file). The patch uses `new_with_meta` which passes the file
/// size directly, avoiding the need for suffix requests.
///
/// **Patches tested**:
/// - `ParquetObjectReader::new_with_meta` constructor
/// - Azure-specific handling in data connectors
#[tokio::test]
async fn test_azure_parquet_reading_with_object_meta() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    tracing::info!("Starting Azure Parquet optimization test (new_with_meta)");
    let azurite_container = prepare_container().await?;

    let res = test_request_context()
        .scope(run_parquet_query_with_meta())
        .await;
    tracing::info!("Test completed");
    azurite_container.stop().await?;
    azurite_container.remove().await?;
    res.map_err(|e| anyhow::anyhow!(e))
}

async fn upload_parquet_file() -> Result<(), anyhow::Error> {
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::parquet::arrow::ArrowWriter;

    let container_client = ClientBuilder::emulator().container_client("testcontainer");

    // Create a simple parquet file in memory
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let batch =
        arrow::record_batch::RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array)])?;

    let mut buffer = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buffer, Arc::clone(&schema), None)?;
        writer.write(&batch)?;
        writer.close()?;
    }

    let blob_client = container_client.blob_client("test_data.parquet");
    blob_client
        .put_block_blob(buffer)
        .content_type("application/octet-stream")
        .await?;

    tracing::trace!("Parquet file uploaded to Azure");
    Ok(())
}

async fn run_parquet_query_with_meta() -> Result<(), anyhow::Error> {
    // First upload a parquet file
    upload_parquet_file().await?;

    let mut emulator_dataset =
        Dataset::new("abfs://testcontainer/test_data.parquet", "azure_parquet");
    let emulator_params = DatasetParams::from_string_map(
        vec![
            ("abfs_use_emulator".to_string(), "true".to_string()),
            ("file_format".to_string(), "parquet".to_string()),
        ]
        .into_iter()
        .collect(),
    );
    emulator_dataset.params = Some(emulator_params);

    let app = AppBuilder::new("azure_parquet_test")
        .with_dataset(emulator_dataset)
        .build();

    configure_test_datafusion();
    let rt = Runtime::builder().with_app(app).build().await;
    let cloned_rt = Arc::new(rt.clone());

    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
            return Err(anyhow!("Timed out waiting for datasets to load"));
        }
        () = cloned_rt.load_components() => {}
    }

    // Query the parquet file - this exercises the new_with_meta optimization
    // because Azure doesn't support suffix range requests
    let query = "SELECT * FROM azure_parquet ORDER BY id";
    let query_result = rt
        .datafusion()
        .query_builder(query)
        .build()
        .run()
        .await
        .map_err(|e| anyhow!(format!("query to plan: {e}")))?;

    let data = query_result
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| anyhow!(format!("query to collect: {e}")))?;

    let expected = [
        "+----+", "| id |", "+----+", "| 1  |", "| 2  |", "| 3  |", "| 4  |", "| 5  |", "+----+",
    ];
    assert_batches_eq!(&expected, &data);

    tracing::info!(
        "Azure Parquet reading test passed - new_with_meta optimization working correctly"
    );

    Ok(())
}
