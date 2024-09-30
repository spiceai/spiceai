/*
Copyright 2024 The Spice.ai OSS Authors

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

use crate::init_tracing;
use bollard::secret::HealthConfig;
use spicepod::component::{
    dataset::Dataset,
    params::Params as DatasetParams,
};
use anyhow::anyhow;
use app::AppBuilder;
use datafusion::assert_batches_eq;
use runtime::Runtime;
use std::path::PathBuf;
use azure_storage::prelude::*;
use azure_storage_blobs::prelude::*;
use tracing::instrument;

use crate::docker::{ContainerRunnerBuilder, RunningContainer};

#[instrument]
pub async fn start_azurite_docker_container() -> Result<RunningContainer<'static>, anyhow::Error> {
    let running_container = ContainerRunnerBuilder::new("spice_test_azurite")
        .image("mcr.microsoft.com/azure-storage/azurite:latest".to_string())
        .add_port_binding(10001, 10001)
        .add_port_binding(10000, 10000)
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD-SHELL".to_string(),
                "netstat -tulpn | grep 10000".to_string()
            ]),
            interval: Some(250_000_000), // 250ms
            timeout: Some(100_000_000),  // 100ms
            retries: Some(5),
            start_period: Some(500_000_000), // 100ms
            start_interval: None,
        })
        .build()?
        .run()
        .await?;

    tokio::time::sleep(std::time::Duration::from_millis(5000)).await;
    Ok(running_container)
}

pub async fn upload_sample_file() -> Result<(), anyhow::Error> {
    tracing::trace!("Creating container");
    let container_client = ClientBuilder::emulator().container_client("testcontainer");
    container_client.create().await?;
    tracing::trace!("Container created");
    tracing::trace!("Uploading sample file");
    let sample_file = std::fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/test_data/taxi_sample.csv"))?;
    let blob_client = container_client.blob_client("taxi_sample.csv");

    blob_client.put_block_blob(sample_file).content_type("text/csv").await?;
    tracing::trace!("Sample file uploaded");
    Ok(())
}

#[tokio::test]
async fn test_azure_connector() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=trace,debug,info"));
    tracing::info!("Starting Azure connector test");
    let azurite_container = start_azurite_docker_container().await?;
    tracing::info!("Azurite container started");
    let res = run_test().await;
    tracing::info!("Test completed");
    azurite_container.stop().await?;
    azurite_container.remove().await?;
    res.map_err(|e| anyhow::anyhow!(e))
}

fn make_test_query(table_name: &str) -> String {
    format!("SELECT DISTINCT(\"VendorID\") FROM {table_name} ORDER BY \"VendorID\" DESC")
}

async fn run_test() -> Result<(), anyhow::Error> {
    tracing::info!("Uploading sample file to Azure Blob Storage");
    upload_sample_file().await?;

    let mut emulator_dataset = Dataset::new("abfs://devstoreaccount1/testcontainer/taxi_sample.csv", "emulator");
    let emulator_params = DatasetParams::from_string_map(
            vec![
                ("use_emulator".to_string(), "true".to_string()),
            ]
            .into_iter()
            .collect(),
        );
    emulator_dataset.params = Some(emulator_params);

    let app = AppBuilder::new("azure_connector_test")
        .with_dataset(Dataset::new("azure://devstoreaccount1/testcontainer/taxi_sample.csv", "azure_prefix"))
        .with_dataset(Dataset::new("azure:http://localhost:10000/devstoreaccount1/testcontainer/taxi_sample.csv", "http_prefix"))
        .with_dataset(emulator_dataset)
        .build();

    let rt = Runtime::builder().with_app(app).build().await;

    // Set a timeout for the test
    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
            return Err(anyhow!("Timed out waiting for datasets to load".to_string()));
        }
        () = rt.load_components() => {}
    }

    let queries = vec![
        ("azure_prefix", make_test_query("azure_prefix")),
        ("http_prefix", make_test_query("http_prefix")),
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
        let data = rt
            .datafusion()
            .ctx
            .sql(&query)
            .await
            .map_err(|e| anyhow!(format!("query `{query}` to plan: {e}")))?
            .collect()
            .await
            .map_err(|e| anyhow!(format!("query `{query}` to results: {e}")))?;

        assert_batches_eq!(&expected_results, &data);
    }

    Ok(())
}
