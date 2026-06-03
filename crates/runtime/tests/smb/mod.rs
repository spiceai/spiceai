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

use std::{collections::HashMap, sync::Arc, time::Duration};

use app::AppBuilder;
use arrow::{
    array::{Float64Array, Int64Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
};
use bollard::secret::HealthConfig;
use datafusion::{assert_batches_eq, parquet::arrow::ArrowWriter};
use futures::TryStreamExt;
use runtime::Runtime;
use spicepod::{component::dataset::Dataset, param::Params as DatasetParams};
use tracing::instrument;

use crate::{
    configure_test_datafusion,
    docker::{ContainerRunnerBuilder, RunningContainer, is_docker_available, wait_for_tcp_port},
    init_tracing,
    utils::{register_test_connectors, runtime_ready_check, test_request_context},
};

const SMB_IMAGE: &str = "docker.io/dperson/samba:latest";
const SMB_DOCKER_CONTAINER: &str = "runtime-integration-test-smb";
const SMB_HOST_PORT: u16 = 13445;
const SMB_CONTAINER_START_TIMEOUT: Duration = Duration::from_secs(60);
const SMB_HOST_PORT_READY_TIMEOUT: Duration = Duration::from_secs(30);

/// Write a simple 3-row Parquet file into `dir` and return its path.
#[instrument]
fn write_test_parquet(dir: &std::path::Path) -> Result<std::path::PathBuf, anyhow::Error> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));

    let ids = Int64Array::from(vec![1_i64, 2, 3]);
    let names = StringArray::from(vec!["alice", "bob", "carol"]);
    let values = Float64Array::from(vec![1.1_f64, 2.2, 3.3]);

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(ids), Arc::new(names), Arc::new(values)],
    )?;

    let file_path = dir.join("test_data.parquet");
    let file = std::fs::File::create(&file_path)?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None)?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(file_path)
}

#[instrument]
async fn start_samba_docker_container(
    tmpdir: &str,
) -> Result<RunningContainer<'static>, anyhow::Error> {
    let running_container = ContainerRunnerBuilder::new(SMB_DOCKER_CONTAINER)
        .image(SMB_IMAGE.to_string())
        .add_port_binding(445, SMB_HOST_PORT)
        .add_env_var("USERID", "0")
        .add_env_var("GROUPID", "0")
        .add_bind_mount(tmpdir, "/share")
        .healthcheck(HealthConfig {
            test: Some(vec!["CMD-SHELL".to_string(), "echo ok".to_string()]),
            interval: Some(1_000_000_000), // 1s
            timeout: Some(5_000_000_000),  // 5s
            retries: Some(60),
            start_period: Some(5_000_000_000), // 5s
            start_interval: None,
        })
        .command([
            "-u",
            "testuser;testpass",
            "-s",
            "data;/share;yes;no;no;testuser",
            "-p",
        ])
        .build()?
        .run(Some(SMB_CONTAINER_START_TIMEOUT))
        .await?;

    wait_for_tcp_port("127.0.0.1", SMB_HOST_PORT, SMB_HOST_PORT_READY_TIMEOUT).await?;

    Ok(running_container)
}

fn make_smb_dataset(name: &str) -> Dataset {
    let mut dataset = Dataset::new(format!("smb://127.0.0.1/data/"), name.to_string());
    let params = HashMap::from([
        ("smb_user".to_string(), "testuser".to_string()),
        ("smb_pass".to_string(), "testpass".to_string()),
        ("smb_port".to_string(), SMB_HOST_PORT.to_string()),
        ("file_format".to_string(), "parquet".to_string()),
    ]);
    dataset.params = Some(DatasetParams::from_string_map(params));
    dataset
}

#[tokio::test]
async fn smb_integration_test() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            if !is_docker_available().await {
                tracing::warn!("Docker not available, skipping SMB integration test");
                return Ok(());
            }

            // Write a Parquet file into a temp directory that will be bind-mounted into the
            // Samba container as the "data" share.
            let tmp_dir = tempfile::TempDir::new().map_err(|e| {
                tracing::error!("Failed to create temp dir: {e}");
                e.to_string()
            })?;
            let tmp_path = tmp_dir.path().to_string_lossy().to_string();

            write_test_parquet(tmp_dir.path()).map_err(|e| {
                tracing::error!("Failed to write test parquet: {e}");
                e.to_string()
            })?;

            let _container = start_samba_docker_container(&tmp_path).await.map_err(|e| {
                tracing::error!("start_samba_docker_container: {e}");
                e.to_string()
            })?;
            tracing::debug!("Samba container started");

            let app = AppBuilder::new("smb_integration_test")
                .with_dataset(make_smb_dataset("smb_test"))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(60)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Verify row count.
            let count_results: Vec<RecordBatch> = rt
                .datafusion()
                .query_builder("SELECT COUNT(*) AS cnt FROM smb_test")
                .build()
                .run()
                .await
                .map_err(|e| format!("COUNT query failed: {e}"))?
                .data
                .try_collect()
                .await
                .map_err(|e| format!("COUNT query stream error: {e}"))?;

            assert_batches_eq!(
                &["+-----+", "| cnt |", "+-----+", "| 3   |", "+-----+",],
                &count_results
            );

            // Spot-check ordered rows.
            let row_results: Vec<RecordBatch> = rt
                .datafusion()
                .query_builder("SELECT id, name, value FROM smb_test ORDER BY id")
                .build()
                .run()
                .await
                .map_err(|e| format!("SELECT query failed: {e}"))?
                .data
                .try_collect()
                .await
                .map_err(|e| format!("SELECT query stream error: {e}"))?;

            let all_rows: usize = row_results.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(all_rows, 3, "expected 3 rows, got {all_rows}");

            _container.remove().await.map_err(|e| {
                tracing::error!("container.remove: {e}");
                e.to_string()
            })?;

            Ok(())
        })
        .await
}
