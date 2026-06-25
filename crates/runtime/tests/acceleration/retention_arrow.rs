/*
Copyright 2026 The Spice.ai OSS Authors

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
use arrow::array::RecordBatch;
use datafusion::assert_batches_eq;
use datafusion::common::TableReference;
use futures::TryStreamExt;
use runtime::Runtime;
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode},
    component::dataset::Dataset,
    partitioning::PartitionedBy,
};
use std::sync::Arc;

use crate::utils::{runtime_ready_check, test_request_context};

async fn run_query(rt: &Arc<Runtime>, sql: &str) -> Result<Vec<RecordBatch>, anyhow::Error> {
    rt.datafusion()
        .query_builder(sql)
        .build()
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("Query failed: {e}"))?
        .data
        .try_collect()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect results: {e}"))
}

async fn refresh_table(rt: &Arc<Runtime>, table_name: &str) -> Result<(), anyhow::Error> {
    let notifier = rt
        .datafusion()
        .refresh_table(&TableReference::from(table_name), None)
        .await?;
    notifier
        .ok_or_else(|| anyhow::anyhow!("Failed to refresh table"))?
        .notified()
        .await;
    Ok(())
}

fn make_dataset(name: &str, partition_by: Vec<PartitionedBy>) -> Result<Dataset, anyhow::Error> {
    let test_file = std::env::current_dir()
        .map_err(|e| anyhow::anyhow!("Failed to get current directory: {e}"))?
        .join("tests/acceleration/data/partition_test.csv");

    let mut dataset = Dataset::new(format!("file://{}", test_file.display()), name);
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("arrow".to_string()),
        mode: Mode::Memory,
        refresh_mode: Some(RefreshMode::Full),
        retention_sql: Some(format!("DELETE FROM {name} WHERE score < 90")),
        retention_check_enabled: false,
        retention_check_interval: None,
        partition_by,
        ..Acceleration::default()
    });
    Ok(dataset)
}

async fn assert_retention_sql_applies_on_refresh(
    dataset_name: &str,
    partition_by: Vec<PartitionedBy>,
) -> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            crate::configure_test_datafusion();

            let app = AppBuilder::new(dataset_name)
                .with_dataset(make_dataset(dataset_name, partition_by)?)
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                    return Err(anyhow::Error::msg("Timeout waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            runtime_ready_check(&rt).await;
            refresh_table(&rt, dataset_name).await?;

            let retained = run_query(
                &rt,
                &format!("SELECT id, score FROM {dataset_name} ORDER BY id"),
            )
            .await?;
            let expected = [
                "+----+-------+",
                "| id | score |",
                "+----+-------+",
                "| 2  | 92    |",
                "| 6  | 94    |",
                "| 10 | 90    |",
                "+----+-------+",
            ];
            assert_batches_eq!(&expected, &retained);

            let violating = run_query(
                &rt,
                &format!("SELECT id FROM {dataset_name} WHERE score < 90"),
            )
            .await?;
            let violating_count: usize = violating.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(
                violating_count, 0,
                "retention_sql should be applied during the Arrow refresh write path"
            );

            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_arrow_retention_sql_applies_on_refresh_without_retention_interval()
-> Result<(), anyhow::Error> {
    assert_retention_sql_applies_on_refresh("arrow_retention_write_path_test", Vec::new()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_partitioned_arrow_retention_sql_applies_on_refresh_without_retention_interval()
-> Result<(), anyhow::Error> {
    assert_retention_sql_applies_on_refresh(
        "partitioned_arrow_retention_write_path_test",
        vec![PartitionedBy {
            name: "expr0".to_string(),
            expression: "bucket(3, id)".to_string(),
        }],
    )
    .await
}
