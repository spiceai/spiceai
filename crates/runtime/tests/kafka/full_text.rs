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

use std::sync::Arc;
use std::time::Duration;

use app::AppBuilder;
use runtime::Runtime;
use spicepod::semantic::{Column, FullTextSearchConfig};

use super::bootstrap::{make_kafka_dataset, send_messages_to_kafka, start_kafka_docker_container};
use tokio::time::sleep;

use super::run_and_snapshot_query;
use crate::configure_test_datafusion;
use crate::utils::runtime_ready_check;
use crate::{init_tracing, utils::test_request_context};

const KAFKA_PORT: u16 = 19094;

#[tokio::test]
async fn kafka_full_text_index() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (running_container, producer) =
                start_kafka_docker_container(KAFKA_PORT, &["stack_qa"]).await?;

            tracing::debug!("Container started");

            // Load test data for stack_qa
            let stack_qa_json: Vec<serde_json::Value> = stack_qa_json();
            send_messages_to_kafka(&producer, "stack_qa", &stack_qa_json).await?;

            let mut ds = make_kafka_dataset("stack_qa", "stack_qa", KAFKA_PORT, None);
            ds.columns =
                vec![Column::new("title").with_full_text_search(
                    FullTextSearchConfig::enabled().with_row_id("question_id"),
                )];
            let app = AppBuilder::new("kafka_full_text_index")
                .with_dataset(ds)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Ensure all messages are processed
            sleep(Duration::from_secs(2)).await;

            let table = "stack_qa";
            let data_snapshot = format!("{table}_data");

            run_and_snapshot_query(
                &rt,
                &format!("SELECT question_id, title FROM text_search({table}, 'gitignore untracked') ORDER BY score DESC LIMIT 10"),
                &data_snapshot,
            )
            .await?;

            rt.shutdown().await;
            drop(rt);

            // Clean up container after test
            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                anyhow::Error::msg(e.to_string())
            })?;

            Ok(())
        })
        .await
}

#[expect(clippy::expect_used)]
fn stack_qa_json() -> Vec<serde_json::Value> {
    include_str!("./test_data/stack_qa.json")
        .lines()
        .filter(|line| !line.trim().is_empty()) // skip blank lines
        .map(|line| serde_json::from_str(line).expect("Failed to parse JSON"))
        .collect()
}
