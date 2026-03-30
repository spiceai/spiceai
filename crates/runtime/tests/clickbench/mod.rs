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

use app::AppBuilder;
use arrow::array::RecordBatch;
use futures::TryStreamExt;

use runtime::Runtime;
use spicepod::{acceleration::Acceleration, component::dataset::Dataset};

use crate::utils::{register_test_connectors, runtime_ready_check};
use crate::{configure_test_datafusion, init_tracing, utils::test_request_context};

mod q8;

fn get_hits_small_accelerated_dataset() -> Dataset {
    let mut dataset = Dataset::new(
        "https://spiceai-public-datasets.s3.us-east-1.amazonaws.com/clickbench/hits_small.parquet"
            .to_string(),
        "hits",
    );
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("duckdb".to_string()),
        ..Acceleration::default()
    });
    dataset
}

async fn test_clickbench_query(query: &str) -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("test_clickbench_query")
                .with_dataset(get_hits_small_accelerated_dataset())
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let query_result = rt
                .datafusion()
                .query_builder(&format!("EXPLAIN VERBOSE {query}"))
                .build()
                .run()
                .await?;
            let explain_plan = query_result.data.try_collect::<Vec<RecordBatch>>().await?;
            let explain_plan_display = arrow::util::pretty::pretty_format_batches(&explain_plan)?;
            // No need to snapshot here, the benchmark tests take care of that.
            tracing::info!("{explain_plan_display}");

            let query_result = rt.datafusion().query_builder(query).build().run().await?;
            let _ = query_result.data.try_collect::<Vec<RecordBatch>>().await?;

            Ok(())
        })
        .await
}
