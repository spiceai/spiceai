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
use arrow::compute::concat_batches;
use futures::StreamExt;

use runtime::Runtime;
use spicepod::{component::dataset::Dataset, param::Params};

use crate::{configure_test_datafusion, init_tracing, utils::test_request_context};

pub fn get_glue_dataset(s3_uri: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(s3_uri, name);
    dataset.params = Some(Params::from_string_map(
        vec![
            ("glue_region".to_string(), "ap-northeast-2".to_string()),
            ("glue_key".to_string(), "${ env:AWS_GLUE_KEY }".to_string()),
            (
                "glue_secret".to_string(),
                "${ env:AWS_GLUE_SECRET }".to_string(),
            ),
        ]
        .into_iter()
        .collect(),
    ));
    dataset
}

#[tokio::test]
async fn glue_federation() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("glue_federation")
                .with_dataset(get_glue_dataset("glue:testdb.iceberg_table_001", "iceberg"))
                .with_dataset(get_glue_dataset("glue:testdb.hive_table_001", "hive"))
                .build();

            let rt = Runtime::builder()
                .with_app(app)
                .with_datafusion_configuration_fn(configure_test_datafusion)
                .build()
                .await;

            let cloned_rt = Arc::new(rt.clone());

            // Set a timeout for the test
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            let mut query_result = rt
                .datafusion()
                .query_builder("SELECT * FROM hive AS h JOIN iceberg AS i ON h.id = i.id LIMIT 10")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let mut batches = vec![];
            while let Some(batch) = query_result.data.next().await {
                batches.push(batch?);
            }

            assert!(!batches.is_empty());

            let schema = batches[0].schema();
            let record = concat_batches(&schema, &batches).expect("concat batches");
            assert_eq!(record.num_rows(), 10);

            Ok(())
        })
        .await
}
