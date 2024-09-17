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

use app::AppBuilder;
use futures::StreamExt;
use runtime::{datafusion::query::Protocol, Runtime};
use spicepod::component::{dataset::Dataset, params::Params};

use crate::init_tracing;

pub fn get_s3_dataset() -> Dataset {
    let mut dataset = Dataset::new("s3://spiceai-demo-datasets/taxi_trips/2024/", "taxi_trips");
    dataset.params = Some(Params::from_string_map(
        vec![
            ("file_format".to_string(), "parquet".to_string()),
            ("client_timeout".to_string(), "120s".to_string()),
        ]
        .into_iter()
        .collect(),
    ));
    dataset
}

#[tokio::test]
async fn s3_federation() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    let app = AppBuilder::new("s3_federation")
        .with_dataset(get_s3_dataset())
        .build();

    let rt = Runtime::builder().with_app(app).build().await;

    // Set a timeout for the test
    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
            return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
        }
        () = rt.load_components() => {}
    }

    let mut query_result = rt
        .datafusion()
        .query_builder("SELECT * FROM taxi_trips LIMIT 10", Protocol::Internal)
        .build()
        .run()
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    let mut batches = vec![];
    while let Some(batch) = query_result.data.next().await {
        batches.push(batch?);
    }

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 10);

    Ok(())
}
