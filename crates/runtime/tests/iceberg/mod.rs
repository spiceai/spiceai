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

use crate::{get_test_datafusion, init_tracing, utils::test_request_context};
use anyhow::Context;
use app::AppBuilder;
use arrow::array::RecordBatch;
use futures::StreamExt;
use runtime::{status, Runtime};
use spicepod::component::{dataset::Dataset, params::Params as DatasetParams};
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::test]
async fn iceberg_integration_test_dataset() -> Result<(), anyhow::Error> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let _tracing = init_tracing(None);

    let account_id =
        std::env::var("AWS_ICEBERG_ACCOUNT_ID").context("AWS_ICEBERG_ACCOUNT_ID is not set")?;
    let region = std::env::var("AWS_ICEBERG_REGION").context("AWS_ICEBERG_REGION is not set")?;
    let _ = std::env::var("AWS_ACCESS_KEY_ID").context("AWS_ACCESS_KEY_ID is not set")?;
    let _ = std::env::var("AWS_SECRET_ACCESS_KEY").context("AWS_SECRET_ACCESS_KEY is not set")?;

    let from = format!("iceberg:https://glue.ap-northeast-2.amazonaws.com/iceberg/v1/catalogs/{account_id}/namespaces/tpch_sf1/tables/customer");
    let mut dataset = Dataset::new(from, "customer");
    dataset.params = Some(DatasetParams::from_string_map(HashMap::from([(
        "iceberg_s3_region".to_string(),
        region,
    )])));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("iceberg_dataset_test")
                .with_dataset(dataset)
                .build();

            let status = status::RuntimeStatus::new();
            let df = get_test_datafusion(Arc::clone(&status));

            let rt = Runtime::builder()
                .with_app(app)
                .with_datafusion(df)
                .with_runtime_status(status)
                .build()
                .await;

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                    panic!("Timeout waiting for components to load");
                }
                () = rt.load_components() => {}
            }

            let mut result = rt
                .datafusion()
                .query_builder("SELECT * FROM customer LIMIT 10")
                .build()
                .run()
                .await?;

            let mut results: Vec<RecordBatch> = vec![];
            while let Some(batch) = result.data.next().await {
                results.push(batch?);
            }

            assert_eq!(results.len(), 1);
            assert_eq!(results[0].num_rows(), 10);

            Ok(())
        })
        .await
}
