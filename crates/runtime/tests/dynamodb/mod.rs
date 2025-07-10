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
use futures::StreamExt;

use runtime::Runtime;
use spicepod::{component::dataset::Dataset, param::Params};

use crate::{configure_test_datafusion, init_tracing, utils::test_request_context};

#[tokio::test]
async fn dynamodb_federated() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("dynamodb_federated")
                .with_dataset(get_test_dataset(
                    "dynamodb:sales_transactions",
                    "test.sales_transactions",
                ))
                .build();

            let rt = Runtime::builder()
                .with_app(app)
                .with_datafusion_configuration_fn(configure_test_datafusion)
                .build()
                .await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            // order of columns for dynamodb connector may vary, so we retrieve and sort them manually instead of using 'describe test.sales_transactions;'
            run_and_snapshot_query(
                &rt,
                "SELECT column_name, data_type, is_nullable \
                 FROM information_schema.columns \
                 WHERE table_schema = 'test' \
                   AND table_name = 'sales_transactions' \
                 ORDER BY column_name;",
                "schema",
            ).await?;
            run_and_snapshot_query(
                &rt,
                "select customer_id, product_id, quantity, timestamp, total_amount, transaction_id from test.sales_transactions order by total_amount limit 5;",
                "query_result",
            )
            .await?;

            Ok(())
        })
        .await
}

async fn run_and_snapshot_query(
    rt: &Runtime,
    query: &str,
    test_name: &str,
) -> Result<(), anyhow::Error> {
    let mut query_result = rt
        .datafusion()
        .query_builder(query)
        .build()
        .run()
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    let mut batches = vec![];
    while let Some(batch) = query_result.data.next().await {
        batches.push(batch?);
    }

    let formatted = arrow::util::pretty::pretty_format_batches(&batches)
        .map_err(|e| anyhow::Error::msg(e.to_string()))?;
    insta::assert_snapshot!(test_name, formatted);
    Ok(())
}

fn get_test_dataset(from: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(from, name);
    dataset.params = Some(Params::from_string_map(
        vec![
            (
                "dynamodb_aws_region".to_string(),
                "ap-northeast-2".to_string(),
            ),
            (
                "dynamodb_aws_access_key_id".to_string(),
                "${ env:AWS_DYNAMODB_KEY }".to_string(),
            ),
            (
                "dynamodb_aws_secret_access_key".to_string(),
                "${ env:AWS_DYNAMODB_SECRET }".to_string(),
            ),
        ]
        .into_iter()
        .collect(),
    ));
    dataset
}
