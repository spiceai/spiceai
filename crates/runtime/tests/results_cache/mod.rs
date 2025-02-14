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
use arrow::array::RecordBatch;
use cache::QueryResultsCacheStatus;
use futures::TryStreamExt;
use runtime::{datafusion::query::QueryBuilder, status, Runtime};
use spicepod::component::{dataset::Dataset, params::Params, runtime::ResultsCache};

use crate::{get_test_datafusion, init_tracing, utils::test_request_context};

fn make_s3_tpch_dataset(name: &str) -> Dataset {
    let mut test_dataset = Dataset::new(
        format!("s3://spiceai-demo-datasets/tpch/{name}/").to_string(),
        name.to_string(),
    );
    test_dataset.params = Some(Params::from_string_map(
        vec![("file_format".to_string(), "parquet".to_string())]
            .into_iter()
            .collect(),
    ));

    test_dataset
}

#[tokio::test]
async fn results_cache_system_queries() -> Result<(), String> {
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            let results_cache = ResultsCache {
                item_ttl: Some("60s".to_string()),
                ..Default::default()
            };

            let app = AppBuilder::new("cache_test")
                .with_results_cache(results_cache)
                .with_dataset(make_s3_tpch_dataset("customer"))
                .build();

            let status = status::RuntimeStatus::new();
            let df = get_test_datafusion(Arc::clone(&status));

            let rt = Runtime::builder()
                .with_app(app)
                .with_datafusion(df)
                .build()
                .await;

            rt.load_components().await;

            assert!(execute_query_and_check_cache_status(
                &rt,
                "show tables",
                QueryResultsCacheStatus::CacheDisabled
            )
            .await
            .is_ok());
            assert!(execute_query_and_check_cache_status(
                &rt,
                "describe customer",
                QueryResultsCacheStatus::CacheDisabled
            )
            .await
            .is_ok());

            Ok(())
        })
        .await
}

async fn execute_query_and_check_cache_status(
    rt: &Runtime,
    query: &str,
    expected_cache_status: QueryResultsCacheStatus,
) -> Result<Vec<RecordBatch>, String> {
    let query = QueryBuilder::new(query, rt.datafusion()).build();

    let query_result = query
        .run()
        .await
        .map_err(|e| format!("Failed to execute query: {e}"))?;

    let records = query_result
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| format!("Failed to collect query results: {e}"))?;

    assert_eq!(query_result.results_cache_status, expected_cache_status);

    Ok(records)
}
