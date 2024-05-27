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

use std::sync::Arc;

use app::AppBuilder;
use arrow::array::RecordBatch;
use futures::TryStreamExt;
use runtime::{datafusion::DataFusion, Runtime};
use spicepod::component::{
    dataset::Dataset, params::Params, runtime::ResultsCache, secrets::SpiceSecretStore,
};
use tokio::sync::RwLock;

use crate::init_tracing;

fn create_test_dataset(name: &str) -> Dataset {
    let mut test_dataset = Dataset::new(
        "s3://spiceai-demo-datasets/tpch/customer/".to_string(),
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
    init_tracing(None);

    let results_cache = ResultsCache {
        item_ttl: Some("60s".to_string()),
        ..Default::default()
    };

    let app = AppBuilder::new("cache_test")
        .with_results_cache(results_cache)
        .with_secret_store(SpiceSecretStore::File)
        .with_dataset(create_test_dataset("customer"))
        .build();

    let df = Arc::new(RwLock::new(DataFusion::new()));

    let rt = Runtime::new(Some(app), df, Arc::new(vec![])).await;

    rt.init_results_cache().await;

    rt.load_secrets().await;
    rt.load_datasets().await;

    assert!(
        execute_query_and_check_cache_status(&rt, "show tables", None)
            .await
            .is_ok()
    );
    assert!(
        execute_query_and_check_cache_status(&rt, "describe customer", None)
            .await
            .is_ok()
    );

    Ok(())
}

async fn execute_query_and_check_cache_status(
    rt: &Runtime,
    query: &str,
    expected_cache_status: Option<bool>,
) -> Result<Vec<RecordBatch>, String> {
    let df = rt.df.read().await;

    let query_result = df
        .query_with_cache(query, None)
        .await
        .map_err(|e| format!("Failed to execute query: {e}"))?;

    let records = query_result
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| format!("Failed to collect query results: {e}"))?;

    assert_eq!(query_result.from_cache, expected_cache_status);

    Ok(records)
}
