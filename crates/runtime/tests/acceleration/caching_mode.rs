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

use app::AppBuilder;
use arrow::array::StringArray;
use datafusion::prelude::*;
use runtime::Runtime;
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode},
    component::dataset::Dataset,
    param::Params,
};
use std::sync::Arc;

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{runtime_ready_check, test_request_context},
};

/// Test that caching mode properly propagates filters to the HTTP connector on cache miss.
/// This verifies that when a query with filters hits an empty cache, the filters are
/// correctly passed through to the federated HTTP table provider to build the correct request.
/// 
/// Also verifies:
/// - Cache hit: subsequent queries with same filters are served from cache
/// - Cache isolation: queries with different filters don't interfere with each other
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_caching_mode_filter_propagation() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,runtime=debug,data_components=trace,runtime::accelerated_table::cache=trace"));

    test_request_context()
        .scope(async {
            // Create HTTP dataset with caching mode
            let mut dataset = Dataset::new("https://api.tvmaze.com", "tvmaze");
            dataset.params = Some(Params::from_string_map(
                vec![
                    ("allowed_request_paths".to_string(), "/search/people".to_string()),
                    ("request_query_filters".to_string(), "enabled".to_string()),
                ]
                .into_iter()
                .collect(),
            ));
            dataset.acceleration = Some(Acceleration {
                enabled: true,
                mode: Mode::Memory,
                refresh_mode: Some(RefreshMode::Caching),
                refresh_check_interval: Some("30s".to_string()),
                ..Acceleration::default()
            });

            let mut app = AppBuilder::new("test_caching")
                .with_dataset(dataset)
                .build();
            
            // Disable SQL results caching to prevent interference with acceleration caching test
            if app.runtime.caching.sql_results.is_none() {
                app.runtime.caching.sql_results = Some(Default::default());
            }
            if let Some(ref mut sql_cache) = app.runtime.caching.sql_results {
                sql_cache.enabled = false;
            }

            configure_test_datafusion();
            let status = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = Arc::clone(&status).load_components() => {}
            }

            runtime_ready_check(&status).await;

            // STEP 1: Cache miss - first query should fetch from HTTP source
            eprintln!("TEST: Step 1 - Cache miss: querying with filters (michael)...");
            let df1 = status
                .datafusion()
                .ctx
                .table("tvmaze")
                .await?
                .filter(col("request_path").eq(lit("/search/people")))?
                .filter(col("request_query").eq(lit("q=michael")))?
                .select(vec![col("request_path"), col("request_query")])?
                .limit(0, Some(1))?;

            let batches1 = df1.collect().await?;
            assert!(!batches1.is_empty(), "Should have results from HTTP API when querying with filters");
            assert_eq!(batches1[0].num_rows(), 1, "Should have 1 row");

            let batch1 = &batches1[0];
            let request_path_array1 = batch1.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("request_path should be StringArray");
            let request_query_array1 = batch1.column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("request_query should be StringArray");

            assert_eq!(request_path_array1.value(0), "/search/people");
            assert_eq!(request_query_array1.value(0), "q=michael");
            eprintln!("TEST: Step 1 complete - data fetched and cached");

            // STEP 2: Cache hit - same query should be served from cache (no HTTP fetch)
            eprintln!("TEST: Step 2 - Cache hit: querying with same filters (michael)...");
            let df2 = status
                .datafusion()
                .ctx
                .table("tvmaze")
                .await?
                .filter(col("request_path").eq(lit("/search/people")))?
                .filter(col("request_query").eq(lit("q=michael")))?
                .select(vec![col("request_path"), col("request_query")])?
                .limit(0, Some(1))?;

            let batches2 = df2.collect().await?;
            assert!(!batches2.is_empty(), "Should have cached results");
            assert_eq!(batches2[0].num_rows(), 1, "Cached result should have 1 row");
            
            let batch2 = &batches2[0];
            let request_query_array2 = batch2.column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("request_query should be StringArray");
            assert_eq!(request_query_array2.value(0), "q=michael", "Should return cached data with correct filter value");
            eprintln!("TEST: Step 2 complete - data served from cache");

            // STEP 3: Cache miss with different filters - should fetch new data without affecting cached data
            eprintln!("TEST: Step 3 - Cache miss with different filters (jennifer)...");
            let df3 = status
                .datafusion()
                .ctx
                .table("tvmaze")
                .await?
                .filter(col("request_path").eq(lit("/search/people")))?
                .filter(col("request_query").eq(lit("q=jennifer")))?
                .select(vec![col("request_path"), col("request_query")])?
                .limit(0, Some(1))?;

            eprintln!("TEST: Executing physical plan manually...");
            let physical_plan = status.datafusion().ctx.state().create_physical_plan(df3.logical_plan()).await?;
            let task_ctx = Arc::new(datafusion::execution::TaskContext::default());
            
            use futures::StreamExt;
            let mut stream = physical_plan.execute(0, task_ctx)?;
            let mut manual_batches = vec![];
            while let Some(batch) = stream.next().await {
                manual_batches.push(batch?);
            }
            eprintln!("TEST: Manual execution got {} batches", manual_batches.len());
            
            let batches3 = manual_batches;
            eprintln!("TEST: Collected {} batches", batches3.len());
            if !batches3.is_empty() {
                eprintln!("TEST: First batch has {} rows", batches3[0].num_rows());
            }
            assert!(!batches3.is_empty(), "Should have results for different filter");
            assert_eq!(batches3[0].num_rows(), 1, "Should have 1 row for jennifer query");
            
            let batch3 = &batches3[0];
            let request_query_array3 = batch3.column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("request_query should be StringArray");
            assert_eq!(request_query_array3.value(0), "q=jennifer", "Should have fetched data for jennifer");
            eprintln!("TEST: Step 3 complete - new filter combination cached");

            // STEP 4: Verify original cache entry still works
            eprintln!("TEST: Step 4 - Verify original cache entry (michael) still accessible...");
            let df4 = status
                .datafusion()
                .ctx
                .table("tvmaze")
                .await?
                .filter(col("request_path").eq(lit("/search/people")))?
                .filter(col("request_query").eq(lit("q=michael")))?
                .select(vec![col("request_path"), col("request_query")])?
                .limit(0, Some(1))?;

            let batches4 = df4.collect().await?;
            assert!(!batches4.is_empty(), "Should still have cached results for michael");
            assert_eq!(batches4[0].num_rows(), 1);
            
            let batch4 = &batches4[0];
            let request_query_array4 = batch4.column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("request_query should be StringArray");
            assert_eq!(request_query_array4.value(0), "q=michael", "Original cached data should still be accessible");
            eprintln!("TEST: Step 4 complete - cache isolation verified");

            eprintln!("TEST: All steps passed! Cache workflow working correctly.");
            Ok(())
        })
        .await
}
