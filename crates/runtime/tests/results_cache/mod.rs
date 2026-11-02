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

#[cfg(feature = "snapshots")]
mod snapshot_refresh;
mod stale_while_revalidate;

use std::sync::Arc;

use app::AppBuilder;
use arrow::array::RecordBatch;
use cache::result::CacheStatus;
use futures::TryStreamExt;

use runtime::{Runtime, datafusion::query::QueryBuilder};
use runtime_request_context::{CacheControl, Protocol, RequestContext};
use spicepod::{
    component::{
        caching::{CacheKeyType, SQLResultsCacheConfig},
        dataset::Dataset,
        view::View,
    },
    param::Params,
};

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{register_test_connectors, test_request_context, wait_until_true},
};

fn make_s3_tpch_dataset(name: &str) -> Dataset {
    let mut test_dataset = Dataset::new(
        format!("s3://spiceai-demo-datasets/tpch/{name}/"),
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
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let sql_results_cache = SQLResultsCacheConfig {
                item_ttl: Some("60s".to_string()),
                ..Default::default()
            };

            let app = AppBuilder::new("cache_test")
                .with_sql_cache(sql_results_cache)
                .with_dataset(make_s3_tpch_dataset("customer"))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            cloned_rt.load_components().await;

            execute_query_and_check_cache_status(&rt, "show tables", CacheStatus::CacheDisabled)
                .await
                .expect("should run query successfully");

            execute_query_and_check_cache_status(
                &rt,
                "describe customer",
                CacheStatus::CacheDisabled,
            )
            .await
            .expect("should run query successfully");

            Ok(())
        })
        .await
}

/// Empty result sets (queries that return zero rows) must be cached, so a
/// repeat request is served from cache instead of being re-executed against the
/// source.
///
/// This is a regression test for empty results not being cached. In
/// `DataFusion`, a filter that matches nothing yields **zero** record batches
/// (not a single zero-row batch) — the common shape for
/// `SELECT ... WHERE <key> = <absent>`, `WHERE 1 = 0`, and `LIMIT 0`. The
/// results cache previously keyed its "should I store this?" decision on the
/// batch count, so these were never cached and re-executed on every request.
#[tokio::test]
async fn results_cache_empty_result_sets() -> Result<(), String> {
    let _tracing = init_tracing(None);
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let sql_results_cache = SQLResultsCacheConfig {
                item_ttl: Some("60s".to_string()),
                ..Default::default()
            };

            let app = AppBuilder::new("cache_test")
                .with_sql_cache(sql_results_cache)
                .with_dataset(make_s3_tpch_dataset("customer"))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());
            cloned_rt.load_components().await;

            // Each query returns zero rows. Verify: first request misses, is
            // empty; second request hits, is still empty (served from cache).
            // `c_custkey = -1` covers the realistic "key not present" case; the
            // other two cover the `WHERE 1 = 0` / `LIMIT 0` shapes.
            for query in [
                "SELECT * FROM customer WHERE c_custkey = -1",
                "SELECT * FROM customer WHERE 1 = 0",
                "SELECT * FROM customer LIMIT 0",
            ] {
                let miss = execute_query_and_check_cache_status(&rt, query, CacheStatus::CacheMiss)
                    .await
                    .expect("first request should run successfully");
                assert_eq!(
                    miss.iter().map(RecordBatch::num_rows).sum::<usize>(),
                    0,
                    "query should return zero rows: {query}"
                );

                let hit = execute_query_and_check_cache_status(&rt, query, CacheStatus::CacheHit)
                    .await
                    .expect("repeat request should run successfully");
                assert_eq!(
                    hit.iter().map(RecordBatch::num_rows).sum::<usize>(),
                    0,
                    "cached query should still return zero rows: {query}"
                );
            }

            Ok(())
        })
        .await
}

async fn execute_query_and_check_cache_status(
    rt: &Runtime,
    query: &str,
    expected_cache_status: CacheStatus,
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

    assert_eq!(query_result.cache_status, expected_cache_status);

    Ok(records)
}

async fn count_of(rt: &Runtime, query: &str) -> Result<i64, String> {
    let batches = QueryBuilder::new(query, rt.datafusion())
        .build()
        .run()
        .await
        .map_err(|e| e.to_string())?
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| e.to_string())?;
    batches
        .first()
        .and_then(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
        })
        .map(|counts| counts.value(0))
        .ok_or_else(|| format!("no count in {batches:?}"))
}

/// A cached result keyed on the SQL text records the tables a view reads, not the
/// view, so editing or removing the view must still stop that result from being served.
#[tokio::test]
async fn results_cache_follows_a_redefined_or_removed_view() -> Result<(), String> {
    let _tracing = init_tracing(None);
    register_test_connectors().await;

    // An HTTP request to a runtime with `cache_key_type: sql` keys results on the SQL text.
    Arc::new(
        RequestContext::builder(Protocol::Http)
            .with_cache_control(CacheControl::Cache(
                runtime_request_context::CacheKeyType::Raw,
            ))
            .build(),
    )
    .scope(async {
        let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
        let csv = dir.path().join("values.csv");
        std::fs::write(&csv, "value\n5\n15\n25\n50\n75\n").map_err(|e| e.to_string())?;

        let app = |view_sql: Option<&str>| {
            let mut builder = AppBuilder::new("results_cache_view")
                .with_sql_cache(SQLResultsCacheConfig {
                    item_ttl: Some("10m".to_string()),
                    cache_key_type: CacheKeyType::Sql,
                    ..Default::default()
                })
                .with_dataset(Dataset::new(format!("file:{}", csv.display()), "t"));
            if let Some(sql) = view_sql {
                let mut view = View::new("v".to_string());
                view.sql = Some(sql.to_string());
                builder = builder.with_view(view);
            }
            builder.build()
        };

        configure_test_datafusion();
        let rt = Arc::new(
            Runtime::builder()
                .with_app(app(Some("SELECT * FROM t WHERE value > 10")))
                .build()
                .await,
        );
        Arc::clone(&rt).load_components().await;

        let query = "SELECT COUNT(*) FROM v";
        assert_eq!(count_of(&rt, query).await?, 4);
        execute_query_and_check_cache_status(&rt, query, CacheStatus::CacheHit).await?;

        assert!(
            Arc::clone(&rt)
                .apply_app(Arc::new(app(Some("SELECT * FROM t WHERE value > 50"))))
                .await
        );
        // The redefined view registers in the background after `apply_app` returns.
        assert!(
            wait_until_true(std::time::Duration::from_secs(30), || async {
                count_of(&rt, query).await.is_ok()
            })
            .await,
            "the redefined view must register"
        );
        assert_eq!(
            count_of(&rt, query).await?,
            1,
            "the redefined view must not be answered from the old definition's result"
        );

        assert!(Arc::clone(&rt).apply_app(Arc::new(app(None))).await);
        let removed = count_of(&rt, query).await;
        assert!(
            removed.is_err(),
            "a removed view must not be answered from the cache: {removed:?}"
        );

        rt.shutdown().await;
        Ok(())
    })
    .await
}
