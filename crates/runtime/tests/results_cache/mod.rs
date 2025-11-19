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

use std::{sync::Arc, time::Duration};

use anyhow::Context;
use app::AppBuilder;
use arrow::{
    array::{Int64Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use cache::result::CacheStatus;
use datafusion::datasource::MemTable;
use futures::TryStreamExt;
use opentelemetry::global;
use opentelemetry_prometheus::exporter;
use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider};
use prometheus::{Registry, proto::MetricType};
use telemetry::noop::NoopMeterProvider;
use tokio::time::sleep;

use runtime::{
    Runtime,
    datafusion::{DataFusion, query::QueryBuilder},
};
use spicepod::{
    component::{caching::ResultsCache, dataset::Dataset},
    param::Params,
};

use crate::{configure_test_datafusion, init_tracing, utils::test_request_context};
use scopeguard::guard;

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

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            cloned_rt.load_components().await;

            assert!(
                execute_query_and_check_cache_status(
                    &rt,
                    "show tables",
                    CacheStatus::CacheDisabled
                )
                .await
                .is_ok()
            );
            assert!(
                execute_query_and_check_cache_status(
                    &rt,
                    "describe customer",
                    CacheStatus::CacheDisabled
                )
                .await
                .is_ok()
            );

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

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn results_cache_stale_while_revalidate_memory_protection_metrics()
-> Result<(), anyhow::Error> {
    const CACHE_MAX_SIZE_BYTES: u64 = 1024 * 1024; // 1 MiB
    const IN_PROGRESS_METRIC: &str = "results_cache_stale_while_revalidate_in_progress_size_bytes";
    const ABORTED_METRIC: &str = "results_cache_stale_while_revalidate_aborted_requests_total";

    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            let registry = Registry::new();
            let resource = Resource::default();
            let prometheus_exporter = exporter()
                .with_registry(registry.clone())
                .without_scope_info()
                .without_units()
                .without_counter_suffixes()
                .without_target_info()
                .build()
                .context("build prometheus exporter")?;
            let provider = SdkMeterProvider::builder()
                .with_resource(resource)
                .with_reader(prometheus_exporter)
                .build();
            global::set_meter_provider(provider);
            let _reset_provider = guard((), |()| {
                global::set_meter_provider(NoopMeterProvider::new());
            });

            configure_test_datafusion();

            let results_cache = ResultsCache {
                item_ttl: Some("0s".to_string()),
                max_stale_while_revalidate: Some("5s".to_string()),
                cache_max_size: Some("1MiB".to_string()),
                ..Default::default()
            };

            let app = AppBuilder::new("cache_guard_test")
                .with_results_cache(results_cache)
                .build();

            let rt = Runtime::builder().with_app(app).build().await;
            rt.init_cache_metrics();

            let df = rt.datafusion();
            let cache_limit_u32 =
                u32::try_from(CACHE_MAX_SIZE_BYTES).expect("cache max size fits u32");
            let cache_limit_bytes_f64 = f64::from(cache_limit_u32);
            // Create tables sized at ~75% of cache limit (Int64 = 8 bytes each)
            let rows_per_batch = (cache_limit_u32 * 3 / 4) / 8;
            let first_table_batch_bytes =
                register_large_table(&df, "cache_guard_large_a", rows_per_batch)
                    .context("register table A")?;
            let second_table_batch_bytes =
                register_large_table(&df, "cache_guard_large_b", rows_per_batch)
                    .context("register table B")?;

            assert!(first_table_batch_bytes < CACHE_MAX_SIZE_BYTES);
            assert!(second_table_batch_bytes < CACHE_MAX_SIZE_BYTES);
            assert!(first_table_batch_bytes > CACHE_MAX_SIZE_BYTES / 2);
            assert!(second_table_batch_bytes > CACHE_MAX_SIZE_BYTES / 2);

            let rt = Arc::new(rt);
            run_query(&rt, "SELECT * FROM cache_guard_large_a")
                .await
                .context("warm cache A")?;
            run_query(&rt, "SELECT * FROM cache_guard_large_b")
                .await
                .context("warm cache B")?;

            let rt_for_a = Arc::clone(&rt);
            let rt_for_b = Arc::clone(&rt);
            tokio::try_join!(
                async move {
                    run_query(&rt_for_a, "SELECT * FROM cache_guard_large_a")
                        .await
                        .map(|_| ())
                },
                async move {
                    run_query(&rt_for_b, "SELECT * FROM cache_guard_large_b")
                        .await
                        .map(|_| ())
                }
            )
            .context("trigger stale queries")?;

            let gauge_value = wait_for_metric_value(
                &registry,
                IN_PROGRESS_METRIC,
                MetricType::GAUGE,
                |value| value > 0.0,
                Duration::from_secs(5),
            )
            .await
            .context("expected in-progress metric to report usage")?;

            assert!(
                gauge_value <= cache_limit_bytes_f64,
                "gauge value {gauge_value} exceeded limit {CACHE_MAX_SIZE_BYTES}"
            );

            let aborted_value = wait_for_metric_value(
                &registry,
                ABORTED_METRIC,
                MetricType::COUNTER,
                |value| value >= 1.0,
                Duration::from_secs(5),
            )
            .await
            .context("expected aborted metric to increment")?;

            assert!(
                aborted_value >= 1.0,
                "aborted metric should be at least 1, got {aborted_value}"
            );

            Ok(())
        })
        .await
}

fn register_large_table(
    df: &Arc<DataFusion>,
    table_name: &str,
    rows: u32,
) -> Result<u64, anyhow::Error> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let values = Int64Array::from_iter_values((0..rows).map(i64::from));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values)])
        .context("create record batch")?;
    let batch_size =
        u64::try_from(batch.get_array_memory_size()).context("record batch size overflowed u64")?;
    let mem_table = MemTable::try_new(schema, vec![vec![batch]]).context("create memtable")?;
    df.ctx
        .register_table(table_name, Arc::new(mem_table))
        .context("register memtable with session")?;
    Ok(batch_size)
}

async fn run_query(rt: &Arc<Runtime>, query: &str) -> Result<Vec<RecordBatch>, anyhow::Error> {
    let query = QueryBuilder::new(query, rt.datafusion()).build();
    let query_result = query.run().await.context("execute query")?;
    let records = query_result
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .context("collect query results")?;
    Ok(records)
}

async fn wait_for_metric_value<F>(
    registry: &Registry,
    metric_name: &str,
    metric_type: MetricType,
    predicate: F,
    timeout: Duration,
) -> Option<f64>
where
    F: Fn(f64) -> bool,
{
    let deadline = std::time::Instant::now() + timeout;
    let mut now = std::time::Instant::now();
    while now < deadline {
        if let Some(value) = read_metric_value(registry, metric_name, metric_type)
            && predicate(value)
        {
            return Some(value);
        }
        sleep(Duration::from_millis(20)).await;
        now = std::time::Instant::now();
    }
    None
}

fn read_metric_value(
    registry: &Registry,
    metric_name: &str,
    metric_type: MetricType,
) -> Option<f64> {
    for family in registry.gather() {
        if family.get_name() == metric_name
            && family.get_field_type() == metric_type
            && let Some(metric) = family.get_metric().first()
        {
            return match metric_type {
                MetricType::GAUGE => Some(metric.get_gauge().get_value()),
                MetricType::COUNTER => Some(metric.get_counter().get_value()),
                _ => None,
            };
        }
    }
    None
}
