/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Runtime integration regression for SQL results-cache warmup:
//! record a plan → persist → restart → first refresh → ready → cache hit.

#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use app::AppBuilder;
use cache::result::CacheStatus;
use futures::StreamExt;
use runtime::Runtime;
use runtime_request_context::{
    CacheControl, CacheKeyType, CacheNamespace, Protocol, RequestContext, UserAgent,
};
use spicepod::acceleration::{Acceleration, Mode, RefreshMode};
use spicepod::component::caching::{ResultsCacheWarmup, SQLResultsCacheConfig};
use spicepod::component::dataset::Dataset;
use spicepod::component::runtime::{Runtime as SpicepodRuntime, RuntimeState, TaskHistory};

fn lookup_dataset(dir: &std::path::Path) -> Dataset {
    let csv = dir.join("lookup.csv");
    std::fs::write(&csv, "id,payload\n1,a\n2,b\n3,c\n").expect("write fixture");
    let mut dataset = Dataset::new(format!("file://{}", csv.display()), "lookup");
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("arrow".to_string()),
        mode: Mode::Memory,
        refresh_mode: Some(RefreshMode::Full),
        ..Acceleration::default()
    });
    dataset
}

async fn run_query(rt: &Arc<Runtime>, query: &str) -> CacheStatus {
    let result = rt
        .datafusion()
        .query_builder(query)
        .build()
        .run()
        .await
        .expect("query should run");
    let status = result.cache_status;
    let mut data = result.data;
    while let Some(batch) = data.next().await {
        batch.expect("stream");
    }
    status
}

async fn wait_ready(rt: &Arc<Runtime>) {
    tokio::time::timeout(Duration::from_secs(120), async {
        while !rt.status().is_ready() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("runtime should become ready");
}

#[tokio::test]
async fn warmup_record_persist_restart_refresh_ready_cache_hit() {
    let request_context = Arc::new(
        RequestContext::builder(Protocol::Internal)
            .with_user_agent(UserAgent::from_ua_str("spiceci/results_cache_warmup"))
            .with_cache_control(CacheControl::Cache(CacheKeyType::Default))
            .with_cache_namespace(CacheNamespace::Public)
            .build(),
    );

    request_context
        .scope(async {
            let dir = tempfile::tempdir().expect("tempdir");
            let state_dir = dir.path().join("state");
            std::fs::create_dir_all(&state_dir).expect("state dir");
            let fixture_dir = dir.path().join("data");
            std::fs::create_dir_all(&fixture_dir).expect("data dir");

            let sql_cache = SQLResultsCacheConfig {
                enabled: true,
                item_ttl: Some("10m".to_string()),
                max_size: Some("64MiB".to_string()),
                warmup: ResultsCacheWarmup::OnFirstRefresh,
                ..Default::default()
            };
            let runtime_cfg = SpicepodRuntime {
                task_history: TaskHistory {
                    enabled: false,
                    ..Default::default()
                },
                state: Some(RuntimeState {
                    location: format!("file://{}", state_dir.display()),
                    params: None,
                }),
                ..Default::default()
            };

            // Process A: load, query once to record a plan shape, wait for persist.
            {
                let app = AppBuilder::new("results_cache_warmup_record")
                    .with_runtime(runtime_cfg.clone())
                    .with_sql_cache(sql_cache.clone())
                    .with_dataset(lookup_dataset(&fixture_dir))
                    .build();
                let rt = Arc::new(Runtime::builder().with_app(app).build().await);
                tokio::time::timeout(Duration::from_secs(120), Arc::clone(&rt).load_components())
                    .await
                    .expect("load");
                wait_ready(&rt).await;

                let miss = run_query(&rt, "SELECT id FROM lookup WHERE id = 1").await;
                assert_eq!(miss, CacheStatus::CacheMiss, "first query records a plan");

                // Wait for async persist into runtime.state.
                let warmup_key = state_dir.join("results_cache_warmup.json");
                tokio::time::timeout(Duration::from_secs(30), async {
                    loop {
                        if warmup_key.is_file() {
                            break;
                        }
                        // ObjectState may nest under a prefix; search the state dir.
                        if std::fs::read_dir(&state_dir)
                            .map(|entries| {
                                entries.filter_map(Result::ok).any(|e| {
                                    e.file_name()
                                        .to_string_lossy()
                                        .contains("results_cache_warmup")
                                })
                            })
                            .unwrap_or(false)
                        {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                })
                .await
                .expect("warmup catalog should persist under runtime.state");

                rt.shutdown().await;
                // Drop runtime before restarting.
            }

            // Process B: restart with the same state location; warmup runs after
            // first refresh and readiness waits for it; then the shape hits.
            let app = AppBuilder::new("results_cache_warmup_replay")
                .with_runtime(runtime_cfg)
                .with_sql_cache(sql_cache)
                .with_dataset(lookup_dataset(&fixture_dir))
                .build();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            tokio::time::timeout(Duration::from_secs(120), Arc::clone(&rt).load_components())
                .await
                .expect("load after restart");
            wait_ready(&rt).await;

            let hit = run_query(&rt, "SELECT id FROM lookup WHERE id = 2").await;
            assert_eq!(
                hit,
                CacheStatus::CacheHit,
                "after restart + first refresh warmup, a distinct key of the recorded shape must hit"
            );

            rt.shutdown().await;
            Ok::<(), anyhow::Error>(())
        })
        .await
        .expect("scope");
}
