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

//! A `refresh_mode: snapshot` check that finds no newer snapshot must leave
//! cached results in place (spiceai/spiceai#14421).
//!
//! A writer publishes one snapshot to a local `file://` store and stops, so no
//! newer snapshot can ever appear. The reader polls every second; each poll is
//! a refresh that changes nothing, and a cached query must still hit after
//! several of them.

use std::{path::Path, sync::Arc, time::Duration};

use app::AppBuilder;
use arrow::util::pretty::pretty_format_batches;
use cache::result::CacheStatus;
use datafusion::common::TableReference;
use futures::TryStreamExt;
use runtime::{Runtime, datafusion::query::QueryBuilder};
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode, RefreshOnStartup, SnapshotBehavior},
    component::{
        caching::SQLResultsCacheConfig,
        dataset::Dataset,
        snapshot::{BootstrapOnFailureBehavior, Snapshots},
    },
    param::Params,
};
use tempfile::TempDir;

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{register_test_connectors, runtime_ready_check, test_request_context, wait_until_true},
};

const DATASET: &str = "snapshot_cached";
const QUERY: &str = "SELECT id, name FROM snapshot_cached WHERE id = 2";
const SOURCE_CSV: &str = "id,name\n1,alpha\n2,bravo\n3,charlie\n";
const ROW: &str = "+----+-------+\n| id | name  |\n+----+-------+\n| 2  | bravo |\n+----+-------+";

fn dataset(source: &Path, local_dir: &Path, refresh_mode: RefreshMode) -> Dataset {
    let mut dataset = Dataset::new(format!("file://{}", source.display()), DATASET);
    dataset.params = Some(Params::from_string_map(
        [
            ("file_format".to_string(), "csv".to_string()),
            ("csv_has_header".to_string(), "true".to_string()),
        ]
        .into(),
    ));
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        mode: Mode::File,
        engine: Some("cayenne".to_string()),
        params: Some(Params::from_string_map(
            [
                (
                    "cayenne_file_path".to_string(),
                    local_dir.join("data").display().to_string(),
                ),
                (
                    "cayenne_metadata_dir".to_string(),
                    local_dir.join("metadata").display().to_string(),
                ),
            ]
            .into(),
        )),
        refresh_mode: Some(refresh_mode),
        refresh_check_interval: Some("1s".to_string()),
        refresh_on_startup: RefreshOnStartup::Auto,
        snapshots: SnapshotBehavior::Enabled,
        ..Acceleration::default()
    });
    dataset
}

fn snapshots(location: &Path) -> Snapshots {
    Snapshots {
        enabled: true,
        location: Some(format!("file://{}/", location.display())),
        bootstrap_on_failure_behavior: BootstrapOnFailureBehavior::Warn,
        params: None,
    }
}

async fn start(app: app::App) -> Arc<Runtime> {
    configure_test_datafusion();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    tokio::time::timeout(Duration::from_mins(2), Arc::clone(&rt).load_components())
        .await
        .expect("runtime components should load");
    runtime_ready_check(&rt).await;
    rt
}

/// Runs `QUERY` on `rt`, returning its rows as a table and its cache status.
async fn query(rt: &Runtime) -> (String, CacheStatus) {
    let result = QueryBuilder::new(QUERY, rt.datafusion())
        .build()
        .run()
        .await
        .expect("query should run");
    let cache_status = result.cache_status;
    let batches: Vec<_> = result.data.try_collect().await.expect("query results");
    let rows = pretty_format_batches(&batches)
        .expect("format query results")
        .to_string();
    (rows, cache_status)
}

#[tokio::test]
async fn snapshot_refresh_without_newer_snapshot_keeps_cached_results() {
    let _tracing = init_tracing(None);
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let dir = TempDir::new().expect("temp dir");
            let source = dir.path().join("source.csv");
            std::fs::write(&source, SOURCE_CSV).expect("write source csv");
            let store = dir.path().join("snapshots");
            std::fs::create_dir_all(&store).expect("create snapshot store");

            // Writer: load the source once, publish its snapshot, then stop.
            let writer = start(
                AppBuilder::new("snapshot_cache_writer")
                    .with_snapshots(snapshots(&store))
                    .with_dataset(dataset(
                        &source,
                        &dir.path().join("writer"),
                        RefreshMode::Full,
                    ))
                    .build(),
            )
            .await;
            let metadata = store.join("metadata.json");
            let published = wait_until_true(Duration::from_mins(1), || {
                let metadata = metadata.clone();
                async move {
                    std::fs::read_to_string(&metadata)
                        .is_ok_and(|m| m.contains("\"current-snapshot-id\""))
                }
            })
            .await;
            assert!(published, "the writer never published a snapshot");
            writer.shutdown().await;

            // Reader: bootstrap from that snapshot and poll every second.
            let reader = start(
                AppBuilder::new("snapshot_cache_reader")
                    .with_sql_cache(SQLResultsCacheConfig {
                        enabled: true,
                        item_ttl: Some("10m".to_string()),
                        ..Default::default()
                    })
                    .with_snapshots(snapshots(&store))
                    .with_dataset(dataset(
                        &source,
                        &dir.path().join("reader"),
                        RefreshMode::Snapshot,
                    ))
                    .build(),
            )
            .await;
            // Readiness can be reported a moment before the table is registered;
            // this test is about the cache, so it waits for the table itself.
            let table = TableReference::bare(DATASET);
            let df = reader.datafusion();
            assert!(
                wait_until_true(Duration::from_secs(30), || {
                    let registered = df.table_exists(&table);
                    async move { registered }
                })
                .await,
                "the reader never registered '{DATASET}'"
            );

            assert_eq!(
                query(&reader).await,
                (ROW.to_string(), CacheStatus::CacheMiss)
            );
            assert_eq!(
                query(&reader).await,
                (ROW.to_string(), CacheStatus::CacheHit)
            );

            // Several snapshot checks, none of which finds a newer snapshot. The
            // check interval is what is under test, so this waits on the clock.
            tokio::time::sleep(Duration::from_millis(3500)).await;
            assert_eq!(
                query(&reader).await,
                (ROW.to_string(), CacheStatus::CacheHit),
                "a snapshot check that found no newer snapshot cleared the cached result"
            );

            reader.shutdown().await;
        })
        .await;
}
