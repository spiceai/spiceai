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

#![allow(clippy::expect_used)]

//! Regression tests for localpod full-refresh synchronization.
//!
//! A `localpod:` child dataset has no refresh interval of its own; it stays current by
//! subscribing to its parent's refreshes. Setting that subscription up requires resolving the
//! parent's registered provider back to its [`AcceleratedTable`]. The parent's provider takes one
//! of two shapes depending on its acceleration engine:
//!
//! - Engines backed by a `PolyTableProvider` (duckdb/sqlite/postgres/cayenne) expose a federated
//!   source, so the parent is wrapped in a `FederatedTableProviderAdaptor`.
//! - The default in-memory Arrow accelerator has no federated source, so the parent is registered
//!   as a bare `AcceleratedTable`.
//!
//! Regression test for <https://github.com/spiceai/spiceai/issues/11137>: a child of an
//! Arrow-accelerated parent used to bail out of synchronization (only the
//! `FederatedTableProviderAdaptor` shape was handled), so the child loaded once at startup and then
//! stayed frozen while the parent kept refreshing.

use std::fmt::Write as _;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use app::{App, AppBuilder};
use arrow::array::{Int64Array, RecordBatch};
use cache::result::CacheStatus;
use futures::TryStreamExt;
use runtime::{Runtime, datafusion::query::QueryBuilder};
use spicepod::{
    acceleration::{Acceleration, RefreshMode},
    component::{caching::SQLResultsCacheConfig, dataset::Dataset},
    param::Params,
};
use tempfile::TempDir;
use tokio::fs;

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{runtime_ready_check, test_request_context},
};

const CSV_HEADER: &str = "id,name\n";

fn rows(start: usize, end: usize) -> String {
    (start..end).fold(String::new(), |mut acc, i| {
        let _ = writeln!(acc, "{i},name_{i}");
        acc
    })
}

async fn count_rows(rt: &Runtime, table: &str) -> usize {
    try_count_rows(rt, table)
        .await
        .expect("count query should plan against a registered table")
}

/// [`count_rows`] for a table that may not be queryable yet: `None` while the count query cannot
/// plan (the table is not registered) or cannot execute (its acceleration is still loading).
async fn try_count_rows(rt: &Runtime, table: &str) -> Option<usize> {
    let batches = rt
        .datafusion()
        .ctx
        .sql(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .ok()?
        .collect()
        .await
        .ok()?;
    let count = batches
        .first()
        .expect("count query should return a batch")
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("COUNT(*) should be an Int64Array")
        .value(0);
    Some(usize::try_from(count).expect("count should be non-negative"))
}

/// Poll until `table` is queryable and its row count reaches `expected`, or the timeout elapses,
/// returning the last observation: `None` when the table was still unregistered or loading.
async fn wait_for_registered_count(
    rt: &Runtime,
    table: &str,
    expected: usize,
    timeout: Duration,
) -> Option<usize> {
    let deadline = timeout.as_millis() / 100;
    let mut last = try_count_rows(rt, table).await;
    for _ in 0..deadline {
        if last == Some(expected) {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        last = try_count_rows(rt, table).await;
    }
    last
}

/// Poll `table`'s row count until it reaches `expected` or the timeout elapses, returning the last
/// observed count.
async fn wait_for_count(rt: &Runtime, table: &str, expected: usize, timeout: Duration) -> usize {
    let deadline = timeout.as_millis() / 100;
    let mut last = count_rows(rt, table).await;
    for _ in 0..deadline {
        if last == expected {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        last = count_rows(rt, table).await;
    }
    last
}

/// Runs `SELECT COUNT(*)` on `table` through the results-cache-aware query path
/// (unlike [`count_rows`], which queries the `SessionContext` directly and
/// bypasses the results cache), returning the observed cache status and count.
async fn cached_count(rt: &Runtime, table: &str) -> (CacheStatus, usize) {
    let sql = format!("SELECT COUNT(*) AS c FROM {table}");
    let query = QueryBuilder::new(&sql, rt.datafusion()).build();
    let result = query.run().await.expect("cached count query should run");
    let status = result.cache_status;
    let batches = result
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .expect("cached count query should collect");
    let count = batches
        .first()
        .expect("count query should return a batch")
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("COUNT(*) should be an Int64Array")
        .value(0);
    (
        status,
        usize::try_from(count).expect("count should be non-negative"),
    )
}

/// A parent refresh must invalidate cached query results for its localpod
/// children, not only for itself.
///
/// Regression test for <https://github.com/spiceai/spiceai/issues/12887>: after
/// a localpod child's initial load its refresher hands off to the parent's
/// refresh task, and refresh-completion cache invalidation only covered the
/// refreshing dataset's own name — queries against the child kept being served
/// pre-refresh results from the cache until `item_ttl` expired. The long TTL
/// here ensures TTL expiry cannot mask a missing invalidation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_localpod_refresh_invalidates_child_cached_results() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,runtime_table::accelerated=trace",
    ));

    test_request_context()
        .scope(async {
            let temp_dir = TempDir::new().expect("create temp dir");
            let csv_path = temp_dir.path().join("data.csv");
            fs::write(&csv_path, format!("{CSV_HEADER}{}", rows(0, 2)))
                .await
                .expect("write initial csv");

            let mut parent = Dataset::new(format!("file://{}", csv_path.display()), "time_series");
            parent.params = Some(Params::from_string_map(
                vec![
                    ("file_format".to_string(), "csv".to_string()),
                    ("csv_has_header".to_string(), "true".to_string()),
                ]
                .into_iter()
                .collect(),
            ));
            parent.acceleration = Some(Acceleration {
                enabled: true,
                refresh_mode: Some(RefreshMode::Full),
                ..Acceleration::default()
            });

            let mut child = Dataset::new("localpod:time_series", "local_time_series");
            child.acceleration = Some(Acceleration {
                enabled: true,
                refresh_mode: Some(RefreshMode::Full),
                ..Acceleration::default()
            });

            let app = AppBuilder::new("test_localpod_refresh_invalidates_child_cache")
                .with_sql_cache(SQLResultsCacheConfig {
                    item_ttl: Some("10m".to_string()),
                    ..Default::default()
                })
                .with_dataset(parent)
                .with_dataset(child)
                .build();

            configure_test_datafusion();
            let runtime = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(30)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = Arc::clone(&runtime).load_components() => {}
            }
            runtime_ready_check(&runtime).await;

            // Populate cache entries for both layers, then confirm they are served
            // from cache on repeat.
            for table in ["time_series", "local_time_series"] {
                let (status, count) = cached_count(&runtime, table).await;
                assert_eq!(status, CacheStatus::CacheMiss, "{table}: first query");
                assert_eq!(count, 2, "{table}: initial row count");
                let (status, count) = cached_count(&runtime, table).await;
                assert_eq!(status, CacheStatus::CacheHit, "{table}: repeat query");
                assert_eq!(count, 2, "{table}: cached row count");
            }

            // Change the source, then drive parent refreshes until both accelerators
            // hold the new data. The child subscribes to the parent's refreshes only
            // once its own startup load completes, so retry rather than assuming the
            // first refresh already fans out to the child.
            fs::write(&csv_path, format!("{CSV_HEADER}{}", rows(0, 5)))
                .await
                .expect("append rows to csv");

            let mut child_raw_count = count_rows(&runtime, "local_time_series").await;
            for _ in 0..15 {
                let notify = runtime
                    .datafusion()
                    .refresh_table(&"time_series".into(), None)
                    .await
                    .expect("trigger parent refresh");
                if let Some(notify) = notify {
                    tokio::select! {
                        _ = notify.wait() => {}
                        () = tokio::time::sleep(Duration::from_secs(5)) => {}
                    }
                }
                child_raw_count =
                    wait_for_count(&runtime, "local_time_series", 5, Duration::from_secs(3)).await;
                if child_raw_count == 5 {
                    break;
                }
            }
            assert_eq!(
                child_raw_count, 5,
                "localpod child accelerator should hold the refreshed rows"
            );

            // The parent's entry was always invalidated; sanity-check it first.
            let (_, parent_count) = cached_count(&runtime, "time_series").await;
            assert_eq!(
                parent_count, 5,
                "parent query should observe the refreshed rows"
            );

            // The child's cache entry must be invalidated by the same refresh.
            // Invalidation runs in the refresher's completion handler, which can lag
            // the data landing by a scheduling beat — poll the actual condition with
            // a bounded timeout instead of asserting the first read. Before the
            // #12887 fix the child keeps serving the stale cached count of 2 until
            // `item_ttl` (10m) and this poll exhausts its deadline.
            let mut child_count = 0;
            for _ in 0..50 {
                (_, child_count) = cached_count(&runtime, "local_time_series").await;
                if child_count == 5 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert_eq!(
                child_count, 5,
                "localpod child query should observe the refreshed rows within its \
                 cache TTL — stale cached results were served before the #12887 fix"
            );

            // The fresh result is itself cached again.
            let (status, count) = cached_count(&runtime, "local_time_series").await;
            assert_eq!(status, CacheStatus::CacheHit, "child repeat after refresh");
            assert_eq!(count, 5, "child cached row count after refresh");

            Ok(())
        })
        .await
}

/// A localpod child whose parent uses the default in-memory (Arrow) accelerator must keep tracking
/// the parent's full refreshes at runtime, not just load once at startup.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_localpod_full_refresh_synchronization_with_arrow_parent() -> Result<(), anyhow::Error>
{
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,runtime_table::accelerated=trace",
    ));

    test_request_context()
        .scope(async {
            // Start with the header plus two rows so the CSV schema is well-defined and the
            // initial load is non-empty (initial sync is known to work; runtime sync is the bug).
            let temp_dir = TempDir::new().expect("create temp dir");
            let csv_path = temp_dir.path().join("data.csv");
            fs::write(&csv_path, format!("{CSV_HEADER}{}", rows(0, 2)))
                .await
                .expect("write initial csv");

            // Parent: file connector, full refresh, default (Arrow / in-memory) accelerator.
            // No refresh_check_interval, so the parent only refreshes when triggered manually,
            // keeping the test deterministic.
            let mut parent = Dataset::new(format!("file://{}", csv_path.display()), "time_series");
            parent.params = Some(Params::from_string_map(
                vec![
                    ("file_format".to_string(), "csv".to_string()),
                    ("csv_has_header".to_string(), "true".to_string()),
                ]
                .into_iter()
                .collect(),
            ));
            parent.acceleration = Some(Acceleration {
                enabled: true,
                refresh_mode: Some(RefreshMode::Full),
                ..Acceleration::default()
            });

            // Child: localpod over the parent, also Arrow + full refresh, with no refresh interval
            // of its own — it must rely on synchronization with the parent.
            let mut child = Dataset::new("localpod:time_series", "local_time_series");
            child.acceleration = Some(Acceleration {
                enabled: true,
                refresh_mode: Some(RefreshMode::Full),
                ..Acceleration::default()
            });

            let app = AppBuilder::new("test_localpod_full_refresh_sync")
                .with_dataset(parent)
                .with_dataset(child)
                .build();

            configure_test_datafusion();
            let runtime = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(30)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = Arc::clone(&runtime).load_components() => {}
            }
            runtime_ready_check(&runtime).await;

            // Initial load: both parent and child see the two startup rows.
            assert_eq!(
                count_rows(&runtime, "time_series").await,
                2,
                "parent should load the two startup rows"
            );
            assert_eq!(
                count_rows(&runtime, "local_time_series").await,
                2,
                "localpod child should load the two startup rows"
            );

            // Append three more rows, for a total of five.
            fs::write(&csv_path, format!("{CSV_HEADER}{}", rows(0, 5)))
                .await
                .expect("append rows to csv");

            // Drive parent refreshes until the child catches up. The child subscribes to the
            // parent's refreshes only once its own startup load completes; retrying keeps the test
            // robust against that subscription landing slightly after readiness, without depending
            // on a fixed sleep. With the fix the child tracks the very first refresh; before it,
            // the child never moves off its startup count and this loop exhausts its retries.
            let mut child_count = count_rows(&runtime, "local_time_series").await;
            for _ in 0..15 {
                let notify = runtime
                    .datafusion()
                    .refresh_table(&"time_series".into(), None)
                    .await
                    .expect("trigger parent refresh");
                if let Some(notify) = notify {
                    tokio::select! {
                        _ = notify.wait() => {}
                        () = tokio::time::sleep(Duration::from_secs(5)) => {}
                    }
                }
                child_count =
                    wait_for_count(&runtime, "local_time_series", 5, Duration::from_secs(3)).await;
                if child_count == 5 {
                    break;
                }
            }

            // The parent tracks the new rows on refresh.
            assert_eq!(
                count_rows(&runtime, "time_series").await,
                5,
                "parent should track the appended rows after a refresh"
            );

            // The child must follow the parent's refresh via synchronization. Before the fix it
            // stayed frozen at its startup count of 2.
            assert_eq!(
                child_count, 5,
                "localpod child should synchronize with the parent's refresh (was frozen at \
                 startup count before #11137 fix)"
            );

            Ok(())
        })
        .await
}

/// Build the issue's Spicepod: a file-backed parent with the default in-memory accelerator, a
/// `localpod:` child over it, and a `localpod:` grandchild over the child. `refresh_sql` is the
/// edit the reload applies to the parent. The long results-cache TTL is what lets the tests
/// observe a stale cached result instead of its expiry.
fn app_with_parent_refresh_sql(csv_path: &Path, refresh_sql: Option<&str>) -> App {
    AppBuilder::new("test_localpod_child_follows_parent_hot_reload")
        .with_sql_cache(long_lived_results_cache())
        .with_dataset(file_parent(csv_path, refresh_sql))
        .with_dataset(localpod_dataset(
            "localpod:time_series",
            "local_time_series",
        ))
        .with_dataset(localpod_dataset(
            "localpod:local_time_series",
            "local_local_time_series",
        ))
        .build()
}

/// The CSV-backed, in-memory-accelerated parent `time_series`, with `refresh_sql` as its
/// hot-reload edit.
fn file_parent(csv_path: &Path, refresh_sql: Option<&str>) -> Dataset {
    let mut parent = Dataset::new(format!("file://{}", csv_path.display()), "time_series");
    parent.params = Some(Params::from_string_map(
        vec![
            ("file_format".to_string(), "csv".to_string()),
            ("csv_has_header".to_string(), "true".to_string()),
        ]
        .into_iter()
        .collect(),
    ));
    parent.acceleration = Some(Acceleration {
        enabled: true,
        refresh_mode: Some(RefreshMode::Full),
        refresh_sql: refresh_sql.map(str::to_string),
        ..Acceleration::default()
    });
    parent
}

/// A Spicepod whose `localpod:` child is *not* accelerated — a pass-through over the parent's
/// table — with the parent present (`Some(csv_path)`) or removed (`None`).
fn app_with_passthrough_child(csv_path: Option<&Path>, refresh_sql: Option<&str>) -> App {
    let mut app = AppBuilder::new("test_localpod_passthrough_child")
        .with_sql_cache(long_lived_results_cache());
    if let Some(csv_path) = csv_path {
        app = app.with_dataset(file_parent(csv_path, refresh_sql));
    }
    app.with_dataset(Dataset::new("localpod:time_series", "local_time_series"))
        .build()
}

/// A `localpod:` dataset of [`app_with_parent_refresh_sql`], identical in every app so no apply
/// ever reloads it for a change of its own.
fn localpod_dataset(from: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(from, name);
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        refresh_mode: Some(RefreshMode::Full),
        ..Acceleration::default()
    });
    dataset
}

fn long_lived_results_cache() -> SQLResultsCacheConfig {
    SQLResultsCacheConfig {
        item_ttl: Some("10m".to_string()),
        ..Default::default()
    }
}

/// The issue's Spicepod with the parent removed: only the `localpod:` chain remains.
fn app_with_localpod_chain_only() -> App {
    AppBuilder::new("test_localpod_child_follows_parent_hot_reload")
        .with_sql_cache(long_lived_results_cache())
        .with_dataset(localpod_dataset(
            "localpod:time_series",
            "local_time_series",
        ))
        .with_dataset(localpod_dataset(
            "localpod:local_time_series",
            "local_local_time_series",
        ))
        .build()
}

/// A localpod child must follow its parent through a hot reload instead of serving the table the
/// parent retired.
///
/// Regression test for <https://github.com/spiceai/spiceai/issues/3288>: editing the parent's
/// acceleration in the Spicepod (here, adding a `refresh_sql`) hot-reloads the parent onto a new
/// accelerated table, but the child's own entry is unchanged, so the diff never reloaded it. The
/// child kept reading from — and synchronizing its refreshes with — the parent's old table, so
/// `local_time_series` went on answering rows the parent no longer had.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_localpod_child_follows_parent_hot_reload() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,runtime_table::accelerated=trace",
    ));

    test_request_context()
        .scope(async {
            let temp_dir = TempDir::new().expect("create temp dir");
            let csv_path = temp_dir.path().join("data.csv");
            fs::write(&csv_path, format!("{CSV_HEADER}{}", rows(0, 5)))
                .await
                .expect("write initial csv");

            configure_test_datafusion();
            let runtime = Arc::new(
                Runtime::builder()
                    .with_app(app_with_parent_refresh_sql(&csv_path, None))
                    .build()
                    .await,
            );

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(30)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = Arc::clone(&runtime).load_components() => {}
            }
            runtime_ready_check(&runtime).await;

            assert_eq!(
                count_rows(&runtime, "time_series").await,
                5,
                "parent should load the five startup rows"
            );
            assert_eq!(
                count_rows(&runtime, "local_time_series").await,
                5,
                "localpod child should load the five startup rows"
            );
            assert_eq!(
                count_rows(&runtime, "local_local_time_series").await,
                5,
                "localpod grandchild should load the five startup rows"
            );

            // The issue's edit: a `refresh_sql` on the parent's acceleration, which keeps two of
            // the five rows and replaces the parent's accelerated table on hot reload.
            let edited = Arc::new(app_with_parent_refresh_sql(
                &csv_path,
                Some("SELECT * FROM time_series WHERE id < 2"),
            ));
            assert!(
                Arc::clone(&runtime).apply_app(edited).await,
                "the edited spicepod differs from the booted one, so it must apply"
            );

            // The parent's hot reload is awaited inside `apply_app`; its new table answers
            // as soon as the apply returns. Polled anyway so the child assertion below is
            // never explained by a parent that had not swapped yet.
            assert_eq!(
                wait_for_count(&runtime, "time_series", 2, Duration::from_secs(30)).await,
                2,
                "parent should serve only the rows its new refresh_sql keeps"
            );

            // The child must be reloaded onto the parent's new table, and the grandchild onto the
            // child's. Before the #3288 fix both kept the retired tables and stayed at the
            // startup count of 5.
            assert_eq!(
                wait_for_count(&runtime, "local_time_series", 2, Duration::from_secs(30)).await,
                2,
                "localpod child should follow its parent's hot reload (kept serving the \
                 parent's retired table before the #3288 fix)"
            );
            assert_eq!(
                wait_for_count(
                    &runtime,
                    "local_local_time_series",
                    2,
                    Duration::from_secs(30)
                )
                .await,
                2,
                "localpod grandchild should follow the chain's hot reload"
            );

            Ok(())
        })
        .await
}

/// A `localpod` chain must bind to a parent that is removed from the Spicepod and added back,
/// instead of staying on the tables the removed parent left behind.
///
/// Regression test for <https://github.com/spiceai/spiceai/issues/3288>, the other way a
/// parent's registration is replaced under an unchanged child: the child's own entry never
/// changes across either apply, so before the fix neither apply reloaded it, and it went on
/// serving the removed parent's rows. A child of a parent added in the same apply cannot bind
/// until that parent is registered, so its reload is chained behind the parent's load — and the
/// grandchild's behind the child's, since a load this apply spawns registers nothing until it
/// runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_localpod_child_follows_parent_removed_and_added_back() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,runtime_table::accelerated=trace",
    ));

    test_request_context()
        .scope(async {
            let temp_dir = TempDir::new().expect("create temp dir");
            let csv_path = temp_dir.path().join("data.csv");
            fs::write(&csv_path, format!("{CSV_HEADER}{}", rows(0, 5)))
                .await
                .expect("write initial csv");

            configure_test_datafusion();
            let runtime = Arc::new(
                Runtime::builder()
                    .with_app(app_with_parent_refresh_sql(&csv_path, None))
                    .build()
                    .await,
            );

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(30)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = Arc::clone(&runtime).load_components() => {}
            }
            runtime_ready_check(&runtime).await;
            assert_eq!(count_rows(&runtime, "local_time_series").await, 5);
            assert_eq!(count_rows(&runtime, "local_local_time_series").await, 5);

            // Warm the query path's caches for the child: the plan cache now holds a plan
            // over the child's current table, and the results cache its count.
            let (status, count) = cached_count(&runtime, "local_time_series").await;
            assert_eq!((status, count), (CacheStatus::CacheMiss, 5));
            let (status, count) = cached_count(&runtime, "local_time_series").await;
            assert_eq!((status, count), (CacheStatus::CacheHit, 5));

            // Remove the parent. Its unload is awaited inside `apply_app`.
            assert!(
                Arc::clone(&runtime)
                    .apply_app(Arc::new(app_with_localpod_chain_only()))
                    .await,
                "dropping the parent differs from the booted spicepod, so it must apply"
            );
            assert!(
                runtime
                    .datafusion()
                    .ctx
                    .sql("SELECT COUNT(*) FROM time_series")
                    .await
                    .is_err(),
                "the removed parent must no longer resolve"
            );

            // Add the parent back with a refresh_sql that keeps two of the five rows. The
            // child's entry is identical to the one it booted with.
            assert!(
                Arc::clone(&runtime)
                    .apply_app(Arc::new(app_with_parent_refresh_sql(
                        &csv_path,
                        Some("SELECT * FROM time_series WHERE id < 2"),
                    )))
                    .await,
                "re-adding the parent differs from the child-only spicepod, so it must apply"
            );
            // The loads are spawned by `apply_app`, so each table is unregistered, then
            // registered but still loading, for a moment: the parent until its load completes,
            // the child and grandchild because they are unloaded and chained behind it.
            assert_eq!(
                wait_for_registered_count(&runtime, "time_series", 2, Duration::from_secs(30))
                    .await,
                Some(2),
                "the re-added parent should serve only the rows its refresh_sql keeps"
            );
            assert_eq!(
                wait_for_registered_count(
                    &runtime,
                    "local_time_series",
                    2,
                    Duration::from_secs(30)
                )
                .await,
                Some(2),
                "localpod child should bind to the re-added parent (kept serving the removed \
                 parent's rows before the #3288 fix)"
            );
            assert_eq!(
                wait_for_registered_count(
                    &runtime,
                    "local_local_time_series",
                    2,
                    Duration::from_secs(30)
                )
                .await,
                Some(2),
                "localpod grandchild should bind to the re-added chain (reloaded inline while \
                 its parent was still unloaded before the fix, and never retried)"
            );

            // The query path, with its plan and results caches warmed over the child's previous
            // table above, must answer from the child's new table.
            let (_, cached) = cached_count(&runtime, "local_time_series").await;
            assert_eq!(
                cached, 2,
                "the query path should answer from the child's new table, not from a plan or \
                 result cached over its previous one"
            );

            Ok(())
        })
        .await
}

/// A pass-through `localpod` child (no acceleration of its own) must also answer from the
/// re-added parent through the query path's caches.
///
/// Regression test for the cache half of <https://github.com/spiceai/spiceai/issues/3288>: an
/// in-place `update_dataset` clears cached plans and invalidates cached results around its swap,
/// and an accelerated child's initial refresh does the same on completion — but a pass-through
/// child re-queued behind its re-added parent has neither, so a plan cached over the removed
/// parent's table would keep answering with its rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_localpod_passthrough_child_follows_parent_removed_and_added_back()
-> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,runtime_table::accelerated=trace",
    ));

    test_request_context()
        .scope(async {
            let temp_dir = TempDir::new().expect("create temp dir");
            let csv_path = temp_dir.path().join("data.csv");
            fs::write(&csv_path, format!("{CSV_HEADER}{}", rows(0, 5)))
                .await
                .expect("write initial csv");

            configure_test_datafusion();
            let runtime = Arc::new(
                Runtime::builder()
                    .with_app(app_with_passthrough_child(Some(&csv_path), None))
                    .build()
                    .await,
            );

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(30)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = Arc::clone(&runtime).load_components() => {}
            }
            runtime_ready_check(&runtime).await;

            // Warm the query path's plan and results caches over the child's current table.
            let (status, count) = cached_count(&runtime, "local_time_series").await;
            assert_eq!((status, count), (CacheStatus::CacheMiss, 5));
            let (status, count) = cached_count(&runtime, "local_time_series").await;
            assert_eq!((status, count), (CacheStatus::CacheHit, 5));

            assert!(
                Arc::clone(&runtime)
                    .apply_app(Arc::new(app_with_passthrough_child(None, None)))
                    .await,
                "dropping the parent differs from the booted spicepod, so it must apply"
            );
            assert!(
                Arc::clone(&runtime)
                    .apply_app(Arc::new(app_with_passthrough_child(
                        Some(&csv_path),
                        Some("SELECT * FROM time_series WHERE id < 2"),
                    )))
                    .await,
                "re-adding the parent differs from the child-only spicepod, so it must apply"
            );
            assert_eq!(
                wait_for_registered_count(&runtime, "time_series", 2, Duration::from_secs(30))
                    .await,
                Some(2),
                "the re-added parent should serve only the rows its refresh_sql keeps"
            );
            assert_eq!(
                wait_for_registered_count(
                    &runtime,
                    "local_time_series",
                    2,
                    Duration::from_secs(30)
                )
                .await,
                Some(2),
                "the pass-through child should read the re-added parent's rows"
            );

            // The query path must answer from the new table too, not from the plan or result
            // cached over the removed parent's.
            let (_, cached) = cached_count(&runtime, "local_time_series").await;
            assert_eq!(
                cached, 2,
                "the query path should answer from the re-added parent's rows, not from a plan \
                 or result cached over the removed parent's table"
            );

            Ok(())
        })
        .await
}
