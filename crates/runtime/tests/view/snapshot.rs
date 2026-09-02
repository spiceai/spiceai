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

//! End-to-end coverage for acceleration snapshots of an accelerated view.
//!
//! Runs entirely on local files: the source is a `file://` CSV and the snapshot store is a
//! `file://` directory, which the snapshot object-store builder reaches through its
//! `object_store::parse_url` fallback. That keeps the round trip — publish, then bootstrap
//! a *fresh* acceleration from what was published — runnable without cloud credentials.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use app::AppBuilder;
use futures::TryStreamExt;
use runtime::Runtime;
use spicepod::acceleration::{Acceleration, Mode, RefreshMode};
use spicepod::component::dataset::Dataset;
use spicepod::component::snapshot::{BootstrapOnFailureBehavior, Snapshots};
use spicepod::component::view::View;
use spicepod::param::Params;

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{register_test_connectors, runtime_ready_check, test_request_context},
};

const SOURCE_CSV: &str = "id,region\n1,us\n2,eu\n3,us\n";

/// Written over the source *after* the snapshot is published. Same schema, different
/// answer: all three rows are now `us`, so the view's query returns 3 from the file and 2
/// from the published snapshot. That difference is what makes the round trip falsifiable —
/// with the original file still in place, a consumer that ignored the snapshot entirely and
/// simply refreshed would produce the same 2 rows and the assertion could not fail.
const SOURCE_CSV_DIVERGED: &str = "id,region\n1,us\n2,us\n3,us\n";

/// Poll `condition` until it holds, so the test waits on the state it needs rather than a
/// fixed sleep.
async fn wait_until<F>(what: &str, timeout: Duration, mut condition: F) -> anyhow::Result<()>
where
    F: FnMut() -> bool,
{
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if condition() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(anyhow::anyhow!(
        "timed out after {timeout:?} waiting for {what}"
    ))
}

/// Every `*.tar` / `*.db` published under the snapshot location, at any depth.
/// Whether a snapshot is fully published — the archive AND the `metadata.json` pointer
/// that makes it the current snapshot.
///
/// `create_snapshot` uploads the archive first and writes the pointer second
/// (`update_metadata_after_upload`), so waiting on the archive alone can return between the
/// two and drop the publisher mid-publication, leaving a snapshot no bootstrap will find.
fn snapshot_is_published(root: &std::path::Path) -> bool {
    !published_snapshots(root).is_empty() && published_metadata(root)
}

/// Whether any `metadata.json` pointer exists under `root`.
fn published_metadata(root: &std::path::Path) -> bool {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.file_name().is_some_and(|name| name == "metadata.json") {
                return true;
            }
        }
    }
    false
}

fn published_snapshots(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|ext| ext != "json") {
                found.push(path);
            }
        }
    }
    found
}

fn snapshots_config(location: &std::path::Path) -> Snapshots {
    Snapshots {
        enabled: true,
        location: Some(format!("file://{}", location.display())),
        bootstrap_on_failure_behavior: BootstrapOnFailureBehavior::Warn,
        params: None,
    }
}

fn accelerated_view(name: &str, sql: &str, duckdb_file: &std::path::Path) -> View {
    let mut view = View::new(name.to_string());
    view.sql = Some(sql.to_string());
    view.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("duckdb".to_string()),
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Full),
        params: Some(Params::from_string_map(HashMap::from([(
            "duckdb_file".to_string(),
            duckdb_file.display().to_string(),
        )]))),
        snapshots: spicepod::acceleration::SnapshotBehavior::Enabled,
        ..Acceleration::default()
    });
    view
}

fn csv_dataset(csv_path: &std::path::Path) -> Dataset {
    let mut dataset = Dataset::new(format!("file://{}", csv_path.display()), "orders");
    dataset.params = Some(Params::from_string_map(HashMap::from([
        ("file_format".to_string(), "csv".to_string()),
        ("csv_has_header".to_string(), "true".to_string()),
    ])));
    dataset
}

async fn load_components(app: app::App) -> anyhow::Result<Arc<Runtime>> {
    configure_test_datafusion();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    let loader = Arc::clone(&rt);
    tokio::select! {
        () = tokio::time::sleep(Duration::from_mins(2)) => {
            anyhow::bail!("timed out loading components");
        }
        () = loader.load_components() => {}
    }
    Ok(rt)
}

/// Load and wait for readiness. Only for pods whose components are all expected to load —
/// a refused component never reports ready, so a test that expects a refusal must not wait.
async fn load(app: app::App) -> anyhow::Result<Arc<Runtime>> {
    let rt = load_components(app).await?;
    runtime_ready_check(&rt).await;
    Ok(rt)
}

/// A single-scan view publishes a snapshot, and a cold start with an empty acceleration
/// restores from it — the whole point of the feature, exercised end to end.
#[cfg(feature = "duckdb")]
#[tokio::test]
async fn accelerated_view_snapshot_round_trips() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let temp = tempfile::tempdir().expect("temp dir");
            let csv_path = temp.path().join("orders.csv");
            std::fs::write(&csv_path, SOURCE_CSV).expect("write source csv");
            let snapshot_dir = temp.path().join("snapshots");
            std::fs::create_dir_all(&snapshot_dir).expect("mkdir snapshots");

            // --- publish -----------------------------------------------------------
            let publisher_db = temp.path().join("publisher.db");
            let app = AppBuilder::new("view_snapshot_publish")
                .with_dataset(csv_dataset(&csv_path))
                .with_view(accelerated_view(
                    "orders_us",
                    "SELECT id FROM orders WHERE region = 'us'",
                    &publisher_db,
                ))
                .with_snapshots(snapshots_config(&snapshot_dir))
                .build();
            let publisher = load(app).await?;

            let snapshot_dir_for_wait = snapshot_dir.clone();
            wait_until(
                "the accelerated view to publish a snapshot and its metadata pointer",
                Duration::from_secs(90),
                || snapshot_is_published(&snapshot_dir_for_wait),
            )
            .await?;
            drop(publisher);

            // Diverge the source so the two paths give different answers. From here, 2 rows
            // can only mean the snapshot was restored; 3 means the view was rebuilt from the
            // file and the bootstrap did nothing.
            std::fs::write(&csv_path, SOURCE_CSV_DIVERGED).expect("diverge source csv");

            // --- bootstrap ---------------------------------------------------------
            // A different acceleration file, so nothing local can satisfy the read: the
            // rows can only come from the snapshot just published.
            let consumer_db = temp.path().join("consumer.db");
            let app = AppBuilder::new("view_snapshot_bootstrap")
                .with_dataset(csv_dataset(&csv_path))
                .with_view(accelerated_view(
                    "orders_us",
                    "SELECT id FROM orders WHERE region = 'us'",
                    &consumer_db,
                ))
                .with_snapshots(snapshots_config(&snapshot_dir))
                .build();
            let consumer = load(app).await?;

            assert!(
                consumer_db.exists(),
                "the bootstrap should have materialized an acceleration at {}",
                consumer_db.display()
            );

            let results = consumer
                .datafusion()
                .query_builder("SELECT id FROM orders_us ORDER BY id")
                .build()
                .run()
                .await
                .expect("querying the bootstrapped view")
                .data
                .try_collect::<Vec<_>>()
                .await
                .expect("collecting bootstrapped rows");
            let rows: usize = results
                .iter()
                .map(arrow::array::RecordBatch::num_rows)
                .sum();
            assert_eq!(
                rows, 2,
                "expected the 2 rows captured in the snapshot; 3 would mean the view was \
                 rebuilt from the diverged source instead of restored from the snapshot"
            );

            Ok(())
        })
        .await
}

/// A view whose query reads its sources twice cannot be snapshotted, and the runtime says
/// so at load rather than publishing a materialization that spans two source positions.
#[cfg(feature = "duckdb")]
#[tokio::test]
async fn multi_read_view_refuses_snapshots() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let temp = tempfile::tempdir().expect("temp dir");
            let csv_path = temp.path().join("orders.csv");
            std::fs::write(&csv_path, SOURCE_CSV).expect("write source csv");
            let snapshot_dir = temp.path().join("snapshots");
            std::fs::create_dir_all(&snapshot_dir).expect("mkdir snapshots");

            let app = AppBuilder::new("view_snapshot_multi_read")
                .with_dataset(csv_dataset(&csv_path))
                .with_view(accelerated_view(
                    "orders_self_join",
                    "SELECT a.id FROM orders a JOIN orders b ON a.id = b.id",
                    &temp.path().join("multi.db"),
                ))
                .with_snapshots(snapshots_config(&snapshot_dir))
                .build();
            // Deliberately not waiting for readiness: the refused view never reports
            // ready, so a readiness wait would time out on the very behaviour under test.
            let rt = load_components(app).await?;

            // The view is refused, so it never registers and never publishes.
            let registered = rt
                .datafusion()
                .query_builder("SELECT id FROM orders_self_join")
                .build()
                .run()
                .await;
            assert!(
                registered.is_err(),
                "a multi-read view with snapshots enabled must not load"
            );
            assert!(
                published_snapshots(&snapshot_dir).is_empty(),
                "a refused view must publish nothing"
            );

            Ok(())
        })
        .await
}
