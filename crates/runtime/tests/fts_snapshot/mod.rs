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

//! End-to-end coverage for acceleration snapshots of a file-backed full-text search index
//! (#7557): the DB payload and its FTS index must be captured and restored together, both at
//! cold bootstrap and on every later `refresh_mode: snapshot` hot-swap.
//!
//! Runs entirely on local files: the source is a `file://` CSV and the snapshot store is a
//! `file://` directory, which the snapshot object-store builder reaches through its
//! `object_store::parse_url` fallback. That keeps every round trip here runnable without cloud
//! credentials — see `view::snapshot` for the same approach applied to accelerated views.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use app::AppBuilder;
use runtime::Runtime;
use spicepod::acceleration::{Acceleration, Mode, RefreshMode, SnapshotBehavior};
use spicepod::component::dataset::Dataset;
use spicepod::component::snapshot::{BootstrapOnFailureBehavior, Snapshots};
use spicepod::param::Params;
use spicepod::semantic::{Column, FullTextSearchConfig, IndexStore};

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{register_test_connectors, runtime_ready_check, test_request_context},
};

/// `body` carries the term a restore-vs-rebuild assertion keys on; `title` is a second
/// full-text column so identity matching (`kind` + sorted `columns`) is exercised too.
const SOURCE_CSV: &str =
    "id,title,body\n1,First,alpha unique needle for round trip\n2,Second,beta filler content\n";

/// Written over the source *after* a snapshot is published. `alpha` is gone and `gamma` is
/// new, so which term a query finds tells you unambiguously whether it read the snapshot or
/// the live (diverged) source: a consumer that ignored the snapshot and rebuilt from this file
/// would find `gamma`, never `alpha`.
const SOURCE_CSV_DIVERGED: &str =
    "id,title,body\n1,First,gamma replacement text\n2,Second,beta filler content\n";

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

/// Like `wait_until`, but for a condition that itself needs to `.await` (e.g. a query) rather
/// than a plain sync check. Never use `futures::executor::block_on` for this from inside a
/// tokio task — it risks a runtime-in-runtime panic the moment the awaited work tries to spawn
/// or otherwise touch the ambient tokio runtime, which query execution does.
async fn wait_until_async<F, Fut>(
    what: &str,
    timeout: Duration,
    mut condition: F,
) -> anyhow::Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if condition().await {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(anyhow::anyhow!(
        "timed out after {timeout:?} waiting for {what}"
    ))
}

/// Every `*.tar` / `*.duckdb` published under the snapshot location, at any depth.
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

/// Every `*.tar` under the snapshot location — the index artifacts specifically, as opposed to
/// the DB payload (`published_snapshots`, which includes both).
fn published_index_artifacts(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    published_snapshots(root)
        .into_iter()
        .filter(|path| path.extension().is_some_and(|ext| ext == "tar"))
        .collect()
}

fn snapshots_config(location: &std::path::Path) -> Snapshots {
    Snapshots {
        enabled: true,
        location: Some(format!("file://{}", location.display())),
        bootstrap_on_failure_behavior: BootstrapOnFailureBehavior::Warn,
        params: None,
    }
}

/// A `duckdb:file` dataset with a file-backed full-text index over `body` and `title`, reading
/// from `csv_path` and storing its DB/index under `duckdb_file`/`fts_dir` — both must be unique
/// per `Runtime` in a test so two instances in the same process never share local state.
fn fts_dataset(
    name: &str,
    csv_path: &std::path::Path,
    duckdb_file: &std::path::Path,
    fts_dir: &std::path::Path,
    snapshots: SnapshotBehavior,
    refresh_mode: RefreshMode,
    refresh_check_interval: Option<String>,
) -> Dataset {
    let mut dataset = Dataset::new(format!("file://{}", csv_path.display()), name);
    dataset.params = Some(Params::from_string_map(HashMap::from([
        ("file_format".to_string(), "csv".to_string()),
        ("csv_has_header".to_string(), "true".to_string()),
    ])));
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("duckdb".to_string()),
        mode: Mode::File,
        refresh_mode: Some(refresh_mode),
        refresh_check_interval,
        params: Some(Params::from_string_map(HashMap::from([(
            "duckdb_file".to_string(),
            duckdb_file.display().to_string(),
        )]))),
        snapshots,
        ..Acceleration::default()
    });
    let full_text_search = FullTextSearchConfig {
        index_store: Some(IndexStore::File),
        index_directory: Some(fts_dir.display().to_string()),
        ..FullTextSearchConfig::enabled().with_row_id("id")
    };
    // `id`'s type is declared explicitly rather than left to CSV schema inference: an
    // undeclared numeric-looking column ("1", "2") can infer as Int64, and the assertions below
    // read it back as a string.
    let mut id_column = Column::new("id");
    id_column.r#type = Some("utf8".to_string());
    dataset.columns = vec![
        id_column,
        Column::new("title").with_full_text_search(full_text_search.clone()),
        Column::new("body").with_full_text_search(full_text_search),
    ];
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

async fn load(app: app::App) -> anyhow::Result<Arc<Runtime>> {
    let rt = load_components(app).await?;
    runtime_ready_check(&rt).await;
    Ok(rt)
}

async fn text_search_ids(
    rt: &Arc<Runtime>,
    term: &str,
    column: &str,
) -> anyhow::Result<Vec<String>> {
    use futures::TryStreamExt;

    let sql = format!("SELECT id FROM text_search(docs, '{term}', {column}) ORDER BY id");
    let results = rt
        .datafusion()
        .query_builder(&sql)
        .build()
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("querying text_search: {e}"))?
        .data
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| anyhow::anyhow!("collecting text_search rows: {e}"))?;

    let mut ids = Vec::new();
    for batch in results {
        let column = batch
            .column_by_name("id")
            .ok_or_else(|| anyhow::anyhow!("text_search result missing `id` column"))?;
        let column = column
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or_else(|| anyhow::anyhow!("`id` column is not a StringArray"))?;
        ids.extend(column.iter().flatten().map(ToString::to_string));
    }
    Ok(ids)
}

/// A dataset with a file-backed full-text index publishes a snapshot that includes an index
/// artifact, and a cold start with an empty accelerator *and* an empty FTS directory restores
/// both the DB and the index from it — the whole point of #7557, exercised end to end.
#[cfg(feature = "duckdb")]
#[tokio::test]
async fn fts_snapshot_round_trips_at_cold_bootstrap() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let temp = tempfile::tempdir().expect("temp dir");
            let csv_path = temp.path().join("docs.csv");
            std::fs::write(&csv_path, SOURCE_CSV).expect("write source csv");
            let snapshot_dir = temp.path().join("snapshots");
            std::fs::create_dir_all(&snapshot_dir).expect("mkdir snapshots");

            // --- publish -----------------------------------------------------------
            let publisher_db = temp.path().join("publisher.duckdb");
            let publisher_fts = temp.path().join("publisher_fts");
            let app = AppBuilder::new("fts_snapshot_publish")
                .with_dataset(fts_dataset(
                    "docs",
                    &csv_path,
                    &publisher_db,
                    &publisher_fts,
                    SnapshotBehavior::Enabled,
                    RefreshMode::Full,
                    None,
                ))
                .with_snapshots(snapshots_config(&snapshot_dir))
                .build();
            let publisher = load(app).await?;

            let snapshot_dir_for_wait = snapshot_dir.clone();
            wait_until(
                "the dataset to publish a snapshot with an index artifact",
                Duration::from_secs(90),
                move || !published_index_artifacts(&snapshot_dir_for_wait).is_empty(),
            )
            .await?;

            // Sanity check on the publisher itself before it's dropped: the term is there.
            let found = text_search_ids(&publisher, "alpha", "body").await?;
            assert_eq!(
                found,
                vec!["1".to_string()],
                "publisher should find 'alpha' pre-divergence"
            );
            drop(publisher);

            // Diverge the source so the two paths give different answers. From here, finding
            // 'alpha' can only mean the index was restored from the snapshot; finding 'gamma'
            // (or nothing) means the index was rebuilt from the diverged file instead.
            std::fs::write(&csv_path, SOURCE_CSV_DIVERGED).expect("diverge source csv");

            // --- bootstrap -----------------------------------------------------------
            // A different DB file *and* a different FTS directory, so nothing local can
            // satisfy the read: both must come from the snapshot just published.
            let consumer_db = temp.path().join("consumer.duckdb");
            let consumer_fts = temp.path().join("consumer_fts");
            let app = AppBuilder::new("fts_snapshot_bootstrap")
                .with_dataset(fts_dataset(
                    "docs",
                    &csv_path,
                    &consumer_db,
                    &consumer_fts,
                    SnapshotBehavior::BootstrapOnly,
                    RefreshMode::Full,
                    None,
                ))
                .with_snapshots(snapshots_config(&snapshot_dir))
                .build();
            let consumer = load(app).await?;

            assert!(
                consumer_db.exists(),
                "the bootstrap should have materialized an acceleration at {}",
                consumer_db.display()
            );
            assert!(
                consumer_fts.exists(),
                "the bootstrap should have installed a full-text index at {}",
                consumer_fts.display()
            );

            let found_alpha = text_search_ids(&consumer, "alpha", "body").await?;
            assert_eq!(
                found_alpha,
                vec!["1".to_string()],
                "expected 'alpha' from the restored snapshot index; \
                 finding nothing would mean the index was left empty, \
                 and this only passing after a rebuild would mean the restore was a no-op"
            );

            let found_gamma = text_search_ids(&consumer, "gamma", "body").await?;
            assert!(
                found_gamma.is_empty(),
                "'gamma' only exists in the diverged source; finding it here would mean the \
                 index was rebuilt from the live file instead of restored from the snapshot"
            );

            Ok(())
        })
        .await
}

/// A `refresh_mode: snapshot` reader restores the DB *and* the index on its very first
/// bootstrap, but also on every later hot-swap when a newer snapshot appears — regression
/// coverage for the index half of that hot-swap silently no-op'ing (the DB reload worked, the
/// index never updated to match, so search kept returning stale/ghost results indefinitely).
#[cfg(feature = "duckdb")]
#[tokio::test]
async fn fts_snapshot_hot_swap_updates_the_index() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let temp = tempfile::tempdir().expect("temp dir");
            let csv_path = temp.path().join("docs.csv");
            std::fs::write(&csv_path, SOURCE_CSV).expect("write source csv");
            let snapshot_dir = temp.path().join("snapshots");
            std::fs::create_dir_all(&snapshot_dir).expect("mkdir snapshots");

            // --- publish snapshot v1 --------------------------------------------------
            let publisher_db = temp.path().join("publisher.duckdb");
            let publisher_fts = temp.path().join("publisher_fts");
            let app = AppBuilder::new("fts_hot_swap_publish_v1")
                .with_dataset(fts_dataset(
                    "docs",
                    &csv_path,
                    &publisher_db,
                    &publisher_fts,
                    SnapshotBehavior::CreateOnly,
                    RefreshMode::Full,
                    None,
                ))
                .with_snapshots(snapshots_config(&snapshot_dir))
                .build();
            let publisher_v1 = load(app).await?;
            let snapshot_dir_for_wait = snapshot_dir.clone();
            wait_until(
                "the first snapshot to publish",
                Duration::from_secs(90),
                move || !published_index_artifacts(&snapshot_dir_for_wait).is_empty(),
            )
            .await?;
            drop(publisher_v1);

            // --- reader bootstraps from v1, then keeps polling ------------------------
            let reader_db = temp.path().join("reader.duckdb");
            let reader_fts = temp.path().join("reader_fts");
            let app = AppBuilder::new("fts_hot_swap_reader")
                .with_dataset(fts_dataset(
                    "docs",
                    &csv_path,
                    &reader_db,
                    &reader_fts,
                    SnapshotBehavior::BootstrapOnly,
                    RefreshMode::Snapshot,
                    Some("200ms".to_string()),
                ))
                .with_snapshots(snapshots_config(&snapshot_dir))
                .build();
            let reader = load(app).await?;

            let found = text_search_ids(&reader, "alpha", "body").await?;
            assert_eq!(
                found,
                vec!["1".to_string()],
                "reader should find 'alpha' from snapshot v1"
            );

            // --- publish snapshot v2 with different content ---------------------------
            let artifact_count_before_v2 = published_index_artifacts(&snapshot_dir).len();
            std::fs::write(&csv_path, SOURCE_CSV_DIVERGED).expect("write v2 source csv");
            let app = AppBuilder::new("fts_hot_swap_publish_v2")
                .with_dataset(fts_dataset(
                    "docs",
                    &csv_path,
                    &publisher_db,
                    &publisher_fts,
                    SnapshotBehavior::CreateOnly,
                    RefreshMode::Full,
                    None,
                ))
                .with_snapshots(snapshots_config(&snapshot_dir))
                .build();
            let publisher_v2 = load(app).await?;
            let snapshot_dir_for_wait = snapshot_dir.clone();
            wait_until(
                "the second snapshot to publish",
                Duration::from_secs(90),
                move || {
                    published_index_artifacts(&snapshot_dir_for_wait).len()
                        > artifact_count_before_v2
                },
            )
            .await?;
            drop(publisher_v2);

            // --- the still-running reader must hot-swap to v2 -------------------------
            // Polling because the reader's `refresh_check_interval` picks up v2 on its own
            // schedule; there is no synchronous signal for "the hot-swap finished" to wait on.
            wait_until_async(
                "the reader's full-text index to reflect snapshot v2 ('gamma' present)",
                Duration::from_secs(30),
                || async {
                    text_search_ids(&reader, "gamma", "body")
                        .await
                        .is_ok_and(|ids| !ids.is_empty())
                },
            )
            .await?;

            let stale = text_search_ids(&reader, "alpha", "body").await?;
            assert!(
                stale.is_empty(),
                "'alpha' only existed in snapshot v1; the reader's index still returning it \
                 after the v2 hot-swap means the index was left on the old generation \
                 (the DB half of the swap can succeed independently of the index half — \
                 this is what regressed before `SnapshotManager::set_indexes` was wired up \
                 for the refresh-side manager, not just the create-side one)"
            );

            Ok(())
        })
        .await
}
