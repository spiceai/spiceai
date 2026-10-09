/*
Copyright 2026 The Spice.ai OSS Authors
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

use super::*;
use accelerator_duckdb as _;
use arrow::array::Int64Array;
use runtime_acceleration::snapshot::{
    AccelerationEngine, AccelerationLayout, ForceCreate, SnapshotBehavior, SnapshotManager,
};
use spicepod::component::{
    dataset::Dataset as PodDataset,
    snapshot::{BootstrapOnFailureBehavior, Snapshots},
};
use spicepod::param::Params;
use tokio::sync::Mutex;

struct Fixture {
    directory: tempfile::TempDir,
    snapshots: Snapshots,
}

impl Fixture {
    fn new(behavior: BootstrapOnFailureBehavior) -> Self {
        let directory = tempfile::tempdir().expect("test directory");
        std::fs::write(directory.path().join("rows.csv"), "id\n1\n2\n3\n").expect("source CSV");
        std::fs::create_dir(directory.path().join("snapshots")).expect("snapshot directory");
        let snapshots = Snapshots {
            enabled: true,
            location: Some(format!("file://{}/snapshots/", directory.path().display())),
            bootstrap_on_failure_behavior: behavior,
            ..Default::default()
        };
        Self {
            directory,
            snapshots,
        }
    }

    fn dataset(&self, name: &str, reader: bool) -> PodDataset {
        let mut dataset = PodDataset::new(
            format!("file:{}", self.directory.path().join("rows.csv").display()),
            name,
        );
        dataset.params = Some(Params::from_string_map(HashMap::from([(
            "file_format".to_string(),
            "csv".to_string(),
        )])));
        if reader {
            dataset.acceleration = Some(spicepod::acceleration::Acceleration {
                enabled: true,
                engine: Some("duckdb".to_string()),
                mode: spicepod::acceleration::Mode::File,
                refresh_mode: Some(spicepod::acceleration::RefreshMode::Snapshot),
                refresh_check_interval: Some("100ms".to_string()),
                snapshots: spicepod::acceleration::SnapshotBehavior::BootstrapOnly,
                params: Some(Params::from_string_map(HashMap::from([(
                    "duckdb_file".to_string(),
                    self.directory
                        .path()
                        .join("reader.db")
                        .to_string_lossy()
                        .into_owned(),
                )]))),
                ..Default::default()
            });
        }
        dataset
    }

    fn app(&self, readers: bool, second: bool) -> App {
        let mut builder = app::AppBuilder::new("snapshot_bootstrap")
            .with_snapshots(self.snapshots.clone())
            .with_dataset(self.dataset("unrelated", false));
        if readers {
            builder = builder.with_dataset(self.dataset("orders", true));
        }
        if second {
            builder = builder.with_dataset(self.dataset("unrelated2", false));
        }
        builder.build()
    }

    async fn publish(&self, runtime: &Runtime) {
        let path = self.directory.path().join("writer.db");
        let writer_path = path.clone();
        tokio::task::spawn_blocking(move || {
            let connection = duckdb::Connection::open(writer_path).expect("writer database");
            connection
                .execute_batch("CREATE TABLE orders AS SELECT 42::BIGINT AS id")
                .expect("writer rows");
        })
        .await
        .expect("writer task");
        let manager = SnapshotManager::try_new(
            "orders".to_string(),
            SnapshotBehavior::Enabled(
                Arc::new(self.snapshots.clone()),
                Arc::downgrade(&runtime.secrets()),
                tokio::runtime::Handle::current(),
                spicepod::acceleration::SnapshotsCompaction::Disabled,
            ),
            AccelerationLayout::File { path },
            AccelerationEngine::DuckDB,
        )
        .await
        .expect("snapshot manager");
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            true,
        )]));
        manager
            .create_snapshot(
                &schema,
                Arc::new(Mutex::new(())).lock_owned().await,
                None,
                Some(1),
                ForceCreate(true),
            )
            .await
            .expect("publish snapshot")
            .expect("snapshot created");
    }
}

async fn wait_for_count(runtime: &Runtime, table: &str, expected: i64) {
    let sql = format!("SELECT COUNT(*) AS n FROM {table}");
    assert!(
        test_framework::utils::wait_until_true(Duration::from_secs(15), || async {
            let Ok(frame) = runtime.datafusion().ctx.sql(&sql).await else {
                return false;
            };
            let Ok(batches) = frame.collect().await else {
                return false;
            };
            batches
                .first()
                .and_then(|batch| batch.column(0).as_any().downcast_ref::<Int64Array>())
                .is_some_and(|counts| counts.value(0) == expected)
        })
        .await,
        "{table} must return {expected} rows"
    );
}

#[tokio::test]
async fn snapshot_bootstrap_does_not_block_startup_and_default_warn_recovers() {
    let fixture = Fixture::new(BootstrapOnFailureBehavior::Warn);
    let runtime = Arc::new(
        Runtime::builder()
            .with_app(fixture.app(true, false))
            .build()
            .await,
    );
    tokio::time::timeout(
        Duration::from_secs(5),
        Arc::clone(&runtime).load_components(),
    )
    .await
    .expect("initial component load must finish before a reader's first publication");
    assert!(!runtime.initial_load_in_flight());
    wait_for_count(&runtime, "unrelated", 3).await;
    assert!(
        !fixture.directory.path().join("reader.db").exists(),
        "the engine must not open the reader file before the snapshot is restored"
    );
    assert_eq!(
        runtime
            .status
            .get_dataset_statuses()
            .get(&TableReference::bare("orders")),
        Some(&status::ComponentStatus::Initializing)
    );
    fixture.publish(&runtime).await;
    wait_for_count(&runtime, "orders", 1).await;
    let batches = runtime
        .datafusion()
        .ctx
        .sql("SELECT id FROM orders")
        .await
        .expect("reader query")
        .collect()
        .await
        .expect("snapshot rows");
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id values")
            .value(0),
        42,
        "the reader must serve the published snapshot, not its three-row source"
    );
    runtime.shutdown().await;
    drop(runtime);
    let path = fixture.directory.path().join("reader.db");
    tokio::task::spawn_blocking(move || {
        let connection = duckdb::Connection::open(path).expect("restored database reopens");
        let id: i64 = connection
            .query_row("SELECT id FROM orders", [], |row| row.get(0))
            .expect("restored database has no stale WAL");
        assert_eq!(id, 42);
    })
    .await
    .expect("reopen task");
}

#[tokio::test]
async fn snapshot_bootstrap_does_not_block_consecutive_applies() {
    let fixture = Fixture::new(BootstrapOnFailureBehavior::Retry);
    let runtime = Arc::new(
        Runtime::builder()
            .with_app(fixture.app(false, false))
            .build()
            .await,
    );
    Arc::clone(&runtime).load_datasets().await;
    wait_for_count(&runtime, "unrelated", 3).await;
    for second in [false, true] {
        assert!(
            tokio::time::timeout(
                Duration::from_secs(5),
                Arc::clone(&runtime).apply_app(Arc::new(fixture.app(true, second)))
            )
            .await
            .expect("snapshot bootstrap must not hold apply_app_lock")
        );
    }
    wait_for_count(&runtime, "unrelated2", 3).await;
    assert!(
        runtime
            .read_app()
            .await
            .expect("applied app")
            .datasets
            .iter()
            .any(|dataset| dataset.name == "orders"),
        "the waiting reader must be visible in the applied app"
    );
    fixture.publish(&runtime).await;
    wait_for_count(&runtime, "orders", 1).await;
    runtime.status.mark_shutdown();
}

async fn bootstrap_is_finished(runtime: &Runtime, name: &str) -> bool {
    runtime
        .tasks
        .read()
        .await
        .get(&Runtime::snapshot_bootstrap_task_name(
            &TableReference::bare(name),
        ))
        .is_none_or(runtime_async::cancellable_task::CancellableTaskHandle::is_finished)
}

#[tokio::test]
async fn snapshot_bootstrap_is_cancelled_on_shutdown() {
    let fixture = Fixture::new(BootstrapOnFailureBehavior::Retry);
    let runtime = Arc::new(
        Runtime::builder()
            .with_app(fixture.app(true, false))
            .build()
            .await,
    );
    Arc::clone(&runtime).load_datasets().await;
    wait_for_count(&runtime, "unrelated", 3).await;
    assert!(!bootstrap_is_finished(&runtime, "orders").await);
    runtime.status.mark_shutdown();
    assert!(
        test_framework::utils::wait_until_true(Duration::from_secs(1), || async {
            bootstrap_is_finished(&runtime, "orders").await
        })
        .await,
        "shutdown cancels the pending bootstrap"
    );
    assert!(!fixture.directory.path().join("reader.db").exists());
}

#[tokio::test]
async fn snapshot_bootstrap_on_registration_serves_source_until_publication() {
    let fixture = Fixture::new(BootstrapOnFailureBehavior::Warn);
    let mut app = fixture.app(true, false);
    app.datasets
        .iter_mut()
        .find(|dataset| dataset.name == "orders")
        .expect("reader")
        .ready_state = spicepod::component::dataset::ReadyState::OnRegistration;
    let runtime = Arc::new(Runtime::builder().with_app(app).build().await);
    Arc::clone(&runtime).load_datasets().await;
    wait_for_count(&runtime, "orders", 3).await;
    assert!(
        !fixture.directory.path().join("reader.db").exists(),
        "source fallback must not create an acceleration file"
    );
    fixture.publish(&runtime).await;
    wait_for_count(&runtime, "orders", 1).await;
    runtime.shutdown().await;
}

#[tokio::test]
async fn snapshot_bootstrap_keeps_serving_during_path_change() {
    let fixture = Fixture::new(BootstrapOnFailureBehavior::Warn);
    let runtime = Arc::new(
        Runtime::builder()
            .with_app(fixture.app(true, false))
            .build()
            .await,
    );
    fixture.publish(&runtime).await;
    Arc::clone(&runtime).load_datasets().await;
    wait_for_count(&runtime, "orders", 1).await;
    let snapshots = fixture.directory.path().join("snapshots");
    let offline = fixture.directory.path().join("offline");
    tokio::fs::rename(&snapshots, &offline)
        .await
        .expect("snapshot store unavailable");
    let mut app = fixture.app(true, false);
    app.datasets
        .iter_mut()
        .find(|dataset| dataset.name == "orders")
        .expect("reader")
        .acceleration
        .as_mut()
        .expect("acceleration")
        .params = Some(Params::from_string_map(HashMap::from([(
        "duckdb_file".to_string(),
        fixture
            .directory
            .path()
            .join("replacement.db")
            .to_string_lossy()
            .into_owned(),
    )])));
    tokio::time::timeout(
        Duration::from_secs(5),
        Arc::clone(&runtime).apply_app(Arc::new(app)),
    )
    .await
    .expect("path change must not wait for snapshot storage");
    wait_for_count(&runtime, "orders", 1).await;
    assert!(!bootstrap_is_finished(&runtime, "orders").await);
    assert!(!fixture.directory.path().join("replacement.db").exists());
    tokio::fs::rename(&offline, &snapshots)
        .await
        .expect("snapshot store recovers");
    assert!(
        test_framework::utils::wait_until_true(Duration::from_secs(15), || async {
            bootstrap_is_finished(&runtime, "orders").await
        })
        .await,
        "replacement bootstrap must finish after storage recovers"
    );
    assert!(fixture.directory.path().join("replacement.db").exists());
    wait_for_count(&runtime, "orders", 1).await;
    runtime.shutdown().await;
}

fn app_with_reader_path(fixture: &Fixture, file: &str) -> App {
    let mut app = fixture.app(true, false);
    app.datasets
        .iter_mut()
        .find(|dataset| dataset.name == "orders")
        .expect("reader")
        .acceleration
        .as_mut()
        .expect("acceleration")
        .params = Some(Params::from_string_map(HashMap::from([(
        "duckdb_file".to_string(),
        fixture
            .directory
            .path()
            .join(file)
            .to_string_lossy()
            .into_owned(),
    )])));
    app
}

/// A second path change arrives while the first replacement is still waiting for
/// its snapshot. The reader that was serving before either change must keep
/// serving until a replacement is restored.
#[tokio::test]
async fn snapshot_bootstrap_keeps_serving_during_consecutive_path_changes() {
    let fixture = Fixture::new(BootstrapOnFailureBehavior::Warn);
    let runtime = Arc::new(
        Runtime::builder()
            .with_app(fixture.app(true, false))
            .build()
            .await,
    );
    fixture.publish(&runtime).await;
    Arc::clone(&runtime).load_datasets().await;
    wait_for_count(&runtime, "orders", 1).await;
    let snapshots = fixture.directory.path().join("snapshots");
    let offline = fixture.directory.path().join("offline");
    tokio::fs::rename(&snapshots, &offline)
        .await
        .expect("snapshot store unavailable");

    for file in ["replacement.db", "replacement2.db"] {
        tokio::time::timeout(
            Duration::from_secs(5),
            Arc::clone(&runtime).apply_app(Arc::new(app_with_reader_path(&fixture, file))),
        )
        .await
        .expect("path change must not wait for snapshot storage");
        wait_for_count(&runtime, "orders", 1).await;
    }
    assert!(!fixture.directory.path().join("replacement2.db").exists());

    tokio::fs::rename(&offline, &snapshots)
        .await
        .expect("snapshot store recovers");
    assert!(
        test_framework::utils::wait_until_true(Duration::from_secs(15), || async {
            fixture.directory.path().join("replacement2.db").exists()
        })
        .await,
        "the latest replacement must restore after storage recovers"
    );
    wait_for_count(&runtime, "orders", 1).await;
    runtime.shutdown().await;
}

#[tokio::test]
async fn snapshot_bootstrap_removed_reader_does_not_register_after_publication() {
    let fixture = Fixture::new(BootstrapOnFailureBehavior::Warn);
    let runtime = Arc::new(
        Runtime::builder()
            .with_app(fixture.app(true, false))
            .build()
            .await,
    );
    Arc::clone(&runtime).load_datasets().await;
    assert!(!bootstrap_is_finished(&runtime, "orders").await);
    Arc::clone(&runtime)
        .apply_app(Arc::new(fixture.app(false, true)))
        .await;
    assert!(bootstrap_is_finished(&runtime, "orders").await);
    fixture.publish(&runtime).await;
    wait_for_count(&runtime, "unrelated2", 3).await;
    assert!(!runtime.df.table_exists(&TableReference::bare("orders")));
    assert!(!fixture.directory.path().join("reader.db").exists());
    runtime.shutdown().await;
}
