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
use accelerator_cayenne as _;
use arrow::array::Int64Array;
use spicepod::component::{
    dataset::Dataset as PodDataset,
    snapshot::{BootstrapOnFailureBehavior, Snapshots},
};
use spicepod::param::Params;

struct Fixture {
    directory: tempfile::TempDir,
    snapshots: Snapshots,
}

impl Fixture {
    fn new(behavior: BootstrapOnFailureBehavior) -> Self {
        let directory = tempfile::tempdir().expect("test directory");
        std::fs::write(directory.path().join("rows.csv"), "id\n1\n2\n3\n").expect("source CSV");
        // The writer publishes one row the reader's three-row source does not have, so
        // a reader that serves it is serving the snapshot.
        std::fs::write(directory.path().join("writer.csv"), "id\n42\n").expect("writer CSV");
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
                engine: Some("cayenne".to_string()),
                mode: spicepod::acceleration::Mode::File,
                refresh_mode: Some(spicepod::acceleration::RefreshMode::Snapshot),
                refresh_check_interval: Some("100ms".to_string()),
                snapshots: spicepod::acceleration::SnapshotBehavior::BootstrapOnly,
                params: Some(Params::from_string_map(HashMap::from([(
                    "cayenne_file_path".to_string(),
                    self.directory
                        .path()
                        .join("reader")
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

    /// Publishes a snapshot of the one-row `writer.csv` as `orders`, from a writer
    /// runtime that accelerates it into Cayenne, and waits until the snapshot
    /// location records it as the current snapshot.
    async fn publish(&self) {
        let writer_app = app::AppBuilder::new("snapshot_bootstrap_writer")
            .with_snapshots(self.snapshots.clone())
            .with_dataset(cayenne_dataset(
                self.directory.path(),
                "writer.csv",
                "writer",
                spicepod::acceleration::Acceleration {
                    snapshots: spicepod::acceleration::SnapshotBehavior::CreateOnly,
                    ..Default::default()
                },
            ))
            .build();
        let writer = Arc::new(Runtime::builder().with_app(writer_app).build().await);
        tokio::time::timeout(
            Duration::from_secs(30),
            Arc::clone(&writer).load_components(),
        )
        .await
        .expect("the writer must finish loading");
        let metadata = self.directory.path().join("snapshots/metadata.json");
        assert!(
            test_framework::utils::wait_until_true(Duration::from_secs(15), || async {
                let Ok(contents) = tokio::fs::read_to_string(&metadata).await else {
                    return false;
                };
                serde_json::from_str::<serde_json::Value>(&contents)
                    .ok()
                    .and_then(|metadata| metadata.get("orders")?.get("current-snapshot-id")?.as_u64())
                    .is_some()
            })
            .await,
            "the writer must publish a current snapshot of orders to {} (files: {:?})",
            metadata.display(),
            walk(&self.directory.path().join("snapshots")),
        );
        writer.shutdown().await;
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
        !fixture.directory.path().join("reader").exists(),
        "the engine must not open the reader file before the snapshot is restored"
    );
    assert_eq!(
        runtime
            .status
            .get_dataset_statuses()
            .get(&TableReference::bare("orders")),
        Some(&status::ComponentStatus::Initializing)
    );
    fixture.publish().await;
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
    fixture.publish().await;
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
    assert!(!fixture.directory.path().join("reader").exists());
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
        !fixture.directory.path().join("reader").exists(),
        "source fallback must not create an acceleration file"
    );
    fixture.publish().await;
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
    fixture.publish().await;
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
        "cayenne_file_path".to_string(),
        fixture
            .directory
            .path()
            .join("replacement")
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
    assert!(!fixture.directory.path().join("replacement").exists());
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
    assert!(fixture.directory.path().join("replacement").exists());
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
        "cayenne_file_path".to_string(),
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
    fixture.publish().await;
    Arc::clone(&runtime).load_datasets().await;
    wait_for_count(&runtime, "orders", 1).await;
    let snapshots = fixture.directory.path().join("snapshots");
    let offline = fixture.directory.path().join("offline");
    tokio::fs::rename(&snapshots, &offline)
        .await
        .expect("snapshot store unavailable");

    for file in ["replacement", "replacement2"] {
        tokio::time::timeout(
            Duration::from_secs(5),
            Arc::clone(&runtime).apply_app(Arc::new(app_with_reader_path(&fixture, file))),
        )
        .await
        .expect("path change must not wait for snapshot storage");
        wait_for_count(&runtime, "orders", 1).await;
    }
    assert!(!fixture.directory.path().join("replacement2").exists());

    tokio::fs::rename(&offline, &snapshots)
        .await
        .expect("snapshot store recovers");
    assert!(
        test_framework::utils::wait_until_true(Duration::from_secs(15), || async {
            fixture.directory.path().join("replacement2").exists()
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
    fixture.publish().await;
    wait_for_count(&runtime, "unrelated2", 3).await;
    assert!(!runtime.df.table_exists(&TableReference::bare("orders")));
    assert!(!fixture.directory.path().join("reader").exists());
    runtime.shutdown().await;
}

/// Every file under `root`, relative to it.
fn walk(root: &std::path::Path) -> Vec<String> {
    let mut files = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if let Ok(relative) = path.strip_prefix(root) {
                files.push(relative.display().to_string());
            }
        }
    }
    files.sort();
    files
}

/// A file-mode Cayenne dataset over `csv`, accelerated into the directory `cayenne_dir`.
fn cayenne_dataset(
    directory: &std::path::Path,
    csv: &str,
    cayenne_dir: &str,
    acceleration: spicepod::acceleration::Acceleration,
) -> PodDataset {
    let mut dataset = PodDataset::new(format!("file:{}", directory.join(csv).display()), "orders");
    dataset.params = Some(Params::from_string_map(HashMap::from([(
        "file_format".to_string(),
        "csv".to_string(),
    )])));
    dataset.acceleration = Some(spicepod::acceleration::Acceleration {
        enabled: true,
        engine: Some("cayenne".to_string()),
        mode: spicepod::acceleration::Mode::File,
        params: Some(Params::from_string_map(HashMap::from([(
            "cayenne_file_path".to_string(),
            directory.join(cayenne_dir).to_string_lossy().into_owned(),
        )]))),
        ..acceleration
    });
    dataset
}

/// Shutdown releases the snapshot writer lease.
#[tokio::test]
async fn shutdown_releases_the_snapshot_writer_lease() {
    let directory = tempfile::tempdir().expect("test directory");
    std::fs::write(directory.path().join("rows.csv"), "id\n1\n2\n3\n").expect("writer source");
    let writer_app = app::AppBuilder::new("snapshot_writer_lease")
        .with_runtime(spicepod::component::runtime::Runtime {
            state: Some(spicepod::component::runtime::RuntimeState {
                location: format!("file://{}/state/", directory.path().display()),
                params: None,
            }),
            ..Default::default()
        })
        .with_dataset(cayenne_dataset(
            directory.path(),
            "rows.csv",
            "writer",
            spicepod::acceleration::Acceleration {
                snapshots: spicepod::acceleration::SnapshotBehavior::CreateOnly,
                ..Default::default()
            },
        ))
        .build();
    let writer = Arc::new(Runtime::builder().with_app(writer_app).build().await);
    tokio::time::timeout(
        Duration::from_secs(30),
        Arc::clone(&writer).load_components(),
    )
    .await
    .expect("the writer must finish loading");

    let lease = directory.path().join("state/snapshots/leases/orders.json");
    let lease_duration_ms = || async {
        let bytes = tokio::fs::read(&lease).await.ok()?;
        object_store_occ::lease::LeaseRecord::parse(&bytes).map(|record| record.lease_duration_ms)
    };
    assert!(
        test_framework::utils::wait_until_true(Duration::from_secs(15), || async {
            lease_duration_ms().await.is_some_and(|ms| ms > 0)
        })
        .await,
        "the writer must hold a live lease at {} (files: {:?})",
        lease.display(),
        walk(&directory.path().join("state")),
    );

    writer.shutdown().await;
    assert_eq!(
        lease_duration_ms().await,
        Some(0),
        "shutdown must release the writer lease"
    );
}

/// With `runtime.state` set and no `snapshots` section, a writer publishes to the
/// `snapshots/` folder of the state location, and a reader with the same
/// `runtime.state` restores from it.
#[tokio::test]
async fn runtime_state_location_holds_snapshots_without_a_snapshots_section() {
    let directory = tempfile::tempdir().expect("test directory");
    std::fs::write(directory.path().join("rows.csv"), "id\n1\n2\n3\n").expect("writer source");
    std::fs::write(
        directory.path().join("reader_rows.csv"),
        "id\n1\n2\n3\n4\n5\n",
    )
    .expect("reader source");
    let runtime_config = spicepod::component::runtime::Runtime {
        state: Some(spicepod::component::runtime::RuntimeState {
            location: format!("file://{}/state/", directory.path().display()),
            params: None,
        }),
        ..Default::default()
    };

    let writer_app = app::AppBuilder::new("snapshot_writer")
        .with_runtime(runtime_config.clone())
        .with_dataset(cayenne_dataset(
            directory.path(),
            "rows.csv",
            "writer",
            spicepod::acceleration::Acceleration {
                snapshots: spicepod::acceleration::SnapshotBehavior::Enabled,
                ..Default::default()
            },
        ))
        .build();
    assert!(
        writer_app.snapshots.is_none(),
        "the spicepod has no snapshots section"
    );
    let writer = Arc::new(Runtime::builder().with_app(writer_app).build().await);
    // A writer publishes its first snapshot once the runtime is ready, which
    // `load_components` reports and `load_datasets` alone does not.
    tokio::time::timeout(
        Duration::from_secs(30),
        Arc::clone(&writer).load_components(),
    )
    .await
    .expect("the writer must finish loading");
    wait_for_count(&writer, "orders", 3).await;

    let metadata = directory.path().join("state/snapshots/metadata.json");
    let current_snapshot_id = || async {
        let contents = tokio::fs::read_to_string(&metadata).await.ok()?;
        let metadata: serde_json::Value = serde_json::from_str(&contents).ok()?;
        metadata.get("orders")?.get("current-snapshot-id")?.as_u64()
    };
    assert!(
        test_framework::utils::wait_until_true(Duration::from_secs(15), || async {
            current_snapshot_id().await.is_some()
        })
        .await,
        "the writer must publish a current snapshot to {} (runtime ready: {}, components: {:?}, files: {:?})",
        metadata.display(),
        writer.status.is_ready(),
        writer.status.get_all_statuses(),
        walk(&directory.path().join("state")),
    );
    writer.shutdown().await;
    let recorded: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&metadata).expect("read snapshot metadata"))
            .expect("parse snapshot metadata");
    assert_eq!(
        recorded.get("location").and_then(serde_json::Value::as_str),
        Some(format!("file://{}/state/snapshots/", directory.path().display()).as_str()),
        "the snapshot location is the `snapshots/` folder of `runtime.state.location`"
    );
    let mut state_root: Vec<_> = std::fs::read_dir(directory.path().join("state"))
        .expect("list state location")
        .map(|entry| entry.expect("state entry").file_name())
        .filter(|name| name != ".spice-object-store-occ.lock")
        .collect();
    state_root.sort();
    assert_eq!(
        state_root,
        ["_spice", "snapshots"],
        "snapshots are kept in their own folder of the state location"
    );

    let reader_app = app::AppBuilder::new("snapshot_reader")
        .with_runtime(runtime_config)
        .with_dataset(cayenne_dataset(
            directory.path(),
            "reader_rows.csv",
            "reader",
            spicepod::acceleration::Acceleration {
                refresh_mode: Some(spicepod::acceleration::RefreshMode::Snapshot),
                refresh_check_interval: Some("100ms".to_string()),
                snapshots: spicepod::acceleration::SnapshotBehavior::BootstrapOnly,
                ..Default::default()
            },
        ))
        .build();
    let reader = Arc::new(Runtime::builder().with_app(reader_app).build().await);
    Arc::clone(&reader).load_datasets().await;
    // The snapshot holds the writer's three rows; the reader's own source has five.
    wait_for_count(&reader, "orders", 3).await;
    reader.shutdown().await;
}
