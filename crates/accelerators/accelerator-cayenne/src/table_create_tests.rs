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

//! What starting a file-mode dataset leaves behind, driven through the calls the
//! runtime makes: `init`, then the table create, then the dataset checkpoint lookup
//! that decides whether the initial load is skipped. Each start is a fresh accelerator
//! over the same directories.

use std::path::Path;
use std::sync::Arc;

use app::AppBuilder;
use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use data_accelerator_api::{AccelerationSource, AcceleratorEngineRegistry, DataAccelerator};
use datafusion::common::TableReference;
use datafusion::datasource::TableProvider;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::collect;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use runtime_acceleration::Engine;
use runtime_acceleration::acceleration::{Acceleration, Mode, RefreshMode};
use runtime_acceleration::sidecar::OpenOption;
use runtime_acceleration::snapshot::SnapshotBehavior;
use runtime_acceleration::testing::TestAccelerationSource;
use spicepod::component::runtime::RuntimeState;

use crate::{CayenneAccelerator, metastore::CAYENNE_VNEXT_PARAM};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

/// `name` under `runtime.params.cayenne_vnext: {flag}`, in `mode`, with its data and
/// metastore under `base`.
fn dataset(base: &Path, name: &str, flag: &str, mode: Mode) -> TestAccelerationSource {
    let mut app = AppBuilder::new("metastore-move")
        .with_runtime_params(
            [(CAYENNE_VNEXT_PARAM.to_string(), flag.to_string())]
                .into_iter()
                .collect(),
        )
        .build();
    // The state location is set whatever the flag, as an app keeps it when the flag
    // changes: a local directory here. With the flag on a table's data is there; with
    // it off it stays at `cayenne_file_path`.
    let state = base.join("state");
    std::fs::create_dir_all(&state).expect("state directory");
    app.runtime.state = Some(RuntimeState {
        location: url::Url::from_directory_path(&state)
            .expect("state location URL")
            .to_string(),
        params: None,
    });
    let mut params = vec![(
        "cayenne_metadata_dir".to_string(),
        base.join("metadata").to_string_lossy().into_owned(),
    )];
    if flag != "enabled" {
        params.push((
            "cayenne_file_path".to_string(),
            base.join("data").to_string_lossy().into_owned(),
        ));
    }
    TestAccelerationSource::new(name)
        .with_app(Arc::new(app))
        .with_acceleration(Acceleration {
            enabled: true,
            engine: Engine::Cayenne,
            mode,
            refresh_mode: Some(RefreshMode::Full),
            params: params.into_iter().collect(),
            ..Default::default()
        })
}

/// A start: a fresh accelerator runs `init` and creates the table, as the runtime does.
async fn start(
    source: &TestAccelerationSource,
) -> (Arc<CayenneAccelerator>, Arc<dyn TableProvider>) {
    start_with(Arc::new(CayenneAccelerator::new()), source).await
}

async fn start_with(
    accelerator: Arc<CayenneAccelerator>,
    source: &TestAccelerationSource,
) -> (Arc<CayenneAccelerator>, Arc<dyn TableProvider>) {
    let registry = Arc::new(AcceleratorEngineRegistry::new());
    registry
        .register_accelerator_engine(
            Engine::Cayenne,
            Arc::clone(&accelerator) as Arc<dyn DataAccelerator>,
        )
        .await;
    accelerator.init(source).await.expect("init");
    let acceleration = source.acceleration().expect("acceleration").clone();
    let table = registry
        .create_accelerator_table(
            TableReference::bare(source.name().to_string()),
            schema(),
            None,
            &acceleration,
            source.secrets(),
            Some(source),
            Arc::new(SessionContext::new()),
        )
        .await
        .expect("create the table");
    (accelerator, table)
}

/// A flag change must refuse the existing acceleration until explicitly recreated.
async fn assert_native_move_refused(source: &TestAccelerationSource) {
    let error = CayenneAccelerator::new()
        .init(source)
        .await
        .expect_err("a flag change alone must not move an existing acceleration");
    assert!(matches!(
        error.downcast_ref::<crate::Error>(),
        Some(crate::Error::InvalidConfiguration { .. })
    ));
    let message = error.to_string();
    assert!(
        message.contains(&format!(
            "Dataset '{}' is accelerated in the object-native Cayenne metastore",
            source.name()
        )),
        "{message}"
    );
    assert!(
        message.contains("`runtime.params.cayenne_vnext` is not enabled"),
        "{message}"
    );
    assert!(
        message.contains("`acceleration.mode: file_create`"),
        "{message}"
    );
    eprintln!("refused implicit metastore move: {message}");
}

/// What the runtime does after a load: write the rows, then record the load.
async fn load(
    accelerator: &CayenneAccelerator,
    source: &TestAccelerationSource,
    table: &Arc<dyn TableProvider>,
    ids: std::ops::Range<i64>,
) {
    let ids: Vec<i64> = ids.collect();
    let values: Vec<i64> = ids.iter().map(|id| id * 10).collect();
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .expect("batch");
    let ctx = SessionContext::new();
    let input = MemorySourceConfig::try_new_exec(&[vec![batch]], schema(), None).expect("input");
    let plan = table
        .insert_into(&ctx.state(), input, InsertOp::Overwrite)
        .await
        .expect("plan the load");
    collect(plan, ctx.task_ctx()).await.expect("load");
    accelerator
        .sidecar(source, OpenOption::CreateIfNotExists)
        .await
        .expect("sidecar")
        .dataset_checkpointer(SnapshotBehavior::Disabled)
        .await
        .expect("checkpointer")
        .checkpoint(&schema(), None)
        .await
        .expect("record the load");
}

/// Whether the runtime would take the table as loaded and skip its initial load.
async fn marked_loaded(accelerator: &CayenneAccelerator, source: &TestAccelerationSource) -> bool {
    match accelerator.sidecar(source, OpenOption::OpenExisting).await {
        Ok(sidecar) => {
            sidecar
                .dataset_checkpointer(SnapshotBehavior::Disabled)
                .await
                .expect("checkpointer")
                .exists()
                .await
        }
        Err(_) => false,
    }
}

async fn row_count(table: &Arc<dyn TableProvider>) -> usize {
    let ctx = SessionContext::new();
    let plan = table
        .scan(&ctx.state(), None, &[], None)
        .await
        .expect("plan the scan");
    collect(plan, ctx.task_ctx())
        .await
        .expect("scan")
        .iter()
        .map(RecordBatch::num_rows)
        .sum()
}

/// The `cayenne-vnext` sequence: loaded on `SQLite` with the flag off, moved to the
/// object-native metastore by a `file_create` start with the flag on, then started with
/// the flag off again. A plain file-mode start is refused. An explicit `file_create`
/// moves it to `SQLite` empty, and the load recorded for the first table must not mark
/// it loaded: the runtime must load the source before reporting a ready dataset.
#[tokio::test]
async fn a_table_created_empty_after_a_metastore_move_is_not_marked_loaded() {
    let base = tempfile::tempdir().expect("temp dir");
    let name = "before_flag";

    let flag_off = dataset(base.path(), name, "disabled", Mode::File);
    let (accelerator, table) = start(&flag_off).await;
    load(&accelerator, &flag_off, &table, 0..2_000).await;
    assert_eq!(row_count(&table).await, 2_000);
    drop((accelerator, table));

    let moved = dataset(base.path(), name, "enabled", Mode::FileCreate);
    let (accelerator, table) = start(&moved).await;
    assert_eq!(row_count(&table).await, 0, "file_create starts empty");
    load(&accelerator, &moved, &table, 0..2_000).await;
    drop((accelerator, table));

    let back = dataset(base.path(), name, "disabled", Mode::File);
    assert_native_move_refused(&back).await;
    let back = dataset(base.path(), name, "disabled", Mode::FileCreate);
    let (accelerator, table) = start(&back).await;
    assert_eq!(
        (
            row_count(&table).await,
            marked_loaded(&accelerator, &back).await
        ),
        (0, false),
        "the table is new on SQLite and empty, so it must not be marked loaded"
    );
}

/// `name` under `runtime.params.cayenne_vnext: enabled` and an `s3://` state location,
/// with no `cayenne_file_path`, so a new table keeps its data files in the location.
fn placed_dataset(base: &Path, name: &str, mode: Mode) -> TestAccelerationSource {
    in_location(base, name, "enabled", mode)
}

/// `name` under `runtime.params.cayenne_vnext: {flag}` and an `s3://` state location,
/// with no `cayenne_file_path`.
fn in_location(base: &Path, name: &str, flag: &str, mode: Mode) -> TestAccelerationSource {
    let mut app = AppBuilder::new("placement")
        .with_runtime_params(
            [(CAYENNE_VNEXT_PARAM.to_string(), flag.to_string())]
                .into_iter()
                .collect(),
        )
        .build();
    app.runtime.state = Some(RuntimeState {
        location: "s3://bucket/state/".to_string(),
        params: None,
    });
    TestAccelerationSource::new(name)
        .with_app(Arc::new(app))
        .with_acceleration(Acceleration {
            enabled: true,
            engine: Engine::Cayenne,
            mode,
            refresh_mode: Some(RefreshMode::Full),
            params: [
                (
                    "cayenne_metadata_dir".to_string(),
                    base.join("metadata").to_string_lossy().into_owned(),
                ),
                // Rows in data files, so the location holds them as files.
                ("cayenne_inline_max_rows".to_string(), "0".to_string()),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        })
}

/// A fresh accelerator whose object-native catalog is the one in `store`, an in-memory
/// store standing in for the location's bucket.
async fn placed_accelerator(store: &Arc<dyn ObjectStore>) -> Arc<CayenneAccelerator> {
    let catalog = cayenne::metastore::object_native::ObjectNativeCatalog::open_or_create_at(
        Arc::clone(store),
        StorePath::from("state/cayenne/v1/catalog"),
        "s3://bucket/state/cayenne/v1",
    )
    .await
    .expect("open the catalog")
    .with_data_store(Arc::clone(store));
    cayenne::MetadataCatalog::init(&catalog)
        .await
        .expect("init the catalog");
    let accelerator = Arc::new(CayenneAccelerator::new());
    assert!(
        accelerator.native_catalog.set(Arc::new(catalog)).is_ok(),
        "the location's catalog is the accelerator's"
    );
    accelerator
}

/// The number of data files of `name` in the location.
async fn data_files_in_location(store: &Arc<dyn ObjectStore>, name: &str) -> usize {
    let mut data_files = 0;
    let mut listing = store.list(Some(&StorePath::from("state/cayenne/v1/datasets")));
    while let Some(meta) = futures::StreamExt::next(&mut listing).await {
        let meta = meta.expect("list the location");
        let key = meta.location.as_ref();
        if key.contains(&format!("/{name}-")) && key.contains("/data/") && key.ends_with(".vortex")
        {
            data_files += 1;
        }
    }
    data_files
}

/// A dataset whose data files go to the state location has no local data directory:
/// its start creates none under `.spice/data`, and its rows land in the location.
#[tokio::test]
async fn a_dataset_placed_in_the_state_location_creates_no_local_data_directory() {
    let base = tempfile::tempdir().expect("temp dir");
    let name = format!("placed_{}", uuid::Uuid::now_v7().simple());
    let local_dir = Path::new(&data_accelerator_api::spice_data_base_path()).join(&name);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let source = placed_dataset(base.path(), &name, Mode::File);

    let (accelerator, table) = start_with(placed_accelerator(&store).await, &source).await;
    load(&accelerator, &source, &table, 0..500).await;
    let created_locally = local_dir.exists();
    if created_locally {
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    assert_eq!(
        (
            row_count(&table).await,
            created_locally,
            data_files_in_location(&store, &name).await > 0
        ),
        (500, false, true),
        "the rows are in the location, and no '{}' is created",
        local_dir.display()
    );
}

/// A `mode: file_create` start of a dataset whose data is in the state location says,
/// at the default log level, that its previous folder is deleted: the line is under
/// the operator-facing target, not the accelerator crate's own, which shows only
/// warnings by default.
#[tokio::test]
async fn a_placed_dataset_dropped_for_file_create_says_so_at_the_default_level() {
    let base = tempfile::tempdir().expect("temp dir");
    let name = format!("placed_{}", uuid::Uuid::now_v7().simple());
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let source = placed_dataset(base.path(), &name, Mode::File);
    let (accelerator, table) = start_with(placed_accelerator(&store).await, &source).await;
    load(&accelerator, &source, &table, 0..100).await;
    drop((accelerator, table));

    let logs = crate::tests::capture_logs();
    let recreate = placed_dataset(base.path(), &name, Mode::FileCreate);
    let (_accelerator, table) = start_with(placed_accelerator(&store).await, &recreate).await;
    assert_eq!(row_count(&table).await, 0, "file_create starts empty");
    assert_eq!(
        logs.occurrences_of(&format!(
            "spiced::acceleration::cayenne: Dataset '{name}' (cayenne) dropped its acceleration for `mode: file_create`"
        )),
        1,
        "the drop is reported once, under the operator-facing target"
    );
}

/// The other direction of the `cayenne-vnext` sequence: created in the object-native
/// metastore with the flag on, explicitly recreated on `SQLite` with the flag off,
/// then moved back by a `file_create` start with the flag on. A flag change alone is
/// refused; each explicit move starts empty and unloaded instead of serving a stale
/// copy from the other metastore.
#[tokio::test]
async fn a_file_create_move_back_to_the_object_native_metastore_starts_empty() {
    let base = tempfile::tempdir().expect("temp dir");
    let name = "after_flag";

    let flag_on = dataset(base.path(), name, "enabled", Mode::File);
    let (accelerator, table) = start(&flag_on).await;
    load(&accelerator, &flag_on, &table, 0..3_000).await;
    drop((accelerator, table));

    let flag_off = dataset(base.path(), name, "disabled", Mode::File);
    assert_native_move_refused(&flag_off).await;
    let flag_off = dataset(base.path(), name, "disabled", Mode::FileCreate);
    let (accelerator, table) = start(&flag_off).await;
    assert_eq!(
        (
            row_count(&table).await,
            marked_loaded(&accelerator, &flag_off).await
        ),
        (0, false),
        "file_create moves to SQLite empty and unloaded"
    );
    load(&accelerator, &flag_off, &table, 0..2_900).await;
    drop((accelerator, table));

    let moved_back = dataset(base.path(), name, "enabled", Mode::FileCreate);
    let (accelerator, table) = start(&moved_back).await;
    assert_eq!(
        (
            row_count(&table).await,
            marked_loaded(&accelerator, &moved_back).await
        ),
        (0, false),
        "file_create starts empty and unloaded, not from the older object-native copy"
    );
}

/// The same move back with the older object-native copy's data in the state location,
/// which a local directory removal does not reach: `file_create` must not serve its
/// rows, which missed every change made while the flag was off.
#[tokio::test]
async fn a_file_create_move_back_does_not_serve_the_older_copy_in_the_location() {
    let base = tempfile::tempdir().expect("temp dir");
    let name = format!("moved_{}", uuid::Uuid::now_v7().simple());
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let flag_on = in_location(base.path(), &name, "enabled", Mode::File);
    let (accelerator, table) = start_with(placed_accelerator(&store).await, &flag_on).await;
    load(&accelerator, &flag_on, &table, 0..3_000).await;
    drop((accelerator, table));

    // A flag change alone is refused while the object-native catalog holds the table,
    // so the move to SQLite is an explicit `file_create`.
    let flag_off = in_location(base.path(), &name, "disabled", Mode::FileCreate);
    let (accelerator, table) = start_with(placed_accelerator(&store).await, &flag_off).await;
    load(&accelerator, &flag_off, &table, 0..2_900).await;
    drop((accelerator, table));

    let moved_back = in_location(base.path(), &name, "enabled", Mode::FileCreate);
    let (accelerator, table) = start_with(placed_accelerator(&store).await, &moved_back).await;
    let served = row_count(&table).await;
    let loaded = marked_loaded(&accelerator, &moved_back).await;
    let local_dir = Path::new(&data_accelerator_api::spice_data_base_path()).join(&name);
    let _ = std::fs::remove_dir_all(&local_dir);
    assert_eq!(
        (served, loaded),
        (0, false),
        "file_create starts empty and unloaded, not from the older copy in the location"
    );
}

async fn scanned_ids(table: &Arc<dyn TableProvider>) -> Vec<i64> {
    let ctx = SessionContext::new();
    let plan = table
        .scan(&ctx.state(), None, &[], None)
        .await
        .expect("plan the scan");
    let batches = collect(plan, ctx.task_ctx()).await.expect("scan");
    let mut ids = Vec::new();
    for batch in batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        ids.extend(column.iter().map(|v| v.expect("id is not null")));
    }
    ids.sort_unstable();
    ids
}

/// Two accelerator instances, same table name, different data and metadata
/// directories: each must serve only the rows it loaded.
#[tokio::test]
async fn two_accelerators_same_table_name_different_dirs_keep_separate_rows() {
    let left_dir = tempfile::tempdir().expect("left dir");
    let right_dir = tempfile::tempdir().expect("right dir");
    let left = dataset(left_dir.path(), "orders", "disabled", Mode::File);
    let right = dataset(right_dir.path(), "orders", "disabled", Mode::File);

    let (left_acc, left_table) = start(&left).await;
    load(&left_acc, &left, &left_table, 1..4).await;
    let (right_acc, right_table) = start(&right).await;
    load(&right_acc, &right, &right_table, 10..12).await;

    assert_eq!(
        scanned_ids(&left_table).await,
        vec![1, 2, 3],
        "left accelerator must not serve the right accelerator's rows"
    );
    assert_eq!(
        scanned_ids(&right_table).await,
        vec![10, 11],
        "right accelerator must not serve the left accelerator's rows"
    );
}

/// One accelerator, two sources with the same table name and different
/// directories. The catalog `OnceCell` must not bind both to the first
/// metadata directory.
#[tokio::test]
async fn one_accelerator_same_table_name_different_dirs_keep_separate_rows() {
    let left_dir = tempfile::tempdir().expect("left dir");
    let right_dir = tempfile::tempdir().expect("right dir");
    let left = dataset(left_dir.path(), "orders", "disabled", Mode::File);
    let right = dataset(right_dir.path(), "orders", "disabled", Mode::File);
    let accelerator = Arc::new(CayenneAccelerator::new());

    let (accelerator, left_table) = start_with(Arc::clone(&accelerator), &left).await;
    load(&accelerator, &left, &left_table, 1..4).await;
    let (accelerator, right_table) = start_with(accelerator, &right).await;
    load(&accelerator, &right, &right_table, 10..12).await;

    assert_eq!(
        scanned_ids(&left_table).await,
        vec![1, 2, 3],
        "the first table must keep its own rows after a second table of the same name is created on another directory"
    );
    assert_eq!(
        scanned_ids(&right_table).await,
        vec![10, 11],
        "the second table must not open the first table's catalog entry"
    );
}

/// Two accelerator instances with no `cayenne_file_path` / `cayenne_metadata_dir`
/// share the process default `.spice/data` catalog. Same table name must still
/// not mix rows: that is the in-process `ClusterHarness` and multi-Runtime case.
#[tokio::test]
async fn two_accelerators_same_table_name_default_dirs_keep_separate_rows() {
    assert!(
        std::env::var("NEXTEST_EXECUTION_MODE").as_deref() == Ok("process-per-test"),
        "this test changes the working directory, so it must run in a process of its own: run it with `cargo nextest run -p accelerator-cayenne --lib`"
    );
    let cwd = tempfile::tempdir().expect("cwd");
    std::env::set_current_dir(cwd.path()).expect("change the working directory");

    let left = TestAccelerationSource::new("orders").with_acceleration(Acceleration {
        enabled: true,
        engine: Engine::Cayenne,
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Full),
        ..Default::default()
    });
    let right = TestAccelerationSource::new("orders").with_acceleration(Acceleration {
        enabled: true,
        engine: Engine::Cayenne,
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Full),
        ..Default::default()
    });

    let (left_acc, left_table) = start(&left).await;
    load(&left_acc, &left, &left_table, 1..4).await;
    let (right_acc, right_table) = start(&right).await;
    load(&right_acc, &right, &right_table, 10..12).await;

    assert_eq!(
        scanned_ids(&left_table).await,
        vec![1, 2, 3],
        "default-path left accelerator must not serve the right accelerator's rows"
    );
    assert_eq!(
        scanned_ids(&right_table).await,
        vec![10, 11],
        "default-path right accelerator must not serve the left accelerator's rows"
    );
}

/// Two accelerator instances, same table name, same explicit data and metadata
/// directories, both alive: the in-process `ClusterHarness` copies one app onto
/// every executor, so the configured paths collide. Each must still serve only
/// the rows it loaded.
#[tokio::test]
async fn two_accelerators_same_explicit_dirs_keep_separate_rows() {
    let dir = tempfile::tempdir().expect("shared dir");
    let left = dataset(dir.path(), "orders", "disabled", Mode::File);
    let right = dataset(dir.path(), "orders", "disabled", Mode::File);

    let (left_acc, left_table) = start(&left).await;
    load(&left_acc, &left, &left_table, 1..4).await;
    let (right_acc, right_table) = start(&right).await;
    load(&right_acc, &right, &right_table, 10..12).await;

    assert_eq!(
        scanned_ids(&left_table).await,
        vec![1, 2, 3],
        "left accelerator must not serve the right accelerator's rows when configured paths collide"
    );
    assert_eq!(
        scanned_ids(&right_table).await,
        vec![10, 11],
        "right accelerator must not serve the left accelerator's rows when configured paths collide"
    );
}

/// A restart drops the previous accelerator and opens the same directories
/// again. The instance that released the path must not leave a suffix that
/// would miss the files still on disk.
#[tokio::test]
async fn dropped_accelerator_releases_dir_so_restart_reuses_path() {
    let dir = tempfile::tempdir().expect("dir");
    let source = dataset(dir.path(), "orders", "disabled", Mode::File);

    let first = CayenneAccelerator::new();
    let first_data = first.cayenne_data_dir(&source).expect("first data dir");
    let first_meta = first.metadata_dir_for(source.acceleration());
    drop(first);

    let second = CayenneAccelerator::new();
    let second_data = second.cayenne_data_dir(&source).expect("second data dir");
    let second_meta = second.metadata_dir_for(source.acceleration());

    assert_eq!(
        first_data, second_data,
        "a restart must reopen the original data directory, not an instance-suffixed sibling"
    );
    assert_eq!(
        first_meta, second_meta,
        "a restart must reopen the original metadata directory, not an instance-suffixed sibling"
    );
    assert!(
        !second_data.contains("-instance-"),
        "restart path must be the configured directory: {second_data}"
    );
}
