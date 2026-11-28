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

//! A `refresh_mode: snapshot` reader restarted over files a killed restore left
//! behind restores the newer snapshot in full. At open, the instance serving the
//! older local snapshot retires the leftover directory; its sweep must not
//! delete the files the restore then keeps or writes there. Driven through the
//! calls the runtime makes: open, `maintain_after_snapshot_restore`,
//! `download_if_newer`, `reload_from_snapshot`.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use accelerator_cayenne::CayenneAccelerator;
use arrow::{
    array::{Int64Array, RecordBatch},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use async_trait::async_trait;
use cayenne::CayenneTableProvider;
use data_accelerator_api::{
    AccelerationSource, AcceleratorEngineRegistry, DataAccelerator, ReloadProviderFactory,
};
use datafusion::{
    common::TableReference,
    datasource::{TableProvider, memory::MemorySourceConfig},
    execution::context::SessionContext,
    logical_expr::dml::InsertOp,
    physical_plan::collect,
};
use runtime_acceleration::{
    Engine,
    acceleration::{Acceleration, Mode},
    dataset_checkpoint::{self, DatasetCheckpointer},
    snapshot::{
        AccelerationEngine, ForceCreate, SnapshotBehavior, SnapshotManager, SnapshotPoll,
        SnapshotPollOutcome,
    },
    testing::TestAccelerationSource,
};
use runtime_secrets::Secrets;
use spice_table::{LayerWalk, find_concrete};
use spicepod::{
    acceleration::{SnapshotsCompaction, SnapshotsCreationPolicy},
    component::snapshot::{BootstrapOnFailureBehavior, Snapshots},
};
use tokio::sync::{Mutex, RwLock};

const OLD_ROWS: i64 = 2_000;
const NEW_ROWS: i64 = 6_000;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

/// A file-mode Cayenne acceleration of `t` under `dir`.
fn source(dir: &Path) -> TestAccelerationSource {
    TestAccelerationSource::new("t").with_acceleration(Acceleration {
        enabled: true,
        engine: Engine::Cayenne,
        mode: Mode::File,
        params: [
            (
                "cayenne_file_path".to_string(),
                dir.join("data").to_string_lossy().into_owned(),
            ),
            (
                "cayenne_metadata_dir".to_string(),
                dir.join("metadata").to_string_lossy().into_owned(),
            ),
            // Rows in files, so each snapshot has a directory of data files.
            ("cayenne_inline_max_rows".to_string(), "0".to_string()),
        ]
        .into_iter()
        .collect(),
        ..Default::default()
    })
}

/// A fresh process: an accelerator and the registry that serves it.
async fn process() -> (Arc<CayenneAccelerator>, Arc<AcceleratorEngineRegistry>) {
    let accelerator = Arc::new(CayenneAccelerator::new());
    let registry = Arc::new(AcceleratorEngineRegistry::new());
    registry
        .register_accelerator_engine(
            Engine::Cayenne,
            Arc::clone(&accelerator) as Arc<dyn DataAccelerator>,
        )
        .await;
    (accelerator, registry)
}

async fn open(
    registry: &Arc<AcceleratorEngineRegistry>,
    source: &TestAccelerationSource,
) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
    let acceleration = source.acceleration().expect("acceleration").clone();
    registry
        .create_accelerator_table(
            TableReference::bare("t"),
            schema(),
            None,
            &acceleration,
            source.secrets(),
            Some(source),
            Arc::new(SessionContext::new()),
        )
        .await
        .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { Box::new(error) })
}

fn cayenne(provider: &Arc<dyn TableProvider>) -> &CayenneTableProvider {
    find_concrete::<CayenneTableProvider>(provider.as_ref(), LayerWalk::Write)
        .expect("a Cayenne table")
}

/// Replace the table's rows with `ids`, each with `v = id * 10`.
async fn overwrite(table: &Arc<dyn TableProvider>, ids: std::ops::Range<i64>) {
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
        .expect("plan overwrite");
    collect(plan, ctx.task_ctx()).await.expect("overwrite");
}

/// Every `(id, v)` row of `table`, sorted.
async fn rows(table: Arc<dyn TableProvider>) -> Vec<(i64, i64)> {
    let ctx = SessionContext::new();
    let plan = table
        .scan(&ctx.state(), None, &[], None)
        .await
        .expect("plan the scan");
    let mut rows = Vec::new();
    for batch in collect(plan, ctx.task_ctx()).await.expect("scan") {
        let column = |index: usize| {
            batch
                .column(index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("an Int64 column")
                .clone()
        };
        let (ids, values) = (column(0), column(1));
        rows.extend(
            ids.values()
                .iter()
                .copied()
                .zip(values.values().iter().copied()),
        );
    }
    rows.sort_unstable();
    rows
}

/// The rows `overwrite` writes for `ids`.
fn expected_rows(ids: std::ops::Range<i64>) -> Vec<(i64, i64)> {
    ids.map(|id| (id, id * 10)).collect()
}

/// The schema-less checkpoint a restore bootstraps from the snapshot metadata.
struct NoCheckpoint;

#[async_trait]
impl DatasetCheckpointer for NoCheckpoint {
    async fn exists(&self) -> bool {
        false
    }
    async fn checkpoint(&self, _: &SchemaRef, _: Option<&str>) -> dataset_checkpoint::Result<()> {
        Ok(())
    }
    async fn get_schema(&self) -> dataset_checkpoint::Result<Option<SchemaRef>> {
        Ok(None)
    }
    async fn last_checkpoint_time(
        &self,
    ) -> dataset_checkpoint::Result<Option<std::time::SystemTime>> {
        Ok(None)
    }
    async fn get_refresh_sql(&self) -> dataset_checkpoint::Result<Option<String>> {
        Ok(None)
    }
    async fn set_schema(&self, _: &SchemaRef) -> dataset_checkpoint::Result<()> {
        Ok(())
    }
    async fn delete(&self) -> dataset_checkpoint::Result<()> {
        Ok(())
    }
}

/// The dataset's snapshot manager as the runtime builds it, over a `file://`
/// snapshot location.
async fn manager(
    accelerator: &CayenneAccelerator,
    source: &TestAccelerationSource,
    location: &Path,
    secrets: &Arc<RwLock<Secrets>>,
) -> SnapshotManager {
    let snapshots = Snapshots {
        enabled: true,
        location: Some(
            url::Url::from_directory_path(location)
                .expect("directory url")
                .to_string(),
        ),
        bootstrap_on_failure_behavior: BootstrapOnFailureBehavior::Warn,
        params: None,
    };
    let behavior = SnapshotBehavior::Enabled(
        Arc::new(snapshots),
        Arc::downgrade(secrets),
        tokio::runtime::Handle::current(),
        SnapshotsCompaction::Disabled,
    );
    let engine = accelerator
        .snapshot_engine_for_source(source)
        .await
        .expect("a Cayenne snapshot engine");
    SnapshotManager::try_new(
        "t".to_string(),
        behavior,
        accelerator.acceleration_layout(source),
        AccelerationEngine::Cayenne,
    )
    .await
    .expect("a snapshot manager")
    .with_snapshot_engine(engine)
    .with_snapshots_creation_policy(SnapshotsCreationPolicy::Always)
    .with_checkpointer_factory(dataset_checkpoint::make_checkpointer_factory(|| async {
        Ok(Arc::new(NoCheckpoint) as Arc<dyn DatasetCheckpointer>)
    }))
}

async fn publish(manager: &SnapshotManager, rows: i64) {
    let lock = Arc::new(Mutex::new(())).lock_owned().await;
    manager
        .create_snapshot(
            &schema(),
            lock,
            None,
            Some(u64::try_from(rows).expect("positive")),
            ForceCreate(true),
        )
        .await
        .expect("publish a snapshot")
        .expect("a snapshot is created");
}

/// Every file under `dir`, sorted.
fn files_under(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir).expect("list dir") {
        let path = entry.expect("entry").path();
        if path.is_dir() {
            files.extend(files_under(&path));
        } else {
            files.push(path);
        }
    }
    files.sort();
    files
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_over_a_partial_restore_serves_the_full_newer_snapshot() {
    let root = tempfile::tempdir().expect("tempdir");
    let location = root.path().join("snapshots");
    std::fs::create_dir_all(&location).expect("snapshot location");
    let secrets = Arc::new(RwLock::new(Secrets::new()));
    let writer_source = source(&root.path().join("writer"));
    let reader_source = source(&root.path().join("reader"));

    // The writer publishes snapshot N, which the reader restores.
    let (writer_accelerator, writer_registry) = process().await;
    let writer = open(&writer_registry, &writer_source)
        .await
        .expect("open the writer");
    overwrite(&writer, 0..OLD_ROWS).await;
    let writer_manager = manager(&writer_accelerator, &writer_source, &location, &secrets).await;
    publish(&writer_manager, OLD_ROWS).await;
    let (reader_accelerator, _reader_registry) = process().await;
    let old = manager(&reader_accelerator, &reader_source, &location, &secrets)
        .await
        .download_latest_snapshot()
        .await
        .expect("restore N")
        .expect("N is published");

    // The writer publishes N+1. A reader killed while extracting it leaves part
    // of N+1's data files behind; its metastore still holds N.
    overwrite(&writer, 0..NEW_ROWS).await;
    publish(&writer_manager, NEW_ROWS).await;
    let writer_table = cayenne(&writer);
    let new_dir = writer_table.current_snapshot_id();
    let relative = PathBuf::from(&writer_table.metadata().table_id).join(&new_dir);
    let new_snapshot_dir = PathBuf::from(&writer_table.metadata().path).join(&relative);
    let new_files: Vec<PathBuf> = files_under(&new_snapshot_dir)
        .into_iter()
        .filter(|file| file.extension().is_some_and(|ext| ext == "vortex"))
        .collect();
    assert!(!new_files.is_empty(), "N+1 has data files");
    let leftover_dir = PathBuf::from(
        reader_accelerator
            .cayenne_data_dir(&reader_source)
            .expect("the reader's data dir"),
    )
    .join(&relative);
    for file in &new_files[..new_files.len().div_ceil(2)] {
        let target = leftover_dir.join(file.strip_prefix(&new_snapshot_dir).expect("under N+1"));
        std::fs::create_dir_all(target.parent().expect("parent")).expect("leftover parent");
        std::fs::copy(file, target).expect("copy a leftover file");
    }

    // The restart: the initial provider serves N and reclaims what it does not
    // reference, the leftover directory included.
    let (accelerator, registry) = process().await;
    let initial = open(&registry, &reader_source)
        .await
        .expect("open the reader");
    assert_eq!(rows(Arc::clone(&initial)).await, expected_rows(0..OLD_ROWS));
    accelerator
        .maintain_after_snapshot_restore(&reader_source, &initial)
        .await;
    let retired = cayenne(&initial)
        .retire_unreferenced_snapshot_dirs()
        .await
        .expect("retire");
    assert_eq!(
        retired,
        vec![new_dir.clone()],
        "precondition: the instance serving N retired the leftover of N+1"
    );

    // The first refresh restores N+1 into the same directory.
    let poll = manager(&accelerator, &reader_source, &location, &secrets)
        .await
        .download_if_newer(Some(old.snapshot_id), None, None)
        .await
        .expect("poll");
    let SnapshotPoll {
        outcome: SnapshotPollOutcome::Downloaded(restored),
        ..
    } = poll
    else {
        panic!("N+1 must be downloaded: {poll:?}");
    };
    assert_eq!(restored.snapshot_id, old.snapshot_id + 1);
    let quiesced_before_reload = cayenne(&initial).is_maintenance_closed();

    // The sweep the instance serving N armed at open comes due before the reload
    // swaps it out.
    cayenne(&initial).sweep_retired_snapshot_dirs_now().await;

    let factory: ReloadProviderFactory = {
        let registry = Arc::clone(&registry);
        let source = reader_source.clone();
        Arc::new(move || {
            let registry = Arc::clone(&registry);
            let source = source.clone();
            Box::pin(async move { open(&registry, &source).await })
        })
    };
    let reloaded = accelerator
        .reload_from_snapshot(&reader_source, Arc::clone(&initial), factory)
        .await
        .expect("reload from snapshot");
    assert_eq!(
        rows(reloaded).await,
        expected_rows(0..NEW_ROWS),
        "the reader must serve every row of N+1"
    );
    assert!(
        quiesced_before_reload,
        "the restore quiesces the instance it extracts over"
    );
}
