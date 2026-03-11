/*
Copyright 2025 The Spice.ai OSS Authors

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

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::init_tracing;
use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use data_components::arrow::write::MemTable;
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use runtime::Runtime;
use runtime::accelerated_table::refresh::{AccelerationRefreshMode, Refresh, Refresher};
use runtime::accelerated_table::{SnapshotCreateTrigger, SnapshotCreationConfig};
use runtime::component::dataset::acceleration::RefreshMode;
use runtime::federated_table::FederatedTable;
use runtime::status;
use runtime_acceleration::dataset_checkpoint::DatasetCheckpointer;
use runtime_acceleration::snapshot::{
    AccelerationEngine, SnapshotBehavior as RuntimeSnapshotBehavior, SnapshotManager,
};
use spicepod::acceleration::SnapshotsCompaction;
use spicepod::component::snapshot::Snapshots;
use tokio::sync::{Mutex, RwLock, mpsc};

struct MockCheckpointer;

#[async_trait]
impl DatasetCheckpointer for MockCheckpointer {
    async fn exists(&self) -> bool {
        true
    }

    async fn checkpoint(
        &self,
        _schema: &arrow::datatypes::SchemaRef,
    ) -> runtime_acceleration::dataset_checkpoint::Result<()> {
        Ok(())
    }

    async fn get_schema(
        &self,
    ) -> runtime_acceleration::dataset_checkpoint::Result<Option<arrow::datatypes::SchemaRef>> {
        Ok(None)
    }

    async fn last_checkpoint_time(
        &self,
    ) -> runtime_acceleration::dataset_checkpoint::Result<Option<SystemTime>> {
        Ok(None)
    }
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}_{nanos}"))
}

async fn count_files(root: &Path) -> anyhow::Result<usize> {
    let mut pending = vec![root.to_path_buf()];
    let mut count = 0usize;

    while let Some(dir) = pending.pop() {
        let Ok(mut entries) = tokio::fs::read_dir(&dir).await else {
            continue;
        };

        while let Some(entry) = entries.next_entry().await? {
            let file_type = entry.file_type().await?;
            let path = entry.path();
            if file_type.is_dir() {
                pending.push(path);
            } else {
                count += 1;
            }
        }
    }

    Ok(count)
}

#[tokio::test]
async fn test_snapshot_interval_serializes_with_accelerator_writes() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    let temp_root = unique_temp_dir("snapshot_mutex");
    let snapshot_dir = temp_root.join("snapshots");
    let local_snapshot_file = temp_root.join("acceleration.db");

    tokio::fs::create_dir_all(&snapshot_dir).await?;
    tokio::fs::write(&local_snapshot_file, b"snapshot-data").await?;

    let snapshots = Snapshots {
        enabled: true,
        location: Some(format!("file://{}", snapshot_dir.display())),
        ..Snapshots::default()
    };

    let runtime = Runtime::builder().build().await;
    let snapshot_behavior = RuntimeSnapshotBehavior::create_only(
        Arc::new(snapshots),
        runtime.secrets_weak(),
        runtime.tokio_io_runtime(),
        SnapshotsCompaction::Disabled,
    );

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![1]))],
    )?;

    let mem_table: Arc<dyn TableProvider> =
        Arc::new(MemTable::try_new(Arc::clone(&schema), vec![vec![batch]])?);
    let federated = Arc::new(FederatedTable::new_unchecked(Arc::clone(&mem_table)));
    let accelerator_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![1]))],
    )?;
    let accelerator: Arc<dyn TableProvider> =
        Arc::new(MemTable::try_new(schema, vec![vec![accelerator_batch]])?);

    let refresh = Refresh::new(RefreshMode::Full);
    let accelerator_write_mutex = Arc::new(Mutex::new(()));

    let mut refresher = Refresher::new(
        status::RuntimeStatus::new(),
        TableReference::bare("snapshot_mutex_test"),
        federated,
        Some("mem_table".to_string()),
        Arc::new(RwLock::new(refresh)),
        accelerator,
        None,
        runtime.tokio_io_runtime(),
        Arc::clone(&accelerator_write_mutex),
    );

    let snapshot_manager = SnapshotManager::try_new(
        "snapshot_mutex_test".to_string(),
        snapshot_behavior,
        runtime_acceleration::snapshot::AccelerationLayout::file(local_snapshot_file.clone()),
        AccelerationEngine::DuckDB,
    )
    .await
    .expect("Failed to create snapshot manager");

    refresher.checkpointer(Some(Arc::new(MockCheckpointer)));
    refresher.with_snapshot_creation_config(Some(SnapshotCreationConfig {
        manager: Arc::new(snapshot_manager),
        create_trigger: SnapshotCreateTrigger::RefreshComplete,
    }));

    let (_start_refresh, on_start_refresh) = mpsc::channel(1);
    let refresh_handle = refresher
        .start(AccelerationRefreshMode::Full(on_start_refresh))
        .await?;

    let lock_guard = accelerator_write_mutex.lock().await;
    tokio::time::sleep(Duration::from_millis(450)).await;

    let locked_count = count_files(&snapshot_dir).await?;
    assert_eq!(
        locked_count, 0,
        "Snapshots should not be created while the accelerator mutex is held."
    );
    drop(lock_guard);

    let snapshot_wait = tokio::time::timeout(Duration::from_secs(3), async move {
        loop {
            if count_files(&snapshot_dir).await? > 0 {
                return Ok::<(), anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    snapshot_wait.map_err(|_| anyhow::anyhow!("Timed out waiting for snapshot creation"))??;

    if let Some(handle) = refresh_handle {
        handle.abort();
    }
    drop(refresher);

    tokio::fs::remove_dir_all(&temp_root).await?;

    Ok(())
}
