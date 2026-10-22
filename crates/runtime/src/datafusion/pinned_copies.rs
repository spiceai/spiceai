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

//! The local copies of a pinned snapshot that an instance refreshing the dataset from
//! its source serves while the dataset is pinned.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow_tools::schema_evolution::{EvolutionContext, SchemaEvolution, classify};
use async_trait::async_trait;
use datafusion::common::Constraints;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use runtime_acceleration::acceleration::Mode;
use runtime_acceleration::snapshot::{AccelerationEngine, SnapshotDownloadError, SnapshotManager};
use tokio::sync::RwLock;

use crate::accelerated::pinned::{BoxError, NotServed, PinnedCopyLoader};
use crate::component::dataset::Dataset;
use crate::dataaccelerator::{AcceleratorEngineRegistry, DataAccelerator};
use crate::secrets::Secrets;

/// The lock a table takes in `<spice data>/pinned/<dataset>/` while it deletes the
/// directories no table holds and claims its own.
const DATASET_LOCK: &str = ".lock";

/// Copies of `dataset`'s pinned snapshots, each in its own directory under
/// `<spice data>/pinned/<dataset>/<table>/`, a directory of this table's own. The table
/// holds the lock on `<table>.lock` while it lives, so a table that starts, in this
/// process or another, deletes only the directories no table holds.
pub(super) struct PinnedSnapshotCopies {
    dataset: Dataset,
    accelerator: Arc<dyn DataAccelerator>,
    registry: Arc<AcceleratorEngineRegistry>,
    engine: AccelerationEngine,
    /// The manager that publishes the dataset's snapshots, which reads its pin.
    publisher: Arc<SnapshotManager>,
    /// The schema the live acceleration was created with.
    schema: SchemaRef,
    /// The schema the live acceleration's table reports, in its engine's types.
    table_schema: SchemaRef,
    constraints: Option<Constraints>,
    secrets: Arc<RwLock<Secrets>>,
    ctx: Arc<SessionContext>,
    table_dir: PathBuf,
    _table_lock: File,
}

impl PinnedSnapshotCopies {
    /// Copies for `dataset`, or `None` when its engine cannot hold a second copy.
    /// Deletes the copies no table holds, such as those of a process that stopped.
    #[expect(clippy::too_many_arguments)]
    pub(super) async fn new(
        dataset: &Dataset,
        accelerator: Arc<dyn DataAccelerator>,
        registry: Arc<AcceleratorEngineRegistry>,
        engine: AccelerationEngine,
        publisher: Arc<SnapshotManager>,
        schema: SchemaRef,
        table_schema: SchemaRef,
        constraints: Option<Constraints>,
        secrets: Arc<RwLock<Secrets>>,
        ctx: Arc<SessionContext>,
    ) -> Option<Self> {
        let dataset_dir = PathBuf::from(data_accelerator_api::spice_data_base_path())
            .join("pinned")
            .join(dir_name(&dataset.name.to_string()));
        let table_dir = dataset_dir.join(uuid::Uuid::new_v4().simple().to_string());
        // Probes that the engine can relocate the acceleration. Cayenne records the
        // probe's directory, and `release_relocated` forgets it again.
        let probe = accelerator.relocated(dataset, &table_dir)?;
        let mut probe_dataset = dataset.clone();
        probe_dataset.acceleration = Some(probe);
        accelerator
            .release_relocated(&probe_dataset, &registry)
            .await;
        let claimed = tokio::task::spawn_blocking({
            let table_dir = table_dir.clone();
            move || claim(&table_dir)
        })
        .await
        .unwrap_or_else(|err| Err(std::io::Error::other(err)));
        let table_lock = match claimed {
            Ok(table_lock) => table_lock,
            Err(err) => {
                tracing::warn!(
                    "Dataset {} cannot serve a pinned snapshot from a local copy, so while it is pinned it serves its live data: failed to lock a directory for the copy under {}: {err}",
                    dataset.name,
                    dataset_dir.display()
                );
                return None;
            }
        };
        Some(Self {
            dataset: dataset.clone(),
            accelerator,
            registry,
            engine,
            publisher,
            schema,
            table_schema,
            constraints,
            secrets,
            ctx,
            table_dir,
            _table_lock: table_lock,
        })
    }

    fn copy_dir(&self, snapshot_id: u64) -> PathBuf {
        self.table_dir.join(snapshot_id.to_string())
    }

    /// The dataset as the copy of `snapshot_id` holds it: the same settings, with its
    /// files in the copy's directory, opened as they are rather than recreated.
    fn copy_dataset(&self, snapshot_id: u64) -> Option<Dataset> {
        let mut acceleration = self
            .accelerator
            .relocated(&self.dataset, &self.copy_dir(snapshot_id))?;
        acceleration.mode = Mode::File;
        let mut copy = self.dataset.clone();
        copy.acceleration = Some(acceleration);
        Some(copy)
    }

    async fn download(&self, copy: &Dataset, snapshot_id: u64) -> Result<SchemaRef, BoxError> {
        let acceleration = copy
            .acceleration
            .as_ref()
            .ok_or("the copy has no acceleration")?;
        let behavior = acceleration.snapshot_behavior.clone();
        let manager = SnapshotManager::try_new(
            copy.name.to_string(),
            behavior.clone(),
            self.accelerator.acceleration_layout(copy),
            self.engine.clone(),
        )
        .await
        .ok_or("its snapshot location could not be opened")?;
        let manager = match self.accelerator.snapshot_engine_for_source(copy).await {
            Some(engine) => manager.with_snapshot_engine(engine),
            None => manager,
        };
        let source: Arc<dyn crate::dataaccelerator::AccelerationSource> = Arc::new(copy.clone());
        let registry = Arc::clone(&self.registry);
        let checkpointer_factory =
            runtime_acceleration::dataset_checkpoint::make_checkpointer_factory(move || {
                let source = Arc::clone(&source);
                let registry = Arc::clone(&registry);
                let behavior = behavior.clone();
                async move {
                    use runtime_acceleration::sidecar::OpenOption;
                    use snafu::ResultExt;
                    crate::dataaccelerator::spice_sys::dataset_checkpointer(
                        source.as_ref(),
                        registry,
                        OpenOption::OpenExisting,
                        behavior,
                    )
                    .await
                    .boxed()
                }
            });
        let downloaded = manager
            .with_checkpointer_factory(checkpointer_factory)
            .download_snapshot(snapshot_id)
            .await?;
        Ok(downloaded.schema)
    }

    async fn open(&self, snapshot_id: u64) -> Result<Arc<dyn TableProvider>, BoxError> {
        let copy = self
            .copy_dataset(snapshot_id)
            .ok_or("its acceleration engine cannot hold a second copy")?;
        tokio::fs::create_dir_all(self.copy_dir(snapshot_id)).await?;

        let snapshot_schema = self.download(&copy, snapshot_id).await?;
        refuse_different_columns(&snapshot_schema, &self.schema)?;

        let acceleration = copy
            .acceleration
            .as_ref()
            .ok_or("the copy has no acceleration")?;
        let provider = self
            .registry
            .create_accelerator_table(
                copy.name.clone(),
                Arc::clone(&self.schema),
                self.constraints.as_ref(),
                acceleration,
                Arc::clone(&self.secrets),
                Some(&copy),
                Arc::clone(&self.ctx),
            )
            .await?;
        // A snapshot that holds no schema of its own, such as Cayenne's, reports the
        // dataset's current schema, so the table opened is checked against the live one.
        refuse_different_columns(&provider.schema(), &self.table_schema)?;
        Ok(provider)
    }
}

#[async_trait]
impl PinnedCopyLoader for PinnedSnapshotCopies {
    async fn pinned_snapshot(&self) -> Result<Option<u64>, BoxError> {
        Ok(self.publisher.pinned_snapshot().await?)
    }

    async fn load(&self, snapshot_id: u64) -> Result<Arc<dyn TableProvider>, NotServed> {
        self.open(snapshot_id).await.map_err(|err| {
            match err.downcast_ref::<SnapshotDownloadError>() {
                Some(SnapshotDownloadError::NotEnoughDisk {
                    snapshot_id,
                    needed,
                    free,
                    ..
                }) => NotServed::NotEnoughDisk {
                    snapshot_id: *snapshot_id,
                    needed: *needed,
                    free: *free,
                },
                _ => NotServed::LoadFailed {
                    reason: err.to_string(),
                },
            }
        })
    }

    async fn release(&self, snapshot_id: u64) {
        if let Some(copy) = self.copy_dataset(snapshot_id) {
            self.accelerator
                .release_relocated(&copy, &self.registry)
                .await;
        }
        remove_dir(&self.copy_dir(snapshot_id)).await;
    }
}

/// The pin of a dataset whose instance cannot serve one: it reads the pin, to report
/// why it is not served, and loads nothing.
pub(super) struct PinNotServed {
    /// The manager that publishes the dataset's snapshots, which reads its pin.
    pub publisher: Arc<SnapshotManager>,
    pub reason: NotServed,
}

#[async_trait]
impl PinnedCopyLoader for PinNotServed {
    async fn pinned_snapshot(&self) -> Result<Option<u64>, BoxError> {
        Ok(self.publisher.pinned_snapshot().await?)
    }

    async fn load(&self, _snapshot_id: u64) -> Result<Arc<dyn TableProvider>, NotServed> {
        Err(self.reason.clone())
    }

    async fn release(&self, _snapshot_id: u64) {}
}

impl Drop for PinnedSnapshotCopies {
    /// Deletes this table's directory when it is empty, as it is once every copy in it
    /// was released, and its lock file.
    fn drop(&mut self) {
        let _ = std::fs::remove_dir(&self.table_dir);
        let _ = std::fs::remove_file(lock_path(&self.table_dir));
    }
}

/// Locks `<table_dir>.lock` for a new table, after deleting the table directories beside
/// it that no table holds. Holds the dataset's lock meanwhile, so no table sees another
/// table's directory or lock file before that table holds its lock.
fn claim(table_dir: &Path) -> std::io::Result<File> {
    let dataset_dir = table_dir
        .parent()
        .ok_or_else(|| std::io::Error::other("the copy directory has no parent"))?;
    std::fs::create_dir_all(dataset_dir)?;
    let dataset_lock = File::create(dataset_dir.join(DATASET_LOCK))?;
    dataset_lock.lock()?;
    remove_unheld(dataset_dir);
    let table_lock = File::create_new(lock_path(table_dir))?;
    table_lock.try_lock()?;
    Ok(table_lock)
}

/// Deletes each table directory under `dataset_dir`, and its lock file, when no table
/// holds the lock.
fn remove_unheld(dataset_dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dataset_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name == DATASET_LOCK {
            continue;
        }
        let table_dir = dataset_dir.join(name.strip_suffix(".lock").unwrap_or(&name));
        let lock_path = lock_path(&table_dir);
        let _unheld = match File::open(&lock_path) {
            Ok(lock) if lock.try_lock().is_ok() => Some(lock),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            // A table holds it, or it cannot be opened: keep the directory.
            Ok(_) | Err(_) => continue,
        };
        match std::fs::remove_dir_all(&table_dir) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                warn_not_deleted(&table_dir, &err);
                continue;
            }
        }
        let _ = std::fs::remove_file(&lock_path);
    }
}

fn lock_path(table_dir: &Path) -> PathBuf {
    let mut path = table_dir.as_os_str().to_owned();
    path.push(".lock");
    PathBuf::from(path)
}

/// Refuses a pinned snapshot whose columns differ from the columns the dataset serves,
/// such as a snapshot taken before a column was added.
fn refuse_different_columns(snapshot: &SchemaRef, serving: &SchemaRef) -> Result<(), BoxError> {
    match classify(
        serving,
        snapshot,
        &EvolutionContext {
            constraint_columns: &[],
        },
    ) {
        SchemaEvolution::Identical => Ok(()),
        SchemaEvolution::Widening(plan) => Err(format!(
            "its columns differ from the columns the dataset serves ({})",
            plan.describe()
        )
        .into()),
        SchemaEvolution::Incompatible { reason } => {
            Err(format!("its columns differ from the columns the dataset serves ({reason})").into())
        }
    }
}

/// `dataset_name` as one directory name, distinct for each dataset name. Letters,
/// digits, `_` and a `.` after the first byte are kept; every other byte becomes `-`
/// and two hex digits, so a name such as `..` never names a directory outside
/// `pinned/`, and the name stays the same when an engine spells the path as a URI.
fn dir_name(dataset_name: &str) -> String {
    use std::fmt::Write as _;
    let mut name = String::with_capacity(dataset_name.len());
    for (index, byte) in dataset_name.bytes().enumerate() {
        if byte.is_ascii_alphanumeric() || byte == b'_' || (byte == b'.' && index > 0) {
            name.push(char::from(byte));
        } else {
            let _ = write!(name, "-{byte:02x}");
        }
    }
    name
}

async fn remove_dir(dir: &Path) {
    match tokio::fs::remove_dir_all(dir).await {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => warn_not_deleted(dir, &err),
    }
}

fn warn_not_deleted(dir: &Path, err: &std::io::Error) {
    tracing::warn!(
        "Failed to delete the pinned snapshot copy at {}: {err}",
        dir.display()
    );
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn schema(fields: &[(&str, DataType)]) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .iter()
                .map(|(name, data_type)| Field::new(*name, data_type.clone(), true))
                .collect::<Vec<_>>(),
        ))
    }

    #[test]
    fn each_dataset_name_is_one_directory_of_its_own() {
        assert_eq!(dir_name("taxi_trips"), "taxi_trips");
        assert_eq!(dir_name("sales.orders"), "sales.orders");
        assert_eq!(dir_name(".."), "-2e.");
        assert_eq!(dir_name("."), "-2e");
        assert_ne!(dir_name("a/b"), dir_name("a_b"));
        assert_ne!(dir_name("a/b"), dir_name("a-2fb"));
    }

    fn names(dir: &Path) -> Vec<String> {
        let mut names = std::fs::read_dir(dir)
            .expect("listed")
            .map(|entry| {
                let name = entry.expect("entry").file_name();
                name.into_string().expect("UTF-8 name")
            })
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    #[test]
    fn a_new_table_deletes_only_the_copies_no_table_holds() {
        let dir = tempfile::tempdir().expect("temp dir");
        let held = claim(&dir.path().join("held")).expect("claimed");
        std::fs::create_dir_all(dir.path().join("held/41")).expect("created");
        // A table of a process that stopped, one that loaded no copy, and a copy
        // without a lock file.
        std::fs::create_dir_all(dir.path().join("stopped/41")).expect("created");
        std::fs::write(dir.path().join("stopped.lock"), "").expect("created");
        std::fs::write(dir.path().join("idle.lock"), "").expect("created");
        std::fs::create_dir_all(dir.path().join("unlocked/40")).expect("created");

        let _new = claim(&dir.path().join("new")).expect("claimed");
        assert_eq!(
            names(dir.path()),
            [".lock", "held", "held.lock", "new.lock"]
        );

        drop(held);
        let _newer = claim(&dir.path().join("newer")).expect("claimed");
        assert_eq!(names(dir.path()), [".lock", "new.lock", "newer.lock"]);
    }

    #[test]
    fn a_snapshot_with_the_columns_served_is_accepted() {
        let serving = schema(&[("id", DataType::Int64), ("fare", DataType::Float64)]);
        refuse_different_columns(&serving, &serving).expect("accepted");
    }

    #[test]
    fn a_snapshot_taken_before_a_column_was_added_is_refused() {
        let serving = schema(&[("id", DataType::Int64), ("fare", DataType::Float64)]);
        let snapshot = schema(&[("id", DataType::Int64)]);
        let err = refuse_different_columns(&snapshot, &serving).expect_err("refused");
        assert!(
            err.to_string().contains("columns differ"),
            "the refusal says why: {err}"
        );
    }
}
