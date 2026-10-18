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

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use arrow::datatypes::SchemaRef;
use arrow_tools::schema_evolution::{EvolutionContext, SchemaEvolution, classify};
use async_trait::async_trait;
use datafusion::common::Constraints;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use runtime_acceleration::acceleration::Mode;
use runtime_acceleration::snapshot::{AccelerationEngine, SnapshotManager};
use tokio::sync::RwLock;

use crate::accelerated::pinned::{BoxError, PinnedCopyLoader};
use crate::component::dataset::Dataset;
use crate::dataaccelerator::{AcceleratorEngineRegistry, DataAccelerator};
use crate::secrets::Secrets;

/// Names this process's copy directories. Each table that serves a dataset's pin has
/// its own, so a table that replaces another, as a Spicepod reload does, never deletes
/// a copy the other's queries still read.
static PROCESS: LazyLock<String> = LazyLock::new(|| uuid::Uuid::new_v4().simple().to_string());
static NEXT_GENERATION: AtomicU64 = AtomicU64::new(0);

/// Copies of `dataset`'s pinned snapshots, each in its own directory under
/// `<spice data>/pinned/<dataset>/<generation>/`, where the generation is this table's.
pub(super) struct PinnedSnapshotCopies {
    dataset: Dataset,
    accelerator: Arc<dyn DataAccelerator>,
    registry: Arc<AcceleratorEngineRegistry>,
    engine: AccelerationEngine,
    /// The manager that publishes the dataset's snapshots, which reads its pin.
    publisher: Arc<SnapshotManager>,
    /// The schema the live acceleration was created with.
    schema: SchemaRef,
    constraints: Option<Constraints>,
    secrets: Arc<RwLock<Secrets>>,
    ctx: Arc<SessionContext>,
    generation_dir: PathBuf,
}

impl PinnedSnapshotCopies {
    /// Copies for `dataset`, or `None` when its engine cannot hold a second copy.
    /// Deletes the copies an earlier process left.
    #[expect(clippy::too_many_arguments)]
    pub(super) async fn new(
        dataset: &Dataset,
        accelerator: Arc<dyn DataAccelerator>,
        registry: Arc<AcceleratorEngineRegistry>,
        engine: AccelerationEngine,
        publisher: Arc<SnapshotManager>,
        schema: SchemaRef,
        constraints: Option<Constraints>,
        secrets: Arc<RwLock<Secrets>>,
        ctx: Arc<SessionContext>,
    ) -> Option<Self> {
        let dataset_dir = PathBuf::from(data_accelerator_api::spice_data_base_path())
            .join("pinned")
            .join(dir_name(&dataset.name.to_string()));
        let generation_dir = dataset_dir.join(format!(
            "{}-{}",
            *PROCESS,
            NEXT_GENERATION.fetch_add(1, Ordering::Relaxed)
        ));
        // Probes that the engine can relocate the acceleration. Cayenne records the
        // probe's directory, and `release_relocated` forgets it again.
        let probe = accelerator.relocated(dataset, &generation_dir)?;
        let mut probe_dataset = dataset.clone();
        probe_dataset.acceleration = Some(probe);
        accelerator.release_relocated(&probe_dataset).await;
        remove_other_processes_copies(&dataset_dir, &PROCESS).await;
        Some(Self {
            dataset: dataset.clone(),
            accelerator,
            registry,
            engine,
            publisher,
            schema,
            constraints,
            secrets,
            ctx,
            generation_dir,
        })
    }

    fn copy_dir(&self, snapshot_id: u64) -> PathBuf {
        self.generation_dir.join(snapshot_id.to_string())
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
}

#[async_trait]
impl PinnedCopyLoader for PinnedSnapshotCopies {
    async fn pinned_snapshot(&self) -> Result<Option<u64>, BoxError> {
        Ok(self.publisher.pinned_snapshot().await?)
    }

    async fn load(&self, snapshot_id: u64) -> Result<Arc<dyn TableProvider>, BoxError> {
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
        Ok(self
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
            .await?)
    }

    async fn release(&self, snapshot_id: u64) {
        if let Some(copy) = self.copy_dataset(snapshot_id) {
            self.accelerator.release_relocated(&copy).await;
        }
        remove_dir(&self.copy_dir(snapshot_id)).await;
    }
}

impl Drop for PinnedSnapshotCopies {
    /// Deletes this table's directory when it is empty: every copy in it was released.
    fn drop(&mut self) {
        let _ = std::fs::remove_dir(&self.generation_dir);
    }
}

/// Deletes the copies under `dataset_dir` that a process other than `process` left.
async fn remove_other_processes_copies(dataset_dir: &Path, process: &str) {
    let Ok(mut entries) = tokio::fs::read_dir(dataset_dir).await else {
        return;
    };
    let prefix = format!("{process}-");
    while let Ok(Some(entry)) = entries.next_entry().await {
        if !entry.file_name().to_string_lossy().starts_with(&prefix) {
            remove_dir(&entry.path()).await;
        }
    }
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
        Err(err) => tracing::warn!(
            "Failed to delete the pinned snapshot copy at {}: {err}",
            dir.display()
        ),
    }
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

    #[tokio::test]
    async fn only_the_copies_other_processes_left_are_deleted() {
        let dir = tempfile::tempdir().expect("temp dir");
        for copy in ["earlier-0/41", "this-0/41", "this-1/40", "41"] {
            tokio::fs::create_dir_all(dir.path().join(copy))
                .await
                .expect("created");
        }
        remove_other_processes_copies(dir.path(), "this").await;
        let mut left = std::fs::read_dir(dir.path())
            .expect("listed")
            .map(|entry| {
                let name = entry.expect("entry").file_name();
                name.into_string().expect("UTF-8 name")
            })
            .collect::<Vec<_>>();
        left.sort();
        assert_eq!(left, ["this-0", "this-1"]);
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
