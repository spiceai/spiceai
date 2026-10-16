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
use std::sync::Arc;

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

/// Copies of `dataset`'s pinned snapshots, each in its own directory under
/// `<spice data>/pinned/<dataset>/`.
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
    base_dir: PathBuf,
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
        let base_dir = PathBuf::from(data_accelerator_api::spice_data_base_path())
            .join("pinned")
            .join(dataset.name.to_string().replace(['/', '\\'], "_"));
        // Probes that the engine can relocate the acceleration. Cayenne records the
        // probe's directory, and `release_relocated` forgets it again.
        let probe = accelerator.relocated(dataset, &base_dir)?;
        let mut probe_dataset = dataset.clone();
        probe_dataset.acceleration = Some(probe);
        accelerator.release_relocated(&probe_dataset).await;
        remove_dir(&base_dir).await;
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
            base_dir,
        })
    }

    fn copy_dir(&self, snapshot_id: u64) -> PathBuf {
        self.base_dir.join(snapshot_id.to_string())
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
        // A copy of the same snapshot left by a table this one replaced may still be
        // open in the engine.
        self.release(snapshot_id).await;
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
