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

//! Resolving datasets that read acceleration snapshots (`file_format: snapshot`): see
//! [`crate::component::dataset::snapshot_source`].

use std::sync::Arc;

use app::App;
use datafusion::sql::TableReference;
use runtime_acceleration::Engine;
use runtime_acceleration::snapshot::{SnapshotBehavior, SnapshotManager};
use runtime_metrics as metrics;
use snafu::prelude::*;
use tokio::sync::Semaphore;
use util::{RetryError, fibonacci_backoff::FibonacciBackoffBuilder, retry, warn_spaced};

use crate::component::dataset::{
    Dataset,
    builder::DatasetBuilder,
    snapshot_source::{
        SnapshotSource, cannot_load_snapshot_message, resolved_message, unavailable_engine_cause,
        waiting_for_snapshot_message,
    },
};
use crate::dataaccelerator::{
    AccelerationSource, BootstrapStatus, CayenneSnapshotValidationError, acceleration_file_path,
    validate_snapshot_consistency,
};
use crate::dataconnector::snapshot_source::{
    projected_publisher_message, publisher_column_projection,
};
use crate::datafusion::{DEFAULT_SNAPSHOT_REFRESH_CHECK_INTERVAL, engine_to_acceleration_engine};
use crate::{LogErrors, Runtime, UnableToBuildDatasetSnafu, status};

impl Runtime {
    /// Resolves a dataset that reads acceleration snapshots whose engine is not known
    /// yet: reads the snapshots' `metadata.json` until it describes a current snapshot
    /// for the dataset, records the engine that created it, and returns the dataset
    /// rebuilt with its acceleration, that acceleration initialized — its current
    /// snapshot restored — as startup initializes every other accelerated dataset.
    ///
    /// The dataset is rebuilt from the app `pending` was built from. A hot reload spawns
    /// this load before it installs its app, and a reload that changes or removes the
    /// dataset supersedes this resolution instead (see
    /// [`SnapshotSourceRegistry::begin_resolution`]).
    ///
    /// Retries on the dataset's refresh interval until a snapshot is described, because
    /// a dataset may start before the snapshots it reads are first published. Returns
    /// `None`, having reported why, when the dataset cannot load its snapshots at all;
    /// and returns `None` silently when the runtime shuts down or a reload supersedes the
    /// resolution, since that reload loads the dataset as it now is.
    ///
    /// [`SnapshotSourceRegistry::begin_resolution`]: crate::component::dataset::snapshot_source::SnapshotSourceRegistry::begin_resolution
    pub(super) async fn resolve_snapshot_source(
        self: &Arc<Self>,
        pending: &Arc<Dataset>,
        load_semaphore: &Arc<Semaphore>,
    ) -> Option<(Arc<Dataset>, BootstrapStatus)> {
        let name = pending.name.clone();
        let resolution = self.snapshot_sources().begin_resolution(&name);
        let stopped = || self.status.is_shutdown() || resolution.is_superseded();

        let resolved = retry(Self::snapshot_source_backoff(pending), || async {
            if stopped() {
                return Err(RetryError::permanent(()));
            }
            self.resolve_snapshot_source_once(pending, &stopped).await
        })
        .await
        .ok()?;

        if let Err(message) = self.check_snapshot_source_conflicts(&resolved).await {
            self.refuse_snapshot_source(&name, &message);
            return None;
        }

        self.status
            .update_dataset(&name, status::ComponentStatus::Initializing);
        // The engines write the local copy under the data directory, which a bootstrap
        // that fails leaves them to create.
        if let Err(err) =
            tokio::fs::create_dir_all(data_accelerator_api::spice_data_base_path()).await
        {
            tracing::debug!(dataset = %name, "Failed to create the Spice data directory: {err}");
        }

        // Restore the current snapshot before the table is created. Bounded by the
        // shared load budget like every other accelerator initialization, and released
        // before the load, which takes its own permit.
        let bootstrap_status = {
            let Ok(_permit) = load_semaphore.acquire().await else {
                return None;
            };
            self.initialize_datasets_accelerators(std::slice::from_ref(&resolved))
                .await
                .remove(&resolved.name)?
                // `initialize_datasets_accelerators` reports its own failures.
                .ok()?
        };
        // A reload that superseded this resolution while the snapshot was restored loads
        // the dataset as it now is; this load must not also register it.
        if stopped() {
            return None;
        }
        // Refused here, before registration: once a snapshot is restored, a failure to
        // describe the dataset's source would serve the restored copy while retrying.
        if let Some(refresh_sql) = publisher_column_projection(&resolved).await {
            self.refuse_snapshot_source(&name, &projected_publisher_message(&name, &refresh_sql));
            return None;
        }

        Some((resolved, bootstrap_status))
    }

    /// One attempt of [`Self::resolve_snapshot_source`]. A permanent error means stop,
    /// with any failure already reported; a transient one, retry.
    async fn resolve_snapshot_source_once(
        self: &Arc<Self>,
        pending: &Arc<Dataset>,
        stopped: &(dyn Fn() -> bool + Sync),
    ) -> Result<Arc<Dataset>, RetryError<()>> {
        let name = &pending.name;
        let app = pending.app();
        let Some(spec) = app
            .datasets
            .iter()
            .find(|spec| Dataset::parse_table_reference(&spec.name).is_ok_and(|n| &n == name))
        else {
            return Err(RetryError::permanent(()));
        };
        let dataset = self.build_snapshot_dataset(&app, spec)?;
        if !dataset.is_pending_snapshot_source() {
            return Ok(Arc::new(dataset));
        }

        // `build_snapshot_dataset` accepted the same Spicepod entry.
        let Ok(Some(source)) = SnapshotSource::from_spicepod(spec) else {
            return Err(RetryError::permanent(()));
        };
        let behavior = SnapshotBehavior::bootstrap_only(
            Arc::new(source.snapshots(&dataset.params)),
            self.secrets_weak(),
            self.tokio_io_runtime(),
        );
        let Some(manager) =
            SnapshotManager::try_new_for_metadata_queries(name.to_string(), behavior).await
        else {
            let cause = format!(
                "the snapshot location '{}' could not be opened. Check the dataset's `s3_*` params",
                source.location()
            );
            self.refuse_snapshot_source(name, &cannot_load_snapshot_message(name, &cause));
            return Err(RetryError::permanent(()));
        };

        let current = manager.current_snapshot().await;
        // The read can take as long as the store takes to answer; a reload that
        // superseded this resolution meanwhile must not record what it read.
        if stopped() {
            return Err(RetryError::permanent(()));
        }
        match current {
            Ok(current) => {
                let Some(engine) = self.snapshot_engine(&current.engine).await else {
                    let cause = unavailable_engine_cause(source.location(), &current.engine);
                    self.refuse_snapshot_source(name, &cannot_load_snapshot_message(name, &cause));
                    return Err(RetryError::permanent(()));
                };
                self.snapshot_sources()
                    .record(name, source.location(), engine);

                let resolved = self.build_snapshot_dataset(&app, spec)?;
                if resolved.is_pending_snapshot_source() {
                    // Unreachable: the engine was just recorded for this location.
                    let cause = "its snapshot engine could not be recorded. Report a bug: https://github.com/spiceai/spiceai/issues";
                    self.refuse_snapshot_source(name, &cannot_load_snapshot_message(name, cause));
                    return Err(RetryError::permanent(()));
                }
                tracing::info!("{}", resolved_message(name, source.location(), engine));
                Ok(Arc::new(resolved))
            }
            Err(err) if err.is_retriable() => {
                let message = waiting_for_snapshot_message(name, &err);
                self.status.update_dataset(
                    name,
                    status::ComponentStatus::error_with_message(message.clone()),
                );
                warn_spaced!(self.spaced_tracer, "{}", message.as_str());
                Err(RetryError::transient(()))
            }
            Err(err) => {
                self.refuse_snapshot_source(
                    name,
                    &cannot_load_snapshot_message(name, &err.to_string()),
                );
                Err(RetryError::permanent(()))
            }
        }
    }

    /// The runtime dataset `spec` builds into, as `get_valid_datasets` builds it. A
    /// configuration error is reported and ends the resolution.
    fn build_snapshot_dataset(
        self: &Arc<Self>,
        app: &Arc<App>,
        spec: &spicepod::component::dataset::Dataset,
    ) -> Result<Dataset, RetryError<()>> {
        let built = match DatasetBuilder::try_from(spec.clone()) {
            Ok(builder) => builder
                .with_app(Arc::clone(app))
                .with_runtime(Arc::clone(self))
                .build()
                .context(UnableToBuildDatasetSnafu {
                    dataset: spec.name.clone(),
                }),
            Err(err) => Err(err),
        };
        built.map_err(|err| {
            if let Ok(name) = Dataset::parse_table_reference(&spec.name) {
                self.refuse_snapshot_source(&name, &err.to_string());
            }
            RetryError::permanent(())
        })
    }

    /// `engine`, as a snapshot records it, if this build can load snapshots into it.
    async fn snapshot_engine(&self, engine: &str) -> Option<Engine> {
        let engine = Engine::try_from(engine).ok()?;
        engine_to_acceleration_engine(engine)?;
        self.accelerator_engine_registry()
            .get_accelerator_engine(engine)
            .await?;
        Some(engine)
    }

    /// Refuses a resolved snapshot dataset that would share accelerator state with
    /// another dataset in a way startup refuses. Startup validates every accelerated
    /// dataset it initializes, and a snapshot dataset is initialized only here, once its
    /// engine is known.
    async fn check_snapshot_source_conflicts(
        self: &Arc<Self>,
        dataset: &Arc<Dataset>,
    ) -> Result<(), String> {
        let Some(acceleration) = &dataset.acceleration else {
            return Ok(());
        };
        let datasets = Arc::clone(self).get_valid_datasets(&dataset.app(), LogErrors(false));

        // Cayenne keeps every dataset of one metadata directory in one catalog, which
        // must not mix datasets that restore snapshots with datasets that do not.
        let sources: Vec<Arc<dyn AccelerationSource>> =
            datasets.iter().map(|other| other.clone_arc()).collect();
        match validate_snapshot_consistency(&sources) {
            Ok(()) => {}
            Err(CayenneSnapshotValidationError::InconsistentSnapshotSettings {
                metadata_dir,
                disabled_components,
                ..
            }) => {
                return Err(cannot_load_snapshot_message(
                    &dataset.name,
                    &format!(
                        "it shares the Cayenne catalog in '{metadata_dir}' with Cayenne datasets that do not restore snapshots ({disabled_components}), and one catalog cannot hold both. Accelerate those datasets with another engine, or serve this dataset from another Spice instance"
                    ),
                ));
            }
            Err(err) => {
                return Err(cannot_load_snapshot_message(
                    &dataset.name,
                    &err.to_string(),
                ));
            }
        }

        // Restoring a `DuckDB`, `SQLite` or Turso snapshot replaces the whole file, so a
        // file two datasets share would serve whichever restored last to both.
        let param = match acceleration.engine {
            Engine::DuckDB => "duckdb_file",
            Engine::Sqlite => "sqlite_file",
            Engine::Turso => "turso_file",
            _ => return Ok(()),
        };
        let registry = self.accelerator_engine_registry();
        let Ok(path) = acceleration_file_path(dataset.as_ref(), &registry).await else {
            return Ok(());
        };
        let others = datasets.iter().filter(|other| {
            other.name != dataset.name
                && other.is_file_accelerated()
                && other
                    .acceleration
                    .as_ref()
                    .is_some_and(|other| other.engine == acceleration.engine)
        });
        for other in others {
            if acceleration_file_path(other.as_ref(), &registry)
                .await
                .is_ok_and(|other_path| other_path == path)
            {
                return Err(cannot_load_snapshot_message(
                    &dataset.name,
                    &format!(
                        "its local copy '{}' is also the file of dataset '{}', and restoring a snapshot replaces the whole file. Set a different `{param}` for '{}'",
                        path.display(),
                        other.name,
                        other.name,
                    ),
                ));
            }
        }
        Ok(())
    }

    /// The backoff between attempts to describe `dataset`'s snapshots, capped at the
    /// interval its refreshes check for a newer snapshot.
    fn snapshot_source_backoff(dataset: &Dataset) -> util::fibonacci_backoff::FibonacciBackoff {
        let interval = dataset
            .app
            .datasets
            .iter()
            .find(|spec| {
                Dataset::parse_table_reference(&spec.name).is_ok_and(|n| n == dataset.name)
            })
            .and_then(|spec| SnapshotSource::from_spicepod(spec).ok().flatten())
            .and_then(|source| source.refresh_check_interval())
            .unwrap_or(DEFAULT_SNAPSHOT_REFRESH_CHECK_INTERVAL);
        FibonacciBackoffBuilder::new()
            .max_retries(None)
            .max_duration(Some(interval))
            .build()
    }

    /// Reports a snapshot dataset that no retry can load.
    fn refuse_snapshot_source(&self, dataset: &TableReference, message: &str) {
        self.status.update_dataset(
            dataset,
            status::ComponentStatus::error_with_message(message.to_string()),
        );
        metrics::datasets::LOAD_ERROR.add(1, &[]);
        tracing::error!("{message}");
    }
}
