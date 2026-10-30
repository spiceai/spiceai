/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use crate::dataconnector::parameters::RuntimeConnectorContext;
use std::sync::Arc;

use arrow::datatypes::Schema;
use datafusion::common::TableReference;
use datafusion::datasource::TableProvider;
use snafu::prelude::*;
use tokio::sync::RwLock;

use crate::Runtime;
use crate::accelerated::{AcceleratedTableBuilderError, Retention};
use crate::component::access::AccessMode;
use crate::component::dataset::acceleration::{Acceleration, Engine, Mode};
use crate::component::dataset::builder::DatasetBuilder;
use crate::federated::FederatedTable;
use crate::secrets::Secrets;
use crate::{
    accelerated::{AcceleratedTable, refresh::Refresh},
    dataaccelerator::{self},
    dataconnector::{DataConnector, DataConnectorError, sink::SinkConnector},
};
use runtime_acceleration::{AccelerationSource, OnSchemaChange};
use runtime_status as status;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Internal table does not support read-write mode"))]
    NoReadWriteProvider {},

    #[snafu(display("Failed to create internal data connector: {source}"))]
    UnableToCreateDataConnector {
        source: Box<dyn std::error::Error + Sync + Send>,
    },

    #[snafu(display("Failed to create source for internal table: {source}"))]
    UnableToCreateSourceTableProvider { source: DataConnectorError },

    #[snafu(display("Failed to create accelerated storage for internal table: {source}"))]
    UnableToCreateAcceleratedTableProvider { source: dataaccelerator::Error },

    #[snafu(display(
        "An internal error occurred. Report a bug on GitHub (github.com/spiceai/spiceai) and reference the code: {code}"
    ))]
    Internal {
        code: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to build accelerated internal table: {source}"))]
    UnableToBuildAcceleratedTable {
        source: AcceleratedTableBuilderError,
    },
}

/// The acceleration of an internal table (`runtime.task_history`,
/// `runtime.metrics`): Cayenne in memory, so nothing it holds is written to disk.
#[must_use]
pub fn internal_acceleration() -> Acceleration {
    Acceleration {
        engine: Engine::Cayenne,
        mode: Mode::Memory,
        ..Acceleration::default()
    }
}

/// The [`AccelerationSource`] of a table the runtime creates for itself rather
/// than one a Spicepod declares. The accelerator reads it for configuration
/// only: an internal table's rows arrive by writes to its acceleration, or by
/// its own refresh, never through a connector the source names.
#[derive(Clone)]
pub struct InternalAccelerationSource {
    app: Arc<app::App>,
    name: TableReference,
    acceleration: Acceleration,
    secrets: Arc<RwLock<Secrets>>,
}

impl InternalAccelerationSource {
    /// The source of the internal table `name`, accelerated as `acceleration`
    /// asks, which must be in memory.
    ///
    /// # Errors
    ///
    /// When `acceleration` is file-backed, or the runtime has no app loaded.
    pub async fn new(
        runtime: &Runtime,
        name: TableReference,
        acceleration: Acceleration,
        secrets: Arc<RwLock<Secrets>>,
    ) -> Result<Self, Error> {
        if acceleration.mode != Mode::Memory {
            return Err(Error::Internal {
                code: "IT-IAS-MODE".to_string(), // InternalTable - InternalAccelerationSource - Mode
                source: format!("internal table {name} must be accelerated in memory").into(),
            });
        }
        let app = runtime.app().read().await.as_ref().map(Arc::clone).ok_or_else(|| {
            Error::Internal {
                code: "IT-IAS-GA".to_string(), // InternalTable - InternalAccelerationSource - GetApp
                source: "No App instance found in Runtime".into(),
            }
        })?;
        Ok(Self {
            app,
            name,
            acceleration,
            secrets,
        })
    }
}

impl AccelerationSource for InternalAccelerationSource {
    fn clone_arc(&self) -> Arc<dyn AccelerationSource> {
        Arc::new(self.clone())
    }

    fn is_file_accelerated(&self) -> bool {
        // `new` admits only memory mode.
        false
    }

    fn app(&self) -> Arc<app::App> {
        Arc::clone(&self.app)
    }

    fn secrets(&self) -> Arc<RwLock<Secrets>> {
        Arc::clone(&self.secrets)
    }

    fn snapshot_notifications(
        &self,
    ) -> Option<Arc<runtime_acceleration::snapshot::notifications::SnapshotNotifications>> {
        // An internal table is never loaded from a snapshot.
        None
    }

    fn acceleration(&self) -> Option<&Acceleration> {
        Some(&self.acceleration)
    }

    fn name(&self) -> &TableReference {
        &self.name
    }

    fn connector_name(&self) -> Option<&str> {
        // No `from:` names a connector: the runtime writes the rows itself.
        None
    }

    fn on_schema_change(&self) -> Option<OnSchemaChange> {
        // The runtime defines the schema, and it never changes under a running table.
        None
    }

    fn allows_write(&self) -> bool {
        // The runtime writes these tables directly, so a scan must read its own writes.
        true
    }

    fn time_column(&self) -> Option<&str> {
        // Retention is the accelerated table's (`Retention`), not the accelerator's.
        None
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    /// An internal table has no snapshot to reconcile a checkpoint against.
    /// Reports that rather than handing back a no-op checkpointer, which would
    /// read as "checkpoint present and empty" to the snapshot bootstrap.
    fn checkpointer_factory(
        &self,
        _snapshot_behavior: runtime_acceleration::snapshot::SnapshotBehavior,
    ) -> runtime_acceleration::dataset_checkpoint::DatasetCheckpointerFactory {
        runtime_acceleration::dataset_checkpoint::make_checkpointer_factory(|| async {
            Err("an internal table has no accelerator checkpoint".into())
        })
    }
}

async fn get_local_table_provider(
    name: &TableReference,
    schema: &Arc<Schema>,
    primary_key: Option<Vec<String>>,
    runtime: Arc<Runtime>,
) -> Result<Arc<dyn TableProvider>, Error> {
    // This shouldn't error because we control the name passed in, and it shouldn't contain a catalog.
    let dataset_builder = DatasetBuilder::try_new("sink".to_string(), &name.to_string())
        .boxed()
        .context(InternalSnafu {
            code: "IT-GLTP-BD".to_string(), // InternalTable - GetLocalTableProvider - DatasetBuilder
        })?;

    let app_ref = runtime.app();
    let app_lock = app_ref.read().await;
    let Some(app) = app_lock.as_ref() else {
        return Err(Error::Internal {
            code: "IT-GLTP-GA".to_string(), // InternalTable - GetLocalTableProvider - GetApp
            source: "No App instance found in Runtime".into(),
        });
    };

    let mut dataset = dataset_builder
        .with_app(Arc::clone(app))
        .with_runtime(Arc::clone(&runtime))
        .build()
        .boxed()
        .context(InternalSnafu {
            code: "IT-GLTP-BD-B".to_string(), // InternalTable - GetLocalTableProvider - DatasetBuilder - Build
        })?;

    dataset.access = AccessMode::ReadWrite;

    let mut sink = SinkConnector::new(Arc::clone(schema));
    if let Some(pk) = primary_key {
        sink = sink.with_primary_key(&pk);
    }

    let data_connector = Arc::new(sink) as Arc<dyn DataConnector>;

    let source_table_provider = data_connector
        .read_write_provider(&RuntimeConnectorContext::for_dataset(&dataset), &dataset)
        .await
        .ok_or_else(|| NoReadWriteProviderSnafu {}.build())?
        .context(UnableToCreateSourceTableProviderSnafu)?;

    Ok(source_table_provider)
}

#[expect(clippy::too_many_arguments)]
pub async fn create_internal_accelerated_table(
    runtime_status: Arc<status::RuntimeStatus>,
    name: TableReference,
    schema: Arc<Schema>,
    primary_key: Option<Vec<String>>,
    acceleration: Acceleration,
    refresh: Refresh,
    retention: Option<Retention>,
    secrets: Arc<RwLock<Secrets>>,
    runtime: Arc<Runtime>,
) -> Result<Arc<AcceleratedTable>, Error> {
    let source_table_provider =
        get_local_table_provider(&name, &schema, primary_key.clone(), Arc::clone(&runtime)).await?;
    let source = InternalAccelerationSource::new(
        &runtime,
        name.clone(),
        acceleration.clone(),
        Arc::clone(&secrets),
    )
    .await?;
    let federated_table = Arc::new(FederatedTable::new_unchecked(Arc::clone(
        &source_table_provider,
    )));
    let accelerated_table_provider = runtime
        .accelerator_engine_registry()
        .create_accelerator_table(
            name.clone(),
            Arc::clone(&schema),
            Arc::clone(&source_table_provider).constraints(),
            &acceleration,
            secrets,
            Some(&source),
            Arc::clone(&runtime.df.ctx),
        )
        .await
        .context(UnableToCreateAcceleratedTableProviderSnafu)?;

    let mut builder = AcceleratedTable::builder(
        runtime_status,
        name.clone(),
        federated_table,
        "internal".to_string(),
        accelerated_table_provider,
        refresh,
        runtime.tokio_io_runtime(),
    );
    builder.cpu_runtime(runtime.datafusion().refresh_runtime().cloned());
    builder.write_to_accelerator_only();
    builder.node_local();

    builder.retention(retention);

    let accelerated_table = builder
        .build()
        .await
        .context(UnableToBuildAcceleratedTableSnafu)?;

    Ok(Arc::new(accelerated_table))
}
