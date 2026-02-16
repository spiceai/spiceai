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

use std::sync::Arc;

use arrow::datatypes::Schema;
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use snafu::prelude::*;
use tokio::sync::RwLock;

use crate::Runtime;
use crate::accelerated_table::{AcceleratedTableBuilderError, Retention};
use crate::component::access::AccessMode;
use crate::component::dataset::acceleration::Acceleration;
use crate::component::dataset::builder::DatasetBuilder;
use crate::federated_table::FederatedTable;
use crate::secrets::Secrets;
use crate::status;
use crate::{
    accelerated_table::{AcceleratedTable, refresh::Refresh},
    dataaccelerator::{self},
    dataconnector::{DataConnector, DataConnectorError, sink::SinkConnector},
};

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
        .read_write_provider(&dataset)
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
            None,
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

    builder.retention(retention);

    let accelerated_table = builder
        .build()
        .await
        .context(UnableToBuildAcceleratedTableSnafu)?;

    Ok(Arc::new(accelerated_table))
}
