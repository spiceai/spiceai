use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use datafusion::{datasource::TableProvider, sql::TableReference};
use snafu::prelude::*;

use runtime::{
    accelerated_table::{refresh::Refresh, AcceleratedTable, Retention},
    component::dataset::{
        acceleration::{Acceleration, RefreshMode},
        replication::Replication,
        Dataset, Mode, TimeFormat,
    },
    dataaccelerator::{self, create_accelerator_table},
    dataconnector::{create_new_connector, DataConnectorError},
    extensions::{Extension, ExtensionFactory, Result},
    spice_metrics::get_metrics_table_reference,
    Runtime,
};
use secrets::Secret;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create data connector"))]
    NoReadWriteProvider {},

    #[snafu(display("Unable to create data connector"))]
    UnableToCreateDataConnector {
        source: Box<dyn std::error::Error + Sync + Send>,
    },

    #[snafu(display("Unable to create source table provider"))]
    UnableToCreateSourceTableProvider { source: DataConnectorError },

    #[snafu(display("Unable to create accelerated table provider: {source}"))]
    UnableToCreateAcceleratedTableProvider { source: dataaccelerator::Error },
}

pub struct SpiceExtension;

impl SpiceExtension {
    #[must_use]
    pub fn new() -> Self {
        SpiceExtension {}
    }
}

impl Default for SpiceExtension {
    fn default() -> Self {
        SpiceExtension::new()
    }
}

#[async_trait]
impl Extension for SpiceExtension {
    fn name(&self) -> &'static str {
        "spiceai"
    }

    async fn initialize(&mut self, _runtime: &mut Runtime) -> Result<()> {
        tracing::info!("Initializing Spice.ai Extension");

        Ok(())
    }

    async fn on_start(&mut self, runtime: &Runtime) -> Result<()> {
        tracing::info!("Starting Spiceai Extension");

        let secrets = runtime.secrets_provider.read().await;
        let secret = secrets
            .get_secret("spiceai")
            .await
            .map_err(|e| runtime::extensions::Error::UnableToStartExtension { source: e })?;

        let retention = Retention::new(
            Some("timestamp".to_string()),
            Some(TimeFormat::UnixSeconds),
            Some(Duration::from_secs(1800)), // delete metrics older then 30 minutes
            Some(Duration::from_secs(300)),  // run retention every 5 minutes
            true,
        );

        let refresh = Refresh::new(
            Some("timestamp".to_string()),
            Some(TimeFormat::UnixSeconds),
            Some(Duration::from_secs(10)),
            None,
            RefreshMode::Full,
            Some(Duration::from_secs(1800)), // sync only last 30 minutes from cloud
        );

        let metrics_table_reference = get_metrics_table_reference();

        let table = create_synced_internal_accelerated_table(
            metrics_table_reference.table(),
            "spice.ai/ewgenius/demo/runtime_metrics_test",
            secret,
            Acceleration::default(),
            refresh,
            retention,
        )
        .await
        .boxed()
        .map_err(|e| runtime::extensions::Error::UnableToStartExtension { source: e })?;

        runtime
            .datafusion()
            .write()
            .await
            .register_runtime_table(metrics_table_reference.table(), table)
            .boxed()
            .map_err(|e| runtime::extensions::Error::UnableToStartExtension { source: e })?;

        tracing::info!("Enabled sync to Spice.ai for runtime.metrics");

        Ok(())
    }
}

pub struct SpiceExtensionFactory;

impl ExtensionFactory for SpiceExtensionFactory {
    fn create(&self) -> Box<dyn Extension> {
        Box::new(SpiceExtension)
    }
}

async fn get_spiceai_table_provider(
    name: &str,
    cloud_dataset_path: &str,
    secret: Option<Secret>,
) -> Result<Arc<dyn TableProvider>, Error> {
    let mut dataset = Dataset::try_new(cloud_dataset_path.to_string(), name)
        .boxed()
        .context(UnableToCreateDataConnectorSnafu)?;

    dataset.mode = Mode::ReadWrite;
    dataset.replication = Some(Replication { enabled: true });

    let data_connector = create_new_connector("spiceai", secret, Arc::new(HashMap::new()))
        .await
        .ok_or_else(|| NoReadWriteProviderSnafu {}.build())?
        .context(UnableToCreateDataConnectorSnafu)?;

    let source_table_provider = data_connector
        .read_write_provider(&dataset)
        .await
        .ok_or_else(|| NoReadWriteProviderSnafu {}.build())?
        .context(UnableToCreateSourceTableProviderSnafu)?;

    Ok(source_table_provider)
}

/// Create a new accelerated table that is synced with the cloud dataset
///
/// # Errors
///
/// This function will return an error if the accelerated table provider cannot be created
pub async fn create_synced_internal_accelerated_table(
    name: &str,
    from: &str,
    secret: Option<Secret>,
    acceleration: Acceleration,
    refresh: Refresh,
    retention: Option<Retention>,
) -> Result<Arc<AcceleratedTable>, Error> {
    let source_table_provider = get_spiceai_table_provider(name, from, secret).await?;

    let accelerated_table_provider = create_accelerator_table(
        TableReference::bare(name),
        source_table_provider.schema(),
        &acceleration,
        None,
    )
    .await
    .context(UnableToCreateAcceleratedTableProviderSnafu)?;

    let mut builder = AcceleratedTable::builder(
        TableReference::bare(name),
        source_table_provider,
        accelerated_table_provider,
        refresh,
    );

    builder.retention(retention);

    let (accelerated_table, _) = builder.build().await;

    Ok(Arc::new(accelerated_table))
}
