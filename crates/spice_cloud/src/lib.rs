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

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use datafusion::{datasource::TableProvider, sql::TableReference};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::json;
use snafu::prelude::*;

use runtime::{
    accelerated_table::{
        refresh::Refresh, AcceleratedTable, AcceleratedTableBuilderError, Retention,
    },
    component::dataset::{
        acceleration::{Acceleration, RefreshMode},
        replication::Replication,
        Dataset, Mode, TimeFormat,
    },
    dataaccelerator::{self, create_accelerator_table},
    dataconnector::{create_new_connector, ConnectorParamsBuilder, DataConnectorError},
    extension::{Error as ExtensionError, Extension, ExtensionFactory, ExtensionManifest, Result},
    federated_table::FederatedTable,
    secrets::{ExposeSecret, Secrets},
    spice_metrics::get_metrics_table_reference,
    status, Runtime,
};
use tokio::sync::RwLock;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to get read-write table provider"))]
    NoReadWriteProvider {},

    #[snafu(display("Unable to create data connector"))]
    UnableToCreateDataConnector {
        source: Box<dyn std::error::Error + Sync + Send>,
    },

    #[snafu(display("Unable to create source table provider"))]
    UnableToCreateSourceTableProvider { source: DataConnectorError },

    #[snafu(display("Unable to create accelerated table provider: {source}"))]
    UnableToCreateAcceleratedTableProvider { source: dataaccelerator::Error },

    #[snafu(display("Unable to get Spice Cloud secret: {source}"))]
    UnableToGetSpiceSecret {
        source: Box<dyn std::error::Error + Sync + Send>,
    },

    #[snafu(display("Spice Cloud secret not found"))]
    SpiceSecretNotFound {},

    #[snafu(display("Spice Cloud api_key not provided"))]
    SpiceApiKeyNotFound {},

    #[snafu(display("Unable to connect to Spice Cloud: {source}"))]
    UnableToConnectToSpiceCloud { source: reqwest::Error },

    #[snafu(display("Unable to build accelerated table: {source}"))]
    UnableToBuildAcceleratedTable {
        source: AcceleratedTableBuilderError,
    },
}

pub struct SpiceExtension {
    manifest: ExtensionManifest,
    api_key: String,
}

impl SpiceExtension {
    #[must_use]
    pub fn new(manifest: ExtensionManifest) -> Self {
        SpiceExtension {
            manifest,
            api_key: String::new(),
        }
    }

    fn metrics_enabled(&self) -> bool {
        if !self.manifest.enabled {
            return false;
        }

        self.manifest
            .params
            .get("metrics")
            .map_or_else(|| false, |v| v.eq_ignore_ascii_case("true"))
    }

    fn spice_http_url(&self) -> String {
        self.manifest
            .params
            .get("endpoint")
            .unwrap_or(&"https://data.spiceai.io".to_string())
            .to_string()
    }

    async fn get_spice_api_key(&self, runtime: &Runtime) -> Result<String, Error> {
        let secret = runtime.secrets();
        let secret = secret.read().await;
        let api_key = secret
            .get_secret("spiceai_api_key")
            .await
            .context(UnableToGetSpiceSecretSnafu)?
            .ok_or(Error::SpiceApiKeyNotFound {})?;

        Ok(api_key.expose_secret().to_string())
    }

    async fn connect(&self) -> Result<SpiceCloudConnectResponse, Error> {
        self.post_json("/v1/connect", json!({})).await
    }

    async fn post_json<Req: Serialize, Resp: DeserializeOwned>(
        &self,
        path: &str,
        body: Req,
    ) -> Result<Resp, Error> {
        let client = reqwest::Client::new();
        let response = client
            .post(format!("{}{path}", self.spice_http_url()))
            .json(&body)
            .header("Content-Type", "application/json")
            .header("X-API-Key", &self.api_key)
            .send()
            .await
            .context(UnableToConnectToSpiceCloudSnafu)?;

        let response: Resp = response
            .json()
            .await
            .context(UnableToConnectToSpiceCloudSnafu)?;

        Ok(response)
    }

    async fn register_runtime_metrics_table(&self, runtime: &Runtime, from: String) -> Result<()> {
        let retention = Retention::new(
            Some("timestamp".to_string()),
            Some(TimeFormat::UnixSeconds),
            None,
            None,
            Some(Duration::from_secs(1800)), // delete metrics older then 30 minutes
            Some(Duration::from_secs(300)),  // run retention every 5 minutes
            true,
        );

        let refresh = Refresh::new(RefreshMode::Full)
            .time_column("timestamp".to_string())
            .time_format(TimeFormat::UnixSeconds)
            .check_interval(Duration::from_secs(10))
            .period(Duration::from_secs(1800)); // sync only last 30 minutes from cloud

        let metrics_table_reference = get_metrics_table_reference();

        let table = create_synced_internal_accelerated_table(
            runtime.status(),
            metrics_table_reference.clone(),
            from.as_str(),
            Acceleration::default(),
            refresh,
            retention,
            runtime.secrets(),
        )
        .await
        .boxed()
        .map_err(|e| runtime::extension::Error::UnableToStartExtension { source: e })?;

        runtime
            .datafusion()
            .register_table_as_writable_and_with_schema(metrics_table_reference, table)
            .boxed()
            .map_err(|e| runtime::extension::Error::UnableToStartExtension { source: e })?;

        Ok(())
    }

    async fn start_metrics(&self, runtime: &Runtime) -> Result<()> {
        if !self.metrics_enabled() {
            return Ok(());
        }

        let connection = self
            .connect()
            .await
            .boxed()
            .map_err(|e| runtime::extension::Error::UnableToStartExtension { source: e })?;

        let spiceai_metrics_dataset_path = format!(
            "spice.ai/{}/{}/{}",
            connection.org_name, connection.app_name, connection.metrics_dataset_name
        );

        let from = spiceai_metrics_dataset_path.to_string();
        self.register_runtime_metrics_table(runtime, from.clone())
            .await?;
        tracing::info!("Enabled metrics sync from runtime.metrics to {from}");

        Ok(())
    }
}

impl Default for SpiceExtension {
    fn default() -> Self {
        SpiceExtension::new(ExtensionManifest::default())
    }
}

#[async_trait]
impl Extension for SpiceExtension {
    fn name(&self) -> &'static str {
        "spice_cloud"
    }

    async fn initialize(&mut self, runtime: &Runtime) -> Result<()> {
        if !self.manifest.enabled {
            return Ok(());
        }

        let api_key = self
            .get_spice_api_key(runtime)
            .await
            .boxed()
            .map_err(|source| ExtensionError::UnableToInitializeExtension { source })?;
        self.api_key = api_key;

        Ok(())
    }

    async fn on_start(&self, runtime: &Runtime) -> Result<()> {
        self.start_metrics(runtime).await?;

        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct SpiceExtensionFactory {
    manifest: ExtensionManifest,
}

impl SpiceExtensionFactory {
    #[must_use]
    pub fn new(manifest: ExtensionManifest) -> Self {
        SpiceExtensionFactory { manifest }
    }
}

impl ExtensionFactory for SpiceExtensionFactory {
    fn create(&self) -> Box<dyn Extension> {
        Box::new(SpiceExtension {
            manifest: self.manifest.clone(),
            api_key: String::new(),
        })
    }
}

async fn get_spiceai_table_provider(
    name: &str,
    cloud_dataset_path: &str,
    secrets: Arc<RwLock<Secrets>>,
) -> Result<Arc<dyn TableProvider>, Error> {
    let mut dataset = Dataset::try_new(cloud_dataset_path.to_string(), name)
        .boxed()
        .context(UnableToCreateDataConnectorSnafu)?;

    dataset.mode = Mode::ReadWrite;
    dataset.replication = Some(Replication { enabled: true });

    let params = ConnectorParamsBuilder::new(name.into(), (&dataset).into())
        .build(secrets)
        .await
        .context(UnableToCreateDataConnectorSnafu)?;

    let data_connector = create_new_connector("spice.ai", params)
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
    runtime_status: Arc<status::RuntimeStatus>,
    table_reference: TableReference,
    from: &str,
    acceleration: Acceleration,
    refresh: Refresh,
    retention: Option<Retention>,
    secrets: Arc<RwLock<Secrets>>,
) -> Result<Arc<AcceleratedTable>, Error> {
    let source_table_provider =
        get_spiceai_table_provider(table_reference.table(), from, Arc::clone(&secrets)).await?;
    let federated_table = Arc::new(FederatedTable::new(source_table_provider));

    let accelerated_table_provider = create_accelerator_table(
        table_reference.clone(),
        federated_table.schema(),
        None,
        &acceleration,
        secrets,
        None,
    )
    .await
    .context(UnableToCreateAcceleratedTableProviderSnafu)?;

    let mut builder = AcceleratedTable::builder(
        runtime_status,
        table_reference.clone(),
        federated_table,
        "spice.ai".to_string(),
        accelerated_table_provider,
        refresh,
    );

    builder.retention(retention);

    let (accelerated_table, _) = builder
        .build()
        .await
        .context(UnableToBuildAcceleratedTableSnafu)?;

    Ok(Arc::new(accelerated_table))
}

#[derive(Deserialize, Debug)]
#[allow(clippy::struct_field_names)]
struct SpiceCloudConnectResponse {
    org_name: String,
    app_name: String,
    metrics_dataset_name: String,
}
