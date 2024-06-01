use std::{collections::HashMap, sync::Arc, time::Duration};

use arrow::array::{ArrayRef, Float64Array, StringArray, StructArray, TimestampNanosecondArray};
use async_trait::async_trait;
use datafusion::{arrow::array::RecordBatch, datasource::TableProvider, sql::TableReference};
use flight_client::FlightClient;
use futures::TryStreamExt;
use serde::Deserialize;
use serde_json::json;
use snafu::prelude::*;

use runtime::{
    accelerated_table::{refresh::Refresh, AcceleratedTable, Retention},
    component::dataset::{
        acceleration::{Acceleration, RefreshMode},
        replication::Replication,
        Dataset, Mode, TimeFormat,
    },
    dataaccelerator,
    dataconnector::{create_new_connector, DataConnectorError},
    datafusion::{query::QueryBuilder, DataFusion},
    extension::{Extension, ExtensionFactory, ExtensionManifest, Result},
    internal_table::{create_internal_accelerated_table, Error as InternalTableError},
    spice_metrics::{get_metrics_schema, get_metrics_table_reference, get_tags_fields},
    Runtime,
};
use secrets::Secret;

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

    #[snafu(display("Error creating metrics table: {source}"))]
    UnableToCreateMetricsTable { source: InternalTableError },

    #[snafu(display("Unable to create flight client: {source}"))]
    UnableToCreateFlightClient { source: flight_client::Error },
}

pub struct SpiceExtension {
    manifest: ExtensionManifest,
}

impl SpiceExtension {
    #[must_use]
    pub fn new(manifest: ExtensionManifest) -> Self {
        SpiceExtension { manifest }
    }

    fn spice_http_url(&self) -> String {
        self.manifest
            .params
            .get("endpoint")
            .unwrap_or(&"https://data.spiceai.io".to_string())
            .to_string()
    }

    fn spice_flight_url(&self) -> String {
        self.manifest
            .params
            .get("flight_endpoint")
            .unwrap_or(&"https://flight.spiceai.io".to_string())
            .to_string()
    }

    async fn get_spice_secret(&self, runtime: &Runtime) -> Result<Secret, Error> {
        let secrets = runtime.secrets_provider.read().await;
        let secret = secrets
            .get_secret("spiceai")
            .await
            .context(UnableToGetSpiceSecretSnafu)?;

        secret.ok_or(Error::SpiceSecretNotFound {})
    }

    async fn get_spice_api_key(&self, runtime: &Runtime) -> Result<String, Error> {
        let secret = self.get_spice_secret(runtime).await?;
        let api_key = secret.get("key").ok_or(Error::SpiceApiKeyNotFound {})?;

        Ok(api_key.to_string())
    }

    async fn connect(&self, runtime: &Runtime) -> Result<SpiceCloudConnectResponse, Error> {
        let api_key = self.get_spice_api_key(runtime).await?;
        let client = reqwest::Client::new();
        let response = client
            .post(format!("{}/v1/connect", self.spice_http_url()))
            .json(&json!({}))
            .header("Content-Type", "application/json")
            .header("X-API-Key", api_key)
            .send()
            .await
            .context(UnableToConnectToSpiceCloudSnafu)?;

        let response: SpiceCloudConnectResponse = response
            .json()
            .await
            .context(UnableToConnectToSpiceCloudSnafu)?;

        Ok(response)
    }

    async fn register_runtime_metrics_table(
        &mut self,
        runtime: &Runtime,
        from: String,
        secret: Secret,
    ) -> Result<()> {
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
            metrics_table_reference.clone(),
            from.as_str(),
            Some(secret),
            Acceleration::default(),
            refresh,
            retention,
        )
        .await
        .boxed()
        .map_err(|e| runtime::extension::Error::UnableToStartExtension { source: e })?;

        runtime
            .datafusion()
            .register_runtime_table(metrics_table_reference, table)
            .boxed()
            .map_err(|e| runtime::extension::Error::UnableToStartExtension { source: e })?;

        Ok(())
    }

    async fn start_sync(
        flight_url: String,
        api_key: String,
        dataset_path: String,
        datafusion: Arc<DataFusion>,
    ) {
        let mut flight_client =
            match FlightClient::new(flight_url.as_str(), "", api_key.as_str()).await {
                Ok(client) => client,
                Err(err) => {
                    tracing::error!("Unable to create flight client: {}", err);
                    return;
                }
            };

        let mut interval_timer = tokio::time::interval(Duration::from_secs(30));

        loop {
            interval_timer.tick().await;

            let query = QueryBuilder::new(
                "SELECT * FROM runtime.metrics".to_string(),
                Arc::clone(&datafusion),
            )
            .build();

            match query.run().await {
                Ok(result) => match result.data.try_collect::<Vec<RecordBatch>>().await {
                    Ok(record_batches) => {
                        if let Err(err) = flight_client
                            .publish(dataset_path.as_str(), record_batches)
                            .await
                        {
                            tracing::error!("Failed to publish metrics to Spice Cloud: {}", err);
                            continue;
                        } else {
                            tracing::info!("Metrics published to Spice Cloud");
                        }
                    }
                    Err(err) => {
                        tracing::error!("Error collecting record batches: {}", err);
                    }
                },
                Err(err) => {
                    tracing::error!("Error running query: {}", err);
                }
            }

            // let schema = get_metrics_schema();
            // let timestamps: Vec<i64> = vec![12345678];
            // let instances: Vec<String> = vec![String::from("instance1")];
            // let names: Vec<String> = vec![String::from("name1")];
            // let values: Vec<f64> = vec![1.0];
            // let labels: Vec<String> = vec![String::from("label1")];
            // let datasets: Vec<Option<String>> = vec![Some(String::from("dataset1"))];
            // let engines: Vec<Option<String>> = vec![Some(String::from("engine1"))];
            // let datasets_array: ArrayRef = Arc::new(StringArray::from(datasets));
            // let engines_array: ArrayRef = Arc::new(StringArray::from(engines));

            // let Ok(record_batch) = RecordBatch::try_new(
            //     Arc::clone(&schema),
            //     vec![
            //         Arc::new(
            //             TimestampNanosecondArray::from(timestamps).with_timezone(Arc::from("UTC")),
            //         ),
            //         Arc::new(StringArray::from(instances)),
            //         Arc::new(StringArray::from(names)),
            //         Arc::new(Float64Array::from(values)),
            //         Arc::new(StringArray::from(labels)),
            //         Arc::new(StructArray::new(
            //             get_tags_fields(),
            //             vec![datasets_array, engines_array],
            //             None,
            //         )),
            //     ],
            // ) else {
            //     tracing::error!("Failed to create record batch");
            //     continue;
            // };

            // if let Err(err) = flight_client
            //     .publish(dataset_path.as_str(), vec![record_batch])
            //     .await
            // {
            //     tracing::error!("Failed to publish metrics to Spice Cloud: {}", err);
            //     continue;
            // }

            // tracing::info!("Metrics published to Spice Cloud");
        }
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

    async fn initialize(&mut self, _runtime: &mut Runtime) -> Result<()> {
        if !self.manifest.enabled {
            return Ok(());
        }

        Ok(())
    }

    async fn on_start(&mut self, runtime: &Runtime) -> Result<()> {
        if !self.manifest.enabled {
            return Ok(());
        }

        let secret = self
            .get_spice_secret(runtime)
            .await
            .boxed()
            .map_err(|e| runtime::extension::Error::UnableToStartExtension { source: e })?;

        let connection = self
            .connect(runtime)
            .await
            .boxed()
            .map_err(|e| runtime::extension::Error::UnableToStartExtension { source: e })?;

        let spiceai_metrics_dataset_path = format!(
            "spice.ai/{}/{}/{}",
            connection.org_name, connection.app_name, connection.metrics_dataset_name
        );

        let api_key = secret.get("key").unwrap_or_default();
        // let mut flight_client = FlightClient::new(self.spice_flight_url().as_str(), "", api_key)
        //     .await
        //     .boxed()
        //     .map_err(|e| runtime::extension::Error::UnableToStartExtension { source: e })?;
        //

        let flight_url = self.spice_flight_url().to_string();
        let api_key = api_key.to_string();
        let dataset_path = format!(
            "{}.{}.{}",
            connection.org_name, connection.app_name, connection.metrics_dataset_name
        )
        .to_string();
        let df = runtime.datafusion();
        let clone = Arc::clone(&df);

        tokio::spawn(SpiceExtension::start_sync(
            flight_url,
            api_key,
            dataset_path,
            clone,
        ));

        let from = spiceai_metrics_dataset_path.to_string();
        self.register_runtime_metrics_table(runtime, from.clone(), secret)
            .await?;
        tracing::info!("Enabled metrics sync from runtime.metrics to {from}",);

        Ok(())
    }
}

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
        Box::new(SpiceExtension::new(self.manifest.clone()))
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
    _table_reference: TableReference,
    _from: &str,
    _secret: Option<Secret>,
    _acceleration: Acceleration,
    _refresh: Refresh,
    _retention: Option<Retention>,
) -> Result<Arc<AcceleratedTable>, Error> {
    let metrics_table_reference = get_metrics_table_reference();

    let retention = Retention::new(
        Some("timestamp".to_string()),
        Some(TimeFormat::UnixSeconds),
        Some(Duration::from_secs(1800)), // delete metrics older then 30 minutes
        Some(Duration::from_secs(300)),  // run retention every 5 minutes
        true,
    );

    let table = create_internal_accelerated_table(
        metrics_table_reference.clone(),
        get_metrics_schema(),
        Acceleration::default(),
        Refresh::default(),
        retention,
    )
    .await
    .context(UnableToCreateMetricsTableSnafu)?;

    Ok(table)

    // let source_table_provider =
    //     get_spiceai_table_provider(table_reference.table(), from, secret).await?;

    // let accelerated_table_provider = create_accelerator_table(
    //     table_reference.clone(),
    //     source_table_provider.schema(),
    //     &acceleration,
    //     None,
    // )
    // .await
    // .context(UnableToCreateAcceleratedTableProviderSnafu)?;

    // let mut builder = AcceleratedTable::builder(
    //     table_reference.clone(),
    //     source_table_provider,
    //     accelerated_table_provider,
    //     refresh,
    // );

    // builder.retention(retention);

    // let (accelerated_table, _) = builder.build().await;

    // Ok(Arc::new(accelerated_table))
}

#[derive(Deserialize, Debug)]
#[allow(clippy::struct_field_names)]
struct SpiceCloudConnectResponse {
    org_name: String,
    app_name: String,
    metrics_dataset_name: String,
}
