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

const TASK_HISTORY_SINK_REMOTE_TABLE: &str = "runtime.task_history";
const TASK_HISTORY_SINK_TABLE: &str = "scp.task_history";
const DEFAULT_EXPORT_INTERVAL_SECS: u64 = 5;

use crate::dataconnector::parameters::RuntimeConnectorContext;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

use arrow::array::RecordBatch;
use chrono::{DateTime, Utc};
use datafusion::{
    catalog::TableProvider,
    common::TableReference,
    datasource::DefaultTableSource,
    error::DataFusionError,
    execution::SessionStateBuilder,
    logical_expr::LogicalPlanBuilder,
    prelude::{DataFrame, SessionContext, col, lit},
};
use runtime_secrets::{ParamStr, Secrets, get_params_with_secrets, iter_secret_references};
use secrecy::{ExposeSecret, SecretString};
use snafu::{ResultExt, Snafu, ensure};
use spice_cloud_client::endpoints::{
    DEFAULT_REGION as SPICE_CLOUD_DEFAULT_REGION, flight_endpoint as spice_cloud_flight_endpoint,
};
use spicepod::component::management::Management as SpicepodManagement;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use util::{
    RetryError,
    fibonacci_backoff::{FibonacciBackoff, FibonacciBackoffBuilder},
    retry,
};

use crate::{
    Runtime,
    component::access::AccessMode,
    component::dataset::builder::DatasetBuilder,
    dataconnector::{DataConnectorError, create_new_connector, parameters::ConnectorParamsBuilder},
    datafusion::{
        DataFusion, SPICE_RUNTIME_SCHEMA, builder::get_df_default_config, error::SpiceExternalError,
    },
    dataupdate::{DataUpdate, UpdateType},
    status::ComponentStatus,
    task_history::DEFAULT_TASK_HISTORY_TABLE,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing required secret: {name}. Specify a value."))]
    MissingRequiredSecret { name: String },

    #[snafu(display(
        "Dataset does not support read-write mode. Ensure the acceleration engine supports writes."
    ))]
    NoReadWriteProvider {},

    #[snafu(display(
        "Unable to create data connector: {source} Report a bug to request support: https://github.com/spiceai/spiceai/issues"
    ))]
    UnableToCreateDataConnector {
        source: Box<dyn std::error::Error + Sync + Send>,
    },

    #[snafu(display("Failed to create data connector for cloud management: {source}"))]
    UnableToCreateCloudTableProvider { source: DataConnectorError },

    #[snafu(display("Failed to export runtime task history records: {source}"))]
    UnableToExportTaskHistoryData {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub(crate) async fn init_management(
    runtime: Arc<Runtime>,
    config: &SpicepodManagement,
) -> Result<(), Error> {
    if !config.enabled {
        return Ok(());
    }

    let status = runtime.status();
    let endpoint_hint = management_endpoint_from_raw_params(&config.params);
    status.update_management(ComponentStatus::Initializing, endpoint_hint.clone());

    match Management::try_from(config, runtime).await {
        Ok(management) => {
            let endpoint = management.endpoint.clone();
            match management.start().await {
                Ok(()) => {
                    status.update_management(ComponentStatus::Ready, endpoint);
                    Ok(())
                }
                Err(err) => {
                    status.update_management(
                        ComponentStatus::error_with_message(err.to_string()),
                        endpoint,
                    );
                    Err(err)
                }
            }
        }
        Err(err) => {
            status.update_management(
                ComponentStatus::error_with_message(err.to_string()),
                endpoint_hint,
            );
            Err(err)
        }
    }
}

/// Spice Cloud Management
///
/// Implements runtime management and observability for Spice instances running
/// outside of the Spice Cloud Platform, enabling monitoring and troubleshooting capabilities
/// across on-premises and cluster environments.
pub(crate) struct Management {
    runtime: Arc<Runtime>,
    api_key: SecretString,
    params: HashMap<String, SecretString>,
    endpoint: String,
}

impl Management {
    pub async fn try_from(
        config: &SpicepodManagement,
        runtime: Arc<Runtime>,
    ) -> Result<Self, Error> {
        let secrets = runtime.secrets();

        // Validate `api_key` before region so a missing secret is not hidden
        // behind a dataset-key `spiceai_region` error from the spice.ai connector.
        let api_key = resolve_api_key(&secrets, &config.api_key).await?;
        let params = get_params_with_secrets(secrets, &config.params).await;
        let endpoint = management_endpoint(&params);

        Ok(Self {
            runtime,
            api_key,
            params,
            endpoint,
        })
    }

    pub async fn start(&self) -> Result<(), Error> {
        self.start_task_history_export().await?;
        tracing::info!("Connected to Spice Cloud for management and monitoring");
        Ok(())
    }

    async fn start_task_history_export(&self) -> Result<(), Error> {
        let app_ref = self.runtime.app();
        let app_lock = app_ref.read().await;
        if let Some(app) = app_lock.as_ref()
            && !app.runtime.task_history.enabled
        {
            tracing::debug!("Task history is disabled via configuration.");
            return Ok(());
        }
        drop(app_lock);

        self.init_task_history_sink_table().await?;

        let cancellation_token = CancellationToken::new();
        let df = self.runtime.datafusion();

        let _task = self
            .runtime
            .start_runtime_task(
                "task_history_export",
                Some(cancellation_token.clone()),
                async move {
                    let mut interval =
                        tokio::time::interval(Duration::from_secs(DEFAULT_EXPORT_INTERVAL_SECS));

                    loop {
                        tokio::select! {
                            _ = interval.tick() => {
                            }
                            () = cancellation_token.cancelled() => {
                                // Runtime shutdown requested, write latest available data and stop exporting
                                let since = Self::calculate_export_since_time();
                                let _= Self::export_task_history_records(&df, since).await;
                                tracing::debug!("Task history data export stopped");
                                break;
                            }
                        };

                        let since = Self::calculate_export_since_time();
                        let _ = Self::export_task_history_records(&df, since).await;
                    }

                    Ok(())
                },
            )
            .await;

        tracing::debug!("Enabled task history data export");

        Ok(())
    }

    async fn init_task_history_sink_table(&self) -> Result<(), Error> {
        let params = spiceai_sink_params(&self.api_key, &self.params);

        let sink = get_spiceai_table_provider(
            TASK_HISTORY_SINK_TABLE,
            TASK_HISTORY_SINK_REMOTE_TABLE,
            params,
            Arc::clone(&self.runtime),
        )
        .await?;

        self.runtime
            .datafusion()
            .register_table_as_writable_and_with_schema(TASK_HISTORY_SINK_TABLE.into(), sink)
            .boxed()
            .context(UnableToCreateDataConnectorSnafu)
    }

    // Calculate the timestamp for 3 days ago from now
    fn calculate_export_since_time() -> SystemTime {
        SystemTime::now()
            .checked_sub(Duration::from_hours(72))
            .unwrap_or(SystemTime::UNIX_EPOCH)
    }

    // Export task history records to Spice Cloud, returning true if any records were exported
    async fn export_task_history_records(df: &Arc<DataFusion>, since: SystemTime) -> bool {
        let data = match get_task_history_records(df, since).await {
            Ok(records) => records,
            Err(e) => {
                if is_table_not_ready_error(&e) {
                    tracing::debug!("Task history table is not ready yet, retrying later");
                    return false;
                }

                tracing::warn!(
                    "{}. Retrying in {DEFAULT_EXPORT_INTERVAL_SECS} seconds",
                    Error::UnableToExportTaskHistoryData {
                        source: Box::new(e)
                    }
                );
                return false;
            }
        };

        if data.is_empty() {
            tracing::trace!("No task history records to export");
            return false;
        }

        if let Err(e) = write_task_history_records_to_remote(df, data).await {
            tracing::warn!("{e}. Retrying in {DEFAULT_EXPORT_INTERVAL_SECS} seconds");
            return false;
        }

        true
    }
}

async fn get_spiceai_table_provider(
    name: &str,
    cloud_dataset_path: &str,
    params: HashMap<String, String>,
    runtime: Arc<Runtime>,
) -> Result<Arc<dyn TableProvider>, Error> {
    let app_ref = runtime.app();
    let app_lock = app_ref.read().await;
    let Some(app) = app_lock.as_ref() else {
        return Err(Error::UnableToCreateDataConnector {
            source: "Missing App From Runtime".into(),
        });
    };

    let secrets = runtime.secrets();
    let tokio_io_runtime = runtime.tokio_io_runtime();

    let mut dataset = DatasetBuilder::try_new(format!("spice.ai/{cloud_dataset_path}"), name)
        .boxed()
        .context(UnableToCreateDataConnectorSnafu)?
        .with_app(Arc::clone(app))
        .with_runtime(runtime)
        .build()
        .boxed()
        .context(UnableToCreateDataConnectorSnafu)?
        .with_params(params);

    dataset.access = AccessMode::ReadWrite;

    let params = ConnectorParamsBuilder::for_dataset("spice.ai".into(), &dataset)
        .build(secrets, tokio_io_runtime)
        .await
        .context(UnableToCreateDataConnectorSnafu)?;

    let context = RuntimeConnectorContext::for_dataset(&dataset);
    let data_connector = create_new_connector("spice.ai", params, &context)
        .await
        .ok_or_else(|| NoReadWriteProviderSnafu {}.build())?
        .context(UnableToCreateDataConnectorSnafu)?;

    let source_table_provider = data_connector
        .read_write_provider(&context, &dataset)
        .await
        .ok_or_else(|| NoReadWriteProviderSnafu {}.build())?
        .context(UnableToCreateCloudTableProviderSnafu)?;

    Ok(source_table_provider)
}

async fn write_task_history_records_to_remote(
    df: &Arc<DataFusion>,
    data: Vec<RecordBatch>,
) -> Result<(), Error> {
    let Some(schema) = data.first().map(RecordBatch::schema) else {
        tracing::trace!("No records to export for task history");
        return Ok(());
    };

    let num_records: usize = data
        .iter()
        .map(datafusion::arrow::array::RecordBatch::num_rows)
        .sum();

    let data_update = DataUpdate {
        schema,
        data,
        update_type: UpdateType::Append,
    };

    retry(retry_strategy(), || async {
        df.write_data(&TASK_HISTORY_SINK_TABLE.into(), data_update.clone())
            .await
            .map_err(RetryError::transient)
    })
    .await
    .boxed()
    .context(UnableToExportTaskHistoryDataSnafu)?;

    tracing::debug!("Exported {num_records} task history records");

    Ok(())
}

async fn get_task_history_records(
    df: &Arc<DataFusion>,
    since: SystemTime,
) -> Result<Vec<RecordBatch>, datafusion::error::DataFusionError> {
    let state = SessionStateBuilder::new()
        .with_config(get_df_default_config())
        .build();

    let ctx = SessionContext::new_with_state(state);

    let Ok(table_provider) = df
        .get_accelerated_table_provider("runtime.task_history")
        .await
    else {
        // If the table provider is not available, it means task history is not registered or ready yet
        tracing::debug!("Task history table is not registered or ready yet.");
        return Ok(vec![]);
    };

    // Build filter expression: end_time >= since
    let since_dt = Into::<DateTime<Utc>>::into(since).to_rfc3339();
    let filter_expr = col("end_time").gt_eq(lit(since_dt));

    let table_source = Arc::new(DefaultTableSource::new(Arc::clone(&table_provider)));

    let logical_plan = LogicalPlanBuilder::scan(
        TableReference::partial(SPICE_RUNTIME_SCHEMA, DEFAULT_TASK_HISTORY_TABLE),
        table_source,
        None,
    )?
    .filter(filter_expr)?
    .build()?;

    let df = DataFrame::new(ctx.state(), logical_plan);

    df.collect().await
}

fn is_table_not_ready_error(e: &DataFusionError) -> bool {
    if let DataFusionError::External(e) = e
        && let Some(e) = e.downcast_ref::<SpiceExternalError>()
    {
        match e {
            SpiceExternalError::AccelerationNotReady { .. } => return true,
            SpiceExternalError::PartitionsUnavailable { .. }
            | SpiceExternalError::ShuttingDown { .. } => {}
        }
    }
    false
}

/// Resolves `management.api_key` before any Cloud region is considered.
///
/// A `${ store:key }` reference that does not resolve — and an empty value —
/// become `Missing required secret: api_key`, matching the documented
/// troubleshooting table.
async fn resolve_api_key(
    secrets: &Arc<RwLock<Secrets>>,
    api_key: &str,
) -> Result<SecretString, Error> {
    ensure!(
        !api_key.trim().is_empty(),
        MissingRequiredSecretSnafu { name: "api_key" }
    );

    let secrets = Secrets::snapshot(secrets).await;

    if iter_secret_references(api_key).next().is_some() {
        let injected = secrets.inject_secrets("api_key", ParamStr(api_key)).await;
        ensure!(
            !injected.expose_secret().trim().is_empty(),
            MissingRequiredSecretSnafu { name: "api_key" }
        );
        return Ok(injected);
    }

    if let Ok(Some(secret)) = secrets.get_secret(api_key).await {
        return Ok(secret);
    }

    Ok(SecretString::new(api_key.to_string().into()))
}

fn optional_secret_param(params: &HashMap<String, SecretString>, key: &str) -> Option<String> {
    params
        .get(key)
        .map(|value| value.expose_secret().to_string())
        .filter(|value| !value.is_empty())
}

/// Connector params for the Cloud management sink.
///
/// When neither `region` nor `data_endpoint` is set, uses the documented Spice
/// Cloud default region so the `api_key`-only `management:` block works.
fn spiceai_sink_params(
    api_key: &SecretString,
    params: &HashMap<String, SecretString>,
) -> HashMap<String, String> {
    let mut sink = HashMap::new();
    sink.insert(
        "spiceai_api_key".to_string(),
        api_key.expose_secret().to_string(),
    );

    if let Some(endpoint) = optional_secret_param(params, "data_endpoint") {
        sink.insert("spiceai_endpoint".to_string(), endpoint);
    }

    if let Some(region) = optional_secret_param(params, "region") {
        sink.insert("spiceai_region".to_string(), region);
    } else if !sink.contains_key("spiceai_endpoint") {
        sink.insert(
            "spiceai_region".to_string(),
            SPICE_CLOUD_DEFAULT_REGION.to_string(),
        );
    }

    sink
}

fn resolve_management_endpoint(region: Option<&str>, data_endpoint: Option<&str>) -> String {
    if let Some(endpoint) = data_endpoint.filter(|value| !value.is_empty()) {
        return endpoint.to_string();
    }

    let region = region
        .filter(|value| !value.is_empty())
        .unwrap_or(SPICE_CLOUD_DEFAULT_REGION);
    spice_cloud_flight_endpoint(region)
}

fn management_endpoint(params: &HashMap<String, SecretString>) -> String {
    resolve_management_endpoint(
        optional_secret_param(params, "region").as_deref(),
        optional_secret_param(params, "data_endpoint").as_deref(),
    )
}

fn management_endpoint_from_raw_params(params: &HashMap<String, String>) -> String {
    resolve_management_endpoint(
        params.get("region").map(String::as_str),
        params.get("data_endpoint").map(String::as_str),
    )
}

fn retry_strategy() -> FibonacciBackoff {
    // Retry up to 10 times, with a maximum interval of 55 seconds between retries
    FibonacciBackoffBuilder::new().max_retries(Some(10)).build()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secret_map(pairs: &[(&str, &str)]) -> HashMap<String, SecretString> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), SecretString::from(*value)))
            .collect()
    }

    #[test]
    fn minimal_block_defaults_spice_cloud_region() {
        let api_key = SecretString::from("000000|bogus");
        let params = spiceai_sink_params(&api_key, &HashMap::new());
        assert_eq!(
            params.get("spiceai_region").map(String::as_str),
            Some(SPICE_CLOUD_DEFAULT_REGION)
        );
        assert!(!params.contains_key("spiceai_endpoint"));
        assert_eq!(
            management_endpoint(&HashMap::new()),
            spice_cloud_flight_endpoint(SPICE_CLOUD_DEFAULT_REGION)
        );
        assert_eq!(
            management_endpoint_from_raw_params(&HashMap::new()),
            spice_cloud_flight_endpoint(SPICE_CLOUD_DEFAULT_REGION)
        );
    }

    #[test]
    fn explicit_region_is_preserved() {
        let api_key = SecretString::from("key");
        let params = spiceai_sink_params(&api_key, &secret_map(&[("region", "eu-central-1")]));
        assert_eq!(
            params.get("spiceai_region").map(String::as_str),
            Some("eu-central-1")
        );
    }

    #[test]
    fn data_endpoint_does_not_default_region() {
        let api_key = SecretString::from("key");
        let params = spiceai_sink_params(
            &api_key,
            &secret_map(&[("data_endpoint", "http://127.0.0.1:50051")]),
        );
        assert!(
            !params.contains_key("spiceai_region"),
            "a local data_endpoint must not invent a Cloud region: {params:?}"
        );
        assert_eq!(
            params.get("spiceai_endpoint").map(String::as_str),
            Some("http://127.0.0.1:50051")
        );
    }

    #[test]
    fn empty_region_is_treated_as_unset() {
        let api_key = SecretString::from("key");
        let params = spiceai_sink_params(&api_key, &secret_map(&[("region", "")]));
        assert_eq!(
            params.get("spiceai_region").map(String::as_str),
            Some(SPICE_CLOUD_DEFAULT_REGION)
        );
    }

    #[tokio::test]
    async fn empty_api_key_is_missing_secret() {
        let secrets = Arc::new(RwLock::new(Secrets::new()));
        let err = resolve_api_key(&secrets, "")
            .await
            .expect_err("empty api_key must fail");
        assert!(
            err.to_string()
                .starts_with("Missing required secret: api_key"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn unresolved_secret_reference_is_missing_secret() {
        let secrets = Arc::new(RwLock::new(Secrets::new()));
        let err = resolve_api_key(&secrets, "${secrets:MANAGEMENT_TEST_UNSET_API_KEY}")
            .await
            .expect_err("missing secret must fail");
        assert!(
            err.to_string()
                .starts_with("Missing required secret: api_key"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn literal_api_key_is_kept() {
        let secrets = Arc::new(RwLock::new(Secrets::new()));
        let key = resolve_api_key(&secrets, "000000|bogus")
            .await
            .expect("literal api_key");
        assert_eq!(key.expose_secret(), "000000|bogus");
    }
}
