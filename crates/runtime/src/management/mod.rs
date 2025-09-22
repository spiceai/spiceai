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

const TASK_HISTORY_SINK_REMOTE_TABLE: &str = "runtime.task_history";
const TASK_HISTORY_SINK_TABLE: &str = "scp.task_history";
const DEFAULT_EXPORT_INTERVAL_SECS: u64 = 5;

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

use arrow::array::RecordBatch;
use chrono::{DateTime, Utc};
use datafusion::{
    catalog::TableProvider,
    datasource::DefaultTableSource,
    error::DataFusionError,
    execution::SessionStateBuilder,
    logical_expr::LogicalPlanBuilder,
    prelude::{DataFrame, SessionContext, col, lit},
    sql::TableReference,
};
use secrecy::{ExposeSecret, SecretString};
use snafu::{ResultExt, Snafu};
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
    component::dataset::{Mode, builder::DatasetBuilder},
    dataconnector::{DataConnectorError, create_new_connector, parameters::ConnectorParamsBuilder},
    datafusion::{
        DataFusion, SPICE_RUNTIME_SCHEMA, builder::get_df_default_config, error::SpiceExternalError,
    },
    dataupdate::{DataUpdate, UpdateType},
    get_params_with_secrets,
    secrets::Secrets,
    task_history::DEFAULT_TASK_HISTORY_TABLE,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing required secret: {name}. Specify a value."))]
    MissingRequiredSecret { name: String },

    #[snafu(display("Table provider does not support read_write mode"))]
    NoReadWriteProvider {},

    #[snafu(display(
        "Unable to create data connector: {source} Report a bug to request support: https://github.com/spiceai/spiceai/issues"
    ))]
    UnableToCreateDataConnector {
        source: Box<dyn std::error::Error + Sync + Send>,
    },

    #[snafu(display("{source}"))]
    UnableToCreateCloudTableProvider { source: DataConnectorError },

    #[snafu(display("Error exporting task_history records: {source}"))]
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

    match Management::try_from(config, runtime).await {
        Ok(management) => management.start().await,
        Err(e) => Err(e),
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
}

impl Management {
    pub async fn try_from(
        config: &SpicepodManagement,
        runtime: Arc<Runtime>,
    ) -> Result<Self, Error> {
        let secrets = runtime.secrets();

        let api_key: SecretString = if config.api_key.is_empty() {
            return Err(Error::MissingRequiredSecret {
                name: "api_key".to_string(),
            });
        } else {
            resolve_secret(&secrets, &config.api_key).await
        };

        let params = get_params_with_secrets(secrets, &config.params).await;

        Ok(Self {
            runtime,
            api_key,
            params,
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
        let mut params = HashMap::new();
        params.insert(
            "spiceai_api_key".to_string(),
            self.api_key.expose_secret().to_string(),
        );

        if let Some(flight_endpoint) = self.params.get("data_endpoint") {
            params.insert(
                "spiceai_endpoint".to_string(),
                flight_endpoint.expose_secret().to_string(),
            );
        }

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
            .checked_sub(Duration::from_secs(3 * 24 * 60 * 60))
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

    let mut dataset = DatasetBuilder::try_new(format!("spice.ai/{cloud_dataset_path}"), name)
        .boxed()
        .context(UnableToCreateDataConnectorSnafu)?
        .with_app(Arc::clone(app))
        .with_runtime(runtime)
        .build()
        .boxed()
        .context(UnableToCreateDataConnectorSnafu)?
        .with_params(params);

    dataset.mode = Mode::ReadWrite;

    let params = ConnectorParamsBuilder::new("spice.ai".into(), (&dataset).into())
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
        }
    }
    false
}

// Resolve a secret by key, returning the secret string if found, or the original key if not.
async fn resolve_secret(secrets: &Arc<RwLock<Secrets>>, key: &str) -> SecretString {
    let secrets = secrets.read().await;
    if let Ok(Some(secret)) = secrets.get_secret(key).await {
        secret
    } else {
        SecretString::new(key.to_string().into())
    }
}

fn retry_strategy() -> FibonacciBackoff {
    // Retry up to 10 times, with a maximum interval of 55 seconds between retries
    FibonacciBackoffBuilder::new().max_retries(Some(10)).build()
}
