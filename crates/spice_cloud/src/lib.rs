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

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datafusion::{
    arrow::array::RecordBatch,
    datasource::{DefaultTableSource, TableProvider},
    execution::SessionStateBuilder,
    logical_expr::LogicalPlanBuilder,
    prelude::{DataFrame, SessionContext},
    sql::TableReference,
};
use snafu::{ResultExt, prelude::*};

use datafusion::logical_expr::{col, lit};
use runtime::{
    Runtime,
    accelerated_table::AcceleratedTableBuilderError,
    component::dataset::{Mode, builder::DatasetBuilder},
    dataaccelerator::{self},
    dataconnector::{ConnectorParamsBuilder, DataConnectorError, create_new_connector},
    datafusion::{
        DataFusion, SPICE_RUNTIME_SCHEMA, builder::get_df_default_config,
        error::find_datafusion_root,
    },
    dataupdate::{DataUpdate, UpdateType},
    extension::{Error as ExtensionError, Extension, ExtensionFactory, ExtensionManifest, Result},
    secrets::Secrets,
    task_history::DEFAULT_TASK_HISTORY_TABLE,
};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

const TASK_HISTORY_SINK_REMOTE_TABLE: &str = "runtime.task_history";
const TASK_HISTORY_SINK_TABLE: &str = "runtime.task_history_scp";
const DEFAULT_EXPORT_INTERVAL_SECS: u64 = 5;
const DEFAULT_EXPORT_LATER_ARRIVED_RECORDS_SECS: u64 = 5;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to get read-write table provider"))]
    NoReadWriteProvider {},

    #[snafu(display(
        "Unable to create data connector: {source}\nReport a bug to request support: https://github.com/spiceai/spiceai/issues"
    ))]
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

    #[snafu(display("Spice Cloud api_key not provided"))]
    SpiceApiKeyNotFound {},

    #[snafu(display("Unable to build accelerated table: {source}"))]
    UnableToBuildAcceleratedTable {
        source: AcceleratedTableBuilderError,
    },

    #[snafu(display("Error exporting task_history records: {source}"))]
    UnableToExportTaskHistoryData {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub struct ScpManagementExtension {
    manifest: ExtensionManifest,
    api_key: String,
}

impl ScpManagementExtension {
    #[must_use]
    pub fn new(manifest: ExtensionManifest) -> Self {
        ScpManagementExtension {
            manifest,
            api_key: String::new(),
        }
    }

    fn get_flight_url(&self) -> String {
        self.manifest
            .params
            .get("flight_endpoint")
            .unwrap_or(&"https://flight.spiceai.io".to_string())
            .to_string()
    }

    fn get_api_key(&self, _runtime: &Runtime) -> Result<String, Error> {
        self.manifest
            .params
            .get("api_key")
            .ok_or(Error::SpiceApiKeyNotFound {})
            .cloned()
    }

    fn calculate_export_since_time(last_exported_time: SystemTime) -> SystemTime {
        last_exported_time
            .checked_sub(Duration::from_secs(
                DEFAULT_EXPORT_LATER_ARRIVED_RECORDS_SECS,
            ))
            .unwrap_or(last_exported_time)
    }

    async fn export_task_history_records(df: &Arc<DataFusion>, since: SystemTime) -> bool {
        let data = match get_task_history_records(df, since)
            .await
            .map_err(find_datafusion_root)
            .boxed()
            .context(UnableToExportTaskHistoryDataSnafu)
        {
            Ok(records) => records,
            Err(e) => {
                tracing::warn!("{e}. Retrying in {DEFAULT_EXPORT_INTERVAL_SECS} seconds");
                return false;
            }
        };

        if data.is_empty() {
            tracing::trace!("No new task history records to export");
            return true;
        }

        if let Err(e) = write_task_history_records_to_remote(df, data).await {
            tracing::warn!("{e}. Retrying in {DEFAULT_EXPORT_INTERVAL_SECS} seconds");
            return false;
        }

        true
    }

    async fn start_task_history_export(&self, runtime: Arc<Runtime>) -> Result<()> {
        let app_ref = runtime.app();
        let app_lock = app_ref.read().await;
        if let Some(app) = app_lock.as_ref() {
            if !app.runtime.task_history.enabled {
                tracing::debug!("Task history is disabled via configuration.");
                return Ok(());
            }
        }
        drop(app_lock);

        self.init_task_history_sink_table(&runtime).await?;

        let cancellation_token = CancellationToken::new();
        let df = runtime.datafusion();

        let _task = runtime
            .start_runtime_task(
                "task_history_export",
                Some(cancellation_token.clone()),
                async move {
                    let mut interval =
                        tokio::time::interval(Duration::from_secs(DEFAULT_EXPORT_INTERVAL_SECS));

                    let mut last_exported_time = SystemTime::now();

                    loop {
                        tokio::select! {
                            _ = interval.tick() => {
                            }
                            () = cancellation_token.cancelled() => {
                                // Runtime shutdown requested, write latest available data and stop exporting
                                let since = Self::calculate_export_since_time(last_exported_time);
                                let _= Self::export_task_history_records(&df, since).await;
                                tracing::debug!("Task history data export stopped");
                                break;
                            }
                        };

                        // new candidate time for last export, as retrieving and exporting records is not atomic and can take time,
                        // we calcualte new time candidate before fetching records, not after we retrieved or send them
                        let last_exported_time_new = SystemTime::now();

                        // export only records added after last export (plus additional buffer for late arrivals)
                        let since = Self::calculate_export_since_time(last_exported_time);

                        if !Self::export_task_history_records(&df, since).await {
                            continue;
                        }

                        last_exported_time = last_exported_time_new;
                    }

                    Ok(())
                },
            )
            .await;

        tracing::debug!("Enabled task history data export to Spice Cloud");

        Ok(())
    }

    async fn init_task_history_sink_table(&self, runtime: &Arc<Runtime>) -> Result<()> {
        let mut params = HashMap::new();
        params.insert("spiceai_endpoint".to_string(), self.get_flight_url());
        params.insert("spiceai_api_key".to_string(), self.api_key.to_string());

        let sink = get_spiceai_table_provider(
            TASK_HISTORY_SINK_TABLE,
            TASK_HISTORY_SINK_REMOTE_TABLE,
            runtime.secrets(),
            params,
            Arc::clone(runtime),
        )
        .await
        .boxed()
        .map_err(|e| runtime::extension::Error::UnableToStartExtension { source: e })?;

        runtime
            .datafusion()
            .register_table_as_writable_and_with_schema(TASK_HISTORY_SINK_TABLE.into(), sink)
            .boxed()
            .map_err(|e| runtime::extension::Error::UnableToStartExtension { source: e })?;

        Ok(())
    }
}

impl Default for ScpManagementExtension {
    fn default() -> Self {
        ScpManagementExtension::new(ExtensionManifest::default())
    }
}

#[async_trait]
impl Extension for ScpManagementExtension {
    fn name(&self) -> &'static str {
        "management"
    }

    async fn initialize(&mut self, runtime: &Runtime) -> Result<()> {
        if !self.manifest.enabled {
            return Ok(());
        }

        let api_key = self
            .get_api_key(runtime)
            .boxed()
            .map_err(|source| ExtensionError::UnableToInitializeExtension { source })?;

        self.api_key = api_key;

        Ok(())
    }

    async fn on_start(&self, runtime: Arc<Runtime>) -> Result<()> {
        self.start_task_history_export(runtime).await?;
        tracing::info!("Initialized Spice Cloud management");
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
        Box::new(ScpManagementExtension {
            manifest: self.manifest.clone(),
            api_key: String::new(),
        })
    }
}

async fn get_spiceai_table_provider(
    name: &str,
    cloud_dataset_path: &str,
    secrets: Arc<RwLock<Secrets>>,
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
        .context(UnableToCreateSourceTableProviderSnafu)?;

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

    df.write_data(&TASK_HISTORY_SINK_TABLE.into(), data_update)
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
