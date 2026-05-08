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

use crate::cluster::DistributedNode;
use crate::component::access::AccessMode;
use crate::component::dataset::builder::DatasetBuilder;
use crate::dataconnector::{
    self, parameters::ConnectorParamsBuilder, registered_connector_names, suggest_connector,
};
use crate::{
    Error, Result, Runtime, UnableToCreateBackendSnafu, UnableToInitializeDataConnectorSnafu,
    UnknownDataConnectorSnafu, datafusion::SPICE_RUNTIME_SCHEMA, task_history,
};
use datafusion::catalog::TableProvider;
use datafusion::sql::TableReference;
use snafu::prelude::*;
use spicepod::component::runtime::TaskHistoryPersistence;
use spicepod::param::Params;
use std::fmt::Write;
use std::sync::Arc;

impl Runtime {
    pub async fn init_task_history(self: Arc<Self>) -> Result<()> {
        // Skip task history initialization if there's no valid spicepod
        // Task history requires App infrastructure (datasets, table providers) to function
        let Some(app) = self.read_app().await else {
            tracing::debug!(
                "Task history initialization skipped: no valid spicepod configuration."
            );
            return Ok(());
        };

        if !app.runtime.task_history.enabled {
            tracing::debug!("Task history is disabled via configuration.");
            return Ok(());
        }

        let retention_period_secs = app
            .runtime
            .task_history
            .retention_period_as_secs()
            .map_err(|e| Error::UnableToTrackTaskHistory {
                source: task_history::Error::InvalidConfiguration { source: e },
            })?;

        let retention_check_interval_secs = app
            .runtime
            .task_history
            .retention_check_interval_as_secs()
            .map_err(|e| Error::UnableToTrackTaskHistory {
                source: task_history::Error::InvalidConfiguration { source: e },
            })?;

        // Log task history configuration details
        let mut config_details = format!(
            "Task history enabled: retention_period={retention_period_secs}s, retention_check_interval={retention_check_interval_secs}s"
        );

        if app.runtime.task_history.captured_context.as_ref() != "truncated" {
            let captured_context = &app.runtime.task_history.captured_context;
            let _ = write!(config_details, ", captured_context={captured_context}");
        }

        // Add min_sql_duration if configured
        if let Some(min_sql_duration) = &app.runtime.task_history.min_sql_duration {
            let _ = write!(config_details, ", min_sql_duration={min_sql_duration}");
        }

        // Add captured_plan and min_plan_duration if configured
        if let Some(captured_plan) = &app.runtime.task_history.captured_plan
            && captured_plan.as_ref() != "none"
        {
            let _ = write!(config_details, ", captured_plan={captured_plan}");

            if let Some(min_plan_duration) = &app.runtime.task_history.min_plan_duration {
                let _ = write!(config_details, ", min_plan_duration={min_plan_duration}");
            }
        }

        tracing::info!("{}", config_details);

        // Determine if we're in cluster mode (node_id column needed)
        let effective_role = self.df.cluster_config.effective_role();
        let is_cluster_mode = effective_role.is_some();

        let persistence_source = match app.runtime.task_history.persistence.clone() {
            Some(persistence) => Some(
                self.build_task_history_persistence_source(persistence)
                    .await?,
            ),
            None => None,
        };

        let local_table = task_history::TaskSpan::instantiate_table(
            self.status(),
            retention_period_secs,
            retention_check_interval_secs,
            persistence_source,
            Arc::clone(&self),
            is_cluster_mode,
        )
        .await
        .map_err(|source| Error::UnableToTrackTaskHistory { source })?;

        // In cluster scheduler mode, wrap the local table with FederatedTaskHistoryTable
        // to enable cluster-wide task history queries, and also register the local table
        // separately for use by the GetTaskHistory RPC handler
        let table_to_register: Arc<dyn TableProvider> = match &self.distributed {
            Some(DistributedNode::Scheduler {
                peers,
                executor_registry,
                ..
            }) => {
                let schema = local_table.schema();

                // Compute node_id: {advertise_host}:{bind_port}
                let node_id =
                    if let Some(advertise_host) = self.df.cluster_config.node_advertise_address() {
                        let bind_port = self.df.cluster_config.node_bind_address().port();
                        format!("{advertise_host}:{bind_port}")
                    } else {
                        // Fallback: use bind address directly (shouldn't happen in valid scheduler config)
                        self.df.cluster_config.node_bind_address().to_string()
                    };

                tracing::debug!("Registering federated task_history table with node_id={node_id}");

                // Register the local table under a separate name for RPC handlers to use
                // This avoids infinite recursion when peers query each other
                let local_table_provider: Arc<dyn TableProvider> =
                    local_table as Arc<dyn TableProvider>;
                self.df
                    .register_table_as_writable_and_with_schema(
                        TableReference::partial(
                            SPICE_RUNTIME_SCHEMA,
                            task_history::LOCAL_TASK_HISTORY_TABLE,
                        ),
                        Arc::clone(&local_table_provider),
                    )
                    .context(UnableToCreateBackendSnafu)?;

                let federated = task_history::federated::FederatedTaskHistoryTable::new(
                    schema,
                    local_table_provider,
                    Arc::clone(peers),
                    executor_registry.flight_sql_clients_handle(),
                    self.df.cluster_config.client_tls_config().cloned(),
                    node_id,
                );
                Arc::new(federated)
            }
            _ => local_table,
        };

        self.df
            .register_table_as_writable_and_with_schema(
                TableReference::partial(
                    SPICE_RUNTIME_SCHEMA,
                    task_history::DEFAULT_TASK_HISTORY_TABLE,
                ),
                table_to_register,
            )
            .context(UnableToCreateBackendSnafu)
    }

    /// Resolves the configured `task_history.persistence` block into a
    /// writable `TableProvider` that the accelerated table will write back to.
    /// The connector name is the prefix of `from:` (e.g. `postgres` in
    /// `postgres:public.task_history`), and must support DDL/DML.
    async fn build_task_history_persistence_source(
        self: &Arc<Self>,
        persistence: TaskHistoryPersistence,
    ) -> Result<Arc<dyn TableProvider>> {
        let app = self.app();
        let app_guard = app.read().await;
        let app_ref =
            app_guard
                .as_ref()
                .cloned()
                .ok_or_else(|| Error::UnableToTrackTaskHistory {
                    source: task_history::Error::InvalidConfiguration {
                        source: "task_history.persistence requires a loaded spicepod App".into(),
                    },
                })?;
        drop(app_guard);

        let table_name = format!(
            "{SPICE_RUNTIME_SCHEMA}.{}",
            task_history::DEFAULT_TASK_HISTORY_TABLE
        );
        let mut dataset = DatasetBuilder::try_new(persistence.from.clone(), &table_name)?
            .with_app(app_ref)
            .with_runtime(Arc::clone(self))
            .build()
            .map_err(|source| Error::InvalidSpicepodDataset { source })?;
        dataset.access = AccessMode::ReadWrite;
        dataset.params = persistence
            .params
            .as_ref()
            .map(Params::as_string_map)
            .unwrap_or_default();

        let source = dataset.source().to_string();
        let dataset = Arc::new(dataset);

        let connector_name: Arc<str> = Arc::from(source.as_str());
        let params = ConnectorParamsBuilder::new(connector_name, (&dataset).into())
            .build(self.secrets(), self.tokio_io_runtime())
            .await
            .context(UnableToInitializeDataConnectorSnafu)?;

        let connector =
            if let Some(result) = dataconnector::create_new_connector(&source, params).await {
                result.context(UnableToInitializeDataConnectorSnafu)?
            } else {
                let suggestion = suggest_connector(&source).await;
                let available = registered_connector_names().await;
                return Err(UnknownDataConnectorSnafu {
                    data_connector: source,
                    suggestion,
                    available,
                }
                .build());
            };

        let provider = connector
            .read_write_provider(&dataset)
            .await
            .ok_or_else(|| Error::UnableToTrackTaskHistory {
                source: task_history::Error::InvalidConfiguration {
                    source: format!(
                        "data connector `{source}` does not expose a writable provider \
                         (INSERT/DELETE) and cannot back `runtime.task_history.persistence`"
                    )
                    .into(),
                },
            })?
            .map_err(|e| Error::UnableToInitializeDataConnector {
                source: Box::new(e),
            })?;

        Ok(provider)
    }
}
