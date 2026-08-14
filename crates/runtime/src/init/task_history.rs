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
use crate::{
    Error, Result, Runtime, UnableToCreateBackendSnafu, datafusion::SPICE_RUNTIME_SCHEMA,
    task_history,
};
use datafusion::catalog::TableProvider;
use datafusion::sql::TableReference;
use snafu::prelude::*;
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

        // A runtime that came up with no spicepod has no task-history table:
        // there was no configuration to read it from. The app that arrives later
        // — deployed, or written into a watched directory — initializes it, so
        // this can be reached twice, and the second call has to be a no-op.
        //
        // What decides that is this runtime's own record of having registered the
        // table, never the table's name: a spicepod may declare a dataset called
        // `runtime.task_history`, and on the arriving-app path datasets are
        // registered before this runs. A name check would take that dataset for
        // the internal table, report success, and send every task-history write
        // to it. So a name that is taken while this runtime has registered
        // nothing is a conflict, and it is reported rather than written into.
        if self
            .task_history_initialized
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            tracing::debug!("Task history is already initialized.");
            return Ok(());
        }
        let table = TableReference::partial(
            SPICE_RUNTIME_SCHEMA,
            task_history::DEFAULT_TASK_HISTORY_TABLE,
        );
        if self.df.table_exists(&table) {
            // Reporting the conflict is not enough on its own: the exporter
            // resolves this table by name at write time, so leaving emission on
            // would keep aiming internal rows at a table the runtime does not
            // own — filling it if the schema happens to fit, and failing every
            // export if it does not. Stop writing, then say why.
            self.df.set_task_history_enabled(false);
            return Err(Error::UnableToTrackTaskHistory {
                source: task_history::Error::TableNameTaken {
                    table: table.to_string(),
                },
            });
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

        let local_table = task_history::TaskSpan::instantiate_table(
            self.status(),
            retention_period_secs,
            retention_check_interval_secs,
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
                    local_table.into_table() as Arc<dyn TableProvider>;
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
                    self.df.cluster_config.client_tls_config(),
                    node_id,
                );
                Arc::new(federated)
            }
            _ => local_table.into_table() as Arc<dyn TableProvider>,
        };

        self.df
            .register_table_as_writable_and_with_schema(
                TableReference::partial(
                    SPICE_RUNTIME_SCHEMA,
                    task_history::DEFAULT_TASK_HISTORY_TABLE,
                ),
                table_to_register,
            )
            .context(UnableToCreateBackendSnafu)?;
        self.task_history_initialized
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}
