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

use crate::{
    Error, Result, Runtime, UnableToCreateBackendSnafu, config::ClusterRole,
    datafusion::SPICE_RUNTIME_SCHEMA, task_history,
};
use datafusion::catalog::TableProvider;
use datafusion::sql::TableReference;
use snafu::prelude::*;
use std::fmt::Write;
use std::sync::Arc;

impl Runtime {
    pub async fn init_task_history(self: Arc<Self>) -> Result<()> {
        let app = self.app.read().await;

        // Skip task history initialization if there's no valid spicepod
        // Task history requires App infrastructure (datasets, table providers) to function
        let Some(app) = app.as_ref() else {
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

        // Determine if we're in cluster mode (scheduler_id column needed)
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
        let table_to_register: Arc<dyn TableProvider> =
            if matches!(effective_role, Some(ClusterRole::Scheduler)) {
                let schema = local_table.schema();

                // Compute scheduler_id: {advertise_host}:{bind_port}
                let scheduler_id =
                    if let Some(advertise_host) = self.df.cluster_config.node_advertise_address() {
                        let bind_port = self.df.cluster_config.node_bind_address().port();
                        format!("{advertise_host}:{bind_port}")
                    } else {
                        // Fallback: use bind address directly (shouldn't happen in valid scheduler config)
                        self.df.cluster_config.node_bind_address().to_string()
                    };

                tracing::debug!(
                    "Registering federated task_history table with scheduler_id={scheduler_id}"
                );

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
                    self.scheduler_peers(),
                    self.df.cluster_config.client_tls_config().cloned(),
                    scheduler_id,
                );
                Arc::new(federated)
            } else {
                local_table
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
}
