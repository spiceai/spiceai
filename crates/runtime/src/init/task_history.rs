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
    Error, Result, Runtime, UnableToCreateBackendSnafu, datafusion::SPICE_RUNTIME_SCHEMA,
    task_history,
};
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

        match task_history::TaskSpan::instantiate_table(
            self.status(),
            retention_period_secs,
            retention_check_interval_secs,
            Arc::clone(&self),
        )
        .await
        {
            Ok(table) => self
                .df
                .register_table_as_writable_and_with_schema(
                    TableReference::partial(
                        SPICE_RUNTIME_SCHEMA,
                        task_history::DEFAULT_TASK_HISTORY_TABLE,
                    ),
                    table,
                )
                .context(UnableToCreateBackendSnafu),
            Err(source) => Err(Error::UnableToTrackTaskHistory { source }),
        }
    }
}
