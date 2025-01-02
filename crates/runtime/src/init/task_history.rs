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
    datafusion::SPICE_RUNTIME_SCHEMA, task_history, Error, Result, Runtime,
    UnableToCreateBackendSnafu,
};
use datafusion::sql::TableReference;
use snafu::prelude::*;

impl Runtime {
    pub async fn init_task_history(&self) -> Result<()> {
        let app = self.app.read().await;

        if let Some(app) = app.as_ref() {
            if !app.runtime.task_history.enabled {
                tracing::debug!("Task history is disabled via configuration.");
                return Ok(());
            }
        }

        let (retention_period_secs, retention_check_interval_secs) = match app.as_ref() {
            Some(app) => (
                app.runtime
                    .task_history
                    .retention_period_as_secs()
                    .map_err(|e| Error::UnableToTrackTaskHistory {
                        source: task_history::Error::InvalidConfiguration { source: e }, // keeping the spicepod detached but still want to return snafu errors
                    })?,
                app.runtime
                    .task_history
                    .retention_check_interval_as_secs()
                    .map_err(|e| Error::UnableToTrackTaskHistory {
                        source: task_history::Error::InvalidConfiguration { source: e },
                    })?,
            ),
            None => (
                task_history::DEFAULT_TASK_HISTORY_RETENTION_PERIOD_SECS,
                task_history::DEFAULT_TASK_HISTORY_RETENTION_CHECK_INTERVAL_SECS,
            ),
        };

        match task_history::TaskSpan::instantiate_table(
            self.status(),
            retention_period_secs,
            retention_check_interval_secs,
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
