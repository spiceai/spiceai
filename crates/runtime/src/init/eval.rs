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

use core::time::Duration;
use std::sync::Arc;

use datafusion::sql::TableReference;
use snafu::ResultExt;
use tokio::sync::RwLock;

use crate::{
    accelerated_table::{refresh::Refresh, Retention},
    component::dataset::{acceleration::Acceleration, TimeFormat},
    datafusion::SPICE_EVAL_SCHEMA,
    internal_table::create_internal_accelerated_table,
    model::{
        builtin_scorer, EVAL_RESULTS_TABLE_REFERENCE, EVAL_RESULTS_TABLE_SCHEMA,
        EVAL_RESULTS_TABLE_TIME_COLUMN, EVAL_RUNS_TABLE_PRIMARY_KEY, EVAL_RUNS_TABLE_REFERENCE,
        EVAL_RUNS_TABLE_SCHEMA, EVAL_RUNS_TABLE_TIME_COLUMN,
    },
    secrets::Secrets,
    Result, Runtime, UnableToCreateBackendSnafu, UnableToCreateEvalRunsTableSnafu,
};

impl Runtime {
    #[allow(clippy::implicit_hasher)]
    pub(crate) async fn load_eval_scorer(&self) {
        for (name, scorer) in builtin_scorer() {
            let mut reg = self.eval_scorers.write().await;
            reg.insert(name.to_string(), Arc::clone(&scorer));
            tracing::debug!("Successfully loaded eval scorer {name}");
        }
    }

    pub(crate) async fn load_eval_tables(&self) -> Result<()> {
        self.load_eval_run_table().await?;
        self.load_eval_results_table().await
    }

    pub(crate) async fn load_eval_results_table(&self) -> Result<()> {
        let retention = Retention::new(
            Some(EVAL_RESULTS_TABLE_TIME_COLUMN.to_string()),
            Some(TimeFormat::Timestamptz),
            None,
            None,
            Some(Duration::from_secs(24 * 3600)), // Keep data for last 24 hours
            Some(Duration::from_secs(1800)),      // Check every 30 minutes
            true,
        );

        let table = create_internal_accelerated_table(
            self.status(),
            TableReference::partial(SPICE_EVAL_SCHEMA, EVAL_RESULTS_TABLE_REFERENCE.table()), // Cannot parse Catalog.
            EVAL_RESULTS_TABLE_SCHEMA.clone(),
            None,
            Acceleration::default(),
            Refresh::default(),
            retention,
            Arc::new(RwLock::new(Secrets::default())),
        )
        .await
        .context(UnableToCreateEvalRunsTableSnafu)?;

        self.df
            .register_table_as_writable_and_with_schema(EVAL_RESULTS_TABLE_REFERENCE.clone(), table)
            .context(UnableToCreateBackendSnafu)?;

        Ok(())
    }

    pub(crate) async fn load_eval_run_table(&self) -> Result<()> {
        let retention = Retention::new(
            Some(EVAL_RUNS_TABLE_TIME_COLUMN.to_string()),
            Some(TimeFormat::Timestamptz),
            None,
            None,
            Some(Duration::from_secs(24 * 3600)), // Keep data for last 24 hours
            Some(Duration::from_secs(1800)),      // Check every 30 minutes
            true,
        );

        let table = create_internal_accelerated_table(
            self.status(),
            TableReference::partial(SPICE_EVAL_SCHEMA, EVAL_RUNS_TABLE_REFERENCE.table()), // Cannot parse Catalog.
            EVAL_RUNS_TABLE_SCHEMA.clone(),
            Some(vec![EVAL_RUNS_TABLE_PRIMARY_KEY.to_string()]),
            Acceleration::default(),
            Refresh::default(),
            retention,
            Arc::new(RwLock::new(Secrets::default())),
        )
        .await
        .context(UnableToCreateEvalRunsTableSnafu)?;

        self.df
            .register_table_as_writable_and_with_schema(EVAL_RUNS_TABLE_REFERENCE.clone(), table)
            .context(UnableToCreateBackendSnafu)?;

        Ok(())
    }
}
