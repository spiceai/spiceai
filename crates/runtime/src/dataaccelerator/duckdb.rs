/*
Copyright 2024 The Spice.ai OSS Authors

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

use async_trait::async_trait;
use data_components::delete::{get_deletion_provider, DeletionTableProviderAdapter};
use datafusion::{
    datasource::{provider::TableProviderFactory, TableProvider},
    execution::context::SessionContext,
    logical_expr::CreateExternalTable,
};
use datafusion_table_providers::duckdb::DuckDBTableProviderFactory;
use duckdb::AccessMode;
use snafu::prelude::*;
use std::{any::Any, sync::Arc};

use crate::component::dataset::Dataset;

use super::DataAccelerator;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create table: {source}"))]
    UnableToCreateTable {
        source: datafusion::error::DataFusionError,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct DuckDBAccelerator {
    duckdb_factory: DuckDBTableProviderFactory,
}

impl DuckDBAccelerator {
    #[must_use]
    pub fn new() -> Self {
        Self {
            // DuckDB accelerator uses params.duckdb_file for file connection
            duckdb_factory: DuckDBTableProviderFactory::new()
                .db_path_param("duckdb_file")
                .access_mode(AccessMode::ReadWrite),
        }
    }

    /// Returns the `DuckDB` file path that would be used for a file-based `DuckDB` accelerator from this dataset
    #[must_use]
    pub fn duckdb_file_path(&self, dataset: &Dataset) -> Option<String> {
        if !dataset.is_file_accelerated() {
            return None;
        }
        let acceleration = dataset.acceleration.as_ref()?;

        let mut params = acceleration.params.clone();

        Some(
            self.duckdb_factory
                .duckdb_file_path(&dataset.name.to_string(), &mut params),
        )
    }
}

impl Default for DuckDBAccelerator {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DataAccelerator for DuckDBAccelerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Creates a new table in the accelerator engine, returning a `TableProvider` that supports reading and writing.
    async fn create_external_table(
        &self,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        let ctx = SessionContext::new();
        let table_provider = TableProviderFactory::create(&self.duckdb_factory, &ctx.state(), cmd)
            .await
            .context(UnableToCreateTableSnafu)
            .boxed()?;

        let deletion_table_provider = get_deletion_provider(Arc::clone(&table_provider));

        match deletion_table_provider {
            Some(deletion_table_provider) => {
                let deletion_adapter = DeletionTableProviderAdapter::new(deletion_table_provider);
                Ok(Arc::new(deletion_adapter))
            }
            None => Ok(table_provider),
        }
    }
}
