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

use async_trait::async_trait;
use data_components::poly::PolyTableProvider;
use datafusion::{
    catalog::TableProviderFactory, datasource::TableProvider, execution::context::SessionContext,
    logical_expr::CreateExternalTable,
};
use datafusion_table_providers::postgres::{
    write::PostgresTableWriter, PostgresTableProviderFactory,
};
use snafu::prelude::*;
use std::{any::Any, sync::Arc};

use crate::{component::dataset::Dataset, parameters::ParameterSpec};

use super::DataAccelerator;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create table: {source}"))]
    UnableToCreateTable {
        source: datafusion::error::DataFusionError,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct PostgresAccelerator {
    postgres_factory: PostgresTableProviderFactory,
}

impl PostgresAccelerator {
    #[must_use]
    pub fn new() -> Self {
        Self {
            postgres_factory: PostgresTableProviderFactory::new(),
        }
    }
}

impl Default for PostgresAccelerator {
    fn default() -> Self {
        Self::new()
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::accelerator("host"),
    ParameterSpec::accelerator("port"),
    ParameterSpec::accelerator("db"),
    ParameterSpec::accelerator("user").secret(),
    ParameterSpec::accelerator("pass").secret(),
    ParameterSpec::accelerator("sslmode"),
    ParameterSpec::accelerator("sslrootcert"),
    ParameterSpec::runtime("file_watcher"),
    ParameterSpec::runtime("connection_pool_size")
        .description("The maximum number of connections created in the connection pool")
        .default("10"),
];

#[async_trait]
impl DataAccelerator for PostgresAccelerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "postgres"
    }

    /// Creates a new table in the accelerator engine, returning a `TableProvider` that supports reading and writing.
    async fn create_external_table(
        &self,
        cmd: &CreateExternalTable,
        _dataset: Option<&Dataset>,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        let ctx = SessionContext::new();
        let table_provider =
            TableProviderFactory::create(&self.postgres_factory, &ctx.state(), cmd)
                .await
                .context(UnableToCreateTableSnafu)
                .boxed()?;

        let Some(postgres_writer) = table_provider
            .as_any()
            .downcast_ref::<PostgresTableWriter>()
        else {
            unreachable!("PostgresTableWriter should be returned from PostgresTableProviderFactory")
        };

        let read_provider = Arc::clone(&postgres_writer.read_provider);
        let postgres_writer = Arc::new(postgres_writer.clone());
        let cloned_writer = Arc::clone(&postgres_writer);

        Ok(Arc::new(PolyTableProvider::new(
            cloned_writer,
            postgres_writer,
            read_provider,
        )))
    }

    fn prefix(&self) -> &'static str {
        "pg"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}
