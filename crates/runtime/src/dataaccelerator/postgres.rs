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
    PostgresTableProviderFactory, write::PostgresTableWriter,
};
use runtime_table_partition::expression::PartitionedBy;
use snafu::prelude::*;
use std::{any::Any, sync::Arc};

use crate::{datafusion::udf::deny_spice_specific_functions, parameters::ParameterSpec};

use super::{AccelerationSource, DataAccelerator};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create table: {source}"))]
    UnableToCreateTable {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display(
        "Invalid connection pool configuration: connection_pool_min_idle ({connection_pool_min_idle}) cannot be greater than connection_pool_max ({connection_pool_max})"
    ))]
    InvalidConnectionPoolConfiguration {
        connection_pool_min_idle: usize,
        connection_pool_max: usize,
    },

    #[snafu(display(
        "Invalid value for parameter '{parameter}': '{value}'. Expected a positive integer."
    ))]
    InvalidParameterValue { parameter: String, value: String },
}

type Result<T, E = Error> = std::result::Result<T, E>;

const DEFAULT_CONNECTION_POOL_MIN: usize = 5;
const DEFAULT_CONNECTION_POOL_MAX: usize = 10;

pub struct PostgresAccelerator {
    postgres_factory: PostgresTableProviderFactory,
}

impl PostgresAccelerator {
    #[must_use]
    pub fn new() -> Self {
        Self {
            postgres_factory: PostgresTableProviderFactory::new()
                .with_function_support(deny_spice_specific_functions()),
        }
    }
}

impl Default for PostgresAccelerator {
    fn default() -> Self {
        Self::new()
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("host"),
    ParameterSpec::component("port"),
    ParameterSpec::component("db"),
    ParameterSpec::component("user").secret(),
    ParameterSpec::component("pass").secret(),
    ParameterSpec::component("sslmode"),
    ParameterSpec::component("sslrootcert"),
    ParameterSpec::component("connection_pool_min")
        .description("The minimum number of connections to keep open in the pool, lazily created when requested.")
        .default("5"),
    ParameterSpec::runtime("file_watcher"),
    ParameterSpec::runtime("connection_pool_size")
        .description("The maximum number of connections created in the connection pool.")
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
        mut cmd: CreateExternalTable,
        _source: Option<&dyn AccelerationSource>,
        partition_by: Vec<PartitionedBy>,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        ensure!(
            partition_by.is_empty(),
            super::InvalidConfigurationSnafu {
                msg: "Postgres data accelerator does not support the `partition_by` parameter but it was provided".to_string()
            }
        );

        let ctx = SessionContext::new();

        // Validate and normalize pool_min and connection_pool_size
        let connection_pool_min_idle = match cmd.options.get("connection_pool_min") {
            Some(s) => s
                .parse::<usize>()
                .map_err(|_| Error::InvalidParameterValue {
                    parameter: "connection_pool_min".to_string(),
                    value: s.clone(),
                })?,
            None => DEFAULT_CONNECTION_POOL_MIN,
        };
        let connection_pool_max = match cmd.options.get("connection_pool_size") {
            Some(s) => s
                .parse::<usize>()
                .map_err(|_| Error::InvalidParameterValue {
                    parameter: "connection_pool_size".to_string(),
                    value: s.clone(),
                })?,
            None => DEFAULT_CONNECTION_POOL_MAX,
        };

        if connection_pool_min_idle > connection_pool_max {
            return Err(Error::InvalidConnectionPoolConfiguration {
                connection_pool_min_idle,
                connection_pool_max,
            }
            .into());
        }

        cmd.options.insert(
            "application_name".to_string(),
            format!("Spice.ai {}", env!("CARGO_PKG_VERSION")),
        );

        let table_provider =
            TableProviderFactory::create(&self.postgres_factory, &ctx.state(), &cmd)
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

        let table_provider = Arc::new(PolyTableProvider::new(
            cloned_writer,
            postgres_writer,
            read_provider,
        ));

        Ok(table_provider)
    }

    fn prefix(&self) -> &'static str {
        "pg"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}
