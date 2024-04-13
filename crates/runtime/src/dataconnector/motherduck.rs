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
use data_components::Read;
use datafusion::common::OwnedTableReference;
use datafusion::datasource::TableProvider;
use db_connection_pool::duckdb_legacy_pool::LegacyDuckDbConnectionPool;
use duckdb_0_9_2::ToSql;
use secrets::Secret;
use snafu::prelude::*;
use spicepod::component::dataset::Dataset;
use sql_provider_datafusion::SqlTable;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

use super::{DataConnector, DataConnectorFactory};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create DuckDB connection pool: {source}"))]
    UnableToCreateDuckDBConnectionPool { source: db_connection_pool::Error },

    #[snafu(display("{source}"))]
    UnableToGetReadProvider {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct LegacyDuckDBTableFactory {
    pool: Arc<LegacyDuckDbConnectionPool>,
}

impl LegacyDuckDBTableFactory {
    #[must_use]
    pub fn new(pool: Arc<LegacyDuckDbConnectionPool>) -> Self {
        Self { pool }
    }
}

type DynDuckDbConnectionPool = dyn db_connection_pool::DbConnectionPool<
        r2d2::PooledConnection<duckdb_0_9_2::DuckdbConnectionManager>,
        &'static dyn ToSql,
    > + Send
    + Sync;

#[async_trait]
impl Read for LegacyDuckDBTableFactory {
    async fn table_provider(
        &self,
        table_reference: OwnedTableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let pool = Arc::clone(&self.pool);
        let dyn_pool: Arc<DynDuckDbConnectionPool> = pool;
        let table_provider = SqlTable::new(&dyn_pool, table_reference)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        Ok(Arc::new(table_provider))
    }
}

pub struct Motherduck {
    duckdb_factory: LegacyDuckDBTableFactory,
}

impl DataConnectorFactory for Motherduck {
    fn create(
        _secret: Option<Secret>,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let pool = Arc::new(
                LegacyDuckDbConnectionPool::new_with_file_mode(&params)
                    .context(UnableToCreateDuckDBConnectionPoolSnafu)?,
            );

            let duckdb_factory = LegacyDuckDBTableFactory::new(pool);

            Ok(Arc::new(Self { duckdb_factory }) as Arc<dyn DataConnector>)
        })
    }
}

#[async_trait]
impl DataConnector for Motherduck {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::AnyErrorResult<Arc<dyn TableProvider>> {
        Ok(
            Read::table_provider(&self.duckdb_factory, dataset.path().into())
                .await
                .context(UnableToGetReadProviderSnafu)?,
        )
    }
}
