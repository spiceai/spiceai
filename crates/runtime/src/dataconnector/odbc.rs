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

use crate::component::dataset::Dataset;
use async_trait::async_trait;
use data_components::odbc::ODBCTableFactory;
use data_components::Read;
use datafusion::datasource::TableProvider;
use db_connection_pool::dbconnection::odbcconn::ODBCDbConnectionPool;
use db_connection_pool::odbcpool::ODBCPool;
use secrecy::SecretString;
use snafu::prelude::*;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

use super::{DataConnector, DataConnectorFactory};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create ODBC connection pool: {source}"))]
    UnableToCreateODBCConnectionPool {
        source: db_connection_pool::odbcpool::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct ODBC<'a>
where
    'a: 'static,
{
    odbc_factory: ODBCTableFactory<'a>,
}

#[derive(Default, Copy, Clone)]
pub struct ODBCFactory {}

impl ODBCFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

impl DataConnectorFactory for ODBCFactory {
    fn create(
        &self,
        params: HashMap<String, SecretString>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let pool: Arc<ODBCDbConnectionPool> =
                Arc::new(ODBCPool::new(params).context(UnableToCreateODBCConnectionPoolSnafu)?);

            let odbc_factory = ODBCTableFactory::new(pool);

            Ok(Arc::new(ODBC { odbc_factory }) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "odbc"
    }

    fn autoload_secrets(&self) -> &'static [&'static str] {
        &["connection_string"]
    }
}

#[async_trait]
impl<'a> DataConnector for ODBC<'a>
where
    'a: 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        Ok(
            Read::table_provider(&self.odbc_factory, dataset.path().into(), dataset.schema())
                .await
                .context(super::UnableToGetReadProviderSnafu {
                    dataconnector: "odbc",
                })?,
        )
    }
}
