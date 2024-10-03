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
use data_components::Read;
use datafusion::datasource::TableProvider;
use datafusion_table_providers::mysql::MySQLTableFactory;
use datafusion_table_providers::sql::db_connection_pool::{
    dbconnection,
    mysqlpool::{self, MySQLConnectionPool},
    DbConnectionPool, Error as DbConnectionPoolError,
};
use mysql_async::prelude::ToValue;
use snafu::prelude::*;
use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::{DataConnector, DataConnectorError, DataConnectorFactory, ParameterSpec, Parameters};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create MySQL connection pool: {source}"))]
    UnableToCreateMySQLConnectionPool { source: DbConnectionPoolError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct MySQL {
    mysql_factory: MySQLTableFactory,
}

#[derive(Default, Copy, Clone)]
pub struct MySQLFactory {}

impl MySQLFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::connector("connection_string").secret(),
    ParameterSpec::connector("user").secret(),
    ParameterSpec::connector("pass").secret(),
    ParameterSpec::connector("host"),
    ParameterSpec::connector("tcp_port"),
    ParameterSpec::connector("db"),
    ParameterSpec::connector("sslmode"),
    ParameterSpec::connector("sslrootcert"),
];

impl DataConnectorFactory for MySQLFactory {
    fn create(
        &self,
        params: Parameters,
        _metadata: Option<HashMap<String, String>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let pool: Arc<
                dyn DbConnectionPool<mysql_async::Conn, &'static (dyn ToValue + Sync)>
                    + Send
                    + Sync,
            > = match MySQLConnectionPool::new(params.to_secret_map()).await {
                Ok(pool) => Arc::new(pool),
                Err(error) => match error {
                    mysqlpool::Error::InvalidUsernameOrPassword { .. } => {
                        return Err(
                            DataConnectorError::UnableToConnectInvalidUsernameOrPassword {
                                dataconnector: "mysql".to_string(),
                            }
                            .into(),
                        )
                    }

                    mysqlpool::Error::InvalidHostOrPortError {
                        source: _,
                        host,
                        port,
                    } => {
                        return Err(DataConnectorError::UnableToConnectInvalidHostOrPort {
                            dataconnector: "mysql".to_string(),
                            host,
                            port: format!("{port}"),
                        }
                        .into())
                    }

                    _ => {
                        return Err(DataConnectorError::UnableToConnectInternal {
                            dataconnector: "mysql".to_string(),
                            source: Box::new(error),
                        }
                        .into())
                    }
                },
            };
            let mysql_factory = MySQLTableFactory::new(pool);

            Ok(Arc::new(MySQL { mysql_factory }) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "mysql"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

#[async_trait]
impl DataConnector for MySQL {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        match Read::table_provider(&self.mysql_factory, dataset.path().into(), dataset.schema())
            .await
        {
            Ok(provider) => Ok(provider),
            Err(e) => {
                if let Some(err_source) = e.source() {
                    if let Some(dbconnection::Error::UndefinedTable {
                        table_name,
                        source: _,
                    }) = err_source.downcast_ref::<dbconnection::Error>()
                    {
                        return Err(DataConnectorError::InvalidTableName {
                            dataconnector: "mysql".to_string(),
                            dataset_name: dataset.name.to_string(),
                            table_name: table_name.clone(),
                        });
                    }
                }

                return Err(DataConnectorError::UnableToGetReadProvider {
                    dataconnector: "mysql".to_string(),
                    source: e,
                });
            }
        }
    }
}
