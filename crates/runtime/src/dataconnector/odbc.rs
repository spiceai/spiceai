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
use crate::secrets::{Secret, SecretMap};
use async_trait::async_trait;
use data_components::odbc::ODBCTableFactory;
use data_components::Read;
use datafusion::datasource::TableProvider;
use datafusion::sql::unparser::dialect::{Dialect, MySqlDialect, PostgreSqlDialect, SqliteDialect};
use datafusion_table_providers::sql::db_connection_pool::dbconnection::GenericError;
use db_connection_pool::dbconnection::odbcconn::ODBCDbConnectionPool;
use db_connection_pool::odbcpool::ODBCPool;
use secrecy::ExposeSecret;
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
    #[snafu(display("A required ODBC parameter is missing: {param}"))]
    MissingParameter { param: String },
    #[snafu(display("An ODBC parameter is configured incorrectly: {param}. {msg}"))]
    InvalidParameter { param: String, msg: String },
    #[snafu(display("No ODBC driver was specified in the connection string"))]
    NoDriverSpecified,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct ODBC<'a>
where
    'a: 'static,
{
    odbc_factory: ODBCTableFactory<'a>,
}

const SUPPORTED_DIALECTS: [&str; 3] = ["mysql", "postgresql", "sqlite"];
fn match_driver_to_dialect(driver: &str) -> Option<Arc<dyn Dialect + Send + Sync>> {
    match driver {
        _ if driver.contains("mysql") => Some(Arc::new(MySqlDialect {})), // odbcinst.ini profile name
        _ if driver.contains("libmyodbc") => Some(Arc::new(MySqlDialect {})), // library filename
        _ if driver.contains("postgres") => Some(Arc::new(PostgreSqlDialect {})), // odbcinst.ini profile name
        _ if driver.contains("psqlodbc") => Some(Arc::new(PostgreSqlDialect {})), // library filename
        _ if driver.contains("sqlite") => Some(Arc::new(SqliteDialect {})), // profile and library name
        _ => {
            tracing::debug!("No dialect detected for driver: {}", driver);
            None
        }
    }
}

impl<'a> DataConnectorFactory for ODBC<'a>
where
    'a: 'static,
{
    fn create(
        secret: Option<Secret>,
        params: Arc<HashMap<String, String>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        let mut params: SecretMap = params.as_ref().into();
        if let Some(secret) = secret {
            secret.insert_to_params(
                &mut params,
                "odbc_connection_string_key",
                "odbc_connection_string",
            );
        }

        Box::pin(async move {
            let driver = if let Some(sql_dialect) = params.get("sql_dialect") {
                if SUPPORTED_DIALECTS.contains(&sql_dialect.expose_secret().as_str()) {
                    Ok::<_, GenericError>(sql_dialect.expose_secret().clone())
                } else {
                    Err(Error::InvalidParameter {
                        param: "sql_dialect".to_string(),
                        msg: "Only 'mysql', 'postgresql', and 'sqlite' are supported".to_string(),
                    }
                    .into())
                }
            } else {
                let driver = params
                    .get("odbc_connection_string")
                    .context(MissingParameterSnafu {
                        param: "odbc_connection_string".to_string(),
                    })?
                    .expose_secret()
                    .to_lowercase();

                let driver = driver
                    .split(';')
                    .find(|s| s.starts_with("driver="))
                    .context(NoDriverSpecifiedSnafu)?;

                Ok(driver.to_string())
            }?;

            let dialect = match_driver_to_dialect(&driver);

            let pool: Arc<ODBCDbConnectionPool<'a>> = Arc::new(
                ODBCPool::new(Arc::new(params.into_map()))
                    .context(UnableToCreateODBCConnectionPoolSnafu)?,
            );

            let odbc_factory = ODBCTableFactory::new(pool, dialect);

            Ok(Arc::new(Self { odbc_factory }) as Arc<dyn DataConnector>)
        })
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
