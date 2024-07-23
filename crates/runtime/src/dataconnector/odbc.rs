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
use datafusion::sql::unparser::dialect::{
    CustomDialect, CustomDialectBuilder, DefaultDialect, Dialect, IntervalStyle, MySqlDialect,
    PostgreSqlDialect, SqliteDialect,
};
use db_connection_pool::dbconnection::odbcconn::ODBCDbConnectionPool;
use db_connection_pool::odbcpool::ODBCPool;
use snafu::prelude::*;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::{DataConnector, DataConnectorFactory, ParameterSpec, Parameters};

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

pub struct SQLDialectParam(String);
impl SQLDialectParam {
    #[must_use]
    pub fn new(val: &str) -> Self {
        Self(val.to_string())
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ODBCDriver {
    MySql,
    PostgreSql,
    Sqlite,
    Databricks,
    Unknown,
}

fn databricks_dialect() -> CustomDialect {
    CustomDialectBuilder::new()
        .with_identifier_quote_style('`')
        .with_interval_style(IntervalStyle::MySQL)
        .build()
}

impl From<&str> for ODBCDriver {
    fn from(val: &str) -> Self {
        match val {
            _ if val.contains("mysql") => ODBCDriver::MySql, // odbcinst.ini profile name
            _ if val.contains("libmyodbc") => ODBCDriver::MySql, // library filename
            _ if val.contains("postgres") => ODBCDriver::PostgreSql, // odbcinst.ini profile name
            _ if val.contains("psqlodbc") => ODBCDriver::PostgreSql, // library filename
            _ if val.contains("sqlite") => ODBCDriver::Sqlite, // profile and library name
            _ if val.contains("libsparkodbc") => ODBCDriver::Databricks, // library filename
            _ if val.contains("databricks") => ODBCDriver::Databricks, // profile name
            _ => {
                tracing::debug!("Unknown ODBC driver detected: {}", val);
                ODBCDriver::Unknown
            }
        }
    }
}

impl From<ODBCDriver> for Option<Arc<dyn Dialect + Send + Sync>> {
    fn from(val: ODBCDriver) -> Self {
        match val {
            ODBCDriver::MySql => Some(Arc::new(MySqlDialect {})),
            ODBCDriver::PostgreSql => Some(Arc::new(PostgreSqlDialect {})),
            ODBCDriver::Sqlite => Some(Arc::new(SqliteDialect {})),
            ODBCDriver::Databricks => Some(Arc::new(databricks_dialect())),
            ODBCDriver::Unknown => Some(Arc::new(DefaultDialect {})),
        }
    }
}

impl TryFrom<SQLDialectParam> for Option<Arc<dyn Dialect + Send + Sync>> {
    type Error = Error;

    fn try_from(val: SQLDialectParam) -> Result<Self> {
        match val.0.as_str() {
            "mysql" => Ok(Some(Arc::new(MySqlDialect {}))),
            "postgresql" => Ok(Some(Arc::new(PostgreSqlDialect {}))),
            "sqlite" => Ok(Some(Arc::new(SqliteDialect {}))),
            "databricks" => Ok(Some(Arc::new(databricks_dialect()))),
            _ => Err(Error::InvalidParameter {
                param: "odbc_sql_dialect".to_string(),
                msg: "Only 'mysql', 'postgresql', 'sqlite', and 'databricks' are supported"
                    .to_string(),
            }),
        }
    }
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

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::connector("connection_string").secret(),
    ParameterSpec::connector("max_binary_size"),
    ParameterSpec::connector("max_text_size"),
    ParameterSpec::connector("max_bytes_per_batch"),
    ParameterSpec::connector("max_num_rows_per_batch"),
    ParameterSpec::runtime("sql_dialect"),
];

impl DataConnectorFactory for ODBCFactory {
    fn create(
        &self,
        params: Parameters,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let dialect = if let Some(sql_dialect) = params.get("sql_dialect").expose().ok() {
                let sql_dialect = SQLDialectParam::new(sql_dialect);
                sql_dialect.try_into()
            } else {
                let driver = params
                    .get("connection_string")
                    .expose()
                    .ok_or_else(|p| MissingParameterSnafu { param: p.0 }.build())?
                    .to_lowercase();

                let driver = driver
                    .split(';')
                    .find(|s| s.starts_with("driver="))
                    .context(NoDriverSpecifiedSnafu)?;

                Ok(ODBCDriver::from(driver).into())
            }?;

            let pool: Arc<ODBCDbConnectionPool> = Arc::new(
                ODBCPool::new(params.to_secret_map())
                    .context(UnableToCreateODBCConnectionPoolSnafu)?,
            );

            let odbc_factory = ODBCTableFactory::new(pool, dialect);

            Ok(Arc::new(ODBC { odbc_factory }) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "odbc"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
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
