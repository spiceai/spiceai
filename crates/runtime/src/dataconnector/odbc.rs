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

use crate::component::dataset::Dataset;
use crate::parameters::Parameters;
use async_trait::async_trait;
use data_components::odbc::ODBCTableFactory;
use data_components::Read;
use datafusion::datasource::TableProvider;
use datafusion::sql::unparser::dialect::{
    CustomDialect, CustomDialectBuilder, DateFieldExtractStyle, DefaultDialect, Dialect,
    IntervalStyle, MySqlDialect, PostgreSqlDialect, SqliteDialect,
};
use db_connection_pool::dbconnection::odbcconn::ODBCDbConnectionPool;
use db_connection_pool::odbcpool::ODBCPool;
use snafu::prelude::*;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorFactory, ParameterSpec,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to setup the ODBC connection pool.\nVerify the ODBC connection details are valid, and try again.\n{source}"))]
    UnableToCreateODBCConnectionPool {
        source: db_connection_pool::odbcpool::Error,
    },
    #[snafu(display("Missing required parameter: {param}. Specify a value.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/odbc"))]
    MissingParameter { param: String },
    #[snafu(display("An ODBC parameter is configured incorrectly: {param}.\n{msg}\nFor details, visit: https://spiceai.org/docs/components/data-connectors/odbc"))]
    InvalidParameter { param: String, msg: String },
    #[snafu(display("No ODBC driver was specified in the connection string.\nSpecify an installed driver in the connection string.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/odbc"))]
    NoDriverSpecified,
    #[snafu(display("Accessing an ODBC driver with a file path is not permitted.\nInstall a driver using the system driver manager, and specify the driver name instead.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/odbc"))]
    DirectDriverNotPermitted,
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
enum ODBCProfile {
    MySql,
    PostgreSql,
    Sqlite,
    Databricks,
    Athena,
    Unknown,
}

fn databricks_dialect() -> CustomDialect {
    CustomDialectBuilder::new()
        .with_identifier_quote_style('`')
        .with_interval_style(IntervalStyle::MySQL)
        .build()
}

fn athena_dialect() -> CustomDialect {
    CustomDialectBuilder::new()
        .with_interval_style(IntervalStyle::MySQL)
        .with_date_field_extract_style(DateFieldExtractStyle::Extract)
        .build()
}

impl From<&str> for ODBCProfile {
    fn from(val: &str) -> Self {
        // match a profile name to a dialect
        match val {
            _ if val.contains("mysql") => ODBCProfile::MySql,
            _ if val.contains("postgres") => ODBCProfile::PostgreSql,
            _ if val.contains("sqlite") => ODBCProfile::Sqlite,
            _ if val.contains("databricks") => ODBCProfile::Databricks,
            _ if val.contains("athena") => ODBCProfile::Athena,
            _ => {
                tracing::debug!("Unknown ODBC profile detected: {}", val);
                ODBCProfile::Unknown
            }
        }
    }
}

impl From<ODBCProfile> for Option<Arc<dyn Dialect + Send + Sync>> {
    fn from(val: ODBCProfile) -> Self {
        match val {
            ODBCProfile::MySql => Some(Arc::new(MySqlDialect {})),
            ODBCProfile::PostgreSql => Some(Arc::new(PostgreSqlDialect {})),
            ODBCProfile::Sqlite => Some(Arc::new(SqliteDialect {})),
            ODBCProfile::Databricks => Some(Arc::new(databricks_dialect())),
            ODBCProfile::Athena => Some(Arc::new(athena_dialect())),
            ODBCProfile::Unknown => Some(Arc::new(DefaultDialect {})),
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
            "athena" => Ok(Some(Arc::new(athena_dialect()))),
            _ => Err(Error::InvalidParameter {
                param: "odbc_sql_dialect".to_string(),
                msg:
                    "Only values of 'mysql', 'postgresql', 'sqlite', 'athena', or 'databricks' are supported."
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

fn driver_is_file(driver: &str) -> bool {
    driver
        .split('=')
        .last()
        // if the file doesn't yet exist, the connector will fail registration
        // when the connector re-tries, if the file exists it will fail again
        .filter(|s| std::fs::metadata(s).is_ok())
        .is_some()
}

fn parameter_is_integer(parameters: &Parameters, param: &str) -> Result<()> {
    if let Some(value) = parameters.get(param).expose().ok() {
        let value = value
            .parse::<usize>()
            .map_err(|_| Error::InvalidParameter {
                param: param.to_string(),
                msg: "The parameter must be a positive integer.".to_string(),
            })?;

        if value == 0 {
            return Err(Error::InvalidParameter {
                param: param.to_string(),
                msg: "The parameter must be a positive integer.".to_string(),
            });
        }
    }

    Ok(())
}

impl DataConnectorFactory for ODBCFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            parameter_is_integer(&params.parameters, "max_binary_size")?;
            parameter_is_integer(&params.parameters, "max_text_size")?;
            parameter_is_integer(&params.parameters, "max_bytes_per_batch")?;
            parameter_is_integer(&params.parameters, "max_num_rows_per_batch")?;

            let dialect =
                if let Some(sql_dialect) = params.parameters.get("sql_dialect").expose().ok() {
                    let sql_dialect = SQLDialectParam::new(sql_dialect);
                    sql_dialect.try_into()
                } else {
                    let driver = params
                        .parameters
                        .get("connection_string")
                        .expose()
                        .ok_or_else(|p| MissingParameterSnafu { param: p.0 }.build())?
                        .to_lowercase();

                    let driver = driver
                        .split(';')
                        .find(|s| s.starts_with("driver="))
                        .context(NoDriverSpecifiedSnafu)?;

                    // explicitly check if the user has tried to specify a file path
                    if driver_is_file(driver) {
                        return Err(Error::DirectDriverNotPermitted {}.into());
                    }

                    Ok(ODBCProfile::from(driver).into())
                }?;

            let pool: Arc<ODBCDbConnectionPool> = Arc::new(
                ODBCPool::new(params.parameters.to_secret_map())
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
                    connector_component: ConnectorComponent::from(dataset),
                })?,
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_odbc_driver_is_file() {
        std::fs::File::create("something.so").expect("file should be created");
        std::fs::File::create("noextfile").expect("file should be created");

        assert!(!driver_is_file("driver=foo"));
        assert!(!driver_is_file("driver={mysql}"));
        assert!(!driver_is_file("driver={microsoft sql server}"));
        assert!(driver_is_file("driver=./something.so"));
        assert!(driver_is_file("driver=something.so"));
        assert!(driver_is_file("driver=noextfile"));
        assert!(driver_is_file("driver=./noextfile"));

        std::fs::remove_file("something.so").expect("file should be deleted");
        std::fs::remove_file("noextfile").expect("file should be deleted");
    }
}
