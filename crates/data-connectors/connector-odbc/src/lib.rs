/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

// odbcpool/odbcconn are exported (as they were in db_connection_pool) so clippy's
// avoid-breaking-exported-api treats their public API items as exported; the
// missing_errors_doc relaxation mirrors data_components/connector-git for moved code.
#![allow(clippy::missing_errors_doc)]

pub mod odbc;
pub mod odbcconn;
pub mod odbcpool;

use crate::odbc::ODBCTableFactory;
use crate::odbcconn::{ODBCDbConnectionPool, SqlCheck};
use crate::odbcpool::ODBCPool;
use async_trait::async_trait;
use data_components::Read;
use data_connector_api::ConnectorContext;
use data_connector_api::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorFactory, DataConnectorResult,
    NewDataConnectorResult,
};
use datafusion::datasource::TableProvider;
use datafusion::sql::unparser::dialect::{
    CustomDialect, CustomDialectBuilder, DateFieldExtractStyle, DefaultDialect, Dialect,
    IntervalStyle, MySqlDialect, PostgreSqlDialect, SqliteDialect,
};
use datafusion_table_providers::util::supported_functions::FunctionSupport;
use runtime_component::dataset::DatasetSpec;
use runtime_datafusion::function_support::expression_support_for_engine;
use runtime_parameters::{ParameterSpec, Parameters};
use runtime_udfs_api::deny_spice_specific_functions;
use snafu::prelude::*;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use util::sql_string_literal::{BackslashEscapedStrings, QuoteEscape, refuse_variable_reference};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to setup the ODBC connection pool. Verify the ODBC connection details are valid, and try again. {source}"
    ))]
    UnableToCreateODBCConnectionPool { source: crate::odbcpool::Error },
    #[snafu(display(
        "Missing required parameter: {param}. Specify a value. For details, visit: https://spiceai.org/docs/components/data-connectors/odbc"
    ))]
    MissingParameter { param: String },
    #[snafu(display(
        "An ODBC parameter is configured incorrectly: {param}. {msg} For details, visit: https://spiceai.org/docs/components/data-connectors/odbc"
    ))]
    InvalidParameter { param: String, msg: String },
    #[snafu(display(
        "No ODBC driver was specified in the connection string. Specify an installed driver in the connection string. For details, visit: https://spiceai.org/docs/components/data-connectors/odbc"
    ))]
    NoDriverSpecified,
    #[snafu(display(
        "Accessing an ODBC driver with a file path is not permitted. Install a driver using the system driver manager, and specify the driver name instead. For details, visit: https://spiceai.org/docs/components/data-connectors/odbc"
    ))]
    DirectDriverNotPermitted,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct ODBC<'a>
where
    'a: 'static,
{
    odbc_factory: ODBCTableFactory<'a>,
}

impl std::fmt::Debug for ODBC<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ODBC").finish_non_exhaustive()
    }
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

fn databricks_dialect() -> BackslashEscapedStrings<CustomDialect> {
    let dialect = CustomDialectBuilder::new()
        .with_identifier_quote_style('`')
        .with_interval_style(IntervalStyle::MySQL)
        .with_utf8_cast_dtype(datafusion::sql::sqlparser::ast::DataType::String(None))
        .with_large_utf8_cast_dtype(datafusion::sql::sqlparser::ast::DataType::String(None))
        .build();
    BackslashEscapedStrings::new(dialect, QuoteEscape::Unicode)
}

/// Athena (Trino) SQL. Identifiers are double-quoted: the statement names
/// computed outputs by their logical name (`sum(t.a) * Float64(0.0001)`), which
/// Athena rejects unquoted.
fn athena_dialect() -> CustomDialect {
    CustomDialectBuilder::new()
        .with_identifier_quote_style('"')
        .with_interval_style(IntervalStyle::MySQL)
        .with_date_field_extract_style(DateFieldExtractStyle::Extract)
        .with_large_utf8_cast_dtype(datafusion::sql::sqlparser::ast::DataType::Varchar(None))
        .build()
}

impl From<&str> for ODBCProfile {
    fn from(val: &str) -> Self {
        // match a profile name to a dialect
        match val {
            _ if val.contains("mysql") || val.contains("mariadb") => ODBCProfile::MySql,
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

impl ODBCProfile {
    /// The engine behind the profile, by the name
    /// [`expression_support_for_engine`] keys on; `None` where the profile
    /// names no engine Spice has a per-expression gate for.
    fn engine(&self) -> Option<&'static str> {
        match self {
            ODBCProfile::MySql => Some("mysql"),
            ODBCProfile::PostgreSql => Some("postgresql"),
            ODBCProfile::Sqlite => Some("sqlite"),
            ODBCProfile::Athena => Some("athena"),
            ODBCProfile::Databricks | ODBCProfile::Unknown => None,
        }
    }

    /// The check a connection runs on SQL text before it sends the text to
    /// the engine behind the profile.
    fn sql_check(&self) -> Option<SqlCheck> {
        match self {
            ODBCProfile::Databricks => Some(refuse_variable_reference),
            _ => None,
        }
    }
}

/// The federation function-support policy for an ODBC connection to `engine`:
/// the Spice function deny-list, so Spice-only UDFs are evaluated locally
/// instead of pushed into SQL the driver rejects (#10703), plus the engine's
/// per-expression gate where one exists, so a cast the engine evaluates
/// differently from `DataFusion` stays local on this route as on the engine's
/// own connector (issue #14482).
fn function_support_for_engine(engine: Option<&str>) -> FunctionSupport {
    let support = deny_spice_specific_functions().as_ref().clone();
    match engine.and_then(expression_support_for_engine) {
        Some(gate) => runtime_udfs_api::and_expression_support(support, gate),
        None => support,
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
    ParameterSpec::component("connection_string").secret(),
    ParameterSpec::component("max_binary_size"),
    ParameterSpec::component("max_text_size"),
    ParameterSpec::component("max_bytes_per_batch"),
    ParameterSpec::component("max_num_rows_per_batch"),
    ParameterSpec::runtime("sql_dialect"),
];

/// The `key=value` attributes of an ODBC connection string, read the way the
/// driver manager reads them: `;`-separated, keys trimmed and compared without
/// case, and a value in `{…}` that may contain `;` and spells `}` as `}}`.
fn connection_string_attributes(connection_string: &str) -> Vec<(String, String)> {
    let mut attributes = Vec::new();
    let mut rest = connection_string;
    while !rest.is_empty() {
        let Some((key, after_key)) = rest.split_once('=') else {
            break;
        };
        let key = key
            .trim()
            .trim_start_matches(';')
            .trim()
            .to_ascii_lowercase();
        let after_key = after_key.trim_start();
        let (value, after_value) = if let Some(braced) = after_key.strip_prefix('{') {
            let mut value = String::new();
            let mut chars = braced.char_indices();
            let mut end = braced.len();
            while let Some((index, c)) = chars.next() {
                if c == '}' {
                    if braced[index + 1..].starts_with('}') {
                        value.push('}');
                        chars.next();
                        continue;
                    }
                    end = index + 1;
                    break;
                }
                value.push(c);
            }
            let after = braced[end..].split_once(';').map_or("", |(_, after)| after);
            (value, after)
        } else {
            match after_key.split_once(';') {
                Some((value, after)) => (value.trim().to_string(), after),
                None => (after_key.trim().to_string(), ""),
            }
        };
        if !key.is_empty() {
            attributes.push((key, value));
        }
        rest = after_value;
    }
    attributes
}

/// Whether a connection string makes the driver manager load a library or
/// read a DSN file named by path, rather than a driver installed through the
/// driver manager: a `Driver` given as a path, or any `FILEDSN` or `SAVEFILE`.
fn names_a_file(connection_string: &str) -> bool {
    connection_string_attributes(connection_string)
        .iter()
        .any(|(key, value)| match key.as_str() {
            "driver" => driver_is_file(value),
            "filedsn" | "savefile" => true,
            _ => false,
        })
}

/// Whether a `Driver` value names a library file: a path with a directory
/// separator or a shared-library extension, or a file that exists.
fn driver_is_file(driver: &str) -> bool {
    let driver = driver.trim();
    let lowercase = driver.to_ascii_lowercase();
    !driver.is_empty()
        && (driver.contains('/')
            || driver.contains('\\')
            || [".so", ".dylib", ".dll"]
                .iter()
                .any(|extension| lowercase.ends_with(extension))
            || lowercase.contains(".so.")
            || std::fs::metadata(driver).is_ok_and(|metadata| metadata.is_file()))
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

impl std::fmt::Debug for ODBCFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ODBCFactory").finish()
    }
}

impl DataConnectorFactory for ODBCFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create<'a>(
        &'a self,
        params: ConnectorParams,
        _context: &'a dyn ConnectorContext,
    ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send + 'a>> {
        Box::pin(async move {
            parameter_is_integer(&params.parameters, "max_binary_size")?;
            parameter_is_integer(&params.parameters, "max_text_size")?;
            parameter_is_integer(&params.parameters, "max_bytes_per_batch")?;
            parameter_is_integer(&params.parameters, "max_num_rows_per_batch")?;

            // Explicitly check if the user has tried to specify a file path,
            // whatever else is configured: a driver given as a path, or a DSN
            // file, makes the driver manager load native code from it.
            if let Some(connection_string) =
                params.parameters.get("connection_string").expose().ok()
                && names_a_file(connection_string)
            {
                return Err(Error::DirectDriverNotPermitted {}.into());
            }

            let sql_check;
            let (dialect, engine): (
                Result<Option<Arc<dyn Dialect + Send + Sync>>>,
                Option<String>,
            ) = if let Some(sql_dialect) = params.parameters.get("sql_dialect").expose().ok() {
                let engine = sql_dialect.to_string();
                sql_check = ODBCProfile::from(sql_dialect).sql_check();
                let sql_dialect = SQLDialectParam::new(sql_dialect);
                (sql_dialect.try_into(), Some(engine))
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

                let profile = ODBCProfile::from(driver);
                let engine = profile.engine().map(str::to_string);
                sql_check = profile.sql_check();
                (Ok(profile.into()), engine)
            };
            let dialect = dialect?;

            let pool: Arc<ODBCDbConnectionPool> = Arc::new(
                ODBCPool::new(params.parameters.to_secret_map())
                    .context(UnableToCreateODBCConnectionPoolSnafu)?
                    .with_sql_check(sql_check),
            );

            let odbc_factory = ODBCTableFactory::new(pool, dialect)
                .with_function_support(function_support_for_engine(engine.as_deref()));

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

/// The name used to identify this connector in configuration.
pub const CONNECTOR_NAME: &str = "odbc";

/// Returns a new instance of the `ODBC` connector factory.
#[must_use]
pub fn factory() -> Arc<dyn DataConnectorFactory> {
    ODBCFactory::new_arc()
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
        _context: &dyn ConnectorContext,
        dataset: &DatasetSpec,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        Ok(
            Read::table_provider(&self.odbc_factory, dataset.path().into())
                .await
                .map_err(|source| {
                    data_connector_api::DataConnectorError::UnableToGetReadProvider {
                        dataconnector: "odbc".to_string(),
                        connector_component: ConnectorComponent::from(dataset),
                        source,
                    }
                })?,
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::common::Result;
    use datafusion::logical_expr::cast;
    use datafusion::prelude::col;
    use datafusion::sql::unparser::Unparser;

    #[test]
    fn databricks_string_literals_read_back_as_their_value() {
        use util::sql_string_literal::test_support::{
            UNICODE_QUOTE_LITERALS, assert_string_literals, read_spark,
        };

        let dialect = databricks_dialect();
        let unparser = Unparser::new(&dialect);
        assert_string_literals(
            |expr| unparser.expr_to_sql(expr).expect("unparse").to_string(),
            "`c`",
            UNICODE_QUOTE_LITERALS,
            read_spark,
        );
    }

    /// A Databricks connection unparses a plan with this dialect and checks
    /// the statement before it sends it, whether `sql_dialect` or the driver
    /// name selects the profile.
    #[test]
    fn databricks_statements_holding_a_variable_reference_are_refused() {
        use datafusion::logical_expr::LogicalPlan;
        use util::sql_string_literal::test_support::{
            assert_no_variable_reference_sent, variable_reference_plans,
        };

        let dialect = databricks_dialect();
        for selected_by in ["databricks", "driver={databricks odbc driver}"] {
            let check = ODBCProfile::from(selected_by)
                .sql_check()
                .expect("a Databricks connection checks SQL text");
            let send = |plan: &LogicalPlan| -> Result<String> {
                let sql = Unparser::new(&dialect).plan_to_sql(plan)?.to_string();
                check(&sql)?;
                Ok(sql)
            };
            let (refused, literal) = variable_reference_plans();
            for plan in &refused {
                assert_no_variable_reference_sent(send(plan));
            }
            send(&literal).expect("a string literal that holds ${ is sent");
        }
    }

    #[test]
    fn test_athena_dialect_overrides() -> Result<()> {
        let dialect = athena_dialect();
        let unparser = Unparser::new(&dialect);

        // large_utf8_cast_dtype: VARCHAR instead of TEXT
        let expr = cast(col("a"), DataType::LargeUtf8);
        let actual = format!("{}", unparser.expr_to_sql(&expr)?);
        assert_eq!(actual, r#"CAST("a" AS VARCHAR)"#);

        // utf8_cast_dtype: VARCHAR
        let expr = cast(col("a"), DataType::Utf8);
        let actual = format!("{}", unparser.expr_to_sql(&expr)?);
        assert_eq!(actual, r#"CAST("a" AS VARCHAR)"#);

        Ok(())
    }

    /// Regression test for #14482 on the ODBC route: the MySQL, PostgreSQL and
    /// Athena profiles reach engines that round a fractional-to-integer cast
    /// `DataFusion` truncates, so their policy must keep the cast local. The
    /// `SQLite` profile keeps it local too, under the `SQLite` gate: `SQLite`
    /// saturates a float past `i64` where `DataFusion` refuses it. A profile
    /// with no engine keeps the plain policy and the pushdown.
    #[test]
    fn a_fractional_to_integer_cast_stays_local_on_the_odbc_profiles_that_round() {
        use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
        use datafusion::prelude::lit;
        use datafusion_table_providers::util::supported_functions::contains_unsupported_functions;

        fn plan_projecting(expr: datafusion::prelude::Expr) -> LogicalPlan {
            LogicalPlanBuilder::values(vec![vec![lit(1_i64)]])
                .expect("values")
                .project(vec![expr])
                .expect("project")
                .build()
                .expect("build plan")
        }
        let rounding = cast(lit(1.5_f64), DataType::Int64);
        for profile in [
            ODBCProfile::MySql,
            ODBCProfile::PostgreSql,
            ODBCProfile::Athena,
            ODBCProfile::Sqlite,
        ] {
            let support = function_support_for_engine(profile.engine());
            assert!(
                contains_unsupported_functions(&plan_projecting(rounding.clone()), &support)
                    .expect("the support check must not error"),
                "the {profile:?} profile does not evaluate {rounding} as DataFusion does, so its policy must keep it local"
            );
        }
        for profile in [ODBCProfile::Databricks, ODBCProfile::Unknown] {
            let support = function_support_for_engine(profile.engine());
            assert!(
                !contains_unsupported_functions(&plan_projecting(rounding.clone()), &support)
                    .expect("the support check must not error"),
                "the {profile:?} profile evaluates {rounding} as DataFusion does, so the pushdown is kept"
            );
        }
        assert_eq!(
            SQLDialectParam::new("postgresql").0,
            "postgresql",
            "the sql_dialect parameter is the engine name the gate keys on"
        );
    }

    /// A `MariaDB` driver (`MariaDB`, `MariaDB Unicode`) gets the `MySQL`
    /// profile, so a string with a backslash, which `MariaDB` reads as an
    /// escape, stays local.
    #[test]
    fn a_mariadb_driver_gets_the_mysql_profile() {
        use datafusion::logical_expr::LogicalPlanBuilder;
        use datafusion::prelude::lit;
        use datafusion_table_providers::util::supported_functions::contains_unsupported_functions;

        for driver in [
            "driver={mariadb}",
            "driver={mariadb unicode}",
            "driver=mysql",
        ] {
            let profile = ODBCProfile::from(driver);
            assert!(
                matches!(profile, ODBCProfile::MySql),
                "{driver}: {profile:?}"
            );
            let plan = LogicalPlanBuilder::values(vec![vec![lit("a\\'b")]])
                .expect("values")
                .build()
                .expect("build plan");
            assert!(
                contains_unsupported_functions(
                    &plan,
                    &function_support_for_engine(profile.engine())
                )
                .expect("the support check must not error"),
                "{driver}: a string with a backslash must stay local"
            );
        }
    }

    /// Regression test for #14753 and #14754 on the ODBC route: `SQLite` has
    /// no date, time or interval types, so the `SQLite` profile keeps a
    /// timestamp literal, a date literal and an interval local, while a plain
    /// literal still pushes down.
    #[test]
    fn a_temporal_value_stays_local_on_the_odbc_sqlite_profile() {
        use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
        use datafusion::prelude::lit;
        use datafusion::scalar::ScalarValue;
        use datafusion_table_providers::util::supported_functions::contains_unsupported_functions;

        fn plan_projecting(expr: datafusion::prelude::Expr) -> LogicalPlan {
            LogicalPlanBuilder::values(vec![vec![lit(1_i64)]])
                .expect("values")
                .project(vec![expr])
                .expect("project")
                .build()
                .expect("build plan")
        }
        let sqlite = function_support_for_engine(ODBCProfile::Sqlite.engine());
        for temporal in [
            cast(
                lit("2026-01-30 23:00:00"),
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, None),
            ),
            cast(lit("2026-01-31"), DataType::Date32),
            lit(ScalarValue::new_interval_mdn(0, 0, 3_600_000_000_000)),
        ] {
            assert!(
                contains_unsupported_functions(&plan_projecting(temporal.clone()), &sqlite)
                    .expect("the support check must not error"),
                "the Sqlite profile must keep {temporal} local"
            );
        }
        assert!(
            !contains_unsupported_functions(&plan_projecting(lit(1_i64)), &sqlite)
                .expect("the support check must not error"),
            "the Sqlite profile must still push down a plain literal"
        );
    }

    #[test]
    fn test_odbc_driver_is_file() {
        std::fs::File::create("noextfile").expect("file should be created");

        for named_driver in [
            "Driver=foo",
            "driver={mysql}",
            "Driver={Microsoft SQL Server}",
            "DRIVER={PostgreSQL Unicode};Server=localhost;Database=x",
            "Driver={Amazon Athena ODBC Driver};\nCatalog=AwsDataCatalog;",
            "DSN=sales;UID=me",
        ] {
            assert!(!names_a_file(named_driver), "{named_driver}");
        }
        for path in [
            "driver=./something.so",
            "driver=something.so",
            "driver=noextfile",
            "driver=./noextfile",
            // Case is kept: a path with capitals is checked as written.
            "Driver=/opt/Drivers/LibEvil.dylib;",
            // Braces are the driver manager's quoting, not part of the path.
            "Driver={/opt/drivers/libevil.so};",
            "Driver = {C:\\drivers\\evil.dll};",
            "Server=x; DRIVER=/opt/libevil.so.1",
            // A DSN file names its driver, and SAVEFILE writes one.
            "FILEDSN=/tmp/x.dsn;",
            "Driver={PostgreSQL};SaveFile=/tmp/out.dsn",
        ] {
            assert!(names_a_file(path), "{path}");
        }

        std::fs::remove_file("noextfile").expect("file should be deleted");
    }

    #[test]
    fn connection_string_attributes_follow_odbc_quoting() {
        assert_eq!(
            connection_string_attributes("Driver={a;b}}c}; Server = x ;PWD={p;w}"),
            vec![
                ("driver".to_string(), "a;b}c".to_string()),
                ("server".to_string(), "x".to_string()),
                ("pwd".to_string(), "p;w".to_string()),
            ]
        );
    }

    /// Regression test for #16082: TPC-H Q11 and Q17, planned by the runtime's
    /// own session, push a scalar subquery whose output carries its logical
    /// name (`Float64(0.2) * avg(lineitem.l_quantity)`). Athena rejected that
    /// name unquoted with `mismatched input '('`, so every identifier the
    /// Athena dialect writes is double-quoted.
    #[test]
    fn athena_sql_for_a_scalar_subquery_comparison_quotes_its_output_name() {
        use async_trait::async_trait;
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::common::TableReference;
        use datafusion_table_providers::sql::db_connection_pool::{
            DbConnectionPool, JoinPushDown, dbconnection::DbConnection,
        };
        use datafusion_table_providers::sql::sql_provider_datafusion::SqlTable;
        use std::any::Any;

        struct PlanningOnlyConn;
        impl DbConnection<(), &'static dyn ToString> for PlanningOnlyConn {
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn as_any_mut(&mut self) -> &mut dyn Any {
                self
            }
        }

        /// One Athena workgroup: every table shares the compute context, so a
        /// join federates as one statement, as through `ODBCDbConnectionPool`.
        struct AthenaPool;
        #[async_trait]
        impl DbConnectionPool<(), &'static dyn ToString> for AthenaPool {
            async fn connect(
                &self,
            ) -> std::result::Result<
                Box<dyn DbConnection<(), &'static dyn ToString>>,
                Box<dyn std::error::Error + Send + Sync>,
            > {
                Ok(Box::new(PlanningOnlyConn))
            }
            fn join_push_down(&self) -> JoinPushDown {
                JoinPushDown::AllowedFor("athena".to_string())
            }
        }

        const Q11: &str = "SELECT ps_partkey, sum(ps_supplycost * ps_availqty) AS value \
            FROM partsupp, supplier, nation \
            WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY' \
            GROUP BY ps_partkey HAVING sum(ps_supplycost * ps_availqty) > ( \
              SELECT sum(ps_supplycost * ps_availqty) * 0.0001 FROM partsupp, supplier, nation \
              WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY') \
            ORDER BY value DESC";
        const Q17: &str = "SELECT sum(l_extendedprice) / 7.0 AS avg_yearly FROM lineitem, part \
            WHERE p_partkey = l_partkey AND p_brand = 'Brand#23' AND p_container = 'MED BOX' \
            AND l_quantity < (SELECT 0.2 * avg(l_quantity) FROM lineitem WHERE l_partkey = p_partkey)";

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        let df = runtime::datafusion::builder::DataFusionBuilder::new(
            runtime::status::RuntimeStatus::new(),
            Arc::new(data_accelerator_api::AcceleratorEngineRegistry::default()),
            rt.handle().clone(),
        )
        .build();

        let int = || DataType::Int64;
        let money = || DataType::Decimal128(15, 2);
        let pool: Arc<dyn DbConnectionPool<(), &'static dyn ToString> + Send + Sync> =
            Arc::new(AthenaPool);
        for (name, columns) in [
            (
                "partsupp",
                vec![
                    ("ps_partkey", int()),
                    ("ps_suppkey", int()),
                    ("ps_availqty", int()),
                    ("ps_supplycost", money()),
                ],
            ),
            (
                "supplier",
                vec![("s_suppkey", int()), ("s_nationkey", int())],
            ),
            (
                "nation",
                vec![("n_nationkey", int()), ("n_name", DataType::Utf8)],
            ),
            (
                "lineitem",
                vec![
                    ("l_partkey", int()),
                    ("l_quantity", money()),
                    ("l_extendedprice", money()),
                ],
            ),
            (
                "part",
                vec![
                    ("p_partkey", int()),
                    ("p_brand", DataType::Utf8),
                    ("p_container", DataType::Utf8),
                ],
            ),
        ] {
            let schema = Arc::new(Schema::new(
                columns
                    .into_iter()
                    .map(|(column, data_type)| Field::new(column, data_type, true))
                    .collect::<Vec<_>>(),
            ));
            let remote = TableReference::partial("tpch", name);
            #[expect(
                clippy::disallowed_methods,
                reason = "routed through `create_spice_federated_table_provider` with the Athena policy, as `ODBCTableFactory` does"
            )]
            let table = SqlTable::new_with_schema("odbc", &pool, schema, remote.clone(), None)
                .with_dialect(Arc::new(athena_dialect()));
            let schema = table.schema();
            let provider = data_components::federation::create_spice_federated_table_provider(
                table,
                schema,
                remote,
                Some(function_support_for_engine(ODBCProfile::Athena.engine())),
            );
            df.ctx
                .register_table(name, Arc::new(provider))
                .expect("register the Athena table");
        }

        let pushed_sql = |sql: &str| -> Vec<String> {
            let plan = rt.block_on(async {
                df.ctx
                    .sql(sql)
                    .await
                    .unwrap_or_else(|e| panic!("plan {sql}: {e}"))
                    .create_physical_plan()
                    .await
                    .unwrap_or_else(|e| panic!("physical plan for {sql}: {e}"))
            });
            let rendered = datafusion::physical_plan::displayable(plan.as_ref())
                .indent(true)
                .to_string();
            // `base_sql` is the statement executed unless a later stage
            // rewrites it, which the plan then shows as a `rewritten_*` field.
            assert!(
                !rendered.contains(" rewritten_"),
                "the statement was rewritten after unparsing:\n{rendered}"
            );
            rendered
                .lines()
                .filter_map(|line| {
                    line.split_once(" base_sql=")
                        .map(|(_, sql)| sql.trim().to_string())
                })
                .collect()
        };

        assert_eq!(
            pushed_sql(Q11),
            [concat!(
                r#"SELECT "partsupp"."ps_partkey", sum(("partsupp"."ps_supplycost" * "partsupp"."ps_availqty")) AS "value" "#,
                r#"FROM "tpch"."partsupp" INNER JOIN "tpch"."supplier" ON ("partsupp"."ps_suppkey" = "supplier"."s_suppkey") "#,
                r#"INNER JOIN "tpch"."nation" ON ("supplier"."s_nationkey" = "nation"."n_nationkey") "#,
                r#"WHERE ("nation"."n_name" = 'GERMANY') GROUP BY "partsupp"."ps_partkey" "#,
                r#"HAVING (sum(("partsupp"."ps_supplycost" * "partsupp"."ps_availqty")) > "#,
                r#"(SELECT (sum(("partsupp"."ps_supplycost" * "partsupp"."ps_availqty")) * 0.0001) "#,
                r#"AS "sum(partsupp.ps_supplycost * partsupp.ps_availqty) * Float64(0.0001)" "#,
                r#"FROM "tpch"."partsupp" INNER JOIN "tpch"."supplier" ON ("partsupp"."ps_suppkey" = "supplier"."s_suppkey") "#,
                r#"INNER JOIN "tpch"."nation" ON ("supplier"."s_nationkey" = "nation"."n_nationkey") "#,
                r#"WHERE ("nation"."n_name" = 'GERMANY'))) ORDER BY "value" DESC NULLS FIRST"#,
            )]
        );
        assert_eq!(
            pushed_sql(Q17),
            [concat!(
                r#"SELECT (sum("lineitem"."l_extendedprice") / 7.0) AS "avg_yearly" "#,
                r#"FROM "tpch"."lineitem" INNER JOIN "tpch"."part" ON ("part"."p_partkey" = "lineitem"."l_partkey") "#,
                r#"WHERE ("lineitem"."l_quantity" < (SELECT (0.2 * avg("lineitem"."l_quantity")) "#,
                r#"AS "Float64(0.2) * avg(lineitem.l_quantity)" "#,
                r#"FROM "tpch"."lineitem" WHERE ("lineitem"."l_partkey" = "part"."p_partkey"))) "#,
                r#"AND ("part"."p_brand" = 'Brand#23') AND ("part"."p_container" = 'MED BOX')"#,
            )]
        );
    }
}

// Self-register into `data-connector-api`'s linkme `DATA_CONNECTOR_REGISTRATIONS` slice. Any binary/tool that
// should see this connector must force-link the crate (`use connector_odbc as _;`) -- a plain
// Cargo dependency won't link the slice static. See `register_data_connector!` docs.
data_connector_api::register_data_connector!(
    register_odbc_connector,
    ODBC_CONNECTOR_REGISTRATION,
    CONNECTOR_NAME,
    ODBCFactory
);
