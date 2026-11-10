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
use crate::odbcconn::ODBCDbConnectionPool;
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

fn databricks_dialect() -> CustomDialect {
    CustomDialectBuilder::new()
        .with_identifier_quote_style('`')
        .with_interval_style(IntervalStyle::MySQL)
        .with_utf8_cast_dtype(datafusion::sql::sqlparser::ast::DataType::String(None))
        .with_large_utf8_cast_dtype(datafusion::sql::sqlparser::ast::DataType::String(None))
        .build()
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

fn driver_is_file(driver: &str) -> bool {
    driver
        .split('=')
        .next_back()
        .as_ref()
        .is_some_and(|s| std::fs::metadata(s).is_ok())
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

            let (dialect, engine): (
                Result<Option<Arc<dyn Dialect + Send + Sync>>>,
                Option<String>,
            ) = if let Some(sql_dialect) = params.parameters.get("sql_dialect").expose().ok() {
                let engine = sql_dialect.to_string();
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

                // explicitly check if the user has tried to specify a file path
                if driver_is_file(driver) {
                    return Err(Error::DirectDriverNotPermitted {}.into());
                }

                let profile = ODBCProfile::from(driver);
                let engine = profile.engine().map(str::to_string);
                (Ok(profile.into()), engine)
            };
            let dialect = dialect?;

            let pool: Arc<ODBCDbConnectionPool> = Arc::new(
                ODBCPool::new(params.parameters.to_secret_map())
                    .context(UnableToCreateODBCConnectionPoolSnafu)?,
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
