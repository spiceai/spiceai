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

use clap::{ArgAction, Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use test_framework::anyhow;
use test_framework::queries::{QueryOverrides, QuerySet};

use super::CommonArgs;

#[derive(Parser, Debug, Clone)]
pub struct DatasetTestArgs {
    #[command(flatten)]
    pub(crate) common: CommonArgs,

    /// The expected scale factor for the test, used in metrics calculation
    #[arg(long)]
    pub(crate) scale_factor: Option<f64>,

    /// The query set to use for the test
    #[arg(long)]
    pub(crate) query_set: QuerySetArg,

    /// Path to a scenario query set file (YAML format, required when using --query-set scenario)
    #[arg(long, required_if_eq("query_set", "scenario"))]
    pub(crate) scenario_query_file: Option<PathBuf>,

    #[arg(long)]
    pub(crate) query_overrides: Option<QueryOverridesArg>,

    #[arg(long, action = ArgAction::Set, default_value_t = false, default_missing_value = "true", num_args = 0..=1, require_equals = false)]
    pub(crate) validate: bool,

    /// Reference schema containing known good tables for validation (e.g., "arrow" to validate against arrow.customer instead of customer)
    #[arg(long)]
    pub(crate) reference_schema: Option<String>,

    /// Whether to disable results caching, by supplying the cache control header through flight
    #[arg(long)]
    pub(crate) disable_caching: bool,

    /// Whether to add HTTP clients for the test
    #[arg(long)]
    pub(crate) http_clients: bool,
}

#[derive(Clone, ValueEnum, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum QuerySetArg {
    Tpch,
    Tpcds,
    Clickbench,
    #[value(name = "tpch[parameterized]")]
    #[serde(rename = "tpch[parameterized]")]
    ParameterizedTpch,
    /// Scenario query set loaded from a file (use --scenario-query-file)
    Scenario,
}

#[derive(Clone, ValueEnum, Debug, Deserialize, Serialize)]
pub enum QueryOverridesArg {
    #[serde(rename = "sqlite")]
    Sqlite,
    #[serde(rename = "postgresql")]
    Postgresql,
    #[serde(rename = "mysql")]
    Mysql,
    #[serde(rename = "dremio")]
    Dremio,
    #[serde(rename = "spark")]
    Spark,
    #[serde(rename = "odbc-athena")]
    ODBCAthena,
    #[serde(rename = "odbc-databricks")]
    ODBCDatabricks,
    #[serde(rename = "duckdb")]
    Duckdb,
    #[serde(rename = "duckdb-zero-results")]
    DuckdbZeroResults,
    #[serde(rename = "snowflake")]
    Snowflake,
    #[serde(rename = "oracle")]
    Oracle,
    #[serde(rename = "iceberg-sf1")]
    IcebergSF1,
    #[serde(rename = "iceberg-hadoop")]
    IcebergHadoop,
    #[serde(rename = "spicecloud-catalog")]
    SpicecloudCatalog,
    #[serde(rename = "glue-catalog")]
    GlueCatalog,
    #[serde(rename = "databricks-catalog")]
    DatabricksCatalog,
    #[serde(rename = "spicecloud")]
    Spicecloud,
    #[serde(rename = "dynamodb")]
    DynamoDB,
}

impl From<QuerySetArg> for QuerySet {
    fn from(arg: QuerySetArg) -> Self {
        match arg {
            QuerySetArg::Tpch => QuerySet::Tpch,
            QuerySetArg::Tpcds => QuerySet::Tpcds,
            QuerySetArg::Clickbench => QuerySet::Clickbench,
            QuerySetArg::ParameterizedTpch => QuerySet::ParameterizedTpch,
            QuerySetArg::Scenario => {
                // This should never be reached - callers must use DatasetTestArgs::load_query_set()
                // for Scenario query sets as they require loading from a file.
                unreachable!(
                    "Scenario query set requires loading from file - use DatasetTestArgs::load_query_set() instead"
                )
            }
        }
    }
}

impl DatasetTestArgs {
    /// Load the query set, handling scenario query sets from files
    pub fn load_query_set(&self) -> anyhow::Result<QuerySet> {
        match self.query_set {
            QuerySetArg::Scenario => {
                let Some(file_path) = self.scenario_query_file.as_ref() else {
                    anyhow::bail!("scenario_query_file is required when query_set is Scenario");
                };

                let scenario_set =
                    test_framework::queries::scenario::ScenarioQuerySet::from_file(file_path)?;
                let queries = scenario_set.clone().into_queries();

                Ok(QuerySet::Scenario {
                    queries,
                    scenario_set,
                })
            }
            _ => Ok(QuerySet::from(self.query_set.clone())),
        }
    }
}

impl From<QueryOverridesArg> for QueryOverrides {
    fn from(arg: QueryOverridesArg) -> Self {
        match arg {
            QueryOverridesArg::Sqlite => QueryOverrides::SQLite,
            QueryOverridesArg::Postgresql => QueryOverrides::PostgreSQL,
            QueryOverridesArg::Mysql => QueryOverrides::MySQL,
            QueryOverridesArg::Dremio => QueryOverrides::Dremio,
            QueryOverridesArg::Spark => QueryOverrides::Spark,
            QueryOverridesArg::ODBCAthena => QueryOverrides::ODBCAthena,
            QueryOverridesArg::ODBCDatabricks => QueryOverrides::ODBCDatabricks,
            QueryOverridesArg::Duckdb => QueryOverrides::DuckDB,
            QueryOverridesArg::DuckdbZeroResults => QueryOverrides::DuckDBOnZeroResults,
            QueryOverridesArg::Snowflake => QueryOverrides::Snowflake,
            QueryOverridesArg::Oracle => QueryOverrides::Oracle,
            QueryOverridesArg::IcebergSF1 => QueryOverrides::IcebergSF1,
            QueryOverridesArg::SpicecloudCatalog | QueryOverridesArg::DatabricksCatalog => {
                QueryOverrides::SpicecloudCatalog
            }
            QueryOverridesArg::Spicecloud => QueryOverrides::Spicecloud,
            QueryOverridesArg::GlueCatalog => QueryOverrides::GlueCatalog,
            QueryOverridesArg::IcebergHadoop => QueryOverrides::IcebergHadoop,
            QueryOverridesArg::DynamoDB => QueryOverrides::DynamoDB,
        }
    }
}

#[derive(Parser, Debug)]
pub struct DataConsistencyArgs {
    #[command(flatten)]
    pub(crate) test_args: DatasetTestArgs,

    #[arg(long)]
    pub(crate) compare_spicepod: PathBuf,
}

#[derive(Parser, Debug)]
pub struct LoadTestArgs {
    #[command(flatten)]
    pub(crate) test_args: DatasetTestArgs,

    #[arg(long)]
    pub(crate) no_error: bool,
}
