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

use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
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

    #[arg(long)]
    pub(crate) query_overrides: Option<QueryOverridesArg>,
}

#[derive(Clone, ValueEnum, Debug)]
pub enum QuerySetArg {
    Tpch,
    Tpcds,
    Clickbench,
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
    #[serde(rename = "duckdb")]
    Duckdb,
    #[serde(rename = "snowflake")]
    Snowflake,
    #[serde(rename = "iceberg-sf1")]
    IcebergSF1,
    #[serde(rename = "spicecloud-catalog")]
    SpicecloudCatalog,
}

impl From<QuerySetArg> for QuerySet {
    fn from(arg: QuerySetArg) -> Self {
        match arg {
            QuerySetArg::Tpch => QuerySet::Tpch,
            QuerySetArg::Tpcds => QuerySet::Tpcds,
            QuerySetArg::Clickbench => QuerySet::Clickbench,
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
            QueryOverridesArg::Duckdb => QueryOverrides::DuckDB,
            QueryOverridesArg::Snowflake => QueryOverrides::Snowflake,
            QueryOverridesArg::IcebergSF1 => QueryOverrides::IcebergSF1,
            QueryOverridesArg::SpicecloudCatalog => QueryOverrides::SpicecloudCatalog,
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
