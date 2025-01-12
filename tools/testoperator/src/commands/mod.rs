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

use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;
use test_framework::queries::{QueryOverrides, QuerySet};

#[derive(Subcommand)]
pub enum Commands {
    // Run a test
    #[command(subcommand)]
    Run(TestCommands),
    // Export the spicepod environment that would run for a test
    #[command(subcommand)]
    Export(TestCommands),
}

#[derive(Subcommand)]
pub enum TestCommands {
    Throughput(TestArgs),
    Load(TestArgs),
}

#[derive(Parser)]
pub struct TestArgs {
    /// Path to the spicepod.yaml file
    #[arg(short('p'), long)]
    pub(crate) spicepod_path: PathBuf,

    /// Path to the spiced binary
    #[arg(short, long)]
    pub(crate) spiced_path: PathBuf,

    /// An optional data directory, to symlink into the spiced instance
    #[arg(short, long)]
    pub(crate) data_dir: Option<PathBuf>,

    /// The expected scale factor for the test, used in metrics calculation
    #[arg(long)]
    pub(crate) scale_factor: Option<f64>,

    /// The duration of the test in seconds
    #[arg(long)]
    pub(crate) duration: Option<usize>,

    /// The query set to use for the test
    #[arg(long)]
    pub(crate) query_set: QuerySetArg,

    #[arg(long)]
    pub(crate) query_overrides: Option<QueryOverridesArg>,

    #[arg(long)]
    pub(crate) concurrency: Option<usize>,

    #[arg(long)]
    pub(crate) ready_wait: Option<usize>,
}

#[derive(Clone, ValueEnum)]
pub enum QuerySetArg {
    Tpch,
    Tpcds,
    ClickBench,
}

#[derive(Clone, ValueEnum)]
pub enum QueryOverridesArg {
    Sqlite,
    Postgresql,
    Mysql,
    Dremio,
    Spark,
    ODBCAthena,
    Duckdb,
}

impl From<QuerySetArg> for QuerySet {
    fn from(arg: QuerySetArg) -> Self {
        match arg {
            QuerySetArg::Tpch => QuerySet::Tpch,
            QuerySetArg::Tpcds => QuerySet::Tpcds,
            QuerySetArg::ClickBench => QuerySet::ClickBench,
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
        }
    }
}
