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
#[expect(clippy::struct_excessive_bools)]
pub struct QueryArgs {
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

    /// Use distributed query mode via /v1/queries API (requires cluster mode with scheduler role)
    #[arg(long)]
    pub(crate) distributed: bool,
}

#[derive(Parser, Debug, Clone)]
#[expect(clippy::struct_excessive_bools)]
pub struct DatasetTestArgs {
    #[command(flatten)]
    pub(crate) common: CommonArgs,

    /// The expected scale factor for the test, used in metrics calculation
    #[arg(long)]
    pub(crate) scale_factor: Option<f64>,

    /// Source database for the CH-benCH workload (only used with `--query-set chbench`).
    /// Defaults to postgres.
    #[arg(long, value_enum, default_value = "postgres")]
    pub(crate) source_type: SourceType,

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

    /// Use distributed query mode via /v1/queries API (requires cluster mode with scheduler role)
    #[arg(long)]
    pub(crate) distributed: bool,

    /// Random parameter set count for parameterized queries (tests with different random parameters each run).
    /// If not specified or 0, fixed parameters are used (no randomization).
    #[arg(long)]
    pub(crate) random_param_set_count: Option<usize>,

    /// Mark queries as failed if they exceed this duration threshold (e.g., "500ms", "2s").
    /// Useful for identifying slow queries that should be treated as failures in metrics.
    #[arg(long, value_parser = parse_duration)]
    pub(crate) mark_query_failed_if_exceeds: Option<std::time::Duration>,

    /// How concurrent clients connect to spiced: `shared` multiplexes every client
    /// over one connection; `per-client` opens a dedicated connection per client,
    /// matching a fleet of independent clients (use for connection-scale load tests).
    /// For a full client fleet, use `--clients`/`--connections-per-client`/
    /// `--queries-per-client` instead, which model all three dimensions.
    #[arg(long, value_enum, default_value = "shared")]
    pub(crate) client_connections: ClientConnectionsArg,

    /// Number of simulated client instances (application servers). Each holds its
    /// own connection pool and runs its own query threads, so the three fleet
    /// flags are set together. Total server connections are
    /// `clients * connections-per-client`; total concurrent queries are
    /// `clients * queries-per-client`, which supersedes `--concurrency`.
    #[arg(
        long,
        requires = "connections_per_client",
        requires = "queries_per_client"
    )]
    pub(crate) clients: Option<usize>,

    /// Connection-pool size **within each client**. A client's query threads use
    /// only that client's connections, exactly as an application server's pool is
    /// private to that process.
    #[arg(long, requires = "clients")]
    pub(crate) connections_per_client: Option<usize>,

    /// Concurrent query threads **within each client**. When this exceeds
    /// `--connections-per-client`, the client's threads share its pooled
    /// connections; when it is smaller, some pooled connections stay idle — both
    /// mirror how a real pool behaves under load.
    #[arg(long, requires = "clients")]
    pub(crate) queries_per_client: Option<usize>,
}

/// A simulated client fleet: `clients` application instances, each holding
/// `connections_per_client` connections and running `queries_per_client`
/// concurrent query threads over them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Fleet {
    pub clients: usize,
    pub connections_per_client: usize,
    pub queries_per_client: usize,
}

impl Fleet {
    /// Concurrent query threads across the whole fleet — the test's parallel count.
    #[must_use]
    pub fn total_queries(&self) -> usize {
        self.clients * self.queries_per_client
    }

    /// Client-side connection slots *provisioned* across the fleet: `clients`
    /// pools of `connections_per_client` each.
    ///
    /// This is the configured size, not a prediction of what the server will
    /// observe, and the two coincide only under both of these:
    ///
    /// - One executor object is one connection on the wire. True for the Flight
    ///   executor, whose `spiceai::Client` is a single multiplexed HTTP/2 channel
    ///   however many workers share it. NOT true of a `reqwest`-backed executor,
    ///   where an executor is an uncapped HTTP/1.1 pool that opens a connection
    ///   per concurrent in-flight request; [`DatasetTestArgs::validate_fleet`]
    ///   rejects that combination rather than reporting a figure it cannot
    ///   establish.
    /// - Every slot is actually exercised. [`Self::connection_for_worker`] hands a
    ///   client's threads round-robin over that client's own pool, so a client
    ///   touches `min(connections_per_client, queries_per_client)` of its slots; an
    ///   over-provisioned pool leaves the remainder idle and the server sees fewer
    ///   connections than this returns.
    #[must_use]
    pub fn total_connections(&self) -> usize {
        self.clients * self.connections_per_client
    }

    /// The connection a given worker uses. Workers are laid out client-major, so
    /// worker `w` belongs to client `w / queries_per_client`; within that client
    /// its threads round-robin over that client's own pool and never touch
    /// another client's connections.
    #[must_use]
    pub fn connection_for_worker(&self, worker: usize) -> usize {
        let client = (worker / self.queries_per_client) % self.clients;
        let thread = worker % self.queries_per_client;
        client * self.connections_per_client + (thread % self.connections_per_client)
    }
}

/// How concurrent test clients connect to the spiced endpoint under test.
#[derive(Clone, Copy, ValueEnum, Debug, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ClientConnectionsArg {
    /// All clients share one connection (gRPC channel for Flight, one HTTP pool),
    /// multiplexing queries over it.
    #[default]
    Shared,
    /// Every client opens its own connection, so concurrency N exercises N real
    /// connections on the server.
    PerClient,
}

#[derive(Clone, ValueEnum, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum QuerySetArg {
    Tpch,
    Tpcds,
    Clickbench,
    #[value(name = "chbench")]
    #[serde(rename = "chbench")]
    ChBench,
    #[value(name = "tpch[parameterized]")]
    #[serde(rename = "tpch[parameterized]")]
    ParameterizedTpch,
    /// Scenario query set loaded from a file (use --scenario-query-file)
    Scenario,
}

/// Source database backing the CH-benCH workload.
#[derive(Clone, Copy, ValueEnum, Debug, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum SourceType {
    #[default]
    Postgres,
    Mysql,
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
    #[serde(rename = "postgres-catalog")]
    PostgresCatalog,
    #[serde(rename = "mysql-catalog")]
    MysqlCatalog,
    #[serde(rename = "mssql-catalog")]
    #[value(name = "mssql-catalog")]
    MsSqlCatalog,
    #[serde(rename = "oracle-catalog")]
    OracleCatalog,
    #[serde(rename = "snowflake-catalog")]
    #[value(name = "snowflake-catalog")]
    SnowflakeCatalog,
    #[serde(rename = "ducklake-catalog")]
    #[value(name = "ducklake-catalog")]
    DucklakeCatalog,
    #[serde(rename = "spicecloud")]
    Spicecloud,
    #[serde(rename = "dynamodb")]
    #[value(name = "dynamodb")]
    DynamoDB,
    #[serde(rename = "arrow")]
    Arrow,
    #[serde(rename = "cayenne")]
    Cayenne,
    #[serde(rename = "turso")]
    Turso,
    #[serde(rename = "bigquery")]
    #[value(name = "bigquery")]
    BigQuery,
    #[serde(rename = "scylladb")]
    #[value(name = "scylladb")]
    ScyllaDB,
    #[serde(rename = "chbench-skip-slow")]
    #[value(name = "chbench-skip-slow")]
    ChbenchSkipSlow,
}

impl From<QuerySetArg> for QuerySet {
    fn from(arg: QuerySetArg) -> Self {
        match arg {
            QuerySetArg::Tpch => QuerySet::Tpch,
            QuerySetArg::Tpcds => QuerySet::Tpcds,
            QuerySetArg::Clickbench => QuerySet::Clickbench,
            QuerySetArg::ChBench => QuerySet::ChBench,
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

impl PartialEq<QuerySet> for QuerySetArg {
    fn eq(&self, other: &QuerySet) -> bool {
        matches!(
            (self, other),
            (QuerySetArg::Tpch, QuerySet::Tpch)
                | (QuerySetArg::Tpcds, QuerySet::Tpcds)
                | (QuerySetArg::Clickbench, QuerySet::Clickbench)
                | (QuerySetArg::ChBench, QuerySet::ChBench)
                | (QuerySetArg::ParameterizedTpch, QuerySet::ParameterizedTpch)
                | (QuerySetArg::Scenario, QuerySet::Scenario { .. })
        )
    }
}

pub trait QuerySetLoader {
    fn query_set(&self) -> &QuerySetArg;
    fn scenario_query_file(&self) -> Option<&PathBuf>;

    fn load_query_set(&self) -> anyhow::Result<QuerySet> {
        match self.query_set() {
            QuerySetArg::Scenario => {
                let Some(file_path) = self.scenario_query_file() else {
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
            query_set => Ok(QuerySet::from(query_set.clone())),
        }
    }
}

impl DatasetTestArgs {
    /// Load the query set, handling scenario query sets from files
    pub fn load_query_set(&self) -> anyhow::Result<QuerySet> {
        QuerySetLoader::load_query_set(self)
    }

    /// The simulated client fleet, when the three fleet flags were given.
    /// `clap` requires them together, so a present `clients` implies the rest.
    #[must_use]
    pub fn fleet(&self) -> Option<Fleet> {
        Some(Fleet {
            clients: self.clients?,
            connections_per_client: self.connections_per_client?,
            queries_per_client: self.queries_per_client?,
        })
    }

    /// Concurrent query threads to run: the fleet's total when one is
    /// configured, otherwise `--concurrency`.
    #[must_use]
    pub fn effective_concurrency(&self) -> usize {
        self.fleet()
            .map_or(self.common.concurrency, |f| f.total_queries())
    }

    /// Validate the fleet flags. Called at command entry so a bad combination
    /// fails before spiced is started or waited on.
    pub fn validate_fleet(&self) -> anyhow::Result<()> {
        let Some(fleet) = self.fleet() else {
            return Ok(());
        };
        anyhow::ensure!(
            self.client_connections == ClientConnectionsArg::Shared,
            "--clients/--connections-per-client/--queries-per-client already describe the \
             connection topology; drop --client-connections"
        );
        anyhow::ensure!(
            fleet.clients >= 1
                && fleet.connections_per_client >= 1
                && fleet.queries_per_client >= 1,
            "--clients, --connections-per-client and --queries-per-client must each be at least 1"
        );
        // A fleet promises the server sees `clients * connections-per-client`
        // connections, and its `connections-per-client` dimension only means
        // anything if a shared executor is a shared connection. That holds for
        // the Flight executor (one multiplexed HTTP/2 channel per client) but
        // not for a `reqwest`-backed one: that is a pool with no maximum, so the
        // threads sharing it each open their own connection and the dimension is
        // inert. Refuse the combination rather than announce a topology that was
        // never established.
        anyhow::ensure!(
            !self.http_clients && !self.distributed,
            "--clients/--connections-per-client/--queries-per-client model a connection pool per \
             client, which needs the Flight executor: an HTTP executor (--http-clients, \
             --distributed) opens a connection per concurrent request, so \
             --connections-per-client would not bound anything. Pick one: (1) drop \
             --http-clients/--distributed and keep the fleet flags to run it over Flight, or (2) \
             drop the fleet flags and pass --client-connections per-client (one pool per query \
             thread, which an HTTP executor does honor) — the fleet flags and \
             --client-connections describe the same topology and cannot be combined"
        );
        Ok(())
    }
}

impl QuerySetLoader for DatasetTestArgs {
    fn query_set(&self) -> &QuerySetArg {
        &self.query_set
    }

    fn scenario_query_file(&self) -> Option<&PathBuf> {
        self.scenario_query_file.as_ref()
    }
}

impl QuerySetLoader for QueryArgs {
    fn query_set(&self) -> &QuerySetArg {
        &self.query_set
    }

    fn scenario_query_file(&self) -> Option<&PathBuf> {
        self.scenario_query_file.as_ref()
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
            QueryOverridesArg::Arrow => QueryOverrides::Arrow,
            QueryOverridesArg::Cayenne => QueryOverrides::Cayenne,
            QueryOverridesArg::PostgresCatalog => QueryOverrides::PostgresCatalog,
            QueryOverridesArg::MysqlCatalog => QueryOverrides::MysqlCatalog,
            QueryOverridesArg::MsSqlCatalog => QueryOverrides::MSSqlCatalog,
            QueryOverridesArg::OracleCatalog => QueryOverrides::OracleCatalog,
            QueryOverridesArg::SnowflakeCatalog => QueryOverrides::SnowflakeCatalog,
            QueryOverridesArg::DucklakeCatalog => QueryOverrides::DucklakeCatalog,
            QueryOverridesArg::Turso => QueryOverrides::Turso,
            QueryOverridesArg::BigQuery => QueryOverrides::BigQuery,
            QueryOverridesArg::ScyllaDB => QueryOverrides::ScyllaDB,
            QueryOverridesArg::ChbenchSkipSlow => QueryOverrides::ChbenchSkipSlow,
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

    /// Run until manually interrupted; disables duration-based stopping for the load phase
    #[arg(long)]
    pub(crate) run_until_stopped: bool,

    /// Pin the aggregate query issue rate, in queries per second across the
    /// whole client fleet, for the load phase.
    ///
    /// Without it the load phase is CLOSED-LOOP: each client sends a query,
    /// waits, and sends the next, so the offered rate is a result of the
    /// server's latency rather than an input. That makes two builds hard to
    /// compare — a slower build simply issues fewer queries and can post the
    /// same per-query latency, hiding the regression in the throughput column.
    /// Pinning the rate makes both do identical work, so the difference lands
    /// in latency. Unset (or 0) keeps the closed-loop behaviour.
    #[arg(long)]
    pub(crate) target_qps: Option<f64>,

    /// API key for authenticating with an external spiced instance.
    /// Only applicable when --spiced-path is a URL to an already-running instance.
    #[arg(long)]
    pub(crate) api_key: Option<String>,
}

/// Parse a duration string like "500ms", "2s", "1m" into a `Duration`
fn parse_duration(s: &str) -> Result<std::time::Duration, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("duration cannot be empty".to_string());
    }

    // Find where the numeric part ends
    let num_end = s
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(s.len());

    let (num_str, unit) = s.split_at(num_end);
    let num: f64 = num_str
        .parse()
        .map_err(|_| format!("invalid number: {num_str}"))?;

    let multiplier = match unit.trim().to_lowercase().as_str() {
        "ms" | "millis" | "milliseconds" => 1.0,
        "s" | "sec" | "secs" | "second" | "seconds" | "" => 1000.0,
        "m" | "min" | "mins" | "minute" | "minutes" => 60_000.0,
        _ => return Err(format!("unknown time unit: {unit}")),
    };

    #[expect(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    let millis = (num * multiplier) as u64;
    Ok(std::time::Duration::from_millis(millis))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fleet(clients: usize, connections_per_client: usize, queries_per_client: usize) -> Fleet {
        Fleet {
            clients,
            connections_per_client,
            queries_per_client,
        }
    }

    #[test]
    fn fleet_totals_multiply_the_right_dimensions() {
        // 4 clients, each holding a 128-connection pool and running 32 query
        // threads: 512 connections on the server, 128 concurrent queries.
        let f = fleet(4, 128, 32);
        assert_eq!(f.total_connections(), 512);
        assert_eq!(f.total_queries(), 128);
    }

    #[test]
    fn a_clients_threads_only_use_that_clients_pool() {
        // 3 clients x 2 connections x 4 threads. Client i owns connections
        // [i*2, i*2+1] and must never be handed another client's.
        let f = fleet(3, 2, 4);
        for worker in 0..f.total_queries() {
            let client = worker / f.queries_per_client;
            let conn = f.connection_for_worker(worker);
            let owned = client * f.connections_per_client;
            assert!(
                (owned..owned + f.connections_per_client).contains(&conn),
                "worker {worker} (client {client}) got connection {conn}, outside its pool"
            );
        }
    }

    #[test]
    fn threads_round_robin_within_their_pool() {
        // 1 client, 2 connections, 5 threads: threads alternate over the pool,
        // so a pool smaller than the thread count is shared rather than grown.
        let f = fleet(1, 2, 5);
        let conns: Vec<usize> = (0..f.total_queries())
            .map(|w| f.connection_for_worker(w))
            .collect();
        assert_eq!(conns, vec![0, 1, 0, 1, 0]);
    }

    /// Parse a `DatasetTestArgs` from flags, defaulting everything the fleet
    /// tests do not care about. `--query-set` is the one argument with no
    /// default.
    fn args_from(flags: &[&str]) -> DatasetTestArgs {
        let mut argv = vec!["testoperator", "--query-set", "tpch"];
        argv.extend_from_slice(flags);
        DatasetTestArgs::try_parse_from(argv).expect("flags parse")
    }

    const FLEET_FLAGS: [&str; 6] = [
        "--clients",
        "4",
        "--connections-per-client",
        "2",
        "--queries-per-client",
        "32",
    ];

    #[test]
    fn a_fleet_is_accepted_for_the_flight_executor() {
        // The default executor is Flight, where one `spiceai::Client` is one
        // multiplexed HTTP/2 channel however many workers share it — so
        // `connections-per-client` really does bound what the server sees.
        let args = args_from(&FLEET_FLAGS);
        args.validate_fleet().expect("a Flight fleet is valid");
        assert_eq!(args.effective_concurrency(), 128);
    }

    #[test]
    fn a_fleet_is_rejected_for_http_backed_executors() {
        // A `reqwest` pool has no maximum, so the threads sharing one open a
        // connection each and `connections-per-client` bounds nothing. Announcing
        // "8 connections" for what the server sees as up to 128 would make a
        // connection-scale run measure something other than what it reports.
        for http_flag in ["--http-clients", "--distributed"] {
            let mut flags = FLEET_FLAGS.to_vec();
            flags.push(http_flag);
            let err = args_from(&flags)
                .validate_fleet()
                .expect_err("a fleet over an HTTP executor must be rejected")
                .to_string();
            assert!(
                err.contains("Flight executor"),
                "{http_flag} rejection must name the executor that supports a fleet, got: {err}"
            );
        }
    }

    #[test]
    fn per_client_connections_remain_valid_over_http() {
        // Deliberately NOT rejected: one pool per query thread means one
        // in-flight request per pool, so an HTTP executor does establish exactly
        // the advertised connection per client. Only the fleet's shared-pool
        // dimension is unrealizable.
        for http_flag in ["--http-clients", "--distributed"] {
            let args = args_from(&["--client-connections", "per-client", http_flag]);
            args.validate_fleet()
                .expect("per-client over HTTP stays valid");
        }
    }

    #[test]
    fn a_pool_larger_than_the_thread_count_leaves_connections_idle() {
        // 1 client, 8 connections, 2 threads: only two connections ever carry a
        // query, matching an over-provisioned pool.
        let f = fleet(1, 8, 2);
        let used: Vec<usize> = (0..f.total_queries())
            .map(|w| f.connection_for_worker(w))
            .collect();
        assert_eq!(used, vec![0, 1]);
        assert_eq!(f.total_connections(), 8);
    }
}
