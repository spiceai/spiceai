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

//! CH-benCH driver — TPC-C schema + seed data loader and OLTP workload driver.
//!
//! Creates 12 tables (9 TPC-C + 3 CH supplemental), loads seed data,
/// and runs a TPC-C OLTP workload with configurable terminals.
pub mod config;
pub mod csv_gen;
pub mod loader;
pub mod loader_mysql;
pub mod metrics;
pub mod rand;
pub mod schema;
pub mod schema_mysql;
pub mod txn;
pub mod watermark;

pub use config::{ChBenchConfig, MysqlSourceConfig, PostgresSourceConfig};
pub use metrics::OltpReport;
pub use txn::TxnType;
pub use watermark::{BenchTs, Watermarks};

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;

use ::rand::SeedableRng;
use ::rand::rngs::StdRng;
use arrow::array::RecordBatch;
use async_trait::async_trait;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::AsyncDbConnection;
use datafusion_table_providers::sql::db_connection_pool::mysqlpool::MySQLConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use futures::TryStreamExt;
use governor::{
    Quota, RateLimiter,
    clock::DefaultClock,
    middleware::NoOpMiddleware,
    state::{InMemoryState, NotKeyed},
};
use mysql_async::prelude::Queryable;
use secrecy::SecretBox;
use snafu::{Snafu, ensure};
use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;

/// Shared rate limiter gating the aggregate OLTP transaction rate across all terminals.
type OltpRateLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to {action}: {source}"))]
    Sql {
        action: String,
        source: tokio_postgres::Error,
    },

    #[snafu(display("Failed to {action}: {source}"))]
    MySql {
        action: String,
        source: mysql_async::Error,
    },

    #[snafu(display(
        "{failed}/{total} OLTP terminal(s) failed — benchmark results are unreliable"
    ))]
    OltpTerminalFailures { failed: usize, total: usize },

    #[snafu(display("Invalid OLTP target rate: {rate} (must be > 0)"))]
    InvalidRate { rate: u32 },

    #[snafu(display("Failed to {action}: {message}"))]
    Arrow { action: String, message: String },

    #[snafu(display("Background loader task failed: {message}"))]
    TaskJoin { message: String },

    #[snafu(display(
        "--skip-prepare source has {found} warehouse(s) but --scale-factor expects {expected}; \
         restore a matching template or drop --skip-prepare to re-seed"
    ))]
    SourceScaleMismatch { found: u64, expected: u64 },

    #[snafu(display("Failed to {action}: {source}"))]
    Io {
        action: String,
        source: std::io::Error,
    },

    #[snafu(display(
        "Internal error: no column list registered for table {table} in csv_gen::TABLE_COLUMNS"
    ))]
    UnknownTable { table: String },

    #[snafu(display(
        "Internal error: table {table} has no _bench_ts watermark (not a mutated TPC-C table)"
    ))]
    UnknownWatermarkTable { table: String },

    #[snafu(display(
        "Internal error: the _bench_ts watermark for {table} was never seeded — \
         prepare() or verify_prepared() must seed every mutated table before the workload starts"
    ))]
    UnseededWatermark { table: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Await a batch of spawned loader tasks (from either the Postgres or `MySQL`
/// seed load workers), propagating the first task error — or a panic as
/// [`Error::TaskJoin`]. On failure, the tasks not yet finished are aborted so a
/// failed load does not leave workers mutating the database detached. `what`
/// names the phase for the panic message (e.g. "COPY load").
///
/// Tasks are polled concurrently via `select_all`, so a failure in *any* task is
/// noticed as soon as it completes — the abort is not delayed behind an earlier
/// still-running task.
pub(crate) async fn join_loader_tasks(
    handles: Vec<tokio::task::JoinHandle<Result<()>>>,
    what: &str,
) -> Result<()> {
    let mut pending = handles;
    while !pending.is_empty() {
        let (joined, _idx, rest) = futures::future::select_all(pending).await;
        let err = match joined {
            Ok(Ok(())) => {
                pending = rest;
                continue;
            }
            Ok(Err(e)) => e,
            Err(e) => Error::TaskJoin {
                message: format!("{what} loader task panicked: {e}"),
            },
        };
        for handle in &rest {
            handle.abort();
        }
        // Wait for the aborted tasks to actually finish (not just request
        // cancellation) before returning. The caller may drop a resource the
        // workers were using right after this returns (e.g. the seed loaders'
        // tempdir, deleted as soon as `load_all` returns its error) — without
        // this, an aborted-but-still-running worker could race against that
        // cleanup and produce a confusing secondary error alongside the real one.
        for handle in rest {
            let _ = handle.await;
        }
        return Err(err);
    }
    Ok(())
}

/// Source-agnostic interface for CH-benCH benchmarks.
///
/// Each source (Postgres, `MySQL`, `DynamoDB`, etc.) implements this trait to
/// provide schema setup, OLTP workload execution, and staleness probing.
#[async_trait]
pub trait ChBenchDriver: Send + Sync {
    /// Set up the schema, seed data, and any source-specific instrumentation
    /// (e.g. `_bench_ts` triggers).
    async fn prepare(&self) -> Result<()>;

    /// Run the TPC-C OLTP workload until `stop` is triggered.
    ///
    /// The caller controls lifetime — trigger the token to stop the workload.
    async fn run(&self, stop: CancellationToken) -> Result<OltpReport>;

    /// Tables to probe for staleness measurement.
    ///
    /// Returns a subset of mutated tables chosen for CDC path diversity
    /// (e.g. small-table updates, high-volume inserts, deletes).
    fn probe_tables(&self) -> &[&str];

    /// High-water mark of `_bench_ts` for `table`, in microseconds since the
    /// Unix epoch, or `None` if the table is empty.
    ///
    /// The `MySQL` driver answers this from its own record of committed writes
    /// (see [`crate::watermark`])  - a `SELECT MAX(_bench_ts)` scan is very expensive
    /// on `MySQL` large tables. Tables with DELETEs cannot use a monotone watermark and are
    /// answered from the source instead — see
    /// [`crate::watermark::DELETE_BEARING_TABLES`] and
    /// [`Self::max_bench_ts_exact`].
    async fn max_bench_ts(&self, table: &str) -> Result<Option<i64>>;

    /// Authoritative `SELECT MAX(_bench_ts)` against the source.
    ///
    /// Expensive on large tables (a full scan) — call once per run for final
    /// verification, never in a poll loop. This is what makes a driver
    /// bookkeeping bug visible instead of letting it pass the drain gate.
    async fn max_bench_ts_exact(&self, table: &str) -> Result<Option<i64>>;

    /// Read `COUNT(*)` from the *source* for a given table.
    async fn row_count(&self, table: &str) -> Result<i64>;

    /// Execute an arbitrary read-only SQL statement against the source and
    /// return the results as Arrow `RecordBatch`es.
    ///
    /// Used by the analytical-query gate to produce ground-truth results
    /// for CH-benCH queries that are then compared against the same query run
    /// through Spice.
    async fn query_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>>;
}

/// Postgres-backed CH-benCH driver.
pub struct PostgresChBenchDriver {
    client: tokio_postgres::Client,
    config: ChBenchConfig,
    source: PostgresSourceConfig,
    /// Postgres pool that returns query results as Arrow `RecordBatch`es,
    /// kept separate from `client` because the analytical-query gate
    /// needs Arrow output to compare against Spice results
    arrow_client: OnceCell<Arc<PostgresConnectionPool>>,
}

impl PostgresChBenchDriver {
    /// Connect to Postgres using the provided configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection cannot be established.
    pub async fn connect(config: ChBenchConfig, source: PostgresSourceConfig) -> Result<Self> {
        let conn_str = source.connection_string();
        let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .map_err(|source| Error::Sql {
                action: "connect to PostgreSQL".into(),
                source,
            })?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("CH-benCH source PostgreSQL connection error: {e}");
            }
        });

        Ok(Self {
            client,
            config,
            source,
            arrow_client: OnceCell::new(),
        })
    }

    /// Build the Arrow-returning connection pool from the source config.
    async fn build_arrow_client(&self) -> Result<Arc<PostgresConnectionPool>> {
        let mut params: HashMap<String, SecretBox<str>> = HashMap::new();
        params.insert("host".into(), SecretBox::from(self.source.host.clone()));
        params.insert("port".into(), SecretBox::from(self.source.port.to_string()));
        params.insert("db".into(), SecretBox::from(self.source.db.clone()));
        params.insert("user".into(), SecretBox::from(self.source.user.clone()));
        params.insert("pass".into(), SecretBox::from(self.source.pass.clone()));
        params.insert("sslmode".into(), SecretBox::from("disable".to_string()));

        let pool = PostgresConnectionPool::new(params)
            .await
            .map_err(|e| Error::Arrow {
                action: "build PostgresConnectionPool".into(),
                message: e.to_string(),
            })?;
        Ok(Arc::new(pool))
    }

    /// Verify the source already holds a prepared CH-benCH dataset matching the
    /// configured warehouse count. Used on the `--skip-prepare` path, where the
    /// source is restored externally (e.g. from a Postgres template) rather than
    /// seeded here, so a missing or wrong-scale source would otherwise produce
    /// silently-wrong benchmark results.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Sql`] if the `warehouse` table is absent (source not
    /// prepared at all) and [`Error::SourceScaleMismatch`] if its row count does
    /// not equal the configured scale factor.
    pub async fn verify_prepared(&self) -> Result<()> {
        let row = self
            .client
            .query_one("SELECT count(*)::bigint FROM warehouse", &[])
            .await
            .map_err(|source| Error::Sql {
                action: "verify --skip-prepare source (is the warehouse table seeded?)".into(),
                source,
            })?;
        // count(*) is non-negative; compare in u64 so the configured warehouse
        // count (usize) needs no lossy cast (usize -> u64 is always lossless).
        let found = u64::try_from(row.get::<_, i64>(0)).unwrap_or(0);
        let expected = self.config.warehouses as u64;
        ensure!(
            found == expected,
            SourceScaleMismatchSnafu { found, expected }
        );
        Ok(())
    }
}

#[async_trait]
impl ChBenchDriver for PostgresChBenchDriver {
    /// Drop and recreate all 12 CH-benCH tables, then load seed data.
    async fn prepare(&self) -> Result<()> {
        println!(
            "Preparing CH-benCHmark schema with {} warehouse(s)",
            self.config.warehouses,
        );

        schema::drop_tables(&self.client).await?;
        schema::create_tables(&self.client).await?;
        let conn_str = self.source.connection_string();
        loader::load_all(&conn_str, self.config.warehouses, self.config.seed).await?;
        // Build secondary indexes and attach the _bench_ts triggers *after* the
        // bulk load so neither is maintained per-row during the seed load.
        schema::create_indexes(&self.client).await?;
        schema::create_triggers(&self.client).await?;

        Ok(())
    }

    /// Run the TPC-C OLTP workload until `stop` is triggered.
    ///
    /// Each terminal opens its own Postgres connection and runs transactions
    /// in a tight loop with the configured mix weights. The caller controls
    /// lifetime by triggering the token.
    async fn run(&self, stop: CancellationToken) -> Result<OltpReport> {
        let terminals = self.config.terminals;
        let mix = self.config.mix;
        let warehouses = i32::try_from(self.config.warehouses).unwrap_or(1);
        let base_seed = self.config.seed.unwrap_or(42);

        let assignments = txn::TerminalAssignment::compute(terminals, warehouses);

        // A single shared, work-conserving limiter caps the aggregate transaction rate across all terminals.
        let rate_limiter = match self.config.rate {
            Some(rate) => {
                let cells = NonZeroU32::new(rate).ok_or(Error::InvalidRate { rate })?;
                Some(Arc::new(RateLimiter::direct(Quota::per_second(cells))))
            }
            None => None,
        };

        match self.config.rate {
            Some(rate) => println!(
                "Starting OLTP workload: {warehouses} warehouse(s), {terminals} terminals, mix={mix:?}, target rate={rate} txn/s",
            ),
            None => println!(
                "Starting OLTP workload: {warehouses} warehouse(s), {terminals} terminals, mix={mix:?}, target rate=unlimited",
            ),
        }

        let mut handles = Vec::with_capacity(terminals);
        for (terminal_id, &assignment) in assignments.iter().enumerate() {
            let conn_str = self.source.connection_string();
            let stop = stop.clone();
            let rate_limiter = rate_limiter.clone();

            handles.push(tokio::spawn(async move {
                run_terminal(
                    terminal_id,
                    &conn_str,
                    stop,
                    assignment,
                    mix,
                    base_seed,
                    rate_limiter,
                )
                .await
            }));
        }

        // Collect results from all terminals
        let mut combined = metrics::OltpMetrics::new();
        let mut failed_terminals: usize = 0;
        for handle in handles {
            match handle.await {
                Ok(Ok(terminal_metrics)) => {
                    combined.merge(&terminal_metrics);
                }
                Ok(Err(e)) => {
                    eprintln!("Terminal error: {e}");
                    failed_terminals += 1;
                }
                Err(e) => {
                    eprintln!("Terminal join error: {e}");
                    failed_terminals += 1;
                }
            }
        }

        ensure!(
            failed_terminals == 0,
            OltpTerminalFailuresSnafu {
                failed: failed_terminals,
                total: terminals,
            }
        );

        let report = combined.finish();
        Ok(report)
    }

    fn probe_tables(&self) -> &[&str] {
        schema::STALENESS_PROBE_TABLES
    }

    /// The Postgres path still reads the source directly (the in-memory
    /// watermark is a MySQL-driver optimization so far); `max_bench_ts` and
    /// `max_bench_ts_exact` are therefore the same scan.
    async fn max_bench_ts(&self, table: &str) -> Result<Option<i64>> {
        self.max_bench_ts_exact(table).await
    }

    async fn max_bench_ts_exact(&self, table: &str) -> Result<Option<i64>> {
        let query = format!("SELECT MAX(_bench_ts) FROM {table}");
        let rows = self
            .client
            .query(&query, &[])
            .await
            .map_err(|source| Error::Sql {
                action: format!("query MAX(_bench_ts) from {table}"),
                source,
            })?;
        if rows.is_empty() {
            return Ok(None);
        }
        let ts: Option<chrono::DateTime<chrono::Utc>> = rows[0].get(0);
        Ok(ts.map(|t| t.timestamp_micros()))
    }

    async fn row_count(&self, table: &str) -> Result<i64> {
        let query = format!("SELECT COUNT(*) FROM {table}");
        let rows = self
            .client
            .query(&query, &[])
            .await
            .map_err(|source| Error::Sql {
                action: format!("query COUNT(*) from {table}"),
                source,
            })?;
        Ok(rows.first().map_or(0, |row| row.get::<_, i64>(0)))
    }

    async fn query_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let client = self
            .arrow_client
            .get_or_try_init(|| self.build_arrow_client())
            .await?;

        let conn = client.connect_direct().await.map_err(|e| Error::Arrow {
            action: "acquire Postgres connection".into(),
            message: e.to_string(),
        })?;

        let stream = conn
            .query_arrow(sql, &[], None)
            .await
            .map_err(|e| Error::Arrow {
                action: format!("execute arrow query: {sql}"),
                message: e.to_string(),
            })?;

        stream
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| Error::Arrow {
                action: format!("collect arrow query results: {sql}"),
                message: e.to_string(),
            })
    }
}

/// Run a single OLTP terminal loop until cancelled.
async fn run_terminal(
    terminal_id: usize,
    conn_str: &str,
    stop: CancellationToken,
    assignment: txn::TerminalAssignment,
    mix: [u32; 5],
    base_seed: u64,
    rate_limiter: Option<Arc<OltpRateLimiter>>,
) -> Result<metrics::OltpMetrics> {
    let (mut client, connection) = tokio_postgres::connect(conn_str, tokio_postgres::NoTls)
        .await
        .map_err(|source| Error::Sql {
            action: format!("connect terminal {terminal_id}"),
            source,
        })?;

    let stop_conn = stop.clone();
    tokio::spawn(async move {
        tokio::select! {
            result = connection => {
                if let Err(e) = result {
                    eprintln!("Terminal {terminal_id} connection error: {e}");
                }
            }
            () = stop_conn.cancelled() => {}
        }
    });

    let stmts = txn::PreparedStatements::prepare(&client).await?;

    let mut rng = StdRng::seed_from_u64(base_seed.wrapping_add(terminal_id as u64));
    let mut metrics = metrics::OltpMetrics::new();

    loop {
        if stop.is_cancelled() {
            break;
        }

        // Wait for a slot from the shared rate limiter (work-conserving across all terminals)
        if let Some(limiter) = &rate_limiter {
            tokio::select! {
                () = limiter.until_ready() => {}
                () = stop.cancelled() => break,
            }
        }

        let txn_type = txn::pick_txn_type(&mut rng, &mix);

        match txn::execute(&mut client, &mut rng, txn_type, &assignment, &stmts).await {
            Ok(()) => {
                metrics.record_success(txn_type);
            }
            Err(e) => {
                metrics.record_abort();
                if !stop.is_cancelled() {
                    // Log only if not shutting down — connection-closed errors
                    // during shutdown are expected and noisy.
                    eprintln!("Terminal {terminal_id} {txn_type} error: {e}");
                }
            }
        }
    }

    Ok(metrics)
}

/// Pin a `MySQL` session's time zone to UTC so `NOW(3)`/`_bench_ts` writes and
/// reads line up with how the Spice CDC path interprets timestamps (which pins
/// the replication session to UTC).
pub(crate) async fn set_mysql_utc(conn: &mut mysql_async::Conn) -> Result<()> {
    conn.query_drop("SET time_zone = '+00:00'")
        .await
        .map_err(|source| Error::MySql {
            action: "set MySQL session time zone to UTC".into(),
            source,
        })
}

/// `MySQL`-backed CH-benCH driver.
///
/// Mirrors [`PostgresChBenchDriver`]: same schema/seed/OLTP/staleness contract,
/// implemented against a binlog-enabled `MySQL` source over `mysql_async`.
pub struct MysqlChBenchDriver {
    config: ChBenchConfig,
    source: MysqlSourceConfig,
    /// Parsed connection options, cloned to open per-terminal connections.
    opts: mysql_async::Opts,
    /// `MySQL` pool that returns query results as Arrow `RecordBatch`es, kept
    /// separate from the OLTP connections because the analytical-query gate
    /// needs Arrow output to compare against Spice results.
    arrow_client: OnceCell<Arc<MySQLConnectionPool>>,
    /// Per-table `_bench_ts` high-water marks, shared with every OLTP terminal.
    /// Seeded by `prepare` or `verify_prepared` before `run` spawns terminals.
    watermarks: Arc<Watermarks>,
}

impl MysqlChBenchDriver {
    /// Connect to `MySQL` using the provided configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if a connection to `MySQL` cannot be established.
    pub async fn connect(config: ChBenchConfig, source: MysqlSourceConfig) -> Result<Self> {
        let opts = source.opts();

        // Validate connectivity up front (mirrors PostgresChBenchDriver::connect).
        let mut conn = mysql_async::Conn::new(opts.clone())
            .await
            .map_err(|source| Error::MySql {
                action: "connect to MySQL".into(),
                source,
            })?;
        set_mysql_utc(&mut conn).await?;
        drop(conn);

        Ok(Self {
            config,
            source,
            opts,
            arrow_client: OnceCell::new(),
            watermarks: Arc::new(Watermarks::new()),
        })
    }

    /// Open a fresh UTC-pinned connection to the source.
    async fn new_conn(&self) -> Result<mysql_async::Conn> {
        let mut conn = mysql_async::Conn::new(self.opts.clone())
            .await
            .map_err(|source| Error::MySql {
                action: "open MySQL connection".into(),
                source,
            })?;
        set_mysql_utc(&mut conn).await?;
        Ok(conn)
    }

    /// Build the Arrow-returning connection pool from the source config.
    async fn build_arrow_client(&self) -> Result<Arc<MySQLConnectionPool>> {
        let mut params: HashMap<String, SecretBox<str>> = HashMap::new();
        params.insert("host".into(), SecretBox::from(self.source.host.clone()));
        params.insert(
            "tcp_port".into(),
            SecretBox::from(self.source.port.to_string()),
        );
        params.insert("db".into(), SecretBox::from(self.source.db.clone()));
        params.insert("user".into(), SecretBox::from(self.source.user.clone()));
        params.insert("pass".into(), SecretBox::from(self.source.pass.clone()));
        params.insert("sslmode".into(), SecretBox::from("disabled".to_string()));

        let pool = MySQLConnectionPool::new(params)
            .await
            .map_err(|e| Error::Arrow {
                action: "build MySQLConnectionPool".into(),
                message: e.to_string(),
            })?;
        Ok(Arc::new(pool))
    }

    /// Verify the source already holds a prepared CH-benCH dataset matching the
    /// configured warehouse count. Used on the `--skip-prepare` path.
    ///
    /// # Errors
    ///
    /// Returns [`Error::MySql`] if the `warehouse` table is absent and
    /// [`Error::SourceScaleMismatch`] if its row count does not match the
    /// configured scale factor.
    pub async fn verify_prepared(&self) -> Result<()> {
        let mut conn = self.new_conn().await?;
        let found: Option<i64> = conn
            .query_first("SELECT COUNT(*) FROM warehouse")
            .await
            .map_err(|source| Error::MySql {
                action: "verify --skip-prepare source (is the warehouse table seeded?)".into(),
                source,
            })?;
        let found = u64::try_from(found.unwrap_or(0)).unwrap_or(0);
        let expected = self.config.warehouses as u64;
        ensure!(
            found == expected,
            SourceScaleMismatchSnafu { found, expected }
        );
        // No schema reconciliation is needed here: the CI template fingerprint
        // includes the chbench-driver tree hash, so a --skip-prepare source was
        // seeded by this exact code. Watermarks start unseeded (the freshness
        // probe skips a table until its first committed write).
        Ok(())
    }
}

#[async_trait]
impl ChBenchDriver for MysqlChBenchDriver {
    /// Drop and recreate all CH-benCH tables, then load seed data.
    async fn prepare(&self) -> Result<()> {
        println!(
            "Preparing CH-benCHmark schema with {} warehouse(s)",
            self.config.warehouses,
        );

        // Captured before the load and used as the `_bench_ts` column default,
        // so every seed row carries exactly this value and the initial
        // watermark is known without a scan.
        let load_ts = BenchTs::now_mysql();

        let mut conn = self.new_conn().await?;
        schema_mysql::drop_tables(&mut conn).await?;
        schema_mysql::create_tables(&mut conn, load_ts).await?;
        // load_all opens its own connections from `self.opts`; `conn` is only
        // used for the DDL before and after.
        loader_mysql::load_all(&self.opts, self.config.warehouses, self.config.seed).await?;
        // Build secondary indexes *after* the bulk load so InnoDB builds each
        // B-tree once instead of maintaining it per seed row.
        schema_mysql::create_indexes(&mut conn).await?;

        // Every seed row carries load_ts, so the initial watermarks are known
        // without a scan.
        self.watermarks.seed_all(load_ts.micros());

        Ok(())
    }

    /// Run the TPC-C OLTP workload until `stop` is triggered.
    ///
    /// Each terminal opens its own `MySQL` connection and runs transactions in a
    /// tight loop with the configured mix weights.
    async fn run(&self, stop: CancellationToken) -> Result<OltpReport> {
        let terminals = self.config.terminals;
        let mix = self.config.mix;
        let warehouses = i32::try_from(self.config.warehouses).unwrap_or(1);
        let base_seed = self.config.seed.unwrap_or(42);

        let assignments = txn::TerminalAssignment::compute(terminals, warehouses);

        let rate_limiter = match self.config.rate {
            Some(rate) => {
                let cells = NonZeroU32::new(rate).ok_or(Error::InvalidRate { rate })?;
                Some(Arc::new(RateLimiter::direct(Quota::per_second(cells))))
            }
            None => None,
        };

        match self.config.rate {
            Some(rate) => println!(
                "Starting OLTP workload: {warehouses} warehouse(s), {terminals} terminals, mix={mix:?}, target rate={rate} txn/s",
            ),
            None => println!(
                "Starting OLTP workload: {warehouses} warehouse(s), {terminals} terminals, mix={mix:?}, target rate=unlimited",
            ),
        }

        let mut handles = Vec::with_capacity(terminals);
        for (terminal_id, &assignment) in assignments.iter().enumerate() {
            let opts = self.opts.clone();
            let stop = stop.clone();
            let rate_limiter = rate_limiter.clone();
            let watermarks = Arc::clone(&self.watermarks);

            handles.push(tokio::spawn(async move {
                run_terminal_mysql(
                    terminal_id,
                    opts,
                    stop,
                    assignment,
                    mix,
                    base_seed,
                    rate_limiter,
                    &watermarks,
                )
                .await
            }));
        }

        let mut combined = metrics::OltpMetrics::new();
        let mut failed_terminals: usize = 0;
        for handle in handles {
            match handle.await {
                Ok(Ok(terminal_metrics)) => combined.merge(&terminal_metrics),
                Ok(Err(e)) => {
                    eprintln!("Terminal error: {e}");
                    failed_terminals += 1;
                }
                Err(e) => {
                    eprintln!("Terminal join error: {e}");
                    failed_terminals += 1;
                }
            }
        }

        ensure!(
            failed_terminals == 0,
            OltpTerminalFailuresSnafu {
                failed: failed_terminals,
                total: terminals,
            }
        );

        Ok(combined.finish())
    }

    fn probe_tables(&self) -> &[&str] {
        schema::STALENESS_PROBE_TABLES
    }

    async fn max_bench_ts(&self, table: &str) -> Result<Option<i64>> {
        // Delete-bearing tables' true maximum can decrease, which a monotone
        // watermark cannot follow — answer those from the source (a plain scan;
        // new_order is bounded at ~9k rows per warehouse, so ~1s at SF1000).
        if watermark::is_delete_bearing(table) {
            return self.max_bench_ts_exact(table).await;
        }
        match self.watermarks.get(table) {
            // No committed write yet (--skip-prepare source probed early):
            // report "no value" so the probe skips until the first commit.
            Err(Error::UnseededWatermark { .. }) => Ok(None),
            other => other,
        }
    }

    async fn max_bench_ts_exact(&self, table: &str) -> Result<Option<i64>> {
        let mut conn = self.new_conn().await?;
        let sql = format!("SELECT MAX(_bench_ts) FROM {table}");
        let value: Option<Option<chrono::NaiveDateTime>> =
            conn.query_first(&sql)
                .await
                .map_err(|source| Error::MySql {
                    action: format!("query MAX(_bench_ts) from {table}"),
                    source,
                })?;
        // Session pinned to UTC, so the naive datetime is UTC wall-clock time.
        Ok(value.flatten().map(|dt| dt.and_utc().timestamp_micros()))
    }

    async fn row_count(&self, table: &str) -> Result<i64> {
        let mut conn = self.new_conn().await?;
        let sql = format!("SELECT COUNT(*) FROM {table}");
        let value: Option<i64> = conn
            .query_first(&sql)
            .await
            .map_err(|source| Error::MySql {
                action: format!("query COUNT(*) from {table}"),
                source,
            })?;
        Ok(value.unwrap_or(0))
    }

    async fn query_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let client = self
            .arrow_client
            .get_or_try_init(|| self.build_arrow_client())
            .await?;

        let conn = client.connect_direct().await.map_err(|e| Error::Arrow {
            action: "acquire MySQL connection".into(),
            message: e.to_string(),
        })?;

        let stream = conn
            .query_arrow(sql, &[], None)
            .await
            .map_err(|e| Error::Arrow {
                action: format!("execute arrow query: {sql}"),
                message: e.to_string(),
            })?;

        stream
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| Error::Arrow {
                action: format!("collect arrow query results: {sql}"),
                message: e.to_string(),
            })
    }
}

/// Run a single `MySQL` OLTP terminal loop until cancelled.
#[expect(clippy::too_many_arguments)]
async fn run_terminal_mysql(
    terminal_id: usize,
    opts: mysql_async::Opts,
    stop: CancellationToken,
    assignment: txn::TerminalAssignment,
    mix: [u32; 5],
    base_seed: u64,
    rate_limiter: Option<Arc<OltpRateLimiter>>,
    watermarks: &Watermarks,
) -> Result<metrics::OltpMetrics> {
    let mut conn = mysql_async::Conn::new(opts)
        .await
        .map_err(|source| Error::MySql {
            action: format!("connect terminal {terminal_id}"),
            source,
        })?;
    set_mysql_utc(&mut conn).await?;

    let mut rng = StdRng::seed_from_u64(base_seed.wrapping_add(terminal_id as u64));
    let mut metrics = metrics::OltpMetrics::new();

    loop {
        if stop.is_cancelled() {
            break;
        }

        if let Some(limiter) = &rate_limiter {
            tokio::select! {
                () = limiter.until_ready() => {}
                () = stop.cancelled() => break,
            }
        }

        let txn_type = txn::pick_txn_type(&mut rng, &mix);

        match txn::mysql::execute(&mut conn, &mut rng, txn_type, &assignment, watermarks).await {
            Ok(()) => metrics.record_success(txn_type),
            Err(e) => {
                metrics.record_abort();
                if !stop.is_cancelled() {
                    eprintln!("Terminal {terminal_id} {txn_type} error: {e}");
                }
            }
        }
    }

    Ok(metrics)
}
