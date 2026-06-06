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
pub mod loader;
pub mod metrics;
pub mod rand;
pub mod schema;
pub mod txn;

pub use config::{ChBenchConfig, PostgresSourceConfig};
pub use metrics::OltpReport;
pub use txn::TxnType;

use std::num::NonZeroU32;
use std::sync::Arc;

use ::rand::SeedableRng;
use ::rand::rngs::StdRng;
use async_trait::async_trait;
use governor::{
    Quota, RateLimiter,
    clock::DefaultClock,
    middleware::NoOpMiddleware,
    state::{InMemoryState, NotKeyed},
};
use snafu::{Snafu, ensure};
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

    #[snafu(display(
        "{failed}/{total} OLTP terminal(s) failed — benchmark results are unreliable"
    ))]
    OltpTerminalFailures { failed: usize, total: usize },

    #[snafu(display("Invalid OLTP target rate: {rate} (must be > 0)"))]
    InvalidRate { rate: u32 },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

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

    /// Read `MAX(_bench_ts)` from the *source* for a given table.
    ///
    /// Returns microseconds since Unix epoch, or `None` if the table is empty
    /// or the column doesn't exist.
    async fn max_bench_ts(&self, table: &str) -> Result<Option<i64>>;

    /// Read `COUNT(*)` from the *source* for a given table.
    async fn row_count(&self, table: &str) -> Result<i64>;
}

/// Postgres-backed CH-benCH driver.
pub struct PostgresChBenchDriver {
    client: tokio_postgres::Client,
    config: ChBenchConfig,
    source: PostgresSourceConfig,
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
        })
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
        loader::load_all(
            &self.client,
            &conn_str,
            self.config.warehouses,
            self.config.seed,
        )
        .await?;

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

    async fn max_bench_ts(&self, table: &str) -> Result<Option<i64>> {
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
