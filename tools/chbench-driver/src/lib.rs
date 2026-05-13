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
//! and runs a TPC-C OLTP workload with configurable terminals and duration.

pub mod config;
pub mod loader;
pub mod metrics;
pub mod rand;
pub mod schema;
pub mod txn;

pub use config::{ChBenchConfig, PostgresSourceConfig};
pub use metrics::OltpReport;
pub use txn::TxnType;

use ::rand::SeedableRng;
use ::rand::rngs::StdRng;
use async_trait::async_trait;
use snafu::Snafu;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to {action}: {source}"))]
    Sql {
        action: String,
        source: tokio_postgres::Error,
    },
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

    /// Run the TPC-C OLTP workload until `cancel` fires or the configured
    /// duration elapses.
    async fn run(&self, cancel: CancellationToken) -> Result<OltpReport>;

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
            "Preparing CH-benCH schema with {} warehouse(s)",
            self.config.warehouses,
        );

        schema::drop_tables(&self.client).await?;
        schema::create_tables(&self.client).await?;
        loader::load_all(&self.client, self.config.warehouses, self.config.seed).await?;

        println!("CH-benCH prepare complete");
        Ok(())
    }

    /// Run the TPC-C OLTP workload until the cancellation token is triggered
    /// or `config.duration` elapses.
    ///
    /// Each terminal opens its own Postgres connection and runs transactions
    /// in a tight loop with the configured mix weights.
    async fn run(&self, cancel: CancellationToken) -> Result<OltpReport> {
        let terminals = self.config.terminals;
        let duration = self.config.duration;
        let mix = self.config.mix;
        let warehouses = i32::try_from(self.config.warehouses).unwrap_or(1);
        let base_seed = self.config.seed.unwrap_or(42);

        println!(
            "Starting OLTP workload: {} terminals, {}s duration, mix={:?}",
            terminals,
            duration.as_secs(),
            mix,
        );

        // Spawn a task that cancels after the configured duration
        let duration_cancel = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            duration_cancel.cancel();
        });

        let mut handles = Vec::with_capacity(terminals);
        for terminal_id in 0..terminals {
            let conn_str = self.source.connection_string();
            let cancel = cancel.clone();

            handles.push(tokio::spawn(async move {
                run_terminal(terminal_id, &conn_str, cancel, warehouses, mix, base_seed).await
            }));
        }

        // Collect results from all terminals
        let mut combined = metrics::OltpMetrics::new();
        for handle in handles {
            match handle.await {
                Ok(Ok(terminal_metrics)) => {
                    combined.merge(&terminal_metrics);
                }
                Ok(Err(e)) => {
                    eprintln!("Terminal error: {e}");
                }
                Err(e) => {
                    eprintln!("Terminal join error: {e}");
                }
            }
        }

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
}

/// Run a single OLTP terminal loop until cancelled.
async fn run_terminal(
    terminal_id: usize,
    conn_str: &str,
    cancel: CancellationToken,
    warehouses: i32,
    mix: [u32; 5],
    base_seed: u64,
) -> Result<metrics::OltpMetrics> {
    let (mut client, connection) = tokio_postgres::connect(conn_str, tokio_postgres::NoTls)
        .await
        .map_err(|source| Error::Sql {
            action: format!("connect terminal {terminal_id}"),
            source,
        })?;

    let cancel_conn = cancel.clone();
    tokio::spawn(async move {
        tokio::select! {
            result = connection => {
                if let Err(e) = result {
                    eprintln!("Terminal {terminal_id} connection error: {e}");
                }
            }
            () = cancel_conn.cancelled() => {}
        }
    });

    let mut rng = StdRng::seed_from_u64(base_seed.wrapping_add(terminal_id as u64));
    let mut metrics = metrics::OltpMetrics::new();

    loop {
        if cancel.is_cancelled() {
            break;
        }

        let txn_type = txn::pick_txn_type(&mut rng, &mix);

        match txn::execute(&mut client, &mut rng, txn_type, warehouses).await {
            Ok(()) => {
                metrics.record_success(txn_type);
            }
            Err(e) => {
                metrics.record_abort();
                if !cancel.is_cancelled() {
                    // Log only if not shutting down — connection-closed errors
                    // during cancellation are expected and noisy.
                    eprintln!("Terminal {terminal_id} {txn_type} error: {e}");
                }
            }
        }
    }

    Ok(metrics)
}
