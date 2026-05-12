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

//! CH-benCH driver — TPC-C schema + seed data loader and OLTP workload driver for PostgreSQL.
//!
//! Creates 12 tables (9 TPC-C + 3 CH supplemental), loads seed data,
//! and runs a TPC-C OLTP workload with configurable terminals and duration.
//!
//! # Usage
//!
//! ```rust,no_run
//! use chbench_driver::{ChBenchConfig, ChBenchDriver};
//!
//! # async fn example() -> chbench_driver::Result<()> {
//! let config = ChBenchConfig { warehouses: 1, ..Default::default() };
//! let driver = ChBenchDriver::connect(config).await?;
//! driver.prepare().await?;
//! # Ok(())
//! # }
//! ```

pub mod config;
pub mod loader;
pub mod metrics;
pub mod rand;
pub mod schema;
pub mod txn;

pub use config::ChBenchConfig;
pub use metrics::OltpReport;
pub use txn::TxnType;

use std::time::Instant;

use ::rand::rngs::StdRng;
use ::rand::SeedableRng;
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

/// CH-benCH driver for TPC-C schema setup, data loading, and OLTP workload.
pub struct ChBenchDriver {
    client: tokio_postgres::Client,
    config: ChBenchConfig,
}

impl ChBenchDriver {
    /// Connect to PostgreSQL using the provided configuration.
    pub async fn connect(config: ChBenchConfig) -> Result<Self> {
        let conn_str = config.pg_connection_string();
        let (client, connection) =
            tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
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

        Ok(Self { client, config })
    }

    /// Drop and recreate all 12 CH-benCH tables, then load seed data.
    pub async fn prepare(&self) -> Result<()> {
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
    pub async fn run(&self, cancel: CancellationToken) -> Result<OltpReport> {
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
            let conn_str = self.config.pg_connection_string();
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
                    combined.merge(terminal_metrics);
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
        report.print_summary();
        Ok(report)
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
        let start = Instant::now();

        match txn::execute(&mut client, &mut rng, txn_type, warehouses).await {
            Ok(()) => {
                metrics.record_success(txn_type, start.elapsed());
            }
            Err(e) => {
                metrics.record_abort(txn_type, start.elapsed());
                // Log at debug level — aborts are expected in TPC-C
                eprintln!("Terminal {terminal_id} {txn_type} error: {e}");
            }
        }
    }

    Ok(metrics)
}
