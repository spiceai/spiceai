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

//! CH-benCH driver — TPC-C schema + seed data loader for PostgreSQL.
//!
//! Creates 12 tables (9 TPC-C + 3 CH supplemental) and loads seed data
//! for the given number of warehouses.
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
pub mod rand;
pub mod schema;

pub use config::ChBenchConfig;

use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to {action}: {source}"))]
    Sql {
        action: String,
        source: tokio_postgres::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// CH-benCH driver for TPC-C schema setup and data loading.
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
                tracing::error!("CH-benCH source PostgreSQL connection error: {e}");
            }
        });

        Ok(Self { client, config })
    }

    /// Drop and recreate all 12 CH-benCH tables, then load seed data.
    pub async fn prepare(&self) -> Result<()> {
        tracing::info!(
            "Preparing CH-benCH schema with {} warehouse(s)",
            self.config.warehouses,
        );

        schema::drop_tables(&self.client).await?;
        schema::create_tables(&self.client).await?;
        loader::load_all(&self.client, self.config.warehouses, self.config.seed).await?;

        tracing::info!("CH-benCH prepare complete");
        Ok(())
    }
}
