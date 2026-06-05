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

/// Configuration for the CH-benCH driver.
pub struct ChBenchConfig {
    /// Number of TPC-C warehouses (scale factor). Each warehouse ≈ 100 MB seed data.
    pub warehouses: usize,

    /// Optional RNG seed for deterministic data generation.
    /// When `Some(seed)`, the same seed produces the exact same dataset.
    pub seed: Option<u64>,

    /// Number of concurrent OLTP terminals for the HTAP workload.
    pub terminals: usize,

    /// Optional target transaction rate for the OLTP workload
    pub rate: Option<u32>,

    /// Transaction mix weights: \[`NewOrder`, Payment, Delivery, `OrderStatus`, `StockLevel`\].
    /// Must sum to 100.
    pub mix: [u32; 5],
}

/// Default RNG seed for deterministic data generation.
const DEFAULT_SEED: u64 = 42;

impl Default for ChBenchConfig {
    fn default() -> Self {
        Self {
            warehouses: 1,
            seed: Some(DEFAULT_SEED),
            terminals: 10,
            rate: None,
            mix: crate::txn::DEFAULT_MIX,
        }
    }
}

/// Postgres-specific connection configuration.
pub struct PostgresSourceConfig {
    /// Postgres host.
    pub host: String,

    /// Postgres port.
    pub port: u16,

    /// Postgres database name.
    pub db: String,

    /// Postgres user (must have REPLICATION privilege for Spice CDC).
    pub user: String,

    /// Postgres password.
    pub pass: String,
}

impl Default for PostgresSourceConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 5432,
            db: "chbench".into(),
            user: "bench".into(),
            pass: "bench".into(),
        }
    }
}

impl PostgresSourceConfig {
    /// Build a `tokio-postgres` connection string from this config.
    #[must_use]
    pub fn connection_string(&self) -> String {
        format!(
            "host={} port={} dbname={} user={} password={}",
            self.host, self.port, self.db, self.user, self.pass,
        )
    }
}
