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

/// `MySQL`-specific connection configuration.
///
/// The source server must have binary logging enabled for Spice CDC
/// (`log_bin = ON`, `binlog_format = ROW`, `binlog_row_image = FULL`), and the
/// user needs `REPLICATION SLAVE` + `REPLICATION CLIENT` alongside DDL rights.
pub struct MysqlSourceConfig {
    /// `MySQL` host.
    pub host: String,

    /// `MySQL` port.
    pub port: u16,

    /// `MySQL` database name.
    pub db: String,

    /// `MySQL` user.
    pub user: String,

    /// `MySQL` password.
    pub pass: String,
}

impl Default for MysqlSourceConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 3306,
            db: "chbench".into(),
            user: "bench".into(),
            pass: "bench".into(),
        }
    }
}

impl MysqlSourceConfig {
    /// Build `mysql_async` connection options from this config.
    ///
    /// Uses `OptsBuilder` rather than a formatted `mysql://` URL so credentials
    /// or a database name containing URL-reserved characters (`@`, `:`, `/`,
    /// `#`, `%`, ...) are passed through verbatim instead of being misparsed or
    /// silently requiring percent-encoding.
    #[must_use]
    pub fn opts(&self) -> mysql_async::Opts {
        mysql_async::OptsBuilder::default()
            .ip_or_hostname(self.host.clone())
            .tcp_port(self.port)
            .user(Some(self.user.clone()))
            .pass(Some(self.pass.clone()))
            .db_name(Some(self.db.clone()))
            // The OLTP workload prepares 40+ distinct statements per terminal
            // connection (ten s_dist_XX SELECT variants, eleven order_line
            // INSERT arities, plus each transaction's fixed set). mysql_async
            // caches prepared statements per connection with an LRU capacity of
            // 32 by default, so the workload constantly evicts and re-prepares
            // statements — an extra PREPARE round trip per evicted statement.
            .stmt_cache_size(256)
            .into()
    }
}
