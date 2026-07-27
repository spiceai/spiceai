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

//! TPC-C + CH-benCH DDL for `MySQL`.
//!
//! Mirror of the Postgres [`crate::schema`] module. Reuses
//! [`crate::schema::ALL_TABLES`] for the table set/order, but emits `MySQL`
//! dialect DDL. Postgres-WAL-specific replication cleanup has no `MySQL`
//! equivalent and is omitted.

use std::time::Instant;

use mysql_async::prelude::Queryable;

use crate::Result;
use crate::watermark::{BenchTs, MUTATED_TABLES};

/// Drop all CH-benCH tables.
///
/// `MySQL` has no `DROP TABLE ... CASCADE`, so foreign-key checks are disabled
/// for the duration of the drop instead. Drops the same set of tables as the
/// Postgres [`crate::schema::drop_tables`].
///
/// # Errors
///
/// Returns an error if any table cannot be dropped.
pub async fn drop_tables(conn: &mut mysql_async::Conn) -> Result<()> {
    println!("  dropping {} tables", crate::schema::ALL_TABLES.len());

    // Bound metadata-lock waits. A `DROP TABLE` blocked by another session holding
    // a lock otherwise waits up to the server default `lock_wait_timeout`
    // (31536000s ≈ 1 year), so a single stuck drop silently consumes the whole CI
    // job budget. 60s makes a blocked drop fail fast with a clear
    // "Lock wait timeout exceeded" error instead of hanging.
    conn.query_drop("SET SESSION lock_wait_timeout = 60")
        .await
        .map_err(|source| crate::Error::MySql {
            action: "set lock_wait_timeout".into(),
            source,
        })?;

    // A prior run killed abruptly (e.g. the CI runner was terminated mid-run) can
    // leave sessions still holding metadata locks on the tables we are about to
    // drop, which is exactly what blocks the drop above. Terminate any other
    // sessions before dropping. Without PROCESS/CONNECTION_ADMIN this account sees
    // and can KILL only its own threads — precisely the CDC/loader connections a
    // previous run may have leaked. Best-effort: a session may already be gone
    // (killed/timed out) between the SELECT and the KILL, which is fine.
    let stale: Vec<u64> = conn
        .query("SELECT ID FROM information_schema.PROCESSLIST WHERE ID <> CONNECTION_ID()")
        .await
        .map_err(|source| crate::Error::MySql {
            action: "list stale sessions".into(),
            source,
        })?;
    if !stale.is_empty() {
        println!(
            "  terminating {} stale MySQL session(s) before drop",
            stale.len()
        );
        for id in stale {
            if let Err(e) = conn.query_drop(format!("KILL {id}")).await {
                eprintln!("  KILL {id} skipped (session likely already gone): {e}");
            }
        }
    }

    conn.query_drop("SET FOREIGN_KEY_CHECKS=0")
        .await
        .map_err(|source| crate::Error::MySql {
            action: "disable foreign key checks".into(),
            source,
        })?;

    for table in crate::schema::ALL_TABLES.iter().rev() {
        let started = Instant::now();
        let sql = format!("DROP TABLE IF EXISTS {table}");
        conn.query_drop(&sql)
            .await
            .map_err(|source| crate::Error::MySql {
                action: format!("drop table {table}"),
                source,
            })?;
        // Per-table timing so a future hang is immediately attributable to the
        // exact table (and therefore the lock) it stuck on.
        println!(
            "    dropped {table} ({:.1}s)",
            started.elapsed().as_secs_f64()
        );
    }

    conn.query_drop("SET FOREIGN_KEY_CHECKS=1")
        .await
        .map_err(|source| crate::Error::MySql {
            action: "enable foreign key checks".into(),
            source,
        })?;

    Ok(())
}

/// Create all 12 CH-benCH tables (9 TPC-C + 3 supplemental) and add the
/// `_bench_ts` columns. Secondary indexes are created separately
/// ([`create_indexes`]) *after* the bulk load.
///
/// # Errors
///
/// Returns an error if any table or `_bench_ts` column cannot be created.
pub async fn create_tables(conn: &mut mysql_async::Conn, load_ts: BenchTs) -> Result<()> {
    let ddl_statements: &[(&str, &str)] = &[
        (
            "warehouse",
            "CREATE TABLE IF NOT EXISTS warehouse (
                w_id INT NOT NULL,
                w_name VARCHAR(10) NOT NULL,
                w_street_1 VARCHAR(20) NOT NULL,
                w_street_2 VARCHAR(20) NOT NULL,
                w_city VARCHAR(20) NOT NULL,
                w_state CHAR(2) NOT NULL,
                w_zip CHAR(9) NOT NULL,
                w_tax DECIMAL(4,4) NOT NULL,
                w_ytd DECIMAL(12,2) NOT NULL,
                PRIMARY KEY (w_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs",
        ),
        (
            "district",
            "CREATE TABLE IF NOT EXISTS district (
                d_id INT NOT NULL,
                d_w_id INT NOT NULL,
                d_name VARCHAR(10) NOT NULL,
                d_street_1 VARCHAR(20) NOT NULL,
                d_street_2 VARCHAR(20) NOT NULL,
                d_city VARCHAR(20) NOT NULL,
                d_state CHAR(2) NOT NULL,
                d_zip CHAR(9) NOT NULL,
                d_tax DECIMAL(4,4) NOT NULL,
                d_ytd DECIMAL(12,2) NOT NULL,
                d_next_o_id INT NOT NULL,
                PRIMARY KEY (d_w_id, d_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs",
        ),
        (
            "customer",
            "CREATE TABLE IF NOT EXISTS customer (
                c_id INT NOT NULL,
                c_d_id INT NOT NULL,
                c_w_id INT NOT NULL,
                c_first VARCHAR(16) NOT NULL,
                c_middle CHAR(2) NOT NULL,
                c_last VARCHAR(16) NOT NULL,
                c_street_1 VARCHAR(20) NOT NULL,
                c_street_2 VARCHAR(20) NOT NULL,
                c_city VARCHAR(20) NOT NULL,
                c_state CHAR(2) NOT NULL,
                c_zip CHAR(9) NOT NULL,
                c_phone CHAR(16) NOT NULL,
                c_since DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                c_credit CHAR(2) NOT NULL,
                c_credit_lim DECIMAL(12,2) NOT NULL,
                c_discount DECIMAL(4,4) NOT NULL,
                c_balance DECIMAL(12,2) NOT NULL,
                c_ytd_payment DOUBLE NOT NULL,
                c_payment_cnt INT NOT NULL,
                c_delivery_cnt INT NOT NULL,
                c_data VARCHAR(500) NOT NULL,
                PRIMARY KEY (c_w_id, c_d_id, c_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs",
        ),
        (
            "history",
            "CREATE TABLE IF NOT EXISTS history (
                h_c_id INT NOT NULL,
                h_c_d_id INT NOT NULL,
                h_c_w_id INT NOT NULL,
                h_d_id INT NOT NULL,
                h_w_id INT NOT NULL,
                h_date DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                h_amount DECIMAL(6,2) NOT NULL,
                h_data VARCHAR(24) NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs",
        ),
        (
            "new_order",
            "CREATE TABLE IF NOT EXISTS new_order (
                no_o_id INT NOT NULL,
                no_d_id INT NOT NULL,
                no_w_id INT NOT NULL,
                PRIMARY KEY (no_w_id, no_d_id, no_o_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs",
        ),
        (
            "oorder",
            "CREATE TABLE IF NOT EXISTS oorder (
                o_id INT NOT NULL,
                o_d_id INT NOT NULL,
                o_w_id INT NOT NULL,
                o_c_id INT NOT NULL,
                o_entry_d DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                o_carrier_id INT DEFAULT NULL,
                o_ol_cnt INT NOT NULL,
                o_all_local INT NOT NULL,
                PRIMARY KEY (o_w_id, o_d_id, o_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs",
        ),
        (
            "order_line",
            "CREATE TABLE IF NOT EXISTS order_line (
                ol_o_id INT NOT NULL,
                ol_d_id INT NOT NULL,
                ol_w_id INT NOT NULL,
                ol_number INT NOT NULL,
                ol_i_id INT NOT NULL,
                ol_supply_w_id INT NOT NULL,
                ol_delivery_d DATETIME(6) NULL DEFAULT NULL,
                ol_quantity INT NOT NULL,
                ol_amount DECIMAL(6,2) NOT NULL,
                ol_dist_info CHAR(24) NOT NULL,
                PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs",
        ),
        (
            "stock",
            "CREATE TABLE IF NOT EXISTS stock (
                s_i_id INT NOT NULL,
                s_w_id INT NOT NULL,
                s_quantity INT NOT NULL,
                s_dist_01 CHAR(24) NOT NULL,
                s_dist_02 CHAR(24) NOT NULL,
                s_dist_03 CHAR(24) NOT NULL,
                s_dist_04 CHAR(24) NOT NULL,
                s_dist_05 CHAR(24) NOT NULL,
                s_dist_06 CHAR(24) NOT NULL,
                s_dist_07 CHAR(24) NOT NULL,
                s_dist_08 CHAR(24) NOT NULL,
                s_dist_09 CHAR(24) NOT NULL,
                s_dist_10 CHAR(24) NOT NULL,
                s_ytd INT NOT NULL,
                s_order_cnt INT NOT NULL,
                s_remote_cnt INT NOT NULL,
                s_data VARCHAR(50) NOT NULL,
                PRIMARY KEY (s_w_id, s_i_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs",
        ),
        (
            "item",
            "CREATE TABLE IF NOT EXISTS item (
                i_id INT NOT NULL,
                i_im_id INT NOT NULL,
                i_name VARCHAR(24) NOT NULL,
                i_price DECIMAL(5,2) NOT NULL,
                i_data VARCHAR(50) NOT NULL,
                PRIMARY KEY (i_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs",
        ),
        // CH-benCH supplemental tables
        (
            "nation",
            "CREATE TABLE IF NOT EXISTS nation (
                n_nationkey BIGINT NOT NULL,
                n_name VARCHAR(25) NOT NULL,
                n_regionkey BIGINT NOT NULL,
                n_comment VARCHAR(152) NOT NULL,
                PRIMARY KEY (n_nationkey)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs",
        ),
        (
            "region",
            "CREATE TABLE IF NOT EXISTS region (
                r_regionkey BIGINT NOT NULL,
                r_name VARCHAR(25) NOT NULL,
                r_comment VARCHAR(152) NOT NULL,
                PRIMARY KEY (r_regionkey)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs",
        ),
        (
            "supplier",
            "CREATE TABLE IF NOT EXISTS supplier (
                su_suppkey BIGINT NOT NULL,
                su_name VARCHAR(25) NOT NULL,
                su_address VARCHAR(40) NOT NULL,
                su_nationkey BIGINT NOT NULL,
                su_phone VARCHAR(15) NOT NULL,
                su_acctbal DECIMAL(12,2) NOT NULL,
                su_comment VARCHAR(101) NOT NULL,
                PRIMARY KEY (su_suppkey)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs",
        ),
    ];

    println!("  creating {} tables", ddl_statements.len());
    for (table, ddl) in ddl_statements {
        conn.query_drop(*ddl)
            .await
            .map_err(|source| crate::Error::MySql {
                action: format!("create table {table}"),
                source,
            })?;
    }

    // Add the _bench_ts column to all mutated TPC-C tables with a constant
    // default of `load_ts`, so every seed row carries a known stamp and the
    // initial watermarks need no scan (see `add_bench_ts_columns`).
    add_bench_ts_columns(conn, load_ts).await?;

    Ok(())
}

/// Create the 4 secondary indexes (matching go-tpc `MySQL` DDL).
///
/// Called *after* the bulk load so `InnoDB` builds each index once via its sorted
/// bulk-index build, instead of maintaining the B-trees incrementally on every
/// seed load insert.
///
/// # Errors
///
/// Returns an error if any index cannot be created.
pub async fn create_indexes(conn: &mut mysql_async::Conn) -> Result<()> {
    let indexes: &[(&str, &str)] = &[
        (
            "idx_customer",
            "CREATE INDEX idx_customer ON customer (c_w_id, c_d_id, c_last, c_first)",
        ),
        ("idx_h_w_id", "CREATE INDEX idx_h_w_id ON history (h_w_id)"),
        (
            "idx_h_c_w_id",
            "CREATE INDEX idx_h_c_w_id ON history (h_c_w_id)",
        ),
        (
            "idx_order",
            "CREATE INDEX idx_order ON oorder (o_w_id, o_d_id, o_c_id, o_id)",
        ),
    ];

    println!("  creating {} secondary indexes", indexes.len());
    for (name, ddl) in indexes {
        conn.query_drop(*ddl)
            .await
            .map_err(|source| crate::Error::MySql {
                action: format!("create index {name}"),
                source,
            })?;
    }

    Ok(())
}

/// Add the `_bench_ts DATETIME(3)` column to all mutated TPC-C tables with a
/// constant `load_ts` default, so every seed row carries a known stamp and the
/// initial watermarks need no scan. Live statements always bind `_bench_ts`
/// explicitly, so the default is never consulted after the load; a forgotten
/// binding is caught by the watermark-equals-source e2e test.
async fn add_bench_ts_columns(conn: &mut mysql_async::Conn, load_ts: BenchTs) -> Result<()> {
    let default = load_ts.mysql_literal();
    for table in MUTATED_TABLES {
        // Tables are freshly created by `create_tables`, so a plain ADD COLUMN
        // is safe (MySQL has no reliable ADD COLUMN IF NOT EXISTS on older
        // versions).
        let add_col = format!(
            "ALTER TABLE {table} ADD COLUMN _bench_ts DATETIME(3) NOT NULL DEFAULT {default}"
        );
        conn.query_drop(&add_col)
            .await
            .map_err(|source| crate::Error::MySql {
                action: format!("add _bench_ts column to {table}"),
                source,
            })?;
    }

    println!(
        "  added _bench_ts column to {} tables (seed default {default})",
        MUTATED_TABLES.len()
    );
    Ok(())
}
