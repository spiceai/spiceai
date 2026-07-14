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

use mysql_async::prelude::Queryable;

use crate::Result;

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

    conn.query_drop("SET FOREIGN_KEY_CHECKS=0")
        .await
        .map_err(|source| crate::Error::MySql {
            action: "disable foreign key checks".into(),
            source,
        })?;

    for table in crate::schema::ALL_TABLES.iter().rev() {
        let sql = format!("DROP TABLE IF EXISTS {table}");
        conn.query_drop(&sql)
            .await
            .map_err(|source| crate::Error::MySql {
                action: format!("drop table {table}"),
                source,
            })?;
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
/// `_bench_ts` columns. Secondary indexes and triggers are created separately
/// (see [`create_indexes`] and [`create_triggers`]) *after* the bulk load.
///
/// # Errors
///
/// Returns an error if any table or `_bench_ts` column cannot be created.
pub async fn create_tables(conn: &mut mysql_async::Conn) -> Result<()> {
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
                w_tax DOUBLE NOT NULL,
                w_ytd DOUBLE NOT NULL,
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
                d_tax DOUBLE NOT NULL,
                d_ytd DOUBLE NOT NULL,
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
                c_credit_lim DOUBLE NOT NULL,
                c_discount DOUBLE NOT NULL,
                c_balance DOUBLE NOT NULL,
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
                h_amount DOUBLE NOT NULL,
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
                ol_amount DOUBLE NOT NULL,
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
                i_price DOUBLE NOT NULL,
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
                su_acctbal DOUBLE NOT NULL,
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

    // Add the _bench_ts column (with default) to all mutated TPC-C tables so the
    // seed rows are stamped by the column default. The BEFORE INSERT/UPDATE
    // triggers are created *after* the load (see `create_triggers`) so they do
    // not fire per-row during the bulk seed load.
    add_bench_ts_columns(conn).await?;

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

/// Tables mutated by TPC-C transactions. `_bench_ts` is added only to these.
/// `item`, `nation`, `region`, `supplier` are static reference tables.
const MUTATED_TABLES: &[&str] = &[
    "warehouse",
    "district",
    "customer",
    "history",
    "new_order",
    "oorder",
    "order_line",
    "stock",
];

/// Add a `_bench_ts DATETIME(3)` column defaulting to `CURRENT_TIMESTAMP(3)` to
/// all mutated TPC-C tables. The default stamps the seed rows; the per-row
/// triggers for live mutations are created separately by [`create_triggers`]
/// *after* the load.
async fn add_bench_ts_columns(conn: &mut mysql_async::Conn) -> Result<()> {
    for table in MUTATED_TABLES {
        // Add column with default for seed data rows. Tables are freshly
        // created by `create_tables`, so a plain ADD COLUMN is safe (MySQL
        // has no reliable ADD COLUMN IF NOT EXISTS on older versions).
        let add_col = format!(
            "ALTER TABLE {table} ADD COLUMN _bench_ts DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)"
        );
        conn.query_drop(&add_col)
            .await
            .map_err(|source| crate::Error::MySql {
                action: format!("add _bench_ts column to {table}"),
                source,
            })?;
    }

    println!(
        "  added _bench_ts column to {} tables",
        MUTATED_TABLES.len()
    );
    Ok(())
}

/// Create the `_bench_ts` `BEFORE INSERT` and `BEFORE UPDATE` triggers on all
/// mutated TPC-C tables. Called *after* the bulk load so the triggers do not
/// fire per-row during the seed load — the seed rows are already stamped by
/// the column default (see [`add_bench_ts_columns`]).
///
/// `MySQL` cannot combine INSERT and UPDATE into a single trigger, so two
/// triggers are created per table. `NOW(3)` provides millisecond wall-clock
/// timing per row.
///
/// # Errors
///
/// Returns an error if any trigger cannot be created.
pub async fn create_triggers(conn: &mut mysql_async::Conn) -> Result<()> {
    for table in MUTATED_TABLES {
        // Attach the INSERT trigger (idempotent via DROP IF EXISTS + CREATE).
        let ins_trigger = format!("trg_bench_ts_ins_{table}");
        let drop_ins = format!("DROP TRIGGER IF EXISTS {ins_trigger}");
        conn.query_drop(&drop_ins)
            .await
            .map_err(|source| crate::Error::MySql {
                action: format!("drop trigger {ins_trigger}"),
                source,
            })?;

        let create_ins = format!(
            "CREATE TRIGGER {ins_trigger} BEFORE INSERT ON {table} FOR EACH ROW SET NEW._bench_ts = NOW(3)"
        );
        conn.query_drop(&create_ins)
            .await
            .map_err(|source| crate::Error::MySql {
                action: format!("create trigger {ins_trigger}"),
                source,
            })?;

        // Attach the UPDATE trigger (idempotent via DROP IF EXISTS + CREATE).
        let upd_trigger = format!("trg_bench_ts_upd_{table}");
        let drop_upd = format!("DROP TRIGGER IF EXISTS {upd_trigger}");
        conn.query_drop(&drop_upd)
            .await
            .map_err(|source| crate::Error::MySql {
                action: format!("drop trigger {upd_trigger}"),
                source,
            })?;

        let create_upd = format!(
            "CREATE TRIGGER {upd_trigger} BEFORE UPDATE ON {table} FOR EACH ROW SET NEW._bench_ts = NOW(3)"
        );
        conn.query_drop(&create_upd)
            .await
            .map_err(|source| crate::Error::MySql {
                action: format!("create trigger {upd_trigger}"),
                source,
            })?;
    }

    println!(
        "  added _bench_ts triggers to {} tables",
        MUTATED_TABLES.len()
    );
    Ok(())
}
