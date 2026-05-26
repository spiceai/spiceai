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

//! TPC-C + CH-benCH DDL for Postgres.

use tokio_postgres::Client;

use crate::Result;

/// All table names in creation order (respects implicit FK dependencies).
pub const ALL_TABLES: &[&str] = &[
    "warehouse",
    "district",
    "item",
    "stock",
    "customer",
    "history",
    "oorder",
    "new_order",
    "order_line",
    "nation",
    "region",
    "supplier",
];

/// Drop stale Spice replication slots and publications left from previous runs (if any).
///
/// # Errors
///
/// Returns an error if querying or dropping replication artifacts fails.
pub async fn drop_replication_artifacts(client: &Client) -> Result<()> {
    // Drop replication slots named spice_*
    let slot_rows = client
        .query(
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'spice_%'",
            &[],
        )
        .await
        .map_err(|source| crate::Error::Sql {
            action: "list replication slots".into(),
            source,
        })?;

    for row in &slot_rows {
        let slot_name: &str = row.get(0);
        let sql = format!("SELECT pg_drop_replication_slot('{slot_name}')");
        let _unused = client.execute(&sql, &[]).await;
    }

    // Drop publications named spice_*
    let pub_rows = client
        .query(
            "SELECT pubname FROM pg_publication WHERE pubname LIKE 'spice_%'",
            &[],
        )
        .await
        .map_err(|source| crate::Error::Sql {
            action: "list publications".into(),
            source,
        })?;

    for row in &pub_rows {
        let pubname: &str = row.get(0);
        let sql = format!("DROP PUBLICATION IF EXISTS {pubname}");
        let _unused = client.execute(&sql, &[]).await;
    }

    if !slot_rows.is_empty() || !pub_rows.is_empty() {
        println!(
            "  cleaned up {} replication slots, {} publications",
            slot_rows.len(),
            pub_rows.len()
        );
    }

    Ok(())
}

/// Drop all CH-benCH tables (reverse order).
///
/// # Errors
///
/// Returns an error if any table cannot be dropped.
pub async fn drop_tables(client: &Client) -> Result<()> {
    drop_replication_artifacts(client).await?;
    println!("  dropping {} tables", ALL_TABLES.len());
    for table in ALL_TABLES.iter().rev() {
        let sql = format!("DROP TABLE IF EXISTS {table} CASCADE");
        client
            .execute(&sql, &[])
            .await
            .map_err(|source| crate::Error::Sql {
                action: format!("drop table {table}"),
                source,
            })?;
    }
    Ok(())
}

/// Create all 12 CH-benCH tables (9 TPC-C + 3 supplemental).
///
/// # Errors
///
/// Returns an error if any table or index cannot be created.
#[expect(clippy::too_many_lines)]
pub async fn create_tables(client: &Client) -> Result<()> {
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
                w_tax DOUBLE PRECISION NOT NULL,
                w_ytd DOUBLE PRECISION NOT NULL,
                PRIMARY KEY (w_id)
            )",
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
                d_tax DOUBLE PRECISION NOT NULL,
                d_ytd DOUBLE PRECISION NOT NULL,
                d_next_o_id INT NOT NULL,
                PRIMARY KEY (d_w_id, d_id)
            )",
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
                c_since TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                c_credit CHAR(2) NOT NULL,
                c_credit_lim DOUBLE PRECISION NOT NULL,
                c_discount DOUBLE PRECISION NOT NULL,
                c_balance DOUBLE PRECISION NOT NULL,
                c_ytd_payment DOUBLE PRECISION NOT NULL,
                c_payment_cnt INT NOT NULL,
                c_delivery_cnt INT NOT NULL,
                c_data VARCHAR(500) NOT NULL,
                PRIMARY KEY (c_w_id, c_d_id, c_id)
            )",
        ),
        (
            "history",
            "CREATE TABLE IF NOT EXISTS history (
                h_c_id INT NOT NULL,
                h_c_d_id INT NOT NULL,
                h_c_w_id INT NOT NULL,
                h_d_id INT NOT NULL,
                h_w_id INT NOT NULL,
                h_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                h_amount DOUBLE PRECISION NOT NULL,
                h_data VARCHAR(24) NOT NULL
            )",
        ),
        (
            "new_order",
            "CREATE TABLE IF NOT EXISTS new_order (
                no_o_id INT NOT NULL,
                no_d_id INT NOT NULL,
                no_w_id INT NOT NULL,
                PRIMARY KEY (no_w_id, no_d_id, no_o_id)
            )",
        ),
        (
            "oorder",
            "CREATE TABLE IF NOT EXISTS oorder (
                o_id INT NOT NULL,
                o_d_id INT NOT NULL,
                o_w_id INT NOT NULL,
                o_c_id INT NOT NULL,
                o_entry_d TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                o_carrier_id INT DEFAULT NULL,
                o_ol_cnt INT NOT NULL,
                o_all_local INT NOT NULL,
                PRIMARY KEY (o_w_id, o_d_id, o_id)
            )",
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
                ol_delivery_d TIMESTAMP NULL DEFAULT NULL,
                ol_quantity INT NOT NULL,
                ol_amount DOUBLE PRECISION NOT NULL,
                ol_dist_info CHAR(24) NOT NULL,
                PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
            )",
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
            )",
        ),
        (
            "item",
            "CREATE TABLE IF NOT EXISTS item (
                i_id INT NOT NULL,
                i_im_id INT NOT NULL,
                i_name VARCHAR(24) NOT NULL,
                i_price DOUBLE PRECISION NOT NULL,
                i_data VARCHAR(50) NOT NULL,
                PRIMARY KEY (i_id)
            )",
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
            )",
        ),
        (
            "region",
            "CREATE TABLE IF NOT EXISTS region (
                r_regionkey BIGINT NOT NULL,
                r_name VARCHAR(25) NOT NULL,
                r_comment VARCHAR(152) NOT NULL,
                PRIMARY KEY (r_regionkey)
            )",
        ),
        (
            "supplier",
            "CREATE TABLE IF NOT EXISTS supplier (
                su_suppkey BIGINT NOT NULL,
                su_name VARCHAR(25) NOT NULL,
                su_address VARCHAR(40) NOT NULL,
                su_nationkey BIGINT NOT NULL,
                su_phone VARCHAR(15) NOT NULL,
                su_acctbal DOUBLE PRECISION NOT NULL,
                su_comment VARCHAR(101) NOT NULL,
                PRIMARY KEY (su_suppkey)
            )",
        ),
    ];

    println!("  creating {} tables + 4 indexes", ddl_statements.len());
    for (table, ddl) in ddl_statements {
        client
            .execute(*ddl, &[])
            .await
            .map_err(|source| crate::Error::Sql {
                action: format!("create table {table}"),
                source,
            })?;
    }

    // Indexes (matching go-tpc Postgres DDL)
    let indexes: &[(&str, &str)] = &[
        (
            "idx_customer",
            "CREATE INDEX IF NOT EXISTS idx_customer ON customer (c_w_id, c_d_id, c_last, c_first)",
        ),
        (
            "idx_h_w_id",
            "CREATE INDEX IF NOT EXISTS idx_h_w_id ON history (h_w_id)",
        ),
        (
            "idx_h_c_w_id",
            "CREATE INDEX IF NOT EXISTS idx_h_c_w_id ON history (h_c_w_id)",
        ),
        (
            "idx_order",
            "CREATE INDEX IF NOT EXISTS idx_order ON oorder (o_w_id, o_d_id, o_c_id, o_id)",
        ),
    ];

    for (name, ddl) in indexes {
        client
            .execute(*ddl, &[])
            .await
            .map_err(|source| crate::Error::Sql {
                action: format!("create index {name}"),
                source,
            })?;
    }

    // Add _bench_ts column and trigger to all mutated TPC-C tables.
    // Used for staleness gap measurement between Postgres and Spice accelerated copy.
    add_bench_ts_column_and_triggers(client).await?;

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

/// Tables probed for staleness gap measurement (alphabetical).
/// All mutated TPC-C tables that are accelerated/replicated by Spice.
/// Excludes static reference tables (`item`, `nation`, `region`, `supplier`)
/// and `history` (no primary key, not supported for CDC replication).
pub const STALENESS_PROBE_TABLES: &[&str] = &[
    "customer",
    "district",
    "new_order",
    "order_line",
    "oorder",
    "stock",
    "warehouse",
];

/// Add `_bench_ts TIMESTAMPTZ` column with `clock_timestamp()` default and
/// a `BEFORE INSERT OR UPDATE` trigger to all mutated TPC-C tables.
///
/// Uses `clock_timestamp()` (wall-clock time per statement) instead of `now()`
/// (transaction-start time) for accurate per-row timing.
async fn add_bench_ts_column_and_triggers(client: &Client) -> Result<()> {
    // Create the shared trigger function once.
    client
        .execute(
            "CREATE OR REPLACE FUNCTION bench_ts_trigger()
             RETURNS TRIGGER AS $$
             BEGIN
                 NEW._bench_ts := clock_timestamp();
                 RETURN NEW;
             END;
             $$ LANGUAGE plpgsql",
            &[],
        )
        .await
        .map_err(|source| crate::Error::Sql {
            action: "create bench_ts_trigger function".into(),
            source,
        })?;

    for table in MUTATED_TABLES {
        // Add column with default for seed data rows.
        let add_col = format!(
            "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS _bench_ts TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()"
        );
        client
            .execute(&add_col, &[])
            .await
            .map_err(|source| crate::Error::Sql {
                action: format!("add _bench_ts column to {table}"),
                source,
            })?;

        // Attach the trigger (idempotent via DROP IF EXISTS + CREATE).
        let trigger_name = format!("trg_bench_ts_{table}");
        let drop_trg = format!("DROP TRIGGER IF EXISTS {trigger_name} ON {table}");
        client
            .execute(&drop_trg, &[])
            .await
            .map_err(|source| crate::Error::Sql {
                action: format!("drop trigger {trigger_name}"),
                source,
            })?;

        let create_trg = format!(
            "CREATE TRIGGER {trigger_name} BEFORE INSERT OR UPDATE ON {table} FOR EACH ROW EXECUTE FUNCTION bench_ts_trigger()"
        );
        client
            .execute(&create_trg, &[])
            .await
            .map_err(|source| crate::Error::Sql {
                action: format!("create trigger {trigger_name}"),
                source,
            })?;
    }

    println!(
        "  added _bench_ts column + trigger to {} tables",
        MUTATED_TABLES.len()
    );
    Ok(())
}
