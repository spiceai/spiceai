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

//! TPC-C + CH-benCH DDL for PostgreSQL.

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
    "orders",
    "new_order",
    "order_line",
    "nation",
    "region",
    "supplier",
];

/// Drop stale Spice replication slots and publications left from previous runs (if any).
pub async fn drop_replication_artifacts(client: &Client) -> Result<()> {
    // Drop replication slots named spice_*
    let rows = client
        .query(
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'spice_%'",
            &[],
        )
        .await
        .map_err(|source| crate::Error::Sql {
            action: "list replication slots".into(),
            source,
        })?;

    for row in &rows {
        let slot_name: &str = row.get(0);
        let sql = format!("SELECT pg_drop_replication_slot('{slot_name}')");
        let _unused = client.execute(&sql, &[]).await;
    }

    // Drop publications named spice_*
    let rows = client
        .query(
            "SELECT pubname FROM pg_publication WHERE pubname LIKE 'spice_%'",
            &[],
        )
        .await
        .map_err(|source| crate::Error::Sql {
            action: "list publications".into(),
            source,
        })?;

    for row in &rows {
        let pubname: &str = row.get(0);
        let sql = format!("DROP PUBLICATION IF EXISTS {pubname}");
        let _unused = client.execute(&sql, &[]).await;
    }

    if !rows.is_empty() {
        println!(
            "  cleaned up {} replication slots, {} publications",
            rows.len(),
            rows.len()
        );
    }

    Ok(())
}

/// Drop all CH-benCH tables (reverse order).
pub async fn drop_tables(client: &Client) -> Result<()> {
    drop_replication_artifacts(client).await?;
    println!("  dropping {} tables", ALL_TABLES.len());
    for table in ALL_TABLES.iter().rev() {
        let sql = format!("DROP TABLE IF EXISTS {table} CASCADE");
        client.execute(&sql, &[]).await.map_err(|source| {
            crate::Error::Sql {
                action: format!("drop table {table}"),
                source,
            }
        })?;
    }
    Ok(())
}

/// Create all 12 CH-benCH tables (9 TPC-C + 3 supplemental).
pub async fn create_tables(client: &Client) -> Result<()> {
    let ddl_statements: &[(&str, &str)] = &[
        (
            "warehouse",
            "CREATE TABLE IF NOT EXISTS warehouse (
                w_id INT NOT NULL,
                w_name VARCHAR(10),
                w_street_1 VARCHAR(20),
                w_street_2 VARCHAR(20),
                w_city VARCHAR(20),
                w_state CHAR(2),
                w_zip CHAR(9),
                w_tax DOUBLE PRECISION,
                w_ytd DOUBLE PRECISION,
                PRIMARY KEY (w_id)
            )",
        ),
        (
            "district",
            "CREATE TABLE IF NOT EXISTS district (
                d_id INT NOT NULL,
                d_w_id INT NOT NULL,
                d_name VARCHAR(10),
                d_street_1 VARCHAR(20),
                d_street_2 VARCHAR(20),
                d_city VARCHAR(20),
                d_state CHAR(2),
                d_zip CHAR(9),
                d_tax DOUBLE PRECISION,
                d_ytd DOUBLE PRECISION,
                d_next_o_id INT,
                PRIMARY KEY (d_w_id, d_id)
            )",
        ),
        (
            "customer",
            "CREATE TABLE IF NOT EXISTS customer (
                c_id INT NOT NULL,
                c_d_id INT NOT NULL,
                c_w_id INT NOT NULL,
                c_first VARCHAR(16),
                c_middle CHAR(2),
                c_last VARCHAR(16),
                c_street_1 VARCHAR(20),
                c_street_2 VARCHAR(20),
                c_city VARCHAR(20),
                c_state CHAR(2),
                c_zip CHAR(9),
                c_phone CHAR(16),
                c_since TIMESTAMP,
                c_credit CHAR(2),
                c_credit_lim DOUBLE PRECISION,
                c_discount DOUBLE PRECISION,
                c_balance DOUBLE PRECISION,
                c_ytd_payment DOUBLE PRECISION,
                c_payment_cnt INT,
                c_delivery_cnt INT,
                c_data VARCHAR(500),
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
                h_date TIMESTAMP,
                h_amount DOUBLE PRECISION,
                h_data VARCHAR(24)
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
            "orders",
            "CREATE TABLE IF NOT EXISTS orders (
                o_id INT NOT NULL,
                o_d_id INT NOT NULL,
                o_w_id INT NOT NULL,
                o_c_id INT,
                o_entry_d TIMESTAMP,
                o_carrier_id INT,
                o_ol_cnt INT,
                o_all_local INT,
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
                ol_supply_w_id INT,
                ol_delivery_d TIMESTAMP,
                ol_quantity INT,
                ol_amount DOUBLE PRECISION,
                ol_dist_info CHAR(24),
                PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
            )",
        ),
        (
            "stock",
            "CREATE TABLE IF NOT EXISTS stock (
                s_i_id INT NOT NULL,
                s_w_id INT NOT NULL,
                s_quantity INT,
                s_dist_01 CHAR(24),
                s_dist_02 CHAR(24),
                s_dist_03 CHAR(24),
                s_dist_04 CHAR(24),
                s_dist_05 CHAR(24),
                s_dist_06 CHAR(24),
                s_dist_07 CHAR(24),
                s_dist_08 CHAR(24),
                s_dist_09 CHAR(24),
                s_dist_10 CHAR(24),
                s_ytd INT,
                s_order_cnt INT,
                s_remote_cnt INT,
                s_data VARCHAR(50),
                PRIMARY KEY (s_w_id, s_i_id)
            )",
        ),
        (
            "item",
            "CREATE TABLE IF NOT EXISTS item (
                i_id INT NOT NULL,
                i_im_id INT,
                i_name VARCHAR(24),
                i_price DOUBLE PRECISION,
                i_data VARCHAR(50),
                PRIMARY KEY (i_id)
            )",
        ),
        // CH-benCH supplemental tables
        (
            "nation",
            "CREATE TABLE IF NOT EXISTS nation (
                n_nationkey BIGINT NOT NULL,
                n_name CHAR(25) NOT NULL,
                n_regionkey BIGINT NOT NULL,
                n_comment VARCHAR(152),
                PRIMARY KEY (n_nationkey)
            )",
        ),
        (
            "region",
            "CREATE TABLE IF NOT EXISTS region (
                r_regionkey BIGINT NOT NULL,
                r_name CHAR(25) NOT NULL,
                r_comment VARCHAR(152),
                PRIMARY KEY (r_regionkey)
            )",
        ),
        (
            "supplier",
            "CREATE TABLE IF NOT EXISTS supplier (
                s_suppkey BIGINT NOT NULL,
                s_name CHAR(25) NOT NULL,
                s_address VARCHAR(40) NOT NULL,
                s_nationkey BIGINT NOT NULL,
                s_phone CHAR(15) NOT NULL,
                s_acctbal DOUBLE PRECISION NOT NULL,
                s_comment VARCHAR(101) NOT NULL,
                PRIMARY KEY (s_suppkey)
            )",
        ),
    ];

    println!("  creating {} tables + 4 indexes", ddl_statements.len());
    for (table, ddl) in ddl_statements {
        client.execute(*ddl, &[]).await.map_err(|source| {
            crate::Error::Sql {
                action: format!("create table {table}"),
                source,
            }
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
            "CREATE INDEX IF NOT EXISTS idx_order ON orders (o_w_id, o_d_id, o_c_id, o_id)",
        ),
    ];

    for (name, ddl) in indexes {
        client.execute(*ddl, &[]).await.map_err(|source| {
            crate::Error::Sql {
                action: format!("create index {name}"),
                source,
            }
        })?;
    }

    Ok(())
}
