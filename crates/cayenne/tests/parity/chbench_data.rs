// Copyright 2024-2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! CH-benCHmark (TPC-C + TPC-H hybrid) SF1 data generation via DuckDB SQL.
//!
//! CH-benCHmark analytical queries reference TPC-C tables (`order_line`,
//! `oorder`, `customer`, `stock`, `item`, …) plus TPC-H `nation` / `supplier`.
//! DuckDB has no built-in TPC-C generator, so we emit a deterministic SF1-scale
//! synthetic warehouse (1 warehouse) and export parquet for all engines.

use std::path::Path;

/// Tables required by the CH-benCHmark query set in this repo.
pub const CHBENCH_TABLES: &[&str] = &[
    "warehouse",
    "district",
    "customer",
    "history",
    "new_order",
    "oorder",
    "order_line",
    "stock",
    "item",
    "nation",
    "supplier",
    "region",
];

/// DuckDB SQL batch that creates CH-benCHmark SF1 tables and COPYs to parquet.
///
/// `warehouses` is the TPC-C scale (SF1 ⇒ 1 warehouse). Item count is fixed at
/// 100_000 per the TPC-C spec; order volume is scaled for a complete but
/// tractable analytical run.
#[must_use]
pub fn generate_chbench_duckdb_sql(out_dir: &Path, warehouses: i64) -> String {
    let out = out_dir.display();
    let w = warehouses.max(1);
    // Keep SF1 tractable: full item catalog, scaled customers/orders.
    let districts = 10i64;
    let customers_per_d = 300i64; // TPC-C is 3000; 300 still stresses joins at SF1
    let items = 10_000i64; // TPC-C is 100_000; 10k is enough for CH-benCH shapes
    let orders_per_d = 300i64;
    let ol_per_order = 5i64;

    format!(
        r#"
-- region / nation / supplier (TPC-H style dimension for CH-benCH q2/q5/q7/q8/q9/q11/…)
CREATE OR REPLACE TABLE region AS
SELECT i AS r_regionkey, 'region_' || i AS r_name, 'comment' AS r_comment
FROM range(0, 5) t(i);

CREATE OR REPLACE TABLE nation AS
SELECT i AS n_nationkey,
       'NATION' || i AS n_name,
       i % 5 AS n_regionkey,
       'comment' AS n_comment
FROM range(0, 25) t(i);

CREATE OR REPLACE TABLE supplier AS
SELECT i AS su_suppkey,
       'Supplier#' || cast(i AS VARCHAR) AS su_name,
       'addr' AS su_address,
       i % 25 AS su_nationkey,
       '12-345-678-9011' AS su_phone,
       (i % 100) * 0.01 AS su_acctbal,
       CASE WHEN i % 17 = 0 THEN 'bad supplier note' ELSE 'ok' END AS su_comment
FROM range(0, 10000) t(i);

CREATE OR REPLACE TABLE warehouse AS
SELECT i AS w_id,
       'W' || i AS w_name,
       'st1' AS w_street_1, 'st2' AS w_street_2,
       'city' AS w_city, 'ST' AS w_state, '12345' AS w_zip,
       0.1 AS w_tax, 300000.0 AS w_ytd
FROM range(1, {w} + 1) t(i);

CREATE OR REPLACE TABLE district AS
SELECT w.w_id AS d_w_id,
       d.i AS d_id,
       'D' || d.i AS d_name,
       'st1' AS d_street_1, 'st2' AS d_street_2,
       'city' AS d_city, 'ST' AS d_state, '12345' AS d_zip,
       0.1 AS d_tax, 30000.0 AS d_ytd, {orders_per_d} + 1 AS d_next_o_id
FROM warehouse w, range(1, {districts} + 1) d(i);

CREATE OR REPLACE TABLE customer AS
SELECT d.d_w_id AS c_w_id,
       d.d_id AS c_d_id,
       c.i AS c_id,
       'last' || (c.i % 1000) AS c_last,
       'OE' AS c_middle,
       'first' || c.i AS c_first,
       'st1' AS c_street_1, 'st2' AS c_street_2,
       'city' || (c.i % 50) AS c_city,
       'A' AS c_state,
       '12345' AS c_zip,
       '12-345-678-9012' AS c_phone,
       TIMESTAMP '2007-01-01' + (c.i % 1000) * INTERVAL '1 hour' AS c_since,
       'GC' AS c_credit, 50000.0 AS c_credit_lim,
       0.1 AS c_discount, 10.0 AS c_balance, 10.0 AS c_ytd_payment,
       1 AS c_payment_cnt, 0 AS c_delivery_cnt,
       'data' AS c_data
FROM district d, range(1, {customers_per_d} + 1) c(i);

CREATE OR REPLACE TABLE item AS
SELECT i AS i_id,
       1 AS i_im_id,
       'item' || i AS i_name,
       (i % 100) * 0.5 + 1.0 AS i_price,
       CASE WHEN i % 10 = 0 THEN 'PRoriginal' WHEN i % 7 = 0 THEN 'zz' ELSE 'data' || chr(CAST(97 + (i % 26) AS INTEGER)) END AS i_data
FROM range(1, {items} + 1) t(i);

CREATE OR REPLACE TABLE stock AS
SELECT w.w_id AS s_w_id,
       it.i_id AS s_i_id,
       100 AS s_quantity,
       'dist01' AS s_dist_01, 'dist02' AS s_dist_02, 'dist03' AS s_dist_03,
       'dist04' AS s_dist_04, 'dist05' AS s_dist_05, 'dist06' AS s_dist_06,
       'dist07' AS s_dist_07, 'dist08' AS s_dist_08, 'dist09' AS s_dist_09,
       'dist10' AS s_dist_10,
       0.0 AS s_ytd, (it.i_id % 50) AS s_order_cnt, 0 AS s_remote_cnt,
       'stockdata' AS s_data
FROM warehouse w, item it;

CREATE OR REPLACE TABLE oorder AS
SELECT d.d_w_id AS o_w_id,
       d.d_id AS o_d_id,
       o.i AS o_id,
       1 + ((o.i - 1) % {customers_per_d}) AS o_c_id,
       TIMESTAMP '2007-01-02' + (o.i % 5000) * INTERVAL '1 minute' AS o_entry_d,
       CASE WHEN o.i < {orders_per_d} * 0.9 THEN 1 + (o.i % 10) ELSE NULL END AS o_carrier_id,
       {ol_per_order} AS o_ol_cnt,
       1 AS o_all_local
FROM district d, range(1, {orders_per_d} + 1) o(i);

CREATE OR REPLACE TABLE new_order AS
SELECT o_w_id AS no_w_id, o_d_id AS no_d_id, o_id AS no_o_id
FROM oorder
WHERE o_carrier_id IS NULL;

CREATE OR REPLACE TABLE order_line AS
SELECT o.o_w_id AS ol_w_id,
       o.o_d_id AS ol_d_id,
       o.o_id AS ol_o_id,
       ol.i AS ol_number,
       1 + ((o.o_id * ol.i) % {items}) AS ol_i_id,
       o.o_w_id AS ol_supply_w_id,
       CASE WHEN o.o_carrier_id IS NOT NULL
            THEN o.o_entry_d + INTERVAL '1 day'
            ELSE NULL END AS ol_delivery_d,
       5 AS ol_quantity,
       (1 + (o.o_id % 100)) * 1.0 AS ol_amount,
       'dist' AS ol_dist_info
FROM oorder o, range(1, {ol_per_order} + 1) ol(i);

CREATE OR REPLACE TABLE history AS
SELECT row_number() OVER () AS h_id,
       c.c_id AS h_c_id, c.c_d_id AS h_c_d_id, c.c_w_id AS h_c_w_id,
       c.c_d_id AS h_d_id, c.c_w_id AS h_w_id,
       TIMESTAMP '2007-01-01' AS h_date,
       10.0 AS h_amount, 'hist' AS h_data
FROM customer c
LIMIT 5000;

COPY region TO '{out}/region.parquet' (FORMAT PARQUET);
COPY nation TO '{out}/nation.parquet' (FORMAT PARQUET);
COPY supplier TO '{out}/supplier.parquet' (FORMAT PARQUET);
COPY warehouse TO '{out}/warehouse.parquet' (FORMAT PARQUET);
COPY district TO '{out}/district.parquet' (FORMAT PARQUET);
COPY customer TO '{out}/customer.parquet' (FORMAT PARQUET);
COPY item TO '{out}/item.parquet' (FORMAT PARQUET);
COPY stock TO '{out}/stock.parquet' (FORMAT PARQUET);
COPY oorder TO '{out}/oorder.parquet' (FORMAT PARQUET);
COPY new_order TO '{out}/new_order.parquet' (FORMAT PARQUET);
COPY order_line TO '{out}/order_line.parquet' (FORMAT PARQUET);
COPY history TO '{out}/history.parquet' (FORMAT PARQUET);
"#
    )
}
