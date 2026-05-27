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

//! Seed data loader for TPC-C + CH-benCH supplemental tables.
//!
//! Uses batched multi-row `INSERT ... VALUES (...), (...), ...` statements
//! (1024 rows per batch) for faster inserts.
//!
//! For scale factors > `SEED_WAREHOUSES` (10), the first 10 warehouses are
//! loaded with full independent random data. Remaining warehouses are cloned
//! from the seed set using server-side `INSERT ... SELECT` (rotating across
//! seed warehouses for slight variation).

use std::fmt::Write as _;
use std::time::Instant;

use rand::rngs::StdRng;
use rand::{Rng, RngExt, SeedableRng};
use tokio_postgres::Client;

use crate::Result;
use crate::rand as tpcc_rand;

const MAX_ITEMS: i32 = 100_000;
const STOCK_PER_WAREHOUSE: i32 = 100_000;
const DISTRICTS_PER_WAREHOUSE: i32 = 10;
const CUSTOMERS_PER_DISTRICT: i32 = 3_000;
const ORDERS_PER_DISTRICT: i32 = 3_000;
const NEW_ORDERS_PER_DISTRICT: i32 = 900;

const INIT_LOAD_TIME: &str = "2007-01-02 15:04:05";
const BATCH_SIZE: usize = 1024;

// ─── Batch sink ───────────────────────────────────────────────────────────────

/// Accumulates rows as SQL value-tuple strings and flushes them as multi-row
/// `INSERT ... VALUES (...), (...), ...` statements.
struct BatchSink {
    insert_hint: String,
    buf: String,
    buffered_rows: usize,
    max_batch_rows: usize,
}

impl BatchSink {
    fn new(insert_hint: &str) -> Self {
        Self {
            insert_hint: insert_hint.to_owned(),
            buf: String::with_capacity(64 * 1024),
            buffered_rows: 0,
            max_batch_rows: BATCH_SIZE,
        }
    }

    fn write_row(&mut self, row: &str) {
        if self.buffered_rows == 0 {
            self.buf.push_str(&self.insert_hint);
            self.buf.push(' ');
            self.buf.push_str(row);
        } else {
            self.buf.push_str(", ");
            self.buf.push_str(row);
        }
        self.buffered_rows += 1;
    }

    fn needs_flush(&self) -> bool {
        self.buffered_rows >= self.max_batch_rows
    }

    async fn flush(&mut self, client: &Client, table: &str) -> Result<()> {
        if self.buffered_rows == 0 {
            return Ok(());
        }
        client
            .execute(self.buf.as_str(), &[])
            .await
            .map_err(|source| crate::Error::Sql {
                action: format!("batch insert into {table}"),
                source,
            })?;
        self.buf.clear();
        self.buffered_rows = 0;
        Ok(())
    }

    async fn maybe_flush(&mut self, client: &Client, table: &str) -> Result<()> {
        if self.needs_flush() {
            self.flush(client, table).await?;
        }
        Ok(())
    }
}

/// Escape a string value for SQL literal inclusion (single-quote escaping).
fn sql_str(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn sql_opt_str(s: Option<&str>) -> String {
    match s {
        Some(v) => sql_str(v),
        None => "NULL".to_owned(),
    }
}

fn sql_opt_i32(v: Option<i32>) -> String {
    match v {
        Some(n) => n.to_string(),
        None => "NULL".to_owned(),
    }
}

// ─── Public entry point ───────────────────────────────────────────────────────

/// Number of warehouses loaded with full independent random data.
/// Warehouses beyond this count are cloned from the seed set.
/// Set to 10 to guarantee Q19 (which hard-codes `ol_w_id IN (1..5)`) gets
/// genuinely diverse data, plus extra headroom for other per-warehouse queries.
const SEED_WAREHOUSES: usize = 10;

/// Tables that carry a `w_id`-like column and need per-warehouse cloning.
/// Each entry: (`table_name`, `w_id_column`, `column_list_for_select`).
const WAREHOUSE_TABLES: &[(&str, &str, &str)] = &[
    (
        "warehouse",
        "w_id",
        "w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd",
    ),
    (
        "district",
        "d_w_id",
        "d_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id",
    ),
    (
        "stock",
        "s_w_id",
        "s_i_id, s_quantity, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt, s_data",
    ),
    (
        "customer",
        "c_w_id",
        "c_id, c_d_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data",
    ),
    (
        "history",
        "h_c_w_id",
        "h_c_id, h_c_d_id, h_d_id, h_w_id, h_date, h_amount, h_data",
    ),
    (
        "oorder",
        "o_w_id",
        "o_id, o_d_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local",
    ),
    ("new_order", "no_w_id", "no_o_id, no_d_id"),
    (
        "order_line",
        "ol_w_id",
        "ol_o_id, ol_d_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info",
    ),
];

/// Load all seed data for the given number of warehouses.
///
/// Strategy:
/// - Shared tables (item, nation, region, supplier) are loaded once on `client`.
/// - The first `min(warehouses, SEED_WAREHOUSES)` warehouses are loaded **in
///   parallel**, each on its own Postgres connection (spawned from `conn_str`).
///   Each warehouse gets a deterministic per-warehouse RNG derived from `seed`.
/// - Remaining warehouses are cloned from the seed set using server-side
///   `INSERT ... SELECT` (rotating source across seed warehouses).
///
/// When `seed` is `Some`, a deterministic RNG is used so that the same seed
/// always produces the same dataset.
///
/// # Errors
///
/// Returns an error if any database operation fails.
pub async fn load_all(
    client: &Client,
    conn_str: &str,
    warehouses: usize,
    seed: Option<u64>,
) -> Result<()> {
    let mut rng: StdRng = match seed {
        Some(s) => StdRng::seed_from_u64(s),
        None => StdRng::from_rng(&mut rand::rng()),
    };

    load_item(client, &mut rng).await?;
    load_nation(client).await?;
    load_region(client).await?;
    load_supplier(client, &mut rng).await?;

    let seed_count = warehouses.min(SEED_WAREHOUSES);

    // Phase 1: Load seed warehouses in parallel, each with its own connection.
    let phase1_start = Instant::now();
    println!(
        "  loading {seed_count} seed warehouse(s) in parallel ({DISTRICTS_PER_WAREHOUSE} districts, \
         {}K customers, {}K orders, ~{}K order lines each)...",
        DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT / 1000,
        DISTRICTS_PER_WAREHOUSE * ORDERS_PER_DISTRICT / 1000,
        DISTRICTS_PER_WAREHOUSE * ORDERS_PER_DISTRICT * 10 / 1000,
    );

    let mut handles = Vec::with_capacity(seed_count);
    for w in 1..=seed_count {
        let conn_str = conn_str.to_owned();
        // Derive a deterministic per-warehouse seed: base_seed XOR warehouse index.
        // This ensures each warehouse gets different random data while remaining
        // reproducible across runs.
        let warehouse_seed = seed.map(|s| s ^ (w as u64));

        handles.push(tokio::spawn(async move {
            let (wh_client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
                .await
                .map_err(|source| crate::Error::Sql {
                    action: format!("connect for warehouse {w} loader"),
                    source,
                })?;

            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("warehouse {w} loader connection error: {e}");
                }
            });

            let w_id = i32::try_from(w).unwrap_or(i32::MAX);

            let mut wh_rng: StdRng = match warehouse_seed {
                Some(s) => StdRng::seed_from_u64(s),
                None => StdRng::from_rng(&mut rand::rng()),
            };
            let wh_c_load: usize = wh_rng.random_range(0..256);

            load_warehouse(&wh_client, &mut wh_rng, w_id).await?;
            load_district(&wh_client, &mut wh_rng, w_id).await?;
            load_stock(&wh_client, &mut wh_rng, w_id).await?;

            for d in 1..=DISTRICTS_PER_WAREHOUSE {
                load_customer(&wh_client, &mut wh_rng, w_id, d, wh_c_load).await?;
                load_history(&wh_client, &mut wh_rng, w_id, d).await?;
                let ol_cnts = load_orders(&wh_client, &mut wh_rng, w_id, d).await?;
                load_new_order(&wh_client, w_id, d).await?;
                load_order_line(&wh_client, &mut wh_rng, w_id, d, &ol_cnts).await?;
            }

            Ok::<(), crate::Error>(())
        }));
    }

    // Await all seed warehouse tasks.
    for handle in handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(e) => {
                return Err(crate::Error::Sql {
                    action: format!("seed warehouse loader task panicked: {e}"),
                    source: tokio_postgres::Error::__private_api_timeout(),
                });
            }
        }
    }

    println!(
        "  seed phase complete ({seed_count} warehouses in {:.1?})",
        phase1_start.elapsed()
    );

    // Phase 2: Clone remaining warehouses from the seed set.
    // Parallelism adds no benefit here since INSERT...SELECT is
    // server-side I/O bound (shared WAL writer, buffer pool, disk).
    if warehouses > seed_count {
        let clone_start = Instant::now();
        let to_clone = warehouses - seed_count;
        println!(
            "  cloning {to_clone} warehouse(s) from {seed_count} seed warehouse(s) \
             using server-side INSERT...SELECT..."
        );

        for w in (seed_count + 1)..=warehouses {
            let target_w_id = i32::try_from(w).unwrap_or(i32::MAX);
            // Rotate source across seed warehouses (1-based).
            let source_w_id = i32::try_from((w - 1) % seed_count + 1).unwrap_or(1);

            clone_warehouse(client, source_w_id, target_w_id).await?;

            // Progress reporting every 50 warehouses.
            let done = w - seed_count;
            if done.is_multiple_of(50) || w == warehouses {
                let elapsed = clone_start.elapsed();
                println!("    cloned {done}/{to_clone} warehouses ({elapsed:.1?} elapsed)",);
            }
        }

        println!(
            "  clone phase complete ({to_clone} warehouses in {:.1?})",
            clone_start.elapsed()
        );
    }

    Ok(())
}

/// Clone all warehouse-scoped data from `source_w_id` to `target_w_id` using
/// server-side `INSERT ... SELECT` with the `w_id` column substituted.
async fn clone_warehouse(client: &Client, source_w_id: i32, target_w_id: i32) -> Result<()> {
    for &(table, wid_col, cols) in WAREHOUSE_TABLES {
        // For `history`, h_w_id is a separate column that also references the
        // warehouse — substitute both h_c_w_id (filter) and h_w_id (value).
        let select_cols = if table == "history" {
            cols.replace("h_w_id", &target_w_id.to_string())
        } else if table == "order_line" {
            // ol_supply_w_id should point to the target warehouse too.
            cols.replace("ol_supply_w_id", &target_w_id.to_string())
        } else {
            cols.to_owned()
        };

        let sql = format!(
            "INSERT INTO {table} ({wid_col}, {cols}) \
             SELECT {target_w_id}, {select_cols} FROM {table} WHERE {wid_col} = {source_w_id}"
        );

        client
            .execute(sql.as_str(), &[])
            .await
            .map_err(|source| crate::Error::Sql {
                action: format!("clone {table} from warehouse {source_w_id} to {target_w_id}"),
                source,
            })?;
    }
    Ok(())
}

// ─── Per-table loaders ────────────────────────────────────────────────────────

async fn load_item(client: &Client, rng: &mut impl Rng) -> Result<()> {
    println!("  loading item ({MAX_ITEMS} rows)");
    let mut sink =
        BatchSink::new("INSERT INTO item (i_id, i_im_id, i_name, i_price, i_data) VALUES");
    let mut row = String::new();

    for i in 1..=MAX_ITEMS {
        let i_im_id: i32 = rng.random_range(1..=10_000);
        let i_price: f64 = f64::from(rng.random_range(100..=10_000)) / 100.0;
        let i_name = tpcc_rand::rand_chars(rng, 14, 24);
        let i_data = tpcc_rand::rand_original_string(rng);

        row.clear();
        let _ = write!(
            row,
            "({i}, {i_im_id}, {}, {i_price}, {})",
            sql_str(&i_name),
            sql_str(&i_data)
        );
        sink.write_row(&row);
        sink.maybe_flush(client, "item").await?;
    }
    sink.flush(client, "item").await
}

async fn load_warehouse(client: &Client, rng: &mut impl Rng, w_id: i32) -> Result<()> {
    let mut sink = BatchSink::new(
        "INSERT INTO warehouse (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) VALUES",
    );

    let w_name = tpcc_rand::rand_chars(rng, 6, 10);
    let w_street_1 = tpcc_rand::rand_chars(rng, 10, 20);
    let w_street_2 = tpcc_rand::rand_chars(rng, 10, 20);
    let w_city = tpcc_rand::rand_chars(rng, 10, 20);
    let w_state = tpcc_rand::rand_state(rng);
    let w_zip = tpcc_rand::rand_zip(rng);
    let w_tax = tpcc_rand::rand_tax(rng);
    let w_ytd: f64 = 300_000.00;

    let row = format!(
        "({w_id}, {}, {}, {}, {}, {}, {}, {w_tax}, {w_ytd})",
        sql_str(&w_name),
        sql_str(&w_street_1),
        sql_str(&w_street_2),
        sql_str(&w_city),
        sql_str(&w_state),
        sql_str(&w_zip),
    );
    sink.write_row(&row);
    sink.flush(client, "warehouse").await
}

async fn load_district(client: &Client, rng: &mut impl Rng, w_id: i32) -> Result<()> {
    let mut sink = BatchSink::new(
        "INSERT INTO district (d_id, d_w_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id) VALUES",
    );
    let mut row = String::new();

    for d in 1..=DISTRICTS_PER_WAREHOUSE {
        let d_name = tpcc_rand::rand_chars(rng, 6, 10);
        let d_street_1 = tpcc_rand::rand_chars(rng, 10, 20);
        let d_street_2 = tpcc_rand::rand_chars(rng, 10, 20);
        let d_city = tpcc_rand::rand_chars(rng, 10, 20);
        let d_state = tpcc_rand::rand_state(rng);
        let d_zip = tpcc_rand::rand_zip(rng);
        let d_tax = tpcc_rand::rand_tax(rng);
        let d_ytd: f64 = 30_000.00;

        row.clear();
        let _ = write!(
            row,
            "({d}, {w_id}, {}, {}, {}, {}, {}, {}, {d_tax}, {d_ytd}, 3001)",
            sql_str(&d_name),
            sql_str(&d_street_1),
            sql_str(&d_street_2),
            sql_str(&d_city),
            sql_str(&d_state),
            sql_str(&d_zip),
        );
        sink.write_row(&row);
        sink.maybe_flush(client, "district").await?;
    }
    sink.flush(client, "district").await
}

async fn load_stock(client: &Client, rng: &mut impl Rng, w_id: i32) -> Result<()> {
    let mut sink = BatchSink::new(
        "INSERT INTO stock (s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt, s_data) VALUES",
    );
    let mut row = String::new();

    for i in 1..=STOCK_PER_WAREHOUSE {
        let s_quantity: i32 = rng.random_range(10..=100);
        let s_dist_01 = tpcc_rand::rand_letters(rng, 24, 24);
        let s_dist_02 = tpcc_rand::rand_letters(rng, 24, 24);
        let s_dist_03 = tpcc_rand::rand_letters(rng, 24, 24);
        let s_dist_04 = tpcc_rand::rand_letters(rng, 24, 24);
        let s_dist_05 = tpcc_rand::rand_letters(rng, 24, 24);
        let s_dist_06 = tpcc_rand::rand_letters(rng, 24, 24);
        let s_dist_07 = tpcc_rand::rand_letters(rng, 24, 24);
        let s_dist_08 = tpcc_rand::rand_letters(rng, 24, 24);
        let s_dist_09 = tpcc_rand::rand_letters(rng, 24, 24);
        let s_dist_10 = tpcc_rand::rand_letters(rng, 24, 24);
        let s_data = tpcc_rand::rand_original_string(rng);

        row.clear();
        let _ = write!(
            row,
            "({i}, {w_id}, {s_quantity}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, 0, 0, 0, {})",
            sql_str(&s_dist_01),
            sql_str(&s_dist_02),
            sql_str(&s_dist_03),
            sql_str(&s_dist_04),
            sql_str(&s_dist_05),
            sql_str(&s_dist_06),
            sql_str(&s_dist_07),
            sql_str(&s_dist_08),
            sql_str(&s_dist_09),
            sql_str(&s_dist_10),
            sql_str(&s_data),
        );
        sink.write_row(&row);
        sink.maybe_flush(client, "stock").await?;
    }
    sink.flush(client, "stock").await
}

async fn load_customer(
    client: &Client,
    rng: &mut impl Rng,
    w_id: i32,
    d_id: i32,
    c_load: usize,
) -> Result<()> {
    let mut sink = BatchSink::new(
        "INSERT INTO customer (c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data) VALUES",
    );
    let mut row = String::new();

    for i in 1..=CUSTOMERS_PER_DISTRICT {
        let c_last = if i <= 1000 {
            tpcc_rand::c_last_syllables(usize::try_from(i - 1).unwrap_or(0))
        } else {
            tpcc_rand::rand_c_last(rng, c_load)
        };
        let c_first = tpcc_rand::rand_chars(rng, 8, 16);
        let c_street_1 = tpcc_rand::rand_chars(rng, 10, 20);
        let c_street_2 = tpcc_rand::rand_chars(rng, 10, 20);
        let c_city = tpcc_rand::rand_chars(rng, 10, 20);
        let c_state = tpcc_rand::rand_state(rng);
        let c_zip = tpcc_rand::rand_zip(rng);
        let c_phone = tpcc_rand::rand_numbers(rng, 16, 16);
        let c_credit = if rng.random_range(0..10) == 0 {
            "BC"
        } else {
            "GC"
        };
        let c_credit_lim: f64 = 50_000.00;
        let c_discount: f64 = f64::from(rng.random_range(0..=5_000)) / 10_000.0;
        let c_balance: f64 = -10.00;
        let c_ytd_payment: f64 = 10.00;
        let c_data = tpcc_rand::rand_chars(rng, 300, 500);

        row.clear();
        let _ = write!(
            row,
            "({i}, {d_id}, {w_id}, {}, 'OE', {}, {}, {}, {}, {}, {}, {}, {}, {}, {c_credit_lim}, {c_discount}, {c_balance}, {c_ytd_payment}, 1, 0, {})",
            sql_str(&c_first),
            sql_str(&c_last),
            sql_str(&c_street_1),
            sql_str(&c_street_2),
            sql_str(&c_city),
            sql_str(&c_state),
            sql_str(&c_zip),
            sql_str(&c_phone),
            sql_str(INIT_LOAD_TIME),
            sql_str(c_credit),
            sql_str(&c_data),
        );
        sink.write_row(&row);
        sink.maybe_flush(client, "customer").await?;
    }
    sink.flush(client, "customer").await
}

async fn load_history(client: &Client, rng: &mut impl Rng, w_id: i32, d_id: i32) -> Result<()> {
    let mut sink = BatchSink::new(
        "INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES",
    );
    let mut row = String::new();

    for i in 1..=CUSTOMERS_PER_DISTRICT {
        let h_data = tpcc_rand::rand_chars(rng, 12, 24);

        row.clear();
        let _ = write!(
            row,
            "({i}, {d_id}, {w_id}, {d_id}, {w_id}, {}, 10.00, {})",
            sql_str(INIT_LOAD_TIME),
            sql_str(&h_data),
        );
        sink.write_row(&row);
        sink.maybe_flush(client, "history").await?;
    }
    sink.flush(client, "history").await
}

/// Load orders and return per-order `ol_cnt` values (needed by `load_order_line`).
async fn load_orders(
    client: &Client,
    rng: &mut impl Rng,
    w_id: i32,
    d_id: i32,
) -> Result<Vec<i32>> {
    let mut sink = BatchSink::new(
        "INSERT INTO oorder (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) VALUES",
    );
    let mut row = String::new();

    // Random permutation of customer IDs
    let mut cids: Vec<i32> = (1..=ORDERS_PER_DISTRICT).collect();
    for i in (1..cids.len()).rev() {
        let j = rng.random_range(0..=i);
        cids.swap(i, j);
    }

    let mut ol_cnts = Vec::with_capacity(usize::try_from(ORDERS_PER_DISTRICT).unwrap_or(0));

    for i in 0..ORDERS_PER_DISTRICT {
        let o_id = i + 1;
        let o_c_id = cids[usize::try_from(i).unwrap_or(0)];
        let o_carrier_id: Option<i32> = if o_id < 2101 {
            Some(rng.random_range(1..=10))
        } else {
            None
        };
        let o_ol_cnt: i32 = rng.random_range(5..=15);
        ol_cnts.push(o_ol_cnt);

        row.clear();
        let _ = write!(
            row,
            "({o_id}, {d_id}, {w_id}, {o_c_id}, {}, {}, {o_ol_cnt}, 1)",
            sql_str(INIT_LOAD_TIME),
            sql_opt_i32(o_carrier_id),
        );
        sink.write_row(&row);
        sink.maybe_flush(client, "oorder").await?;
    }
    sink.flush(client, "oorder").await?;

    Ok(ol_cnts)
}

async fn load_new_order(client: &Client, w_id: i32, d_id: i32) -> Result<()> {
    let mut sink = BatchSink::new("INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES");
    let mut row = String::new();

    for i in 0..NEW_ORDERS_PER_DISTRICT {
        let no_o_id = 2101 + i;
        row.clear();
        let _ = write!(row, "({no_o_id}, {d_id}, {w_id})");
        sink.write_row(&row);
        sink.maybe_flush(client, "new_order").await?;
    }
    sink.flush(client, "new_order").await
}

async fn load_order_line(
    client: &Client,
    rng: &mut impl Rng,
    w_id: i32,
    d_id: i32,
    ol_cnts: &[i32],
) -> Result<()> {
    let mut sink = BatchSink::new(
        "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) VALUES",
    );
    let mut row = String::new();

    for (i, &ol_cnt) in ol_cnts.iter().enumerate() {
        let o_id = i32::try_from(i).unwrap_or(0) + 1;

        for j in 1..=ol_cnt {
            let ol_i_id: i32 = rng.random_range(1..=100_000);

            let (ol_delivery_d, ol_amount): (Option<&str>, f64) = if o_id < 2101 {
                (Some(INIT_LOAD_TIME), 0.00)
            } else {
                (None, f64::from(rng.random_range(1..=999_999)) / 100.0)
            };
            let ol_dist_info = tpcc_rand::rand_chars(rng, 24, 24);

            row.clear();
            let _ = write!(
                row,
                "({o_id}, {d_id}, {w_id}, {j}, {ol_i_id}, {w_id}, {}, 5, {ol_amount}, {})",
                sql_opt_str(ol_delivery_d),
                sql_str(&ol_dist_info),
            );
            sink.write_row(&row);
            sink.maybe_flush(client, "order_line").await?;
        }
    }
    sink.flush(client, "order_line").await
}

// ─── CH-benCH supplemental tables (static data) ──────────────────────────────

/// TPC-H nation data. 25 nations with region keys.
/// Column order: (`n_nationkey`, `n_name`, `n_regionkey`).
const NATIONS: &[(i64, &str, i64)] = &[
    (0, "ALGERIA", 0),
    (1, "ARGENTINA", 1),
    (2, "BRAZIL", 1),
    (3, "CANADA", 1),
    (4, "EGYPT", 4),
    (5, "ETHIOPIA", 0),
    (6, "FRANCE", 3),
    (7, "GERMANY", 3),
    (8, "INDIA", 2),
    (9, "INDONESIA", 2),
    (10, "IRAN", 4),
    (11, "IRAQ", 4),
    (12, "JAPAN", 2),
    (13, "JORDAN", 4),
    (14, "KENYA", 0),
    (15, "MOROCCO", 0),
    (16, "MOZAMBIQUE", 0),
    (17, "PERU", 1),
    (18, "CHINA", 2),
    (19, "ROMANIA", 3),
    (20, "SAUDI ARABIA", 4),
    (21, "VIETNAM", 2),
    (22, "RUSSIA", 3),
    (23, "UNITED KINGDOM", 3),
    (24, "UNITED STATES", 1),
];

/// CH-benCH extends the 25 TPC-H nations with 37 additional nations for more
/// diverse customer-state mapping (total 62, matching go-tpc).
const EXTRA_NATIONS: &[(i64, &str, i64)] = &[
    (25, "AUSTRALIA", 2),
    (26, "BANGLADESH", 2),
    (27, "BELGIUM", 3),
    (28, "BOLIVIA", 1),
    (29, "CAMEROON", 0),
    (30, "CHILE", 1),
    (31, "COLOMBIA", 1),
    (32, "CONGO", 0),
    (33, "COSTA RICA", 1),
    (34, "CUBA", 1),
    (35, "CZECH REPUBLIC", 3),
    (36, "DENMARK", 3),
    (37, "DOMINICAN REPUBLIC", 1),
    (38, "ECUADOR", 1),
    (39, "EL SALVADOR", 1),
    (40, "FINLAND", 3),
    (41, "GREECE", 3),
    (42, "GUATEMALA", 1),
    (43, "HONDURAS", 1),
    (44, "HUNGARY", 3),
    (45, "ICELAND", 3),
    (46, "IRELAND", 3),
    (47, "ISRAEL", 4),
    (48, "ITALY", 3),
    (49, "JAMAICA", 1),
    (50, "SOUTH KOREA", 2),
    (51, "MALAYSIA", 2),
    (52, "MEXICO", 1),
    (53, "NEPAL", 2),
    (54, "NEW ZEALAND", 2),
    (55, "NICARAGUA", 1),
    (56, "NORWAY", 3),
    (57, "PAKISTAN", 2),
    (58, "PANAMA", 1),
    (59, "PHILIPPINES", 2),
    (60, "POLAND", 3),
    (61, "SOUTH AFRICA", 0),
];

const REGIONS: &[(i64, &str)] = &[
    (0, "AFRICA"),
    (1, "AMERICA"),
    (2, "ASIA"),
    (3, "EUROPE"),
    (4, "MIDDLE EAST"),
];

async fn load_nation(client: &Client) -> Result<()> {
    let total = NATIONS.len() + EXTRA_NATIONS.len();
    println!("  loading nation ({total} rows)");
    let mut sink =
        BatchSink::new("INSERT INTO nation (n_nationkey, n_name, n_regionkey, n_comment) VALUES");
    let mut row = String::new();

    for &(key, name, region) in NATIONS.iter().chain(EXTRA_NATIONS.iter()) {
        row.clear();
        let _ = write!(row, "({key}, {}, {region}, '')", sql_str(name));
        sink.write_row(&row);
        sink.maybe_flush(client, "nation").await?;
    }
    sink.flush(client, "nation").await
}

async fn load_region(client: &Client) -> Result<()> {
    println!("  loading region ({} rows)", REGIONS.len());
    let mut sink = BatchSink::new("INSERT INTO region (r_regionkey, r_name, r_comment) VALUES");
    let mut row = String::new();

    for &(key, name) in REGIONS {
        row.clear();
        let _ = write!(row, "({key}, {}, '')", sql_str(name));
        sink.write_row(&row);
    }
    sink.flush(client, "region").await
}

/// Load 10,000 supplier rows (matching `BenchBase` CH-Benchmark reference).
async fn load_supplier(client: &Client, rng: &mut impl Rng) -> Result<()> {
    const SUPPLIER_COUNT: i64 = 10_000;
    println!("  loading supplier ({SUPPLIER_COUNT} rows)");
    let mut sink = BatchSink::new(
        "INSERT INTO supplier (su_suppkey, su_name, su_address, su_nationkey, su_phone, su_acctbal, su_comment) VALUES",
    );
    let mut row = String::new();

    let nation_count = i64::try_from(NATIONS.len() + EXTRA_NATIONS.len()).unwrap_or(62);

    for i in 1..=SUPPLIER_COUNT {
        let s_name = tpcc_rand::rand_chars(rng, 25, 25);
        let s_address = tpcc_rand::rand_chars(rng, 10, 25);
        let s_nationkey: i64 = rng.random_range(0..nation_count);
        let s_phone = format!(
            "{:02}-{}-{}-{}",
            (s_nationkey + 10),
            rng.random_range(100..=999),
            rng.random_range(100..=999),
            rng.random_range(1000..=9999),
        );
        let s_acctbal: f64 = f64::from(rng.random_range(10_000i32..=1_000_000_000i32)) / 100.0;
        let s_comment = tpcc_rand::rand_chars(rng, 25, 63);

        row.clear();
        let _ = write!(
            row,
            "({i}, {}, {}, {s_nationkey}, {}, {s_acctbal}, {})",
            sql_str(&s_name),
            sql_str(&s_address),
            sql_str(&s_phone),
            sql_str(&s_comment),
        );
        sink.write_row(&row);
        sink.maybe_flush(client, "supplier").await?;
    }
    sink.flush(client, "supplier").await
}
