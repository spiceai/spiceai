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

//! Engine-agnostic CH-benCH seed-data generator, shared by the Postgres and
//! `MySQL` loaders.
//!
//! Generates one CSV file per table (large per-warehouse tables are sharded
//! across files, one per worker thread) so both loaders can bulk-load via
//! `COPY` / `LOAD DATA INFILE` instead of building and executing `INSERT`
//! statements. Every warehouse gets fully independent random data — unlike
//! the previous loader, which only randomized the first 10 "seed" warehouses
//! and cloned the rest via server-side `INSERT ... SELECT` to avoid paying
//! generation cost per warehouse. That clone step is no longer needed:
//! generation is CPU-bound and embarrassingly parallel across warehouses (no
//! database round-trips), so generating every warehouse directly is both
//! simpler and, at scale, faster than cloning (see the loader module docs for
//! measured numbers). It also means every warehouse's data is genuinely
//! independent rather than a relabeled duplicate of one of 10 source
//! warehouses, which is a more realistic dataset.
//!
//! Column order and generation formulas match the TPC-C + CH-benCH spec (see
//! `crate::rand`); nation/region data matches the CH-benCH reference dataset.

use std::fs::File;
use std::io::{BufWriter, Write as _};
use std::path::{Path, PathBuf};

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

use crate::rand as tpcc_rand;
use crate::{Error, Result};

const MAX_ITEMS: i32 = 100_000;
const STOCK_PER_WAREHOUSE: i32 = 100_000;
const DISTRICTS_PER_WAREHOUSE: i32 = 10;
const CUSTOMERS_PER_DISTRICT: i32 = 3_000;
const ORDERS_PER_DISTRICT: i32 = 3_000;
const NEW_ORDERS_PER_DISTRICT: i32 = 900;
const INIT_LOAD_TIME: &str = "2007-01-02 15:04:05";
/// Matches `BenchBase` CH-Benchmark reference row count.
const SUPPLIER_COUNT: i64 = 10_000;

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

/// Explicit `(table, column_list)` for every CH-benCH table, in the order
/// each `INSERT`/`COPY`/`LOAD DATA` statement should list them. Shared by
/// both loaders so the load statement's column list always matches what was
/// generated here. `_bench_ts` is deliberately absent — seed rows are
/// stamped by the column default (see `schema`/`schema_mysql`), not set here.
pub const TABLE_COLUMNS: &[(&str, &str)] = &[
    ("item", "i_id, i_im_id, i_name, i_price, i_data"),
    ("nation", "n_nationkey, n_name, n_regionkey, n_comment"),
    ("region", "r_regionkey, r_name, r_comment"),
    (
        "supplier",
        "su_suppkey, su_name, su_address, su_nationkey, su_phone, su_acctbal, su_comment",
    ),
    (
        "warehouse",
        "w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd",
    ),
    (
        "district",
        "d_id, d_w_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id",
    ),
    (
        "stock",
        "s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt, s_data",
    ),
    (
        "customer",
        "c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data",
    ),
    (
        "history",
        "h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data",
    ),
    (
        "oorder",
        "o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local",
    ),
    ("new_order", "no_o_id, no_d_id, no_w_id"),
    (
        "order_line",
        "ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info",
    ),
];

/// A generated CSV file ready to load: `table` is the schema table it
/// belongs to (multiple shards may share a table — load every shard into
/// it), `path` is the file on disk, `columns` is the exact column list for
/// the load statement (see [`TABLE_COLUMNS`]).
pub struct GeneratedShard {
    pub table: &'static str,
    pub path: PathBuf,
    pub columns: &'static str,
}

struct CsvString<'a>(&'a str);

impl std::fmt::Display for CsvString<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("\"")?;

        for ch in self.0.chars() {
            match ch {
                '"' => f.write_str("\"\"")?,
                _ => write!(f, "{ch}")?,
            }
        }

        f.write_str("\"")
    }
}

/// CSV-quote a string value (fields are alphanumeric by construction — see
/// `crate::rand` — but quoted defensively in case that ever changes).
fn csv_str(s: &str) -> CsvString<'_> {
    CsvString(s)
}

struct TableWriter {
    table: &'static str,
    columns: &'static str,
    path: PathBuf,
    w: BufWriter<File>,
    rows: u64,
}

impl TableWriter {
    fn new(dir: &Path, table: &'static str, columns: &'static str, shard: usize) -> Result<Self> {
        let path = dir.join(format!("{table}.shard{shard}.csv"));
        let f = File::create(&path).map_err(|source| Error::Io {
            action: format!("create seed CSV {}", path.display()),
            source,
        })?;
        Ok(Self {
            table,
            columns,
            path,
            w: BufWriter::with_capacity(1 << 20, f),
            rows: 0,
        })
    }

    fn write_line(&mut self, line: &str) -> Result<()> {
        self.w
            .write_all(line.as_bytes())
            .map_err(io_err(self.table))?;
        self.w.write_all(b"\n").map_err(io_err(self.table))?;
        self.rows += 1;
        Ok(())
    }

    fn finish(mut self) -> Result<GeneratedShard> {
        self.w.flush().map_err(io_err(self.table))?;
        Ok(GeneratedShard {
            table: self.table,
            path: self.path,
            columns: self.columns,
        })
    }
}

fn io_err(table: &'static str) -> impl FnOnce(std::io::Error) -> Error {
    move |source| Error::Io {
        action: format!("write seed CSV for {table}"),
        source,
    }
}

/// Look up `table`'s column list from [`TABLE_COLUMNS`] — the single source
/// of truth both the generator and the loaders use for column ordering.
/// Every call site here passes a literal from that same table, so a miss can
/// only happen if a future edit adds/renames a table inconsistently; fail
/// loudly with a structured error rather than silently generating an empty
/// column list, which would produce a confusing `COPY`/`LOAD DATA` SQL error
/// far from the actual mistake.
fn table_columns(table: &str) -> Result<&'static str> {
    TABLE_COLUMNS
        .iter()
        .find(|(t, _)| *t == table)
        .map(|(_, c)| *c)
        .ok_or_else(|| Error::UnknownTable {
            table: table.to_owned(),
        })
}

/// Best-effort extraction of a message from a `std::thread::Result` panic
/// payload (the common `&str`/`String` panic payloads; anything else falls
/// back to a generic message rather than failing to report the panic at all).
fn panic_message(payload: &(dyn std::any::Any + Send)) -> Error {
    let message = payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic payload".to_owned());
    Error::TaskJoin {
        message: format!("csv generation thread panicked: {message}"),
    }
}

/// Generate every per-warehouse table's rows for warehouses `start..=end`
/// into shard-suffixed files, using the same per-warehouse seed derivation
/// (`seed ^ warehouse_id`) as the rest of the driver — so results are
/// identical regardless of how warehouses are partitioned across shards.
fn generate_warehouse_range(
    dir: &Path,
    shard: usize,
    start: usize,
    end: usize,
    seed: u64,
) -> Result<Vec<GeneratedShard>> {
    let mut tw_warehouse = TableWriter::new(dir, "warehouse", table_columns("warehouse")?, shard)?;
    let mut tw_district = TableWriter::new(dir, "district", table_columns("district")?, shard)?;
    let mut tw_stock = TableWriter::new(dir, "stock", table_columns("stock")?, shard)?;
    let mut tw_customer = TableWriter::new(dir, "customer", table_columns("customer")?, shard)?;
    let mut tw_history = TableWriter::new(dir, "history", table_columns("history")?, shard)?;
    let mut tw_oorder = TableWriter::new(dir, "oorder", table_columns("oorder")?, shard)?;
    let mut tw_new_order = TableWriter::new(dir, "new_order", table_columns("new_order")?, shard)?;
    let mut tw_order_line =
        TableWriter::new(dir, "order_line", table_columns("order_line")?, shard)?;

    for w in start..=end {
        let w_id = i32::try_from(w).unwrap_or(i32::MAX);
        let warehouse_seed = seed ^ (w as u64);
        let mut wh_rng = StdRng::seed_from_u64(warehouse_seed);
        let wh_c_load: usize = wh_rng.random_range(0..256);

        {
            let w_name = tpcc_rand::rand_chars(&mut wh_rng, 6, 10);
            let w_street_1 = tpcc_rand::rand_chars(&mut wh_rng, 10, 20);
            let w_street_2 = tpcc_rand::rand_chars(&mut wh_rng, 10, 20);
            let w_city = tpcc_rand::rand_chars(&mut wh_rng, 10, 20);
            let w_state = tpcc_rand::rand_state(&mut wh_rng);
            let w_zip = tpcc_rand::rand_zip(&mut wh_rng);
            let w_tax = tpcc_rand::rand_tax(&mut wh_rng);
            tw_warehouse.write_line(&format!(
                "{w_id},{},{},{},{},{},{},{w_tax:.4},300000.00",
                csv_str(&w_name),
                csv_str(&w_street_1),
                csv_str(&w_street_2),
                csv_str(&w_city),
                csv_str(&w_state),
                csv_str(&w_zip),
            ))?;
        }

        for d in 1..=DISTRICTS_PER_WAREHOUSE {
            let d_name = tpcc_rand::rand_chars(&mut wh_rng, 6, 10);
            let d_street_1 = tpcc_rand::rand_chars(&mut wh_rng, 10, 20);
            let d_street_2 = tpcc_rand::rand_chars(&mut wh_rng, 10, 20);
            let d_city = tpcc_rand::rand_chars(&mut wh_rng, 10, 20);
            let d_state = tpcc_rand::rand_state(&mut wh_rng);
            let d_zip = tpcc_rand::rand_zip(&mut wh_rng);
            let d_tax = tpcc_rand::rand_tax(&mut wh_rng);
            tw_district.write_line(&format!(
                "{d},{w_id},{},{},{},{},{},{},{d_tax:.4},30000.00,3001",
                csv_str(&d_name),
                csv_str(&d_street_1),
                csv_str(&d_street_2),
                csv_str(&d_city),
                csv_str(&d_state),
                csv_str(&d_zip),
            ))?;
        }

        for i in 1..=STOCK_PER_WAREHOUSE {
            let s_quantity: i32 = wh_rng.random_range(10..=100);
            let dist_0 = tpcc_rand::rand_letters(&mut wh_rng, 24, 24);
            let dist_1 = tpcc_rand::rand_letters(&mut wh_rng, 24, 24);
            let dist_2 = tpcc_rand::rand_letters(&mut wh_rng, 24, 24);
            let dist_3 = tpcc_rand::rand_letters(&mut wh_rng, 24, 24);
            let dist_4 = tpcc_rand::rand_letters(&mut wh_rng, 24, 24);
            let dist_5 = tpcc_rand::rand_letters(&mut wh_rng, 24, 24);
            let dist_6 = tpcc_rand::rand_letters(&mut wh_rng, 24, 24);
            let dist_7 = tpcc_rand::rand_letters(&mut wh_rng, 24, 24);
            let dist_8 = tpcc_rand::rand_letters(&mut wh_rng, 24, 24);
            let dist_9 = tpcc_rand::rand_letters(&mut wh_rng, 24, 24);

            let s_data = tpcc_rand::rand_original_string(&mut wh_rng);
            tw_stock.write_line(&format!(
                "{i},{w_id},{s_quantity},{},{},{},{},{},{},{},{},{},{},0,0,0,{}",
                csv_str(&dist_0),
                csv_str(&dist_1),
                csv_str(&dist_2),
                csv_str(&dist_3),
                csv_str(&dist_4),
                csv_str(&dist_5),
                csv_str(&dist_6),
                csv_str(&dist_7),
                csv_str(&dist_8),
                csv_str(&dist_9),
                csv_str(&s_data),
            ))?;
        }

        for d in 1..=DISTRICTS_PER_WAREHOUSE {
            for i in 1..=CUSTOMERS_PER_DISTRICT {
                let c_last = if i <= 1000 {
                    tpcc_rand::c_last_syllables(usize::try_from(i - 1).unwrap_or(0))
                } else {
                    tpcc_rand::rand_c_last(&mut wh_rng, wh_c_load)
                };
                let c_first = tpcc_rand::rand_chars(&mut wh_rng, 8, 16);
                let c_street_1 = tpcc_rand::rand_chars(&mut wh_rng, 10, 20);
                let c_street_2 = tpcc_rand::rand_chars(&mut wh_rng, 10, 20);
                let c_city = tpcc_rand::rand_chars(&mut wh_rng, 10, 20);
                let c_state = tpcc_rand::rand_state(&mut wh_rng);
                let c_zip = tpcc_rand::rand_zip(&mut wh_rng);
                let c_phone = tpcc_rand::rand_numbers(&mut wh_rng, 16, 16);
                let c_credit = if wh_rng.random_range(0..10) == 0 {
                    "BC"
                } else {
                    "GC"
                };
                let c_discount: f64 = f64::from(wh_rng.random_range(0..=5_000)) / 10_000.0;
                let c_data = tpcc_rand::rand_chars(&mut wh_rng, 300, 500);
                tw_customer.write_line(&format!(
                    "{i},{d},{w_id},{},OE,{},{},{},{},{},{},{},{},{},50000.00,{c_discount:.4},-10.00,10.00,1,0,{}",
                    csv_str(&c_first),
                    csv_str(&c_last),
                    csv_str(&c_street_1),
                    csv_str(&c_street_2),
                    csv_str(&c_city),
                    csv_str(&c_state),
                    csv_str(&c_zip),
                    csv_str(&c_phone),
                    csv_str(INIT_LOAD_TIME),
                    csv_str(c_credit),
                    csv_str(&c_data),
                ))?;
            }

            for i in 1..=CUSTOMERS_PER_DISTRICT {
                let h_data = tpcc_rand::rand_chars(&mut wh_rng, 12, 24);
                tw_history.write_line(&format!(
                    "{i},{d},{w_id},{d},{w_id},{},10.00,{}",
                    csv_str(INIT_LOAD_TIME),
                    csv_str(&h_data),
                ))?;
            }

            let mut cids: Vec<i32> = (1..=ORDERS_PER_DISTRICT).collect();
            for i in (1..cids.len()).rev() {
                let j = wh_rng.random_range(0..=i);
                cids.swap(i, j);
            }
            let mut ol_cnts = Vec::with_capacity(usize::try_from(ORDERS_PER_DISTRICT).unwrap_or(0));
            for i in 0..ORDERS_PER_DISTRICT {
                let o_id = i + 1;
                let o_c_id = cids[usize::try_from(i).unwrap_or(0)];
                let o_carrier_id_str = if o_id < 2101 {
                    wh_rng.random_range(1..=10i32).to_string()
                } else {
                    "\\N".to_owned()
                };
                let o_ol_cnt: i32 = wh_rng.random_range(5..=15);
                ol_cnts.push(o_ol_cnt);
                tw_oorder.write_line(&format!(
                    "{o_id},{d},{w_id},{o_c_id},{},{o_carrier_id_str},{o_ol_cnt},1",
                    csv_str(INIT_LOAD_TIME),
                ))?;
            }

            for i in 0..NEW_ORDERS_PER_DISTRICT {
                let no_o_id = 2101 + i;
                tw_new_order.write_line(&format!("{no_o_id},{d},{w_id}"))?;
            }

            for (i, &ol_cnt) in ol_cnts.iter().enumerate() {
                let o_id = i32::try_from(i).unwrap_or(0) + 1;
                for j in 1..=ol_cnt {
                    let ol_i_id: i32 = wh_rng.random_range(1..=100_000);
                    if o_id < 2101 {
                        let ol_delivery_d = csv_str(INIT_LOAD_TIME);
                        let ol_amount: f64 = 0.00;
                        let ol_dist_info = tpcc_rand::rand_chars(&mut wh_rng, 24, 24);
                        tw_order_line.write_line(&format!(
                            "{o_id},{d},{w_id},{j},{ol_i_id},{w_id},{ol_delivery_d},5,{ol_amount:.2},{}",
                            csv_str(&ol_dist_info),
                        ))?;
                    } else {
                        let ol_delivery_d = "\\N";
                        let ol_amount = f64::from(wh_rng.random_range(1..=999_999)) / 100.0;
                        let ol_dist_info = tpcc_rand::rand_chars(&mut wh_rng, 24, 24);
                        tw_order_line.write_line(&format!(
                            "{o_id},{d},{w_id},{j},{ol_i_id},{w_id},{ol_delivery_d},5,{ol_amount:.2},{}",
                            csv_str(&ol_dist_info),
                        ))?;
                    }
                }
            }
        }
    }

    [
        tw_warehouse,
        tw_district,
        tw_stock,
        tw_customer,
        tw_history,
        tw_oorder,
        tw_new_order,
        tw_order_line,
    ]
    .into_iter()
    .map(TableWriter::finish)
    .collect()
}

/// Generate CSV seed data for all 12 CH-benCH tables into `dir`, one file per
/// shared table (`item`/`nation`/`region`/`supplier`) plus sharded files for
/// the 8 per-warehouse tables (parallelized across
/// [`std::thread::available_parallelism`] threads, each handling a
/// contiguous warehouse range). Blocking/CPU-bound — callers on an async
/// runtime should run this via [`tokio::task::spawn_blocking`].
///
/// When `seed` is `Some`, a deterministic RNG is used so the same seed always
/// produces the same dataset (independent of shard/thread count).
///
/// # Errors
///
/// Returns [`Error::Io`] if any CSV file cannot be created or written.
pub fn generate(dir: &Path, warehouses: usize, seed: Option<u64>) -> Result<Vec<GeneratedShard>> {
    let base_seed = seed.unwrap_or_else(|| rand::rng().random());
    let mut rng = StdRng::seed_from_u64(base_seed);
    let mut shards: Vec<GeneratedShard> = Vec::new();

    // Shared tables (not warehouse-scoped): small, generated up front.
    {
        let mut tw = TableWriter::new(dir, "item", table_columns("item")?, 0)?;
        for i in 1..=MAX_ITEMS {
            let i_im_id: i32 = rng.random_range(1..=10_000);
            let i_price: f64 = f64::from(rng.random_range(100..=10_000)) / 100.0;
            let i_name = tpcc_rand::rand_chars(&mut rng, 14, 24);
            let i_data = tpcc_rand::rand_original_string(&mut rng);
            tw.write_line(&format!(
                "{i},{i_im_id},{},{i_price:.2},{}",
                csv_str(&i_name),
                csv_str(&i_data)
            ))?;
        }
        shards.push(tw.finish()?);
    }
    {
        let mut tw = TableWriter::new(dir, "nation", table_columns("nation")?, 0)?;
        for &(key, name, region) in NATIONS.iter().chain(EXTRA_NATIONS.iter()) {
            tw.write_line(&format!("{key},{},{region},\"\"", csv_str(name)))?;
        }
        shards.push(tw.finish()?);
    }
    {
        let mut tw = TableWriter::new(dir, "region", table_columns("region")?, 0)?;
        for &(key, name) in REGIONS {
            tw.write_line(&format!("{key},{},\"\"", csv_str(name)))?;
        }
        shards.push(tw.finish()?);
    }
    {
        let mut tw = TableWriter::new(dir, "supplier", table_columns("supplier")?, 0)?;
        let nation_count = i64::try_from(NATIONS.len() + EXTRA_NATIONS.len()).unwrap_or(62);
        for i in 1..=SUPPLIER_COUNT {
            let s_name = tpcc_rand::rand_chars(&mut rng, 25, 25);
            let s_address = tpcc_rand::rand_chars(&mut rng, 10, 25);
            let s_nationkey: i64 = rng.random_range(0..nation_count);
            let s_phone = format!(
                "{:02}-{}-{}-{}",
                s_nationkey + 10,
                rng.random_range(100..=999),
                rng.random_range(100..=999),
                rng.random_range(1000..=9999)
            );
            let s_acctbal: f64 = f64::from(rng.random_range(10_000i32..=1_000_000_000i32)) / 100.0;
            let s_comment = tpcc_rand::rand_chars(&mut rng, 25, 63);
            tw.write_line(&format!(
                "{i},{},{},{s_nationkey},{},{s_acctbal:.2},{}",
                csv_str(&s_name),
                csv_str(&s_address),
                csv_str(&s_phone),
                csv_str(&s_comment)
            ))?;
        }
        shards.push(tw.finish()?);
    }

    // Per-warehouse tables: partition `1..=warehouses` into contiguous shards,
    // one worker thread per shard. Generation has no shared state across
    // warehouses (each derives its own RNG from `base_seed ^ warehouse_id`),
    // so this is embarrassingly parallel.
    #[expect(
        clippy::disallowed_methods,
        reason = "a host-local data-generation tool, not spiced: it should use the whole machine it runs on, not spiced's CPU entitlement"
    )]
    let threads = std::thread::available_parallelism()
        .map_or(4, std::num::NonZeroUsize::get)
        .min(warehouses.max(1));
    let per_shard = warehouses.div_ceil(threads);
    let ranges = (0..threads).filter_map(|t| {
        let start = t * per_shard + 1;
        let end = ((t + 1) * per_shard).min(warehouses);
        (start <= end).then_some((start, end))
    });

    let shard_results: Vec<Result<Vec<GeneratedShard>>> = std::thread::scope(|scope| {
        #[expect(
            clippy::needless_collect,
            reason = "the collect is required, not needless: it forces every thread to be \
                      spawned before any is joined. Fusing this into the join map below would \
                      make the outer iterator lazy, so each thread would be spawned and joined \
                      one at a time — serializing generation instead of parallelizing it."
        )]
        let handles: Vec<_> = ranges
            .enumerate()
            .map(|(shard, (start, end))| {
                scope.spawn(move || generate_warehouse_range(dir, shard, start, end, base_seed))
            })
            .collect();
        handles
            .into_iter()
            .map(|h| {
                h.join()
                    .unwrap_or_else(|panic_payload| Err(panic_message(&panic_payload)))
            })
            .collect()
    });
    for shard in shard_results {
        shards.extend(shard?);
    }

    Ok(shards)
}
