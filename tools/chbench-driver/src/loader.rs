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
//! Mirrors go-tpc's loading logic: for each warehouse, loads warehouse, district,
//! stock, customer, history, orders, new_order, and order_line. Also loads the
//! static item, nation, region, and supplier tables.

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio_postgres::Client;

use crate::rand as tpcc_rand;
use crate::Result;

const MAX_ITEMS: i32 = 100_000;
const STOCK_PER_WAREHOUSE: i32 = 100_000;
const DISTRICTS_PER_WAREHOUSE: i32 = 10;
const CUSTOMERS_PER_DISTRICT: i32 = 3_000;
const ORDERS_PER_DISTRICT: i32 = 3_000;
const NEW_ORDERS_PER_DISTRICT: i32 = 900;

const INIT_LOAD_TIME: &str = "2007-01-02 15:04:05";
const BATCH_SIZE: usize = 500;

/// Load all seed data for the given number of warehouses.
///
/// When `seed` is `Some`, a deterministic RNG is used so that the same seed
/// always produces the same dataset.
pub async fn load_all(client: &Client, warehouses: usize, seed: Option<u64>) -> Result<()> {
    let mut rng: StdRng = match seed {
        Some(s) => StdRng::seed_from_u64(s),
        None => StdRng::from_rng(rand::thread_rng()).unwrap_or_else(|_| StdRng::seed_from_u64(0)),
    };
    let c_load: usize = rng.gen_range(0..256);

    load_item(client, &mut rng).await?;
    load_nation(client).await?;
    load_region(client).await?;
    load_supplier(client, &mut rng).await?;

    for w in 1..=warehouses {
        let w_id = i32::try_from(w).unwrap_or(i32::MAX);
        load_warehouse(client, &mut rng, w_id).await?;
        load_district(client, &mut rng, w_id).await?;
        load_stock(client, &mut rng, w_id).await?;

        for d in 1..=DISTRICTS_PER_WAREHOUSE {
            load_customer(client, &mut rng, w_id, d, c_load).await?;
            load_history(client, &mut rng, w_id, d).await?;
            let ol_cnts = load_orders(client, &mut rng, w_id, d).await?;
            load_new_order(client, w_id, d).await?;
            load_order_line(client, &mut rng, w_id, d, &ol_cnts).await?;
        }
    }

    Ok(())
}

async fn load_item(client: &Client, rng: &mut impl Rng) -> Result<()> {
    tracing::info!("loading item ({MAX_ITEMS} rows)");
    let stmt = client
        .prepare("INSERT INTO item (i_id, i_im_id, i_name, i_price, i_data) VALUES ($1, $2, $3, $4, $5)")
        .await
        .map_err(|source| crate::Error::Sql { action: "prepare item insert".into(), source })?;

    for batch_start in (1..=MAX_ITEMS).step_by(BATCH_SIZE) {
        let batch_end = (batch_start + i32::try_from(BATCH_SIZE).unwrap_or(i32::MAX) - 1).min(MAX_ITEMS);
        for i in batch_start..=batch_end {
            let i_im_id: i32 = rng.gen_range(1..=10_000);
            let i_price: f64 = f64::from(rng.gen_range(100..=10_000)) / 100.0;
            let i_name = tpcc_rand::rand_chars(rng, 14, 24);
            let i_data = tpcc_rand::rand_original_string(rng);

            client
                .execute(&stmt, &[&i, &i_im_id, &i_name, &i_price, &i_data])
                .await
                .map_err(|source| crate::Error::Sql { action: format!("insert item {i}"), source })?;
        }
    }
    Ok(())
}

async fn load_warehouse(client: &Client, rng: &mut impl Rng, w_id: i32) -> Result<()> {
    tracing::info!("loading warehouse {w_id}");
    let w_name = tpcc_rand::rand_chars(rng, 6, 10);
    let w_street_1 = tpcc_rand::rand_chars(rng, 10, 20);
    let w_street_2 = tpcc_rand::rand_chars(rng, 10, 20);
    let w_city = tpcc_rand::rand_chars(rng, 10, 20);
    let w_state = tpcc_rand::rand_state(rng);
    let w_zip = tpcc_rand::rand_zip(rng);
    let w_tax = tpcc_rand::rand_tax(rng);
    let w_ytd: f64 = 300_000.00;

    client
        .execute(
            "INSERT INTO warehouse (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            &[&w_id, &w_name, &w_street_1, &w_street_2, &w_city, &w_state, &w_zip, &w_tax, &w_ytd],
        )
        .await
        .map_err(|source| crate::Error::Sql { action: format!("insert warehouse {w_id}"), source })?;

    Ok(())
}

async fn load_district(client: &Client, rng: &mut impl Rng, w_id: i32) -> Result<()> {
    tracing::info!("loading district for warehouse {w_id}");
    let stmt = client
        .prepare(
            "INSERT INTO district (d_id, d_w_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
        )
        .await
        .map_err(|source| crate::Error::Sql { action: "prepare district insert".into(), source })?;

    for d in 1..=DISTRICTS_PER_WAREHOUSE {
        let d_name = tpcc_rand::rand_chars(rng, 6, 10);
        let d_street_1 = tpcc_rand::rand_chars(rng, 10, 20);
        let d_street_2 = tpcc_rand::rand_chars(rng, 10, 20);
        let d_city = tpcc_rand::rand_chars(rng, 10, 20);
        let d_state = tpcc_rand::rand_state(rng);
        let d_zip = tpcc_rand::rand_zip(rng);
        let d_tax = tpcc_rand::rand_tax(rng);
        let d_ytd: f64 = 30_000.00;
        let d_next_o_id: i32 = 3001;

        client
            .execute(
                &stmt,
                &[&d, &w_id, &d_name, &d_street_1, &d_street_2, &d_city, &d_state, &d_zip, &d_tax, &d_ytd, &d_next_o_id],
            )
            .await
            .map_err(|source| crate::Error::Sql { action: format!("insert district {d} warehouse {w_id}"), source })?;
    }
    Ok(())
}

async fn load_stock(client: &Client, rng: &mut impl Rng, w_id: i32) -> Result<()> {
    tracing::info!("loading stock for warehouse {w_id} ({STOCK_PER_WAREHOUSE} rows)");
    let stmt = client
        .prepare(
            "INSERT INTO stock (s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt, s_data) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)",
        )
        .await
        .map_err(|source| crate::Error::Sql { action: "prepare stock insert".into(), source })?;

    for i in 1..=STOCK_PER_WAREHOUSE {
        let s_quantity: i32 = rng.gen_range(10..=100);
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
        let s_ytd: i32 = 0;
        let s_order_cnt: i32 = 0;
        let s_remote_cnt: i32 = 0;
        let s_data = tpcc_rand::rand_original_string(rng);

        client
            .execute(
                &stmt,
                &[
                    &i, &w_id, &s_quantity,
                    &s_dist_01, &s_dist_02, &s_dist_03, &s_dist_04, &s_dist_05,
                    &s_dist_06, &s_dist_07, &s_dist_08, &s_dist_09, &s_dist_10,
                    &s_ytd, &s_order_cnt, &s_remote_cnt, &s_data,
                ],
            )
            .await
            .map_err(|source| crate::Error::Sql { action: format!("insert stock i_id={i} w_id={w_id}"), source })?;
    }
    Ok(())
}

async fn load_customer(
    client: &Client,
    rng: &mut impl Rng,
    w_id: i32,
    d_id: i32,
    c_load: usize,
) -> Result<()> {
    tracing::info!("loading customer for warehouse {w_id} district {d_id}");
    let stmt = client
        .prepare(
            "INSERT INTO customer (c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)",
        )
        .await
        .map_err(|source| crate::Error::Sql { action: "prepare customer insert".into(), source })?;

    for i in 1..=CUSTOMERS_PER_DISTRICT {
        let c_last = if i <= 1000 {
            tpcc_rand::c_last_syllables(usize::try_from(i - 1).unwrap_or(0))
        } else {
            tpcc_rand::rand_c_last(rng, c_load)
        };
        let c_first = tpcc_rand::rand_chars(rng, 8, 16);
        let c_middle = "OE";
        let c_street_1 = tpcc_rand::rand_chars(rng, 10, 20);
        let c_street_2 = tpcc_rand::rand_chars(rng, 10, 20);
        let c_city = tpcc_rand::rand_chars(rng, 10, 20);
        let c_state = tpcc_rand::rand_state(rng);
        let c_zip = tpcc_rand::rand_zip(rng);
        let c_phone = tpcc_rand::rand_numbers(rng, 16, 16);
        let c_since = INIT_LOAD_TIME;
        let c_credit = if rng.gen_range(0..10) == 0 { "BC" } else { "GC" };
        let c_credit_lim: f64 = 50_000.00;
        let c_discount: f64 = f64::from(rng.gen_range(0..=5_000)) / 10_000.0;
        let c_balance: f64 = -10.00;
        let c_ytd_payment: f64 = 10.00;
        let c_payment_cnt: i32 = 1;
        let c_delivery_cnt: i32 = 0;
        let c_data = tpcc_rand::rand_chars(rng, 300, 500);

        client
            .execute(
                &stmt,
                &[
                    &i, &d_id, &w_id, &c_first, &c_middle, &c_last,
                    &c_street_1, &c_street_2, &c_city, &c_state, &c_zip, &c_phone,
                    &c_since, &c_credit, &c_credit_lim, &c_discount, &c_balance,
                    &c_ytd_payment, &c_payment_cnt, &c_delivery_cnt, &c_data,
                ],
            )
            .await
            .map_err(|source| crate::Error::Sql { action: format!("insert customer {i} d={d_id} w={w_id}"), source })?;
    }
    Ok(())
}

async fn load_history(client: &Client, rng: &mut impl Rng, w_id: i32, d_id: i32) -> Result<()> {
    tracing::info!("loading history for warehouse {w_id} district {d_id}");
    let stmt = client
        .prepare(
            "INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .await
        .map_err(|source| crate::Error::Sql { action: "prepare history insert".into(), source })?;

    for i in 1..=CUSTOMERS_PER_DISTRICT {
        let h_amount: f64 = 10.00;
        let h_data = tpcc_rand::rand_chars(rng, 12, 24);

        client
            .execute(&stmt, &[&i, &d_id, &w_id, &d_id, &w_id, &INIT_LOAD_TIME, &h_amount, &h_data])
            .await
            .map_err(|source| crate::Error::Sql { action: format!("insert history c={i} d={d_id} w={w_id}"), source })?;
    }
    Ok(())
}

/// Load orders and return per-order `ol_cnt` values (needed by `load_order_line`).
async fn load_orders(
    client: &Client,
    rng: &mut impl Rng,
    w_id: i32,
    d_id: i32,
) -> Result<Vec<i32>> {
    tracing::info!("loading orders for warehouse {w_id} district {d_id}");
    let stmt = client
        .prepare(
            "INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .await
        .map_err(|source| crate::Error::Sql { action: "prepare orders insert".into(), source })?;

    // Random permutation of customer IDs
    let mut cids: Vec<i32> = (1..=ORDERS_PER_DISTRICT).collect();
    for i in (1..cids.len()).rev() {
        let j = rng.gen_range(0..=i);
        cids.swap(i, j);
    }

    let mut ol_cnts = Vec::with_capacity(usize::try_from(ORDERS_PER_DISTRICT).unwrap_or(0));

    for i in 0..ORDERS_PER_DISTRICT {
        let o_id = i + 1;
        let o_c_id = cids[usize::try_from(i).unwrap_or(0)];
        let o_carrier_id: Option<i32> = if o_id < 2101 {
            Some(rng.gen_range(1..=10))
        } else {
            None
        };
        let o_ol_cnt: i32 = rng.gen_range(5..=15);
        ol_cnts.push(o_ol_cnt);
        let o_all_local: i32 = 1;

        client
            .execute(
                &stmt,
                &[&o_id, &d_id, &w_id, &o_c_id, &INIT_LOAD_TIME, &o_carrier_id, &o_ol_cnt, &o_all_local],
            )
            .await
            .map_err(|source| crate::Error::Sql { action: format!("insert order {o_id} d={d_id} w={w_id}"), source })?;
    }

    Ok(ol_cnts)
}

async fn load_new_order(client: &Client, w_id: i32, d_id: i32) -> Result<()> {
    tracing::info!("loading new_order for warehouse {w_id} district {d_id}");
    let stmt = client
        .prepare("INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES ($1, $2, $3)")
        .await
        .map_err(|source| crate::Error::Sql { action: "prepare new_order insert".into(), source })?;

    for i in 0..NEW_ORDERS_PER_DISTRICT {
        let no_o_id = 2101 + i;
        client
            .execute(&stmt, &[&no_o_id, &d_id, &w_id])
            .await
            .map_err(|source| crate::Error::Sql { action: format!("insert new_order {no_o_id} d={d_id} w={w_id}"), source })?;
    }
    Ok(())
}

async fn load_order_line(
    client: &Client,
    rng: &mut impl Rng,
    w_id: i32,
    d_id: i32,
    ol_cnts: &[i32],
) -> Result<()> {
    tracing::info!("loading order_line for warehouse {w_id} district {d_id}");
    let stmt = client
        .prepare(
            "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .await
        .map_err(|source| crate::Error::Sql { action: "prepare order_line insert".into(), source })?;

    for (i, &ol_cnt) in ol_cnts.iter().enumerate() {
        let o_id = i32::try_from(i).unwrap_or(0) + 1;

        for j in 1..=ol_cnt {
            let ol_i_id: i32 = rng.gen_range(1..=100_000);
            let ol_quantity: i32 = 5;

            let (ol_delivery_d, ol_amount): (Option<&str>, f64) = if o_id < 2101 {
                (Some(INIT_LOAD_TIME), 0.00)
            } else {
                (None, f64::from(rng.gen_range(1..=999_999)) / 100.0)
            };
            let ol_dist_info = tpcc_rand::rand_chars(rng, 24, 24);

            client
                .execute(
                    &stmt,
                    &[&o_id, &d_id, &w_id, &j, &ol_i_id, &w_id, &ol_delivery_d, &ol_quantity, &ol_amount, &ol_dist_info],
                )
                .await
                .map_err(|source| crate::Error::Sql {
                    action: format!("insert order_line o={o_id} ol={j} d={d_id} w={w_id}"),
                    source,
                })?;
        }
    }
    Ok(())
}

// ─── CH-benCH supplemental tables (static data) ──────────────────────────────

/// TPC-H nation data. 25 nations with region keys.
/// Column order: (n_nationkey, n_name, n_regionkey).
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
    tracing::info!("loading nation ({total} rows)");
    let stmt = client
        .prepare("INSERT INTO nation (n_nationkey, n_name, n_regionkey, n_comment) VALUES ($1, $2, $3, $4)")
        .await
        .map_err(|source| crate::Error::Sql { action: "prepare nation insert".into(), source })?;

    let comment: Option<&str> = None;
    for &(key, name, region) in NATIONS.iter().chain(EXTRA_NATIONS.iter()) {
        client
            .execute(&stmt, &[&key, &name, &region, &comment])
            .await
            .map_err(|source| crate::Error::Sql { action: format!("insert nation {key}"), source })?;
    }
    Ok(())
}

async fn load_region(client: &Client) -> Result<()> {
    tracing::info!("loading region ({} rows)", REGIONS.len());
    let stmt = client
        .prepare("INSERT INTO region (r_regionkey, r_name, r_comment) VALUES ($1, $2, $3)")
        .await
        .map_err(|source| crate::Error::Sql { action: "prepare region insert".into(), source })?;

    let comment: Option<&str> = None;
    for &(key, name) in REGIONS {
        client
            .execute(&stmt, &[&key, &name, &comment])
            .await
            .map_err(|source| crate::Error::Sql { action: format!("insert region {key}"), source })?;
    }
    Ok(())
}

/// Load 10,000 supplier rows (matching go-tpc / TPC-H dbgen at SF1).
async fn load_supplier(client: &Client, rng: &mut impl Rng) -> Result<()> {
    const SUPPLIER_COUNT: i64 = 10_000;
    tracing::info!("loading supplier ({SUPPLIER_COUNT} rows)");
    let stmt = client
        .prepare("INSERT INTO supplier (s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment) VALUES ($1, $2, $3, $4, $5, $6, $7)")
        .await
        .map_err(|source| crate::Error::Sql { action: "prepare supplier insert".into(), source })?;

    let nation_count = i64::try_from(NATIONS.len()).unwrap_or(25);

    for i in 1..=SUPPLIER_COUNT {
        let s_name = format!("Supplier#{i:09}");
        let s_address = tpcc_rand::rand_chars(rng, 10, 25);
        let s_nationkey: i64 = rng.gen_range(0..nation_count);
        let s_phone = format!(
            "{:02}-{}-{}-{}",
            (s_nationkey + 10),
            rng.gen_range(100..=999),
            rng.gen_range(100..=999),
            rng.gen_range(1000..=9999),
        );
        let s_acctbal: f64 = f64::from(rng.gen_range(-99_999..=999_999)) / 100.0;
        let s_comment = tpcc_rand::rand_chars(rng, 25, 63);

        client
            .execute(&stmt, &[&i, &s_name, &s_address, &s_nationkey, &s_phone, &s_acctbal, &s_comment])
            .await
            .map_err(|source| crate::Error::Sql { action: format!("insert supplier {i}"), source })?;
    }
    Ok(())
}
