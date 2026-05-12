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

//! TPC-C NewOrder transaction (45% default mix).
//!
//! Inserts an order with 5-15 line items. Updates district.d_next_o_id and stock quantities.
//! CDC impact: ~15-35 rows per transaction.

use std::time::SystemTime;

use ::rand::Rng;
use tokio_postgres::Client;

use crate::rand as tpcc_rand;
use crate::Result;

pub async fn run(client: &mut Client, rng: &mut impl Rng, warehouses: i32) -> Result<()> {
    let w_id = rng.gen_range(1..=warehouses);
    let d_id = rng.gen_range(1..=10);
    let c_id = tpcc_rand::rand_customer_id(rng);
    let ol_cnt = rng.gen_range(5..=15);
    let rbk = rng.gen_range(1..=100);

    // Generate order items
    let mut items: Vec<(i32, i32, i32, i32)> = Vec::with_capacity(ol_cnt as usize); // (ol_i_id, ol_supply_w_id, ol_quantity, remote)
    let mut all_local = 1i32;

    for i in 0..ol_cnt {
        let ol_i_id = if i == ol_cnt - 1 && rbk == 1 {
            // 1% rollback: invalid item ID
            -1
        } else {
            rng.gen_range(1..=100_000)
        };

        let (ol_supply_w_id, remote) = if warehouses == 1 || rng.gen_range(1..=100) != 1 {
            (w_id, 0)
        } else {
            let mut other = rng.gen_range(1..=warehouses);
            while other == w_id {
                other = rng.gen_range(1..=warehouses);
            }
            all_local = 0;
            (other, 1)
        };

        let ol_quantity = rng.gen_range(1..=10);
        items.push((ol_i_id, ol_supply_w_id, ol_quantity, remote));
    }

    let tx = client
        .transaction()
        .await
        .map_err(|source| crate::Error::Sql {
            action: "begin new_order transaction".into(),
            source,
        })?;

    // 1. SELECT customer + warehouse info
    let _customer_row = tx
        .query_one(
            "SELECT c_discount, c_last, c_credit, w_tax FROM customer, warehouse WHERE w_id = $1 AND c_w_id = w_id AND c_d_id = $2 AND c_id = $3",
            &[&w_id, &d_id, &c_id],
        )
        .await
        .map_err(|source| crate::Error::Sql {
            action: "new_order: select customer".into(),
            source,
        })?;

    let c_discount: f64 = _customer_row.get(0);
    let w_tax: f64 = _customer_row.get(3);

    // 2. SELECT district FOR UPDATE
    let district_row = tx
        .query_one(
            "SELECT d_next_o_id, d_tax FROM district WHERE d_id = $1 AND d_w_id = $2 FOR UPDATE",
            &[&d_id, &w_id],
        )
        .await
        .map_err(|source| crate::Error::Sql {
            action: "new_order: select district".into(),
            source,
        })?;

    let d_next_o_id: i32 = district_row.get(0);
    let d_tax: f64 = district_row.get(1);

    // 3. UPDATE district d_next_o_id
    tx.execute(
        "UPDATE district SET d_next_o_id = $1 + 1 WHERE d_id = $2 AND d_w_id = $3",
        &[&d_next_o_id, &d_id, &w_id],
    )
    .await
    .map_err(|source| crate::Error::Sql {
        action: "new_order: update district".into(),
        source,
    })?;

    let o_id = d_next_o_id;
    let now = SystemTime::now();

    // 4. INSERT orders
    tx.execute(
        "INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES ($1, $2, $3, $4, $5, $6, $7)",
        &[&o_id, &d_id, &w_id, &c_id, &now, &ol_cnt, &all_local],
    )
    .await
    .map_err(|source| crate::Error::Sql {
        action: "new_order: insert orders".into(),
        source,
    })?;

    // 5. INSERT new_order
    tx.execute(
        "INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES ($1, $2, $3)",
        &[&o_id, &d_id, &w_id],
    )
    .await
    .map_err(|source| crate::Error::Sql {
        action: "new_order: insert new_order".into(),
        source,
    })?;

    // 6-9. Process each order line: select item, select/update stock, insert order_line
    for (ol_number_0, &(ol_i_id, ol_supply_w_id, ol_quantity, remote)) in items.iter().enumerate()
    {
        let ol_number = i32::try_from(ol_number_0).unwrap_or(0) + 1;

        // Check for rollback item
        if ol_i_id < 0 {
            tx.execute("ROLLBACK", &[]).await.ok();
            return Ok(());
        }

        // Select item
        let item_row = tx
            .query_one(
                "SELECT i_price, i_name, i_data FROM item WHERE i_id = $1",
                &[&ol_i_id],
            )
            .await
            .map_err(|source| crate::Error::Sql {
                action: "new_order: select item".into(),
                source,
            })?;

        let i_price: f64 = item_row.get(0);

        // Select stock FOR UPDATE
        let dist_col = format!("s_dist_{d_id:02}");
        let stock_sql = format!(
            "SELECT s_quantity, s_data, {dist_col} FROM stock WHERE s_i_id = $1 AND s_w_id = $2 FOR UPDATE"
        );
        let stock_row = tx
            .query_one(&stock_sql, &[&ol_i_id, &ol_supply_w_id])
            .await
            .map_err(|source| crate::Error::Sql {
                action: "new_order: select stock".into(),
                source,
            })?;

        let mut s_quantity: i32 = stock_row.get(0);
        let ol_dist_info: String = stock_row.get(2);

        s_quantity -= ol_quantity;
        if s_quantity < 10 {
            s_quantity += 91;
        }

        // Update stock
        tx.execute(
            "UPDATE stock SET s_quantity = $1, s_ytd = s_ytd + $2, s_order_cnt = s_order_cnt + 1, s_remote_cnt = s_remote_cnt + $3 WHERE s_i_id = $4 AND s_w_id = $5",
            &[&s_quantity, &ol_quantity, &remote, &ol_i_id, &ol_supply_w_id],
        )
        .await
        .map_err(|source| crate::Error::Sql {
            action: "new_order: update stock".into(),
            source,
        })?;

        // Calculate amount
        let ol_amount =
            f64::from(ol_quantity) * i_price * (1.0 + w_tax + d_tax) * (1.0 - c_discount);

        // Insert order_line
        tx.execute(
            "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            &[&o_id, &d_id, &w_id, &ol_number, &ol_i_id, &ol_supply_w_id, &ol_quantity, &ol_amount, &ol_dist_info],
        )
        .await
        .map_err(|source| crate::Error::Sql {
            action: "new_order: insert order_line".into(),
            source,
        })?;
    }

    tx.commit().await.map_err(|source| crate::Error::Sql {
        action: "new_order: commit".into(),
        source,
    })?;

    Ok(())
}
