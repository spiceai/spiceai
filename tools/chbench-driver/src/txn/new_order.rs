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

//! TPC-C `NewOrder` transaction (45% default mix).
//!
//! Inserts an order with 5-15 line items. Updates `district.d_next_o_id` and stock quantities.
//! CDC impact: ~15-35 rows per transaction.

use std::time::SystemTime;

use ::rand::{Rng, RngExt};
use tokio_postgres::Client;

use super::TerminalAssignment;
use super::prepared::NewOrderStmts;
use crate::Result;
use crate::rand as tpcc_rand;

/// # Errors
///
/// Returns an error if any database operation fails.
pub async fn run(
    client: &mut Client,
    rng: &mut impl Rng,
    assignment: &TerminalAssignment,
    stmts: &NewOrderStmts,
) -> Result<()> {
    let w_id = assignment.home_w_id;
    let d_id = rng.random_range(assignment.district_lo..=assignment.district_hi);
    let c_id = tpcc_rand::rand_customer_id(rng);
    let ol_cnt = rng.random_range(5..=15);
    let rbk = rng.random_range(1..=100);

    // Generate order items
    #[expect(clippy::cast_sign_loss)]
    let mut items: Vec<(i32, i32, i32, i32)> = Vec::with_capacity(ol_cnt as usize); // (ol_i_id, ol_supply_w_id, ol_quantity, remote)
    let mut all_local = 1i32;

    for i in 0..ol_cnt {
        let ol_i_id = if i == ol_cnt - 1 && rbk == 1 {
            // 1% rollback: invalid item ID
            -1
        } else {
            rng.random_range(1..=100_000)
        };

        let (ol_supply_w_id, remote) =
            if assignment.num_warehouses == 1 || rng.random_range(1..=100) != 1 {
                (w_id, 0)
            } else {
                let mut other = rng.random_range(1..=assignment.num_warehouses);
                while other == w_id {
                    other = rng.random_range(1..=assignment.num_warehouses);
                }
                all_local = 0;
                (other, 1)
            };

        let ol_quantity = rng.random_range(1..=10);
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
    let customer_row = tx
        .query_one(&stmts.select_customer_warehouse, &[&w_id, &d_id, &c_id])
        .await
        .map_err(|source| crate::Error::Sql {
            action: "new_order: select customer".into(),
            source,
        })?;

    let c_discount: f64 = customer_row.get(0);
    let w_tax: f64 = customer_row.get(3);

    // 2. SELECT district FOR UPDATE
    let district_row = tx
        .query_one(&stmts.select_district, &[&d_id, &w_id])
        .await
        .map_err(|source| crate::Error::Sql {
            action: "new_order: select district".into(),
            source,
        })?;

    let d_next_o_id: i32 = district_row.get(0);
    let d_tax: f64 = district_row.get(1);

    // 3. UPDATE district d_next_o_id
    tx.execute(&stmts.update_district, &[&d_next_o_id, &d_id, &w_id])
        .await
        .map_err(|source| crate::Error::Sql {
            action: "new_order: update district".into(),
            source,
        })?;

    let o_id = d_next_o_id;
    let now = SystemTime::now();

    // 4. INSERT oorder
    tx.execute(
        &stmts.insert_oorder,
        &[&o_id, &d_id, &w_id, &c_id, &now, &ol_cnt, &all_local],
    )
    .await
    .map_err(|source| crate::Error::Sql {
        action: "new_order: insert oorder".into(),
        source,
    })?;

    // 5. INSERT new_order
    tx.execute(&stmts.insert_new_order, &[&o_id, &d_id, &w_id])
        .await
        .map_err(|source| crate::Error::Sql {
            action: "new_order: insert new_order".into(),
            source,
        })?;

    // 6-9. Process order lines in two phases (similar to BenchBase-style batching):
    //   Phase 1: Sequential reads (SELECT item + SELECT stock FOR UPDATE)
    //   Phase 2: Single batch_execute for all writes (UPDATE stock + INSERT order_line)

    // Collected write data from Phase 1.
    struct LineWrite {
        s_quantity: i32,
        ol_quantity: i32,
        remote: i32,
        ol_i_id: i32,
        ol_supply_w_id: i32,
        ol_number: i32,
        ol_amount: f64,
        ol_dist_info: String,
    }

    #[expect(clippy::cast_sign_loss)]
    let mut writes: Vec<LineWrite> = Vec::with_capacity(ol_cnt as usize);

    // Phase 1: reads — pipeline SELECT item + SELECT stock per item.
    for (ol_number_0, &(ol_i_id, ol_supply_w_id, ol_quantity, remote)) in items.iter().enumerate() {
        let ol_number = i32::try_from(ol_number_0).unwrap_or(0) + 1;

        // Check for rollback item
        if ol_i_id < 0 {
            tx.rollback().await.map_err(|source| crate::Error::Sql {
                action: "new_order: rollback".into(),
                source,
            })?;
            return Ok(());
        }

        // Pipeline: SELECT item + SELECT stock FOR UPDATE in one round-trip
        let stock_stmt = &stmts.select_stock[usize::try_from(d_id - 1).unwrap_or(0)];
        let item_params: [&(dyn tokio_postgres::types::ToSql + Sync); 1] = [&ol_i_id];
        let stock_params_sel: [&(dyn tokio_postgres::types::ToSql + Sync); 2] =
            [&ol_i_id, &ol_supply_w_id];
        let (item_row, stock_row) = tokio::try_join!(
            tx.query_one(&stmts.select_item, &item_params),
            tx.query_one(stock_stmt, &stock_params_sel),
        )
        .map_err(|source| crate::Error::Sql {
            action: "new_order: select item+stock".into(),
            source,
        })?;

        let i_price: f64 = item_row.get(0);

        let mut s_quantity: i32 = stock_row.get(0);
        let ol_dist_info: String = stock_row.get(2);

        s_quantity -= ol_quantity;
        if s_quantity < 10 {
            s_quantity += 91;
        }

        let ol_amount =
            f64::from(ol_quantity) * i_price * (1.0 + w_tax + d_tax) * (1.0 - c_discount);

        writes.push(LineWrite {
            s_quantity,
            ol_quantity,
            remote,
            ol_i_id,
            ol_supply_w_id,
            ol_number,
            ol_amount,
            ol_dist_info,
        });
    }

    // Phase 2: batch all writes in a single round-trip via simple query protocol.
    //
    // Uses `batch_execute` (simple query)  which does not support prepared statements
    // but batching is still faster than using prepared statements with multiple round-trips.
    use std::fmt::Write;
    let mut batch_sql = String::with_capacity(writes.len() * 200);

    for w in &writes {
        write!(
            &mut batch_sql,
            "UPDATE stock SET s_quantity = {}, s_ytd = s_ytd + {}, \
             s_order_cnt = s_order_cnt + 1, s_remote_cnt = s_remote_cnt + {} \
             WHERE s_i_id = {} AND s_w_id = {};",
            w.s_quantity, w.ol_quantity, w.remote, w.ol_i_id, w.ol_supply_w_id
        )
        .unwrap_or(());
    }

    batch_sql.push_str(
        "INSERT INTO order_line \
         (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, \
          ol_quantity, ol_amount, ol_dist_info) VALUES ",
    );
    for (i, w) in writes.iter().enumerate() {
        if i > 0 {
            batch_sql.push(',');
        }
        // ol_dist_info is a fixed 24-char alphanumeric string from stock table;
        // escape single quotes defensively.
        let escaped_dist = w.ol_dist_info.replace('\'', "''");
        write!(
            &mut batch_sql,
            "({}, {}, {}, {}, {}, {}, {}, {}, '{escaped_dist}')",
            o_id, d_id, w_id, w.ol_number, w.ol_i_id, w.ol_supply_w_id, w.ol_quantity, w.ol_amount
        )
        .unwrap_or(());
    }
    batch_sql.push(';');

    tx.batch_execute(&batch_sql)
        .await
        .map_err(|source| crate::Error::Sql {
            action: "new_order: batch write stock+order_line".into(),
            source,
        })?;

    tx.commit().await.map_err(|source| crate::Error::Sql {
        action: "new_order: commit".into(),
        source,
    })?;

    Ok(())
}
