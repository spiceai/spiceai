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

//! TPC-C `OrderStatus` transaction (4% default mix).
//!
//! Read-only: look up the most recent order for a customer.
//! No WAL events — included for spec-compliant tpmC measurement.

use ::rand::{Rng, RngExt};
use tokio_postgres::Client;

use crate::Result;
use crate::rand as tpcc_rand;

/// # Errors
///
/// Returns an error if any database operation fails.
pub async fn run(client: &mut Client, rng: &mut impl Rng, warehouses: i32) -> Result<()> {
    let w_id = rng.random_range(1..=warehouses);
    let d_id = rng.random_range(1..=10);

    // 60% by last name, 40% by customer ID (spec 2.6.1.2)
    let by_name = rng.random_range(0..100) < 60;
    let c_load: usize = rng.random_range(0..256);

    let tx = client
        .transaction()
        .await
        .map_err(|source| crate::Error::Sql {
            action: "begin order_status transaction".into(),
            source,
        })?;

    let c_id: i32 = if by_name {
        let c_last = tpcc_rand::rand_c_last(rng, c_load);

        // Get count for name
        let cnt_row = tx
            .query_one(
                "SELECT count(c_id) FROM customer WHERE c_w_id = $1 AND c_d_id = $2 AND c_last = $3",
                &[&w_id, &d_id, &c_last],
            )
            .await
            .map_err(|source| crate::Error::Sql {
                action: "order_status: count customer by last".into(),
                source,
            })?;

        let name_cnt: i64 = cnt_row.get(0);
        let mut target = name_cnt;
        if target % 2 == 1 {
            target += 1;
        }

        let rows = tx
            .query(
                "SELECT c_balance, c_first, c_middle, c_id FROM customer WHERE c_w_id = $1 AND c_d_id = $2 AND c_last = $3 ORDER BY c_first",
                &[&w_id, &d_id, &c_last],
            )
            .await
            .map_err(|source| crate::Error::Sql {
                action: "order_status: select customer by last".into(),
                source,
            })?;

        if rows.is_empty() {
            tx.commit().await.map_err(|source| crate::Error::Sql {
                action: "order_status: commit (no customer)".into(),
                source,
            })?;
            return Ok(());
        }

        #[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let idx = ((target / 2) as usize).min(rows.len()) - 1;
        rows[idx.min(rows.len().saturating_sub(1))].get(3)
    } else {
        tpcc_rand::rand_customer_id(rng)
    };

    // SELECT customer
    let _c_row = tx
        .query_opt(
            "SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3",
            &[&w_id, &d_id, &c_id],
        )
        .await
        .map_err(|source| crate::Error::Sql {
            action: "order_status: select customer by id".into(),
            source,
        })?;

    // SELECT latest order
    let o_row = tx
        .query_opt(
            "SELECT o_id, o_carrier_id, o_entry_d FROM oorder WHERE o_w_id = $1 AND o_d_id = $2 AND o_c_id = $3 ORDER BY o_id DESC LIMIT 1",
            &[&w_id, &d_id, &c_id],
        )
        .await
        .map_err(|source| crate::Error::Sql {
            action: "order_status: select latest order".into(),
            source,
        })?;

    if let Some(o_row) = o_row {
        let o_id: i32 = o_row.get(0);

        // SELECT order lines
        let _ol_rows = tx
            .query(
                "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id = $3",
                &[&w_id, &d_id, &o_id],
            )
            .await
            .map_err(|source| crate::Error::Sql {
                action: "order_status: select order_line".into(),
                source,
            })?;
    }

    tx.commit().await.map_err(|source| crate::Error::Sql {
        action: "order_status: commit".into(),
        source,
    })?;

    Ok(())
}
