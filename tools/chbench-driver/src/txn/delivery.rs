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

//! TPC-C Delivery transaction (4% default mix).
//!
//! Delivers the oldest pending order for each of 10 districts.
//! CDC impact: ~30 rows per transaction (10 DELETEs + 10+10 UPDATEs + up to 10 customer UPDATEs).

use ::rand::Rng;
use tokio_postgres::Client;

use crate::Result;

const TIME_FORMAT: &str = "2026-01-02 15:04:05";

pub async fn run(client: &mut Client, rng: &mut impl Rng, warehouses: i32) -> Result<()> {
    let w_id = rng.gen_range(1..=warehouses);
    let o_carrier_id = rng.gen_range(1..=10);

    let tx = client
        .transaction()
        .await
        .map_err(|source| crate::Error::Sql {
            action: "begin delivery transaction".into(),
            source,
        })?;

    for d_id in 1..=10i32 {
        // 1. Find oldest undelivered order
        let rows = tx
            .query(
                "SELECT no_o_id FROM new_order WHERE no_w_id = $1 AND no_d_id = $2 ORDER BY no_o_id ASC LIMIT 1 FOR UPDATE",
                &[&w_id, &d_id],
            )
            .await
            .map_err(|source| crate::Error::Sql {
                action: "delivery: select new_order".into(),
                source,
            })?;

        if rows.is_empty() {
            continue;
        }

        let no_o_id: i32 = rows[0].get(0);

        // 2. DELETE from new_order
        tx.execute(
            "DELETE FROM new_order WHERE no_w_id = $1 AND no_d_id = $2 AND no_o_id = $3",
            &[&w_id, &d_id, &no_o_id],
        )
        .await
        .map_err(|source| crate::Error::Sql {
            action: "delivery: delete new_order".into(),
            source,
        })?;

        // 3. UPDATE orders with carrier
        tx.execute(
            "UPDATE orders SET o_carrier_id = $1 WHERE o_w_id = $2 AND o_d_id = $3 AND o_id = $4",
            &[&o_carrier_id, &w_id, &d_id, &no_o_id],
        )
        .await
        .map_err(|source| crate::Error::Sql {
            action: "delivery: update orders".into(),
            source,
        })?;

        // 4. Get customer ID for this order
        let o_row = tx
            .query_one(
                "SELECT o_c_id FROM orders WHERE o_w_id = $1 AND o_d_id = $2 AND o_id = $3",
                &[&w_id, &d_id, &no_o_id],
            )
            .await
            .map_err(|source| crate::Error::Sql {
                action: "delivery: select orders c_id".into(),
                source,
            })?;

        let o_c_id: i32 = o_row.get(0);

        // 5. UPDATE order_line delivery date
        tx.execute(
            "UPDATE order_line SET ol_delivery_d = $1 WHERE ol_w_id = $2 AND ol_d_id = $3 AND ol_o_id = $4",
            &[&TIME_FORMAT, &w_id, &d_id, &no_o_id],
        )
        .await
        .map_err(|source| crate::Error::Sql {
            action: "delivery: update order_line".into(),
            source,
        })?;

        // 6. SUM order_line amounts
        let sum_row = tx
            .query_one(
                "SELECT COALESCE(SUM(ol_amount), 0) FROM order_line WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id = $3",
                &[&w_id, &d_id, &no_o_id],
            )
            .await
            .map_err(|source| crate::Error::Sql {
                action: "delivery: sum order_line amount".into(),
                source,
            })?;

        let total_amount: f64 = sum_row.get(0);

        // 7. UPDATE customer balance
        tx.execute(
            "UPDATE customer SET c_balance = c_balance + $1, c_delivery_cnt = c_delivery_cnt + 1 WHERE c_w_id = $2 AND c_d_id = $3 AND c_id = $4",
            &[&total_amount, &w_id, &d_id, &o_c_id],
        )
        .await
        .map_err(|source| crate::Error::Sql {
            action: "delivery: update customer".into(),
            source,
        })?;
    }

    tx.commit().await.map_err(|source| crate::Error::Sql {
        action: "delivery: commit".into(),
        source,
    })?;

    Ok(())
}
