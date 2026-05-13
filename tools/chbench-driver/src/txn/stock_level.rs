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

//! TPC-C StockLevel transaction (4% default mix).
//!
//! Read-only: count distinct items in recent orders with stock below threshold.
//! No WAL events — included for spec-compliant tpmC measurement.

use ::rand::{Rng, RngExt};
use tokio_postgres::Client;

use crate::Result;

pub async fn run(client: &mut Client, rng: &mut impl Rng, warehouses: i32) -> Result<()> {
    let w_id = rng.random_range(1..=warehouses);
    let d_id = rng.random_range(1..=10);
    let threshold = rng.random_range(10..=20);

    let tx = client
        .transaction()
        .await
        .map_err(|source| crate::Error::Sql {
            action: "begin stock_level transaction".into(),
            source,
        })?;

    // SELECT d_next_o_id
    let d_row = tx
        .query_one(
            "SELECT d_next_o_id FROM district WHERE d_w_id = $1 AND d_id = $2",
            &[&w_id, &d_id],
        )
        .await
        .map_err(|source| crate::Error::Sql {
            action: "stock_level: select district".into(),
            source,
        })?;

    let o_id: i32 = d_row.get(0);

    // Count low-stock items in last 20 orders
    let _count_row = tx
        .query_one(
            "SELECT COUNT(DISTINCT s_i_id) FROM order_line, stock WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id < $3 AND ol_o_id >= $3 - 20 AND s_w_id = $1 AND s_i_id = ol_i_id AND s_quantity < $4",
            &[&w_id, &d_id, &o_id, &threshold],
        )
        .await
        .map_err(|source| crate::Error::Sql {
            action: "stock_level: count low stock".into(),
            source,
        })?;

    tx.commit().await.map_err(|source| crate::Error::Sql {
        action: "stock_level: commit".into(),
        source,
    })?;

    Ok(())
}
