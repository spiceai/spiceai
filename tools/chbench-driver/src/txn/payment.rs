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

//! TPC-C Payment transaction (43% default mix).
//!
//! Updates warehouse/district/customer balances and inserts a history record.
//! CDC impact: 4 rows per transaction (3 UPDATEs + 1 INSERT).

use std::time::SystemTime;

use ::rand::{Rng, RngExt};
use tokio_postgres::Client;

use crate::Result;
use crate::rand as tpcc_rand;
use super::prepared::PaymentStmts;

/// # Errors
///
/// Returns an error if any database operation fails.
pub async fn run(client: &mut Client, rng: &mut impl Rng, warehouses: i32, stmts: &PaymentStmts) -> Result<()> {
    let w_id = rng.random_range(1..=warehouses);
    let d_id = rng.random_range(1..=10);
    let h_amount: f64 = f64::from(rng.random_range(100..=500_000)) / 100.0;

    // 60% by last name, 40% by customer ID (spec 2.5.1.2)
    let by_name = rng.random_range(0..100) < 60;
    let c_last = if by_name {
        let c_load: usize = rng.random_range(0..256);
        Some(tpcc_rand::rand_c_last(rng, c_load))
    } else {
        None
    };
    let mut c_id = if by_name {
        0
    } else {
        tpcc_rand::rand_customer_id(rng)
    };

    // 85% local, 15% remote (spec 2.5.1.2)
    let (customer_wh, customer_dist) = if warehouses == 1 || rng.random_range(0..100) < 85 {
        (w_id, d_id)
    } else {
        let mut other = rng.random_range(1..=warehouses);
        while other == w_id {
            other = rng.random_range(1..=warehouses);
        }
        (other, rng.random_range(1..=10))
    };

    let tx = client
        .transaction()
        .await
        .map_err(|source| crate::Error::Sql {
            action: "begin payment transaction".into(),
            source,
        })?;

    // 1. UPDATE warehouse
    tx.execute(&stmts.update_warehouse, &[&h_amount, &w_id])
        .await
        .map_err(|source| crate::Error::Sql {
            action: "payment: update warehouse".into(),
            source,
        })?;

    // 2. SELECT warehouse
    let w_row = tx
        .query_one(&stmts.select_warehouse, &[&w_id])
        .await
        .map_err(|source| crate::Error::Sql {
            action: "payment: select warehouse".into(),
            source,
        })?;

    let w_name: String = w_row.get(5);

    // 3. UPDATE district
    tx.execute(&stmts.update_district, &[&h_amount, &w_id, &d_id])
        .await
        .map_err(|source| crate::Error::Sql {
            action: "payment: update district".into(),
            source,
        })?;

    // 4. SELECT district
    let d_row = tx
        .query_one(&stmts.select_district, &[&w_id, &d_id])
        .await
        .map_err(|source| crate::Error::Sql {
            action: "payment: select district".into(),
            source,
        })?;

    let d_name: String = d_row.get(5);

    // 5. Resolve customer ID if by last name
    if by_name {
        let rows = tx
            .query(
                &stmts.select_customer_by_last,
                &[&customer_wh, &customer_dist, &c_last.as_deref().unwrap_or("")],
            )
            .await
            .map_err(|source| crate::Error::Sql {
                action: "payment: select customer by last name".into(),
                source,
            })?;

        if rows.is_empty() {
            // Customer not found — skip (can happen with random data)
            tx.commit().await.map_err(|source| crate::Error::Sql {
                action: "payment: commit (no customer)".into(),
                source,
            })?;
            return Ok(());
        }

        // Pick the middle customer (spec 2.5.2.2)
        let idx = rows.len().div_ceil(2) - 1;
        c_id = rows[idx].get(0);
    }

    // 6. SELECT customer FOR UPDATE
    let c_row = tx
        .query_one(
            &stmts.select_customer_for_update,
            &[&customer_wh, &customer_dist, &c_id],
        )
        .await
        .map_err(|source| crate::Error::Sql {
            action: "payment: select customer for update".into(),
            source,
        })?;

    let c_credit: String = c_row.get(9);

    // 7-8. Update customer (with data for "BC" credit)
    if c_credit.trim() == "BC" {
        let c_data_row = tx
            .query_one(
                &stmts.select_customer_data,
                &[&customer_wh, &customer_dist, &c_id],
            )
            .await
            .map_err(|source| crate::Error::Sql {
                action: "payment: select customer data".into(),
                source,
            })?;

        let old_data: String = c_data_row.get(0);
        let now_secs = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let new_prefix = format!(
            "| {c_id:4} {customer_dist:2} {customer_wh:4} {d_id:2} {w_id:4} ${h_amount:7.2} {now_secs}"
        );
        let mut new_data = new_prefix;
        let remaining = 500 - new_data.len().min(500);
        if remaining > 0 {
            let end = remaining.min(old_data.len());
            new_data.push_str(&old_data[..end]);
        }

        tx.execute(
            &stmts.update_customer_with_data,
            &[&h_amount, &h_amount, &new_data, &customer_wh, &customer_dist, &c_id],
        )
        .await
        .map_err(|source| crate::Error::Sql {
            action: "payment: update customer with data".into(),
            source,
        })?;
    } else {
        tx.execute(
            &stmts.update_customer,
            &[&h_amount, &h_amount, &customer_wh, &customer_dist, &c_id],
        )
        .await
        .map_err(|source| crate::Error::Sql {
            action: "payment: update customer".into(),
            source,
        })?;
    }

    // 9. INSERT history
    let history_data = format!("{w_name:>10}    {d_name:>10}");
    let history_ts = SystemTime::now();
    tx.execute(
        &stmts.insert_history,
        &[&customer_dist, &customer_wh, &c_id, &d_id, &w_id, &history_ts, &h_amount, &history_data],
    )
    .await
    .map_err(|source| crate::Error::Sql {
        action: "payment: insert history".into(),
        source,
    })?;

    tx.commit().await.map_err(|source| crate::Error::Sql {
        action: "payment: commit".into(),
        source,
    })?;

    Ok(())
}
