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

use ::rand::Rng;
use tokio_postgres::Client;

use crate::Result;
use crate::rand as tpcc_rand;

pub async fn run(client: &mut Client, rng: &mut impl Rng, warehouses: i32) -> Result<()> {
    let w_id = rng.gen_range(1..=warehouses);
    let d_id = rng.gen_range(1..=10);
    let h_amount: f64 = f64::from(rng.gen_range(100..=500_000)) / 100.0;

    // 60% by last name, 40% by customer ID (spec 2.5.1.2)
    let by_name = rng.gen_range(0..100) < 60;
    let c_last = if by_name {
        let c_load: usize = rng.gen_range(0..256);
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
    let (c_w_id, c_d_id) = if warehouses == 1 || rng.gen_range(0..100) < 85 {
        (w_id, d_id)
    } else {
        let mut other = rng.gen_range(1..=warehouses);
        while other == w_id {
            other = rng.gen_range(1..=warehouses);
        }
        (other, rng.gen_range(1..=10))
    };

    let tx = client
        .transaction()
        .await
        .map_err(|source| crate::Error::Sql {
            action: "begin payment transaction".into(),
            source,
        })?;

    // 1. UPDATE warehouse
    tx.execute(
        "UPDATE warehouse SET w_ytd = w_ytd + $1 WHERE w_id = $2",
        &[&h_amount, &w_id],
    )
    .await
    .map_err(|source| crate::Error::Sql {
        action: "payment: update warehouse".into(),
        source,
    })?;

    // 2. SELECT warehouse
    let w_row = tx
        .query_one(
            "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = $1",
            &[&w_id],
        )
        .await
        .map_err(|source| crate::Error::Sql {
            action: "payment: select warehouse".into(),
            source,
        })?;

    let w_name: String = w_row.get(5);

    // 3. UPDATE district
    tx.execute(
        "UPDATE district SET d_ytd = d_ytd + $1 WHERE d_w_id = $2 AND d_id = $3",
        &[&h_amount, &w_id, &d_id],
    )
    .await
    .map_err(|source| crate::Error::Sql {
        action: "payment: update district".into(),
        source,
    })?;

    // 4. SELECT district
    let d_row = tx
        .query_one(
            "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district WHERE d_w_id = $1 AND d_id = $2",
            &[&w_id, &d_id],
        )
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
                "SELECT c_id FROM customer WHERE c_w_id = $1 AND c_d_id = $2 AND c_last = $3 ORDER BY c_first",
                &[&c_w_id, &c_d_id, &c_last.as_deref().unwrap_or("")],
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
        let idx = (rows.len() + 1) / 2 - 1;
        c_id = rows[idx].get(0);
    }

    // 6. SELECT customer FOR UPDATE
    let c_row = tx
        .query_one(
            "SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3 FOR UPDATE",
            &[&c_w_id, &c_d_id, &c_id],
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
                "SELECT c_data FROM customer WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3",
                &[&c_w_id, &c_d_id, &c_id],
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
            "| {c_id:4} {c_d_id:2} {c_w_id:4} {d_id:2} {w_id:4} ${h_amount:7.2} {now_secs}"
        );
        let mut new_data = new_prefix;
        let remaining = 500 - new_data.len().min(500);
        if remaining > 0 {
            let end = remaining.min(old_data.len());
            new_data.push_str(&old_data[..end]);
        }

        tx.execute(
            "UPDATE customer SET c_balance = c_balance - $1, c_ytd_payment = c_ytd_payment + $2, c_payment_cnt = c_payment_cnt + 1, c_data = $3 WHERE c_w_id = $4 AND c_d_id = $5 AND c_id = $6",
            &[&h_amount, &h_amount, &new_data, &c_w_id, &c_d_id, &c_id],
        )
        .await
        .map_err(|source| crate::Error::Sql {
            action: "payment: update customer with data".into(),
            source,
        })?;
    } else {
        tx.execute(
            "UPDATE customer SET c_balance = c_balance - $1, c_ytd_payment = c_ytd_payment + $2, c_payment_cnt = c_payment_cnt + 1 WHERE c_w_id = $3 AND c_d_id = $4 AND c_id = $5",
            &[&h_amount, &h_amount, &c_w_id, &c_d_id, &c_id],
        )
        .await
        .map_err(|source| crate::Error::Sql {
            action: "payment: update customer".into(),
            source,
        })?;
    }

    // 9. INSERT history
    let h_data = format!("{w_name:>10}    {d_name:>10}");
    let h_date = SystemTime::now();
    tx.execute(
        "INSERT INTO history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        &[&c_d_id, &c_w_id, &c_id, &d_id, &w_id, &h_date, &h_amount, &h_data],
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
