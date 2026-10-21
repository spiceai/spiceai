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

//! Pre-prepared statement handles for TPC-C transactions.
//!
//! Preparing statements once per connection eliminates repeated Parse+Plan overhead
//! on every transaction execution significantly improving performance.

use tokio_postgres::{Client, Statement};

use crate::Result;

/// Prepared statements for the `NewOrder` transaction.
pub struct NewOrderStmts {
    pub select_customer_warehouse: Statement,
    pub select_district: Statement,
    pub update_district: Statement,
    pub insert_oorder: Statement,
    pub insert_new_order: Statement,
    pub select_item: Statement,
    /// One prepared statement per district (`s_dist_01` through `s_dist_10`).
    pub select_stock: [Statement; 10],
    pub update_stock: Statement,
    pub insert_order_line: Statement,
}

/// Prepared statements for the Payment transaction.
pub struct PaymentStmts {
    pub update_warehouse: Statement,
    pub select_warehouse: Statement,
    pub update_district: Statement,
    pub select_district: Statement,
    pub select_customer_by_last: Statement,
    pub select_customer_for_update: Statement,
    pub select_customer_data: Statement,
    pub update_customer_with_data: Statement,
    pub update_customer: Statement,
    pub insert_history: Statement,
}

/// All prepared statements for the OLTP terminal, grouped by transaction type.
///
/// Created once per connection via [`PreparedStatements::prepare`], then reused
/// across all transaction invocations for that terminal.
pub struct PreparedStatements {
    pub new_order: NewOrderStmts,
    pub payment: PaymentStmts,
}

impl PreparedStatements {
    /// Prepare all statements on the given client connection.
    ///
    /// # Errors
    ///
    /// Returns an error if any statement fails to prepare (e.g., schema mismatch).
    pub async fn prepare(client: &Client) -> Result<Self> {
        let new_order = Self::prepare_new_order(client).await?;
        let payment = Self::prepare_payment(client).await?;
        Ok(Self { new_order, payment })
    }

    async fn prepare_new_order(client: &Client) -> Result<NewOrderStmts> {
        let select_customer_warehouse = client
            .prepare(
                "SELECT c_discount, c_last, c_credit, w_tax \
                 FROM customer, warehouse \
                 WHERE w_id = $1 AND c_w_id = w_id AND c_d_id = $2 AND c_id = $3",
            )
            .await
            .map_err(|source| crate::Error::Sql {
                action: "prepare new_order: select_customer_warehouse".into(),
                source,
            })?;

        let select_district = client
            .prepare(
                "SELECT d_next_o_id, d_tax FROM district \
                 WHERE d_id = $1 AND d_w_id = $2 FOR UPDATE",
            )
            .await
            .map_err(|source| crate::Error::Sql {
                action: "prepare new_order: select_district".into(),
                source,
            })?;

        let update_district = client
            .prepare(
                "UPDATE district SET d_next_o_id = $1 + 1 \
                 WHERE d_id = $2 AND d_w_id = $3",
            )
            .await
            .map_err(|source| crate::Error::Sql {
                action: "prepare new_order: update_district".into(),
                source,
            })?;

        let insert_oorder = client
            .prepare(
                "INSERT INTO oorder (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
            )
            .await
            .map_err(|source| crate::Error::Sql {
                action: "prepare new_order: insert_oorder".into(),
                source,
            })?;

        let insert_new_order = client
            .prepare("INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES ($1, $2, $3)")
            .await
            .map_err(|source| crate::Error::Sql {
                action: "prepare new_order: insert_new_order".into(),
                source,
            })?;

        let select_item = client
            .prepare("SELECT i_price, i_name, i_data FROM item WHERE i_id = $1")
            .await
            .map_err(|source| crate::Error::Sql {
                action: "prepare new_order: select_item".into(),
                source,
            })?;

        // Prepare 10 variants for s_dist_01..s_dist_10
        let mut select_stock_vec = Vec::with_capacity(10);
        for dist in 1..=10 {
            let sql = format!(
                "SELECT s_quantity, s_data, s_dist_{dist:02} FROM stock \
                 WHERE s_i_id = $1 AND s_w_id = $2 FOR UPDATE"
            );
            let stmt = client
                .prepare(&sql)
                .await
                .map_err(|source| crate::Error::Sql {
                    action: format!("prepare new_order: select_stock[{dist}]"),
                    source,
                })?;
            select_stock_vec.push(stmt);
        }
        let Ok(select_stock): Result<[Statement; 10], _> = select_stock_vec.try_into() else {
            unreachable!("exactly 10 elements pushed")
        };

        let update_stock = client
            .prepare(
                "UPDATE stock SET s_quantity = $1, s_ytd = s_ytd + $2, \
                 s_order_cnt = s_order_cnt + 1, s_remote_cnt = s_remote_cnt + $3 \
                 WHERE s_i_id = $4 AND s_w_id = $5",
            )
            .await
            .map_err(|source| crate::Error::Sql {
                action: "prepare new_order: update_stock".into(),
                source,
            })?;

        let insert_order_line = client
            .prepare(
                "INSERT INTO order_line \
                 (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            )
            .await
            .map_err(|source| crate::Error::Sql {
                action: "prepare new_order: insert_order_line".into(),
                source,
            })?;

        Ok(NewOrderStmts {
            select_customer_warehouse,
            select_district,
            update_district,
            insert_oorder,
            insert_new_order,
            select_item,
            select_stock,
            update_stock,
            insert_order_line,
        })
    }

    async fn prepare_payment(client: &Client) -> Result<PaymentStmts> {
        let update_warehouse = client
            .prepare("UPDATE warehouse SET w_ytd = w_ytd + $1 WHERE w_id = $2")
            .await
            .map_err(|source| crate::Error::Sql {
                action: "prepare payment: update_warehouse".into(),
                source,
            })?;

        let select_warehouse = client
            .prepare(
                "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name \
                 FROM warehouse WHERE w_id = $1",
            )
            .await
            .map_err(|source| crate::Error::Sql {
                action: "prepare payment: select_warehouse".into(),
                source,
            })?;

        let update_district = client
            .prepare("UPDATE district SET d_ytd = d_ytd + $1 WHERE d_w_id = $2 AND d_id = $3")
            .await
            .map_err(|source| crate::Error::Sql {
                action: "prepare payment: update_district".into(),
                source,
            })?;

        let select_district = client
            .prepare(
                "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name \
                 FROM district WHERE d_w_id = $1 AND d_id = $2",
            )
            .await
            .map_err(|source| crate::Error::Sql {
                action: "prepare payment: select_district".into(),
                source,
            })?;

        let select_customer_by_last = client
            .prepare(
                "SELECT c_id FROM customer \
                 WHERE c_w_id = $1 AND c_d_id = $2 AND c_last = $3 ORDER BY c_first",
            )
            .await
            .map_err(|source| crate::Error::Sql {
                action: "prepare payment: select_customer_by_last".into(),
                source,
            })?;

        let select_customer_for_update = client
            .prepare(
                "SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, \
                 c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since \
                 FROM customer WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3 FOR UPDATE",
            )
            .await
            .map_err(|source| crate::Error::Sql {
                action: "prepare payment: select_customer_for_update".into(),
                source,
            })?;

        let select_customer_data = client
            .prepare("SELECT c_data FROM customer WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3")
            .await
            .map_err(|source| crate::Error::Sql {
                action: "prepare payment: select_customer_data".into(),
                source,
            })?;

        let update_customer_with_data = client
            .prepare(
                "UPDATE customer SET c_balance = c_balance - $1, c_ytd_payment = c_ytd_payment + $2, \
                 c_payment_cnt = c_payment_cnt + 1, c_data = $3 \
                 WHERE c_w_id = $4 AND c_d_id = $5 AND c_id = $6",
            )
            .await
            .map_err(|source| crate::Error::Sql {
                action: "prepare payment: update_customer_with_data".into(),
                source,
            })?;

        let update_customer = client
            .prepare(
                "UPDATE customer SET c_balance = c_balance - $1, c_ytd_payment = c_ytd_payment + $2, \
                 c_payment_cnt = c_payment_cnt + 1 \
                 WHERE c_w_id = $3 AND c_d_id = $4 AND c_id = $5",
            )
            .await
            .map_err(|source| crate::Error::Sql {
                action: "prepare payment: update_customer".into(),
                source,
            })?;

        let insert_history = client
            .prepare(
                "INSERT INTO history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            )
            .await
            .map_err(|source| crate::Error::Sql {
                action: "prepare payment: insert_history".into(),
                source,
            })?;

        Ok(PaymentStmts {
            update_warehouse,
            select_warehouse,
            update_district,
            select_district,
            select_customer_by_last,
            select_customer_for_update,
            select_customer_data,
            update_customer_with_data,
            update_customer,
            insert_history,
        })
    }
}
