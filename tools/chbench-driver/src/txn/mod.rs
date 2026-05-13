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

//! TPC-C transaction implementations for the CH-benCH OLTP driver.
//!
//! Five transaction types with standard 45/43/4/4/4 mix:
//! - **NewOrder** (45%): Insert an order with 5-15 line items, update stock and district.
//! - **Payment** (43%): Update warehouse/district/customer balances, insert history.
//! - **Delivery** (4%): Deliver oldest pending order for each of 10 districts.
//! - **OrderStatus** (4%): Read-only — look up latest order for a customer.
//! - **StockLevel** (4%): Read-only — count low-stock items in recent orders.

pub mod delivery;
pub mod new_order;
pub mod order_status;
pub mod payment;
pub mod stock_level;

use std::fmt;

use ::rand::{Rng, RngExt};
use tokio_postgres::Client;

use crate::Result;

/// The five TPC-C transaction types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TxnType {
    NewOrder,
    Payment,
    Delivery,
    OrderStatus,
    StockLevel,
}

impl fmt::Display for TxnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NewOrder => write!(f, "new_order"),
            Self::Payment => write!(f, "payment"),
            Self::Delivery => write!(f, "delivery"),
            Self::OrderStatus => write!(f, "order_status"),
            Self::StockLevel => write!(f, "stock_level"),
        }
    }
}

/// Standard TPC-C transaction mix weights (must sum to 100).
/// Index order: NewOrder, Payment, Delivery, OrderStatus, StockLevel.
pub const DEFAULT_MIX: [u32; 5] = [45, 43, 4, 4, 4];

/// All transaction types in mix-weight index order.
const TXN_TYPES: [TxnType; 5] = [
    TxnType::NewOrder,
    TxnType::Payment,
    TxnType::Delivery,
    TxnType::OrderStatus,
    TxnType::StockLevel,
];

/// Select a random transaction type according to the given mix weights.
pub fn pick_txn_type(rng: &mut impl Rng, mix: &[u32; 5]) -> TxnType {
    let total: u32 = mix.iter().sum();
    let roll = rng.random_range(0..total);
    let mut cumulative = 0;
    for (i, &weight) in mix.iter().enumerate() {
        cumulative += weight;
        if roll < cumulative {
            return TXN_TYPES[i];
        }
    }
    TxnType::NewOrder
}

/// Execute one TPC-C transaction of the given type.
pub async fn execute(
    client: &mut Client,
    rng: &mut impl Rng,
    txn_type: TxnType,
    warehouses: i32,
) -> Result<()> {
    match txn_type {
        TxnType::NewOrder => new_order::run(client, rng, warehouses).await,
        TxnType::Payment => payment::run(client, rng, warehouses).await,
        TxnType::Delivery => delivery::run(client, rng, warehouses).await,
        TxnType::OrderStatus => order_status::run(client, rng, warehouses).await,
        TxnType::StockLevel => stock_level::run(client, rng, warehouses).await,
    }
}
