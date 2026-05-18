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
//! - **`NewOrder`** (45%): Insert an order with 5-15 line items, update stock and district.
//! - **Payment** (43%): Update warehouse/district/customer balances, insert history.
//! - **Delivery** (4%): Deliver oldest pending order for each of 10 districts.
//! - **`OrderStatus`** (4%): Read-only — look up latest order for a customer.
//! - **`StockLevel`** (4%): Read-only — count low-stock items in recent orders.

pub mod delivery;
pub mod new_order;
pub mod order_status;
pub mod payment;
pub mod prepared;
pub mod stock_level;

use std::fmt;

use ::rand::{Rng, RngExt};
use tokio_postgres::Client;

use crate::Result;
pub use prepared::PreparedStatements;

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
/// Index order: `NewOrder`, Payment, Delivery, `OrderStatus`, `StockLevel`.
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

/// Per-terminal warehouse/district assignment to reduce contention.
///
/// Each terminal "owns" a home warehouse and a slice of districts within it.
/// Transactions use the home warehouse and pick districts only from the assigned
/// range, eliminating `d_next_o_id` lock collisions between terminals.
///
/// Follows TPC-C spec clause 4.2.2 (each terminal is "home" to one warehouse)
/// and BenchBase's `TPCCBenchmark.createTerminals()` district-partitioning strategy.
#[derive(Debug, Clone, Copy)]
pub struct TerminalAssignment {
    /// Home warehouse ID (1-based).
    pub home_w_id: i32,
    /// Lower district ID (inclusive, 1-based).
    pub district_lo: i32,
    /// Upper district ID (inclusive, 1-based).
    pub district_hi: i32,
    /// Total number of warehouses (for remote warehouse selection).
    pub num_warehouses: i32,
}

impl TerminalAssignment {
    /// Compute assignments for all terminals, distributing evenly across
    /// warehouses and districts (like BenchBase's `createTerminals`).
    pub fn compute(num_terminals: usize, num_warehouses: i32) -> Vec<Self> {
        let nw = num_warehouses.max(1) as usize;
        let terminals_per_wh = num_terminals as f64 / nw as f64;
        let mut assignments = Vec::with_capacity(num_terminals);

        for w in 0..nw {
            let w_id = (w as i32) + 1;
            let lower = (w as f64 * terminals_per_wh) as usize;
            let upper = if w + 1 == nw {
                num_terminals
            } else {
                ((w + 1) as f64 * terminals_per_wh) as usize
            };
            let wh_terminals = upper - lower;
            if wh_terminals == 0 {
                continue;
            }

            let districts_per_terminal = 10.0 / wh_terminals as f64;
            for t in 0..wh_terminals {
                let d_lo = (t as f64 * districts_per_terminal) as i32 + 1;
                let d_hi = if t + 1 == wh_terminals {
                    10
                } else {
                    ((t + 1) as f64 * districts_per_terminal) as i32
                };
                assignments.push(Self {
                    home_w_id: w_id,
                    district_lo: d_lo,
                    district_hi: d_hi.max(d_lo),
                    num_warehouses: num_warehouses.max(1),
                });
            }
        }

        assignments
    }
}

/// Execute one TPC-C transaction of the given type.
///
/// # Errors
///
/// Returns an error if the transaction fails.
pub async fn execute(
    client: &mut Client,
    rng: &mut impl Rng,
    txn_type: TxnType,
    assignment: &TerminalAssignment,
    stmts: &PreparedStatements,
) -> Result<()> {
    match txn_type {
        TxnType::NewOrder => new_order::run(client, rng, assignment, &stmts.new_order).await,
        TxnType::Payment => payment::run(client, rng, assignment, &stmts.payment).await,
        TxnType::Delivery => delivery::run(client, rng, assignment.num_warehouses).await,
        TxnType::OrderStatus => order_status::run(client, rng, assignment.num_warehouses).await,
        TxnType::StockLevel => stock_level::run(client, rng, assignment.num_warehouses).await,
    }
}
