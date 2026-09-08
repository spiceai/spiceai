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

//! OLTP metrics: tpmC and abort rate tracking.
//!
//! Per-transaction-type latency histograms are intentionally omitted — they measure
//! Postgres performance, not Spice. The metrics that matter for Spice benchmarking are
//! tpmC (CDC input intensity), the analytical query latencies, and staleness gap reported by testoperator.

use std::time::{Duration, Instant};

use crate::txn::TxnType;

/// Collected OLTP metrics from a benchmark run.
#[derive(Debug)]
pub struct OltpReport {
    /// `NewOrder` committed transactions per minute.
    pub tpmc: f64,
    /// Total committed transactions.
    pub total_committed: u64,
    /// Total aborted/failed transactions.
    pub total_aborted: u64,
    /// Abort rate (0.0..1.0).
    pub abort_rate: f64,
    /// Wall-clock duration of the OLTP run.
    pub duration: Duration,
}

/// Accumulates metrics during an OLTP run.
pub struct OltpMetrics {
    start: Instant,
    new_order_committed: u64,
    committed: u64,
    aborted: u64,
}

impl Default for OltpMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl OltpMetrics {
    #[must_use]
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
            new_order_committed: 0,
            committed: 0,
            aborted: 0,
        }
    }

    /// Record a successful transaction.
    pub fn record_success(&mut self, txn_type: TxnType) {
        if txn_type == TxnType::NewOrder {
            self.new_order_committed += 1;
        }
        self.committed += 1;
    }

    /// Record a failed/aborted transaction.
    pub fn record_abort(&mut self) {
        self.aborted += 1;
    }

    /// Merge another terminal's metrics into this one.
    pub fn merge(&mut self, other: &Self) {
        self.new_order_committed += other.new_order_committed;
        self.committed += other.committed;
        self.aborted += other.aborted;
    }

    /// Finalize and produce the report.
    #[must_use]
    pub fn finish(self) -> OltpReport {
        let duration = self.start.elapsed();
        let minutes = duration.as_secs_f64() / 60.0;

        let tpmc = if minutes > 0.0 {
            #[expect(clippy::cast_precision_loss)]
            {
                self.new_order_committed as f64 / minutes
            }
        } else {
            0.0
        };

        let total = self.committed + self.aborted;
        let abort_rate = if total > 0 {
            #[expect(clippy::cast_precision_loss)]
            {
                self.aborted as f64 / total as f64
            }
        } else {
            0.0
        };

        OltpReport {
            tpmc,
            total_committed: self.committed,
            total_aborted: self.aborted,
            abort_rate,
            duration,
        }
    }
}

impl OltpReport {
    /// Print a human-readable summary.
    pub fn print_summary(&self) {
        println!("OLTP Report ({:.1}s)", self.duration.as_secs_f64());
        println!("  tpmC (NewOrder/min): {:.1}", self.tpmc);
        println!(
            "  transactions: {} committed, {} aborted ({:.2}% abort rate)",
            self.total_committed,
            self.total_aborted,
            self.abort_rate * 100.0,
        );
    }
}
