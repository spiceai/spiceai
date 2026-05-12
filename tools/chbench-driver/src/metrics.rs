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

//! OLTP metrics: per-transaction latency tracking and tpmC calculation.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::txn::TxnType;

/// Per-transaction-type latency histogram (simple sorted-list approach).
#[derive(Debug, Default)]
pub struct LatencyHistogram {
    samples: Vec<Duration>,
}

impl LatencyHistogram {
    pub fn record(&mut self, d: Duration) {
        self.samples.push(d);
    }

    pub fn count(&self) -> usize {
        self.samples.len()
    }

    /// Merge samples from another histogram into this one.
    pub fn merge(&mut self, other: Self) {
        self.samples.extend(other.samples);
    }

    fn sorted(&self) -> Vec<Duration> {
        let mut s = self.samples.clone();
        s.sort();
        s
    }

    /// Return the percentile value. `p` should be 0.0..=1.0.
    pub fn percentile(&self, p: f64) -> Duration {
        let sorted = self.sorted();
        if sorted.is_empty() {
            return Duration::ZERO;
        }
        let idx = ((sorted.len() as f64) * p).ceil() as usize;
        sorted[idx.min(sorted.len()) - 1]
    }

    pub fn p50(&self) -> Duration {
        self.percentile(0.50)
    }

    pub fn p90(&self) -> Duration {
        self.percentile(0.90)
    }

    pub fn p95(&self) -> Duration {
        self.percentile(0.95)
    }

    pub fn p99(&self) -> Duration {
        self.percentile(0.99)
    }

    pub fn min(&self) -> Duration {
        self.samples.iter().copied().min().unwrap_or(Duration::ZERO)
    }

    pub fn max(&self) -> Duration {
        self.samples.iter().copied().max().unwrap_or(Duration::ZERO)
    }
}

/// Collected OLTP metrics from a benchmark run.
#[derive(Debug)]
pub struct OltpReport {
    /// NewOrder committed transactions per minute.
    pub tpmc: f64,
    /// Per-transaction-type latency histograms.
    pub latencies: HashMap<TxnType, LatencyHistogram>,
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
    latencies: HashMap<TxnType, LatencyHistogram>,
    committed: u64,
    aborted: u64,
}

impl OltpMetrics {
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
            latencies: HashMap::new(),
            committed: 0,
            aborted: 0,
        }
    }

    /// Record a successful transaction.
    pub fn record_success(&mut self, txn_type: TxnType, latency: Duration) {
        self.latencies
            .entry(txn_type)
            .or_default()
            .record(latency);
        self.committed += 1;
    }

    /// Record a failed/aborted transaction.
    pub fn record_abort(&mut self, txn_type: TxnType, latency: Duration) {
        self.latencies
            .entry(txn_type)
            .or_default()
            .record(latency);
        self.aborted += 1;
    }

    /// Merge another terminal's metrics into this one.
    pub fn merge(&mut self, other: Self) {
        for (txn_type, hist) in other.latencies {
            self.latencies
                .entry(txn_type)
                .or_default()
                .merge(hist);
        }
        self.committed += other.committed;
        self.aborted += other.aborted;
    }

    /// Finalize and produce the report.
    pub fn finish(self) -> OltpReport {
        let duration = self.start.elapsed();
        let minutes = duration.as_secs_f64() / 60.0;

        // tpmC = NewOrder committed per minute
        let new_order_committed = self
            .latencies
            .get(&TxnType::NewOrder)
            .map_or(0, |h| h.count());

        let tpmc = if minutes > 0.0 {
            new_order_committed as f64 / minutes
        } else {
            0.0
        };

        let total = self.committed + self.aborted;
        let abort_rate = if total > 0 {
            self.aborted as f64 / total as f64
        } else {
            0.0
        };

        OltpReport {
            tpmc,
            latencies: self.latencies,
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

        let types = [
            TxnType::NewOrder,
            TxnType::Payment,
            TxnType::Delivery,
            TxnType::OrderStatus,
            TxnType::StockLevel,
        ];

        for txn_type in &types {
            if let Some(hist) = self.latencies.get(txn_type) {
                println!(
                    "  {}: {} txns, p50={:.1}ms p90={:.1}ms p95={:.1}ms p99={:.1}ms min={:.1}ms max={:.1}ms",
                    txn_type,
                    hist.count(),
                    hist.p50().as_secs_f64() * 1000.0,
                    hist.p90().as_secs_f64() * 1000.0,
                    hist.p95().as_secs_f64() * 1000.0,
                    hist.p99().as_secs_f64() * 1000.0,
                    hist.min().as_secs_f64() * 1000.0,
                    hist.max().as_secs_f64() * 1000.0,
                );
            }
        }
    }
}
