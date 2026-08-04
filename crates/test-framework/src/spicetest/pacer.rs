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

//! A global issue-rate limiter shared by every query worker in a test.
//!
//! Without one, a load test is CLOSED-LOOP: each worker sends a query, waits for
//! the response, and immediately sends the next. The offered rate is then a
//! *result* of the server's latency rather than an input, which makes two builds
//! incomparable in the way that matters — a slower build simply issues fewer
//! queries and can post the same per-query latency, so the regression hides in
//! the throughput column instead of showing up as latency.
//!
//! Pinning the rate makes both builds do identical work, and the question
//! becomes "what latency did each pay to sustain it".

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;

/// Paces query issuance across all workers to an aggregate target rate.
#[derive(Debug)]
pub struct QueryPacer {
    /// Gap between successive query issues, i.e. `1 / target_qps`.
    interval: Duration,
    /// The instant the next query may be issued. Shared across workers, so the
    /// rate is a fleet-wide property rather than per-worker.
    next_slot: Mutex<Instant>,
}

impl QueryPacer {
    /// Build a pacer for `target_qps` queries per second across the whole test.
    ///
    /// Returns `None` for a non-positive rate, which is how callers spell "run
    /// closed-loop" — the historical behaviour.
    #[must_use]
    pub fn new(target_qps: f64) -> Option<Arc<Self>> {
        if !target_qps.is_finite() || target_qps <= 0.0 {
            return None;
        }
        Some(Arc::new(Self {
            interval: Duration::from_secs_f64(1.0 / target_qps),
            next_slot: Mutex::new(Instant::now()),
        }))
    }

    /// Wait until this caller's slot in the schedule comes up.
    ///
    /// Slots are handed out under a short-held lock and the sleep happens
    /// outside it, so workers queue for a slot rather than for each other's
    /// sleeps.
    ///
    /// A run that falls behind does NOT bank credit: if the next slot is already
    /// in the past, the schedule resumes from now instead of firing a burst to
    /// catch up. Bursting would be worse than useless here — it would convert a
    /// stall into a thundering herd and record a latency spike caused by the
    /// harness rather than the server.
    pub async fn acquire(&self) {
        let slot = {
            let mut next = self.next_slot.lock().await;
            let now = Instant::now();
            let slot = if *next < now { now } else { *next };
            *next = slot + self.interval;
            slot
        };
        tokio::time::sleep_until(slot).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_non_positive_rate_means_closed_loop() {
        assert!(QueryPacer::new(0.0).is_none());
        assert!(QueryPacer::new(-5.0).is_none());
        assert!(QueryPacer::new(f64::NAN).is_none());
        assert!(QueryPacer::new(1000.0).is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn slots_are_spaced_by_the_target_interval() {
        let pacer = QueryPacer::new(1000.0).expect("pacer");
        let start = Instant::now();
        for _ in 0..10 {
            pacer.acquire().await;
        }
        // 10 slots at 1000/s: the first is immediate, so the tenth lands at 9ms.
        assert_eq!(start.elapsed(), Duration::from_millis(9));
    }

    /// The property that keeps a stall from being recorded as a latency spike.
    #[tokio::test(start_paused = true)]
    async fn falling_behind_does_not_bank_a_burst() {
        let pacer = QueryPacer::new(1000.0).expect("pacer");
        pacer.acquire().await;

        // Simulate the fleet stalling well past several slots.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // The next two acquires resume the schedule from now — they must not
        // both return instantly to "catch up" on the ~50 missed slots.
        let resumed = Instant::now();
        pacer.acquire().await;
        pacer.acquire().await;
        assert_eq!(resumed.elapsed(), Duration::from_millis(1));
    }
}
