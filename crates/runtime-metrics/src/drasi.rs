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

//! Delivery metrics for Drasi change forwarding.
//!
//! Recorded on the same global meter as the other runtime metrics. Labels are
//! the component name, the transport (`http` or `redis`), and the outcome.
//! Gauges carry only the component. Nothing here is an endpoint, a payload, or
//! a row value.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, ObservableGauge};

use super::{LazyLock, Meter, global};
use telemetry::DURATION_MS_HISTOGRAM_BUCKETS;

pub const BATCHES: &str = "drasi_forward_batches";
pub const LATENCY_MS: &str = "drasi_forward_latency_ms";
pub const QUEUE_DEPTH: &str = "drasi_queue_depth";
pub const DEAD_LETTER_BATCHES: &str = "drasi_dead_letter_batches";

pub const OUTCOME_DELIVERED: &str = "delivered";
pub const OUTCOME_RETRIED: &str = "retried";
pub const OUTCOME_DEAD_LETTERED: &str = "dead_lettered";
pub const OUTCOME_DISCARDED: &str = "discarded";

pub const LABEL_COMPONENT: &str = "component";
pub const LABEL_TRANSPORT: &str = "transport";
pub const LABEL_OUTCOME: &str = "outcome";

const METER_NAME: &str = "drasi";

static GLOBAL: LazyLock<DrasiForwardMetrics> =
    LazyLock::new(|| DrasiForwardMetrics::from_meter(&global::meter(METER_NAME)));

type Slots = Arc<Mutex<HashMap<String, Arc<AtomicU64>>>>;

/// Instruments for one Drasi forwarder.
///
/// Gauges are observable: a scrape reads the current queue and dead-letter
/// occupancy, so a concurrent update cannot publish a stale value over a newer
/// one. The caller holds an [`Arc<AtomicU64>`][AtomicU64] per component and
/// stores the current count into it.
#[derive(Clone, Debug)]
pub struct DrasiForwardMetrics {
    batches: Counter<u64>,
    latency_ms: Histogram<f64>,
    queue_depths: Slots,
    dead_letters: Slots,
    _queue_depth: ObservableGauge<u64>,
    _dead_letter_batches: ObservableGauge<u64>,
}

impl DrasiForwardMetrics {
    /// The process-wide instruments, on the same meter as the other runtime metrics.
    #[must_use]
    pub fn global() -> Self {
        GLOBAL.clone()
    }

    /// Builds the instruments on `meter`.
    ///
    /// Production uses [`Self::global`]. Tests pass a meter whose reader they collect.
    #[must_use]
    pub fn from_meter(meter: &Meter) -> Self {
        let queue_depths: Slots = Arc::new(Mutex::new(HashMap::new()));
        let dead_letters: Slots = Arc::new(Mutex::new(HashMap::new()));

        let queue_depths_cb = Arc::clone(&queue_depths);
        let queue_depth = meter
            .u64_observable_gauge(QUEUE_DEPTH)
            .with_description(
                "In-memory Drasi change batches still waiting to be delivered, one series per component.",
            )
            .with_callback(move |observer| {
                for_each_slot(&queue_depths_cb, |component, value| {
                    let labels = [KeyValue::new(LABEL_COMPONENT, component.to_string())];
                    observer.observe(value, &labels);
                });
            })
            .build();

        let dead_letters_cb = Arc::clone(&dead_letters);
        let dead_letter_batches = meter
            .u64_observable_gauge(DEAD_LETTER_BATCHES)
            .with_description(
                "Drasi change batches currently retained in the dead-letter store, one series per component.",
            )
            .with_callback(move |observer| {
                for_each_slot(&dead_letters_cb, |component, value| {
                    let labels = [KeyValue::new(LABEL_COMPONENT, component.to_string())];
                    observer.observe(value, &labels);
                });
            })
            .build();

        Self {
            batches: meter
                .u64_counter(BATCHES)
                .with_description(
                    "Drasi change batches by delivery outcome (delivered, retried, dead_lettered, or discarded).",
                )
                .build(),
            latency_ms: meter
                .f64_histogram(LATENCY_MS)
                .with_description(
                    "Milliseconds from a Drasi change (source commit time, or queue time when it has none) to the delivery acknowledgement.",
                )
                .with_unit("ms")
                .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
                .build(),
            queue_depths,
            dead_letters,
            _queue_depth: queue_depth,
            _dead_letter_batches: dead_letter_batches,
        }
    }

    /// The counter slot a component's in-memory queue updates.
    ///
    /// The first call publishes the series at zero.
    #[must_use]
    pub fn queue_depth_slot(&self, component: &str) -> Arc<AtomicU64> {
        slot(&self.queue_depths, component)
    }

    /// The counter slot a component's dead-letter store updates.
    ///
    /// The first call publishes the series at zero.
    #[must_use]
    pub fn dead_letter_slot(&self, component: &str) -> Arc<AtomicU64> {
        slot(&self.dead_letters, component)
    }

    /// Publishes every outcome counter for `(component, transport)` at zero.
    ///
    /// Call when a forwarder is registered so scrapes can tell "none yet" from
    /// a missing series (see [`crate::publish_component_counters_at_zero`]).
    pub fn seed_batch_outcomes(&self, component: &str, transport: &str) {
        for outcome in [
            OUTCOME_DELIVERED,
            OUTCOME_RETRIED,
            OUTCOME_DEAD_LETTERED,
            OUTCOME_DISCARDED,
        ] {
            self.batches.add(
                0,
                &[
                    KeyValue::new(LABEL_COMPONENT, component.to_string()),
                    KeyValue::new(LABEL_TRANSPORT, transport.to_string()),
                    KeyValue::new(LABEL_OUTCOME, outcome.to_string()),
                ],
            );
        }
    }

    /// Counts one delivery attempt.
    ///
    /// `outcome` is `delivered`, `retried`, `dead_lettered`, or `discarded`.
    /// `transport` is `http` or `redis`.
    pub fn record_batch(&self, component: &str, transport: &str, outcome: &str) {
        self.batches.add(
            1,
            &[
                KeyValue::new(LABEL_COMPONENT, component.to_string()),
                KeyValue::new(LABEL_TRANSPORT, transport.to_string()),
                KeyValue::new(LABEL_OUTCOME, outcome.to_string()),
            ],
        );
    }

    /// Records acknowledgement latency. Called only when Drasi accepts the batch.
    pub fn record_latency_ms(&self, component: &str, transport: &str, latency_ms: f64) {
        self.latency_ms.record(
            latency_ms,
            &[
                KeyValue::new(LABEL_COMPONENT, component.to_string()),
                KeyValue::new(LABEL_TRANSPORT, transport.to_string()),
            ],
        );
    }
}

fn slot(slots: &Slots, component: &str) -> Arc<AtomicU64> {
    let mut guard = slots
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Arc::clone(
        guard
            .entry(component.to_string())
            .or_insert_with(|| Arc::new(AtomicU64::new(0))),
    )
}

fn for_each_slot(slots: &Slots, mut observe: impl FnMut(&str, u64)) {
    let guard = slots
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for (component, value) in guard.iter() {
        observe(component, value.load(Ordering::Relaxed));
    }
}
