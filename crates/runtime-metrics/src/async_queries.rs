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

//! Async query (`/v1/queries`) budget metrics.

use super::{Counter, LazyLock, Meter, UpDownCounter, global};
use opentelemetry::KeyValue;

static ASYNC_QUERIES_METER: LazyLock<Meter> = LazyLock::new(|| global::meter("async_queries"));

/// Why a submission was refused: a full queue, or a full stored-results budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectionReason {
    QueueFull,
    ResultBudget,
}

impl RejectionReason {
    pub const ALL: [Self; 2] = [Self::QueueFull, Self::ResultBudget];

    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::QueueFull => "queue_full",
            Self::ResultBudget => "result_budget",
        }
    }
}

/// Whether an async query is waiting for a running slot or running.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActiveStatus {
    Pending,
    Running,
}

impl ActiveStatus {
    pub const ALL: [Self; 2] = [Self::Pending, Self::Running];

    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
        }
    }
}

static SUBMISSIONS_REJECTED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    let counter = ASYNC_QUERIES_METER
        .u64_counter("async_query_submissions_rejected")
        .with_description(
            "Async query submissions refused because the caller's queue or stored-results budget was full.",
        )
        .with_unit("submissions")
        .build();
    for reason in RejectionReason::ALL {
        counter.add(0, &[KeyValue::new("reason", reason.label())]);
    }
    counter
});

static ACTIVE_COUNT: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
    let gauge = ASYNC_QUERIES_METER
        .i64_up_down_counter("async_query_active_count")
        .with_description("Async queries this scheduler holds pending or running.")
        .with_unit("queries")
        .build();
    for status in ActiveStatus::ALL {
        gauge.add(0, &[KeyValue::new("status", status.label())]);
    }
    gauge
});

/// Publishes both series at `0`, so a dashboard shows them before the first submission.
pub fn init() {
    LazyLock::force(&SUBMISSIONS_REJECTED);
    LazyLock::force(&ACTIVE_COUNT);
}

pub fn track_submission_rejected(reason: RejectionReason) {
    SUBMISSIONS_REJECTED.add(1, &[KeyValue::new("reason", reason.label())]);
}

pub fn add_active(status: ActiveStatus, delta: i64) {
    ACTIVE_COUNT.add(delta, &[KeyValue::new("status", status.label())]);
}
