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

//! Microbenchmarks for the two per-event costs the shared Postgres replication
//! pump ([`data_components::postgres_replication::shared`]) sheds:
//!
//! 1. **Boundary member-metrics fan-out.** Each commit/keepalive used to walk
//!    the member map several times (reader timing, then `confirmed_flush`, then
//!    the commit watermark), each walk re-taking the members lock and cloning a
//!    `Vec<(MemberKey, Arc<..>)>` (as `SharedSource::live_members` does). The
//!    consolidated `flush_member_metrics` does one lock + one iteration.
//! 2. **Recv drain.** The pump used to arm a `tokio::time::timeout` timer per
//!    message; it now drains buffered events with the vendored client's
//!    non-blocking `try_recv` and only arms the timer when the buffer is empty.
//!
//! The pump internals are crate-private, so these mirror the exact operations
//! (member-map lock + `Vec` clone; monotonic-CAS/lock setters on the public
//! `ReplicationMetricsCollector`; bounded-channel drain) rather than calling the
//! private functions directly. `--baseline`-comparing the `old_*` and `new_*`
//! arms gives the per-event reduction the change delivers at 5 members.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use data_components::postgres_replication::ReplicationMetricsCollector;

/// Representative shared-slot fan-out width (the measured regression case).
const MEMBERS: usize = 5;
/// Events buffered ahead of the pump in the drain benchmark.
const DRAIN_BATCH: usize = 256;

type MemberKey = (String, String);

/// Mirror of `SharedSource`'s member map + `live_members()` snapshot so the
/// bench pays the same lock + `Vec` clone the real fan-out does.
struct Members {
    map: Mutex<HashMap<MemberKey, Arc<ReplicationMetricsCollector>>>,
}

impl Members {
    fn with(n: usize) -> Self {
        let mut map = HashMap::new();
        for i in 0..n {
            map.insert(
                ("public".to_string(), format!("t{i}")),
                ReplicationMetricsCollector::new(),
            );
        }
        Self {
            map: Mutex::new(map),
        }
    }

    fn live_members(&self) -> Vec<(MemberKey, Arc<ReplicationMetricsCollector>)> {
        self.map
            .lock()
            .expect("members lock")
            .iter()
            .map(|(k, v)| (k.clone(), Arc::clone(v)))
            .collect()
    }
}

fn bench_boundary_fanout(c: &mut Criterion) {
    let members = Members::with(MEMBERS);
    let now = SystemTime::now();
    let mut group = c.benchmark_group("shared_pump/boundary_fanout_5_members");

    // Pre-consolidation: three separate lock + clone + iterate passes.
    group.bench_function("old_multi_pass", |b| {
        b.iter(|| {
            for (_, m) in members.live_members() {
                m.add_reader_input_wait_micros(10);
                m.add_reader_processing_micros(20);
                m.set_server_wal_end(500);
            }
            for (_, m) in members.live_members() {
                m.set_confirmed_flush_lsn(400);
            }
            for (_, m) in members.live_members() {
                m.record_commit_watermark(now);
            }
        });
    });

    // Consolidated: one lock + clone + iterate applying every field.
    group.bench_function("new_single_pass", |b| {
        b.iter(|| {
            for (_, m) in members.live_members() {
                m.add_reader_input_wait_micros(10);
                m.add_reader_processing_micros(20);
                m.set_server_wal_end(500);
                m.set_confirmed_flush_lsn(400);
                m.record_commit_watermark(now);
            }
        });
    });

    group.finish();
}

fn bench_recv_drain(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("tokio runtime");
    let mut group = c.benchmark_group("shared_pump/recv_drain_256");

    // A bounded channel pre-filled to `DRAIN_BATCH` (as the worker's prefetch
    // buffer would be under load). `try_send` fills synchronously in setup.
    let prefill = || {
        let (tx, rx) = tokio::sync::mpsc::channel::<u64>(DRAIN_BATCH);
        for i in 0..DRAIN_BATCH {
            tx.try_send(i as u64).expect("prefill channel");
        }
        (tx, rx)
    };

    // Pre-consolidation: arm a timeout timer for every message.
    group.bench_function("old_timeout_per_msg", |b| {
        b.to_async(&rt).iter_batched(
            prefill,
            |(_tx, mut rx)| async move {
                let mut drained = 0usize;
                while let Ok(Some(v)) =
                    tokio::time::timeout(Duration::from_secs(1), rx.recv()).await
                {
                    std::hint::black_box(v);
                    drained += 1;
                    if drained == DRAIN_BATCH {
                        break;
                    }
                }
            },
            BatchSize::SmallInput,
        );
    });

    // Non-blocking drain: no timer while events are buffered.
    group.bench_function("new_try_recv_drain", |b| {
        b.to_async(&rt).iter_batched(
            prefill,
            |(_tx, mut rx)| async move {
                while let Ok(v) = rx.try_recv() {
                    std::hint::black_box(v);
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_boundary_fanout, bench_recv_drain);
criterion_main!(benches);
