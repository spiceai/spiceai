// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

//! Regression bench: per-maintenance-cycle metastore RPC count in
//! [`crate::provider::table::CayenneTableProvider::persist_table_stats`].
//!
//! `persist_table_stats` (`crates/cayenne/src/provider/table.rs:5783-5832`)
//! issues two RPCs against the metastore on every post-write maintenance
//! tick:
//!
//! 1. `get_table_statistics` to read the existing blob,
//! 2. `upsert_table_statistics` to write the merged blob,
//!
//! followed by an in-memory write of the *derived* `Statistics` cache
//! (`provider/table.rs:5834-5837`). The cached `Statistics` is the
//! optimizer-facing view; the raw catalog blob the merge needs is *not*
//! cached, so the read RPC is re-paid every cycle even though the writer
//! that just upserted the new blob is the same single in-process owner of
//! the table.
//!
//! Under sustained CDC ingestion the post-write maintenance loop debounces
//! at 100 ms (`provider/table.rs:112` — `POST_WRITE_MAINTENANCE_DEBOUNCE`),
//! so each table contributes ~10 stats cycles per second, each costing two
//! metastore RPCs against the K-slot connection pool. With N tables the
//! per-process stats-persistence RPC rate is `20·N RPC/s`. After caching
//! the catalog blob alongside the derived statistics it drops to `10·N + 1
//! RPC/s` — a 2× reduction in the metastore-bound term of post-write
//! maintenance.
//!
//! This bench measures the gap directly. Like
//! `metastore_connection_contention.rs` it uses
//! `tokio::time::sleep(rtt)` instead of a real `SqliteMetastore`, so the
//! scheduling pattern is isolated from SQLite-specific cost.
//!
//! Two lanes per `(N_tables, cycles_per_table, RTT)`:
//!
//! - `current_two_rpc` — every maintenance cycle does `get` + `upsert`.
//!   Total wall time ≈ `2·N·cycles·RTT / K` where K is the connection
//!   pool size (modelled here as the multi-thread runtime worker count
//!   for simplicity — the bench amplifies the RPC count, not the
//!   contention).
//! - `cached_one_rpc` — every maintenance cycle does only `upsert`. The
//!   cached catalog blob serves the read locally. One cold `get` happens
//!   at table open and is included in the lane's total.
//!
//! `cargo bench --bench stats_persistence_rpc_ceiling -p cayenne`.
//! Throughput is reported in "cycles" so the gap is a clean 2× at the
//! ceiling.

#![allow(clippy::expect_used)]

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::sync::Mutex;

/// Per-RPC simulated round trip. Two realistic shapes mirror
/// `metastore_connection_contention.rs`:
/// - 1 ms: local SQLite (WAL, NORMAL sync).
/// - 10 ms: same-zone network metastore (Turso, managed Postgres).
const RTTS: &[(&str, Duration)] = &[
    ("rtt_1ms", Duration::from_millis(1)),
    ("rtt_10ms", Duration::from_millis(10)),
];

/// Maintenance cycles per table per iteration. At 100 ms debounce a
/// table emits ~10 cycles/s, so 16 cycles ≈ 1.6 s of sustained ingestion.
/// Kept at 16 so the worst-case shape (`N=32`, `rtt_10ms`,
/// `current_two_rpc` = `2·32·16·10 ms ≈ 10.2 s` per iteration) still fits
/// inside Criterion `--quick`'s shrunken budget without dropping below
/// the 2-sample floor that triggers the `slice.len() > 1` stats panic.
const CYCLES_PER_TABLE: usize = 16;

/// Table counts. 4 is a small pipeline; 14 matches CH-benCH SF100; 32
/// stresses the multi-table case where the metastore-pool is saturated.
const TABLE_COUNTS: &[usize] = &[4, 14, 32];

/// One simulated metastore RPC. Holds a slot in the (shared) connection
/// pool for the round-trip duration. The bench models the pool as a
/// single `Mutex` so RPC count differences translate directly into wall
/// time differences; the per-table contention pattern is exercised by
/// `metastore_connection_contention.rs`.
async fn one_rpc(pool: &Mutex<()>, rtt: Duration) {
    let _guard = pool.lock().await;
    tokio::time::sleep(rtt).await;
}

/// Lane A: every maintenance cycle pays two RPCs.
///
/// Mirrors today's `persist_table_stats`:
///   `catalog.get_table_statistics(...) → catalog.upsert_table_statistics(...)`
/// Per cycle: one read RPC + one write RPC = 2 RPCs.
async fn run_two_rpc_per_cycle(n_tables: usize, cycles: usize, rtt: Duration) {
    let pool = Arc::new(Mutex::new(()));
    let mut handles = Vec::with_capacity(n_tables);
    for _ in 0..n_tables {
        let pool = Arc::clone(&pool);
        handles.push(tokio::spawn(async move {
            for _ in 0..cycles {
                one_rpc(&pool, rtt).await; // get_table_statistics
                one_rpc(&pool, rtt).await; // upsert_table_statistics
            }
        }));
    }
    for h in handles {
        h.await.expect("join");
    }
    black_box(pool);
}

/// Lane B: every maintenance cycle pays one RPC; one cold `get` is paid
/// at table open.
///
/// Models the proposed cache of the raw `TableStatistics` blob alongside
/// the derived `Statistics`. The `persist_table_stats` call reads the
/// blob from the cache (no RPC), merges in the new accumulator, and
/// upserts. Per cycle: one write RPC. Cold start: one read RPC per
/// table.
async fn run_one_rpc_per_cycle(n_tables: usize, cycles: usize, rtt: Duration) {
    let pool = Arc::new(Mutex::new(()));
    let mut handles = Vec::with_capacity(n_tables);
    for _ in 0..n_tables {
        let pool = Arc::clone(&pool);
        handles.push(tokio::spawn(async move {
            // Cold start: one get_table_statistics to seed the cache.
            one_rpc(&pool, rtt).await;
            for _ in 0..cycles {
                one_rpc(&pool, rtt).await; // upsert_table_statistics only
            }
        }));
    }
    for h in handles {
        h.await.expect("join");
    }
    black_box(pool);
}

fn bench_stats_persistence_rpc_ceiling(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("tokio runtime");

    let mut group = c.benchmark_group("stats_persistence_rpc_ceiling");
    // Sample size kept small so the N=32/rtt_10ms case (≈25 s per iteration)
    // still produces a multi-sample distribution under `--quick`.
    group.sample_size(10);
    for &(rtt_label, rtt) in RTTS {
        for &n in TABLE_COUNTS {
            let cycles_total = u64::try_from(n * CYCLES_PER_TABLE).unwrap_or(u64::MAX);
            group.throughput(Throughput::Elements(cycles_total));

            let id = format!("N={n}/{rtt_label}");
            group.bench_with_input(BenchmarkId::new("current_two_rpc", &id), &n, |b, &n| {
                b.to_async(&rt).iter(|| async move {
                    run_two_rpc_per_cycle(n, CYCLES_PER_TABLE, rtt).await;
                });
            });
            group.bench_with_input(BenchmarkId::new("cached_one_rpc", &id), &n, |b, &n| {
                b.to_async(&rt).iter(|| async move {
                    run_one_rpc_per_cycle(n, CYCLES_PER_TABLE, rtt).await;
                });
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_stats_persistence_rpc_ceiling);
criterion_main!(benches);
