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

//! Source-side (Postgres) statistics sampler for the HTAP benchmark.
//!
//! Complements the spiced `/metrics` scraper with the *source's own* view —
//! whether the walsenders are idle-waiting-for-WAL vs busy, whether OLTP backends
//! are lock-contended, and the WAL-production / commit rate. All from SQL
//! (`pg_stat_activity` / `pg_stat_wal` / `pg_stat_database`), so it needs no node
//! access — just the connection testoperator already has. Best-effort: connection
//! or query failures are logged and the run is never blocked.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::Serialize;
use test_framework::{
    anyhow::{self, Context},
    tokio_util::sync::CancellationToken,
};

const SAMPLE_INTERVAL: Duration = Duration::from_secs(2);

/// One timestamped snapshot of source-side Postgres stats.
#[derive(Debug, Clone, Serialize, Default)]
pub struct PgStatSample {
    pub ts_ms: i64,
    /// Walsender `wait_event` -> count (`RUNNING` = not waiting). Idle walsenders
    /// show `WalSenderWaitForWAL`; `ClientWrite` = blocked writing to us (we're slow).
    pub walsender_waits: BTreeMap<String, i64>,
    pub walsenders: i64,
    /// Active client-backend `wait_event_type:wait_event` -> count. `Lock:*` /
    /// `LWLock:WALInsert` dominating = OLTP lock contention (overload).
    pub active_backend_waits: BTreeMap<String, i64>,
    /// Cumulative WAL generated (bytes/records) — Δ/Δt = source WAL-production rate.
    pub wal_bytes: i64,
    pub wal_records: i64,
    /// Cumulative committed transactions for the benchmark DB — Δ/Δt = txn/s.
    pub xact_commit: i64,
    /// AUTHORITATIVE per-slot retained WAL in bytes, from the source's own
    /// `pg_replication_slots` (`pg_current_wal_lsn() − confirmed_flush_lsn`). Unlike
    /// the client-view `dataset_postgres_replication_lag_bytes` (server_wal_end −
    /// confirmed_flush), this does NOT stall when the walsender is WriteData-blocked
    /// — the client stops receiving keepalives so its wal_end freezes, but the
    /// source's WAL head keeps advancing. The two diverging is itself a strong
    /// "sender is blocked on us" signal; this is the truth for drain/caught-up.
    pub slot_retained_bytes: BTreeMap<String, i64>,
}

fn now_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// Background sampler of source Postgres stats. Mirrors `MetricsScraper`: spawn a
/// task, stop it to collect the samples.
pub struct PgStatsScraper {
    cancel_token: CancellationToken,
    task: Option<tokio::task::JoinHandle<Vec<PgStatSample>>>,
}

impl PgStatsScraper {
    /// Connect to the source and start sampling. Returns `Ok(None)` (not an error)
    /// when the connection can't be established, so a missing PG stats scraper
    /// never fails the benchmark.
    pub async fn spawn(conn_str: String, db: String) -> anyhow::Result<Option<Self>> {
        // Connect once up front so a bad config surfaces immediately (as a warning).
        let (client, connection) = match tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
        {
            Ok(pair) => pair,
            Err(e) => {
                eprintln!("pg_stats: could not connect to source for stats sampling: {e}");
                return Ok(None);
            }
        };
        // Drive the connection in the background (tokio-postgres requirement).
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("pg_stats: source stats connection ended: {e}");
            }
        });

        let cancel_token = CancellationToken::new();
        let task_token = cancel_token.clone();
        let task = tokio::spawn(async move {
            let mut samples = Vec::new();
            loop {
                tokio::select! {
                    () = task_token.cancelled() => return samples,
                    () = tokio::time::sleep(SAMPLE_INTERVAL) => {
                        if let Some(s) = Self::sample_once(&client, &db).await {
                            samples.push(s);
                        }
                    }
                }
            }
        });

        Ok(Some(Self {
            cancel_token,
            task: Some(task),
        }))
    }

    /// Stop sampling and return the collected snapshots.
    pub async fn stop(mut self) -> Vec<PgStatSample> {
        self.cancel_token.cancel();
        match self.task.take() {
            Some(task) => task.await.unwrap_or_default(),
            None => Vec::new(),
        }
    }

    async fn sample_once(client: &tokio_postgres::Client, db: &str) -> Option<PgStatSample> {
        let mut s = PgStatSample {
            ts_ms: now_unix_ms(),
            ..Default::default()
        };

        // Walsender wait-event distribution.
        if let Ok(rows) = client
            .query(
                "SELECT coalesce(wait_event,'RUNNING') AS we, count(*)::int8 AS n \
                 FROM pg_stat_activity WHERE backend_type='walsender' GROUP BY 1",
                &[],
            )
            .await
        {
            for r in &rows {
                let we: String = r.get("we");
                let n: i64 = r.get("n");
                s.walsenders += n;
                s.walsender_waits.insert(we, n);
            }
        }

        // Active client-backend wait distribution (OLTP lock contention).
        if let Ok(rows) = client
            .query(
                "SELECT coalesce(wait_event_type||':'||wait_event,'RUNNING') AS we, count(*)::int8 AS n \
                 FROM pg_stat_activity WHERE state='active' AND backend_type='client backend' GROUP BY 1",
                &[],
            )
            .await
        {
            for r in &rows {
                let we: String = r.get("we");
                let n: i64 = r.get("n");
                s.active_backend_waits.insert(we, n);
            }
        }

        // WAL production (cumulative).
        if let Ok(rows) = client
            .query("SELECT wal_records::int8 AS recs, wal_bytes::int8 AS bytes FROM pg_stat_wal", &[])
            .await
            && let Some(r) = rows.first()
        {
            s.wal_records = r.get("recs");
            s.wal_bytes = r.get("bytes");
        }

        // Committed transactions for the benchmark DB (cumulative).
        if let Ok(rows) = client
            .query(
                "SELECT xact_commit::int8 AS xc FROM pg_stat_database WHERE datname=$1",
                &[&db],
            )
            .await
            && let Some(r) = rows.first()
        {
            s.xact_commit = r.get("xc");
        }

        // Authoritative per-slot retained WAL from the source's own view (does not
        // stall when the walsender is WriteData-blocked, unlike the client-view
        // lag_bytes). Guard the subtraction with a NULL confirmed_flush (slot created
        // but not yet consumed) and clamp negatives to 0 (a slot momentarily ahead of
        // the read wal head races to 0, not a huge unsigned wrap).
        if let Ok(rows) = client
            .query(
                "SELECT slot_name, \
                 GREATEST((pg_current_wal_lsn() - confirmed_flush_lsn), 0)::int8 AS retained \
                 FROM pg_replication_slots \
                 WHERE slot_type = 'logical' AND confirmed_flush_lsn IS NOT NULL",
                &[],
            )
            .await
        {
            for r in &rows {
                let slot: String = r.get("slot_name");
                let retained: i64 = r.get("retained");
                s.slot_retained_bytes.insert(slot, retained);
            }
        }

        Some(s)
    }
}

/// Build the source connection string + db name from the CH-benCH env config,
/// for the stats scraper.
pub fn source_conn_from_env() -> anyhow::Result<(String, String)> {
    let source = crate::commands::bench::chbench_source_from_env()
        .context("building CH-benCH source config for pg_stats")?;
    Ok((source.connection_string(), source.db.clone()))
}

/// Estimate the source↔local clock skew in ms (local − server), NTP-style: probe
/// `clock_timestamp()` a few times and keep the sample with the smallest round trip,
/// pairing the server time with the local midpoint of that round trip. Lag gauges are
/// `local_now − upstream_commit_ts`, so this offset biases them; the waterfall can
/// subtract it. Best-effort — returns `None` if the source can't be reached.
pub async fn probe_clock_skew_ms() -> Option<i64> {
    let (conn_str, _db) = source_conn_from_env().ok()?;
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .ok()?;
    let conn_task = tokio::spawn(async move {
        let _ = connection.await;
    });

    let mut best_rtt = Duration::MAX;
    let mut best_skew: i64 = 0;
    for _ in 0..3 {
        let t0 = now_unix_ms();
        let Ok(row) = client
            .query_one(
                "SELECT (extract(epoch from clock_timestamp()) * 1000)::int8 AS ms",
                &[],
            )
            .await
        else {
            continue;
        };
        let t1 = now_unix_ms();
        let server_ms: i64 = row.get("ms");
        let rtt = Duration::from_millis(u64::try_from((t1 - t0).max(0)).unwrap_or(0));
        // Local time at the moment the server read its clock ≈ midpoint of the round
        // trip; skew = that local time − server time.
        let local_mid = t0 + (t1 - t0) / 2;
        if rtt < best_rtt {
            best_rtt = rtt;
            best_skew = local_mid - server_ms;
        }
    }
    conn_task.abort();
    // If every probe failed, `best_rtt` is still `Duration::MAX` and `best_skew` is
    // its 0 default — report `None` (the docstring's contract) rather than a bogus
    // 0ms skew that would silently suppress the skew caveat.
    (best_rtt != Duration::MAX).then_some(best_skew)
}
