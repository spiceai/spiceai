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

//! `MySQL` mirror of the Postgres seed data loader for TPC-C + CH-benCH
//! supplemental tables.
//!
//! Generates seed data as CSV files (`crate::csv_gen`, parallelized across
//! warehouses) and bulk-loads them via `LOAD DATA LOCAL INFILE` — the
//! client-streamed variant (`mysql_async`'s built-in `WhiteListFsHandler`),
//! so no filesystem access to the database server is required. That matters
//! because the SF1000 CI runs target a persistent in-cluster `MySQL` pod
//! that shares no filesystem with the CI runner generating the CSVs; a
//! server-side `LOAD DATA INFILE '/path'` would not be reachable there.
//!
//! Measured (SF200, local, with `innodb_doublewrite=0` +
//! `innodb_flush_method=O_DIRECT_NO_FSYNC`): the previous per-row
//! `INSERT ... SELECT` clone step degraded as tables grew, which is why it
//! scaled so poorly to SF1000 even after parallelizing it. CSV +
//! `LOAD DATA LOCAL INFILE` throughput stayed close to linear at the same
//! scale, and the shared generator finishes SF1000 in ~2 minutes
//! (parallelized across warehouses) — see `crate::csv_gen` for why the old
//! seed-10-then-clone-the-rest split is no longer needed at all.
//!
//! Future improvement: this streams every seed row over the client
//! connection each run. If the generated CSVs were instead cached on the
//! database server's own disk (or a volume it can read directly), loading
//! could use server-side `LOAD DATA INFILE '/path'` and skip the
//! network-streaming step entirely (measured ~11% faster locally) — worth
//! revisiting if load time becomes the bottleneck again at larger scale
//! factors.
//!
//! Every loader connection runs with bulk-load session flags
//! (`unique_checks=0`, `foreign_key_checks=0`, and best-effort
//! `sql_log_bin=0`) and is pinned to UTC so `_bench_ts` defaults line up with
//! the CDC replication session.
//!
//! The `_bench_ts` column is never listed or set in any load statement here —
//! seed rows are stamped by the column default created in `schema_mysql`; the
//! per-row triggers are attached *after* the load so they never fire during
//! the seed.

use std::path::PathBuf;
use std::time::Instant;

use mysql_async::prelude::Queryable;
use mysql_async::{OptsBuilder, WhiteListFsHandler};

use crate::Result;
use crate::csv_gen::{self, GeneratedShard};

/// `MySQL` 1227 = `ER_SPECIFIC_ACCESS_DENIED_ERROR` — the session lacks a
/// required privilege (here, `SESSION_VARIABLES_ADMIN` for `SET sql_log_bin`).
fn is_privilege_denied(e: &mysql_async::Error) -> bool {
    matches!(e, mysql_async::Error::Server(se) if se.code == 1227)
}

/// Apply session-level settings that speed up bulk loading on `conn`. Every
/// connection this is applied to is short-lived — opened per load and dropped
/// when the load finishes — so the flags never need restoring.
///
/// `unique_checks=0` and `foreign_key_checks=0` need no special privilege.
/// `sql_log_bin=0` skips writing the multi-GB seed to the binary log — the Spice
/// CDC path snapshots the seeded tables when spiced starts, so the seed rows do
/// not need to be in the binlog — but it requires `SESSION_VARIABLES_ADMIN`.
/// Returns whether `sql_log_bin` was disabled.
async fn apply_bulk_load_session(conn: &mut mysql_async::Conn) -> Result<bool> {
    conn.query_drop("SET unique_checks=0, foreign_key_checks=0")
        .await
        .map_err(|source| crate::Error::MySql {
            action: "set bulk-load session flags".into(),
            source,
        })?;
    // Only swallow the specific "privilege missing" error (1227): losing the
    // binlog skip merely makes the seed slower, never wrong. Any other error
    // (e.g. a broken connection) is propagated so real failures are not masked.
    match conn.query_drop("SET sql_log_bin=0").await {
        Ok(()) => Ok(true),
        Err(e) if is_privilege_denied(&e) => Ok(false),
        Err(source) => Err(crate::Error::MySql {
            action: "disable binlogging for the seed session".into(),
            source,
        }),
    }
}

/// Open a fresh connection from `opts`, pinned to UTC via [`crate::set_mysql_utc`]
/// (the same helper the CDC session uses) and configured with the bulk-load
/// session flags.
async fn open_worker(opts: &mysql_async::Opts) -> Result<mysql_async::Conn> {
    let mut conn = mysql_async::Conn::new(opts.clone())
        .await
        .map_err(|source| crate::Error::MySql {
            action: "open MySQL loader connection".into(),
            source,
        })?;
    crate::set_mysql_utc(&mut conn).await?;
    apply_bulk_load_session(&mut conn).await?;
    Ok(conn)
}

/// `InnoDB` deadlock (1213) and lock-wait-timeout (1205) are transient under
/// concurrent loaders — especially on `history`, which has no primary key, so
/// concurrent inserts contend on its hidden clustered index. A `LOAD DATA`
/// statement is one transaction, so the server rolls the whole statement back
/// on such an error (no partial rows); re-running it therefore produces
/// byte-identical data. Retry these errors instead of failing.
fn is_retriable_lock_error(e: &mysql_async::Error) -> bool {
    matches!(e, mysql_async::Error::Server(se) if se.code == 1213 || se.code == 1205)
}

/// Execute `sql` on `conn`, retrying transient `InnoDB` lock errors (see
/// [`is_retriable_lock_error`]) up to a bounded number of attempts with a small
/// linear backoff. Non-lock errors fail immediately.
async fn exec_with_lock_retry(
    conn: &mut mysql_async::Conn,
    sql: &str,
    action: impl Fn() -> String,
) -> Result<()> {
    const MAX_ATTEMPTS: u32 = 32;
    let mut attempt: u32 = 0;
    loop {
        match conn.query_drop(sql).await {
            Ok(()) => return Ok(()),
            Err(e) if is_retriable_lock_error(&e) && attempt < MAX_ATTEMPTS => {
                attempt += 1;
                tokio::time::sleep(std::time::Duration::from_millis(5 * u64::from(attempt))).await;
            }
            Err(source) => {
                return Err(crate::Error::MySql {
                    action: action(),
                    source,
                });
            }
        }
    }
}

/// Max concurrent connections for the parallel CSV load.
#[expect(
    clippy::disallowed_methods,
    reason = "a host-local data-generation tool, not spiced: it should use the whole machine it runs on, not spiced's CPU entitlement"
)]
fn loader_concurrency() -> usize {
    std::thread::available_parallelism()
        .map_or(4, std::num::NonZeroUsize::get)
        .min(8)
}

/// Load all seed data for the given number of warehouses, opening every
/// connection from `opts` (the caller's connection is never touched).
///
/// Generates CSV seed data (every warehouse gets fully independent random
/// data — see `crate::csv_gen`), then loads it via `LOAD DATA LOCAL INFILE`
/// in parallel across worker connections. The generated CSVs live in a
/// temporary directory that is deleted before this function returns, freeing
/// runner disk space ahead of the benchmark run.
///
/// When `seed` is `Some`, a deterministic RNG is used so that the same seed
/// always produces the same dataset — independent of loader parallelism.
///
/// # Errors
///
/// Returns an error if CSV generation fails or any database operation fails.
pub async fn load_all(
    opts: &mysql_async::Opts,
    warehouses: usize,
    seed: Option<u64>,
) -> Result<()> {
    let tmp_dir = tempfile::tempdir().map_err(|source| crate::Error::Io {
        action: "create seed CSV temp directory".into(),
        source,
    })?;
    let dir: PathBuf = tmp_dir.path().to_path_buf();

    println!("  generating seed CSV data for {warehouses} warehouse(s)...");
    let gen_start = Instant::now();
    let gen_dir = dir.clone();
    let shards = tokio::task::spawn_blocking(move || csv_gen::generate(&gen_dir, warehouses, seed))
        .await
        .map_err(|e| crate::Error::TaskJoin {
            message: if e.is_panic() {
                format!("csv generation task panicked: {e}")
            } else {
                format!("csv generation task was cancelled: {e}")
            },
        })??;
    println!(
        "  generated {} CSV file(s) in {:.1?}",
        shards.len(),
        gen_start.elapsed()
    );

    let concurrency = loader_concurrency().min(shards.len().max(1));
    println!(
        "  loading {} CSV shard(s) via LOAD DATA LOCAL INFILE across {concurrency} connection(s)...",
        shards.len()
    );
    let load_start = Instant::now();
    load_shards(opts, shards, concurrency).await?;
    println!("  LOAD DATA load complete in {:.1?}", load_start.elapsed());

    // `tmp_dir` is dropped here, deleting the generated CSVs from local disk
    // before this function returns and the benchmark run starts.
    Ok(())
}

async fn load_shards(
    opts: &mysql_async::Opts,
    shards: Vec<GeneratedShard>,
    concurrency: usize,
) -> Result<()> {
    // Round-robin shards across `concurrency` worker connections.
    let mut workers: Vec<Vec<GeneratedShard>> = (0..concurrency).map(|_| Vec::new()).collect();
    for (i, shard) in shards.into_iter().enumerate() {
        workers[i % concurrency].push(shard);
    }

    let mut handles = Vec::with_capacity(concurrency);
    for worker_shards in workers {
        if worker_shards.is_empty() {
            continue;
        }
        let base_opts = opts.clone();
        handles.push(tokio::spawn(async move {
            // `LOAD DATA LOCAL INFILE` requires the client to whitelist which
            // local paths it will serve — scoped to just this worker's own
            // shard files, not the whole seed directory.
            let whitelist: Vec<PathBuf> = worker_shards.iter().map(|s| s.path.clone()).collect();
            let worker_opts: mysql_async::Opts = OptsBuilder::from_opts(base_opts)
                .local_infile_handler(Some(WhiteListFsHandler::new(whitelist)))
                .into();
            let mut conn = open_worker(&worker_opts).await?;
            for shard in worker_shards {
                load_shard(&mut conn, &shard).await?;
            }
            Ok::<(), crate::Error>(())
        }));
    }
    crate::join_loader_tasks(handles, "LOAD DATA load").await
}

/// Load one generated CSV shard into its table via `LOAD DATA LOCAL INFILE`.
/// `\N` (the default `MySQL` NULL marker) matches what `csv_gen` writes for
/// logically-NULL fields (e.g. `ol_delivery_d` on undelivered order lines),
/// so no explicit NULL-marker clause is needed here (unlike Postgres's COPY).
async fn load_shard(conn: &mut mysql_async::Conn, shard: &GeneratedShard) -> Result<()> {
    let path = shard.path.to_str().ok_or_else(|| crate::Error::Io {
        action: format!("convert generated CSV path to UTF-8 for {}", shard.table),
        source: std::io::Error::new(std::io::ErrorKind::InvalidData, "non-UTF-8 path"),
    })?;
    let path = path.replace('\'', "''");
    let sql = format!(
        "LOAD DATA LOCAL INFILE '{path}' INTO TABLE {} \
         FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' \
         LINES TERMINATED BY '\\n' ({})",
        shard.table, shard.columns
    );
    exec_with_lock_retry(conn, &sql, || format!("LOAD DATA for {}", shard.table)).await
}
