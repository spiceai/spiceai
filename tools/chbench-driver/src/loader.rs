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

//! Seed data loader for TPC-C + CH-benCH supplemental tables.
//!
//! Generates seed data as CSV files (`crate::csv_gen`, parallelized across
//! warehouses) and bulk-loads them via client-streamed `COPY ... FROM STDIN`
//! — the same mechanism `psql \copy` uses, so no filesystem access to the
//! database server is required. That matters because the SF1000 CI runs
//! target a persistent in-cluster Postgres pod that shares no filesystem
//! with the CI runner generating the CSVs; a server-side `COPY FROM '/path'`
//! would not be reachable there.
//!
//! Measured (SF200, local): the previous per-row `INSERT ... SELECT` clone
//! step got progressively slower as tables grew — cloning cost roughly
//! doubled every 50 additional warehouses within a single run, which is why
//! it scaled so poorly to SF1000. CSV + `COPY` throughput stayed close to
//! linear at the same scale, and the shared generator finishes SF1000 in
//! ~2 minutes (parallelized across warehouses).
//!
//! Future improvement: this streams every seed row over the client
//! connection each run. If the generated CSVs were instead cached on the
//! database server's own disk (or a volume it can read directly), loading
//! could use server-side `COPY FROM '/path'` and skip the network-streaming
//! step entirely — worth revisiting if load time becomes the bottleneck
//! again at larger scale factors.

use std::path::PathBuf;
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use futures::SinkExt;
use tokio::io::AsyncReadExt;
use tokio_postgres::Client;

use crate::Result;
use crate::csv_gen::{self, GeneratedShard};

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

/// Load all seed data for the given number of warehouses.
///
/// Generates CSV seed data (every warehouse gets fully independent random
/// data — see `crate::csv_gen`), then loads it via `COPY ... FROM STDIN` in
/// parallel across `conn_str`-derived connections. The generated CSVs live in
/// a temporary directory that is deleted before this function returns,
/// freeing runner disk space ahead of the benchmark run.
///
/// When `seed` is `Some`, a deterministic RNG is used so that the same seed
/// always produces the same dataset.
///
/// # Errors
///
/// Returns an error if CSV generation fails or any database operation fails.
pub async fn load_all(conn_str: &str, warehouses: usize, seed: Option<u64>) -> Result<()> {
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
        "  loading {} CSV shard(s) via COPY across {concurrency} connection(s)...",
        shards.len()
    );
    let load_start = Instant::now();
    load_shards(conn_str, shards, concurrency).await?;
    println!("  COPY load complete in {:.1?}", load_start.elapsed());

    // `tmp_dir` is dropped here, deleting the generated CSVs from local disk
    // before this function returns and the benchmark run starts.
    Ok(())
}

async fn load_shards(
    conn_str: &str,
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
        let conn_str = conn_str.to_owned();
        handles.push(tokio::spawn(async move {
            let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
                .await
                .map_err(|source| crate::Error::Sql {
                    action: "open COPY loader connection".into(),
                    source,
                })?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("COPY loader connection error: {e}");
                }
            });
            for shard in worker_shards {
                copy_shard(&client, &shard).await?;
            }
            Ok::<(), crate::Error>(())
        }));
    }
    crate::join_loader_tasks(handles, "COPY load").await
}

/// Stream one generated CSV shard into its table via `COPY ... FROM STDIN`.
///
/// `NULL '\N'` matches the marker `csv_gen` writes for logically-NULL fields
/// (e.g. `ol_delivery_d` on undelivered order lines) — Postgres's CSV `COPY`
/// format otherwise treats an empty field as NULL, not the literal `\N`.
async fn copy_shard(client: &Client, shard: &GeneratedShard) -> Result<()> {
    let sql = format!(
        "COPY {} ({}) FROM STDIN WITH (FORMAT csv, QUOTE '\"', NULL '\\N')",
        shard.table, shard.columns
    );
    let sink = client
        .copy_in::<_, Bytes>(&sql)
        .await
        .map_err(|source| crate::Error::Sql {
            action: format!("start COPY for {}", shard.table),
            source,
        })?;
    tokio::pin!(sink);

    let mut file = tokio::fs::File::open(&shard.path)
        .await
        .map_err(|source| crate::Error::Io {
            action: format!("open generated CSV {}", shard.path.display()),
            source,
        })?;
    // `reserve` every iteration (not just once) because `split()` below can
    // leave `buf` with zero spare capacity: BytesMut::split() hands off
    // [0, len) and leaves self with only [len, capacity) remaining, which is
    // empty once a chunk fills the buffer exactly. Without re-reserving,
    // read_buf would then read 0 bytes on the next call — indistinguishable
    // from EOF — silently truncating the upload.
    let mut buf = BytesMut::with_capacity(1 << 20);
    loop {
        buf.reserve(1 << 20);
        let n = file
            .read_buf(&mut buf)
            .await
            .map_err(|source| crate::Error::Io {
                action: format!("read generated CSV {}", shard.path.display()),
                source,
            })?;
        if n == 0 {
            break;
        }
        sink.send(buf.split().freeze())
            .await
            .map_err(|source| crate::Error::Sql {
                action: format!("stream COPY data for {}", shard.table),
                source,
            })?;
    }
    sink.as_mut()
        .finish()
        .await
        .map_err(|source| crate::Error::Sql {
            action: format!("finish COPY for {}", shard.table),
            source,
        })?;
    Ok(())
}
