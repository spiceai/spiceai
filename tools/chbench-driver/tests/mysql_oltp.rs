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

//! `MySQL` OLTP throughput and `_bench_ts` watermark tests.
//!
//! These need a live binlog-enabled `MySQL` source, so they are gated on
//! `CHBENCH_MYSQL_E2E=1` and skip otherwise — `cargo test` on a machine without
//! a container stays green. Start a matching server with the same flags CI uses
//! (`.github/actions/setup-chbench-mysql/action.yml`), then:
//!
//! ```shell
//! CHBENCH_MYSQL_E2E=1 cargo test -p chbench-driver --test mysql_oltp -- --nocapture
//! ```
//!
//! Connection settings come from `MysqlSourceConfig::default()`
//! (127.0.0.1:3306, db `chbench`, user/pass `bench`), overridable via
//! `CHBENCH_MYSQL_{HOST,PORT,DB,USER,PASS}`.
//!
//! The tests share one database and serialize on [`SOURCE`], so they can be run
//! with the default `cargo test` concurrency.
//!
//! `throughput` is a measurement, not an assertion: it reports tpmC for the
//! configured terminal count and rate so a change to the transaction round-trip
//! count can be compared before and after. It deliberately has no threshold —
//! absolute numbers depend on the host, and a CI-hostile assertion would be
//! either flaky or meaningless.

use std::sync::LazyLock;
use std::time::{Duration, Instant};

use chbench_driver::{ChBenchConfig, ChBenchDriver, MysqlChBenchDriver, MysqlSourceConfig};
use mysql_async::prelude::Queryable;
use tokio_util::sync::CancellationToken;

/// Serializes the tests in this file.
///
/// They share one `MySQL` database and most call `prepare()`, which drops and
/// recreates every table — so running two at once has each pulling the schema out
/// from under the other. `cargo test` runs tests in a binary concurrently by
/// default, so the exclusion has to be enforced here rather than left to a
/// `--test-threads=1` convention a caller can forget.
static SOURCE: LazyLock<tokio::sync::Mutex<()>> = LazyLock::new(|| tokio::sync::Mutex::new(()));

/// Take exclusive use of the shared source for the rest of the test.
async fn exclusive_source() -> tokio::sync::MutexGuard<'static, ()> {
    SOURCE.lock().await
}

/// Whether the caller asked for the live-`MySQL` tests.
fn enabled() -> bool {
    std::env::var("CHBENCH_MYSQL_E2E").is_ok_and(|v| v == "1")
}

/// Source config from the environment, defaulting to the CI container's setup.
fn source() -> MysqlSourceConfig {
    let mut cfg = MysqlSourceConfig::default();
    if let Ok(host) = std::env::var("CHBENCH_MYSQL_HOST") {
        cfg.host = host;
    }
    if let Ok(port) = std::env::var("CHBENCH_MYSQL_PORT") {
        cfg.port = port.parse().expect("CHBENCH_MYSQL_PORT must be a u16");
    }
    if let Ok(db) = std::env::var("CHBENCH_MYSQL_DB") {
        cfg.db = db;
    }
    if let Ok(user) = std::env::var("CHBENCH_MYSQL_USER") {
        cfg.user = user;
    }
    if let Ok(pass) = std::env::var("CHBENCH_MYSQL_PASS") {
        cfg.pass = pass;
    }
    cfg
}

/// A `usize` knob from the environment.
fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Open a raw connection for the assertions that inspect server state directly.
async fn raw_conn(cfg: &MysqlSourceConfig) -> mysql_async::Conn {
    let mut conn = mysql_async::Conn::new(cfg.opts())
        .await
        .expect("connect to the MySQL source");
    conn.query_drop("SET time_zone = '+00:00'")
        .await
        .expect("pin session to UTC");
    conn
}

/// Prepare a scale-factor-1 dataset and run the OLTP workload, reporting tpmC.
///
/// The knobs mirror the HTAP dispatch config so a local run can reproduce what
/// CI asks for: `CHBENCH_TERMINALS` (default 10, CI uses `scale_factor * 10`),
/// `CHBENCH_RATE` (txn/s; unset = unlimited, CI uses 9250 at SF1), and
/// `CHBENCH_BENCH_SECS` (default 30).
#[tokio::test(flavor = "multi_thread")]
async fn throughput() {
    if !enabled() {
        eprintln!("skipping: set CHBENCH_MYSQL_E2E=1 to run against a live MySQL source");
        return;
    }
    let _exclusive = exclusive_source().await;

    let warehouses = env_usize("CHBENCH_WAREHOUSES", 1);
    let terminals = env_usize("CHBENCH_TERMINALS", 10);
    let secs = env_usize("CHBENCH_BENCH_SECS", 30);
    let rate: Option<u32> = std::env::var("CHBENCH_RATE")
        .ok()
        .and_then(|v| v.parse().ok());

    // `CHBENCH_MIX` forces the transaction mix (five comma-separated weights
    // summing to 100) so one transaction type can be measured on its own.
    let mix = std::env::var("CHBENCH_MIX").ok().map(|v| {
        let parts: Vec<u32> = v
            .split(',')
            .map(|p| {
                p.trim()
                    .parse()
                    .expect("CHBENCH_MIX weights must be integers")
            })
            .collect();
        let arr: [u32; 5] = parts
            .try_into()
            .expect("CHBENCH_MIX needs exactly 5 weights");
        arr
    });

    let cfg = ChBenchConfig {
        warehouses,
        terminals,
        rate,
        mix: mix.unwrap_or(chbench_driver::txn::DEFAULT_MIX),
        ..Default::default()
    };
    let src = source();

    let driver = MysqlChBenchDriver::connect(cfg, src)
        .await
        .expect("connect the CH-benCH driver");

    // `CHBENCH_SKIP_PREPARE=1` reuses whatever is already loaded, so a server-side
    // change (an index, a trigger) can be measured without the reseed undoing it.
    if std::env::var("CHBENCH_SKIP_PREPARE").is_ok_and(|v| v == "1") {
        driver
            .verify_prepared()
            .await
            .expect("verify the existing dataset");
        println!("prepare: skipped (CHBENCH_SKIP_PREPARE=1)");
    } else {
        let prepare_started = Instant::now();
        driver.prepare().await.expect("prepare the SF dataset");
        println!(
            "prepare: {:.1}s ({warehouses} warehouse(s))",
            prepare_started.elapsed().as_secs_f64()
        );
    }

    let stop = CancellationToken::new();
    let stopper = stop.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(secs as u64)).await;
        stopper.cancel();
    });

    let report = driver.run(stop).await.expect("run the OLTP workload");
    report.print_summary();

    // The headline comparison: tpmC against the rate the SF1 HTAP dispatch asks
    // for (9250 txn/s x 45% new-order x 60 = 249_750 tpmC).
    let target_tpmc = f64::from(rate.unwrap_or(9250)) * 0.45 * 60.0;
    println!(
        "  terminals: {terminals}, rate: {}",
        rate.map_or_else(|| "unlimited".to_string(), |r| format!("{r} txn/s")),
    );
    println!(
        "  tpmC vs {target_tpmc:.0} target: {:.1}%",
        report.tpmc / target_tpmc * 100.0
    );

    assert!(
        report.total_committed > 0,
        "no transaction committed in {secs}s — the workload did not run"
    );
}

/// A single `_bench_ts` value read straight from the server, for the tables whose
/// stamp the driver now owns.
async fn server_max_bench_ts(conn: &mut mysql_async::Conn, table: &str) -> Option<i64> {
    let v: Option<Option<chrono::NaiveDateTime>> = conn
        .query_first(format!("SELECT MAX(_bench_ts) FROM {table}"))
        .await
        .expect("read MAX(_bench_ts)");
    v.flatten().map(|dt| dt.and_utc().timestamp_micros())
}

/// After `prepare()`, the schema must carry none of the old per-row stamping
/// machinery and all of the new supporting structure.
///
/// Guards the upgrade hazard: a surviving `BEFORE UPDATE` trigger would overwrite
/// the driver's bound stamp with `NOW(3)`, putting the stored value above the
/// recorded watermark so the drain gate could never converge.
#[tokio::test(flavor = "multi_thread")]
async fn prepare_leaves_no_triggers_and_no_column_default() {
    if !enabled() {
        eprintln!("skipping: set CHBENCH_MYSQL_E2E=1 to run against a live MySQL source");
        return;
    }
    let _exclusive = exclusive_source().await;
    let cfg = source();
    let driver = MysqlChBenchDriver::connect(
        ChBenchConfig {
            warehouses: 1,
            terminals: 1,
            ..Default::default()
        },
        source(),
    )
    .await
    .expect("connect");
    driver.prepare().await.expect("prepare");

    let mut conn = raw_conn(&cfg).await;

    let triggers: Vec<String> = conn
        .query(
            "SELECT TRIGGER_NAME FROM information_schema.TRIGGERS \
             WHERE TRIGGER_SCHEMA = DATABASE() AND TRIGGER_NAME LIKE 'trg_bench_ts%'",
        )
        .await
        .expect("list triggers");
    assert!(
        triggers.is_empty(),
        "prepare() must leave no _bench_ts triggers, found {triggers:?}"
    );

    // The constant seed default stays after the load (live statements always
    // bind _bench_ts, so it is never consulted again). It must be a constant,
    // not a live expression: CURRENT_TIMESTAMP here would re-stamp rows behind
    // the driver's back on any unbound INSERT.
    let defaults: Vec<(String, Option<String>)> = conn
        .query(
            "SELECT TABLE_NAME, COLUMN_DEFAULT FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = DATABASE() AND COLUMN_NAME = '_bench_ts' \
             ORDER BY TABLE_NAME",
        )
        .await
        .expect("list _bench_ts columns");
    assert_eq!(
        defaults.len(),
        8,
        "every mutated table needs a _bench_ts column"
    );
    for (table, default) in &defaults {
        let d = default.as_deref().unwrap_or("");
        assert!(
            !d.to_ascii_uppercase().contains("CURRENT_TIMESTAMP"),
            "{table}._bench_ts default must be a constant, found {default:?}"
        );
    }

    // The delete-bearing table is answered by a plain scan — deliberately no
    // _bench_ts index anywhere (an order_line index measured a 7.7% tpmC cost;
    // new_order is bounded so its scan is cheap).
    let idx: Option<i64> = conn
        .query_first(
            "SELECT COUNT(*) FROM information_schema.STATISTICS \
             WHERE TABLE_SCHEMA = DATABASE() AND COLUMN_NAME = '_bench_ts'",
        )
        .await
        .expect("check index");
    assert_eq!(
        idx.unwrap_or(0),
        0,
        "no _bench_ts index may exist after prepare"
    );
}

/// The core invariant: for every table the in-memory watermark serves, it must
/// equal the true source `MAX(_bench_ts)` exactly.
///
/// Exercised after a real workload, so it covers the whole surface at once — a
/// mutating statement that forgot to bind `_bench_ts`, a stamp that rounded up on
/// store, a table recorded by a transaction that did not write it, and a
/// watermark advanced by one of the ~1% of new-order transactions that roll back
/// (any premature stamping would leave the watermark above the stored maximum).
#[tokio::test(flavor = "multi_thread")]
async fn watermark_equals_source_max_after_workload() {
    if !enabled() {
        eprintln!("skipping: set CHBENCH_MYSQL_E2E=1 to run against a live MySQL source");
        return;
    }
    let _exclusive = exclusive_source().await;
    let cfg = source();
    let driver = MysqlChBenchDriver::connect(
        ChBenchConfig {
            warehouses: 1,
            terminals: 4,
            ..Default::default()
        },
        source(),
    )
    .await
    .expect("connect");
    driver.prepare().await.expect("prepare");

    // Straight after prepare, before any mutation: every seed row carries the
    // load timestamp, so the watermark is already exact — including for tables a
    // short workload might never touch.
    let mut conn = raw_conn(&cfg).await;
    for table in chbench_driver::watermark::MutatedTable::ALL {
        let name = table.as_str();
        let server = server_max_bench_ts(&mut conn, name).await;
        let watermark = driver.max_bench_ts(name).await.expect("watermark");
        assert_eq!(
            watermark, server,
            "{name}: watermark must equal the source max straight after prepare"
        );
    }

    let stop = CancellationToken::new();
    let stopper = stop.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(15)).await;
        stopper.cancel();
    });
    let report = driver.run(stop).await.expect("run workload");
    assert!(
        report.total_committed > 100,
        "workload too short to be meaningful: {} committed",
        report.total_committed
    );
    println!(
        "workload: {} committed, {} aborted",
        report.total_committed, report.total_aborted
    );

    for table in chbench_driver::watermark::MutatedTable::ALL {
        let name = table.as_str();
        let server = server_max_bench_ts(&mut conn, name).await;
        let watermark = driver.max_bench_ts(name).await.expect("watermark");
        let exact = driver.max_bench_ts_exact(name).await.expect("exact");

        assert_eq!(
            exact, server,
            "{name}: max_bench_ts_exact must agree with a direct server read"
        );
        assert_eq!(
            watermark, exact,
            "{name}: watermark {watermark:?} != source MAX(_bench_ts) {exact:?}"
        );
    }
}

/// `new_order` is the one mutated table with DELETEs, so its `MAX(_bench_ts)` can
/// *decrease* — a monotone watermark cannot follow that down. Draining a
/// district's queue to empty is the case that breaks a watermark-served answer,
/// so it must still agree with the source.
///
/// This fails if someone later routes `new_order` onto the in-memory path.
#[tokio::test(flavor = "multi_thread")]
async fn new_order_deletes_do_not_strand_the_watermark() {
    if !enabled() {
        eprintln!("skipping: set CHBENCH_MYSQL_E2E=1 to run against a live MySQL source");
        return;
    }
    let _exclusive = exclusive_source().await;
    assert!(
        chbench_driver::watermark::is_delete_bearing("new_order"),
        "new_order must be classified delete-bearing"
    );

    let cfg = source();
    let driver = MysqlChBenchDriver::connect(
        ChBenchConfig {
            warehouses: 1,
            terminals: 2,
            ..Default::default()
        },
        source(),
    )
    .await
    .expect("connect");
    driver.prepare().await.expect("prepare");

    // Build up orders, then let delivery drain them.
    for (secs, mix) in [(8u64, [100, 0, 0, 0, 0]), (8, [0, 0, 100, 0, 0])] {
        let d = MysqlChBenchDriver::connect(
            ChBenchConfig {
                warehouses: 1,
                terminals: 2,
                mix,
                ..Default::default()
            },
            source(),
        )
        .await
        .expect("connect");
        d.verify_prepared().await.expect("verify");
        let stop = CancellationToken::new();
        let stopper = stop.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(secs)).await;
            stopper.cancel();
        });
        d.run(stop).await.expect("run phase");
    }

    let mut conn = raw_conn(&cfg).await;
    // Empty every district's queue so the row holding the maximum is gone.
    conn.query_drop("DELETE FROM new_order")
        .await
        .expect("drain new_order");

    let remaining: Option<i64> = conn
        .query_first("SELECT COUNT(*) FROM new_order")
        .await
        .expect("count");
    assert_eq!(remaining, Some(0), "new_order must be empty for this check");

    let server = server_max_bench_ts(&mut conn, "new_order").await;
    assert_eq!(server, None, "an empty table has no maximum");
    let watermark = driver.max_bench_ts("new_order").await.expect("watermark");
    assert_eq!(
        watermark, server,
        "new_order must be answered from the source, so an emptied table reports None, \
         not a stranded monotone watermark"
    );

    // A non-delete-bearing table would strand here; new_order does not.
    let exact = driver.max_bench_ts_exact("new_order").await.expect("exact");
    assert_eq!(exact, None);
}

/// The negative check: a surviving `_bench_ts` trigger must produce a *detectable*
/// watermark/source disagreement.
///
/// Without this there is no evidence the drain gate's authoritative
/// `max_bench_ts_exact` comparison can actually catch driver/source divergence —
/// only that it agrees when everything is correct. A `BEFORE UPDATE` trigger
/// overwrites the driver's bound stamp with `NOW(3)`, which is strictly later, so
/// the stored maximum ends up *above* the recorded watermark.
#[tokio::test(flavor = "multi_thread")]
async fn a_stale_trigger_makes_the_watermark_disagree_with_the_source() {
    if !enabled() {
        eprintln!("skipping: set CHBENCH_MYSQL_E2E=1 to run against a live MySQL source");
        return;
    }
    let _exclusive = exclusive_source().await;
    let cfg = source();
    let driver = MysqlChBenchDriver::connect(
        ChBenchConfig {
            warehouses: 1,
            terminals: 2,
            mix: [0, 100, 0, 0, 0],
            ..Default::default()
        },
        source(),
    )
    .await
    .expect("connect");
    driver.prepare().await.expect("prepare");

    // Reintroduce exactly what an older build left behind, after prepare has
    // cleaned up — the upgrade hazard this guards.
    let mut conn = raw_conn(&cfg).await;
    conn.query_drop(
        "CREATE TRIGGER trg_bench_ts_upd_customer BEFORE UPDATE ON customer \
         FOR EACH ROW SET NEW._bench_ts = NOW(3)",
    )
    .await
    .expect("recreate a legacy trigger");

    let stop = CancellationToken::new();
    let stopper = stop.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(6)).await;
        stopper.cancel();
    });
    driver.run(stop).await.expect("run payment workload");

    let watermark = driver.max_bench_ts("customer").await.expect("watermark");
    let exact = driver.max_bench_ts_exact("customer").await.expect("exact");
    assert_ne!(
        watermark, exact,
        "a stale BEFORE UPDATE trigger must make the watermark disagree with the \
         source, otherwise the drain gate's authoritative check proves nothing"
    );
    assert!(
        exact > watermark,
        "the trigger stamps later than the driver, so the stored max must exceed \
         the watermark (watermark={watermark:?} exact={exact:?})"
    );

    // Clean up so later runs are not poisoned.
    conn.query_drop("DROP TRIGGER IF EXISTS trg_bench_ts_upd_customer")
        .await
        .expect("drop the legacy trigger");
}
