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

//! Self-contained `MySQL` OLTP throughput benchmark.
//!
//! Bootstraps everything itself: starts a `mysql:8.0` Docker container with the
//! same server flags CI uses (`.github/actions/setup-chbench-mysql`), waits for
//! it to become ready, seeds the CH-benCH dataset, runs the TPC-C OLTP workload
//! for a fixed window, and prints the tpmC report. It is a *measurement*, not an
//! assertion — absolute numbers depend on the host, so it only asserts that the
//! workload actually ran. Its purpose is comparing the OLTP generator's
//! throughput before and after driver changes on the same machine.
//!
//! Gated on `CHBENCH_MYSQL_BENCH=1` so plain `cargo test` stays fast and never
//! pulls Docker images:
//!
//! ```shell
//! CHBENCH_MYSQL_BENCH=1 cargo test -p chbench-driver --test mysql_oltp_bench -- --nocapture
//! ```
//!
//! Defaults follow the SF10 HTAP dispatch shape: 10 warehouses, 100 terminals,
//! unlimited rate. Knobs (all optional): `CHBENCH_WAREHOUSES`,
//! `CHBENCH_TERMINALS`, `CHBENCH_RATE` (txn/s; unset = unlimited),
//! `CHBENCH_BENCH_SECS` (default 30), `CHBENCH_SKIP_PREPARE=1` (reuse the
//! already-seeded dataset — turns the multi-minute seed into a ~15s re-measure
//! loop), and `CHBENCH_MYSQL_BENCH_PORT` (host port for a fresh container,
//! default 33306 so a local `MySQL` on 3306 is untouched).
//!
//! The container is deliberately left running after the test so the next run
//! can skip image pull, server init, and seeding. Remove it with:
//!
//! ```shell
//! docker rm -f chbench-mysql-oltp-bench
//! ```

use std::time::{Duration, Instant};

use chbench_driver::{ChBenchConfig, ChBenchDriver, MysqlChBenchDriver, MysqlSourceConfig};
use tokio_util::sync::CancellationToken;

/// Fixed container name so re-runs find and reuse the same server.
const CONTAINER: &str = "chbench-mysql-oltp-bench";

/// Whether the caller asked for the self-contained benchmark.
fn enabled() -> bool {
    std::env::var("CHBENCH_MYSQL_BENCH").is_ok_and(|v| v == "1")
}

/// A `usize` knob from the environment.
fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Run the Docker CLI and return its output; panics only if the binary itself
/// cannot be spawned (individual command failures are for callers to judge).
async fn docker(args: &[&str]) -> std::process::Output {
    tokio::process::Command::new("docker")
        .args(args)
        .output()
        .await
        .unwrap_or_else(|e| panic!("failed to spawn `docker {}`: {e}", args.join(" ")))
}

/// Stdout of a Docker command that must succeed.
async fn docker_ok(args: &[&str]) -> String {
    let out = docker(args).await;
    assert!(
        out.status.success(),
        "`docker {}` failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// Ensure the benchmark container is up, returning the host port to connect to.
///
/// Reuses a running container (reading back whatever host port it was created
/// with), replaces a stopped one, and otherwise creates a fresh one with the
/// same server flags as `.github/actions/setup-chbench-mysql/action.yml` — keep
/// the two in sync so local numbers stay comparable to CI runs.
async fn ensure_container() -> u16 {
    let inspect = docker(&["inspect", "-f", "{{.State.Running}}", CONTAINER]).await;
    if inspect.status.success() {
        if String::from_utf8_lossy(&inspect.stdout).trim() == "true" {
            // Running — reuse it on whatever port it was created with.
            let ports = docker_ok(&["port", CONTAINER, "3306/tcp"]).await;
            let port: u16 = ports
                .lines()
                .next()
                .and_then(|l| l.rsplit(':').next())
                .and_then(|p| p.trim().parse().ok())
                .unwrap_or_else(|| panic!("cannot parse `docker port` output: {ports}"));
            println!("container: reusing running {CONTAINER} on port {port}");
            return port;
        }
        // Exists but stopped — its flags may predate this test; start fresh.
        docker_ok(&["rm", "-f", CONTAINER]).await;
    }

    let port = env_usize("CHBENCH_MYSQL_BENCH_PORT", 33306);
    let port = u16::try_from(port).expect("CHBENCH_MYSQL_BENCH_PORT must fit a u16");
    println!("container: starting {CONTAINER} on port {port} (first run pulls mysql:8.0)");
    let publish = format!("{port}:3306");
    docker_ok(&[
        "run",
        "-d",
        "--name",
        CONTAINER,
        "-e",
        "MYSQL_ROOT_PASSWORD=rootpw",
        "-e",
        "MYSQL_DATABASE=chbench",
        "-e",
        "MYSQL_USER=bench",
        "-e",
        "MYSQL_PASSWORD=bench",
        "-p",
        &publish,
        "mysql:8.0",
        "--default-authentication-plugin=mysql_native_password",
        "--log-bin-trust-function-creators=1",
        "--binlog-format=ROW",
        "--binlog-row-image=FULL",
        "--max-connections=200",
        "--innodb-buffer-pool-size=4G",
        "--sync-binlog=0",
        "--innodb-flush-log-at-trx-commit=0",
        "--innodb-use-native-aio=0",
        "--innodb-doublewrite=0",
        "--innodb-flush-method=O_DIRECT_NO_FSYNC",
        "--local-infile=1",
    ])
    .await;
    port
}

/// Poll until the server authenticates (a bare ping reports "alive" even
/// mid-init), then grant the replication/session privileges the seed loader
/// uses — mirroring the readiness step of the CI setup action.
async fn wait_ready_and_grant() {
    let deadline = Instant::now() + Duration::from_mins(3);
    loop {
        let out = docker(&[
            "exec", CONTAINER, "mysql", "-uroot", "-prootpw", "-e", "SELECT 1",
        ])
        .await;
        if out.status.success() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "MySQL did not become ready within 3 minutes; last error: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    docker_ok(&[
        "exec",
        CONTAINER,
        "mysql",
        "-uroot",
        "-prootpw",
        "-e",
        "GRANT REPLICATION SLAVE, REPLICATION CLIENT, SESSION_VARIABLES_ADMIN ON *.* TO 'bench'@'%'; \
         FLUSH PRIVILEGES;",
    ])
    .await;
}

/// Bootstrap a `MySQL` container, seed the SF dataset, run the OLTP workload,
/// and print the tpmC report.
#[tokio::test(flavor = "multi_thread")]
async fn dockerized_throughput() {
    if !enabled() {
        eprintln!("skipping: set CHBENCH_MYSQL_BENCH=1 to run the self-contained OLTP benchmark");
        return;
    }
    let docker_probe = docker(&["info", "--format", "{{.ServerVersion}}"]).await;
    assert!(
        docker_probe.status.success(),
        "CHBENCH_MYSQL_BENCH=1 requires a running Docker daemon: {}",
        String::from_utf8_lossy(&docker_probe.stderr)
    );

    let port = ensure_container().await;
    wait_ready_and_grant().await;

    let warehouses = env_usize("CHBENCH_WAREHOUSES", 10);
    let terminals = env_usize("CHBENCH_TERMINALS", 100);
    let secs = env_usize("CHBENCH_BENCH_SECS", 30);
    let rate: Option<u32> = std::env::var("CHBENCH_RATE")
        .ok()
        .and_then(|v| v.parse().ok());

    let cfg = ChBenchConfig {
        warehouses,
        terminals,
        rate,
        ..Default::default()
    };
    let src = MysqlSourceConfig {
        port,
        ..Default::default()
    };
    let driver = MysqlChBenchDriver::connect(cfg, src)
        .await
        .expect("connect the CH-benCH driver");

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
    let secs = u64::try_from(secs).expect("CHBENCH_BENCH_SECS must fit a u64");
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(secs)).await;
        stopper.cancel();
    });

    let report = driver.run(stop).await.expect("run the OLTP workload");
    report.print_summary();
    println!(
        "  warehouses: {warehouses}, terminals: {terminals}, rate: {}",
        rate.map_or_else(|| "unlimited".to_string(), |r| format!("{r} txn/s")),
    );
    println!(
        "container {CONTAINER} left running for fast re-runs \
         (CHBENCH_SKIP_PREPARE=1 to reuse the seed); remove with: docker rm -f {CONTAINER}"
    );

    assert!(
        report.total_committed > 0,
        "no transaction committed in {secs}s — the workload did not run"
    );
}
