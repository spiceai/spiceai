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

//! The lock that lets result-correctness lanes in concurrent nextest processes
//! share generated fixtures (`support::ensure_fixture`). A module of the
//! `integration` binary only: the support module is also compiled into the chDB
//! lane's own target, which has no reason to run these.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::support::{chbench_data, ssb_data, tpcds_data, tpch_data};
use crate::support::{ensure_fixture, fixture_is_current, generator_revision, lock_fixture};

/// Rows in each `*.parquet` file of `dir`, by table name.
fn parquet_rows(dir: &Path) -> BTreeMap<String, usize> {
    std::fs::read_dir(dir)
        .expect("read the fixture dir")
        .map(|entry| entry.expect("fixture dir entry").path())
        .filter_map(|path| {
            let name = path
                .file_name()?
                .to_str()?
                .strip_suffix(".parquet")?
                .to_string();
            let file = std::fs::File::open(&path).expect("open a fixture file");
            let rows = ParquetRecordBatchReaderBuilder::try_new(file)
                .and_then(ParquetRecordBatchReaderBuilder::build)
                .unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
                .map(|batch| batch.expect("parquet batch").num_rows())
                .sum();
            Some((name, rows))
        })
        .collect()
}

/// Each fixture helper, started while another holder has that fixture's
/// lock, must not write the fixture or return until the lock is released,
/// and must then leave it complete and current.
///
/// Lanes that share a fixture run in separate nextest processes. Each holder
/// here opens the lock file itself, as two processes would: `File::lock`
/// locks are held per open file, so two opens contend within one process
/// too. The helpers run at small scales, side by side, under one window.
///
/// TPC-DS takes about 16 seconds to generate even at SF 0.01, so its
/// fixture is stamped current before the lock is taken: its helper must
/// still wait for the lock, and then return without writing anything. Its
/// generator runs in the TPC-DS lanes themselves.
type Helper = (&'static str, PathBuf, Box<dyn FnOnce(&Path) + Send>);

#[test]
fn every_fixture_helper_waits_for_its_lock() {
    let scratch = tempfile::tempdir().expect("scratch dir");
    let at = |name: &str| scratch.path().join(name);
    let tpcds_dir = at("tpcds_tpcdsgen_sf0.01");
    std::fs::create_dir_all(&tpcds_dir).expect("tpcds fixture dir");
    crate::support::mark_fixture_complete(
        &tpcds_dir,
        &generator_revision(include_str!("correctness/support/tpcds_data.rs")),
    );
    let helpers: Vec<Helper> = vec![
        (
            "tpch",
            at("tpch_sf0.01"),
            Box::new(|dir: &Path| tpch_data::ensure_tpch_fixture(dir, 0.01)),
        ),
        (
            "ssb",
            at("ssb_scale1"),
            Box::new(|dir: &Path| ssb_data::ensure_ssb_fixture(dir, 1)),
        ),
        (
            "chbench",
            at("chbench_sf1"),
            Box::new(|dir: &Path| chbench_data::ensure_chbench_fixture(dir, 1)),
        ),
        (
            "tpcds",
            tpcds_dir,
            Box::new(|dir: &Path| tpcds_data::ensure_tpcds_fixture(dir, 0.01)),
        ),
    ];

    let held: Vec<std::fs::File> = helpers
        .iter()
        .map(|(_, dir, _)| lock_fixture(dir))
        .collect();
    let workers: Vec<(&str, PathBuf, JoinHandle<()>)> = helpers
        .into_iter()
        .map(|(name, dir, ensure)| {
            let worker_dir = dir.clone();
            (name, dir, std::thread::spawn(move || ensure(&worker_dir)))
        })
        .collect();

    // Unlocked, each helper writes its fixture directory within
    // milliseconds of starting. Watching for that over two seconds is a
    // window in which a helper that ignored the lock would have started,
    // so this wait is the property under test, not a readiness guess.
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        for (name, dir, worker) in &workers {
            let written = if *name == "tpcds" {
                !parquet_rows(dir).is_empty()
            } else {
                dir.exists()
            };
            assert!(
                !written,
                "the {name} fixture was written while another holder had its lock"
            );
            assert!(
                !worker.is_finished(),
                "the {name} helper returned while the lock was held"
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    drop(held);
    for (name, dir, worker) in workers {
        worker
            .join()
            .unwrap_or_else(|_| panic!("the {name} helper after the lock was released"));
        let revision = match name {
            "tpch" => generator_revision(include_str!("correctness/support/tpch_data.rs")),
            "ssb" => generator_revision(include_str!("correctness/support/ssb_data.rs")),
            "chbench" => generator_revision(include_str!("correctness/support/chbench_data.rs")),
            _ => generator_revision(include_str!("correctness/support/tpcds_data.rs")),
        };
        assert!(
            fixture_is_current(&dir, &revision),
            "the {name} fixture is stamped with its generator"
        );
        let rows = parquet_rows(&dir);
        let expected: BTreeMap<String, usize> = match name {
            "tpch" => tpch_data::tpch_batches(0.01)
                .into_iter()
                .map(|(table, batch)| (table.to_string(), batch.num_rows()))
                .collect(),
            "ssb" => ssb_data::ssb_batches(1)
                .into_iter()
                .map(|(table, batch)| (table.to_string(), batch.num_rows()))
                .collect(),
            "chbench" => chbench_data::chbench_batches(1)
                .into_iter()
                .map(|(table, batch)| (table.to_string(), batch.num_rows()))
                .collect(),
            // Stamped current before the lock was taken, so its helper
            // found it current and wrote nothing.
            _ => BTreeMap::new(),
        };
        assert_eq!(rows, expected, "rows of each {name} table");
    }
}

/// `ensure_fixture` is what every lane goes through, including the `DuckDB`
/// lane's `dsdgen` fixture, which needs the network and so is not run
/// above. A generator that cannot supply the fixture leaves it unstamped
/// and reported missing; one that can is stamped and found current after.
#[test]
fn ensure_fixture_stamps_only_a_generated_fixture() {
    let scratch = tempfile::tempdir().expect("scratch dir");
    let dir = scratch.path().join("fixture");

    assert!(
        !ensure_fixture(&dir, "r1", |_| false),
        "an unavailable fixture is reported missing"
    );
    assert!(
        !fixture_is_current(&dir, "r1"),
        "an unavailable fixture is not stamped"
    );

    // A closure that only reads a `Cell` is `Copy`, so each call below gets
    // its own copy while they all count into one place.
    let generated = std::cell::Cell::new(0);
    let generate = |dir: &Path| {
        generated.set(generated.get() + 1);
        std::fs::create_dir_all(dir).expect("fixture dir");
        true
    };
    assert!(
        ensure_fixture(&dir, "r1", generate),
        "a generated fixture is reported current"
    );
    assert!(
        ensure_fixture(&dir, "r1", generate),
        "a current fixture is reported current"
    );
    assert_eq!(
        generated.get(),
        1,
        "a current fixture is not generated again"
    );
    assert!(
        ensure_fixture(&dir, "r2", generate),
        "a fixture from another revision is regenerated"
    );
    assert_eq!(
        generated.get(),
        2,
        "a fixture from another revision is generated again"
    );
}
