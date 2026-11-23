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

//! One Spicepod, three clients: the micro queries run against a runtime built from a
//! Spicepod, in-process, over HTTP `/v1/sql` and over Flight SQL, and every answer is
//! compared with standalone `DuckDB` and standalone `SQLite` loaded with the same rows.
//!
//! Unlike the micro tests beside this module, the SQL is planned by the runtime, so
//! filters, aggregates, joins and limits reach the accelerator through the accelerated
//! table, and the answer crosses the HTTP JSON and Flight encoders on its way back. The
//! source files are deleted once the acceleration is loaded, so a query that read the
//! source instead of the acceleration fails rather than passing on the source's rows.
//!
//! A `partition_by: [bucket(4, id)]` Cayenne acceleration runs the same queries, so a
//! union of partitions, and a filter on `id` pruned to one partition, answer the same as
//! one unpartitioned table in the oracles.

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use runtime::Runtime;

use super::{
    compare, make_dim, make_fact, micro_queries, standalone_duckdb, standalone_sqlite,
    write_parquet,
};
use crate::configure_test_datafusion;
use crate::utils::{
    register_test_connectors, run_query, runtime_ready_check,
    sql_clients::{SURFACES, SqlClients},
    test_request_context,
};

const FACT_ROWS: usize = 2_048;
const DIM_ROWS: usize = 256;

/// Where an acceleration keeps its rows.
#[derive(Clone, Copy)]
enum Storage {
    Memory,
    File,
}

impl Storage {
    fn label(self) -> &'static str {
        match self {
            Storage::Memory => "memory",
            Storage::File => "file",
        }
    }
}

/// How an acceleration lays its rows out.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Layout {
    Unpartitioned,
    /// `partition_by: [bucket(4, id)]` on both tables, so every query reads a union of
    /// partitions, and a filter on `id` can prune all but one.
    BucketById,
}

impl Layout {
    /// The number of partitions `BucketById` splits a table into.
    const BUCKETS: usize = 4;

    fn label(self) -> &'static str {
        match self {
            Layout::Unpartitioned => "unpartitioned",
            Layout::BucketById => "bucket(4, id)",
        }
    }

    fn spicepod_lines(self) -> &'static str {
        match self {
            Layout::Unpartitioned => "",
            Layout::BucketById => "      partition_by:\n        - bucket(4, id)\n",
        }
    }
}

/// A Spicepod accelerating `t` and `d` from parquet files in `dir` with `engine`, keeping
/// the rows in `storage`, laid out as `layout`.
fn spicepod(engine: &str, storage: Storage, layout: Layout, dir: &std::path::Path) -> String {
    let mut datasets = String::new();
    for name in ["t", "d"] {
        // A file-mode Cayenne acceleration keeps its data and metastore under `dir`, and one
        // process keeps one file-mode Cayenne metastore, so both datasets share its
        // directory. A memory-mode Cayenne acceleration keeps both in RAM.
        let storage = match (engine, storage) {
            ("cayenne", Storage::File) => format!(
                "      mode: file\n      params:\n        cayenne_file_path: {data}\n        cayenne_metadata_dir: {meta}\n",
                data = dir.join(format!("cayenne_{name}_data")).display(),
                meta = dir.join("cayenne_metadata").display(),
            ),
            (_, Storage::File) => panic!("only Cayenne runs these surfaces in `mode: file`"),
            (_, Storage::Memory) => "      mode: memory\n".to_string(),
        };
        write!(
            datasets,
            "  - from: file://{path}\n    name: {name}\n    acceleration:\n      enabled: true\n      engine: {engine}\n      refresh_mode: full\n{storage}{layout}",
            path = dir.join(format!("{name}.parquet")).display(),
            layout = layout.spicepod_lines(),
        )
        .expect("writing to a String cannot fail");
    }
    format!(
        "version: v1\nkind: Spicepod\nname: surfaces_{engine}_{mode}{partitioned}\ndatasets:\n{datasets}",
        mode = storage.label(),
        partitioned = if layout == Layout::Unpartitioned {
            ""
        } else {
            "_partitioned"
        },
    )
}

fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

/// Loads `engine` in `storage` from a Spicepod, deletes the source files, starts the HTTP
/// and Flight servers, and compares every micro query on every client with both standalone
/// oracles.
async fn surfaces_match_standalone_oracles(engine: &str, storage: Storage, layout: Layout) {
    let subject = match layout {
        Layout::Unpartitioned => format!("{engine}[{}]", storage.label()),
        Layout::BucketById => format!("{engine}[{}, {}]", storage.label(), layout.label()),
    };
    let tables = [("t", make_fact(FACT_ROWS, 64)), ("d", make_dim(DIM_ROWS))];
    let dir = tempfile::tempdir().expect("temp dir");
    for (name, batch) in &tables {
        write_parquet(&dir.path().join(format!("{name}.parquet")), batch);
    }
    std::fs::write(
        dir.path().join("spicepod.yaml"),
        spicepod(engine, storage, layout, dir.path()),
    )
    .expect("the spicepod must be written");

    let (_sqlite_dir, sqlite) = standalone_sqlite::load_standalone_sqlite(&tables);
    let (_duckdb_dir, duckdb) = standalone_duckdb::load_standalone_duckdb(&tables);

    register_test_connectors().await;
    configure_test_datafusion();
    let app = app::AppBuilder::build_from_path(dir.path().to_path_buf())
        .await
        .expect("the spicepod must load");
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    tokio::time::timeout(Duration::from_mins(2), Arc::clone(&rt).load_components())
        .await
        .expect("components must load");
    runtime_ready_check(&rt).await;

    // From here on only the acceleration holds the rows.
    for (name, _) in &tables {
        std::fs::remove_file(dir.path().join(format!("{name}.parquet")))
            .unwrap_or_else(|e| panic!("remove {name}.parquet: {e}"));
    }

    let clients = SqlClients::start(&rt)
        .await
        .unwrap_or_else(|e| panic!("{e}"));

    if engine == "cayenne" {
        assert_cayenne_storage(&rt, storage).await;
    }
    if layout == Layout::BucketById {
        assert_bucket_partitions(&rt).await;
    }

    let mut fails = Vec::new();
    for q in micro_queries() {
        let oracles = [
            (
                "standalone-duckdb",
                standalone_duckdb::standalone_query(&duckdb, &q.sql)
                    .unwrap_or_else(|e| panic!("duckdb sql {}: {e}", q.name)),
            ),
            (
                "standalone-sqlite",
                standalone_sqlite::standalone_query(&sqlite, &q.sql)
                    .unwrap_or_else(|e| panic!("sqlite sql {}: {e}", q.name)),
            ),
        ];
        // Every micro query returns rows, so an empty answer on either side is a load or a
        // fallback that did not happen, not an agreement.
        for (oracle, expected) in &oracles {
            assert!(
                row_count(expected) > 0,
                "{oracle} returned no rows for {}",
                q.name
            );
        }
        let mut surfaces = Vec::with_capacity(SURFACES.len());
        for surface in SURFACES {
            surfaces.push((surface.label(), clients.query(surface, &q.sql).await));
        }
        for (surface, spice) in &surfaces {
            let spice = match spice {
                Ok(batches) if row_count(batches) > 0 => batches,
                Ok(_) => {
                    fails.push(format!("{subject}/{surface}/{}: empty answer", q.name));
                    continue;
                }
                Err(e) => {
                    fails.push(format!("{subject}/{surface}/{}: query failed: {e}", q.name));
                    continue;
                }
            };
            for (oracle, expected) in &oracles {
                let status = compare(&q, spice, expected);
                eprintln!("{subject}/{surface} vs {oracle} / {} -> {status}", q.name);
                if !status.starts_with("PASS") {
                    fails.push(format!(
                        "{subject}/{surface} vs {oracle}/{}: {status}",
                        q.name
                    ));
                }
            }
        }
    }
    assert!(
        fails.is_empty(),
        "Spice {subject} acceleration vs standalone oracles failures: {fails:#?}"
    );
}

/// Fails unless a scan of `t` reads Cayenne in `storage`: a memory-mode acceleration scans
/// its in-RAM tier and no file, while a file-mode one scans the Vortex files it wrote.
async fn assert_cayenne_storage(rt: &Arc<Runtime>, storage: Storage) {
    let plan = run_query(
        rt,
        "EXPLAIN SELECT SUM(value) FROM t WHERE id BETWEEN 10 AND 50",
    )
    .await
    .expect("EXPLAIN must succeed");
    let plan = arrow::util::pretty::pretty_format_batches(&plan)
        .expect("the plan must format")
        .to_string();
    let (scan, files) = match storage {
        Storage::Memory => (
            "CayenneAccelerationExec: snapshots_scanned=0, files_scanned=0",
            false,
        ),
        Storage::File => ("CayenneAccelerationExec", true),
    };
    assert!(
        plan.contains(scan) && plan.contains("file_type=vortex") == files,
        "a `mode: {}` Cayenne scan must contain `{scan}` and {} Vortex files:\n{plan}",
        storage.label(),
        if files { "read" } else { "read no" },
    );
}

/// Fails unless the acceleration of `t` is split into `Layout::BUCKETS` partitions that a
/// full scan reads as a union, and a filter on `id` prunes to one, so the micro queries
/// below ran on the partitioned path rather than on one unpartitioned table.
async fn assert_bucket_partitions(rt: &Arc<Runtime>) {
    for (sql, scans) in [
        ("EXPLAIN SELECT SUM(value) FROM t", Layout::BUCKETS),
        ("EXPLAIN SELECT id, name, value FROM t WHERE id = 42", 1),
    ] {
        let plan = run_query(rt, sql).await.expect("EXPLAIN must succeed");
        let plan = arrow::util::pretty::pretty_format_batches(&plan)
            .expect("the plan must format")
            .to_string();
        assert_eq!(
            plan.matches("CayenneAccelerationExec").count(),
            scans,
            "`{sql}` must scan {scans} of the {} `bucket(4, id)` partitions:\n{plan}",
            Layout::BUCKETS,
        );
        if scans > 1 {
            assert!(
                plan.contains("PartitionedUnionExec"),
                "`{sql}` must read the partitions as a union:\n{plan}"
            );
        }
    }
}

async fn run(engine: &'static str, storage: Storage, layout: Layout) {
    test_request_context()
        .scope(surfaces_match_standalone_oracles(engine, storage, layout))
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn arrow_surfaces_vs_standalone_engines() {
    run("arrow", Storage::Memory, Layout::Unpartitioned).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cayenne_surfaces_vs_standalone_engines() {
    run("cayenne", Storage::File, Layout::Unpartitioned).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cayenne_memory_surfaces_vs_standalone_engines() {
    run("cayenne", Storage::Memory, Layout::Unpartitioned).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cayenne_bucket_partitioned_surfaces_vs_standalone_engines() {
    run("cayenne", Storage::File, Layout::BucketById).await;
}
