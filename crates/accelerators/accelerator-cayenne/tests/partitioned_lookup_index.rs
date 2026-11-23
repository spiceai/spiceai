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

//! Secondary indexes (`acceleration.indexes`) on a partitioned Cayenne
//! acceleration (`partition_by`) answer every lookup exactly as an unindexed
//! acceleration of the same data does, and every partition answers its lookups
//! from its own index.
//!
//! Two accelerations take the same writes: one declares `indexes`, the other
//! does not, and both are created through
//! `AcceleratorEngineRegistry::create_accelerator_table`, the runtime's path,
//! so each partition gets its index the way a dataset's do. After each write —
//! an overwrite, an append into existing partitions, an append that creates a
//! partition, upserts that move a row to another index key, CDC deletes, and a
//! restart with and without the persisted index runs — every lookup returns
//! the same rows from both. The plan of each lookup on the indexed acceleration
//! must show every partition's scan served by the index with no file read in
//! full, so a partition that lost its index fails the test rather than
//! returning the same rows through a full scan.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use accelerator_cayenne::CayenneAccelerator;
use arrow::{
    array::{Array, Int64Array, ListArray, RecordBatch, StringArray, StructArray},
    buffer::OffsetBuffer,
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use data_accelerator_api::{AccelerationSource, AcceleratorEngineRegistry, DataAccelerator};
use data_components::cdc;
use datafusion::{
    common::TableReference, datasource::TableProvider, execution::context::SessionContext,
};
use datafusion_table_providers::util::column_reference::ColumnReference;
use runtime_acceleration::{
    Engine,
    acceleration::{Acceleration, IndexType, Mode, RefreshMode},
    change_sink::{ChangeBatch, ChangeSink, ChangeSinkContext, Recovery, WriteOptions},
    testing::TestAccelerationSource,
};
use tokio::runtime::Handle;

/// `(id, p, k, a, b, v)`: `p` is the partition column, `k` and `(a, b)` are
/// the indexed keys.
type Row = (i64, i64, Option<String>, Option<i64>, i64, i64);

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("p", DataType::Int64, false),
        Field::new("k", DataType::Utf8, true),
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

/// Rows `ids` spread over `partitions` partitions. `k` takes 50 values, each
/// in every partition, and is NULL on every seventh row; `a` is NULL on every
/// eleventh. Rows in partition 1 whose id is a multiple of 97 carry `k =
/// 'solo'`, a key no other partition holds.
fn rows(ids: std::ops::Range<i64>, partitions: i64, v: i64) -> Vec<Row> {
    ids.map(|id| {
        let p = id % partitions;
        let k = if id % 7 == 0 {
            None
        } else if p == 1 && id % 97 == 0 {
            Some("solo".to_string())
        } else {
            Some(format!("k{}", id % 50))
        };
        let a = (id % 11 != 0).then_some(id % 13);
        (id, p, k, a, id % 5, v)
    })
    .collect()
}

fn batch(rows: &[Row]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1))),
            Arc::new(StringArray::from_iter(rows.iter().map(|r| r.2.clone()))),
            Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.3))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.4))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.5))),
        ],
    )
    .expect("batch")
}

/// A lookup the indexes can answer, and whether the partition filter in it
/// prunes the scan to a single partition.
struct Lookup {
    sql: &'static str,
    single_partition: bool,
}

/// Lookups on `k` and on `(a, b)`: keys held by every partition, by one
/// partition, and by none; `IN` lists with a `NULL` member and a missing key;
/// a residual predicate beside the key; and a partition filter beside it.
const LOOKUPS: &[Lookup] = &[
    Lookup {
        sql: "SELECT * FROM t WHERE k = 'k7'",
        single_partition: false,
    },
    Lookup {
        sql: "SELECT * FROM t WHERE k = 'solo'",
        single_partition: false,
    },
    Lookup {
        sql: "SELECT * FROM t WHERE k = 'absent'",
        single_partition: false,
    },
    Lookup {
        sql: "SELECT * FROM t WHERE k IN ('k1', 'k2', NULL, 'absent')",
        single_partition: false,
    },
    Lookup {
        sql: "SELECT * FROM t WHERE k = 'k3' AND v > 0",
        single_partition: false,
    },
    Lookup {
        sql: "SELECT * FROM t WHERE a = 3 AND b = 3",
        single_partition: false,
    },
    Lookup {
        sql: "SELECT * FROM t WHERE a IN (1, 2, 12) AND b IN (1, 2)",
        single_partition: false,
    },
    Lookup {
        sql: "SELECT * FROM t WHERE a = 4 AND b = 4 AND p = 1",
        single_partition: true,
    },
];

/// Queries the indexes cannot answer, compared on rows only: `NULL` keys never
/// match an equality, so the index holds no position for them.
const SCANS: &[&str] = &[
    "SELECT * FROM t WHERE k IS NULL",
    "SELECT * FROM t WHERE a IS NULL AND b = 2",
    "SELECT * FROM t",
];

/// One acceleration of `t` under its own directory: indexed on `k` and
/// `(a, b)` when `indexed`, keyed on `id` when `keyed`.
struct Arm {
    dir: tempfile::TempDir,
    indexed: bool,
    keyed: bool,
    ctx: SessionContext,
    table: Arc<dyn TableProvider>,
    sink: Option<ChangeSink>,
}

impl Arm {
    async fn new(indexed: bool, keyed: bool) -> Self {
        let dir = tempfile::tempdir().expect("dir");
        let ctx = SessionContext::new();
        let table = open(dir.path(), indexed, keyed).await;
        ctx.register_table("t", Arc::clone(&table))
            .expect("register");
        let mut arm = Self {
            dir,
            indexed,
            keyed,
            ctx,
            table,
            sink: None,
        };
        arm.bind_sink().await;
        arm
    }

    async fn bind_sink(&mut self) {
        if !self.keyed {
            return;
        }
        let sink = CayenneAccelerator::new()
            .change_sink(
                ChangeSinkContext::new(TableReference::bare("t"), Arc::clone(&self.table)),
                &Handle::current(),
                2,
            )
            .await
            .expect("bind")
            .expect("sink");
        self.sink = Some(sink);
    }

    async fn write(&self, statement: &str, rows: &[Row]) {
        self.ctx.register_batch("src", batch(rows)).expect("source");
        self.ctx
            .sql(statement)
            .await
            .expect("plan write")
            .collect()
            .await
            .expect("write");
        self.ctx.deregister_table("src").expect("deregister source");
    }

    async fn overwrite(&self, rows: &[Row]) {
        self.write("INSERT OVERWRITE t SELECT * FROM src", rows)
            .await;
    }

    async fn append(&self, rows: &[Row]) {
        self.write("INSERT INTO t SELECT * FROM src", rows).await;
    }

    async fn delete(&self, rows: &[Row]) {
        let sink = self.sink.as_ref().expect("a keyed arm has a change sink");
        let receipt = sink
            .reserve()
            .await
            .expect("capacity")
            .submit(
                ChangeBatch::cdc(deletes(rows)),
                WriteOptions {
                    recovery: Recovery::Replayable,
                    ..WriteOptions::default()
                },
            )
            .expect("admission")
            .wait()
            .await
            .expect("receipt");
        receipt.published().await.expect("published");
    }

    /// Reopens `t` from its directory, as a restart does; without its index
    /// runs when `drop_runs`, as a snapshot that did not carry them leaves it.
    async fn reopen(&mut self, drop_runs: bool) {
        if let Some(sink) = self.sink.take() {
            sink.begin_close().wait().await.expect("close");
        }
        self.ctx.deregister_table("t").expect("deregister");
        let table = std::mem::replace(&mut self.table, Arc::new(empty_table()));
        drop(table);
        if drop_runs {
            let runs = run_files(self.dir.path());
            assert!(!runs.is_empty(), "the indexed writes persisted their runs");
            for run in runs {
                std::fs::remove_file(run).expect("drop a run file");
            }
        }
        self.table = open(self.dir.path(), self.indexed, self.keyed).await;
        self.ctx
            .register_table("t", Arc::clone(&self.table))
            .expect("register reopened");
        self.bind_sink().await;
    }

    async fn query(&self, sql: &str) -> Vec<String> {
        let batches = self
            .ctx
            .sql(sql)
            .await
            .unwrap_or_else(|e| panic!("plan {sql}: {e}"))
            .collect()
            .await
            .unwrap_or_else(|e| panic!("run {sql}: {e}"));
        rendered(&batches)
    }

    /// The outermost `CayenneAccelerationExec` line of each partition scan in
    /// `EXPLAIN ANALYZE sql`. A keyed partition's scan nests further
    /// `CayenneAccelerationExec`s inside the one that reports the lookup; those
    /// are skipped.
    async fn scans(&self, sql: &str) -> Vec<String> {
        let plan = self
            .ctx
            .sql(&format!("EXPLAIN ANALYZE {sql}"))
            .await
            .expect("plan explain")
            .collect()
            .await
            .expect("explain");
        let text = arrow::util::pretty::pretty_format_batches(&plan)
            .expect("format plan")
            .to_string();
        let mut scans = Vec::new();
        // The indent of the scan whose subtree the current line is in, if any.
        let mut open: Option<usize> = None;
        for line in text.lines() {
            if open.is_some_and(|scan| indent(line) <= scan) {
                open = None;
            }
            if line.contains("CayenneAccelerationExec") && open.is_none() {
                open = Some(indent(line));
                scans.push(line.to_string());
            }
        }
        scans
    }
}

/// The column at which a plan line's operator name starts.
fn indent(line: &str) -> usize {
    line.trim_start_matches(['|', ' '])
        .len()
        .abs_diff(line.len())
}

fn empty_table() -> datafusion::datasource::MemTable {
    datafusion::datasource::MemTable::try_new(schema(), vec![vec![]]).expect("empty table")
}

/// Creates `t` under `dir`, or reopens it when its metastore already holds it.
async fn open(dir: &Path, indexed: bool, keyed: bool) -> Arc<dyn TableProvider> {
    let declared = if indexed {
        [vec!["k"], vec!["a", "b"]]
            .into_iter()
            .map(|columns| {
                (
                    ColumnReference::new(columns.into_iter().map(str::to_string).collect()),
                    IndexType::Enabled,
                )
            })
            .collect()
    } else {
        std::collections::HashMap::new()
    };
    let source = TestAccelerationSource::new("t").with_acceleration(Acceleration {
        enabled: true,
        engine: Engine::Cayenne,
        mode: Mode::File,
        refresh_mode: keyed.then_some(RefreshMode::Changes),
        primary_key: keyed.then(|| ColumnReference::new(vec!["id".to_string()])),
        partition_by: vec![spicepod::partitioning::PartitionedBy {
            name: "p".to_string(),
            expression: "p".to_string(),
        }],
        indexes: declared,
        params: [
            (
                "cayenne_file_path".to_string(),
                dir.join("data").to_string_lossy().into_owned(),
            ),
            (
                "cayenne_metadata_dir".to_string(),
                dir.join("metadata").to_string_lossy().into_owned(),
            ),
        ]
        .into_iter()
        .collect(),
        ..Default::default()
    });
    let registry = AcceleratorEngineRegistry::new();
    registry
        .register_accelerator_engine(
            Engine::Cayenne,
            Arc::new(CayenneAccelerator::new()) as Arc<dyn DataAccelerator>,
        )
        .await;
    let acceleration = source.acceleration().expect("acceleration").clone();
    registry
        .create_accelerator_table(
            TableReference::bare("t"),
            schema(),
            None,
            &acceleration,
            source.secrets(),
            Some(&source),
            Arc::new(SessionContext::new()),
        )
        .await
        .expect("create or reopen t")
}

/// A CDC batch deleting each of `rows`.
fn deletes(rows: &[Row]) -> cdc::ChangeBatch {
    let count = i32::try_from(rows.len()).expect("row count");
    let keys = ListArray::try_new(
        Arc::new(Field::new("item", DataType::Utf8, false)),
        OffsetBuffer::new((0..=count).collect::<Vec<_>>().into()),
        Arc::new(StringArray::from(vec!["id"; rows.len()])),
        None,
    )
    .expect("primary keys");
    let schema = schema();
    let record = RecordBatch::try_new(
        Arc::new(cdc::changes_schema(&schema)),
        vec![
            Arc::new(StringArray::from(vec!["d"; rows.len()])),
            Arc::new(keys),
            Arc::new(StructArray::from(batch(rows))),
        ],
    )
    .expect("changes");
    cdc::ChangeBatch::try_new(record).expect("CDC batch")
}

/// Every persisted index run file under `dir`.
fn run_files(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .map(|entry| entry.expect("dir entry").path())
        .flat_map(|path| {
            if path.is_dir() {
                run_files(&path)
            } else if path.extension().is_some_and(|ext| ext == "run") {
                vec![path]
            } else {
                Vec::new()
            }
        })
        .collect()
}

/// Every row of `batches` as `cell|cell|…`, NULL spelled out, sorted, so two
/// results compare as multisets.
fn rendered(batches: &[RecordBatch]) -> Vec<String> {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let cells: Vec<String> = (0..batch.num_columns())
                .map(|column| {
                    let array = batch.column(column);
                    if array.is_null(row) {
                        "NULL".to_string()
                    } else {
                        arrow::util::display::array_value_to_string(array, row)
                            .expect("render cell")
                    }
                })
                .collect();
            rows.push(cells.join("|"));
        }
    }
    rows.sort();
    rows
}

/// Whether every one of `scans` was answered from the index alone.
fn served_by_index(scans: &[String]) -> bool {
    scans.iter().all(|scan| {
        scan.contains("lookup_index=")
            && !scan.contains("lookup_index=none")
            && scan.contains("candidate_files=")
            && scan.contains("uncovered_files=0")
    })
}

/// Checks every lookup and scan against the unindexed acceleration, and that
/// the indexed acceleration answers each lookup from the index of every one of
/// its `partitions` partitions. `stage` names the write just made.
async fn check(indexed: &Arm, plain: &Arm, partitions: usize, stage: &str) {
    for lookup in LOOKUPS {
        let expected_scans = if lookup.single_partition {
            1
        } else {
            partitions
        };
        // A lookup asks for any file its index does not cover yet to be
        // indexed in the background, so poll until the plan shows full
        // coverage.
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let scans = indexed.scans(lookup.sql).await;
            if scans.len() == expected_scans && served_by_index(&scans) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "after {stage}, `{}` must scan {expected_scans} partitions, each answered from \
                 its index alone:\n{}",
                lookup.sql,
                scans.join("\n")
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let plain_scans = plain.scans(lookup.sql).await;
        assert!(
            plain_scans.len() == expected_scans
                && plain_scans
                    .iter()
                    .all(|scan| !scan.contains("lookup_index=")),
            "after {stage}, the unindexed acceleration must scan without an index:\n{}",
            plain_scans.join("\n")
        );
        assert_eq!(
            indexed.query(lookup.sql).await,
            plain.query(lookup.sql).await,
            "after {stage}, `{}` differs between the indexed and unindexed accelerations",
            lookup.sql
        );
    }
    for sql in SCANS {
        assert_eq!(
            indexed.query(sql).await,
            plain.query(sql).await,
            "after {stage}, `{sql}` differs between the indexed and unindexed accelerations"
        );
    }
}

/// The first lookups on `arm`, made before any lookup has asked for a
/// background build, are answered from every partition's index alone: the
/// reopened partitions were indexed before the table was returned.
async fn indexed_before_first_lookup(arm: &Arm, partitions: usize, stage: &str) {
    for sql in [
        "SELECT * FROM t WHERE k = 'k7'",
        "SELECT * FROM t WHERE a = 3 AND b = 3",
    ] {
        let scans = arm.scans(sql).await;
        assert!(
            scans.len() == partitions && served_by_index(&scans),
            "after {stage}, the first `{sql}` must be answered from every partition's index \
             alone:\n{}",
            scans.join("\n")
        );
    }
}

/// The rows `sql` returns from `arm`, which must not be empty: a lookup whose
/// answer is empty on both arms compares nothing.
async fn non_empty(arm: &Arm, sql: &str) -> Vec<String> {
    let rows = arm.query(sql).await;
    assert!(!rows.is_empty(), "`{sql}` must find rows");
    rows
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partitioned_indexes_answer_lookups_like_an_unindexed_table_through_appends_and_overwrites()
{
    let mut indexed = Arm::new(true, false).await;
    let mut plain = Arm::new(false, false).await;

    let initial = rows(0..30_000, 3, 0);
    indexed.overwrite(&initial).await;
    plain.overwrite(&initial).await;
    check(&indexed, &plain, 3, "an overwrite").await;
    // The fixture puts `k7` in every partition and `solo` in one; a `NULL`
    // never matches.
    assert_eq!(
        non_empty(
            &indexed,
            "SELECT DISTINCT p FROM t WHERE k = 'k7' ORDER BY p"
        )
        .await,
        ["0", "1", "2"]
    );
    assert_eq!(
        non_empty(&indexed, "SELECT DISTINCT p FROM t WHERE k = 'solo'").await,
        ["1"]
    );
    non_empty(&indexed, "SELECT * FROM t WHERE a = 3 AND b = 3").await;
    non_empty(&indexed, "SELECT * FROM t WHERE k IS NULL").await;

    let appended = rows(30_000..45_000, 3, 1);
    indexed.append(&appended).await;
    plain.append(&appended).await;
    check(&indexed, &plain, 3, "an append into existing partitions").await;

    // Partition 3 does not exist until this write creates it.
    let new_partition: Vec<Row> = rows(45_000..57_000, 4, 2)
        .into_iter()
        .filter(|row| row.1 == 3)
        .collect();
    indexed.append(&new_partition).await;
    plain.append(&new_partition).await;
    check(&indexed, &plain, 4, "an append that creates a partition").await;
    assert_eq!(
        non_empty(
            &indexed,
            "SELECT DISTINCT p FROM t WHERE k = 'k7' ORDER BY p"
        )
        .await,
        ["0", "1", "2", "3"]
    );

    let replaced = rows(100_000..130_000, 4, 3);
    indexed.overwrite(&replaced).await;
    plain.overwrite(&replaced).await;
    check(&indexed, &plain, 4, "a second overwrite").await;
    assert!(
        indexed
            .query("SELECT * FROM t WHERE id < 100000")
            .await
            .is_empty(),
        "the overwrite replaced every earlier row"
    );

    indexed.reopen(false).await;
    plain.reopen(false).await;
    check(&indexed, &plain, 4, "a restart").await;

    indexed.reopen(true).await;
    indexed_before_first_lookup(&indexed, 4, "a restart without the index runs").await;
    check(&indexed, &plain, 4, "a restart without the index runs").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partitioned_indexes_answer_lookups_like_an_unindexed_table_through_upserts_and_deletes() {
    let mut indexed = Arm::new(true, true).await;
    let mut plain = Arm::new(false, true).await;

    let initial = rows(0..30_000, 3, 0);
    indexed.append(&initial).await;
    plain.append(&initial).await;
    check(&indexed, &plain, 3, "a keyed load").await;

    // Upsert every tenth stored key to new index keys in the same partition:
    // its row must stop matching its old `k` and `(a, b)` and match the new.
    let moved: Vec<Row> = initial
        .iter()
        .filter(|row| row.0 % 10 == 1)
        .map(|row| (row.0, row.1, Some("k7".to_string()), Some(3), 3, 9))
        .collect();
    indexed.append(&moved).await;
    plain.append(&moved).await;
    check(&indexed, &plain, 3, "upserts that change index keys").await;
    assert!(
        indexed
            .query("SELECT * FROM t WHERE k = 'k1' AND id % 10 = 1")
            .await
            .is_empty(),
        "an upserted row no longer matches its old key"
    );
    assert!(
        non_empty(&indexed, "SELECT id FROM t WHERE k = 'k7' AND v = 9")
            .await
            .contains(&"11".to_string())
    );

    // Delete every row of `k3` and every row with `a = 4` from partition 2.
    let deleted: Vec<Row> = initial
        .iter()
        .filter(|row| row.0 % 10 != 1)
        .filter(|row| row.2.as_deref() == Some("k3") || (row.1 == 2 && row.3 == Some(4)))
        .cloned()
        .collect();
    indexed.delete(&deleted).await;
    plain.delete(&deleted).await;
    check(&indexed, &plain, 3, "CDC deletes").await;
    assert!(
        indexed
            .query("SELECT * FROM t WHERE k = 'k3'")
            .await
            .is_empty(),
        "every row of k3 was deleted"
    );

    indexed.reopen(false).await;
    plain.reopen(false).await;
    check(&indexed, &plain, 3, "a keyed restart").await;

    indexed.reopen(true).await;
    indexed_before_first_lookup(&indexed, 3, "a keyed restart without the index runs").await;
    check(
        &indexed,
        &plain,
        3,
        "a keyed restart without the index runs",
    )
    .await;
}
