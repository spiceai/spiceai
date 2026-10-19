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

//! A catalog durable in object storage (`mode: datalake`) survives the loss of
//! its local replica. The oracle is the catalog itself: a replica rebuilt from
//! object storage must hold exactly the rows, rowids included, of the replica
//! that wrote them, and serve the same query results.

#![expect(clippy::expect_used, reason = "test failures carry their context")]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use cayenne::metadata::CreateTableOptions;
use cayenne::metastore::durable::CatalogLocation;
use cayenne::{CatalogError, CayenneCatalog, CayenneTableProvider, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};
use futures::TryStreamExt;
use object_store::ObjectStore;
use object_store_occ::LocalConditionalPut;
use tempfile::TempDir;

const TABLE: &str = "durable_events";

struct Harness {
    dir: TempDir,
    store: Arc<dyn ObjectStore>,
}

impl Harness {
    fn new() -> Self {
        let dir = TempDir::new().expect("temp dir");
        let bucket = dir.path().join("bucket");
        std::fs::create_dir_all(&bucket).expect("bucket dir");
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalConditionalPut::new(bucket).expect("local conditional store"));
        Self { dir, store }
    }

    fn location(&self) -> CatalogLocation {
        CatalogLocation::new(
            Arc::clone(&self.store),
            object_store::path::Path::from("tables/durable_events/catalog"),
            "file:///bucket/tables/durable_events/catalog",
        )
    }

    fn replica(&self, name: &str) -> PathBuf {
        self.dir.path().join(name).join("cayenne.db")
    }

    fn data_path(&self) -> String {
        self.dir.path().join("data").to_string_lossy().to_string()
    }

    async fn catalog(&self, replica: &str) -> Arc<CayenneCatalog> {
        let catalog = Arc::new(
            CayenneCatalog::new_durable(
                format!("sqlite://{}", self.replica(replica).display()),
                self.location(),
            )
            .expect("durable catalog"),
        );
        catalog.init().await.expect("init durable catalog");
        catalog
    }

    async fn object_keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self
            .store
            .list(None)
            .map_ok(|meta| meta.location.to_string())
            .try_collect()
            .await
            .expect("list bucket");
        keys.sort();
        keys
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

async fn open_table(
    harness: &Harness,
    catalog: &Arc<CayenneCatalog>,
) -> (Arc<CayenneTableProvider>, SessionContext) {
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(catalog) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: TABLE.to_string(),
                schema: schema(),
                primary_key: vec!["id".to_string()],
                on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                    "id".to_string(),
                ]))),
                base_path: harness.data_path(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            },
            ctx.runtime_env(),
        )
        .await
        .expect("create table"),
    );
    ctx.register_table(TABLE, Arc::clone(&table) as Arc<dyn TableProvider>)
        .expect("register table");
    (table, ctx)
}

fn batch(ids: impl IntoIterator<Item = i64>, tag: &str) -> RecordBatch {
    let ids: Vec<i64> = ids.into_iter().collect();
    let names: Vec<String> = ids.iter().map(|id| format!("{tag}-{id}")).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .expect("batch")
}

async fn insert(table: &CayenneTableProvider, batch: RecordBatch) {
    crate::common::insert_batch(table, batch)
        .await
        .expect("insert");
}

async fn rows(ctx: &SessionContext) -> Vec<(i64, String)> {
    let batches = ctx
        .sql(&format!("SELECT id, name FROM {TABLE} ORDER BY id"))
        .await
        .expect("query")
        .collect()
        .await
        .expect("collect");
    let mut out = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ids");
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("names");
        for i in 0..batch.num_rows() {
            out.push((ids.value(i), names.value(i).to_string()));
        }
    }
    out
}

/// Every row of every catalog table in the file at `db`, keyed by table, with
/// the rowid of rowid tables, in a canonical order. The position table is
/// local bookkeeping and differs by design.
fn dump(db: &Path) -> BTreeMap<String, Vec<String>> {
    let conn =
        rusqlite::Connection::open_with_flags(db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("open replica");
    let tables: Vec<(String, bool)> = conn
        .prepare(
            "SELECT name, wr FROM pragma_table_list WHERE schema = 'main' AND type = 'table' AND name LIKE 'cayenne_%' AND name <> 'cayenne_durable_position'",
        )
        .and_then(|mut stmt| {
            stmt.query_map([], |row| Ok((row.get(0)?, row.get::<_, i64>(1)? != 0)))?
                .collect()
        })
        .expect("list tables");
    let mut out = BTreeMap::new();
    for (table, without_rowid) in tables {
        let sql = if without_rowid {
            format!("SELECT * FROM \"{table}\"")
        } else {
            format!("SELECT rowid, * FROM \"{table}\"")
        };
        let mut stmt = conn.prepare(&sql).expect("prepare dump");
        let width = stmt.column_count();
        let mut rows: Vec<String> = stmt
            .query_map([], |row| {
                (0..width)
                    .map(|i| row.get::<_, rusqlite::types::Value>(i))
                    .collect::<Result<Vec<_>, _>>()
                    .map(|values| format!("{values:?}"))
            })
            .and_then(Iterator::collect)
            .expect("dump rows");
        rows.sort();
        out.insert(table, rows);
    }
    out
}

/// Every catalog row, rowids included, must be restored; report the first
/// table that differs rather than the whole catalog.
fn assert_same_catalog(
    restored: &BTreeMap<String, Vec<String>>,
    expected: &BTreeMap<String, Vec<String>>,
) {
    assert_eq!(
        restored.keys().collect::<Vec<_>>(),
        expected.keys().collect::<Vec<_>>(),
        "restored catalog tables"
    );
    for (table, rows) in expected {
        let got = &restored[table];
        let missing: Vec<&String> = rows.iter().filter(|r| !got.contains(r)).take(3).collect();
        let extra: Vec<&String> = got.iter().filter(|r| !rows.contains(r)).take(3).collect();
        assert!(
            got == rows,
            "table {table}: restored {} rows, expected {}; missing {missing:?}; unexpected {extra:?}",
            got.len(),
            rows.len()
        );
    }
}

/// A history long enough to rotate several times, with inline appends,
/// upserts of existing keys, deletes, a transaction too large to inline, and a
/// checkpoint of the inline rows into data files.
async fn write_history(table: &CayenneTableProvider, ctx: &SessionContext, writes: i64) {
    for i in 0..writes {
        insert(table, batch([i * 3, i * 3 + 1], "v1")).await;
        if i % 5 == 0 {
            // Upsert keys written earlier.
            insert(table, batch([i, i / 2], "v2")).await;
        }
        if i % 7 == 3 {
            ctx.sql(&format!("DELETE FROM {TABLE} WHERE id = {}", i * 3))
                .await
                .expect("delete plan")
                .collect()
                .await
                .expect("delete");
        }
        if i == writes / 2 {
            // Large enough that its transaction is uploaded as its own object.
            insert(table, batch(100_000..104_000, "bulk")).await;
            table
                .checkpoint_inlined_data()
                .await
                .expect("checkpoint inline rows into data files");
        }
    }
}

#[tokio::test]
async fn replica_rebuilt_from_object_storage_matches_the_original() {
    let harness = Harness::new();
    let original = harness.catalog("node-a").await;
    let (table, ctx) = open_table(&harness, &original).await;
    write_history(&table, &ctx, 150).await;
    // The table's background maintenance commits too; let it finish so the
    // original writer is quiet before another node takes the table over.
    table
        .flush_pending_maintenance()
        .await
        .expect("flush maintenance");
    original.shutdown().await.expect("shutdown original");
    let expected_rows = rows(&ctx).await;
    assert!(
        expected_rows.len() > 4_000,
        "the history must leave rows to compare, got {}",
        expected_rows.len()
    );
    let expected = dump(&harness.replica("node-a"));
    assert!(
        expected.values().any(|rows| !rows.is_empty()),
        "the catalog must hold rows to compare"
    );
    drop((table, ctx));
    drop(original);

    // A new node with an empty disk: the catalog comes back from object storage.
    let restored = harness.catalog("node-b").await;
    assert_same_catalog(&dump(&harness.replica("node-b")), &expected);
    let (_table, ctx) = open_table(&harness, &restored).await;
    assert_eq!(rows(&ctx).await, expected_rows);
}

#[tokio::test]
async fn replica_behind_object_storage_catches_up() {
    let harness = Harness::new();
    let catalog = harness.catalog("node-a").await;
    let (table, ctx) = open_table(&harness, &catalog).await;
    write_history(&table, &ctx, 20).await;
    // Keep a copy of the replica as it was, then keep writing.
    table
        .flush_pending_maintenance()
        .await
        .expect("flush maintenance");
    catalog.shutdown().await.expect("checkpoint replica");
    let stale = harness.dir.path().join("stale");
    std::fs::create_dir_all(&stale).expect("stale dir");
    for suffix in ["", "-wal", "-shm"] {
        let from = PathBuf::from(format!("{}{suffix}", harness.replica("node-a").display()));
        if from.exists() {
            std::fs::copy(&from, stale.join(format!("cayenne.db{suffix}"))).expect("copy replica");
        }
    }
    for i in 1_000..1_010 {
        insert(&table, batch([i], "late")).await;
    }
    table
        .flush_pending_maintenance()
        .await
        .expect("flush maintenance");
    catalog.shutdown().await.expect("shutdown");
    let expected_rows = rows(&ctx).await;
    let expected = dump(&harness.replica("node-a"));
    drop((table, ctx));
    drop(catalog);

    // The stale copy is a usable prefix: recovery applies only what follows.
    let node_b = harness.dir.path().join("node-b");
    std::fs::create_dir_all(&node_b).expect("node-b dir");
    for entry in std::fs::read_dir(&stale).expect("stale files") {
        let entry = entry.expect("entry");
        std::fs::copy(entry.path(), node_b.join(entry.file_name())).expect("seed node-b");
    }
    let restored = harness.catalog("node-b").await;
    assert_same_catalog(&dump(&harness.replica("node-b")), &expected);
    let (_table, ctx) = open_table(&harness, &restored).await;
    assert_eq!(rows(&ctx).await, expected_rows);
}

#[tokio::test]
async fn a_second_writer_fences_the_first() {
    let harness = Harness::new();
    let first = harness.catalog("node-a").await;
    let (table, ctx) = open_table(&harness, &first).await;
    insert(&table, batch([1, 2], "a")).await;
    table
        .flush_pending_maintenance()
        .await
        .expect("flush maintenance");

    // Opening the catalog elsewhere rotates it, which seals the epoch the
    // first writer commits to.
    let _second = harness.catalog("node-b").await;

    let error = crate::common::insert_batch(&table, batch([3], "a"))
        .await
        .expect_err("a fenced writer must not commit");
    assert!(
        error
            .to_string()
            .contains("Only one Spice instance may write"),
        "unexpected error: {error}"
    );
    // The refused write never became visible locally either.
    assert_eq!(
        rows(&ctx).await,
        vec![(1, "a-1".to_string()), (2, "a-2".to_string())]
    );
}

#[tokio::test]
async fn rotation_keeps_object_storage_bounded() {
    let harness = Harness::new();
    let catalog = harness.catalog("node-a").await;
    let (table, _ctx) = open_table(&harness, &catalog).await;
    for i in 0..400 {
        insert(&table, batch([i], "x")).await;
    }
    // Let a rotation started by the last commits finish.
    let mut keys = harness.object_keys().await;
    for _ in 0..100 {
        let epochs: std::collections::BTreeSet<&str> = keys
            .iter()
            .filter_map(|k| k.split("/epochs/").nth(1)?.split('/').next())
            .collect();
        if epochs.len() <= 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        keys = harness.object_keys().await;
    }
    let epochs: std::collections::BTreeSet<&str> = keys
        .iter()
        .filter_map(|k| k.split("/epochs/").nth(1)?.split('/').next())
        .collect();
    let images = keys.iter().filter(|k| k.contains("/images/")).count();
    assert!(
        epochs.len() <= 2,
        "at most the current epoch and the one holding its tail remain, found {epochs:?}"
    );
    assert!(images <= 2, "at most two images remain, found {images}");
}

#[tokio::test]
async fn a_memory_replica_is_refused() {
    let harness = Harness::new();
    let error = CayenneCatalog::new_durable(
        "sqlite://file:/cayenne-mem-durable?vfs=memdb",
        harness.location(),
    )
    .err()
    .expect("a memory replica cannot back a durable catalog");
    assert!(
        matches!(error, CatalogError::InvalidOperationNoSource { .. }),
        "{error}"
    );
}

fn percentile(sorted: &[std::time::Duration], p: f64) -> std::time::Duration {
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss,
        reason = "an index into a short sample"
    )]
    let index = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[index]
}

async fn time_inserts(table: &CayenneTableProvider, count: i64) -> Vec<std::time::Duration> {
    let mut samples = Vec::with_capacity(usize::try_from(count).unwrap_or(0));
    for i in 0..count {
        let started = std::time::Instant::now();
        insert(table, batch([i], "lat")).await;
        samples.push(started.elapsed());
    }
    samples.sort();
    samples
}

fn report(label: &str, samples: &[std::time::Duration]) {
    eprintln!(
        "{label}: n={} p50={:?} p99={:?} p99.9={:?} max={:?}",
        samples.len(),
        percentile(samples, 0.50),
        percentile(samples, 0.99),
        percentile(samples, 0.999),
        samples[samples.len() - 1]
    );
}

/// Single-row insert latency, the shape of an OTLP sink's exports: a plain
/// local catalog against a catalog durable in object storage. Prints the tail;
/// set `CAYENNE_DATALAKE_TEST_*` (see `accelerator-cayenne`'s datalake test)
/// to add an S3-compatible store. Run with `--ignored --nocapture`.
#[tokio::test]
#[ignore = "latency measurement; run explicitly with --ignored --nocapture"]
async fn insert_latency_percentiles() {
    const COUNT: i64 = 2_000;
    for round in 0..2 {
        // Plain `mode: file` catalog.
        let harness = Harness::new();
        let plain = Arc::new(
            CayenneCatalog::new(format!("sqlite://{}", harness.replica("plain").display()))
                .expect("plain catalog"),
        );
        plain.init().await.expect("init plain");
        let (table, _ctx) = open_table(&harness, &plain).await;
        report(
            &format!("round {round} file"),
            &time_inserts(&table, COUNT).await,
        );

        // Durable catalog on a local conditional-write store.
        let harness = Harness::new();
        let durable = harness.catalog("durable").await;
        let (table, _ctx) = open_table(&harness, &durable).await;
        report(
            &format!("round {round} datalake(local)"),
            &time_inserts(&table, COUNT).await,
        );

        let env = |name: &str| std::env::var(format!("CAYENNE_DATALAKE_TEST_{name}")).ok();
        if let (Some(endpoint), Some(bucket), Some(key), Some(secret)) =
            (env("ENDPOINT"), env("BUCKET"), env("KEY"), env("SECRET"))
        {
            let store: Arc<dyn ObjectStore> = Arc::new(
                object_store::aws::AmazonS3Builder::new()
                    .with_endpoint(endpoint)
                    .with_bucket_name(bucket)
                    .with_access_key_id(key)
                    .with_secret_access_key(secret)
                    .with_region("us-east-1")
                    .with_allow_http(true)
                    .build()
                    .expect("s3 store"),
            );
            let harness = Harness::new();
            let prefix = format!("latency-{}", uuid::Uuid::now_v7());
            let catalog = Arc::new(
                CayenneCatalog::new_durable(
                    format!("sqlite://{}", harness.replica("s3").display()),
                    CatalogLocation::new(
                        store,
                        object_store::path::Path::from(format!("{prefix}/catalog")),
                        format!("s3://{prefix}/catalog"),
                    ),
                )
                .expect("s3 durable catalog"),
            );
            catalog.init().await.expect("init s3 durable catalog");
            let (table, _ctx) = open_table(&harness, &catalog).await;
            report(
                &format!("round {round} datalake(s3)"),
                &time_inserts(&table, COUNT).await,
            );
        }
    }
}
