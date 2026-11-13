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

//! Each Cayenne dataset reaches its data files through its own object store,
//! registered in the session's `RuntimeEnv` under a URL of its own,
//! `cayenne://<table id>/`. `DataFusion` keys stores by `scheme://host`, so a
//! dataset's store never shares a key with another dataset in the same bucket, with
//! a source that registered a store for that bucket, or with the shared `file://`
//! store.

use std::fmt::{self, Display, Formatter};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::common;
use arrow::array::{Array, Int64Array};
use async_trait::async_trait;
use bytes::Bytes;
use cayenne::metadata::{CreateTableOptions, ObjectStoreConfig, VortexConfig};
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::TableProvider;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    PutMultipartOptions, PutOptions, PutResult, RenameOptions,
};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use vortex::VortexSessionDefault;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn vortex_config() -> VortexConfig {
    // Every write becomes data files, not rows inlined into the metastore.
    VortexConfig {
        inline_max_rows: 0,
        inline_max_bytes: 0,
        ..VortexConfig::default()
    }
}

async fn count_and_sum(ctx: &SessionContext, table: &str) -> TestResult<(i64, i64)> {
    let batches = ctx
        .sql(&format!(
            "SELECT count(*), coalesce(sum(v), 0) FROM {table}"
        ))
        .await?
        .collect()
        .await?;
    let batch = batches.first().ok_or("no result batch")?;
    let column = |i: usize| -> TestResult<i64> {
        Ok(batch
            .column(i)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("not an Int64 column")?
            .value(0))
    };
    Ok((column(0)?, column(1)?))
}

/// Sum of `v = id * 10` for `id` in `first..=last`.
fn expected_sum(first: i64, last: i64) -> i64 {
    (first..=last).map(|id| id * 10).sum()
}

async fn keys(store: &Arc<dyn ObjectStore>) -> TestResult<Vec<String>> {
    Ok(store
        .list(None)
        .map_ok(|meta| meta.location.to_string())
        .try_collect()
        .await?)
}

fn dataset_url(table: &CayenneTableProvider) -> TestResult<ObjectStoreUrl> {
    Ok(ObjectStoreUrl::parse(format!(
        "cayenne://{}/",
        table.table_id()
    ))?)
}

/// `DataFusion` and Vortex take a store under an arbitrary scheme and host: a
/// `ListingTable` on `cayenne://ds1/…` writes Vortex files through a sink into the
/// store registered for `cayenne://ds1/` and scans them back from it.
#[tokio::test]
async fn a_vortex_listing_table_writes_and_scans_through_a_per_dataset_url() -> TestResult {
    let ctx = SessionContext::new();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ctx.runtime_env()
        .register_object_store(&url::Url::parse("cayenne://ds1/")?, Arc::clone(&store));
    let shared_file_store = ctx
        .runtime_env()
        .object_store(ObjectStoreUrl::local_filesystem())?;

    let format = Arc::new(vortex_datafusion::VortexFormat::new(
        vortex_session::VortexSession::default(),
    ));
    let config = ListingTableConfig::new(ListingTableUrl::parse("cayenne://ds1/t/snap/")?)
        .with_listing_options(ListingOptions::new(format).with_file_extension(".vortex"))
        .with_schema(schema());
    ctx.register_table("t", Arc::new(ListingTable::try_new(config)?))?;

    ctx.sql("INSERT INTO t SELECT value AS id, value * 10 AS v FROM generate_series(1, 1000)")
        .await?
        .collect()
        .await?;
    ctx.sql("INSERT INTO t SELECT value AS id, value * 10 AS v FROM generate_series(1001, 1500)")
        .await?
        .collect()
        .await?;
    assert_eq!(
        count_and_sum(&ctx, "t").await?,
        (1500, expected_sum(1, 1500))
    );

    let written = keys(&store).await?;
    assert!(
        written.len() >= 2
            && written
                .iter()
                .all(|key| key.starts_with("t/snap/") && key.ends_with(".vortex")),
        "every file the sink wrote is a Vortex file under the listing URL's path: {written:?}"
    );
    assert!(
        Arc::ptr_eq(
            &ctx.runtime_env()
                .object_store(ObjectStoreUrl::local_filesystem())?,
            &shared_file_store
        ),
        "the shared file:// store is untouched"
    );
    Ok(())
}

/// A source's store for a bucket and two Cayenne datasets in that bucket, each
/// with a store of its own (as with different credentials): every dataset writes
/// and reads only its own store whichever registered last, and the bucket's entry
/// in the `RuntimeEnv` stays the source's.
#[tokio::test]
async fn datasets_in_a_shared_bucket_reach_only_their_own_store() -> TestResult {
    let fixture = common::TestFixture::new(common::BackendType::ObjectNative).await?;
    let catalog: Arc<dyn MetadataCatalog> = Arc::clone(&fixture.catalog);
    let ctx = SessionContext::new();
    let bucket = url::Url::parse("s3://bucket/")?;

    // A federated source registered its store for the bucket first.
    let source: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    source
        .put(
            &Path::from("src/facts.csv"),
            PutPayload::from_static(b"id\n1\n"),
        )
        .await?;
    ctx.runtime_env()
        .register_object_store(&bucket, Arc::clone(&source));

    let store_a: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store_b: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut tables = Vec::new();
    for (name, store) in [("orders", &store_a), ("events", &store_b)] {
        let data_url = format!("s3://bucket/state/cayenne/v1/datasets/{name}/data");
        let table = Arc::new(
            crate::common::table_builder_on_store(
                Arc::clone(&catalog),
                ctx.runtime_env(),
                ObjectStoreConfig {
                    url: url::Url::parse(&data_url)?,
                    store: Arc::clone(store),
                },
            )
            .create(CreateTableOptions {
                table_name: name.to_string(),
                schema: schema(),
                primary_key: vec![],
                on_conflict: None,
                base_path: data_url,
                partition_column: None,
                vortex_config: vortex_config(),
            })
            .await?,
        );
        ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)?;
        tables.push(table);
    }

    ctx.sql("INSERT INTO orders SELECT value AS id, value * 10 AS v FROM generate_series(1, 1000)")
        .await?
        .collect()
        .await?;
    ctx.sql("INSERT INTO events SELECT value AS id, value * 10 AS v FROM generate_series(1, 300)")
        .await?
        .collect()
        .await?;

    // The source registers its store for the bucket again, after the datasets.
    ctx.runtime_env()
        .register_object_store(&bucket, Arc::clone(&source));

    ctx.sql(
        "INSERT INTO orders SELECT value AS id, value * 10 AS v FROM generate_series(1001, 2000)",
    )
    .await?
    .collect()
    .await?;
    ctx.sql(
        "INSERT INTO events SELECT value AS id, value * 10 AS v FROM generate_series(301, 600)",
    )
    .await?
    .collect()
    .await?;
    assert_eq!(
        count_and_sum(&ctx, "orders").await?,
        (2000, expected_sum(1, 2000))
    );
    assert_eq!(
        count_and_sum(&ctx, "events").await?,
        (600, expected_sum(1, 600))
    );

    let in_a = keys(&store_a).await?;
    let in_b = keys(&store_b).await?;
    assert!(
        in_a.iter().any(|key| key.ends_with(".vortex"))
            && in_a
                .iter()
                .all(|key| key.starts_with("state/cayenne/v1/datasets/orders/data/")),
        "the orders store holds only orders' data: {in_a:?}"
    );
    assert!(
        in_b.iter().any(|key| key.ends_with(".vortex"))
            && in_b
                .iter()
                .all(|key| key.starts_with("state/cayenne/v1/datasets/events/data/")),
        "the events store holds only events' data: {in_b:?}"
    );
    assert_eq!(
        keys(&source).await?,
        vec!["src/facts.csv".to_string()],
        "the source's store holds only the source's object"
    );
    assert!(
        Arc::ptr_eq(
            &ctx.runtime_env()
                .object_store(ObjectStoreUrl::parse("s3://bucket")?)?,
            &source
        ),
        "the bucket's store in the RuntimeEnv is still the source's"
    );
    for table in &tables {
        ctx.runtime_env().object_store(dataset_url(table)?)?;
    }
    Ok(())
}

/// The store registered for a dataset refuses a path outside that dataset's
/// data path, so a request can never reach another dataset's objects through it.
#[tokio::test]
async fn a_dataset_store_refuses_a_path_outside_the_dataset() -> TestResult {
    let fixture = common::TestFixture::new(common::BackendType::ObjectNative).await?;
    let catalog: Arc<dyn MetadataCatalog> = Arc::clone(&fixture.catalog);
    let ctx = SessionContext::new();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    store
        .put(
            &Path::from("state/cayenne/v1/datasets/other/data/x.vortex"),
            PutPayload::from_static(b"x"),
        )
        .await?;
    let data_url = "s3://bucket/state/cayenne/v1/datasets/mine/data".to_string();
    let table = crate::common::table_builder_on_store(
        catalog,
        ctx.runtime_env(),
        ObjectStoreConfig {
            url: url::Url::parse(&data_url)?,
            store,
        },
    )
    .create(CreateTableOptions {
        table_name: "mine".to_string(),
        schema: schema(),
        primary_key: vec![],
        on_conflict: None,
        base_path: data_url,
        partition_column: None,
        vortex_config: vortex_config(),
    })
    .await?;

    let registered = ctx.runtime_env().object_store(dataset_url(&table)?)?;
    let error = registered
        .get(&Path::from("state/cayenne/v1/datasets/other/data/x.vortex"))
        .await
        .err()
        .ok_or("a read outside the dataset's data path succeeded")?;
    assert_eq!(
        error.to_string(),
        "Generic cayenne error: No Cayenne dataset's storage contains 's3://bucket/state/cayenne/v1/datasets/other/data/x.vortex', so it was not accessed. A dataset reads and writes only under its own data path."
    );
    Ok(())
}

/// A table whose data directory is addressed as `file://` reads and writes through
/// its own durable local store and leaves the session's shared `file://` store as
/// it was.
#[tokio::test]
async fn a_local_dataset_store_leaves_the_shared_file_store_alone() -> TestResult {
    let fixture = common::TestFixture::new(common::BackendType::ObjectNative).await?;
    let catalog: Arc<dyn MetadataCatalog> = Arc::clone(&fixture.catalog);
    let ctx = SessionContext::new();
    let shared_file_store = ctx
        .runtime_env()
        .object_store(ObjectStoreUrl::local_filesystem())?;

    // The dataset's data directory in a local state location.
    let dir = fixture
        .data_path
        .join("state/cayenne/v1/datasets/local_orders/data");
    std::fs::create_dir_all(&dir)?;
    let dir = std::fs::canonicalize(&dir)?;
    let dir_url = url::Url::from_directory_path(&dir).map_err(|()| "directory URL")?;
    let table = Arc::new(
        crate::common::table_builder_on_store(
            catalog,
            ctx.runtime_env(),
            ObjectStoreConfig {
                url: url::Url::parse("file:///")?,
                store: Arc::new(object_store::local::LocalFileSystem::new()),
            },
        )
        .create(CreateTableOptions {
            table_name: "local_orders".to_string(),
            schema: schema(),
            primary_key: vec![],
            on_conflict: None,
            base_path: dir_url.to_string(),
            partition_column: None,
            vortex_config: vortex_config(),
        })
        .await?,
    );
    ctx.register_table("local_orders", Arc::clone(&table) as Arc<dyn TableProvider>)?;
    ctx.sql(
        "INSERT INTO local_orders SELECT value AS id, value * 10 AS v FROM generate_series(1, 700)",
    )
    .await?
    .collect()
    .await?;
    assert_eq!(
        count_and_sum(&ctx, "local_orders").await?,
        (700, expected_sum(1, 700))
    );

    assert!(
        Arc::ptr_eq(
            &ctx.runtime_env()
                .object_store(ObjectStoreUrl::local_filesystem())?,
            &shared_file_store
        ),
        "the shared file:// store is untouched"
    );
    ctx.runtime_env().object_store(dataset_url(&table)?)?;
    let on_disk = walkdir_vortex(&dir)?;
    assert!(
        !on_disk.is_empty(),
        "the data files are on disk under {}",
        dir.display()
    );
    Ok(())
}

fn walkdir_vortex(dir: &std::path::Path) -> TestResult<Vec<std::path::PathBuf>> {
    let mut found = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(next) = pending.pop() {
        for entry in std::fs::read_dir(&next)? {
            let path = entry?.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|ext| ext == "vortex") {
                found.push(path);
            }
        }
    }
    Ok(found)
}

/// A table whose data path is a `file://` URI but that is given no store (the
/// `cayenne_vnext` flag off) keeps the local data path, as it always has: it opens
/// and serves its rows rather than failing as if it were in an object store.
#[tokio::test]
async fn a_file_uri_table_without_a_store_keeps_the_local_data_path() -> TestResult {
    // The local data path resolves a `file://` table path relative to the working
    // directory, so the test runs from a directory of its own.
    common::require_process_per_test("this test changes the working directory");
    let fixture = common::TestFixture::new(common::BackendType::Sqlite).await?;
    std::env::set_current_dir(fixture.temp_dir.path())?;
    let catalog: Arc<dyn MetadataCatalog> = Arc::clone(&fixture.catalog);
    let ctx = SessionContext::new();
    let dir = std::fs::canonicalize(&fixture.data_path)?;
    let dir_url = url::Url::from_directory_path(&dir).map_err(|()| "directory URL")?;
    let table = Arc::new(
        CayenneTableProviderBuilder::new(Arc::clone(&catalog), ctx.runtime_env())
            .create(CreateTableOptions {
                table_name: "region".to_string(),
                schema: schema(),
                primary_key: vec![],
                on_conflict: None,
                base_path: dir_url.to_string(),
                partition_column: None,
                vortex_config: VortexConfig::default(),
            })
            .await?,
    );
    ctx.register_table("region", Arc::clone(&table) as Arc<dyn TableProvider>)?;
    ctx.sql("INSERT INTO region SELECT value AS id, value * 10 AS v FROM generate_series(0, 4)")
        .await?
        .collect()
        .await?;
    assert_eq!(
        count_and_sum(&ctx, "region").await?,
        (5, expected_sum(0, 4))
    );

    // A reopen serves the same rows.
    drop(table);
    let ctx = SessionContext::new();
    let table = CayenneTableProviderBuilder::new(catalog, ctx.runtime_env())
        .open("region")
        .await?;
    ctx.register_table("region", Arc::new(table) as Arc<dyn TableProvider>)?;
    assert_eq!(
        count_and_sum(&ctx, "region").await?,
        (5, expected_sum(0, 4))
    );
    Ok(())
}

/// An in-memory store that counts reads of data files (`.vortex`).
#[derive(Debug, Default)]
struct DataReadCountingStore {
    inner: InMemory,
    data_reads: AtomicUsize,
}

impl DataReadCountingStore {
    fn note_read(&self, location: &Path) {
        if location.as_ref().ends_with(".vortex") {
            self.data_reads.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn take_data_reads(&self) -> usize {
        self.data_reads.swap(0, Ordering::SeqCst)
    }
}

impl Display for DataReadCountingStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "DataReadCountingStore")
    }
}

#[async_trait]
impl ObjectStore for DataReadCountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.note_read(location);
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.note_read(location);
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

/// A scan reads the table's data files through the dataset's store: a reopened
/// table in a new session, with no cache of its own yet, sends GET or ranged-GET
/// requests for its `.vortex` files to the store it was given, and answers from
/// what they return.
#[tokio::test]
async fn a_scan_reads_its_data_files_through_the_dataset_store() -> TestResult {
    let fixture = common::TestFixture::new(common::BackendType::ObjectNative).await?;
    let catalog: Arc<dyn MetadataCatalog> = Arc::clone(&fixture.catalog);
    let counting = Arc::new(DataReadCountingStore::default());
    let store: Arc<dyn ObjectStore> = Arc::clone(&counting) as Arc<dyn ObjectStore>;
    let data_url = "s3://bucket/state/cayenne/v1/datasets/scanned/data".to_string();
    let config = ObjectStoreConfig {
        url: url::Url::parse(&data_url)?,
        store,
    };

    let ctx = SessionContext::new();
    let table = Arc::new(
        crate::common::table_builder_on_store(
            Arc::clone(&catalog),
            ctx.runtime_env(),
            config.clone(),
        )
        .create(CreateTableOptions {
            table_name: "scanned".to_string(),
            schema: schema(),
            primary_key: vec![],
            on_conflict: None,
            base_path: data_url,
            partition_column: None,
            vortex_config: vortex_config(),
        })
        .await?,
    );
    ctx.register_table("scanned", Arc::clone(&table) as Arc<dyn TableProvider>)?;
    for (first, last) in [(1, 1000), (1001, 2500)] {
        ctx.sql(&format!(
            "INSERT INTO scanned SELECT value AS id, value * 10 AS v FROM generate_series({first}, {last})"
        ))
        .await?
        .collect()
        .await?;
    }
    drop(ctx);
    drop(table);

    let ctx = SessionContext::new();
    let table = crate::common::table_builder_on_store(catalog, ctx.runtime_env(), config)
        .open("scanned")
        .await?;
    ctx.register_table("scanned", Arc::new(table) as Arc<dyn TableProvider>)?;
    counting.take_data_reads();
    // A predicate on a value column, so the answer comes from the rows, not from
    // file statistics.
    let batches = ctx
        .sql("SELECT count(*), sum(v) FROM scanned WHERE v % 7 = 0")
        .await?
        .collect()
        .await?;
    let reads = counting.take_data_reads();
    let batch = batches.first().ok_or("no result batch")?;
    let count = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("count is not Int64")?
        .value(0);
    let sum = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("sum is not Int64")?
        .value(0);
    let expected: Vec<i64> = (1..=2500)
        .map(|id| id * 10)
        .filter(|v| v % 7 == 0)
        .collect();
    assert_eq!(
        (count, sum),
        (i64::try_from(expected.len())?, expected.iter().sum::<i64>())
    );
    assert!(
        reads > 0,
        "the scan read its data files through the dataset's store ({reads} reads)"
    );
    Ok(())
}

/// The appends of the `append-kill` script's object-store data path variant: an
/// initial load, then appends of 500, 100, 100, 4 × 50 and 30 × 10 rows.
fn append_kill_batches() -> Vec<i64> {
    let mut batches = vec![1000, 500, 100, 100, 50, 50, 50, 50];
    batches.extend(std::iter::repeat_n(10, 30));
    batches
}

/// Appends to a keyed dataset in the state location write data files and
/// compact them when inline storage is disabled.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn appends_to_an_object_store_data_path_reach_data_files_and_compact() -> TestResult {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog: Arc<dyn MetadataCatalog> = Arc::new(
        cayenne::metastore::object_native::ObjectNativeCatalog::open_or_create_at(
            Arc::clone(&store),
            object_store::path::Path::from("v1/catalog"),
            "s3://lake/v1",
        )
        .await?,
    );
    catalog.init().await?;
    let ctx = SessionContext::new();
    let data_url = "s3://lake/v1/datasets/events/data".to_string();
    // `cayenne_inline_max_rows: '0'`, as the script sets it.
    let vortex_config = VortexConfig {
        cdc_durability: cayenne::metadata::CdcDurability::File,
        compaction_background_interval_ms: 100,
        compaction_trigger_files: 4,
        inline_max_rows: 0,
        ..VortexConfig::default()
    };
    let table = Arc::new(
        crate::common::table_builder_on_store(
            Arc::clone(&catalog),
            ctx.runtime_env(),
            ObjectStoreConfig {
                url: url::Url::parse(&data_url)?,
                store: Arc::clone(&store),
            },
        )
        .create(CreateTableOptions {
            table_name: "events".to_string(),
            schema: schema(),
            primary_key: vec!["id".to_string()],
            on_conflict: Some(
                datafusion_table_providers::util::on_conflict::OnConflict::Upsert(
                    datafusion_table_providers::util::column_reference::ColumnReference::new(vec![
                        "id".to_string(),
                    ]),
                ),
            ),
            base_path: data_url,
            partition_column: None,
            vortex_config,
        })
        .await?,
    );
    ctx.register_table("events", Arc::clone(&table) as Arc<dyn TableProvider>)?;
    assert!(
        table.spawn_background_compaction(Arc::new(tokio::sync::Semaphore::new(1))),
        "background compaction"
    );

    let mut last = 0_i64;
    for rows in append_kill_batches() {
        ctx.sql(&format!(
            "INSERT INTO events SELECT value AS id, value * 10 AS v FROM generate_series({}, {})",
            last + 1,
            last + rows
        ))
        .await?
        .collect()
        .await?;
        last += rows;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    table.drain_in_flight_maintenance().await?;
    let data_files = keys(&store)
        .await?
        .iter()
        .filter(|key| key.ends_with(".vortex"))
        .count();
    // Each keyed append publishes a protected snapshot; compaction merges them once
    // eight wait, so without it the 38 appends would leave 38.
    let protected = catalog
        .get_all_snapshot_sequences(&table.metadata().table_id)
        .await?
        .len();
    assert_eq!(
        (
            count_and_sum(&ctx, "events").await?,
            data_files > 0,
            protected < 16,
        ),
        ((last, expected_sum(1, last)), true, true),
        "every row once, in data files in the store ({data_files}), and compaction committed ({protected} protected snapshots left of {} appends)",
        append_kill_batches().len()
    );
    Ok(())
}
