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

//! Scans of a table whose data is in the object-native location read each
//! snapshot's files from its committed manifest. A maintenance commit that
//! changes which snapshots are live publishes to the in-memory scan state only
//! after the catalog commit, so a scan landing between the two pairs the old
//! snapshot set with the new manifest. These tests run a scan in exactly that
//! window and require every committed row.

use super::*;
use crate::metadata::{CdcDurability, DeletionMode, ObjectStoreConfig};
use crate::metastore::object_native::{ObjectNativeCatalog, new_dataset_folder};
use arrow::array::Int64Array;
use datafusion_table_providers::util::column_reference::ColumnReference;
use object_store::memory::InMemory;

const ROWS_PER_WRITE: i64 = 50;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

const CATALOG_PREFIX: &str = "state/cayenne/v1/catalog";
const LOCATION: &str = "s3://bucket/state/cayenne/v1";

/// A keyed upsert table whose data is in the object-native location of an
/// in-memory store, with every change stream write durable on its own: each
/// upsert publishes a protected snapshot.
struct Placed {
    name: &'static str,
    bucket: Arc<dyn ObjectStore>,
    data_url: String,
    provider: CayenneTableProvider,
}

impl Placed {
    async fn create(name: &'static str) -> Self {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = Self::open_catalog(&bucket).await;
        catalog.init().await.expect("init the catalog");
        let data_url = format!(
            "s3://bucket/{}/{}/data",
            catalog.datasets_prefix(),
            new_dataset_folder(name)
        );
        let ctx = SessionContext::new();
        let provider = Self::builder(&bucket, &data_url, Arc::new(catalog), &ctx)
            .create(CreateTableOptions {
                table_name: name.to_string(),
                schema: schema(),
                primary_key: vec!["id".to_string()],
                on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                    "id".to_string(),
                ]))),
                base_path: data_url.clone(),
                partition_column: None,
                vortex_config: VortexConfig {
                    cdc_durability: CdcDurability::File,
                    deletion_mode: DeletionMode::Key,
                    compaction_background_interval_ms: 0,
                    inline_max_rows: 0,
                    inline_max_bytes: 0,
                    ..VortexConfig::default()
                },
            })
            .await
            .expect("create the placed table");
        assert!(
            provider.data_in_object_native_location() && provider.scans_from_manifest(),
            "precondition: the table's scans read only its manifest"
        );
        Self {
            name,
            bucket,
            data_url,
            provider,
        }
    }

    async fn open_catalog(bucket: &Arc<dyn ObjectStore>) -> ObjectNativeCatalog {
        ObjectNativeCatalog::open_or_create_at(
            Arc::clone(bucket),
            ObjectStorePath::from(CATALOG_PREFIX),
            LOCATION,
        )
        .await
        .expect("open the object-native catalog")
    }

    fn builder(
        bucket: &Arc<dyn ObjectStore>,
        data_url: &str,
        catalog: Arc<dyn MetadataCatalog>,
        ctx: &SessionContext,
    ) -> CayenneTableProviderBuilder {
        CayenneTableProviderBuilder::new(catalog, ctx.runtime_env()).with_object_store(
            ObjectStoreConfig {
                url: url::Url::parse(data_url).expect("data url"),
                store: Arc::clone(bucket),
            },
        )
    }

    /// The table as a new process opens it from the store alone, after this
    /// one's maintenance has settled.
    async fn reopen(&self) -> CayenneTableProvider {
        self.provider.quiesce().await;
        let catalog = Self::open_catalog(&self.bucket).await;
        let ctx = SessionContext::new();
        Self::builder(&self.bucket, &self.data_url, Arc::new(catalog), &ctx)
            .open(self.name)
            .await
            .expect("reopen the placed table")
    }
}

/// Upserts the keys of write `k`: `k * 50 + 1 ..= (k + 1) * 50`, each with
/// `v = id * 10`.
async fn upsert_write(provider: &CayenneTableProvider, k: i64) {
    let ids: Vec<i64> = (k * ROWS_PER_WRITE + 1..=(k + 1) * ROWS_PER_WRITE).collect();
    let values: Vec<i64> = ids.iter().map(|id| id * 10).collect();
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .expect("batch");
    let stream: SendableRecordBatchStream = Box::pin(
        datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
            schema(),
            futures::stream::iter([Ok(batch)]),
        ),
    );
    let ctx = SessionContext::new();
    provider
        .write_cdc_append_stream(stream, &ctx.task_ctx())
        .await
        .expect("apply the upsert")
        .finish()
        .await
        .expect("publish the upsert");
}

/// `(count(*), sum(v))` of the table, through a full scan.
async fn count_and_sum(provider: &CayenneTableProvider) -> (i64, i64) {
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(provider.clone_for_write()))
        .expect("register");
    let batches = ctx
        .sql("SELECT count(*), coalesce(sum(v), 0) FROM t")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("scan");
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count")
        .value(0);
    let sum = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("sum")
        .value(0);
    (count, sum)
}

fn expected_after(writes: i64) -> (i64, i64) {
    let rows = writes * ROWS_PER_WRITE;
    (rows, (1..=rows).map(|id| id * 10).sum())
}

/// Merging protected snapshots commits the swap (inputs out, merged output in)
/// to the catalog, then swaps the in-memory protected map under the listing
/// fence. A scan between the two still sees the inputs as live and must still
/// read their rows: the commit that retires them must not take away the
/// manifest a scan reads them from.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_scan_between_a_protected_merge_commit_and_its_publish_reads_every_row() {
    const WRITES: i64 = 8;
    let placed = Placed::create("merge_window").await;
    let provider = &placed.provider;

    let seen: Arc<ParkingMutex<Option<(i64, i64)>>> = Arc::default();
    {
        let reader = provider.clone_for_write();
        let seen = Arc::clone(&seen);
        *provider.test_pre_publish_hook.lock() = Some(Box::new(move || {
            Box::pin(async move {
                let answer = count_and_sum(&reader).await;
                *seen.lock() = Some(answer);
            })
        }));
    }
    for k in 0..WRITES {
        upsert_write(provider, k).await;
    }
    assert!(
        provider.protected_snapshots.load().len() >= 2,
        "precondition: the upserts published protected snapshots to merge"
    );
    // The trigger may have started a merge already; either pass runs the hook.
    provider
        .compact_protected_snapshots_subset(usize::MAX)
        .await
        .expect("merge the protected snapshots");
    provider.quiesce().await;

    let in_window = (*seen.lock()).expect("precondition: a merge committed and ran the hook");
    assert_eq!(
        in_window,
        expected_after(WRITES),
        "a scan between the merge's catalog commit and its publish must read every committed row"
    );
    assert_eq!(count_and_sum(provider).await, expected_after(WRITES));
    assert_eq!(
        count_and_sum(&placed.reopen().await).await,
        expected_after(WRITES),
        "rows after a reopen from the store"
    );
}

/// A full rewrite of a key-delete table folds the protected snapshots it
/// captured and leaves a protected snapshot published after its capture live,
/// for a later pass. The rewrite's commit and what follows it must keep that
/// snapshot's rows: they are in no other snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_protected_snapshot_published_during_a_full_rewrite_keeps_its_rows() {
    const BEFORE: i64 = 3;
    let placed = Placed::create("rewrite_window").await;
    let provider = &placed.provider;
    for k in 0..BEFORE {
        upsert_write(provider, k).await;
    }
    let captured = provider.protected_snapshot_ids();
    assert_eq!(
        captured.len(),
        3,
        "precondition: three protected snapshots to fold"
    );

    let published_mid_rewrite = Arc::new(AtomicBool::new(false));
    {
        let writer = provider.clone_for_write();
        let published = Arc::clone(&published_mid_rewrite);
        *provider.test_rewrite_post_capture_hook.lock() = Some(Box::new(move || {
            Box::pin(async move {
                upsert_write(&writer, BEFORE).await;
                published.store(true, Ordering::Release);
            })
        }));
    }
    assert!(
        provider
            .rewrite_current_snapshot_for_compaction()
            .await
            .expect("the full rewrite runs"),
        "precondition: the rewrite committed a new current snapshot"
    );
    assert!(
        published_mid_rewrite.load(Ordering::Acquire),
        "precondition: an upsert published after the rewrite's capture"
    );
    let after = provider.protected_snapshot_ids();
    assert_eq!(
        after.len(),
        1,
        "precondition: the snapshot published mid-rewrite stays protected"
    );
    assert!(
        after.is_disjoint(&captured),
        "precondition: the captured snapshots were folded"
    );
    provider.quiesce().await;

    assert_eq!(
        count_and_sum(provider).await,
        expected_after(BEFORE + 1),
        "rows after the rewrite, including the upsert published during it"
    );
    assert_eq!(
        count_and_sum(&placed.reopen().await).await,
        expected_after(BEFORE + 1),
        "rows after a reopen from the store"
    );
}
