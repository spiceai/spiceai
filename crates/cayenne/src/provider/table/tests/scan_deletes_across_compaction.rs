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

//! A scan of a table without a primary key captures its files, then builds its
//! plan, attaching each file's position deletes. A full compaction that commits
//! between the two rewrites the files without the deleted rows and clears the
//! position deletes. The scan still reads the files it captured, so it must
//! apply the deletes those files had: it returns exactly the rows of the state
//! it captured.

use super::*;
use crate::metadata::ObjectStoreConfig;
use crate::metastore::object_native::{ObjectNativeCatalog, new_dataset_folder};
use object_store::memory::InMemory;

fn vortex_config() -> VortexConfig {
    VortexConfig {
        inline_max_rows: 0,
        inline_max_bytes: 0,
        compaction_background_interval_ms: 0,
        ..VortexConfig::default()
    }
}

/// A table without a primary key whose data and metastore are in the
/// object-native location of an in-memory store.
async fn object_native_position_table(name: &str, ctx: &SessionContext) -> CayenneTableProvider {
    use arrow::datatypes::{DataType, Field, Schema};
    let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = ObjectNativeCatalog::open_or_create_at(
        Arc::clone(&bucket),
        ObjectStorePath::from("state/cayenne/v1/catalog"),
        "s3://bucket/state/cayenne/v1",
    )
    .await
    .expect("open the object-native catalog");
    catalog.init().await.expect("init the catalog");
    let data_url = format!(
        "s3://bucket/{}/{}/data",
        catalog.datasets_prefix(),
        new_dataset_folder(name)
    );
    let provider = CayenneTableProviderBuilder::new(Arc::new(catalog), ctx.runtime_env())
        .with_object_store(ObjectStoreConfig {
            url: url::Url::parse(&data_url).expect("data url"),
            store: bucket,
        })
        .create(CreateTableOptions {
            table_name: name.to_string(),
            schema: Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("value", DataType::Int64, false),
            ])),
            primary_key: vec![],
            on_conflict: None,
            base_path: data_url,
            partition_column: None,
            vortex_config: vortex_config(),
        })
        .await
        .expect("create the object-native table");
    assert!(provider.data_in_object_native_location());
    provider
}

/// Ids 1..=100 in two writes (two files), then ids 1..=10 deleted by position.
async fn table_with_position_deletes(
    name: &str,
    ctx: &SessionContext,
    object_native: bool,
) -> (Arc<CayenneTableProvider>, Option<TempDir>) {
    let (provider, tmp) = if object_native {
        (object_native_position_table(name, ctx).await, None)
    } else {
        let (provider, _catalog, tmp) = create_position_based_table_with_vortex_config(
            name,
            ctx.runtime_env(),
            vortex_config(),
        )
        .await;
        (provider, Some(tmp))
    };
    let provider = Arc::new(provider);
    let schema = Arc::clone(&provider.table_metadata.schema);
    let first: Vec<i64> = (1..=50).collect();
    let second: Vec<i64> = (51..=100).collect();
    insert_batch(
        &provider,
        id_value_batch(Arc::clone(&schema), &first, &first),
    )
    .await;
    insert_batch(&provider, id_value_batch(schema, &second, &second)).await;
    assert!(provider.pk_deletion_strategy().is_position_based());
    let delete = provider
        .delete_from(&ctx.state(), vec![col("id").lt_eq(lit_i64(10))])
        .await
        .expect("position delete plan");
    collect(delete, ctx.task_ctx())
        .await
        .expect("execute delete");
    assert!(
        !provider
            .pk_deletion_strategy
            .position_cache()
            .load()
            .is_empty(),
        "precondition: the delete left position deletes, not a rewrite"
    );
    (provider, tmp)
}

fn expected() -> Vec<(i64, i64)> {
    (11..=100).map(|id| (id, id)).collect()
}

/// A compaction that commits after the scan captured its files and before it
/// built its plan, with the SQLite metastore and in the object-native location.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scan_racing_compaction_returns_only_live_rows() {
    for object_native in [false, true] {
        let ctx = SessionContext::new();
        let (provider, _tmp) =
            table_with_position_deletes("deletes_compaction_race", &ctx, object_native).await;
        let compacted = Arc::new(ParkingMutex::new(None::<bool>));
        let compactor = Arc::clone(&provider);
        let outcome = Arc::clone(&compacted);
        *provider.test_post_scan_view_selection_hook.lock() = Some(Box::new(move || {
            Box::pin(async move {
                // Bounded: a compaction that waits on the scan would deadlock here.
                let committed = tokio::time::timeout(
                    Duration::from_secs(60),
                    compactor.force_full_compaction(),
                )
                .await
                .expect("the compaction must not wait for the scan that captured its files")
                .expect("full compaction");
                *outcome.lock() = Some(committed);
            })
        }));
        let rows = scan_id_values(&provider).await;
        assert_eq!(
            *compacted.lock(),
            Some(true),
            "precondition: the compaction committed inside the scan's window"
        );
        assert!(
            provider
                .pk_deletion_strategy
                .position_cache()
                .load()
                .is_empty()
        );
        assert_eq!(rows, expected(), "object_native={object_native}");
        // The next scan, planned after the compaction, reads the new files.
        assert_eq!(
            scan_id_values(&provider).await,
            expected(),
            "object_native={object_native}"
        );
    }
}
