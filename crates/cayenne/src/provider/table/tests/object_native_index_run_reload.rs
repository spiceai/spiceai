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

//! A provider opened over another's pins, as a reload opens its replacement,
//! shares the replaced provider's record of uploads no commit names yet: the
//! replaced provider's writes may still be in flight. Its orphan sweep must
//! not retire a lookup-index run file over a data file such a write holds,
//! since the write may still commit it and the same run, rebuilt, register
//! that file again.

use super::*;
use crate::metadata::ObjectStoreConfig;
use crate::metastore::object_native::{ObjectNativeCatalog, new_dataset_folder};
use object_store::memory::InMemory;

const CATALOG_PREFIX: &str = "state/cayenne/v1/catalog";
const LOCATION: &str = "s3://bucket/state/cayenne/v1";
const TABLE: &str = "orders";

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn builder(
    bucket: &Arc<dyn ObjectStore>,
    data_url: &str,
    catalog: &Arc<dyn MetadataCatalog>,
    ctx: &SessionContext,
) -> CayenneTableProviderBuilder {
    CayenneTableProviderBuilder::new(Arc::clone(catalog), ctx.runtime_env()).with_object_store(
        ObjectStoreConfig {
            url: url::Url::parse(data_url).expect("data url"),
            store: Arc::clone(bucket),
        },
    )
}

/// A run file over `file`, as an index sync writes one: its name and bytes.
fn run_file(file: &str) -> (String, Vec<u8>) {
    let encoder = key_index::KeyEncoder::new(vec![key_index::KeyField::new(DataType::Int64, true)])
        .expect("an encoder");
    let mut builder = key_index::tiered::RunBuilder::new(encoder);
    let keys: arrow::array::ArrayRef = Arc::new(arrow::array::Int64Array::from(vec![1_i64]));
    builder.add_batch(file, 0, &[keys]).expect("index a row");
    let run = builder.finish().expect("a run");
    (run.file_name(), run.to_bytes())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reload_sees_the_replaced_providers_uploads_in_flight() {
    let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let native = ObjectNativeCatalog::open_or_create_at(
        Arc::clone(&bucket),
        ObjectStorePath::from(CATALOG_PREFIX),
        LOCATION,
    )
    .await
    .expect("open the object-native catalog");
    native.init().await.expect("init the catalog");
    let data_url = format!(
        "s3://bucket/{}/{}/data",
        native.datasets_prefix(),
        new_dataset_folder(TABLE)
    );
    let catalog: Arc<dyn MetadataCatalog> = Arc::new(native);
    let ctx = SessionContext::new();
    let replaced = builder(&bucket, &data_url, &catalog, &ctx)
        .create(CreateTableOptions {
            table_name: TABLE.to_string(),
            schema: schema(),
            primary_key: Vec::new(),
            on_conflict: None,
            base_path: data_url.clone(),
            partition_column: None,
            vortex_config: VortexConfig {
                inline_max_rows: 0,
                inline_max_bytes: 0,
                ..VortexConfig::default()
            },
        })
        .await
        .expect("create the table");
    replaced.set_orphan_age_floor(std::time::Duration::ZERO);

    // An append of the replaced provider has uploaded data file `held` at its
    // final key, and no commit names it yet.
    let held = "01b0000000000000000000000000000d_00000.vortex";
    let files = replaced.uncommitted_files();
    files.begin_final_key_write("w-in-flight", "current");
    files.note("w-in-flight", held, 8);

    // A run file over it, written and not yet registered.
    let (run_name, run_bytes) = run_file(held);
    let table_root = replaced.snapshot_root_key_prefix().expect("table root");
    let run_key = format!("_lookup_index/00000000000000000000000000000001/{run_name}");
    bucket
        .put(
            &ObjectStorePath::from(format!("{table_root}{run_key}")),
            run_bytes.into(),
        )
        .await
        .expect("plant the run file");

    // The reload, over the replaced provider's pins, sees the upload in flight.
    let reloaded = builder(&bucket, &data_url, &catalog, &ctx)
        .with_snapshot_dir_pins(replaced.snapshot_dir_pins())
        .open(TABLE)
        .await
        .expect("reload");
    reloaded.set_orphan_age_floor(std::time::Duration::ZERO);
    reloaded.wait_for_orphan_sweep().await;
    let swept = reloaded.sweep_orphan_files_now().await.expect("sweep");
    assert_eq!(
        swept.retired_index_runs,
        Vec::<String>::new(),
        "the reload retires no run over a file the replaced provider holds"
    );

    // A provider opened afresh, sharing nothing, judges the same file gone:
    // the protection above came from the shared record.
    let fresh = builder(&bucket, &data_url, &catalog, &ctx)
        .open(TABLE)
        .await
        .expect("open afresh");
    fresh.set_orphan_age_floor(std::time::Duration::ZERO);
    fresh.wait_for_orphan_sweep().await;
    let swept = fresh.sweep_orphan_files_now().await.expect("sweep");
    let retired = catalog
        .retired_files(&fresh.table_metadata.table_id)
        .await
        .expect("retired files")
        .unwrap_or_default();
    assert!(
        swept.retired_index_runs == vec![run_key.clone()] || retired.contains(&run_key),
        "a provider that does not see the upload retires the run: swept {swept:?}, retired {retired:?}"
    );
}
