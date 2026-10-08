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

use super::*;
use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use cayenne::{
    CayenneCatalog, CayenneTableProviderBuilder, MetadataCatalog,
    metadata::{CdcDurability, CreateTableOptions, DeletionMode, VortexConfig},
};
use datafusion::{
    common::TableReference, datasource::memory::MemorySourceConfig, logical_expr::dml::InsertOp,
    physical_plan::collect,
};
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};
use runtime_acceleration::change_sink::ChangeSink;
use tokio::runtime::Handle;

async fn table(
    ctx: &SessionContext,
    shards: usize,
) -> (Arc<CayenneTableProvider>, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("table directory");
    let catalog: Arc<dyn MetadataCatalog> = Arc::new(
        CayenneCatalog::new(format!(
            "sqlite://{}",
            dir.path().join("catalog.db").display()
        ))
        .expect("catalog"),
    );
    catalog.init().await.expect("initialize catalog");
    let provider = CayenneTableProviderBuilder::new(catalog, ctx.runtime_env())
        .create(CreateTableOptions {
            table_name: "rebuildable".into(),
            schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            primary_key: vec!["id".into()],
            on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec!["id".into()]))),
            base_path: dir.path().join("data").to_string_lossy().into_owned(),
            partition_column: None,
            vortex_config: VortexConfig {
                cdc_durability: CdcDurability::Memory,
                deletion_mode: DeletionMode::Key,
                cdc_mem_tier_shards: shards,
                cdc_mem_tier_checkpoint_interval_ms: 0,
                compaction_background_interval_ms: 0,
                ..VortexConfig::default()
            },
        })
        .await
        .expect("create native table");
    (Arc::new(provider), dir)
}

fn batch(table: &CayenneTableProvider, ids: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(table.schema(), vec![Arc::new(Int64Array::from(ids))]).expect("rows")
}

async fn write(
    sink: &ChangeSink,
    table: &CayenneTableProvider,
    ids: Vec<i64>,
    recovery: Recovery,
) -> StorageDurability {
    let receipt = sink
        .reserve()
        .await
        .expect("capacity")
        .submit(
            ChangeBatch::append(table.schema(), vec![batch(table, ids)]).expect("append"),
            WriteOptions {
                recovery,
                ..WriteOptions::default()
            },
        )
        .expect("admission")
        .wait()
        .await
        .expect("write receipt");
    receipt.published().await.expect("published rows");
    receipt.durability
}

#[tokio::test]
async fn rebuildable_composed_rows_use_native_ram_without_source_callback() {
    for shards in [1, 4] {
        let ctx = SessionContext::new();
        let (table, _dir) = table(&ctx, shards).await;
        let target = spice_table::SpiceTable::over(
            Arc::new(spice_table::IndexLayer::new()),
            Arc::clone(&table) as Arc<dyn TableProvider>,
        ) as Arc<dyn TableProvider>;
        assert!(!target.is::<CayenneTableProvider>());
        let backend = CayenneChangeSinkBackend::try_new(ChangeSinkContext::new(
            TableReference::bare("rebuildable"),
            target,
        ))
        .expect("native binding through a write-transparent layer");
        let sink = ChangeSink::new(backend, ctx.clone(), &Handle::current(), 2);
        assert_eq!(
            write(&sink, &table, vec![1, 2], Recovery::Rebuildable).await,
            StorageDurability::NotPromised
        );
        assert!(
            !table.has_slot_advancer(),
            "cache writes must not invent source callbacks"
        );
        assert_eq!(
            ctx.read_table(Arc::clone(&table) as Arc<dyn TableProvider>)
                .expect("scan")
                .count()
                .await
                .expect("visible rows"),
            2
        );
        assert!(
            table
                .checkpoint_mem_tier()
                .await
                .expect("cover buffered epoch")
                > 0,
            "a durable append would not have a RAM epoch to checkpoint"
        );
        assert_eq!(
            write(&sink, &table, vec![2, 3], Recovery::Rebuildable).await,
            StorageDurability::NotPromised
        );
        assert_eq!(
            write(&sink, &table, vec![4], Recovery::Durable).await,
            StorageDurability::Durable
        );
        assert_eq!(
            ctx.read_table(Arc::clone(&table) as Arc<dyn TableProvider>)
                .expect("scan")
                .count()
                .await
                .expect("upsert plus durable transition"),
            4
        );
        sink.begin_close().wait().await.expect("drain");
    }
}

#[tokio::test]
async fn write_opaque_metadata_preserves_provider_fallback() {
    let ctx = SessionContext::new();
    let (table, _dir) = table(&ctx, 1).await;
    let target = data_components::metadata_enriched_table_provider(
        Arc::clone(&table) as Arc<dyn TableProvider>,
        std::collections::HashMap::from([("test_marker".into(), "wrapped".into())]),
        std::collections::HashMap::default(),
    );
    assert!(!target.is::<CayenneTableProvider>());
    assert_eq!(target.schema().metadata()["test_marker"], "wrapped");
    let context = ChangeSinkContext::new(TableReference::bare("rebuildable"), target);
    assert!(CayenneChangeSinkBackend::try_new(context.clone()).is_none());
    let sink = ChangeSink::new(
        Arc::new(ProviderChangeSinkBackend::new(context)),
        ctx.clone(),
        &Handle::current(),
        2,
    );
    assert_eq!(
        write(&sink, &table, vec![7], Recovery::Rebuildable).await,
        StorageDurability::NotPromised
    );
    assert!(!table.has_slot_advancer());
    assert_eq!(
        table.checkpoint_mem_tier().await.expect("empty RAM tier"),
        0
    );
    assert_eq!(
        ctx.read_table(table as Arc<dyn TableProvider>)
            .expect("scan")
            .count()
            .await
            .expect("provider-backed row"),
        1
    );
    sink.begin_close().wait().await.expect("drain fallback");
}

#[tokio::test]
async fn rebuildable_transition_preserves_replayable_cdc_durability_fence() {
    use arrow::{
        array::{ListArray, StringArray, StructArray},
        buffer::OffsetBuffer,
    };
    use std::sync::atomic::{AtomicU64, Ordering};

    struct ObservedFence(AtomicU64);
    #[async_trait]
    impl DurabilityObserver for ObservedFence {
        async fn on_durable(&self, epoch: u64) {
            self.0.store(epoch, Ordering::SeqCst);
        }
    }
    let ctx = SessionContext::new();
    let (table, _dir) = table(&ctx, 1).await;
    let backend = CayenneChangeSinkBackend::try_new(ChangeSinkContext::new(
        TableReference::bare("rebuildable"),
        Arc::clone(&table) as Arc<dyn TableProvider>,
    ))
    .expect("native backend");
    let sink = ChangeSink::new(backend, ctx, &Handle::current(), 2);
    let observer = Arc::new(ObservedFence(AtomicU64::new(0)));
    sink.set_durability_observer(Arc::clone(&observer) as Arc<dyn DurabilityObserver>);
    let keys = ListArray::try_new(
        Arc::new(Field::new("item", DataType::Utf8, false)),
        OffsetBuffer::new(vec![0_i32, 1].into()),
        Arc::new(StringArray::from(vec!["id"])),
        None,
    )
    .expect("keys");
    let record = RecordBatch::try_new(
        Arc::new(data_components::cdc::changes_schema(&table.schema())),
        vec![
            Arc::new(StringArray::from(vec!["u"])),
            Arc::new(keys),
            Arc::new(StructArray::from(batch(&table, vec![1]))),
        ],
    )
    .expect("source change");
    let receipt = sink
        .reserve()
        .await
        .expect("capacity")
        .submit(
            ChangeBatch::cdc(CdcBatch::try_new(record).expect("CDC batch")),
            WriteOptions {
                recovery: Recovery::Replayable,
                ..WriteOptions::default()
            },
        )
        .expect("admit CDC")
        .wait()
        .await
        .expect("CDC receipt");
    receipt.published().await.expect("CDC publication");
    let StorageDurability::Deferred(epoch) = receipt.durability else {
        panic!("CDC must report its pending storage epoch");
    };
    assert!(table.has_slot_advancer());
    assert_eq!(
        observer.0.load(Ordering::SeqCst),
        0,
        "publication cannot acknowledge a volatile write"
    );
    assert_eq!(
        write(&sink, &table, vec![2], Recovery::Rebuildable).await,
        StorageDurability::NotPromised
    );
    assert!(
        observer.0.load(Ordering::SeqCst) >= epoch,
        "transition must checkpoint the CDC prefix"
    );
    assert!(
        !table.has_slot_advancer(),
        "cache work has no source callback"
    );
    sink.begin_close().wait().await.expect("drain cache work");
}

#[tokio::test]
async fn rebuildable_permission_does_not_affect_unmarked_or_other_table_writes() {
    let ctx = SessionContext::new();
    let (first, _first_dir) = table(&ctx, 1).await;
    let (other, _other_dir) = table(&ctx, 1).await;
    let mut state = ctx.state();
    state
        .config_mut()
        .set_extension(Arc::new(RebuildableWrite::new(&first)));
    let marked = SessionContext::new_with_state(state);
    for (session, table) in [(&ctx, &first), (&marked, &other)] {
        let row = batch(table, vec![7]);
        let input =
            MemorySourceConfig::try_new_exec(&[vec![row]], table.schema(), None).expect("input");
        let plan = table
            .insert_into(&session.state(), input, InsertOp::Append)
            .await
            .expect("composed insert");
        collect(plan, session.task_ctx())
            .await
            .expect("insert execution");
        assert!(!table.has_slot_advancer());
        assert_eq!(
            table.checkpoint_mem_tier().await.expect("empty RAM tier"),
            0
        );
        assert_eq!(
            session
                .read_table(Arc::clone(table) as Arc<dyn TableProvider>)
                .expect("scan")
                .count()
                .await
                .expect("durable row"),
            1
        );
    }
}
