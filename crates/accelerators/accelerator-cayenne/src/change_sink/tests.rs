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
    array::{Array, AsArray, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use cayenne::{
    CayenneCatalog, CayenneTableProviderBuilder, MetadataCatalog, RebuildableWrite,
    metadata::{CdcDurability, CreateTableOptions, DeletionMode, VortexConfig},
};
use datafusion::logical_expr::{col, lit};
use datafusion::{
    common::TableReference, datasource::memory::MemorySourceConfig, logical_expr::dml::InsertOp,
    physical_plan::collect,
};
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};
use runtime_acceleration::change_sink::ChangeSink;
use runtime_acceleration::change_sink::SetKey;
use runtime_acceleration::change_sink::batching::{AppendIngress, CoalescingLimits};
use std::collections::BTreeMap;
use std::time::Duration;
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
async fn rebuildable_composed_rows_append_durably_without_source_callback() {
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
        assert_eq!(
            table.checkpoint_mem_tier().await.expect("empty RAM tier"),
            0,
            "cache rows take the normal append path, not the RAM tier"
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

        async fn record_position(
            &self,
            _fence: u64,
        ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Ok(())
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

/// Columns of a cached HTTP response: the request that produced it and its
/// payload. Like a cache dataset's, the primary key leaves out the body.
fn cache_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("request_path", DataType::Utf8, false),
        Field::new("request_query", DataType::Utf8, false),
        Field::new("request_body", DataType::Utf8, true),
        Field::new("content", DataType::Utf8, false),
    ]))
}

async fn cache_table(ctx: &SessionContext) -> (Arc<CayenneTableProvider>, tempfile::TempDir) {
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
            table_name: "responses".into(),
            schema: cache_schema(),
            primary_key: vec!["request_query".into(), "request_path".into()],
            on_conflict: None,
            base_path: dir.path().join("data").to_string_lossy().into_owned(),
            partition_column: None,
            vortex_config: VortexConfig {
                cdc_durability: CdcDurability::Memory,
                deletion_mode: DeletionMode::Key,
                cdc_mem_tier_checkpoint_interval_ms: 0,
                compaction_background_interval_ms: 0,
                ..VortexConfig::default()
            },
        })
        .await
        .expect("create cache table");
    (Arc::new(provider), dir)
}

fn cache_lane() -> Arc<AppendIngress> {
    Arc::new(AppendIngress::new(
        &TableReference::bare("responses"),
        CoalescingLimits {
            max_inputs: 64,
            max_bytes: usize::MAX,
            max_age: Duration::from_millis(50),
        },
    ))
}

/// One cached response, written as the cache writer writes it: the scope is the
/// whole request, so it carries the body as well as the key.
fn cache_write(
    schema: &SchemaRef,
    lane: &Arc<AppendIngress>,
    (path, query, body): (&str, &str, Option<&str>),
    content: Option<&str>,
    replaces: bool,
) -> ChangeBatch {
    let body_filter = match body {
        Some(body) => col("request_body").eq(lit(body)),
        None => col("request_body").is_null(),
    };
    let scope = SetKey::from_filters(
        Arc::clone(schema),
        &[
            col("request_path").eq(lit(path)),
            col("request_query").eq(lit(query)),
            body_filter,
        ],
    )
    .expect("request scope");
    let rows = content
        .map(|content| {
            RecordBatch::try_new(
                Arc::clone(schema),
                vec![
                    Arc::new(StringArray::from(vec![path])),
                    Arc::new(StringArray::from(vec![query])),
                    Arc::new(StringArray::from(vec![body])),
                    Arc::new(StringArray::from(vec![content])),
                ],
            )
            .expect("response row")
        })
        .into_iter()
        .collect();
    if replaces {
        ChangeBatch::replace_set(scope, Arc::clone(schema), rows)
    } else {
        ChangeBatch::append_scoped(scope, Arc::clone(schema), rows)
    }
    .expect("cache write")
    .with_append_ingress(Arc::clone(lane))
    .expect("append lane")
}

type StoredResponse = (String, String, Option<String>, String);

async fn stored_responses(
    ctx: &SessionContext,
    table: &Arc<CayenneTableProvider>,
) -> Vec<StoredResponse> {
    let batches = ctx
        .read_table(Arc::clone(table) as Arc<dyn TableProvider>)
        .expect("scan")
        .collect()
        .await
        .expect("stored responses");
    let mut rows = Vec::new();
    for batch in &batches {
        let columns = (0..4)
            .map(|index| {
                arrow::compute::cast(batch.column(index), &DataType::Utf8).expect("text column")
            })
            .collect::<Vec<_>>();
        let [path, query, body, content] =
            [0, 1, 2, 3].map(|index| columns[index].as_string::<i32>());
        for row in 0..batch.num_rows() {
            rows.push((
                path.value(row).to_string(),
                query.value(row).to_string(),
                (!body.is_null(row)).then(|| body.value(row).to_string()),
                content.value(row).to_string(),
            ));
        }
    }
    rows.sort();
    rows
}

struct Xorshift(u64);

impl Xorshift {
    fn below(&mut self, bound: usize) -> usize {
        let mut state = self.0;
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        self.0 = state;
        usize::try_from(state % u64::try_from(bound).expect("bound fits u64"))
            .expect("remainder fits usize")
    }
}

/// Replaced cache scopes delete by primary key, without reading stored rows,
/// and must leave exactly what the scope delete leaves. Each seeded history is
/// applied in coalesced bursts to one Cayenne table through this backend and to
/// another through the provider backend, whose `DELETE` matches each scope's
/// full predicate. Both must match an in-memory model of the cache: one
/// response per key, a write refused while a response with another body holds
/// its key, and an empty replacement deleting only a response stored under its
/// own body.
#[tokio::test]
async fn cache_replacements_by_key_match_the_scope_delete() {
    const PATHS: [&str; 3] = ["/v1/devices", "/v1/sites", "/v1/users"];
    const QUERIES: [&str; 6] = ["a=1", "a=2", "a=3", "a=4", "a=5", "a=6"];
    const BODIES: [Option<&str>; 2] = [None, Some("{\"page\":2}")];
    const WRITES: usize = 200;
    // Replaced a stored key, replaced an absent key, refused, empty kept, empty deleted.
    let mut seen = [0_usize; 5];
    for seed in 1..=4_u64 {
        let ctx = SessionContext::new();
        let schema = cache_schema();
        let (keyed, _keyed_dir) = cache_table(&ctx).await;
        let (scoped, _scoped_dir) = cache_table(&ctx).await;
        let keyed_sink = ChangeSink::new(
            CayenneChangeSinkBackend::try_new(ChangeSinkContext::new(
                TableReference::bare("responses"),
                Arc::clone(&keyed) as Arc<dyn TableProvider>,
            ))
            .expect("native binding"),
            ctx.clone(),
            &Handle::current(),
            1024,
        );
        let scoped_sink = ChangeSink::new(
            Arc::new(
                ProviderChangeSinkBackend::new(ChangeSinkContext::new(
                    TableReference::bare("responses"),
                    Arc::clone(&scoped) as Arc<dyn TableProvider>,
                ))
                .with_ordered_replacement(),
            ),
            ctx.clone(),
            &Handle::current(),
            1024,
        );
        let lanes = [cache_lane(), cache_lane()];
        let mut random = Xorshift(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let mut model: BTreeMap<(&str, &str), (Option<&str>, String)> = BTreeMap::new();
        let mut expected_outcomes = Vec::with_capacity(WRITES);
        let (done, mut completions) = tokio::sync::mpsc::unbounded_channel();
        for index in 0..WRITES {
            let key = (
                PATHS[random.below(PATHS.len())],
                QUERIES[random.below(QUERIES.len())],
            );
            let body = BODIES[usize::from(random.below(5) == 0)];
            let held = model.get(&key).cloned();
            let content = format!("{seed}:{index}");
            let empty = random.below(8) == 0;
            // A fill appends only while nothing holds its key, as a proven-fresh fill does.
            let replaces = empty || held.is_some() || random.below(2) == 0;
            let refused = !empty && held.as_ref().is_some_and(|(stored, _)| *stored != body);
            let kind = match (&held, empty) {
                _ if refused => Some(2),
                (Some(_), false) => Some(0),
                (None, false) if replaces => Some(1),
                (Some((stored, _)), true) if *stored != body => Some(3),
                (Some(_), true) => Some(4),
                _ => None,
            };
            if let Some(kind) = kind {
                seen[kind] += 1;
            }
            expected_outcomes.push(!refused);
            if !refused {
                if !empty {
                    model.insert(key, (body, content.clone()));
                } else if held.is_some_and(|(stored, _)| stored == body) {
                    model.remove(&key);
                }
            }
            let rows = (!empty).then_some(content.as_str());
            for (sink_index, (sink, lane)) in [(&keyed_sink, &lanes[0]), (&scoped_sink, &lanes[1])]
                .into_iter()
                .enumerate()
            {
                let done = done.clone();
                sink.enqueue(
                    cache_write(&schema, lane, (key.0, key.1, body), rows, replaces),
                    WriteOptions {
                        recovery: Recovery::Rebuildable,
                        ..WriteOptions::default()
                    },
                    Box::new(move |result| {
                        let _ = done.send((
                            sink_index,
                            index,
                            result.map_err(|error| error.to_string()),
                        ));
                    }),
                )
                .await
                .expect("admitted");
            }
        }
        keyed_sink.flush().await.expect("flush key-delete sink");
        scoped_sink.flush().await.expect("flush scope-delete sink");
        let mut outcomes = [vec![None; WRITES], vec![None; WRITES]];
        for _ in 0..2 * WRITES {
            let (sink_index, index, result) =
                tokio::time::timeout(Duration::from_secs(30), completions.recv())
                    .await
                    .expect("every write completes")
                    .expect("completion delivered");
            if let Err(error) = &result {
                assert!(
                    error.contains("conflicts with a row outside the declared scope")
                        || error.contains("Unique key conflicts between scoped appends"),
                    "seed {seed}: write {index} failed for another reason: {error}"
                );
            }
            outcomes[sink_index][index] = Some(result.is_ok());
        }
        for (name, outcomes) in ["key delete", "scope delete"].into_iter().zip(outcomes) {
            let outcomes = outcomes
                .into_iter()
                .map(|outcome| outcome.expect("each write completes once"))
                .collect::<Vec<_>>();
            assert_eq!(
                outcomes, expected_outcomes,
                "seed {seed}: {name} per-write outcomes"
            );
        }
        let mut expected = model
            .iter()
            .map(|(&(path, query), (body, content))| {
                (
                    path.to_string(),
                    query.to_string(),
                    body.map(str::to_string),
                    content.clone(),
                )
            })
            .collect::<Vec<_>>();
        expected.sort();
        assert_eq!(
            stored_responses(&ctx, &keyed).await,
            expected,
            "seed {seed}: key delete"
        );
        assert_eq!(
            stored_responses(&ctx, &scoped).await,
            expected,
            "seed {seed}: scope delete"
        );
        // Only a key delete tombstones a key whose replacement found nothing stored.
        assert!(
            keyed.deletion_index_len() > scoped.deletion_index_len(),
            "seed {seed}: replacements did not delete by key ({} vs {} tombstones)",
            keyed.deletion_index_len(),
            scoped.deletion_index_len()
        );
        keyed_sink
            .begin_close()
            .wait()
            .await
            .expect("drain key-delete sink");
        scoped_sink
            .begin_close()
            .wait()
            .await
            .expect("drain scope-delete sink");
    }
    assert!(
        seen.iter().all(|&count| count > 0),
        "a kind of write never occurred: {seen:?}"
    );
}

/// The prefix of `dataset`'s object-native log in the catalog at `catalog/`.
async fn object_native_log(
    store: &Arc<dyn object_store::ObjectStore>,
    dataset: &str,
) -> object_store::path::Path {
    cayenne::metastore::object_native::dataset_log_at(
        Arc::clone(store),
        &object_store::path::Path::from("catalog"),
        dataset,
    )
    .await
    .expect("read the registry")
    .expect("the dataset has a log")
}

/// The action names of each commit to `dataset`'s object-native log after
/// `after`, oldest first.
async fn object_native_records_after(
    store: &Arc<dyn object_store::ObjectStore>,
    dataset: &str,
    after: u64,
) -> Vec<Vec<&'static str>> {
    use prost::Message;
    let (_, opened) = object_store_occ::headlog::Reader::open(
        Arc::clone(store),
        object_native_log(store, dataset).await,
    )
    .await
    .expect("open head log");
    opened
        .records
        .into_iter()
        .filter(|entry| entry.commit > after)
        .map(|entry| {
            object_store_occ::headlog::Group::decode(entry.record.payload)
                .expect("group")
                .payloads
                .into_iter()
                .flat_map(|payload| {
                    cayenne_metastore_actions::Record::decode(payload)
                        .expect("record")
                        .actions
                        .iter()
                        .map(cayenne_metastore_actions::Action::name)
                        .collect::<Vec<_>>()
                })
                .collect()
        })
        .collect()
}

/// With the object-native metastore, a durable CDC write and its input's source
/// position commit as one record, so no record holds a position without the
/// data it covers, and the position is never saved before the data.
#[tokio::test]
async fn an_object_native_durable_cdc_write_commits_its_position_with_its_data() {
    use arrow::{
        array::{ListArray, StructArray},
        buffer::OffsetBuffer,
    };
    use cayenne::metastore::object_native::ObjectNativeCatalog;

    let ctx = SessionContext::new();
    let dir = tempfile::tempdir().expect("table directory");
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let catalog = Arc::new(
        ObjectNativeCatalog::create(
            Arc::clone(&store),
            object_store::path::Path::from("catalog"),
        )
        .await
        .expect("object-native catalog"),
    );
    let table = Arc::new(
        CayenneTableProviderBuilder::new(
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
            ctx.runtime_env(),
        )
        .create(CreateTableOptions {
            table_name: "positions".into(),
            schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            primary_key: vec!["id".into()],
            on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec!["id".into()]))),
            base_path: dir.path().join("data").to_string_lossy().into_owned(),
            partition_column: None,
            vortex_config: VortexConfig {
                cdc_durability: CdcDurability::File,
                deletion_mode: DeletionMode::Key,
                compaction_background_interval_ms: 0,
                ..VortexConfig::default()
            },
        })
        .await
        .expect("create table"),
    );
    let backend = CayenneChangeSinkBackend::try_new(ChangeSinkContext::new(
        TableReference::bare("positions"),
        Arc::clone(&table) as Arc<dyn TableProvider>,
    ))
    .expect("native backend");
    let sink = ChangeSink::new(backend, ctx, &Handle::current(), 2);
    let keys = ListArray::try_new(
        Arc::new(Field::new("item", DataType::Utf8, false)),
        OffsetBuffer::new(vec![0_i32, 1, 2].into()),
        Arc::new(StringArray::from(vec!["id", "id"])),
        None,
    )
    .expect("keys");
    let record = RecordBatch::try_new(
        Arc::new(data_components::cdc::changes_schema(&table.schema())),
        vec![
            Arc::new(StringArray::from(vec!["c", "c"])),
            Arc::new(keys),
            Arc::new(StructArray::from(batch(&table, vec![1, 2]))),
        ],
    )
    .expect("source changes");
    let (reader, _) = object_store_occ::headlog::Reader::open(
        Arc::clone(&store),
        object_native_log(&store, "positions").await,
    )
    .await
    .expect("open head log");
    let before = reader.state().head.commit;

    let saver = Arc::clone(&catalog);
    let position: runtime_acceleration::change_sink::PositionWrite = Box::pin(async move {
        saver
            .set_sidecar_value("spice_sys_applied_lsn/positions", Some(b"lsn 7".to_vec()))
            .await
            .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)
    });
    let receipt = sink
        .reserve()
        .await
        .expect("capacity")
        .submit_with_position(
            ChangeBatch::cdc(CdcBatch::try_new(record).expect("CDC batch")),
            WriteOptions {
                recovery: Recovery::Durable,
                ..WriteOptions::default()
            },
            Some(position),
        )
        .expect("admit CDC")
        .wait()
        .await
        .expect("CDC receipt");
    receipt.published().await.expect("CDC publication");
    assert_eq!(receipt.durability, StorageDurability::Durable);

    let records = object_native_records_after(&store, "positions", before).await;
    let with_position: Vec<_> = records
        .iter()
        .filter(|kinds| kinds.contains(&"AdvanceSourcePosition"))
        .collect();
    assert_eq!(with_position.len(), 1, "records: {records:?}");
    assert!(
        with_position[0]
            .iter()
            .any(|kind| *kind != "AdvanceSourcePosition" && *kind != "AdvanceSeq"),
        "the position must commit with the data it covers, records: {records:?}"
    );
    assert_eq!(
        catalog.sidecar_value("spice_sys_applied_lsn/positions").await.expect("read"),
        Some(b"lsn 7".to_vec())
    );
}
