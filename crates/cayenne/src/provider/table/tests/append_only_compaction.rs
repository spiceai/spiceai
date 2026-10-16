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

//! Small-file compaction of a table with no primary key while it holds no position
//! deletes: the subset rewrite encodes without holding writers off, and its commit
//! reconciles the snapshot it read with every write that landed meanwhile
//! (`hold_writers_and_carry_appends`).

use super::*;
use arrow::array::{StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

/// `id` first, as [`scan_sorted_ids`] reads column 0.
fn id_value_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]))
}

/// Rows `start..start + n`, padded so each append is a file of some size.
fn id_value_rows(schema: &SchemaRef, start: i64, n: i64) -> RecordBatch {
    let ids: Vec<i64> = (start..start + n).collect();
    let values: Vec<String> = ids.iter().map(|i| format!("v_{i:0400}")).collect();
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(values)),
        ],
    )
    .expect("batch")
}

/// Inlining off, so each append settles as its own file, and no background pass.
fn one_file_per_append() -> VortexConfig {
    VortexConfig {
        inline_max_rows: 0,
        inline_max_bytes: 0,
        inline_max_buffer_bytes: 0,
        compaction_background_interval_ms: 3_600_000,
        ..VortexConfig::default()
    }
}

/// The ids the table holds once reopened from its catalog, as after a restart.
async fn ids_after_reopen(provider: &CayenneTableProvider, name: &str) -> Vec<i64> {
    let ctx = SessionContext::new();
    let reopened =
        CayenneTableProviderBuilder::new(Arc::clone(provider.catalog()), ctx.runtime_env())
            .open(name)
            .await
            .expect("reopen the table from its catalog");
    scan_sorted_ids(&reopened).await
}

/// What a pass samples before it lists the current snapshot, and the listing.
struct Listed {
    snapshot_id: String,
    generation: u64,
    files: Vec<(String, u64)>,
}

async fn list_current(provider: &CayenneTableProvider) -> Listed {
    let snapshot_id = provider.get_current_snapshot_id();
    let generation = provider.current_dir_generation.load(Ordering::Relaxed);
    let files = provider
        .list_snapshot_files_with_sizes(&snapshot_id)
        .await
        .expect("listed current snapshot files");
    Listed {
        snapshot_id,
        generation,
        files,
    }
}

/// Every file of the listing, as the picker's candidate.
fn whole_set(files: &[(String, u64)]) -> CompactionCandidate<&str> {
    CompactionCandidate {
        tier: crate::provider::compaction::Tier::Small,
        paths: files.iter().map(|(path, _)| path.as_str()).collect(),
        total_bytes: files.iter().map(|(_, size)| *size).sum(),
    }
}

/// An append that lands after a PK-less table's subset pass listed its files only adds
/// files to the source snapshot, so the pass carries them into its snapshot instead of
/// aborting: it commits, and every row — the appended ones included — stays visible,
/// now and after a reopen. Under steady ingest an aborting pass would never commit, and
/// the table's small files would pile up.
#[tokio::test]
async fn pk_less_subset_rewrite_carries_an_append_that_raced_it() {
    const NAME: &str = "pk_less_subset_carry";
    let schema = id_value_schema();
    let ctx = SessionContext::new();
    let (provider, _temp_dir) = create_cayenne_table_with_config(
        NAME,
        Arc::clone(&schema),
        one_file_per_append(),
        Vec::new(),
        ctx.runtime_env(),
    )
    .await;
    for i in 0..4 {
        insert_batch(&provider, id_value_rows(&schema, i * 50, 50)).await;
    }
    let listed = list_current(&provider).await;
    assert_eq!(listed.files.len(), 4, "fixture: one file per append");
    let candidate = subset_candidate_of_first_two(&listed.files);
    assert!(provider.can_subset_rewrite_current_small_files(&candidate, &listed.files));

    // Lands after the listing the pass picked from, as an append during its encode does.
    insert_batch(&provider, id_value_rows(&schema, 1_000, 50)).await;
    assert_ne!(
        provider.current_dir_generation.load(Ordering::Relaxed),
        listed.generation,
        "precondition: the append published into the snapshot the pass is rewriting"
    );

    assert!(
        provider
            .rewrite_current_snapshot_small_file_subset(
                &candidate,
                &listed.files,
                &listed.snapshot_id,
                listed.generation,
            )
            .await
            .expect("the subset rewrite runs"),
        "the pass carries the append instead of aborting"
    );
    assert_ne!(
        provider.get_current_snapshot_id(),
        listed.snapshot_id,
        "the pass published a new snapshot"
    );
    let expected: Vec<i64> = (0..200).chain(1_000..1_050).collect();
    assert_eq!(
        scan_sorted_ids(&provider).await,
        expected,
        "every row, the raced append's included, stays visible"
    );
    assert_eq!(
        ids_after_reopen(&provider, NAME).await,
        expected,
        "after a restart the table holds the same rows"
    );
}

/// A table with no primary key compacts by the subset rewrite while it holds no
/// position deletes: the pass re-encodes only the picked small files, carries the rest
/// as they are, and keeps every row. Once the table holds a position delete — file
/// scoped, so a subset rewrite of the file it targets would lose it — its small files
/// go back to the full rewrite, which keeps the deleted rows deleted. Regression guard for
/// the PK-less small-file compaction that re-encoded every row of the table, under the
/// write lock, on every pass.
#[tokio::test]
async fn pk_less_table_compacts_by_subset_until_it_holds_a_position_delete() {
    const NAME: &str = "pk_less_subset";
    let schema = id_value_schema();
    let ctx = SessionContext::new();
    let (provider, _temp_dir) = create_cayenne_table_with_config(
        NAME,
        Arc::clone(&schema),
        one_file_per_append(),
        Vec::new(),
        ctx.runtime_env(),
    )
    .await;
    assert!(
        provider.pk_deletion_strategy().is_position_based(),
        "fixture: a table with no primary key uses position deletes"
    );
    for i in 0..4 {
        insert_batch(&provider, id_value_rows(&schema, i * 50, 50)).await;
    }

    let listed = list_current(&provider).await;
    assert_eq!(listed.files.len(), 4, "fixture: one file per append");
    let candidate = subset_candidate_of_first_two(&listed.files);
    assert!(
        provider.can_subset_rewrite_current_small_files(&candidate, &listed.files),
        "a PK-less table holding no position deletes takes the subset rewrite"
    );
    assert!(
        provider
            .rewrite_current_snapshot_small_file_subset(
                &candidate,
                &listed.files,
                &listed.snapshot_id,
                listed.generation,
            )
            .await
            .expect("the subset rewrite runs"),
        "the subset rewrite commits"
    );
    assert_eq!(
        scan_sorted_ids(&provider).await,
        (0..200).collect::<Vec<i64>>(),
        "the subset rewrite keeps every row"
    );
    let after = list_current(&provider).await.files;
    assert_eq!(
        after.len(),
        3,
        "the two picked files merge into one and the other two are carried: {after:?}"
    );

    let delete_plan = provider
        .delete_from(
            &ctx.state(),
            vec![datafusion_expr::col("id").lt(datafusion_expr::lit(10_i64))],
        )
        .await
        .expect("filtered delete plan");
    datafusion::physical_plan::collect(delete_plan, ctx.task_ctx())
        .await
        .expect("filtered delete executed");
    let files = list_current(&provider).await.files;
    assert!(files.len() >= 3, "listed {} file(s)", files.len());
    let candidate = subset_candidate_of_first_two(&files);
    assert!(
        !provider.can_subset_rewrite_current_small_files(&candidate, &files),
        "a PK-less table holding a position delete must not take the subset rewrite"
    );
    assert!(
        provider
            .rewrite_current_snapshot_for_compaction()
            .await
            .expect("the full rewrite runs"),
        "the full rewrite commits"
    );
    assert_eq!(
        scan_sorted_ids(&provider).await,
        (10..200).collect::<Vec<i64>>(),
        "the deleted rows stay deleted through the full rewrite"
    );
}

/// A table with no primary key that holds no position deletes takes the subset rewrite
/// even when the candidate is every file it holds: the pass merges them all into one
/// file and keeps every row, now and after a reopen. Regression guard for the pass that
/// sent such a candidate to the full rewrite, which holds writers off for its whole
/// encode — under steady ingest the picker's candidate covers every file whenever fewer
/// files than its width have piled up, so ingest stalled behind most passes.
#[tokio::test]
async fn pk_less_table_compacts_its_whole_file_set_by_subset() {
    const NAME: &str = "pk_less_whole_set";
    let schema = id_value_schema();
    let ctx = SessionContext::new();
    let (provider, _temp_dir) = create_cayenne_table_with_config(
        NAME,
        Arc::clone(&schema),
        one_file_per_append(),
        Vec::new(),
        ctx.runtime_env(),
    )
    .await;
    for i in 0..4 {
        insert_batch(&provider, id_value_rows(&schema, i * 50, 50)).await;
    }

    let listed = list_current(&provider).await;
    assert_eq!(listed.files.len(), 4, "fixture: one file per append");
    let candidate = whole_set(&listed.files);
    assert!(
        provider.can_subset_rewrite_current_small_files(&candidate, &listed.files),
        "a PK-less table holding no position deletes takes the subset rewrite for its whole file set"
    );
    assert!(
        provider
            .rewrite_current_snapshot_small_file_subset(
                &candidate,
                &listed.files,
                &listed.snapshot_id,
                listed.generation,
            )
            .await
            .expect("the subset rewrite runs"),
        "the subset rewrite commits"
    );
    let expected: Vec<i64> = (0..200).collect();
    assert_eq!(
        scan_sorted_ids(&provider).await,
        expected,
        "the subset rewrite keeps every row"
    );
    let after = list_current(&provider).await.files;
    assert_eq!(after.len(), 1, "the four files merge into one: {after:?}");
    assert_eq!(
        ids_after_reopen(&provider, NAME).await,
        expected,
        "after a restart the table holds the same rows"
    );
}

/// An inline flush writes the inlined rows to a file in the current snapshot without
/// moving `current_dir_generation`. One that lands while the pass encodes is carried
/// into the pass's snapshot like an append: its rows stay visible, now and after a
/// reopen. A commit that looked for appends only when the generation moved published a
/// snapshot without the file, and the rows the flush had already removed from the
/// inline corpus were gone.
#[tokio::test]
async fn pk_less_subset_rewrite_carries_an_inline_flush_that_raced_it() {
    const NAME: &str = "pk_less_subset_inline_flush";
    let schema = id_value_schema();
    let ctx = SessionContext::new();
    let (provider, _temp_dir) = create_cayenne_table_with_config(
        NAME,
        Arc::clone(&schema),
        // Inlining on: an append above the row cap lands as a file, one below it inline.
        VortexConfig {
            compaction_background_interval_ms: 3_600_000,
            ..VortexConfig::default()
        },
        Vec::new(),
        ctx.runtime_env(),
    )
    .await;
    let file_rows = i64::try_from(crate::metadata::DEFAULT_INLINE_MAX_ROWS).expect("fits") + 100;
    for i in 0..4 {
        insert_batch(&provider, id_value_rows(&schema, i * file_rows, file_rows)).await;
    }
    let listed = list_current(&provider).await;
    assert_eq!(listed.files.len(), 4, "fixture: one file per append");
    let candidate = subset_candidate_of_first_two(&listed.files);

    // During the encode: a small append lands inline, and a flush writes it to a file.
    let racer = provider.clone_for_write();
    let racer_schema = Arc::clone(&schema);
    let listed_generation = listed.generation;
    *provider.test_pre_rewrite_commit_hook.lock() = Some(Box::new(move || {
        Box::pin(async move {
            insert_batch(&racer, id_value_rows(&racer_schema, 100_000, 10)).await;
            assert_eq!(
                racer.cached_inlined_row_count(),
                10,
                "precondition: the small append is inlined"
            );
            racer
                .checkpoint_inlined_data()
                .await
                .expect("flush the inline rows to a file");
            assert_eq!(
                racer.current_dir_generation.load(Ordering::Relaxed),
                listed_generation,
                "precondition: the flush published a file without moving the generation"
            );
        })
    }));

    assert!(
        provider
            .rewrite_current_snapshot_small_file_subset(
                &candidate,
                &listed.files,
                &listed.snapshot_id,
                listed.generation,
            )
            .await
            .expect("the subset rewrite runs"),
        "the pass commits, carrying the flushed file"
    );
    assert!(
        provider.test_pre_rewrite_commit_hook.lock().is_none(),
        "the race ran inside the pass"
    );
    let expected: Vec<i64> = (0..4 * file_rows).chain(100_000..100_010).collect();
    assert_eq!(
        scan_sorted_ids(&provider).await,
        expected,
        "the flushed rows stay visible"
    );
    assert_eq!(
        ids_after_reopen(&provider, NAME).await,
        expected,
        "after a restart the table holds the same rows"
    );
}

/// A file-unlinking retention delete removes a file the pass already read (the pass
/// holds a link to it, so the file outlives the unlink). The pass must not publish its
/// rows: it aborts, and the expired rows stay deleted, now and after a reopen. A commit
/// that only added files it had not listed published the re-encoded expired rows, and
/// the delete was undone.
#[tokio::test]
async fn pk_less_subset_rewrite_aborts_when_retention_unlinks_a_file_it_read() {
    const NAME: &str = "pk_less_subset_retention_unlink";
    let ctx = SessionContext::new();
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let metadata_dir = temp_dir.path().join("metadata");
    let data_dir = temp_dir.path().join("data");
    std::fs::create_dir_all(&metadata_dir).expect("metadata dir created");
    let catalog = Arc::new(
        CayenneCatalog::new(format!(
            "sqlite://{}/cayenne.db",
            metadata_dir
                .to_str()
                .expect("metadata path should be UTF-8")
        ))
        .expect("catalog created"),
    ) as Arc<dyn MetadataCatalog>;
    catalog.init().await.expect("catalog initialized");
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]));
    // A period no row reaches, so only the explicit delete below expires rows.
    let retention = crate::TimeRetentionFilterBuilder::try_new("event_time", u64::MAX, &schema)
        .expect("retention builder");
    let provider = CayenneTableProviderBuilder::new(catalog, ctx.runtime_env())
        .with_time_retention_filter_builder(retention)
        .create(CreateTableOptions {
            table_name: NAME.to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec![],
            on_conflict: None,
            base_path: data_dir.to_string_lossy().into_owned(),
            partition_column: None,
            vortex_config: one_file_per_append(),
        })
        .await
        .expect("retention table created");
    // Four files; every row of the first is at time 1, the others at 10 and later.
    for (i, time) in [1_i64, 10, 20, 30].into_iter().enumerate() {
        let start = i64::try_from(i).expect("fits") * 50;
        insert_batch(
            &provider,
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from((start..start + 50).collect::<Vec<_>>())),
                    Arc::new(TimestampNanosecondArray::from(vec![time; 50])),
                ],
            )
            .expect("batch"),
        )
        .await;
    }
    let listed = list_current(&provider).await;
    assert_eq!(listed.files.len(), 4, "fixture: one file per append");
    let candidate = whole_set(&listed.files);
    assert!(provider.can_subset_rewrite_current_small_files(&candidate, &listed.files));

    // During the encode: the retention-pattern delete unlinks the expired file.
    let racer = provider.clone_for_write();
    let racer_ctx = ctx.clone();
    *provider.test_pre_rewrite_commit_hook.lock() = Some(Box::new(move || {
        Box::pin(async move {
            let plan = racer
                .delete_from(
                    &racer_ctx.state(),
                    vec![datafusion_expr::col("event_time").lt(datafusion_expr::lit(
                        ScalarValue::TimestampNanosecond(Some(2), None),
                    ))],
                )
                .await
                .expect("retention delete plan");
            let output = datafusion::physical_plan::collect(plan, racer_ctx.task_ctx())
                .await
                .expect("retention delete executed");
            let deleted = output[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .expect("delete count is UInt64")
                .value(0);
            assert_eq!(deleted, 50, "precondition: the expired file is deleted");
            assert!(
                !racer.has_rows_hidden_from_file_statistics(),
                "precondition: the delete unlinked the file rather than hiding its rows"
            );
        })
    }));

    assert!(
        !provider
            .rewrite_current_snapshot_small_file_subset(
                &candidate,
                &listed.files,
                &listed.snapshot_id,
                listed.generation,
            )
            .await
            .expect("the subset rewrite runs"),
        "the pass aborts: a file it read is gone"
    );
    assert!(
        provider.test_pre_rewrite_commit_hook.lock().is_none(),
        "the race ran inside the pass"
    );
    assert_eq!(
        provider.get_current_snapshot_id(),
        listed.snapshot_id,
        "the pass published nothing"
    );
    let expected: Vec<i64> = (50..200).collect();
    assert_eq!(
        scan_sorted_ids(&provider).await,
        expected,
        "the expired rows stay deleted"
    );
    assert_eq!(
        ids_after_reopen(&provider, NAME).await,
        expected,
        "after a restart the expired rows are still deleted"
    );
}

/// A pipelined append reserves its write (stage A) and publishes it later (stage B), both
/// under `write_lock`, into the snapshot it reserved against. A pass that reaches its
/// commit between the two waits for the publish, then carries the append's files: the
/// append succeeds and every row stays visible. A commit that replaced the snapshot first
/// left the publish with a vanished target, which failed the append and left the table
/// refusing every later write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pk_less_subset_rewrite_waits_out_a_pipelined_append() {
    const NAME: &str = "pk_less_subset_pipelined";
    let schema = id_value_schema();
    let ctx = SessionContext::new();
    let (provider, _temp_dir) = create_cayenne_table_with_config(
        NAME,
        Arc::clone(&schema),
        VortexConfig {
            cdc_durability: crate::metadata::CdcDurability::File,
            ..one_file_per_append()
        },
        Vec::new(),
        ctx.runtime_env(),
    )
    .await;
    for i in 0..4 {
        insert_batch(&provider, id_value_rows(&schema, i * 50, 50)).await;
    }
    let listed = list_current(&provider).await;
    assert_eq!(listed.files.len(), 4, "fixture: one file per append");
    let candidate = subset_candidate_of_first_two(&listed.files);

    let staged = provider
        .write_cdc_append_stream(
            single_batch_stream(id_value_rows(&schema, 1_000, 50)),
            &ctx.task_ctx(),
        )
        .await
        .expect("stage the append");
    assert!(
        staged.has_pending_finalize(),
        "precondition: the append is pipelined, its publish still to come"
    );

    // Stage B runs after the pass has had its chance to commit: a commit that waits for the
    // publish leaves the snapshot alone meanwhile, and one that does not replaces it at once.
    let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
    let watcher = provider.clone_for_write();
    let source_snapshot_id = listed.snapshot_id.clone();
    *provider.test_pre_rewrite_commit_hook.lock() = Some(Box::new(move || {
        Box::pin(async move {
            tokio::spawn(async move {
                let replaced_while_in_flight =
                    tokio::time::timeout(Duration::from_millis(500), async {
                        while watcher.get_current_snapshot_id() == source_snapshot_id {
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    })
                    .await
                    .is_ok();
                let _ = finished_tx.send((replaced_while_in_flight, staged.finish().await));
            });
        })
    }));

    assert!(
        provider
            .rewrite_current_snapshot_small_file_subset(
                &candidate,
                &listed.files,
                &listed.snapshot_id,
                listed.generation,
            )
            .await
            .expect("the subset rewrite runs"),
        "the pass commits after the publish"
    );
    let (replaced_while_in_flight, published) = finished_rx.await.expect("the publish ran");
    assert!(
        !replaced_while_in_flight,
        "the pass replaced the snapshot while the append was in flight"
    );
    assert_eq!(
        published.expect("the pipelined append publishes"),
        50,
        "the append publishes its rows"
    );

    let expected: Vec<i64> = (0..200).chain(1_000..1_050).collect();
    assert_eq!(
        scan_sorted_ids(&provider).await,
        expected,
        "every row, the pipelined append's included, stays visible"
    );
    insert_batch(&provider, id_value_rows(&schema, 2_000, 50)).await;
    let expected: Vec<i64> = expected.into_iter().chain(2_000..2_050).collect();
    assert_eq!(
        scan_sorted_ids(&provider).await,
        expected,
        "the table takes later writes"
    );
    assert_eq!(
        ids_after_reopen(&provider, NAME).await,
        expected,
        "after a restart the table holds the same rows"
    );
}

/// A pass that found nothing to merge retires only the trigger credits its listing covered:
/// credits an append added after the pass sampled them stay, so the appended files are still
/// owed a pass, and nothing is retired once the snapshot was replaced.
#[tokio::test]
async fn a_declined_pass_keeps_the_credits_of_appends_that_landed_during_it() {
    let schema = id_value_schema();
    let ctx = SessionContext::new();
    let (provider, _temp_dir) = create_cayenne_table_with_config(
        "pk_less_declined_credits",
        Arc::clone(&schema),
        one_file_per_append(),
        Vec::new(),
        ctx.runtime_env(),
    )
    .await;
    insert_batch(&provider, id_value_rows(&schema, 0, 50)).await;
    let snapshot_id = provider.get_current_snapshot_id();
    let counter = &provider.new_files_since_last_compaction;

    // Five credits when the pass sampled them, two appends' worth more by the time it declined.
    counter.store(7, Ordering::Release);
    provider
        .retire_listed_small_file_credits(&snapshot_id, 5)
        .await;
    assert_eq!(
        counter.load(Ordering::Acquire),
        2,
        "the credits counted after the sample stay"
    );

    // Fewer credits than sampled (another pass retired some): none go negative.
    provider
        .retire_listed_small_file_credits(&snapshot_id, 5)
        .await;
    assert_eq!(counter.load(Ordering::Acquire), 0);

    // A replacement restarts the count: a pass over the replaced snapshot retires nothing.
    counter.store(3, Ordering::Release);
    provider
        .retire_listed_small_file_credits("a-replaced-snapshot", 3)
        .await;
    assert_eq!(
        counter.load(Ordering::Acquire),
        3,
        "nothing is retired once the snapshot changed"
    );
}
