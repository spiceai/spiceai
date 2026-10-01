/*
Copyright 2026 The Spice.ai OSS Authors

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

//! A file-backed overwrite whose incoming data repeats keys across record
//! batches keeps the last copy of each key under every upsert policy, through
//! its whole lifecycle, in both deletion modes (regression tests for #14578).

use super::*;
use crate::metadata::DeletionMode;
use crate::provider::key_conflicts::UpsertDedup;
use arrow::array::{AsArray, StringArray};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("value", DataType::Utf8, false),
    ]))
}

fn batch(rows: &[(i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|(id, _)| *id))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|(_, v)| *v))),
        ],
    )
    .expect("batch")
}

async fn write(
    provider: &CayenneTableProvider,
    op: InsertOp,
    batches: Vec<RecordBatch>,
) -> datafusion_common::Result<()> {
    let ctx = SessionContext::new();
    let source = MemorySourceConfig::try_new_exec(&[batches], schema(), None)?;
    let plan = provider.insert_into(&ctx.state(), source, op).await?;
    collect(plan, ctx.task_ctx()).await.map(|_| ())
}

async fn visible(provider: &CayenneTableProvider) -> (Vec<(i64, String)>, i64) {
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(provider.clone_for_write()))
        .expect("register");
    let batches = ctx
        .sql("SELECT id, value FROM t ORDER BY id")
        .await
        .expect("query")
        .collect()
        .await
        .expect("collect");
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_primitive::<arrow::datatypes::Int64Type>();
        let values = batch.column(1).as_string::<i32>();
        for row in 0..batch.num_rows() {
            rows.push((ids.value(row), values.value(row).to_string()));
        }
    }
    let count = ctx
        .sql("SELECT COUNT(*) FROM t")
        .await
        .expect("query")
        .collect()
        .await
        .expect("collect")[0]
        .column(0)
        .as_primitive::<arrow::datatypes::Int64Type>()
        .value(0);
    (rows, count)
}

fn owned(rows: &[(i64, &str)]) -> Vec<(i64, String)> {
    rows.iter().map(|(id, v)| (*id, (*v).to_string())).collect()
}

async fn table(
    mode: DeletionMode,
    dedup: UpsertDedup,
) -> (
    CayenneTableProvider,
    Arc<dyn MetadataCatalog>,
    Arc<RuntimeEnv>,
    TempDir,
) {
    let runtime_env = SessionContext::new().runtime_env();
    let (mut provider, catalog, dir) = create_cdc_table_with_schema(
        "t",
        Arc::clone(&runtime_env),
        schema(),
        vec!["id".to_string()],
        VortexConfig {
            deletion_mode: mode,
            inline_max_rows: 0,
            stream_publish_interval_ms: 0,
            compaction_background_interval_ms: 3_600_000,
            compaction_trigger_protected_snapshots: 2,
            ..VortexConfig::default()
        },
        OnConflict::Upsert(
            datafusion_table_providers::util::column_reference::ColumnReference::new(vec![
                "id".to_string(),
            ]),
        ),
    )
    .await;
    provider.upsert_dedup = dedup;
    // Flush the collapse window after every batch, so repeats across batches
    // exercise the layers.
    provider.collapse_window_bytes = 1;
    (provider, catalog, runtime_env, dir)
}

async fn reopen(
    catalog: &Arc<dyn MetadataCatalog>,
    runtime_env: &Arc<RuntimeEnv>,
    dedup: UpsertDedup,
) -> CayenneTableProvider {
    CayenneTableProviderBuilder::new(Arc::clone(catalog), Arc::clone(runtime_env))
        .with_upsert_dedup(dedup)
        .open("t")
        .await
        .expect("reopen")
}

#[tokio::test(flavor = "multi_thread")]
async fn streaming_append_keeps_last_copy_through_reopen_and_compaction() {
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        for dedup in [
            UpsertDedup::None,
            UpsertDedup::DropIdentical,
            UpsertDedup::KeepLast,
        ] {
            let label = format!("{mode:?}/{dedup:?}");
            let (provider, catalog, runtime_env, _dir) = table(mode, dedup).await;
            write(
                &provider,
                InsertOp::Append,
                vec![batch(&[(1, "old"), (9, "old")])],
            )
            .await
            .expect("seed");
            write(&provider, InsertOp::Append, repeated_across_batches())
                .await
                .expect("streaming append");
            let expected = (
                owned(&[
                    (1, "c"),
                    (2, "c"),
                    (3, "a"),
                    (4, "b"),
                    (5, "c"),
                    (6, "d"),
                    (9, "old"),
                ]),
                7,
            );
            let check = |stage: &str, actual: (Vec<(i64, String)>, i64)| {
                eprintln!("{label} {stage}: {:?} COUNT(*) {}", actual.0, actual.1);
                assert_eq!(actual, expected, "{label} {stage}");
            };
            check("after append", visible(&provider).await);
            let provider = reopen(&catalog, &runtime_env, dedup).await;
            check("after reopen", visible(&provider).await);
            provider
                .compact_protected_snapshots_subset(8)
                .await
                .expect("merge");
            check("after protected merge", visible(&provider).await);
            provider
                .sort_and_rewrite_data(64 * 1024 * 1024)
                .await
                .expect("rewrite");
            check("after full rewrite", visible(&provider).await);
            let provider = reopen(&catalog, &runtime_env, dedup).await;
            check("after second reopen", visible(&provider).await);
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn streaming_append_failure_leaves_previous_rows() {
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        let (provider, catalog, runtime_env, _dir) = table(mode, UpsertDedup::None).await;
        write(&provider, InsertOp::Append, vec![batch(&[(9, "old")])])
            .await
            .expect("seed");
        let error = write(
            &provider,
            InsertOp::Append,
            vec![
                batch(&[(1, "a"), (2, "a")]),
                batch(&[(2, "b")]),
                batch(&[(5, "c"), (5, "d")]),
            ],
        )
        .await
        .expect_err("in-batch duplicate");
        eprintln!("{mode:?} failed append: {error}");
        assert_eq!(visible(&provider).await, (owned(&[(9, "old")]), 1));
        let provider = reopen(&catalog, &runtime_env, UpsertDedup::None).await;
        assert_eq!(visible(&provider).await, (owned(&[(9, "old")]), 1));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn streaming_append_drop_keeps_first_copy() {
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        let runtime_env = SessionContext::new().runtime_env();
        let (provider, catalog, _dir) = create_cdc_table_with_schema(
            "t",
            Arc::clone(&runtime_env),
            schema(),
            vec!["id".to_string()],
            VortexConfig {
                deletion_mode: mode,
                inline_max_rows: 0,
                stream_publish_interval_ms: 0,
                compaction_background_interval_ms: 3_600_000,
                compaction_trigger_protected_snapshots: 2,
                ..VortexConfig::default()
            },
            OnConflict::DoNothing(
                datafusion_table_providers::util::column_reference::ColumnReference::new(vec![
                    "id".to_string(),
                ]),
            ),
        )
        .await;
        write(&provider, InsertOp::Append, vec![batch(&[(9, "old")])])
            .await
            .expect("seed");
        write(&provider, InsertOp::Append, repeated_across_batches())
            .await
            .expect("streaming drop append");
        let expected = (
            owned(&[
                (1, "a"),
                (2, "a"),
                (3, "a"),
                (4, "b"),
                (5, "c"),
                (6, "d"),
                (9, "old"),
            ]),
            7,
        );
        assert_eq!(visible(&provider).await, expected);
        let provider = reopen(&catalog, &runtime_env, UpsertDedup::None).await;
        assert_eq!(visible(&provider).await, expected);
        provider
            .compact_protected_snapshots_subset(8)
            .await
            .expect("merge");
        assert_eq!(visible(&provider).await, expected);
        provider
            .sort_and_rewrite_data(64 * 1024 * 1024)
            .await
            .expect("rewrite");
        assert_eq!(visible(&provider).await, expected);
        let provider = reopen(&catalog, &runtime_env, UpsertDedup::None).await;
        assert_eq!(visible(&provider).await, expected);
    }
}

/// `[(1,a),(2,a),(3,a)]`, `[(4,b),(2,b)]`, `[(2,c),(5,c),(1,c)]`, `[(6,d)]`: key 2
/// repeats across three batches and key 1 across two, so the overwrite writes a
/// main snapshot and two layers above it.
fn repeated_across_batches() -> Vec<RecordBatch> {
    vec![
        batch(&[(1, "a"), (2, "a"), (3, "a")]),
        batch(&[(4, "b"), (2, "b")]),
        batch(&[(2, "c"), (5, "c"), (1, "c")]),
        batch(&[(6, "d")]),
    ]
}

fn last_copies() -> Vec<(i64, String)> {
    owned(&[(1, "c"), (2, "c"), (3, "a"), (4, "b"), (5, "c"), (6, "d")])
}

#[tokio::test(flavor = "multi_thread")]
async fn overwrite_keeps_the_last_copy_across_batches_through_its_lifecycle() {
    let mut failures = Vec::new();
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        for dedup in [
            UpsertDedup::None,
            UpsertDedup::DropIdentical,
            UpsertDedup::KeepLast,
        ] {
            let label = format!("{mode:?}/{dedup:?}");
            let (provider, catalog, runtime_env, _dir) = table(mode, dedup).await;
            write(&provider, InsertOp::Overwrite, vec![batch(&[(9, "old")])])
                .await
                .expect("seed");
            write(&provider, InsertOp::Overwrite, repeated_across_batches())
                .await
                .expect("layered overwrite");
            let expected = last_copies();
            let mut check =
                |stage: &str, (rows, count): (Vec<(i64, String)>, i64), layers: usize| {
                    let ok = rows == expected && count == 6;
                    eprintln!(
                        "{label} {stage}: protected layers {layers}, rows {}, COUNT(*) {count}: {}",
                        rows.len(),
                        if ok { "ok" } else { "WRONG" }
                    );
                    if !ok {
                        failures.push(format!("{label} {stage}: {rows:?} COUNT(*) {count}"));
                    }
                };
            check(
                "after overwrite",
                visible(&provider).await,
                provider.protected_snapshot_ids().len(),
            );
            let provider = reopen(&catalog, &runtime_env, dedup).await;
            check(
                "after reopen",
                visible(&provider).await,
                provider.protected_snapshot_ids().len(),
            );
            let merged = provider
                .compact_protected_snapshots_subset(8)
                .await
                .expect("merge");
            check(
                &format!("after protected merge ({merged})"),
                visible(&provider).await,
                provider.protected_snapshot_ids().len(),
            );
            provider
                .sort_and_rewrite_data(64 * 1024 * 1024)
                .await
                .expect("full rewrite");
            check(
                "after full rewrite",
                visible(&provider).await,
                provider.protected_snapshot_ids().len(),
            );
            let provider = reopen(&catalog, &runtime_env, dedup).await;
            check(
                "after second reopen",
                visible(&provider).await,
                provider.protected_snapshot_ids().len(),
            );
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// A layered overwrite publishes one snapshot: a position-deletion table hides
/// the superseded copies by position, and a key-deletion table drops them from
/// its files, leaving no deletes at all.
fn assert_layered_shape(provider: &CayenneTableProvider, mode: DeletionMode) {
    let position_deleted: u64 = provider
        .pk_deletion_strategy
        .position_cache()
        .load()
        .values()
        .map(|deletes| deletes.len())
        .sum();
    assert!(
        provider.protected_snapshot_ids().is_empty(),
        "{mode:?}: layers"
    );
    assert!(
        !provider.has_pending_deletions(),
        "{mode:?}: key tombstones"
    );
    let expected = if mode == DeletionMode::Position { 3 } else { 0 };
    assert_eq!(position_deleted, expected, "{mode:?}: position deletes");
}

/// After a layered overwrite on a position-deletion table, position capture and
/// a later upsert of the repeated keys still leave exactly one row per key.
#[tokio::test(flavor = "multi_thread")]
async fn position_capture_after_a_layered_overwrite_locates_the_live_copy() {
    let (provider, _catalog, _runtime_env, _dir) =
        table(DeletionMode::Position, UpsertDedup::None).await;
    write(&provider, InsertOp::Overwrite, repeated_across_batches())
        .await
        .expect("layered overwrite");
    // Rebuild the keyset from a scan, then locate its keys by read-back.
    write(&provider, InsertOp::Append, vec![batch(&[(7, "x")])])
        .await
        .expect("warm the keyset");
    provider.run_position_capture().await.expect("capture");
    write(
        &provider,
        InsertOp::Append,
        vec![batch(&[(2, "e"), (1, "e")])],
    )
    .await
    .expect("upsert superseded keys");
    let (rows, count) = visible(&provider).await;
    assert_eq!(
        rows,
        owned(&[
            (1, "e"),
            (2, "e"),
            (3, "a"),
            (4, "b"),
            (5, "c"),
            (6, "d"),
            (7, "x")
        ])
    );
    assert_eq!(count, 7);
}

/// A composite, non-`Int64` key must be identified the same way by the write
/// and by the read-back that locates its superseded copies.
#[tokio::test(flavor = "multi_thread")]
async fn a_layered_overwrite_resolves_a_string_key_in_both_modes() {
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let rows_of = |rows: &[(&str, i64)]| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from_iter_values(rows.iter().map(|(k, _)| *k))),
                    Arc::new(Int64Array::from_iter_values(rows.iter().map(|(_, v)| *v))),
                ],
            )
            .expect("batch")
        };
        let runtime_env = SessionContext::new().runtime_env();
        let (mut provider, _catalog, _dir) = create_cdc_table_with_schema(
            "t",
            Arc::clone(&runtime_env),
            Arc::clone(&schema),
            vec!["k".to_string()],
            VortexConfig {
                deletion_mode: mode,
                inline_max_rows: 0,
                compaction_background_interval_ms: 3_600_000,
                ..VortexConfig::default()
            },
            OnConflict::Upsert(
                datafusion_table_providers::util::column_reference::ColumnReference::new(vec![
                    "k".to_string(),
                ]),
            ),
        )
        .await;
        provider.collapse_window_bytes = 1;
        let ctx = SessionContext::new();
        let source = MemorySourceConfig::try_new_exec(
            &[vec![
                rows_of(&[("a", 1), ("b", 1)]),
                rows_of(&[("a", 2)]),
                rows_of(&[("b", 3), ("c", 3)]),
            ]],
            Arc::clone(&schema),
            None,
        )
        .expect("source");
        let plan = provider
            .insert_into(&ctx.state(), source, InsertOp::Overwrite)
            .await
            .expect("plan");
        collect(plan, ctx.task_ctx()).await.expect("overwrite");
        ctx.register_table("t", Arc::new(provider.clone_for_write()))
            .expect("register");
        let batches = ctx
            .sql("SELECT k, v FROM t ORDER BY k")
            .await
            .expect("query")
            .collect()
            .await
            .expect("collect");
        let mut rows = Vec::new();
        for batch in &batches {
            let keys = batch.column(0).as_string::<i32>();
            let values = batch
                .column(1)
                .as_primitive::<arrow::datatypes::Int64Type>();
            for row in 0..batch.num_rows() {
                rows.push((keys.value(row).to_string(), values.value(row)));
            }
        }
        assert_eq!(
            rows,
            vec![
                ("a".to_string(), 2),
                ("b".to_string(), 3),
                ("c".to_string(), 3)
            ],
            "{mode:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_layered_overwrite_is_superseded_by_a_later_upsert_and_a_later_overwrite() {
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        let (provider, catalog, runtime_env, _dir) = table(mode, UpsertDedup::None).await;
        write(&provider, InsertOp::Overwrite, repeated_across_batches())
            .await
            .expect("layered overwrite");
        assert_layered_shape(&provider, mode);
        write(
            &provider,
            InsertOp::Append,
            vec![batch(&[(2, "e"), (7, "e")])],
        )
        .await
        .expect("upsert");
        let (rows, count) = visible(&provider).await;
        assert_eq!(
            rows,
            owned(&[
                (1, "c"),
                (2, "e"),
                (3, "a"),
                (4, "b"),
                (5, "c"),
                (6, "d"),
                (7, "e")
            ]),
            "{mode:?}: after upsert"
        );
        assert_eq!(count, 7, "{mode:?}: COUNT(*) after upsert");

        write(&provider, InsertOp::Overwrite, vec![batch(&[(1, "z")])])
            .await
            .expect("plain overwrite");
        assert_eq!(
            visible(&provider).await,
            (owned(&[(1, "z")]), 1),
            "{mode:?}: replaced"
        );
        assert!(
            provider.protected_snapshot_ids().is_empty(),
            "{mode:?}: layers cleared"
        );
        let provider = reopen(&catalog, &runtime_env, UpsertDedup::None).await;
        assert_eq!(
            visible(&provider).await,
            (owned(&[(1, "z")]), 1),
            "{mode:?}: reopened"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_layered_overwrite_leaves_the_previous_table() {
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        let (provider, catalog, runtime_env, _dir) = table(mode, UpsertDedup::None).await;
        write(&provider, InsertOp::Overwrite, vec![batch(&[(9, "old")])])
            .await
            .expect("seed");
        // The third batch repeats key 5 within itself, which plain `upsert` rejects,
        // after the second batch has already opened a layer.
        let error = write(
            &provider,
            InsertOp::Overwrite,
            vec![
                batch(&[(1, "a"), (2, "a")]),
                batch(&[(2, "b")]),
                batch(&[(5, "c"), (5, "d")]),
            ],
        )
        .await
        .expect_err("a repeat within one batch fails plain upsert");
        assert!(
            error
                .to_string()
                .contains("uniqueness constraint on column(s): 'id'"),
            "{mode:?}: {error}"
        );
        assert_eq!(
            visible(&provider).await,
            (owned(&[(9, "old")]), 1),
            "{mode:?}"
        );
        assert!(
            provider.protected_snapshot_ids().is_empty(),
            "{mode:?}: no layers"
        );
        let provider = reopen(&catalog, &runtime_env, UpsertDedup::None).await;
        assert_eq!(
            visible(&provider).await,
            (owned(&[(9, "old")]), 1),
            "{mode:?}: reopened"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_keeps_the_first_copy_across_batches() {
    let runtime_env = SessionContext::new().runtime_env();
    let (provider, _catalog, _dir) = create_cdc_table_with_schema(
        "t",
        Arc::clone(&runtime_env),
        schema(),
        vec!["id".to_string()],
        VortexConfig {
            inline_max_rows: 0,
            compaction_background_interval_ms: 3_600_000,
            ..VortexConfig::default()
        },
        OnConflict::DoNothing(
            datafusion_table_providers::util::column_reference::ColumnReference::new(vec![
                "id".to_string(),
            ]),
        ),
    )
    .await;
    write(&provider, InsertOp::Overwrite, repeated_across_batches())
        .await
        .expect("overwrite");
    assert_eq!(
        visible(&provider).await,
        (
            owned(&[(1, "a"), (2, "a"), (3, "a"), (4, "b"), (5, "c"), (6, "d")]),
            6
        )
    );
    assert!(provider.protected_snapshot_ids().is_empty());
}

/// Repeats the collapse window holds are resolved in memory: the refresh
/// publishes one snapshot, with no layer and no tombstone.
#[tokio::test(flavor = "multi_thread")]
async fn repeats_within_the_collapse_window_publish_one_snapshot() {
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        let (mut provider, _catalog, _runtime_env, _dir) = table(mode, UpsertDedup::None).await;
        provider.collapse_window_bytes =
            super::super::super::overwrite_layers::COLLAPSE_WINDOW_BYTES;
        write(&provider, InsertOp::Overwrite, repeated_across_batches())
            .await
            .expect("overwrite");
        assert_eq!(visible(&provider).await, (last_copies(), 6), "{mode:?}");
        assert!(
            provider.protected_snapshot_ids().is_empty(),
            "{mode:?}: no layers"
        );
        assert!(!provider.has_pending_deletions(), "{mode:?}: no tombstones");
    }
}

/// A superseded copy's values never reach an aggregate, whether it reads the
/// rows or the table's statistics. A key-deletion table keeps its statistics
/// exact, since its superseded copies leave its files.
#[tokio::test(flavor = "multi_thread")]
async fn aggregates_after_a_layered_overwrite_ignore_superseded_copies() {
    let mut failures = Vec::new();
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        let (provider, catalog, runtime_env, _dir) = table(mode, UpsertDedup::None).await;
        write(
            &provider,
            InsertOp::Overwrite,
            vec![
                batch(&[(1, "m"), (2, "zzz"), (3, "m")]),
                batch(&[(2, "aaa"), (4, "m")]),
            ],
        )
        .await
        .expect("layered overwrite");
        for (stage, provider) in [
            ("after overwrite", provider.clone_for_write()),
            (
                "after reopen",
                reopen(&catalog, &runtime_env, UpsertDedup::None).await,
            ),
        ] {
            let ctx = SessionContext::new();
            ctx.register_table("t", Arc::new(provider))
                .expect("register");
            let df = ctx
                .sql("SELECT MIN(value), MAX(value), COUNT(*), COUNT(value) FROM t")
                .await
                .expect("query");
            let plan = arrow::util::pretty::pretty_format_batches(
                &df.clone()
                    .explain(false, false)
                    .expect("explain")
                    .collect()
                    .await
                    .expect("plan"),
            )
            .expect("format")
            .to_string();
            let from_statistics = plan.contains("PlaceholderRowExec");
            if mode == DeletionMode::Key && !from_statistics {
                failures.push(format!(
                    "{mode:?} {stage}: scanned instead of using statistics"
                ));
            }
            let batches = df.collect().await.expect("collect");
            let row = &batches[0];
            let observed = (
                row.column(0).as_string::<i32>().value(0).to_string(),
                row.column(1).as_string::<i32>().value(0).to_string(),
                row.column(2)
                    .as_primitive::<arrow::datatypes::Int64Type>()
                    .value(0),
                row.column(3)
                    .as_primitive::<arrow::datatypes::Int64Type>()
                    .value(0),
            );
            let expected = ("aaa".to_string(), "m".to_string(), 4, 4);
            eprintln!("{mode:?} {stage}: {observed:?}, from statistics: {from_statistics}");
            if observed != expected {
                failures.push(format!(
                    "{mode:?} {stage}: {observed:?}, expected {expected:?}"
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// A transaction's staged upsert keeps the last copy of a key it repeats across
/// record batches, and supersedes the stored copy exactly once.
#[tokio::test(flavor = "multi_thread")]
async fn a_staged_upsert_keeps_the_last_copy_across_batches() {
    let mut failures = Vec::new();
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        let (provider, catalog, runtime_env, _dir) = table(mode, UpsertDedup::None).await;
        write(
            &provider,
            InsertOp::Overwrite,
            vec![batch(&[(1, "old"), (9, "old")])],
        )
        .await
        .expect("seed");
        let token = provider.transaction_write_token().await;
        let ctx = SessionContext::new();
        let source = MemorySourceConfig::try_new_exec(
            &[vec![
                batch(&[(1, "a"), (2, "zzz")]),
                batch(&[(2, "b"), (3, "b")]),
                batch(&[(1, "c")]),
            ]],
            schema(),
            None,
        )
        .expect("source");
        let stream = source.execute(0, ctx.task_ctx()).expect("stream");
        let staged = provider
            .begin_staged_upsert_occ(token, stream, 4)
            .await
            .expect("stage");
        staged
            .commit(std::collections::HashSet::new(), true)
            .await
            .expect("commit");
        let expected = owned(&[(1, "c"), (2, "b"), (3, "b"), (9, "old")]);
        for (stage, provider) in [
            ("after commit", provider.clone_for_write()),
            ("after reopen", reopen(&catalog, &runtime_env, UpsertDedup::None).await),
        ] {
            let (rows, count) = visible(&provider).await;
            let ctx = SessionContext::new();
            ctx.register_table("t", Arc::new(provider)).expect("register");
            let max = ctx
                .sql("SELECT MAX(value) FROM t")
                .await
                .expect("query")
                .collect()
                .await
                .expect("collect")[0]
                .column(0)
                .as_string::<i32>()
                .value(0)
                .to_string();
            eprintln!("{mode:?} {stage}: {rows:?} COUNT(*) {count} MAX {max}");
            if rows != expected || count != 4 || max != "old" {
                failures.push(format!("{mode:?} {stage}: {rows:?} COUNT(*) {count} MAX {max}"));
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}
