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

//! Repeated-key policies across batches, publication, restart, and compaction
//! in both deletion modes.

use super::*;
use crate::metadata::DeletionMode;
use crate::provider::pk_index::{CachedPkIndex, CachedPkKeyset};
use arrow::array::{AsArray, StringArray};

mod policy_contract;

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

/// A batch whose second row has no primary key, which fails every write.
fn null_key_batch() -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vec![Some(5), None])),
            Arc::new(StringArray::from_iter_values(["c", "d"])),
        ],
    )
    .expect("batch")
}

fn owned(rows: &[(i64, &str)]) -> Vec<(i64, String)> {
    rows.iter().map(|(id, v)| (*id, (*v).to_string())).collect()
}

async fn table(
    mode: DeletionMode,
) -> (
    CayenneTableProvider,
    Arc<dyn MetadataCatalog>,
    Arc<RuntimeEnv>,
    TempDir,
) {
    let runtime_env = SessionContext::new().runtime_env();
    let (provider, catalog, dir) = create_cdc_table_with_schema(
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
    (provider, catalog, runtime_env, dir)
}

async fn reopen(
    catalog: &Arc<dyn MetadataCatalog>,
    runtime_env: &Arc<RuntimeEnv>,
) -> CayenneTableProvider {
    CayenneTableProviderBuilder::new(Arc::clone(catalog), Arc::clone(runtime_env))
        .open("t")
        .await
        .expect("reopen")
}

#[tokio::test(flavor = "multi_thread")]
async fn streaming_append_resolves_copies_through_reopen_and_compaction() {
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        let label = format!("{mode:?}");
        let (provider, catalog, runtime_env, _dir) = table(mode).await;
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
        let provider = reopen(&catalog, &runtime_env).await;
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
        let provider = reopen(&catalog, &runtime_env).await;
        check("after second reopen", visible(&provider).await);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn streaming_append_failure_leaves_previous_rows() {
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        let (provider, catalog, runtime_env, _dir) = table(mode).await;
        write(&provider, InsertOp::Append, vec![batch(&[(9, "old")])])
            .await
            .expect("seed");
        let error = write(
            &provider,
            InsertOp::Append,
            vec![
                batch(&[(1, "a"), (2, "a")]),
                batch(&[(2, "b")]),
                null_key_batch(),
            ],
        )
        .await
        .expect_err("a null primary key fails the append");
        eprintln!("{mode:?} failed append: {error}");
        assert_eq!(visible(&provider).await, (owned(&[(9, "old")]), 1));
        let provider = reopen(&catalog, &runtime_env).await;
        assert_eq!(visible(&provider).await, (owned(&[(9, "old")]), 1));
    }
}

/// A table created with `DoNothing` keeps the last copy of a key too.
#[tokio::test(flavor = "multi_thread")]
async fn streaming_append_into_a_drop_table_keeps_the_last_copy() {
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
        assert_eq!(visible(&provider).await, expected);
        let provider = reopen(&catalog, &runtime_env).await;
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
        let provider = reopen(&catalog, &runtime_env).await;
        assert_eq!(visible(&provider).await, expected);
    }
}

/// `[(1,a),(2,a),(3,a)]`, `[(4,b),(2,b)]`, `[(2,c),(5,c),(1,c)]`, `[(6,d)]`: key 2
/// repeats across three batches and key 1 across two, so the overwrite writes
/// three layers into its snapshot.
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
async fn overwrite_resolves_copies_across_batches_through_its_lifecycle() {
    let mut failures = Vec::new();
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        let label = format!("{mode:?}");
        let (provider, catalog, runtime_env, _dir) = table(mode).await;
        write(&provider, InsertOp::Overwrite, vec![batch(&[(9, "old")])])
            .await
            .expect("seed");
        write(&provider, InsertOp::Overwrite, repeated_across_batches())
            .await
            .expect("overwrite repeating keys");
        let expected = last_copies();
        let mut check = |stage: &str, (rows, count): (Vec<(i64, String)>, i64), layers: usize| {
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
        let provider = reopen(&catalog, &runtime_env).await;
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
        let provider = reopen(&catalog, &runtime_env).await;
        check(
            "after second reopen",
            visible(&provider).await,
            provider.protected_snapshot_ids().len(),
        );
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// An overwrite that repeats keys publishes one snapshot: a position-deletion table hides
/// the superseded copies by position, and a key-deletion table drops them from
/// its files, leaving no deletes at all.
fn assert_resolved_shape(provider: &CayenneTableProvider, mode: DeletionMode) {
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

/// An overwrite's position deletes live in the snapshot it publishes, not in the
/// snapshot it replaces: the replaced snapshot's directory is retired, and an
/// overwrite that fails removes only its own directory.
#[tokio::test(flavor = "multi_thread")]
async fn an_overwrite_writes_its_position_deletes_into_its_own_snapshot() {
    let (provider, catalog, _runtime_env, _dir) = table(DeletionMode::Position).await;
    // A first overwrite, so the second replaces a snapshot of its own.
    write(&provider, InsertOp::Overwrite, vec![batch(&[(9, "z")])])
        .await
        .expect("first overwrite");
    let replaced = provider.get_current_snapshot_id();
    write(&provider, InsertOp::Overwrite, repeated_across_batches())
        .await
        .expect("overwrite repeating keys");
    let published = provider.get_current_snapshot_id();
    assert_ne!(
        replaced, published,
        "the overwrite publishes a new snapshot"
    );
    let delete_files = catalog
        .get_table_delete_files(provider.table_id())
        .await
        .expect("delete files");
    assert!(
        !delete_files.is_empty(),
        "the overwrite hides its repeats by position"
    );
    for delete_file in &delete_files {
        assert!(
            delete_file.path.contains(&format!("/{published}/")),
            "delete file {} is outside the published snapshot {published}",
            delete_file.path
        );
    }
}

/// After an overwrite that repeats keys on a position-deletion table, position capture and
/// a later upsert of the repeated keys still leave exactly one row per key.
#[tokio::test(flavor = "multi_thread")]
async fn position_capture_after_an_overwrite_repeating_keys_locates_the_live_copy() {
    let (provider, _catalog, _runtime_env, _dir) = table(DeletionMode::Position).await;
    write(&provider, InsertOp::Overwrite, repeated_across_batches())
        .await
        .expect("overwrite repeating keys");
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
async fn an_overwrite_repeating_a_string_key_resolves_it_in_both_modes() {
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
        let (provider, _catalog, _dir) = create_cdc_table_with_schema(
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
async fn an_overwrite_repeating_keys_is_superseded_by_a_later_upsert_and_overwrite() {
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        let (provider, catalog, runtime_env, _dir) = table(mode).await;
        write(&provider, InsertOp::Overwrite, repeated_across_batches())
            .await
            .expect("overwrite repeating keys");
        assert_resolved_shape(&provider, mode);
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
        let provider = reopen(&catalog, &runtime_env).await;
        assert_eq!(
            visible(&provider).await,
            (owned(&[(1, "z")]), 1),
            "{mode:?}: reopened"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_overwrite_repeating_keys_leaves_the_previous_table() {
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        let (provider, catalog, runtime_env, _dir) = table(mode).await;
        write(&provider, InsertOp::Overwrite, vec![batch(&[(9, "old")])])
            .await
            .expect("seed");
        // The third batch carries a null primary key, which fails every policy,
        // after the second batch has already been written.
        let error = write(
            &provider,
            InsertOp::Overwrite,
            vec![
                batch(&[(1, "a"), (2, "a")]),
                batch(&[(2, "b")]),
                null_key_batch(),
            ],
        )
        .await
        .expect_err("a null primary key fails the overwrite");
        assert!(
            error.to_string().contains("'id' has null values"),
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
        let provider = reopen(&catalog, &runtime_env).await;
        assert_eq!(
            visible(&provider).await,
            (owned(&[(9, "old")]), 1),
            "{mode:?}: reopened"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_overwrite_into_a_drop_table_keeps_the_last_copy() {
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
    assert_eq!(visible(&provider).await, (last_copies(), 6));
    assert!(provider.protected_snapshot_ids().is_empty());
}

/// An overwrite resolves the keys it repeats across batches within its own
/// snapshot: it publishes no layer and no key tombstone.
#[tokio::test(flavor = "multi_thread")]
async fn an_overwrite_repeating_keys_publishes_one_snapshot() {
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        let (provider, _catalog, _runtime_env, _dir) = table(mode).await;
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
async fn aggregates_after_an_overwrite_repeating_keys_ignore_superseded_copies() {
    let mut failures = Vec::new();
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        let (provider, catalog, runtime_env, _dir) = table(mode).await;
        write(
            &provider,
            InsertOp::Overwrite,
            vec![
                batch(&[(1, "m"), (2, "zzz"), (3, "m")]),
                batch(&[(2, "aaa"), (4, "m")]),
            ],
        )
        .await
        .expect("overwrite repeating keys");
        for (stage, provider) in [
            ("after overwrite", provider.clone_for_write()),
            ("after reopen", reopen(&catalog, &runtime_env).await),
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

/// Arrival ordering applies to both an INSERT and a transaction's write.
#[tokio::test(flavor = "multi_thread")]
async fn an_arrival_statement_resolves_repeated_keys_across_batches() {
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        let (provider, catalog, runtime_env, _dir) = table(mode).await;
        let repeated = || {
            MemorySourceConfig::try_new_exec(
                &[vec![batch(&[(1, "a"), (2, "a")]), batch(&[(1, "b")])]],
                schema(),
                None,
            )
            .expect("source")
        };
        let ctx = SessionContext::new();
        let plan = provider
            .insert_into(&ctx.state(), repeated(), InsertOp::Append)
            .await
            .expect("plan");
        collect(plan, ctx.task_ctx())
            .await
            .expect("a statement resolves repeated keys by arrival");
        assert_eq!(visible(&provider).await, (owned(&[(1, "b"), (2, "a")]), 2));

        let token = provider.transaction_write_token().await;
        let stream = repeated().execute(0, ctx.task_ctx()).expect("stream");
        let staged = provider
            .begin_staged_upsert_occ(token, stream, 4)
            .await
            .expect("stage a statement resolving repeated keys by arrival");
        staged
            .commit(std::collections::HashSet::new(), true)
            .await
            .expect("commit");
        assert_eq!(visible(&provider).await, (owned(&[(1, "b"), (2, "a")]), 2));
        let provider = reopen(&catalog, &runtime_env).await;
        assert_eq!(visible(&provider).await, (owned(&[(1, "b"), (2, "a")]), 2));
    }
}

/// A refresh whose map of keys outgrows a bounded memory pool spills it and
/// still keeps the last copy of every key, rather than failing.
#[tokio::test(flavor = "multi_thread")]
async fn a_refresh_in_a_small_memory_pool_spills_and_keeps_the_last_copy() {
    const KEYS: i64 = 200_000;
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        let runtime_env = datafusion_execution::runtime_env::RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(
                datafusion_execution::memory_pool::GreedyMemoryPool::new(8 * 1024 * 1024),
            ))
            .build_arc()
            .expect("runtime env");
        let (provider, _catalog, _dir) = create_cdc_table_with_schema(
            "t",
            Arc::clone(&runtime_env),
            schema(),
            vec!["id".to_string()],
            VortexConfig {
                deletion_mode: mode,
                inline_max_rows: 0,
                compaction_background_interval_ms: 3_600_000,
                ..VortexConfig::default()
            },
            OnConflict::Upsert(
                datafusion_table_providers::util::column_reference::ColumnReference::new(vec![
                    "id".to_string(),
                ]),
            ),
        )
        .await;
        let rows: Vec<(i64, &str)> = ["first", "last"]
            .iter()
            .flat_map(|value| (0..KEYS).map(move |id| (id, *value)))
            .collect();
        write(
            &provider,
            InsertOp::Overwrite,
            rows.chunks(8192).map(batch).collect(),
        )
        .await
        .unwrap_or_else(|error| panic!("{mode:?}: refresh in a bounded pool failed: {error}"));
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(provider.clone_for_write()))
            .expect("register");
        let counts = ctx
            .sql("SELECT COUNT(*), COUNT(DISTINCT id), SUM(CASE WHEN value = 'last' THEN 1 ELSE 0 END) FROM t")
            .await
            .expect("query")
            .collect()
            .await
            .expect("collect");
        let column = |index: usize| {
            counts[0]
                .column(index)
                .as_primitive::<arrow::datatypes::Int64Type>()
                .value(0)
        };
        assert_eq!(
            (column(0), column(1), column(2)),
            (KEYS, KEYS, KEYS),
            "{mode:?}: (rows, keys, last copies)"
        );
    }
}

/// The live row count the table's statistics report once its post-write
/// maintenance has run.
async fn statistics_rows(provider: &CayenneTableProvider) -> Option<usize> {
    provider
        .flush_pending_maintenance()
        .await
        .expect("flush maintenance");
    provider
        .statistics()
        .and_then(|stats| stats.num_rows.get_value().copied())
}

async fn upsert_table(
    mode: DeletionMode,
    on_conflict: OnConflict,
) -> (
    CayenneTableProvider,
    Arc<dyn MetadataCatalog>,
    Arc<RuntimeEnv>,
    TempDir,
) {
    let runtime_env = SessionContext::new().runtime_env();
    let (provider, catalog, dir) = create_cdc_table_with_schema(
        "t",
        Arc::clone(&runtime_env),
        schema(),
        vec!["id".to_string()],
        VortexConfig {
            deletion_mode: mode,
            inline_max_rows: 0,
            stream_publish_interval_ms: 0,
            compaction_background_interval_ms: 3_600_000,
            ..VortexConfig::default()
        },
        on_conflict,
    )
    .await;
    (provider, catalog, runtime_env, dir)
}

fn upsert_on_id() -> OnConflict {
    OnConflict::Upsert(
        datafusion_table_providers::util::column_reference::ColumnReference::new(vec![
            "id".to_string(),
        ]),
    )
}

fn drop_on_id() -> OnConflict {
    OnConflict::DoNothing(
        datafusion_table_providers::util::column_reference::ColumnReference::new(vec![
            "id".to_string(),
        ]),
    )
}

/// A key a streaming append repeats across batches that also meets a stored
/// copy supersedes that stored copy once: the rows, `COUNT(*)` and the live row
/// count the statistics carry all hold one row per key.
#[tokio::test(flavor = "multi_thread")]
async fn streaming_append_supersedes_a_stored_copy_once_per_key() {
    let mut failures = Vec::new();
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        let (provider, catalog, runtime_env, _dir) = upsert_table(mode, upsert_on_id()).await;
        write(
            &provider,
            InsertOp::Append,
            vec![batch(&[(1, "old"), (2, "old"), (9, "old")])],
        )
        .await
        .expect("seed");
        write(
            &provider,
            InsertOp::Append,
            vec![
                batch(&[(1, "a"), (2, "a")]),
                batch(&[(2, "b"), (1, "b")]),
                batch(&[(1, "c")]),
            ],
        )
        .await
        .expect("streaming append");
        let expected = (owned(&[(1, "c"), (2, "b"), (9, "old")]), 3);
        for (stage, provider) in [
            ("after append", provider.clone_for_write()),
            ("after reopen", reopen(&catalog, &runtime_env).await),
        ] {
            let observed = visible(&provider).await;
            let rows = statistics_rows(&provider).await;
            eprintln!("{mode:?} {stage}: {observed:?}, statistics rows {rows:?}");
            if observed != expected {
                failures.push(format!("{mode:?} {stage}: {observed:?}"));
            }
            if rows.is_some_and(|rows| rows != 3) {
                failures.push(format!("{mode:?} {stage}: statistics rows {rows:?}"));
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// A table created with `DoNothing` lets a streaming append supersede a stored
/// copy, keeping each key's last copy.
#[tokio::test(flavor = "multi_thread")]
async fn streaming_append_into_a_drop_table_supersedes_stored_copies() {
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        let (provider, catalog, runtime_env, _dir) = upsert_table(mode, drop_on_id()).await;
        write(&provider, InsertOp::Append, vec![batch(&[(1, "old")])])
            .await
            .expect("seed");
        write(
            &provider,
            InsertOp::Append,
            vec![
                batch(&[(1, "a"), (2, "a")]),
                batch(&[(2, "b"), (1, "b"), (3, "b")]),
                batch(&[(3, "c")]),
            ],
        )
        .await
        .expect("streaming append");
        let expected = (owned(&[(1, "b"), (2, "b"), (3, "c")]), 3);
        assert_eq!(visible(&provider).await, expected, "{mode:?}");
        assert_eq!(statistics_rows(&provider).await.unwrap_or(3), 3, "{mode:?}");
        let provider = reopen(&catalog, &runtime_env).await;
        assert_eq!(visible(&provider).await, expected, "{mode:?}: reopened");
    }
}

/// A streaming append spanning many batches and files keeps the last copy of
/// every key, including keys it repeats that were already stored.
#[tokio::test(flavor = "multi_thread")]
async fn streaming_append_resolves_repeats_across_many_batches() {
    const KEYS: i64 = 50_000;
    const STORED: i64 = 10_000;
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        let (provider, _catalog, _runtime_env, _dir) = upsert_table(mode, upsert_on_id()).await;
        let seed: Vec<(i64, &str)> = (0..STORED).map(|id| (id, "old")).collect();
        write(
            &provider,
            InsertOp::Append,
            seed.chunks(4096).map(batch).collect(),
        )
        .await
        .expect("seed");
        let rows: Vec<(i64, &str)> = ["first", "last"]
            .iter()
            .flat_map(|value| (0..KEYS).map(move |id| (id, *value)))
            .collect();
        write(
            &provider,
            InsertOp::Append,
            rows.chunks(8192).map(batch).collect(),
        )
        .await
        .unwrap_or_else(|error| panic!("{mode:?}: streaming append failed: {error}"));
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(provider.clone_for_write()))
            .expect("register");
        let counts = ctx
            .sql("SELECT COUNT(*), COUNT(DISTINCT id), SUM(CASE WHEN value = 'last' THEN 1 ELSE 0 END) FROM t")
            .await
            .expect("query")
            .collect()
            .await
            .expect("collect");
        let column = |index: usize| {
            counts[0]
                .column(index)
                .as_primitive::<arrow::datatypes::Int64Type>()
                .value(0)
        };
        assert_eq!(
            (column(0), column(1), column(2)),
            (KEYS, KEYS, KEYS),
            "{mode:?}: (rows, keys, last copies)"
        );
        let rows = statistics_rows(&provider).await;
        eprintln!("{mode:?}: statistics rows {rows:?}");
        let keys = usize::try_from(KEYS).expect("key count fits");
        assert_eq!(rows.unwrap_or(keys), keys, "{mode:?}: statistics rows");
    }
}

/// A refresh whose written files are cut into many key ranges — equal-width for
/// an integer key, at sampled quantiles for a string key — still keeps exactly
/// the last copy of every key, in both deletion modes.
#[tokio::test(flavor = "multi_thread")]
async fn a_refresh_cut_into_many_key_ranges_keeps_the_last_copy_of_every_key() {
    use std::sync::atomic::Ordering;
    const KEYS: i64 = 20_000;
    super::super::super::overwrite_postpass::TEST_CHUNK_ROWS.store(1_000, Ordering::Relaxed);
    for string_key in [false, true] {
        for mode in [DeletionMode::Key, DeletionMode::Position] {
            let key_type = if string_key {
                DataType::Utf8
            } else {
                DataType::Int64
            };
            let schema = Arc::new(Schema::new(vec![
                Field::new("k", key_type, false),
                Field::new("pass", DataType::Int64, false),
            ]));
            let batch_of = |pass: i64, keys: &[i64]| {
                let key: arrow::array::ArrayRef = if string_key {
                    Arc::new(StringArray::from_iter_values(
                        keys.iter()
                            .map(|k| format!("key-{:016x}", k.wrapping_mul(0x9e37_79b9))),
                    ))
                } else {
                    Arc::new(Int64Array::from_iter_values(keys.iter().copied()))
                };
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![key, Arc::new(Int64Array::from_value(pass, keys.len()))],
                )
                .expect("batch")
            };
            // Each pass visits every key in a scrambled order, in 4,096-row batches.
            let order: Vec<i64> = (0..KEYS).map(|k| (k * 7_919) % KEYS).collect();
            let batches: Vec<RecordBatch> = [0, 1]
                .iter()
                .flat_map(|&pass| order.chunks(4_096).map(move |keys| (pass, keys)))
                .map(|(pass, keys)| batch_of(pass, keys))
                .collect();
            let runtime_env = SessionContext::new().runtime_env();
            let (provider, _catalog, _dir) = create_cdc_table_with_schema(
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
            let ctx = SessionContext::new();
            let source = MemorySourceConfig::try_new_exec(&[batches], Arc::clone(&schema), None)
                .expect("source");
            let plan = provider
                .insert_into(&ctx.state(), source, InsertOp::Overwrite)
                .await
                .expect("plan");
            collect(plan, ctx.task_ctx()).await.expect("overwrite");
            ctx.register_table("t", Arc::new(provider.clone_for_write()))
                .expect("register");
            let batches = ctx
                .sql("SELECT COUNT(*), COUNT(DISTINCT k), MIN(pass) FROM t")
                .await
                .expect("query")
                .collect()
                .await
                .expect("collect");
            let value = |column: usize| {
                batches[0]
                    .column(column)
                    .as_primitive::<arrow::datatypes::Int64Type>()
                    .value(0)
            };
            assert_eq!(
                (value(0), value(1), value(2)),
                (KEYS, KEYS, 1),
                "string key {string_key}, {mode:?}"
            );
        }
    }
    super::super::super::overwrite_postpass::TEST_CHUNK_ROWS.store(0, Ordering::Relaxed);
}

/// The staged dual-write append path rejects primary-key deletion handling.
#[tokio::test(flavor = "multi_thread")]
async fn a_staged_append_refuses_a_keyed_table() {
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        let (provider, _catalog, _runtime_env, _dir) = table(mode).await;
        assert!(provider.key_resolver().expect("resolver").is_some());
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            schema(),
            futures::stream::iter(
                vec![batch(&[(1, "a"), (2, "a")]), batch(&[(1, "b")])]
                    .into_iter()
                    .map(Ok),
            ),
        ));
        let Err(error) = provider.begin_staged_append(stream, 1).await else {
            panic!("{mode:?}: a staged append into a keyed table must be refused");
        };
        assert!(
            error
                .to_string()
                .contains("staged append for Cayenne tables with primary-key deletion handling"),
            "{mode:?}: {error}"
        );
    }
}

/// A table may name its own columns like the post-write resolution's arrival
/// column; the refresh still resolves every key by the column it wrote, and the
/// user's columns keep their values.
#[tokio::test(flavor = "multi_thread")]
async fn user_columns_named_like_the_helper_columns_are_left_alone() {
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("__cayenne_arrival", DataType::Int64, false),
            Field::new("__cayenne_content_lo", DataType::UInt64, false),
            Field::new("__cayenne_content_hi", DataType::UInt64, false),
        ]));
        // The user's arrival column counts down, so it disagrees with arrival order.
        let batch_of = |rows: &[(i64, i64)]| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from_iter_values(rows.iter().map(|(k, _)| *k))),
                    Arc::new(Int64Array::from_iter_values(rows.iter().map(|(_, v)| *v))),
                    Arc::new(arrow::array::UInt64Array::from_value(7, rows.len())),
                    Arc::new(arrow::array::UInt64Array::from_value(7, rows.len())),
                ],
            )
            .expect("batch")
        };
        for (batches, expected) in [(
            vec![
                batch_of(&[(1, 30), (2, 30)]),
                batch_of(&[(1, 20)]),
                batch_of(&[(2, 10), (3, 10)]),
            ],
            vec![(1, 20), (2, 10), (3, 10)],
        )] {
            let label = format!("{mode:?}");
            let runtime_env = SessionContext::new().runtime_env();
            let (provider, _catalog, _dir) = create_cdc_table_with_schema(
                "t",
                Arc::clone(&runtime_env),
                Arc::clone(&schema),
                vec!["id".to_string()],
                VortexConfig {
                    deletion_mode: mode,
                    inline_max_rows: 0,
                    compaction_background_interval_ms: 3_600_000,
                    ..VortexConfig::default()
                },
                OnConflict::Upsert(
                    datafusion_table_providers::util::column_reference::ColumnReference::new(vec![
                        "id".to_string(),
                    ]),
                ),
            )
            .await;
            let ctx = SessionContext::new();
            let source = MemorySourceConfig::try_new_exec(&[batches], Arc::clone(&schema), None)
                .expect("source");
            let plan = provider
                .insert_into(&ctx.state(), source, InsertOp::Overwrite)
                .await
                .expect("plan");
            let result = collect(plan, ctx.task_ctx()).await;
            result.unwrap_or_else(|error| panic!("{label}: overwrite failed: {error}"));
            ctx.register_table("t", Arc::new(provider.clone_for_write()))
                .expect("register");
            let batches = ctx
                .sql(
                    "SELECT id, \"__cayenne_arrival\", \"__cayenne_content_lo\", \
                     \"__cayenne_content_hi\" FROM t ORDER BY id",
                )
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
                let values = batch
                    .column(1)
                    .as_primitive::<arrow::datatypes::Int64Type>();
                for row in 0..batch.num_rows() {
                    for column in [2, 3] {
                        assert_eq!(
                            batch
                                .column(column)
                                .as_primitive::<arrow::datatypes::UInt64Type>()
                                .value(row),
                            7,
                            "{label}: the user's column {column} keeps its value"
                        );
                    }
                    rows.push((ids.value(row), values.value(row)));
                }
            }
            assert_eq!(rows, expected, "{label}");
        }
    }
}

/// A refresh's load into a table that holds no rows is written as one append
/// without the conflict check: it publishes as any append does (one protected
/// snapshot, the current snapshot unmoved) and resolves the keys it repeats. The
/// next load, into a table that now holds rows, is checked and supersedes what it
/// repeats.
#[tokio::test(flavor = "multi_thread")]
async fn a_load_into_an_empty_table_appends_and_the_next_supersedes() {
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        for on_conflict in [upsert_on_id(), drop_on_id()] {
            let label = format!("{mode:?}/{on_conflict:?}");
            let kept = last_copies();
            let (provider, _catalog, _runtime_env, _dir) = upsert_table(mode, on_conflict).await;
            let empty = provider.current_snapshot_id();
            write(&provider, InsertOp::Append, repeated_across_batches())
                .await
                .expect("first load");
            let loaded = provider.current_snapshot_id();
            assert_eq!(loaded, empty, "{label}: the first load appends");
            assert_eq!(
                provider.protected_snapshots.load().len(),
                1,
                "{label}: the first load publishes one snapshot"
            );
            assert_eq!(visible(&provider).await, (kept.clone(), 6), "{label}");
            // The load records no primary-key index; one is warmed for the next
            // load in the background.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while provider.pk_keyset_cache.lock().is_none() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "{label}: no primary-key index was warmed after the first load"
                );
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }

            write(
                &provider,
                InsertOp::Append,
                vec![batch(&[(1, "z"), (7, "z")])],
            )
            .await
            .expect("second load");
            assert_eq!(
                provider.current_snapshot_id(),
                loaded,
                "{label}: a load into a table holding rows appends"
            );
            let mut expected = kept;
            expected[0] = (1, "z".to_string());
            expected.push((7, "z".to_string()));
            assert_eq!(visible(&provider).await, (expected, 7), "{label}");
        }
    }
}

/// A table holding rows only in the inline tier is not empty: a load into it
/// appends and keeps every stored row, rather than replacing them.
#[tokio::test(flavor = "multi_thread")]
async fn a_load_into_a_table_holding_only_inline_rows_appends() {
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        let runtime_env = SessionContext::new().runtime_env();
        let (provider, _catalog, _dir) = create_cdc_table_with_schema(
            "t",
            Arc::clone(&runtime_env),
            schema(),
            vec!["id".to_string()],
            VortexConfig {
                deletion_mode: mode,
                inline_max_rows: 1_000,
                stream_publish_interval_ms: 0,
                compaction_background_interval_ms: 3_600_000,
                ..VortexConfig::default()
            },
            upsert_on_id(),
        )
        .await;
        write(
            &provider,
            InsertOp::Append,
            vec![batch(&[(1, "a"), (2, "a")])],
        )
        .await
        .expect("first load");
        assert_eq!(
            provider.cached_inlined_row_count(),
            2,
            "{mode:?}: the first load sits in the inline tier"
        );
        let loaded = provider.current_snapshot_id();
        write(
            &provider,
            InsertOp::Append,
            vec![batch(&[(2, "b"), (3, "b")])],
        )
        .await
        .expect("second load");
        assert_eq!(
            provider.current_snapshot_id(),
            loaded,
            "{mode:?}: a load into a table holding inline rows appends"
        );
        assert_eq!(
            visible(&provider).await,
            (owned(&[(1, "a"), (2, "b"), (3, "b")]), 3),
            "{mode:?}"
        );
    }
}

/// A load into an empty table records no keys, so it must drop a primary-key
/// index cached before it (here an empty one): a later load validated against
/// that index would miss every key the first load wrote and store them twice.
#[tokio::test(flavor = "multi_thread")]
async fn a_load_into_an_empty_table_drops_a_cached_index_it_did_not_fill() {
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        let (provider, _catalog, _runtime_env, _dir) = upsert_table(mode, upsert_on_id()).await;
        *provider.pk_keyset_cache.lock() =
            Some(CachedPkIndex::Exact(CachedPkKeyset::with_capacity(0)));
        write(&provider, InsertOp::Append, repeated_across_batches())
            .await
            .expect("first load");
        write(&provider, InsertOp::Append, vec![batch(&[(1, "z")])])
            .await
            .expect("second load");
        let mut expected = last_copies();
        expected[0] = (1, "z".to_string());
        assert_eq!(visible(&provider).await, (expected, 6), "{mode:?}");
    }
}

/// A partition's append resolves the keys it repeats over its whole input,
/// however far apart its batches repeat them, before it is staged for
/// publication.
#[tokio::test(flavor = "multi_thread")]
async fn a_partition_append_resolves_its_whole_input() {
    let input = || {
        let batches = vec![
            batch(&[(1, "a"), (2, "a")]),
            batch(&[(1, "b")]),
            batch(&[(3, "a")]),
            batch(&[(1, "c"), (2, "a")]),
        ];
        Box::pin(RecordBatchStreamAdapter::new(
            schema(),
            futures::stream::iter(batches.into_iter().map(Ok)),
        )) as SendableRecordBatchStream
    };
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        for on_conflict in [upsert_on_id(), drop_on_id()] {
            let label = format!("{mode:?}/{on_conflict:?}");
            let (provider, _catalog, _runtime_env, _dir) = upsert_table(mode, on_conflict).await;
            let prepared = provider
                .begin_deferred_snapshot_append(input(), 2)
                .await
                .unwrap_or_else(|error| panic!("{label}: {error}"));
            assert_eq!(prepared.row_count(), 3, "{label}: staged rows");
            prepared.rollback().await.expect("rollback");
        }
    }
}
