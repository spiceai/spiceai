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

//! Per-statement resolution of repeated keys and failed-refresh visibility (#14576).

use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn identical_copies_collapse_for_refreshes_and_statements() {
    let mut failures = Vec::new();
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        for op in [InsertOp::Overwrite, InsertOp::Append] {
            for split in [false, true] {
                let label = format!("{mode:?}/{op:?}/split={split}");
                let (provider, catalog, runtime_env, _dir) = table(mode).await;
                write(&provider, InsertOp::Append, vec![batch(&[(9, "old")])])
                    .await
                    .expect("seed");
                let batches = if split {
                    vec![batch(&[(1, "a"), (2, "b")]), batch(&[(1, "a")])]
                } else {
                    vec![batch(&[(1, "a"), (2, "b"), (1, "a")])]
                };
                if let Err(error) = write(&provider, op, batches).await {
                    failures.push(format!("{label}: identical copies rejected: {error}"));
                    continue;
                }
                let expected = if op == InsertOp::Append {
                    (owned(&[(1, "a"), (2, "b"), (9, "old")]), 3)
                } else {
                    (owned(&[(1, "a"), (2, "b")]), 2)
                };
                let actual = visible(&provider).await;
                if actual != expected {
                    failures.push(format!("{label}: wrong rows: {actual:?}"));
                }
                let reopened = reopen(&catalog, &runtime_env).await;
                let actual = visible(&reopened).await;
                if actual != expected {
                    failures.push(format!("{label}: wrong persisted rows: {actual:?}"));
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test(flavor = "multi_thread")]
async fn staged_statements_keep_each_keys_last_copy() {
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        for policy in [upsert_on_id(), drop_on_id()] {
            for different in [false, true] {
                let label = format!("{mode:?}/{policy:?}/different={different}");
                let (provider, catalog, runtime_env, _dir) =
                    upsert_table(mode, policy.clone()).await;
                let prior = (owned(&[(1, "old"), (9, "old")]), 2);
                write(
                    &provider,
                    InsertOp::Append,
                    vec![batch(&[(1, "old"), (9, "old")])],
                )
                .await
                .expect("seed");
                let token = provider.transaction_write_token().await;
                let latest = if different { "b" } else { "a" };
                let batches = vec![
                    batch(&[(1, "a"), (1, "a"), (2, "b"), (3, "a")]),
                    batch(&[(1, latest), (3, latest)]),
                ];
                let stream = Box::pin(RecordBatchStreamAdapter::new(
                    schema(),
                    futures::stream::iter(batches.into_iter().map(Ok)),
                ));
                let staged = provider.begin_staged_upsert_occ(token, stream, 4).await;
                assert_eq!(
                    visible(&provider).await,
                    prior,
                    "{label}: staging is private"
                );
                staged
                    .expect("stage")
                    .commit(std::collections::HashSet::new(), true)
                    .await
                    .expect("commit");
                let expected = (owned(&[(1, latest), (2, "b"), (3, latest), (9, "old")]), 4);
                assert_eq!(visible(&provider).await, expected, "{label}: live rows");
                let reopened = reopen(&catalog, &runtime_env).await;
                assert_eq!(visible(&reopened).await, expected, "{label}: durable rows");
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_snapshot_publication_keeps_prior_rows_after_reopen() {
    let mut failures = Vec::new();
    for (mode, inline_max_rows) in [
        (DeletionMode::Key, 0),
        (DeletionMode::Key, 1),
        (DeletionMode::Position, 0),
        (DeletionMode::Position, 1),
    ] {
        let label = format!("{mode:?}/inline={inline_max_rows}");
        let runtime_env = SessionContext::new().runtime_env();
        let (provider, catalog, _dir) = create_cdc_table_with_schema(
            "t",
            Arc::clone(&runtime_env),
            schema(),
            vec!["id".to_string()],
            VortexConfig {
                deletion_mode: mode,
                inline_max_rows,
                stream_publish_interval_ms: 0,
                compaction_background_interval_ms: 3_600_000,
                ..VortexConfig::default()
            },
            upsert_on_id(),
        )
        .await;
        write(&provider, InsertOp::Append, vec![batch(&[(9, "old")])])
            .await
            .expect("seed");
        assert_eq!(
            provider.cached_inlined_row_count(),
            i64::from(inline_max_rows != 0),
            "{label}: seed storage tier",
        );
        let concrete = catalog
            .as_any()
            .downcast_ref::<CayenneCatalog>()
            .expect("concrete fixture catalog");
        let txn = concrete
            .begin_transaction()
            .await
            .expect("begin fault setup");
        txn.execute_batch(
            "CREATE TRIGGER fail_snapshot_publish BEFORE INSERT ON cayenne_snapshot_sequence BEGIN SELECT RAISE(ABORT, 'injected snapshot publish failure'); END;",
        )
        .await
        .expect("install fixture fault");
        txn.commit().await.expect("commit fault setup");

        // Two rows exceed the inline row limit, forcing a file replacement.
        let replacement = || vec![batch(&[(9, "new"), (10, "new")])];
        let error = write(&provider, InsertOp::Append, replacement())
            .await
            .expect_err("the snapshot metadata write must reach the fault");
        assert!(
            error
                .to_string()
                .contains("injected snapshot publish failure"),
            "{label}: unexpected error: {error}",
        );
        let txn = concrete
            .begin_transaction()
            .await
            .expect("begin fault removal");
        txn.execute_batch("DROP TRIGGER fail_snapshot_publish;")
            .await
            .expect("remove fixture fault");
        txn.commit().await.expect("commit fault removal");

        let expected = (owned(&[(9, "old")]), 1);
        let actual = visible(&provider).await;
        if actual != expected {
            failures.push(format!(
                "{label}: live data changed after failure: {actual:?}"
            ));
        }
        let reopened = reopen(&catalog, &runtime_env).await;
        let actual = visible(&reopened).await;
        if actual != expected {
            failures.push(format!(
                "{label}: durable data changed after failure: {actual:?}"
            ));
        }
        write(&provider, InsertOp::Append, replacement())
            .await
            .expect("publish the replacement without the fault");
        let expected = (owned(&[(9, "new"), (10, "new")]), 2);
        assert_eq!(visible(&provider).await, expected, "{label}: published");
        let reopened = reopen(&catalog, &runtime_env).await;
        assert_eq!(
            visible(&reopened).await,
            expected,
            "{label}: published and reopened"
        );

        let snapshots = catalog
            .get_all_snapshot_sequences(provider.table_id())
            .await
            .expect("snapshots");
        write(&provider, InsertOp::Append, vec![])
            .await
            .expect("empty append");
        assert_eq!(visible(&provider).await, expected, "{label}: empty append");
        assert_eq!(
            catalog
                .get_all_snapshot_sequences(provider.table_id())
                .await
                .expect("snapshots"),
            snapshots,
            "{label}: an empty append must not publish a phantom snapshot",
        );
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test(flavor = "multi_thread")]
async fn late_append_error_does_not_publish_an_earlier_segment() {
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
                target_vortex_file_size_mb: 1,
                stream_publish_interval_ms: 60_000,
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
        write(&provider, InsertOp::Append, vec![batch(&[(9, "old")])])
            .await
            .expect("seed");

        // Exceed the segment byte threshold without depending on timer timing.
        let replacement = "x".repeat(8 * 1024 * 1024);
        let error = write(
            &provider,
            InsertOp::Append,
            vec![batch(&[(9, &replacement)]), null_key_batch()],
        )
        .await
        .expect_err("the null key rejects the whole append");
        eprintln!("{mode:?} late append error: {error}");
        let assert_unchanged = |actual: (Vec<(i64, String)>, i64)| {
            // Report exact equality to the old value, not the oversized payload.
            let rows: Vec<_> = actual
                .0
                .iter()
                .map(|(id, value)| (*id, value == "old"))
                .collect();
            assert_eq!((rows, actual.1), (vec![(9, true)], 1), "{mode:?}");
        };
        assert_unchanged(visible(&provider).await);
        let reopened = reopen(&catalog, &runtime_env).await;
        assert_unchanged(visible(&reopened).await);
    }
}

/// A file-backed keyed table of `mode` whose writes always stream, holding
/// `(9, "old")`.
async fn seeded_streaming_table(
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
            ..VortexConfig::default()
        },
        upsert_on_id(),
    )
    .await;
    write(&provider, InsertOp::Append, vec![batch(&[(9, "old")])])
        .await
        .expect("seed");
    (provider, catalog, runtime_env, dir)
}

/// A catalog commit that reports a failure is resolved by reading back
/// whether it committed: a commit that happened is published, one that did
/// not leaves the prior rows, and one whose outcome cannot be read keeps its
/// files and refuses writes, deletes and checkpoints until the table is
/// reloaded, which then serves it.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_commit_is_resolved_by_its_durable_outcome() {
    use crate::provider::append_commit::test_seams::{self, CommitFault};

    let old = (owned(&[(9, "old")]), 1);
    let new = (owned(&[(9, "new"), (10, "new")]), 2);
    let replacement = || vec![batch(&[(9, "new"), (10, "new")])];
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        for fault in [
            CommitFault::CommittedButReported,
            CommitFault::RolledBack,
            CommitFault::CommittedUnreadable,
        ] {
            let label = format!("{mode:?}/{fault:?}");
            let (provider, catalog, runtime_env, _dir) = seeded_streaming_table(mode).await;
            test_seams::inject(provider.table_id(), fault);
            let result = write(&provider, InsertOp::Append, replacement()).await;
            let durable = match fault {
                CommitFault::CommittedButReported => {
                    result.unwrap_or_else(|error| {
                        panic!("{label}: a commit that happened is published: {error}")
                    });
                    assert_eq!(visible(&provider).await, new, "{label}: live rows");
                    new.clone()
                }
                CommitFault::RolledBack => {
                    let error = result.expect_err("a commit that did not happen fails");
                    assert!(
                        error.to_string().contains("injected commit failure"),
                        "{label}: {error}"
                    );
                    assert_eq!(visible(&provider).await, old, "{label}: live rows");
                    write(&provider, InsertOp::Append, replacement())
                        .await
                        .unwrap_or_else(|error| panic!("{label}: retry: {error}"));
                    assert_eq!(visible(&provider).await, new, "{label}: retried");
                    new.clone()
                }
                CommitFault::CommittedUnreadable => {
                    let error = result.expect_err("an unknown outcome fails");
                    assert!(
                        error
                            .to_string()
                            .contains("reading back whether it committed failed too"),
                        "{label}: {error}"
                    );
                    let refused = "writes are refused until the table is reloaded";
                    let error = write(&provider, InsertOp::Append, vec![batch(&[(11, "x")])])
                        .await
                        .expect_err("writes are refused after an unknown outcome");
                    assert!(error.to_string().contains(refused), "{label}: {error}");
                    // Deletes and maintenance change the table too, so they refuse
                    // as well: a statement `DELETE`, a retention `DELETE` that
                    // removes whole files, and an inline checkpoint.
                    let ctx = datafusion::prelude::SessionContext::new();
                    let id = || datafusion_expr::col("id");
                    let deletes = [
                        provider
                            .delete_from(&ctx.state(), vec![id().eq(datafusion_expr::lit(9_i64))])
                            .await
                            .expect("statement delete plan"),
                        provider
                            .delete_using_files(&[id().lt(datafusion_expr::lit(100_i64))])
                            .expect("file delete plan"),
                    ];
                    for delete in deletes {
                        let error = datafusion::physical_plan::collect(delete, ctx.task_ctx())
                            .await
                            .expect_err("deletes are refused after an unknown outcome");
                        assert!(error.to_string().contains(refused), "{label}: {error}");
                    }
                    let error = provider
                        .checkpoint_inlined_data()
                        .await
                        .expect_err("checkpoints are refused after an unknown outcome");
                    assert!(error.to_string().contains(refused), "{label}: {error}");
                    new.clone()
                }
            };
            let reopened = reopen(&catalog, &runtime_env).await;
            assert_eq!(visible(&reopened).await, durable, "{label}: reopened");
            write(&reopened, InsertOp::Append, vec![batch(&[(11, "x")])])
                .await
                .unwrap_or_else(|error| panic!("{label}: the reloaded table writes: {error}"));
        }
    }
}

/// Cancelling a write after its commit began does not release the write lock
/// early or abandon the commit: the next writer waits until the snapshot is
/// published, and it survives reopening.
#[tokio::test(flavor = "multi_thread")]
async fn a_cancelled_append_publishes_before_releasing_the_write_lock() {
    use crate::provider::append_commit::test_seams;

    for mode in [DeletionMode::Key, DeletionMode::Position] {
        let (provider, catalog, runtime_env, _dir) = seeded_streaming_table(mode).await;
        let (reached_tx, reached) = tokio::sync::oneshot::channel::<()>();
        let release = Arc::new(tokio::sync::Notify::new());
        let release_in_commit = Arc::clone(&release);
        test_seams::pause_before_commit(
            provider.table_id(),
            Box::new(move || {
                Box::pin(async move {
                    let _ = reached_tx.send(());
                    release_in_commit.notified().await;
                })
            }),
        );
        let writer = provider.clone_for_write();
        let caller = tokio::spawn(async move {
            write(
                &writer,
                InsertOp::Append,
                vec![batch(&[(9, "new"), (10, "new")])],
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(30), reached)
            .await
            .expect("the commit starts")
            .expect("the commit signals");
        caller.abort();
        assert!(
            caller
                .await
                .expect_err("the caller is cancelled")
                .is_cancelled(),
            "{mode:?}"
        );
        assert!(
            provider.write_lock_arc().try_lock_owned().is_err(),
            "{mode:?}: the commit still holds the write lock"
        );
        assert_eq!(
            visible(&provider).await,
            (owned(&[(9, "old")]), 1),
            "{mode:?}: nothing is published before the commit"
        );
        release.notify_one();
        let next_writer = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            provider.write_lock_arc().lock_owned(),
        )
        .await
        .expect("the commit releases the write lock");
        let expected = (owned(&[(9, "new"), (10, "new")]), 2);
        assert_eq!(visible(&provider).await, expected, "{mode:?}: published");
        drop(next_writer);
        let reopened = reopen(&catalog, &runtime_env).await;
        assert_eq!(visible(&reopened).await, expected, "{mode:?}: reopened");
    }
}

/// Strict `upsert` names how many keys hold different versions in the whole
/// statement, whatever its batch boundaries and write path: versions within one
/// batch, across batches, or both, each count once.
/// A transaction's write to each table is one statement over its own input,
/// and the fused commit publishes every table's together. A table the
/// transaction writes no rows to gains no snapshot.
#[tokio::test(flavor = "multi_thread")]
async fn a_transaction_resolves_each_statement_and_commits_them_together() {
    use crate::provider::transaction::CayenneTransaction;

    for mode in [DeletionMode::Key, DeletionMode::Position] {
        let label = format!("{mode:?}");
        let (first, catalog, runtime_env, dir) = seeded_streaming_table(mode).await;
        let create = |name: &str| CreateTableOptions {
            table_name: name.to_string(),
            schema: schema(),
            primary_key: vec!["id".to_string()],
            on_conflict: Some(upsert_on_id()),
            base_path: dir.path().join("data").display().to_string(),
            partition_column: None,
            vortex_config: VortexConfig {
                deletion_mode: mode,
                inline_max_rows: 0,
                stream_publish_interval_ms: 0,
                compaction_background_interval_ms: 3_600_000,
                ..VortexConfig::default()
            },
        };
        let mut tables = vec![first];
        for name in ["u", "empty"] {
            let table =
                CayenneTableProviderBuilder::new(Arc::clone(&catalog), Arc::clone(&runtime_env))
                    .create(create(name))
                    .await
                    .expect("create table");
            write(&table, InsertOp::Append, vec![batch(&[(9, "old")])])
                .await
                .expect("seed");
            tables.push(table);
        }
        let latest = "b";
        let statement = |rows: bool| {
            let batches = if rows {
                vec![
                    batch(&[(1, "a"), (2, "a")]),
                    batch(&[(1, latest), (9, "new")]),
                ]
            } else {
                Vec::new()
            };
            Box::pin(RecordBatchStreamAdapter::new(
                schema(),
                futures::stream::iter(batches.into_iter().map(Ok)),
            )) as SendableRecordBatchStream
        };
        let empty_snapshots = catalog
            .get_all_snapshot_sequences(tables[2].table_id())
            .await
            .expect("snapshots");
        let txn = CayenneTransaction::new();
        for (index, table) in tables.iter().enumerate() {
            let token = table.transaction_write_token().await;
            txn.register(
                table.table_id().to_string(),
                token,
                table.clone_for_write_operations(),
            );
            let staged = table
                .begin_staged_upsert_occ(token, statement(index < 2), 2)
                .await
                .unwrap_or_else(|error| panic!("{label}: stage: {error}"));
            txn.set_staged(table.table_id(), staged);
        }
        txn.commit()
            .await
            .unwrap_or_else(|error| panic!("{label}: commit: {error}"));
        let written = (owned(&[(1, latest), (2, "a"), (9, "new")]), 3);
        let untouched = (owned(&[(9, "old")]), 1);
        for (name, table, expected) in [
            ("t", &tables[0], &written),
            ("u", &tables[1], &written),
            ("empty", &tables[2], &untouched),
        ] {
            assert_eq!(&visible(table).await, expected, "{label}: {name} live");
            let reopened =
                CayenneTableProviderBuilder::new(Arc::clone(&catalog), Arc::clone(&runtime_env))
                    .open(name)
                    .await
                    .expect("reopen");
            assert_eq!(
                &visible(&reopened).await,
                expected,
                "{label}: {name} reopened"
            );
        }
        assert_eq!(
            catalog
                .get_all_snapshot_sequences(tables[2].table_id())
                .await
                .expect("snapshots"),
            empty_snapshots,
            "{label}: a table written no rows gains no snapshot"
        );
    }
}

/// A transaction's commit that reports a failure is resolved like an append's:
/// by reading back whether the shared transaction committed.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_transaction_commit_is_resolved_by_its_durable_outcome() {
    use crate::provider::append_commit::test_seams::{self, CommitFault};
    use crate::provider::transaction::CayenneTransaction;

    let old = (owned(&[(9, "old")]), 1);
    let new = (owned(&[(9, "new"), (10, "new")]), 2);
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        for fault in [
            CommitFault::CommittedButReported,
            CommitFault::RolledBack,
            CommitFault::CommittedUnreadable,
        ] {
            let label = format!("{mode:?}/{fault:?}");
            let (first, catalog, runtime_env, dir) = seeded_streaming_table(mode).await;
            let second =
                CayenneTableProviderBuilder::new(Arc::clone(&catalog), Arc::clone(&runtime_env))
                    .create(CreateTableOptions {
                        table_name: "u".to_string(),
                        schema: schema(),
                        primary_key: vec!["id".to_string()],
                        on_conflict: Some(upsert_on_id()),
                        base_path: dir.path().join("data").display().to_string(),
                        partition_column: None,
                        vortex_config: VortexConfig {
                            deletion_mode: mode,
                            inline_max_rows: 0,
                            stream_publish_interval_ms: 0,
                            compaction_background_interval_ms: 3_600_000,
                            ..VortexConfig::default()
                        },
                    })
                    .await
                    .expect("create table");
            write(&second, InsertOp::Append, vec![batch(&[(9, "old")])])
                .await
                .expect("seed");
            let tables = [first, second];
            let commit = || async {
                let txn = CayenneTransaction::new();
                for table in &tables {
                    let token = table.transaction_write_token().await;
                    txn.register(
                        table.table_id().to_string(),
                        token,
                        table.clone_for_write_operations(),
                    );
                    let stream = Box::pin(RecordBatchStreamAdapter::new(
                        schema(),
                        futures::stream::iter([Ok(batch(&[(9, "new"), (10, "new")]))]),
                    ));
                    let staged = table
                        .begin_staged_upsert_occ(token, stream, 1)
                        .await
                        .expect("stage");
                    txn.set_staged(table.table_id(), staged);
                }
                txn.commit().await
            };
            // The fused commit consults the first participant in table id order.
            let first_id = tables
                .iter()
                .map(CayenneTableProvider::table_id)
                .min()
                .expect("tables")
                .to_string();
            test_seams::inject(&first_id, fault);
            let result = commit().await;
            let durable = match fault {
                CommitFault::CommittedButReported => {
                    result.unwrap_or_else(|error| panic!("{label}: published: {error}"));
                    new.clone()
                }
                CommitFault::RolledBack => {
                    let error = result.expect_err("an uncommitted transaction fails");
                    assert!(
                        error.to_string().contains("injected commit failure"),
                        "{label}: {error}"
                    );
                    for table in &tables {
                        assert_eq!(visible(table).await, old, "{label}: live rows");
                    }
                    commit()
                        .await
                        .unwrap_or_else(|error| panic!("{label}: retry: {error}"));
                    new.clone()
                }
                CommitFault::CommittedUnreadable => {
                    let error = result.expect_err("an unknown outcome fails");
                    assert!(
                        error
                            .to_string()
                            .contains("reading back whether it committed failed too"),
                        "{label}: {error}"
                    );
                    for table in &tables {
                        let error = write(table, InsertOp::Append, vec![batch(&[(11, "x")])])
                            .await
                            .expect_err("writes are refused after an unknown outcome");
                        assert!(
                            error
                                .to_string()
                                .contains("writes are refused until the table is reloaded"),
                            "{label}: {error}"
                        );
                    }
                    new.clone()
                }
            };
            for name in ["t", "u"] {
                let reopened = CayenneTableProviderBuilder::new(
                    Arc::clone(&catalog),
                    Arc::clone(&runtime_env),
                )
                .open(name)
                .await
                .unwrap_or_else(|error| panic!("{label}: reopen {name}: {error}"));
                assert_eq!(
                    visible(&reopened).await,
                    durable,
                    "{label}: {name} reopened"
                );
            }
        }
    }
}

/// A write counts the rows it receives but does not keep into the counter its
/// session carries, as `arrival`, whether the copies are identical or not and
/// whether they share a batch or span batches. A stored row the write replaces
/// was not received by it, so it is not counted.
#[tokio::test(flavor = "multi_thread")]
async fn a_write_counts_the_rows_it_does_not_keep_by_reason() {
    use util::session_state::{SupersededReason, SupersededRows, with_superseded_rows};

    let counted = |rows: &SupersededRows| rows.get(SupersededReason::Arrival);
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        for inline_max_rows in [0, 1_000] {
            for op in [InsertOp::Overwrite, InsertOp::Append] {
                let label = format!("{mode:?}/inline={inline_max_rows}/{op:?}");
                let runtime_env = SessionContext::new().runtime_env();
                let (provider, _catalog, _dir) = create_cdc_table_with_schema(
                    "t",
                    Arc::clone(&runtime_env),
                    schema(),
                    vec!["id".to_string()],
                    VortexConfig {
                        deletion_mode: mode,
                        inline_max_rows,
                        stream_publish_interval_ms: 0,
                        compaction_background_interval_ms: 3_600_000,
                        ..VortexConfig::default()
                    },
                    upsert_on_id(),
                )
                .await;
                let rows = Arc::new(SupersededRows::default());
                let ctx = SessionContext::new();
                let state = with_superseded_rows(&ctx.state(), Arc::clone(&rows));
                // Key 1: an identical copy in its own batch and in a later one.
                // Key 2: a different version in a later batch.
                let source = MemorySourceConfig::try_new_exec(
                    &[vec![
                        batch(&[(1, "a"), (2, "a"), (1, "a")]),
                        batch(&[(2, "b"), (3, "a")]),
                        batch(&[(1, "a")]),
                    ]],
                    schema(),
                    None,
                )
                .expect("source");
                let plan = provider
                    .insert_into(&state, source, op)
                    .await
                    .expect("plan");
                collect(plan, ctx.task_ctx())
                    .await
                    .unwrap_or_else(|error| panic!("{label}: {error}"));
                assert_eq!(
                    visible(&provider).await,
                    (owned(&[(1, "a"), (2, "b"), (3, "a")]), 3),
                    "{label}: rows"
                );
                assert_eq!(counted(&rows), 3, "{label}: counted");
            }
        }

        // A table created with `DoNothing` replaces the stored row too.
        let (provider, _catalog, _runtime_env, _dir) = upsert_table(mode, drop_on_id()).await;
        write(&provider, InsertOp::Append, vec![batch(&[(9, "old")])])
            .await
            .expect("seed");
        let rows = Arc::new(SupersededRows::default());
        let ctx = SessionContext::new();
        let state = with_superseded_rows(&ctx.state(), Arc::clone(&rows));
        let source = MemorySourceConfig::try_new_exec(
            &[vec![batch(&[(9, "new"), (4, "a")])]],
            schema(),
            None,
        )
        .expect("source");
        let plan = provider
            .insert_into(&state, source, InsertOp::Append)
            .await
            .expect("plan");
        collect(plan, ctx.task_ctx()).await.expect("append");
        assert_eq!(
            visible(&provider).await,
            (owned(&[(4, "a"), (9, "new")]), 2),
            "{mode:?}: the incoming row replaces the stored one"
        );
        assert_eq!(counted(&rows), 0, "{mode:?}: nothing received was dropped");
    }
}
