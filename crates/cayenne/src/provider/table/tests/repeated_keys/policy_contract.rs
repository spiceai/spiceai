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

//! Per-statement conflict policies and failed-refresh visibility (#14576).

use super::*;

const CONFLICT_CAUSE: &str = "its data holds different versions of 1 value of 'id', and `on_conflict: upsert` does not choose between versions. Set `on_conflict` to `upsert_by_time` to keep the newest by `time_column`, or `upsert_by_arrival` to keep the version that arrived last. See: https://spiceai.org/docs/features/data-acceleration/constraints";

async fn apply(
    provider: &CayenneTableProvider,
    op: InsertOp,
    batches: Vec<RecordBatch>,
    user_statement: bool,
) -> datafusion_common::Result<()> {
    let ctx = SessionContext::new();
    let state = if user_statement {
        util::session_state::mark_user_statement(&ctx.state())
    } else {
        ctx.state()
    };
    let source = MemorySourceConfig::try_new_exec(&[batches], schema(), None)?;
    let plan = provider.insert_into(&state, source, op).await?;
    collect(plan, ctx.task_ctx()).await.map(|_| ())
}

#[tokio::test(flavor = "multi_thread")]
async fn upsert_rejects_different_versions_without_changing_stored_rows() {
    let mut failures = Vec::new();
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        for dedup in [UpsertDedup::None, UpsertDedup::DropIdentical] {
            for op in [InsertOp::Overwrite, InsertOp::Append] {
                for user_statement in [false, true] {
                    for split in [false, true] {
                        let label = format!(
                            "{mode:?}/{dedup:?}/{op:?}/user={user_statement}/split={split}"
                        );
                        let (provider, catalog, runtime_env, _dir) = table(mode, dedup).await;
                        write(&provider, InsertOp::Append, vec![batch(&[(9, "old")])])
                            .await
                            .expect("seed");
                        let batches = if split {
                            vec![batch(&[(1, "a"), (2, "b")]), batch(&[(1, "c")])]
                        } else {
                            vec![batch(&[(1, "a"), (2, "b"), (1, "c")])]
                        };
                        match apply(&provider, op, batches, user_statement).await {
                            Ok(()) => failures.push(format!("{label}: ambiguous write succeeded")),
                            Err(error) if error.to_string().contains(CONFLICT_CAUSE) => {}
                            Err(error) => failures.push(format!("{label}: wrong cause: {error}")),
                        }
                        let expected = (owned(&[(9, "old")]), 1);
                        let actual = visible(&provider).await;
                        if actual != expected {
                            failures
                                .push(format!("{label}: failed write changed rows: {actual:?}"));
                        }
                        let reopened = reopen(&catalog, &runtime_env, dedup).await;
                        let actual = visible(&reopened).await;
                        if actual != expected {
                            failures.push(format!("{label}: persisted rows changed: {actual:?}"));
                        }
                    }
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test(flavor = "multi_thread")]
async fn identical_copies_collapse_for_refreshes_and_statements() {
    let mut failures = Vec::new();
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        for dedup in [UpsertDedup::None, UpsertDedup::DropIdentical] {
            for op in [InsertOp::Overwrite, InsertOp::Append] {
                for user_statement in [false, true] {
                    for split in [false, true] {
                        let label = format!(
                            "{mode:?}/{dedup:?}/{op:?}/user={user_statement}/split={split}"
                        );
                        let (provider, catalog, runtime_env, _dir) = table(mode, dedup).await;
                        write(&provider, InsertOp::Append, vec![batch(&[(9, "old")])])
                            .await
                            .expect("seed");
                        let batches = if split {
                            vec![batch(&[(1, "a"), (2, "b")]), batch(&[(1, "a")])]
                        } else {
                            vec![batch(&[(1, "a"), (2, "b"), (1, "a")])]
                        };
                        if let Err(error) = apply(&provider, op, batches, user_statement).await {
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
                        let reopened = reopen(&catalog, &runtime_env, dedup).await;
                        let actual = visible(&reopened).await;
                        if actual != expected {
                            failures.push(format!("{label}: wrong persisted rows: {actual:?}"));
                        }
                    }
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_snapshot_publication_keeps_prior_rows_after_reopen() {
    let mut failures = Vec::new();
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        let (provider, catalog, runtime_env, _dir) = table(mode, UpsertDedup::KeepLast).await;
        write(&provider, InsertOp::Append, vec![batch(&[(9, "old")])])
            .await
            .expect("seed");
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

        let error = write(&provider, InsertOp::Append, vec![batch(&[(9, "new")])])
            .await
            .expect_err("the snapshot metadata write must reach the fault");
        assert!(
            error
                .to_string()
                .contains("injected snapshot publish failure"),
            "{mode:?}: unexpected error: {error}",
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
                "{mode:?}: live data changed after failure: {actual:?}"
            ));
        }
        let reopened = reopen(&catalog, &runtime_env, UpsertDedup::KeepLast).await;
        let actual = visible(&reopened).await;
        if actual != expected {
            failures.push(format!(
                "{mode:?}: durable data changed after failure: {actual:?}"
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test(flavor = "multi_thread")]
async fn late_append_error_does_not_publish_an_earlier_segment() {
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        let runtime_env = SessionContext::new().runtime_env();
        let (mut provider, catalog, _dir) = create_cdc_table_with_schema(
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
        provider.upsert_dedup = UpsertDedup::KeepLast;
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
        let reopened = reopen(&catalog, &runtime_env, UpsertDedup::KeepLast).await;
        assert_unchanged(visible(&reopened).await);
    }
}
