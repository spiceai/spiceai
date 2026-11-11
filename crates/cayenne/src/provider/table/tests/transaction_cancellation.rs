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

//! A caller cancelled after a transaction's shared catalog commit is durable
//! but before the in-memory switch, for both publication routes: a keyed table
//! (protected snapshot) and a table without a primary key (carried
//! generation). The outcome it never observed is kept (no committed file is
//! removed) and the table refuses writes until it is reloaded, so nothing is
//! written against an in-memory view the catalog has moved past; a reopen
//! reads exactly the committed rows.
//!
//! Contract (fail-closed, NOT a normal finish): the in-memory switch does not
//! run, so the live instance never serves the commit; it refuses writes until
//! the table is reloaded, and the reload serves the durable outcome. Locks are
//! released by unwinding the cancelled future (asserted below). A fused
//! transaction acknowledges no source position, so there is no source ack to
//! leak. Cancellation before the shared COMMIT starts is not covered here.

use super::*;
use crate::provider::append_commit::test_seams;
use crate::provider::transaction::CayenneTransaction;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

async fn rows(provider: &CayenneTableProvider) -> Vec<(i64, i64)> {
    let ctx = SessionContext::new();
    let plan = provider
        .scan(&ctx.state(), None, &[], None)
        .await
        .expect("scan plan");
    let batches = collect(plan, ctx.task_ctx()).await.expect("scan");
    let mut out = Vec::new();
    for b in &batches {
        let ids = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id");
        let values = b
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("v");
        out.extend((0..b.num_rows()).map(|r| (ids.value(r), values.value(r))));
    }
    out.sort_unstable();
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_caller_cancelled_after_the_durable_commit_keeps_the_outcome_and_refuses_writes() {
    for keyed in [true, false] {
        let label = if keyed { "protected" } else { "generation" };
        let dir = tempfile::tempdir().expect("temp dir");
        let metadata_dir = format!("{}/metadata", dir.path().to_str().expect("path"));
        std::fs::create_dir_all(&metadata_dir).expect("metadata dir");
        let catalog = Arc::new(
            crate::CayenneCatalog::new(format!("sqlite://{metadata_dir}/cayenne.db"))
                .expect("catalog"),
        ) as Arc<dyn MetadataCatalog>;
        catalog.init().await.expect("catalog init");
        let name = format!("txn_cancel_{label}");
        let p = CayenneTableProviderBuilder::new(
            Arc::clone(&catalog),
            SessionContext::new().runtime_env(),
        )
        .create(CreateTableOptions {
            table_name: name.clone(),
            schema: schema(),
            primary_key: if keyed {
                vec!["id".to_string()]
            } else {
                vec![]
            },
            on_conflict: keyed.then(|| {
                OnConflict::Upsert(
                    datafusion_table_providers::util::column_reference::ColumnReference::new(vec![
                        "id".to_string(),
                    ]),
                )
            }),
            base_path: format!("{}/data", dir.path().to_str().expect("path")),
            partition_column: None,
            vortex_config: VortexConfig {
                deletion_mode: crate::metadata::DeletionMode::Position,
                inline_max_rows: 0,
                inline_max_bytes: 0,
                inline_max_buffer_bytes: 0,
                compaction_background_interval_ms: 3_600_000,
                ..VortexConfig::default()
            },
        })
        .await
        .expect("table created");
        insert_batch(&p, id_value_batch(schema(), &[1, 2], &[1, 2])).await;

        // The commit reaches the durable catalog commit, signals, and never
        // resumes: the task is aborted there.
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel::<()>();
        test_seams::pause_after_commit(
            p.table_id(),
            Box::new(move || {
                Box::pin(async move {
                    reached_tx.send(()).expect("test waits");
                    futures::future::pending::<()>().await;
                })
            }),
        );
        let table = p.clone_for_write_operations();
        let task = tokio::spawn(async move {
            let txn = CayenneTransaction::new();
            let token = table.transaction_write_token().await;
            txn.register(
                table.table_id().to_string(),
                token,
                table.clone_for_write_operations(),
            );
            // Keyed: supersedes key 2 (a position delete in the commit).
            let staged = table
                .begin_staged_upsert_occ(
                    token,
                    single_batch_stream(id_value_batch(schema(), &[2, 10], &[20, 10])),
                    1,
                )
                .await
                .expect("staged");
            txn.set_staged(table.table_id(), staged);
            txn.commit().await
        });
        reached_rx
            .await
            .expect("the commit reached its durable point");
        task.abort();
        assert!(task.await.expect_err("aborted").is_cancelled(), "{label}");

        // The cancelled commit released every lock it held: write lock,
        // visibility lock and listing fence are all acquirable.
        let free = std::time::Duration::from_secs(5);
        let _w = tokio::time::timeout(free, p.write_lock_arc().lock_owned())
            .await
            .unwrap_or_else(|_| panic!("{label}: write lock released"));
        let _v = tokio::time::timeout(free, p.visibility_lock_arc().lock_owned())
            .await
            .unwrap_or_else(|_| panic!("{label}: visibility lock released"));
        let _l = tokio::time::timeout(free, p.lock_listing_fence_write_owned())
            .await
            .unwrap_or_else(|_| panic!("{label}: listing fence released"));
        drop((_l, _v, _w));

        let committed = if keyed {
            vec![(1, 1), (2, 20), (10, 10)]
        } else {
            vec![(1, 1), (2, 2), (2, 20), (10, 10)]
        };
        let error = p
            .insert_into(
                &SessionContext::new().state(),
                datafusion::datasource::memory::MemorySourceConfig::try_new_exec(
                    &[vec![id_value_batch(schema(), &[99], &[99])]],
                    schema(),
                    None,
                )
                .expect("source"),
                datafusion::logical_expr::dml::InsertOp::Append,
            )
            .await
            .map(|plan| (plan, SessionContext::new()));
        let refused = match error {
            Err(error) => Some(error.to_string()),
            Ok((plan, ctx)) => collect(plan, ctx.task_ctx())
                .await
                .err()
                .map(|e| e.to_string()),
        };
        assert!(
            refused
                .as_deref()
                .is_some_and(|e| e.contains("refused until the table is reloaded")),
            "{label}: a write after an unobserved durable commit is refused: {refused:?}"
        );
        let reopened = CayenneTableProviderBuilder::new(
            Arc::clone(&catalog),
            SessionContext::new().runtime_env(),
        )
        .open(&name)
        .await
        .expect("reopen");
        assert_eq!(
            rows(&reopened).await,
            committed,
            "{label}: reopened exactly"
        );
    }
}
