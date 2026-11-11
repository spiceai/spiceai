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

//! A transactional write to a table without a primary key (deletes per file)
//! publishes the complete current generation: its staged snapshot carries
//! every committed run with its deletes and becomes `current_snapshot_id` in
//! the shared commit. No `retention_sql` here: this is the durability repair,
//! independent of retention.

use super::*;
use crate::provider::append_commit::test_seams::{self, CommitFault};
use crate::provider::transaction::CayenneTransaction;
use datafusion::prelude::{col, lit};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

struct Fixture {
    provider: CayenneTableProvider,
    catalog: Arc<dyn MetadataCatalog>,
    runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
    name: String,
    _dir: TempDir,
}

impl Fixture {
    async fn reopen(&self) -> CayenneTableProvider {
        CayenneTableProviderBuilder::new(Arc::clone(&self.catalog), Arc::clone(&self.runtime_env))
            .open(&self.name)
            .await
            .expect("reopen")
    }
}

async fn unkeyed(name: &str) -> Fixture {
    let dir = tempfile::tempdir().expect("temp dir");
    let metadata_dir = format!("{}/metadata", dir.path().to_str().expect("path"));
    std::fs::create_dir_all(&metadata_dir).expect("metadata dir");
    let catalog = Arc::new(
        crate::CayenneCatalog::new(format!("sqlite://{metadata_dir}/cayenne.db")).expect("catalog"),
    ) as Arc<dyn MetadataCatalog>;
    catalog.init().await.expect("catalog init");
    let runtime_env = SessionContext::new().runtime_env();
    let provider = CayenneTableProviderBuilder::new(Arc::clone(&catalog), Arc::clone(&runtime_env))
        .create(CreateTableOptions {
            table_name: name.to_string(),
            schema: schema(),
            primary_key: vec![],
            on_conflict: None,
            base_path: format!("{}/data", dir.path().to_str().expect("path")),
            partition_column: None,
            vortex_config: VortexConfig {
                inline_max_rows: 0,
                inline_max_bytes: 0,
                inline_max_buffer_bytes: 0,
                compaction_background_interval_ms: 3_600_000,
                ..VortexConfig::default()
            },
        })
        .await
        .expect("table created");
    assert!(provider.pk_deletion_strategy.is_position_based());
    Fixture {
        provider,
        catalog,
        runtime_env,
        name: name.to_string(),
        _dir: dir,
    }
}

async fn ids(provider: &CayenneTableProvider) -> Vec<i64> {
    let ctx = SessionContext::new();
    let plan = provider
        .scan(&ctx.state(), None, &[], None)
        .await
        .expect("scan plan");
    collect_ids(plan, &ctx).await
}

async fn collect_ids(plan: Arc<dyn ExecutionPlan>, ctx: &SessionContext) -> Vec<i64> {
    let batches = collect(plan, ctx.task_ctx()).await.expect("scan");
    let mut out: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            let c = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id");
            (0..c.len()).map(|i| c.value(i)).collect::<Vec<_>>()
        })
        .collect();
    out.sort_unstable();
    out
}

async fn delete_id(provider: &CayenneTableProvider, id: i64) {
    let ctx = SessionContext::new();
    let plan = provider
        .delete_from(&ctx.state(), vec![col("id").eq(lit(id))])
        .await
        .expect("delete plan");
    collect(plan, ctx.task_ctx()).await.expect("delete");
}

async fn transaction(provider: &CayenneTableProvider, rows: &[i64]) -> Result<()> {
    let txn = CayenneTransaction::new();
    let token = provider.transaction_write_token().await;
    txn.register(
        provider.table_id().to_string(),
        token,
        provider.clone_for_write_operations(),
    );
    let staged = provider
        .begin_staged_upsert_occ(
            token,
            single_batch_stream(id_value_batch(schema(), rows, rows)),
            1,
        )
        .await?;
    txn.set_staged(provider.table_id(), staged);
    txn.commit().await.map(|_| ())
}

/// Committed transactions survive a reopen with the rows written before them,
/// a per-file delete before them, and each other; the generation is the
/// current snapshot and no protected snapshot is left in memory. A scan
/// planned before a commit still reads the generation it planned against.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unkeyed_transactions_publish_the_complete_current_generation() {
    let f = unkeyed("ugen_reopen").await;
    let p = &f.provider;
    insert_batch(p, id_value_batch(schema(), &[1, 2, 3], &[1, 2, 3])).await;
    delete_id(p, 2).await;
    assert_eq!(ids(p).await, vec![1, 3]);

    let ctx = SessionContext::new();
    let old_plan = p
        .scan(&ctx.state(), None, &[], None)
        .await
        .expect("old plan");
    let before = p.get_current_snapshot_id();
    transaction(p, &[10, 11]).await.expect("first transaction");
    assert_ne!(
        p.get_current_snapshot_id(),
        before,
        "the generation is current"
    );
    assert!(
        p.protected_snapshots.load().is_empty(),
        "no protected snapshot holds the transaction's rows"
    );
    assert_eq!(ids(p).await, vec![1, 3, 10, 11], "live");
    assert_eq!(
        collect_ids(old_plan, &ctx).await,
        vec![1, 3],
        "a scan planned before the commit reads its own generation"
    );
    transaction(p, &[20]).await.expect("second transaction");
    insert_batch(p, id_value_batch(schema(), &[30], &[30])).await;
    delete_id(p, 10).await;
    let expected = vec![1, 3, 11, 20, 30];
    assert_eq!(ids(p).await, expected, "live after more writes");
    assert_eq!(ids(&f.reopen().await).await, expected, "reopened");
}

/// A shared commit that reports a failure: committed (read back by
/// `current_snapshot_id`) it is published; rolled back (a crash before the
/// durable pointer) nothing is published or durable and a retry commits; of
/// unknown outcome, writes are refused and the staged files are kept, and a
/// reopen (a crash after the durable pointer) reads exactly the committed rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unkeyed_transaction_failed_commit_is_resolved_by_the_durable_pointer() {
    for fault in [
        CommitFault::CommittedButReported,
        CommitFault::RolledBack,
        CommitFault::CommittedUnreadable,
    ] {
        let f = unkeyed(&format!("ugen_{fault:?}").to_lowercase()).await;
        let p = &f.provider;
        insert_batch(p, id_value_batch(schema(), &[1, 2], &[1, 2])).await;
        delete_id(p, 2).await;
        let before = p.get_current_snapshot_id();
        test_seams::inject(p.table_id(), fault);
        let result = transaction(p, &[10]).await;
        let committed = vec![1, 10];
        match fault {
            CommitFault::CommittedButReported => {
                result.unwrap_or_else(|e| panic!("{fault:?}: published: {e}"));
                assert_eq!(ids(p).await, committed, "{fault:?}: live");
            }
            CommitFault::RolledBack => {
                let error = result.expect_err("uncommitted fails");
                assert!(
                    error.to_string().contains("injected commit failure"),
                    "{error}"
                );
                assert_eq!(p.get_current_snapshot_id(), before, "{fault:?}: pointer");
                assert_eq!(ids(p).await, vec![1], "{fault:?}: live unchanged");
                assert_eq!(
                    ids(&f.reopen().await).await,
                    vec![1],
                    "{fault:?}: durable unchanged"
                );
                transaction(p, &[10]).await.expect("retry");
                assert_eq!(ids(p).await, committed, "{fault:?}: retried");
            }
            CommitFault::CommittedUnreadable => {
                let error = result.expect_err("unknown outcome fails");
                assert!(
                    error
                        .to_string()
                        .contains("reading back whether it committed failed too"),
                    "{error}"
                );
                assert!(
                    transaction(p, &[99]).await.is_err(),
                    "{fault:?}: writes refused until reload"
                );
            }
        }
        assert_eq!(
            ids(&f.reopen().await).await,
            committed,
            "{fault:?}: reopened"
        );
    }
}

fn data_files(root: &std::path::Path) -> usize {
    let Ok(entries) = std::fs::read_dir(root) else {
        return 0;
    };
    entries
        .flatten()
        .map(|e| {
            let path = e.path();
            if path.is_dir() { data_files(&path) } else { 1 }
        })
        .sum()
}

/// The single-table `CayenneStagedUpsert::commit` (no `CayenneTransaction`)
/// is refused on a table without a primary key: at 9aa41988c it published a
/// protected snapshot that a reopen does not load (the pre-existing unkeyed
/// reopen loss, `unkeyed_transaction_reopen_test`), and the carried generation
/// that fixes it must flip under the listing fence only the fused commit
/// holds. The refusal is explicit, publishes nothing, and leaves no staged
/// files behind; the table stays writable. In-repo callers of this API are
/// tests only (`grep staged.commit(`); it remains `pub` via `cayenne::CayenneStagedUpsert`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_single_table_staged_commit_without_a_primary_key_is_refused() {
    let f = unkeyed("ut_gen_single_table_refused");
    let f = f.await;
    let p = &f.provider;
    insert_batch(p, id_value_batch(schema(), &[1, 2], &[1, 2])).await;
    let root = std::path::PathBuf::from(&p.metadata().path);
    let files_before = data_files(&root);

    let token = p.transaction_write_token().await;
    let staged = p
        .begin_staged_upsert_occ(
            token,
            single_batch_stream(id_value_batch(schema(), &[10], &[10])),
            1,
        )
        .await
        .expect("staged");
    let error = staged
        .commit(std::collections::HashSet::new(), true)
        .await
        .expect_err("refused");
    assert!(
        matches!(error, Error::Unsupported { .. }),
        "an explicit unsupported-operation error: {error}"
    );
    assert_eq!(ids(p).await, vec![1, 2], "nothing published");
    assert_eq!(
        data_files(&root),
        files_before,
        "no staged files left behind"
    );
    assert_eq!(ids(&f.reopen().await).await, vec![1, 2], "reopened");

    transaction(p, &[11])
        .await
        .expect("the fused route still commits");
    assert_eq!(
        ids(&f.reopen().await).await,
        vec![1, 2, 11],
        "reopened after txn"
    );
}
