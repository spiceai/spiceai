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

//! The object-native catalog's commit under injected store faults: an uncertain
//! head CAS is resolved and lands exactly once, and a fenced writer stops.

use std::sync::Arc;

use cayenne::metastore::object_native::ObjectNativeCatalog;
use object_store::{ObjectStore, memory::InMemory, path::Path};
use object_store_occ::headlog::Reader;
use object_store_sim::{
    Op,
    fault::{Fault, FaultStore},
};

fn is_head(path: &Path) -> bool {
    path.as_ref() == "catalog/head"
}

/// Head CAS requests that reached storage and succeeded.
fn landed_head_writes(store: &FaultStore) -> usize {
    store
        .log()
        .iter()
        .filter(|applied| applied.op == Op::Cas && is_head(&applied.path) && applied.result.is_ok())
        .count()
}

async fn head_commit(store: &Arc<dyn ObjectStore>) -> u64 {
    let (reader, _) = Reader::open(Arc::clone(store), Path::from("catalog"))
        .await
        .expect("open the head log");
    reader.state().head.commit
}

async fn setup() -> (Arc<FaultStore>, Arc<dyn ObjectStore>, ObjectNativeCatalog) {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let store: Arc<dyn ObjectStore> = Arc::clone(&faults) as Arc<dyn ObjectStore>;
    let catalog = ObjectNativeCatalog::create(Arc::clone(&store), Path::from("catalog"))
        .await
        .expect("create the catalog");
    catalog
        .set_sidecar_value("position", Some(b"1".to_vec()))
        .await
        .expect("first commit");
    (faults, store, catalog)
}

/// The CAS lands but its response is lost, and the read that would resolve it
/// fails too: the head log returns an error, not an outcome. The catalog must
/// resolve the same attempt from the head, find it accepted, and not send it
/// again.
#[tokio::test]
async fn a_lost_response_resolves_to_one_accepted_commit() {
    let (faults, store, catalog) = setup().await;
    let before = head_commit(&store).await;
    let writes = landed_head_writes(&faults);

    faults.arm(Op::Cas, is_head, Fault::DropResponse);
    faults.arm(Op::Get, is_head, Fault::Fail);
    catalog
        .set_sidecar_value("position", Some(b"2".to_vec()))
        .await
        .expect("the uncertain commit resolves as accepted");

    assert_eq!(landed_head_writes(&faults) - writes, 1);
    assert_eq!(head_commit(&store).await, before + 1);
    assert_eq!(
        catalog.sidecar_value("position").await.expect("read"),
        Some(b"2".to_vec())
    );
    drop(catalog);
    let reopened = ObjectNativeCatalog::open(Arc::clone(&store), Path::from("catalog"))
        .await
        .expect("reopen");
    assert_eq!(
        reopened.sidecar_value("position").await.expect("read"),
        Some(b"2".to_vec())
    );
}

/// The CAS fails before reaching storage, and the read that would resolve it
/// fails: the catalog must find the base unchanged and send the same bytes
/// under the same condition, once.
#[tokio::test]
async fn a_failed_send_is_retried_with_the_same_attempt() {
    let (faults, store, catalog) = setup().await;
    let before = head_commit(&store).await;
    let writes = landed_head_writes(&faults);

    faults.arm(Op::Cas, is_head, Fault::Fail);
    faults.arm(Op::Get, is_head, Fault::Fail);
    catalog
        .set_sidecar_value("position", Some(b"2".to_vec()))
        .await
        .expect("the failed commit is retried and accepted");

    assert_eq!(landed_head_writes(&faults) - writes, 1);
    let blocked: Vec<_> = faults
        .blocked()
        .into_iter()
        .filter(|(path, _, _)| is_head(path))
        .collect();
    assert_eq!(blocked.len(), 1);
    let sent = faults
        .log()
        .into_iter()
        .rfind(|applied| applied.op == Op::Cas && is_head(&applied.path))
        .and_then(|applied| applied.payload)
        .expect("the retried CAS");
    assert_eq!(
        sent.as_ref().concat(),
        blocked[0].1.as_ref().concat(),
        "the retry sends the bytes of the failed attempt"
    );
    assert_eq!(head_commit(&store).await, before + 1);
}

/// Another writer took the log over: the old catalog's commit is fenced, it
/// refuses further work, and its change is not in the log.
#[tokio::test]
async fn a_fenced_catalog_stops_and_its_change_is_absent() {
    let (_faults, store, catalog) = setup().await;
    let successor = ObjectNativeCatalog::open(Arc::clone(&store), Path::from("catalog"))
        .await
        .expect("take over");
    let before = head_commit(&store).await;

    let fenced = catalog
        .set_sidecar_value("position", Some(b"stale".to_vec()))
        .await
        .expect_err("a fenced commit is refused");
    assert!(
        fenced.to_string().contains("Fenced"),
        "unexpected error: {fenced}"
    );
    let refused = catalog
        .set_sidecar_value("position", Some(b"later".to_vec()))
        .await
        .expect_err("a fenced catalog refuses later work");
    assert_eq!(
        refused.to_string(),
        "Invalid operation: The Cayenne object-native metastore stopped writing, because another Spice instance now writes its state; this instance keeps answering queries from the state saved so far. Restart this instance to make it the writer again."
    );

    assert_eq!(head_commit(&store).await, before);
    assert_eq!(
        successor.sidecar_value("position").await.expect("read"),
        Some(b"1".to_vec())
    );
    assert_eq!(
        catalog
            .sidecar_value("position")
            .await
            .expect("a fenced catalog still reads"),
        Some(b"1".to_vec()),
        "the fenced catalog reads the committed value, not its refused change"
    );
}

/// The source positions each record committed to `dataset`'s log holds, oldest
/// first, from the head's window (these tests commit too few records to seal a
/// batch).
async fn committed_positions(store: &Arc<dyn ObjectStore>, dataset: &str) -> Vec<Vec<String>> {
    use prost::Message;
    let log = cayenne::metastore::object_native::dataset_log_at(
        Arc::clone(store),
        &Path::from("catalog"),
        dataset,
    )
    .await
    .expect("read the registry")
    .expect("the dataset has a log");
    let (reader, _) = Reader::open(Arc::clone(store), log)
        .await
        .expect("open the head log");
    reader
        .state()
        .head
        .window
        .iter()
        .map(|entry| {
            let record = object_store_occ::headlog::Record::decode(entry.encoded.as_slice())
                .expect("log record");
            object_store_occ::headlog::Group::decode(record.payload)
                .expect("group")
                .payloads
                .into_iter()
                .flat_map(|payload| {
                    cayenne_metastore_actions::Record::decode(payload)
                        .expect("catalog record")
                        .actions
                })
                .filter_map(|action| match action.kind {
                    Some(cayenne_metastore_actions::Kind::AdvanceSourcePosition(position)) => {
                        Some(position.source)
                    }
                    _ => None,
                })
                .collect()
        })
        .collect()
}

async fn create(catalog: &ObjectNativeCatalog, name: &str) -> String {
    use cayenne::MetadataCatalog;
    use cayenne::metadata::{CreateTableOptions, VortexConfig};
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
    ]));
    catalog
        .create_table(CreateTableOptions {
            table_name: name.to_string(),
            schema,
            primary_key: vec!["id".to_string()],
            on_conflict: None,
            base_path: format!("/tmp/unused/{name}"),
            partition_column: None,
            vortex_config: VortexConfig::default(),
        })
        .await
        .expect("create table")
}

/// A dataset's buffered changes commit at its own acknowledgement point, in
/// one record with its source position: another dataset's commit publishes only
/// its own changes.
#[tokio::test]
async fn another_datasets_commit_leaves_a_datasets_buffered_changes_pending() {
    use cayenne::MetadataCatalog;
    let (_faults, store, catalog) = setup().await;
    let orders = create(&catalog, "orders").await;
    create(&catalog, "audit").await;
    // orders stages its change and position for its acknowledgement point.
    cayenne::catalog::with_deferred_commits(async {
        catalog
            .set_sidecar_value("spice_sys_checkpoint/orders", Some(b"data".to_vec()))
            .await
            .expect("stage orders' change");
        catalog
            .set_sidecar_value("spice_sys_applied_lsn/orders", Some(b"10".to_vec()))
            .await
            .expect("stage orders' position");
    })
    .await;
    // audit commits on its own meanwhile.
    catalog
        .set_sidecar_value("spice_sys_applied_lsn/audit", Some(b"7".to_vec()))
        .await
        .expect("audit commits");
    let records = committed_positions(&store, "audit").await;
    assert_eq!(
        records.last(),
        Some(&vec!["spice_sys_applied_lsn/audit".to_string()]),
        "audit's commit did not commit its position: {records:?}"
    );
    let records = committed_positions(&store, "orders").await;
    assert!(
        records.iter().all(Vec::is_empty),
        "audit's commit published orders' buffered changes: {records:?}"
    );
    catalog
        .commit_pending_for(&orders)
        .await
        .expect("orders' acknowledgement point");
    let records = committed_positions(&store, "orders").await;
    assert_eq!(
        records.last(),
        Some(&vec![
            "spice_sys_checkpoint/orders".to_string(),
            "spice_sys_applied_lsn/orders".to_string()
        ]),
        "orders' change and position did not commit as one record: {records:?}"
    );
}

/// A table dropped in a deferred scope and created again under its name outside
/// it: the log must replay, and hold only the new table under that name.
#[tokio::test]
async fn a_name_dropped_in_a_deferred_scope_can_be_created_again() {
    use cayenne::MetadataCatalog;
    let (_faults, store, catalog) = setup().await;
    let first = create(&catalog, "orders").await;
    cayenne::catalog::with_deferred_commits(async {
        catalog.drop_table("orders").await.expect("drop orders");
    })
    .await;
    let second = create(&catalog, "orders").await;
    assert_ne!(first, second);
    drop(catalog);
    let reopened = ObjectNativeCatalog::open(Arc::clone(&store), Path::from("catalog"))
        .await
        .expect("the log replays");
    assert_eq!(
        reopened.get_table("orders").await.expect("orders").table_id,
        second
    );
}

/// Datasets commit independently: with one dataset's head CAS held in flight,
/// another dataset's commit completes.
#[tokio::test]
async fn a_dataset_commits_while_another_datasets_commit_is_in_flight() {
    let (faults, _store, catalog) = setup().await;
    create(&catalog, "orders").await;
    create(&catalog, "audit").await;
    let catalog = Arc::new(catalog);
    let orders_head = catalog
        .dataset_log("orders")
        .expect("orders has a log")
        .join("head");
    faults.arm(
        Op::Cas,
        move |path: &Path| path == &orders_head,
        Fault::Hold,
    );
    let orders = tokio::spawn({
        let catalog = Arc::clone(&catalog);
        async move {
            catalog
                .set_sidecar_value("spice_sys_applied_lsn/orders", Some(b"1".to_vec()))
                .await
        }
    });
    let held = faults.wait_held(None).await;
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        catalog.set_sidecar_value("spice_sys_applied_lsn/audit", Some(b"2".to_vec())),
    )
    .await
    .expect("audit's commit waited for orders' head CAS")
    .expect("audit commits");
    assert!(!orders.is_finished(), "orders' commit is still held");
    faults.clear_rules();
    faults.deliver(held).await.expect("deliver orders' CAS");
    faults.respond(held).expect("release orders' response");
    orders
        .await
        .expect("orders' task")
        .expect("orders commits once its CAS is delivered");
    assert_eq!(
        catalog
            .sidecar_value("spice_sys_applied_lsn/orders")
            .await
            .expect("read orders"),
        Some(b"1".to_vec())
    );
}

// ---------------------------------------------------------------------------
// Commit outcomes of a dataset whose data files are in the location.
// ---------------------------------------------------------------------------

mod data_path {
    use super::*;
    use arrow::array::Int64Array;
    use cayenne::MetadataCatalog;
    use cayenne::metadata::{CreateTableOptions, ObjectStoreConfig, VortexConfig};
    use cayenne::metastore::object_native::{NativeCatalogConfig, new_dataset_folder};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::TableProvider;
    use datafusion::prelude::SessionContext;

    const CATALOG: &str = "state/cayenne/v1/catalog";
    const LOCATION: &str = "s3://bucket/state/cayenne/v1";

    /// Few, short retries, so an unresolved commit reaches the background reopen quickly.
    fn fast() -> NativeCatalogConfig {
        NativeCatalogConfig {
            publish_attempts: 2,
            retry_delay: std::time::Duration::from_millis(10),
            ..NativeCatalogConfig::default()
        }
    }

    async fn catalog(store: &Arc<dyn ObjectStore>) -> ObjectNativeCatalog {
        let catalog = ObjectNativeCatalog::open_or_create_at_with_config(
            Arc::clone(store),
            Path::from(CATALOG),
            LOCATION,
            fast(),
        )
        .await
        .expect("open the catalog");
        catalog.init().await.expect("init");
        catalog
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]))
    }

    struct Placed {
        data_url: String,
        ctx: SessionContext,
        table: Arc<cayenne::CayenneTableProvider>,
    }

    async fn place(
        store: &Arc<dyn ObjectStore>,
        catalog: &Arc<dyn MetadataCatalog>,
        name: &str,
        create: bool,
        data_url: Option<String>,
    ) -> Placed {
        place_with_key(store, catalog, name, create, data_url, false).await
    }

    async fn place_with_key(
        store: &Arc<dyn ObjectStore>,
        catalog: &Arc<dyn MetadataCatalog>,
        name: &str,
        create: bool,
        data_url: Option<String>,
        keyed: bool,
    ) -> Placed {
        place_with_config(
            store,
            catalog,
            name,
            create,
            data_url,
            keyed,
            VortexConfig {
                inline_max_rows: 0,
                inline_max_bytes: 0,
                ..VortexConfig::default()
            },
        )
        .await
    }

    /// Small files and a pick of 3, so an explicit compaction pass runs.
    fn compactable() -> VortexConfig {
        VortexConfig {
            inline_max_rows: 0,
            inline_max_bytes: 0,
            compaction_trigger_files: 3,
            target_vortex_file_size_mb: 1,
            compaction_max_levels: 3,
            compaction_max_files_per_pick: 3,
            compaction_background_interval_ms: 0,
            ..VortexConfig::default()
        }
    }

    async fn place_with_config(
        store: &Arc<dyn ObjectStore>,
        catalog: &Arc<dyn MetadataCatalog>,
        name: &str,
        create: bool,
        data_url: Option<String>,
        keyed: bool,
        vortex_config: VortexConfig,
    ) -> Placed {
        let data_url = data_url.unwrap_or_else(|| {
            format!(
                "s3://bucket/state/cayenne/v1/datasets/{}/data",
                new_dataset_folder(name)
            )
        });
        let ctx = SessionContext::new();
        let builder = crate::common::table_builder_on_store(
            Arc::clone(catalog),
            ctx.runtime_env(),
            ObjectStoreConfig {
                url: url::Url::parse(&data_url).expect("data url"),
                store: Arc::clone(store),
            },
        );
        let table = if create {
            builder
                .create(CreateTableOptions {
                    table_name: name.to_string(),
                    schema: schema(),
                    primary_key: if keyed { vec!["id".to_string()] } else { vec![] },
                    on_conflict: keyed.then(|| {
                        datafusion_table_providers::util::on_conflict::OnConflict::Upsert(
                            datafusion_table_providers::util::column_reference::ColumnReference::new(
                                vec!["id".to_string()],
                            ),
                        )
                    }),
                    base_path: data_url.clone(),
                    partition_column: None,
                    vortex_config,
                })
                .await
                .expect("create the table")
        } else {
            builder.open(name).await.expect("open the table")
        };
        let table = Arc::new(table);
        ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)
            .expect("register");
        Placed {
            data_url,
            ctx,
            table,
        }
    }

    /// Upsert ids `first..=last` into each of `tables` in one transaction.
    async fn commit_across(
        tables: &[Arc<cayenne::CayenneTableProvider>],
        first: i64,
        last: i64,
    ) -> Result<cayenne::TransactionCommit, cayenne::provider::Error> {
        let txn = cayenne::CayenneTransaction::new();
        for table in tables {
            let token = table.transaction_write_token().await;
            txn.register(
                table.table_id().to_string(),
                token,
                table.clone_for_write_operations(),
            );
            let ids: Vec<i64> = (first..=last).collect();
            let values: Vec<i64> = ids.iter().map(|id| id * 10).collect();
            let batch = arrow::array::RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(Int64Array::from(values)),
                ],
            )
            .expect("batch");
            let staged = table
                .begin_staged_upsert_occ(token, crate::common::single_batch_stream(batch), 1)
                .await?;
            txn.set_staged(table.table_id(), staged);
        }
        txn.commit().await
    }

    fn is_decision(path: &Path) -> bool {
        path.as_ref().ends_with("/decision")
    }

    /// Two keyed datasets in the location, each with ids `1..=100`, over `store`.
    async fn two_datasets(
        store: &Arc<dyn ObjectStore>,
    ) -> (Arc<dyn MetadataCatalog>, Placed, Placed) {
        let catalog: Arc<dyn MetadataCatalog> = Arc::new(catalog(store).await);
        let accounts = place_with_key(store, &catalog, "accounts", true, None, true).await;
        let ledger = place_with_key(store, &catalog, "ledger", true, None, true).await;
        commit_across(
            &[Arc::clone(&accounts.table), Arc::clone(&ledger.table)],
            1,
            100,
        )
        .await
        .expect("the first transaction commits");
        (catalog, accounts, ledger)
    }

    /// Both datasets' row counts, read by a restart from the store alone.
    async fn counts_after_restart(
        store: &Arc<dyn ObjectStore>,
        accounts: &Placed,
        ledger: &Placed,
    ) -> (i64, i64) {
        let reopened: Arc<dyn MetadataCatalog> = Arc::new(catalog(store).await);
        let a = place(
            store,
            &reopened,
            "accounts",
            false,
            Some(accounts.data_url.clone()),
        )
        .await;
        let l = place(
            store,
            &reopened,
            "ledger",
            false,
            Some(ledger.data_url.clone()),
        )
        .await;
        (
            count(&a.ctx, "accounts").await.expect("count"),
            count(&l.ctx, "ledger").await.expect("count"),
        )
    }

    /// A cross-dataset transaction whose decision lands with its response lost:
    /// the coordinator reads the decision back and reports the commit, and both
    /// datasets have its rows, before and after a restart.
    #[tokio::test]
    async fn a_transaction_with_data_files_whose_decision_response_is_lost_commits_on_both() {
        let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
        let store: Arc<dyn ObjectStore> = Arc::clone(&faults) as Arc<dyn ObjectStore>;
        let (_catalog, accounts, ledger) = two_datasets(&store).await;
        faults.arm(Op::Create, is_decision, Fault::DropResponse);
        commit_across(
            &[Arc::clone(&accounts.table), Arc::clone(&ledger.table)],
            101,
            150,
        )
        .await
        .expect("the commit is read back and reported");
        assert_eq!(count(&accounts.ctx, "accounts").await.expect("count"), 150);
        assert_eq!(count(&ledger.ctx, "ledger").await.expect("count"), 150);
        assert_eq!(
            counts_after_restart(&store, &accounts, &ledger).await,
            (150, 150)
        );
    }

    /// The coordinator is cancelled while its decision is in flight. When the
    /// decision lands the restart commits the transaction on both datasets;
    /// when it never lands the restart aborts it on both.
    #[tokio::test]
    async fn a_cancelled_transaction_with_data_files_commits_on_both_or_neither() {
        for delivered in [true, false] {
            let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
            let store: Arc<dyn ObjectStore> = Arc::clone(&faults) as Arc<dyn ObjectStore>;
            let (catalog, accounts, ledger) = two_datasets(&store).await;
            faults.arm(Op::Create, is_decision, Fault::Hold);
            let tables = [Arc::clone(&accounts.table), Arc::clone(&ledger.table)];
            let coordinator =
                tokio::spawn(async move { commit_across(&tables, 101, 150).await.map(|_| ()) });
            let held = faults.wait_held(None).await;
            coordinator.abort();
            assert!(coordinator.await.is_err(), "the coordinator was cancelled");
            if delivered {
                faults.deliver(held).await.expect("the decision lands");
            }
            drop(catalog);
            let expected = if delivered { (150, 150) } else { (100, 100) };
            assert_eq!(
                counts_after_restart(&store, &accounts, &ledger).await,
                expected,
                "decision delivered: {delivered}"
            );
        }
    }

    async fn insert(
        ctx: &SessionContext,
        name: &str,
        first: i64,
        last: i64,
    ) -> datafusion::error::Result<()> {
        ctx.sql(&format!(
            "INSERT INTO {name} SELECT value AS id, value * 10 AS v FROM generate_series({first}, {last})"
        ))
        .await?
        .collect()
        .await
        .map(|_| ())
    }

    async fn count(ctx: &SessionContext, name: &str) -> datafusion::error::Result<i64> {
        let batches = ctx
            .sql(&format!("SELECT count(*) FROM {name}"))
            .await?
            .collect()
            .await?;
        Ok(batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count")
            .value(0))
    }

    fn dataset_head(catalog: &ObjectNativeCatalog, dataset: &str) -> Path {
        catalog
            .dataset_log(dataset)
            .expect("a dataset log")
            .join("head")
    }

    /// Polls `condition` every 20 ms for up to 15 s.
    async fn eventually<F, Fut>(what: &str, mut condition: F)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while std::time::Instant::now() < deadline {
            if condition().await {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        panic!("{what} within 15 s");
    }

    /// A commit whose outcome stays unknown after its retries, because its
    /// head CAS landed with the response lost and every read of the head fails:
    /// the write is not acknowledged, the dataset accepts nothing meanwhile, and
    /// once the head is readable again it reopens its log in the background,
    /// finds the commit landed, and resumes with all of the write's rows.
    #[tokio::test]
    async fn an_unknown_outcome_reopens_in_the_background_and_resumes() {
        let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
        let store: Arc<dyn ObjectStore> = Arc::clone(&faults) as Arc<dyn ObjectStore>;
        let native = catalog(&store).await;
        let catalog_handle = Arc::new(native);
        let shared: Arc<dyn MetadataCatalog> =
            Arc::clone(&catalog_handle) as Arc<dyn MetadataCatalog>;
        let placed = place(&store, &shared, "orders", true, None).await;
        insert(&placed.ctx, "orders", 1, 1000)
            .await
            .expect("first write");
        assert_eq!(count(&placed.ctx, "orders").await.expect("count"), 1000);

        let head = dataset_head(&catalog_handle, "orders");
        let cas_head = head.clone();
        faults.arm(
            Op::Cas,
            move |path: &Path| path == &cas_head,
            Fault::DropResponse,
        );
        let get_head = head.clone();
        faults.fail_while(Op::Get, move |path: &Path| path == &get_head);
        let failed = insert(&placed.ctx, "orders", 1001, 3000)
            .await
            .expect_err("a write whose commit outcome is unknown is not acknowledged");
        // The commit that lost its outcome, or a later step of the same write
        // that found the dataset reading its state back, reports the failure.
        let failed = failed.to_string();
        assert!(
            failed.contains(
                "The outcome of this change to dataset 'orders' (cayenne) is unknown: Spice could not confirm whether it was saved"
            ) || failed.contains(
                "Dataset 'orders' (cayenne) is reading its state back after a change whose outcome is unknown, so it accepts no change until that finishes; retry shortly."
            ),
            "unexpected error: {failed}"
        );
        let refused = insert(&placed.ctx, "orders", 5001, 5001)
            .await
            .expect_err("the dataset accepts no change while its outcome is unknown");
        assert!(
            refused.to_string().contains(
                "Dataset 'orders' (cayenne) is reading its state back after a change whose outcome is unknown"
            ),
            "unexpected error: {refused}"
        );

        faults.clear_rules();
        eventually("the dataset accepts writes again", || async {
            insert(&placed.ctx, "orders", 3001, 3100).await.is_ok()
        })
        .await;
        // The write was not acknowledged: after the resume its rows are all
        // there (its commit landed) or none are (it did not), never part, and
        // a staged write that never committed is not rolled forward.
        let resumed = keyed_totals(&placed.ctx, "orders").await;
        assert!(
            resumed == vec![3100, 3100, sum_v(1, 3100, 0)]
                || resumed == vec![1100, 1100, sum_v(1, 1000, 0) + sum_v(3001, 3100, 0)],
            "the write whose outcome was unknown is whole or absent after the resume: {resumed:?}"
        );

        // A restart from the store alone sees the same rows.
        let data_url = placed.data_url.clone();
        drop((placed, shared, catalog_handle));
        let reopened: Arc<dyn MetadataCatalog> = Arc::new(catalog(&store).await);
        let placed = place(&store, &reopened, "orders", false, Some(data_url)).await;
        assert_eq!(keyed_totals(&placed.ctx, "orders").await, resumed);
        eprintln!("RESULT unknown-outcome append after the resume: {resumed:?}");
    }

    /// A change the dataset's state refuses (a bug in the catalog's own checks)
    /// stops that dataset until restart with an error that names it and the
    /// fix; another dataset keeps working.
    #[tokio::test]
    async fn a_refused_change_stops_only_that_dataset() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let native = Arc::new(catalog(&store).await);
        let catalog: Arc<dyn MetadataCatalog> = Arc::clone(&native) as Arc<dyn MetadataCatalog>;
        let orders = place(&store, &catalog, "orders", true, None).await;
        let audit = place(&store, &catalog, "audit", true, None).await;
        insert(&orders.ctx, "orders", 1, 100).await.expect("orders");
        insert(&audit.ctx, "audit", 1, 100).await.expect("audit");

        let refused = native
            .commit_change_the_state_refuses("orders")
            .await
            .expect_err("the state refuses the change");
        let text = "Dataset 'orders' (cayenne) stopped accepting changes until Spice restarts, because its state refused a change, which is a bug; queries return an error until then. Restart Spice to reload the dataset from its saved state, and report the issue at https://github.com/spiceai/spiceai/issues. Cause: ";
        assert!(
            refused.to_string().contains(text),
            "unexpected error: {refused}"
        );
        let later = insert(&orders.ctx, "orders", 101, 200)
            .await
            .expect_err("the stopped dataset accepts no change");
        assert!(
            later.to_string().contains(
                "Dataset 'orders' (cayenne) stopped accepting changes until Spice restarts"
            ),
            "unexpected error: {later}"
        );
        insert(&audit.ctx, "audit", 101, 200)
            .await
            .expect("another dataset keeps accepting changes");
        assert_eq!(count(&audit.ctx, "audit").await.expect("count"), 200);
    }

    /// Another instance took the dataset's log over: this instance's next
    /// commit is fenced, its writes stop with the fenced text, and its reads
    /// keep returning the committed rows, without its refused change.
    #[tokio::test]
    async fn a_fenced_dataset_stops_writing_and_keeps_reading() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let first: Arc<dyn MetadataCatalog> = Arc::new(catalog(&store).await);
        let placed = place(&store, &first, "orders", true, None).await;
        insert(&placed.ctx, "orders", 1, 1000)
            .await
            .expect("first write");

        // A second instance opens the same location and takes every log over.
        let second: Arc<dyn MetadataCatalog> = Arc::new(catalog(&store).await);
        let successor = place(
            &store,
            &second,
            "orders",
            false,
            Some(placed.data_url.clone()),
        )
        .await;

        let fenced = insert(&placed.ctx, "orders", 1001, 2000)
            .await
            .expect_err("the fenced instance's write is not saved");
        assert!(
            fenced.to_string().contains(
                "Dataset 'orders' (cayenne) stopped writing, because another Spice instance now writes its state; this instance keeps answering queries from the state saved so far. Restart this instance to make it the writer again."
            ),
            "unexpected error: {fenced}"
        );
        assert_eq!(
            count(&placed.ctx, "orders")
                .await
                .expect("the fenced instance still reads"),
            1000,
            "the fenced instance reads the committed rows, not its refused write"
        );
        insert(&successor.ctx, "orders", 1001, 1500)
            .await
            .expect("the new writer writes");
        assert_eq!(count(&successor.ctx, "orders").await.expect("count"), 1500);
    }

    /// Arms the dataset's head so the next commit lands with its response lost
    /// and every read of the head fails, leaving the outcome unknown.
    fn lose_the_next_commit_response(faults: &FaultStore, head: &Path) {
        let cas_head = head.clone();
        faults.arm(
            Op::Cas,
            move |path: &Path| path == &cas_head,
            Fault::DropResponse,
        );
        let get_head = head.clone();
        faults.fail_while(Op::Get, move |path: &Path| path == &get_head);
    }

    /// Whether the catalog refuses to read `name` because the dataset is reading
    /// its state back after a commit whose outcome is unknown. Asked of the
    /// catalog, not through the table, whose write in flight holds its scans.
    async fn reading_back(catalog: &dyn MetadataCatalog, name: &str) -> bool {
        catalog
            .get_table(name)
            .await
            .is_err_and(|error| error.to_string().contains("is reading its state back"))
    }

    async fn scalars(ctx: &SessionContext, sql: &str) -> datafusion::error::Result<Vec<i64>> {
        let batches = ctx.sql(sql).await?.collect().await?;
        let batch = &batches[0];
        Ok((0..batch.num_columns())
            .map(|column| {
                batch
                    .column(column)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("an Int64 column")
                    .value(0)
            })
            .collect())
    }

    async fn upsert(
        ctx: &SessionContext,
        name: &str,
        first: i64,
        last: i64,
        offset: i64,
    ) -> datafusion::error::Result<()> {
        ctx.sql(&format!(
            "INSERT INTO {name} SELECT value AS id, value * 10 + {offset} AS v FROM generate_series({first}, {last})"
        ))
        .await?
        .collect()
        .await
        .map(|_| ())
    }

    /// `SELECT count(*), count(DISTINCT id), sum(v)` once the dataset reads.
    async fn keyed_totals(ctx: &SessionContext, name: &str) -> Vec<i64> {
        let sql = format!("SELECT count(*), count(DISTINCT id), sum(v) FROM {name}");
        let mut last = None;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while std::time::Instant::now() < deadline {
            match scalars(ctx, &sql).await {
                Ok(values) => return values,
                Err(error) => last = Some(error.to_string()),
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        panic!("the dataset reads within 15 s: {last:?}");
    }

    fn sum_v(first: i64, last: i64, offset: i64) -> i64 {
        (first..=last).map(|id| id * 10 + offset).sum()
    }

    /// The writer whose commit response is lost is told the outcome is unknown
    /// and to check before retrying, not that the change failed.
    #[tokio::test]
    async fn a_write_whose_commit_outcome_is_unknown_says_so() {
        let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
        let store: Arc<dyn ObjectStore> = Arc::clone(&faults) as Arc<dyn ObjectStore>;
        let native = Arc::new(catalog(&store).await);
        let shared: Arc<dyn MetadataCatalog> = Arc::clone(&native) as Arc<dyn MetadataCatalog>;
        let placed = place(&store, &shared, "orders", true, None).await;
        insert(&placed.ctx, "orders", 1, 100)
            .await
            .expect("first write");
        lose_the_next_commit_response(&faults, &dataset_head(&native, "orders"));
        let failed = insert(&placed.ctx, "orders", 101, 200)
            .await
            .expect_err("the write is not acknowledged")
            .to_string();
        assert!(
            failed.contains(
                "The outcome of this change to dataset 'orders' (cayenne) is unknown: Spice could not confirm whether it was saved, so check whether its rows are visible before retrying it. The dataset accepts no change until it has read its state back, which it retries in the background."
            ),
            "unexpected error: {failed}"
        );
        faults.clear_rules();
    }

    /// A refresh that appends after the largest `id` it holds: an append whose
    /// commit landed with its response lost is not appended again after the
    /// resume, because the cursor is read back from the committed rows.
    #[tokio::test]
    async fn a_refresh_append_whose_commit_response_is_lost_does_not_duplicate_rows_after_the_resume()
     {
        let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
        let store: Arc<dyn ObjectStore> = Arc::clone(&faults) as Arc<dyn ObjectStore>;
        let native = Arc::new(catalog(&store).await);
        let shared: Arc<dyn MetadataCatalog> = Arc::clone(&native) as Arc<dyn MetadataCatalog>;
        let placed = place(&store, &shared, "events", true, None).await;
        insert(&placed.ctx, "events", 1, 1000)
            .await
            .expect("first refresh");
        lose_the_next_commit_response(&faults, &dataset_head(&native, "events"));
        insert(&placed.ctx, "events", 1001, 2000)
            .await
            .expect_err("the second refresh is not acknowledged");
        faults.clear_rules();
        // The next refresh: its cursor is the committed state's largest id.
        let cursor = keyed_totals_max(&placed.ctx, "events").await;
        assert_eq!(cursor, 2000, "the append whose response was lost committed");
        eventually("the refresh appends after the cursor", || async {
            insert(&placed.ctx, "events", cursor + 1, 3000)
                .await
                .is_ok()
        })
        .await;
        assert_eq!(
            keyed_totals(&placed.ctx, "events").await,
            vec![3000, 3000, sum_v(1, 3000, 0)],
            "every source row once"
        );
    }

    /// An append writes each data file once, at its final key, and commits it
    /// by naming it, whatever happens to its commit (the `nth` commit of the
    /// write lost after landing, or failed before): it is whole or absent once
    /// the dataset reads its state back, the caller's retry of what is missing
    /// writes new keys, no key is written twice, nothing is staged or copied, and
    /// the orphan sweep then leaves no data file that no commit names. Returns
    /// whether the fault fired.
    async fn append_with_a_lost_commit_writes_each_key_once(nth: usize, landed: bool) -> bool {
        let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
        let store: Arc<dyn ObjectStore> = Arc::clone(&faults) as Arc<dyn ObjectStore>;
        let native = Arc::new(catalog(&store).await);
        let shared: Arc<dyn MetadataCatalog> = Arc::clone(&native) as Arc<dyn MetadataCatalog>;
        let placed = place(&store, &shared, "events", true, None).await;
        insert(&placed.ctx, "events", 1, 1000)
            .await
            .expect("first append");
        let armed = lose_the_nth_commit(&faults, &dataset_head(&native, "events"), nth, landed);
        let writer = {
            let ctx = placed.ctx.clone();
            tokio::spawn(async move { insert(&ctx, "events", 1001, 2000).await })
        };
        let fired = Arc::clone(&armed);
        eventually("the fault fires or the append returns", || async {
            fired.load(std::sync::atomic::Ordering::SeqCst) || writer.is_finished()
        })
        .await;
        if fired.load(std::sync::atomic::Ordering::SeqCst) {
            eventually("the dataset reads its state back", || async {
                reading_back(native.as_ref(), "events").await || writer.is_finished()
            })
            .await;
        }
        faults.clear_rules();
        let outcome = writer.await.expect("join the append");
        let fired = armed.load(std::sync::atomic::Ordering::SeqCst);

        // The caller retries what it cannot see committed.
        let committed = keyed_totals_max(&placed.ctx, "events").await;
        assert!(
            committed == 1000 || committed == 2000,
            "nth={nth} landed={landed}: the append is whole or absent: max id {committed}"
        );
        if outcome.is_ok() {
            assert_eq!(
                committed, 2000,
                "nth={nth} landed={landed}: an acknowledged append is visible"
            );
        }
        eventually("the dataset accepts the retry", || async {
            insert(&placed.ctx, "events", committed + 1, 3000)
                .await
                .is_ok()
        })
        .await;
        assert_eq!(
            keyed_totals(&placed.ctx, "events").await,
            vec![3000, 3000, sum_v(1, 3000, 0)],
            "nth={nth} landed={landed}: every row once"
        );

        let log = faults.log();
        let copies = log.iter().filter(|applied| applied.op == Op::Copy).count();
        assert_eq!(
            copies, 0,
            "nth={nth} landed={landed}: data files were copied"
        );
        let staged: Vec<String> = log
            .iter()
            .filter(|applied| applied.path.as_ref().contains("/_staging/"))
            .map(|applied| format!("{:?} {}", applied.op, applied.path))
            .collect();
        assert!(
            staged.is_empty(),
            "nth={nth} landed={landed}: requests into a staging directory: {staged:?}"
        );
        let mut written: std::collections::BTreeMap<String, usize> =
            std::collections::BTreeMap::new();
        for applied in log.iter().filter(|applied| {
            applied.result.is_ok()
                && matches!(applied.op, Op::Put | Op::Create | Op::MultipartComplete)
                && applied.path.as_ref().ends_with(".vortex")
        }) {
            *written.entry(applied.path.to_string()).or_default() += 1;
        }
        let twice: Vec<&String> = written
            .iter()
            .filter(|(_, writes)| **writes > 1)
            .map(|(key, _)| key)
            .collect();
        assert!(
            twice.is_empty(),
            "nth={nth} landed={landed}: data keys written more than once: {twice:?}"
        );
        assert!(
            written.len() >= 3,
            "nth={nth} landed={landed}: each append wrote its files: {written:?}"
        );

        // The orphan sweep takes what the lost append left if it never
        // committed, and nothing a commit names.
        placed.table.set_orphan_age_floor(std::time::Duration::ZERO);
        let mut swept = None;
        for _ in 0..100 {
            let report = placed
                .table
                .sweep_orphan_files_now()
                .await
                .expect("the orphan sweep");
            if report.skipped.is_none() {
                swept = Some(report);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let swept = swept.expect("the orphan sweep runs once the outcome is resolved");
        let table_id = placed.table.table_id().to_string();
        let data_prefix = placed
            .data_url
            .strip_prefix("s3://bucket/")
            .expect("a data url in the bucket")
            .to_string();
        let named: std::collections::BTreeSet<String> = shared
            .get_all_snapshot_files(&table_id)
            .await
            .expect("the manifest")
            .into_iter()
            .map(|file| {
                // A placed table's rows name their files relative to its flat
                // directory.
                format!("{data_prefix}/{table_id}/{}", file.file_path)
            })
            .collect();
        let stored: std::collections::BTreeSet<String> =
            futures::TryStreamExt::try_collect::<Vec<_>>(
                store.list(Some(&Path::from(data_prefix.as_str()))),
            )
            .await
            .expect("list the dataset")
            .into_iter()
            .map(|meta| meta.location.to_string())
            .filter(|key| key.ends_with(".vortex"))
            .collect();
        assert_eq!(
            stored, named,
            "nth={nth} landed={landed}: after the orphan sweep the stored data files are the named ones (it deleted {:?})",
            swept.deleted
        );
        assert_eq!(
            keyed_totals(&placed.ctx, "events").await,
            vec![3000, 3000, sum_v(1, 3000, 0)],
            "nth={nth} landed={landed}: the orphan sweep changed no row"
        );
        fired
    }

    #[tokio::test]
    async fn an_append_whose_commit_is_lost_and_retried_writes_each_key_once() {
        let mut fired = 0;
        for nth in 1..=3 {
            for landed in [true, false] {
                fired +=
                    usize::from(append_with_a_lost_commit_writes_each_key_once(nth, landed).await);
            }
        }
        assert!(fired > 0, "at least one commit of the append was lost");
    }

    async fn keyed_totals_max(ctx: &SessionContext, name: &str) -> i64 {
        let sql = format!("SELECT max(id) FROM {name}");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        loop {
            if let Ok(values) = scalars(ctx, &sql).await {
                return values[0];
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the dataset reads within 15 s"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    /// Arms the `nth` commit on `head` from now (1-based) to land with its
    /// response lost (`landed`) or to fail before it lands, with every read of
    /// the head failing meanwhile; returns whether it fired.
    fn lose_the_nth_commit(
        faults: &FaultStore,
        head: &Path,
        nth: usize,
        landed: bool,
    ) -> Arc<std::sync::atomic::AtomicBool> {
        let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let cas_head = head.clone();
        let get_head = head.clone();
        let fired_flag = Arc::clone(&fired);
        let fault = if landed {
            Fault::DropResponse
        } else {
            Fault::Fail
        };
        faults.arm(
            Op::Cas,
            move |path: &Path| {
                if path != &cas_head {
                    return false;
                }
                let n = seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                if n == nth {
                    fired_flag.store(true, std::sync::atomic::Ordering::SeqCst);
                    true
                } else {
                    false
                }
            },
            fault,
        );
        let gate = Arc::clone(&fired);
        faults.fail_while(Op::Get, move |path: &Path| {
            path == &get_head && gate.load(std::sync::atomic::Ordering::SeqCst)
        });
        fired
    }

    /// A keyed upsert one of whose commits has an unknown outcome (the `nth`
    /// commit of the write, landed or not): once the dataset reads its state back
    /// in the background it serves one row per key, with the upsert's values
    /// for every key or for none, keeps upserting and compacting, and serves
    /// exactly what a restart from the store serves. Returns whether the fault
    /// fired.
    async fn keyed_upsert_after_a_background_reopen(nth: usize, landed: bool) -> bool {
        let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
        let store: Arc<dyn ObjectStore> = Arc::clone(&faults) as Arc<dyn ObjectStore>;
        let native = Arc::new(catalog(&store).await);
        let shared: Arc<dyn MetadataCatalog> = Arc::clone(&native) as Arc<dyn MetadataCatalog>;
        let placed = place_with_key(&store, &shared, "accounts", true, None, true).await;
        upsert(&placed.ctx, "accounts", 1, 100, 0)
            .await
            .expect("first upsert");
        let armed = lose_the_nth_commit(&faults, &dataset_head(&native, "accounts"), nth, landed);
        // The upsert may wait for the dataset to read its state back, which it
        // can do once the store answers again: restore the store as soon as the
        // fault has fired, or the upsert has returned without it.
        let writer = {
            let ctx = placed.ctx.clone();
            tokio::spawn(async move { upsert(&ctx, "accounts", 1, 100, 1).await })
        };
        let fired = Arc::clone(&armed);
        eventually("the fault fires or the upsert returns", || async {
            fired.load(std::sync::atomic::Ordering::SeqCst) || writer.is_finished()
        })
        .await;
        // Keep the store failing until the dataset has given up on the commit
        // and reads its state back in the background.
        if fired.load(std::sync::atomic::Ordering::SeqCst) {
            eventually("the dataset reads its state back", || async {
                reading_back(native.as_ref(), "accounts").await || writer.is_finished()
            })
            .await;
        }
        faults.clear_rules();
        let outcome = writer.await.expect("join the upsert");
        let fired = armed.load(std::sync::atomic::Ordering::SeqCst);
        eventually("the dataset accepts upserts again", || async {
            upsert(&placed.ctx, "accounts", 101, 101, 0).await.is_ok()
        })
        .await;
        let totals = keyed_totals(&placed.ctx, "accounts").await;
        let (old, new) = (sum_v(1, 100, 0) + 1010, sum_v(1, 100, 1) + 1010);
        assert!(
            totals == vec![101, 101, old] || totals == vec![101, 101, new],
            "nth={nth} landed={landed}: one row per key, all of the upsert or none of it: {totals:?}"
        );
        if outcome.is_ok() {
            assert_eq!(
                totals,
                vec![101, 101, new],
                "nth={nth} landed={landed}: an acknowledged upsert is visible"
            );
        }

        upsert(&placed.ctx, "accounts", 1, 50, 2)
            .await
            .expect("an upsert after the resume");
        let after = keyed_totals(&placed.ctx, "accounts").await;
        let _ = placed
            .table
            .compact_current_snapshot_small_files()
            .await
            .expect("compaction");
        placed
            .table
            .drain_in_flight_maintenance()
            .await
            .expect("drain");
        assert_eq!(
            keyed_totals(&placed.ctx, "accounts").await,
            after,
            "nth={nth} landed={landed}: after compaction"
        );
        let data_url = placed.data_url.clone();
        drop((placed, shared, native));
        let reopened: Arc<dyn MetadataCatalog> = Arc::new(catalog(&store).await);
        let placed = place(&store, &reopened, "accounts", false, Some(data_url)).await;
        assert_eq!(
            keyed_totals(&placed.ctx, "accounts").await,
            after,
            "nth={nth} landed={landed}: a restart from the store serves what the resumed instance served"
        );
        fired
    }

    #[tokio::test]
    async fn a_keyed_upsert_whose_commit_landed_unconfirmed_serves_what_the_store_holds_after_a_background_reopen()
     {
        let mut fired = 0;
        for nth in 1..=4 {
            fired += usize::from(keyed_upsert_after_a_background_reopen(nth, true).await);
        }
        assert!(
            fired >= 2,
            "the upsert's commits lost their responses ({fired} fired)"
        );
    }

    #[tokio::test]
    async fn a_keyed_upsert_whose_commit_did_not_land_serves_what_the_store_holds_after_a_background_reopen()
     {
        let mut fired = 0;
        for nth in 1..=4 {
            fired += usize::from(keyed_upsert_after_a_background_reopen(nth, false).await);
        }
        assert!(
            fired >= 2,
            "the upsert's commits failed unconfirmed ({fired} fired)"
        );
    }

    /// A whole-table rewrite one of whose commits has an unknown outcome (the
    /// `nth` of the pass, landed or not): after the background reopen, appends land in
    /// the snapshot the state points at, every row is there once, and a restart
    /// from the store serves the same rows.
    #[tokio::test]
    async fn appends_after_a_compaction_with_an_unknown_outcome_keep_every_row() {
        let mut fired = 0;
        for landed in [true, false] {
            for nth in 1..=3 {
                let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
                let store: Arc<dyn ObjectStore> = Arc::clone(&faults) as Arc<dyn ObjectStore>;
                let native = Arc::new(catalog(&store).await);
                let shared: Arc<dyn MetadataCatalog> =
                    Arc::clone(&native) as Arc<dyn MetadataCatalog>;
                let placed =
                    place_with_config(&store, &shared, "facts", true, None, false, compactable())
                        .await;
                for i in 0..5 {
                    insert(&placed.ctx, "facts", i * 1000 + 1, (i + 1) * 1000)
                        .await
                        .expect("append");
                }
                placed
                    .table
                    .drain_in_flight_maintenance()
                    .await
                    .expect("drain");
                let armed =
                    lose_the_nth_commit(&faults, &dataset_head(&native, "facts"), nth, landed);
                // A whole-table rewrite: its commits are the ones that lose.
                let _ = placed.table.sort_and_rewrite_data(64 * 1024 * 1024).await;
                fired += usize::from(armed.load(std::sync::atomic::Ordering::SeqCst));
                faults.clear_rules();
                eventually("the dataset accepts appends again", || async {
                    insert(&placed.ctx, "facts", 5001, 6000).await.is_ok()
                })
                .await;
                insert(&placed.ctx, "facts", 6001, 7000)
                    .await
                    .expect("another append");
                placed
                    .table
                    .drain_in_flight_maintenance()
                    .await
                    .expect("drain");
                let expected = vec![7000, 7000, sum_v(1, 7000, 0)];
                assert_eq!(
                    keyed_totals(&placed.ctx, "facts").await,
                    expected,
                    "nth={nth} landed={landed}: after the resume"
                );
                let data_url = placed.data_url.clone();
                drop((placed, shared, native));
                let reopened: Arc<dyn MetadataCatalog> = Arc::new(catalog(&store).await);
                let placed = place(&store, &reopened, "facts", false, Some(data_url)).await;
                assert_eq!(
                    keyed_totals(&placed.ctx, "facts").await,
                    expected,
                    "nth={nth} landed={landed}: after a restart"
                );
            }
        }
        assert!(
            fired >= 2,
            "the compaction's commits lost their outcome ({fired} fired)"
        );
    }

    /// A transaction across datasets whose `Resolve` on one dataset fails with
    /// its outcome unknown: the dataset reopens its log in the background and
    /// resolves the transaction there from its decision, as an open does, so it
    /// shows the transaction's rows and accepts upserts again.
    #[tokio::test]
    async fn a_transaction_left_prepared_in_a_log_reopened_in_the_background_is_resolved() {
        let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
        let store: Arc<dyn ObjectStore> = Arc::clone(&faults) as Arc<dyn ObjectStore>;
        let (catalog, accounts, ledger) = two_datasets(&store).await;
        let native = catalog
            .as_any()
            .downcast_ref::<ObjectNativeCatalog>()
            .expect("an object-native catalog");
        let accounts_head = dataset_head(native, "accounts");
        faults.arm(Op::Create, is_decision, Fault::Hold);
        let tables = [Arc::clone(&accounts.table), Arc::clone(&ledger.table)];
        let coordinator =
            tokio::spawn(async move { commit_across(&tables, 101, 150).await.map(|_| ()) });
        let held = faults.wait_held(None).await;
        // Both are prepared; the decision is about to land. The `Resolve` on
        // `accounts` then fails before it lands, and its outcome stays unknown.
        let fired = lose_the_nth_commit(&faults, &accounts_head, 1, false);
        faults.deliver(held).await.expect("the decision lands");
        faults.respond(held).expect("the coordinator hears it");
        eventually("the `Resolve` on accounts fails unconfirmed", || async {
            fired.load(std::sync::atomic::Ordering::SeqCst)
        })
        .await;
        eventually("accounts reads its state back", || async {
            reading_back(native, "accounts").await
        })
        .await;
        faults.clear_rules();
        let _ = coordinator.await.expect("the coordinator finishes");
        eventually("accounts accepts upserts again", || async {
            upsert(&accounts.ctx, "accounts", 1000, 1000, 0)
                .await
                .is_ok()
        })
        .await;
        assert_eq!(count(&accounts.ctx, "accounts").await.expect("count"), 151);
        assert_eq!(count(&ledger.ctx, "ledger").await.expect("count"), 150);
    }
}

/// Publish snapshot `snapshot` of `table_id` as a protected snapshot at `sequence`.
fn protected_snapshot<'a>(
    table_id: &'a str,
    snapshot: &'a str,
    sequence: i64,
) -> cayenne::catalog::CatalogPublication<'a> {
    cayenne::catalog::CatalogPublication {
        table_id,
        snapshot_id: snapshot,
        snapshot_sequence: sequence,
        insert_sequence: None,
        delete_files: &[],
        insert_pk_bytes: &[],
        inline_tombstone: None,
        pending_durable_flips: &[],
        dirty_pk_bytes: std::borrow::Cow::Borrowed(&[]),
        files: &[],
    }
}


/// One data file of `snapshot`, so retiring the snapshot retires a file.
fn one_file(table_id: &str, snapshot: &str) -> [cayenne::metadata::SnapshotFile; 1] {
    [cayenne::metadata::SnapshotFile {
        table_id: table_id.to_string(),
        snapshot_id: snapshot.to_string(),
        file_path: "part-0.vortex".to_string(),
        row_count: 1,
        file_size_bytes: 1,
        min_sequence: 0,
        max_sequence: 0,
        digest: None,
    }]
}

/// A retirement licenses deleting files only once it is committed. While the
/// change that takes a protected snapshot's file out of the live set is
/// buffered, the retired set is not answered; once committed it is, from
/// storage alone after a reopen too; forgetting it is one commit; and a dataset
/// log a successor fenced answers nothing once a commit to it was refused.
#[tokio::test]
async fn retired_files_are_answered_from_committed_state_only() {
    use cayenne::MetadataCatalog;
    let (_faults, store, catalog) = setup().await;
    let orders = create(&catalog, "orders").await;
    let first = uuid::Uuid::now_v7().to_string();
    let merged = uuid::Uuid::now_v7().to_string();
    let first_files = one_file(&orders, &first);
    let first_key = format!("{first}/part-0.vortex");
    catalog
        .commit_publications(&[cayenne::catalog::CatalogPublication {
            files: &first_files,
            ..protected_snapshot(&orders, &first, 5)
        }])
        .await
        .expect("publish a protected snapshot");
    assert_eq!(
        catalog.retired_files(&orders).await.expect("read"),
        Some(vec![])
    );

    cayenne::catalog::with_deferred_commits(async {
        assert!(
            catalog
                .swap_protected_snapshots(&orders, std::slice::from_ref(&first), &merged, 6)
                .await
                .expect("merge"),
            "the merge replaces the protected snapshot"
        );
        assert_eq!(
            catalog.retired_files(&orders).await.expect("read"),
            None,
            "a buffered retirement licenses no delete"
        );
    })
    .await;
    assert_eq!(
        catalog.retired_files(&orders).await.expect("read"),
        None,
        "a retirement still buffered after its scope licenses no delete"
    );
    catalog
        .commit_pending_for(&orders)
        .await
        .expect("the acknowledgement point commits the merge");
    assert_eq!(
        catalog.retired_files(&orders).await.expect("read"),
        Some(vec![first_key.clone()])
    );

    // A successor reads the record from storage alone, and fences this catalog.
    let successor = ObjectNativeCatalog::open(Arc::clone(&store), Path::from("catalog"))
        .await
        .expect("take over");
    assert_eq!(
        successor.retired_files(&orders).await.expect("read"),
        Some(vec![first_key.clone()]),
        "the retirement record survives a restart"
    );
    catalog
        .commit_publications(&[protected_snapshot(
            &orders,
            &uuid::Uuid::now_v7().to_string(),
            7,
        )])
        .await
        .expect_err("a fenced commit to the dataset's log is refused");
    assert_eq!(
        catalog.retired_files(&orders).await.expect("read"),
        None,
        "a fenced catalog licenses no delete"
    );

    assert!(
        successor
            .forget_retired_files(&orders, std::slice::from_ref(&first_key))
            .await
            .expect("forget")
    );
    assert_eq!(
        successor.retired_files(&orders).await.expect("read"),
        Some(vec![])
    );
    let reopened = ObjectNativeCatalog::open(Arc::clone(&store), Path::from("catalog"))
        .await
        .expect("reopen");
    assert_eq!(
        reopened.retired_files(&orders).await.expect("read"),
        Some(vec![]),
        "the forget is committed"
    );
}
