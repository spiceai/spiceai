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
use object_store_occ::{
    headlog::Reader,
    sim::{
        Op,
        fault::{Fault, FaultStore},
    },
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
        catalog.sidecar_value("position").await.expect("a fenced catalog still reads"),
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
    use cayenne::metadata::{CreateTableOptions, ObjectStoreConfig, VortexConfig};
    use cayenne::metastore::object_native::{NativeCatalogConfig, new_dataset_folder};
    use cayenne::{CayenneTableProviderBuilder, MetadataCatalog};
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
        let data_url = data_url.unwrap_or_else(|| {
            format!(
                "s3://bucket/state/cayenne/v1/datasets/{}/data",
                new_dataset_folder(name)
            )
        });
        let ctx = SessionContext::new();
        let builder = CayenneTableProviderBuilder::new(Arc::clone(catalog), ctx.runtime_env())
            .with_object_store(ObjectStoreConfig {
                url: url::Url::parse(&data_url).expect("data url"),
                store: Arc::clone(store),
            });
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
                    vortex_config: VortexConfig {
                        inline_max_rows: 0,
                        inline_max_bytes: 0,
                        ..VortexConfig::default()
                    },
                })
                .await
                .expect("create the table")
        } else {
            builder.open(name).await.expect("open the table")
        };
        let table = Arc::new(table);
        ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)
            .expect("register");
        Placed { data_url, ctx, table }
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
            txn.register(table.table_id().to_string(), token, table.clone_for_write_operations());
            let ids: Vec<i64> = (first..=last).collect();
            let values: Vec<i64> = ids.iter().map(|id| id * 10).collect();
            let batch = arrow::array::RecordBatch::try_new(
                schema(),
                vec![Arc::new(Int64Array::from(ids)), Arc::new(Int64Array::from(values))],
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
        let a = place(store, &reopened, "accounts", false, Some(accounts.data_url.clone())).await;
        let l = place(store, &reopened, "ledger", false, Some(ledger.data_url.clone())).await;
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
        commit_across(&[Arc::clone(&accounts.table), Arc::clone(&ledger.table)], 101, 150)
            .await
            .expect("the commit is read back and reported");
        assert_eq!(count(&accounts.ctx, "accounts").await.expect("count"), 150);
        assert_eq!(count(&ledger.ctx, "ledger").await.expect("count"), 150);
        assert_eq!(counts_after_restart(&store, &accounts, &ledger).await, (150, 150));
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
            let coordinator = tokio::spawn(async move { commit_across(&tables, 101, 150).await.map(|_| ()) });
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

    async fn insert(ctx: &SessionContext, name: &str, first: i64, last: i64) -> datafusion::error::Result<()> {
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
        catalog.dataset_log(dataset).expect("a dataset log").join("head")
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
        let shared: Arc<dyn MetadataCatalog> = Arc::clone(&catalog_handle) as Arc<dyn MetadataCatalog>;
        let placed = place(&store, &shared, "orders", true, None).await;
        insert(&placed.ctx, "orders", 1, 1000).await.expect("first write");
        assert_eq!(count(&placed.ctx, "orders").await.expect("count"), 1000);

        let head = dataset_head(&catalog_handle, "orders");
        let cas_head = head.clone();
        faults.arm(Op::Cas, move |path: &Path| path == &cas_head, Fault::DropResponse);
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
                "Dataset 'orders' (cayenne) could not confirm whether its last change was saved, so it accepts no change, and acknowledges none to its source, until it has read its state back"
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
        assert_eq!(
            count(&placed.ctx, "orders").await.expect("count"),
            3100,
            "the commit that landed with its response lost is there, whole, after the resume"
        );

        // A restart from the store alone sees the same rows.
        let data_url = placed.data_url.clone();
        drop((placed, shared, catalog_handle));
        let reopened: Arc<dyn MetadataCatalog> = Arc::new(catalog(&store).await);
        let placed = place(&store, &reopened, "orders", false, Some(data_url)).await;
        assert_eq!(count(&placed.ctx, "orders").await.expect("count"), 3100);
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
        assert!(refused.to_string().contains(text), "unexpected error: {refused}");
        let later = insert(&orders.ctx, "orders", 101, 200)
            .await
            .expect_err("the stopped dataset accepts no change");
        assert!(
            later.to_string().contains("Dataset 'orders' (cayenne) stopped accepting changes until Spice restarts"),
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
        insert(&placed.ctx, "orders", 1, 1000).await.expect("first write");

        // A second instance opens the same location and takes every log over.
        let second: Arc<dyn MetadataCatalog> = Arc::new(catalog(&store).await);
        let successor = place(&store, &second, "orders", false, Some(placed.data_url.clone())).await;

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
            count(&placed.ctx, "orders").await.expect("the fenced instance still reads"),
            1000,
            "the fenced instance reads the committed rows, not its refused write"
        );
        insert(&successor.ctx, "orders", 1001, 1500)
            .await
            .expect("the new writer writes");
        assert_eq!(count(&successor.ctx, "orders").await.expect("count"), 1500);
    }
}
