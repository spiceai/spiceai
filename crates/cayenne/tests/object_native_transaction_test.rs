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

//! A transaction across two datasets of the object-native catalog commits by
//! two-phase commit over their logs: both or neither, including when the
//! process stops before or after the decision. What no log needs afterwards is
//! removed, and nothing else: a finished transaction's records by whoever
//! finishes it, and the log of a dropped dataset or of a create that stopped
//! before the dataset was named once older than `reclaim_after`.

use std::sync::Arc;

use cayenne::{
    MetadataCatalog,
    catalog::CatalogPublication,
    metastore::object_native::{NativeCatalogConfig, ObjectNativeCatalog, dataset_log_at},
};
use object_store::{ObjectStore, memory::InMemory, path::Path};
use object_store_occ::{
    headlog::Reader,
    sim::{
        Op,
        fault::{Fault, FaultStore},
    },
};

async fn setup() -> (
    Arc<FaultStore>,
    Arc<dyn ObjectStore>,
    Arc<ObjectNativeCatalog>,
    String,
    String,
) {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let store: Arc<dyn ObjectStore> = Arc::clone(&faults) as Arc<dyn ObjectStore>;
    let catalog = ObjectNativeCatalog::create(Arc::clone(&store), Path::from("catalog"))
        .await
        .expect("create the catalog");
    let orders = create(&catalog, "orders").await;
    let audit = create(&catalog, "audit").await;
    (faults, store, Arc::new(catalog), orders, audit)
}

async fn create(catalog: &ObjectNativeCatalog, name: &str) -> String {
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

/// Publish snapshot `snapshot` at `sequence` on `table_id`.
fn publication(table_id: &str, snapshot: &str, sequence: i64) -> CatalogPublication {
    CatalogPublication {
        table_id: table_id.to_string(),
        snapshot_id: snapshot.to_string(),
        snapshot_sequence: sequence,
        insert_sequence: None,
        delete_files: Vec::new(),
        insert_pk_bytes: Vec::new(),
        inline_tombstone: None,
        pending_durable_flips: Vec::new(),
        dirty_pk_bytes: Vec::new(),
    }
}

fn across(orders: &str, audit: &str) -> Vec<CatalogPublication> {
    vec![
        publication(orders, "orders-txn", 5),
        publication(audit, "audit-txn", 6),
    ]
}

/// Each table's sequence for its transaction snapshot.
async fn published(catalog: &ObjectNativeCatalog, orders: &str, audit: &str) -> [Option<i64>; 2] {
    [
        catalog
            .get_snapshot_sequence(orders, "orders-txn")
            .await
            .expect("read orders"),
        catalog
            .get_snapshot_sequence(audit, "audit-txn")
            .await
            .expect("read audit"),
    ]
}

fn is_decision(path: &Path) -> bool {
    path.as_ref().ends_with("/decision")
}

/// Every object under `prefix`.
async fn objects_under(store: &Arc<dyn ObjectStore>, prefix: &str) -> Vec<String> {
    use futures::TryStreamExt;
    let listed: Vec<_> = store
        .list(Some(&Path::from(prefix)))
        .try_collect()
        .await
        .expect("list objects");
    listed
        .into_iter()
        .map(|meta| meta.location.to_string())
        .collect()
}

/// Open the catalog, removing anything no log needs however recent it is.
async fn open_reclaiming(store: &Arc<dyn ObjectStore>) -> ObjectNativeCatalog {
    ObjectNativeCatalog::open_with_config(
        Arc::clone(store),
        Path::from("catalog"),
        NativeCatalogConfig {
            reclaim_after: std::time::Duration::ZERO,
            ..NativeCatalogConfig::default()
        },
    )
    .await
    .expect("open the catalog")
}

/// The action names committed to `dataset`'s log, oldest first.
async fn actions(store: &Arc<dyn ObjectStore>, dataset: &str) -> Vec<&'static str> {
    use prost::Message;
    let log = dataset_log_at(Arc::clone(store), &Path::from("catalog"), dataset)
        .await
        .expect("read the registry")
        .expect("the dataset has a log");
    let (_, opened) = Reader::open(Arc::clone(store), log)
        .await
        .expect("open the log");
    let mut names = Vec::new();
    for entry in opened.records {
        for payload in object_store_occ::headlog::Group::decode(entry.record.payload)
            .expect("group")
            .payloads
        {
            names.extend(
                cayenne_metastore_actions::Record::decode(payload)
                    .expect("record")
                    .actions
                    .iter()
                    .map(cayenne_metastore_actions::Action::name),
            );
        }
    }
    names
}

#[tokio::test]
async fn a_transaction_across_two_datasets_commits_on_both_logs() {
    let (_faults, store, catalog, orders, audit) = setup().await;
    catalog
        .commit_publications(&across(&orders, &audit))
        .await
        .expect("commit across both datasets");
    assert_eq!(published(&catalog, &orders, &audit).await, [Some(5), Some(6)]);
    for dataset in ["orders", "audit"] {
        let names = actions(&store, dataset).await;
        assert_eq!(
            names[names.len() - 2..],
            ["Prepare", "Resolve"],
            "{dataset}'s log: {names:?}"
        );
    }
    // Both participants resolved it, so the coordinator removed its records.
    assert_eq!(objects_under(&store, "transactions").await, Vec::<String>::new());
    drop(catalog);
    let reopened = ObjectNativeCatalog::open(Arc::clone(&store), Path::from("catalog"))
        .await
        .expect("reopen");
    assert_eq!(published(&reopened, &orders, &audit).await, [Some(5), Some(6)]);
}

/// The coordinator stops before recording its decision: the process that opens
/// the logs next has fenced it on both, so it aborts the transaction without
/// waiting for the deadline and removes its records. Neither dataset shows it,
/// and both accept changes again.
#[tokio::test]
async fn a_transaction_stopped_before_its_decision_aborts_when_the_catalog_reopens() {
    let (faults, store, catalog, orders, audit) = setup().await;
    faults.arm(Op::Create, is_decision, Fault::Hold);
    let coordinator = tokio::spawn({
        let catalog = Arc::clone(&catalog);
        let (orders, audit) = (orders.clone(), audit.clone());
        async move { catalog.commit_publications(&across(&orders, &audit)).await }
    });
    faults.wait_held(None).await;
    coordinator.abort();
    assert!(coordinator.await.is_err_and(|error| error.is_cancelled()));
    drop(catalog);
    faults.clear_rules();
    let reopened = ObjectNativeCatalog::open(Arc::clone(&store), Path::from("catalog"))
        .await
        .expect("reopen resolves the prepared transaction");
    assert_eq!(published(&reopened, &orders, &audit).await, [None, None]);
    assert_eq!(objects_under(&store, "transactions").await, Vec::<String>::new());
    reopened
        .commit_publications(&[publication(&orders, "orders-later", 7)])
        .await
        .expect("orders accepts changes after the abort");
    reopened
        .commit_publications(&[publication(&audit, "audit-later", 8)])
        .await
        .expect("audit accepts changes after the abort");
}

/// The decision to commit lands, then the coordinator stops before resolving:
/// the next open finishes the commit on both datasets and removes the
/// transaction's records.
#[tokio::test]
async fn a_transaction_stopped_after_its_commit_decision_commits_when_the_catalog_reopens() {
    let (faults, store, catalog, orders, audit) = setup().await;
    faults.arm(Op::Create, is_decision, Fault::Hold);
    let coordinator = tokio::spawn({
        let catalog = Arc::clone(&catalog);
        let (orders, audit) = (orders.clone(), audit.clone());
        async move { catalog.commit_publications(&across(&orders, &audit)).await }
    });
    let held = faults.wait_held(None).await;
    faults.deliver(held).await.expect("the decision lands");
    coordinator.abort();
    assert!(coordinator.await.is_err_and(|error| error.is_cancelled()));
    drop(catalog);
    faults.clear_rules();
    let reopened = ObjectNativeCatalog::open(Arc::clone(&store), Path::from("catalog"))
        .await
        .expect("reopen resolves the prepared transaction");
    assert_eq!(published(&reopened, &orders, &audit).await, [Some(5), Some(6)]);
    assert_eq!(objects_under(&store, "transactions").await, Vec::<String>::new());
}

/// Between a transaction's `Prepare` and `Resolve`, another change to one of its
/// datasets waits rather than being refused by the reservation, and commits
/// after the transaction.
#[tokio::test]
async fn a_change_to_a_reserved_dataset_waits_for_the_transaction() {
    let (faults, _store, catalog, orders, audit) = setup().await;
    faults.arm(Op::Create, is_decision, Fault::Hold);
    let coordinator = tokio::spawn({
        let catalog = Arc::clone(&catalog);
        let (orders, audit) = (orders.clone(), audit.clone());
        async move { catalog.commit_publications(&across(&orders, &audit)).await }
    });
    let held = faults.wait_held(None).await;
    let change = tokio::spawn({
        let catalog = Arc::clone(&catalog);
        let orders = orders.clone();
        async move {
            catalog
                .commit_publications(&[publication(&orders, "orders-later", 7)])
                .await
        }
    });
    // A bounded wait for something that must not happen: the change is blocked
    // for as long as the decision is held.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(!change.is_finished(), "the change did not wait for the transaction");
    faults.clear_rules();
    faults.deliver(held).await.expect("the decision lands");
    faults.respond(held).expect("release the decision's response");
    coordinator
        .await
        .expect("coordinator task")
        .expect("the transaction commits");
    change
        .await
        .expect("change task")
        .expect("the change commits after the transaction");
    assert_eq!(published(&catalog, &orders, &audit).await, [Some(5), Some(6)]);
    assert_eq!(
        catalog
            .get_snapshot_sequence(&orders, "orders-later")
            .await
            .expect("read orders"),
        Some(7)
    );
}

/// A dropped dataset's log, and the log of a create that stopped before the
/// registry named the dataset, are removed once older than `reclaim_after`; the
/// logs of datasets that exist are kept with their state.
#[tokio::test]
async fn an_open_removes_the_logs_no_dataset_uses_and_keeps_the_others() {
    let (faults, store, catalog, orders, audit) = setup().await;
    catalog
        .commit_publications(&across(&orders, &audit))
        .await
        .expect("commit across both datasets");
    let dropped_log = catalog.dataset_log("audit").expect("audit has a log");
    assert!(catalog.drop_table("audit").await.expect("drop audit"));
    // A create whose registry commit fails leaves a log nothing names.
    faults.fail_while(Op::Cas, |path| path.as_ref() == "catalog/head");
    create_fails(&catalog, "unnamed").await;
    faults.clear_rules();
    let kept = objects_under(&store, "datasets").await;
    assert!(
        kept.iter().any(|path| path.starts_with("datasets/unnamed-")),
        "the stopped create wrote its log: {kept:?}"
    );
    drop(catalog);
    // Recent: kept.
    drop(
        ObjectNativeCatalog::open(Arc::clone(&store), Path::from("catalog"))
            .await
            .expect("reopen"),
    );
    assert_eq!(objects_under(&store, "datasets").await, kept);
    let orders_log = format!(
        "{}/",
        dataset_log_at(Arc::clone(&store), &Path::from("catalog"), "orders")
            .await
            .expect("read the registry")
            .expect("orders has a log")
    );
    let orders_objects: Vec<_> = kept
        .iter()
        .filter(|path| path.starts_with(&orders_log))
        .cloned()
        .collect();
    assert!(!orders_objects.is_empty(), "orders' log has objects: {kept:?}");
    drop(open_reclaiming(&store).await);
    // Exactly orders' log is left, every object of it, and the dropped one is gone.
    assert_eq!(objects_under(&store, "datasets").await, orders_objects);
    assert!(!orders_objects.iter().any(|path| path.starts_with(&format!("{dropped_log}/"))));
    // Read back from storage alone.
    let reopened = ObjectNativeCatalog::open(Arc::clone(&store), Path::from("catalog"))
        .await
        .expect("reopen");
    assert_eq!(
        reopened
            .get_snapshot_sequence(&orders, "orders-txn")
            .await
            .expect("read orders"),
        Some(5)
    );
    // A dataset created again under the dropped name gets a new log.
    let audit_again = create(&reopened, "audit").await;
    reopened
        .commit_publications(&[publication(&audit_again, "audit-again", 1)])
        .await
        .expect("the new audit commits");
}

async fn create_fails(catalog: &ObjectNativeCatalog, name: &str) {
    use cayenne::metadata::{CreateTableOptions, VortexConfig};
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
    ]));
    let created = catalog
        .create_table(CreateTableOptions {
            table_name: name.to_string(),
            schema,
            primary_key: vec!["id".to_string()],
            on_conflict: None,
            base_path: format!("/tmp/unused/{name}"),
            partition_column: None,
            vortex_config: VortexConfig::default(),
        })
        .await;
    assert!(created.is_err(), "the create succeeded although the registry could not commit");
}
