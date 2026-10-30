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
    assert!(
        refused.to_string().contains("requires reopening"),
        "unexpected error: {refused}"
    );

    assert_eq!(head_commit(&store).await, before);
    assert_eq!(
        successor.sidecar_value("position").await.expect("read"),
        Some(b"1".to_vec())
    );
}
