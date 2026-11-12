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

//! Instances on one object-native location under a writer lease: the holder
//! writes, the others follow as replicas, and writing passes on when the
//! holder restarts, stops, or shuts down.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::Int64Array;
use cayenne::metadata::{CreateTableOptions, ObjectStoreConfig, VortexConfig};
use cayenne::metastore::object_native::{
    Access, LeaseSettings, NativeCatalogConfig, ObjectNativeCatalog,
};
use cayenne::{CayenneTableProviderBuilder, MetadataCatalog};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store_sim::Op;
use object_store_sim::fault::FaultStore;

const LOCATION: &str = "s3://bucket/state/cayenne/v1";
const TABLE: &str = "orders";
/// Short, so a lapse takes well under a second.
const LEASE: Duration = Duration::from_millis(600);

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

/// `(count, sum(v))` of the rows `ids` with `v = id * 10`.
fn expected(ids: impl Iterator<Item = i64>) -> (i64, i64) {
    ids.fold((0, 0), |(count, sum), id| (count + 1, sum + id * 10))
}

/// One process of instance `identity` over the shared objects. Its requests go
/// through its own `FaultStore`, whose log shows what it sent.
struct Instance {
    requests: Arc<FaultStore>,
    store: Arc<dyn ObjectStore>,
    catalog: Arc<dyn MetadataCatalog>,
    ctx: SessionContext,
    data_url: String,
}

impl Instance {
    async fn start(
        objects: &Arc<dyn ObjectStore>,
        identity: &'static str,
        lease: Duration,
    ) -> Self {
        let requests = Arc::new(FaultStore::new(Arc::clone(objects)));
        let store: Arc<dyn ObjectStore> = Arc::clone(&requests) as Arc<dyn ObjectStore>;
        let catalog = ObjectNativeCatalog::open_or_create_at_with_config(
            Arc::clone(&store),
            Path::from(crate::common::LOCATION_CATALOG),
            LOCATION,
            NativeCatalogConfig {
                access: Access::Leased(LeaseSettings {
                    duration: lease,
                    identity: Some(identity),
                }),
                ..NativeCatalogConfig::default()
            },
        )
        .await
        .expect("open the object-native catalog");
        catalog.init().await.expect("init");
        let data_url = crate::common::location_data_url(&catalog, TABLE);
        Self {
            requests,
            store,
            catalog: Arc::new(catalog),
            ctx: SessionContext::new(),
            data_url,
        }
    }

    fn builder(&self, data_url: &str) -> CayenneTableProviderBuilder {
        crate::common::table_builder_on_store(
            Arc::clone(&self.catalog),
            self.ctx.runtime_env(),
            ObjectStoreConfig {
                url: url::Url::parse(data_url).expect("data url"),
                store: Arc::clone(&self.store),
            },
        )
    }

    async fn create(&self) -> String {
        let table = self
            .builder(&self.data_url)
            .create(CreateTableOptions {
                table_name: TABLE.to_string(),
                schema: schema(),
                primary_key: Vec::new(),
                on_conflict: None,
                base_path: self.data_url.clone(),
                partition_column: None,
                vortex_config: VortexConfig {
                    inline_max_rows: 0,
                    inline_max_bytes: 0,
                    ..VortexConfig::default()
                },
            })
            .await
            .expect("create the table");
        self.ctx
            .register_table(TABLE, Arc::new(table))
            .expect("register");
        self.data_url.clone()
    }

    /// Open (or open again) the table, as the runtime does when it loads a
    /// dataset or reloads it after taking it over.
    async fn open_table(&self, data_url: &str) {
        let table = self
            .builder(data_url)
            .open(TABLE)
            .await
            .expect("open the table");
        let _ = self.ctx.deregister_table(TABLE);
        self.ctx
            .register_table(TABLE, Arc::new(table))
            .expect("register");
    }

    async fn insert(&self, first: i64, last: i64) -> datafusion::error::Result<()> {
        self.ctx
            .sql(&format!(
                "INSERT INTO {TABLE} SELECT value AS id, value * 10 AS v FROM generate_series({first}, {last})"
            ))
            .await?
            .collect()
            .await
            .map(|_| ())
    }

    async fn count_and_sum(&self) -> (i64, i64) {
        let batches = self
            .ctx
            .sql(&format!(
                "SELECT count(*), coalesce(sum(v), 0) FROM {TABLE}"
            ))
            .await
            .expect("plan")
            .collect()
            .await
            .expect("execute");
        let value = |column: usize| {
            batches[0]
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64")
                .value(0)
        };
        (value(0), value(1))
    }

    /// Requests that changed the location: writes and deletes, lease objects
    /// included.
    fn writes(&self) -> Vec<String> {
        self.requests
            .log()
            .into_iter()
            .filter(|request| {
                !matches!(
                    request.op,
                    Op::Get | Op::GetConditional | Op::Head | Op::List
                )
            })
            .map(|request| format!("{:?} {}", request.op, request.path))
            .collect()
    }

    /// Reopen the table and insert, until this instance writes the dataset.
    /// Returns how long that took.
    async fn insert_once_writer(&self, data_url: &str, first: i64, last: i64) -> Duration {
        let started = Instant::now();
        let deadline = started + Duration::from_secs(15);
        loop {
            self.open_table(data_url).await;
            match self.insert(first, last).await {
                Ok(()) => return started.elapsed(),
                Err(error) => {
                    assert!(
                        Instant::now() < deadline,
                        "never became the writer; last refusal: {error}"
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }
}

/// The first instance writes; a second follows its commits as a replica,
/// writes nothing to the location, the lease object included, and refuses a
/// write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_first_instance_writes_and_a_second_follows() {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let a = Instance::start(&objects, "a", LEASE).await;
    let data_url = a.create().await;
    a.insert(1, 100).await.expect("A writes");

    let b = Instance::start(&objects, "b", LEASE).await;
    b.open_table(&data_url).await;
    assert_eq!(b.count_and_sum().await, expected(1..=100));

    // Several renewals pass: A keeps writing, B keeps following.
    tokio::time::sleep(LEASE * 2).await;
    a.insert(101, 200).await.expect("A still writes");
    assert_eq!(b.count_and_sum().await, expected(1..=200));
    let refused = b.insert(1_000, 1_000).await.expect_err("B is a replica");
    assert!(
        refused
            .to_string()
            .contains("accepts no change from this table"),
        "{refused}"
    );
    assert_eq!(b.writes(), Vec::<String>::new(), "a replica writes nothing");
}

/// A restarted instance, with the same identity, takes over at once rather
/// than wait out its previous run's lease; the previous run's next write is
/// refused, and it then follows the new one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_instance_takes_over_at_once() {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    // A long lease: waiting it out would fail the test.
    let lease = Duration::from_secs(60);
    let old = Instance::start(&objects, "a", lease).await;
    let data_url = old.create().await;
    old.insert(1, 100).await.expect("the first run writes");

    let new = Instance::start(&objects, "a", lease).await;
    let waited = new.insert_once_writer(&data_url, 101, 200).await;
    assert!(waited < Duration::from_secs(5), "took {waited:?}");
    assert_eq!(new.count_and_sum().await, expected(1..=200));

    old.insert(1_000, 1_000)
        .await
        .expect_err("the previous run was fenced");
    assert_eq!(
        old.count_and_sum().await,
        expected(1..=200),
        "the previous run follows the new one"
    );
}

/// A writer that stops renewing (a crash) is replaced once its lease lapses:
/// the replica takes over, its table opened before that refuses writes, and
/// once reopened it writes on from every committed row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replica_takes_over_when_the_writer_stops() {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let a = Instance::start(&objects, "a", LEASE).await;
    let data_url = a.create().await;
    a.insert(1, 100).await.expect("A writes");
    let b = Instance::start(&objects, "b", LEASE).await;
    b.open_table(&data_url).await;
    assert_eq!(b.count_and_sum().await, expected(1..=100));
    // A table B opened as a replica, kept past the takeover.
    let before_takeover = b
        .builder(&data_url)
        .open(TABLE)
        .await
        .expect("open the table");
    b.ctx
        .register_table("before_takeover", Arc::new(before_takeover))
        .expect("register");

    drop(a);
    let waited = b.insert_once_writer(&data_url, 101, 200).await;
    assert!(
        waited >= LEASE / 2,
        "B wrote {waited:?} after A stopped, before A's lease could lapse"
    );
    assert_eq!(b.count_and_sum().await, expected(1..=200));

    // Seeded before B wrote the dataset, it would reuse sequence numbers and
    // miss A's keys: it refuses until it is opened again.
    let refused = b
        .ctx
        .sql("INSERT INTO before_takeover VALUES (1000, 10000)")
        .await
        .expect("plan")
        .collect()
        .await
        .expect_err("a table opened before the takeover does not write");
    assert!(
        refused
            .to_string()
            .contains("accepts no change from this table"),
        "{refused}"
    );
    assert_eq!(b.count_and_sum().await, expected(1..=200));
}

/// Writing passes from A to B and back to A, each time to an instance that
/// serves every row the previous writer committed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writing_passes_back_and_forth() {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let a = Instance::start(&objects, "a", LEASE).await;
    let data_url = a.create().await;
    a.insert(1, 100).await.expect("A writes");

    let b = Instance::start(&objects, "b", LEASE).await;
    drop(a);
    b.insert_once_writer(&data_url, 101, 200).await;
    assert_eq!(b.count_and_sum().await, expected(1..=200));

    let a = Instance::start(&objects, "a", LEASE).await;
    a.open_table(&data_url).await;
    assert_eq!(a.count_and_sum().await, expected(1..=200), "A follows B");
    drop(b);
    a.insert_once_writer(&data_url, 201, 300).await;
    assert_eq!(a.count_and_sum().await, expected(1..=300));
}

/// A graceful shutdown hands the lease back, so a replica takes over well
/// before the lease would have lapsed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_graceful_shutdown_hands_over_before_the_lease_lapses() {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let lease = Duration::from_secs(6);
    let a = Instance::start(&objects, "a", lease).await;
    let data_url = a.create().await;
    a.insert(1, 100).await.expect("A writes");
    let b = Instance::start(&objects, "b", lease).await;
    b.open_table(&data_url).await;

    a.catalog.shutdown().await.expect("A shuts down");
    drop(a);
    let waited = b.insert_once_writer(&data_url, 101, 200).await;
    assert!(
        waited < lease,
        "B took over {waited:?} after A handed the lease back, no sooner than a lapse"
    );
    assert_eq!(b.count_and_sum().await, expected(1..=200));
}
