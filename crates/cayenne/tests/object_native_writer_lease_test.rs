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
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use object_store_sim::Op;
use object_store_sim::fault::{Fault, FaultStore};

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

    /// Load the dataset as the accelerator does: open it when the catalog
    /// holds it, create it otherwise.
    async fn open_or_create(&self) -> Result<(), String> {
        match self.catalog.get_table(TABLE).await {
            Ok(_) => {
                let table = self
                    .builder(&self.data_url)
                    .open(TABLE)
                    .await
                    .map_err(|error| format!("open: {error}"))?;
                self.ctx
                    .register_table(TABLE, Arc::new(table))
                    .map_err(|error| error.to_string())?;
                Ok(())
            }
            Err(cayenne::CatalogError::TableNotFound { .. }) => {
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
                    .map_err(|error| format!("create: {error}"))?;
                self.ctx
                    .register_table(TABLE, Arc::new(table))
                    .map_err(|error| error.to_string())?;
                Ok(())
            }
            Err(error) => Err(format!("get_table: {error}")),
        }
    }

    /// Load the dataset as the accelerator does, defined as `definition`.
    async fn load_defined(
        &self,
        definition: Option<cayenne::DatasetDefinition>,
    ) -> Result<(), String> {
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
                    definition: definition.map(Arc::new),
                    ..VortexConfig::default()
                },
            })
            .await
            .map_err(|error| error.to_string())?;
        let _ = self.ctx.deregister_table(TABLE);
        self.ctx
            .register_table(TABLE, Arc::new(table))
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    /// Whether this instance writes the dataset now.
    fn writes_dataset(&self) -> bool {
        self.catalog
            .as_any()
            .downcast_ref::<ObjectNativeCatalog>()
            .and_then(|catalog| catalog.writer_watch(TABLE))
            .is_some_and(|watch| *watch.borrow())
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

/// Two instances start together on an empty location, as a deployment of two
/// replicas does, and each loads the dataset the way the accelerator does:
/// open it if the location holds it, create it otherwise. Both serve it, and
/// exactly one writes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_instances_starting_together_both_serve_the_dataset() {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (a, b) = tokio::join!(
        Instance::start(&objects, "a", LEASE),
        Instance::start(&objects, "b", LEASE)
    );
    let (loaded_a, loaded_b) = tokio::join!(a.open_or_create(), b.open_or_create());
    loaded_a.expect("A loads the dataset");
    loaded_b.expect("B loads the dataset");

    // Whichever writes it takes the rows; both serve them.
    let (written_a, written_b) = tokio::join!(a.insert(1, 100), b.insert(1, 100));
    assert!(
        written_a.is_ok() != written_b.is_ok(),
        "exactly one instance writes: A {written_a:?}, B {written_b:?}"
    );
    assert_eq!(a.count_and_sum().await, expected(1..=100));
    assert_eq!(b.count_and_sum().await, expected(1..=100));
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

/// A writer that can no longer renew its lease stops writing data once the
/// lease could have lapsed, before another instance can take over: it deletes
/// nothing its successor commits. Here A still reaches the data and the log
/// but not its lease object, as on a partial network failure; B takes over and
/// commits rows A never read. A's orphan sweep, which deletes what A's own
/// state does not name, must not remove B's files.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_writer_that_cannot_renew_its_lease_deletes_nothing_its_successor_commits() {
    // One clock for the location, which both instances' requests reach.
    let clock = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let objects: Arc<dyn ObjectStore> = Arc::clone(&clock) as Arc<dyn ObjectStore>;
    let a = Instance::start(&objects, "a", LEASE).await;
    let a_table = Arc::new(
        a.builder(&a.data_url)
            .create(CreateTableOptions {
                table_name: TABLE.to_string(),
                schema: schema(),
                primary_key: Vec::new(),
                on_conflict: None,
                base_path: a.data_url.clone(),
                partition_column: None,
                vortex_config: VortexConfig {
                    inline_max_rows: 0,
                    inline_max_bytes: 0,
                    ..VortexConfig::default()
                },
            })
            .await
            .expect("create the table"),
    );
    a.ctx
        .register_table(
            TABLE,
            Arc::clone(&a_table) as Arc<dyn datafusion::datasource::TableProvider>,
        )
        .expect("register");
    let data_url = a.data_url.clone();
    a.insert(1, 100).await.expect("A writes");

    let b = Instance::start(&objects, "b", LEASE).await;
    b.open_table(&data_url).await;
    assert_eq!(b.count_and_sum().await, expected(1..=100));

    // A's lease requests fail from now on; everything else still reaches the store.
    let is_lease = |path: &Path| path.as_ref().contains("/leases/");
    for op in [Op::Get, Op::GetConditional, Op::Head, Op::Put] {
        a.requests.fail_while(op, is_lease);
    }
    b.insert_once_writer(&data_url, 101, 200).await;
    assert_eq!(b.count_and_sum().await, expected(1..=200), "B took over");

    // Two hours pass on the store's clock, and B commits again: B's first
    // files are past the orphan sweep's age floor.
    clock.advance_clock(Duration::from_hours(2));
    b.insert(201, 300).await.expect("B writes again");
    assert_eq!(b.count_and_sum().await, expected(1..=300));
    let swept = a_table.sweep_orphan_files_now().await;
    eprintln!("A's orphan sweep: {swept:?}");

    assert_eq!(
        b.count_and_sum().await,
        expected(1..=300),
        "B still reads every row it committed after A's sweep"
    );
    // Every data file B uploaded is still in the store: B committed each one
    // in an acknowledged insert (none of them was retired since).
    for request in b.requests.log() {
        if request.op == Op::MultipartComplete && request.path.as_ref().ends_with(".vortex") {
            assert!(
                objects.head(&request.path).await.is_ok(),
                "B's committed data file {} is gone from the store",
                request.path
            );
        }
    }
    let deletes: Vec<String> = a
        .requests
        .log()
        .into_iter()
        .filter(|request| request.op == Op::Delete && request.path.as_ref().contains("/data/"))
        .map(|request| format!("{:?} {}", request.op, request.path))
        .collect();
    assert_eq!(
        deletes,
        Vec::<String>::new(),
        "A deletes no data once its lease could have lapsed"
    );
}

/// A writer whose lease requests fail stops writing at the lease's deadline,
/// and writes again, from the same table, once a renewal succeeds while no
/// other instance has taken over.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_writer_whose_lease_requests_recover_writes_again() {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let a = Instance::start(&objects, "a", LEASE).await;
    a.create().await;
    a.insert(1, 100).await.expect("A writes");

    let is_lease = |path: &Path| path.as_ref().contains("/leases/");
    for op in [Op::Get, Op::GetConditional, Op::Head, Op::Put] {
        a.requests.fail_while(op, is_lease);
    }
    tokio::time::sleep(LEASE * 2).await;
    let refused = a
        .insert(101, 200)
        .await
        .expect_err("past its lease's deadline, A sends no write");
    assert!(
        refused
            .to_string()
            .contains("accepts no change from this table"),
        "{refused}"
    );

    a.requests.clear_rules();
    let deadline = Instant::now() + LEASE * 10;
    loop {
        match a.insert(101, 200).await {
            Ok(()) => break,
            Err(error) => {
                assert!(Instant::now() < deadline, "A never wrote again: {error}");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    assert_eq!(a.count_and_sum().await, expected(1..=200));
}

/// `orders` from one `from`, at the server `host` names.
fn defined_at(app: &str, host: &str) -> cayenne::DatasetDefinition {
    cayenne::DatasetDefinition::new(app)
        .with_setting("from", "postgres:public.orders")
        .with_connection([("pg_connection_string", host, true)])
}

/// The dataset's own writer lease object, not the registry's.
fn dataset_lease(path: &Path) -> bool {
    path.as_ref().contains("/datasets/") && path.as_ref().ends_with("leases/writer.json")
}

/// An instance whose Spicepod defines the dataset differently is refused its
/// copy, and never becomes its writer: not even when the lease request that
/// takes the copy over, once its writer stopped, was in flight while it was
/// refused. It hands that lease back, so the next instance that matches the
/// copy writes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_instance_refused_its_copy_hands_back_a_lease_it_won_meanwhile() {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let a = Instance::start(&objects, "a", LEASE).await;
    a.load_defined(Some(defined_at("shop", "db-a")))
        .await
        .expect("A creates the copy");
    let b = Instance::start(&objects, "b", LEASE).await;
    assert!(!b.writes_dataset(), "B opens the copy as a replica");

    // A stops without handing its lease back. Once it lapses, B's request to
    // take the dataset over is held before it reaches the store.
    b.requests.arm(Op::Cas, dataset_lease, Fault::Hold);
    drop(a);
    let held = b.requests.wait_held(None).await;

    let error = b
        .load_defined(Some(defined_at("store", "db-b")))
        .await
        .expect_err("another definition is refused");
    assert!(
        error.contains(
            "was built from a different definition (one of the secret settings `pg_connection_string` changed)"
        ),
        "{error}"
    );
    assert!(
        !error.contains("db-a") && !error.contains("db-b"),
        "{error}"
    );

    // The takeover lands, and B learns it holds the lease.
    b.requests.deliver(held).await.expect("the takeover lands");
    b.requests.respond(held).expect("B reads its answer");
    let deadline = Instant::now() + LEASE * 2;
    while Instant::now() < deadline {
        assert!(
            !b.writes_dataset(),
            "B became the writer of a copy it does not serve"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // An instance with the copy's definition writes it.
    let c = Instance::start(&objects, "c", LEASE).await;
    c.load_defined(Some(defined_at("shop", "db-a")))
        .await
        .expect("C serves the copy");
    c.insert_once_writer(&c.data_url.clone(), 1, 10).await;
    assert_eq!(c.count_and_sum().await, expected(1..=10));
}

/// A copy built with no definition (as by DDL) says nothing about its source,
/// so a dataset a Spicepod defines is refused it rather than serve its rows:
/// both the instance that writes it and a replica.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_copy_with_no_recorded_definition_is_refused() {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let a = Instance::start(&objects, "a", LEASE).await;
    a.load_defined(None)
        .await
        .expect("A creates a copy with no definition");
    let b = Instance::start(&objects, "b", LEASE).await;
    for (instance, name) in [(&b, "the replica"), (&a, "the writer")] {
        let error = instance
            .load_defined(Some(defined_at("store", "db-b")))
            .await
            .expect_err("a copy with no definition is refused");
        assert!(
            error
                .contains("was built from a different definition (the copy records no definition)"),
            "{name}: {error}"
        );
    }
}
