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
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
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

/// The same takeover, but A's own clock stood still while B wrote for two
/// hours of store time, as a suspended VM's or a sleeping laptop's monotonic
/// clock does: on A's clock its lease has not lapsed, so its write gate is
/// still open. A's orphan sweep, which judges unnamed from A's view of the
/// table, predates every commit of B; that judgment alone must delete nothing.
/// The sweep retires through A's fenced log, which refuses it, and A's
/// retirement sweep deletes only what a commit retired.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_writer_whose_clock_stood_still_deletes_nothing_its_successor_commits() {
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
    let table_id = a.catalog.get_table(TABLE).await.expect("table").table_id;

    let b = Instance::start(&objects, "b", LEASE).await;
    b.open_table(&data_url).await;
    let is_lease = |path: &Path| path.as_ref().contains("/leases/");
    for op in [Op::Get, Op::GetConditional, Op::Head, Op::Put] {
        a.requests.fail_while(op, is_lease);
    }
    b.insert_once_writer(&data_url, 101, 200).await;
    clock.advance_clock(Duration::from_hours(2));
    b.insert(201, 300).await.expect("B writes again");
    assert_eq!(b.count_and_sum().await, expected(1..=300), "B took over");

    // A resumes: on its own clock its lease's deadline is still ahead.
    let gate = a.catalog.data_write_gate(&table_id);
    gate.hold_until(Instant::now() + Duration::from_hours(3));
    assert!(gate.allows(), "A's write gate is open, as its clock says");

    let swept = a_table
        .sweep_orphan_files_now()
        .await
        .expect_err("A's orphan sweep is refused after the takeover");
    assert!(
        swept
            .to_string()
            .contains("is now written by another instance"),
        "{swept}"
    );
    a_table.reclaim_retired_snapshots_now().await;

    let mut missing = Vec::new();
    for request in b.requests.log() {
        if request.op == Op::MultipartComplete
            && request.path.as_ref().ends_with(".vortex")
            && objects.head(&request.path).await.is_err()
        {
            missing.push(request.path.to_string());
        }
    }
    assert_eq!(
        missing,
        Vec::<String>::new(),
        "B's committed data files are all still stored"
    );
    // A third instance, with nothing cached, reads every row B committed.
    let c = Instance::start(&objects, "c", LEASE).await;
    c.open_table(&data_url).await;
    let rows = c
        .ctx
        .sql(&format!(
            "SELECT count(*), coalesce(sum(v), 0) FROM (SELECT id, v FROM {TABLE} WHERE v >= 0)"
        ))
        .await
        .expect("plan")
        .collect()
        .await
        .expect("a fresh instance reads every committed file");
    let value = |column: usize| {
        rows[0]
            .column(column)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64")
            .value(0)
    };
    assert_eq!((value(0), value(1)), expected(1..=300));
}

/// A writer is taken over in the middle of a sweep, with its deletes still in
/// flight: A retired orphans while it held the lease, and its retirement sweep
/// has sent their deletes, which the network holds. A's lease requests then
/// fail, B takes over and commits for two hours of store time, and only then
/// do A's held deletes land. They remove only what A's committed record
/// retired; A's forget record is refused, so A learns it was fenced and a
/// later orphan sweep of A's retires nothing; and every file A or B committed
/// is still stored, so a fresh instance reads every row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_writer_taken_over_mid_sweep_with_deletes_in_flight_deletes_nothing_its_successor_commits()
 {
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
    let table_id = a.catalog.get_table(TABLE).await.expect("table").table_id;

    // Objects no commit names beside A's data file, as a crashed write leaves
    // them, then two hours of store time and a commit that dates the head.
    let table_dir = a
        .requests
        .log()
        .into_iter()
        .find(|request| {
            request.op == Op::MultipartComplete && request.path.as_ref().ends_with(".vortex")
        })
        .and_then(|request| {
            request
                .path
                .as_ref()
                .rsplit_once('/')
                .map(|(dir, _)| dir.to_string())
        })
        .expect("A's data file");
    let orphans: Vec<Path> = (0..3)
        .map(|i| {
            Path::from(format!(
                "{table_dir}/{}_{i:05}.vortex",
                uuid::Uuid::now_v7().simple()
            ))
        })
        .collect();
    for orphan in &orphans {
        objects
            .put(orphan, PutPayload::from_static(b"orphan"))
            .await
            .expect("plant an orphan");
    }
    clock.advance_clock(Duration::from_hours(2));
    a.insert(101, 200).await.expect("A writes again");
    let retired = a_table
        .sweep_orphan_files_now()
        .await
        .expect("A's orphan sweep");
    assert_eq!(
        retired.retired.len(),
        orphans.len(),
        "A retired the orphans: {retired:?}"
    );

    // A's retirement sweep sends their deletes; the first is held in flight,
    // and the rest of its batch waits behind it.
    let is_orphan = {
        let orphans = orphans.clone();
        move |path: &Path| orphans.contains(path)
    };
    a.requests
        .arm(Op::Delete, is_orphan, object_store_sim::fault::Fault::Hold);
    let sweeping = {
        let a_table = Arc::clone(&a_table);
        tokio::spawn(async move { a_table.reclaim_retired_snapshots_now().await })
    };
    let held = tokio::time::timeout(Duration::from_secs(15), a.requests.wait_held(None))
        .await
        .expect("A's delete is held in flight");
    assert!(
        orphans.iter().all(|orphan| clock
            .log()
            .iter()
            .all(|r| r.path != *orphan || r.op != Op::Delete)),
        "no delete has landed yet"
    );

    // B takes over and writes for two hours of store time.
    let b = Instance::start(&objects, "b", LEASE).await;
    b.open_table(&data_url).await;
    let is_lease = |path: &Path| path.as_ref().contains("/leases/");
    for op in [Op::Get, Op::GetConditional, Op::Head, Op::Put] {
        a.requests.fail_while(op, is_lease);
    }
    b.insert_once_writer(&data_url, 201, 300).await;
    clock.advance_clock(Duration::from_hours(2));
    b.insert(301, 400).await.expect("B writes again");
    assert_eq!(b.count_and_sum().await, expected(1..=400), "B took over");

    // A's deletes in flight land now.
    a.requests
        .deliver(held)
        .await
        .expect("the held delete lands");
    a.requests.respond(held).expect("A hears it");
    tokio::time::timeout(Duration::from_secs(15), sweeping)
        .await
        .expect("A's retirement sweep finishes")
        .expect("join");

    // The sweep's `ForgetRetired` record, sent once its deletes returned, is
    // refused: A learns it was fenced, even with its lease's deadline ahead on
    // its own clock, and a later orphan sweep of A's retires nothing.
    let gate = a.catalog.data_write_gate(&table_id);
    gate.hold_until(Instant::now() + Duration::from_hours(3));
    assert!(!gate.allows(), "A learned it no longer writes the dataset");
    let late = a_table.sweep_orphan_files_now().await;
    assert!(
        late.as_ref().map_or(true, |sweep| sweep.retired.is_empty()),
        "A's late orphan sweep retires nothing: {late:?}"
    );
    let landed: Vec<String> = clock
        .log()
        .into_iter()
        .filter(|request| request.op == Op::Delete && request.result.is_ok())
        .map(|request| request.path.to_string())
        .collect();
    let mut deleted_orphans: Vec<&Path> = orphans
        .iter()
        .filter(|orphan| landed.iter().any(|path| path == orphan.as_ref()))
        .collect();
    deleted_orphans.sort();
    let mut expected_orphans: Vec<&Path> = orphans.iter().collect();
    expected_orphans.sort();
    assert_eq!(
        deleted_orphans, expected_orphans,
        "A's in-flight deletes landed after the takeover, on the orphans A retired"
    );

    let mut missing = Vec::new();
    for request in a.requests.log().into_iter().chain(b.requests.log()) {
        if request.op == Op::MultipartComplete
            && request.path.as_ref().ends_with(".vortex")
            && objects.head(&request.path).await.is_err()
        {
            missing.push(request.path.to_string());
        }
    }
    assert_eq!(
        missing,
        Vec::<String>::new(),
        "every data file A and B committed is still stored"
    );
    let c = Instance::start(&objects, "c", LEASE).await;
    c.open_table(&data_url).await;
    let rows = c
        .ctx
        .sql(&format!(
            "SELECT count(*), coalesce(sum(v), 0) FROM (SELECT id, v FROM {TABLE} WHERE v >= 0)"
        ))
        .await
        .expect("plan")
        .collect()
        .await
        .expect("a fresh instance reads every committed file");
    let value = |column: usize| {
        rows[0]
            .column(column)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64")
            .value(0)
    };
    assert_eq!((value(0), value(1)), expected(1..=400));
}

/// A reopens the table, and its load of the persisted lookup-index runs is
/// paused on a run read. B takes over and persists a run of its own. A's clock
/// stands still, so its lease's deadline is still ahead, and A's load then
/// removes the run files that the registered set it read before the pause
/// does not name. It must not delete the run B registered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_paused_index_load_deletes_no_run_its_successor_registers() {
    let clock = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let objects: Arc<dyn ObjectStore> = Arc::clone(&clock) as Arc<dyn ObjectStore>;
    let indexed = |builder: CayenneTableProviderBuilder| {
        builder.with_secondary_indexes(vec![vec!["v".to_string()]])
    };
    let a = Instance::start(&objects, "a", LEASE).await;
    let data_url = a.data_url.clone();
    let a_first = indexed(a.builder(&data_url))
        .create(CreateTableOptions {
            table_name: TABLE.to_string(),
            schema: schema(),
            primary_key: Vec::new(),
            on_conflict: None,
            base_path: data_url.clone(),
            partition_column: None,
            vortex_config: VortexConfig {
                inline_max_rows: 0,
                inline_max_bytes: 0,
                ..VortexConfig::default()
            },
        })
        .await
        .expect("create the table");
    a.ctx
        .register_table(TABLE, Arc::new(a_first))
        .expect("register");
    a.insert(1, 100).await.expect("A writes");
    let table_id = a.catalog.get_table(TABLE).await.expect("table").table_id;
    let runs = |catalog: Arc<dyn MetadataCatalog>, table_id: String| async move {
        let mut names: Vec<String> = catalog
            .list_index_runs(&table_id)
            .await
            .expect("list runs")
            .into_iter()
            .map(|run| format!("{}/{}", run.index_key, run.run_name))
            .collect();
        names.sort();
        names
    };
    let deadline = Instant::now() + Duration::from_secs(15);
    while runs(Arc::clone(&a.catalog), table_id.clone())
        .await
        .is_empty()
    {
        assert!(Instant::now() < deadline, "A persisted no index run");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let a_runs = runs(Arc::clone(&a.catalog), table_id.clone()).await;
    eprintln!("A's registered runs: {a_runs:?}");

    // A reopens the table; its load pauses on the first run read.
    let is_run = |path: &Path| path.as_ref().contains("/_lookup_index/");
    a.requests.gate_reads(is_run);
    let reopening = {
        let builder = indexed(a.builder(&data_url));
        tokio::spawn(async move { builder.open(TABLE).await.map(|_| ()) })
    };
    tokio::time::timeout(Duration::from_secs(15), a.requests.wait_gated(1))
        .await
        .expect("A's load reads a run");

    // B takes over and persists a run of its own, as its index sync does:
    // the run file first, then its registration. (B opens no table here: in
    // this one test process both instances' index loads would share one lock.)
    let b = Instance::start(&objects, "b", LEASE).await;
    for op in [Op::Get, Op::GetConditional, Op::Head, Op::Put] {
        a.requests
            .fail_while(op, |path: &Path| path.as_ref().contains("/leases/"));
    }
    let (key, a_name) = a_runs[0].split_once('/').expect("key/run");
    let b_name = "00000000000000b1.run";
    let b_path = {
        let stored: Vec<String> = objects
            .list(None)
            .map_ok(|meta| meta.location.to_string())
            .try_collect::<Vec<_>>()
            .await
            .expect("list");
        let a_path = stored
            .into_iter()
            .find(|path| path.ends_with(&format!("{key}/{a_name}")))
            .expect("A's run file");
        Path::from(a_path.replace(a_name, b_name))
    };
    let record = cayenne::metadata::IndexRunRecord {
        table_id: table_id.clone(),
        index_key: key.to_string(),
        run_name: b_name.to_string(),
        row_count: 100,
        size_bytes: 6,
    };
    objects
        .put(&b_path, PutPayload::from_static(b"b-run!"))
        .await
        .expect("B's run file");
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if b.catalog.register_index_run(&record).await.is_ok() {
            break;
        }
        assert!(Instant::now() < deadline, "B never became the writer");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let b_runs = runs(Arc::clone(&b.catalog), table_id.clone()).await;
    eprintln!("B's registered runs: {b_runs:?}");

    // A's clock stood still; its load resumes.
    a.catalog
        .data_write_gate(&table_id)
        .hold_until(Instant::now() + Duration::from_hours(3));
    a.requests.open_reads();
    let reopened = tokio::time::timeout(Duration::from_secs(30), reopening)
        .await
        .expect("A's reopen finishes")
        .expect("join");
    eprintln!("A's reopen: {reopened:?}");

    let stored: Vec<String> = objects
        .list(None)
        .map_ok(|meta| meta.location.to_string())
        .try_collect::<Vec<_>>()
        .await
        .expect("list")
        .into_iter()
        .filter(|path| path.contains("/_lookup_index/"))
        .collect();
    eprintln!("stored run files: {stored:?}");
    let deletes: Vec<String> = a
        .requests
        .log()
        .into_iter()
        .filter(|request| {
            request.op == Op::Delete && request.path.as_ref().contains("/_lookup_index/")
        })
        .map(|request| format!("{} ok={}", request.path, request.result.is_ok()))
        .collect();
    assert!(
        deletes.iter().all(|delete| delete.ends_with("ok=false")),
        "A deletes no run file: {deletes:?}"
    );
    let missing: Vec<&String> = b_runs
        .iter()
        .filter(|run| !stored.iter().any(|path| path.ends_with(run.as_str())))
        .collect();
    assert_eq!(
        missing,
        Vec::<&String>::new(),
        "every run B registered keeps its file"
    );
}

/// A run file no run is registered for, as a sync that stopped before
/// registering it leaves, is retired when the table loads its persisted
/// lookup index, and the retirement sweep deletes it; the registered run and
/// its file are kept.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unregistered_index_run_is_retired_at_load_and_then_deleted() {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let indexed = |builder: CayenneTableProviderBuilder| {
        builder.with_secondary_indexes(vec![vec!["v".to_string()]])
    };
    let a = Instance::start(&objects, "a", LEASE).await;
    let data_url = a.data_url.clone();
    let table = indexed(a.builder(&data_url))
        .create(CreateTableOptions {
            table_name: TABLE.to_string(),
            schema: schema(),
            primary_key: Vec::new(),
            on_conflict: None,
            base_path: data_url.clone(),
            partition_column: None,
            vortex_config: VortexConfig {
                inline_max_rows: 0,
                inline_max_bytes: 0,
                ..VortexConfig::default()
            },
        })
        .await
        .expect("create the table");
    a.ctx
        .register_table(TABLE, Arc::new(table))
        .expect("register");
    a.insert(1, 100).await.expect("A writes");
    let table_id = a.catalog.get_table(TABLE).await.expect("table").table_id;
    let deadline = Instant::now() + Duration::from_secs(15);
    let registered = loop {
        let runs = a.catalog.list_index_runs(&table_id).await.expect("runs");
        if let Some(run) = runs.into_iter().next() {
            break run;
        }
        assert!(Instant::now() < deadline, "A persisted no index run");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let run_files = || async {
        objects
            .list(None)
            .map_ok(|meta| meta.location.to_string())
            .try_collect::<Vec<_>>()
            .await
            .expect("list")
            .into_iter()
            .filter(|path| path.contains("/_lookup_index/"))
            .collect::<Vec<String>>()
    };
    let registered_path = run_files()
        .await
        .into_iter()
        .find(|path| path.ends_with(&registered.run_name))
        .expect("the registered run's file");
    let stray = registered_path.replace(&registered.run_name, "00000000000000aa.run");
    objects
        .put(
            &Path::from(stray.as_str()),
            PutPayload::from_static(b"stray"),
        )
        .await
        .expect("plant an unregistered run file");

    // Reopen: the load retires the stray file and keeps the registered one.
    let reopened = Arc::new(
        indexed(a.builder(&data_url))
            .open(TABLE)
            .await
            .expect("reopen"),
    );
    let native = a
        .catalog
        .as_any()
        .downcast_ref::<ObjectNativeCatalog>()
        .expect("the object-native catalog");
    let retiring = native.retiring(&table_id).await.expect("retired keys");
    let stray_key = format!(
        "_lookup_index/{}/00000000000000aa.run",
        registered.index_key
    );
    assert!(
        retiring.contains(&stray_key),
        "the unregistered run file is retired: {retiring:?}"
    );
    assert!(
        run_files().await.contains(&stray),
        "and not deleted before the retention"
    );

    reopened.reclaim_retired_snapshots_now().await;
    let files = run_files().await;
    assert_eq!(
        (files.contains(&stray), files.contains(&registered_path)),
        (false, true),
        "the retirement sweep deletes the unregistered file and keeps the registered run's: {files:?}"
    );
}
