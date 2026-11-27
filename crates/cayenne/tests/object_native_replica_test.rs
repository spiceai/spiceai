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

//! Two instances on one object-native location: one writes a dataset, the
//! other serves queries from what the first committed.

use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray};
use cayenne::metadata::{CreateTableOptions, ObjectStoreConfig, VortexConfig};
use cayenne::metastore::object_native::{Access, NativeCatalogConfig, ObjectNativeCatalog};
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog, ScanViewReuse};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};
use object_store::ObjectStore;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store_sim::Op;
use object_store_sim::fault::FaultStore;

const LOCATION: &str = "s3://bucket/state/cayenne/v1";
const TABLE: &str = "orders";

/// Registration on a replica keeps the writer's settings and schema without
/// publishing a config change or a schema widening of its own.
#[tokio::test]
async fn replica_reopens_with_different_settings_and_wider_schema() {
    use cayenne::metadata::SchemaEvolutionMode;
    use futures::TryStreamExt;
    use object_store::ObjectStoreExt;

    for inline in [false, true] {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let requests = Arc::new(FaultStore::new(Arc::clone(&objects)));
        let replica_store: Arc<dyn ObjectStore> = Arc::clone(&requests) as Arc<dyn ObjectStore>;
        let (a, data_url) = Instance::open(&objects, Access::Writer).await;
        let writer_config = VortexConfig {
            sort_columns: vec!["id".to_string()],
            inline_max_rows: if inline { 1000 } else { 0 },
            inline_max_bytes: if inline { 1_000_000 } else { 0 },
            ..VortexConfig::default()
        };
        let options = CreateTableOptions {
            table_name: TABLE.to_string(),
            schema: schema(),
            primary_key: Vec::new(),
            on_conflict: None,
            base_path: data_url.clone(),
            partition_column: None,
            vortex_config: writer_config.clone(),
        };
        a.register(
            a.builder(&objects, &data_url)
                .create(options.clone())
                .await
                .expect("create writer"),
        );
        a.run("INSERT INTO orders VALUES (3, 30), (1, 10), (2, 20)")
            .await
            .expect("write rows");
        let stored = a.catalog.get_table(TABLE).await.expect("writer metadata");
        let heads: Vec<_> = objects
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .expect("list heads")
            .into_iter()
            .filter(|object| object.location.filename() == Some("head"))
            .collect();
        assert_ne!(heads.len(), 0, "the writer's commits wrote a log head");
        let mut before = Vec::new();
        for head in &heads {
            before.push(
                objects
                    .get(&head.location)
                    .await
                    .expect("head")
                    .bytes()
                    .await
                    .expect("head bytes"),
            );
        }

        let (b, _) = Instance::open(&replica_store, Access::Replica).await;
        let replica = b
            .builder(&replica_store, &data_url)
            .create(CreateTableOptions {
                schema: Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("v", DataType::Int64, false),
                    Field::new("extra", DataType::Utf8, true),
                ])),
                vortex_config: VortexConfig {
                    sort_columns: vec!["v".to_string()],
                    schema_evolution: SchemaEvolutionMode::Widen,
                    ..writer_config
                },
                ..options
            })
            .await
            .expect("replica opens with its own settings and wider schema");
        assert_eq!(replica.schema().as_ref(), stored.schema.as_ref());
        let reopened = b.catalog.get_table(TABLE).await.expect("replica metadata");
        assert_eq!(reopened.table_id, stored.table_id);
        assert_eq!(reopened.schema, stored.schema);
        assert_eq!(
            serde_json::to_value(&reopened.vortex_config).expect("config"),
            serde_json::to_value(&stored.vortex_config).expect("config")
        );
        b.register(replica);
        let batches = b
            .ctx
            .sql("SELECT id, v FROM orders ORDER BY id")
            .await
            .expect("plan rows")
            .collect()
            .await
            .expect("read rows");
        let rows: Vec<_> = batches
            .iter()
            .flat_map(|batch| {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("ids");
                let values = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("values");
                (0..batch.num_rows()).map(move |row| (ids.value(row), values.value(row)))
            })
            .collect();
        assert_eq!(rows, vec![(1, 10), (2, 20), (3, 30)]);
        for (head, expected) in heads.iter().zip(before) {
            assert_eq!(
                objects
                    .get(&head.location)
                    .await
                    .expect("head")
                    .bytes()
                    .await
                    .expect("head bytes"),
                expected
            );
        }
        assert!(
            requests.log().iter().all(|request| matches!(
                request.op,
                Op::Get | Op::GetConditional | Op::Head | Op::List
            )),
            "replica sends only reads"
        );
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

/// One instance: its own catalog handle and session over the shared store.
struct Instance {
    catalog: Arc<dyn MetadataCatalog>,
    ctx: SessionContext,
}

impl Instance {
    /// Open the location; returns the instance and the data path a new
    /// dataset `TABLE` would get there.
    async fn open(store: &Arc<dyn ObjectStore>, access: Access) -> (Self, String) {
        let catalog = ObjectNativeCatalog::open_or_create_at_with_config(
            Arc::clone(store),
            Path::from(crate::common::LOCATION_CATALOG),
            LOCATION,
            NativeCatalogConfig {
                access,
                ..NativeCatalogConfig::default()
            },
        )
        .await
        .expect("open the object-native catalog");
        catalog.init().await.expect("init");
        let data_url = crate::common::location_data_url(&catalog, TABLE);
        (
            Self {
                catalog: Arc::new(catalog),
                ctx: SessionContext::new(),
            },
            data_url,
        )
    }

    fn builder(&self, store: &Arc<dyn ObjectStore>, data_url: &str) -> CayenneTableProviderBuilder {
        crate::common::table_builder_on_store(
            Arc::clone(&self.catalog),
            self.ctx.runtime_env(),
            ObjectStoreConfig {
                url: url::Url::parse(data_url).expect("data url"),
                store: Arc::clone(store),
            },
        )
    }

    fn register(&self, table: impl TableProvider + 'static) {
        self.ctx
            .register_table(TABLE, Arc::new(table))
            .expect("register");
    }

    async fn run(&self, sql: &str) -> datafusion::error::Result<()> {
        self.ctx.sql(sql).await?.collect().await.map(|_| ())
    }

    async fn insert(&self, first: i64, last: i64) -> datafusion::error::Result<()> {
        self.run(&format!(
            "INSERT INTO {TABLE} SELECT value AS id, value * 10 AS v FROM generate_series({first}, {last})"
        ))
        .await
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
}

/// `(count, sum(v))` of the rows `ids` with `v = id * 10`.
fn expected(ids: impl Iterator<Item = i64>) -> (i64, i64) {
    ids.fold((0, 0), |(count, sum), id| (count + 1, sum + id * 10))
}

/// How the scenario's table keeps its rows.
#[derive(Clone, Copy, Debug)]
struct Shape {
    /// A primary key with upserts, so a delete lands in deletion vectors.
    keyed: bool,
    /// Small writes inlined into the metastore rather than written as files.
    inline: bool,
}

/// A writes, B opens the same location as a replica and serves A's rows, A
/// keeps writing and deleting, and every query B starts after A's change was
/// acknowledged returns it. B sends no write to the location, and refuses one.
async fn writer_and_replica(shape: Shape) {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let replica_requests = Arc::new(FaultStore::new(Arc::clone(&objects)));
    let replica_store: Arc<dyn ObjectStore> = Arc::clone(&replica_requests) as Arc<dyn ObjectStore>;

    let (a, data_url) = Instance::open(&objects, Access::Writer).await;
    let vortex_config = if shape.inline {
        VortexConfig::default()
    } else {
        VortexConfig {
            inline_max_rows: 0,
            inline_max_bytes: 0,
            ..VortexConfig::default()
        }
    };
    let table = a
        .builder(&objects, &data_url)
        .create(CreateTableOptions {
            table_name: TABLE.to_string(),
            schema: schema(),
            primary_key: if shape.keyed {
                vec!["id".to_string()]
            } else {
                Vec::new()
            },
            on_conflict: shape
                .keyed
                .then(|| OnConflict::Upsert(ColumnReference::new(vec!["id".to_string()]))),
            base_path: data_url.clone(),
            partition_column: None,
            vortex_config,
        })
        .await
        .expect("create the table on A");
    a.register(table);
    a.insert(1, 100).await.expect("A writes 1..=100");
    assert_eq!(a.count_and_sum().await, expected(1..=100), "{shape:?}: A");

    let (b, _) = Instance::open(&replica_store, Access::Replica).await;
    let table = b
        .builder(&replica_store, &data_url)
        .open(TABLE)
        .await
        .expect("open the table on B");
    b.register(table);
    assert_eq!(
        b.count_and_sum().await,
        expected(1..=100),
        "{shape:?}: B serves what A committed before B opened"
    );
    // Unchanged head: B serves the same rows again.
    assert_eq!(
        b.count_and_sum().await,
        expected(1..=100),
        "{shape:?}: B again"
    );

    a.insert(101, 200)
        .await
        .expect("A keeps writing after B opened");
    assert_eq!(a.count_and_sum().await, expected(1..=200), "{shape:?}: A");
    assert_eq!(
        b.count_and_sum().await,
        expected(1..=200),
        "{shape:?}: a query B starts after A's insert was acknowledged returns it"
    );

    a.run(&format!("DELETE FROM {TABLE} WHERE id <= 50"))
        .await
        .expect("A deletes 1..=50");
    assert_eq!(a.count_and_sum().await, expected(51..=200), "{shape:?}: A");
    assert_eq!(
        b.count_and_sum().await,
        expected(51..=200),
        "{shape:?}: a query B starts after A's delete was acknowledged leaves those rows out"
    );

    let refused = b
        .insert(1_000, 1_000)
        .await
        .expect_err("B accepts no write");
    assert!(
        refused
            .to_string()
            .contains("accepts no change from this table"),
        "{shape:?}: {refused}"
    );
    assert_eq!(
        b.count_and_sum().await,
        expected(51..=200),
        "{shape:?}: B after refusing"
    );

    let writes: Vec<String> = replica_requests
        .log()
        .into_iter()
        .filter(|request| {
            !matches!(
                request.op,
                Op::Get | Op::GetConditional | Op::Head | Op::List
            )
        })
        .map(|request| format!("{:?} {}", request.op, request.path))
        .collect();
    assert!(
        writes.is_empty(),
        "{shape:?}: a replica writes nothing to the location: {writes:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replica_follows_appends_and_deletes_in_files() {
    writer_and_replica(Shape {
        keyed: false,
        inline: false,
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replica_follows_inlined_appends_and_deletes() {
    writer_and_replica(Shape {
        keyed: false,
        inline: true,
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replica_follows_keyed_upserts_and_deletes_in_files() {
    writer_and_replica(Shape {
        keyed: true,
        inline: false,
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replica_follows_keyed_inlined_upserts_and_deletes() {
    writer_and_replica(Shape {
        keyed: true,
        inline: true,
    })
    .await;
}

/// Concurrent queries on a replica share one read of the dataset head, and a
/// query only shares a read sent after it started. One query's read is held in
/// flight while A commits and sixteen more queries start: those sixteen cannot
/// use the held read, sent before they started, so they share exactly one
/// more, and every query returns A's commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_replica_queries_share_one_head_read_sent_after_they_start() {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let replica_requests = Arc::new(FaultStore::new(Arc::clone(&objects)));
    let replica_store: Arc<dyn ObjectStore> = Arc::clone(&replica_requests) as Arc<dyn ObjectStore>;

    let (a, data_url) = Instance::open(&objects, Access::Writer).await;
    let table = a
        .builder(&objects, &data_url)
        .create(CreateTableOptions {
            table_name: TABLE.to_string(),
            schema: schema(),
            primary_key: Vec::new(),
            on_conflict: None,
            base_path: data_url.clone(),
            partition_column: None,
            vortex_config: VortexConfig::default(),
        })
        .await
        .expect("create the table on A");
    a.register(table);
    a.insert(1, 100).await.expect("A writes 1..=100");

    let (b, _) = Instance::open(&replica_store, Access::Replica).await;
    let table = b
        .builder(&replica_store, &data_url)
        .open(TABLE)
        .await
        .expect("open the table on B");
    b.register(table);
    let b = Arc::new(b);
    assert_eq!(b.count_and_sum().await, expected(1..=100), "B before");

    let is_head = |path: &Path| path.filename() == Some("head");
    let head_polls = |from: usize| {
        replica_requests.log()[from..]
            .iter()
            .filter(|request| request.op == Op::GetConditional && is_head(&request.path))
            .count()
    };
    let from = replica_requests.log().len();
    replica_requests.gate_polls(is_head);

    // The first query's read of the head is held before it is sent.
    let first = tokio::spawn({
        let b = Arc::clone(&b);
        async move { b.count_and_sum().await }
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        replica_requests.wait_gated(1),
    )
    .await
    .expect("a query on B confirms the head with a read sent after it started");

    a.insert(101, 200).await.expect("A writes 101..=200");

    // Sixteen queries start after A's commit, while the first read is held:
    // each runs until it waits for the read in flight.
    let mut later: Vec<_> = (0..16)
        .map(|_| {
            let b = Arc::clone(&b);
            Box::pin(async move { b.count_and_sum().await })
        })
        .collect();
    for query in &mut later {
        assert!(
            futures::poll!(query.as_mut()).is_pending(),
            "a later query waits for the read in flight"
        );
    }

    replica_requests.open_reads();
    assert_eq!(
        first.await.expect("first query"),
        expected(1..=200),
        "the first query"
    );
    for (index, query) in later.into_iter().enumerate() {
        assert_eq!(
            query.await,
            expected(1..=200),
            "later query {index} returns A's commit"
        );
    }
    assert_eq!(
        head_polls(from),
        2,
        "the held read, and one more shared by the sixteen later queries"
    );
}

/// A writer and a replica of an unkeyed table with rows 1..=100, the replica's
/// table built with `reuse`, serving views up to `lag` old (`None`: the
/// default), its requests recorded.
async fn writer_and_replica_with(
    reuse: ScanViewReuse,
    lag: Option<std::time::Duration>,
) -> (
    Instance,
    Instance,
    Arc<CayenneTableProvider>,
    Arc<FaultStore>,
) {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let replica_requests = Arc::new(FaultStore::new(Arc::clone(&objects)));
    let replica_store: Arc<dyn ObjectStore> = Arc::clone(&replica_requests) as Arc<dyn ObjectStore>;

    let (a, data_url) = Instance::open(&objects, Access::Writer).await;
    let table = a
        .builder(&objects, &data_url)
        .create(CreateTableOptions {
            table_name: TABLE.to_string(),
            schema: schema(),
            primary_key: Vec::new(),
            on_conflict: None,
            base_path: data_url.clone(),
            partition_column: None,
            vortex_config: VortexConfig::default(),
        })
        .await
        .expect("create the table on A");
    a.register(table);
    a.insert(1, 100).await.expect("A writes 1..=100");

    let (b, _) = Instance::open(&replica_store, Access::Replica).await;
    let table = Arc::new(
        b.builder(&replica_store, &data_url)
            .with_scan_view_reuse(reuse)
            .open(TABLE)
            .await
            .expect("open the table on B"),
    );
    if let Some(lag) = lag {
        table.set_replica_view_lag(lag);
    }
    b.ctx
        .register_table(TABLE, Arc::clone(&table) as Arc<dyn TableProvider>)
        .expect("register");
    (a, b, table, replica_requests)
}

/// Conditional reads of a dataset head recorded from `from` on.
fn head_polls(requests: &FaultStore, from: usize) -> usize {
    requests.log()[from..]
        .iter()
        .filter(|request| {
            request.op == Op::GetConditional && request.path.filename() == Some("head")
        })
        .count()
}

/// A replica's query serves its cached view for the replica's lag without
/// reading the location, so a commit made meanwhile shows only once the view
/// is older than the lag and a query confirms the head again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replica_serves_its_view_for_its_lag_then_confirms_the_head() {
    let lag = std::time::Duration::from_secs(2);
    let (a, b, _table, requests) =
        writer_and_replica_with(ScanViewReuse::UntilInvalidated, Some(lag)).await;
    let first_confirmed = std::time::Instant::now();
    assert_eq!(
        b.count_and_sum().await,
        expected(1..=100),
        "B's first query"
    );

    a.insert(101, 200).await.expect("A writes 101..=200");
    let from = requests.log().len();
    assert_eq!(
        b.count_and_sum().await,
        expected(1..=100),
        "within its lag, B serves the view it confirmed before A's commit"
    );
    assert_eq!(head_polls(&requests, from), 0, "and reads nothing to do so");

    let deadline = first_confirmed + lag * 5;
    loop {
        let answer = b.count_and_sum().await;
        if answer == expected(1..=200) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "B still serves {answer:?} {:?} after its first confirmation",
            first_confirmed.elapsed()
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        first_confirmed.elapsed() >= lag,
        "B showed A's commit only once its view was older than its lag"
    );
    assert!(
        head_polls(&requests, from) >= 1,
        "a query past the lag confirmed the head"
    );
}

/// While queries use a replica's table, the background refresh confirms the
/// head and rebuilds the view ahead of them, so a query after a commit finds a
/// fresh view and reads nothing itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replica_refreshes_its_view_in_the_background_while_queried() {
    let lag = std::time::Duration::from_secs(6);
    let (a, b, table, requests) =
        writer_and_replica_with(ScanViewReuse::UntilInvalidated, Some(lag)).await;
    table.init_scan_view_cache();
    assert_eq!(
        b.count_and_sum().await,
        expected(1..=100),
        "B's first query"
    );

    let from = requests.log().len();
    a.insert(101, 200).await.expect("A writes 101..=200");
    tokio::time::timeout(lag * 2, async {
        while head_polls(&requests, from) == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the background refresh confirms the head while no query runs");

    // The refresh rebuilt the view after confirming the head; the next round is
    // half a lag away, so this query is served by that view.
    let before = requests.log().len();
    assert_eq!(
        b.count_and_sum().await,
        expected(1..=200),
        "B's next query shows A's commit"
    );
    assert_eq!(head_polls(&requests, before), 0, "and reads nothing itself");
}

/// A replica is strict by default, whatever view lag the table allows its
/// writer: a query serves every commit acknowledged before it started.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replica_serves_every_commit_acknowledged_before_a_query_by_default() {
    for reuse in [
        ScanViewReuse::UntilInvalidated,
        ScanViewReuse::WithinLag(std::time::Duration::from_secs(3_600)),
    ] {
        let (a, b, _table, requests) = writer_and_replica_with(reuse, None).await;
        assert_eq!(
            b.count_and_sum().await,
            expected(1..=100),
            "{reuse:?}: B before"
        );
        a.insert(101, 200).await.expect("A writes 101..=200");
        let from = requests.log().len();
        assert_eq!(
            b.count_and_sum().await,
            expected(1..=200),
            "{reuse:?}: B's next query returns A's commit"
        );
        assert_eq!(
            head_polls(&requests, from),
            1,
            "{reuse:?}: after one read of the head"
        );
    }
}

/// A location's identity is recorded once in the location: instances that
/// reach it by different spellings of its URL read the same one, a replica
/// reads the one its writer recorded, and another location has its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_location_has_one_identity_however_it_is_reached() {
    let open = |store: Arc<dyn ObjectStore>, location: &'static str, access: Access| async move {
        ObjectNativeCatalog::open_or_create_at_with_config(
            store,
            Path::from(crate::common::LOCATION_CATALOG),
            location,
            NativeCatalogConfig {
                access,
                ..NativeCatalogConfig::default()
            },
        )
        .await
        .expect("open the object-native catalog")
    };
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = open(Arc::clone(&objects), LOCATION, Access::Writer).await;
    let replica = open(
        Arc::clone(&objects),
        "s3://bucket/state/cayenne/v1/",
        Access::Replica,
    )
    .await;
    let id = writer.location_id().await.expect("the writer's identity");
    assert!(uuid::Uuid::parse_str(&id).is_ok(), "a UUID: {id}");
    assert_eq!(
        replica.location_id().await.expect("the replica's identity"),
        id,
        "the replica reads the identity its writer recorded"
    );
    assert_eq!(
        writer.location_id().await.expect("asked again"),
        id,
        "recorded once"
    );

    let elsewhere = open(Arc::new(InMemory::new()), LOCATION, Access::Writer).await;
    assert_ne!(
        elsewhere
            .location_id()
            .await
            .expect("another location's identity"),
        id,
        "another location at the same URL has an identity of its own"
    );
}

/// A replica follows both live CDC evolution and registration-time schema
/// publication without reopening, for files and an inline corpus.
#[tokio::test]
async fn replica_adopts_committed_schema() {
    use arrow_tools::schema_evolution::{EvolutionContext, SchemaEvolution, classify};

    for live in [false, true] {
        for inline in [false, true] {
            let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let (a, data_url) = Instance::open(&objects, Access::Writer).await;
            let initial = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("v", DataType::Int32, false),
            ]));
            let mut writer = Arc::new(
                a.builder(&objects, &data_url)
                    .create(CreateTableOptions {
                        table_name: TABLE.to_string(),
                        schema: Arc::clone(&initial),
                        primary_key: Vec::new(),
                        on_conflict: None,
                        base_path: data_url.clone(),
                        partition_column: None,
                        vortex_config: VortexConfig {
                            inline_max_rows: if inline { 1000 } else { 0 },
                            inline_max_bytes: if inline { 1_000_000 } else { 0 },
                            ..VortexConfig::default()
                        },
                    })
                    .await
                    .expect("create writer"),
            );
            a.ctx
                .register_table(TABLE, Arc::clone(&writer) as Arc<dyn TableProvider>)
                .expect("register writer");
            a.run("INSERT INTO orders VALUES (1, 7)")
                .await
                .expect("initial row");
            let (b, _) = Instance::open(&objects, Access::Replica).await;
            let replica = Arc::new(
                b.builder(&objects, &data_url)
                    .open(TABLE)
                    .await
                    .expect("open replica"),
            );
            b.ctx
                .register_table(TABLE, Arc::clone(&replica) as Arc<dyn TableProvider>)
                .expect("register replica");
            assert_eq!(b.count_and_sum().await, (1, 7));

            let evolved = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("v", DataType::Int64, false),
                Field::new("extra", DataType::Utf8, true),
            ]));
            if live {
                let SchemaEvolution::Widening(plan) = classify(
                    &initial,
                    &evolved,
                    &EvolutionContext {
                        constraint_columns: &[],
                    },
                ) else {
                    panic!("the change must widen");
                };
                writer
                    .evolve_schema_live(&plan)
                    .await
                    .expect("live evolution");
            } else {
                a.ctx.deregister_table(TABLE).expect("unregister writer");
                drop(writer);
                let metadata = a.catalog.get_table(TABLE).await.expect("stored writer");
                a.catalog
                    .update_table_schema(&metadata.table_id, &evolved)
                    .await
                    .expect("registration schema publication");
                writer = Arc::new(
                    a.builder(&objects, &data_url)
                        .open(TABLE)
                        .await
                        .expect("open evolved writer"),
                );
                a.ctx
                    .register_table(TABLE, Arc::clone(&writer) as Arc<dyn TableProvider>)
                    .expect("replace writer");
            }
            a.run("INSERT INTO orders VALUES (2, 3000000001, 'wide')")
                .await
                .unwrap_or_else(|error| panic!("wide row (live={live}, inline={inline}): {error}"));
            writer
                .flush_pending_maintenance()
                .await
                .expect("drain writer statistics");
            assert_eq!(
                a.count_and_sum().await,
                (2, 3_000_000_008),
                "live={live}, inline={inline}"
            );

            // This scan confirms the head using a column whose type did not change.
            let ids = b
                .ctx
                .sql("SELECT id FROM orders ORDER BY id")
                .await
                .expect("plan confirmation")
                .collect()
                .await
                .expect("confirm head");
            let ids: Vec<i64> = ids
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("id")
                        .values()
                        .iter()
                        .copied()
                })
                .collect();
            assert_eq!(ids, vec![1, 2], "live={live}, inline={inline}");
            assert_eq!(
                replica
                    .schema()
                    .field_with_name("v")
                    .expect("v")
                    .data_type(),
                &DataType::Int64
            );

            let batches = b
                .ctx
                .sql("SELECT id, v, extra FROM orders ORDER BY id")
                .await
                .expect("plan adopted schema")
                .collect()
                .await
                .expect("read adopted schema");
            let rows: Vec<(i64, i64, Option<String>)> = batches
                .iter()
                .flat_map(|batch| {
                    let id = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("id");
                    let v = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("v");
                    // Query output may use view strings; cast to the stored string representation.
                    let extra =
                        arrow::compute::cast(batch.column(2), &DataType::Utf8).expect("extra Utf8");
                    let extra = extra.as_any().downcast_ref::<StringArray>().expect("extra");
                    (0..batch.num_rows())
                        .map(|i| {
                            (
                                id.value(i),
                                v.value(i),
                                (!extra.is_null(i)).then(|| extra.value(i).to_string()),
                            )
                        })
                        .collect::<Vec<_>>()
                })
                .collect();
            assert_eq!(
                rows,
                vec![(1, 7, None), (2, 3_000_000_001, Some("wide".to_string()))],
                "live={live}, inline={inline}"
            );

            assert_eq!(
                b.count_and_sum().await,
                (2, 3_000_000_008),
                "live={live}, inline={inline}"
            );
            let filtered = b
                .ctx
                .sql("SELECT v FROM orders WHERE extra = 'wide' AND v > 2147483647")
                .await
                .expect("plan inline pruning at adopted width")
                .collect()
                .await
                .expect("execute filtered scan");
            let values: Vec<i64> = filtered
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("v")
                        .values()
                        .iter()
                        .copied()
                })
                .collect();
            assert_eq!(values, vec![3_000_000_001], "live={live}, inline={inline}");
        }
    }
}
