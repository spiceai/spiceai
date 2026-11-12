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

use arrow::array::Int64Array;
use cayenne::metadata::{CreateTableOptions, ObjectStoreConfig, VortexConfig};
use cayenne::metastore::object_native::{Access, NativeCatalogConfig, ObjectNativeCatalog};
use cayenne::{CayenneTableProviderBuilder, MetadataCatalog};
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
            .contains("is written by another Spice instance"),
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
