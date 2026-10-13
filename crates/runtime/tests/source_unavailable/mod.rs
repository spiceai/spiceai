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

//! Datasets whose source is unreachable when the runtime starts.

use std::{
    any::Any,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use app::AppBuilder;
use arrow::{
    array::{Int32Array, RecordBatch},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use async_trait::async_trait;
use data_connector_api::{
    ConnectorComponent, ConnectorContext, ConnectorParams, DataConnector, DataConnectorError,
    DataConnectorFactory, NewDataConnectorResult,
};
use datafusion::datasource::{MemTable, TableProvider};
use object_store::ObjectStoreExt;
use runtime::{Runtime, component::dataset::DatasetSpec, dataconnector, status::ComponentStatus};
use runtime_parameters::ParameterSpec;
use spicepod::component::dataset::Dataset as SpicepodDataset;

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{run_query, wait_until_true},
};

/// A source that fails while it is down the way the `PostgreSQL` and `MySQL`
/// connectors do when their database refuses the connection: with
/// `UnableToConnectInvalidHostOrPort`. By default the connector cannot be built
/// (`create()` fails); with [`Self::refuse_reads_instead_of_connecting`] it builds,
/// and `read_provider` fails instead.
///
/// It can also be slow (building its connector waits `build_delay_ms` first, and every
/// `read_provider` waits `read_delay_ms`), counts every build and read it is asked
/// for, and can report a primary key on `id`, the way `DynamoDB` reports its key
/// schema.
struct UnreachableSource {
    prefix: &'static str,
    up: AtomicBool,
    refuse_reads: AtomicBool,
    connect_attempts: AtomicUsize,
    value: AtomicUsize,
    read_delay_ms: AtomicU64,
    build_delay_ms: AtomicU64,
    builds: AtomicUsize,
    reads: AtomicUsize,
    primary_key: AtomicBool,
    extra_column: AtomicBool,
    reject_credentials: AtomicBool,
    on_trigger: AtomicBool,
    dynamic_reads: AtomicBool,
    fail_scans: AtomicBool,
    failed_scans: AtomicUsize,
}

impl UnreachableSource {
    fn new(prefix: &'static str, value: usize) -> Arc<Self> {
        Arc::new(Self {
            prefix,
            up: AtomicBool::new(false),
            refuse_reads: AtomicBool::new(false),
            connect_attempts: AtomicUsize::new(0),
            value: AtomicUsize::new(value),
            read_delay_ms: AtomicU64::new(0),
            build_delay_ms: AtomicU64::new(0),
            builds: AtomicUsize::new(0),
            reads: AtomicUsize::new(0),
            primary_key: AtomicBool::new(false),
            extra_column: AtomicBool::new(false),
            reject_credentials: AtomicBool::new(false),
            on_trigger: AtomicBool::new(false),
            dynamic_reads: AtomicBool::new(false),
            fail_scans: AtomicBool::new(false),
            failed_scans: AtomicUsize::new(0),
        })
    }

    fn refuse_reads_instead_of_connecting(&self) {
        self.refuse_reads.store(true, Ordering::SeqCst);
    }

    /// Counts an attempt to reach the source, and fails it while the source is down,
    /// or with rejected credentials while it rejects them.
    fn attempt(&self, connector_component: ConnectorComponent) -> Result<(), DataConnectorError> {
        self.connect_attempts.fetch_add(1, Ordering::SeqCst);
        if self.reject_credentials.load(Ordering::SeqCst) {
            return Err(
                DataConnectorError::UnableToConnectInvalidUsernameOrPassword {
                    dataconnector: self.prefix.to_string(),
                    connector_component,
                },
            );
        }
        if self.up.load(Ordering::SeqCst) {
            return Ok(());
        }
        Err(DataConnectorError::UnableToConnectInvalidHostOrPort {
            dataconnector: self.prefix.to_string(),
            connector_component,
            host: "127.0.0.1".to_string(),
            port: "5432".to_string(),
        })
    }

    fn bring_up(&self) {
        self.up.store(true, Ordering::SeqCst);
    }

    fn take_down(&self) {
        self.up.store(false, Ordering::SeqCst);
    }

    fn set_value(&self, value: usize) {
        self.value.store(value, Ordering::SeqCst);
    }

    fn set_read_delay(&self, delay: Duration) {
        self.read_delay_ms.store(
            u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
            Ordering::SeqCst,
        );
    }

    fn set_build_delay(&self, delay: Duration) {
        self.build_delay_ms.store(
            u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
            Ordering::SeqCst,
        );
    }

    /// How many times a connector was built (counted as each build starts).
    fn builds(&self) -> usize {
        self.builds.load(Ordering::SeqCst)
    }

    /// How many times the source was read (counted as each read starts).
    fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }

    /// Rejects every connection with invalid credentials, a configuration error.
    fn reject_credentials(&self) {
        self.reject_credentials.store(true, Ordering::SeqCst);
    }

    /// Adds a column `w` to the source's schema, as a source-side schema change.
    fn add_column(&self) {
        self.extra_column.store(true, Ordering::SeqCst);
    }

    fn report_primary_key(&self) {
        self.primary_key.store(true, Ordering::SeqCst);
    }

    fn connect_attempts(&self) -> usize {
        self.connect_attempts.load(Ordering::SeqCst)
    }

    fn schema(&self) -> SchemaRef {
        let mut fields = vec![
            Field::new("id", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
        ];
        if self.extra_column.load(Ordering::SeqCst) {
            fields.push(Field::new("w", DataType::Int32, false));
        }
        Arc::new(Schema::new(fields))
    }

    /// Three rows, each with the source's current `value` in `v`.
    fn table(&self) -> Result<MemTable, datafusion::error::DataFusionError> {
        let value = i32::try_from(self.value.load(Ordering::SeqCst)).unwrap_or(i32::MAX);
        let schema = self.schema();
        let mut columns: Vec<arrow::array::ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![value; 3])),
        ];
        if self.extra_column.load(Ordering::SeqCst) {
            columns.push(Arc::new(Int32Array::from(vec![0; 3])));
        }
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
        let table = MemTable::try_new(schema, vec![vec![batch]])?;
        Ok(if self.primary_key.load(Ordering::SeqCst) {
            table.with_constraints(datafusion::common::Constraints::new_unverified(vec![
                datafusion::common::Constraint::PrimaryKey(vec![0]),
            ]))
        } else {
            table
        })
    }

    async fn register(self: &Arc<Self>) {
        dataconnector::register_connector_factory(
            self.prefix,
            Arc::new(UnreachableSourceFactory {
                source: Arc::clone(self),
            }),
        )
        .await;
    }
}

#[derive(Debug)]
struct UnreachableSourceConnector {
    source: Arc<UnreachableSource>,
}

#[derive(Debug)]
struct DynamicSourceTable {
    source: Arc<UnreachableSource>,
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for DynamicSourceTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        datafusion::logical_expr::TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        if self.source.fail_scans.load(Ordering::SeqCst) {
            self.source.failed_scans.fetch_add(1, Ordering::SeqCst);
            return Err(datafusion::error::DataFusionError::Execution(
                "injected source scan failure".to_string(),
            ));
        }
        self.source
            .table()?
            .scan(state, projection, filters, limit)
            .await
    }
}

#[async_trait]
impl DataConnector for UnreachableSourceConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn metadata_provider(
        &self,
        dataset: &DatasetSpec,
    ) -> Option<Result<Arc<dyn TableProvider>, DataConnectorError>> {
        if !dataset.has_metadata_table {
            return None;
        }
        Some(
            self.source
                .attempt(ConnectorComponent::from(dataset))
                .and_then(|()| {
                    self.source
                        .table()
                        .map(|table| Arc::new(table) as Arc<dyn TableProvider>)
                        .map_err(|source| DataConnectorError::UnableToGetReadProvider {
                            dataconnector: self.source.prefix.to_string(),
                            connector_component: ConnectorComponent::from(dataset),
                            source: Box::new(source),
                        })
                }),
        )
    }

    fn initialization(&self) -> runtime::component::ComponentInitialization {
        if self.source.on_trigger.load(Ordering::SeqCst) {
            runtime::component::ComponentInitialization::OnTrigger
        } else {
            runtime::component::ComponentInitialization::default()
        }
    }

    async fn read_provider(
        &self,
        _context: &dyn ConnectorContext,
        dataset: &DatasetSpec,
    ) -> Result<Arc<dyn TableProvider>, DataConnectorError> {
        self.source.reads.fetch_add(1, Ordering::SeqCst);
        let delay = self.source.read_delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        if self.source.refuse_reads.load(Ordering::SeqCst) {
            self.source.attempt(ConnectorComponent::from(dataset))?;
        }
        if self.source.dynamic_reads.load(Ordering::SeqCst) {
            return Ok(Arc::new(DynamicSourceTable {
                source: Arc::clone(&self.source),
                schema: self.source.schema(),
            }));
        }
        let table =
            self.source
                .table()
                .map_err(|source| DataConnectorError::UnableToGetReadProvider {
                    dataconnector: self.source.prefix.to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: Box::new(source),
                })?;
        Ok(Arc::new(table))
    }
}

impl std::fmt::Debug for UnreachableSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnreachableSource")
            .field("prefix", &self.prefix)
            .field("up", &self.up.load(Ordering::SeqCst))
            .finish_non_exhaustive()
    }
}

struct UnreachableSourceFactory {
    source: Arc<UnreachableSource>,
}

impl DataConnectorFactory for UnreachableSourceFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create<'a>(
        &'a self,
        params: ConnectorParams,
        _context: &'a dyn ConnectorContext,
    ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send + 'a>> {
        Box::pin(async move {
            self.source.builds.fetch_add(1, Ordering::SeqCst);
            let delay = self.source.build_delay_ms.load(Ordering::SeqCst);
            if delay > 0 {
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
            if !self.source.refuse_reads.load(Ordering::SeqCst) {
                self.source.attempt(params.component.clone())?;
            }
            Ok(Arc::new(UnreachableSourceConnector {
                source: Arc::clone(&self.source),
            }) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        self.source.prefix
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        &[]
    }
}

fn dataset_status(rt: &Runtime, name: &str) -> Option<ComponentStatus> {
    rt.status().get_component_status(&format!("dataset:{name}"))
}

#[derive(Debug)]
struct FlakyObjectStoreConnector {
    attempts: AtomicUsize,
    store_url: url::Url,
}

#[async_trait]
impl DataConnector for FlakyObjectStoreConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        _context: &dyn ConnectorContext,
        _dataset: &DatasetSpec,
    ) -> Result<Arc<dyn TableProvider>, DataConnectorError> {
        Ok(Arc::new(
            MemTable::try_new(Arc::new(Schema::empty()), vec![vec![]])
                .expect("empty source table is valid"),
        ))
    }

    async fn register_object_stores(
        &self,
        dataset: &DatasetSpec,
        runtime_env: &Arc<datafusion::execution::runtime_env::RuntimeEnv>,
    ) -> Result<(), DataConnectorError> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(DataConnectorError::UnableToConnectInvalidHostOrPort {
                dataconnector: "replay-test".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                host: "127.0.0.1".to_string(),
                port: "5432".to_string(),
            });
        }
        runtime_env.register_object_store(
            &self.store_url,
            Arc::new(object_store::memory::InMemory::new()),
        );
        Ok(())
    }
}

#[tokio::test]
async fn reconnecting_object_store_replay_recovers_after_registration_failure()
-> Result<(), anyhow::Error> {
    use runtime::dataconnector::reconnecting::{ConnectorBuilder, ReconnectingConnector};
    let rt = Arc::new(Runtime::builder().build().await);
    let dataset = runtime::component::dataset::builder::DatasetBuilder::try_new(
        "replay-test://orders".into(),
        "orders",
    )?
    .with_app(Arc::new(AppBuilder::new("object_store_replay").build()))
    .with_runtime(Arc::clone(&rt))
    .build()?;
    let source = Arc::new(FlakyObjectStoreConnector {
        attempts: AtomicUsize::new(0),
        store_url: url::Url::parse("replay-test://store/")?,
    });
    let build: ConnectorBuilder = Arc::new({
        let source = Arc::clone(&source);
        move || {
            let source = Arc::clone(&source);
            Box::pin(async move { Ok(source as Arc<dyn DataConnector>) })
        }
    });
    let wrapper = ReconnectingConnector::new("replay-test", build);
    let runtime_env = rt.datafusion().ctx.runtime_env();
    wrapper
        .register_object_stores(&dataset, &runtime_env)
        .await?;
    let context =
        runtime::dataconnector::parameters::RuntimeConnectorContext::for_dataset(&dataset);
    let first = wrapper.read_provider(&context, &dataset).await;
    let second = wrapper.read_provider(&context, &dataset).await;
    let store = runtime_env.object_store(
        datafusion::execution::object_store::ObjectStoreUrl::parse(source.store_url.as_str())?,
    );
    eprintln!(
        "object store replay: first_failed={} second_succeeded={} attempts={} store_registered={}",
        first.is_err(),
        second.is_ok(),
        source.attempts.load(Ordering::SeqCst),
        store.is_ok()
    );
    assert!(
        matches!(
            first,
            Err(DataConnectorError::UnableToConnectInvalidHostOrPort { .. })
        ),
        "the failed replay is propagated to the source retry loop"
    );
    second?;
    let store = store?;
    let path = object_store::path::Path::from("probe");
    store.put(&path, "recovered".into()).await?;
    assert_eq!(
        store.get(&path).await?.bytes().await?.as_ref(),
        b"recovered"
    );
    rt.shutdown().await;
    Ok(())
}

/// Starts a runtime with one dataset on `source` while it is down, and asserts the
/// dataset reports the failure, then loads once the source is reachable.
async fn assert_loads_once_reachable(
    source: &Arc<UnreachableSource>,
    snapshot: &str,
) -> Result<(), anyhow::Error> {
    source.register().await;

    let app = AppBuilder::new("source_unavailable")
        .with_dataset(SpicepodDataset::new(
            format!("{}://orders", source.prefix),
            "orders",
        ))
        .build();
    configure_test_datafusion();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    let loader = tokio::spawn({
        let rt = Arc::clone(&rt);
        async move { rt.load_components().await }
    });

    // While the source is down the dataset reports the failure and cannot be queried.
    let reported = wait_until_true(Duration::from_secs(10), || async {
        matches!(
            dataset_status(&rt, "orders"),
            Some(ComponentStatus::Error(_))
        )
    })
    .await;
    assert!(
        reported,
        "a dataset whose source is down must report an error, got {:?}",
        dataset_status(&rt, "orders")
    );
    assert!(
        run_query(&rt, "SELECT COUNT(*) FROM orders").await.is_err(),
        "a dataset whose source never answered has nothing to serve"
    );

    source.bring_up();

    // Fibonacci backoff retries after 1s, 1s, 2s, 3s, ..., so a few attempts land
    // well inside this bound once the source is up.
    let queryable = wait_until_true(Duration::from_secs(30), || async {
        run_query(&rt, "SELECT COUNT(*) FROM orders").await.is_ok()
    })
    .await;
    assert!(
        queryable,
        "the dataset must load once its source is reachable; status {:?} after {} connection attempts",
        dataset_status(&rt, "orders"),
        source.connect_attempts()
    );
    assert!(
        source.connect_attempts() >= 2,
        "loading after the source came back takes a retry, got {} attempts",
        source.connect_attempts()
    );

    let batches = run_query(&rt, "SELECT SUM(v) AS s, COUNT(*) AS n FROM orders").await?;
    insta::assert_snapshot!(
        snapshot,
        arrow::util::pretty::pretty_format_batches(&batches)?
    );

    loader.abort();
    Ok(())
}

/// Regression test for #14609: a source that is down when the runtime starts used
/// to fail the dataset permanently, so it never loaded even after the source came
/// back. It must keep retrying, and load once the source is reachable.
#[tokio::test]
async fn a_dataset_whose_source_is_down_at_startup_loads_once_the_source_is_reachable()
-> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let source = UnreachableSource::new("unreachable-at-startup", 7);
    assert_loads_once_reachable(&source, "source_down_at_startup_loads_once_reachable").await
}

/// The same failure surfacing from `read_provider` instead: a connector that builds
/// without contacting the source, and is refused when it reads the schema.
#[tokio::test]
async fn a_dataset_whose_source_refuses_reads_at_startup_loads_once_the_source_is_reachable()
-> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let source = UnreachableSource::new("refuses-reads-at-startup", 5);
    source.refuse_reads_instead_of_connecting();
    assert_loads_once_reachable(
        &source,
        "source_refuses_reads_at_startup_loads_once_reachable",
    )
    .await
}

/// Accelerated datasets whose acceleration is already on disk when the runtime
/// starts with the source down or slow (#14610).
#[cfg(feature = "duckdb")]
mod served_from_acceleration {
    use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};

    use app::AppBuilder;
    use arrow::array::Int64Array;
    use arrow_tools::metadata_keys::ACCELERATION_PRIMARY_KEY_METADATA_KEY;
    use runtime::{Runtime, status::ComponentStatus};
    use spicepod::{
        acceleration::{Acceleration, Mode, RefreshMode},
        component::dataset::{Dataset as SpicepodDataset, ReadyState},
        param::Params,
        semantic::Column,
    };
    use tempfile::TempDir;

    use super::{UnreachableSource, dataset_status};
    use crate::{
        configure_test_datafusion, init_tracing,
        utils::{run_query, wait_until_true},
    };

    /// A `DuckDB` file acceleration that refreshes every second, so a refresh is due
    /// as soon as the runtime restarts.
    fn dataset(prefix: &str, duckdb_file: &Path, ready_state: ReadyState) -> SpicepodDataset {
        let mut dataset = SpicepodDataset::new(format!("{prefix}://orders"), "orders");
        dataset.ready_state = ready_state;
        dataset.acceleration = Some(Acceleration {
            enabled: true,
            engine: Some("duckdb".to_string()),
            mode: Mode::File,
            refresh_mode: Some(RefreshMode::Full),
            refresh_check_interval: Some("1s".to_string()),
            params: Some(Params::from_string_map(HashMap::from([(
                "duckdb_file".to_string(),
                duckdb_file.to_string_lossy().to_string(),
            )]))),
            ..Default::default()
        });
        dataset
    }

    /// `dataset` evolving its acceleration with `on_schema_change: append_new_columns`.
    fn appending_new_columns(mut dataset: SpicepodDataset) -> SpicepodDataset {
        dataset.on_schema_change = spicepod::component::dataset::OnSchemaChange::AppendNewColumns;
        dataset
    }

    /// `dataset` refreshed on the cron schedule `cron` instead of an interval.
    fn with_refresh_cron(mut dataset: SpicepodDataset, cron: &str) -> SpicepodDataset {
        if let Some(acceleration) = dataset.acceleration.as_mut() {
            acceleration.refresh_check_interval = None;
            acceleration.refresh_cron = Some(cron.to_string());
        }
        dataset
    }

    /// `orders`'s `last_refresh` and `next_refresh` from `/v1/datasets?status=true`.
    async fn freshness(
        rt: &Arc<Runtime>,
    ) -> (
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
    ) {
        let infos = runtime::dataset_infos_with_status(rt).await;
        let Some(orders) = infos.iter().find(|info| info.name == "orders") else {
            return (None, None);
        };
        let parse = |time: &Option<String>| {
            time.as_deref()
                .and_then(|time| chrono::DateTime::parse_from_rfc3339(time).ok())
                .map(|time| time.with_timezone(&chrono::Utc))
        };
        (parse(&orders.last_refresh), parse(&orders.next_refresh))
    }

    /// `dataset` checking for a due refresh only every `interval`.
    fn with_refresh_check_interval(
        mut dataset: SpicepodDataset,
        interval: &str,
    ) -> SpicepodDataset {
        if let Some(acceleration) = dataset.acceleration.as_mut() {
            acceleration.refresh_check_interval = Some(interval.to_string());
        }
        dataset
    }

    /// `dataset` with every column's type declared, which makes an
    /// `on_registration` dataset eligible for deferred initialization.
    fn with_declared_columns(mut dataset: SpicepodDataset) -> SpicepodDataset {
        dataset.columns = ["id", "v"]
            .into_iter()
            .map(|name| {
                let mut column = Column::new(name);
                column.r#type = Some("int".to_string());
                column
            })
            .collect();
        dataset
    }

    /// A registered test source (three rows, `v = 1`) and the directory its
    /// acceleration lives in.
    struct Fixture {
        source: Arc<UnreachableSource>,
        dir: TempDir,
    }

    impl Fixture {
        async fn new(prefix: &'static str) -> Result<Self, anyhow::Error> {
            let source = UnreachableSource::new(prefix, 1);
            source.register().await;
            Ok(Self {
                source,
                dir: TempDir::new()?,
            })
        }

        fn dataset(&self, ready_state: ReadyState) -> SpicepodDataset {
            dataset(
                self.source.prefix,
                &self.dir.path().join("orders.duckdb"),
                ready_state,
            )
        }
    }

    async fn start(dataset: SpicepodDataset) -> (Arc<Runtime>, tokio::task::JoinHandle<()>) {
        let app = AppBuilder::new("source_unavailable")
            .with_dataset(dataset)
            .build();
        configure_test_datafusion();
        let rt = Arc::new(Runtime::builder().with_app(app).build().await);
        let loader = tokio::spawn({
            let rt = Arc::clone(&rt);
            async move { rt.load_components().await }
        });
        (rt, loader)
    }

    async fn stop(rt: Arc<Runtime>, loader: tokio::task::JoinHandle<()>) {
        rt.shutdown().await;
        loader.abort();
    }

    /// `(SUM(v), COUNT(*))` of `orders`, or `None` when the query fails.
    async fn sum_and_count(rt: &Arc<Runtime>) -> Option<(i64, i64)> {
        query_sum_and_count(rt).await.ok()
    }

    /// `(SUM(v), COUNT(*))` of `orders`, or why the query failed.
    async fn query_sum_and_count(rt: &Arc<Runtime>) -> Result<(i64, i64), String> {
        query_table_sum_and_count(rt, "orders").await
    }

    async fn query_table_sum_and_count(
        rt: &Arc<Runtime>,
        table: &str,
    ) -> Result<(i64, i64), String> {
        let sql = format!("SELECT CAST(SUM(v) AS BIGINT) AS s, COUNT(*) AS n FROM {table}");
        let batches = run_query(rt, &sql).await.map_err(|err| err.to_string())?;
        let batch = batches.first().ok_or("no batches")?;
        let sum = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("SUM(v) is not Int64")?;
        let count = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("COUNT(*) is not Int64")?;
        Ok((sum.value(0), count.value(0)))
    }

    /// Builds the acceleration from the source (three rows, `v = 1`) and stops the
    /// runtime once the load has completed.
    async fn seed(
        source: &Arc<UnreachableSource>,
        dataset: SpicepodDataset,
    ) -> Result<(), anyhow::Error> {
        source.bring_up();
        let (rt, loader) = start(dataset).await;
        let seeded = wait_until_true(Duration::from_secs(30), || async {
            sum_and_count(&rt).await == Some((3, 3))
                && dataset_status(&rt, "orders") == Some(ComponentStatus::Ready)
        })
        .await;
        let observed = sum_and_count(&rt).await;
        stop(rt, loader).await;
        anyhow::ensure!(seeded, "seeding the acceleration, got {observed:?}");
        Ok(())
    }

    /// Restarts with the source down and its data changed to `v = 2`.
    async fn restart_with_source_down(
        source: &Arc<UnreachableSource>,
        dataset: SpicepodDataset,
    ) -> (Arc<Runtime>, tokio::task::JoinHandle<()>) {
        source.take_down();
        source.set_value(2);
        start(dataset).await
    }

    async fn served_from_acceleration(rt: &Arc<Runtime>, within: Duration) -> bool {
        wait_until_true(within, || async { sum_and_count(rt).await == Some((3, 3)) }).await
    }

    /// Whether the dataset reports `Error` while saying it is still served from its
    /// acceleration.
    async fn reports_served_error(rt: &Arc<Runtime>) -> bool {
        wait_until_true(Duration::from_secs(10), || async {
            matches!(
                dataset_status(rt, "orders"),
                Some(ComponentStatus::Error(message))
                    if message
                        .as_deref()
                        .is_some_and(|message| message.contains("Serving data from the existing acceleration"))
            )
        })
        .await
    }

    async fn refreshed_from_source(rt: &Arc<Runtime>) -> bool {
        wait_until_true(Duration::from_secs(30), || async {
            sum_and_count(rt).await == Some((6, 3))
        })
        .await
    }

    /// `ready_state: on_load` with the source down: queries are answered from the
    /// existing acceleration and the runtime reports ready, instead of the dataset
    /// staying unregistered; once the source is back the refresh brings the data up
    /// to date.
    #[tokio::test]
    async fn an_acceleration_is_served_and_ready_while_its_source_is_down()
    -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(Some("integration=debug,info"));
        let fixture = Fixture::new("served-while-down").await?;
        let source = &fixture.source;
        let spec = || fixture.dataset(ReadyState::OnLoad);
        seed(source, spec()).await?;

        let (rt, loader) = restart_with_source_down(source, spec()).await;

        assert!(
            served_from_acceleration(&rt, Duration::from_secs(10)).await,
            "queries must be answered from the existing acceleration, got {:?} ({:?})",
            sum_and_count(&rt).await,
            dataset_status(&rt, "orders")
        );
        assert!(
            rt.status().is_ready(),
            "on_load is ready once the existing acceleration can serve"
        );
        assert!(
            reports_served_error(&rt).await,
            "a source that cannot be reached sets Error, saying the dataset is still served ({:?})",
            dataset_status(&rt, "orders")
        );
        assert!(
            rt.status().is_ready(),
            "a served dataset in Error stays ready"
        );

        source.bring_up();
        assert!(
            refreshed_from_source(&rt).await,
            "the refresh must bring the data up to date once the source is back, got {:?} ({:?})",
            sum_and_count(&rt).await,
            dataset_status(&rt, "orders")
        );
        assert!(
            !matches!(
                dataset_status(&rt, "orders"),
                Some(ComponentStatus::Error(_))
            ),
            "the Error clears once the source is reached ({:?})",
            dataset_status(&rt, "orders")
        );
        stop(rt, loader).await;
        Ok(())
    }

    /// `ready_state: on_schema_resolved` promises the source has been reached, so
    /// the runtime is not ready while it is down — but queries are still answered
    /// from the existing acceleration.
    #[tokio::test]
    async fn on_schema_resolved_serves_the_acceleration_but_waits_for_the_source_to_be_ready()
    -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(Some("integration=debug,info"));
        let fixture = Fixture::new("schema-resolved-while-down").await?;
        let source = &fixture.source;
        let spec = || fixture.dataset(ReadyState::OnSchemaResolved);
        seed(source, spec()).await?;

        let (rt, loader) = restart_with_source_down(source, spec()).await;

        assert!(
            served_from_acceleration(&rt, Duration::from_secs(10)).await,
            "queries must be answered from the existing acceleration, got {:?}",
            sum_and_count(&rt).await
        );
        assert!(
            !rt.status().is_ready(),
            "on_schema_resolved must not report ready before the source is reached ({:?})",
            dataset_status(&rt, "orders")
        );

        source.bring_up();
        let ready =
            wait_until_true(Duration::from_secs(30), || async { rt.status().is_ready() }).await;
        assert!(
            ready,
            "on_schema_resolved is ready once the source is reached ({:?})",
            dataset_status(&rt, "orders")
        );
        assert!(refreshed_from_source(&rt).await);
        stop(rt, loader).await;
        Ok(())
    }

    /// A deferred dataset (`on_registration` with typed `columns:`) still does not
    /// contact its source at startup, and its first query is answered from the
    /// existing acceleration rather than failing on the unreachable source.
    #[tokio::test]
    async fn a_deferred_dataset_does_not_contact_its_source_at_startup_and_serves_its_acceleration()
    -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(Some("integration=debug,info"));
        let fixture = Fixture::new("deferred-while-down").await?;
        let source = &fixture.source;
        let spec = || with_declared_columns(fixture.dataset(ReadyState::OnRegistration));
        // Build the acceleration the way a previous run would have. Seeding with the
        // deferred spec itself would not do: under `on_registration` a query is answered
        // by the source before the acceleration has initial_load_complete.
        seed(source, fixture.dataset(ReadyState::OnLoad)).await?;
        let attempts_before_restart = source.connect_attempts();

        let (rt, loader) = restart_with_source_down(source, spec()).await;
        let registered = wait_until_true(Duration::from_secs(10), || async {
            dataset_status(&rt, "orders") == Some(ComponentStatus::Ready)
        })
        .await;
        assert!(registered, "the deferred dataset registers at startup");
        assert_eq!(
            source.connect_attempts(),
            attempts_before_restart,
            "a deferred dataset must not contact its source at startup"
        );

        assert!(
            served_from_acceleration(&rt, Duration::from_secs(10)).await,
            "the first query must be answered from the existing acceleration, got {:?}",
            query_sum_and_count(&rt).await
        );

        source.bring_up();
        assert!(
            refreshed_from_source(&rt).await,
            "once the source is back the refresh brings the data up to date, got {:?}",
            sum_and_count(&rt).await
        );
        stop(rt, loader).await;
        Ok(())
    }

    /// A source that is reachable but slow to answer does not hold the dataset
    /// unregistered: queries are answered from the existing acceleration at once, the
    /// source is read once in the background, and the data catches up when it answers.
    #[tokio::test]
    async fn a_slow_source_does_not_hold_back_an_acceleration() -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(Some("integration=debug,info"));
        let fixture = Fixture::new("slow-source").await?;
        let source = &fixture.source;
        let spec = || fixture.dataset(ReadyState::OnLoad);
        seed(source, spec()).await?;

        source.set_value(2);
        source.set_read_delay(Duration::from_secs(6));
        let reads_before_restart = source.reads();
        let (rt, loader) = start(spec()).await;

        assert!(
            served_from_acceleration(&rt, Duration::from_secs(5)).await,
            "a slow source must not keep the acceleration from serving, got {:?} ({:?})",
            sum_and_count(&rt).await,
            dataset_status(&rt, "orders")
        );
        assert!(
            refreshed_from_source(&rt).await,
            "the data catches up once the slow source answers, got {:?}",
            sum_and_count(&rt).await
        );
        assert_eq!(
            source.reads() - reads_before_restart,
            1,
            "the source is read once in the background, not repeated"
        );
        stop(rt, loader).await;
        Ok(())
    }

    /// A source that is slow to connect to does not hold the dataset unregistered: it
    /// is served from its existing acceleration at once, and connected to once in the
    /// background.
    #[tokio::test]
    async fn a_slow_connection_does_not_hold_back_an_acceleration() -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(Some("integration=debug,info"));
        let fixture = Fixture::new("slow-connection").await?;
        let source = &fixture.source;
        let spec = || fixture.dataset(ReadyState::OnLoad);
        seed(source, spec()).await?;

        source.set_value(2);
        source.set_build_delay(Duration::from_secs(6));
        let builds_before_restart = source.builds();
        let (rt, loader) = start(spec()).await;

        assert!(
            served_from_acceleration(&rt, Duration::from_secs(5)).await,
            "a slow connection must not keep the acceleration from serving, got {:?} ({:?})",
            sum_and_count(&rt).await,
            dataset_status(&rt, "orders")
        );
        assert!(
            refreshed_from_source(&rt).await,
            "the data catches up once the connection completes, got {:?}",
            sum_and_count(&rt).await
        );
        assert_eq!(
            source.builds() - builds_before_restart,
            1,
            "the source is connected to once in the background, not repeatedly"
        );
        stop(rt, loader).await;
        Ok(())
    }

    /// Under `ready_state: on_schema_resolved` with no refresh due, a source that cannot
    /// be reached sets `Error`, saying the dataset is still served, not `Refreshing`
    /// (nothing is refreshing), and the dataset is `Ready` once its source is reached.
    #[tokio::test]
    async fn on_schema_resolved_reports_error_until_the_source_is_reached()
    -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(Some("integration=debug,info"));
        let fixture = Fixture::new("initializing-while-down").await?;
        let source = &fixture.source;
        // A refresh is checked for only hourly, so none is due after the restart.
        let spec = |ready_state| with_refresh_check_interval(fixture.dataset(ready_state), "1h");
        seed(source, spec(ReadyState::OnLoad)).await?;

        let (rt, loader) =
            restart_with_source_down(source, spec(ReadyState::OnSchemaResolved)).await;
        assert!(
            served_from_acceleration(&rt, Duration::from_secs(10)).await,
            "queries must be answered from the existing acceleration, got {:?}",
            sum_and_count(&rt).await
        );
        assert!(
            reports_served_error(&rt).await,
            "with no refresh due, a source that cannot be reached sets Error, not Refreshing ({:?})",
            dataset_status(&rt, "orders")
        );
        assert!(
            !rt.status().is_ready(),
            "on_schema_resolved waits for the source"
        );

        source.bring_up();
        let ready = wait_until_true(Duration::from_secs(30), || async {
            dataset_status(&rt, "orders") == Some(ComponentStatus::Ready)
        })
        .await;
        assert!(
            ready,
            "the dataset is ready once its source is reached ({:?})",
            dataset_status(&rt, "orders")
        );
        stop(rt, loader).await;
        Ok(())
    }

    /// A source reached with a different schema has been reached: under
    /// `ready_state: on_schema_resolved` the dataset is ready, serving the
    /// acceleration's schema, rather than waiting for a source schema that matches.
    #[tokio::test]
    async fn on_schema_resolved_is_ready_when_the_reached_source_schema_changed()
    -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(Some("integration=debug,info"));
        let fixture = Fixture::new("schema-changed").await?;
        let source = &fixture.source;
        seed(source, fixture.dataset(ReadyState::OnLoad)).await?;

        source.add_column();
        let (rt, loader) = start(fixture.dataset(ReadyState::OnSchemaResolved)).await;
        let ready =
            wait_until_true(Duration::from_secs(10), || async { rt.status().is_ready() }).await;
        assert!(
            ready,
            "a reached source with a changed schema must not hold on_schema_resolved unready ({:?})",
            dataset_status(&rt, "orders")
        );
        assert!(
            served_from_acceleration(&rt, Duration::from_secs(10)).await,
            "the acceleration keeps serving its schema, got {:?}",
            sum_and_count(&rt).await
        );
        stop(rt, loader).await;
        Ok(())
    }

    /// A source that rejects the dataset's credentials while the dataset is served from
    /// its acceleration is a configuration error no retry clears: the dataset reports
    /// `Error`, rather than staying silently served, while queries are still served
    /// and the runtime stays ready.
    #[tokio::test]
    async fn rejected_credentials_while_served_set_the_dataset_status_to_error()
    -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(Some("integration=debug,info"));
        let fixture = Fixture::new("rejected-credentials").await?;
        let source = &fixture.source;
        seed(source, fixture.dataset(ReadyState::OnLoad)).await?;

        source.reject_credentials();
        let (rt, loader) = start(with_declared_columns(
            fixture.dataset(ReadyState::OnRegistration),
        ))
        .await;
        assert!(
            served_from_acceleration(&rt, Duration::from_secs(10)).await,
            "the acceleration is served despite the rejected credentials, got {:?}",
            sum_and_count(&rt).await
        );
        let reported = wait_until_true(Duration::from_secs(10), || async {
            matches!(
                dataset_status(&rt, "orders"),
                Some(ComponentStatus::Error(_))
            )
        })
        .await;
        assert!(
            reported,
            "rejected credentials must set the dataset's status to Error ({:?})",
            dataset_status(&rt, "orders")
        );
        assert!(
            rt.status().is_ready(),
            "a served dataset in Error is still ready"
        );
        stop(rt, loader).await;
        Ok(())
    }

    /// A configuration error no retry can clear (here a misspelled connector, after
    /// the acceleration was built) is reported as one while the acceleration is
    /// served, rather than as an unreachable source that is being retried.
    #[tokio::test]
    async fn a_configuration_error_while_served_is_reported_as_one() -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(Some("integration=debug,info"));
        let fixture = Fixture::new("configuration-error").await?;
        seed(&fixture.source, fixture.dataset(ReadyState::OnLoad)).await?;

        let mut misspelled = fixture.dataset(ReadyState::OnLoad);
        misspelled.from = "no_such_connector:orders".to_string();
        let (rt, loader) = start(misspelled).await;
        assert!(
            served_from_acceleration(&rt, Duration::from_secs(10)).await,
            "the acceleration is served despite the configuration error, got {:?}",
            sum_and_count(&rt).await
        );
        let reported = wait_until_true(Duration::from_secs(10), || async {
            matches!(
                dataset_status(&rt, "orders"),
                Some(ComponentStatus::Error(Some(message)))
                    if message.contains("cannot connect to its source because of its configuration")
            )
        })
        .await;
        assert!(
            reported,
            "the status reports a configuration error, not a source being retried ({:?})",
            dataset_status(&rt, "orders")
        );
        stop(rt, loader).await;
        Ok(())
    }

    /// A dataset whose configuration needs its source to start (here
    /// `on_schema_change: append_new_columns`, which needs the live schema) waits for
    /// it even with an acceleration on disk, and its status says it is not served and
    /// why.
    #[tokio::test]
    async fn an_excluded_dataset_says_it_is_not_served_and_why() -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(Some("integration=debug,info"));
        let fixture = Fixture::new("excluded-append").await?;
        let source = &fixture.source;
        let spec = || appending_new_columns(fixture.dataset(ReadyState::OnLoad));
        seed(source, spec()).await?;

        let (rt, loader) = restart_with_source_down(source, spec()).await;
        let explained = wait_until_true(Duration::from_secs(10), || async {
            matches!(
                dataset_status(&rt, "orders"),
                Some(ComponentStatus::Error(Some(message)))
                    if message.starts_with("Not served: waits for its source because it uses `on_schema_change: append_new_columns`.")
            )
        })
        .await;
        assert!(
            explained,
            "the status says the dataset is not served and why ({:?})",
            dataset_status(&rt, "orders")
        );
        assert!(
            sum_and_count(&rt).await.is_none(),
            "an excluded dataset is not served while its source is down"
        );
        stop(rt, loader).await;
        Ok(())
    }

    /// The acceleration's last refresh, and when its next scheduled refresh is due, are
    /// reported from startup, read from its checkpoint, while the source is down.
    #[tokio::test]
    async fn freshness_is_reported_from_startup_while_the_source_is_down()
    -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(Some("integration=debug,info"));
        let fixture = Fixture::new("freshness-from-startup").await?;
        let source = &fixture.source;
        let spec = || with_refresh_check_interval(fixture.dataset(ReadyState::OnLoad), "1h");
        seed(source, spec()).await?;

        let restarted_at = chrono::Utc::now();
        let (rt, loader) = restart_with_source_down(source, spec()).await;
        assert!(served_from_acceleration(&rt, Duration::from_secs(10)).await);
        let reported = wait_until_true(Duration::from_secs(10), || async {
            matches!(freshness(&rt).await, (Some(_), Some(_)))
        })
        .await;
        assert!(
            reported,
            "last_refresh and next_refresh are reported from startup"
        );
        let (Some(last_refresh), Some(next_refresh)) = freshness(&rt).await else {
            anyhow::bail!("freshness disappeared");
        };
        assert!(
            last_refresh <= restarted_at,
            "last_refresh comes from the checkpoint written before the restart ({last_refresh} > {restarted_at})"
        );
        let due_after = (next_refresh - last_refresh).num_seconds();
        assert!(
            (3590..=3610).contains(&due_after),
            "next_refresh is refresh_check_interval after last_refresh, got {due_after}s"
        );
        stop(rt, loader).await;
        Ok(())
    }

    /// A dataset refreshed on a cron schedule reports the next future cron time.
    #[tokio::test]
    async fn a_cron_scheduled_dataset_reports_its_next_cron_time() -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(Some("integration=debug,info"));
        let fixture = Fixture::new("freshness-cron").await?;
        let source = &fixture.source;
        // Midnight on January 1st: never due during the test.
        let spec = || with_refresh_cron(fixture.dataset(ReadyState::OnLoad), "0 0 1 1 *");
        seed(source, spec()).await?;

        let (rt, loader) = restart_with_source_down(source, spec()).await;
        assert!(served_from_acceleration(&rt, Duration::from_secs(10)).await);
        let (Some(last_refresh), Some(next_refresh)) = freshness(&rt).await else {
            anyhow::bail!("a cron-scheduled dataset reports last_refresh and next_refresh");
        };
        let next_local = next_refresh.with_timezone(&chrono::Local);
        assert!(
            next_refresh > last_refresh,
            "the next cron time follows the last refresh"
        );
        assert_eq!(
            (
                chrono::Datelike::month(&next_local),
                chrono::Datelike::day(&next_local),
                chrono::Timelike::hour(&next_local)
            ),
            (1, 1, 0),
            "next_refresh is the cron's next time, got {next_local}"
        );
        stop(rt, loader).await;
        Ok(())
    }

    /// Missed cron occurrences are skipped unless a refresh has already been triggered.
    #[tokio::test]
    async fn cron_freshness_skips_missed_occurrences_and_retains_pending_refresh()
    -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(Some("integration=debug,info"));
        let fixture = Fixture::new("freshness-missed-cron").await?;
        let source = &fixture.source;
        let spec = || with_refresh_cron(fixture.dataset(ReadyState::OnLoad), "0 0 1 1 *");
        seed(source, spec()).await?;

        let (rt, loader) = restart_with_source_down(source, spec()).await;
        assert!(served_from_acceleration(&rt, Duration::from_secs(10)).await);
        let name = datafusion::common::TableReference::bare("orders");
        let now = std::time::SystemTime::now();
        let old_refresh = now - Duration::from_hours(9600);
        rt.status().record_dataset_last_refresh(&name, old_refresh);
        rt.status().clear_dataset_next_refresh(&name);
        let (Some(last_refresh), Some(next_refresh)) = freshness(&rt).await else {
            anyhow::bail!("the cron schedule reports freshness");
        };
        let expected = scheduler::channel::cron::next_cron_time("0 0 1 1 *", now)?;
        let expected: chrono::DateTime<chrono::Utc> = expected.into();
        eprintln!(
            "old_last_refresh={last_refresh} api_next={next_refresh} scheduler_next={expected}"
        );
        assert_eq!(
            next_refresh, expected,
            "missed cron occurrences are skipped"
        );

        let pending = now - Duration::from_secs(60);
        rt.status().record_dataset_next_refresh(&name, pending);
        let (_, Some(next_refresh)) = freshness(&rt).await else {
            anyhow::bail!("the pending refresh retains its due time");
        };
        let pending: chrono::DateTime<chrono::Utc> = pending.into();
        assert_eq!(next_refresh.timestamp(), pending.timestamp());
        rt.status().clear_dataset_next_refresh(&name);
        assert_eq!(freshness(&rt).await.1, Some(expected));
        stop(rt, loader).await;
        Ok(())
    }

    /// A cron-triggered refresh retains its due time while waiting for the source
    /// when jitter is disabled.
    #[tokio::test]
    async fn pending_cron_refresh_retains_its_due_time_without_jitter() -> Result<(), anyhow::Error>
    {
        assert_pending_cron_deadline("cron-no-jitter", false).await
    }

    /// A manual refresh replaces the interval timer's due time while its source is down.
    #[tokio::test]
    async fn manual_refresh_replaces_the_cancelled_interval_deadline() -> Result<(), anyhow::Error>
    {
        let _tracing = init_tracing(Some("integration=debug,runtime_table=debug,info"));
        let fixture = Fixture::new("manual-interval-deadline").await?;
        let source = &fixture.source;
        let spec = || {
            let mut dataset =
                with_refresh_check_interval(fixture.dataset(ReadyState::OnLoad), "1h");
            if let Some(acceleration) = dataset.acceleration.as_mut() {
                acceleration.refresh_jitter_enabled = false;
            }
            dataset
        };
        seed(source, spec()).await?;
        let (rt, loader) = restart_with_source_down(source, spec()).await;
        assert!(served_from_acceleration(&rt, Duration::from_secs(10)).await);
        let name = datafusion::common::TableReference::bare("orders");
        assert!(
            wait_until_true(Duration::from_secs(5), || async {
                freshness(&rt)
                    .await
                    .1
                    .is_some_and(|due| due > chrono::Utc::now())
            })
            .await,
            "the interval timer is scheduled before the manual request"
        );
        let scheduled = freshness(&rt)
            .await
            .1
            .ok_or_else(|| anyhow::anyhow!("the interval's initial deadline is present"))?;
        rt.datafusion().refresh_table(&name, None).await?;
        let manual_pending = wait_until_true(Duration::from_secs(3), || async {
            freshness(&rt)
                .await
                .1
                .is_some_and(|due| due <= chrono::Utc::now())
        })
        .await;
        let pending_due = freshness(&rt).await.1;
        eprintln!(
            "manual refresh pending: cancelled_interval={scheduled} now={} api_due={pending_due:?}",
            chrono::Utc::now()
        );
        source.bring_up();
        let refreshed = refreshed_from_source(&rt).await;
        let rescheduled = wait_until_true(Duration::from_secs(5), || async {
            freshness(&rt)
                .await
                .1
                .is_some_and(|due| due > chrono::Utc::now() + chrono::Duration::minutes(59))
        })
        .await;
        eprintln!(
            "manual refresh completed: refreshed={refreshed} api_due={:?} rows={:?}",
            freshness(&rt).await.1,
            sum_and_count(&rt).await
        );
        stop(rt, loader).await;
        assert!(
            manual_pending,
            "the manual request replaces the cancelled interval deadline"
        );
        assert!(
            refreshed,
            "the pending manual refresh completes after source recovery"
        );
        assert!(
            rescheduled,
            "completion schedules the next interval deadline"
        );
        Ok(())
    }

    #[tokio::test]
    async fn failed_cron_refresh_retains_its_due_time() -> Result<(), anyhow::Error> {
        let fixture = Fixture::new("failed-cron-deadline").await?;
        let source = &fixture.source;
        source
            .dynamic_reads
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let spec = || {
            let mut dataset =
                with_refresh_cron(fixture.dataset(ReadyState::OnLoad), "*/10 * * * * *");
            if let Some(acceleration) = dataset.acceleration.as_mut() {
                acceleration.refresh_jitter_enabled = false;
                acceleration.refresh_retry_enabled = false;
            }
            dataset
        };
        seed(source, spec()).await?;
        source.set_value(2);
        source
            .fail_scans
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let (rt, loader) = start(spec()).await;
        let failed = wait_until_true(Duration::from_secs(15), || async {
            source
                .failed_scans
                .load(std::sync::atomic::Ordering::SeqCst)
                > 0
        })
        .await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        let due = freshness(&rt).await.1;
        let retained = due.is_some_and(|due| due <= chrono::Utc::now());
        eprintln!(
            "failed cron: observed={failed} api_due={due:?} now={} rows={:?}",
            chrono::Utc::now(),
            sum_and_count(&rt).await
        );
        source
            .fail_scans
            .store(false, std::sync::atomic::Ordering::SeqCst);
        let recovered = refreshed_from_source(&rt).await;
        eprintln!(
            "cron recovered: refreshed={recovered} rows={:?}",
            sum_and_count(&rt).await
        );
        stop(rt, loader).await;
        assert!(failed, "the injected scan failure reaches the refresh task");
        assert!(
            retained,
            "an unsuccessful cron refresh retains its due time"
        );
        assert!(recovered, "the next cron occurrence recovers the refresh");
        Ok(())
    }

    #[tokio::test]
    async fn synchronized_child_uses_parent_refresh_deadline() -> Result<(), anyhow::Error> {
        assert_synchronized_deadline("sync-child-deadline", false, Some("10m")).await
    }

    #[tokio::test]
    async fn synchronized_child_uses_parent_cron_schedule() -> Result<(), anyhow::Error> {
        assert_synchronized_deadline("sync-child-cron", true, Some("10m")).await
    }

    #[tokio::test]
    async fn synchronized_child_without_own_schedule_uses_parent_deadline()
    -> Result<(), anyhow::Error> {
        assert_synchronized_deadline("sync-child-no-schedule", false, None).await
    }

    async fn assert_synchronized_deadline(
        prefix: &'static str,
        parent_cron: bool,
        child_interval: Option<&str>,
    ) -> Result<(), anyhow::Error> {
        let fixture = Fixture::new(prefix).await?;
        let source = &fixture.source;
        source
            .dynamic_reads
            .store(true, std::sync::atomic::Ordering::SeqCst);
        source.bring_up();
        let mut parent = if parent_cron {
            with_refresh_cron(fixture.dataset(ReadyState::OnLoad), "0 0 1 1 *")
        } else {
            with_refresh_check_interval(fixture.dataset(ReadyState::OnLoad), "1h")
        };
        if let Some(acceleration) = parent.acceleration.as_mut() {
            acceleration.refresh_jitter_enabled = false;
        }
        let mut child = SpicepodDataset::new("localpod:orders", "child");
        child.acceleration = Some(Acceleration {
            enabled: true,
            engine: Some("arrow".to_string()),
            refresh_mode: Some(RefreshMode::Full),
            refresh_check_interval: child_interval.map(str::to_string),
            refresh_jitter_enabled: false,
            ..Acceleration::default()
        });
        configure_test_datafusion();
        let app = AppBuilder::new("sync_child_deadline")
            .with_dataset(parent)
            .with_dataset(child)
            .build();
        let rt = Arc::new(Runtime::builder().with_app(app).build().await);
        let loader = tokio::spawn({
            let rt = Arc::clone(&rt);
            async move { rt.load_components().await }
        });
        let initial_load_complete = wait_until_true(Duration::from_secs(30), || async {
            query_table_sum_and_count(&rt, "child").await.ok() == Some((3, 3))
                && dataset_status(&rt, "child") == Some(ComponentStatus::Ready)
        })
        .await;
        anyhow::ensure!(
            initial_load_complete,
            "the synchronized child initially loads"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        source.set_value(2);
        rt.datafusion()
            .refresh_table(&datafusion::common::TableReference::bare("orders"), None)
            .await?;
        let synchronized = wait_until_true(Duration::from_secs(30), || async {
            query_table_sum_and_count(&rt, "child").await.ok() == Some((6, 3))
                && freshness(&rt)
                    .await
                    .1
                    .is_some_and(|due| due > chrono::Utc::now())
        })
        .await;
        let infos = runtime::dataset_infos_with_status(&rt).await;
        let parent_due = infos
            .iter()
            .find(|info| info.name == "orders")
            .and_then(|info| info.next_refresh.clone());
        let child_due = infos
            .iter()
            .find(|info| info.name == "child")
            .and_then(|info| info.next_refresh.clone());
        eprintln!(
            "synchronized deadlines: synchronized={synchronized} parent={parent_due:?} child={child_due:?} child_rows={:?}",
            query_table_sum_and_count(&rt, "child").await.ok()
        );
        stop(rt, loader).await;
        assert!(synchronized, "the parent refresh updates the child rows");
        assert!(parent_due.is_some(), "the parent interval is scheduled");
        assert_eq!(
            child_due, parent_due,
            "the child follows its actual parent scheduler"
        );
        Ok(())
    }

    #[tokio::test]
    async fn subsecond_refresh_times_preserve_the_recorded_precision() -> Result<(), anyhow::Error>
    {
        let fixture = Fixture::new("subsecond-freshness-precision").await?;
        fixture.source.bring_up();
        let mut dataset = with_refresh_check_interval(fixture.dataset(ReadyState::OnLoad), "100ms");
        if let Some(acceleration) = dataset.acceleration.as_mut() {
            acceleration.refresh_jitter_enabled = false;
        }
        let (rt, loader) = start(dataset).await;
        let name = datafusion::common::TableReference::bare("orders");
        let initial_load_complete = wait_until_true(Duration::from_secs(30), || async {
            sum_and_count(&rt).await == Some((3, 3))
                && rt.status().dataset_freshness(&name).last_refresh.is_some()
        })
        .await;
        let mut observation = None;
        for _ in 0..100 {
            let before = rt.status().dataset_freshness(&name);
            let api = freshness(&rt).await;
            let after = rt.status().dataset_freshness(&name);
            if before == after
                && before.last_refresh.is_some()
                && before
                    .next_refresh
                    .is_some_and(|due| due > std::time::SystemTime::now())
            {
                observation = Some((before, api));
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let rows = sum_and_count(&rt).await;
        stop(rt, loader).await;
        assert!(
            initial_load_complete,
            "the 100 ms schedule refreshes a real acceleration"
        );
        let (recorded, api) = observation
            .ok_or_else(|| anyhow::anyhow!("a stable scheduled freshness snapshot is available"))?;
        let expected = (
            recorded
                .last_refresh
                .map(chrono::DateTime::<chrono::Utc>::from),
            recorded
                .next_refresh
                .map(chrono::DateTime::<chrono::Utc>::from),
        );
        eprintln!("subsecond freshness: recorded={expected:?} api={api:?} rows={rows:?}");
        assert_eq!(
            api, expected,
            "the API preserves actual completion and scheduled deadline precision"
        );
        assert_eq!(rows, Some((3, 3)));
        Ok(())
    }

    #[tokio::test]
    #[ignore = "Run explicitly to collect freshness API latency samples"]
    async fn dataset_freshness_lookup_scaling() -> Result<(), anyhow::Error> {
        let fixture = Fixture::new("freshness-lookup-scaling").await?;
        for count in [1_000, 10_000] {
            let mut app = AppBuilder::new("freshness_lookup_scaling");
            for index in 0..count {
                let mut dataset = SpicepodDataset::new(
                    format!("{}://orders", fixture.source.prefix),
                    format!("orders_{index}"),
                );
                dataset.acceleration = Some(Acceleration {
                    enabled: true,
                    engine: Some("arrow".to_string()),
                    refresh_check_interval: Some("1h".to_string()),
                    ..Acceleration::default()
                });
                app = app.with_dataset(dataset);
            }
            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app.build()).build().await);
            let due = std::time::SystemTime::now() + Duration::from_secs(3600);
            let mut expected = HashMap::with_capacity(count);
            for index in 0..count {
                let name = datafusion::common::TableReference::bare(format!("orders_{index}"));
                let next = due + Duration::from_secs(u64::try_from(index)?);
                rt.status().record_dataset_next_refresh(&name, next);
                expected.insert(
                    name.to_quoted_string(),
                    chrono::DateTime::<chrono::Utc>::from(next)
                        .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
                );
            }
            // Exercise the same response builder as `/v1/datasets?status=true`.
            // No providers need loading to list configured datasets and freshness.
            let warmup = runtime::dataset_infos_with_status(&rt).await;
            assert_eq!(warmup.len(), count);
            assert!(
                warmup
                    .iter()
                    .all(|item| item.next_refresh.as_ref() == expected.get(&item.name))
            );
            let mut samples = Vec::with_capacity(200);
            for _ in 0..200 {
                let started = std::time::Instant::now();
                let infos = runtime::dataset_infos_with_status(&rt).await;
                samples.push(started.elapsed());
                assert_eq!(infos.len(), count);
                assert!(
                    infos
                        .iter()
                        .all(|item| item.next_refresh.as_ref() == expected.get(&item.name))
                );
            }
            let artifact =
                std::env::temp_dir().join(format!("spice-freshness-lookup-{count}.json"));
            std::fs::write(
                &artifact,
                serde_json::to_vec_pretty(&serde_json::json!({
                    "datasets": count,
                    "samples_us": samples.iter().map(std::time::Duration::as_micros).collect::<Vec<_>>(),
                    "response": &warmup,
                    "expected_next_refresh": &expected,
                }))?,
            )?;
            eprintln!("freshness lookup artifact: {}", artifact.display());
            samples.sort_unstable();
            let p99 = samples[197];
            eprintln!(
                "freshness lookup benchmark: datasets={count} samples={} p99_us={} rows={}",
                samples.len(),
                p99.as_micros(),
                warmup.len()
            );
            rt.shutdown().await;
        }
        Ok(())
    }

    #[tokio::test]
    async fn on_trigger_source_refreshes_existing_acceleration_after_recovery()
    -> Result<(), anyhow::Error> {
        let fixture = Fixture::new("on-trigger-source-recovery").await?;
        let source = &fixture.source;
        let dataset = fixture.dataset(ReadyState::OnSchemaResolved);
        seed(source, dataset.clone()).await?;
        source.refuse_reads_instead_of_connecting();
        source
            .on_trigger
            .store(true, std::sync::atomic::Ordering::SeqCst);
        source.reads.store(0, std::sync::atomic::Ordering::SeqCst);
        let (rt, loader) = restart_with_source_down(source, dataset).await;
        let served = served_from_acceleration(&rt, Duration::from_secs(10)).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        let ready_while_source_down = rt.status().is_ready();
        let source_read_count = source.reads();
        source.bring_up();
        let refreshed = refreshed_from_source(&rt).await;
        let rows = sum_and_count(&rt).await;
        eprintln!(
            "on-trigger recovery: served={served} ready_while_source_down={ready_while_source_down} source_reads={source_read_count} refreshed={refreshed} rows={rows:?}"
        );
        stop(rt, loader).await;
        assert!(
            served,
            "the existing acceleration serves during deferred authentication"
        );
        assert!(
            !ready_while_source_down,
            "schema-resolved readiness waits for the real source"
        );
        assert!(
            refreshed,
            "source recovery refreshes through the on-trigger connector"
        );
        assert_eq!(rows, Some((6, 3)));
        Ok(())
    }

    #[tokio::test]
    async fn metadata_enabled_acceleration_serves_while_source_is_down() -> Result<(), anyhow::Error>
    {
        let _tracing = init_tracing(Some("integration=debug,info"));
        let fixture = Fixture::new("metadata-source-down").await?;
        let source = &fixture.source;
        let mut dataset = fixture.dataset(ReadyState::OnLoad);
        dataset.has_metadata_table = Some(true);
        seed(source, dataset.clone()).await?;
        let (rt, loader) = restart_with_source_down(source, dataset).await;
        let served = served_from_acceleration(&rt, Duration::from_secs(10)).await;
        let ready = rt.status().is_ready();
        eprintln!(
            "metadata source down: served={served} ready={ready} rows={:?} status={:?} loader_finished={}",
            sum_and_count(&rt).await,
            dataset_status(&rt, "orders"),
            loader.is_finished()
        );
        source.bring_up();
        let refreshed = refreshed_from_source(&rt).await;
        let metadata_registered = wait_until_true(Duration::from_secs(10), || async {
            rt.datafusion()
                .get_table(&datafusion::common::TableReference::partial(
                    "metadata", "orders",
                ))
                .await
                .is_some()
        })
        .await;
        eprintln!(
            "metadata source recovered: refreshed={refreshed} metadata_registered={metadata_registered} rows={:?} status={:?}",
            sum_and_count(&rt).await,
            dataset_status(&rt, "orders")
        );
        stop(rt, loader).await;
        assert!(
            served,
            "existing acceleration serves without its metadata source"
        );
        assert!(refreshed, "source recovery refreshes the acceleration");
        assert!(
            ready,
            "serving the existing acceleration satisfies on_load readiness"
        );
        assert!(
            metadata_registered,
            "metadata registers after source recovery"
        );
        Ok(())
    }

    /// A sampled zero jitter also retains the triggered refresh's due time.
    #[tokio::test]
    async fn pending_cron_refresh_retains_its_due_time_with_zero_jitter()
    -> Result<(), anyhow::Error> {
        assert_pending_cron_deadline("cron-zero-jitter", true).await
    }

    async fn assert_pending_cron_deadline(
        prefix: &'static str,
        jitter_enabled: bool,
    ) -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(Some(
            "integration=debug,scheduler=debug,runtime_table=debug,info",
        ));
        let fixture = Fixture::new(prefix).await?;
        let source = &fixture.source;
        let spec = || {
            let mut dataset =
                with_refresh_cron(fixture.dataset(ReadyState::OnLoad), "*/2 * * * * *");
            if let Some(acceleration) = dataset.acceleration.as_mut() {
                acceleration.refresh_jitter_enabled = jitter_enabled;
                acceleration.refresh_jitter_max = Some("0s".to_string());
            }
            dataset
        };
        seed(source, spec()).await?;
        let (rt, loader) = restart_with_source_down(source, spec()).await;
        assert!(served_from_acceleration(&rt, Duration::from_secs(10)).await);
        let name = datafusion::common::TableReference::bare("orders");
        let pending = wait_until_true(Duration::from_secs(8), || async {
            rt.status()
                .dataset_freshness(&name)
                .next_refresh
                .is_some_and(|due| due <= std::time::SystemTime::now())
        })
        .await;
        let (_, due) = freshness(&rt).await;
        eprintln!(
            "jitter_enabled={jitter_enabled} now={} recorded_due={:?} api_due={due:?}",
            chrono::Utc::now(),
            rt.status().dataset_freshness(&name).next_refresh
        );
        assert!(
            pending,
            "a pending cron refresh retains its due time with jitter_enabled={jitter_enabled}"
        );
        let due = due.ok_or_else(|| anyhow::anyhow!("the pending cron deadline is present"))?;
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(
            freshness(&rt).await.1,
            Some(due),
            "the pending deadline is retained across another cron occurrence"
        );
        source.bring_up();
        assert!(
            refreshed_from_source(&rt).await,
            "the pending refresh completes after source recovery"
        );
        assert!(
            wait_until_true(Duration::from_secs(5), || async {
                freshness(&rt).await.1.is_some_and(|next| next > due)
            })
            .await,
            "completion releases the pending deadline"
        );
        stop(rt, loader).await;
        Ok(())
    }

    /// A refresh started while the source cannot be reached waits for the source before
    /// it runs, so the dataset keeps reporting `Error` rather than flipping to
    /// `Refreshing` for as long as the source stays down.
    #[tokio::test]
    async fn a_refresh_waiting_for_an_unreachable_source_does_not_report_refreshing()
    -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(Some("integration=debug,info"));
        let fixture = Fixture::new("refresh-while-down").await?;
        let source = &fixture.source;
        // No refresh is due after the restart; the test starts one itself.
        let spec = || with_refresh_check_interval(fixture.dataset(ReadyState::OnLoad), "1h");
        seed(source, spec()).await?;

        let (rt, loader) = restart_with_source_down(source, spec()).await;
        assert!(served_from_acceleration(&rt, Duration::from_secs(10)).await);
        assert!(
            reports_served_error(&rt).await,
            "the unreachable source sets Error ({:?})",
            dataset_status(&rt, "orders")
        );

        rt.datafusion()
            .refresh_table(&datafusion::common::TableReference::bare("orders"), None)
            .await
            .map_err(|err| anyhow::anyhow!("the refresh request to be accepted: {err}"))?;
        // Sampled over time on purpose: the defect is a status that flips after the
        // refresh starts.
        for _ in 0..30 {
            assert!(
                !matches!(
                    dataset_status(&rt, "orders"),
                    Some(ComponentStatus::Refreshing)
                ),
                "a refresh waiting for an unreachable source does not report Refreshing"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        stop(rt, loader).await;
        Ok(())
    }

    /// A source that reports a primary key creates the acceleration's table with
    /// it. Registering while the source is down must recover that key from the
    /// checkpoint: an accelerator built without it fails every refresh after the
    /// source returns with "Primary keys do not match", and the data stays stale.
    #[tokio::test]
    async fn a_keyed_acceleration_refreshes_after_its_source_returns() -> Result<(), anyhow::Error>
    {
        let _tracing = init_tracing(Some("integration=debug,info"));
        let fixture = Fixture::new("keyed-while-down").await?;
        fixture.source.report_primary_key();
        let source = &fixture.source;
        let spec = || fixture.dataset(ReadyState::OnLoad);
        seed(source, spec()).await?;

        let (rt, loader) = restart_with_source_down(source, spec()).await;
        assert!(served_from_acceleration(&rt, Duration::from_secs(10)).await);

        source.bring_up();
        assert!(
            refreshed_from_source(&rt).await,
            "a keyed acceleration must refresh once its source is back, got {:?} ({:?})",
            sum_and_count(&rt).await,
            dataset_status(&rt, "orders")
        );
        // A refresh is due every second, so the status alternates with `Refreshing`;
        // a key mismatch would leave it in `Error` instead.
        let ready = wait_until_true(Duration::from_secs(10), || async {
            dataset_status(&rt, "orders") == Some(ComponentStatus::Ready)
        })
        .await;
        assert!(
            ready,
            "a keyed acceleration stays ready after refreshing ({:?})",
            dataset_status(&rt, "orders")
        );
        stop(rt, loader).await;
        Ok(())
    }

    async fn open_checkpoint(
        rt: &Arc<Runtime>,
        spec: SpicepodDataset,
    ) -> Result<Arc<dyn runtime_acceleration::dataset_checkpoint::DatasetCheckpointer>, anyhow::Error>
    {
        let app_ref = rt.app();
        let app = app_ref.read().await;
        let app = app.as_ref().expect("runtime has its configured app");
        let dataset = runtime::component::dataset::builder::DatasetBuilder::try_from(spec)?
            .with_app(Arc::clone(app))
            .with_runtime(Arc::clone(rt))
            .build()?;
        runtime::dataaccelerator::spice_sys::dataset_checkpointer(
            &dataset,
            rt.accelerator_engine_registry(),
            runtime_acceleration::sidecar::OpenOption::OpenExisting,
            runtime_acceleration::snapshot::SnapshotBehavior::Disabled,
        )
        .await
        .map_err(|error| anyhow::anyhow!("opening checkpoint: {error}"))
    }

    /// A checkpoint without primary-key metadata waits for its source once, then
    /// a successful refresh records the key for subsequent source-down starts.
    #[tokio::test]
    async fn a_legacy_keyed_checkpoint_is_upgraded_before_serving_without_its_source()
    -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(Some("integration=debug,info"));
        let fixture = Fixture::new("legacy-keyed-checkpoint").await?;
        fixture.source.report_primary_key();
        let source = &fixture.source;
        source.bring_up();
        let spec = || fixture.dataset(ReadyState::OnLoad);
        let (rt, loader) = start(spec()).await;
        assert!(served_from_acceleration(&rt, Duration::from_secs(30)).await);

        let checkpoint = open_checkpoint(&rt, spec()).await?;
        let recorded = wait_until_true(Duration::from_secs(10), || async {
            checkpoint.get_schema().await.ok().flatten().is_some()
        })
        .await;
        stop(rt, loader).await;
        assert!(recorded, "seed refresh must persist its schema checkpoint");
        let schema = checkpoint
            .get_schema()
            .await
            .map_err(|error| anyhow::anyhow!("reading checkpoint: {error}"))?
            .expect("seeded checkpoint has a schema");
        let mut metadata = schema.metadata().clone();
        assert!(
            metadata
                .remove(ACCELERATION_PRIMARY_KEY_METADATA_KEY)
                .is_some()
        );
        let legacy = Arc::new(schema.as_ref().clone().with_metadata(metadata));
        checkpoint
            .set_schema(&legacy)
            .await
            .map_err(|error| anyhow::anyhow!("writing legacy checkpoint: {error}"))?;

        let persisted_legacy = checkpoint
            .get_schema()
            .await
            .map_err(|error| anyhow::anyhow!("reading legacy checkpoint: {error}"))?
            .expect("legacy checkpoint has a schema");
        assert!(
            !persisted_legacy
                .metadata()
                .contains_key(ACCELERATION_PRIMARY_KEY_METADATA_KEY)
        );

        drop(checkpoint);
        let attempts = source.connect_attempts();
        let (rt, loader) = restart_with_source_down(source, spec()).await;
        let attempted = wait_until_true(Duration::from_secs(10), || async {
            source.connect_attempts() > attempts
                && matches!(
                    dataset_status(&rt, "orders"),
                    Some(ComponentStatus::Error(_))
                )
        })
        .await;
        let rows_while_down = sum_and_count(&rt).await;
        let ready_while_down = rt.status().is_ready();
        eprintln!(
            "legacy checkpoint: attempted={attempted} ready={ready_while_down} rows={rows_while_down:?}"
        );
        assert!(attempted, "legacy restart must attempt its source");
        assert!(!ready_while_down, "legacy checkpoint waits for the source");
        assert_eq!(rows_while_down, None);

        source.bring_up();
        let refreshed = refreshed_from_source(&rt).await;
        let checkpoint = open_checkpoint(&rt, spec()).await?;
        let upgraded = wait_until_true(Duration::from_secs(10), || async {
            checkpoint
                .get_schema()
                .await
                .ok()
                .flatten()
                .is_some_and(|schema| {
                    schema
                        .metadata()
                        .get(ACCELERATION_PRIMARY_KEY_METADATA_KEY)
                        .is_some_and(|key| key == r#"["id"]"#)
                })
        })
        .await;
        eprintln!(
            "legacy upgrade: refreshed={refreshed} upgraded={upgraded} rows={:?} checkpoint={:?}",
            sum_and_count(&rt).await,
            checkpoint
                .get_schema()
                .await
                .map(|schema| schema.map(|schema| schema.metadata().clone()))
        );
        stop(rt, loader).await;
        assert!(refreshed, "source recovery must refresh the keyed table");
        assert!(upgraded, "successful refresh must record the primary key");

        source.take_down();
        let (rt, loader) = start(spec()).await;
        let served = wait_until_true(Duration::from_secs(10), || async {
            sum_and_count(&rt).await == Some((6, 3)) && rt.status().is_ready()
        })
        .await;
        let rows = sum_and_count(&rt).await;
        eprintln!("upgraded checkpoint: served={served} rows={rows:?}");
        stop(rt, loader).await;
        assert!(
            served,
            "upgraded checkpoint serves while its source is down"
        );
        assert_eq!(rows, Some((6, 3)));
        Ok(())
    }

    /// A source error that escalates from a connection failure to rejected
    /// credentials is reported immediately while the acceleration keeps serving.
    #[tokio::test]
    async fn a_configuration_error_is_logged_after_a_transient_source_failure()
    -> Result<(), anyhow::Error> {
        let fixture = Fixture::new("escalating-source-error").await?;
        let source = &fixture.source;
        source.refuse_reads_instead_of_connecting();
        let spec = || fixture.dataset(ReadyState::OnLoad);
        seed(source, spec()).await?;

        let log_path = fixture.dir.path().join("source-errors.log");
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_writer(std::sync::Mutex::new(std::fs::File::create(&log_path)?))
            .finish();
        let _tracing = tracing::subscriber::set_default(subscriber);
        let (rt, loader) = restart_with_source_down(source, spec()).await;
        let served = served_from_acceleration(&rt, Duration::from_secs(10)).await;
        let warned = wait_until_true(Duration::from_secs(10), || async {
            std::fs::read_to_string(&log_path).is_ok_and(|logs| {
                logs.lines().any(|line| {
                    line.contains("WARN")
                        && line.contains("Serving data from the existing acceleration")
                })
            })
        })
        .await;
        source.reject_credentials();
        let configuration_status = wait_until_true(Duration::from_secs(10), || async {
            matches!(dataset_status(&rt, "orders"), Some(ComponentStatus::Error(Some(message))) if message.contains("configuration is fixed"))
        })
        .await;
        let error_reported = wait_until_true(Duration::from_secs(3), || async {
            std::fs::read_to_string(&log_path).is_ok_and(|logs| {
                logs.lines().any(|line| {
                    line.contains("ERROR")
                        && line
                            .contains("cannot connect to its source because of its configuration")
                })
            })
        })
        .await;
        let rows = sum_and_count(&rt).await;
        stop(rt, loader).await;
        let logs = std::fs::read_to_string(&log_path)?;
        for line in logs.lines().filter(|line| {
            line.contains("Serving data from the existing acceleration")
                || line.contains("configuration is fixed")
        }) {
            eprintln!("source error log: {line}");
        }
        eprintln!(
            "source error escalation: served={served} warned={warned} configuration_status={configuration_status} error_reported={error_reported} rows={rows:?}"
        );
        assert!(
            served && warned,
            "transient failure is reported while acceleration serves"
        );
        assert!(
            configuration_status,
            "rejected credentials must update status"
        );
        assert!(
            error_reported,
            "escalated configuration error must bypass the transient warning throttle"
        );
        assert_eq!(rows, Some((3, 3)));
        Ok(())
    }
}
