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
/// It can also be slow (every `read_provider` waits `read_delay_ms` first) and can
/// report a primary key on `id`, the way `DynamoDB` reports its key schema.
struct UnreachableSource {
    prefix: &'static str,
    up: AtomicBool,
    refuse_reads: AtomicBool,
    connect_attempts: AtomicUsize,
    value: AtomicUsize,
    read_delay_ms: AtomicU64,
    primary_key: AtomicBool,
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
            primary_key: AtomicBool::new(false),
        })
    }

    fn refuse_reads_instead_of_connecting(&self) {
        self.refuse_reads.store(true, Ordering::SeqCst);
    }

    /// Counts an attempt to reach the source, and fails it while the source is down.
    fn attempt(&self, connector_component: ConnectorComponent) -> Result<(), DataConnectorError> {
        self.connect_attempts.fetch_add(1, Ordering::SeqCst);
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

    fn report_primary_key(&self) {
        self.primary_key.store(true, Ordering::SeqCst);
    }

    fn connect_attempts(&self) -> usize {
        self.connect_attempts.load(Ordering::SeqCst)
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
        ]))
    }

    /// Three rows, each with the source's current `value` in `v`.
    fn table(&self) -> Result<MemTable, datafusion::error::DataFusionError> {
        let value = i32::try_from(self.value.load(Ordering::SeqCst)).unwrap_or(i32::MAX);
        let batch = RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![value; 3])),
            ],
        )?;
        let table = MemTable::try_new(Self::schema(), vec![vec![batch]])?;
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

#[async_trait]
impl DataConnector for UnreachableSourceConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        _context: &dyn ConnectorContext,
        dataset: &DatasetSpec,
    ) -> Result<Arc<dyn TableProvider>, DataConnectorError> {
        let delay = self.source.read_delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        if self.source.refuse_reads.load(Ordering::SeqCst) {
            self.source.attempt(ConnectorComponent::from(dataset))?;
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
        let batches = run_query(
            rt,
            "SELECT CAST(SUM(v) AS BIGINT) AS s, COUNT(*) AS n FROM orders",
        )
        .await
        .map_err(|err| err.to_string())?;
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

        source.bring_up();
        assert!(
            refreshed_from_source(&rt).await,
            "the refresh must bring the data up to date once the source is back, got {:?} ({:?})",
            sum_and_count(&rt).await,
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
        // by the source before the acceleration has loaded.
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
    /// unregistered: queries are answered from the existing acceleration after a
    /// short wait, and the data catches up once the source answers.
    #[tokio::test]
    async fn a_slow_source_does_not_hold_back_an_acceleration() -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(Some("integration=debug,info"));
        let fixture = Fixture::new("slow-source").await?;
        let source = &fixture.source;
        let spec = || fixture.dataset(ReadyState::OnLoad);
        seed(source, spec()).await?;

        source.set_value(2);
        source.set_read_delay(Duration::from_secs(6));
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
}
