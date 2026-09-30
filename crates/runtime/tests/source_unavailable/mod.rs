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
        atomic::{AtomicBool, AtomicUsize, Ordering},
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

/// A source whose connector cannot be built while it is down, failing the way the
/// `PostgreSQL` and `MySQL` connectors do when their database refuses the
/// connection: with `UnableToConnectInvalidHostOrPort`, raised from `create()`.
struct UnreachableSource {
    prefix: &'static str,
    up: AtomicBool,
    connect_attempts: AtomicUsize,
    value: AtomicUsize,
}

impl UnreachableSource {
    fn new(prefix: &'static str, value: usize) -> Arc<Self> {
        Arc::new(Self {
            prefix,
            up: AtomicBool::new(false),
            connect_attempts: AtomicUsize::new(0),
            value: AtomicUsize::new(value),
        })
    }

    fn bring_up(&self) {
        self.up.store(true, Ordering::SeqCst);
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
        MemTable::try_new(Self::schema(), vec![vec![batch]])
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
            self.source.connect_attempts.fetch_add(1, Ordering::SeqCst);
            if !self.source.up.load(Ordering::SeqCst) {
                return Err(DataConnectorError::UnableToConnectInvalidHostOrPort {
                    dataconnector: self.source.prefix.to_string(),
                    connector_component: params.component.clone(),
                    host: "127.0.0.1".to_string(),
                    port: "5432".to_string(),
                }
                .into());
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

/// Regression test for #14609: a source that is down when the runtime starts used
/// to fail the dataset permanently, so it never loaded even after the source came
/// back. It must keep retrying, and load once the source is reachable.
#[tokio::test]
async fn a_dataset_whose_source_is_down_at_startup_loads_once_the_source_is_reachable()
-> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let source = UnreachableSource::new("unreachable-at-startup", 7);
    source.register().await;

    let app = AppBuilder::new("source_unavailable")
        .with_dataset(SpicepodDataset::new(
            "unreachable-at-startup://orders",
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
        "source_down_at_startup_loads_once_reachable",
        arrow::util::pretty::pretty_format_batches(&batches)?
    );

    loader.abort();
    Ok(())
}
