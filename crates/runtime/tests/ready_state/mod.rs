/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

#![allow(clippy::expect_used)]

use std::{
    any::Any,
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use app::AppBuilder;
use arrow::{
    array::RecordBatch,
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use async_trait::async_trait;
use datafusion::{
    arrow::array::Int32Array,
    catalog::Session,
    common::Statistics,
    datasource::{MemTable, TableProvider},
    error::{DataFusionError, Result as DataFusionResult},
    execution::TaskContext,
    physical_plan::{
        memory::MemoryExec, stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType,
        ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    },
    prelude::{Expr, SessionContext},
    sql::unparser::dialect::{Dialect, PostgreSqlDialect},
};
use datafusion_federation::{
    table_reference::MultiPartTableReference, FederatedTableProviderAdaptor,
};
use datafusion_federation_sql::{SQLExecutor, SQLFederationProvider, SQLTableSource};
use futures::{Stream, TryStreamExt};
use runtime::{
    component::dataset::Dataset, dataconnector::{
        self, ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError,
        DataConnectorFactory, NewDataConnectorResult,
    }, parameters::ParameterSpec, request::{AsyncMarker, Protocol, RequestContext}, status, Runtime
};
use spicepod::component::dataset::{
    acceleration::Acceleration, Dataset as SpicepodDataset, ReadyState,
};

use crate::{get_test_datafusion, init_tracing};

/// A stream that only yields data when signaled
struct DelayedStream<T: Stream> {
    inner: T,
    delay_duration: Duration,
    start_time: Instant,
}

impl<T: Stream> DelayedStream<T> {
    fn new(inner: T, delay_duration: Duration) -> Self {
        Self {
            inner,
            delay_duration,
            start_time: Instant::now(),
        }
    }
}

impl<T: Stream + Unpin> Stream for DelayedStream<T> {
    type Item = T::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.start_time + self.delay_duration > Instant::now() {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        Pin::new(&mut self.inner).poll_next(cx)
    }
}

fn mock_data_mem_table() -> Arc<dyn TableProvider> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let data = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
    )
    .expect("Failed to create record batch");

    let mock_table =
        Arc::new(MemTable::try_new(schema, vec![vec![data]]).expect("Failed to create mock table"));
    mock_table as Arc<dyn TableProvider>    
}

// Native data connector implementation
struct SlowNativeDataConnector {
    mock_data: Arc<dyn TableProvider>,
}

impl SlowNativeDataConnector {
    fn new() -> Self {
        Self {
            mock_data: mock_data_mem_table(),
        }
    }
}

#[async_trait]
impl DataConnector for SlowNativeDataConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        _dataset: &Dataset,
    ) -> Result<Arc<dyn TableProvider>, DataConnectorError> {
        // Create wrapper table provider that delays the stream
        let delayed_provider = DelayedNativeTableProvider {
            schema: self.mock_data.schema(),
            inner_provider: Arc::clone(&self.mock_data),
        };

        Ok(Arc::new(delayed_provider) as Arc<dyn TableProvider>)
    }
}

// Federated data connector implementation
struct SlowFederatedDataConnector {
    schema: SchemaRef,
}

impl SlowFederatedDataConnector {
    fn new() -> Self {
        // Create mock schema
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        Self { schema }
    }
}

#[async_trait]
impl DataConnector for SlowFederatedDataConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> Result<Arc<dyn TableProvider>, DataConnectorError> {
        // Create SQLExecutor implementation
        let executor = Arc::new(MockSQLExecutor {
            schema: Arc::clone(&self.schema),
            table_name: dataset.name.to_string(),
        });

        // Create federation provider
        let federation_provider = Arc::new(SQLFederationProvider::new(executor));

        // Create table source
        let table_reference = MultiPartTableReference::TableReference(dataset.name.clone());

        let source = SQLTableSource::new_with_schema(
            federation_provider,
            table_reference,
            Arc::clone(&self.schema),
        )
        .map_err(|e| DataConnectorError::UnableToConnectInternal {
            dataconnector: "SlowFederatedDataConnector".to_string(),
            connector_component: ConnectorComponent::Dataset(Arc::new(dataset.clone())),
            source: e.into(),
        })?;

        let fallback_provider = Arc::new(DelayedNativeTableProvider {
            schema: Arc::clone(&self.schema),
            inner_provider: mock_data_mem_table(),
        }) as Arc<dyn TableProvider>;

        // Create the federated table provider adaptor
        let provider = FederatedTableProviderAdaptor::new_with_provider(Arc::new(source), fallback_provider);

        Ok(Arc::new(provider) as Arc<dyn TableProvider>)
    }
}

// Native table provider that delays responses
#[derive(Debug)]
struct DelayedNativeTableProvider {
    schema: SchemaRef,
    inner_provider: Arc<dyn TableProvider>,
}

#[async_trait]
impl TableProvider for DelayedNativeTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> datafusion::datasource::TableType {
        self.inner_provider.table_type()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let request_context = RequestContext::current(AsyncMarker::new().await);

        // Get the inner execution plan
        let inner_plan = self
            .inner_provider
            .scan(state, projection, filters, limit)
            .await?;

        if request_context.protocol() == Protocol::Internal {
            // Create a wrapper execution plan that delays the stream on internal requests (i.e. accelerator loads)
            tracing::info!("Delaying stream on internal request");
            Ok(Arc::new(DelayedExecutionPlan { inner: inner_plan }))
        } else {
            tracing::info!("Not delaying stream on non-internal request");

            Ok(inner_plan)
        }
    }
}

// Execution plan that delays its output stream
struct DelayedExecutionPlan {
    inner: Arc<dyn ExecutionPlan>,
}

impl std::fmt::Debug for DelayedExecutionPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DelayedExecutionPlan")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl ExecutionPlan for DelayedExecutionPlan {
    fn name(&self) -> &'static str {
        "DelayedExecutionPlan"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.inner.properties()
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn datafusion::physical_plan::ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        assert_eq!(children.len(), 1);
        Ok(Arc::new(Self {
            inner: Arc::clone(&children[0]),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let stream = self.inner.execute(partition, context)?;
        let schema = stream.schema();

        let record_batch_stream = RecordBatchStreamAdapter::new(
            schema,
            DelayedStream::new(stream, Duration::from_secs(5)),
        );

        Ok(Box::pin(record_batch_stream))
    }

    fn statistics(&self) -> datafusion::error::Result<Statistics> {
        self.inner.statistics()
    }
}

impl DisplayAs for DelayedExecutionPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DelayedExecutionPlan")
    }
}

// Mock SQL executor for federated providers
struct MockSQLExecutor {
    schema: SchemaRef,
    table_name: String,
}

#[async_trait]
impl SQLExecutor for MockSQLExecutor {
    fn name(&self) -> &'static str {
        "mock_sql_executor"
    }

    fn compute_context(&self) -> Option<String> {
        Some("mock_context".to_string())
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(PostgreSqlDialect {})
    }

    fn execute(
        &self,
        _query: &str,
        schema: SchemaRef,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        // Create test data
        let data = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .expect("Failed to create record batch");

        // Create a context to execute against the mock data
        let ctx = SessionContext::new();

        let exec = MemoryExec::try_new(&[vec![data]], Arc::clone(&schema), None)?;

        let stream = exec.execute(0, ctx.task_ctx())?;

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            DelayedStream::new(stream, Duration::from_secs(5)),
        )))
    }

    async fn table_names(&self) -> DataFusionResult<Vec<String>> {
        Ok(vec![self.table_name.clone()])
    }

    async fn get_table_schema(&self, table_name: &str) -> DataFusionResult<SchemaRef> {
        if table_name == self.table_name {
            Ok(Arc::clone(&self.schema))
        } else {
            Err(DataFusionError::Plan(format!(
                "Table '{table_name}' not found"
            )))
        }
    }
}

// Data connector provider for native provider
struct SlowNativeDataConnectorProvider {}

impl SlowNativeDataConnectorProvider {
    fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

#[async_trait]
impl DataConnectorFactory for SlowNativeDataConnectorProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        _params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send>> {
        Box::pin(
            async move { Ok(Arc::new(SlowNativeDataConnector::new()) as Arc<dyn DataConnector>) },
        )
    }

    fn prefix(&self) -> &'static str {
        "slow-loading-native"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        &[]
    }
}

// Data connector provider for federated provider
struct SlowFederatedDataConnectorProvider {}

impl SlowFederatedDataConnectorProvider {
    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

impl DataConnectorFactory for SlowFederatedDataConnectorProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        _params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            Ok(Arc::new(SlowFederatedDataConnector::new()) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "test"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        &[]
    }
}

// Register our data connector providers
async fn register_slow_loading_providers() {
    dataconnector::register_connector_factory(
        "slow-loading-native",
        SlowNativeDataConnectorProvider::new_arc(),
    )
    .await;

    dataconnector::register_connector_factory(
        "slow-loading-federated",
        SlowFederatedDataConnectorProvider::new_arc(),
    )
    .await;
}

fn get_native_dataset(
    name: &str,
    ready_state: ReadyState,
    engine: Option<String>,
) -> SpicepodDataset {
    let mut dataset = SpicepodDataset::new("slow-loading-native://dummy", name);
    dataset.ready_state = ready_state;
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine,
        ..Default::default()
    });

    dataset
}

fn get_federated_dataset(
    name: &str, 
    ready_state: ReadyState,
    engine: Option<String>) -> SpicepodDataset {
    let mut dataset = SpicepodDataset::new("slow-loading-federated://dummy", name);
    dataset.ready_state = ready_state;
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine,
        ..Default::default()
    });
    dataset
}

// Test that the runtime is ready immediately with ready_state = on_registration for native provider
#[tokio::test]
async fn test_ready_state_on_registration_native_arrow_acceleration() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    tracing::info!("Starting test");

    register_slow_loading_providers().await;

    tracing::info!("Registered providers");

    let request_context = Arc::new(RequestContext::builder(Protocol::Http).build());
    request_context.scope(async {
        tracing::info!("Creating app");
        let app = AppBuilder::new("ready_state_tests")
            .with_dataset(get_native_dataset(
                "native_on_registration",
                ReadyState::OnRegistration,
                None, // Arrow by default
            ))
            .build();

        let status = status::RuntimeStatus::new();
        let df = get_test_datafusion(Arc::clone(&status));

        let rt = Runtime::builder()
            .with_datafusion(df)
            .with_app(app)
            .build()
            .await;

        tracing::info!("Loading components");
        tokio::select! {
            () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
            }
            () = rt.load_components() => {}
        }

        tracing::info!("Running query");
        // Now run a query before data is loaded - it should succeed by falling back to the source
        let query_result = tokio::select! {
            () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                return Err(anyhow::anyhow!("Timed out waiting for query to complete"));
            }
            result = rt
                .datafusion()
                .query_builder("SELECT * FROM native_on_registration")
                .build()
                .run() => {
                result.map_err(|e| anyhow::anyhow!(e))?
            }
        };
        tracing::info!("Query complete");
        // Convert the stream to a vector with timeout
        let results: Result<Vec<RecordBatch>, _> = tokio::select! {
            () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                Err(DataFusionError::Execution("Timed out waiting for query results".to_string()))
            }
            result = query_result.data.try_collect::<Vec<_>>() => {
                result
            }
        };
        tracing::info!("Results collected");
        let results = results.expect("Query should not return an error");
        assert_eq!(results.len(), 1, "Query should return 1 record batch");
        assert_eq!(results[0].num_rows(), 5, "Should have 5 rows of data");

        let explain_result = tokio::select! {
            () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                return Err(anyhow::anyhow!("Timed out waiting for query to complete"));
            }
            result = rt
                .datafusion()
                .query_builder("EXPLAIN SELECT * FROM native_on_registration")
                .build()
                .run() => {
                result.map_err(|e| anyhow::anyhow!(e))?
            }
        };
        let explain_result = explain_result.data.try_collect::<Vec<RecordBatch>>().await.expect("Explain should not return an error");
        let explain_str_fallback =
            arrow::util::pretty::pretty_format_batches(&explain_result).expect("pretty batches");
        insta::assert_snapshot!(explain_str_fallback);

        // Wait for acceleration to load
        tokio::time::sleep(std::time::Duration::from_secs(6)).await;

        // Query again, now we should get the same results
        let query_result = rt
            .datafusion()
            .query_builder("SELECT * FROM native_on_registration")
            .build()
            .run()
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        // Convert the stream to a vector
        let results: Vec<RecordBatch> = query_result.data.try_collect::<Vec<RecordBatch>>().await.expect("Query should not return an error");

        assert_eq!(results.len(), 1, "Query should return 1 record batch");
        assert_eq!(results[0].num_rows(), 5, "Should have 5 rows of data");

        // Now re-run the explain query
        let explain_result = tokio::select! {
            () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                return Err(anyhow::anyhow!("Timed out waiting for query to complete"));
            }
            result = rt.datafusion().query_builder("EXPLAIN SELECT * FROM native_on_registration").build().run() => {
                result.map_err(|e| anyhow::anyhow!(e))?
            }
        };
        let explain_result = explain_result.data.try_collect::<Vec<RecordBatch>>().await.expect("Explain should not return an error");
        let explain_str_acceleration =
            arrow::util::pretty::pretty_format_batches(&explain_result).expect("pretty batches");
        insta::assert_snapshot!(explain_str_acceleration);

        Ok(())
    })
    .await
}

// Test that the runtime is ready immediately with ready_state = on_registration for native provider
#[cfg(feature = "duckdb")]
#[tokio::test]
async fn test_ready_state_on_registration_native_duckdb_acceleration() -> Result<(), anyhow::Error>
{
    let _tracing = init_tracing(Some("integration=debug,info"));

    register_slow_loading_providers().await;

    let request_context = Arc::new(RequestContext::builder(Protocol::Http).build());
    request_context
        .scope(async {
            let app = AppBuilder::new("ready_state_tests")
                .with_dataset(get_native_dataset(
                    "native_on_registration",
                    ReadyState::OnRegistration,
                    Some("duckdb".to_string()),
                ))
                .build();

            let status = status::RuntimeStatus::new();
            let df = get_test_datafusion(Arc::clone(&status));

            let rt = Runtime::builder()
                .with_datafusion(df)
                .with_app(app)
                .build()
                .await;

            tracing::info!("Loading components");
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = rt.load_components() => {}
            }

            tracing::info!("Running query");
            // Now run a query before data is loaded - it should succeed by falling back to the source
            let query_result = rt
                .datafusion()
                .query_builder("SELECT * FROM native_on_registration")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            tracing::info!("Query complete");
            // Convert the stream to a vector with timeout
            let results: Result<Vec<RecordBatch>, _> = tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    Err(DataFusionError::Execution("Timed out waiting for query results".to_string()))
                }
                result = query_result.data.try_collect::<Vec<_>>() => {
                    result
                }
            };
            tracing::info!("Results collected");
            let results = results.expect("Query should not return an error");
            assert_eq!(results.len(), 1, "Query should return 1 record batch");
            assert_eq!(results[0].num_rows(), 5, "Should have 5 rows of data");
    
            let explain_result = tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for query to complete"));
                }
                result = rt
                    .datafusion()
                    .query_builder("EXPLAIN SELECT * FROM native_on_registration")
                    .build()
                    .run() => {
                    result.map_err(|e| anyhow::anyhow!(e))?
                }
            };
            let explain_result = explain_result.data.try_collect::<Vec<RecordBatch>>().await.expect("Explain should not return an error");
            let explain_str_fallback =
                arrow::util::pretty::pretty_format_batches(&explain_result).expect("pretty batches");
            insta::assert_snapshot!(explain_str_fallback);
    
            // Wait for acceleration to load
            tokio::time::sleep(std::time::Duration::from_secs(6)).await;
    
            // Query again, now we should get the same results
            let query_result = rt
                .datafusion()
                .query_builder("SELECT * FROM native_on_registration")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
    
            // Convert the stream to a vector
            let results: Vec<RecordBatch> = query_result.data.try_collect::<Vec<RecordBatch>>().await.expect("Query should not return an error");
    
            assert_eq!(results.len(), 1, "Query should return 1 record batch");
            assert_eq!(results[0].num_rows(), 5, "Should have 5 rows of data");
    
            // Now re-run the explain query
            let explain_result = tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for query to complete"));
                }
                result = rt.datafusion().query_builder("EXPLAIN SELECT * FROM native_on_registration").build().run() => {
                    result.map_err(|e| anyhow::anyhow!(e))?
                }
            };
            let explain_result = explain_result.data.try_collect::<Vec<RecordBatch>>().await.expect("Explain should not return an error");
            let explain_str_acceleration =
                arrow::util::pretty::pretty_format_batches(&explain_result).expect("pretty batches");
            insta::assert_snapshot!(explain_str_acceleration);
    
            Ok(())
        })
        .await
}

// // Test that the runtime is ready immediately with ready_state = on_registration for federated provider
#[tokio::test]
async fn test_ready_state_on_registration_federated_arrow_acceleration() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    register_slow_loading_providers().await;

    let request_context = Arc::new(RequestContext::builder(Protocol::Http).build());
    request_context
        .scope(async {
            let app = AppBuilder::new("ready_state_federated_tests")
                .with_dataset(get_federated_dataset(
                    "federated_on_registration",
                    ReadyState::OnRegistration,
                    None,
                ))
                .build();

            let status = status::RuntimeStatus::new();
            let df = get_test_datafusion(Arc::clone(&status));

            let rt = Runtime::builder()
                .with_datafusion(df)
                .with_app(app)
                .build()
                .await;

            tracing::info!("Loading components");
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = rt.load_components() => {}
            }

            // Now run a query before data is loaded - it should succeed by falling back to the source
            let query_result = rt
                .datafusion()
                .query_builder("SELECT * FROM federated_on_registration")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            tracing::info!("Query complete");
            // Convert the stream to a vector with timeout
            let results: Result<Vec<RecordBatch>, _> = tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    Err(DataFusionError::Execution("Timed out waiting for query results".to_string()))
                }
                result = query_result.data.try_collect::<Vec<_>>() => {
                    result
                }
            };
            tracing::info!("Results collected");
            let results = results.expect("Query should not return an error");
            assert_eq!(results.len(), 1, "Query should return 1 record batch");
            assert_eq!(results[0].num_rows(), 5, "Should have 5 rows of data");

            let explain_result = tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for query to complete"));
                }
                result = rt
                    .datafusion()
                    .query_builder("EXPLAIN SELECT * FROM federated_on_registration")
                    .build()
                    .run() => {
                    result.map_err(|e| anyhow::anyhow!(e))?
                }
            };
            let explain_result = explain_result.data.try_collect::<Vec<RecordBatch>>().await.expect("Explain should not return an error");
            let explain_str_fallback =
                arrow::util::pretty::pretty_format_batches(&explain_result).expect("pretty batches");
            insta::assert_snapshot!(explain_str_fallback);
    
            // Wait for acceleration to load
            tokio::time::sleep(std::time::Duration::from_secs(6)).await;

            // Query again, now we should get the same results
            let query_result = rt
                .datafusion()
                .query_builder("SELECT * FROM federated_on_registration")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
    
            // Convert the stream to a vector
            let results: Vec<RecordBatch> = query_result.data.try_collect::<Vec<RecordBatch>>().await.expect("Query should not return an error");
    
            assert_eq!(results.len(), 1, "Query should return 1 record batch");
            assert_eq!(results[0].num_rows(), 5, "Should have 5 rows of data");
    
            // Now re-run the explain query
            let explain_result = tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for query to complete"));
                }
                result = rt.datafusion().query_builder("EXPLAIN SELECT * FROM federated_on_registration").build().run() => {
                    result.map_err(|e| anyhow::anyhow!(e))?
                }
            };
            let explain_result = explain_result.data.try_collect::<Vec<RecordBatch>>().await.expect("Explain should not return an error");
            let explain_str_acceleration =
                arrow::util::pretty::pretty_format_batches(&explain_result).expect("pretty batches");
            insta::assert_snapshot!(explain_str_acceleration);
    
            Ok(())
        })
        .await
}

// Test that the runtime is ready immediately with ready_state = on_registration for federated provider
#[cfg(feature = "duckdb")]
#[tokio::test]
async fn test_ready_state_on_registration_federated_duckdb_acceleration() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    register_slow_loading_providers().await;

    let request_context = Arc::new(RequestContext::builder(Protocol::Http).build());
    request_context
        .scope(async {
            let app = AppBuilder::new("ready_state_federated_tests")
                .with_dataset(get_federated_dataset(
                    "federated_on_registration",
                    ReadyState::OnRegistration,
                    Some("duckdb".to_string()),
                ))
                .build();

            let status = status::RuntimeStatus::new();
            let df = get_test_datafusion(Arc::clone(&status));

            let rt = Runtime::builder()
                .with_datafusion(df)
                .with_app(app)
                .build()
                .await;

            tracing::info!("Loading components");
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = rt.load_components() => {}
            }

            // Now run a query before data is loaded - it should succeed by falling back to the source
            let query_result = rt
                .datafusion()
                .query_builder("SELECT * FROM federated_on_registration")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            tracing::info!("Query complete");
            // Convert the stream to a vector with timeout
            let results: Result<Vec<RecordBatch>, _> = tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    Err(DataFusionError::Execution("Timed out waiting for query results".to_string()))
                }
                result = query_result.data.try_collect::<Vec<_>>() => {
                    result
                }
            };
            tracing::info!("Results collected");
            let results = results.expect("Query should not return an error");
            assert_eq!(results.len(), 1, "Query should return 1 record batch");
            assert_eq!(results[0].num_rows(), 5, "Should have 5 rows of data");
    
            let explain_result = tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for query to complete"));
                }
                result = rt
                    .datafusion()
                    .query_builder("EXPLAIN SELECT * FROM federated_on_registration")
                    .build()
                    .run() => {
                    result.map_err(|e| anyhow::anyhow!(e))?
                }
            };
            let explain_result = explain_result.data.try_collect::<Vec<RecordBatch>>().await.expect("Explain should not return an error");
            let explain_str_fallback =
                arrow::util::pretty::pretty_format_batches(&explain_result).expect("pretty batches");
            insta::assert_snapshot!(explain_str_fallback);
    
            // Wait for acceleration to load
            tokio::time::sleep(std::time::Duration::from_secs(6)).await;
    
            // Query again, now we should get the same results
            let query_result = rt
                .datafusion()
                .query_builder("SELECT * FROM federated_on_registration")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
    
            // Convert the stream to a vector
            let results: Vec<RecordBatch> = query_result.data.try_collect::<Vec<RecordBatch>>().await.expect("Query should not return an error");
    
            assert_eq!(results.len(), 1, "Query should return 1 record batch");
            assert_eq!(results[0].num_rows(), 5, "Should have 5 rows of data");
    
            // Now re-run the explain query
            let explain_result = tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for query to complete"));
                }
                result = rt.datafusion().query_builder("EXPLAIN SELECT * FROM federated_on_registration").build().run() => {
                    result.map_err(|e| anyhow::anyhow!(e))?
                }
            };
            let explain_result = explain_result.data.try_collect::<Vec<RecordBatch>>().await.expect("Explain should not return an error");
            let explain_str_acceleration =
                arrow::util::pretty::pretty_format_batches(&explain_result).expect("pretty batches");
            insta::assert_snapshot!(explain_str_acceleration);
    
            Ok(())
        })
        .await
}

// Test that the runtime is NOT ready until data loads with ready_state = on_load for native provider
#[tokio::test]
async fn test_ready_state_on_load_native_arrow_acceleration() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    register_slow_loading_providers().await;

    let request_context = Arc::new(RequestContext::builder(Protocol::Http).build());
    request_context.scope(async {
            tracing::info!("Creating app");
            let app = AppBuilder::new("ready_state_tests")
                .with_dataset(get_native_dataset(
                    "native_on_registration",
                    ReadyState::OnLoad,
                    None, // Arrow by default
                ))
                .build();

            let status = status::RuntimeStatus::new();
            let df = get_test_datafusion(Arc::clone(&status));

            let rt = Runtime::builder()
                .with_datafusion(df)
                .with_app(app)
                .build()
                .await;

            tracing::info!("Loading components");
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = rt.load_components() => {}
            }

            tracing::info!("Running query");
            // Now run a query before data is loaded - it should return an error indicating the data is still loading
            let query_result = tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for query to complete"));
                }
                result = rt
                    .datafusion()
                    .query_builder("SELECT * FROM native_on_registration")
                    .build()
                    .run() => {result}
            };
            tracing::info!("Query complete");
            let results = query_result.expect_err("Query should return an error - the acceleration should still be loading data");
            assert!(results.to_string().contains("Acceleration not ready; loading initial data for native_on_registration"));
    
            let explain_result = tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for query to complete"));
                }
                result = rt
                    .datafusion()
                    .query_builder("EXPLAIN SELECT * FROM native_on_registration")
                    .build()
                    .run() => {
                    result.map_err(|e| anyhow::anyhow!(e))?
                }
            };
            let explain_result = explain_result.data.try_collect::<Vec<RecordBatch>>().await.expect("Explain should not return an error");
            let explain_str_fallback =
                arrow::util::pretty::pretty_format_batches(&explain_result).expect("pretty batches");
            insta::assert_snapshot!(explain_str_fallback);
    
            // Wait for acceleration to load
            tokio::time::sleep(std::time::Duration::from_secs(6)).await;
    
            // Query again, now we should get the results
            let query_result = rt
                .datafusion()
                .query_builder("SELECT * FROM native_on_registration")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
    
            // Convert the stream to a vector
            let results: Vec<RecordBatch> = query_result.data.try_collect::<Vec<RecordBatch>>().await.expect("Query should not return an error");
    
            assert_eq!(results.len(), 1, "Query should return 1 record batch");
            assert_eq!(results[0].num_rows(), 5, "Should have 5 rows of data");
    
            // Now re-run the explain query
            let explain_result = tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for query to complete"));
                }
                result = rt.datafusion().query_builder("EXPLAIN SELECT * FROM native_on_registration").build().run() => {
                    result.map_err(|e| anyhow::anyhow!(e))?
                }
            };
            let explain_result = explain_result.data.try_collect::<Vec<RecordBatch>>().await.expect("Explain should not return an error");
            let explain_str_acceleration =
                arrow::util::pretty::pretty_format_batches(&explain_result).expect("pretty batches");
            insta::assert_snapshot!(explain_str_acceleration);
    
            Ok(())
        })
        .await
}

// Test that the runtime is NOT ready until data loads with ready_state = on_load for native provider
#[cfg(feature = "duckdb")]
#[tokio::test]
async fn test_ready_state_on_load_native_duckdb_acceleration() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    register_slow_loading_providers().await;

    let request_context = Arc::new(RequestContext::builder(Protocol::Http).build());
    request_context
        .scope(async {
            let app = AppBuilder::new("ready_state_on_load_tests")
                .with_dataset(get_native_dataset(
                    "native_on_load",
                    ReadyState::OnLoad,
                    Some("duckdb".to_string()),
                ))
                .build();

            let status = status::RuntimeStatus::new();
            let df = get_test_datafusion(Arc::clone(&status));

            let rt = Runtime::builder()
                .with_datafusion(df)
                .with_app(app)
                .build()
                .await;

            tracing::info!("Loading components");
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = rt.load_components() => {}
            }

            tracing::info!("Running query");
            // Now run a query before data is loaded - it should return an error indicating the data is still loading
            let query_result = tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for query to complete"));
                }
                result = rt
                    .datafusion()
                    .query_builder("SELECT * FROM native_on_load")
                    .build()
                    .run() => {result}
            };
            tracing::info!("Query complete");
            let results = query_result.expect_err("Query should return an error - the acceleration should still be loading data");
            assert!(results.to_string().contains("Acceleration not ready; loading initial data for native_on_load"));
    
            let explain_result = tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for query to complete"));
                }
                result = rt
                    .datafusion()
                    .query_builder("EXPLAIN SELECT * FROM native_on_load")
                    .build()
                    .run() => {
                    result.map_err(|e| anyhow::anyhow!(e))?
                }
            };
            let explain_result = explain_result.data.try_collect::<Vec<RecordBatch>>().await.expect("Explain should not return an error");
            let explain_str_fallback =
                arrow::util::pretty::pretty_format_batches(&explain_result).expect("pretty batches");
            insta::assert_snapshot!(explain_str_fallback);
    
            // Wait for acceleration to load
            tokio::time::sleep(std::time::Duration::from_secs(6)).await;
    
            // Query again, now we should get the results
            let query_result = rt
                .datafusion()
                .query_builder("SELECT * FROM native_on_load")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
    
            // Convert the stream to a vector
            let results: Vec<RecordBatch> = query_result.data.try_collect::<Vec<RecordBatch>>().await.expect("Query should not return an error");
    
            assert_eq!(results.len(), 1, "Query should return 1 record batch");
            assert_eq!(results[0].num_rows(), 5, "Should have 5 rows of data");
    
            // Now re-run the explain query
            let explain_result = tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for query to complete"));
                }
                result = rt.datafusion().query_builder("EXPLAIN SELECT * FROM native_on_load").build().run() => {
                    result.map_err(|e| anyhow::anyhow!(e))?
                }
            };
            let explain_result = explain_result.data.try_collect::<Vec<RecordBatch>>().await.expect("Explain should not return an error");
            let explain_str_acceleration =
                arrow::util::pretty::pretty_format_batches(&explain_result).expect("pretty batches");
            insta::assert_snapshot!(explain_str_acceleration);
    
            Ok(())
        })
        .await
}

// Test that the runtime is NOT ready until data loads with ready_state = on_load for federated provider
#[tokio::test]
async fn test_ready_state_on_load_federated_arrow_acceleration() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    register_slow_loading_providers().await;

    let request_context = Arc::new(RequestContext::builder(Protocol::Http).build());
    request_context
        .scope(async {
            let app = AppBuilder::new("ready_state_on_load_fed_tests")
                .with_dataset(get_federated_dataset(
                    "federated_on_load",
                    ReadyState::OnLoad,
                    None,
                ))
                .build();

            let status = status::RuntimeStatus::new();
            let df = get_test_datafusion(Arc::clone(&status));

            let rt = Runtime::builder()
                .with_datafusion(df)
                .with_app(app)
                .build()
                .await;

            tracing::info!("Loading components");
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = rt.load_components() => {}
            }

            tracing::info!("Running query");
            // Now run a query before data is loaded - it should return an error indicating the data is still loading
            let query_result = tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for query to complete"));
                }
                result = rt
                    .datafusion()
                    .query_builder("SELECT * FROM federated_on_load")
                    .build()
                    .run() => {result}
            };
            tracing::info!("Query complete");
            let results = query_result.expect_err("Query should return an error - the acceleration should still be loading data");
            assert!(results.to_string().contains("Acceleration not ready; loading initial data for federated_on_load"));
    
            let explain_result = tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for query to complete"));
                }
                result = rt
                    .datafusion()
                    .query_builder("EXPLAIN SELECT * FROM federated_on_load")
                    .build()
                    .run() => {
                    result.map_err(|e| anyhow::anyhow!(e))?
                }
            };
            let explain_result = explain_result.data.try_collect::<Vec<RecordBatch>>().await.expect("Explain should not return an error");
            let explain_str_fallback =
                arrow::util::pretty::pretty_format_batches(&explain_result).expect("pretty batches");
            insta::assert_snapshot!(explain_str_fallback);
    
            // Wait for acceleration to load
            tokio::time::sleep(std::time::Duration::from_secs(6)).await;
    
            // Query again, now we should get the results
            let query_result = rt
                .datafusion()
                .query_builder("SELECT * FROM federated_on_load")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
    
            // Convert the stream to a vector
            let results: Vec<RecordBatch> = query_result.data.try_collect::<Vec<RecordBatch>>().await.expect("Query should not return an error");
    
            assert_eq!(results.len(), 1, "Query should return 1 record batch");
            assert_eq!(results[0].num_rows(), 5, "Should have 5 rows of data");
    
            // Now re-run the explain query
            let explain_result = tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for query to complete"));
                }
                result = rt.datafusion().query_builder("EXPLAIN SELECT * FROM federated_on_load").build().run() => {
                    result.map_err(|e| anyhow::anyhow!(e))?
                }
            };
            let explain_result = explain_result.data.try_collect::<Vec<RecordBatch>>().await.expect("Explain should not return an error");
            let explain_str_acceleration =
                arrow::util::pretty::pretty_format_batches(&explain_result).expect("pretty batches");
            insta::assert_snapshot!(explain_str_acceleration);
    
            Ok(())
        })
        .await
}

// Test that the runtime is NOT ready until data loads with ready_state = on_load for federated provider
#[cfg(feature = "duckdb")]
#[tokio::test]
async fn test_ready_state_on_load_federated_duckdb_acceleration() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    register_slow_loading_providers().await;

    let request_context = Arc::new(RequestContext::builder(Protocol::Http).build());
    request_context
        .scope(async {
            let app = AppBuilder::new("ready_state_on_load_fed_tests")
                .with_dataset(get_federated_dataset(
                    "federated_on_load",
                    ReadyState::OnLoad,
                    Some("duckdb".to_string()),
                ))
                .build();

            let status = status::RuntimeStatus::new();
            let df = get_test_datafusion(Arc::clone(&status));

            let rt = Runtime::builder()
                .with_datafusion(df)
                .with_app(app)
                .build()
                .await;

            tracing::info!("Loading components");
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = rt.load_components() => {}
            }

            tracing::info!("Running query");
            // Now run a query before data is loaded - it should return an error indicating the data is still loading
            let query_result = tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for query to complete"));
                }
                result = rt
                    .datafusion()
                    .query_builder("SELECT * FROM federated_on_load")
                    .build()
                    .run() => {result}
            };
            tracing::info!("Query complete");
            let results = query_result.expect_err("Query should return an error - the acceleration should still be loading data");
            assert!(results.to_string().contains("Acceleration not ready; loading initial data for federated_on_load"));
    
            let explain_result = tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for query to complete"));
                }
                result = rt
                    .datafusion()
                    .query_builder("EXPLAIN SELECT * FROM federated_on_load")
                    .build()
                    .run() => {
                    result.map_err(|e| anyhow::anyhow!(e))?
                }
            };
            let explain_result = explain_result.data.try_collect::<Vec<RecordBatch>>().await.expect("Explain should not return an error");
            let explain_str_fallback =
                arrow::util::pretty::pretty_format_batches(&explain_result).expect("pretty batches");
            insta::assert_snapshot!(explain_str_fallback);
    
            // Wait for acceleration to load
            tokio::time::sleep(std::time::Duration::from_secs(6)).await;
    
            // Query again, now we should get the results
            let query_result = rt
                .datafusion()
                .query_builder("SELECT * FROM federated_on_load")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
    
            // Convert the stream to a vector
            let results: Vec<RecordBatch> = query_result.data.try_collect::<Vec<RecordBatch>>().await.expect("Query should not return an error");
    
            assert_eq!(results.len(), 1, "Query should return 1 record batch");
            assert_eq!(results[0].num_rows(), 5, "Should have 5 rows of data");
    
            // Now re-run the explain query
            let explain_result = tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for query to complete"));
                }
                result = rt.datafusion().query_builder("EXPLAIN SELECT * FROM federated_on_load").build().run() => {
                    result.map_err(|e| anyhow::anyhow!(e))?
                }
            };
            let explain_result = explain_result.data.try_collect::<Vec<RecordBatch>>().await.expect("Explain should not return an error");
            let explain_str_acceleration =
                arrow::util::pretty::pretty_format_batches(&explain_result).expect("pretty batches");
            insta::assert_snapshot!(explain_str_acceleration);
    
            Ok(())
        })
        .await
}

// Test both native and federated providers together with different ready states
#[tokio::test]
async fn test_ready_state_mixed_arrow_acceleration() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    register_slow_loading_providers().await;

    let request_context = Arc::new(RequestContext::builder(Protocol::Http).build());
    request_context
        .scope(async {
            let app = AppBuilder::new("ready_state_mixed_tests")
                .with_dataset(get_native_dataset(
                    "native_on_registration_mixed",
                    ReadyState::OnRegistration,
                    None,
                ))
                .with_dataset(get_federated_dataset(
                    "federated_on_load_mixed",
                    ReadyState::OnLoad,
                    None,
                ))
                .build();

            let status = status::RuntimeStatus::new();
            let df = get_test_datafusion(Arc::clone(&status));

            let rt = Runtime::builder()
                .with_datafusion(df)
                .with_app(app)
                .build()
                .await;

            tracing::info!("Loading components");
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = rt.load_components() => {}
            }

            // Queries to native_on_registration_mixed should work right away
            let query_result = rt
                .datafusion()
                .query_builder("SELECT * FROM native_on_registration_mixed")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;

            // Convert the stream to a vector with timeout
            let results: Result<Vec<RecordBatch>, _> = tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    Err(DataFusionError::Execution("Timed out waiting for query results".to_string()))
                }
                result = query_result.data.try_collect::<Vec<_>>() => {
                    result
                }
            };
            let results = results.expect("Query to native_on_registration_mixed should not return an error");
            assert_eq!(results.len(), 1, "Query should return 1 record batch");
            assert_eq!(results[0].num_rows(), 5, "Should have 5 rows of data");

            // But queries to federated_on_load_mixed should fail because it's not ready yet
            let query_result = tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for query to complete"));
                }
                result = rt
                    .datafusion()
                    .query_builder("SELECT * FROM federated_on_load_mixed")
                    .build()
                    .run() => {result}
            };
            let results = query_result.expect_err("Query should return an error - the acceleration should still be loading data");
            assert!(results.to_string().contains("Acceleration not ready; loading initial data for federated_on_load_mixed"));

            // Wait for acceleration to load for the second dataset
            tokio::time::sleep(std::time::Duration::from_secs(6)).await;

            // Now queries to federated_on_load_mixed should work
            let query_result = rt
                .datafusion()
                .query_builder("SELECT * FROM federated_on_load_mixed")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;

            // Convert the stream to a vector
            let results: Vec<RecordBatch> = query_result.data.try_collect::<Vec<RecordBatch>>().await.expect("Query should not return an error");
            
            assert_eq!(results.len(), 1, "Query should return 1 record batch");
            assert_eq!(results[0].num_rows(), 5, "Should have 5 rows of data");

            // Make sure the first dataset still works
            let query_result = rt
                .datafusion()
                .query_builder("SELECT * FROM native_on_registration_mixed")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;

            // Convert the stream to a vector
            let results: Vec<RecordBatch> = query_result.data.try_collect::<Vec<RecordBatch>>().await.expect("Query should not return an error");
            
            assert_eq!(results.len(), 1, "Query should return 1 record batch");
            assert_eq!(results[0].num_rows(), 5, "Should have 5 rows of data");

            Ok(())
        })
        .await
}

// Test both native and federated providers together with different ready states
#[cfg(feature = "duckdb")]
#[tokio::test]
async fn test_ready_state_mixed_duckdb_acceleration() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    register_slow_loading_providers().await;

    let request_context = Arc::new(RequestContext::builder(Protocol::Http).build());
    request_context
        .scope(async {
            let app = AppBuilder::new("ready_state_mixed_tests")
                .with_dataset(get_native_dataset(
                    "native_on_registration_mixed",
                    ReadyState::OnRegistration,
                    Some("duckdb".to_string()),
                ))
                .with_dataset(get_federated_dataset(
                    "federated_on_load_mixed",
                    ReadyState::OnLoad,
                    Some("duckdb".to_string()),
                ))
                .build();

            let status = status::RuntimeStatus::new();
            let df = get_test_datafusion(Arc::clone(&status));

            let rt = Runtime::builder()
                .with_datafusion(df)
                .with_app(app)
                .build()
                .await;

            tracing::info!("Loading components");
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = rt.load_components() => {}
            }

            // Queries to native_on_registration_mixed should work right away
            let query_result = rt
                .datafusion()
                .query_builder("SELECT * FROM native_on_registration_mixed")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;

            // Convert the stream to a vector with timeout
            let results: Result<Vec<RecordBatch>, _> = tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    Err(DataFusionError::Execution("Timed out waiting for query results".to_string()))
                }
                result = query_result.data.try_collect::<Vec<_>>() => {
                    result
                }
            };
            let results = results.expect("Query to native_on_registration_mixed should not return an error");
            assert_eq!(results.len(), 1, "Query should return 1 record batch");
            assert_eq!(results[0].num_rows(), 5, "Should have 5 rows of data");

            // But queries to federated_on_load_mixed should fail because it's not ready yet
            let query_result = tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for query to complete"));
                }
                result = rt
                    .datafusion()
                    .query_builder("SELECT * FROM federated_on_load_mixed")
                    .build()
                    .run() => {result}
            };
            let results = query_result.expect_err("Query should return an error - the acceleration should still be loading data");
            assert!(results.to_string().contains("Acceleration not ready; loading initial data for federated_on_load_mixed"));

            // Wait for acceleration to load for the second dataset
            tokio::time::sleep(std::time::Duration::from_secs(6)).await;

            // Now queries to federated_on_load_mixed should work
            let query_result = rt
                .datafusion()
                .query_builder("SELECT * FROM federated_on_load_mixed")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;

            // Convert the stream to a vector
            let results: Vec<RecordBatch> = query_result.data.try_collect::<Vec<RecordBatch>>().await.expect("Query should not return an error");
            
            assert_eq!(results.len(), 1, "Query should return 1 record batch");
            assert_eq!(results[0].num_rows(), 5, "Should have 5 rows of data");

            // Make sure the first dataset still works
            let query_result = rt
                .datafusion()
                .query_builder("SELECT * FROM native_on_registration_mixed")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;

            // Convert the stream to a vector
            let results: Vec<RecordBatch> = query_result.data.try_collect::<Vec<RecordBatch>>().await.expect("Query should not return an error");
            
            assert_eq!(results.len(), 1, "Query should return 1 record batch");
            assert_eq!(results[0].num_rows(), 5, "Should have 5 rows of data");

            Ok(())
        })
        .await
}
