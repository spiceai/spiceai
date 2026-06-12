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

//! Arrow [`TableProvider`] implementation for an Azure Cosmos DB `NoSQL`
//! container.
//!
//! The provider executes a single Cosmos SQL query (defaulting to
//! `SELECT * FROM c`) against the configured container, infers an Arrow
//! schema from a sample of documents on first access, and streams the full
//! result set into record batches via `arrow::json::ReaderBuilder`.
//!
//! This is an RC-quality implementation with the following current
//! limitations:
//! * Read-only (no INSERT / UPDATE / DELETE).
//! * Cross-partition scan only — no filter or projection push-down.
//! * Schema inferred from a sample; pin the schema via the dataset
//!   `columns:` spicepod property when stability is required.
//! * Retries/backoff apply to the schema-inference pass only; mid-stream
//!   pager errors during scan execution propagate directly.
//! * Cosmos DB Rust SDK 0.30 has limited cross-partition capabilities; see
//!   the module-level documentation.

use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::json::ReaderBuilder;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{Result as DataFusionResult, project_schema};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
};
use datafusion::prelude::Expr;
use datafusion_table_providers::UnsupportedTypeAction;
use futures::StreamExt;
use serde_json::Value;
use snafu::ResultExt;

use azure_data_cosmos::clients::ContainerClient;

use super::resilience::{CosmosResilienceConfig, ResilienceError, run_with_resilience};
use super::schema::{infer_schema, strip_system_fields};
use super::{DEFAULT_SCHEMA_INFER_MAX_RECORDS, EmptyContainerSnafu, Error, JsonDecodeSnafu};

/// Number of documents emitted per `RecordBatch` when streaming results.
const STREAM_BATCH_SIZE: usize = 1024;

/// Configuration for a single Cosmos DB dataset.
#[derive(Debug, Clone)]
pub struct CosmosDBTableProviderConfig {
    pub database: String,
    pub container: String,
    /// Cosmos SQL query to execute. Defaults to `SELECT * FROM c`.
    pub query: String,
    /// Number of documents sampled when inferring the schema.
    pub schema_infer_max_records: usize,
    /// How to handle columns whose type Cosmos DB cannot represent (e.g.
    /// all-null samples that Arrow's JSON inference returns as
    /// [`DataType::Null`]). Defaults to [`UnsupportedTypeAction::Warn`].
    pub unsupported_type_action: UnsupportedTypeAction,
    pub resilience: CosmosResilienceConfig,
}

impl CosmosDBTableProviderConfig {
    #[must_use]
    pub fn new(
        database: impl Into<String>,
        container: impl Into<String>,
        query: impl Into<String>,
    ) -> Self {
        Self {
            database: database.into(),
            container: container.into(),
            query: query.into(),
            schema_infer_max_records: DEFAULT_SCHEMA_INFER_MAX_RECORDS,
            unsupported_type_action: UnsupportedTypeAction::Warn,
            resilience: CosmosResilienceConfig::default(),
        }
    }

    #[must_use]
    pub fn with_schema_infer_max_records(mut self, n: usize) -> Self {
        self.schema_infer_max_records = n;
        self
    }

    #[must_use]
    pub fn with_resilience(mut self, resilience: CosmosResilienceConfig) -> Self {
        self.resilience = resilience;
        self
    }

    #[must_use]
    pub fn with_unsupported_type_action(mut self, action: UnsupportedTypeAction) -> Self {
        self.unsupported_type_action = action;
        self
    }
}

/// Arrow [`TableProvider`] backed by an Azure Cosmos DB container.
#[derive(Clone)]
pub struct CosmosDBTableProvider {
    container_client: ContainerClient,
    endpoint: Arc<str>,
    config: Arc<CosmosDBTableProviderConfig>,
    schema: SchemaRef,
}

impl std::fmt::Debug for CosmosDBTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CosmosDBTableProvider")
            .field("endpoint", &self.endpoint)
            .field("config", &self.config)
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl CosmosDBTableProvider {
    /// Build a new table provider, inferring the schema by sampling a batch
    /// of documents from the container.
    ///
    /// `container_client` is pre-built for the `(database, container)` pair
    /// carried on `config`; `endpoint` is the Cosmos account endpoint used for
    /// resilience keying and error messages.
    ///
    /// # Errors
    /// Returns an error if the sample query fails or the container is empty.
    pub async fn try_new(
        container_client: ContainerClient,
        endpoint: Arc<str>,
        config: CosmosDBTableProviderConfig,
    ) -> Result<Self, Error> {
        let samples = fetch_samples(
            &container_client,
            &endpoint,
            &config.database,
            &config.container,
            &config.query,
            config.schema_infer_max_records,
            &config.resilience,
        )
        .await?;

        if samples.is_empty() {
            return EmptyContainerSnafu {
                database: config.database.clone(),
                container: config.container.clone(),
            }
            .fail();
        }

        let inferred = infer_schema(&samples)?;
        let schema = apply_unsupported_type_action(
            &inferred,
            config.unsupported_type_action,
            &config.database,
            &config.container,
        )?;

        Ok(Self {
            container_client,
            endpoint,
            config: Arc::new(config),
            schema,
        })
    }

    #[must_use]
    pub fn schema_ref(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Apply the configured [`UnsupportedTypeAction`] to an inferred schema.
///
/// For Cosmos DB, the only type Arrow's JSON inference can produce that
/// downstream query engines may refuse is [`DataType::Null`] — it appears when
/// every sampled document has `null` for a field.
fn apply_unsupported_type_action(
    inferred: &SchemaRef,
    action: UnsupportedTypeAction,
    database: &str,
    container: &str,
) -> Result<SchemaRef, Error> {
    if !schema_has_unsupported_columns(inferred) {
        return Ok(Arc::clone(inferred));
    }

    let mut kept: Vec<Arc<Field>> = Vec::with_capacity(inferred.fields().len());
    for field in inferred.fields() {
        if is_unsupported_cosmos_field(field) {
            match action {
                UnsupportedTypeAction::Error => {
                    return Err(Error::UnsupportedColumn {
                        database: database.to_string(),
                        container: container.to_string(),
                        column: field.name().clone(),
                        data_type: format!("{:?}", field.data_type()),
                    });
                }
                UnsupportedTypeAction::Warn => {
                    tracing::warn!(
                        database = %database,
                        container = %container,
                        column = %field.name(),
                        data_type = %format!("{:?}", field.data_type()),
                        "Dropping column '{}' from Cosmos DB dataset {database}.{container}: Arrow inferred an unsupported data type ({:?}). All sampled documents were null for this field — populate the field or pin a schema via `columns:` to override.",
                        field.name(),
                        field.data_type()
                    );
                }
                UnsupportedTypeAction::Ignore => {
                    // Silently drop the column.
                }
                UnsupportedTypeAction::String => {
                    kept.push(Arc::new(Field::new(
                        field.name(),
                        DataType::Utf8,
                        field.is_nullable(),
                    )));
                }
            }
        } else {
            kept.push(Arc::<Field>::clone(field));
        }
    }

    Ok(Arc::new(Schema::new(kept)))
}

fn is_unsupported_cosmos_field(field: &Arc<Field>) -> bool {
    matches!(field.data_type(), DataType::Null)
}

fn schema_has_unsupported_columns(schema: &SchemaRef) -> bool {
    schema.fields().iter().any(is_unsupported_cosmos_field)
}

/// Sample up to `limit` documents from the container for schema inference.
///
/// Wrapped by [`run_with_resilience`] so the whole sampling operation is
/// retried (with fresh pager construction) on transient errors, bounded by
/// the configured retry budget.
async fn fetch_samples(
    container_client: &ContainerClient,
    endpoint: &str,
    database: &str,
    container: &str,
    query: &str,
    limit: usize,
    resilience: &CosmosResilienceConfig,
) -> Result<Vec<Value>, Error> {
    run_with_resilience(resilience, endpoint, || async {
        let mut pager = container_client.query_items::<Value>(query, (), None)?;
        let mut samples = Vec::with_capacity(limit.min(1024));
        while samples.len() < limit {
            match pager.next().await {
                Some(Ok(doc)) => samples.push(strip_system_fields(doc)),
                Some(Err(e)) => return Err(e),
                None => break,
            }
        }
        Ok(samples)
    })
    .await
    .map_err(|e| match e {
        ResilienceError::Disabled => Error::ConnectorDisabled {
            endpoint: endpoint.to_string(),
        },
        ResilienceError::Request(source) => Error::QueryFailed {
            database: database.to_string(),
            container: container.to_string(),
            source: Box::new(source),
        },
    })
}

#[async_trait]
impl TableProvider for CosmosDBTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let projected_schema = project_schema(&self.schema, projection)?;

        Ok(Arc::new(CosmosDBExec::new(
            self.container_client.clone(),
            Arc::clone(&self.endpoint),
            Arc::clone(&self.config),
            Arc::clone(&self.schema),
            projected_schema,
            projection.cloned(),
        )))
    }
}

/// [`ExecutionPlan`] that streams documents from a Cosmos DB container and
/// converts them into Arrow record batches.
struct CosmosDBExec {
    container_client: ContainerClient,
    endpoint: Arc<str>,
    config: Arc<CosmosDBTableProviderConfig>,
    /// Full (un-projected) schema used when decoding JSON.
    full_schema: SchemaRef,
    /// Schema presented to `DataFusion` after projection.
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    properties: Arc<PlanProperties>,
}

impl CosmosDBExec {
    fn new(
        container_client: ContainerClient,
        endpoint: Arc<str>,
        config: Arc<CosmosDBTableProviderConfig>,
        full_schema: SchemaRef,
        projected_schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&projected_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            container_client,
            endpoint,
            config,
            full_schema,
            projected_schema,
            projection,
            properties,
        }
    }
}

impl std::fmt::Debug for CosmosDBExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("CosmosDBExec")
            .field("database", &self.config.database)
            .field("container", &self.config.container)
            .field("query", &self.config.query)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for CosmosDBExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "CosmosDBExec: database={}, container={}, query={}",
            self.config.database, self.config.container, self.config.query
        )
    }
}

impl ExecutionPlan for CosmosDBExec {
    fn name(&self) -> &'static str {
        "CosmosDBExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let mut builder = RecordBatchReceiverStream::builder(Arc::clone(&self.projected_schema), 2);
        let tx = builder.tx();

        let container_client = self.container_client.clone();
        let endpoint = Arc::clone(&self.endpoint);
        let config = Arc::clone(&self.config);
        let full_schema = Arc::clone(&self.full_schema);
        let projection = self.projection.clone();

        builder.spawn(async move {
            if config.resilience.disabled.load(Ordering::Acquire) {
                return Err(to_df_error(Error::ConnectorDisabled {
                    endpoint: endpoint.to_string(),
                }));
            }

            // Permit + inflight guard are held as `_`-bindings so they release
            // automatically when the async block returns — including on
            // cancellation or receiver-drop mid-stream.
            let _permit = match &config.resilience.semaphore {
                Some(s) => Some(
                    Arc::<tokio::sync::Semaphore>::clone(s)
                        .acquire_owned()
                        .await
                        .map_err(|_| {
                            to_df_error(Error::ConnectorDisabled {
                                endpoint: endpoint.to_string(),
                            })
                        })?,
                ),
                None => None,
            };
            let _inflight = crate::cosmosdb::resilience::InflightGuard::enter(
                Arc::<AtomicU64>::clone(&config.resilience.inflight),
            );

            let handle_stream_error = |resilience: &CosmosResilienceConfig,
                                       endpoint: &str,
                                       err: azure_core::Error|
             -> DataFusionError {
                if crate::cosmosdb::resilience::is_permanent_error(&err)
                    && resilience.disable_on_permanent_error
                {
                    resilience.disabled.store(true, Ordering::Release);
                    tracing::error!(
                        endpoint = %endpoint,
                        "Permanent error from Azure Cosmos DB; disabling connector. {err}"
                    );
                }
                to_df_error(Error::QueryFailed {
                    database: config.database.clone(),
                    container: config.container.clone(),
                    source: Box::new(err),
                })
            };

            let mut pager = container_client
                .query_items::<Value>(config.query.as_str(), (), None)
                .map_err(|e| handle_stream_error(&config.resilience, &endpoint, e))?;

            let mut buffer: Vec<Value> = Vec::with_capacity(STREAM_BATCH_SIZE);

            while let Some(item) = pager.next().await {
                let doc =
                    item.map_err(|e| handle_stream_error(&config.resilience, &endpoint, e))?;

                buffer.push(strip_system_fields(doc));

                if buffer.len() >= STREAM_BATCH_SIZE {
                    let batch = decode_batch(&buffer, &full_schema, projection.as_deref())
                        .map_err(to_df_error)?;
                    buffer.clear();
                    if tx.send(Ok(batch)).await.is_err() {
                        // Receiver dropped; stop scanning.
                        return Ok(());
                    }
                }
            }

            if !buffer.is_empty() {
                let batch = decode_batch(&buffer, &full_schema, projection.as_deref())
                    .map_err(to_df_error)?;
                let _ = tx.send(Ok(batch)).await;
            }

            Ok::<_, DataFusionError>(())
        });

        Ok(builder.build())
    }
}

fn decode_batch(
    docs: &[Value],
    full_schema: &SchemaRef,
    projection: Option<&[usize]>,
) -> Result<RecordBatch, Error> {
    // Hand the Value slice directly to arrow-json's serde-aware decoder,
    // avoiding the NDJSON serialize -> parse round-trip.
    let mut decoder = ReaderBuilder::new(Arc::clone(full_schema))
        .build_decoder()
        .context(JsonDecodeSnafu)?;

    if !docs.is_empty() {
        decoder.serialize(docs).context(JsonDecodeSnafu)?;
    }

    let full_batch = decoder
        .flush()
        .context(JsonDecodeSnafu)?
        .unwrap_or_else(|| RecordBatch::new_empty(Arc::clone(full_schema)));

    if let Some(indices) = projection {
        full_batch.project(indices).context(JsonDecodeSnafu)
    } else {
        Ok(full_batch)
    }
}

fn to_df_error(e: Error) -> DataFusionError {
    DataFusionError::External(Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use serde_json::json;

    fn sample_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("count", DataType::Int64, true),
        ]))
    }

    #[test]
    fn decode_batch_decodes_multiple_documents() {
        let schema = sample_schema();
        let docs = vec![
            json!({"id": "a", "count": 1}),
            json!({"id": "b", "count": 2}),
            json!({"id": "c", "count": 3}),
        ];
        let batch = decode_batch(&docs, &schema, None).expect("decode_batch failed");
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);

        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("column 0 should be StringArray");
        assert_eq!(id_col.value(0), "a");
        assert_eq!(id_col.value(2), "c");

        let count_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("column 1 should be Int64Array");
        assert_eq!(count_col.value(0), 1);
        assert_eq!(count_col.value(2), 3);
    }

    #[test]
    fn decode_batch_applies_projection() {
        let schema = sample_schema();
        let docs = vec![json!({"id": "a", "count": 1})];
        // Project only the second column (`count`).
        let batch =
            decode_batch(&docs, &schema, Some(&[1])).expect("decode_batch with projection failed");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "count");
    }

    #[test]
    fn decode_batch_handles_empty_input() {
        let schema = sample_schema();
        let batch = decode_batch(&[], &schema, None).expect("decode_batch on empty input failed");
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn decode_batch_fills_missing_fields_with_null() {
        // Cosmos documents are schemaless — some docs may omit fields the
        // inferred schema includes. Those cells must surface as nulls.
        let schema = sample_schema();
        let docs = vec![json!({"id": "a"}), json!({"id": "b", "count": 2})];
        let batch = decode_batch(&docs, &schema, None).expect("decode_batch failed");
        let count_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("column 1 should be Int64Array");
        assert!(count_col.is_null(0));
        assert_eq!(count_col.value(1), 2);
    }

    /// Beta criterion: the connector must handle datasets whose column count
    /// matches the source limit. Cosmos DB has no formal column cap, but
    /// production tenants routinely store 1024+ top-level fields. Build a
    /// synthetic schema + document of that size and verify end-to-end
    /// JSON-to-Arrow decoding does not OOM or regress.
    #[test]
    fn decode_batch_handles_wide_schema() {
        const COLS: usize = 1024;
        let fields: Vec<Field> = (0..COLS)
            .map(|i| Field::new(format!("col_{i}"), DataType::Int64, true))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        let mut obj = serde_json::Map::with_capacity(COLS);
        for i in 0..COLS {
            obj.insert(format!("col_{i}"), json!(i64::try_from(i).unwrap_or(0)));
        }
        let docs = vec![Value::Object(obj)];

        let batch = decode_batch(&docs, &schema, None).expect("decode_batch on wide schema failed");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), COLS);
        let mid = batch
            .column(COLS / 2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("middle column should be Int64Array");
        assert_eq!(mid.value(0), i64::try_from(COLS / 2).unwrap_or(0));
    }

    fn schema_with_null_column() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("always_null", DataType::Null, true),
        ]))
    }

    #[test]
    fn unsupported_type_action_warn_drops_null_columns() {
        let schema = schema_with_null_column();
        let projected =
            apply_unsupported_type_action(&schema, UnsupportedTypeAction::Warn, "db", "container")
                .expect("Warn action should succeed");
        assert_eq!(projected.fields().len(), 1);
        assert_eq!(projected.field(0).name(), "id");
    }

    #[test]
    fn unsupported_type_action_ignore_drops_silently() {
        let schema = schema_with_null_column();
        let projected = apply_unsupported_type_action(
            &schema,
            UnsupportedTypeAction::Ignore,
            "db",
            "container",
        )
        .expect("Ignore action should succeed");
        assert_eq!(projected.fields().len(), 1);
    }

    #[test]
    fn unsupported_type_action_string_coerces_to_utf8() {
        let schema = schema_with_null_column();
        let projected = apply_unsupported_type_action(
            &schema,
            UnsupportedTypeAction::String,
            "db",
            "container",
        )
        .expect("String action should succeed");
        assert_eq!(projected.fields().len(), 2);
        assert_eq!(projected.field(1).data_type(), &DataType::Utf8);
    }

    #[test]
    fn unsupported_type_action_error_surfaces_to_caller() {
        let schema = schema_with_null_column();
        let err =
            apply_unsupported_type_action(&schema, UnsupportedTypeAction::Error, "db", "container")
                .expect_err("Error action should fail on unsupported column");
        assert!(matches!(err, Error::UnsupportedColumn { .. }));
    }

    #[test]
    fn unsupported_type_action_is_noop_on_clean_schema() {
        let schema = sample_schema();
        let projected =
            apply_unsupported_type_action(&schema, UnsupportedTypeAction::Error, "db", "container")
                .expect("Error action on clean schema should succeed");
        assert!(Arc::ptr_eq(&schema, &projected));
    }
}
