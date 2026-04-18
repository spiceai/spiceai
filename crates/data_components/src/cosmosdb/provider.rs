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
//! This is an alpha-quality implementation:
//! * Read-only (no INSERT / UPDATE / DELETE).
//! * Cross-partition scan only — no filter or projection push-down.
//! * Schema inferred from a sample; pin the schema via the dataset
//!   `columns:` spicepod property when stability is required.
//! * Cosmos DB Rust SDK 0.30 has limited cross-partition capabilities; see
//!   the module-level documentation.

use std::any::Any;
use std::io::Cursor;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
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
use futures::StreamExt;
use serde_json::Value;
use snafu::ResultExt;

use super::client::CosmosDBClient;
use super::schema::{infer_schema, strip_system_fields};
use super::{
    DEFAULT_SCHEMA_INFER_MAX_RECORDS, EmptyContainerSnafu, Error, JsonDecodeSnafu, QueryFailedSnafu,
};

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
    /// Optional pre-pinned schema. If supplied, schema inference is skipped
    /// entirely.
    pub schema_override: Option<SchemaRef>,
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
            schema_override: None,
        }
    }

    #[must_use]
    pub fn with_schema_infer_max_records(mut self, n: usize) -> Self {
        self.schema_infer_max_records = n;
        self
    }

    #[must_use]
    pub fn with_schema_override(mut self, schema: SchemaRef) -> Self {
        self.schema_override = Some(schema);
        self
    }
}

/// Arrow [`TableProvider`] backed by an Azure Cosmos DB container.
#[derive(Debug, Clone)]
pub struct CosmosDBTableProvider {
    client: CosmosDBClient,
    config: Arc<CosmosDBTableProviderConfig>,
    schema: SchemaRef,
}

impl CosmosDBTableProvider {
    /// Build a new table provider, sampling the container for a schema if
    /// no override was supplied.
    ///
    /// # Errors
    /// Returns an error if the sample query fails or the container is empty
    /// and no schema override was supplied.
    pub async fn try_new(
        client: CosmosDBClient,
        config: CosmosDBTableProviderConfig,
    ) -> Result<Self, Error> {
        let schema = if let Some(schema) = &config.schema_override {
            Arc::clone(schema)
        } else {
            let samples = fetch_samples(
                &client,
                &config.database,
                &config.container,
                &config.query,
                config.schema_infer_max_records,
            )
            .await?;

            if samples.is_empty() {
                return EmptyContainerSnafu {
                    database: config.database.clone(),
                    container: config.container.clone(),
                }
                .fail();
            }

            infer_schema(&samples)?
        };

        Ok(Self {
            client,
            config: Arc::new(config),
            schema,
        })
    }

    #[must_use]
    pub fn schema_ref(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Sample up to `limit` documents from the container for schema inference.
async fn fetch_samples(
    client: &CosmosDBClient,
    database: &str,
    container: &str,
    query: &str,
    limit: usize,
) -> Result<Vec<Value>, Error> {
    let container_client = client.container_client(database, container);

    // `()` = cross-partition query; see CosmosClient::query_items docs.
    let mut pager = container_client
        .query_items::<Value>(query, (), None)
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        .context(QueryFailedSnafu {
            database: database.to_string(),
            container: container.to_string(),
        })?;

    let mut samples = Vec::with_capacity(limit.min(1024));
    while samples.len() < limit {
        match pager.next().await {
            Some(Ok(doc)) => samples.push(strip_system_fields(doc)),
            Some(Err(e)) => {
                return Err(Error::QueryFailed {
                    database: database.to_string(),
                    container: container.to_string(),
                    source: Box::new(e),
                });
            }
            None => break,
        }
    }

    Ok(samples)
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
            self.client.clone(),
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
    client: CosmosDBClient,
    config: Arc<CosmosDBTableProviderConfig>,
    /// Full (un-projected) schema used when decoding JSON.
    full_schema: SchemaRef,
    /// Schema presented to `DataFusion` after projection.
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    properties: PlanProperties,
}

impl CosmosDBExec {
    fn new(
        client: CosmosDBClient,
        config: Arc<CosmosDBTableProviderConfig>,
        full_schema: SchemaRef,
        projected_schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&projected_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            client,
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

    fn properties(&self) -> &PlanProperties {
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
        let mut builder =
            RecordBatchReceiverStream::builder(Arc::clone(&self.projected_schema), 2);
        let tx = builder.tx();

        let client = self.client.clone();
        let config = Arc::clone(&self.config);
        let full_schema = Arc::clone(&self.full_schema);
        let projection = self.projection.clone();

        builder.spawn(async move {
            let container_client = client.container_client(&config.database, &config.container);

            let mut pager = container_client
                .query_items::<Value>(config.query.as_str(), (), None)
                .map_err(|e| {
                    DataFusionError::External(
                        Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                    )
                })?;

            let mut buffer: Vec<Value> = Vec::with_capacity(STREAM_BATCH_SIZE);

            while let Some(item) = pager.next().await {
                let doc = item.map_err(|e| {
                    DataFusionError::External(
                        Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                    )
                })?;

                buffer.push(strip_system_fields(doc));

                if buffer.len() >= STREAM_BATCH_SIZE {
                    let batch =
                        decode_batch(&buffer, &full_schema, projection.as_deref())
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
    // `arrow::json::ReaderBuilder` consumes newline-delimited JSON, so join
    // the document stream with newlines.
    let mut buf = Vec::with_capacity(docs.len() * 256);
    for doc in docs {
        serde_json::to_writer(&mut buf, doc).map_err(|e| Error::JsonDecode {
            source: arrow::error::ArrowError::JsonError(e.to_string()),
        })?;
        buf.push(b'\n');
    }

    let reader = ReaderBuilder::new(Arc::clone(full_schema))
        .with_batch_size(docs.len().max(1))
        .build(Cursor::new(buf))
        .context(JsonDecodeSnafu)?;

    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.context(JsonDecodeSnafu)?);
    }

    let full_batch = if batches.len() == 1 {
        batches.pop().unwrap_or_else(|| {
            RecordBatch::new_empty(Arc::clone(full_schema))
        })
    } else if batches.is_empty() {
        RecordBatch::new_empty(Arc::clone(full_schema))
    } else {
        arrow::compute::concat_batches(full_schema, &batches).context(JsonDecodeSnafu)?
    };

    if let Some(indices) = projection {
        full_batch
            .project(indices)
            .context(JsonDecodeSnafu)
    } else {
        Ok(full_batch)
    }
}

fn to_df_error(e: Error) -> DataFusionError {
    DataFusionError::External(Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
}
