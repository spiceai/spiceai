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

use arrow::{
    array::{Array, RecordBatch},
    datatypes::{Field, Schema, SchemaRef},
    ipc::reader::StreamReader,
};
use async_trait::async_trait;
use datafusion::{
    datasource::TableProvider,
    error::DataFusionError,
    execution::SendableRecordBatchStream,
    physical_plan::stream::RecordBatchStreamAdapter,
    sql::{TableReference, unparser::dialect},
};
use datafusion_table_providers::sql::{
    db_connection_pool::{
        DbConnectionPool, JoinPushDown,
        dbconnection::{self, AsyncDbConnection, DbConnection},
    },
    sql_provider_datafusion::SqlTable,
};
use futures::{Stream, StreamExt, TryStreamExt, stream};
use reqwest::{Client, ClientBuilder};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use snafu::{Snafu, prelude::*};
use std::{
    fmt::{Display, Formatter},
    io::Cursor,
    pin::Pin,
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use token_provider::TokenProvider;
use util::fibonacci_backoff::{Backoff, FibonacciBackoffBuilder};

mod datatypes;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Not implemented"))]
    NotImplemented,

    #[snafu(display("HTTP client build failed: {source}"))]
    ClientBuildFailed { source: reqwest::Error },

    #[snafu(display("Databricks datatype {ty} not supported"))]
    UnsupportedType { ty: String },

    #[snafu(display("Unable to retrieve schema: {reason}"))]
    UnableToRetrieveSchema { reason: String },

    #[snafu(display(
        "Warehouse is not ready (state: '{state}'). Verify the warehouse state and try again later."
    ))]
    InvalidWarehouseState { state: String },

    #[snafu(display("Unexpected Statement execution state: '{state}'."))]
    UnexpectedStatementState { state: String },

    #[snafu(display("Query canceled or timed out (state: 'CANCELED')."))]
    QueryCanceled,

    #[snafu(display("Long-running operations are not supported (state: 'RUNNING')."))]
    QueryStillRunning,

    #[snafu(display("HTTP request failed: {source}"))]
    HttpRequestFailed { source: reqwest::Error },

    #[snafu(display("JSON parsing failed: {source}"))]
    JsonParsingFailed { source: reqwest::Error },

    #[snafu(display("Missing JSON field: {field}"))]
    MissingJsonField { field: String },

    #[snafu(display("Invalid JSON array: {field}"))]
    InvalidJsonArray { field: String },

    #[snafu(display("Failed to deserialize external link: {source}"))]
    DeserializeExternalLinkFailed { source: serde_json::Error },

    #[snafu(display("Failed to read Arrow stream: {source}"))]
    ArrowStreamReadFailed { source: arrow::error::ArrowError },

    #[snafu(display("Failed to create table provider: {source}"))]
    TableProviderCreationFailed { source: DataFusionError },

    #[snafu(display("Failed to initialize SQL table: {source}"))]
    SqlTableInitializationFailed {
        source: datafusion_table_providers::sql::sql_provider_datafusion::Error,
    },

    #[snafu(display("A fully-qualified path is required: {reason}"))]
    FullyQualifiedPath { reason: String },

    #[snafu(display("Failed to parse Databricks datatype: {reason}"))]
    ParseError { reason: String },

    #[snafu(display(
        "Failed to execute the query. {message} Verify the query is valid, or report a bug at: https://github.com/spiceai/spiceai/issues"
    ))]
    QueryFailure { message: String },
}

/// Main struct for interacting with Databricks SQL Warehouse
pub struct DatabricksSqlWarehouse {
    pool: Arc<dyn DbConnectionPool<Arc<SqlWarehouseApi>, &'static dyn Sync> + Send + Sync>,
}

impl DatabricksSqlWarehouse {
    /// Creates a new Databricks SQL Warehouse instance
    pub fn new(
        endpoint: &str,
        sql_warehouse_id: &str,
        token_provider: Arc<dyn TokenProvider>,
    ) -> Result<Self, Error> {
        let api = Arc::new(SqlWarehouseApi::new(
            endpoint,
            sql_warehouse_id,
            token_provider,
        )?);
        let pool = Arc::new(SqlWarehouseConnectionPool { api });
        Ok(Self { pool })
    }
}

struct SqlWarehouseConnectionPool {
    api: Arc<SqlWarehouseApi>,
}

#[async_trait]
impl DbConnectionPool<Arc<SqlWarehouseApi>, &'static dyn Sync> for SqlWarehouseConnectionPool {
    async fn connect(
        &self,
    ) -> Result<
        Box<dyn DbConnection<Arc<SqlWarehouseApi>, &'static dyn Sync>>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        Ok(Box::new(SqlWarehouseConnection {
            api: Arc::clone(&self.api),
        }))
    }

    fn join_push_down(&self) -> JoinPushDown {
        JoinPushDown::AllowedFor(self.api.sql_warehouse_id.clone())
    }
}

// https://docs.databricks.com/api/workspace/statementexecution/executestatement#status-error
// states: Enum: PENDING | RUNNING | SUCCEEDED | FAILED | CANCELED | CLOSED
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum ResponseStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Canceled,
    Closed,
}

impl Display for ResponseStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ResponseStatus::Pending => write!(f, "PENDING"),
            ResponseStatus::Running => write!(f, "RUNNING"),
            ResponseStatus::Succeeded => write!(f, "SUCCEEDED"),
            ResponseStatus::Failed => write!(f, "FAILED"),
            ResponseStatus::Canceled => write!(f, "CANCELED"),
            ResponseStatus::Closed => write!(f, "CLOSED"),
        }
    }
}

impl FromStr for ResponseStatus {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "SUCCEEDED" => Ok(Self::Succeeded),
            "FAILED" => Ok(Self::Failed),
            // waiting for warehouse or async query
            "PENDING" => Ok(Self::Pending),
            "RUNNING" => Ok(Self::Running),
            "CANCELED" => Ok(Self::Canceled),
            "CLOSED" => Ok(Self::Closed),
            other => Err(Error::UnexpectedStatementState {
                state: other.to_string(),
            }),
        }
    }
}

struct SqlWarehouseApi {
    client: Client,
    host: String,
    sql_warehouse_id: String,
    token_provider: Arc<dyn TokenProvider>,
}

impl SqlWarehouseApi {
    fn new(
        host: &str,
        sql_warehouse_id: &str,
        token_provider: Arc<dyn TokenProvider>,
    ) -> Result<Self, Error> {
        let client = ClientBuilder::new()
            .user_agent(super::user_agent())
            .build()
            .context(ClientBuildFailedSnafu)?;

        Ok(Self {
            client,
            host: host.to_string(),
            sql_warehouse_id: sql_warehouse_id.to_string(),
            token_provider,
        })
    }

    async fn get_schema(&self, table: &TableReference) -> Result<SchemaRef, Error> {
        let token = self.token_provider.get_token();
        let payload = self.create_schema_payload(table)?;
        let response = self.execute_sql_statement(&token, &payload).await?;
        schema_from_json(&response)
    }

    fn create_schema_payload(&self, table: &TableReference) -> Result<Value, Error> {
        let table_schema = table.schema().ok_or_else(|| Error::FullyQualifiedPath {
            reason: "missing schema".into(),
        })?;
        let table_catalog = table.catalog().ok_or_else(|| Error::FullyQualifiedPath {
            reason: "missing catalog".into(),
        })?;
        // Escape single quotes by doubling them to prevent SQL injection
        let escaped_table = table.table().replace('\'', "''");
        let escaped_schema = table_schema.replace('\'', "''");
        let escaped_catalog = table_catalog.replace('\'', "''");
        let sql = format!(
            "SELECT column_name, full_data_type, is_nullable FROM information_schema.columns WHERE table_name = '{escaped_table}' AND table_schema = '{escaped_schema}' AND table_catalog = '{escaped_catalog}'"
        );
        Ok(json!({
            "warehouse_id": self.sql_warehouse_id,
            "catalog": table_catalog,
            "schema": table_schema,
            "statement": sql,
        }))
    }

    async fn execute_sql_statement(&self, token: &str, payload: &Value) -> Result<Value, Error> {
        let url = format!("https://{}/api/2.0/sql/statements/", self.host);
        self.client
            .post(&url)
            .bearer_auth(token)
            .json(payload)
            .send()
            .await
            .context(HttpRequestFailedSnafu)?
            .error_for_status()
            .context(HttpRequestFailedSnafu)?
            .json()
            .await
            .context(JsonParsingFailedSnafu)
    }

    async fn get_sql_statement_status(
        &self,
        token: &str,
        statement_id: &str,
    ) -> Result<Value, Error> {
        let url = format!(
            "https://{}/api/2.0/sql/statements/{statement_id}",
            self.host
        );
        self.client
            .get(&url)
            .bearer_auth(token)
            .send()
            .await
            .context(HttpRequestFailedSnafu)?
            .error_for_status()
            .context(HttpRequestFailedSnafu)?
            .json()
            .await
            .context(JsonParsingFailedSnafu)
    }

    // Fetch the arrow data at the external links, repeating for each chunk
    async fn fetch_external_links(
        self: Arc<Self>,
        result_object: Value,
    ) -> Result<SendableRecordBatchStream, Error> {
        let token = self.token_provider.get_token();
        let initial_external_link = Self::extract_external_links(result_object)?;

        // If no external link, return an empty stream
        if initial_external_link.is_none() {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::new(Schema::empty()),
                Box::pin(stream::empty()),
            )));
        }

        let token = token.clone();
        let stream = stream::unfold(initial_external_link, move |current_link| {
            let api = Arc::clone(&self);
            let token = token.clone();
            async move {
                let link = current_link?;

                tracing::trace!(
                    "Fetching chunk {} from {}",
                    link.chunk_index,
                    link.external_link
                );

                let bytes = match api.fetch_chunk_data(&link.external_link).await {
                    Ok(bytes) => bytes,
                    Err(e) => return Some((Err(e), None)),
                };

                let batches = match Self::read_arrow_batches(bytes) {
                    Ok(batches) => batches,
                    Err(e) => return Some((Err(e), None)),
                };

                let next_link = match link.next_chunk_internal_link {
                    Some(path) => {
                        let url = format!("https://{}{path}", api.host);
                        match api
                            .client
                            .get(&url)
                            .bearer_auth(&token)
                            .send()
                            .await
                            .context(HttpRequestFailedSnafu)
                            .and_then(|resp| {
                                resp.error_for_status().context(HttpRequestFailedSnafu)
                            }) {
                            Ok(response) => match response
                                .json()
                                .await
                                .context(JsonParsingFailedSnafu)
                                .and_then(Self::extract_external_links)
                            {
                                Ok(next) => next,
                                Err(e) => return Some((Err(e), None)),
                            },
                            Err(e) => return Some((Err(e), None)),
                        }
                    }
                    None => None,
                };

                Some((Ok(batches), next_link))
            }
        });

        // Flatten the stream of Vec<RecordBatch> into individual RecordBatch items
        let batch_stream = stream.flat_map(|result| match result {
            Ok(batches) => Box::pin(futures::stream::iter(batches.into_iter().map(Ok)))
                as Pin<Box<dyn Stream<Item = Result<RecordBatch, Error>> + Send>>,
            Err(e) => Box::pin(futures::stream::iter(vec![Err(e)]))
                as Pin<Box<dyn Stream<Item = Result<RecordBatch, Error>> + Send>>,
        });

        // Handle the first batch to extract schema
        let mut batch_stream = batch_stream.boxed();
        let first_batch: RecordBatch = match batch_stream.next().await {
            Some(Ok(batch)) => batch,
            Some(Err(e)) => return Err(e),
            None => {
                return Ok(Box::pin(RecordBatchStreamAdapter::new(
                    Arc::new(Schema::empty()),
                    Box::pin(stream::empty()),
                )));
            }
        };

        let schema = first_batch.schema();
        let run_once = stream::once(async move { Ok(first_batch) });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            Box::pin(run_once.chain(batch_stream))
                .map_err(|e| DataFusionError::Execution(e.to_string())),
        )))
    }

    /// Deserializes the first [`ExternalLink`] in the `external_links` array, or None if missing or empty
    fn extract_external_links(mut response: Value) -> Result<Option<ExternalLink>, Error> {
        let Some(links) = response.get_mut("external_links").map(Value::take) else {
            return Ok(None);
        };

        let Value::Array(mut links) = links else {
            return Err(Error::InvalidJsonArray {
                field: "external_links".into(),
            });
        };

        // Return None if the array is empty
        let Some(link) = links.pop() else {
            return Ok(None);
        };

        serde_json::from_value(link)
            .context(DeserializeExternalLinkFailedSnafu)
            .map(Some)
    }

    async fn fetch_chunk_data(&self, url: &str) -> Result<bytes::Bytes, Error> {
        self.client
            .get(url)
            .send()
            .await
            .context(HttpRequestFailedSnafu)?
            .error_for_status()
            .context(HttpRequestFailedSnafu)?
            .bytes()
            .await
            .context(HttpRequestFailedSnafu)
    }

    fn read_arrow_batches(
        bytes: bytes::Bytes,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, Error> {
        let cursor = Cursor::new(bytes);
        let reader = StreamReader::try_new(cursor, None).context(ArrowStreamReadFailedSnafu)?;
        Ok(reader
            .collect::<Result<Vec<_>, _>>()
            .context(ArrowStreamReadFailedSnafu)?
            .into_iter()
            .filter(|batch| batch.num_rows() > 0)
            .collect())
    }

    fn extract_response_status(response: &Value) -> Result<ResponseStatus, Error> {
        let state = response
            .get("status")
            .and_then(|s| s.get("state"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::MissingJsonField {
                field: "status.state".to_string(),
            })?;
        ResponseStatus::from_str(state)
    }

    fn extract_statement_id(response: &Value) -> Result<String, Error> {
        response
            .get("statement_id")
            .and_then(|v| v.as_str())
            .map(ToString::to_string)
            .ok_or_else(|| Error::MissingJsonField {
                field: "statement_id".to_string(),
            })
    }

    /// This is an async query running on the Databricks SQL Warehouse
    fn is_async_query(state: ResponseStatus) -> bool {
        matches!(state, ResponseStatus::Pending | ResponseStatus::Running)
    }

    fn verify_response_status(response: &Value) -> Result<(), Error> {
        let state = Self::extract_response_status(response)?;

        match state {
            ResponseStatus::Succeeded => Ok(()),
            ResponseStatus::Failed => {
                let message = Self::extract_error_message(response)
                    .unwrap_or_else(|| "Unknown error".to_string());
                Err(Error::QueryFailure {
                    message: format!("Query failed with state FAILED: {message}"),
                })
            }
            // waiting for warehouse
            ResponseStatus::Pending => Err(Error::InvalidWarehouseState {
                state: state.to_string(),
            }),
            // long-running queries are not currently supported
            ResponseStatus::Running => Err(Error::QueryStillRunning),
            ResponseStatus::Canceled => Err(Error::QueryCanceled),
            ResponseStatus::Closed => Err(Error::QueryFailure {
                message: "Query failed with state CLOSED".to_string(),
            }),
        }
    }

    fn extract_error_message(response: &Value) -> Option<String> {
        response
            .get("status")
            .and_then(|s| s.get("error"))
            .and_then(|e| e.get("message"))
            .and_then(|m| m.as_str())
            .map(ToString::to_string)
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct ExternalLink {
    chunk_index: u64,
    #[expect(clippy::struct_field_names)]
    external_link: String,
    next_chunk_internal_link: Option<String>,
}

fn schema_from_json(json_value: &Value) -> Result<SchemaRef, Error> {
    tracing::trace!("Parsing schema definition from Databricks JSON response: {json_value}");

    SqlWarehouseApi::verify_response_status(json_value)?;

    let data_array = json_value
        .get("result")
        .and_then(|r| r.get("data_array"))
        .and_then(|d| d.as_array())
        .ok_or_else(|| Error::UnableToRetrieveSchema {
            reason: "result.data_array".to_string(),
        })?;

    let mut fields = Vec::new();

    for (i, row) in data_array.iter().enumerate() {
        let row_array = row
            .as_array()
            .ok_or_else(|| Error::UnableToRetrieveSchema {
                reason: format!("data_array[{i}] is not an array"),
            })?;

        if row_array.len() < 3 {
            return Err(Error::UnableToRetrieveSchema {
                reason: format!(
                    "data_array[{i}] lacks column_name or full_data_type or is_nullable"
                ),
            });
        }

        let col_name = row_array[0]
            .as_str()
            .ok_or_else(|| Error::UnableToRetrieveSchema {
                reason: format!("data_array[{i}][0] is not a string"),
            })?;

        // If we see #, DB is now providing the clustering metadata info and
        // we have all the columns we need
        if col_name.starts_with('#') {
            break;
        }

        let data_type_str = row_array[1]
            .as_str()
            .ok_or_else(|| Error::UnableToRetrieveSchema {
                reason: format!("data_array[{i}][1] is not a string"),
            })?;

        let data_type = datatypes::Parser::new(data_type_str)
            .parse()
            .map_err(|reason| Error::ParseError { reason })?;

        let nullable = row_array[2]
            .as_str()
            .map(|s| s.to_lowercase() == "yes")
            .ok_or_else(|| Error::UnableToRetrieveSchema {
                reason: format!("data_array[{i}][2] is not a boolean"),
            })?;

        let field: Field = Field::new(col_name, data_type, nullable);

        fields.push(field);
    }

    Ok(Arc::new(Schema::new(fields)))
}

struct SqlWarehouseConnection {
    api: Arc<SqlWarehouseApi>,
}

impl<'a> DbConnection<Arc<SqlWarehouseApi>, &'a dyn Sync> for SqlWarehouseConnection {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn as_async(&self) -> Option<&dyn AsyncDbConnection<Arc<SqlWarehouseApi>, &'a dyn Sync>> {
        Some(self)
    }
}

#[async_trait]
impl<'a> AsyncDbConnection<Arc<SqlWarehouseApi>, &'a dyn Sync> for SqlWarehouseConnection {
    fn new(api: Arc<SqlWarehouseApi>) -> Self {
        Self { api }
    }

    async fn tables(&self, _schema: &str) -> Result<Vec<String>, dbconnection::Error> {
        Err(dbconnection::Error::UnableToGetTables {
            source: "Databricks tables() not implemented".into(),
        })
    }

    async fn schemas(&self) -> Result<Vec<String>, dbconnection::Error> {
        let query = "SELECT schema_name FROM information_schema.schemata";

        let token = self.api.token_provider.get_token();
        let payload = json!({
            "warehouse_id": self.api.sql_warehouse_id,
            "format": "ARROW_STREAM",
            "disposition": "EXTERNAL_LINKS",
            "wait_timeout": "30s",
            "on_wait_timeout": "CONTINUE",
            "statement": query,
        });

        let response = self
            .api
            .execute_sql_statement(&token, &payload)
            .await
            .map_err(|e| dbconnection::Error::UnableToGetSchemas {
                source: Box::new(e),
            })?;

        SqlWarehouseApi::verify_response_status(&response).map_err(|e| {
            dbconnection::Error::UnableToGetSchemas {
                source: Box::new(e),
            }
        })?;

        let mut stream = Arc::clone(&self.api)
            .fetch_external_links(response)
            .await
            .map_err(|e| dbconnection::Error::UnableToGetSchemas {
                source: Box::new(e),
            })?;

        let mut schemas = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|e| dbconnection::Error::UnableToGetSchemas {
                source: Box::new(e),
            })?;

            if let Some(name_column) = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
            {
                for value in name_column.iter().flatten() {
                    schemas.push(value.to_string());
                }
            }
        }

        Ok(schemas)
    }

    async fn get_schema(
        &self,
        table_reference: &TableReference,
    ) -> Result<SchemaRef, dbconnection::Error> {
        self.api
            .get_schema(table_reference)
            .await
            .map_err(|source| dbconnection::Error::UnableToGetSchema {
                source: Box::new(source),
            })
    }

    async fn query_arrow(
        &self,
        sql: &str,
        _: &[&'a dyn Sync],
        _projected_schema: Option<SchemaRef>,
    ) -> Result<SendableRecordBatchStream, Box<dyn std::error::Error + Send + Sync>> {
        let token = self.api.token_provider.get_token();
        let payload = json!({
            "warehouse_id": self.api.sql_warehouse_id,
            "format": "ARROW_STREAM",
            "disposition": "EXTERNAL_LINKS",
            "wait_timeout": "30s",
            "on_wait_timeout": "CONTINUE",
            "statement": sql,
        });

        let mut response = self.api.execute_sql_statement(&token, &payload).await?;

        tracing::trace!("Parsing Databricks JSON response: {response}");

        let mut state = SqlWarehouseApi::extract_response_status(&response)?;
        let statement_id = SqlWarehouseApi::extract_statement_id(&response)?;

        let mut backoff = FibonacciBackoffBuilder::new()
            .max_duration(Some(Duration::from_secs(5)))
            .build();
        while SqlWarehouseApi::is_async_query(state) {
            tracing::trace!("Query is still running (state: '{state}')");
            let Some(backoff_duration) = backoff.next_backoff() else {
                break;
            };
            tokio::time::sleep(backoff_duration).await;
            response = self
                .api
                .get_sql_statement_status(&token, &statement_id)
                .await?;
            state = SqlWarehouseApi::extract_response_status(&response)?;
        }

        SqlWarehouseApi::verify_response_status(&response)?;

        let result_object = response.get_mut("result").map(Value::take).ok_or_else(|| {
            MissingJsonFieldSnafu {
                field: "result".to_string(),
            }
            .build()
        })?;

        Ok(SqlWarehouseApi::fetch_external_links(Arc::clone(&self.api), result_object).await?)
    }

    async fn execute(
        &self,
        _query: &str,
        _: &[&'a dyn Sync],
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        Ok(NotImplementedSnafu.fail()?)
    }
}

fn databricks_dialect() -> dialect::CustomDialect {
    dialect::CustomDialectBuilder::new()
        .with_identifier_quote_style('`')
        .with_interval_style(dialect::IntervalStyle::MySQL)
        .build()
}

#[async_trait]
impl crate::Read for DatabricksSqlWarehouse {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        let dialect = Arc::new(databricks_dialect());

        let table_provider = Arc::new(
            SqlTable::new("databricks", &self.pool, table_reference, None)
                .await
                .context(SqlTableInitializationFailedSnafu)?
                .with_dialect(dialect),
        );

        Ok(Arc::new(
            table_provider
                .create_federated_table_provider()
                .context(TableProviderCreationFailedSnafu)?,
        ))
    }
}
