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
    datasource::TableProvider, error::DataFusionError, execution::SendableRecordBatchStream,
    physical_plan::stream::RecordBatchStreamAdapter, sql::TableReference,
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
};
use token_provider::TokenProvider;
use util::{
    fibonacci_backoff::{Backoff, FibonacciBackoffBuilder},
    format_datafusion_error,
};

use crate::resilient_http::{enable_supported_compression, send_request_with_retry};

mod datatypes;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("This Databricks SQL Warehouse operation is not implemented"))]
    NotImplemented,

    #[snafu(display("HTTP client build failed: {source}"))]
    ClientBuildFailed { source: reqwest::Error },

    #[snafu(display("Databricks datatype {ty} not supported"))]
    UnsupportedType { ty: String },

    #[snafu(display(
        "The table '{dataset_name}' has no column metadata registered in Unity Catalog. Run 'SELECT * FROM {dataset_name} LIMIT 1' in Databricks SQL to populate the schema, then retry. For details, visit: https://spiceai.org/docs/components/data-connectors/databricks"
    ))]
    TableSchemaNotRegistered { dataset_name: String },

    #[snafu(display(
        "Failed to infer schema for dataset '{dataset_name}' (databricks): unexpected schema response format. Verify the table exists and is accessible, or report a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    UnexpectedSchemaResponse {
        dataset_name: String,
        reason: String,
    },

    #[snafu(display(
        "The dataset '{dataset_name}' in Databricks has no columns. Verify the table exists and has at least one column."
    ))]
    NoColumnsInDataset { dataset_name: String },

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

    #[snafu(display("Failed to create table provider: {}", format_datafusion_error(source)))]
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
    base_url: String,
    sql_warehouse_id: String,
    token_provider: Arc<dyn TokenProvider>,
}

impl SqlWarehouseApi {
    fn new(
        host: &str,
        sql_warehouse_id: &str,
        token_provider: Arc<dyn TokenProvider>,
    ) -> Result<Self, Error> {
        let client = enable_supported_compression(ClientBuilder::new())
            .user_agent(super::user_agent())
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .context(ClientBuildFailedSnafu)?;

        Ok(Self {
            client,
            base_url: format!("https://{host}"),
            sql_warehouse_id: sql_warehouse_id.to_string(),
            token_provider,
        })
    }

    async fn get_schema(&self, table: &TableReference) -> Result<SchemaRef, Error> {
        let token = self.token_provider.get_token();
        let table_name = table.to_string();

        match self.get_schema_from_information_schema(&token, table).await {
            Ok(schema) => return Ok(schema),
            Err(Error::TableSchemaNotRegistered { .. } | Error::NoColumnsInDataset { .. }) => {
                tracing::warn!(
                    table = %table,
                    "information_schema.columns has no metadata for this table, falling back to DESCRIBE TABLE. Column nullability will default to nullable."
                );
            }
            Err(e) => return Err(e),
        }

        let payload = self.create_describe_payload(table)?;
        let response = self.execute_sql_statement(&token, &payload).await?;
        let response = self.wait_for_statement_completion(&token, response).await?;
        schema_from_describe_json(&response, &table_name)
    }

    /// Attempts to read the schema from `information_schema.columns`,
    /// trying `full_data_type` first and falling back to `data_type` if
    /// the column does not exist.
    async fn get_schema_from_information_schema(
        &self,
        token: &str,
        table: &TableReference,
    ) -> Result<SchemaRef, Error> {
        let payload = self.create_schema_payload(table, "full_data_type")?;
        let response = self.execute_sql_statement(token, &payload).await?;
        let response = self.wait_for_statement_completion(token, response).await?;

        match schema_from_json(&response, &table.to_string()) {
            Ok(schema) => Ok(schema),
            Err(Error::QueryFailure { ref message })
                if message.contains("UNRESOLVED_COLUMN") && message.contains("full_data_type") =>
            {
                tracing::warn!(
                    table = %table,
                    "Databricks information_schema does not have 'full_data_type' column, falling back to 'data_type'. Complex types (ARRAY, MAP, STRUCT) may lose inner type details."
                );
                let payload = self.create_schema_payload(table, "data_type")?;
                let response = self.execute_sql_statement(token, &payload).await?;
                let response = self.wait_for_statement_completion(token, response).await?;
                schema_from_json(&response, &table.to_string())
            }
            Err(e) => Err(e),
        }
    }

    fn create_schema_payload(
        &self,
        table: &TableReference,
        data_type_column: &str,
    ) -> Result<Value, Error> {
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
            "SELECT column_name, {data_type_column}, is_nullable FROM information_schema.columns WHERE table_name = '{escaped_table}' AND table_schema = '{escaped_schema}' AND table_catalog = '{escaped_catalog}'"
        );
        // Databricks SQL Statements API max wait_timeout is 50s.
        // https://docs.databricks.com/api/workspace/statementexecution/executestatement
        Ok(json!({
            "warehouse_id": self.sql_warehouse_id,
            "catalog": table_catalog,
            "schema": table_schema,
            "statement": sql,
            "format": "JSON_ARRAY",
            "disposition": "INLINE",
            "wait_timeout": "50s",
            "on_wait_timeout": "CONTINUE",
        }))
    }

    /// Builds a `DESCRIBE TABLE` payload for tables where
    /// `information_schema.columns` has no metadata (e.g. Lakehouse
    /// Federation foreign tables).
    fn create_describe_payload(&self, table: &TableReference) -> Result<Value, Error> {
        let table_schema = table.schema().ok_or_else(|| Error::FullyQualifiedPath {
            reason: "missing schema".into(),
        })?;
        let table_catalog = table.catalog().ok_or_else(|| Error::FullyQualifiedPath {
            reason: "missing catalog".into(),
        })?;
        // Use backtick quoting for DESCRIBE TABLE identifiers. Escape
        // embedded backticks by doubling them.
        let sql = format!(
            "DESCRIBE TABLE `{}`.`{}`.`{}`",
            table_catalog.replace('`', "``"),
            table_schema.replace('`', "``"),
            table.table().replace('`', "``"),
        );
        Ok(json!({
            "warehouse_id": self.sql_warehouse_id,
            "catalog": table_catalog,
            "schema": table_schema,
            "statement": sql,
            "format": "JSON_ARRAY",
            "disposition": "INLINE",
            "wait_timeout": "50s",
            "on_wait_timeout": "CONTINUE",
        }))
    }

    async fn execute_sql_statement(&self, token: &str, payload: &Value) -> Result<Value, Error> {
        let url = format!("{}/api/2.0/sql/statements/", self.base_url);
        send_request_with_retry("Databricks SQL Warehouse", "execute SQL statement", || {
            self.client.post(&url).bearer_auth(token).json(payload)
        })
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
        let url = format!("{}/api/2.0/sql/statements/{statement_id}", self.base_url);
        send_request_with_retry(
            "Databricks SQL Warehouse",
            "poll SQL statement status",
            || self.client.get(&url).bearer_auth(token),
        )
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
            let empty_stream: Pin<
                Box<dyn Stream<Item = Result<RecordBatch, DataFusionError>> + Send>,
            > = Box::pin(stream::empty::<Result<RecordBatch, DataFusionError>>());
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::new(Schema::empty()),
                empty_stream,
            )) as SendableRecordBatchStream);
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
                        let url = format!("{}{path}", api.base_url);
                        match send_request_with_retry(
                            "Databricks SQL Warehouse",
                            "fetch next external chunk link",
                            || api.client.get(&url).bearer_auth(&token),
                        )
                        .await
                        .context(HttpRequestFailedSnafu)
                        .and_then(|resp| resp.error_for_status().context(HttpRequestFailedSnafu))
                        {
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
                let empty_stream: Pin<
                    Box<dyn Stream<Item = Result<RecordBatch, DataFusionError>> + Send>,
                > = Box::pin(stream::empty::<Result<RecordBatch, DataFusionError>>());
                return Ok(Box::pin(RecordBatchStreamAdapter::new(
                    Arc::new(Schema::empty()),
                    empty_stream,
                )) as SendableRecordBatchStream);
            }
        };

        let schema = first_batch.schema();
        let run_once = stream::once(async move { Ok(first_batch) });

        let stream: Pin<Box<dyn Stream<Item = Result<RecordBatch, DataFusionError>> + Send>> =
            Box::pin(
                run_once
                    .chain(batch_stream)
                    .map_err(|e| DataFusionError::Execution(e.to_string())),
            );

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)) as SendableRecordBatchStream)
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
        send_request_with_retry(
            "Databricks SQL Warehouse",
            "fetch statement result chunk",
            || self.client.get(url),
        )
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

    /// Polls the statement status until it reaches a terminal state, with bounded retries.
    async fn wait_for_statement_completion(
        &self,
        token: &str,
        mut response: Value,
    ) -> Result<Value, Error> {
        let mut state = Self::extract_response_status(&response)?;
        let statement_id = Self::extract_statement_id(&response)?;

        let mut backoff = FibonacciBackoffBuilder::new().max_retries(Some(14)).build();
        while Self::is_async_query(state) {
            let Some(backoff_duration) = backoff.next_backoff() else {
                break;
            };
            tokio::time::sleep(backoff_duration).await;
            response = self.get_sql_statement_status(token, &statement_id).await?;
            state = Self::extract_response_status(&response)?;
        }

        match state {
            ResponseStatus::Pending => Err(Error::InvalidWarehouseState {
                state: state.to_string(),
            }),
            ResponseStatus::Running => Err(Error::QueryStillRunning),
            _ => Ok(response),
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

fn schema_from_json(json_value: &Value, dataset_name: &str) -> Result<SchemaRef, Error> {
    tracing::trace!("Parsing schema definition from Databricks JSON response: {json_value}");

    SqlWarehouseApi::verify_response_status(json_value)?;

    let result = json_value
        .get("result")
        .ok_or_else(|| Error::UnexpectedSchemaResponse {
            dataset_name: dataset_name.to_string(),
            reason: "missing result object in response".to_string(),
        })?;

    let data_array_value = result.get("data_array");

    let data_array = match data_array_value {
        None | Some(serde_json::Value::Null) => {
            return Err(Error::TableSchemaNotRegistered {
                dataset_name: dataset_name.to_string(),
            });
        }
        Some(v) => v
            .as_array()
            .ok_or_else(|| Error::UnexpectedSchemaResponse {
                dataset_name: dataset_name.to_string(),
                reason: "result.data_array is not an array".to_string(),
            })?,
    };

    if data_array.is_empty() {
        return Err(Error::NoColumnsInDataset {
            dataset_name: dataset_name.to_string(),
        });
    }

    let mut fields = Vec::new();

    for (i, row) in data_array.iter().enumerate() {
        let row_array = row
            .as_array()
            .ok_or_else(|| Error::UnexpectedSchemaResponse {
                dataset_name: dataset_name.to_string(),
                reason: format!("data_array[{i}] is not an array"),
            })?;

        if row_array.len() < 3 {
            return Err(Error::UnexpectedSchemaResponse {
                dataset_name: dataset_name.to_string(),
                reason: format!("data_array[{i}] has fewer than 3 fields"),
            });
        }

        let col_name = row_array[0]
            .as_str()
            .ok_or_else(|| Error::UnexpectedSchemaResponse {
                dataset_name: dataset_name.to_string(),
                reason: format!("data_array[{i}][0] (column name) is not a string"),
            })?;

        // If we see #, DB is now providing the clustering metadata info and
        // we have all the columns we need
        if col_name.starts_with('#') {
            break;
        }

        let data_type_str =
            row_array[1]
                .as_str()
                .ok_or_else(|| Error::UnexpectedSchemaResponse {
                    dataset_name: dataset_name.to_string(),
                    reason: format!("data_array[{i}][1] (data type) is not a string"),
                })?;

        let data_type = datatypes::Parser::new(data_type_str)
            .parse()
            .map_err(|reason| Error::ParseError { reason })?;

        let nullable = row_array[2]
            .as_str()
            .map(|s| s.to_lowercase() == "yes")
            .ok_or_else(|| Error::UnexpectedSchemaResponse {
                dataset_name: dataset_name.to_string(),
                reason: format!("data_array[{i}][2] (is_nullable) is not a string"),
            })?;

        let field: Field = Field::new(col_name, data_type, nullable);

        fields.push(field);
    }

    if fields.is_empty() {
        return Err(Error::NoColumnsInDataset {
            dataset_name: dataset_name.to_string(),
        });
    }

    Ok(Arc::new(Schema::new(fields)))
}

/// Parses a schema from a `DESCRIBE TABLE` response.
///
/// `DESCRIBE TABLE` returns rows of `[col_name, data_type, comment]`.
/// Since it does not include nullability information, all columns default
/// to nullable. Blank separator rows are skipped, and metadata rows
/// (starting with `#`) stop parsing.
fn schema_from_describe_json(json_value: &Value, dataset_name: &str) -> Result<SchemaRef, Error> {
    tracing::trace!("Parsing schema from DESCRIBE TABLE response: {json_value}");

    SqlWarehouseApi::verify_response_status(json_value)?;

    let result = json_value
        .get("result")
        .ok_or_else(|| Error::UnexpectedSchemaResponse {
            dataset_name: dataset_name.to_string(),
            reason: "missing result object in DESCRIBE TABLE response".to_string(),
        })?;

    let data_array = result
        .get("data_array")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::UnexpectedSchemaResponse {
            dataset_name: dataset_name.to_string(),
            reason: "missing or invalid data_array in DESCRIBE TABLE response".to_string(),
        })?;

    let mut fields = Vec::new();

    for (i, row) in data_array.iter().enumerate() {
        let row_array = row
            .as_array()
            .ok_or_else(|| Error::UnexpectedSchemaResponse {
                dataset_name: dataset_name.to_string(),
                reason: format!("data_array[{i}] is not an array"),
            })?;

        if row_array.len() < 2 {
            return Err(Error::UnexpectedSchemaResponse {
                dataset_name: dataset_name.to_string(),
                reason: format!("data_array[{i}] has fewer than 2 fields"),
            });
        }

        let col_name = row_array[0]
            .as_str()
            .ok_or_else(|| Error::UnexpectedSchemaResponse {
                dataset_name: dataset_name.to_string(),
                reason: format!("data_array[{i}][0] (column name) is not a string"),
            })?;

        // Metadata rows start with #; stop parsing here.
        if col_name.starts_with('#') {
            break;
        }

        // Skip blank separator rows between columns and metadata.
        if col_name.trim().is_empty() {
            continue;
        }

        let data_type_str =
            row_array[1]
                .as_str()
                .ok_or_else(|| Error::UnexpectedSchemaResponse {
                    dataset_name: dataset_name.to_string(),
                    reason: format!("data_array[{i}][1] (data type) is not a string"),
                })?;

        let data_type = datatypes::Parser::new(data_type_str)
            .parse()
            .map_err(|reason| Error::ParseError { reason })?;

        // DESCRIBE TABLE does not report nullability; default to nullable.
        fields.push(Field::new(col_name, data_type, true));
    }

    if fields.is_empty() {
        return Err(Error::NoColumnsInDataset {
            dataset_name: dataset_name.to_string(),
        });
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
            "wait_timeout": "50s",
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
            "wait_timeout": "50s",
            "on_wait_timeout": "CONTINUE",
            "statement": sql,
        });

        let response = self.api.execute_sql_statement(&token, &payload).await?;

        tracing::trace!("Parsing Databricks JSON response: {response}");

        let mut response = self
            .api
            .wait_for_statement_completion(&token, response)
            .await?;

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

fn databricks_dialect() -> super::dialect::DatabricksDialect {
    super::dialect::DatabricksDialect::new()
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use serde_json::json;

    /// Helper to create a valid Databricks schema response JSON.
    fn make_schema_response(data_array: &Value) -> Value {
        json!({
            "status": { "state": "SUCCEEDED" },
            "statement_id": "test-stmt-id",
            "result": { "data_array": data_array }
        })
    }

    #[test]
    fn test_schema_from_json_basic() {
        let response = make_schema_response(&json!([
            ["id", "int", "NO"],
            ["name", "string", "YES"],
            ["amount", "double", "NO"]
        ]));

        let schema = schema_from_json(&response, "test_table").expect("should parse schema");
        assert_eq!(schema.fields().len(), 3);

        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert!(!schema.field(0).is_nullable());

        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert!(schema.field(1).is_nullable());

        assert_eq!(schema.field(2).name(), "amount");
        assert_eq!(schema.field(2).data_type(), &DataType::Float64);
        assert!(!schema.field(2).is_nullable());
    }

    #[test]
    fn test_schema_from_json_many_types() {
        let response = make_schema_response(&json!([
            ["col_bigint", "bigint", "NO"],
            ["col_smallint", "smallint", "YES"],
            ["col_boolean", "boolean", "NO"],
            ["col_float", "float", "YES"],
            ["col_date", "date", "NO"],
            ["col_timestamp", "timestamp", "YES"],
            ["col_binary", "binary", "NO"],
            ["col_decimal", "decimal(10,2)", "YES"]
        ]));

        let schema = schema_from_json(&response, "test_table").expect("should parse schema");
        assert_eq!(schema.fields().len(), 8);
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &DataType::Int16);
        assert_eq!(schema.field(2).data_type(), &DataType::Boolean);
        assert_eq!(schema.field(3).data_type(), &DataType::Float32);
        assert_eq!(schema.field(4).data_type(), &DataType::Date32);
        assert_eq!(
            schema.field(5).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(schema.field(6).data_type(), &DataType::Binary);
        assert_eq!(schema.field(7).data_type(), &DataType::Decimal128(10, 2));
    }

    #[test]
    fn test_schema_from_json_empty_table() {
        let response = make_schema_response(&json!([]));

        let err = schema_from_json(&response, "my_catalog.my_schema.my_table")
            .expect_err("should fail on empty schema");
        assert!(
            matches!(&err, Error::NoColumnsInDataset { dataset_name } if dataset_name == "my_catalog.my_schema.my_table"),
            "unexpected error: {err}"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("my_catalog.my_schema.my_table"),
            "error should contain dataset name: {msg}"
        );
        assert!(
            msg.contains("has no columns"),
            "error should mention no columns: {msg}"
        );
    }

    #[test]
    fn test_schema_from_json_stops_at_clustering_metadata() {
        let response = make_schema_response(&json!([
            ["id", "int", "NO"],
            ["name", "string", "YES"],
            ["# Clustering Information", "", ""],
            ["# col_name", "data_type", "comment"]
        ]));

        let schema =
            schema_from_json(&response, "test_table").expect("should stop at clustering marker");
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
    }

    #[test]
    fn test_schema_from_json_missing_result() {
        let response = json!({
            "status": { "state": "SUCCEEDED" }
        });

        let err =
            schema_from_json(&response, "test_table").expect_err("should fail without result");
        assert!(
            matches!(&err, Error::UnexpectedSchemaResponse { reason, .. } if reason.contains("missing result")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_schema_from_json_missing_data_array() {
        let response = json!({
            "status": { "state": "SUCCEEDED" },
            "result": {}
        });

        let err =
            schema_from_json(&response, "test_table").expect_err("should fail without data_array");
        assert!(
            matches!(&err, Error::TableSchemaNotRegistered { .. }),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_schema_from_json_data_array_not_array() {
        let response = json!({
            "status": { "state": "SUCCEEDED" },
            "result": { "data_array": "not_an_array" }
        });

        let err = schema_from_json(&response, "test_table")
            .expect_err("should fail when data_array is string");
        assert!(
            matches!(&err, Error::UnexpectedSchemaResponse { reason, .. } if reason.contains("is not an array")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_schema_from_json_empty_data_array() {
        let response = json!({
            "status": { "state": "SUCCEEDED" },
            "result": { "data_array": [] }
        });

        let err =
            schema_from_json(&response, "test_table").expect_err("should fail on empty data_array");
        assert!(
            matches!(&err, Error::NoColumnsInDataset { .. }),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_schema_from_json_row_not_array() {
        let response = make_schema_response(&json!(["not_an_array"]));

        let err =
            schema_from_json(&response, "test_table").expect_err("should fail on non-array row");
        assert!(
            matches!(&err, Error::UnexpectedSchemaResponse { reason, .. } if reason.contains("is not an array")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_schema_from_json_row_too_short() {
        let response = make_schema_response(&json!([["id", "int"]]));

        let err = schema_from_json(&response, "test_table").expect_err("should fail on short row");
        assert!(
            matches!(&err, Error::UnexpectedSchemaResponse { reason, .. } if reason.contains("fewer than 3 fields")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_schema_from_json_column_name_not_string() {
        let response = make_schema_response(&json!([[123, "int", "NO"]]));

        let err = schema_from_json(&response, "test_table")
            .expect_err("should fail on non-string col name");
        assert!(
            matches!(&err, Error::UnexpectedSchemaResponse { reason, .. } if reason.contains("[0] (column name) is not a string")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_schema_from_json_data_type_not_string() {
        let response = make_schema_response(&json!([["id", 42, "NO"]]));

        let err = schema_from_json(&response, "test_table")
            .expect_err("should fail on non-string data type");
        assert!(
            matches!(&err, Error::UnexpectedSchemaResponse { reason, .. } if reason.contains("[1] (data type) is not a string")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_schema_from_json_nullable_not_string() {
        let response = make_schema_response(&json!([["id", "int", true]]));

        let err = schema_from_json(&response, "test_table")
            .expect_err("should fail on non-string nullable");
        assert!(
            matches!(&err, Error::UnexpectedSchemaResponse { reason, .. } if reason.contains("[2] (is_nullable) is not a string")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_schema_from_json_nullable_case_insensitive() {
        let response = make_schema_response(&json!([
            ["a", "int", "YES"],
            ["b", "int", "Yes"],
            ["c", "int", "yes"],
            ["d", "int", "NO"],
            ["e", "int", "No"],
            ["f", "int", "no"],
            ["g", "int", "anything_else"]
        ]));

        let schema =
            schema_from_json(&response, "test_table").expect("should parse nullable variations");
        assert!(schema.field(0).is_nullable());
        assert!(schema.field(1).is_nullable());
        assert!(schema.field(2).is_nullable());
        assert!(!schema.field(3).is_nullable());
        assert!(!schema.field(4).is_nullable());
        assert!(!schema.field(5).is_nullable());
        assert!(!schema.field(6).is_nullable());
    }

    #[test]
    fn test_schema_from_json_failed_status() {
        let response = json!({
            "status": {
                "state": "FAILED",
                "error": { "message": "table not found" }
            },
            "statement_id": "test-stmt-id",
            "result": { "data_array": [] }
        });

        let err =
            schema_from_json(&response, "test_table").expect_err("should fail on FAILED status");
        assert!(
            matches!(&err, Error::QueryFailure { message } if message.contains("table not found")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_schema_from_json_pending_status() {
        let response = json!({
            "status": { "state": "PENDING" },
            "statement_id": "test-stmt-id"
        });

        let err =
            schema_from_json(&response, "test_table").expect_err("should fail on PENDING status");
        assert!(
            matches!(&err, Error::InvalidWarehouseState { .. }),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_schema_from_json_unsupported_type() {
        let response = make_schema_response(&json!([["col", "TOTALLY_FAKE_TYPE", "NO"]]));

        let err =
            schema_from_json(&response, "test_table").expect_err("should fail on unsupported type");
        assert!(
            matches!(&err, Error::ParseError { .. }),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_schema_from_json_extra_columns_ignored() {
        // Rows with more than 3 elements should still work (extra fields ignored)
        let response =
            make_schema_response(&json!([["id", "int", "NO", "extra_col", "another_extra"]]));

        let schema =
            schema_from_json(&response, "test_table").expect("should parse with extra columns");
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "id");
    }

    #[test]
    fn test_schema_from_json_missing_status() {
        let response = json!({
            "result": { "data_array": [["id", "int", "NO"]] }
        });

        let err =
            schema_from_json(&response, "test_table").expect_err("should fail without status");
        assert!(
            matches!(&err, Error::MissingJsonField { field } if field == "status.state"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_create_schema_payload_includes_inline_disposition() {
        let api = SqlWarehouseApi::new(
            "host.example.com",
            "warehouse-123",
            Arc::new(StaticTokenProvider("token".to_string())),
        )
        .expect("should create api");
        let table = TableReference::full("my_catalog", "my_schema", "my_table");

        let payload = api
            .create_schema_payload(&table, "full_data_type")
            .expect("should create payload");

        assert_eq!(payload["format"], "JSON_ARRAY");
        assert_eq!(payload["disposition"], "INLINE");
        assert_eq!(payload["wait_timeout"], "50s");
        assert_eq!(payload["on_wait_timeout"], "CONTINUE");
        assert_eq!(payload["warehouse_id"], "warehouse-123");
        assert!(
            payload["statement"]
                .as_str()
                .expect("statement should be string")
                .contains("my_table"),
            "statement should reference the table"
        );
    }

    #[test]
    fn test_create_schema_payload_missing_schema() {
        let api = SqlWarehouseApi::new(
            "host.example.com",
            "wh-1",
            Arc::new(StaticTokenProvider("t".to_string())),
        )
        .expect("should create api");
        let table = TableReference::bare("just_table");

        let err = api
            .create_schema_payload(&table, "full_data_type")
            .expect_err("should fail without schema");
        assert!(
            matches!(&err, Error::FullyQualifiedPath { reason } if reason.contains("missing schema")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_create_schema_payload_missing_catalog() {
        let api = SqlWarehouseApi::new(
            "host.example.com",
            "wh-1",
            Arc::new(StaticTokenProvider("t".to_string())),
        )
        .expect("should create api");
        let table = TableReference::partial("my_schema", "my_table");

        let err = api
            .create_schema_payload(&table, "full_data_type")
            .expect_err("should fail without catalog");
        assert!(
            matches!(&err, Error::FullyQualifiedPath { reason } if reason.contains("missing catalog")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_create_schema_payload_sql_injection_prevention() {
        let api = SqlWarehouseApi::new(
            "host.example.com",
            "wh-1",
            Arc::new(StaticTokenProvider("t".to_string())),
        )
        .expect("should create api");
        let table = TableReference::full("cat'alog", "sch'ema", "tab'le");

        let payload = api
            .create_schema_payload(&table, "full_data_type")
            .expect("should create payload");
        let stmt = payload["statement"]
            .as_str()
            .expect("statement should be string");

        // Single quotes should be escaped as double single-quotes
        assert!(stmt.contains("tab''le"), "table name not escaped: {stmt}");
        assert!(stmt.contains("sch''ema"), "schema not escaped: {stmt}");
        assert!(stmt.contains("cat''alog"), "catalog not escaped: {stmt}");
        // Should NOT contain unescaped single quotes between the SQL string quotes
        assert!(
            !stmt.contains("tab'le"),
            "unescaped table name found: {stmt}"
        );
    }

    /// Simple test [`TokenProvider`] for unit tests.
    #[derive(Debug)]
    struct StaticTokenProvider(String);

    impl TokenProvider for StaticTokenProvider {
        fn get_token(&self) -> String {
            self.0.clone()
        }

        fn dyn_hash(&self) -> String {
            self.0.clone()
        }
    }

    /// Starts a mock HTTP server that serves JSON responses in order.
    /// Once the queue is exhausted, `default_response` is returned for all subsequent requests.
    async fn start_mock_server(responses: Vec<Value>, default_response: Value) -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("should bind to a port");
        let port = listener
            .local_addr()
            .expect("should have an address")
            .port();
        let responses = Arc::new(tokio::sync::Mutex::new(std::collections::VecDeque::from(
            responses,
        )));
        let default = Arc::new(default_response);

        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let responses = Arc::clone(&responses);
                let default = Arc::clone(&default);
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut buf = vec![0u8; 4096];
                    let _ = stream.read(&mut buf).await;

                    let response_json = {
                        let mut q = responses.lock().await;
                        q.pop_front().unwrap_or_else(|| (*default).clone())
                    };

                    let body =
                        serde_json::to_string(&response_json).expect("should serialize response");
                    let http_response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(http_response.as_bytes()).await;
                });
            }
        });

        port
    }

    #[derive(Clone)]
    struct MockHttpResponse {
        status_line: &'static str,
        headers: Vec<(String, String)>,
        body: String,
    }

    async fn start_mock_http_server(
        responses: Vec<MockHttpResponse>,
        default_response: MockHttpResponse,
    ) -> (u16, Arc<std::sync::atomic::AtomicUsize>) {
        use std::{
            collections::VecDeque,
            sync::atomic::{AtomicUsize, Ordering},
        };

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("should bind to a port");
        let port = listener
            .local_addr()
            .expect("should have an address")
            .port();
        let responses = Arc::new(tokio::sync::Mutex::new(VecDeque::from(responses)));
        let default = Arc::new(default_response);
        let requests = Arc::new(AtomicUsize::new(0));

        let requests_for_server = Arc::clone(&requests);
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let responses = Arc::clone(&responses);
                let default = Arc::clone(&default);
                let requests = Arc::clone(&requests_for_server);
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};

                    let mut buf = vec![0u8; 4096];
                    let _ = stream.read(&mut buf).await;
                    requests.fetch_add(1, Ordering::SeqCst);

                    let response = {
                        let mut q = responses.lock().await;
                        q.pop_front().unwrap_or_else(|| (*default).clone())
                    };

                    let mut http_response = format!(
                        "HTTP/1.1 {}\r\nContent-Length: {}\r\n",
                        response.status_line,
                        response.body.len()
                    );
                    use std::fmt::Write as _;
                    for (header_name, header_value) in response.headers {
                        let _ = write!(http_response, "{header_name}: {header_value}\r\n");
                    }
                    http_response.push_str("\r\n");
                    http_response.push_str(&response.body);

                    let _ = stream.write_all(http_response.as_bytes()).await;
                });
            }
        });

        (port, requests)
    }

    fn create_test_api(port: u16) -> SqlWarehouseApi {
        let client = enable_supported_compression(ClientBuilder::new())
            .build()
            .expect("should build client");
        SqlWarehouseApi {
            client,
            base_url: format!("http://127.0.0.1:{port}"),
            sql_warehouse_id: "test-warehouse".to_string(),
            token_provider: Arc::new(StaticTokenProvider("test-token".to_string())),
        }
    }

    fn pending_response() -> Value {
        json!({"status": {"state": "PENDING"}, "statement_id": "stmt-1"})
    }

    fn running_response() -> Value {
        json!({"status": {"state": "RUNNING"}, "statement_id": "stmt-1"})
    }

    fn succeeded_response() -> Value {
        json!({"status": {"state": "SUCCEEDED"}, "statement_id": "stmt-1", "result": {"data_array": []}})
    }

    fn failed_response() -> Value {
        json!({"status": {"state": "FAILED"}, "statement_id": "stmt-1", "status": {"state": "FAILED", "error": {"message": "table not found"}}})
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_wait_for_completion_immediate_success() {
        let port = start_mock_server(vec![], json!({})).await;
        let api = create_test_api(port);

        let result = api
            .wait_for_statement_completion("token", succeeded_response())
            .await;
        let response = result.expect("SUCCEEDED should return Ok");
        assert_eq!(response["status"]["state"], "SUCCEEDED");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_wait_for_completion_immediate_failed() {
        let port = start_mock_server(vec![], json!({})).await;
        let api = create_test_api(port);

        let result = api
            .wait_for_statement_completion("token", failed_response())
            .await;
        // FAILED is a terminal state — wait_for_statement_completion returns it as Ok
        let response = result.expect("FAILED should return Ok (terminal state)");
        assert_eq!(response["status"]["state"], "FAILED");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_wait_for_completion_immediate_canceled() {
        let port = start_mock_server(vec![], json!({})).await;
        let api = create_test_api(port);
        let response = json!({"status": {"state": "CANCELED"}, "statement_id": "stmt-1"});

        let result = api.wait_for_statement_completion("token", response).await;
        let response = result.expect("CANCELED should return Ok (terminal state)");
        assert_eq!(response["status"]["state"], "CANCELED");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_wait_for_completion_pending_then_success() {
        let port = start_mock_server(vec![succeeded_response()], pending_response()).await;
        let api = create_test_api(port);

        let result = api
            .wait_for_statement_completion("token", pending_response())
            .await;
        let response = result.expect("should eventually succeed");
        assert_eq!(response["status"]["state"], "SUCCEEDED");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_wait_for_completion_running_then_success() {
        let port = start_mock_server(vec![succeeded_response()], running_response()).await;
        let api = create_test_api(port);

        let result = api
            .wait_for_statement_completion("token", running_response())
            .await;
        let response = result.expect("should eventually succeed");
        assert_eq!(response["status"]["state"], "SUCCEEDED");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_wait_for_completion_exhaustion_pending() {
        let port = start_mock_server(vec![], pending_response()).await;
        let api = create_test_api(port);

        let result = api
            .wait_for_statement_completion("token", pending_response())
            .await;
        let err = result.expect_err("should fail after exhausting retries");
        assert!(
            matches!(&err, Error::InvalidWarehouseState { state } if state == "PENDING"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_wait_for_completion_exhaustion_running() {
        let port = start_mock_server(vec![], running_response()).await;
        let api = create_test_api(port);

        let result = api
            .wait_for_statement_completion("token", running_response())
            .await;
        let err = result.expect_err("should fail after exhausting retries");
        assert!(
            matches!(&err, Error::QueryStillRunning),
            "unexpected error: {err}"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_wait_for_completion_missing_status() {
        let port = start_mock_server(vec![], json!({})).await;
        let api = create_test_api(port);
        let response = json!({"statement_id": "stmt-1"});

        let result = api.wait_for_statement_completion("token", response).await;
        let err = result.expect_err("should fail on missing status");
        assert!(
            matches!(&err, Error::MissingJsonField { field } if field == "status.state"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_wait_for_completion_missing_statement_id() {
        let port = start_mock_server(vec![], json!({})).await;
        let api = create_test_api(port);
        let response = json!({"status": {"state": "PENDING"}});

        let result = api.wait_for_statement_completion("token", response).await;
        let err = result.expect_err("should fail on missing statement_id");
        assert!(
            matches!(&err, Error::MissingJsonField { field } if field == "statement_id"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_schema_from_json_no_columns_error_includes_dataset_name() {
        let response = make_schema_response(&json!([]));

        let err = schema_from_json(&response, "my_catalog.my_schema.orders")
            .expect_err("should fail when no columns");
        let msg = err.to_string();
        assert!(
            msg.contains("my_catalog.my_schema.orders"),
            "error should contain the full dataset name: {msg}"
        );
        assert!(
            msg.contains("has no columns"),
            "error should mention 'has no columns': {msg}"
        );
        assert!(
            msg.contains("Verify the table exists"),
            "error should suggest verifying table existence: {msg}"
        );
    }

    #[test]
    fn test_schema_from_json_only_clustering_metadata_returns_no_columns_error() {
        // When the data_array only contains clustering metadata markers,
        // no real columns are parsed and we should get a NoColumnsInDataset error.
        let response = make_schema_response(&json!([
            ["# Clustering Information", "", ""],
            ["# col_name", "data_type", "comment"]
        ]));

        let err = schema_from_json(&response, "test_table")
            .expect_err("should fail when only clustering metadata present");
        assert!(
            matches!(&err, Error::NoColumnsInDataset { dataset_name } if dataset_name == "test_table"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_schema_from_json_missing_result_error_is_actionable() {
        let response = json!({
            "status": { "state": "SUCCEEDED" }
        });

        let err = schema_from_json(&response, "catalog.schema.my_orders")
            .expect_err("should fail without result");
        let msg = err.to_string();
        assert!(
            msg.contains("catalog.schema.my_orders"),
            "error should contain dataset name: {msg}"
        );
        assert!(
            msg.contains("Verify the table exists"),
            "error should suggest verifying table: {msg}"
        );
    }

    #[test]
    fn test_schema_from_json_happy_path_with_dataset_name() {
        // Ensure the dataset_name parameter doesn't affect successful parsing.
        let response =
            make_schema_response(&json!([["id", "int", "NO"], ["name", "string", "YES"]]));

        let schema = schema_from_json(&response, "catalog.schema.users")
            .expect("should parse schema successfully");
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
    }

    #[test]
    fn test_create_schema_payload_uses_data_type_column() {
        let api = SqlWarehouseApi::new(
            "host.example.com",
            "wh-1",
            Arc::new(StaticTokenProvider("t".to_string())),
        )
        .expect("should create api");
        let table = TableReference::full("my_catalog", "my_schema", "my_table");

        let payload_full = api
            .create_schema_payload(&table, "full_data_type")
            .expect("should create payload with full_data_type");
        let stmt_full = payload_full["statement"]
            .as_str()
            .expect("statement should be string");
        assert!(
            stmt_full.contains("full_data_type"),
            "SQL should reference full_data_type: {stmt_full}"
        );
        assert!(
            !stmt_full.contains(", data_type,"),
            "SQL should not reference plain data_type: {stmt_full}"
        );
        let payload_plain = api
            .create_schema_payload(&table, "data_type")
            .expect("should create payload with data_type");
        let stmt_plain = payload_plain["statement"]
            .as_str()
            .expect("statement should be string");
        assert!(
            stmt_plain.contains(", data_type,"),
            "SQL should reference data_type: {stmt_plain}"
        );
        assert!(
            !stmt_plain.contains("full_data_type"),
            "SQL should not reference full_data_type: {stmt_plain}"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_execute_sql_statement_retries_rate_limited_response() {
        let success_body = json!({
            "status": { "state": "SUCCEEDED" },
            "statement_id": "stmt-1",
            "result": { "data_array": [] }
        })
        .to_string();

        let (port, requests) = start_mock_http_server(
            vec![
                MockHttpResponse {
                    status_line: "429 Too Many Requests",
                    headers: vec![
                        ("Content-Type".to_string(), "application/json".to_string()),
                        ("Retry-After".to_string(), "0".to_string()),
                    ],
                    body: json!({"error_code": "RATE_LIMITED"}).to_string(),
                },
                MockHttpResponse {
                    status_line: "200 OK",
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: success_body,
                },
            ],
            MockHttpResponse {
                status_line: "200 OK",
                headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                body: json!({"ok": true}).to_string(),
            },
        )
        .await;

        let api = create_test_api(port);
        let response = api
            .execute_sql_statement("token", &json!({"statement": "SELECT 1"}))
            .await
            .expect("SQL statement should succeed after retrying the rate-limited response");

        assert_eq!(response["status"]["state"], "SUCCEEDED");
        assert_eq!(
            requests.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "expected the SQL statement request to be retried once"
        );
    }

    #[test]
    fn test_schema_from_json_parameterless_complex_types() {
        // When using `data_type` column, complex types lack inner type info.
        let response = make_schema_response(&json!([
            ["id", "int", "NO"],
            ["tags", "ARRAY", "YES"],
            ["metadata", "MAP", "YES"],
            ["details", "STRUCT", "YES"],
            ["price", "DECIMAL", "YES"]
        ]));

        let schema = schema_from_json(&response, "test_table")
            .expect("should parse parameterless complex types");
        assert_eq!(schema.fields().len(), 5);

        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);

        // ARRAY without type params falls back to List(Utf8)
        assert_eq!(schema.field(1).name(), "tags");
        assert!(
            matches!(schema.field(1).data_type(), DataType::List(_)),
            "ARRAY should become List, got {:?}",
            schema.field(1).data_type()
        );

        // MAP without type params falls back to Map(Utf8, Utf8)
        assert_eq!(schema.field(2).name(), "metadata");
        assert!(
            matches!(schema.field(2).data_type(), DataType::Map(_, _)),
            "MAP should become Map, got {:?}",
            schema.field(2).data_type()
        );

        // STRUCT without type params falls back to Utf8
        assert_eq!(schema.field(3).name(), "details");
        assert_eq!(schema.field(3).data_type(), &DataType::Utf8);

        // DECIMAL without precision/scale falls back to Decimal128(38,10)
        assert_eq!(schema.field(4).name(), "price");
        assert_eq!(schema.field(4).data_type(), &DataType::Decimal128(38, 10));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_get_schema_falls_back_to_data_type_on_unresolved_column() {
        // First call returns UNRESOLVED_COLUMN error (full_data_type doesn't exist).
        // Second call returns a successful schema response using data_type column.
        let unresolved_column_response = json!({
            "status": {
                "state": "FAILED",
                "error": {
                    "message": "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `full_data_type` cannot be resolved."
                }
            },
            "statement_id": "stmt-1"
        });
        let success_response = json!({
            "status": { "state": "SUCCEEDED" },
            "statement_id": "stmt-2",
            "result": {
                "data_array": [
                    ["id", "int", "NO"],
                    ["name", "string", "YES"]
                ]
            }
        });

        let port = start_mock_server(
            vec![unresolved_column_response, success_response],
            json!({}),
        )
        .await;
        let api = create_test_api(port);
        let table = TableReference::full("my_catalog", "my_schema", "my_table");

        let schema = api
            .get_schema(&table)
            .await
            .expect("should succeed via data_type fallback");
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_get_schema_succeeds_with_full_data_type() {
        let success_response = json!({
            "status": { "state": "SUCCEEDED" },
            "statement_id": "stmt-1",
            "result": {
                "data_array": [
                    ["id", "bigint", "NO"],
                    ["amount", "decimal(10,2)", "YES"]
                ]
            }
        });

        let port = start_mock_server(vec![success_response], json!({})).await;
        let api = create_test_api(port);
        let table = TableReference::full("catalog", "schema", "orders");

        let schema = api
            .get_schema(&table)
            .await
            .expect("should succeed on first try with full_data_type");
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).data_type(), &DataType::Decimal128(10, 2));
    }

    /// Regression test: Databricks sends timestamps as `Timestamp(Microsecond, "Etc/UTC")`
    /// in Arrow IPC. The declared schema must also use Microsecond so that `try_cast_to`
    /// doesn't attempt a µs→ns multiplication that overflows for far-future sentinel
    /// values like year 9999 (253402300799999000 µs × 1000 > `i64::MAX`).
    #[test]
    fn test_schema_from_json_timestamp_microsecond_avoids_overflow() {
        use arrow::array::TimestampMicrosecondArray;
        use arrow_tools::record_batch::try_cast_to;

        // Parse schema from a Databricks JSON response with timestamp columns
        let response = make_schema_response(&json!([
            ["id", "int", "NO"],
            ["end_datetime", "timestamp", "YES"],
            ["created_ntz", "timestamp_ntz", "YES"]
        ]));
        let declared_schema =
            schema_from_json(&response, "test_table").expect("should parse schema");

        // Verify both timestamp fields use Microsecond
        assert_eq!(
            declared_schema.field(1).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
            "TIMESTAMP must be Microsecond"
        );
        assert_eq!(
            declared_schema.field(2).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            "TIMESTAMP_NTZ must be Microsecond"
        );

        // Simulate Databricks Arrow IPC: data arrives as Timestamp(Microsecond, "Etc/UTC")
        // with a far-future sentinel value (year 9999).
        let sentinel_us: i64 = 253_402_300_799_999_000; // 9999-12-31T23:59:59.999 in µs
        let ipc_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "end_datetime",
                DataType::Timestamp(
                    arrow::datatypes::TimeUnit::Microsecond,
                    Some("Etc/UTC".into()),
                ),
                true,
            ),
            Field::new(
                "created_ntz",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&ipc_schema),
            vec![
                Arc::new(arrow::array::Int32Array::from(vec![1])),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![sentinel_us]).with_timezone("Etc/UTC"),
                ),
                Arc::new(TimestampMicrosecondArray::from(vec![sentinel_us])),
            ],
        )
        .expect("should create batch");

        // Cast from IPC schema to declared schema — must NOT overflow
        let result = try_cast_to(batch, declared_schema);
        assert!(
            result.is_ok(),
            "try_cast_to should not overflow for year-9999 sentinel: {:?}",
            result.err()
        );

        let casted = result.expect("already checked");
        let ts_col = casted
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("should be TimestampMicrosecondArray");
        assert_eq!(
            ts_col.value(0),
            sentinel_us,
            "sentinel value must be preserved"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_get_schema_propagates_non_unresolved_column_errors() {
        // A FAILED response that is NOT about UNRESOLVED_COLUMN should not trigger fallback.
        let other_failure = json!({
            "status": {
                "state": "FAILED",
                "error": { "message": "Table or view not found: my_table" }
            },
            "statement_id": "stmt-1"
        });

        let port = start_mock_server(vec![other_failure], json!({})).await;
        let api = create_test_api(port);
        let table = TableReference::full("catalog", "schema", "my_table");

        let err = api
            .get_schema(&table)
            .await
            .expect_err("should propagate non-UNRESOLVED_COLUMN error");
        assert!(
            matches!(&err, Error::QueryFailure { message } if message.contains("Table or view not found")),
            "unexpected error: {err}"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_get_schema_no_fallback_for_unresolved_column_other_than_full_data_type() {
        // UNRESOLVED_COLUMN for a column other than full_data_type should NOT trigger fallback.
        let unresolved_other = json!({
            "status": {
                "state": "FAILED",
                "error": {
                    "message": "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `some_other_col` cannot be resolved."
                }
            },
            "statement_id": "stmt-1"
        });

        let port = start_mock_server(vec![unresolved_other], json!({})).await;
        let api = create_test_api(port);
        let table = TableReference::full("catalog", "schema", "my_table");

        let err = api
            .get_schema(&table)
            .await
            .expect_err("should not fall back for unrelated UNRESOLVED_COLUMN");
        assert!(
            matches!(&err, Error::QueryFailure { message } if message.contains("UNRESOLVED_COLUMN")),
            "unexpected error: {err}"
        );
    }

    /// Schema parsed from `full_data_type` column values matching a real
    /// Databricks `information_schema` dump (bigint, string, timestamp,
    /// boolean, double).
    #[test]
    fn test_schema_from_json_real_full_data_type_schema() {
        let response = make_schema_response(&json!([
            ["record_skey", "bigint", "YES"],
            ["record_hkey", "string", "NO"],
            ["address_city", "string", "NO"],
            ["address_state", "string", "NO"],
            ["address_latitude", "double", "NO"],
            ["address_longitude", "double", "NO"],
            ["start_datetime", "timestamp", "NO"],
            ["end_datetime", "timestamp", "NO"],
            ["is_current_flag", "boolean", "NO"],
            ["is_deleted_flag", "boolean", "NO"]
        ]));

        let schema = schema_from_json(&response, "catalog.test_schema.dim_records")
            .expect("should parse real full_data_type schema");
        assert_eq!(schema.fields().len(), 10);

        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert!(schema.field(0).is_nullable());
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(4).data_type(), &DataType::Float64);
        assert_eq!(
            schema.field(6).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(
            schema.field(7).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(schema.field(8).data_type(), &DataType::Boolean);
    }

    /// Schema parsed from `data_type` column values matching a real
    /// Databricks `information_schema` dump. The fallback path receives
    /// LONG instead of bigint, and all types are uppercase.
    #[test]
    fn test_schema_from_json_real_data_type_fallback_schema() {
        let response = make_schema_response(&json!([
            ["record_skey", "LONG", "YES"],
            ["record_hkey", "STRING", "NO"],
            ["address_city", "STRING", "NO"],
            ["address_latitude", "DOUBLE", "NO"],
            ["address_longitude", "DOUBLE", "NO"],
            ["start_datetime", "TIMESTAMP", "NO"],
            ["end_datetime", "TIMESTAMP", "NO"],
            ["is_current_flag", "BOOLEAN", "NO"],
            ["is_deleted_flag", "BOOLEAN", "NO"]
        ]));

        let schema = schema_from_json(&response, "catalog.test_schema.dim_records")
            .expect("should parse real data_type fallback schema");
        assert_eq!(schema.fields().len(), 9);

        // LONG must map to Int64
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert!(schema.field(0).is_nullable());
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(3).data_type(), &DataType::Float64);
        assert_eq!(
            schema.field(5).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(schema.field(7).data_type(), &DataType::Boolean);
    }

    /// Full bridge table schema from real Databricks `information_schema`,
    /// using `full_data_type` values (bigint, string, timestamp, boolean).
    #[test]
    fn test_schema_from_json_real_bridge_table_full_data_type() {
        let response = make_schema_response(&json!([
            ["bridge_id", "bigint", "YES"],
            ["entity_id", "bigint", "YES"],
            ["entity_skey", "bigint", "YES"],
            ["entity_hkey", "string", "YES"],
            ["snapshot_id", "bigint", "YES"],
            ["related_id", "bigint", "YES"],
            ["related_address", "string", "YES"],
            ["related_skey", "bigint", "YES"],
            ["related_hkey", "string", "YES"],
            ["created_datetime_utc", "timestamp", "YES"],
            ["updated_datetime_utc", "timestamp", "YES"],
            ["valid_from_datetime", "timestamp", "YES"],
            ["end_datetime", "timestamp", "YES"],
            ["is_current_flag", "boolean", "YES"],
            ["is_deleted_flag", "boolean", "YES"]
        ]));

        let schema = schema_from_json(&response, "catalog.test_schema.bridge_entities")
            .expect("should parse bridge table schema");
        assert_eq!(schema.fields().len(), 15);
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(6).data_type(), &DataType::Utf8);
        assert_eq!(
            schema.field(12).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(schema.field(13).data_type(), &DataType::Boolean);
    }

    /// Same bridge table but using `data_type` column values (LONG instead of bigint).
    #[test]
    fn test_schema_from_json_real_bridge_table_data_type_fallback() {
        let response = make_schema_response(&json!([
            ["bridge_id", "LONG", "YES"],
            ["entity_id", "LONG", "YES"],
            ["entity_skey", "LONG", "YES"],
            ["entity_hkey", "STRING", "YES"],
            ["snapshot_id", "LONG", "YES"],
            ["related_id", "LONG", "YES"],
            ["related_address", "STRING", "YES"],
            ["related_skey", "LONG", "YES"],
            ["related_hkey", "STRING", "YES"],
            ["created_datetime_utc", "TIMESTAMP", "YES"],
            ["updated_datetime_utc", "TIMESTAMP", "YES"],
            ["valid_from_datetime", "TIMESTAMP", "YES"],
            ["end_datetime", "TIMESTAMP", "YES"],
            ["is_current_flag", "BOOLEAN", "YES"],
            ["is_deleted_flag", "BOOLEAN", "YES"]
        ]));

        let schema = schema_from_json(&response, "catalog.test_schema.bridge_entities")
            .expect("should parse bridge table with data_type fallback values");
        assert_eq!(schema.fields().len(), 15);
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(6).data_type(), &DataType::Utf8);
        assert_eq!(
            schema.field(12).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(schema.field(13).data_type(), &DataType::Boolean);
    }

    /// Cohorted spend view — `full_data_type` column: date, string, decimal(26,2), int.
    #[test]
    fn test_schema_from_json_real_cohorted_spend_full_data_type() {
        let response = make_schema_response(&json!([
            ["spend_dt", "date", "YES"],
            ["source_nm", "string", "YES"],
            ["channel_id", "string", "YES"],
            ["campaign_nm", "string", "YES"],
            ["brand_campaign_nm", "string", "YES"],
            ["state_cd", "string", "YES"],
            ["county_cd", "string", "YES"],
            ["designated_market_area", "string", "YES"],
            ["zip_code_cd", "string", "YES"],
            ["region_cd", "string", "YES"],
            ["lead_tier_id", "string", "YES"],
            ["sub_id", "string", "YES"],
            ["publisher_nm", "string", "YES"],
            ["non_experiment__decimal_amt", "decimal(26,2)", "YES"],
            ["mrktng_spnd_amt", "decimal(26,2)", "YES"],
            ["mrktng_spnd_exprmnt_amt", "decimal(26,2)", "YES"],
            ["lead_cnt", "int", "YES"],
            ["new_lead_cnt", "int", "YES"],
            ["returning_lead_cnt", "int", "YES"],
            ["leads_with_assgnmnts_cnt", "int", "YES"],
            ["leads_with_any_rep_assgnmnts_cnt", "int", "YES"],
            ["bind_cnt", "int", "YES"],
            ["cohort_binds_0_days_amt", "decimal(26,2)", "YES"],
            ["cohort_binds_1_days_amt", "decimal(26,2)", "YES"],
            ["cohort_binds_3_days_amt", "decimal(26,2)", "YES"],
            ["cohort_binds_7_days_amt", "decimal(26,2)", "YES"],
            ["cohort_binds_14_days_amt", "decimal(26,2)", "YES"],
            ["cohort_binds_30_days_amt", "decimal(26,2)", "YES"],
            ["cohort_binds_45_days_amt", "decimal(26,2)", "YES"],
            ["cohort_binds_60_days_amt", "decimal(26,2)", "YES"],
            ["cohort_binds_90_days_amt", "decimal(26,2)", "YES"],
            ["cohort_bound_premium_0_days_amt", "decimal(26,2)", "YES"],
            ["cohort_bound_premium_1_days_amt", "decimal(26,2)", "YES"],
            ["cohort_bound_premium_3_days_amt", "decimal(26,2)", "YES"],
            ["cohort_bound_premium_7_days_amt", "decimal(26,2)", "YES"],
            ["cohort_bound_premium_14_days_amt", "decimal(26,2)", "YES"],
            ["cohort_bound_premium_30_days_amt", "decimal(26,2)", "YES"],
            ["cohort_bound_premium_45_days_amt", "decimal(26,2)", "YES"],
            ["cohort_bound_premium_60_days_amt", "decimal(26,2)", "YES"],
            ["cohort_bound_premium_90_days_amt", "decimal(26,2)", "YES"],
            ["bound_premium_amt", "decimal(26,2)", "YES"]
        ]));

        let schema = schema_from_json(
            &response,
            "catalog.ext_dbt_dwh_v.mart_mrktng_cohorted_spend_v",
        )
        .expect("should parse cohorted spend schema");
        assert_eq!(schema.fields().len(), 41);
        assert_eq!(schema.field(0).data_type(), &DataType::Date32);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(13).data_type(), &DataType::Decimal128(26, 2));
        assert_eq!(schema.field(16).data_type(), &DataType::Int32);
    }

    /// Same cohorted spend view using `data_type` column (DATE, STRING, DECIMAL, INT).
    #[test]
    fn test_schema_from_json_real_cohorted_spend_data_type_fallback() {
        let response = make_schema_response(&json!([
            ["spend_dt", "DATE", "YES"],
            ["source_nm", "STRING", "YES"],
            ["channel_id", "STRING", "YES"],
            ["campaign_nm", "STRING", "YES"],
            ["brand_campaign_nm", "STRING", "YES"],
            ["state_cd", "STRING", "YES"],
            ["county_cd", "STRING", "YES"],
            ["designated_market_area", "STRING", "YES"],
            ["zip_code_cd", "STRING", "YES"],
            ["region_cd", "STRING", "YES"],
            ["lead_tier_id", "STRING", "YES"],
            ["sub_id", "STRING", "YES"],
            ["publisher_nm", "STRING", "YES"],
            ["non_experiment__decimal_amt", "DECIMAL", "YES"],
            ["mrktng_spnd_amt", "DECIMAL", "YES"],
            ["mrktng_spnd_exprmnt_amt", "DECIMAL", "YES"],
            ["lead_cnt", "INT", "YES"],
            ["new_lead_cnt", "INT", "YES"],
            ["returning_lead_cnt", "INT", "YES"],
            ["leads_with_assgnmnts_cnt", "INT", "YES"],
            ["leads_with_any_rep_assgnmnts_cnt", "INT", "YES"],
            ["bind_cnt", "INT", "YES"],
            ["cohort_binds_0_days_amt", "DECIMAL", "YES"],
            ["cohort_binds_1_days_amt", "DECIMAL", "YES"],
            ["cohort_binds_3_days_amt", "DECIMAL", "YES"],
            ["cohort_binds_7_days_amt", "DECIMAL", "YES"],
            ["cohort_binds_14_days_amt", "DECIMAL", "YES"],
            ["cohort_binds_30_days_amt", "DECIMAL", "YES"],
            ["cohort_binds_45_days_amt", "DECIMAL", "YES"],
            ["cohort_binds_60_days_amt", "DECIMAL", "YES"],
            ["cohort_binds_90_days_amt", "DECIMAL", "YES"],
            ["cohort_bound_premium_0_days_amt", "DECIMAL", "YES"],
            ["cohort_bound_premium_1_days_amt", "DECIMAL", "YES"],
            ["cohort_bound_premium_3_days_amt", "DECIMAL", "YES"],
            ["cohort_bound_premium_7_days_amt", "DECIMAL", "YES"],
            ["cohort_bound_premium_14_days_amt", "DECIMAL", "YES"],
            ["cohort_bound_premium_30_days_amt", "DECIMAL", "YES"],
            ["cohort_bound_premium_45_days_amt", "DECIMAL", "YES"],
            ["cohort_bound_premium_60_days_amt", "DECIMAL", "YES"],
            ["cohort_bound_premium_90_days_amt", "DECIMAL", "YES"],
            ["bound_premium_amt", "DECIMAL", "YES"]
        ]));

        let schema = schema_from_json(
            &response,
            "catalog.ext_dbt_dwh_v.mart_mrktng_cohorted_spend_v",
        )
        .expect("should parse cohorted spend with data_type fallback");
        assert_eq!(schema.fields().len(), 41);
        assert_eq!(schema.field(0).data_type(), &DataType::Date32);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        // DECIMAL without params falls back to Decimal128(38,10)
        assert_eq!(schema.field(13).data_type(), &DataType::Decimal128(38, 10));
        assert_eq!(schema.field(16).data_type(), &DataType::Int32);
    }

    /// Schema with geometry, decimal(22,4), and all common scalar types
    /// from a real Databricks `information_schema` dump using `full_data_type`.
    #[test]
    fn test_schema_from_json_real_mixed_types_with_geometry() {
        let response = make_schema_response(&json!([
            ["id", "string", "YES"],
            ["distance_ft", "double", "YES"],
            ["state", "string", "YES"],
            ["score", "double", "YES"],
            ["count", "int", "YES"],
            ["flag", "int", "YES"],
            ["created_at", "timestamp", "YES"],
            ["census", "bigint", "YES"],
            ["coverage_a", "decimal(22,4)", "YES"],
            ["spend_dt", "date", "YES"],
            ["geom", "geometry(5070)", "YES"]
        ]));

        let schema = schema_from_json(&response, "catalog.test_schema.mixed_types")
            .expect("should parse mixed types including geometry");
        assert_eq!(schema.fields().len(), 11);
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(1).data_type(), &DataType::Float64);
        assert_eq!(schema.field(4).data_type(), &DataType::Int32);
        assert_eq!(
            schema.field(6).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(schema.field(7).data_type(), &DataType::Int64);
        assert_eq!(schema.field(8).data_type(), &DataType::Decimal128(22, 4));
        assert_eq!(schema.field(9).data_type(), &DataType::Date32);
        assert_eq!(
            schema.field(10).data_type(),
            &DataType::Binary,
            "GEOMETRY should map to Binary"
        );
    }

    /// Same mixed schema using `data_type` column (uppercase, GEOMETRY without SRID).
    #[test]
    fn test_schema_from_json_real_mixed_types_data_type_fallback() {
        let response = make_schema_response(&json!([
            ["id", "STRING", "YES"],
            ["distance_ft", "DOUBLE", "YES"],
            ["state", "STRING", "YES"],
            ["score", "DOUBLE", "YES"],
            ["count", "INT", "YES"],
            ["flag", "INT", "YES"],
            ["created_at", "TIMESTAMP", "YES"],
            ["census", "LONG", "YES"],
            ["coverage_a", "DECIMAL", "YES"],
            ["spend_dt", "DATE", "YES"],
            ["geom", "GEOMETRY", "YES"]
        ]));

        let schema = schema_from_json(&response, "catalog.test_schema.mixed_types")
            .expect("should parse mixed types with data_type fallback");
        assert_eq!(schema.fields().len(), 11);
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(1).data_type(), &DataType::Float64);
        assert_eq!(schema.field(4).data_type(), &DataType::Int32);
        assert_eq!(
            schema.field(6).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(schema.field(7).data_type(), &DataType::Int64);
        // DECIMAL without params falls back to Decimal128(38,10)
        assert_eq!(schema.field(8).data_type(), &DataType::Decimal128(38, 10));
        assert_eq!(schema.field(9).data_type(), &DataType::Date32);
        assert_eq!(
            schema.field(10).data_type(),
            &DataType::Binary,
            "GEOMETRY should map to Binary"
        );
    }

    // ---- DESCRIBE TABLE fallback tests ----

    /// Basic DESCRIBE TABLE response: [`col_name`, `data_type`, comment].
    /// All columns should default to nullable.
    #[test]
    fn test_schema_from_describe_json_basic() {
        let response = make_schema_response(&json!([
            ["id", "int", "primary key"],
            ["name", "string", "user name"],
            ["amount", "double", ""]
        ]));

        let schema = schema_from_describe_json(&response, "test_table")
            .expect("should parse DESCRIBE TABLE response");
        assert_eq!(schema.fields().len(), 3);

        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert!(
            schema.field(0).is_nullable(),
            "DESCRIBE TABLE defaults to nullable"
        );

        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert!(schema.field(1).is_nullable());

        assert_eq!(schema.field(2).name(), "amount");
        assert_eq!(schema.field(2).data_type(), &DataType::Float64);
    }

    /// DESCRIBE TABLE with metadata rows (partition info) after a blank separator.
    #[test]
    fn test_schema_from_describe_json_with_metadata_rows() {
        let response = make_schema_response(&json!([
            ["id", "int", ""],
            ["name", "string", ""],
            ["", "", ""],
            ["# Partition Information", "", ""],
            ["# col_name", "data_type", "comment"],
            ["part_col", "string", ""]
        ]));

        let schema = schema_from_describe_json(&response, "test_table")
            .expect("should stop before metadata rows");
        assert_eq!(schema.fields().len(), 2, "only real columns, not metadata");
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
    }

    /// DESCRIBE TABLE for a Lakehouse Federation foreign table using
    /// Spark SQL types (uppercase, no params on DECIMAL).
    #[test]
    fn test_schema_from_describe_json_federation_table() {
        let response = make_schema_response(&json!([
            ["record_skey", "LONG", ""],
            ["record_hkey", "STRING", ""],
            ["address_latitude", "DOUBLE", ""],
            ["start_datetime", "TIMESTAMP", ""],
            ["is_current_flag", "BOOLEAN", ""],
            ["spend_dt", "DATE", ""],
            ["total_amt", "DECIMAL", ""],
            ["count", "INT", ""],
            ["geom", "GEOMETRY", ""]
        ]));

        let schema = schema_from_describe_json(&response, "neon_pg_foreign.public.test_table")
            .expect("should parse federation table DESCRIBE");
        assert_eq!(schema.fields().len(), 9);

        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(2).data_type(), &DataType::Float64);
        assert_eq!(
            schema.field(3).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(schema.field(4).data_type(), &DataType::Boolean);
        assert_eq!(schema.field(5).data_type(), &DataType::Date32);
        assert_eq!(schema.field(6).data_type(), &DataType::Decimal128(38, 10));
        assert_eq!(schema.field(7).data_type(), &DataType::Int32);
        assert_eq!(schema.field(8).data_type(), &DataType::Binary);

        // All columns should be nullable
        for field in schema.fields() {
            assert!(field.is_nullable(), "{} should be nullable", field.name());
        }
    }

    /// DESCRIBE TABLE with only blank/metadata rows should return an error.
    #[test]
    fn test_schema_from_describe_json_no_columns() {
        let response = make_schema_response(&json!([
            ["", "", ""],
            ["# Detailed Table Information", "", ""],
            ["Database", "test_schema", ""]
        ]));

        let err = schema_from_describe_json(&response, "test_table")
            .expect_err("should fail with no columns");
        assert!(
            matches!(&err, Error::NoColumnsInDataset { .. }),
            "expected NoColumnsInDataset, got {err:?}"
        );
    }

    /// DESCRIBE TABLE with missing `data_array` should return an error.
    #[test]
    fn test_schema_from_describe_json_missing_data_array() {
        let response = json!({
            "status": { "state": "SUCCEEDED" },
            "statement_id": "test-stmt-id",
            "result": {}
        });

        let err = schema_from_describe_json(&response, "test_table")
            .expect_err("should fail with missing data_array");
        assert!(
            matches!(&err, Error::UnexpectedSchemaResponse { .. }),
            "expected UnexpectedSchemaResponse, got {err:?}"
        );
    }

    /// DESCRIBE TABLE fallback for the `dim_records` CSV schema.
    /// Types match the `data_type` column: LONG, STRING, DOUBLE,
    /// TIMESTAMP, BOOLEAN.
    #[test]
    fn test_schema_from_describe_json_dim_records() {
        let response = make_schema_response(&json!([
            ["record_skey", "LONG", ""],
            ["record_hkey", "STRING", ""],
            ["address_city", "STRING", ""],
            ["address_latitude", "DOUBLE", ""],
            ["address_longitude", "DOUBLE", ""],
            ["start_datetime", "TIMESTAMP", ""],
            ["end_datetime", "TIMESTAMP", ""],
            ["is_current_flag", "BOOLEAN", ""],
            ["is_deleted_flag", "BOOLEAN", ""]
        ]));

        let schema = schema_from_describe_json(&response, "neon_pg_foreign.public.dim_records")
            .expect("should parse dim_records DESCRIBE");
        assert_eq!(schema.fields().len(), 9);
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(3).data_type(), &DataType::Float64);
        assert_eq!(
            schema.field(5).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(schema.field(7).data_type(), &DataType::Boolean);
        for field in schema.fields() {
            assert!(field.is_nullable(), "{} should be nullable", field.name());
        }
    }

    /// DESCRIBE TABLE fallback for the `bridge_entities` CSV schema.
    /// Types: LONG, STRING, TIMESTAMP, BOOLEAN.
    #[test]
    fn test_schema_from_describe_json_bridge_entities() {
        let response = make_schema_response(&json!([
            ["bridge_id", "LONG", ""],
            ["entity_id", "LONG", ""],
            ["entity_skey", "LONG", ""],
            ["entity_hkey", "STRING", ""],
            ["snapshot_id", "LONG", ""],
            ["related_id", "LONG", ""],
            ["related_address", "STRING", ""],
            ["related_skey", "LONG", ""],
            ["related_hkey", "STRING", ""],
            ["created_datetime_utc", "TIMESTAMP", ""],
            ["updated_datetime_utc", "TIMESTAMP", ""],
            ["valid_from_datetime", "TIMESTAMP", ""],
            ["end_datetime", "TIMESTAMP", ""],
            ["is_current_flag", "BOOLEAN", ""],
            ["is_deleted_flag", "BOOLEAN", ""]
        ]));

        let schema = schema_from_describe_json(&response, "neon_pg_foreign.public.bridge_entities")
            .expect("should parse bridge_entities DESCRIBE");
        assert_eq!(schema.fields().len(), 15);
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(6).data_type(), &DataType::Utf8);
        assert_eq!(
            schema.field(12).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(schema.field(13).data_type(), &DataType::Boolean);
        for field in schema.fields() {
            assert!(field.is_nullable(), "{} should be nullable", field.name());
        }
    }

    /// DESCRIBE TABLE fallback for the `cohorted_spend` CSV schema.
    /// Types: DATE, STRING, DECIMAL (bare), INT.
    #[test]
    fn test_schema_from_describe_json_cohorted_spend() {
        let response = make_schema_response(&json!([
            ["spend_dt", "DATE", ""],
            ["source_nm", "STRING", ""],
            ["channel_id", "STRING", ""],
            ["campaign_nm", "STRING", ""],
            ["brand_campaign_nm", "STRING", ""],
            ["state_cd", "STRING", ""],
            ["county_cd", "STRING", ""],
            ["designated_market_area", "STRING", ""],
            ["zip_code_cd", "STRING", ""],
            ["region_cd", "STRING", ""],
            ["lead_tier_id", "STRING", ""],
            ["sub_id", "STRING", ""],
            ["publisher_nm", "STRING", ""],
            ["non_experiment__decimal_amt", "DECIMAL", ""],
            ["mrktng_spnd_amt", "DECIMAL", ""],
            ["mrktng_spnd_exprmnt_amt", "DECIMAL", ""],
            ["lead_cnt", "INT", ""],
            ["new_lead_cnt", "INT", ""],
            ["returning_lead_cnt", "INT", ""],
            ["leads_with_assgnmnts_cnt", "INT", ""],
            ["leads_with_any_rep_assgnmnts_cnt", "INT", ""],
            ["bind_cnt", "INT", ""],
            ["cohort_binds_0_days_amt", "DECIMAL", ""],
            ["cohort_binds_1_days_amt", "DECIMAL", ""],
            ["cohort_binds_3_days_amt", "DECIMAL", ""],
            ["cohort_binds_7_days_amt", "DECIMAL", ""],
            ["cohort_binds_14_days_amt", "DECIMAL", ""],
            ["cohort_binds_30_days_amt", "DECIMAL", ""],
            ["cohort_binds_45_days_amt", "DECIMAL", ""],
            ["cohort_binds_60_days_amt", "DECIMAL", ""],
            ["cohort_binds_90_days_amt", "DECIMAL", ""],
            ["cohort_bound_premium_0_days_amt", "DECIMAL", ""],
            ["cohort_bound_premium_1_days_amt", "DECIMAL", ""],
            ["cohort_bound_premium_3_days_amt", "DECIMAL", ""],
            ["cohort_bound_premium_7_days_amt", "DECIMAL", ""],
            ["cohort_bound_premium_14_days_amt", "DECIMAL", ""],
            ["cohort_bound_premium_30_days_amt", "DECIMAL", ""],
            ["cohort_bound_premium_45_days_amt", "DECIMAL", ""],
            ["cohort_bound_premium_60_days_amt", "DECIMAL", ""],
            ["cohort_bound_premium_90_days_amt", "DECIMAL", ""],
            ["bound_premium_amt", "DECIMAL", ""]
        ]));

        let schema = schema_from_describe_json(
            &response,
            "neon_pg_foreign.ext_dbt_dwh_v.mart_mrktng_cohorted_spend_v",
        )
        .expect("should parse cohorted spend DESCRIBE");
        assert_eq!(schema.fields().len(), 41);
        assert_eq!(schema.field(0).data_type(), &DataType::Date32);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        // DECIMAL without params falls back to Decimal128(38,10)
        assert_eq!(schema.field(13).data_type(), &DataType::Decimal128(38, 10));
        assert_eq!(schema.field(16).data_type(), &DataType::Int32);
        for field in schema.fields() {
            assert!(field.is_nullable(), "{} should be nullable", field.name());
        }
    }

    /// DESCRIBE TABLE fallback for the mixed types CSV schema with GEOMETRY.
    /// Types: STRING, DOUBLE, INT, TIMESTAMP, LONG, DATE, DECIMAL, GEOMETRY.
    #[test]
    fn test_schema_from_describe_json_mixed_types() {
        let response = make_schema_response(&json!([
            ["id", "STRING", ""],
            ["distance_ft", "DOUBLE", ""],
            ["state", "STRING", ""],
            ["score", "DOUBLE", ""],
            ["count", "INT", ""],
            ["flag", "INT", ""],
            ["created_at", "TIMESTAMP", ""],
            ["census", "LONG", ""],
            ["coverage_a", "DECIMAL", ""],
            ["spend_dt", "DATE", ""],
            ["geom", "GEOMETRY", ""]
        ]));

        let schema = schema_from_describe_json(&response, "neon_pg_foreign.public.mixed_types")
            .expect("should parse mixed types DESCRIBE");
        assert_eq!(schema.fields().len(), 11);
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(1).data_type(), &DataType::Float64);
        assert_eq!(schema.field(4).data_type(), &DataType::Int32);
        assert_eq!(
            schema.field(6).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(schema.field(7).data_type(), &DataType::Int64);
        assert_eq!(schema.field(8).data_type(), &DataType::Decimal128(38, 10));
        assert_eq!(schema.field(9).data_type(), &DataType::Date32);
        assert_eq!(
            schema.field(10).data_type(),
            &DataType::Binary,
            "GEOMETRY should map to Binary"
        );
        for field in schema.fields() {
            assert!(field.is_nullable(), "{} should be nullable", field.name());
        }
    }

    /// DESCRIBE TABLE response for a Neon `PostgreSQL` foreign table.
    /// DESCRIBE TABLE returns Spark SQL types (consistent with `full_data_type`).
    #[test]
    fn test_schema_from_describe_json_neon_pg_table() {
        let response = make_schema_response(&json!([
            ["id", "int", ""],
            ["name", "string", ""],
            ["amount", "decimal(10,2)", ""],
            ["created_at", "timestamp", ""],
            ["active", "boolean", ""]
        ]));

        let schema =
            schema_from_describe_json(&response, "neon_pg_foreign.public.test_schema_repro")
                .expect("should parse Neon PG DESCRIBE TABLE");
        assert_eq!(schema.fields().len(), 5);
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(2).data_type(), &DataType::Decimal128(10, 2));
        assert_eq!(
            schema.field(3).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(schema.field(4).data_type(), &DataType::Boolean);
        for field in schema.fields() {
            assert!(field.is_nullable(), "{} should be nullable", field.name());
        }
    }

    /// `information_schema.columns.data_type` for a Neon `PostgreSQL` foreign
    /// table returns source-native type names (`integer`, `text`, `numeric`,
    /// `timestamp without time zone`). These must parse correctly.
    #[test]
    fn test_schema_from_json_neon_pg_native_types() {
        let response = make_schema_response(&json!([
            ["id", "integer", "YES"],
            ["name", "text", "YES"],
            ["amount", "numeric", "YES"],
            ["created_at", "timestamp without time zone", "YES"],
            ["active", "boolean", "YES"]
        ]));

        let schema = schema_from_json(&response, "neon_pg_foreign.public.test_schema_repro")
            .expect("should parse Neon PG source-native types");
        assert_eq!(schema.fields().len(), 5);
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(2).data_type(), &DataType::Decimal128(38, 10));
        assert_eq!(
            schema.field(3).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            "timestamp without time zone maps to Timestamp(Microsecond, None)"
        );
        assert_eq!(schema.field(4).data_type(), &DataType::Boolean);
    }
}
