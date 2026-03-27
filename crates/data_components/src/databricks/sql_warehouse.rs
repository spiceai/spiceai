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
        "The table '{dataset_name}' has no column metadata registered. Run 'SELECT * FROM {dataset_name} LIMIT 1' in Databricks to populate the schema. For details, visit: https://spiceai.org/docs/components/data-connectors/databricks"
    ))]
    TableSchemaNotRegistered { dataset_name: String },

    #[snafu(display(
        "Failed to load the table '{dataset_name}' from Databricks: unexpected schema response format. Report a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    UnexpectedSchemaResponse { dataset_name: String, reason: String },

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
        let client = ClientBuilder::new()
            .user_agent(super::user_agent())
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
        let payload = self.create_schema_payload(table)?;
        let response = self.execute_sql_statement(&token, &payload).await?;
        let response = self.wait_for_statement_completion(&token, response).await?;
        schema_from_json(&response, &table.to_string())
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

    async fn execute_sql_statement(&self, token: &str, payload: &Value) -> Result<Value, Error> {
        let url = format!("{}/api/2.0/sql/statements/", self.base_url);
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
        let url = format!("{}/api/2.0/sql/statements/{statement_id}", self.base_url);
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
                        let url = format!("{}{path}", api.base_url);
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

    let data_array = json_value
        .get("result")
        .and_then(|r| r.get("data_array"))
        .and_then(|d| d.as_array())
        .ok_or_else(|| Error::TableSchemaNotRegistered {
            dataset_name: dataset_name.to_string(),
        })?;

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
                reason: format!(
                    "data_array[{i}] has fewer than 3 fields"
                ),
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

        let data_type_str = row_array[1]
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
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, Some("UTC".into()))
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
            matches!(&err, Error::TableSchemaNotRegistered { .. }),
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
            matches!(&err, Error::TableSchemaNotRegistered { .. }),
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
            .create_schema_payload(&table)
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
            .create_schema_payload(&table)
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
            .create_schema_payload(&table)
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
            .create_schema_payload(&table)
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

    fn create_test_api(port: u16) -> SqlWarehouseApi {
        let client = ClientBuilder::new().build().expect("should build client");
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
}
