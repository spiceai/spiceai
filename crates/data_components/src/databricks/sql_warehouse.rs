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
    array::RecordBatch,
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
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
use std::{io::Cursor, pin::Pin, sync::Arc};
use token_provider::TokenProvider;

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
}

/// Main struct for interacting with Databricks SQL Warehouse
pub struct DatabricksSqlWarehouse {
    pool: Arc<dyn DbConnectionPool<Arc<SqlWarehouseApi>, &'static (dyn Sync)> + Send + Sync>,
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
        let sql = format!("DESCRIBE TABLE {table}");
        let payload = self.create_schema_payload(table, &sql)?;

        let response = self.execute_request(&token, &payload).await?;
        schema_from_json(&response)
    }

    fn create_schema_payload(&self, table: &TableReference, sql: &str) -> Result<Value, Error> {
        Ok(json!({
            "warehouse_id": self.sql_warehouse_id,
            "catalog": table.catalog().ok_or_else(|| Error::FullyQualifiedPath{ reason: "missing catalog".into() })?,
            "schema": table.schema().ok_or_else(|| Error::FullyQualifiedPath{ reason: "missing schema".into() })?,
            "statement": sql,
        }))
    }

    async fn execute_request(&self, token: &str, payload: &Value) -> Result<Value, Error> {
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

    // Fetch the arrow data at the external links, repeating for each chunk
    async fn fetch_external_links(
        self: Arc<Self>,
        result_object: Value,
    ) -> Result<SendableRecordBatchStream, Error> {
        let token = self.token_provider.get_token();
        let initial_external_link = Self::extract_external_links(result_object)?;

        let token = token.to_string();
        let stream = stream::unfold(Some(initial_external_link), move |current_link| {
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
                                Ok(next) => Some(next),
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

    /// Deserializes the first [`ExternalLink`] in the `external_links` array
    fn extract_external_links(mut response: Value) -> Result<ExternalLink, Error> {
        let links = response
            .get_mut("external_links")
            .map(Value::take)
            .ok_or_else(|| {
                MissingJsonFieldSnafu {
                    field: "external_links",
                }
                .build()
            })?;

        let Value::Array(mut links) = links else {
            return Err(Error::InvalidJsonArray {
                field: "external_links".into(),
            });
        };

        // Only ever returns 1 external link in the array
        let link = links.pop().ok_or_else(|| Error::InvalidJsonArray {
            field: "external_links".into(),
        })?;

        serde_json::from_value(link).context(DeserializeExternalLinkFailedSnafu)
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
}

#[derive(Debug, Deserialize, Serialize)]
struct ExternalLink {
    chunk_index: u64,
    #[allow(clippy::struct_field_names)]
    external_link: String,
    next_chunk_internal_link: Option<String>,
}

fn map_databricks_type(type_name: &str) -> Result<DataType, Error> {
    let type_name_upper = type_name.to_uppercase();

    if type_name_upper.starts_with("ARRAY") {
        let inner_type = type_name
            .split('<')
            .nth(1)
            .ok_or_else(|| Error::UnsupportedType {
                ty: type_name.to_string(),
            })?
            .trim_end_matches('>')
            .trim();

        let inner_data_type = map_databricks_type(inner_type)?;
        return Ok(DataType::List(Arc::new(Field::new(
            "item",
            inner_data_type,
            true,
        ))));
    }

    if type_name_upper.starts_with("DECIMAL") {
        let (precision, scale) = match type_name.split_once('(') {
            Some((_, params)) => {
                let params = params.trim_end_matches(')').trim();
                let parts: Vec<_> = params.split(',').map(str::trim).collect();
                if parts.len() != 2 {
                    return Err(Error::UnsupportedType {
                        ty: type_name.to_string(),
                    });
                }
                let precision: u8 = parts[0].parse().map_err(|_| Error::UnsupportedType {
                    ty: type_name.to_string(),
                })?;
                let scale: i8 = parts[1].parse().map_err(|_| Error::UnsupportedType {
                    ty: type_name.to_string(),
                })?;
                (precision, scale)
            }
            None => (10, 0),
        };
        return Ok(DataType::Decimal128(precision, scale));
    }

    Ok(match type_name_upper.as_str() {
        "BOOLEAN" => DataType::Boolean,
        "TINYINT" => DataType::Int8,
        "SMALLINT" => DataType::Int16,
        "INT" => DataType::Int32,
        "BIGINT" => DataType::Int64,
        "FLOAT" => DataType::Float32,
        "DOUBLE" => DataType::Float64,
        "STRING" | "CHAR" | "VARCHAR" => DataType::Utf8,
        "BINARY" => DataType::Binary,
        "DATE" => DataType::Date32,
        "TIMESTAMP" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "TIMESTAMP_NTZ" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "VOID" => DataType::Null,
        ty => return Err(Error::UnsupportedType { ty: ty.to_string() }),
    })
}

fn schema_from_json(json_value: &Value) -> Result<SchemaRef, Error> {
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

        if row_array.len() < 2 {
            return Err(Error::UnableToRetrieveSchema {
                reason: format!("data_array[{i}] lacks col_name or data_type"),
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

        let field = Field::new(col_name, map_databricks_type(data_type_str)?, true);

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
            "statement": sql,
        });

        let mut response = self.api.execute_request(&token, &payload).await?;

        // Get the result object
        let result_object = response
            .get_mut("result")
            .map(Value::take)
            .ok_or_else(|| MissingJsonFieldSnafu { field: "result" }.build())?;

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
        schema: Option<SchemaRef>,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        let dialect = Arc::new(databricks_dialect());

        let table_provider = match schema {
            Some(schema) => Arc::new(
                SqlTable::new_with_schema("databricks", &self.pool, schema, table_reference, None)
                    .with_dialect(dialect),
            ),
            None => Arc::new(
                SqlTable::new("databricks", &self.pool, table_reference, None)
                    .await
                    .context(SqlTableInitializationFailedSnafu)?
                    .with_dialect(dialect),
            ),
        };

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

    fn timestamp_type(tz: Option<String>) -> DataType {
        DataType::Timestamp(TimeUnit::Microsecond, tz.map(Into::into))
    }

    #[test]
    fn test_map_databricks_type() {
        let test_cases = &[
            ("BOOLEAN", Ok(DataType::Boolean), "BOOLEAN mapping failed"),
            ("TINYINT", Ok(DataType::Int8), "TINYINT mapping failed"),
            ("SMALLINT", Ok(DataType::Int16), "SMALLINT mapping failed"),
            ("INT", Ok(DataType::Int32), "INT mapping failed"),
            ("BIGINT", Ok(DataType::Int64), "BIGINT mapping failed"),
            ("FLOAT", Ok(DataType::Float32), "FLOAT mapping failed"),
            ("DOUBLE", Ok(DataType::Float64), "DOUBLE mapping failed"),
            ("STRING", Ok(DataType::Utf8), "STRING mapping failed"),
            ("CHAR", Ok(DataType::Utf8), "CHAR mapping failed"),
            ("VARCHAR", Ok(DataType::Utf8), "VARCHAR mapping failed"),
            ("BINARY", Ok(DataType::Binary), "BINARY mapping failed"),
            ("DATE", Ok(DataType::Date32), "DATE mapping failed"),
            (
                "TIMESTAMP",
                Ok(timestamp_type(Some("UTC".into()))),
                "TIMESTAMP mapping failed",
            ),
            (
                "TIMESTAMP_NTZ",
                Ok(timestamp_type(None)),
                "TIMESTAMP_NTZ mapping failed",
            ),
            ("VOID", Ok(DataType::Null), "VOID mapping failed"),
            (
                "DECIMAL(8,4)",
                Ok(DataType::Decimal128(8, 4)),
                "DECIMAL(8,4) mapping failed",
            ),
            (
                "DECIMAL",
                Ok(DataType::Decimal128(10, 0)),
                "Plain DECIMAL mapping failed",
            ),
            (
                "DECIMAL(10,2)",
                Ok(DataType::Decimal128(10, 2)),
                "DECIMAL(10,2) mapping failed",
            ),
            (
                "decimal(5,0)",
                Ok(DataType::Decimal128(5, 0)),
                "Case-insensitive DECIMAL(5,0) mapping failed",
            ),
            (
                "ARRAY<STRING>",
                Ok(DataType::new_list(DataType::Utf8, true)),
                "ARRAY<STRING> mapping failed",
            ),
            (
                "UNKNOWN",
                Err(Error::UnsupportedType {
                    ty: "UNKNOWN".to_string(),
                }),
                "UNKNOWN type should fail",
            ),
            (
                "DECIMAL(abc)",
                Err(Error::UnsupportedType {
                    ty: "DECIMAL(abc)".to_string(),
                }),
                "Malformed DECIMAL should fail",
            ),
            (
                "DECIMAL(8,)",
                Err(Error::UnsupportedType {
                    ty: "DECIMAL(8,)".to_string(),
                }),
                "Incomplete DECIMAL parameters should fail",
            ),
        ];

        for (input, expected, error_msg) in test_cases {
            let result = map_databricks_type(input);
            match (result, expected) {
                (Ok(got), Ok(want)) => assert_eq!(got, *want, "{error_msg}"),
                (Err(_), Err(_)) => {}
                _ => panic!("{error_msg}"),
            }
        }
    }
}
