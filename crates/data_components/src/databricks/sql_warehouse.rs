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
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
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
use futures::{StreamExt, TryStreamExt, stream};
use reqwest::{Client, ClientBuilder};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use snafu::{Snafu, prelude::*};
use std::{io::Cursor, sync::Arc};
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
        JoinPushDown::Disallow
    }
}

struct SqlWarehouseApi {
    client: Client,
    url: String,
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

        let url = format!("https://{host}/api/2.0/sql/statements/");

        Ok(Self {
            client,
            url,
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
        self.client
            .post(&self.url)
            .bearer_auth(token)
            .json(payload)
            .send()
            .await
            .context(HttpRequestFailedSnafu)?
            .json()
            .await
            .context(JsonParsingFailedSnafu)
    }

    async fn fetch_arrow_streams(
        &self,
        response: Value,
    ) -> Result<SendableRecordBatchStream, Error> {
        let external_links = Self::extract_external_links(response)?;
        let mut streams = Vec::new();

        for link in external_links {
            tracing::trace!(
                "Fetching chunk {} from {}",
                link.chunk_index,
                link.external_link
            );
            let bytes = self.fetch_chunk_data(&link.external_link).await?;
            let batches = Self::read_arrow_batches(bytes)?;
            streams.push(futures::stream::iter(batches.into_iter().map(Ok)));
        }

        let mut combined_stream = stream::select_all(streams);

        let first_batch = match combined_stream.next().await {
            Some(Ok(batch)) => batch,
            None => {
                return Ok(Box::pin(RecordBatchStreamAdapter::new(
                    Arc::new(Schema::empty()),
                    stream::empty(),
                )));
            }
            Some(Err(e)) => return Err(Error::ArrowStreamReadFailed { source: e }),
        };

        let schema = first_batch.schema();
        let run_once = stream::once(async move { Ok(first_batch) });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            Box::pin(
                run_once
                    .chain(combined_stream)
                    .map_err(|e| DataFusionError::Execution(e.to_string())),
            ),
        )))
    }

    fn extract_external_links(mut response: Value) -> Result<Vec<ExternalLink>, Error> {
        let links = response
            .get_mut("result")
            .and_then(|result| result.get_mut("external_links").map(Value::take))
            .ok_or_else(|| {
                MissingJsonFieldSnafu {
                    field: "result.external_links",
                }
                .build()
            })?;

        let Value::Array(links) = links else {
            return Err(Error::InvalidJsonArray {
                field: "external_links".into(),
            });
        };

        links
            .into_iter()
            .map(|link| serde_json::from_value(link).context(DeserializeExternalLinkFailedSnafu))
            .collect()
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
    external_link: String,
}

fn map_databricks_type(type_name: &str) -> Result<DataType, Error> {
    if type_name.to_uppercase().starts_with("DECIMAL") {
        let (precision, scale) = if type_name.contains('(') {
            let params: &str = type_name
                .split('(')
                .nth(1)
                .ok_or_else(|| Error::UnsupportedType {
                    ty: type_name.to_string(),
                })?
                .trim_end_matches(')')
                .trim();

            let parts: Vec<&str> = params.split(',').map(str::trim).collect();
            if parts.len() == 2 {
                let precision: u8 = parts[0].parse().map_err(|_| Error::UnsupportedType {
                    ty: type_name.to_string(),
                })?;
                let scale: i8 = parts[1].parse().map_err(|_| Error::UnsupportedType {
                    ty: type_name.to_string(),
                })?;
                (precision, scale)
            } else {
                return Err(Error::UnsupportedType {
                    ty: type_name.to_string(),
                });
            }
        } else {
            (10, 0)
        };

        return Ok(DataType::Decimal128(precision, scale));
    }

    Ok(match type_name.to_uppercase().as_str() {
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

    let fields = data_array
        .iter()
        .enumerate()
        .map(|(i, row)| {
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

            let data_type_str =
                row_array[1]
                    .as_str()
                    .ok_or_else(|| Error::UnableToRetrieveSchema {
                        reason: format!("data_array[{i}][1] is not a string"),
                    })?;

            Ok(Field::new(
                col_name,
                map_databricks_type(data_type_str)?,
                true,
            ))
        })
        .collect::<Result<Vec<Field>, Error>>()?;

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
        let sql = sql.replace('\"', "");
        let token = self.api.token_provider.get_token();
        let payload = json!({
            "warehouse_id": self.api.sql_warehouse_id,
            "format": "ARROW_STREAM",
            "disposition": "EXTERNAL_LINKS",
            "statement": sql,
        });

        let response = self.api.execute_request(&token, &payload).await?;
        Ok(self.api.fetch_arrow_streams(response).await?)
    }

    async fn execute(
        &self,
        _query: &str,
        _: &[&'a dyn Sync],
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        Ok(NotImplementedSnafu.fail()?)
    }
}

#[async_trait]
impl crate::Read for DatabricksSqlWarehouse {
    async fn table_provider(
        &self,
        table_reference: TableReference,
        schema: Option<SchemaRef>,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        let table_provider = match schema {
            Some(schema) => Arc::new(SqlTable::new_with_schema(
                "databricks",
                &self.pool,
                schema,
                table_reference,
                None,
            )),
            None => Arc::new(
                SqlTable::new("databricks", &self.pool, table_reference, None)
                    .await
                    .context(SqlTableInitializationFailedSnafu)?,
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
