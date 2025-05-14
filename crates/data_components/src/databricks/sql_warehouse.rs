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

use crate::Read;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::ipc::reader::StreamReader;
use async_trait::async_trait;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::TableReference;
use datafusion::{datasource::TableProvider, execution::SendableRecordBatchStream};
use datafusion_table_providers::sql::{
    db_connection_pool::{
        DbConnectionPool, JoinPushDown,
        dbconnection::{self, AsyncDbConnection, DbConnection},
    },
    sql_provider_datafusion::SqlTable,
};
use futures::{StreamExt as _, stream};
use reqwest::{Client, ClientBuilder};
use serde::Deserialize;
use serde_json::{Value, json};
use snafu::prelude::*;
use std::io::Cursor;
use std::{any::Any, sync::Arc};
use token_provider::TokenProvider;

pub struct DatabricksSqlWarehouse {
    pool: Arc<dyn DbConnectionPool<Arc<SqlWarehouseApi>, &'static (dyn Sync)> + Send + Sync>,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Databricks datatype {ty} not supported"))]
    UnsupportedType { ty: String },
    #[snafu(display("Failed to get schema from Databricks: {issue}"))]
    GetSchema { issue: String },
}

impl DatabricksSqlWarehouse {
    pub fn new(
        endpoint: &str,
        sql_warehouse_id: &str,
        token_provider: Arc<dyn TokenProvider>,
    ) -> Self {
        let api = Arc::new(SqlWarehouseApi::new(
            endpoint,
            sql_warehouse_id,
            token_provider,
        ));
        let pool = Arc::new(SqlWarehouseConnectionPool { api });
        Self { pool }
    }
}

struct SqlWarehouseConnectionPool {
    api: Arc<SqlWarehouseApi>,
}

#[async_trait]
impl DbConnectionPool<Arc<SqlWarehouseApi>, &'static (dyn Sync)> for SqlWarehouseConnectionPool {
    async fn connect(
        &self,
    ) -> Result<
        Box<dyn DbConnection<Arc<SqlWarehouseApi>, &'static (dyn Sync)>>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let api = Arc::clone(&self.api);
        Ok(Box::new(SqlWarehouseConnection { api }))
    }

    fn join_push_down(&self) -> JoinPushDown {
        // TODO: allow?
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
    fn new(host: &str, sql_warehouse_id: &str, token_provider: Arc<dyn TokenProvider>) -> Self {
        let client = ClientBuilder::new()
            .user_agent(super::user_agent())
            .build()
            .unwrap();

        let url = format!("https://{host}/api/2.0/sql/statements/");

        Self {
            client,
            url,
            sql_warehouse_id: sql_warehouse_id.to_string(),
            token_provider,
        }
    }

    pub async fn get_schema(
        &self,
        table: &TableReference,
    ) -> Result<SchemaRef, Box<dyn std::error::Error + Send + Sync>> {
        let token = self.token_provider.get_token();
        let sql = format!("DESCRIBE TABLE {table}");

        let payload = json!({
            "warehouse_id": self.sql_warehouse_id,
            "catalog": table.catalog().unwrap_or("spiceai"),
            "schema": table.schema().unwrap_or("public"),
            "statement": sql,
        });

        let response = self
            .client
            .post(&self.url)
            .bearer_auth(token)
            .json(&payload)
            .send()
            .await?
            .json()
            .await?;

        Ok(schema_from_json(response)?)
    }

    async fn fetch_arrow_streams(
        &self,
        response: Value,
    ) -> Result<SendableRecordBatchStream, Box<dyn std::error::Error + Send + Sync>> {
        let external_links = response
            .get("result")
            .and_then(|result| result.get("external_links"))
            .ok_or("Missing result.external_links in JSON response")?
            .as_array()
            .ok_or("external_links is not an array")?;

        let external_links: Vec<ExternalLink> = external_links
            .iter()
            .map(|link| serde_json::from_value(link.clone()))
            .collect::<Result<Vec<ExternalLink>, _>>()?;

        let mut streams = vec![];
        for link in external_links {
            tracing::trace!(
                "Fetching chunk {} from {}",
                link.chunk_index,
                link.external_link
            );
            let response = self.client.get(&link.external_link).send().await?;

            if !response.status().is_success() {
                return Err(format!(
                    "Failed to fetch {}: {}",
                    link.external_link,
                    response.status()
                )
                .into());
            }

            let bytes = response.bytes().await?;
            let cursor = Cursor::new(bytes);

            let reader = StreamReader::try_new(cursor, None)?;
            let batches: Vec<_> = reader
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .filter(|batch| batch.num_rows() > 0)
                .collect();

            let stream = futures::stream::iter(batches.into_iter().map(Ok));
            streams.push(stream);
        }

        let mut combined_stream = futures::stream::select_all(streams);

        let Some(first_batch) = combined_stream.next().await else {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::new(Schema::empty()),
                stream::empty(),
            )));
        };

        let batch = first_batch?;

        let schema = batch.schema();

        // add first batch back to stream
        let run_once = stream::once(async move { Ok(batch) });
        let stream_adapter =
            RecordBatchStreamAdapter::new(schema, Box::pin(run_once.chain(combined_stream)));

        Ok(Box::pin(stream_adapter))
    }
}

#[derive(Debug, Deserialize)]
struct ExternalLink {
    chunk_index: u64,
    external_link: String,
}

fn schema_from_json(json_value: Value) -> Result<SchemaRef, Error> {
    let data_array = json_value
        .get("result")
        .and_then(|r| r.get("data_array"))
        .and_then(|d| d.as_array())
        .ok_or_else(|| Error::GetSchema {
            issue: "result.data_array".to_string(),
        })?;

    let fields: Result<Vec<Field>, Error> = data_array
        .iter()
        .enumerate()
        .map(|(i, row)| {
            let row_array = row.as_array().ok_or_else(|| Error::GetSchema {
                issue: format!("data_array[{}] is not an array", i),
            })?;

            if row_array.len() < 2 {
                return Err(Error::GetSchema {
                    issue: format!("data_array[{}] lacks col_name or data_type", i),
                });
            }

            let col_name = row_array[0].as_str().ok_or_else(|| Error::GetSchema {
                issue: format!("data_array[{}][0] is not a string", i),
            })?;

            let data_type_str = row_array[1].as_str().ok_or_else(|| Error::GetSchema {
                issue: format!("data_array[{}][1] is not a string", i),
            })?;

            let data_type = map_databricks_type(data_type_str)?;
            Ok(Field::new(col_name, data_type, true))
        })
        .collect();

    let fields = fields?;

    let schema = Schema::new(fields);
    Ok(Arc::new(schema))
}

fn map_databricks_type(type_name: &str) -> Result<DataType, Error> {
    Ok(match type_name.to_uppercase().as_str() {
        "BOOLEAN" => DataType::Boolean,
        "TINYINT" => DataType::Int8,
        "SMALLINT" => DataType::Int16,
        "INT" => DataType::Int32,
        "BIGINT" => DataType::Int64,
        "FLOAT" => DataType::Float32,
        "DOUBLE" => DataType::Float64,
        "STRING" => DataType::Utf8,
        "CHAR" => DataType::Utf8,
        "VARCHAR" => DataType::Utf8,
        "BINARY" => DataType::Binary,
        "DATE" => DataType::Date32,
        "TIMESTAMP" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "TIMESTAMP_NTZ" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "VOID" => DataType::Null,
        ty => return Err(Error::UnsupportedType { ty: ty.to_string() }),
    })
}

struct SqlWarehouseConnection {
    api: Arc<SqlWarehouseApi>,
}

impl<'a> DbConnection<Arc<SqlWarehouseApi>, &'a (dyn Sync)> for SqlWarehouseConnection {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_async(&self) -> Option<&dyn AsyncDbConnection<Arc<SqlWarehouseApi>, &'a (dyn Sync)>> {
        Some(self)
    }
}

#[async_trait]
impl<'a> AsyncDbConnection<Arc<SqlWarehouseApi>, &'a (dyn Sync)> for SqlWarehouseConnection {
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
            .map_err(|source| dbconnection::Error::UnableToGetSchema { source })
    }

    async fn query_arrow(
        &self,
        sql: &str,
        _: &[&'a (dyn Sync)],
        _projected_schema: Option<SchemaRef>,
    ) -> Result<SendableRecordBatchStream, Box<dyn std::error::Error + Send + Sync>> {
        // databricks does not like escaping default schema
        let sql = sql.replace("\"default\"", "default");
        let token = self.api.token_provider.get_token();
        let payload = json!({
            "warehouse_id": self.api.sql_warehouse_id,
            // "catalog": table.catalog().unwrap_or("spiceai"),
            // "schema": table.schema().unwrap_or("public"),
            "format": "ARROW_STREAM",
            "disposition": "EXTERNAL_LINKS",
            "statement": sql,
        });

        let response = self
            .api
            .client
            .post(&self.api.url)
            .bearer_auth(token)
            .json(&payload)
            .send()
            .await?
            .json::<Value>()
            .await?;

        self.api.fetch_arrow_streams(response).await
    }

    async fn execute(
        &self,
        _query: &str,
        _: &[&'a (dyn Sync)],
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        todo!()
    }
}

#[async_trait]
impl Read for DatabricksSqlWarehouse {
    async fn table_provider(
        &self,
        table_reference: TableReference,
        schema: Option<SchemaRef>,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let pool = Arc::clone(&self.pool);
        let table_provider = match schema {
            Some(schema) => Arc::new(SqlTable::new_with_schema(
                "databricks",
                &pool,
                schema,
                table_reference,
                None,
            )),
            None => Arc::new(SqlTable::new("databricks", &pool, table_reference, None).await?),
        };

        let table_provider = Arc::new(
            table_provider
                .create_federated_table_provider()
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        );

        Ok(table_provider)
    }
}
