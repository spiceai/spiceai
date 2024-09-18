/*
Copyright 2024 The Spice.ai OSS Authors

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
use async_trait::async_trait;
use bb8::Pool;
use futures::StreamExt;
use snafu::{OptionExt, ResultExt, Snafu};
use tiberius::{Client, Config};
use tokio::net::TcpStream;

use crate::arrow::write::MemTable;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::{
    catalog::Session,
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
    sql::TableReference,
};
use std::{any::Any, sync::Arc};
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error executing query: {source}"))]
    QueryError { source: tiberius::error::Error },

    #[snafu(display("Unable to parse connection string: {source}"))]
    InvalidConnectionStringError { source: tiberius::error::Error },

    #[snafu(display("Unable to connect: {source}"))]
    SqlServerAccessError { source: tiberius::error::Error },

    #[snafu(display("Failed to retrieve the schema"))]
    SchemaRetrieval,

    #[snafu(display("Unsupported MS SQL data type: {data_type}"))]
    UnsupportedType { data_type: String },
}

pub type SqlServerConnectionPool = Pool<TiberiusConnectionManager>;

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct SqlServerTableProvider {
    #[allow(dead_code)]
    conn: Arc<SqlServerConnectionPool>,
    schema: SchemaRef,
}

impl SqlServerTableProvider {
    pub async fn new(conn: Arc<SqlServerConnectionPool>, table: &TableReference) -> Result<Self> {
        let schema = Self::get_schema(Arc::clone(&conn), table).await?;

        Ok(Self { conn, schema })
    }

    pub async fn get_schema(
        conn: Arc<SqlServerConnectionPool>,
        table: &TableReference,
    ) -> Result<SchemaRef> {
        let table_name = table.to_string();
        let query: String = format!("SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'");

        tracing::debug!("Executing schema query for dataset {table_name}: {query}");

        let mut conn = conn.get().await.unwrap();

        let mut query_res = conn
            .simple_query(query)
            .await
            .context(QuerySnafu)?
            .into_row_stream();

        let mut fields = Vec::new();

        while let Some(row_result) = query_res.next().await {
            let row = row_result.context(QuerySnafu)?;

            let column_name: &str = row.get(0).context(SchemaRetrievalSnafu)?;
            let data_type: &str = row.get(1).context(SchemaRetrievalSnafu)?;
            let numeric_precision: Option<u8> = row.get(2);
            let numeric_scale: Option<i32> = row.get(3);
            let arrow_data_type =
                map_mssql_type_to_arrow(data_type, numeric_precision, numeric_scale)?;

            fields.push(Field::new(column_name, arrow_data_type, true));
        }
        tracing::trace!("Retrieved schema for dataset {table_name}: {fields:?}");

        Ok(Arc::new(Schema::new(fields)))
    }
}

fn map_mssql_type_to_arrow(
    data_type: &str,
    numeric_precision: Option<u8>,
    numeric_scale: Option<i32>,
) -> Result<DataType> {
    // https://learn.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql
    let arrow_type = match data_type.to_lowercase().as_str() {
        "int" => DataType::Int32,
        "bigint" => DataType::Int64,
        "smallint" => DataType::Int16,
        "tinyint" => DataType::Int8,
        "float" => DataType::Float64,
        "real" => DataType::Float32,
        "decimal" | "numeric" => {
            let precision = numeric_precision.unwrap_or(38) as u8;
            let scale = numeric_scale.unwrap_or(0) as i8;
            DataType::Decimal128(precision, scale)
        }
        "char" | "varchar" | "text" | "uniqueidentifier" | "xml" => DataType::Utf8,
        "nchar" | "nvarchar" | "ntext" => DataType::LargeUtf8,
        "binary" => DataType::FixedSizeBinary(numeric_precision.unwrap_or(1) as i32),
        "varbinary" | "image" => DataType::Binary,
        "date" => DataType::Date32,
        "time" => DataType::Time64(TimeUnit::Microsecond),
        "datetime" | "smalldatetime" | "datetime2" => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        // TODO: correct support for time zones
        "datetimeoffset" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "bit" => DataType::Boolean,
        other => {
            return Err(Error::UnsupportedType {
                data_type: other.to_string(),
            })
        }
    };

    Ok(arrow_type)
}

#[async_trait]
impl TableProvider for SqlServerTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> std::result::Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let table = MemTable::try_new(Arc::clone(&self.schema), vec![])?;
        table.scan(state, projection, filters, limit).await
    }
}

#[derive(Clone, Debug)]
pub struct TiberiusConnectionManager {
    config: Config,
}

impl TiberiusConnectionManager {
    fn new(config: Config) -> tiberius::Result<TiberiusConnectionManager> {
        Ok(TiberiusConnectionManager { config })
    }

    pub async fn create_connection_pool(
        connection_string: &str,
    ) -> Result<SqlServerConnectionPool> {
        let config =
            Config::from_ado_string(connection_string).context(InvalidConnectionStringSnafu)?;
        let manager = TiberiusConnectionManager::new(config).unwrap();
        let pool = bb8::Pool::builder()
            .build(manager)
            .await
            .context(SqlServerAccessSnafu)?;
        Ok(pool)
    }
}

#[async_trait]
impl bb8::ManageConnection for TiberiusConnectionManager {
    type Connection = Client<Compat<TcpStream>>;
    type Error = tiberius::error::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let tcp = TcpStream::connect(&self.config.get_addr()).await?;
        tcp.set_nodelay(true)?;
        Client::connect(self.config.clone(), tcp.compat_write()).await
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.simple_query("SELECT 1").await?.into_row().await?;
        Ok(())
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}
