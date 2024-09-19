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
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use datafusion_table_providers::sql::arrow_sql_gen::arrow::map_data_type_to_array_builder_optional;
use futures::StreamExt;
use snafu::{OptionExt, ResultExt, Snafu};
use tiberius::{numeric::Numeric, Client, ColumnType, Config, Row};
use tokio::net::TcpStream;
use uuid::Uuid;

use crate::arrow::write::MemTable;

use arrow::{
    array::{
        ArrayBuilder, ArrayRef, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
        Float64Builder, Int16Builder, Int32Builder, Int64Builder, NullBuilder, RecordBatch,
        RecordBatchOptions, StringBuilder, Time64NanosecondBuilder, TimestampMillisecondBuilder,
        TimestampNanosecondBuilder, UInt8Builder,
    },
    datatypes::{DataType, Date32Type, Field, Schema, SchemaRef, TimeUnit},
};
use datafusion::{
    catalog::Session,
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
    sql::TableReference,
};

use chrono::Timelike;

use std::{any::Any, sync::Arc};
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error executing query: {source}"))]
    QueryError { source: tiberius::error::Error },

    #[snafu(display("Invalid connection string: {source}"))]
    InvalidConnectionStringError { source: tiberius::error::Error },

    #[snafu(display("Unable to connect: {source}"))]
    SqlServerAccessError { source: tiberius::error::Error },

    #[snafu(display("Unable to connect: {source}"))]
    ConnectionPoolError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to retrieve table schema"))]
    SchemaRetrieval,

    #[snafu(display("Unable to retrieve table schema: dataset not found"))]
    SchemaRetrievalTableNotFound,

    #[snafu(display("Unsupported data type: {data_type}"))]
    UnsupportedType { data_type: String },

    #[snafu(display("Failed to build record batch: {source}"))]
    FailedToBuildRecordBatch { source: arrow::error::ArrowError },

    #[snafu(display("No builder found for index {index}"))]
    NoBuilderForIndex { index: usize },

    #[snafu(display("Failed to downcast builder for {mssql_type}"))]
    FailedToDowncastBuilder { mssql_type: String },
}

pub type SqlServerConnectionPool = Pool<SqlServerConnectionManager>;

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct SqlServerTableProvider {
    #[allow(dead_code)]
    conn: Arc<SqlServerConnectionPool>,
    schema: SchemaRef,
    table: TableReference,
}

impl SqlServerTableProvider {
    pub async fn new(conn: Arc<SqlServerConnectionPool>, table: &TableReference) -> Result<Self> {
        let schema = Self::get_schema(Arc::clone(&conn), table).await?;

        Ok(Self {
            conn,
            schema,
            table: table.clone(),
        })
    }

    pub async fn get_schema(
        conn: Arc<SqlServerConnectionPool>,
        table: &TableReference,
    ) -> Result<SchemaRef> {
        let table_name = table.table();
        let table_schema = table.schema().unwrap_or("dbo");

        let columns_meta_query: String = format!(
            "SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS \
            WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{table_schema}'"
        );
        tracing::debug!("Executing schema query for dataset {table_name}: {columns_meta_query}");

        let mut conn = conn.get().await.boxed().context(ConnectionPoolSnafu)?;

        let mut query_res = conn
            .simple_query(columns_meta_query)
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

            let column_type = map_type_name_to_column_type(data_type)?;
            let arrow_data_type = map_column_type_to_arrow_type(
                column_type,
                numeric_precision,
                numeric_scale.map(|v| i8::try_from(v).unwrap_or_default()),
            );

            fields.push(Field::new(column_name, arrow_data_type, true));
        }

        if fields.is_empty() {
            return Err(Error::SchemaRetrievalTableNotFound {});
        }

        tracing::trace!("Retrieved dataset {table_name} schema: {fields:?}");

        Ok(Arc::new(Schema::new(fields)))
    }

    async fn query_arrow(
        &self,
        sql: &str,
        projected_schema: SchemaRef,
    ) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error + Send + Sync>> {
        tracing::debug!("Executing sql: {sql}");

        let schema = Arc::clone(&projected_schema);

        let mut conn = self.conn.get().await.boxed().context(ConnectionPoolSnafu)?;

        let query_res = conn
            .simple_query(sql)
            .await
            .boxed()
            .context(ConnectionPoolSnafu)?
            .into_row_stream();

        let mut chunked_stream = query_res.chunks(4_000).boxed();

        let mut records: Vec<RecordBatch> = Vec::new();

        while let Some(chunk) = chunked_stream.next().await {
            let rows = chunk
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .context(QuerySnafu)
                .map_err(to_datafusion_err)?;

            records.push(rows_to_arrow(&rows, &schema)?);
        }

        Ok(records)
    }
}

macro_rules! handle_primitive_type {
    ($builder:expr, $type:expr, $builder_ty:ty, $value_ty:ty, $row:expr, $index:expr) => {{
        let Some(builder) = $builder else {
            return NoBuilderForIndexSnafu { index: $index }.fail();
        };
        let Some(builder) = builder.as_any_mut().downcast_mut::<$builder_ty>() else {
            return FailedToDowncastBuilderSnafu {
                mssql_type: format!("{:?}", $type),
            }
            .fail();
        };
        let v = $row.get::<$value_ty, usize>($index);
        match v {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }};
}

#[allow(clippy::too_many_lines)]
fn rows_to_arrow(rows: &[Row], schema: &SchemaRef) -> Result<RecordBatch> {
    let mut arrow_columns_builders: Vec<Option<Box<dyn ArrayBuilder>>> = Vec::new();
    let mut mssql_types: Vec<ColumnType> = Vec::new();
    let mut column_names: Vec<String> = Vec::new();

    if !rows.is_empty() {
        let row = &rows[0];
        for column in row.columns() {
            let column_name = column.name();
            let column_type = column.column_type();
            let (decimal_precision, decimal_scale) = match column_type {
                ColumnType::Decimaln | ColumnType::Numericn => {
                    // use 38, 10 as default precision and scale for decimal types
                    let (precision, scale) =
                        get_column_precision_and_scale(column_name, schema).unwrap_or((38, 10));
                    (Some(precision), Some(scale))
                }
                _ => (None, None),
            };
            let data_type =
                map_column_type_to_arrow_type(column_type, decimal_precision, decimal_scale);

            // arrow_fields.push(Field::new(column_name.clone(), data_type.clone(), true));
            arrow_columns_builders.push(map_data_type_to_array_builder_optional(Some(&data_type)));
            mssql_types.push(column_type);
            column_names.push(column_name.to_string());
        }
    }

    for row in rows {
        for (i, mssql_type) in mssql_types.iter().enumerate() {
            let Some(builder) = arrow_columns_builders.get_mut(i) else {
                return NoBuilderForIndexSnafu { index: i }.fail();
            };
            match *mssql_type {
                ColumnType::Null => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder.as_any_mut().downcast_mut::<NullBuilder>() else {
                        return FailedToDowncastBuilderSnafu {
                            mssql_type: format!("{mssql_type:?}"),
                        }
                        .fail();
                    };
                    builder.append_null();
                }
                ColumnType::Int4 => {
                    handle_primitive_type!(builder, ColumnType::Int4, Int32Builder, i32, row, i);
                }
                ColumnType::Int1 => {
                    handle_primitive_type!(builder, ColumnType::Int1, UInt8Builder, u8, row, i);
                }
                ColumnType::Int2 => {
                    handle_primitive_type!(builder, ColumnType::Int2, Int16Builder, i16, row, i);
                }
                ColumnType::Int8 => {
                    handle_primitive_type!(builder, ColumnType::Int8, Int64Builder, i64, row, i);
                }
                ColumnType::Float4 => {
                    handle_primitive_type!(
                        builder,
                        ColumnType::Float4,
                        Float32Builder,
                        f32,
                        row,
                        i
                    );
                }
                ColumnType::Float8 | ColumnType::Money | ColumnType::Money4 => {
                    handle_primitive_type!(builder, *mssql_type, Float64Builder, f64, row, i);
                }
                ColumnType::Bit | ColumnType::Bitn => {
                    handle_primitive_type!(builder, *mssql_type, BooleanBuilder, bool, row, i);
                }
                ColumnType::Datetime | ColumnType::Datetimen | ColumnType::Datetime4 => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<TimestampMillisecondBuilder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            mssql_type: format!("{mssql_type:?}"),
                        }
                        .fail();
                    };
                    let v = row.get::<NaiveDateTime, usize>(i);
                    match v {
                        Some(v) => builder.append_value(v.and_utc().timestamp_millis()),
                        None => builder.append_null(),
                    }
                }
                ColumnType::Datetime2 => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<TimestampNanosecondBuilder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            mssql_type: format!("{mssql_type:?}"),
                        }
                        .fail();
                    };
                    let v = row.get::<NaiveDateTime, usize>(i);
                    match v {
                        Some(v) => builder
                            .append_value(v.and_utc().timestamp_nanos_opt().unwrap_or_default()),
                        None => builder.append_null(),
                    }
                }
                ColumnType::Daten => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder.as_any_mut().downcast_mut::<Date32Builder>() else {
                        return FailedToDowncastBuilderSnafu {
                            mssql_type: format!("{mssql_type:?}"),
                        }
                        .fail();
                    };
                    let v = row.get::<NaiveDate, usize>(i);
                    match v {
                        Some(v) => builder.append_value(Date32Type::from_naive_date(v)),
                        None => builder.append_null(),
                    }
                }
                ColumnType::Timen => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<Time64NanosecondBuilder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            mssql_type: format!("{mssql_type:?}"),
                        }
                        .fail();
                    };
                    let v = row.get::<NaiveTime, usize>(i);
                    match v {
                        Some(v) => {
                            let timestamp: i64 = i64::from(v.num_seconds_from_midnight())
                                * 1_000_000_000
                                + i64::from(v.nanosecond());
                            builder.append_value(timestamp);
                        }
                        None => builder.append_null(),
                    }
                }
                ColumnType::Decimaln | ColumnType::Numericn => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder.as_any_mut().downcast_mut::<Decimal128Builder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            mssql_type: format!("{mssql_type:?}"),
                        }
                        .fail();
                    };
                    let v = row.get::<Numeric, usize>(i);
                    match v {
                        Some(v) => builder.append_value(v.value()),
                        None => builder.append_null(),
                    }
                }
                ColumnType::Guid => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder.as_any_mut().downcast_mut::<StringBuilder>() else {
                        return FailedToDowncastBuilderSnafu {
                            mssql_type: format!("{mssql_type:?}"),
                        }
                        .fail();
                    };
                    let v = row.get::<Uuid, usize>(i);
                    match v {
                        Some(v) => builder.append_value(v.to_string()),
                        None => builder.append_null(),
                    }
                }
                ColumnType::BigVarChar
                | ColumnType::BigChar
                | ColumnType::NVarchar
                | ColumnType::NChar
                | ColumnType::Xml
                | ColumnType::Udt
                | ColumnType::Text
                | ColumnType::NText
                | ColumnType::SSVariant => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder.as_any_mut().downcast_mut::<StringBuilder>() else {
                        return FailedToDowncastBuilderSnafu {
                            mssql_type: format!("{mssql_type:?}"),
                        }
                        .fail();
                    };
                    let v = row.get::<&str, usize>(i);
                    match v {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    }
                }
                _ => {
                    return UnsupportedTypeSnafu {
                        data_type: format!("{mssql_type:?}"),
                    }
                    .fail();
                }
            }
        }
    }

    let columns = arrow_columns_builders
        .into_iter()
        .filter_map(|builder| builder.map(|mut b| b.finish()))
        .collect::<Vec<ArrayRef>>();

    let options = &RecordBatchOptions::new().with_row_count(Some(rows.len()));
    RecordBatch::try_new_with_options(Arc::clone(schema), columns, options)
        .map_err(|err| Error::FailedToBuildRecordBatch { source: err })
}

fn map_column_type_to_arrow_type(
    column_type: ColumnType,
    decimal_precision: Option<u8>,
    decimal_scale: Option<i8>,
) -> DataType {
    match column_type {
        ColumnType::Null => DataType::Null,
        // https://learn.microsoft.com/en-us/sql/t-sql/data-types/int-bigint-smallint-and-tinyint-transact-sql
        ColumnType::Int1 => DataType::UInt8,
        ColumnType::Int2 => DataType::Int16,
        ColumnType::Int4 => DataType::Int32,
        ColumnType::Int8 | ColumnType::Intn => DataType::Int64,
        ColumnType::Float4 => DataType::Float32,
        ColumnType::Float8 | ColumnType::Floatn | ColumnType::Money | ColumnType::Money4 => {
            DataType::Float64
        }
        ColumnType::Datetime4 | ColumnType::Datetime | ColumnType::Datetimen => {
            DataType::Timestamp(TimeUnit::Millisecond, None)
        }
        ColumnType::Datetime2 | ColumnType::DatetimeOffsetn => {
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        }
        ColumnType::Decimaln | ColumnType::Numericn => {
            let precision = decimal_precision.unwrap_or(38);
            let scale = decimal_scale.unwrap_or(10);
            DataType::Decimal128(precision, scale)
        }
        ColumnType::Daten => DataType::Date32,
        ColumnType::Timen => DataType::Time64(TimeUnit::Nanosecond),
        ColumnType::Image | ColumnType::BigVarBin | ColumnType::BigBinary => DataType::Binary,
        ColumnType::Guid
        | ColumnType::BigVarChar
        | ColumnType::BigChar
        | ColumnType::NVarchar
        | ColumnType::NChar
        | ColumnType::Xml
        | ColumnType::Udt
        | ColumnType::Text
        | ColumnType::NText
        | ColumnType::SSVariant => DataType::Utf8,
        ColumnType::Bit | ColumnType::Bitn => DataType::Boolean,
    }
}

fn map_type_name_to_column_type(data_type: &str) -> Result<ColumnType> {
    // https://github.com/prisma/tiberius/blob/51f0cbb3e430db74ba0ea4830b236e89f1b1e03f/src/tds/codec/token/token_col_metadata.rs#L26
    let column_type = match data_type.to_lowercase().as_str() {
        "int" => ColumnType::Int4,
        "bigint" => ColumnType::Int8,
        "smallint" => ColumnType::Int2,
        "tinyint" => ColumnType::Int1,
        "float" => ColumnType::Float8,
        "real" => ColumnType::Float4,
        "decimal" | "numeric" => ColumnType::Decimaln,
        "char" | "varchar" | "text" => ColumnType::BigVarChar,
        "nchar" | "nvarchar" | "ntext" => ColumnType::NVarchar,
        "uniqueidentifier" => ColumnType::Guid,
        "binary" => ColumnType::BigBinary,
        "varbinary" | "image" => ColumnType::BigVarBin,
        "xml" => ColumnType::Xml,
        "money" => ColumnType::Money,
        "date" => ColumnType::Daten,
        "time" => ColumnType::Timen,
        "datetime" => ColumnType::Datetime,
        "smalldatetime" => ColumnType::Datetime4,
        "datetime2" => ColumnType::Datetime2,
        "datetimeoffset" => ColumnType::DatetimeOffsetn,
        "bit" => ColumnType::Bit,
        other => {
            return Err(Error::UnsupportedType {
                data_type: other.to_string(),
            })
        }
    };

    Ok(column_type)
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
        let sql = format!("SELECT * from {dataset}", dataset = self.table);
        let records = self.query_arrow(&sql, Arc::clone(&self.schema)).await?;

        let table = MemTable::try_new(Arc::clone(&self.schema), vec![records])?;
        table.scan(state, projection, filters, limit).await
    }
}

fn to_datafusion_err(e: Error) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::External(Box::new(e))
}

fn get_column_precision_and_scale(
    column_name: &str,
    projected_schema: &SchemaRef,
) -> Option<(u8, i8)> {
    let field = projected_schema.field_with_name(column_name).ok()?;
    match field.data_type() {
        DataType::Decimal128(precision, scale) => Some((*precision, *scale)),
        _ => None,
    }
}

#[derive(Clone, Debug)]
pub struct SqlServerConnectionManager {
    config: Config,
}

impl SqlServerConnectionManager {
    fn new(config: Config) -> SqlServerConnectionManager {
        Self { config }
    }

    pub async fn create_connection_pool(
        connection_string: &str,
    ) -> Result<SqlServerConnectionPool> {
        let config =
            Config::from_ado_string(connection_string).context(InvalidConnectionStringSnafu)?;
        let manager = SqlServerConnectionManager::new(config);
        let pool = bb8::Pool::builder()
            .build(manager)
            .await
            .context(SqlServerAccessSnafu)?;
        Ok(pool)
    }
}

#[async_trait]
impl bb8::ManageConnection for SqlServerConnectionManager {
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
