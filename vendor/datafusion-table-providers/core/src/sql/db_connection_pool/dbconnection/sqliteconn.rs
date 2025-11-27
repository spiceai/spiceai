use std::any::Any;

use crate::sql::arrow_sql_gen::sqlite::rows_to_arrow;
use crate::util::schema::SchemaValidator;
use crate::UnsupportedTypeAction;
use arrow::datatypes::SchemaRef;
use arrow_schema::DataType;
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::sql::TableReference;
use rusqlite::ToSql;
use snafu::prelude::*;
use tokio_rusqlite::Connection;

use super::AsyncDbConnection;
use super::DbConnection;
use super::Result;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("ConnectionError {source}"))]
    ConnectionError { source: tokio_rusqlite::Error<rusqlite::Error> },

    #[snafu(display("Unable to query: {source}"))]
    QueryError { source: rusqlite::Error },

    #[snafu(display("Failed to convert query result to Arrow: {source}"))]
    ConversionError {
        source: crate::sql::arrow_sql_gen::sqlite::Error,
    },
}

pub struct SqliteConnection {
    pub conn: Connection,
}

impl SchemaValidator for SqliteConnection {
    type Error = super::Error;

    fn is_data_type_supported(data_type: &DataType) -> bool {
        match data_type {
            DataType::Dictionary(_, _) | DataType::Interval(_) | DataType::Map(_, _) => false,
            DataType::List(inner_field)
            | DataType::FixedSizeList(inner_field, _)
            | DataType::LargeList(inner_field) => {
                match inner_field.data_type() {
                    dt if dt.is_primitive() => true,
                    DataType::Utf8
                    | DataType::Binary
                    | DataType::Utf8View
                    | DataType::BinaryView
                    | DataType::Boolean => true,
                    _ => false, // nested lists don't support anything else yet
                }
            }
            DataType::Struct(inner_fields) => inner_fields
                .iter()
                .all(|field| Self::is_data_type_supported(field.data_type())),
            _ => true,
        }
    }

    fn unsupported_type_error(data_type: &DataType, field_name: &str) -> Self::Error {
        super::Error::UnsupportedDataType {
            data_type: data_type.to_string(),
            field_name: field_name.to_string(),
        }
    }
}

impl DbConnection<Connection, &'static (dyn ToSql + Sync)> for SqliteConnection {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_async(&self) -> Option<&dyn AsyncDbConnection<Connection, &'static (dyn ToSql + Sync)>> {
        Some(self)
    }
}

#[async_trait]
impl AsyncDbConnection<Connection, &'static (dyn ToSql + Sync)> for SqliteConnection {
    fn new(conn: Connection) -> Self {
        SqliteConnection { conn }
    }

    async fn tables(&self, _schema: &str) -> Result<Vec<String>, super::Error> {
        let tables = self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
                )?;
                let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
                let tables: Result<Vec<_>, rusqlite::Error> = rows.collect();
                tables
            })
            .await
            .boxed()
            .context(super::UnableToGetTablesSnafu)?;

        Ok(tables)
    }

    async fn schemas(&self) -> Result<Vec<String>, super::Error> {
        Ok(vec!["main".to_string()])
    }

    async fn get_schema(
        &self,
        table_reference: &TableReference,
    ) -> Result<SchemaRef, super::Error> {
        let table_reference = table_reference.to_quoted_string();
        let schema: SchemaRef = self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(&format!("SELECT * FROM {table_reference} LIMIT 1"))?;
                let column_count = stmt.column_count();
                let rows = stmt.query([])?;
                let rec = rows_to_arrow(rows, column_count, None)
                    .context(ConversionSnafu)
                    .map_err(to_tokio_rusqlite_error)?;
                let schema = rec.schema();
                Ok::<SchemaRef, rusqlite::Error>(schema)
            })
            .await
            .boxed()
            .context(super::UnableToGetSchemaSnafu)?;

        Self::handle_unsupported_schema(&schema, UnsupportedTypeAction::Error)
    }

    async fn query_arrow(
        &self,
        sql: &str,
        params: &[&'static (dyn ToSql + Sync)],
        projected_schema: Option<SchemaRef>,
    ) -> Result<SendableRecordBatchStream> {
        let sql = sql.to_string();
        let params = params.to_vec();

        let rec = self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(sql.as_str())?;
                for (i, param) in params.iter().enumerate() {
                    stmt.raw_bind_parameter(i + 1, param)?;
                }
                let column_count = stmt.column_count();
                let rows = stmt.raw_query();

                let rec = rows_to_arrow(rows, column_count, projected_schema)
                    .context(ConversionSnafu)
                    .map_err(to_tokio_rusqlite_error)?;
                Ok(rec)
            })
            .await
            .context(ConnectionSnafu)?;

        let schema = rec.schema();
        let recs = if rec.num_rows() > 0 {
            vec![rec]
        } else {
            vec![]
        };
        Ok(Box::pin(MemoryStream::try_new(recs, schema, None)?))
    }

    async fn execute(&self, sql: &str, params: &[&'static (dyn ToSql + Sync)]) -> Result<u64> {
        let sql = sql.to_string();
        let params = params.to_vec();

        let rows_modified = self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(sql.as_str())?;
                for (i, param) in params.iter().enumerate() {
                    stmt.raw_bind_parameter(i + 1, param)?;
                }
                let rows_modified = stmt.raw_execute()?;
                Ok(rows_modified)
            })
            .await
            .context(ConnectionSnafu)?;
        Ok(rows_modified as u64)
    }
}

fn to_tokio_rusqlite_error(e: impl Into<Error>) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(e.into()))
}
