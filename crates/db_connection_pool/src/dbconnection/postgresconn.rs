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

use std::any::Any;
use std::error::Error;

use arrow::datatypes::SchemaRef;
use arrow_sql_gen::postgres::rows_to_arrow;
use bb8_postgres::tokio_postgres::types::ToSql;
use bb8_postgres::PostgresConnectionManager;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::sql::TableReference;
use postgres_native_tls::MakeTlsConnector;
use snafu::prelude::*;

use super::AsyncDbConnection;
use super::DbConnection;
use super::Result;

#[derive(Debug, Snafu)]
pub enum PostgresError {
    #[snafu(display("{source}"))]
    QueryError {
        source: bb8_postgres::tokio_postgres::Error,
    },

    #[snafu(display("Failed to convert query result to Arrow: {source}"))]
    ConversionError {
        source: arrow_sql_gen::postgres::Error,
    },

    #[snafu(display("Table {table_name} not found. Ensure the table name is correctly spelled."))]
    UndefinedTableError {
        source: Box<tokio_postgres::error::DbError>,
        table_name: String,
    },

    #[snafu(display("{source}"))]
    InternalError {
        source: tokio_postgres::error::Error,
    },
}

pub struct PostgresConnection {
    pub conn: bb8::PooledConnection<'static, PostgresConnectionManager<MakeTlsConnector>>,
}

impl<'a>
    DbConnection<
        bb8::PooledConnection<'static, PostgresConnectionManager<MakeTlsConnector>>,
        &'a (dyn ToSql + Sync),
    > for PostgresConnection
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_async(
        &self,
    ) -> Option<
        &dyn AsyncDbConnection<
            bb8::PooledConnection<'static, PostgresConnectionManager<MakeTlsConnector>>,
            &'a (dyn ToSql + Sync),
        >,
    > {
        Some(self)
    }
}

#[async_trait::async_trait]
impl<'a>
    AsyncDbConnection<
        bb8::PooledConnection<'static, PostgresConnectionManager<MakeTlsConnector>>,
        &'a (dyn ToSql + Sync),
    > for PostgresConnection
{
    fn new(
        conn: bb8::PooledConnection<'static, PostgresConnectionManager<MakeTlsConnector>>,
    ) -> Self {
        PostgresConnection { conn }
    }

    async fn get_schema(
        &self,
        table_reference: &TableReference,
    ) -> Result<SchemaRef, super::Error> {
        match self
            .conn
            .query(
                &format!(
                    "SELECT * FROM {} LIMIT 1",
                    table_reference.to_quoted_string()
                ),
                &[],
            )
            .await
        {
            Ok(rows) => {
                let rec = rows_to_arrow(rows.as_slice())
                    .boxed()
                    .context(super::UnableToGetSchemaSnafu)?;

                Ok(rec.schema())
            }
            Err(err) => {
                if let Some(error_source) = err.source() {
                    if let Some(pg_error) =
                        error_source.downcast_ref::<tokio_postgres::error::DbError>()
                    {
                        if pg_error.code() == &tokio_postgres::error::SqlState::UNDEFINED_TABLE {
                            return Err(super::Error::UndefinedTable {
                                source: Box::new(pg_error.clone()),
                                table_name: table_reference.to_string(),
                            });
                        }
                    }
                }

                return Err(super::Error::UnableToGetSchema {
                    source: Box::new(err),
                });
            }
        }
    }

    async fn query_arrow(
        &self,
        sql: &str,
        params: &[&'a (dyn ToSql + Sync)],
    ) -> Result<SendableRecordBatchStream> {
        let rows = self.conn.query(sql, params).await.context(QuerySnafu)?;
        let rec = rows_to_arrow(rows.as_slice()).context(ConversionSnafu)?;
        let schema = rec.schema();
        let recs = vec![rec];
        Ok(Box::pin(MemoryStream::try_new(recs, schema, None)?))
    }

    async fn execute(&self, sql: &str, params: &[&'a (dyn ToSql + Sync)]) -> Result<u64> {
        Ok(self.conn.execute(sql, params).await?)
    }
}
