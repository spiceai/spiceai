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

use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use arrow_sql_gen::mysql::rows_to_arrow;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::sql::TableReference;
use futures::lock::Mutex;
use mysql_async::prelude::Queryable;
use mysql_async::{prelude::ToValue, Conn, Params, Row};
use snafu::prelude::*;

use super::Result;
use super::{AsyncDbConnection, DbConnection};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{source}"))]
    QueryError { source: mysql_async::Error },

    #[snafu(display("Failed to convert query result to Arrow: {source}"))]
    ConversionError { source: arrow_sql_gen::mysql::Error },
}

pub struct MySQLConnection {
    pub conn: Arc<Mutex<Conn>>,
}

impl<'a> DbConnection<Conn, &'a (dyn ToValue + Sync)> for MySQLConnection {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_async(&self) -> Option<&dyn super::AsyncDbConnection<Conn, &'a (dyn ToValue + Sync)>> {
        Some(self)
    }
}

#[async_trait::async_trait]
impl<'a> AsyncDbConnection<Conn, &'a (dyn ToValue + Sync)> for MySQLConnection {
    fn new(conn: Conn) -> Self {
        MySQLConnection {
            conn: Arc::new(Mutex::new(conn)),
        }
    }

    async fn get_schema(
        &self,
        table_reference: &TableReference,
    ) -> Result<SchemaRef, super::Error> {
        let mut conn = self.conn.lock().await;
        let conn = &mut *conn;
        let rows: Vec<Row> = conn
            .exec(
                &format!(
                    "SELECT * FROM {} LIMIT 1",
                    table_reference.to_quoted_string()
                ),
                Params::Empty,
            )
            .await
            .boxed()
            .context(super::UnableToGetSchemaSnafu)?;

        let rec = rows_to_arrow(&rows)
            .boxed()
            .context(super::UnableToGetSchemaSnafu)?;

        Ok(rec.schema())
    }

    async fn query_arrow(
        &self,
        sql: &str,
        params: &[&'a (dyn ToValue + Sync)],
    ) -> Result<SendableRecordBatchStream> {
        let mut conn = self.conn.lock().await;
        let conn = &mut *conn;

        let params_vec: Vec<_> = params.iter().map(|&p| p.to_value()).collect();
        let rows: Vec<Row> = conn
            .exec(sql.replace('"', ""), Params::from(params_vec))
            .await
            .context(QuerySnafu)?;
        let rec = rows_to_arrow(&rows).context(ConversionSnafu)?;
        let schema = rec.schema();
        let recs = vec![rec];
        Ok(Box::pin(MemoryStream::try_new(recs, schema, None)?))
    }

    async fn execute(&self, query: &str, params: &[&'a (dyn ToValue + Sync)]) -> Result<u64> {
        let mut conn = self.conn.lock().await;
        let conn = &mut *conn;
        let params_vec: Vec<_> = params.iter().map(|&p| p.to_value()).collect();
        let _: Vec<Row> = conn
            .exec(query, Params::from(params_vec))
            .await
            .context(QuerySnafu)?;
        return Ok(conn.affected_rows());
    }
}
