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
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow_sql_gen::clickhouse::block_to_arrow;
use clickhouse_rs::ClientHandle;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::sql::TableReference;
use futures::lock::Mutex;
use snafu::prelude::*;

use super::Result;
use super::{AsyncDbConnection, DbConnection};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{source}"))]
    QueryError {
        source: clickhouse_rs::errors::Error,
    },
    #[snafu(display("Failed to convert query result to Arrow: {source}"))]
    ConversionError {
        source: arrow_sql_gen::clickhouse::Error,
    },
}

pub struct ClickhouseConnection {
    pub conn: Arc<Mutex<ClientHandle>>,
}

impl<'a> DbConnection<ClientHandle, &'a (dyn Sync)> for ClickhouseConnection {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_async(&self) -> Option<&dyn super::AsyncDbConnection<ClientHandle, &'a (dyn Sync)>> {
        Some(self)
    }
}

// Clickhouse doesn't have a params in signature. So just setting to `dyn Sync`.
// Looks like we don't actually pass any params to query_arrow.
// But keep it in mind.
#[async_trait::async_trait]
impl<'a> AsyncDbConnection<ClientHandle, &'a (dyn Sync)> for ClickhouseConnection {
    fn new(conn: ClientHandle) -> Self {
        ClickhouseConnection {
            conn: Arc::new(Mutex::new(conn)),
        }
    }

    async fn get_schema(&self, table_reference: &TableReference) -> Result<SchemaRef> {
        let mut conn = self.conn.lock().await;
        let conn = &mut *conn;
        let block = conn
            .query(&format!(
                "SELECT * FROM {} LIMIT 1",
                table_reference.to_quoted_string()
            ))
            .fetch_all()
            .await
            .context(QuerySnafu)?;
        let rec = block_to_arrow(&block).context(ConversionSnafu)?;
        Ok(rec.schema())
    }

    async fn query_arrow(
        &self,
        sql: &str,
        _: &[&'a (dyn Sync)],
        _schema: SchemaRef,
    ) -> Result<SendableRecordBatchStream> {
        let mut conn = self.conn.lock().await;
        let conn = &mut *conn;
        let block = conn.query(sql).fetch_all().await.context(QuerySnafu)?;
        let rec = block_to_arrow(&block).context(ConversionSnafu)?;
        let schema = rec.schema();
        let recs = vec![rec];

        Ok(Box::pin(MemoryStream::try_new(recs, schema, None)?))
    }

    async fn execute(&self, query: &str, _: &[&'a (dyn Sync)]) -> Result<u64> {
        let mut conn = self.conn.lock().await;
        let conn = &mut *conn;
        conn.execute(query).await.context(QuerySnafu)?;
        // Clickhouse driver doesn't return number of rows affected.
        // Shouldn't be an issue for now since we don't have a data accelerator for now.
        Ok(0)
    }
}
