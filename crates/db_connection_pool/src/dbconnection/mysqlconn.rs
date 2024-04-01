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

use std::{any::Any, ops::DerefMut, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::sql::TableReference;
use futures::lock::Mutex;
use mysql_async::{prelude::Queryable, Conn, Params};
use mysql_common::value::convert::ToValue;
use snafu::prelude::*;

use super::{AsyncDbConnection, DbConnection, Result};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{source}"))]
    QueryError { source: mysql_async::Error },
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

    async fn get_schema(&self, table_reference: &TableReference) -> Result<SchemaRef> {
        // let conn = self.conn.lock().await.deref_mut();
        // let rec: Vec<Row> = conn
        //     .query(&format!("SELECT * FROM {table_reference} LIMIT 1"))
        //     .await?;
        unimplemented!()
    }

    async fn query_arrow(
        &self,
        sql: &str,
        params: &[&'a (dyn ToValue + Sync)],
    ) -> Result<SendableRecordBatchStream> {
        unimplemented!()
    }

    async fn execute(&self, query: &str, params: &[&'a (dyn ToValue + Sync)]) -> Result<u64> {
        let mut conn = self.conn.lock().await;
        let conn = conn.deref_mut();
        let params_vec: Vec<_> = params.iter().map(|&p| p.to_value()).collect();
        let _: Vec<mysql_async::Row> = conn
            .exec(query, Params::from(params_vec))
            .await
            .context(QuerySnafu)?;
        return Ok(conn.affected_rows());
    }
}
