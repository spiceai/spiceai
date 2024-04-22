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
use std::borrow::BorrowMut;
use std::sync::Arc;
use std::sync::Mutex;

use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow_odbc::OdbcReaderBuilder;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::sql::TableReference;
use rusqlite::ToSql;
use snafu::whatever;

use super::AsyncDbConnection;
use super::DbConnection;
use super::Result;
use odbc_api::{buffers::Item, sys::AttrConnectionPooling, Connection, ConnectionOptions, Environment, IntoParameter};
use itertools::Itertools;

pub type ODBCParameter = (dyn IntoParameter<Parameter = dyn Any> + Sync);

pub struct ODBCConnection<'a> {
  pub conn: Arc<Mutex<Connection<'a>>>,
}

impl<'a> DbConnection<Connection<'a>, &'a ODBCParameter> for ODBCConnection<'a> where 'a: 'static {
  fn as_any(&self) -> &dyn Any {
      self
  }

  fn as_any_mut(&mut self) -> &mut dyn Any {
      self
  }

  fn as_async(&self) -> Option<&dyn super::AsyncDbConnection<Connection<'a>, &'a ODBCParameter>> {
      Some(self)
  }
}

#[async_trait::async_trait]
impl<'a> AsyncDbConnection<Connection<'a>, &'a ODBCParameter> for ODBCConnection<'a> where 'a: 'static {
    fn new(conn: Connection<'a>) -> Self where Self:Sized {
        ODBCConnection { conn: Arc::new(conn.into()) }
    }

    #[must_use]
    #[allow(clippy::type_complexity,clippy::type_repetition_in_bounds)]
    async fn get_schema(&self, table_reference: &TableReference) -> Result<SchemaRef>{
      let cxn = self.conn.lock().expect("Must get mutex");
  
      let cursor = cxn.execute(
        &format!(
          "SELECT * FROM {} LIMIT 1",
          table_reference.to_quoted_string()
        ),
        ()
      )?;

      let schema = cursor
        .and_then(|c| {
          let mut arb = OdbcReaderBuilder::new().build(c).expect("Must reader");
          arb.next().and_then(|b| b.ok()).map(|b| b.schema())
        })
        .expect("Must read schema");

      Ok(schema)
    }

    async fn query_arrow(
      &self,
      sql: &str,
      params: &[&'a ODBCParameter],
    ) -> Result<SendableRecordBatchStream> {
      let cxn = self.conn.lock().expect("Must get mutex");
      let cursor = cxn.execute(sql, ())?;
      let results = cursor
        .and_then(|c| OdbcReaderBuilder::new().build(c).ok())
        .map(|or| or.collect::<Vec<Result<arrow::array::RecordBatch, arrow::error::ArrowError>>>())
        .expect("results");

      let schema: Arc<Schema> = results.first().unwrap().as_ref().unwrap().schema();
      Ok(Box::pin(MemoryStream::try_new(results.iter().map(|r| r.as_ref().unwrap().clone()).collect_vec(), schema, None)?))
  }

  async fn execute(&self, query: &str, params: &[&'a (dyn IntoParameter<Parameter = dyn Any> + Sync)]) -> Result<u64> {
    let cxn = self.conn.lock().expect("Must get mutex");
    let mut prepared = cxn.prepare(query)?;
    prepared.execute(())?;
    Ok(prepared.row_count()?.unwrap().try_into().unwrap())
  }
}