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
use std::ops::Deref;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;

use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow_odbc::arrow_schema_from;
use arrow_odbc::OdbcReaderBuilder;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::sql::TableReference;
use odbc_api::handles::HasDataType;
use odbc_api::handles::Statement;
use odbc_api::handles::StatementImpl;
use odbc_api::parameter::CElement;
use odbc_api::parameter::InputParameter;
use odbc_api::CursorImpl;
use odbc_api::ParameterCollection;
use snafu::prelude::*;
use snafu::Snafu;

use crate::DbConnectionPool;

use super::AsyncDbConnection;
use super::DbConnection;
use super::Result;
use itertools::Itertools;
use odbc_api::{Connection, IntoParameter};

pub type ODBCParameter = dyn InputParameter + Sync;
pub type ODBCDbConnection<'a> = (dyn DbConnection<Connection<'a>, &'a ODBCParameter>);
pub type ODBCDbConnectionPool<'a> =
    dyn DbConnectionPool<Connection<'a>, &'a ODBCParameter> + Sync + Send;

#[derive(Debug, Snafu)]
pub enum Error {
    ArrowODBCError { source: arrow_odbc::Error },
    ODBCAPIError { source: odbc_api::Error },
}

pub struct ODBCConnection<'a> {
    pub conn: Arc<Mutex<Connection<'a>>>,
}

impl<'a> DbConnection<Connection<'a>, &'a ODBCParameter> for ODBCConnection<'a>
where
    'a: 'static,
{
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
impl<'a> AsyncDbConnection<Connection<'a>, &'a ODBCParameter> for ODBCConnection<'a>
where
    'a: 'static,
{
    fn new(conn: Connection<'a>) -> Self {
        ODBCConnection {
            conn: Arc::new(conn.into()),
        }
    }

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    async fn get_schema(&self, table_reference: &TableReference) -> Result<SchemaRef> {
        let cxn = self.conn.lock().expect("Must lock");

        let cursor = cxn
            .execute(
                &format!(
                    "SELECT * FROM {} LIMIT 1",
                    table_reference.to_quoted_string()
                ),
                (),
            )
            .context(ODBCAPISnafu)?
            .expect("Must produce cursor for schema reflection");

        let mut reader = OdbcReaderBuilder::new()
            .build(cursor)
            .context(ArrowODBCSnafu)?;

        let record_batch = reader
            .next()
            .expect("Must produce a result batch for schema reflection")?;

        Ok(record_batch.schema())
    }

    async fn query_arrow(
        &self,
        sql: &str,
        params: &[&'a ODBCParameter],
    ) -> Result<SendableRecordBatchStream> {
        let cxn = self.conn.lock().expect("Must lock");
        let mut prepared = cxn.prepare(sql)?;
        let schema = Arc::new(arrow_schema_from(&mut prepared)?);
        let mut statement = prepared.into_statement();

        bind_parameters(&mut statement, params);

        let cursor = unsafe {
            statement.execute().unwrap();
            CursorImpl::new(statement.as_stmt_ref())
        };

        let reader = OdbcReaderBuilder::new()
            .with_schema(schema.clone())
            .build(cursor)
            .context(ArrowODBCSnafu)?;

        let results = reader.map(|b| b.unwrap()).collect_vec();

        Ok(Box::pin(MemoryStream::try_new(results, schema, None)?))
    }

    async fn execute(&self, query: &str, params: &[&'a ODBCParameter]) -> Result<u64> {
        let cxn = self.conn.lock().expect("Must lock");
        let prepared = cxn.prepare(query)?;
        let mut statement = prepared.into_statement();

        bind_parameters(&mut statement, params);

        let row_count = unsafe {
            statement.execute().unwrap();
            statement.row_count()
        };

        Ok(row_count.unwrap().try_into().expect("Must obtain row count"))
    }
}

fn bind_parameters(statement: &mut StatementImpl, params: &[&'_ ODBCParameter]) -> () {
    for (i, param) in params.iter().enumerate() {
        unsafe {
            statement
                .bind_input_parameter((i + 1).try_into().unwrap(), *param)
                .unwrap();
        }
    }
}