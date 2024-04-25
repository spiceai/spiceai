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
use arrow::record_batch::RecordBatch;
use arrow_odbc::arrow_schema_from;
use arrow_odbc::OdbcReaderBuilder;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::sql::TableReference;
use futures::lock::Mutex;
use odbc_api::handles::Statement;
use odbc_api::handles::StatementImpl;
use odbc_api::parameter::InputParameter;
use odbc_api::CursorImpl;
use snafu::prelude::*;
use snafu::Snafu;

use crate::DbConnectionPool;

use super::AsyncDbConnection;
use super::DbConnection;
use super::Result;
use odbc_api::Connection;

pub trait ODBCSyncParameter: InputParameter + Sync {
    fn as_input_parameter(&self) -> &dyn InputParameter;
}

impl<T: InputParameter + Sync> ODBCSyncParameter for T {
    fn as_input_parameter(&self) -> &dyn InputParameter {
        self
    }
}

pub type ODBCParameter = Box<dyn ODBCSyncParameter>;
pub type ODBCDbConnection<'a> = (dyn DbConnection<Connection<'a>, ODBCParameter>);
pub type ODBCDbConnectionPool<'a> =
    dyn DbConnectionPool<Connection<'a>, ODBCParameter> + Sync + Send;

#[derive(Debug, Snafu)]
pub enum Error {
    ArrowODBCError { source: arrow_odbc::Error },
    ODBCAPIError { source: odbc_api::Error },
}

pub struct ODBCConnection<'a> {
    pub conn: Arc<Mutex<Connection<'a>>>,
}

impl<'a> DbConnection<Connection<'a>, ODBCParameter> for ODBCConnection<'a>
where
    'a: 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_async(&self) -> Option<&dyn super::AsyncDbConnection<Connection<'a>, ODBCParameter>> {
        Some(self)
    }
}

#[async_trait::async_trait]
impl<'a> AsyncDbConnection<Connection<'a>, ODBCParameter> for ODBCConnection<'a>
where
    'a: 'static,
{
    fn new(conn: Connection<'a>) -> Self {
        ODBCConnection {
            conn: Arc::new(conn.into()),
        }
    }

    #[must_use]
    async fn get_schema(&self, table_reference: &TableReference) -> Result<SchemaRef> {
        let cxn = self.conn.lock().await;

        let mut prepared = cxn.prepare(&format!(
            "SELECT * FROM {} LIMIT 1",
            table_reference.to_quoted_string()
        ))?;
        let schema = Arc::new(arrow_schema_from(&mut prepared)?);

        Ok(schema)
    }

    async fn query_arrow(
        &self,
        sql: &str,
        params: &[ODBCParameter],
    ) -> Result<SendableRecordBatchStream> {
        let cxn = self.conn.lock().await;
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

        let results: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();

        Ok(Box::pin(MemoryStream::try_new(results, schema, None)?))
    }

    async fn execute(&self, query: &str, params: &[ODBCParameter]) -> Result<u64> {
        let cxn = self.conn.lock().await;
        let prepared = cxn.prepare(query)?;
        let mut statement = prepared.into_statement();

        bind_parameters(&mut statement, params);

        let row_count = unsafe {
            statement.execute().unwrap();
            statement.row_count()
        };

        Ok(row_count
            .unwrap()
            .try_into()
            .expect("Must obtain row count"))
    }
}

fn bind_parameters(statement: &mut StatementImpl, params: &[ODBCParameter]) {
    for (i, param) in params.iter().enumerate() {
        unsafe {
            statement
                .bind_input_parameter((i + 1).try_into().unwrap(), param.as_input_parameter())
                .unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use odbc_api::handles::OutputStringBuffer;
    use odbc_api::IntoParameter;

    use crate::odbcpool::ODBCPool;
    use crate::Result;
    use std::error::Error;
    use std::str;

    use super::*;

    // This test crudely validates that parameters are being received by the ODBC driver
    #[cfg(feature = "odbc")]
    #[tokio::test]
    async fn test_bind_parameters() -> Result<(), Box<dyn Error + Send + Sync>> {
        use odbc_api::Cursor;

        // It is possible to connect to the SQLite driver without an underlying file
        let pool = ODBCPool::new(Arc::new(None), None)
            .await
            .expect("Must make pool");
        let env = unsafe { pool.odbc_environment() };
        let driver_cxn = env
            .driver_connect(
                "Driver={SQLite}",
                &mut OutputStringBuffer::empty(),
                odbc_api::DriverCompleteOption::NoPrompt,
            )
            .expect("Must make driver connection");

        // Using a projection as our 'table', we can make a simple query to determine whether
        // or not our parameters are being correctly bound. This won't return any rows unless
        // we bind ("hopper", 100).
        let mut statement = driver_cxn
            .prepare("select * from (select 'hopper' as name, 100 as age) as cats where name = ? and age = ?")
            .expect("Must prepare")
            .into_statement();

        let params: Vec<Box<dyn ODBCSyncParameter>> = vec![
            Box::new("hopper".into_parameter()),
            Box::new((100 as i32).into_parameter()),
        ];

        bind_parameters(&mut statement, params.as_slice());

        let mut cursor = unsafe {
            statement.execute().unwrap();
            CursorImpl::new(statement.as_stmt_ref())
        };

        let mut first = cursor
            .next_row()
            .expect("At least one row")
            .expect("That is present");

        let mut name_vec = vec![];
        let mut age_vec = vec![];

        first.get_text(1, &mut name_vec).unwrap();
        first.get_text(2, &mut age_vec).unwrap();

        assert_eq!(str::from_utf8(&name_vec).unwrap(), "hopper");
        assert_eq!(str::from_utf8(&age_vec).unwrap(), "100");

        Ok(())
    }
}
