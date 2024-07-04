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
use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_odbc::arrow_schema_from;
use arrow_odbc::OdbcReader;
use arrow_odbc::OdbcReaderBuilder;
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::sql::TableReference;
use datafusion_table_providers::sql::db_connection_pool::{
    dbconnection::{self, AsyncDbConnection, DbConnection},
    DbConnectionPool,
};
use futures::lock::Mutex;
use odbc_api::handles::Statement;
use odbc_api::handles::StatementImpl;
use odbc_api::parameter::InputParameter;
use odbc_api::Cursor;
use odbc_api::CursorImpl;
use secrecy::{ExposeSecret, Secret, SecretString};
use snafu::prelude::*;
use snafu::Snafu;

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
    #[snafu(display("Failed to convert query result to Arrow: {source}"))]
    ArrowError { source: arrow::error::ArrowError },
    #[snafu(display("arrow_odbc error: {source}"))]
    ArrowODBCError { source: arrow_odbc::Error },
    #[snafu(display("odbc_api Error: {source}"))]
    ODBCAPIError { source: odbc_api::Error },
    #[snafu(display("Failed to convert query result to Arrow: {source}"))]
    TryFromError { source: std::num::TryFromIntError },
    #[snafu(display("Unable to bind integer parameter: {source}"))]
    UnableToBindIntParameter { source: std::num::TryFromIntError },
}

pub struct ODBCConnection<'a> {
    pub conn: Arc<Mutex<Connection<'a>>>,
    pub params: Arc<HashMap<String, SecretString>>,
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

    fn as_async(&self) -> Option<&dyn AsyncDbConnection<Connection<'a>, ODBCParameter>> {
        Some(self)
    }
}

#[async_trait]
impl<'a> AsyncDbConnection<Connection<'a>, ODBCParameter> for ODBCConnection<'a>
where
    'a: 'static,
{
    fn new(conn: Connection<'a>) -> Self {
        ODBCConnection {
            conn: Arc::new(conn.into()),
            params: Arc::new(HashMap::new()),
        }
    }

    #[must_use]
    async fn get_schema(
        &self,
        table_reference: &TableReference,
    ) -> Result<SchemaRef, dbconnection::Error> {
        let cxn = self.conn.lock().await;

        let mut prepared = cxn
            .prepare(&format!(
                "SELECT * FROM {} LIMIT 1",
                table_reference.to_quoted_string()
            ))
            .boxed()
            .map_err(|e| dbconnection::Error::UnableToGetSchema { source: e })?;

        let schema = Arc::new(
            arrow_schema_from(&mut prepared)
                .boxed()
                .map_err(|e| dbconnection::Error::UnableToGetSchema { source: e })?,
        );

        Ok(schema)
    }

    async fn query_arrow(
        &self,
        sql: &str,
        params: &[ODBCParameter],
    ) -> Result<SendableRecordBatchStream, Box<dyn std::error::Error + Send + Sync>> {
        let cxn = self.conn.lock().await;
        let mut prepared = cxn.prepare(sql)?;
        let schema = Arc::new(arrow_schema_from(&mut prepared)?);
        let mut statement = prepared.into_statement();

        bind_parameters(&mut statement, params)?;

        // StatementImpl<'_>::execute is unsafe, CursorImpl<_>::new is unsafe
        let cursor = unsafe {
            statement.execute().unwrap();
            CursorImpl::new(statement.as_stmt_ref())
        };

        let reader = build_odbc_reader(cursor, &schema, &self.params)?;
        let mut results: Vec<RecordBatch> = vec![];
        for batch in reader {
            results.push(batch.context(ArrowSnafu)?);
        }

        Ok(Box::pin(MemoryStream::try_new(results, schema, None)?))
    }

    async fn execute(
        &self,
        query: &str,
        params: &[ODBCParameter],
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let cxn = self.conn.lock().await;
        let prepared = cxn.prepare(query)?;
        let mut statement = prepared.into_statement();

        bind_parameters(&mut statement, params)?;

        let row_count = unsafe {
            statement.execute().unwrap();
            statement.row_count()
        };

        Ok(row_count.unwrap().try_into().context(TryFromSnafu)?)
    }
}

fn build_odbc_reader<C: Cursor>(
    cursor: C,
    schema: &Arc<Schema>,
    params: &HashMap<String, SecretString>,
) -> Result<OdbcReader<C>, Error> {
    let mut builder = OdbcReaderBuilder::new();
    builder.with_schema(Arc::clone(schema));

    let bind_as_usize = |k: &str, f: &mut dyn FnMut(usize)| {
        params
            .get(k)
            .map(Secret::expose_secret)
            .cloned()
            .and_then(|s| s.parse::<usize>().ok())
            .into_iter()
            .for_each(f);
    };

    bind_as_usize("max_binary_size", &mut |s| {
        builder.with_max_binary_size(s);
    });
    bind_as_usize("max_text_size", &mut |s| {
        builder.with_max_text_size(s);
    });
    bind_as_usize("max_bytes_per_batch", &mut |s| {
        builder.with_max_bytes_per_batch(s);
    });
    bind_as_usize("max_num_rows_per_batch", &mut |s| {
        builder.with_max_num_rows_per_batch(s);
    });

    builder.build(cursor).context(ArrowODBCSnafu)
}

/// Binds parameter to an ODBC statement.
///
/// `StatementImpl<'_>::bind_input_parameter` is unsafe.
fn bind_parameters(statement: &mut StatementImpl, params: &[ODBCParameter]) -> Result<(), Error> {
    for (i, param) in params.iter().enumerate() {
        unsafe {
            statement
                .bind_input_parameter(
                    (i + 1).try_into().context(UnableToBindIntParameterSnafu)?,
                    param.as_input_parameter(),
                )
                .unwrap();
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use odbc_api::handles::OutputStringBuffer;
    use odbc_api::IntoParameter;

    use crate::odbcpool::ODBCPool;
    use std::str;

    use super::*;

    // This test crudely validates that parameters are being received by the ODBC driver
    #[cfg(feature = "odbc")]
    #[tokio::test]
    async fn test_bind_parameters() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // It is possible to connect to the SQLite driver without an underlying file
        let pool = ODBCPool::new(Arc::new(HashMap::new())).expect("Must create ODBC pool");
        let env = pool.odbc_environment();
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
            Box::new((100_i32).into_parameter()),
        ];

        bind_parameters(&mut statement, params.as_slice()).expect("Must bind parameters");

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

        first.get_text(1, &mut name_vec).expect("Must get name");
        first.get_text(2, &mut age_vec).expect("Must get age");

        assert_eq!(str::from_utf8(&name_vec).expect("valid utf-8"), "hopper");
        assert_eq!(str::from_utf8(&age_vec).expect("valid utf-8"), "100");

        Ok(())
    }
}
