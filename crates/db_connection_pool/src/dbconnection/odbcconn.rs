/*
Copyright 2024-2025 The Spice.ai OSS Authors

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
use arrow_odbc::OdbcReader;
use arrow_odbc::OdbcReaderBuilder;
use arrow_odbc::arrow_schema_from;
use async_stream::stream;
use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::TableReference;
use datafusion_table_providers::sql::db_connection_pool::{
    DbConnectionPool,
    dbconnection::{self, AsyncDbConnection, DbConnection, GenericError},
};
use dyn_clone::DynClone;
use futures::lock::Mutex;
use odbc_api::Cursor;
use odbc_api::CursorImpl;
use odbc_api::handles::SqlResult;
use odbc_api::handles::Statement;
use odbc_api::handles::StatementImpl;
use odbc_api::handles::{Diagnostics, Record};
use odbc_api::parameter::InputParameter;
use secrecy::{ExposeSecret, SecretBox, SecretString};
use snafu::Snafu;
use snafu::prelude::*;
use tokio::runtime::Handle;

use odbc_api::Connection;
use tokio::sync::mpsc::Sender;
use util::shutdown_signal;

type Result<T, E = GenericError> = std::result::Result<T, E>;

pub trait ODBCSyncParameter: InputParameter + Sync + Send + DynClone {
    fn as_input_parameter(&self) -> &dyn InputParameter;
}

impl<T: InputParameter + Sync + Send + DynClone> ODBCSyncParameter for T {
    fn as_input_parameter(&self) -> &dyn InputParameter {
        self
    }
}

dyn_clone::clone_trait_object!(ODBCSyncParameter);

pub type ODBCParameter = Box<dyn ODBCSyncParameter>;
pub type ODBCDbConnection<'a> = dyn DbConnection<Connection<'a>, ODBCParameter>;
pub type ODBCDbConnectionPool<'a> =
    dyn DbConnectionPool<Connection<'a>, ODBCParameter> + Sync + Send;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to convert query result to Arrow: {source}"))]
    ArrowError { source: arrow::error::ArrowError },
    #[snafu(display("Failed to read ODBC Arrow batch: {source}"))]
    ODBCArrowReadError {
        source: arrow_odbc::arrow::error::ArrowError,
    },
    #[snafu(display("ODBC driver error: {source}"))]
    ArrowODBCError { source: arrow_odbc::Error },
    #[snafu(display("ODBC connection error: {source}"))]
    ODBCAPIError { source: odbc_api::Error },
    #[snafu(display("{message}"))]
    ODBCAPIErrorNoSource { message: String },
    #[snafu(display("Failed to convert query result to Arrow: {source}"))]
    TryFromError { source: std::num::TryFromIntError },
    #[snafu(display("Unable to bind integer parameter: {source}"))]
    UnableToBindIntParameter { source: std::num::TryFromIntError },
    #[snafu(display("Internal communication channel error: {message}"))]
    ChannelError { message: String },
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

fn blocking_channel_send<T>(channel: &Sender<T>, item: T) -> Result<()> {
    match channel.blocking_send(item) {
        Ok(()) => Ok(()),
        Err(e) => Err(Error::ChannelError {
            message: format!("{e}"),
        }
        .into()),
    }
}

#[async_trait]
impl<'a> AsyncDbConnection<Connection<'a>, ODBCParameter> for ODBCConnection<'a>
where
    'a: 'static,
{
    fn new(_: Connection<'a>) -> Self {
        unimplemented!(
            "ODBCConnection::new() is not used; use the constructor with parameters instead"
        )
    }

    async fn tables(&self, _schema: &str) -> Result<Vec<String>, dbconnection::Error> {
        // ODBC catalog functions are driver-specific and require complex C API calls.
        // Each ODBC driver has different capabilities, so this method is not implemented.
        Err(dbconnection::Error::UnableToGetTables {
            source: "ODBC tables() requires driver-specific implementation".into(),
        })
    }

    async fn schemas(&self) -> Result<Vec<String>, dbconnection::Error> {
        // ODBC catalog functions are driver-specific and require complex C API calls.
        // Each ODBC driver has different capabilities, so this method is not implemented.
        Err(dbconnection::Error::UnableToGetSchemas {
            source: "ODBC schemas() requires driver-specific implementation".into(),
        })
    }

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

        let odbc_schema = arrow_schema_from(&mut prepared, None, false)
            .boxed()
            .map_err(|e| dbconnection::Error::UnableToGetSchema { source: e })?;
        let schema = Arc::new(odbc_schema_to_arrow(&odbc_schema).map_err(|e| {
            dbconnection::Error::UnableToGetSchema {
                source: e.to_string().into(),
            }
        })?);

        Ok(schema)
    }

    async fn query_arrow(
        &self,
        sql: &str,
        params: &[ODBCParameter],
        _projected_schema: Option<SchemaRef>,
    ) -> Result<SendableRecordBatchStream> {
        // prepare some tokio channels to communicate query results back from the thread
        let (batch_tx, mut batch_rx) = tokio::sync::mpsc::channel::<RecordBatch>(4);
        let (schema_tx, mut schema_rx) = tokio::sync::mpsc::channel::<SchemaRef>(1);

        // clone internals and parameters to let the thread own them
        let conn = Arc::clone(&self.conn); // clones the mutex not the connection, so we can .lock a connection inside the thread
        let sql = sql.to_string();

        // ODBCParameter is a dynamic trait object, so we can't use std::clone::Clone because it's not object safe
        // DynClone provides an object-safe clone trait, which we use to clone the boxed parameters
        let params = params.iter().map(dyn_clone::clone).collect::<Vec<_>>();
        let secrets = Arc::clone(&self.params);

        let cancellation_token = tokio_util::sync::CancellationToken::new();
        let cloned_token = cancellation_token.clone();

        tokio::spawn(async move {
            let shutdown = async {
                shutdown_signal().await;
                cancellation_token.clone().cancel();
            };

            tokio::select! {
                () = cancellation_token.cancelled() => {},
                () = shutdown => {},
            }
        });

        let join_handle = tokio::task::spawn_blocking(move || {
            let handle = Handle::current();
            let cxn = handle.block_on(async { conn.lock().await });

            let mut prepared = cxn.prepare(&sql)?;
            let odbc_schema = Arc::new(arrow_schema_from(&mut prepared, None, false)?);
            let schema = Arc::new(odbc_schema_to_arrow(&odbc_schema)?);
            blocking_channel_send(&schema_tx, Arc::clone(&schema))?;

            let mut statement = prepared.into_handle();

            bind_parameters(&mut statement, &params)?;

            // StatementImpl<'_>::execute is unsafe, CursorImpl<_>::new is unsafe
            let cursor = unsafe {
                match statement.execute() {
                    SqlResult::Success(()) | SqlResult::SuccessWithInfo(()) => {}
                    SqlResult::Error { function } => {
                        let detail = odbc_error_message(&statement, function);
                        return Err(Error::ODBCAPIErrorNoSource {
                            message: format!("SQLExecute failed: {detail}"),
                        }
                        .into());
                    }
                    SqlResult::NoData => {
                        return Err(Error::ODBCAPIErrorNoSource {
                            message: "Query returned NoData (no result set)".to_string(),
                        }
                        .into());
                    }
                    state @ (SqlResult::NeedData | SqlResult::StillExecuting) => {
                        return Err(Error::ODBCAPIErrorNoSource {
                            message: format!("Unexpected ODBC state after execute: {state:?}"),
                        }
                        .into());
                    }
                }

                // Verify the statement actually produced a result set before creating a cursor
                // Reading from an invalid cursor would segfault in the ODBC C driver.
                match statement.num_result_cols() {
                    SqlResult::Success(cols) | SqlResult::SuccessWithInfo(cols) => {
                        if cols == 0 {
                            return Err(Error::ODBCAPIErrorNoSource {
                                message:
                                    "Statement did not produce a result set (0 columns reported)"
                                        .to_string(),
                            }
                            .into());
                        }
                    }
                    SqlResult::Error { function } => {
                        let detail = odbc_error_message(&statement, function);
                        return Err(Error::ODBCAPIErrorNoSource {
                            message: format!("Failed to determine result column count: {detail}"),
                        }
                        .into());
                    }
                    state => {
                        return Err(Error::ODBCAPIErrorNoSource {
                            message: format!(
                                "Unexpected ODBC state checking result columns: {state:?}"
                            ),
                        }
                        .into());
                    }
                }

                Ok::<_, GenericError>(CursorImpl::new(statement.as_stmt_ref()))
            }?;

            let reader = build_odbc_reader(cursor, &odbc_schema, &secrets)?;
            for batch in reader {
                if cloned_token.is_cancelled() {
                    return Err(Error::ChannelError {
                        message: "process is terminated".to_string(),
                    }
                    .into());
                }
                let batch = batch.context(ODBCArrowReadSnafu)?;
                blocking_channel_send(&batch_tx, odbc_record_batch_to_arrow(&batch)?)?;
            }
            if !cloned_token.is_cancelled() {
                cloned_token.cancel();
            }

            Ok::<_, GenericError>(())
        });

        // we need to wait for the schema first before we can build our RecordBatchStreamAdapter
        let Some(schema) = schema_rx.recv().await else {
            // if the channel drops, the task errored
            if !join_handle.is_finished() {
                unreachable!("Schema channel should not have dropped before the task finished");
            }

            let result = join_handle.await?;
            let Err(err) = result else {
                unreachable!("Task should have errored");
            };

            return Err(err);
        };

        let output_stream = stream! {
            while let Some(batch) = batch_rx.recv().await {
                yield Ok(batch);
            }

            match join_handle.await {
                Err(e) => {
                    yield Err(DataFusionError::Execution(format!(
                        "Failed to execute ODBC query: {e}"
                    )))
                }
                Ok(Err(e)) => {
                    yield Err(DataFusionError::Execution(format!(
                        "Failed to execute ODBC query: {e}"
                    )))
                }
                Ok(Ok(())) => {}
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            output_stream,
        )))
    }

    async fn execute(&self, query: &str, params: &[ODBCParameter]) -> Result<u64> {
        let cxn = self.conn.lock().await;
        let prepared = cxn.prepare(query)?;
        let mut statement = prepared.into_handle();

        bind_parameters(&mut statement, params)?;

        let row_count = unsafe {
            if let SqlResult::Error { function } = statement.execute() {
                let detail = odbc_error_message(&statement, function);
                return Err(Error::ODBCAPIErrorNoSource {
                    message: format!("Failed to execute statement: {detail}"),
                }
                .into());
            }

            match statement.row_count() {
                SqlResult::Success(count) | SqlResult::SuccessWithInfo(count) => count,
                SqlResult::NoData => 0,
                SqlResult::Error { function } => {
                    let detail = odbc_error_message(&statement, function);
                    return Err(Error::ODBCAPIErrorNoSource {
                        message: format!("Failed to get row count: {detail}"),
                    }
                    .into());
                }
                SqlResult::NeedData | SqlResult::StillExecuting => {
                    return Err(Error::ODBCAPIErrorNoSource {
                        message: "Unexpected SQL state when getting row count".to_string(),
                    }
                    .into());
                }
            }
        };

        Ok(row_count.try_into().context(TryFromSnafu)?)
    }
}

fn build_odbc_reader<C: Cursor>(
    cursor: C,
    schema: &Arc<arrow_odbc::arrow::datatypes::Schema>,
    params: &HashMap<String, SecretString>,
) -> Result<OdbcReader<C>, Error> {
    let mut builder = OdbcReaderBuilder::new();
    builder.with_schema(Arc::clone(schema));
    builder.trim_fixed_sized_characters(true);

    let bind_as_usize = |k: &str, default: Option<usize>, f: &mut dyn FnMut(usize)| {
        params
            .get(k)
            .map(SecretBox::expose_secret)
            .map(String::from)
            .and_then(|s| s.parse::<usize>().ok())
            .or(default)
            .into_iter()
            .for_each(f);
    };

    bind_as_usize("max_binary_size", None, &mut |s| {
        builder.with_max_binary_size(s);
    });
    bind_as_usize("max_text_size", None, &mut |s| {
        builder.with_max_text_size(s);
    });
    bind_as_usize("max_bytes_per_batch", Some(512_000_000), &mut |s| {
        builder.with_max_bytes_per_batch(s);
    });

    // larger default max_num_rows_per_batch reduces IO overhead but increases memory usage
    // lower numbers reduce memory usage but increase IO overhead
    bind_as_usize("max_num_rows_per_batch", Some(4000), &mut |s| {
        builder.with_max_num_rows_per_batch(s);
    });

    builder.build(cursor).context(ArrowODBCSnafu)
}

// arrow-odbc is built against the workspace Arrow (its `arrow` dep resolves to the
// patched workspace Arrow), so its schema type is already the workspace `Schema` —
// no conversion (previously an Arrow-57 -> 58 IPC round-trip) is needed.
fn odbc_schema_to_arrow(schema: &arrow_odbc::arrow::datatypes::Schema) -> Result<Schema, Error> {
    Ok(schema.clone())
}

// arrow-odbc record batches are already workspace Arrow batches (Arrow clones are
// shallow Arc bumps), so we pass them through directly.
fn odbc_record_batch_to_arrow(
    record_batch: &arrow_odbc::arrow::record_batch::RecordBatch,
) -> Result<RecordBatch, Error> {
    Ok(record_batch.clone())
}

/// Extracts diagnostic error messages from an ODBC handle after a failed call.
///
/// Returns a formatted string containing all diagnostic records, or the function name
/// if no diagnostic records are available.
fn odbc_error_message(handle: &(impl Diagnostics + ?Sized), function: &str) -> String {
    let mut messages = Vec::new();
    let mut record = Record::with_capacity(512);
    let mut rec_number: i16 = 1;
    while record.fill_from(handle, rec_number) {
        messages.push(format!("{record}"));
        rec_number += 1;

        if rec_number > 10 {
            // A failed ODBC call typically produces 1–3 diagnostic records; cap at 10 to guard
            // against misbehaving drivers that could cause an infinite loop or an overflow.
            messages.push("...".to_string());
            break;
        }
    }
    if messages.is_empty() {
        function.to_string()
    } else {
        messages.join("; ")
    }
}

/// Binds parameter to an ODBC statement.
///
/// `StatementImpl<'_>::bind_input_parameter` is unsafe.
fn bind_parameters(statement: &mut StatementImpl, params: &[ODBCParameter]) -> Result<(), Error> {
    for (i, param) in params.iter().enumerate() {
        unsafe {
            let param_index = (i + 1).try_into().context(UnableToBindIntParameterSnafu)?;
            if let SqlResult::Error { function } =
                statement.bind_input_parameter(param_index, param.as_input_parameter())
            {
                let detail = odbc_error_message(statement, function);
                return Err(Error::ODBCAPIErrorNoSource {
                    message: format!("Failed to bind parameter {}: {detail}", i + 1),
                });
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use odbc_api::IntoParameter;
    use odbc_api::handles::OutputStringBuffer;

    use crate::odbcpool::ODBCPool;
    use std::str;

    use super::*;

    // This test crudely validates that parameters are being received by the ODBC driver.
    // Requires SQLite ODBC driver to be installed: `apt install libsqliteodbc` or equivalent.
    #[ignore = "Requires SQLite ODBC driver"]
    #[cfg(feature = "odbc")]
    #[tokio::test]
    async fn test_bind_parameters() -> Result<()> {
        // It is possible to connect to the SQLite driver without an underlying file
        // We provide a dummy connection_string since ODBCPool::new requires it,
        // but we use driver_connect directly which ignores it
        let mut params = HashMap::new();
        params.insert(
            "connection_string".to_string(),
            secrecy::SecretString::from("Driver={SQLite}"),
        );
        let pool = ODBCPool::new(params).expect("Must create ODBC pool");
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
            .into_handle();

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
