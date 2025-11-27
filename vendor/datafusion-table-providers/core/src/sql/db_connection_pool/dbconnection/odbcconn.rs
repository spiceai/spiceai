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

use crate::sql::db_connection_pool::{
    dbconnection::{self, AsyncDbConnection, DbConnection, GenericError},
    runtime::run_async_with_tokio,
    DbConnectionPool,
};
use arrow_odbc::arrow_schema_from;
use arrow_odbc::odbc_api::{
    self, handles::SqlResult, handles::Statement, handles::StatementImpl,
    parameter::InputParameter, Connection, Cursor, CursorImpl,
};
use arrow_odbc::OdbcReader;
use arrow_odbc::OdbcReaderBuilder;
use async_stream::stream;
use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::TableReference;
use dyn_clone::DynClone;
use futures::lock::Mutex;
use secrecy::{ExposeSecret, SecretBox, SecretString};
use snafu::prelude::*;
use snafu::Snafu;
use tokio::runtime::Handle;

use tokio::sync::mpsc::Sender;

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
    ArrowError {
        source: datafusion::arrow::error::ArrowError,
    },
    #[snafu(display("arrow_odbc error: {source}"))]
    ArrowODBCError { source: arrow_odbc::Error },
    #[snafu(display("odbc_api Error: {source}"))]
    ODBCAPIError { source: odbc_api::Error },
    #[snafu(display("odbc_api Error: {message}"))]
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
    fn new(conn: Connection<'a>) -> Self {
        ODBCConnection {
            conn: Arc::new(conn.into()),
            params: Arc::new(HashMap::new()),
        }
    }

    async fn tables(&self, _schema: &str) -> Result<Vec<String>, super::Error> {
        unimplemented!()
    }

    async fn schemas(&self) -> Result<Vec<String>, super::Error> {
        unimplemented!()
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

        let schema = Arc::new(
            arrow_schema_from(&mut prepared, None, false)
                .boxed()
                .map_err(|e| dbconnection::Error::UnableToGetSchema { source: e })?,
        );

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
        let (schema_tx, mut schema_rx) = tokio::sync::mpsc::channel::<Arc<Schema>>(1);

        // clone internals and parameters to let the thread own them
        let conn = Arc::clone(&self.conn); // clones the mutex not the connection, so we can .lock a connection inside the thread
        let sql = sql.to_string();

        // ODBCParameter is a dynamic trait object, so we can't use std::clone::Clone because it's not object safe
        // DynClone provides an object-safe clone trait, which we use to clone the boxed parameters
        let params = params.iter().map(dyn_clone::clone).collect::<Vec<_>>();
        let secrets = Arc::clone(&self.params);

        let create_stream = async || -> Result<SendableRecordBatchStream> {
            let join_handle = tokio::task::spawn_blocking(move || {
                let handle = Handle::current();
                let cxn = handle.block_on(async { conn.lock().await });

                let mut prepared = cxn.prepare(&sql)?;
                let schema = Arc::new(arrow_schema_from(&mut prepared, None, false)?);
                blocking_channel_send(&schema_tx, Arc::clone(&schema))?;

                let mut statement = prepared.into_handle();

                bind_parameters(&mut statement, &params)?;

                // StatementImpl<'_>::execute is unsafe, CursorImpl<_>::new is unsafe
                let cursor = unsafe {
                    if let SqlResult::Error { function } = statement.execute() {
                        return Err(Error::ODBCAPIErrorNoSource {
                            message: function.to_string(),
                        }
                        .into());
                    }

                    Ok::<_, GenericError>(CursorImpl::new(statement.as_stmt_ref()))
                }?;

                let reader = build_odbc_reader(cursor, &schema, &secrets)?;
                for batch in reader {
                    blocking_channel_send(&batch_tx, batch.context(ArrowSnafu)?)?;
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

                if let Err(e) = join_handle.await {
                    yield Err(DataFusionError::Execution(format!(
                        "Failed to execute ODBC query: {e}"
                    )))
                }
            };

            let result: SendableRecordBatchStream =
                Box::pin(RecordBatchStreamAdapter::new(schema, output_stream));
            Ok(result)
        };
        run_async_with_tokio(create_stream).await
    }

    async fn execute(&self, query: &str, params: &[ODBCParameter]) -> Result<u64> {
        let cxn = self.conn.lock().await;
        let prepared = cxn.prepare(query)?;
        let mut statement = prepared.into_handle();

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

    let bind_as_usize = |k: &str, default: Option<usize>, f: &mut dyn FnMut(usize)| {
        params
            .get(k)
            .map(SecretBox::expose_secret)
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
