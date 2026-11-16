/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! A Flight SQL prepared-statement handle is held by the client and sent back verbatim, so the
//! runtime must only act on handles it issued. These drive a real runtime over Flight with
//! handles a client built or altered itself, and with the handles the runtime issued, through
//! the calls that read one: `GetFlightInfo`, `DoGet` and `DoPut`.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use arrow_flight::{
    Action, FlightClient, FlightDescriptor, Ticket,
    encode::FlightDataEncoderBuilder,
    error::FlightError,
    sql::{
        ActionCreatePreparedStatementRequest, ActionCreatePreparedStatementResult, Any,
        CommandPreparedStatementQuery, CommandPreparedStatementUpdate,
        DoPutPreparedStatementResult, ProstMessageExt,
    },
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use futures::TryStreamExt as _;
use prost::Message as _;
use serde::Serialize;
use tonic::Code;

use crate::{flight::start_spice_test_app, init_tracing, utils::test_request_context};

const INVALID_HANDLE: &str = "Invalid prepared statement handle: it was not issued by this Spice runtime, or the runtime has restarted since. Create the prepared statement again and retry. See: https://spiceai.org/docs/api/arrow-flight-sql";

fn expect_invalid_handle<T: std::fmt::Debug>(result: Result<T, FlightError>, what: &str) {
    match result {
        Err(FlightError::Tonic(status)) => {
            assert_eq!(status.code(), Code::InvalidArgument, "{what}: {status}");
            assert_eq!(status.message(), INVALID_HANDLE, "{what}");
        }
        other => panic!("{what}: expected InvalidArgument, got {other:?}"),
    }
}

async fn create_handle(client: &mut FlightClient, query: &str) -> Result<Bytes, anyhow::Error> {
    let request = ActionCreatePreparedStatementRequest {
        query: query.to_string(),
        transaction_id: None,
    };
    let mut results = client
        .do_action(Action::new(
            "CreatePreparedStatement",
            request.as_any().encode_to_vec(),
        ))
        .await?;
    let body = results
        .try_next()
        .await?
        .ok_or_else(|| anyhow::anyhow!("CreatePreparedStatement returned no result"))?;
    let result: ActionCreatePreparedStatementResult = Any::decode(body)?
        .unpack()?
        .ok_or_else(|| anyhow::anyhow!("CreatePreparedStatement returned another message"))?;
    Ok(result.prepared_statement_handle)
}

fn query_command(handle: Bytes) -> Vec<u8> {
    CommandPreparedStatementQuery {
        prepared_statement_handle: handle,
    }
    .as_any()
    .encode_to_vec()
}

fn update_command(handle: Bytes) -> Vec<u8> {
    CommandPreparedStatementUpdate {
        prepared_statement_handle: handle,
    }
    .as_any()
    .encode_to_vec()
}

async fn get_flight_info(
    client: &mut FlightClient,
    handle: Bytes,
) -> Result<arrow_flight::FlightInfo, FlightError> {
    client
        .get_flight_info(FlightDescriptor::new_cmd(query_command(handle)))
        .await
}

async fn do_get(
    client: &mut FlightClient,
    command: Vec<u8>,
) -> Result<Vec<RecordBatch>, FlightError> {
    client
        .do_get(Ticket::new(command))
        .await?
        .try_collect()
        .await
}

/// Executes a prepared query the way a Flight SQL client does: `GetFlightInfo` on the handle,
/// then `DoGet` on the ticket it returns.
async fn execute(
    client: &mut FlightClient,
    handle: Bytes,
) -> Result<Vec<RecordBatch>, FlightError> {
    let info = get_flight_info(client, handle).await?;
    let ticket = info
        .endpoint
        .first()
        .and_then(|endpoint| endpoint.ticket.clone())
        .ok_or_else(|| FlightError::protocol("no ticket in FlightInfo"))?;
    do_get(client, ticket.ticket.to_vec()).await
}

/// Binds `parameters` to a query `handle` with `DoPut` and returns the handle the runtime hands
/// back.
async fn bind(
    client: &mut FlightClient,
    handle: Bytes,
    parameters: RecordBatch,
) -> Result<Bytes, FlightError> {
    bind_command(client, query_command(handle), parameters).await
}

/// Binds `parameters` with `DoPut` on a query or update `command` and returns the handle the
/// runtime hands back.
async fn bind_command(
    client: &mut FlightClient,
    command: Vec<u8>,
    parameters: RecordBatch,
) -> Result<Bytes, FlightError> {
    let stream = FlightDataEncoderBuilder::new()
        .with_flight_descriptor(Some(FlightDescriptor::new_cmd(command)))
        .build(futures::stream::iter([Ok(parameters)]));
    let results: Vec<_> = client.do_put(stream).await?.try_collect().await?;
    let metadata = results
        .first()
        .ok_or_else(|| FlightError::protocol("DoPut returned no result"))?
        .app_metadata
        .clone();
    let result = DoPutPreparedStatementResult::decode(metadata)
        .map_err(|e| FlightError::protocol(e.to_string()))?;
    result
        .prepared_statement_handle
        .ok_or_else(|| FlightError::protocol("DoPut returned no handle"))
}

fn int_parameter(value: i64) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("$1", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![value])) as ArrayRef],
    )
    .expect("parameter batch")
}

fn int64_values(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("column is Int64")
                .iter()
                .flatten()
                .collect::<Vec<_>>()
        })
        .collect()
}

fn ipc_stream(batch: &RecordBatch) -> Vec<u8> {
    let mut bytes = Vec::new();
    {
        let mut writer =
            arrow_ipc::writer::StreamWriter::try_new(&mut bytes, &batch.schema()).expect("writer");
        writer.write(batch).expect("write");
        writer.finish().expect("finish");
    }
    bytes
}

/// The runtime's statement layout, which any client can reproduce.
#[derive(Serialize)]
struct Statement {
    query: String,
    parameters: Vec<u8>,
    parameter_schema: Option<Vec<u8>>,
}

/// A handle as any client could build one by hand: the postcard encoding of a statement with
/// parameters the runtime never wrote.
fn forged_handle(query: &str, parameters: Vec<u8>) -> Bytes {
    postcard::to_stdvec(&Statement {
        query: query.to_string(),
        parameters,
        parameter_schema: None,
    })
    .expect("serialize")
    .into()
}

#[tokio::test]
async fn a_forged_handle_is_rejected() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (channel, _df) = start_spice_test_app(None, None, None).await?;
            let mut client = FlightClient::new(channel);

            let forged = forged_handle("SELECT $1 + 1 AS x", ipc_stream(&int_parameter(41)));

            expect_invalid_handle(
                get_flight_info(&mut client, forged.clone()).await,
                "GetFlightInfo on a forged query handle",
            );
            expect_invalid_handle(
                do_get(&mut client, query_command(forged.clone())).await,
                "DoGet on a forged query handle",
            );
            expect_invalid_handle(
                do_get(&mut client, update_command(forged.clone())).await,
                "DoGet on a forged update handle",
            );
            expect_invalid_handle(
                bind(&mut client, forged.clone(), int_parameter(1)).await,
                "DoPut on a forged query handle",
            );
            expect_invalid_handle(
                bind_command(&mut client, update_command(forged), int_parameter(1)).await,
                "DoPut on a forged update handle",
            );

            // The runtime is still serving: a handle it issued executes.
            let handle = create_handle(&mut client, "SELECT CAST(1 AS BIGINT) AS x").await?;
            assert_eq!(int64_values(&execute(&mut client, handle).await?), vec![1]);

            Ok(())
        })
        .await
}

#[tokio::test]
async fn an_issued_handle_executes_and_an_altered_one_is_rejected() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (channel, _df) = start_spice_test_app(None, None, None).await?;
            let mut client = FlightClient::new(channel);

            // Without parameters.
            let handle = create_handle(&mut client, "SELECT CAST(7 AS BIGINT) AS x").await?;
            assert_eq!(int64_values(&execute(&mut client, handle).await?), vec![7]);

            // With parameters: the handle `DoPut` returns is issued by the runtime too.
            let handle = create_handle(&mut client, "SELECT $1 + 1 AS x").await?;
            let bound = bind(&mut client, handle, int_parameter(41)).await?;
            assert_eq!(
                int64_values(&execute(&mut client, bound.clone()).await?),
                vec![42]
            );
            // Rebinding the bound handle replaces its parameters.
            let rebound = bind(&mut client, bound.clone(), int_parameter(99)).await?;
            assert_eq!(
                int64_values(&execute(&mut client, rebound).await?),
                vec![100]
            );
            // Binding through the update command takes the same handle, and the handle it
            // returns carries the new value.
            let rebound =
                bind_command(&mut client, update_command(bound.clone()), int_parameter(5)).await?;
            assert_eq!(int64_values(&execute(&mut client, rebound).await?), vec![6]);

            // Alter the bound parameter value inside the handle: 41 becomes 40.
            let value_at = bound
                .windows(8)
                .position(|window| window == 41_i64.to_le_bytes())
                .ok_or_else(|| anyhow::anyhow!("the bound value is carried in the handle"))?;
            let mut altered_parameters = bound.to_vec();
            altered_parameters[value_at] = 40;
            expect_invalid_handle(
                get_flight_info(&mut client, altered_parameters.clone().into()).await,
                "GetFlightInfo on a handle whose parameters were altered",
            );
            expect_invalid_handle(
                do_get(
                    &mut client,
                    query_command(altered_parameters.clone().into()),
                )
                .await,
                "DoGet on a handle whose parameters were altered",
            );
            expect_invalid_handle(
                bind(
                    &mut client,
                    altered_parameters.clone().into(),
                    int_parameter(1),
                )
                .await,
                "DoPut on a handle whose parameters were altered",
            );
            expect_invalid_handle(
                bind_command(
                    &mut client,
                    update_command(altered_parameters.into()),
                    int_parameter(1),
                )
                .await,
                "DoPut on an update handle whose parameters were altered",
            );

            Ok(())
        })
        .await
}
