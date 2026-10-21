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
//! every call that reads one: `GetFlightInfo`, `DoGet` and `DoPut`.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use arrow_flight::{
    Action, FlightClient, FlightDescriptor, Ticket,
    encode::FlightDataEncoderBuilder,
    error::FlightError,
    sql::{
        ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
        ActionCreatePreparedStatementResult, Any, CommandPreparedStatementQuery,
        CommandPreparedStatementUpdate, DoPutPreparedStatementResult, ProstMessageExt,
    },
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use futures::TryStreamExt as _;
use prost::Message as _;
use tonic::Code;

use crate::{flight::start_spice_test_app, init_tracing, utils::test_request_context};

const INVALID_HANDLE: &str = "Invalid prepared statement handle: it was not issued by this Spice runtime, or the runtime has restarted since. Create the prepared statement again and retry. See: https://spiceai.org/docs/api/arrow-flight-sql";

/// Far beyond any memory a test runner has: a server that allocates this before reading fails
/// the allocation, and the process with it.
const OVER_DECLARED_BODY: i64 = 500_000_000_000;

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

async fn close_handle(client: &mut FlightClient, handle: Bytes) -> Result<(), FlightError> {
    let request = ActionClosePreparedStatementRequest {
        prepared_statement_handle: handle,
    };
    let mut results = client
        .do_action(Action::new(
            "ClosePreparedStatement",
            request.as_any().encode_to_vec(),
        ))
        .await?;
    while results.try_next().await?.is_some() {}
    Ok(())
}

fn query_command(handle: Bytes) -> Vec<u8> {
    CommandPreparedStatementQuery {
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

/// Binds `parameters` to `handle` with `DoPut` and returns the handle the runtime hands back.
async fn bind(
    client: &mut FlightClient,
    handle: Bytes,
    parameters: RecordBatch,
) -> Result<Bytes, FlightError> {
    let stream = FlightDataEncoderBuilder::new()
        .with_flight_descriptor(Some(FlightDescriptor::new_cmd(query_command(handle))))
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

/// The parameter stream a client would bind for `int_parameter(value)`, with the batch message
/// patched to declare a body of `declared` bytes. The field is found by value: each occurrence
/// of the true length in the batch metadata is tried, and the one that changes what the parsed
/// message reports is the field.
fn over_declared_parameter_stream(declared: i64) -> Vec<u8> {
    let batch = int_parameter(42);
    let mut bytes = Vec::new();
    {
        let mut writer =
            arrow_ipc::writer::StreamWriter::try_new(&mut bytes, &batch.schema()).expect("writer");
        writer.write(&batch).expect("write");
        writer.finish().expect("finish");
    }
    let message_at = |at: usize| {
        let len = u32::from_le_bytes(bytes[at + 4..at + 8].try_into().expect("4 bytes"));
        let metadata = at + 8..at + 8 + len as usize;
        let body = arrow_ipc::root_as_message(&bytes[metadata.clone()])
            .expect("metadata parses")
            .bodyLength();
        (metadata, body)
    };
    let (schema_metadata, schema_body) = message_at(0);
    let (metadata, body) =
        message_at(schema_metadata.end + usize::try_from(schema_body).expect("non-negative"));
    let current = body.to_le_bytes();
    for at in metadata.start..=metadata.end - current.len() {
        if bytes[at..at + current.len()] != current {
            continue;
        }
        let mut patched = bytes.clone();
        patched[at..at + current.len()].copy_from_slice(&declared.to_le_bytes());
        if arrow_ipc::root_as_message(&patched[metadata.clone()])
            .is_ok_and(|message| message.bodyLength() == declared)
        {
            return patched;
        }
    }
    panic!("the batch message has no bodyLength field holding its body length");
}

fn postcard_varint(mut value: usize, out: &mut Vec<u8>) {
    loop {
        let byte = u8::try_from(value & 0x7f).expect("seven bits");
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// A handle as any client could build one by hand: the postcard encoding of the runtime's
/// `{ query, parameters, parameter_schema }` statement, with parameters the runtime never wrote.
fn forged_handle(query: &str, parameters: &[u8]) -> Bytes {
    let mut handle = Vec::new();
    postcard_varint(query.len(), &mut handle);
    handle.extend_from_slice(query.as_bytes());
    postcard_varint(parameters.len(), &mut handle);
    handle.extend_from_slice(parameters);
    handle.push(0); // parameter_schema: None
    handle.into()
}

#[tokio::test]
async fn a_forged_handle_is_rejected_before_its_parameters_are_decoded() -> Result<(), anyhow::Error>
{
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (channel, _df) = start_spice_test_app(None, None, None).await?;
            let mut client = FlightClient::new(channel);

            let forged = forged_handle(
                "SELECT $1 AS x",
                &over_declared_parameter_stream(OVER_DECLARED_BODY),
            );

            expect_invalid_handle(
                get_flight_info(&mut client, forged.clone()).await,
                "GetFlightInfo on a forged query handle",
            );
            expect_invalid_handle(
                do_get(&mut client, query_command(forged.clone())).await,
                "DoGet on a forged query handle",
            );
            expect_invalid_handle(
                do_get(
                    &mut client,
                    CommandPreparedStatementUpdate {
                        prepared_statement_handle: forged.clone(),
                    }
                    .as_any()
                    .encode_to_vec(),
                )
                .await,
                "DoGet on a forged update handle",
            );
            expect_invalid_handle(
                bind(&mut client, forged.clone(), int_parameter(1)).await,
                "DoPut on a forged query handle",
            );
            expect_invalid_handle(
                close_handle(&mut client, forged).await,
                "ClosePreparedStatement on a forged handle",
            );

            // The runtime is still serving: a handle it issued executes.
            let handle = create_handle(&mut client, "SELECT CAST(1 AS BIGINT) AS x").await?;
            assert_eq!(int64_values(&execute(&mut client, handle).await?), vec![1]);

            Ok(())
        })
        .await
}

#[tokio::test]
async fn an_issued_handle_executes_and_any_alteration_is_rejected() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (channel, _df) = start_spice_test_app(None, None, None).await?;
            let mut client = FlightClient::new(channel);

            // Without parameters.
            let query = "SELECT CAST(7 AS BIGINT) AS x";
            let handle = create_handle(&mut client, query).await?;
            assert_eq!(
                int64_values(&execute(&mut client, handle.clone()).await?),
                vec![7]
            );
            close_handle(&mut client, handle.clone())
                .await
                .expect("a handle this runtime issued closes");

            let query_at = handle
                .windows(query.len())
                .position(|window| window == query.as_bytes())
                .ok_or_else(|| anyhow::anyhow!("the query is carried in the handle"))?;
            let mut altered_query = handle.to_vec();
            altered_query[query_at + "SELECT CAST(".len()] = b'8';
            expect_invalid_handle(
                get_flight_info(&mut client, altered_query.clone().into()).await,
                "GetFlightInfo on a handle whose query was altered",
            );
            expect_invalid_handle(
                do_get(&mut client, query_command(altered_query.into())).await,
                "DoGet on a handle whose query was altered",
            );

            let mut altered_tag = handle.to_vec();
            if let Some(last) = altered_tag.last_mut() {
                *last ^= 0x01;
            }
            expect_invalid_handle(
                get_flight_info(&mut client, altered_tag.into()).await,
                "GetFlightInfo on a handle whose tag was altered",
            );
            expect_invalid_handle(
                get_flight_info(&mut client, handle.slice(..handle.len() - 1)).await,
                "GetFlightInfo on a truncated handle",
            );

            // With parameters: the handle `DoPut` returns is issued by the runtime too.
            let handle = create_handle(&mut client, "SELECT $1 + 1 AS x").await?;
            let bound = bind(&mut client, handle.clone(), int_parameter(41)).await?;
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
                bind(&mut client, altered_parameters.into(), int_parameter(1)).await,
                "DoPut on a handle whose parameters were altered",
            );

            Ok(())
        })
        .await
}

#[tokio::test]
async fn an_altered_update_handle_is_rejected() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (channel, _df) = start_spice_test_app(None, None, None).await?;
            let mut client = FlightClient::new(channel);

            let handle = create_handle(&mut client, "SELECT CAST(1 AS BIGINT) AS x").await?;
            let mut altered = handle.to_vec();
            altered[0] ^= 0x01;
            expect_invalid_handle(
                do_get(
                    &mut client,
                    CommandPreparedStatementUpdate {
                        prepared_statement_handle: altered.into(),
                    }
                    .as_any()
                    .encode_to_vec(),
                )
                .await,
                "DoGet on an altered update handle",
            );

            Ok(())
        })
        .await
}
