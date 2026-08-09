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

//! The trace id a Flight SQL client reads back for a query.
//!
//! These go through the raw `FlightServiceClient` rather than
//! `FlightSqlServiceClient`, which hands back a `FlightInfo` with the response
//! metadata already discarded — and the metadata is half of what is under test.

use arrow::array::RecordBatch;
use arrow_flight::{
    FlightClient, FlightDescriptor, Ticket,
    flight_service_client::FlightServiceClient,
    sql::{CommandStatementQuery, ProstMessageExt},
};
use futures::TryStreamExt as _;
use prost::Message as _;
use runtime_request_context::SPICE_TRACE_ID_HEADER as TRACE_ID_HEADER;
use tonic::{Request, Response, metadata::MetadataMap, transport::Channel};

use crate::{flight::start_spice_test_app, init_tracing, utils::test_request_context};

const PINNED: &str = "4bf92f3577b34da6a3ce929d0e0e4736";

fn get_flight_info_request(sql: &str) -> Request<FlightDescriptor> {
    let command = CommandStatementQuery {
        query: sql.to_string(),
        transaction_id: None,
    };
    Request::new(FlightDescriptor::new_cmd(command.as_any().encode_to_vec()))
}

/// The [`TRACE_ID_HEADER`] response metadata, which must be there on every
/// query response. Read through the constant, so a rename of the header
/// cannot leave the test asserting the old name.
fn trace_id_metadata(metadata: &MetadataMap) -> String {
    metadata
        .get(TRACE_ID_HEADER)
        .unwrap_or_else(|| panic!("the response must carry `{TRACE_ID_HEADER}`"))
        .to_str()
        .expect("a trace id is ASCII")
        .to_string()
}

/// The `trace_id` from `FlightInfo.app_metadata` — the one surface the Arrow
/// Flight SQL JDBC driver exposes, so its exact shape is the contract.
fn trace_id_app_metadata(app_metadata: &[u8]) -> String {
    let json = std::str::from_utf8(app_metadata).expect("app_metadata is UTF-8 JSON");
    let parsed: serde_json::Value = serde_json::from_str(json)
        .unwrap_or_else(|e| panic!("app_metadata must be JSON, got `{json}`: {e}"));

    parsed["trace_id"]
        .as_str()
        .unwrap_or_else(|| panic!("app_metadata must carry a string `trace_id`, got `{json}`"))
        .to_string()
}

fn assert_is_a_trace_id(trace_id: &str) {
    assert_eq!(trace_id.len(), 32, "a trace id is 32 hex characters");
    assert!(
        trace_id
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
        "a trace id is lowercase hexadecimal, got `{trace_id}`"
    );
    assert_ne!(
        trace_id,
        "0".repeat(32),
        "the all-zero id correlates nothing"
    );
}

async fn get_flight_info(
    client: &mut FlightServiceClient<Channel>,
    request: Request<FlightDescriptor>,
) -> Result<Response<arrow_flight::FlightInfo>, anyhow::Error> {
    Ok(client.get_flight_info(request).await?)
}

/// The whole point: a Flight SQL query answers with an id the client can read,
/// and the RPC that *runs* the query — a separate request — uses that same id.
/// An id returned by `GetFlightInfo` that `DoGet` did not adopt would name the
/// planning call and correlate nothing.
#[tokio::test]
async fn flight_sql_returns_one_trace_id_across_get_flight_info_and_do_get()
-> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (channel, _df) = start_spice_test_app(None, None, None).await?;
            let mut client = FlightServiceClient::new(channel.clone());

            let info =
                get_flight_info(&mut client, get_flight_info_request("SELECT 1 AS n")).await?;

            let from_metadata = trace_id_metadata(info.metadata());
            assert_is_a_trace_id(&from_metadata);

            let info = info.into_inner();
            assert_eq!(
                trace_id_app_metadata(&info.app_metadata),
                from_metadata,
                "`app_metadata` and the response metadata must name one id"
            );

            let ticket = info
                .endpoint
                .first()
                .and_then(|endpoint| endpoint.ticket.clone())
                .ok_or_else(|| anyhow::anyhow!("GetFlightInfo returned no ticket"))?;

            let results = FlightClient::new(channel).do_get(ticket).await?;
            assert_eq!(
                trace_id_metadata(results.headers()),
                from_metadata,
                "the execution must run under the id the client was already given"
            );

            // The wrapped ticket has to still describe the query, or the id
            // would have been bought at the cost of the result.
            let batches: Vec<RecordBatch> = results.try_collect().await?;
            assert_eq!(
                batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                1,
                "the query still has to return its row"
            );

            Ok(())
        })
        .await
}

/// A pooled client reuses one connection for many queries, so an id that
/// repeated per connection would correlate a whole pool's traffic to one row.
#[tokio::test]
async fn each_query_on_a_connection_gets_its_own_trace_id() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (channel, _df) = start_spice_test_app(None, None, None).await?;
            let mut client = FlightServiceClient::new(channel);

            let first =
                get_flight_info(&mut client, get_flight_info_request("SELECT 1 AS n")).await?;
            let second =
                get_flight_info(&mut client, get_flight_info_request("SELECT 1 AS n")).await?;

            assert_ne!(
                trace_id_metadata(first.metadata()),
                trace_id_metadata(second.metadata()),
                "two queries on one connection must not share an id"
            );

            Ok(())
        })
        .await
}

/// A caller that pins an id already knows what it wants correlated, so the id
/// returned is the one it sent rather than one of the runtime's.
#[tokio::test]
async fn a_pinned_trace_id_is_the_one_returned() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (channel, _df) = start_spice_test_app(None, None, None).await?;
            let mut client = FlightServiceClient::new(channel);

            let mut request = get_flight_info_request("SELECT 1 AS n");
            request
                .metadata_mut()
                .insert(TRACE_ID_HEADER, PINNED.parse()?);

            let info = get_flight_info(&mut client, request).await?;
            assert_eq!(trace_id_metadata(info.metadata()), PINNED);
            assert_eq!(
                trace_id_app_metadata(&info.into_inner().app_metadata),
                PINNED
            );

            Ok(())
        })
        .await
}

/// A ticket this runtime did not wrap — one a client built itself, or one from
/// an older runtime — still runs. The id is what is lost, never the query.
#[tokio::test]
async fn an_unwrapped_ticket_still_runs() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (channel, _df) = start_spice_test_app(None, None, None).await?;

            let command = CommandStatementQuery {
                query: "SELECT 1 AS n".to_string(),
                transaction_id: None,
            };
            let results = FlightClient::new(channel)
                .do_get(Ticket {
                    ticket: command.as_any().encode_to_vec().into(),
                })
                .await?;

            let batches: Vec<RecordBatch> = results.try_collect().await?;
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);

            Ok(())
        })
        .await
}
