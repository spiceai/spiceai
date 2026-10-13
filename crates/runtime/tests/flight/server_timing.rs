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

//! The `server-timing` trailer a Flight client reads a query's server time from.
//!
//! Only the `DoGet` that runs a query reports it, and it covers the
//! `GetFlightInfo` that planned the query too. `GetFlightInfo` reports nothing
//! itself, or a client adding up the two calls would count the planning twice.

use std::time::Instant;

use arrow::array::RecordBatch;
use arrow_flight::{
    FlightDescriptor, Ticket,
    decode::FlightRecordBatchStream,
    sql::{
        CommandGetCatalogs, CommandStatementQuery, ProstMessageExt, client::FlightSqlServiceClient,
    },
};
use futures::TryStreamExt as _;
use prost::Message as _;
use tonic::{Request, metadata::MetadataMap};

use crate::{
    flight::{create_flight_client, start_spice_test_app},
    init_tracing,
    utils::test_request_context,
};

const SERVER_TIMING: &str = "server-timing";

/// The milliseconds in a `total;dur=<ms>` value, which must have exactly that
/// shape: one `total` metric, a duration with three decimals.
#[track_caller]
fn parse_total(value: &str) -> f64 {
    let duration = value
        .strip_prefix("total;dur=")
        .unwrap_or_else(|| panic!("expected `total;dur=<ms>`, got `{value}`"));
    let (_, decimals) = duration
        .split_once('.')
        .unwrap_or_else(|| panic!("the duration must carry decimals, got `{value}`"));
    assert_eq!(
        decimals.len(),
        3,
        "the duration must carry exactly three decimals, got `{value}`"
    );
    duration
        .parse::<f64>()
        .unwrap_or_else(|e| panic!("the duration must be a number, got `{value}`: {e}"))
}

/// Drains a `DoGet` and returns its rows and its `server-timing` trailer, if any.
async fn drain(
    mut stream: FlightRecordBatchStream,
) -> Result<(usize, Option<String>), anyhow::Error> {
    let mut rows = 0;
    while let Some(batch) = stream.try_next().await? {
        rows += batch.num_rows();
    }
    let trailers = stream
        .trailers()
        .ok_or_else(|| anyhow::anyhow!("a completed DoGet must expose its trailers"))?;
    let server_timing = trailers
        .get(SERVER_TIMING)
        .map(|value| value.to_str().map(str::to_string))
        .transpose()?;
    Ok((rows, server_timing))
}

fn no_server_timing(metadata: &MetadataMap) -> bool {
    metadata.get(SERVER_TIMING).is_none()
}

/// The Flight SQL statement path: `GetFlightInfo` then `DoGet`. The trailer's
/// total is a server measurement of both calls, so it is positive and within
/// the time the client spent on the pair.
#[tokio::test]
async fn a_flight_sql_query_reports_its_server_time_in_the_do_get_trailer()
-> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (channel, _df) = start_spice_test_app(None, None, None).await?;
            let mut client = create_flight_client(channel, None)?;

            let started = Instant::now();
            let info = client
                .inner_mut()
                .get_flight_info(Request::new(FlightDescriptor::new_cmd(
                    CommandStatementQuery {
                        query: "SELECT 1 AS n".to_string(),
                        transaction_id: None,
                    }
                    .as_any()
                    .encode_to_vec(),
                )))
                .await?;
            // tonic merges a unary call's trailers into this metadata, so this
            // covers both: planning reports nothing on its own.
            assert!(
                no_server_timing(info.metadata()),
                "GetFlightInfo must not report its time separately"
            );
            let ticket = info
                .into_inner()
                .endpoint
                .first()
                .and_then(|endpoint| endpoint.ticket.clone())
                .ok_or_else(|| anyhow::anyhow!("GetFlightInfo returned no ticket"))?;

            let (rows, server_timing) = drain(client.do_get(ticket).await?).await?;
            let client_ms = started.elapsed().as_secs_f64() * 1000.0;
            assert_eq!(rows, 1, "the query still returns its row");
            let server_ms = parse_total(
                &server_timing.ok_or_else(|| anyhow::anyhow!("no `server-timing` trailer"))?,
            );
            assert!(
                server_ms > 0.0 && server_ms <= client_ms,
                "the server time {server_ms} ms must be positive and within the {client_ms} ms \
                 the client spent on GetFlightInfo and DoGet"
            );

            Ok(())
        })
        .await
}

/// A raw SQL ticket a client built itself has no `GetFlightInfo` behind it, and
/// its `DoGet` reports its own time.
#[tokio::test]
async fn a_raw_sql_do_get_reports_its_server_time() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (channel, _df) = start_spice_test_app(None, None, None).await?;
            let mut client = create_flight_client(channel, None)?;

            let started = Instant::now();
            let (rows, server_timing) =
                drain(client.do_get(Ticket::new("SELECT 1 AS n")).await?).await?;
            let client_ms = started.elapsed().as_secs_f64() * 1000.0;
            assert_eq!(rows, 1);
            let server_ms = parse_total(
                &server_timing.ok_or_else(|| anyhow::anyhow!("no `server-timing` trailer"))?,
            );
            assert!(
                server_ms > 0.0 && server_ms <= client_ms,
                "the server time {server_ms} ms must be positive and within the {client_ms} ms \
                 the client waited"
            );

            Ok(())
        })
        .await
}

/// A prepared statement is executed through the same `GetFlightInfo` and
/// `DoGet` pair, and reports the same way.
#[tokio::test]
async fn a_prepared_statement_reports_its_server_time_in_the_do_get_trailer()
-> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (channel, _df) = start_spice_test_app(None, None, None).await?;
            let mut client = FlightSqlServiceClient::new(channel);

            let mut prepared = client.prepare("SELECT 1 AS n".to_string(), None).await?;
            let info = prepared.execute().await?;
            let ticket = info
                .endpoint
                .first()
                .and_then(|endpoint| endpoint.ticket.clone())
                .ok_or_else(|| anyhow::anyhow!("the prepared statement returned no ticket"))?;

            let (rows, server_timing) = drain(client.do_get(ticket).await?).await?;
            assert_eq!(rows, 1);
            let server_ms = parse_total(
                &server_timing.ok_or_else(|| anyhow::anyhow!("no `server-timing` trailer"))?,
            );
            assert!(
                server_ms > 0.0,
                "the server time must be positive, got {server_ms}"
            );

            Ok(())
        })
        .await
}

/// A catalog listing is not a query, and its `DoGet` reports no server time.
#[tokio::test]
async fn a_metadata_do_get_reports_no_server_time() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (channel, _df) = start_spice_test_app(None, None, None).await?;
            let mut client = create_flight_client(channel, None)?;

            let info = client
                .get_flight_info(FlightDescriptor::new_cmd(
                    CommandGetCatalogs {}.as_any().encode_to_vec(),
                ))
                .await?;
            let ticket = info
                .endpoint
                .first()
                .and_then(|endpoint| endpoint.ticket.clone())
                .ok_or_else(|| anyhow::anyhow!("GetFlightInfo returned no ticket"))?;

            let stream = client.do_get(ticket).await?;
            let (_, server_timing) = drain(stream).await?;
            assert_eq!(server_timing, None, "a catalog listing is not a query");

            // The rows themselves are unaffected either way.
            let batches: Vec<RecordBatch> = client
                .do_get(Ticket::new("SELECT 1 AS n"))
                .await?
                .try_collect()
                .await?;
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);

            Ok(())
        })
        .await
}
