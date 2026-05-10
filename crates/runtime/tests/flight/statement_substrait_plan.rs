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

//! Integration tests for FlightSQL `CommandStatementSubstraitPlan`.

use arrow::array::RecordBatch;
use arrow_flight::{
    FlightDescriptor, Ticket,
    error::FlightError,
    sql::{CommandStatementSubstraitPlan, ProstMessageExt, SubstraitPlan},
};
use bytes::Bytes;
use datafusion::prelude::SessionContext;
use datafusion_substrait::logical_plan::producer::to_substrait_plan;
use futures::TryStreamExt;
use prost::Message;
use tonic::Code;

/// Extracts the underlying [`tonic::Status`] from a [`FlightError`], or panics
/// with a descriptive message if the error did not originate from tonic.
fn status_from(err: FlightError) -> tonic::Status {
    match err {
        FlightError::Tonic(status) => *status,
        other => panic!("expected tonic status, got: {other:?}"),
    }
}

use crate::{
    flight::{create_flight_client, start_spice_test_app},
    init_tracing,
    utils::test_request_context,
};

/// Build a Substrait `Plan` from `sql` using a standalone DataFusion
/// `SessionContext` and return it encoded as protobuf bytes. The server
/// rebuilds the `LogicalPlan` from these bytes, so the producer's context
/// does not need to match the server's catalog as long as the SQL is
/// self-contained (e.g. `SELECT 1 AS x`).
async fn substrait_plan_bytes(sql: &str) -> Result<Bytes, anyhow::Error> {
    let ctx = SessionContext::new();
    let df = ctx.sql(sql).await?;
    let plan = df.into_optimized_plan()?;
    let substrait = to_substrait_plan(&plan, &ctx.state())?;
    let mut buf = Vec::with_capacity(substrait.encoded_len());
    substrait.encode(&mut buf)?;
    Ok(Bytes::from(buf))
}

fn substrait_command(plan_bytes: Bytes) -> CommandStatementSubstraitPlan {
    CommandStatementSubstraitPlan {
        plan: Some(SubstraitPlan {
            plan: plan_bytes,
            version: "0.62.0".to_string(),
        }),
        transaction_id: None,
    }
}

#[tokio::test]
async fn test_substrait_plan_round_trip() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (channel, _df) = start_spice_test_app(None, None, None).await?;
            let mut client = create_flight_client(channel, None)?;

            let plan_bytes = substrait_plan_bytes("SELECT 1 AS x, 'spice' AS name").await?;
            let cmd = substrait_command(plan_bytes);

            // GetFlightInfo: server decodes the plan, returns a schema-bearing
            // FlightInfo with a Substrait-typed ticket.
            let descriptor = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
            let info = client.get_flight_info(descriptor).await?;
            assert!(!info.endpoint.is_empty(), "expected at least one endpoint");
            assert!(
                !info.schema.is_empty(),
                "expected a non-empty schema in FlightInfo"
            );

            // DoGet against the returned ticket should stream the result rows.
            let ticket = info.endpoint[0]
                .ticket
                .clone()
                .expect("endpoint must carry a ticket");
            let stream = client.do_get(ticket).await?;
            let data: Vec<RecordBatch> = stream.try_collect().await?;
            let result_str = arrow::util::pretty::pretty_format_batches(&data)?.to_string();

            insta::assert_snapshot!("substrait_round_trip", result_str);

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_substrait_plan_missing_plan_rejected() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (channel, _df) = start_spice_test_app(None, None, None).await?;
            let mut client = create_flight_client(channel, None)?;

            let cmd = CommandStatementSubstraitPlan {
                plan: None,
                transaction_id: None,
            };
            let descriptor = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());

            let err = client
                .get_flight_info(descriptor)
                .await
                .expect_err("missing plan should be rejected");
            let status = status_from(err);
            assert_eq!(status.code(), Code::InvalidArgument);
            assert!(
                status.message().contains("plan is required"),
                "unexpected message: {status}"
            );
            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_substrait_plan_invalid_bytes_rejected() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (channel, _df) = start_spice_test_app(None, None, None).await?;
            let mut client = create_flight_client(channel, None)?;

            // Non-empty bytes that aren't a valid `substrait.proto.Plan` encoding.
            let cmd = substrait_command(Bytes::from_static(&[0xff, 0xff, 0xff, 0xff]));
            let descriptor = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());

            let err = client
                .get_flight_info(descriptor)
                .await
                .expect_err("invalid bytes should be rejected");
            assert_eq!(status_from(err).code(), Code::InvalidArgument);
            Ok(())
        })
        .await
}

/// Round-tripping the same plan bytes twice should hit the same cache key on
/// the server side. We verify the second `do_get` returns the same result,
/// which exercises the cache key derivation end-to-end.
#[tokio::test]
async fn test_substrait_plan_repeated_execution_is_stable() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (channel, _df) = start_spice_test_app(None, None, None).await?;
            let mut client = create_flight_client(channel, None)?;

            let plan_bytes = substrait_plan_bytes("SELECT 42 AS answer").await?;
            let cmd = substrait_command(plan_bytes);

            let ticket_bytes = Bytes::from(cmd.as_any().encode_to_vec());

            let first: Vec<RecordBatch> = client
                .do_get(Ticket {
                    ticket: ticket_bytes.clone(),
                })
                .await?
                .try_collect()
                .await?;
            let second: Vec<RecordBatch> = client
                .do_get(Ticket {
                    ticket: ticket_bytes,
                })
                .await?
                .try_collect()
                .await?;

            assert_eq!(
                arrow::util::pretty::pretty_format_batches(&first)?.to_string(),
                arrow::util::pretty::pretty_format_batches(&second)?.to_string(),
            );

            Ok(())
        })
        .await
}
