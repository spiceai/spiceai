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

//! A query that fails because of its SQL or the values it computes must reach a Flight
//! client as a client error that carries the cause, not as `Internal`.
//!
//! Regression test for <https://github.com/spiceai/spiceai/issues/14922>.

use std::collections::HashMap;

use arrow::array::RecordBatch;
use arrow_flight::{FlightClient, Ticket, error::FlightError};
use axum::{Router, http::StatusCode};
use futures::TryStreamExt;
use spicepod::{component::dataset::Dataset, param::Params as DatasetParams};
use tokio::net::TcpListener;
use tonic::Code;

use crate::{
    flight::{create_flight_client, start_spice_test_app},
    init_tracing,
    utils::test_request_context,
};

/// Runs `sql` through `DoGet` and returns the status that ended it, whether the
/// failure arrived with the response or partway through its stream.
async fn failure_of(client: &mut FlightClient, sql: &str) -> tonic::Status {
    let outcome = match client.do_get(Ticket::new(sql.as_bytes().to_vec())).await {
        Ok(stream) => stream.try_collect::<Vec<RecordBatch>>().await.map(|_| ()),
        Err(err) => Err(err),
    };
    match outcome {
        Err(FlightError::Tonic(status)) => *status,
        Err(other) => panic!("{sql}: expected a gRPC status, got: {other:?}"),
        Ok(()) => panic!("{sql}: expected the query to fail, but it succeeded"),
    }
}

#[tokio::test]
async fn query_and_data_failures_are_invalid_argument() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (channel, _df) = start_spice_test_app(None, None, None).await?;
            let mut client = create_flight_client(channel, None)?;

            let cases = [
                ("SELECT 1/0", "Divide by zero error"),
                (
                    "SELECT CAST('abc' AS BIGINT)",
                    "Cast error: Cannot cast string 'abc' to value of Int64 type",
                ),
                // The cast fails below the hash repartition of the `GROUP BY`, which
                // hands the failure to every output partition wrapped in `Shared`.
                (
                    "SELECT x, count(*) FROM (SELECT CAST(s AS BIGINT) AS x \
                     FROM (VALUES ('1'), ('2'), ('abc'), ('4')) t(s)) GROUP BY x",
                    "Cast error: Cannot cast string 'abc' to value of Int64 type",
                ),
            ];

            for (sql, message) in cases {
                let status = failure_of(&mut client, sql).await;
                assert_eq!(
                    (status.code(), status.message()),
                    (Code::InvalidArgument, message),
                    "{sql}"
                );
            }

            Ok(())
        })
        .await
}

/// An HTTP origin's 404 fails the query by default (`on_error_response: error`). The
/// connector raises it as a planning error, and execution hands it on wrapped in
/// `Shared`, so this covers the connector, the execution wrapping and the Flight status
/// together.
#[tokio::test]
async fn an_http_origin_404_is_invalid_argument() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let addr = listener.local_addr()?;
            let origin = Router::new().fallback(|| async { (StatusCode::NOT_FOUND, "{}") });
            tokio::spawn(async move { axum::serve(listener, origin).await });

            let mut dataset = Dataset::new(format!("http://{addr}"), "origin");
            dataset.params = Some(DatasetParams::from_string_map(HashMap::from([
                ("file_format".to_string(), "json".to_string()),
                ("allowed_request_paths".to_string(), "/shows/**".to_string()),
                ("max_retries".to_string(), "0".to_string()),
            ])));

            let (channel, _df) = start_spice_test_app(None, None, Some(dataset)).await?;
            let mut client = create_flight_client(channel, None)?;

            let sql = "SELECT * FROM origin WHERE request_path = '/shows/404'";
            let status = failure_of(&mut client, sql).await;
            let expected = format!(
                "Failed to fetch http://{addr} for dataset 'origin': the origin answered 404, \
                 so the request failed rather than becoming data."
            );
            assert_eq!(status.code(), Code::InvalidArgument, "{sql}: {status:?}");
            assert!(
                status.message().starts_with(&expected),
                "{sql}: expected the message to start with {expected:?}, got {:?}",
                status.message()
            );

            Ok(())
        })
        .await
}
