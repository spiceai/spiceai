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

//! Which session a Flight SQL request lands in, and who is allowed to reach it.
//!
//! The session is selected from a client-controlled header before
//! authentication runs, so the checks here are only meaningful against the wired
//! path: middleware, the auth layer and the query path have to agree, and a unit
//! test exercises none of that.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::TryStreamExt as _;
use runtime_auth::{FlightBasicAuth, api_key::ApiKeyAuth};
use spicepod::component::runtime::ApiKey;
use tonic::{Code, transport::Channel};

use crate::{flight::start_spice_test_app, init_tracing, utils::test_request_context};

/// Both keys are `:rw`; `PREPARE` is refused outright to a read-only principal,
/// which would make these assertions pass for the wrong reason.
fn two_key_auth() -> Arc<dyn FlightBasicAuth + Send + Sync> {
    Arc::new(ApiKeyAuth::new(vec![
        ApiKey::parse_str("a:rw"),
        ApiKey::parse_str("b:rw"),
    ])) as Arc<dyn FlightBasicAuth + Send + Sync>
}

/// A client presenting `key`, naming `session` when one is given.
fn client_for(
    channel: &Channel,
    key: &str,
    session: Option<&str>,
) -> FlightSqlServiceClient<Channel> {
    let mut client = FlightSqlServiceClient::new(channel.clone());
    client.set_header("authorization", format!("Bearer {key}"));
    if let Some(session) = session {
        client.set_header("x-session-id", session.to_string());
    }
    client
}

/// Runs `sql` to completion, returning the rows it produced.
async fn run(
    client: &mut FlightSqlServiceClient<Channel>,
    sql: &str,
) -> Result<Vec<RecordBatch>, tonic::Status> {
    let info = client
        .execute(sql.to_string(), None)
        .await
        .map_err(status_of)?;
    let ticket = info
        .endpoint
        .first()
        .and_then(|endpoint| endpoint.ticket.as_ref())
        .ok_or_else(|| tonic::Status::internal(format!("no ticket in the FlightInfo for `{sql}`")))?
        .clone();

    client
        .do_get(ticket)
        .await
        .map_err(status_of)?
        .try_collect()
        .await
        .map_err(status_of)
}

fn status_of(error: arrow_flight::error::FlightError) -> tonic::Status {
    match error {
        arrow_flight::error::FlightError::Tonic(status) => *status,
        other => tonic::Status::internal(other.to_string()),
    }
}

/// A handshake hands the client a session id; the statements it prepares there
/// are executable on later requests naming the same id.
#[tokio::test]
async fn a_handshake_session_carries_prepared_statements() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (channel, _df) = start_spice_test_app(Some(two_key_auth()), None, None).await?;

            let mut client = FlightSqlServiceClient::new(channel);
            let session = client.handshake("", "a").await?;
            assert!(!session.is_empty(), "the handshake returns a session id");

            run(&mut client, "PREPARE p AS SELECT 1 + 10 AS result").await?;
            let rows = run(&mut client, "EXECUTE p").await?;

            let total: usize = rows.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(total, 1, "EXECUTE runs what PREPARE put in the session");

            Ok(())
        })
        .await
}

/// Regression: a session id reaching another principal must not carry it into
/// the owner's prepared statements.
///
/// `EXECUTE` and `DEALLOCATE` are the cases that used to slip through — they are
/// `LogicalPlan::Statement` plans, which reached the session context by a path
/// that did not check ownership, so a second principal could read the owner's
/// statements and then destroy them.
#[tokio::test]
async fn a_session_cannot_be_used_by_another_principal() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (channel, _df) = start_spice_test_app(Some(two_key_auth()), None, None).await?;

            let mut owner = FlightSqlServiceClient::new(channel.clone());
            let session = owner.handshake("", "a").await?;
            let session = String::from_utf8(session.to_vec())?;
            run(&mut owner, "PREPARE victim AS SELECT 'a data' AS v").await?;

            for sql in ["EXECUTE victim", "DEALLOCATE victim", "SELECT 1"] {
                let mut intruder = client_for(&channel, "b", Some(&session));
                let status = run(&mut intruder, sql)
                    .await
                    .expect_err(&format!("`{sql}` from another principal must be refused"));
                assert_eq!(
                    status.code(),
                    Code::PermissionDenied,
                    "`{sql}`: {}",
                    status.message()
                );
            }

            let rows = run(&mut owner, "EXECUTE victim").await?;
            let total: usize = rows.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(total, 1, "the owner's statement survived every attempt");

            Ok(())
        })
        .await
}

/// Regression: session ids are issued, never accepted. Two principals that pick
/// the same `x-session-id` must not land in one context — which is what happened
/// when naming an unknown id created a session under it.
#[tokio::test]
async fn two_principals_naming_the_same_id_do_not_share_a_session() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            const GUESSABLE: &str = "shared-guessable-id";

            let (channel, _df) = start_spice_test_app(Some(two_key_auth()), None, None).await?;

            let mut first = client_for(&channel, "a", Some(GUESSABLE));
            let status = run(&mut first, "PREPARE squat AS SELECT 'a private' AS v")
                .await
                .expect_err("a client-chosen id names no session");
            assert_eq!(status.code(), Code::NotFound, "{}", status.message());

            let mut second = client_for(&channel, "b", Some(GUESSABLE));
            let status = run(&mut second, "EXECUTE squat")
                .await
                .expect_err("and creates none for the next caller to find");
            assert_eq!(status.code(), Code::NotFound, "{}", status.message());

            Ok(())
        })
        .await
}

/// A client that never handshakes and just presents its API key still gets a
/// session — `spice sql --api-key …` relies on it — but the session is keyed on
/// the principal, so a second key cannot reach the first one's statements.
#[tokio::test]
async fn a_client_that_skips_the_handshake_gets_a_session_of_its_own() -> Result<(), anyhow::Error>
{
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (channel, _df) = start_spice_test_app(Some(two_key_auth()), None, None).await?;

            let mut first = client_for(&channel, "a", None);
            run(&mut first, "PREPARE implicit AS SELECT 5 AS n").await?;

            // A separate connection with the same key reaches the same session:
            // the session follows the principal, not the connection.
            let mut same_key = client_for(&channel, "a", None);
            let rows = run(&mut same_key, "EXECUTE implicit").await?;
            let total: usize = rows.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(total, 1, "the principal comes back to its own session");

            let mut other_key = client_for(&channel, "b", None);
            let status = run(&mut other_key, "EXECUTE implicit")
                .await
                .expect_err("a different principal has its own session");
            assert!(
                status.message().contains("'implicit' does not exist"),
                "expected a missing statement, got: {}",
                status.message()
            );

            Ok(())
        })
        .await
}
