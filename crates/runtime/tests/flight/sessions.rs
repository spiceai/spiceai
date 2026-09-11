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

/// Regression: the session id is client-chosen, so a second principal naming
/// the same one must not land in the first's session and its prepared
/// statements.
#[tokio::test]
async fn two_principals_naming_the_same_id_do_not_share_a_session() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (channel, _df) = start_spice_test_app(Some(two_key_auth()), None, None).await?;
            const GUESSABLE: &str = "shared-guessable-id";

            let mut first = client_for(&channel, "a", Some(GUESSABLE));
            run(&mut first, "PREPARE squat AS SELECT 'a private' AS v").await?;

            let mut second = client_for(&channel, "b", Some(GUESSABLE));
            let status = run(&mut second, "EXECUTE squat")
                .await
                .expect_err("the second principal must be refused");
            assert_eq!(
                status.code(),
                Code::PermissionDenied,
                "{}",
                status.message()
            );

            let rows = run(&mut first, "EXECUTE squat").await?;
            let total: usize = rows.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(total, 1, "and the owner still has it");

            Ok(())
        })
        .await
}

/// A client that neither handshakes nor sends back the id it was given is
/// given a fresh session every request, so nothing carries between them.
///
/// This is the contract, not a defect: the runtime cannot make a client
/// remember anything. A handshake hands the id over as the bearer token, and
/// `spice sql` keeps the `session-id` cookie; a client doing neither has no way
/// to say which session it means.
#[tokio::test]
async fn a_client_that_never_names_a_session_gets_a_fresh_one() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (channel, _df) = start_spice_test_app(Some(two_key_auth()), None, None).await?;

            let mut client = client_for(&channel, "a", None);
            run(&mut client, "PREPARE adrift AS SELECT 5 AS n").await?;

            let status = run(&mut client, "EXECUTE adrift")
                .await
                .expect_err("the next request is in a session of its own");
            assert!(
                status.message().contains("'adrift' does not exist"),
                "expected a missing statement, got: {}",
                status.message()
            );

            Ok(())
        })
        .await
}

/// Naming the same id on both requests is all it takes, which is what the
/// handshake and the cookie each arrange on the client's behalf.
#[tokio::test]
async fn naming_the_same_id_carries_prepared_statements() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (channel, _df) = start_spice_test_app(Some(two_key_auth()), None, None).await?;

            let mut first = client_for(&channel, "a", Some("pinned"));
            run(&mut first, "PREPARE kept AS SELECT 5 AS n").await?;

            let mut second = client_for(&channel, "a", Some("pinned"));
            let rows = run(&mut second, "EXECUTE kept").await?;

            let total: usize = rows.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(total, 1, "a separate connection naming the id reaches it");

            Ok(())
        })
        .await
}
