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

use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
    flight_service_server::FlightService,
    sql::{self, ProstMessageExt},
};
use prost::Message;
use tonic::{Request, Response, Status};

use crate::{
    datafusion::{
        query::{run_transaction, schema_statement, transaction_statements},
        request_context_extension::get_current_datafusion,
    },
    flight::{
        Service, is_auth_read_only,
        metrics::track_flight_request,
        record_batches_to_flight_stream, to_tonic_err, transaction_error_to_status,
        util::{attach_cache_metadata, set_flightsql_protocol},
    },
};
use runtime_request_context::{AsyncMarker, RequestContext};
use telemetry::timing::TimedStream;

/// Get a `FlightInfo` for executing a SQL query.
pub(crate) async fn get_flight_info(
    query: sql::CommandStatementQuery,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    tracing::trace!("get_flight_info: {query:?}");
    let _start = track_flight_request("get_flight_info", Some("statement_query")).await;
    set_flightsql_protocol().await;

    let sql = query.query.as_str();

    let context = RequestContext::current(AsyncMarker::new().await);
    let datafusion = get_current_datafusion(&context);

    // A `BEGIN … COMMIT` body advertises the schema of its FINAL statement (for
    // the canonical gate+write shape, the write's row-count schema). Plan just
    // that statement for its schema — the body is NOT executed here; any write
    // runs at `do_get`.
    let schema_sql = schema_statement(sql);

    let (arrow_schema, _) = Service::get_arrow_schema(datafusion, &schema_sql)
        .await
        .map_err(to_tonic_err)?;

    let fd = request.into_inner();

    let endpoint = FlightEndpoint::new().with_ticket(Ticket {
        ticket: query.as_any().encode_to_vec().into(),
    });

    let info = FlightInfo::new()
        .with_endpoint(endpoint)
        .try_with_schema(&arrow_schema)
        .map_err(to_tonic_err)?
        .with_descriptor(fd);

    Ok(Response::new(info))
}

pub(crate) async fn do_get(
    cmd: sql::CommandStatementQuery,
) -> Result<Response<<Service as FlightService>::DoGetStream>, Status> {
    let start = track_flight_request("do_get", Some("statement_query")).await;
    set_flightsql_protocol().await;

    let context = RequestContext::current(AsyncMarker::new().await);
    let datafusion = get_current_datafusion(&context);

    tracing::trace!("do_get_statement: {:?}", cmd.query);

    // A `BEGIN … COMMIT` body runs through the shared transaction orchestrator
    // (one atomic staged commit across every table it touches); stream the final
    // statement's result. A read-only principal running a write transaction is
    // rejected by the write statement's read-only check inside the orchestrator.
    if let Some(statements) = transaction_statements(&cmd.query) {
        let read_only = is_auth_read_only(&context);
        // Scope the orchestrator to this request's context so the
        // `CayenneTransaction` it installs is the exact one the write-path sink
        // reads back (mirrors `prepared_statement_query::do_get`). Without the
        // scope `RequestContext::current` can fall back to the internal context
        // and the write would publish immediately, breaking atomicity/staging.
        let context_clone = std::sync::Arc::clone(&context);
        let outcome = context_clone
            .scope(async { run_transaction(&datafusion, &statements, None, read_only).await })
            .await
            .map_err(transaction_error_to_status)?;
        let batches = outcome
            .result
            .map(|(batches, _)| batches)
            .unwrap_or_default();
        let stream = record_batches_to_flight_stream(batches);
        let timed = TimedStream::new(stream, move || start);
        return Ok(Response::new(
            Box::pin(timed) as <Service as FlightService>::DoGetStream
        ));
    }

    let pre_parsed_plan =
        super::super::check_read_only_sql(&context, &datafusion, &cmd.query, None).await?;
    let (output, from_cache) = Box::pin(Service::sql_to_flight_stream(
        datafusion,
        &cmd.query,
        None,
        pre_parsed_plan,
    ))
    .await?;
    let timed_output = TimedStream::new(output, move || start);

    let mut response =
        Response::new(Box::pin(timed_output) as <Service as FlightService>::DoGetStream);
    attach_cache_metadata(&mut response, from_cache, &context);
    Ok(response)
}
