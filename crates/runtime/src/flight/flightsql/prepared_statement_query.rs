use std::sync::Arc;

use arrow_flight::{
    flight_service_server::FlightService,
    sql::{self, ProstMessageExt},
    FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
};
use prost::Message;
use tonic::{Request, Response, Status};

use crate::{
    flight::{to_tonic_err, Service},
    timing::{TimeMeasurement, TimedStream},
};

/// Create a prepared statement from given SQL statement.
pub(crate) async fn do_action_create_prepared_statement(
    flight_svc: &Service,
    statement: sql::ActionCreatePreparedStatementRequest,
) -> Result<sql::ActionCreatePreparedStatementResult, Status> {
    tracing::trace!("do_action_create_prepared_statement: {statement:?}");
    let (arrow_schema, _) = Service::get_arrow_schema_and_size_sql(
        Arc::clone(&flight_svc.datafusion),
        statement.query.clone(),
    )
    .await
    .map_err(to_tonic_err)?;

    let schema_bytes = Service::serialize_schema(&arrow_schema)?;

    Ok(sql::ActionCreatePreparedStatementResult {
        prepared_statement_handle: statement.query.into(),
        dataset_schema: schema_bytes,
        ..Default::default()
    })
}

pub(crate) async fn get_flight_info(
    flight_svc: &Service,
    handle: sql::CommandPreparedStatementQuery,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    tracing::trace!("get_flight_info: {handle:?}");

    let sql = match std::str::from_utf8(&handle.prepared_statement_handle) {
        Ok(sql) => sql.to_string(),
        Err(e) => {
            return Err(Status::invalid_argument(format!(
                "Invalid prepared statement handle: {e}"
            )))
        }
    };

    let (arrow_schema, num_rows) =
        Service::get_arrow_schema_and_size_sql(Arc::clone(&flight_svc.datafusion), sql)
            .await
            .map_err(to_tonic_err)?;

    tracing::trace!(
        "get_flight_info_prepared_statement: arrow_schema={arrow_schema:?} num_rows={num_rows:?}"
    );

    let fd = request.into_inner();

    let endpoint = FlightEndpoint::new().with_ticket(Ticket {
        ticket: handle.as_any().encode_to_vec().into(),
    });

    let info = FlightInfo::new()
        .with_endpoint(endpoint)
        .try_with_schema(&arrow_schema)
        .map_err(to_tonic_err)?
        .with_descriptor(fd)
        .with_total_records(num_rows.try_into().map_err(to_tonic_err)?);

    Ok(Response::new(info))
}

pub(crate) async fn do_get(
    flight_svc: &Service,
    query: sql::CommandPreparedStatementQuery,
) -> Result<Response<<Service as FlightService>::DoGetStream>, Status> {
    let datafusion = Arc::clone(&flight_svc.datafusion);
    tracing::trace!("do_get: {query:?}");
    match std::str::from_utf8(&query.prepared_statement_handle) {
        Ok(sql) => {
            let start =
                TimeMeasurement::new("flight_do_get_prepared_statement_query_duration_ms", vec![]);
            let output = Service::sql_to_flight_stream(datafusion, sql.to_owned()).await?;
            let timed_output = TimedStream::new(output, move || start);
            Ok(Response::new(
                Box::pin(timed_output) as <Service as FlightService>::DoGetStream
            ))
        }
        Err(e) => Err(Status::invalid_argument(format!(
            "Invalid prepared statement handle: {e}"
        ))),
    }
}
