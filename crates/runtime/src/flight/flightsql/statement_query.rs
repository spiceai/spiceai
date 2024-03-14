use std::sync::Arc;

use arrow_flight::{
    flight_service_server::FlightService,
    sql::{self, ProstMessageExt},
    FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
};
use prost::Message;
use tonic::{Request, Response, Status};

use crate::flight::{to_tonic_err, Service};

/// Get a `FlightInfo` for executing a SQL query.
pub(crate) async fn get_flight_info(
    flight_svc: &Service,
    query: sql::CommandStatementQuery,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    tracing::trace!("get_flight_info: {query:?}");

    let sql = query.query.as_str();

    let (arrow_schema, num_rows) =
        Service::get_arrow_schema_and_size_sql(Arc::clone(&flight_svc.datafusion), sql.to_string())
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
        .with_descriptor(fd)
        .with_total_records(num_rows.try_into().map_err(to_tonic_err)?);

    Ok(Response::new(info))
}

pub(crate) async fn do_get(
    flight_svc: &Service,
    ticket: sql::TicketStatementQuery,
) -> Result<Response<<Service as FlightService>::DoGetStream>, Status> {
    let datafusion = Arc::clone(&flight_svc.datafusion);
    tracing::trace!("do_get_statement: {ticket:?}");
    match std::str::from_utf8(&ticket.statement_handle) {
        Ok(sql) => {
            let output = Service::sql_to_flight_stream(datafusion, sql.to_owned()).await?;
            Ok(Response::new(
                Box::pin(output) as <Service as FlightService>::DoGetStream
            ))
        }
        Err(e) => Err(Status::invalid_argument(format!("Invalid ticket: {e:?}"))),
    }
}
