use std::sync::Arc;

use arrow_flight::{
    flight_service_server::FlightService,
    sql::{Any, Command},
    Ticket,
};
use prost::Message;
use tonic::{Request, Response, Status};

use crate::timing::{TimeMeasurement, TimedStream};

use super::{flightsql, to_tonic_err, Service};

pub(crate) async fn handle(
    flight_svc: &Service,
    request: Request<Ticket>,
) -> Result<Response<<Service as FlightService>::DoGetStream>, Status> {
    let msg: Any = match Message::decode(&*request.get_ref().ticket) {
        Ok(msg) => msg,
        Err(_) => return do_get_simple(flight_svc, request).await,
    };

    match Command::try_from(msg).map_err(to_tonic_err)? {
        Command::CommandStatementQuery(command) => {
            flightsql::statement_query::do_get(flight_svc, command).await
        }
        Command::CommandPreparedStatementQuery(command) => {
            flightsql::prepared_statement_query::do_get(flight_svc, command).await
        }
        Command::CommandGetCatalogs(command) => {
            flightsql::get_catalogs::do_get(flight_svc, command).await
        }
        Command::CommandGetDbSchemas(command) => {
            flightsql::get_schemas::do_get(flight_svc, command).await
        }
        Command::CommandGetTables(command) => {
            flightsql::get_tables::do_get(flight_svc, command).await
        }
        Command::CommandGetSqlInfo(command) => flightsql::get_sql_info::do_get(command),
        _ => Err(Status::unimplemented("Not yet implemented")),
    }
}

async fn do_get_simple(
    flight_svc: &Service,
    request: Request<Ticket>,
) -> Result<Response<<Service as FlightService>::DoGetStream>, Status> {
    let datafusion = Arc::clone(&flight_svc.datafusion);
    let ticket = request.into_inner();
    tracing::trace!("do_get_simple: {ticket:?}");
    match std::str::from_utf8(&ticket.ticket) {
        Ok(sql) => {
            let start = TimeMeasurement::new("flight_do_get_simple_duration_ms", vec![]);
            let output = Service::sql_to_flight_stream(datafusion, sql.to_owned()).await?;

            let timed_output = TimedStream::new(output, move || start);

            Ok(Response::new(
                Box::pin(timed_output) as <Service as FlightService>::DoGetStream
            ))
        }
        Err(e) => Err(Status::invalid_argument(format!("Invalid ticket: {e:?}"))),
    }
}
