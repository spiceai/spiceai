use arrow_flight::{
    flight_service_server::FlightService,
    sql::{Any, Command},
    Ticket,
};
use prost::Message;
use tonic::{Request, Response, Status};

use super::{flightsql, to_tonic_err, Service};

pub(crate) async fn handle(
    flight_svc: &Service,
    request: Request<Ticket>,
) -> Result<Response<<Service as FlightService>::DoGetStream>, Status> {
    let msg: Any = Message::decode(&*request.get_ref().ticket).map_err(to_tonic_err)?;

    match Command::try_from(msg).map_err(to_tonic_err)? {
        Command::TicketStatementQuery(command) => {
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
