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
            flight_svc.do_get_statement(command, request).await
        }
        Command::CommandPreparedStatementQuery(command) => {
            flight_svc.do_get_prepared_statement(command, request).await
        }
        Command::CommandGetCatalogs(command) => flight_svc.do_get_catalogs(command, request).await,
        Command::CommandGetDbSchemas(command) => flight_svc.do_get_schemas(command, request).await,
        Command::CommandGetTables(command) => flight_svc.do_get_tables(command, request).await,
        Command::CommandGetSqlInfo(command) => flightsql::get_sql_info::do_get(command, request),
        _ => Err(Status::unimplemented("Not yet implemented")),
    }
}
