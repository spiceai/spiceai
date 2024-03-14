use arrow_flight::{
    sql::{Any, Command},
    FlightDescriptor, FlightInfo,
};
use prost::Message;
use tonic::{Request, Response, Status};

use super::{flightsql, to_tonic_err, Service};

pub(crate) async fn handle(
    flight_svc: &Service,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    let message = Any::decode(&*request.get_ref().cmd).map_err(to_tonic_err)?;

    match Command::try_from(message).map_err(to_tonic_err)? {
        Command::CommandStatementQuery(token) => {
            flightsql::statement_query::get_flight_info(flight_svc, token, request).await
        }
        Command::CommandPreparedStatementQuery(handle) => {
            flightsql::prepared_statement_query::get_flight_info(flight_svc, handle, request).await
        }
        Command::CommandGetCatalogs(token) => {
            Ok(flightsql::get_catalogs::get_flight_info(&token, request))
        }
        Command::CommandGetDbSchemas(token) => {
            Ok(flightsql::get_schemas::get_flight_info(&token, request))
        }
        Command::CommandGetTables(token) => {
            Ok(flightsql::get_tables::get_flight_info(&token, request))
        }
        Command::CommandGetSqlInfo(token) => {
            flightsql::get_sql_info::get_flight_info(&token, request)
        }
        _ => Err(Status::unimplemented("Not yet implemented")),
    }
}
