use crate::flight::flightsql::statement_query;
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
            statement_query::get_flight_info(flight_svc, token, request).await
        }
        Command::CommandPreparedStatementQuery(handle) => {
            flight_svc
                .get_flight_info_prepared_statement(handle, request)
                .await
        }
        Command::CommandGetCatalogs(token) => {
            flight_svc.get_flight_info_catalogs(token, request).await
        }
        Command::CommandGetDbSchemas(token) => {
            flight_svc.get_flight_info_schemas(token, request).await
        }
        Command::CommandGetTables(token) => flight_svc.get_flight_info_tables(token, request).await,
        Command::CommandGetSqlInfo(token) => {
            flightsql::get_sql_info::get_flight_info(token, request)
        }
        _ => Err(Status::unimplemented("Not yet implemented")),
    }
}
