use arrow_flight::{
    flight_service_server::FlightService,
    sql::{self, ProstMessageExt},
    FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
};
use prost::Message;
use tonic::{Request, Response, Status};

use crate::{
    flight::{record_batches_to_flight_stream, to_tonic_err, Service},
    timing::{TimeMeasurement, TimedStream},
};

/// Get a `FlightInfo` for listing schemas.
pub(crate) fn get_flight_info(
    query: &sql::CommandGetDbSchemas,
    request: Request<FlightDescriptor>,
) -> Response<FlightInfo> {
    tracing::trace!("get_flight_info");
    let fd = request.into_inner();

    let endpoint = FlightEndpoint::new().with_ticket(Ticket {
        ticket: query.as_any().encode_to_vec().into(),
    });

    let info = FlightInfo::new()
        .with_endpoint(endpoint)
        .with_descriptor(fd);

    Response::new(info)
}

pub(crate) async fn do_get(
    flight_svc: &Service,
    query: sql::CommandGetDbSchemas,
) -> Result<Response<<Service as FlightService>::DoGetStream>, Status> {
    let start = TimeMeasurement::new("flight_do_get_get_schemas_duration_ms", vec![]);
    let catalog = &query.catalog;
    tracing::trace!("do_get: {query:?}");
    let filtered_catalogs = match catalog {
        Some(catalog) => vec![catalog.to_string()],
        None => flight_svc.datafusion.read().await.ctx.catalog_names(),
    };
    let mut builder = query.into_builder();

    for catalog in filtered_catalogs {
        let catalog_provider = flight_svc
            .datafusion
            .read()
            .await
            .ctx
            .catalog(&catalog)
            .ok_or_else(|| {
                Status::internal(format!("unable to get catalog provider for {catalog}"))
            })?;
        for schema in catalog_provider.schema_names() {
            builder.append(&catalog, schema);
        }
    }

    let record_batch = builder.build().map_err(to_tonic_err)?;

    Ok(Response::new(Box::pin(TimedStream::new(
        record_batches_to_flight_stream(vec![record_batch]),
        move || start,
    ))
        as <Service as FlightService>::DoGetStream))
}
