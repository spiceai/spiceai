use std::sync::Arc;

use arrow::{
    array::{RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
};
use arrow_flight::{
    flight_service_server::FlightService, sql, FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
};
use datafusion::datasource::TableType;
use tonic::{Request, Response, Status};

use crate::{
    flight::{flightsql::get_tables, record_batches_to_flight_stream, to_tonic_err, Service},
    timing::{TimeMeasurement, TimedStream},
};

pub(crate) fn get_flight_info(
    query: &sql::CommandGetTableTypes,
    request: Request<FlightDescriptor>,
) -> Response<FlightInfo> {
    let fd = request.into_inner();
    tracing::trace!("get_flight_info: {query:?}");
    Response::new(FlightInfo {
        flight_descriptor: Some(fd.clone()),
        endpoint: vec![FlightEndpoint {
            ticket: Some(Ticket { ticket: fd.cmd }),
            ..Default::default()
        }],
        ..Default::default()
    })
}

pub(crate) fn do_get(
    query: &sql::CommandGetTableTypes,
) -> Result<Response<<Service as FlightService>::DoGetStream>, Status> {
    let start = TimeMeasurement::new("flight_do_get_table_types_duration_ms", vec![]);
    tracing::trace!("do_get_table_types: {query:?}");

    let schema = Schema::new(vec![Field::new("table_type", DataType::Utf8, false)]);
    let table_type = vec![
        get_tables::table_type_name(TableType::Base),
        get_tables::table_type_name(TableType::View),
    ];
    let record_batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(StringArray::from(table_type))],
    )
    .map_err(to_tonic_err)?;

    Ok(Response::new(Box::pin(TimedStream::new(
        record_batches_to_flight_stream(vec![record_batch]),
        move || start,
    ))
        as <Service as FlightService>::DoGetStream))
}
