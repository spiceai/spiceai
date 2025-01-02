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
    flight::{
        flightsql::get_tables, metrics, record_batches_to_flight_stream, to_tonic_err,
        util::set_flightsql_protocol, Service,
    },
    timing::TimedStream,
};

pub(crate) async fn get_flight_info(
    query: sql::CommandGetTableTypes,
    request: Request<FlightDescriptor>,
) -> Response<FlightInfo> {
    let _start = metrics::track_flight_request("get_flight_info", Some("get_table_types")).await;
    set_flightsql_protocol().await;

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

pub(crate) async fn do_get(
    query: sql::CommandGetTableTypes,
) -> Result<Response<<Service as FlightService>::DoGetStream>, Status> {
    let start = metrics::track_flight_request("do_get", Some("get_table_types")).await;
    set_flightsql_protocol().await;

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
