/*
Copyright 2026 The Spice.ai OSS Authors

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
    FlightDescriptor, FlightInfo, Ticket, flight_service_server::FlightService, sql,
};
use datafusion::datasource::TableType;
use tonic::{Request, Response, Status};

use crate::{FlightSqlService, record_batches_to_flight_stream, to_tonic_err};

use super::get_tables::table_type_name;

pub(crate) fn get_flight_info(
    _query: sql::CommandGetTableTypes,
    request: Request<FlightDescriptor>,
) -> Response<FlightInfo> {
    let fd = request.into_inner();
    Response::new(FlightInfo {
        flight_descriptor: Some(fd.clone()),
        endpoint: vec![arrow_flight::FlightEndpoint {
            ticket: Some(Ticket { ticket: fd.cmd }),
            ..Default::default()
        }],
        ..Default::default()
    })
}

pub(crate) fn do_get(
    _query: sql::CommandGetTableTypes,
) -> Result<Response<<FlightSqlService as FlightService>::DoGetStream>, Status> {
    let schema = Schema::new(vec![Field::new("table_type", DataType::Utf8, false)]);
    let types = vec![
        table_type_name(TableType::Base),
        table_type_name(TableType::View),
    ];
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(StringArray::from(types))])
        .map_err(to_tonic_err)?;

    Ok(Response::new(Box::pin(record_batches_to_flight_stream(
        vec![batch],
    ))))
}
