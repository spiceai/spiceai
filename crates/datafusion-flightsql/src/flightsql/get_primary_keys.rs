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
    array::RecordBatch,
    datatypes::{DataType, Field, Schema},
};
use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, Ticket, flight_service_server::FlightService, sql,
};
use tonic::{Request, Response};

use crate::{FlightSqlService, record_batches_to_flight_stream};

pub(crate) fn get_flight_info(
    _query: &sql::CommandGetPrimaryKeys,
    request: Request<FlightDescriptor>,
) -> Response<FlightInfo> {
    let fd = request.into_inner();
    Response::new(FlightInfo {
        flight_descriptor: Some(fd.clone()),
        endpoint: vec![FlightEndpoint {
            ticket: Some(Ticket { ticket: fd.cmd }),
            ..Default::default()
        }],
        ..Default::default()
    })
}

/// Returns an empty primary keys result (`DataFusion` has no primary key concept).
pub(crate) fn do_get(
    _query: &sql::CommandGetPrimaryKeys,
) -> Response<<FlightSqlService as FlightService>::DoGetStream> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, true),
        Field::new("db_schema_name", DataType::Utf8, true),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("column_name", DataType::Utf8, false),
        Field::new("key_name", DataType::Utf8, true),
        Field::new("key_sequence", DataType::Int32, false),
    ]));
    let batch = RecordBatch::new_empty(schema);
    Response::new(Box::pin(record_batches_to_flight_stream(vec![batch])))
}
