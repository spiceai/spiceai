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
    array::RecordBatch,
    datatypes::{DataType, Field, Schema},
};
use arrow_flight::{
    flight_service_server::FlightService, sql, FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
};
use tonic::{Request, Response, Status};

use crate::{
    flight::{metrics, record_batches_to_flight_stream, util::set_flightsql_protocol, Service},
    timing::TimedStream,
};

pub(crate) async fn get_flight_info(
    query: &sql::CommandGetPrimaryKeys,
    request: Request<FlightDescriptor>,
) -> Response<FlightInfo> {
    let _start = metrics::track_flight_request("get_flight_info", Some("get_primary_keys")).await;
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

/// <https://arrow.apache.org/docs/format/FlightSql.html#rpc-methods>
/// The returned Arrow schema is:
///   `catalog_name`: utf8,
///   `db_schema_name`: utf8,
///   `table_name`: utf8 not null,
///   `column_name`: utf8 not null,
///   `key_name`: utf8,
///   `key_sequence`: int32 not null
#[allow(clippy::unnecessary_wraps)]
pub(crate) async fn do_get(
    _flight_svc: &Service,
    query: &sql::CommandGetPrimaryKeys,
) -> Result<Response<<Service as FlightService>::DoGetStream>, Status> {
    let start = metrics::track_flight_request("do_get", Some("get_primary_keys")).await;
    set_flightsql_protocol().await;
    tracing::trace!("do_get_get_primary_keys: {query:?}");

    let schema = Arc::new(Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, true),
        Field::new("db_schema_name", DataType::Utf8, true),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("column_name", DataType::Utf8, false),
        Field::new("key_name", DataType::Utf8, true),
        Field::new("key_sequence", DataType::Int32, false),
    ]));

    let record_batch = RecordBatch::new_empty(Arc::clone(&schema));

    Ok(Response::new(Box::pin(TimedStream::new(
        record_batches_to_flight_stream(vec![record_batch]),
        move || start,
    ))
        as <Service as FlightService>::DoGetStream))
}
