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

use arrow::{array::RecordBatch, compute::concat_batches};
use arrow_flight::{
    FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
    decode::{DecodedPayload, FlightDataDecoder},
    flight_service_server::FlightService,
    sql::{self, CommandPreparedStatementQuery, ProstMessageExt},
};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::{ArrowError, SchemaRef};
use bytes::Bytes;
use datafusion::{
    common::ParamValues, error::DataFusionError, parquet::data_type::AsBytes, scalar::ScalarValue,
};
use prost::Message;
use tokio_stream::{StreamExt, adapters::Peekable, empty};
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;

use crate::{
    flight::{
        Service, metrics, to_tonic_err,
        util::{attach_cache_metadata, set_flightsql_protocol},
    },
    timing::TimedStream,
};

pub(crate) struct PreparedStatement {
    query: String,
    parameters: Option<ParamValues>,
}

/// Create a prepared statement from given SQL statement.
pub(crate) async fn do_action_create_prepared_statement(
    flight_svc: &Service,
    statement: sql::ActionCreatePreparedStatementRequest,
) -> Result<sql::ActionCreatePreparedStatementResult, Status> {
    tracing::trace!("do_action_create_prepared_statement: {statement:?}");
    set_flightsql_protocol().await;
    let arrow_schema =
        Service::get_arrow_schema(Arc::clone(&flight_svc.datafusion), &statement.query)
            .await
            .map_err(to_tonic_err)?;

    let schema_bytes = Service::serialize_schema(&arrow_schema)?;

    let stmt = PreparedStatement {
        query: statement.query.clone(),
        parameters: None,
    };

    flight_svc
        .prepared_statements
        .write()
        .await
        .insert(statement.query.clone(), stmt);

    Ok(sql::ActionCreatePreparedStatementResult {
        prepared_statement_handle: statement.query.into(),
        dataset_schema: schema_bytes,
        ..Default::default()
    })
}

pub(crate) async fn get_flight_info(
    flight_svc: &Service,
    handle: sql::CommandPreparedStatementQuery,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    let _start =
        metrics::track_flight_request("get_flight_info", Some("prepared_statement_query")).await;
    set_flightsql_protocol().await;

    tracing::trace!("get_flight_info: {handle:?}");
    let sql = match std::str::from_utf8(&handle.prepared_statement_handle) {
        Ok(sql) => sql,
        Err(e) => {
            return Err(Status::invalid_argument(format!(
                "Invalid prepared statement handle: {e}"
            )));
        }
    };

    let arrow_schema = Service::get_arrow_schema(Arc::clone(&flight_svc.datafusion), sql)
        .await
        .map_err(to_tonic_err)?;

    tracing::trace!("get_flight_info_prepared_statement: arrow_schema={arrow_schema:?}");

    let fd = request.into_inner();

    let endpoint = FlightEndpoint::new().with_ticket(Ticket {
        ticket: handle.as_any().encode_to_vec().into(),
    });

    let info = FlightInfo::new()
        .with_endpoint(endpoint)
        .try_with_schema(&arrow_schema)
        .map_err(to_tonic_err)?
        .with_descriptor(fd);

    Ok(Response::new(info))
}

pub(crate) async fn do_get(
    flight_svc: &Service,
    query: sql::CommandPreparedStatementQuery,
) -> Result<Response<<Service as FlightService>::DoGetStream>, Status> {
    let start = metrics::track_flight_request("do_get", Some("prepared_statement_query")).await;
    set_flightsql_protocol().await;

    let datafusion = Arc::clone(&flight_svc.datafusion);
    tracing::trace!("do_get: {query:?}");

    let handle =
        String::from_utf8(query.prepared_statement_handle.to_vec()).map_err(error_to_status)?;

    match &flight_svc.prepared_statements.read().await.get(&handle) {
        Some(PreparedStatement { query, parameters }) => {
            let (output, from_cache) = Box::pin(Service::sql_to_flight_stream(
                datafusion,
                query,
                parameters.clone(),
            ))
            .await?;
            let timed_output = TimedStream::new(output, move || start);

            let mut response =
                Response::new(Box::pin(timed_output) as <Service as FlightService>::DoGetStream);
            attach_cache_metadata(&mut response, from_cache);
            Ok(response)
        }
        None => todo!(),
    }

    // match std::str::from_utf8(&query.prepared_statement_handle) {
    //     Ok(sql) => {
    //         let (output, from_cache) =
    //             Box::pin(Service::sql_to_flight_stream(datafusion, sql, None)).await?;
    //         let timed_output = TimedStream::new(output, move || start);

    //         let mut response =
    //             Response::new(Box::pin(timed_output) as <Service as FlightService>::DoGetStream);
    //         attach_cache_metadata(&mut response, from_cache);
    //         Ok(response)
    //     }
    //     Err(e) => Err(Status::invalid_argument(format!(
    //         "Invalid prepared statement handle: {e}"
    //     ))),
    // }
}

pub(crate) async fn do_put_query(
    flight_svc: &Service,
    query: CommandPreparedStatementQuery,
    streaming_flight: Peekable<Streaming<FlightData>>,
) -> Result<Response<<Service as FlightService>::DoPutStream>, Status> {
    let streaming_flight = streaming_flight
        .map(|flight_data| flight_data.map_err(|e| arrow_flight::error::FlightError::Tonic(e)));

    let mut decoder = FlightDataDecoder::new(streaming_flight);
    let schema = decode_schema(&mut decoder).await?;

    let mut parameters = Vec::new();
    let mut encoder =
        StreamWriter::try_new(&mut parameters, &schema).map_err(arrow_error_to_status)?;
    let mut total_rows = 0;
    while let Some(msg) = decoder.try_next().await? {
        match msg.payload {
            DecodedPayload::None => {}
            DecodedPayload::Schema(_) => {
                return Err(Status::invalid_argument(
                    "parameter flight data must contain a single schema",
                ));
            }
            DecodedPayload::RecordBatch(record_batch) => {
                total_rows += record_batch.num_rows();
                encoder
                    .write(&record_batch)
                    .map_err(arrow_error_to_status)?;
            }
        }
    }
    if total_rows > 1 {
        return Err(Status::invalid_argument(
            "parameters should contain a single row",
        ));
    }

    let parameters = if parameters.is_empty() {
        None
    } else {
        Some(parameters.as_bytes())
    };

    let parameters = decode_param_values(parameters).map_err(arrow_error_to_status)?;

    let handle =
        String::from_utf8(query.prepared_statement_handle.to_vec()).map_err(error_to_status)?;

    flight_svc
        .prepared_statements
        .write()
        .await
        .entry(handle)
        .and_modify(|stmt| stmt.parameters = parameters);

    Ok(Response::new(Box::pin(empty())))
}

async fn decode_schema(decoder: &mut FlightDataDecoder) -> Result<SchemaRef, Status> {
    while let Some(msg) = decoder.try_next().await? {
        match msg.payload {
            DecodedPayload::None => {}
            DecodedPayload::Schema(schema) => {
                return Ok(schema);
            }
            DecodedPayload::RecordBatch(_) => {
                return Err(Status::invalid_argument(
                    "parameter flight data must have a known schema",
                ));
            }
        }
    }

    Err(Status::invalid_argument(
        "parameter flight data must have a schema",
    ))
}

// Decode parameter ipc stream as ParamValues
fn decode_param_values(
    parameters: Option<&[u8]>,
) -> Result<Option<ParamValues>, arrow::error::ArrowError> {
    parameters
        .map(|parameters| {
            let decoder = StreamReader::try_new(parameters, None)?;
            let schema = decoder.schema();
            let batches = decoder.into_iter().collect::<Result<Vec<_>, _>>()?;
            let batch = concat_batches(&schema, batches.iter())?;
            Ok(record_to_param_values(&batch)?)
        })
        .transpose()
}

// Converts a record batch with a single row into ParamValues
fn record_to_param_values(batch: &RecordBatch) -> Result<ParamValues, DataFusionError> {
    let mut param_values: Vec<(String, Option<usize>, ScalarValue)> = Vec::new();

    let mut is_list = true;
    for col_index in 0..batch.num_columns() {
        let array = batch.column(col_index);
        let scalar = ScalarValue::try_from_array(array, 0)?;
        let name = batch
            .schema_ref()
            .field(col_index)
            .name()
            .trim_start_matches('$')
            .to_string();
        let index = name.parse().ok();
        is_list &= index.is_some();
        param_values.push((name, index, scalar));
    }
    if is_list {
        let mut values: Vec<(Option<usize>, ScalarValue)> = param_values
            .into_iter()
            .map(|(_name, index, value)| (index, value))
            .collect();
        values.sort_by_key(|(index, _value)| *index);
        Ok(values
            .into_iter()
            .map(|(_index, value)| value)
            .collect::<Vec<ScalarValue>>()
            .into())
    } else {
        Ok(param_values
            .into_iter()
            .map(|(name, _index, value)| (name, value))
            .collect::<Vec<(String, ScalarValue)>>()
            .into())
    }
}

fn error_to_status<E: std::fmt::Debug>(err: E) -> Status {
    Status::internal(format!("{err:?}"))
}

fn arrow_error_to_status(err: ArrowError) -> Status {
    Status::internal(format!("{err:?}"))
}
