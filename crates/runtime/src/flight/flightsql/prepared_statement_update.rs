/*
Copyright 2025 The Spice.ai OSS Authors

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

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_flight::{
    FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, PutResult, Ticket,
    decode::{DecodedPayload, FlightDataDecoder},
    error::FlightError,
    flight_service_server::FlightService,
    sql::{self, CommandPreparedStatementUpdate, DoPutPreparedStatementResult, ProstMessageExt},
};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::SchemaRef;
use postcard::{from_bytes, to_stdvec};
use prost::Message;
use std::sync::{Arc, LazyLock};
use tokio_stream::{StreamExt, adapters::Peekable};
use tonic::{Request, Response, Status, Streaming};

use crate::{
    datafusion::{
        request_context_extension::get_current_datafusion,
        sql_validator::validate_sql_query_operations,
    },
    flight::{Service, metrics, to_tonic_err, util::set_flightsql_protocol},
};
use runtime_request_context::{AsyncMarker, RequestContext};

use super::prepared_statement_query::{PreparedStatement, decode_param_values, error_to_status};

/// Static schema for `affected_rows` result to avoid allocation on each request.
static AFFECTED_ROWS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![Field::new(
        "affected_rows",
        DataType::Int64,
        false,
    )]))
});

/// Get a `FlightInfo` for executing a prepared UPDATE/INSERT/DELETE statement.
///
/// Returns metadata about the operation but not the actual results.
/// The number of affected rows will be returned via `do_put`.
pub(crate) async fn get_flight_info(
    handle: sql::CommandPreparedStatementUpdate,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    let _start =
        metrics::track_flight_request("get_flight_info", Some("prepared_statement_update")).await;
    set_flightsql_protocol().await;

    tracing::trace!("get_flight_info_prepared_statement_update");

    let fd = request.into_inner();

    let endpoint = FlightEndpoint::new().with_ticket(Ticket {
        ticket: handle.as_any().encode_to_vec().into(),
    });

    // For UPDATE statements, we return a schema describing the result
    // (typically just the count of affected rows)
    let update_result_schema = Arc::new(Schema::new(vec![Field::new(
        "affected_rows",
        DataType::Int64,
        false,
    )]));

    let info = FlightInfo::new()
        .with_endpoint(endpoint)
        .with_descriptor(fd)
        .try_with_schema(update_result_schema.as_ref())
        .map_err(to_tonic_err)?;

    Ok(Response::new(info))
}

/// Execute a prepared UPDATE/INSERT/DELETE statement that has already had parameters bound.
///
/// This should only be called after `do_put_update` has been called to bind parameters.
/// Returns the number of affected rows.
pub(crate) async fn do_get(
    query: sql::CommandPreparedStatementUpdate,
) -> Result<Response<<Service as FlightService>::DoGetStream>, Status> {
    let _start = metrics::track_flight_request("do_get", Some("prepared_statement_update")).await;
    set_flightsql_protocol().await;

    let context = RequestContext::current(AsyncMarker::new().await);
    let datafusion = get_current_datafusion(&context);

    tracing::trace!("do_get_prepared_statement_update: {query:?}");

    let PreparedStatement {
        query: sql,
        parameters,
        parameter_schema: _,
    } = from_bytes(&query.prepared_statement_handle).map_err(error_to_status)?;

    let parameters = decode_param_values(&parameters).map_err(error_to_status)?;

    // Execute the update statement
    // For UPDATE/INSERT/DELETE statements, we need to execute the full logical plan
    let session = datafusion.ctx.state();
    let plan = session
        .create_logical_plan(&sql)
        .await
        .map_err(|e| Status::internal(format!("Failed to create logical plan: {e}")))?;

    // Validate the plan to ensure only allowed operations are executed
    // This prevents SQL injection attacks via prepared statement updates
    if let Err(e) = validate_sql_query_operations(&plan, &datafusion) {
        return Err(Status::permission_denied(format!(
            "Operation not allowed: {e}"
        )));
    }

    let plan = if let Some(params) = parameters {
        plan.with_param_values(params)
            .map_err(|e| Status::internal(format!("Failed to bind parameters: {e}")))?
    } else {
        plan
    };

    let physical_plan = session
        .create_physical_plan(&plan)
        .await
        .map_err(|e| Status::internal(format!("Failed to create physical plan: {e}")))?;

    // Execute the plan and collect the result (which should be a count for DML statements)
    let results = datafusion::physical_plan::collect(physical_plan, datafusion.ctx.task_ctx())
        .await
        .map_err(|e| Status::internal(format!("Failed to execute statement: {e}")))?;

    // Extract affected rows count from the result
    // DML statements typically return a single row with a count column
    let affected_rows = if !results.is_empty() && results[0].num_rows() > 0 {
        let batch = &results[0];
        if batch.num_columns() > 0 {
            let count_array = batch.column(0);
            if let Some(int64_array) = count_array.as_any().downcast_ref::<Int64Array>() {
                int64_array.value(0)
            } else if let Some(uint64_array) = count_array
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
            {
                let raw_value = uint64_array.value(0);
                // Safely convert u64 to i64, capping at i64::MAX if overflow would occur
                i64::try_from(raw_value).unwrap_or(i64::MAX)
            } else {
                0
            }
        } else {
            0
        }
    } else {
        0
    };

    // Return the affected rows count as a RecordBatch using the static schema
    let batch = RecordBatch::try_new(
        Arc::clone(&AFFECTED_ROWS_SCHEMA),
        vec![Arc::new(Int64Array::from(vec![affected_rows]))],
    )
    .map_err(|e| Status::internal(format!("Failed to create result batch: {e}")))?;

    let output = super::super::record_batches_to_flight_stream(vec![batch]);

    Ok(Response::new(
        Box::pin(output) as <Service as FlightService>::DoGetStream
    ))
}

/// Bind the parameters from the [`FlightData`] to the prepared UPDATE statement.
///
/// This is identical to the query parameter binding, but handles UPDATE statements.
/// See [Sequence Diagrams](https://arrow.apache.org/docs/format/FlightSql.html#sequence-diagrams)
pub(crate) async fn do_put_update(
    query: CommandPreparedStatementUpdate,
    streaming_flight: Peekable<Streaming<FlightData>>,
) -> Result<Response<<Service as FlightService>::DoPutStream>, Status> {
    let streaming_flight = streaming_flight
        .map(|flight_data| flight_data.map_err(|status| FlightError::Tonic(Box::new(status))));

    let mut decoder = FlightDataDecoder::new(streaming_flight);
    let schema = decode_schema(&mut decoder).await?;

    let mut parameters = Vec::new();
    let mut encoder = StreamWriter::try_new(&mut parameters, &schema).map_err(error_to_status)?;
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
                encoder.write(&record_batch).map_err(error_to_status)?;
            }
        }
    }
    encoder.finish().map_err(error_to_status)?;

    if total_rows > 1 {
        return Err(Status::invalid_argument(
            "parameters should contain a single row",
        ));
    }

    let mut stmt: PreparedStatement =
        from_bytes(&query.prepared_statement_handle).map_err(error_to_status)?;
    stmt.parameters = parameters;
    let handle = to_stdvec(&stmt).map_err(error_to_status)?;

    let result = DoPutPreparedStatementResult {
        prepared_statement_handle: Some(handle.into()),
    };

    let output = futures::stream::iter(vec![Ok(PutResult {
        app_metadata: result.encode_to_vec().into(),
    })]);
    Ok(Response::new(Box::pin(output)))
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
