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

use arrow::array::{Int64Array, RecordBatch, UInt64Array};
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
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use postcard::{from_bytes, to_stdvec};
use prost::Message;
use std::sync::LazyLock;
use tokio_stream::adapters::Peekable;
use tonic::{Request, Response, Status, Streaming};

use super::prepared_statement_query::{PreparedStatement, decode_param_values, error_to_status};
use crate::{FlightSqlService, handle_datafusion_error, to_tonic_err};

static AFFECTED_ROWS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![Field::new(
        "affected_rows",
        DataType::Int64,
        false,
    )]))
});

pub(crate) fn get_flight_info(
    handle: &sql::CommandPreparedStatementUpdate,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    let fd = request.into_inner();
    let endpoint = FlightEndpoint::new().with_ticket(Ticket {
        ticket: handle.as_any().encode_to_vec().into(),
    });
    let info = FlightInfo::new()
        .with_endpoint(endpoint)
        .with_descriptor(fd)
        .try_with_schema(AFFECTED_ROWS_SCHEMA.as_ref())
        .map_err(to_tonic_err)?;
    Ok(Response::new(info))
}

pub(crate) async fn do_get(
    ctx: Arc<SessionContext>,
    query: sql::CommandPreparedStatementUpdate,
) -> Result<Response<<FlightSqlService as FlightService>::DoGetStream>, Status> {
    tracing::trace!("do_get prepared_statement_update");

    let PreparedStatement {
        query: sql,
        parameters,
        ..
    } = from_bytes(&query.prepared_statement_handle).map_err(error_to_status)?;

    let param_values = decode_param_values(&parameters).map_err(error_to_status)?;

    let df = ctx.sql(&sql).await.map_err(handle_datafusion_error)?;
    let df = if let Some(params) = param_values {
        df.with_param_values(params)
            .map_err(handle_datafusion_error)?
    } else {
        df
    };
    let results: Vec<RecordBatch> = df.collect().await.map_err(handle_datafusion_error)?;
    let affected_rows = extract_affected_rows(&results);

    let batch = RecordBatch::try_new(
        Arc::clone(&AFFECTED_ROWS_SCHEMA),
        vec![Arc::new(Int64Array::from(vec![affected_rows]))],
    )
    .map_err(|e| Status::internal(format!("failed to build result: {e}")))?;

    let output = crate::record_batches_to_flight_stream(vec![batch]);
    Ok(Response::new(
        Box::pin(output) as <FlightSqlService as FlightService>::DoGetStream
    ))
}

pub(crate) async fn do_put_update(
    query: CommandPreparedStatementUpdate,
    streaming_flight: Peekable<Streaming<FlightData>>,
) -> Result<Response<<FlightSqlService as FlightService>::DoPutStream>, Status> {
    let streaming =
        tokio_stream::StreamExt::map(streaming_flight, |r: Result<FlightData, tonic::Status>| {
            r.map_err(|s| FlightError::Tonic(Box::new(s)))
        });
    let mut decoder = FlightDataDecoder::new(streaming);
    let schema = decode_schema(&mut decoder).await?;

    let mut parameters = Vec::new();
    let mut encoder = StreamWriter::try_new(&mut parameters, &schema).map_err(error_to_status)?;
    let mut total_rows = 0_usize;

    while let Some(msg) = decoder.try_next().await? {
        match msg.payload {
            DecodedPayload::None => {}
            DecodedPayload::Schema(_) => {
                return Err(Status::invalid_argument(
                    "parameter flight data must contain a single schema",
                ));
            }
            DecodedPayload::RecordBatch(batch) => {
                total_rows += batch.num_rows();
                encoder.write(&batch).map_err(error_to_status)?;
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
            DecodedPayload::Schema(schema) => return Ok(schema),
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

fn extract_affected_rows(results: &[RecordBatch]) -> i64 {
    let Some(batch) = results.first() else {
        return 0;
    };
    if batch.num_rows() == 0 || batch.num_columns() == 0 {
        return 0;
    }
    let col = batch.column(0);
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return a.value(0);
    }
    if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
        return i64::try_from(a.value(0)).unwrap_or(i64::MAX);
    }
    0
}
