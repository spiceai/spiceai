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

use arrow::array::RecordBatch;
use arrow_flight::{FlightData, encode::FlightDataEncoderBuilder};
use datafusion::error::DataFusionError;
use datafusion::sql::sqlparser::parser::ParserError;
use futures::{Stream, TryStreamExt, stream};
use tonic::Status;

/// Encode a list of `RecordBatch`es into a Flight data stream.
pub(crate) fn record_batches_to_flight_stream(
    record_batches: Vec<RecordBatch>,
) -> impl Stream<Item = Result<FlightData, Status>> {
    FlightDataEncoderBuilder::new()
        .build(stream::iter(record_batches.into_iter().map(Ok)))
        .map_err(to_tonic_err)
}

pub(crate) fn to_tonic_err<E: std::fmt::Display>(e: E) -> Status {
    Status::internal(format!("{e}"))
}

pub(crate) fn handle_datafusion_error(e: DataFusionError) -> Status {
    match e {
        DataFusionError::Plan(msg) | DataFusionError::Execution(msg) => {
            Status::invalid_argument(msg)
        }
        DataFusionError::SQL(sql_err, _) => match *sql_err {
            ParserError::RecursionLimitExceeded => {
                Status::invalid_argument("Recursion limit exceeded")
            }
            ParserError::ParserError(msg) | ParserError::TokenizerError(msg) => {
                Status::invalid_argument(msg)
            }
        },
        DataFusionError::SchemaError(schema_err, _) => {
            Status::invalid_argument(format!("{schema_err}"))
        }
        DataFusionError::ResourcesExhausted(msg) => Status::resource_exhausted(msg),
        DataFusionError::NotImplemented(msg) => {
            Status::invalid_argument(format!("Unsupported query: {msg}"))
        }
        DataFusionError::Diagnostic(_, source) | DataFusionError::Context(_, source) => {
            handle_datafusion_error(*source)
        }
        DataFusionError::Collection(sources) => sources.into_iter().next().map_or_else(
            || Status::internal("multiple DataFusion errors with no details"),
            handle_datafusion_error,
        ),
        _ => to_tonic_err(e),
    }
}
