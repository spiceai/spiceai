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

//! Handler for `CommandStatementUpdate` — executes a SQL DML statement and
//! returns the number of affected rows.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, UInt64Array};
use arrow_flight::{
    PutResult,
    flight_service_server::FlightService,
    sql::{CommandStatementUpdate, DoPutUpdateResult},
};
use futures::TryStreamExt;
use prost::Message;
use runtime_query_engine::query_engine::{QueryEngine, QueryRequest};
use tonic::{Response, Status};

use crate::{FlightSqlService, query_engine_err_to_status};

pub(crate) async fn do_put(
    engine: Arc<dyn QueryEngine>,
    cmd: CommandStatementUpdate,
) -> Result<Response<<FlightSqlService as FlightService>::DoPutStream>, Status> {
    tracing::trace!("do_put statement_update: {}", cmd.query);

    let stream = engine
        .execute_query(QueryRequest::new(&cmd.query))
        .await
        .map_err(query_engine_err_to_status)?;
    let results: Vec<RecordBatch> = stream
        .try_collect()
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

    let affected_rows = extract_affected_rows(&results);

    let result = DoPutUpdateResult {
        record_count: affected_rows,
    };
    let output = futures::stream::iter(vec![Ok(PutResult {
        app_metadata: result.encode_to_vec().into(),
    })]);
    Ok(Response::new(Box::pin(output)))
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
