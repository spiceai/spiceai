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

//! Handler for `FlightSQL` `CommandStatementUpdate` — executes SQL DML
//! statements (`INSERT INTO`, `CREATE TABLE`, `DROP TABLE`, etc.)
//! and returns the number of affected rows.

use arrow::array::{Int64Array, RecordBatch};
use arrow_flight::{
    PutResult,
    flight_service_server::FlightService,
    sql::{CommandStatementUpdate, DoPutUpdateResult},
};
use datafusion::logical_expr::LogicalPlan;
use futures::TryStreamExt;
use prost::Message;
use tonic::{Response, Status};

use crate::{
    config::ClusterRole,
    datafusion::{query::QueryBuilder, request_context_extension::get_current_datafusion},
    flight::{Service, handle_query_error, metrics, to_tonic_err, util::set_flightsql_protocol},
};
use runtime_auth::AuthRequestContext;
use runtime_request_context::{AsyncMarker, RequestContext};

/// Execute a SQL DML statement received via `DoPut` with `CommandStatementUpdate`.
///
/// Per the `FlightSQL` spec, this executes the SQL and returns a `DoPutUpdateResult`
/// with the count of affected rows.
pub(crate) async fn do_put(
    cmd: CommandStatementUpdate,
) -> Result<Response<<Service as FlightService>::DoPutStream>, Status> {
    let _start = metrics::track_flight_request("do_put", Some("statement_update")).await;
    set_flightsql_protocol().await;

    // Authenticate — this handler is dispatched before the generic DoPut auth
    // check, so we must verify credentials here.
    let context = RequestContext::current(AsyncMarker::new().await);
    let datafusion = get_current_datafusion(&context);

    match context.auth_principal() {
        Some(principal) => {
            if !principal
                .groups()
                .iter()
                .any(|group| *group == "write" || *group == "read_write")
            {
                return Err(Status::permission_denied(
                    "Write access denied. Verify that authentication key used has write access and try again.",
                ));
            }
        }
        None => {
            if allow_scheduler_trusted_executor_write(&datafusion) {
                tracing::debug!(
                    "Allowing unauthenticated statement DoPut on executor in mTLS scheduler-trusted mode"
                );
            } else {
                return Err(Status::unauthenticated(
                    "Flight DoPut requires authentication.\nFor auth details, visit https://spiceai.org/docs/api/auth",
                ));
            }
        }
    }

    let sql = &cmd.query;
    tracing::trace!("do_put_statement_update: {sql}");

    let dml_table = match datafusion.ctx.state().create_logical_plan(sql).await {
        Ok(LogicalPlan::Dml(dml)) => Some(dml.table_name),
        Ok(_) | Err(_) => None,
    };

    // Execute through the standard query path, which handles DELETE and UPDATE
    // via the runtime's DML interception in Query::run().
    let query_result = QueryBuilder::new(sql, std::sync::Arc::clone(&datafusion))
        .build()
        .run()
        .await
        .map_err(handle_query_error)?;

    let results: Vec<RecordBatch> = query_result
        .data
        .try_collect()
        .await
        .map_err(to_tonic_err)?;

    if let Some(table_name) = dml_table {
        let table_name_display = table_name.to_string();
        if let Err(e) = datafusion.caching().invalidate_for_table(table_name) {
            tracing::warn!(
                "Failed to invalidate caches for table {} after statement update: {e}",
                table_name_display
            );
        }
    }

    // Extract affected rows count
    let affected_rows = extract_affected_rows(&results);

    let result = DoPutUpdateResult {
        record_count: affected_rows,
    };

    let output = futures::stream::iter(vec![Ok(PutResult {
        app_metadata: result.encode_to_vec().into(),
    })]);

    Ok(Response::new(Box::pin(output)))
}

fn allow_scheduler_trusted_executor_write(datafusion: &crate::datafusion::DataFusion) -> bool {
    datafusion.cluster_config.effective_role() == Some(ClusterRole::Executor)
        && datafusion.cluster_config.tls_config().is_some()
}

/// Extract affected row count from execution results.
fn extract_affected_rows(results: &[RecordBatch]) -> i64 {
    if results.is_empty() || results[0].num_rows() == 0 {
        return 0;
    }

    let batch = &results[0];
    if batch.num_columns() == 0 {
        return 0;
    }

    let count_array = batch.column(0);
    if let Some(int64_array) = count_array.as_any().downcast_ref::<Int64Array>() {
        int64_array.value(0)
    } else if let Some(uint64_array) = count_array
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
    {
        // Row counts exceeding i64::MAX are physically impossible, but
        // saturate rather than silently returning a wrong value.
        i64::try_from(uint64_array.value(0)).unwrap_or(i64::MAX)
    } else {
        0
    }
}
