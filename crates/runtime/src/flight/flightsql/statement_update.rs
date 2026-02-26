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
    sql::{CommandStatementUpdate, DoPutUpdateResult, ProstMessageExt},
};
use datafusion::logical_expr::{DdlStatement, LogicalPlan};
use futures::StreamExt;
use prost::Message;
use std::sync::Arc;
use tonic::{Response, Status};

use crate::{
    config::ClusterRole,
    datafusion::DataFusion,
    datafusion::request_context_extension::get_current_datafusion,
    flight::{Service, metrics},
};
use runtime_request_context::{AsyncMarker, RequestContext};

use crate::datafusion::sql_validator::validate_sql_query_operations;
use runtime_auth::AuthRequestContext;

/// Execute a SQL DML statement received via `DoPut` with `CommandStatementUpdate`.
///
/// Per the `FlightSQL` spec, this executes the SQL and returns a `DoPutUpdateResult`
/// with the count of affected rows.
pub(crate) async fn do_put(
    cmd: CommandStatementUpdate,
) -> Result<Response<<Service as FlightService>::DoPutStream>, Status> {
    let _start = metrics::track_flight_request("do_put", Some("statement_update")).await;

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

    // Parse and validate
    let session = datafusion.ctx.state();
    let plan = session
        .create_logical_plan(sql)
        .await
        .map_err(|e| Status::internal(format!("Failed to create logical plan: {e}")))?;

    if let Err(e) = validate_sql_query_operations(&plan, &datafusion) {
        return Err(Status::permission_denied(format!(
            "Operation not allowed: {e}"
        )));
    }

    if should_forward_cayenne_statement_update(&plan, &datafusion) {
        let affected_rows = execute_statement_update_on_executor(&datafusion, sql).await?;
        return Ok(update_result_response(affected_rows));
    }

    // Execute
    let physical_plan = session
        .create_physical_plan(&plan)
        .await
        .map_err(|e| Status::internal(format!("Failed to create physical plan: {e}")))?;

    let results = datafusion::physical_plan::collect(physical_plan, datafusion.ctx.task_ctx())
        .await
        .map_err(|e| Status::internal(format!("Failed to execute statement: {e}")))?;

    // Extract affected rows count
    let affected_rows = extract_affected_rows(&results);

    Ok(update_result_response(affected_rows))
}

fn update_result_response(affected_rows: i64) -> Response<<Service as FlightService>::DoPutStream> {
    let result = DoPutUpdateResult {
        record_count: affected_rows,
    };

    let output = futures::stream::iter(vec![Ok(PutResult {
        app_metadata: result.as_any().encode_to_vec().into(),
    })]);

    Response::new(Box::pin(output))
}

pub(crate) async fn execute_statement_update_on_executor(
    datafusion: &Arc<DataFusion>,
    sql: &str,
) -> Result<i64, Status> {
    let Some(executor_registry) = datafusion.executor_registry.as_ref() else {
        return Err(Status::failed_precondition(
            "No executor registry available for distributed statement update",
        ));
    };

    let (executor_id, mut client) = {
        let clients = executor_registry.flight_sql_clients.read().await;
        let Some((executor_id, client)) = clients.iter().next() else {
            return Err(Status::failed_precondition(
                "No connected executors available for distributed statement update",
            ));
        };
        (executor_id.clone(), client.clone())
    };

    let flight_info = client
        .execute(sql.to_string(), None)
        .await
        .map_err(|e| Status::internal(format!("Failed to forward statement to executor: {e}")))?;

    let mut results = Vec::new();
    for endpoint in flight_info.endpoint {
        let Some(ticket) = endpoint.ticket else {
            continue;
        };

        let mut stream = client.do_get(ticket).await.map_err(|e| {
            Status::internal(format!("Failed to fetch executor update result: {e}"))
        })?;

        while let Some(batch) = stream.next().await {
            let batch = batch
                .map_err(|e| Status::internal(format!("Executor returned invalid batch: {e}")))?;
            results.push(batch);
        }
    }

    tracing::debug!(executor_id, sql, "Forwarded statement update to executor");
    Ok(extract_affected_rows(&results))
}

pub(crate) fn should_forward_cayenne_statement_update(
    plan: &LogicalPlan,
    datafusion: &Arc<DataFusion>,
) -> bool {
    if datafusion.cluster_config.effective_role() != Some(ClusterRole::Scheduler) {
        return false;
    }

    let Some(executor_registry) = datafusion.executor_registry.as_ref() else {
        return false;
    };

    let Ok(clients) = executor_registry.flight_sql_clients.try_read() else {
        return false;
    };
    if clients.is_empty() {
        return false;
    }

    match plan {
        LogicalPlan::Dml(dml) => {
            let target_catalog = dml
                .table_name
                .catalog()
                .unwrap_or(crate::datafusion::SPICE_DEFAULT_CATALOG);
            is_cayenne_catalog(datafusion, target_catalog)
        }
        LogicalPlan::Ddl(ddl) => {
            let target_catalog = ddl_catalog_name(ddl)
                .unwrap_or_else(|| crate::datafusion::SPICE_DEFAULT_CATALOG.to_string());
            is_cayenne_catalog(datafusion, &target_catalog)
        }
        _ => false,
    }
}

fn ddl_catalog_name(ddl: &DdlStatement) -> Option<String> {
    match ddl {
        DdlStatement::CreateExternalTable(create) => create.name.catalog().map(String::from),
        DdlStatement::CreateMemoryTable(create) => create.name.catalog().map(String::from),
        DdlStatement::DropTable(drop) => drop.name.catalog().map(String::from),
        DdlStatement::CreateView(create) => create.name.catalog().map(String::from),
        DdlStatement::DropView(drop) => drop.name.catalog().map(String::from),
        DdlStatement::CreateCatalogSchema(create) => create
            .schema_name
            .split('.')
            .next()
            .filter(|_| create.schema_name.contains('.'))
            .map(String::from),
        DdlStatement::DropCatalogSchema(drop) => match &drop.name {
            datafusion::common::SchemaReference::Full { catalog, .. } => Some(catalog.to_string()),
            datafusion::common::SchemaReference::Bare { .. } => None,
        },
        DdlStatement::CreateCatalog(_)
        | DdlStatement::CreateIndex(_)
        | DdlStatement::CreateFunction(_)
        | DdlStatement::DropFunction(_) => None,
    }
}

fn is_cayenne_catalog(datafusion: &Arc<DataFusion>, catalog_name: &str) -> bool {
    #[cfg(not(windows))]
    {
        datafusion.ctx.catalog(catalog_name).is_some_and(|catalog| {
            crate::datafusion::cayenne_ddl::is_cayenne_catalog(catalog.as_ref())
        })
    }

    #[cfg(windows)]
    {
        let _ = datafusion;
        let _ = catalog_name;
        false
    }
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
