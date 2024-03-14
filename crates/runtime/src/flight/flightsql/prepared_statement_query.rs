use std::sync::Arc;

use arrow_flight::{sql, Action};
use tonic::{Request, Status};

use crate::flight::{to_tonic_err, Service};

/// Create a prepared statement from given SQL statement.
pub(crate) async fn do_action_create_prepared_statement(
    flight_svc: &Service,
    statement: sql::ActionCreatePreparedStatementRequest,
    _request: Request<Action>,
) -> Result<sql::ActionCreatePreparedStatementResult, Status> {
    tracing::trace!("do_action_create_prepared_statement: {statement:?}");
    let (arrow_schema, _) = Service::get_arrow_schema_and_size_sql(
        Arc::clone(&flight_svc.datafusion),
        statement.query.clone(),
    )
    .await
    .map_err(to_tonic_err)?;

    let schema_bytes = Service::serialize_schema(&arrow_schema)?;

    Ok(sql::ActionCreatePreparedStatementResult {
        prepared_statement_handle: statement.query.into(),
        dataset_schema: schema_bytes,
        ..Default::default()
    })
}
