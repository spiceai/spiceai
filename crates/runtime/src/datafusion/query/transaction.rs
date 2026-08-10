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

//! Transport-neutral gated-transaction orchestrator.
//!
//! A transaction (`BEGIN; SELECT assert(<gate>); UPDATE …; COMMIT;`) is a single
//! SQL body whose statements must all run through [`QueryBuilder`] — the one
//! place authz, masking, logging, and tracing are applied — while still being
//! atomic. [`run_transaction`] is called identically by the HTTP `/v1/sql` and
//! `FlightSQL` entry points; each maps the returned [`TransactionOutcome`] /
//! [`TransactionError`] into its own transport (HTTP status / gRPC `Status`), so
//! no transport concern lives here.
//!
//! Atomicity and lost-update prevention come from the Cayenne write path, not
//! from intercepting the plan:
//!
//! 1. [`prepare_transaction`] resolves every table the body reads or writes to
//!    its underlying Cayenne provider, captures each participant's
//!    optimistic-concurrency token **before** the gate read, and installs a
//!    [`CayenneTransaction`] on the request context.
//! 2. Every statement runs through [`QueryBuilder`] with the results cache
//!    bypassed (gate reads must see live committed state). Each write's sink
//!    detects the active transaction and *stages* its rows off-lock instead of
//!    publishing; a failed `assert()` gate (or any error) aborts.
//! 3. At COMMIT every staged write is published in one shared metastore
//!    transaction (validating each participant's read footprint + write-set
//!    first); on any conflict or error, all staged writes roll back.

use std::sync::Arc;

use arrow::array::RecordBatch;
use cache::result::CacheStatus;
use cayenne::{CayenneTableProvider, CayenneTransaction};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, ParamValues, TableReference};
use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::{ast::Statement as SqlStatement, dialect::PostgreSqlDialect};
use futures::TryStreamExt;
use runtime_request_context::{AsyncMarker, RequestContext};

use super::{Error as QueryError, QueryBuilder, ResultsCacheMode};
use crate::accelerated::{
    AcceleratedTable,
    write::{CayenneWriteTarget, dual_write::extract_cayenne_write_target},
};
use crate::datafusion::DataFusion;

/// The result of a committed transaction: the final statement's batches and
/// cache status, or `None` for a body that produced no result (bare COMMIT).
pub struct TransactionOutcome {
    pub result: Option<(Vec<RecordBatch>, CacheStatus)>,
}

/// A transaction failure, carrying enough to let each transport pick its own
/// status. Every variant except [`TransactionError::Conflict`] is terminal;
/// `Conflict` is a retryable optimistic-concurrency loss.
pub enum TransactionError {
    /// The body is not a valid v1 transaction (bad write target, unsupported
    /// write op).
    Rejected(String),
    /// A statement failed to plan.
    Plan(DataFusionError),
    /// A statement (including the `assert()` gate) failed during execution.
    Query(QueryError),
    /// A statement's result stream errored while draining.
    Stream(DataFusionError),
    /// An optimistic-concurrency conflict on `table` — retryable at the newest
    /// committed state.
    Conflict { table: String },
    /// The staged writes failed to publish at COMMIT.
    Publish(String),
}

/// Returns the statements inside a well-formed `BEGIN … COMMIT` transaction.
/// Transaction-control statements are stripped. A single statement or a
/// multi-statement string not wrapped in `BEGIN … COMMIT` returns `None` and is
/// handled by the ordinary query path.
#[must_use]
pub fn transaction_statements(sql: &str) -> Option<Vec<String>> {
    let statements: Vec<DFStatement> = DFParser::parse_sql_with_dialect(sql, &PostgreSqlDialect {})
        .ok()?
        .into_iter()
        .collect();
    if statements.len() < 2 {
        return None;
    }
    let first_is_begin = matches!(
        statements.first(),
        Some(DFStatement::Statement(s)) if matches!(s.as_ref(), SqlStatement::StartTransaction { .. })
    );
    let last_is_commit = matches!(
        statements.last(),
        Some(DFStatement::Statement(s)) if matches!(s.as_ref(), SqlStatement::Commit { .. })
    );
    if !(first_is_begin && last_is_commit) {
        return None;
    }
    let inner: Vec<String> = statements[1..statements.len() - 1]
        .iter()
        .map(ToString::to_string)
        .collect();
    if inner.is_empty() {
        return None;
    }
    Some(inner)
}

/// The statement whose schema a `FlightSQL` `GetFlightInfo` /
/// `CreatePreparedStatement` should advertise for `sql`: the FINAL statement of
/// a `BEGIN … COMMIT` body (planned for its schema without executing — the body
/// itself is not a single plannable statement), or `sql` unchanged otherwise.
#[must_use]
pub fn schema_statement(sql: &str) -> String {
    transaction_statements(sql)
        .and_then(|statements| statements.last().cloned())
        .unwrap_or_else(|| sql.to_string())
}

/// Execute a transaction body atomically across every table it touches.
///
/// See the module docs for the protocol. v1 supports one INSERT/UPDATE write per
/// table (no DELETE/MERGE, no PK reassignment). Reads of Cayenne tables outside
/// the participant set fail the transaction closed. Bound parameters are
/// supported across statements via [`normalize_transaction_parameters`]. Durable
/// propagation of the committed writes back to the federated source is tracked
/// separately.
///
/// # Errors
///
/// Returns a [`TransactionError`] if the body is not a valid v1 transaction, a
/// statement fails to plan or execute, the optimistic-concurrency re-check
/// loses (retryable [`TransactionError::Conflict`]), or the staged writes fail
/// to publish.
pub async fn run_transaction(
    df: &Arc<DataFusion>,
    statements: &[String],
    parameters: Option<ParamValues>,
    read_only: bool,
) -> Result<TransactionOutcome, TransactionError> {
    // Prepare the transaction: identify every participant table, validate the
    // write targets, capture their begin tokens, and install the transaction on
    // the request context. A body with no write is a plain read-only transaction.
    let handle = prepare_transaction(df, statements).await?;

    let parameters = normalize_transaction_parameters(parameters);
    let mut last: Option<(Vec<RecordBatch>, CacheStatus)> = None;
    let statement_count = statements.len();
    for (index, statement) in statements.iter().enumerate() {
        let query_res =
            match run_transaction_statement(df, statement, parameters.clone(), read_only).await {
                Ok(result) => result,
                Err(error) => {
                    abort_transaction(handle.as_ref()).await;
                    return Err(TransactionError::Query(error));
                }
            };

        let cache_status = query_res.cache_status;
        let mut data = query_res.data;
        // Every statement must run to completion so its writes stage and any error
        // (including a gate abort) surfaces before the next statement — and before
        // COMMIT. Only the FINAL statement's batches are kept: its result can be
        // emitted to the caller only once the commit is confirmed (a conflict must
        // surface as an error, never as a truncated result), so it is materialized
        // here and returned after commit. Intermediate statements (the gate,
        // earlier writes) are drained without materializing their batches.
        if index + 1 == statement_count {
            match data.try_collect::<Vec<RecordBatch>>().await {
                Ok(batches) => last = Some((batches, cache_status)),
                Err(e) => {
                    abort_transaction(handle.as_ref()).await;
                    return Err(TransactionError::Stream(e));
                }
            }
        } else {
            loop {
                match data.try_next().await {
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(e) => {
                        abort_transaction(handle.as_ref()).await;
                        return Err(TransactionError::Stream(e));
                    }
                }
            }
        }
    }

    // Every statement succeeded (the gate passed). Publish all staged writes
    // atomically; the orchestrator rolls back internally on a conflict or error.
    if let Some(handle) = &handle {
        match handle.txn.commit().await {
            Ok(_summary) => {}
            Err(cayenne::provider::Error::WriteConflict { table }) => {
                return Err(TransactionError::Conflict { table });
            }
            Err(e) => {
                return Err(TransactionError::Publish(e.to_string()));
            }
        }
        // Each write's plan-time cache invalidation ran before the rows were
        // visible, so a concurrent SELECT could have repopulated the cache with
        // pre-commit state. Re-invalidate now that the writes are published.
        for (_table_id, table_ref) in &handle.written {
            if let Err(e) = df.caching().invalidate_for_table(table_ref.clone()) {
                tracing::warn!(
                    "transaction: post-commit cache invalidation for {table_ref} failed: {e}"
                );
            }
        }
    }

    Ok(TransactionOutcome { result: last })
}

/// Makes positional parameters reusable across transaction statements.
///
/// Each statement is planned independently, so a positional list containing
/// `$1` and `$2` would otherwise fail validation when one statement references
/// only a subset. Named values preserve the transaction-wide placeholder
/// indexes while allowing each statement to bind the values it uses.
fn normalize_transaction_parameters(parameters: Option<ParamValues>) -> Option<ParamValues> {
    match parameters {
        Some(ParamValues::List(values)) => Some(ParamValues::Map(
            values
                .into_iter()
                .enumerate()
                .map(|(index, value)| ((index + 1).to_string(), value))
                .collect(),
        )),
        parameters => parameters,
    }
}

async fn run_transaction_statement(
    df: &Arc<DataFusion>,
    sql: &str,
    parameters: Option<ParamValues>,
    read_only: bool,
) -> Result<cache::result::query::QueryResult, QueryError> {
    QueryBuilder::new(sql, Arc::clone(df))
        .parameters(parameters)
        .read_only(read_only)
        // Gate + inner reads must see live committed state, not a (stale) cached
        // result — the transaction is the serialization point, not the cache.
        .results_cache_mode(ResultsCacheMode::Bypass)
        .build()
        .run()
        .await
}

/// A prepared transaction: the installed [`CayenneTransaction`] plus each
/// written table's [`TableReference`] (for the post-commit results-cache
/// invalidation).
struct TransactionHandle {
    /// The transaction shared with the write path via the request context.
    txn: CayenneTransaction,
    /// `(table_id, table_ref)` for every written table, invalidated after commit.
    written: Vec<(String, TableReference)>,
}

/// Identify every table the body reads or writes, validate each write target as
/// a non-partitioned accelerator-only Cayenne table, capture each participant's
/// begin token, and install the [`CayenneTransaction`] on the current request
/// context.
///
/// Returns `Ok(None)` for a body with no write (a read-only transaction), or a
/// [`TransactionError`] if a write target is not a valid v1 transaction table.
async fn prepare_transaction(
    df: &Arc<DataFusion>,
    inner_sqls: &[String],
) -> Result<Option<TransactionHandle>, TransactionError> {
    let session = df.ctx.state();
    let mut write_refs: Vec<TableReference> = Vec::new();
    let mut read_refs: Vec<TableReference> = Vec::new();
    for stmt_sql in inner_sqls {
        let plan = session
            .create_logical_plan(stmt_sql)
            .await
            .map_err(TransactionError::Plan)?;
        // A read-only body (`None`) contributes no write ref; a write target
        // must be a valid v1 transaction table (`Err` → rejected).
        if let Some(result) = classify_transaction_write(&plan) {
            let table = result.map_err(|op| {
                TransactionError::Rejected(format!(
                    "transactions do not support {op} (v1 supports gated INSERT/UPDATE writes)"
                ))
            })?;
            if !write_refs.contains(&table) {
                write_refs.push(table);
            }
        }
        // Collect every scanned table (including gate/subquery reads) so each is
        // registered as a participant; an unregistered Cayenne read fails closed.
        let _ = plan.apply_with_subqueries(|node| {
            if let LogicalPlan::TableScan(scan) = node
                && !read_refs.contains(&scan.table_name)
            {
                read_refs.push(scan.table_name.clone());
            }
            Ok(TreeNodeRecursion::Continue)
        });
    }

    if write_refs.is_empty() {
        // No write: a read-only transaction has nothing to serialize or publish.
        return Ok(None);
    }

    let txn = CayenneTransaction::new();
    let mut written: Vec<(String, TableReference)> = Vec::new();

    // Register every write target. It must be an accelerator-only, non-partitioned
    // Cayenne table — other dataset modes route writes to the federated source,
    // where the gate would not govern them. The begin token is captured here,
    // before any statement (gate read) runs.
    for table_ref in &write_refs {
        let table_name = table_ref.table();
        let Some(provider) = resolve_cayenne_staged(df, table_name).await else {
            return Err(TransactionError::Rejected(format!(
                "transaction target '{table_name}' must be an accelerator-only, non-partitioned Cayenne dataset (configure on_conflict without CDC); other modes route writes to the federated source"
            )));
        };
        let token = provider.transaction_write_token().await;
        let table_id = provider.table_id().to_string();
        txn.register(table_id.clone(), token, provider);
        written.push((table_id, table_ref.clone()));
    }

    // Register read-only Cayenne participants (validated at commit by version).
    // A Cayenne table that does not resolve here (partitioned, or a non
    // accelerator-only mode) is not registered; if the body reads it, its scan
    // marks the transaction for a fail-closed abort.
    for table_ref in &read_refs {
        if let Some(provider) = resolve_cayenne_staged(df, table_ref.table()).await {
            let table_id = provider.table_id().to_string();
            if !txn.is_participant(&table_id) {
                let token = provider.transaction_write_token().await;
                txn.register(table_id, token, provider);
            }
        }
    }

    RequestContext::current(AsyncMarker::new().await).insert_extension(txn.clone());
    Ok(Some(TransactionHandle { txn, written }))
}

/// Resolve a table reference to its non-partitioned, accelerator-only Cayenne
/// provider, or `None` if the table is not such a Cayenne table (and so is not a
/// transaction participant).
async fn resolve_cayenne_staged(
    df: &Arc<DataFusion>,
    table_name: &str,
) -> Option<CayenneTableProvider> {
    let provider = df.get_accelerated_table_provider(table_name).await.ok()?;
    let accel = spice_table::find_layer::<AcceleratedTable>(provider.as_ref(), spice_table::LayerWalk::Read)?;
    // Accelerator-only (the gate governs the sole accelerator write) OR durable
    // write-back (the write stages to the accelerator, marks its keys, and a
    // per-table worker reconciles them to the source — see #11838). Both stage
    // through the Cayenne write path, so a gated transaction is safe.
    if !accel.is_accelerator_only() && !accel.is_durable_write_back() {
        return None;
    }
    match extract_cayenne_write_target(&accel.get_accelerator()) {
        Some(CayenneWriteTarget::Staged(cayenne)) => Some(*cayenne),
        _ => None,
    }
}

/// Classify an inner statement for a transaction:
/// `None` for a read, `Some(Ok(table))` for a stageable INSERT/UPDATE write, or
/// `Some(Err(op))` for a write form v1 cannot stage atomically (its sink is not
/// transaction-aware), which must abort the transaction rather than publish.
fn classify_transaction_write(plan: &LogicalPlan) -> Option<Result<TableReference, &'static str>> {
    use datafusion::logical_expr::WriteOp;
    match plan {
        LogicalPlan::Dml(dml) => Some(match &dml.op {
            WriteOp::Insert(_) | WriteOp::Update => Ok(dml.table_name.clone()),
            WriteOp::Delete => Err("DELETE"),
            WriteOp::Ctas | WriteOp::Truncate => Err("this operation"),
        }),
        LogicalPlan::Extension(ext) => {
            let dml = ext
                .node
                .as_any()
                .downcast_ref::<datafusion_dml::DmlExtensionNode>()?;
            Some(match &dml.op {
                datafusion_dml::DmlNodeOp::Insert(p) => Ok(p.table_name.clone()),
                datafusion_dml::DmlNodeOp::Update(p) => Ok(p.table_name.clone()),
                datafusion_dml::DmlNodeOp::Delete(_) => Err("DELETE"),
                datafusion_dml::DmlNodeOp::Merge(_) => Err("MERGE"),
            })
        }
        _ => None,
    }
}

/// Roll back a transaction before commit: discard every staged write (removing
/// its staged snapshot directory) and drop all transaction state.
async fn abort_transaction(handle: Option<&TransactionHandle>) {
    if let Some(handle) = handle {
        handle.txn.abort().await;
    }
}
