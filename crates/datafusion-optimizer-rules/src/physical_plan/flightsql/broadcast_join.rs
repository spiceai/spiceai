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

//! Physical optimizer rule that distributes a `HashJoinExec` between a
//! partitioned fact table and a SMALL dimension table onto the executors,
//! instead of pulling every fact row up to the scheduler and joining centrally.
//!
//! Today, a distributed-acceleration join federates only the scans:
//! ```text
//! HashJoinExec
//!   UnionExec[ FlightSqlExec(fact part 0), ..., FlightSqlExec(fact part N) ]   -- 6M rows shipped up
//!   UnionExec[ FlightSqlExec(dim part 0),  ..., FlightSqlExec(dim part M)  ]
//! ```
//! The scheduler repartitions and joins. This rule rewrites it to a per-executor
//! join pushed back down — each fact-partition executor joins its local fact
//! slice against the FULL dimension, which it gathers from its peers via the
//! `executor_table` UDTF, and ships only the joined rows:
//! ```text
//! UnionExec[
//!   BroadcastJoinFlightSqlExec(exec0): SELECT ... FROM (fact) f
//!       JOIN (SELECT * FROM executor_table('e0',dim) UNION ALL ...) d ON ...
//!   BroadcastJoinFlightSqlExec(exec1): <same SQL, run on exec1>
//!   ...
//! ]
//! ```
//! The per-executor SQL is identical (each executor's `FROM dim_or_fact` resolves
//! to its own partition; the `executor_table` UNION addresses are the same), so
//! it is built once and run against each fact-partition executor's client.
//!
//! Correctness: for an inner equi-join keyed on the fact, the union of
//! `(fact_partitionᵢ ⋈ full_dim)` equals `full_fact ⋈ full_dim` (each fact row
//! lives in exactly one partition and joins independently). Validated on SF1.
//!
//! Gating: only fires when the dimension side's statistics report a row count
//! below `broadcast_threshold_rows` — broadcasting a large table would move
//! `N_executors × dim_size` rows and lose.

use std::fmt;
use std::sync::Arc;

use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DataFusionError, NullEquality, Result, Statistics};
use datafusion::config::ConfigOptions;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::JoinType;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
#[expect(
    deprecated,
    reason = "DF53 deprecates CoalesceBatchesExec (arrow BatchCoalescer); the wrapper check below still recognizes it where it appears in a plan"
)]
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::StreamExt;

use data_components::flightsql::{FlightSqlClient, FlightSqlExec, query_to_stream};
use data_components::sql_expr::to_sql_preserving_precedence;
use flight_client::cookie::CookieStore;

/// Pass-through nodes that may sit between a `HashJoinExec` and the federated
/// scan union without changing the data (same set the agg-pushdown rule uses).
const PASS_THROUGH_EXEC_NAMES: &[&str] = &["CooperativeExec", "BytesProcessedExec"];

/// The fact side must exceed the broadcast cost (`dim_rows × num_executors`) by
/// at least this factor for a broadcast to be worthwhile. Validated against
/// SF10: genuine wins (lineitem facts) have a fact/cost ratio ≥7.5, while
/// marginal joins that regressed (orders/customer-class) sit around ~2.5, so a
/// factor of 4 separates them with headroom on both sides.
const BROADCAST_FACT_MARGIN: usize = 4;

/// Returns live peer executor addresses (`host:port`, no scheme) for the
/// `executor_table` UNION. Supplied by the runtime (cluster) layer so this
/// crate need not depend on `runtime-cluster`.
pub type ExecutorAddressProvider = Arc<dyn Fn() -> Vec<String> + Send + Sync>;

/// A leaf `ExecutionPlan` that runs a pre-built join SQL against ONE executor's
/// Flight SQL endpoint (the executor owning a fact partition). The SQL gathers
/// the broadcast dimension from all peers via `executor_table`.
#[derive(Clone)]
pub struct BroadcastJoinFlightSqlExec {
    sql: String,
    client: FlightSqlClient,
    cookie_store: Arc<CookieStore>,
    output_schema: SchemaRef,
    trace_parent: Option<String>,
    properties: Arc<PlanProperties>,
    statistics: Arc<Statistics>,
}

impl BroadcastJoinFlightSqlExec {
    fn new(
        sql: String,
        client: FlightSqlClient,
        cookie_store: Arc<CookieStore>,
        output_schema: SchemaRef,
        trace_parent: Option<String>,
        statistics: Statistics,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            sql,
            client,
            cookie_store,
            output_schema,
            trace_parent,
            properties,
            statistics: Arc::new(statistics),
        }
    }
}

impl fmt::Debug for BroadcastJoinFlightSqlExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BroadcastJoinFlightSqlExec: sql={}", self.sql)
    }
}

impl DisplayAs for BroadcastJoinFlightSqlExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BroadcastJoinFlightSqlExec: sql={}", self.sql)
    }
}

impl ExecutionPlan for BroadcastJoinFlightSqlExec {
    fn name(&self) -> &'static str {
        "BroadcastJoinFlightSqlExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut client = self.client.clone();
        if let Some(tp) = &self.trace_parent {
            client.set_header("traceparent", tp.clone());
        }
        // Coerce each returned batch to the expected output schema. The remote
        // executor returns its native string types (e.g. LargeUtf8) but the
        // parent plan expects the join's schema (e.g. Utf8View); cast to match,
        // mirroring `FlightSqlExec`'s own schema alignment.
        let target = Arc::clone(&self.output_schema);
        let target_for_map = Arc::clone(&target);
        let stream = query_to_stream(client, self.sql.clone(), Arc::clone(&self.cookie_store))
            .map(move |res| res.and_then(|batch| coerce_batch(batch, &target_for_map)));
        Ok(Box::pin(RecordBatchStreamAdapter::new(target, stream)))
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        Ok(Arc::clone(&self.statistics))
    }
}

/// Physical optimizer rule: distribute small-dimension joins onto executors.
#[derive(Clone)]
pub struct FlightSQLBroadcastJoinPushdown {
    addresses: ExecutorAddressProvider,
    broadcast_threshold_rows: usize,
}

impl fmt::Debug for FlightSQLBroadcastJoinPushdown {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlightSQLBroadcastJoinPushdown")
            .field("broadcast_threshold_rows", &self.broadcast_threshold_rows)
            .finish_non_exhaustive()
    }
}

impl FlightSQLBroadcastJoinPushdown {
    #[must_use]
    pub fn new(addresses: ExecutorAddressProvider, broadcast_threshold_rows: usize) -> Arc<Self> {
        Arc::new(Self {
            addresses,
            broadcast_threshold_rows,
        })
    }
}

impl PhysicalOptimizerRule for FlightSQLBroadcastJoinPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let addresses = (self.addresses)();
        if addresses.is_empty() {
            return Ok(plan);
        }
        let threshold = self.broadcast_threshold_rows;
        plan.transform_up(|p| try_rewrite(p, &addresses, threshold))
            .map(|t| t.data)
    }

    fn name(&self) -> &'static str {
        "FlightSQLBroadcastJoinPushdown"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// One side of the join, resolved to a federated scan: the per-partition
/// `FlightSqlExec`s and the table/filter/schema info (shared across partitions).
struct FederatedSide<'a> {
    flight_execs: Vec<&'a FlightSqlExec>,
    schema: SchemaRef,
}

fn try_rewrite(
    plan: Arc<dyn ExecutionPlan>,
    addresses: &[String],
    max_broadcast_rows: usize,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let Some(join) = plan.downcast_ref::<HashJoinExec>() else {
        return Ok(Transformed::no(plan));
    };
    // Only inner equi-joins with no residual filter (the common cayenne shape).
    // SQL `=` never matches NULL keys, so a join planned with
    // `NullEquality::NullEqualsNull` (`IS NOT DISTINCT FROM`) cannot be
    // rendered as `f.k = d.k` — rewriting it would silently drop the
    // NULL-key matches the central join produces.
    if *join.join_type() != JoinType::Inner
        || join.filter().is_some()
        || join.on().is_empty()
        || join.null_equality() != NullEquality::NullEqualsNothing
    {
        return Ok(Transformed::no(plan));
    }

    let Some(left) = resolve_federated_side(join.left()) else {
        return Ok(Transformed::no(plan));
    };
    let Some(right) = resolve_federated_side(join.right()) else {
        return Ok(Transformed::no(plan));
    };

    // Pick the smaller side (by stats) as the broadcast dimension; it must be
    // below the threshold. The other side is the partitioned fact.
    let left_rows = side_num_rows(&left);
    let right_rows = side_num_rows(&right);
    let num_executors = addresses.len();
    // Choose the broadcast (dimension) side: the smaller input. Broadcasting it
    // gathers `dim_rows × num_executors` rows across the cluster, so only do it
    // when the fact is large enough that NOT shipping it to the coordinator is a
    // clear win — i.e. the fact must exceed the broadcast cost by a safety
    // factor: `dim_rows × N × BROADCAST_FACT_MARGIN < fact_rows`. This is a
    // scale-invariant broadcast-vs-shuffle cost test (fires for small dims at any
    // scale factor). A plain `dim × N < fact` is too loose: it fired on
    // moderate-ratio joins (e.g. tpch q3's orders⋈customer, fact/cost ratio ~2.5)
    // where the executor_table round-trips outweigh the savings, while the genuine
    // wins (lineitem facts, ratio ≥7.5) clear the margin comfortably. Also cap the
    // dim size to bound per-executor memory. Requires row-count stats on both
    // sides (else we cannot tell which side is small, so leave the plan alone).
    let dim_is_left = match (left_rows, right_rows) {
        (Some(l), Some(r)) => {
            let (dim_rows, fact_rows, is_left) = if l <= r { (l, r, true) } else { (r, l, false) };
            if dim_rows > max_broadcast_rows
                || dim_rows
                    .saturating_mul(num_executors)
                    .saturating_mul(BROADCAST_FACT_MARGIN)
                    >= fact_rows
            {
                return Ok(Transformed::no(plan));
            }
            is_left
        }
        _ => return Ok(Transformed::no(plan)),
    };
    let (fact, dim, fact_is_left) = if dim_is_left {
        (&right, &left, false)
    } else {
        (&left, &right, true)
    };

    let Some(sql) = build_join_sql(join, fact, dim, fact_is_left, addresses) else {
        return Ok(Transformed::no(plan));
    };

    // One BroadcastJoinFlightSqlExec per fact-partition executor, all running
    // the identical join SQL against that executor's client.
    let output_schema = join.schema();
    let statistics = Statistics::new_unknown(&output_schema);
    let mut children: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(fact.flight_execs.len());
    for fe in &fact.flight_execs {
        children.push(Arc::new(BroadcastJoinFlightSqlExec::new(
            sql.clone(),
            fe.client().clone(),
            Arc::clone(fe.cookie_store()),
            Arc::clone(&output_schema),
            fe.trace_parent().map(str::to_string),
            statistics.clone(),
        )));
    }

    let union: Arc<dyn ExecutionPlan> = if children.len() == 1 {
        children.remove(0)
    } else {
        UnionExec::try_new(children)?
    };
    // Preserve the replaced join's output partitioning. This rule runs AFTER
    // EnforceDistribution, so a parent `HashJoinExec(mode=Partitioned)` still
    // expects matching partition counts on its inputs; our union has one
    // partition per executor and would otherwise trip a "partition count
    // mismatch N!=M" assertion. Re-establishing the partitioning is cheap — it
    // repartitions only the small joined result, not the raw fact table — so the
    // broadcast win is preserved.
    //
    // IMPORTANT: the join's reported `Hash([..])` partitioning may reference
    // columns that its own embedded projection dropped (a Partitioned hash join
    // whose key is not in the output schema reports `Hash([UnKnownColumn])` — an
    // internally inconsistent partitioning). Blindly reusing it makes the
    // `RepartitionExec` evaluate that bogus hash and fail at runtime with
    // "UnKnownColumn::evaluate() should not be called" (hit by tpch q3/q5/q8/q14,
    // whose join feeds an aggregate that doesn't reference the keys). So remap the
    // hash columns onto our output schema by name and fall back to round-robin
    // when a key was dropped — a parent that needs hash co-location only
    // references columns that ARE in the output, so its keys always remap.
    let result: Arc<dyn ExecutionPlan> =
        match safe_output_partitioning(join.properties().output_partitioning(), &output_schema) {
            Some(part) => Arc::new(RepartitionExec::try_new(union, part)?),
            None => union,
        };
    Ok(Transformed::yes(result))
}

/// Total estimated row count of a federated side = the SUM of its per-partition
/// `FlightSqlExec` statistics (each scan reports only its own partition's rows).
/// `None` if no partition carries a row-count estimate.
fn side_num_rows(side: &FederatedSide) -> Option<usize> {
    let mut total = 0usize;
    let mut any = false;
    for fe in &side.flight_execs {
        if let Ok(stats) = fe.partition_statistics(None)
            && let Some(n) = stats.num_rows.get_value()
        {
            total += *n;
            any = true;
        }
    }
    any.then_some(total)
}

/// Produce a partitioning for the broadcast result that is VALID for `out_schema`
/// (the broadcast output schema), re-establishing the replaced join's partition
/// count for any parent that requires it.
///
/// `Hash([..])` keys are remapped onto `out_schema` by name; if any key is not a
/// plain `Column` present in the output (i.e. the join dropped it, leaving a
/// bogus `Hash([UnKnownColumn])`), we fall back to round-robin to the same
/// partition count — which satisfies a parent that only needs the count (e.g. a
/// partial aggregate), while a parent needing hash co-location only references
/// columns that are present and so always remaps cleanly. Returns `None` to leave
/// the union unwrapped (unknown partitioning carries no count contract).
fn safe_output_partitioning(part: &Partitioning, out_schema: &SchemaRef) -> Option<Partitioning> {
    match part {
        Partitioning::Hash(exprs, n) => {
            let mut mapped: Vec<Arc<dyn PhysicalExpr>> = Vec::with_capacity(exprs.len());
            for e in exprs {
                let Some(col) = e.downcast_ref::<Column>() else {
                    return Some(Partitioning::RoundRobinBatch(*n));
                };
                match out_schema.index_of(col.name()) {
                    Ok(idx) => mapped.push(Arc::new(Column::new(col.name(), idx))),
                    Err(_) => return Some(Partitioning::RoundRobinBatch(*n)),
                }
            }
            Some(Partitioning::Hash(mapped, *n))
        }
        Partitioning::RoundRobinBatch(n) => Some(Partitioning::RoundRobinBatch(*n)),
        Partitioning::UnknownPartitioning(_) => None,
    }
}

/// Resolve a join input to a federated scan: a `UnionExec` (or single node) of
/// `FlightSqlExec` leaves, walking through repartition / pass-through nodes.
fn resolve_federated_side(plan: &Arc<dyn ExecutionPlan>) -> Option<FederatedSide<'_>> {
    // A join input is typically `RepartitionExec(Hash) -> UnionExec ->
    // [pass-through -> FlightSqlExec]`. Descend through single-input wrapper
    // nodes (repartition / coalesce / cooperative) to reach the `UnionExec`
    // (or a bare `FlightSqlExec`) before collecting the per-partition scans —
    // otherwise `collect_flight_execs` hits the multi-input `UnionExec`
    // mid-walk and bails.
    let mut scan_root = plan;
    while scan_root.downcast_ref::<UnionExec>().is_none()
        && scan_root.downcast_ref::<FlightSqlExec>().is_none()
    {
        if !is_single_input_wrapper(scan_root.as_ref()) {
            return None;
        }
        let children = scan_root.children();
        if children.len() != 1 {
            return None;
        }
        scan_root = children[0];
    }
    let flight_execs = collect_flight_execs(scan_root)?;
    if flight_execs.is_empty() {
        return None;
    }
    let schema = Arc::clone(flight_execs[0].projected_schema());
    // All partitions must scan the same table for the broadcast to be well-defined.
    let table = flight_execs[0].table_reference();
    if flight_execs.iter().any(|fe| fe.table_reference() != table) {
        return None;
    }
    Some(FederatedSide {
        flight_execs,
        schema,
    })
}

fn collect_flight_execs(plan: &Arc<dyn ExecutionPlan>) -> Option<Vec<&FlightSqlExec>> {
    if let Some(union) = plan.downcast_ref::<UnionExec>() {
        let mut out = Vec::with_capacity(union.inputs().len());
        for child in union.inputs() {
            out.push(walk_to_flight_exec(child)?);
        }
        Some(out)
    } else {
        Some(vec![walk_to_flight_exec(plan)?])
    }
}

fn walk_to_flight_exec(plan: &Arc<dyn ExecutionPlan>) -> Option<&FlightSqlExec> {
    let mut current = plan;
    loop {
        if let Some(fe) = current.downcast_ref::<FlightSqlExec>() {
            return Some(fe);
        }
        let children = current.children();
        if children.len() != 1 {
            return None;
        }
        if !is_single_input_wrapper(current.as_ref()) {
            return None;
        }
        current = children[0];
    }
}

/// A single-input node that passes data through unchanged for the purposes of
/// resolving a federated scan: repartition, coalesce-batches, and the
/// name-identified `CooperativeExec` / `BytesProcessedExec` wrappers.
///
/// `FilterExec` is deliberately NOT pass-through. `FlightSQLTable` reports
/// filter pushdown as `Exact` (predicate absorbed into the scan SQL, no
/// `FilterExec` planned) or `Unsupported` (predicate stays in the plan as a
/// `FilterExec` and is NOT in the scan SQL) — it never reports `Inexact`. So a
/// `FilterExec` here always carries a predicate the generated join SQL would
/// lose (e.g. a `CASE` expression or a volatile function), and replacing the
/// subtree would silently return unfiltered rows. Bail and keep the central
/// join instead.
#[expect(
    deprecated,
    reason = "DF53 deprecates CoalesceBatchesExec (arrow BatchCoalescer); kept for plan-shape recognition"
)]
fn is_single_input_wrapper(plan: &dyn ExecutionPlan) -> bool {
    plan.downcast_ref::<RepartitionExec>().is_some()
        || plan.downcast_ref::<CoalesceBatchesExec>().is_some()
        || PASS_THROUGH_EXEC_NAMES.contains(&plan.name())
}

/// Build the per-executor join SQL. Returns `None` (bail) on any shape we can't
/// faithfully render.
fn build_join_sql(
    join: &HashJoinExec,
    fact: &FederatedSide,
    dim: &FederatedSide,
    fact_is_left: bool,
    addresses: &[String],
) -> Option<String> {
    let fact_fe = *fact.flight_execs.first()?;
    let dim_fe = *dim.flight_execs.first()?;

    // Fact subquery: the executor's own local scan (its partition), filters incl.
    let fact_sql = fact_fe.sql().ok()?;

    // Dimension gathered from every peer via executor_table, UNION ALL.
    let dim_table = dim_fe.table_reference().to_string();
    let dim_cols = dim
        .schema
        .fields()
        .iter()
        .map(|f| quote_ident(f.name()))
        .collect::<Vec<_>>()
        .join(", ");
    let dim_where = render_filters(dim_fe.filters())?;
    let dim_union = addresses
        .iter()
        .map(|addr| {
            format!(
                "SELECT {dim_cols} FROM executor_table('https://{addr}', '{dim_table}'){dim_where}"
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ");

    // ON clause: each equi-key pair is (left_col, right_col); assign to f/d by
    // which physical side is the fact. Track key pairs where both sides use the
    // SAME column name — those are the only names that may safely appear in
    // both scan schemas (see the projection attribution below).
    let mut on_terms = Vec::with_capacity(join.on().len());
    let mut same_named_key_pairs: std::collections::HashSet<&str> =
        std::collections::HashSet::new();
    for (l, r) in join.on() {
        let l_name = l.downcast_ref::<Column>()?.name();
        let r_name = r.downcast_ref::<Column>()?.name();
        if l_name == r_name {
            same_named_key_pairs.insert(l_name);
        }
        let (fact_col, dim_col) = if fact_is_left {
            (l_name, r_name)
        } else {
            (r_name, l_name)
        };
        on_terms.push(format!(
            "f.{} = d.{}",
            quote_ident(fact_col),
            quote_ident(dim_col)
        ));
    }
    let on_clause = on_terms.join(" AND ");

    // Output projection: map each output field to f.<col> or d.<col> by name.
    // A name present in BOTH scan schemas is ambiguous — the output field could
    // be either side's column, and attributing it to the wrong side silently
    // returns the other table's values. The one safe case is a name joined to
    // itself (`f.x = d.x`): the inner equi-join guarantees both sides carry
    // equal values on every output row, so either attribution is correct. Bail
    // on any other shared name and keep the central join.
    let fact_names: std::collections::HashSet<&str> = fact
        .schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let dim_names: std::collections::HashSet<&str> = dim
        .schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let mut select_items = Vec::with_capacity(join.schema().fields().len());
    for field in join.schema().fields() {
        let name = field.name();
        let qualified = match (
            fact_names.contains(name.as_str()),
            dim_names.contains(name.as_str()),
        ) {
            (true, true) if same_named_key_pairs.contains(name.as_str()) => {
                format!("f.{}", quote_ident(name))
            }
            (true, false) => format!("f.{}", quote_ident(name)),
            (false, true) => format!("d.{}", quote_ident(name)),
            // Ambiguous non-key name (both sides) — wrong attribution would
            // be silent wrong data; unattributable name (neither side) —
            // wrong schema. Bail on both rather than guess.
            (true, true) | (false, false) => return None,
        };
        select_items.push(format!("{qualified} AS {}", quote_ident(name)));
    }
    let select_list = select_items.join(", ");

    Some(format!(
        "SELECT {select_list} FROM ({fact_sql}) f INNER JOIN ({dim_union}) d ON {on_clause}"
    ))
}

fn render_filters(filters: &[datafusion::logical_expr::Expr]) -> Option<String> {
    if filters.is_empty() {
        return Some(String::new());
    }
    let parts = filters
        .iter()
        .map(|f| to_sql_preserving_precedence(f).map(|s| format!("({s})")))
        .collect::<std::result::Result<Vec<_>, _>>()
        .ok()?;
    Some(format!(" WHERE {}", parts.join(" AND ")))
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Cast a batch returned by the remote executor to the expected output schema.
/// Column count and order already match the generated `SELECT`; only types may
/// differ (e.g. the executor returns `LargeUtf8` where the plan expects
/// `Utf8View`), so cast column-by-column.
fn coerce_batch(batch: RecordBatch, target: &SchemaRef) -> Result<RecordBatch> {
    if batch.schema().as_ref() == target.as_ref() {
        return Ok(batch);
    }
    if batch.num_columns() != target.fields().len() {
        return Err(DataFusionError::Internal(format!(
            "BroadcastJoinFlightSqlExec: result has {} columns, expected {}",
            batch.num_columns(),
            target.fields().len()
        )));
    }
    let columns = target
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let col = batch.column(i);
            if col.data_type() == field.data_type() {
                Ok(Arc::clone(col))
            } else {
                cast(col, field.data_type())
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
            }
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(Arc::clone(target), columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_flight::sql::client::FlightSqlServiceClient;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::stats::Precision;
    use datafusion::physical_expr::expressions::IsNotNullExpr;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::joins::PartitionMode;
    use datafusion::sql::TableReference;
    use tonic::transport::Channel;

    fn dummy_client() -> FlightSqlClient {
        let channel = Channel::from_static("http://[::1]:1").connect_lazy();
        let cookie_svc =
            flight_client::cookie::CookieService::new(channel, Arc::new(CookieStore::new()));
        FlightSqlServiceClient::new(cookie_svc)
    }

    /// A leaf scan with an exact row-count statistic, so the broadcast cost
    /// gate (`dim_rows × executors × margin < fact_rows`) can be evaluated.
    fn flight_exec(schema: &SchemaRef, table: &str, rows: usize) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            FlightSqlExec::new(
                None,
                schema,
                &TableReference::parse_str(table),
                dummy_client(),
                &[],
                None,
                Arc::new(CookieStore::new()),
            )
            .expect("build FlightSqlExec")
            .with_statistics(Statistics::new_unknown(schema).with_num_rows(Precision::Exact(rows))),
        )
    }

    fn schema_of(fields: &[(&str, DataType)]) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .iter()
                .map(|(name, dt)| Field::new(*name, dt.clone(), true))
                .collect::<Vec<_>>(),
        ))
    }

    fn inner_hash_join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: Vec<(&str, usize, &str, usize)>,
        null_equality: NullEquality,
    ) -> Arc<dyn ExecutionPlan> {
        let on: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> = on
            .into_iter()
            .map(|(l_name, l_idx, r_name, r_idx)| {
                (
                    Arc::new(Column::new(l_name, l_idx)) as Arc<dyn PhysicalExpr>,
                    Arc::new(Column::new(r_name, r_idx)) as Arc<dyn PhysicalExpr>,
                )
            })
            .collect();
        Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                on,
                None,
                &JoinType::Inner,
                None,
                PartitionMode::CollectLeft,
                null_equality,
                false,
            )
            .expect("valid HashJoinExec"),
        )
    }

    /// The rewritten SQL when the rewrite fired with a single fact partition
    /// (the root is then the `BroadcastJoinFlightSqlExec` itself).
    fn broadcast_sql(plan: &Arc<dyn ExecutionPlan>) -> Option<String> {
        plan.downcast_ref::<BroadcastJoinFlightSqlExec>()
            .map(|b| b.sql.clone())
    }

    const ADDR: &str = "e0:50052";

    #[tokio::test]
    async fn rewrites_small_dim_join_and_attributes_columns_by_side() {
        let fact_schema = schema_of(&[("f_key", DataType::Int64), ("f_val", DataType::Int64)]);
        let dim_schema = schema_of(&[("d_key", DataType::Int64), ("d_name", DataType::Utf8)]);
        let join = inner_hash_join(
            flight_exec(&fact_schema, "cat.sch.fact", 1_000),
            flight_exec(&dim_schema, "cat.sch.dim", 10),
            vec![("f_key", 0, "d_key", 0)],
            NullEquality::NullEqualsNothing,
        );

        let result =
            try_rewrite(join, &[ADDR.to_string()], 25_000_000).expect("rewrite should not error");
        assert!(result.transformed, "small-dim inner join must broadcast");
        let sql = broadcast_sql(&result.data).expect("root should be the broadcast exec");

        assert!(
            sql.contains(r#"f."f_val" AS "f_val""#),
            "fact column must come from the fact side: {sql}"
        );
        assert!(
            sql.contains(r#"d."d_name" AS "d_name""#),
            "dim column must come from the dim side: {sql}"
        );
        assert!(
            sql.contains(r#"ON f."f_key" = d."d_key""#),
            "equi-key pair must join fact to dim: {sql}"
        );
        assert!(
            sql.contains(&format!("executor_table('https://{ADDR}', 'cat.sch.dim')")),
            "dim must be gathered from the executor: {sql}"
        );
    }

    #[tokio::test]
    async fn bails_on_column_name_shared_by_both_sides() {
        // Both scans expose a non-key column named `note`. The join output
        // contains two `note` fields (one per side); attributing them by name
        // alone would return the fact's values for the dim's column. The
        // rewrite must leave the plan alone.
        let fact_schema = schema_of(&[("k", DataType::Int64), ("note", DataType::Utf8)]);
        let dim_schema = schema_of(&[("d_key", DataType::Int64), ("note", DataType::Utf8)]);
        let join = inner_hash_join(
            flight_exec(&fact_schema, "cat.sch.fact", 1_000),
            flight_exec(&dim_schema, "cat.sch.dim", 10),
            vec![("k", 0, "d_key", 0)],
            NullEquality::NullEqualsNothing,
        );

        let result =
            try_rewrite(join, &[ADDR.to_string()], 25_000_000).expect("rewrite should not error");
        assert!(
            !result.transformed,
            "ambiguous shared column name must not broadcast"
        );
    }

    #[tokio::test]
    async fn allows_shared_name_when_it_is_the_equi_join_key() {
        // `id` exists on both sides but is the equi-join key joined to itself,
        // so both output copies provably carry equal values — attribution to
        // the fact side is safe and the rewrite may fire.
        let fact_schema = schema_of(&[("id", DataType::Int64), ("f_val", DataType::Int64)]);
        let dim_schema = schema_of(&[("id", DataType::Int64), ("d_name", DataType::Utf8)]);
        let join = inner_hash_join(
            flight_exec(&fact_schema, "cat.sch.fact", 1_000),
            flight_exec(&dim_schema, "cat.sch.dim", 10),
            vec![("id", 0, "id", 0)],
            NullEquality::NullEqualsNothing,
        );

        let result =
            try_rewrite(join, &[ADDR.to_string()], 25_000_000).expect("rewrite should not error");
        assert!(
            result.transformed,
            "same-named equi-key pair is unambiguous and must broadcast"
        );
        let sql = broadcast_sql(&result.data).expect("root should be the broadcast exec");
        assert!(
            sql.contains(r#"f."id" AS "id""#),
            "key columns attribute to the fact side: {sql}"
        );
        assert!(
            !sql.contains(r#"d."id" AS "id""#),
            "key columns must not be attributed to the dim side: {sql}"
        );
    }

    #[tokio::test]
    async fn bails_on_null_equals_null_join() {
        // `IS NOT DISTINCT FROM` joins match NULL keys; SQL `=` does not, so
        // rendering the join as `f.k = d.k` would drop NULL-key matches.
        let fact_schema = schema_of(&[("f_key", DataType::Int64), ("f_val", DataType::Int64)]);
        let dim_schema = schema_of(&[("d_key", DataType::Int64), ("d_name", DataType::Utf8)]);
        let join = inner_hash_join(
            flight_exec(&fact_schema, "cat.sch.fact", 1_000),
            flight_exec(&dim_schema, "cat.sch.dim", 10),
            vec![("f_key", 0, "d_key", 0)],
            NullEquality::NullEqualsNull,
        );

        let result =
            try_rewrite(join, &[ADDR.to_string()], 25_000_000).expect("rewrite should not error");
        assert!(
            !result.transformed,
            "NullEqualsNull join must not be rewritten to SQL `=`"
        );
    }

    #[tokio::test]
    async fn bails_on_residual_filter_exec() {
        // A FilterExec above the scan carries a predicate the scan's SQL does
        // NOT apply (FlightSQLTable pushdown is Exact-or-Unsupported). The
        // rewrite would discard the join input subtree — including this filter
        // — so it must bail and keep the central join.
        let fact_schema = schema_of(&[("f_key", DataType::Int64), ("f_val", DataType::Int64)]);
        let dim_schema = schema_of(&[("d_key", DataType::Int64), ("d_name", DataType::Utf8)]);
        let dim = flight_exec(&dim_schema, "cat.sch.dim", 10);
        let predicate: Arc<dyn PhysicalExpr> =
            Arc::new(IsNotNullExpr::new(Arc::new(Column::new("d_name", 1))));
        let filtered: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, dim).expect("valid FilterExec"));
        let join = inner_hash_join(
            flight_exec(&fact_schema, "cat.sch.fact", 1_000),
            filtered,
            vec![("f_key", 0, "d_key", 0)],
            NullEquality::NullEqualsNothing,
        );

        let result =
            try_rewrite(join, &[ADDR.to_string()], 25_000_000).expect("rewrite should not error");
        assert!(
            !result.transformed,
            "a residual FilterExec must block the broadcast rewrite"
        );
    }

    #[tokio::test]
    async fn bails_on_filter_exec_inside_fact_union_leg() {
        // The residual FilterExec can also hide inside one leg of the fact's
        // partition UNION rather than directly above a scan. `resolve_federated_side`
        // must still refuse to walk through it, so the rewrite bails and the
        // un-pushed predicate is preserved by the central join.
        let fact_schema = schema_of(&[("f_key", DataType::Int64), ("f_val", DataType::Int64)]);
        let dim_schema = schema_of(&[("d_key", DataType::Int64), ("d_name", DataType::Utf8)]);
        let predicate: Arc<dyn PhysicalExpr> =
            Arc::new(IsNotNullExpr::new(Arc::new(Column::new("f_val", 1))));
        let filtered_leg: Arc<dyn ExecutionPlan> = Arc::new(
            FilterExec::try_new(predicate, flight_exec(&fact_schema, "cat.sch.fact", 1_000))
                .expect("valid FilterExec"),
        );
        let fact = UnionExec::try_new(vec![
            filtered_leg,
            flight_exec(&fact_schema, "cat.sch.fact", 1_000),
        ])
        .expect("valid UnionExec");
        let join = inner_hash_join(
            fact,
            flight_exec(&dim_schema, "cat.sch.dim", 10),
            vec![("f_key", 0, "d_key", 0)],
            NullEquality::NullEqualsNothing,
        );

        let result =
            try_rewrite(join, &[ADDR.to_string()], 25_000_000).expect("rewrite should not error");
        assert!(
            !result.transformed,
            "a FilterExec inside a fact union leg must block the broadcast rewrite"
        );
    }
}
