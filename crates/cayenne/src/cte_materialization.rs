/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Materialize multi-reference CTEs once on the Cayenne query path.
//!
//! `DataFusion` inlines `WITH` bodies, so a CTE used twice is planned and
//! executed twice. This rule finds `SubqueryAlias` copies that share a name and
//! an equivalent body, require real work (aggregation, join, window, distinct,
//! sort, unnest, or union), and scan a Cayenne-accelerated table. It rewrites
//! them to a producer/consumer pair:
//!
//! ```text
//! MaterializedCte: name=expensive
//!   <CTE body>                          -- computed once
//!   Join / rest of query
//!     CteScan: name=expensive           -- reads the buffer
//!     CteScan: name=expensive
//! ```
//!
//! Simple pass-through CTEs (`SELECT * FROM t`) are left inlined so projection
//! pushdown can still prune columns. Recursive CTEs already have
//! `RecursiveQuery` / `WorkTableExec` and are left alone.
//!
//! The buffer is charged to the query memory pool. The first operator to run
//! (`MaterializedCteExec` or a `CteScanExec`) computes the CTE; later scans
//! reuse that buffer. `DataFusion`'s `ScalarSubqueryExec` evaluates subquery
//! children before its input, so a scan may run before the producer parent.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchemaRef, Result, exec_err, internal_err, plan_err};
use datafusion::datasource::DefaultTableSource;
use datafusion::error::Result as DFResult;
use datafusion::execution::SessionState;
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::logical_expr::{
    Extension, LogicalPlan, TableSource, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream,
};
use parking_lot::RwLock;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::Expr;
use futures::future::BoxFuture;
use futures::{Stream, StreamExt};
use tokio::sync::OnceCell;

use crate::logical_optimizer::PROPAGATED_FILTER_ALIAS_PREFIX;
use crate::provider::CayenneTableProvider;

type TableProviderPredicate = Arc<dyn Fn(&dyn TableProvider) -> bool + Send + Sync>;
type TableSourcePredicate = Arc<dyn Fn(&dyn TableSource) -> bool + Send + Sync>;

/// Explain / node name for the producer/consumer wrapper.
pub const MATERIALIZED_CTE_NODE_NAME: &str = "MaterializedCte";
/// Explain / node name for a scan of a materialized CTE buffer.
pub const CTE_SCAN_NODE_NAME: &str = "CteScan";

const RULE_NAME: &str = "cayenne_cte_materialization";

/// Logical optimizer that materializes multi-reference CTEs on the Cayenne path.
pub struct CayenneCteMaterialization {
    is_cayenne_table_source: TableSourcePredicate,
}

impl Default for CayenneCteMaterialization {
    fn default() -> Self {
        Self::new()
    }
}

impl CayenneCteMaterialization {
    /// Create a rule that recognizes direct [`CayenneTableProvider`] scans.
    #[must_use]
    pub fn new() -> Self {
        Self::new_with_table_provider_predicate(<dyn TableProvider>::is::<CayenneTableProvider>)
    }

    /// Create a rule with a caller-provided table-provider predicate.
    ///
    /// Runtime registration uses this to recognize `AcceleratedTable`s whose
    /// inner accelerator is Cayenne.
    #[must_use]
    pub fn new_with_table_provider_predicate(
        is_cayenne_table_provider: impl Fn(&dyn TableProvider) -> bool + Send + Sync + 'static,
    ) -> Self {
        let is_cayenne_table_provider: TableProviderPredicate = Arc::new(is_cayenne_table_provider);
        Self::new_with_table_source_predicate(move |source| {
            source
                .downcast_ref::<DefaultTableSource>()
                .is_some_and(|source| is_cayenne_table_provider(source.table_provider.as_ref()))
        })
    }

    /// Create a rule with a caller-provided table-source predicate.
    #[must_use]
    pub fn new_with_table_source_predicate(
        is_cayenne_table_source: impl Fn(&dyn TableSource) -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            is_cayenne_table_source: Arc::new(is_cayenne_table_source),
        }
    }
}

impl fmt::Debug for CayenneCteMaterialization {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CayenneCteMaterialization").finish()
    }
}

impl OptimizerRule for CayenneCteMaterialization {
    fn name(&self) -> &'static str {
        RULE_NAME
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let Some(candidate) = self.find_candidate(&plan)? else {
            return Ok(Transformed::no(plan));
        };

        tracing::debug!(
            cte = candidate.name.as_str(),
            "Materializing CTE once for reuse on the Cayenne query path"
        );
        materialize_candidate(plan, candidate).map(Transformed::yes)
    }
}

struct CteCandidate {
    name: String,
    body: LogicalPlan,
    schema: DFSchemaRef,
}

impl CayenneCteMaterialization {
    fn find_candidate(&self, plan: &LogicalPlan) -> Result<Option<CteCandidate>> {
        let mut by_name: HashMap<String, Vec<(LogicalPlan, DFSchemaRef)>> = HashMap::new();

        // Walk `IN` / scalar / `EXISTS` subqueries too. `plan.apply` only
        // follows `inputs()`, so `FROM cte WHERE x = (SELECT max(x) FROM cte)`
        // (TPC-H Q15) would otherwise look like a single reference.
        plan.apply_with_subqueries(|node| {
            if matches!(node, LogicalPlan::RecursiveQuery(_)) {
                return Ok(TreeNodeRecursion::Jump);
            }
            if let LogicalPlan::SubqueryAlias(alias) = node {
                let name = alias.alias.table();
                if should_skip_alias(name) {
                    return Ok(TreeNodeRecursion::Continue);
                }
                by_name
                    .entry(name.to_string())
                    .or_default()
                    .push((alias.input.as_ref().clone(), Arc::clone(&alias.schema)));
            }
            Ok(TreeNodeRecursion::Continue)
        })?;

        let mut chosen: Option<CteCandidate> = None;
        for (name, copies) in by_name {
            if copies.len() < 2 {
                continue;
            }
            let Some((body, schema)) = copies.first() else {
                continue;
            };
            if copies.iter().any(|(other, _)| other != body) {
                continue;
            }
            if contains_recursive(body)? || contains_cte_scan(body)? {
                continue;
            }
            if !is_expensive(body)? {
                continue;
            }
            if !self.contains_cayenne(body)? {
                continue;
            }
            let candidate = CteCandidate {
                name,
                body: body.clone(),
                schema: Arc::clone(schema),
            };
            // Prefer an inner CTE (smaller body) so nested WITH clauses
            // materialize from the inside out across optimizer passes.
            let take = chosen.as_ref().is_none_or(|current| {
                plan_node_count(&candidate.body) < plan_node_count(&current.body)
            });
            if take {
                chosen = Some(candidate);
            }
        }
        Ok(chosen)
    }

    fn contains_cayenne(&self, plan: &LogicalPlan) -> Result<bool> {
        plan.exists(|node| {
            Ok(if let LogicalPlan::TableScan(scan) = node {
                (self.is_cayenne_table_source)(scan.source.as_ref())
            } else {
                false
            })
        })
    }
}

fn should_skip_alias(name: &str) -> bool {
    name.starts_with("__") || name.starts_with(PROPAGATED_FILTER_ALIAS_PREFIX)
}

fn is_expensive(plan: &LogicalPlan) -> Result<bool> {
    plan.exists(|node| {
        Ok(matches!(
            node,
            LogicalPlan::Aggregate(_)
                | LogicalPlan::Join(_)
                | LogicalPlan::Window(_)
                | LogicalPlan::Distinct(_)
                | LogicalPlan::Sort(_)
                | LogicalPlan::Unnest(_)
                | LogicalPlan::Union(_)
        ))
    })
}

fn contains_recursive(plan: &LogicalPlan) -> Result<bool> {
    plan.exists(|node| Ok(matches!(node, LogicalPlan::RecursiveQuery(_))))
}

fn contains_cte_scan(plan: &LogicalPlan) -> Result<bool> {
    plan.exists(|node| {
        Ok(if let LogicalPlan::Extension(extension) = node {
            extension
                .node
                .as_any()
                .downcast_ref::<CteScanNode>()
                .is_some()
        } else {
            false
        })
    })
}

fn plan_node_count(plan: &LogicalPlan) -> usize {
    let mut count = 0;
    let _ = plan.apply(|_node| {
        count += 1;
        Ok(TreeNodeRecursion::Continue)
    });
    count
}

fn materialize_candidate(plan: LogicalPlan, candidate: CteCandidate) -> Result<LogicalPlan> {
    let slot = Arc::new(MaterializedCteSlot::new(candidate.name.clone()));
    let scan_node = CteScanNode {
        name: candidate.name.clone(),
        schema: Arc::clone(&candidate.schema),
        slot: Arc::clone(&slot),
    };
    let scan_plan = LogicalPlan::Extension(Extension {
        node: Arc::new(scan_node),
    });

    let consumer = plan
        .transform_up_with_subqueries(|node| {
            let LogicalPlan::SubqueryAlias(alias) = &node else {
                return Ok(Transformed::no(node));
            };
            if alias.alias.table() != candidate.name {
                return Ok(Transformed::no(node));
            }
            if alias.input.as_ref() != &candidate.body {
                return Ok(Transformed::no(node));
            }
            Ok(Transformed::yes(scan_plan.clone()))
        })?
        .data;

    let materialized = MaterializedCteNode {
        name: candidate.name,
        producer: candidate.body,
        consumer,
        slot,
    };
    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(materialized),
    }))
}

/// Shared buffer filled on first execution of a [`MaterializedCteExec`].
#[derive(Debug)]
struct MaterializedCteSlot {
    name: String,
    producer: RwLock<Option<Arc<dyn ExecutionPlan>>>,
    cell: OnceCell<Arc<MaterializedCteData>>,
}

impl MaterializedCteSlot {
    fn new(name: String) -> Self {
        Self {
            name,
            producer: RwLock::new(None),
            cell: OnceCell::new(),
        }
    }

    fn set_producer(&self, producer: Arc<dyn ExecutionPlan>) {
        *self.producer.write() = Some(producer);
    }
}

struct MaterializedCteData {
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
    /// Held so the query memory pool accounts the buffered CTE for the
    /// lifetime of the physical plan instance.
    _reservation: MemoryReservation,
}

impl fmt::Debug for MaterializedCteData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MaterializedCteData")
            .field("batches", &self.batches.len())
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

/// Producer/consumer wrapper: input 0 is the CTE body, input 1 is the query
/// that reads it via [`CteScanNode`].
#[derive(Debug)]
struct MaterializedCteNode {
    name: String,
    producer: LogicalPlan,
    consumer: LogicalPlan,
    slot: Arc<MaterializedCteSlot>,
}

impl Hash for MaterializedCteNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.producer.hash(state);
        self.consumer.hash(state);
    }
}

impl PartialEq for MaterializedCteNode {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.producer == other.producer
            && self.consumer == other.consumer
    }
}

impl Eq for MaterializedCteNode {}

impl PartialOrd for MaterializedCteNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.name.partial_cmp(&other.name)
    }
}

impl UserDefinedLogicalNodeCore for MaterializedCteNode {
    fn name(&self) -> &'static str {
        MATERIALIZED_CTE_NODE_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.producer, &self.consumer]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.consumer.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{MATERIALIZED_CTE_NODE_NAME}: name={}", self.name)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !exprs.is_empty() {
            return plan_err!("{MATERIALIZED_CTE_NODE_NAME} does not take expressions");
        }
        let mut inputs = inputs.into_iter();
        let Some(producer) = inputs.next() else {
            return plan_err!("{MATERIALIZED_CTE_NODE_NAME} requires a producer and a consumer");
        };
        let Some(consumer) = inputs.next() else {
            return plan_err!("{MATERIALIZED_CTE_NODE_NAME} requires a producer and a consumer");
        };
        if inputs.next().is_some() {
            return plan_err!("{MATERIALIZED_CTE_NODE_NAME} requires exactly two inputs");
        }
        Ok(Self {
            name: self.name.clone(),
            producer,
            consumer,
            slot: Arc::clone(&self.slot),
        })
    }
}

/// Leaf that reads a buffer produced by the enclosing [`MaterializedCteNode`].
#[derive(Debug)]
struct CteScanNode {
    name: String,
    schema: DFSchemaRef,
    slot: Arc<MaterializedCteSlot>,
}

impl Hash for CteScanNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.schema.hash(state);
    }
}

impl PartialEq for CteScanNode {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.schema == other.schema
    }
}

impl Eq for CteScanNode {}

impl PartialOrd for CteScanNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.name.partial_cmp(&other.name)
    }
}

impl UserDefinedLogicalNodeCore for CteScanNode {
    fn name(&self) -> &'static str {
        CTE_SCAN_NODE_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{CTE_SCAN_NODE_NAME}: name={}", self.name)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !exprs.is_empty() || !inputs.is_empty() {
            return plan_err!("{CTE_SCAN_NODE_NAME} is a leaf and takes no expressions or inputs");
        }
        Ok(Self {
            name: self.name.clone(),
            schema: Arc::clone(&self.schema),
            slot: Arc::clone(&self.slot),
        })
    }
}

/// Plans [`MaterializedCteNode`] / [`CteScanNode`] into physical operators.
#[derive(Debug, Default)]
pub struct CayenneCteMaterializationPlanner;

#[async_trait]
impl ExtensionPlanner for CayenneCteMaterializationPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(materialized) = node.as_any().downcast_ref::<MaterializedCteNode>() {
            if physical_inputs.len() != 2 {
                return internal_err!(
                    "{MATERIALIZED_CTE_NODE_NAME} expected 2 physical inputs, got {}",
                    physical_inputs.len()
                );
            }
            let producer = Arc::clone(&physical_inputs[0]);
            let consumer = Arc::clone(&physical_inputs[1]);
            return Ok(Some(Arc::new(MaterializedCteExec::new(
                materialized.name.clone(),
                producer,
                consumer,
                Arc::clone(&materialized.slot),
            ))));
        }

        if let Some(scan) = node.as_any().downcast_ref::<CteScanNode>() {
            if !physical_inputs.is_empty() {
                return internal_err!("{CTE_SCAN_NODE_NAME} expected 0 physical inputs");
            }
            return Ok(Some(Arc::new(CteScanExec::new(
                scan.name.clone(),
                Arc::clone(scan.schema.inner()),
                Arc::clone(&scan.slot),
            ))));
        }

        Ok(None)
    }
}

#[derive(Debug)]
struct MaterializedCteExec {
    name: String,
    producer: Arc<dyn ExecutionPlan>,
    consumer: Arc<dyn ExecutionPlan>,
    slot: Arc<MaterializedCteSlot>,
}

impl MaterializedCteExec {
    fn new(
        name: String,
        producer: Arc<dyn ExecutionPlan>,
        consumer: Arc<dyn ExecutionPlan>,
        slot: Arc<MaterializedCteSlot>,
    ) -> Self {
        slot.set_producer(Arc::clone(&producer));
        Self {
            name,
            producer,
            consumer,
            slot,
        }
    }
}

impl DisplayAs for MaterializedCteExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "MaterializedCteExec: name={}", self.name)
            }
            DisplayFormatType::TreeRender => write!(f, "name={}", self.name),
        }
    }
}

impl ExecutionPlan for MaterializedCteExec {
    fn name(&self) -> &'static str {
        "MaterializedCteExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.consumer.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.producer, &self.consumer]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false, true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false, false]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 2 {
            return internal_err!("MaterializedCteExec requires exactly two children");
        }
        Ok(Arc::new(Self::new(
            self.name.clone(),
            Arc::clone(&children[0]),
            Arc::clone(&children[1]),
            Arc::clone(&self.slot),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let consumer = Arc::clone(&self.consumer);
        let slot = Arc::clone(&self.slot);
        let ctx = Arc::clone(&context);
        let materialize: BoxFuture<'static, Result<Arc<MaterializedCteData>>> =
            Box::pin(async move { ensure_materialized(slot, ctx).await });

        Ok(Box::pin(MaterializedCteStream {
            materialize: Some(materialize),
            consumer,
            context,
            partition,
            inner: None,
            schema: self.consumer.schema(),
        }))
    }
}

struct MaterializedCteStream {
    materialize: Option<BoxFuture<'static, Result<Arc<MaterializedCteData>>>>,
    consumer: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    partition: usize,
    inner: Option<SendableRecordBatchStream>,
    schema: SchemaRef,
}

impl Stream for MaterializedCteStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(inner) = self.inner.as_mut() {
                return inner.as_mut().poll_next(cx);
            }

            let Some(fut) = self.materialize.as_mut() else {
                return Poll::Ready(Some(internal_err!(
                    "MaterializedCteStream polled after materialization future was dropped"
                )));
            };

            match fut.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => {
                    self.materialize = None;
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(Ok(_)) => {
                    self.materialize = None;
                    match self
                        .consumer
                        .execute(self.partition, Arc::clone(&self.context))
                    {
                        Ok(stream) => self.inner = Some(stream),
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    }
                }
            }
        }
    }
}

impl RecordBatchStream for MaterializedCteStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

async fn ensure_materialized(
    slot: Arc<MaterializedCteSlot>,
    context: Arc<TaskContext>,
) -> Result<Arc<MaterializedCteData>> {
    let data = slot
        .cell
        .get_or_try_init(|| {
            let name = slot.name.clone();
            let producer = slot.producer.read().as_ref().map(Arc::clone);
            async move {
                let Some(producer) = producer else {
                    return exec_err!(
                        "CTE '{name}' was scanned before its producer was planned"
                    );
                };
                collect_producer(producer, context, name).await
            }
        })
        .await?;
    Ok(Arc::clone(data))
}

async fn collect_producer(
    producer: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    name: String,
) -> Result<Arc<MaterializedCteData>> {
    let reservation =
        MemoryConsumer::new(format!("MaterializedCte:{name}")).register(context.memory_pool());
    let schema = producer.schema();
    let partition_count = producer.output_partitioning().partition_count();
    let mut batches = Vec::new();

    for partition in 0..partition_count {
        let mut stream = producer.execute(partition, Arc::clone(&context))?;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            reservation.try_grow(batch.get_array_memory_size())?;
            batches.push(batch);
        }
    }

    tracing::debug!(
        cte = name.as_str(),
        batches = batches.len(),
        "Finished materializing CTE"
    );

    Ok(Arc::new(MaterializedCteData {
        batches,
        schema,
        _reservation: reservation,
    }))
}

#[derive(Debug)]
struct CteScanExec {
    name: String,
    slot: Arc<MaterializedCteSlot>,
    cache: Arc<PlanProperties>,
}

impl CteScanExec {
    fn new(name: String, schema: SchemaRef, slot: Arc<MaterializedCteSlot>) -> Self {
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self { name, slot, cache }
    }
}

impl DisplayAs for CteScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "CteScanExec: name={}", self.name)
            }
            DisplayFormatType::TreeRender => write!(f, "name={}", self.name),
        }
    }
}

impl ExecutionPlan for CteScanExec {
    fn name(&self) -> &'static str {
        "CteScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            internal_err!("CteScanExec is a leaf")
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!(
                "CteScanExec for CTE '{}' got partition {partition} (expected 0)",
                self.name
            );
        }
        let slot = Arc::clone(&self.slot);
        let schema = self.schema();
        let load: BoxFuture<'static, Result<Arc<MaterializedCteData>>> =
            Box::pin(async move { ensure_materialized(slot, context).await });
        Ok(Box::pin(CteScanStream {
            load: Some(load),
            data: None,
            index: 0,
            schema,
        }))
    }
}

struct CteScanStream {
    load: Option<BoxFuture<'static, Result<Arc<MaterializedCteData>>>>,
    data: Option<Arc<MaterializedCteData>>,
    index: usize,
    schema: SchemaRef,
}

impl Stream for CteScanStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(batch) = self
                .data
                .as_ref()
                .and_then(|data| data.batches.get(self.index).cloned())
            {
                self.index += 1;
                return Poll::Ready(Some(Ok(batch)));
            }
            if self.data.is_some() {
                return Poll::Ready(None);
            }

            let Some(fut) = self.load.as_mut() else {
                return Poll::Ready(Some(internal_err!(
                    "CteScanStream polled after the load future was dropped"
                )));
            };

            match fut.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => {
                    self.load = None;
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(Ok(data)) => {
                    self.load = None;
                    self.data = Some(data);
                }
            }
        }
    }
}

impl RecordBatchStream for CteScanStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::MemTable;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::optimizer::Optimizer;
    use datafusion::prelude::SessionContext;
    use runtime_datafusion::extension::ExtensionPlanQueryPlanner;

    fn rule() -> CayenneCteMaterialization {
        CayenneCteMaterialization::new_with_table_source_predicate(|_| true)
    }

    fn batches() -> Result<Vec<RecordBatch>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("y", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 2, 2, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
                Arc::new(StringArray::from(vec!["a", "a", "b", "b", "c"])),
            ],
        )?;
        Ok(vec![batch])
    }

    async fn ctx_with_table() -> Result<SessionContext> {
        let ctx = SessionContext::new();
        let rows = batches()?;
        let schema = rows[0].schema();
        ctx.register_table("t", Arc::new(MemTable::try_new(schema, vec![rows])?))?;
        Ok(ctx)
    }

    async fn unoptimized(ctx: &SessionContext, sql: &str) -> Result<LogicalPlan> {
        Ok(ctx.sql(sql).await?.logical_plan().clone())
    }

    fn apply_rule(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        rule().rewrite(plan, &datafusion::optimizer::OptimizerContext::new())
    }

    fn plan_contains_node(plan: &LogicalPlan, name: &str) -> bool {
        let mut found = false;
        let _ = plan.apply_with_subqueries(|node| {
            if let LogicalPlan::Extension(extension) = node
                && extension.node.name() == name
            {
                found = true;
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        });
        found
    }

    fn count_aggregates(plan: &LogicalPlan) -> usize {
        let mut count = 0;
        let _ = plan.apply(|node| {
            if matches!(node, LogicalPlan::Aggregate(_)) {
                count += 1;
            }
            Ok(TreeNodeRecursion::Continue)
        });
        count
    }

    #[tokio::test]
    async fn multi_ref_aggregate_cte_is_materialized() -> Result<()> {
        let ctx = ctx_with_table().await?;
        let plan = unoptimized(
            &ctx,
            "WITH expensive AS (SELECT x, sum(y) AS s FROM t GROUP BY x) \
             SELECT a.x, b.s FROM expensive a JOIN expensive b ON a.x = b.x",
        )
        .await?;
        let rewritten = apply_rule(plan)?;
        assert!(
            rewritten.transformed,
            "expected a rewrite: {}",
            rewritten.data
        );
        assert!(
            plan_contains_node(&rewritten.data, MATERIALIZED_CTE_NODE_NAME),
            "missing MaterializedCte:\n{}",
            rewritten.data
        );
        assert!(
            plan_contains_node(&rewritten.data, CTE_SCAN_NODE_NAME),
            "missing CteScan:\n{}",
            rewritten.data
        );
        assert_eq!(
            count_aggregates(&rewritten.data),
            1,
            "aggregate should run once:\n{}",
            rewritten.data
        );
        Ok(())
    }

    #[tokio::test]
    async fn multi_ref_scalar_subquery_cte_is_materialized() -> Result<()> {
        let ctx = ctx_with_table().await?;
        let plan = unoptimized(
            &ctx,
            "WITH expensive AS (SELECT x, sum(y) AS s FROM t GROUP BY x) \
             SELECT x, s FROM expensive WHERE s = (SELECT max(s) FROM expensive)",
        )
        .await?;
        let rewritten = apply_rule(plan)?;
        assert!(
            rewritten.transformed,
            "expected a rewrite of a CTE used in FROM and in a scalar subquery: {}",
            rewritten.data
        );
        assert!(
            plan_contains_node(&rewritten.data, MATERIALIZED_CTE_NODE_NAME),
            "missing MaterializedCte:\n{}",
            rewritten.data
        );
        assert_eq!(
            count_aggregates(&rewritten.data),
            1,
            "GROUP BY aggregate should run once:\n{}",
            rewritten.data
        );
        Ok(())
    }

    #[tokio::test]
    async fn single_ref_cte_is_not_materialized() -> Result<()> {
        let ctx = ctx_with_table().await?;
        let plan = unoptimized(
            &ctx,
            "WITH expensive AS (SELECT x, sum(y) AS s FROM t GROUP BY x) \
             SELECT * FROM expensive",
        )
        .await?;
        let rewritten = apply_rule(plan)?;
        assert!(
            !rewritten.transformed,
            "single-ref CTE should stay inlined:\n{}",
            rewritten.data
        );
        Ok(())
    }

    #[tokio::test]
    async fn pass_through_cte_is_not_materialized() -> Result<()> {
        let ctx = ctx_with_table().await?;
        let plan = unoptimized(
            &ctx,
            "WITH base AS (SELECT * FROM t) \
             SELECT a.x FROM base a JOIN base b ON a.x = b.x",
        )
        .await?;
        let rewritten = apply_rule(plan)?;
        assert!(
            !rewritten.transformed,
            "pass-through CTE should stay inlined so projection pushdown can prune columns:\n{}",
            rewritten.data
        );
        Ok(())
    }

    #[tokio::test]
    async fn pass_through_scalar_subquery_cte_is_not_materialized() -> Result<()> {
        let ctx = ctx_with_table().await?;
        let plan = unoptimized(
            &ctx,
            "WITH base AS (SELECT * FROM t) \
             SELECT x FROM base WHERE x = (SELECT max(x) FROM base)",
        )
        .await?;
        let rewritten = apply_rule(plan)?;
        assert!(
            !rewritten.transformed,
            "pass-through CTE used in a scalar subquery should stay inlined:\n{}",
            rewritten.data
        );
        Ok(())
    }

    #[tokio::test]
    async fn non_cayenne_scans_are_skipped() -> Result<()> {
        let ctx = ctx_with_table().await?;
        let plan = unoptimized(
            &ctx,
            "WITH expensive AS (SELECT x, sum(y) AS s FROM t GROUP BY x) \
             SELECT a.x, b.s FROM expensive a JOIN expensive b ON a.x = b.x",
        )
        .await?;
        let default_rule = CayenneCteMaterialization::new();
        let rewritten =
            default_rule.rewrite(plan, &datafusion::optimizer::OptimizerContext::new())?;
        assert!(
            !rewritten.transformed,
            "default Cayenne predicate must not rewrite MemTable scans:\n{}",
            rewritten.data
        );
        Ok(())
    }

    fn session_with_rule() -> Result<SessionContext> {
        let mut rules = Optimizer::new().rules;
        rules.insert(0, Arc::new(rule()));
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_optimizer_rules(rules)
            .with_query_planner(Arc::new(
                ExtensionPlanQueryPlanner::from_extension_planners(vec![Arc::new(
                    CayenneCteMaterializationPlanner,
                )]),
            ))
            .build();
        Ok(SessionContext::new_with_state(state))
    }

    fn register_i64(ctx: &SessionContext, table: &str, col: &str, values: &[i64]) -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(col, DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )?;
        ctx.register_table(table, Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))?;
        Ok(())
    }

    fn register_nullable_i64(
        ctx: &SessionContext,
        table: &str,
        col: &str,
        values: &[Option<i64>],
    ) -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(col, DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )?;
        ctx.register_table(table, Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))?;
        Ok(())
    }

    async fn collect_sql(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
        Ok(ctx.sql(sql).await?.collect().await?)
    }

    async fn optimized_plan(ctx: &SessionContext, sql: &str) -> Result<LogicalPlan> {
        Ok(ctx.sql(sql).await?.into_optimized_plan()?)
    }

    fn assert_materialized(plan: &LogicalPlan, sql: &str) {
        assert!(
            plan_contains_node(plan, MATERIALIZED_CTE_NODE_NAME),
            "expected MaterializedCte for {sql}:\n{plan}"
        );
        assert!(
            plan_contains_node(plan, CTE_SCAN_NODE_NAME),
            "expected CteScan for {sql}:\n{plan}"
        );
    }

    fn assert_not_materialized(plan: &LogicalPlan, sql: &str) {
        assert!(
            !plan_contains_node(plan, MATERIALIZED_CTE_NODE_NAME),
            "pass-through or single-ref CTE should stay inlined for {sql}:\n{plan}"
        );
    }

    fn i64_col(batches: &[RecordBatch], col: usize) -> Vec<Option<i64>> {
        let mut out = Vec::new();
        for batch in batches {
            let array = batch
                .column(col)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 column");
            out.extend(array.iter());
        }
        out
    }

    fn utf8_col(batches: &[RecordBatch], col: usize) -> Vec<Option<String>> {
        let mut out = Vec::new();
        for batch in batches {
            let array = batch
                .column(col)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8 column");
            out.extend(array.iter().map(|value| value.map(str::to_owned)));
        }
        out
    }

    async fn materialized_sql(
        ctx: &SessionContext,
        sql: &str,
    ) -> Result<Vec<RecordBatch>> {
        let plan = optimized_plan(ctx, sql).await?;
        assert_materialized(&plan, sql);
        collect_sql(ctx, sql).await
    }

    async fn inlined_sql(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
        let plan = optimized_plan(ctx, sql).await?;
        assert_not_materialized(&plan, sql);
        collect_sql(ctx, sql).await
    }

    #[tokio::test]
    async fn materialized_cte_join_returns_correct_rows() -> Result<()> {
        let ctx = session_with_rule()?;
        let rows = batches()?;
        let schema = rows[0].schema();
        ctx.register_table("t", Arc::new(MemTable::try_new(schema, vec![rows])?))?;

        let plan = ctx
            .sql(
                "WITH expensive AS (SELECT x, sum(y) AS s FROM t GROUP BY x) \
                 SELECT a.x, a.s, b.s FROM expensive a JOIN expensive b ON a.x = b.x \
                 ORDER BY a.x",
            )
            .await?
            .into_optimized_plan()?;
        assert!(
            plan_contains_node(&plan, MATERIALIZED_CTE_NODE_NAME),
            "optimized plan should materialize the CTE:\n{plan}"
        );

        let results = ctx
            .sql(
                "WITH expensive AS (SELECT x, sum(y) AS s FROM t GROUP BY x) \
                 SELECT a.x, a.s, b.s FROM expensive a JOIN expensive b ON a.x = b.x \
                 ORDER BY a.x",
            )
            .await?
            .collect()
            .await?;
        assert_eq!(results.len(), 1);
        let batch = &results[0];
        let x = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("x");
        let s1 = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("s1");
        let s2 = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("s2");
        assert_eq!(x.values().as_ref(), &[1, 2, 3]);
        assert_eq!(s1.values().as_ref(), &[30, 70, 50]);
        assert_eq!(s2.values().as_ref(), &[30, 70, 50]);
        Ok(())
    }

    // Cases adapted from DuckDB `test/sql/cte/materialized/` (v2.0-cyanoptera).
    // DuckDB's `AS MATERIALIZED` is not SQL here; each query uses a
    // multi-reference expensive CTE so auto-materialize rewrites, then checks
    // DuckDB's expected values. Recursive CTEs, DML, EXPLAIN REGEX, `range()`,
    // and `AS [NOT] MATERIALIZED` syntax are not ported.

    #[tokio::test]
    async fn duckdb_multi_use_same_cte_cartesian() -> Result<()> {
        // test_cte_materialized.test: multiple uses of same CTE → 42, 42
        let ctx = session_with_rule()?;
        register_i64(&ctx, "a", "i", &[42])?;
        let sql = "WITH cte1 AS (SELECT i AS j, count(*) AS n FROM a GROUP BY i) \
                   SELECT cte11.j AS j1, cte12.j AS j2 FROM cte1 cte11, cte1 cte12";
        let batches = materialized_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(42)]);
        assert_eq!(i64_col(&batches, 1), vec![Some(42)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_cte_referenced_in_subquery() -> Result<()> {
        // test_cte_materialized.test: `j = (select max(j) from cte1)`
        let ctx = session_with_rule()?;
        register_i64(&ctx, "a", "i", &[42])?;
        let sql = "WITH cte1 AS (SELECT i AS j, count(*) AS n FROM a GROUP BY i) \
                   SELECT j FROM cte1 WHERE j = (SELECT max(j) FROM cte1)";
        let batches = materialized_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(42)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_chained_ctes_cross_join() -> Result<()> {
        // test_cte_materialized.test: cte1/cte2/cte3 → 42, 43
        let ctx = session_with_rule()?;
        register_i64(&ctx, "a", "i", &[42])?;
        let sql = "WITH cte1 AS (SELECT i AS j, count(*) AS n FROM a GROUP BY i), \
                        cte2 AS (SELECT j AS k FROM cte1), \
                        cte3 AS (SELECT j + 1 AS i FROM cte1) \
                   SELECT k, i FROM cte2, cte3";
        let batches = materialized_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(42)]);
        assert_eq!(i64_col(&batches, 1), vec![Some(43)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_chained_ctes_union_all() -> Result<()> {
        // test_cte_materialized.test: cte2 UNION ALL cte3 → 42, 43
        let ctx = session_with_rule()?;
        register_i64(&ctx, "a", "i", &[42])?;
        let sql = "WITH cte1 AS (SELECT i AS j, count(*) AS n FROM a GROUP BY i), \
                        cte2 AS (SELECT j AS k FROM cte1), \
                        cte3 AS (SELECT j + 1 AS i FROM cte1) \
                   SELECT k FROM cte2 UNION ALL SELECT i FROM cte3";
        let batches = materialized_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(42), Some(43)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_cte_column_aliases() -> Result<()> {
        // test_cte_materialized.test: `WITH cte1(xxx) AS ... SELECT xxx`
        let ctx = session_with_rule()?;
        register_i64(&ctx, "a", "i", &[42])?;
        let sql = "WITH cte1(xxx, n) AS (SELECT i, count(*) FROM a GROUP BY i) \
                   SELECT t1.xxx FROM cte1 t1 JOIN cte1 t2 ON t1.xxx = t2.xxx";
        let batches = materialized_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(42)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_union_all_two_refs() -> Result<()> {
        // test_cte_materialized.test: two reads of the same materialized CTE
        let ctx = session_with_rule()?;
        register_i64(&ctx, "a", "i", &[42])?;
        let sql = "WITH cte1 AS (SELECT i AS j, count(*) AS n FROM a GROUP BY i) \
                   SELECT j FROM cte1 UNION ALL SELECT j FROM cte1";
        let batches = materialized_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(42), Some(42)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_nested_with_inner_materialized() -> Result<()> {
        // test_cte_in_cte_materialized.test: WITH inside a CTE, inner used twice
        let ctx = session_with_rule()?;
        register_i64(&ctx, "a", "i", &[42])?;
        let sql = "WITH cte1 AS ( \
                     WITH b AS (SELECT i AS j, count(*) AS n FROM a GROUP BY i) \
                     SELECT b.j FROM b JOIN b AS b2 ON b.j = b2.j \
                   ) SELECT j FROM cte1";
        let batches = materialized_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(42)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_cte_in_subquery_tableref() -> Result<()> {
        // test_cte_in_cte_materialized.test: CTE in a subquery FROM, plus outer ref
        let ctx = session_with_rule()?;
        register_i64(&ctx, "a", "i", &[42])?;
        let sql = "WITH cte1 AS (SELECT i AS j, count(*) AS n FROM a GROUP BY i) \
                   SELECT f.j FROM ( \
                     WITH cte2 AS (SELECT max(j) AS j FROM cte1) \
                     SELECT cte2.j FROM cte2, cte1 \
                   ) f";
        let batches = materialized_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(42)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_generate_series_self_join_cartesian() -> Result<()> {
        // test_materialized_cte.test: generate_series(1,3) self-join → 9 rows
        let ctx = session_with_rule()?;
        register_i64(&ctx, "series", "i", &[1, 2, 3])?;
        let sql = "WITH t AS (SELECT i, count(*) AS n FROM series GROUP BY i) \
                   SELECT t1.i AS x, t2.i AS y FROM t t1, t t2 ORDER BY t1.i, t2.i";
        let batches = materialized_sql(&ctx, sql).await?;
        assert_eq!(
            i64_col(&batches, 0),
            vec![
                Some(1),
                Some(1),
                Some(1),
                Some(2),
                Some(2),
                Some(2),
                Some(3),
                Some(3),
                Some(3)
            ]
        );
        assert_eq!(
            i64_col(&batches, 1),
            vec![
                Some(1),
                Some(2),
                Some(3),
                Some(1),
                Some(2),
                Some(3),
                Some(1),
                Some(2),
                Some(3)
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_generate_series_self_join_sum() -> Result<()> {
        // test_materialized_cte.test: `sum(a.i + b.i)` over range(3) → 18
        let ctx = session_with_rule()?;
        register_i64(&ctx, "series", "i", &[0, 1, 2])?;
        let sql = "WITH t AS (SELECT i, count(*) AS n FROM series GROUP BY i) \
                   SELECT CAST(sum(a.i + b.i) AS BIGINT) AS s FROM t a, t b";
        let batches = materialized_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(18)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_nested_cte_wraps_inner() -> Result<()> {
        // test_materialized_cte.test: t wrapping u → 42
        let ctx = session_with_rule()?;
        register_i64(&ctx, "a", "i", &[42])?;
        let sql = "WITH t AS ( \
                     WITH u AS (SELECT i AS x, count(*) AS n FROM a GROUP BY i) \
                     SELECT u.x FROM u JOIN u u2 ON u.x = u2.x \
                   ) SELECT x FROM t";
        let batches = materialized_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(42)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_union_all_derived_and_base() -> Result<()> {
        // test_materialized_cte.test: `TABLE u UNION ALL TABLE t` → 2, 1
        let ctx = session_with_rule()?;
        register_i64(&ctx, "a", "i", &[1])?;
        let sql = "WITH t AS (SELECT i AS x, count(*) AS n FROM a GROUP BY i), \
                        u AS (SELECT x + 1 AS x FROM t) \
                   SELECT x FROM (SELECT x FROM u UNION ALL SELECT x FROM t) q \
                   ORDER BY x DESC";
        let batches = materialized_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(2), Some(1)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_full_outer_join_two_ctes() -> Result<()> {
        // test_materialized_cte.test: FULL OUTER JOIN of two CTEs → 2, 1
        let ctx = session_with_rule()?;
        register_i64(&ctx, "a", "i", &[1])?;
        let sql = "WITH t AS (SELECT i AS x, count(*) AS n FROM a GROUP BY i), \
                        u AS (SELECT i + 1 AS x, count(*) AS n FROM a GROUP BY i) \
                   SELECT u.x AS ux, t.x AS tx \
                   FROM u FULL OUTER JOIN t ON true \
                   WHERE (SELECT count(*) FROM u) > 0 AND (SELECT count(*) FROM t) > 0";
        let batches = materialized_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(2)]);
        assert_eq!(i64_col(&batches, 1), vec![Some(1)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_null_sum_and_count_scalars() -> Result<()> {
        // test_materialized_cte.test: four scalar refs of `d` over range(16)
        let ctx = session_with_rule()?;
        let series: Vec<i64> = (0..16).collect();
        register_i64(&ctx, "series", "i", &series)?;
        let sql = "WITH d AS ( \
                     SELECT DISTINCT i AS c0, \
                            CASE WHEN i % 4 = 0 THEN CAST(NULL AS BIGINT) ELSE i END AS c1 \
                     FROM series \
                   ) \
                   SELECT \
                     CAST((SELECT sum(c0) FROM d) AS BIGINT) AS sum_c0, \
                     CAST((SELECT sum(c1) FROM d) AS BIGINT) AS sum_c1, \
                     CAST((SELECT count(c0) FROM d) AS BIGINT) AS count_c0, \
                     CAST((SELECT count(c1) FROM d) AS BIGINT) AS count_c1";
        let batches = materialized_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(120)]);
        assert_eq!(i64_col(&batches, 1), vec![Some(96)]);
        assert_eq!(i64_col(&batches, 2), vec![Some(16)]);
        assert_eq!(i64_col(&batches, 3), vec![Some(12)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_correlated_subquery_two_refs() -> Result<()> {
        // test_materialized_cte.test: lhs vs rhs on the same CTE → 256
        let ctx = session_with_rule()?;
        let series: Vec<i64> = (0..16).collect();
        register_i64(&ctx, "series", "i", &series)?;
        let sql = "WITH d AS (SELECT DISTINCT i, i AS q FROM series) \
                   SELECT CAST(count(*) AS BIGINT) AS c \
                   FROM d lhs \
                   WHERE q < (SELECT avg(q) + 1 FROM d rhs WHERE rhs.i = lhs.i)";
        let batches = materialized_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(16)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_tpch_q15_revenue_cte() -> Result<()> {
        // automatic_cte_materialization.test_slow: TPC-H Q15 revenue CTE used
        // in FROM and in a max() subquery.
        let ctx = session_with_rule()?;
        let rows = batches()?;
        let schema = rows[0].schema();
        ctx.register_table("lineitem", Arc::new(MemTable::try_new(schema, vec![rows])?))?;
        let sql = "WITH revenue AS ( \
                     SELECT x AS supplier_no, sum(y) AS total_revenue \
                     FROM lineitem GROUP BY x \
                   ) \
                   SELECT supplier_no, CAST(total_revenue AS BIGINT) AS total_revenue \
                   FROM revenue \
                   WHERE total_revenue = (SELECT max(total_revenue) FROM revenue) \
                   ORDER BY supplier_no";
        let batches = materialized_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(2)]);
        assert_eq!(i64_col(&batches, 1), vec![Some(70)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_tpcds_q57_triple_self_join() -> Result<()> {
        // automatic_cte_materialization.test_slow: TPC-DS Q57 `v1` self-joined
        // three times after a window aggregate.
        let ctx = session_with_rule()?;
        let rows = batches()?;
        let schema = rows[0].schema();
        ctx.register_table("catalog_sales", Arc::new(MemTable::try_new(schema, vec![rows])?))?;
        let sql = "WITH v1 AS ( \
                     SELECT x AS i_category, sum(y) AS sum_sales, \
                            rank() OVER (PARTITION BY x ORDER BY x) AS rn \
                     FROM catalog_sales \
                     GROUP BY x \
                   ) \
                   SELECT a.i_category, CAST(a.sum_sales AS BIGINT) AS sum_sales \
                   FROM v1 a, v1 v1_lag, v1 v1_lead \
                   WHERE a.i_category = v1_lag.i_category \
                     AND a.i_category = v1_lead.i_category \
                     AND a.rn = v1_lag.rn \
                     AND a.rn = v1_lead.rn \
                   ORDER BY a.i_category";
        let batches = materialized_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(1), Some(2), Some(3)]);
        assert_eq!(i64_col(&batches, 1), vec![Some(30), Some(70), Some(50)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_filter_on_materialized_cte() -> Result<()> {
        // cte_filter_pusher.test: generate_series(1,10), x < 8, x % 3 = 1 → 1,4,7
        let ctx = session_with_rule()?;
        let series: Vec<i64> = (1..=10).collect();
        register_i64(&ctx, "series", "i", &series)?;
        let sql = "WITH a AS (SELECT DISTINCT i AS x FROM series) \
                   SELECT t1.x \
                   FROM a t1 JOIN a t2 ON t1.x = t2.x \
                   WHERE t1.x < 8 AND t1.x % 3 = 1 \
                   ORDER BY t1.x";
        let batches = materialized_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(1), Some(4), Some(7)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_filter_cross_join_or_predicate() -> Result<()> {
        // cte_filter_pusher.test: `v IN (1..6) OR s.x > 100` → 180
        let ctx = session_with_rule()?;
        let series: Vec<i64> = (0..100).collect();
        register_i64(&ctx, "series", "i", &series)?;
        register_nullable_i64(&ctx, "filter_ctx", "x", &[Some(10), Some(20), None])?;
        let sql = "WITH cte AS (SELECT DISTINCT i AS k, i % 10 AS v FROM series) \
                   SELECT CAST(count(*) AS BIGINT) AS c \
                   FROM cte c, filter_ctx s \
                   WHERE (c.v IN (1, 2, 3, 4, 5, 6) OR s.x > 100) \
                     AND (SELECT count(*) FROM cte) > 0";
        let batches = materialized_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(180)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_nulls_and_duplicates_projection() -> Result<()> {
        // cte_filter_pusher.test: NULLs and duplicate keys in a computed projection
        let ctx = session_with_rule()?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int64, true),
            Field::new("payload", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(1), None, Some(2)])),
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("b"),
                    Some("c"),
                    None,
                ])),
            ],
        )?;
        ctx.register_table("src", Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))?;
        let sql = "WITH cte AS ( \
                     SELECT DISTINCT i + 1 AS x, upper(payload) AS payload FROM src \
                   ) \
                   SELECT coalesce(t1.x, -1) AS x, coalesce(t1.payload, 'NULL') AS payload \
                   FROM cte t1 \
                   WHERE (t1.x = 2 OR t1.x IS NULL) \
                     AND (SELECT count(*) FROM cte) > 0 \
                   ORDER BY x, payload";
        let batches = materialized_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(-1), Some(2), Some(2)]);
        assert_eq!(
            utf8_col(&batches, 1),
            vec![
                Some("C".to_string()),
                Some("A".to_string()),
                Some("B".to_string())
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_issue_10260_join_cte() -> Result<()> {
        // test_issue_10260.test: JOIN inside a CTE that is then materialized
        let ctx = session_with_rule()?;
        register_i64(&ctx, "t0", "c1", &[1])?;
        register_i64(&ctx, "t1", "c1", &[1])?;
        let sql = "WITH cte AS ( \
                     SELECT t0.c1 AS c0, a1 \
                     FROM t0 \
                     LEFT JOIN (SELECT c1 AS a1 FROM t1) t1s ON t0.c1 = a1 \
                   ) \
                   SELECT c_left.a1 \
                   FROM cte c_left \
                   JOIN cte c_right ON c_left.a1 = c_right.a1";
        let batches = materialized_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(1)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_empty_cte_two_refs() -> Result<()> {
        // materialized_cte_order_preservation.test: empty producer, two consumers
        let ctx = session_with_rule()?;
        register_i64(&ctx, "a", "i", &[42])?;
        let sql = "WITH d AS (SELECT i FROM a WHERE i < 0 GROUP BY i) \
                   SELECT CAST((SELECT count(*) FROM d) AS BIGINT) AS c1, \
                          CAST((SELECT count(*) FROM d) AS BIGINT) AS c2";
        let batches = materialized_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(0)]);
        assert_eq!(i64_col(&batches, 1), vec![Some(0)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_sorted_limit_on_materialized_cte() -> Result<()> {
        // test_materialized_cte.test: DISTINCT + ORDER + LIMIT on generate_series
        let ctx = session_with_rule()?;
        let series: Vec<i64> = (1..=10).collect();
        register_i64(&ctx, "series", "i", &series)?;
        let sql = "WITH t AS (SELECT i FROM series WHERE i <= 4 GROUP BY i) \
                   SELECT t1.i FROM t t1 JOIN t t2 ON t1.i = t2.i \
                   ORDER BY t1.i DESC LIMIT 2";
        let batches = materialized_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(4), Some(3)]);
        Ok(())
    }

    // Cases adapted from DuckDB `test/sql/cte/` (v2.0-cyanoptera), the
    // inlined (non-`MATERIALIZED`) counterparts. Bodies are pass-through or
    // single-ref so auto-materialize must not fire; results still match
    // DuckDB. Recursive CTEs, DML, DESCRIBE/SUMMARIZE, storage back-compat,
    // and unused-CTE lazy bind are not ported.

    #[tokio::test]
    async fn duckdb_inlined_single_ref() -> Result<()> {
        // test_cte.test: `with cte1 as (Select i as j from a) select * from cte1`
        let ctx = session_with_rule()?;
        register_i64(&ctx, "a", "i", &[42])?;
        let sql = "WITH cte1 AS (SELECT i AS j FROM a) SELECT j FROM cte1";
        let batches = inlined_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(42)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_inlined_column_alias() -> Result<()> {
        // test_cte.test: `with cte1(xxx) as (Select i as j from a) select xxx`
        let ctx = session_with_rule()?;
        register_i64(&ctx, "a", "i", &[42])?;
        let sql = "WITH cte1(xxx) AS (SELECT i AS j FROM a) SELECT xxx FROM cte1";
        let batches = inlined_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(42)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_inlined_chained_ctes_cross_join() -> Result<()> {
        // test_cte.test: cte1/cte2/cte3 without MATERIALIZED → 42, 43
        let ctx = session_with_rule()?;
        register_i64(&ctx, "a", "i", &[42])?;
        let sql = "WITH cte1 AS (SELECT i AS j FROM a), \
                        cte2 AS (SELECT j AS k FROM cte1), \
                        cte3 AS (SELECT j + 1 AS i FROM cte1) \
                   SELECT k, i FROM cte2, cte3";
        let batches = inlined_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(42)]);
        assert_eq!(i64_col(&batches, 1), vec![Some(43)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_inlined_chained_ctes_union_all() -> Result<()> {
        // test_cte.test: cte2 UNION ALL cte3 → 42, 43
        let ctx = session_with_rule()?;
        register_i64(&ctx, "a", "i", &[42])?;
        let sql = "WITH cte1 AS (SELECT i AS j FROM a), \
                        cte2 AS (SELECT j AS k FROM cte1), \
                        cte3 AS (SELECT j + 1 AS i FROM cte1) \
                   SELECT k FROM cte2 UNION ALL SELECT i FROM cte3";
        let batches = inlined_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(42), Some(43)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_inlined_multi_use_same_cte_cartesian() -> Result<()> {
        // test_cte.test: multiple uses of a pass-through CTE → 42, 42
        let ctx = session_with_rule()?;
        register_i64(&ctx, "a", "i", &[42])?;
        let sql = "WITH cte1 AS (SELECT i AS j FROM a) \
                   SELECT cte11.j AS j1, cte12.j AS j2 FROM cte1 cte11, cte1 cte12";
        let batches = inlined_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(42)]);
        assert_eq!(i64_col(&batches, 1), vec![Some(42)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_inlined_cte_referenced_in_subquery() -> Result<()> {
        // test_cte.test: `j = (select max(j) from cte1)` on a pass-through CTE
        let ctx = session_with_rule()?;
        register_i64(&ctx, "a", "i", &[42])?;
        let sql = "WITH cte1 AS (SELECT i AS j FROM a) \
                   SELECT j FROM cte1 WHERE j = (SELECT max(j) FROM cte1)";
        let batches = inlined_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(42)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_inlined_union_all_two_refs() -> Result<()> {
        // test_cte.test: two reads of the same inlined CTE
        let ctx = session_with_rule()?;
        register_i64(&ctx, "a", "i", &[42])?;
        let sql = "WITH cte1 AS (SELECT i AS j FROM a) \
                   SELECT j FROM cte1 UNION ALL SELECT j FROM cte1";
        let batches = inlined_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(42), Some(42)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_inlined_cte_in_union_all_branch() -> Result<()> {
        // test_cte.test: `SELECT 1 UNION ALL (WITH cte AS (SELECT 42) SELECT * FROM cte)`
        let ctx = session_with_rule()?;
        register_i64(&ctx, "a", "i", &[42])?;
        let sql = "SELECT CAST(1 AS BIGINT) AS j \
                   UNION ALL (WITH cte AS (SELECT i AS j FROM a) SELECT j FROM cte)";
        let batches = inlined_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(1), Some(42)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_inlined_cte_in_nested_set_operation() -> Result<()> {
        // test_cte.test: CTE used twice inside a UNION ALL branch → 1, 42, 42
        let ctx = session_with_rule()?;
        register_i64(&ctx, "a", "i", &[42])?;
        let sql = "SELECT CAST(1 AS BIGINT) AS j UNION ALL ( \
                     WITH cte AS (SELECT i AS j FROM a) \
                     SELECT j FROM cte UNION ALL SELECT j FROM cte \
                   )";
        let batches = inlined_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(1), Some(42), Some(42)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_inlined_multi_column_alias() -> Result<()> {
        // test_cte.test: `with cte1(x, y) as (select 42 a, 84 b)`
        let ctx = session_with_rule()?;
        let sql = "WITH cte1(x, y) AS (SELECT CAST(42 AS BIGINT) AS a, CAST(84 AS BIGINT) AS b) \
                   SELECT x, y FROM cte1";
        let batches = inlined_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(42)]);
        assert_eq!(i64_col(&batches, 1), vec![Some(84)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_inlined_nested_with() -> Result<()> {
        // test_cte_in_cte.test: WITH inside a CTE
        let ctx = session_with_rule()?;
        register_i64(&ctx, "a", "i", &[42])?;
        let sql = "WITH cte1 AS ( \
                     WITH b AS (SELECT i AS j FROM a) \
                     SELECT j FROM b \
                   ) SELECT j FROM cte1";
        let batches = inlined_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(42)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_inlined_nested_with_column_aliases() -> Result<()> {
        // test_cte_in_cte.test: `with cte1(xxx) as (with ncte(yyy) as ...)`
        let ctx = session_with_rule()?;
        register_i64(&ctx, "a", "i", &[42])?;
        let sql = "WITH cte1(xxx) AS ( \
                     WITH ncte(yyy) AS (SELECT i AS j FROM a) \
                     SELECT yyy FROM ncte \
                   ) SELECT xxx FROM cte1";
        let batches = inlined_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(42)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_inlined_nested_with_cross_join() -> Result<()> {
        // test_cte_in_cte.test: nested WITH producing cte1 × cte2 → 42, 43
        let ctx = session_with_rule()?;
        register_i64(&ctx, "a", "i", &[42])?;
        let sql = "WITH cte1 AS ( \
                     WITH b AS (SELECT i AS j FROM a) SELECT j FROM b \
                   ), cte2 AS ( \
                     WITH c AS (SELECT j + 1 AS k FROM cte1) SELECT k FROM c \
                   ) SELECT j, k FROM cte1, cte2";
        let batches = inlined_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(42)]);
        assert_eq!(i64_col(&batches, 1), vec![Some(43)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_inlined_cte_in_subquery_tableref() -> Result<()> {
        // test_cte_in_cte.test: CTE in a subquery FROM
        let ctx = session_with_rule()?;
        register_i64(&ctx, "a", "i", &[42])?;
        let sql = "WITH cte1 AS (SELECT i AS j FROM a) \
                   SELECT j FROM ( \
                     WITH cte2 AS (SELECT max(j) AS j FROM cte1) \
                     SELECT j FROM cte2 \
                   ) f";
        let batches = inlined_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(42)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_inlined_cte_in_subquery_expression() -> Result<()> {
        // test_cte_in_cte.test: `j = (with cte2 as (select max(j) from cte1) ...)`
        let ctx = session_with_rule()?;
        register_i64(&ctx, "a", "i", &[42])?;
        let sql = "WITH cte1 AS (SELECT i AS j FROM a) \
                   SELECT j FROM cte1 \
                   WHERE j = (WITH cte2 AS (SELECT max(j) AS j FROM cte1) SELECT j FROM cte2)";
        let batches = inlined_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(42)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_inlined_bug_922_empty() -> Result<()> {
        // test_bug_922.test: VALUES + LIMIT 0 OFFSET 1 → no rows
        let ctx = session_with_rule()?;
        let sql = "WITH my_list(value) AS ( \
                     SELECT * FROM (VALUES \
                       (CAST(1 AS BIGINT)), \
                       (CAST(2 AS BIGINT)), \
                       (CAST(3 AS BIGINT)) \
                     ) AS v(value) \
                   ) SELECT value FROM my_list LIMIT 0 OFFSET 1";
        let batches = inlined_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), Vec::<Option<i64>>::new());
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_inlined_null_filter() -> Result<()> {
        // cte_null_values.test: NULL in a CTE, then a predicate that rejects it
        let ctx = session_with_rule()?;
        let sql = "WITH cte1 AS (SELECT CAST(NULL AS TIMESTAMP) AS y), \
                        cte1_filter AS ( \
                          SELECT y FROM cte1 \
                          WHERE y < CAST('2025-12-01' AS TIMESTAMP) \
                        ) \
                   SELECT y FROM cte1_filter";
        let batches = inlined_sql(&ctx, sql).await?;
        assert!(
            batches.iter().all(|batch| batch.num_rows() == 0),
            "NULL timestamp should not pass y < timestamp"
        );
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_inlined_issue_5673_name_shadow() -> Result<()> {
        // test_issue_5673.test: CTE named `orders` shadows the table of the same name
        let ctx = session_with_rule()?;
        register_i64(&ctx, "orders", "ordered_at", &[1])?;
        register_i64(&ctx, "stg_orders", "ordered_at", &[1])?;
        let sql = "WITH orders AS ( \
                     SELECT ordered_at FROM stg_orders \
                     WHERE ordered_at >= (SELECT max(ordered_at) FROM orders) \
                   ), some_more_logic AS ( \
                     SELECT ordered_at FROM orders \
                   ) \
                   SELECT ordered_at FROM some_more_logic";
        let batches = inlined_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(1)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_inlined_issue_10074_column_name() -> Result<()> {
        // cte_colname_issue_10074.test: join of two CTEs keeps `id`
        let ctx = session_with_rule()?;
        let sql = "WITH q AS (SELECT CAST(1 AS BIGINT) AS id, CAST(42 AS BIGINT) AS s), \
                        a AS (SELECT CAST(42 AS BIGINT) AS s) \
                   SELECT id FROM q JOIN a ON q.s = a.s";
        let batches = inlined_sql(&ctx, sql).await?;
        assert_eq!(i64_col(&batches, 0), vec![Some(1)]);
        Ok(())
    }

    #[tokio::test]
    async fn duckdb_inlined_schema_vs_cte_name() -> Result<()> {
        // cte_schema.test: table `s1.tbl` vs CTE `tbl` → hello, world
        let ctx = session_with_rule()?;
        ctx.sql("CREATE SCHEMA s1").await?.collect().await?;
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec!["hello"]))],
        )?;
        ctx.register_table(
            datafusion::common::TableReference::partial("s1", "tbl"),
            Arc::new(MemTable::try_new(schema, vec![vec![batch]])?),
        )?;
        let sql = "WITH tbl AS (SELECT 'world' AS b) SELECT a, b FROM s1.tbl, tbl";
        let batches = inlined_sql(&ctx, sql).await?;
        assert_eq!(utf8_col(&batches, 0), vec![Some("hello".to_string())]);
        assert_eq!(utf8_col(&batches, 1), vec![Some("world".to_string())]);
        Ok(())
    }
}
