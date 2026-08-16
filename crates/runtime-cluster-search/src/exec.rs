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

//! Physical operator for distributed full-text search.
//!
//! On the scheduler, a [`DistributedSearchNode`] fans a scored keyword search
//! out to every executor holding a partition of the index. Its single logical
//! input computes the global BM25 statistics (the per-partition
//! `text_search_stats` rows summed with `SUM ... GROUP BY term`). Those
//! statistics are encoded and shipped to each executor as the `global_stats`
//! argument of the `text_search` table function so every partition scores
//! against the same collection-level `df`/`N`/token counts. The scored rows
//! returned by the executors are merged, sorted by score (then primary key for
//! a stable order), and trimmed to the requested `skip`/`fetch` window.

use std::{
    any::Any,
    cmp::Ordering,
    fmt,
    fmt::Write as _,
    hash::{Hash, Hasher},
    sync::Arc,
};

use arrow::{
    array::{ArrayRef, RecordBatch},
    compute::{SortColumn, SortOptions, concat_batches, lexsort_to_indices, take},
    datatypes::{Schema, SchemaRef},
    error::ArrowError,
};
use async_trait::async_trait;
use data_components::flightsql::{FlightSqlClient, query_to_stream};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::{
    common::{DFSchemaRef, Statistics},
    config::ConfigOptions,
    error::{DataFusionError, Result},
    execution::{SendableRecordBatchStream, SessionState, TaskContext},
    logical_expr::{LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore},
    physical_plan::{
        DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PhysicalExpr,
        PlanProperties,
        execution_plan::{
            Boundedness, CardinalityEffect, EmissionType, InvariantLevel, check_default_invariants,
        },
        filter_pushdown::{
            ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
        },
        metrics::MetricsSet,
        projection::ProjectionExec,
        stream::RecordBatchStreamAdapter,
    },
    physical_planner::{ExtensionPlanner, PhysicalPlanner},
    prelude::Expr,
};
use flight_client::cookie::CookieStore;
use futures::{TryStreamExt, future::try_join_all, stream};
use search::{SEARCH_SCORE_COLUMN_NAME, generation::text_search::GlobalBm25Stats};

/// A ready executor to send a scored search query to.
#[derive(Clone)]
pub struct DistributedExecutor {
    pub id: String,
    pub client: FlightSqlClient,
}

impl fmt::Debug for DistributedExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DistributedExecutor")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

/// Serializable parameters of a distributed text search (everything except the
/// executor clients and the stats subplan).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DistributedSearchParams {
    /// Quoted table reference rendered into the `FROM` of the scored query,
    /// e.g. `"spice"."public"."docs"`.
    pub from_table_sql: String,
    pub query: String,
    pub column: Option<String>,
    pub primary_key: Vec<String>,
    /// `N` — the search limit (top-N). `None` means no limit.
    pub fetch: Option<usize>,
    /// Offset applied on the scheduler after the merge.
    pub skip: usize,
}

/// Escape a string for use as a single-quoted SQL string literal, doubling any
/// embedded single quotes, and wrap it in the surrounding quotes.
#[must_use]
fn sql_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Build the scored `text_search` query sent to a single executor.
///
/// Renders `SELECT <cols> FROM text_search(<from>, '<query>'[, "<column>"][,
/// <effective_fetch>], global_stats => '<encoded>')`, where `<cols>` are the
/// double-quoted fields of `schema` in order and `effective_fetch` is
/// `fetch + skip` (each executor must return enough rows for the scheduler to
/// apply the global offset after the merge).
#[must_use]
fn build_scored_sql(
    params: &DistributedSearchParams,
    schema: &Schema,
    encoded_stats: &str,
) -> String {
    let cols = schema
        .fields()
        .iter()
        .map(|f| format!("\"{}\"", f.name()))
        .collect::<Vec<_>>()
        .join(", ");

    let effective_fetch = params.fetch.map(|n| n + params.skip);

    let mut sql = format!(
        "SELECT {cols} FROM text_search({from}, {query}",
        from = params.from_table_sql,
        query = sql_quote(&params.query),
    );

    if let Some(column) = &params.column {
        let _ = write!(sql, ", \"{column}\"");
    }

    if let Some(fetch) = effective_fetch {
        let _ = write!(sql, ", {fetch}");
    }

    let _ = write!(sql, ", global_stats => {})", sql_quote(encoded_stats));

    sql
}

/// Reproject `batch` into `target`'s field order, selecting columns by name.
fn project_batch_to_schema(batch: &RecordBatch, target: &SchemaRef) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(target.fields().len());
    for field in target.fields() {
        let column = batch.column_by_name(field.name()).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "Distributed search result missing expected column {}",
                field.name()
            ))
        })?;
        columns.push(Arc::clone(column));
    }
    RecordBatch::try_new(Arc::clone(target), columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

/// Logical extension node for a distributed full-text search.
#[derive(Debug)]
pub struct DistributedSearchNode {
    stats_input: LogicalPlan,
    schema: DFSchemaRef,
    params: DistributedSearchParams,
    executors: Vec<DistributedExecutor>,
}

impl DistributedSearchNode {
    #[must_use]
    pub fn new(
        stats_input: LogicalPlan,
        schema: DFSchemaRef,
        params: DistributedSearchParams,
        executors: Vec<DistributedExecutor>,
    ) -> Self {
        Self {
            stats_input,
            schema,
            params,
            executors,
        }
    }

    #[must_use]
    pub fn params(&self) -> &DistributedSearchParams {
        &self.params
    }

    #[must_use]
    pub fn executors(&self) -> &[DistributedExecutor] {
        &self.executors
    }

    #[must_use]
    pub fn output_schema(&self) -> &DFSchemaRef {
        &self.schema
    }
}

impl Hash for DistributedSearchNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.stats_input.hash(state);
        self.params.hash(state);
        for executor in &self.executors {
            executor.id.hash(state);
        }
    }
}

impl PartialEq for DistributedSearchNode {
    fn eq(&self, other: &Self) -> bool {
        self.stats_input == other.stats_input
            && self.params == other.params
            && self
                .executors
                .iter()
                .map(|e| &e.id)
                .eq(other.executors.iter().map(|e| &e.id))
    }
}

impl Eq for DistributedSearchNode {}

impl PartialOrd for DistributedSearchNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.stats_input.partial_cmp(&other.stats_input)
    }
}

impl UserDefinedLogicalNodeCore for DistributedSearchNode {
    fn name(&self) -> &'static str {
        "DistributedSearchNode"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.stats_input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DistributedSearchNode query={} executors={}",
            self.params.query,
            self.executors.len()
        )
    }

    /// The output schema differs from the stats input schema, so there is no
    /// column passthrough mapping to expose for projection push-down.
    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        None
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !exprs.is_empty() {
            return Err(DataFusionError::Internal(format!(
                "DistributedSearchNode expects no expressions, got {}",
                exprs.len()
            )));
        }
        if inputs.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "DistributedSearchNode expects exactly one input, got {}",
                inputs.len()
            )));
        }
        let stats_input = inputs.into_iter().next().ok_or_else(|| {
            DataFusionError::Internal("DistributedSearchNode requires one input".to_string())
        })?;
        Ok(Self {
            stats_input,
            schema: Arc::clone(&self.schema),
            params: self.params.clone(),
            executors: self.executors.clone(),
        })
    }
}

/// [`ExtensionPlanner`] that turns a [`DistributedSearchNode`] into a
/// [`DistributedSearchExec`].
#[derive(Debug, Default)]
pub struct DistributedSearchExtensionPlanner {}

impl DistributedSearchExtensionPlanner {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl ExtensionPlanner for DistributedSearchExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<DistributedSearchNode>() else {
            return Ok(None);
        };

        if physical_inputs.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "DistributedSearchNode should have 1 physical input, got {}",
                physical_inputs.len()
            )));
        }

        let output_schema: SchemaRef = Arc::clone(node.output_schema().inner());
        let exec = DistributedSearchExec::new(
            Arc::clone(&physical_inputs[0]),
            output_schema,
            node.params().clone(),
            node.executors().to_vec(),
        );
        Ok(Some(Arc::new(exec)))
    }
}

/// Physical operator for a distributed full-text search.
pub struct DistributedSearchExec {
    stats_input: Arc<dyn ExecutionPlan>,
    params: DistributedSearchParams,
    executors: Vec<DistributedExecutor>,
    cookie_store: Arc<CookieStore>,
    plan_properties: Arc<PlanProperties>,
}

impl DistributedSearchExec {
    #[must_use]
    pub fn new(
        stats_input: Arc<dyn ExecutionPlan>,
        output_schema: SchemaRef,
        params: DistributedSearchParams,
        executors: Vec<DistributedExecutor>,
    ) -> Self {
        let plan_properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            stats_input,
            params,
            executors,
            cookie_store: Arc::new(CookieStore::new()),
            plan_properties,
        }
    }
}

impl fmt::Debug for DistributedSearchExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DistributedSearchExec")
            .field("query", &self.params.query)
            .field("executors", &self.executors.len())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for DistributedSearchExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DistributedSearchExec query={} executors={}",
            self.params.query,
            self.executors.len()
        )
    }
}

#[deny(clippy::missing_trait_methods)]
impl ExecutionPlan for DistributedSearchExec {
    fn with_preserve_order(&self, _preserve_order: bool) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    fn downcast_delegate(&self) -> Option<&dyn ExecutionPlan> {
        None
    }

    fn name(&self) -> &'static str {
        "DistributedSearchExec"
    }

    fn static_name() -> &'static str
    where
        Self: Sized,
    {
        "DistributedSearchExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(self.properties().eq_properties.schema())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn check_invariants(&self, check: InvariantLevel) -> Result<()> {
        check_default_invariants(self, check)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.stats_input]
    }

    fn required_input_ordering(
        &self,
    ) -> Vec<Option<datafusion::physical_expr::OrderingRequirements>> {
        vec![None]
    }

    /// Collapse the stats aggregate to a single partition so the global BM25
    /// statistics are fully summed before the scored fan-out drains them.
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        let children = self.children().into_iter().cloned().collect();
        self.with_new_children(children)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "DistributedSearchExec requires exactly one input".to_string(),
            ));
        }
        let stats_input = children.into_iter().next().ok_or_else(|| {
            DataFusionError::Internal(
                "DistributedSearchExec requires exactly one input".to_string(),
            )
        })?;
        Ok(Arc::new(Self {
            stats_input,
            params: self.params.clone(),
            executors: self.executors.clone(),
            cookie_store: Arc::clone(&self.cookie_store),
            plan_properties: Arc::clone(&self.plan_properties),
        }))
    }

    fn supports_limit_pushdown(&self) -> bool {
        false
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::LowerEqual
    }

    fn try_pushdown_sort(
        &self,
        _order: &[datafusion::physical_expr::PhysicalSortExpr],
    ) -> Result<datafusion::physical_plan::SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        Ok(datafusion::physical_plan::SortOrderPushdownResult::Unsupported)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "DistributedSearchExec only has a single partition, got partition {partition}"
            )));
        }

        let stats_input = Arc::clone(&self.stats_input);
        let params = self.params.clone();
        let executors = self.executors.clone();
        let cookie_store = Arc::clone(&self.cookie_store);
        let output_schema = self.schema();

        let fut = async move {
            // 1. Drain the stats subplan (one partition) to recover the summed
            //    per-term rows.
            let stats_batches: Vec<RecordBatch> =
                stats_input.execute(0, context)?.try_collect().await?;

            // 2. Reconstruct the global BM25 statistics and encode them for
            //    transport as the `global_stats` UDTF argument.
            let stats = GlobalBm25Stats::from_aggregated_batches(&stats_batches)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            let encoded = stats
                .encode()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // 3./4. Fan the scored query out to every executor concurrently.
            let sql = build_scored_sql(&params, output_schema.as_ref(), &encoded);
            let queries = executors.iter().map(|executor| {
                let sql = sql.clone();
                let cookie_store = Arc::clone(&cookie_store);
                let client = executor.client.clone();
                async move {
                    let batches: Vec<RecordBatch> = query_to_stream(client, sql, cookie_store)
                        .try_collect()
                        .await?;
                    Ok::<Vec<RecordBatch>, DataFusionError>(batches)
                }
            });
            let per_executor = try_join_all(queries).await?;
            let all_batches: Vec<RecordBatch> = per_executor.into_iter().flatten().collect();

            // 5. Merge the executor results, sort by score then primary key, and
            //    apply the global `skip`/`fetch` window.
            if all_batches.is_empty() {
                return Ok(RecordBatch::new_empty(Arc::clone(&output_schema)));
            }
            let combined_schema = all_batches[0].schema();
            let combined = concat_batches(&combined_schema, all_batches.iter())
                .map_err(|e: ArrowError| DataFusionError::ArrowError(Box::new(e), None))?;

            let mut sort_columns: Vec<SortColumn> = Vec::new();
            let Some(score) = combined.column_by_name(SEARCH_SCORE_COLUMN_NAME) else {
                return Err(DataFusionError::Internal(format!(
                    "Distributed search results missing {SEARCH_SCORE_COLUMN_NAME} column"
                )));
            };
            sort_columns.push(SortColumn {
                values: Arc::clone(score),
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: false,
                }),
            });
            for key in &params.primary_key {
                if let Some(column) = combined.column_by_name(key) {
                    sort_columns.push(SortColumn {
                        values: Arc::clone(column),
                        options: Some(SortOptions {
                            descending: false,
                            nulls_first: true,
                        }),
                    });
                }
            }

            let effective_fetch = params.fetch.map(|n| n + params.skip);
            let indices = lexsort_to_indices(&sort_columns, effective_fetch)
                .map_err(|e: ArrowError| DataFusionError::ArrowError(Box::new(e), None))?;
            let sorted_columns: Vec<ArrayRef> = combined
                .columns()
                .iter()
                .map(|column| take(column.as_ref(), &indices, None))
                .collect::<std::result::Result<_, ArrowError>>()
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            let sorted = RecordBatch::try_new(combined_schema, sorted_columns)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

            let num_rows = sorted.num_rows();
            let start = params.skip.min(num_rows);
            let mut length = num_rows - start;
            if let Some(fetch) = params.fetch {
                length = length.min(fetch);
            }
            let windowed = sorted.slice(start, length);

            // 6. Emit a single batch in the declared output schema's column order.
            project_batch_to_schema(&windowed, &output_schema)
        };

        let out = self.schema();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            out,
            stream::once(fut),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        let schema = self.schema();
        Ok(Arc::new(Statistics::new_unknown(&schema)))
    }

    fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    fn fetch(&self) -> Option<usize> {
        self.params.fetch
    }

    fn try_swapping_with_projection(
        &self,
        _projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        Ok(FilterPushdownPropagation::if_all(child_pushdown_result))
    }

    fn with_new_state(&self, _state: Arc<dyn Any + Send + Sync>) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }
}

#[cfg(test)]
mod test {
    use std::collections::hash_map::DefaultHasher;

    use arrow::datatypes::{DataType, Field, Schema};

    use super::{DistributedSearchParams, Hash, Hasher, build_scored_sql, sql_quote};

    fn score_and_id_schema() -> Schema {
        Schema::new(vec![
            Field::new("_score", DataType::Float64, true),
            Field::new("id", DataType::Int64, false),
        ])
    }

    fn hash_of<T: Hash>(value: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn sql_quote_doubles_single_quotes() {
        assert_eq!(sql_quote("plain"), "'plain'");
        assert_eq!(sql_quote("it's"), "'it''s'");
        assert_eq!(sql_quote("a'b'c"), "'a''b''c'");
    }

    #[test]
    fn build_scored_sql_renders_expected_string() {
        let params = DistributedSearchParams {
            from_table_sql: "\"spice\".\"public\".\"docs\"".to_string(),
            query: "it's".to_string(),
            column: None,
            primary_key: vec!["id".to_string()],
            fetch: Some(5),
            skip: 0,
        };
        let sql = build_scored_sql(&params, &score_and_id_schema(), "{\"x\":1}");
        assert_eq!(
            sql,
            "SELECT \"_score\", \"id\" FROM text_search(\"spice\".\"public\".\"docs\", 'it''s', 5, global_stats => '{\"x\":1}')"
        );
    }

    #[test]
    fn build_scored_sql_includes_column_and_offset_fetch() {
        let params = DistributedSearchParams {
            from_table_sql: "\"spice\".\"public\".\"docs\"".to_string(),
            query: "hello".to_string(),
            column: Some("body".to_string()),
            primary_key: vec!["id".to_string()],
            fetch: Some(10),
            skip: 5,
        };
        // effective_fetch = fetch + skip = 15, and the column arg is rendered.
        let sql = build_scored_sql(&params, &score_and_id_schema(), "{}");
        assert_eq!(
            sql,
            "SELECT \"_score\", \"id\" FROM text_search(\"spice\".\"public\".\"docs\", 'hello', \"body\", 15, global_stats => '{}')"
        );
    }

    #[test]
    fn build_scored_sql_omits_limit_when_no_fetch() {
        let params = DistributedSearchParams {
            from_table_sql: "\"docs\"".to_string(),
            query: "q".to_string(),
            column: None,
            primary_key: vec![],
            fetch: None,
            skip: 0,
        };
        let sql = build_scored_sql(&params, &score_and_id_schema(), "{}");
        assert_eq!(
            sql,
            "SELECT \"_score\", \"id\" FROM text_search(\"docs\", 'q', global_stats => '{}')"
        );
    }

    #[test]
    fn params_equality_and_hash() {
        let a = DistributedSearchParams {
            from_table_sql: "\"docs\"".to_string(),
            query: "q".to_string(),
            column: Some("body".to_string()),
            primary_key: vec!["id".to_string()],
            fetch: Some(3),
            skip: 1,
        };
        let b = a.clone();
        let mut c = a.clone();
        c.query = "other".to_string();

        assert_eq!(a, b);
        assert_eq!(hash_of(&a), hash_of(&b));
        assert_ne!(a, c);
        assert_ne!(hash_of(&a), hash_of(&c));
    }
}
