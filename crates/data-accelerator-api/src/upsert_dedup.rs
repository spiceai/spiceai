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

//! A wrapper `TableProvider` that applies deduplication to incoming batches
//! before passing them to the underlying accelerator table provider.
//!
//! This handles the `UpsertDedup` `on_conflict` behavior by removing duplicate rows
//! within incoming batches before they are inserted into the accelerator.

use std::sync::Arc;

use arrow::{compute::concat_batches, datatypes::SchemaRef};
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::Constraints,
    datasource::TableProvider,
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{Expr, TableType, dml::InsertOp},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, metrics::MetricsSet,
        stream::RecordBatchStreamAdapter,
    },
};
use futures::StreamExt;

use datafusion_table_providers::util::constraints::UpsertOptions;

/// A wrapper `TableProvider` that applies batch deduplication based on `UpsertOptions`
/// before passing data to the underlying provider.
///
/// This is used to handle the `UpsertDedup` `on_conflict` behavior, which removes
/// duplicate rows (based on primary key) from incoming batches before insertion.
pub struct UpsertDedupTableProvider {
    /// The underlying table provider for write and delete operations
    inner: Arc<dyn TableProvider>,
    /// Options controlling deduplication behavior
    upsert_options: UpsertOptions,
    /// Constraints for deduplication (e.g., primary key)
    /// Stored explicitly because the inner provider may not expose constraints
    constraints: Constraints,
}

impl UpsertDedupTableProvider {
    /// Creates a new `UpsertDedupTableProvider` wrapping the given provider.
    ///
    /// # Arguments
    /// * `inner` - The underlying table provider to wrap
    /// * `upsert_options` - Options controlling deduplication behavior
    /// * `constraints` - Constraints for deduplication (e.g., primary key)
    #[must_use]
    pub fn new(
        inner: Arc<dyn TableProvider>,
        upsert_options: UpsertOptions,
        constraints: Constraints,
    ) -> Self {
        Self {
            inner,
            upsert_options,
            constraints,
        }
    }

    /// Returns true if deduplication is needed based on the upsert options.
    fn needs_dedup(&self) -> bool {
        self.upsert_options.remove_duplicates || self.upsert_options.last_write_wins
    }

    /// Returns a reference to the inner table provider.
    #[must_use]
    pub fn inner(&self) -> &Arc<dyn TableProvider> {
        &self.inner
    }
}

impl std::fmt::Debug for UpsertDedupTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpsertDedupTableProvider")
            .field("upsert_options", &self.upsert_options)
            .finish_non_exhaustive()
    }
}

#[deny(clippy::missing_trait_methods)]
#[async_trait]
impl TableProvider for UpsertDedupTableProvider {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn statistics(&self) -> Option<datafusion::common::Statistics> {
        self.inner.statistics()
    }

    fn constraints(&self) -> Option<&Constraints> {
        if self.constraints.is_empty() {
            None
        } else {
            Some(&self.constraints)
        }
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        op: InsertOp,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // If no deduplication is needed, pass through to the underlying provider
        if !self.needs_dedup() {
            return self.inner.insert_into(state, input, op).await;
        }

        // Get constraints from the underlying provider
        let constraints = self.constraints().cloned().unwrap_or_default();

        // If there are no constraints, no deduplication is possible
        if constraints.is_empty() {
            return self.inner.insert_into(state, input, op).await;
        }

        // Wrap the input with a deduplication execution plan
        let dedup_exec = Arc::new(UpsertDedupExec::new(
            input,
            constraints,
            self.upsert_options.clone(),
        ));

        self.inner.insert_into(state, dedup_exec, op).await
    }

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.inner.delete_from(state, filters).await
    }

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.inner.update(state, assignments, filters).await
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    fn get_logical_plan(
        &self,
    ) -> Option<std::borrow::Cow<'_, datafusion::logical_expr::LogicalPlan>> {
        self.inner.get_logical_plan()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.inner.get_column_default(column)
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: datafusion::catalog::ScanArgs<'a>,
    ) -> datafusion::error::Result<datafusion::catalog::ScanResult> {
        let plan = self
            .scan(
                state,
                args.projection().map(<[usize]>::to_vec).as_ref(),
                args.filters().unwrap_or(&[]),
                args.limit(),
            )
            .await?;
        Ok(plan.into())
    }

    async fn truncate(
        &self,
        state: &dyn Session,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.inner.truncate(state).await
    }
}

/// An execution plan that applies deduplication to batches before passing them downstream.
#[derive(Debug)]
struct UpsertDedupExec {
    input: Arc<dyn ExecutionPlan>,
    constraints: Constraints,
    upsert_options: UpsertOptions,
    properties: Arc<PlanProperties>,
}

impl UpsertDedupExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        constraints: Constraints,
        upsert_options: UpsertOptions,
    ) -> Self {
        let properties = Arc::clone(input.properties());
        Self {
            input,
            constraints,
            upsert_options,
            properties,
        }
    }
}

impl DisplayAs for UpsertDedupExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "UpsertDedupExec: remove_duplicates={}, last_write_wins={}",
                    self.upsert_options.remove_duplicates, self.upsert_options.last_write_wins
                )
            }
        }
    }
}

impl ExecutionPlan for UpsertDedupExec {
    fn name(&self) -> &'static str {
        "UpsertDedupExec"
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "UpsertDedupExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.constraints.clone(),
            self.upsert_options.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let schema = self.schema();
        let constraints = self.constraints.clone();
        let upsert_options = self.upsert_options.clone();

        // Create a stream that validates constraints and applies deduplication to each batch.
        let stream_schema = Arc::clone(&schema);
        let validated_stream = input_stream.then(move |batch_result| {
            let constraints = constraints.clone();
            let upsert_options = upsert_options.clone();
            let schema = Arc::clone(&stream_schema);
            async move {
                let batch = batch_result?;

                let tp_upsert_options =
                    datafusion_table_providers::util::constraints::UpsertOptions::default()
                        .with_remove_duplicates(upsert_options.remove_duplicates)
                        .with_last_write_wins(upsert_options.last_write_wins);
                let validated_batches =
                    datafusion_table_providers::util::constraints::validate_batch_with_constraints(
                        vec![batch],
                        &constraints,
                        &tp_upsert_options,
                    )
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                if validated_batches.is_empty() {
                    return Err(DataFusionError::Internal(
                        "Expected validated batch".to_string(),
                    ));
                }
                concat_batches(&schema, &validated_batches)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            validated_stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.input.metrics()
    }
}

/// Extracts `UpsertOptions` from the command options.
#[must_use]
pub fn extract_upsert_options<S: std::hash::BuildHasher>(
    options: &std::collections::HashMap<String, String, S>,
) -> UpsertOptions {
    let remove_duplicates = options
        .get("upsert_remove_duplicates")
        .is_some_and(|v| v.eq_ignore_ascii_case("true"));
    let last_write_wins = options
        .get("upsert_last_write_wins")
        .is_some_and(|v| v.eq_ignore_ascii_case("true"));

    UpsertOptions {
        remove_duplicates,
        last_write_wins,
    }
}

/// Wraps a table provider with upsert deduplication if needed based on the options.
///
/// Returns the original provider if deduplication is not needed.
#[must_use]
pub fn wrap_with_upsert_dedup_if_needed<T: TableProvider + 'static, S: std::hash::BuildHasher>(
    provider: Arc<T>,
    options: &std::collections::HashMap<String, String, S>,
    constraints: Constraints,
) -> Arc<dyn TableProvider> {
    let upsert_options = extract_upsert_options(options);

    if upsert_options.remove_duplicates || upsert_options.last_write_wins {
        Arc::new(UpsertDedupTableProvider::new(
            provider,
            upsert_options,
            constraints,
        ))
    } else {
        provider
    }
}

#[cfg(test)]
mod tests {
    use super::{
        UpsertDedupExec, UpsertDedupTableProvider, extract_upsert_options,
        wrap_with_upsert_dedup_if_needed,
    };
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::catalog::MemTable;
    use datafusion::common::{Constraint, Constraints};
    use datafusion::datasource::TableProvider;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::TaskContext;
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::physical_plan::{ExecutionPlan, collect};
    use datafusion::prelude::SessionContext;
    use datafusion_table_providers::util::constraints::UpsertOptions;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("v", DataType::Utf8, true),
        ]))
    }

    fn batch(rows: &[(i32, &str)]) -> RecordBatch {
        let ids: Vec<i32> = rows.iter().map(|(id, _)| *id).collect();
        let vals: Vec<&str> = rows.iter().map(|(_, v)| *v).collect();
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(vals)),
            ],
        )
        .expect("build batch")
    }

    fn source(batches: &[Vec<RecordBatch>]) -> Arc<dyn ExecutionPlan> {
        let src = MemorySourceConfig::try_new(batches, schema(), None).expect("memory source");
        Arc::new(DataSourceExec::new(Arc::new(src)))
    }

    /// `id` is the primary key.
    fn pk_constraints() -> Constraints {
        Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0])])
    }

    fn last_write_wins() -> UpsertOptions {
        UpsertOptions::default().with_last_write_wins(true)
    }

    fn remove_duplicates() -> UpsertOptions {
        UpsertOptions::default().with_remove_duplicates(true)
    }

    /// Sorted `(id, v)` pairs of everything the plan produced.
    async fn rows_of(plan: Arc<dyn ExecutionPlan>) -> Vec<(i32, String)> {
        let ctx = Arc::new(TaskContext::default());
        let batches = collect(plan, ctx).await.expect("plan executes");
        let mut rows = Vec::new();
        for batch in batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("id is Int32");
            let vals = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("v is Utf8");
            for row in 0..batch.num_rows() {
                rows.push((ids.value(row), vals.value(row).to_string()));
            }
        }
        rows.sort_unstable();
        rows
    }

    fn plan_contains(plan: &Arc<dyn ExecutionPlan>, name: &str) -> bool {
        if plan.name() == name {
            return true;
        }
        plan.children()
            .iter()
            .any(|child| plan_contains(child, name))
    }

    #[test]
    fn extract_upsert_options_defaults_to_no_deduplication() {
        let options: HashMap<String, String> = HashMap::new();
        let extracted = extract_upsert_options(&options);

        assert!(!extracted.remove_duplicates);
        assert!(!extracted.last_write_wins);
    }

    #[test]
    fn extract_upsert_options_reads_both_flags_case_insensitively() {
        let options: HashMap<String, String> = [
            ("upsert_remove_duplicates".to_string(), "TRUE".to_string()),
            ("upsert_last_write_wins".to_string(), "True".to_string()),
        ]
        .into_iter()
        .collect();

        let extracted = extract_upsert_options(&options);
        assert!(extracted.remove_duplicates);
        assert!(extracted.last_write_wins);
    }

    /// Only a literal `true` turns deduplication on. Anything else — including
    /// `1` and `yes` — leaves it off, so a typo cannot silently start dropping
    /// rows the caller expected to be written.
    #[test]
    fn extract_upsert_options_treats_any_non_true_value_as_off() {
        for value in ["false", "1", "yes", "", "trueish"] {
            let options: HashMap<String, String> =
                [("upsert_remove_duplicates".to_string(), value.to_string())]
                    .into_iter()
                    .collect();

            assert!(
                !extract_upsert_options(&options).remove_duplicates,
                "{value:?} must not enable deduplication"
            );
        }
    }

    #[test]
    fn wrap_with_upsert_dedup_if_needed_leaves_the_provider_alone_when_off() {
        let inner = Arc::new(MemTable::try_new(schema(), vec![vec![]]).expect("memtable"));
        let options: HashMap<String, String> = HashMap::new();

        let wrapped = wrap_with_upsert_dedup_if_needed(inner, &options, pk_constraints());
        assert!(
            !wrapped.is::<UpsertDedupTableProvider>(),
            "no dedup requested, so no wrapper"
        );
    }

    #[test]
    fn wrap_with_upsert_dedup_if_needed_wraps_for_either_flag() {
        for key in ["upsert_remove_duplicates", "upsert_last_write_wins"] {
            let inner = Arc::new(MemTable::try_new(schema(), vec![vec![]]).expect("memtable"));
            let options: HashMap<String, String> = [(key.to_string(), "true".to_string())]
                .into_iter()
                .collect();

            let wrapped = wrap_with_upsert_dedup_if_needed(inner, &options, pk_constraints());
            assert!(
                wrapped.is::<UpsertDedupTableProvider>(),
                "{key} must install the dedup wrapper"
            );
        }
    }

    /// The wrapper reports its own constraints (the inner accelerator provider
    /// may not expose them), and reports `None` — not an empty set — when there
    /// are none, matching what `TableProvider` callers expect.
    #[test]
    fn constraints_are_reported_from_the_wrapper_and_none_when_empty() {
        let inner = Arc::new(MemTable::try_new(schema(), vec![vec![]]).expect("memtable"));
        let keyed = UpsertDedupTableProvider::new(
            Arc::clone(&inner) as Arc<dyn TableProvider>,
            last_write_wins(),
            pk_constraints(),
        );
        assert_eq!(keyed.constraints(), Some(&pk_constraints()));

        let keyless = UpsertDedupTableProvider::new(
            inner as Arc<dyn TableProvider>,
            last_write_wins(),
            Constraints::default(),
        );
        assert!(keyless.constraints().is_none());
    }

    /// Deduplication is a *write*-side transform: reads must pass straight
    /// through, schema and statistics included, or the optimizer would plan
    /// against numbers the wrapper invented.
    #[tokio::test]
    async fn reads_pass_through_to_the_inner_provider() {
        let inner = Arc::new(
            MemTable::try_new(schema(), vec![vec![batch(&[(1, "a"), (2, "b")])]])
                .expect("memtable"),
        );
        let provider = UpsertDedupTableProvider::new(
            Arc::clone(&inner) as Arc<dyn TableProvider>,
            last_write_wins(),
            pk_constraints(),
        );

        assert_eq!(provider.schema(), inner.schema());
        assert_eq!(provider.table_type(), inner.table_type());
        assert_eq!(provider.statistics(), inner.statistics());

        let ctx = SessionContext::new();
        let plan = provider
            .scan(&ctx.state(), None, &[], None)
            .await
            .expect("scan");
        assert_eq!(
            rows_of(plan).await,
            vec![(1, "a".to_string()), (2, "b".to_string())]
        );
    }

    /// With `last_write_wins`, the *last* row for a key is the one written.
    /// Keeping the first would resurrect a value the source already replaced.
    #[tokio::test]
    async fn last_write_wins_keeps_the_final_row_for_each_key() {
        let input = source(&[vec![batch(&[
            (1, "first"),
            (2, "only"),
            (1, "second"),
            (1, "third"),
        ])]]);
        let dedup = Arc::new(UpsertDedupExec::new(
            input,
            pk_constraints(),
            last_write_wins(),
        ));

        assert_eq!(
            rows_of(dedup).await,
            vec![(1, "third".to_string()), (2, "only".to_string())]
        );
    }

    /// `remove_duplicates` collapses rows that are identical in *every* column.
    #[tokio::test]
    async fn remove_duplicates_collapses_fully_identical_rows() {
        let input = source(&[vec![batch(&[(1, "a"), (1, "a"), (2, "b")])]]);
        let dedup = Arc::new(UpsertDedupExec::new(
            input,
            pk_constraints(),
            remove_duplicates(),
        ));

        assert_eq!(
            rows_of(dedup).await,
            vec![(1, "a".to_string()), (2, "b".to_string())]
        );
    }

    /// Two different rows sharing a primary key are a genuine constraint
    /// violation that `remove_duplicates` alone cannot resolve. It must surface
    /// as an error rather than silently picking a winner.
    #[tokio::test]
    async fn remove_duplicates_errors_on_a_conflicting_key() {
        let input = source(&[vec![batch(&[(1, "a"), (1, "b")])]]);
        let dedup = Arc::new(UpsertDedupExec::new(
            input,
            pk_constraints(),
            remove_duplicates(),
        ));

        let ctx = Arc::new(TaskContext::default());
        assert!(
            collect(dedup, ctx).await.is_err(),
            "conflicting rows for one key must not be silently deduplicated"
        );
    }

    /// A batch with no conflicts must come through byte-for-byte, in order.
    #[tokio::test]
    async fn a_conflict_free_batch_is_passed_through_unchanged() {
        let input = source(&[vec![batch(&[(1, "a"), (2, "b"), (3, "c")])]]);
        let dedup = Arc::new(UpsertDedupExec::new(
            input,
            pk_constraints(),
            last_write_wins(),
        ));

        assert_eq!(
            rows_of(dedup).await,
            vec![
                (1, "a".to_string()),
                (2, "b".to_string()),
                (3, "c".to_string())
            ]
        );
    }

    /// Deduplication is scoped to one batch: each batch is a separate write
    /// statement, and the accelerator's own `ON CONFLICT` resolves a key that
    /// reappears in a later batch. Both batches must therefore reach the sink.
    #[tokio::test]
    async fn deduplication_is_scoped_to_a_single_batch() {
        let input = source(&[vec![
            batch(&[(1, "first"), (1, "second")]),
            batch(&[(1, "third")]),
        ]]);
        let dedup = Arc::new(UpsertDedupExec::new(
            input,
            pk_constraints(),
            last_write_wins(),
        ));

        assert_eq!(
            rows_of(dedup).await,
            vec![(1, "second".to_string()), (1, "third".to_string())],
            "each batch deduplicates independently"
        );
    }

    /// The dedup node must sit between the input and the accelerator's sink, or
    /// duplicate keys reach an `ON CONFLICT DO UPDATE` that cannot touch the
    /// same row twice in one statement.
    #[tokio::test]
    async fn insert_into_installs_the_dedup_node_above_the_inner_sink() {
        let inner = Arc::new(MemTable::try_new(schema(), vec![vec![]]).expect("memtable"));
        let provider = UpsertDedupTableProvider::new(
            inner as Arc<dyn TableProvider>,
            last_write_wins(),
            pk_constraints(),
        );

        let ctx = SessionContext::new();
        let plan = provider
            .insert_into(
                &ctx.state(),
                source(&[vec![batch(&[(1, "a")])]]),
                InsertOp::Append,
            )
            .await
            .expect("insert plan");

        assert!(
            plan_contains(&plan, "UpsertDedupExec"),
            "dedup node missing from the write plan"
        );
    }

    /// Without a key there is nothing to deduplicate *by*. Installing the node
    /// anyway would be a no-op at best; the wrapper must hand the write
    /// straight to the accelerator.
    #[tokio::test]
    async fn insert_into_skips_the_dedup_node_when_there_are_no_constraints() {
        let inner = Arc::new(MemTable::try_new(schema(), vec![vec![]]).expect("memtable"));
        let provider = UpsertDedupTableProvider::new(
            inner as Arc<dyn TableProvider>,
            last_write_wins(),
            Constraints::default(),
        );

        let ctx = SessionContext::new();
        let plan = provider
            .insert_into(
                &ctx.state(),
                source(&[vec![batch(&[(1, "a")])]]),
                InsertOp::Append,
            )
            .await
            .expect("insert plan");

        assert!(!plan_contains(&plan, "UpsertDedupExec"));
    }

    /// A wrapper built with both flags off must not alter the write path at
    /// all, even though it is still in the provider chain.
    #[tokio::test]
    async fn insert_into_skips_the_dedup_node_when_both_flags_are_off() {
        let inner = Arc::new(MemTable::try_new(schema(), vec![vec![]]).expect("memtable"));
        let provider = UpsertDedupTableProvider::new(
            inner as Arc<dyn TableProvider>,
            UpsertOptions::default(),
            pk_constraints(),
        );

        let ctx = SessionContext::new();
        let plan = provider
            .insert_into(
                &ctx.state(),
                source(&[vec![batch(&[(1, "a")])]]),
                InsertOp::Append,
            )
            .await
            .expect("insert plan");

        assert!(!plan_contains(&plan, "UpsertDedupExec"));
    }

    /// End-to-end: the rows that land in the accelerator are the deduplicated
    /// ones, and the dedup node's output schema still matches the table's (a
    /// mismatch would fail the write instead of just dropping duplicates).
    #[tokio::test]
    async fn an_insert_through_the_wrapper_lands_only_the_winning_rows() {
        let inner = Arc::new(MemTable::try_new(schema(), vec![vec![]]).expect("memtable"));
        let provider = UpsertDedupTableProvider::new(
            Arc::clone(&inner) as Arc<dyn TableProvider>,
            last_write_wins(),
            pk_constraints(),
        );

        let ctx = SessionContext::new();
        let plan = provider
            .insert_into(
                &ctx.state(),
                source(&[vec![batch(&[(1, "old"), (2, "keep"), (1, "new")])]]),
                InsertOp::Append,
            )
            .await
            .expect("insert plan");
        collect(plan, ctx.task_ctx()).await.expect("insert runs");

        let scan = inner
            .scan(&ctx.state(), None, &[], None)
            .await
            .expect("scan");
        assert_eq!(
            rows_of(scan).await,
            vec![(1, "new".to_string()), (2, "keep".to_string())]
        );
    }

    /// `with_new_children` must carry the constraints and options across, or a
    /// plan rewrite (repartitioning, coalescing) would quietly disable
    /// deduplication.
    #[tokio::test]
    async fn with_new_children_preserves_the_dedup_configuration() {
        let dedup = Arc::new(UpsertDedupExec::new(
            source(&[vec![batch(&[(1, "a")])]]),
            pk_constraints(),
            last_write_wins(),
        ));

        let rebuilt = Arc::clone(&dedup)
            .with_new_children(vec![source(&[vec![batch(&[(1, "first"), (1, "second")])]])])
            .expect("rebuild with one child");

        assert_eq!(rows_of(rebuilt).await, vec![(1, "second".to_string())]);
    }

    #[test]
    fn with_new_children_rejects_the_wrong_child_count() {
        let dedup = Arc::new(UpsertDedupExec::new(
            source(&[vec![batch(&[(1, "a")])]]),
            pk_constraints(),
            last_write_wins(),
        ));

        Arc::clone(&dedup)
            .with_new_children(vec![])
            .expect_err("no children must be rejected");
        dedup
            .with_new_children(vec![
                source(&[vec![batch(&[(1, "a")])]]),
                source(&[vec![batch(&[(2, "b")])]]),
            ])
            .expect_err("two children must be rejected");
    }
}
