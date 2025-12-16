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

use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use data_components::delete::DeletionTableProvider;
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
use datafusion_table_providers::util::constraints::UpsertOptions;
use futures::{StreamExt, stream};

/// A wrapper `TableProvider` that applies batch deduplication based on `UpsertOptions`
/// before passing data to the underlying provider.
///
/// This is used to handle the `UpsertDedup` `on_conflict` behavior, which removes
/// duplicate rows (based on primary key) from incoming batches before insertion.
pub struct UpsertDedupTableProvider {
    /// The underlying table provider for write operations
    inner: Arc<dyn TableProvider>,
    /// The underlying deletion provider for delete operations
    deletion_provider: Arc<dyn DeletionTableProvider>,
    /// Options controlling deduplication behavior
    upsert_options: UpsertOptions,
}

impl UpsertDedupTableProvider {
    /// Creates a new `UpsertDedupTableProvider` wrapping the given provider.
    ///
    /// # Arguments
    /// * `inner` - The underlying table provider to wrap (must implement `DeletionTableProvider`)
    /// * `upsert_options` - Options controlling deduplication behavior
    #[must_use]
    pub fn new(inner: Arc<dyn DeletionTableProvider>, upsert_options: UpsertOptions) -> Self {
        // Clone the Arc as TableProvider for regular operations
        let inner_tp: Arc<dyn TableProvider> = Arc::<dyn DeletionTableProvider>::clone(&inner);
        Self {
            inner: inner_tp,
            deletion_provider: inner,
            upsert_options,
        }
    }

    /// Returns true if deduplication is needed based on the upsert options.
    fn needs_dedup(&self) -> bool {
        self.upsert_options.remove_duplicates || self.upsert_options.last_write_wins
    }
}

impl std::fmt::Debug for UpsertDedupTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpsertDedupTableProvider")
            .field("upsert_options", &self.upsert_options)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for UpsertDedupTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
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
}

#[async_trait]
impl DeletionTableProvider for UpsertDedupTableProvider {
    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: &[Expr],
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.deletion_provider.delete_from(state, filters).await
    }
}

/// An execution plan that applies deduplication to batches before passing them downstream.
#[derive(Debug)]
struct UpsertDedupExec {
    input: Arc<dyn ExecutionPlan>,
    constraints: Constraints,
    upsert_options: UpsertOptions,
    properties: PlanProperties,
}

impl UpsertDedupExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        constraints: Constraints,
        upsert_options: UpsertOptions,
    ) -> Self {
        let properties = input.properties().clone();
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

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &PlanProperties {
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

        // Create a stream that applies deduplication to each batch
        let dedup_stream = input_stream.then(move |batch_result| {
            let constraints = constraints.clone();
            let upsert_options = upsert_options.clone();
            async move {
                let batch = batch_result?;

                // Apply constraint validation with deduplication
                let deduplicated_batches =
                    datafusion_table_providers::util::constraints::validate_batch_with_constraints(
                        vec![batch],
                        &constraints,
                        &upsert_options,
                    )
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                Ok(deduplicated_batches)
            }
        });

        // Flatten the Vec<RecordBatch> results into individual batches
        let flattened_stream = dedup_stream.flat_map(|result| match result {
            Ok(batches) => stream::iter(batches.into_iter().map(Ok)).boxed(),
            Err(e) => stream::once(async move { Err(e) }).boxed(),
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            flattened_stream,
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
pub fn wrap_with_upsert_dedup_if_needed<
    T: DeletionTableProvider + 'static,
    S: std::hash::BuildHasher,
>(
    provider: Arc<T>,
    options: &std::collections::HashMap<String, String, S>,
) -> (Arc<dyn TableProvider>, Arc<dyn DeletionTableProvider>) {
    let upsert_options = extract_upsert_options(options);

    if upsert_options.remove_duplicates || upsert_options.last_write_wins {
        let wrapper = Arc::new(UpsertDedupTableProvider::new(provider, upsert_options));
        (Arc::<UpsertDedupTableProvider>::clone(&wrapper), wrapper)
    } else {
        (Arc::<T>::clone(&provider), provider)
    }
}
