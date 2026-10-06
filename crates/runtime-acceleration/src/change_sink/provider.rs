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

//! Execution through the composed provider's insert and delete methods.

#[path = "cdc.rs"]
pub mod cdc;
#[path = "deletion.rs"]
pub mod deletion;
#[path = "preflight.rs"]
mod preflight;
#[path = "refusal.rs"]
pub mod refusal;

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow_tools::record_batch::try_cast_to;
use arrow_tools::schema_evolution::WideningPlan;
use async_trait::async_trait;
use data_components::arrow::{IndexedMemTable, write::MemTable};
use data_components::cdc::ChangeBatch as CdcBatch;
use data_components::index_maintenance::perform_index_maintenance;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SessionState;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{Expr, dml::InsertOp, lit};
use datafusion::physical_plan::{ExecutionPlan, collect, stream::RecordBatchStreamAdapter};
use futures::stream;
use parking_lot::Mutex;
use runtime_datafusion::execution_plan::schema_cast::SchemaCastScanExec;
use runtime_table_partition::provider::PartitionTableProvider;
use spice_table::{LayerWalk, SpiceTable, find_concrete};

use super::batch::AppendValidation;
use super::{
    BackendWrite, ChangeBatch, ChangeCapabilities, ChangeIndexes, ChangePayload, ChangeSinkBackend,
    ChangeSinkContext, DurabilityObserver, ReplacementSupport, SchemaEvolutionSupport, SetKey,
    StorageDurability, WriteOptions,
};
use crate::dataupdate::StreamingDataUpdateExecutionPlan;
use cdc::{CdcMetrics, ChangeOperationType, group_into_sub_batches, select_rows};
use deletion::{
    build_batch_delete_expr_from_change_batch, build_pk_only_batch_from_change_batch,
    missing_primary_keys,
};
use refusal::before_mutation;

struct InsertPlanCache {
    target_schema: SchemaRef,
    streaming_plan: Arc<StreamingDataUpdateExecutionPlan>,
    insert_plan: Arc<dyn ExecutionPlan>,
}

impl InsertPlanCache {
    async fn try_new(
        table: &Arc<dyn TableProvider>,
        state: &SessionState,
        target_schema: SchemaRef,
    ) -> Result<Self> {
        let streaming_plan = Arc::new(StreamingDataUpdateExecutionPlan::new_empty(Arc::clone(
            &target_schema,
        )));
        let input: Arc<dyn ExecutionPlan> =
            Arc::<StreamingDataUpdateExecutionPlan>::clone(&streaming_plan);
        let cast = Arc::new(SchemaCastScanExec::new(input, Arc::clone(&target_schema)));
        let insert_plan = table.insert_into(state, cast, InsertOp::Append).await?;
        Ok(Self {
            target_schema,
            streaming_plan,
            insert_plan,
        })
    }
}

/// Provider execution selected at binding when no native backend is available.
/// Plan completion does not establish a storage durability guarantee.
pub struct ProviderChangeSinkBackend {
    context: ChangeSinkContext,
    insert_plan: Mutex<Option<InsertPlanCache>>,
    schema_evolution: SchemaEvolutionSupport,
    metrics: CdcMetrics,
    warned_synchronous_cayenne: AtomicBool,
    replacement: ReplacementSupport,
}

impl ProviderChangeSinkBackend {
    #[must_use]
    pub fn new(context: ChangeSinkContext) -> Self {
        let partitioned_cayenne = context
            .table
            .schema()
            .metadata()
            .get("spice.accelerator")
            .is_some_and(|engine| engine == "cayenne")
            && find_concrete::<PartitionTableProvider>(context.table.as_ref(), LayerWalk::Read)
                .is_some();
        Self {
            metrics: CdcMetrics::new(&context.dataset_name),
            context,
            insert_plan: Mutex::new(None),
            warned_synchronous_cayenne: AtomicBool::new(false),
            replacement: ReplacementSupport::Unsupported,
            schema_evolution: if partitioned_cayenne {
                SchemaEvolutionSupport::Recreate
            } else {
                SchemaEvolutionSupport::Restart
            },
        }
    }

    /// Binding can classify engines hidden by an opaque write wrapper without
    /// bypassing that wrapper during execution.
    #[must_use]
    pub fn with_schema_evolution(mut self, support: SchemaEvolutionSupport) -> Self {
        self.schema_evolution = support;
        self
    }

    /// Enable ordered replacement when the engine has SQL equality semantics
    /// for its unique keys and supports composed delete-then-append execution.
    #[must_use]
    pub fn with_ordered_replacement(mut self) -> Self {
        self.replacement = ReplacementSupport::Ordered;
        self
    }

    /// Execute one finite append or ordered replacement through the composed
    /// provider. All batches are cast and the append is planned before deletion.
    /// The caller must establish that this provider supports ordered replacement.
    ///
    /// # Errors
    /// Returns validation, planning, execution, or index-maintenance failures.
    /// A failure after mutation starts does not roll back prior changes.
    pub async fn apply_rows(
        &self,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        scope: Option<SetKey>,
        append_validations: Vec<AppendValidation>,
        options: WriteOptions,
        ctx: &SessionContext,
    ) -> Result<bool> {
        if scope.is_some() && !append_validations.is_empty() {
            return Err(before_mutation(DataFusionError::Plan(
                "Set replacement cannot carry scoped append validation ranges".into(),
            )));
        }
        let _guard = self.context.write_lock.lock().await;
        let state = ctx.state();
        let target_schema = self.context.table.schema();
        let scope = scope
            .map(|scope| SetKey::from_filters(Arc::clone(&target_schema), &scope.filters()))
            .transpose()
            .map_err(before_mutation)?;
        let batches = batches
            .into_iter()
            .map(|batch| try_cast_to(batch, Arc::clone(&target_schema)).map_err(Into::into))
            .collect::<Result<Vec<_>>>()
            .map_err(before_mutation)?;
        if let Some(scope) = &scope {
            for batch in &batches {
                scope.validate_batch(batch).map_err(before_mutation)?;
            }
            preflight::validate_scope_constraints(
                &self.context,
                scope,
                &batches,
                options.delete_batch_size,
                ctx,
            )
            .await?;
        }
        let rows = batches
            .iter()
            .map(RecordBatch::num_rows)
            .fold(0_usize, usize::saturating_add);
        let has_rows = rows > 0;
        let chunks = batches.len();
        let scoped_inputs = append_validations.len();
        if scoped_inputs > 0 {
            tracing::debug!(
                dataset = %self.context.dataset_name,
                scoped_inputs,
                chunks,
                rows,
                "ChangeSink scoped append preflight"
            );
        }
        preflight::validate_append_scopes(
            &self.context,
            &batches,
            &append_validations,
            options.delete_batch_size,
            ctx,
        )
        .await?;
        // The explicit schema is significant for an empty replacement too.
        try_cast_to(RecordBatch::new_empty(schema), Arc::clone(&target_schema))
            .map_err(|error| before_mutation(error.into()))?;
        let insert_plan = if has_rows {
            let adapter = RecordBatchStreamAdapter::new(
                Arc::clone(&target_schema),
                stream::iter(batches.into_iter().map(Ok)),
            );
            let input: Arc<dyn ExecutionPlan> =
                Arc::new(StreamingDataUpdateExecutionPlan::new(Box::pin(adapter)));
            let cast = Arc::new(SchemaCastScanExec::new(input, target_schema));
            Some(
                self.context
                    .table
                    .insert_into(&state, cast, InsertOp::Append)
                    .await?,
            )
        } else {
            None
        };
        let replaced = scope.is_some();
        if let Some(scope) = scope {
            let delete = self
                .context
                .table
                .delete_from(&state, scope.filters())
                .await?;
            collect(delete, ctx.task_ctx()).await?;
        }
        if let Some(insert) = insert_plan {
            collect(insert, ctx.task_ctx()).await?;
            if scoped_inputs > 0 {
                tracing::debug!(
                    dataset = %self.context.dataset_name,
                    scoped_inputs,
                    chunks,
                    rows,
                    "ChangeSink scoped append published"
                );
            }
        }
        if replaced || has_rows {
            perform_change_write_maintenance(&self.context.table).await?;
        }
        Ok(replaced || has_rows)
    }

    fn warn_if_synchronous_cayenne(&self) {
        if self
            .context
            .table
            .schema()
            .metadata()
            .get("spice.accelerator")
            .is_some_and(|engine| engine == "cayenne")
            && !self
                .warned_synchronous_cayenne
                .swap(true, Ordering::Relaxed)
        {
            tracing::warn!(
                "Cayenne CDC for dataset '{}' uses synchronous writes because its write wrappers do not expose native change execution; pipelined finalization is unavailable. See: https://spiceai.org/docs/components/data-accelerators/cayenne",
                self.context.dataset_name,
            );
        }
    }

    async fn append_cdc(
        &self,
        batch: &CdcBatch,
        rows: &[usize],
        ctx: &SessionContext,
    ) -> Result<()> {
        self.warn_if_synchronous_cayenne();
        let target_schema = self.context.table.schema();
        let selected = try_cast_to(
            select_rows(&batch.data_batch(), rows)?,
            Arc::clone(&target_schema),
        )?;
        let input = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&target_schema),
            stream::once(async move { Ok(selected) }),
        ));
        let _guard = self.context.write_lock.lock().await;
        let rebuild = self
            .insert_plan
            .lock()
            .as_ref()
            .is_none_or(|cached| cached.target_schema != target_schema);
        if rebuild {
            let cached =
                InsertPlanCache::try_new(&self.context.table, &ctx.state(), target_schema).await?;
            *self.insert_plan.lock() = Some(cached);
        }
        let (streaming, insert) = {
            let cache = self.insert_plan.lock();
            let cached = cache.as_ref().ok_or_else(|| {
                DataFusionError::Internal("CDC insert plan was not initialized".into())
            })?;
            cached.streaming_plan.set_stream(input)?;
            (
                Arc::clone(&cached.streaming_plan),
                Arc::clone(&cached.insert_plan),
            )
        };
        let result = collect(insert, ctx.task_ctx()).await;
        streaming.clear_stream()?;
        result?;
        perform_change_write_maintenance(&self.context.table).await
    }

    async fn delete_cdc(
        &self,
        batch: &CdcBatch,
        rows: &[usize],
        options: WriteOptions,
        ctx: &SessionContext,
    ) -> Result<()> {
        self.metrics.delete_keys(batch, rows);
        self.metrics.delete_fallthrough("no_capability");
        self.metrics.path("durable_delete");
        let lock_start = Instant::now();
        let _guard = self.context.write_lock.lock().await;
        self.metrics
            .fixed_cost("durable_delete_lock_wait", lock_start);
        let apply_start = Instant::now();
        let (keyless, keyed): (Vec<_>, Vec<_>) = rows
            .iter()
            .copied()
            .partition(|row| !batch.has_primary_keys(*row));
        if !keyless.is_empty() {
            let selected = select_rows(&batch.data_batch(), &keyless)?;
            if delete_matching_rows_from_arrow_provider(&self.context.table, &selected)
                .await?
                .is_none()
            {
                return Err(missing_primary_keys(&self.context.dataset_name.to_string()));
            }
        }
        for chunk in keyed.chunks(options.delete_batch_size.max(1)) {
            if let Some(filter) = build_batch_delete_expr_from_change_batch(
                batch,
                chunk,
                &self.context.dataset_name.to_string(),
            )? {
                self.delete_filter(filter, ctx).await?;
                delete_index_keys(&self.context, batch, chunk, false).await?;
            }
        }
        self.metrics.fixed_cost("durable_delete_apply", apply_start);
        let maintenance_start = Instant::now();
        perform_change_write_maintenance(&self.context.table).await?;
        self.metrics
            .fixed_cost("durable_delete_maintenance", maintenance_start);
        Ok(())
    }

    /// Execute a delete while the caller holds the shared write lock. Provider
    /// index wrappers handle accelerator-side index deletion.
    ///
    /// # Errors
    /// Returns an error if the provider cannot plan or execute the delete.
    pub async fn delete_filter(&self, filter: Expr, ctx: &SessionContext) -> Result<()> {
        let plan = self
            .context
            .table
            .delete_from(&ctx.state(), vec![filter])
            .await?;
        collect(plan, ctx.task_ctx()).await?;
        Ok(())
    }

    /// Delete all rows and maintain indexes under the shared write lock.
    ///
    /// # Errors
    /// Returns an error if deletion or index maintenance fails.
    pub async fn truncate(&self, ctx: &SessionContext) -> Result<()> {
        let _guard = self.context.write_lock.lock().await;
        // Some engines intentionally treat an empty filter list as a no-op.
        self.delete_filter(lit(true), ctx).await?;
        perform_change_write_maintenance(&self.context.table).await
    }
}

#[async_trait]
impl ChangeSinkBackend for ProviderChangeSinkBackend {
    fn schema(&self) -> SchemaRef {
        self.context.table.schema()
    }

    fn capabilities(&self) -> ChangeCapabilities {
        ChangeCapabilities {
            replacement: self.replacement,
            deferred_durability: false,
            deferred_deletes: false,
            schema_evolution: self.schema_evolution,
        }
    }

    async fn apply(
        &self,
        batch: ChangeBatch,
        options: WriteOptions,
        ctx: &SessionContext,
    ) -> Result<BackendWrite> {
        let (payload, scope) = batch.into_parts();
        if scope.is_some() && self.replacement == ReplacementSupport::Unsupported {
            return Err(before_mutation(DataFusionError::NotImplemented(
                "Set replacement is not enabled for this accelerator".into(),
            )));
        }
        let changed = match payload {
            ChangePayload::Rows {
                schema,
                batches,
                append_validations,
            } => {
                self.apply_rows(schema, batches, scope, append_validations, options, ctx)
                    .await?
            }
            ChangePayload::Cdc(rows) => {
                let batch = rows.into_built().map_err(before_mutation)?;
                let groups = group_into_sub_batches(&batch);
                reject_unknown_operations(&groups)?;
                let changed = !groups.is_empty();
                for (op, rows) in groups {
                    match op {
                        ChangeOperationType::Upsert => self.append_cdc(&batch, &rows, ctx).await?,
                        ChangeOperationType::Delete => {
                            self.delete_cdc(&batch, &rows, options, ctx).await?;
                        }
                        ChangeOperationType::Truncate => self.truncate(ctx).await?,
                        ChangeOperationType::Unknown => unreachable!("validated before mutation"),
                    }
                }
                changed
            }
        };
        Ok(BackendWrite::complete(
            changed,
            StorageDurability::NotPromised,
        ))
    }

    fn set_durability_observer(&self, _observer: Arc<dyn DurabilityObserver>) {}

    async fn flush(&self) -> Result<()> {
        // No deferred work is admitted by this implementation.
        Ok(())
    }

    async fn evolve_schema(&self, plan: &WideningPlan) -> Result<()> {
        if self.schema_evolution == SchemaEvolutionSupport::Recreate {
            return Err(before_mutation(DataFusionError::Execution(
                partitioned_widening_refusal(
                    &self.context.dataset_name.to_string(),
                    &plan.describe(),
                ),
            )));
        }
        Err(before_mutation(DataFusionError::NotImplemented(format!(
            "Live schema evolution is not supported for dataset '{}'",
            self.context.dataset_name,
        ))))
    }
}

#[must_use]
pub fn partitioned_widening_refusal(dataset: &str, change: &str) -> String {
    format!(
        "widening schema change detected on the CDC stream for '{dataset}' ({change}), \
         but a partitioned Cayenne acceleration cannot evolve its schema in place, so the change was refused \
         rather than applied lossily. No part of the batch was applied and the source keeps its position, \
         so the acceleration still holds every row it held before it. \
         Under `mode: file_update`, `mode: file_create` and `mode: memory`, restart Spice to apply it: the acceleration comes back \
         rebuilt against the new schema — dropped and recreated, started from an empty directory, or never persisted at all. \
         Under all three that restart rebuilds the schema, but it reloads the rows only where the source can replay them. \
         Where the connector takes an initial-snapshot setting — `pg_replication_initial_snapshot`, \
         `mysql_replication_initial_snapshot` and `dynamodb_replication_initial_snapshot` — \
         set it to `always` before restarting if the acceleration has to come back with its history — it is the only value \
         that snapshots under every mode on every one of them. `auto` skips the snapshot under `mode: file_update` on \
         PostgreSQL, and skips it under every mode on MySQL and DynamoDB, which resume from their recorded position \
         without consulting the acceleration mode; `disabled` skips it under every mode on all three. \
         Where it does not — Debezium, MongoDB \
         and `cdc_ingest` have no such setting — the acceleration comes back holding only what the change stream delivers \
         from its resume position onward, and restoring its history means replaying the source from an earlier position or \
         reloading the dataset with a full refresh. \
         Under `mode: file` a restart reopens the stored table and refuses again — drop and recreate the dataset against the \
         new source schema, dropping `partition_by` in the same change if partitioning is no longer wanted. \
         Removing `partition_by` on its own does not recover it: the unpartitioned table is a different Cayenne table from \
         the partition children the rows were written to, and changing that setting recreates nothing, so the acceleration \
         would come back holding none of them. \
         See: https://spiceai.org/docs/components/data-accelerators/cayenne"
    )
}

/// Check operation codes before applying any group.
///
/// # Errors
/// Returns a pre-mutation refusal if any group has an unknown operation.
pub fn reject_unknown_operations(groups: &[(ChangeOperationType, Vec<usize>)]) -> Result<()> {
    if groups
        .iter()
        .any(|(op, _)| *op == ChangeOperationType::Unknown)
    {
        return Err(before_mutation(DataFusionError::Execution(
            "Unknown CDC change operation".into(),
        )));
    }
    Ok(())
}

/// Native deletes bypass accelerator index wrappers; provider deletes do not.
/// Both must also maintain source-side external indexes.
///
/// # Errors
/// Returns an error if primary-key projection fails. Individual index deletion
/// failures are logged and do not stop maintenance of the remaining indexes.
pub async fn delete_index_keys(
    context: &ChangeSinkContext,
    batch: &CdcBatch,
    rows: &[usize],
    include_accelerator: bool,
) -> Result<()> {
    let mut indexes: ChangeIndexes = (context.external_indexes)();
    if include_accelerator {
        indexes.extend(
            spice_table::nodes(context.table.as_ref(), LayerWalk::Read)
                .flat_map(SpiceTable::indexes)
                .map(Arc::clone),
        );
    }
    if indexes.is_empty() {
        return Ok(());
    }
    let Some(keys) = build_pk_only_batch_from_change_batch(batch, rows)? else {
        return Ok(());
    };
    let mut seen = HashSet::new();
    for index in indexes {
        if seen.insert(Arc::as_ptr(&index).cast::<()>().addr())
            && let Err(error) = index.delete_by_keys(keys.clone()).await
        {
            tracing::error!(
                "Index '{}' failed to delete entries for dataset '{}' (best-effort, continuing): {error}",
                index.name(),
                context.dataset_name,
            );
        }
    }
    Ok(())
}

/// Delete matching rows from writable Arrow tables, including partition children.
/// Returns `None` when no supported Arrow table is exposed by the write layers.
///
/// # Errors
/// Returns an error if a supported table cannot delete the supplied rows.
pub async fn delete_matching_rows_from_arrow_provider(
    provider: &Arc<dyn TableProvider>,
    rows: &RecordBatch,
) -> Result<Option<u64>> {
    if let Some(table) = find_concrete::<MemTable>(provider.as_ref(), LayerWalk::Write) {
        return table.delete_matching_rows(rows).await.map(Some);
    }
    if let Some(table) = find_concrete::<IndexedMemTable>(provider.as_ref(), LayerWalk::Write) {
        return table.delete_matching_rows(rows).await.map(Some);
    }
    if let Some(partitioned) =
        find_concrete::<PartitionTableProvider>(provider.as_ref(), LayerWalk::Write)
    {
        let mut deleted = 0;
        let mut found = false;
        for partition in partitioned.partition_table_providers().await {
            if let Some(count) =
                Box::pin(delete_matching_rows_from_arrow_provider(&partition, rows)).await?
            {
                deleted += count;
                found = true;
            }
        }
        return Ok(found.then_some(deleted));
    }
    Ok(None)
}

/// Maintain indexes on the provider and its partition children.
///
/// # Errors
/// Returns an error if index maintenance fails for any visited provider.
pub async fn perform_change_write_maintenance(provider: &Arc<dyn TableProvider>) -> Result<()> {
    if let Some(table) = provider.downcast_ref::<SpiceTable>() {
        return Box::pin(perform_change_write_maintenance(table.below())).await;
    }
    if let Some(partitioned) = provider.downcast_ref::<PartitionTableProvider>() {
        for partition in partitioned.partition_table_providers().await {
            Box::pin(perform_change_write_maintenance(&partition)).await?;
        }
        return Ok(());
    }
    perform_index_maintenance(provider.as_ref())
        .await
        .map(|_| ())
}
