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

//! Native CDC execution and storage durability fences for the table owner.

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::SchemaRef;
use arrow_tools::record_batch::try_cast_to;
use arrow_tools::schema_evolution::WideningPlan;
use async_trait::async_trait;
use cayenne::{CayenneTableProvider, RebuildableWrite, SlotAdvancer};
use data_accelerator_api::upsert_dedup::UpsertDedupTableProvider;
use data_components::cdc::ChangeBatch as CdcBatch;
use data_components::poly::PolyTableProvider;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{future::BoxFuture, stream};
use parking_lot::RwLock;
use runtime_acceleration::change_sink::provider::{
    ProviderChangeSinkBackend,
    cdc::{CdcMetrics, ChangeOperationType, group_into_sub_batches, select_rows},
    delete_index_keys, delete_matching_rows_from_arrow_provider,
    deletion::{build_batch_delete_expr_from_change_batch, missing_primary_keys},
    perform_change_write_maintenance,
    refusal::before_mutation,
    reject_unknown_operations,
};
use runtime_acceleration::change_sink::{
    BackendWrite, ChangeBatch, ChangeCapabilities, ChangePayload, ChangeSinkBackend,
    ChangeSinkContext, DurabilityObserver, Recovery, ReplacementSupport, SchemaEvolutionSupport,
    SetKey, StorageDurability, WriteOptions,
};
use runtime_table_partition::provider::PartitionTableProvider;
use spice_table::{LayerWalk, find_concrete};

/// Classify the fallback's schema recovery without unwrapping its execution
/// target. Only known dedup wrappers are inspected beyond the write walk.
#[must_use]
pub fn provider_schema_evolution(table: &Arc<dyn TableProvider>) -> SchemaEvolutionSupport {
    if let Some(partitioned) =
        find_concrete::<PartitionTableProvider>(table.as_ref(), LayerWalk::Write)
        && partitioned.creator().accepts_direct_partition_writes()
    {
        return SchemaEvolutionSupport::Recreate;
    }
    if let Some(dedup) = find_concrete::<UpsertDedupTableProvider>(table.as_ref(), LayerWalk::Write)
    {
        return provider_schema_evolution(dedup.inner());
    }
    SchemaEvolutionSupport::Restart
}

/// Stop the background maintenance of every Cayenne instance `table` serves
/// from — the provider itself, each partition of a partitioned table, and the
/// table a poly or upsert-dedup wrapper writes to — and wait for maintenance
/// they already started. See `CayenneTableProvider::quiesce`. The same walk as
/// the runtime's generation drain (`quiesce_cayenne_maintenance`).
pub async fn quiesce_table_maintenance(table: &Arc<dyn TableProvider>) {
    let mut instances = Vec::new();
    let mut pending = vec![Arc::clone(table)];
    while let Some(provider) = pending.pop() {
        if let Some(cayenne) =
            find_concrete::<CayenneTableProvider>(provider.as_ref(), LayerWalk::Write)
        {
            instances.push(cayenne.clone_for_write_operations());
        } else if let Some(partitioned) =
            find_concrete::<PartitionTableProvider>(provider.as_ref(), LayerWalk::Write)
        {
            pending.extend(partitioned.partition_table_providers().await);
        } else if let Some(poly) =
            spice_table::find_layer::<PolyTableProvider>(provider.as_ref(), LayerWalk::Write)
        {
            pending.push(poly.writer());
        } else if let Some(dedup) =
            find_concrete::<UpsertDedupTableProvider>(provider.as_ref(), LayerWalk::Write)
        {
            pending.push(Arc::clone(dedup.inner()));
        }
    }
    futures::future::join_all(instances.iter().map(CayenneTableProvider::quiesce)).await;
}

struct StorageFenceObserver {
    observer: Arc<dyn DurabilityObserver>,
}

#[async_trait]
impl SlotAdvancer for StorageFenceObserver {
    async fn on_checkpoint_durable(&self, durable_epoch: u64) {
        self.observer.on_durable(durable_epoch).await;
    }
}

/// Bound only through write-transparent layers. Source acknowledgement is owned
/// by the source adapter, not by the storage fence callback.
pub struct CayenneChangeSinkBackend {
    context: ChangeSinkContext,
    table: CayenneTableProvider,
    provider: ProviderChangeSinkBackend,
    observer: RwLock<Option<Arc<dyn SlotAdvancer>>>,
    metrics: CdcMetrics,
}

impl CayenneChangeSinkBackend {
    #[must_use]
    pub fn try_new(context: ChangeSinkContext) -> Option<Arc<dyn ChangeSinkBackend>> {
        let table =
            find_concrete::<CayenneTableProvider>(context.table.as_ref(), LayerWalk::Write)?
                .clone_for_write_operations();
        let provider = ProviderChangeSinkBackend::new(context.clone());
        Some(Arc::new(Self {
            metrics: CdcMetrics::new(&context.dataset_name),
            context,
            table,
            provider,
            observer: RwLock::new(None),
        }))
    }

    /// A checkpoint must cover buffered work before durable writes can supersede
    /// it. Rebuildable writes need no source callback; a real CDC callback must
    /// finish its outstanding acknowledgements before being removed.
    async fn select_recovery_path(&self, recovery: Recovery) -> Result<()> {
        let buffered = self.table.is_cdc_memory_mode() && !self.table.is_memory_resident_mode();
        let observer = if recovery == Recovery::Replayable && buffered {
            self.observer.read().clone()
        } else {
            None
        };
        if let Some(observer) = observer {
            self.table.install_slot_advancer(observer);
        } else if buffered && (recovery != Recovery::Rebuildable || self.table.has_slot_advancer())
        {
            self.table
                .checkpoint_mem_tier()
                .await
                .map_err(DataFusionError::from)?;
            self.table.clear_slot_advancer();
        }
        Ok(())
    }

    async fn apply_cdc(
        &self,
        batch: CdcBatch,
        options: WriteOptions,
        ctx: &SessionContext,
    ) -> Result<BackendWrite> {
        let groups = group_into_sub_batches(&batch);
        reject_unknown_operations(&groups)?;
        let replayable = options.recovery == Recovery::Replayable
            && groups.iter().all(|(op, rows)| match op {
                ChangeOperationType::Upsert => true,
                ChangeOperationType::Delete => {
                    self.table.supports_in_memory_cdc_deletes()
                        && rows.iter().all(|&row| batch.has_primary_keys(row))
                }
                ChangeOperationType::Truncate | ChangeOperationType::Unknown => false,
            });
        self.select_recovery_path(if replayable {
            Recovery::Replayable
        } else {
            Recovery::Durable
        })
        .await?;

        let changed = !groups.is_empty();
        let mut finalizer: Option<BoxFuture<'static, Result<()>>> = None;
        let mut max_epoch: Option<u64> = None;
        for (op, rows) in groups {
            // A later sub-operation can conflict with the previous append.
            if let Some(pending) = finalizer.take() {
                pending.await?;
            }
            let epoch = match op {
                ChangeOperationType::Upsert => {
                    let selected = try_cast_to(
                        select_rows(&batch.data_batch(), &rows)?,
                        self.context.table.schema(),
                    )?;
                    let input = Box::pin(RecordBatchStreamAdapter::new(
                        selected.schema(),
                        stream::once(async move { Ok(selected) }),
                    ));
                    let write = self
                        .table
                        .write_cdc_append_stream_with_source_commit_ts(
                            input,
                            batch.source_commit_ts_ms(),
                            &ctx.task_ctx(),
                        )
                        .await
                        .map_err(DataFusionError::from)?;
                    let epoch = write.in_memory_epoch();
                    if write.has_pending_finalize() {
                        self.metrics.path("durable_append");
                        // The driver owns and polls this future even if the
                        // producer drops its receipt. No task is spawned here.
                        // Dropping an unpublished staged write retains its WAL;
                        // it is recovery-required, not rolled back. A failed
                        // predecessor must fence this owner before dropping it.
                        finalizer = Some(Box::pin(async move {
                            write
                                .finish()
                                .await
                                .map(|_| ())
                                .map_err(DataFusionError::from)
                        }));
                    } else {
                        self.metrics.path("inmem_append");
                        write.finish().await.map_err(DataFusionError::from)?;
                    }
                    epoch
                }
                ChangeOperationType::Delete => self.delete_cdc(&batch, &rows, options, ctx).await?,
                ChangeOperationType::Truncate => {
                    self.provider.truncate(ctx).await?;
                    None
                }
                ChangeOperationType::Unknown => unreachable!("validated before mutation"),
            };
            if let Some(epoch) = epoch {
                max_epoch = Some(max_epoch.map_or(epoch, |current| current.max(epoch)));
            }
        }
        Ok(BackendWrite {
            changed,
            durability: self.storage_durability(max_epoch),
            finalizer,
        })
    }

    fn storage_durability(&self, epoch: Option<u64>) -> StorageDurability {
        if self.table.is_memory_resident_mode() {
            StorageDurability::NotPromised
        } else {
            epoch.map_or(StorageDurability::Durable, StorageDurability::Deferred)
        }
    }

    async fn delete_cdc(
        &self,
        batch: &CdcBatch,
        rows: &[usize],
        options: WriteOptions,
        ctx: &SessionContext,
    ) -> Result<Option<u64>> {
        let cap = options.delete_batch_size.max(1);
        self.metrics.delete_keys(batch, rows);
        let fallthrough = 'absorb: {
            if !self.table.supports_in_memory_cdc_deletes() {
                break 'absorb "no_capability";
            }
            if !self.table.has_slot_advancer() {
                break 'absorb "no_advancer";
            }
            if !rows.iter().all(|&row| batch.has_primary_keys(row)) {
                break 'absorb "inextractable_keys";
            }
            let selected = select_rows(&batch.data_batch(), rows)?;
            if let Some(epoch) = self
                .table
                .write_cdc_delete_keys_in_memory(&selected)
                .await
                .map_err(DataFusionError::from)?
            {
                for chunk in rows.chunks(cap) {
                    delete_index_keys(&self.context, batch, chunk, true).await?;
                }
                self.metrics.path("inmem_delete");
                return Ok(Some(epoch));
            }
            "budget"
        };
        self.metrics.delete_fallthrough(fallthrough);
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
        for chunk in keyed.chunks(cap) {
            if let Some(filter) = build_batch_delete_expr_from_change_batch(
                batch,
                chunk,
                &self.context.dataset_name.to_string(),
            )? {
                let native = self
                    .table
                    .delete_from_cdc_fast(std::slice::from_ref(&filter))
                    .await?
                    .is_some();
                if !native {
                    self.provider.delete_filter(filter, ctx).await?;
                }
                delete_index_keys(&self.context, batch, chunk, native).await?;
            }
        }
        self.metrics.fixed_cost("durable_delete_apply", apply_start);
        let maintenance_start = Instant::now();
        perform_change_write_maintenance(&self.context.table).await?;
        self.metrics
            .fixed_cost("durable_delete_maintenance", maintenance_start);
        Ok(None)
    }
}

#[async_trait]
impl ChangeSinkBackend for CayenneChangeSinkBackend {
    fn schema(&self) -> SchemaRef {
        self.context.table.schema()
    }

    fn capabilities(&self) -> ChangeCapabilities {
        ChangeCapabilities {
            replacement: ReplacementSupport::Ordered,
            deferred_durability: self.table.is_cdc_memory_mode()
                && !self.table.is_memory_resident_mode(),
            deferred_deletes: self.table.supports_in_memory_cdc_deletes()
                && !self.table.is_memory_resident_mode(),
            schema_evolution: SchemaEvolutionSupport::Live,
        }
    }

    async fn apply(
        &self,
        batch: ChangeBatch,
        options: WriteOptions,
        ctx: &SessionContext,
    ) -> Result<BackendWrite> {
        let (payload, scope) = batch.into_parts();
        match payload {
            ChangePayload::Cdc(rows) => {
                let batch = rows.into_built().map_err(before_mutation)?;
                self.apply_cdc(batch, options, ctx).await
            }
            ChangePayload::Rows {
                schema,
                batches,
                append_validations,
            } => {
                let batches = if self.table.is_memory_resident_mode()
                    && batches.iter().any(|batch| batch.num_rows() > 0)
                {
                    let target = self.context.table.schema();
                    let batches = batches
                        .into_iter()
                        .map(|batch| try_cast_to(batch, Arc::clone(&target)).map_err(Into::into))
                        .collect::<Result<Vec<_>>>()
                        .map_err(before_mutation)?;
                    let incoming_bytes = batches
                        .iter()
                        .map(|batch| {
                            u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX)
                        })
                        .fold(0_u64, u64::saturating_add);
                    let filters = scope.as_ref().map(SetKey::filters);
                    self.table
                        .preflight_memory_append(incoming_bytes, filters.as_deref())
                        .await
                        .map_err(|error| before_mutation(error.into()))?;
                    batches
                } else {
                    batches
                };
                let rebuildable = options.recovery == Recovery::Rebuildable;
                self.select_recovery_path(if rebuildable {
                    Recovery::Rebuildable
                } else {
                    Recovery::Durable
                })
                .await?;
                let rebuildable_context = rebuildable.then(|| {
                    let mut state = ctx.state();
                    state
                        .config_mut()
                        .set_extension(Arc::new(RebuildableWrite::new(&self.table)));
                    SessionContext::new_with_state(state)
                });
                let ctx = rebuildable_context.as_ref().unwrap_or(ctx);
                let changed = self
                    .provider
                    .apply_rows(schema, batches, scope, append_validations, options, ctx)
                    .await?;
                Ok(BackendWrite::complete(
                    changed,
                    if rebuildable {
                        StorageDurability::NotPromised
                    } else {
                        self.storage_durability(None)
                    },
                ))
            }
        }
    }

    fn set_durability_observer(&self, observer: Arc<dyn DurabilityObserver>) {
        *self.observer.write() = Some(Arc::new(StorageFenceObserver { observer }));
    }

    async fn flush(&self) -> Result<()> {
        if self.table.is_cdc_memory_mode() && !self.table.is_memory_resident_mode() {
            self.table
                .checkpoint_mem_tier()
                .await
                .map_err(DataFusionError::from)?;
        }
        Ok(())
    }

    async fn evolve_schema(&self, plan: &WideningPlan) -> Result<()> {
        let _guard = self.context.write_lock.lock().await;
        self.table
            .evolve_schema_live(plan)
            .await
            .map_err(DataFusionError::from)
    }
}
