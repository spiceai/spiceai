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

//! Scoped preparation and atomic publication over the shared resident RAM tier.
//! Durable tiers require deletion intents that survive checkpointing; this path
//! accepts only keyless, single-shard, permanently memory-resident tables.

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use arrow::array::RecordBatch;
use arrow::compute::{filter_record_batch, not, or};
use data_components::cdc::mutation::ReplaceSet;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::Expr;

use super::{CayenneCdcWrite, CayenneTableProvider};
use crate::provider::file_pruning::matching_statistics;
use crate::provider::mem_tier::SegmentTombstones;

impl CayenneTableProvider {
    /// Prepare every affected segment privately and publish the retained rows
    /// and new members with one tier swap. The write guard orders preparation
    /// with SQL writes, native row mutations, and schema changes. Existing scans
    /// retain their immutable tier; no delete is exposed before its new rows.
    pub(crate) async fn write_keyless_memory_replacements(
        &self,
        replacements: Vec<ReplaceSet>,
    ) -> Result<CayenneCdcWrite> {
        let start = Instant::now();
        let _write = self.write_lock_arc().lock_owned().await;
        self.ensure_no_incomplete_write().await?;
        if !self.is_memory_resident_mode()
            || !self.pk_column_names().is_empty()
            || self.mem_tier_shard_count() != 1
        {
            return Err(DataFusionError::NotImplemented(
                "Scoped RAM replacement requires a keyless, single-shard memory table".into(),
            ));
        }

        let schema = self.table_schema();
        let mut keys = Vec::with_capacity(replacements.len());
        let mut batches = Vec::new();
        let mut incoming_bytes = 0_u64;
        let mut incoming_rows = 0_u64;
        for replacement in replacements {
            if replacement.schema().fields() != schema.fields()
                && replacement.schema().fields() != self.read_schema().fields()
            {
                return Err(DataFusionError::Plan(
                    "Table schema changed before complete-set replacement".into(),
                ));
            }
            let (key, members) = replacement.into_parts();
            keys.push(key);
            for batch in members {
                let batch = arrow_tools::record_batch::try_cast_to(batch, Arc::clone(&schema))?;
                incoming_rows = incoming_rows
                    .checked_add(batch.num_rows() as u64)
                    .ok_or_else(|| {
                        DataFusionError::Plan(
                            "Complete-set replacement row count exceeds u64".into(),
                        )
                    })?;
                incoming_bytes =
                    incoming_bytes.saturating_add(batch.get_array_memory_size() as u64);
                self.enforce_memory_limit(incoming_bytes)?;
                if batch.num_rows() > 0 {
                    batches.push(batch);
                }
                tokio::task::yield_now().await;
            }
        }
        let predicate = keys
            .iter()
            .filter_map(|key| key.filters().into_iter().reduce(Expr::and))
            .reduce(Expr::or)
            .ok_or_else(|| DataFusionError::Plan("A replacement batch must not be empty".into()))?;
        let filters = self.coerce_filters_for_inlined_delete(&[predicate])?;
        let physical = self.build_physical_filters_for_inlined_delete(&filters)?;
        let predicate = physical.into_iter().next().ok_or_else(|| {
            DataFusionError::Internal("Missing complete-set replacement predicate".into())
        })?;

        let current = self.mem_tier.shard(0).load_full();
        let captured = Arc::clone(&current);
        let prepare_table = self.clone_for_write_operations();
        let (retained, removed) = tokio::task::spawn_blocking(move || {
            let candidates = matching_statistics(
                captured
                    .segments
                    .iter()
                    .map(|segment| Arc::clone(&segment.statistics))
                    .collect(),
                &schema,
                &predicate,
            );
            let mut staged_bytes = incoming_bytes;
            let (mut retained, removed) = captured.retain_rows_matching(
                |index| candidates.get(index).copied().unwrap_or(true),
                |batch, _sequence| {
                    let mut keys = keys.iter();
                    let first = keys.next().ok_or_else(|| {
                        DataFusionError::Internal("Missing complete-set replacement key".into())
                    })?;
                    let mut matching = first.matching_rows(batch)?;
                    for key in keys {
                        matching = or(&matching, &key.matching_rows(batch)?)?;
                    }
                    match matching.true_count() {
                        0 => Ok(batch.clone()),
                        count if count == batch.num_rows() => {
                            Ok(RecordBatch::new_empty(batch.schema()))
                        }
                        _ => {
                            // Retained slices of a touched batch can allocate new
                            // buffers. Admit a batch-sized estimate before filtering,
                            // alongside the old tier and the incoming members.
                            staged_bytes =
                                staged_bytes.saturating_add(batch.get_array_memory_size() as u64);
                            prepare_table.enforce_memory_limit(staged_bytes)?;
                            Ok(filter_record_batch(batch, &not(&matching)?)?)
                        }
                    }
                },
                |batch| {
                    prepare_table
                        .mem_tier_index
                        .as_ref()
                        .and_then(|indexer| indexer.index_batch(batch))
                },
            )?;
            // A resident keyless tier has neither durable prefixes nor PK
            // tombstones. Empty segments carry no state that a later checkpoint
            // needs; keeping them would grow metadata on every refresh.
            if retained.sealed_segments == 0 && retained.tombstones.is_empty() {
                retained.segments = Arc::new(
                    retained
                        .segments
                        .iter()
                        .filter(|segment| segment.rows > 0)
                        .cloned()
                        .collect(),
                );
            }
            Ok::<_, DataFusionError>((retained, removed))
        })
        .await
        .map_err(|error| DataFusionError::External(Box::new(error)))??;

        let batches = Arc::new(batches);
        let index = self.index_mem_tier_segment(&batches).await;
        // Memory-resident tables have no checkpoint/seal writer. The write
        // guard therefore orders this reservation with every tier mutation.
        let sequence = self
            .reserve_sequences_local(1)
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let next = tokio::task::spawn_blocking(move || {
            retained.append_segment_with_source_position(
                batches,
                sequence,
                SegmentTombstones::default(),
                incoming_bytes,
                incoming_rows,
                0,
                None,
                index,
            )
        })
        .await
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let epoch = next.epoch;
        {
            let _publish = self.mem_tier_publish_locks[0].lock().await;
            if !Arc::ptr_eq(&current, &self.mem_tier.shard(0).load_full()) {
                return Err(DataFusionError::Execution(
                    "Memory tier changed during complete-set replacement preparation".into(),
                ));
            }
            let _visibility = self.begin_maintained_aggregate_visibility_write();
            self.mark_maintained_aggregates_stale();
            self.inlined_row_count.store(
                i64::try_from(next.rows).unwrap_or(i64::MAX),
                Ordering::Relaxed,
            );
            self.mem_tier.shard(0).store(Arc::new(next));
            self.notify_scan_input_change();
            self.clear_scan_file_statistics_cache();
        }
        self.context().record_ingest(
            incoming_rows,
            removed,
            incoming_bytes,
            start.elapsed(),
            None,
        );
        Ok(CayenneCdcWrite::in_memory_staged(
            self.clone_for_write_operations(),
            incoming_rows,
            epoch,
        ))
    }
}
