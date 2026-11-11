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

//! `retention_sql` applied to a write's unpublished snapshot, so the rows it
//! deletes are hidden in the same publication that makes the write visible.
//!
//! A reader sees the generation before the write or the one after both the write
//! and its retention delete, never the written rows the delete removes. The
//! predicate is the configured `DELETE ... WHERE` evaluated exactly as a retention
//! pass evaluates it: a row is deleted only where every conjunct is TRUE, so a
//! NULL result keeps the row, and `now()` is folded once for the whole
//! publication. The matches become position deletes on the staged files, which
//! the publication commits with the snapshot pointer. A predicate that fails
//! fails the write, which abandons the snapshot and leaves the previous
//! generation in place.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array as _, AsArray, BooleanArray, RecordBatch};
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef, UInt64Type};
use datafusion::common::DFSchema;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{PhysicalExpr, create_physical_expr};
use futures::{StreamExt, TryStreamExt};
use vortex::VortexSessionDefault;
use vortex::array::VortexSessionExecute;
use vortex::arrow::ArrowSessionExt;
use vortex::dtype::Nullability;
use vortex::expr::{get_item, pack, root};
use vortex::file::OpenOptionsSessionExt;
use vortex::layout::layouts::row_idx::row_idx;
use vortex_session::VortexSession;

use super::table::CayenneTableProvider;

/// The read-back column carrying each row's position in its file.
const POSITION_COLUMN: &str = "__cayenne_retention_position";

impl CayenneTableProvider {
    /// Add the rows of `snapshot_id`'s files that `retention_sql` deletes to
    /// `position_deletions`, the positions the write already hides (the copies its
    /// repeated keys superseded). The predicate is evaluated only over the rows
    /// still visible, in file order, so it sees exactly the generation a
    /// `DELETE ... WHERE` after the publish would: a superseded copy is never
    /// evaluated, so it can neither fail the write nor draw from a volatile
    /// function. Each file's positions come back sorted and unique.
    ///
    /// # Errors
    ///
    /// Returns an error when the predicate cannot be prepared or evaluated over a
    /// staged row, or a staged file cannot be read. The caller abandons the
    /// snapshot: publishing it would serve rows retention owes a delete.
    pub(crate) async fn hide_retention_matches_in_snapshot(
        &self,
        snapshot_id: &str,
        mut position_deletions: HashMap<String, Vec<u32>>,
    ) -> super::Result<HashMap<String, Vec<u32>>> {
        if !self.has_retention_delete_filters() {
            return Ok(position_deletions);
        }
        let matches = self
            .retention_matches_in_snapshot(snapshot_id, &position_deletions)
            .await
            .map_err(|error| super::Error::Internal {
                table: self.table_name().to_string(),
                message: format!(
                    "{} The refresh was not published; the previous data is still served. {error}",
                    self.retention_failed_message()
                ),
            })?;
        let matched: u64 = matches.values().map(|rows| rows.len() as u64).sum();
        position_deletions = union_positions(position_deletions, matches);
        if matched > 0 {
            tracing::info!(
                table = self.table_name(),
                "Retention deleted {matched} row(s) matching `retention_sql` from accelerated dataset '{}' before publishing the write",
                self.table_name()
            );
        }
        Ok(position_deletions)
    }

    /// The rows of an overwrite buffered for a `mode: memory` table that
    /// `retention_sql` keeps, after its repeated keys are resolved. The tier swap
    /// that follows publishes them in one step, so the deleted rows are never
    /// visible; an error leaves the resident tier untouched.
    ///
    /// # Errors
    ///
    /// Returns an error when the predicate cannot be prepared or evaluated.
    pub(crate) fn drop_retention_matches(
        &self,
        batches: Vec<RecordBatch>,
    ) -> super::Result<Vec<RecordBatch>> {
        let mut groups = self.drop_retention_matches_in_groups(vec![batches])?;
        Ok(groups.pop().unwrap_or_default())
    }

    /// [`Self::drop_retention_matches`] over the groups one publication carries,
    /// such as the shards of one sharded CDC apply: the predicate is prepared
    /// once, with one `now()`, for every group, and every group is evaluated
    /// before the caller publishes any of them. An error drops them all.
    ///
    /// # Errors
    ///
    /// Returns an error when the predicate cannot be prepared or evaluated.
    pub(crate) fn drop_retention_matches_in_groups(
        &self,
        groups: Vec<Vec<RecordBatch>>,
    ) -> super::Result<Vec<Vec<RecordBatch>>> {
        let Some(schema) = groups.iter().flatten().next().map(RecordBatch::schema) else {
            return Ok(groups);
        };
        if !self.has_retention_delete_filters() {
            return Ok(groups);
        }
        let failed = |error: datafusion_common::DataFusionError| super::Error::Internal {
            table: self.table_name().to_string(),
            message: format!(
                "{} The refresh was not published; the previous data is still served. {error}",
                self.retention_failed_message()
            ),
        };
        let filters = self.coerced_retention_filters().map_err(failed)?;
        let physical = physical_filters(&filters, &schema).map_err(failed)?;
        let mut matched = 0_usize;
        let mut out = Vec::with_capacity(groups.len());
        for batches in groups {
            let mut kept = Vec::with_capacity(batches.len());
            for batch in batches {
                let deleted = deleted_rows(&physical, &batch).map_err(failed)?;
                let keep: BooleanArray = deleted.iter().map(|d| Some(d != Some(true))).collect();
                let batch = arrow::compute::filter_record_batch(&batch, &keep)
                    .map_err(|e| failed(e.into()))?;
                matched += keep.len() - batch.num_rows();
                if batch.num_rows() > 0 {
                    kept.push(batch);
                }
            }
            out.push(kept);
        }
        if matched > 0 {
            tracing::info!(
                table = self.table_name(),
                "Retention deleted {matched} row(s) matching `retention_sql` from accelerated dataset '{}' before publishing the write",
                self.table_name()
            );
        }
        Ok(out)
    }

    /// `stream` without the rows `retention_sql` deletes, for an append to a table
    /// with no primary key. Such an append supersedes nothing, so the rows a
    /// `DELETE ... WHERE` after it would remove are exactly the rows of the stream
    /// the predicate is TRUE for; dropping them before the write means the
    /// publication never carries them. The predicate is prepared once, with one
    /// `now()`, for the whole stream, and evaluated once per row. An error fails
    /// the append before anything is published. The counter it returns holds the
    /// rows dropped once the stream is drained, so the append can still report
    /// every row it inserted, as it did when the delete ran after it.
    ///
    /// # Errors
    ///
    /// Returns an error when the predicate cannot be prepared.
    pub(crate) fn drop_retention_matches_from_stream(
        &self,
        stream: datafusion::execution::SendableRecordBatchStream,
    ) -> super::Result<(
        datafusion::execution::SendableRecordBatchStream,
        Arc<std::sync::atomic::AtomicU64>,
    )> {
        let dropped = Arc::new(std::sync::atomic::AtomicU64::new(0));
        if !self.has_retention_delete_filters() {
            return Ok((stream, dropped));
        }
        let counter = Arc::clone(&dropped);
        let message = self.retention_failed_message();
        let failed = move |error: datafusion_common::DataFusionError| {
            datafusion_common::DataFusionError::Execution(format!(
                "{message} The write was not published. {error}"
            ))
        };
        let schema = stream.schema();
        let filters = self
            .coerced_retention_filters()
            .map_err(|e| super::Error::DataFusion { source: failed(e) })?;
        let physical = physical_filters(&filters, &schema)
            .map_err(|e| super::Error::DataFusion { source: failed(e) })?;
        let filtered = stream.map(move |batch| {
            let batch = batch?;
            let deleted = deleted_rows(&physical, &batch).map_err(&failed)?;
            let keep: BooleanArray = deleted.iter().map(|d| Some(d != Some(true))).collect();
            let kept =
                arrow::compute::filter_record_batch(&batch, &keep).map_err(|e| failed(e.into()))?;
            counter.fetch_add(
                (batch.num_rows() - kept.num_rows()) as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
            Ok(kept)
        });
        Ok((
            Box::pin(
                datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, filtered),
            ),
            dropped,
        ))
    }

    /// Each staged file's positions where every `retention_sql` conjunct is TRUE.
    async fn retention_matches_in_snapshot(
        &self,
        snapshot_id: &str,
        hidden: &HashMap<String, Vec<u32>>,
    ) -> datafusion_common::Result<HashMap<String, Vec<u32>>> {
        let filters = self.coerced_retention_filters()?;
        let table_schema = self.table_schema();
        let mut names: Vec<String> = Vec::new();
        for filter in &filters {
            for column in filter.column_refs() {
                if !names.iter().any(|name| name == column.name()) {
                    names.push(column.name().to_string());
                }
            }
        }
        let mut fields: Vec<FieldRef> = names
            .iter()
            .map(|name| {
                table_schema
                    .field_with_name(name)
                    .map(|f| Arc::new(f.clone()))
            })
            .collect::<Result<_, _>>()?;
        let data_schema: SchemaRef = Arc::new(Schema::new(fields.clone()));
        let physical = physical_filters(&filters, &data_schema)?;
        fields.push(Arc::new(Field::new(
            POSITION_COLUMN,
            DataType::UInt64,
            false,
        )));
        let stored = Arc::new(Field::new_struct("", fields, false));

        let ctx = datafusion::prelude::SessionContext::new_with_config_rt(
            util::session_state::session_config(),
            self.runtime_env(),
        );
        let state = ctx.state();
        let Some((store, files)) = self
            .snapshot_file_metadata(&state, snapshot_id, &self.read_schema())
            .await
        else {
            return Err(datafusion_common::DataFusionError::Internal(
                "failed to list the refresh's staged files".to_string(),
            ));
        };

        let external = |e| datafusion_common::DataFusionError::External(Box::new(e));
        let mut matches: HashMap<String, Vec<u32>> = HashMap::new();
        for file in files {
            let path = file.location.to_string();
            // Sorted, so each row's visibility is a binary search.
            let hidden_here: Vec<u32> = hidden.get(&path).map_or_else(Vec::new, |rows| {
                let mut rows = rows.clone();
                rows.sort_unstable();
                rows
            });
            let session = VortexSession::default();
            let vxf = session
                .open_options()
                .open_object_store(&store, file.location.clone())
                .await
                .map_err(external)?;
            let projection = pack(
                names
                    .iter()
                    .map(|name| (name.clone(), get_item(name.as_str(), root())))
                    .chain([(POSITION_COLUMN.to_string(), row_idx())]),
                Nullability::NonNullable,
            );
            let mut chunks = vxf
                .scan()
                .map_err(external)?
                .with_projection(projection.bind(vxf.dtype()).map_err(external)?)
                .into_stream()
                .map_err(external)?;
            let mut positions: Vec<u32> = Vec::new();
            while let Some(chunk) = chunks.try_next().await.map_err(external)? {
                if chunk.is_empty() {
                    continue;
                }
                let mut exec = session.create_execution_ctx();
                let array = session
                    .arrow()
                    .execute_arrow(chunk, Some(&stored), &mut exec)
                    .map_err(external)?;
                let batch = RecordBatch::from(array.as_struct_opt().ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(format!(
                        "{path}: read back did not return a struct"
                    ))
                })?);
                // Drop the superseded copies before the predicate sees the batch.
                let batch = if hidden_here.is_empty() {
                    batch
                } else {
                    let positions = batch
                        .column(batch.num_columns() - 1)
                        .as_primitive::<UInt64Type>();
                    let visible: BooleanArray = positions
                        .values()
                        .iter()
                        .map(|&row| {
                            Some(
                                u32::try_from(row)
                                    .map_or(true, |row| hidden_here.binary_search(&row).is_err()),
                            )
                        })
                        .collect();
                    arrow::compute::filter_record_batch(&batch, &visible)?
                };
                if batch.num_rows() == 0 {
                    continue;
                }
                let rows = batch
                    .column(batch.num_columns() - 1)
                    .as_primitive::<UInt64Type>();
                let data = batch.project(&(0..batch.num_columns() - 1).collect::<Vec<_>>())?;
                let data = RecordBatch::try_new_with_options(
                    Arc::clone(&data_schema),
                    data.columns().to_vec(),
                    &arrow::array::RecordBatchOptions::new().with_row_count(Some(data.num_rows())),
                )?;
                let deleted = deleted_rows(&physical, &data)?;
                for (index, delete) in deleted.iter().enumerate() {
                    if delete == Some(true) {
                        let row = u32::try_from(rows.value(index)).map_err(|_| {
                            datafusion_common::DataFusionError::Internal(format!(
                                "{path}: row position {} exceeds u32",
                                rows.value(index)
                            ))
                        })?;
                        positions.push(row);
                    }
                }
            }
            if !positions.is_empty() {
                matches.insert(path, positions);
            }
        }
        Ok(matches)
    }
}

/// `a` and `b` together, each file's positions sorted and unique.
pub(crate) fn union_positions(
    mut a: HashMap<String, Vec<u32>>,
    b: HashMap<String, Vec<u32>>,
) -> HashMap<String, Vec<u32>> {
    for (file, rows) in b {
        let entry = a.entry(file).or_default();
        entry.extend(rows);
        entry.sort_unstable();
        entry.dedup();
    }
    a
}

fn physical_filters(
    filters: &[Expr],
    schema: &SchemaRef,
) -> datafusion_common::Result<Vec<Arc<dyn PhysicalExpr>>> {
    let df_schema = DFSchema::try_from(Arc::clone(schema))?;
    let props = ExecutionProps::new();
    filters
        .iter()
        .map(|filter| {
            create_physical_expr(
                filter,
                &df_schema,
                &props,
                &datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext::default(),
            )
        })
        .collect()
}

/// TRUE where every conjunct is TRUE: the rows `DELETE ... WHERE` removes. A
/// NULL or FALSE conjunct keeps the row.
fn deleted_rows(
    filters: &[Arc<dyn PhysicalExpr>],
    batch: &RecordBatch,
) -> datafusion_common::Result<BooleanArray> {
    let mut deleted = BooleanArray::from(vec![true; batch.num_rows()]);
    for filter in filters {
        let value = filter.evaluate(batch)?.into_array(batch.num_rows())?;
        let value = value.as_boolean_opt().ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(format!(
                "retention_sql predicate evaluated to {}, not a boolean",
                value.data_type()
            ))
        })?;
        deleted = deleted
            .iter()
            .zip(value.iter())
            .map(|(acc, v)| Some(acc == Some(true) && v == Some(true)))
            .collect();
    }
    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use datafusion::prelude::{col, lit};

    #[test]
    fn null_and_false_keep_the_row() -> datafusion_common::Result<()> {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("status", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![
                    Some("archived"),
                    None,
                    Some("active"),
                ])),
            ],
        )?;
        let filters = physical_filters(&[col("status").eq(lit("archived"))], &schema)?;
        let deleted = deleted_rows(&filters, &batch)?;
        assert_eq!(
            deleted.iter().collect::<Vec<_>>(),
            vec![Some(true), Some(false), Some(false)]
        );
        // Conjuncts: a row is deleted only when both are TRUE.
        let filters = physical_filters(
            &[col("status").eq(lit("archived")), col("id").gt(lit(1_i64))],
            &schema,
        )?;
        assert_eq!(
            deleted_rows(&filters, &batch)?.iter().collect::<Vec<_>>(),
            vec![Some(false), Some(false), Some(false)]
        );
        Ok(())
    }
}

tokio::task_local! {
    /// The coerced `retention_sql` conjuncts of the publication in progress, so
    /// every evaluation inside it (incoming rows, resident rows) shares one
    /// `now()` instead of re-simplifying per call.
    static PUBLICATION_RETENTION: Arc<Vec<Expr>>;
}

/// Run `work` with `filters` as the publication's `retention_sql` context.
pub(crate) async fn with_publication_retention<F: std::future::Future>(
    filters: Arc<Vec<Expr>>,
    work: F,
) -> F::Output {
    PUBLICATION_RETENTION.scope(filters, work).await
}

/// The publication's `retention_sql` conjuncts, when one is in progress.
pub(crate) fn publication_retention_filters() -> Option<Vec<Expr>> {
    PUBLICATION_RETENTION
        .try_with(|filters| filters.as_ref().clone())
        .ok()
}

impl CayenneTableProvider {
    /// Prepares the `retention_sql` delete over the resident rows a keyed
    /// append keeps, against the generation it replaces. The rows whose keys
    /// the staged snapshot rewrites, and the rows already deleted, are skipped
    /// first: the predicate never sees them, so a hidden version cannot fail or
    /// be counted. Nothing is
    /// committed; the append's transaction takes the returned delete files.
    pub(crate) async fn prepare_resident_retention(
        &self,
        snapshot_id: &str,
        filters: &[Expr],
        superseded_positions: &std::collections::HashMap<std::sync::Arc<str>, Vec<u64>>,
    ) -> super::Result<Option<super::delete::PreparedDeletionPublish>> {
        let failed = |error: datafusion_common::DataFusionError| super::Error::DataFusion {
            source: datafusion_common::DataFusionError::Execution(format!(
                "{} The write was not published. {error}",
                self.retention_failed_message()
            )),
        };
        let schema = self.table_schema();
        let keys: Vec<String> = self
            .primary_key_indices()?
            .unwrap_or_default()
            .into_iter()
            .map(|index| schema.field(index).name().clone())
            .collect();
        if keys.is_empty() {
            return Err(super::Error::Internal {
                table: self.table_name().to_string(),
                message: "resident retention needs a primary key to exclude superseded rows"
                    .to_string(),
            });
        }
        if self.is_position_based() {
            // A position delete addresses (file, row): the write's superseded
            // positions are excluded from each file's scan with its deletion
            // vector, so neither they nor deleted rows reach the predicate.
            let mut positions: std::collections::HashMap<String, roaring::RoaringBitmap> =
                std::collections::HashMap::new();
            for (file, rows) in superseded_positions {
                let bitmap = positions.entry(file.to_string()).or_default();
                for row in rows {
                    bitmap.insert(u32::try_from(*row).map_err(|_| super::Error::Internal {
                        table: self.table_name().to_string(),
                        message: format!("superseded position {row} exceeds u32 in '{file}'"),
                    })?);
                }
            }
            let sink = self
                .build_deletion_vector_sink(
                    filters,
                    None,
                    super::table::DeletionRequestSource::User,
                )
                .await
                .map_err(failed)?
                .with_skip_positions(positions);
            let prepared = sink.prepare_delete().await?;
            return Ok(prepared.filter(|prepared| !prepared.delete_files().is_empty()));
        }
        let incoming = self
            .read_staged_columns(snapshot_id, &keys)
            .await
            .map_err(failed)?;
        let sink = self
            .build_deletion_vector_sink(filters, None, super::table::DeletionRequestSource::User)
            .await
            .map_err(failed)?;
        // The staged keys and every row already retired are dropped by key
        // before the predicate runs: bounded by the write, never a predicate
        // whose size grows with it.
        let skip = sink.skip_keys_from(&incoming)?;
        let sink = sink.with_skip_before_filters(skip);
        let prepared = sink.prepare_delete().await?;
        Ok(prepared.filter(|prepared| !prepared.delete_files().is_empty()))
    }

    /// The named columns of every row a staged snapshot wrote.
    async fn read_staged_columns(
        &self,
        snapshot_id: &str,
        names: &[String],
    ) -> datafusion_common::Result<Vec<RecordBatch>> {
        let table_schema = self.table_schema();
        let fields: Vec<FieldRef> = names
            .iter()
            .map(|name| {
                table_schema
                    .field_with_name(name)
                    .map(|f| Arc::new(f.clone()))
            })
            .collect::<Result<_, _>>()?;
        let schema: SchemaRef = Arc::new(Schema::new(fields.clone()));
        let stored = Arc::new(Field::new_struct("", fields, false));
        let ctx = datafusion::prelude::SessionContext::new_with_config_rt(
            util::session_state::session_config(),
            self.runtime_env(),
        );
        let state = ctx.state();
        let Some((store, files)) = self
            .snapshot_file_metadata(&state, snapshot_id, &self.read_schema())
            .await
        else {
            return Err(datafusion_common::DataFusionError::Internal(
                "failed to list the write's staged files".to_string(),
            ));
        };
        let external = |e| datafusion_common::DataFusionError::External(Box::new(e));
        let mut batches = Vec::new();
        for file in files {
            let session = VortexSession::default();
            let vxf = session
                .open_options()
                .open_object_store(&store, file.location.clone())
                .await
                .map_err(external)?;
            let projection = pack(
                names
                    .iter()
                    .map(|name| (name.clone(), get_item(name.as_str(), root()))),
                Nullability::NonNullable,
            );
            let mut chunks = vxf
                .scan()
                .map_err(external)?
                .with_projection(projection.bind(vxf.dtype()).map_err(external)?)
                .into_stream()
                .map_err(external)?;
            while let Some(chunk) = chunks.try_next().await.map_err(external)? {
                if chunk.is_empty() {
                    continue;
                }
                let mut exec = session.create_execution_ctx();
                let array = session
                    .arrow()
                    .execute_arrow(chunk, Some(&stored), &mut exec)
                    .map_err(external)?;
                let batch = RecordBatch::from(array.as_struct_opt().ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(
                        "staged read back did not return a struct".to_string(),
                    )
                })?);
                batches.push(RecordBatch::try_new(
                    Arc::clone(&schema),
                    batch.columns().to_vec(),
                )?);
            }
        }
        Ok(batches)
    }
}

/// [`with_publication_retention`] for synchronous work.
pub(crate) fn with_publication_retention_sync<R>(
    filters: Arc<Vec<Expr>>,
    work: impl FnOnce() -> R,
) -> R {
    PUBLICATION_RETENTION.sync_scope(filters, work)
}
