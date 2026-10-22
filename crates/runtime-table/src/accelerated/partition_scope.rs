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

//! Keeps the change stream of a partitioned acceleration on a cluster executor
//! to the partitions that executor serves.
//!
//! A full or append refresh reads only the served partitions through its
//! refresh SQL. A change stream applies whatever its source sends, so without a
//! scope every executor would hold every row, and the scheduler, which reads
//! each executor's acceleration whole, would return each row once per executor.

use std::collections::HashSet;
use std::sync::Arc;
use std::task::{Poll, ready};

use arrow::array::{Array, BooleanArray, StringArray};
use arrow::compute::{filter_record_batch, prep_null_mask_filter};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use data_components::cdc::{
    ChangeBatch, ChangeEnvelope, ChangeOperation, ChangesStream, StreamError,
};
use datafusion::common::DFSchema;
use datafusion::logical_expr::expr_rewriter::unnormalize_col;
use datafusion::logical_expr::utils::conjunction;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::prelude::SessionContext;
use datafusion_expr::{Expr, lit};
use futures::StreamExt;
use parking_lot::Mutex;

/// The partitions a cluster executor serves for an acceleration its change
/// stream refreshes.
#[derive(Debug)]
pub struct PartitionScope {
    state: Mutex<ScopeState>,
}

#[derive(Debug)]
struct ScopeState {
    /// Each filter must hold; no filter serves no partition.
    filters: Vec<Expr>,
    /// Counts the replacements of `filters`.
    generation: u64,
    /// The filters compiled against the schema of the rows last scoped.
    compiled: Option<(SchemaRef, Arc<dyn PhysicalExpr>)>,
}

impl PartitionScope {
    #[must_use]
    pub fn new(filters: Vec<Expr>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(ScopeState {
                filters,
                generation: 0,
                compiled: None,
            }),
        })
    }

    /// Replaces the partitions served. No filter serves no partition.
    pub fn set(&self, filters: Vec<Expr>) {
        let mut state = self.state.lock();
        state.filters = filters;
        state.generation = state.generation.wrapping_add(1);
        state.compiled = None;
    }

    fn generation(&self) -> u64 {
        self.state.lock().generation
    }

    /// Keeps the changes of `batch` that belong to the served partitions.
    ///
    /// A delete or truncate always applies: deleting a key this executor does
    /// not hold changes nothing. An insert or snapshot row outside the served
    /// partitions is dropped. An update outside them becomes a delete of its key,
    /// because the row may have moved out of a served partition; when every
    /// partition expression reads only key columns a row cannot move, and the
    /// update is dropped instead. A batch that replaces the acceleration keeps
    /// only its served rows.
    ///
    /// # Errors
    ///
    /// Returns an error if the partition filters do not evaluate against the
    /// batch's rows.
    pub fn scope(&self, batch: ChangeBatch) -> Result<ChangeBatch, StreamError> {
        self.scope_counted(batch).map(|(batch, _)| batch)
    }

    /// [`Self::scope`], and the generation of the filters it applied.
    fn scope_counted(&self, batch: ChangeBatch) -> Result<(ChangeBatch, u64), StreamError> {
        let generation = self.generation();
        if batch.is_heartbeat() {
            return Ok((batch, generation));
        }
        let (served, key_only, generation) = self.served(&batch)?;
        let rows = batch.record.num_rows();
        if batch.rebuild_from_this_batch() {
            return if served.true_count() == rows {
                Ok((batch, generation))
            } else {
                with_record(&batch, filter_record_batch(&batch.record, &served))
                    .map(|batch| (batch, generation))
            };
        }

        let mut keep = Vec::with_capacity(rows);
        let mut deletes = Vec::new();
        for row in 0..rows {
            let in_scope = served.value(row);
            match batch.op(row) {
                ChangeOperation::Create | ChangeOperation::Read => keep.push(in_scope),
                ChangeOperation::Update if !in_scope => {
                    keep.push(!key_only);
                    if !key_only {
                        deletes.push(row);
                    }
                }
                _ => keep.push(true),
            }
        }
        if deletes.is_empty() && keep.iter().all(|&kept| kept) {
            return Ok((batch, generation));
        }

        let mut record = batch.record.clone();
        if !deletes.is_empty() {
            let (op_index, _) = record.schema().column_with_name("op").ok_or_else(|| {
                StreamError::External("A change batch has no 'op' column".to_string())
            })?;
            let mut ops: Vec<String> = (0..rows).map(|row| batch.op(row).to_string()).collect();
            for row in deletes {
                ops[row] = ChangeOperation::Delete.to_string();
            }
            let mut columns = record.columns().to_vec();
            columns[op_index] = Arc::new(StringArray::from(ops));
            record = RecordBatch::try_new(record.schema(), columns)
                .map_err(|e| StreamError::Arrow(e.to_string()))?;
        }
        with_record(
            &batch,
            filter_record_batch(&record, &BooleanArray::from(keep)),
        )
        .map(|batch| (batch, generation))
    }

    /// Whether each row of `batch` lies in a served partition, whether the
    /// partition expressions read only the batch's key columns, and the
    /// generation of the filters evaluated.
    fn served(&self, batch: &ChangeBatch) -> Result<(BooleanArray, bool, u64), StreamError> {
        let data = batch.data_batch();
        let (predicate, columns, generation) = {
            let mut state = self.state.lock();
            let predicate = match &state.compiled {
                Some((schema, predicate)) if *schema == data.schema() => Arc::clone(predicate),
                _ => {
                    let predicate = compile(&state.filters, &data.schema())?;
                    state.compiled = Some((data.schema(), Arc::clone(&predicate)));
                    predicate
                }
            };
            let columns: HashSet<String> = state
                .filters
                .iter()
                .flat_map(Expr::column_refs)
                .map(|column| column.name.clone())
                .collect();
            (predicate, columns, state.generation)
        };

        let served = predicate
            .evaluate(&data)
            .and_then(|value| value.into_array(data.num_rows()))
            .map_err(|e| StreamError::External(e.to_string()))?;
        let served = served
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                StreamError::External(
                    "The partition filters of a change stream do not evaluate to a boolean"
                        .to_string(),
                )
            })?;
        let keys: HashSet<String> = batch.primary_keys(0).into_iter().collect();
        let key_only = !columns.is_empty() && columns.iter().all(|column| keys.contains(column));
        // A row whose partition filter evaluates to null is not served.
        let served = if served.null_count() > 0 {
            prep_null_mask_filter(served)
        } else {
            served.clone()
        };
        Ok((served, key_only, generation))
    }
}

fn compile(filters: &[Expr], schema: &SchemaRef) -> Result<Arc<dyn PhysicalExpr>, StreamError> {
    let predicate =
        conjunction(filters.iter().cloned().map(unnormalize_col)).unwrap_or_else(|| lit(false));
    // Partition values are stored as text, so the filters compare an expression
    // with a string literal; planning coerces the literal to the expression's type.
    DFSchema::try_from(Arc::clone(schema))
        .and_then(|schema| {
            SessionContext::new()
                .state()
                .create_physical_expr(predicate, &schema)
        })
        .map_err(|e| {
            StreamError::External(format!(
                "Cannot evaluate the partition filters of a change stream: {e}"
            ))
        })
}

fn with_record(
    batch: &ChangeBatch,
    record: Result<RecordBatch, arrow::error::ArrowError>,
) -> Result<ChangeBatch, StreamError> {
    let record = record.map_err(|e| StreamError::Arrow(e.to_string()))?;
    Ok(ChangeBatch::try_new(record)?
        .with_source_commit_ts_ms(batch.source_commit_ts_ms())
        .with_rebuild_from_this_batch(batch.rebuild_from_this_batch()))
}

/// Applies `scope` to every change `stream` delivers. Envelopes keep their
/// committer and flags, so a change scoped down to no rows still acknowledges
/// its source position.
///
/// A change that cannot be scoped ends the stream after its error. Skipping it
/// would let later changes acknowledge its source position, so it would never
/// apply. The source's own errors pass through unchanged: a source may continue
/// after them.
#[must_use]
pub fn scope_changes(stream: ChangesStream, scope: Arc<PartitionScope>) -> ChangesStream {
    let mut scoped = stream
        .then(move |item| {
            let scope = Arc::clone(&scope);
            async move {
                match item {
                    Ok(envelope) => (scope_envelope(envelope, scope).await, true),
                    Err(error) => (Err(error), false),
                }
            }
        })
        .boxed();
    let mut ended = false;
    futures::stream::poll_fn(move |cx| {
        if ended {
            return Poll::Ready(None);
        }
        let item = ready!(scoped.poll_next_unpin(cx));
        ended = matches!(item, Some((Err(_), true)));
        Poll::Ready(item.map(|(item, _)| item))
    })
    .boxed()
}

async fn scope_envelope(
    envelope: ChangeEnvelope,
    scope: Arc<PartitionScope>,
) -> Result<ChangeEnvelope, StreamError> {
    let (committer, unscoped, ready, history_unavailable) = envelope.into_parts_offloaded().await?;
    // The reload that follows new filters waits only for the changes the consume
    // loop has received, so a change scoped under replaced filters is scoped
    // again. The comparison runs in the poll that hands the change to the loop.
    let batch = loop {
        let scoping = Arc::clone(&scope);
        let batch = unscoped.clone();
        let (batch, generation) = tokio::task::spawn_blocking(move || scoping.scope_counted(batch))
            .await
            .map_err(|e| StreamError::External(e.to_string()))??;
        if generation == scope.generation() {
            break batch;
        }
    };
    Ok(ChangeEnvelope::from_parts(
        committer,
        batch,
        ready,
        history_unavailable,
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::array::{Int64Array, ListBuilder, StringArray, StringBuilder, StructArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use data_components::cdc::{
        ChangeBatch, ChangeEnvelope, ChangeOperation, NoOpCommitter, changes_schema,
    };
    use datafusion_expr::{col, lit};
    use futures::StreamExt;

    use super::PartitionScope;

    /// Rows `(op, id, region)` with primary key `id`.
    fn batch(rows: &[(&str, i64, &str)]) -> ChangeBatch {
        let data_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("region", DataType::Utf8, true),
        ]));
        let data = RecordBatch::try_new(
            Arc::clone(&data_schema),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("data");
        let mut keys = ListBuilder::new(StringBuilder::new()).with_field(Field::new(
            "item",
            DataType::Utf8,
            false,
        ));
        for _ in rows {
            keys.values().append_value("id");
            keys.append(true);
        }
        let record = RecordBatch::try_new(
            Arc::new(changes_schema(&data_schema)),
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(keys.finish()),
                Arc::new(StructArray::from(data)),
            ],
        )
        .expect("change record");
        ChangeBatch::try_new(record).expect("change batch")
    }

    fn envelope(rows: &[(&str, i64, &str)]) -> ChangeEnvelope {
        ChangeEnvelope::from_parts(Box::new(NoOpCommitter), batch(rows), false, false)
    }

    fn ops_and_ids(batch: &ChangeBatch) -> Vec<(String, i64)> {
        let data = batch.data_batch();
        let ids = data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ids");
        (0..batch.record.num_rows())
            .map(|row| (batch.op(row).to_string(), ids.value(row)))
            .collect()
    }

    #[test]
    fn rows_outside_the_served_partitions_are_not_applied() {
        let scope = PartitionScope::new(vec![col("region").eq(lit("east"))]);
        let scoped = scope
            .scope(batch(&[
                ("r", 1, "east"),
                ("r", 2, "west"),
                ("c", 3, "west"),
                ("u", 4, "east"),
                ("u", 5, "west"),
                ("d", 6, "west"),
            ]))
            .expect("scope");
        assert_eq!(
            ops_and_ids(&scoped),
            vec![
                ("r".to_string(), 1),
                ("u".to_string(), 4),
                ("d".to_string(), 5),
                ("d".to_string(), 6),
            ],
            "served rows apply, an update that left the served partitions deletes its key, a delete applies"
        );
    }

    #[test]
    fn an_update_outside_a_key_only_partition_is_dropped() {
        let scope = PartitionScope::new(vec![col("id").lt(lit(3_i64))]);
        let scoped = scope
            .scope(batch(&[("u", 1, "a"), ("u", 7, "b"), ("d", 8, "c")]))
            .expect("scope");
        assert_eq!(
            ops_and_ids(&scoped),
            vec![("u".to_string(), 1), ("d".to_string(), 8)],
            "a row partitioned by its key cannot move, so an update outside the scope has nothing to remove"
        );
    }

    /// Partition values are stored as text, so a filter compares an integer
    /// expression with a string literal, the way an executor receives it.
    #[test]
    fn a_partition_value_stored_as_text_is_compared_by_the_expression_type() {
        let scope = PartitionScope::new(vec![col("id").eq(lit("2"))]);
        let scoped = scope
            .scope(batch(&[("r", 1, "east"), ("r", 2, "east")]))
            .expect("the filter is coerced to the column's type");
        assert_eq!(ops_and_ids(&scoped), vec![("r".to_string(), 2)]);
    }

    /// The source stays open after the change, so the stream has to end on its
    /// own rather than on the source's next change.
    #[tokio::test]
    async fn a_change_that_cannot_be_scoped_ends_the_stream() {
        let source = futures::stream::iter(vec![Ok(envelope(&[("r", 1, "east")]))])
            .chain(futures::stream::pending())
            .boxed();
        let scope = PartitionScope::new(vec![col("not_a_column").eq(lit("east"))]);
        let mut scoped = super::scope_changes(source, scope);
        let Some(Err(error)) = scoped.next().await else {
            panic!("the change that could not be scoped must reach the consumer as an error");
        };
        assert!(
            error.to_string().contains("partition filters"),
            "unexpected error: {error}"
        );
        let next = tokio::time::timeout(Duration::from_secs(5), scoped.next())
            .await
            .expect("the stream must end after the change it could not scope");
        assert!(
            next.is_none(),
            "no change may follow one that could not be scoped"
        );
    }

    /// A reload for new filters does not wait for a change still being scoped,
    /// so that change must not reach the acceleration under the old filters.
    #[tokio::test]
    async fn a_change_scoped_while_the_filters_are_replaced_is_scoped_again() {
        let source =
            futures::stream::iter(vec![Ok(envelope(&[("r", 1, "east"), ("r", 2, "west")]))])
                .boxed();
        let scope = PartitionScope::new(vec![col("region").eq(lit("east"))]);
        let mut scoped = super::scope_changes(source, Arc::clone(&scope));
        {
            // The blocking thread waits for this lock, so the change is still
            // being scoped when this poll returns.
            let _held = scope.state.lock();
            let waker = futures::task::noop_waker();
            let mut cx = std::task::Context::from_waker(&waker);
            assert!(
                scoped.poll_next_unpin(&mut cx).is_pending(),
                "the change is scoped on a blocking thread"
            );
        }
        // The filters are compiled under the lock that also reads their
        // generation, so once they are compiled the blocking thread scopes the
        // change under the first filters.
        tokio::time::timeout(Duration::from_secs(5), async {
            while scope.state.lock().compiled.is_none() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the change is scoped under the first filters");
        scope.set(vec![col("region").eq(lit("west"))]);

        let Some(Ok(envelope)) = scoped.next().await else {
            panic!("the change must be delivered");
        };
        let (_, batch, _, _) = envelope.into_parts().expect("parts");
        assert_eq!(
            ops_and_ids(&batch),
            vec![("r".to_string(), 2)],
            "the change must be delivered scoped by the filters in place when it is delivered"
        );
    }

    #[test]
    fn no_filter_serves_no_partition() {
        let scope = PartitionScope::new(vec![]);
        let scoped = scope
            .scope(batch(&[("r", 1, "east"), ("d", 2, "east")]))
            .expect("scope");
        assert_eq!(ops_and_ids(&scoped), vec![("d".to_string(), 2)]);
    }

    #[test]
    fn a_new_scope_applies_to_the_next_batch() {
        let scope = PartitionScope::new(vec![col("region").eq(lit("east"))]);
        let scoped = scope
            .scope(batch(&[("r", 1, "east"), ("r", 2, "west")]))
            .expect("scope");
        assert_eq!(ops_and_ids(&scoped), vec![("r".to_string(), 1)]);
        scope.set(vec![col("region").eq(lit("west"))]);
        let scoped = scope
            .scope(batch(&[("r", 1, "east"), ("r", 2, "west")]))
            .expect("scope");
        assert_eq!(ops_and_ids(&scoped), vec![("r".to_string(), 2)]);
    }

    #[test]
    fn a_replacement_batch_keeps_only_served_rows_and_its_marker() {
        let scope = PartitionScope::new(vec![col("region").eq(lit("east"))]);
        let replacement = batch(&[("r", 1, "east"), ("r", 2, "west")])
            .with_rebuild_from_this_batch(true)
            .with_source_commit_ts_ms(Some(42));
        let scoped = scope.scope(replacement).expect("scope");
        assert_eq!(ops_and_ids(&scoped), vec![("r".to_string(), 1)]);
        assert!(scoped.rebuild_from_this_batch());
        assert_eq!(scoped.source_commit_ts_ms(), Some(42));
        assert!(matches!(scoped.op(0), ChangeOperation::Read));
    }
}
