/*
Copyright 2025 The Spice.ai OSS Authors

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

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use arrow::compute::take;
use arrow::datatypes::{FieldRef, Schema};
use arrow::error::ArrowError;
use data_components::cdc::{ChangeEnvelope, StreamError, replace_change_batch_data};
use spice_table::{Index, LayerWalk, SpiceTable};

/// A newtype wrapper around a vector of indexes to prevent cloning the vector for each item in a stream.
pub struct Indexes(Vec<Arc<dyn Index + Send + Sync>>);

impl Indexes {
    pub fn new(indexes: Vec<Arc<dyn Index + Send + Sync>>) -> Arc<Self> {
        Arc::new(Self(indexes))
    }
}

impl From<Arc<SpiceTable>> for Indexes {
    /// Collects the indexes carried anywhere in the table's stack, so a change
    /// stream maintains every one of them and not just the outermost layer's.
    ///
    /// Deduplicates by pointer identity: the walk reaches both sides of a router,
    /// and maintaining one index twice would apply every change to it twice.
    fn from(table: Arc<SpiceTable>) -> Self {
        let mut seen = std::collections::HashSet::new();
        Self(
            spice_table::nodes(table.as_ref(), LayerWalk::Index)
                .flat_map(SpiceTable::indexes)
                .filter(|index| seen.insert(Arc::as_ptr(index).cast::<()>()))
                .map(Arc::clone)
                .collect(),
        )
    }
}

pub async fn index_change_envelope(
    maybe_envelope: Result<ChangeEnvelope, StreamError>,
    indexes: Arc<Indexes>,
) -> Result<ChangeEnvelope, StreamError> {
    let envelope = maybe_envelope.map_err(|e| {
        tracing::debug!("Error in underlying base stream: {e:?}");
        e
    })?;

    // Materialize a deferred batch here (this index wrapper transforms the
    // stream before the accelerator consumes it). Offload the synchronous build
    // so a large deferred burst can't stall this async worker.
    let (change_committer, batch, is_dataset_ready, history_unavailable) =
        envelope.into_parts_offloaded().await?;

    // Project the change batch into the operations an index maintains (the upsert
    // rows, the delete rows, and a truncate flag), then maintain every index by
    // composing the primitives it already has, one per operation. `compute_index`
    // runs on the upserts only, so a delete or snapshot row is never embedded, and
    // the delete happens here — in the same task and turn as the upsert — so no
    // deleted row is left resurrected in the index.
    let change_set = batch
        .index_change_set()
        .map_err(|e| StreamError::External(e.to_string()))?;
    let data_batch = batch.data_batch();

    // Truncate first: clear each index before anything is (re)indexed, so a batch
    // that truncates and then inserts keeps the inserted rows.
    if change_set.truncated() {
        for index in &indexes.0 {
            index
                .truncate()
                .await
                .map_err(|e| StreamError::External(e.to_string()))?;
        }
    }

    // Upserts: (re)index the upsert rows through the index stack — each index
    // augments the batch in turn (the output of one is the input of the next).
    let upsert_rows = change_set.upsert_rows();
    let mut upserts: Vec<RecordBatch> = if upsert_rows.is_empty() {
        Vec::new()
    } else {
        vec![take_rows(&data_batch, upsert_rows).map_err(|e| StreamError::Arrow(e.to_string()))?]
    };
    for index in &indexes.0 {
        upserts = index
            .compute_index(upserts)
            .await
            .map_err(|e| StreamError::External(e.to_string()))?;
    }

    // Deletes: remove the deleted keys (the delete rows projected to the primary
    // key) from each index's own store.
    let delete_keys = if change_set.delete_rows().is_empty() {
        None
    } else {
        Some(
            delete_key_batch(
                &data_batch,
                change_set.delete_rows(),
                change_set.primary_key_columns(),
            )
            .map_err(|e| StreamError::Arrow(e.to_string()))?,
        )
    };
    if let Some(keys) = &delete_keys {
        for index in &indexes.0 {
            index
                .delete_by_keys(keys.clone())
                .await
                .map_err(|e| StreamError::External(e.to_string()))?;
        }
    }

    // Scatter the augmented upserts back into the full change batch so the
    // accelerator sees every row under its original `op` / `primary_keys`; delete
    // and truncate rows keep their original data and take null for any derived
    // column (they carry no indexed value).
    let augmented_data = scatter_upserts(&data_batch, &upserts, upsert_rows)
        .map_err(|e| StreamError::Arrow(e.to_string()))?;

    let new_change_batch = replace_change_batch_data(&augmented_data, &batch)
        .map_err(|e| StreamError::Arrow(e.to_string()))?;

    // `from_parts` rather than `new`: this wrapper transforms the batch and must
    // carry every envelope flag through untouched. A rebuild request dropped here
    // would let the accelerator keep applying changes after the source lost the
    // history that explains them.
    Ok(ChangeEnvelope::from_parts(
        change_committer,
        new_change_batch,
        is_dataset_ready,
        history_unavailable,
    ))
}

/// The delete keys for `Index::delete_by_keys`: the `rows` of `batch`, projected
/// to the primary-key columns `pk`.
fn delete_key_batch(
    batch: &RecordBatch,
    rows: &[usize],
    pk: &[String],
) -> Result<RecordBatch, ArrowError> {
    let schema = batch.schema();
    let mut fields = Vec::with_capacity(pk.len());
    let mut columns = Vec::with_capacity(pk.len());
    for name in pk {
        let (idx, field) = schema.column_with_name(name).ok_or_else(|| {
            ArrowError::SchemaError(format!(
                "primary key column '{name}' absent from change data"
            ))
        })?;
        fields.push(field.clone());
        columns.push(Arc::clone(batch.column(idx)));
    }
    let projected = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
    take_rows(&projected, rows)
}

/// The rows of `batch` at `rows`, in order, as a new `RecordBatch`.
fn take_rows(batch: &RecordBatch, rows: &[usize]) -> Result<RecordBatch, ArrowError> {
    let indices = rows
        .iter()
        .map(|&r| {
            u64::try_from(r).map_err(|e| ArrowError::CastError(format!("row index {r}: {e}")))
        })
        .collect::<Result<Vec<u64>, ArrowError>>()?;
    let indices = UInt64Array::from(indices);
    let columns = batch
        .columns()
        .iter()
        .map(|column| take(column, &indices, None))
        .collect::<Result<Vec<ArrayRef>, ArrowError>>()?;
    RecordBatch::try_new(batch.schema(), columns)
}

/// Reassemble the augmented upsert rows into the full change batch layout.
///
/// The indexes augment only the upsert rows (`compute_index` never sees deletes
/// or snapshot rows), so the result carries every original row: an upsert row
/// gets its augmented values, and every other row keeps its original data with
/// null for any column the indexes derived. The output aligns 1:1 with the
/// original `op` / `primary_keys` columns that `replace_change_batch_data` reuses.
fn scatter_upserts(
    original: &RecordBatch,
    augmented: &[RecordBatch],
    upsert_rows: &[usize],
) -> Result<RecordBatch, ArrowError> {
    // No upserts, or no index augmented anything: the accelerator gets the batch
    // unchanged.
    let augmented = match augmented {
        [] => return Ok(original.clone()),
        [single] => single,
        _ => {
            return Err(ArrowError::ComputeError(format!(
                "index augmentation must produce a single batch, got {}",
                augmented.len()
            )));
        }
    };
    if augmented.num_rows() != upsert_rows.len() {
        return Err(ArrowError::ComputeError(format!(
            "index augmentation changed the upsert row count: expected {}, got {}",
            upsert_rows.len(),
            augmented.num_rows()
        )));
    }

    // Inverse map: for each original row, `Some(i)` if it is the i-th upsert row,
    // else null. `take` then places the augmented value at the upsert rows and
    // null everywhere else.
    let mut inverse: Vec<Option<u64>> = vec![None; original.num_rows()];
    for (i, &row) in upsert_rows.iter().enumerate() {
        let i = u64::try_from(i)
            .map_err(|e| ArrowError::CastError(format!("upsert index {i}: {e}")))?;
        inverse[row] = Some(i);
    }
    let scatter_index = UInt64Array::from(inverse);

    let mut fields: Vec<FieldRef> = Vec::with_capacity(augmented.num_columns());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(augmented.num_columns());
    for (idx, field) in augmented.schema().fields().iter().enumerate() {
        if let Some(original_column) = original.column_by_name(field.name()) {
            // An original column: forward its full-length version untouched.
            fields.push(Arc::clone(field));
            columns.push(Arc::clone(original_column));
        } else {
            // A derived column: scatter the augmented values to their upsert rows,
            // null for every non-upsert row, so the field must read as nullable.
            columns.push(take(augmented.column(idx), &scatter_index, None)?);
            fields.push(Arc::new(field.as_ref().clone().with_nullable(true)));
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Int32Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use async_trait::async_trait;
    use data_components::cdc::{
        ChangeEnvelope, CommitChange, CommitError, wrap_data_as_change_batch,
    };
    use datafusion::catalog::TableProvider;
    use datafusion::error::{DataFusionError, Result as DataFusionResult};
    use spice_table::{Index, IndexLayer};
    use std::any::Any;
    use std::sync::Arc;

    struct MockCommitChange;

    #[async_trait]
    impl CommitChange for MockCommitChange {
        async fn commit(&self) -> Result<(), CommitError> {
            Ok(())
        }
    }

    #[derive(Debug)]
    struct MockIndex {
        name: &'static str,
        should_fail: bool,
        add_column: bool,
    }

    impl MockIndex {
        fn new(name: &'static str) -> Self {
            Self {
                name,
                should_fail: false,
                add_column: false,
            }
        }

        fn with_failure(mut self) -> Self {
            self.should_fail = true;
            self
        }

        fn with_added_column(mut self) -> Self {
            self.add_column = true;
            self
        }
    }

    #[async_trait]
    impl Index for MockIndex {
        fn name(&self) -> &'static str {
            self.name
        }

        fn required_columns(&self) -> Vec<String> {
            vec!["id".to_string()]
        }

        async fn compute_index(
            &self,
            mut batches: Vec<RecordBatch>,
        ) -> DataFusionResult<Vec<RecordBatch>> {
            if self.should_fail {
                return Err(DataFusionError::Execution("Mock index error".to_string()));
            }

            if self.add_column {
                for batch in &mut batches {
                    let embedding_array = Arc::new(StringArray::from(
                        (0..batch.num_rows())
                            .map(|i| format!("embedding_{i}"))
                            .collect::<Vec<_>>(),
                    ));

                    let mut columns = batch.columns().to_vec();
                    columns.push(embedding_array);

                    let mut fields = batch.schema().fields().to_vec();
                    fields.push(Arc::new(Field::new("embedding", DataType::Utf8, false)));

                    let new_schema = Arc::new(Schema::new(fields));
                    *batch = RecordBatch::try_new(new_schema, columns)?;
                }
            }

            Ok(batches)
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[derive(Debug)]
    struct MockTableProvider;

    #[async_trait]
    impl TableProvider for MockTableProvider {
        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, true),
            ]))
        }

        fn table_type(&self) -> datafusion::datasource::TableType {
            datafusion::datasource::TableType::Base
        }

        async fn scan(
            &self,
            _state: &dyn datafusion::catalog::Session,
            _projection: Option<&Vec<usize>>,
            _filters: &[datafusion::prelude::Expr],
            _limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
            unimplemented!("Not needed for tests")
        }
    }

    fn create_test_data_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let id_array = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let name_array = Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"]));

        RecordBatch::try_new(schema, vec![id_array, name_array])
            .expect("Failed to create test data batch")
    }

    fn create_test_change_envelope() -> ChangeEnvelope {
        let data_batch = create_test_data_batch();
        let change_batch = wrap_data_as_change_batch(&data_batch.schema(), &data_batch)
            .expect("Failed to create change batch");
        let committer = Box::new(MockCommitChange);
        ChangeEnvelope::new(committer, change_batch, true)
    }

    #[tokio::test]
    async fn test_index_change_envelope_success_no_indexes() {
        let envelope = create_test_change_envelope();
        let table_provider = Arc::new(MockTableProvider);
        let embedding_table = SpiceTable::over(Arc::new(IndexLayer::new()), table_provider);

        let result = index_change_envelope(Ok(envelope), Arc::new(embedding_table.into())).await;

        assert!(result.is_ok());
        let result_envelope = result.expect("Expected successful result");
        assert_eq!(
            result_envelope
                .change_batch()
                .expect("built change batch")
                .record
                .num_rows(),
            3
        );
        assert_eq!(
            result_envelope
                .change_batch()
                .expect("built change batch")
                .record
                .num_columns(),
            3
        ); // op, primary_keys, data
    }

    #[tokio::test]
    async fn test_index_change_envelope_success_with_single_index() {
        let envelope = create_test_change_envelope();
        let table_provider = Arc::new(MockTableProvider);
        let index = Arc::new(MockIndex::new("test_index").with_added_column());
        let embedding_table = SpiceTable::over(
            Arc::new(IndexLayer::with_indexes(vec![index])),
            table_provider,
        );

        let result = index_change_envelope(Ok(envelope), Arc::new(embedding_table.into())).await;

        assert!(result.is_ok());
        let result_envelope = result.expect("Expected successful result");
        assert_eq!(
            result_envelope
                .change_batch()
                .expect("built change batch")
                .record
                .num_rows(),
            3
        );

        let data_batch = result_envelope
            .change_batch()
            .expect("built change batch")
            .data_batch();
        assert_eq!(data_batch.num_columns(), 3); // id, name, embedding
        assert!(data_batch.schema().column_with_name("embedding").is_some());
    }

    #[tokio::test]
    async fn test_index_change_envelope_success_with_multiple_indexes() {
        let envelope = create_test_change_envelope();
        let table_provider = Arc::new(MockTableProvider);
        let index1 = Arc::new(MockIndex::new("index1"));
        let index2 = Arc::new(MockIndex::new("index2"));
        let embedding_table = SpiceTable::over(
            Arc::new(IndexLayer::with_indexes(vec![index1, index2])),
            table_provider,
        );

        let result = index_change_envelope(Ok(envelope), Arc::new(embedding_table.into())).await;

        assert!(result.is_ok());
        let result_envelope = result.expect("Expected successful result");
        assert_eq!(
            result_envelope
                .change_batch()
                .expect("built change batch")
                .record
                .num_rows(),
            3
        );
    }

    #[tokio::test]
    async fn test_index_change_envelope_input_stream_error() {
        let table_provider = Arc::new(MockTableProvider);
        let embedding_table = SpiceTable::over(Arc::new(IndexLayer::new()), table_provider);
        let input_error = StreamError::External("Input stream error".to_string());

        let result =
            index_change_envelope(Err(input_error), Arc::new(embedding_table.into())).await;

        assert!(result.is_err());
        if let Err(StreamError::External(msg)) = result {
            assert_eq!(msg, "Input stream error");
        } else {
            panic!("Expected External error");
        }
    }

    #[tokio::test]
    async fn test_index_change_envelope_index_computation_error() {
        let envelope = create_test_change_envelope();
        let table_provider = Arc::new(MockTableProvider);
        let failing_index = Arc::new(MockIndex::new("failing_index").with_failure());
        let embedding_table = SpiceTable::over(
            Arc::new(IndexLayer::with_indexes(vec![failing_index])),
            table_provider,
        );

        let result = index_change_envelope(Ok(envelope), Arc::new(embedding_table.into())).await;

        assert!(result.is_err());
        if let Err(StreamError::External(msg)) = result {
            assert!(msg.contains("Mock index error"));
        } else {
            panic!("Expected External error from index computation");
        }
    }

    #[tokio::test]
    async fn test_index_change_envelope_preserves_original_operations() {
        let data_batch = create_test_data_batch();
        let change_batch = wrap_data_as_change_batch(&data_batch.schema(), &data_batch)
            .expect("Failed to create change batch");
        let committer = Box::new(MockCommitChange);
        let envelope = ChangeEnvelope::new(committer, change_batch, true);

        let table_provider = Arc::new(MockTableProvider);
        let index = Arc::new(MockIndex::new("test_index"));
        let embedding_table = SpiceTable::over(
            Arc::new(IndexLayer::with_indexes(vec![index])),
            table_provider,
        );

        let result = index_change_envelope(Ok(envelope), Arc::new(embedding_table.into())).await;

        assert!(result.is_ok());
        let result_envelope = result.expect("Expected successful result");

        // Verify that all rows still have the "c" (create) operation
        for i in 0..result_envelope
            .change_batch()
            .expect("built change batch")
            .record
            .num_rows()
        {
            let op = result_envelope
                .change_batch()
                .expect("built change batch")
                .op(i);
            assert!(matches!(op, data_components::cdc::ChangeOperation::Create));
        }
    }

    #[tokio::test]
    async fn test_index_change_envelope_maintains_row_count() {
        let envelope = create_test_change_envelope();
        let original_row_count = envelope
            .change_batch()
            .expect("built change batch")
            .record
            .num_rows();

        let table_provider = Arc::new(MockTableProvider);
        let index = Arc::new(MockIndex::new("test_index").with_added_column());
        let embedding_table = SpiceTable::over(
            Arc::new(IndexLayer::with_indexes(vec![index])),
            table_provider,
        );

        let result = index_change_envelope(Ok(envelope), Arc::new(embedding_table.into())).await;

        assert!(result.is_ok());
        let result_envelope = result.expect("Expected successful result");
        assert_eq!(
            result_envelope
                .change_batch()
                .expect("built change batch")
                .record
                .num_rows(),
            original_row_count
        );
    }
}
