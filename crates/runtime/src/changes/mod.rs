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

use data_components::cdc::{ChangeEnvelope, StreamError, replace_change_batch_data};
use runtime_datafusion_index::{Index, IndexedTableProvider};

/// A newtype wrapper around a vector of indexes to prevent cloning the vector for each item in a stream.
pub struct Indexes(Vec<Arc<dyn Index + Send + Sync>>);

impl Indexes {
    pub fn new(indexes: Vec<Arc<dyn Index + Send + Sync>>) -> Arc<Self> {
        Arc::new(Self(indexes))
    }
}

impl From<Arc<IndexedTableProvider>> for Indexes {
    fn from(indexed_table: Arc<IndexedTableProvider>) -> Self {
        Self(indexed_table.get_all_indexes())
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
    let (change_committer, batch, is_dataset_ready) = envelope.into_parts_offloaded().await?;
    // The data batch carries row data alone. The mask is what tells an index which of those
    // rows the source deleted, so an index holding its own copy of a row removes it instead
    // of re-indexing it as an upsert.
    let deleted = batch.deletion_mask();
    let mut data_batch = batch.data_batch();

    for index in &indexes.0 {
        data_batch = index
            .compute_index_for_changes(data_batch, &deleted)
            .await
            .map_err(|e| StreamError::External(e.to_string()))?;
    }

    let new_change_batch = replace_change_batch_data(&data_batch, &batch)
        .map_err(|e| StreamError::Arrow(e.to_string()))?;

    Ok(ChangeEnvelope::new(
        change_committer,
        new_change_batch,
        is_dataset_ready,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{BooleanArray, Int32Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use async_trait::async_trait;
    use data_components::cdc::{
        ChangeBatch, ChangeEnvelope, CommitChange, CommitError, wrap_data_as_change_batch,
    };
    use datafusion::catalog::TableProvider;
    use datafusion::error::{DataFusionError, Result as DataFusionResult};
    use runtime_datafusion_index::{Index, IndexedTableProvider};
    use std::any::Any;
    use std::sync::{Arc, Mutex};

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
        /// The deletion mask of every change batch this index was given, in call order.
        observed_masks: Arc<Mutex<Vec<Vec<Option<bool>>>>>,
    }

    impl MockIndex {
        fn new(name: &'static str) -> Self {
            Self {
                name,
                should_fail: false,
                add_column: false,
                observed_masks: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn observed_masks(&self) -> Vec<Vec<Option<bool>>> {
            self.observed_masks
                .lock()
                .expect("the observed-mask log is not poisoned")
                .clone()
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

        async fn compute_index_for_changes(
            &self,
            batch: RecordBatch,
            deleted: &BooleanArray,
        ) -> DataFusionResult<RecordBatch> {
            self.observed_masks
                .lock()
                .expect("the observed-mask log is not poisoned")
                .push(deleted.iter().collect());

            let batches = self.compute_index(vec![batch]).await?;
            batches.into_iter().next().ok_or_else(|| {
                DataFusionError::Execution("Mock index returned no batches".to_string())
            })
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

    /// A change envelope over [`create_test_data_batch`] whose rows carry `ops`, one per row.
    fn change_envelope_with_ops(ops: &[&str]) -> ChangeEnvelope {
        let data_batch = create_test_data_batch();
        let record = wrap_data_as_change_batch(&data_batch.schema(), &data_batch)
            .expect("Failed to create change batch")
            .record;
        let op_idx = record
            .schema()
            .index_of("op")
            .expect("the change schema has an 'op' column");
        let mut columns = record.columns().to_vec();
        columns[op_idx] = Arc::new(StringArray::from(ops.to_vec()));
        let record = RecordBatch::try_new(record.schema(), columns)
            .expect("Failed to rebuild the change batch with the given operations");

        ChangeEnvelope::new(
            Box::new(MockCommitChange),
            ChangeBatch::try_new(record).expect("Failed to create change batch"),
            true,
        )
    }

    /// Regression test for #12084: the change stream must tell each index which rows the source
    /// deleted. Without the mask an index that keeps its own copy of a row (a full-text index)
    /// re-indexes a deleted row as an upsert and keeps matching it after the row is gone.
    #[tokio::test]
    async fn test_index_change_envelope_forwards_the_deletion_mask() {
        let index = Arc::new(MockIndex::new("mock_index"));
        let indexes = Indexes::new(vec![Arc::clone(&index) as Arc<dyn Index + Send + Sync>]);

        let result = index_change_envelope(Ok(change_envelope_with_ops(&["c", "d", "u"])), indexes)
            .await
            .expect("Expected successful result");

        assert_eq!(
            index.observed_masks(),
            vec![vec![Some(false), Some(true), Some(false)]],
            "the index must receive one mask aligned with the change batch's rows"
        );
        // The rows still ride the envelope the accelerator applies.
        assert_eq!(
            result
                .change_batch()
                .expect("built change batch")
                .record
                .num_rows(),
            3
        );
    }

    #[tokio::test]
    async fn test_index_change_envelope_success_no_indexes() {
        let envelope = create_test_change_envelope();
        let table_provider = Arc::new(MockTableProvider);
        let embedding_table = Arc::new(IndexedTableProvider::new(table_provider));

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
        let embedding_table = Arc::new(IndexedTableProvider::with_indexes(
            table_provider,
            vec![index],
        ));

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
        let embedding_table = Arc::new(IndexedTableProvider::with_indexes(
            table_provider,
            vec![index1, index2],
        ));

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
        let embedding_table = Arc::new(IndexedTableProvider::new(table_provider));
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
        let embedding_table = Arc::new(IndexedTableProvider::with_indexes(
            table_provider,
            vec![failing_index],
        ));

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
        let embedding_table = Arc::new(IndexedTableProvider::with_indexes(
            table_provider,
            vec![index],
        ));

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
        let embedding_table = Arc::new(IndexedTableProvider::with_indexes(
            table_provider,
            vec![index],
        ));

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
