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

use std::sync::Arc;

use datafusion::sql::TableReference;
use datafusion_expr::ScalarUDF;
use llms::embeddings::Embed;
use runtime_acceleration::acceleration::ZeroResultsAction;
use search::index::{
    VectorIndex,
    compound::{CompoundReadMode, CompoundVectorIndex},
    memory::{MemoryDistanceMetric, MemoryVectorIndex},
};
use search::metadata::MetadataColumns;

/// Pair `engine_index` with an in-memory warm index in a writethrough
/// [`CompoundVectorIndex`]: writes go to both indexes, and searches are served from
/// memory. Whether a search that returns zero results from memory (e.g. before writes
/// have repopulated it after a restart) falls back to the vector engine is controlled
/// by `on_zero_results`, mirroring the dataset's `acceleration.on_zero_results` setting:
/// [`ZeroResultsAction::UseSource`] falls back to the engine index, while
/// [`ZeroResultsAction::ReturnEmpty`] serves the (possibly empty) in-memory result as-is.
///
/// `on_zero_results` is `None` when the table's acceleration cannot keep a warm index
/// complete — see [`crate::embeddings::warm_index_on_zero_results`] for the cases and why
/// each one disqualifies the tier. The in-memory index starts empty on every process start
/// and is only ever filled by the acceleration write path, so where it cannot be filled in
/// full, serving reads from it would narrow searches to whatever subset it happens to hold,
/// or to nothing at all. The engine index is then returned unchanged.
///
/// The warm index is an optimization: when a compatible in-memory index cannot be built
/// (e.g. the engine's distance metric has no in-memory equivalent), the engine index is
/// returned unchanged and a warning is logged — a dataset that works without a warm
/// index must never fail to load because of one.
///
/// `metadata_columns` must only contain columns the engine index exposes in both its
/// list and query plans; otherwise the fallback projection cannot be built at read time.
/// When the index is chunked, `engine_index`'s primary key must already be augmented
/// with the chunk key.
#[must_use]
#[expect(clippy::too_many_arguments)]
pub fn with_memory_warm_index(
    tbl: &TableReference,
    engine_index: Arc<dyn VectorIndex>,
    metadata_columns: MetadataColumns,
    embedder: Arc<dyn Embed>,
    embed_udf: &Arc<ScalarUDF>,
    model_name: &String,
    metric: &str,
    on_zero_results: Option<&ZeroResultsAction>,
) -> Arc<dyn VectorIndex> {
    let read_mode = match on_zero_results {
        Some(ZeroResultsAction::ReturnEmpty) => CompoundReadMode::PrimaryOnly,
        Some(ZeroResultsAction::UseSource) => CompoundReadMode::FallbackToSecondary,
        // `None` covers every reason the acceleration cannot keep a warm tier complete —
        // no enabled acceleration, or an accelerator that keeps its rows across a restart —
        // so this cannot name one. `warm_index_on_zero_results` logs which it was.
        None => {
            tracing::debug!(
                "Not adding an in-memory warm vector index for table {tbl}: the acceleration configuration cannot keep it complete. Searches will be served by the vector engine directly."
            );
            return engine_index;
        }
    };

    let memory_metric = match MemoryDistanceMetric::try_from(metric) {
        Ok(memory_metric) => memory_metric,
        Err(err) => {
            tracing::warn!(
                "Not adding an in-memory warm vector index for table {tbl}: {err} Searches will be served by the vector engine directly."
            );
            return engine_index;
        }
    };

    let memory_index = match MemoryVectorIndex::try_new(
        engine_index.search_column(),
        engine_index.primary_fields(),
        metadata_columns,
        embedder,
        Arc::clone(embed_udf),
        model_name.to_owned(),
        memory_metric,
    ) {
        Ok(memory_index) => Arc::new(memory_index) as Arc<dyn VectorIndex>,
        Err(err) => {
            tracing::warn!(
                "Not adding an in-memory warm vector index for table {tbl}: {err} Searches will be served by the vector engine directly."
            );
            return engine_index;
        }
    };

    match CompoundVectorIndex::try_new(memory_index, Arc::clone(&engine_index), read_mode) {
        Ok(compound) => {
            tracing::debug!(
                "Added an in-memory warm vector index for table {tbl} column {}",
                engine_index.search_column()
            );
            Arc::new(compound) as Arc<dyn VectorIndex>
        }
        Err(err) => {
            tracing::warn!(
                "Not adding an in-memory warm vector index for table {tbl}: {err} Searches will be served by the vector engine directly."
            );
            engine_index
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::any::Any;

    use arrow::array::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;
    use datafusion::error::DataFusionError;
    use datafusion::logical_expr::{Volatility, create_udf};
    use datafusion_expr::LogicalPlan;
    use llms::embeddings::EmbeddingInput;
    use runtime_datafusion_index::Index;
    use search::index::SearchIndex;

    fn noop_embed_udf() -> Arc<ScalarUDF> {
        Arc::new(create_udf(
            "embed",
            vec![],
            DataType::Null,
            Volatility::Volatile,
            Arc::new(|_| unimplemented!("not exercised by with_memory_warm_index tests")),
        ))
    }

    #[derive(Debug)]
    struct NoopEmbed;

    #[async_trait]
    impl Embed for NoopEmbed {
        async fn embed(&self, _input: EmbeddingInput) -> llms::embeddings::Result<Vec<Vec<f32>>> {
            Ok(vec![])
        }

        fn size(&self) -> i32 {
            3
        }
    }

    /// A [`VectorIndex`] test double whose embedded column carries `dimension`-sized
    /// vectors (`dimension: 0` produces a plain string column, i.e. an invalid vector
    /// dimension). None of its query/list plans are exercised by `with_memory_warm_index`.
    #[derive(Debug)]
    struct PretendVectorIndex {
        schema: Schema,
    }

    impl PretendVectorIndex {
        fn new(dimension: i32) -> Self {
            let content_type = if dimension > 0 {
                DataType::FixedSizeList(
                    Arc::new(Field::new_list_field(DataType::Float32, false)),
                    dimension,
                )
            } else {
                DataType::Utf8
            };
            Self {
                schema: Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("content", content_type, true),
                ]),
            }
        }
    }

    #[async_trait]
    impl Index for PretendVectorIndex {
        fn name(&self) -> &'static str {
            "PretendVectorIndex"
        }

        fn required_columns(&self) -> Vec<String> {
            vec!["id".to_string(), "content".to_string()]
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[async_trait]
    impl SearchIndex for PretendVectorIndex {
        fn search_column(&self) -> String {
            "content".to_string()
        }

        fn primary_fields(&self) -> Vec<Field> {
            vec![Field::new("id", DataType::Int64, false)]
        }

        async fn write(
            &self,
            record: RecordBatch,
        ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
            Ok(record)
        }

        fn query_table_provider(&self, _query: &str) -> Result<Arc<LogicalPlan>, DataFusionError> {
            Err(DataFusionError::NotImplemented(
                "not exercised by with_memory_warm_index tests".to_string(),
            ))
        }

        fn as_vector_index(self: Arc<Self>) -> Option<Arc<dyn VectorIndex>> {
            Some(self as Arc<dyn VectorIndex>)
        }
    }

    impl VectorIndex for PretendVectorIndex {
        fn list_table_provider(&self) -> Result<LogicalPlan, DataFusionError> {
            Err(DataFusionError::NotImplemented(
                "not exercised by with_memory_warm_index tests".to_string(),
            ))
        }

        fn dimension(&self) -> i32 {
            match self.schema.column_with_name("content") {
                Some((_, f)) => match f.data_type() {
                    DataType::FixedSizeList(_, dim) => *dim,
                    _ => 0,
                },
                None => 0,
            }
        }
    }

    fn pretend_index(dimension: i32) -> Arc<dyn VectorIndex> {
        Arc::new(PretendVectorIndex::new(dimension))
    }

    #[test]
    fn warm_index_wraps_the_engine_index_in_a_compound() {
        let index = with_memory_warm_index(
            &TableReference::bare("tbl"),
            pretend_index(3),
            MetadataColumns::none(),
            Arc::new(NoopEmbed),
            &noop_embed_udf(),
            &"model".to_string(),
            "cosine",
            Some(&ZeroResultsAction::UseSource),
        );
        assert!(
            index
                .as_any()
                .downcast_ref::<CompoundVectorIndex>()
                .is_some(),
            "the engine index should be wrapped in a CompoundVectorIndex"
        );
    }

    /// Regression test for #12101: nothing writes to a warm index for a table without
    /// acceleration, so wrapping the engine index in a compound would serve searches from an
    /// index that is empty (or holds only whatever rows a scan happened to write since
    /// startup) instead of from the vector engine.
    #[test]
    fn warm_index_is_skipped_without_acceleration() {
        let index = with_memory_warm_index(
            &TableReference::bare("tbl"),
            pretend_index(3),
            MetadataColumns::none(),
            Arc::new(NoopEmbed),
            &noop_embed_udf(),
            &"model".to_string(),
            "cosine",
            None,
        );
        assert!(
            index
                .as_any()
                .downcast_ref::<PretendVectorIndex>()
                .is_some(),
            "a table without acceleration must keep the engine index unchanged"
        );
    }

    #[test]
    fn warm_index_read_mode_follows_on_zero_results() {
        let use_source = with_memory_warm_index(
            &TableReference::bare("tbl"),
            pretend_index(3),
            MetadataColumns::none(),
            Arc::new(NoopEmbed),
            &noop_embed_udf(),
            &"model".to_string(),
            "cosine",
            Some(&ZeroResultsAction::UseSource),
        );
        assert_eq!(
            use_source
                .as_any()
                .downcast_ref::<CompoundVectorIndex>()
                .expect("wrapped in a CompoundVectorIndex")
                .read_mode(),
            CompoundReadMode::FallbackToSecondary,
            "on_zero_results: use_source must fall back to the engine index on zero results"
        );

        let return_empty = with_memory_warm_index(
            &TableReference::bare("tbl"),
            pretend_index(3),
            MetadataColumns::none(),
            Arc::new(NoopEmbed),
            &noop_embed_udf(),
            &"model".to_string(),
            "cosine",
            Some(&ZeroResultsAction::ReturnEmpty),
        );
        assert_eq!(
            return_empty
                .as_any()
                .downcast_ref::<CompoundVectorIndex>()
                .expect("wrapped in a CompoundVectorIndex")
                .read_mode(),
            CompoundReadMode::PrimaryOnly,
            "on_zero_results: return_empty must not fall back to the engine index"
        );
    }

    #[test]
    fn warm_index_is_skipped_for_an_unknown_metric() {
        let index = with_memory_warm_index(
            &TableReference::bare("tbl"),
            pretend_index(3),
            MetadataColumns::none(),
            Arc::new(NoopEmbed),
            &noop_embed_udf(),
            &"model".to_string(),
            "hyperbolic",
            Some(&ZeroResultsAction::UseSource),
        );
        assert!(
            index
                .as_any()
                .downcast_ref::<PretendVectorIndex>()
                .is_some(),
            "an unknown metric must return the engine index unchanged"
        );
    }

    #[test]
    fn warm_index_is_skipped_when_the_memory_index_cannot_be_built() {
        // A non-vector embedded column reports dimension 0, which the memory index rejects.
        let index = with_memory_warm_index(
            &TableReference::bare("tbl"),
            pretend_index(0),
            MetadataColumns::none(),
            Arc::new(NoopEmbed),
            &noop_embed_udf(),
            &"model".to_string(),
            "cosine",
            Some(&ZeroResultsAction::UseSource),
        );
        assert!(
            index
                .as_any()
                .downcast_ref::<PretendVectorIndex>()
                .is_some(),
            "a memory index construction failure must return the engine index unchanged"
        );
    }
}
