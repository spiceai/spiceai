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

//! End-to-end integration test for issue #7507: bounded intermediate batch size in
//! [`ChunkedSearchIndex`]. The test wires up an `IndexedTableProvider` whose only index is a
//! `ChunkedSearchIndex` (over a row-counting inner index), runs a `SELECT *` through the
//! `IndexTableScanOptimizerRule` + `IndexTableScanExtensionPlanner` pipeline, and asserts that
//! the inner index's `write` method is called with strictly-bounded batches even when the input
//! row count would otherwise produce a multi-million-row intermediate.

use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{
    ArrayRef, FixedSizeListArray, Float32Array, Int64Array, ListArray, RecordBatch, StringArray,
};
use arrow::buffer::OffsetBuffer;
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use chunking::Chunker;
use datafusion::{
    catalog::{MemTable, TableProvider},
    error::DataFusionError,
    execution::{SessionStateBuilder, context::QueryPlanner},
    logical_expr::LogicalPlan,
    physical_plan::ExecutionPlan,
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
    prelude::SessionContext,
};
use runtime_datafusion_index::{
    Index, IndexedTableProvider,
    analyzer::{IndexTableScanExtensionPlanner, IndexTableScanOptimizerRule},
};
use search::index::{
    SearchIndex,
    chunking::{CHUNKED_INDEX_CHUNK_KEY, ChunkedSearchIndex},
};

/// A chunker that always produces exactly `n` evenly-spaced chunks per non-empty input row,
/// regardless of content. Deterministic chunk counts make the assertions in this test crisp.
struct FixedCountChunker {
    n: usize,
}

impl Chunker for FixedCountChunker {
    fn chunk_indices<'a>(&self, text: &'a str) -> Box<dyn Iterator<Item = (usize, &'a str)> + 'a> {
        if text.is_empty() || self.n == 0 {
            return Box::new(std::iter::empty());
        }
        let chars: Vec<(usize, char)> = text.char_indices().collect();
        let stride = (chars.len() / self.n).max(1);
        let n = self.n;
        Box::new((0..n).map(move |i| {
            let start_idx = i * stride;
            let end_idx = if i + 1 == n {
                chars.len()
            } else {
                (i + 1) * stride
            };
            let start_byte = chars.get(start_idx).map(|(b, _)| *b).unwrap_or(text.len());
            let end_byte = chars.get(end_idx).map(|(b, _)| *b).unwrap_or(text.len());
            (start_byte, &text[start_byte..end_byte])
        }))
    }
}

/// Records every `write` call so the test can assert on per-call batch sizes. Returns a synthetic
/// embedding column whose values encode the row index within the call, plus the input's offset
/// column (so `ChunkedSearchIndex` has both list payloads to fold back into the final output).
#[derive(Debug)]
struct RecordingInner {
    search_column: String,
    write_calls: AtomicUsize,
    write_row_counts: std::sync::Mutex<Vec<usize>>,
}

impl RecordingInner {
    fn new(search_column: &str) -> Self {
        Self {
            search_column: search_column.to_string(),
            write_calls: AtomicUsize::new(0),
            write_row_counts: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn row_counts(&self) -> Vec<usize> {
        self.write_row_counts.lock().expect("mutex").clone()
    }
}

#[async_trait]
impl Index for RecordingInner {
    fn name(&self) -> &'static str {
        "RecordingInner"
    }
    fn required_columns(&self) -> Vec<String> {
        vec![self.search_column.clone()]
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[async_trait]
impl SearchIndex for RecordingInner {
    fn search_column(&self) -> String {
        self.search_column.clone()
    }

    fn primary_fields(&self) -> Vec<Field> {
        vec![Field::new("id", DataType::Int64, false)]
    }

    async fn write(
        &self,
        record: RecordBatch,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        self.write_calls.fetch_add(1, Ordering::SeqCst);
        self.write_row_counts
            .lock()
            .expect("mutex")
            .push(record.num_rows());

        let n = record.num_rows();
        let mut emb_values: Vec<f32> = Vec::with_capacity(n * 4);
        for i in 0..n {
            emb_values.extend_from_slice(&[i as f32, 0.0, 0.0, 0.0]);
        }
        let emb_item = Arc::new(Field::new("item", DataType::Float32, true));
        let emb = FixedSizeListArray::try_new(
            Arc::clone(&emb_item),
            4,
            Arc::new(Float32Array::from(emb_values)),
            None,
        )?;

        let offset_col_name = ChunkedSearchIndex::chunking_offset_col(&self.search_column);
        let offset_arr = Arc::clone(
            record
                .column_by_name(&offset_col_name)
                .expect("chunked input must carry the offset column"),
        );

        let mut fields: Vec<Field> = record
            .schema()
            .fields()
            .iter()
            .map(|f| Arc::unwrap_or_clone(Arc::clone(f)))
            .collect();
        let mut cols: Vec<ArrayRef> = record.columns().iter().map(Arc::clone).collect();

        fields.push(Field::new(
            format!("{}_embedding", self.search_column),
            DataType::FixedSizeList(Arc::clone(&emb_item), 4),
            true,
        ));
        cols.push(Arc::new(emb) as ArrayRef);

        // Ensure the offset column survives (the chunker passes it through unchanged).
        let _ = offset_arr;

        Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)?)
    }

    fn query_table_provider(&self, _query: &str) -> Result<Arc<LogicalPlan>, DataFusionError> {
        Err(DataFusionError::NotImplemented("unused in tests".into()))
    }
}

#[derive(Debug, Default)]
struct PlannerWithIndexExtension;

#[async_trait]
impl QueryPlanner for PlannerWithIndexExtension {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &datafusion::execution::SessionState,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let physical_planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            IndexTableScanExtensionPlanner::new(),
        )]);
        physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

fn session_with_index_planner() -> SessionContext {
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_query_planner(Arc::new(PlannerWithIndexExtension))
        .with_optimizer_rule(Arc::new(IndexTableScanOptimizerRule::new()))
        .build();
    SessionContext::new_with_state(state)
}

/// Builds the schema the underlying table is expected to expose: the base columns *plus* the
/// list-typed `<col>_offset` and `<col>_embedding` columns that `ChunkedSearchIndex` reads and
/// rewrites on each pass. In production these are persisted alongside the data by the
/// accelerator; here we present them as empty per-row lists on the input so the indexer's
/// schema-stability check passes.
fn schema_with_search_col() -> Arc<Schema> {
    let offset_item = Arc::new(Field::new(
        "item",
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, false)), 2),
        true,
    ));
    let embed_item = Arc::new(Field::new(
        "item",
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
        true,
    ));
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("content", DataType::Utf8, true),
        Field::new_list("content_offset", Arc::unwrap_or_clone(offset_item), false),
        Field::new_list("content_embedding", Arc::unwrap_or_clone(embed_item), false),
    ]))
}

fn empty_list_column(item_field: Field, num_rows: usize) -> ArrayRef {
    let item_field = Arc::new(item_field);
    let empty_values: ArrayRef = match item_field.data_type() {
        DataType::FixedSizeList(_, size) if *size == 2 => Arc::new(
            FixedSizeListArray::try_new(
                Arc::new(Field::new("item", DataType::Int32, false)),
                2,
                Arc::new(arrow::array::Int32Array::from(Vec::<i32>::new())),
                None,
            )
            .expect("offset values"),
        ),
        DataType::FixedSizeList(_, size) if *size == 4 => Arc::new(
            FixedSizeListArray::try_new(
                Arc::new(Field::new("item", DataType::Float32, true)),
                4,
                Arc::new(Float32Array::from(Vec::<f32>::new())),
                None,
            )
            .expect("embedding values"),
        ),
        other => panic!("unexpected item type {other:?}"),
    };
    Arc::new(
        ListArray::try_new(
            item_field,
            OffsetBuffer::from_lengths(std::iter::repeat_n(0_usize, num_rows)),
            empty_values,
            None,
        )
        .expect("list array"),
    )
}

fn build_input_batch(num_rows: usize, content_size_bytes: usize) -> RecordBatch {
    let schema = schema_with_search_col();
    let ids: Vec<i64> = (0..num_rows as i64).collect();
    let content: Vec<String> = (0..num_rows)
        .map(|i| format!("doc{i:08}-{}", "x".repeat(content_size_bytes)))
        .collect();
    let content_refs: Vec<&str> = content.iter().map(String::as_str).collect();

    let offset_item = Field::new(
        "item",
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, false)), 2),
        true,
    );
    let embed_item = Field::new(
        "item",
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
        true,
    );

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(content_refs)),
            empty_list_column(offset_item, num_rows),
            empty_list_column(embed_item, num_rows),
        ],
    )
    .expect("valid batch")
}

/// Verifies that when a large input batch flows through `IndexerExec` → `ChunkedSearchIndex` →
/// inner index, no single intermediate batch handed to `inner.write` exceeds the configured
/// budget. The OOM regression in issue #7507 manifested as the inner index receiving a single
/// ~1.6M-row intermediate batch; this test pins the upper bound.
#[tokio::test]
async fn chunked_index_emits_bounded_intermediate_batches_to_inner() {
    // 4096 rows × 100 chunks/row = ~409,600 chunks. Before the fix this would be presented to
    // `inner.write` as one record batch.
    const NUM_ROWS: usize = 4096;
    const CHUNKS_PER_ROW: usize = 100;
    // Mirrors the constant in chunking.rs; if that ever changes the test must follow.
    const BUDGET: usize = 8192;

    let ctx = session_with_index_planner();
    let inner = Arc::new(RecordingInner::new("content"));
    let chunker = Arc::new(FixedCountChunker { n: CHUNKS_PER_ROW });
    let chunked_index = Arc::new(ChunkedSearchIndex::new(
        Arc::clone(&inner) as Arc<dyn SearchIndex>,
        chunker as Arc<dyn Chunker>,
    ));

    let input = build_input_batch(NUM_ROWS, 32);
    let schema = input.schema();
    let mem_table = Arc::new(MemTable::try_new(schema, vec![vec![input]]).expect("valid table"));

    let indexed = Arc::new(
        IndexedTableProvider::new(mem_table as Arc<dyn TableProvider>)
            .add_index(chunked_index as Arc<dyn Index + Send + Sync>),
    );

    ctx.register_table("docs", indexed as Arc<dyn TableProvider>)
        .expect("register");

    let df = ctx.table("docs").await.expect("table");
    let results: Vec<RecordBatch> = df.collect().await.expect("collect");

    // Output cardinality is preserved by ChunkedSearchIndex (one row per input doc).
    let total_out_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_out_rows, NUM_ROWS);

    // The fix's core assertion: every inner.write must have stayed under the budget.
    let call_sizes = inner.row_counts();
    assert!(
        !call_sizes.is_empty(),
        "inner.write should have been invoked"
    );
    let max = *call_sizes.iter().max().expect("non-empty");
    assert!(
        max <= BUDGET,
        "max inner.write batch size {max} exceeded budget {BUDGET}; call sizes: {call_sizes:?}"
    );

    // Total chunk count routed through the inner index must equal NUM_ROWS * CHUNKS_PER_ROW;
    // nothing was dropped on the seams between groups.
    let total_inner_chunks: usize = call_sizes.iter().sum();
    assert_eq!(total_inner_chunks, NUM_ROWS * CHUNKS_PER_ROW);

    // And multiple groups must have been needed (otherwise this test isn't exercising the fix).
    assert!(
        call_sizes.len() >= 2,
        "expected >=2 inner.write calls for {NUM_ROWS} rows × {CHUNKS_PER_ROW} chunks; got {}",
        call_sizes.len()
    );

    // Sanity-check the output: every batch has the original schema columns plus the chunked
    // index's list-typed offset and embedding columns. Just confirm one column is present so
    // the schema-mismatch failure mode would surface here.
    let first = results.first().expect("at least one output batch");
    assert!(
        first
            .schema()
            .column_with_name("content_embedding")
            .is_some(),
        "output must include content_embedding"
    );
    // CHUNKED_INDEX_CHUNK_KEY is only used inside the intermediate chunked batch; downstream
    // sees per-row list columns, not the chunk-keyed expansion. Confirm that intent.
    assert!(
        first
            .schema()
            .column_with_name(CHUNKED_INDEX_CHUNK_KEY)
            .is_none(),
        "{CHUNKED_INDEX_CHUNK_KEY} must not leak out of the indexer"
    );
}
