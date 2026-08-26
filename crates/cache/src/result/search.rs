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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::sql::TableReference;

use crate::sizing::{
    ARC_HEADER_BYTES, ENTRY_OVERHEAD_BYTES, arc_heap_size, schema_size, string_vec_heap_size,
    table_reference_heap_size, table_refs_size,
};
use crate::{AsTableRefs, Sizeable};

#[derive(Clone)]
pub struct CachedAggregationResult {
    pub records: Arc<Vec<RecordBatch>>,
    pub primary_keys: Vec<String>,
    pub data_columns: Vec<String>,
    pub matches: HashMap<String, Vec<String>>,
    pub schema: SchemaRef,
}

impl CachedAggregationResult {
    /// Batches are compacted on the way in, for the same reason
    /// [`crate::CachedQueryResult`] compacts its own: a top-k search plan emits
    /// zero-copy slices, and storing one as it arrives would pin — and, through
    /// [`Sizeable`] below, bill — the whole scan batch it was carved from.
    #[must_use]
    pub fn new(
        records: Vec<RecordBatch>,
        primary_keys: Vec<String>,
        data_columns: Vec<String>,
        matches: HashMap<String, Vec<String>>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            records: Arc::new(crate::result::compact_for_storage(records)),
            primary_keys,
            data_columns,
            matches,
            schema,
        }
    }

    /// The memory one table's aggregated results hold, excluding the struct
    /// itself — the caller charges that through the map slot holding it.
    fn heap_size(&self) -> usize {
        arc_heap_size::<Vec<RecordBatch>>()
            + self.records.len() * std::mem::size_of::<RecordBatch>()
            + self
                .records
                .iter()
                .map(RecordBatch::get_array_memory_size)
                .sum::<usize>()
            + string_vec_heap_size(&self.primary_keys)
            + string_vec_heap_size(&self.data_columns)
            + self.matches.capacity() * std::mem::size_of::<(String, Vec<String>)>()
            + self
                .matches
                .iter()
                .map(|(key, values)| key.capacity() + string_vec_heap_size(values))
                .sum::<usize>()
            + ARC_HEADER_BYTES
            + schema_size(&self.schema)
    }
}

#[derive(Clone)]
pub struct CachedSearchResult {
    pub results: Arc<HashMap<TableReference, CachedAggregationResult>>,
    pub input_tables: Arc<HashSet<TableReference>>,
}

impl AsTableRefs for CachedSearchResult {
    fn as_table_refs(&self) -> Arc<HashSet<TableReference>> {
        Arc::clone(&self.input_tables)
    }
}

impl Sizeable for CachedSearchResult {
    fn get_memory_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + arc_heap_size::<HashMap<TableReference, CachedAggregationResult>>()
            + self.results.capacity()
                * std::mem::size_of::<(TableReference, CachedAggregationResult)>()
            + self
                .results
                .iter()
                .map(|(table, result)| table_reference_heap_size(table) + result.heap_size())
                .sum::<usize>()
            + ARC_HEADER_BYTES
            + table_refs_size(&self.input_tables)
            + ENTRY_OVERHEAD_BYTES
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};

    /// Regression test for <https://github.com/spiceai/spiceai/issues/12921>.
    /// A top-k search plan emits slices, and an entry built from one must not
    /// hold — or be billed — the scan batch it was carved out of.
    #[test]
    fn a_sliced_search_result_is_billed_its_own_rows() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Utf8,
            false,
        )]));
        let payloads: Vec<String> = (0..2_000)
            .map(|row| {
                std::iter::repeat_n(
                    char::from(b'a' + u8::try_from(row % 26).unwrap_or_default()),
                    4_096,
                )
                .collect()
            })
            .collect();
        let scan_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(payloads))],
        )
        .expect("should create batch");
        let sliced = scan_batch.slice(1_000, 1);
        let expected = sliced
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("payload is a StringArray")
            .value(0)
            .to_string();

        let result = CachedAggregationResult::new(
            vec![sliced],
            vec!["id".to_string()],
            vec!["payload".to_string()],
            HashMap::new(),
            schema,
        );

        let cached = CachedSearchResult {
            results: Arc::new(HashMap::from([(
                TableReference::bare("docs"),
                result.clone(),
            )])),
            input_tables: Arc::new(HashSet::new()),
        };
        assert!(
            cached.get_memory_size() * 100 < scan_batch.get_array_memory_size(),
            "a one-row search entry should be billed a small fraction of its parent, got {} of {}",
            cached.get_memory_size(),
            scan_batch.get_array_memory_size()
        );

        assert_eq!(result.records.len(), 1);
        assert_eq!(
            result.records[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("payload is a StringArray")
                .value(0),
            expected,
            "compacting the entry must not change the row it holds"
        );
    }

    fn empty_result_over(schema: SchemaRef, primary_keys: Vec<String>) -> CachedSearchResult {
        CachedSearchResult {
            results: Arc::new(HashMap::from([(
                TableReference::bare("docs"),
                CachedAggregationResult::new(
                    Vec::new(),
                    primary_keys,
                    Vec::new(),
                    HashMap::new(),
                    schema,
                ),
            )])),
            input_tables: Arc::new(HashSet::from([TableReference::bare("docs")])),
        }
    }

    fn schema_of_width(columns: usize) -> SchemaRef {
        Arc::new(Schema::new(
            (0..columns)
                .map(|i| Field::new(format!("column_{i}"), DataType::Utf8, true))
                .collect::<Vec<_>>(),
        ))
    }

    /// Regression test for <https://github.com/spiceai/spiceai/issues/12931>,
    /// the same defect on the search cache: an entry that matched nothing
    /// counted zero bytes, so no number of them could exhaust the budget.
    #[test]
    fn a_search_result_that_matched_nothing_is_still_billed() {
        let empty = empty_result_over(schema_of_width(4), Vec::new());

        assert!(
            empty.get_memory_size() > std::mem::size_of::<CachedSearchResult>(),
            "an entry holding a schema and a table set costs more than its own struct, got {}",
            empty.get_memory_size()
        );
    }

    #[test]
    fn a_wider_search_result_costs_more() {
        let narrow = empty_result_over(schema_of_width(4), Vec::new());
        let wide = empty_result_over(schema_of_width(200), Vec::new());

        assert!(
            wide.get_memory_size() > 10 * narrow.get_memory_size(),
            "a 200-column search entry must cost far more than a 4-column one, got {} vs {}",
            wide.get_memory_size(),
            narrow.get_memory_size()
        );
    }

    /// The pre-fix accounting charged `primary_keys.len() * size_of::<String>()`,
    /// which is the pointer triple and never the characters behind it.
    #[test]
    fn a_long_primary_key_name_is_billed_its_characters() {
        let short = empty_result_over(schema_of_width(1), vec!["id".to_string()]);
        let long = empty_result_over(schema_of_width(1), vec!["k".repeat(4_096)]);

        assert!(
            long.get_memory_size() >= short.get_memory_size() + 4_000,
            "a 4 KiB key name must be charged its bytes, got {} vs {}",
            long.get_memory_size(),
            short.get_memory_size()
        );
    }
}
