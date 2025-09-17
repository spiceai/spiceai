/*
Copyright 2024-2025 The Spice.ai OSS Authors

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
    #[must_use]
    pub fn new(
        records: Arc<Vec<RecordBatch>>,
        primary_keys: Vec<String>,
        data_columns: Vec<String>,
        matches: HashMap<String, Vec<String>>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            records,
            primary_keys,
            data_columns,
            matches,
            schema,
        }
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
        self.results
            .values()
            .map(|result| {
                result
                    .records
                    .iter()
                    .map(arrow::array::RecordBatch::get_array_memory_size)
                    .sum::<usize>()
                    + (result.primary_keys.len() * std::mem::size_of::<String>())
                    + (result.data_columns.len() * std::mem::size_of::<String>())
                    + result
                        .matches
                        .iter()
                        .map(|(key, values)| {
                            key.len() + values.iter().map(std::string::String::len).sum::<usize>()
                        })
                        .sum::<usize>()
            })
            .sum()
    }
}
