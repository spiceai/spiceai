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

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion::sql::TableReference;

use crate::Sizeable;

use super::CacheStatus;

// TODO: Move VectorSearchResult into the `search` crate to prevent circular dependency, to reuse here?
// https://github.com/spiceai/spiceai/issues/6018
#[derive(Clone)]
pub struct CachedAggregationResult {
    pub records: Arc<Vec<RecordBatch>>,
    pub primary_keys: Vec<Arc<str>>,
    pub data_columns: Vec<Arc<str>>,
    pub matches: Arc<HashMap<String, Vec<String>>>,
}

impl CachedAggregationResult {
    #[must_use]
    pub fn new(
        records: Arc<Vec<RecordBatch>>,
        primary_keys: Vec<Arc<str>>,
        data_columns: Vec<Arc<str>>,
        matches: Arc<HashMap<String, Vec<String>>>,
    ) -> Self {
        Self {
            records,
            primary_keys,
            data_columns,
            matches,
        }
    }
}

#[derive(Clone)]
pub struct CachedSearchResult {
    pub results: Arc<HashMap<TableReference, CachedAggregationResult>>,
    pub cache_status: CacheStatus,
}

impl Sizeable for CachedSearchResult {
    fn get_memory_size(&self) -> usize {
        self.results
            .iter()
            .map(|(_, result)| {
                result
                    .records
                    .iter()
                    .map(arrow::array::RecordBatch::get_array_memory_size)
                    .sum::<usize>()
                    + (result.primary_keys.len() * std::mem::size_of::<Arc<str>>())
                    + (result.data_columns.len() * std::mem::size_of::<Arc<str>>())
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
