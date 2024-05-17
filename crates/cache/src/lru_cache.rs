/*
Copyright 2024 The Spice.ai OSS Authors

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
use crate::AnyErrorResult;
use crate::QueryResultCache;
use arrow::array::RecordBatch;
use async_trait::async_trait;

pub struct LruCache {}

impl LruCache {
    pub fn new(_cache_max_size: usize) -> Self {
        LruCache {}
    }
}

#[async_trait]
impl QueryResultCache for LruCache {
    async fn get(&mut self, _key: u64) -> AnyErrorResult<Option<Vec<RecordBatch>>> {
        Ok(None)
    }

    async fn put(&mut self, _key: u64, _batches: Vec<RecordBatch>) -> AnyErrorResult<()> {
        Ok(())
    }
}
