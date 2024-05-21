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

use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion::{
    execution::SendableRecordBatchStream, logical_expr::LogicalPlan,
    physical_plan::stream::RecordBatchStreamAdapter,
};

use crate::{CachedQueryResult, QueryResultCacheProvider};

use async_stream::stream;

use futures::StreamExt;

#[must_use]
pub fn to_cached_record_batch_stream(
    cache_provider: QueryResultCacheProvider,
    mut stream: SendableRecordBatchStream,
    plan: LogicalPlan,
) -> SendableRecordBatchStream {
    let schema = stream.schema();
    let schema_copy = Arc::clone(&schema);

    let cached_result_stream = stream! {
        let mut records: Vec<RecordBatch> = Vec::new();
        let mut records_size: usize = 0;
        let cache_max_size: usize = cache_provider.cache_max_size().try_into().unwrap_or(usize::MAX);

        while let Some(batch_result) = stream.next().await {
            if records_size < cache_max_size {
                if let Ok(batch) = &batch_result {
                    records.push(batch.clone());
                    records_size += batch.get_array_memory_size();
                }
            }

            yield batch_result;
        }

        if records_size < cache_max_size {
            let cached_result = CachedQueryResult {
                records: Arc::new(records),
                schema: schema_copy
            };

            if let Err(e) = cache_provider.put(&plan, cached_result).await {
                tracing::error!("Failed to cache query results: {e}");
            }
        }
    };

    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        Box::pin(cached_result_stream),
    ))
}
