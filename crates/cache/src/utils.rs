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

use std::{collections::HashSet, sync::Arc};

use arrow::array::RecordBatch;
use datafusion::{
    execution::SendableRecordBatchStream, logical_expr::LogicalPlan,
    physical_plan::stream::RecordBatchStreamAdapter,
};

use crate::{CachedQueryResult, QueryResultsCacheProvider};

use async_stream::stream;

use futures::StreamExt;

#[must_use]
pub fn to_cached_record_batch_stream(
    cache_provider: Arc<QueryResultsCacheProvider>,
    mut stream: SendableRecordBatchStream,
    plan: LogicalPlan,
) -> SendableRecordBatchStream {
    let schema = stream.schema();
    let schema_copy = Arc::clone(&schema);

    let cached_result_stream = stream! {
        let mut records: Vec<RecordBatch> = Vec::new();
        let mut records_size: usize = 0;
        let cache_max_size: usize = cache_provider.max_size().try_into().unwrap_or(usize::MAX);

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
                schema: schema_copy,
                input_tables: Arc::new(get_logical_plan_input_tables(&plan)),
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

#[must_use]
pub fn get_logical_plan_input_tables(plan: &LogicalPlan) -> HashSet<String> {
    let mut table_names: HashSet<String> = HashSet::new();
    collect_table_names(plan, &mut table_names);
    table_names
}

fn collect_table_names(plan: &LogicalPlan, table_names: &mut HashSet<String>) {
    if let LogicalPlan::TableScan(source, ..) = plan {
        table_names.insert(source.table_name.to_string().to_lowercase());
    }

    plan.inputs().iter().for_each(|input| {
        collect_table_names(input, table_names);
    });
}
