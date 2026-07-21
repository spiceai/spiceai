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

use arrow::array::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::LogicalPlanBuilder;
use runtime_datafusion_index::{Index, build_key_match_predicate};

use crate::index::VectorIndex;
use crate::index::s3_vectors::S3Vector;
use crate::index::s3_vectors::write::extract_and_format_primary_key;

/// Deletes exact keys from `index`. `keys` must have every column of `index.primary_key`.
///
/// Does not yet support `partition_by` (each partition value maps to a distinct virtual S3
/// Vectors index; resolving which partition a given key row belongs to needs its own bridge —
/// not yet exercised by any caller, since only `Chunked*` delete needs `delete_by_key_prefix`
/// today, and no partitioned+chunked S3 Vectors config exists in practice).
pub async fn delete_by_keys(index: &S3Vector, keys: &RecordBatch) -> DataFusionResult<()> {
    if !index.partition_by.is_empty() {
        return Err(DataFusionError::NotImplemented(
            "S3Vector delete is not yet supported for a partitioned vector index".to_string(),
        ));
    }

    let key_strings: Vec<String> =
        extract_and_format_primary_key(index.name(), &index.primary_key, keys)
            .map_err(|e| DataFusionError::External(Box::new(*e)))?
            .into_iter()
            .flatten()
            .collect();

    index
        .table
        .delete_by_keys(key_strings)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

/// Deletes every entry whose columns match `prefix_keys` on just the columns `prefix_keys`
/// itself has (a strict subset of `index.primary_key` — the chunked-index case, where the outer
/// caller doesn't know the chunk-id column that makes up the rest of the real key).
///
/// S3 Vectors' `ListVectors` has no server-side metadata filter, so this resolves matches by
/// scanning the index's own [`VectorIndex::list_table_provider`] with the prefix predicate
/// applied client-side, then re-derives the exact composite keys for the matches and deletes
/// them via [`delete_by_keys`].
pub async fn delete_by_key_prefix(
    index: &S3Vector,
    prefix_keys: &RecordBatch,
) -> DataFusionResult<()> {
    if !index.partition_by.is_empty() {
        return Err(DataFusionError::NotImplemented(
            "S3Vector prefix delete is not yet supported for a partitioned vector index"
                .to_string(),
        ));
    }

    let prefix_columns: Vec<String> = prefix_keys
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let Some(predicate) = build_key_match_predicate(prefix_keys, &prefix_columns)? else {
        return Ok(());
    };

    let list_plan = index.list_table_provider()?;
    let filtered_plan = LogicalPlanBuilder::from(list_plan)
        .filter(predicate)?
        .build()?;

    let ctx = SessionContext::new();
    let matches = ctx
        .execute_logical_plan(filtered_plan)
        .await?
        .collect()
        .await?;

    if matches.is_empty() || matches.iter().all(|b| b.num_rows() == 0) {
        return Ok(());
    }

    for batch in &matches {
        delete_by_keys(index, batch).await?;
    }

    Ok(())
}
