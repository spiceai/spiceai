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
use runtime_datafusion_index::Index;

use crate::index::s3_vectors::S3Vector;
use crate::index::write_util::extract_and_format_primary_key;

/// Deletes exact keys from `index`. `keys` must have every column of `index.primary_key`
/// (extras are ignored — only `index.primary_key`'s own columns are read from `keys`).
///
/// Does not yet support `partition_by` (each partition value maps to a distinct virtual S3
/// Vectors index; resolving which partition a given key row belongs to needs its own bridge —
/// not yet exercised by any caller in practice).
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
