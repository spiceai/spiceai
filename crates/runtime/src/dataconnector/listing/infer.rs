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

use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::{
    datasource::listing::ListingTableUrl, error::DataFusionError, execution::SessionState,
};
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use object_store::path::{Path, DELIMITER};
use object_store::ObjectStore;
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to list all files when inferring partitions: {source}"))]
    ListAllFiles { source: DataFusionError },

    #[snafu(display("Found mixed partition values: {sorted_diff:?}"))]
    MixedPartitionValues { sorted_diff: [Vec<String>; 2] },

    #[snafu(display("Could not get object store: {source}"))]
    ObjectStore { source: DataFusionError },
}

pub type Result<T> = std::result::Result<T, Error>;

pub(crate) async fn infer_partitions_with_types(
    state: &SessionState,
    table_path: &ListingTableUrl,
    file_extension: &str,
) -> Result<Vec<(String, DataType)>> {
    let store = state
        .runtime_env()
        .object_store(table_path)
        .context(ObjectStoreSnafu)?;
    Ok(infer_partitions(state, table_path, store, file_extension)
        .await?
        .into_iter()
        .map(|col_name| (col_name, DataType::Utf8))
        .collect::<Vec<_>>())
}

/// Infer the partitioning at the given path on the provided object store.
/// For performance reasons, it doesn't read all the files on disk
/// and therefore may fail to detect invalid partitioning.
///
/// Modified from: <https://github.com/apache/datafusion/blob/main/datafusion/core/src/datasource/listing/table.rs>
async fn infer_partitions(
    state: &SessionState,
    table_path: &ListingTableUrl,
    store: Arc<dyn ObjectStore>,
    file_extension: &str,
) -> Result<Vec<String>> {
    // only use 10 files for inference
    // This can fail to detect inconsistent partition keys
    // A DFS traversal approach of the store can help here
    let files: Vec<_> = table_path
        .list_all_files(state, store.as_ref(), file_extension)
        .await
        .context(ListAllFilesSnafu)?
        .take(10)
        .try_collect()
        .await
        .context(ListAllFilesSnafu)?;

    let stripped_path_parts = files.iter().map(|file| {
        strip_prefix(table_path, &file.location)
            .unwrap_or_else(|| unreachable!("prefix always exists"))
            .collect_vec()
    });

    let partition_keys = stripped_path_parts
        .map(|path_parts| {
            path_parts
                .into_iter()
                .rev()
                .skip(1) // get parents only; skip the file itself
                .rev()
                .map(|s| s.split('=').take(1).collect())
                .collect_vec()
        })
        .collect_vec();

    match partition_keys.into_iter().all_equal_value() {
        Ok(v) => Ok(v),
        Err(None) => Ok(vec![]),
        Err(Some(diff)) => {
            let mut sorted_diff = [diff.0, diff.1];
            sorted_diff.sort();
            MixedPartitionValuesSnafu { sorted_diff }.fail()
        }
    }
}

/// Strips the prefix of this [`ListingTableUrl`] from the provided path, returning
/// an iterator of the remaining path segments
///
/// Modified from: <https://github.com/apache/datafusion/blob/main/datafusion/core/src/datasource/listing/url.rs>
fn strip_prefix<'a, 'b: 'a>(
    table_path: &'a ListingTableUrl,
    path: &'b Path,
) -> Option<impl Iterator<Item = &'b str> + 'a> {
    let mut stripped = path.as_ref().strip_prefix(table_path.prefix().as_ref())?;
    if !stripped.is_empty() && !table_path.prefix().as_ref().is_empty() {
        stripped = stripped.strip_prefix(DELIMITER)?;
    }
    Some(stripped.split_terminator(DELIMITER))
}
