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

use arrow::datatypes::DataType;
use datafusion::{
    datasource::listing::ListingTableUrl, error::DataFusionError, execution::SessionState,
};
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use object_store::path::{DELIMITER, Path};
use object_store::{ObjectMeta, ObjectStore};
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to list all files when inferring partitions: {source}"))]
    ListAllFiles { source: DataFusionError },

    #[snafu(display("Found mixed partition values: {sorted_diff:?}"))]
    MixedPartitionValues { sorted_diff: [Vec<String>; 2] },

    #[snafu(display("Prefix: {prefix} does not contain file: {file_path}"))]
    FileNotContainedInPrefix { prefix: Path, file_path: Path },

    #[snafu(display("Could not get object store: {source}"))]
    ObjectStore { source: DataFusionError },
}

pub type Result<T> = std::result::Result<T, Error>;

/// Infer the partitioning at the given path prefix on the provided object store.
pub(crate) async fn infer_partitions_with_types_prefix(
    state: &SessionState,
    table_path_prefix: &ListingTableUrl,
    file_extension: &str,
) -> Result<Vec<(String, DataType)>> {
    let store = state
        .runtime_env()
        .object_store(table_path_prefix)
        .context(ObjectStoreSnafu)?;
    let files = list_max_10_files(state, table_path_prefix, &store, file_extension).await?;

    infer_partitions_with_types_from_files(table_path_prefix, &files)
}

/// Infer the partitioning from a given list of files and a common prefix.
pub(crate) fn infer_partitions_with_types_from_files(
    table_path_prefix: &ListingTableUrl,
    files: &[ObjectMeta],
) -> Result<Vec<(String, DataType)>> {
    Ok(infer_partitions(table_path_prefix, files)?
        .into_iter()
        .map(|col_name| (col_name, DataType::Utf8))
        .collect::<Vec<_>>())
}

async fn list_max_10_files(
    state: &SessionState,
    table_path: &ListingTableUrl,
    store: &dyn ObjectStore,
    file_extension: &str,
) -> Result<Vec<ObjectMeta>> {
    table_path
        .list_all_files(state, store, file_extension)
        .await
        .context(ListAllFilesSnafu)?
        .take(10)
        .try_collect()
        .await
        .context(ListAllFilesSnafu)
}

/// Infer the partitioning at the given path on the provided object store.
/// For performance reasons, it doesn't read all the files on disk
/// and therefore may fail to detect invalid partitioning.
///
/// Modified from: <https://github.com/apache/datafusion/blob/main/datafusion/core/src/datasource/listing/table.rs>
fn infer_partitions(
    table_path_prefix: &ListingTableUrl,
    files: &[ObjectMeta],
) -> Result<Vec<String>> {
    let stripped_path_parts = files.iter().map(|file| {
        Ok(strip_prefix(table_path_prefix, &file.location)
            .ok_or_else(|| {
                FileNotContainedInPrefixSnafu {
                    prefix: table_path_prefix.prefix().to_string(),
                    file_path: file.location.to_string(),
                }
                .build()
            })?
            .collect_vec())
    });

    let partition_keys = stripped_path_parts
        .map(|path_parts| {
            Ok(path_parts?
                .into_iter()
                .rev()
                .skip(1) // get parents only; skip the file itself
                .rev()
                .map(|s| s.split('=').take(1).collect())
                .collect_vec())
        })
        .collect::<Result<Vec<_>>>()?;

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

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_object_meta(path: &str, size: usize) -> ObjectMeta {
        ObjectMeta {
            location: object_store::path::Path::from(path),
            last_modified: chrono::Utc::now(),
            size,
            e_tag: None,
            version: None,
        }
    }

    #[test]
    fn test_infer_partitions_with_types_from_files() {
        let files = vec![
            create_test_object_meta("data/year=2023/month=01/file.parquet", 100),
            create_test_object_meta("data/year=2023/month=02/file.parquet", 100),
        ];

        let table_url =
            ListingTableUrl::parse("memory://bucket/data/").expect("Failed to parse table URL");
        println!("table_url.prefix(): {:?}", table_url.prefix());
        let result = infer_partitions_with_types_from_files(&table_url, &files)
            .expect("Failed to infer partitions");

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, "year");
        assert_eq!(result[1].0, "month");
        assert!(matches!(result[0].1, DataType::Utf8));
        assert!(matches!(result[1].1, DataType::Utf8));
    }

    #[test]
    fn test_infer_partitions_with_types_from_files_mixed_values() {
        let files = vec![
            create_test_object_meta("data/year=2023/month=01/file.parquet", 100),
            create_test_object_meta("data/year=2023/day=01/file.parquet", 100),
        ];

        let table_url =
            ListingTableUrl::parse("memory://bucket/data").expect("Failed to parse table URL");
        let result = infer_partitions_with_types_from_files(&table_url, &files);

        assert!(matches!(result, Err(Error::MixedPartitionValues { .. })));
    }

    #[test]
    fn test_infer_partitions_with_types_from_files_not_contained() {
        let files = vec![create_test_object_meta(
            "other/year=2023/month=01/file.parquet",
            100,
        )];

        let table_url =
            ListingTableUrl::parse("memory://bucket/data").expect("Failed to parse table URL");
        let result = infer_partitions_with_types_from_files(&table_url, &files);

        assert!(matches!(
            result,
            Err(Error::FileNotContainedInPrefix { .. })
        ));
    }

    #[test]
    fn test_infer_partitions_with_types_from_files_empty() {
        let files = vec![];
        let table_url = ListingTableUrl::parse("memory://data").expect("Failed to parse table URL");
        let result = infer_partitions_with_types_from_files(&table_url, &files)
            .expect("Expected empty partitions");
        assert!(result.is_empty());
    }

    #[test]
    fn test_strip_prefix() {
        let table_url =
            ListingTableUrl::parse("memory://bucket/data").expect("Failed to parse table URL");
        let path = object_store::path::Path::from("data/year=2023/month=01/file.parquet");

        let stripped = strip_prefix(&table_url, &path).expect("Failed to strip prefix");
        let parts: Vec<&str> = stripped.collect();

        assert_eq!(parts, vec!["year=2023", "month=01", "file.parquet"]);
    }

    #[test]
    fn test_strip_prefix_no_match() {
        let table_url =
            ListingTableUrl::parse("memory://data/path").expect("Failed to parse table URL");
        let path = object_store::path::Path::from("other/year=2023/month=01/file.parquet");

        let stripped = strip_prefix(&table_url, &path);
        assert!(stripped.is_none());
    }

    #[test]
    fn test_strip_prefix_empty() {
        let table_url =
            ListingTableUrl::parse("memory://bucket/data").expect("Failed to parse table URL");
        let path = object_store::path::Path::from("data");

        let stripped = strip_prefix(&table_url, &path).expect("Failed to strip prefix");
        let parts: Vec<&str> = stripped.collect();

        assert!(parts.is_empty());
    }
}
