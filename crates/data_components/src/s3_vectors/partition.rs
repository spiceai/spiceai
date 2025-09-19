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

use datafusion::{
    error::DataFusionError, prelude::Expr, scalar::ScalarValue, sql::unparser::expr_to_sql,
};
use snafu::prelude::*;
use twox_hash::XxHash64;

const HASH_SEED: u64 = 7;

const INDEX_NAME_MAX_LENGTH: usize = 25;
const COLUMN_NAME_MAX_LENGTH: usize = 25;
const PARTITION_VALUE_MAX_LENGTH: usize = 5;
const PARTITION_BY_MAX_LENGTH: usize = 5;

static PARTS_SEPARATOR: &str = ".";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Expected exactly 4 parts in index name separated by hyphens, but found {num_parts}"
    ))]
    IncorrectNumPartsInName { num_parts: usize },
    #[snafu(display("Failed to unparse a partition_by expression: {source}"))]
    UnparsingExpression { source: DataFusionError },
}

#[derive(Debug)]
pub struct PartitionedIndexName {
    index_name: String,
    column_name: String,
    partition_value_hash: String,
    partition_by_hash: String,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum BelongsWith {
    SameDataset,
    DifferentDataset,
    DifferentParitionByExpressions,
}

impl PartitionedIndexName {
    pub fn new(
        index_name: &str,
        column_name: &str,
        partition_value: &ScalarValue,
        partition_by: &[Expr],
    ) -> Result<Self, Error> {
        let index_name = truncate(&sanitize(index_name), INDEX_NAME_MAX_LENGTH);
        let column_name = truncate(&sanitize(column_name), COLUMN_NAME_MAX_LENGTH);
        let partition_value_hash = truncate(
            &hash_to_hex(&partition_value.to_string()),
            PARTITION_VALUE_MAX_LENGTH,
        );
        let partition_by_hash = truncate(
            &hash_to_hex(&to_stable_string(partition_by).context(UnparsingExpressionSnafu)?),
            PARTITION_BY_MAX_LENGTH,
        );
        Ok(Self {
            index_name,
            column_name,
            partition_value_hash,
            partition_by_hash,
        })
    }

    /// Format an index name suitable for S3 Vectors
    #[must_use]
    pub fn to_index_name(&self) -> String {
        [
            self.index_name.clone(),
            self.column_name.clone(),
            self.partition_value_hash.clone(),
            self.partition_by_hash.clone(),
        ]
        .join(PARTS_SEPARATOR)
    }

    pub fn from_index_name(index_name: &str) -> Result<Self, Error> {
        let parts: Vec<&str> = index_name.split(PARTS_SEPARATOR).collect();
        let num_parts = parts.len();
        ensure!(num_parts == 4, IncorrectNumPartsInNameSnafu { num_parts });
        Ok(Self {
            index_name: parts[0].to_string(),
            column_name: parts[1].to_string(),
            partition_value_hash: parts[2].to_string(),
            partition_by_hash: parts[3].to_string(),
        })
    }

    /// Determines if the partitions come from the same dataset
    #[must_use]
    pub fn belongs_with(&self, other: &Self) -> BelongsWith {
        if self.index_name != other.index_name || self.column_name != other.column_name {
            return BelongsWith::DifferentDataset;
        }
        if self.partition_by_hash != other.partition_by_hash {
            return BelongsWith::DifferentParitionByExpressions;
        }
        BelongsWith::SameDataset
    }
}

fn sanitize(s: &str) -> String {
    s.replace('_', "-")
}

fn truncate(s: &str, len: usize) -> String {
    s.chars().take(len).collect()
}

fn hash_to_hex(input: &str) -> String {
    let hash = XxHash64::oneshot(HASH_SEED, input.as_bytes());
    format!("{hash:x}")
}

// Provide a stable string representation of the expressions
fn to_stable_string(exprs: &[Expr]) -> Result<String, DataFusionError> {
    Ok(exprs
        .iter()
        .map(|expr| expr_to_sql(expr).map(|e| e.to_string()))
        .collect::<Result<Vec<String>, DataFusionError>>()?
        .join(PARTS_SEPARATOR))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::col;
    use datafusion::scalar::ScalarValue;

    /// See [CreateIndex](https://docs.aws.amazon.com/AmazonS3/latest/API/API_S3VectorBuckets_CreateIndex.html#API_S3VectorBuckets_CreateIndex_RequestSyntax)
    const S3_VECTOR_INDEX_NAME_MAX_LENGTH: usize = 63;

    #[test]
    fn index_name_length_restricted() {
        let index_name = "a".repeat(INDEX_NAME_MAX_LENGTH + 1);
        let column_name = "col1";
        let partition_value = ScalarValue::from("val");
        let partition_by = vec![col("col1")];

        let result =
            PartitionedIndexName::new(&index_name, column_name, &partition_value, &partition_by)
                .expect("result");
        let result = result.to_index_name();

        assert!(result.len() <= S3_VECTOR_INDEX_NAME_MAX_LENGTH);
        assert_eq!(
            result.split(PARTS_SEPARATOR).next().expect("next").len(),
            INDEX_NAME_MAX_LENGTH
        );
    }

    #[test]
    fn new_index_partition_name() {
        let index_name = "test_index";
        let column_name = "test_col";
        let partition_value = ScalarValue::from("value");
        let partition_by = vec![col("col1")];

        let result =
            PartitionedIndexName::new(index_name, column_name, &partition_value, &partition_by)
                .expect("new");

        assert_eq!(result.index_name, "test-index");
        assert_eq!(result.column_name, "test-col");
        assert_eq!(
            result.partition_value_hash.len(),
            PARTITION_VALUE_MAX_LENGTH
        );
        assert_eq!(result.partition_by_hash.len(), PARTITION_BY_MAX_LENGTH);
    }

    #[test]
    fn from_index_name_valid() {
        let name = "test-index.test-col.12345.abcde";
        let result = PartitionedIndexName::from_index_name(name).expect("from_index_name");

        assert_eq!(result.index_name, "test-index");
        assert_eq!(result.column_name, "test-col");
        assert_eq!(result.partition_value_hash, "12345");
        assert_eq!(result.partition_by_hash, "abcde");
    }

    #[test]
    fn from_index_name_invalid_parts() {
        let name = "test.index.col";
        let result = PartitionedIndexName::from_index_name(name);

        assert!(result.is_err());
    }

    #[test]
    fn sanitize_replaces_underscores() {
        let input = "test_index_name";
        let result = sanitize(input);
        assert_eq!(result, "test-index-name");
    }

    #[test]
    fn truncate_limits_length() {
        let input = "a".repeat(10);
        let result = truncate(&input, 5);
        assert_eq!(result, "aaaaa");
    }

    #[test]
    fn hash_to_hex_consistent() {
        let input = "test";
        let result1 = hash_to_hex(input);
        let result2 = hash_to_hex(input);
        assert_eq!(result1, result2);
    }

    #[test]
    fn to_index_name_format() {
        let index = PartitionedIndexName {
            index_name: "idx".to_string(),
            column_name: "col".to_string(),
            partition_value_hash: "12345".to_string(),
            partition_by_hash: "abcde".to_string(),
        };

        let result = index.to_index_name();
        assert_eq!(result, "idx.col.12345.abcde");
    }
}
