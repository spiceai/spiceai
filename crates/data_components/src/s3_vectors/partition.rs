/*
Copyright 2025 The Spice.ai OSS Authors

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

use datafusion::{prelude::Expr, scalar::ScalarValue};
use runtime_table_partition::naming::{
    BelongsWith, PartitionNameConfig, PartitionedName, Result, hash_to_hex, truncate,
};

const INDEX_NAME_MAX_LENGTH: usize = 45;
const COLUMN_NAME_MAX_LENGTH: usize = 5;

/// See [CreateIndex](https://docs.aws.amazon.com/AmazonS3/latest/API/API_S3VectorBuckets_CreateIndex.html#API_S3VectorBuckets_CreateIndex_RequestSyntax)
const _S3_VECTOR_INDEX_NAME_MAX_LENGTH: usize = 63;

struct Config;

impl PartitionNameConfig for Config {
    const PREFIX_MAX_LENGTH: usize = INDEX_NAME_MAX_LENGTH + COLUMN_NAME_MAX_LENGTH + 1; // 1 separator
    const PARTITION_BY_MAX_LENGTH: usize = 5;
    const PARTITION_VALUE_MAX_LENGTH: usize = 5;
    const PARTS_SEPARATOR: &'static str = ".";
}

#[derive(Debug)]
pub struct PartitionedIndexName {
    inner: PartitionedName,
}

fn make_prefix(index_name: &str, column_name: &str) -> String {
    let index_name = Config::sanitize(index_name);
    let column_name_hash = truncate(&hash_to_hex(column_name), COLUMN_NAME_MAX_LENGTH);
    let combined_prefix = [index_name, column_name_hash].join(Config::PARTS_SEPARATOR);
    Config::sanitize(&combined_prefix)
}

impl PartitionedIndexName {
    pub fn new(
        index_name: &str,
        column_name: &str,
        partition_by: &[Expr],
        partition_value: &ScalarValue,
    ) -> Result<Self> {
        let combined_prefix = make_prefix(index_name, &column_name);
        let inner =
            PartitionedName::new::<Config>(&combined_prefix, partition_by, partition_value)?;
        Ok(Self { inner })
    }

    pub fn common_prefix(
        index_name: &str,
        column_name: &str,
        partition_by: &[Expr],
    ) -> Result<String> {
        let prefix = make_prefix(index_name, column_name);
        PartitionedName::common_prefix::<Config>(&prefix, partition_by)
    }

    pub fn from_index_name(index_name: &str) -> Result<Self> {
        let inner = PartitionedName::from_partition_name::<Config>(index_name)?;
        Ok(Self { inner })
    }

    pub fn to_index_name(&self) -> String {
        self.inner.to_partition_name::<Config>()
    }

    pub fn belongs_with(
        &self,
        index_name: &str,
        column_name: &str,
        partition_by: &[Expr],
    ) -> BelongsWith {
        let prefix = make_prefix(index_name, column_name);
        self.inner.belongs_with::<Config>(&prefix, partition_by)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use datafusion::prelude::col;
    use datafusion::scalar::ScalarValue;

    #[test]
    fn belongs_with() -> Result<()> {
        let index_name = "mydataset";
        let column_name = "_my.column";
        let partition_by = &[col(column_name)];

        let this = PartitionedIndexName::from_index_name("mydataset-29d6f.7f7c5.blahh")?;

        assert_eq!(
            this.belongs_with(index_name, column_name, partition_by),
            BelongsWith::ThisDataset
        );
        assert_eq!(
            this.belongs_with(index_name, "_your.column", partition_by),
            BelongsWith::DifferentDataset
        );
        assert_eq!(
            this.belongs_with(index_name, column_name, &[]),
            BelongsWith::DifferentPartitionByExpressions
        );
        assert_eq!(
            this.belongs_with("yourdataset", column_name, partition_by),
            BelongsWith::DifferentDataset
        );

        Ok(())
    }

    #[test]
    fn index_name_length_restricted() {
        let index_name = "a".repeat(INDEX_NAME_MAX_LENGTH + 1);
        let column_name = "col1";
        let partition_value = ScalarValue::from("val");
        let partition_by = vec![col("col1")];

        assert!(
            PartitionedIndexName::new(&index_name, column_name, &partition_by, &partition_value)
                .is_err()
        );
    }

    #[test]
    fn new_index_partition_name() -> Result<()> {
        let index_name = "test_index";
        let column_name = "test_col";
        let partition_value = ScalarValue::from("value");
        let partition_by = vec![col("col1")];

        let result =
            PartitionedIndexName::new(index_name, column_name, &partition_by, &partition_value)?;

        assert_eq!(result.inner.prefix, "test-index-17308");

        Ok(())
    }

    #[test]
    fn from_index_name_invalid_parts() {
        let name = "mydata.col.expr.value";
        let result = PartitionedIndexName::from_index_name(name);

        assert!(result.is_err());
    }

    #[test]
    fn common_prefix() -> Result<()> {
        let index_name = "test_index";
        let column_name = "test_col";
        let partition_value = ScalarValue::from("value");
        let partition_by = vec![col("col1")];

        let result =
            PartitionedIndexName::new(index_name, column_name, &partition_by, &partition_value)?;

        let prefix = PartitionedIndexName::common_prefix(index_name, column_name, &partition_by)?;
        let index_name = result.to_index_name();
        assert!(index_name.starts_with(&prefix));

        Ok(())
    }
}
