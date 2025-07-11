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

use std::fmt::Debug;

use async_trait::async_trait;
use datafusion::{
    error::DataFusionError, logical_expr::TableProviderFilterPushDown, prelude::Expr,
    scalar::ScalarValue,
};
use snafu::prelude::*;

use crate::Partition;

pub mod filename;

type StdError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create an accelerated partition: {source}"))]
    CreatePartition { source: StdError },
    #[snafu(display("Failed to infer accelerated partitions: {source}"))]
    InferringPartitions { source: StdError },
    #[snafu(display(
        "The 'partition_by' expressions are different from the expressions used to create the existing partition files. Revert the 'partition_by' expressions, delete the partition files, or change the location the partition files are stored to create new partitions."
    ))]
    PartitionByExpressionsChanged,
}

#[async_trait]
pub trait PartitionCreator: Debug + Send + Sync {
    /// Create a new [`Partition`] using the `partition_value`.
    ///
    /// # Errors
    /// Returns an error when creating a [`Partition`] is unsuccessful.
    async fn create_partition(&self, partition_value: ScalarValue) -> Result<Partition, Error>;

    /// Find and load previously created [`Partition`]s
    ///
    /// # Errors
    /// Returns an error when [`Partition`]s cannot be inferred.
    async fn infer_existing_partitions(&self) -> Result<Vec<Partition>, Error>;

    /// See [`TableProvider::supports_filters_pushdown`].
    ///
    /// # Errors
    /// See [`TableProvider::supports_filters_pushdown`].
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError>;
}
