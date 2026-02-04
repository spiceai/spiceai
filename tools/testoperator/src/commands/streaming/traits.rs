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

//! Trait definitions for streaming ingestion benchmarks.
//!
//! The streaming benchmark system is built around these abstractions:
//! - [`StreamingDataset`]: Defines a table within a benchmark dataset (e.g., TPCH lineitem)
//! - [`StreamingSource`]: Generic interface for streaming sources (`DynamoDB`, Kafka, etc.)
//! - [`DynamoDBStreamingSource`]: DynamoDB-specific extension with snapshot/checkpoint support

use arrow::array::RecordBatch;
use spicepod::spec::SpicepodDefinition;
use test_framework::anyhow::Result;

use super::datasets::DatasetType;

/// Configuration for snapshot storage (S3).
#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    /// Base S3 location for snapshots (e.g., "<s3://bucket/snapshots>/")
    pub location: String,
    /// S3 access key ID
    pub access_key_id: Option<String>,
    /// S3 secret access key
    pub secret_access_key: Option<String>,
    /// S3 region
    pub region: Option<String>,
}

/// Represents a table that can be generated and inserted into a streaming source.
///
/// Each dataset implementation knows:
/// - How to generate test data at various scale factors
/// - How to create and detect marker records for benchmark timing
pub trait StreamingDataset: Send + Sync {
    /// Returns the table name for this dataset.
    fn table_name(&self) -> &str;

    /// Returns the dataset type enum for this dataset.
    fn dataset_type(&self) -> DatasetType;

    /// Generate records for the given scale factor.
    fn generate(&self, scale_factor: f64) -> Result<Vec<RecordBatch>>;

    /// Create a marker record that fits this dataset's schema.
    fn marker_record(&self) -> Result<RecordBatch>;

    /// Returns the expected number of marker records.
    /// Defaults to 1, but datasets with multiple shards (like lineitem) may return more.
    fn marker_count(&self) -> usize {
        1
    }

    /// Returns a SQL query to detect the marker in Spice's accelerated table.
    fn marker_detection_query(&self) -> String;

    /// Returns the Arrow schema for this dataset.
    fn schema(&self) -> arrow::datatypes::Schema;

    /// Returns the primary key column names for this dataset.
    fn primary_key_columns(&self) -> Vec<&'static str>;

    /// Returns a SQL query for liveness checks (COUNT(*) by default).
    fn liveness_query(&self) -> String {
        format!("SELECT COUNT(*) FROM {}", self.table_name())
    }
}

/// Represents a streaming source that can receive data.
///
/// This is the generic interface for all streaming sources (`DynamoDB`, Kafka, etc.).
/// Sources match on `DatasetType` to determine source-specific configuration
/// like key schemas, topic settings, etc.
#[expect(dead_code)]
#[async_trait::async_trait]
pub trait StreamingSource: Send + Sync {
    /// Set a table name prefix for isolated test runs.
    ///
    /// When set, all table names will be prefixed with this value.
    /// For example, with prefix "abc123", table "lineitem" becomes "`abc123_lineitem`".
    fn set_table_prefix(&mut self, prefix: String);

    /// Set the scale factor for TPCH data generation.
    ///
    /// This is used for tagging tables with metadata about the data they contain.
    fn set_scale_factor(&mut self, scale_factor: f64);

    /// Get the actual table name, applying the prefix if set.
    fn get_table_name(&self, base_name: &str) -> String;

    /// Start containers and initialize the source.
    async fn prepare(&mut self) -> Result<()>;

    /// Create a table/topic for the given dataset type.
    ///
    /// The implementation matches on dataset type to determine the appropriate
    /// key schema and other source-specific configuration.
    async fn create_table(&self, dataset: DatasetType) -> Result<()>;

    /// Insert records into the specified table.
    async fn insert(&self, table: &str, records: &[RecordBatch]) -> Result<()>;

    /// Delete a marker record.
    async fn delete_marker(&self, dataset: DatasetType) -> Result<()>;

    /// Update existing records in the specified table.
    ///
    /// The records should contain the primary key columns and the columns to update.
    async fn update(&self, table: &str, records: &[RecordBatch]) -> Result<()>;

    /// Delete records from the specified table.
    ///
    /// The records should contain only the primary key columns.
    async fn delete(&self, table: &str, keys: &[RecordBatch]) -> Result<()>;

    /// Cleanup resources (stop containers, etc.).
    async fn cleanup(&self) -> Result<()>;
}

/// DynamoDB-specific streaming source with snapshot/checkpoint support.
///
/// This trait extends [`StreamingSource`] with methods for transforming spicepods
/// to capture checkpoints and restore from snapshots. This is required for `DynamoDB`
/// benchmarks because `DynamoDB` Streams has limited retention (24 hours) and shard
/// lifecycle issues that require snapshot-based checkpoint capture.
pub trait DynamoDBStreamingSource: StreamingSource {
    /// Transform spicepod for checkpoint capture phase.
    ///
    /// This method:
    /// - Renames datasets with `run_id` prefix to match `DynamoDB` table names
    /// - Sets `acceleration.snapshots: create_only` to capture checkpoint
    /// - Configures runtime snapshot location
    fn prepare_checkpoint_spicepod(
        &self,
        spicepod: SpicepodDefinition,
        run_id: &str,
        config_name: &str,
        snapshot_config: &SnapshotConfig,
    ) -> SpicepodDefinition;

    /// Transform spicepod for benchmark phase.
    ///
    /// This method:
    /// - Renames datasets with `run_id` prefix to match `DynamoDB` table names
    /// - Sets `acceleration.snapshots: bootstrap_only` to restore from snapshot
    /// - Configures runtime snapshot location
    fn prepare_benchmark_spicepod(
        &self,
        spicepod: SpicepodDefinition,
        run_id: &str,
        config_name: &str,
        snapshot_config: &SnapshotConfig,
    ) -> SpicepodDefinition;
}
