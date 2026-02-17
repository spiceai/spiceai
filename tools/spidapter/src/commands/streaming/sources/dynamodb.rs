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

//! AWS `DynamoDB` Streams source implementation.
//!
//! This source connects to actual AWS `DynamoDB` (not a local Docker container).
//! It supports key-based authentication configured via environment variables.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    Array, Date32Array, Decimal128Array, Float64Array, Int16Array, Int32Array, Int64Array,
    RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use aws_config::{BehaviorVersion, Region, SdkConfig, retry::RetryConfig};
use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType, PutRequest,
    ScalarAttributeType, StreamSpecification, StreamViewType, Tag, WriteRequest,
};
use futures::stream::{self, StreamExt};
use test_framework::anyhow::{self, Context, Result};
use tokio::time::sleep;

/// Maximum items per batch write request (`DynamoDB` limit).
const BATCH_SIZE: usize = 25;

/// Number of concurrent batch write requests.
const CONCURRENT_BATCHES: usize = 20;

/// Tag key for creation timestamp (Unix seconds).
const TAG_CREATED_AT: &str = "testoperator:created_at";

/// Tag key for run ID.
const TAG_RUN_ID: &str = "testoperator:run_id";

/// Tag key for scale factor.
const TAG_SCALE_FACTOR: &str = "testoperator:scale_factor";

/// Maximum age of tables before cleanup (24 hours).
const STALE_TABLE_AGE_SECS: u64 = 24 * 60 * 60;

use spicepod::acceleration::SnapshotBehavior;
use spicepod::component::ComponentOrReference;
use spicepod::component::snapshot::Snapshots;
use spicepod::metric::{Metric, Metrics};
use spicepod::param::{ParamValue, Params};
use spicepod::spec::SpicepodDefinition;

use crate::commands::streaming::datasets::DatasetType;
use crate::commands::streaming::traits::{
    DynamoDBStreamingSource, SnapshotConfig, StreamingSource,
};

/// Configuration for AWS `DynamoDB` source.
///
/// Configuration is read from environment variables:
/// - `DYNAMODB_AWS_REGION`: AWS region (required)
/// - `DYNAMODB_AWS_ACCESS_KEY_ID`: AWS access key ID (required)
/// - `DYNAMODB_AWS_SECRET_ACCESS_KEY`: AWS secret access key (required)
/// - `DYNAMODB_AWS_ENDPOINT_URL`: Custom endpoint URL (optional, for `LocalStack`)
#[derive(Debug, Clone)]
pub struct DynamoDbConfig {
    /// AWS region (e.g., "us-east-1")
    pub region: String,
    /// AWS access key ID
    pub access_key_id: String,
    /// AWS secret access key
    pub secret_access_key: String,
    /// Optional custom endpoint URL (for `LocalStack`, testing, etc.)
    pub endpoint_url: Option<String>,
}

impl DynamoDbConfig {
    /// Create configuration from environment variables.
    ///
    /// # Errors
    /// Returns an error if required environment variables are not set.
    pub fn from_env() -> Result<Self> {
        let region = std::env::var("DYNAMODB_AWS_REGION")
            .context("DYNAMODB_AWS_REGION environment variable is required")?;
        let access_key_id = std::env::var("DYNAMODB_AWS_ACCESS_KEY_ID")
            .context("DYNAMODB_AWS_ACCESS_KEY_ID environment variable is required")?;
        let secret_access_key = std::env::var("DYNAMODB_AWS_SECRET_ACCESS_KEY")
            .context("DYNAMODB_AWS_SECRET_ACCESS_KEY environment variable is required")?;
        let endpoint_url = std::env::var("DYNAMODB_AWS_ENDPOINT_URL").ok();

        Ok(Self {
            region,
            access_key_id,
            secret_access_key,
            endpoint_url,
        })
    }
}

/// AWS `DynamoDB` Streams source for streaming benchmarks.
///
/// Unlike the local Docker-based source, this connects to actual AWS `DynamoDB`.
pub struct DynamoDbStreamsSource {
    config: DynamoDbConfig,
    client: Option<Client>,
    /// Optional table name prefix for isolated test runs.
    table_prefix: Option<String>,
    /// Scale factor used for TPCH data generation.
    scale_factor: Option<f64>,
}

impl DynamoDbStreamsSource {
    /// Create a new AWS `DynamoDB` Streams source with the given configuration.
    #[must_use]
    pub fn new(config: DynamoDbConfig) -> Self {
        Self {
            config,
            client: None,
            table_prefix: None,
            scale_factor: None,
        }
    }

    /// Get the actual table name, applying prefix if set.
    fn prefixed_table_name(&self, base_name: &str) -> String {
        match &self.table_prefix {
            Some(prefix) => format!("{prefix}_{base_name}"),
            None => base_name.to_string(),
        }
    }

    /// Get the `DynamoDB` client.
    fn client(&self) -> Result<&Client> {
        self.client.as_ref().ok_or_else(|| {
            anyhow::anyhow!("DynamoDB client not initialized - call prepare() first")
        })
    }

    /// Create a `DynamoDB` client with the configured authentication.
    fn create_client(config: &DynamoDbConfig) -> Client {
        let mut sdk_config_builder = SdkConfig::builder()
            .retry_config(RetryConfig::standard().with_max_attempts(5))
            .behavior_version(BehaviorVersion::latest())
            .region(Some(Region::new(config.region.clone())));

        // Configure endpoint URL if provided
        if let Some(ref endpoint_url) = config.endpoint_url {
            sdk_config_builder = sdk_config_builder.endpoint_url(endpoint_url.clone());
        }

        // Configure credentials from config (key-based auth only)
        let credentials = Credentials::new(
            config.access_key_id.clone(),
            config.secret_access_key.clone(),
            None,
            None,
            "testoperator-aws-dynamodb",
        );
        sdk_config_builder =
            sdk_config_builder.credentials_provider(SharedCredentialsProvider::new(credentials));

        let sdk_config = sdk_config_builder.build();
        Client::new(&sdk_config)
    }

    /// Format a Decimal128 value as a string with proper decimal places.
    ///
    /// Decimal128 stores values as integers scaled by 10^scale.
    /// For example, 12345 with scale=2 represents 123.45
    #[expect(clippy::cast_sign_loss)]
    fn format_decimal128(value: i128, _precision: u8, scale: i8) -> String {
        if scale <= 0 {
            // No decimal places needed
            let multiplier = 10_i128.pow((-scale) as u32);
            return (value * multiplier).to_string();
        }

        let scale = scale as u32;
        let divisor = 10_i128.pow(scale);
        let integer_part = value / divisor;
        let fractional_part = (value % divisor).abs();

        format!(
            "{integer_part}.{fractional_part:0>width$}",
            width = scale as usize
        )
    }

    /// Convert an Arrow array value to a `DynamoDB` `AttributeValue`.
    fn array_to_attribute(array: &Arc<dyn Array>, row: usize) -> Result<AttributeValue> {
        match array.data_type() {
            DataType::Int16 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Int16Array"))?;
                Ok(AttributeValue::N(arr.value(row).to_string()))
            }
            DataType::Int32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Int32Array"))?;
                Ok(AttributeValue::N(arr.value(row).to_string()))
            }
            DataType::Int64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Int64Array"))?;
                Ok(AttributeValue::N(arr.value(row).to_string()))
            }
            DataType::Float64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Float64Array"))?;
                let value = arr.value(row);
                // Ensure decimal point is always present for proper DynamoDB number handling
                let str_value = if value.fract() == 0.0 {
                    format!("{value:.1}")
                } else {
                    value.to_string()
                };
                Ok(AttributeValue::N(str_value))
            }
            DataType::Decimal128(precision, scale) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Decimal128Array"))?;
                let value = arr.value(row);
                // Convert Decimal128 to string with proper scale
                let str_value = Self::format_decimal128(value, *precision, *scale);
                Ok(AttributeValue::N(str_value))
            }
            DataType::Utf8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| anyhow::anyhow!("Failed to downcast to StringArray"))?;
                Ok(AttributeValue::S(arr.value(row).to_string()))
            }
            DataType::Date32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Date32Array"))?;
                // Store as days since epoch
                Ok(AttributeValue::N(arr.value(row).to_string()))
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| {
                        anyhow::anyhow!("Failed to downcast to TimestampMicrosecondArray")
                    })?;
                // Store as microseconds since epoch
                Ok(AttributeValue::N(arr.value(row).to_string()))
            }
            dt => Err(anyhow::anyhow!("Unsupported data type: {dt:?}")),
        }
    }

    /// Check if an error is a "resource not found" error.
    fn is_resource_not_found<E: std::fmt::Debug>(
        err: &aws_sdk_dynamodb::error::SdkError<E>,
    ) -> bool {
        // Check the raw response for 400 status with ResourceNotFoundException
        if let aws_sdk_dynamodb::error::SdkError::ServiceError(service_err) = err {
            let raw = service_err.raw();
            // ResourceNotFoundException returns 400 status
            if raw.status().as_u16() == 400 {
                // Check error code in the response
                let body = format!("{:?}", service_err.err());
                return body.contains("ResourceNotFoundException");
            }
        }
        // Fallback to string matching
        let err_str = format!("{err:?}");
        err_str.contains("ResourceNotFoundException") || err_str.contains("resource not found")
    }

    /// Build tags for a new table.
    #[expect(clippy::expect_used)]
    fn build_table_tags(run_id: &str, scale_factor: Option<f64>) -> Vec<Tag> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Note: Tag::builder().build() can't fail with valid key/value
        let mut tags = vec![
            Tag::builder()
                .key(TAG_CREATED_AT)
                .value(now.to_string())
                .build()
                .expect("valid tag"),
            Tag::builder()
                .key(TAG_RUN_ID)
                .value(run_id)
                .build()
                .expect("valid tag"),
        ];

        if let Some(sf) = scale_factor {
            tags.push(
                Tag::builder()
                    .key(TAG_SCALE_FACTOR)
                    .value(sf.to_string())
                    .build()
                    .expect("valid tag"),
            );
        }

        tags
    }

    /// Clean up stale tables (older than 24 hours) created by testoperator.
    ///
    /// This scans all tables, checks for the `testoperator:created_at` tag,
    /// and deletes tables older than `STALE_TABLE_AGE_SECS`.
    async fn cleanup_stale_tables(client: &Client) -> Result<()> {
        println!("Scanning for stale testoperator tables (>24h old)...");

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let cutoff = now.saturating_sub(STALE_TABLE_AGE_SECS);

        // List all tables
        let mut tables_to_delete = Vec::new();
        let mut last_evaluated_table: Option<String> = None;

        loop {
            let mut request = client.list_tables();
            if let Some(ref last) = last_evaluated_table {
                request = request.exclusive_start_table_name(last);
            }

            let response = request.send().await.context("Failed to list tables")?;

            if let Some(table_names) = response.table_names {
                for table_name in table_names {
                    // Get table ARN via describe_table
                    let table_arn =
                        match client.describe_table().table_name(&table_name).send().await {
                            Ok(desc) => desc.table().and_then(|t| t.table_arn()).map(String::from),
                            Err(_) => continue, // Skip if we can't describe the table
                        };

                    let Some(arn) = table_arn else {
                        continue;
                    };

                    // Get tags for this table using the ARN
                    if let Ok(tags_response) = client
                        .list_tags_of_resource()
                        .resource_arn(&arn)
                        .send()
                        .await
                    {
                        if let Some(tags) = tags_response.tags {
                            // Look for our created_at tag
                            for tag in tags {
                                if tag.key() == TAG_CREATED_AT {
                                    let value = tag.value();
                                    if let Ok(created_at) = value.parse::<u64>()
                                        && created_at < cutoff
                                    {
                                        let age_hours = (now - created_at) / 3600;
                                        println!(
                                            "  Found stale table: {table_name} ({age_hours}h old)"
                                        );
                                        tables_to_delete.push(table_name.clone());
                                    }
                                }
                            }
                        }
                    } else {
                        // Can't get tags, skip this table (might not be ours)
                    }
                }
            }

            last_evaluated_table = response.last_evaluated_table_name;
            if last_evaluated_table.is_none() {
                break;
            }
        }

        if tables_to_delete.is_empty() {
            println!("No stale tables found");
            return Ok(());
        }

        println!("Deleting {} stale tables...", tables_to_delete.len());

        for table_name in tables_to_delete {
            match client.delete_table().table_name(&table_name).send().await {
                Ok(_) => {
                    println!("  Deleted stale table: {table_name}");
                }
                Err(e) => {
                    if Self::is_resource_not_found(&e) {
                        // Already deleted by another process
                        println!("  Table {table_name} already deleted");
                    } else {
                        // Log but continue with other tables
                        eprintln!("  Failed to delete {table_name}: {e}");
                    }
                }
            }
        }

        println!("Stale table cleanup complete");
        Ok(())
    }

    /// Delete a table if it exists and wait for deletion to complete.
    async fn delete_table_if_exists(client: &Client, table_name: &str) -> Result<()> {
        // Check if table exists
        match client.describe_table().table_name(table_name).send().await {
            Ok(_) => {
                println!("Table '{table_name}' exists, deleting...");
                client
                    .delete_table()
                    .table_name(table_name)
                    .send()
                    .await
                    .with_context(|| format!("Failed to delete table {table_name}"))?;

                // Wait for table to be deleted
                let timeout = Duration::from_secs(120);
                let start = std::time::Instant::now();

                loop {
                    match client.describe_table().table_name(table_name).send().await {
                        Ok(_) => {
                            if start.elapsed() > timeout {
                                return Err(anyhow::anyhow!(
                                    "Timeout waiting for table '{table_name}' to be deleted"
                                ));
                            }
                            sleep(Duration::from_secs(2)).await;
                        }
                        Err(e) => {
                            if Self::is_resource_not_found(&e) {
                                println!("Table '{table_name}' deleted successfully");
                                return Ok(());
                            }
                            // Some other error, keep waiting
                            if start.elapsed() > timeout {
                                return Err(anyhow::anyhow!(
                                    "Timeout waiting for table '{table_name}' to be deleted"
                                ));
                            }
                            sleep(Duration::from_secs(2)).await;
                        }
                    }
                }
            }
            Err(e) => {
                if Self::is_resource_not_found(&e) {
                    // Table doesn't exist, nothing to delete
                    println!("Table '{table_name}' does not exist, skipping deletion");
                    Ok(())
                } else {
                    Err(anyhow::anyhow!(
                        "Failed to describe table {table_name}: {e}"
                    ))
                }
            }
        }
    }

    /// Wait for a table to become ACTIVE.
    async fn wait_for_table_active(client: &Client, table_name: &str) -> Result<()> {
        let timeout = Duration::from_secs(120);
        let start = std::time::Instant::now();

        loop {
            let response = client
                .describe_table()
                .table_name(table_name)
                .send()
                .await
                .with_context(|| format!("Failed to describe table {table_name}"))?;

            if let Some(table) = response.table()
                && let Some(status) = table.table_status()
            {
                if status.as_str() == "ACTIVE" {
                    println!("Table '{table_name}' is now ACTIVE");
                    return Ok(());
                }
                println!(
                    "Table '{table_name}' status: {}, waiting...",
                    status.as_str()
                );
            }

            if start.elapsed() > timeout {
                return Err(anyhow::anyhow!(
                    "Timeout waiting for table '{table_name}' to become ACTIVE"
                ));
            }

            sleep(Duration::from_secs(2)).await;
        }
    }

    /// Convert a record batch row to a `HashMap` of attribute values.
    fn row_to_item(batch: &RecordBatch, row: usize) -> Result<HashMap<String, AttributeValue>> {
        let schema = batch.schema();
        let mut item = HashMap::new();

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let array = batch.column(col_idx);
            let attr_value = Self::array_to_attribute(array, row)?;
            item.insert(field.name().clone(), attr_value);
        }

        Ok(item)
    }

    /// Perform batch writes with parallelization.
    #[expect(clippy::cast_precision_loss)]
    async fn batch_write_items(
        client: &Client,
        table: &str,
        records: &[RecordBatch],
    ) -> Result<()> {
        let total_rows: usize = records.iter().map(RecordBatch::num_rows).sum();
        println!("Inserting {total_rows} records into DynamoDB table '{table}' using batch writes");

        // Collect all items as WriteRequests
        let mut all_requests: Vec<WriteRequest> = Vec::with_capacity(total_rows);

        for batch in records {
            for row in 0..batch.num_rows() {
                let item = Self::row_to_item(batch, row)?;
                let put_request = PutRequest::builder().set_item(Some(item)).build()?;
                let write_request = WriteRequest::builder().put_request(put_request).build();
                all_requests.push(write_request);
            }
        }

        // Split into batches of BATCH_SIZE
        let batches: Vec<Vec<WriteRequest>> = all_requests
            .chunks(BATCH_SIZE)
            .map(<[aws_sdk_dynamodb::types::WriteRequest]>::to_vec)
            .collect();

        let total_batches = batches.len();
        println!(
            "Split into {total_batches} batches of up to {BATCH_SIZE} items, processing {CONCURRENT_BATCHES} concurrently"
        );

        let inserted = std::sync::atomic::AtomicUsize::new(0);

        // Process batches concurrently
        let results: Vec<Result<()>> = stream::iter(batches.into_iter().enumerate())
            .map(|(batch_idx, batch)| {
                let client = client.clone();
                let table = table.to_string();
                let inserted = &inserted;
                async move {
                    const MAX_RETRIES: usize = 5;
                    let batch_len = batch.len();

                    // Retry logic for unprocessed items
                    let mut items_to_write = batch;
                    let mut retry_count = 0;

                    while !items_to_write.is_empty() && retry_count < MAX_RETRIES {
                        let mut request_items = HashMap::new();
                        request_items.insert(table.clone(), items_to_write.clone());

                        let response = client
                            .batch_write_item()
                            .set_request_items(Some(request_items))
                            .send()
                            .await
                            .with_context(|| {
                                format!("Failed to batch write items (batch {batch_idx})")
                            })?;

                        // Check for unprocessed items
                        if let Some(unprocessed) = response.unprocessed_items()
                            && let Some(remaining) = unprocessed.get(&table)
                            && !remaining.is_empty()
                        {
                            retry_count += 1;
                            let backoff = Duration::from_millis(100 * (1 << retry_count));
                            sleep(backoff).await;
                            items_to_write.clone_from(remaining);
                            continue;
                        }

                        // All items written successfully
                        items_to_write.clear();
                    }

                    if !items_to_write.is_empty() {
                        return Err(anyhow::anyhow!(
                            "Failed to write {} items after {MAX_RETRIES} retries",
                            items_to_write.len()
                        ));
                    }

                    let prev = inserted.fetch_add(batch_len, std::sync::atomic::Ordering::Relaxed);
                    let current = prev + batch_len;

                    if current.is_multiple_of(1000) || current == total_rows {
                        println!(
                            "Inserted {current}/{total_rows} records ({:.1}%)",
                            (current as f64 / total_rows as f64) * 100.0
                        );
                    }

                    Ok(())
                }
            })
            .buffer_unordered(CONCURRENT_BATCHES)
            .collect()
            .await;

        // Check for any errors
        for result in results {
            result?;
        }

        let final_count = inserted.load(std::sync::atomic::Ordering::Relaxed);
        println!("Successfully inserted {final_count} records into '{table}'");
        Ok(())
    }

    /// Perform batch deletes with parallelization.
    #[expect(clippy::cast_precision_loss)]
    async fn batch_delete_items(client: &Client, table: &str, keys: &[RecordBatch]) -> Result<()> {
        use aws_sdk_dynamodb::types::DeleteRequest;

        let total_rows: usize = keys.iter().map(RecordBatch::num_rows).sum();
        println!("Deleting {total_rows} records from DynamoDB table '{table}' using batch deletes");

        // Collect all items as WriteRequests (for delete)
        let mut all_requests: Vec<WriteRequest> = Vec::with_capacity(total_rows);

        for batch in keys {
            for row in 0..batch.num_rows() {
                let item = Self::row_to_item(batch, row)?;
                let delete_request = DeleteRequest::builder().set_key(Some(item)).build()?;
                let write_request = WriteRequest::builder()
                    .delete_request(delete_request)
                    .build();
                all_requests.push(write_request);
            }
        }

        // Split into batches of BATCH_SIZE
        let batches: Vec<Vec<WriteRequest>> = all_requests
            .chunks(BATCH_SIZE)
            .map(<[aws_sdk_dynamodb::types::WriteRequest]>::to_vec)
            .collect();

        let total_batches = batches.len();
        println!(
            "Split into {total_batches} batches of up to {BATCH_SIZE} items, processing {CONCURRENT_BATCHES} concurrently"
        );

        let deleted = std::sync::atomic::AtomicUsize::new(0);

        // Process batches concurrently
        let results: Vec<Result<()>> = stream::iter(batches.into_iter().enumerate())
            .map(|(batch_idx, batch)| {
                let client = client.clone();
                let table = table.to_string();
                let deleted = &deleted;
                async move {
                    const MAX_RETRIES: usize = 5;
                    let batch_len = batch.len();

                    // Retry logic for unprocessed items
                    let mut items_to_delete = batch;
                    let mut retry_count = 0;

                    while !items_to_delete.is_empty() && retry_count < MAX_RETRIES {
                        let mut request_items = HashMap::new();
                        request_items.insert(table.clone(), items_to_delete.clone());

                        let response = client
                            .batch_write_item()
                            .set_request_items(Some(request_items))
                            .send()
                            .await
                            .with_context(|| {
                                format!("Failed to batch delete items (batch {batch_idx})")
                            })?;

                        // Check for unprocessed items
                        if let Some(unprocessed) = response.unprocessed_items()
                            && let Some(remaining) = unprocessed.get(&table)
                            && !remaining.is_empty()
                        {
                            retry_count += 1;
                            let backoff = Duration::from_millis(100 * (1 << retry_count));
                            sleep(backoff).await;
                            items_to_delete.clone_from(remaining);
                            continue;
                        }

                        // All items deleted successfully
                        items_to_delete.clear();
                    }

                    if !items_to_delete.is_empty() {
                        return Err(anyhow::anyhow!(
                            "Failed to delete {} items after {MAX_RETRIES} retries",
                            items_to_delete.len()
                        ));
                    }

                    let prev = deleted.fetch_add(batch_len, std::sync::atomic::Ordering::Relaxed);
                    let current = prev + batch_len;

                    if current.is_multiple_of(100) || current == total_rows {
                        println!(
                            "Deleted {current}/{total_rows} records ({:.1}%)",
                            (current as f64 / total_rows as f64) * 100.0
                        );
                    }

                    Ok(())
                }
            })
            .buffer_unordered(CONCURRENT_BATCHES)
            .collect()
            .await;

        // Check for any errors
        for result in results {
            result?;
        }

        let final_count = deleted.load(std::sync::atomic::Ordering::Relaxed);
        println!("Successfully deleted {final_count} records from '{table}'");
        Ok(())
    }

    /// Create a simple table with a single hash key.
    async fn create_simple_table(
        &self,
        client: &Client,
        table_name: &str,
        hash_key: &str,
    ) -> Result<()> {
        let run_id = self.table_prefix.as_deref().unwrap_or("unknown");
        let tags = Self::build_table_tags(run_id, self.scale_factor);

        client
            .create_table()
            .table_name(table_name)
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name(hash_key)
                    .attribute_type(ScalarAttributeType::N)
                    .build()
                    .context("Failed to build attribute definition")?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name(hash_key)
                    .key_type(KeyType::Hash)
                    .build()
                    .context("Failed to build key schema")?,
            )
            .billing_mode(BillingMode::PayPerRequest)
            .stream_specification(
                StreamSpecification::builder()
                    .stream_enabled(true)
                    .stream_view_type(StreamViewType::NewAndOldImages)
                    .build()
                    .context("Failed to build stream specification")?,
            )
            .set_tags(Some(tags))
            .send()
            .await
            .with_context(|| format!("Failed to create {table_name} table"))?;

        println!("Created table '{table_name}' with DynamoDB Streams enabled");
        Ok(())
    }

    /// Create the lineitem table with composite key and specified table name.
    async fn create_lineitem_table_named(&self, client: &Client, table_name: &str) -> Result<()> {
        let run_id = self.table_prefix.as_deref().unwrap_or("unknown");
        let tags = Self::build_table_tags(run_id, self.scale_factor);

        client
            .create_table()
            .table_name(table_name)
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("l_orderkey")
                    .attribute_type(ScalarAttributeType::N)
                    .build()
                    .context("Failed to build l_orderkey attribute definition")?,
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("l_linenumber")
                    .attribute_type(ScalarAttributeType::N)
                    .build()
                    .context("Failed to build l_linenumber attribute definition")?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("l_orderkey")
                    .key_type(KeyType::Hash)
                    .build()
                    .context("Failed to build l_orderkey key schema")?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("l_linenumber")
                    .key_type(KeyType::Range)
                    .build()
                    .context("Failed to build l_linenumber key schema")?,
            )
            .billing_mode(BillingMode::PayPerRequest)
            .stream_specification(
                StreamSpecification::builder()
                    .stream_enabled(true)
                    .stream_view_type(StreamViewType::NewAndOldImages)
                    .build()
                    .context("Failed to build stream specification")?,
            )
            .set_tags(Some(tags))
            .send()
            .await
            .with_context(|| format!("Failed to create {table_name} table"))?;

        println!("Created table '{table_name}' with DynamoDB Streams enabled");
        Ok(())
    }

    /// Create the partsupp table with composite key and specified table name.
    async fn create_partsupp_table_named(&self, client: &Client, table_name: &str) -> Result<()> {
        let run_id = self.table_prefix.as_deref().unwrap_or("unknown");
        let tags = Self::build_table_tags(run_id, self.scale_factor);

        client
            .create_table()
            .table_name(table_name)
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("ps_partkey")
                    .attribute_type(ScalarAttributeType::N)
                    .build()
                    .context("Failed to build ps_partkey attribute definition")?,
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("ps_suppkey")
                    .attribute_type(ScalarAttributeType::N)
                    .build()
                    .context("Failed to build ps_suppkey attribute definition")?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("ps_partkey")
                    .key_type(KeyType::Hash)
                    .build()
                    .context("Failed to build ps_partkey key schema")?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("ps_suppkey")
                    .key_type(KeyType::Range)
                    .build()
                    .context("Failed to build ps_suppkey key schema")?,
            )
            .billing_mode(BillingMode::PayPerRequest)
            .stream_specification(
                StreamSpecification::builder()
                    .stream_enabled(true)
                    .stream_view_type(StreamViewType::NewAndOldImages)
                    .build()
                    .context("Failed to build stream specification")?,
            )
            .set_tags(Some(tags))
            .send()
            .await
            .with_context(|| format!("Failed to create {table_name} table"))?;

        println!("Created table '{table_name}' with DynamoDB Streams enabled");
        Ok(())
    }

    /// Delete a marker record with a single hash key.
    async fn delete_simple_marker(
        &self,
        client: &Client,
        table_name: &str,
        hash_key: &str,
    ) -> Result<()> {
        client
            .delete_item()
            .table_name(table_name)
            .key(hash_key, AttributeValue::N("-1".to_string()))
            .send()
            .await
            .with_context(|| format!("Failed to delete {table_name} marker record"))?;

        println!("Deleted marker record from '{table_name}'");
        Ok(())
    }

    /// Delete the lineitem marker record from the specified table.
    async fn delete_lineitem_marker_named(&self, client: &Client, table_name: &str) -> Result<()> {
        // Delete all marker records (multiple markers with different l_orderkey values)
        const MARKER_ORDER_KEYS: [i64; 8] = [
            -1,
            -1000,
            -10000,
            -100_000,
            -1_000_000,
            -2_000_000,
            -5_000_000,
            -10_000_000,
        ];

        for order_key in MARKER_ORDER_KEYS {
            client
                .delete_item()
                .table_name(table_name)
                .key("l_orderkey", AttributeValue::N(order_key.to_string()))
                .key("l_linenumber", AttributeValue::N("-1".to_string()))
                .send()
                .await
                .with_context(|| {
                    format!("Failed to delete {table_name} marker record (l_orderkey={order_key})")
                })?;
        }

        println!(
            "Deleted {} marker records from '{table_name}'",
            MARKER_ORDER_KEYS.len()
        );
        Ok(())
    }

    /// Delete the partsupp marker record from the specified table.
    async fn delete_partsupp_marker_named(&self, client: &Client, table_name: &str) -> Result<()> {
        client
            .delete_item()
            .table_name(table_name)
            .key("ps_partkey", AttributeValue::N("-1".to_string()))
            .key("ps_suppkey", AttributeValue::N("-1".to_string()))
            .send()
            .await
            .with_context(|| format!("Failed to delete {table_name} marker record"))?;

        println!("Deleted marker record from '{table_name}'");
        Ok(())
    }
}

#[async_trait::async_trait]
impl StreamingSource for DynamoDbStreamsSource {
    fn set_table_prefix(&mut self, prefix: String) {
        println!("Setting table prefix: {prefix}");
        self.table_prefix = Some(prefix);
    }

    fn set_scale_factor(&mut self, scale_factor: f64) {
        println!("Setting scale factor: {scale_factor}");
        self.scale_factor = Some(scale_factor);
    }

    fn get_table_name(&self, base_name: &str) -> String {
        self.prefixed_table_name(base_name)
    }

    async fn prepare(&mut self) -> Result<()> {
        // For AWS DynamoDB, we just need to create the client
        // No container management needed
        println!(
            "Initializing AWS DynamoDB client for region {}",
            self.config.region
        );

        if let Some(ref endpoint) = self.config.endpoint_url {
            println!("Using custom endpoint: {endpoint}");
        }

        if let Some(ref prefix) = self.table_prefix {
            println!("Using table prefix: {prefix}");
        }

        let client = Self::create_client(&self.config);

        // Clean up stale tables from previous runs (>24h old)
        if let Err(e) = Self::cleanup_stale_tables(&client).await {
            // Log but don't fail - cleanup is best-effort
            eprintln!("Warning: Failed to cleanup stale tables: {e}");
        }

        self.client = Some(client);

        println!("AWS DynamoDB client initialized successfully");
        Ok(())
    }

    async fn create_table(&self, dataset: DatasetType) -> Result<()> {
        let client = self.client()?;
        let base_name = dataset.table_name();
        let table_name = self.prefixed_table_name(base_name);

        // Delete table if it exists
        Self::delete_table_if_exists(client, &table_name).await?;

        // Create the table with prefixed name
        match dataset {
            DatasetType::Lineitem => {
                self.create_lineitem_table_named(client, &table_name)
                    .await?;
            }
            DatasetType::Orders => {
                self.create_simple_table(client, &table_name, "o_orderkey")
                    .await?;
            }
            DatasetType::Customer => {
                self.create_simple_table(client, &table_name, "c_custkey")
                    .await?;
            }
            DatasetType::Part => {
                self.create_simple_table(client, &table_name, "p_partkey")
                    .await?;
            }
            DatasetType::Supplier => {
                self.create_simple_table(client, &table_name, "s_suppkey")
                    .await?;
            }
            DatasetType::Partsupp => {
                self.create_partsupp_table_named(client, &table_name)
                    .await?;
            }
            DatasetType::Nation => {
                self.create_simple_table(client, &table_name, "n_nationkey")
                    .await?;
            }
            DatasetType::Region => {
                self.create_simple_table(client, &table_name, "r_regionkey")
                    .await?;
            }
        }

        // Wait for table to be ACTIVE
        Self::wait_for_table_active(client, &table_name).await?;

        Ok(())
    }

    async fn insert(&self, table: &str, records: &[RecordBatch]) -> Result<()> {
        let client = self.client()?;
        Self::batch_write_items(client, table, records).await
    }

    async fn delete_marker(&self, dataset: DatasetType) -> Result<()> {
        let client = self.client()?;
        let base_name = dataset.table_name();
        let table_name = self.prefixed_table_name(base_name);

        match dataset {
            DatasetType::Lineitem => {
                self.delete_lineitem_marker_named(client, &table_name)
                    .await?;
            }
            DatasetType::Orders => {
                self.delete_simple_marker(client, &table_name, "o_orderkey")
                    .await?;
            }
            DatasetType::Customer => {
                self.delete_simple_marker(client, &table_name, "c_custkey")
                    .await?;
            }
            DatasetType::Part => {
                self.delete_simple_marker(client, &table_name, "p_partkey")
                    .await?;
            }
            DatasetType::Supplier => {
                self.delete_simple_marker(client, &table_name, "s_suppkey")
                    .await?;
            }
            DatasetType::Partsupp => {
                self.delete_partsupp_marker_named(client, &table_name)
                    .await?;
            }
            DatasetType::Nation => {
                self.delete_simple_marker(client, &table_name, "n_nationkey")
                    .await?;
            }
            DatasetType::Region => {
                self.delete_simple_marker(client, &table_name, "r_regionkey")
                    .await?;
            }
        }

        Ok(())
    }

    async fn update(&self, table: &str, records: &[RecordBatch]) -> Result<()> {
        // DynamoDB uses upsert semantics, so update is the same as insert
        self.insert(table, records).await
    }

    async fn delete(&self, table: &str, keys: &[RecordBatch]) -> Result<()> {
        let client = self.client()?;
        Self::batch_delete_items(client, table, keys).await
    }

    async fn cleanup(&self) -> Result<()> {
        // For AWS DynamoDB, we don't delete the tables on cleanup
        // as they may be expensive to recreate or contain other data.
        // Users should manage table lifecycle separately.
        println!("AWS DynamoDB cleanup complete (tables preserved)");
        Ok(())
    }
}

impl DynamoDBStreamingSource for DynamoDbStreamsSource {
    fn prepare_checkpoint_spicepod(
        &self,
        spicepod: SpicepodDefinition,
        run_id: &str,
        config_name: &str,
        snapshot_config: &SnapshotConfig,
    ) -> Result<SpicepodDefinition> {
        transform_spicepod(
            spicepod,
            run_id,
            config_name,
            snapshot_config,
            SnapshotBehavior::CreateOnly,
        )
    }

    fn prepare_benchmark_spicepod(
        &self,
        spicepod: SpicepodDefinition,
        run_id: &str,
        config_name: &str,
        snapshot_config: &SnapshotConfig,
    ) -> Result<SpicepodDefinition> {
        transform_spicepod(
            spicepod,
            run_id,
            config_name,
            snapshot_config,
            SnapshotBehavior::BootstrapOnly,
        )
    }
}

/// Transform a spicepod for `DynamoDB` streaming benchmarks.
///
/// This function:
/// 1. Prefixes the table name in `from` field (e.g., `dynamodb:lineitem` -> `dynamodb:{run_id}_lineitem`)
/// 2. Sets `acceleration.snapshots` to the specified behavior
/// 3. Adds engine-specific file path configs (`duckdb_file` or `cayenne_file_path`) based on acceleration.engine
/// 4. Configures runtime snapshots with unique location per config
/// 5. Uses different paths for checkpoint vs benchmark phases
fn transform_spicepod(
    mut spicepod: SpicepodDefinition,
    run_id: &str,
    config_name: &str,
    snapshot_config: &SnapshotConfig,
    snapshot_behavior: SnapshotBehavior,
) -> Result<SpicepodDefinition> {
    // Determine phase suffix for file paths
    let phase_suffix = match snapshot_behavior {
        SnapshotBehavior::CreateOnly => "checkpoint",
        SnapshotBehavior::BootstrapOnly => "benchmark",
        _ => "unknown",
    };

    std::fs::create_dir_all(format!("/tmp/benchmarks/{run_id}"))
        .context("Failed to create benchmark directory")?;

    // 1. Update dataset `from` field with prefixed table name, keep `name` unchanged
    for dataset in &mut spicepod.datasets {
        if let ComponentOrReference::Component(d) = dataset {
            // Prefix the table name in the `from` field (e.g., dynamodb:lineitem -> dynamodb:abc123_lineitem)
            // The `name` field stays unchanged so SQL queries work as expected
            if let Some(table_name) = d.from.strip_prefix("dynamodb:") {
                // Skip if table name already appears to have a hex prefix (e.g., "57be98_supplier")
                // This prevents double-prefixing if the spicepod was already transformed
                let already_prefixed = table_name.split('_').next().is_some_and(|first| {
                    first.len() == 6 && first.chars().all(|c| c.is_ascii_hexdigit())
                });

                if already_prefixed {
                    eprintln!(
                        "Warning: Table '{table_name}' appears to already have a prefix, skipping transformation"
                    );
                } else {
                    d.from = format!("dynamodb:{run_id}_{table_name}");
                }

                // Add DynamoDB-specific metrics for tracking records consumed and transient errors
                d.metrics = Some(Metrics {
                    metrics: vec![
                        Metric {
                            name: "records_consumed_total".to_string(),
                            enabled: true,
                        },
                        Metric {
                            name: "errors_transient_total".to_string(),
                            enabled: true,
                        },
                    ],
                });
            }

            // Set acceleration snapshot behavior and add engine-specific file paths
            if let Some(ref mut accel) = d.acceleration {
                accel.snapshots = snapshot_behavior;

                let dataset_name = &d.name;
                let params = accel.params.get_or_insert_with(Params::default);

                // Check acceleration.engine to determine which path to set
                // engine: None or "duckdb" -> set duckdb_file
                // engine: "cayenne" -> set cayenne_file_path
                let engine = accel.engine.as_deref();

                match engine {
                    None | Some("duckdb") => {
                        let duckdb_file = format!(
                            "/tmp/benchmarks/{run_id}/{config_name}_{dataset_name}_{phase_suffix}.db"
                        );
                        params
                            .data
                            .insert("duckdb_file".to_string(), ParamValue::String(duckdb_file));
                    }
                    Some("cayenne") => {
                        let cayenne_dir = format!(
                            "/tmp/benchmarks/{run_id}/{config_name}_{dataset_name}_{phase_suffix}_cayenne/"
                        );
                        params.data.insert(
                            "cayenne_file_path".to_string(),
                            ParamValue::String(cayenne_dir),
                        );
                    }
                    Some(other) => {
                        // Unknown engine, log and skip
                        eprintln!(
                            "Warning: Unknown acceleration engine '{other}' for dataset {dataset_name}, skipping path configuration"
                        );
                    }
                }
            }
        }
    }

    // 2. Configure runtime snapshots
    let location = format!(
        "{}/{}/{}/",
        snapshot_config.location.trim_end_matches('/'),
        run_id,
        config_name
    );

    let mut params = Params::default();
    params
        .data
        .insert("s3_auth".to_string(), ParamValue::String("key".to_string()));
    params.data.insert(
        "s3_key".to_string(),
        ParamValue::String("${secrets:SNAPSHOT_S3_ACCESS_KEY_ID}".to_string()),
    );
    params.data.insert(
        "s3_secret".to_string(),
        ParamValue::String("${secrets:SNAPSHOT_S3_SECRET_ACCESS_KEY}".to_string()),
    );
    if let Some(ref region) = snapshot_config.region {
        params
            .data
            .insert("s3_region".to_string(), ParamValue::String(region.clone()));
    }

    spicepod.snapshots = Some(Snapshots {
        enabled: true,
        location: Some(location),
        params: if params.data.is_empty() {
            None
        } else {
            Some(params)
        },
        ..Default::default()
    });

    Ok(spicepod)
}
